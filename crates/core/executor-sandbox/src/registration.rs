//! The provider kinds a run can reach, and how each is reached.
//!
//! A [`ProviderRegistrations`] maps a provider kind to its transport: a
//! built-in provider linked into this process, the kind's first-party
//! plugin configured from the environment on first use, or an explicitly
//! registered plugin for a custom kind with the [`SandboxSpec`] template
//! its process scopes are built from. The router holds one; a kind with no
//! entry fails routably at acquire, and nothing is launched in its place.
//!
//! Registering never launches a plugin or contacts a provider. A plugin's
//! settings are frozen as given: an absolute executable, its arguments and
//! complete environment, and a checksum or an explicit development opt-in.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use sandbox_driver::{ProviderKind, SandboxSpec};
use sandbox_driver_protocol::discovery::PluginConfig;

use crate::in_process::{InProcessProviders, ProviderFactory};
use crate::plugin::PluginSettings;

/// The kinds Petri prepares scopes for itself.
pub(crate) fn is_builtin(kind: &str) -> bool {
    matches!(kind, "host" | "docker" | "daytona")
}

/// A provider kind a run can select: valid, and not a name Petri reserves
/// (`local` is the frontends' spelling of the host, `simulated` a dry
/// run's provider).
pub(crate) fn selectable_kind(kind: &str) -> Result<ProviderKind, RegistrationError> {
    if matches!(kind, "local" | "simulated") {
        return Err(RegistrationError::Reserved(kind.to_owned()));
    }
    ProviderKind::try_new(kind).map_err(|_| RegistrationError::InvalidKind(kind.to_owned()))
}

/// Why a registration was refused. Launch values are never quoted: a
/// plugin's arguments and environment can hold credentials.
#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    #[error("`{0}` is not a provider kind; use lowercase letters, digits, and interior hyphens")]
    InvalidKind(String),
    #[error("`{0}` is a reserved provider name")]
    Reserved(String),
    #[error("provider `{0}` is already registered")]
    Duplicate(String),
    #[error(
        "`{0}` is a built-in provider kind; it is reached through its linked factory or plugin, not a registered plugin"
    )]
    BuiltIn(String),
    #[error(
        "`{0}` is not a built-in provider kind; link a factory for host, docker, or daytona only"
    )]
    NotBuiltIn(String),
    #[error("the plugin launch configuration is invalid: {0}")]
    Launch(&'static str),
    #[error("the process sandbox template is invalid: {0}")]
    Template(&'static str),
}

/// How one provider kind is reached.
#[derive(Clone)]
pub(crate) enum ProviderTransport {
    /// A built-in provider linked into this process.
    Linked(Arc<dyn ProviderFactory>),
    /// The kind's first-party plugin, configured from this process's
    /// environment on first use.
    EnvPlugin,
    /// An explicitly registered plugin for a custom kind, and the template
    /// its process scopes are built from.
    Plugin(Box<RegisteredPlugin>),
}

/// A plugin registered for a custom kind: how it is launched, and the
/// template its process scopes are built from.
#[derive(Clone)]
pub(crate) struct RegisteredPlugin {
    pub(crate) settings:     PluginSettings,
    pub(crate) process_spec: SandboxSpec,
}

/// The provider kinds a router can reach, and how. A kind with no entry
/// fails routably at acquire; nothing is launched in its place.
#[derive(Clone, Default)]
pub struct ProviderRegistrations {
    entries: BTreeMap<String, ProviderTransport>,
}

impl fmt::Debug for ProviderRegistrations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = formatter.debug_map();
        for (kind, transport) in &self.entries {
            let transport = match transport {
                ProviderTransport::Linked(_) => "linked",
                ProviderTransport::EnvPlugin => "plugin from environment",
                ProviderTransport::Plugin(_) => "registered plugin",
            };
            map.entry(kind, &transport);
        }
        map.finish()
    }
}

impl ProviderRegistrations {
    /// No providers at all. Every kind fails at acquire until registered.
    pub fn new() -> Self {
        Self::default()
    }

    /// Standalone Petri's: the Host, Docker, and Daytona plugins, each
    /// configured from the environment when first used.
    pub fn standalone() -> Self {
        Self {
            entries: ["host", "docker", "daytona"]
                .into_iter()
                .map(|kind| (kind.to_owned(), ProviderTransport::EnvPlugin))
                .collect(),
        }
    }

    /// Only the built-in providers linked in `providers`: a kind with no
    /// factory has no entry, so no plugin is launched in its place.
    pub fn linked(providers: InProcessProviders) -> Self {
        Self {
            entries: providers
                .into_factories()
                .into_iter()
                .map(|(kind, factory)| (kind, ProviderTransport::Linked(factory)))
                .collect(),
        }
    }

    /// Reach a built-in kind through `factory`, linked into this process.
    pub fn register_linked(
        &mut self,
        factory: Arc<dyn ProviderFactory>,
    ) -> Result<(), RegistrationError> {
        let kind = factory.kind().to_owned();
        if !is_builtin(&kind) {
            return Err(RegistrationError::NotBuiltIn(kind));
        }
        self.insert(kind, ProviderTransport::Linked(factory))
    }

    /// Reach a custom kind through the plugin `config` launches, building
    /// each of its process scopes from `process_spec`. The kind is the
    /// config's; it must not be a built-in one. Petri names the sandbox and
    /// owns the `petri.*` labels, so the template carries neither.
    pub fn register_plugin(
        &mut self,
        config: PluginConfig,
        process_spec: SandboxSpec,
    ) -> Result<(), RegistrationError> {
        let kind = selectable_kind(config.kind.as_str())?;
        if is_builtin(kind.as_str()) {
            return Err(RegistrationError::BuiltIn(kind.to_string()));
        }
        if process_spec.name.is_some() {
            return Err(RegistrationError::Template(
                "Petri names each sandbox; leave `name` unset",
            ));
        }
        if process_spec
            .labels
            .keys()
            .any(|key| key.starts_with("petri."))
        {
            return Err(RegistrationError::Template(
                "Petri owns the `petri.*` labels; leave them unset",
            ));
        }
        process_spec
            .network
            .validate()
            .map_err(|_| RegistrationError::Template("the network policy is invalid"))?;
        let settings = PluginSettings::explicit(config)?;
        self.insert(
            kind.to_string(),
            ProviderTransport::Plugin(Box::new(RegisteredPlugin {
                settings,
                process_spec,
            })),
        )
    }

    fn insert(
        &mut self,
        kind: String,
        transport: ProviderTransport,
    ) -> Result<(), RegistrationError> {
        if self.entries.contains_key(&kind) {
            return Err(RegistrationError::Duplicate(kind));
        }
        self.entries.insert(kind, transport);
        Ok(())
    }

    pub(crate) fn get(&self, kind: &str) -> Result<&ProviderTransport, String> {
        self.entries.get(kind).ok_or_else(|| {
            format!(
                "sandbox provider `{kind}` is not registered for this run, and no plugin is \
                 launched in its place"
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use sandbox_driver::{SandboxProvider, SandboxSource};

    use super::*;
    use crate::in_process::{ProviderContext, ProviderNetwork};

    /// A factory for `kind` that is registered and never connected.
    struct Factory(&'static str);

    #[async_trait]
    impl ProviderFactory for Factory {
        fn kind(&self) -> &str {
            self.0
        }

        fn fingerprint_seed(&self, _: &ProviderContext) -> String {
            self.0.to_owned()
        }

        fn network(&self) -> ProviderNetwork {
            ProviderNetwork::none()
        }

        async fn connect(
            &self,
            _: &ProviderContext,
        ) -> sandbox_driver::Result<Arc<dyn SandboxProvider>> {
            panic!("registration never connects a provider")
        }
    }

    fn config(kind: &str) -> PluginConfig {
        let mut config = PluginConfig::new(ProviderKind::try_new(kind).expect("a valid kind"));
        config.path = Some("/not-installed/provider".into());
        config.dev = true;
        config
    }

    fn template() -> SandboxSpec {
        SandboxSpec::new(SandboxSource::HostDirectory)
    }

    #[test]
    fn a_registry_takes_each_kind_once_and_keeps_built_in_and_custom_apart() {
        let mut providers = ProviderRegistrations::new();
        providers
            .register_linked(Arc::new(Factory("host")))
            .expect("a linked host");
        assert!(matches!(
            providers.register_linked(Arc::new(Factory("host"))),
            Err(RegistrationError::Duplicate(kind)) if kind == "host"
        ));
        assert!(matches!(
            providers.register_linked(Arc::new(Factory("lima"))),
            Err(RegistrationError::NotBuiltIn(kind)) if kind == "lima"
        ));
        assert!(matches!(
            providers.register_plugin(config("docker"), template()),
            Err(RegistrationError::BuiltIn(kind)) if kind == "docker"
        ));
        assert!(matches!(
            providers.register_plugin(config("simulated"), template()),
            Err(RegistrationError::Reserved(_))
        ));
        assert!(matches!(
            providers.register_plugin(config("local"), template()),
            Err(RegistrationError::Reserved(_))
        ));
        providers
            .register_plugin(config("lima"), template())
            .expect("a custom plugin");
        assert!(matches!(
            providers.register_plugin(config("lima"), template()),
            Err(RegistrationError::Duplicate(kind)) if kind == "lima"
        ));
        assert!(providers.get("lima").is_ok());
        let missing = providers.get("e2b").err().expect("no e2b");
        assert!(missing.contains("not registered"), "{missing}");
        assert_eq!(
            format!("{providers:?}"),
            r#"{"host": "linked", "lima": "registered plugin"}"#
        );
    }

    #[test]
    fn a_template_leaves_the_name_and_petri_labels_to_petri() {
        let mut providers = ProviderRegistrations::new();
        assert!(matches!(
            providers.register_plugin(config("lima"), template().name("mine")),
            Err(RegistrationError::Template(_))
        ));
        assert!(matches!(
            providers.register_plugin(config("lima"), template().label("petri.run", "mine")),
            Err(RegistrationError::Template(_))
        ));
        let mut empty_allow_list = template();
        empty_allow_list.network =
            sandbox_driver::NetworkPolicy::CidrAllowList { cidrs: Vec::new() };
        assert!(matches!(
            providers.register_plugin(config("lima"), empty_allow_list),
            Err(RegistrationError::Template(_))
        ));
    }

    #[test]
    fn an_explicit_launch_is_absolute_and_trusted_and_reads_no_environment() {
        let mut launch = config("lima");
        launch.dev = false;
        assert!(matches!(
            PluginSettings::explicit(launch.clone()),
            Err(RegistrationError::Launch(_))
        ));
        launch.sha256 = Some("a".repeat(64));
        assert!(PluginSettings::explicit(launch.clone()).is_ok());
        launch.inherit_env.push("SECRET".to_owned());
        assert!(PluginSettings::explicit(launch.clone()).is_err());
        launch.inherit_env.clear();
        launch.path = Some("provider-on-path".into());
        assert!(PluginSettings::explicit(launch.clone()).is_err());
        launch.path = None;
        assert!(PluginSettings::explicit(launch.clone()).is_err());
        launch.path = Some("/opt/provider".into());
        launch.args.push("--token=value".to_owned());
        launch.env.insert("TOKEN".to_owned(), "value".to_owned());
        let settings = PluginSettings::explicit(launch).expect("valid");
        let debug = format!("{settings:?}");
        assert!(debug.contains("TOKEN"), "{debug}");
        assert!(!debug.contains("value"), "{debug}");
        assert!(debug.contains("argument_count: 1"), "{debug}");
    }
}
