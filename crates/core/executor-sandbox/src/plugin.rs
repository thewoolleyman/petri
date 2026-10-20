//! Launching, verifying, and supervising sandbox-driver plugins.
//!
//! By default Petri reaches every sandbox provider through sandbox-driver's
//! JSON-RPC plugin protocol, its own first-party providers included; a
//! third-party provider can only ever be a plugin. An embedder that links the
//! first-party providers can hand them over in process instead
//! ([`crate::in_process`]), and no plugin is launched. One [`PluginSettings`]
//! per provider kind says where the executable is, what its checksum must be,
//! and what a sandbox uses to reach services on this machine; one
//! [`PluginSupervisor`] per kind owns the running process.
//!
//! # Resolution and trust
//!
//! The executable is `PETRI_SANDBOX_<KIND>_PLUGIN` when set, else
//! `sandbox-driver-<kind>` beside Petri's own executable (a release bundle),
//! else `sandbox-driver-<kind>` on `PATH`. Its SHA-256 must match the
//! target-matched pin compiled into this build, or
//! `PETRI_SANDBOX_<KIND>_SHA256` when that overrides it. An unpinned executable
//! launches only in dev mode — `PETRI_SANDBOX_PLUGIN_DEV=1`, or the CLI's
//! `--sandbox-plugin-dev` — which a debug build turns on by default so
//! development works before a first release. Dev mode is logged clearly when it
//! lets an unverified plugin run.
//!
//! The plugin starts from an empty environment. Only named variables are
//! forwarded: `PATH` and `HOME`, the Docker daemon selection and its TLS
//! companions, and Daytona's credentials. The values are captured with the
//! settings and reused at each launch. Credentials are not persisted or
//! included in the settings' debug output.
//!
//! # Supervision
//!
//! sandbox-driver's [`PluginSupervisor`] owns the plugin process. It probes
//! the backend's health before a generation serves, numbers generations in
//! launch order, and refuses a replacement that reports a different resource
//! namespace than the first. When the transport closes, every in-flight call
//! fails routably through the protocol client, and no call is ever replayed:
//! a mutating call whose outcome is unknown is reconciled by the lease
//! manager from durable records and provider labels, not by trying again.
//! The next provider call launches one new process, single-flight, with the
//! same kind, path, checksum, and environment, and hands back a new
//! generation; the lease manager fences every sandbox once before a holder
//! resumes on it.
//!
//! [`PluginSource`] is that supervisor as a [`ProviderSource`]: it launches
//! the supervisor on the first call, so a run that never needs the provider
//! never starts its process, and records the fingerprint the first
//! generation's health report confirms.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::{env, fmt};

use executor::EnvError;
use sandbox_driver::{ProviderHealth, ProviderKind, SandboxProvider};
use sandbox_driver_protocol::discovery::PluginConfig;
use sandbox_driver_protocol::{PluginGeneration, PluginSupervisor};
use tokio::sync::OnceCell;

use crate::registration::RegistrationError;
use crate::{DOCKER_HOST_ALIAS, fingerprint};

/// The naming prefix plugin discovery searches `PATH` for:
/// `sandbox-driver-<kind>`.
pub const PLUGIN_PREFIX: &str = "sandbox-driver";
/// Turns unpinned plugins on for every kind.
pub const DEV_MODE_VAR: &str = "PETRI_SANDBOX_PLUGIN_DEV";

// Generated from the release bundle's plugin binaries. Ordinary development
// builds have no pins and use the explicit override or development policy.
include!(concat!(env!("OUT_DIR"), "/plugin_pins.rs"));

/// What the plugin process inherits, per kind. Everything else is scrubbed.
fn forwarded_env(kind: &str) -> Vec<&'static str> {
    // `RUST_LOG` too: the plugin's stderr is the host's, and an operator
    // turning logging up expects to see the plugin's side of a call.
    let mut vars = vec!["PATH", "HOME", "RUST_LOG"];
    match kind {
        "host" => vars.extend([
            "USER",
            "SHELL",
            "LANG",
            "TERM",
            "TMPDIR",
            "GOPATH",
            "CARGO_HOME",
            "NVM_DIR",
        ]),
        "docker" => vars.extend([
            "DOCKER_HOST",
            "DOCKER_TLS_VERIFY",
            "DOCKER_CERT_PATH",
            "DOCKER_API_VERSION",
            "DOCKER_CONFIG",
        ]),
        "daytona" => vars.extend([
            "DAYTONA_API_KEY",
            "DAYTONA_JWT_TOKEN",
            "DAYTONA_ORGANIZATION_ID",
            "DAYTONA_API_URL",
            "DAYTONA_TARGET",
        ]),
        _ => {}
    }
    vars
}

/// Why a plugin could not be configured or launched.
#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    #[error("`{0}` is not a provider kind")]
    Kind(String),
    #[error(
        "the {kind} plugin has no pinned checksum for this build; set \
         PETRI_SANDBOX_{upper}_SHA256, or allow an unverified plugin with \
         PETRI_SANDBOX_PLUGIN_DEV=1 (--sandbox-plugin-dev)"
    )]
    Unpinned { kind: String, upper: String },
    #[error(
        "the docker daemon at `{docker_host}` is remote, so a sandbox cannot reach this \
         machine by inference; set PETRI_SANDBOX_DOCKER_HOST_ADDRESS to the host name or \
         address the daemon's containers can reach Petri at"
    )]
    RemoteDaemonNeedsHostAddress { docker_host: String },
    #[error("launching the {kind} plugin failed: {source}")]
    Launch {
        kind:   String,
        #[source]
        source: Box<sandbox_driver::Error>,
    },
    #[error("the {kind} plugin reports its backend {status}: {message}")]
    Unhealthy {
        kind:    String,
        status:  &'static str,
        message: String,
    },
}

impl PluginError {
    /// The routable form: every plugin failure surfaces at acquire.
    pub fn into_env_error(self) -> EnvError {
        EnvError::backend("sandbox", "plugin", self.to_string())
    }
}

/// Where a plugin is and how it is trusted, for one provider kind.
#[derive(Clone)]
pub struct PluginSettings {
    kind:         ProviderKind,
    path:         Option<PathBuf>,
    sha256:       Option<String>,
    dev:          bool,
    /// Arguments the executable is launched with. Only an explicit
    /// registration supplies any.
    args:         Vec<String>,
    host_address: Option<String>,
    env:          BTreeMap<String, String>,
    /// A non-secret description of the backend the plugin will drive, from
    /// the same environment it inherits: the effective daemon endpoint,
    /// account, and target. Recorded on every lease and checked before a
    /// recorded sandbox is touched again.
    fingerprint:  String,
}

impl fmt::Debug for PluginSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginSettings")
            .field("kind", &self.kind)
            .field("path", &self.path)
            .field("sha256", &self.sha256)
            .field("dev", &self.dev)
            .field("argument_count", &self.args.len())
            .field("host_address", &self.host_address)
            .field("env_keys", &self.env.keys())
            .field("fingerprint", &self.fingerprint)
            .finish()
    }
}

impl PluginSettings {
    /// The settings for `kind` from this process's environment: the
    /// resolution order, pin, dev flag, and host address documented at the
    /// module level. `dev_override` is the CLI flag; `None` reads the
    /// environment and the build profile.
    pub fn from_env(kind: &str, dev_override: Option<bool>) -> Result<Self, PluginError> {
        Self::from_lookup(kind, dev_override, |name| env::var_os(name))
    }

    fn from_lookup(
        kind: &str,
        dev_override: Option<bool>,
        read: impl Fn(&str) -> Option<OsString>,
    ) -> Result<Self, PluginError> {
        let text = |name: &str| read(name).and_then(|value| value.into_string().ok());
        let provider_kind =
            ProviderKind::try_new(kind).map_err(|_| PluginError::Kind(kind.to_owned()))?;
        let upper = kind.to_ascii_uppercase().replace('-', "_");
        let path = read(&format!("PETRI_SANDBOX_{upper}_PLUGIN")).map(PathBuf::from);
        let sha256 = text(&format!("PETRI_SANDBOX_{upper}_SHA256"))
            .filter(|value| !value.trim().is_empty())
            .or_else(|| pinned_sha256(kind).map(str::to_owned));
        let dev = dev_override.unwrap_or_else(|| {
            text(DEV_MODE_VAR).is_some_and(|value| !value.is_empty() && value != "0")
                || cfg!(debug_assertions)
        });
        let env = forwarded_env(kind)
            .into_iter()
            .filter_map(|name| text(name).map(|value| (name.to_owned(), value)))
            .collect();
        let host_address = text(&format!("PETRI_SANDBOX_{upper}_HOST_ADDRESS"))
            .filter(|value| !value.trim().is_empty());
        let fingerprint = fingerprint_for(kind, &env);
        Ok(Self {
            kind: provider_kind,
            path,
            sha256,
            dev,
            args: Vec::new(),
            host_address,
            env,
            fingerprint,
        })
    }

    /// Settings frozen as `config` gives them, for a registered plugin: an
    /// absolute executable, its arguments and complete environment, and a
    /// checksum or an explicit development opt-in. Nothing is read from this
    /// process's environment, and nothing is looked for on `PATH`.
    pub(crate) fn explicit(config: PluginConfig) -> Result<Self, RegistrationError> {
        let invalid = RegistrationError::Launch;
        let path = config
            .path
            .as_ref()
            .ok_or(invalid("an executable path is required"))?;
        if !path.is_absolute() {
            return Err(invalid("the executable path must be absolute"));
        }
        if !config.inherit_env.is_empty() {
            return Err(invalid(
                "the environment must be complete; nothing is inherited from this process",
            ));
        }
        if config.sha256.is_none() && !config.dev {
            return Err(invalid(
                "a sha256 checksum is required unless development mode is explicit",
            ));
        }
        let fingerprint = fingerprint_for(config.kind.as_str(), &config.env);
        Ok(Self {
            kind: config.kind,
            path: config.path,
            sha256: config.sha256,
            dev: config.dev,
            args: config.args,
            host_address: None,
            env: config.env,
            fingerprint,
        })
    }

    /// Settings for a plugin at an explicit path, trusted as given: tests
    /// and hosts that ship their own plugin.
    pub fn at_path(kind: &str, path: impl Into<PathBuf>) -> Result<Self, PluginError> {
        let mut settings = Self::from_env(kind, Some(true))?;
        settings.path = Some(path.into());
        settings
            .env
            .insert("RUST_LOG".to_owned(), "warn".to_owned());
        Ok(settings)
    }

    /// Sets the run-owned Host registry and includes it in recovery identity.
    #[must_use]
    pub fn with_host_registry(mut self, directory: &Path) -> Self {
        self.env.insert(
            "SANDBOX_DRIVER_HOST_REGISTRY".to_owned(),
            directory.to_string_lossy().into_owned(),
        );
        self.fingerprint = fingerprint_for(self.kind.as_str(), &self.env);
        self
    }

    pub fn kind(&self) -> &ProviderKind {
        &self.kind
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    fn effective_fingerprint(&self, health: &ProviderHealth) -> Result<String, PluginError> {
        fingerprint::verified(self.kind.as_str(), &self.fingerprint, health).map_err(|error| {
            PluginError::Unhealthy {
                kind:    self.kind.to_string(),
                status:  "without a verified resource identity",
                message: error.to_string(),
            }
        })
    }

    /// The host name or address a sandbox of this provider uses to reach
    /// services Petri runs on this machine. A local Docker daemon has a
    /// known alias; a remote one needs the operator to say.
    pub fn host_address(&self) -> Result<Option<String>, PluginError> {
        infer_host_address(
            self.kind.as_str(),
            self.host_address.as_deref(),
            self.env.get("DOCKER_HOST").map_or("", String::as_str),
        )
    }

    /// Host action workspaces can only be mounted by a local Docker daemon.
    pub(crate) fn supports_host_workspace(&self) -> bool {
        self.kind.as_str() == "docker"
            && docker_host_is_local(self.env.get("DOCKER_HOST").map_or("", String::as_str))
    }

    /// The executable, in resolution order: explicit path, the release
    /// bundle sibling, `PATH`.
    fn config(&self) -> Result<PluginConfig, PluginError> {
        let mut config = PluginConfig::new(self.kind.clone());
        if let Some(path) = &self.path {
            config.path = Some(path.clone());
        } else if let Some(sibling) = bundled_sibling(self.kind.as_str()) {
            config.path = Some(sibling);
        }
        config.sha256.clone_from(&self.sha256);
        if config.sha256.is_none() {
            if !self.dev {
                return Err(PluginError::Unpinned {
                    kind:  self.kind.to_string(),
                    upper: self.kind.as_str().to_ascii_uppercase().replace('-', "_"),
                });
            }
            config.dev = true;
        }
        config.env.clone_from(&self.env);
        config.args.clone_from(&self.args);
        Ok(config)
    }
}

fn pinned_sha256(kind: &str) -> Option<&'static str> {
    let target = env!("PETRI_TARGET_TRIPLE");
    PINNED_PLUGINS
        .iter()
        .find(|(pinned_kind, pinned_target, _)| *pinned_kind == kind && *pinned_target == target)
        .map(|(_, _, sha256)| *sha256)
}

/// `sandbox-driver-<kind>` beside this executable, when it exists.
fn bundled_sibling(kind: &str) -> Option<PathBuf> {
    let exe = env::current_exe().ok()?;
    let sibling = exe.parent()?.join(format!("{PLUGIN_PREFIX}-{kind}"));
    sibling.is_file().then_some(sibling)
}

/// The host address for `kind`: the configured one when given; else, for
/// Docker, the local alias when `docker_host` names a daemon on this
/// machine, and a refusal to guess when it does not.
pub(crate) fn infer_host_address(
    kind: &str,
    configured: Option<&str>,
    docker_host: &str,
) -> Result<Option<String>, PluginError> {
    if let Some(address) = configured {
        return Ok(Some(address.to_owned()));
    }
    match kind {
        "host" => Ok(Some("127.0.0.1".to_owned())),
        "daytona" => Ok(None),
        "docker" if docker_host_is_local(docker_host) => Ok(Some(DOCKER_HOST_ALIAS.to_owned())),
        "docker" => Err(PluginError::RemoteDaemonNeedsHostAddress {
            docker_host: docker_host.trim().to_owned(),
        }),
        other => Err(PluginError::RemoteDaemonNeedsHostAddress {
            docker_host: format!("<{other} backend>"),
        }),
    }
}

/// Whether `DOCKER_HOST` names a daemon on this machine: unset, a Unix
/// socket, or a named pipe.
pub(crate) fn docker_host_is_local(docker_host: &str) -> bool {
    let value = docker_host.trim();
    value.is_empty() || value.starts_with("unix://") || value.starts_with("npipe://")
}

/// The non-secret backend identity for `kind`, from the environment the
/// plugin inherits: the same seed an in-process provider records.
fn fingerprint_for(kind: &str, env: &BTreeMap<String, String>) -> String {
    let value = |name: &str| env.get(name).map(String::as_str);
    match kind {
        "host" => fingerprint::host(Path::new(
            value("SANDBOX_DRIVER_HOST_REGISTRY").unwrap_or_default(),
        )),
        "docker" => fingerprint::docker(value("DOCKER_HOST")),
        "daytona" => fingerprint::daytona(
            value("DAYTONA_API_URL"),
            value("DAYTONA_ORGANIZATION_ID"),
            value("DAYTONA_TARGET"),
        ),
        other => other.to_owned(),
    }
}

/// A supervised plugin as a manager's provider source: sandbox-driver's
/// [`PluginSupervisor`] under this kind's [`PluginSettings`], launched on
/// the first call.
pub struct PluginSource {
    settings:    PluginSettings,
    supervisor:  OnceCell<PluginSupervisor>,
    /// Filled once the first generation verifies its resource namespace.
    /// The supervisor refuses a later generation that names another.
    fingerprint: OnceLock<String>,
}

impl PluginSource {
    pub fn new(settings: PluginSettings) -> Self {
        Self {
            settings,
            supervisor: OnceCell::new(),
            fingerprint: OnceLock::new(),
        }
    }

    pub fn settings(&self) -> &PluginSettings {
        &self.settings
    }

    pub fn kind(&self) -> &ProviderKind {
        &self.settings.kind
    }

    /// The live generation, launching the plugin on first use and again
    /// after its transport closed. Single-flight: concurrent callers share
    /// one launch, and the next call retries a failed one.
    pub async fn current(&self) -> Result<Arc<PluginGeneration>, PluginError> {
        let supervisor = self.supervisor.get_or_try_init(|| self.launch()).await?;
        supervisor
            .current()
            .await
            .map_err(|source| self.launch_failed(source))
    }

    /// Launches the supervisor and its first generation, and verifies the
    /// resource namespace the fingerprint records before any lease uses it.
    async fn launch(&self) -> Result<PluginSupervisor, PluginError> {
        let config = self.settings.config()?;
        let supervisor = PluginSupervisor::launch(PLUGIN_PREFIX, config)
            .await
            .map_err(|source| self.launch_failed(source))?;
        let first = supervisor
            .current()
            .await
            .map_err(|source| self.launch_failed(source))?;
        match self.settings.effective_fingerprint(first.health()) {
            Ok(fingerprint) => {
                let _ = self.fingerprint.set(fingerprint);
                Ok(supervisor)
            }
            Err(error) => {
                let _ = supervisor.shutdown().await;
                Err(error)
            }
        }
    }

    fn launch_failed(&self, source: sandbox_driver::Error) -> PluginError {
        PluginError::Launch {
            kind:   self.settings.kind.to_string(),
            source: Box::new(source),
        }
    }

    /// Asks the live plugin, if any, to exit.
    pub async fn shutdown(&self) {
        if let Some(supervisor) = self.supervisor.get()
            && let Err(error) = supervisor.shutdown().await
        {
            tracing::debug!(error = %error, "sandbox plugin shutdown failed");
        }
    }
}

/// Where a manager gets its provider from: a supervised plugin in
/// production, a fixed provider in tests.
#[async_trait::async_trait]
pub trait ProviderSource: Send + Sync {
    /// The provider to use now and the generation it belongs to. A
    /// generation change means every handle from before it is dead.
    async fn current(&self) -> Result<(Arc<dyn SandboxProvider>, u64), EnvError>;

    /// Stop and await the process this source owns, after all users finish.
    /// Sources backed by a caller-owned provider have no process to stop.
    async fn shutdown(&self) {}

    /// Whether [`ProviderSource::shutdown`] itself ends the work admitted
    /// on this source, as a plugin's closing transport does. When it does
    /// not, the router waits for admitted work before shutting down.
    fn ends_work_on_shutdown(&self) -> bool {
        false
    }

    /// The non-secret backend fingerprint every lease records. Call
    /// `current` first, so a plugin can verify its effective namespace.
    fn fingerprint(&self) -> &str;

    /// The provider kind, as recorded on leases.
    fn kind(&self) -> &str;

    /// The configured region for sandbox and snapshot placement.
    fn region(&self) -> Option<&str> {
        None
    }
}

#[async_trait::async_trait]
impl ProviderSource for PluginSource {
    async fn current(&self) -> Result<(Arc<dyn SandboxProvider>, u64), EnvError> {
        let generation = self.current().await.map_err(PluginError::into_env_error)?;
        let provider: Arc<dyn SandboxProvider> =
            Arc::clone(generation.provider()) as Arc<dyn SandboxProvider>;
        Ok((provider, generation.number()))
    }

    async fn shutdown(&self) {
        Self::shutdown(self).await;
    }

    fn ends_work_on_shutdown(&self) -> bool {
        true
    }

    fn fingerprint(&self) -> &str {
        self.fingerprint
            .get()
            .map_or_else(|| self.settings.fingerprint(), String::as_str)
    }

    fn kind(&self) -> &str {
        self.settings.kind.as_str()
    }

    fn region(&self) -> Option<&str> {
        self.settings
            .env
            .get("DAYTONA_TARGET")
            .map(String::as_str)
            .filter(|region| !region.is_empty())
    }
}

/// A provider handed in directly, with one fixed generation.
pub struct FixedProvider {
    provider:    Arc<dyn SandboxProvider>,
    kind:        String,
    fingerprint: String,
}

impl FixedProvider {
    pub fn new(provider: Arc<dyn SandboxProvider>) -> Self {
        let kind = provider.kind().to_string();
        Self {
            provider,
            fingerprint: format!("{kind}:fixed"),
            kind,
        }
    }
}

#[async_trait::async_trait]
impl ProviderSource for FixedProvider {
    async fn current(&self) -> Result<(Arc<dyn SandboxProvider>, u64), EnvError> {
        Ok((Arc::clone(&self.provider), 1))
    }

    fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    fn kind(&self) -> &str {
        &self.kind
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use sandbox_driver::HealthStatus;

    use super::*;

    #[test]
    fn the_source_region_uses_the_environment_captured_for_its_plugin() {
        let settings = PluginSettings::from_lookup("daytona", Some(true), |name| {
            (name == "DAYTONA_TARGET").then(|| "us-central-1".into())
        })
        .unwrap();
        let source = PluginSource::new(settings);
        assert_eq!(source.region(), Some("us-central-1"));
        let settings = PluginSettings::from_lookup("daytona", Some(true), |_| None).unwrap();
        assert_eq!(PluginSource::new(settings).region(), None);
    }

    #[test]
    fn launch_failures_keep_the_cause_in_the_acquire_diagnostic() {
        let error = PluginError::Launch {
            kind:   "host".to_owned(),
            source: Box::new(sandbox_driver::Error::invalid_spec(
                "sha256",
                "checksum mismatch",
            )),
        }
        .into_env_error();
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn docker_fingerprints_distinguish_daemon_endpoints() {
        let endpoints = [
            "",
            "unix:///var/run/docker.sock",
            "unix:///run/user/1000/docker.sock",
            "npipe:////./pipe/docker_engine",
            "npipe:////./pipe/another_engine",
            "tcp://10.0.0.5:2376",
        ];
        let fingerprints: BTreeSet<_> = endpoints
            .into_iter()
            .map(|endpoint| {
                fingerprint_for(
                    "docker",
                    &BTreeMap::from([("DOCKER_HOST".to_owned(), endpoint.to_owned())]),
                )
            })
            .collect();
        assert_eq!(fingerprints.len(), endpoints.len());
    }

    #[test]
    fn daytona_key_rotation_preserves_identity_but_an_account_change_does_not() {
        let settings_for = |key: &str| {
            PluginSettings::from_lookup("daytona", Some(true), |name| {
                (name == "DAYTONA_API_KEY").then(|| OsString::from(key))
            })
            .expect("API-key-only settings")
        };
        let mut health = ProviderHealth::new(HealthStatus::Ok);
        health.identity = Some("organization:first".to_owned());
        let original = settings_for("original-private-key")
            .effective_fingerprint(&health)
            .expect("verified organization");
        let rotated = settings_for("rotated-private-key")
            .effective_fingerprint(&health)
            .expect("rotated key in the same organization");
        assert_eq!(original, rotated);
        assert!(!original.contains("private-key"));
        health.identity = Some("organization:second".to_owned());
        let other = settings_for("other-private-key")
            .effective_fingerprint(&health)
            .expect("verified other organization");
        assert_ne!(original, other);
    }

    #[test]
    fn daytona_requires_verified_identity_before_recovery() {
        let settings =
            PluginSettings::from_lookup("daytona", Some(true), |_| None).expect("settings");
        assert!(
            settings
                .effective_fingerprint(&ProviderHealth::new(HealthStatus::Ok))
                .is_err()
        );
    }

    #[test]
    fn settings_keep_the_environment_used_for_their_fingerprint() {
        let original = "tcp://10.0.0.5:2376";
        let mut environment = BTreeMap::from([
            ("DOCKER_HOST", OsString::from(original)),
            ("UNRELATED_SECRET", OsString::from("do not forward")),
        ]);
        let settings = PluginSettings::from_lookup("docker", Some(true), |name| {
            environment.get(name).cloned()
        })
        .expect("settings");
        environment.insert("DOCKER_HOST", OsString::from("unix:///another.sock"));

        let config = settings.config().expect("dev configuration");
        assert_eq!(
            config.env.get("DOCKER_HOST").map(String::as_str),
            Some(original)
        );
        assert!(!config.env.contains_key("UNRELATED_SECRET"));
        assert!(config.inherit_env.is_empty());
        assert_eq!(settings.fingerprint(), format!("docker:{original}"));
        assert!(matches!(
            settings.host_address(),
            Err(PluginError::RemoteDaemonNeedsHostAddress { docker_host }) if docker_host == original
        ));
    }

    #[test]
    fn settings_debug_does_not_expose_forwarded_credentials() {
        let secret = "private-daytona-token";
        let settings = PluginSettings::from_lookup("daytona", Some(true), |name| {
            (name == "DAYTONA_API_KEY").then(|| OsString::from(secret))
        })
        .expect("settings");
        assert_eq!(
            settings
                .config()
                .expect("dev configuration")
                .env
                .get("DAYTONA_API_KEY")
                .map(String::as_str),
            Some(secret)
        );
        let debug = format!("{settings:?}");
        assert!(debug.contains("DAYTONA_API_KEY"));
        assert!(!debug.contains(secret));
    }

    #[test]
    fn a_local_docker_host_is_recognized() {
        assert!(docker_host_is_local(""));
        assert!(docker_host_is_local("unix:///var/run/docker.sock"));
        assert!(docker_host_is_local("npipe:////./pipe/docker_engine"));
        assert!(!docker_host_is_local("tcp://10.0.0.5:2376"));
        assert!(!docker_host_is_local("ssh://build@remote"));
    }

    #[test]
    fn a_local_daemon_infers_the_docker_alias() {
        for local in [
            "",
            "unix:///var/run/docker.sock",
            "npipe:////./pipe/docker_engine",
        ] {
            let address = infer_host_address("docker", None, local).expect("inferred");
            assert_eq!(address.as_deref(), Some(DOCKER_HOST_ALIAS), "for `{local}`");
        }
    }

    #[test]
    fn a_remote_daemon_needs_an_explicit_address() {
        let error = infer_host_address("docker", None, "tcp://10.0.0.5:2376")
            .expect_err("no inference for a remote daemon");
        assert!(
            matches!(&error, PluginError::RemoteDaemonNeedsHostAddress { docker_host } if docker_host == "tcp://10.0.0.5:2376"),
            "{error}"
        );
        assert!(
            error
                .to_string()
                .contains("PETRI_SANDBOX_DOCKER_HOST_ADDRESS"),
            "the error names the setting: {error}"
        );
    }

    #[test]
    fn a_configured_address_wins_everywhere() {
        let address = infer_host_address("docker", Some("petri.internal"), "tcp://10.0.0.5:2376")
            .expect("configured");
        assert_eq!(address.as_deref(), Some("petri.internal"));
        let address = infer_host_address("daytona", Some("203.0.113.7"), "").expect("configured");
        assert_eq!(address.as_deref(), Some("203.0.113.7"));
    }

    #[test]
    fn a_remote_only_provider_never_guesses() {
        assert_eq!(infer_host_address("daytona", None, "").unwrap(), None);
    }

    /// Leases already record these exact strings; an in-process provider
    /// must produce the same ones from the same inputs to recover them.
    #[test]
    fn plugin_fingerprints_keep_their_recorded_form() {
        let env = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect::<BTreeMap<_, _>>()
        };
        let cases = [
            ("host", env(&[]), "host:"),
            (
                "host",
                env(&[("SANDBOX_DRIVER_HOST_REGISTRY", "/runs/a/host-registry")]),
                "host:/runs/a/host-registry",
            ),
            ("docker", env(&[]), "docker:default"),
            ("docker", env(&[("DOCKER_HOST", "  ")]), "docker:default"),
            (
                "docker",
                env(&[("DOCKER_HOST", " tcp://10.0.0.5:2376 ")]),
                "docker:tcp://10.0.0.5:2376",
            ),
            ("daytona", env(&[]), "daytona:::"),
            (
                "daytona",
                env(&[
                    ("DAYTONA_API_URL", "https://app.daytona.io/api"),
                    ("DAYTONA_ORGANIZATION_ID", "org-1"),
                    ("DAYTONA_TARGET", "us"),
                    ("DAYTONA_API_KEY", "private"),
                ]),
                "daytona:https://app.daytona.io/api:org-1:us",
            ),
        ];
        for (kind, env, expected) in cases {
            assert_eq!(fingerprint_for(kind, &env), expected, "{kind} {env:?}");
        }
        assert_eq!(
            fingerprint::host(Path::new("/runs/a/host-registry")),
            "host:/runs/a/host-registry"
        );
        assert_eq!(fingerprint::docker(None), "docker:default");
        assert_eq!(
            fingerprint::daytona(
                Some("https://app.daytona.io/api"),
                Some("org-1"),
                Some("us")
            ),
            "daytona:https://app.daytona.io/api:org-1:us"
        );
    }
}
