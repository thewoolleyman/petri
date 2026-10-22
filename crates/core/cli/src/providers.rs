//! `--sandbox-providers <file>`: the plugins a run, resume, or prune may
//! reach under custom provider kinds, beside the first-party ones.
//!
//! ```toml
//! [providers.lima]
//! executable = "./sandbox-driver-lima"   # relative to this file
//! args = ["--socket", "/tmp/lima.sock"]
//! sha256 = "…"                           # or `dev = true`
//!
//! [providers.lima.env]                   # the plugin's whole environment
//! PATH = "/usr/bin:/bin"
//!
//! [providers.lima.process_spec]          # sandbox-driver's `SandboxSpec`
//! source = "host_directory"
//! ```
//!
//! Nothing in the file reaches the workflow graph or the run's records; a
//! resume or prune is given the file again.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf, absolute};

use runtime::{ProviderRegistrations, Runtime};
use sandbox_driver::{ProviderKind, SandboxSpec};
use sandbox_driver_protocol::PluginConfig;
use serde::Deserialize;
use tokio::fs;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderFile {
    providers: BTreeMap<String, ProviderEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderEntry {
    executable:   PathBuf,
    #[serde(default)]
    args:         Vec<String>,
    #[serde(default)]
    env:          BTreeMap<String, String>,
    sha256:       Option<String>,
    #[serde(default)]
    dev:          bool,
    process_spec: toml::Value,
}

impl ProviderEntry {
    /// The entry as a registration: its executable resolved against
    /// `base`, the provider file's directory, and its template checked for
    /// keys the sandbox specification does not have.
    fn register(
        self,
        kind: &str,
        base: &Path,
        providers: &mut ProviderRegistrations,
    ) -> Result<(), String> {
        let kind =
            ProviderKind::try_new(kind).map_err(|_| format!("`{kind}` is not a provider kind"))?;
        if self.executable.as_os_str().is_empty() {
            return Err(format!("provider `{kind}`: the executable path is empty"));
        }
        let executable = absolute(base.join(&self.executable))
            .map_err(|_| format!("provider `{kind}`: the executable path cannot be resolved"))?;
        let mut config = PluginConfig::new(kind.clone());
        config.path = Some(executable);
        config.args = self.args;
        config.env = self.env;
        config.sha256 = self.sha256;
        config.dev = self.dev;
        let spec = process_spec(&kind, &self.process_spec)?;
        providers
            .register_plugin(config, spec)
            .map_err(|error| format!("provider `{kind}`: {error}"))
    }
}

/// The template as a `SandboxSpec`. The specification accepts unknown keys
/// silently, so a misspelt one would be ignored: the parsed template is
/// serialized back and every key of the input must have survived.
fn process_spec(kind: &ProviderKind, value: &toml::Value) -> Result<SandboxSpec, String> {
    let given = serde_json::to_value(value)
        .map_err(|_| format!("provider `{kind}`: process_spec is not a table"))?;
    let keys: Vec<String> = given
        .as_object()
        .ok_or_else(|| format!("provider `{kind}`: process_spec must be a table"))?
        .keys()
        .cloned()
        .collect();
    let spec: SandboxSpec = serde_json::from_value(given).map_err(|_| {
        format!(
            "provider `{kind}`: process_spec is not a sandbox specification; check its field \
             types against sandbox-driver's SandboxSpec"
        )
    })?;
    let kept = serde_json::to_value(&spec)
        .map_err(|_| format!("provider `{kind}`: process_spec could not be checked"))?;
    let kept = kept.as_object();
    let unknown: Vec<String> = keys
        .into_iter()
        .filter(|key| !kept.is_some_and(|kept| kept.contains_key(key)))
        .collect();
    if !unknown.is_empty() {
        return Err(format!(
            "provider `{kind}`: process_spec has no field named {}",
            unknown.join(", ")
        ));
    }
    Ok(spec)
}

/// The runtime with the file's providers registered beside standalone
/// Petri's first-party plugins.
pub(crate) async fn configure(runtime: Runtime, path: &Path) -> Result<Runtime, String> {
    let text = fs::read_to_string(path)
        .await
        .map_err(|error| format!("reading {}: {error}", path.display()))?;
    // The parser's message can quote the input, which can hold
    // credentials: only the location is reported.
    let file: ProviderFile = toml::from_str(&text).map_err(|error| {
        let location = error
            .span()
            .map_or_else(String::new, |span| format!(" near byte {}", span.start));
        format!(
            "{} is not a valid provider file{location}; check its field names and value types",
            path.display()
        )
    })?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let mut providers = ProviderRegistrations::standalone();
    for (kind, entry) in file.providers {
        entry.register(&kind, base, &mut providers)?;
    }
    Ok(runtime.sandbox_providers(providers))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(text: &str) -> ProviderEntry {
        toml::from_str(text).expect("a provider entry")
    }

    #[test]
    fn a_relative_executable_is_resolved_against_the_file_without_opening_it() {
        let mut providers = ProviderRegistrations::new();
        entry(
            "executable = 'not-installed'\ndev = true\n[process_spec]\nsource = 'host_directory'\n",
        )
        .register("lima", Path::new("/providers"), &mut providers)
        .expect("registers");
        assert!(format!("{providers:?}").contains("lima"));
    }

    #[test]
    fn a_misspelt_template_field_is_named_instead_of_ignored() {
        let mut providers = ProviderRegistrations::new();
        let error = entry(
            "executable = '/opt/lima'\ndev = true\n[process_spec]\nsource = 'host_directory'\nworking_dir = '/work'\n",
        )
        .register("lima", Path::new("/providers"), &mut providers)
        .expect_err("an unknown field");
        assert!(error.contains("working_dir"), "{error}");
        assert!(!error.contains("/work"), "{error}");
    }

    #[test]
    fn an_unknown_entry_field_and_a_bad_kind_are_refused() {
        assert!(
            toml::from_str::<ProviderEntry>("executable = '/opt/lima'\nnamespace = 'x'\n[process_spec]\nsource = 'host_directory'\n").is_err()
        );
        let mut providers = ProviderRegistrations::new();
        assert!(
            entry(
                "executable = '/opt/lima'\ndev = true\n[process_spec]\nsource = 'host_directory'\n"
            )
            .register("Lima", Path::new("/"), &mut providers)
            .is_err()
        );
    }
}
