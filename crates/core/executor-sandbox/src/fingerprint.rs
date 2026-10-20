//! The non-secret backend identity every lease records.
//!
//! A fingerprint names the resource namespace a provider drives: the Host
//! registry, the Docker daemon endpoint, or the Daytona account and target.
//! It is recorded on every lease and checked before a recorded sandbox is
//! touched again, so a resume or prune against a different backend refuses
//! instead of acting on the wrong resources.
//!
//! The seed is the same whichever way the provider is reached, a plugin or
//! an in-process factory, so a lease recorded one way is recoverable the
//! other. It is built from the settings as configured, not as the provider
//! resolves them: an unset value stays unset (`None`) rather than becoming
//! the default the provider falls back to, because that is what a plugin
//! records. The first health report's identity, when the provider gives
//! one, is appended to the seed ([`verified`]).

use std::path::Path;

use sandbox_driver::ProviderHealth;

/// `host:<registry>`: the canonical Host registry directory.
pub fn host(registry: &Path) -> String {
    format!("host:{}", registry.to_string_lossy())
}

/// `docker:<endpoint>`: the configured `DOCKER_HOST`, or `default` when
/// none is configured. The local socket's path is never passed for `None`.
pub fn docker(docker_host: Option<&str>) -> String {
    let endpoint = match docker_host.map(str::trim) {
        None | Some("") => "default",
        Some(configured) => configured,
    };
    format!("docker:{endpoint}")
}

/// `daytona:<api url>:<organization>:<target>`, each as configured and
/// empty when unset. An unset API URL is `None`, not the Daytona default.
pub fn daytona(
    api_url: Option<&str>,
    organization_id: Option<&str>,
    target: Option<&str>,
) -> String {
    format!(
        "daytona:{}:{}:{}",
        api_url.unwrap_or_default(),
        organization_id.unwrap_or_default(),
        target.unwrap_or_default()
    )
}

/// Why a provider's health report cannot confirm its namespace.
#[derive(Debug, thiserror::Error)]
#[error(
    "the {kind} provider reports no verified resource identity; its resource namespace \
     must be known before sandbox resources can be created or recovered"
)]
pub struct MissingIdentity {
    kind: String,
}

/// The fingerprint a provider serves under: `seed`, plus `:<identity>` when
/// its health report names one. Host and Docker need none: their seeds
/// name the registry directory and the daemon endpoint. Every other kind
/// must name one, because its seed alone cannot tell two organizations
/// behind the same key, or two hosts behind the same plugin, apart.
pub fn verified(
    kind: &str,
    seed: &str,
    health: &ProviderHealth,
) -> Result<String, MissingIdentity> {
    let identity = health
        .identity
        .as_deref()
        .filter(|identity| !identity.trim().is_empty());
    if identity.is_none() && !matches!(kind, "host" | "docker") {
        return Err(MissingIdentity {
            kind: kind.to_owned(),
        });
    }
    Ok(match identity {
        Some(identity) => format!("{seed}:{identity}"),
        None => seed.to_owned(),
    })
}
