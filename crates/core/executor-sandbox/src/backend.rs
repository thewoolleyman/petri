//! Run-level placement. Workflow targets describe a process or container;
//! these options choose the system that provides it.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use executor::EnvError;
use ir::RuntimeSpec;
use sandbox_driver::{NetworkPolicy, ProviderKind, Resources, SandboxKind};

use crate::registration::selectable_kind;

const RUNNER_PIN: &str = "f8bbbfd81934";
const DEFAULT_LABEL: &str = "ubuntu-24.04";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SandboxBackend {
    /// Local processes, with Docker for explicit container targets.
    #[default]
    Host,
    /// Docker runner images for process targets and job images for containers.
    Docker,
    /// A Daytona sandbox, with a nested job container when requested.
    Daytona,
    /// Ordinary process scopes on a provider registered under this kind
    /// (`ProviderRegistrations`). Any valid kind but a reserved name parses
    /// to this arm; whether it is registered is decided at acquire.
    Registered(ProviderKind),
}

impl fmt::Display for SandboxBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Host => "host",
            Self::Docker => "docker",
            Self::Daytona => "daytona",
            Self::Registered(kind) => kind.as_str(),
        })
    }
}

impl FromStr for SandboxBackend {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "host" => Ok(Self::Host),
            "docker" => Ok(Self::Docker),
            "daytona" => Ok(Self::Daytona),
            _ => selectable_kind(value)
                .map(Self::Registered)
                .map_err(|error| error.to_string()),
        }
    }
}

/// What an acquire does when a lease's recorded sandbox is gone from its
/// provider: the container was removed, the remote sandbox deleted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LostSandbox {
    /// Fail the acquire. The workspace was lost with the sandbox, and
    /// Petri does not replace it silently.
    #[default]
    Refuse,
    /// Create a fresh sandbox under the lease, with an empty workspace, for
    /// a host that restores the workspace itself before any attempt runs
    /// in it (`ExecutionHooks::scope_acquired`).
    Replace,
}

/// Configuration for the built-in sandbox router. Credentials come from
/// provider environment variables and never belong in this structure.
#[derive(Clone, Debug, Default)]
pub struct SandboxOptions {
    pub backend:           SandboxBackend,
    /// Network policy for every sandbox created by this run. Providers reject
    /// policies they cannot enforce. Blocked runs also verify attached
    /// sandboxes.
    pub network:           NetworkPolicy,
    /// What to do when a lease's recorded sandbox is gone from the provider.
    pub lost_sandbox:      LostSandbox,
    /// Label-to-image overrides. Daytona images must include Docker,
    /// `start-docker`, and Python 3. Empty requirements use `ubuntu-24.04`.
    pub runner_images:     BTreeMap<String, String>,
    pub daytona_resources: DaytonaResources,
    /// The outer Daytona sandbox. Workflow `container:` selects a nested job.
    pub daytona_kind:      DaytonaSandboxKind,
    /// `None` leaves development mode to the environment and build profile.
    pub plugin_dev:        Option<bool>,
}

/// The Daytona offering that hosts a workflow runner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DaytonaSandboxKind {
    #[default]
    Container,
    VirtualMachine,
}

impl DaytonaSandboxKind {
    pub(crate) fn sandbox_kind(self) -> SandboxKind {
        match self {
            Self::Container => SandboxKind::Container,
            Self::VirtualMachine => SandboxKind::VirtualMachine,
        }
    }
}

impl FromStr for DaytonaSandboxKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "container" => Ok(Self::Container),
            "vm" => Ok(Self::VirtualMachine),
            _ => Err(format!(
                "unknown Daytona sandbox kind {value:?}; expected container or vm"
            )),
        }
    }
}

/// Snapshot allocation. Include these values in the snapshot identity:
/// Daytona fixes a sandbox's resources when its snapshot is created.
#[derive(Clone, Copy, Debug)]
pub struct DaytonaResources {
    pub cpu_cores: u32,
    /// Requested memory in MiB. The driver's effective allocation must
    /// meet the runner minimum; Daytona rounds up to whole GiB.
    pub memory_mb: u64,
    /// `None` lets Daytona choose the disk allocation when building the
    /// snapshot. Explicit allocations must be positive; Daytona validates
    /// whether the image fits and the account allows the requested size.
    pub disk_mb:   Option<u64>,
}

impl Default for DaytonaResources {
    fn default() -> Self {
        Self {
            cpu_cores: 2,
            memory_mb: 4096,
            disk_mb:   None,
        }
    }
}

impl DaytonaResources {
    pub(crate) fn validated(self) -> Result<Resources, EnvError> {
        let memory_mb = sandbox_driver_daytona_config::allocation_mib(self.memory_mb);
        if self.cpu_cores < 2 || memory_mb < 4096 || self.disk_mb == Some(0) {
            return Err(EnvError::backend(
                "daytona",
                "configure",
                "the Daytona runner needs at least 2 CPUs and 4096 MiB of memory; an explicit disk allocation must be positive",
            ));
        }
        let mut resources = Resources::default();
        resources.cpu_cores = Some(self.cpu_cores);
        // Snapshot identity, creation and status validation all use the
        // same effective allocation that the provider sends to Daytona.
        resources.memory_mb = Some(memory_mb);
        resources.disk_mb = self
            .disk_mb
            .map(sandbox_driver_daytona_config::allocation_mib);
        Ok(resources)
    }
}

impl SandboxOptions {
    pub(crate) fn runner_image(&self, runtime: &RuntimeSpec) -> Result<String, EnvError> {
        let mut selected = None;
        let labels = runtime
            .requirements
            .iter()
            .map(smol_str::SmolStr::as_str)
            .chain(runtime.requirements.is_empty().then_some(DEFAULT_LABEL));
        for label in labels {
            let image = self.runner_images.get(label).cloned().or_else(|| self.default_image(label))
                .filter(|image| !image.trim().is_empty())
                .ok_or_else(|| EnvError::backend(&self.backend.to_string(), "configure",
                    format!("no runner image for label {label:?}; configure --runner-image {label}=IMAGE")))?;
            if selected.as_ref().is_some_and(|previous| previous != &image) {
                return Err(EnvError::backend(
                    &self.backend.to_string(),
                    "configure",
                    "placement labels select different runner images",
                ));
            }
            selected = Some(image);
        }
        Ok(selected.expect("the default supplies a label for empty requirements"))
    }

    fn default_image(&self, label: &str) -> Option<String> {
        let version = match label {
            "ubuntu-latest" | "ubuntu-24.04" => "24.04",
            "ubuntu-22.04" if self.backend != SandboxBackend::Daytona => "22.04",
            "ubuntu-26.04" if self.backend != SandboxBackend::Daytona => "26.04",
            _ => return None,
        };
        let flavor = if self.backend == SandboxBackend::Daytona {
            "dind"
        } else {
            "slim"
        };
        Some(format!(
            "ghcr.io/lithoscomputer/ubuntu-{version}:{flavor}-{RUNNER_PIN}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use ir::RuntimeTarget;

    use super::*;

    fn runtime(labels: &[&str]) -> RuntimeSpec {
        RuntimeSpec {
            target:       RuntimeTarget::HostProcess,
            requirements: labels.iter().map(|label| (*label).into()).collect(),
        }
    }

    #[test]
    fn any_valid_kind_but_a_reserved_name_selects_a_registered_provider() {
        let backend: SandboxBackend = "fixture-provider".parse().expect("a valid kind");
        assert_eq!(backend.to_string(), "fixture-provider");
        assert_eq!("host".parse::<SandboxBackend>(), Ok(SandboxBackend::Host));
        assert!("../plugin".parse::<SandboxBackend>().is_err());
        assert!("local".parse::<SandboxBackend>().is_err());
        assert!("simulated".parse::<SandboxBackend>().is_err());
    }

    #[test]
    fn daytona_uses_only_available_pinned_dind_images_unless_overridden() {
        let mut options = SandboxOptions {
            backend: SandboxBackend::Daytona,
            ..Default::default()
        };
        assert_eq!(
            options.runner_image(&runtime(&[])).unwrap(),
            format!("ghcr.io/lithoscomputer/ubuntu-24.04:dind-{RUNNER_PIN}")
        );
        assert!(options.runner_image(&runtime(&["ubuntu-22.04"])).is_err());
        options.runner_images.insert(
            "ubuntu-22.04".to_owned(),
            "custom:22-dind-pinned".to_owned(),
        );
        assert_eq!(
            options.runner_image(&runtime(&["ubuntu-22.04"])).unwrap(),
            "custom:22-dind-pinned"
        );
        assert!(
            options
                .runner_image(&runtime(&["ubuntu-22.04", "ubuntu-24.04"]))
                .is_err()
        );
    }

    #[test]
    fn docker_maps_supported_labels_and_rejects_unknown_placement() {
        let options = SandboxOptions {
            backend: SandboxBackend::Docker,
            ..Default::default()
        };
        assert_eq!(
            options.runner_image(&runtime(&["ubuntu-22.04"])).unwrap(),
            format!("ghcr.io/lithoscomputer/ubuntu-22.04:slim-{RUNNER_PIN}")
        );
        assert!(options.runner_image(&runtime(&["custom-runner"])).is_err());
    }

    #[test]
    fn daytona_rejects_resources_below_the_dind_minimum() {
        assert!(DaytonaResources::default().validated().is_ok());
        assert!(
            DaytonaResources {
                cpu_cores: 1,
                ..Default::default()
            }
            .validated()
            .is_err()
        );
        assert!(
            DaytonaResources {
                memory_mb: 2048,
                ..Default::default()
            }
            .validated()
            .is_err()
        );
        assert!(
            DaytonaResources {
                disk_mb: Some(0),
                ..Default::default()
            }
            .validated()
            .is_err()
        );
        for disk in [1024, 3 * 1024, 4096, 20 * 1024] {
            let resources = DaytonaResources {
                disk_mb: Some(disk),
                ..Default::default()
            }
            .validated()
            .unwrap();
            assert_eq!(resources.disk_mb, Some(disk));
        }
    }

    #[test]
    fn daytona_defaults_use_a_container_with_provider_selected_disk() {
        let options = SandboxOptions {
            backend: SandboxBackend::Daytona,
            ..Default::default()
        };
        assert_eq!(options.daytona_kind.sandbox_kind(), SandboxKind::Container);
        let resources = options.daytona_resources.validated().unwrap();
        assert_eq!(resources.cpu_cores, Some(2));
        assert_eq!(resources.memory_mb, Some(4096));
        assert_eq!(resources.disk_mb, None);
    }

    #[test]
    fn daytona_validates_the_effective_memory_allocation() {
        for memory_mb in [0, 1, 2048, 3072] {
            assert!(
                DaytonaResources {
                    memory_mb,
                    ..Default::default()
                }
                .validated()
                .is_err()
            );
        }
        for memory_mb in [3073, 3815, 4096] {
            let resources = DaytonaResources {
                memory_mb,
                ..Default::default()
            }
            .validated()
            .unwrap();
            assert_eq!(resources.memory_mb, Some(4096));
            assert_eq!(resources.disk_mb, None);
        }
    }
}
