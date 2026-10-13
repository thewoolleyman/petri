//! Petri executors over the sandbox-driver provider family.
//!
//! [`SandboxExecutor`] maps scopes and durable leases to Host, Docker, or
//! Daytona sandboxes. Providers own process execution, files, one-shot
//! containers, and cleanup. [`RoutingExecutor`] selects the provider from
//! runtime options and the scope's target. [`HostExecutor`] is a convenience
//! wrapper for standalone Host execution.
//!
//! No provider crate is linked here. A provider is reached one of two ways:
//!
//! - as a plugin process over sandbox-driver's JSON-RPC protocol ([`plugin`]),
//!   the default, and the only path a third-party provider has;
//! - as a built-in the embedding application links and hands over through
//!   [`InProcessProviders`] ([`in_process`]), in which case no plugin is
//!   launched.
//!
//! Both record the same fingerprints ([`fingerprint`]), so a lease recorded
//! one way is recoverable the other. The [`SimulatedProvider`] is a dry
//! run's: it creates nothing and runs nothing.

#[cfg(test)]
mod acquire_tests;
mod actions;
mod backend;
#[cfg(test)]
mod daytona_tests;
mod env;
pub mod fingerprint;
mod gate;
mod host;
pub mod in_process;
#[cfg(test)]
mod in_process_tests;
pub mod lease;
pub mod plugin;
#[cfg(test)]
mod reconcile_tests;
mod registration;
mod routing;
mod run;
mod simulated;
#[cfg(test)]
mod simulated_tests;
mod snapshots;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use executor::{
    AcquireContext, EnvError, EnvHandle, Executor, ReleaseReport, Retention, SandboxLeaseId,
    ScopeOutcome, ScopeSpec,
};
use ir::{ContainerOptions, RuntimeTarget, SandboxInstance};
use sandbox_driver::{
    NetworkPolicy, Sandbox, SandboxProvider, SandboxSource, SandboxSpec, WorkspaceOwnership,
};
use sandbox_driver_daytona_config::{
    DaytonaProviderConfig, DockerExecutionTarget, NestedDockerConfig,
};
use sandbox_driver_docker_config::{DockerProviderConfig, Health, RegistryAuth, Sidecar};
use smol_str::SmolStr;

pub use crate::backend::{
    DaytonaResources, DaytonaSandboxKind, LostSandbox, SandboxBackend, SandboxOptions,
};
use crate::env::{OneShotRunner, SandboxEnv};
use crate::gate::RunGate;
pub use crate::host::HostExecutor;
pub use crate::in_process::{
    InProcessProviders, ProviderContext, ProviderFactory, ProviderNetwork,
};
pub use crate::lease::{
    LeaseLedger, LeaseRecord, LeaseState, LedgerError, MemoryLedger, PendingIntent,
    ReconcileReport, RecordedLease, SandboxLeaseManager,
};
pub use crate::plugin::{FixedProvider, PluginError, PluginSettings, PluginSource, ProviderSource};
pub use crate::registration::{ProviderRegistrations, RegistrationError};
pub use crate::routing::{CONTAINER_KIND, RoutingExecutor};
use crate::run::workspace_dir;
pub use crate::run::{LEASE_LABEL, RUN_LABEL, RunIdentity, WORKSPACE_LABEL};
pub use crate::simulated::{SIMULATED_KIND, SimulatedProvider};
use crate::snapshots::{RunnerSnapshot, RunnerSnapshots};

/// The container path every scope's workspace lives at.
pub const CONTAINER_WORKSPACE: &str = "/workspace";
/// The alias a container on a local daemon reaches the driver's machine
/// through; the provider config maps it to the daemon's gateway.
const DOCKER_HOST_ALIAS: &str = "host.docker.internal";

/// The backend name this crate's errors carry.
const BACKEND: &str = "sandbox";

pub(crate) fn acquire_failed(error: &sandbox_driver::Error) -> EnvError {
    if let sandbox_driver::Error::Provider(error) = error
        && error.provider.as_str() == "host"
        && error.code.as_deref() == Some("fence_leaked")
    {
        return EnvError::FenceLeaked {
            detail: error.message.clone(),
        };
    }
    EnvError::backend(BACKEND, "acquire", error.to_string())
}

/// An [`Executor`] that realizes container scopes on one sandbox-driver
/// provider. The sandbox owns its workspace; every acquisition names the
/// lease it lives under, and one live handle serves every holder of that
/// lease.
pub struct SandboxExecutor {
    manager:      Arc<SandboxLeaseManager>,
    identity:     Arc<RunIdentity>,
    retention:    Retention,
    host_address: Option<String>,
    options:      SandboxOptions,
    /// A registered provider's template: every process scope is built
    /// from it. Built-in kinds have none and use their own recipes.
    process_spec: Option<SandboxSpec>,
    snapshots:    RunnerSnapshots,
    /// Every scope is realized on the [`SimulatedProvider`], whatever its
    /// target: a bare spec, no host-process semantics, no route back.
    simulated:    bool,
    /// Admission to the environments this executor hands out.
    gate:         RunGate,
}

impl SandboxExecutor {
    /// Builds an executor over `source`, with `ledger` as the durable
    /// record of its leases; `identity` names the run.
    pub fn new(
        source: Arc<dyn ProviderSource>,
        ledger: Arc<dyn LeaseLedger>,
        identity: Arc<RunIdentity>,
        retention: Retention,
        host_address: Option<String>,
        options: SandboxOptions,
    ) -> Self {
        Self {
            manager: Arc::new(
                SandboxLeaseManager::new(source, ledger, identity.clone())
                    .with_lost_sandbox(options.lost_sandbox),
            ),
            identity,
            retention,
            host_address,
            options,
            process_spec: None,
            snapshots: RunnerSnapshots::default(),
            simulated: false,
            gate: RunGate::default(),
        }
    }

    /// Build every process scope from `template`: a registered provider's.
    #[must_use]
    pub(crate) fn with_process_spec(mut self, template: SandboxSpec) -> Self {
        self.process_spec = Some(template);
        self
    }

    /// A registered provider's template describes one ordinary process
    /// sandbox. A container target, services, or placement requirements
    /// need a recipe Petri has only for its built-in kinds, so a scope
    /// asking for any of them is refused before the provider is reached.
    fn validate_scope(&self, scope: &ScopeSpec) -> Result<(), EnvError> {
        if self.process_spec.is_none() {
            return Ok(());
        }
        let refused = match &scope.runtime.target {
            RuntimeTarget::Container { .. } => Some("a container target"),
            RuntimeTarget::HostProcess if !scope.services.is_empty() => Some("services"),
            RuntimeTarget::HostProcess if !scope.runtime.requirements.is_empty() => {
                Some("placement requirements")
            }
            RuntimeTarget::HostProcess => None,
        };
        match refused {
            Some(what) => {
                let kind = self.manager.source().kind();
                Err(EnvError::backend(
                    kind,
                    "acquire",
                    format!(
                        "the {kind} provider runs ordinary process scopes only; this scope asks for {what}"
                    ),
                ))
            }
            None => Ok(()),
        }
    }

    /// The policy this run's sandboxes are created with. A registered
    /// provider's template may fix one and the run may ask for one; when
    /// both do, they must agree, since neither refines the other.
    fn network_policy(&self) -> Result<NetworkPolicy, EnvError> {
        let requested = &self.options.network;
        let Some(template) = &self.process_spec else {
            return Ok(requested.clone());
        };
        match (&template.network, requested) {
            (policy, NetworkPolicy::ProviderDefault) | (NetworkPolicy::ProviderDefault, policy) => {
                Ok(policy.clone())
            }
            (template, requested) if template == requested => Ok(requested.clone()),
            _ => Err(EnvError::backend(
                self.manager.source().kind(),
                "acquire",
                "the run's network policy differs from the registered provider's template; ask for \
                 the template's policy or the provider default",
            )),
        }
    }

    /// Admits this executor's environments through the router's gate, so
    /// closing the run closes them.
    #[must_use]
    pub(crate) fn with_gate(mut self, gate: RunGate) -> Self {
        self.gate = gate;
        self
    }

    /// An executor over the [`SimulatedProvider`] in `source`: what a dry
    /// run acquires every scope on. A simulated sandbox does not outlive
    /// its process, so a lease whose sandbox is gone gets a fresh one
    /// ([`LostSandbox::Replace`]): there was never a workspace to lose. Nor
    /// is there one to keep, so a standalone lease ends deleted.
    pub fn simulated(
        source: Arc<dyn ProviderSource>,
        ledger: Arc<dyn LeaseLedger>,
        identity: Arc<RunIdentity>,
    ) -> Self {
        let options = SandboxOptions {
            lost_sandbox: LostSandbox::Replace,
            ..SandboxOptions::default()
        };
        let mut executor = Self::new(source, ledger, identity, Retention::Never, None, options);
        executor.simulated = true;
        executor
    }

    pub fn manager(&self) -> &Arc<SandboxLeaseManager> {
        &self.manager
    }

    /// The name prefix every sandbox this run owns starts with,
    /// `petri-<run id>-`, for a leak check after release.
    pub fn container_prefix(&self) -> String {
        self.identity.container_prefix()
    }

    async fn acquire_inner(
        &self,
        scope: &ScopeSpec,
        ctx: &AcquireContext,
    ) -> Result<EnvHandle, EnvError> {
        // A coordinator names the lease; a bare driver has none, and its
        // sandbox is the scope's alone, keyed by the scope and ended with it.
        let (lease, standalone) = match ctx.lease() {
            Some(lease) => (lease, false),
            None => (SandboxLeaseId::new(u64::from(scope.id.raw())), true),
        };
        let name = self.identity.container_name(lease);
        self.validate_scope(scope)?;
        let network = self.network_policy()?;
        let created_with = network.clone();
        let acquired = self
            .manager
            .acquire(
                lease::LeaseRequest {
                    lease,
                    workspace_id: scope.workspace_id.as_str(),
                    standalone,
                },
                |labels, provider| async move {
                    let mut spec = self
                        .build_spec(scope, &labels, &name, ctx, &*provider)
                        .await?;
                    if !self.simulated {
                        spec.network = created_with;
                    }
                    Ok(spec)
                },
            )
            .await?;
        let sandbox = acquired.sandbox();
        // A retained sandbox may predate the requested block policy. Check
        // provider state before handing it to hooks or executing any stage.
        if !self.simulated && network == NetworkPolicy::Block {
            let status = sandbox
                .describe()
                .await
                .map_err(|error| acquire_failed(&error));
            let verified = status.and_then(|status| {
                if status.network == Some(NetworkPolicy::Block) {
                    Ok(())
                } else {
                    Err(EnvError::backend(
                        BACKEND,
                        "acquire",
                        "the sandbox does not report blocked networking; refusing to execute a blocked run",
                    ))
                }
            });
            if let Err(error) = verified {
                acquired.release().await;
                return Err(error);
            }
        }

        // The ambient environment is a fact the steps rely on (`PATH` for
        // the process step, say); a provider that cannot report it is not
        // one this executor can serve, and says so at acquire.
        let ambient = match sandbox.environment().await {
            Ok(ambient) => ambient,
            Err(error) => {
                acquired.release().await;
                return Err(EnvError::backend(
                    BACKEND,
                    "acquire",
                    format!("the sandbox's environment could not be read: {error}"),
                ));
            }
        };
        let mut env_overrides = scope.env.clone();
        if let RuntimeTarget::Container { options, .. } = &scope.runtime.target {
            env_overrides.extend(options.env.clone());
        }
        let workspace = sandbox.working_directory().to_owned();
        let instance = self.describe(&**sandbox, &workspace).await;
        let host = !self.simulated
            && self.options.backend == SandboxBackend::Host
            && matches!(scope.runtime.target, RuntimeTarget::HostProcess);
        let env = SandboxEnv {
            host,
            sandbox: sandbox.clone(),
            workspace: workspace.clone(),
            ambient,
            env: env_overrides,
            grace: scope.grace,
            host_address: self.host_address.clone(),
            gate: self.gate.clone(),
        };
        // Docker actions in this scope run as one-shot containers in the
        // sandbox's world: same workspace, same services, same host alias.
        let runner = OneShotRunner {
            sandbox: sandbox.clone(),
            workspace,
            host_address: self.host_address.clone(),
            env: scope.env.clone(),
            gate: self.gate.clone(),
        };
        let teardown = SandboxTeardown {
            provider: self.manager.source().kind().to_owned(),
            sandbox: acquired.into_sandbox(),
            lease,
            standalone,
        };
        let handle = EnvHandle::new(
            scope.id,
            SmolStr::new(scope.environment.as_str()),
            instance,
            Arc::new(env),
            teardown,
        );
        // A registered provider's template is for process scopes alone: no
        // Docker actions run in its sandboxes, so no runner is attached, and
        // an action step fails with the executor's own clear error.
        Ok(if self.process_spec.is_some() {
            handle
        } else {
            handle.with_runner(Arc::new(runner))
        })
    }

    /// The record of an acquired sandbox: its provider and id, and what the
    /// provider reports it runs. A provider that cannot describe the sandbox
    /// still has one: the image and snapshot are then unknown.
    async fn describe(&self, sandbox: &dyn Sandbox, workspace: &str) -> SandboxInstance {
        let (image, snapshot) = match sandbox.describe().await {
            Ok(status) => (status.image, status.snapshot),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    sandbox = %sandbox.id(),
                    "the acquired sandbox could not be described; its image is unrecorded"
                );
                (None, None)
            }
        };
        SandboxInstance {
            provider:          SmolStr::new(self.manager.source().kind()),
            instance:          SmolStr::new(sandbox.id().as_str()),
            image:             image.map(SmolStr::new),
            snapshot:          snapshot.map(SmolStr::new),
            working_directory: SmolStr::new(workspace),
        }
    }

    async fn build_spec(
        &self,
        scope: &ScopeSpec,
        labels: &[(String, String)],
        name: &str,
        ctx: &AcquireContext,
        provider: &dyn SandboxProvider,
    ) -> Result<SandboxSpec, EnvError> {
        if self.simulated {
            // A name, a working directory string and the run's labels:
            // nothing the scope declares (its image, services, env or
            // credentials) reaches a provider, because none is involved.
            let mut spec = SandboxSpec::new(SandboxSource::HostDirectory)
                .name(name)
                .working_directory(format!("/{SIMULATED_KIND}/{}", scope.workspace_id));
            spec.labels.extend(labels.iter().cloned());
            return Ok(spec);
        }
        if let Some(template) = &self.process_spec {
            // The registration's template, as given, under Petri's name and
            // labels, with the scope's environment over the template's.
            let mut spec = template.clone();
            spec.name = Some(name.to_owned());
            spec.labels.extend(labels.iter().cloned());
            spec.env.extend(
                scope
                    .env
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string())),
            );
            return Ok(spec);
        }
        if self.options.backend == SandboxBackend::Host
            && matches!(scope.runtime.target, RuntimeTarget::HostProcess)
        {
            if !scope.services.is_empty() {
                return Err(EnvError::backend(
                    "host",
                    "acquire",
                    "services require a containerized job; set container: or select the docker backend",
                ));
            }
            let workspace = workspace_dir(self.identity.run_dir(), scope.workspace_id.as_str());
            let mut spec = SandboxSpec::new(SandboxSource::HostDirectory)
                .name(name)
                .working_directory(workspace.to_string_lossy());
            spec.workspace_ownership = Some(WorkspaceOwnership::Managed);
            spec.labels.extend(labels.iter().cloned());
            spec.env.extend(
                scope
                    .env
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string())),
            );
            return Ok(spec);
        }
        if self.options.backend != SandboxBackend::Daytona {
            if self.options.backend == SandboxBackend::Docker
                && matches!(scope.runtime.target, RuntimeTarget::HostProcess)
            {
                let mut runner = scope.clone();
                runner.runtime.target = RuntimeTarget::Container {
                    image:       self.options.runner_image(&scope.runtime)?.into(),
                    options:     ContainerOptions::default(),
                    credentials: None,
                };
                return build_spec(&runner, labels, name, ctx);
            }
            return build_spec(scope, labels, name, ctx);
        }
        let image = self.options.runner_image(&scope.runtime)?;
        let snapshot = RunnerSnapshot::new(
            &image,
            self.options.daytona_resources.validated()?,
            self.options.daytona_kind.sandbox_kind(),
            self.manager.source().region(),
        );
        let mut spec = build_daytona_spec(scope, ctx, &snapshot)?;
        spec.name = Some(name.to_owned());
        spec.labels.extend(labels.iter().cloned());
        let snapshots = provider.snapshots().ok_or_else(|| {
            EnvError::backend(
                "daytona",
                "snapshot",
                "the provider does not support runner snapshots",
            )
        })?;
        self.snapshots.ensure(snapshots, &snapshot).await?;
        Ok(spec)
    }
}

fn build_daytona_spec(
    scope: &ScopeSpec,
    ctx: &AcquireContext,
    snapshot: &RunnerSnapshot,
) -> Result<SandboxSpec, EnvError> {
    let (target, image, user, options) = match &scope.runtime.target {
        RuntimeTarget::HostProcess => {
            if !scope.services.is_empty() {
                return Err(EnvError::backend(
                    "daytona",
                    "acquire",
                    "services require a container job",
                ));
            }
            (
                DockerExecutionTarget::VirtualMachine,
                String::new(),
                None,
                DockerProviderConfig::default(),
            )
        }
        RuntimeTarget::Container {
            image,
            options,
            credentials,
        } => {
            let mut config = docker_provider_config(
                registry_auth(credentials.as_ref(), ctx)?,
                sidecars(scope, ctx)?,
                options,
            );
            // A Daytona job cannot use the local Docker daemon's host alias.
            config.extra_hosts.clear();
            (
                DockerExecutionTarget::Container,
                image.to_string(),
                options.user.as_ref().map(ToString::to_string),
                config,
            )
        }
    };
    let mut spec = snapshot
        .sandbox_spec()
        .working_directory(CONTAINER_WORKSPACE);
    spec.user = Some("root".to_owned());
    spec.public = Some(false);
    spec.timers.auto_stop_after_idle = Some(Duration::ZERO);
    spec.timers.auto_pause_after_idle = Some(Duration::ZERO);
    spec.timers.auto_delete_after_stop = Some(Duration::ZERO);
    spec.timers.ttl = Some(Duration::ZERO);
    spec.env.extend(
        scope
            .env
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string())),
    );
    if let RuntimeTarget::Container { options, .. } = &scope.runtime.target {
        spec.env.extend(
            options
                .env
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string())),
        );
    }
    spec.provider_config = DaytonaProviderConfig {
        docker: Some(NestedDockerConfig {
            image,
            target,
            user,
            options,
        }),
        ..Default::default()
    }
    .into_value();
    Ok(spec)
}

/// Maps a container scope to a `SandboxSpec`: the workspace inside the
/// sandbox, the run's labels, and the provider's typed options.
fn build_spec(
    scope: &ScopeSpec,
    labels: &[(String, String)],
    name: &str,
    ctx: &AcquireContext,
) -> Result<SandboxSpec, EnvError> {
    let RuntimeTarget::Container {
        image,
        options,
        credentials,
    } = &scope.runtime.target
    else {
        return Err(EnvError::backend(
            BACKEND,
            "acquire",
            "this executor realizes container scopes only; a host-process scope needs the host \
             executor or a runner image",
        ));
    };
    let registry_auth = registry_auth(credentials.as_ref(), ctx)?;
    let sidecars = sidecars(scope, ctx)?;
    let provider_config = docker_provider_config(registry_auth, sidecars, options);
    let mut spec = SandboxSpec::new(SandboxSource::Image {
        reference: image.to_string(),
    })
    .working_directory(CONTAINER_WORKSPACE)
    .provider_config(provider_config.into_value())
    .name(name);
    for (key, value) in labels {
        spec = spec.label(key.clone(), value.clone());
    }
    spec.user = options.user.as_ref().map(ToString::to_string);

    // Scope env is the trusted channel for the container; a `-e` option lands
    // on top, as the later `docker create` flag would have.
    for (key, value) in &scope.env {
        spec = spec.env_var(key.as_str(), value.as_str());
    }
    for (key, value) in &options.env {
        spec = spec.env_var(key.as_str(), value.as_str());
    }
    Ok(spec)
}

/// Resolves image pull credentials to registry auth, or `None` when the
/// scope declares none.
fn registry_auth(
    credentials: Option<&ir::RegistryCredentials>,
    ctx: &AcquireContext,
) -> Result<Option<RegistryAuth>, EnvError> {
    let Some(credentials) = credentials else {
        return Ok(None);
    };
    let password = resolve_secret(&credentials.password_secret, ctx)?;
    Ok(Some(RegistryAuth {
        username: credentials.username.to_string(),
        password,
        server: None,
    }))
}

/// Maps the scope's services to Docker sidecars. Ports are dropped: a
/// containerized job reaches a service by its network alias, not a published
/// port.
fn sidecars(scope: &ScopeSpec, ctx: &AcquireContext) -> Result<Vec<Sidecar>, EnvError> {
    let mut sidecars = Vec::with_capacity(scope.services.len());
    for service in &scope.services {
        let typed = &service.options;
        let mut sidecar = Sidecar::new(service.name.as_str(), service.image.as_str());
        // Declared env first, then `-e` flags on top, as `docker create`
        // would apply them.
        sidecar.env = service
            .env
            .iter()
            .chain(typed.env.iter().map(|(key, value)| (key, value)))
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        sidecar.dns = typed.dns.iter().map(ToString::to_string).collect();
        sidecar.cap_add = typed.cap_add.iter().map(ToString::to_string).collect();
        sidecar.user = typed.user.as_ref().map(ToString::to_string);
        sidecar.privileged = typed.privileged;
        sidecar.entrypoint = typed
            .entrypoint
            .as_ref()
            .map(|words| words.iter().map(ToString::to_string).collect());
        sidecar.health = typed.health.as_ref().map(|health| Health {
            cmd:             health
                .cmd
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            interval_ms:     health.interval_ms,
            timeout_ms:      health.timeout_ms,
            retries:         health.retries,
            start_period_ms: health.start_period_ms,
        });
        if let Some(credentials) = &service.credentials {
            let password = resolve_secret(&credentials.password_secret, ctx)?;
            sidecar.registry_auth = Some(RegistryAuth {
                username: credentials.username.to_string(),
                password,
                server: None,
            });
        }
        sidecars.push(sidecar);
    }
    Ok(sidecars)
}

/// Resolves a secret by name inside acquire, registering it for masking.
fn resolve_secret(name: &str, ctx: &AcquireContext) -> Result<String, EnvError> {
    ctx.secrets()
        .resolve(name)
        .map(|secret| secret.expose().to_string())
        .map_err(|error| {
            EnvError::backend(
                BACKEND,
                "acquire",
                format!("resolving secret `{name}`: {error}"),
            )
        })
}

/// The Docker `provider_config` for a scope container: an init process
/// reaps zombies, the host alias resolves to the gateway, and the
/// workspace is the sandbox's own volume — no bind from this machine.
fn docker_provider_config(
    registry_auth: Option<RegistryAuth>,
    sidecars: Vec<Sidecar>,
    container: &ir::ContainerOptions,
) -> DockerProviderConfig {
    // Steps run as a program plus arguments, so an image without bash
    // (alpine) works: the provider's exec wrapper needs only /bin/sh.
    DockerProviderConfig {
        init: true,
        privileged: container.privileged,
        platform: container.platform.as_ref().map(ToString::to_string),
        extra_hosts: vec![format!("{DOCKER_HOST_ALIAS}:host-gateway")],
        dns: container.dns.iter().map(ToString::to_string).collect(),
        cap_add: container.cap_add.iter().map(ToString::to_string).collect(),
        registry_auth,
        sidecars,
        ..DockerProviderConfig::default()
    }
}

/// What release needs: the lease the environment held, and whether this
/// executor alone owns it.
pub(crate) struct SandboxTeardown {
    /// The provider kind that acquired the sandbox: where release goes.
    provider:   String,
    sandbox:    Arc<dyn Sandbox>,
    lease:      SandboxLeaseId,
    /// No coordinator named the lease: the sandbox ends with the scope.
    standalone: bool,
}

impl fmt::Debug for SandboxTeardown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandboxTeardown")
            .field("provider", &self.provider)
            .field("sandbox", &self.sandbox.id())
            .field("lease", &self.lease)
            .field("standalone", &self.standalone)
            .finish()
    }
}

#[async_trait]
impl Executor for SandboxExecutor {
    #[tracing::instrument(name = "scope.acquire", skip_all, fields(scope = scope.id.raw()))]
    async fn acquire(
        &self,
        scope: &ScopeSpec,
        ctx: &AcquireContext,
    ) -> Result<EnvHandle, EnvError> {
        match self.acquire_inner(scope, ctx).await {
            Ok(handle) => {
                tracing::info!("scope environment acquired");
                Ok(handle)
            }
            Err(error) => {
                tracing::error!(error = ?error, "scope environment acquire failed");
                Err(error)
            }
        }
    }

    /// Drops this execution's holder. A standalone acquisition — no
    /// coordinator, no lease — ends its sandbox here by retention; a
    /// managed one leaves that to the lease's release.
    #[tracing::instrument(
        name = "scope.release",
        skip_all,
        fields(scope = env.scope().raw(), outcome = ?outcome)
    )]
    async fn release(&self, env: EnvHandle, outcome: ScopeOutcome) -> ReleaseReport {
        let Some(teardown) = env.teardown::<SandboxTeardown>() else {
            return ReleaseReport::default()
                .problem("sandbox executor was handed a foreign environment");
        };
        let (lease, standalone) = (teardown.lease, teardown.standalone);
        drop(env);
        let remaining = self.manager.release_holder(lease).await;
        let mut report = ReleaseReport::default().released(format!("holder of lease {lease}"));
        if !standalone || remaining > 0 {
            return report;
        }
        let ended = self
            .manager
            .release_lease(lease, self.retention, outcome)
            .await;
        report.merge(ended);
        report
    }
}
