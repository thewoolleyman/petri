//! An [`Executor`] that routes each scope to the backend its runtime target
//! needs, using [`SandboxExecutor`] over the Host, Docker, or Daytona
//! provider (its plugin, or a built-in linked into this process through
//! [`InProcessProviders`]), or over the in-process [`SimulatedProvider`] for
//! a dry run. It is the composition the runtime registers for a run.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use executor::{
    AcquireContext, EnvError, EnvHandle, Executor, ReleaseReport, Retention, SandboxLeaseId,
    ScopeOutcome, ScopeSpec, WorkspaceId,
};
use ir::RuntimeTarget;
use sandbox_driver::SandboxId;
use tokio::fs;
use tokio::sync::OnceCell;

use crate::actions::{ActionHost, ActionHostRunner, remove_recorded};
use crate::gate::{DRAIN_BUDGET, RunGate};
use crate::in_process::{InProcessProviders, InProcessSource, ProviderContext, ProviderNetwork};
use crate::lease::{LeaseLedger, MemoryLedger, ReconcileReport, RecordedLease};
use crate::plugin::{FixedProvider, PluginSettings, PluginSource, ProviderSource};
use crate::run::{RunIdentity, workspace_dir};
use crate::simulated::{SIMULATED_KIND, SimulatedProvider};
use crate::{SandboxBackend, SandboxExecutor, SandboxOptions, SandboxTeardown};

/// The provider kind container scopes go to.
pub const CONTAINER_KIND: &str = "docker";

/// What ends a host scope's action host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ActionOwner {
    Lease(SandboxLeaseId),
    Scope(ir::ScopeId),
}

/// How the container side is built: from this process's environment, or
/// from a provider a test handed in.
enum ContainerSource {
    Env,
    Fixed(Arc<dyn ProviderSource>),
}

/// One settings snapshot and one supervised process for the entire router.
struct ProviderConfig {
    source:                  Arc<dyn ProviderSource>,
    host_address:            Result<Option<String>, String>,
    supports_host_workspace: bool,
}

/// Routes scopes to one [`SandboxExecutor`] per provider kind.
///
/// A host-process scope runs as real processes on this machine, with the
/// sentinel and crash fence a bare process needs; a container scope runs on
/// the Docker provider, reached through its plugin. The plugin is launched
/// on the first container scope, so a host-only run never touches a daemon,
/// and a container scope fails routably at acquire when it cannot be.
///
/// With [`RoutingExecutor::with_in_process`], the same routes go to the
/// built-in providers linked into this process instead of their plugins,
/// and no plugin is launched.
///
/// A [`RoutingExecutor::simulated`] router is a dry run's: every scope,
/// whatever its target and whatever backend the options name, is acquired
/// on the [`SimulatedProvider`], and no plugin is ever launched. A lease
/// another provider holds (a real run resumed as a dry run) is left where
/// it is and reported, never touched.
pub struct RoutingExecutor {
    host:         OnceCell<Result<Arc<SandboxExecutor>, String>>,
    source:       ContainerSource,
    provider:     OnceLock<Result<ProviderConfig, String>>,
    container:    OnceCell<Result<Arc<SandboxExecutor>, String>>,
    /// The action hosts of host scopes, each keyed by what ends it: the
    /// lease that owns it, or the scope itself when no coordinator named one.
    action_hosts: Mutex<BTreeMap<(ActionOwner, WorkspaceId), Arc<ActionHost>>>,
    ledger:       OnceLock<Arc<dyn LeaseLedger>>,
    identity:     Arc<RunIdentity>,
    retention:    Retention,
    options:      SandboxOptions,
    /// Every scope goes to the simulated provider: a dry run.
    simulated:    bool,
    /// Built-in providers to use instead of launching plugins.
    in_process:   Option<InProcessProviders>,
    /// Admission to every environment this router hands out; closed at
    /// shutdown, so a retained environment cannot outlive the run.
    gate:         RunGate,
}

impl RoutingExecutor {
    /// Host and Docker plugins, launched on first use. The standard runtime's
    /// default.
    pub fn local(run_dir: impl Into<PathBuf>, retention: Retention) -> Self {
        Self::with_options(run_dir, retention, SandboxOptions::default())
    }

    /// [`RoutingExecutor::local`] with the plugin dev-mode decision made by
    /// the caller (the CLI's `--sandbox-plugin-dev`).
    pub fn local_with_dev(run_dir: impl Into<PathBuf>, retention: Retention, dev: bool) -> Self {
        Self::with_options(run_dir, retention, SandboxOptions {
            plugin_dev: Some(dev),
            ..Default::default()
        })
    }

    pub fn with_options(
        run_dir: impl Into<PathBuf>,
        retention: Retention,
        options: SandboxOptions,
    ) -> Self {
        let mut router = Self::over(run_dir, retention, ContainerSource::Env);
        router.options = options;
        router
    }

    /// A router whose container scopes go to `source`: tests over a fake
    /// or fixed provider.
    pub fn with_provider_source(
        source: Arc<dyn ProviderSource>,
        run_dir: impl Into<PathBuf>,
        retention: Retention,
    ) -> Self {
        Self::over(run_dir, retention, ContainerSource::Fixed(source))
    }

    /// A router over a fixed in-process provider, for tests.
    pub fn with_provider(
        provider: Arc<dyn sandbox_driver::SandboxProvider>,
        run_dir: impl Into<PathBuf>,
        retention: Retention,
    ) -> Self {
        Self::with_provider_source(Arc::new(FixedProvider::new(provider)), run_dir, retention)
    }

    /// A dry run's router: every scope is acquired on the
    /// [`SimulatedProvider`], and no plugin is launched. The sandbox options
    /// a run would place scopes with do not apply, and neither does its
    /// retention: there is nothing to place and nothing to keep, so every
    /// lease is recorded deleted when it is released.
    pub fn simulated(run_dir: impl Into<PathBuf>) -> Self {
        let provider: Arc<dyn sandbox_driver::SandboxProvider> = Arc::new(SimulatedProvider::new());
        let source: Arc<dyn ProviderSource> = Arc::new(FixedProvider::new(provider));
        let mut router = Self::over(run_dir, Retention::Never, ContainerSource::Fixed(source));
        router.simulated = true;
        router
    }

    fn over(run_dir: impl Into<PathBuf>, retention: Retention, source: ContainerSource) -> Self {
        let run_dir = run_dir.into();
        Self {
            host: OnceCell::new(),
            source,
            provider: OnceLock::new(),
            container: OnceCell::new(),
            action_hosts: Mutex::new(BTreeMap::new()),
            ledger: OnceLock::new(),
            identity: Arc::new(RunIdentity::for_run_dir(run_dir)),
            retention,
            options: SandboxOptions::default(),
            simulated: false,
            in_process: None,
            gate: RunGate::default(),
        }
    }

    /// Reach Host, Docker, and Daytona through the built-in providers in
    /// `providers` instead of their plugins. A kind with no factory fails
    /// routably at acquire; no plugin is launched in its place. A router
    /// over a supplied source keeps that source for its container scopes
    /// and still takes its Host scopes here; a simulated router reaches no
    /// provider at all.
    #[must_use]
    pub fn with_in_process(mut self, providers: InProcessProviders) -> Self {
        self.in_process = Some(providers);
        self
    }

    /// Name the run on its providers: the run's store key, so a resume
    /// reaches the crashed run's sandboxes. A router built without one is
    /// named after its run directory ([`RunIdentity::for_run_dir`]).
    #[must_use]
    pub fn with_run_id(mut self, run_id: impl Into<smol_str::SmolStr>) -> Self {
        self.identity = Arc::new(RunIdentity::new(
            self.identity.run_dir().to_path_buf(),
            run_id,
        ));
        self
    }

    /// The durable ledger container leases are recorded in. A coordinator
    /// sets it before the first container scope; without one, leases live
    /// in memory and sandboxes end with their scopes.
    pub fn set_ledger(&self, ledger: Arc<dyn LeaseLedger>) {
        let _ = self.ledger.set(ledger);
    }

    pub fn identity(&self) -> &Arc<RunIdentity> {
        &self.identity
    }

    /// Close the run's access to its sandboxes, then stop the providers
    /// this router created, after its run's leases settle. Environments
    /// handed out earlier refuse new work from here on. A supplied container
    /// source belongs to its caller and can be shared. Durable sandbox
    /// records and retained workspaces remain available.
    pub async fn shutdown(&self) {
        // A plugin's closing transport ends its in-flight work; any other
        // source needs admitted work to settle first.
        let host_source = self
            .host
            .get()
            .and_then(|executor| executor.as_ref().ok())
            .map(|executor| executor.manager().source());
        let container_source = self
            .provider
            .get()
            .and_then(|provider| provider.as_ref().ok())
            .map(|provider| &provider.source);
        let settles = host_source
            .into_iter()
            .chain(container_source)
            .any(|source| !source.ends_work_on_shutdown());
        let budget = if settles {
            DRAIN_BUDGET
        } else {
            Duration::ZERO
        };
        if !self.gate.close(budget).await && !budget.is_zero() {
            tracing::warn!(
                budget_secs = DRAIN_BUDGET.as_secs(),
                "sandbox operations were still running when the run closed its sandboxes"
            );
        }
        let host = async {
            if let Some(Ok(executor)) = self.host.get() {
                executor.manager().source().shutdown().await;
            }
        };
        let container = async {
            if matches!(self.source, ContainerSource::Env)
                && let Some(Ok(provider)) = self.provider.get()
            {
                provider.source.shutdown().await;
            }
        };
        tokio::join!(host, container);
    }

    /// Provider identity for the lease reservation, before resource creation.
    pub fn provider_kind_for(&self, runtime: &ir::RuntimeSpec) -> &str {
        if self.simulated {
            return SIMULATED_KIND;
        }
        if self.options.backend == SandboxBackend::Host
            && matches!(runtime.target, RuntimeTarget::HostProcess)
        {
            return "host";
        }
        match &self.source {
            ContainerSource::Fixed(source) => source.kind(),
            ContainerSource::Env => self.plugin_kind(),
        }
    }

    fn plugin_kind(&self) -> &str {
        if self.simulated {
            return SIMULATED_KIND;
        }
        match self.options.backend {
            SandboxBackend::Host | SandboxBackend::Docker => "docker",
            SandboxBackend::Daytona => "daytona",
            SandboxBackend::Plugin => self
                .options
                .plugin
                .as_ref()
                .map_or("plugin", |settings| settings.kind().as_str()),
        }
    }

    fn provider(&self) -> Result<&ProviderConfig, EnvError> {
        self.provider
            .get_or_init(|| match &self.source {
                // A simulated sandbox offers no route to this machine and
                // mounts no workspace: nothing runs in it.
                ContainerSource::Fixed(source) => Ok(ProviderConfig {
                    source:                  source.clone(),
                    host_address:            Ok(
                        (!self.simulated).then(|| crate::DOCKER_HOST_ALIAS.to_owned())
                    ),
                    supports_host_workspace: !self.simulated,
                }),
                ContainerSource::Env => {
                    // A plugin backend names a third-party provider, which is
                    // always a plugin even when built-in providers are linked.
                    let in_process = self
                        .in_process
                        .as_ref()
                        .filter(|_| self.options.backend != SandboxBackend::Plugin);
                    if let Some(providers) = in_process {
                        let (source, network) =
                            self.in_process_source(providers, self.plugin_kind(), None)?;
                        return Ok(ProviderConfig {
                            source,
                            host_address: network.host_address(),
                            supports_host_workspace: network.supports_host_workspace(),
                        });
                    }
                    let settings = match (&self.options.plugin, self.options.backend) {
                        (Some(settings), SandboxBackend::Plugin) => settings.clone(),
                        (None, SandboxBackend::Plugin) => {
                            return Err("the plugin backend needs plugin settings".to_owned());
                        }
                        _ => PluginSettings::from_env(self.plugin_kind(), self.options.plugin_dev)
                            .map_err(|error| error.to_string())?,
                    };
                    let host_address = settings.host_address().map_err(|error| error.to_string());
                    let supports_host_workspace = settings.supports_host_workspace();
                    Ok(ProviderConfig {
                        source: Arc::new(PluginSource::new(settings)),
                        host_address,
                        supports_host_workspace,
                    })
                }
            })
            .as_ref()
            .map_err(|message| EnvError::backend(self.plugin_kind(), "configure", message.clone()))
    }

    async fn host(&self) -> Result<Arc<SandboxExecutor>, EnvError> {
        self.host
            .get_or_init(|| async {
                if self.simulated {
                    return Err("a dry run launches no provider plugin; a lease the host \
                                provider holds is left as recorded"
                        .to_owned());
                }
                let directory = self.identity.run_dir().join("host-registry");
                fs::create_dir_all(&directory)
                    .await
                    .map_err(|e| e.to_string())?;
                let directory = fs::canonicalize(directory)
                    .await
                    .map_err(|e| e.to_string())?;
                let (source, host_address): (Arc<dyn ProviderSource>, _) =
                    if let Some(providers) = &self.in_process {
                        let (source, network) =
                            self.in_process_source(providers, "host", Some(directory))?;
                        (source, network.host_address()?)
                    } else {
                        let settings = PluginSettings::from_env("host", self.options.plugin_dev)
                            .map_err(|e| e.to_string())?
                            .with_host_registry(&directory);
                        (
                            Arc::new(PluginSource::new(settings)),
                            Some("127.0.0.1".to_owned()),
                        )
                    };
                let ledger = self
                    .ledger
                    .get()
                    .cloned()
                    .unwrap_or_else(|| Arc::new(MemoryLedger::default()));
                Ok(Arc::new(
                    SandboxExecutor::new(
                        source,
                        ledger,
                        self.identity.clone(),
                        self.retention,
                        host_address,
                        self.options.clone(),
                    )
                    .with_gate(self.gate.clone()),
                ))
            })
            .await
            .clone()
            .map_err(|message| EnvError::backend("host", "configure", message))
    }

    /// `kind`'s built-in provider for this run, and how its sandboxes reach
    /// this machine.
    fn in_process_source(
        &self,
        providers: &InProcessProviders,
        kind: &str,
        host_registry: Option<PathBuf>,
    ) -> Result<(Arc<dyn ProviderSource>, ProviderNetwork), String> {
        let factory = providers.factory(kind)?;
        let network = factory.network();
        let context = ProviderContext::new(
            self.identity.run_id(),
            self.identity.run_dir(),
            host_registry,
        );
        Ok((Arc::new(InProcessSource::new(factory, context)), network))
    }

    /// The provider kind a lease is recorded on, when the ledger knows it.
    async fn recorded_provider(&self, lease: SandboxLeaseId) -> Result<Option<String>, EnvError> {
        let Some(ledger) = self.ledger.get() else {
            return Ok(None);
        };
        let record = ledger
            .lookup(lease)
            .await
            .map_err(|e| EnvError::backend("sandbox", "lookup", e.to_string()))?;
        Ok(record.and_then(|record| record.provider.map(|provider| provider.to_string())))
    }

    async fn executor_for_record(
        &self,
        lease: SandboxLeaseId,
    ) -> Result<Arc<SandboxExecutor>, EnvError> {
        match self.recorded_provider(lease).await?.as_deref() {
            Some("host") => self.host().await,
            // A dry run's lease under a real router: no provider holds it,
            // so no plugin is launched to look for it.
            Some(SIMULATED_KIND) if !self.simulated => Err(EnvError::backend(
                SIMULATED_KIND,
                "lookup",
                format!("lease {lease} was simulated by a dry run; no provider holds it"),
            )),
            _ => self.container().await,
        }
    }

    /// The container executor, built on first use. A configuration error
    /// is remembered: every container scope then fails routably with it.
    async fn container(&self) -> Result<Arc<SandboxExecutor>, EnvError> {
        self.container
            .get_or_init(|| async {
                let provider = self.provider().map_err(|error| error.to_string())?;
                let host_address = provider.host_address.clone()?;
                let ledger =
                    self.ledger.get().cloned().unwrap_or_else(|| {
                        Arc::new(MemoryLedger::default()) as Arc<dyn LeaseLedger>
                    });
                if self.simulated {
                    return Ok(Arc::new(
                        SandboxExecutor::simulated(
                            provider.source.clone(),
                            ledger,
                            Arc::clone(&self.identity),
                        )
                        .with_gate(self.gate.clone()),
                    ));
                }
                Ok(Arc::new(
                    SandboxExecutor::new(
                        provider.source.clone(),
                        ledger,
                        Arc::clone(&self.identity),
                        self.retention,
                        host_address,
                        self.options.clone(),
                    )
                    .with_gate(self.gate.clone()),
                ))
            })
            .await
            .clone()
            .map_err(|message| EnvError::backend(self.plugin_kind(), "acquire", message))
    }

    /// The name prefix every sandbox this run owns starts with,
    /// `petri-<run id>-`, for a leak check after release.
    pub fn container_prefix(&self) -> String {
        self.identity.container_prefix()
    }

    fn action_host(
        &self,
        owner: ActionOwner,
        scope: &ScopeSpec,
    ) -> Result<Arc<ActionHost>, EnvError> {
        let provider = self.provider()?;
        if !provider.supports_host_workspace {
            return Err(EnvError::backend(
                CONTAINER_KIND,
                "one-shot",
                "Docker actions in Host jobs require a local Docker daemon to mount their workspace; \
                 use --backend docker for a remote daemon",
            ));
        }
        let host_address = provider
            .host_address
            .as_ref()
            .map_err(|message| EnvError::backend(CONTAINER_KIND, "configure", message.clone()))?
            .as_ref()
            .ok_or(EnvError::HostUnreachable)?;
        let key = (owner, scope.workspace_id.clone());
        let mut hosts = self
            .action_hosts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(hosts
            .entry(key)
            .or_insert_with(|| {
                Arc::new(ActionHost::new(
                    provider.source.clone(),
                    self.identity.clone(),
                    workspace_dir(self.identity.run_dir(), scope.workspace_id.as_str()),
                    scope.workspace_id.as_str().to_owned(),
                    host_address.clone(),
                ))
            })
            .clone())
    }

    /// Keep failed hosts registered so a later release can retry cleanup.
    async fn release_action_hosts(&self, owner: ActionOwner) -> ReleaseReport {
        let hosts: Vec<_> = self
            .action_hosts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|((candidate, _), _)| *candidate == owner)
            .map(|(key, host)| (key.clone(), host.clone()))
            .collect();
        let mut report = ReleaseReport::default();
        for (key, host) in hosts {
            match host.teardown().await {
                Ok(removed) => {
                    if removed {
                        report = report.released("action host");
                    }
                    self.action_hosts
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&key);
                }
                Err(error) => {
                    report = report.problem(format!("action host teardown failed: {error}"));
                }
            }
        }
        report
    }

    /// Ends a lease: stops its sandbox, then keeps or deletes it by this
    /// router's retention for `outcome`. The coordinator calls this when
    /// the invocation that owns the lease finishes.
    pub async fn release_lease(
        &self,
        lease: SandboxLeaseId,
        outcome: ScopeOutcome,
    ) -> ReleaseReport {
        let mut report = self.release_action_hosts(ActionOwner::Lease(lease)).await;
        if !report.is_clean() {
            // The action host can still have this workspace mounted. Keep its
            // owning lease intact so release or prune can retry safely.
            return report;
        }
        match self.executor_for_record(lease).await {
            Ok(container) => {
                let ended = container
                    .manager()
                    .release_lease(lease, self.retention, outcome)
                    .await;
                report.merge(ended);
            }
            Err(error) => {
                report = report.problem(format!(
                    "no container executor to release lease {lease}: {error}"
                ));
            }
        }
        report
    }

    /// Reconcile the run's recorded leases with their providers before any
    /// create: the takeover step a coordinator runs when it opens a stored
    /// run for writing. `host` names the leases the host provider holds and
    /// `container` the rest; a provider with no lease recorded on it is not
    /// launched.
    pub async fn reconcile(
        &self,
        host: &[RecordedLease],
        container: &[RecordedLease],
    ) -> ReconcileReport {
        let mut report = ReconcileReport::default();
        if !host.is_empty() {
            match self.host().await {
                Ok(executor) => report.merge(executor.manager().reconcile(host).await),
                Err(error) => report
                    .problems
                    .push(format!("no host executor to reconcile with: {error}")),
            }
        }
        if !container.is_empty() {
            match self.container().await {
                Ok(executor) => report.merge(executor.manager().reconcile(container).await),
                Err(error) => report
                    .problems
                    .push(format!("no container executor to reconcile with: {error}")),
            }
        }
        report
    }

    /// Deletes a recorded lease's sandbox with no live handle: prune.
    /// `workspace_id` is the lease's workspace, the reconcile key for a
    /// record that never learned its resource id.
    pub async fn delete_recorded(
        &self,
        lease: SandboxLeaseId,
        workspace_id: &str,
    ) -> Result<Vec<SandboxId>, EnvError> {
        if !self.simulated
            && self.recorded_provider(lease).await?.as_deref() == Some(SIMULATED_KIND)
        {
            // A dry run's lease: nothing was ever on a provider.
            return Ok(Vec::new());
        }
        let executor = self.executor_for_record(lease).await?;
        // An action host has its own marker and Docker resource. Remove it
        // before the Host provider deletes the workspace, even if an earlier
        // cleanup left a tombstone for the Host lease itself.
        // The container provider is only needed when a marker says one
        // created an action host.
        let mut deleted = if executor.manager().source().kind() == "host" {
            let source = self.provider().map(|provider| &*provider.source);
            remove_recorded(source, &self.identity, workspace_id, None).await?
        } else {
            Vec::new()
        };
        deleted.extend(
            executor
                .manager()
                .delete_recorded(lease, workspace_id)
                .await?,
        );
        Ok(deleted)
    }
}

#[async_trait]
impl Executor for RoutingExecutor {
    async fn acquire(
        &self,
        scope: &ScopeSpec,
        ctx: &AcquireContext,
    ) -> Result<EnvHandle, EnvError> {
        if self.simulated || self.options.backend != SandboxBackend::Host {
            return self.container().await?.acquire(scope, ctx).await;
        }
        match scope.runtime.target {
            RuntimeTarget::HostProcess => {
                let owner = ctx
                    .lease()
                    .map_or(ActionOwner::Scope(scope.id), ActionOwner::Lease);
                let action_host = self.action_host(owner, scope);
                if let Ok(host) = &action_host {
                    host.prepare().await;
                }
                let handle = self.host().await?.acquire(scope, ctx).await?;
                Ok(handle.with_runner(Arc::new(ActionHostRunner::new(
                    action_host,
                    scope,
                    self.gate.clone(),
                ))))
            }
            RuntimeTarget::Container { .. } => self.container().await?.acquire(scope, ctx).await,
        }
    }

    /// Release goes back to the backend whose teardown record the handle
    /// carries: the handle itself says which acquired it.
    async fn release(&self, env: EnvHandle, outcome: ScopeOutcome) -> ReleaseReport {
        let Some(teardown) = env.teardown::<SandboxTeardown>() else {
            return ReleaseReport::default()
                .problem("no backend of this router acquired this environment");
        };
        let executor = if teardown.host {
            self.host.get()
        } else {
            self.container.get()
        };
        let Some(Ok(executor)) = executor else {
            return ReleaseReport::default()
                .problem("the environment has no executor to release it");
        };
        let ended = self
            .release_action_hosts(ActionOwner::Scope(env.scope()))
            .await;
        if !ended.is_clean() {
            // End the holder but retain its workspace while an action host
            // can still mount it. A later acquire/release can retry cleanup.
            executor.manager().release_holder(teardown.lease).await;
            return ended;
        }
        let mut report = executor.release(env, outcome).await;
        report.merge(ended);
        report
    }
}
