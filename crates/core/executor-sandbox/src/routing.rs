//! An [`Executor`] that routes each scope to the provider its placement
//! needs: one [`SandboxExecutor`] per provider kind, over the kind's plugin,
//! a built-in linked into this process through [`InProcessProviders`], or a
//! plugin registered for a custom kind ([`ProviderRegistrations`]), or over
//! the in-process [`SimulatedProvider`] for a dry run. It is the
//! composition the runtime registers for a run.

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
use crate::registration::{ProviderRegistrations, ProviderTransport};
use crate::run::{RunIdentity, workspace_dir};
use crate::simulated::{SIMULATED_KIND, SimulatedProvider};
use crate::{SandboxBackend, SandboxExecutor, SandboxOptions, SandboxTeardown};

/// The provider kind container scopes go to.
pub const CONTAINER_KIND: &str = "docker";

/// The provider kind host-process scopes go to.
const HOST_KIND: &str = "host";

/// What ends a host scope's action host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ActionOwner {
    Lease(SandboxLeaseId),
    Scope(ir::ScopeId),
}

/// One provider kind as this run reaches it: its executor, and how its
/// sandboxes reach this machine.
struct Bound {
    executor: Arc<SandboxExecutor>,
    network:  ProviderNetwork,
}

type Slot = Arc<OnceCell<Result<Arc<Bound>, String>>>;

/// Routes scopes to one [`SandboxExecutor`] per provider kind.
///
/// A host-process scope runs as real processes on this machine, with the
/// sentinel and crash fence a bare process needs; a container scope runs on
/// the Docker provider, reached through its plugin. Each provider is
/// reached on the first scope that needs it, so a host-only run never
/// touches a daemon, and a scope whose provider cannot be reached fails
/// routably at acquire. A lease recorded earlier goes back to the provider
/// its record names, whatever backend the run is configured for now.
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
    providers:    ProviderRegistrations,
    /// A source a caller handed in, serving every kind but Host: tests
    /// over a fake or fixed provider, and a dry run's simulated provider.
    fixed:        Option<Arc<dyn ProviderSource>>,
    /// One executor per provider kind, built on first use. A configuration
    /// error is remembered: every scope on that kind then fails routably
    /// with it.
    bound:        Mutex<BTreeMap<String, Slot>>,
    /// The action hosts of host scopes, each keyed by what ends it: the
    /// lease that owns it, or the scope itself when no coordinator named one.
    action_hosts: Mutex<BTreeMap<(ActionOwner, WorkspaceId), Arc<ActionHost>>>,
    ledger:       OnceLock<Arc<dyn LeaseLedger>>,
    identity:     Arc<RunIdentity>,
    retention:    Retention,
    options:      SandboxOptions,
    /// Every scope goes to the simulated provider: a dry run.
    simulated:    bool,
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
        let mut router = Self::over(run_dir, retention, None);
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
        Self::over(run_dir, retention, Some(source))
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
        let mut router = Self::over(run_dir, Retention::Never, Some(source));
        router.simulated = true;
        router
    }

    fn over(
        run_dir: impl Into<PathBuf>,
        retention: Retention,
        fixed: Option<Arc<dyn ProviderSource>>,
    ) -> Self {
        let run_dir = run_dir.into();
        Self {
            providers: ProviderRegistrations::standalone(),
            fixed,
            bound: Mutex::new(BTreeMap::new()),
            action_hosts: Mutex::new(BTreeMap::new()),
            ledger: OnceLock::new(),
            identity: Arc::new(RunIdentity::for_run_dir(run_dir)),
            retention,
            options: SandboxOptions::default(),
            simulated: false,
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
    pub fn with_in_process(self, providers: InProcessProviders) -> Self {
        self.with_providers(ProviderRegistrations::linked(providers))
    }

    /// Reach exactly the providers `providers` registers: linked built-ins,
    /// first-party plugins, and plugins registered for custom kinds. A kind
    /// with no entry fails routably at acquire; no plugin is launched in
    /// its place.
    #[must_use]
    pub fn with_providers(mut self, providers: ProviderRegistrations) -> Self {
        self.providers = providers;
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
        let fixed_kind = self.fixed.as_ref().map(|source| source.kind().to_owned());
        let bound: Vec<(String, Arc<Bound>)> = self
            .bound
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(|(kind, slot)| {
                let bound = slot.get()?.as_ref().ok()?;
                Some((kind.clone(), bound.clone()))
            })
            .collect();
        // A plugin's closing transport ends its in-flight work; any other
        // source needs admitted work to settle first.
        let settles = bound
            .iter()
            .any(|(_, bound)| !bound.executor.manager().source().ends_work_on_shutdown());
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
        for (kind, bound) in bound {
            if fixed_kind.as_deref() == Some(kind.as_str()) {
                continue;
            }
            bound.executor.manager().source().shutdown().await;
        }
    }

    /// Provider identity for the lease reservation, before resource creation.
    pub fn provider_kind_for(&self, runtime: &ir::RuntimeSpec) -> &str {
        if !self.simulated
            && self.options.backend == SandboxBackend::Host
            && matches!(runtime.target, RuntimeTarget::HostProcess)
        {
            return HOST_KIND;
        }
        self.default_kind()
    }

    /// The kind a scope goes to when it is not a host-process scope on the
    /// Host backend, and the kind a lease with no recorded provider is
    /// looked for on.
    fn default_kind(&self) -> &str {
        if self.simulated {
            return SIMULATED_KIND;
        }
        match &self.fixed {
            Some(source) => source.kind(),
            None => match &self.options.backend {
                SandboxBackend::Host | SandboxBackend::Docker => CONTAINER_KIND,
                SandboxBackend::Daytona => "daytona",
                SandboxBackend::Registered(kind) => kind.as_str(),
            },
        }
    }

    /// The kind `kind` is served under: a supplied source serves every
    /// kind but Host, under its own name.
    fn route<'a>(&'a self, kind: &'a str) -> &'a str {
        match &self.fixed {
            Some(source) if kind != HOST_KIND => source.kind(),
            _ => kind,
        }
    }

    /// `kind`'s executor for this run, built on first use.
    async fn bound(&self, kind: &str) -> Result<Arc<Bound>, String> {
        let kind = self.route(kind);
        let slot = self
            .bound
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(kind.to_owned())
            .or_default()
            .clone();
        slot.get_or_init(|| self.bind(kind)).await.clone()
    }

    async fn executor(&self, kind: &str) -> Result<Arc<Bound>, EnvError> {
        self.bound(kind)
            .await
            .map_err(|message| EnvError::backend(self.route(kind), "configure", message))
    }

    /// The executor for `kind` if this run has built it; nothing is built
    /// to answer.
    fn built(&self, kind: &str) -> Option<Arc<Bound>> {
        self.bound
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(self.route(kind))?
            .get()?
            .as_ref()
            .ok()
            .cloned()
    }

    /// Builds `kind`'s source and executor. Nothing here contacts a
    /// provider: the source connects on its first call.
    async fn bind(&self, kind: &str) -> Result<Arc<Bound>, String> {
        let ledger = self
            .ledger
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(MemoryLedger::default()) as Arc<dyn LeaseLedger>);
        if let Some(source) = &self.fixed
            && kind != HOST_KIND
            && kind == source.kind()
        {
            if self.simulated {
                // A simulated sandbox offers no route to this machine and
                // mounts no workspace: nothing runs in it.
                let executor =
                    SandboxExecutor::simulated(source.clone(), ledger, Arc::clone(&self.identity))
                        .with_gate(self.gate.clone());
                return Ok(Arc::new(Bound {
                    executor: Arc::new(executor),
                    network:  ProviderNetwork::from_parts(Ok(None), false),
                }));
            }
            let network =
                ProviderNetwork::from_parts(Ok(Some(crate::DOCKER_HOST_ALIAS.to_owned())), true);
            return Ok(Arc::new(Bound {
                executor: Arc::new(self.executor_over(source.clone(), ledger, &network)?),
                network,
            }));
        }
        if self.simulated {
            return Err(format!(
                "a dry run launches no provider plugin; a lease the {kind} provider holds is left \
                 as recorded"
            ));
        }
        let transport = self.providers.get(kind)?;
        let host_registry = if kind == HOST_KIND {
            let directory = self.identity.run_dir().join("host-registry");
            fs::create_dir_all(&directory)
                .await
                .map_err(|error| error.to_string())?;
            Some(
                fs::canonicalize(directory)
                    .await
                    .map_err(|error| error.to_string())?,
            )
        } else {
            None
        };
        let (source, network, process_spec): (Arc<dyn ProviderSource>, ProviderNetwork, _) =
            match transport {
                ProviderTransport::Linked(factory) => {
                    let network = factory.network();
                    let context = ProviderContext::new(
                        self.identity.run_id(),
                        self.identity.run_dir(),
                        host_registry,
                    );
                    (
                        Arc::new(InProcessSource::new(factory.clone(), context)),
                        network,
                        None,
                    )
                }
                ProviderTransport::EnvPlugin => {
                    let settings = PluginSettings::from_env(kind, self.options.plugin_dev)
                        .map_err(|error| error.to_string())?;
                    if let Some(directory) = host_registry {
                        (
                            Arc::new(PluginSource::new(settings.with_host_registry(&directory))),
                            ProviderNetwork::from_parts(Ok(Some("127.0.0.1".to_owned())), false),
                            None,
                        )
                    } else {
                        let network = ProviderNetwork::from_parts(
                            settings.host_address().map_err(|error| error.to_string()),
                            settings.supports_host_workspace(),
                        );
                        (Arc::new(PluginSource::new(settings)), network, None)
                    }
                }
                // A registered provider's sandboxes have no route back to
                // this machine and mount no Host workspace: ordinary
                // process scopes need neither.
                ProviderTransport::Plugin(plugin) => (
                    Arc::new(PluginSource::new(plugin.settings.clone())),
                    ProviderNetwork::from_parts(Ok(None), false),
                    Some(plugin.process_spec.clone()),
                ),
            };
        let mut executor = self.executor_over(source, ledger, &network)?;
        if let Some(template) = process_spec {
            executor = executor.with_process_spec(template);
        }
        Ok(Arc::new(Bound {
            executor: Arc::new(executor),
            network,
        }))
    }

    fn executor_over(
        &self,
        source: Arc<dyn ProviderSource>,
        ledger: Arc<dyn LeaseLedger>,
        network: &ProviderNetwork,
    ) -> Result<SandboxExecutor, String> {
        Ok(SandboxExecutor::new(
            source,
            ledger,
            Arc::clone(&self.identity),
            self.retention,
            network.host_address()?,
            self.options.clone(),
        )
        .with_gate(self.gate.clone()))
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

    /// The executor of the provider `lease` is recorded on; the run's
    /// default kind when no record names one.
    async fn executor_for_record(&self, lease: SandboxLeaseId) -> Result<Arc<Bound>, EnvError> {
        let kind = self.recorded_provider(lease).await?;
        let kind = kind.as_deref().unwrap_or_else(|| self.default_kind());
        // A dry run's lease under a real router: no provider holds it, so
        // no plugin is launched to look for it.
        if kind == SIMULATED_KIND && !self.simulated {
            return Err(EnvError::backend(
                SIMULATED_KIND,
                "lookup",
                format!("lease {lease} was simulated by a dry run; no provider holds it"),
            ));
        }
        self.executor(kind).await
    }

    /// The name prefix every sandbox this run owns starts with,
    /// `petri-<run id>-`, for a leak check after release.
    pub fn container_prefix(&self) -> String {
        self.identity.container_prefix()
    }

    async fn action_host(
        &self,
        owner: ActionOwner,
        scope: &ScopeSpec,
    ) -> Result<Arc<ActionHost>, EnvError> {
        let container = self.executor(CONTAINER_KIND).await?;
        if !container.network.supports_host_workspace() {
            return Err(EnvError::backend(
                CONTAINER_KIND,
                "one-shot",
                "Docker actions in Host jobs require a local Docker daemon to mount their workspace; \
                 use --backend docker for a remote daemon",
            ));
        }
        let host_address = container
            .network
            .host_address()
            .map_err(|message| EnvError::backend(CONTAINER_KIND, "configure", message))?
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
                    container.executor.manager().source().clone(),
                    self.identity.clone(),
                    workspace_dir(self.identity.run_dir(), scope.workspace_id.as_str()),
                    scope.workspace_id.as_str().to_owned(),
                    host_address,
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
            Ok(bound) => {
                let ended = bound
                    .executor
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
    /// run for writing. Each lease goes to the provider its record names; a
    /// provider with no lease recorded on it is not launched.
    pub async fn reconcile(&self, leases: &[RecordedLease]) -> ReconcileReport {
        let mut report = ReconcileReport::default();
        let mut groups: BTreeMap<String, Vec<RecordedLease>> = BTreeMap::new();
        for recorded in leases {
            match self.recorded_provider(recorded.lease).await {
                Ok(kind) => groups
                    .entry(kind.unwrap_or_else(|| self.default_kind().to_owned()))
                    .or_default()
                    .push(recorded.clone()),
                Err(error) => report.problems.push(error.to_string()),
            }
        }
        for (kind, group) in groups {
            match self.executor(&kind).await {
                Ok(bound) => report.merge(bound.executor.manager().reconcile(&group).await),
                Err(error) => report
                    .problems
                    .push(format!("no {kind} executor to reconcile with: {error}")),
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
        let bound = self.executor_for_record(lease).await?;
        // An action host has its own marker and Docker resource. Remove it
        // before the Host provider deletes the workspace, even if an earlier
        // cleanup left a tombstone for the Host lease itself.
        // The container provider is only needed when a marker says one
        // created an action host.
        let mut deleted = if bound.executor.manager().source().kind() == HOST_KIND {
            let container = self.bound(CONTAINER_KIND).await;
            let source = container
                .as_ref()
                .map(|container| &**container.executor.manager().source())
                .map_err(|message| EnvError::backend(CONTAINER_KIND, "configure", message.clone()));
            remove_recorded(source, &self.identity, workspace_id, None).await?
        } else {
            Vec::new()
        };
        deleted.extend(
            bound
                .executor
                .manager()
                .delete_recorded(lease, workspace_id)
                .await?,
        );
        Ok(deleted)
    }
}

#[async_trait]
impl Executor for RoutingExecutor {
    /// A scope goes where its placement puts it, unless its lease is
    /// already recorded on a provider: then it goes back there, whatever
    /// the run is configured for now, so a resume reaches the sandbox the
    /// lease holds. A dry run places every scope on the simulated provider.
    async fn acquire(
        &self,
        scope: &ScopeSpec,
        ctx: &AcquireContext,
    ) -> Result<EnvHandle, EnvError> {
        let recorded = match ctx.lease() {
            Some(lease) if !self.simulated => self.recorded_provider(lease).await?,
            _ => None,
        };
        let kind = recorded
            .as_deref()
            .unwrap_or_else(|| self.provider_kind_for(&scope.runtime));
        if kind == SIMULATED_KIND && !self.simulated {
            return Err(EnvError::backend(
                SIMULATED_KIND,
                "acquire",
                "the lease was simulated by a dry run; no provider holds it",
            ));
        }
        if kind != HOST_KIND {
            return self
                .executor(kind)
                .await?
                .executor
                .acquire(scope, ctx)
                .await;
        }
        let owner = ctx
            .lease()
            .map_or(ActionOwner::Scope(scope.id), ActionOwner::Lease);
        let action_host = self.action_host(owner, scope).await;
        if let Ok(host) = &action_host {
            host.prepare().await;
        }
        let handle = self
            .executor(HOST_KIND)
            .await?
            .executor
            .acquire(scope, ctx)
            .await?;
        Ok(handle.with_runner(Arc::new(ActionHostRunner::new(
            action_host,
            scope,
            self.gate.clone(),
        ))))
    }

    /// Release goes back to the provider whose teardown record the handle
    /// carries: the handle itself says which acquired it.
    async fn release(&self, env: EnvHandle, outcome: ScopeOutcome) -> ReleaseReport {
        let Some(teardown) = env.teardown::<SandboxTeardown>() else {
            return ReleaseReport::default()
                .problem("no backend of this router acquired this environment");
        };
        let Some(bound) = self.built(&teardown.provider) else {
            return ReleaseReport::default()
                .problem("the environment has no executor to release it");
        };
        let ended = self
            .release_action_hosts(ActionOwner::Scope(env.scope()))
            .await;
        if !ended.is_clean() {
            // End the holder but retain its workspace while an action host
            // can still mount it. A later acquire/release can retry cleanup.
            bound
                .executor
                .manager()
                .release_holder(teardown.lease)
                .await;
            return ended;
        }
        let mut report = bound.executor.release(env, outcome).await;
        report.merge(ended);
        report
    }
}
