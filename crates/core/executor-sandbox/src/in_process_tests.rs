//! The in-process source: connection, health, identity, and closing.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use sandbox_driver::{
    Capabilities, Capability, Error as DriverError, EventContext, HealthStatus, Isolation,
    ProviderHealth, ProviderKind, Sandbox, SandboxFilter, SandboxId, SandboxProvider,
    SandboxSource, SandboxSpec, SandboxState, SandboxStatus,
};
use sandbox_driver_host::HostProvider;
use testkit::RunDir;
use tokio::sync::Notify;
use tokio::time::sleep;

use crate::in_process::{
    InProcessProviders, InProcessSource, ProviderContext, ProviderFactory, ProviderNetwork,
};
use crate::plugin::ProviderSource;
use crate::registration::{ProviderRegistrations, ProviderTransport};
use crate::{DOCKER_HOST_ALIAS, RUN_LABEL, fingerprint};

/// A provider with a scripted kind and health report; it creates nothing.
struct ScriptedProvider {
    kind:         ProviderKind,
    capabilities: Capabilities,
    health:       ProviderHealth,
}

#[async_trait]
impl SandboxProvider for ScriptedProvider {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn create(
        &self,
        _: &SandboxSpec,
        _: Option<EventContext>,
    ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
        Err(DriverError::unsupported(Capability::LifecycleUndelete))
    }

    async fn attach(
        &self,
        _: &SandboxId,
        _: Option<EventContext>,
    ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
        Err(DriverError::unsupported(Capability::LifecycleUndelete))
    }

    async fn list(&self, _: &SandboxFilter) -> sandbox_driver::Result<Vec<SandboxStatus>> {
        Ok(Vec::new())
    }

    async fn health(&self) -> sandbox_driver::Result<ProviderHealth> {
        Ok(self.health.clone())
    }
}

/// Connects scripted providers: each connect takes the next health
/// report, and the last one repeats.
struct ScriptedFactory {
    kind:          &'static str,
    /// The kind the connected provider reports, when it lies.
    provider_kind: &'static str,
    reports:       Vec<ProviderHealth>,
    region:        Option<&'static str>,
    connects:      AtomicUsize,
    /// Held by a test to keep a connection in flight.
    release:       Option<Arc<Notify>>,
}

impl ScriptedFactory {
    fn new(kind: &'static str, reports: Vec<ProviderHealth>) -> Self {
        Self {
            kind,
            provider_kind: kind,
            reports,
            region: None,
            connects: AtomicUsize::new(0),
            release: None,
        }
    }

    fn connects(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ProviderFactory for ScriptedFactory {
    fn kind(&self) -> &str {
        self.kind
    }

    fn fingerprint_seed(&self, _: &ProviderContext) -> String {
        format!("{}:seed", self.kind)
    }

    fn region(&self) -> Option<&str> {
        self.region
    }

    fn network(&self) -> ProviderNetwork {
        ProviderNetwork::none()
    }

    async fn connect(
        &self,
        _: &ProviderContext,
    ) -> sandbox_driver::Result<Arc<dyn SandboxProvider>> {
        let attempt = self.connects.fetch_add(1, Ordering::SeqCst);
        if let Some(release) = &self.release {
            release.notified().await;
        }
        let health = self.reports[attempt.min(self.reports.len() - 1)].clone();
        Ok(Arc::new(ScriptedProvider {
            kind: ProviderKind::try_new(self.provider_kind).expect("kind"),
            capabilities: Capabilities::minimal(Isolation::Container),
            health,
        }))
    }
}

fn healthy(identity: Option<&str>) -> ProviderHealth {
    let mut health = ProviderHealth::new(HealthStatus::Ok);
    health.identity = identity.map(str::to_owned);
    health
}

fn source(factory: &Arc<ScriptedFactory>) -> InProcessSource {
    InProcessSource::new(
        factory.clone(),
        ProviderContext::new("run-1", Path::new("/runs/run-1"), None),
    )
}

#[tokio::test]
async fn concurrent_callers_share_one_connection_and_its_verified_fingerprint() {
    let release = Arc::new(Notify::new());
    let factory = Arc::new(ScriptedFactory {
        release: Some(release.clone()),
        ..ScriptedFactory::new("docker", vec![healthy(Some("daemon-1"))])
    });
    let source = Arc::new(source(&factory));
    assert_eq!(
        source.fingerprint(),
        "docker:seed",
        "the seed before connecting"
    );
    let callers: Vec<_> = (0..4)
        .map(|_| {
            let source = source.clone();
            tokio::spawn(async move { source.current().await.map(|(_, generation)| generation) })
        })
        .collect();
    sleep(Duration::from_millis(20)).await;
    release.notify_one();
    for caller in callers {
        assert_eq!(caller.await.expect("caller").expect("served"), 1);
    }
    assert_eq!(factory.connects(), 1);
    assert_eq!(source.fingerprint(), "docker:seed:daemon-1");
    assert_eq!(source.kind(), "docker");
}

#[tokio::test]
async fn an_unhealthy_backend_is_refused_and_retried_by_the_next_call() {
    let mut unauthorized = ProviderHealth::new(HealthStatus::Unauthorized);
    unauthorized.message = Some("the key was revoked".to_owned());
    unauthorized.missing_permissions = vec!["write:sandboxes".to_owned()];
    let factory = Arc::new(ScriptedFactory::new("docker", vec![
        ProviderHealth::new(HealthStatus::Unreachable),
        unauthorized,
        ProviderHealth::new(HealthStatus::Unknown),
    ]));
    let source = source(&factory);
    let unreachable = source
        .current()
        .await
        .err()
        .expect("unreachable is refused");
    assert!(
        unreachable.to_string().contains("Unreachable"),
        "{unreachable}"
    );
    let refused = source
        .current()
        .await
        .err()
        .expect("unauthorized is refused");
    assert!(refused.to_string().contains("revoked"), "{refused}");
    assert!(refused.to_string().contains("write:sandboxes"), "{refused}");
    assert_eq!(source.fingerprint(), "docker:seed", "nothing verified yet");
    source
        .current()
        .await
        .expect("an unknown health report serves");
    assert_eq!(factory.connects(), 3);
    assert_eq!(source.fingerprint(), "docker:seed");
}

#[tokio::test]
async fn a_provider_of_another_kind_is_refused() {
    let factory = Arc::new(ScriptedFactory {
        provider_kind: "daytona",
        ..ScriptedFactory::new("docker", vec![healthy(None)])
    });
    let error = source(&factory).current().await.err().expect("refused");
    assert!(error.to_string().contains("`daytona`"), "{error}");
}

#[tokio::test]
async fn daytona_must_verify_its_organization_before_serving() {
    let factory = Arc::new(ScriptedFactory::new("daytona", vec![healthy(None)]));
    let error = source(&factory).current().await.err().expect("refused");
    assert!(error.to_string().contains("identity"), "{error}");

    let factory = Arc::new(ScriptedFactory {
        region: Some("eu"),
        ..ScriptedFactory::new("daytona", vec![healthy(Some("organization:org-1"))])
    });
    let source = source(&factory);
    source.current().await.expect("verified");
    assert_eq!(source.fingerprint(), "daytona:seed:organization:org-1");
    assert_eq!(source.region(), Some("eu"));
}

#[tokio::test]
async fn a_closed_source_never_serves() {
    let factory = Arc::new(ScriptedFactory::new("docker", vec![healthy(None)]));
    let closed_first = source(&factory);
    closed_first.shutdown().await;
    assert!(closed_first.current().await.is_err());
    assert_eq!(factory.connects(), 0, "a closed source does not connect");

    let serving = source(&factory);
    serving.current().await.expect("served");
    serving.shutdown().await;
    serving.shutdown().await;
    assert!(serving.current().await.is_err());
}

#[tokio::test]
async fn a_shutdown_racing_the_connection_wins() {
    let release = Arc::new(Notify::new());
    let factory = Arc::new(ScriptedFactory {
        release: Some(release.clone()),
        ..ScriptedFactory::new("docker", vec![healthy(None)])
    });
    let source = Arc::new(source(&factory));
    let connecting = tokio::spawn({
        let source = source.clone();
        async move { source.current().await.map(|_| ()) }
    });
    sleep(Duration::from_millis(20)).await;
    source.shutdown().await;
    release.notify_one();
    assert!(connecting.await.expect("caller").is_err());
}

#[test]
fn linked_factories_serve_the_kind_they_build_and_nothing_else() {
    let providers = ProviderRegistrations::linked(InProcessProviders::new().with(Arc::new(
        ScriptedFactory::new("daytona", vec![healthy(None)]),
    )));
    let missing = providers
        .get("docker")
        .err()
        .expect("no docker factory, and no plugin in its place");
    assert!(missing.contains("not registered"), "{missing}");
    assert!(
        matches!(
            providers.get("daytona"),
            Ok(ProviderTransport::Linked(factory)) if factory.kind() == "daytona"
        ),
        "the daytona factory serves its kind"
    );
}

#[test]
fn networks_follow_the_daemon_they_describe() {
    let local = ProviderNetwork::docker(None, None);
    assert_eq!(
        local.host_address().unwrap().as_deref(),
        Some(DOCKER_HOST_ALIAS)
    );
    assert!(local.supports_host_workspace());

    let remote = ProviderNetwork::docker(Some("tcp://10.0.0.5:2376"), None);
    assert!(remote.host_address().is_err());
    assert!(!remote.supports_host_workspace());
    let configured = ProviderNetwork::docker(Some("tcp://10.0.0.5:2376"), Some("petri.internal"));
    assert_eq!(
        configured.host_address().unwrap().as_deref(),
        Some("petri.internal")
    );
    assert!(!configured.supports_host_workspace());

    assert_eq!(
        ProviderNetwork::host().host_address().unwrap().as_deref(),
        Some("127.0.0.1")
    );
    assert_eq!(ProviderNetwork::none().host_address().unwrap(), None);
}

#[test]
fn verified_fingerprints_append_a_reported_identity() {
    let health = healthy(Some("organization:org-1"));
    assert_eq!(
        fingerprint::verified("docker", "docker:default", &healthy(None)).unwrap(),
        "docker:default"
    );
    assert_eq!(
        fingerprint::verified("daytona", "daytona:u:o:t", &health).unwrap(),
        "daytona:u:o:t:organization:org-1"
    );
    assert!(fingerprint::verified("daytona", "daytona:u:o:t", &healthy(Some(" "))).is_err());
}

/// sandbox-driver's Host provider over the run's registry.
struct HostFactory;

#[async_trait]
impl ProviderFactory for HostFactory {
    fn kind(&self) -> &'static str {
        "host"
    }

    fn fingerprint_seed(&self, context: &ProviderContext) -> String {
        fingerprint::host(context.host_registry().expect("a Host registry"))
    }

    fn network(&self) -> ProviderNetwork {
        ProviderNetwork::host()
    }

    async fn connect(
        &self,
        context: &ProviderContext,
    ) -> sandbox_driver::Result<Arc<dyn SandboxProvider>> {
        let registry = context.host_registry().expect("a Host registry");
        Ok(Arc::new(HostProvider::with_registry(registry).await?))
    }
}

#[tokio::test]
async fn shutdown_stops_the_runs_host_sandboxes_that_are_still_running() {
    let dir = RunDir::new("in-process-host-shutdown");
    let registry = dir.path().join("host-registry");
    fs::create_dir_all(&registry).expect("registry");
    let source = InProcessSource::new(
        Arc::new(HostFactory),
        ProviderContext::new("run-1", dir.path(), Some(registry)),
    );
    let (provider, _) = source.current().await.expect("connects");
    let create = |run: &'static str, name: &'static str| {
        let provider = provider.clone();
        let workspace = dir.path().join(name);
        async move {
            fs::create_dir_all(&workspace).expect("workspace");
            let spec = SandboxSpec::new(SandboxSource::HostDirectory)
                .name(name)
                .working_directory(workspace.to_string_lossy())
                .label(RUN_LABEL, run);
            provider.create(&spec, None).await.expect("created")
        }
    };
    // A lease whose release failed, one already stopped, and another run's.
    let leaked = create("run-1", "leaked").await;
    let stopped = create("run-1", "stopped").await;
    stopped.stop().await.expect("stopped");
    let other = create("run-2", "other").await;

    source.shutdown().await;

    let states: Vec<_> = provider
        .list(&SandboxFilter::default())
        .await
        .expect("listed")
        .into_iter()
        .map(|status| (status.id, status.state))
        .collect();
    let state = |id: &SandboxId| {
        states
            .iter()
            .find(|(listed, _)| listed == id)
            .map(|(_, state)| *state)
            .expect("listed")
    };
    assert_eq!(state(leaked.id()), SandboxState::Stopped);
    assert_eq!(state(stopped.id()), SandboxState::Stopped);
    assert_ne!(
        state(other.id()),
        SandboxState::Stopped,
        "another run's sandbox is left alone"
    );
    assert!(
        source.current().await.is_err(),
        "the closed source never serves"
    );
}

#[tokio::test]
async fn only_a_plugin_ends_admitted_work_by_shutting_down() {
    let factory = Arc::new(ScriptedFactory::new("docker", vec![healthy(None)]));
    assert!(!source(&factory).ends_work_on_shutdown());
}
