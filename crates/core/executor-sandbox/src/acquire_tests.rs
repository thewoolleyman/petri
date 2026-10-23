//! Acquisition failures and cancellation must release a standalone sandbox,
//! including a provider create that completes after its caller is gone.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use executor::{
    AcquireContext, EnvError, EnvHandle, Executor, Retention, SandboxLeaseId, ScopeSpec,
};
use sandbox_driver::{
    Capabilities, Capability, Error, EventContext, Exec, Filesystem, Isolation, NetworkPolicy,
    PlatformInfo, ProviderKind, Sandbox, SandboxFilter, SandboxId, SandboxProvider, SandboxSpec,
    SandboxState, SandboxStatus,
};
use testkit::RunDir;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::{FixedProvider, LeaseLedger, LeaseState, MemoryLedger, RunIdentity, SandboxExecutor};

const WAIT: Duration = Duration::from_secs(5);
const LEASE: SandboxLeaseId = SandboxLeaseId::new(0);

enum EnvironmentBehavior {
    Fail,
    Wait,
}

struct FakeSandbox {
    id:                  SandboxId,
    capabilities:        Capabilities,
    environment:         EnvironmentBehavior,
    network:             Mutex<Option<NetworkPolicy>>,
    environment_started: Notify,
    allow_environment:   Notify,
    created:             AtomicUsize,
    deletes:             AtomicUsize,
    deleted:             Notify,
}

#[async_trait]
impl Sandbox for FakeSandbox {
    fn id(&self) -> &SandboxId {
        &self.id
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn describe(&self) -> sandbox_driver::Result<SandboxStatus> {
        let state = if self.deletes.load(Ordering::SeqCst) == 0 {
            SandboxState::Running
        } else {
            SandboxState::Deleted
        };
        let mut status = SandboxStatus::new(self.id.clone(), state);
        status.network = self.network.lock().unwrap().clone();
        Ok(status)
    }

    fn working_directory(&self) -> &str {
        crate::CONTAINER_WORKSPACE
    }

    async fn environment(&self) -> sandbox_driver::Result<BTreeMap<String, String>> {
        self.environment_started.notify_one();
        match self.environment {
            EnvironmentBehavior::Fail => Err(Error::unsupported(Capability::ExecEnvironment)),
            EnvironmentBehavior::Wait => {
                self.allow_environment.notified().await;
                Ok(BTreeMap::new())
            }
        }
    }

    async fn platform_info(&self) -> sandbox_driver::Result<PlatformInfo> {
        panic!("acquisition does not probe the platform")
    }

    async fn start(&self) -> sandbox_driver::Result<()> {
        panic!("a new sandbox does not need recovery")
    }

    async fn stop(&self) -> sandbox_driver::Result<()> {
        panic!("a failed standalone acquisition must delete its sandbox")
    }

    async fn delete(&self) -> sandbox_driver::Result<()> {
        assert_eq!(self.created.load(Ordering::SeqCst), 1);
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.deleted.notify_one();
        Ok(())
    }

    fn exec(&self) -> &dyn Exec {
        panic!("acquisition does not execute a step")
    }

    fn fs(&self) -> &dyn Filesystem {
        panic!("acquisition does not read workspace files")
    }
}

struct FakeProvider {
    kind:              ProviderKind,
    requests:          Mutex<Vec<SandboxSpec>>,
    sandbox:           Arc<FakeSandbox>,
    create_started:    Notify,
    allow_create:      Notify,
    lose_create_reply: AtomicBool,
}

#[async_trait]
impl SandboxProvider for FakeProvider {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }

    fn capabilities(&self) -> &Capabilities {
        self.sandbox.capabilities()
    }

    async fn create(
        &self,
        spec: &SandboxSpec,
        _events: Option<EventContext>,
    ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
        self.requests.lock().unwrap().push(spec.clone());
        self.create_started.notify_one();
        self.allow_create.notified().await;
        self.sandbox.created.fetch_add(1, Ordering::SeqCst);
        if self.lose_create_reply.load(Ordering::SeqCst) {
            return Err(Error::Timeout {
                operation: "creating sandbox".to_owned(),
                elapsed:   Duration::from_secs(1),
            });
        }
        Ok(self.sandbox.clone())
    }

    async fn attach(
        &self,
        id: &SandboxId,
        _events: Option<EventContext>,
    ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
        assert_eq!(id, self.sandbox.id());
        Ok(self.sandbox.clone())
    }

    async fn list(&self, _filter: &SandboxFilter) -> sandbox_driver::Result<Vec<SandboxStatus>> {
        if self.sandbox.created.load(Ordering::SeqCst) > self.sandbox.deletes.load(Ordering::SeqCst)
        {
            Ok(vec![self.sandbox.describe().await?])
        } else {
            Ok(Vec::new())
        }
    }
}

struct Fixture {
    _dir:     RunDir,
    executor: Arc<SandboxExecutor>,
    provider: Arc<FakeProvider>,
    ledger:   Arc<MemoryLedger>,
}

impl Fixture {
    fn new(environment: EnvironmentBehavior) -> Self {
        let dir = RunDir::new("sandbox-acquire-cancel");
        let provider = Arc::new(FakeProvider {
            requests:          Mutex::new(Vec::new()),
            kind:              ProviderKind::try_new("fake")
                .expect("the test provider kind is valid"),
            sandbox:           Arc::new(FakeSandbox {
                id: SandboxId::try_new("sandbox-1").expect("the test sandbox id is valid"),
                capabilities: Capabilities::minimal(Isolation::Container),
                environment,
                network: Mutex::new(None),
                environment_started: Notify::new(),
                allow_environment: Notify::new(),
                created: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
                deleted: Notify::new(),
            }),
            create_started:    Notify::new(),
            allow_create:      Notify::new(),
            lose_create_reply: AtomicBool::new(false),
        });
        let ledger = Arc::new(MemoryLedger::default());
        let executor = Arc::new(SandboxExecutor::new(
            Arc::new(FixedProvider::new(provider.clone())),
            ledger.clone(),
            Arc::new(RunIdentity::for_run_dir(dir.path().to_path_buf())),
            Retention::Always,
            Some(crate::DOCKER_HOST_ALIAS.to_owned()),
            crate::SandboxOptions::default(),
        ));
        Self {
            _dir: dir,
            executor,
            provider,
            ledger,
        }
    }

    fn acquire(&self) -> JoinHandle<Result<EnvHandle, EnvError>> {
        let executor = self.executor.clone();
        tokio::spawn(async move {
            let scope = ScopeSpec::new(ir::ScopeId::new(0), "scope-0")
                .with_runtime(ir::RuntimeSpec::container("test-image"));
            executor.acquire(&scope, &AcquireContext::bare()).await
        })
    }

    async fn assert_deleted(&self) {
        timeout(WAIT, self.provider.sandbox.deleted.notified())
            .await
            .expect("the abandoned sandbox is deleted");
        // Taking the lease lock waits for the delete and ledger confirmation
        // to finish, without racing the provider's notification.
        assert!(self.executor.manager().live(LEASE).await.is_none());
        assert_eq!(self.provider.sandbox.created.load(Ordering::SeqCst), 1);
        assert_eq!(self.provider.sandbox.deletes.load(Ordering::SeqCst), 1);
        let record = self
            .ledger
            .lookup(LEASE)
            .await
            .expect("the ledger reads")
            .expect("the lease exists");
        assert_eq!(record.state, LeaseState::Deleted);
        assert_eq!(record.pending, None);
    }
}

#[tokio::test]
async fn an_environment_error_deletes_a_standalone_sandbox() {
    let fixture = Fixture::new(EnvironmentBehavior::Fail);
    fixture.provider.allow_create.notify_one();
    let error = timeout(WAIT, fixture.acquire())
        .await
        .expect("acquisition reports the environment failure")
        .expect("the acquire task completes")
        .expect_err("the provider cannot report its environment");
    assert!(
        error.to_string().contains("environment could not be read"),
        "{error}"
    );
    fixture.assert_deleted().await;
}

#[tokio::test]
async fn cancellation_during_environment_read_deletes_the_sandbox() {
    let fixture = Fixture::new(EnvironmentBehavior::Wait);
    fixture.provider.allow_create.notify_one();
    let acquire = fixture.acquire();
    timeout(
        WAIT,
        fixture.provider.sandbox.environment_started.notified(),
    )
    .await
    .expect("acquisition reaches the environment read");
    acquire.abort();
    assert!(
        acquire
            .await
            .expect_err("the acquire task is cancelled")
            .is_cancelled()
    );
    fixture.assert_deleted().await;
}

#[tokio::test]
async fn cancellation_during_create_deletes_the_late_sandbox() {
    let fixture = Fixture::new(EnvironmentBehavior::Fail);
    let acquire = fixture.acquire();
    timeout(WAIT, fixture.provider.create_started.notified())
        .await
        .expect("acquisition starts the provider create");
    acquire.abort();
    assert!(
        acquire
            .await
            .expect_err("the acquire task is cancelled")
            .is_cancelled()
    );
    assert_eq!(fixture.provider.sandbox.created.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.sandbox.deletes.load(Ordering::SeqCst), 0);
    // The provider finishes only after its caller has been dropped.
    fixture.provider.allow_create.notify_one();
    fixture.assert_deleted().await;
}

#[tokio::test]
async fn a_lost_create_reply_deletes_the_standalone_sandbox() {
    let fixture = Fixture::new(EnvironmentBehavior::Fail);
    fixture
        .provider
        .lose_create_reply
        .store(true, Ordering::SeqCst);
    fixture.provider.allow_create.notify_one();
    let result = fixture.acquire().await.expect("acquisition task");
    assert!(result.is_err());
    fixture.assert_deleted().await;
}

#[tokio::test]
async fn network_policy_reaches_creation_and_block_requires_provider_confirmation() {
    for (requested, reported, reaches_environment) in [
        (NetworkPolicy::ProviderDefault, None, true),
        (NetworkPolicy::AllowAll, Some(NetworkPolicy::AllowAll), true),
        (NetworkPolicy::Block, Some(NetworkPolicy::Block), true),
        (NetworkPolicy::Block, Some(NetworkPolicy::AllowAll), false),
        (NetworkPolicy::Block, None, false),
    ] {
        let mut fixture = Fixture::new(EnvironmentBehavior::Fail);
        Arc::get_mut(&mut fixture.executor).unwrap().options.network = requested.clone();
        *fixture.provider.sandbox.network.lock().unwrap() = reported;
        fixture.provider.allow_create.notify_one();
        let error = fixture
            .acquire()
            .await
            .unwrap()
            .expect_err("acquisition stops at the scripted boundary");
        assert_eq!(
            fixture.provider.requests.lock().unwrap()[0].network,
            requested
        );
        let message = error.to_string();
        assert!(
            message.contains(if reaches_environment {
                "environment could not be read"
            } else {
                "does not report blocked networking"
            }),
            "{message}"
        );
        fixture.assert_deleted().await;
    }
}

#[tokio::test]
async fn a_reused_sandbox_cannot_bypass_the_block_policy() {
    let mut fixture = Fixture::new(EnvironmentBehavior::Wait);
    Arc::get_mut(&mut fixture.executor).unwrap().options.network = NetworkPolicy::Block;
    *fixture.provider.sandbox.network.lock().unwrap() = Some(NetworkPolicy::Block);
    fixture.provider.allow_create.notify_one();
    fixture.provider.sandbox.allow_environment.notify_one();
    let scope = ScopeSpec::new(ir::ScopeId::new(0), "scope-0")
        .with_runtime(ir::RuntimeSpec::container("test-image"));
    let ctx = AcquireContext::bare().with_lease(LEASE);
    let handle = fixture.executor.acquire(&scope, &ctx).await.unwrap();
    fixture
        .executor
        .release(handle, executor::ScopeOutcome::Succeeded)
        .await;

    // Reacquisition returns the cached sandbox without calling create or
    // rebuilding its spec. It must still confirm that networking is blocked.
    *fixture.provider.sandbox.network.lock().unwrap() = Some(NetworkPolicy::AllowAll);
    let error = fixture.executor.acquire(&scope, &ctx).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not report blocked networking")
    );
    assert_eq!(fixture.provider.requests.lock().unwrap().len(), 1);
    fixture
        .executor
        .manager()
        .release_lease(LEASE, Retention::Never, executor::ScopeOutcome::Failed)
        .await;
    fixture.assert_deleted().await;
}

#[tokio::test]
async fn a_docker_backend_sends_the_runs_resources() {
    let mut resources = sandbox_driver::Resources::default();
    resources.cpu_cores = Some(4);
    resources.memory_mb = Some(7629);
    let mut fixture = Fixture::new(EnvironmentBehavior::Fail);
    {
        let options = &mut Arc::get_mut(&mut fixture.executor).unwrap().options;
        options.backend = crate::SandboxBackend::Docker;
        options.resources = Some(resources);
    }
    fixture.provider.allow_create.notify_one();
    fixture
        .acquire()
        .await
        .unwrap()
        .expect_err("acquisition stops at the scripted boundary");
    let request = fixture.provider.requests.lock().unwrap()[0].clone();
    assert_eq!(
        request.resources.cpu_cores,
        Some(4),
        "the run's CPU reaches Docker"
    );
    assert_eq!(
        request.resources.memory_mb,
        Some(7629),
        "the run's memory reaches Docker"
    );
    fixture.assert_deleted().await;
}
