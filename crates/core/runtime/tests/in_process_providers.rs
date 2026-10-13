//! The standard runtime over built-in providers linked into this process:
//! sandbox-driver's Host provider through a factory, with no plugin.

use std::fs;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use executor::{AcquireContext, ExecEnv, Executor as _, Retention, ScopeOutcome, ScopeSpec};
use ir::{GraphBuilder, Outcome, RunStatus, RuntimeSpec, ScopeId, Value};
use runtime::steps::{Step, StepCtx};
use runtime::{InProcessProviders, RunOptions, Runtime};
use testkit::RunDir;
use testkit::in_process::{HostFactory, host_providers};

struct CaptureEnvironment(Arc<OnceLock<Arc<dyn ExecEnv>>>);

#[async_trait::async_trait]
impl Step for CaptureEnvironment {
    const NAME: &'static str = "test/capture-environment";
    type Config = ();

    async fn run(&self, (): (), ctx: StepCtx) -> Outcome {
        ctx.env
            .write_file(Path::new("retained"), b"kept")
            .await
            .expect("retained file");
        assert!(self.0.set(ctx.env).is_ok());
        Outcome::success(Value::Null)
    }
}

fn retained(directory: &RunDir) -> RunOptions {
    let mut options = RunOptions::new(directory.path());
    options.retention = Retention::Always;
    options
}

#[tokio::test]
async fn standalone_and_resumed_drivers_run_on_the_built_in_host_and_close_it() {
    for resumed in [false, true] {
        let directory = RunDir::new("in-process-standalone");
        let (providers, host, docker) = host_providers();
        let environment = Arc::new(OnceLock::new());
        let runtime = Runtime::standard()
            .step(CaptureEnvironment(environment.clone()))
            .options(retained(&directory))
            .in_process_providers(providers);
        let mut graph = GraphBuilder::new();
        graph.add_step("capture", ScopeId::new(0), CaptureEnvironment::NAME);
        let graph = graph.build();
        let report = if resumed {
            runtime
                .resume_driver(graph, engine::EventLog::new())
                .expect("resumed driver")
                .0
                .run()
                .await
        } else {
            runtime.run(graph).await.expect("standalone run")
        };
        assert_eq!(report.status, RunStatus::Success);
        assert_eq!(host.connects(), 1, "one Host provider for the run");
        assert_eq!(docker.connects(), 0, "a Host-only run never reaches Docker");
        // The environment outlived the run in this test's hands; the run's
        // gate, not a closed transport, is what refuses it now.
        let error = environment
            .get()
            .expect("captured environment")
            .read_file(Path::new("retained"))
            .await
            .expect_err("a finished run's environment refuses work");
        assert!(error.to_string().contains("finished"), "{error}");
        assert_eq!(
            fs::read(directory.workspace().join("retained")).expect("retained file"),
            b"kept"
        );
    }
}

#[tokio::test]
async fn run_finish_closes_its_environments_and_preserves_other_runs_and_retained_files() {
    let first_dir = RunDir::new("in-process-finish-first");
    let second_dir = RunDir::new("in-process-finish-second");
    let (providers, host, docker) = host_providers();
    let runtime = Runtime::standard()
        .options(retained(&first_dir))
        .in_process_providers(providers);
    let first = runtime.prepare_run(first_dir.path());
    let second = runtime.prepare_run(second_dir.path());
    let first_router = first.sandbox_router().expect("standard router").clone();
    let second_router = second.sandbox_router().expect("standard router").clone();
    let scope = ScopeSpec::new(ScopeId::new(0), "scope-0");
    let first_env = first_router
        .acquire(&scope, &AcquireContext::bare())
        .await
        .expect("first host scope");
    let second_env = second_router
        .acquire(&scope, &AcquireContext::bare())
        .await
        .expect("second host scope");
    assert_eq!(host.connects(), 2, "each run opens its own registry");
    let first_exec = first_env.exec();
    first_exec
        .write_file(Path::new("retained"), b"kept")
        .await
        .expect("retained data");
    assert!(
        first_router
            .release(first_env, ScopeOutcome::Succeeded)
            .await
            .is_clean()
    );
    assert_eq!(
        first_exec
            .read_file(Path::new("retained"))
            .await
            .expect("the run is still open"),
        Some(b"kept".to_vec())
    );

    first.finish().await;

    // Keep the old router and environment alive: the run must refuse them
    // explicitly, because nothing in this process closes on drop.
    assert!(first_exec.read_file(Path::new("retained")).await.is_err());
    assert_eq!(
        fs::read(first_dir.workspace().join("retained")).expect("retained workspace"),
        b"kept"
    );
    assert!(first_dir.path().join("host-registry").is_dir());
    second_env
        .exec()
        .write_file(Path::new("still-live"), b"yes")
        .await
        .expect("other run remains live");
    assert!(
        second_router
            .release(second_env, ScopeOutcome::Succeeded)
            .await
            .is_clean()
    );
    second.finish().await;
    assert_eq!(docker.connects(), 0);
}

#[tokio::test]
async fn a_kind_without_a_factory_fails_at_acquire_instead_of_launching_its_plugin() {
    let directory = RunDir::new("in-process-missing-kind");
    let host = Arc::new(HostFactory::default());
    let runtime = Runtime::standard()
        .options(retained(&directory))
        .in_process_providers(InProcessProviders::new().with(host.clone()));
    let run = runtime.prepare_run(directory.path());
    let router = run.sandbox_router().expect("standard router").clone();
    let mut scope = ScopeSpec::new(ScopeId::new(0), "scope-0");
    scope.runtime = RuntimeSpec::container("alpine:3.20");
    let error = router
        .acquire(&scope, &AcquireContext::bare())
        .await
        .expect_err("no Docker factory");
    assert!(error.to_string().contains("not registered"), "{error}");

    // Host scopes still run: the missing kind is only needed by containers.
    let env = router
        .acquire(
            &ScopeSpec::new(ScopeId::new(1), "scope-1"),
            &AcquireContext::bare(),
        )
        .await
        .expect("host scope");
    assert!(
        router
            .release(env, ScopeOutcome::Succeeded)
            .await
            .is_clean()
    );
    run.finish().await;
    assert_eq!(host.connects(), 1);
}

#[tokio::test]
async fn a_dry_run_connects_no_built_in_provider() {
    let directory = RunDir::new("in-process-dry-run");
    let (providers, host, docker) = host_providers();
    let runtime = Runtime::standard()
        .options(retained(&directory))
        .simulated_sandboxes()
        .in_process_providers(providers);
    let mut graph = GraphBuilder::new();
    graph.add_step("noop", ScopeId::new(0), "noop");
    let report = runtime.run(graph.build()).await.expect("dry run");
    assert_eq!(report.status, RunStatus::Success);
    assert_eq!(host.connects(), 0);
    assert_eq!(docker.connects(), 0);
}
