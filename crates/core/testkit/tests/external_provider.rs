//! A provider of an unfamiliar kind, registered as a plugin and reached
//! through the real stdio protocol: ordinary process scopes run on it, and
//! resume, release, and prune go back to it by the lease's record.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use executor::{
    AcquireContext, Executor as _, ProcessSpec, Retention, SandboxLeaseId, ScopeOutcome, ScopeSpec,
};
use executor_sandbox::{
    LeaseLedger as _, LeaseState, MemoryLedger, PluginSettings, PluginSource,
    ProviderRegistrations, RecordedLease, RoutingExecutor, SandboxOptions,
};
use ir::ScopeId;
use sandbox_driver::{NetworkPolicy, ProviderKind, SandboxSource, SandboxSpec, WorkspaceOwnership};
use sandbox_driver_protocol::PluginConfig;
use testkit::RunDir;
use testkit::in_process::{HostFactory, UnusedFactory};

const LEASE: SandboxLeaseId = SandboxLeaseId::new(7);
const KIND: &str = "fixture-provider";

/// The fixture plugin as a registration: launched with its explicit
/// arguments and environment, reporting `identity`, over a backend
/// directory under `dir`.
fn plugin_config(dir: &RunDir, identity: Option<&str>, kind: &str) -> PluginConfig {
    let mut config = PluginConfig::new(ProviderKind::try_new(kind).expect("a valid kind"));
    config.path = Some(env!("CARGO_BIN_EXE_fixture-provider").into());
    config.args = vec!["--fixture".to_owned()];
    config.dev = true;
    config
        .env
        .insert("FIXTURE_KIND".to_owned(), kind.to_owned());
    config.env.insert(
        "FIXTURE_ROOT".to_owned(),
        dir.path().join("backend").to_string_lossy().into_owned(),
    );
    config
        .env
        .insert("PATH".to_owned(), "/usr/bin:/bin".to_owned());
    if let Some(identity) = identity {
        config
            .env
            .insert("FIXTURE_IDENTITY".to_owned(), identity.to_owned());
    }
    config
}

/// The template the fixture's process scopes are built from: a managed
/// workspace under `dir`, with an environment value, a label, and an opaque
/// option for the provider, all of which the fixture must see unchanged.
fn template(dir: &RunDir) -> SandboxSpec {
    let mut spec = SandboxSpec::new(SandboxSource::HostDirectory)
        .working_directory(dir.path().join("workspace").to_string_lossy())
        .env_var("TEMPLATE_ENV", "template-value")
        .label("custom-label", "preserved")
        .provider_config(serde_json::json!({"opaque": "preserved"}));
    spec.workspace_ownership = Some(WorkspaceOwnership::Managed);
    spec
}

fn register(
    providers: &mut ProviderRegistrations,
    dir: &RunDir,
    identity: Option<&str>,
    kind: &str,
) {
    providers
        .register_plugin(plugin_config(dir, identity, kind), template(dir))
        .expect("a valid registration");
}

/// A router over a linked Host, an unused linked Docker, and the fixture
/// registered under `KIND`, placing scopes on `selected`.
fn router(
    dir: &RunDir,
    ledger: &Arc<MemoryLedger>,
    selected: &str,
    identity: Option<&str>,
) -> RoutingExecutor {
    let mut providers = ProviderRegistrations::new();
    providers
        .register_linked(Arc::new(HostFactory::default()))
        .expect("a linked host");
    providers
        .register_linked(Arc::new(UnusedFactory::new("docker")))
        .expect("a linked docker");
    register(&mut providers, dir, identity, KIND);
    router_over(
        dir,
        ledger,
        selected,
        providers,
        NetworkPolicy::ProviderDefault,
    )
}

fn router_over(
    dir: &RunDir,
    ledger: &Arc<MemoryLedger>,
    selected: &str,
    providers: ProviderRegistrations,
    network: NetworkPolicy,
) -> RoutingExecutor {
    let router = RoutingExecutor::with_options(dir.path(), Retention::Always, SandboxOptions {
        backend: selected.parse().expect("a valid kind"),
        network,
        ..Default::default()
    })
    .with_providers(providers);
    router.set_ledger(ledger.clone());
    router
}

fn scope() -> ScopeSpec {
    let mut scope = ScopeSpec::new(ScopeId::new(0), "work");
    scope.env.insert("SCOPE_ENV".into(), "scope-value".into());
    scope
}

fn recorded() -> Vec<RecordedLease> {
    vec![RecordedLease {
        lease:        LEASE,
        workspace_id: "work".to_owned(),
    }]
}

/// The calls the fixture served, one per line.
fn calls(dir: &RunDir) -> String {
    fs::read_to_string(dir.path().join("backend/calls")).unwrap_or_default()
}

fn count(dir: &RunDir, call: &str) -> usize {
    calls(dir).lines().filter(|line| *line == call).count()
}

#[tokio::test]
async fn a_wrong_checksum_never_launches_the_plugin() {
    let dir = RunDir::new("external-checksum");
    let mut config = plugin_config(&dir, Some("account-a"), KIND);
    config.dev = false;
    config.sha256 = Some("0".repeat(64));
    let mut providers = ProviderRegistrations::new();
    providers
        .register_plugin(config.clone(), template(&dir))
        .expect("registration checks nothing on disk");
    let source = PluginSource::new(PluginSettings::from_env(KIND, Some(true)).expect("settings"));
    drop(source);
    let ledger = Arc::new(MemoryLedger::default());
    let router = router_over(
        &dir,
        &ledger,
        KIND,
        providers,
        NetworkPolicy::ProviderDefault,
    );
    let error = router
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect_err("the checksum refuses");
    assert!(error.to_string().contains("checksum"), "{error}");
    assert!(calls(&dir).is_empty(), "a refused executable never runs");
    router.shutdown().await;
}

#[tokio::test]
async fn an_unregistered_kind_never_falls_back_to_host() {
    let dir = RunDir::new("external-missing-registration");
    let router = RoutingExecutor::with_options(dir.path(), Retention::Never, SandboxOptions {
        backend: "unregistered-kind".parse().expect("a valid kind"),
        ..Default::default()
    });
    let error = router
        .acquire(&scope(), &AcquireContext::bare())
        .await
        .expect_err("nothing serves the kind");
    assert!(error.to_string().contains("not registered"), "{error}");
    assert!(!dir.path().join("host-registry").exists());
    router.shutdown().await;
}

#[tokio::test]
async fn one_registry_routes_two_registered_providers_and_a_linked_host_by_recorded_kind() {
    let run = RunDir::new("external-mixed-run");
    let alpha = RunDir::new("external-alpha");
    let beta = RunDir::new("external-beta");
    let ledger = Arc::new(MemoryLedger::default());
    let host = Arc::new(HostFactory::default());
    let docker = Arc::new(UnusedFactory::new("docker"));
    let mut providers = ProviderRegistrations::new();
    providers
        .register_linked(host.clone())
        .expect("a linked host");
    providers
        .register_linked(docker.clone())
        .expect("a linked docker");
    register(&mut providers, &alpha, Some("a"), "provider-alpha");
    register(&mut providers, &beta, Some("b"), "provider-beta");
    let make_router = |selected: &str| {
        router_over(
            &run,
            &ledger,
            selected,
            providers.clone(),
            NetworkPolicy::ProviderDefault,
        )
    };
    for (lease, kind) in [(1, "provider-alpha"), (2, "provider-beta")] {
        let router = make_router(kind);
        let mut scope = scope();
        scope.workspace_id = executor::WorkspaceId::new(format!("work-{lease}"));
        let env = router
            .acquire(
                &scope,
                &AcquireContext::bare().with_lease(SandboxLeaseId::new(lease)),
            )
            .await
            .expect("acquires on the selected provider");
        env.exec()
            .write_file(Path::new("owner"), kind.as_bytes())
            .await
            .expect("writes");
        router.release(env, ScopeOutcome::Succeeded).await;
        router.shutdown().await;
    }
    // A fresh router placing new scopes on the host: the recorded leases
    // still go to the providers their records name.
    let router = make_router("host");
    let recorded = [1, 2].map(|lease| RecordedLease {
        lease:        SandboxLeaseId::new(lease),
        workspace_id: format!("work-{lease}"),
    });
    let report = router.reconcile(&recorded).await;
    assert!(report.is_clean(), "{report:?}");
    for (lease, kind) in [(1, "provider-alpha"), (2, "provider-beta")] {
        let mut scope = scope();
        scope.workspace_id = executor::WorkspaceId::new(format!("work-{lease}"));
        let env = router
            .acquire(
                &scope,
                &AcquireContext::bare().with_lease(SandboxLeaseId::new(lease)),
            )
            .await
            .expect("attaches on the recorded provider");
        assert_eq!(
            env.exec()
                .read_file(Path::new("owner"))
                .await
                .expect("reads")
                .expect("the file"),
            kind.as_bytes()
        );
        router.release(env, ScopeOutcome::Succeeded).await;
        router
            .delete_recorded(SandboxLeaseId::new(lease), scope.workspace_id.as_str())
            .await
            .expect("prunes on the recorded provider");
    }
    let host_env = router
        .acquire(
            &scope(),
            &AcquireContext::bare().with_lease(SandboxLeaseId::new(3)),
        )
        .await
        .expect("acquires on the linked host");
    host_env
        .exec()
        .write_file(Path::new("host"), b"linked")
        .await
        .expect("writes");
    router.release(host_env, ScopeOutcome::Succeeded).await;
    router
        .delete_recorded(SandboxLeaseId::new(3), "work")
        .await
        .expect("prunes on the host");
    router.shutdown().await;
    assert_eq!(host.connects(), 1);
    assert_eq!(docker.connects(), 0, "an unused provider is never reached");
    for dir in [&alpha, &beta] {
        assert_eq!(count(dir, "create"), 1);
    }
}

#[tokio::test]
async fn plugin_exec_files_and_recovery_use_the_recorded_provider_after_the_default_changes() {
    let dir = RunDir::new("external-recovery");
    let ledger = Arc::new(MemoryLedger::default());
    let first = router(&dir, &ledger, KIND, Some("account-a"));
    assert!(calls(&dir).is_empty(), "registration launches nothing");
    let env = first
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect("acquires on the fixture");
    let exec = env.exec();
    exec.write_file(Path::new("input"), b"persistent")
        .await
        .expect("writes");
    let mut child = exec
        .spawn(ProcessSpec::new("/bin/sh", &[
            "-c",
            "printf '%s:%s:' \"$TEMPLATE_ENV\" \"$SCOPE_ENV\" > output; cat input >> output",
        ]))
        .await
        .expect("spawns");
    assert_eq!(child.wait().await.expect("exits").code, Some(0));
    assert_eq!(
        exec.read_file(Path::new("output"))
            .await
            .expect("reads")
            .expect("the file"),
        b"template-value:scope-value:persistent"
    );
    // The spec the fixture was asked to create: the template under Petri's
    // name and labels.
    let spec: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.path().join("backend/spec.json")).expect("the spec"))
            .expect("JSON");
    assert_eq!(spec["labels"]["custom-label"], "preserved");
    assert_eq!(spec["provider_config"]["opaque"], "preserved");
    assert!(spec["name"].as_str().expect("a name").starts_with("petri-"));
    assert!(spec["labels"]["petri.run"].is_string());
    assert!(first.release(env, ScopeOutcome::Succeeded).await.is_clean());
    first.shutdown().await;
    assert!(
        exec.read_file(Path::new("input")).await.is_err(),
        "the old generation is closed"
    );

    // A new process placing scopes on the host resumes the lease where it
    // was recorded, then stops and prunes it there.
    let second = router(&dir, &ledger, "host", Some("account-a"));
    let env = second
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect("attaches on the fixture");
    assert_eq!(
        env.exec()
            .read_file(Path::new("input"))
            .await
            .expect("reads")
            .expect("the file"),
        b"persistent"
    );
    assert!(
        second
            .release(env, ScopeOutcome::Succeeded)
            .await
            .is_clean()
    );
    let report = second.release_lease(LEASE, ScopeOutcome::Succeeded).await;
    assert!(report.is_clean(), "{report:?}");
    assert_eq!(
        ledger
            .lookup(LEASE)
            .await
            .expect("reads")
            .expect("recorded")
            .state,
        LeaseState::Stopped
    );
    assert_eq!(
        second
            .delete_recorded(LEASE, "work")
            .await
            .expect("prunes")
            .len(),
        1
    );
    assert_eq!(
        ledger
            .lookup(LEASE)
            .await
            .expect("reads")
            .expect("recorded")
            .state,
        LeaseState::Deleted
    );
    second.shutdown().await;
    assert_eq!(
        count(&dir, "create"),
        1,
        "resume attaches; it never creates again"
    );
    assert_eq!(count(&dir, "launch"), 2, "one plugin per process");
}

#[tokio::test]
async fn a_namespace_mismatch_refuses_attach_delete_and_the_orphan_sweep() {
    let dir = RunDir::new("external-namespace");
    let ledger = Arc::new(MemoryLedger::default());
    let first = router(&dir, &ledger, KIND, Some("account-a"));
    let env = first
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect("acquires");
    first.release(env, ScopeOutcome::Succeeded).await;
    first.shutdown().await;
    let before = calls(&dir);

    // The same plugin reporting another identity: the lease was recorded
    // elsewhere, so nothing is attached, deleted, or swept.
    let second = router(&dir, &ledger, KIND, Some("account-b"));
    let report = second.reconcile(&recorded()).await;
    assert!(!report.is_clean(), "{report:?}");
    assert!(report.removed.is_empty() && report.adopted.is_empty());
    let error = second
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect_err("the recorded lease refuses");
    assert!(error.to_string().contains("account-a"), "{error}");
    assert!(second.delete_recorded(LEASE, "work").await.is_err());
    second.shutdown().await;
    let after = calls(&dir);
    for call in after
        .strip_prefix(&before)
        .expect("calls only append")
        .lines()
    {
        assert!(
            matches!(call, "launch" | "health"),
            "the mismatched provider served {call}"
        );
    }
    assert_eq!(
        ledger
            .lookup(LEASE)
            .await
            .expect("reads")
            .expect("recorded")
            .state,
        LeaseState::Live
    );
    let cleanup = router(&dir, &ledger, "host", Some("account-a"));
    cleanup
        .delete_recorded(LEASE, "work")
        .await
        .expect("prunes in the right namespace");
    cleanup.shutdown().await;
}

#[tokio::test]
async fn a_lost_create_reply_is_reconciled_without_replaying_the_create() {
    let dir = RunDir::new("external-crash");
    fs::create_dir_all(dir.path().join("backend")).expect("the backend directory");
    fs::write(dir.path().join("backend/crash-after-create"), "once").expect("the crash marker");
    let ledger = Arc::new(MemoryLedger::default());
    let router = router(&dir, &ledger, KIND, Some("account-a"));
    assert!(
        router
            .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
            .await
            .is_err(),
        "the plugin dies before it answers"
    );
    assert_eq!(
        ledger
            .lookup(LEASE)
            .await
            .expect("reads")
            .expect("recorded")
            .state,
        LeaseState::Allocating
    );
    // The next acquire relaunches the plugin, finds the created sandbox by
    // its labels, and adopts it.
    let env = router
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect("adopts the lost create");
    env.exec()
        .write_file(Path::new("recovered"), b"yes")
        .await
        .expect("writes");
    router.release(env, ScopeOutcome::Succeeded).await;
    router.delete_recorded(LEASE, "work").await.expect("prunes");
    router.shutdown().await;
    assert_eq!(count(&dir, "create"), 1, "the create is never replayed");
    assert_eq!(count(&dir, "launch"), 2);
}

#[tokio::test]
async fn a_missing_identity_and_an_unsupported_scope_never_create() {
    let dir = RunDir::new("external-refusals");
    let ledger = Arc::new(MemoryLedger::default());
    let router = router(&dir, &ledger, KIND, None);
    let mut container = scope();
    container.runtime = ir::RuntimeSpec::container("unused");
    let error = router
        .acquire(&container, &AcquireContext::bare())
        .await
        .expect_err("a container scope is refused");
    assert!(error.to_string().contains("process scopes"), "{error}");
    let mut placed = scope();
    placed.runtime.requirements.push("ubuntu-24.04".into());
    let error = router
        .acquire(&placed, &AcquireContext::bare())
        .await
        .expect_err("placement requirements are refused");
    assert!(error.to_string().contains("placement"), "{error}");
    assert!(calls(&dir).is_empty(), "refused before the plugin launches");
    let error = router
        .acquire(&scope(), &AcquireContext::bare())
        .await
        .expect_err("no identity, no namespace");
    assert!(error.to_string().contains("identity"), "{error}");
    assert_eq!(count(&dir, "create"), 0);
    router.shutdown().await;
}

#[tokio::test]
async fn a_run_policy_must_match_the_template_or_leave_it_alone() {
    let dir = RunDir::new("external-network");
    let ledger = Arc::new(MemoryLedger::default());
    let mut providers = ProviderRegistrations::new();
    let mut open = template(&dir);
    open.network = NetworkPolicy::AllowAll;
    providers
        .register_plugin(plugin_config(&dir, Some("account-a"), KIND), open)
        .expect("a valid registration");
    let blocked = router_over(&dir, &ledger, KIND, providers.clone(), NetworkPolicy::Block);
    let error = blocked
        .acquire(&scope(), &AcquireContext::bare())
        .await
        .expect_err("the run's policy differs from the template's");
    assert!(error.to_string().contains("network policy"), "{error}");
    assert!(calls(&dir).is_empty(), "refused before the plugin launches");
    blocked.shutdown().await;
    // The same policy passes Petri's check and reaches the provider, which
    // is the Host provider under the fixture and manages no networking: its
    // own refusal, at create, is the proof the policy was sent.
    let same = router_over(&dir, &ledger, KIND, providers, NetworkPolicy::AllowAll);
    let error = same
        .acquire(&scope(), &AcquireContext::bare().with_lease(LEASE))
        .await
        .expect_err("the fixture's host backend manages no networking");
    assert!(error.to_string().contains("networking"), "{error}");
    assert_eq!(
        count(&dir, "create"),
        1,
        "the matching policy reached the provider"
    );
    same.shutdown().await;
}
