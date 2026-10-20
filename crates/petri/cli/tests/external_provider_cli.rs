//! Whole Petri processes over a plugin registered under an unfamiliar kind:
//! run, prune, a dry run, and a resume in a fresh process.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};
use std::{env, fs, thread};

use serde_json::json;
use testkit::RunDir;

fn petri() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_petri"));
    command.env_clear().env("PATH", "/usr/bin:/bin");
    command
}

/// A provider file registering the fixture at `executable` under
/// `fixture-provider`, with a managed workspace under `dir`.
fn config(dir: &RunDir, executable: &Path) -> PathBuf {
    let path = dir.path().join("providers.toml");
    let quoted = |path: &Path| json!(path.to_string_lossy()).to_string();
    fs::write(
        &path,
        format!(
            r#"
[providers.fixture-provider]
executable = {}
args = ["--fixture"]
dev = true
[providers.fixture-provider.env]
PATH = "/usr/bin:/bin"
FIXTURE_ROOT = {}
FIXTURE_IDENTITY = "account-a"
[providers.fixture-provider.process_spec]
source = "host_directory"
working_directory = {}
workspace_ownership = "managed"
"#,
            quoted(executable),
            quoted(&dir.path().join("backend")),
            quoted(&dir.path().join("workspace"))
        ),
    )
    .expect("the provider file");
    path
}

/// The fixture plugin the workspace test gate builds beside this test.
fn fixture() -> PathBuf {
    let path = env::current_exe()
        .expect("this test's executable")
        .parent()
        .expect("deps")
        .parent()
        .expect("the profile directory")
        .join("fixture-provider");
    assert!(
        path.is_file(),
        "build the fixture with `cargo build -p petri-testkit --bin fixture-provider` (the \
         workspace test gate builds it)"
    );
    path
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn calls(dir: &RunDir) -> String {
    fs::read_to_string(dir.path().join("backend/calls")).unwrap_or_default()
}

fn count(dir: &RunDir, call: &str) -> usize {
    calls(dir).lines().filter(|line| *line == call).count()
}

#[test]
fn a_native_run_and_its_prune_keep_the_recorded_provider() {
    let dir = RunDir::new("external-cli-native");
    let config = config(&dir, &fixture());
    let workflow = dir.path().join("workflow.petri.yaml");
    fs::write(
        &workflow,
        "name: custom\nnodes:\n  write:\n    run: printf plugin-output > result\n",
    )
    .expect("the workflow");
    let run = dir.path().join("run");
    let output = petri()
        .args([
            "run",
            "--retain",
            "always",
            "--backend",
            "fixture-provider",
            "--sandbox-providers",
        ])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .arg(&workflow)
        .output()
        .expect("petri run");
    assert_success(&output);
    assert_eq!(
        fs::read(dir.path().join("workspace/result")).expect("the result"),
        b"plugin-output"
    );
    // Prune under another backend: the lease's record names the provider.
    let output = petri()
        .args([
            "sandbox",
            "prune",
            "--backend",
            "host",
            "--sandbox-providers",
        ])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .output()
        .expect("petri sandbox prune");
    assert_success(&output);
    assert!(
        !dir.path().join("workspace").exists(),
        "prune deletes the managed workspace"
    );
    assert_eq!(count(&dir, "create"), 1);
    assert_eq!(count(&dir, "delete"), 1);
}

#[test]
fn an_unregistered_backend_is_an_error_before_anything_runs() {
    let dir = RunDir::new("external-cli-unregistered");
    let workflow = dir.path().join("workflow.petri.yaml");
    fs::write(
        &workflow,
        "name: custom\nnodes:\n  write:\n    run: \"true\"\n",
    )
    .expect("the workflow");
    let output = petri()
        .args(["run", "--backend", "no-such-provider", "--run-dir"])
        .arg(dir.path().join("run"))
        .arg(&workflow)
        .output()
        .expect("petri run");
    assert!(!output.status.success());
    // The step's failure is recorded in the run's event log.
    let events = fs::read_to_string(dir.path().join("run/events.json")).expect("the event log");
    assert!(events.contains("not registered"), "{events}");
    assert!(
        !dir.path().join("run/host-registry").exists(),
        "never fell back to the host"
    );
}

/// A Fabro workflow whose settings select the fixture.
fn fabro(dir: &RunDir, script: &str) -> PathBuf {
    let workflow = dir.path().join("workflow.fabro");
    fs::write(
        &workflow,
        format!(
            r"digraph G {{
        start [shape=Mdiamond]
        exit [shape=Msquare]
        work [shape=parallelogram, script={}]
        start -> work -> exit
    }}",
            json!(script)
        ),
    )
    .expect("the workflow");
    fs::write(
        dir.path().join("workflow.toml"),
        "[run.environment]\nid = 'selected'\n[environments.selected]\nprovider = \
         'fixture-provider'\n",
    )
    .expect("the settings");
    workflow
}

#[test]
fn a_fabro_environment_selects_the_provider_and_a_dry_run_launches_nothing() {
    let dir = RunDir::new("external-cli-fabro");
    let config = config(&dir, &fixture());
    let workflow = fabro(&dir, "printf fabro-output > result");
    let output = petri()
        .args(["run", "--retain", "always", "--sandbox-providers"])
        .arg(&config)
        .arg("--run-dir")
        .arg(dir.path().join("run"))
        .arg(&workflow)
        .output()
        .expect("petri run");
    assert_success(&output);
    assert_eq!(
        fs::read(dir.path().join("workspace/result")).expect("the result"),
        b"fabro-output"
    );
    let before = calls(&dir);
    // A dry run with an executable that does not exist: nothing resolves
    // or launches it.
    let missing = self::config(&dir, Path::new("/missing/fixture-provider"));
    let output = petri()
        .args(["run", "--dry-run", "--sandbox-providers"])
        .arg(&missing)
        .arg("--run-dir")
        .arg(dir.path().join("dry-run"))
        .arg(&workflow)
        .output()
        .expect("petri run --dry-run");
    assert_success(&output);
    assert_eq!(calls(&dir), before, "a dry run never launches the plugin");
}

#[test]
fn a_fresh_process_resumes_on_the_recorded_provider_after_a_crash() {
    let dir = RunDir::new("external-cli-resume");
    let config = config(&dir, &fixture());
    let marker = dir.path().join("continue");
    let ready = dir.path().join("ready");
    let workflow = fabro(
        &dir,
        &format!(
            "touch '{}'; while [ ! -f '{}' ]; do sleep 0.05; done; printf resumed > result",
            ready.display(),
            marker.display()
        ),
    );
    let run = dir.path().join("run");
    let mut child = petri()
        .args(["run", "--retain", "always", "--sandbox-providers"])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .arg(&workflow)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("petri run");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() {
        if child.try_wait().expect("polls").is_some() || Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the step never reached its readiness marker");
        }
        thread::sleep(Duration::from_millis(20));
    }
    child.kill().expect("kills");
    child.wait().expect("reaps");
    fs::write(&marker, "continue").expect("the marker");
    // The resume names another backend; the lease's record wins.
    let output = petri()
        .args([
            "resume",
            "--retain",
            "always",
            "--backend",
            "host",
            "--sandbox-providers",
        ])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .output()
        .expect("petri resume");
    assert_success(&output);
    assert_eq!(
        fs::read(dir.path().join("workspace/result")).expect("the result"),
        b"resumed"
    );
    assert_eq!(
        count(&dir, "create"),
        1,
        "resume attaches and fences; it never creates again"
    );
    let output = petri()
        .args(["sandbox", "prune", "--sandbox-providers"])
        .arg(&config)
        .arg("--run-dir")
        .arg(&run)
        .output()
        .expect("petri sandbox prune");
    assert_success(&output);
    assert_eq!(count(&dir, "delete"), 1);
}
