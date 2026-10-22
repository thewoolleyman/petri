use super::*;

fn daytona_options(command: &str, flags: &[&str]) -> SandboxOptions {
    let mut args = vec!["petri", command];
    if command == "run" {
        args.push("workflow.fabro");
    } else {
        args.extend(["--run-dir", "run"]);
    }
    args.extend(["--backend", "daytona"]);
    args.extend_from_slice(flags);
    let cli = Cli::try_parse_from(args).unwrap();
    let (Command::Run {
        session,
        provider,
        runner,
        ..
    }
    | Command::Resume {
        session,
        provider,
        runner,
        ..
    }) = cli.command
    else {
        unreachable!()
    };
    session
        .run_options(
            Path::new("run"),
            None,
            &LaunchSettings::default(),
            &provider,
            runner,
        )
        .expect("valid launch options")
        .sandbox
}

#[test]
fn run_and_resume_use_the_library_daytona_defaults() {
    let expected = SandboxOptions::default();
    for command in ["run", "resume"] {
        let options = daytona_options(command, &[]);
        assert_eq!(options.backend, SandboxBackend::Daytona);
        assert_eq!(options.daytona_kind, DaytonaSandboxKind::Container);
        assert_eq!(options.daytona_kind, expected.daytona_kind);
        let resources = options.daytona_resources;
        assert_eq!(resources.cpu_cores, expected.daytona_resources.cpu_cores);
        assert_eq!(resources.memory_mb, expected.daytona_resources.memory_mb);
        assert_eq!(resources.disk_mb, expected.daytona_resources.disk_mb);
        assert_eq!(resources.disk_mb, None);
    }
}

#[test]
fn run_and_resume_honor_explicit_daytona_overrides() {
    for command in ["run", "resume"] {
        let options = daytona_options(command, &[
            "--daytona-kind",
            "vm",
            "--daytona-cpus",
            "4",
            "--daytona-memory-mb",
            "8192",
            "--daytona-disk-mb",
            "10240",
            "--runner-image",
            "ubuntu-24.04=custom:runner",
        ]);
        assert_eq!(options.daytona_kind, DaytonaSandboxKind::VirtualMachine);
        assert_eq!(options.daytona_resources.cpu_cores, 4);
        assert_eq!(options.daytona_resources.memory_mb, 8192);
        assert_eq!(options.daytona_resources.disk_mb, Some(10240));
        assert_eq!(options.runner_images["ubuntu-24.04"], "custom:runner");
    }
}

#[test]
fn a_disk_override_keeps_the_other_daytona_defaults() {
    for command in ["run", "resume"] {
        let options = daytona_options(command, &["--daytona-disk-mb", "3072"]);
        assert_eq!(options.daytona_kind, DaytonaSandboxKind::Container);
        assert_eq!(options.daytona_resources.cpu_cores, 2);
        assert_eq!(options.daytona_resources.memory_mb, 4096);
        assert_eq!(options.daytona_resources.disk_mb, Some(3072));
    }
}

#[test]
fn cli_rejects_resources_below_the_runner_requirements() {
    for (flag, value) in [
        ("--daytona-cpus", "1"),
        ("--daytona-memory-mb", "2048"),
        ("--daytona-disk-mb", "0"),
    ] {
        assert!(Cli::try_parse_from(["petri", "run", "workflow.fabro", flag, value]).is_err());
    }
}
