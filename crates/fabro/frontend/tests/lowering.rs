//! The Fabro frontend's lowering: what the settings layers put into the
//! graph. `workflow.toml` beside the workflow, `.fabro/project.toml` at the
//! bundle root, the operator's settings layer, and the launch variables; the
//! launch record they persist; the bundle root. The language itself is
//! `crates/attractor/frontend/tests/lowering.rs`.

mod support;

use std::time::Duration;
use std::{env, fs, process};

use frontend::{CompileInputs, Frontend};
use frontend_attractor::kinds::COMMAND_KIND;
use frontend_fabro::{Fabro, SETTINGS_HOOKS_VAR, load};
use serde_json::json;
use support::*;

#[test]
fn file_references_and_workflow_toml_defaults_resolve_through_the_file_source() {
    let files = files(&[
        (
            "wf/prompts/plan.md",
            "Plan {{ inputs.mode }}\n{% include \"partials/tail.md\" %}",
        ),
        (
            "wf/prompts/partials/tail.md",
            "tail {% include \"nested/end.md\" %}",
        ),
        ("wf/prompts/partials/nested/end.md", "done"),
        ("wf/workflow.toml", "[run.inputs]\nmode = \"fast\"\n"),
    ]);
    let text = dot(r#"
        a [prompt="@prompts/plan.md"]
        start -> a -> exit
    "#);
    let lowered = load("wf/workflow.fabro", &text, &files, &CompileInputs::new());
    let graph = lowered.graph.expect("lowers");
    assert_eq!(
        node(&graph, "a").step.config["prompt"],
        json!("Plan fast\ntail done")
    );
    assert!(
        codes(&dot(r#"
        a [prompt="@missing.md"]
        start -> a -> exit
    "#))
        .contains(&"attractor.file_not_found".to_string()),
        "a missing `@file` prompt reference is the language's diagnostic"
    );
}

/// Every `workflow.toml` section is diagnosed. Platform-only sections warn
/// with why, a requirement the standalone runner cannot meet is an
/// `unsupported.workflow_toml.*` error, and a key Fabro's own parser refuses
/// is an error with Fabro's rename hint.
#[test]
fn workflow_toml_sections_warn_or_reject_and_never_pass_silently() {
    let diags = |toml: &str| {
        let files = files(&[("wf/workflow.toml", toml)]);
        load(
            "wf/workflow.fabro",
            &dot(r#"
                a [shape=parallelogram, script="true"]
                start -> a -> exit
            "#),
            &files,
            &CompileInputs::new(),
        )
        .diagnostics
        .iter()
        .cloned()
        .collect::<Vec<_>>()
    };
    let codes = |toml: &str| {
        let mut out: Vec<String> = diags(toml).iter().map(|d| d.code.to_string()).collect();
        out.sort();
        out.dedup();
        out
    };

    // The code-review bundle's platform-only sections: warnings, and the
    // graph still lowers.
    let platform_only = diags(
        "_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n[run]\ngoal = \"g\"\n\
         [run.inputs]\nmode = \"changes\"\n[run.clone]\ndepth = 1\n[run.run_branch]\n\
         enabled = false\n[run.pull_request]\nenabled = false\n[run.model.fallbacks]\n\
         \"m\" = [\"p:m\"]\n[run.model]\nprovider = \"openai\"\n[run.environment]\n\
         id = \"review\"\n[run.integrations.github.permissions]\npull_requests = \"write\"\n\
         [run.checkpoint]\nexclude_globs = []\n[run.artifacts]\ninclude = []\n\
         [run.execution]\nmode = \"normal\"\n[run.agent]\nfabro_tools = true\n\
         [environments.review]\nprovider = \"docker\"\n[environments.review.network]\n\
         mode = \"none\"\n[environments.review.image]\ndockerfile = { path = \"Dockerfile\" }\n",
    );
    assert!(
        platform_only.iter().all(|d| !d.is_error()),
        "platform-only sections are warnings: {platform_only:#?}"
    );
    let platform_codes: Vec<String> = platform_only.iter().map(|d| d.code.to_string()).collect();
    for code in [
        "ignored.workflow_toml.run.run_branch",
        "ignored.workflow_toml.run.pull_request",
        "ignored.workflow_toml.run.integrations",
        "ignored.workflow_toml.run.checkpoint",
        "ignored.workflow_toml.run.artifacts",
        "ignored.workflow_toml.run.agent.fabro_tools",
    ] {
        assert!(
            platform_codes.contains(&code.to_string()),
            "{code} in {platform_codes:?}"
        );
    }
    // Sections the runner now applies do not warn, and neither do the
    // platform's own environment keys (`network`, `image.dockerfile`).
    for code in [
        "ignored.workflow_toml.run.goal",
        "ignored.workflow_toml.run.clone",
        "ignored.workflow_toml.run.model",
        "ignored.workflow_toml.run.model.fallbacks",
        "ignored.workflow_toml.run.environment",
        "ignored.workflow_toml.run.execution",
        "ignored.workflow_toml.environments",
        "ignored.workflow_toml.environments.review.network",
        "ignored.workflow_toml.environments.review.image.dockerfile",
    ] {
        assert!(
            !platform_codes.contains(&code.to_string()),
            "{code} is applied or known, not ignored: {platform_codes:?}"
        );
    }
    assert!(
        platform_only
            .iter()
            .all(|d| d.message.contains("is ignored: ")),
        "each warning says why: {platform_only:#?}"
    );

    // Requirements the standalone runner cannot meet: specific errors.
    assert_eq!(
        codes("[run.environment]\nid = \"nowhere\"\n"),
        ["unsupported.workflow_toml.run.environment"],
        "an environment id with no table is refused, as Fabro refuses it"
    );
    assert_eq!(
        codes("[run.environment]\nid = \"e\"\n[environments.e]\nprovider = \"bad/k8s\"\n"),
        ["unsupported.workflow_toml.environments.provider"]
    );
    assert_eq!(
        codes("[run.prepare]\nsteps = [{ script = \"a\", command = [\"b\"] }]\n"),
        ["unsupported.workflow_toml.run.prepare"],
        "exactly one of script or command"
    );
    // A hook whose event Fabro does not know is a specific error; a good one
    // loads (see `hooks_load_from_every_layer_and_merge_by_id`).
    assert_eq!(
        codes("[[run.hooks]]\nevent = \"stage.completed\"\nscript = \"true\"\n"),
        ["fabro.hooks.event"]
    );
    // A malformed MCP entry is an error from the MCP reader (`fabro.mcps.*`);
    // a well-formed one lowers onto the agent nodes
    // (`mcps_lower_onto_agent_nodes`).
    assert_eq!(
        codes("[run.agent.mcps.files]\ntype = \"stdio\"\ncommand = \"mcp\"\n"),
        ["fabro.mcps.entry"]
    );
    // An empty hook list or MCP table asks for nothing.
    assert!(codes("[run]\nhooks = []\n[run.agent.mcps]\n").is_empty());
    // Sub-agents and compaction have no `workflow.toml` surface at the
    // pinned Fabro (its `[run.agent]` accepts `fabro_tools` and `mcps` only),
    // so a request for one is refused as Fabro refuses it, never passed
    // silently. `skills` is the runner's own extension
    // (`run_agent_skills_is_a_warned_extension`).
    for key in ["subagents", "compaction", "context_window"] {
        assert_eq!(
            codes(&format!("[run.agent]\n{key} = {{ enabled = true }}\n")),
            ["unsupported.workflow_toml.key"],
            "`[run.agent] {key}` is refused"
        );
    }
    // The model fallback chain is read (task 12): a well-formed table is
    // silent, a provider-qualified key is refused as Fabro refuses it.
    assert!(codes("[run.model.fallbacks]\n\"m\" = [\"p:m\"]\n").is_empty());
    assert_eq!(codes("[run.model.fallbacks]\n\"p/m\" = [\"q:m\"]\n"), [
        "fabro.model_fallbacks"
    ]);

    // Keys Fabro's parser refuses, with its rename hint.
    let legacy = diags("version = 1\n[vars]\nmode = \"x\"\n[llm]\nmodel = \"m\"\n");
    let hints: Vec<(String, Option<String>)> = legacy
        .iter()
        .filter(|d| d.is_error())
        .map(|d| (d.code.to_string(), d.hint.clone()))
        .collect();
    assert!(
        hints.contains(&(
            "unsupported.workflow_toml.key".to_string(),
            Some("rename to `_version`".to_string())
        )),
        "{hints:?}"
    );
    assert!(
        hints.contains(&(
            "unsupported.workflow_toml.key".to_string(),
            Some("rename to `[run.inputs]`".to_string())
        )),
        "{hints:?}"
    );
    assert!(
        hints.contains(&(
            "unsupported.workflow_toml.key".to_string(),
            Some("rename to `[run.model]`".to_string())
        )),
        "{hints:?}"
    );
    assert_eq!(codes("_version = 2\n"), [
        "unsupported.workflow_toml.version"
    ]);
    assert_eq!(codes("[run]\nnot_a_key = 1\n"), [
        "unsupported.workflow_toml.key"
    ]);
    // A clean file is clean.
    assert!(codes("_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n").is_empty());
}

/// `[workflow]` carries what Fabro's parser accepts: `name`, `description`,
/// `graph`, `metadata` and `engine`, which Fabro reads to choose the engine.
/// Petri is the engine, so the lowering reads none of them and none
/// diagnoses; a key outside the set is refused as Fabro refuses it.
#[test]
fn workflow_engine_is_a_known_key_and_unknown_workflow_keys_are_refused() {
    let lowered = |toml: &str| {
        load(
            "wf/workflow.fabro",
            &dot(r#"
                a [shape=parallelogram, script="true"]
                start -> a -> exit
            "#),
            &files(&[("wf/workflow.toml", toml)]),
            &CompileInputs::new(),
        )
    };
    for engine in ["petri", "legacy"] {
        let lowered = lowered(&format!(
            "_version = 1\n[workflow]\nname = \"n\"\ndescription = \"d\"\n\
             graph = \"workflow.fabro\"\nengine = \"{engine}\"\n[workflow.metadata]\n\
             team = \"t\"\n"
        ));
        assert!(
            lowered.diagnostics.is_empty(),
            "`engine = \"{engine}\"` is accepted silently: {:?}",
            lowered.diagnostics
        );
        let graph = lowered.graph.expect("lowers");
        assert_eq!(
            node(&graph, "a").step.kind,
            COMMAND_KIND,
            "the engine key changes nothing in the lowering"
        );
    }
    let lowered = lowered("[workflow]\nengine = \"petri\"\nnot_a_key = 1\n");
    let refused: Vec<_> = lowered.diagnostics.errors().collect();
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(refused[0].code, "unsupported.workflow_toml.key");
    assert!(
        refused[0].message.contains("`workflow.not_a_key`"),
        "{refused:?}"
    );
    assert!(lowered.graph.is_none(), "a refused key withholds the graph");
}

/// `[run.environment]` maps the provider onto the launch settings the CLI
/// reads, the image onto the scope's container target, and literal env onto
/// the scope env; `[run.prepare]` steps become the first command nodes.
#[test]
fn run_environment_and_prepare_lower_onto_the_scope_and_the_graph() {
    let files = files(&[(
        "wf/workflow.toml",
        "[run]\ngoal = \"Fix {{ inputs.target }}\"\n[run.inputs]\ntarget = \"main\"\n\
         [run.model]\nprovider = \"openai\"\nname = \"gpt-5.6-sol\"\n\
         [run.model.controls]\nreasoning_effort = \"low\"\nspeed = \"fast\"\n\
         [run.model.fallbacks]\n\"gpt-5.6-sol\" = [\"anthropic:claude-opus\", \"openrouter/kimi-k3\"]\n\
         [run.execution]\nmode = \"dry_run\"\napproval = \"auto\"\n\
         [run.environment]\nid = \"review\"\n[run.environment.env]\nOVERRIDE = \"run\"\n\
         [environments.review]\nprovider = \"docker\"\n[environments.review.image]\n\
         docker = \"ghcr.io/acme/review:1\"\n[environments.review.resources]\ncpu = 4\n\
         [environments.review.env]\nLANG = \"C.UTF-8\"\nOVERRIDE = \"base\"\n\
         TOKEN = \"{{ secrets.REVIEW_TOKEN }}\"\n\
         [run.prepare]\ntimeout = \"30s\"\n[[run.prepare.steps]]\nscript = \"make deps\"\n\
         [[run.prepare.steps]]\ncommand = [\"sh\", \"-c\", \"echo {{ inputs.target }}\"]\n\
         env = { STEP = \"two\" }\n",
    )]);
    let lowered = load(
        "wf/workflow.fabro",
        &dot(r#"
            a [prompt="x"]
            c [shape=parallelogram, script="true"]
            start -> a -> c -> exit
        "#),
        &files,
        &CompileInputs::new(),
    );
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    let graph = lowered.graph.expect("lowers");
    // Launch settings, as the CLI reads them.
    let launch = Fabro::new().launch_settings(&graph);
    assert_eq!(launch.sandbox_backend.as_deref(), Some("docker"));
    assert!(launch.dry_run && launch.auto_approve);
    assert_eq!(
        graph.params["fabro.launch"]["cpu_cores"],
        json!(null),
        "resources size Daytona only"
    );
    assert_eq!(
        graph.params["goal"],
        json!("Fix main"),
        "[run] goal renders and applies"
    );
    // The scope: image and literal env; the secret is not on the scope.
    let scope = &graph.scopes[0];
    assert!(
        matches!(&scope.runtime.target, ir::RuntimeTarget::Container { image, .. } if image == "ghcr.io/acme/review:1"),
        "{:?}",
        scope.runtime.target
    );
    assert_eq!(scope.env["LANG"], ir::ExprOrValue::Value(json!("C.UTF-8")));
    assert_eq!(
        scope.env["OVERRIDE"],
        ir::ExprOrValue::Value(json!("run")),
        "the run's env wins over the named environment's"
    );
    assert!(
        !scope.env.contains_key("TOKEN"),
        "a secret never lands on the scope"
    );
    // Commands carry the secret as a reference, resolved at spawn.
    assert_eq!(
        node(&graph, "c").step.config["env"]["TOKEN"],
        json!({ "$secret": "REVIEW_TOKEN" })
    );
    // Model defaults reach the LLM node.
    let a = &node(&graph, "a").step.config;
    assert_eq!(a["model"], json!("gpt-5.6-sol"));
    assert_eq!(a["provider"], json!("openai"));
    assert_eq!(a["reasoning_effort"], json!("low"));
    assert_eq!(a["speed"], json!("fast"));
    // The fallback chains ride on the node as written, references canonical.
    assert_eq!(
        a["fallbacks"],
        json!({ "gpt-5.6-sol": ["anthropic:claude-opus", "openrouter:kimi-k3"] })
    );
    // Prepare steps: first after start, in order, with env, timeout, exit.
    assert_eq!(tiers(&graph, "start")[0].1[0].0, "run_prepare_1");
    assert_eq!(tiers(&graph, "run_prepare_1")[0].1[0].0, "run_prepare_2");
    assert_eq!(tiers(&graph, "run_prepare_2")[0].1[0].0, "a");
    let two = node(&graph, "run_prepare_2");
    assert_eq!(two.step.kind, COMMAND_KIND);
    assert_eq!(two.step.config["script"], json!("sh -c 'echo main'"));
    assert_eq!(two.step.config["env"]["STEP"], json!("two"));
    assert_eq!(
        two.step.config["env"]["TOKEN"],
        json!({ "$secret": "REVIEW_TOKEN" })
    );
    assert_eq!(two.step.config["on_failure"], json!("exit"));
    assert_eq!(two.budget.timeout, Duration::from_secs(30));
    assert_eq!(two.meta["classes"], json!(["run-prepare"]));
    // A reserved id is refused.
    let clash = load(
        "wf/workflow.fabro",
        &dot(r#"
            run_prepare_1 [prompt="x"]
            start -> run_prepare_1 -> exit
        "#),
        &files,
        &CompileInputs::new(),
    );
    assert!(
        clash
            .diagnostics
            .iter()
            .any(|d| d.code == "attractor.reserved_node_id"),
        "{:?}",
        clash.diagnostics
    );
}

/// `[run.model.fallbacks]` rides on the `start` node as written, beside
/// every LLM node's copy, so the start stage can check the table against
/// the catalog before anything runs.
#[test]
fn the_start_node_carries_the_fallback_table() {
    let files = files(&[(
        "wf/workflow.toml",
        "[run.model.fallbacks]\n\"gpt-5.6-sol\" = [\"anthropic:claude-sonnet-5\"]\n",
    )]);
    let lowered = load(
        "wf/workflow.fabro",
        &dot(r#"
            a [prompt="x", backend="api", model="gpt-5.6-sol"]
            start -> a -> exit
        "#),
        &files,
        &CompileInputs::new(),
    );
    let graph = lowered.graph.expect("a graph");
    let table = json!({ "gpt-5.6-sol": ["anthropic:claude-sonnet-5"] });
    assert_eq!(node(&graph, "start").step.config["fallbacks"], table);
    assert_eq!(node(&graph, "a").step.config["fallbacks"], table);
    assert!(
        node(&graph, "exit").step.config.get("fallbacks").is_none(),
        "only `start` checks the table"
    );
}

#[test]
fn the_bundle_root_is_the_parent_of_dot_fabro() {
    let base = env::temp_dir().join(format!("petri-fabro-root-{}", process::id()));
    let inside = base.join(".fabro/workflows/one");
    fs::create_dir_all(&inside).expect("create the bundle");
    fs::create_dir_all(base.join("docs")).expect("create docs");
    let frontend = Fabro::new();
    assert_eq!(
        frontend.repo_root(&inside.join("workflow.fabro")),
        base,
        "a file inside the bundle belongs to the bundle's parent"
    );
    assert_eq!(frontend.repo_root(&base.join("docs/demo.fabro")), base);
    let loose = env::temp_dir().join(format!("petri-fabro-loose-{}", process::id()));
    fs::create_dir_all(&loose).expect("create a dir with no bundle");
    assert_eq!(
        frontend.repo_root(&loose.join("w.fabro")),
        loose,
        "no bundle: the file's own directory"
    );
    let _ = fs::remove_dir_all(&base);
    let _ = fs::remove_dir_all(&loose);
}

#[test]
fn an_unparseable_workflow_toml_that_configures_hooks_is_an_error() {
    let files = files(&[(
        "wf/workflow.toml",
        "[[run.hooks]]\nevent = \"stage_start\"\nscript = \"sed 's/\\(x\\)/y/'\"\n",
    )]);
    let lowered = load(
        "wf/workflow.fabro",
        &dot(r#"
            a [prompt="x"]
            start -> a -> exit
        "#),
        &files,
        &CompileInputs::new(),
    );
    let codes: Vec<String> = lowered
        .diagnostics
        .iter()
        .map(|d| d.code.to_string())
        .collect();
    assert!(codes.contains(&"fabro.hooks.toml".to_string()), "{codes:?}");
    assert!(
        lowered.graph.is_none(),
        "a hook that cannot be read is never skipped silently"
    );
    // The same broken file without hooks stays a warning.
    let plain = self::files(&[("wf/workflow.toml", "[run]\ngoal = \"bad \\( escape\"\n")]);
    let lowered = load(
        "wf/workflow.fabro",
        &dot(r#"
            a [prompt="x"]
            start -> a -> exit
        "#),
        &plain,
        &CompileInputs::new(),
    );
    assert!(lowered.graph.is_some(), "{:?}", lowered.diagnostics);
    assert!(
        lowered
            .diagnostics
            .iter()
            .any(|d| d.code == "fabro.workflow_toml")
    );
}

#[test]
fn hooks_load_from_every_layer_and_merge_by_id() {
    let files = files(&[
        (
            ".fabro/project.toml",
            "[[run.hooks]]\nid = \"guard\"\nevent = \"stage_start\"\nscript = \"project-guard\"\n\
             [[run.hooks]]\nevent = \"run_complete\"\nurl = \"https://example.test/done\"\n",
        ),
        (
            "wf/workflow.toml",
            "[[run.hooks]]\nid = \"guard\"\nevent = \"stage_start\"\nscript = \"workflow-guard\"\nmatcher = \"^agent$\"\n\
             [[run.hooks]]\nevent = \"checkpoint_saved\"\nscript = \"never\"\n",
        ),
    ]);
    let inputs = CompileInputs::new().with_var(
        SETTINGS_HOOKS_VAR,
        "[[run.hooks]]\nevent = \"run_start\"\nscript = \"user-start\"\nsandbox = false\n",
    );
    let lowered = load(
        "wf/workflow.fabro",
        &dot(r#"
            a [prompt="x"]
            start -> a -> exit
        "#),
        &files,
        &inputs,
    );
    let codes: Vec<String> = lowered
        .diagnostics
        .iter()
        .map(|d| d.code.to_string())
        .collect();
    assert!(
        codes.contains(&"fabro.hooks.checkpoint_saved".to_string()),
        "{codes:?}"
    );
    assert!(lowered.diagnostics.errors().count() == 0, "{codes:?}");
    let graph = lowered.graph.expect("lowers");
    let hooks = graph.params["attractor.hooks"]
        .as_array()
        .expect("hook list")
        .clone();
    let summary: Vec<(String, String, String)> = hooks
        .iter()
        .map(|h| {
            (
                h["event"].as_str().unwrap_or("?").to_owned(),
                h["command"]
                    .as_str()
                    .or(h["url"].as_str())
                    .unwrap_or("?")
                    .to_owned(),
                h["source"].as_str().unwrap_or("?").to_owned(),
            )
        })
        .collect();
    assert_eq!(summary, vec![
        (
            "run_start".into(),
            "user-start".into(),
            "settings.toml".into()
        ),
        (
            "stage_start".into(),
            "workflow-guard".into(),
            "wf/workflow.toml".into()
        ),
        (
            "run_complete".into(),
            "https://example.test/done".into(),
            ".fabro/project.toml".into()
        ),
        (
            "checkpoint_saved".into(),
            "never".into(),
            "wf/workflow.toml".into()
        ),
    ]);
    assert_eq!(hooks[1]["matcher"], json!("^agent$"));
    assert_eq!(hooks[0]["sandbox"], json!(false));
}

/// `[run.agent.mcps]` from the three settings layers lands on every agent
/// node (never on a prompt node), merged by name with the higher layer
/// winning, interpolated, with secrets as `$secret` references; a nested
/// workflow's agents inherit the parent's servers.
#[test]
fn mcps_lower_onto_agent_nodes_and_into_nested_workflows() {
    let files = files(&[
        (
            "wf/workflow.toml",
            "[run.agent.mcps.notes]\ntype = \"stdio\"\ncommand = [\"srv\", \"{{ inputs.root }}\"]\nenv = { TOKEN = \"{{ secrets.NOTES }}\" }\ntool_timeout = \"90s\"\n[run.agent.mcps.gone]\ntype = \"http\"\nurl = \"http://gone\"\nenabled = false\n",
        ),
        (
            ".fabro/project.toml",
            "[run.agent.mcps.notes]\ntype = \"http\"\nurl = \"http://low\"\n[run.agent.mcps.gone]\ntype = \"http\"\nurl = \"http://gone\"\n",
        ),
        (
            "wf/child.fabro",
            "digraph C { start [shape=Mdiamond] exit [shape=Msquare] inner [prompt=\"x\"] start -> inner -> exit }",
        ),
    ]);
    let inputs = CompileInputs::new().with_input("root", "/srv").with_var(
        SETTINGS_HOOKS_VAR,
        "[run.agent.mcps.user]\ntype = \"http\"\nurl = \"http://user\"\n",
    );
    let lowered = load(
        "wf/w.fabro",
        &dot(r#"
            graph [backend="api", default_model="m"]
            a [prompt="a"]
            p [shape=tab, prompt="p"]
            child [shape=house, stack.child_workflow="child.fabro"]
            start -> a -> p -> child -> exit
        "#),
        &files,
        &inputs,
    );
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    let graph = lowered.graph.expect("graph");
    let mcps = &node(&graph, "a").step.config["mcps"];
    let names: Vec<&str> = mcps
        .as_array()
        .expect("a list")
        .iter()
        .map(|s| s["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, ["notes", "user"], "merged by name, `gone` disabled");
    assert_eq!(mcps[0]["transport"]["type"], json!("stdio"));
    assert_eq!(
        mcps[0]["transport"]["command"],
        json!(["srv", "/srv"]),
        "the workflow layer wins and interpolates"
    );
    assert_eq!(
        mcps[0]["transport"]["env"]["TOKEN"],
        json!({"$secret": "NOTES"})
    );
    assert_eq!(mcps[0]["tool_timeout_ms"], json!(90_000));
    assert_eq!(mcps[0]["startup_timeout_ms"], json!(10_000));
    assert_eq!(mcps[0]["source"], json!("wf/workflow.toml"));
    assert_eq!(mcps[1]["transport"]["url"], json!("http://user"));
    assert!(
        node(&graph, "p").step.config.get("mcps").is_none(),
        "a prompt node has no tools"
    );
    let child = lowered.children.first().expect("the child graph");
    let inner = child
        .body
        .nodes
        .iter()
        .find(|n| n.name == "inner")
        .expect("inner");
    assert_eq!(
        inner.step.config["mcps"].as_array().map(Vec::len),
        Some(2),
        "the nested workflow inherits the servers"
    );
}

/// `[run.agent] skills` names extra skill directories: a Petri extension
/// Fabro refuses, so it warns, and it reaches agent nodes (not prompt
/// nodes) as `skill_dirs`. A value that is not a list of paths is refused.
#[test]
fn run_agent_skills_is_a_warned_extension() {
    let lowered = |toml: &str| {
        let files = files(&[("wf/workflow.toml", toml)]);
        load(
            "wf/workflow.fabro",
            &dot(r#"
                a [prompt="Work."]
                p [shape=tab, prompt="Summarize."]
                start -> a -> p -> exit
            "#),
            &files,
            &CompileInputs::new(),
        )
    };
    let good =
        lowered("_version = 1\n[run.agent]\nskills = [\"own/skills\", \"/shared/skills\"]\n");
    let codes: Vec<String> = good
        .diagnostics
        .iter()
        .map(|d| d.code.to_string())
        .collect();
    assert_eq!(codes, ["fabro.petri_extension"], "{:?}", good.diagnostics);
    let graph = good.graph.expect("lowers");
    let config = |name: &str| {
        graph
            .body
            .nodes
            .iter()
            .find(|n| n.name == name)
            .expect("node")
            .step
            .config
            .clone()
    };
    assert_eq!(
        config("a")["skill_dirs"],
        serde_json::json!(["own/skills", "/shared/skills"])
    );
    assert!(
        config("p").get("skill_dirs").is_none(),
        "a prompt node has no tools, so no skills"
    );
    let none = lowered("_version = 1\n[run.agent]\nskills = []\n");
    assert!(
        none.diagnostics.iter().next().is_none(),
        "{:?}",
        none.diagnostics
    );
    for bad in [
        "[run.agent]\nskills = { enabled = true }\n",
        "[run.agent]\nskills = [\"\"]\n",
        "[run.agent]\nskills = [1]\n",
    ] {
        let refused = lowered(bad);
        let codes: Vec<String> = refused
            .diagnostics
            .iter()
            .map(|d| d.code.to_string())
            .collect();
        assert_eq!(
            codes,
            ["unsupported.workflow_toml.run.agent.skills"],
            "{bad}: {:?}",
            refused.diagnostics
        );
    }
}

/// Every agent node carries the reference sub-agent configuration (on,
/// Pebble's open-session bound); a prompt node, which runs no tools, does
/// not; and the `[run.agent] subagents` key stays refused as Fabro refuses
/// it, with a hint that says the tools are always on.
#[test]
fn agent_nodes_carry_the_reference_subagent_configuration() {
    use frontend_attractor::subagents::{CONFIG_KEY, DEFAULT_MAX_OPEN_SESSIONS, SubagentConfig};
    let graph = lower_ok(&dot(r#"
        graph [backend="api", default_model="test/model"]
        agent [prompt="delegate"]
        ask [shape=tab, prompt="summarize"]
        start -> agent -> ask -> exit
    "#));
    let agent = &node(&graph, "agent").step.config;
    let read: SubagentConfig =
        serde_json::from_value(agent[CONFIG_KEY].clone()).expect("the configuration reads");
    assert_eq!(read, SubagentConfig::reference());
    assert!(read.enabled);
    assert_eq!(read.max_open_sessions, DEFAULT_MAX_OPEN_SESSIONS);
    assert!(
        node(&graph, "ask").step.config.get(CONFIG_KEY).is_none(),
        "a prompt node runs no tools"
    );

    let files = files(&[(
        "wf/workflow.toml",
        "[run.agent]\nsubagents = { enabled = false }\n",
    )]);
    let lowered = load(
        "wf/workflow.fabro",
        &dot(r#"
            a [shape=parallelogram, script="true"]
            start -> a -> exit
        "#),
        &files,
        &CompileInputs::new(),
    );
    let refusal = lowered
        .diagnostics
        .iter()
        .find(|d| d.code.as_str() == "unsupported.workflow_toml.key")
        .expect("the key is refused");
    assert!(refusal.is_error());
    assert!(
        refusal.message.contains("run.agent.subagents")
            && refusal
                .message
                .contains("always available to native agents"),
        "{}",
        refusal.message
    );
}

/// `[run.clone]` lowers onto the launch parameter with Fabro's defaults
/// (enabled, 100 commits), the repository the host bound rides beside it,
/// and the root `start` stage reads the same entry as its `checkout`.
#[test]
fn run_clone_lands_on_the_launch_param_with_the_bound_repository() {
    let lowered = load(
        "wf/workflow.fabro",
        &dot("c [shape=parallelogram, script=\"true\"]\nstart -> c -> exit"),
        &files(&[("wf/workflow.toml", "_version = 1\n")]),
        &CompileInputs::new(),
    );
    let graph = lowered.graph.expect("lowers");
    assert_eq!(
        graph.params["fabro.launch"]["clone"],
        json!({ "enabled": true, "depth": 100, "repository": null }),
        "Fabro's defaults, no repository when the host bound none"
    );
    assert_eq!(
        node(&graph, "start").step.config["checkout"],
        json!({ "enabled": true, "depth": 100, "repository": null }),
        "the start stage carries the same clone settings as its checkout"
    );

    let lowered = load(
        "wf/workflow.fabro",
        &dot("c [shape=parallelogram, script=\"true\"]\nstart -> c -> exit"),
        &files(&[(
            "wf/workflow.toml",
            "_version = 1\n[run.clone]\nenabled = false\ndepth = 0\n",
        )]),
        &CompileInputs::new().with_var(frontend::REPOSITORY_VAR, "/srv/repo"),
    );
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    assert!(
        !lowered
            .diagnostics
            .iter()
            .any(|d| d.code.as_str() == "ignored.workflow_toml.run.clone"),
        "[run.clone] is applied, not ignored"
    );
    let graph = lowered.graph.expect("lowers");
    assert_eq!(
        graph.params["fabro.launch"]["clone"],
        json!({ "enabled": false, "depth": 0, "repository": "/srv/repo" })
    );

    let lowered = load(
        "wf/workflow.fabro",
        &dot("c [shape=parallelogram, script=\"true\"]\nstart -> c -> exit"),
        &files(&[(
            "wf/workflow.toml",
            "_version = 1\n[run.clone]\nmirror = true\n",
        )]),
        &CompileInputs::new(),
    );
    assert!(
        lowered
            .diagnostics
            .iter()
            .any(|d| d.code.as_str() == "unsupported.workflow_toml.key"),
        "a key Fabro's clone table refuses is refused: {:?}",
        lowered.diagnostics
    );
}

/// `[run.model]` from the settings and project layers fills what
/// `workflow.toml` left unset, in Fabro's order: workflow over project over
/// settings.
#[test]
fn run_model_defaults_come_from_the_project_and_settings_layers() {
    let layered = files(&[
        (
            ".fabro/project.toml",
            "_version = 1\n[run.model]\nname = \"project-model\"\n[run.model.controls]\nreasoning_effort = \"low\"\n",
        ),
        (
            "wf/workflow.toml",
            "_version = 1\n[run.model.controls]\nreasoning_effort = \"high\"\n",
        ),
    ]);
    let inputs = CompileInputs::new().with_var(
        SETTINGS_HOOKS_VAR,
        "[run.model]\nprovider = \"openai\"\nname = \"settings-model\"\n[run.model.controls]\nspeed = \"fast\"\n",
    );
    let lowered = load(
        "wf/workflow.fabro",
        &dot("a [prompt=\"x\"]\nstart -> a -> exit"),
        &layered,
        &inputs,
    );
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    let graph = lowered.graph.expect("lowers");
    let config = &node(&graph, "a").step.config;
    assert_eq!(
        config["model"],
        json!("project-model"),
        "project over settings"
    );
    assert_eq!(
        config["provider"],
        json!("openai"),
        "settings fills what no layer above set"
    );
    assert_eq!(
        config["reasoning_effort"],
        json!("high"),
        "workflow over project"
    );
    assert_eq!(config["speed"], json!("fast"));

    // A bundle that declares no model at all takes the settings layer's.
    let lowered = load(
        "wf/workflow.fabro",
        &dot("a [prompt=\"x\"]\nstart -> a -> exit"),
        &files(&[("wf/workflow.toml", "_version = 1\n")]),
        &CompileInputs::new().with_var(
            SETTINGS_HOOKS_VAR,
            "[run.model]\nprovider = \"anthropic\"\nname = \"claude-sonnet-5\"\n",
        ),
    );
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    let graph = lowered.graph.expect("lowers");
    let config = &node(&graph, "a").step.config;
    assert_eq!(config["model"], json!("claude-sonnet-5"));
    assert_eq!(config["provider"], json!("anthropic"));
}

fn lower_model(workflow_toml: &str, body: &str, inputs: &CompileInputs) -> ir::Graph {
    let lowered = load(
        "wf/workflow.fabro",
        &dot(body),
        &files(&[("wf/workflow.toml", workflow_toml)]),
        inputs,
    );
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    lowered.graph.expect("lowers")
}

/// What the launch asks for (`petri run --model`, `--provider`, bound as the
/// `petri.launch_*` compile variables) overrides the graph's defaults and
/// `[run.model]`, each on its own, on agent and prompt nodes alike. A node's
/// own model and a stylesheet rule keep theirs. The launch parameter records
/// the launch as given.
#[test]
fn the_launch_model_overrides_the_defaults_but_not_the_node() {
    let body = r##"
        graph [default_model="graph-model", default_provider="graph-provider",
               model_stylesheet="#styled { model: styled-model; provider: styled-provider; }"]
        agent [prompt="x"]
        prompt [shape=tab, prompt="x"]
        explicit [prompt="x", model="node-model", provider="node-provider"]
        styled [prompt="x"]
        start -> agent -> prompt -> explicit -> styled -> exit
    "##;
    let workflow_toml = "[run.model]\nname = \"file-model\"\nprovider = \"file-provider\"\n";
    for (model, provider, expected_model, expected_provider) in [
        (
            "launch-model",
            "launch-provider",
            "launch-model",
            "launch-provider",
        ),
        ("launch-model", "", "launch-model", "graph-provider"),
        ("", "launch-provider", "graph-model", "launch-provider"),
        (" ", "\t", "graph-model", "graph-provider"),
    ] {
        let inputs = CompileInputs::new()
            .with_var(frontend::LAUNCH_MODEL_VAR, model)
            .with_var(frontend::LAUNCH_PROVIDER_VAR, provider);
        let graph = lower_model(workflow_toml, body, &inputs);
        for id in ["agent", "prompt"] {
            let config = &node(&graph, id).step.config;
            assert_eq!(config["model"], json!(expected_model), "{id}");
            assert_eq!(config["provider"], json!(expected_provider), "{id}");
        }
        let explicit = &node(&graph, "explicit").step.config;
        assert_eq!(explicit["model"], json!("node-model"));
        assert_eq!(explicit["provider"], json!("node-provider"));
        let styled = &node(&graph, "styled").step.config;
        assert_eq!(styled["model"], json!("styled-model"));
        assert_eq!(styled["provider"], json!("styled-provider"));
        let given = |text: &'static str| (!text.trim().is_empty()).then_some(text);
        assert_eq!(
            graph.params["fabro.launch"]["model"],
            json!(given(model)),
            "the launch parameter records the launch as given"
        );
        assert_eq!(
            graph.params["fabro.launch"]["provider"],
            json!(given(provider))
        );
    }

    // With no graph defaults, each half replaces its `[run.model]` field
    // and keeps the other.
    for (variable, value, model, provider) in [
        (
            frontend::LAUNCH_MODEL_VAR,
            "launch-model",
            "launch-model",
            "file-provider",
        ),
        (
            frontend::LAUNCH_PROVIDER_VAR,
            "launch-provider",
            "file-model",
            "launch-provider",
        ),
    ] {
        let graph = lower_model(
            workflow_toml,
            "agent [prompt=\"x\"]; start -> agent -> exit",
            &CompileInputs::new().with_var(variable, value),
        );
        let config = &node(&graph, "agent").step.config;
        assert_eq!(config["model"], json!(model));
        assert_eq!(config["provider"], json!(provider));
    }

    // A provider alone, where nothing names a model: the node carries the
    // provider and no model; the runner picks the provider's default model
    // from its catalog.
    let graph = lower_model(
        "_version = 1\n",
        "a [prompt=\"x\"]\nstart -> a -> exit",
        &CompileInputs::new().with_var(frontend::LAUNCH_PROVIDER_VAR, "openai"),
    );
    let config = &node(&graph, "a").step.config;
    assert_eq!(config.get("model"), None, "{config}");
    assert_eq!(config["provider"], json!("openai"));
}

/// A node that names its own model, by attribute or stylesheet rule, keeps
/// the launch's provider off it: its provider comes from the node and the
/// defaults, so the launch never pairs its provider with a model it did not
/// choose.
#[test]
fn a_node_that_names_its_model_keeps_the_launch_provider_off_it() {
    let graph = lower_model(
        "_version = 1\n",
        r##"
            graph [default_provider="graph-provider",
                   model_stylesheet="#styled { model: styled-model; }"]
            named [prompt="x", model="node-model"]
            styled [prompt="x"]
            other [prompt="x"]
            start -> named -> styled -> other -> exit
        "##,
        &CompileInputs::new()
            .with_var(frontend::LAUNCH_MODEL_VAR, "launch-model")
            .with_var(frontend::LAUNCH_PROVIDER_VAR, "launch-provider"),
    );
    for (id, model) in [("named", "node-model"), ("styled", "styled-model")] {
        let config = &node(&graph, id).step.config;
        assert_eq!(config["model"], json!(model), "{id}");
        assert_eq!(config["provider"], json!("graph-provider"), "{id}");
    }
    let other = &node(&graph, "other").step.config;
    assert_eq!(other["model"], json!("launch-model"));
    assert_eq!(other["provider"], json!("launch-provider"));
}

/// A nested workflow's nodes take the launch's model choice too, over the
/// child graph's own defaults.
#[test]
fn the_launch_model_reaches_nested_workflows() {
    let lowered = load(
        "wf/workflow.fabro",
        &dot(r#"child [shape=house, stack.child_workflow="child.fabro"]; start -> child -> exit"#),
        &files(&[(
            "wf/child.fabro",
            r#"digraph C {
                graph [default_model="child-model", default_provider="child-provider"]
                start [shape=Mdiamond] exit [shape=Msquare]
                inner [prompt="x"]
                named [prompt="x", model="node-model"]
                start -> inner -> named -> exit
            }"#,
        )]),
        &CompileInputs::new()
            .with_var(frontend::LAUNCH_MODEL_VAR, "launch-model")
            .with_var(frontend::LAUNCH_PROVIDER_VAR, "launch-provider"),
    );
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    let child = lowered.children.first().expect("the child graph");
    let config = |name: &str| {
        &child
            .body
            .nodes
            .iter()
            .find(|n| n.name == name)
            .unwrap_or_else(|| panic!("{name}"))
            .step
            .config
    };
    assert_eq!(config("inner")["model"], json!("launch-model"));
    assert_eq!(config("inner")["provider"], json!("launch-provider"));
    assert_eq!(config("named")["model"], json!("node-model"));
    assert_eq!(config("named")["provider"], json!("child-provider"));
}

/// The host's default (`petri.default_model`, `petri.default_provider`, such
/// as a server's catalog default) sits below every other layer: a node's own
/// attribute, the graph's `default_model`, then `[run.model]` all beat it, and
/// it fills only what none of them set. The launch parameter records it as
/// given.
#[test]
fn the_host_default_model_sits_below_every_layer() {
    let default = CompileInputs::new()
        .with_var(frontend::DEFAULT_MODEL_VAR, "default-model")
        .with_var(frontend::DEFAULT_PROVIDER_VAR, "default-provider");

    // The node's attribute, then `[run.model]`, beat the default.
    let graph = lower_model(
        "_version = 1\n[run.model]\nname = \"wf-model\"\n",
        "a [prompt=\"x\", model=\"node-model\"]\nb [prompt=\"x\"]\nstart -> a -> b -> exit",
        &default,
    );
    assert_eq!(node(&graph, "a").step.config["model"], json!("node-model"));
    assert_eq!(node(&graph, "b").step.config["model"], json!("wf-model"));
    assert_eq!(
        node(&graph, "b").step.config["provider"],
        json!("default-provider"),
        "the default fills the provider no layer set"
    );
    assert_eq!(
        graph.params["fabro.launch"]["default_model"],
        json!("default-model")
    );
    assert_eq!(
        graph.params["fabro.launch"]["default_provider"],
        json!("default-provider")
    );
    assert_eq!(graph.params["fabro.launch"]["model"], json!(null));

    // The graph's `default_model` beats the default.
    let graph = lower_model(
        "_version = 1\n",
        "graph [default_model=\"graph-model\"]\na [prompt=\"x\"]\nstart -> a -> exit",
        &default,
    );
    assert_eq!(node(&graph, "a").step.config["model"], json!("graph-model"));

    // Nothing else named a model: the default applies.
    let graph = lower_model(
        "_version = 1\n",
        "a [prompt=\"x\"]\nstart -> a -> exit",
        &default,
    );
    let config = &node(&graph, "a").step.config;
    assert_eq!(config["model"], json!("default-model"));
    assert_eq!(config["provider"], json!("default-provider"));

    // The launch beats the default.
    let graph = lower_model(
        "_version = 1\n",
        "a [prompt=\"x\"]\nstart -> a -> exit",
        &default
            .clone()
            .with_var(frontend::LAUNCH_MODEL_VAR, "launch-model"),
    );
    let config = &node(&graph, "a").step.config;
    assert_eq!(config["model"], json!("launch-model"));
    assert_eq!(config["provider"], json!("default-provider"));

    // Neither: the parameter says so, and the node names nothing.
    let graph = lower_model(
        "_version = 1\n",
        "a [prompt=\"x\"]\nstart -> a -> exit",
        &CompileInputs::new(),
    );
    assert_eq!(node(&graph, "a").step.config.get("model"), None);
    for key in ["model", "provider", "default_model", "default_provider"] {
        assert_eq!(graph.params["fabro.launch"][key], json!(null), "{key}");
    }
}

/// `[environments.<id>]` and `[run.environment]` come from every settings
/// layer: a bundle names an environment only the host's layer declares (a
/// Fabro server's catalog), a bundle with no `[run.environment]` takes the
/// host's selection, the bundle's keys win over the project's over the
/// host's key by key, and every diagnostic names the layer its setting came
/// from.
#[test]
fn environments_come_from_every_layer_and_the_bundle_wins() {
    let host = "[run.environment]\nid = \"docker-small\"\n\
                [environments.docker-small]\nprovider = \"docker\"\n\
                [environments.docker-small.image]\ndocker = \"catalog/runner:1\"\n\
                [environments.docker-small.env]\nFROM = \"catalog\"\nSHARED = \"catalog\"\n\
                [environments.docker-small.lifecycle]\npreserve = false\n\
                [environments.local]\nprovider = \"local\"\n\
                [environments.local.image]\ndocker = \"catalog/host:1\"\n";
    let inputs = CompileInputs::new().with_var(SETTINGS_HOOKS_VAR, host);
    let lower = |files: &dyn frontend::FileSource| {
        let lowered = load("wf/w.fabro", &dot("start -> exit"), files, &inputs);
        assert!(
            !lowered.diagnostics.has_errors(),
            "{:?}",
            lowered.diagnostics
        );
        let files: Vec<(String, String)> = lowered
            .diagnostics
            .iter()
            .map(|d| (d.code.to_string(), d.span.file.to_string()))
            .collect();
        (lowered.graph.expect("lowers"), files)
    };

    // The bundle names the host's environment and declares none itself.
    let (graph, diagnostics) = lower(&files(&[(
        "wf/workflow.toml",
        "[run.environment]\nid = \"docker-small\"\n",
    )]));
    let environment = &graph.params["fabro.environment"];
    assert_eq!(environment["id"], json!("docker-small"));
    assert_eq!(environment["provider"], json!("docker"));
    assert_eq!(environment["image"], json!("catalog/runner:1"));
    assert_eq!(environment["env"]["FROM"], json!("catalog"));
    assert_eq!(
        Fabro::new()
            .launch_settings(&graph)
            .sandbox_backend
            .as_deref(),
        Some("docker")
    );
    assert!(
        matches!(&graph.scopes[0].runtime.target, ir::RuntimeTarget::Container { image, .. } if image == "catalog/runner:1"),
        "{:?}",
        graph.scopes[0].runtime.target
    );
    assert!(
        diagnostics.is_empty(),
        "the host's `lifecycle` is the platform's key, known and silent: {diagnostics:?}"
    );

    // A bundle with no `workflow.toml` at all runs in the host's selection.
    let (graph, _) = lower(&frontend::NoFiles);
    assert_eq!(
        graph.params["fabro.environment"]["id"],
        json!("docker-small")
    );

    // Key by key: the bundle over the project over the host.
    let (graph, _) = lower(&files(&[
        (
            ".fabro/project.toml",
            "[environments.docker-small.image]\ndocker = \"project/runner:2\"\n\
             [environments.docker-small.env]\nSHARED = \"project\"\n",
        ),
        (
            "wf/workflow.toml",
            "[run.environment]\nid = \"docker-small\"\n\
             [environments.docker-small.env]\nSHARED = \"bundle\"\nOWN = \"bundle\"\n",
        ),
    ]));
    let environment = &graph.params["fabro.environment"];
    assert_eq!(environment["image"], json!("project/runner:2"));
    assert_eq!(
        environment["env"],
        json!({ "FROM": "catalog", "SHARED": "bundle", "OWN": "bundle" })
    );
    let (graph, _) = lower(&files(&[(
        "wf/workflow.toml",
        "[run.environment]\nid = \"docker-small\"\n\
         [environments.docker-small.image]\ndocker = \"bundle/runner:3\"\n",
    )]));
    assert_eq!(
        graph.params["fabro.environment"]["image"],
        json!("bundle/runner:3")
    );

    // Refusals name the layer the offending setting came from.
    let refused = |files: &dyn frontend::FileSource| {
        load("wf/w.fabro", &dot("start -> exit"), files, &inputs)
            .diagnostics
            .iter()
            .filter(|d| d.code.starts_with("unsupported."))
            .map(|d| (d.code.to_string(), d.span.file.to_string()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        refused(&files(&[(
            "wf/workflow.toml",
            "[run.environment]\nid = \"nowhere\"\n"
        )])),
        [(
            "unsupported.workflow_toml.run.environment".to_string(),
            "wf/workflow.toml".to_string()
        )],
        "an id no layer declares"
    );
    // The host's image on a local environment is ignored, as Fabro ignores
    // it on the host, and the warning names the layer that set it.
    let lowered = load(
        "wf/w.fabro",
        &dot("start -> exit"),
        &files(&[("wf/workflow.toml", "[run.environment]\nid = \"local\"\n")]),
        &inputs,
    );
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    let warnings: Vec<(String, String)> = lowered
        .diagnostics
        .iter()
        .map(|d| (d.code.to_string(), d.span.file.to_string()))
        .collect();
    assert_eq!(warnings, [(
        "ignored.workflow_toml.environments.local.image".to_string(),
        "settings.toml".to_string()
    )]);
    assert_eq!(
        lowered.graph.expect("lowers").params["fabro.environment"]["image"],
        json!(null)
    );
}

/// Fabro's platform-only environment keys are known in every layer and
/// warn nothing: a project layer like the Fabro repository's own, which
/// declares a Daytona environment with a Dockerfile, a lifecycle and
/// labels, lowers clean. A key neither Petri nor Fabro's table accepts
/// still warns, against the layer that set it.
#[test]
fn platform_environment_keys_are_known_and_unknown_ones_warn() {
    let project = "[run.environment]\nid = \"fabro-dev\"\n\
                   [environments.fabro-dev]\nprovider = \"daytona\"\n\
                   [environments.fabro-dev.image]\ndockerfile = { path = \"Dockerfile\" }\n\
                   [environments.fabro-dev.resources]\ncpu = 8\nmemory = \"16GB\"\n\
                   [environments.fabro-dev.lifecycle]\nauto_stop = \"30m\"\n\
                   [environments.fabro-dev.labels]\nrepo = \"fabro-sh/fabro\"\n";
    let lower = |project: &str, workflow: &str| {
        let lowered = load(
            "wf/w.fabro",
            &dot("start -> exit"),
            &files(&[
                (".fabro/project.toml", project),
                ("wf/workflow.toml", workflow),
            ]),
            &CompileInputs::new(),
        );
        let diagnostics: Vec<(String, String)> = lowered
            .diagnostics
            .iter()
            .map(|d| (d.code.to_string(), d.span.file.to_string()))
            .collect();
        (lowered.graph.expect("lowers"), diagnostics)
    };

    let (graph, diagnostics) = lower(project, "");
    assert!(
        diagnostics.is_empty(),
        "the platform's keys are known and silent: {diagnostics:?}"
    );
    let environment = &graph.params["fabro.environment"];
    assert_eq!(environment["id"], json!("fabro-dev"));
    assert_eq!(environment["provider"], json!("daytona"));
    assert_eq!(graph.params["fabro.launch"]["cpu_cores"], json!(8));

    // The same keys on an environment no run selects, and `cwd` and
    // `network` on the selected one, are as silent.
    let with_cwd = project.replacen(
        "provider = \"daytona\"\n",
        "provider = \"daytona\"\ncwd = \"/work\"\n",
        1,
    );
    let (_, diagnostics) = lower(
        &format!(
            "{with_cwd}[environments.fabro-dev.network]\nmode = \"block\"\n\
             [environments.other]\nprovider = \"docker\"\n\
             [environments.other.lifecycle]\npreserve = true\n"
        ),
        "",
    );
    assert!(diagnostics.is_empty(), "{diagnostics:?}");

    // A key outside Fabro's table warns, naming the layer, in every layer
    // and on every environment; the graph still lowers.
    let (_, diagnostics) = lower(
        &format!("{project}[environments.other]\nprovider = \"docker\"\nprovisioner = \"x\"\n"),
        "[environments.fabro-dev]\nimage_pull = \"always\"\n\
         [run.environment]\nid = \"fabro-dev\"\nlabel = \"y\"\n",
    );
    assert_eq!(diagnostics, [
        (
            "ignored.workflow_toml.environments.fabro-dev.image_pull".to_string(),
            "wf/workflow.toml".to_string()
        ),
        (
            "ignored.workflow_toml.environments.other.provisioner".to_string(),
            ".fabro/project.toml".to_string()
        ),
        (
            "ignored.workflow_toml.run.environment.label".to_string(),
            "wf/workflow.toml".to_string()
        ),
    ]);
}

/// The launch's environment (`petri run --environment`, the
/// `petri.launch_environment` variable) selects the id over every layer's
/// `[run.environment]`, as `fabro run --environment` does: a bundle whose
/// project file selects one environment runs in the launch's; a bundle with
/// no `[run.environment]` anywhere runs in it too; an id no layer declares
/// is refused.
#[test]
fn the_launch_environment_wins_over_every_layer() {
    let host = "[environments.local]\nprovider = \"local\"\n\
                [environments.big]\nprovider = \"daytona\"\n\
                [environments.big.resources]\ncpu = 8\n";
    let files = files(&[(".fabro/project.toml", "[run.environment]\nid = \"big\"\n")]);
    let launch = |id: &str| {
        CompileInputs::new()
            .with_var(SETTINGS_HOOKS_VAR, host)
            .with_var(frontend::LAUNCH_ENVIRONMENT_VAR, id)
    };
    let lowered = load(
        "wf/w.fabro",
        &dot("start -> exit"),
        &files,
        &launch("local"),
    );
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    let graph = lowered.graph.expect("lowers");
    assert_eq!(graph.params["fabro.environment"]["id"], json!("local"));
    assert_eq!(
        Fabro::new()
            .launch_settings(&graph)
            .sandbox_backend
            .as_deref(),
        Some("host")
    );
    // Without the launch, the project's selection stands.
    let graph = lower_ok_with(
        &dot("start -> exit"),
        &files,
        &CompileInputs::new().with_var(SETTINGS_HOOKS_VAR, host),
    );
    assert_eq!(graph.params["fabro.environment"]["id"], json!("big"));
    assert_eq!(graph.params["fabro.launch"]["cpu_cores"], json!(8));
    // No `[run.environment]` anywhere: the launch alone selects.
    let graph = lower_ok_with(&dot("start -> exit"), &frontend::NoFiles, &launch("big"));
    assert_eq!(graph.params["fabro.environment"]["id"], json!("big"));
    // An id no layer declares.
    let codes: Vec<String> = load(
        "wf/w.fabro",
        &dot("start -> exit"),
        &files,
        &launch("nowhere"),
    )
    .diagnostics
    .iter()
    .map(|d| d.code.to_string())
    .collect();
    assert_eq!(codes, ["unsupported.workflow_toml.run.environment"]);
}

/// One rule for the run goal, as Fabro's run materialization orders it: the
/// launch's goal (`petri run --goal`, the `petri.launch_goal` variable) over
/// `[run] goal` over the graph's own `goal` attribute, which is the default.
/// The goal that wins is the one `Graph.params["goal"]`, every stage's
/// config and `{{ goal }}` see, and both settings forms render as the
/// graph's does.
#[test]
fn the_run_goal_overrides_the_graphs_goal_and_the_launch_overrides_both() {
    let workflow = |graph_attrs: &str| {
        dot(&format!(
            r#"
            {graph_attrs}
            a [prompt="Toward: {{{{ goal }}}}"]
            start -> a -> exit
        "#
        ))
    };
    let with_settings = files(&[(
        "wf/workflow.toml",
        "[run]\ngoal = \"Settings {{ inputs.target }}\"\n[run.inputs]\ntarget = \"main\"\n",
    )]);
    let goal_of = |text: &str, files: &dyn frontend::FileSource, inputs: &CompileInputs| {
        let lowered = load("wf/workflow.fabro", text, files, inputs);
        assert!(
            !lowered.diagnostics.has_errors(),
            "{:?}",
            lowered.diagnostics
        );
        let graph = lowered.graph.expect("lowers");
        let stage = &node(&graph, "a").step.config;
        assert_eq!(
            stage["goal"], graph.params["goal"],
            "the stage sees the run's goal"
        );
        assert_eq!(
            stage["prompt"],
            json!(format!(
                "Toward: {}",
                graph.params["goal"].as_str().expect("text")
            )),
            "`{{{{ goal }}}}` renders the run's goal"
        );
        graph.params["goal"].clone()
    };
    let inputs = CompileInputs::new().with_input("target", "main");
    let launch = |goal: &str| inputs.clone().with_var(frontend::LAUNCH_GOAL_VAR, goal);

    // The graph alone: its own goal, rendered.
    assert_eq!(
        goal_of(
            &workflow("graph [goal=\"Graph {{ inputs.target }}\"]"),
            &frontend::NoFiles,
            &inputs
        ),
        json!("Graph main")
    );
    // `[run] goal` alone: the settings' goal.
    assert_eq!(
        goal_of(&workflow(""), &with_settings, &CompileInputs::new()),
        json!("Settings main")
    );
    // Both: the settings' goal replaces the graph's.
    assert_eq!(
        goal_of(
            &workflow("graph [goal=\"Graph {{ inputs.target }}\"]"),
            &with_settings,
            &CompileInputs::new()
        ),
        json!("Settings main")
    );
    // The launch replaces both, and renders like `[run] goal`.
    assert_eq!(
        goal_of(
            &workflow("graph [goal=\"Graph goal\"]"),
            &with_settings,
            &launch("Launch {{ inputs.target }}")
        ),
        json!("Launch main")
    );
    assert_eq!(
        goal_of(
            &workflow("graph [goal=\"Graph goal\"]"),
            &frontend::NoFiles,
            &launch("Launch")
        ),
        json!("Launch")
    );
    // A blank launch goal states none.
    assert_eq!(
        goal_of(
            &workflow("graph [goal=\"Graph goal\"]"),
            &with_settings,
            &launch("  ")
        ),
        json!("Settings main")
    );
}

#[test]
fn workflow_secrets_reach_agent_processes_as_references() {
    for agent in [
        r#"backend="acp", acp.command="agent --acp""#,
        r#"backend="acp", acp.config="{\"command\":\"agent\"}""#,
        r#"backend="api", model="claude-sonnet-5-5""#,
    ] {
        let files = files(&[(
            "wf/workflow.toml",
            "[environments.default]\nprovider = \"local\"\n[run.environment.env]\nTOKEN = \"{{ secrets.REVIEW_TOKEN }}\"\n",
        )]);
        let lowered = load(
            "wf/workflow.fabro",
            &dot(&format!(r#"a [{agent}, prompt="x"]; start -> a -> exit"#)),
            &files,
            &CompileInputs::new(),
        );
        assert!(
            !lowered.diagnostics.has_errors(),
            "{:?}",
            lowered.diagnostics
        );
        let graph = lowered.graph.expect("lowers");
        assert!(!graph.scopes[0].env.contains_key("TOKEN"));
        assert_eq!(
            node(&graph, "a").step.config["env"]["TOKEN"],
            json!({"$secret": "REVIEW_TOKEN"})
        );
    }
}

#[test]
fn a_registered_provider_name_survives_lowering_refuses_an_image_and_lowers_resources() {
    let settings =
        "[run.environment]\nid = 'custom'\n[environments.custom]\nprovider = 'fixture-provider'\n";
    let text = dot("start -> exit");
    let input = files(&[("wf/workflow.toml", settings)]);
    let lowered = load("wf/workflow.fabro", &text, &input, &CompileInputs::new());
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    let graph = lowered.graph.expect("a registered kind lowers");
    assert_eq!(
        Fabro::new()
            .launch_settings(&graph)
            .sandbox_backend
            .as_deref(),
        Some("fixture-provider")
    );
    assert!(matches!(
        graph.scopes[0].runtime.target,
        ir::RuntimeTarget::HostProcess
    ));
    let image = format!("{settings}image = {{ docker = 'alpine' }}\n");
    let input = files(&[("wf/workflow.toml", &image)]);
    let lowered = load("wf/workflow.fabro", &text, &input, &CompileInputs::new());
    let codes: Vec<String> = lowered
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code.to_string())
        .collect();
    assert_eq!(codes, ["unsupported.workflow_toml.environments.custom"]);
    assert!(lowered.graph.is_none(), "silently accepted an image");
    // Resources size the run's sandbox over the registration's template, so
    // they lower exactly as Daytona's do.
    let sized = format!("{settings}resources = {{ cpu = 2, memory = '4GB', disk = '20GB' }}\n");
    let input = files(&[("wf/workflow.toml", &sized)]);
    let lowered = load("wf/workflow.fabro", &text, &input, &CompileInputs::new());
    assert!(
        lowered.diagnostics.iter().next().is_none(),
        "{:?}",
        lowered.diagnostics
    );
    let graph = lowered
        .graph
        .expect("resources lower under a registered kind");
    let launch = &graph.params["fabro.launch"];
    assert_eq!(launch["cpu_cores"], json!(2));
    assert_eq!(launch["memory_mb"], json!(4096));
    assert_eq!(launch["disk_mb"], json!(20480));
}
