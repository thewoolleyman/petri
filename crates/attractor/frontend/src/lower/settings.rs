//! What a run's settings ask of the lowering, as resolved values.
//!
//! Attractor lowers a DOT file; it reads no settings file of its own. A
//! host frontend that has one (Fabro's `workflow.toml`, `.fabro/project.toml`
//! and the user settings layer) resolves it into a [`RunSettings`] and hands
//! that to [`crate::lower`]. The default is a bare run: no model default, no
//! environment, no prepare steps, no hooks, no MCP servers, the reference
//! compaction values, and a checkout of the bound repository.

use std::collections::BTreeMap;

use frontend::{Diagnostics, Span};
use serde_json::{Value, json};

use super::compaction::CompactionSettings;
use super::skills::SkillSettings;
use crate::hooks::HookDefinition;
use crate::mcps::McpServer;
use crate::model::{AttrValue, Attrs, EdgeDecl, NodeDecl, Workflow};

/// The reserved id prefix of the synthetic nodes `[run.prepare]` steps lower
/// to: `run_prepare_1`, `run_prepare_2`, and so on.
pub const PREPARE_NODE_PREFIX: &str = "run_prepare_";

/// What the run's settings ask of the lowering. Field names follow the
/// `workflow.toml` sections they come from in Fabro; every value is
/// resolved (rendered, merged across layers) before it arrives here.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RunSettings {
    /// The run goal the settings state, rendered (`[run] goal`, or the
    /// launch's goal above it). It replaces the graph's own `goal`
    /// attribute; `None` leaves the graph's in charge.
    pub goal:               Option<String>,
    /// Model defaults for LLM nodes (`[run.model]`, every layer applied).
    pub model:              ModelDefaults,
    /// The model the launch asks for, over [`Self::model`] and the graph's
    /// defaults, below a model a node names itself.
    pub model_override:     ModelOverride,
    /// The resolved execution environment, when the run names one
    /// (`[run.environment]`).
    pub environment:        Option<Environment>,
    /// Setup steps that run before any node, in order (`[run.prepare]`).
    pub prepare:            Vec<PrepareStep>,
    /// The per-step timeout of the prepare steps, in milliseconds.
    pub prepare_timeout_ms: u64,
    /// Agent context compaction (`lower::compaction`).
    pub compaction:         CompactionSettings,
    /// The workflow's own skill directories (`[run.agent] skills`).
    pub skills:             SkillSettings,
    /// Whether the run starts from a checkout of the repository and how
    /// much history it carries (`[run.clone]`).
    pub clone:              CloneSettings,
    /// The run's merged hooks, carried on the stage steps (`[[run.hooks]]`).
    pub hooks:              Vec<HookDefinition>,
    /// The run's merged MCP servers, carried on every agent node
    /// (`[run.agent.mcps]`).
    pub mcps:               Vec<McpServer>,
}

/// `[run.clone]`, with Fabro's defaults: enabled, 100 commits of history.
/// `depth = 0` is the full history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloneSettings {
    pub enabled: bool,
    pub depth:   i64,
}

impl Default for CloneSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            depth:   100,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModelDefaults {
    pub provider:         Option<String>,
    pub name:             Option<String>,
    pub reasoning_effort: Option<String>,
    /// `controls.speed`: `standard` or `fast`, the default an LLM node gets
    /// when it names none.
    pub speed:            Option<String>,
    /// `fallbacks`: the model-keyed chains as written, references in their
    /// canonical spelling ([`super::fallbacks`]).
    pub fallbacks:        BTreeMap<String, Vec<String>>,
}

/// The model a launch asks for. Each field applies on its own, to an LLM node
/// that names no model of its own: a node that names one keeps its provider
/// to the node and the defaults, so the launch's provider never pairs with a
/// model the node chose.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModelOverride {
    pub provider: Option<String>,
    pub name:     Option<String>,
}

/// One environment value: a literal, or a secret name to resolve at spawn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvValue {
    Literal(String),
    Secret(String),
}

impl EnvValue {
    /// The JSON form a step config carries: the string, or a `$secret`
    /// reference.
    pub fn to_json(&self) -> Value {
        match self {
            Self::Literal(text) => Value::String(text.clone()),
            Self::Secret(name) => json!({ "$secret": name }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Environment {
    pub id:        String,
    /// `local`, `docker`, `daytona`, or a registered provider kind, as
    /// written and already validated.
    pub provider:  String,
    /// `image.docker`, when set.
    pub image:     Option<String>,
    pub env:       BTreeMap<String, EnvValue>,
    /// `resources`, as Daytona runner sizing.
    pub cpu_cores: Option<u32>,
    pub memory_mb: Option<u64>,
    pub disk_mb:   Option<u64>,
}

impl Environment {
    /// The sandbox backend this provider maps to, in the host's spelling:
    /// `local` is the host; every other name is carried as written.
    pub fn sandbox_backend(&self) -> &str {
        match self.provider.as_str() {
            "local" => "host",
            kind => kind,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrepareStep {
    /// The shell text: `script` as written, or the `command` argv joined.
    pub script: String,
    pub env:    BTreeMap<String, EnvValue>,
}

/// The merged hooks as the `Graph.params` value and the stage config value.
pub(super) fn hooks_param(hooks: &[HookDefinition]) -> Value {
    serde_json::to_value(hooks).unwrap_or(Value::Array(Vec::new()))
}

/// The merged MCP servers as an agent node's `mcps` config value.
pub(super) fn mcps_param(servers: &[McpServer]) -> Value {
    serde_json::to_value(servers).unwrap_or(Value::Array(Vec::new()))
}

/// Insert the prepare steps as command nodes between `start` and
/// its successors. Returns the env each synthetic node carries, keyed by
/// node id, for the command config.
pub(super) fn insert_prepare_nodes(
    workflow: &mut Workflow,
    start: &str,
    settings: &RunSettings,
    span: &Span,
    diags: &mut Diagnostics,
) -> BTreeMap<String, BTreeMap<String, EnvValue>> {
    let mut envs = BTreeMap::new();
    if settings.prepare.is_empty() {
        return envs;
    }
    for node in &workflow.nodes {
        if node.id.starts_with(PREPARE_NODE_PREFIX) {
            diags.error(
                "attractor.reserved_node_id",
                node.span.clone(),
                format!("`{}` is reserved for `[run.prepare]` lowering", node.id),
            );
            return envs;
        }
    }
    let mut nodes = workflow.nodes.clone();
    let mut edges: Vec<EdgeDecl> = Vec::new();
    let mut previous = start.to_string();
    let timeout = format!("{}ms", settings.prepare_timeout_ms);
    for (index, step) in settings.prepare.iter().enumerate() {
        let id = format!("{PREPARE_NODE_PREFIX}{}", index + 1);
        let mut attrs = Attrs::default();
        attrs.insert(
            "shape",
            AttrValue::Str("parallelogram".into()),
            span.clone(),
        );
        attrs.insert(
            "label",
            AttrValue::Str(format!("Prepare {}", index + 1)),
            span.clone(),
        );
        attrs.insert("script", AttrValue::Str(step.script.clone()), span.clone());
        attrs.insert("timeout", AttrValue::Str(timeout.clone()), span.clone());
        attrs.insert("on_failure", AttrValue::Str("exit".into()), span.clone());
        nodes.push(NodeDecl {
            id: id.clone(),
            attrs,
            classes: vec!["run-prepare".to_string()],
            span: span.clone(),
            declared: true,
        });
        edges.push(EdgeDecl {
            from:    previous.clone(),
            to:      id.clone(),
            attrs:   Attrs::default(),
            span:    span.clone(),
            to_span: span.clone(),
        });
        envs.insert(id.clone(), step.env.clone());
        previous = id;
    }
    for edge in &workflow.edges {
        if edge.from == start {
            edges.push(EdgeDecl {
                from: previous.clone(),
                ..edge.clone()
            });
        } else {
            edges.push(edge.clone());
        }
    }
    *workflow = Workflow::from_parts(
        workflow.name.clone(),
        workflow.attrs.clone(),
        nodes,
        edges,
        workflow.span.clone(),
    );
    envs
}
