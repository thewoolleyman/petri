//! The routing directive a Fabro stage reports: the last JSON object in its
//! text that carries a routing field (`outcome`, `preferred_next_label`,
//! `suggested_next_ids`, `context_updates`, `failure_reason`). What Fabro's
//! `structured_output` module scans for, in the same order and with the same
//! closed outcome set.

use std::collections::BTreeMap;

use frontend_attractor::kinds::{OUTCOMES, StageOutcome};
use frontend_attractor::labels::strip_accelerator;
use ir::Value;
use serde_json::json;
use smol_str::SmolStr;

use crate::outcome::Stage;

/// The fields that make a JSON object a routing directive.
pub const ROUTING_FIELDS: &[&str] = &[
    "preferred_next_label",
    "outcome",
    "failure_reason",
    "suggested_next_ids",
    "context_updates",
];

/// A parsed directive.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Directive {
    /// One of the four Fabro outcomes, when the object names one.
    pub outcome:            Option<StageOutcome>,
    pub failure_reason:     Option<String>,
    /// The preferred label, accelerator stripped.
    pub preferred_label:    Option<String>,
    pub suggested_next_ids: Vec<String>,
    pub context_updates:    BTreeMap<SmolStr, Value>,
}

/// Why a directive was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DirectiveError {
    #[error("no JSON object in the response carries a routing field ({})", ROUTING_FIELDS.join(", "))]
    Missing,
    #[error("the routing JSON object is malformed: {0}")]
    Malformed(String),
    #[error("`outcome` must be one of {}, not `{value}`", OUTCOMES.join(", "))]
    UnknownOutcome { value: String },
}

/// Every top-level JSON object in `text`, outermost only, as slices.
///
/// An opening brace that never closes (prose quoting a lone `{`) must not
/// swallow every object after it: when the scan ends with an object still
/// open, it resumes one byte past that object's opening brace, so the stray
/// brace costs only itself and a trailing routing object is still found.
fn json_objects(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(unclosed) = scan_objects(text, from, &mut out) {
        from = unclosed + 1;
    }
    out
}

/// Scan `text` from byte `from`, pushing each complete top-level object onto
/// `out`. Returns the start of an object still open at the end, if any.
fn scan_objects<'a>(text: &'a str, from: usize, out: &mut Vec<&'a str>) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0_usize;
    let mut start = None;
    let mut in_string = false;
    let mut escaped = false;
    for (index, byte) in bytes.iter().enumerate().skip(from) {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' if depth > 0 => in_string = true,
            b'{' => {
                if depth == 0 {
                    start = Some(index);
                }
                depth += 1;
            }
            b'}' if depth > 0 => {
                depth -= 1;
                if depth == 0
                    && let Some(s) = start.take()
                    && text.is_char_boundary(s)
                    && text.is_char_boundary(index + 1)
                {
                    out.push(&text[s..=index]);
                }
            }
            _ => {}
        }
    }
    start
}

/// The last complete top-level JSON object in `text`.
pub(crate) fn last_json_object(text: &str) -> Option<&str> {
    json_objects(text).into_iter().next_back()
}

fn has_routing_field(object: &serde_json::Map<String, Value>) -> bool {
    ROUTING_FIELDS.iter().any(|f| object.contains_key(*f))
}

/// The directive in `text`: the last JSON object with a routing field.
pub fn parse(text: &str) -> Result<Directive, DirectiveError> {
    let mut last: Option<serde_json::Map<String, Value>> = None;
    let mut malformed: Option<String> = None;
    for candidate in json_objects(text) {
        match serde_json::from_str::<Value>(candidate) {
            Ok(Value::Object(object)) if has_routing_field(&object) => last = Some(object),
            Ok(_) => {}
            Err(error) => {
                if ROUTING_FIELDS
                    .iter()
                    .any(|f| candidate.contains(&format!("\"{f}\"")))
                {
                    malformed = Some(error.to_string());
                }
            }
        }
    }
    let Some(object) = last else {
        return Err(malformed.map_or(DirectiveError::Missing, DirectiveError::Malformed));
    };
    let mut directive = Directive::default();
    if let Some(outcome) = object.get("outcome").and_then(Value::as_str) {
        directive.outcome =
            Some(
                StageOutcome::parse(outcome).ok_or_else(|| DirectiveError::UnknownOutcome {
                    value: outcome.to_string(),
                })?,
            );
    }
    directive.failure_reason = object
        .get("failure_reason")
        .and_then(Value::as_str)
        .map(str::to_string);
    directive.preferred_label = object
        .get("preferred_next_label")
        .and_then(Value::as_str)
        .map(|l| strip_accelerator(l).to_string());
    if let Some(ids) = object.get("suggested_next_ids").and_then(Value::as_array) {
        directive.suggested_next_ids = ids
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
    }
    if let Some(updates) = object.get("context_updates").and_then(Value::as_object) {
        directive.context_updates = updates
            .iter()
            .map(|(k, v)| (SmolStr::new(k), v.clone()))
            .collect();
    }
    Ok(directive)
}

impl Directive {
    /// Apply the routing fields to a stage result.
    pub fn apply_to(self, stage: &mut Stage) {
        let output_fields = self.output_fields();
        if let Some(outcome) = self.outcome {
            stage.outcome = outcome;
            if outcome == StageOutcome::Failed {
                stage.failure_reason = self.failure_reason;
                stage.failure_class.clear();
            }
        }
        for (key, value) in output_fields {
            stage.output.insert(key, value);
        }
        stage.context_updates.extend(self.context_updates);
    }

    /// The output fields a step reports from this directive.
    pub fn output_fields(&self) -> serde_json::Map<String, Value> {
        let mut out = serde_json::Map::new();
        if let Some(label) = &self.preferred_label {
            out.insert("preferred_label".into(), json!(label));
        }
        if !self.suggested_next_ids.is_empty() {
            out.insert("suggested_next_ids".into(), json!(self.suggested_next_ids));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_routing_object_wins_and_prose_is_ignored() {
        let text = r#"Working... {"note": 1}
        {"outcome": "failed", "failure_reason": "tests", "preferred_next_label": "[F] Fix"}
        done {"context_updates": {"verified": true}, "suggested_next_ids": ["a", "b"]}"#;
        let directive = parse(text).expect("parses");
        assert_eq!(directive.outcome, None);
        assert_eq!(directive.suggested_next_ids, ["a", "b"]);
        assert_eq!(
            directive.context_updates.get("verified"),
            Some(&json!(true))
        );
        let first =
            parse(r#"{"outcome": "failed", "preferred_next_label": "[F] Fix"}"#).expect("parses");
        assert_eq!(first.outcome, Some(StageOutcome::Failed));
        assert_eq!(first.preferred_label.as_deref(), Some("Fix"));
    }

    #[test]
    fn an_unbalanced_brace_in_prose_does_not_hide_the_trailing_label() {
        // The misrouted review (bd-ib-7smzwo): prose quoting two lone opening
        // braces, then the label object the review contract asks for.
        let text = r#"The opener is built as "{" + "{" in the test, which is fine.
        Advisory only.
        {"preferred_next_label": "approve"}"#;
        let directive = parse(text).expect("parses");
        assert_eq!(directive.preferred_label.as_deref(), Some("approve"));
        assert_eq!(
            parse(r#"Quoting "{" alone, and no label object."#).expect_err("missing"),
            DirectiveError::Missing
        );
    }

    #[test]
    fn refusals_are_specific() {
        assert_eq!(
            parse("no json here").expect_err("missing"),
            DirectiveError::Missing
        );
        assert_eq!(
            parse(r#"{"outcome": "error"}"#).expect_err("unknown"),
            DirectiveError::UnknownOutcome {
                value: "error".into(),
            }
        );
        assert!(matches!(
            parse(r#"{"outcome": "failed", }"#).expect_err("malformed"),
            DirectiveError::Malformed(_)
        ));
        assert_eq!(
            parse(r#"{"summary": "ok"}"#).expect_err("missing"),
            DirectiveError::Missing
        );
    }

    #[test]
    fn nested_and_string_braces_do_not_split_objects() {
        let text = r#"{"context_updates": {"a": {"b": "}"}}, "outcome": "succeeded"}"#;
        let directive = parse(text).expect("parses");
        assert_eq!(directive.outcome, Some(StageOutcome::Succeeded));
        assert_eq!(directive.context_updates["a"], json!({ "b": "}" }));
    }
}
