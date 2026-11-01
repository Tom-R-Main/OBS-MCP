// SPDX-License-Identifier: GPL-2.0-only
//! Pinned SDK/Zod input-error projection, never a dispatch validator.
//! Only exact exported schemas are admitted. Unsupported/lossy projections
//! return a typed refusal; a valid input never becomes an invented tool error.
use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::OnceLock,
};
const MAX_DEPTH: usize = 32;
const MAX_NODES: usize = 4096;
const MAX_INPUT_BYTES: usize = 1024 * 1024;
const MAX_ISSUES: usize = 64;
const MAX_OUTPUT: usize = 32 * 1024;
const SAFE: f64 = 9_007_199_254_740_991.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionUnavailable {
    UnknownTool,
    SchemaMismatch,
    UnsupportedSchema,
    InputLimit,
    OutputLimit,
    NoIssues,
}
impl fmt::Display for ProjectionUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownTool => "Validation projection unavailable: unknown tool",
            Self::SchemaMismatch => "Validation projection unavailable: schema mismatch",
            Self::UnsupportedSchema => "Validation projection unavailable: unsupported schema",
            Self::InputLimit => "Validation projection unavailable: input bound exceeded",
            Self::OutputLimit => "Validation projection unavailable: error bound exceeded",
            Self::NoIssues => "Validation projection unavailable: no source validation issues",
        })
    }
}
impl std::error::Error for ProjectionUnavailable {}
type Result<T> = std::result::Result<T, ProjectionUnavailable>;

/// Called only after the caller's schema validator refused admission. This
/// function cannot authorize dispatch or normalize an invalid request.
pub fn tool_error(name: &str, schema: &Value, args: &Value) -> Result<Value> {
    input_error(name, schema, args, None)
}
/// Diagnostic-only original argument ordering for the one exhaustive record
/// schema. A parsed Value cannot reconstruct insertion order of invalid keys.
/// The complete frame is bounded by the existing 8 MiB MCP framing limit; the
/// argument/error limits below apply before this second, owned parse.
pub fn tool_error_from_frame(
    name: &str,
    schema: &Value,
    args: &Value,
    frame: &[u8],
) -> Result<Value> {
    if name != "obs-set-input-audio-tracks" {
        return Err(ProjectionUnavailable::UnknownTool);
    }
    if frame.len() > 8 * 1024 * 1024 {
        return Err(ProjectionUnavailable::InputLimit);
    }
    let mut nodes = 0;
    let mut bytes = 0;
    bound_input(args, 0, &mut nodes, &mut bytes)?;
    let ordered: Ordered =
        serde_json::from_slice(frame).map_err(|_| ProjectionUnavailable::UnsupportedSchema)?;
    let empty = Ordered::Object(vec![]);
    let arguments = ordered
        .get("params")
        .and_then(|params| params.get("arguments"))
        .unwrap_or(&empty);
    if arguments.value() != *args {
        return Err(ProjectionUnavailable::UnsupportedSchema);
    }
    input_error(name, schema, args, Some(arguments))
}
fn input_error(name: &str, schema: &Value, args: &Value, order: Option<&Ordered>) -> Result<Value> {
    if name.len() > 256 {
        return Err(ProjectionUnavailable::UnknownTool);
    }
    let schema_node = catalog()
        .get(name)
        .ok_or(ProjectionUnavailable::UnknownTool)?;
    if schema_node.value() != *schema {
        return Err(ProjectionUnavailable::SchemaMismatch);
    }
    let mut nodes = 0;
    let mut bytes = 0;
    bound_input(args, 0, &mut nodes, &mut bytes)?;
    qualify(schema_node, 0)?;
    // The qualified public MCP call shape owns an arguments object. Non-object
    // RPC-envelope validation belongs to its protocol owner, not this helper.
    if !args.is_object() {
        return Err(ProjectionUnavailable::UnsupportedSchema);
    }
    let mut issues = Issues::default();
    walk(
        schema_node,
        Some(args),
        order,
        &mut Vec::new(),
        &mut issues,
        0,
    )?;
    if issues.values.is_empty() {
        return Err(ProjectionUnavailable::NoIssues);
    }
    let text = format!(
        "Input validation error: Invalid arguments for tool {name}: {}",
        issues.values.join(", ")
    );
    if text.len() > MAX_OUTPUT {
        return Err(ProjectionUnavailable::OutputLimit);
    }
    Ok(json!({"content":[{"type":"text","text":text}],"isError":true}))
}
/// Narrow SDK output-error projection. The input is already a framed OBS
/// response; only the declared duplicate-result field is inspected. Unknown
/// extension data is neither formatted into diagnostics nor traversed. Schema
/// depth and diagnostics retain the existing explicit bounds.
pub fn tool_output_error(name: &str, schema: &Value, output: &Value) -> Result<Value> {
    if name != "obs-duplicate-scene-item" {
        return Err(ProjectionUnavailable::UnknownTool);
    }
    static OUTPUTS: OnceLock<HashMap<String, Ordered>> = OnceLock::new();
    let node = OUTPUTS
        .get_or_init(|| catalog_field("outputSchema"))
        .get(name)
        .ok_or(ProjectionUnavailable::UnknownTool)?;
    if node.value() != *schema {
        return Err(ProjectionUnavailable::SchemaMismatch);
    }
    qualify(node, 0)?;
    let mut issues = Issues::default();
    walk(node, Some(output), None, &mut vec![], &mut issues, 0)?;
    if issues.values.is_empty() {
        return Err(ProjectionUnavailable::NoIssues);
    }
    let text = format!(
        "Output validation error: Invalid structured content for tool {name}: {}",
        issues.values.join(", ")
    );
    if text.len() > MAX_OUTPUT {
        return Err(ProjectionUnavailable::OutputLimit);
    }
    Ok(json!({"content":[{"type":"text","text":text}],"isError":true}))
}
fn bound_input(value: &Value, depth: usize, nodes: &mut usize, bytes: &mut usize) -> Result<()> {
    *nodes += 1;
    if depth > MAX_DEPTH || *nodes > MAX_NODES {
        return Err(ProjectionUnavailable::InputLimit);
    }
    match value {
        Value::String(s) => *bytes = bytes.saturating_add(s.len()),
        Value::Array(a) => {
            for v in a {
                bound_input(v, depth + 1, nodes, bytes)?;
            }
        }
        Value::Object(o) => {
            for (k, v) in o {
                *bytes = bytes.saturating_add(k.len());
                bound_input(v, depth + 1, nodes, bytes)?;
            }
        }
        _ => {}
    }
    if *bytes > MAX_INPUT_BYTES {
        return Err(ProjectionUnavailable::InputLimit);
    }
    Ok(())
}

// serde_json::Value sorts object keys in this crate. Zod parses object fields in
// declaration order, so recover that order from the embedded, source-generated
// JSON rather than sorting or guessing from the required-fields list.
enum Ordered {
    Scalar(Value),
    Array(Vec<Ordered>),
    Object(Vec<(String, Ordered)>),
}
impl Ordered {
    fn get(&self, key: &str) -> Option<&Self> {
        match self {
            Self::Object(o) => o.iter().rev().find_map(|(k, v)| (k == key).then_some(v)),
            _ => None,
        }
    }
    fn array(&self) -> Option<&[Self]> {
        if let Self::Array(a) = self {
            Some(a)
        } else {
            None
        }
    }
    fn object(&self) -> Option<&[(String, Self)]> {
        if let Self::Object(o) = self {
            Some(o)
        } else {
            None
        }
    }
    fn string(&self) -> Option<&str> {
        if let Self::Scalar(Value::String(s)) = self {
            Some(s)
        } else {
            None
        }
    }
    fn value(&self) -> Value {
        match self {
            Self::Scalar(v) => v.clone(),
            Self::Array(a) => Value::Array(a.iter().map(Self::value).collect()),
            Self::Object(o) => {
                Value::Object(o.iter().map(|(k, v)| (k.clone(), v.value())).collect())
            }
        }
    }
    fn number(&self) -> Option<f64> {
        if let Self::Scalar(v) = self {
            v.as_f64()
        } else {
            None
        }
    }
}
impl<'de> Deserialize<'de> for Ordered {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct OrderedVisitor;
        impl<'de> Visitor<'de> for OrderedVisitor {
            type Value = Ordered;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("source-generated JSON")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> std::result::Result<Ordered, E> {
                Ok(Ordered::Scalar(json!(v)))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<Ordered, E> {
                Ok(Ordered::Scalar(json!(v)))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<Ordered, E> {
                Ok(Ordered::Scalar(json!(v)))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<Ordered, E> {
                Ok(Ordered::Scalar(json!(v)))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<Ordered, E> {
                Ok(Ordered::Scalar(json!(v)))
            }
            fn visit_string<E: serde::de::Error>(
                self,
                v: String,
            ) -> std::result::Result<Ordered, E> {
                Ok(Ordered::Scalar(Value::String(v)))
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Ordered, E> {
                Ok(Ordered::Scalar(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Ordered, A::Error> {
                let mut values = vec![];
                while let Some(v) = a.next_element()? {
                    values.push(v);
                }
                Ok(Ordered::Array(values))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Ordered, A::Error> {
                let mut values = vec![];
                while let Some(v) = a.next_entry()? {
                    values.push(v);
                }
                Ok(Ordered::Object(values))
            }
        }
        d.deserialize_any(OrderedVisitor)
    }
}
fn catalog() -> &'static HashMap<String, Ordered> {
    static CATALOG: OnceLock<HashMap<String, Ordered>> = OnceLock::new();
    CATALOG.get_or_init(|| catalog_field("inputSchema"))
}
fn catalog_field(field: &str) -> HashMap<String, Ordered> {
    let root: Ordered = serde_json::from_str(include_str!("../assets/tools.json"))
        .expect("checked source-generated tool catalog");
    let Ordered::Object(mut root) = root else {
        panic!("checked catalog object")
    };
    let tools = root
        .swap_remove(
            root.iter()
                .position(|(k, _)| k == "tools")
                .expect("checked tools list"),
        )
        .1;
    let Ordered::Array(tools) = tools else {
        panic!("checked tools array")
    };
    tools
        .into_iter()
        .filter_map(|tool| {
            let Ordered::Object(mut fields) = tool else {
                return None;
            };
            let name = fields
                .iter()
                .find_map(|(k, v)| (k == "name").then(|| v.string()).flatten())
                .map(str::to_owned)?;
            let index = fields.iter().position(|(k, _)| k == field)?;
            Some((name, fields.swap_remove(index).1))
        })
        .collect()
}
// Only nullable wrappers emitted by the pinned Zod schemas are supported;
// this is not a general arbitrary-union error formatter.
fn nullable_base(schema: &Ordered) -> Option<&Ordered> {
    let variants = schema.get("anyOf")?.array()?;
    if variants.len() != 2 {
        return None;
    }
    let nonnull = variants
        .iter()
        .find(|variant| variant.get("type").and_then(Ordered::string) != Some("null"))?;
    variants
        .iter()
        .any(|variant| variant.get("type").and_then(Ordered::string) == Some("null"))
        .then_some(nonnull)
}
fn schema_type(schema: &Ordered) -> Option<&str> {
    let value = schema.get("type")?;
    value.string().or_else(|| {
        let types = value.array()?;
        if types.len() != 2 || !types.iter().any(|value| value.string() == Some("null")) {
            return None;
        }
        types
            .iter()
            .find_map(|value| value.string().filter(|value| *value != "null"))
    })
}
fn nullable(schema: &Ordered) -> bool {
    schema
        .get("type")
        .and_then(Ordered::array)
        .is_some_and(|types| types.iter().any(|value| value.string() == Some("null")))
}
// The exact Source leaves for z.unknown and z.record(z.string(),z.unknown()).
// These are diagnostic shapes only; closed tool parsers remain admission owners.
fn unknown_json(schema: &Ordered) -> bool {
    schema.object().is_some_and(|fields| {
        fields
            .iter()
            .all(|(key, _)| matches!(key.as_str(), "description" | "title" | "$schema"))
    })
}
fn opaque_record(schema: &Ordered) -> bool {
    schema_type(schema) == Some("object")
        && schema
            .get("propertyNames")
            .is_some_and(|keys| keys.value() == json!({"type":"string"}))
        && schema.get("additionalProperties").is_some_and(unknown_json)
        && schema.get("properties").is_none()
        && schema.get("required").is_none()
}
const TRACK_KEYS: [&str; 6] = ["1", "2", "3", "4", "5", "6"];
fn track_record(schema: &Ordered) -> Option<&Ordered> {
    let keys = schema.get("propertyNames")?;
    if schema_type(schema) != Some("object") || keys.get("type")?.string() != Some("string") {
        return None;
    }
    let enumeration = keys.get("enum")?.array()?;
    let required = schema.get("required")?.array()?;
    if enumeration.len() != 6
        || required.len() != 6
        || !enumeration
            .iter()
            .zip(TRACK_KEYS)
            .all(|(key, expected)| key.string() == Some(expected))
        || !required
            .iter()
            .zip(TRACK_KEYS)
            .all(|(key, expected)| key.string() == Some(expected))
    {
        return None;
    }
    let boolean = schema.get("additionalProperties")?;
    (boolean.value() == json!({"type":"boolean"})).then_some(boolean)
}
fn record_keys(input: &Value, order: Option<&Ordered>) -> Vec<String> {
    let mut keys = vec![];
    if let Some(fields) = order.and_then(Ordered::object) {
        let mut seen = HashSet::new();
        for (key, _) in fields {
            if seen.insert(key.as_str()) {
                keys.push(key.clone());
            }
        }
    } else if let Some(fields) = input.as_object() {
        keys.extend(fields.keys().cloned());
    }
    // JavaScript enumerates canonical array indices first, regardless of their
    // placement in the original JSON, then other strings in insertion order.
    let index = |key: &str| {
        key.parse::<u32>()
            .ok()
            .filter(|index| *index != u32::MAX && index.to_string() == key)
    };
    keys.sort_by(|left, right| match (index(left), index(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    keys
}
fn qualify(schema: &Ordered, depth: usize) -> Result<()> {
    if depth > MAX_DEPTH {
        return Err(ProjectionUnavailable::UnsupportedSchema);
    }
    let object = schema
        .object()
        .ok_or(ProjectionUnavailable::UnsupportedSchema)?;
    let supported = [
        "$schema",
        "type",
        "properties",
        "required",
        "default",
        "description",
        "title",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "enum",
        "const",
        "items",
        "minItems",
        "maxItems",
        "format",
        "pattern",
        "anyOf",
        "additionalProperties",
        "propertyNames",
    ];
    if object.iter().any(|(k, _)| !supported.contains(&k.as_str())) {
        return Err(ProjectionUnavailable::UnsupportedSchema);
    }
    if unknown_json(schema) {
        return Ok(());
    }
    if schema.get("propertyNames").is_some() {
        if track_record(schema).is_some() || opaque_record(schema) {
            return Ok(());
        }
        return Err(ProjectionUnavailable::UnsupportedSchema);
    }
    if schema.get("anyOf").is_some() {
        let base = nullable_base(schema).ok_or(ProjectionUnavailable::UnsupportedSchema)?;
        return qualify(base, depth + 1);
    }
    if schema
        .get("additionalProperties")
        .is_some_and(|value| value.object().is_none_or(|fields| !fields.is_empty()))
    {
        return Err(ProjectionUnavailable::UnsupportedSchema);
    }
    let ty = schema_type(schema).ok_or(ProjectionUnavailable::UnsupportedSchema)?;
    if ![
        "object", "array", "string", "number", "integer", "boolean", "null",
    ]
    .contains(&ty)
    {
        return Err(ProjectionUnavailable::UnsupportedSchema);
    }
    if (schema.get("format").is_some() || schema.get("pattern").is_some())
        && (schema.get("format").and_then(Ordered::string) != Some("uuid")
            || schema.get("pattern").and_then(Ordered::string) != Some(UUID_PATTERN))
    {
        return Err(ProjectionUnavailable::UnsupportedSchema);
    }
    if let Some(properties) = schema.get("properties") {
        for (name, property) in properties
            .object()
            .ok_or(ProjectionUnavailable::UnsupportedSchema)?
        {
            if name.len() > 256 {
                return Err(ProjectionUnavailable::UnsupportedSchema);
            }
            qualify(property, depth + 1)?;
        }
    }
    if let Some(item) = schema.get("items") {
        qualify(item, depth + 1)?;
    }
    for key in ["enum", "const"] {
        if let Some(value) = schema.get(key) {
            let values = if key == "enum" {
                value
                    .array()
                    .ok_or(ProjectionUnavailable::UnsupportedSchema)?
            } else {
                std::slice::from_ref(value)
            };
            if values.is_empty()
                || values.len() > 64
                || values.iter().any(|v| {
                    !matches!(v, Ordered::Scalar(_)) || v.string().is_some_and(|v| v.len() > 512)
                })
            {
                return Err(ProjectionUnavailable::UnsupportedSchema);
            }
        }
    }
    Ok(())
}
#[derive(Default)]
struct Issues {
    values: Vec<String>,
    bytes: usize,
}
impl Issues {
    fn push(&mut self, path: &[String], message: String) -> Result<()> {
        if self.values.len() >= MAX_ISSUES {
            return Err(ProjectionUnavailable::OutputLimit);
        }
        let issue = if path.is_empty() {
            message
        } else {
            format!("{}: {message}", path.join("."))
        };
        self.bytes = self.bytes.saturating_add(issue.len() + 2);
        if self.bytes > MAX_OUTPUT {
            return Err(ProjectionUnavailable::OutputLimit);
        }
        self.values.push(issue);
        Ok(())
    }
}
fn walk(
    schema: &Ordered,
    input: Option<&Value>,
    order: Option<&Ordered>,
    path: &mut Vec<String>,
    issues: &mut Issues,
    depth: usize,
) -> Result<()> {
    if depth > MAX_DEPTH {
        return Err(ProjectionUnavailable::InputLimit);
    }
    if unknown_json(schema) {
        if input.is_none() {
            return issues.push(
                path,
                "Invalid input: expected nonoptional, received undefined".into(),
            );
        }
        return Ok(());
    }
    if let Some(base) = nullable_base(schema) {
        if input.is_some_and(Value::is_null) {
            return Ok(());
        }
        return walk(base, input, order, path, issues, depth + 1);
    }
    if nullable(schema) && input.is_some_and(Value::is_null) {
        return Ok(());
    }
    if input.is_none() && schema.get("default").is_some() {
        return Ok(());
    }
    for keyword in ["enum", "const"] {
        if let Some(values) = schema.get(keyword) {
            let values = if keyword == "enum" {
                values.array().unwrap()
            } else {
                std::slice::from_ref(values)
            };
            if !values
                .iter()
                .any(|v| input.is_some_and(|i| equivalent(i, &v.value())))
            {
                let message = if values.len() == 1 {
                    format!("Invalid input: expected {}", primitive(&values[0].value()))
                } else {
                    format!(
                        "Invalid option: expected one of {}",
                        values
                            .iter()
                            .map(|v| primitive(&v.value()))
                            .collect::<Vec<_>>()
                            .join("|")
                    )
                };
                return issues.push(path, message);
            }
            return Ok(());
        }
    }
    let ty = schema_type(schema).ok_or(ProjectionUnavailable::UnsupportedSchema)?;
    let valid = match ty {
        "object" => input.is_some_and(Value::is_object),
        "array" => input.is_some_and(Value::is_array),
        "string" => input.is_some_and(Value::is_string),
        "number" | "integer" => input.and_then(Value::as_f64).is_some_and(f64::is_finite),
        "boolean" => input.is_some_and(Value::is_boolean),
        "null" => input.is_some_and(Value::is_null),
        _ => false,
    };
    if !valid {
        return issues.push(
            path,
            format!(
                "Invalid input: expected {}, received {}",
                if track_record(schema).is_some() || opaque_record(schema) {
                    "record"
                } else if ty == "integer" {
                    "number"
                } else {
                    ty
                },
                received(input)
            ),
        );
    }
    let input = input.unwrap();
    match ty {
        "object" => {
            if opaque_record(schema) {
                return Ok(());
            }
            if let Some(boolean) = track_record(schema) {
                for key in TRACK_KEYS {
                    path.push(key.into());
                    walk(
                        boolean,
                        input.get(key),
                        order.and_then(|node| node.get(key)),
                        path,
                        issues,
                        depth + 1,
                    )?;
                    path.pop();
                }
                let unknown = record_keys(input, order)
                    .into_iter()
                    .filter(|key| !TRACK_KEYS.contains(&key.as_str()))
                    .collect::<Vec<_>>();
                if !unknown.is_empty() {
                    issues.push(
                        path,
                        format!(
                            "Unrecognized key{}: {}",
                            if unknown.len() == 1 { "" } else { "s" },
                            unknown
                                .iter()
                                .map(|key| format!("\"{key}\""))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    )?;
                }
                return Ok(());
            }
            if let Some(properties) = schema.get("properties").and_then(Ordered::object) {
                for (name, property) in properties {
                    let value = input.get(name);
                    let required = schema
                        .get("required")
                        .and_then(Ordered::array)
                        .is_some_and(|a| a.iter().any(|v| v.string() == Some(name)));
                    if value.is_none() && !required {
                        continue;
                    }
                    path.push(name.clone());
                    walk(
                        property,
                        value,
                        order.and_then(|node| node.get(name)),
                        path,
                        issues,
                        depth + 1,
                    )?;
                    path.pop();
                }
            }
        }
        "array" => {
            let values = input.as_array().unwrap();
            // Pinned Zod 4.6 capSize runs a leading maximum before parsing
            // any element. Do not manufacture child issues for an oversized
            // container which its source parser never admitted.
            if schema
                .get("maxItems")
                .and_then(Ordered::number)
                .is_some_and(|max| values.len() as f64 > max)
            {
                return limits(schema, values.len() as f64, "array", true, path, issues);
            }
            if let Some(item) = schema.get("items") {
                for (i, v) in values.iter().enumerate() {
                    path.push(i.to_string());
                    walk(
                        item,
                        Some(v),
                        order
                            .and_then(Ordered::array)
                            .and_then(|values| values.get(i)),
                        path,
                        issues,
                        depth + 1,
                    )?;
                    path.pop();
                }
            }
            limits(schema, values.len() as f64, "array", true, path, issues)?;
        }
        "string" => {
            let text = input.as_str().unwrap();
            limits(
                schema,
                text.chars().count() as f64,
                "string",
                true,
                path,
                issues,
            )?;
            if schema.get("format").is_some() && !uuid(text) {
                issues.push(path, "Invalid UUID".into())?;
            }
        }
        "number" | "integer" => {
            let number = input.as_f64().unwrap();
            if ty == "integer" {
                if number.fract() != 0.0 {
                    return issues
                        .push(path, "Invalid input: expected int, received number".into());
                }
                if number > SAFE {
                    issues.push(
                        path,
                        format!("Too big: expected int to be <={}", js_number(SAFE)),
                    )?;
                } else if number < -SAFE {
                    issues.push(
                        path,
                        format!("Too small: expected int to be >={}", js_number(-SAFE)),
                    )?;
                }
            }
            limits(schema, number, "number", false, path, issues)?;
        }
        _ => {}
    }
    Ok(())
}
fn limits(
    schema: &Ordered,
    n: f64,
    origin: &str,
    sized: bool,
    path: &[String],
    issues: &mut Issues,
) -> Result<()> {
    for (key, value) in schema.object().unwrap() {
        let kind = match key.as_str() {
            "minimum" | "exclusiveMinimum" if !sized => Some((false, key == "minimum")),
            "maximum" | "exclusiveMaximum" if !sized => Some((true, key == "maximum")),
            "minLength" if origin == "string" => Some((false, true)),
            "maxLength" if origin == "string" => Some((true, true)),
            "minItems" if origin == "array" => Some((false, true)),
            "maxItems" if origin == "array" => Some((true, true)),
            _ => None,
        };
        if let Some((big, inclusive)) = kind {
            let bound = value
                .number()
                .ok_or(ProjectionUnavailable::UnsupportedSchema)?;
            if origin == "number"
                && schema.get("type").and_then(Ordered::string) == Some("integer")
                && bound.abs() == SAFE
            {
                continue;
            }
            let failed = if big {
                if inclusive { n > bound } else { n >= bound }
            } else if inclusive {
                n < bound
            } else {
                n <= bound
            };
            if failed {
                issues.push(
                    path,
                    format!(
                        "Too {}: expected {origin} to {} {}{}{}",
                        if big { "big" } else { "small" },
                        if sized { "have" } else { "be" },
                        if big { "<" } else { ">" },
                        if inclusive { "=" } else { "" },
                        format_args!(
                            "{}{}",
                            js_number(bound),
                            if origin == "string" {
                                " characters"
                            } else if origin == "array" {
                                " items"
                            } else {
                                ""
                            }
                        )
                    ),
                )?;
            }
        }
    }
    Ok(())
}
fn received(input: Option<&Value>) -> &'static str {
    match input {
        None => "undefined",
        Some(Value::Null) => "null",
        Some(Value::Bool(_)) => "boolean",
        Some(Value::Number(_)) => "number",
        Some(Value::String(_)) => "string",
        Some(Value::Array(_)) => "array",
        Some(Value::Object(_)) => "object",
    }
}
fn primitive(v: &Value) -> String {
    if let Value::String(s) = v {
        format!("\"{s}\"")
    } else if let Some(n) = v.as_f64() {
        js_number(n)
    } else {
        v.to_string()
    }
}
fn js_number(n: f64) -> String {
    crate::observation::js_string(Some(&json!(n)))
}
fn equivalent(a: &Value, b: &Value) -> bool {
    if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
        a == b
    } else {
        a == b
    }
}
const UUID_PATTERN: &str = "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$";
fn uuid(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                *b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
        && (s == "00000000-0000-0000-0000-000000000000"
            || s.eq_ignore_ascii_case("ffffffff-ffff-ffff-ffff-ffffffffffff")
            || matches!(b[14], b'1'..=b'8')
                && matches!(b[19].to_ascii_lowercase(), b'8' | b'9' | b'a' | b'b'))
}
