// SPDX-License-Identifier: GPL-2.0-only
use serde_json::{Map, Value};
use std::sync::OnceLock;

fn asset() -> &'static Value {
    static ASSET: OnceLock<Value> = OnceLock::new();
    ASSET.get_or_init(|| {
        serde_json::from_str(include_str!("../assets/tools.json"))
            .expect("checked generated contract")
    })
}
pub(crate) fn catalog() -> &'static Value {
    asset()
}

pub(crate) fn workflow_declared(name: &str, handler: &str) -> bool {
    workflow_declaration(asset(), name, handler)
}

fn workflow_declaration(catalog: &Value, name: &str, handler: &str) -> bool {
    let Some(definition) = catalog["workflows"].get(name) else {
        return false;
    };
    let Some(tool) = catalog["tools"]
        .as_array()
        .and_then(|tools| tools.iter().find(|tool| tool["name"] == name))
    else {
        return false;
    };
    definition["handler"] == handler
        && tool["annotations"]["readOnlyHint"] == true
        && definition.get("inputSchema").is_some()
        && definition.get("inputSchema") == tool.get("inputSchema")
}

pub(crate) fn validated_local_arguments(
    name: &str,
    handler: &str,
    arguments: &Value,
) -> Option<Value> {
    local_arguments(asset(), name, handler, arguments)
}

fn local_arguments(catalog: &Value, name: &str, handler: &str, arguments: &Value) -> Option<Value> {
    let definition = catalog["locals"].get(name)?;
    let tool = catalog["tools"]
        .as_array()?
        .iter()
        .find(|tool| tool["name"] == name)?;
    if definition["handler"] != handler
        || tool["annotations"]["readOnlyHint"] != true
        || definition.get("inputSchema")? != tool.get("inputSchema")?
    {
        return None;
    }
    validate(tool.get("inputSchema")?, arguments, true)
}

pub(crate) fn service_handler(name: &str) -> Option<&'static str> {
    let service = &asset()["service"];
    if service["activation"] != "OBS_MCP_NATIVE_STATEFUL" {
        return None;
    }
    let definition = service["handlers"].get(name)?;
    let tool = asset()["tools"]
        .as_array()?
        .iter()
        .find(|tool| tool["name"] == name)?;
    if definition.get("inputSchema")? != tool.get("inputSchema")? {
        return None;
    }
    definition["handler"].as_str()
}
pub(crate) fn service_arguments(name: &str, args: &Value) -> Option<Value> {
    service_handler(name)?;
    if name == "obs-snapshot" {
        obs_mcp_native::snapshot_tools::Request::parse(args).ok()?;
        return Some(args.clone());
    }
    if name == "obs-get-input-property-list-items" {
        obs_mcp_native::property_list::Request::parse(args).ok()?;
        return Some(args.clone());
    }
    if matches!(
        service_handler(name),
        Some("literal-setter" | "confirmed-output-start")
    ) {
        // A refused closed parser must not fall through to generic normalization:
        // doing so could erase an opaque record or bypass its resource limits.
        return obs_mcp_native::generic_requests::Call::parse(name, args)
            .map(|call| call.into_arguments());
    }
    if matches!(
        name,
        "obs-get-status"
            | "obs-test-connection"
            | "obs-sleep"
            | "obs-get-input-list"
            | "obs-get-input-kind-list"
            | "obs-get-source-screenshot"
            | "obs-preflight"
    ) {
        obs_mcp_native::service_read_tools::ReadToolRequest::parse(name, args).ok()?;
        return Some(args.clone());
    }
    if name == "obs-plan-scene-template" {
        obs_mcp_native::scene_plan::PlanRequest::from_value(args).ok()?;
        return Some(args.clone());
    }
    if name == "obs-audit-scene" {
        obs_mcp_native::scene_audit::AuditOptions::from_value(args).ok()?;
        return Some(args.clone());
    }
    let mut schema = asset()["service"]["handlers"][name]["inputSchema"].clone();
    // The one schema extension in this explicit allowlist is Zod's UUID. Do
    // not broaden the default transitional validator or ignore arbitrary regex.
    if name == "obs-take-scene" || name == "obs-recover-demo" {
        let field = if name == "obs-take-scene" {
            "stageId"
        } else {
            "operationId"
        };
        let text = args.get(field)?.as_str()?;
        let bytes = text.as_bytes();
        let uuid = bytes.len() == 36
            && bytes.iter().enumerate().all(|(index, byte)| {
                if [8, 13, 18, 23].contains(&index) {
                    *byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            })
            && (text == "00000000-0000-0000-0000-000000000000"
                || text.eq_ignore_ascii_case("ffffffff-ffff-ffff-ffff-ffffffffffff")
                || (matches!(bytes[14], b'1'..=b'8')
                    && matches!(bytes[19].to_ascii_lowercase(), b'8' | b'9' | b'a' | b'b')));
        if !uuid {
            return None;
        }
        let property = schema
            .get_mut("properties")?
            .get_mut(field)?
            .as_object_mut()?;
        if property.get("format")?.as_str() != Some("uuid") || !property.get("pattern")?.is_string()
        {
            return None;
        }
        property.remove("format");
        property.remove("pattern");
    }
    validate(&schema, args, true)
}
pub fn native_request(name: &str, args: &Value) -> Result<Option<(String, Value)>, String> {
    let Some(definition) = asset()["requests"].get(name) else {
        return Ok(None);
    };
    let args = validate(&definition["inputSchema"], args, true)
        .ok_or_else(|| "SDK argument validation required".to_string())?;
    Ok(Some((
        definition["requestType"]
            .as_str()
            .ok_or("invalid generated request")?
            .to_owned(),
        args,
    )))
}
pub fn valid_response(name: &str, response: &Value) -> bool {
    let Some(tool) = asset()["tools"]
        .as_array()
        .and_then(|tools| tools.iter().find(|t| t["name"] == name))
    else {
        return false;
    };
    tool.get("outputSchema")
        .is_none_or(|s| validate(s, response, false).is_some())
}

// Fail closed to the SDK for unsupported schema features. No weaker native validator.
fn validate(schema: &Value, value: &Value, normalize: bool) -> Option<Value> {
    if let Some(allowed) = schema.as_bool() {
        return allowed.then(|| value.clone());
    }
    let s = schema.as_object()?;
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
        "minLength",
        "maxLength",
        "enum",
        "additionalProperties",
        "items",
        "anyOf",
        "oneOf",
        "allOf",
        "const",
        "minItems",
        "maxItems",
        "propertyNames",
    ];
    if s.keys().any(|k| !supported.contains(&k.as_str())) {
        return None;
    }
    for combinator in ["anyOf", "oneOf", "allOf"] {
        if let Some(variants) = s.get(combinator) {
            let variants = variants.as_array()?;
            let count = variants
                .iter()
                .filter(|v| validate(v, value, normalize).is_some())
                .count();
            if (combinator == "allOf" && count != variants.len())
                || (combinator == "oneOf" && count != 1)
                || (combinator == "anyOf" && count == 0)
            {
                return None;
            }
        }
    }
    if s.get("const").is_some_and(|expected| expected != value) {
        return None;
    }
    if let Some(values) = s.get("enum")
        && !values.as_array()?.contains(value)
    {
        return None;
    }
    let matches_type = |ty: &str| match ty {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.as_f64().is_some_and(f64::is_finite),
        "integer" => value
            .as_f64()
            .is_some_and(|x| x.is_finite() && x.fract() == 0.0),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    };
    if let Some(ty) = s.get("type") {
        if let Some(ty) = ty.as_str() {
            if !matches_type(ty) {
                return None;
            }
        } else if !ty
            .as_array()?
            .iter()
            .any(|t| t.as_str().is_some_and(matches_type))
        {
            return None;
        }
    }
    if let Some(n) = value.as_f64()
        && (s
            .get("minimum")
            .and_then(Value::as_f64)
            .is_some_and(|m| n < m)
            || s.get("maximum")
                .and_then(Value::as_f64)
                .is_some_and(|m| n > m))
    {
        return None;
    }
    if let Some(text) = value.as_str() {
        let count = text.chars().count() as u64;
        if s.get("minLength")
            .and_then(Value::as_u64)
            .is_some_and(|m| count < m)
            || s.get("maxLength")
                .and_then(Value::as_u64)
                .is_some_and(|m| count > m)
        {
            return None;
        }
    }
    if let Some(items) = value.as_array() {
        if s.get("minItems")
            .and_then(Value::as_u64)
            .is_some_and(|m| (items.len() as u64) < m)
            || s.get("maxItems")
                .and_then(Value::as_u64)
                .is_some_and(|m| (items.len() as u64) > m)
        {
            return None;
        }
        if let Some(item_schema) = s.get("items") {
            for item in items {
                validate(item_schema, item, false)?;
            }
        }
    }
    if let Some(values) = value.as_object() {
        let properties = s.get("properties").and_then(Value::as_object);
        let mut output = if normalize {
            Map::new()
        } else {
            values.clone()
        };
        if let Some(properties) = properties {
            for (name, property) in properties {
                if let Some(v) = values.get(name).or_else(|| {
                    if normalize {
                        property.get("default")
                    } else {
                        None
                    }
                }) {
                    output.insert(name.clone(), validate(property, v, normalize)?);
                }
            }
        }
        if let Some(required) = s.get("required") {
            for name in required.as_array()? {
                if !output.contains_key(name.as_str()?) {
                    return None;
                }
            }
        }
        for (name, v) in values {
            if let Some(names) = s.get("propertyNames") {
                validate(names, &Value::String(name.clone()), false)?;
            }
            if properties.is_some_and(|p| p.contains_key(name)) {
                continue;
            }
            if let Some(extra) = s.get("additionalProperties") {
                if extra == &Value::Bool(false) && !normalize {
                    return None;
                }
                if extra.is_object() {
                    validate(extra, v, false)?;
                }
            }
        }
        return Some(Value::Object(output));
    }
    Some(value.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn local_dispatch_requires_the_reviewed_handler_and_unchanged_tool_schema() {
        let args = json!({"requestType":"GetVersion", "ignored":true});
        assert_eq!(
            validated_local_arguments("obs-describe-request", "describe-request", &args),
            Some(json!({"requestType":"GetVersion"}))
        );
        let mut catalog = asset().clone();
        catalog["locals"]["obs-describe-request"]["handler"] = json!("unimplemented");
        assert!(
            local_arguments(&catalog, "obs-describe-request", "describe-request", &args).is_none()
        );
        let mut catalog = asset().clone();
        catalog["locals"]["obs-describe-request"]["inputSchema"] = json!({"type":"object"});
        assert!(
            local_arguments(&catalog, "obs-describe-request", "describe-request", &args).is_none()
        );
        assert!(validated_local_arguments("obs-get-version", "describe-request", &args).is_none());
        assert!(
            validated_local_arguments(
                "obs-describe-request",
                "describe-request",
                &json!({"category":42})
            )
            .is_none()
        );
    }
    #[test]
    fn generated_inventory_has_no_native_effect_routes() {
        assert_eq!(asset()["tools"].as_array().unwrap().len(), 178);
        assert!(!asset()["requests"].as_object().unwrap().is_empty());
        for (name, route) in asset()["requests"].as_object().unwrap() {
            let tool = asset()["tools"]
                .as_array()
                .unwrap()
                .iter()
                .find(|t| t["name"] == *name)
                .unwrap();
            assert_eq!(tool["annotations"]["readOnlyHint"], true);
            assert!(route["requestType"].as_str().unwrap().starts_with("Get"));
            assert_ne!(route["requestType"], "GetInputPropertiesListPropertyItems");
        }
    }
    #[test]
    fn workflow_dispatch_requires_reviewed_read_only_handler_and_exact_schema() {
        let name = "obs-plan-scene-template";
        let handler = "scene-template-plan";
        assert!(workflow_declaration(asset(), name, handler));
        let mut changed = asset().clone();
        changed["workflows"][name]["handler"] = json!("another-handler");
        assert!(!workflow_declaration(&changed, name, handler));
        changed = asset().clone();
        changed["workflows"][name]["inputSchema"] = json!({"type":"object"});
        assert!(!workflow_declaration(&changed, name, handler));
        changed = asset().clone();
        changed["tools"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|tool| tool["name"] == name)
            .unwrap()["annotations"]["readOnlyHint"] = json!(false);
        assert!(!workflow_declaration(&changed, name, handler));
        changed = asset().clone();
        changed.as_object_mut().unwrap().remove("workflows");
        assert!(!workflow_declaration(&changed, name, handler));
    }
    #[test]
    fn argument_defaults_and_unknown_field_stripping_match_sdk() {
        let schema = json!({"type":"object","properties":{"count":{"type":"integer","minimum":0,"maximum":5,"default":2}},"required":["count"]});
        assert_eq!(
            validate(&schema, &json!({"extra":9}), true),
            Some(json!({"count":2}))
        );
        assert!(validate(&schema, &json!({"count":2.2}), true).is_none());
        assert!(validate(&schema, &json!({"count":6}), true).is_none());
    }
    #[test]
    fn unsupported_constraints_and_incomplete_outputs_require_compatibility() {
        assert!(
            validate(
                &json!({"type":"string","pattern":"^x$"}),
                &json!("x"),
                false
            )
            .is_none()
        );
        assert!(!valid_response(
            "obs-get-version",
            &json!({"obsVersion":42})
        ));
        assert!(
            native_request("obs-capture-window", &json!({}))
                .unwrap()
                .is_none()
        );
        assert!(native_request("obs-get-input-settings", &json!({"inputName":42})).is_err());
    }
    #[test]
    fn string_bounds_count_codepoints_and_output_records_retain_fields() {
        assert!(validate(&json!({"type":"string","maxLength":1}), &json!("💡"), false).is_some());
        assert!(validate(&json!({"type":"string","minLength":2}), &json!("💡"), false).is_none());
        assert_eq!(
            validate(
                &json!({"type":"object","additionalProperties":true}),
                &json!({"new":1}),
                false
            ),
            Some(json!({"new":1}))
        );
    }
}
