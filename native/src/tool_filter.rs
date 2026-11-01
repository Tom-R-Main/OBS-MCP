// SPDX-License-Identifier: GPL-2.0-only
//! Static tool selection from the generated SDK registration. Dynamic selection
//! remains SDK-owned until its state changes and notifications move together.
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

pub(crate) struct ToolFilter {
    visible: HashSet<String>,
    tools: Vec<Value>,
}

pub(crate) use obs_mcp_native::config::enabled;
use obs_mcp_native::config::trim;

fn split_list(value: Option<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    value
        .unwrap_or_default()
        .split(',')
        .map(trim)
        .filter(|entry| !entry.is_empty() && seen.insert((*entry).to_owned()))
        .map(str::to_owned)
        .collect()
}

fn strings(value: &Value) -> Result<Vec<&str>, String> {
    value
        .as_array()
        .and_then(|values| values.iter().map(Value::as_str).collect())
        .ok_or_else(|| "Generated toolset metadata is invalid".into())
}

impl ToolFilter {
    pub(crate) fn parse(
        get: impl Fn(&str) -> Option<String>,
        catalog: &Value,
    ) -> Result<Self, String> {
        let metadata = &catalog["toolsets"];
        let known_groups = strings(&metadata["groups"])?;
        let core_groups = strings(&metadata["coreGroups"])?;
        let tool_groups = metadata["toolGroups"]
            .as_object()
            .ok_or("Generated toolset metadata is missing")?;
        let tools = catalog["tools"]
            .as_array()
            .ok_or("Generated tool catalog is missing")?;
        let mut known_tools = HashMap::new();
        for tool in tools {
            let name = tool["name"]
                .as_str()
                .ok_or("Generated tool name is invalid")?;
            let group = tool_groups
                .get(name)
                .and_then(Value::as_str)
                .filter(|group| known_groups.contains(group))
                .ok_or("Generated tool group is missing or invalid")?;
            if known_tools.insert(name, group).is_some() {
                return Err("Generated tool names are duplicated".into());
            }
        }
        if tool_groups.len() != tools.len()
            || known_groups.iter().collect::<HashSet<_>>().len() != known_groups.len()
            || core_groups
                .iter()
                .any(|group| !known_groups.contains(group))
        {
            return Err("Generated toolset metadata does not match the catalog".into());
        }

        let group_names = split_list(get("OBS_MCP_TOOLSETS"));
        let explicit = split_list(get("OBS_MCP_TOOLS"));
        let read_only = get("OBS_MCP_READ_ONLY").is_some_and(|value| enabled(&value));
        // Match the established precedence exactly: `all` short-circuits group
        // validation, but never explicit tool validation or read-only filtering.
        let all = (group_names.is_empty() && explicit.is_empty())
            || group_names.iter().any(|name| name == "all");
        let mut selected_groups = HashSet::new();
        if !all {
            for name in &group_names {
                if name == "core" {
                    selected_groups.extend(core_groups.iter().copied());
                } else if known_groups.contains(&name.as_str()) {
                    selected_groups.insert(name.as_str());
                } else {
                    return Err(format!(
                        "Unknown OBS_MCP_TOOLSETS entry \"{name}\". Use all, core, or: {}",
                        known_groups.join(", ")
                    ));
                }
            }
        }
        let unknown = explicit
            .iter()
            .filter(|name| !known_tools.contains_key(name.as_str()))
            .map(String::as_str)
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(format!(
                "Unknown OBS_MCP_TOOLS entries: {}",
                unknown.join(", ")
            ));
        }
        let selected_names: HashSet<&str> = explicit.iter().map(String::as_str).collect();
        let mut visible = HashSet::new();
        let tools: Vec<Value> = tools
            .iter()
            .filter(|tool| {
                let name = tool["name"]
                    .as_str()
                    .expect("validated generated tool name");
                let allowed = (!read_only || tool["annotations"]["readOnlyHint"] == true)
                    && (all
                        || selected_groups.contains(known_tools[name])
                        || selected_names.contains(name));
                if allowed {
                    visible.insert(name.to_owned());
                }
                allowed
            })
            .cloned()
            .collect();
        if tools.is_empty() {
            return Err("The tool filter excludes every tool".into());
        }
        Ok(Self { visible, tools })
    }

    pub(crate) fn includes(&self, name: &str) -> bool {
        self.visible.contains(name)
    }

    pub(crate) fn manifest(&self) -> Value {
        json!({"tools":self.tools})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> Value {
        json!({"tools":[
            {"name":"read","annotations":{"readOnlyHint":true}},
            {"name":"write","annotations":{"readOnlyHint":false}},
            {"name":"other","annotations":{"readOnlyHint":true}}
        ],"toolsets":{"groups":["general","record"],"coreGroups":["general"],"toolGroups":{"read":"general","write":"general","other":"record"}}})
    }

    fn filter(env: &[(&str, &str)]) -> Result<ToolFilter, String> {
        ToolFilter::parse(
            |name| {
                env.iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| (*value).into())
            },
            &catalog(),
        )
    }

    #[test]
    fn preserves_registry_order_and_filter_precedence() {
        let selected = filter(&[
            ("OBS_MCP_TOOLSETS", "core"),
            ("OBS_MCP_TOOLS", "other,other"),
            ("OBS_MCP_READ_ONLY", "YES"),
        ])
        .unwrap();
        assert_eq!(
            selected.manifest()["tools"],
            json!([
                {"name":"read","annotations":{"readOnlyHint":true}},
                {"name":"other","annotations":{"readOnlyHint":true}}
            ])
        );
        assert!(!selected.includes("write"));
        let all = filter(&[("OBS_MCP_TOOLSETS", "unknown,all")]).unwrap();
        assert_eq!(all.manifest()["tools"].as_array().unwrap().len(), 3);
        assert!(filter(&[("OBS_MCP_TOOLSETS", "all"), ("OBS_MCP_TOOLS", "unknown")]).is_err());
    }

    #[test]
    fn refuses_unknown_names_and_empty_inventory() {
        assert!(
            matches!(filter(&[("OBS_MCP_TOOLSETS", "typo")]), Err(message) if message.contains("Unknown OBS_MCP_TOOLSETS entry"))
        );
        assert!(
            matches!(filter(&[("OBS_MCP_TOOLS", "typo,other,typo,next")]), Err(message) if message == "Unknown OBS_MCP_TOOLS entries: typo, next")
        );
        assert!(
            matches!(filter(&[("OBS_MCP_TOOLS", "write"), ("OBS_MCP_READ_ONLY", "true")]), Err(message) if message == "The tool filter excludes every tool")
        );
    }

    #[test]
    fn environment_whitespace_matches_javascript() {
        assert!(enabled("\u{feff} YES\u{feff}"));
        assert!(!enabled("\u{0085}yes\u{0085}"));
        assert!(filter(&[("OBS_MCP_TOOLSETS", "\u{feff}core\u{feff}")]).is_ok());
        assert!(filter(&[("OBS_MCP_TOOLSETS", "\u{0085}core\u{0085}")]).is_err());
    }

    #[test]
    fn missing_or_inconsistent_generated_metadata_cannot_enable_tools() {
        let mut input = catalog();
        input["toolsets"]["toolGroups"]
            .as_object_mut()
            .unwrap()
            .remove("read");
        assert!(ToolFilter::parse(|_| None, &input).is_err());
        input = catalog();
        input["toolsets"]["coreGroups"] = json!(["unknown"]);
        assert!(ToolFilter::parse(|_| None, &input).is_err());
        input = catalog();
        input.as_object_mut().unwrap().remove("toolsets");
        assert!(ToolFilter::parse(|_| None, &input).is_err());
    }
}
