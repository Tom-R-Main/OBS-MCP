// SPDX-License-Identifier: GPL-2.0-only
//! Session-local metadata only. Visibility never grants OBS, journal, policy or
//! worker authority. Effects are owned descriptions for a later frontend owner;
//! this module cannot publish notifications, dispatch work, or activate a mode.
use crate::config::{enabled, trim};
use serde_json::{Value, json};
use std::{collections::HashSet, fmt};

const CONTROLLERS: [&str; 3] = [
    "obs-list-toolsets",
    "obs-enable-toolset",
    "obs-disable-toolset",
];
const STREAM: [&str; 3] = ["obs-start-stream", "obs-stop-stream", "obs-toggle-stream"];
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Contract,
    Configuration(String),
    UnavailableController,
    InvalidArguments,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract => f.write_str("Dynamic visibility contract is unavailable"),
            Self::Configuration(message) => f.write_str(message),
            Self::UnavailableController => {
                f.write_str("Dynamic visibility controller is unavailable")
            }
            Self::InvalidArguments => {
                f.write_str("Dynamic visibility arguments require frontend validation")
            }
        }
    }
}
impl std::error::Error for Error {}
#[derive(Clone, Debug)]
struct Tool {
    name: String,
    group: usize,
    read_only: bool,
}
#[derive(Clone, Debug)]
struct Group {
    name: String,
    covers: String,
}
#[derive(Clone, Debug)]
pub struct Contract {
    tools: Vec<Tool>,
    groups: Vec<Group>,
    core: HashSet<usize>,
    controllers: Value,
    registry_order: Vec<usize>,
}
fn strings(value: &Value) -> Result<Vec<String>, Error> {
    value
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .ok_or(Error::Contract)
}
impl Contract {
    /// Static catalog plus separately captured REAL Source controller contract.
    /// No second handwritten group-description or controller-schema inventory.
    pub fn from_source(catalog: &Value, controller: &Value) -> Result<Self, Error> {
        let names = strings(&catalog["toolsets"]["groups"])?;
        if names.is_empty()
            || names.iter().any(String::is_empty)
            || names.iter().collect::<HashSet<_>>().len() != names.len()
        {
            return Err(Error::Contract);
        }
        let descriptions = controller["groups"].as_array().ok_or(Error::Contract)?;
        if descriptions.len() != names.len() {
            return Err(Error::Contract);
        }
        let groups = names
            .iter()
            .zip(descriptions)
            .map(|(name, entry)| {
                if entry["group"] != *name {
                    return Err(Error::Contract);
                }
                Ok(Group {
                    name: name.clone(),
                    covers: entry["covers"].as_str().ok_or(Error::Contract)?.to_owned(),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut core = HashSet::new();
        for name in strings(&catalog["toolsets"]["coreGroups"])? {
            let index = names
                .iter()
                .position(|n| n == &name)
                .ok_or(Error::Contract)?;
            if !core.insert(index) {
                return Err(Error::Contract);
            }
        }
        let assignments = catalog["toolsets"]["toolGroups"]
            .as_object()
            .ok_or(Error::Contract)?;
        let mut known = HashSet::new();
        let tools = catalog["tools"]
            .as_array()
            .ok_or(Error::Contract)?
            .iter()
            .map(|value| {
                let name = value["name"]
                    .as_str()
                    .filter(|n| !n.is_empty())
                    .ok_or(Error::Contract)?;
                if CONTROLLERS.contains(&name) || !known.insert(name) {
                    return Err(Error::Contract);
                }
                let group = assignments
                    .get(name)
                    .and_then(Value::as_str)
                    .and_then(|group| names.iter().position(|n| n == group))
                    .ok_or(Error::Contract)?;
                Ok(Tool {
                    name: name.to_owned(),
                    group,
                    read_only: value["annotations"]["readOnlyHint"] == true,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        if assignments.len() != tools.len() {
            return Err(Error::Contract);
        }
        let registry_names = strings(&controller["registryOrder"])?;
        let registry_order = registry_names
            .iter()
            .map(|name| {
                tools
                    .iter()
                    .position(|tool| tool.name == *name)
                    .ok_or(Error::Contract)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if registry_order.len() != tools.len()
            || registry_order.iter().collect::<HashSet<_>>().len() != tools.len()
        {
            return Err(Error::Contract);
        }
        let controllers = controller["tools"].as_array().ok_or(Error::Contract)?;
        if controllers.len() != 3
            || CONTROLLERS.iter().any(|name| {
                controllers
                    .iter()
                    .filter(|tool| tool["name"] == *name && tool["inputSchema"].is_object())
                    .count()
                    != 1
            })
        {
            return Err(Error::Contract);
        }
        if !names.iter().any(|name| name == "general") {
            return Err(Error::Contract);
        }
        Ok(Self {
            tools,
            groups,
            core,
            controllers: Value::Array(controllers.clone()),
            registry_order,
        })
    }
    pub fn known_names(&self) -> Vec<&str> {
        self.tools.iter().map(|tool| tool.name.as_str()).collect()
    }
    pub fn controller_contract(&self) -> &Value {
        &self.controllers
    }
}
/// Captured once. Completing a job or changing groups cannot clear a capture
/// session's immutable exclusion of Stream controls with queued record effects.
#[derive(Clone, Copy, Debug)]
pub enum CaptureProfile {
    Standalone,
    TakeOrMedia,
}
#[derive(Debug)]
pub struct Availability {
    names: HashSet<String>,
}
impl Availability {
    pub fn new(
        contract: &Contract,
        names: impl IntoIterator<Item = String>,
        profile: CaptureProfile,
    ) -> Result<Self, Error> {
        let known = contract.known_names().into_iter().collect::<HashSet<_>>();
        let mut admitted = HashSet::new();
        for name in names {
            if !known.contains(name.as_str()) {
                return Err(Error::Contract);
            }
            if !matches!(profile, CaptureProfile::TakeOrMedia) || !STREAM.contains(&name.as_str()) {
                admitted.insert(name);
            }
        }
        Ok(Self { names: admitted })
    }
}
#[derive(Default, Debug)]
pub struct Settings {
    pub dynamic: bool,
    pub toolsets: Option<String>,
    pub tools: Option<String>,
    pub read_only: bool,
}
impl Settings {
    /// Caller supplies configuration values; this module reads no environment.
    pub fn parse(get: impl Fn(&str) -> Option<String>) -> Self {
        Self {
            dynamic: get("OBS_MCP_DYNAMIC_TOOLSETS").is_some_and(|s| enabled(&s)),
            toolsets: get("OBS_MCP_TOOLSETS"),
            tools: get("OBS_MCP_TOOLS"),
            read_only: get("OBS_MCP_READ_ONLY").is_some_and(|s| enabled(&s)),
        }
    }
}
fn split(value: Option<String>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct State {
    pub available: bool,
    pub registered: bool,
    pub enabled: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    VisibilityChanged { name: String, enabled: bool },
}
/// Rust-only provenance, never an extra public result/schema field. Exact
/// Source results are qualified only with complete Source availability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Projection {
    CompleteSourceAvailability,
    AvailabilityRestricted,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub known: usize,
    pub available: usize,
    pub registered: usize,
    pub enabled: usize,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupFacts {
    pub group: String,
    pub counts: Counts,
}
#[derive(Debug)]
pub struct Transition {
    pub result: Value,
    pub effects: Vec<Effect>,
    pub projection: Projection,
}
#[derive(Debug)]
pub struct Visibility {
    contract: Contract,
    states: Vec<State>,
    dynamic: bool,
    projection: Projection,
}
impl Visibility {
    pub fn new(
        contract: Contract,
        availability: Availability,
        settings: Settings,
    ) -> Result<Self, Error> {
        let known = contract.known_names().into_iter().collect::<HashSet<_>>();
        if availability
            .names
            .iter()
            .any(|name| !known.contains(name.as_str()))
        {
            return Err(Error::Contract);
        }
        let group_names = split(settings.toolsets);
        let mut explicit = Vec::new();
        let mut seen = HashSet::new();
        for name in split(settings.tools) {
            if seen.insert(name.clone()) {
                explicit.push(name);
            }
        }
        let mut groups = HashSet::new();
        let all = if settings.dynamic && group_names.is_empty() {
            groups.insert(
                contract
                    .groups
                    .iter()
                    .position(|g| g.name == "general")
                    .ok_or(Error::Contract)?,
            );
            false
        } else if (group_names.is_empty() && explicit.is_empty())
            || group_names.iter().any(|g| g == "all")
        {
            true
        } else {
            for name in &group_names {
                if name == "core" {
                    groups.extend(contract.core.iter().copied());
                } else if let Some(index) = contract.groups.iter().position(|g| g.name == *name) {
                    groups.insert(index);
                } else {
                    return Err(Error::Configuration(format!(
                        "Unknown OBS_MCP_TOOLSETS entry \"{name}\". Use all, core, or: {}",
                        contract
                            .groups
                            .iter()
                            .map(|g| g.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
            }
            false
        };
        let unknown = explicit
            .iter()
            .filter(|name| !known.contains(name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(Error::Configuration(format!(
                "Unknown OBS_MCP_TOOLS entries: {}",
                unknown.join(", ")
            )));
        }
        let states = contract
            .tools
            .iter()
            .map(|tool| {
                let available = availability.names.contains(&tool.name);
                let selected = (!settings.read_only || tool.read_only)
                    && (all || groups.contains(&tool.group) || explicit.contains(&tool.name));
                let registered = available
                    && (!settings.read_only || tool.read_only)
                    && (settings.dynamic || selected);
                State {
                    available,
                    registered,
                    enabled: registered && selected,
                }
            })
            .collect::<Vec<_>>();
        if !states.iter().any(|state| state.registered) {
            return Err(Error::Configuration(
                "The tool filter excludes every tool".into(),
            ));
        }
        let projection = if states.iter().all(|state| state.available) {
            Projection::CompleteSourceAvailability
        } else {
            Projection::AvailabilityRestricted
        };
        Ok(Self {
            contract,
            states,
            dynamic: settings.dynamic,
            projection,
        })
    }
    pub fn state(&self, name: &str) -> Option<&State> {
        self.contract
            .tools
            .iter()
            .position(|tool| tool.name == name)
            .map(|index| &self.states[index])
    }
    pub fn registered_names(&self) -> Vec<&str> {
        self.contract
            .registry_order
            .iter()
            .filter(|index| self.states[**index].registered)
            .map(|index| self.contract.tools[*index].name.as_str())
            .collect()
    }
    pub fn enabled_names(&self) -> Vec<&str> {
        self.contract
            .tools
            .iter()
            .zip(&self.states)
            .filter(|(_, state)| state.enabled)
            .map(|(tool, _)| tool.name.as_str())
            .collect()
    }
    /// Source controllers do not belong to group totals and remain available
    /// even when every ordinary group is disabled. No physical handler implied.
    pub fn controllers_available(&self) -> bool {
        self.dynamic
    }
    /// All four counts remain explicit, including unavailable/filtered groups.
    /// With restricted availability, list totals and changed names describe
    /// admitted registered handles, NOT the complete Source registry. A later
    /// frontend must deliberately choose and label that product policy.
    pub fn group_facts(&self) -> Vec<GroupFacts> {
        self.contract
            .groups
            .iter()
            .enumerate()
            .map(|(index, group)| {
                let mut counts = Counts::default();
                for (tool, state) in self.contract.tools.iter().zip(&self.states) {
                    if tool.group == index {
                        counts.known += 1;
                        counts.available += usize::from(state.available);
                        counts.registered += usize::from(state.registered);
                        counts.enabled += usize::from(state.enabled);
                    }
                }
                GroupFacts {
                    group: group.name.clone(),
                    counts,
                }
            })
            .collect()
    }
    pub fn list(&self) -> Result<Transition, Error> {
        if !self.dynamic {
            return Err(Error::UnavailableController);
        }
        let groups=self.contract.groups.iter().enumerate().filter_map(|(index,group)|{
            let states=self.contract.tools.iter().zip(&self.states).filter(|(tool,state)|tool.group==index&&state.registered).map(|(_,state)|state).collect::<Vec<_>>();
            (!states.is_empty()).then(||json!({"group":group.name,"enabled":states.iter().filter(|s|s.enabled).count(),"total":states.len(),"covers":group.covers}))
        }).collect::<Vec<_>>();
        let text = groups
            .iter()
            .map(|g| {
                format!(
                    "{} {} ({}/{}): {}",
                    if g["enabled"] == g["total"] {
                        "on "
                    } else if g["enabled"] == 0 {
                        "off"
                    } else {
                        "part"
                    },
                    g["group"].as_str().unwrap(),
                    g["enabled"],
                    g["total"],
                    g["covers"].as_str().unwrap()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(Transition {
            result: json!({"content":[{"type":"text","text":text}],"structuredContent":{"groups":groups}}),
            effects: vec![],
            projection: self.projection,
        })
    }
    /// Accepts already frontend-schema-validated groups. Invalid inputs still
    /// refuse atomically here; exact SDK schema-error projection stays frontend
    /// owned rather than invented by this library.
    pub fn set_groups(&mut self, groups: Vec<String>, enable: bool) -> Result<Transition, Error> {
        if !self.dynamic {
            return Err(Error::UnavailableController);
        }
        if groups.is_empty()
            || groups
                .iter()
                .any(|name| !self.contract.groups.iter().any(|g| g.name == *name))
        {
            return Err(Error::InvalidArguments);
        }
        let mut changed = Vec::new();
        let mut effects = Vec::new();
        for index in &self.contract.registry_order {
            let tool = &self.contract.tools[*index];
            let state = &mut self.states[*index];
            if state.registered
                && groups
                    .iter()
                    .any(|name| self.contract.groups[tool.group].name == *name)
                && state.enabled != enable
            {
                state.enabled = enable;
                changed.push(tool.name.clone());
                effects.push(Effect::VisibilityChanged {
                    name: tool.name.clone(),
                    enabled: enable,
                });
            }
        }
        let text = if changed.is_empty() {
            format!(
                "Nothing to change: {} {}",
                groups.join(", "),
                if enable { "already on" } else { "already off" }
            )
        } else {
            format!(
                "{} {} tool(s) in {}: {}",
                if enable { "Enabled" } else { "Disabled" },
                changed.len(),
                groups.join(", "),
                changed.join(", ")
            )
        };
        Ok(Transition {
            result: json!({"content":[{"type":"text","text":text}],"structuredContent":{"groups":groups,"changed":changed}}),
            effects,
            projection: self.projection,
        })
    }
}
