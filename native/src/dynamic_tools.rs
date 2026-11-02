// SPDX-License-Identifier: GPL-2.0-only
//! Binary-local visibility and publication owner. No OBS or Service capability.
use crate::{
    protocol::OwnedReplyContext,
    schema,
    stdio_output::{Cycle, Output, Reservation},
};
use obs_mcp_native::dynamic_visibility::{
    Availability, CaptureProfile, Contract, Projection, Settings, Transition, Visibility,
};
use serde_json::{Value, json};
use std::io;

pub(crate) const CONTROLLERS: [&str; 3] = [
    "obs-list-toolsets",
    "obs-enable-toolset",
    "obs-disable-toolset",
];
const GROUPS: usize = 14;
const GROUP_ENTRIES: usize = 1024;
const ARGUMENT_BYTES: usize = 64 * 1024;
const IDENTIFIER_BYTES: usize = 1024;
const SUBSCRIPTIONS: usize = 1024;
const SUBSCRIPTION_BYTES: usize = 256 * 1024;

pub(crate) fn controller(name: &str) -> bool {
    CONTROLLERS.contains(&name)
}
fn failure() -> io::Error {
    io::Error::other(
        "Native dynamic visibility capacity or contract unavailable; no visibility change was admitted",
    )
}
fn encoded_bound(value: &Value, limit: usize) -> bool {
    struct Count {
        bytes: usize,
        limit: usize,
    }
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(failure)?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(&mut Count { bytes: 0, limit }, value).is_ok()
}
/// Applied before protocol clones a controller argument graph or retained ID.
pub(crate) fn bounded_call(id: &Value, args: &Value) -> bool {
    encoded_bound(id, IDENTIFIER_BYTES)
        && encoded_bound(args, ARGUMENT_BYTES)
        && args
            .get("groups")
            .and_then(Value::as_array)
            .is_none_or(|groups| groups.len() <= GROUP_ENTRIES)
}
pub(crate) fn bounded_context(value: &Value) -> bool {
    encoded_bound(value, ARGUMENT_BYTES)
}
pub(crate) fn bounded_identity(value: &Value) -> bool {
    encoded_bound(value, IDENTIFIER_BYTES)
}
struct Subscriber {
    id: Value,
    tools: bool,
    bytes: usize,
}
pub(crate) struct DynamicTools {
    contract: Contract,
    available: Vec<String>,
    capture: bool,
    settings: Settings,
    overrides: [Option<bool>; GROUPS],
    groups: Vec<String>,
    visibility: Visibility,
    manifest: Value,
    subscribers: Vec<Subscriber>,
    subscriber_bytes: usize,
}
impl DynamicTools {
    pub(crate) fn new(
        available: Vec<String>,
        capture: bool,
        get: impl Fn(&str) -> Option<String>,
    ) -> io::Result<Self> {
        let catalog = schema::catalog();
        let dynamic = &catalog["toolsets"]["dynamic"];
        let contract = Contract::from_source(catalog, dynamic).map_err(|_| failure())?;
        let settings = Settings::parse(get);
        if !settings.dynamic
            || settings
                .toolsets
                .as_ref()
                .is_some_and(|v| v.len() > ARGUMENT_BYTES || v.split(',').count() > GROUP_ENTRIES)
            || settings
                .tools
                .as_ref()
                .is_some_and(|v| v.len() > ARGUMENT_BYTES || v.split(',').count() > GROUP_ENTRIES)
        {
            return Err(failure());
        }
        let groups = catalog["toolsets"]["groups"]
            .as_array()
            .ok_or_else(failure)?
            .iter()
            .map(|g| g.as_str().map(str::to_owned).ok_or_else(failure))
            .collect::<io::Result<Vec<_>>>()?;
        if groups.len() != GROUPS
            || CONTROLLERS.iter().any(|name| {
                schema::tool(name).is_none_or(|tool| tool.get("outputSchema").is_some())
            })
        {
            return Err(failure());
        }
        let visibility = Self::fresh(&contract, &available, capture, &settings)?;
        let manifest = Self::manifest_for(&visibility);
        Ok(Self {
            contract,
            available,
            capture,
            settings,
            overrides: [None; GROUPS],
            groups,
            visibility,
            manifest,
            subscribers: Vec::new(),
            subscriber_bytes: 0,
        })
    }
    fn fresh(
        contract: &Contract,
        available: &[String],
        capture: bool,
        settings: &Settings,
    ) -> io::Result<Visibility> {
        let settings = Settings {
            dynamic: settings.dynamic,
            toolsets: settings.toolsets.clone(),
            tools: settings.tools.clone(),
            read_only: settings.read_only,
        };
        let profile = if capture {
            CaptureProfile::TakeOrMedia
        } else {
            CaptureProfile::Standalone
        };
        let admitted = Availability::new(contract, available.iter().cloned(), profile)
            .map_err(|_| failure())?;
        Visibility::new(contract.clone(), admitted, settings)
            .map_err(|error| io::Error::other(error.to_string()))
    }
    fn manifest_for(visibility: &Visibility) -> Value {
        let enabled = visibility.enabled_names();
        let mut tools = schema::catalog()["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|tool| enabled.contains(&tool["name"].as_str().unwrap()))
            .cloned()
            .collect::<Vec<_>>();
        tools.extend(
            schema::catalog()["toolsets"]["dynamic"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .cloned(),
        );
        json!({"tools":tools})
    }
    pub(crate) fn manifest(&self) -> &Value {
        &self.manifest
    }
    pub(crate) fn includes(&self, name: &str) -> bool {
        controller(name)
            || self
                .visibility
                .state(name)
                .is_some_and(|state| state.enabled)
    }
    pub(crate) fn refusal(&self, name: &str) -> Option<(i64, String)> {
        self.visibility.state(name).and_then(|state| {
            if !state.available {
                None
            } else if !state.registered {
                Some((-32602, format!("Tool {name} not found")))
            } else if !state.enabled {
                Some((-32602, format!("Tool {name} disabled")))
            } else {
                None
            }
        })
    }
    fn disclose(visibility: &Visibility, transition: &mut Transition) {
        if transition.projection == Projection::AvailabilityRestricted {
            let groups=visibility.group_facts().into_iter().map(|g|json!({"group":g.group,"known":g.counts.known,"available":g.counts.available,"registered":g.counts.registered,"enabled":g.counts.enabled})).collect::<Vec<_>>();
            transition.result["structuredContent"]["nativeVisibility"] =
                json!({"projection":"availability-restricted","groups":groups});
        }
    }
    /// Preview uses only immutable initial configuration and <=14 last group
    /// overrides. No unbounded transition history or writer clone is retained.
    pub(crate) fn call(
        &mut self,
        name: &str,
        args: &Value,
        reply: &OwnedReplyContext,
        output: &Output,
    ) -> io::Result<Reservation> {
        let mut preview = Self::fresh(
            &self.contract,
            &self.available,
            self.capture,
            &self.settings,
        )?;
        for (index, enabled) in self.overrides.iter().enumerate() {
            if let Some(enabled) = enabled {
                preview
                    .set_groups(vec![self.groups[index].clone()], *enabled)
                    .map_err(|_| failure())?;
            }
        }
        let requested = if name == CONTROLLERS[0] {
            Vec::new()
        } else {
            args["groups"]
                .as_array()
                .ok_or_else(failure)?
                .iter()
                .map(|group| group.as_str().map(str::to_owned).ok_or_else(failure))
                .collect::<io::Result<Vec<_>>>()?
        };
        let enabled = name == CONTROLLERS[1];
        let mut transition = if name == CONTROLLERS[0] {
            preview.list()
        } else {
            preview.set_groups(requested.clone(), enabled)
        }
        .map_err(|_| failure())?;
        Self::disclose(&preview, &mut transition);
        let notices = if transition.effects.is_empty() {
            Vec::new()
        } else {
            reply.tool_changes(self.subscribers.iter().filter(|s| s.tools).map(|s| &s.id))
        };
        let notifications = if transition.effects.is_empty() || notices.is_empty() {
            None
        } else {
            Some(output.reserve_publication(&[Cycle {
                values: &notices,
                repetitions: transition.effects.len(),
            }])?)
        };
        let frame = reply.complete(transition.result, false);
        // Both the finite notification cursor and separately suppressible reply
        // are reserved BEFORE replacing any visible state. No pipe await here.
        let reply_reservation = output.reserve_publication(&[Cycle {
            values: std::slice::from_ref(&frame),
            repetitions: 1,
        }])?;
        let manifest = Self::manifest_for(&preview);
        for name in requested {
            let index = self
                .groups
                .iter()
                .position(|g| g == &name)
                .ok_or_else(failure)?;
            self.overrides[index] = Some(enabled);
        }
        self.visibility = preview;
        self.manifest = manifest;
        if let Some(reserved) = notifications {
            drop(reserved.commit());
        }
        Ok(reply_reservation)
    }
    pub(crate) fn listen(
        &mut self,
        reply: &OwnedReplyContext,
        filter: &Value,
        valid_filter: bool,
        output: &Output,
    ) -> io::Result<Option<Value>> {
        let Some(id) = reply.subscription_id(IDENTIFIER_BYTES) else {
            return Ok(Some(
                reply.error(-32603, "Native subscription identity capacity exceeded"),
            ));
        };
        // Source's limit check precedes body validation and duplicate-ID replacement.
        if self.subscribers.len() >= SUBSCRIPTIONS {
            return Ok(Some(reply.error(-32603, "Subscription limit reached")));
        }
        if !valid_filter {
            return Ok(Some(reply.error(-32602,"Invalid params: 'notifications' is required and must be a valid SubscriptionFilter")));
        }
        let bytes = serde_json::to_vec(&id).map_err(io::Error::other)?.len();
        let prior = self.subscribers.iter().position(|s| s.id == id);
        let retained =
            self.subscriber_bytes - prior.map_or(0, |index| self.subscribers[index].bytes);
        if retained
            .checked_add(bytes)
            .is_none_or(|n| n > SUBSCRIPTION_BYTES)
        {
            return Ok(Some(
                reply.error(-32603, "Native subscription identity capacity exceeded"),
            ));
        }
        let tools = filter["toolsListChanged"] == true;
        let honored = if tools {
            json!({"toolsListChanged":true})
        } else {
            json!({})
        };
        let ack = reply.subscription_ack(honored);
        let reserved = output.reserve_publication(&[Cycle {
            values: std::slice::from_ref(&ack),
            repetitions: 1,
        }])?;
        let subscriber = Subscriber { id, tools, bytes };
        if let Some(index) = prior {
            self.subscribers[index] = subscriber;
        } else {
            self.subscribers.push(subscriber);
        }
        self.subscriber_bytes = retained + bytes;
        drop(reserved.commit());
        Ok(None)
    }
    pub(crate) fn cancel_subscription(&mut self, id: &Value) -> bool {
        if let Some(index) = self.subscribers.iter().position(|s| &s.id == id) {
            let old = self.subscribers.remove(index);
            self.subscriber_bytes -= old.bytes;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Admission, ProtocolSession};
    fn reply() -> OwnedReplyContext {
        let mut protocol = ProtocolSession::native_dynamic_service();
        let initialize = json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","clientInfo":{"name":"capacity","version":"1"},"capabilities":{}}});
        assert!(matches!(
            protocol.admit(&initialize, &serde_json::to_vec(&initialize).unwrap()),
            Admission::Reply(_)
        ));
        let initialized = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
        assert!(matches!(
            protocol.admit(&initialized, &serde_json::to_vec(&initialized).unwrap()),
            Admission::Initialized
        ));
        let frame = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"obs-enable-toolset","arguments":{"groups":["record"]}}});
        match protocol.admit(&frame, &serde_json::to_vec(&frame).unwrap()) {
            Admission::Call { reply, .. } => reply.owned(),
            _ => panic!("call admission"),
        }
    }
    fn full() -> DynamicTools {
        let names = schema::catalog()["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_owned())
            .collect();
        DynamicTools::new(names, false, |name| {
            (name == "OBS_MCP_DYNAMIC_TOOLSETS").then(|| "true".into())
        })
        .unwrap()
    }
    #[tokio::test]
    async fn reply_capacity_refusal_drops_notification_reservation_before_visibility_commit() {
        let (mut tools, reply) = (full(), reply());
        let before = tools.manifest().clone();
        let (writer, reader) = tokio::io::duplex(1);
        let (output, worker) = Output::spawn(writer);
        for _ in 0..127 {
            output.try_send(&json!(0)).unwrap();
        }
        let args = json!({"groups":["record"]});
        assert!(
            tools
                .call("obs-enable-toolset", &args, &reply, &output)
                .is_err()
        );
        assert_eq!(tools.manifest(), &before);
        output
            .try_send(&json!(1))
            .expect("failed notification reservation returned its credit");
        drop(output);
        drop(reader);
        assert!(worker.await.unwrap().is_err());
    }
    #[test]
    fn bounds_precede_cloning_of_retained_controller_graphs() {
        assert!(!bounded_call(&json!("x".repeat(1025)), &json!({})));
        assert!(!bounded_call(
            &json!(1),
            &json!({"groups":vec!["record";1025]})
        ));
        assert!(!bounded_context(&json!({"x":"x".repeat(65536)})));
    }
}
