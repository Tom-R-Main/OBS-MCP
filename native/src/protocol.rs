// SPDX-License-Identifier: GPL-2.0-only
//! Admission and response context for the qualified native wire surface.
//! Unsupported semantics transfer the whole session to the SDK, never an
//! individual stateful call. A session cannot change eras while native-owned.
use crate::compat::MAX_FRAME_BYTES;
use serde_json::{Value, json};

const MODERN: &str = "2026-07-28";
const LEGACY: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const MAX_REPLAY_BYTES: usize = 2 * MAX_FRAME_BYTES;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum State {
    #[default]
    Fresh,
    Modern,
    LegacyInitializing,
    LegacyReady,
}

/// Retains only the admitted legacy initialization request and notification.
/// Both frame count and total retained bytes are bounded before copying input.
#[derive(Default)]
pub(crate) struct ProtocolSession {
    state: State,
    history: Vec<Vec<u8>>,
    replay_bytes: usize,
    native_service: bool,
    legacy_name: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Era {
    Modern,
    Legacy,
}

/// Carries the era and request identity that passed admission together through
/// dispatch and completion. Callers cannot independently select a reply era.
pub(crate) struct ReplyContext<'a> {
    id: &'a Value,
    era: Era,
}

#[derive(Clone)]
pub(crate) struct OwnedReplyContext {
    id: Value,
    era: Era,
}
impl OwnedReplyContext {
    pub(crate) fn error(&self, code: i64, message: &str) -> Value {
        json!({"jsonrpc":"2.0","id":self.id,"error":{"code":code,"message":message}})
    }
    pub(crate) fn complete(&self, result: Value, cacheable: bool) -> Value {
        ReplyContext {
            id: &self.id,
            era: self.era,
        }
        .complete(result, cacheable)
    }
}
pub(crate) enum Admission<'a> {
    Reply(Value),
    List(ReplyContext<'a>),
    ResourceList(ReplyContext<'a>),
    ResourceCompletion {
        reply: ReplyContext<'a>,
        prefix: &'a str,
    },
    ResourceRead {
        reply: ReplyContext<'a>,
        uri: &'a str,
    },
    Call {
        reply: ReplyContext<'a>,
        name: &'a str,
        arguments: Value,
    },
    Initialized,
    Compatibility,
}

fn capabilities(native_service: bool) -> Value {
    if native_service {
        return json!({"tools":{"listChanged":false},"resources":{"listChanged":false},"completions":{},"prompts":{"listChanged":false}});
    }
    json!({"tools":{"listChanged":true},"resources":{"listChanged":true,"subscribe":true},"completions":{},"prompts":{"listChanged":true}})
}

fn server_info() -> Value {
    json!({"name":"obs-mcp","version":env!("CARGO_PKG_VERSION")})
}

fn only_keys(value: &Value, allowed: &[&str]) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.keys().all(|key| allowed.contains(&key.as_str())))
}

fn empty_params(params: &Value) -> bool {
    params.is_null() || params.as_object().is_some_and(|object| object.is_empty())
}

fn client_info(info: &Value) -> bool {
    only_keys(info, &["name", "version"])
        && info.get("name").is_some_and(Value::is_string)
        && info.get("version").is_some_and(Value::is_string)
}

fn optional(value: &Value, key: &str, valid: impl FnOnce(&Value) -> bool) -> bool {
    value.get(key).is_none_or(valid)
}

fn native_icon(value: &Value) -> bool {
    only_keys(value, &["src", "mimeType", "sizes", "theme"])
        && value.get("src").is_some_and(Value::is_string)
        && optional(value, "mimeType", Value::is_string)
        && optional(value, "sizes", |sizes| {
            sizes
                .as_array()
                .is_some_and(|sizes| sizes.iter().all(Value::is_string))
        })
        && optional(value, "theme", |theme| {
            matches!(theme.as_str(), Some("light" | "dark"))
        })
}

/// Extended, present implementation metadata is display data. Only name is
/// used by the existing caller resolver; URLs/icons are never fetched here.
fn native_client_info(info: &Value) -> bool {
    only_keys(
        info,
        &[
            "name",
            "version",
            "title",
            "description",
            "websiteUrl",
            "icons",
        ],
    ) && info.get("name").is_some_and(Value::is_string)
        && info.get("version").is_some_and(Value::is_string)
        && ["title", "description", "websiteUrl"]
            .iter()
            .all(|key| optional(info, key, Value::is_string))
        && optional(info, "icons", |icons| {
            icons
                .as_array()
                .is_some_and(|icons| icons.iter().all(native_icon))
        })
}

fn object_record(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.values().all(Value::is_object))
}

/// These are recognized client OFFERS, not server capabilities or invocation
/// permissions. Runtime admits this mode only with live confirmation/dynamic
/// features disabled. No client request or input_required is enabled by this
/// parser. Legacy tasks and unknown vocabulary remain outside this deliberately
/// bounded admission slice.
fn native_client_offers(caps: &Value) -> bool {
    only_keys(
        caps,
        &[
            "roots",
            "sampling",
            "elicitation",
            "experimental",
            "extensions",
        ],
    ) && optional(caps, "roots", |roots| {
        only_keys(roots, &["listChanged"]) && optional(roots, "listChanged", Value::is_boolean)
    }) && optional(caps, "sampling", |sampling| {
        only_keys(sampling, &["context", "tools"])
            && optional(sampling, "context", Value::is_object)
            && optional(sampling, "tools", Value::is_object)
    }) && optional(caps, "elicitation", |elicitation| {
        only_keys(elicitation, &["form", "url"])
            && optional(elicitation, "form", |form| {
                form.is_object() && optional(form, "applyDefaults", Value::is_boolean)
            })
            && optional(elicitation, "url", Value::is_object)
    }) && optional(caps, "experimental", object_record)
        && optional(caps, "extensions", object_record)
}

fn admitted_client_info(info: &Value, native_service: bool) -> bool {
    if native_service {
        native_client_info(info)
    } else {
        client_info(info)
    }
}

fn admitted_client_offers(caps: &Value, native_service: bool) -> bool {
    if native_service {
        native_client_offers(caps)
    } else {
        caps.as_object().is_some_and(|caps| caps.is_empty())
    }
}

/// Default transitional admission stays strict and delegates richer metadata.
/// Native service admission validates only the recognized unused offer data.
fn inline_metadata(params: &Value, native_service: bool) -> bool {
    let Some(meta) = params.get("_meta") else {
        return false;
    };
    only_keys(
        meta,
        &[
            "io.modelcontextprotocol/protocolVersion",
            "io.modelcontextprotocol/clientInfo",
            "io.modelcontextprotocol/clientCapabilities",
        ],
    ) && meta
        .get("io.modelcontextprotocol/protocolVersion")
        .and_then(Value::as_str)
        == Some(MODERN)
        && meta
            .get("io.modelcontextprotocol/clientCapabilities")
            .is_some_and(|caps| admitted_client_offers(caps, native_service))
        && match meta.get("io.modelcontextprotocol/clientInfo") {
            Some(info) => admitted_client_info(info, native_service),
            // Modern Source metadata makes implementation info optional. Its
            // absence is not a remembered name or a synthesized identity. Only
            // the native service opts in; default handoff stays unchanged.
            None => native_service,
        }
}

impl ReplyContext<'_> {
    pub(crate) fn owned(&self) -> OwnedReplyContext {
        OwnedReplyContext {
            id: self.id.clone(),
            era: self.era,
        }
    }
    pub(crate) fn complete(&self, mut result: Value, cacheable: bool) -> Value {
        if self.era == Era::Modern {
            result["_meta"] = json!({"io.modelcontextprotocol/serverInfo":server_info()});
            result["resultType"] = json!("complete");
            if cacheable {
                result["ttlMs"] = json!(0);
                result["cacheScope"] = json!("private");
            }
        }
        json!({"jsonrpc":"2.0","id":self.id,"result":result})
    }

    pub(crate) fn cancelled_by(&self, frame: &[u8]) -> bool {
        serde_json::from_slice::<Value>(frame)
            .ok()
            .is_some_and(|next| {
                next.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
                    && next.get("id").is_none()
                    && next.get("method").and_then(Value::as_str) == Some("notifications/cancelled")
                    && next.pointer("/params/requestId") == Some(self.id)
            })
    }
}

impl ProtocolSession {
    pub(crate) fn native_service() -> Self {
        Self {
            native_service: true,
            ..Default::default()
        }
    }
    pub(crate) fn legacy_name(&self) -> Option<&str> {
        self.legacy_name.as_deref()
    }
    pub(crate) fn history(&self) -> &[Vec<u8>] {
        &self.history
    }

    fn retain_handshake(&mut self, frame: &[u8]) -> bool {
        let Some(total) = self.replay_bytes.checked_add(frame.len()) else {
            return false;
        };
        if self.history.len() >= 2 || frame.len() > MAX_FRAME_BYTES || total > MAX_REPLAY_BYTES {
            return false;
        }
        self.history.push(frame.to_vec());
        self.replay_bytes = total;
        true
    }

    pub(crate) fn admit<'a>(&mut self, value: &'a Value, frame: &[u8]) -> Admission<'a> {
        if !only_keys(value, &["jsonrpc", "method", "id", "params"])
            || value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        {
            return Admission::Compatibility;
        }
        let Some(method) = value.get("method").and_then(Value::as_str) else {
            return Admission::Compatibility;
        };
        let params = value.get("params").unwrap_or(&Value::Null);
        let id = value.get("id");
        if method == "notifications/initialized"
            && self.state == State::LegacyInitializing
            && id.is_none()
            && empty_params(params)
            && self.retain_handshake(frame)
        {
            self.state = State::LegacyReady;
            return Admission::Initialized;
        }
        let Some(id) = id.filter(|id| id.is_string() || id.is_i64() || id.is_u64()) else {
            return Admission::Compatibility;
        };
        if method == "initialize"
            && self.state == State::Fresh
            && only_keys(params, &["protocolVersion", "clientInfo", "capabilities"])
            && params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .is_some_and(|version| LEGACY.contains(&version))
            && params
                .get("clientInfo")
                .is_some_and(|info| admitted_client_info(info, self.native_service))
            && params
                .get("capabilities")
                .is_some_and(|caps| admitted_client_offers(caps, self.native_service))
            && self.retain_handshake(frame)
        {
            self.state = State::LegacyInitializing;
            self.legacy_name = params
                .pointer("/clientInfo/name")
                .and_then(Value::as_str)
                .map(str::to_owned);
            return Admission::Reply(
                ReplyContext {
                    id,
                    era: Era::Legacy,
                }
                .complete(
                    json!({"protocolVersion":params["protocolVersion"],"capabilities":capabilities(self.native_service),"serverInfo":server_info()}),
                    false,
                ),
            );
        }
        let era = if matches!(self.state, State::Fresh | State::Modern)
            && inline_metadata(params, self.native_service)
        {
            Era::Modern
        } else if self.state == State::LegacyReady && params.get("_meta").is_none() {
            Era::Legacy
        } else {
            return Admission::Compatibility;
        };
        let reply = ReplyContext { id, era };
        if method == "server/discover" && era == Era::Modern && only_keys(params, &["_meta"]) {
            self.state = State::Modern;
            return Admission::Reply(reply.complete(
                json!({"supportedVersions":[MODERN],"capabilities":capabilities(self.native_service)}),
                true,
            ));
        }
        if method == "prompts/list" && (empty_params(params) || only_keys(params, &["_meta"])) {
            if era == Era::Modern {
                self.state = State::Modern;
            }
            return Admission::Reply(reply.complete(crate::prompt_catalog::list(), true));
        }
        if method == "resources/list" && (empty_params(params) || only_keys(params, &["_meta"])) {
            if era == Era::Modern {
                self.state = State::Modern;
            }
            return Admission::ResourceList(reply);
        }
        if method == "resources/templates/list"
            && (empty_params(params) || only_keys(params, &["_meta"]))
        {
            if era == Era::Modern {
                self.state = State::Modern;
            }
            return Admission::Reply(
                reply.complete(crate::prompt_catalog::resource_templates(), true),
            );
        }
        if self.native_service
            && method == "resources/read"
            && only_keys(params, &["uri", "_meta"])
            && let Some(uri) = params["uri"].as_str()
        {
            if era == Era::Modern {
                self.state = State::Modern;
            }
            return Admission::ResourceRead { reply, uri };
        }
        if self.native_service
            && method == "completion/complete"
            && only_keys(params, &["ref", "argument", "context", "_meta"])
            && let Some(completion) = crate::resource_catalog::completion(params)
        {
            if era == Era::Modern {
                self.state = State::Modern;
            }
            return match completion {
                crate::resource_catalog::Completion::Reply(result) => {
                    Admission::Reply(reply.complete(result, false))
                }
                crate::resource_catalog::Completion::Missing(message) => {
                    Admission::Reply(reply.owned().error(-32602, &message))
                }
                crate::resource_catalog::Completion::Scenes(prefix) => {
                    Admission::ResourceCompletion { reply, prefix }
                }
            };
        }
        if method == "prompts/get"
            && only_keys(params, &["name", "arguments", "_meta"])
            && let Some(name) = params["name"].as_str()
            && let Some(result) =
                crate::prompt_catalog::get(name, params.get("arguments").unwrap_or(&json!({})))
        {
            if era == Era::Modern {
                self.state = State::Modern;
            }
            return Admission::Reply(reply.complete(result, false));
        }
        if method == "tools/list" && (empty_params(params) || only_keys(params, &["_meta"])) {
            if era == Era::Modern {
                self.state = State::Modern;
            }
            return Admission::List(reply);
        }
        if method == "ping" && era == Era::Legacy && empty_params(params) {
            return Admission::Reply(reply.complete(json!({}), false));
        }
        if method != "tools/call" || !only_keys(params, &["name", "arguments", "_meta"]) {
            return Admission::Compatibility;
        }
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return Admission::Compatibility;
        };
        if era == Era::Modern {
            self.state = State::Modern;
        }
        Admission::Call {
            reply,
            name,
            arguments: params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({})),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn modern(method: &str) -> Value {
        json!({"jsonrpc":"2.0","id":0,"method":method,"params":{"_meta":{"io.modelcontextprotocol/protocolVersion":MODERN,"io.modelcontextprotocol/clientInfo":{"name":"probe","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}})
    }

    fn initialize() -> Value {
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":LEGACY[0],"clientInfo":{"name":"probe","version":"1"},"capabilities":{}}})
    }

    fn admit<'a>(session: &mut ProtocolSession, message: &'a Value) -> Admission<'a> {
        session.admit(message, &serde_json::to_vec(message).unwrap())
    }

    #[test]
    fn legacy_initialization_is_ordered_and_bounded() {
        let mut session = ProtocolSession::default();
        assert!(matches!(
            admit(&mut session, &initialize()),
            Admission::Reply(_)
        ));
        let ping = json!({"jsonrpc":"2.0","id":2,"method":"ping"});
        assert!(matches!(
            admit(&mut session, &ping),
            Admission::Compatibility
        ));
        let initialized = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
        assert!(matches!(
            admit(&mut session, &initialized),
            Admission::Initialized
        ));
        assert!(matches!(admit(&mut session, &ping), Admission::Reply(_)));
        assert!(matches!(
            admit(&mut session, &initialized),
            Admission::Compatibility
        ));
        assert!(matches!(
            admit(&mut session, &initialize()),
            Admission::Compatibility
        ));
        assert_eq!(session.history().len(), 2);
        assert!(session.replay_bytes <= MAX_REPLAY_BYTES);
        assert!(!session.retain_handshake(b"another frame"));
        let mut fresh = ProtocolSession::default();
        assert!(!fresh.retain_handshake(&vec![0; MAX_FRAME_BYTES + 1]));
        assert!(fresh.history().is_empty());
    }

    #[test]
    fn admitted_modern_calls_also_lock_the_session_era() {
        let mut session = ProtocolSession::default();
        assert!(matches!(
            admit(&mut session, &modern("tools/list")),
            Admission::List(_)
        ));
        assert!(matches!(
            admit(&mut session, &initialize()),
            Admission::Compatibility
        ));
        assert!(session.history().is_empty());
    }

    #[test]
    fn metadata_and_replies_preserve_the_qualified_sdk_shapes() {
        let mut session = ProtocolSession::default();
        let message = modern("server/discover");
        let Admission::Reply(response) = admit(&mut session, &message) else {
            panic!("ordinary discovery must be native");
        };
        assert_eq!(response["id"], 0);
        assert_eq!(response["result"]["resultType"], "complete");
        assert_eq!(response["result"]["ttlMs"], 0);
        assert_eq!(response["result"]["cacheScope"], "private");
        assert_eq!(
            response["result"]["_meta"]["io.modelcontextprotocol/serverInfo"],
            server_info()
        );
        let mut extended = message.clone();
        extended["params"]["_meta"]["progressToken"] = json!("needs-sdk");
        assert!(matches!(
            admit(&mut session, &extended),
            Admission::Compatibility
        ));
        let mut rich = message;
        rich["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] =
            json!({"elicitation":{"form":{}}});
        assert!(matches!(
            admit(&mut session, &rich),
            Admission::Compatibility
        ));
        let result = ReplyContext {
            id: &json!(0),
            era: Era::Legacy,
        }
        .complete(json!({}), true);
        assert_eq!(result, json!({"jsonrpc":"2.0","id":0,"result":{}}));
    }

    #[test]
    fn cancellation_is_bound_to_the_admitted_request_and_envelope() {
        let id = json!("read-1");
        let reply = ReplyContext {
            id: &id,
            era: Era::Modern,
        };
        let cancelled =
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":id}});
        assert!(reply.cancelled_by(&serde_json::to_vec(&cancelled).unwrap()));
        let mut request = cancelled.clone();
        request["id"] = json!(2);
        assert!(!reply.cancelled_by(&serde_json::to_vec(&request).unwrap()));
        let mut unrelated = cancelled.clone();
        unrelated["params"]["requestId"] = json!("other");
        assert!(!reply.cancelled_by(&serde_json::to_vec(&unrelated).unwrap()));
        let mut malformed = cancelled;
        malformed["jsonrpc"] = json!("1.0");
        assert!(!reply.cancelled_by(&serde_json::to_vec(&malformed).unwrap()));
    }

    fn extended_info() -> Value {
        json!({"name":"offer-name","version":"1","title":"Not the caller","description":"Display metadata","websiteUrl":"not validated as a URL by Source","icons":[{"src":"also only a string","mimeType":"image/png","sizes":["16x16","any"],"theme":"dark"}]})
    }
    fn unused_offers() -> Value {
        json!({"roots":{"listChanged":true},"sampling":{"context":{},"tools":{}},"elicitation":{"form":{"applyDefaults":true,"extension":{"any":[1,null]}},"url":{}},"experimental":{"vendor":{}},"extensions":{"vendor":{}}})
    }
    #[test]
    fn stateful_typed_unused_offers_and_extended_present_info_do_not_change_default_admission() {
        assert!(native_client_info(&extended_info()));
        assert!(native_client_offers(&unused_offers()));
        let mut message = modern("server/discover");
        message["params"]["_meta"]["io.modelcontextprotocol/clientInfo"] = extended_info();
        message["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] = unused_offers();
        assert!(matches!(
            admit(&mut ProtocolSession::default(), &message),
            Admission::Compatibility
        ));
        let mut session = ProtocolSession::native_service();
        let Admission::Reply(result) = admit(&mut session, &message) else {
            panic!("recognized offers refused");
        };
        assert_eq!(result["result"]["capabilities"], capabilities(true));
        for name in ["roots", "sampling", "elicitation", "tasks", "extensions"] {
            assert!(result["result"]["capabilities"].get(name).is_none());
        }
        let caller = obs_mcp_native::control::caller_name(obs_mcp_native::control::CallerContext {
            envelope: message.pointer("/params/_meta"),
            ..Default::default()
        });
        assert_eq!(caller, "offer-name");
        message["params"]["_meta"]["io.modelcontextprotocol/clientInfo"]["name"] = json!("");
        assert_eq!(
            obs_mcp_native::control::caller_name(obs_mcp_native::control::CallerContext {
                envelope: message.pointer("/params/_meta"),
                ..Default::default()
            }),
            obs_mcp_native::control::UNIDENTIFIED
        );
    }
    #[test]
    fn unused_offer_shape_validation_is_closed_nullable_safe_and_not_invocation_admission() {
        for valid in [
            json!({}),
            json!({"roots":{}}),
            json!({"sampling":{}}),
            json!({"elicitation":{}}),
            json!({"elicitation":{"form":{}}}),
            json!({"elicitation":{"url":{"nested":[1,null]}}}),
            json!({"roots":{"listChanged":false}}),
        ] {
            assert!(native_client_offers(&valid), "{valid}");
        }
        for invalid in [
            Value::Null,
            json!([]),
            json!(true),
            json!({"roots":null}),
            json!({"roots":{"listChanged":1}}),
            json!({"roots":{"unknown":true}}),
            json!({"sampling":{"context":null}}),
            json!({"sampling":{"tools":[]}}),
            json!({"elicitation":null}),
            json!({"elicitation":{"form":[]}}),
            json!({"elicitation":{"form":{"applyDefaults":"yes"}}}),
            json!({"elicitation":{"url":false}}),
            json!({"experimental":{"vendor":[]}}),
            json!({"extensions":{"vendor":false}}),
            json!({"tasks":{}}),
            json!({"futureCapability":{}}),
        ] {
            assert!(!native_client_offers(&invalid), "{invalid}");
        }
        let mut session = ProtocolSession::native_service();
        let mut message = modern("tools/list");
        message["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] = unused_offers();
        assert!(matches!(admit(&mut session, &message), Admission::List(_)));
        for method in [
            "roots/list",
            "sampling/createMessage",
            "elicitation/create",
            "tasks/list",
        ] {
            message["method"] = json!(method);
            assert!(matches!(
                admit(&mut session, &message),
                Admission::Compatibility
            ));
        }
        message["method"] = json!("tools/list");
        assert!(matches!(admit(&mut session, &message), Admission::List(_)));
    }
    #[test]
    fn extended_info_rejects_malformed_shapes_without_retaining_or_changing_identity() {
        for invalid in [
            Value::Null,
            json!([]),
            json!({"name":"n"}),
            json!({"name":null,"version":"1"}),
            json!({"name":"n","version":1}),
            json!({"name":"n","version":"1","title":null}),
            json!({"name":"n","version":"1","description":false}),
            json!({"name":"n","version":"1","icons":null}),
            json!({"name":"n","version":"1","icons":[{}]}),
            json!({"name":"n","version":"1","icons":[{"src":1}]}),
            json!({"name":"n","version":"1","icons":[{"src":"x","sizes":[1]}]}),
            json!({"name":"n","version":"1","icons":[{"src":"x","theme":"sepia"}]}),
            json!({"name":"n","version":"1","futureInfo":{}}),
        ] {
            assert!(!native_client_info(&invalid), "{invalid}");
            let mut session = ProtocolSession::native_service();
            let mut message = initialize();
            message["params"]["clientInfo"] = invalid;
            assert!(matches!(
                admit(&mut session, &message),
                Admission::Compatibility
            ));
            assert!(session.history().is_empty());
            assert!(session.legacy_name().is_none());
        }
        let mut missing = modern("tools/list");
        missing["params"]["_meta"]
            .as_object_mut()
            .unwrap()
            .remove("io.modelcontextprotocol/clientInfo");
        assert!(matches!(
            admit(&mut ProtocolSession::default(), &missing),
            Admission::Compatibility
        ));
    }
    #[test]
    fn absent_modern_info_is_unidentified_per_request_without_loosening_legacy_or_defaults() {
        use obs_mcp_native::control::{CallerContext, UNIDENTIFIED, caller_name};
        let mut session = ProtocolSession::native_service();
        let named = modern("tools/list");
        assert!(matches!(admit(&mut session, &named), Admission::List(_)));
        let mut absent = named.clone();
        absent["params"]["_meta"]
            .as_object_mut()
            .unwrap()
            .remove("io.modelcontextprotocol/clientInfo");
        assert!(matches!(admit(&mut session, &absent), Admission::List(_)));
        assert_eq!(session.state, State::Modern);
        assert!(session.legacy_name().is_none());
        assert!(session.history().is_empty());
        assert_eq!(
            caller_name(CallerContext {
                envelope: absent.pointer("/params/_meta"),
                legacy_name: session.legacy_name(),
                ..Default::default()
            }),
            UNIDENTIFIED
        );
        for invalid in [Value::Null, json!({}), json!({"name":1,"version":"1"})] {
            let mut present = absent.clone();
            present["params"]["_meta"]["io.modelcontextprotocol/clientInfo"] = invalid;
            assert!(matches!(
                admit(&mut session, &present),
                Admission::Compatibility
            ));
        }
        let mut without_caps = absent.clone();
        without_caps["params"]["_meta"]
            .as_object_mut()
            .unwrap()
            .remove("io.modelcontextprotocol/clientCapabilities");
        assert!(matches!(
            admit(&mut session, &without_caps),
            Admission::Compatibility
        ));
        assert!(matches!(
            admit(&mut ProtocolSession::default(), &absent),
            Admission::Compatibility
        ));
        let mut missing_legacy = initialize();
        missing_legacy["params"]
            .as_object_mut()
            .unwrap()
            .remove("clientInfo");
        let mut legacy = ProtocolSession::native_service();
        assert!(matches!(
            admit(&mut legacy, &missing_legacy),
            Admission::Compatibility
        ));
        assert_eq!(legacy.state, State::Fresh);
        assert!(legacy.history().is_empty());
        assert!(matches!(
            admit(&mut session, &initialize()),
            Admission::Compatibility
        ));
        assert!(matches!(admit(&mut session, &absent), Admission::List(_)));
    }
    #[test]
    fn rich_legacy_handshake_preserves_era_caller_and_existing_replay_limits() {
        let mut message = initialize();
        message["params"]["clientInfo"] = extended_info();
        message["params"]["capabilities"] = unused_offers();
        assert!(matches!(
            admit(&mut ProtocolSession::default(), &message),
            Admission::Compatibility
        ));
        let mut session = ProtocolSession::native_service();
        assert!(matches!(admit(&mut session, &message), Admission::Reply(_)));
        assert_eq!(session.legacy_name(), Some("offer-name"));
        assert!(matches!(
            admit(
                &mut session,
                &json!({"jsonrpc":"2.0","method":"notifications/initialized"})
            ),
            Admission::Initialized
        ));
        assert_eq!(session.history().len(), 2);
        assert!(matches!(
            admit(&mut session, &modern("tools/list")),
            Admission::Compatibility
        ));
        assert!(matches!(
            admit(
                &mut session,
                &json!({"jsonrpc":"2.0","id":3,"method":"tools/list"})
            ),
            Admission::List(_)
        ));
        assert_eq!(session.legacy_name(), Some("offer-name"));
        let mut bounded = ProtocolSession::native_service();
        let mut bytes = serde_json::to_vec(&message).unwrap();
        bytes.resize(MAX_FRAME_BYTES + 1, b' ');
        assert!(matches!(
            bounded.admit(&message, &bytes),
            Admission::Compatibility
        ));
        assert!(bounded.history().is_empty());
        assert!(bounded.legacy_name().is_none());
    }
}
