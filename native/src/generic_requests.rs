// SPDX-License-Identifier: GPL-2.0-only
//! Closed literal setters. Validation and callback formatting follow the actual
//! Source registrations; receipt acceptance never establishes a postcondition.
use crate::{
    control::{Disposition, GateRequest},
    custody::{Clock, SharedControl},
    custom_mutations::{self, FollowUp, SessionReadback},
    observation::ObservationError,
    operation_store::{GenericKind as K, GenericPhase as P, OperationStore},
    scene_audit::ScreenshotLimit,
    session::{BeforeDispatch, Invalidation, RequestError, SessionHandle},
    stage_transport::Cancellation,
};
use serde_json::{Map, Value, json};
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;
#[derive(Clone)]
pub struct Call {
    kind: K,
    name: &'static str,
    title: &'static str,
    arguments: Value,
    follow_up: Option<FollowUp>,
}
impl std::fmt::Debug for Call {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericCall")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}
impl Call {
    pub fn requires_readback(&self) -> bool {
        self.follow_up.is_some()
    }
    pub fn kind(&self) -> K {
        self.kind
    }
    pub fn cancelled_or_expired(&self) -> Value {
        error(format!(
            "{} failed: Operation cancelled or expired before dispatch",
            self.title
        ))
    }
    pub fn unsupported(&self) -> Value {
        // Source's callback rejects the missing-volume combination before it
        // asks the transport about capability. Service evaluates policy first.
        if let Some(result) = self.business_refusal() {
            return result;
        }
        self.failure(format!(
            "OBS WebSocket does not advertise support for request '{}'",
            self.kind.request_type()
        ))
    }
    /// Encoded argument bytes, counted without allocating a second frame.
    pub fn retained_bytes(&self) -> Option<usize> {
        crate::js_json::encoded_len(&self.arguments, crate::js_json::Limits::default()).ok()
    }
    /// Compatibility copy; the owning schema path can move this value instead.
    pub fn arguments(&self) -> Value {
        self.arguments.clone()
    }
    pub fn into_arguments(self) -> Value {
        self.arguments
    }
    // Allocate the wire value only for the active command. The retained FIFO
    // and final guard share the single normalized public argument owner.
    fn request_data(&self) -> Value {
        if matches!(
            self.kind,
            K::TriggerStudioModeTransition | K::SaveReplayBuffer
        ) {
            return Value::Null;
        }
        let mut data = self.arguments.clone();
        if self.follow_up.is_some()
            && let Some(fields) = data.as_object_mut()
        {
            fields.remove("includeScreenshot");
        }
        let mapping = match self.kind {
            K::CreateSceneItem | K::SceneItemEnabled => Some(("enabled", "sceneItemEnabled")),
            K::CurrentSceneTransitionDuration => Some(("duration", "transitionDuration")),
            _ => None,
        };
        if let Some((public, wire)) = mapping
            && let Some(fields) = data.as_object_mut()
            && let Some(value) = fields.remove(public)
        {
            fields.insert(wire.into(), value);
        }
        data
    }
    pub fn name(&self) -> &'static str {
        self.name
    }
    pub fn parse(name: &str, args: &Value) -> Option<Self> {
        if let Some(input) = custom_mutations::Request::parse(name, args) {
            let input_kind = input.kind();
            let kind = match input_kind {
                custom_mutations::Kind::CreateInput => K::CreateInput,
                custom_mutations::Kind::InputSettings => K::InputSettings,
            };
            let title = match input_kind {
                custom_mutations::Kind::CreateInput => "Create Input",
                custom_mutations::Kind::InputSettings => "Set Input Settings",
            };
            let (arguments, follow_up) = input.into_public_parts();
            return Some(Self {
                kind,
                name: input_kind.name(),
                title,
                arguments,
                follow_up: Some(follow_up),
            });
        }
        if let Some(definition) = opaque_definition(name) {
            return parse_opaque(definition, args);
        }
        if let Some(definition) = configuration_definition(name) {
            return parse_configuration(definition, args);
        }
        if let Some(definition) = extra_definition(name) {
            return parse_extra(definition, args);
        }
        let (kind, title) = match name {
            "obs-set-input-mute" => (K::InputMute, "Set Input Mute"),
            "obs-set-input-audio-balance" => (K::InputAudioBalance, "Set Input Audio Balance"),
            "obs-set-input-audio-sync-offset" => {
                (K::InputAudioSyncOffset, "Set Input Audio Sync Offset")
            }
            "obs-set-input-audio-monitor-type" => {
                (K::InputAudioMonitorType, "Set Input Audio Monitor Type")
            }
            "obs-set-input-deinterlace-field-order" => (
                K::InputDeinterlaceFieldOrder,
                "Set Input Deinterlace Field Order",
            ),
            "obs-set-input-deinterlace-mode" => {
                (K::InputDeinterlaceMode, "Set Input Deinterlace Mode")
            }
            "obs-set-scene-item-index" => (K::SceneItemIndex, "Set Scene Item Index"),
            "obs-set-scene-item-locked" => (K::SceneItemLocked, "Set Scene Item Locked"),
            "obs-set-scene-item-blend-mode" => (K::SceneItemBlendMode, "Set Scene Item Blend Mode"),
            "obs-set-source-filter-enabled" => {
                (K::SourceFilterEnabled, "Set Source Filter Enabled")
            }
            _ => return None,
        };
        let source = args.as_object()?;
        let mut data = Map::new();
        let mut string = |key: &str, required: bool| -> Option<()> {
            if let Some(value) = source.get(key) {
                value.as_str()?;
                data.insert(key.into(), value.clone());
            } else if required {
                return None;
            }
            Some(())
        };
        match kind {
            K::InputMute
            | K::InputAudioBalance
            | K::InputAudioSyncOffset
            | K::InputAudioMonitorType => string("inputName", true)?,
            K::InputDeinterlaceFieldOrder | K::InputDeinterlaceMode => {
                string("inputName", false)?;
                string("inputUuid", false)?;
            }
            K::SceneItemIndex | K::SceneItemLocked | K::SceneItemBlendMode => {
                for key in ["canvasUuid", "sceneName", "sceneUuid"] {
                    string(key, false)?;
                }
            }
            K::SourceFilterEnabled => {
                string("canvasUuid", false)?;
                string("sourceName", true)?;
                string("filterName", true)?;
            }
            _ => return None,
        }
        let mut add = |key: &str, check: fn(&Value) -> bool| -> Option<()> {
            let value = source.get(key)?;
            if !check(value) {
                return None;
            }
            data.insert(key.into(), value.clone());
            Some(())
        };
        fn boolean(v: &Value) -> bool {
            v.is_boolean()
        }
        fn index(v: &Value) -> bool {
            v.as_f64().is_some_and(|n| {
                n.is_finite() && (0. ..=9_007_199_254_740_991.).contains(&n) && n.fract() == 0.
            })
        }
        match kind {
            K::InputMute => add("inputMuted", boolean)?,
            K::InputAudioBalance => add("inputAudioBalance", |v| {
                v.as_f64()
                    .is_some_and(|n| n.is_finite() && (0.0..=1.0).contains(&n))
            })?,
            K::InputAudioSyncOffset => add("inputAudioSyncOffset", |v| {
                v.as_f64()
                    .is_some_and(|n| n.is_finite() && (-950.0..=20000.0).contains(&n))
            })?,
            K::InputAudioMonitorType => add("monitorType", Value::is_string)?,
            K::InputDeinterlaceFieldOrder => add("inputDeinterlaceFieldOrder", |v| {
                matches!(
                    v.as_str(),
                    Some("OBS_DEINTERLACE_FIELD_ORDER_TOP" | "OBS_DEINTERLACE_FIELD_ORDER_BOTTOM")
                )
            })?,
            K::InputDeinterlaceMode => add("inputDeinterlaceMode", |v| {
                matches!(
                    v.as_str(),
                    Some(
                        "OBS_DEINTERLACE_MODE_DISABLE"
                            | "OBS_DEINTERLACE_MODE_DISCARD"
                            | "OBS_DEINTERLACE_MODE_RETRO"
                            | "OBS_DEINTERLACE_MODE_BLEND"
                            | "OBS_DEINTERLACE_MODE_BLEND_2X"
                            | "OBS_DEINTERLACE_MODE_LINEAR"
                            | "OBS_DEINTERLACE_MODE_LINEAR_2X"
                            | "OBS_DEINTERLACE_MODE_YADIF"
                            | "OBS_DEINTERLACE_MODE_YADIF_2X"
                    )
                )
            })?,
            K::SceneItemIndex => {
                add("sceneItemId", index)?;
                add("sceneItemIndex", index)?;
            }
            K::SceneItemLocked => {
                add("sceneItemId", index)?;
                add("sceneItemLocked", boolean)?;
            }
            K::SceneItemBlendMode => {
                add("sceneItemId", index)?;
                add("sceneItemBlendMode", |v| {
                    matches!(
                        v.as_str(),
                        Some(
                            "OBS_BLEND_NORMAL"
                                | "OBS_BLEND_ADDITIVE"
                                | "OBS_BLEND_SUBTRACT"
                                | "OBS_BLEND_SCREEN"
                                | "OBS_BLEND_MULTIPLY"
                                | "OBS_BLEND_LIGHTEN"
                                | "OBS_BLEND_DARKEN"
                        )
                    )
                })?;
            }
            K::SourceFilterEnabled => add("filterEnabled", boolean)?,
            _ => return None,
        }
        let name = match kind {
            K::InputMute => "obs-set-input-mute",
            K::InputAudioBalance => "obs-set-input-audio-balance",
            K::InputAudioSyncOffset => "obs-set-input-audio-sync-offset",
            K::InputAudioMonitorType => "obs-set-input-audio-monitor-type",
            K::InputDeinterlaceFieldOrder => "obs-set-input-deinterlace-field-order",
            K::InputDeinterlaceMode => "obs-set-input-deinterlace-mode",
            K::SceneItemIndex => "obs-set-scene-item-index",
            K::SceneItemLocked => "obs-set-scene-item-locked",
            K::SceneItemBlendMode => "obs-set-scene-item-blend-mode",
            K::SourceFilterEnabled => "obs-set-source-filter-enabled",
            _ => return None,
        };
        Self {
            kind,
            name,
            title,
            arguments: Value::Object(data),
            follow_up: None,
        }
        .source_numbers()
    }
    // This owns the complete normalized public payload before FIFO charging.
    // Shared Source encoding and the physical actor must use this same codec.
    fn source_numbers(mut self) -> Option<Self> {
        crate::js_json::normalize_with_limits(
            &mut self.arguments,
            crate::js_json::Limits::default(),
        )
        .ok()?;
        Some(self)
    }
    fn output_settings_ack(&self) -> bool {
        self.kind == K::ProfileParameter
            && matches!(
                self.arguments["parameterCategory"].as_str(),
                Some("Output" | "SimpleOutput" | "AdvOut")
            )
    }
    /// Pure callback business validation. The caller must evaluate Source's
    /// lease/take policy first; this returns no authority or dispatch request.
    pub fn business_refusal(&self) -> Option<Value> {
        (self.kind == K::InputVolume
            && self.arguments.get("inputVolumeMul").is_none()
            && self.arguments.get("inputVolumeDb").is_none())
        .then(|| self.failure("Either inputVolumeMul or inputVolumeDb must be provided"))
    }
    fn failure(&self, message: impl AsRef<str>) -> Value {
        let prefix = match self.kind {
            K::CreateSourceFilter => "Error creating source filter",
            K::SourceFilterSettings => "Error setting source filter settings",
            K::CreateInput => "Error creating input",
            K::InputSettings => "Error setting input settings",
            K::CreateSceneItem => "Error creating scene item",
            K::SceneItemEnabled => "Error setting scene item visibility",
            K::CurrentSceneTransitionDuration => "Error setting transition duration",
            K::InputMuteToggle => "Error toggling input mute state",
            K::InputVolume => "Error setting input volume",
            K::VideoSettings => "Error setting video settings",
            K::ProfileParameter => "Error setting profile parameter",
            _ => return error(format!("{} failed: {}", self.title, message.as_ref())),
        };
        error(format!("{prefix}: {}", message.as_ref()))
    }
    fn success(&self, response: Value) -> Value {
        if self.kind == K::DuplicateSceneItem {
            let response = if response.is_object() {
                response
            } else {
                json!({})
            };
            match crate::tool_validation::tool_output_error(
                self.name,
                &output_schema(self.name),
                &response,
            ) {
                Ok(error) => return error,
                Err(crate::tool_validation::ProjectionUnavailable::NoIssues) => {}
                Err(_) => {
                    return error(
                        "Native output validation projection unavailable; request acceptance is retained without an observed postcondition",
                    );
                }
            }
            return json!({"content":[{"type":"text","text":serde_json::to_string_pretty(&response).unwrap()}],"structuredContent":response});
        }
        let optional = |key: &str| {
            self.arguments
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or("undefined")
        };

        let s = |key: &str| self.arguments[key].as_str().unwrap();
        let message = match self.kind {
            K::BroadcastCustomEvent => "Custom event broadcast successfully".into(),
            K::CurrentSceneTransitionSettings => {
                "Successfully updated current transition settings".into()
            }
            K::CreateSourceFilter => format!(
                "Successfully created filter '{}' of kind '{}' on source '{}'",
                s("filterName"),
                s("filterKind"),
                s("sourceName")
            ),
            K::SourceFilterSettings => format!(
                "Successfully updated settings for filter '{}' on source '{}'",
                s("filterName"),
                s("sourceName")
            ),
            K::SaveSourceScreenshot => {
                format!("Successfully saved screenshot to: {}", s("imageFilePath"))
            }
            K::PersistentData => format!(
                "Successfully set value for slot '{}' in realm '{}'",
                s("slotName"),
                s("realm")
            ),
            K::CurrentSceneCollection => format!(
                "Successfully switched to scene collection: {}",
                s("sceneCollectionName")
            ),
            K::CreateSceneCollection => format!(
                "Successfully created and switched to scene collection: {}",
                s("sceneCollectionName")
            ),
            K::CurrentProfile => format!("Successfully switched to profile: {}", s("profileName")),
            K::CreateProfile => format!(
                "Successfully created and switched to profile: {}",
                s("profileName")
            ),
            K::RemoveProfile => format!("Successfully removed profile: {}", s("profileName")),
            K::StreamServiceSettings => format!(
                "Successfully updated stream service settings to type: {}",
                s("streamServiceType")
            ),
            K::RecordDirectory => format!(
                "Successfully set record directory to: {}",
                s("recordDirectory")
            ),
            K::OutputSettings => format!("Settings updated for output '{}'", s("outputName")),
            K::VideoSettings => "Successfully updated video settings".into(),
            K::ProfileParameter => format!(
                "Successfully set parameter '{}' in category '{}'{}",
                s("parameterName"),
                s("parameterCategory"),
                if self.output_settings_ack() {
                    ". Warning: OBS applies output settings only when it rebuilds its outputs; restart OBS before starting an output"
                } else {
                    ""
                }
            ),
            K::MediaInputCursor => format!(
                "Successfully set media cursor position to {}ms for input: {}",
                number(self.arguments["mediaCursor"].as_f64().unwrap()),
                s("inputName")
            ),
            K::MediaInputCursorOffset => format!(
                "Successfully offset media cursor position by {}ms for input: {}",
                number(self.arguments["mediaCursorOffset"].as_f64().unwrap()),
                s("inputName")
            ),
            K::MediaInputAction => format!(
                "Successfully triggered media action '{}' for input: {}",
                s("mediaAction"),
                s("inputName")
            ),
            K::InputMuteToggle => format!(
                "Successfully toggled mute state for input '{}', now {}",
                s("inputName"),
                if js_truthy(response.get("inputMuted")) {
                    "muted"
                } else {
                    "unmuted"
                }
            ),
            K::InputVolume => format!("Successfully set volume for input: {}", s("inputName")),
            K::StreamCaption => "Successfully sent stream caption".into(),
            K::SaveReplayBuffer => "Replay buffer saved".into(),
            K::InputMute => format!(
                "Successfully {} input: {}",
                if self.arguments["inputMuted"] == true {
                    "muted"
                } else {
                    "unmuted"
                },
                s("inputName")
            ),
            K::InputAudioBalance => format!(
                "Successfully set audio balance to {} for input: {}",
                number(self.arguments["inputAudioBalance"].as_f64().unwrap()),
                s("inputName")
            ),
            K::InputAudioSyncOffset => format!(
                "Successfully set audio sync offset to {}ms for input: {}",
                number(self.arguments["inputAudioSyncOffset"].as_f64().unwrap()),
                s("inputName")
            ),
            K::InputAudioMonitorType => format!(
                "Successfully set audio monitor type to {} for input: {}",
                s("monitorType"),
                s("inputName")
            ),
            K::SourceFilterEnabled => format!(
                "Successfully {} filter '{}' on source '{}'",
                if self.arguments["filterEnabled"] == true {
                    "enabled"
                } else {
                    "disabled"
                },
                s("filterName"),
                s("sourceName")
            ),
            K::CreateScene => format!("Successfully created scene: {}", s("sceneName")),
            K::RemoveScene => format!("Successfully removed scene: {}", s("sceneName")),
            K::RemoveSceneItem => format!(
                "Successfully removed item with ID {} from {}",
                number(self.arguments["sceneItemId"].as_f64().unwrap()),
                optional("sceneName")
            ),
            K::RemoveSourceFilter => format!(
                "Successfully removed filter '{}' from source '{}'",
                s("filterName"),
                s("sourceName")
            ),
            K::SourceFilterName => format!(
                "Successfully renamed filter '{}' to '{}' on source '{}'",
                s("filterName"),
                s("newFilterName"),
                s("sourceName")
            ),
            K::SourceFilterIndex => format!(
                "Successfully set filter '{}' to index {} on source '{}'",
                s("filterName"),
                number(self.arguments["filterIndex"].as_f64().unwrap()),
                s("sourceName")
            ),
            K::RemoveInput => format!("Successfully removed input: {}", s("inputName")),
            K::InputName => format!(
                "Successfully renamed input '{}' to '{}'",
                s("inputName"),
                s("newInputName")
            ),
            K::CurrentProgramScene => format!("Successfully switched to scene: {}", s("sceneName")),
            K::CurrentPreviewScene => {
                format!("Successfully set preview scene to: {}", s("sceneName"))
            }
            K::TriggerStudioModeTransition => {
                "Successfully triggered studio mode transition".into()
            }
            K::CurrentSceneTransition => format!(
                "Successfully set current transition to: {}",
                s("transitionName")
            ),
            K::CreateSceneItem => {
                let id = match js_string(
                    response
                        .as_object()
                        .and_then(|object| object.get("sceneItemId")),
                ) {
                    Ok(value) => value,
                    Err(()) => return self.failure("Cannot convert object to primitive value"),
                };
                format!(
                    "Successfully added {} to {} with ID: {id}",
                    optional("sourceName"),
                    optional("sceneName")
                )
            }
            K::SceneItemEnabled => format!(
                "Successfully {} item with ID {} in {}",
                if self.arguments["enabled"] == true {
                    "showed"
                } else {
                    "hid"
                },
                number(self.arguments["sceneItemId"].as_f64().unwrap()),
                optional("sceneName")
            ),
            K::CurrentSceneTransitionDuration => format!(
                "Successfully set transition duration to: {}ms",
                number(self.arguments["duration"].as_f64().unwrap())
            ),
            _ => format!("{} completed successfully", self.title),
        };
        json!({"content":[{"type":"text","text":message}],"structuredContent":{"message":message}})
    }
}
fn number(n: f64) -> String {
    crate::observation::js_string(Some(&json!(n)))
}

fn error(message: impl Into<String>) -> Value {
    json!({"content":[{"type":"text","text":message.into()}],"isError":true})
}
pub(crate) fn policy(
    call: &Call,
    caller: &str,
    control: &SharedControl,
    clock: &Clock,
) -> Result<Disposition, ()> {
    Ok(control.lock().map_err(|_| ())?.evaluate(
        GateRequest {
            tool_name: call.name,
            arguments: &call.arguments,
            read_only_hint: false,
            caller,
            confirmation: Default::default(),
        },
        clock().epoch_ms,
    ))
}
/// One execution-services context from the persistent physical owner. No
/// credentials or additional socket/writer ownership is carried here.
#[derive(Clone, Default)]
pub struct GenericServices {
    pub pending_output_settings: Option<Arc<AtomicBool>>,
    pub readback: Option<ReadbackServices>,
}
#[derive(Clone)]
pub struct ReadbackServices {
    pub capabilities: Option<Arc<HashSet<String>>>,
    pub screenshot_limit: Result<ScreenshotLimit, ObservationError>,
}
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    store: OperationStore,
    call: Call,
    session: SessionHandle,
    control: SharedControl,
    clock: Clock,
    caller: String,
    cancel: Cancellation,
    id: String,
    timeout: Duration,
    admitted_at: Instant,
) -> Value {
    execute_with_services(
        store,
        call,
        session,
        control,
        clock,
        caller,
        cancel,
        id,
        timeout,
        admitted_at,
        GenericServices::default(),
    )
    .await
}
/// The retained physical Service supplies its one client-local readiness flag.
/// The acknowledgement hook cannot dispatch, publish, or establish observation.
#[allow(clippy::too_many_arguments)]
pub async fn execute_with_pending(
    store: OperationStore,
    call: Call,
    session: SessionHandle,
    control: SharedControl,
    clock: Clock,
    caller: String,
    cancel: Cancellation,
    id: String,
    timeout: Duration,
    admitted_at: Instant,
    pending_output_settings: Arc<AtomicBool>,
) -> Value {
    execute_with_services(
        store,
        call,
        session,
        control,
        clock,
        caller,
        cancel,
        id,
        timeout,
        admitted_at,
        GenericServices {
            pending_output_settings: Some(pending_output_settings),
            ..Default::default()
        },
    )
    .await
}
#[allow(clippy::too_many_arguments)]
pub async fn execute_with_services(
    mut store: OperationStore,
    call: Call,
    session: SessionHandle,
    control: SharedControl,
    clock: Clock,
    caller: String,
    cancel: Cancellation,
    id: String,
    timeout: Duration,
    admitted_at: Instant,
    services: GenericServices,
) -> Value {
    // Share immutable validated arguments with the final guard. The socket
    // frame remains separately owned; active buffers are outside the FIFO's
    // encoded-payload budget and are not a process-RSS guarantee.
    let call = Arc::new(call);
    let Some(deadline) = admitted_at
        .checked_add(timeout)
        .filter(|_| !timeout.is_zero())
    else {
        return error("Invalid operation deadline");
    };
    if store
        .read_view()
        .admission()
        .map_or(true, |admission| admission.blocking)
    {
        return error("Unresolved receipt custody prevents dispatch");
    }
    match policy(&call, &caller, &control, &clock) {
        Ok(Disposition::Allow) => {}
        Ok(refusal) => {
            return refusal
                .tool_result()
                .unwrap_or_else(|| error("Control policy refused dispatch"));
        }
        Err(()) => return error("Control policy is unavailable"),
    }
    if let Some(result) = call.business_refusal() {
        return result;
    }
    if call.output_settings_ack() && services.pending_output_settings.is_none() {
        return error("Native output-settings state is unavailable; no OBS request was dispatched");
    }
    if call.follow_up.is_some() && services.readback.is_none() {
        return error(
            "Native input readback services are unavailable; no OBS request was dispatched",
        );
    }
    if cancel.is_cancelled() || Instant::now() >= deadline {
        return call.cancelled_or_expired();
    }
    let prepared = match store.prepare_generic(
        &id,
        call.kind,
        session.token().generation(),
        &clock().utc,
    ) {
        Ok(receipt) => receipt,
        Err(_) => {
            return error(
                "Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change",
            );
        }
    };
    let receipt = match store.update_generic(&prepared, P::Dispatching, &clock().utc) {
        Ok(receipt) => receipt,
        Err(_) => {
            return error(
                "Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change",
            );
        }
    };
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        let _ = store.update_generic(&receipt, P::NotDispatched, &clock().utc);
        return error("Operation deadline expired before dispatch");
    };
    let view = store.read_view();
    let fence = store.dispatch_fence();
    let own = id;
    let command = call.clone();
    let checked_clock = clock.clone();
    let refusal = Arc::new(Mutex::new(None));
    let observed = refusal.clone();
    let readback_cancel = cancel.clone();
    let response = session
        .request_checked(
            session.token(),
            call.kind.request_type(),
            call.request_data(),
            remaining,
            move || {
                let decision = policy(&command, &caller, &control, &checked_clock)
                    .map_err(|_| "Control policy unavailable".to_owned())?;
                if decision != Disposition::Allow {
                    let _ = observed.lock().map(|mut target| *target = Some(decision));
                    return Err("Control policy changed before dispatch".into());
                }
                if cancel.is_cancelled() || Instant::now() >= deadline {
                    return Err("Operation cancelled or expired before dispatch".into());
                }
                if fence.as_ref().map_or_else(
                    || {
                        !view.is_dispatching(&own).unwrap_or(false)
                            || view
                                .admission_except(Some(&own))
                                .map_or(true, |admission| admission.blocking)
                    },
                    |fence| !fence.permits(&own).unwrap_or(false),
                ) {
                    return Err("Unresolved receipt custody prevents dispatch".into());
                }
                Ok(())
            },
        )
        .await;
    // A known ACK changes client-local readiness even if publishing its durable
    // outcome subsequently fails. Caller cancellation cannot undo that knowledge.
    if response.is_ok()
        && call.output_settings_ack()
        && let Some(pending) = services.pending_output_settings.as_ref()
    {
        pending.store(true, Ordering::Release);
    }
    let phase = match &response {
        Ok(_) => P::Accepted,
        Err(RequestError::BeforeDispatch(_)) => P::NotDispatched,
        Err(e) if e.rejection_code().is_some() => P::Rejected,
        Err(_) => P::Unknown,
    };
    if store.update_generic(&receipt, phase, &clock().utc).is_err() {
        return error(
            "Operation receipt outcome is uncertain; inspect obs-observe before any further production change",
        );
    }
    match response {
        Ok(response) => {
            if let Some(follow_up) = call.follow_up {
                let context = services
                    .readback
                    .expect("required readback services checked before intent");
                let mut reader = SessionReadback::new(session, context.capabilities);
                let result = follow_up
                    .finish(
                        &call.arguments,
                        &response,
                        &mut reader,
                        context.screenshot_limit,
                        &readback_cancel,
                        deadline,
                    )
                    .await;
                drop(store);
                result
            } else {
                call.success(response)
            }
        }
        Err(e) => {
            if let Some(result) = refusal
                .lock()
                .ok()
                .and_then(|mut value| value.take())
                .and_then(|value| value.tool_result())
            {
                return result;
            }
            let message=e.obs_failure_message(call.kind.request_type()).unwrap_or_else(||match e {RequestError::Unknown(Invalidation::DeadlineAfterDispatch)=>format!("Request {} timed out after {}ms",call.kind.request_type(),timeout.as_millis()),RequestError::Unknown(Invalidation::Disconnected)=>"WebSocket connection closed: No reason provided".into(),RequestError::BeforeDispatch(reason)=>match reason{BeforeDispatch::GuardRefused(reason)=>reason,BeforeDispatch::Capacity|BeforeDispatch::ByteCapacity=>"OBS request was not sent: request capacity or encoded-byte policy unavailable".into(),BeforeDispatch::InvalidRequest=>"OBS request was not sent: native request or encoding policy refused it".into(),BeforeDispatch::Deadline=>"OBS request was not sent: original operation deadline expired".into(),BeforeDispatch::Cancelled=>"OBS request was not sent: cancelled before dispatch".into(),BeforeDispatch::StaleConnection=>"OBS request was not sent: connection identity changed".into(),BeforeDispatch::Closed(_)=>"OBS request was not sent: admitted connection is unavailable".into(),BeforeDispatch::IdentityExhausted=>"OBS request was not sent: request identity exhausted".into()},_=>"OBS request outcome is uncertain; inspect obs-observe before another change".into()});
            call.failure(message)
        }
    }
}

// These plugin and remote-file callbacks carry opaque data, but their request
// identities remain closed. Their ACKs grant neither local file authority nor
// an observed plugin postcondition. No follow-up reader is created here.
fn opaque_definition(name: &str) -> Option<(K, &'static str, &'static str)> {
    Some(match name {
        "obs-broadcast-custom-event" => (
            K::BroadcastCustomEvent,
            "Broadcast Custom Event",
            "obs-broadcast-custom-event",
        ),
        "obs-set-transition-settings" => (
            K::CurrentSceneTransitionSettings,
            "Set Transition Settings",
            "obs-set-transition-settings",
        ),
        "obs-create-source-filter" => (
            K::CreateSourceFilter,
            "Create Source Filter",
            "obs-create-source-filter",
        ),
        "obs-set-source-filter-settings" => (
            K::SourceFilterSettings,
            "Set Source Filter Settings",
            "obs-set-source-filter-settings",
        ),
        "obs-press-input-property-button" => (
            K::InputPropertiesButton,
            "Press Input Property Button",
            "obs-press-input-property-button",
        ),
        "obs-save-source-screenshot" => (
            K::SaveSourceScreenshot,
            "Save Source Screenshot",
            "obs-save-source-screenshot",
        ),
        _ => return None,
    })
}
fn parse_opaque(
    (kind, title, name): (K, &'static str, &'static str),
    args: &Value,
) -> Option<Call> {
    let source = args.as_object()?;
    let mut data = Map::new();
    let fields: &[(&str, bool)] = match kind {
        K::BroadcastCustomEvent | K::CurrentSceneTransitionSettings => &[],
        K::CreateSourceFilter => &[
            ("canvasUuid", false),
            ("sourceName", true),
            ("filterName", true),
            ("filterKind", true),
        ],
        K::SourceFilterSettings => &[
            ("canvasUuid", false),
            ("sourceName", true),
            ("filterName", true),
        ],
        K::InputPropertiesButton => &[
            ("inputName", false),
            ("inputUuid", false),
            ("propertyName", true),
        ],
        K::SaveSourceScreenshot => &[
            ("canvasUuid", false),
            ("sourceName", false),
            ("sourceUuid", false),
            ("imageFormat", true),
            ("imageFilePath", true),
        ],
        _ => return None,
    };
    for &(key, required) in fields {
        if let Some(value) = source.get(key) {
            value.as_str()?;
            data.insert(key.into(), value.clone());
        } else if required {
            return None;
        }
    }
    let record = match kind {
        K::BroadcastCustomEvent => Some(("eventData", true)),
        K::CurrentSceneTransitionSettings => Some(("transitionSettings", true)),
        K::CreateSourceFilter => Some(("filterSettings", false)),
        K::SourceFilterSettings => Some(("filterSettings", true)),
        _ => None,
    };
    if let Some((key, required)) = record {
        if let Some(value) = source.get(key) {
            let mut value = value.as_object()?.clone();
            // Exact Source Zod record boundary: nested unknown JSON stays owned
            // and unchanged; constructor/prototype/private sentinel keys remain.
            value.remove("__proto__");
            data.insert(key.into(), Value::Object(value));
        } else if required {
            return None;
        }
    }
    if kind == K::SourceFilterSettings
        && let Some(value) = source.get("overlay")
    {
        value.as_bool()?;
        data.insert("overlay".into(), value.clone());
    }
    if kind == K::SaveSourceScreenshot {
        for key in ["imageWidth", "imageHeight", "imageCompressionQuality"] {
            if let Some(value) = source.get(key) {
                if !value.as_f64()?.is_finite() {
                    return None;
                }
                data.insert(key.into(), value.clone());
            }
        }
    }
    Call {
        kind,
        name,
        title,
        arguments: Value::Object(data),
        follow_up: None,
    }
    .source_numbers()
}

fn configuration_definition(name: &str) -> Option<(K, &'static str, &'static str)> {
    Some(match name {
        "obs-set-persistent-data" => (
            K::PersistentData,
            "Set Persistent Data",
            "obs-set-persistent-data",
        ),
        "obs-set-current-scene-collection" => (
            K::CurrentSceneCollection,
            "Set Current Scene Collection",
            "obs-set-current-scene-collection",
        ),
        "obs-create-scene-collection" => (
            K::CreateSceneCollection,
            "Create Scene Collection",
            "obs-create-scene-collection",
        ),
        "obs-set-current-profile" => (
            K::CurrentProfile,
            "Set Current Profile",
            "obs-set-current-profile",
        ),
        "obs-create-profile" => (K::CreateProfile, "Create Profile", "obs-create-profile"),
        "obs-remove-profile" => (K::RemoveProfile, "Remove Profile", "obs-remove-profile"),
        "obs-set-stream-service-settings" => (
            K::StreamServiceSettings,
            "Set Stream Service Settings",
            "obs-set-stream-service-settings",
        ),
        "obs-set-record-directory" => (
            K::RecordDirectory,
            "Set Record Directory",
            "obs-set-record-directory",
        ),
        "obs-set-output-settings" => (
            K::OutputSettings,
            "Set Output Settings",
            "obs-set-output-settings",
        ),
        "obs-set-video-settings" => (
            K::VideoSettings,
            "Set Video Settings",
            "obs-set-video-settings",
        ),
        "obs-set-profile-parameter" => (
            K::ProfileParameter,
            "Set Profile Parameter",
            "obs-set-profile-parameter",
        ),
        _ => return None,
    })
}
fn parse_configuration(
    (kind, title, name): (K, &'static str, &'static str),
    args: &Value,
) -> Option<Call> {
    let source = args.as_object()?;
    let mut data = Map::new();
    let strings: &[&str] = match kind {
        K::PersistentData => &["realm", "slotName"],
        K::CurrentSceneCollection | K::CreateSceneCollection => &["sceneCollectionName"],
        K::CurrentProfile | K::CreateProfile | K::RemoveProfile => &["profileName"],
        K::StreamServiceSettings => &["streamServiceType"],
        K::RecordDirectory => &["recordDirectory"],
        K::OutputSettings => &["outputName"],
        K::VideoSettings => &[],
        K::ProfileParameter => &["parameterCategory", "parameterName"],
        _ => return None,
    };
    for key in strings {
        let value = source.get(*key)?;
        value.as_str()?;
        data.insert((*key).into(), value.clone());
    }
    match kind {
        K::PersistentData => {
            data.insert("slotValue".into(), source.get("slotValue")?.clone());
        }
        K::StreamServiceSettings | K::OutputSettings => {
            let key = if kind == K::StreamServiceSettings {
                "streamServiceSettings"
            } else {
                "outputSettings"
            };
            let mut record = source.get(key)?.as_object()?.clone();
            record.remove("__proto__");
            data.insert(key.into(), Value::Object(record));
        }
        K::VideoSettings => {
            for key in [
                "fpsNumerator",
                "fpsDenominator",
                "baseWidth",
                "baseHeight",
                "outputWidth",
                "outputHeight",
            ] {
                if let Some(value) = source.get(key) {
                    let number = value.as_f64()?;
                    if !number.is_finite()
                        || number < 1.0
                        || (!key.starts_with("fps") && number > 4096.0)
                    {
                        return None;
                    }
                    data.insert(key.into(), value.clone());
                }
            }
        }
        K::ProfileParameter => {
            let value = source.get("parameterValue")?;
            if !value.is_null() && !value.is_string() {
                return None;
            }
            data.insert("parameterValue".into(), value.clone());
        }
        _ => {}
    }
    Call {
        kind,
        name,
        title,
        arguments: Value::Object(data),
        follow_up: None,
    }
    .source_numbers()
}
fn extra_definition(name: &str) -> Option<(K, &'static str, &'static str)> {
    Some(match name {
        "obs-set-media-input-cursor" => (
            K::MediaInputCursor,
            "Set Media Input Cursor",
            "obs-set-media-input-cursor",
        ),
        "obs-offset-media-input-cursor" => (
            K::MediaInputCursorOffset,
            "Offset Media Input Cursor",
            "obs-offset-media-input-cursor",
        ),
        "obs-trigger-media-input-action" => (
            K::MediaInputAction,
            "Trigger Media Input Action",
            "obs-trigger-media-input-action",
        ),
        "obs-set-input-audio-tracks" => (
            K::InputAudioTracks,
            "Set Input Audio Tracks",
            "obs-set-input-audio-tracks",
        ),
        "obs-toggle-input-mute" => (
            K::InputMuteToggle,
            "Toggle Input Mute",
            "obs-toggle-input-mute",
        ),
        "obs-set-input-volume" => (K::InputVolume, "Set Input Volume", "obs-set-input-volume"),
        "obs-send-stream-caption" => (
            K::StreamCaption,
            "Send Stream Caption",
            "obs-send-stream-caption",
        ),
        "obs-save-replay-buffer" => (
            K::SaveReplayBuffer,
            "Save Replay Buffer",
            "obs-save-replay-buffer",
        ),
        "obs-create-scene" => (K::CreateScene, "Create Scene", "obs-create-scene"),
        "obs-remove-scene" => (K::RemoveScene, "Remove Scene", "obs-remove-scene"),
        "obs-remove-scene-item" => (
            K::RemoveSceneItem,
            "Remove Scene Item",
            "obs-remove-scene-item",
        ),
        "obs-remove-source-filter" => (
            K::RemoveSourceFilter,
            "Remove Source Filter",
            "obs-remove-source-filter",
        ),
        "obs-set-source-filter-name" => (
            K::SourceFilterName,
            "Rename Source Filter",
            "obs-set-source-filter-name",
        ),
        "obs-set-source-filter-index" => (
            K::SourceFilterIndex,
            "Set Source Filter Index",
            "obs-set-source-filter-index",
        ),
        "obs-remove-input" => (K::RemoveInput, "Remove Input", "obs-remove-input"),
        "obs-set-input-name" => (K::InputName, "Rename Input", "obs-set-input-name"),
        "obs-duplicate-scene-item" => (
            K::DuplicateSceneItem,
            "Duplicate Scene Item",
            "obs-duplicate-scene-item",
        ),
        "obs-set-scene-name" => (K::SceneName, "Set Scene Name", "obs-set-scene-name"),
        "obs-create-scene-item" => (
            K::CreateSceneItem,
            "Create Scene Item",
            "obs-create-scene-item",
        ),
        "obs-set-scene-item-enabled" => (
            K::SceneItemEnabled,
            "Set Scene Item Visibility",
            "obs-set-scene-item-enabled",
        ),
        "obs-set-current-scene" => (
            K::CurrentProgramScene,
            "Set Current Scene",
            "obs-set-current-scene",
        ),
        "obs-set-preview-scene" => (
            K::CurrentPreviewScene,
            "Set Preview Scene",
            "obs-set-preview-scene",
        ),
        "obs-trigger-studio-transition" => (
            K::TriggerStudioModeTransition,
            "Trigger Studio Transition",
            "obs-trigger-studio-transition",
        ),
        "obs-trigger-transition" => (
            K::TriggerStudioModeTransition,
            "Trigger Transition",
            "obs-trigger-transition",
        ),
        "obs-set-current-transition" => (
            K::CurrentSceneTransition,
            "Set Current Transition",
            "obs-set-current-transition",
        ),
        "obs-set-scene-transition-override" => (
            K::SceneSceneTransitionOverride,
            "Set Scene Transition Override",
            "obs-set-scene-transition-override",
        ),
        "obs-set-tbar-position" => (
            K::TBarPosition,
            "Set T-Bar Position",
            "obs-set-tbar-position",
        ),
        "obs-set-transition-duration" => (
            K::CurrentSceneTransitionDuration,
            "Set Transition Duration",
            "obs-set-transition-duration",
        ),
        _ => return None,
    })
}
fn parse_extra((kind, title, name): (K, &'static str, &'static str), args: &Value) -> Option<Call> {
    let source = args.as_object()?;
    let mut data = Map::new();
    if matches!(kind, K::TriggerStudioModeTransition | K::SaveReplayBuffer) {
        return Some(Call {
            kind,
            title,
            name,
            arguments: json!({}),
            follow_up: None,
        });
    }
    let scene = &[
        ("canvasUuid", false),
        ("sceneName", false),
        ("sceneUuid", false),
    ][..];
    let fields: &[(&str, bool)] = match kind {
        K::MediaInputCursor
        | K::MediaInputCursorOffset
        | K::MediaInputAction
        | K::InputMuteToggle
        | K::InputVolume => &[("inputName", true)],
        K::InputAudioTracks => &[("inputName", false), ("inputUuid", false)],
        K::StreamCaption => &[("captionText", true)],
        K::CreateScene | K::RemoveScene => &[("canvasUuid", false), ("sceneName", true)],
        K::RemoveSceneItem
        | K::DuplicateSceneItem
        | K::SceneName
        | K::SceneItemEnabled
        | K::SceneSceneTransitionOverride => scene,
        K::CreateSceneItem => &[
            ("canvasUuid", false),
            ("sceneName", false),
            ("sceneUuid", false),
            ("sourceName", false),
            ("sourceUuid", false),
        ],
        K::RemoveSourceFilter | K::SourceFilterName | K::SourceFilterIndex => &[
            ("canvasUuid", false),
            ("sourceName", true),
            ("filterName", true),
        ],
        K::RemoveInput | K::InputName => &[("inputName", true)],
        K::CurrentProgramScene | K::CurrentPreviewScene => &[("sceneName", true)],
        K::CurrentSceneTransition => &[("transitionName", true)],
        K::TBarPosition | K::CurrentSceneTransitionDuration => &[],
        _ => return None,
    };
    for (key, required) in fields {
        if let Some(value) = source.get(*key) {
            value.as_str()?;
            data.insert((*key).into(), value.clone());
        } else if *required {
            return None;
        }
    }
    let mut text = |key: &str, required: bool, nonempty: bool| -> Option<()> {
        if let Some(value) = source.get(key) {
            let value = value.as_str()?;
            if nonempty && value.is_empty() {
                return None;
            }
            data.insert(key.into(), json!(value));
        } else if required {
            return None;
        }
        Some(())
    };
    match kind {
        K::DuplicateSceneItem => {
            text("destinationSceneName", false, false)?;
            text("destinationSceneUuid", false, false)?;
        }
        K::SceneName => text("newSceneName", true, true)?,
        K::InputName => text("newInputName", true, false)?,
        K::SourceFilterName => text("newFilterName", true, false)?,
        _ => {}
    }
    if matches!(
        kind,
        K::RemoveSceneItem | K::DuplicateSceneItem | K::SceneItemEnabled
    ) {
        let value = source.get("sceneItemId")?;
        let n = value.as_f64()?;
        if !n.is_finite() || !(0.0..=9_007_199_254_740_991.0).contains(&n) || n.fract() != 0.0 {
            return None;
        }
        data.insert("sceneItemId".into(), value.clone());
    }
    match kind {
        K::MediaInputCursor | K::MediaInputCursorOffset => {
            let key = if kind == K::MediaInputCursor {
                "mediaCursor"
            } else {
                "mediaCursorOffset"
            };
            let value = source.get(key)?;
            let number = value.as_f64()?;
            if !number.is_finite() || (kind == K::MediaInputCursor && number < 0.0) {
                return None;
            }
            data.insert(key.into(), value.clone());
        }
        K::MediaInputAction => {
            let action = source.get("mediaAction")?.as_str()?;
            if !matches!(
                action,
                "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_PLAY"
                    | "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_PAUSE"
                    | "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_STOP"
                    | "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_RESTART"
                    | "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_NEXT"
                    | "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_PREVIOUS"
            ) {
                return None;
            }
            data.insert("mediaAction".into(), json!(action));
        }
        K::InputAudioTracks => {
            let tracks = source.get("inputAudioTracks")?.as_object()?;
            if tracks.len() != 6
                || !(1..=6).all(|index| {
                    tracks
                        .get(&index.to_string())
                        .is_some_and(Value::is_boolean)
                })
            {
                return None;
            }
            data.insert("inputAudioTracks".into(), Value::Object(tracks.clone()));
        }
        K::InputVolume => {
            for (key, low, high) in [
                ("inputVolumeMul", 0.0, 20.0),
                ("inputVolumeDb", -100.0, 26.0),
            ] {
                if let Some(value) = source.get(key) {
                    let number = value.as_f64()?;
                    if !number.is_finite() || !(low..=high).contains(&number) {
                        return None;
                    }
                    data.insert(key.into(), value.clone());
                }
            }
        }
        K::SourceFilterIndex | K::CurrentSceneTransitionDuration | K::TBarPosition => {
            let key = match kind {
                K::SourceFilterIndex => "filterIndex",
                K::CurrentSceneTransitionDuration => "duration",
                _ => "position",
            };
            let value = source.get(key)?;
            let n = value.as_f64()?;
            if !n.is_finite() || n < 0.0 || (kind == K::TBarPosition && n > 1.0) {
                return None;
            }
            data.insert(key.into(), value.clone());
            if kind == K::TBarPosition {
                let release = source
                    .get("release")
                    .map(Value::as_bool)
                    .unwrap_or(Some(true))?;
                data.insert("release".into(), json!(release));
            }
        }
        K::CreateSceneItem | K::SceneItemEnabled => {
            let enabled = if kind == K::CreateSceneItem {
                source
                    .get("enabled")
                    .map(Value::as_bool)
                    .unwrap_or(Some(true))?
            } else {
                source.get("enabled")?.as_bool()?
            };
            data.insert("enabled".into(), json!(enabled));
        }
        K::SceneSceneTransitionOverride => {
            if let Some(value) = source.get("transitionName") {
                if !value.is_null() && !value.is_string() {
                    return None;
                }
                data.insert("transitionName".into(), value.clone());
            }
            if let Some(value) = source.get("transitionDuration") {
                if !value.is_null() {
                    let n = value.as_f64()?;
                    if !n.is_finite() || !(50.0..=20000.0).contains(&n) || n.fract() != 0.0 {
                        return None;
                    }
                }
                data.insert("transitionDuration".into(), value.clone());
            }
        }
        _ => {}
    }
    Call {
        kind,
        title,
        name,
        arguments: Value::Object(data),
        follow_up: None,
    }
    .source_numbers()
}
fn js_truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value.as_f64().is_some_and(|number| number != 0.0),
        Some(Value::String(value)) => !value.is_empty(),
        Some(Value::Array(_) | Value::Object(_)) => true,
    }
}
fn js_string(value: Option<&Value>) -> Result<String, ()> {
    Ok(match value {
        None => "undefined".into(),
        Some(Value::Null) => "null".into(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Number(value)) => number(value.as_f64().ok_or(())?),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Array(values)) => {
            let mut strings = Vec::with_capacity(values.len());
            for value in values {
                strings.push(if value.is_null() {
                    String::new()
                } else {
                    js_string(Some(value))?
                });
            }
            strings.join(",")
        }
        Some(Value::Object(value)) => {
            if value.contains_key("toString") {
                return Err(());
            }
            "[object Object]".into()
        }
    })
}
fn output_schema(name: &str) -> Value {
    static CATALOG: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
    let catalog = CATALOG.get_or_init(|| {
        serde_json::from_str(include_str!("../assets/tools.json")).expect("checked tool catalog")
    });
    catalog["tools"]
        .as_array()
        .and_then(|tools| tools.iter().find(|tool| tool["name"] == name))
        .map(|tool| tool["outputSchema"].clone())
        .unwrap_or(Value::Null)
}
