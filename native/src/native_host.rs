// SPDX-License-Identifier: MIT
//! One server lifetime, independent of any transport or client wire id.
//! This owner adds no command queue and never restamps Service admission.
use super::*;
use serde_json::{Value, json};
use std::{collections::HashMap, io, sync::atomic::Ordering};
use tokio::task::JoinSet;

/// Trusted protocol admission plus the original frame and per-exchange facts.
pub(super) struct Incoming<'a> {
    pub(super) reply: OwnedReplyContext,
    pub(super) name: String,
    pub(super) args: Value,
    pub(super) resource: bool,
    pub(super) controller: bool,
    pub(super) value: &'a Value,
    pub(super) frame: &'a [u8],
    pub(super) legacy_name: Option<&'a str>,
    pub(super) legacy_capabilities: Option<&'a Value>,
}
/// Only Host::authorize can construct this permit. It contains the original
/// prepared command, not a reconstructed resume frame or effect capability.
pub(super) struct Invocation(PreparedCall);
pub(super) type Authorization = Result<Invocation, Value>;
impl Invocation {
    pub(super) fn is_controller(&self) -> bool {
        self.0.controller
    }
    pub(super) fn into_controller(self) -> (OwnedReplyContext, String, Value) {
        assert!(self.0.controller, "controller permit");
        (self.0.reply, self.0.name, self.0.args)
    }
}
pub(super) struct Finished {
    pub(super) frontend: FrontendId,
    pub(super) response: Option<Value>,
}
/// The transport owns this guard, independently of the server's in-flight
/// dispatch borrow. Dropping a response can signal departure during lazy connect.
pub(super) struct Frontend {
    id: FrontendId,
    departed: Cancellation,
}
impl Frontend {
    pub(super) fn id(&self) -> FrontendId {
        self.id
    }
    pub(super) fn departure(&self) -> Cancellation {
        self.departed.clone()
    }
}
impl Drop for Frontend {
    fn drop(&mut self) {
        self.departed.cancel();
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct FrontendId(u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct ExchangeId(pub(super) u64);
impl FrontendId {
    pub(super) fn listener_key(self) -> Value {
        json!(format!("http-listener/{:016x}", self.0))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Refusal {
    Capacity,
    Closed,
    Identity,
    Duplicate,
}
#[derive(Clone, Copy)]
pub(super) enum Class {
    Ordinary,
    TakeStop,
    SafetyStop,
}
impl Class {
    pub(super) fn for_tool(name: &str, resource: bool) -> Self {
        if resource {
            return Self::Ordinary;
        }
        match name {
            "obs-take-stop" => Self::TakeStop,
            "obs-stop-stream"
            | "obs-stop-virtual-cam"
            | "obs-stop-replay-buffer"
            | "obs-stop-record" => Self::SafetyStop,
            _ => Self::Ordinary,
        }
    }
}
struct Entry {
    frontend: FrontendId,
    wire: String,
    pending: Pending,
    departed: Cancellation,
}
/// Retained work owns capacity until explicit retirement after join/publication.
/// Cancellation and frontend departure suppress delivery without retiring work.
pub(super) struct Admissions {
    next_frontend: Option<u64>,
    next_exchange: Option<u64>,
    frontends: HashMap<FrontendId, Cancellation>,
    entries: HashMap<ExchangeId, Entry>,
    wire: HashMap<(FrontendId, String), ExchangeId>,
    closing: bool,
    ingress_take: Option<FrontendId>,
    ingress_safety: Option<FrontendId>,
}
impl Admissions {
    fn new() -> Self {
        Self {
            next_frontend: Some(0),
            next_exchange: Some(0),
            frontends: HashMap::new(),
            entries: HashMap::new(),
            wire: HashMap::new(),
            closing: false,
            ingress_take: None,
            ingress_safety: None,
        }
    }
    #[cfg(test)]
    pub(super) fn for_test() -> Self {
        Self::new()
    }
    pub(super) fn frontend(&mut self) -> Result<FrontendId, Refusal> {
        self.frontend_for(Class::Ordinary)
    }
    pub(super) fn frontend_for(&mut self, class: Class) -> Result<FrontendId, Refusal> {
        if self.closing {
            return Err(Refusal::Closed);
        }
        // Short HTTP exchanges can end without an admitted job. Reap only
        // their detached registrations; retained entries still own their slot.
        let entries = &self.entries;
        self.frontends.retain(|id, departed| {
            !departed.is_cancelled() || entries.values().any(|entry| entry.frontend == *id)
        });
        if self
            .ingress_take
            .is_some_and(|id| !self.frontends.contains_key(&id))
        {
            self.ingress_take = None;
        }
        if self
            .ingress_safety
            .is_some_and(|id| !self.frontends.contains_key(&id))
        {
            self.ingress_safety = None;
        }
        let full = self.frontends.len() >= 128;
        if full
            && !matches!(class,Class::TakeStop if self.ingress_take.is_none())
            && !matches!(class,Class::SafetyStop if self.ingress_safety.is_none())
        {
            return Err(Refusal::Capacity);
        }
        let id = FrontendId(self.next_frontend.ok_or(Refusal::Identity)?);
        if full {
            match class {
                Class::TakeStop => self.ingress_take = Some(id),
                Class::SafetyStop => self.ingress_safety = Some(id),
                Class::Ordinary => unreachable!("checked ingress class"),
            }
        }
        self.next_frontend = id.0.checked_add(1);
        self.frontends.insert(id, Cancellation::new());
        Ok(id)
    }
    pub(super) fn ordinary(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| !entry.pending.reserved_stop && !entry.pending.reserved_safety_stop)
            .count()
    }
    pub(super) fn check(
        &self,
        frontend: FrontendId,
        wire: &Value,
        class: Class,
    ) -> Result<(bool, bool), Refusal> {
        if self.closing
            || self
                .frontends
                .get(&frontend)
                .is_none_or(Cancellation::is_cancelled)
        {
            return Err(Refusal::Closed);
        }
        if self.next_exchange.is_none() {
            return Err(Refusal::Identity);
        }
        if self.wire.contains_key(&(frontend, wire.to_string())) {
            return Err(Refusal::Duplicate);
        }
        if self.ordinary() < ORDINARY_FRONTEND_JOBS {
            return Ok((false, false));
        }
        match class {
            Class::TakeStop
                if !self
                    .entries
                    .values()
                    .any(|entry| entry.pending.reserved_stop) =>
            {
                Ok((true, false))
            }
            Class::SafetyStop
                if !self
                    .entries
                    .values()
                    .any(|entry| entry.pending.reserved_safety_stop) =>
            {
                Ok((false, true))
            }
            _ => Err(Refusal::Capacity),
        }
    }
    pub(super) fn admit(
        &mut self,
        frontend: FrontendId,
        wire: &Value,
        class: Class,
        mut pending: Pending,
    ) -> Result<ExchangeId, Refusal> {
        let (reserved_stop, reserved_safety_stop) = self.check(frontend, wire, class)?;
        let id = ExchangeId(self.next_exchange.ok_or(Refusal::Identity)?);
        self.next_exchange = id.0.checked_add(1);
        pending.reserved_stop = reserved_stop;
        pending.reserved_safety_stop = reserved_safety_stop;
        let wire = wire.to_string();
        self.wire.insert((frontend, wire.clone()), id);
        self.entries.insert(
            id,
            Entry {
                frontend,
                wire,
                pending,
                departed: self
                    .frontends
                    .get(&frontend)
                    .expect("checked frontend")
                    .clone(),
            },
        );
        Ok(id)
    }
    fn cancel_pending(pending: &Pending) {
        pending.frontend_cancelled.store(true, Ordering::Release);
        pending.cancel.cancel();
        if let Some(media) = &pending.media {
            media.cancel();
        }
    }
    pub(super) fn cancel(&self, frontend: FrontendId, wire: &Value) {
        if let Some(id) = self.wire.get(&(frontend, wire.to_string()))
            && let Some(entry) = self.entries.get(id)
        {
            Self::cancel_pending(&entry.pending);
        }
    }
    pub(super) fn disconnect(&mut self, frontend: FrontendId) {
        if let Some(departed) = self.frontends.remove(&frontend) {
            departed.cancel();
        }
        for entry in self
            .entries
            .values()
            .filter(|entry| entry.frontend == frontend)
        {
            Self::cancel_pending(&entry.pending);
        }
    }
    pub(super) fn retire(&mut self, id: ExchangeId) -> Option<Pending> {
        let entry = self.entries.remove(&id)?;
        self.wire.remove(&(entry.frontend, entry.wire));
        Some(entry.pending)
    }
    fn close(&mut self) {
        self.closing = true;
        for departed in self.frontends.values() {
            departed.cancel();
        }
        self.frontends.clear();
        for entry in self.entries.values() {
            Self::cancel_pending(&entry.pending);
        }
    }
    /// Only internal continuation duplicate/cancel keys use this scope. The
    /// actual reply id and opaque modern token retain their existing wire form.
    pub(super) fn continuation_key(frontend: FrontendId, id: &Value) -> Value {
        json!([frontend.0, id])
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ResourceScope {
    Session,
    Exchange,
    Frontend(FrontendId),
}
struct InitializationBudget {
    ordinary: Arc<tokio::sync::Semaphore>,
    take: Arc<tokio::sync::Semaphore>,
    safety: Arc<tokio::sync::Semaphore>,
    take_slot: Arc<tokio::sync::Semaphore>,
    safety_slot: Arc<tokio::sync::Semaphore>,
}
struct InitializationPermit {
    _bytes: tokio::sync::OwnedSemaphorePermit,
    _slot: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl InitializationBudget {
    fn new() -> Self {
        Self {
            ordinary: Arc::new(tokio::sync::Semaphore::new(32 * 1024 * 1024)),
            take: Arc::new(tokio::sync::Semaphore::new(64 * 1024)),
            safety: Arc::new(tokio::sync::Semaphore::new(64 * 1024)),
            take_slot: Arc::new(tokio::sync::Semaphore::new(1)),
            safety_slot: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }
    fn reserve(&self, class: Class, bytes: usize) -> Result<InitializationPermit, ()> {
        let n = u32::try_from(bytes).map_err(|_| ())?;
        if let Ok(permit) = self.ordinary.clone().try_acquire_many_owned(n) {
            return Ok(InitializationPermit {
                _bytes: permit,
                _slot: None,
            });
        }
        if bytes > 64 * 1024 {
            return Err(());
        }
        let (pool, slot) = match class {
            Class::TakeStop => (&self.take, &self.take_slot),
            Class::SafetyStop => (&self.safety, &self.safety_slot),
            Class::Ordinary => return Err(()),
        };
        let slot = slot.clone().try_acquire_owned().map_err(|_| ())?;
        let permit = pool.clone().try_acquire_many_owned(n).map_err(|_| ())?;
        Ok(InitializationPermit {
            _bytes: permit,
            _slot: Some(slot),
        })
    }
}
pub(super) struct Host {
    pub(super) runtime: Runtime,
    pub(super) confirmations: confirmation::Continuations<PreparedCall>,
    pub(super) admissions: Admissions,
    pub(super) jobs: JoinSet<Completed>,
    retain_legacy_session: bool,
    initialization_bytes: InitializationBudget,
}
impl Host {
    pub(super) fn from_runtime(runtime: Runtime, retain_legacy_session: bool) -> io::Result<Self> {
        let confirmations = if runtime.confirm_live {
            confirmation::Continuations::new()
                .map_err(|_| io::Error::other("Native confirmation identity is unavailable"))?
        } else {
            confirmation::Continuations::disabled()
        };
        Ok(Self {
            runtime,
            confirmations,
            admissions: Admissions::new(),
            jobs: JoinSet::new(),
            retain_legacy_session,
            initialization_bytes: InitializationBudget::new(),
        })
    }
    pub(super) fn frontend(&mut self) -> Result<Frontend, Refusal> {
        let id = self.admissions.frontend()?;
        Ok(Frontend {
            id,
            departed: self
                .admissions
                .frontends
                .get(&id)
                .expect("registered frontend")
                .clone(),
        })
    }
    pub(super) fn frontend_for(&mut self, class: Class) -> Result<Frontend, Refusal> {
        let id = self.admissions.frontend_for(class)?;
        Ok(Frontend {
            id,
            departed: self
                .admissions
                .frontends
                .get(&id)
                .expect("registered frontend")
                .clone(),
        })
    }
    /// Both adapters prepare once from the original admitted frame. The typed
    /// ordered Apply/Capture commands survive confirmation and later dispatch.
    pub(super) fn prepare(
        &self,
        frontend: FrontendId,
        incoming: Incoming<'_>,
    ) -> Result<PreparedCall, Value> {
        let Incoming {
            reply,
            name,
            args,
            resource,
            controller,
            value,
            frame,
            legacy_name,
            legacy_capabilities,
        } = incoming;
        // HTTP initialization does not retain a client's capability offer.
        let legacy_capabilities = if self.retain_legacy_session {
            legacy_capabilities
        } else {
            None
        };
        if !resource
            && !controller
            && !owned(
                &name,
                self.runtime.host.is_some(),
                self.runtime.media.is_some(),
                self.runtime.profile,
            )
        {
            return Err(reply.error(
                -32601,
                "Tool is unavailable in the native-only qualification lane",
            ));
        }
        let read = read_definition(&name);
        let apply_request = if !resource && name == "obs-apply-scene" {
            schema::apply_scene_request(
                value.pointer("/params/arguments").unwrap_or(&json!({})),
                frame,
            )
        } else {
            None
        };
        let capture_request = if !resource && name == "obs-capture-window" {
            schema::capture_window_request(value.pointer("/params/arguments").unwrap_or(&json!({})))
        } else {
            None
        };
        let explicit_confirm = if self.runtime.confirm_live && confirmation::eligible(&name) {
            value
                .pointer("/params/arguments/confirm")
                .and_then(Value::as_bool)
        } else {
            None
        };
        let confirm_valid = !(self.runtime.confirm_live && confirmation::eligible(&name))
            || value
                .pointer("/params/arguments/confirm")
                .is_none_or(Value::is_boolean);
        let args = if !confirm_valid {
            None
        } else if name == "obs-apply-scene" {
            apply_request
                .as_ref()
                .map(obs_mcp_native::apply_scene::Request::arguments)
        } else if name == "obs-capture-window" {
            capture_request
                .as_ref()
                .map(obs_mcp_native::capture_window::Request::arguments)
        } else if resource {
            Some(args)
        } else if controller {
            schema::dynamic_arguments(&name, &args)
        } else if name == "obs-record-clip" {
            validated_clip_arguments(args)
        } else if read.is_some() {
            schema::native_request(&name, &args)
                .ok()
                .flatten()
                .map(|(_, data)| data)
        } else if name == "obs-describe-request" {
            schema::validated_local_arguments(&name, "describe-request", &args)
        } else {
            schema::service_arguments(&name, &args)
        };
        let Some(args) = args else {
            let tool = schema::tool(&name).unwrap();
            let validation = if name == "obs-set-input-audio-tracks" {
                obs_mcp_native::tool_validation::tool_error_from_frame(
                    &name,
                    &tool["inputSchema"],
                    value.pointer("/params/arguments").unwrap_or(&json!({})),
                    frame,
                )
            } else if self.runtime.confirm_live && confirmation::eligible(&name) {
                obs_mcp_native::tool_validation::confirmation_error(
                    &name,
                    value.pointer("/params/arguments").unwrap_or(&json!({})),
                )
            } else {
                obs_mcp_native::tool_validation::tool_error(
                    &name,
                    &tool["inputSchema"],
                    value.pointer("/params/arguments").unwrap_or(&json!({})),
                )
            };
            match validation {
                Ok(result) => return Err(reply.complete(result, false)),
                Err(_) => {
                    return Err(rpc_error(
                        value["id"].clone(),
                        -32602,
                        "Native argument validation projection unavailable; no dispatch or SDK fallback",
                    ));
                }
            }
        };
        let envelope = value.pointer("/params/_meta");
        let caller = caller_name(CallerContext {
            envelope,
            legacy_name,
            ..Default::default()
        });
        let binding = CallerBinding::resolve(
            CallerContext {
                envelope,
                legacy_name,
                ..Default::default()
            },
            None,
            false,
            None,
            None,
        );
        let can_elicit = client_can_elicit(envelope, legacy_capabilities);
        let form_capable = confirmation::supports_form(
            envelope
                .and_then(|meta| meta.get("io.modelcontextprotocol/clientCapabilities"))
                .or(legacy_capabilities),
        );
        let consent_binding = if self.runtime.confirm_live && confirmation::eligible(&name) {
            match confirmation::binding(
                &name,
                value.pointer("/params/arguments").unwrap_or(&json!({})),
                envelope.unwrap_or(&Value::Null),
                &caller,
                u8::from(self.runtime.host.is_some())
                    | u8::from(self.runtime.media.is_some()) << 1
                    | u8::from(self.runtime.profile == NativeProfile::Advanced) << 2,
            ) {
                Ok(binding) => binding,
                Err(_) => {
                    return Err(
                        reply.error(-32602, "Native confirmation binding capacity exceeded")
                    );
                }
            }
        } else {
            [0; 32]
        };
        let confirmed_command = if self.runtime.confirm_live {
            ConfirmedCommand::parse(&name, &args)
        } else {
            None
        };
        Ok(PreparedCall {
            frontend,
            reply,
            name,
            args,
            resource,
            controller,
            read,
            binding,
            caller,
            apply_request,
            capture_request,
            confirmed_command,
            consent_binding,
            explicit_confirm,
            can_elicit,
            form_capable,
        })
    }
    pub(super) fn has_frontend_jobs(&self, frontend: FrontendId) -> bool {
        self.admissions
            .entries
            .values()
            .any(|entry| entry.frontend == frontend)
    }
    /// Hand publication lifetime to a response sink without cancelling work.
    /// A completed exchange cannot submit another call under its old scope.
    pub(super) fn retire_frontend(&mut self, frontend: FrontendId) -> Result<(), Refusal> {
        if self
            .admissions
            .entries
            .values()
            .any(|entry| entry.frontend == frontend)
        {
            return Err(Refusal::Capacity);
        }
        self.admissions.frontends.remove(&frontend);
        Ok(())
    }
    pub(super) fn resume(
        &mut self,
        frontend: FrontendId,
        reply: OwnedReplyContext,
        name: &str,
        value: &Value,
        legacy_name: Option<&str>,
    ) -> Result<(PreparedCall, Value), Value> {
        if reply.legacy() {
            return Err(reply.error(-32602, "Invalid or expired requestState"));
        }
        let envelope = value.pointer("/params/_meta");
        let caller = caller_name(CallerContext {
            envelope,
            legacy_name,
            ..Default::default()
        });
        let digest = confirmation::binding(
            name,
            value.pointer("/params/arguments").unwrap_or(&json!({})),
            envelope.unwrap_or(&Value::Null),
            &caller,
            u8::from(self.runtime.host.is_some())
                | u8::from(self.runtime.media.is_some()) << 1
                | u8::from(self.runtime.profile == NativeProfile::Advanced) << 2,
        )
        .map_err(|_| reply.error(-32602, "Native confirmation binding capacity exceeded"))?;
        let state = value
            .pointer("/params/requestState")
            .and_then(Value::as_str)
            .ok_or_else(|| reply.error(-32602, "Invalid or expired requestState"))?;
        let response = value
            .pointer("/params/inputResponses")
            .filter(|r| {
                r.is_object()
                    && !matches!(
                        obs_mcp_native::control::ConfirmationAnswer::from_responses(Some(r)),
                        obs_mcp_native::control::ConfirmationAnswer::Missing
                    )
            })
            .ok_or_else(|| reply.error(-32602, "Invalid confirmation response"))?;
        match self
            .confirmations
            .take_modern(state, digest, tokio::time::Instant::now())
        {
            Ok(mut original) => {
                original.frontend = frontend;
                original.reply = reply;
                Ok((original, response.clone()))
            }
            Err(_) => Err(reply.error(-32602, "Invalid or expired requestState")),
        }
    }
    pub(super) fn authorize(
        &mut self,
        prepared: PreparedCall,
        answers: Option<&Value>,
    ) -> Authorization {
        let frontend = prepared.frontend;
        if self
            .admissions
            .check(
                frontend,
                prepared.reply.id(),
                Class::for_tool(&prepared.name, prepared.resource),
            )
            .is_err()
        {
            return Authorization::Err(prepared.reply.error(
                -32000,
                "Native request admission unavailable; live state retained",
            ));
        }
        let read_only = prepared.resource
            || schema::tool(&prepared.name)
                .is_some_and(|tool| tool["annotations"]["readOnlyHint"] == true);
        let gate_args = prepared.explicit_confirm.map(|explicit| {
            let mut args = prepared.args.clone();
            args["confirm"] = json!(explicit);
            args
        });
        let decision = self.runtime.control.lock().unwrap().evaluate(
            GateRequest {
                tool_name: &prepared.name,
                arguments: gate_args.as_ref().unwrap_or(&prepared.args),
                read_only_hint: read_only,
                caller: &prepared.caller,
                confirmation: ConfirmationContext {
                    enabled: self.runtime.confirm_live,
                    can_elicit: prepared.can_elicit
                        && (self.retain_legacy_session || !prepared.reply.legacy()),
                    input_responses: answers,
                },
            },
            (self.runtime.clock)().epoch_ms,
        );
        if let Disposition::InputRequired { question } = decision {
            if !prepared.form_capable {
                return Authorization::Err(if prepared.reply.legacy() {
                    prepared.reply.complete(error("Cannot request input 'confirm' (elicitation/create): the client on this 2025-era connection did not declare the required capability"),false)
                } else {
                    prepared.reply.error_data(-32021,"Cannot request input 'confirm' (elicitation/create): the request's client capabilities do not declare the required capability",json!({"requiredCapabilities":{"elicitation":{"form":{}}}}))
                });
            }
            let reply = prepared.reply.clone();
            let Some(bytes) = prepared.retained_bytes() else {
                return Authorization::Err(
                    reply.error(-32000, "Native confirmation capacity exceeded"),
                );
            };
            let legacy = reply.legacy();
            let binding = prepared.consent_binding;
            let response = match self.confirmations.issue(
                Admissions::continuation_key(frontend, reply.id()),
                binding,
                prepared,
                bytes,
                legacy,
                tokio::time::Instant::now(),
            ) {
                Ok(confirmation::Challenge::Modern(token)) => {
                    let mut required = confirmation::required(question);
                    required["requestState"] = json!(token);
                    reply.input_required(required)
                }
                Ok(confirmation::Challenge::Legacy(id)) => {
                    confirmation::legacy_question(id, question)
                }
                Err((_reason, _call)) => reply.error(
                    -32000,
                    "Native confirmation capacity or identity unavailable; nothing was changed",
                ),
            };
            return Authorization::Err(response);
        }
        if let Some(result) = decision.tool_result() {
            return Authorization::Err(prepared.reply.complete(ensure_structured(result), false));
        }
        Authorization::Ok(Invocation(prepared))
    }
    pub(super) fn complete(&mut self, completed: Completed) -> Option<Finished> {
        let (id, reply, result, cancelled, cacheable) = completed;
        let entry = self.admissions.entries.get(&id)?;
        let frontend = entry.frontend;
        let departed = entry.departed.is_cancelled();
        self.admissions.retire(id)?;
        let response = if departed || cancelled.load(Ordering::Acquire) {
            None
        } else {
            Some(match result {
                Ok(result) => reply.complete(result, cacheable),
                Err(reason) => reply.error(reason.code, &reason.message),
            })
        };
        Some(Finished { frontend, response })
    }
    /// No transport queue is introduced here: original ServiceHandle admission
    /// and operation budgets start at the same boundary as the stdio owner.
    pub(super) async fn dispatch(&mut self, invocation: Invocation) -> io::Result<Option<Value>> {
        let admitted_at = tokio::time::Instant::now();
        let prepared = invocation.0;
        let initialization_charge = if self.runtime.owner.is_none() || self.runtime.initializing() {
            prepared.initialization_bytes()
        } else {
            None
        };
        let frontend = prepared.frontend;
        if self
            .admissions
            .check(
                frontend,
                prepared.reply.id(),
                Class::for_tool(&prepared.name, prepared.resource),
            )
            .is_err()
        {
            return Ok(Some(prepared.reply.error(
                -32000,
                "Native request admission unavailable; live state retained",
            )));
        }
        let departed = self
            .admissions
            .frontends
            .get(&frontend)
            .expect("checked frontend")
            .clone();
        let control = self.runtime.control.clone();
        let clock = self.runtime.clock.clone();
        let PreparedCall {
            reply,
            name,
            args,
            resource,
            read,
            binding,
            caller,
            apply_request,
            capture_request,
            confirmed_command,
            ..
        } = prepared;
        // Volume's missing-value callback can finish without OBS.
        // Other literal callbacks have no early business refusal, so
        // avoid re-cloning/counting their validated opaque arguments.
        if !resource
            && (name == "obs-set-input-volume" || obs_mcp_native::opaque_commands::is_name(&name))
            && let Some(result) = obs_mcp_native::generic_requests::Call::parse(&name, &args)
                .and_then(|call| call.business_refusal())
        {
            return Ok(Some(reply.complete(result, false)));
        }
        // Source performs late-chapter business validation before any
        // connection/preflight, after the common caller-control policy.
        if name == "obs-record-clip"
            && let Some(result) = record_clip::Request::parse(&args)
                .expect("validated clip")
                .refusal()
        {
            return Ok(Some(reply.complete(result, false)));
        }
        // Source's selector business refusal follows caller policy and
        // precedes lazy physical connection; platform checks need OBS metadata.
        if let Some(ref request) = capture_request
            && let Some(refusal) = request.selector_refusal()
        {
            return Ok(Some(reply.complete(refusal.result(), false)));
        }
        if name == "obs-stage-scene"
            && let Err(reason) = self.runtime.screenshot_limit.as_ref()
        {
            return Ok(Some(reply.complete(error(reason.message.clone()), false)));
        }
        if name == "obs-snapshot" && args["list"] == true && self.runtime.owner.is_none() {
            let Some(mut owners) = self.runtime.preparation.take() else {
                return Ok(Some(
                    reply.complete(service_error(ServiceError::Unsupported), false),
                ));
            };
            let endpoint = self.runtime.url.clone();
            let (owners, result) = match tokio::task::spawn_blocking(move || {
                let result = owners.snapshot_list(&endpoint);
                (owners, result)
            })
            .await
            {
                Ok(result) => result,
                Err(_) => return Err(io::Error::other("Snapshot listing worker failed to join")),
            };
            self.runtime.preparation = Some(owners);
            return Ok(Some(reply.complete(result, false)));
        }
        if name == "obs-restore" && self.runtime.owner.is_none() {
            let Some(mut owners) = self.runtime.preparation.take() else {
                return Ok(Some(
                    reply.complete(service_error(ServiceError::Unsupported), false),
                ));
            };
            let endpoint = self.runtime.url.clone();
            let request = restore_workflow::Request::parse(&args).expect("validated restore");
            // Borrow the SAME offline file owner, after caller policy.
            // A valid selection grants no authority; Service reselects
            // and checks policy again after lazy physical connection.
            let (owners, refusal) = match tokio::task::spawn_blocking(move || {
                let refusal = owners.local_restore_refusal(&endpoint, &request);
                (owners, refusal)
            })
            .await
            {
                Ok(result) => result,
                Err(_) => return Err(io::Error::other("Restore lookup worker failed to join")),
            };
            self.runtime.preparation = Some(owners);
            if let Some(result) = refusal {
                return Ok(Some(reply.complete(result, false)));
            }
        }
        let sleep = if name == "obs-sleep" {
            ReadToolRequest::parse(&name, &args).ok()
        } else {
            None
        };
        let local_sleep = matches!(
            &sleep,
            Some(ReadToolRequest::Sleep { frames: None, .. })
                | Some(ReadToolRequest::Sleep {
                    millis: Some(_),
                    frames: Some(_)
                })
        );
        let status = name == "obs-get-status";
        let local = status
            || local_sleep
            || matches!(
                name.as_str(),
                "obs-describe-request"
                    | "obs-claim-control"
                    | "obs-release-control"
                    | "obs-control-status"
            );
        if name == "obs-recover-demo"
            && self.runtime.owner.is_none()
            && let Some(result) = self.runtime.preparation.as_mut().and_then(|owners| {
                owners.local_recovery_refusal(
                    &self.runtime.url,
                    args["operationId"].as_str().unwrap(),
                )
            })
        {
            return Ok(Some(reply.complete(result, false)));
        }
        if departed.is_cancelled() {
            return Ok(None);
        }
        let initializing_remote =
            !local && (self.runtime.owner.is_none() || self.runtime.initializing());
        let initialization_permit = if initializing_remote {
            let Some(bytes) = initialization_charge else {
                return Ok(Some(reply.error(-32000,"Native initialization retention capacity unavailable; no OBS effect was dispatched")));
            };
            match self.initialization_bytes.reserve(Class::for_tool(&name,resource),bytes){Ok(permit)=>Some(permit),Err(())=>return Ok(Some(reply.error(-32000,"Native initialization retention capacity unavailable; no OBS effect was dispatched")))}
        } else {
            None
        };
        let service = if status {
            ServiceSource::Ready(Ok(self.runtime.owner.as_ref().map(Service::handle)))
        } else if local {
            ServiceSource::Ready(Ok(None))
        } else {
            self.runtime.service_source(matches!(
                name.as_str(),
                "obs-snapshot"
                    | "obs-restore"
                    | "obs-apply-scene"
                    | "obs-capture-window"
                    | "obs-stage-scene"
                    | "obs-take-scene"
                    | "obs-record-clip"
                    | "obs-take-start"
                    | "obs-take-stop"
                    | "obs-take-mark"
                    | "obs-take-status"
                    | "obs-prepare-demo"
                    | "obs-recover-demo"
                    | "obs-contact-sheet"
                    | "obs-trim-take"
                    | "obs-save-replay-verified"
            ))
        };
        let native_profile = self.runtime.profile;
        let descriptor = ConnectionDescriptor {
            url: self.runtime.url.clone(),
            has_password: self.runtime.password.is_some(),
        };
        let screenshot_limit = self.runtime.screenshot_limit.clone();
        let cancel = Cancellation::new();
        let media_cancel = matches!(name.as_str(), "obs-contact-sheet" | "obs-trim-take")
            .then(MediaCancellation::default);
        let cancelled = Arc::new(AtomicBool::new(false));
        let key = match self.admissions.admit(
            frontend,
            reply.id(),
            Class::for_tool(&name, resource),
            Pending {
                cancel: cancel.clone(),
                frontend_cancelled: cancelled.clone(),
                media: media_cancel.clone(),
                reserved_stop: false,
                reserved_safety_stop: false,
            },
        ) {
            Ok(id) => id,
            Err(Refusal::Closed) => return Ok(None),
            Err(_) => {
                return Err(io::Error::other(
                    "Native retained admission changed unexpectedly",
                ));
            }
        };
        let worker_cancel = cancel.clone();
        let worker_media = media_cancel.clone();
        let replay_deadline = (name == "obs-save-replay-verified").then(|| {
            admitted_at
                + obs_mcp_native::replay_save::Request::parse(&args)
                    .expect("validated replay arguments")
                    .timeout
        });
        self.jobs.spawn(async move {
            let work=async move {
                    let _initialization_permit=initialization_permit;
                    let resolved=if let Some(deadline)=replay_deadline{tokio::time::timeout_at(deadline,service.resolve(cancel.clone())).await.unwrap_or_else(|_|{cancel.cancel();Err(error("Replay original operation deadline expired during initialization; no save was dispatched"))})}else{service.resolve(cancel.clone()).await};
                    let service=match resolved {Ok(handle)=>handle,Err(value)=>return (key,reply,Ok(value),cancelled,false)};
                    if name=="native-take-resource" {
                        let result=service.as_ref().unwrap().take_resource().await.map(|summary|json!({"contents":[{"uri":"obs://take/current","mimeType":"application/json","text":serde_json::to_string_pretty(&summary).unwrap()}]})).map_err(|_|ResourceFailure{code:-32603,message:"Native current-take resource is unavailable".into()});
                        return (key,reply,result,cancelled,true);
                    }
                    if name=="native-resource-list"||name=="native-resource-completion" {
                        let result=match service.as_ref().unwrap().read(ReadRequest::SceneList,Value::Null,Duration::from_secs(10)).await {
                            Ok(scenes)=>if name=="native-resource-list"{crate::resource_catalog::listing(&scenes)}else{crate::resource_catalog::complete_scenes(&scenes,args["prefix"].as_str().unwrap())}.ok_or_else(||ResourceFailure{code:-32603,message:"Native resource inventory limit exceeded; live state retained".into()}),
                            Err(reason)=>Err(ResourceFailure {code:reason.request_code().and_then(|code|i64::try_from(code).ok()).unwrap_or(-32603),message:reason.message}),
                        };
                        return (key,reply,result,cancelled,name=="native-resource-list");
                    }
                    if name=="native-resource" {let result=read_resource(service.as_ref().unwrap(),args["uri"].as_str().unwrap(),screenshot_limit).await;return (key,reply,result,cancelled,true);}
                    let mut result=if let Some(command)=confirmed_command {command.execute(service.as_ref().unwrap(),caller,binding,cancel).await}else{match name.as_str() {
                        "obs-get-status"=>if let Some(service)=service.as_ref(){service.read_tool(ReadToolRequest::Status,screenshot_limit,cancel).await.unwrap_or_else(service_error)}else{service_read_tools::status(StatusMetadata {connection:descriptor,connected:false,identified:false},clock.clone()).unwrap_or_else(|reason|error(reason.message))},
                        "obs-sleep" if local_sleep=>match sleep.unwrap(){ReadToolRequest::Sleep{millis:Some(millis),frames:None}=>service_read_tools::sleep_millis(millis,cancel,tokio::time::Instant::now()+Duration::from_secs(60)).await,_=>service_read_tools::sleep_selection_error()},
                        "obs-apply-scene"=>service.as_ref().unwrap().apply_scene(apply_request.expect("validated original Apply request"),caller,cancel).await.unwrap_or_else(service_error),
                        "obs-capture-window"=>service.as_ref().unwrap().capture_window(capture_request.expect("validated original Capture request"),caller,cancel).await.unwrap_or_else(service_error),
                        "obs-describe-request"=>crate::protocol_catalog::describe(&args),
                        "obs-claim-control"=>control.lock().unwrap().claim(&caller,args["minutes"].as_u64().unwrap_or(30) as u16,args["reason"].as_str().unwrap_or(""),(clock)().epoch_ms).map(|result|result.tool_result()).unwrap_or_else(|_|error("Invalid control lease arguments")),
                        "obs-release-control"=>control.lock().unwrap().release(&caller,(clock)().epoch_ms).tool_result(),
                        "obs-control-status"=>control.lock().unwrap().status_result(&caller,(clock)().epoch_ms),
                        _=>dispatch(service.as_ref().unwrap(),&name,args,read,binding,caller,cancel,media_cancel,resource,screenshot_limit,admitted_at).await,
                    }};
                    if name=="obs-get-status" && native_profile==NativeProfile::Advanced {
                        result=ensure_structured(result);
                        result["structuredContent"]["nativeProfile"]=service.as_ref().map_or_else(||native_profile.metadata(),|service|service.profile_metadata());
                    }
                    if !resource {result=ensure_structured(result);}
                    if resource && !result.get("isError").is_some_and(|value|value==true) {let report=result["structuredContent"].clone();result=json!({"contents":[{"uri":"obs://status","mimeType":"application/json","text":serde_json::to_string_pretty(&report).unwrap()}]});}
                    (key,reply,Ok(result),cancelled,resource)

            };
            tokio::pin!(work);
            tokio::select! {
                biased;
                ()=departed.cancelled()=>{worker_cancel.cancel();if let Some(media)=worker_media{media.cancel();}work.await},
                result=&mut work=>result,
            }
        });
        Ok(None)
    }
    pub(super) fn has_work(&self) -> bool {
        !self.jobs.is_empty() || self.runtime.initializing()
    }
    pub(super) async fn next_completed(&mut self) -> io::Result<Option<Completed>> {
        loop {
            let initializing = self.runtime.initializing();
            if !initializing {
                return self
                    .jobs
                    .join_next()
                    .await
                    .transpose()
                    .map_err(|_| io::Error::other("Native frontend worker failed to join"));
            }
            tokio::select! {
                result=self.jobs.join_next(),if !self.jobs.is_empty()=>return result.transpose().map_err(|_|io::Error::other("Native frontend worker failed to join")),
                result=self.runtime.progress_initialization()=>result?,
            }
        }
    }
    /// HTTP legacy exchanges have no retained notification session. Reporting
    /// an exchange-local scope never flips the modern shared router's era.
    pub(super) fn resource_scope(
        &mut self,
        frontend: FrontendId,
        legacy: bool,
    ) -> Result<ResourceScope, obs_mcp_native::resource_events::Failure> {
        if self.retain_legacy_session {
            self.runtime.notifications.set_legacy(legacy)?;
            Ok(ResourceScope::Session)
        } else if legacy {
            Ok(ResourceScope::Exchange)
        } else {
            // HTTP adapters must namespace listener ids and route sinks by this
            // frontend before registering with the one observation publisher.
            Ok(ResourceScope::Frontend(frontend))
        }
    }
    /// A frontend leaving is not server shutdown and never discards Service,
    /// Take, observer, journal or peer jobs. Modern inert grants may span HTTP
    /// exchanges; an explicit scoped cancellation or expiry retires them.
    pub(super) fn disconnect(&mut self, frontend: FrontendId) {
        self.admissions.disconnect(frontend);
    }
    pub(super) async fn shutdown(&mut self) -> io::Result<()> {
        self.admissions.close();
        self.confirmations = confirmation::Continuations::disabled();
        self.runtime.notifications.close();
        let jobs = &mut self.jobs;
        let (joined, shutdown) = tokio::join!(
            async {
                let mut joined = true;
                while let Some(result) = jobs.join_next().await {
                    joined &= result.is_ok();
                }
                joined
            },
            self.runtime.shutdown()
        );
        self.admissions.entries.clear();
        self.admissions.wire.clear();
        shutdown?;
        if joined {
            Ok(())
        } else {
            Err(io::Error::other(
                "Native frontend worker did not join successfully",
            ))
        }
    }
}

#[cfg(test)]
#[path = "../tests/support/output_start_three.rs"]
pub(super) mod support;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        native_service::ResourceFailure,
        protocol::{Admission, ProtocolSession},
    };
    use futures_util::poll;
    use obs_mcp_native::{
        observation::ReadRequest, resource_events, scene_audit::ScreenshotLimit,
        stage_transport::Cancellation,
    };
    use std::{
        sync::{Arc, Mutex, atomic::AtomicBool},
        time::Duration,
    };
    fn pending() -> Pending {
        Pending {
            cancel: Cancellation::new(),
            frontend_cancelled: Arc::new(AtomicBool::new(false)),
            media: None,
            reserved_stop: false,
            reserved_safety_stop: false,
        }
    }
    fn reply(id: Value) -> crate::protocol::OwnedReplyContext {
        let mut protocol = ProtocolSession::native_service();
        let init = json!({"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2025-11-25","clientInfo":{"name":"Fixture","version":"1"},"capabilities":{}}});
        assert!(matches!(
            protocol.admit(&init, &serde_json::to_vec(&init).unwrap()),
            Admission::Reply(_)
        ));
        let initialized = json!({"jsonrpc":"2.0","method":"notifications/initialized"});
        assert!(matches!(
            protocol.admit(&initialized, &serde_json::to_vec(&initialized).unwrap()),
            Admission::Initialized
        ));
        let frame = json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"obs-get-stats","arguments":{}}});
        match protocol.admit(&frame, &serde_json::to_vec(&frame).unwrap()) {
            Admission::Call { reply, .. } => reply.owned(),
            _ => panic!("fixture protocol admission"),
        }
    }
    #[test]
    fn identical_wire_ids_are_scoped_and_departure_keeps_capacity_until_join() {
        let mut ledger = Admissions::new();
        let a = ledger.frontend().unwrap();
        let b = ledger.frontend().unwrap();
        let pa = pending();
        let ca = pa.frontend_cancelled.clone();
        let pb = pending();
        let cb = pb.frontend_cancelled.clone();
        let ia = ledger.admit(a, &json!(7), Class::Ordinary, pa).unwrap();
        let ib = ledger.admit(b, &json!(7), Class::Ordinary, pb).unwrap();
        assert_ne!(ia, ib);
        assert!(matches!(
            ledger.check(a, &json!(7), Class::Ordinary),
            Err(Refusal::Duplicate)
        ));
        ledger.disconnect(a);
        assert!(ca.load(Ordering::Acquire));
        assert!(!cb.load(Ordering::Acquire));
        assert_eq!(ledger.ordinary(), 2);
        assert!(matches!(
            ledger.check(a, &json!(8), Class::Ordinary),
            Err(Refusal::Closed)
        ));
        ledger.retire(ia);
        assert_eq!(ledger.ordinary(), 1);
        assert!(ledger.check(b, &json!(7), Class::Ordinary).is_err());
        ledger.retire(ib);
        assert_eq!(ledger.ordinary(), 0);
    }
    #[test]
    fn completed_short_frontends_do_not_exhaust_lifetime_registration_capacity() {
        let mut ledger = Admissions::new();
        let mut previous = None;
        for _ in 0..512 {
            let id = ledger.frontend().unwrap();
            assert_ne!(Some(id), previous);
            previous = Some(id);
            ledger.frontends.get(&id).unwrap().cancel();
        }
        assert!(ledger.frontend().is_ok());
        assert!(ledger.frontends.len() < 128);
    }
    #[test]
    fn stop_allowances_are_global_across_frontends_and_cancellation_does_not_refund() {
        let mut ledger = Admissions::new();
        let a = ledger.frontend().unwrap();
        let b = ledger.frontend().unwrap();
        for n in 0..ORDINARY_FRONTEND_JOBS {
            ledger
                .admit(a, &json!(n), Class::Ordinary, pending())
                .unwrap();
        }
        let take = ledger
            .admit(b, &json!(100), Class::TakeStop, pending())
            .unwrap();
        let safety = ledger
            .admit(a, &json!(101), Class::SafetyStop, pending())
            .unwrap();
        ledger.cancel(b, &json!(100));
        assert_eq!(ledger.ordinary(), ORDINARY_FRONTEND_JOBS);
        assert!(
            ledger
                .admit(a, &json!(102), Class::TakeStop, pending())
                .is_err()
        );
        assert!(
            ledger
                .admit(b, &json!(103), Class::SafetyStop, pending())
                .is_err()
        );
        ledger.retire(take);
        assert!(
            ledger
                .admit(a, &json!(102), Class::TakeStop, pending())
                .is_ok()
        );
        ledger.retire(safety);
    }
    #[test]
    fn exchange_and_frontend_identity_exhaustion_retires_without_wrapping() {
        let mut ledger = Admissions::new();
        ledger.next_frontend = Some(u64::MAX);
        let a = ledger.frontend().unwrap();
        assert_eq!(a.0, u64::MAX);
        assert_eq!(ledger.frontend(), Err(Refusal::Identity));
        ledger.next_exchange = Some(u64::MAX);
        assert_eq!(
            ledger
                .admit(a, &json!(1), Class::Ordinary, pending())
                .unwrap()
                .0,
            u64::MAX
        );
        assert_eq!(
            ledger.admit(a, &json!(2), Class::Ordinary, pending()),
            Err(Refusal::Identity)
        );
    }
    #[test]
    fn scoped_confirmation_cancel_cannot_remove_peer_with_identical_wire_id() {
        let mut ledger = Admissions::new();
        let a = ledger.frontend().unwrap();
        let b = ledger.frontend().unwrap();
        let mut grants = confirmation::Continuations::new().unwrap();
        let now = tokio::time::Instant::now();
        let ca = grants
            .issue(
                Admissions::continuation_key(a, &json!(5)),
                [1; 32],
                "A",
                16,
                false,
                now,
            )
            .unwrap();
        let cb = grants
            .issue(
                Admissions::continuation_key(b, &json!(5)),
                [2; 32],
                "B",
                16,
                false,
                now,
            )
            .unwrap();
        assert_eq!(
            grants
                .cancel(&Admissions::continuation_key(a, &json!(5)))
                .len(),
            1
        );
        let confirmation::Challenge::Modern(token) = cb else {
            panic!()
        };
        assert_eq!(grants.take_modern(&token, [2; 32], now).unwrap(), "B");
        let confirmation::Challenge::Modern(token) = ca else {
            panic!()
        };
        assert!(grants.take_modern(&token, [1; 32], now).is_err());
    }
    fn prepared(frontend: FrontendId, name: &str, args: Value) -> PreparedCall {
        PreparedCall {
            frontend,
            reply: reply(json!(1)),
            name: name.into(),
            args,
            resource: false,
            controller: false,
            read: None,
            binding: CallerBinding::resolve(
                CallerContext {
                    legacy_name: Some("B"),
                    ..Default::default()
                },
                None,
                false,
                None,
                None,
            ),
            caller: "B".into(),
            apply_request: None,
            capture_request: None,
            confirmed_command: None,
            consent_binding: [0; 32],
            explicit_confirm: None,
            can_elicit: true,
            form_capable: true,
        }
    }
    fn runtime(
        socket: &support::Socket,
        dir: &support::Dir,
        owner: Option<Service>,
        control: SharedControl,
    ) -> Runtime {
        Runtime {
            profile: NativeProfile::Production,
            notifications: resource_events::Owner::new(false),
            owner,
            initialization: None,
            initialization_broken: false,
            network: None,
            tls_ca: None,
            url: socket.url.clone(),
            password: None,
            root: dir.0.clone(),
            host: None,
            control,
            clock: support::clock(),
            preparation: None,
            media: None,
            replay: None,
            media_work: None,
            screenshot_limit: Ok(ScreenshotLimit::default()),
            confirm_live: false,
        }
    }
    #[tokio::test]
    async fn shared_prepare_enforces_profiles_and_binds_confirmation_to_original_profile() {
        let dir = support::Dir::new();
        let mut bindings = Vec::new();
        for profile in [NativeProfile::Production, NativeProfile::Advanced] {
            let runtime = Runtime {
                profile,
                notifications: resource_events::Owner::new(false),
                owner: None,
                initialization: None,
                initialization_broken: false,
                network: None,
                tls_ca: None,
                url: "ws://profile-prepare-fixture.invalid:4455".into(),
                password: None,
                root: dir.0.clone(),
                host: None,
                control: Arc::new(Mutex::new(Default::default())),
                clock: support::clock(),
                preparation: None,
                media: None,
                replay: None,
                media_work: None,
                screenshot_limit: Ok(ScreenshotLimit::default()),
                confirm_live: true,
            };
            let mut host = Host::from_runtime(runtime, false).unwrap();
            let frontend = host.admissions.frontend().unwrap();
            for (name, allowed) in [
                ("obs-start-output", profile == NativeProfile::Advanced),
                ("obs-start-stream", true),
            ] {
                let frame = json!({"jsonrpc":"2.0","id":11,"method":"tools/call",
                    "params":{"name":name,"arguments":{"outputName":"private"}}});
                let raw = serde_json::to_vec(&frame).unwrap();
                let result = host.prepare(
                    frontend,
                    Incoming {
                        reply: reply(json!(11)),
                        name: name.into(),
                        args: frame["params"]["arguments"].clone(),
                        resource: false,
                        controller: false,
                        value: &frame,
                        frame: &raw,
                        legacy_name: Some("Fixture"),
                        legacy_capabilities: None,
                    },
                );
                assert_eq!(result.is_ok(), allowed, "{profile:?}/{name}");
                if name == "obs-start-stream" {
                    bindings.push(result.unwrap().consent_binding);
                }
            }
            assert!(host.jobs.is_empty());
            assert!(host.runtime.owner.is_none());
            host.shutdown().await.unwrap();
        }
        assert_ne!(bindings[0], bindings[1]);
    }
    #[tokio::test]
    async fn stateless_legacy_offer_never_creates_or_retains_confirmation_authority() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let service = socket.service(&dir, Duration::from_secs(3)).await;
        let mut runtime = runtime(
            &socket,
            &dir,
            Some(service),
            Arc::new(Mutex::new(Default::default())),
        );
        runtime.confirm_live = true;
        let mut host = Host::from_runtime(runtime, false).unwrap();
        let frontend = host.admissions.frontend().unwrap();
        let call = prepared(frontend, "obs-stop-record", json!({}));
        let Authorization::Err(response) = host.authorize(call, None) else {
            panic!("legacy offer became consent")
        };
        assert!(response.to_string().contains("client cannot ask"));
        assert!(host.confirmations.next_deadline().is_none());
        let mut call = prepared(frontend, "obs-stop-record", json!({}));
        call.explicit_confirm = Some(true);
        assert!(matches!(host.authorize(call, None), Authorization::Ok(_)));
        assert_eq!(socket.count("StopRecord"), 0);
        host.shutdown().await.unwrap();
        socket.join().await;
    }
    #[tokio::test]
    async fn departing_initiator_preserves_locked_take_peer_refusal_and_peer_owned_stop() {
        use obs_mcp_native::{
            preflight::{self, Report},
            take_monitor::Summary,
            take_transport::{HostServices, PreflightReader},
            takes::{ClipCheck, IoFuture},
        };
        struct FixtureHost(AtomicBool);
        impl HostServices for FixtureHost {
            fn preflight(&self, _: PreflightReader, _: preflight::Options) -> IoFuture<Report> {
                Box::pin(async {
                    Report {
                        ready: true,
                        checks: vec![preflight::Check {
                            id: "record-directory".into(),
                            status: preflight::Status::Pass,
                            message: "injected host".into(),
                        }],
                    }
                })
            }
            fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
                Box::pin(async {
                    ClipCheck {
                        skipped: Some("fake media".into()),
                        ..Default::default()
                    }
                })
            }
            fn write_log(&self, _: String, _: Summary) -> IoFuture<Option<String>> {
                Box::pin(async { None })
            }
            fn join_observers(&self) -> IoFuture<Result<(), String>> {
                self.0.store(true, Ordering::Release);
                Box::pin(async { Ok(()) })
            }
        }
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let control = Arc::new(Mutex::new(ControlDomain::default()));
        let fixture_host = Arc::new(FixtureHost(AtomicBool::new(false)));
        let (session, bridge) = socket.connect().await;
        let mut next = 0;
        let service = Service::spawn(
            Resources {
                session,
                bridge,
                store: OperationStore::new(&dir.0, &socket.url).unwrap(),
                control: control.clone(),
                clock: support::clock(),
                identities: Box::new(move || {
                    next += 1;
                    support::id(next)
                }),
            },
            Options {
                state_root: Some(dir.0.clone()),
                connection: Some(ConnectionDescriptor {
                    url: socket.url.clone(),
                    has_password: false,
                }),
                take: Some(TakeServices {
                    host: fixture_host.clone(),
                    exact_endpoint: socket.url.clone(),
                    pending_settings: Arc::new(AtomicBool::new(false)),
                    bounds: Bounds::default(),
                    limits: Limits {
                        stats_interval: Duration::from_secs(60),
                        picture_interval: Duration::from_secs(60),
                        ..Default::default()
                    },
                }),
                ..Default::default()
            },
        )
        .map_err(|(error, _)| error)
        .unwrap();
        let mut host =
            Host::from_runtime(runtime(&socket, &dir, Some(service), control), false).unwrap();
        let a = host.admissions.frontend().unwrap();
        let b = host.admissions.frontend().unwrap();
        let handle = host.runtime.connect().await.unwrap();
        let started = handle
            .start_take(
                StartOptions {
                    max_duration_seconds: Some(10),
                    lock: true,
                    ..Default::default()
                },
                Policy {
                    caller: "A".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_ne!(started["isError"], true, "{started}");
        let take_id = started["structuredContent"]["takeId"].clone();
        assert!(take_id.is_string(), "{started}");
        host.disconnect(a);
        assert_eq!(socket.count("StopRecord"), 0);
        let handle = host.runtime.connect().await.unwrap();
        let status = handle.take_status().await.unwrap();
        assert_eq!(status["structuredContent"]["takeId"], take_id);
        assert_eq!(status["structuredContent"]["ownership"], "continuous");
        let call = prepared(
            b,
            "obs-set-input-mute",
            json!({"inputName":"Fixture","inputMuted":true}),
        );
        let Authorization::Err(refused) = host.authorize(call, None) else {
            panic!("locked Take was lost")
        };
        assert!(refused.to_string().contains("Not changed"));
        let mut same_caller =
            prepared(b, "obs-set-current-profile", json!({"profileName":"Other"}));
        same_caller.caller = "A".into();
        let Authorization::Err(locked) = host.authorize(same_caller, None) else {
            panic!("locked Take profile change allowed")
        };
        assert!(locked.to_string().contains("is recording"));
        assert_eq!(socket.count("SetInputMute"), 0);
        let call = prepared(b, "obs-take-stop", json!({}));
        let Authorization::Ok(invocation) = host.authorize(call, None) else {
            panic!("peer safety Stop refused")
        };
        assert!(host.dispatch(invocation).await.unwrap().is_none());
        let completed = host.jobs.join_next().await.unwrap().unwrap();
        let finished = host.complete(completed).unwrap();
        assert_eq!(finished.frontend, b);
        assert_eq!(
            finished.response.unwrap()["result"]["structuredContent"]["stopConfirmed"],
            true
        );
        host.shutdown().await.unwrap();
        assert!(fixture_host.0.load(Ordering::Acquire));
        assert_eq!(socket.count("StartRecord"), 1);
        assert_eq!(socket.count("StopRecord"), 1);
        socket.join().await;
    }
    #[tokio::test]
    async fn departure_during_lazy_connect_joins_initialization_without_dispatching_mutation() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        socket.plan(
            "GetVersion",
            support::Plan {
                held: true,
                ..Default::default()
            },
        );
        let mut runtime = runtime(
            &socket,
            &dir,
            None,
            Arc::new(Mutex::new(Default::default())),
        );
        runtime.preparation = Some(PreparationOwners::new(&dir.0).unwrap());
        let mut host = Host::from_runtime(runtime, false).unwrap();
        let guard = host.frontend().unwrap();
        let frontend = guard.id();
        let call = prepared(
            frontend,
            "obs-set-input-mute",
            json!({"inputName":"Fixture","inputMuted":true}),
        );
        let Authorization::Ok(invocation) = host.authorize(call, None) else {
            panic!()
        };
        assert!(host.dispatch(invocation).await.unwrap().is_none());
        socket.seen("GetVersion", 1).await;
        assert!(host.runtime.initializing());
        assert!(host.runtime.owner.is_none());
        assert!(host.runtime.preparation.is_some());
        // The same Host stays borrowable while the sole physical attempt waits.
        let peer = host.frontend().unwrap();
        let call = prepared(peer.id(), "obs-snapshot", json!({"list":true}));
        let Authorization::Ok(invocation) = host.authorize(call, None) else {
            panic!()
        };
        assert!(host.dispatch(invocation).await.unwrap().is_some());
        assert!(host.runtime.preparation.is_some());
        let call = prepared(peer.id(), "obs-control-status", json!({}));
        let Authorization::Ok(invocation) = host.authorize(call, None) else {
            panic!()
        };
        assert!(host.dispatch(invocation).await.unwrap().is_none());
        let completed = host.next_completed().await.unwrap().unwrap();
        assert!(host.complete(completed).unwrap().response.is_some());
        drop(guard);
        let completed = host.next_completed().await.unwrap().unwrap();
        assert!(host.complete(completed).unwrap().response.is_none());
        assert!(host.jobs.is_empty());
        assert!(host.runtime.initializing());
        assert_eq!(socket.count("SetInputMute"), 0);
        socket.release("GetVersion").await;
        assert!(host.next_completed().await.unwrap().is_none());
        assert!(host.runtime.owner.is_some());
        let handle = host.runtime.connect().await.unwrap();
        assert!(
            handle
                .read(ReadRequest::Stats, Value::Null, Duration::from_secs(3))
                .await
                .is_ok()
        );
        assert_eq!(socket.count("GetVersion"), 1);
        host.shutdown().await.unwrap();
        socket.join().await;
    }
    #[tokio::test]
    async fn shared_initialization_waiters_use_global32_plus2_and_shutdown_joins_without_effects() {
        use futures_util::poll;
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        socket.plan(
            "GetVersion",
            support::Plan {
                held: true,
                ..Default::default()
            },
        );
        let mut runtime = runtime(&socket, &dir, None, support::control());
        runtime.host = Some(Arc::new(LocalHost::new(&dir.0, None, None).unwrap()));
        runtime.preparation = Some(PreparationOwners::new(&dir.0).unwrap());
        let mut host = Host::from_runtime(runtime, false).unwrap();
        let a = host.frontend().unwrap();
        let b = host.frontend().unwrap();
        for index in 0..32 {
            let mut call = prepared(
                a.id(),
                "obs-set-input-mute",
                json!({"inputName":"Fixture","inputMuted":true}),
            );
            call.reply = reply(json!(index));
            let Authorization::Ok(invocation) = host.authorize(call, None) else {
                panic!()
            };
            assert!(host.dispatch(invocation).await.unwrap().is_none());
        }
        for (id, name) in [(100, "obs-take-stop"), (101, "obs-stop-record")] {
            let mut call = prepared(b.id(), name, json!({}));
            call.reply = reply(json!(id));
            let Authorization::Ok(invocation) = host.authorize(call, None) else {
                panic!()
            };
            assert!(host.dispatch(invocation).await.unwrap().is_none());
        }
        assert_eq!(host.admissions.entries.len(), 34);
        assert_eq!(host.admissions.ordinary(), 32);
        let mut call = prepared(
            b.id(),
            "obs-set-input-mute",
            json!({"inputName":"Fixture","inputMuted":true}),
        );
        call.reply = reply(json!(999));
        assert!(
            matches!(host.authorize(call,None),Authorization::Err(value)if value.get("error").is_some())
        );
        socket.seen("GetVersion", 1).await;
        assert!(host.runtime.owner.is_none());
        assert!(host.runtime.preparation.is_some());
        drop(a);
        drop(b);
        while !host.jobs.is_empty() {
            let completed = host.next_completed().await.unwrap().unwrap();
            assert!(host.complete(completed).unwrap().response.is_none());
        }
        assert!(host.admissions.entries.is_empty());
        assert_eq!(
            host.initialization_bytes.ordinary.available_permits(),
            32 * 1024 * 1024
        );
        assert!(host.runtime.initializing());
        let mut closing = Box::pin(host.shutdown());
        assert!(poll!(&mut closing).is_pending());
        socket.release("GetVersion").await;
        closing.await.unwrap();
        assert_eq!(socket.count("GetVersion"), 1);
        assert_eq!(socket.count("SetInputMute"), 0);
        assert_eq!(socket.count("StopRecord"), 0);
        socket.join().await;
    }
    #[tokio::test]
    async fn initialization_byte_pressure_refuses_before_connect_and_preserves_each_stop_fallback()
    {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        socket.plan(
            "GetVersion",
            support::Plan {
                held: true,
                ..Default::default()
            },
        );
        let mut host =
            Host::from_runtime(runtime(&socket, &dir, None, support::control()), false).unwrap();
        let guard = host.frontend().unwrap();
        let ordinary = host
            .initialization_bytes
            .ordinary
            .clone()
            .try_acquire_many_owned(32 * 1024 * 1024)
            .unwrap();
        let call = prepared(
            guard.id(),
            "obs-set-input-mute",
            json!({"inputName":"Fixture","inputMuted":true}),
        );
        let Authorization::Ok(invocation) = host.authorize(call, None) else {
            panic!()
        };
        assert!(
            host.dispatch(invocation)
                .await
                .unwrap()
                .unwrap()
                .get("error")
                .is_some()
        );
        assert!(!host.runtime.initializing());
        for (id, name) in [(100, "obs-stop-record"), (101, "obs-take-stop")] {
            let mut call = prepared(guard.id(), name, json!({}));
            call.reply = reply(json!(id));
            let Authorization::Ok(invocation) = host.authorize(call, None) else {
                panic!()
            };
            assert!(host.dispatch(invocation).await.unwrap().is_none());
        }
        for (id, name) in [(102, "obs-stop-record"), (103, "obs-take-stop")] {
            let mut call = prepared(guard.id(), name, json!({}));
            call.reply = reply(json!(id));
            let Authorization::Ok(invocation) = host.authorize(call, None) else {
                panic!()
            };
            assert!(
                host.dispatch(invocation)
                    .await
                    .unwrap()
                    .unwrap()
                    .get("error")
                    .is_some()
            );
        }
        socket.seen("GetVersion", 1).await;
        drop(guard);
        while !host.jobs.is_empty() {
            let done = host.next_completed().await.unwrap().unwrap();
            assert!(host.complete(done).unwrap().response.is_none());
        }
        assert_eq!(host.initialization_bytes.take_slot.available_permits(), 1);
        assert_eq!(host.initialization_bytes.safety_slot.available_permits(), 1);
        drop(ordinary);
        socket.release("GetVersion").await;
        host.shutdown().await.unwrap();
        assert_eq!(socket.count("StopRecord"), 0);
        socket.join().await;
    }
    #[tokio::test]
    async fn actual_connect_close_joins_failed_resources_and_cold_peer_work_survives() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        socket.plan(
            "GetVersion",
            support::Plan {
                lost: true,
                ..Default::default()
            },
        );
        let mut host =
            Host::from_runtime(runtime(&socket, &dir, None, support::control()), false).unwrap();
        let guard = host.frontend().unwrap();
        let call = prepared(
            guard.id(),
            "obs-set-input-mute",
            json!({"inputName":"Fixture","inputMuted":true}),
        );
        let Authorization::Ok(invocation) = host.authorize(call, None) else {
            panic!()
        };
        assert!(host.dispatch(invocation).await.unwrap().is_none());
        let completed = host.next_completed().await.unwrap().unwrap();
        assert_eq!(
            host.complete(completed).unwrap().response.unwrap()["result"]["isError"],
            true
        );
        assert!(host.runtime.owner.is_none());
        assert!(!host.runtime.initializing());
        assert!(!host.runtime.initialization_broken);
        let call = prepared(guard.id(), "obs-control-status", json!({}));
        let Authorization::Ok(invocation) = host.authorize(call, None) else {
            panic!()
        };
        assert!(host.dispatch(invocation).await.unwrap().is_none());
        let completed = host.next_completed().await.unwrap().unwrap();
        assert!(host.complete(completed).unwrap().response.is_some());
        assert_eq!(socket.count("SetInputMute"), 0);
        host.shutdown().await.unwrap();
        socket.join().await;
    }
    #[tokio::test]
    async fn one_departing_frontend_preserves_peer_socket_and_global_shutdown_joins_held_work() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let service = socket.service(&dir, Duration::from_secs(3)).await;
        let runtime = Runtime {
            profile: NativeProfile::Production,
            notifications: resource_events::Owner::new(false),
            owner: Some(service),
            initialization: None,
            initialization_broken: false,
            network: None,
            tls_ca: None,
            url: socket.url.clone(),
            password: None,
            root: dir.0.clone(),
            host: None,
            control: Arc::new(Mutex::new(Default::default())),
            clock: support::clock(),
            preparation: None,
            media: None,
            replay: None,
            media_work: None,
            screenshot_limit: Ok(ScreenshotLimit::default()),
            confirm_live: false,
        };
        let mut host = Host::from_runtime(runtime, false).unwrap();
        let a = host.admissions.frontend().unwrap();
        let b = host.admissions.frontend().unwrap();
        assert_eq!(
            host.resource_scope(a, true).unwrap(),
            ResourceScope::Exchange
        );
        assert_eq!(
            host.resource_scope(b, false).unwrap(),
            ResourceScope::Frontend(b)
        );
        let handle = host.runtime.connect().await.unwrap();
        socket.plan(
            "GetStats",
            support::Plan {
                held: true,
                ..Default::default()
            },
        );
        let pa = pending();
        let cancelled_a = pa.frontend_cancelled.clone();
        let ia = host
            .admissions
            .admit(a, &json!(1), Class::Ordinary, pa)
            .unwrap();
        let owned = reply(json!(1));
        host.jobs.spawn(async move {
            let result = handle
                .read(ReadRequest::Stats, Value::Null, Duration::from_secs(3))
                .await
                .map_err(|_| ResourceFailure {
                    code: -32603,
                    message: "fixture read".into(),
                });
            (ia, owned, result, cancelled_a, false)
        });
        socket.seen("GetStats", 1).await;
        host.disconnect(a);
        assert_eq!(host.jobs.len(), 1);
        assert_eq!(host.admissions.ordinary(), 1);
        let handle = host.runtime.connect().await.unwrap();
        let pb = pending();
        let cancelled_b = pb.frontend_cancelled.clone();
        let ib = host
            .admissions
            .admit(b, &json!(1), Class::Ordinary, pb)
            .unwrap();
        let owned = reply(json!(1));
        host.jobs.spawn(async move {
            let result = handle
                .read(ReadRequest::Version, Value::Null, Duration::from_secs(3))
                .await
                .map_err(|_| ResourceFailure {
                    code: -32603,
                    message: "fixture version".into(),
                });
            (ib, owned, result, cancelled_b, false)
        });
        // The Session serializes physical reads; a held ACK may queue a peer
        // read, but departing A must not close the socket or cancel B.
        socket.release("GetStats").await;
        for _ in 0..2 {
            let (id, _, result, cancelled, _) =
                tokio::time::timeout(Duration::from_secs(3), host.jobs.join_next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            assert!(result.is_ok());
            assert_eq!(cancelled.load(Ordering::Acquire), id == ia);
            assert!(id == ia || id == ib);
            host.admissions.retire(id);
        }
        assert_eq!(socket.count("GetStats"), 1);
        // Global shutdown, unlike a frontend departure, joins a held operation.
        let handle = host.runtime.connect().await.unwrap();
        socket.plan(
            "GetStats",
            support::Plan {
                held: true,
                ..Default::default()
            },
        );
        let pending = pending();
        let cancelled = pending.frontend_cancelled.clone();
        let id = host
            .admissions
            .admit(b, &json!(2), Class::Ordinary, pending)
            .unwrap();
        let owned = reply(json!(2));
        host.jobs.spawn(async move {
            let result = handle
                .read(ReadRequest::Stats, Value::Null, Duration::from_secs(3))
                .await
                .map_err(|_| ResourceFailure {
                    code: -32603,
                    message: "shutdown read".into(),
                });
            (id, owned, result, cancelled, false)
        });
        socket.seen("GetStats", 2).await;
        let mut shutdown = Box::pin(host.shutdown());
        assert!(poll!(shutdown.as_mut()).is_pending());
        socket.release("GetStats").await;
        shutdown.await.unwrap();
        assert!(host.runtime.owner.is_some());
        assert!(host.admissions.entries.is_empty());
        socket.join().await;
    }
}
