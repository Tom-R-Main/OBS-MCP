// SPDX-License-Identifier: GPL-2.0-only
//! Control policy for one OBS domain. Decisions are observations, never reusable dispatch permits.

use serde::Serialize;
use serde_json::{Value, json};

pub const UNIDENTIFIED: &str = "an unidentified client";
const CLIENT_INFO: &str = "io.modelcontextprotocol/clientInfo";
const CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";

#[derive(Default)]
pub struct CallerContext<'a> {
    pub http_client_id: Option<&'a Value>,
    /// The request envelope exposed by the MCP adapter, including namespaced metadata keys.
    pub envelope: Option<&'a Value>,
    pub legacy_name: Option<&'a str>,
}

pub fn caller_name(context: CallerContext<'_>) -> String {
    context
        .http_client_id
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .or_else(|| {
            context
                .envelope?
                .get(CLIENT_INFO)?
                .get("name")?
                .as_str()
                .filter(|name| !name.is_empty())
        })
        .or(context.legacy_name.filter(|name| !name.is_empty()))
        .unwrap_or(UNIDENTIFIED)
        .to_owned()
}

pub fn is_identified(name: &str) -> bool {
    name != UNIDENTIFIED
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// A modern capability object overrides legacy capabilities, including when it is empty.
pub fn client_can_elicit(envelope: Option<&Value>, legacy_capabilities: Option<&Value>) -> bool {
    if let Some(capabilities) = envelope
        .and_then(|value| value.get(CLIENT_CAPABILITIES))
        .filter(|value| truthy(value))
    {
        return capabilities.get("elicitation").is_some();
    }
    legacy_capabilities
        .and_then(|value| value.get("elicitation"))
        .is_some()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Lease {
    pub holder: String,
    pub reason: String,
    pub since: u64,
    pub expires_at: u64,
}

impl Lease {
    pub fn description(&self) -> String {
        let minutes = self.expires_at / 60_000;
        let reason = if self.reason.is_empty() {
            String::new()
        } else {
            format!(" ({})", self.reason)
        };
        format!(
            "{} has control{} until {:02}:{:02} UTC",
            self.holder,
            reason,
            (minutes / 60) % 24,
            minutes % 60
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimOutcome {
    Claimed(Lease),
    Unidentified,
    HeldBy(Lease),
}

fn text_result(text: impl Into<String>, is_error: bool) -> Value {
    let mut result = json!({"content":[{"type":"text","text":text.into()}]});
    if is_error {
        result["isError"] = json!(true);
    }
    result
}

impl ClaimOutcome {
    pub fn tool_result(&self) -> Value {
        match self {
            Self::Claimed(lease) => {
                let mut result = text_result(
                    format!(
                        "{}. Release it with obs-release-control",
                        lease.description()
                    ),
                    false,
                );
                result["structuredContent"] = json!(lease);
                result
            }
            Self::Unidentified => text_result(
                "Not claimed: this client did not identify itself. Over HTTP, connect with a name in the URL, e.g. http://127.0.0.1:<port>/mcp?client=codex",
                true,
            ),
            Self::HeldBy(lease) => {
                text_result(format!("Not claimed: {}", lease.description()), true)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReleaseOutcome {
    Released,
    NoLease,
    HeldBy(Lease),
}

impl ReleaseOutcome {
    pub fn tool_result(&self) -> Value {
        match self {
            Self::Released => text_result("Released control of OBS", false),
            Self::NoLease => text_result("No client has control", false),
            Self::HeldBy(lease) => {
                text_result(format!("Not released: {}", lease.description()), true)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimError {
    InvalidMinutes,
    ReasonTooLong,
    ClockOverflow,
}

impl std::fmt::Display for ClaimError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidMinutes => "Control duration must be 1 to 240 minutes",
            Self::ReasonTooLong => "Control reason exceeds 200 Unicode code points",
            Self::ClockOverflow => "Control expiry exceeds the supported clock range",
        })
    }
}

impl std::error::Error for ClaimError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TakeLock {
    pub id: String,
    pub locked: bool,
}

#[derive(Default)]
pub struct ControlDomain {
    lease: Option<Lease>,
    take: Option<TakeLock>,
}

impl ControlDomain {
    pub fn active_lease(&mut self, now_ms: u64) -> Option<&Lease> {
        if self
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at <= now_ms)
        {
            self.lease = None;
        }
        self.lease.as_ref()
    }

    /// Schema bounds are checked here too, so a future adapter cannot bypass them.
    pub fn claim(
        &mut self,
        holder: &str,
        minutes: u16,
        reason: &str,
        now_ms: u64,
    ) -> Result<ClaimOutcome, ClaimError> {
        if !(1..=240).contains(&minutes) {
            return Err(ClaimError::InvalidMinutes);
        }
        if reason.chars().count() > 200 {
            return Err(ClaimError::ReasonTooLong);
        }
        if !is_identified(holder) {
            return Ok(ClaimOutcome::Unidentified);
        }
        let current = self.active_lease(now_ms);
        if let Some(lease) = current.filter(|lease| lease.holder != holder) {
            return Ok(ClaimOutcome::HeldBy(lease.clone()));
        }
        let since = current.map_or(now_ms, |lease| lease.since);
        let expires_at = now_ms
            .checked_add(u64::from(minutes) * 60_000)
            .ok_or(ClaimError::ClockOverflow)?;
        let lease = Lease {
            holder: holder.to_owned(),
            reason: reason.to_owned(),
            since,
            expires_at,
        };
        self.lease = Some(lease.clone());
        Ok(ClaimOutcome::Claimed(lease))
    }

    pub fn release(&mut self, holder: &str, now_ms: u64) -> ReleaseOutcome {
        match self.active_lease(now_ms) {
            None => ReleaseOutcome::NoLease,
            Some(lease) if lease.holder != holder => ReleaseOutcome::HeldBy(lease.clone()),
            Some(_) => {
                self.lease = None;
                ReleaseOutcome::Released
            }
        }
    }

    pub fn status_result(&mut self, caller: &str, now_ms: u64) -> Value {
        let lease = self.active_lease(now_ms);
        let description = lease.map_or_else(
            || "No client has control; any client may change OBS".to_owned(),
            |lease| {
                format!(
                    "{}{}",
                    lease.description(),
                    if lease.holder == caller {
                        " (this client)"
                    } else {
                        ""
                    }
                )
            },
        );
        let mut result = text_result(description, false);
        result["structuredContent"] = json!({"caller":caller,"lease":lease});
        result
    }

    /// The take supervisor supplies reconciled state; this does not start, stop or infer a take.
    pub fn set_take_lock(&mut self, take: Option<TakeLock>) {
        self.take = take;
    }

    /// Evaluate again at dispatch after any await or change in state. Allow is not a lease token.
    pub fn evaluate(&mut self, request: GateRequest<'_>, now_ms: u64) -> Disposition {
        if !request.read_only_hint
            && !always_allowed(request.tool_name)
            && let Some(lease) = self.active_lease(now_ms)
            && (!is_identified(request.caller) || request.caller != lease.holder)
        {
            return Disposition::Refused {
                kind: RefusalKind::Lease,
                message: format!(
                    "Not changed: {}. Ask them to call obs-release-control, or wait. Reading OBS and stopping outputs still work",
                    lease.description()
                ),
            };
        }
        if let Some(take) = self.take.as_ref().filter(|take| take.locked)
            && let Some(action) = locked_action(request.tool_name, request.arguments)
        {
            return Disposition::Refused {
                kind: RefusalKind::TakeLock,
                message: format!(
                    "Take {} is recording, and changing this now would {action} under it. Stop the take with obs-take-stop first, or start takes with lock: false",
                    take.id
                ),
            };
        }
        if !request.confirmation.enabled {
            return Disposition::Allow;
        }
        let Some(question) = live_question(request.tool_name, request.arguments) else {
            return Disposition::Allow;
        };
        match ConfirmationAnswer::from_responses(request.confirmation.input_responses) {
            ConfirmationAnswer::Accepted(true) => return Disposition::Allow,
            ConfirmationAnswer::Accepted(false) | ConfirmationAnswer::OtherResponse => {
                return Disposition::Cancelled;
            }
            ConfirmationAnswer::Missing => {}
        }
        if request.confirmation.can_elicit {
            return Disposition::InputRequired { question };
        }
        if request.arguments.get("confirm").and_then(Value::as_bool) == Some(true) {
            return Disposition::Allow;
        }
        Disposition::Refused {
            kind: RefusalKind::Confirmation,
            message: format!(
                "{question} This server requires confirmation for live actions and this client cannot ask. Ask the user, then call again with confirm: true."
            ),
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct ConfirmationContext<'a> {
    pub enabled: bool,
    pub can_elicit: bool,
    /// SDK neutral context map: confirm -> bare elicitation result, not a method/result wrapper.
    pub input_responses: Option<&'a Value>,
}

pub struct GateRequest<'a> {
    pub tool_name: &'a str,
    pub arguments: &'a Value,
    /// Trusted tool registration annotation, never a hint supplied by the requesting client.
    pub read_only_hint: bool,
    pub caller: &'a str,
    pub confirmation: ConfirmationContext<'a>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusalKind {
    Lease,
    TakeLock,
    Confirmation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Disposition {
    Allow,
    Refused { kind: RefusalKind, message: String },
    InputRequired { question: String },
    Cancelled,
}

impl Disposition {
    /// None means the caller may continue only within the same checked dispatch turn.
    pub fn tool_result(&self) -> Option<Value> {
        match self {
            Self::Allow => None,
            Self::Refused { message, .. } => Some(text_result(message, true)),
            Self::Cancelled => Some(text_result("Cancelled; nothing was changed", false)),
            Self::InputRequired { question } => Some(json!({
                "resultType":"input_required",
                "inputRequests":{"confirm":{"method":"elicitation/create","params":{
                    "mode":"form","message":question,"requestedSchema":{
                        "type":"object","properties":{"confirm":{"type":"boolean","title":"Confirm","description":question}},
                        "required":["confirm"]
                    }
                }}}
            })),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfirmationAnswer {
    Missing,
    Accepted(bool),
    OtherResponse,
}

impl ConfirmationAnswer {
    /// Matches the pinned SDK inputResponse/acceptedContent behavior for the confirm key.
    pub fn from_responses(responses: Option<&Value>) -> Self {
        let Some(entry) = responses
            .and_then(|responses| responses.get("confirm"))
            .filter(|entry| entry.is_object())
        else {
            return Self::Missing;
        };
        match entry.get("action").and_then(Value::as_str) {
            Some("accept") => entry
                .get("content")
                .filter(|content| content.is_object())
                .and_then(|content| content.get("confirm"))
                .and_then(Value::as_bool)
                .map_or(Self::OtherResponse, Self::Accepted),
            Some("decline" | "cancel") => Self::OtherResponse,
            _ if entry.get("roots").is_some_and(Value::is_array)
                || (entry.get("role").is_some_and(Value::is_string)
                    && entry.get("content").is_some()) =>
            {
                Self::OtherResponse
            }
            _ => Self::Missing,
        }
    }
}

pub fn live_confirmation_enabled(value: Option<&str>) -> bool {
    value.is_some_and(|value| matches!(value.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
}

fn always_allowed(tool: &str) -> bool {
    matches!(
        tool,
        "obs-claim-control"
            | "obs-release-control"
            | "obs-stop-record"
            | "obs-stop-stream"
            | "obs-take-stop"
            | "obs-stop-virtual-cam"
            | "obs-stop-replay-buffer"
            | "obs-stop-output"
    )
}

fn fixed_lock(tool: &str) -> Option<&'static str> {
    Some(match tool {
        "obs-set-current-profile" => "switch profiles",
        "obs-create-profile" => "create and switch to a profile",
        "obs-remove-profile" => "remove a profile",
        "obs-set-current-scene-collection" => "switch scene collections",
        "obs-create-scene-collection" => "create and switch to a scene collection",
        "obs-set-profile-parameter" => "change profile settings",
        "obs-set-record-directory" => "change the recording directory",
        "obs-set-video-settings" => "change the canvas and frame rate",
        "obs-set-stream-service-settings" => "change the stream service",
        "obs-set-output-settings" => "change an output's settings",
        _ => return None,
    })
}

fn locked_request(request: &str) -> bool {
    matches!(
        request,
        "SetCurrentProfile"
            | "CreateProfile"
            | "RemoveProfile"
            | "SetCurrentSceneCollection"
            | "CreateSceneCollection"
            | "SetProfileParameter"
            | "SetRecordDirectory"
            | "SetVideoSettings"
            | "SetStreamServiceSettings"
            | "SetOutputSettings"
    )
}

pub fn locked_action(tool: &str, arguments: &Value) -> Option<String> {
    if let Some(action) = fixed_lock(tool) {
        return Some(action.to_owned());
    }
    if !arguments.is_object() {
        return None;
    }
    let requests: Vec<_> = match tool {
        "obs-call-request" => vec![arguments],
        "obs-batch" => arguments.get("requests")?.as_array()?.iter().collect(),
        _ => return None,
    };
    let locked: Vec<_> = requests
        .iter()
        .filter_map(|request| request.get("requestType").and_then(Value::as_str))
        .filter(|request| locked_request(request))
        .collect();
    (!locked.is_empty()).then(|| format!("send {}", locked.join(", ")))
}

fn fixed_question(tool: &str) -> Option<&'static str> {
    Some(match tool {
        "obs-take-scene" => "Transition the staged preview scene to program?",
        "obs-start-stream" => "Start the live stream?",
        "obs-stop-stream" => "Stop the live stream?",
        "obs-toggle-stream" => "Toggle the live stream? If it is live, this ends the broadcast.",
        "obs-stop-record" | "obs-take-stop" => "Stop the recording?",
        "obs-toggle-record" => "Toggle recording? If it is recording, this ends the take.",
        _ => return None,
    })
}

fn js_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".to_owned(),
        Some(Value::Null) => "null".to_owned(),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Number(value)) => {
            let number = value.as_f64().unwrap_or(0.0);
            if number == 0.0 {
                "0".to_owned()
            } else if number.abs() >= 1e21 || number.abs() < 1e-6 {
                let formatted = format!("{number:e}");
                if let Some((base, exponent)) = formatted.split_once('e') {
                    format!(
                        "{base}e{}{exponent}",
                        if exponent.starts_with('-') { "" } else { "+" }
                    )
                } else {
                    formatted
                }
            } else {
                number.to_string()
            }
        }
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    String::new()
                } else {
                    js_string(Some(value))
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(Value::Object(_)) => "[object Object]".to_owned(),
    }
}

fn request_question(kind: &str, data: Option<&Value>) -> Option<String> {
    Some(match kind {
        "StartStream" => "Start the live stream?".to_owned(),
        "StopStream" => "Stop the live stream?".to_owned(),
        "ToggleStream" => {
            "Toggle the live stream? If it is live, this ends the broadcast.".to_owned()
        }
        "StopRecord" => "Stop the recording?".to_owned(),
        "ToggleRecord" => "Toggle recording? If it is recording, this ends the take.".to_owned(),
        "StartOutput" => format!(
            "Start the output {}? This may start a live broadcast or recording.",
            js_string(data.and_then(|data| data.get("outputName")))
        ),
        "StopOutput" => format!(
            "Stop the output {}? If it is the stream or the recording, this ends it.",
            js_string(data.and_then(|data| data.get("outputName")))
        ),
        "ToggleOutput" => format!(
            "Toggle the output {}? If it is the stream or the recording, this can end it.",
            js_string(data.and_then(|data| data.get("outputName")))
        ),
        _ => return None,
    })
}

pub fn live_question(tool: &str, arguments: &Value) -> Option<String> {
    if let Some(question) = fixed_question(tool) {
        return Some(question.to_owned());
    }
    match tool {
        "obs-call-request" => {
            return request_question(
                arguments.get("requestType")?.as_str()?,
                arguments.get("requestData"),
            );
        }
        "obs-start-output" => return request_question("StartOutput", Some(arguments)),
        "obs-stop-output" => return request_question("StopOutput", Some(arguments)),
        "obs-toggle-output" => return request_question("ToggleOutput", Some(arguments)),
        "obs-batch" => {}
        _ => return None,
    }
    let requests = arguments.get("requests")?.as_array()?;
    let mut questions = Vec::new();
    for request in requests {
        if let Some(question) = request
            .get("requestType")
            .and_then(Value::as_str)
            .and_then(|kind| request_question(kind, request.get("requestData")))
            && !questions.contains(&question)
        {
            questions.push(question);
        }
    }
    (!questions.is_empty()).then(|| questions.join(" "))
}
