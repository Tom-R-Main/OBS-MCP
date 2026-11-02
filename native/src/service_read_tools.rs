// SPDX-License-Identifier: GPL-2.0-only
//! Custom read-only callbacks on a supplied physical session. The service owns
//! the entire future through cancellation/deadline settlement. No task, socket,
//! observer, journal writer or take-feed consumer is created here.
use crate::{
    clock,
    custody::Clock,
    observation::{ObservationError, ReadAdapter, ReadRequest},
    preflight::{self, Adapter, ConnectionContext, ReadError},
    scene_audit::{ScreenshotLimit, screenshot_content},
    service::ConnectionDescriptor,
    session::{BatchOptions, BatchRequest, SessionHandle},
    session_observation::SessionReader,
    stage_transport::Cancellation,
    take_transport::LocalHost,
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::{Instant, sleep_until};

#[derive(Clone, Debug)]
pub enum ReadToolRequest {
    Status,
    TestConnection,
    Sleep {
        millis: Option<u64>,
        frames: Option<u64>,
    },
    InputList {
        input_kind: Option<String>,
    },
    InputKindList {
        unversioned: Option<bool>,
    },
    Screenshot(ScreenshotArguments),
    Preflight(preflight::Options),
}
#[derive(Clone)]
pub struct ScreenshotArguments {
    data: Value,
}
impl std::fmt::Debug for ScreenshotArguments {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str("ScreenshotArguments(..)")
    }
}
fn invalid() -> ObservationError {
    ObservationError::new("invalid_arguments", "Invalid custom read-tool arguments")
}
fn integer(value: &Value, min: f64, max: f64) -> Option<f64> {
    value
        .as_f64()
        .filter(|n| n.is_finite() && n.fract() == 0.0 && *n >= min && *n <= max)
}
fn optional_string(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Option<String>, ObservationError> {
    object
        .get(key)
        .map(|v| v.as_str().map(str::to_owned).ok_or_else(invalid))
        .transpose()
}
fn optional_bool(object: &Map<String, Value>, key: &str) -> Result<Option<bool>, ObservationError> {
    object
        .get(key)
        .map(|v| v.as_bool().ok_or_else(invalid))
        .transpose()
}
fn optional_integer(
    object: &Map<String, Value>,
    key: &str,
    min: f64,
    max: f64,
) -> Result<Option<f64>, ObservationError> {
    object
        .get(key)
        .map(|v| integer(v, min, max).ok_or_else(invalid))
        .transpose()
}
impl ReadToolRequest {
    /// Typed admission after the public SDK schema/error boundary. Unknown
    /// outer fields are stripped; present null is never mistaken for omission.
    pub fn parse(name: &str, args: &Value) -> Result<Self, ObservationError> {
        let object = args.as_object().ok_or_else(invalid)?;
        Ok(match name {
            "obs-get-status" => Self::Status,
            "obs-test-connection" => Self::TestConnection,
            "obs-sleep" => Self::Sleep {
                millis: optional_integer(object, "sleepMillis", 0.0, 50_000.0)?.map(|v| v as u64),
                frames: optional_integer(object, "sleepFrames", 0.0, 10_000.0)?.map(|v| v as u64),
            },
            "obs-get-input-list" => Self::InputList {
                input_kind: optional_string(object, "inputKind")?,
            },
            "obs-get-input-kind-list" => Self::InputKindList {
                unversioned: optional_bool(object, "unversioned")?,
            },
            "obs-get-source-screenshot" => {
                let format = object
                    .get("imageFormat")
                    .and_then(Value::as_str)
                    .filter(|s| {
                        let n = s.chars().count();
                        (1..=16).contains(&n)
                    })
                    .ok_or_else(invalid)?;
                let mut data = Map::new();
                data.insert("imageFormat".into(), json!(format));
                for key in ["canvasUuid", "sourceName", "sourceUuid"] {
                    if let Some(value) = optional_string(object, key)? {
                        data.insert(key.into(), json!(value));
                    }
                }
                for (key, min, max) in [
                    ("imageWidth", 8.0, 4096.0),
                    ("imageHeight", 8.0, 4096.0),
                    ("imageCompressionQuality", -1.0, 100.0),
                ] {
                    if let Some(value) = optional_integer(object, key, min, max)? {
                        data.insert(key.into(), json!(value as i64));
                    }
                }
                Self::Screenshot(ScreenshotArguments {
                    data: Value::Object(data),
                })
            }
            "obs-preflight" => Self::Preflight(preflight::Options {
                min_free_disk_mb: optional_integer(
                    object,
                    "minFreeDiskMb",
                    0.0,
                    9_007_199_254_740_991.0,
                )?
                .unwrap_or(1024.0),
                expect_silent: optional_bool(object, "expectSilent")?.unwrap_or(false),
                require_verified: false,
            }),
            _ => {
                return Err(ObservationError::new(
                    "capability_unavailable",
                    "Unknown custom read-only tool",
                ));
            }
        })
    }
}
#[derive(Clone, Copy, Debug)]
pub struct ReadToolBounds {
    pub deadline: Instant,
    pub rpc: Duration,
}
impl Default for ReadToolBounds {
    fn default() -> Self {
        Self {
            deadline: Instant::now() + Duration::from_secs(60),
            rpc: Duration::from_secs(10),
        }
    }
}
pub struct StatusMetadata {
    pub connection: ConnectionDescriptor,
    pub connected: bool,
    pub identified: bool,
}
pub struct ReadToolContext {
    pub handle: SessionHandle,
    pub reader: SessionReader,
    /// None is unknown metadata, not a known empty unsupported inventory.
    pub capabilities: Option<Arc<HashSet<String>>>,
    pub platform: Option<String>,
    pub connection: ConnectionDescriptor,
    pub clock: Clock,
    pub cancellation: Cancellation,
    pub pending_output_settings: Arc<AtomicBool>,
    pub writable: Option<Arc<LocalHost>>,
    pub screenshot_limit: Result<ScreenshotLimit, ObservationError>,
    pub bounds: ReadToolBounds,
}
fn text(text: impl Into<String>, error: bool) -> Value {
    let text = text.into();
    if error {
        json!({"content":[{"type":"text","text":text}],"isError":true})
    } else {
        let structured = match serde_json::from_str::<Value>(&text) {
            Ok(v) if v.is_object() => v,
            Ok(v) => json!({"result":v}),
            Err(_) => json!({"message":text}),
        };
        json!({"content":[{"type":"text","text":text}],"structuredContent":structured})
    }
}
fn pretty(value: &Value) -> Result<Value, ObservationError> {
    Ok(text(
        serde_json::to_string_pretty(value).map_err(|_| invalid())?,
        false,
    ))
}
fn canceled() -> ObservationError {
    ObservationError::new(
        "cancelled",
        "Operation cancelled; inspect OBS before any further action",
    )
}
fn expired() -> ObservationError {
    ObservationError::new("observation_limit", "Custom read-tool deadline expired")
}
fn checked_clock(clock: &Clock) -> Result<String, ObservationError> {
    let now = clock();
    let actual = clock::from_epoch_duration(Duration::from_millis(now.epoch_ms))
        .map_err(|_| ObservationError::new("clock_unavailable", "Status clock unavailable"))?;
    if actual.utc != now.utc {
        return Err(ObservationError::new(
            "clock_unavailable",
            "Status epoch and UTC do not match",
        ));
    }
    Ok(now.utc)
}
/// Available even when a runtime connection attempt failed. The runtime supplies
/// observed connected/identified facts; this function invents neither state.
pub fn status(metadata: StatusMetadata, clock: Clock) -> Result<Value, ObservationError> {
    #[derive(Serialize)]
    struct Server<'a> {
        name: &'a str,
        version: &'a str,
        status: &'a str,
    }
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Obs {
        connected: bool,
        url: String,
        has_password: bool,
        identified: bool,
    }
    #[derive(Serialize)]
    struct Status<'a> {
        server: Server<'a>,
        obs: Obs,
        timestamp: String,
    }
    let report = Status {
        server: Server {
            name: "obs-mcp",
            version: env!("CARGO_PKG_VERSION"),
            status: "running",
        },
        obs: Obs {
            connected: metadata.connected,
            url: metadata.connection.url,
            has_password: metadata.connection.has_password,
            identified: metadata.identified,
        },
        timestamp: checked_clock(&clock)?,
    };
    Ok(text(
        serde_json::to_string_pretty(&report).map_err(|_| invalid())?,
        false,
    ))
}
struct Scope {
    handle: SessionHandle,
    reader: SessionReader,
    capabilities: Option<Arc<HashSet<String>>>,
    platform: Option<String>,
    hostname: String,
    cancellation: Cancellation,
    pending: Arc<AtomicBool>,
    bounds: ReadToolBounds,
}
impl Scope {
    fn bind(context: &ReadToolContext) -> Result<Self, ObservationError> {
        if context.handle.token() != context.reader.token()
            || context.handle.endpoint_hash() != context.reader.endpoint_hash()
            || format!("{:x}", Sha256::digest(context.connection.url.as_bytes()))
                != context.handle.endpoint_hash()
        {
            return Err(ObservationError::new(
                "observation_invalidated",
                "Read tool does not match its physical session",
            ));
        }
        if context.bounds.rpc.is_zero() || context.bounds.rpc > Duration::from_secs(60) {
            return Err(invalid());
        }
        let uri: tokio_tungstenite::tungstenite::http::Uri =
            context.connection.url.parse().map_err(|_| invalid())?;
        if uri.scheme_str() != Some("ws")
            || uri.authority().is_none_or(|a| a.as_str().contains('@'))
        {
            return Err(invalid());
        }
        let hostname = uri.host().ok_or_else(invalid)?.to_ascii_lowercase();
        Ok(Self {
            handle: context.handle.clone(),
            reader: context.reader.clone(),
            capabilities: context.capabilities.clone(),
            platform: context.platform.clone(),
            hostname,
            cancellation: context.cancellation.clone(),
            pending: context.pending_output_settings.clone(),
            bounds: context.bounds,
        })
    }
    fn check(&self) -> Result<(), ObservationError> {
        if self.cancellation.is_cancelled() {
            return Err(canceled());
        }
        if Instant::now() >= self.bounds.deadline {
            return Err(expired());
        }
        let state = self.reader.evidence();
        if self.handle.invalidation().is_some() || !state.connected || state.cancelled {
            return Err(ObservationError::new(
                "observation_invalidated",
                "OBS connection is unavailable",
            ));
        }
        Ok(())
    }
    fn supports(&self, request: ReadRequest) -> Result<(), ObservationError> {
        if self
            .capabilities
            .as_ref()
            .is_some_and(|set| !set.contains(request.as_str()))
        {
            Err(ObservationError::new(
                "capability_unavailable",
                format!(
                    "OBS WebSocket does not advertise support for request '{}'",
                    request.as_str()
                ),
            ))
        } else {
            Ok(())
        }
    }
    async fn read(&self, request: ReadRequest, data: Value) -> Result<Value, ObservationError> {
        self.check()?;
        self.supports(request)?;
        let mut reader = self.reader.clone();
        let deadline = self.bounds.deadline.min(Instant::now() + self.bounds.rpc);
        reader.synchronize(deadline).await?;
        let result = reader.read(request, data, deadline).await;
        reader.synchronize(deadline).await?;
        self.check()?;
        result
    }
}
impl Adapter for Scope {
    fn connect(&self) -> preflight::ReadFuture<'_, ()> {
        Box::pin(async move {
            self.check().map_err(|e| ReadError::new(e.message))?;
            let mut reader = self.reader.clone();
            reader
                .synchronize(self.bounds.deadline)
                .await
                .map_err(|e| ReadError::new(e.message))
        })
    }
    fn connection_context(&self) -> ConnectionContext {
        ConnectionContext {
            hostname: Some(self.hostname.clone()),
            platform: self.platform.clone(),
            pending_output_settings: self.pending.load(Ordering::Acquire),
        }
    }
    fn read(&self, request: ReadRequest, data: Option<Value>) -> preflight::ReadFuture<'_, Value> {
        Box::pin(async move {
            Scope::read(self, request, data.unwrap_or(Value::Null))
                .await
                .map_err(|e| ReadError::new(e.message))
        })
    }
    fn batch(
        &self,
        calls: Vec<preflight::Call>,
    ) -> preflight::ReadFuture<'_, Vec<preflight::BatchResult>> {
        Box::pin(async move {
            self.check().map_err(|e| ReadError::new(e.message))?;
            for call in &calls {
                self.supports(call.request)
                    .map_err(|e| ReadError::new(e.message))?;
            }
            if calls.is_empty() {
                return Ok(vec![]);
            }
            let deadline = self.bounds.deadline.min(Instant::now() + self.bounds.rpc);
            let mut reader = self.reader.clone();
            reader
                .synchronize(deadline)
                .await
                .map_err(|e| ReadError::new(e.message))?;
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or_else(|| ReadError::new(expired().message))?;
            let cancel = self.cancellation.clone();
            let result = self
                .handle
                .batch_checked(
                    self.handle.token(),
                    calls
                        .into_iter()
                        .map(|call| BatchRequest {
                            request_type: call.request.as_str().into(),
                            request_data: Some(call.data),
                        })
                        .collect(),
                    BatchOptions {
                        timeout: Some(remaining),
                        ..Default::default()
                    },
                    move || {
                        if cancel.is_cancelled() || Instant::now() >= deadline {
                            Err("Preflight batch permission expired".into())
                        } else {
                            Ok(())
                        }
                    },
                )
                .await;
            reader
                .synchronize(deadline)
                .await
                .map_err(|e| ReadError::new(e.message))?;
            self.check().map_err(|e| ReadError::new(e.message))?;
            result
                .map_err(|error| {
                    ReadError::new(
                        error
                            .obs_failure_message("RequestBatch")
                            .unwrap_or_else(|| "Preflight batch evidence unavailable".into()),
                    )
                })?
                .into_iter()
                .map(|row| {
                    Ok(preflight::BatchResult {
                        ok: row.ok,
                        code: Some(i64::try_from(row.code).map_err(|_| {
                            ReadError::new("OBS batch code exceeds native projection")
                        })?),
                        response_data: row.response_data,
                    })
                })
                .collect()
        })
    }
}
fn numeric(value: Option<&Value>) -> Result<f64, ObservationError> {
    fn string(value: &str) -> Result<f64, ObservationError> {
        let value =
            value.trim_matches(|c: char| c == '\u{feff}' || (c.is_whitespace() && c != '\u{85}'));
        if value.is_empty() {
            return Ok(0.0);
        }
        for (prefix, radix) in [
            ("0x", 16),
            ("0X", 16),
            ("0o", 8),
            ("0O", 8),
            ("0b", 2),
            ("0B", 2),
        ] {
            if let Some(digits) = value.strip_prefix(prefix) {
                return match u128::from_str_radix(digits, radix) {
                    Ok(n) => Ok(n as f64),
                    Err(_) if !digits.is_empty() && digits.chars().all(|c| c.is_digit(radix)) => {
                        Err(ObservationError::new(
                            "native_projection_unavailable",
                            "Oversized radix FPS value is unqualified",
                        ))
                    }
                    Err(_) => Ok(f64::NAN),
                };
            }
        }
        if !value
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'+' | b'-' | b'e' | b'E'))
        {
            return Ok(f64::NAN);
        }
        Ok(value.parse().unwrap_or(f64::NAN))
    }
    fn primitive_error() -> ObservationError {
        ObservationError::new(
            "evidence_unavailable",
            "Cannot convert object to primitive value",
        )
    }
    fn joined(value: &Value) -> Result<String, ObservationError> {
        Ok(match value {
            Value::Null => String::new(),
            Value::Array(v) => v
                .iter()
                .map(joined)
                .collect::<Result<Vec<_>, _>>()?
                .join(","),
            Value::String(v) => v.clone(),
            Value::Bool(v) => v.to_string(),
            Value::Number(v) => v.to_string(),
            Value::Object(v) if v.contains_key("toString") => return Err(primitive_error()),
            Value::Object(_) => "[object Object]".into(),
        })
    }
    match value {
        None => Ok(f64::NAN),
        Some(Value::Null) => Ok(0.0),
        Some(Value::Bool(v)) => Ok(f64::from(u8::from(*v))),
        Some(Value::Number(v)) => Ok(v.as_f64().unwrap_or(f64::NAN)),
        Some(Value::String(v)) => string(v),
        Some(v @ Value::Array(_)) => string(&joined(v)?),
        Some(Value::Object(v)) if v.contains_key("toString") => Err(primitive_error()),
        Some(Value::Object(_)) => Ok(f64::NAN),
    }
}
/// A pure local wait shared by the runtime and connected read-tool lane. It
/// creates no OBS request/task; cancellation ends this owned timer promptly.
pub async fn sleep_millis(millis: u64, cancellation: Cancellation, deadline: Instant) -> Value {
    if millis > 50_000 {
        return text(
            "Error during sleep operation: Sleep cannot exceed 50000ms",
            true,
        );
    }
    let finish = Instant::now() + Duration::from_millis(millis.max(1));
    let result = tokio::select! {
        ()=cancellation.cancelled()=>Err(canceled()),
        ()=sleep_until(finish.min(deadline))=>if cancellation.is_cancelled(){Err(canceled())}else if Instant::now()>=deadline{Err(expired())}else{Ok(())},
    };
    match result {
        Ok(()) => text(format!("Slept for {millis}ms"), false),
        Err(error) => text(
            format!("Error during sleep operation: {}", error.message),
            true,
        ),
    }
}
pub fn sleep_selection_error() -> Value {
    text("Provide exactly one of sleepMillis or sleepFrames", true)
}
pub async fn run(
    context: ReadToolContext,
    request: ReadToolRequest,
) -> Result<Value, ObservationError> {
    if let ReadToolRequest::Sleep { millis, frames } = &request {
        if millis.is_some() == frames.is_some() {
            return Ok(sleep_selection_error());
        }
        if let Some(millis) = millis {
            return Ok(sleep_millis(*millis, context.cancellation, context.bounds.deadline).await);
        }
    }
    let scope = Scope::bind(&context)?;
    match request {
        ReadToolRequest::Status => {
            let connected = scope.handle.invalidation().is_none();
            status(
                StatusMetadata {
                    connection: context.connection,
                    connected,
                    identified: connected,
                },
                context.clock,
            )
        }
        ReadToolRequest::TestConnection => {
            Ok(match scope.read(ReadRequest::Version, Value::Null).await {
                Ok(_) => text("Connection test successful - OBS is responding", false),
                Err(e) => text(format!("Connection test failed: {}", e.message), true),
            })
        }
        ReadToolRequest::InputList { input_kind } => {
            let mut data = json!({});
            if let Some(kind) = input_kind {
                data["inputKind"] = json!(kind);
            }
            Ok(match scope.read(ReadRequest::InputList, data).await {
                Ok(v) => pretty(&v)?,
                Err(e) => text(format!("Error getting input list: {}", e.message), true),
            })
        }
        ReadToolRequest::InputKindList { unversioned } => {
            let mut data = json!({});
            if let Some(value) = unversioned {
                data["unversioned"] = json!(value);
            }
            Ok(match scope.read(ReadRequest::InputKindList, data).await {
                Ok(v) => pretty(&v)?,
                Err(e) => text(
                    format!("Error getting input kind list: {}", e.message),
                    true,
                ),
            })
        }
        ReadToolRequest::Screenshot(arguments) => {
            let result = scope
                .read(ReadRequest::SourceScreenshot, arguments.data)
                .await
                .and_then(|response| {
                    context
                        .screenshot_limit
                        .and_then(|limit| screenshot_content(&response, limit))
                });
            Ok(match result {
                Ok(image) => {
                    let payload = image["data"].as_str().expect("admitted image");
                    let padding = payload.bytes().rev().take_while(|b| *b == b'=').count();
                    let size = payload.len() / 4 * 3 - padding;
                    let mime = image["mimeType"].as_str().expect("admitted MIME");
                    json!({"content":[{"type":"text","text":format!("OBS screenshot ({mime}, {size} bytes)")},image],"structuredContent":{"mimeType":mime,"sizeBytes":size}})
                }
                Err(e) => text(
                    format!("Error getting source screenshot: {}", e.message),
                    true,
                ),
            })
        }
        ReadToolRequest::Sleep { millis, frames } => {
            if millis.is_some() == frames.is_some() {
                return Ok(text(
                    "Provide exactly one of sleepMillis or sleepFrames",
                    true,
                ));
            }
            let result = async {
                scope.check()?;
                let millis = if let Some(frames) = frames {
                    let video = scope.read(ReadRequest::VideoSettings, Value::Null).await?;
                    let fps =
                        numeric(video.get("fpsNumerator"))? / numeric(video.get("fpsDenominator"))?;
                    if !fps.is_finite() || fps <= 0.0 {
                        return Err(ObservationError::new(
                            "evidence_unavailable",
                            "OBS reported an invalid frame rate",
                        ));
                    }
                    ((frames as f64 / fps) * 1000.0).round()
                } else {
                    millis.expect("exclusive duration") as f64
                };
                if millis > 50_000.0 {
                    return Err(ObservationError::new(
                        "evidence_unavailable",
                        "Sleep cannot exceed 50000ms",
                    ));
                }
                Ok(millis as u64)
            }
            .await;
            match result {
                Ok(millis) => {
                    Ok(
                        sleep_millis(millis, scope.cancellation.clone(), scope.bounds.deadline)
                            .await,
                    )
                }
                Err(e) if e.reason == "native_projection_unavailable" => Err(e),
                Err(e) => Ok(text(
                    format!("Error during sleep operation: {}", e.message),
                    true,
                )),
            }
        }
        ReadToolRequest::Preflight(options) => {
            let host=context.writable.ok_or_else(||ObservationError::new("capability_unavailable","Native preflight requires an explicitly approved recording root for its writable-directory probe"))?;
            let report = preflight::run_preflight(&scope, host.as_ref(), &options).await;
            // All paired/concurrent check futures have settled before either
            // the report or a cancellation is returned to the service owner.
            if scope.cancellation.is_cancelled() {
                return Err(canceled());
            }
            Ok(report.tool_result())
        }
    }
}
