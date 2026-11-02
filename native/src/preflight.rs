// SPDX-License-Identifier: GPL-2.0-only
//! Injected, read-only port of src/tools/preflight.ts and its audio-only helper.
//! No connection, batch transport, filesystem probe, or recording custody is
//! implemented here. The adapter owns deadlines and cancellation of every read.
//! All concurrent checks are retained until settled, with no spawned tasks.
//! Unknown checks do not themselves make ready=false; strict take admission
//! additionally owns the DEMO_REQUIRED_CHECKS requirement in takes.ts.
use crate::observation::ReadRequest;
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::HashMap, fmt, future::Future, pin::Pin, sync::Mutex};

pub const DEMO_REQUIRED_CHECKS: [&str; 7] = [
    "connection",
    "record-idle",
    "record-directory",
    "disk-space",
    "encoder",
    "audio",
    "scene-items",
];
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Pass,
    Warn,
    Fail,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Check {
    pub id: String,
    pub status: Status,
    pub message: String,
}
fn check(id: &str, status: Status, message: impl Into<String>) -> Check {
    Check {
        id: id.into(),
        status,
        message: message.into(),
    }
}
#[derive(Clone, Debug)]
pub struct Options {
    pub min_free_disk_mb: f64,
    pub expect_silent: bool,
    pub require_verified: bool,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            min_free_disk_mb: 1024.0,
            expect_silent: false,
            require_verified: false,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Report {
    pub ready: bool,
    pub checks: Vec<Check>,
}
impl Report {
    pub fn tool_result(&self) -> Value {
        let lines = self
            .checks
            .iter()
            .map(|check| {
                let mark = match check.status {
                    Status::Pass => "ok  ",
                    Status::Warn => "warn",
                    Status::Fail => "FAIL",
                    Status::Unknown => "?   ",
                };
                format!("{mark} {}: {}", check.id, check.message)
            })
            .collect::<Vec<_>>()
            .join("\n");
        json!({"content":[{"type":"text","text":format!("{}\n{lines}",if self.ready {"Ready to record"} else {"Not ready to record"})}],"structuredContent":self})
    }
}
#[derive(Clone, PartialEq, Eq)]
pub struct ReadError {
    pub message: String,
}
impl ReadError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
    pub fn tool_message(&self) -> &str {
        &self.message
    }
}
impl fmt::Debug for ReadError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("PreflightReadError")
            .field("message_bytes", &self.message.len())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for ReadError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("Preflight read unavailable")
    }
}
impl std::error::Error for ReadError {}
pub type ReadFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ReadError>> + Send + 'a>>;
#[derive(Clone, Debug)]
pub struct ConnectionContext {
    /// Canonical hostname from the provider's URL parser, never a raw URL.
    /// The current Rust session has no WHATWG URL adapter; supplying this
    /// classification remains an explicit concrete-adapter obligation.
    pub hostname: Option<String>,
    pub platform: Option<String>,
    pub pending_output_settings: bool,
}
impl ConnectionContext {
    pub fn is_local(&self) -> bool {
        self.hostname
            .as_deref()
            .is_some_and(|host| ["localhost", "127.0.0.1", "::1", "[::1]"].contains(&host))
    }
}
#[derive(Clone, Debug)]
pub struct Call {
    pub request: ReadRequest,
    pub data: Value,
}
#[derive(Clone, Debug)]
pub struct BatchResult {
    pub ok: bool,
    pub code: Option<i64>,
    pub response_data: Value,
}
/// This seam deliberately requires real batch results (including code604).
/// Splitting a batch into unrelated reads and losing rejection codes is not a
/// qualified implementation. Nothing in this module promotes a native route.
pub trait Adapter: Sync {
    fn connect(&self) -> ReadFuture<'_, ()>;
    fn connection_context(&self) -> ConnectionContext;
    fn read(&self, request: ReadRequest, data: Option<Value>) -> ReadFuture<'_, Value>;
    fn batch(&self, calls: Vec<Call>) -> ReadFuture<'_, Vec<BatchResult>>;
}
/// Must perform an effective writable-directory check, including file type and
/// access/ACL semantics. Permission bits or metadata.readonly() are insufficient.
/// Remote OBS paths are never sent to this local-host probe.
pub trait WritableDirectoryProbe: Sync {
    fn check(&self, path: &str) -> Result<(), String>;
}
async fn object_read(
    adapter: &impl Adapter,
    request: ReadRequest,
    data: Option<Value>,
) -> Result<Value, ReadError> {
    let value = adapter.read(request, data).await?;
    Ok(if value.is_object() { value } else { json!({}) })
}
async fn parameter(
    adapter: &impl Adapter,
    category: &str,
    name: &str,
) -> Result<Option<String>, ReadError> {
    let response = object_read(
        adapter,
        ReadRequest::ProfileParameter,
        Some(json!({"parameterCategory":category,"parameterName":name})),
    )
    .await?;
    Ok(response
        .get("parameterValue")
        .filter(|v| !v.is_null())
        .or_else(|| response.get("defaultParameterValue"))
        .and_then(Value::as_str)
        .map(str::to_owned))
}
/// Retain both read futures rather than detaching a failed Promise.all sibling.
/// The first observed error is retained; all admitted siblings settle before
/// this read-only scope returns. Adapters must bound their own read lifetimes.
async fn pair<A, B>(
    a: impl Future<Output = Result<A, ReadError>>,
    b: impl Future<Output = Result<B, ReadError>>,
) -> Result<(A, B), ReadError> {
    let first = Mutex::new(None);
    let a = async {
        let result = a.await;
        if let Err(error) = &result {
            first.lock().unwrap().get_or_insert_with(|| error.clone());
        }
        result
    };
    let b = async {
        let result = b.await;
        if let Err(error) = &result {
            first.lock().unwrap().get_or_insert_with(|| error.clone());
        }
        result
    };
    let (a, b) = futures_util::future::join(a, b).await;
    if let Some(error) = first.into_inner().unwrap() {
        return Err(error);
    }
    Ok((a?, b?))
}
async fn checked(
    id: &str,
    future: impl Future<Output = Result<Vec<Check>, ReadError>>,
) -> Vec<Check> {
    future.await.unwrap_or_else(|error| {
        vec![check(
            id,
            Status::Unknown,
            format!("Could not check: {}", error.tool_message()),
        )]
    })
}
pub async fn run_preflight(
    adapter: &impl Adapter,
    host: &impl WritableDirectoryProbe,
    options: &Options,
) -> Report {
    if let Err(error) = adapter.connect().await {
        return Report {
            ready: false,
            checks: vec![check(
                "connection",
                Status::Fail,
                format!("Cannot reach OBS: {}", error.tool_message()),
            )],
        };
    }
    let (idle, directory, disk, encoder, audio, scene) = futures_util::join!(
        checked("record-idle", outputs_idle(adapter)),
        checked("record-directory", record_directory(adapter, host)),
        checked("disk-space", disk_space(adapter, options.min_free_disk_mb)),
        checked("encoder", encoder(adapter)),
        checked(
            "audio",
            audio(adapter, options.expect_silent, options.require_verified)
        ),
        checked("scene-items", scene_items(adapter)),
    );
    let mut checks = vec![check("connection", Status::Pass, "Connected to OBS")];
    for group in [idle, directory, disk, encoder, audio, scene] {
        checks.extend(group);
    }
    checks.push(check("settings-dialog",Status::Unknown,"OBS cannot report open Settings or error dialogs over WebSocket. If a start is accepted but never becomes active, close OBS Settings or dismiss its error dialog"));
    Report {
        ready: !checks.iter().any(|check| check.status == Status::Fail),
        checks,
    }
}
async fn outputs_idle(adapter: &impl Adapter) -> Result<Vec<Check>, ReadError> {
    let record = object_read(adapter, ReadRequest::RecordStatus, None).await?;
    Ok(vec![
        match record.get("outputActive").and_then(Value::as_bool) {
            None => check(
                "record-idle",
                Status::Unknown,
                "OBS did not report recording state",
            ),
            Some(true) => check("record-idle", Status::Fail, "Recording is already active"),
            Some(false) => check("record-idle", Status::Pass, "Recording is not active"),
        },
    ])
}
async fn record_directory(
    adapter: &impl Adapter,
    host: &impl WritableDirectoryProbe,
) -> Result<Vec<Check>, ReadError> {
    let record = object_read(adapter, ReadRequest::RecordDirectory, None).await?;
    let Some(path) = record
        .get("recordDirectory")
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty())
    else {
        return Ok(vec![check(
            "record-directory",
            Status::Fail,
            "OBS has no recording directory set",
        )]);
    };
    if !adapter.connection_context().is_local() {
        return Ok(vec![check(
            "record-directory",
            Status::Unknown,
            format!("OBS records to {path}; OBS is remote, so the path was not checked"),
        )]);
    }
    Ok(vec![match host.check(path) {
        Ok(()) => check(
            "record-directory",
            Status::Pass,
            format!("Recording to {path}"),
        ),
        Err(error) => check(
            "record-directory",
            Status::Fail,
            format!("Recording directory {path} is not a writable directory ({error})"),
        ),
    }])
}
async fn disk_space(adapter: &impl Adapter, minimum: f64) -> Result<Vec<Check>, ReadError> {
    let stats = object_read(adapter, ReadRequest::Stats, None).await?;
    let Some(free) = stats.get("availableDiskSpace").and_then(Value::as_f64) else {
        return Ok(vec![check(
            "disk-space",
            Status::Unknown,
            "OBS did not report free disk space",
        )]);
    };
    let free = (free + 0.5).floor();
    Ok(vec![if free < minimum {
        check(
            "disk-space",
            Status::Fail,
            format!(
                "{} MB free, below the {} MB minimum",
                number_string(free),
                number_string(minimum)
            ),
        )
    } else {
        check(
            "disk-space",
            Status::Pass,
            format!("{} MB free", number_string(free)),
        )
    }])
}
async fn record_encoder(adapter: &impl Adapter) -> Result<(String, Option<String>), ReadError> {
    let mode = parameter(adapter, "Output", "Mode")
        .await?
        .unwrap_or("Simple".into());
    let encoder = if mode == "Advanced" {
        let encoder = parameter(adapter, "AdvOut", "RecEncoder").await?;
        if encoder
            .as_deref()
            .is_none_or(|encoder| encoder.is_empty() || encoder == "none")
        {
            parameter(adapter, "AdvOut", "Encoder").await?
        } else {
            encoder
        }
    } else {
        let quality = parameter(adapter, "SimpleOutput", "RecQuality").await?;
        parameter(
            adapter,
            "SimpleOutput",
            if quality.as_deref() == Some("Stream") {
                "StreamEncoder"
            } else {
                "RecEncoder"
            },
        )
        .await?
    };
    Ok((mode, encoder))
}
async fn encoder(adapter: &impl Adapter) -> Result<Vec<Check>, ReadError> {
    let (mode, encoder) = record_encoder(adapter).await?;
    let context = adapter.connection_context();
    let mut checks = vec![match encoder.filter(|encoder| !encoder.is_empty()) {
        None => check(
            "encoder",
            Status::Unknown,
            format!("{mode} output mode has no recording encoder set"),
        ),
        Some(encoder) => {
            let lower = encoder.to_ascii_lowercase();
            if context.platform.as_deref() == Some("macos")
                && ["nvenc", "qsv", "amd", "amf", "jim_"]
                    .iter()
                    .any(|part| lower.contains(part))
            {
                check(
                    "encoder",
                    Status::Fail,
                    format!("Recording encoder {encoder} is not available on macOS"),
                )
            } else if context
                .platform
                .as_deref()
                .is_some_and(|platform| !platform.is_empty() && platform != "macos")
                && ["apple", "videotoolbox"]
                    .iter()
                    .any(|part| lower.contains(part))
            {
                check(
                    "encoder",
                    Status::Fail,
                    format!("Recording encoder {encoder} is only available on macOS"),
                )
            } else {
                check(
                    "encoder",
                    Status::Pass,
                    format!("{mode} output mode records with {encoder}"),
                )
            }
        }
    }];
    if context.pending_output_settings {
        checks.push(check("output-settings",Status::Fail,"Output settings were changed through this server since OBS last started; OBS will ignore them (or fail to start) until it rebuilds its outputs. Restart OBS first"));
    }
    Ok(checks)
}
pub async fn recorded_tracks(
    adapter: &impl Adapter,
    require_verified: bool,
) -> Result<Vec<u8>, ReadError> {
    let reported = parameter(adapter, "Output", "Mode").await?;
    if require_verified && !matches!(reported.as_deref(), Some("Simple" | "Advanced")) {
        return Err(ReadError::new("Recording output mode is unavailable"));
    }
    let mode = reported.as_deref().unwrap_or("Simple");
    let value = if mode == "Advanced" {
        let record_type = parameter(adapter, "AdvOut", "RecType").await?;
        if require_verified && !matches!(record_type.as_deref(), Some("Standard" | "FFmpeg")) {
            return Err(ReadError::new("Recording output type is unavailable"));
        }
        parameter(
            adapter,
            "AdvOut",
            if record_type.as_deref() == Some("FFmpeg") {
                "FFAudioMixes"
            } else {
                "RecTracks"
            },
        )
        .await?
    } else {
        let (quality, format) = pair(
            parameter(adapter, "SimpleOutput", "RecQuality"),
            parameter(adapter, "SimpleOutput", "RecFormat2"),
        )
        .await?;
        if quality.as_deref() == Some("Stream") || format.as_deref() == Some("flv") {
            return Ok(vec![1]);
        }
        if require_verified
            && (quality.as_deref().is_none_or(str::is_empty)
                || format.as_deref().is_none_or(str::is_empty))
        {
            return Err(ReadError::new("Recording format or quality is unavailable"));
        }
        parameter(adapter, "SimpleOutput", "RecTracks").await?
    };
    let mask = number(value.as_deref().unwrap_or("1"));
    if require_verified
        && (value
            .as_deref()
            .is_none_or(|value| js_trim(value).is_empty())
            || !mask.is_finite()
            || mask.fract() != 0.0
            || !(1.0..=63.0).contains(&mask))
    {
        return Err(ReadError::new(
            "Recorded audio track selection is unavailable",
        ));
    }
    // Only six low bits are inspected. This is exactly ToInt32(mask)'s six
    // low bits, including fractional, negative, huge and nonfinite masks.
    let low = if mask.is_finite() {
        mask.trunc().rem_euclid(64.0) as u8
    } else {
        0
    };
    let tracks = (1..=6)
        .filter(|track| low & (1 << (track - 1)) != 0)
        .collect::<Vec<_>>();
    Ok(if tracks.is_empty() { vec![1] } else { tracks })
}
async fn audio(
    adapter: &impl Adapter,
    expect_silent: bool,
    require_verified: bool,
) -> Result<Vec<Check>, ReadError> {
    let (inputs, tracks) = pair(
        object_read(adapter, ReadRequest::InputList, None),
        recorded_tracks(adapter, require_verified),
    )
    .await?;
    let Some(inputs) = inputs
        .get("inputs")
        .and_then(Value::as_array)
        .filter(|inputs| {
            inputs.iter().all(|input| {
                input.is_object()
                    && input
                        .get("inputName")
                        .and_then(Value::as_str)
                        .is_some_and(|name| !name.is_empty())
            })
        })
    else {
        return Ok(vec![check(
            "audio",
            Status::Unknown,
            "OBS did not report a complete input list",
        )]);
    };
    let names = inputs
        .iter()
        .map(|input| input["inputName"].as_str().unwrap())
        .collect::<Vec<_>>();
    let results = adapter
        .batch(
            names
                .iter()
                .flat_map(|name| {
                    [
                        Call {
                            request: ReadRequest::InputMute,
                            data: json!({"inputName":name}),
                        },
                        Call {
                            request: ReadRequest::InputAudioTracks,
                            data: json!({"inputName":name}),
                        },
                    ]
                })
                .collect(),
        )
        .await?;
    let (mut audible, mut unreadable) = (vec![], vec![]);
    for (index, name) in names.iter().enumerate() {
        let mute = results.get(index * 2);
        if !mute.is_some_and(|result| result.ok) {
            if mute.and_then(|result| result.code) != Some(604) {
                unreadable.push(*name);
            }
            continue;
        }
        let Some(muted) = mute
            .unwrap()
            .response_data
            .get("inputMuted")
            .filter(|_| mute.unwrap().response_data.is_object())
            .and_then(Value::as_bool)
        else {
            unreadable.push(*name);
            continue;
        };
        if muted {
            continue;
        }
        let route = results
            .get(index * 2 + 1)
            .filter(|result| result.ok && result.response_data.is_object())
            .and_then(|result| result.response_data.get("inputAudioTracks"))
            .and_then(Value::as_object);
        let Some(route) = route.filter(|route| {
            tracks
                .iter()
                .all(|track| route.get(&track.to_string()).is_some_and(Value::is_boolean))
        }) else {
            unreadable.push(*name);
            if !require_verified {
                audible.push(format!(
                    "{name} (audio routing unavailable; assumed recorded)"
                ));
            }
            continue;
        };
        let enabled = tracks
            .iter()
            .filter(|track| route.get(&track.to_string()) == Some(&Value::Bool(true)))
            .map(u8::to_string)
            .collect::<Vec<_>>();
        if !enabled.is_empty() {
            audible.push(format!("{name} (track {})", enabled.join(", ")));
        }
    }
    Ok(vec![if audible.is_empty() {
        if unreadable.is_empty() {
            check(
                "audio",
                Status::Pass,
                "No unmuted input feeds a recorded audio track",
            )
        } else {
            check(
                "audio",
                Status::Unknown,
                format!("Could not read audio state for: {}", unreadable.join(", ")),
            )
        }
    } else {
        check(
            "audio",
            if expect_silent {
                Status::Fail
            } else {
                Status::Warn
            },
            format!(
                "Recorded audio comes from: {}. macOS screen_capture inputs carry application audio; mute them or clear their tracks for a silent recording",
                audible.join("; ")
            ),
        )
    }])
}
async fn scene_items(adapter: &impl Adapter) -> Result<Vec<Check>, ReadError> {
    let program = object_read(adapter, ReadRequest::CurrentProgramScene, None).await?;
    let Some(scene) = program
        .get("currentProgramSceneName")
        .filter(|value| !value.is_null())
        .or_else(|| program.get("sceneName"))
        .and_then(Value::as_str)
    else {
        return Ok(vec![check(
            "scene-items",
            Status::Unknown,
            "OBS did not report the program scene",
        )]);
    };
    let response = object_read(
        adapter,
        ReadRequest::SceneItemList,
        Some(json!({"sceneName":scene})),
    )
    .await?;
    let Some(items) = response
        .get("sceneItems")
        .and_then(Value::as_array)
        .filter(|items| {
            items.iter().all(|item| {
                item.is_object() && item.get("sceneItemEnabled").is_some_and(Value::is_boolean)
            })
        })
    else {
        return Ok(vec![check(
            "scene-items",
            Status::Unknown,
            "OBS did not report a complete scene item list",
        )]);
    };
    let visible = items
        .iter()
        .filter(|item| item["sceneItemEnabled"] != false)
        .collect::<Vec<_>>();
    let missing = visible
        .iter()
        .enumerate()
        .filter(|(_, item)| {
            !item.get("sceneItemTransform").is_some_and(Value::is_object)
                && item.get("sceneItemId").is_some_and(Value::is_number)
        })
        .collect::<Vec<_>>();
    let looked = adapter
        .batch(
            missing
                .iter()
                .map(|(_, item)| Call {
                    request: ReadRequest::SceneItemTransform,
                    data: json!({"sceneName":scene,"sceneItemId":item["sceneItemId"]}),
                })
                .collect(),
        )
        .await?;
    let mut transforms = vec![None; visible.len()];
    for (index, (position, _)) in missing.iter().enumerate() {
        transforms[*position] = looked
            .get(index)
            .filter(|result| result.ok && result.response_data.is_object())
            .and_then(|result| result.response_data.get("sceneItemTransform"));
    }
    if visible.is_empty() && !items.is_empty() {
        return Ok(vec![check(
            "scene-items",
            Status::Warn,
            format!("Program scene {scene} has no enabled items"),
        )]);
    }
    if visible.iter().enumerate().any(|(index, item)| {
        item_transform(item, transforms[index]).is_none_or(|transform| {
            !transform.is_object()
                || ["sourceWidth", "sourceHeight"].iter().any(|size| {
                    transform
                        .get(size)
                        .and_then(Value::as_f64)
                        .is_none_or(|size| !size.is_finite() || size < 0.0)
                })
        })
    }) {
        return Ok(vec![check(
            "scene-items",
            Status::Unknown,
            "OBS did not report dimensions for every visible scene item",
        )]);
    }
    let blank = visible
        .iter()
        .enumerate()
        .filter(|(index, item)| {
            item_transform(item, transforms[*index]).is_some_and(|transform| {
                ["sourceWidth", "sourceHeight"]
                    .iter()
                    .any(|key| transform.get(key).and_then(Value::as_f64) == Some(0.0))
            })
        })
        .map(|(_, item)| {
            (
                js_string(
                    item.get("sourceName")
                        .filter(|value| !value.is_null())
                        .or_else(|| item.get("sceneItemId")),
                ),
                item.get("inputKind").cloned().unwrap_or(Value::Null),
            )
        })
        .collect::<Vec<_>>();
    let blank = without_audio_only(adapter, &blank).await;
    Ok(vec![if items.is_empty() {
        check(
            "scene-items",
            Status::Warn,
            format!("Program scene {scene} is empty"),
        )
    } else if !blank.is_empty() {
        check(
            "scene-items",
            Status::Warn,
            format!(
                "Visible items in {scene} render at 0×0: {}. A screen capture usually needs a display or window selected (and screen recording permission)",
                blank.join(", ")
            ),
        )
    } else {
        check(
            "scene-items",
            Status::Pass,
            format!("{} item(s) in {scene} have a size", items.len()),
        )
    }])
}
fn item_transform<'a>(item: &'a Value, fallback: Option<&'a Value>) -> Option<&'a Value> {
    item.get("sceneItemTransform")
        .filter(|value| value.is_object())
        .or(fallback)
}
pub fn renders_no_video(kind: &Value, settings: Option<&Value>) -> bool {
    let Some(kind) = kind.as_str() else {
        return false;
    };
    if kind == "sck_audio_capture"
        || kind.starts_with("jack_")
        || kind == "input_capture"
        || kind == "output_capture"
        || kind.ends_with("_input_capture")
        || kind.ends_with("_output_capture")
    {
        return true;
    }
    let Some(path) = settings
        .and_then(|settings| settings.get("local_file"))
        .and_then(Value::as_str)
    else {
        return false;
    };
    let path = path.to_ascii_lowercase();
    matches!(kind, "ffmpeg_source" | "vlc_source")
        && [
            "wav", "mp3", "m4a", "aac", "flac", "ogg", "oga", "opus", "aif", "aiff", "wma", "caf",
        ]
        .iter()
        .any(|extension| path.ends_with(&format!(".{extension}")))
}
async fn without_audio_only(adapter: &impl Adapter, blank: &[(String, Value)]) -> Vec<String> {
    let media = blank
        .iter()
        .filter(|(_, kind)| matches!(kind.as_str(), Some("ffmpeg_source" | "vlc_source")))
        .collect::<Vec<_>>();
    let results = adapter
        .batch(
            media
                .iter()
                .map(|(name, _)| Call {
                    request: ReadRequest::InputSettings,
                    data: json!({"inputName":name}),
                })
                .collect(),
        )
        .await
        .unwrap_or_default();
    let mut settings = HashMap::new();
    for (index, (name, _)) in media.iter().enumerate() {
        settings.insert(
            name.as_str(),
            results
                .get(index)
                .filter(|result| result.ok && result.response_data.is_object())
                .and_then(|result| result.response_data.get("inputSettings"))
                .filter(|settings| settings.is_object()),
        );
    }
    blank
        .iter()
        .filter(|(name, kind)| {
            !renders_no_video(kind, settings.get(name.as_str()).copied().flatten())
        })
        .map(|(name, _)| name.clone())
        .collect()
}
fn js_trim(value: &str) -> &str {
    value.trim_matches(|c: char| c == '\u{feff}' || (c.is_whitespace() && c != '\u{85}'))
}
fn number(value: &str) -> f64 {
    let value = js_trim(value);
    if value.is_empty() {
        return 0.0;
    }
    for (prefix, radix) in [
        ("0x", 16),
        ("0X", 16),
        ("0b", 2),
        ("0B", 2),
        ("0o", 8),
        ("0O", 8),
    ] {
        if let Some(digits) = value.strip_prefix(prefix) {
            // Parse once as an integer, then round once to f64. Accumulating
            // floating digits double-rounds values such as 0x200000000000011,
            // changing the six track bits. Beyond u128, valid integers have
            // zero low six f64 bits; NaN has that same fallback/refusal outcome
            // in this private track-mask parser. Invalid digits do as well.
            return u128::from_str_radix(digits, radix).map_or(f64::NAN, |n| n as f64);
        }
    }
    if matches!(
        value,
        "inf" | "+inf" | "-inf" | "infinity" | "+infinity" | "-infinity"
    ) {
        f64::NAN
    } else {
        value.parse().unwrap_or(f64::NAN)
    }
}
fn number_string(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value == f64::INFINITY {
        "Infinity".into()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".into()
    } else if value == 0.0 {
        "0".into()
    } else if value.abs() >= 1e21 || value.abs() < 1e-6 {
        let scientific = format!("{value:e}");
        let (n, e) = scientific.split_once('e').unwrap();
        format!("{n}e{}{e}", if e.starts_with('-') { "" } else { "+" })
    } else {
        value.to_string()
    }
}
fn js_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".into(),
        Some(Value::Null) => "null".into(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => number_string(value.as_f64().unwrap_or(f64::NAN)),
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
        Some(Value::Object(_)) => "[object Object]".into(),
    }
}
