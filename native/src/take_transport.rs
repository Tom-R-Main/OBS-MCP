// SPDX-License-Identifier: GPL-2.0-only
//! Take I/O bound to one externally owned physical OBS session. This adapter
//! owns no socket, event consumer, journal writer or spawned worker. Retain its
//! returned futures in the take supervisor through cancellation and cleanup.
use crate::{
    media_verification::{
        self, LocalMediaInput, ProcessOptions, StdoutSink, local_media_input_options,
    },
    observation::ReadRequest,
    preflight::{self, Adapter, ConnectionContext, ReadError, ReadFuture},
    session::{BatchOptions, BatchRequest, ConnectionToken, RequestError, SessionHandle},
    take_monitor::Summary,
    takes::{
        self, ClipCheck, DispatchError, DispatchGuard, Effect, IoFuture, MonitorSetup, TakeIo,
    },
};
use base64::{
    Engine as _, alphabet,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::{self, Metadata, OpenOptions},
    future::Future,
    io::Write,
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

pub type EventFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
/// A non-owning event fence supplied by the combined service. Synchronization
/// must wait until the take actor has APPLIED every event through the intrinsic
/// socket watermark, not merely until a producer queued a delivery barrier.
pub trait EventFence: Send + Sync + 'static {
    fn token(&self) -> ConnectionToken;
    fn ready_at_cursor(&self, cursor: u64) -> bool;
    /// Structural revision excludes meter samples, but includes program,
    /// collection, profile, pause/stop and other scene-evidence invalidations.
    fn picture_revision(&self) -> u64;
    fn synchronize(&self, deadline: Instant) -> EventFuture<'_>;
}
/// Local filesystem/media authority is explicit and separate from transport.
/// An implementation must validate every path against its approved recording
/// root and retain every media job through terminal status and cleanup. It may
/// not reconnect OBS, own control policy, or own an operation-store writer.
pub trait HostServices: Send + Sync + 'static {
    fn preflight(
        &self,
        reader: PreflightReader,
        options: preflight::Options,
    ) -> IoFuture<preflight::Report>;
    fn inspect(&self, path: String, measure_loudness: bool) -> IoFuture<ClipCheck>;
    fn write_log(&self, path: String, summary: Summary) -> IoFuture<Option<String>>;
    /// Joins only adapter-owned work. The combined service separately joins
    /// its event-feed/bridge and physical socket before reclaiming their loan.
    fn join_observers(&self) -> IoFuture<Result<(), String>>;
}
#[derive(Clone, Debug)]
pub struct Bounds {
    pub rpc: Duration,
    pub read_bytes: usize,
    pub max_ppm_bytes: usize,
    pub max_pixels: usize,
}
impl Default for Bounds {
    fn default() -> Self {
        Self {
            rpc: Duration::from_secs(2),
            read_bytes: 4 * 1024 * 1024,
            max_ppm_bytes: 2 * 1024 * 1024,
            max_pixels: 256 * 1024,
        }
    }
}
impl Bounds {
    fn validate(&self) -> Result<(), String> {
        if self.rpc.is_zero()
            || self.rpc > Duration::from_secs(60)
            || self.read_bytes == 0
            || self.read_bytes > 8 * 1024 * 1024
            || self.max_ppm_bytes == 0
            || self.max_ppm_bytes > 6 * 1024 * 1024
            || self.max_pixels == 0
            || self.max_pixels > 1024 * 1024
        {
            return Err("Take transport bounds are unavailable".into());
        }
        Ok(())
    }
}
struct Bound {
    handle: SessionHandle,
    events: Arc<dyn EventFence>,
    requests: HashSet<String>,
    ppm: bool,
    hostname: String,
    platform: Option<String>,
    pending_settings: Arc<AtomicBool>,
    bounds: Bounds,
}
impl Bound {
    fn live(&self) -> Result<(), String> {
        if self.handle.token() != self.events.token() || self.handle.invalidation().is_some() {
            return Err("Take connection is unavailable".into());
        }
        Ok(())
    }
    fn supports(&self, name: &str) -> Result<(), String> {
        self.live()?;
        if self.requests.contains(name) {
            Ok(())
        } else {
            Err(format!(
                "OBS does not advertise support for request '{name}'"
            ))
        }
    }
    async fn sync(&self, deadline: Instant) -> Result<(), String> {
        loop {
            self.live()?;
            self.events.synchronize(deadline).await?;
            self.live()?;
            if self.events.ready_at_cursor(self.handle.event_cursor()) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("Take event evidence is unavailable".into());
            }
            tokio::task::yield_now().await;
        }
    }
    async fn read(&self, name: &str, data: Value, duration: Duration) -> Result<Value, String> {
        self.supports(name)?;
        let deadline = Instant::now()
            .checked_add(duration)
            .ok_or("Take read deadline is unavailable")?;
        let result = self
            .handle
            .request(self.handle.token(), name, data, duration)
            .await;
        // Rejections, including capability604, may not outrun prior events.
        self.sync(deadline).await?;
        let value = result.map_err(|error| read_error(name, error))?;
        if !within_encoded_limit(&value, self.bounds.read_bytes) {
            return Err(format!("Oversized {name} evidence"));
        }
        Ok(value)
    }
    async fn batch(
        &self,
        calls: Vec<preflight::Call>,
    ) -> Result<Vec<preflight::BatchResult>, ReadError> {
        for call in &calls {
            self.supports(call.request.as_str())
                .map_err(ReadError::new)?;
        }
        let deadline = Instant::now()
            .checked_add(self.bounds.rpc)
            .ok_or_else(|| ReadError::new("Take batch deadline is unavailable"))?;
        let requests = calls
            .into_iter()
            .map(|call| BatchRequest {
                request_type: call.request.as_str().into(),
                request_data: Some(call.data),
            })
            .collect();
        let result = self
            .handle
            .batch(
                self.handle.token(),
                requests,
                BatchOptions {
                    timeout: Some(self.bounds.rpc),
                    ..Default::default()
                },
            )
            .await;
        self.sync(deadline).await.map_err(ReadError::new)?;
        let result = result.map_err(|error| ReadError::new(read_error("batch", error)))?;
        if !within_encoded_limit(&result, self.bounds.read_bytes) {
            return Err(ReadError::new("Oversized take batch evidence"));
        }
        Ok(result
            .into_iter()
            .map(|row| preflight::BatchResult {
                ok: row.ok,
                code: i64::try_from(row.code).ok(),
                response_data: row.response_data,
            })
            .collect())
    }
}
fn read_error(name: &str, error: RequestError) -> String {
    match error {
        RequestError::BeforeDispatch(crate::session::BeforeDispatch::GuardRefused(message)) => {
            message
        }
        RequestError::BeforeDispatch(_) => format!("Request {name} was not dispatched"),
        RequestError::Rejected { .. } | RequestError::RejectedDetailed { .. } => error
            .obs_failure_message(name)
            .unwrap_or_else(|| format!("Request {name} rejection is unavailable")),
        RequestError::Unknown(_) => {
            format!("Request {name} outcome is unknown; no replay was made")
        }
    }
}
fn within_encoded_limit(value: &impl Serialize, limit: usize) -> bool {
    struct Count {
        len: usize,
        limit: usize,
    }
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.len = self
                .len
                .checked_add(bytes.len())
                .filter(|len| *len <= self.limit)
                .ok_or_else(|| {
                    std::io::Error::other("Take evidence exceeds its encoded byte limit")
                })?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Count { len: 0, limit }, value).is_ok()
}
#[derive(Clone)]
pub struct PreflightReader(Arc<Bound>);
impl fmt::Debug for PreflightReader {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("PreflightReader")
            .field("connection", &self.0.handle.token())
            .finish_non_exhaustive()
    }
}
impl Adapter for PreflightReader {
    fn connect(&self) -> ReadFuture<'_, ()> {
        Box::pin(async move { self.0.live().map_err(ReadError::new) })
    }
    fn connection_context(&self) -> ConnectionContext {
        ConnectionContext {
            hostname: Some(self.0.hostname.clone()),
            platform: self.0.platform.clone(),
            pending_output_settings: self.0.pending_settings.load(Ordering::Acquire),
        }
    }
    fn read(&self, request: ReadRequest, data: Option<Value>) -> ReadFuture<'_, Value> {
        Box::pin(async move {
            self.0
                .read(
                    request.as_str(),
                    data.unwrap_or(Value::Null),
                    self.0.bounds.rpc,
                )
                .await
                .map_err(ReadError::new)
        })
    }
    fn batch(&self, calls: Vec<preflight::Call>) -> ReadFuture<'_, Vec<preflight::BatchResult>> {
        Box::pin(self.0.batch(calls))
    }
}
#[derive(Clone)]
pub struct BoundTakeIo {
    bound: Arc<Bound>,
    host: Arc<dyn HostServices>,
}
impl fmt::Debug for BoundTakeIo {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("BoundTakeIo")
            .field("connection", &self.bound.handle.token())
            .field("bounds", &self.bound.bounds)
            .finish_non_exhaustive()
    }
}
impl BoundTakeIo {
    pub async fn bind(
        handle: SessionHandle,
        events: Arc<dyn EventFence>,
        host: Arc<dyn HostServices>,
        exact_endpoint: &str,
        pending_settings: Arc<AtomicBool>,
        bounds: Bounds,
    ) -> Result<Self, String> {
        bounds.validate()?;
        if handle.token() != events.token()
            || handle.invalidation().is_some()
            || handle.event_subscriptions() & takes::REQUIRED_SUBSCRIPTIONS
                != takes::REQUIRED_SUBSCRIPTIONS
            || format!("{:x}", Sha256::digest(exact_endpoint.as_bytes())) != handle.endpoint_hash()
        {
            return Err("Take transport does not match its physical session".into());
        }
        let uri: tokio_tungstenite::tungstenite::http::Uri = exact_endpoint
            .parse()
            .map_err(|_| "Take endpoint is unavailable")?;
        if uri.scheme_str() != Some("ws")
            || uri
                .authority()
                .is_none_or(|authority| authority.as_str().contains('@'))
        {
            return Err("Take endpoint requires an unqualified transport".into());
        }
        let hostname = uri
            .host()
            .ok_or("Take endpoint hostname is unavailable")?
            .to_ascii_lowercase();
        let version = handle
            .request(handle.token(), "GetVersion", Value::Null, bounds.rpc)
            .await;
        // Metadata-only bootstrap precedes TakeSupervisor creation: waiting for
        // that not-yet-running consumer would deadlock. Operational calls all
        // synchronize after the supervisor has acquired its single writer.
        if events.token() != handle.token() || handle.invalidation().is_some() {
            return Err("Take capability evidence is unavailable".into());
        }
        let version = version.map_err(|error| read_error("GetVersion", error))?;
        let list = version
            .get("availableRequests")
            .and_then(Value::as_array)
            .filter(|list| !list.is_empty() && list.len() <= 2048)
            .ok_or("OBS request capabilities are unavailable")?;
        let mut requests = HashSet::new();
        for request in list {
            let name = request
                .as_str()
                .filter(|name| {
                    !name.is_empty()
                        && name.len() <= 128
                        && name.bytes().all(|b| b.is_ascii_alphanumeric())
                })
                .ok_or("OBS request capabilities are incomplete")?;
            requests.insert(name.into());
        }
        if version
            .get("supportedImageFormats")
            .and_then(Value::as_array)
            .is_some_and(|formats| {
                formats.len() > 128
                    || formats
                        .iter()
                        .any(|format| format.as_str().is_some_and(|format| format.len() > 64))
            })
        {
            return Err("OBS image-format capabilities exceeded their bound".into());
        }
        let ppm = version
            .get("supportedImageFormats")
            .and_then(Value::as_array)
            .is_some_and(|formats| formats.iter().any(|format| format == "ppm"));
        let platform = version
            .get("platform")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if platform
            .as_ref()
            .is_some_and(|platform| platform.len() > 32 || !platform.is_ascii())
        {
            return Err("OBS platform capability exceeded its bound".into());
        }
        Ok(Self {
            bound: Arc::new(Bound {
                handle,
                events,
                requests,
                ppm,
                hostname,
                platform,
                pending_settings,
                bounds,
            }),
            host,
        })
    }
    pub fn reader(&self) -> PreflightReader {
        PreflightReader(self.bound.clone())
    }
    pub fn token(&self) -> ConnectionToken {
        self.bound.handle.token()
    }
}
impl TakeIo for BoundTakeIo {
    fn preflight(&self, options: preflight::Options) -> IoFuture<preflight::Report> {
        self.host.preflight(self.reader(), options)
    }
    fn read(&self, request: ReadRequest, duration: Duration) -> IoFuture<Result<Value, String>> {
        let bound = self.bound.clone();
        Box::pin(async move { bound.read(request.as_str(), Value::Null, duration).await })
    }
    fn stop_status(&self, duration: Duration) -> IoFuture<Result<Value, String>> {
        let bound = self.bound.clone();
        Box::pin(async move {
            bound.supports("GetRecordStatus")?;
            let deadline = Instant::now()
                .checked_add(duration)
                .ok_or("Take read deadline is unavailable")?;
            let result = bound
                .handle
                .stop_record_status_checked(bound.handle.token(), duration, || Ok(()))
                .await;
            bound.sync(deadline).await?;
            let value = result.map_err(|error| read_error("GetRecordStatus", error))?;
            if !within_encoded_limit(&value, bound.bounds.read_bytes) {
                return Err("Oversized GetRecordStatus evidence".into());
            }
            Ok(value)
        })
    }
    fn dispatch(
        &self,
        expected: ConnectionToken,
        effect: Effect,
        duration: Duration,
        guard: DispatchGuard,
    ) -> IoFuture<Result<Value, DispatchError>> {
        let bound = self.bound.clone();
        Box::pin(async move {
            let name = effect.request_type();
            if expected != bound.handle.token() {
                return Err(DispatchError::BeforeDispatch(
                    "Take connection changed".into(),
                ));
            }
            bound
                .supports(name)
                .map_err(DispatchError::BeforeDispatch)?;
            let deadline = Instant::now().checked_add(duration).ok_or_else(|| {
                DispatchError::BeforeDispatch("Take dispatch deadline is unavailable".into())
            })?;
            bound
                .sync(deadline)
                .await
                .map_err(DispatchError::BeforeDispatch)?;
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|duration| !duration.is_zero())
                .ok_or_else(|| {
                    DispatchError::BeforeDispatch(
                        "Take dispatch deadline expired before queue admission".into(),
                    )
                })?;
            let checked = bound.clone();
            let final_guard = move || {
                if Instant::now() >= deadline {
                    return Err("Take dispatch deadline expired before send".into());
                }
                checked.live()?;
                if !checked
                    .events
                    .ready_at_cursor(checked.handle.event_cursor())
                {
                    return Err("Take event evidence changed before dispatch".into());
                }
                guard()?;
                if Instant::now() >= deadline {
                    return Err("Take dispatch deadline expired during policy validation".into());
                }
                Ok(())
            };
            let result = if matches!(effect, Effect::StopRecord) {
                bound
                    .handle
                    .stop_record_checked(expected, remaining, final_guard)
                    .await
            } else {
                bound
                    .handle
                    .request_checked(
                        expected,
                        name,
                        effect.request_data(),
                        remaining,
                        final_guard,
                    )
                    .await
            };
            if matches!(&result, Err(RequestError::BeforeDispatch(_))) {
                return result
                    .map_err(|error| DispatchError::BeforeDispatch(read_error(name, error)));
            }
            bound.sync(deadline).await.map_err(DispatchError::Unknown)?;
            result.map_err(|error| match error {
                RequestError::Rejected { code } | RequestError::RejectedDetailed { code, .. } => {
                    DispatchError::Rejected {
                        code,
                        message: read_error(name, error),
                    }
                }
                RequestError::BeforeDispatch(_) => {
                    DispatchError::BeforeDispatch(read_error(name, error))
                }
                RequestError::Unknown(_) => DispatchError::Unknown(read_error(name, error)),
            })
        })
    }
    fn monitor_setup(&self) -> IoFuture<Result<MonitorSetup, String>> {
        let bound = self.bound.clone();
        Box::pin(async move {
            let reader = PreflightReader(bound.clone());
            let tracks = async {
                let mut record_tracks = vec![1];
                let mut input_tracks = HashMap::new();
                let result = async {
                    record_tracks = preflight::recorded_tracks(&reader, false)
                        .await
                        .map_err(|error| error.message)?;
                    let list = bound
                        .read("GetInputList", Value::Null, bound.bounds.rpc)
                        .await?;
                    let names = list
                        .get("inputs")
                        .and_then(Value::as_array)
                        .map(|inputs| {
                            inputs
                                .iter()
                                .filter_map(|input| input.as_object()?.get("inputName")?.as_str())
                                .map(str::to_owned)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    if names.len() > 2048 {
                        return Err("Take input inventory exceeded its bound".into());
                    }
                    let rows = bound
                        .batch(
                            names
                                .iter()
                                .map(|name| preflight::Call {
                                    request: ReadRequest::InputAudioTracks,
                                    data: json!({"inputName":name}),
                                })
                                .collect(),
                        )
                        .await
                        .map_err(|error| error.message)?;
                    for (name, row) in names.into_iter().zip(rows) {
                        if row.ok
                            && let Some(tracks) = row
                                .response_data
                                .get("inputAudioTracks")
                                .and_then(Value::as_object)
                        {
                            input_tracks.insert(name, tracks.clone());
                        }
                    }
                    Ok::<(), String>(())
                }
                .await;
                (record_tracks, input_tracks, result.err())
            };
            let program = async {
                bound
                    .read("GetCurrentProgramScene", Value::Null, bound.bounds.rpc)
                    .await
                    .unwrap_or_else(|_| json!({}))
            };
            let stats = async {
                bound
                    .read("GetStats", Value::Null, bound.bounds.rpc)
                    .await
                    .ok()
            };
            let ((record_tracks, input_tracks, tracks_error), program, stats) =
                futures_util::future::join3(tracks, program, stats).await;
            Ok(MonitorSetup {
                record_tracks,
                input_tracks,
                tracks_error,
                program,
                stats,
                supports_ppm: bound.ppm,
            })
        })
    }
    fn picture(&self, program: String) -> IoFuture<Result<Vec<u8>, String>> {
        let bound = self.bound.clone();
        Box::pin(async move {
            if !bound.ppm {
                return Err("OBS cannot return PPM screenshots".into());
            }
            let deadline = Instant::now() + bound.bounds.rpc;
            bound.sync(deadline).await?;
            let revision = bound.events.picture_revision();
            let value = bound
                .read(
                    "GetSourceScreenshot",
                    json!({"sourceName":program,"imageFormat":"ppm","imageWidth":96}),
                    bound.bounds.rpc,
                )
                .await?;
            if bound.events.picture_revision() != revision {
                return Err("Picture source changed while sampling".into());
            }
            let text = value
                .get("imageData")
                .and_then(Value::as_str)
                .ok_or("OBS returned no PPM screenshot")?;
            let bytes = decode_base64(text, bound.bounds.max_ppm_bytes)?;
            let sample = decode_ppm(&bytes, bound.bounds.max_pixels)?;
            if bound.events.picture_revision() != revision
                || !bound.events.ready_at_cursor(bound.handle.event_cursor())
            {
                return Err("Picture source changed while decoding".into());
            }
            Ok(sample.luma)
        })
    }
    fn inspect(&self, path: String, measure_loudness: bool) -> IoFuture<ClipCheck> {
        self.host.inspect(path, measure_loudness)
    }
    fn write_log(&self, path: String, summary: Summary) -> IoFuture<Option<String>> {
        self.host.write_log(path, summary)
    }
    fn join_observers(&self) -> IoFuture<Result<(), String>> {
        self.host.join_observers()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameSample {
    pub width: usize,
    pub height: usize,
    pub luma: Vec<u8>,
}
fn decode_base64(text: &str, limit: usize) -> Result<Vec<u8>, String> {
    let encoded = text.split_once(',').map_or(text, |(_, data)| data);
    if encoded.len() > limit.saturating_mul(4) / 3 + 1024 {
        return Err("PPM screenshot exceeds its byte limit".into());
    }
    // Buffer.from(...,'base64') accepts URL-safe alphabet, ignored separators,
    // absent padding and nonzero trailing bits. Preserve those bounded forms.
    let mut normalized = Vec::with_capacity(encoded.len());
    for byte in encoded.bytes() {
        match byte {
            b'=' => break,
            b'-' => normalized.push(b'+'),
            b'_' => normalized.push(b'/'),
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' => normalized.push(byte),
            _ => {}
        }
    }
    if normalized.len() % 4 == 1 {
        normalized.pop();
    }
    let engine = GeneralPurpose::new(
        &alphabet::STANDARD,
        GeneralPurposeConfig::new()
            .with_decode_padding_mode(DecodePaddingMode::Indifferent)
            .with_decode_allow_trailing_bits(true),
    );
    let bytes = engine
        .decode(normalized)
        .map_err(|_| "PPM screenshot base64 is unavailable")?;
    if bytes.len() > limit {
        return Err("PPM screenshot exceeds its byte limit".into());
    }
    Ok(bytes)
}
/// Source-equivalent P6 luma for bounded positive dimensions and maxval1..65535.
/// Unlike the predecessor's permissive parser, zero/overflow dimensions and
/// unterminated/malformed separators refuse before allocation.
pub fn decode_ppm(bytes: &[u8], max_pixels: usize) -> Result<FrameSample, String> {
    let magic = bytes
        .iter()
        .take(2)
        .map(|byte| char::from(byte & 127))
        .collect::<String>();
    if magic != "P6" {
        return Err(format!(
            "Not a binary PPM (starts with {})",
            serde_json::to_string(&magic).map_err(|_| "PPM magic unavailable")?
        ));
    }
    let mut offset = 2;
    let mut fields = Vec::with_capacity(3);
    while fields.len() < 3 {
        let byte = *bytes.get(offset).ok_or("Truncated PPM header")?;
        if byte == b'#' {
            while bytes.get(offset).is_some_and(|byte| *byte != b'\n') {
                offset += 1;
            }
        } else if byte.is_ascii_digit() {
            let mut value = 0usize;
            while let Some(byte) = bytes.get(offset).filter(|byte| byte.is_ascii_digit()) {
                value = value
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(usize::from(*byte - b'0')))
                    .ok_or("PPM dimensions exceed their bound")?;
                offset += 1;
            }
            fields.push(value);
            continue;
        } else if !byte.is_ascii_whitespace() {
            return Err("PPM header separator is unavailable".into());
        }
        offset += 1;
    }
    if !bytes.get(offset).is_some_and(u8::is_ascii_whitespace) {
        return Err("Truncated PPM pixel data".into());
    }
    offset += 1;
    let (width, height, maxval) = (fields[0], fields[1], fields[2]);
    if width == 0 || height == 0 || maxval == 0 || maxval > 65535 || max_pixels == 0 {
        return Err("PPM dimensions or maxval are unavailable".into());
    }
    let pixels = width
        .checked_mul(height)
        .filter(|pixels| *pixels <= max_pixels)
        .ok_or("PPM dimensions exceed their bound")?;
    let sample_bytes = if maxval > 255 { 2 } else { 1 };
    let needed = pixels
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_mul(sample_bytes))
        .and_then(|bytes| bytes.checked_add(offset))
        .ok_or("PPM dimensions exceed their bound")?;
    if bytes.len() < needed {
        return Err("Truncated PPM pixel data".into());
    }
    let read = |sample: usize| {
        let at = offset + sample * sample_bytes;
        if sample_bytes == 1 {
            f64::from(bytes[at])
        } else {
            f64::from(u16::from_be_bytes([bytes[at], bytes[at + 1]]))
        }
    };
    let scale = 255.0 / maxval as f64;
    let mut luma = Vec::with_capacity(pixels);
    for pixel in 0..pixels {
        let value = ((0.2126 * read(pixel * 3)
            + 0.7152 * read(pixel * 3 + 1)
            + 0.0722 * read(pixel * 3 + 2))
            * scale)
            .round();
        luma.push((value as u64 % 256) as u8);
    }
    Ok(FrameSample {
        width,
        height,
        luma,
    })
}

/// Host authority is limited to existing regular files beneath this explicit
/// recording root. It uses no global paths and does not create the root. These
/// checks reject ordinary replacement/symlink/hardlink accidents; they are not
/// a sandbox against concurrent same-user ancestor swaps.
#[derive(Clone)]
pub struct LocalHost {
    root: PathBuf,
    root_identity: Metadata,
    ffmpeg: Option<Tool>,
    ffprobe: Option<Tool>,
}
#[derive(Clone)]
struct Tool {
    path: PathBuf,
    identity: Metadata,
}
impl Tool {
    fn new(path: &Path) -> Result<Self, String> {
        if !path.is_absolute() {
            return Err("Media tool path must be absolute".into());
        }
        let path = path
            .canonicalize()
            .map_err(|_| "Media tool path is unavailable")?;
        let identity =
            fs::symlink_metadata(&path).map_err(|_| "Media tool identity is unavailable")?;
        if !identity.is_file() || identity.file_type().is_symlink() {
            return Err("Media tool is not a regular file".into());
        }
        Ok(Self { path, identity })
    }
    fn checked(&self) -> Result<&Path, String> {
        let current =
            fs::symlink_metadata(&self.path).map_err(|_| "Media tool identity changed")?;
        if !same_file(&current, &self.identity)
            || !current.is_file()
            || current.file_type().is_symlink()
        {
            return Err("Media tool identity changed".into());
        }
        Ok(&self.path)
    }
}
#[cfg(unix)]
fn same_file(a: &Metadata, b: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}
#[cfg(not(unix))]
fn same_file(_a: &Metadata, _b: &Metadata) -> bool {
    false
}
#[cfg(unix)]
fn independent_file(metadata: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.nlink() == 1
}
#[cfg(not(unix))]
fn independent_file(_metadata: &Metadata) -> bool {
    false
}
fn unchanged(a: &Metadata, b: &Metadata) -> bool {
    same_file(a, b) && a.len() == b.len() && a.modified().ok() == b.modified().ok()
}
static TEMPORARY: AtomicU64 = AtomicU64::new(0);
impl LocalHost {
    pub fn new(root: &Path, ffmpeg: Option<&Path>, ffprobe: Option<&Path>) -> Result<Self, String> {
        if !cfg!(unix) {
            return Err("Native recording-host identity is unqualified on this platform".into());
        }
        if !root.is_absolute()
            || root
                .components()
                .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
        {
            return Err("Approved recording root must be absolute and normalized".into());
        }
        let canonical = root
            .canonicalize()
            .map_err(|_| "Approved recording root is unavailable")?;
        let root_identity =
            fs::symlink_metadata(root).map_err(|_| "Approved recording root is unavailable")?;
        if canonical != root || !root_identity.is_dir() || root_identity.file_type().is_symlink() {
            return Err("Approved recording root must be an existing non-symlink directory".into());
        }
        Ok(Self {
            root: canonical,
            root_identity,
            ffmpeg: ffmpeg.map(Tool::new).transpose()?,
            ffprobe: ffprobe.map(Tool::new).transpose()?,
        })
    }
    fn root_checked(&self) -> Result<(), String> {
        let current = fs::symlink_metadata(&self.root)
            .map_err(|_| "Approved recording root is unavailable")?;
        if !current.is_dir()
            || current.file_type().is_symlink()
            || !same_file(&current, &self.root_identity)
        {
            return Err("Approved recording root identity changed".into());
        }
        Ok(())
    }
    fn directory(&self, path: &Path) -> Result<Metadata, String> {
        self.root_checked()?;
        if !path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
            || !path.starts_with(&self.root)
        {
            return Err("Path is outside the approved recording root".into());
        }
        let canonical = path
            .canonicalize()
            .map_err(|_| "Recording directory is unavailable")?;
        let metadata =
            fs::symlink_metadata(path).map_err(|_| "Recording directory is unavailable")?;
        if canonical != path || !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err("Recording directory identity is unavailable".into());
        }
        Ok(metadata)
    }
    fn recording(&self, path: &Path) -> Result<Metadata, String> {
        self.directory(path.parent().ok_or("Recording parent is unavailable")?)?;
        let metadata = fs::symlink_metadata(path).map_err(|_| "Recording file is unavailable")?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || !independent_file(&metadata)
        {
            return Err("Recording must be an independent regular file".into());
        }
        Ok(metadata)
    }
    fn stable_recording(&self, path: &Path, before: &Metadata) -> Result<(), String> {
        let after = self.recording(path)?;
        if !unchanged(before, &after) {
            return Err("Recording identity or content changed during inspection".into());
        }
        Ok(())
    }
    fn temporary(&self, directory: &Path) -> Result<(PathBuf, std::fs::File, Metadata), String> {
        self.directory(directory)?;
        let id = TEMPORARY
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| "Recording temporary identity exhausted")?;
        let path = directory.join(format!(".obs-mcp-take-{}-{id}.tmp", std::process::id()));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .map_err(|error| format!("Could not reserve private recording temporary: {error}"))?;
        let identity = file
            .metadata()
            .map_err(|_| "Recording temporary identity is unavailable")?;
        Ok((path, file, identity))
    }
    fn remove_owned(&self, path: &Path, identity: &Metadata) -> Result<(), String> {
        self.directory(path.parent().ok_or("Temporary parent is unavailable")?)?;
        let current =
            fs::symlink_metadata(path).map_err(|_| "Recording temporary is unavailable")?;
        if !current.is_file() || current.file_type().is_symlink() || !same_file(&current, identity)
        {
            return Err("Recording temporary identity changed; cleanup withheld".into());
        }
        fs::remove_file(path)
            .map_err(|error| format!("Recording temporary cleanup failed: {error}"))
    }
    async fn inspect_file(&self, path: &Path, measure_loudness: bool) -> ClipCheck {
        let skipped = |message: String| ClipCheck {
            skipped: Some(message),
            ..Default::default()
        };
        let before = match self.recording(path) {
            Ok(value) => value,
            Err(error) => return skipped(error),
        };
        let Some(probe) = &self.ffprobe else {
            return skipped("ffprobe is not installed".into());
        };
        let executable = match probe.checked() {
            Ok(path) => path,
            Err(error) => return skipped(error),
        };
        let args = ["-v".into(), "error".into()]
            .into_iter()
            .chain(local_media_input_options(LocalMediaInput::Recording))
            .chain([
                "-print_format".into(),
                "json".into(),
                "-show_format".into(),
                "-show_streams".into(),
                path.as_os_str().to_owned(),
            ])
            .collect::<Vec<_>>();
        let mut job = match media_verification::capture_bounded_media_process(
            executable,
            &args,
            ProcessOptions {
                timeout: Duration::from_secs(15),
                ..Default::default()
            },
            1024 * 1024,
        ) {
            Ok(job) => job,
            Err(error) => return skipped(format!("ffprobe failed: {error}")),
        };
        let bytes = match job.finish().await {
            Ok(bytes) => bytes,
            Err(error) => return skipped(format!("ffprobe failed: {error}")),
        };
        drop(job);
        if let Err(error) = self.stable_recording(path, &before) {
            return skipped(error);
        }
        let probe: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => return skipped(format!("ffprobe failed: {error}")),
        };
        drop(bytes);
        let Some(duration) = js_number(
            probe
                .get("format")
                .and_then(|format| format.get("duration")),
        )
        .filter(|value| value.is_finite()) else {
            return skipped(
                "ffprobe could not read the file's length; it may be incomplete".into(),
            );
        };
        let streams = probe.get("streams").and_then(Value::as_array);
        let audio = streams.map_or(0, |streams| {
            streams
                .iter()
                .filter(|stream| {
                    stream.is_object()
                        && stream.get("codec_type").is_some_and(|kind| kind == "audio")
                })
                .count()
        });
        let video = streams.map_or(0, |streams| {
            streams
                .iter()
                .filter(|stream| {
                    stream.is_object()
                        && stream.get("codec_type").is_some_and(|kind| kind == "video")
                })
                .count()
        });
        drop(probe);
        let mut check = ClipCheck {
            duration_seconds: Some(duration),
            audio_streams: Some(audio),
            video_streams: Some(video),
            ..Default::default()
        };
        if !measure_loudness || audio == 0 {
            return check;
        }
        if audio > 6 {
            check.skipped = Some(
                "Native loudness inspection supports at most six recording audio tracks".into(),
            );
            return check;
        }
        let Some(ffmpeg) = &self.ffmpeg else {
            check.skipped = Some("ffmpeg loudness check failed: ffmpeg is not installed".into());
            return check;
        };
        let deadline = Instant::now() + Duration::from_secs(75);
        for track in 0..audio {
            let executable = match ffmpeg.checked() {
                Ok(path) => path,
                Err(error) => {
                    check.skipped = Some(error);
                    break;
                }
            };
            let Some(remaining) = deadline
                .checked_duration_since(Instant::now())
                .filter(|duration| !duration.is_zero())
            else {
                check.skipped =
                    Some("ffmpeg loudness check failed: inspection deadline expired".into());
                break;
            };
            let args = [
                "-v".into(),
                "error".into(),
                "-hide_banner".into(),
                "-nostats".into(),
            ]
            .into_iter()
            .chain(local_media_input_options(LocalMediaInput::Recording))
            .chain([
                "-i".into(),
                path.as_os_str().to_owned(),
                "-map".into(),
                format!("0:a:{track}").into(),
                "-c:a".into(),
                "pcm_s16le".into(),
                "-f".into(),
                "s16le".into(),
                "-".into(),
            ])
            .collect::<Vec<_>>();
            let mut job = match media_verification::run_streamed_media_process(
                executable,
                &args,
                ProcessOptions {
                    timeout: remaining.min(Duration::from_secs(60)),
                    ..Default::default()
                },
                PcmPeak::default(),
            ) {
                Ok(job) => job,
                Err(error) => {
                    check.skipped = Some(format!("ffmpeg loudness check failed: {error}"));
                    break;
                }
            };
            match job.finish().await {
                Ok(peak) => match peak {
                    Some(level) => {
                        check.max_volume_db =
                            Some(check.max_volume_db.unwrap_or(f64::NEG_INFINITY).max(level))
                    }
                    None => {
                        check.skipped = Some(format!(
                            "ffmpeg did not report loudness for audio track {track}"
                        ))
                    }
                },
                Err(error) => {
                    check.skipped = Some(format!("ffmpeg loudness check failed: {error}"));
                    break;
                }
            }
            if let Err(error) = self.stable_recording(path, &before) {
                check.skipped = Some(error);
                break;
            }
        }
        check
    }
    fn publish_log(&self, path: &Path, summary: &Summary) -> Option<String> {
        let before = self.recording(path).ok()?;
        let spelling = path.to_str()?;
        let dot = spelling.rfind('.');
        let slash = spelling.rfind(['/', '\\']);
        let base = dot
            .filter(|dot| slash.is_none_or(|slash| *dot > slash))
            .map_or(spelling, |dot| &spelling[..dot]);
        let destination = PathBuf::from(format!("{base}.take.json"));
        let parent = destination.parent()?;
        let directory = self.directory(parent).ok()?;
        if fs::symlink_metadata(&destination).is_ok() {
            return None;
        }
        let mut value = serde_json::to_value(summary).ok()?;
        value
            .as_object_mut()?
            .insert("recording".into(), Value::String(spelling.into()));
        let mut bytes = serde_json::to_vec_pretty(&value).ok()?;
        if bytes.len() > 8 * 1024 * 1024 {
            return None;
        }
        bytes.push(b'\n');
        let (temporary, mut file, identity) = self.temporary(parent).ok()?;
        let result = (|| {
            file.write_all(&bytes).ok()?;
            file.sync_all().ok()?;
            self.stable_recording(path, &before).ok()?;
            if !same_file(&self.directory(parent).ok()?, &directory) {
                return None;
            }
            // A source log is best-effort, not a custody journal. Publish only
            // complete bytes and never overwrite an existing human sidecar.
            fs::hard_link(&temporary, &destination).ok()?;
            Some(destination.to_string_lossy().into_owned())
        })();
        drop(file);
        let _ = self.remove_owned(&temporary, &identity);
        result
    }
}
impl fmt::Debug for LocalHost {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("LocalHost")
            .field("ffmpeg_present", &self.ffmpeg.is_some())
            .field("ffprobe_present", &self.ffprobe.is_some())
            .finish_non_exhaustive()
    }
}
impl preflight::WritableDirectoryProbe for LocalHost {
    fn check(&self, path: &str) -> Result<(), String> {
        let directory = Path::new(path);
        self.directory(directory)?;
        // Effective create permission, including ACL/readonly filesystem, not
        // mode bits. The private probe is confined to the approved root.
        let (path, file, identity) = self.temporary(directory)?;
        drop(file);
        self.remove_owned(&path, &identity)
    }
}
impl HostServices for LocalHost {
    fn preflight(
        &self,
        reader: PreflightReader,
        options: preflight::Options,
    ) -> IoFuture<preflight::Report> {
        let host = self.clone();
        Box::pin(async move { preflight::run_preflight(&reader, &host, &options).await })
    }
    fn inspect(&self, path: String, measure_loudness: bool) -> IoFuture<ClipCheck> {
        let host = self.clone();
        Box::pin(async move { host.inspect_file(Path::new(&path), measure_loudness).await })
    }
    fn write_log(&self, path: String, summary: Summary) -> IoFuture<Option<String>> {
        let host = self.clone();
        Box::pin(async move { host.publish_log(Path::new(&path), &summary) })
    }
    fn join_observers(&self) -> IoFuture<Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}
fn js_number(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Null => Some(0.0),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::Number(number) => number.as_f64(),
        Value::String(text) => {
            let text = crate::config::trim(text);
            if text.is_empty() {
                Some(0.0)
            } else {
                text.parse().ok()
            }
        }
        _ => None,
    }
}
/// Peak over every interleaved channel, with at most one partial sample byte.
/// Raw PCM carries no filename/metadata diagnostics. The formula and decimal
/// projection match FFmpeg af_volumedetect's s16 histogram peak and MAX_DB91.
#[derive(Default)]
struct PcmPeak {
    trailing: Option<u8>,
    peak: u16,
    samples: u64,
    stderr_bytes: usize,
}
impl PcmPeak {
    fn sample(&mut self, bytes: [u8; 2]) -> Result<(), String> {
        self.peak = self.peak.max(i16::from_le_bytes(bytes).unsigned_abs());
        self.samples = self
            .samples
            .checked_add(1)
            .ok_or("PCM sample count exhausted")?;
        Ok(())
    }
}
impl StdoutSink for PcmPeak {
    type Output = Option<f64>;
    fn consume(&mut self, bytes: &[u8]) -> Result<(), String> {
        let mut bytes = bytes;
        if let Some(low) = self.trailing.take() {
            if let Some((high, rest)) = bytes.split_first() {
                self.sample([low, *high])?;
                bytes = rest;
            } else {
                self.trailing = Some(low);
                return Ok(());
            }
        }
        let mut chunks = bytes.chunks_exact(2);
        for sample in &mut chunks {
            self.sample([sample[0], sample[1]])?;
        }
        self.trailing = chunks.remainder().first().copied();
        Ok(())
    }
    fn consume_stderr(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.stderr_bytes = self
            .stderr_bytes
            .checked_add(bytes.len())
            .filter(|bytes| *bytes <= 1024 * 1024)
            .ok_or("Loudness stderr exceeded its byte limit")?;
        Ok(())
    }
    fn finish(self) -> Result<Self::Output, String> {
        if self.trailing.is_some() {
            return Err("Incomplete s16 PCM sample at EOF".into());
        }
        if self.samples == 0 {
            return Ok(None);
        }
        let db = if self.peak == 0 {
            -91.0
        } else {
            let power = u64::from(self.peak) * u64::from(self.peak);
            let ratio = power as f64 / (32768.0 * 32768.0);
            -(-ratio.log10() * 10.0)
        };
        let projected = format!("{db:.1}")
            .parse::<f64>()
            .map_err(|_| "PCM peak cannot be projected")?;
        Ok(Some(if projected == 0.0 { 0.0 } else { projected }))
    }
}

#[cfg(test)]
mod pcm_tests {
    use super::*;
    #[test]
    fn pcm_peak_preserves_partial_samples_and_requires_complete_eof() {
        let mut peak = PcmPeak::default();
        peak.consume(&[0]).unwrap();
        peak.consume(&[]).unwrap();
        peak.consume(&[0, 255, 127, 0]).unwrap();
        peak.consume(&[128]).unwrap();
        assert_eq!(peak.finish().unwrap(), Some(0.0));
        let mut silence = PcmPeak::default();
        silence.consume(&[0, 0, 0, 0]).unwrap();
        assert_eq!(silence.finish().unwrap(), Some(-91.0));
        assert_eq!(PcmPeak::default().finish().unwrap(), None);
        let mut odd = PcmPeak::default();
        odd.consume(&[0]).unwrap();
        assert!(odd.finish().unwrap_err().contains("Incomplete"));
    }
    #[test]
    fn pcm_peak_uses_all_interleaved_channels_and_bounded_stderr() {
        let mut peak = PcmPeak::default();
        for sample in [0i16, 1, 0, 4096] {
            for byte in sample.to_le_bytes() {
                peak.consume(&[byte]).unwrap();
            }
        }
        assert_eq!(peak.finish().unwrap(), Some(-18.1));
        let mut peak = PcmPeak::default();
        assert!(peak.consume_stderr(&vec![0; 1024 * 1024 + 1]).is_err());
    }
}
