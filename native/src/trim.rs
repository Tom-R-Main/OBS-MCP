// SPDX-License-Identifier: GPL-2.0-only
//! Trim planning and an owned no-clobber publication boundary. Process adapters
//! are injected. A frontend must retain ExportOwner while cancelling its finish
//! await; dropping the await does not cancel, retry, or relinquish its jobs.
//! Metadata/full-decode verification proves structural scope, not editorial
//! content, pixel fidelity, HDR tone mapping, or power-loss durability.
//! One trusted writer owns each stage and metadata work file. Path/inode
//! checks do not sandbox concurrent same-user ancestor replacement. A concrete
//! encoder must reserve its metadata work file without clobbering other files.
use crate::{media_verification::Cancellation, observation::js_string, take_monitor::StillTracker};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    ffi::OsString,
    fmt,
    fs::{self, File, Metadata, OpenOptions},
    future::Future,
    io::{self, Read},
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

pub const SAMPLE_FPS: f64 = 2.0;
pub const SAMPLE_WIDTH: usize = 96;
pub const MAX_SHEET_FRAMES: usize = 24;
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
pub const ENCODER_QUERY_TIMEOUT: Duration = Duration::from_secs(10);
pub const ENCODE_TIMEOUT: Duration = Duration::from_secs(600);
pub const SAMPLE_TIMEOUT: Duration = Duration::from_secs(300);
pub const SHEET_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
pub const PROBE_STDOUT_LIMIT: usize = 1024 * 1024;
pub const ENCODE_OUTPUT_LIMIT: usize = 32 * 1024 * 1024;
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub start: f64,
    pub end: f64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Chapter {
    pub at: f64,
    pub name: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CutOptions {
    pub duration: f64,
    pub idle: Vec<Span>,
    pub chapters: Vec<Chapter>,
    pub min_idle_seconds: f64,
    pub keep_seconds: f64,
    pub hold_after_chapter_seconds: f64,
    pub hold_before_chapter_seconds: f64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CutPlan {
    pub cuts: Vec<Span>,
    pub keep: Vec<Span>,
}
fn rounded(value: f64) -> f64 {
    if !value.is_finite() || value == 0.0 {
        return value;
    }
    let floor = value.floor();
    if value - floor < 0.5 {
        floor
    } else {
        floor + 1.0
    }
}
pub fn round_seconds(value: f64) -> f64 {
    rounded(value * 100.0) / 100.0
}
fn max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}
fn min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.min(b)
    }
}
fn number(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value == f64::INFINITY {
        "Infinity".into()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".into()
    } else {
        js_string(Some(&json!(value)))
    }
}
fn merge(mut spans: Vec<Span>) -> Vec<Span> {
    spans.sort_by(|a, b| {
        (a.start - b.start)
            .partial_cmp(&0.0)
            .unwrap_or(Ordering::Equal)
    });
    let mut merged: Vec<Span> = vec![];
    for span in spans {
        if let Some(last) = merged.last_mut()
            && span.start <= last.end
        {
            last.end = max(last.end, span.end);
        } else {
            merged.push(span)
        }
    }
    merged
}
fn subtract(mut spans: Vec<Span>, holes: &[Span]) -> Vec<Span> {
    for hole in holes {
        spans = spans
            .into_iter()
            .flat_map(|span| {
                if hole.end <= span.start || hole.start >= span.end {
                    return vec![span];
                }
                let mut result = vec![];
                if hole.start > span.start {
                    result.push(Span {
                        start: span.start,
                        end: hole.start,
                    })
                }
                if hole.end < span.end {
                    result.push(Span {
                        start: hole.end,
                        end: span.end,
                    })
                }
                result
            })
            .collect();
    }
    spans
}
pub fn plan_cuts(options: &CutOptions) -> CutPlan {
    let candidates = options
        .idle
        .iter()
        .filter(|span| span.end - span.start >= options.min_idle_seconds)
        .map(|span| Span {
            start: span.start + options.keep_seconds,
            end: span.end - options.keep_seconds,
        })
        .filter(|span| span.end - span.start >= 0.5)
        .collect();
    let mut protected = options
        .chapters
        .iter()
        .map(|chapter| Span {
            start: chapter.at - options.hold_before_chapter_seconds,
            end: chapter.at + options.hold_after_chapter_seconds,
        })
        .collect::<Vec<_>>();
    protected.push(Span {
        start: options.duration - options.hold_before_chapter_seconds,
        end: options.duration,
    });
    let cuts = merge(subtract(candidates, &protected))
        .into_iter()
        .map(|span| Span {
            start: max(0.0, span.start),
            end: min(options.duration, span.end),
        })
        .filter(|span| span.end - span.start >= 0.5)
        .map(|span| Span {
            start: round_seconds(span.start),
            end: round_seconds(span.end),
        })
        .collect::<Vec<_>>();
    let mut keep = vec![];
    let mut cursor = 0.0;
    for cut in &cuts {
        if cut.start > cursor {
            keep.push(Span {
                start: round_seconds(cursor),
                end: cut.start,
            });
        }
        cursor = cut.end;
    }
    if options.duration > cursor {
        keep.push(Span {
            start: round_seconds(cursor),
            end: round_seconds(options.duration),
        });
    }
    CutPlan { cuts, keep }
}
pub fn remap_time(time: f64, cuts: &[Span]) -> f64 {
    let mut removed = 0.0;
    for cut in cuts {
        if time >= cut.end {
            removed += cut.end - cut.start;
        } else if time > cut.start {
            removed += time - cut.start;
        }
    }
    round_seconds(time - removed)
}
pub fn seam_sample_times(seams: &[f64], duration: f64) -> Vec<f64> {
    let last = max(0.0, duration - min(0.1, duration / 2.0));
    seams
        .iter()
        .flat_map(|seam| [max(0.0, seam - 0.5), min(last, seam + 0.2)])
        .take(MAX_SHEET_FRAMES)
        .collect()
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutputChapter {
    pub at_seconds: f64,
    pub name: String,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Timeline {
    pub path: String,
    pub duration_seconds: f64,
    pub trimmed_seconds: f64,
    pub removed_seconds: f64,
    pub idle: Vec<Span>,
    pub cuts: Vec<Span>,
    pub keep: Vec<Span>,
    pub seams: Vec<f64>,
    pub chapters: Vec<OutputChapter>,
    pub audio_considered: bool,
}
pub fn timeline(path: &str, options: &CutOptions, audible: bool) -> Timeline {
    let plan = plan_cuts(options);
    let removed = round_seconds(plan.cuts.iter().map(|span| span.end - span.start).sum());
    Timeline {
        path: path.to_owned(),
        duration_seconds: round_seconds(options.duration),
        trimmed_seconds: round_seconds(options.duration - removed),
        removed_seconds: removed,
        idle: options
            .idle
            .iter()
            .map(|s| Span {
                start: round_seconds(s.start),
                end: round_seconds(s.end),
            })
            .collect(),
        seams: plan
            .cuts
            .iter()
            .map(|span| remap_time(span.start, &plan.cuts))
            .collect(),
        chapters: options
            .chapters
            .iter()
            .map(|c| OutputChapter {
                at_seconds: remap_time(c.at, &plan.cuts),
                name: c.name.clone(),
            })
            .collect(),
        cuts: plan.cuts,
        keep: plan.keep,
        audio_considered: audible,
    }
}
pub fn plan_report(plan: &Timeline, min_idle_seconds: f64, apply: bool) -> Value {
    let mut lines = vec![
        if plan.cuts.is_empty() {
            format!(
                "Nothing to cut: no unchanged stretch of {}s or more outside the chapter starts",
                number(min_idle_seconds)
            )
        } else {
            format!(
                "Cut {} stretch(es), {}s of {}s: {}",
                plan.cuts.len(),
                number(plan.removed_seconds),
                number(plan.duration_seconds),
                plan.cuts
                    .iter()
                    .map(|s| format!("{}–{}s", number(s.start), number(s.end)))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        },
        format!("Trimmed length: {}s", number(plan.trimmed_seconds)),
    ];
    if !plan.chapters.is_empty() {
        lines.push(format!(
            "Chapters after trimming: {}",
            plan.chapters
                .iter()
                .map(|c| format!("{}s {}", number(c.at_seconds), c.name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !apply && !plan.cuts.is_empty() {
        lines.push("Plan only; call again with apply: true to export".into());
    }
    json!({"content":[{"type":"text","text":lines.join("\n")}],"structuredContent":plan})
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaProbe {
    pub duration: f64,
    pub audio_streams: usize,
    pub video_streams: usize,
    pub chapters: Vec<Chapter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pixel_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bit_depth: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_transfer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_primaries: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_space: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_range: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpectedMetadata {
    pub duration: f64,
    pub width: Option<f64>,
    pub height: Option<f64>,
    pub audio_streams: usize,
    pub chapters: Vec<Chapter>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Evidence,
    Cancelled,
    Encode,
    Probe,
    Decode,
    Preview,
    UnsafePath,
    DestinationExists,
    StageChanged,
    Storage,
    Limit,
    Unsupported,
}
#[derive(Clone, PartialEq, Eq)]
pub struct TrimError {
    pub kind: ErrorKind,
    message: String,
    pub cleanup_issue: Option<String>,
}
impl TrimError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            cleanup_issue: None,
        }
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}
impl fmt::Display for TrimError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(&self.message)?;
        if let Some(issue) = &self.cleanup_issue {
            write!(out, "; cleanup unconfirmed: {issue}")?;
        }
        Ok(())
    }
}
impl fmt::Debug for TrimError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("TrimError")
            .field("kind", &self.kind)
            .field("message", &"[REDACTED]")
            .field("cleanup_issue", &self.cleanup_issue.is_some())
            .finish()
    }
}
impl std::error::Error for TrimError {}
fn evidence(message: impl Into<String>) -> TrimError {
    TrimError::new(ErrorKind::Evidence, message)
}
fn cancelled(token: &Cancellation) -> Result<(), TrimError> {
    if token.is_cancelled() {
        Err(TrimError::new(
            ErrorKind::Cancelled,
            "Trim export cancelled",
        ))
    } else {
        Ok(())
    }
}
fn js_number(value: Option<&Value>) -> f64 {
    match value {
        None => f64::NAN,
        Some(Value::Null) => 0.0,
        Some(Value::Bool(v)) => {
            if *v {
                1.0
            } else {
                0.0
            }
        }
        Some(Value::Number(v)) => v.as_f64().unwrap_or(f64::NAN),
        Some(Value::Object(_)) => f64::NAN,
        Some(Value::Array(_)) => js_number(Some(&Value::String(js_string(value)))),
        Some(Value::String(s)) => {
            let s =
                s.trim_matches(|c: char| c == '\u{feff}' || (c.is_whitespace() && c != '\u{85}'));
            if s.is_empty() {
                return 0.0;
            }
            for (prefix, base) in [
                ("0x", 16),
                ("0X", 16),
                ("0o", 8),
                ("0O", 8),
                ("0b", 2),
                ("0B", 2),
            ] {
                if let Some(digits) = s.strip_prefix(prefix) {
                    return if digits.is_empty() {
                        f64::NAN
                    } else {
                        digits
                            .chars()
                            .try_fold(0.0, |n, c| {
                                c.to_digit(base).map(|v| n * f64::from(base) + f64::from(v))
                            })
                            .unwrap_or(f64::NAN)
                    };
                }
            }
            if matches!(
                s,
                "inf" | "-inf" | "+inf" | "infinity" | "-infinity" | "+infinity"
            ) {
                f64::NAN
            } else {
                s.parse().unwrap_or(f64::NAN)
            }
        }
    }
}
pub fn parse_probe(data: &Value) -> MediaProbe {
    let streams = data
        .get("streams")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|v| v.is_object())
        .collect::<Vec<_>>();
    let video = streams.iter().find(|s| s["codec_type"] == "video");
    let text = |key: &str| video.and_then(|v| v[key].as_str()).map(str::to_owned);
    let bit_depth = video
        .map(|v| js_number(v.get("bits_per_raw_sample")))
        .filter(|v| *v != 0.0 && !v.is_nan());
    MediaProbe {
        duration: js_number(
            data.get("format")
                .filter(|v| v.is_object())
                .and_then(|v| v.get("duration")),
        ),
        audio_streams: streams
            .iter()
            .filter(|s| s["codec_type"] == "audio")
            .count(),
        video_streams: streams
            .iter()
            .filter(|s| s["codec_type"] == "video")
            .count(),
        chapters: data
            .get("chapters")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|v| v.is_object())
            .enumerate()
            .map(|(index, c)| Chapter {
                at: js_number(c.get("start_time")),
                name: c["tags"]["title"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("Chapter {}", index + 1)),
            })
            .collect(),
        width: video.map(|v| js_number(v.get("width"))),
        height: video.map(|v| js_number(v.get("height"))),
        codec: text("codec_name"),
        pixel_format: text("pix_fmt"),
        bit_depth,
        color_transfer: text("color_transfer"),
        color_primaries: text("color_primaries"),
        color_space: text("color_space"),
        color_range: text("color_range"),
    }
}
pub fn require_sdr_input(info: &MediaProbe) -> Result<(), TrimError> {
    if matches!(
        info.color_transfer.as_deref(),
        Some("smpte2084" | "arib-std-b67")
    ) {
        return Err(evidence(
            "Trim export supports 8-bit SDR only; PQ/HLG input requires an explicit tone-mapping workflow",
        ));
    }
    const FORMATS: &[&str] = &[
        "yuv420p", "yuvj420p", "yuv422p", "yuvj422p", "yuv444p", "yuvj444p", "nv12", "nv21",
        "uyvy422", "yuyv422", "rgb24", "bgr24", "rgba", "bgra", "argb", "abgr", "gbrp", "gray",
        "pal8", "0rgb", "rgb0", "0bgr", "bgr0",
    ];
    if info.bit_depth.unwrap_or(0.0) > 8.0
        || !FORMATS.contains(&info.pixel_format.as_deref().unwrap_or(""))
    {
        return Err(evidence(format!(
            "Trim export requires a known 8-bit pixel format; got {}",
            info.pixel_format.as_deref().unwrap_or("unknown")
        )));
    }
    Ok(())
}
pub fn chapter_title(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c <= '\u{1f}' || c == '\u{7f}' {
                ' '
            } else {
                c
            }
        })
        .collect()
}
pub fn validate_trimmed_metadata(
    actual: &MediaProbe,
    expected: &ExpectedMetadata,
) -> Result<(), TrimError> {
    if !actual.duration.is_finite() || (actual.duration - expected.duration).abs() > 0.25 {
        return Err(evidence("Staged export duration differs from the cut plan"));
    }
    if actual.video_streams != 1
        || actual.codec.as_deref() != Some("h264")
        || actual.pixel_format.as_deref() != Some("yuv420p")
    {
        return Err(evidence(
            "Staged export must contain one H.264 yuv420p video stream",
        ));
    }
    if actual.width != expected.width || actual.height != expected.height {
        return Err(evidence("Staged export dimensions differ from the source"));
    }
    if actual.audio_streams != expected.audio_streams.min(1) {
        return Err(evidence(
            "Staged export audio stream count differs from the first-track policy",
        ));
    }
    if actual.chapters.len() != expected.chapters.len()
        || expected
            .chapters
            .iter()
            .zip(&actual.chapters)
            .any(|(expected, actual)| {
                actual.name != chapter_title(&expected.name)
                    || !actual.at.is_finite()
                    || (actual.at - expected.at).abs() > 0.05
            })
    {
        return Err(evidence(
            "Staged export chapters differ from the remapped cut plan",
        ));
    }
    Ok(())
}
pub fn qualified_encoder_args(encoders: &str) -> Result<Vec<String>, TrimError> {
    let word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let has = |needle: &str| {
        encoders.match_indices(needle).any(|(offset, _)| {
            (offset == 0 || !word(encoders.as_bytes()[offset - 1]))
                && (offset + needle.len() == encoders.len()
                    || !word(encoders.as_bytes()[offset + needle.len()]))
        })
    };
    let args = if has("libx264") {
        vec![
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p",
        ]
    } else if has("h264_videotoolbox") {
        vec![
            "-c:v",
            "h264_videotoolbox",
            "-q:v",
            "65",
            "-pix_fmt",
            "yuv420p",
        ]
    } else {
        return Err(evidence(
            "Trim export requires libx264 or h264_videotoolbox; no qualified H.264 encoder is available",
        ));
    };
    Ok(args.into_iter().map(str::to_owned).collect())
}
pub fn ffmetadata(chapters: &[Chapter], duration: f64) -> String {
    let mut lines = vec![";FFMETADATA1".to_owned()];
    for (index, chapter) in chapters.iter().enumerate() {
        let end = chapters.get(index + 1).map_or(duration, |c| c.at);
        let mut title = String::new();
        for c in chapter_title(&chapter.name).chars() {
            if matches!(c, '=' | ';' | '#' | '\\') {
                title.push('\\');
            }
            title.push(c);
        }
        lines.extend([
            "[CHAPTER]".into(),
            "TIMEBASE=1/1000".into(),
            format!("START={}", number(rounded(chapter.at * 1000.0))),
            format!("END={}", number(rounded(end * 1000.0))),
            format!("title={title}"),
        ]);
    }
    format!("{}\n", lines.join("\n"))
}
#[derive(Clone)]
pub struct EncodeRequest {
    args: Vec<OsString>,
    pub metadata: String,
    pub metadata_path: PathBuf,
    pub input: PathBuf,
    pub output: PathBuf,
}
impl EncodeRequest {
    pub fn args(&self) -> &[OsString] {
        &self.args
    }
}
impl fmt::Debug for EncodeRequest {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("EncodeRequest([REDACTED])")
    }
}
pub fn encode_command(
    input: &Path,
    output: &Path,
    metadata: &Path,
    plan: EncodePlan<'_>,
    encoders: &str,
) -> Result<EncodeRequest, TrimError> {
    let EncodePlan {
        keep,
        audio_streams,
        chapters,
        duration,
    } = plan;
    let audio = audio_streams > 0;
    let parts = keep
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let video = format!(
                "[0:v:0]trim=start={}:end={},setpts=PTS-STARTPTS[v{i}]",
                number(s.start),
                number(s.end)
            );
            if audio {
                format!(
                    "{video};[0:a:0]atrim=start={}:end={},asetpts=PTS-STARTPTS[a{i}]",
                    number(s.start),
                    number(s.end)
                )
            } else {
                video
            }
        })
        .collect::<Vec<_>>();
    let inputs = (0..keep.len())
        .map(|i| {
            format!(
                "[v{i}]{}",
                if audio {
                    format!("[a{i}]")
                } else {
                    String::new()
                }
            )
        })
        .collect::<String>();
    let filter = format!(
        "{};{inputs}concat=n={}:v=1:a={}[v]{}",
        parts.join(";"),
        keep.len(),
        usize::from(audio),
        if audio { "[a]" } else { "" }
    );
    let mut args = vec![
        "-hide_banner".into(),
        "-nostats".into(),
        "-y".into(),
        "-i".into(),
        input.as_os_str().to_owned(),
        "-f".into(),
        "ffmetadata".into(),
        "-i".into(),
        metadata.as_os_str().to_owned(),
        "-filter_complex".into(),
        filter.into(),
        "-map".into(),
        "[v]".into(),
    ];
    if audio {
        args.extend(["-map", "[a]", "-c:a", "aac", "-b:a", "160k"].map(OsString::from));
    }
    args.extend(["-map_metadata", "1", "-map_chapters", "1"].map(OsString::from));
    args.extend(
        qualified_encoder_args(encoders)?
            .into_iter()
            .map(OsString::from),
    );
    args.extend([
        OsString::from("-movflags"),
        OsString::from("+faststart"),
        output.as_os_str().to_owned(),
    ]);
    Ok(EncodeRequest {
        args,
        metadata: ffmetadata(chapters, duration),
        metadata_path: metadata.to_owned(),
        input: input.to_owned(),
        output: output.to_owned(),
    })
}
pub struct EncodePlan<'a> {
    pub keep: &'a [Span],
    pub audio_streams: usize,
    pub chapters: &'a [Chapter],
    pub duration: f64,
}
pub fn probe_args(path: &Path) -> Vec<OsString> {
    [
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        "-show_chapters",
    ]
    .map(OsString::from)
    .into_iter()
    .chain([path.as_os_str().to_owned()])
    .collect()
}
pub fn take_log_chapters(value: &Value) -> Vec<Chapter> {
    value
        .get("chapters")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|v| v.is_object())
        .map(|v| Chapter {
            at: js_number(v.get("atSeconds")),
            name: js_string(v.get("name")),
        })
        .filter(|c| c.at.is_finite())
        .collect()
}
pub fn parse_silence(stderr: &str, duration: f64) -> Vec<Span> {
    let mut spans = vec![];
    let mut start = None;
    let mut rest = stderr;
    while let Some(offset) = rest.find("silence_") {
        rest = &rest[offset + 8..];
        let kind = if let Some(next) = rest.strip_prefix("start:") {
            rest = next;
            true
        } else if let Some(next) = rest.strip_prefix("end:") {
            rest = next;
            false
        } else {
            continue;
        };
        rest = rest
            .trim_start_matches(|c: char| c == '\u{feff}' || (c.is_whitespace() && c != '\u{85}'));
        let sign = usize::from(rest.starts_with('-'));
        let digits = rest[sign..]
            .bytes()
            .take_while(|c| c.is_ascii_digit() || *c == b'.')
            .count();
        let end = sign + digits;
        if digits == 0 {
            continue;
        }
        let token = &rest[..end];
        let value = js_number(Some(&Value::String(token.to_owned())));
        rest = &rest[end..];
        if kind {
            start = Some(max(0.0, value));
        } else if let Some(at) = start.take() {
            spans.push(Span {
                start: at,
                end: value,
            });
        }
    }
    if let Some(start) = start {
        spans.push(Span {
            start,
            end: duration,
        });
    }
    spans
}
#[derive(Clone, Debug, Serialize)]
pub struct IdleAnalysis {
    pub idle: Vec<Span>,
    pub audible: bool,
}
pub fn detect_idle(still: &[Span], stderr: &str, info: &MediaProbe) -> IdleAnalysis {
    if info.audio_streams == 0 {
        return IdleAnalysis {
            idle: still.to_vec(),
            audible: false,
        };
    }
    let silent = parse_silence(stderr, info.duration);
    if silent.len() == 1 && silent[0].start <= 0.1 && silent[0].end >= info.duration - 0.1 {
        return IdleAnalysis {
            idle: still.to_vec(),
            audible: false,
        };
    }
    IdleAnalysis {
        idle: still
            .iter()
            .flat_map(|f| {
                silent
                    .iter()
                    .map(|s| Span {
                        start: max(f.start, s.start),
                        end: min(f.end, s.end),
                    })
                    .filter(|s| s.end > s.start)
            })
            .collect(),
        audible: true,
    }
}
#[derive(Clone, Debug)]
pub struct SamplingRequest {
    pub args: Vec<OsString>,
    pub height: usize,
    pub frame_bytes: usize,
    pub duration: f64,
    pub min_idle_seconds: f64,
}
pub fn sampling_command(
    path: &Path,
    info: &MediaProbe,
    min_idle_seconds: f64,
    max_frame_bytes: usize,
) -> Result<SamplingRequest, TrimError> {
    let aspect = match (info.width, info.height) {
        (Some(w), Some(h)) if w != 0.0 && h != 0.0 && !w.is_nan() && !h.is_nan() => h / w,
        _ => 9.0 / 16.0,
    };
    let height = max(2.0, rounded(SAMPLE_WIDTH as f64 * aspect / 2.0) * 2.0);
    if !height.is_finite() || height > usize::MAX as f64 {
        return Err(TrimError::new(
            ErrorKind::Limit,
            "Grayscale sample dimensions exceed the configured bound",
        ));
    }
    let height = height as usize;
    let frame_bytes = SAMPLE_WIDTH
        .checked_mul(height)
        .filter(|n| *n <= max_frame_bytes)
        .ok_or_else(|| {
            TrimError::new(
                ErrorKind::Limit,
                "Grayscale sample dimensions exceed the configured bound",
            )
        })?;
    let mut args = vec![
        "-hide_banner".into(),
        "-nostats".into(),
        "-i".into(),
        path.as_os_str().to_owned(),
        "-map".into(),
        "0:v:0".into(),
        "-vf".into(),
        format!("fps=2,scale=96:{height},format=gray").into(),
        "-f".into(),
        "rawvideo".into(),
        "pipe:1".into(),
    ];
    if info.audio_streams > 0 {
        args.extend([
            OsString::from("-map"),
            OsString::from("0:a:0"),
            OsString::from("-af"),
            format!("silencedetect=n=-50dB:d={}", number(min_idle_seconds)).into(),
            "-f".into(),
            "null".into(),
            "-".into(),
        ]);
    }
    Ok(SamplingRequest {
        args,
        height,
        frame_bytes,
        duration: info.duration,
        min_idle_seconds,
    })
}
#[derive(Clone, Debug)]
pub struct GrayAnalysis {
    pub spans: Vec<Span>,
    pub frames: u64,
    pub trailing_bytes: usize,
    pub stderr: String,
}
pub struct GraySink {
    tracker: StillTracker,
    pending: Vec<u8>,
    frame_bytes: usize,
    duration: f64,
    frames: u64,
    max_spans: usize,
    stderr: Vec<u16>,
}
impl GraySink {
    pub fn new(request: &SamplingRequest, max_spans: usize) -> Result<Self, TrimError> {
        if request.frame_bytes == 0 || max_spans == 0 {
            return Err(TrimError::new(
                ErrorKind::Limit,
                "Grayscale stream bounds must be positive",
            ));
        }
        Ok(Self {
            tracker: StillTracker::new(request.min_idle_seconds),
            pending: Vec::with_capacity(request.frame_bytes),
            frame_bytes: request.frame_bytes,
            duration: request.duration,
            frames: 0,
            max_spans,
            stderr: vec![],
        })
    }
    pub fn consume(&mut self, mut bytes: &[u8]) -> Result<(), TrimError> {
        while !bytes.is_empty() {
            let count = (self.frame_bytes - self.pending.len()).min(bytes.len());
            self.pending.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.pending.len() == self.frame_bytes {
                self.tracker.push(
                    self.frames as f64 / SAMPLE_FPS,
                    std::mem::replace(&mut self.pending, Vec::with_capacity(self.frame_bytes)),
                );
                self.frames = self.frames.checked_add(1).ok_or_else(|| {
                    TrimError::new(ErrorKind::Limit, "Grayscale frame counter exhausted")
                })?;
                if self.tracker.spans().len() > self.max_spans {
                    return Err(TrimError::new(
                        ErrorKind::Limit,
                        "Still-span history exceeds the configured bound",
                    ));
                }
            }
        }
        Ok(())
    }
    pub fn consume_stderr(&mut self, bytes: &[u8]) {
        self.stderr
            .extend(String::from_utf8_lossy(bytes).encode_utf16());
        if self.stderr.len() > 4_000_000 {
            self.stderr.drain(..self.stderr.len() - 2_000_000);
        }
    }
    pub fn finish(mut self) -> Result<GrayAnalysis, TrimError> {
        let spans = self
            .tracker
            .finish(min(self.duration, self.frames as f64 / SAMPLE_FPS))
            .iter()
            .map(|s| Span {
                start: s.start_seconds,
                end: s.end_seconds,
            })
            .collect::<Vec<_>>();
        if spans.len() > self.max_spans {
            return Err(TrimError::new(
                ErrorKind::Limit,
                "Still-span history exceeds the configured bound",
            ));
        }
        Ok(GrayAnalysis {
            spans,
            frames: self.frames,
            trailing_bytes: self.pending.len(),
            stderr: String::from_utf16_lossy(&self.stderr),
        })
    }
}
impl crate::media_verification::StdoutSink for GraySink {
    type Output = Arc<GrayAnalysis>;
    fn consume(&mut self, chunk: &[u8]) -> Result<(), String> {
        GraySink::consume(self, chunk).map_err(|error| error.to_string())
    }
    fn consume_stderr(&mut self, chunk: &[u8]) -> Result<(), String> {
        GraySink::consume_stderr(self, chunk);
        Ok(())
    }
    fn finish(self) -> Result<Self::Output, String> {
        GraySink::finish(self)
            .map(Arc::new)
            .map_err(|error| error.to_string())
    }
}
#[derive(Clone, Debug)]
pub struct SheetRequest {
    pub path: PathBuf,
    pub times: Vec<f64>,
    pub columns: usize,
    pub width: usize,
    pub max_image_bytes: usize,
}
pub fn contact_sheet_times(
    explicit: Option<&[f64]>,
    chapters: &[Chapter],
    duration: f64,
) -> Vec<f64> {
    explicit
        .map(|v| v.to_vec())
        .unwrap_or_else(|| {
            if chapters.is_empty() {
                (0..8).map(|i| (i as f64 + 0.5) * duration / 8.0).collect()
            } else {
                chapters.iter().map(|c| c.at + 1.0).collect()
            }
        })
        .into_iter()
        .filter(|t| *t < duration)
        .take(MAX_SHEET_FRAMES)
        .collect()
}
pub fn contact_sheet_commands(
    request: &SheetRequest,
    work: &Path,
) -> Result<Vec<Vec<OsString>>, TrimError> {
    if !(1..=8).contains(&request.columns)
        || !(80..=640).contains(&request.width)
        || request.times.is_empty()
        || request.times.len() > MAX_SHEET_FRAMES
        || request
            .times
            .iter()
            .any(|time| !time.is_finite() || *time < 0.0)
    {
        return Err(evidence(
            "Contact-sheet request is outside the supported schema",
        ));
    }
    let mut commands = request
        .times
        .iter()
        .enumerate()
        .map(|(index, time)| {
            vec![
                "-hide_banner".into(),
                "-v".into(),
                "error".into(),
                "-y".into(),
                "-ss".into(),
                number(max(0.0, *time)).into(),
                "-i".into(),
                request.path.as_os_str().to_owned(),
                "-frames:v".into(),
                "1".into(),
                "-vf".into(),
                format!("scale={}:-2", request.width).into(),
                "-q:v".into(),
                "4".into(),
                work.join(format!("{index:03}.jpg")).into_os_string(),
            ]
        })
        .collect::<Vec<_>>();
    let rows = request.times.len().div_ceil(request.columns);
    commands.push(vec![
        "-hide_banner".into(),
        "-v".into(),
        "error".into(),
        "-y".into(),
        "-framerate".into(),
        "1".into(),
        "-i".into(),
        work.join("%03d.jpg").into_os_string(),
        "-vf".into(),
        format!(
            "tile={}x{rows}:padding=4:color=white",
            request.columns.min(request.times.len())
        )
        .into(),
        "-frames:v".into(),
        "1".into(),
        "-q:v".into(),
        "4".into(),
        work.join("sheet.jpg").into_os_string(),
    ]);
    Ok(commands)
}

/// Closed phase-specific jobs. Concrete owners retain actors across a cancelled
/// `finish` await and report success only after terminal status and pipe EOF.
pub type JobFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, TrimError>> + Send + 'a>>;
pub trait OwnedJob<T>: Send {
    fn cancel(&self);
    fn finish(&mut self) -> JobFuture<'_, T>;
}
pub trait Encode {
    fn encode(
        &mut self,
        request: &EncodeRequest,
        cancellation: Cancellation,
    ) -> Result<Box<dyn OwnedJob<()>>, TrimError>;
}
pub trait Probe {
    fn probe(
        &mut self,
        path: &Path,
        cancellation: Cancellation,
    ) -> Result<Box<dyn OwnedJob<MediaProbe>>, TrimError>;
}
pub trait Decode {
    fn decode(
        &mut self,
        path: &Path,
        cancellation: Cancellation,
    ) -> Result<Box<dyn OwnedJob<()>>, TrimError>;
}
pub trait Preview {
    fn preview(
        &mut self,
        request: &SheetRequest,
        cancellation: Cancellation,
    ) -> Result<Box<dyn OwnedJob<Arc<[u8]>>>, TrimError>;
}
pub trait Stream {
    fn sample(
        &mut self,
        request: &SamplingRequest,
        sink: GraySink,
        cancellation: Cancellation,
    ) -> Result<Box<dyn OwnedJob<GrayAnalysis>>, TrimError>;
}
/// A testable cleanup seam. The publisher checks owned inode identity before
/// passing this one stage path. Implementations must touch only that path.
pub trait StageCleanup: Send {
    fn remove(&mut self, path: &Path) -> io::Result<()>;
}
pub struct FilesystemCleanup;
impl StageCleanup for FilesystemCleanup {
    fn remove(&mut self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }
}
pub struct ExportStage {
    root: PathBuf,
    partial: PathBuf,
    output: PathBuf,
    root_identity: Metadata,
    file: File,
    identity: Metadata,
    committed: bool,
    cleaned: bool,
    cleanup: Box<dyn StageCleanup>,
}
#[derive(Clone)]
pub struct CommittedFile {
    pub output_path: PathBuf,
    pub sha256: String,
    identity: Arc<Metadata>,
}
impl fmt::Debug for CommittedFile {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("CommittedFile")
            .field("sha256", &self.sha256)
            .finish_non_exhaustive()
    }
}
impl CommittedFile {
    pub fn still_names_committed_file(&self) -> bool {
        fs::symlink_metadata(&self.output_path)
            .is_ok_and(|m| regular(&m) && same_version(&m, &self.identity))
    }
}
impl ExportStage {
    pub fn new(root: &Path, partial_name: &str, output_name: &str) -> Result<Self, TrimError> {
        Self::with_cleanup(root, partial_name, output_name, Box::new(FilesystemCleanup))
    }
    pub fn with_cleanup(
        root: &Path,
        partial_name: &str,
        output_name: &str,
        cleanup: Box<dyn StageCleanup>,
    ) -> Result<Self, TrimError> {
        trusted_directory(root)?;
        for name in [partial_name, output_name] {
            let mut components = Path::new(name).components();
            if name.is_empty()
                || !matches!(components.next(), Some(Component::Normal(_)))
                || components.next().is_some()
            {
                return Err(TrimError::new(
                    ErrorKind::UnsafePath,
                    "Export filenames must name files in the approved directory",
                ));
            }
        }
        if partial_name == output_name {
            return Err(evidence(
                "Could not choose a temporary name for the export; try again",
            ));
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let partial = root.join(partial_name);
        let file = options.open(&partial).map_err(|_| {
            TrimError::new(
                ErrorKind::Storage,
                "Could not reserve an owned temporary export",
            )
        })?;
        let identity = file.metadata().map_err(|_| {
            TrimError::new(ErrorKind::Storage, "Could not inspect the temporary export")
        })?;
        Ok(Self {
            root: root.to_owned(),
            partial,
            output: root.join(output_name),
            root_identity: fs::symlink_metadata(root).map_err(|_| {
                TrimError::new(ErrorKind::UnsafePath, "Export directory is unavailable")
            })?,
            file,
            identity,
            committed: false,
            cleaned: false,
            cleanup,
        })
    }
    pub fn partial_path(&self) -> &Path {
        &self.partial
    }
    pub fn output_path(&self) -> &Path {
        &self.output
    }
    fn check(&self) -> Result<Metadata, TrimError> {
        trusted_directory(&self.root)?;
        let root = fs::symlink_metadata(&self.root).map_err(|_| {
            TrimError::new(ErrorKind::UnsafePath, "Export directory is unavailable")
        })?;
        let named = fs::symlink_metadata(&self.partial).map_err(|_| {
            TrimError::new(
                ErrorKind::StageChanged,
                "Owned temporary export is missing or replaced",
            )
        })?;
        let opened = self.file.metadata().map_err(|_| {
            TrimError::new(
                ErrorKind::StageChanged,
                "Owned temporary export is unavailable",
            )
        })?;
        if !same_file(&root, &self.root_identity)
            || !regular(&named)
            || !regular(&opened)
            || !same_file(&self.identity, &named)
            || !same_version(&named, &opened)
        {
            return Err(TrimError::new(
                ErrorKind::StageChanged,
                "Owned temporary export identity changed",
            ));
        }
        Ok(opened)
    }
    fn publish(
        &mut self,
        verified: &Metadata,
        cancellation: &Cancellation,
    ) -> Result<CommittedFile, TrimError> {
        let before = self.check()?;
        if !same_version(&before, verified) {
            return Err(TrimError::new(
                ErrorKind::StageChanged,
                "Staged export changed after verification",
            ));
        }
        let mut reader = self
            .file
            .try_clone()
            .map_err(|_| TrimError::new(ErrorKind::Storage, "Staged export could not be hashed"))?;
        use std::io::Seek;
        reader
            .rewind()
            .map_err(|_| TrimError::new(ErrorKind::Storage, "Staged export could not be hashed"))?;
        let mut hash = Sha256::new();
        let mut bytes = [0u8; 64 * 1024];
        loop {
            cancelled(cancellation)?;
            let count = reader.read(&mut bytes).map_err(|_| {
                TrimError::new(ErrorKind::Storage, "Staged export could not be hashed")
            })?;
            if count == 0 {
                break;
            }
            hash.update(&bytes[..count]);
        }
        let after = self.check()?;
        if !same_version(&before, &after) {
            return Err(TrimError::new(
                ErrorKind::StageChanged,
                "Staged export changed while hashing",
            ));
        }
        cancelled(cancellation)?;
        fs::hard_link(&self.partial, &self.output).map_err(|error| {
            TrimError::new(
                if error.kind() == io::ErrorKind::AlreadyExists {
                    ErrorKind::DestinationExists
                } else {
                    ErrorKind::Storage
                },
                if error.kind() == io::ErrorKind::AlreadyExists {
                    "Export destination already exists"
                } else {
                    "Export could not be published without overwriting"
                },
            )
        })?;
        self.committed = true;
        Ok(CommittedFile {
            output_path: self.output.clone(),
            sha256: format!("{:x}", hash.finalize()),
            identity: Arc::new(after),
        })
    }
    pub fn cleanup(&mut self) -> Result<(), TrimError> {
        if self.cleaned {
            return Ok(());
        }
        let named = match fs::symlink_metadata(&self.partial) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                self.cleaned = true;
                return Ok(());
            }
            Err(_) => {
                return Err(TrimError::new(
                    ErrorKind::Storage,
                    "Temporary export cleanup failed",
                ));
            }
        };
        if !regular(&named) || !same_file(&named, &self.identity) {
            return Err(TrimError::new(
                ErrorKind::StageChanged,
                "Temporary export cleanup refused a replaced path",
            ));
        }
        self.cleanup
            .remove(&self.partial)
            .map_err(|_| TrimError::new(ErrorKind::Storage, "Temporary export cleanup failed"))?;
        self.cleaned = true;
        Ok(())
    }
}
impl Drop for ExportStage {
    fn drop(&mut self) {
        if !self.cleaned {
            let _ = self.cleanup();
        }
    }
}
fn trusted_directory(path: &Path) -> Result<(), TrimError> {
    if !path.is_absolute() {
        return Err(TrimError::new(
            ErrorKind::UnsafePath,
            "Export directory must be an explicit absolute directory",
        ));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::Normal(_) | Component::Prefix(_) => {
                current.push(component.as_os_str())
            }
            _ => {
                return Err(TrimError::new(
                    ErrorKind::UnsafePath,
                    "Export directory contains unsupported components",
                ));
            }
        }
        let metadata = fs::symlink_metadata(&current).map_err(|_| {
            TrimError::new(ErrorKind::UnsafePath, "Export directory is unavailable")
        })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(TrimError::new(
                ErrorKind::UnsafePath,
                "Export directory cannot traverse a symlink",
            ));
        }
    }
    Ok(())
}
fn regular(metadata: &Metadata) -> bool {
    metadata.is_file() && !metadata.file_type().is_symlink()
}
fn same_file(a: &Metadata, b: &Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        a.dev() == b.dev() && a.ino() == b.ino()
    }
    #[cfg(not(unix))]
    {
        a.created().ok() == b.created().ok() && a.is_file() == b.is_file()
    }
}
fn same_version(a: &Metadata, b: &Metadata) -> bool {
    same_file(a, b) && a.len() == b.len() && a.modified().ok() == b.modified().ok()
}
#[derive(Clone)]
pub struct CommittedExport {
    pub file: CommittedFile,
    pub verified: MediaProbe,
    pub preview: Option<Arc<[u8]>>,
    pub preview_warning: Option<String>,
    pub cleanup_warning: Option<String>,
}
impl fmt::Debug for CommittedExport {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("CommittedExport")
            .field("file", &self.file)
            .field("preview_bytes", &self.preview.as_ref().map(|v| v.len()))
            .finish_non_exhaustive()
    }
}
pub struct ExportOwner<B> {
    backend: B,
    stage: ExportStage,
    encode_request: EncodeRequest,
    expected: ExpectedMetadata,
    preview_request: Option<SheetRequest>,
    cancellation: Cancellation,
    encode_job: Option<Box<dyn OwnedJob<()>>>,
    probe_job: Option<Box<dyn OwnedJob<MediaProbe>>>,
    decode_job: Option<Box<dyn OwnedJob<()>>>,
    preview_job: Option<Box<dyn OwnedJob<Arc<[u8]>>>>,
    encoded: bool,
    probed: Option<MediaProbe>,
    verified_stamp: Option<Metadata>,
    decoded: bool,
    committed: Option<CommittedExport>,
    outcome: Option<Result<CommittedExport, TrimError>>,
}
impl<B: Encode + Probe + Decode + Preview> ExportOwner<B> {
    pub fn new(
        backend: B,
        stage: ExportStage,
        encode_request: EncodeRequest,
        source: &MediaProbe,
        expected: ExpectedMetadata,
        preview_request: Option<SheetRequest>,
        cancellation: Cancellation,
    ) -> Result<Self, TrimError> {
        require_sdr_input(source)?;
        if encode_request.output != stage.partial
            || encode_request.input == stage.partial
            || encode_request.input == stage.output
        {
            return Err(evidence("The trimmed copy cannot replace the original"));
        }
        if let Some(request) = &preview_request
            && (request.path != stage.output || request.max_image_bytes == 0)
        {
            return Err(evidence(
                "Contact-sheet publication target or byte bound is invalid",
            ));
        }
        Ok(Self {
            backend,
            stage,
            encode_request,
            expected,
            preview_request,
            cancellation,
            encode_job: None,
            probe_job: None,
            decode_job: None,
            preview_job: None,
            encoded: false,
            probed: None,
            verified_stamp: None,
            decoded: false,
            committed: None,
            outcome: None,
        })
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
        if let Some(job) = &self.encode_job {
            job.cancel()
        }
        if let Some(job) = &self.probe_job {
            job.cancel()
        }
        if let Some(job) = &self.decode_job {
            job.cancel()
        }
        if let Some(job) = &self.preview_job {
            job.cancel()
        }
    }
    pub async fn finish(&mut self) -> Result<CommittedExport, TrimError> {
        if let Some(result) = &self.outcome {
            return result.clone();
        }
        let result = self.advance().await;
        match result {
            Ok(result) => {
                self.outcome = Some(Ok(result.clone()));
                Ok(result)
            }
            Err(mut error) => {
                if let Some(issue) = self.drain_jobs().await {
                    error.cleanup_issue = Some(issue);
                }
                if let Err(cleanup) = self.stage.cleanup() {
                    error.cleanup_issue = Some(match error.cleanup_issue.take() {
                        Some(issue) => format!("{issue}; {cleanup}"),
                        None => cleanup.to_string(),
                    });
                }
                self.outcome = Some(Err(error.clone()));
                Err(error)
            }
        }
    }
    async fn drain_jobs(&mut self) -> Option<String> {
        let mut issues = vec![];
        if let Some(issue) = settle(&mut self.encode_job).await {
            issues.push(issue)
        }
        if let Some(issue) = settle(&mut self.probe_job).await {
            issues.push(issue)
        }
        if let Some(issue) = settle(&mut self.decode_job).await {
            issues.push(issue)
        }
        if let Some(issue) = settle(&mut self.preview_job).await {
            issues.push(issue)
        }
        if issues.is_empty() {
            None
        } else {
            Some(issues.join("; "))
        }
    }
    async fn advance(&mut self) -> Result<CommittedExport, TrimError> {
        if self.committed.is_none() {
            cancelled(&self.cancellation)?;
            if !self.encoded {
                if self.encode_job.is_none() {
                    self.encode_job = Some(
                        self.backend
                            .encode(&self.encode_request, self.cancellation.clone())?,
                    );
                }
                self.encode_job.as_mut().unwrap().finish().await?;
                self.encode_job.take();
                self.encoded = true;
            }
            cancelled(&self.cancellation)?;
            if self.probed.is_none() {
                if self.probe_job.is_none() {
                    self.verified_stamp = Some(self.stage.check()?);
                    self.probe_job = Some(
                        self.backend
                            .probe(self.stage.partial_path(), self.cancellation.clone())?,
                    );
                }
                let info = self.probe_job.as_mut().unwrap().finish().await?;
                validate_trimmed_metadata(&info, &self.expected)?;
                self.probe_job.take();
                self.probed = Some(info);
            }
            if !self.decoded {
                if self.decode_job.is_none() {
                    self.decode_job = Some(
                        self.backend
                            .decode(self.stage.partial_path(), self.cancellation.clone())?,
                    );
                }
                self.decode_job.as_mut().unwrap().finish().await?;
                self.decode_job.take();
                self.decoded = true;
            }
            let file = self
                .stage
                .publish(self.verified_stamp.as_ref().unwrap(), &self.cancellation)?;
            let cleanup_warning = self
                .stage
                .cleanup()
                .err()
                .map(|e| format!("Export committed; temporary-file cleanup failed: {e}"));
            self.committed = Some(CommittedExport {
                file,
                verified: self.probed.clone().unwrap(),
                preview: None,
                preview_warning: None,
                cleanup_warning,
            });
        }
        if let Some(request) = &self.preview_request {
            let preview = if let Some(job) = &mut self.preview_job {
                job.finish().await
            } else {
                match self.backend.preview(request, self.cancellation.clone()) {
                    Ok(job) => {
                        self.preview_job = Some(job);
                        self.preview_job.as_mut().unwrap().finish().await
                    }
                    Err(error) => Err(error),
                }
            };
            let committed = self.committed.as_mut().unwrap();
            match preview{Ok(bytes)if bytes.len()<=request.max_image_bytes=>committed.preview=Some(bytes),Ok(_)=>committed.preview_warning=Some("Export committed; contact sheet unavailable: Contact sheet exceeds the configured byte bound".into()),Err(error)=>committed.preview_warning=Some(format!("Export committed; contact sheet unavailable: {error}"))}
            self.preview_job.take();
        }
        Ok(self.committed.clone().unwrap())
    }
}
async fn settle<T>(job: &mut Option<Box<dyn OwnedJob<T>>>) -> Option<String> {
    if let Some(owned) = job {
        owned.cancel();
        let issue = owned.finish().await.err().and_then(|e| e.cleanup_issue);
        job.take();
        issue
    } else {
        None
    }
}
impl<B> Drop for ExportOwner<B> {
    fn drop(&mut self) {
        if self.outcome.is_some() {
            return;
        }
        self.cancellation.cancel();
        if let Some(job) = &self.encode_job {
            job.cancel()
        }
        if let Some(job) = &self.probe_job {
            job.cancel()
        }
        if let Some(job) = &self.decode_job {
            job.cancel()
        }
        if let Some(job) = &self.preview_job {
            job.cancel()
        }
    }
}
