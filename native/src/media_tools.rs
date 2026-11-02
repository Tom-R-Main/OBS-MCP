// SPDX-License-Identifier: GPL-2.0-only
//! Owned recording-media operations, without MCP routing or an OBS socket.
//! The service retains Operation through finish and cleanup. Dropping a frontend
//! finish await preserves every admitted provider/media future in this owner.
//! Dropping the owner itself is emergency cancellation, not joined cleanup.
use crate::{
    media_backend::{Bounds as MediaBounds, SystemMedia},
    media_verification::{
        self, Cancellation, LocalMediaInput, MediaProcess, ProcessOptions,
        local_media_input_options,
    },
    trim::{
        self, Chapter, CutOptions, Decode, Encode, EncodeRequest, ErrorKind, ExportOwner,
        ExportStage, GrayAnalysis, GraySink, JobFuture, MediaProbe, OwnedJob, Preview, Probe,
        SheetRequest, Timeline, TrimError,
    },
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fmt,
    fs::{self, File, Metadata},
    future::Future,
    io::Read,
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

pub type DirectoryFuture = Pin<Box<dyn Future<Output = Result<PathBuf, String>> + Send + 'static>>;
/// One closed read, bound by its adapter to the service-owned OBS connection.
/// The provider must settle independently within its own bounded deadline and
/// must not reconnect, own a socket, or mutate recording configuration.
pub trait RecordingDirectory: Send + Sync + 'static {
    fn current_directory(&self) -> DirectoryFuture;
}

#[derive(Clone)]
pub struct Config {
    authority: Arc<Authority>,
    ffmpeg: PathBuf,
    ffmpeg_identity: Metadata,
    ffprobe: PathBuf,
    ffprobe_identity: Metadata,
    work: PathBuf,
    work_identity: Metadata,
    pub media_bounds: MediaBounds,
    pub sampling_timeout: Duration,
    pub max_frame_bytes: usize,
    pub max_spans: usize,
    pub max_sidecar_bytes: usize,
    pub max_image_bytes: usize,
}
impl Config {
    pub fn new(
        root: &Path,
        ffmpeg: &Path,
        ffprobe: &Path,
        private_work: &Path,
    ) -> Result<Self, TrimError> {
        let authority = Arc::new(Authority::new(root)?);
        SystemMedia::new(ffmpeg, ffprobe, private_work, MediaBounds::default())?;
        let ffmpeg = ffmpeg.canonicalize().map_err(|_| {
            err(
                ErrorKind::Unsupported,
                "Configured media executable is unavailable",
            )
        })?;
        let ffmpeg_identity = fs::symlink_metadata(&ffmpeg).map_err(|_| {
            err(
                ErrorKind::Unsupported,
                "Configured media executable is unavailable",
            )
        })?;
        let ffprobe = ffprobe.canonicalize().map_err(|_| {
            err(
                ErrorKind::Unsupported,
                "Configured media executable is unavailable",
            )
        })?;
        let ffprobe_identity = fs::symlink_metadata(&ffprobe).map_err(|_| {
            err(
                ErrorKind::Unsupported,
                "Configured media executable is unavailable",
            )
        })?;
        let work_identity = fs::symlink_metadata(private_work)
            .map_err(|_| err(ErrorKind::UnsafePath, "Media work directory is unavailable"))?;
        for identity in [&ffmpeg_identity, &ffprobe_identity] {
            executable_file(identity)?;
        }
        Ok(Self {
            authority,
            ffmpeg,
            ffmpeg_identity,
            ffprobe,
            ffprobe_identity,
            work: private_work.to_owned(),
            work_identity,
            media_bounds: MediaBounds::default(),
            sampling_timeout: trim::SAMPLE_TIMEOUT,
            max_frame_bytes: 1024 * 1024,
            max_spans: 100_000,
            max_sidecar_bytes: 1024 * 1024,
            max_image_bytes: 32 * 1024 * 1024,
        })
    }
    fn validate(&self) -> Result<(), TrimError> {
        if self.sampling_timeout.is_zero()
            || self.sampling_timeout > trim::SAMPLE_TIMEOUT
            || !(1..=1024 * 1024).contains(&self.max_frame_bytes)
            || !(1..=100_000).contains(&self.max_spans)
            || !(1..=1024 * 1024).contains(&self.max_sidecar_bytes)
            || !(1..=32 * 1024 * 1024).contains(&self.max_image_bytes)
        {
            return Err(err(
                ErrorKind::Limit,
                "Recording media bounds are unavailable",
            ));
        }
        for (path, identity) in [
            (&self.ffmpeg, &self.ffmpeg_identity),
            (&self.ffprobe, &self.ffprobe_identity),
        ] {
            let current = fs::symlink_metadata(path).map_err(|_| {
                err(
                    ErrorKind::Unsupported,
                    "Configured media executable is unavailable",
                )
            })?;
            executable_file(&current)?;
            if !unchanged(&current, identity) {
                return Err(err(
                    ErrorKind::UnsafePath,
                    "Configured media executable identity changed",
                ));
            }
        }
        let work = fs::symlink_metadata(&self.work)
            .map_err(|_| err(ErrorKind::UnsafePath, "Media work directory is unavailable"))?;
        if !work.is_dir()
            || work.file_type().is_symlink()
            || !same(&work, &self.work_identity)
            || self.work.canonicalize().ok().as_deref() != Some(self.work.as_path())
        {
            return Err(err(
                ErrorKind::UnsafePath,
                "Media work directory identity changed",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if work.mode() & 0o077 != 0 {
                return Err(err(
                    ErrorKind::UnsafePath,
                    "Media work directory must be private",
                ));
            }
        }
        self.authority.check()
    }
}
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MediaToolsConfig([REDACTED])")
    }
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SheetArgs {
    pub path: String,
    pub times: Option<Vec<f64>>,
    #[serde(default = "four")]
    pub columns: usize,
    #[serde(default = "width")]
    pub width: usize,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrimArgs {
    pub path: String,
    #[serde(default)]
    pub apply: bool,
    pub output_path: Option<String>,
    #[serde(default = "idle")]
    pub min_idle_seconds: f64,
    #[serde(default = "one")]
    pub keep_seconds: f64,
    #[serde(default = "after")]
    pub hold_after_chapter_seconds: f64,
    #[serde(default = "before")]
    pub hold_before_chapter_seconds: f64,
    #[serde(default = "yes")]
    pub contact_sheet: bool,
}
fn four() -> usize {
    4
}
fn width() -> usize {
    320
}
fn idle() -> f64 {
    4.0
}
fn one() -> f64 {
    1.0
}
fn after() -> f64 {
    1.5
}
fn before() -> f64 {
    3.0
}
fn yes() -> bool {
    true
}
#[derive(Clone)]
pub enum Request {
    ContactSheet(SheetArgs),
    TrimTake(TrimArgs),
}
impl Request {
    pub fn parse(tool: &str, arguments: &Value) -> Result<Self, TrimError> {
        let invalid = || err(ErrorKind::Evidence, "Invalid recording media arguments");
        if !arguments.is_object() {
            return Err(invalid());
        }
        let request = match tool {
            "obs-contact-sheet" => {
                if arguments.get("times").is_some_and(Value::is_null) {
                    return Err(invalid());
                }
                Self::ContactSheet(
                    serde_json::from_value(arguments.clone()).map_err(|_| invalid())?,
                )
            }
            "obs-trim-take" => {
                if arguments.get("outputPath").is_some_and(Value::is_null) {
                    return Err(invalid());
                }
                Self::TrimTake(serde_json::from_value(arguments.clone()).map_err(|_| invalid())?)
            }
            _ => return Err(invalid()),
        };
        request.validate()?;
        Ok(request)
    }
    fn validate(&self) -> Result<(), TrimError> {
        let valid = match self {
            Self::ContactSheet(a) => {
                (1..=8).contains(&a.columns)
                    && (80..=640).contains(&a.width)
                    && a.times.as_ref().is_none_or(|t| {
                        t.len() <= 24 && t.iter().all(|v| v.is_finite() && *v >= 0.0)
                    })
            }
            Self::TrimTake(a) => [
                (a.min_idle_seconds, 1.0, 60.0),
                (a.keep_seconds, 0.0, 5.0),
                (a.hold_after_chapter_seconds, 0.0, 10.0),
                (a.hold_before_chapter_seconds, 0.0, 10.0),
            ]
            .iter()
            .all(|(v, min, max)| v.is_finite() && v >= min && v <= max),
        };
        if valid {
            Ok(())
        } else {
            Err(err(
                ErrorKind::Evidence,
                "Invalid recording media arguments",
            ))
        }
    }
    fn path(&self) -> &str {
        match self {
            Self::ContactSheet(a) => &a.path,
            Self::TrimTake(a) => &a.path,
        }
    }
    fn prefix(&self) -> &str {
        match self {
            Self::ContactSheet(_) => "Contact sheet failed",
            Self::TrimTake(_) => "Trim failed",
        }
    }
}
impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MediaToolsRequest([REDACTED])")
    }
}

/// Single service-owned operation. No worker is detached, and no phase respawns
/// when a caller abandons an await and later resumes this same owner.
pub struct Operation {
    config: Config,
    provider: Arc<dyn RecordingDirectory>,
    request: Request,
    cancellation: Cancellation,
    directory_future: Option<DirectoryFuture>,
    source: Option<Recording>,
    info: Option<MediaProbe>,
    chapters: Option<Vec<Chapter>>,
    backend: Option<SystemMedia>,
    probe_job: Option<Box<dyn OwnedJob<MediaProbe>>>,
    sample_job: Option<MediaProcess<Arc<GrayAnalysis>>>,
    encoder_job: Option<Box<dyn OwnedJob<Arc<[u8]>>>>,
    sheet_job: Option<Box<dyn OwnedJob<Arc<[u8]>>>>,
    plan: Option<Timeline>,
    output: Option<PathBuf>,
    export: Option<ExportOwner<GuardedBackend>>,
    result: Option<Value>,
}
impl fmt::Debug for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MediaOperation")
            .field("complete", &self.result.is_some())
            .finish_non_exhaustive()
    }
}
impl Operation {
    pub fn new(
        config: Config,
        provider: Arc<dyn RecordingDirectory>,
        request: Request,
        cancellation: Cancellation,
    ) -> Result<Self, TrimError> {
        config.validate()?;
        request.validate()?;
        let backend = Some(SystemMedia::new(
            &config.ffmpeg,
            &config.ffprobe,
            &config.work,
            config.media_bounds.clone(),
        )?);
        Ok(Self {
            config,
            provider,
            request,
            cancellation,
            directory_future: None,
            source: None,
            info: None,
            chapters: None,
            backend,
            probe_job: None,
            sample_job: None,
            encoder_job: None,
            sheet_job: None,
            plan: None,
            output: None,
            export: None,
            result: None,
        })
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
    pub async fn finish(&mut self) -> Value {
        if let Some(result) = &self.result {
            return result.clone();
        }
        let mut result = match self.advance().await {
            Ok(result) => result,
            Err(error) => error_result(&format!("{}: {error}", self.request.prefix())),
        };
        source_json_numbers(&mut result);
        self.result = Some(result.clone());
        result
    }
    async fn directory(&mut self) -> Result<(), TrimError> {
        if self.directory_future.is_none() {
            self.directory_future = Some(self.provider.current_directory());
        }
        let root = self.directory_future.as_mut().unwrap().await.map_err(|_| {
            err(
                ErrorKind::Evidence,
                "Recording directory evidence is unavailable",
            )
        })?;
        self.directory_future.take();
        self.config.authority.current(&root)?;
        cancelled(&self.cancellation)
    }
    async fn advance(&mut self) -> Result<Value, TrimError> {
        if self.source.is_none() {
            self.directory().await?;
            self.source = Some(self.config.authority.recording(self.request.path())?);
        }
        if self.info.is_none() {
            if self.probe_job.is_none() {
                self.config.validate()?;
                self.probe_job = Some(self.backend.as_mut().unwrap().probe(
                    &self.source.as_ref().unwrap().path,
                    self.cancellation.clone(),
                )?);
            }
            self.info = Some(self.probe_job.as_mut().unwrap().finish().await?);
            self.source
                .as_ref()
                .unwrap()
                .check(&self.config.authority)?;
        }
        let info = self.info.as_ref().unwrap().clone();
        let source = self.source.as_ref().unwrap().clone();
        if let Request::TrimTake(args) = &self.request {
            if !info.duration.is_finite() {
                return Ok(error_result(&format!(
                    "ffprobe could not read the length of {}",
                    source.path.display()
                )));
            }
            if args.apply {
                trim::require_sdr_input(&info)?;
            }
        }
        if self.chapters.is_none() {
            self.chapters = Some(if info.chapters.is_empty() {
                self.config
                    .authority
                    .sidecar(&source.path, self.config.max_sidecar_bytes)?
            } else {
                info.chapters.clone()
            });
        }
        let chapters = self.chapters.as_ref().unwrap().clone();
        if let Request::ContactSheet(args) = &self.request {
            let times = trim::contact_sheet_times(args.times.as_deref(), &chapters, info.duration);
            if times.is_empty() {
                return Ok(error_result("No requested time falls inside the recording"));
            }
            if self.sheet_job.is_none() {
                self.config.validate()?;
                self.sheet_job = Some(self.backend.as_mut().unwrap().preview(
                    &SheetRequest {
                        path: source.path.clone(),
                        times: times.clone(),
                        columns: args.columns,
                        width: args.width,
                        max_image_bytes: self.config.max_image_bytes,
                    },
                    self.cancellation.clone(),
                )?);
            }
            let image = self.sheet_job.as_mut().unwrap().finish().await?;
            source.check(&self.config.authority)?;
            let legend = times
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    format!(
                        "{}. {}s{}",
                        i + 1,
                        number(trim::round_seconds(*t)),
                        if args.times.is_none() {
                            chapters
                                .get(i)
                                .filter(|c| !c.name.is_empty())
                                .map(|c| format!(" {}", c.name))
                                .unwrap_or_default()
                        } else {
                            String::new()
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            return Ok(
                json!({"content":[{"type":"text","text":format!("{} frame(s) from {}, left to right, top to bottom:\n{legend}",times.len(),source.path.display())},image_content(&image)]}),
            );
        }
        let Request::TrimTake(args) = self.request.clone() else {
            unreachable!()
        };
        if self.plan.is_none() {
            if self.sample_job.is_none() {
                self.config.validate()?;
                source.check(&self.config.authority)?;
                let request = trim::sampling_command(
                    &source.path,
                    &info,
                    args.min_idle_seconds,
                    self.config.max_frame_bytes,
                )?;
                let mut arguments = Vec::new();
                for arg in &request.args {
                    if arg == "-i" {
                        arguments.extend(local_media_input_options(LocalMediaInput::Recording));
                    }
                    arguments.push(arg.clone());
                }
                self.sample_job = Some(
                    media_verification::run_streamed_media_process(
                        &self.config.ffmpeg,
                        &arguments,
                        ProcessOptions {
                            timeout: self.config.sampling_timeout,
                            cancellation: self.cancellation.clone(),
                        },
                        GraySink::new(&request, self.config.max_spans)?,
                    )
                    .map_err(sample_error)?,
                );
            }
            let samples = self
                .sample_job
                .as_mut()
                .unwrap()
                .finish()
                .await
                .map_err(sample_error)?;
            source.check(&self.config.authority)?;
            if samples.trailing_bytes != 0 {
                return Err(err(
                    ErrorKind::Evidence,
                    "Grayscale sample ended with an incomplete frame",
                ));
            }
            let idle = trim::detect_idle(&samples.spans, &samples.stderr, &info);
            self.plan = Some(trim::timeline(
                source
                    .path
                    .to_str()
                    .ok_or_else(|| err(ErrorKind::UnsafePath, "Recording path is not UTF-8"))?,
                &CutOptions {
                    duration: info.duration,
                    idle: idle.idle,
                    chapters,
                    min_idle_seconds: args.min_idle_seconds,
                    keep_seconds: args.keep_seconds,
                    hold_after_chapter_seconds: args.hold_after_chapter_seconds,
                    hold_before_chapter_seconds: args.hold_before_chapter_seconds,
                },
                idle.audible,
            ));
        }
        let plan = self.plan.as_ref().unwrap().clone();
        let mut report = trim::plan_report(&plan, args.min_idle_seconds, args.apply);
        if !args.apply || plan.cuts.is_empty() {
            return Ok(report);
        }
        if self.output.is_none() {
            self.directory().await?;
            let default = source.path.with_file_name(format!(
                "{}.trimmed.mp4",
                source
                    .path
                    .file_stem()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| err(ErrorKind::UnsafePath, "Recording path is not UTF-8"))?
            ));
            let output = self.config.authority.output(
                args.output_path
                    .as_deref()
                    .map(Path::new)
                    .unwrap_or(&default),
            )?;
            if output == source.path {
                return Ok(error_result("The trimmed copy cannot replace the original"));
            }
            if fs::symlink_metadata(&output).is_ok() {
                return Ok(error_result(&format!(
                    "{} already exists; choose another outputPath",
                    output.display()
                )));
            }
            self.output = Some(output);
        }
        let output = self.output.as_ref().unwrap().clone();
        if self.export.is_none() {
            if self.encoder_job.is_none() {
                self.config.validate()?;
                self.encoder_job = Some(
                    self.backend
                        .as_ref()
                        .unwrap()
                        .encoders(self.cancellation.clone())?,
                );
            }
            let encoders = self.encoder_job.as_mut().unwrap().finish().await?;
            let encoders = std::str::from_utf8(&encoders)
                .map_err(|_| err(ErrorKind::Evidence, "FFmpeg encoder list is not UTF-8"))?;
            source.check(&self.config.authority)?;
            let parent = output.parent().unwrap();
            let name = output.file_name().unwrap().to_str().unwrap();
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let sequence = NEXT
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .map_err(|_| err(ErrorKind::Limit, "Export temporary identity exhausted"))?;
            let partial = format!(
                ".{name}.{}-{sequence}.partial{}",
                std::process::id(),
                output
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|s| format!(".{s}"))
                    .unwrap_or_default()
            );
            let stage = ExportStage::new(parent, &partial, name)?;
            let chapters = plan
                .chapters
                .iter()
                .map(|c| Chapter {
                    at: c.at_seconds,
                    name: c.name.clone(),
                })
                .collect::<Vec<_>>();
            let encode = trim::encode_command(
                &source.path,
                stage.partial_path(),
                &self.config.work.join("unused-chapters.ffmetadata"),
                trim::EncodePlan {
                    keep: &plan.keep,
                    audio_streams: info.audio_streams,
                    chapters: &chapters,
                    duration: plan.trimmed_seconds,
                },
                encoders,
            )?;
            let times = trim::seam_sample_times(&plan.seams, plan.trimmed_seconds);
            let preview = if args.contact_sheet && !times.is_empty() {
                Some(SheetRequest {
                    path: output.clone(),
                    times,
                    columns: 4,
                    width: 320,
                    max_image_bytes: self.config.max_image_bytes,
                })
            } else {
                None
            };
            let guarded = GuardedBackend {
                config: self.config.clone(),
                inner: self.backend.take().unwrap(),
                authority: self.config.authority.clone(),
                provider: self.provider.clone(),
                source: source.clone(),
            };
            self.export = Some(ExportOwner::new(
                guarded,
                stage,
                encode,
                &info,
                trim::ExpectedMetadata {
                    duration: plan.trimmed_seconds,
                    width: info.width,
                    height: info.height,
                    audio_streams: info.audio_streams,
                    chapters,
                },
                preview,
                self.cancellation.clone(),
            )?);
        }
        let exported = self.export.as_mut().unwrap().finish().await?;
        let mut text = report["content"][0]["text"].as_str().unwrap().to_owned();
        text.push_str(&format!(
            "\nExported {}: {}s, {} chapter(s)",
            output.display(),
            number(trim::round_seconds(exported.verified.duration)),
            exported.verified.chapters.len()
        ));
        let mut content = Vec::new();
        if let Some(image) = &exported.preview {
            text.push_str(&format!("\nContact sheet: frames around each seam (clamped near the end), in pairs, left to right: {}",trim::seam_sample_times(&plan.seams,plan.trimmed_seconds).iter().map(|t|format!("{}s",number(trim::round_seconds(*t)))).collect::<Vec<_>>().join(", ")));
            content.push(image_content(image));
        }
        let warnings = [exported.preview_warning, exported.cleanup_warning]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        for warning in &warnings {
            text.push('\n');
            text.push_str(warning);
        }
        content.insert(0, json!({"type":"text","text":text}));
        report["content"] = Value::Array(content);
        let result = &exported.verified;
        let structured = report["structuredContent"].as_object_mut().unwrap();
        structured.extend(json!({"committed":true,"outputPath":output,"sha256":exported.file.sha256,"outputDurationSeconds":trim::round_seconds(result.duration),"outputChapters":result.chapters.len(),"verification":{"metadata":"pass","strictDecode":"pass","scope":"Cut-plan duration (0.25s tolerance), dimensions, first audio track, chapters and complete decode; editorial content requires review"},"format":{"codec":result.codec,"pixelFormat":result.pixel_format,"colorTransfer":result.color_transfer.as_deref().unwrap_or("unknown"),"colorPrimaries":result.color_primaries.as_deref().unwrap_or("unknown"),"colorSpace":result.color_space.as_deref().unwrap_or("unknown"),"colorFidelity":"unverified"},"previewStatus":if exported.preview.is_some(){"available"}else if warnings.iter().any(|w|w.contains("contact sheet unavailable")){"unavailable"}else{"not-requested"},"warnings":warnings}).as_object().unwrap().clone());
        Ok(report)
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        if self.result.is_none() {
            self.cancel();
        }
    }
}

struct GuardedBackend {
    config: Config,
    inner: SystemMedia,
    authority: Arc<Authority>,
    provider: Arc<dyn RecordingDirectory>,
    source: Recording,
}
impl Encode for GuardedBackend {
    fn encode(
        &mut self,
        r: &EncodeRequest,
        c: Cancellation,
    ) -> Result<Box<dyn OwnedJob<()>>, TrimError> {
        self.config.validate()?;
        self.source.check(&self.authority)?;
        self.inner.encode(r, c)
    }
}
impl Probe for GuardedBackend {
    fn probe(
        &mut self,
        p: &Path,
        c: Cancellation,
    ) -> Result<Box<dyn OwnedJob<MediaProbe>>, TrimError> {
        self.config.validate()?;
        self.inner.probe(p, c)
    }
}
impl Preview for GuardedBackend {
    fn preview(
        &mut self,
        r: &SheetRequest,
        c: Cancellation,
    ) -> Result<Box<dyn OwnedJob<Arc<[u8]>>>, TrimError> {
        self.config.validate()?;
        self.inner.preview(r, c)
    }
}
impl Decode for GuardedBackend {
    fn decode(&mut self, p: &Path, c: Cancellation) -> Result<Box<dyn OwnedJob<()>>, TrimError> {
        self.config.validate()?;
        Ok(Box::new(PublicationDecode {
            config: self.config.clone(),
            inner: self.inner.decode(p, c.clone())?,
            authority: self.authority.clone(),
            provider: self.provider.clone(),
            source: self.source.clone(),
            cancellation: c,
            decoded: false,
            future: None,
            result: None,
        }))
    }
}
struct PublicationDecode {
    config: Config,
    inner: Box<dyn OwnedJob<()>>,
    authority: Arc<Authority>,
    provider: Arc<dyn RecordingDirectory>,
    source: Recording,
    cancellation: Cancellation,
    decoded: bool,
    future: Option<DirectoryFuture>,
    result: Option<Result<(), TrimError>>,
}
impl OwnedJob<()> for PublicationDecode {
    fn cancel(&self) {
        self.cancellation.cancel();
        self.inner.cancel();
    }
    fn finish(&mut self) -> JobFuture<'_, ()> {
        Box::pin(async {
            if let Some(result) = &self.result {
                return result.clone();
            }
            if !self.decoded {
                self.inner.finish().await?;
                self.decoded = true;
            }
            if self.future.is_none() {
                self.future = Some(self.provider.current_directory());
            }
            let result = match self.future.as_mut().unwrap().await {
                Ok(root) => self
                    .config
                    .validate()
                    .and_then(|()| self.authority.current(&root))
                    .and_then(|()| self.source.check(&self.authority))
                    .and_then(|()| cancelled(&self.cancellation)),
                Err(_) => Err(err(
                    ErrorKind::Evidence,
                    "Recording directory evidence is unavailable",
                )),
            };
            self.future.take();
            self.result = Some(result.clone());
            result
        })
    }
}
#[derive(Clone)]
struct Recording {
    path: PathBuf,
    identity: Metadata,
}
impl Recording {
    fn check(&self, a: &Authority) -> Result<(), TrimError> {
        let current = a.file(&self.path)?;
        if unchanged(&current, &self.identity) {
            Ok(())
        } else {
            Err(err(
                ErrorKind::StageChanged,
                "Recording identity or content changed during inspection",
            ))
        }
    }
}
struct Authority {
    root: PathBuf,
    identity: Metadata,
}
impl Authority {
    fn new(root: &Path) -> Result<Self, TrimError> {
        if !cfg!(unix)
            || !root.is_absolute()
            || root
                .components()
                .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
            || root.to_str().is_none()
        {
            return Err(err(
                ErrorKind::UnsafePath,
                "Approved recording root must be absolute, canonical and UTF-8",
            ));
        }
        let identity = fs::symlink_metadata(root).map_err(|_| {
            err(
                ErrorKind::UnsafePath,
                "Approved recording root is unavailable",
            )
        })?;
        if root.canonicalize().ok().as_deref() != Some(root)
            || !identity.is_dir()
            || identity.file_type().is_symlink()
        {
            return Err(err(
                ErrorKind::UnsafePath,
                "Approved recording root must be an existing non-symlink directory",
            ));
        }
        Ok(Self {
            root: root.to_owned(),
            identity,
        })
    }
    fn check(&self) -> Result<(), TrimError> {
        let current = fs::symlink_metadata(&self.root).map_err(|_| {
            err(
                ErrorKind::UnsafePath,
                "Approved recording root is unavailable",
            )
        })?;
        if current.is_dir() && !current.file_type().is_symlink() && same(&current, &self.identity) {
            Ok(())
        } else {
            Err(err(
                ErrorKind::UnsafePath,
                "Approved recording root identity changed",
            ))
        }
    }
    fn current(&self, current: &Path) -> Result<(), TrimError> {
        self.check()?;
        if current.as_os_str().is_empty() {
            return Err(err(
                ErrorKind::Evidence,
                "OBS has no recording directory set",
            ));
        }
        if current.canonicalize().ok().as_deref() != Some(self.root.as_path()) {
            return Err(err(
                ErrorKind::UnsafePath,
                "OBS recording directory differs from the approved recording root",
            ));
        }
        Ok(())
    }
    fn path(&self, path: &Path) -> Result<PathBuf, TrimError> {
        self.check()?;
        let absolute = if path.is_absolute() {
            path.to_owned()
        } else {
            self.root.join(path)
        };
        if absolute
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
            || !absolute.starts_with(&self.root)
        {
            return Err(err(
                ErrorKind::UnsafePath,
                &format!(
                    "{} is outside the OBS recording directory {}",
                    path.display(),
                    self.root.display()
                ),
            ));
        }
        Ok(absolute)
    }
    fn directory(&self, path: &Path) -> Result<(), TrimError> {
        self.check()?;
        if !path.starts_with(&self.root)
            || path.canonicalize().ok().as_deref() != Some(path)
            || !fs::symlink_metadata(path).is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink())
        {
            return Err(err(
                ErrorKind::UnsafePath,
                "Recording directory identity is unavailable",
            ));
        }
        Ok(())
    }
    fn file(&self, path: &Path) -> Result<Metadata, TrimError> {
        self.directory(
            path.parent()
                .ok_or_else(|| err(ErrorKind::UnsafePath, "Recording parent is unavailable"))?,
        )?;
        let metadata = fs::symlink_metadata(path)
            .map_err(|_| err(ErrorKind::UnsafePath, "Recording file is unavailable"))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || !independent(&metadata) {
            return Err(err(
                ErrorKind::UnsafePath,
                "Recording must be an independent regular file",
            ));
        }
        Ok(metadata)
    }
    fn recording(&self, path: &str) -> Result<Recording, TrimError> {
        let path = self.path(Path::new(path))?;
        let identity = self.file(&path)?;
        Ok(Recording { path, identity })
    }
    fn output(&self, path: &Path) -> Result<PathBuf, TrimError> {
        let path = self.path(path)?;
        self.directory(
            path.parent()
                .ok_or_else(|| err(ErrorKind::UnsafePath, "Recording parent is unavailable"))?,
        )?;
        if path.to_str().is_none() {
            return Err(err(ErrorKind::UnsafePath, "Recording path is not UTF-8"));
        }
        Ok(path)
    }
    fn sidecar(&self, recording: &Path, limit: usize) -> Result<Vec<Chapter>, TrimError> {
        let name = recording
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| err(ErrorKind::UnsafePath, "Recording path is not UTF-8"))?;
        let stem = name.rfind('.').map(|dot| &name[..dot]).unwrap_or(name);
        let path = recording.with_file_name(format!("{stem}.take.json"));
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(_) => return Ok(vec![]),
            Ok(_) => {}
        }
        let before = self.file(&path)?;
        if before.len() > limit as u64 {
            return Err(err(
                ErrorKind::Limit,
                "Take-log chapter sidecar exceeds its configured byte bound",
            ));
        }
        let file = File::open(&path).map_err(|_| {
            err(
                ErrorKind::Storage,
                "Take-log chapter sidecar is unavailable",
            )
        })?;
        if !same(
            &file.metadata().map_err(|_| {
                err(
                    ErrorKind::Storage,
                    "Take-log chapter sidecar is unavailable",
                )
            })?,
            &before,
        ) {
            return Err(err(
                ErrorKind::StageChanged,
                "Take-log chapter sidecar changed during inspection",
            ));
        }
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| {
                err(
                    ErrorKind::Storage,
                    "Take-log chapter sidecar is unavailable",
                )
            })?;
        if bytes.len() > limit || !unchanged(&self.file(&path)?, &before) {
            return Err(err(
                ErrorKind::StageChanged,
                "Take-log chapter sidecar changed during inspection",
            ));
        }
        Ok(serde_json::from_slice::<Value>(&bytes)
            .ok()
            .map(|v| trim::take_log_chapters(&v))
            .unwrap_or_default())
    }
}
fn cancelled(c: &Cancellation) -> Result<(), TrimError> {
    if c.is_cancelled() {
        Err(err(ErrorKind::Cancelled, "Media verification cancelled"))
    } else {
        Ok(())
    }
}
fn sample_error(e: media_verification::MediaError) -> TrimError {
    let mut error = err(
        if e.kind() == media_verification::ErrorKind::Cancelled {
            ErrorKind::Cancelled
        } else {
            ErrorKind::Decode
        },
        if e.kind() == media_verification::ErrorKind::TimedOut {
            "Finding still stretches took longer than 5 minutes; try a shorter recording"
        } else {
            e.primary_message()
        },
    );
    error.cleanup_issue = e.cleanup_issue().map(str::to_owned);
    error
}
fn err(kind: ErrorKind, text: &str) -> TrimError {
    TrimError::new(kind, text)
}
fn error_result(text: &str) -> Value {
    json!({"content":[{"type":"text","text":text}],"isError":true})
}
fn image_content(bytes: &[u8]) -> Value {
    json!({"type":"image","data":STANDARD.encode(bytes),"mimeType":"image/jpeg"})
}
// JSON.stringify exposes integral JS numbers without .0 and serializes -0 as
// 0. Preserve that public wire representation; no report fields are removed.
fn source_json_numbers(value: &mut Value) {
    match value {
        Value::Number(number) => {
            if let Some(n) = number.as_f64()
                && n.is_finite()
                && n.fract() == 0.0
                && n.abs() <= 9_007_199_254_740_991.0
            {
                *number = serde_json::Number::from(n as i64);
            }
        }
        Value::Array(values) => {
            for value in values {
                source_json_numbers(value);
            }
        }
        Value::Object(object) => {
            for value in object.values_mut() {
                source_json_numbers(value);
            }
        }
        _ => {}
    }
}
fn number(n: f64) -> String {
    crate::observation::js_string(Some(&json!(n)))
}
#[cfg(unix)]
fn same(a: &Metadata, b: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}
#[cfg(not(unix))]
fn same(_a: &Metadata, _b: &Metadata) -> bool {
    false
}
#[cfg(unix)]
fn independent(m: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    m.nlink() == 1
}
#[cfg(not(unix))]
fn independent(_m: &Metadata) -> bool {
    false
}
fn unchanged(a: &Metadata, b: &Metadata) -> bool {
    same(a, b) && a.len() == b.len() && a.modified().ok() == b.modified().ok()
}
fn executable_file(metadata: &Metadata) -> Result<(), TrimError> {
    if !metadata.is_file() || metadata.file_type().is_symlink() || !independent(metadata) {
        return Err(err(
            ErrorKind::UnsafePath,
            "Configured media executable must be an independent regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o111 == 0 {
            return Err(err(
                ErrorKind::Unsupported,
                "Configured media executable is not executable",
            ));
        }
    }
    Ok(())
}
