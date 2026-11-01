// SPDX-License-Identifier: GPL-2.0-only
//! Explicit executable/work-directory adapters for checked trim commands.
//! Keep each job (or ExportOwner) outside cancellable frontend awaits. A job's
//! metadata remains private until its child wait/pipe cleanup is confirmed.
//! Dropping an unfinished owner requests emergency child cleanup but leaves
//! metadata for inspection; it does not claim joined cleanup. No MCP routes,
//! recording-directory authority or sandbox is added.
use crate::{
    media_verification::{
        self, Cancellation, LocalMediaInput, MediaError, MediaProcess, ProcessOptions, StdoutSink,
        local_media_input_options,
    },
    trim::{
        self, Decode, Encode, EncodeRequest, ErrorKind, JobFuture, MediaProbe, OwnedJob, Preview,
        Probe, SheetRequest, TrimError,
    },
};
use std::{
    ffi::OsString,
    fmt,
    fs::{self, File, Metadata, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
const DECODE_TIMEOUT: Duration = Duration::from_secs(60);
const SHEET_IMAGE_LIMIT: usize = 32 * 1024 * 1024;
const SHEET_WORK_LIMIT: usize = 64 * 1024 * 1024;
const SHEET_PIXEL_LIMIT: usize = 16 * 1024 * 1024;
#[derive(Clone, Debug)]
pub struct Bounds {
    pub encode_timeout: Duration,
    pub probe_timeout: Duration,
    pub decode_timeout: Duration,
    pub encoder_query_timeout: Duration,
    pub probe_stdout_bytes: usize,
    pub encode_pipe_bytes: usize,
    pub sheet_command_timeout: Duration,
}
impl Default for Bounds {
    fn default() -> Self {
        Self {
            encode_timeout: trim::ENCODE_TIMEOUT,
            probe_timeout: trim::PROBE_TIMEOUT,
            decode_timeout: DECODE_TIMEOUT,
            encoder_query_timeout: trim::ENCODER_QUERY_TIMEOUT,
            probe_stdout_bytes: trim::PROBE_STDOUT_LIMIT,
            encode_pipe_bytes: trim::ENCODE_OUTPUT_LIMIT,
            sheet_command_timeout: trim::SHEET_COMMAND_TIMEOUT,
        }
    }
}
impl Bounds {
    fn validate(&self) -> Result<(), TrimError> {
        for (actual, maximum) in [
            (self.encode_timeout, trim::ENCODE_TIMEOUT),
            (self.probe_timeout, trim::PROBE_TIMEOUT),
            (self.decode_timeout, DECODE_TIMEOUT),
            (self.encoder_query_timeout, trim::ENCODER_QUERY_TIMEOUT),
            (self.sheet_command_timeout, trim::SHEET_COMMAND_TIMEOUT),
        ] {
            if actual.is_zero() || actual > maximum {
                return Err(error(
                    ErrorKind::Limit,
                    "Media phase timeout is outside its supported bound",
                ));
            }
        }
        if self.probe_stdout_bytes == 0
            || self.probe_stdout_bytes > trim::PROBE_STDOUT_LIMIT
            || self.encode_pipe_bytes == 0
            || self.encode_pipe_bytes > trim::ENCODE_OUTPUT_LIMIT
        {
            return Err(error(
                ErrorKind::Limit,
                "Media pipe capacity is outside its supported bound",
            ));
        }
        Ok(())
    }
}
#[derive(Clone)]
struct Executable {
    path: PathBuf,
    identity: Metadata,
}
impl Executable {
    fn new(path: &Path) -> Result<Self, TrimError> {
        if !path.is_absolute() {
            return Err(error(
                ErrorKind::UnsafePath,
                "Media executable must be an explicit absolute path",
            ));
        }
        let path = path.canonicalize().map_err(|_| {
            error(
                ErrorKind::Unsupported,
                "Configured media executable is unavailable",
            )
        })?;
        let identity = fs::symlink_metadata(&path).map_err(|_| {
            error(
                ErrorKind::Unsupported,
                "Configured media executable is unavailable",
            )
        })?;
        if !regular(&identity) {
            return Err(error(
                ErrorKind::UnsafePath,
                "Media executable is not a regular file",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if identity.mode() & 0o111 == 0 {
                return Err(error(
                    ErrorKind::Unsupported,
                    "Configured media executable is not executable",
                ));
            }
        }
        Ok(Self { path, identity })
    }
    fn checked(&self) -> Result<&Path, TrimError> {
        let current = fs::symlink_metadata(&self.path).map_err(|_| {
            error(
                ErrorKind::Unsupported,
                "Configured media executable is unavailable",
            )
        })?;
        if !same_version(&self.identity, &current) || !regular(&current) {
            return Err(error(
                ErrorKind::UnsafePath,
                "Configured media executable identity changed",
            ));
        }
        Ok(&self.path)
    }
}
pub struct SystemMedia {
    ffmpeg: Executable,
    ffprobe: Executable,
    work: PathBuf,
    work_identity: Metadata,
    bounds: Bounds,
}
impl fmt::Debug for SystemMedia {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("SystemMedia")
            .field("bounds", &self.bounds)
            .finish_non_exhaustive()
    }
}
impl SystemMedia {
    pub fn new(
        ffmpeg: &Path,
        ffprobe: &Path,
        private_work: &Path,
        bounds: Bounds,
    ) -> Result<Self, TrimError> {
        bounds.validate()?;
        let identity = private_directory(private_work)?;
        Ok(Self {
            ffmpeg: Executable::new(ffmpeg)?,
            ffprobe: Executable::new(ffprobe)?,
            work: private_work.to_owned(),
            work_identity: identity,
            bounds,
        })
    }
    fn check_work(&self) -> Result<(), TrimError> {
        let current = private_directory(&self.work)?;
        if !same_file(&current, &self.work_identity) {
            return Err(error(
                ErrorKind::UnsafePath,
                "Media work directory identity changed",
            ));
        }
        Ok(())
    }
    /// Query only the configured ffmpeg; no PATH fallback or installation.
    pub fn encoders(
        &self,
        cancellation: Cancellation,
    ) -> Result<Box<dyn OwnedJob<Arc<[u8]>>>, TrimError> {
        let process = media_verification::capture_bounded_media_process(
            self.ffmpeg.checked()?,
            &["-hide_banner".into(), "-encoders".into()],
            ProcessOptions {
                timeout: self.bounds.encoder_query_timeout,
                cancellation,
            },
            self.bounds.probe_stdout_bytes,
        )
        .map_err(|e| process_error(ErrorKind::Encode, e))?;
        Ok(Box::new(BytesJob {
            process,
            result: None,
            kind: ErrorKind::Encode,
        }))
    }
}
fn error(kind: ErrorKind, message: &str) -> TrimError {
    TrimError::new(kind, message)
}
fn process_error(phase: ErrorKind, error: MediaError) -> TrimError {
    let mut converted = TrimError::new(
        if error.kind() == media_verification::ErrorKind::Cancelled {
            ErrorKind::Cancelled
        } else {
            phase
        },
        error.primary_message(),
    );
    converted.cleanup_issue = error.cleanup_issue().map(str::to_owned);
    converted
}
struct EncodeSink {
    limit: usize,
    stdout: usize,
    stderr: usize,
}
impl StdoutSink for EncodeSink {
    type Output = ();
    fn consume(&mut self, chunk: &[u8]) -> Result<(), String> {
        count(&mut self.stdout, chunk.len(), self.limit)
    }
    fn consume_stderr(&mut self, chunk: &[u8]) -> Result<(), String> {
        count(&mut self.stderr, chunk.len(), self.limit)
    }
    fn finish(self) -> Result<(), String> {
        Ok(())
    }
}
fn count(count: &mut usize, added: usize, limit: usize) -> Result<(), String> {
    if added > limit.saturating_sub(*count) {
        return Err("Encoder pipe exceeded its configured byte bound".into());
    }
    *count += added;
    Ok(())
}
impl Encode for SystemMedia {
    fn encode(
        &mut self,
        request: &EncodeRequest,
        cancellation: Cancellation,
    ) -> Result<Box<dyn OwnedJob<()>>, TrimError> {
        if cancellation.is_cancelled() {
            return Err(error(ErrorKind::Cancelled, "Media verification cancelled"));
        }
        media_file(&request.input)?;
        self.check_work()?;
        let executable = self.ffmpeg.checked()?;
        let output = fs::symlink_metadata(&request.output).map_err(|_| {
            error(
                ErrorKind::UnsafePath,
                "Encode output must be an already reserved private stage",
            )
        })?;
        private_file(&output)?;
        let mut metadata =
            OwnedMetadata::create(&self.work, &self.work_identity, request.metadata.as_bytes())?;
        let args = qualified_encode_args(request, &metadata.path)?;
        self.check_work()?;
        metadata.check()?;
        let process = media_verification::run_streamed_media_process(
            executable,
            &args,
            ProcessOptions {
                timeout: self.bounds.encode_timeout,
                cancellation,
            },
            EncodeSink {
                limit: self.bounds.encode_pipe_bytes,
                stdout: 0,
                stderr: 0,
            },
        )
        .map_err(|e| process_error(ErrorKind::Encode, e))?;
        metadata.in_flight = true;
        Ok(Box::new(EncodeJob {
            process,
            metadata: Some(metadata),
            result: None,
        }))
    }
}
impl Probe for SystemMedia {
    fn probe(
        &mut self,
        path: &Path,
        cancellation: Cancellation,
    ) -> Result<Box<dyn OwnedJob<MediaProbe>>, TrimError> {
        media_file(path)?;
        let process = media_verification::capture_bounded_media_process(
            self.ffprobe.checked()?,
            &qualified_probe_args(path),
            ProcessOptions {
                timeout: self.bounds.probe_timeout,
                cancellation,
            },
            self.bounds.probe_stdout_bytes,
        )
        .map_err(|e| process_error(ErrorKind::Probe, e))?;
        Ok(Box::new(ProbeJob {
            process,
            result: None,
        }))
    }
}
/// Compose the sealed cut/filter plan with the closed local input policy.
/// The metadata path belongs to the caller's private, uniquely owned work file.
pub fn qualified_encode_args(
    request: &EncodeRequest,
    metadata_path: &Path,
) -> Result<Vec<OsString>, TrimError> {
    let mut args = request.args().to_vec();
    let metadata_inputs = args
        .windows(4)
        .enumerate()
        .filter(|(_, v)| v[0] == "-f" && v[1] == "ffmetadata" && v[2] == "-i")
        .map(|(i, _)| i + 3)
        .collect::<Vec<_>>();
    if metadata_inputs.len() != 1
        || args.get(metadata_inputs[0]) != Some(&request.metadata_path.as_os_str().to_owned())
        || args.get(4) != Some(&request.input.as_os_str().to_owned())
        || args.last() != Some(&request.output.as_os_str().to_owned())
    {
        return Err(error(
            ErrorKind::Evidence,
            "Encode command fields do not match the checked plan",
        ));
    }
    args[metadata_inputs[0]] = metadata_path.as_os_str().to_owned();
    let mut qualified = Vec::with_capacity(args.len() + 8);
    let mut inputs = 0;
    for arg in args {
        if arg == "-i" {
            qualified.extend(local_media_input_options(match inputs {
                0 => LocalMediaInput::Recording,
                1 => LocalMediaInput::Metadata,
                _ => {
                    return Err(error(
                        ErrorKind::Evidence,
                        "Encode plan has an unexpected input",
                    ));
                }
            }));
            inputs += 1;
        }
        qualified.push(arg);
    }
    if inputs != 2 {
        return Err(error(
            ErrorKind::Evidence,
            "Encode plan lacks its checked inputs",
        ));
    }
    Ok(qualified)
}
pub fn qualified_probe_args(path: &Path) -> Vec<OsString> {
    let mut args = trim::probe_args(path);
    let path = args.pop().expect("probe input");
    args.extend(local_media_input_options(LocalMediaInput::Recording));
    args.push(path);
    args
}
impl Decode for SystemMedia {
    fn decode(
        &mut self,
        path: &Path,
        cancellation: Cancellation,
    ) -> Result<Box<dyn OwnedJob<()>>, TrimError> {
        media_file(path)?;
        let process = media_verification::run_bounded_media_process(
            self.ffmpeg.checked()?,
            &media_verification::strict_decode_args(path),
            ProcessOptions {
                timeout: self.bounds.decode_timeout,
                cancellation,
            },
        )
        .map_err(|e| process_error(ErrorKind::Decode, e))?;
        Ok(Box::new(DecodeJob {
            process,
            result: None,
        }))
    }
}
impl Preview for SystemMedia {
    fn preview(
        &mut self,
        request: &SheetRequest,
        cancellation: Cancellation,
    ) -> Result<Box<dyn OwnedJob<Arc<[u8]>>>, TrimError> {
        if cancellation.is_cancelled() {
            return Err(error(ErrorKind::Cancelled, "Media verification cancelled"));
        }
        media_file(&request.path)?;
        self.check_work()?;
        self.ffmpeg.checked()?;
        if request.max_image_bytes == 0 || request.max_image_bytes > SHEET_IMAGE_LIMIT {
            return Err(error(
                ErrorKind::Limit,
                "Contact-sheet byte capacity is outside its supported bound",
            ));
        }
        // Validate before reserving any work. The source planner owns temporal,
        // scale, tile, JPEG quality and frame ordering semantics.
        trim::contact_sheet_commands(request, &self.work)?;
        let work = SheetWork::create(&self.work, &self.work_identity)?;
        let commands = trim::contact_sheet_commands(request, &work.directory)?;
        Ok(Box::new(SheetJob {
            executable: self.ffmpeg.clone(),
            work,
            commands,
            request: request.clone(),
            phase: 0,
            process: None,
            dimensions: None,
            bytes: 0,
            timeout: self.bounds.sheet_command_timeout,
            cancellation,
            result: None,
        }))
    }
}

/// The source writes JPEGs directly with image2. Capture the same MJPEG output
/// through a bounded pipe before publishing a private create_new frame instead:
/// FFmpeg never owns an overwritable output filename or unbounded disk stream.
pub fn qualified_sheet_args(
    command: &[OsString],
    images: bool,
) -> Result<Vec<OsString>, TrimError> {
    if command.last().is_none() || command.iter().filter(|arg| *arg == "-i").count() != 1 {
        return Err(error(
            ErrorKind::Evidence,
            "Contact-sheet command lacks its checked input",
        ));
    }
    let mut args = Vec::with_capacity(command.len() + 8);
    for arg in &command[..command.len() - 1] {
        if arg == "-i" {
            args.extend(local_media_input_options(if images {
                LocalMediaInput::Images
            } else {
                LocalMediaInput::Recording
            }));
        }
        args.push(arg.clone());
    }
    args.extend(["-f", "image2pipe", "-c:v", "mjpeg", "pipe:1"].map(OsString::from));
    Ok(args)
}

struct SheetJob {
    executable: Executable,
    work: SheetWork,
    commands: Vec<Vec<OsString>>,
    request: SheetRequest,
    phase: usize,
    process: Option<MediaProcess<Arc<[u8]>>>,
    dimensions: Option<(usize, usize)>,
    bytes: usize,
    timeout: Duration,
    cancellation: Cancellation,
    result: Option<Result<Arc<[u8]>, TrimError>>,
}
impl SheetJob {
    async fn run(&mut self) -> Result<Arc<[u8]>, TrimError> {
        loop {
            let tile = self.phase == self.request.times.len();
            if self.process.is_none() {
                if self.cancellation.is_cancelled() {
                    return Err(error(ErrorKind::Cancelled, "Media verification cancelled"));
                }
                self.work.check()?;
                if !tile {
                    media_file(&self.request.path)?;
                }
                let arguments = qualified_sheet_args(&self.commands[self.phase], tile)?;
                let process = media_verification::capture_bounded_media_process(
                    self.executable.checked()?,
                    &arguments,
                    ProcessOptions {
                        timeout: self.timeout,
                        cancellation: self.cancellation.clone(),
                    },
                    self.request.max_image_bytes,
                )
                .map_err(|e| process_error(ErrorKind::Preview, e))?;
                self.process = Some(process);
                self.work.in_flight = true;
            }
            let output = self.process.as_mut().unwrap().finish().await;
            if output
                .as_ref()
                .err()
                .is_some_and(|error| error.cleanup_issue().is_some())
            {
                let mut error = process_error(ErrorKind::Preview, output.unwrap_err());
                let retained =
                    "Private contact-sheet work retained because child cleanup is unconfirmed";
                error.cleanup_issue = Some(match error.cleanup_issue.take() {
                    Some(issue) => format!("{issue}; {retained}"),
                    None => retained.into(),
                });
                return Err(error);
            }
            self.work.in_flight = false;
            self.process.take();
            let output = output.map_err(|e| process_error(ErrorKind::Preview, e))?;
            self.work.check()?;
            let dimensions = jpeg_dimensions(&output)?;
            if tile {
                let (width, height) = self.dimensions.unwrap();
                let columns = self.request.columns.min(self.request.times.len());
                let rows = self.request.times.len().div_ceil(self.request.columns);
                if dimensions
                    != (
                        width * columns + 4 * (columns - 1),
                        height * rows + 4 * (rows - 1),
                    )
                {
                    return Err(error(
                        ErrorKind::Preview,
                        "Contact-sheet dimensions disagree with its checked tile plan",
                    ));
                }
                return Ok(output);
            }
            if dimensions.0 != self.request.width
                || self
                    .dimensions
                    .is_some_and(|expected| expected != dimensions)
            {
                return Err(error(
                    ErrorKind::Preview,
                    "Contact-sheet frames have inconsistent dimensions",
                ));
            }
            self.dimensions = Some(dimensions);
            let columns = self.request.columns.min(self.request.times.len());
            let rows = self.request.times.len().div_ceil(self.request.columns);
            let pixels = (dimensions.0 * columns + 4 * (columns - 1))
                .checked_mul(dimensions.1 * rows + 4 * (rows - 1));
            if pixels.is_none_or(|pixels| pixels > SHEET_PIXEL_LIMIT) {
                return Err(error(
                    ErrorKind::Limit,
                    "Contact-sheet pixel capacity exceeded",
                ));
            }
            self.bytes = self
                .bytes
                .checked_add(output.len())
                .filter(|bytes| *bytes <= SHEET_WORK_LIMIT)
                .ok_or_else(|| {
                    error(
                        ErrorKind::Limit,
                        "Contact-sheet work byte capacity exceeded",
                    )
                })?;
            self.work
                .publish(&format!("{:03}.jpg", self.phase), &output)?;
            self.phase += 1;
        }
    }
}
impl OwnedJob<Arc<[u8]>> for SheetJob {
    fn cancel(&self) {
        self.cancellation.cancel();
    }
    fn finish(&mut self) -> JobFuture<'_, Arc<[u8]>> {
        Box::pin(async {
            if self.result.is_none() {
                let mut result = self.run().await;
                if !self.work.in_flight
                    && let Err(cleanup) = self.work.cleanup()
                {
                    match &mut result {
                        Ok(_) => result = Err(cleanup),
                        Err(primary) => primary.cleanup_issue = Some(cleanup.to_string()),
                    }
                }
                self.result = Some(result);
            }
            self.result.clone().unwrap()
        })
    }
}

struct SheetWork {
    root: PathBuf,
    root_identity: Metadata,
    directory: PathBuf,
    identity: Metadata,
    files: Vec<(PathBuf, File, Metadata)>,
    in_flight: bool,
    removed: bool,
}
impl SheetWork {
    fn create(root: &Path, root_identity: &Metadata) -> Result<Self, TrimError> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| error(ErrorKind::Limit, "Contact-sheet work identity exhausted"))?;
        let directory = root.join(format!("sheet-{}-{id}", std::process::id()));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&directory).map_err(|_| {
            error(
                ErrorKind::Storage,
                "Could not reserve private contact-sheet work",
            )
        })?;
        let identity = private_directory(&directory)?;
        Ok(Self {
            root: root.to_owned(),
            root_identity: root_identity.clone(),
            directory,
            identity,
            files: vec![],
            in_flight: false,
            removed: false,
        })
    }
    fn check(&self) -> Result<(), TrimError> {
        if !same_file(&private_directory(&self.root)?, &self.root_identity)
            || !same_file(&private_directory(&self.directory)?, &self.identity)
        {
            return Err(error(
                ErrorKind::UnsafePath,
                "Private contact-sheet directory identity changed",
            ));
        }
        for (path, file, identity) in &self.files {
            let named = fs::symlink_metadata(path).map_err(|_| {
                error(
                    ErrorKind::UnsafePath,
                    "Private contact-sheet frame is missing or replaced",
                )
            })?;
            private_file(&named)?;
            let opened = file.metadata().map_err(|_| {
                error(
                    ErrorKind::UnsafePath,
                    "Private contact-sheet frame is unavailable",
                )
            })?;
            if !same_version(&named, identity) || !same_version(&opened, identity) {
                return Err(error(
                    ErrorKind::UnsafePath,
                    "Private contact-sheet frame identity changed",
                ));
            }
        }
        let mut entries = fs::read_dir(&self.directory).map_err(|_| {
            error(
                ErrorKind::Storage,
                "Private contact-sheet inventory is unavailable",
            )
        })?;
        let mut count = 0;
        for entry in &mut entries {
            let entry = entry.map_err(|_| {
                error(
                    ErrorKind::Storage,
                    "Private contact-sheet inventory is unavailable",
                )
            })?;
            count += 1;
            if count > trim::MAX_SHEET_FRAMES
                || !self.files.iter().any(|(path, _, _)| *path == entry.path())
            {
                return Err(error(
                    ErrorKind::UnsafePath,
                    "Private contact-sheet inventory has an unowned entry",
                ));
            }
        }
        Ok(())
    }
    fn publish(&mut self, name: &str, bytes: &[u8]) -> Result<(), TrimError> {
        self.check()?;
        let path = self.directory.join(name);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path).map_err(|_| {
            error(
                ErrorKind::Storage,
                "Could not reserve private contact-sheet frame",
            )
        })?;
        let identity = file.metadata().map_err(|_| {
            error(
                ErrorKind::Storage,
                "Private contact-sheet frame identity is unavailable",
            )
        })?;
        self.files.push((
            path,
            file.try_clone().map_err(|_| {
                error(
                    ErrorKind::Storage,
                    "Private contact-sheet descriptor is unavailable",
                )
            })?,
            identity,
        ));
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| {
                error(
                    ErrorKind::Storage,
                    "Could not write private contact-sheet frame",
                )
            })?;
        self.files.last_mut().unwrap().2 = file.metadata().map_err(|_| {
            error(
                ErrorKind::Storage,
                "Private contact-sheet frame identity is unavailable",
            )
        })?;
        self.check()
    }
    fn cleanup(&mut self) -> Result<(), TrimError> {
        if self.removed {
            return Ok(());
        }
        let refused = || {
            let mut error = error(
                ErrorKind::Storage,
                "Contact-sheet cleanup refused unknown ownership",
            );
            error.cleanup_issue = Some("Private contact-sheet work retained for inspection".into());
            error
        };
        self.check().map_err(|_| refused())?;
        for (path, _, _) in &self.files {
            fs::remove_file(path).map_err(|_| refused())?;
        }
        fs::remove_dir(&self.directory).map_err(|_| refused())?;
        self.removed = true;
        Ok(())
    }
}
impl Drop for SheetWork {
    fn drop(&mut self) {
        if !self.in_flight && !self.removed {
            let _ = self.cleanup();
        }
    }
}
fn jpeg_dimensions(bytes: &[u8]) -> Result<(usize, usize), TrimError> {
    let invalid = || {
        error(
            ErrorKind::Preview,
            "FFmpeg returned an invalid contact-sheet JPEG",
        )
    };
    if !bytes.starts_with(&[0xff, 0xd8]) || !bytes.ends_with(&[0xff, 0xd9]) {
        return Err(invalid());
    }
    let mut at = 2;
    while at + 4 <= bytes.len() {
        if bytes[at] != 0xff {
            return Err(invalid());
        }
        while at < bytes.len() && bytes[at] == 0xff {
            at += 1;
        }
        let marker = *bytes.get(at).ok_or_else(invalid)?;
        at += 1;
        if marker == 0xda || marker == 0xd9 {
            break;
        }
        let len = bytes.get(at..at + 2).ok_or_else(invalid)?;
        let len = u16::from_be_bytes([len[0], len[1]]) as usize;
        if len < 2 || at + len > bytes.len() {
            return Err(invalid());
        }
        if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) {
            let frame = bytes.get(at + 2..at + 7).ok_or_else(invalid)?;
            let height = u16::from_be_bytes([frame[1], frame[2]]) as usize;
            let width = u16::from_be_bytes([frame[3], frame[4]]) as usize;
            if width == 0 || height == 0 || width * height > SHEET_PIXEL_LIMIT {
                return Err(invalid());
            }
            return Ok((width, height));
        }
        at += len;
    }
    Err(invalid())
}
struct BytesJob {
    process: MediaProcess<Arc<[u8]>>,
    result: Option<Result<Arc<[u8]>, TrimError>>,
    kind: ErrorKind,
}
impl OwnedJob<Arc<[u8]>> for BytesJob {
    fn cancel(&self) {
        self.process.cancel()
    }
    fn finish(&mut self) -> JobFuture<'_, Arc<[u8]>> {
        Box::pin(async {
            if self.result.is_none() {
                self.result = Some(
                    self.process
                        .finish()
                        .await
                        .map_err(|e| process_error(self.kind, e)),
                );
            }
            self.result.clone().unwrap()
        })
    }
}
struct ProbeJob {
    process: MediaProcess<Arc<[u8]>>,
    result: Option<Result<MediaProbe, TrimError>>,
}
impl OwnedJob<MediaProbe> for ProbeJob {
    fn cancel(&self) {
        self.process.cancel()
    }
    fn finish(&mut self) -> JobFuture<'_, MediaProbe> {
        Box::pin(async {
            if self.result.is_none() {
                let result = self
                    .process
                    .finish()
                    .await
                    .map_err(|e| process_error(ErrorKind::Probe, e))
                    .and_then(|bytes| {
                        serde_json::from_slice::<serde_json::Value>(&bytes)
                            .map_err(|_| error(ErrorKind::Probe, "ffprobe returned malformed JSON"))
                            .map(|json| trim::parse_probe(&json))
                    });
                self.result = Some(result);
            }
            self.result.clone().unwrap()
        })
    }
}
struct DecodeJob {
    process: MediaProcess,
    result: Option<Result<(), TrimError>>,
}
impl OwnedJob<()> for DecodeJob {
    fn cancel(&self) {
        self.process.cancel()
    }
    fn finish(&mut self) -> JobFuture<'_, ()> {
        Box::pin(async {
            if self.result.is_none() {
                self.result = Some(
                    self.process
                        .finish()
                        .await
                        .map_err(|e| process_error(ErrorKind::Decode, e)),
                );
            }
            self.result.clone().unwrap()
        })
    }
}
struct EncodeJob {
    process: MediaProcess,
    metadata: Option<OwnedMetadata>,
    result: Option<Result<(), TrimError>>,
}
impl OwnedJob<()> for EncodeJob {
    fn cancel(&self) {
        self.process.cancel()
    }
    fn finish(&mut self) -> JobFuture<'_, ()> {
        Box::pin(async {
            if self.result.is_none() {
                let result = self.process.finish().await;
                let joined = result
                    .as_ref()
                    .err()
                    .is_none_or(|error| error.cleanup_issue().is_none());
                let mut result = result.map_err(|e| process_error(ErrorKind::Encode, e));
                if joined {
                    let metadata = self.metadata.as_mut().unwrap();
                    metadata.in_flight = false;
                    if let Err(cleanup) = metadata.cleanup() {
                        match &mut result {
                            Ok(()) => result = Err(cleanup),
                            Err(primary) => {
                                primary.cleanup_issue = Some(match primary.cleanup_issue.take() {
                                    Some(issue) => format!("{issue}; {cleanup}"),
                                    None => cleanup.to_string(),
                                })
                            }
                        }
                    }
                } else {
                    let issue =
                        "Private encoder metadata retained because child cleanup is unconfirmed";
                    if let Err(error) = &mut result {
                        error.cleanup_issue = Some(match error.cleanup_issue.take() {
                            Some(prior) => format!("{prior}; {issue}"),
                            None => issue.into(),
                        });
                    }
                }
                self.result = Some(result);
            }
            self.result.clone().unwrap()
        })
    }
}
struct OwnedMetadata {
    root: PathBuf,
    root_identity: Metadata,
    path: PathBuf,
    identity: Metadata,
    _file: File,
    in_flight: bool,
    removed: bool,
}
impl OwnedMetadata {
    fn create(root: &Path, root_identity: &Metadata, bytes: &[u8]) -> Result<Self, TrimError> {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let id = SEQUENCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| error(ErrorKind::Limit, "Media metadata identity exhausted"))?;
        let path = root.join(format!("chapters-{}-{id}.ffmetadata", std::process::id()));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path).map_err(|_| {
            error(
                ErrorKind::Storage,
                "Could not reserve private encoder metadata",
            )
        })?;
        let identity = file.metadata().map_err(|_| {
            error(
                ErrorKind::Storage,
                "Private encoder metadata identity is unavailable",
            )
        })?;
        let mut owned = Self {
            root: root.to_owned(),
            root_identity: root_identity.clone(),
            path,
            identity,
            _file: file.try_clone().map_err(|_| {
                error(
                    ErrorKind::Storage,
                    "Private encoder metadata descriptor is unavailable",
                )
            })?,
            in_flight: false,
            removed: false,
        };
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| {
                error(
                    ErrorKind::Storage,
                    "Could not write private encoder metadata",
                )
            })?;
        owned.identity = file.metadata().map_err(|_| {
            error(
                ErrorKind::Storage,
                "Private encoder metadata identity is unavailable",
            )
        })?;
        owned.check()?;
        Ok(owned)
    }
    fn check(&self) -> Result<(), TrimError> {
        let root = private_directory(&self.root)?;
        let named = fs::symlink_metadata(&self.path).map_err(|_| {
            error(
                ErrorKind::UnsafePath,
                "Private encoder metadata is missing or replaced",
            )
        })?;
        private_file(&named)?;
        let opened = self._file.metadata().map_err(|_| {
            error(
                ErrorKind::UnsafePath,
                "Private encoder metadata is unavailable",
            )
        })?;
        if !same_file(&root, &self.root_identity)
            || !same_version(&named, &self.identity)
            || !same_version(&named, &opened)
        {
            return Err(error(
                ErrorKind::UnsafePath,
                "Private encoder metadata identity changed",
            ));
        }
        Ok(())
    }
    fn cleanup(&mut self) -> Result<(), TrimError> {
        if self.removed {
            return Ok(());
        }
        self.check().map_err(|_| {
            let mut error = error(
                ErrorKind::Storage,
                "Encoder metadata cleanup refused unknown ownership",
            );
            error.cleanup_issue = Some("Private metadata was retained for inspection".into());
            error
        })?;
        fs::remove_file(&self.path).map_err(|_| {
            let mut error = error(ErrorKind::Storage, "Encoder metadata cleanup failed");
            error.cleanup_issue = Some("Private metadata was retained for inspection".into());
            error
        })?;
        self.removed = true;
        Ok(())
    }
}
impl Drop for OwnedMetadata {
    fn drop(&mut self) {
        if !self.in_flight && !self.removed {
            let _ = self.cleanup();
        }
    }
}
fn private_directory(path: &Path) -> Result<Metadata, TrimError> {
    if !path.is_absolute() {
        return Err(error(
            ErrorKind::UnsafePath,
            "Media work directory must be an explicit absolute path",
        ));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                current.push(component.as_os_str())
            }
            _ => {
                return Err(error(
                    ErrorKind::UnsafePath,
                    "Media work directory contains unsupported components",
                ));
            }
        }
        let metadata = fs::symlink_metadata(&current).map_err(|_| {
            error(
                ErrorKind::UnsafePath,
                "Media work directory must already exist",
            )
        })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(error(
                ErrorKind::UnsafePath,
                "Media work directory cannot traverse symlinks",
            ));
        }
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| error(ErrorKind::UnsafePath, "Media work directory is unavailable"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 {
            return Err(error(
                ErrorKind::UnsafePath,
                "Media work directory must be private",
            ));
        }
    }
    Ok(metadata)
}
fn private_file(metadata: &Metadata) -> Result<(), TrimError> {
    if !regular(metadata) {
        return Err(error(
            ErrorKind::UnsafePath,
            "Media metadata or stage must be a regular private file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 || metadata.nlink() != 1 {
            return Err(error(
                ErrorKind::UnsafePath,
                "Media metadata or stage must be private and unshared",
            ));
        }
    }
    Ok(())
}
fn regular(metadata: &Metadata) -> bool {
    metadata.is_file() && !metadata.file_type().is_symlink()
}
fn media_file(path: &Path) -> Result<(), TrimError> {
    if !path.is_absolute() {
        return Err(error(
            ErrorKind::UnsafePath,
            "Media input must be an explicit absolute local file",
        ));
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| error(ErrorKind::UnsafePath, "Media input is unavailable"))?;
    if !regular(&metadata) {
        return Err(error(
            ErrorKind::UnsafePath,
            "Media input must be a regular local file",
        ));
    }
    Ok(())
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
