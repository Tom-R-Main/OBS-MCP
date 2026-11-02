// SPDX-License-Identifier: GPL-2.0-only
//! Bounded direct-child media verification, ported from media-verification.ts.
//!
//! Keep MediaProcess outside any cancellable frontend future. Its owned actor
//! drains owned pipes and initiates absolute-deadline cleanup while finish is not
//! polled. Cancel then await finish to obtain a joined cleanup result. Dropping
//! only finish keeps the same actor and child; it never retries the command.
//! Dropping the owner aborts the actor with kill_on_drop as an emergency path,
//! and is not evidence of joined cleanup.
//!
//! Signals target only the owned direct child. Descendants may escape or keep
//! a pipe open. A bounded post-exit drain reports that uncertainty; this
//! module makes no process-group or arbitrary-descendant containment claim.
use std::{
    collections::VecDeque,
    ffi::{OsStr, OsString},
    fmt, io,
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    process::{Child, ChildStderr, ChildStdout, Command},
    sync::watch,
    task::JoinHandle,
    time::{Instant, sleep_until},
};

const TERM_GRACE: Duration = Duration::from_millis(500);
const STDERR_DRAIN_GRACE: Duration = Duration::from_millis(500);
const EXIT_WAIT_LIMIT: Duration = Duration::from_secs(2);
const STDERR_TAIL_UNITS: usize = 4_000;

#[derive(Clone, Debug)]
pub struct Cancellation {
    sender: watch::Sender<bool>,
}
impl Default for Cancellation {
    fn default() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }
}
impl Cancellation {
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }
}
#[derive(Clone, Debug)]
pub struct ProcessOptions {
    pub timeout: Duration,
    pub cancellation: Cancellation,
}
impl Default for ProcessOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            cancellation: Cancellation::default(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Cancelled,
    TimedOut,
    Spawn,
    Decode,
    Output,
    Cleanup,
    InvalidOptions,
    Actor,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaError {
    kind: ErrorKind,
    message: String,
    cleanup_issue: Option<&'static str>,
}
impl MediaError {
    fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            cleanup_issue: None,
        }
    }
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
    /// The source's primary public error, before any cleanup uncertainty.
    pub fn primary_message(&self) -> &str {
        &self.message
    }
    pub fn cleanup_issue(&self) -> Option<&'static str> {
        self.cleanup_issue
    }
    fn with_cleanup(mut self, issue: &'static str) -> Self {
        self.cleanup_issue = Some(issue);
        self
    }
}
impl fmt::Display for MediaError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(&self.message)?;
        if let Some(issue) = self.cleanup_issue {
            write!(out, "; cleanup unconfirmed: {issue}")?;
        }
        Ok(())
    }
}
impl std::error::Error for MediaError {}

/// Owns exactly one spawn and its actor. finish may be cancelled and resumed.
pub struct MediaProcess<T = ()> {
    task: Option<JoinHandle<Result<T, MediaError>>>,
    cancellation: Cancellation,
    result: Option<Result<T, MediaError>>,
    pid: u32,
}
impl<T: Clone + Send + 'static> MediaProcess<T> {
    pub fn pid(&self) -> u32 {
        self.pid
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
    pub fn is_finished(&self) -> bool {
        self.result.is_some() || self.task.as_ref().is_some_and(JoinHandle::is_finished)
    }
    /// A cancelled await leaves the same JoinHandle owned here. A result is
    /// returned only after the actor exits; successful actor work includes
    /// direct-child wait and every captured pipe's EOF. Unconfirmed pipe cleanup
    /// is an error.
    pub async fn finish(&mut self) -> Result<T, MediaError> {
        if let Some(result) = &self.result {
            return result.clone();
        }
        let joined = self.task.as_mut().expect("pending media actor").await;
        let result = joined.unwrap_or_else(|_| {
            Err(
                MediaError::new(ErrorKind::Actor, "Media verification worker failed")
                    .with_cleanup("direct child cleanup was not joined"),
            )
        });
        self.task.take();
        self.result = Some(result.clone());
        result
    }
}
impl<T> Drop for MediaProcess<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.cancellation.cancel();
            task.abort();
        }
    }
}

/// Trusted bounded synchronous parser. Each callback gets at most 8 KiB;
/// consumers must cap retained output/history and avoid blocking I/O or effects.
/// The actor owns it through child wait and both pipe EOFs, including cancellation.
pub trait StdoutSink: Send + 'static {
    type Output: Clone + Send + 'static;
    fn consume(&mut self, chunk: &[u8]) -> Result<(), String>;
    fn consume_stderr(&mut self, _chunk: &[u8]) -> Result<(), String> {
        Ok(())
    }
    fn finish(self) -> Result<Self::Output, String>;
}
struct IgnoreOutput;
impl StdoutSink for IgnoreOutput {
    type Output = ();
    fn consume(&mut self, _chunk: &[u8]) -> Result<(), String> {
        Ok(())
    }
    fn finish(self) -> Result<(), String> {
        Ok(())
    }
}
struct CaptureBytes {
    bytes: Vec<u8>,
    limit: usize,
}
impl StdoutSink for CaptureBytes {
    type Output = Arc<[u8]>;
    fn consume(&mut self, chunk: &[u8]) -> Result<(), String> {
        if chunk.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err("Media process stdout exceeded its byte limit".into());
        }
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
    fn finish(self) -> Result<Self::Output, String> {
        Ok(self.bytes.into())
    }
}
/// Default strict-verification runner discards stdout, as its source does.
pub fn run_bounded_media_process(
    command: impl AsRef<OsStr>,
    args: &[OsString],
    options: ProcessOptions,
) -> Result<MediaProcess, MediaError> {
    spawn_process(command, args, options, IgnoreOutput, false)
}
/// Capture is explicitly bounded per job and shared across repeated finish
/// calls. This does not bound aggregate process RSS or all active jobs.
pub fn capture_bounded_media_process(
    command: impl AsRef<OsStr>,
    args: &[OsString],
    options: ProcessOptions,
    max_bytes: usize,
) -> Result<MediaProcess<Arc<[u8]>>, MediaError> {
    if max_bytes == 0 || max_bytes > 32 * 1024 * 1024 {
        return Err(MediaError::new(
            ErrorKind::InvalidOptions,
            "Media capture byte limit must be from 1 to 33554432",
        ));
    }
    spawn_process(
        command,
        args,
        options,
        CaptureBytes {
            bytes: Vec::new(),
            limit: max_bytes,
        },
        true,
    )
}
/// Stream arbitrary pipe chunks into a bounded parser without collecting the
/// whole video. Deadline/cancellation continues if frontend finish is unpolled.
pub fn run_streamed_media_process<S: StdoutSink>(
    command: impl AsRef<OsStr>,
    args: &[OsString],
    options: ProcessOptions,
    sink: S,
) -> Result<MediaProcess<S::Output>, MediaError> {
    spawn_process(command, args, options, sink, true)
}
fn spawn_process<S: StdoutSink>(
    command: impl AsRef<OsStr>,
    args: &[OsString],
    options: ProcessOptions,
    sink: S,
    capture: bool,
) -> Result<MediaProcess<S::Output>, MediaError> {
    if options.cancellation.is_cancelled() {
        return Err(MediaError::new(
            ErrorKind::Cancelled,
            "Media verification cancelled",
        ));
    }
    let deadline = Instant::now().checked_add(options.timeout).ok_or_else(|| {
        MediaError::new(
            ErrorKind::InvalidOptions,
            "Media verification timeout is out of range",
        )
    })?;
    let mut child = Command::new(command.as_ref())
        .args(args)
        .stdin(Stdio::null())
        .stdout(if capture {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            let reason = match error.kind() {
                io::ErrorKind::NotFound => "ENOENT".into(),
                io::ErrorKind::PermissionDenied => "EACCES".into(),
                _ => error.to_string(),
            };
            MediaError::new(
                ErrorKind::Spawn,
                format!("spawn {} {reason}", command.as_ref().to_string_lossy()),
            )
        })?;
    let pid = child.id().expect("new direct child has a PID");
    let stderr = child.stderr.take().expect("configured piped stderr");
    let stdout = child.stdout.take();
    let cancellation = options.cancellation;
    let receiver = cancellation.sender.subscribe();
    let task = tokio::spawn(drive(child, stderr, stdout, sink, deadline, receiver));
    Ok(MediaProcess {
        task: Some(task),
        cancellation,
        result: None,
        pid,
    })
}

pub const RECORDING_FORMATS: &str =
    "matroska,webm,mov,mp4,m4a,3gp,3g2,mj2,wav,avi,flv,mpegts,ogg,nut,yuv4mpegpipe";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalMediaInput {
    Recording,
    Metadata,
    Images,
}
/// Per-input policy for standalone local containers; playlists and network
/// protocols are excluded. Metadata/images must be private owned work files.
pub fn local_media_input_options(kind: LocalMediaInput) -> Vec<OsString> {
    let format = match kind {
        LocalMediaInput::Recording => RECORDING_FORMATS,
        LocalMediaInput::Metadata => "ffmetadata",
        LocalMediaInput::Images => "image2",
    };
    [
        "-protocol_whitelist",
        "file,pipe",
        "-format_whitelist",
        format,
    ]
    .map(OsString::from)
    .into()
}

/// Exact strictDecode argument vector, including required first video stream
/// and optional audio. Paths are passed as one OS argument without a shell.
pub fn strict_decode_args(path: impl AsRef<OsStr>) -> Vec<OsString> {
    [
        "-nostdin",
        "-v",
        "error",
        "-xerror",
        "-err_detect",
        "explode",
    ]
    .into_iter()
    .map(OsString::from)
    .chain(local_media_input_options(LocalMediaInput::Recording))
    .chain(std::iter::once(OsString::from("-i")))
    .chain(std::iter::once(path.as_ref().to_os_string()))
    .chain(
        ["-map", "0:v:0", "-map", "0:a?", "-f", "null", "-"]
            .into_iter()
            .map(OsString::from),
    )
    .collect()
}
/// The caller owns this process across its cancellable finish await.
pub fn strict_decode(
    path: impl AsRef<OsStr>,
    options: ProcessOptions,
) -> Result<MediaProcess, MediaError> {
    run_bounded_media_process("ffmpeg", &strict_decode_args(path), options)
}

struct StderrTail {
    units: VecDeque<u16>,
}
impl StderrTail {
    fn new() -> Self {
        Self {
            units: VecDeque::with_capacity(STDERR_TAIL_UNITS),
        }
    }
    fn append(&mut self, chunk: &[u8]) {
        // Like Node chunk.toString(), replace invalid UTF-8 in each read.
        // Retained diagnostic storage is at most 4,000 UTF-16 code units.
        for unit in String::from_utf8_lossy(chunk).encode_utf16() {
            if self.units.len() == STDERR_TAIL_UNITS {
                self.units.pop_front();
            }
            self.units.push_back(unit);
        }
    }
    fn message(&self) -> String {
        // Rust strings cannot represent a lone surrogate at the tail boundary;
        // it is rendered as U+FFFD. This is diagnostic text, not byte evidence.
        String::from_utf16_lossy(&self.units.iter().copied().collect::<Vec<_>>())
            .trim_matches(|c: char| c == '\u{feff}' || (c.is_whitespace() && c != '\u{85}'))
            .to_owned()
    }
}
fn decode_failure(status: ExitStatus, tail: &StderrTail) -> MediaError {
    let closed_by = if let Some(code) = status.code() {
        code.to_string()
    } else {
        signal_name(status)
    };
    MediaError::new(
        ErrorKind::Decode,
        format!("Media decode failed ({closed_by}): {}", tail.message()),
    )
}
#[cfg(unix)]
fn signal_name(status: ExitStatus) -> String {
    use std::os::unix::process::ExitStatusExt;
    if let Some(signal) = status.signal()
        && let Some(name) = platform_signal_name(signal)
    {
        return name.into();
    }
    match status.signal() {
        Some(1) => "SIGHUP".into(),
        Some(2) => "SIGINT".into(),
        Some(3) => "SIGQUIT".into(),
        Some(4) => "SIGILL".into(),
        Some(5) => "SIGTRAP".into(),
        Some(6) => "SIGABRT".into(),
        Some(8) => "SIGFPE".into(),
        Some(9) => "SIGKILL".into(),
        Some(11) => "SIGSEGV".into(),
        Some(13) => "SIGPIPE".into(),
        Some(14) => "SIGALRM".into(),
        Some(15) => "SIGTERM".into(),
        Some(signal) => format!("signal {signal}"),
        None => "unknown".into(),
    }
}
// Numeric values verified from pinned libc0.2.191's Apple/BSD and Linux
// architecture definitions. No libc calls, unsafe code, or new dependency.
#[cfg(target_os = "macos")]
fn platform_signal_name(signal: i32) -> Option<&'static str> {
    let names = [
        "SIGHUP",
        "SIGINT",
        "SIGQUIT",
        "SIGILL",
        "SIGTRAP",
        "SIGABRT",
        "SIGEMT",
        "SIGFPE",
        "SIGKILL",
        "SIGBUS",
        "SIGSEGV",
        "SIGSYS",
        "SIGPIPE",
        "SIGALRM",
        "SIGTERM",
        "SIGURG",
        "SIGSTOP",
        "SIGTSTP",
        "SIGCONT",
        "SIGCHLD",
        "SIGTTIN",
        "SIGTTOU",
        "SIGIO",
        "SIGXCPU",
        "SIGXFSZ",
        "SIGVTALRM",
        "SIGPROF",
        "SIGWINCH",
        "SIGINFO",
        "SIGUSR1",
        "SIGUSR2",
    ];
    usize::try_from(signal)
        .ok()
        .and_then(|index| index.checked_sub(1))
        .and_then(|index| names.get(index).copied())
}
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn platform_signal_name(signal: i32) -> Option<&'static str> {
    let names = [
        "SIGHUP",
        "SIGINT",
        "SIGQUIT",
        "SIGILL",
        "SIGTRAP",
        "SIGABRT",
        "SIGBUS",
        "SIGFPE",
        "SIGKILL",
        "SIGUSR1",
        "SIGSEGV",
        "SIGUSR2",
        "SIGPIPE",
        "SIGALRM",
        "SIGTERM",
        "SIGSTKFLT",
        "SIGCHLD",
        "SIGCONT",
        "SIGSTOP",
        "SIGTSTP",
        "SIGTTIN",
        "SIGTTOU",
        "SIGURG",
        "SIGXCPU",
        "SIGXFSZ",
        "SIGVTALRM",
        "SIGPROF",
        "SIGWINCH",
        "SIGIO",
        "SIGPWR",
        "SIGSYS",
    ];
    usize::try_from(signal)
        .ok()
        .and_then(|index| index.checked_sub(1))
        .and_then(|index| names.get(index).copied())
}
#[cfg(all(
    unix,
    not(any(
        target_os = "macos",
        all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )
    ))
))]
fn platform_signal_name(_signal: i32) -> Option<&'static str> {
    None
}
#[cfg(not(unix))]
fn signal_name(_status: ExitStatus) -> String {
    "unknown".into()
}

#[cfg(unix)]
async fn terminate_direct(child: &mut Child) -> Result<(), &'static str> {
    // No wait on the media child has completed here: its unreaped PID remains
    // reserved while this helper runs. An unconfirmed helper wait becomes an
    // explicit cleanup error, never a joined-cleanup receipt.
    if let Some(pid) = child.id()
        && let Ok(mut signaler) = Command::new("/bin/kill")
            .arg("-TERM")
            .arg("--")
            .arg(pid.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
    {
        match tokio::time::timeout(TERM_GRACE, signaler.wait()).await {
            Ok(Ok(_)) => {}
            _ => {
                // Do not delay the media KILL escalation behind a stuck helper.
                let _ = child.start_kill();
                let _ = signaler.start_kill();
                if !matches!(
                    tokio::time::timeout(EXIT_WAIT_LIMIT, signaler.wait()).await,
                    Ok(Ok(_))
                ) {
                    return Err("SIGTERM helper exit could not be confirmed");
                }
            }
        }
    }
    Ok(())
}
#[cfg(not(unix))]
async fn terminate_direct(child: &mut Child) -> Result<(), &'static str> {
    let _ = child.start_kill();
    Ok(())
}

async fn drive<S: StdoutSink>(
    mut child: Child,
    stderr: ChildStderr,
    mut stdout: Option<ChildStdout>,
    mut sink: S,
    deadline: Instant,
    mut cancellation: watch::Receiver<bool>,
) -> Result<S::Output, MediaError> {
    let mut stderr = Some(stderr);
    let mut tail = StderrTail::new();
    let mut buffer = [0_u8; 8_192];
    let mut output_buffer = [0_u8; 8_192];
    let mut status: Option<ExitStatus> = None;
    let mut failure: Option<MediaError> = None;
    let mut kill_at = None;
    let mut drain_until = None;
    let mut exit_until = None;
    loop {
        if let Some(status) = status
            && stderr.is_none()
            && stdout.is_none()
        {
            return if let Some(error) = failure {
                Err(error)
            } else if status.success() {
                sink.finish()
                    .map_err(|message| MediaError::new(ErrorKind::Output, message))
            } else {
                Err(decode_failure(status, &tail))
            };
        }
        // Biased timer/cancellation branches precede unbounded producer reads.
        tokio::select! {
            biased;
            _ = async { cancellation.wait_for(|cancelled| *cancelled).await.map(|_| ()) }, if failure.is_none() => {
                failure = Some(MediaError::new(ErrorKind::Cancelled,"Media verification cancelled"));
                if status.is_none() {
                    kill_at = Some(Instant::now()+TERM_GRACE);
                    if let Err(issue) = terminate_direct(&mut child).await {
                        return Err(failure.unwrap().with_cleanup(issue));
                    }
                }
            },
            _ = sleep_until(deadline), if failure.is_none() => {
                failure = Some(MediaError::new(ErrorKind::TimedOut,"Media verification timed out"));
                if status.is_none() {
                    kill_at = Some(Instant::now()+TERM_GRACE);
                    if let Err(issue) = terminate_direct(&mut child).await {
                        return Err(failure.unwrap().with_cleanup(issue));
                    }
                }
            },
            _ = async {sleep_until(kill_at.unwrap()).await}, if kill_at.is_some() && status.is_none() => {
                kill_at = None;
                exit_until = Some(Instant::now()+EXIT_WAIT_LIMIT);
                // SIGKILL targets the owned child through Tokio. Once wait
                // completes this branch is disabled, so a PID is never reused.
                if child.start_kill().is_err() {
                    failure = Some(failure.take().unwrap().with_cleanup("direct child kill could not be confirmed"));
                }
            },
            _ = async {sleep_until(exit_until.unwrap()).await}, if exit_until.is_some() && status.is_none() => {
                return Err(failure.unwrap().with_cleanup("direct child exit could not be confirmed within cleanup deadline"));
            },
            _ = async {sleep_until(drain_until.unwrap()).await}, if drain_until.is_some() && (stderr.is_some() || stdout.is_some()) => {
                // Close our read end; do not infer that a descendant retaining
                // the write end exited, and never signal the already reaped PID.
                let issue=if stdout.is_some(){"media stdout pipe did not close after direct child exit"}else{"stderr pipe did not close after direct child exit"};
                stderr.take(); stdout.take();
                let error = failure.take().unwrap_or_else(|| {
                    if let Some(status) = status.filter(|status| !status.success()) {decode_failure(status,&tail)}
                    else {MediaError::new(ErrorKind::Cleanup,"Media verification cleanup unconfirmed")}
                });
                failure = Some(error.with_cleanup(issue));
            },
            waited = child.wait(), if status.is_none() => {
                match waited {
                    Ok(exit) => {
                        status = Some(exit);
                        kill_at = None;
                        exit_until = None;
                        // A timed-out child is still entitled to its bounded
                        // close drain after KILL+wait. Reusing an expired job
                        // deadline here would falsely report pipe uncertainty
                        // before a ready EOF could be consumed.
                        drain_until = Some(Instant::now()+STDERR_DRAIN_GRACE);
                    },
                    Err(_) => return Err(failure.unwrap_or_else(|| MediaError::new(ErrorKind::Cleanup,"Media verification cleanup unconfirmed")).with_cleanup("direct child wait failed")),
                }
            },
            read = async {
                // Fair pipe selection prevents a stdout/stderr flood starving
                // the other pipe; outer timer branches still win admission.
                tokio::select! {
                    read=async{stderr.as_mut().unwrap().read(&mut buffer).await},if stderr.is_some()=> (false,read),
                    read=async{stdout.as_mut().unwrap().read(&mut output_buffer).await},if stdout.is_some()=> (true,read),
                }
            }, if stderr.is_some() || stdout.is_some() => {
                let (is_stdout,result)=read;
                let consumed=match result {
                    Ok(0)=>{if is_stdout{stdout.take();}else{stderr.take();}Ok(())},
                    Ok(count)=>{
                        if is_stdout {
                            if failure.is_none(){sink.consume(&output_buffer[..count])}else{Ok(())}
                        } else {
                            tail.append(&buffer[..count]);
                            if failure.is_none(){sink.consume_stderr(&buffer[..count])}else{Ok(())}
                        }
                    },
                    Err(_)=>{
                        if is_stdout{stdout.take();}else{stderr.take();}
                        let issue=if is_stdout{"stdout read failed"}else{"stderr read failed"};
                        let was_stopping=failure.is_some();
                        failure=Some(failure.take().unwrap_or_else(||MediaError::new(ErrorKind::Cleanup,"Media verification cleanup unconfirmed")).with_cleanup(issue));
                        if status.is_none()&&!was_stopping {
                            kill_at=Some(Instant::now()+TERM_GRACE);
                            if let Err(issue)=terminate_direct(&mut child).await{return Err(failure.unwrap().with_cleanup(issue));}
                        }
                        Ok(())
                    },
                };
                if let Err(message)=consumed {
                    failure=Some(MediaError::new(ErrorKind::Output,message));
                    stdout.take();
                    if status.is_none() {
                        kill_at=Some(Instant::now()+TERM_GRACE);
                        if let Err(issue)=terminate_direct(&mut child).await{return Err(failure.unwrap().with_cleanup(issue));}
                    }
                }
            },
        }
    }
}
