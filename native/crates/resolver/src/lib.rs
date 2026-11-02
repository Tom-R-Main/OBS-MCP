//! Isolated fixed self-helper resolver. No default product activation.
#![forbid(unsafe_code)]
mod helper;
mod protocol;
pub use helper::internal_entry;
pub use protocol::{
    Error, ExpectedPeer, MAX_ADDRESSES, MAX_HOST, MAX_REPLY, MAX_REQUEST, Resolved,
};
use std::{
    collections::VecDeque,
    future::Future,
    path::PathBuf,
    pin::Pin,
    process::{ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::{oneshot, watch},
    task::JoinHandle,
    time::Instant,
};

pub const MAX_JOBS: usize = 2;
const HISTORY: usize = 16;
#[derive(Clone, Copy)]
enum Mode {
    Normal,
    #[cfg(feature = "resolver-fixture")]
    Stall,
    #[cfg(all(feature = "resolver-fixture", unix))]
    Resistant,
    #[cfg(feature = "resolver-fixture")]
    Oversize,
    #[cfg(feature = "resolver-fixture")]
    Linger,
    #[cfg(feature = "resolver-fixture")]
    Trailing,
}
impl Mode {
    fn argument(self) -> &'static str {
        match self {
            Self::Normal => helper::ENTRY,
            #[cfg(feature = "resolver-fixture")]
            Self::Stall => "--obs-mcp-internal-resolver-stall-v1",
            #[cfg(all(feature = "resolver-fixture", unix))]
            Self::Resistant => "--obs-mcp-internal-resolver-resistant-v1",
            #[cfg(feature = "resolver-fixture")]
            Self::Oversize => "--obs-mcp-internal-resolver-oversize-v1",
            #[cfg(feature = "resolver-fixture")]
            Self::Linger => "--obs-mcp-internal-resolver-linger-v1",
            #[cfg(feature = "resolver-fixture")]
            Self::Trailing => "--obs-mcp-internal-resolver-stall-v1",
        }
    }
}
#[derive(Debug, Clone)]
pub struct Receipt {
    pub pid: Option<u32>,
    pub joined: bool,
    pub kill_requested: bool,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub reaped: bool,
    pub output_bytes: usize,
    pub fixture_ready: bool,
    pub class: Result<(), Error>,
    pub elapsed_micros: u128,
}
struct Job {
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<Receipt>>,
    monitor: Arc<JobMonitor>,
}
#[derive(Default)]
struct JobMonitor {
    pid: AtomicU32,
    bytes: AtomicUsize,
    ready: AtomicBool,
}
#[derive(Debug)]
pub struct JobSnapshot {
    pub spawned: bool,
    pub pid: Option<u32>,
    pub output_bytes: usize,
    pub fixture_ready: bool,
}
/// Keep in the Service/connection-bootstrap owner. Never move the sole Owner
/// into a request future; dropped Resolve/shutdown futures preserve its jobs.
#[must_use = "retain resolver Owner and await shutdown before stopping its runtime"]
pub struct Owner {
    executable: PathBuf,
    jobs: Vec<Job>,
    history: VecDeque<Receipt>,
    stopped: bool,
    broken: bool,
}
impl Owner {
    pub fn new() -> Result<Self, Error> {
        let executable = std::env::current_exe().map_err(|_| Error::Spawn)?;
        if !executable.is_absolute() {
            return Err(Error::Spawn);
        }
        Ok(Self {
            executable,
            jobs: Vec::with_capacity(MAX_JOBS),
            history: VecDeque::with_capacity(HISTORY),
            stopped: false,
            broken: false,
        })
    }
    /// Test-only seam for the main product executable supplied by Cargo's
    /// CARGO_BIN_EXE_obs-mcp-native. Default construction always uses current_exe;
    /// callers cannot select helper arguments, environment or a program via MCP.
    #[cfg(feature = "resolver-fixture")]
    pub fn fixture_with_executable(executable: PathBuf) -> Result<Self, Error> {
        if !executable.is_absolute()
            || !std::fs::symlink_metadata(&executable)
                .is_ok_and(|metadata| metadata.file_type().is_file())
        {
            return Err(Error::Spawn);
        }
        let mut owner = Self::new()?;
        owner.executable = executable;
        Ok(owner)
    }
    pub fn resolve(
        &mut self,
        expected: ExpectedPeer,
        port: u16,
        deadline: Instant,
    ) -> Result<Resolve, Error> {
        self.start(expected, port, deadline, Mode::Normal)
    }
    fn remember(&mut self, receipt: Receipt) {
        if !receipt.joined {
            self.broken = true;
        }
        if self.history.len() == HISTORY {
            self.history.pop_front();
        }
        self.history.push_back(receipt);
    }
    fn collect_finished(&mut self) {
        let mut index = 0;
        while index < self.jobs.len() {
            let finished = self.jobs[index]
                .task
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if !finished {
                index += 1;
                continue;
            }
            let result = Pin::new(self.jobs[index].task.as_mut().unwrap())
                .poll(&mut Context::from_waker(std::task::Waker::noop()));
            match result {
                Poll::Ready(result) => {
                    self.jobs.swap_remove(index);
                    self.remember(result.unwrap_or_else(|_| failed_task()));
                }
                Poll::Pending => index += 1,
            }
        }
    }
    fn start(
        &mut self,
        expected: ExpectedPeer,
        port: u16,
        deadline: Instant,
        mode: Mode,
    ) -> Result<Resolve, Error> {
        self.collect_finished();
        if self.stopped || self.broken {
            return Err(Error::OwnerStopped);
        }
        if port == 0 {
            return Err(Error::InvalidInput);
        }
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        if let ExpectedPeer::Ip(ip) = expected {
            return Ok(Resolve {
                ready: Some(Ok(Resolved {
                    expected: ExpectedPeer::Ip(ip),
                    port,
                    addresses: vec![std::net::SocketAddr::new(ip, port)],
                })),
                receiver: None,
                cancel: None,
                finished: false,
            });
        }
        // Revalidate privately constructed enum data at the admission boundary.
        let expected = ExpectedPeer::parse(expected.dns_name().ok_or(Error::InvalidInput)?)?;
        if self.jobs.len() >= MAX_JOBS {
            return Err(Error::Capacity);
        }
        let input = protocol::request(&expected, port)?;
        let executable = self.executable.clone();
        let (cancel, changed) = watch::channel(false);
        let (reply, receiver) = oneshot::channel();
        let monitor = Arc::<JobMonitor>::default();
        let driver_monitor = monitor.clone();
        let task = tokio::spawn(async move {
            let (result, receipt) = drive(DriverInput {
                executable,
                input,
                expected,
                port,
                deadline,
                cancel: changed,
                mode,
                monitor: driver_monitor,
            })
            .await;
            let _ = reply.send(result);
            receipt
        });
        self.jobs.push(Job {
            cancel: cancel.clone(),
            task: Some(task),
            monitor,
        });
        Ok(Resolve {
            ready: None,
            receiver: Some(receiver),
            cancel: Some(cancel),
            finished: false,
        })
    }
    pub fn retained_jobs(&self) -> usize {
        self.jobs.len()
    }
    pub fn snapshots(&self) -> Vec<JobSnapshot> {
        self.jobs
            .iter()
            .map(|job| JobSnapshot {
                spawned: job.monitor.pid.load(Ordering::SeqCst) != 0,
                pid: match job.monitor.pid.load(Ordering::SeqCst) {
                    0 => None,
                    pid => Some(pid),
                },
                output_bytes: job.monitor.bytes.load(Ordering::SeqCst),
                fixture_ready: job.monitor.ready.load(Ordering::SeqCst),
            })
            .collect()
    }
    pub fn receipts(&self) -> Vec<Receipt> {
        self.history.iter().cloned().collect()
    }
    pub async fn shutdown(&mut self) -> Result<Vec<Receipt>, Error> {
        self.stopped = true;
        for job in &self.jobs {
            job.cancel.send_replace(true);
        }
        while !self.jobs.is_empty() {
            let result = self.jobs[0]
                .task
                .as_mut()
                .ok_or(Error::CleanupUnconfirmed)?
                .await;
            self.jobs.swap_remove(0);
            self.remember(result.unwrap_or_else(|_| failed_task()));
        }
        if self.broken {
            Err(Error::CleanupUnconfirmed)
        } else {
            Ok(self.receipts())
        }
    }
    #[cfg(feature = "resolver-fixture")]
    pub fn fixture(&mut self, kind: Fixture, deadline: Instant) -> Result<Resolve, Error> {
        let mode = match kind {
            Fixture::Stall => Mode::Stall,
            #[cfg(unix)]
            Fixture::Resistant => Mode::Resistant,
            Fixture::Oversize => Mode::Oversize,
            Fixture::Linger => Mode::Linger,
            Fixture::Trailing => Mode::Trailing,
        };
        self.start(ExpectedPeer::parse("localhost")?, 4455, deadline, mode)
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        for job in &self.jobs {
            job.cancel.send_replace(true);
        }
    }
}
fn failed_task() -> Receipt {
    Receipt {
        pid: None,
        joined: false,
        kill_requested: false,
        exit_code: None,
        exit_signal: None,
        reaped: false,
        output_bytes: 0,
        fixture_ready: false,
        class: Err(Error::CleanupUnconfirmed),
        elapsed_micros: 0,
    }
}
#[cfg(feature = "resolver-fixture")]
#[derive(Clone, Copy)]
pub enum Fixture {
    Stall,
    #[cfg(unix)]
    Resistant,
    Oversize,
    Linger,
    Trailing,
}

pub struct Resolve {
    ready: Option<Result<Resolved, Error>>,
    receiver: Option<oneshot::Receiver<Result<Resolved, Error>>>,
    cancel: Option<watch::Sender<bool>>,
    finished: bool,
}
impl Future for Resolve {
    type Output = Result<Resolved, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.ready.take() {
            self.finished = true;
            return Poll::Ready(result);
        }
        let Some(receiver) = &mut self.receiver else {
            return Poll::Ready(Err(Error::Protocol));
        };
        match Pin::new(receiver).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.finished = true;
                Poll::Ready(result.unwrap_or(Err(Error::CleanupUnconfirmed)))
            }
        }
    }
}
impl Drop for Resolve {
    fn drop(&mut self) {
        if !self.finished
            && let Some(cancel) = &self.cancel
        {
            cancel.send_replace(true);
        }
    }
}

async fn kill_and_reap_owned(
    child: &mut Child,
    lifeline: &mut Option<tokio::process::ChildStdin>,
) -> Option<ExitStatus> {
    // Request kill while the parent writer is still held. Only afterward close
    // the lifeline; an observed exit signal is distinct from a kill request.
    let _ = child.start_kill();
    drop(lifeline.take());
    child.wait().await.ok()
}
struct DriverInput {
    executable: PathBuf,
    input: Vec<u8>,
    expected: ExpectedPeer,
    port: u16,
    deadline: Instant,
    cancel: watch::Receiver<bool>,
    mode: Mode,
    monitor: Arc<JobMonitor>,
}
async fn drive(job: DriverInput) -> (Result<Resolved, Error>, Receipt) {
    let DriverInput {
        executable,
        input,
        expected,
        port,
        deadline,
        mut cancel,
        mode,
        monitor,
    } = job;
    let start = Instant::now();
    let mut receipt = Receipt {
        pid: None,
        joined: true,
        kill_requested: false,
        exit_code: None,
        exit_signal: None,
        reaped: false,
        output_bytes: 0,
        fixture_ready: false,
        class: Ok(()),
        elapsed_micros: 0,
    };
    if *cancel.borrow() || Instant::now() >= deadline {
        let error = if *cancel.borrow() {
            Error::Cancelled
        } else {
            Error::Deadline
        };
        receipt.class = Err(error);
        return (Err(error), receipt);
    }
    // No hostname/port/credentials are arguments. No PATH search, shell, caller
    // executable, current-directory override, inherited environment or stderr log.
    let child = Command::new(executable)
        .arg(mode.argument())
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn();
    let mut child = match child {
        Ok(child) => child,
        Err(_) => {
            receipt.class = Err(Error::Spawn);
            return (Err(Error::Spawn), receipt);
        }
    };
    receipt.pid = child.id();
    monitor
        .pid
        .store(receipt.pid.unwrap_or(0), Ordering::SeqCst);
    let mut stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let mut buffer = [0; MAX_REPLY + 1];
    let mut count = 0;
    let mut exit_status = None;
    let exchange = async {
        let input_pipe = stdin.as_mut().ok_or(Error::Io)?;
        let mut stdout = stdout.ok_or(Error::Io)?;
        input_pipe.write_all(&input).await.map_err(|_| Error::Io)?;
        // Keep the writer open through response and actual child wait.
        #[cfg(feature = "resolver-fixture")]
        let mut trailing_sent = false;
        loop {
            if count == buffer.len() {
                return Err(Error::Protocol);
            }
            let n = stdout
                .read(&mut buffer[count..])
                .await
                .map_err(|_| Error::Io)?;
            if n == 0 {
                break;
            }
            count += n;
            monitor.bytes.store(count, Ordering::SeqCst);
            monitor
                .ready
                .store(count >= 4 && &buffer[..4] == b"RDY1", Ordering::SeqCst);
            #[cfg(feature = "resolver-fixture")]
            if matches!(mode, Mode::Trailing) && count >= 4 && !trailing_sent {
                input_pipe.write_all(b"X").await.map_err(|_| Error::Io)?;
                trailing_sent = true;
            }
            if count > MAX_REPLY {
                return Err(Error::Protocol);
            }
        }
        let status = child.wait().await.map_err(|_| Error::Io)?;
        exit_status = Some(status);
        let result = protocol::decode(&buffer[..count], expected, port)?;
        if !status.success() {
            return Err(Error::ChildFailed);
        }
        Ok(result)
    };
    let result = tokio::select! {biased;changed=cancel.changed()=>{let _=changed;Err(Error::Cancelled)},_=tokio::time::sleep_until(deadline)=>Err(Error::Deadline),result=exchange=>result};
    receipt.output_bytes = count;
    receipt.fixture_ready = count >= 4 && &buffer[..4] == b"RDY1";
    if exit_status.is_none() {
        receipt.kill_requested = true;
        exit_status = kill_and_reap_owned(&mut child, &mut stdin).await;
    }
    drop(stdin);
    receipt.reaped = exit_status.is_some();
    if let Some(status) = exit_status {
        receipt.exit_code = status.code();
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            receipt.exit_signal = status.signal();
        }
    }
    receipt.joined = receipt.reaped;
    let result = if !receipt.joined {
        Err(Error::CleanupUnconfirmed)
    } else if result.is_ok() && Instant::now() >= deadline {
        Err(Error::Deadline)
    } else {
        result
    };
    receipt.class = result.as_ref().map(|_| ()).map_err(|error| *error);
    receipt.elapsed_micros = start.elapsed().as_micros();
    (result, receipt)
}
