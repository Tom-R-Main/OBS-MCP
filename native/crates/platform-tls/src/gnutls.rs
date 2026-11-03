//! Nonblocking GnuTLS experiment. Mac execution of this module is not Linux proof.
use super::gnutls_core::{self as core, Direction, Step, Version};
use std::{
    future::Future,
    io,
    os::fd::{AsRawFd, RawFd},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Instant,
};
use tokio::{
    io::unix::AsyncFd,
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    task::JoinHandle,
};

const MAX_ENTRIES: usize = 16;
pub use super::gnutls_core::Version as TlsVersions;
pub fn runtime_version() -> String {
    core::runtime_version()
}
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

struct SendCore(core::Stream);
// SAFETY: installed GnuTLS3.8.12 manual6.1.5 permits sessions divided across
// threads with exclusive access. This wrapper never permits concurrent calls:
// setup/teardown transfer ownership through joined tasks; active calls require
// the entry mutex. The core's Rc marker contains no actual Rc or thread-local
// callback. Owned boxed verification/buffer addresses survive moves unchanged.
unsafe impl Send for SendCore {}
impl AsRawFd for SendCore {
    fn as_raw_fd(&self) -> RawFd {
        self.0.fd()
    }
}
struct Io {
    fd: AsyncFd<SendCore>,
    handshake_wait: Option<Direction>,
    read_wait: Option<Direction>,
    write_wait: Option<Direction>,
}
#[derive(Default)]
struct Signal {
    wakers: Mutex<[Option<Waker>; 4]>,
    deferrals: AtomicUsize,
    pending: AtomicBool,
}
impl Signal {
    fn register(&self, slot: usize, waker: &Waker) {
        lock(&self.wakers)[slot] = Some(waker.clone());
    }
    fn notify(&self) {
        self.pending.store(true, Ordering::SeqCst);
        if self.deferrals.load(Ordering::SeqCst) == 0 {
            self.deliver();
        }
    }
    fn deliver(&self) {
        if !self.pending.swap(false, Ordering::SeqCst) {
            return;
        }
        let wakers = std::mem::take(&mut *lock(&self.wakers));
        for waker in wakers.into_iter().flatten() {
            waker.wake();
        }
    }
    fn defer(self: &Arc<Self>) -> Defer {
        self.deferrals.fetch_add(1, Ordering::SeqCst);
        Defer(self.clone())
    }
}
struct Defer(Arc<Signal>);
impl Drop for Defer {
    fn drop(&mut self) {
        if self.0.deferrals.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.deliver();
        }
    }
}
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.notify();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}
#[derive(Default)]
struct State {
    setup: Option<JoinHandle<io::Result<SendCore>>>,
    cleanup: Option<JoinHandle<()>>,
    io: Option<Io>,
    retired: bool,
    ready: bool,
    joined: bool,
    eof: bool,
    error: Option<String>,
    read: Vec<u8>,
    read_offset: usize,
    accepted_bytes: usize,
    version: Option<i32>,
    alpn: Option<Vec<u8>>,
}
struct Entry {
    state: Mutex<State>,
    signal: Arc<Signal>,
    monitor: core::Monitor,
    cancel: Arc<AtomicBool>,
    runtime: tokio::runtime::Handle,
}
impl Entry {
    fn retire(&self, error: Option<String>) {
        {
            let mut state = lock(&self.state);
            state.retired = true;
            self.cancel.store(true, Ordering::SeqCst);
            if state.error.is_none() {
                state.error = error;
            }
            if let Some(io) = state.io.take() {
                self.destroy(&mut state, io.fd.into_inner());
            }
        }
        self.signal.notify();
    }
    fn destroy(&self, state: &mut State, native: SendCore) {
        // No per-I/O blocking task. Native destruction/global finalization is a
        // single retained cleanup job, so Stream Drop cannot wait on the executor.
        assert!(state.cleanup.is_none());
        state.cleanup = Some(self.runtime.spawn_blocking(move || drop(native)));
    }
    fn cleanup_step(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.signal.register(3, cx.waker());
        let _defer = self.signal.defer();
        let waker = Waker::from(self.signal.clone());
        let mut driver_cx = Context::from_waker(&waker);
        let mut state = lock(&self.state);
        if let Some(setup) = state.setup.as_mut() {
            match Pin::new(setup).poll(&mut driver_cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    state.setup = None;
                    match result {
                        Ok(Ok(native)) => self.destroy(&mut state, native),
                        Ok(Err(error)) => {
                            if state.error.is_none() {
                                state.error = Some(error.to_string());
                            }
                        }
                        Err(error) => {
                            state.error = Some(format!("native setup task failed: {error}"));
                        }
                    }
                }
            }
        }
        if let Some(cleanup) = state.cleanup.as_mut() {
            match Pin::new(cleanup).poll(&mut driver_cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    state.cleanup = None;
                    if let Err(error) = result {
                        state.error = Some(format!("native cleanup task failed: {error}"));
                    }
                }
            }
        }
        let counters = self.monitor.snapshot();
        if state.io.is_none() && counters[..3] == [0, 0, 0] {
            state.joined = true;
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

fn poll_step<T>(
    fd: &mut AsyncFd<SendCore>,
    wait: &mut Option<Direction>,
    cx: &mut Context<'_>,
    mut operation: impl FnMut(&mut core::Stream) -> io::Result<Step<T>>,
) -> Poll<io::Result<T>> {
    // Bounded work per poll. Positive partial progress is not E_AGAIN and must
    // not clear readiness. A Pending result is returned only by native AGAIN/INTERRUPTED.
    for _ in 0..16 {
        let step = if let Some(direction) = *wait {
            let guard = match direction {
                Direction::Read => fd.poll_read_ready_mut(cx),
                Direction::Write => fd.poll_write_ready_mut(cx),
            };
            let mut guard = match guard {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(guard)) => guard,
            };
            let step = operation(&mut guard.get_inner_mut().0);
            if matches!(step, Ok(Step::Pending(_))) {
                guard.clear_ready();
            }
            step
        } else {
            operation(&mut fd.get_mut().0)
        };
        match step {
            Ok(Step::Ready(value)) => {
                *wait = None;
                return Poll::Ready(Ok(value));
            }
            Ok(Step::Pending(direction)) => *wait = Some(direction),
            Ok(Step::Progress) => *wait = None,
            Err(error) => return Poll::Ready(Err(error)),
        }
    }
    // The caller wakes after releasing the state mutex; no reentrant wake under it.
    Poll::Ready(Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        "TLS poll work budget exhausted",
    )))
}
fn io_error(state: &State) -> Option<io::Error> {
    state
        .error
        .as_ref()
        .map(|error| io::Error::new(io::ErrorKind::ConnectionAborted, error.clone()))
        .or_else(|| {
            state
                .retired
                .then(|| io::Error::new(io::ErrorKind::NotConnected, "retired TLS stream"))
        })
}
fn deferred<T>(result: Poll<io::Result<T>>, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
    if matches!(&result, Poll::Ready(Err(error)) if error.kind()==io::ErrorKind::WouldBlock) {
        cx.waker().wake_by_ref();
        Poll::Pending
    } else {
        result
    }
}

#[derive(Default)]
#[must_use = "retain the Service supervisor and await shutdown"]
pub struct Supervisor {
    entries: Vec<Arc<Entry>>,
    stopped: bool,
    additional_roots: Option<crate::AdditionalRoots>,
}
impl Supervisor {
    pub fn with_additional_roots(roots: crate::AdditionalRoots) -> io::Result<Self> {
        Ok(Self {
            entries: Vec::new(),
            stopped: false,
            additional_roots: Some(roots),
        })
    }

    /// DNS/TCP ownership remains with the parent connector. This method consumes
    /// its already connected socket, expected identity and unchanged deadline.
    pub fn connect_socket(
        &mut self,
        socket: TcpStream,
        expected: &str,
        deadline: Instant,
    ) -> io::Result<Connect> {
        self.connect_inner(socket, expected, Version::Supported, deadline, None)
    }
    pub fn connect_socket_with_versions(
        &mut self,
        socket: TcpStream,
        expected: &str,
        version: Version,
        deadline: Instant,
    ) -> io::Result<Connect> {
        self.connect_inner(socket, expected, version, deadline, None)
    }
    #[cfg(feature = "gnutls-synthetic-probe")]
    pub fn connect_synthetic_fixture(
        &mut self,
        socket: TcpStream,
        expected: &str,
        version: Version,
        deadline: Instant,
        anchor: Vec<u8>,
    ) -> io::Result<Connect> {
        if !socket.peer_addr()?.ip().is_loopback() || anchor.len() > 16_384 {
            return Err(io::Error::other(
                "synthetic trust requires bounded loopback fixture",
            ));
        }
        self.connect_inner(socket, expected, version, deadline, Some(anchor))
    }
    fn connect_inner(
        &mut self,
        socket: TcpStream,
        expected: &str,
        version: Version,
        deadline: Instant,
        anchor: Option<Vec<u8>>,
    ) -> io::Result<Connect> {
        if self.stopped || Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "inactive owner or original establishment deadline",
            ));
        }
        if expected.is_empty()
            || expected.len() > 253
            || !expected.is_ascii()
            || expected.contains('\0')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "explicit ASCII DNS/IP expected identity required",
            ));
        }
        self.entries.retain(|entry| !lock(&entry.state).joined);
        if self.entries.len() >= MAX_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "retained TLS owner capacity",
            ));
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(io::Error::other)?;
        let socket = socket.into_std()?;
        let expected = expected.to_owned();
        let monitor = core::Monitor::default();
        let cancel = Arc::new(AtomicBool::new(false));
        let job_monitor = monitor.clone();
        let job_cancel = cancel.clone();
        let additional_roots = self.additional_roots.clone();
        let setup = runtime.spawn_blocking(move || {
            let native = if let Some(anchor) = anchor {
                #[cfg(feature = "gnutls-synthetic-probe")]
                {
                    let native = core::Stream::synthetic(
                        socket,
                        &expected,
                        version,
                        deadline,
                        job_cancel,
                        job_monitor,
                        &anchor,
                    )?;
                    if let Some(roots) = additional_roots.as_ref() {
                        native.with_additional_roots(roots)?
                    } else {
                        native
                    }
                }
                #[cfg(not(feature = "gnutls-synthetic-probe"))]
                {
                    let _ = anchor;
                    return Err(io::Error::other("synthetic trust excluded"));
                }
            } else {
                core::Stream::system_trust(
                    socket,
                    &expected,
                    version,
                    deadline,
                    job_cancel,
                    job_monitor,
                    additional_roots.as_ref(),
                )?
            };
            Ok(SendCore(native))
        });
        let entry = Arc::new(Entry {
            state: Mutex::new(State {
                setup: Some(setup),
                ..State::default()
            }),
            signal: Arc::default(),
            monitor,
            cancel,
            runtime,
        });
        self.entries.push(entry.clone());
        Ok(Connect {
            entry,
            deadline: Box::pin(tokio::time::sleep_until(deadline.into())),
            transferred: false,
        })
    }
    pub fn snapshots(&self) -> Vec<Snapshot> {
        self.entries
            .iter()
            .map(|entry| {
                let state = lock(&entry.state);
                Snapshot {
                    retired: state.retired,
                    ready: state.ready,
                    joined: state.joined,
                    error: state.error.clone(),
                    counters: entry.monitor.snapshot(),
                    accepted_bytes: state.accepted_bytes,
                    version: state.version,
                    alpn: state.alpn.clone(),
                    setup_pending: state.setup.is_some(),
                    cleanup_pending: state.cleanup.is_some(),
                }
            })
            .collect()
    }
    pub async fn shutdown(&mut self) -> Vec<Snapshot> {
        self.stopped = true;
        for entry in &self.entries {
            entry.retire(None);
        }
        std::future::poll_fn(|cx| {
            let mut joined = true;
            for entry in &self.entries {
                if entry.cleanup_step(cx).is_pending() {
                    joined = false;
                }
            }
            if joined {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        let snapshots = self.snapshots();
        self.entries.clear();
        snapshots
    }
}
impl Drop for Supervisor {
    fn drop(&mut self) {
        for entry in &self.entries {
            entry.retire(None);
        }
    }
}
#[derive(Debug)]
pub struct Snapshot {
    pub retired: bool,
    pub ready: bool,
    pub joined: bool,
    pub error: Option<String>,
    pub counters: [usize; 9],
    pub accepted_bytes: usize,
    pub version: Option<i32>,
    pub alpn: Option<Vec<u8>>,
    pub setup_pending: bool,
    pub cleanup_pending: bool,
}
pub struct Connect {
    entry: Arc<Entry>,
    deadline: Pin<Box<tokio::time::Sleep>>,
    transferred: bool,
}
impl Future for Connect {
    type Output = io::Result<Stream>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.deadline.as_mut().poll(cx).is_ready() {
            self.entry
                .retire(Some("original establishment deadline".into()));
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "original establishment deadline",
            )));
        }
        self.entry.signal.register(0, cx.waker());
        let _defer = self.entry.signal.defer();
        let driver_waker = Waker::from(self.entry.signal.clone());
        let mut driver_cx = Context::from_waker(&driver_waker);
        let result = {
            let mut state = lock(&self.entry.state);
            if let Some(error) = io_error(&state) {
                Poll::Ready(Err(error))
            } else {
                if let Some(setup) = state.setup.as_mut() {
                    match Pin::new(setup).poll(&mut driver_cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => {
                            state.setup = None;
                            let native = match result {
                                Ok(Ok(native)) => native,
                                Ok(Err(error)) => {
                                    drop(state);
                                    self.entry.retire(Some(error.to_string()));
                                    return Poll::Ready(Err(error));
                                }
                                Err(error) => {
                                    drop(state);
                                    let error = io::Error::other(error);
                                    self.entry.retire(Some(error.to_string()));
                                    return Poll::Ready(Err(error));
                                }
                            };
                            // SAFETY: the private core owns its TcpStream and
                            // always returns the same open fd. TLS close frees
                            // session/credentials but retains the socket until
                            // core destruction, which follows deregistration.
                            match unsafe { AsyncFd::register(native) } {
                                Ok(fd) => {
                                    state.io = Some(Io {
                                        fd,
                                        handshake_wait: None,
                                        read_wait: None,
                                        write_wait: None,
                                    })
                                }
                                Err(registration) => {
                                    let (native, error) = registration.into_parts();
                                    self.entry.destroy(&mut state, native);
                                    drop(state);
                                    self.entry.retire(Some(error.to_string()));
                                    return Poll::Ready(Err(error));
                                }
                            }
                        }
                    }
                }
                let io = state.io.as_mut().unwrap();
                let result = poll_step(
                    &mut io.fd,
                    &mut io.handshake_wait,
                    &mut driver_cx,
                    core::Stream::handshake,
                );
                if let Poll::Ready(Ok(())) = result {
                    let version = io.fd.get_ref().0.version();
                    let alpn = match io.fd.get_mut().0.alpn() {
                        Ok(alpn) => alpn,
                        Err(error) => {
                            drop(state);
                            self.entry.retire(Some(error.to_string()));
                            return Poll::Ready(Err(error));
                        }
                    };
                    state.version = Some(version);
                    state.alpn = alpn;
                    state.ready = true;
                }
                result
            }
        };
        match result {
            Poll::Ready(Ok(())) => {
                self.transferred = true;
                Poll::Ready(Ok(Stream {
                    entry: self.entry.clone(),
                }))
            }
            Poll::Ready(Err(error)) if error.kind() != io::ErrorKind::WouldBlock => {
                self.entry.retire(Some(error.to_string()));
                Poll::Ready(Err(error))
            }
            other => deferred(other.map(|r| r.map(|_| unreachable!())), cx),
        }
    }
}
impl Drop for Connect {
    fn drop(&mut self) {
        if !self.transferred {
            self.entry.retire(None);
        }
    }
}
pub struct Stream {
    entry: Arc<Entry>,
}
impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        self.entry.signal.register(1, cx.waker());
        let _defer = self.entry.signal.defer();
        let driver_waker = Waker::from(self.entry.signal.clone());
        let mut driver_cx = Context::from_waker(&driver_waker);
        let result = {
            let mut state = lock(&self.entry.state);
            if state.read_offset < state.read.len() {
                let n = output.remaining().min(state.read.len() - state.read_offset);
                output.put_slice(&state.read[state.read_offset..state.read_offset + n]);
                state.read_offset += n;
                if state.read_offset == state.read.len() {
                    state.read.clear();
                    state.read_offset = 0;
                }
                return Poll::Ready(Ok(()));
            }
            if state.eof {
                return Poll::Ready(Ok(()));
            }
            if let Some(error) = io_error(&state) {
                return Poll::Ready(Err(error));
            }
            let io = state.io.as_mut().unwrap();
            poll_step(
                &mut io.fd,
                &mut io.read_wait,
                &mut driver_cx,
                core::Stream::read_step,
            )
        };
        match result {
            Poll::Ready(Ok(bytes)) => {
                if bytes.is_empty() {
                    lock(&self.entry.state).eof = true;
                    self.entry.retire(None);
                    return Poll::Ready(Ok(()));
                }
                let n = output.remaining().min(bytes.len());
                output.put_slice(&bytes[..n]);
                if n < bytes.len() {
                    let mut state = lock(&self.entry.state);
                    state.read = bytes;
                    state.read_offset = n;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) if error.kind() != io::ErrorKind::WouldBlock => {
                self.entry.retire(Some(error.to_string()));
                Poll::Ready(Err(error))
            }
            other => deferred(other.map(|r| r.map(|_| ())), cx),
        }
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        input: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        let result = {
            let mut state = lock(&self.entry.state);
            if let Some(error) = io_error(&state) {
                return Poll::Ready(Err(error));
            }
            let n = state.io.as_mut().unwrap().fd.get_mut().0.enqueue(input)?;
            state.accepted_bytes += n;
            n
        };
        Poll::Ready(Ok(result))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.entry.signal.register(2, cx.waker());
        let driver_waker = Waker::from(self.entry.signal.clone());
        let mut driver_cx = Context::from_waker(&driver_waker);
        let _defer = self.entry.signal.defer();
        let result = {
            let mut state = lock(&self.entry.state);
            if let Some(error) = io_error(&state) {
                return Poll::Ready(Err(error));
            }
            let io = state.io.as_mut().unwrap();
            if !io.fd.get_ref().0.has_pending_send() {
                return Poll::Ready(Ok(()));
            }
            poll_step(
                &mut io.fd,
                &mut io.write_wait,
                &mut driver_cx,
                core::Stream::flush_step,
            )
        };
        match result {
            Poll::Ready(Err(error)) if error.kind() != io::ErrorKind::WouldBlock => {
                self.entry.retire(Some(error.to_string()));
                Poll::Ready(Err(error))
            }
            other => deferred(other, cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !lock(&self.entry.state).retired {
            match self.as_mut().poll_flush(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
        }
        self.entry.retire(None);
        self.entry.cleanup_step(cx).map(Ok)
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        self.entry.retire(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn io_and_owner_types_are_send_without_shared_native_access() {
        fn required<T: Send + Unpin>() {}
        required::<Stream>();
        required::<Connect>();
        required::<Supervisor>();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn abandoned_setup_remains_retained_until_supervisor_join() {
        let gate = Arc::new(std::sync::Barrier::new(2));
        let task_gate = gate.clone();
        let runtime = tokio::runtime::Handle::current();
        let setup = runtime.spawn_blocking(move || {
            task_gate.wait();
            Err(io::Error::other("synthetic delayed setup refusal"))
        });
        let entry = Arc::new(Entry {
            state: Mutex::new(State {
                setup: Some(setup),
                ..State::default()
            }),
            signal: Arc::default(),
            monitor: core::Monitor::default(),
            cancel: Arc::new(AtomicBool::new(false)),
            runtime,
        });
        let connect = Connect {
            entry: entry.clone(),
            deadline: Box::pin(tokio::time::sleep(Duration::from_secs(30))),
            transferred: false,
        };
        drop(connect);
        assert!(lock(&entry.state).retired);
        assert!(lock(&entry.state).setup.is_some());
        let mut owner = Supervisor {
            entries: vec![entry],
            stopped: false,
            additional_roots: None,
        };
        let mut shutdown = Box::pin(owner.shutdown());
        assert!(
            shutdown
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        gate.wait();
        let snapshots = tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .unwrap();
        assert!(snapshots[0].joined);
        assert!(!snapshots[0].setup_pending);
        assert!(!snapshots[0].cleanup_pending);
        assert_eq!(&snapshots[0].counters[..3], &[0, 0, 0]);
    }
    #[test]
    fn shared_readiness_wakes_both_operations_after_state_unlock() {
        struct Reenter {
            state: Arc<Mutex<()>>,
            calls: Arc<AtomicUsize>,
        }
        impl Wake for Reenter {
            fn wake(self: Arc<Self>) {
                assert!(self.state.try_lock().is_ok());
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
        }
        let signal = Arc::<Signal>::default();
        let state = Arc::new(Mutex::new(()));
        let calls = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(Reenter {
            state: state.clone(),
            calls: calls.clone(),
        }));
        signal.register(1, &waker);
        signal.register(2, &waker);
        let defer = signal.defer();
        let guard = lock(&state);
        signal.notify();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(guard);
        drop(defer);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
