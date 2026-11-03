//! Isolated asynchronous Network.framework ownership prototype, not admitted product code.
#![deny(unsafe_op_in_unsafe_fn)]
use crate::ffi;
use block2::RcBlock;
use ffi::*;
use std::{
    ffi::{CStr, CString},
    future::Future,
    io,
    pin::Pin,
    ptr::{self, NonNull},
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Instant,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const CAP: usize = 65_536;
const MAX_ENTRIES: usize = 16;
type Code = (i32, i32);
#[derive(Clone, Copy)]
pub enum TlsVersions {
    Tls12,
    Tls13,
    Supported,
}
impl TlsVersions {
    fn bounds(self) -> (u16, u16) {
        match self {
            Self::Tls12 => (0x0303, 0x0303),
            Self::Tls13 => (0x0304, 0x0304),
            Self::Supported => (0x0303, 0x0304),
        }
    }
}
struct Native(NonNull<std::ffi::c_void>);
impl Native {
    fn new(raw: Raw) -> io::Result<Self> {
        NonNull::new(raw)
            .map(Self)
            .ok_or_else(|| io::Error::other("native allocation failed"))
    }
    fn raw(&self) -> Raw {
        self.0.as_ptr()
    }
}
// SAFETY: wrapper owns one refcounted OS object, not borrowed Rust memory. Active
// operations and final connection destruction run on its single private queue.
unsafe impl Send for Native {}
unsafe impl Sync for Native {}
impl Drop for Native {
    fn drop(&mut self) {
        unsafe { nw_release(self.raw()) };
    }
}
struct Security(Raw);
impl Drop for Security {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { sec_release(self.0) };
        }
    }
}
struct Data(Raw);
impl Drop for Data {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { dispatch_release(self.0) };
        }
    }
}
struct Queue(NonNull<std::ffi::c_void>);
// SAFETY: libdispatch queues support concurrent thread-safe submissions; queued
// work is proven Send+Sync before erasing it into the Objective-C Blocks ABI.
unsafe impl Send for Queue {}
unsafe impl Sync for Queue {}
impl Queue {
    fn new() -> io::Result<Self> {
        let attr =
            unsafe { dispatch_queue_attr_make_with_autorelease_frequency(ptr::null_mut(), 1) };
        NonNull::new(unsafe {
            dispatch_queue_create(c"obs-mcp.async-tls-evaluation".as_ptr(), attr)
        })
        .map(Self)
        .ok_or_else(|| io::Error::other("queue allocation failed"))
    }
    fn raw(&self) -> Raw {
        self.0.as_ptr()
    }
    fn submit(&self, job: impl FnOnce() + Send + 'static) {
        let slot = Mutex::new(Some(job));
        let callback = move || {
            if let Some(job) = slot.lock().unwrap_or_else(|e| e.into_inner()).take() {
                objc2::rc::autoreleasepool(|_| job());
            }
        };
        prove(&callback);
        let block = RcBlock::new(callback);
        // SAFETY: dispatch_async copies the block before returning; all captures
        // are owned Send+Sync. No synchronous wait occurs on an executor thread.
        unsafe { dispatch_async(self.raw(), &block) };
    }
}
impl Drop for Queue {
    fn drop(&mut self) {
        unsafe { dispatch_release(self.raw()) };
    }
}
fn prove<T: Send + Sync + 'static>(_: &T) {}
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|e| e.into_inner())
}
fn code(error: Raw) -> Option<Code> {
    if error.is_null() {
        None
    } else {
        Some(unsafe {
            (
                nw_error_get_error_domain(error),
                nw_error_get_error_code(error),
            )
        })
    }
}
fn failure(code: Code) -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionAborted,
        format!("native TLS terminal {}:{}", code.0, code.1),
    )
}

#[derive(Default)]
struct Facts {
    ready: bool,
    retired: bool,
    cancel_enqueued: bool,
    cancelled: bool,
    finishing: bool,
    released: bool,
    error: Option<Code>,
    eof: bool,
    pending: usize,
    captures: usize,
    read_pending: bool,
    write_pending: bool,
    read: Vec<u8>,
    read_offset: usize,
    reads: usize,
    writes: usize,
    accepted_bytes: usize,
    completed_bytes: usize,
    states: Vec<i32>,
    tls_version: Option<u16>,
    alpn: Option<String>,
    connect_waker: Option<Waker>,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    owner_waker: Option<Waker>,
}
#[derive(Default)]
struct Shared(Mutex<Facts>);
impl Shared {
    fn change(&self, f: impl FnOnce(&mut Facts)) {
        let wakers = {
            let mut facts = lock(&self.0);
            f(&mut facts);
            [
                facts.connect_waker.take(),
                facts.read_waker.take(),
                facts.write_waker.take(),
                facts.owner_waker.take(),
            ]
        };
        // A wake may immediately repoll. Never invoke arbitrary executor code under our mutex.
        for waker in wakers.into_iter().flatten() {
            waker.wake();
        }
    }
}
struct Hold(Arc<Shared>);
impl Hold {
    fn new(shared: &Arc<Shared>) -> Self {
        shared.change(|f| f.captures += 1);
        Self(shared.clone())
    }
}
impl Drop for Hold {
    fn drop(&mut self) {
        self.0.change(|f| f.captures -= 1);
    }
}
#[derive(Default)]
struct Resources {
    connection: Option<Arc<Native>>,
    _parameters: Option<Native>,
    privacy: Option<Native>,
}
struct Entry {
    shared: Arc<Shared>,
    resources: Mutex<Resources>,
    queue: Arc<Queue>,
    tls: bool,
    versions: TlsVersions,
    expected_peer: Arc<CString>,
    endpoint_port: CString,
}
impl Entry {
    fn retire(self: &Arc<Self>, error: Option<Code>) {
        let enqueue = {
            let mut f = lock(&self.shared.0);
            f.retired = true;
            if f.error.is_none() {
                f.error = error;
            }
            let enqueue = !f.cancel_enqueued;
            f.cancel_enqueued = true;
            enqueue
        };
        self.shared.change(|_| {});
        if !enqueue {
            return;
        }
        let entry = self.clone();
        self.queue.submit(move || {
            let connection = lock(&entry.resources).connection.clone();
            if let Some(connection) = connection {
                unsafe { nw_connection_force_cancel(connection.raw()) };
            } else {
                entry.shared.change(|f| f.cancelled = true);
            }
            entry.finalize();
        });
    }
    fn finalize(self: &Arc<Self>) {
        let eligible = {
            let mut f = lock(&self.shared.0);
            let yes = f.retired && f.cancelled && f.pending == 0 && !f.finishing;
            if yes {
                f.finishing = true;
            }
            yes
        };
        if !eligible {
            return;
        }
        // Only called on the private queue. Remove the callback cycle before
        // releasing handles; an in-flight callback retains its own Hold to return.
        let resources = std::mem::take(&mut *lock(&self.resources));
        if let Some(connection) = &resources.connection {
            unsafe { nw_connection_set_state_changed_handler(connection.raw(), None) };
        }
        if let Some(context) = &resources.privacy {
            // Only our owned private context, never the process default/global one.
            unsafe { nw_privacy_context_flush_cache(context.raw()) };
        }
        drop(resources);
        let entry = self.clone();
        self.queue
            .submit(move || entry.shared.change(|f| f.released = true));
    }
    fn joined(&self) -> bool {
        let f = lock(&self.shared.0);
        f.released && f.captures == 0 && f.pending == 0
    }
    fn start(self: &Arc<Self>) {
        let entry = self.clone();
        self.queue.submit(move || {
            if lock(&entry.shared.0).retired {
                entry.shared.change(|f| f.cancelled = true);
                entry.finalize();
                return;
            }
            if let Err(error) = entry.setup() {
                let _ = error; // Detailed native/certificate errors arrive through the state callback.
                entry.shared.change(|f| {
                    f.error = Some((-1, -1));
                    f.retired = true;
                    f.cancelled = true;
                });
                entry.finalize();
            }
        });
    }
    fn setup(self: &Arc<Self>) -> io::Result<()> {
        // Preserve the exact input allocations through native-handle destruction,
        // even if a native string setter/factory borrows instead of copying.
        let peer = self.expected_peer.clone();
        let endpoint = Native::new(unsafe {
            nw_endpoint_create_host(peer.as_ptr(), self.endpoint_port.as_ptr())
        })?;
        let hold = Hold::new(&self.shared);
        let (minimum, maximum) = self.versions.bounds();
        let configure = move |options: Raw| {
            let sec = Security(unsafe { nw_tls_copy_sec_protocol_options(options) });
            if sec.0.is_null() {
                hold.0.change(|f| f.error = Some((-1, -2)));
                return;
            }
            // Only default OS trust, required peer authentication, owned peer
            // name and HTTP/1.1. No verification/client-identity/URLSession API.
            unsafe {
                sec_protocol_options_set_min_tls_protocol_version(sec.0, minimum);
                sec_protocol_options_set_max_tls_protocol_version(sec.0, maximum);
                sec_protocol_options_set_tls_server_name(sec.0, peer.as_ptr());
                sec_protocol_options_add_tls_application_protocol(sec.0, c"http/1.1".as_ptr());
                sec_protocol_options_set_peer_authentication_required(sec.0, true);
                sec_protocol_options_set_tls_tickets_enabled(sec.0, false);
                sec_protocol_options_set_tls_resumption_enabled(sec.0, false);
                sec_protocol_options_set_tls_false_start_enabled(sec.0, false);
            }
        };
        prove(&configure);
        let configure = RcBlock::new(configure);
        let tcp = |options: Raw| unsafe {
            nw_tcp_options_set_no_delay(options, true);
            nw_tcp_options_set_enable_fast_open(options, false);
            nw_tcp_options_set_connection_timeout(options, 5);
        };
        prove(&tcp);
        let tcp = RcBlock::new(tcp);
        #[cfg(feature = "plain-fixture")]
        let raw_parameters = if self.tls {
            unsafe { nw_parameters_create_secure_tcp(&configure, &tcp) }
        } else {
            unsafe {
                nw_parameters_create_secure_tcp(&*_nw_parameters_configure_protocol_disable, &tcp)
            }
        };
        #[cfg(not(feature = "plain-fixture"))]
        let raw_parameters = unsafe { nw_parameters_create_secure_tcp(&configure, &tcp) };
        let parameters = Native::new(raw_parameters)?;
        let privacy =
            Native::new(unsafe { nw_privacy_context_create(c"obs-mcp.async-tls-owner".as_ptr()) })?;
        unsafe {
            nw_privacy_context_disable_logging(privacy.raw());
            nw_parameters_set_privacy_context(parameters.raw(), privacy.raw());
            nw_parameters_set_fast_open_enabled(parameters.raw(), false);
            nw_parameters_set_multipath_service(parameters.raw(), 0);
            nw_parameters_set_prefer_no_proxy(parameters.raw(), true);
        }
        let connection = Arc::new(Native::new(unsafe {
            nw_connection_create(endpoint.raw(), parameters.raw())
        })?);
        *lock(&self.resources) = Resources {
            connection: Some(connection.clone()),
            _parameters: Some(parameters),
            privacy: Some(privacy),
        };
        let entry = self.clone();
        let hold = Hold::new(&self.shared);
        let state = move |state: i32, error: Raw| {
            let _ = &hold;
            if state == 3 {
                let details = if entry.tls { entry.tls_details() } else { None };
                let (minimum, maximum) = entry.versions.bounds();
                let allowed = !entry.tls
                    || details.as_ref().is_some_and(|(version, alpn)| {
                        *version >= minimum
                            && *version <= maximum
                            && alpn.as_deref().is_none_or(|p| p == "http/1.1")
                    });
                if !allowed {
                    entry.retire(Some((-1, -3)));
                } else {
                    entry.shared.change(|f| {
                        if !f.retired {
                            f.ready = true;
                            if let Some((version, alpn)) = details {
                                f.tls_version = Some(version);
                                f.alpn = alpn;
                            }
                        }
                    });
                }
            }
            entry.shared.change(|f| {
                if f.states.len() < 16 {
                    f.states.push(state);
                }
                if state == 5 {
                    f.cancelled = true;
                }
            });
            if matches!(state, 0 | 1 | 4) || !(0..=5).contains(&state) {
                entry.retire(code(error).or(Some((-1, state))));
            }
            entry.finalize();
        };
        prove(&state);
        let state = RcBlock::new(state);
        unsafe {
            nw_connection_set_queue(connection.raw(), self.queue.raw());
            nw_connection_set_state_changed_handler(connection.raw(), Some(&state));
            nw_connection_start(connection.raw());
        }
        // Local block references disappear now; parameters/connection own any
        // copies. No RcBlock or unproven borrowed capture crosses Rust threads.
        Ok(())
    }
    fn tls_details(&self) -> Option<(u16, Option<String>)> {
        let connection = lock(&self.resources).connection.clone()?;
        // SAFETY: private queue, ready connection, correct Copy/Create families.
        unsafe {
            let definition = Native::new(nw_protocol_copy_tls_definition()).ok()?;
            let metadata = Native::new(nw_connection_copy_protocol_metadata(
                connection.raw(),
                definition.raw(),
            ))
            .ok()?;
            let sec = Security(nw_tls_copy_sec_protocol_metadata(metadata.raw()));
            if sec.0.is_null() {
                return None;
            }
            let protocol = sec_protocol_metadata_get_negotiated_protocol(sec.0);
            let alpn = if protocol.is_null() {
                None
            } else {
                let bytes = CStr::from_ptr(protocol).to_bytes();
                if bytes.len() > 255 {
                    return None;
                }
                Some(String::from_utf8(bytes.to_vec()).ok()?)
            };
            Some((
                sec_protocol_metadata_get_negotiated_tls_protocol_version(sec.0),
                alpn,
            ))
        }
    }
    fn begin_read(self: &Arc<Self>) {
        let entry = self.clone();
        self.queue.submit(move || {
            let connection = lock(&entry.resources).connection.clone();
            if lock(&entry.shared.0).retired || connection.is_none() {
                entry.shared.change(|f| {
                    f.pending -= 1;
                    f.read_pending = false;
                });
                entry.finalize();
                return;
            }
            let connection = connection.unwrap();
            let hold = Hold::new(&entry.shared);
            let callback_entry = entry.clone();
            let callback = move |content: Raw, context: Raw, complete: CBool, error: Raw| {
                let _ = &hold;
                let mut terminal = code(error);
                let mut bytes = Vec::new();
                let eof = complete.0
                    && !context.is_null()
                    && unsafe { nw_content_context_get_is_final(context) };
                if !content.is_null() {
                    let size = unsafe { dispatch_data_get_size(content) };
                    if size > CAP {
                        terminal = Some((-1, -4));
                    } else if size > 0 {
                        let mut pointer = ptr::null();
                        let mut count = 0;
                        let mapped = Data(unsafe {
                            dispatch_data_create_map(content, &mut pointer, &mut count)
                        });
                        if mapped.0.is_null() || pointer.is_null() || count != size {
                            terminal = Some((-1, -5));
                        } else {
                            bytes =
                                unsafe { std::slice::from_raw_parts(pointer.cast::<u8>(), size) }
                                    .to_vec();
                        }
                    }
                }
                callback_entry.shared.change(|f| {
                    f.pending -= 1;
                    f.read_pending = false;
                    if !f.retired {
                        f.read = bytes;
                        f.read_offset = 0;
                        f.eof = eof;
                    }
                });
                if terminal.is_some() || eof {
                    callback_entry.retire(terminal);
                }
                callback_entry.finalize();
            };
            prove(&callback);
            let callback = RcBlock::new(callback);
            unsafe { nw_connection_receive(connection.raw(), 1, CAP as u32, &callback) };
        });
    }
    fn begin_write(self: &Arc<Self>, bytes: Box<[u8]>) {
        let entry = self.clone();
        self.queue.submit(move || {
            let connection = lock(&entry.resources).connection.clone();
            if lock(&entry.shared.0).retired || connection.is_none() {
                entry.shared.change(|f| {
                    f.pending -= 1;
                    f.write_pending = false;
                });
                entry.finalize();
                return;
            }
            let connection = connection.unwrap();
            let n = bytes.len();
            // NULL destructor means libdispatch copies bytes before returning.
            let data = Data(unsafe {
                dispatch_data_create(bytes.as_ptr().cast(), n, ptr::null_mut(), None)
            });
            if data.0.is_null() {
                entry.shared.change(|f| {
                    f.pending -= 1;
                    f.write_pending = false;
                });
                entry.retire(Some((-1, -6)));
                return;
            }
            let hold = Hold::new(&entry.shared);
            let callback_entry = entry.clone();
            let callback = move |error: Raw| {
                let _ = &hold;
                let terminal = code(error);
                callback_entry.shared.change(|f| {
                    f.pending -= 1;
                    f.write_pending = false;
                    if terminal.is_none() {
                        f.completed_bytes += n;
                    }
                });
                if terminal.is_some() {
                    callback_entry.retire(terminal);
                }
                callback_entry.finalize();
            };
            prove(&callback);
            let callback = RcBlock::new(callback);
            unsafe {
                nw_connection_send(
                    connection.raw(),
                    data.0,
                    _nw_content_context_default_message,
                    true,
                    &callback,
                )
            };
        });
    }
}

/// Service-owned registry. Keep this owner across connection/upgrade timeouts
/// and actor shutdown, and await shutdown before its runtime can stop.
#[must_use = "retain supervisor and await shutdown after every stream/connection scope"]
#[derive(Default)]
pub struct Supervisor {
    entries: Vec<Arc<Entry>>,
    stopped: bool,
}
impl Supervisor {
    pub fn connect(&mut self, peer: &str, port: u16, deadline: Instant) -> io::Result<Connect> {
        self.connect_with_versions(peer, port, deadline, TlsVersions::Supported)
    }
    pub fn connect_with_versions(
        &mut self,
        peer: &str,
        port: u16,
        deadline: Instant,
        versions: TlsVersions,
    ) -> io::Result<Connect> {
        self.connect_inner(peer, port, deadline, true, versions)
    }
    #[cfg(feature = "plain-fixture")]
    pub fn connect_plain_fixture(
        &mut self,
        peer: &str,
        port: u16,
        deadline: Instant,
    ) -> io::Result<Connect> {
        if !matches!(peer, "localhost" | "127.0.0.1") {
            return Err(io::Error::other("plain fixture must be loopback"));
        }
        self.connect_inner(peer, port, deadline, false, TlsVersions::Supported)
    }
    fn connect_inner(
        &mut self,
        peer: &str,
        port: u16,
        deadline: Instant,
        tls: bool,
        versions: TlsVersions,
    ) -> io::Result<Connect> {
        if self.stopped || port == 0 || peer.is_empty() || peer.len() > 253 || !peer.is_ascii() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "explicit DNS/IP and active owner required",
            ));
        }
        let peer = CString::new(peer)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL peer"))?;
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "expired original deadline",
            ));
        }
        self.entries.retain(|entry| !entry.joined());
        if self.entries.len() >= MAX_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "retained TLS owner capacity",
            ));
        }
        let entry = Arc::new(Entry {
            shared: Arc::default(),
            resources: Mutex::default(),
            queue: Arc::new(Queue::new()?),
            tls,
            versions,
            expected_peer: Arc::new(peer),
            endpoint_port: CString::new(port.to_string()).unwrap(),
        });
        self.entries.push(entry.clone());
        entry.start();
        Ok(Connect {
            entry,
            deadline: Box::pin(tokio::time::sleep_until(deadline.into())),
            transferred: false,
        })
    }
    pub fn snapshots(&self) -> Vec<Snapshot> {
        self.entries
            .iter()
            .map(|e| Snapshot::from(&*lock(&e.shared.0)))
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
                let mut f = lock(&entry.shared.0);
                if !(f.released && f.captures == 0 && f.pending == 0) {
                    f.owner_waker = Some(cx.waker().clone());
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
        // Nonblocking emergency cancellation preserves native callback custody.
        // This fallback is NOT a joined shutdown; production must retain the
        // Service owner and await shutdown. Integration admission tests enforce it.
        for entry in &self.entries {
            if !entry.joined() {
                entry.retire(None);
            }
        }
    }
}
#[derive(Debug)]
pub struct Snapshot {
    pub ready: bool,
    pub retired: bool,
    pub cancelled: bool,
    pub released: bool,
    pub captures: usize,
    pub pending: usize,
    pub error: Option<Code>,
    pub states: Vec<i32>,
    pub reads: usize,
    pub writes: usize,
    pub accepted_bytes: usize,
    pub completed_bytes: usize,
    pub tls_version: Option<u16>,
    pub alpn: Option<String>,
}
impl From<&Facts> for Snapshot {
    fn from(f: &Facts) -> Self {
        Self {
            ready: f.ready,
            retired: f.retired,
            cancelled: f.cancelled,
            released: f.released,
            captures: f.captures,
            pending: f.pending,
            error: f.error,
            states: f.states.clone(),
            reads: f.reads,
            writes: f.writes,
            accepted_bytes: f.accepted_bytes,
            completed_bytes: f.completed_bytes,
            tls_version: f.tls_version,
            alpn: f.alpn.clone(),
        }
    }
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
            self.entry.retire(Some((-1, -7)));
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "original establishment deadline",
            )));
        }
        let state = {
            let mut f = lock(&self.entry.shared.0);
            if let Some(error) = f.error {
                Some(Err(failure(error)))
            } else if f.retired {
                Some(Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "retired connect",
                )))
            } else if f.ready {
                Some(Ok(()))
            } else {
                f.connect_waker = Some(cx.waker().clone());
                None
            }
        };
        match state {
            Some(Ok(())) => {
                self.transferred = true;
                Poll::Ready(Ok(Stream {
                    entry: self.entry.clone(),
                }))
            }
            Some(Err(error)) => {
                self.entry.retire(None);
                Poll::Ready(Err(error))
            }
            None => Poll::Pending,
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
        let mut f = lock(&self.entry.shared.0);
        if let Some(error) = f.error {
            return Poll::Ready(Err(failure(error)));
        }
        if f.read_offset < f.read.len() {
            let n = output.remaining().min(f.read.len() - f.read_offset);
            output.put_slice(&f.read[f.read_offset..f.read_offset + n]);
            f.read_offset += n;
            if f.read_offset == f.read.len() {
                f.read.clear();
                f.read_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }
        if f.eof {
            return Poll::Ready(Ok(()));
        }
        if f.retired {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "retired TLS stream",
            )));
        }
        f.read_waker = Some(cx.waker().clone());
        if !f.read_pending {
            f.read_pending = true;
            f.pending += 1;
            f.reads += 1;
            drop(f);
            self.entry.begin_read();
        }
        Poll::Pending
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        input: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut f = lock(&self.entry.shared.0);
        if let Some(error) = f.error {
            return Poll::Ready(Err(failure(error)));
        }
        if f.retired {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "retired TLS stream",
            )));
        }
        if f.write_pending {
            f.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = input.len().min(CAP);
        if n == 0 {
            return Poll::Ready(Ok(0));
        }
        let owned: Box<[u8]> = input[..n].into();
        f.write_pending = true;
        f.pending += 1;
        f.writes += 1;
        f.accepted_bytes += n;
        drop(f);
        self.entry.begin_write(owned);
        // Acceptance means copied into this bounded owner. Flush drives native
        // completion; a new caller never receives an earlier write's byte count.
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut f = lock(&self.entry.shared.0);
        if let Some(error) = f.error {
            return Poll::Ready(Err(failure(error)));
        }
        if f.write_pending {
            f.write_waker = Some(cx.waker().clone());
            Poll::Pending
        } else if f.retired {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "retired TLS stream",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        {
            let mut f = lock(&self.entry.shared.0);
            if f.write_pending && !f.retired {
                f.write_waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
        }
        self.entry.retire(None);
        let mut f = lock(&self.entry.shared.0);
        if f.released && f.captures == 0 && f.pending == 0 {
            Poll::Ready(Ok(()))
        } else {
            f.write_waker = Some(cx.waker().clone());
            Poll::Pending
        }
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

    fn entry() -> Arc<Entry> {
        Arc::new(Entry {
            shared: Arc::default(),
            resources: Mutex::default(),
            queue: Arc::new(Queue::new().unwrap()),
            tls: true,
            versions: TlsVersions::Supported,
            expected_peer: Arc::new(CString::new("localhost").unwrap()),
            endpoint_port: CString::new("0").unwrap(),
        })
    }
    #[tokio::test(flavor = "current_thread")]
    async fn abandoned_connect_stays_with_supervisor_until_native_release() {
        let entry = entry();
        let mut owner = Supervisor {
            entries: vec![entry.clone()],
            stopped: false,
        };
        let mut connect = Connect {
            entry: entry.clone(),
            deadline: Box::pin(tokio::time::sleep(Duration::from_secs(30))),
            transferred: false,
        };
        assert!(
            Pin::new(&mut connect)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(connect);
        assert!(lock(&entry.shared.0).retired);
        let joined = tokio::time::timeout(Duration::from_secs(1), owner.shutdown())
            .await
            .unwrap();
        assert!(joined[0].cancelled && joined[0].released);
        assert_eq!((joined[0].captures, joined[0].pending), (0, 0));
        assert!(owner.entries.is_empty());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn replacement_caller_never_receives_old_write_completion_count() {
        let entry = entry();
        entry.shared.change(|f| {
            f.ready = true;
            f.write_pending = true;
            f.pending = 1;
            f.accepted_bytes = CAP;
        });
        let mut stream = Stream {
            entry: entry.clone(),
        };
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            Pin::new(&mut stream)
                .poll_write(&mut cx, &[9; 3])
                .is_pending()
        );
        assert_eq!(lock(&entry.shared.0).accepted_bytes, CAP);
        entry.shared.change(|f| {
            f.write_pending = false;
            f.pending = 0;
            f.completed_bytes = CAP;
        });
        assert!(matches!(
            Pin::new(&mut stream).poll_write(&mut cx, &[8; 3]),
            Poll::Ready(Ok(3))
        ));
        assert_eq!(lock(&entry.shared.0).accepted_bytes, CAP + 3);
        drop(stream);
        let mut owner = Supervisor {
            entries: vec![entry],
            stopped: false,
        };
        let joined = tokio::time::timeout(Duration::from_secs(1), owner.shutdown())
            .await
            .unwrap();
        assert_eq!((joined[0].captures, joined[0].pending), (0, 0));
    }
    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_waits_for_pending_completion_and_capture_destruction() {
        let entry = entry();
        let hold = Hold::new(&entry.shared);
        entry.shared.change(|f| {
            f.pending = 1;
            f.read_pending = true;
        });
        let mut owner = Supervisor {
            entries: vec![entry.clone()],
            stopped: false,
        };
        let mut shutdown = Box::pin(owner.shutdown());
        assert!(
            shutdown
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let finish = entry.clone();
        entry.queue.submit(move || {
            finish.shared.change(|f| {
                f.pending = 0;
                f.read_pending = false;
            });
            finish.finalize();
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            shutdown
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(hold);
        let joined = tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .unwrap();
        assert_eq!((joined[0].captures, joined[0].pending), (0, 0));
    }
    #[test]
    fn waking_cannot_reenter_mutex_while_it_is_locked() {
        struct Check(Arc<Shared>);
        impl std::task::Wake for Check {
            fn wake(self: Arc<Self>) {
                assert!(self.0.0.try_lock().is_ok());
            }
        }
        let shared = Arc::<Shared>::default();
        lock(&shared.0).read_waker = Some(Waker::from(Arc::new(Check(shared.clone()))));
        shared.change(|f| f.ready = true);
    }
    #[test]
    fn public_transport_and_connect_future_have_required_owned_send_traits() {
        fn send<T: Send + Unpin>() {}
        send::<Stream>();
        send::<Connect>();
        send::<Supervisor>();
    }
}
