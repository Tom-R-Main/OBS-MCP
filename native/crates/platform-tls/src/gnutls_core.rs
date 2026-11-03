//! Isolated step-oriented experiment. This is not a production TLS backend.
#![deny(unsafe_op_in_unsafe_fn)]
use super::gnutls_ffi as ffi;

use std::{
    ffi::{CStr, CString},
    io,
    marker::PhantomData,
    net::{IpAddr, TcpStream},
    os::fd::{AsRawFd, RawFd},
    ptr,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

pub const CAPACITY: usize = 65_536;

#[derive(Clone, Default)]
pub struct Monitor(Arc<Counters>);
#[derive(Default)]
struct Counters {
    global: AtomicUsize,
    credentials: AtomicUsize,
    session: AtomicUsize,
    handshake_calls: AtomicUsize,
    send_calls: AtomicUsize,
    send_pending: AtomicUsize,
    read_calls: AtomicUsize,
    partial_sends: AtomicUsize,
    retry_mismatches: AtomicUsize,
}
impl Monitor {
    pub fn snapshot(&self) -> [usize; 9] {
        let c = &self.0;
        [
            &c.global,
            &c.credentials,
            &c.session,
            &c.handshake_calls,
            &c.send_calls,
            &c.send_pending,
            &c.read_calls,
            &c.partial_sends,
            &c.retry_mismatches,
        ]
        .map(|v| v.load(Ordering::SeqCst))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Read,
    Write,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step<T> {
    Ready(T),
    Pending(Direction),
    Progress,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    Tls12,
    Tls13,
    Supported,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Handshaking,
    Ready,
    Terminal,
}

struct Global(Monitor);
impl Global {
    fn new(monitor: Monitor) -> io::Result<Self> {
        // Raw IP typed verification is since3.6.0; installed NEWS identifies
        //3.6.4 as the final RFC8446 version numbering. This is an API/protocol
        // floor, not an assertion that old releases are security-supported.
        if unsafe { ffi::gnutls_check_version(c"3.6.4".as_ptr()) }.is_null() {
            return Err(io::Error::other(
                "GnuTLS API/protocol version is unsupported",
            ));
        }
        // SAFETY: documented thread-safe/refcounted initializer; balances only this reference.
        checked(unsafe { ffi::gnutls_global_init() })?;
        monitor.0.global.fetch_add(1, Ordering::SeqCst);
        Ok(Self(monitor))
    }
}
impl Drop for Global {
    fn drop(&mut self) {
        // SAFETY: this guard is the final field, after session, credentials and socket.
        unsafe { ffi::gnutls_global_deinit() };
        self.0.0.global.fetch_sub(1, Ordering::SeqCst);
    }
}

struct SendBuffer {
    bytes: Box<[u8]>,
    offset: usize,
    retry: Option<(usize, usize)>,
}
enum Trust<'a> {
    System(Option<&'a crate::AdditionalRoots>),
    #[cfg(feature = "gnutls-synthetic-probe")]
    Fixture(&'a [u8]),
}
impl SendBuffer {
    fn remaining(&self) -> &[u8] {
        &self.bytes[self.offset..]
    }
    fn advance(&mut self, n: usize) -> io::Result<bool> {
        if n == 0 || n > self.remaining().len() {
            return Err(io::Error::other("invalid native send progress"));
        }
        self.offset += n;
        Ok(self.offset == self.bytes.len())
    }
}

/// One owning thread, no callbacks, no background tasks and no concurrent C calls.
/// Deliberately !Send/!Sync until migration between threads is separately qualified.
pub struct Stream {
    session: ffi::Session,
    credentials: ffi::Credentials,
    expected: CString,
    ip: Option<Box<[u8]>>,
    verification: Box<[ffi::VerifyData; 2]>,
    alpn_offer: Box<ffi::Datum>,
    send: Option<SendBuffer>,
    receive: Box<[u8]>,
    socket: TcpStream,
    deadline: Option<Instant>,
    cancel: Arc<AtomicBool>,
    state: State,
    requested: Version,
    monitor: Monitor,
    _single_owner: PhantomData<Rc<()>>,
    _global: Global,
    additional_roots: Option<crate::AdditionalRoots>,
}

impl Stream {
    pub fn system_trust(
        socket: TcpStream,
        expected: &str,
        version: Version,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        monitor: Monitor,
        additional_roots: Option<&crate::AdditionalRoots>,
    ) -> io::Result<Self> {
        Self::new_inner(
            socket,
            expected,
            version,
            deadline,
            cancel,
            monitor,
            Trust::System(additional_roots),
        )
    }

    #[cfg(feature = "gnutls-synthetic-probe")]
    pub fn synthetic(
        socket: TcpStream,
        expected: &str,
        version: Version,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        monitor: Monitor,
        anchor: &[u8],
    ) -> io::Result<Self> {
        Self::new_inner(
            socket,
            expected,
            version,
            deadline,
            cancel,
            monitor,
            Trust::Fixture(anchor),
        )
    }

    fn new_inner(
        socket: TcpStream,
        expected: &str,
        version: Version,
        deadline: Instant,
        cancel: Arc<AtomicBool>,
        monitor: Monitor,
        trust: Trust<'_>,
    ) -> io::Result<Self> {
        if expected.is_empty() || expected.len() > 253 || !expected.is_ascii() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "explicit ASCII DNS/IP peer required",
            ));
        }
        let expected = CString::new(expected)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in peer"))?;
        check_permission(deadline, &cancel)?;
        socket.set_nonblocking(true)?;
        let global = Global::new(monitor.clone())?;
        let ip = expected
            .to_str()
            .unwrap()
            .parse::<IpAddr>()
            .ok()
            .map(|ip| match ip {
                IpAddr::V4(v) => v.octets().to_vec().into_boxed_slice(),
                IpAddr::V6(v) => v.octets().to_vec().into_boxed_slice(),
            });
        let mut this = Self {
            session: ptr::null_mut(),
            credentials: ptr::null_mut(),
            expected,
            ip,
            verification: Box::new([
                ffi::VerifyData {
                    kind: 0,
                    data: ptr::null_mut(),
                    size: 0,
                },
                ffi::VerifyData {
                    kind: 2,
                    data: c"1.3.6.1.5.5.7.3.1".as_ptr().cast_mut().cast(),
                    size: 0,
                },
            ]),
            alpn_offer: Box::new(ffi::Datum {
                data: b"http/1.1".as_ptr().cast_mut(),
                size: 8,
            }),
            send: None,
            receive: vec![0; CAPACITY].into_boxed_slice(),
            socket,
            deadline: Some(deadline),
            cancel,
            state: State::Handshaking,
            requested: version,
            monitor,
            _single_owner: PhantomData,
            _global: global,
            additional_roots: None,
        };
        // SAFETY: exclusive out-pointer, initialized to null and owned by this RAII object.
        checked(unsafe { ffi::gnutls_certificate_allocate_credentials(&mut this.credentials) })?;
        this.monitor.0.credentials.fetch_add(1, Ordering::SeqCst);
        let (loaded, additional_roots) = match trust {
            Trust::System(roots) => {
                // SAFETY: valid exclusively owned credentials; no global trust mutation.
                (
                    unsafe { ffi::gnutls_certificate_set_x509_system_trust(this.credentials) },
                    roots,
                )
            }
            #[cfg(feature = "gnutls-synthetic-probe")]
            Trust::Fixture(anchor) => {
                let size = u32::try_from(anchor.len())
                    .map_err(|_| io::Error::other("anchor too large"))?;
                let datum = ffi::Datum {
                    data: anchor.as_ptr().cast_mut(),
                    size,
                };
                // SAFETY: GnuTLS parses/copies the PEM datum before returning; credentials own parsed CAs.
                (
                    unsafe {
                        ffi::gnutls_certificate_set_x509_trust_mem(this.credentials, &datum, 1)
                    },
                    None,
                )
            }
        };
        if loaded < 0 {
            return Err(native_error(loaded, 0));
        }
        if let Some(roots) = additional_roots {
            this = this.with_additional_roots(roots)?;
        }
        if loaded == 0 && this.additional_roots.is_none() {
            return Err(io::Error::other("no trust anchors loaded"));
        }
        // SAFETY: exclusive out-pointer, nonblocking client; no early data, auto-reauth or resumption API.
        checked(unsafe {
            ffi::gnutls_init(
                &mut this.session,
                ffi::CLIENT | ffi::NONBLOCK | ffi::NO_SIGNAL | ffi::NO_TICKETS,
            )
        })?;
        this.monitor.0.session.fetch_add(1, Ordering::SeqCst);
        let priority = match version {
            Version::Tls12 => c"NORMAL:-VERS-ALL:+VERS-TLS1.2",
            Version::Tls13 => c"NORMAL:-VERS-ALL:+VERS-TLS1.3",
            Version::Supported => c"NORMAL:-VERS-ALL:+VERS-TLS1.3:+VERS-TLS1.2",
        };
        // SAFETY: static priority storage and valid session/credentials, return values checked.
        checked(unsafe {
            ffi::gnutls_priority_set_direct(this.session, priority.as_ptr(), ptr::null_mut())
        })?;
        checked(unsafe { ffi::gnutls_credentials_set(this.session, 1, this.credentials) })?;
        if let Some(ip) = &mut this.ip {
            this.verification[0] = ffi::VerifyData {
                kind: 4,
                data: ip.as_mut_ptr(),
                size: ip.len() as u32,
            };
        } else {
            this.verification[0] = ffi::VerifyData {
                kind: 1,
                data: this.expected.as_ptr().cast_mut().cast(),
                size: 0,
            };
            // SAFETY: name is an owned CString; SNI uses DNS only and excludes NUL.
            checked(unsafe {
                ffi::gnutls_server_name_set(
                    this.session,
                    1,
                    this.expected.as_ptr().cast(),
                    this.expected.as_bytes().len(),
                )
            })?;
        }
        // SAFETY: boxed verification array, CString/IP backing and static OID outlive session deinit.
        unsafe {
            ffi::gnutls_session_set_verify_cert2(this.session, this.verification.as_mut_ptr(), 2, 0)
        };
        // SAFETY: boxed datum and static byte backing both outlive session deinit.
        checked(unsafe { ffi::gnutls_alpn_set_protocols(this.session, &*this.alpn_offer, 1, 0) })?;
        let fd = this.socket.as_raw_fd();
        // SAFETY: owned socket remains open until after session deinit. Actual function, not C macro.
        unsafe { ffi::gnutls_transport_set_int2(this.session, fd, fd) };
        this.guard()?;
        Ok(this)
    }

    pub fn with_additional_roots(mut self, roots: &crate::AdditionalRoots) -> io::Result<Self> {
        for der in roots.certificates() {
            let datum = ffi::Datum {
                data: der.as_ptr().cast_mut(),
                size: der.len() as u32,
            };
            // SAFETY: exclusively owned credentials before handshake; native DER
            // parser copies certificates. Adds to already loaded system anchors.
            let loaded =
                unsafe { ffi::gnutls_certificate_set_x509_trust_mem(self.credentials, &datum, 0) };
            if loaded <= 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid additional TLS certificate anchors",
                ));
            }
        }
        self.additional_roots = Some(roots.clone());
        Ok(self)
    }
    pub fn fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }
    pub fn handshake(&mut self) -> io::Result<Step<()>> {
        self.guard()?;
        if self.state != State::Handshaking {
            return self.fail(io::Error::other("handshake cannot restart"));
        }
        self.monitor
            .0
            .handshake_calls
            .fetch_add(1, Ordering::SeqCst);
        // SAFETY: single owned initialized nonblocking session; no application operations admitted yet.
        let result = unsafe { ffi::gnutls_handshake(self.session) };
        if result == 0 {
            // SAFETY: read-only queries on completed session; returned ALPN is copied before any mutation.
            let verified = unsafe { ffi::gnutls_session_get_verify_cert_status(self.session) };
            if verified != 0 {
                return self.fail(native_error(ffi::VERIFY_ERROR, verified));
            }
            let version = self.version();
            if !matches!(
                (self.requested, version),
                (Version::Supported, 4 | 5) | (Version::Tls12, 4) | (Version::Tls13, 5)
            ) {
                return self.fail(io::Error::other("unexpected TLS version"));
            }
            let alpn = self.alpn()?;
            if alpn.as_deref().is_some_and(|v| v != b"http/1.1") {
                return self.fail(io::Error::other("incompatible ALPN"));
            }
            self.state = State::Ready;
            self.deadline = None;
            Ok(Step::Ready(()))
        } else {
            self.result(result as isize)
        }
    }
    pub fn version(&self) -> i32 {
        if self.session.is_null() {
            return 0;
        }
        // SAFETY: valid owned session, read-only query.
        unsafe { ffi::gnutls_protocol_get_version(self.session) }
    }
    pub fn alpn(&mut self) -> io::Result<Option<Vec<u8>>> {
        if self.session.is_null() {
            return Err(io::Error::other("closed session"));
        }
        let mut datum = ffi::Datum {
            data: ptr::null_mut(),
            size: 0,
        };
        // SAFETY: valid out datum, native-owned data copied before returning to caller.
        let result = unsafe { ffi::gnutls_alpn_get_selected_protocol(self.session, &mut datum) };
        if result == ffi::NO_DATA {
            return Ok(None);
        }
        checked(result)?;
        if datum.data.is_null() || datum.size > 255 {
            return self.fail(io::Error::other("invalid ALPN datum"));
        }
        // SAFETY: successful getter guarantees native datum extent for the live session.
        Ok(Some(
            unsafe { std::slice::from_raw_parts(datum.data, datum.size as usize) }.to_vec(),
        ))
    }
    /// Accept owned data, independent of any caller's future/poll buffer lifetime.
    pub fn enqueue(&mut self, input: &[u8]) -> io::Result<usize> {
        self.ready()?;
        if self.send.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "owned write still pending",
            ));
        }
        let n = input.len().min(CAPACITY);
        if n != 0 {
            self.send = Some(SendBuffer {
                bytes: input[..n].into(),
                offset: 0,
                retry: None,
            });
        }
        Ok(n)
    }
    pub fn flush_step(&mut self) -> io::Result<Step<()>> {
        self.ready()?;
        let Some(send) = &self.send else {
            return Ok(Step::Ready(()));
        };
        let key = (send.remaining().as_ptr() as usize, send.remaining().len());
        if send.retry.is_some_and(|prior| prior != key) {
            self.monitor
                .0
                .retry_mismatches
                .fetch_add(1, Ordering::SeqCst);
            return self.fail(io::Error::other(
                "native send continuation changed backing data",
            ));
        }
        self.monitor.0.send_calls.fetch_add(1, Ordering::SeqCst);
        // SAFETY: owned boxed bytes remain at identical pointer/length after AGAIN/INTERRUPTED.
        // A positive result alone advances the pointer. Teardown deinitializes session before freeing it.
        let n = unsafe {
            ffi::gnutls_record_send(
                self.session,
                send.remaining().as_ptr().cast(),
                send.remaining().len(),
            )
        };
        if n > 0 {
            if (n as usize) < key.1 {
                self.monitor.0.partial_sends.fetch_add(1, Ordering::SeqCst);
            }
            self.send.as_mut().unwrap().retry = None;
            if self.send.as_mut().unwrap().advance(n as usize)? {
                self.send = None;
            }
            Ok(if self.send.is_none() {
                Step::Ready(())
            } else {
                Step::Progress
            })
        } else {
            if matches!(n, ffi::AGAIN | ffi::INTERRUPTED) {
                self.send.as_mut().unwrap().retry = Some(key);
                self.monitor.0.send_pending.fetch_add(1, Ordering::SeqCst);
            }
            self.result(n)
        }
    }
    pub fn read_step(&mut self) -> io::Result<Step<Vec<u8>>> {
        self.ready()?;
        self.monitor.0.read_calls.fetch_add(1, Ordering::SeqCst);
        // SAFETY: writable owned bounded allocation; no borrowed ReadBuf is retained across a step.
        let n = unsafe {
            ffi::gnutls_record_recv(
                self.session,
                self.receive.as_mut_ptr().cast(),
                self.receive.len(),
            )
        };
        if n == 0 {
            self.state = State::Terminal;
            Ok(Step::Ready(Vec::new()))
        } else if n > 0 {
            Ok(Step::Ready(self.receive[..n as usize].to_vec()))
        } else {
            let step = self.result(n)?;
            if n == ffi::AGAIN && matches!(step, Step::Pending(Direction::Read)) {
                // TLS 1.3 ticket processing can return AGAIN without exhausting
                // the socket. Clearing edge-triggered readiness then loses the
                // already-readable application record. Peek without consuming
                // bytes; only an actual WouldBlock permits waiting for an edge.
                match self.socket.peek(&mut [0; 1]) {
                    Ok(_) => return Ok(Step::Progress),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                        return Ok(Step::Progress);
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => return self.fail(error),
                }
            }
            Ok(step)
        }
    }
    pub fn has_pending_send(&self) -> bool {
        self.send.is_some()
    }
    fn ready(&mut self) -> io::Result<()> {
        self.guard()?;
        if self.state != State::Ready {
            return self.fail(io::Error::other(
                "application bytes require verified ready session",
            ));
        }
        Ok(())
    }
    fn guard(&mut self) -> io::Result<()> {
        if self.state == State::Terminal {
            return Err(io::Error::other("terminal session"));
        }
        if let Some(deadline) = self.deadline {
            if let Err(error) = check_permission(deadline, &self.cancel) {
                return self.fail(error);
            }
        } else if self.cancel.load(Ordering::SeqCst) {
            return self.fail(io::Error::new(
                io::ErrorKind::Interrupted,
                "operation retired",
            ));
        }
        Ok(())
    }
    fn result<T>(&mut self, result: isize) -> io::Result<Step<T>> {
        if matches!(result, ffi::AGAIN | ffi::INTERRUPTED) {
            // SAFETY: query immediately after the same session's pending operation.
            let direction = unsafe { ffi::gnutls_record_get_direction(self.session) };
            Ok(Step::Pending(if direction == 0 {
                Direction::Read
            } else {
                Direction::Write
            }))
        } else {
            // SAFETY: error status query while session remains live; captured before fail deinitializes it.
            let status = unsafe { ffi::gnutls_session_get_verify_cert_status(self.session) };
            self.fail(native_error(result as i32, status))
        }
    }
    fn fail<T>(&mut self, error: io::Error) -> io::Result<T> {
        // Retire permission immediately; the wrapper retains this owner for
        // the joined cleanup job rather than destroying native allocations here.
        self.state = State::Terminal;
        Err(error)
    }
    pub fn close(&mut self) {
        self.state = State::Terminal;
        if !self.session.is_null() {
            // SAFETY: exclusive live session; synchronous destruction clears its internal buffers before our backing allocations.
            unsafe { ffi::gnutls_deinit(self.session) };
            self.session = ptr::null_mut();
            self.monitor.0.session.fetch_sub(1, Ordering::SeqCst);
        }
        if !self.credentials.is_null() {
            // SAFETY: no session still borrows these credentials.
            unsafe { ffi::gnutls_certificate_free_credentials(self.credentials) };
            self.credentials = ptr::null_mut();
            self.monitor.0.credentials.fetch_sub(1, Ordering::SeqCst);
        }
        self.send = None;
        // TcpStream remains owned until Drop, including during all native destruction calls.
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        self.close();
    }
}
fn check_permission(deadline: Instant, cancel: &AtomicBool) -> io::Result<()> {
    if cancel.load(Ordering::SeqCst) {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "original operation cancelled",
        ))
    } else if Instant::now() >= deadline {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "original deadline expired",
        ))
    } else {
        Ok(())
    }
}
fn checked(result: i32) -> io::Result<()> {
    if result < 0 {
        Err(native_error(result, 0))
    } else {
        Ok(())
    }
}
fn native_error(code: i32, status: u32) -> io::Error {
    // SAFETY: documented static native error string, never freed or retained as borrowed storage.
    let text = unsafe { CStr::from_ptr(ffi::gnutls_strerror(code)) }.to_string_lossy();
    io::Error::other(format!("gnutls:{code}; verify:{status}; {text}"))
}
#[allow(dead_code)]
pub fn runtime_version() -> String {
    // SAFETY: null asks for static library version, copied into Rust-owned text.
    unsafe { CStr::from_ptr(ffi::gnutls_check_version(ptr::null())) }
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn send_continuation_preserves_identity_and_only_advances_positive_progress() {
        let mut send = SendBuffer {
            bytes: vec![7; 100].into(),
            offset: 0,
            retry: None,
        };
        let original = (send.remaining().as_ptr(), send.remaining().len());
        // Pending continuations make no state change; caller data is already independent.
        assert_eq!(
            (send.remaining().as_ptr(), send.remaining().len()),
            original
        );
        assert!(!send.advance(17).unwrap());
        assert_eq!(send.remaining().as_ptr(), original.0.wrapping_add(17));
        assert_eq!(send.remaining().len(), 83);
        assert!(send.advance(83).unwrap());
    }
    #[test]
    fn cancellation_and_original_deadline_are_not_extended() {
        assert_eq!(
            check_permission(Instant::now(), &AtomicBool::new(false))
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            check_permission(
                Instant::now() + std::time::Duration::from_secs(1),
                &AtomicBool::new(true)
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::Interrupted
        );
    }
    #[test]
    fn invalid_send_progress_is_refused() {
        let mut send = SendBuffer {
            bytes: vec![1; 4].into(),
            offset: 0,
            retry: None,
        };
        assert!(send.advance(0).is_err());
        assert!(send.advance(5).is_err());
        assert_eq!(send.offset, 0);
    }
    #[test]
    fn rust_layout_matches_64bit_c_header_assertions() {
        assert_eq!(size_of::<ffi::Datum>(), 16);
        assert_eq!(size_of::<ffi::VerifyData>(), 24);
        assert_eq!(std::mem::offset_of!(ffi::VerifyData, data), 8);
    }
    #[test]
    fn terminal_permission_keeps_native_resources_for_retained_cleanup_owner() {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let socket = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (_peer, _) = listener.accept().unwrap();
        let monitor = Monitor::default();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut native = Stream::system_trust(
            socket,
            "localhost",
            Version::Supported,
            Instant::now() + std::time::Duration::from_secs(3),
            cancel.clone(),
            monitor.clone(),
            None,
        )
        .unwrap();
        assert_eq!(&monitor.snapshot()[..3], &[1, 1, 1]);
        cancel.store(true, Ordering::SeqCst);
        assert!(native.handshake().is_err());
        // Permission is gone, but native allocations remain with the owner
        // which the wrapper transfers into its joined cleanup job.
        assert_eq!(&monitor.snapshot()[..3], &[1, 1, 1]);
        drop(native);
        assert_eq!(&monitor.snapshot()[..3], &[0, 0, 0]);
    }
}
