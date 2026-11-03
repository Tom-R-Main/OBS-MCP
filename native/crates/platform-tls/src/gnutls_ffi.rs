//! Installed GnuTLS3.8.12 ABI, checked separately against the actual C header.
use std::ffi::{c_char, c_int, c_uint, c_void};

pub type Session = *mut c_void;
pub type Credentials = *mut c_void;
#[repr(C)]
pub struct Datum {
    pub data: *mut u8,
    pub size: c_uint,
}
#[repr(C)]
pub struct VerifyData {
    pub kind: c_int,
    pub data: *mut u8,
    pub size: c_uint,
}
pub const AGAIN: isize = -28;
pub const INTERRUPTED: isize = -52;
pub const NO_DATA: c_int = -56;
pub const VERIFY_ERROR: c_int = -348;
pub const CLIENT: c_uint = 1 << 1;
pub const NONBLOCK: c_uint = 1 << 3;
pub const NO_SIGNAL: c_uint = 1 << 6;
pub const NO_TICKETS: c_uint = 1 << 10;
unsafe extern "C" {
    pub fn gnutls_global_init() -> c_int;
    pub fn gnutls_global_deinit();
    pub fn gnutls_init(session: *mut Session, flags: c_uint) -> c_int;
    pub fn gnutls_deinit(session: Session);
    pub fn gnutls_certificate_allocate_credentials(creds: *mut Credentials) -> c_int;
    pub fn gnutls_certificate_free_credentials(creds: Credentials);
    pub fn gnutls_certificate_set_x509_system_trust(creds: Credentials) -> c_int;
    pub fn gnutls_certificate_set_x509_trust_mem(
        creds: Credentials,
        ca: *const Datum,
        format: c_int,
    ) -> c_int;
    pub fn gnutls_credentials_set(session: Session, kind: c_int, cred: *mut c_void) -> c_int;
    pub fn gnutls_priority_set_direct(
        session: Session,
        priority: *const c_char,
        error: *mut *const c_char,
    ) -> c_int;
    pub fn gnutls_server_name_set(
        session: Session,
        kind: c_int,
        data: *const c_void,
        length: usize,
    ) -> c_int;
    pub fn gnutls_session_set_verify_cert2(
        session: Session,
        data: *mut VerifyData,
        elements: c_uint,
        flags: c_uint,
    );
    pub fn gnutls_session_get_verify_cert_status(session: Session) -> c_uint;
    pub fn gnutls_transport_set_int2(session: Session, read: c_int, write: c_int);
    pub fn gnutls_alpn_set_protocols(
        session: Session,
        protocols: *const Datum,
        count: c_uint,
        flags: c_uint,
    ) -> c_int;
    pub fn gnutls_alpn_get_selected_protocol(session: Session, protocol: *mut Datum) -> c_int;
    pub fn gnutls_protocol_get_version(session: Session) -> c_int;
    pub fn gnutls_handshake(session: Session) -> c_int;
    pub fn gnutls_record_get_direction(session: Session) -> c_int;
    pub fn gnutls_record_send(session: Session, data: *const c_void, length: usize) -> isize;
    pub fn gnutls_record_recv(session: Session, data: *mut c_void, length: usize) -> isize;
    pub fn gnutls_strerror(error: c_int) -> *const c_char;
    pub fn gnutls_check_version(required: *const c_char) -> *const c_char;
}
