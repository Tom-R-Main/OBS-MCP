//! Narrow ABI declarations, manually checked against the installed macOS SDK.
use block2::DynBlock;
use std::ffi::{c_char, c_void};
pub type Raw = *mut c_void;
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct CBool(pub bool);
// SAFETY: This is C99 _Bool (SDK bool), not Objective-C's platform-specific BOOL.
// The transparent wrapper keeps Rust bool's C ABI and gives Blocks its B code.
unsafe impl objc2::Encode for CBool {
    const ENCODING: objc2::encode::Encoding = objc2::encode::Encoding::Bool;
}
pub type Configure = DynBlock<dyn Fn(Raw)>;
pub type State = DynBlock<dyn Fn(i32, Raw)>;
pub type Receive = DynBlock<dyn Fn(Raw, Raw, CBool, Raw)>;
pub type SendCompletion = DynBlock<dyn Fn(Raw)>;
#[link(name = "Network", kind = "framework")]
unsafe extern "C" {
    pub fn nw_release(object: Raw);
    pub fn nw_endpoint_create_host(host: *const c_char, port: *const c_char) -> Raw;
    pub fn nw_parameters_create_secure_tcp(tls: &Configure, tcp: &Configure) -> Raw;
    pub fn nw_parameters_set_fast_open_enabled(parameters: Raw, enabled: bool);
    pub fn nw_parameters_set_multipath_service(parameters: Raw, service: i32);
    pub fn nw_parameters_set_prefer_no_proxy(parameters: Raw, prefer: bool);
    pub fn nw_privacy_context_create(description: *const c_char) -> Raw;
    pub fn nw_privacy_context_disable_logging(context: Raw);
    pub fn nw_privacy_context_flush_cache(context: Raw);
    pub fn nw_parameters_set_privacy_context(parameters: Raw, context: Raw);
    pub fn nw_tls_copy_sec_protocol_options(options: Raw) -> Raw;
    pub fn nw_protocol_copy_tls_definition() -> Raw;
    pub fn nw_connection_copy_protocol_metadata(connection: Raw, definition: Raw) -> Raw;
    pub fn nw_tls_copy_sec_protocol_metadata(metadata: Raw) -> Raw;
    pub fn nw_tcp_options_set_no_delay(options: Raw, no_delay: bool);
    pub fn nw_tcp_options_set_enable_fast_open(options: Raw, enabled: bool);
    pub fn nw_tcp_options_set_connection_timeout(options: Raw, seconds: u32);
    pub fn nw_connection_create(endpoint: Raw, parameters: Raw) -> Raw;
    pub fn nw_connection_set_queue(connection: Raw, queue: Raw);
    pub fn nw_connection_set_state_changed_handler(connection: Raw, handler: Option<&State>);
    pub fn nw_connection_start(connection: Raw);
    pub fn nw_connection_force_cancel(connection: Raw);
    pub fn nw_connection_send(
        connection: Raw,
        content: Raw,
        context: Raw,
        complete: bool,
        callback: &SendCompletion,
    );
    pub fn nw_connection_receive(connection: Raw, minimum: u32, maximum: u32, callback: &Receive);
    pub fn nw_content_context_get_is_final(context: Raw) -> bool;
    pub fn nw_error_get_error_domain(error: Raw) -> i32;
    pub fn nw_error_get_error_code(error: Raw) -> i32;
    pub static _nw_content_context_default_message: Raw;
    #[cfg(feature = "plain-fixture")]
    pub static _nw_parameters_configure_protocol_disable: *const Configure;
}
#[link(name = "Security", kind = "framework")]
unsafe extern "C" {
    pub fn sec_release(object: Raw);
    pub fn sec_protocol_metadata_get_negotiated_tls_protocol_version(metadata: Raw) -> u16;
    pub fn sec_protocol_metadata_get_negotiated_protocol(metadata: Raw) -> *const c_char;
    pub fn sec_protocol_options_set_min_tls_protocol_version(options: Raw, version: u16);
    pub fn sec_protocol_options_set_max_tls_protocol_version(options: Raw, version: u16);
    pub fn sec_protocol_options_set_tls_server_name(options: Raw, name: *const c_char);
    pub fn sec_protocol_options_add_tls_application_protocol(options: Raw, protocol: *const c_char);
    pub fn sec_protocol_options_set_tls_tickets_enabled(options: Raw, enabled: bool);
    pub fn sec_protocol_options_set_tls_resumption_enabled(options: Raw, enabled: bool);
    pub fn sec_protocol_options_set_tls_false_start_enabled(options: Raw, enabled: bool);
    pub fn sec_protocol_options_set_peer_authentication_required(options: Raw, required: bool);
}
unsafe extern "C" {
    pub fn dispatch_queue_attr_make_with_autorelease_frequency(
        attributes: Raw,
        frequency: usize,
    ) -> Raw;
    pub fn dispatch_queue_create(label: *const c_char, attributes: Raw) -> Raw;
    pub fn dispatch_async(queue: Raw, callback: &DynBlock<dyn Fn()>);
    pub fn dispatch_release(object: Raw);
    pub fn dispatch_data_create(
        bytes: *const c_void,
        length: usize,
        queue: Raw,
        destructor: Option<&DynBlock<dyn Fn()>>,
    ) -> Raw;
    pub fn dispatch_data_get_size(data: Raw) -> usize;
    pub fn dispatch_data_create_map(
        data: Raw,
        bytes: *mut *const c_void,
        length: *mut usize,
    ) -> Raw;
}

pub type Cf = *const c_void;
pub type Verify = DynBlock<dyn Fn(Raw, Raw, *const DynBlock<dyn Fn(CBool)>)>;
#[repr(C)]
pub struct ArrayCallbacks {
    pub version: isize,
    pub retain: *const c_void,
    pub release: *const c_void,
    pub copy_description: *const c_void,
    pub equal: *const c_void,
}
#[link(name = "Security", kind = "framework")]
unsafe extern "C" {
    pub fn sec_trust_copy_ref(trust: Raw) -> Cf;
    pub fn sec_protocol_options_set_verify_block(options: Raw, callback: &Verify, queue: Raw);
    pub fn SecCertificateCreateWithData(allocator: Cf, data: Cf) -> Cf;
    pub fn SecTrustSetAnchorCertificates(trust: Cf, anchors: Cf) -> i32;
    pub fn SecTrustSetAnchorCertificatesOnly(trust: Cf, only: u8) -> i32;
    pub fn SecTrustSetNetworkFetchAllowed(trust: Cf, allowed: u8) -> i32;
    pub fn SecTrustEvaluateWithError(trust: Cf, error: *mut Cf) -> bool;
}
#[link(name = "CoreFoundation", kind = "framework")]
unsafe extern "C" {
    pub static kCFTypeArrayCallBacks: ArrayCallbacks;
    pub fn CFDataCreate(allocator: Cf, bytes: *const u8, length: isize) -> Cf;
    pub fn CFArrayCreate(
        allocator: Cf,
        values: *const Cf,
        count: isize,
        callbacks: *const ArrayCallbacks,
    ) -> Cf;
    pub fn CFRelease(object: Cf);
}
