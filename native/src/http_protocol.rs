// SPDX-License-Identifier: GPL-2.0-only
//! Pure, bounded HTTP exchange admission. HTTP parsing and connection ownership
//! belong to a mature transport; this module never reads a socket or calls OBS.
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

pub const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_HEADER_BYTES: usize = 16 * 1024;
pub const MAX_HEADERS: usize = 64;
pub const MAX_TARGET_BYTES: usize = 4096;
pub const MODERN: &str = "2026-07-28";
const VERSION: &str = "io.modelcontextprotocol/protocolVersion";
const INFO: &str = "io.modelcontextprotocol/clientInfo";
const CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
const LEGACY: [&str; 5] = [
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
    "2024-10-07",
];

/// Values have already passed the transport's HTTP syntax validation. Repeated
/// field values must be combined according to that transport's HTTP semantics.
pub struct RequestHead<'a> {
    pub method: &'a str,
    pub target: &'a str,
    pub headers: &'a [(&'a str, &'a str)],
}
impl RequestHead<'_> {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| ows(value))
    }
}
/// Secret bytes are intentionally neither Debug nor serializable.
pub struct Policy {
    token: Option<Vec<u8>>,
}
impl Policy {
    pub fn new(token: Option<&str>) -> Self {
        Self {
            token: token
                .filter(|token| !token.is_empty())
                .map(|token| token.as_bytes().to_vec()),
        }
    }
    fn authenticated(&self, authorization: Option<&str>) -> bool {
        let Some(expected) = self.token.as_deref() else {
            return true;
        };
        let presented = authorization
            .and_then(|header| header.strip_prefix("Bearer "))
            .unwrap_or("")
            .as_bytes();
        if presented.len() != expected.len() {
            return false;
        }
        // Equal-length inputs always examine every byte. There is no diagnostic
        // or serialization path for either the expected or presented token.
        presented
            .iter()
            .zip(expected)
            .fold(0_u8, |different, (a, b)| different | (a ^ b))
            == 0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Era {
    Modern,
    Legacy,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseMode {
    Json,
    Sse,
    Listen,
    Empty,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorOrigin {
    Handler,
    Protocol,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Response {
    pub status: u16,
    pub content_type: Option<&'static str>,
    pub authenticate_bearer: bool,
    pub body: Vec<u8>,
}
pub enum Admission {
    Reply(Response),
    Exchange(Exchange),
    Unqualified(Unqualified),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Unqualified {
    /// WHATWG URL normalization beyond canonical loopback authorities needs a
    /// qualified URL implementation; it must never become a permissive guess.
    UrlForm,
    EnvelopeSchema,
    JsonDepth,
    DuplicateHeader,
}
#[derive(Clone, Copy)]
pub struct ResponseContext {
    era: Era,
    mode: ResponseMode,
}
impl ResponseContext {
    pub fn response_mode(self) -> ResponseMode {
        self.mode
    }
    pub fn respond(self, payload: &Value, origin: ErrorOrigin) -> Response {
        response(self.era, self.mode, payload, origin)
    }
}
pub struct Exchange {
    original_body: Vec<u8>,
    message: Value,
    era: Era,
    mode: ResponseMode,
    declared_client: Option<String>,
}
impl Exchange {
    /// Split an already-qualified legacy batch without reserializing objects.
    /// Serde consumes each original JSON span; no HTTP/JSON framing is guessed.
    pub fn into_items(self) -> Result<Vec<Exchange>, Unqualified> {
        let Exchange {
            original_body,
            message,
            era,
            mode,
            declared_client,
        } = self;
        let Value::Array(items) = message else {
            return Ok(vec![Exchange {
                original_body,
                message,
                era,
                mode,
                declared_client,
            }]);
        };
        let mut at = 0;
        let skip = |at: &mut usize| {
            while original_body.get(*at).is_some_and(u8::is_ascii_whitespace) {
                *at += 1;
            }
        };
        skip(&mut at);
        if original_body.get(at) != Some(&b'[') {
            return Err(Unqualified::EnvelopeSchema);
        }
        at += 1;
        let mut exchanges = Vec::with_capacity(items.len());
        for (index, message) in items.into_iter().enumerate() {
            skip(&mut at);
            if index > 0 {
                if original_body.get(at) != Some(&b',') {
                    return Err(Unqualified::EnvelopeSchema);
                }
                at += 1;
                skip(&mut at);
            }
            let mut parser = serde_json::Deserializer::from_slice(&original_body[at..])
                .into_iter::<serde::de::IgnoredAny>();
            parser
                .next()
                .ok_or(Unqualified::EnvelopeSchema)?
                .map_err(|_| Unqualified::EnvelopeSchema)?;
            let end = at
                .checked_add(parser.byte_offset())
                .ok_or(Unqualified::EnvelopeSchema)?;
            let body = original_body
                .get(at..end)
                .ok_or(Unqualified::EnvelopeSchema)?
                .to_vec();
            at = end;
            let notification = message.get("method").is_some() && message.get("id").is_none();
            let mode = if notification
                || message.get("result").is_some()
                || message.get("error").is_some()
            {
                ResponseMode::Empty
            } else {
                ResponseMode::Sse
            };
            exchanges.push(Exchange {
                original_body: body,
                message,
                era,
                mode,
                declared_client: declared_client.clone(),
            });
        }
        skip(&mut at);
        if original_body.get(at) != Some(&b']') {
            return Err(Unqualified::EnvelopeSchema);
        }
        Ok(exchanges)
    }
    pub fn original_body(&self) -> &[u8] {
        &self.original_body
    }
    pub fn message(&self) -> &Value {
        &self.message
    }
    pub fn era(&self) -> Era {
        self.era
    }
    pub fn response_mode(&self) -> ResponseMode {
        self.mode
    }
    /// URL caller identity is a declaration, never an authenticated principal.
    pub fn legacy_declared_client(&self) -> Option<&str> {
        (self.era == Era::Legacy)
            .then_some(self.declared_client.as_deref())
            .flatten()
    }
    /// No prior initialize exchange is consulted. Modern capability authority
    /// comes exclusively from this body's envelope; legacy offers are absent.
    pub fn client_capabilities(&self) -> Option<&Value> {
        (self.era == Era::Modern)
            .then(|| meta(&self.message)?.get(CAPABILITIES))
            .flatten()
    }
    pub fn response_context(&self) -> ResponseContext {
        ResponseContext {
            era: self.era,
            mode: self.mode,
        }
    }
    pub fn respond(&self, payload: &Value, origin: ErrorOrigin) -> Response {
        self.response_context().respond(payload, origin)
    }
}
fn response(era: Era, mode: ResponseMode, payload: &Value, origin: ErrorOrigin) -> Response {
    if mode == ResponseMode::Empty {
        return Response::empty(202);
    }
    let code = payload.pointer("/error/code").and_then(Value::as_i64);
    let status = if era == Era::Modern {
        match (origin, code) {
            (_, Some(-32021)) => 400,
            (ErrorOrigin::Protocol, Some(-32601)) => 404,
            (ErrorOrigin::Protocol, Some(-32700 | -32600 | -32020 | -32022)) => 400,
            _ => 200,
        }
    } else {
        200
    };
    if mode == ResponseMode::Json {
        Response::json(status, payload)
    } else {
        Response::sse(status, payload)
    }
}
impl Response {
    fn empty(status: u16) -> Self {
        Self {
            status,
            content_type: None,
            authenticate_bearer: false,
            body: Vec::new(),
        }
    }
    fn text(status: u16, text: &str) -> Self {
        Self {
            status,
            content_type: Some("text/plain"),
            authenticate_bearer: false,
            body: text.as_bytes().to_vec(),
        }
    }
    fn json(status: u16, value: &Value) -> Self {
        Self {
            status,
            content_type: Some("application/json"),
            authenticate_bearer: false,
            body: serde_json::to_vec(value).expect("Value serialization"),
        }
    }
    /// SSE event encoding only. HTTP chunking, content length and connection
    /// framing remain exclusively with the selected HTTP transport.
    pub fn sse(status: u16, value: &Value) -> Self {
        let mut body = b"event: message\ndata: ".to_vec();
        body.extend(serde_json::to_vec(value).expect("Value serialization"));
        body.extend_from_slice(b"\n\n");
        Self {
            status,
            content_type: Some("text/event-stream"),
            authenticate_bearer: false,
            body,
        }
    }
}
fn error_response(
    status: u16,
    code: i64,
    message: impl Into<String>,
    id: Value,
    data: Option<Value>,
) -> Response {
    let mut value = json!({"jsonrpc":"2.0","error":{"code":code,"message":message.into()},"id":id});
    if let Some(data) = data {
        value["error"]["data"] = data;
    }
    Response::json(status, &value)
}
fn error(
    status: u16,
    code: i64,
    message: impl Into<String>,
    id: Value,
    data: Option<Value>,
) -> Admission {
    Admission::Reply(error_response(status, code, message, id, data))
}
fn mismatch(header: &str, body: String, id: Value) -> Admission {
    error(
        400,
        -32020,
        format!("Bad Request: the request headers and body disagree: {body}"),
        id,
        Some(json!({"mismatch":{"header":header,"body":body}})),
    )
}
fn ows(value: &str) -> &str {
    value.trim_matches([' ', '\t'])
}
fn meta(message: &Value) -> Option<&serde_json::Map<String, Value>> {
    message.get("params")?.get("_meta")?.as_object()
}
fn id(message: &Value) -> Value {
    message
        .get("id")
        .filter(|id| id.is_string() || id.is_number() || id.is_null())
        .cloned()
        .unwrap_or(Value::Null)
}
fn modern(version: &str) -> bool {
    version >= "2026-07-28"
}
fn canonical_hostname(authority: &str) -> Result<&str, Unqualified> {
    if authority.is_empty()
        || authority
            .bytes()
            .any(|b| b.is_ascii_control() || matches!(b, b'/' | b'?' | b'#' | b'@' | b'\\' | b'%'))
    {
        return Err(Unqualified::UrlForm);
    }
    let (host, port) = if authority.starts_with('[') {
        let end = authority.find(']').ok_or(Unqualified::UrlForm)? + 1;
        let rest = &authority[end..];
        (
            &authority[..end],
            if rest.is_empty() {
                None
            } else {
                Some(rest.strip_prefix(':').ok_or(Unqualified::UrlForm)?)
            },
        )
    } else {
        authority
            .split_once(':')
            .map_or((authority, None), |(host, port)| (host, Some(port)))
    };
    if let Some(port) = port
        && !port.is_empty()
        && port.parse::<u16>().is_err()
    {
        return Err(Unqualified::UrlForm);
    }
    if host.is_ascii() && !host.is_empty() {
        Ok(host)
    } else {
        Err(Unqualified::UrlForm)
    }
}
fn allowed(host: &str) -> bool {
    ["localhost", "127.0.0.1", "[::1]"]
        .iter()
        .any(|allowed| host.eq_ignore_ascii_case(allowed))
}
fn url_security(head: &RequestHead<'_>) -> Result<Option<Response>, Unqualified> {
    let Some(host) = head.header("host").filter(|host| !host.is_empty()) else {
        return Ok(Some(error_response(
            403,
            -32000,
            "Missing Host header",
            Value::Null,
            None,
        )));
    };
    let hostname = canonical_hostname(host)?;
    if !allowed(hostname) {
        return Ok(Some(error_response(
            403,
            -32000,
            format!("Invalid Host: {}", hostname.to_ascii_lowercase()),
            Value::Null,
            None,
        )));
    }
    if let Some(origin) = head.header("origin").filter(|origin| !origin.is_empty()) {
        let Some((scheme, rest)) = origin.split_once("://") else {
            return Ok(Some(error_response(
                403,
                -32000,
                format!("Invalid Origin header: {origin}"),
                Value::Null,
                None,
            )));
        };
        if scheme.is_empty()
            || !scheme
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
        {
            return Err(Unqualified::UrlForm);
        }
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        let hostname = canonical_hostname(authority)?;
        if !allowed(hostname) {
            return Ok(Some(error_response(
                403,
                -32000,
                format!("Invalid Origin: {}", hostname.to_ascii_lowercase()),
                Value::Null,
                None,
            )));
        }
    }
    Ok(None)
}
fn declared_client(target: &str) -> Option<String> {
    let query = target.split_once('?')?.1.split('#').next()?;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_form(key).as_deref() == Some("client") {
            let name = decode_form(value)?;
            let name = crate_trim(&name);
            return (!name.is_empty()
                && name.len() <= 64
                && name.bytes().all(|b| {
                    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'@' | b':' | b'-')
                }))
            .then(|| name.to_owned());
        }
    }
    None
}
fn crate_trim(value: &str) -> &str {
    value.trim_matches(|ch| matches!(ch,'\u{0009}'..='\u{000d}'|'\u{0020}'|'\u{00a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}'))
}
fn decode_form(value: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut iter = value.as_bytes().iter().copied();
    while let Some(byte) = iter.next() {
        bytes.push(match byte {
            b'+' => b' ',
            b'%' => {
                let a = (iter.next()? as char).to_digit(16)?;
                let b = (iter.next()? as char).to_digit(16)?;
                (a * 16 + b) as u8
            }
            byte => byte,
        });
    }
    String::from_utf8(bytes).ok()
}
fn message_shape(message: &Value) -> bool {
    if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return false;
    }
    if message.get("method").is_some_and(Value::is_string) {
        return message
            .get("id")
            .is_none_or(|id| id.is_string() || id.as_i64().is_some() || id.as_u64().is_some())
            && message.get("params").is_none_or(Value::is_object);
    }
    message
        .get("id")
        .is_some_and(|id| id.is_string() || id.is_number() || id.is_null())
        && (message.get("result").is_some_and(Value::is_object)
            || message.get("error").is_some_and(Value::is_object))
}
fn qualified_capabilities(value: &Value) -> bool {
    let Some(offers) = value.as_object() else {
        return false;
    };
    offers.iter().all(|(key, value)| {
        let Some(fields) = value.as_object() else {
            return false;
        };
        match key.as_str() {
            "roots" => fields
                .iter()
                .all(|(key, value)| key == "listChanged" && value.is_boolean()),
            "sampling" => fields.iter().all(|(key, value)| {
                matches!(key.as_str(), "context" | "tools") && value.is_object()
            }),
            "elicitation" => fields.iter().all(|(key, value)| match key.as_str() {
                "url" => value.is_object(),
                "form" => value
                    .as_object()
                    .is_some_and(|form| form.get("applyDefaults").is_none_or(Value::is_boolean)),
                _ => false,
            }),
            "experimental" | "extensions" => true,
            _ => false,
        }
    })
}
fn envelope_issue(message: &Value, notification: bool) -> Option<(String, String)> {
    let meta = meta(message)?;
    for key in if notification {
        &[VERSION][..]
    } else {
        &[VERSION, CAPABILITIES][..]
    } {
        if !meta.contains_key(*key) {
            return Some(((*key).into(), "missing".into()));
        }
    }
    if !meta[VERSION].is_string() {
        return Some((VERSION.into(), "expected a protocol version string".into()));
    }
    if notification {
        return None;
    }
    // Complex implementation/capability schema validation is deliberately not
    // guessed here; non-object offers are handled by the qualified oracle seam.
    if !qualified_capabilities(&meta[CAPABILITIES]) {
        return Some((CAPABILITIES.into(), "unqualified".into()));
    }
    if let Some(info) = meta.get(INFO)
        && (!info.is_object()
            || !info.get("name").is_some_and(Value::is_string)
            || !info.get("version").is_some_and(Value::is_string)
            || info.as_object().is_some_and(|fields| {
                fields.iter().any(|(key, value)| {
                    !matches!(
                        key.as_str(),
                        "name" | "version" | "title" | "description" | "websiteUrl"
                    ) || !value.is_string()
                })
            }))
    {
        return Some((INFO.into(), "unqualified".into()));
    }
    None
}
fn name_header_rejection(
    head: &RequestHead<'_>,
    message: &Value,
    method: &str,
) -> Option<Admission> {
    let field = match method {
        "tools/call" | "prompts/get" => "name",
        "resources/read" => "uri",
        "tasks/get" | "tasks/update" | "tasks/cancel" => "taskId",
        _ => return None,
    };
    let body = message
        .get("params")
        .and_then(|params| params.get(field))
        .and_then(Value::as_str);
    let Some(header) = head.header("mcp-name") else {
        return body.map(|body| mismatch("(missing)",format!("the body carries params.{field}=\"{body}\" but the required Mcp-Name header is absent"),id(message)));
    };
    let decoded = if header.starts_with("=?base64?") && header.ends_with("?=") {
        let body = &header[9..header.len() - 2];
        STANDARD
            .decode(body)
            .ok()
            .filter(|decoded| STANDARD.encode(decoded) == body)
            .and_then(|bytes| String::from_utf8(bytes).ok())
    } else {
        Some(header.to_owned())
    };
    match decoded {
        None => Some(mismatch(
            header,
            "the Mcp-Name header carries an invalid Base64 sentinel value".into(),
            id(message),
        )),
        Some(decoded) if body.is_some_and(|body| body != decoded) => Some(mismatch(
            header,
            format!(
                "the body carries params.{field}=\"{}\" but the Mcp-Name header names \"{decoded}\"",
                body.unwrap()
            ),
            id(message),
        )),
        _ => None,
    }
}

/// A pre-body refusal contains no effect or exchange capability.
#[derive(Debug)]
pub enum HeadRefusal {
    Reply(Response),
    Unqualified(Unqualified),
}
/// Reuse the exact Source-observable path/Host/Origin/auth/method/media order
/// before a mature transport polls or allocates a request body.
pub fn preflight(head: &RequestHead<'_>, policy: &Policy) -> Result<(), HeadRefusal> {
    let reject = |status, code, message: &str| {
        HeadRefusal::Reply(error_response(status, code, message, Value::Null, None))
    };
    if head.target.split(['?', '#']).next() != Some("/mcp") {
        return Err(HeadRefusal::Reply(Response::text(
            404,
            "Not found; the MCP endpoint is /mcp",
        )));
    }
    if head.target.len() > MAX_TARGET_BYTES {
        return Err(reject(
            414,
            -32000,
            "Native HTTP target admission capacity exceeded",
        ));
    }
    let bytes = head.headers.iter().try_fold(0usize, |n, (key, value)| {
        n.checked_add(key.len())?.checked_add(value.len())
    });
    if head.headers.len() > MAX_HEADERS || bytes.is_none_or(|n| n > MAX_HEADER_BYTES) {
        return Err(reject(
            431,
            -32000,
            "Native HTTP header admission capacity exceeded",
        ));
    }
    for (at, (name, _)) in head.headers.iter().enumerate() {
        if head.headers[..at]
            .iter()
            .any(|(other, _)| name.eq_ignore_ascii_case(other))
        {
            return Err(HeadRefusal::Unqualified(Unqualified::DuplicateHeader));
        }
    }
    match url_security(head) {
        Ok(Some(reply)) => return Err(HeadRefusal::Reply(reply)),
        Ok(None) => {}
        Err(reason) => return Err(HeadRefusal::Unqualified(reason)),
    }
    if !policy.authenticated(head.header("authorization")) {
        let mut reply = Response::text(401, "Missing or wrong bearer token");
        reply.authenticate_bearer = true;
        return Err(HeadRefusal::Reply(reply));
    }
    if !head.method.eq_ignore_ascii_case("POST") {
        return Err(reject(405, -32000, "Method not allowed."));
    }
    let content_type = head.header("content-type").unwrap_or("");
    if !content_type
        .split(';')
        .next()
        .is_some_and(|kind| kind.trim().eq_ignore_ascii_case("application/json"))
        || content_type.contains(',')
    {
        return Err(reject(
            415,
            -32000,
            "Unsupported Media Type: Content-Type must be application/json",
        ));
    }
    Ok(())
}
pub fn admit(head: RequestHead<'_>, body: Vec<u8>, policy: &Policy) -> Admission {
    if let Err(refusal) = preflight(&head, policy) {
        return match refusal {
            HeadRefusal::Reply(reply) => Admission::Reply(reply),
            HeadRefusal::Unqualified(reason) => Admission::Unqualified(reason),
        };
    }
    if body.len() > MAX_BODY_BYTES {
        return error(
            413,
            -32000,
            format!("Payload Too Large: Request body must not exceed {MAX_BODY_BYTES} bytes"),
            Value::Null,
            None,
        );
    }
    let message: Value = match serde_json::from_slice(&body) {
        Ok(message) => message,
        Err(error)
            if error.classify() == serde_json::error::Category::Syntax
                && error.to_string().contains("recursion limit") =>
        {
            return Admission::Unqualified(Unqualified::JsonDepth);
        }
        Err(_) => return error(400, -32700, "Parse error: Invalid JSON", Value::Null, None),
    };
    if let Some(batch) = message.as_array() {
        if batch.is_empty() {
            return error(
                400,
                -32600,
                "Bad Request: empty JSON-RPC batch",
                Value::Null,
                None,
            );
        }
        for item in batch {
            if meta(item).is_some_and(|meta| meta.contains_key(VERSION)) {
                return error(
                    400,
                    -32600,
                    "Bad Request: JSON-RPC batches may not contain requests for protocol revision 2026-07-28 or later",
                    Value::Null,
                    None,
                );
            }
            if !message_shape(item) {
                return error(
                    400,
                    -32600,
                    "Bad Request: JSON-RPC batch contains an invalid message",
                    Value::Null,
                    None,
                );
            }
        }
        if batch.len() > 100 {
            return error(
                400,
                -32600,
                "Invalid Request: Batch must not exceed 100 messages",
                Value::Null,
                None,
            );
        }
        if batch.len() > 1
            && batch
                .iter()
                .any(|message| message["method"] == "initialize")
        {
            return error(
                400,
                -32600,
                "Invalid Request: Only one initialization request is allowed",
                Value::Null,
                None,
            );
        }
    } else if !message_shape(&message) {
        return error(
            400,
            -32600,
            "Bad Request: the request body is not a valid JSON-RPC message",
            Value::Null,
            None,
        );
    }
    let method = message.get("method").and_then(Value::as_str);
    let notification = method.is_some() && message.get("id").is_none();
    let version = head.header("mcp-protocol-version");
    let claim = meta(&message).and_then(|meta| meta.get(VERSION));
    let modern_initialize = method == Some("initialize")
        && claim.and_then(Value::as_str).is_some_and(modern)
        && envelope_issue(&message, false).is_none();
    if method == Some("initialize") && !modern_initialize {
        if version.is_some_and(modern) {
            return mismatch(version.unwrap(),"an initialize request (legacy handshake) was sent with a modern MCP-Protocol-Version header".into(),id(&message));
        }
    } else if claim.is_some() || (notification && version.is_some_and(modern)) {
        if claim.is_some()
            && let Some((key, problem)) = envelope_issue(&message, notification)
        {
            if problem == "unqualified" {
                return Admission::Unqualified(Unqualified::EnvelopeSchema);
            }
            return error(
                400,
                -32602,
                format!("Invalid _meta envelope for protocol revision {MODERN}: {key}: {problem}"),
                id(&message),
                Some(json!({"envelope":{"key":key,"problem":problem}})),
            );
        }
        let claimed = claim
            .and_then(Value::as_str)
            .or(version)
            .unwrap_or("unknown");
        if let Some(header) = version
            && claim.is_some()
            && header != claimed
        {
            return mismatch(
                header,
                format!(
                    "the {} envelope names protocol version {claimed} but the MCP-Protocol-Version header names {header}",
                    if notification { "notification" } else { "body" }
                ),
                id(&message),
            );
        }
        if let Some(header) = head.header("mcp-method")
            && Some(header) != method
        {
            return mismatch(
                header,
                format!(
                    "the {}names method {} but the Mcp-Method header names {header}",
                    if notification {
                        "notification body "
                    } else {
                        "body "
                    },
                    method.unwrap_or("")
                ),
                id(&message),
            );
        }
        if claimed != MODERN {
            return error(
                400,
                -32022,
                format!("Unsupported protocol version: {claimed}"),
                id(&message),
                Some(json!({"supported":[MODERN],"requested":claimed})),
            );
        }
        if !notification {
            if version.is_none() {
                return mismatch(
                    "(missing)",
                    format!(
                        "the body envelope names protocol version {claimed} but the required MCP-Protocol-Version header is absent"
                    ),
                    id(&message),
                );
            }
            if head.header("mcp-method").is_none() {
                return mismatch(
                    "(missing)",
                    format!(
                        "the body names method {} but the required Mcp-Method header is absent",
                        method.unwrap_or("")
                    ),
                    id(&message),
                );
            }
            if let Some(rejection) = name_header_rejection(&head, &message, method.unwrap_or("")) {
                return rejection;
            }
        }
        let mode = if notification {
            ResponseMode::Empty
        } else if method == Some("subscriptions/listen") {
            ResponseMode::Listen
        } else {
            ResponseMode::Json
        };
        return Admission::Exchange(Exchange {
            original_body: body,
            message,
            era: Era::Modern,
            mode,
            declared_client: None,
        });
    } else if method.is_some() && version.is_some_and(modern) {
        let missing = if meta(&message).is_none() {
            vec!["_meta"]
        } else {
            [VERSION, CAPABILITIES]
                .into_iter()
                .filter(|key| !meta(&message).unwrap().contains_key(*key))
                .collect::<Vec<_>>()
        };
        return error(
            400,
            -32602,
            format!(
                "Invalid params: the MCP-Protocol-Version header names protocol revision {}, but the request is missing the required per-request envelope key(s): {}",
                version.unwrap(),
                missing.join(", ")
            ),
            id(&message),
            Some(json!({"envelope":{"missing":missing}})),
        );
    }
    let accept = head.header("accept").unwrap_or("");
    if !accept.contains("application/json") || !accept.contains("text/event-stream") {
        return error(
            406,
            -32000,
            "Not Acceptable: Client must accept both application/json and text/event-stream",
            Value::Null,
            None,
        );
    }
    if method != Some("initialize") && version.is_some_and(|v| !LEGACY.contains(&v)) {
        return error(
            400,
            -32000,
            format!(
                "Bad Request: Unsupported protocol version: {} (supported versions: {})",
                version.unwrap(),
                LEGACY.join(", ")
            ),
            Value::Null,
            None,
        );
    }
    let batch_without_request = message.as_array().is_some_and(|batch| {
        !batch
            .iter()
            .any(|message| message.get("method").is_some() && message.get("id").is_some())
    });
    let mode = if notification
        || batch_without_request
        || message.get("result").is_some()
        || message.get("error").is_some()
    {
        ResponseMode::Empty
    } else {
        ResponseMode::Sse
    };
    Admission::Exchange(Exchange {
        original_body: body,
        message,
        era: Era::Legacy,
        mode,
        declared_client: declared_client(head.target),
    })
}
