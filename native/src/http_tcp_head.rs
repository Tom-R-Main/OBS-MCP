// SPDX-License-Identifier: GPL-2.0-only
//! Bounded first-head facts from httparse, before Hyper's framing normalization.
//! This never decodes body framing or grants an MCP/Stop capability.
use obs_mcp_native::http_protocol::{MAX_HEADER_BYTES, MAX_HEADERS, MAX_TARGET_BYTES, RequestHead};
use std::sync::Arc;

pub(super) struct Head {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
}
impl Head {
    pub fn borrowed(&self) -> Vec<(&str, &str)> {
        self.headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }
    pub fn request<'a>(&'a self, headers: &'a [(&'a str, &'a str)]) -> RequestHead<'a> {
        RequestHead {
            method: &self.method,
            target: &self.target,
            headers,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Refusal {
    Syntax,
    Capacity,
    Expect,
    Unsupported,
}
pub(super) enum Parsed {
    Partial,
    Complete(Arc<Head>),
    Refuse(Refusal),
}

fn canonical_authority(value: &str) -> bool {
    if value.bytes().any(|b| {
        !b.is_ascii()
            || b.is_ascii_control()
            || matches!(b, b'@' | b'%' | b'\\' | b'/' | b'?' | b'#')
    }) {
        return false;
    }
    let Ok(authority) = value.parse::<hyper::http::uri::Authority>() else {
        return false;
    };
    if authority.port().is_some() && authority.port_u16().is_none() {
        return false;
    }
    let host = authority.host();
    if host.to_ascii_lowercase().starts_with("0x") {
        return false;
    }
    if host.starts_with('[') {
        return host == "[::1]";
    }
    if host.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.to_string() == host);
    }
    if host
        .rsplit('.')
        .next()
        .is_some_and(|label| label.bytes().all(|b| b.is_ascii_digit()))
        || host
            .split('.')
            .any(|label| label.to_ascii_lowercase().starts_with("xn--"))
    {
        return false;
    }
    !host.is_empty()
        && !host.starts_with('.')
        && host.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        })
}
fn closed_urls(target: &str, headers: &[(String, String)]) -> bool {
    let path = target.split('?').next().unwrap_or("");
    if !target.is_ascii()
        || !target.starts_with('/')
        || target.starts_with("//")
        || target.contains(['#', '\\'])
        || path.contains('%')
        || path.split('/').any(|part| matches!(part, "." | ".."))
    {
        return false;
    }
    if let Some(host) = headers
        .iter()
        .find(|(name, _)| name == "host")
        .map(|(_, value)| value)
        && !canonical_authority(host)
    {
        return false;
    }
    if let Some(origin) = headers
        .iter()
        .find(|(name, _)| name == "origin")
        .map(|(_, value)| value)
        && !origin.is_empty()
        && origin != "null"
    {
        let Ok(uri) = origin.parse::<hyper::http::Uri>() else {
            return false;
        };
        if uri.scheme().is_none()
            || !uri
                .authority()
                .is_some_and(|a| canonical_authority(a.as_str()))
        {
            return false;
        }
    }
    true
}
fn first_only(name: &str) -> bool {
    matches!(
        name,
        "age"
            | "authorization"
            | "content-length"
            | "content-type"
            | "etag"
            | "expires"
            | "from"
            | "host"
            | "if-modified-since"
            | "if-unmodified-since"
            | "last-modified"
            | "location"
            | "max-forwards"
            | "proxy-authorization"
            | "referer"
            | "retry-after"
            | "server"
            | "user-agent"
    )
}
pub(super) fn parse(bytes: &[u8]) -> Parsed {
    let mut slots = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut request = httparse::Request::new(&mut slots);
    let offset = match request.parse(bytes) {
        Ok(httparse::Status::Partial) => {
            return if bytes.len() >= MAX_HEADER_BYTES {
                Parsed::Refuse(Refusal::Capacity)
            } else {
                Parsed::Partial
            };
        }
        Ok(httparse::Status::Complete(n)) => n,
        Err(httparse::Error::TooManyHeaders) => return Parsed::Refuse(Refusal::Capacity),
        Err(_) => return Parsed::Refuse(Refusal::Syntax),
    };
    if offset > MAX_HEADER_BYTES {
        return Parsed::Refuse(Refusal::Capacity);
    }
    // httparse supplies the boundary. This stricter byte predicate rejects its
    // bare-LF admission without scanning/decoding HTTP body or chunk boundaries.
    if bytes[..offset]
        .iter()
        .enumerate()
        .any(|(i, b)| *b == b'\n' && (i == 0 || bytes[i - 1] != b'\r'))
    {
        return Parsed::Refuse(Refusal::Syntax);
    }
    let count = |name: &str| {
        request
            .headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case(name))
            .count()
    };
    if (count("transfer-encoding") > 0 && count("content-length") > 0)
        || count("content-length") > 1
        || count("transfer-encoding") > 1
    {
        return Parsed::Refuse(Refusal::Syntax);
    }
    if request
        .headers
        .iter()
        .any(|h| h.name.eq_ignore_ascii_case("content-length") && h.value.contains(&b','))
    {
        return Parsed::Refuse(Refusal::Syntax);
    }
    if request
        .headers
        .iter()
        .any(|h| h.name.eq_ignore_ascii_case("expect"))
    {
        return Parsed::Refuse(Refusal::Expect);
    }
    let mut headers = Vec::<(String, String)>::with_capacity(MAX_HEADERS);
    for header in request.headers.iter() {
        let name = header.name.to_ascii_lowercase();
        if !header.value.is_ascii() {
            return Parsed::Refuse(Refusal::Unsupported);
        }
        let Ok(value) = std::str::from_utf8(header.value) else {
            return Parsed::Refuse(Refusal::Unsupported);
        };
        let value = value.trim_matches([' ', '\t']);
        if name == "transfer-encoding" && !value.eq_ignore_ascii_case("chunked") {
            return Parsed::Refuse(Refusal::Unsupported);
        }
        if let Some((_, old)) = headers.iter_mut().find(|(key, _)| key == &name) {
            if !first_only(&name) {
                let separator = if name == "cookie" { "; " } else { ", " };
                let mut joined = String::with_capacity(old.len() + separator.len() + value.len());
                joined.push_str(old);
                joined.push_str(separator);
                joined.push_str(value);
                *old = joined;
            }
        } else {
            headers.push((name, value.into()));
        }
    }
    let target = request.path.unwrap_or("");
    if target.len() > MAX_TARGET_BYTES {
        return Parsed::Refuse(Refusal::Capacity);
    }
    if !closed_urls(target, &headers) {
        return Parsed::Refuse(Refusal::Unsupported);
    }
    Parsed::Complete(Arc::new(Head {
        method: request.method.unwrap_or("").into(),
        target: target.into(),
        headers,
    }))
}
