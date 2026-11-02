use std::{
    fmt, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
};
pub const MAX_HOST: usize = 253;
pub const MAX_ADDRESSES: usize = 16;
pub const MAX_SCANNED: usize = 64;
pub const MAX_REQUEST: usize = 7 + MAX_HOST;
pub const MAX_REPLY: usize = 512;
pub const REQUEST_MAGIC: &[u8; 4] = b"ODR1";
pub const REPLY_MAGIC: &[u8; 4] = b"ODA1";

#[derive(Clone, PartialEq, Eq)]
pub enum ExpectedPeer {
    Dns(String),
    Ip(IpAddr),
}
impl ExpectedPeer {
    pub fn parse(host: &str) -> Result<Self, Error> {
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(Self::Ip(ip));
        }
        let labels = host.strip_suffix('.').unwrap_or(host);
        if host.is_empty()
            || host.len() > MAX_HOST
            || !host.is_ascii()
            || labels.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || !label
                        .as_bytes()
                        .first()
                        .is_some_and(u8::is_ascii_alphanumeric)
                    || !label
                        .as_bytes()
                        .last()
                        .is_some_and(u8::is_ascii_alphanumeric)
                    || !label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
        {
            return Err(Error::InvalidInput);
        }
        Ok(Self::Dns(host.to_owned()))
    }
    pub fn dns_name(&self) -> Option<&str> {
        match self {
            Self::Dns(host) => Some(host),
            Self::Ip(_) => None,
        }
    }
}
impl fmt::Debug for ExpectedPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dns(host) => f.debug_struct("Dns").field("bytes", &host.len()).finish(),
            Self::Ip(IpAddr::V4(_)) => f.write_str("Ipv4"),
            Self::Ip(IpAddr::V6(_)) => f.write_str("Ipv6"),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidInput,
    Deadline,
    Cancelled,
    Capacity,
    Spawn,
    Io,
    Protocol,
    Lookup,
    TooManyAddresses,
    ChildFailed,
    OwnerStopped,
    CleanupUnconfirmed,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Clone)]
pub struct Resolved {
    pub expected: ExpectedPeer,
    pub port: u16,
    pub addresses: Vec<SocketAddr>,
}
impl fmt::Debug for Resolved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resolved")
            .field("expected", &self.expected)
            .field("address_count", &self.addresses.len())
            .finish()
    }
}

pub(crate) fn request(peer: &ExpectedPeer, port: u16) -> Result<Vec<u8>, Error> {
    if port == 0 {
        return Err(Error::InvalidInput);
    }
    let host = peer.dns_name().ok_or(Error::InvalidInput)?;
    let mut out = Vec::with_capacity(7 + host.len());
    out.extend_from_slice(REQUEST_MAGIC);
    out.extend_from_slice(&port.to_be_bytes());
    out.push(host.len() as u8);
    out.extend_from_slice(host.as_bytes());
    Ok(out)
}
pub(crate) fn read_request(mut input: impl io::Read) -> Result<(ExpectedPeer, u16), Error> {
    let mut header = [0; 7];
    input.read_exact(&mut header).map_err(|_| Error::Protocol)?;
    if &header[..4] != REQUEST_MAGIC || header[6] == 0 || usize::from(header[6]) > MAX_HOST {
        return Err(Error::Protocol);
    }
    let mut host = [0; MAX_HOST];
    input
        .read_exact(&mut host[..usize::from(header[6])])
        .map_err(|_| Error::Protocol)?;
    // Leave all post-frame bytes for the unbuffered lifetime watchdog.
    let port = u16::from_be_bytes([header[4], header[5]]);
    if port == 0 {
        return Err(Error::Protocol);
    }
    let host = std::str::from_utf8(&host[..usize::from(header[6])]).map_err(|_| Error::Protocol)?;
    let expected = ExpectedPeer::parse(host).map_err(|_| Error::Protocol)?;
    if !matches!(expected, ExpectedPeer::Dns(_)) {
        return Err(Error::Protocol);
    }
    Ok((expected, port))
}
pub(crate) fn reply(addresses: Result<&[SocketAddr], Error>) -> Vec<u8> {
    let mut out = Vec::with_capacity(6 + MAX_ADDRESSES * 25);
    out.extend_from_slice(REPLY_MAGIC);
    match addresses {
        Err(error) => {
            out.push(match error {
                Error::TooManyAddresses => 2,
                Error::InvalidInput | Error::Protocol => 3,
                _ => 1,
            });
            out.push(0);
        }
        Ok(addresses) => {
            if addresses.is_empty() || addresses.len() > MAX_ADDRESSES {
                return reply(Err(Error::TooManyAddresses));
            }
            out.push(0);
            out.push(addresses.len() as u8);
            for address in addresses {
                match address {
                    SocketAddr::V4(address) => {
                        out.push(4);
                        out.extend_from_slice(&address.ip().octets());
                    }
                    SocketAddr::V6(address) => {
                        out.push(6);
                        out.extend_from_slice(&address.ip().octets());
                        out.extend_from_slice(&address.flowinfo().to_be_bytes());
                        out.extend_from_slice(&address.scope_id().to_be_bytes());
                    }
                }
            }
        }
    }
    out
}
pub(crate) fn decode(bytes: &[u8], expected: ExpectedPeer, port: u16) -> Result<Resolved, Error> {
    if bytes.len() < 6 || bytes.len() > MAX_REPLY || &bytes[..4] != REPLY_MAGIC {
        return Err(Error::Protocol);
    }
    if bytes[4] != 0 {
        if bytes.len() != 6 || bytes[5] != 0 {
            return Err(Error::Protocol);
        }
        return Err(match bytes[4] {
            1 => Error::Lookup,
            2 => Error::TooManyAddresses,
            3 => Error::InvalidInput,
            _ => Error::Protocol,
        });
    }
    let count = usize::from(bytes[5]);
    if count == 0 || count > MAX_ADDRESSES {
        return Err(Error::Protocol);
    }
    let mut addresses = Vec::with_capacity(count);
    let mut offset = 6;
    for _ in 0..count {
        let kind = *bytes.get(offset).ok_or(Error::Protocol)?;
        offset += 1;
        let address = match kind {
            4 => {
                let data = bytes.get(offset..offset + 4).ok_or(Error::Protocol)?;
                offset += 4;
                SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(data[0], data[1], data[2], data[3])),
                    port,
                )
            }
            6 => {
                let data = bytes.get(offset..offset + 24).ok_or(Error::Protocol)?;
                offset += 24;
                let mut ip = [0; 16];
                ip.copy_from_slice(&data[..16]);
                let flow =
                    u32::from_be_bytes(data[16..20].try_into().map_err(|_| Error::Protocol)?);
                let scope =
                    u32::from_be_bytes(data[20..24].try_into().map_err(|_| Error::Protocol)?);
                SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(ip), port, flow, scope))
            }
            _ => return Err(Error::Protocol),
        };
        if addresses.contains(&address) {
            return Err(Error::Protocol);
        }
        addresses.push(address);
    }
    if offset != bytes.len() {
        return Err(Error::Protocol);
    }
    Ok(Resolved {
        expected,
        port,
        addresses,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_wire_vectors_and_identity_are_preserved() {
        let peer = ExpectedPeer::parse("localhost").unwrap();
        let decoded = decode(b"ODA1\x00\x01\x04\x7f\x00\x00\x01", peer.clone(), 4455).unwrap();
        assert_eq!(decoded.addresses, ["127.0.0.1:4455".parse().unwrap()]);
        assert_eq!(decoded.expected, peer);
        assert_eq!(request(&peer, 4455).unwrap(), b"ODR1\x11\x67\x09localhost");
    }
    #[test]
    fn hostile_and_oversized_protocols_refuse_without_name_echo() {
        for name in ["", "bad..name", "-bad.invalid", "a/b", "a\0b", "a b"] {
            assert!(ExpectedPeer::parse(name).is_err());
        }
        let peer = ExpectedPeer::parse("private-name.invalid").unwrap();
        assert!(!format!("{peer:?}").contains("private-name"));
        for data in [
            &b"ODA1\x00\x00"[..],
            &b"ODA1\x00\x11"[..],
            &b"ODA1\x00\x01\x04\x7f\x00\x00\x01extra"[..],
        ] {
            assert!(decode(data, peer.clone(), 1).is_err());
        }
    }
}
