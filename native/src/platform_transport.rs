// SPDX-License-Identifier: GPL-2.0-only
//! Opt-in integration staging. The outer owner outlives establishment futures.
#[cfg(target_os = "linux")]
use std::net::SocketAddr;
use std::{
    io,
    net::IpAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::Mutex,
    time::Instant,
};
use tokio_tungstenite::tungstenite::http::Uri;

#[derive(Clone, PartialEq, Eq)]
pub enum Peer {
    Dns(String),
    Ip(IpAddr),
}
impl Peer {
    fn identity(&self) -> String {
        match self {
            Self::Dns(name) => name.clone(),
            Self::Ip(ip) => ip.to_string(),
        }
    }
    pub fn from_wss(url: &str) -> io::Result<(Self, u16)> {
        if url.contains('#') {
            return Err(invalid());
        }
        let uri: Uri = url.parse().map_err(|_| invalid())?;
        if uri.scheme_str() != Some("wss") {
            return Err(invalid());
        }
        let authority = uri.authority().ok_or_else(invalid)?;
        if authority.as_str().contains('@') {
            return Err(invalid());
        }
        let host = authority.host();
        // http::Authority::port() returns None for an invalid numeric suffix;
        // distinguish absent from invalid rather than silently selecting443.
        let suffix = authority.as_str().strip_prefix(host).ok_or_else(invalid)?;
        let port = if suffix.is_empty() {
            443
        } else {
            suffix
                .strip_prefix(':')
                .ok_or_else(invalid)?
                .parse::<u16>()
                .map_err(|_| invalid())?
        };
        if port == 0 {
            return Err(invalid());
        }
        let host = host
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(host);
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok((Self::Ip(ip), port));
        }
        let labels = host.strip_suffix('.').unwrap_or(host);
        if host.is_empty()
            || host.len() > 253
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
            return Err(invalid());
        }
        Ok((Self::Dns(host.to_owned()), port))
    }
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "explicit WSS DNS/IP identity required",
    )
}

/// Retain in Native Runtime before calling ObsSession::connect_with_platform_owner.
/// Default Runtime construction does not enable this integration candidate.
pub struct Owner {
    custody: Custody,
}
/// Cloneable establishment handle. The Runtime retains Owner and its shutdown
/// authority while a static bootstrap task carries this handle by value.
#[derive(Clone)]
pub struct Connector {
    custody: Custody,
}
#[derive(Clone)]
pub(crate) struct Custody {
    supervisor: Arc<Mutex<obs_mcp_platform_tls::Supervisor>>,
    #[cfg(test)]
    fixture: Option<Arc<Fixture>>,
}
impl Default for Owner {
    fn default() -> Self {
        Self::new()
    }
}
impl Owner {
    pub fn new() -> Self {
        Self {
            custody: Custody {
                supervisor: Arc::new(Mutex::new(obs_mcp_platform_tls::Supervisor::default())),
                #[cfg(test)]
                fixture: None,
            },
        }
    }
    pub(crate) fn custody(&self) -> Custody {
        self.custody.clone()
    }
    pub fn connector(&self) -> Connector {
        Connector {
            custody: self.custody.clone(),
        }
    }
    /// Borrowing self leaves the same supervisor in custody if this await is dropped.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.custody.join().await
    }
    #[cfg(test)]
    pub(crate) fn fixture(stream: tokio::io::DuplexStream) -> (Self, Arc<Fixture>) {
        let fixture = Arc::new(Fixture {
            stream: std::sync::Mutex::new(Some(stream)),
            retired: std::sync::atomic::AtomicBool::new(false),
            released: std::sync::atomic::AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        });
        let mut owner = Self::new();
        owner.custody.fixture = Some(fixture.clone());
        (owner, fixture)
    }
}
impl Connector {
    pub(crate) fn into_custody(self) -> Custody {
        self.custody
    }
}
impl Custody {
    pub async fn open(&self, peer: &Peer, port: u16, deadline: Instant) -> io::Result<ByteStream> {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "original establishment deadline",
            ));
        }
        #[cfg(test)]
        if let Some(fixture) = &self.fixture {
            let stream = fixture
                .stream
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| io::Error::other("fixture already consumed"))?;
            return Ok(ByteStream::Fixture {
                stream,
                owner: fixture.clone(),
            });
        }
        #[cfg(target_os = "macos")]
        let connect = {
            self.supervisor
                .lock()
                .await
                .connect(&peer.identity(), port, deadline.into_std())?
        };
        #[cfg(target_os = "linux")]
        let connect = {
            // No hidden getaddrinfo/lookup_host task is introduced. A retained
            // resolver must be qualified before DNS may use this Linux factory.
            let Peer::Ip(ip) = peer else {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Linux TLS DNS resolver ownership is unqualified",
                ));
            };
            let socket = tokio::time::timeout_at(
                deadline,
                tokio::net::TcpStream::connect(SocketAddr::new(*ip, port)),
            )
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "original TCP establishment deadline",
                )
            })??;
            self.supervisor.lock().await.connect_socket(
                socket,
                &peer.identity(),
                deadline.into_std(),
            )?
        };
        Ok(ByteStream::Native(connect.await?))
    }
    #[cfg(target_os = "linux")]
    pub async fn open_connected(
        &self,
        socket: tokio::net::TcpStream,
        expected: &str,
        deadline: Instant,
    ) -> io::Result<ByteStream> {
        let connect = {
            self.supervisor
                .lock()
                .await
                .connect_socket(socket, expected, deadline.into_std())?
        };
        Ok(ByteStream::Native(connect.await?))
    }
    pub async fn join(&self) -> io::Result<()> {
        #[cfg(test)]
        if let Some(fixture) = &self.fixture {
            loop {
                let changed = fixture.changed.notified();
                if fixture.released.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                changed.await;
            }
        }
        // Guard owns a borrow of the retained supervisor, never a moved local owner.
        // Dropping this future releases the guard while leaving entries in place.
        let snapshots = self.supervisor.lock().await.shutdown().await;
        #[cfg(target_os = "macos")]
        let joined = snapshots
            .iter()
            .all(|f| f.cancelled && f.released && f.captures == 0 && f.pending == 0);
        #[cfg(target_os = "linux")]
        let joined = snapshots.iter().all(|f| {
            f.joined && f.counters[..3] == [0, 0, 0] && !f.setup_pending && !f.cleanup_pending
        });
        if joined {
            Ok(())
        } else {
            Err(io::Error::other("native TLS cleanup is unconfirmed"))
        }
    }
}
pub(crate) enum ByteStream {
    Native(obs_mcp_platform_tls::Stream),
    #[cfg(test)]
    Fixture {
        stream: tokio::io::DuplexStream,
        owner: Arc<Fixture>,
    },
}
impl AsyncRead for ByteStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(stream) => Pin::new(stream).poll_read(cx, out),
            #[cfg(test)]
            Self::Fixture { stream, .. } => Pin::new(stream).poll_read(cx, out),
        }
    }
}
impl AsyncWrite for ByteStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Native(stream) => Pin::new(stream).poll_write(cx, bytes),
            #[cfg(test)]
            Self::Fixture { stream, .. } => Pin::new(stream).poll_write(cx, bytes),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(test)]
            Self::Fixture { stream, .. } => Pin::new(stream).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(test)]
            Self::Fixture { stream, .. } => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
#[cfg(test)]
impl Drop for ByteStream {
    fn drop(&mut self) {
        if let Self::Fixture { owner, .. } = self {
            owner
                .retired
                .store(true, std::sync::atomic::Ordering::SeqCst);
            owner.changed.notify_waiters();
        }
    }
}
#[cfg(test)]
pub(crate) struct Fixture {
    stream: std::sync::Mutex<Option<tokio::io::DuplexStream>>,
    pub retired: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}
#[cfg(test)]
impl Fixture {
    pub fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parsed_identity_preserves_dns_and_bracketless_ip() {
        assert!(
            matches!(Peer::from_wss("wss://localhost:4455/path?x=y").unwrap(),(Peer::Dns(name),4455)if name=="localhost")
        );
        assert!(matches!(
            Peer::from_wss("wss://[::1]:4455/").unwrap(),
            (Peer::Ip(IpAddr::V6(_)), 4455)
        ));
        assert!(matches!(
            Peer::from_wss("wss://127.0.0.1/").unwrap(),
            (Peer::Ip(IpAddr::V4(_)), 443)
        ));
    }
    #[test]
    fn ambiguous_or_credentialed_identities_are_refused_before_transport() {
        for url in [
            "ws://localhost/",
            "wss://user:password@localhost/",
            "wss://localhost:0/",
            "wss://localhost:99999/",
            "wss://-bad.invalid/",
            "wss://bad..invalid/",
            "wss://[fe80::1%25en0]/",
        ] {
            assert!(Peer::from_wss(url).is_err());
        }
    }
    #[tokio::test]
    async fn interrupted_shutdown_retains_same_owner() {
        let (client, _peer) = tokio::io::duplex(64);
        let (mut owner, fixture) = Owner::fixture(client);
        let mut closing = Box::pin(owner.shutdown());
        assert!(futures_util::poll!(closing.as_mut()).is_pending());
        drop(closing);
        fixture.release();
        owner.shutdown().await.unwrap();
    }
}
