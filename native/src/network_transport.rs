// SPDX-License-Identifier: GPL-2.0-only
//! Retained resolution/transport owner shared by plaintext and encrypted OBS.
use obs_mcp_resolver::{ExpectedPeer, Resolved};
#[cfg(test)]
use std::net::IpAddr;
use std::{
    fmt,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::Mutex,
    time::{Instant, timeout_at},
};
use tokio_tungstenite::tungstenite::http::Uri;

/// Admission for adapters already bound to an established physical session.
/// Parsing an origin alone does not confer transport or mutation authority.
pub(crate) fn admitted_scheme(scheme: Option<&str>) -> bool {
    scheme == Some("ws")
        || cfg!(all(
            feature = "platform-tls-evaluation",
            any(target_os = "macos", target_os = "linux")
        )) && scheme == Some("wss")
}

pub struct Origin {
    pub encrypted: bool,
    pub expected: ExpectedPeer,
    pub port: u16,
}
impl Origin {
    pub fn parse(url: &str) -> io::Result<Self> {
        if url.contains('#') {
            return Err(invalid());
        }
        let uri: Uri = url.parse().map_err(|_| invalid())?;
        let encrypted = match uri.scheme_str() {
            Some("ws") => false,
            Some("wss") => true,
            _ => return Err(invalid()),
        };
        let authority = uri.authority().ok_or_else(invalid)?;
        if authority.as_str().contains('@') {
            return Err(invalid());
        }
        let host = authority.host();
        let suffix = authority.as_str().strip_prefix(host).ok_or_else(invalid)?;
        let port = if suffix.is_empty() {
            if encrypted { 443 } else { 80 }
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
        Ok(Self {
            encrypted,
            expected: ExpectedPeer::parse(host).map_err(|_| invalid())?,
            port,
        })
    }
    #[cfg(all(feature = "platform-tls-evaluation", target_os = "linux"))]
    fn identity(&self) -> String {
        match &self.expected {
            ExpectedPeer::Dns(name) => name.clone(),
            ExpectedPeer::Ip(ip) => ip.to_string(),
        }
    }
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "explicit WS/WSS DNS/IP origin required",
    )
}
fn unavailable() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "encrypted transport requires its qualified platform backend",
    )
}
pub struct Owner {
    custody: Custody,
}
#[derive(Clone)]
pub struct Connector {
    custody: Custody,
}
#[derive(Clone)]
pub(crate) struct Custody {
    resolver: Arc<Mutex<obs_mcp_resolver::Owner>>,
    #[cfg(all(
        feature = "platform-tls-evaluation",
        any(target_os = "macos", target_os = "linux")
    ))]
    tls: crate::platform_transport::Custody,
}
impl Owner {
    pub fn new() -> io::Result<Self> {
        Self::with_additional_roots(None)
    }
    pub fn with_additional_roots(roots: Option<Vec<Vec<u8>>>) -> io::Result<Self> {
        #[cfg(all(
            feature = "platform-tls-evaluation",
            any(target_os = "macos", target_os = "linux")
        ))]
        let tls = match roots {
            Some(roots) => crate::platform_transport::Owner::with_additional_roots(roots)?,
            None => crate::platform_transport::Owner::new(),
        }
        .custody();
        #[cfg(not(all(
            feature = "platform-tls-evaluation",
            any(target_os = "macos", target_os = "linux")
        )))]
        if roots.is_some() {
            return Err(unavailable());
        }
        Ok(Self {
            custody: Custody {
                resolver: Arc::new(Mutex::new(
                    obs_mcp_resolver::Owner::new().map_err(io::Error::other)?,
                )),
                #[cfg(all(
                    feature = "platform-tls-evaluation",
                    any(target_os = "macos", target_os = "linux")
                ))]
                tls,
            },
        })
    }
    pub fn connector(&self) -> Connector {
        Connector {
            custody: self.custody.clone(),
        }
    }
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.shutdown_after_backend(Ok(())).await
    }
    pub async fn shutdown_after_backend(&mut self, backend: io::Result<()>) -> io::Result<()> {
        await_all(
            backend,
            self.custody.join_tls(),
            self.custody.join_resolver(),
        )
        .await
    }
}
impl Connector {
    pub(crate) fn into_custody(self) -> Custody {
        self.custody
    }
}

/// Preserve each failure and await all owners before returning any failure.
#[derive(Debug)]
pub struct CleanupFailures {
    pub backend: Option<io::Error>,
    pub tls: Option<io::Error>,
    pub resolver: Option<io::Error>,
}
impl fmt::Display for CleanupFailures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Native cleanup is unconfirmed (backend={}, TLS={}, resolver={})",
            self.backend.is_some(),
            self.tls.is_some(),
            self.resolver.is_some()
        )
    }
}
impl std::error::Error for CleanupFailures {}
pub(crate) async fn await_all(
    backend: io::Result<()>,
    tls: impl Future<Output = io::Result<()>>,
    resolver: impl Future<Output = io::Result<()>>,
) -> io::Result<()> {
    let (tls, resolver) = tokio::join!(tls, resolver);
    let failures = CleanupFailures {
        backend: backend.err(),
        tls: tls.err(),
        resolver: resolver.err(),
    };
    if failures.backend.is_some() || failures.tls.is_some() || failures.resolver.is_some() {
        Err(io::Error::other(failures))
    } else {
        Ok(())
    }
}
impl Custody {
    async fn join_tls(&self) -> io::Result<()> {
        #[cfg(all(
            feature = "platform-tls-evaluation",
            any(target_os = "macos", target_os = "linux")
        ))]
        {
            self.tls.join().await?;
        }
        Ok(())
    }
    async fn join_resolver(&self) -> io::Result<()> {
        self.resolver
            .lock()
            .await
            .shutdown()
            .await
            .map(|_| ())
            .map_err(io::Error::other)
    }
    pub async fn join(&self) -> io::Result<()> {
        await_all(Ok(()), self.join_tls(), self.join_resolver()).await
    }
    pub async fn open(&self, origin: &Origin, deadline: Instant) -> io::Result<ByteStream> {
        if origin.encrypted {
            #[cfg(not(all(
                feature = "platform-tls-evaluation",
                any(target_os = "macos", target_os = "linux")
            )))]
            {
                return Err(unavailable());
            }
            #[cfg(all(feature = "platform-tls-evaluation", target_os = "macos"))]
            {
                // Network.framework owns asynchronous DNS/TCP/TLS and its cancellation
                // callbacks. Do not open and discard a second Rust TCP socket.
                let peer = match &origin.expected {
                    ExpectedPeer::Dns(name) => crate::platform_transport::Peer::Dns(name.clone()),
                    ExpectedPeer::Ip(ip) => crate::platform_transport::Peer::Ip(*ip),
                };
                return Ok(ByteStream::Platform(
                    self.tls.open(&peer, origin.port, deadline).await?,
                ));
            }
        }
        let resolve = {
            self.resolver
                .lock()
                .await
                .resolve(origin.expected.clone(), origin.port, deadline)
                .map_err(io::Error::other)?
        };
        let resolved = timeout_at(deadline, resolve)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "original resolution deadline"))?
            .map_err(io::Error::other)?;
        let socket = connect_addresses(resolved, deadline).await?;
        if origin.encrypted {
            #[cfg(all(feature = "platform-tls-evaluation", target_os = "linux"))]
            {
                return Ok(ByteStream::Platform(
                    self.tls
                        .open_connected(socket, &origin.identity(), deadline)
                        .await?,
                ));
            }
            #[cfg(not(all(feature = "platform-tls-evaluation", target_os = "linux")))]
            {
                return Err(unavailable());
            }
        }
        Ok(ByteStream::Plain(socket))
    }
}
async fn connect_addresses(resolved: Resolved, deadline: Instant) -> io::Result<TcpStream> {
    let mut last = None;
    for address in resolved.addresses {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "original TCP establishment deadline",
            ));
        }
        match timeout_at(deadline, TcpStream::connect(address)).await {
            Ok(Ok(socket)) => return Ok(socket),
            Ok(Err(error)) => last = Some(error),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "original TCP establishment deadline",
                ));
            }
        }
    }
    Err(last
        .unwrap_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "no bounded addresses")))
}
/// Legacy library callers without a retained owner may use numeric plaintext
/// endpoints. They refuse hostnames before any nonabortable resolver work.
pub(crate) async fn numeric_plain(url: &str, deadline: Instant) -> io::Result<ByteStream> {
    let origin = Origin::parse(url)?;
    if origin.encrypted {
        return Err(unavailable());
    }
    let ExpectedPeer::Ip(ip) = origin.expected else {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "hostname transport requires a retained connection owner",
        ));
    };
    let socket = timeout_at(
        deadline,
        TcpStream::connect(SocketAddr::new(ip, origin.port)),
    )
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "original TCP establishment deadline",
        )
    })??;
    Ok(ByteStream::Plain(socket))
}
pub(crate) enum ByteStream {
    Plain(TcpStream),
    #[cfg(all(
        feature = "platform-tls-evaluation",
        any(target_os = "macos", target_os = "linux")
    ))]
    Platform(crate::platform_transport::ByteStream),
}
impl AsyncRead for ByteStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, out),
            #[cfg(all(
                feature = "platform-tls-evaluation",
                any(target_os = "macos", target_os = "linux")
            ))]
            Self::Platform(stream) => Pin::new(stream).poll_read(cx, out),
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
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, bytes),
            #[cfg(all(
                feature = "platform-tls-evaluation",
                any(target_os = "macos", target_os = "linux")
            ))]
            Self::Platform(stream) => Pin::new(stream).poll_write(cx, bytes),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(all(
                feature = "platform-tls-evaluation",
                any(target_os = "macos", target_os = "linux")
            ))]
            Self::Platform(stream) => Pin::new(stream).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(all(
                feature = "platform-tls-evaluation",
                any(target_os = "macos", target_os = "linux")
            ))]
            Self::Platform(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    #[tokio::test]
    async fn failed_service_shutdown_still_awaits_both_outer_owners_and_keeps_failure() {
        let tls_gate = Arc::new(tokio::sync::Notify::new());
        let dns_gate = Arc::new(tokio::sync::Notify::new());
        let tls_done = Arc::new(AtomicBool::new(false));
        let dns_done = Arc::new(AtomicBool::new(false));
        let tls = {
            let gate = tls_gate.clone();
            let done = tls_done.clone();
            async move {
                gate.notified().await;
                done.store(true, Ordering::SeqCst);
                Ok(())
            }
        };
        let dns = {
            let gate = dns_gate.clone();
            let done = dns_done.clone();
            async move {
                gate.notified().await;
                done.store(true, Ordering::SeqCst);
                Ok(())
            }
        };
        let mut closing = Box::pin(await_all(
            Err(io::Error::other("injected Service shutdown failure")),
            tls,
            dns,
        ));
        assert!(futures_util::poll!(closing.as_mut()).is_pending());
        assert!(!tls_done.load(Ordering::SeqCst));
        assert!(!dns_done.load(Ordering::SeqCst));
        dns_gate.notify_one();
        assert!(futures_util::poll!(closing.as_mut()).is_pending());
        assert!(!tls_done.load(Ordering::SeqCst));
        assert!(dns_done.load(Ordering::SeqCst));
        tls_gate.notify_one();
        let error = closing.await.unwrap_err();
        let failures = error
            .get_ref()
            .unwrap()
            .downcast_ref::<CleanupFailures>()
            .unwrap();
        assert!(failures.backend.is_some());
        assert!(failures.tls.is_none() && failures.resolver.is_none());
        assert!(dns_done.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn multiple_cleanup_errors_remain_observable() {
        let error = await_all(
            Err(io::Error::other("backend")),
            async { Err(io::Error::other("TLS")) },
            async { Err(io::Error::other("resolver")) },
        )
        .await
        .unwrap_err();
        let failures = error
            .get_ref()
            .unwrap()
            .downcast_ref::<CleanupFailures>()
            .unwrap();
        assert!(
            failures.backend.is_some() && failures.tls.is_some() && failures.resolver.is_some()
        );
    }
    #[test]
    fn typed_origins_preserve_identity_and_do_not_default_invalid_ports() {
        assert!(
            matches!(Origin::parse("ws://localhost:4455/").unwrap().expected,ExpectedPeer::Dns(name)if name=="localhost")
        );
        assert!(matches!(
            Origin::parse("wss://[::1]/").unwrap().expected,
            ExpectedPeer::Ip(IpAddr::V6(_))
        ));
        for url in [
            "ws://localhost:99999/",
            "ws://u:p@localhost/",
            "ws://localhost:0/",
            "ws://localhost/#x",
        ] {
            assert!(Origin::parse(url).is_err());
        }
    }
}
