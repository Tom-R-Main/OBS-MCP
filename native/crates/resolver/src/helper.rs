use crate::protocol::{self, Error, ExpectedPeer, MAX_ADDRESSES, MAX_SCANNED};
use std::{
    fs::File,
    io::{self, Read, Write},
    net::{SocketAddr, ToSocketAddrs},
    process::ExitCode,
};
#[cfg(unix)]
fn owned_stdin() -> io::Result<File> {
    use std::os::fd::AsFd;
    io::stdin().as_fd().try_clone_to_owned().map(File::from)
}
#[cfg(windows)]
fn owned_stdin() -> io::Result<File> {
    use std::os::windows::io::AsHandle;
    io::stdin().as_handle().try_clone_to_owned().map(File::from)
}

pub(crate) const ENTRY: &str = "--obs-mcp-internal-resolver-v1";

/// Dispatch before constructing the app runtime/configuration. The helper
/// launches no child process. One local watchdog owns the stdin lifeline.
pub fn internal_entry() -> Option<ExitCode> {
    let mut args = std::env::args_os().skip(1);
    let arg = args.next()?;
    let normal = arg == ENTRY;
    #[cfg(feature = "resolver-fixture")]
    let fixture = match arg.to_str() {
        Some("--obs-mcp-internal-resolver-stall-v1") => Some(1),
        #[cfg(unix)]
        Some("--obs-mcp-internal-resolver-resistant-v1") => Some(2),
        Some("--obs-mcp-internal-resolver-oversize-v1") => Some(3),
        Some("--obs-mcp-internal-resolver-linger-v1") => Some(4),
        _ => None,
    };
    #[cfg(not(feature = "resolver-fixture"))]
    let fixture: Option<u8> = None;
    if !normal && fixture.is_none() {
        return None;
    }
    if args.next().is_some() {
        return Some(ExitCode::from(2));
    }
    // File reads are unbuffered. No StdinLock/BufReader can prefetch post-frame
    // bytes and hide them from the lifetime watchdog.
    let input = owned_stdin();
    let Ok(mut input) = input else {
        return Some(ExitCode::from(2));
    };
    let Ok((peer, port)) = protocol::read_request(&mut input) else {
        return Some(ExitCode::from(2));
    };
    let watchdog = std::thread::Builder::new()
        .name("obs-dns-lifeline".into())
        .stack_size(128 * 1024)
        .spawn(move || {
            let mut extra = [0; 1];
            loop {
                match input.read(&mut extra) {
                    Ok(0) => std::process::exit(75),
                    Ok(_) => std::process::exit(76),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => std::process::exit(77),
                }
            }
        });
    if watchdog.is_err() {
        return Some(ExitCode::from(2));
    }
    // This thread is contained by the helper process, not detached into the
    // Service. Normal process exit ends it; the parent still explicitly reaps.
    #[cfg(feature = "resolver-fixture")]
    if let Some(fixture) = fixture {
        return Some(run_fixture(fixture, peer, port));
    }
    Some(run(peer, port))
}
fn lookup(peer: ExpectedPeer, port: u16) -> Result<Vec<SocketAddr>, Error> {
    let host = peer.dns_name().ok_or(Error::InvalidInput)?;
    // Uncancelable NSS stays inside this process. Internal NSS allocation/RSS is
    // not bounded by our output/address quotas.
    let addresses = (host, port).to_socket_addrs().map_err(|_| Error::Lookup)?;
    let mut result = Vec::with_capacity(MAX_ADDRESSES);
    let mut scanned = 0;
    for address in addresses.take(MAX_SCANNED) {
        scanned += 1;
        if address.port() != port {
            return Err(Error::Protocol);
        }
        if !result.contains(&address) {
            if result.len() == MAX_ADDRESSES {
                return Err(Error::TooManyAddresses);
            }
            result.push(address);
        }
    }
    if scanned == MAX_SCANNED {
        return Err(Error::TooManyAddresses);
    }
    if result.is_empty() {
        return Err(Error::Lookup);
    }
    Ok(result)
}
fn emit(bytes: &[u8]) -> bool {
    let mut stdout = io::stdout().lock();
    stdout
        .write_all(bytes)
        .and_then(|()| stdout.flush())
        .is_ok()
}
fn run(peer: ExpectedPeer, port: u16) -> ExitCode {
    let result = lookup(peer, port);
    let bytes = protocol::reply(result.as_deref().map_err(|error| *error));
    if emit(&bytes) && result.is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(2)
    }
}
#[cfg(feature = "resolver-fixture")]
fn run_fixture(mode: u8, peer: ExpectedPeer, port: u16) -> ExitCode {
    if mode == 3 {
        let _ = emit(&[0x58; 1024]);
        return ExitCode::SUCCESS;
    }
    if mode == 4 {
        let _ = run(peer, port);
        loop {
            std::thread::park_timeout(std::time::Duration::from_secs(60));
        }
    }
    #[cfg(unix)]
    if mode == 2 {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
        else {
            return ExitCode::from(2);
        };
        return runtime.block_on(async {
            let Ok(mut termination) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            else {
                return ExitCode::from(2);
            };
            if !emit(b"RDY1") {
                return ExitCode::from(2);
            }
            // No assertion that SIGTERM was actually delivered is made by readiness.
            loop {
                termination.recv().await;
            }
        });
    }
    let _ = emit(b"RDY1");
    loop {
        std::thread::park_timeout(std::time::Duration::from_secs(60));
    }
}
