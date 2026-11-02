// SPDX-License-Identifier: GPL-2.0-only
//! Mature HTTP/1 transport around the retained Bridge and Ingress owners.
//! One exchange per connection; response departure never owns OBS lifetime.
#[path = "http_tcp_head.rs"]
mod head;
#[path = "http_tcp_io.rs"]
mod prefix;
use super::{Bridge, ClientResponse, http_sink::Packet};
use bytes::Bytes;
use futures_util::Stream;
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{
    Request, Response, StatusCode, body::Incoming, server::conn::http1, service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use obs_mcp_native::{
    http_ingress::{self, Connection, Connections, Kind},
    http_protocol::{self as protocol, Policy},
    stage_transport::Cancellation,
};
use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
type WireBody = UnsyncBoxBody<Bytes, io::Error>;
const MAX_CONNECTIONS: usize =
    http_ingress::ORDINARY_CONNECTIONS + http_ingress::EMERGENCY_CONNECTIONS;
// Conservative APPLICATION head precharge, independent of body/output pools:
// 1 raw slab +2 projected/exact-join storage +2 mature URI scratch +3 bounded
// policy/refusal encoding copies. Head/ref vector metadata is count-bounded.
// Arc<Head> is immutable and shared with queued actor handoff (never cloned).
// Hyper's16KiB connection/read buffers and kernel/allocator overhead are separate;
// this is an application custody allowance, never an RSS measurement.
const HEAD_CUSTODY: usize = 8 * protocol::MAX_HEADER_BYTES
    + protocol::MAX_HEADERS
        * (std::mem::size_of::<(String, String)>() + std::mem::size_of::<(&str, &str)>())
    + std::mem::size_of::<head::Head>()
    + 512;

#[derive(Clone, Copy)]
pub(super) struct Bounds {
    read: Duration,
    write: Duration,
    shutdown: Duration,
}
impl Default for Bounds {
    fn default() -> Self {
        Self {
            read: Duration::from_secs(5),
            write: Duration::from_secs(5),
            shutdown: Duration::from_secs(5),
        }
    }
}
#[derive(Default)]
struct Stats {
    accepted: AtomicUsize,
    drivers: AtomicUsize,
    bridge_opens: AtomicUsize,
    refused: AtomicUsize,
}
enum Command {
    Open {
        head: Arc<head::Head>,
        body: http_ingress::Body,
        _head: Arc<OwnedSemaphorePermit>,
        reply: oneshot::Sender<io::Result<ClientResponse>>,
    },
    Immediate {
        response: protocol::Response,
        _head: Arc<OwnedSemaphorePermit>,
        reply: oneshot::Sender<io::Result<ClientResponse>>,
    },
}
#[derive(Clone)]
struct Handle {
    commands: mpsc::Sender<Command>,
    policy: Arc<Policy>,
    closing: Cancellation,
    stats: Arc<Stats>,
}
impl Handle {
    async fn open(
        &self,
        head: Arc<head::Head>,
        body: http_ingress::Body,
        head_credit: Arc<OwnedSemaphorePermit>,
    ) -> io::Result<ClientResponse> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .try_send(Command::Open {
                head,
                body,
                _head: head_credit,
                reply: tx,
            })
            .map_err(|_| io::Error::other("Native HTTP handoff capacity"))?;
        rx.await
            .map_err(|_| io::Error::other("Native HTTP host ended"))?
    }
    async fn immediate(
        &self,
        response: protocol::Response,
        head_credit: Arc<OwnedSemaphorePermit>,
    ) -> io::Result<ClientResponse> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .try_send(Command::Immediate {
                response,
                _head: head_credit,
                reply: tx,
            })
            .map_err(|_| io::Error::other("Native HTTP reply capacity"))?;
        rx.await
            .map_err(|_| io::Error::other("Native HTTP host ended"))?
    }
}
async fn actor(
    mut bridge: Bridge,
    mut commands: mpsc::Receiver<Command>,
    handle: Handle,
) -> io::Result<()> {
    let terminal = loop {
        let work = bridge.has_jobs();
        let tick = bridge.next_tick();
        tokio::select! {biased;
            ()=handle.closing.cancelled()=>break Ok(()),
            command=commands.recv()=>match command{
                Some(Command::Immediate{response,_head,reply})=>{let result=bridge.immediate(response).map(|opened|opened.response);let _=reply.send(result);},
                Some(Command::Open{head,body,_head,reply})=>{
                    let headers=head.borrowed();
                    let result=body.handoff(|bytes,kind,_deadline|{
                        handle.stats.bridge_opens.fetch_add(1,Ordering::SeqCst);
                        match kind{Kind::Ordinary=>bridge.open(head.request(&headers),bytes),Kind::Emergency=>bridge.open_emergency(head.request(&headers),bytes)}
                    }).map_err(|_|io::Error::other("Native HTTP original read deadline/custody ended")).and_then(|result|result);
                    match result{
                        Ok(mut opened)=>{let _=reply.send(Ok(opened.response));if let Some(submission)=opened.submission.take() && let Err(error)=bridge.submit(submission).await{break Err(error);}},
                        Err(error)=>{let _=reply.send(Err(error));},
                    }
                },
                None=>break Ok(()),
            },
            result=bridge.complete_next(),if work=>{if let Err(error)=result{break Err(error);}},
            ()=tokio::time::sleep_until(tick)=>bridge.tick(Instant::now()),
        }
    };
    // Every actor exit retains the same Host until joined shutdown. Unsubmitted
    // bodies are released as not dispatched before waiting for physical jobs.
    handle.closing.cancel();
    commands.close();
    while commands.try_recv().is_ok() {}
    let closed = bridge.close().await;
    terminal.and(closed)
}
struct PacketBody {
    first: Option<Packet>,
    response: ClientResponse,
    ended: bool,
    empty: bool,
    _head: Arc<OwnedSemaphorePermit>,
    _connection: Arc<Connection>,
}
impl HttpBody for PacketBody {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        let next = if let Some(first) = this.first.take() {
            Poll::Ready(Some(first))
        } else {
            Pin::new(&mut this.response).poll_next(cx)
        };
        match next {
            Poll::Ready(Some(packet)) => Poll::Ready(Some(Ok(Frame::data(packet.into_bytes())))),
            Poll::Ready(None) => {
                this.ended = true;
                if this.response.failure().is_some() {
                    Poll::Ready(Some(Err(io::Error::other("Native HTTP publication ended"))))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn size_hint(&self) -> SizeHint {
        let mut hint = SizeHint::new();
        if self.empty {
            hint.set_exact(0);
        }
        hint
    }
}
fn fixed(status: StatusCode, message: &'static str) -> Response<WireBody> {
    let mut response = Response::new(
        Full::new(Bytes::from_static(message.as_bytes()))
            .map_err(|never| match never {})
            .boxed_unsync(),
    );
    *response.status_mut() = status;
    response
}
async fn owned_response(
    mut response: ClientResponse,
    head: Arc<OwnedSemaphorePermit>,
    connection: Arc<Connection>,
) -> Response<WireBody> {
    let Some(first) = response.next().await else {
        return fixed(
            StatusCode::SERVICE_UNAVAILABLE,
            "Native HTTP response ended",
        );
    };
    let metadata = first.response();
    let status = metadata.status;
    let content_type = metadata.content_type;
    let bearer = metadata.authenticate_bearer;
    let body = PacketBody {
        first: Some(first),
        response,
        ended: false,
        empty: matches!(status, 202 | 204),
        _head: head,
        _connection: connection,
    }
    .boxed_unsync();
    let mut result = Response::new(body);
    *result.status_mut() =
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    if let Some(kind) = content_type {
        result.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static(kind),
        );
    }
    if bearer {
        result.headers_mut().insert(
            hyper::header::WWW_AUTHENTICATE,
            hyper::header::HeaderValue::from_static("Bearer"),
        );
    }
    result
}
async fn exchange(
    request: Request<Incoming>,
    facts: Arc<head::Parsed>,
    used: Arc<AtomicBool>,
    connection: Arc<Connection>,
    head_credit: Arc<OwnedSemaphorePermit>,
    deadline: Instant,
    handle: Handle,
) -> Result<Response<WireBody>, std::convert::Infallible> {
    if used.swap(true, Ordering::AcqRel) {
        return Ok(fixed(
            StatusCode::BAD_REQUEST,
            "One HTTP exchange per connection",
        ));
    }
    let head = match facts.as_ref() {
        head::Parsed::Complete(head) => head,
        head::Parsed::Refuse(reason) => {
            handle.stats.refused.fetch_add(1, Ordering::SeqCst);
            return Ok(match reason {
                head::Refusal::Capacity => fixed(
                    StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                    "Native HTTP header capacity",
                ),
                head::Refusal::Expect => fixed(
                    StatusCode::EXPECTATION_FAILED,
                    "Native HTTP Expect form is unqualified; no dispatch",
                ),
                head::Refusal::Syntax => fixed(StatusCode::BAD_REQUEST, "Invalid HTTP framing"),
                head::Refusal::Unsupported => fixed(
                    StatusCode::BAD_REQUEST,
                    "Native HTTP header/URL form is unqualified; no dispatch",
                ),
            });
        }
        head::Parsed::Partial => return Ok(fixed(StatusCode::BAD_REQUEST, "Incomplete HTTP head")),
    };
    if request.method().as_str() != head.method || request.uri().to_string() != head.target {
        return Ok(fixed(
            StatusCode::BAD_REQUEST,
            "HTTP parser facts disagree; no dispatch",
        ));
    }
    let headers = head.borrowed();
    let borrowed = head.request(&headers);
    if let Err(reason) = protocol::preflight(&borrowed, &handle.policy) {
        handle.stats.refused.fetch_add(1, Ordering::SeqCst);
        return Ok(match reason {
            protocol::HeadRefusal::Reply(response) => {
                match handle.immediate(response, head_credit.clone()).await {
                    Ok(response) => owned_response(response, head_credit, connection).await,
                    Err(_) => fixed(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Native HTTP reply admission",
                    ),
                }
            }
            protocol::HeadRefusal::Unqualified(_) => fixed(
                StatusCode::BAD_REQUEST,
                "Native HTTP header/URL form is unqualified; no dispatch",
            ),
        });
    }
    let body = request.into_body();
    let length = body
        .size_hint()
        .exact()
        .and_then(|n| usize::try_from(n).ok());
    let mut data = body.into_data_stream();
    let body = match connection
        .collect(&borrowed, &handle.policy, length, deadline, &mut data)
        .await
    {
        Ok(body) => body,
        Err(reason) => {
            handle.stats.refused.fetch_add(1, Ordering::SeqCst);
            return Ok(match reason {
                http_ingress::Failure::TooLarge => {
                    fixed(StatusCode::PAYLOAD_TOO_LARGE, "Native HTTP body capacity")
                }
                http_ingress::Failure::Deadline => fixed(
                    StatusCode::REQUEST_TIMEOUT,
                    "Native HTTP original read deadline",
                ),
                http_ingress::Failure::Capacity => fixed(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Native HTTP body admission",
                ),
                _ => fixed(
                    StatusCode::BAD_REQUEST,
                    "Native HTTP body ended; no dispatch",
                ),
            });
        }
    };
    let owned = Arc::clone(head);
    // The queued actor creates its own bounded borrowed view, not a second
    // live borrowed-vector allocation retained by this observer future.
    drop(headers);
    Ok(match handle.open(owned, body, head_credit.clone()).await {
        Ok(response) => owned_response(response, head_credit, connection).await,
        Err(_) => fixed(
            StatusCode::SERVICE_UNAVAILABLE,
            "Native HTTP exchange admission; no dispatch",
        ),
    })
}
async fn drive(
    mut stream: TcpStream,
    connection: Connection,
    handle: Handle,
    head_pool: Arc<Semaphore>,
    accepted: Instant,
    bounds: Bounds,
) -> Result<(), http_ingress::Failure> {
    handle.stats.drivers.fetch_add(1, Ordering::SeqCst);
    struct DriverCount(Arc<Stats>);
    impl Drop for DriverCount {
        fn drop(&mut self) {
            self.0.drivers.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let _count = DriverCount(handle.stats.clone());
    let head_credit = Arc::new(
        head_pool
            .try_acquire_many_owned(HEAD_CUSTODY as u32)
            .map_err(|_| http_ingress::Failure::Capacity)?,
    );
    let deadline = accepted + bounds.read;
    let mut prefix = Vec::with_capacity(protocol::MAX_HEADER_BYTES);
    let facts = loop {
        match head::parse(&prefix) {
            head::Parsed::Partial => {}
            complete => break complete,
        }
        if prefix.len() >= protocol::MAX_HEADER_BYTES {
            break head::Parsed::Refuse(head::Refusal::Capacity);
        }
        let mut slab = [0u8; 4096];
        let available = slab.len().min(protocol::MAX_HEADER_BYTES - prefix.len());
        let n = tokio::select! {biased;
            ()=connection.shutdown_requested()=>return Ok(()),
            ()=handle.closing.cancelled()=>return Ok(()),
            ()=tokio::time::sleep_until(deadline)=>return Ok(()),
            n=stream.read(&mut slab[..available])=>n.map_err(|_|http_ingress::Failure::Transport)?,
        };
        if n == 0 {
            return Ok(());
        }
        prefix.extend_from_slice(&slab[..n]);
        tokio::task::consume_budget().await;
    };
    let connection = Arc::new(connection);
    let owner = connection.clone();
    let facts = Arc::new(facts);
    let used = Arc::new(AtomicBool::new(false));
    let callback_handle = handle.clone();
    let callback = service_fn(move |request| {
        exchange(
            request,
            facts.clone(),
            used.clone(),
            owner.clone(),
            head_credit.clone(),
            deadline,
            callback_handle.clone(),
        )
    });
    let io = TokioIo::new(prefix::PrefixIo::new(stream, prefix, bounds.write));
    let mut builder = http1::Builder::new();
    builder
        .keep_alive(false)
        .half_close(false)
        .max_headers(protocol::MAX_HEADERS)
        .max_buf_size(protocol::MAX_HEADER_BYTES)
        .writev(true)
        .timer(TokioTimer::new())
        .header_read_timeout(bounds.read)
        .auto_date_header(false);
    let future = builder.serve_connection(io, callback);
    tokio::pin!(future);
    tokio::select! {biased;
        ()=handle.closing.cancelled()=>{future.as_mut().graceful_shutdown();let _=tokio::time::timeout_at(Instant::now()+bounds.shutdown,&mut future).await;},
        ()=connection.shutdown_requested()=>{future.as_mut().graceful_shutdown();let _=tokio::time::timeout_at(Instant::now()+bounds.shutdown,&mut future).await;},
        _=&mut future=>{},
    }
    // Parser/peer refusals are connection-local; they do not poison the Host.
    Ok(())
}
pub(super) struct Running {
    port: u16,
    handle: Handle,
    joined: JoinHandle<io::Result<()>>,
}
impl Running {
    pub(super) fn port(&self) -> u16 {
        self.port
    }
    pub(super) async fn stopped(&self) {
        self.handle.closing.cancelled().await;
    }
    pub(super) async fn start(
        bridge: Bridge,
        policy: Policy,
        bounds: Bounds,
        port: u16,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
        let port = listener.local_addr()?.port();
        let (commands, receiver) = mpsc::channel(MAX_CONNECTIONS);
        let closing = Cancellation::default();
        let stats = Arc::new(Stats::default());
        let handle = Handle {
            commands,
            closing: closing.clone(),
            policy: Arc::new(policy),
            stats,
        };
        let actor_handle = handle.clone();
        let actor = tokio::spawn(actor(bridge, receiver, actor_handle));
        let listen_handle = handle.clone();
        let head_pool = Arc::new(Semaphore::new(MAX_CONNECTIONS * HEAD_CUSTODY));
        let joined = tokio::spawn(async move {
            let mut connections = Connections::new();
            let terminal = loop {
                tokio::select! {biased;
                    ()=closing.cancelled()=>break Ok(()),
                    accept=listener.accept()=>{let(stream,_)=match accept{Ok(pair)=>pair,Err(error)=>break Err(error)};let accepted=Instant::now();let handle=listen_handle.clone();let pool=head_pool.clone();listen_handle.stats.accepted.fetch_add(1,Ordering::SeqCst);let _=connections.spawn(move|connection|drive(stream,connection,handle,pool,accepted,bounds));},
                }
            };
            closing.cancel();
            drop(listener);
            let (host, transport) = tokio::join!(actor, connections.shutdown());
            host.map_err(|_| io::Error::other("Native HTTP host owner panicked"))??;
            transport.map_err(|_| io::Error::other("Native HTTP drivers failed to join"))?;
            if head_pool.available_permits() != MAX_CONNECTIONS * HEAD_CUSTODY {
                return Err(io::Error::other("Native HTTP head custody did not drain"));
            }
            terminal
        });
        Ok(Self {
            port,
            handle,
            joined,
        })
    }
    pub(super) async fn close(self) -> io::Result<()> {
        self.handle.closing.cancel();
        self.joined
            .await
            .map_err(|_| io::Error::other("Native HTTP server owner panicked"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn queued_head_custody_survives_abandoned_http_observer_until_actual_queue_drop() {
        let pool = Arc::new(Semaphore::new(HEAD_CUSTODY));
        let permit = Arc::new(
            pool.clone()
                .try_acquire_many_owned(HEAD_CUSTODY as u32)
                .unwrap(),
        );
        let (tx, rx) = oneshot::channel();
        let (sender, receiver) = mpsc::channel(1);
        sender
            .try_send(Command::Immediate {
                response: protocol::Response {
                    status: 400,
                    content_type: None,
                    authenticate_bearer: false,
                    body: Vec::new(),
                },
                _head: permit.clone(),
                reply: tx,
            })
            .ok()
            .unwrap();
        drop(permit);
        drop(rx);
        assert_eq!(pool.available_permits(), 0);
        drop(receiver);
        assert_eq!(pool.available_permits(), HEAD_CUSTODY);
    }
    #[test]
    #[ignore = "owned loopback TCP SDK/raw-wire fixture; no product listener"]
    fn probe_child() {
        if std::env::var("NATIVE_HTTP_TCP_PROBE").as_deref() != Ok("1") {
            return;
        }
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async{
            use super::super::super::{Runtime,ToolFilter,admitted_clock};
            use obs_mcp_native::{control::ControlDomain,native_profile::NativeProfile,resource_events::Owner,scene_audit::ScreenshotLimit,service::PreparationOwners};
            use std::sync::Mutex;use tokio::io::AsyncBufReadExt;
            let root=std::path::PathBuf::from(std::env::var_os("NATIVE_HTTP_TCP_STATE").unwrap());
            let runtime=Runtime{profile:NativeProfile::Production,notifications:Owner::new(false),owner:None,initialization:None,initialization_broken:false,network:None,url:std::env::var("NATIVE_HTTP_TCP_OBS").unwrap(),password:None,root:root.clone(),host:std::env::var_os("NATIVE_HTTP_TCP_RECORDING_ROOT").map(|path|obs_mcp_native::take_transport::LocalHost::new(&std::path::PathBuf::from(path),None,None).map(Arc::new).unwrap()),control:Arc::new(Mutex::new(ControlDomain::default())),clock:admitted_clock(obs_mcp_native::clock::snapshot).unwrap(),preparation:Some(PreparationOwners::new(&root).unwrap()),media:None,media_work:None,screenshot_limit:Ok(ScreenshotLimit::default()),confirm_live:std::env::var("NATIVE_HTTP_TCP_CONFIRM").as_deref()==Ok("1")};
            let token=std::env::var("NATIVE_HTTP_TCP_TOKEN").ok();let bridge=Bridge::new(runtime,ToolFilter::parse(|name|(name=="OBS_MCP_TOOLSETS").then(||"all".into()),crate::schema::catalog()).unwrap(),Policy::new(token.as_deref())).unwrap();
            let write=std::env::var("NATIVE_HTTP_TCP_WRITE_MS").ok().and_then(|n|n.parse::<u64>().ok()).unwrap_or(5000);let read=std::env::var("NATIVE_HTTP_TCP_READ_MS").ok().and_then(|n|n.parse::<u64>().ok()).unwrap_or(5000);let server=Running::start(bridge,Policy::new(token.as_deref()),Bounds{read:Duration::from_millis(read),write:Duration::from_millis(write),..Bounds::default()},0).await.unwrap();
            fn send(value:serde_json::Value){use std::io::Write;let mut out=std::io::stdout().lock();out.write_all(b"HTTP_TCP_PROBE ").unwrap();serde_json::to_writer(&mut out,&value).unwrap();out.write_all(b"\n").unwrap();out.flush().unwrap();}
            send(serde_json::json!({"ready":true,"port":server.port(),"headCustodyBytes":HEAD_CUSTODY}));let mut lines=tokio::io::BufReader::new(tokio::io::stdin()).lines();
            while let Some(line)=lines.next_line().await.unwrap(){if line=="shutdown"{break;}else if line=="stats"{send(serde_json::json!({"accepted":server.handle.stats.accepted.load(Ordering::SeqCst),"drivers":server.handle.stats.drivers.load(Ordering::SeqCst),"bridgeOpens":server.handle.stats.bridge_opens.load(Ordering::SeqCst),"refused":server.handle.stats.refused.load(Ordering::SeqCst)}));}}
            server.handle.closing.cancel();server.stopped().await;server.close().await.unwrap();send(serde_json::json!({"joined":true}));
        });
    }
}
