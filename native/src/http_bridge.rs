// SPDX-License-Identifier: GPL-2.0-only
//! Exchange owner shared by the mature TCP driver and in-memory controls.
//! TCP parsing, framing, bounded I/O and joins belong to the transport.
#[path = "http_resources.rs"]
mod http_resources;
#[path = "http_sink.rs"]
mod http_sink;
#[path = "http_tcp.rs"]
mod http_tcp;

use super::{
    Runtime, confirmed_manifest, inventory,
    native_host::{self, Class, Frontend, FrontendId, Host, Incoming},
    resource_request,
};
use crate::{
    protocol::{self, Admission},
    tool_filter::ToolFilter,
};
use http_resources::{Lease, Resources as ResourceStreams};
use http_sink::{Budget, Packet, Sender};
use obs_mcp_native::{
    http_protocol::{
        self as http, ErrorOrigin, Exchange, Policy, RequestHead, Response, ResponseContext,
        ResponseMode,
    },
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
pub(super) async fn run(
    runtime: Runtime,
    filter: ToolFilter,
    port: u16,
    token: Option<String>,
) -> io::Result<()> {
    let bridge = Bridge::new(runtime, filter, Policy::new(token.as_deref()))?;
    let server = http_tcp::Running::start(
        bridge,
        Policy::new(token.as_deref()),
        http_tcp::Bounds::default(),
        port,
    )
    .await?;
    eprintln!(
        "Serving MCP over HTTP at http://127.0.0.1:{}/mcp{}",
        server.port(),
        if token.is_some() {
            " (bearer token required)"
        } else {
            ""
        },
    );
    // Frontend departure and stdin EOF cannot terminate the shared owner.
    // Every normal/error exit closes admission and joins Host and TCP drivers.
    tokio::select! {
        () = crate::shutdown_signal() => {},
        () = server.stopped() => {},
    }
    server.close().await
}

const HEARTBEAT: Duration = Duration::from_secs(15);
struct Route {
    context: ResponseContext,
    sink: Arc<Sender>,
    remaining: usize,
}
pub(super) struct ClientResponse {
    receiver: http_sink::Receiver,
    _frontend: Option<Frontend>,
    cleanup: Arc<Mutex<Option<Lease>>>,
}
impl ClientResponse {
    pub(super) async fn next(&mut self) -> Option<Packet> {
        self.receiver.next().await
    }
    pub(super) fn failure(&self) -> Option<http_sink::Failure> {
        self.receiver.failure()
    }
}
impl futures_util::Stream for ClientResponse {
    type Item = Packet;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().receiver.poll_next(cx)
    }
}
impl Drop for ClientResponse {
    fn drop(&mut self) {
        let lease = self.cleanup.lock().ok().and_then(|mut lease| lease.take());
        if let Some(lease) = lease {
            lease.cancel();
        }
    }
}
// These fields retain admission until the submission is dropped. Reading the
// permit is unnecessary; replacing it with a unit field would refund it early.
enum BodyCredits {
    Ordinary { _permit: OwnedSemaphorePermit },
    Stop { _lease: http_sink::StopLease },
}
pub(super) struct Submission {
    frontend: FrontendId,
    items: Vec<Exchange>,
    _body: BodyCredits,
}
pub(super) struct Opened {
    pub(super) response: ClientResponse,
    pub(super) submission: Option<Submission>,
}
pub(super) struct Bridge {
    host: Host,
    filter: ToolFilter,
    policy: Policy,
    budget: Budget,
    streams: Arc<ResourceStreams>,
    routes: HashMap<FrontendId, Route>,
    cleanups: HashMap<FrontendId, Arc<Mutex<Option<Lease>>>>,
    heartbeat: Instant,
    closing: bool,
    input_bytes: Arc<Semaphore>,
}
impl Bridge {
    pub(super) fn new(runtime: Runtime, filter: ToolFilter, policy: Policy) -> io::Result<Self> {
        let host = Host::from_runtime(runtime, false)?;
        let streams =
            ResourceStreams::bind(&host.runtime.notifications).map_err(io::Error::other)?;
        Ok(Self {
            host,
            filter,
            policy,
            budget: Budget::new(),
            streams,
            routes: HashMap::new(),
            cleanups: HashMap::new(),
            heartbeat: Instant::now() + HEARTBEAT,
            closing: false,
            input_bytes: Arc::new(Semaphore::new(32 * 1024 * 1024)),
        })
    }
    fn immediate(&self, response: Response) -> io::Result<Opened> {
        let (sender, receiver) = Sender::channel(&self.budget, false, Cancellation::new());
        sender
            .send(response)
            .map_err(|_| io::Error::other("Native HTTP publication unavailable"))?;
        Ok(Opened {
            response: ClientResponse {
                receiver,
                _frontend: None,
                cleanup: Arc::new(Mutex::new(None)),
            },
            submission: None,
        })
    }
    /// Opening and submission are separate ownership transfers. The transport
    /// receives its departure guard before any lazy connection can await.
    pub(super) fn open(&mut self, head: RequestHead<'_>, body: Vec<u8>) -> io::Result<Opened> {
        self.open_mode(head, body, false)
    }
    /// Only the transport's actual emergency reader lease may select this
    /// path; no wire flag can promote an ordinary call or batch into a Stop.
    pub(super) fn open_emergency(
        &mut self,
        head: RequestHead<'_>,
        body: Vec<u8>,
    ) -> io::Result<Opened> {
        self.open_mode(head, body, true)
    }
    fn open_mode(
        &mut self,
        head: RequestHead<'_>,
        body: Vec<u8>,
        emergency: bool,
    ) -> io::Result<Opened> {
        if emergency && body.len() > http_sink::STOP_BODY_BYTES {
            return Err(io::Error::other(
                "Emergency HTTP reader body capacity exceeded; no dispatch",
            ));
        }
        if self.closing {
            return Err(io::Error::other("Native HTTP host is closing"));
        }
        self.reap_departed();
        let n = u32::try_from(body.len())
            .map_err(|_| io::Error::other("Native HTTP body admission unavailable"))?;
        // The mature transport already owns this bounded, syntax-validated
        // body. A failed ordinary reservation can only proceed synchronously
        // far enough to qualify a small typed Stop; it grants no effect rights.
        let ordinary_credit = self.input_bytes.clone().try_acquire_many_owned(n).ok();
        let exchange=match http::admit(head,body,&self.policy){
            http::Admission::Reply(response)=>return self.immediate(response),
            http::Admission::Unqualified(reason)=>return self.immediate(Response{status:400,content_type:Some("application/json"),authenticate_bearer:false,body:serde_json::to_vec(&json!({"jsonrpc":"2.0","id":null,"error":{"code":-32000,"message":format!("Native HTTP exchange form is unavailable ({reason:?}); no dispatch")}})).expect("fixed refusal")}),
            http::Admission::Exchange(exchange)=>exchange,
        };
        if exchange.response_mode() == ResponseMode::Empty {
            return self.immediate(exchange.respond(&json!({}), ErrorOrigin::Protocol));
        }
        let context = exchange.response_context();
        let stream = exchange.response_mode() == ResponseMode::Listen;
        let class = self.reserved_class(&exchange);
        if emergency && matches!(class, Class::Ordinary) {
            return Err(io::Error::other(
                "Emergency HTTP reader did not qualify a typed Stop; no dispatch",
            ));
        }
        let stop = self
            .budget
            .reserve_stop(class, n as usize)
            .map_err(|_| io::Error::other("Native HTTP typed Stop byte reservation unavailable"))?;
        let body_credit = if let Some(lease) = &stop {
            drop(ordinary_credit);
            BodyCredits::Stop {
                _lease: lease.clone(),
            }
        } else {
            BodyCredits::Ordinary {
                _permit: ordinary_credit
                    .ok_or_else(|| io::Error::other("Native HTTP body admission unavailable"))?,
            }
        };
        let items = exchange
            .into_items()
            .map_err(|_| io::Error::other("Native original batch spans unavailable"))?;
        let remaining = items
            .iter()
            .filter(|item| item.response_mode() != ResponseMode::Empty)
            .count();
        let frontend = self
            .host
            .frontend_for(class)
            .map_err(|_| io::Error::other("Native HTTP frontend admission unavailable"))?;
        let id = frontend.id();
        let (sender, receiver) =
            Sender::channel_reserved(&self.budget, stream, frontend.departure(), stop);
        let cleanup = Arc::new(Mutex::new(None));
        self.routes.insert(
            id,
            Route {
                context,
                sink: sender,
                remaining,
            },
        );
        self.cleanups.insert(id, cleanup.clone());
        Ok(Opened {
            response: ClientResponse {
                receiver,
                _frontend: Some(frontend),
                cleanup,
            },
            submission: Some(Submission {
                frontend: id,
                items,
                _body: body_credit,
            }),
        })
    }
    fn reserved_class(&self, exchange: &Exchange) -> Class {
        if exchange.original_body().len() > http_sink::STOP_BODY_BYTES
            || exchange.message().is_array()
        {
            return Class::Ordinary;
        }
        let Admission::Call {
            name, arguments, ..
        } = protocol::admit_http(exchange, self.host.runtime.confirm_live)
        else {
            return Class::Ordinary;
        };
        let class = Class::for_tool(name, false);
        if matches!(class, Class::Ordinary)
            || !self.filter.includes(name)
            || !super::owned(
                name,
                self.host.runtime.host.is_some(),
                self.host.runtime.media.is_some(),
                self.host.runtime.profile,
            )
            || crate::schema::service_arguments(name, &arguments).is_none()
            || (self.host.runtime.confirm_live
                && super::confirmation::eligible(name)
                && arguments
                    .get("confirm")
                    .is_some_and(|value| !value.is_boolean()))
        {
            return Class::Ordinary;
        }
        class
    }
    fn reap_departed(&mut self) {
        let departed = self
            .routes
            .iter()
            .filter_map(|(id, route)| (!route.sink.available()).then_some(*id))
            .collect::<Vec<_>>();
        for frontend in departed {
            self.host.disconnect(frontend);
            if self
                .routes
                .get(&frontend)
                .is_some_and(|route| route.sink.reserved_stop())
                && self.host.has_frontend_jobs(frontend)
            {
                // Cancellation suppresses a reply, never refunds an admitted
                // Stop's byte custody while its retained job is still running.
                continue;
            }
            self.routes.remove(&frontend);
            self.cleanups.remove(&frontend);
            let _ = self.host.retire_frontend(frontend);
        }
    }
    fn emit(&mut self, frontend: FrontendId, payload: Value, origin: ErrorOrigin) {
        let Some(route) = self.routes.get_mut(&frontend) else {
            return;
        };
        if let Err(reason) = route.sink.send(route.context.respond(&payload, origin)) {
            route.sink.fail(reason);
            self.host.disconnect(frontend);
        }
        route.remaining = route.remaining.saturating_sub(1);
        if route.remaining == 0 || !route.sink.available() {
            self.routes.remove(&frontend);
            self.cleanups.remove(&frontend);
            let _ = self.host.retire_frontend(frontend);
        }
    }
    fn silent(&mut self, frontend: FrontendId) {
        let Some(route) = self.routes.get_mut(&frontend) else {
            return;
        };
        route.remaining = route.remaining.saturating_sub(1);
        if route.remaining == 0 {
            self.routes.remove(&frontend);
            self.cleanups.remove(&frontend);
            let _ = self.host.retire_frontend(frontend);
        }
    }
    fn unavailable(
        &mut self,
        frontend: FrontendId,
        item: &Exchange,
        code: i64,
        message: &str,
        origin: ErrorOrigin,
    ) {
        let id = item.message().get("id").cloned().unwrap_or(Value::Null);
        self.emit(
            frontend,
            json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}}),
            origin,
        );
    }
    /// The one server loop retains this future; a response task must never own
    /// or drop it. Client departure travels through the independent guard.
    pub(super) async fn submit(&mut self, submission: Submission) -> io::Result<()> {
        let frontend = submission.frontend;
        self.host.confirmations.expire(Instant::now());
        for item in submission.items {
            if !self
                .routes
                .get(&frontend)
                .is_some_and(|route| route.sink.available())
            {
                self.host.disconnect(frontend);
                self.routes.remove(&frontend);
                self.cleanups.remove(&frontend);
                break;
            }
            if item.response_mode() == ResponseMode::Empty {
                // Cancellation in THIS legacy batch may target its own calls.
                // A separate POST has a distinct frontend and cannot cancel peers.
                if item.message()["method"] == "notifications/cancelled"
                    && let Some(id) = item.message().pointer("/params/requestId")
                {
                    self.host.admissions.cancel(frontend, id);
                }
                continue;
            }
            let admitted = protocol::admit_http(&item, self.host.runtime.confirm_live);
            let (reply, name, args, resource) = match admitted {
                Admission::Reply(response) => {
                    self.emit(frontend, response, ErrorOrigin::Protocol);
                    continue;
                }
                Admission::Initialized => continue,
                Admission::List(reply) => {
                    let manifest = confirmed_manifest(
                        inventory(
                            &self.filter,
                            self.host.runtime.host.is_some(),
                            self.host.runtime.media.is_some(),
                            self.host.runtime.profile,
                        ),
                        self.host.runtime.confirm_live,
                    );
                    self.emit(
                        frontend,
                        reply.complete(manifest, true),
                        ErrorOrigin::Handler,
                    );
                    continue;
                }
                Admission::Listen {
                    reply,
                    notifications,
                    valid_filter,
                } => {
                    let cleanup = self
                        .cleanups
                        .get(&frontend)
                        .expect("opened cleanup")
                        .clone();
                    let sink = self
                        .routes
                        .get(&frontend)
                        .expect("opened route")
                        .sink
                        .clone();
                    match self.streams.install(
                        &self.host.runtime.notifications,
                        http_resources::Listen {
                            frontend,
                            reply: &reply.owned(),
                            filter: notifications,
                            valid: valid_filter,
                        },
                        sink,
                        &cleanup,
                    ) {
                        Ok(()) => {
                            self.routes.remove(&frontend);
                            self.cleanups.remove(&frontend);
                            let _ = self.host.retire_frontend(frontend);
                        }
                        Err(response) => self.emit(frontend, response, ErrorOrigin::Handler),
                    }
                    continue;
                }
                Admission::ResourceSubscription { reply, uri, .. } => {
                    // Source HTTP creates no legacy session Set or notifications.
                    let response = match uri {
                        Ok(_) => reply.complete(json!({}), false),
                        Err(message) => reply.owned().error(-32603, &message),
                    };
                    self.emit(frontend, response, ErrorOrigin::Handler);
                    continue;
                }
                Admission::ResourceList(reply) => (
                    reply.owned(),
                    "native-resource-list".into(),
                    json!({}),
                    true,
                ),
                Admission::ResourceCompletion { reply, prefix } => (
                    reply.owned(),
                    "native-resource-completion".into(),
                    json!({"prefix":prefix}),
                    true,
                ),
                Admission::ResourceRead {
                    reply,
                    uri: "obs://take/current",
                } => {
                    if self.host.runtime.owner.is_none() {
                        self.emit(frontend,reply.complete(json!({"contents":[{"uri":"obs://take/current","mimeType":"application/json","text":"{\n  \"running\": false\n}"}]}),true),ErrorOrigin::Handler);
                        continue;
                    }
                    (
                        reply.owned(),
                        "native-take-resource".into(),
                        json!({}),
                        true,
                    )
                }
                Admission::ResourceRead {
                    reply,
                    uri: "obs://status",
                } => (reply.owned(), "obs-observe".into(), json!({}), true),
                Admission::ResourceRead { reply, uri } => {
                    if let Err(reason) = resource_request(uri) {
                        self.emit(
                            frontend,
                            reply.owned().error(reason.code, &reason.message),
                            ErrorOrigin::Handler,
                        );
                        continue;
                    }
                    (
                        reply.owned(),
                        "native-resource".into(),
                        json!({"uri":uri}),
                        true,
                    )
                }
                Admission::Call {
                    reply,
                    name,
                    arguments,
                } => (reply.owned(), name.to_owned(), arguments, false),
                Admission::Compatibility => {
                    self.unavailable(
                        frontend,
                        &item,
                        -32601,
                        "Method not found",
                        ErrorOrigin::Protocol,
                    );
                    continue;
                }
            };
            if !resource && !self.filter.includes(&name) {
                self.emit(
                    frontend,
                    reply.error(-32602, &format!("Tool {name} not found")),
                    ErrorOrigin::Handler,
                );
                continue;
            }
            if !resource
                && !super::owned(
                    &name,
                    self.host.runtime.host.is_some(),
                    self.host.runtime.media.is_some(),
                    self.host.runtime.profile,
                )
            {
                self.emit(
                    frontend,
                    reply.error(
                        -32601,
                        "Tool is unavailable in the native-only qualification lane",
                    ),
                    ErrorOrigin::Handler,
                );
                continue;
            }
            let (prepared, answers) = if self.host.runtime.confirm_live
                && (item.message().pointer("/params/requestState").is_some()
                    || item.message().pointer("/params/inputResponses").is_some())
            {
                match self.host.resume(
                    frontend,
                    reply,
                    &name,
                    item.message(),
                    item.legacy_declared_client(),
                ) {
                    Ok((call, answer)) => (call, Some(answer)),
                    Err(response) => {
                        self.emit(frontend, response, ErrorOrigin::Handler);
                        continue;
                    }
                }
            } else {
                match self.host.prepare(
                    frontend,
                    Incoming {
                        reply,
                        name,
                        args,
                        resource,
                        controller: false,
                        value: item.message(),
                        frame: item.original_body(),
                        legacy_name: item.legacy_declared_client(),
                        legacy_capabilities: None,
                    },
                ) {
                    Ok(call) => (call, None),
                    Err(response) => {
                        self.emit(frontend, response, ErrorOrigin::Handler);
                        continue;
                    }
                }
            };
            match self.host.authorize(prepared, answers.as_ref()) {
                Ok(invocation) => match self.host.dispatch(invocation).await {
                    Ok(Some(response)) => self.emit(frontend, response, ErrorOrigin::Handler),
                    Ok(None) => {}
                    Err(error) => return Err(error),
                },
                Err(response) => self.emit(frontend, response, ErrorOrigin::Handler),
            }
        }
        Ok(())
    }
    pub(super) fn has_jobs(&self) -> bool {
        self.host.has_work()
    }
    pub(super) async fn complete_next(&mut self) -> io::Result<()> {
        let Some(completed) = self.host.next_completed().await? else {
            return Ok(());
        };
        let finished = self
            .host
            .complete(completed)
            .ok_or_else(|| io::Error::other("Native HTTP completion ownership unavailable"))?;
        if let Some(response) = finished.response {
            self.emit(finished.frontend, response, ErrorOrigin::Handler)
        } else {
            self.silent(finished.frontend)
        }
        Ok(())
    }
    pub(super) fn next_tick(&self) -> Instant {
        self.heartbeat
    }
    pub(super) fn tick(&mut self, now: Instant) {
        self.reap_departed();
        self.host.confirmations.expire(now);
        if now >= self.heartbeat {
            self.streams.heartbeat();
            self.heartbeat = now + HEARTBEAT;
        }
    }
    pub(super) async fn close(&mut self) -> io::Result<()> {
        self.closing = true;
        self.streams.close();
        for (frontend, route) in self.routes.drain() {
            route.sink.fail(http_sink::Failure::Closed);
            self.host.disconnect(frontend);
        }
        self.cleanups.clear();
        self.host.shutdown().await
    }
}

#[cfg(test)]
use super::native_host::support;
#[cfg(test)]
mod tests {
    use super::*;
    use obs_mcp_native::{
        control::ControlDomain, observation::ReadRequest, resource_events::Owner,
        scene_audit::ScreenshotLimit, service::PreparationOwners,
    };
    fn runtime(socket: &support::Socket, dir: &support::Dir) -> Runtime {
        Runtime {
            profile: obs_mcp_native::native_profile::NativeProfile::Production,
            notifications: Owner::new(false),
            owner: None,
            initialization: None,
            initialization_broken: false,
            network: None,
            tls_ca: None,
            url: socket.url.clone(),
            password: None,
            root: dir.0.clone(),
            host: None,
            control: Arc::new(Mutex::new(ControlDomain::default())),
            clock: support::clock(),
            preparation: Some(PreparationOwners::new(&dir.0).unwrap()),
            media: None,
            replay: None,
            media_work: None,
            screenshot_limit: Ok(ScreenshotLimit::default()),
            confirm_live: false,
        }
    }
    fn bridge(socket: &support::Socket, dir: &support::Dir, confirm: bool) -> Bridge {
        let mut runtime = runtime(socket, dir);
        runtime.confirm_live = confirm;
        Bridge::new(
            runtime,
            ToolFilter::parse(
                |name| (name == "OBS_MCP_TOOLSETS").then(|| "all".into()),
                crate::schema::catalog(),
            )
            .unwrap(),
            Policy::new(None),
        )
        .unwrap()
    }
    fn frame(id: u64, method: &str, params: Value, legacy: bool) -> Value {
        let mut value = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        if !legacy {
            value["params"]["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"fixture","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}});
        }
        value
    }
    fn open(bridge: &mut Bridge, value: Value, legacy: bool) -> Opened {
        let method = value["method"].as_str().unwrap_or("");
        let mut headers = vec![
            ("host", "127.0.0.1:4455"),
            ("content-type", "application/json"),
            ("accept", "application/json, text/event-stream"),
            (
                "mcp-protocol-version",
                if legacy { "2025-11-25" } else { "2026-07-28" },
            ),
        ];
        if !legacy {
            headers.push(("mcp-method", method));
            if method == "tools/call" {
                headers.push(("mcp-name", value["params"]["name"].as_str().unwrap()));
            }
        }
        bridge
            .open(
                RequestHead {
                    method: "POST",
                    target: "/mcp?client=legacy-fixture",
                    headers: &headers,
                },
                serde_json::to_vec(&value).unwrap(),
            )
            .unwrap()
    }
    fn decode(packet: &Packet) -> Value {
        let response = packet.response();
        let bytes = if response.content_type == Some("text/event-stream") {
            let text = std::str::from_utf8(&response.body).unwrap();
            text.lines()
                .find_map(|line| line.strip_prefix("data: "))
                .unwrap()
                .as_bytes()
        } else {
            &response.body
        };
        serde_json::from_slice(bytes).unwrap()
    }
    async fn submit(bridge: &mut Bridge, mut opened: Opened) -> ClientResponse {
        if let Some(submission) = opened.submission.take() {
            bridge.submit(submission).await.unwrap();
        }
        opened.response
    }
    async fn answer(bridge: &mut Bridge, response: &mut ClientResponse) -> Packet {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(packet) = response.receiver.try_next() {
                    return packet;
                }
                if bridge.has_jobs() {
                    bridge.complete_next().await.unwrap();
                } else {
                    return response.next().await.expect("response ended");
                }
            }
        })
        .await
        .unwrap()
    }
    #[tokio::test]
    async fn stateless_eras_caller_facts_and_legacy_batch_preserve_source_shapes() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, false);
        bridge.host.runtime.connect().await.unwrap();
        for legacy in [false, true] {
            let opened = open(
                &mut bridge,
                frame(
                    7,
                    "tools/call",
                    json!({"name":"obs-control-status","arguments":{}}),
                    legacy,
                ),
                legacy,
            );
            let mut response = submit(&mut bridge, opened).await;
            let packet = answer(&mut bridge, &mut response).await;
            assert_eq!(packet.response().status, 200);
            assert_eq!(
                packet.response().content_type,
                Some(if legacy {
                    "text/event-stream"
                } else {
                    "application/json"
                })
            );
            let reply = decode(&packet);
            assert_eq!(reply["id"], 7);
            assert_eq!(
                reply["result"]["structuredContent"]["caller"],
                if legacy { "legacy-fixture" } else { "fixture" }
            );
        }
        let body=b"[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"},{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}]".to_vec();
        let headers = [
            ("host", "127.0.0.1:4455"),
            ("content-type", "application/json"),
            ("accept", "application/json, text/event-stream"),
        ];
        let opened = bridge
            .open(
                RequestHead {
                    method: "POST",
                    target: "/mcp",
                    headers: &headers,
                },
                body,
            )
            .unwrap();
        let mut response = submit(&mut bridge, opened).await;
        assert_eq!(decode(&response.next().await.unwrap())["id"], 1);
        assert_eq!(decode(&response.next().await.unwrap())["id"], 2);
        assert!(response.next().await.is_none());
        bridge.close().await.unwrap();
        socket.join().await;
    }
    #[tokio::test]
    async fn identical_listen_ids_and_mixed_eras_are_isolated_and_drop_releases_one_slot() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, false);
        bridge.host.runtime.connect().await.unwrap();
        let listen = frame(
            500,
            "subscriptions/listen",
            json!({"notifications":{"resourceSubscriptions":["obs://status"]}}),
            false,
        );
        let a = open(&mut bridge, listen.clone(), false);
        let mut a = submit(&mut bridge, a).await;
        let b = open(&mut bridge, listen, false);
        let mut b = submit(&mut bridge, b).await;
        assert_eq!(
            decode(&a.next().await.unwrap())["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
            500
        );
        assert_eq!(
            decode(&b.next().await.unwrap())["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
            500
        );
        assert_eq!(bridge.streams.count(), 2);
        let legacy = open(
            &mut bridge,
            frame(
                500,
                "resources/subscribe",
                json!({"uri":"obs://status"}),
                true,
            ),
            true,
        );
        let mut legacy = submit(&mut bridge, legacy).await;
        assert_eq!(
            decode(&answer(&mut bridge, &mut legacy).await)["result"],
            json!({})
        );
        assert!(legacy.next().await.is_none());
        assert_eq!(bridge.streams.count(), 2);
        socket
            .emit(
                "CurrentProgramSceneChanged",
                json!({"sceneName":"Changed","sceneUuid":"changed"}),
            )
            .await;
        for client in [&mut a, &mut b] {
            let packet = tokio::time::timeout(Duration::from_secs(3), client.next())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(decode(&packet)["method"], "notifications/resources/updated");
            assert_eq!(
                decode(&packet)["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
                500
            );
        }
        drop(a);
        assert_eq!(bridge.streams.count(), 1);
        socket
            .emit(
                "CurrentProgramSceneChanged",
                json!({"sceneName":"Again","sceneUuid":"again"}),
            )
            .await;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), b.next())
                .await
                .unwrap()
                .is_some()
        );
        let cancelled = frame(
            501,
            "notifications/cancelled",
            json!({"requestId":500}),
            false,
        );
        let mut cancelled = cancelled;
        cancelled.as_object_mut().unwrap().remove("id");
        let opened = open(&mut bridge, cancelled, false);
        let mut response = submit(&mut bridge, opened).await;
        assert_eq!(response.next().await.unwrap().response().status, 202);
        assert_eq!(bridge.streams.count(), 1);
        bridge.close().await.unwrap();
        drop(b);
        socket.join().await;
        assert_eq!(bridge.budget.available(), 32 * 1024 * 1024);
    }
    #[tokio::test]
    async fn one_slow_stream_overflow_does_not_poison_observer_peer_or_shared_service() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, false);
        bridge.host.runtime.connect().await.unwrap();
        let listen = frame(
            7,
            "subscriptions/listen",
            json!({"notifications":{"resourceSubscriptions":["obs://status"]}}),
            false,
        );
        let slow = open(&mut bridge, listen.clone(), false);
        let slow = submit(&mut bridge, slow).await;
        let fast = open(&mut bridge, listen, false);
        let mut fast = submit(&mut bridge, fast).await;
        drop(fast.next().await.unwrap());
        for n in 0..24 {
            socket
                .emit(
                    "CurrentProgramSceneChanged",
                    json!({"sceneName":format!("Scene{n}"),"sceneUuid":format!("id{n}")}),
                )
                .await;
            let packet = tokio::time::timeout(Duration::from_secs(3), fast.next())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(decode(&packet)["method"], "notifications/resources/updated");
        }
        assert_eq!(bridge.streams.count(), 1);
        assert_eq!(slow.failure(), Some(http_sink::Failure::Capacity));
        assert!(
            bridge
                .host
                .runtime
                .notifications
                .terminal_failure()
                .is_none()
        );
        assert!(
            bridge
                .host
                .runtime
                .connect()
                .await
                .unwrap()
                .read(ReadRequest::Version, Value::Null, Duration::from_secs(3))
                .await
                .is_ok()
        );
        bridge.close().await.unwrap();
        drop(slow);
        drop(fast);
        socket.join().await;
        assert_eq!(bridge.budget.available(), 32 * 1024 * 1024);
    }
    #[tokio::test]
    async fn confirmation_crosses_exchanges_once_and_legacy_offers_never_leak() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, true);
        let mut ask = frame(
            7,
            "tools/call",
            json!({"name":"obs-stop-record","arguments":{}}),
            false,
        );
        ask["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] =
            json!({"elicitation":{"form":{}}});
        let opened = open(&mut bridge, ask.clone(), false);
        let mut first = submit(&mut bridge, opened).await;
        let required = decode(&answer(&mut bridge, &mut first).await);
        assert_eq!(required["result"]["resultType"], "input_required");
        let token = required["result"]["requestState"].clone();
        assert!(token.is_string());
        assert_eq!(socket.count("GetVersion"), 0);
        drop(first);
        ask["params"]["requestState"] = token;
        ask["params"]["inputResponses"] =
            json!({"confirm":{"action":"accept","content":{"confirm":true}}});
        let opened = open(&mut bridge, ask.clone(), false);
        let mut resumed = submit(&mut bridge, opened).await;
        let result = decode(&answer(&mut bridge, &mut resumed).await);
        assert_ne!(result["result"]["isError"], true, "{result}");
        assert_eq!(socket.count("StopRecord"), 1);
        let opened = open(&mut bridge, ask, false);
        let mut duplicate = submit(&mut bridge, opened).await;
        assert_eq!(
            decode(&answer(&mut bridge, &mut duplicate).await)["error"]["code"],
            -32602
        );
        assert_eq!(socket.count("StopRecord"), 1);
        let init = frame(
            7,
            "initialize",
            json!({"protocolVersion":"2025-11-25","clientInfo":{"name":"offered","version":"1"},"capabilities":{"elicitation":{"form":{}}}}),
            true,
        );
        let opened = open(&mut bridge, init, true);
        let mut initialized = submit(&mut bridge, opened).await;
        assert_eq!(
            decode(&answer(&mut bridge, &mut initialized).await)["result"]["protocolVersion"],
            "2025-11-25"
        );
        let opened = open(
            &mut bridge,
            frame(
                7,
                "tools/call",
                json!({"name":"obs-stop-record","arguments":{}}),
                true,
            ),
            true,
        );
        let mut legacy = submit(&mut bridge, opened).await;
        let refused = decode(&answer(&mut bridge, &mut legacy).await);
        assert!(refused.to_string().contains("client cannot ask"));
        assert_eq!(socket.count("StopRecord"), 1);
        let opened = open(
            &mut bridge,
            frame(
                7,
                "tools/call",
                json!({"name":"obs-stop-record","arguments":{"confirm":true}}),
                true,
            ),
            true,
        );
        let mut explicit = submit(&mut bridge, opened).await;
        assert_ne!(
            decode(&answer(&mut bridge, &mut explicit).await)["result"]["isError"],
            true
        );
        assert_eq!(socket.count("StopRecord"), 2);
        bridge.close().await.unwrap();
        socket.join().await;
    }
    #[tokio::test]
    async fn held_ack_departure_keeps_known_effect_and_same_wire_peer_is_live() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, false);
        bridge.host.runtime.connect().await.unwrap();
        socket.plan(
            "SetInputMute",
            support::Plan {
                held: true,
                ..Default::default()
            },
        );
        let opened = open(
            &mut bridge,
            frame(
                7,
                "tools/call",
                json!({"name":"obs-set-input-mute","arguments":{"inputName":"Fixture","inputMuted":true}}),
                false,
            ),
            false,
        );
        let a = submit(&mut bridge, opened).await;
        socket.seen("SetInputMute", 1).await;
        drop(a);
        let opened = open(
            &mut bridge,
            frame(
                7,
                "tools/call",
                json!({"name":"obs-control-status","arguments":{}}),
                false,
            ),
            false,
        );
        let mut peer = submit(&mut bridge, opened).await;
        let reply = decode(&answer(&mut bridge, &mut peer).await);
        assert_eq!(reply["id"], 7);
        assert_ne!(reply["result"]["isError"], true);
        assert_eq!(socket.count("SetInputMute"), 1);
        socket.release("SetInputMute").await;
        while bridge.has_jobs() {
            bridge.complete_next().await.unwrap();
        }
        let saved = std::fs::read_dir(dir.0.join("operation-receipts"))
            .unwrap()
            .map(|entry| {
                serde_json::from_slice::<Value>(&std::fs::read(entry.unwrap().path()).unwrap())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0]["phase"], "accepted");
        assert_eq!(saved[0]["requestAccepted"], true);
        assert_eq!(socket.count("SetInputMute"), 1);
        bridge.close().await.unwrap();
        drop(peer);
        socket.join().await;
        assert_eq!(bridge.budget.available(), 32 * 1024 * 1024);
    }
    #[tokio::test]
    async fn global_subscription_limit_is_not_multiplied_by_frontends_and_drop_refunds_one() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, false);
        bridge.host.runtime.connect().await.unwrap();
        let mut clients = Vec::new();
        for _ in 0..1024 {
            let opened = open(
                &mut bridge,
                frame(
                    7,
                    "subscriptions/listen",
                    json!({"notifications":{"resourceSubscriptions":["obs://status"]}}),
                    false,
                ),
                false,
            );
            let mut client = submit(&mut bridge, opened).await;
            assert_eq!(
                decode(&client.next().await.unwrap())["method"],
                "notifications/subscriptions/acknowledged"
            );
            clients.push(client);
        }
        assert_eq!(bridge.streams.count(), 1024);
        let opened = open(
            &mut bridge,
            frame(
                7,
                "subscriptions/listen",
                json!({"notifications":{}}),
                false,
            ),
            false,
        );
        let mut full = submit(&mut bridge, opened).await;
        assert!(
            decode(&full.next().await.unwrap())
                .to_string()
                .contains("Subscription limit reached")
        );
        drop(clients.pop());
        assert_eq!(bridge.streams.count(), 1023);
        let opened = open(
            &mut bridge,
            frame(
                7,
                "subscriptions/listen",
                json!({"notifications":{}}),
                false,
            ),
            false,
        );
        let mut replacement = submit(&mut bridge, opened).await;
        assert_eq!(
            decode(&replacement.next().await.unwrap())["method"],
            "notifications/subscriptions/acknowledged"
        );
        assert_eq!(bridge.streams.count(), 1024);
        bridge.close().await.unwrap();
        drop(clients);
        drop(replacement);
        drop(full);
        socket.join().await;
        assert_eq!(bridge.budget.available(), 32 * 1024 * 1024);
    }
    #[tokio::test]
    async fn ordinary_work_and_metadata_pressure_preserve_one_global_safety_stop_ingress() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, false);
        let mut sleepers = Vec::new();
        for _ in 0..32 {
            let opened = open(
                &mut bridge,
                frame(
                    7,
                    "tools/call",
                    json!({"name":"obs-sleep","arguments":{"sleepMillis":50000}}),
                    false,
                ),
                false,
            );
            sleepers.push(submit(&mut bridge, opened).await);
        }
        let mut unopened = Vec::new();
        for _ in 0..96 {
            unopened.push(open(
                &mut bridge,
                frame(7, "server/discover", json!({}), false),
                false,
            ));
        }
        socket.plan(
            "StopRecord",
            support::Plan {
                held: true,
                ..Default::default()
            },
        );
        let opened = open(
            &mut bridge,
            frame(
                7,
                "tools/call",
                json!({"name":"obs-stop-record","arguments":{}}),
                false,
            ),
            false,
        );
        let mut stop = submit(&mut bridge, opened).await;
        if bridge.host.runtime.initializing() {
            bridge.host.runtime.progress_initialization().await.unwrap();
        }
        socket.seen("StopRecord", 1).await;
        let frame = frame(
            7,
            "tools/call",
            json!({"name":"obs-stop-record","arguments":{}}),
            false,
        );
        let headers = [
            ("host", "127.0.0.1:4455"),
            ("content-type", "application/json"),
            ("mcp-protocol-version", "2026-07-28"),
            ("mcp-method", "tools/call"),
            ("mcp-name", "obs-stop-record"),
        ];
        assert!(
            bridge
                .open(
                    RequestHead {
                        method: "POST",
                        target: "/mcp",
                        headers: &headers
                    },
                    serde_json::to_vec(&frame).unwrap()
                )
                .is_err()
        );
        socket.release("StopRecord").await;
        let response = decode(&answer(&mut bridge, &mut stop).await);
        assert_ne!(response["result"]["isError"], true, "{response}");
        assert_eq!(socket.count("StopRecord"), 1);
        bridge.close().await.unwrap();
        drop(stop);
        drop(sleepers);
        drop(unopened);
        socket.join().await;
        assert_eq!(bridge.budget.available(), 32 * 1024 * 1024);
    }
    #[test]
    fn legacy_batch_preserves_each_original_ordered_frame_without_reserialization() {
        let body=br#"[{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"obs-apply-scene","arguments":{"sources":[{"settings":{"z":1,"a":2,"7":"index"}}]}}},{"jsonrpc":"2.0","id":2,"method":"ping"}]"#.to_vec();
        let headers = [
            ("host", "127.0.0.1:4455"),
            ("content-type", "application/json"),
            ("accept", "application/json, text/event-stream"),
        ];
        let http::Admission::Exchange(exchange) = http::admit(
            RequestHead {
                method: "POST",
                target: "/mcp",
                headers: &headers,
            },
            body.clone(),
            &Policy::new(None),
        ) else {
            panic!()
        };
        let items = exchange.into_items().unwrap();
        assert_eq!(items.len(), 2);
        let expected=br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"obs-apply-scene","arguments":{"sources":[{"settings":{"z":1,"a":2,"7":"index"}}]}}}"#;
        assert_eq!(items[0].original_body(), expected);
        let arguments = &items[0].message()["params"]["arguments"];
        assert!(
            obs_mcp_native::tool_validation::ApplySceneSettingsOrder::from_frame(
                arguments,
                items[0].original_body()
            )
            .is_ok()
        );
        assert!(
            obs_mcp_native::tool_validation::ApplySceneSettingsOrder::from_frame(arguments, &body)
                .is_err()
        );
    }
    #[tokio::test]
    async fn abandoned_unsubmitted_exchange_scopes_are_reaped_and_stale_submission_has_no_effect() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, false);
        let mut stale = None;
        for index in 0..1024 {
            let opened = open(
                &mut bridge,
                frame(
                    index,
                    "tools/call",
                    json!({"name":"obs-set-input-mute","arguments":{"inputName":"Mic","inputMuted":true}}),
                    false,
                ),
                false,
            );
            drop(opened.response);
            if index == 0 {
                stale = opened.submission;
            } else {
                drop(opened.submission);
            }
            assert!(bridge.routes.len() <= 1);
        }
        bridge.tick(Instant::now());
        assert!(bridge.routes.is_empty());
        assert!(bridge.cleanups.is_empty());
        bridge.submit(stale.unwrap()).await.unwrap();
        assert!(!bridge.has_jobs());
        assert!(bridge.host.runtime.owner.is_none());
        assert_eq!(bridge.input_bytes.available_permits(), 32 * 1024 * 1024);
        bridge.host.runtime.connect().await.unwrap();
        bridge.close().await.unwrap();
        socket.join().await;
    }
    #[tokio::test]
    async fn invalid_legacy_initialize_refuses_without_retaining_offers_or_opening_obs() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, true);
        for params in [
            json!({}),
            json!({"protocolVersion":"2025-11-25","clientInfo":{"name":"x","version":"1"},"capabilities":{"elicitation":{"form":false}}}),
            json!({"protocolVersion":"2025-11-25","clientInfo":{"name":1,"version":"1"},"capabilities":{}}),
        ] {
            let opened = open(
                &mut bridge,
                json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":params}),
                true,
            );
            let mut response = submit(&mut bridge, opened).await;
            assert!(
                decode(&answer(&mut bridge, &mut response).await)
                    .get("error")
                    .is_some()
            );
        }
        assert!(bridge.host.runtime.owner.is_none());
        assert!(socket.trace.lock().unwrap().is_empty());
        // Complete this fixture's accept lifetime only after cold refusal was proven.
        bridge.host.runtime.connect().await.unwrap();
        bridge.close().await.unwrap();
        socket.join().await;
    }
    fn try_open_stop(bridge: &mut Bridge, id: u64, name: &str) -> io::Result<Opened> {
        let value = frame(id, "tools/call", json!({"name":name,"arguments":{}}), false);
        let headers = [
            ("host", "127.0.0.1:4455"),
            ("content-type", "application/json"),
            ("mcp-protocol-version", "2026-07-28"),
            ("mcp-method", "tools/call"),
            ("mcp-name", name),
        ];
        bridge.open(
            RequestHead {
                method: "POST",
                target: "/mcp",
                headers: &headers,
            },
            serde_json::to_vec(&value).unwrap(),
        )
    }
    #[tokio::test]
    async fn saturated_ordinary_body_and_output_credits_still_admit_typed_stop() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, false);
        bridge.host.runtime.host = Some(Arc::new(
            obs_mcp_native::take_transport::LocalHost::new(&dir.0, None, None).unwrap(),
        ));
        let mut listeners = Vec::new();
        for _ in 0..1024 {
            let opened = open(
                &mut bridge,
                frame(
                    7,
                    "subscriptions/listen",
                    json!({"notifications":{"resourceSubscriptions":["obs://status"]}}),
                    false,
                ),
                false,
            );
            let mut listener = submit(&mut bridge, opened).await;
            drop(listener.next().await.unwrap());
            listeners.push(listener);
        }
        assert_eq!(bridge.streams.count(), 1024);
        let mut ordinary_bodies = Vec::new();
        let mut value = frame(
            7,
            "tools/call",
            json!({"name":"obs-sleep","arguments":{"sleepMillis":50000,"padding":""}}),
            true,
        );
        let base = serde_json::to_vec(&value).unwrap().len();
        value["params"]["arguments"]["padding"] = json!("x".repeat(http::MAX_BODY_BYTES - base));
        let body = serde_json::to_vec(&value).unwrap();
        assert_eq!(body.len(), http::MAX_BODY_BYTES);
        let headers = [
            ("host", "127.0.0.1:4455"),
            ("content-type", "application/json"),
            ("accept", "application/json, text/event-stream"),
        ];
        for _ in 0..8 {
            ordinary_bodies.push(
                bridge
                    .open(
                        RequestHead {
                            method: "POST",
                            target: "/mcp",
                            headers: &headers,
                        },
                        body.clone(),
                    )
                    .unwrap(),
            );
        }
        assert_eq!(bridge.input_bytes.available_permits(), 0);
        let mut output_readers = Vec::new();
        let mut output_senders = Vec::new();
        for _ in 0..32 {
            let (sender, reader) = Sender::channel(&bridge.budget, true, Cancellation::new());
            sender
                .send(Response {
                    status: 200,
                    content_type: None,
                    authenticate_bearer: false,
                    body: vec![0; 1024 * 1024],
                })
                .unwrap();
            output_readers.push(reader);
            output_senders.push(sender);
        }
        assert_eq!(bridge.budget.available(), 0);
        let opened = try_open_stop(&mut bridge, 7, "obs-stop-record").unwrap();
        assert!(try_open_stop(&mut bridge, 7, "obs-stop-stream").is_err());
        let mut response = submit(&mut bridge, opened).await;
        let packet = answer(&mut bridge, &mut response).await;
        assert_ne!(decode(&packet)["result"]["isError"], true);
        assert_eq!(socket.count("StopRecord"), 1);
        drop(response);
        assert!(
            try_open_stop(&mut bridge, 7, "obs-stop-record").is_err(),
            "dequeued Packet still owns Stop slot/body/output credits"
        );
        let opened = try_open_stop(&mut bridge, 7, "obs-take-stop").unwrap();
        let mut take_response = submit(&mut bridge, opened).await;
        let take_packet = answer(&mut bridge, &mut take_response).await;
        assert_eq!(take_packet.response().status, 200);
        assert!(decode(&take_packet)["result"].get("content").is_some());
        assert!(try_open_stop(&mut bridge, 7, "obs-take-stop").is_err());
        assert_eq!(bridge.budget.available(), 0);
        assert_eq!(bridge.input_bytes.available_permits(), 0);
        drop(packet);
        let replacement = try_open_stop(&mut bridge, 7, "obs-stop-record").unwrap();
        drop(replacement);
        drop(take_packet);
        drop(take_response);
        bridge.tick(Instant::now());
        for class in [Class::TakeStop, Class::SafetyStop] {
            assert_eq!(
                bridge.budget.stop_available(class),
                (1, http_sink::STOP_BODY_BYTES, http_sink::STOP_OUTPUT_BYTES)
            );
        }
        drop(ordinary_bodies);
        drop(output_readers);
        drop(output_senders);
        drop(listeners);
        bridge.close().await.unwrap();
        socket.join().await;
        assert_eq!(bridge.input_bytes.available_permits(), 32 * 1024 * 1024);
        assert_eq!(bridge.budget.available(), 32 * 1024 * 1024);
    }
    #[tokio::test]
    async fn dropped_held_stop_retains_reserved_custody_until_job_joins_without_replay() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, false);
        socket.plan(
            "StopRecord",
            support::Plan {
                held: true,
                ..Default::default()
            },
        );
        let opened = try_open_stop(&mut bridge, 7, "obs-stop-record").unwrap();
        let response = submit(&mut bridge, opened).await;
        if bridge.host.runtime.initializing() {
            bridge.host.runtime.progress_initialization().await.unwrap();
        }
        socket.seen("StopRecord", 1).await;
        drop(response);
        bridge.tick(Instant::now());
        assert!(try_open_stop(&mut bridge, 7, "obs-stop-stream").is_err());
        assert_eq!(bridge.budget.stop_available(Class::SafetyStop).0, 0);
        socket.release("StopRecord").await;
        bridge.complete_next().await.unwrap();
        assert_eq!(
            bridge.budget.stop_available(Class::SafetyStop),
            (1, http_sink::STOP_BODY_BYTES, http_sink::STOP_OUTPUT_BYTES)
        );
        let replacement = try_open_stop(&mut bridge, 7, "obs-stop-record").unwrap();
        drop(replacement);
        assert_eq!(socket.count("StopRecord"), 1);
        bridge.close().await.unwrap();
        socket.join().await;
    }
    #[tokio::test]
    async fn unqualified_names_batches_malformed_arguments_and_reads_cannot_borrow_stop_bytes() {
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, true);
        let ordinary = bridge
            .input_bytes
            .clone()
            .try_acquire_many_owned(32 * 1024 * 1024)
            .unwrap();
        let valid = frame(
            7,
            "tools/call",
            json!({"name":"obs-stop-record","arguments":{}}),
            true,
        );
        let mut oversized = valid.clone();
        oversized["params"]["arguments"]["padding"] = json!("x".repeat(http_sink::STOP_BODY_BYTES));
        for value in [
            frame(
                7,
                "tools/call",
                json!({"name":"obs-stop-record","arguments":[]}),
                true,
            ),
            frame(
                7,
                "tools/call",
                json!({"name":"obs-stop-record","arguments":{"confirm":1}}),
                true,
            ),
            frame(
                7,
                "tools/call",
                json!({"name":"obs-stop-record","arguments":{},"reserveStop":true}),
                true,
            ),
            frame(
                7,
                "tools/call",
                json!({"name":"obs-get-version","arguments":{"reserveStop":true}}),
                true,
            ),
            frame(
                7,
                "resources/read",
                json!({"uri":"obs://status","name":"obs-stop-record"}),
                true,
            ),
            json!([valid]),
            oversized,
        ] {
            let headers = [
                ("host", "127.0.0.1:4455"),
                ("content-type", "application/json"),
                ("accept", "application/json, text/event-stream"),
                ("x-reserve-stop", "true"),
            ];
            let result = bridge.open(
                RequestHead {
                    method: "POST",
                    target: "/mcp",
                    headers: &headers,
                },
                serde_json::to_vec(&value).unwrap(),
            );
            if let Ok(opened) = result {
                assert!(opened.submission.is_none());
                drop(opened);
            }
            for class in [Class::TakeStop, Class::SafetyStop] {
                assert_eq!(
                    bridge.budget.stop_available(class),
                    (1, http_sink::STOP_BODY_BYTES, http_sink::STOP_OUTPUT_BYTES)
                );
            }
        }
        assert!(!bridge.has_jobs());
        assert!(bridge.host.runtime.owner.is_none());
        drop(ordinary);
        bridge.host.runtime.connect().await.unwrap();
        bridge.close().await.unwrap();
        socket.join().await;
    }
    #[tokio::test]
    async fn real_emergency_reader_handoff_refuses_listen_batch_and_ordinary_call_but_admits_only_stop()
     {
        use obs_mcp_native::http_ingress::{Ingress, Kind};
        let socket = support::Socket::new().await;
        let dir = support::Dir::new();
        let mut bridge = bridge(&socket, &dir, false);
        let gate = Ingress::new();
        let ordinary = (0..32).map(|_| gate.admit().unwrap()).collect::<Vec<_>>();
        for value in [
            frame(7, "subscriptions/listen", json!({"notifications":{}}), true),
            frame(
                7,
                "tools/call",
                json!({"name":"obs-get-version","arguments":{}}),
                true,
            ),
            json!([frame(
                7,
                "tools/call",
                json!({"name":"obs-stop-record","arguments":{}}),
                true
            )]),
            frame(
                7,
                "tools/call",
                json!({"name":"obs-stop-record","arguments":{}}),
                true,
            ),
        ] {
            let connection = gate.admit().unwrap();
            assert_eq!(connection.kind(), Kind::Emergency);
            let bytes = serde_json::to_vec(&value).unwrap();
            let length = bytes.len();
            let headers = [
                ("host", "127.0.0.1:4455"),
                ("content-type", "application/json"),
                ("accept", "application/json, text/event-stream"),
            ];
            let head = RequestHead {
                method: "POST",
                target: "/mcp",
                headers: &headers,
            };
            let mut stream = futures_util::stream::iter([Ok::<_, ()>(bytes)]);
            let body = connection
                .collect(
                    &head,
                    &Policy::new(None),
                    Some(length),
                    Instant::now() + Duration::from_secs(5),
                    &mut stream,
                )
                .await
                .unwrap();
            let result = body
                .handoff(|bytes, kind, _| {
                    assert_eq!(kind, Kind::Emergency);
                    bridge.open_emergency(head, bytes)
                })
                .unwrap();
            if value.get("method") == Some(&json!("tools/call"))
                && value.pointer("/params/name") == Some(&json!("obs-stop-record"))
            {
                let mut response = submit(&mut bridge, result.unwrap()).await;
                assert_ne!(
                    decode(&answer(&mut bridge, &mut response).await)["result"]["isError"],
                    true
                );
            } else {
                assert!(result.is_err());
                assert!(bridge.host.runtime.owner.is_none());
                assert!(!bridge.has_jobs());
            }
        }
        assert_eq!(socket.count("StopRecord"), 1);
        drop(ordinary);
        gate.close();
        gate.drained().await.unwrap();
        bridge.close().await.unwrap();
        socket.join().await;
    }
    #[test]
    #[ignore = "owned in-memory SDK fixture process; no TCP HTTP server"]
    fn probe_child() {
        if std::env::var("NATIVE_HTTP_BRIDGE_PROBE").as_deref() != Ok("1") {
            return;
        }
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async{
            use tokio::io::AsyncBufReadExt;
            use futures_util::Stream;
            let root=std::path::PathBuf::from(std::env::var_os("NATIVE_HTTP_BRIDGE_STATE").unwrap());
            let runtime=Runtime{profile:obs_mcp_native::native_profile::NativeProfile::Production,notifications:Owner::new(false),owner:None,initialization:None,initialization_broken:false,network:None, tls_ca:None,url:std::env::var("NATIVE_HTTP_BRIDGE_OBS").unwrap(),password:None,root:root.clone(),host:None,control:Arc::new(Mutex::new(ControlDomain::default())),clock:super::super::admitted_clock(obs_mcp_native::clock::snapshot).unwrap(),preparation:Some(PreparationOwners::new(&root).unwrap()),media:None,replay:None,media_work:None,screenshot_limit:Ok(ScreenshotLimit::default()),confirm_live:std::env::var("NATIVE_HTTP_BRIDGE_CONFIRM").as_deref()==Ok("1")};
            let mut bridge=Bridge::new(runtime,ToolFilter::parse(|name|(name=="OBS_MCP_TOOLSETS").then(||"all".into()),crate::schema::catalog()).unwrap(),Policy::new(None)).unwrap();
            let mut clients=HashMap::<u64,ClientResponse>::new();let mut lines=tokio::io::BufReader::new(tokio::io::stdin()).lines();
            fn send(value:Value){use std::io::Write;let mut out=std::io::stdout().lock();out.write_all(b"HTTP_BRIDGE_PROBE ").unwrap();serde_json::to_writer(&mut out,&value).unwrap();out.write_all(b"\n").unwrap();out.flush().unwrap();}
            send(json!({"ready":true}));
            loop{
                let has_jobs=bridge.has_jobs();let tick=bridge.next_tick();
                tokio::select!{biased;
                    line=lines.next_line()=>{
                        let Some(line)=line.unwrap()else{break;};assert!(line.len()<=8*1024*1024);let command:Value=serde_json::from_str(&line).unwrap();let id=command["id"].as_u64().unwrap_or(0);
                        match command["action"].as_str().unwrap(){
                            "open"=>{
                                let headers=command["headers"].as_object().unwrap().iter().map(|(k,v)|(k.as_str(),v.as_str().unwrap())).collect::<Vec<_>>();
                                match bridge.open(RequestHead{method:command["method"].as_str().unwrap(),target:command["target"].as_str().unwrap(),headers:&headers},command["body"].as_str().unwrap_or("").as_bytes().to_vec()){
                                    Ok(mut opened)=>{clients.insert(id,opened.response);if let Some(submission)=opened.submission.take() && let Err(error)=bridge.submit(submission).await{send(json!({"id":id,"failure":error.to_string()}));clients.remove(&id);}},
                                    Err(error)=>send(json!({"id":id,"failure":error.to_string()})),
                                }
                            },
                            "close"=>{clients.remove(&id);send(json!({"id":id,"closed":true}));},
                            "shutdown"=>{bridge.close().await.unwrap();clients.clear();send(json!({"id":id,"shutdown":true}));return;},
                            _=>panic!("fixture action"),
                        }
                    },
                    result=bridge.complete_next(),if has_jobs=>{result.unwrap();},
                    next=futures_util::future::poll_fn(|cx|{
                        for(id,client)in &mut clients{if let std::task::Poll::Ready(packet)=std::pin::Pin::new(client).poll_next(cx){return std::task::Poll::Ready((*id,packet));}}
                        std::task::Poll::Pending
                    }),if !clients.is_empty()=>{
                        let(id,packet)=next;
                        if let Some(packet)=packet{let response=packet.response();send(json!({"id":id,"packet":{"status":response.status,"contentType":response.content_type,"authenticateBearer":response.authenticate_bearer,"body":std::str::from_utf8(&response.body).unwrap()}}));}
                        else{clients.remove(&id);send(json!({"id":id,"ended":true}));}
                    },
                    ()=tokio::time::sleep_until(tick)=>bridge.tick(Instant::now()),
                }
            }
            bridge.close().await.unwrap();clients.clear();
        });
    }
}
