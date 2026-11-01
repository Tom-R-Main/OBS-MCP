// SPDX-License-Identifier: GPL-2.0-only
//! Real socket/sole-bridge prototype controls, not exposed output tool parity.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    output_events::{
        MAX_SUBSCRIPTIONS, OutputEventError as Error, OutputKind, PATH_BYTES, Terminal,
    },
    session::{EventReceiver, ObsSession, SessionHandle, SessionOptions},
    session_observation::ObservationBridge,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const WATCHDOG: Duration = Duration::from_secs(5);
enum Command {
    Emit(&'static str, Value, oneshot::Sender<()>),
    Close(oneshot::Sender<()>),
}
struct SocketFixture {
    owner: ObsSession,
    events: EventReceiver,
    commands: mpsc::Sender<Command>,
    peer: JoinHandle<()>,
    connections: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
}
impl SocketFixture {
    async fn new(options: SessionOptions, start_events: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let counted = connections.clone();
        let requests = Arc::new(AtomicUsize::new(0));
        let request_count = requests.clone();
        let (commands, mut receive) = mpsc::channel::<Command>(8);
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            counted.fetch_add(1, Ordering::SeqCst);
            let mut socket = accept_async(stream).await.unwrap();
            socket
                .send(Message::Text(
                    json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let identify = socket.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(identify.to_text().unwrap()).unwrap()["op"],
                1
            );
            socket
                .send(Message::Text(
                    json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            loop {
                tokio::select! {
                    command=receive.recv()=>match command {
                        Some(Command::Emit(name,data,done))=>{
                            let intent=if name=="InputVolumeMeters" {1<<16}else{1<<6};
                            if socket.send(Message::Text(json!({"op":5,"d":{"eventType":name,"eventIntent":intent,"eventData":data}}).to_string().into())).await.is_err(){break;}
                            let _=done.send(());
                        },
                        Some(Command::Close(done))=>{let _=socket.send(Message::Close(None)).await;let _=done.send(());break;},
                        None=>break,
                    },
                    packet=socket.next()=>match packet {
                        Some(Ok(Message::Text(text)))=>{
                            let frame:Value=serde_json::from_str(&text).unwrap();assert_eq!(frame["op"],6);request_count.fetch_add(1,Ordering::SeqCst);
                            if start_events && frame["d"]["requestType"]=="StartRecord" {
                                for state in ["OBS_WEBSOCKET_OUTPUT_STARTED","OBS_WEBSOCKET_OUTPUT_STOPPED"] {
                                    socket.send(Message::Text(json!({"op":5,"d":{"eventType":"RecordStateChanged","eventIntent":64,"eventData":{"outputState":state,"outputPath":"/fixture/first.mkv"}}}).to_string().into())).await.unwrap();
                                }
                            }
                            if socket.send(Message::Text(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":frame["d"]["requestType"],"requestStatus":{"result":true,"code":100},"responseData":{}}}).to_string().into())).await.is_err(){break;}
                        },
                        Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                        _=>{},
                    }
                }
            }
        });
        let (owner, events) = ObsSession::connect(&url, None, options).await.unwrap();
        Self {
            owner,
            events,
            commands,
            peer,
            connections,
            requests,
        }
    }
}
struct Fixture {
    owner: ObsSession,
    bridge: ObservationBridge,
    commands: mpsc::Sender<Command>,
    peer: JoinHandle<()>,
    connections: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
}
impl Fixture {
    async fn new(options: SessionOptions, start_events: bool) -> Self {
        let SocketFixture {
            owner,
            events,
            commands,
            peer,
            connections,
            requests,
        } = SocketFixture::new(options, start_events).await;
        let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
        Self {
            owner,
            bridge,
            commands,
            peer,
            connections,
            requests,
        }
    }
    async fn emit(&self, name: &'static str, data: Value) {
        emit(&self.commands, name, data).await;
    }
    async fn observed(&self) {
        received(&self.owner.handle()).await;
        self.bridge
            .synchronize(Instant::now() + WATCHDOG)
            .await
            .unwrap();
    }
    async fn close(self) {
        self.owner.shutdown().await.unwrap();
        self.bridge.shutdown().await.unwrap();
        drop(self.commands);
        self.peer.await.unwrap();
        assert_eq!(self.connections.load(Ordering::SeqCst), 1);
    }
}
async fn emit(commands: &mpsc::Sender<Command>, name: &'static str, data: Value) {
    let (done, wait) = oneshot::channel();
    commands
        .send(Command::Emit(name, data, done))
        .await
        .unwrap();
    timeout(WATCHDOG, wait).await.unwrap().unwrap();
}
async fn received(handle: &SessionHandle) {
    handle
        .request(handle.token(), "GetVersion", json!({}), WATCHDOG)
        .await
        .unwrap();
}
fn started(path: Value) -> Value {
    json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":path})
}

#[tokio::test]
async fn no_take_first_terminal_survives_pre_ack_replacement_and_late_consumption() {
    let f = Fixture::new(Default::default(), true).await;
    let view = f.bridge.output_events().unwrap();
    let sub = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    let before = Instant::now();
    f.owner
        .handle()
        .request(f.owner.token(), "StartRecord", Value::Null, WATCHDOG)
        .await
        .unwrap();
    let acknowledged = Instant::now();
    f.bridge
        .synchronize(Instant::now() + WATCHDOG)
        .await
        .unwrap();
    let event = sub.receive(acknowledged + WATCHDOG).await.unwrap();
    assert_eq!(event.terminal(), Terminal::Started);
    assert_eq!(event.output_path(), Some("/fixture/first.mkv"));
    assert_eq!(event.sequence(), 1);
    assert_eq!(event.connection(), f.owner.token());
    assert!(event.received_at() >= before && event.received_at() <= acknowledged);
    assert_eq!(
        f.requests.load(Ordering::SeqCst),
        1,
        "subscription created an RPC"
    );
    f.close().await;
}
#[tokio::test]
async fn four_closed_kinds_preserve_only_string_paths_including_empty() {
    let f = Fixture::new(Default::default(), false).await;
    let view = f.bridge.output_events().unwrap();
    for kind in [
        OutputKind::Record,
        OutputKind::Stream,
        OutputKind::VirtualCam,
        OutputKind::ReplayBuffer,
    ] {
        for path in [
            None,
            Some(Value::Null),
            Some(json!(7)),
            Some(json!("")),
            Some(json!("/fixture/é.mkv")),
        ] {
            let sub = view.subscribe(kind, Instant::now() + WATCHDOG).unwrap();
            let mut data = json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED"});
            if let Some(path) = path.as_ref() {
                data["outputPath"] = path.clone();
            }
            f.emit(kind.event_name(), data).await;
            f.observed().await;
            let event = sub.receive(Instant::now() + WATCHDOG).await.unwrap();
            assert_eq!(event.output_path(), path.as_ref().and_then(Value::as_str));
        }
    }
    let sub = view
        .subscribe(
            OutputKind::Record,
            Instant::now() + Duration::from_millis(100),
        )
        .unwrap();
    for data in [
        Value::Null,
        json!([]),
        json!({}),
        json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STARTING"}),
    ] {
        f.emit("RecordStateChanged", data).await;
        f.observed().await;
    }
    assert_eq!(
        sub.receive(Instant::now() + WATCHDOG).await.unwrap_err(),
        Error::ConfirmationExpired
    );
    f.close().await;
}
#[tokio::test]
async fn unrelated_flood_does_not_overwrite_or_consume_subscription_capacity() {
    let f = Fixture::new(Default::default(), false).await;
    let view = f.bridge.output_events().unwrap();
    let sub = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    for _ in 0..48 {
        f.emit(
            "InputMuteStateChanged",
            json!({"largeUnselected":"ignored","outputState":"OBS_WEBSOCKET_OUTPUT_STARTED"}),
        )
        .await;
        f.observed().await;
    }
    f.emit("RecordStateChanged", started(json!("first"))).await;
    f.observed().await;
    f.emit(
        "RecordStateChanged",
        json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"}),
    )
    .await;
    f.observed().await;
    assert_eq!(
        sub.receive(Instant::now() + WATCHDOG)
            .await
            .unwrap()
            .output_path(),
        Some("first")
    );
    f.close().await;
}
#[tokio::test]
async fn fixed_slots_release_on_consumption_drop_and_cancelled_receive() {
    let f = Fixture::new(Default::default(), false).await;
    let view = f.bridge.output_events().unwrap();
    let mut slots = (0..MAX_SUBSCRIPTIONS)
        .map(|_| {
            view.subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        view.subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
            .unwrap_err(),
        Error::Capacity
    );
    drop(slots.pop());
    let replacement = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    assert_eq!(
        view.subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
            .unwrap_err(),
        Error::Capacity
    );
    f.emit("RecordStateChanged", started(Value::Null)).await;
    f.observed().await;
    let event = slots
        .pop()
        .unwrap()
        .receive(Instant::now() + WATCHDOG)
        .await
        .unwrap();
    let reused = view
        .subscribe(OutputKind::Stream, Instant::now() + WATCHDOG)
        .unwrap();
    assert_eq!(
        view.subscribe(OutputKind::Stream, Instant::now() + WATCHDOG)
            .unwrap_err(),
        Error::Capacity
    );
    drop(event);
    drop(reused);
    drop(slots);
    drop(replacement);
    let mut waiting = Box::pin(
        view.subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
            .unwrap()
            .receive(Instant::now() + WATCHDOG),
    );
    assert!(futures_util::poll!(waiting.as_mut()).is_pending());
    drop(waiting);
    let slots = (0..MAX_SUBSCRIPTIONS)
        .map(|_| {
            view.subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        view.subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
            .unwrap_err(),
        Error::Capacity
    );
    drop(slots);
    f.close().await;
}
#[tokio::test]
async fn utf8_byte_budget_follows_consumed_event_and_releases_on_each_drop() {
    let f = Fixture::new(
        SessionOptions {
            event_items: 1,
            ..Default::default()
        },
        false,
    )
    .await;
    let view = f.bridge.output_events().unwrap();
    let a = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    let b = view
        .subscribe(OutputKind::Stream, Instant::now() + WATCHDOG)
        .unwrap();
    let path = "é".repeat(PATH_BYTES / 4);
    assert_eq!(path.len(), PATH_BYTES / 2);
    f.emit("RecordStateChanged", started(json!(path))).await;
    f.observed().await;
    f.emit("StreamStateChanged", started(json!(path))).await;
    f.observed().await;
    let owned = a.receive(Instant::now() + WATCHDOG).await.unwrap();
    assert_eq!(owned.output_path().unwrap().len(), PATH_BYTES / 2);
    let c = view
        .subscribe(OutputKind::VirtualCam, Instant::now() + WATCHDOG)
        .unwrap();
    f.emit("VirtualcamStateChanged", started(json!("x"))).await;
    f.observed().await;
    assert_eq!(
        c.receive(Instant::now() + WATCHDOG).await.unwrap_err(),
        Error::ByteCapacity
    );
    drop(owned);
    let c = view
        .subscribe(OutputKind::VirtualCam, Instant::now() + WATCHDOG)
        .unwrap();
    f.emit(
        "VirtualcamStateChanged",
        started(json!("private-path-marker")),
    )
    .await;
    f.observed().await;
    let c = c.receive(Instant::now() + WATCHDOG).await.unwrap();
    assert_eq!(c.output_path(), Some("private-path-marker"));
    assert!(!format!("{c:?}").contains("private-path-marker"));
    drop(c);
    drop(b);
    let c = view
        .subscribe(OutputKind::ReplayBuffer, Instant::now() + WATCHDOG)
        .unwrap();
    f.emit("ReplayBufferStateChanged", started(json!(path)))
        .await;
    f.observed().await;
    assert!(c.receive(Instant::now() + WATCHDOG).await.is_ok());
    assert!(f.owner.handle().invalidation().is_none());
    f.close().await;
}
#[tokio::test]
async fn oversized_selected_path_is_refused_not_truncated_or_session_fatal() {
    let f = Fixture::new(
        SessionOptions {
            event_bytes: PATH_BYTES + 64 * 1024,
            ..Default::default()
        },
        false,
    )
    .await;
    let view = f.bridge.output_events().unwrap();
    let sub = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    f.emit(
        "RecordStateChanged",
        started(json!("x".repeat(PATH_BYTES + 1))),
    )
    .await;
    f.observed().await;
    assert_eq!(
        sub.receive(Instant::now() + WATCHDOG).await.unwrap_err(),
        Error::PathTooLarge
    );
    assert!(f.owner.handle().invalidation().is_none());
    f.close().await;
}
#[tokio::test]
async fn known_terminal_wins_later_revocation_but_pending_clones_settle() {
    let f = Fixture::new(Default::default(), false).await;
    let view = f.bridge.output_events().unwrap();
    let retained = view.clone();
    let known = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    let pending = view
        .subscribe(OutputKind::Stream, Instant::now() + WATCHDOG)
        .unwrap();
    f.emit("RecordStateChanged", started(json!("known"))).await;
    f.observed().await;
    f.bridge.invalidate();
    assert_eq!(
        known
            .receive(Instant::now() + WATCHDOG)
            .await
            .unwrap()
            .output_path(),
        Some("known")
    );
    assert_eq!(
        pending
            .receive(Instant::now() + WATCHDOG)
            .await
            .unwrap_err(),
        Error::Revoked
    );
    assert_eq!(
        retained
            .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
            .unwrap_err(),
        Error::Revoked
    );
    f.close().await;
}
#[tokio::test]
async fn owner_drop_and_real_disconnect_revoke_pending_waiters() {
    let f = Fixture::new(Default::default(), false).await;
    let view = f.bridge.output_events().unwrap();
    let sub = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    let Fixture {
        owner,
        bridge,
        commands,
        peer,
        connections,
        ..
    } = f;
    drop(bridge);
    assert_eq!(
        sub.receive(Instant::now() + WATCHDOG).await.unwrap_err(),
        Error::Revoked
    );
    assert_eq!(
        view.subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
            .unwrap_err(),
        Error::Revoked
    );
    owner.shutdown().await.unwrap();
    drop(commands);
    peer.await.unwrap();
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    let f = Fixture::new(Default::default(), false).await;
    let view = f.bridge.output_events().unwrap();
    let sub = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    let (done, wait) = oneshot::channel();
    f.commands.send(Command::Close(done)).await.unwrap();
    wait.await.unwrap();
    assert_eq!(
        timeout(WATCHDOG, sub.receive(Instant::now() + WATCHDOG))
            .await
            .unwrap()
            .unwrap_err(),
        Error::Revoked
    );
    f.close().await;
}
#[tokio::test]
async fn original_take_frame_permits_and_independent_confirmation_are_preserved() {
    let f = Fixture::new(
        SessionOptions {
            event_items: 1,
            event_subscriptions: obs_mcp_native::takes::REQUIRED_SUBSCRIPTIONS | (1 << 19),
            ..Default::default()
        },
        false,
    )
    .await;
    let mut feed = f.bridge.attach_take_feed().unwrap();
    let fence = feed.fence();
    let mut receiver = feed.take_receiver().unwrap();
    let view = f.bridge.output_events().unwrap();
    let sub = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    f.emit("RecordStateChanged", started(json!("owned"))).await;
    f.observed().await;
    let held = receiver.recv().await.unwrap();
    assert!(!fence.ready());
    assert_eq!(
        sub.receive(Instant::now() + WATCHDOG)
            .await
            .unwrap()
            .output_path(),
        Some("owned")
    );
    assert!(
        !fence.ready(),
        "confirmation falsely acknowledged take application"
    );
    let pending = view
        .subscribe(OutputKind::Stream, Instant::now() + WATCHDOG)
        .unwrap();
    f.emit("StreamStateChanged", started(Value::Null)).await;
    assert_eq!(
        timeout(WATCHDOG, pending.receive(Instant::now() + WATCHDOG))
            .await
            .unwrap()
            .unwrap_err(),
        Error::Revoked
    );
    assert!(!fence.ready());
    drop(held);
    drop(feed);
    f.close().await;
}
#[tokio::test]
async fn output_subscription_requires_actual_outputs_mask() {
    let f = Fixture::new(
        SessionOptions {
            event_subscriptions: 1,
            ..Default::default()
        },
        false,
    )
    .await;
    assert_eq!(
        f.bridge.output_events().unwrap_err(),
        Error::IncompleteEvents
    );
    assert_eq!(f.requests.load(Ordering::SeqCst), 0);
    f.close().await;
}

struct Gate {
    entered: Notify,
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            open: Mutex::new(false),
            changed: Condvar::new(),
        }
    }
    fn hold(&self) {
        self.entered.notify_one();
        let guard = self.open.lock().unwrap();
        let (_guard, result) = self
            .changed
            .wait_timeout_while(guard, WATCHDOG, |open| !*open)
            .unwrap();
        assert!(
            !result.timed_out(),
            "owned test observer gate exceeded watchdog"
        );
    }
    fn arm(&self) {
        *self.open.lock().unwrap() = false;
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
static GATE: OnceLock<Arc<Gate>> = OnceLock::new();
fn held_filter(name: &str) -> bool {
    if name == "OutputEventsContractHold" {
        GATE.get().unwrap().hold();
    }
    false
}
struct BridgeThread {
    finish: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    gate: Arc<Gate>,
}
impl BridgeThread {
    fn join(mut self) {
        self.gate.release();
        self.finish.take().unwrap().send(()).unwrap();
        self.thread.take().unwrap().join().unwrap();
    }
}
impl Drop for BridgeThread {
    fn drop(&mut self) {
        self.gate.release();
        if let Some(finish) = self.finish.take() {
            let _ = finish.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
#[tokio::test]
async fn held_bridge_uses_physical_time_and_cutoff_settlement_without_take_fence() {
    let gate = GATE.get_or_init(|| Arc::new(Gate::new())).clone();
    let SocketFixture {
        owner,
        events,
        commands,
        peer,
        connections,
        ..
    } = SocketFixture::new(Default::default(), false).await;
    let handle = owner.handle();
    let (finish, finished) = oneshot::channel();
    let (built, bridge_ready) = oneshot::channel();
    let worker_gate = gate.clone();
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let bridge =
                ObservationBridge::bind_with_event_filter(handle, events, None, held_filter)
                    .unwrap();
            built
                .send(bridge)
                .unwrap_or_else(|_| panic!("bridge receiver dropped"));
            finished.await.unwrap();
        });
        worker_gate.release();
    });
    // Declared before bridge so assertion unwind revokes/drops the owner first.
    let worker = BridgeThread {
        finish: Some(finish),
        thread: Some(thread),
        gate: gate.clone(),
    };
    let bridge = bridge_ready.await.unwrap();
    let view = bridge.output_events().unwrap();
    // A physically received old event is still rejected even while publication
    // is held: registration uses intrinsic cursor, not bridge progress.
    gate.arm();
    emit(&commands, "OutputEventsContractHold", json!({})).await;
    timeout(WATCHDOG, gate.entered.notified()).await.unwrap();
    emit(&commands, "RecordStateChanged", started(json!("old"))).await;
    received(&owner.handle()).await;
    let stale = view
        .subscribe(
            OutputKind::Record,
            Instant::now() + Duration::from_millis(100),
        )
        .unwrap();
    gate.release();
    bridge.synchronize(Instant::now() + WATCHDOG).await.unwrap();
    assert_eq!(
        stale.receive(Instant::now() + WATCHDOG).await.unwrap_err(),
        Error::ConfirmationExpired
    );
    // ACK arrives before confirmation expiry, but physical timely event remains
    // queued behind held bridge. Expiry must drain its projection cutoff.
    let confirmation = Instant::now() + Duration::from_millis(200);
    let sub = view.subscribe(OutputKind::Record, confirmation).unwrap();
    gate.arm();
    emit(&commands, "OutputEventsContractHold", json!({})).await;
    timeout(WATCHDOG, gate.entered.notified()).await.unwrap();
    emit(&commands, "RecordStateChanged", started(json!("timely"))).await;
    received(&owner.handle()).await;
    assert!(
        Instant::now() < confirmation,
        "fixture missed early-ACK boundary"
    );
    let mut waiting = Box::pin(sub.receive(Instant::now() + WATCHDOG));
    assert!(futures_util::poll!(waiting.as_mut()).is_pending());
    tokio::time::sleep_until(confirmation + Duration::from_millis(5)).await;
    assert!(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "timely physical event was falsely timed out while observer held"
    );
    gate.release();
    let event = waiting.await.unwrap();
    assert_eq!(event.output_path(), Some("timely"));
    assert!(event.received_at() <= confirmation);
    // The same unsettled prefix has an explicit finite local limit rather than
    // claiming an output timeout or blocking forever.
    let confirmation = Instant::now() + Duration::from_millis(100);
    let sub = view.subscribe(OutputKind::Stream, confirmation).unwrap();
    gate.arm();
    emit(&commands, "OutputEventsContractHold", json!({})).await;
    timeout(WATCHDOG, gate.entered.notified()).await.unwrap();
    emit(&commands, "StreamStateChanged", started(Value::Null)).await;
    received(&owner.handle()).await;
    assert_eq!(
        sub.receive(confirmation + Duration::from_millis(20))
            .await
            .unwrap_err(),
        Error::SettlementUnavailable
    );
    gate.release();
    bridge.synchronize(Instant::now() + WATCHDOG).await.unwrap();
    // Already projected timely evidence can still be consumed after a late ACK
    // or caller poll, without changing its timestamp to application time.
    let confirmation = Instant::now() + Duration::from_millis(100);
    let sub = view
        .subscribe(OutputKind::VirtualCam, confirmation)
        .unwrap();
    emit(
        &commands,
        "VirtualcamStateChanged",
        started(json!("late-poll")),
    )
    .await;
    received(&owner.handle()).await;
    bridge.synchronize(Instant::now() + WATCHDOG).await.unwrap();
    tokio::time::sleep_until(confirmation + Duration::from_millis(2)).await;
    assert!(sub.receive(Instant::now() + WATCHDOG).await.is_ok());
    owner.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    drop(commands);
    peer.await.unwrap();
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    worker.join();
}

#[tokio::test]
async fn late_terminal_cannot_spend_budget_or_report_projection_error_after_confirmation() {
    let f = Fixture::new(Default::default(), false).await;
    let view = f.bridge.output_events().unwrap();
    let a = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    let b = view
        .subscribe(OutputKind::Stream, Instant::now() + WATCHDOG)
        .unwrap();
    let path = "x".repeat(PATH_BYTES / 2);
    f.emit("RecordStateChanged", started(json!(path))).await;
    f.observed().await;
    f.emit("StreamStateChanged", started(json!(path))).await;
    f.observed().await;
    let a = a.receive(Instant::now() + WATCHDOG).await.unwrap();
    let b = b.receive(Instant::now() + WATCHDOG).await.unwrap();
    let confirmation = Instant::now() + Duration::from_millis(20);
    let expired = view.subscribe(OutputKind::Record, confirmation).unwrap();
    tokio::time::sleep_until(confirmation + Duration::from_millis(1)).await;
    f.emit("RecordStateChanged", started(json!("outside-window")))
        .await;
    f.observed().await;
    let result = expired.receive(Instant::now() + WATCHDOG).await;
    drop(a);
    drop(b);
    f.close().await;
    assert_eq!(result.unwrap_err(), Error::ConfirmationExpired);
}

#[tokio::test]
async fn late_terminal_does_not_retain_budget_against_another_live_output() {
    let f = Fixture::new(Default::default(), false).await;
    let view = f.bridge.output_events().unwrap();
    let a = view
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    let b = view
        .subscribe(OutputKind::Stream, Instant::now() + WATCHDOG)
        .unwrap();
    let path = "x".repeat(PATH_BYTES / 2);
    f.emit("RecordStateChanged", started(json!(path))).await;
    f.observed().await;
    f.emit("StreamStateChanged", started(json!(path))).await;
    f.observed().await;
    let a = a.receive(Instant::now() + WATCHDOG).await.unwrap();
    let b = b.receive(Instant::now() + WATCHDOG).await.unwrap();
    drop(a);
    let confirmation = Instant::now() + Duration::from_millis(20);
    let expired = view.subscribe(OutputKind::Record, confirmation).unwrap();
    tokio::time::sleep_until(confirmation + Duration::from_millis(1)).await;
    f.emit("RecordStateChanged", started(json!(path))).await;
    f.observed().await;
    let live = view
        .subscribe(OutputKind::VirtualCam, Instant::now() + WATCHDOG)
        .unwrap();
    f.emit("VirtualcamStateChanged", started(json!("live")))
        .await;
    f.observed().await;
    let good = live.receive(Instant::now() + WATCHDOG).await;
    assert_eq!(
        expired
            .receive(Instant::now() + WATCHDOG)
            .await
            .unwrap_err(),
        Error::ConfirmationExpired
    );
    drop(b);
    f.close().await;
    assert!(
        good.is_ok(),
        "expired output consumed another live output's budget: {good:?}"
    );
}
