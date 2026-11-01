// SPDX-License-Identifier: GPL-2.0-only
//! Real one-socket Service ownership, live custody, bounded queues and joins.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    clock,
    control::ControlDomain,
    generic_requests::Call,
    observation::ReadRequest,
    operation_store::OperationStore,
    service::{
        ConnectionDescriptor, Options, REQUIRED_SUBSCRIPTIONS, Resources, Service, ServiceError,
        ServiceHandle,
    },
    session::{ObsSession, SessionOptions},
    session_observation::ObservationBridge,
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(6);
const EPOCH: u64 = 1_793_448_000_000;
fn id(n: u64) -> String {
    format!("00000000-0000-4000-8000-{n:012x}")
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "public-stop-service-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&p).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(p)
    }
    fn receipt(&self, n: u64) -> Value {
        serde_json::from_slice(
            &fs::read(
                self.0
                    .join("operation-receipts")
                    .join(format!("{}.json", id(n))),
            )
            .unwrap(),
        )
        .unwrap()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
struct Fixture {
    url: String,
    trace: Arc<Mutex<Vec<Value>>>,
    hold: Arc<Mutex<HashSet<String>>>,
    held: Arc<Mutex<HashMap<String, Value>>>,
    reject: Arc<Mutex<HashSet<String>>>,
    lose: Arc<Mutex<HashSet<String>>>,
    notify: Arc<Notify>,
    out: mpsc::Sender<Value>,
    task: JoinHandle<()>,
}
impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let trace = Arc::new(Mutex::new(vec![]));
        let hold = Arc::new(Mutex::new(HashSet::<String>::new()));
        let held = Arc::new(Mutex::new(HashMap::new()));
        let reject = Arc::new(Mutex::new(HashSet::<String>::new()));
        let lose = Arc::new(Mutex::new(HashSet::<String>::new()));
        let notify = Arc::new(Notify::new());
        let (out, mut incoming) = mpsc::channel::<Value>(8);
        let (history, holding, pending, rejected, lost, wake) = (
            trace.clone(),
            hold.clone(),
            held.clone(),
            reject.clone(),
            lose.clone(),
            notify.clone(),
        );
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            peer.send(Message::Text(
                json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let identify = peer.next().await.unwrap().unwrap().into_text().unwrap();
            assert_eq!(serde_json::from_str::<Value>(&identify).unwrap()["op"], 1);
            peer.send(Message::Text(
                json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            loop {
                tokio::select! {
                 message=peer.next()=>{let Some(Ok(message))=message else{return;};let Message::Text(text)=message else{let _=peer.flush().await;return;};let frame:Value=serde_json::from_str(&text).unwrap();let kind=frame["d"]["requestType"].as_str().unwrap().to_owned();history.lock().unwrap().push(frame.clone());wake.notify_waiters();if lost.lock().unwrap().remove(&kind){let _=peer.close(None).await;return;}
                  if holding.lock().unwrap().remove(&kind){pending.lock().unwrap().insert(kind,frame);continue;}
                  let data=if kind=="GetVersion"{json!({"obsVersion":"32.2.2","obsWebSocketVersion":"5.7.0","rpcVersion":1,"platform":"macos","availableRequests":["GetVersion","SetInputMute","StopStream","StopVirtualCam","StopReplayBuffer"],"supportedImageFormats":["png"]})}else{json!({"outputActive":true})};let status=if rejected.lock().unwrap().remove(&kind){json!({"result":false,"code":500,"comment":"PRIVATE exact Stop refusal"})}else{json!({"result":true,"code":100})};let response=json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":kind,"requestStatus":status,"responseData":data}});if peer.send(Message::Text(response.to_string().into())).await.is_err(){return;}
                 },
                 frame=incoming.recv()=>{let Some(frame)=frame else{return;};if peer.send(Message::Text(frame.to_string().into())).await.is_err(){return;}}
                }
            }
        });
        Self {
            url,
            trace,
            hold,
            held,
            reject,
            lose,
            notify,
            out,
            task,
        }
    }
    fn hold(&self, kind: &str) {
        self.hold.lock().unwrap().insert(kind.into());
    }
    fn count(&self, kind: &str) -> usize {
        self.trace
            .lock()
            .unwrap()
            .iter()
            .filter(|f| f["d"]["requestType"] == kind)
            .count()
    }
    async fn seen(&self, kind: &str, count: usize) {
        timeout(WAIT, async {
            loop {
                let signal = self.notify.notified();
                if self.count(kind) >= count {
                    break;
                }
                signal.await;
            }
        })
        .await
        .unwrap();
    }
    async fn release(&self, kind: &str) {
        let frame = self.held.lock().unwrap().remove(kind).unwrap();
        self.out.send(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":kind,"requestStatus":{"result":true,"code":100},"responseData":{"outputActive":true}}})).await.unwrap();
    }
    async fn service(&self, dir: &Dir, duration: Duration) -> Service {
        let (session, events) = ObsSession::connect(
            &self.url,
            None,
            SessionOptions {
                event_subscriptions: REQUIRED_SUBSCRIPTIONS,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let bridge = ObservationBridge::bind(session.handle(), events, None).unwrap();
        let mut next = 0;
        let resources = Resources {
            session,
            bridge,
            store: OperationStore::new(&dir.0, &self.url).unwrap(),
            control: Arc::new(Mutex::new(ControlDomain::default())),
            clock: Arc::new(|| clock::from_epoch_duration(Duration::from_millis(EPOCH)).unwrap()),
            identities: Box::new(move || {
                next += 1;
                id(next)
            }),
        };
        let service = Service::spawn(
            resources,
            Options {
                operation_timeout: duration,
                state_root: Some(dir.0.clone()),
                connection: Some(ConnectionDescriptor {
                    url: self.url.clone(),
                    has_password: false,
                }),
                ..Default::default()
            },
        )
        .map_err(|(e, _)| e)
        .unwrap();
        service
            .handle()
            .read(ReadRequest::Version, Value::Null, duration.min(WAIT))
            .await
            .unwrap();
        service
    }
    async fn joined(self) {
        timeout(WAIT, self.task).await.unwrap().unwrap();
    }
}
fn call(name: &str) -> Call {
    Call::parse(name, &json!({})).unwrap()
}
fn mute() -> Call {
    Call::parse(
        "obs-set-input-mute",
        &json!({"inputName":"Fixture","inputMuted":true}),
    )
    .unwrap()
}
fn job(
    handle: ServiceHandle,
    call: Call,
    cancel: Cancellation,
) -> JoinHandle<Result<Value, ServiceError>> {
    tokio::spawn(async move { handle.generic(call, "fixture".into(), cancel).await })
}
async fn summary_count(handle: &ServiceHandle, key: &str, count: u64) {
    timeout(WAIT, async {
        loop {
            if handle.summary().await.unwrap()[key] == count {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn all_three_public_stops_progress_while_ordinary_live_dispatching_and_normal_work_stays_fifo()
 {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let mut service = fixture.service(&dir, WAIT).await;
    let handle = service.handle();
    fixture.hold("SetInputMute");
    let ordinary = job(handle.clone(), mute(), Cancellation::new());
    fixture.seen("SetInputMute", 1).await;
    for (name, kind, message) in [
        (
            "obs-stop-stream",
            "StopStream",
            "Successfully stopped streaming",
        ),
        (
            "obs-stop-virtual-cam",
            "StopVirtualCam",
            "Virtual camera stopped",
        ),
        (
            "obs-stop-replay-buffer",
            "StopReplayBuffer",
            "Replay buffer stopped",
        ),
    ] {
        let result = timeout(
            WAIT,
            handle.generic(call(name), "foreign-caller".into(), Cancellation::new()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result["structuredContent"]["message"], message);
        assert_eq!(fixture.count(kind), 1);
        assert!(fixture.held.lock().unwrap().contains_key("SetInputMute"));
    }
    assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Busy);
    let queued = job(handle.clone(), mute(), Cancellation::new());
    summary_count(&handle, "queuedCommands", 1).await;
    assert_eq!(fixture.count("SetInputMute"), 1);
    assert_eq!(dir.receipt(1)["phase"], "dispatching");
    for n in 2..=4 {
        assert_eq!(dir.receipt(n)["phase"], "accepted");
        assert_eq!(dir.receipt(n)["postconditionObserved"], false);
    }
    fixture.release("SetInputMute").await;
    assert_ne!(ordinary.await.unwrap().unwrap()["isError"], true);
    assert_ne!(queued.await.unwrap().unwrap()["isError"], true);
    assert_eq!(fixture.count("SetInputMute"), 2);
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}
#[tokio::test]
async fn public_stop_queue_is_bounded_and_cancellation_never_creates_delayed_intent() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let mut service = fixture.service(&dir, WAIT).await;
    let handle = service.handle();
    fixture.hold("StopStream");
    let active = job(handle.clone(), call("obs-stop-stream"), Cancellation::new());
    fixture.seen("StopStream", 1).await;
    let huge = handle
        .generic(
            call("obs-stop-virtual-cam"),
            "x".repeat(8192),
            Cancellation::new(),
        )
        .await;
    assert_eq!(huge.unwrap_err(), ServiceError::Capacity);
    let mut queued = vec![];
    for _ in 0..8 {
        let cancel = Cancellation::new();
        queued.push((
            cancel.clone(),
            job(handle.clone(), call("obs-stop-replay-buffer"), cancel),
        ));
    }
    summary_count(&handle, "queuedSafetyStops", 8).await;
    assert_eq!(
        handle
            .generic(
                call("obs-stop-virtual-cam"),
                "fixture".into(),
                Cancellation::new()
            )
            .await
            .unwrap_err(),
        ServiceError::Capacity
    );
    assert_eq!(handle.summary().await.unwrap()["total"], 1);
    for (cancel, _) in &queued {
        cancel.cancel();
    }
    fixture.release("StopStream").await;
    assert_ne!(active.await.unwrap().unwrap()["isError"], true);
    for (_, job) in queued {
        assert_eq!(job.await.unwrap().unwrap()["isError"], true);
    }
    summary_count(&handle, "queuedSafetyStops", 0).await;
    assert_eq!(fixture.count("StopReplayBuffer"), 0);
    assert_eq!(fixture.count("StopVirtualCam"), 0);
    assert_eq!(handle.summary().await.unwrap()["total"], 1);
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}
#[tokio::test]
async fn frontend_abandonment_and_shutdown_join_the_same_safety_effect_before_return() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let mut service = fixture.service(&dir, WAIT).await;
    let handle = service.handle();
    fixture.hold("StopStream");
    let frontend = job(handle.clone(), call("obs-stop-stream"), Cancellation::new());
    fixture.seen("StopStream", 1).await;
    frontend.abort();
    let _ = frontend.await;
    let mut shutdown = Box::pin(service.shutdown());
    assert!(poll!(&mut shutdown).is_pending());
    assert_eq!(dir.receipt(1)["phase"], "dispatching");
    fixture.release("StopStream").await;
    assert!(timeout(WAIT, shutdown).await.unwrap().unwrap().joined);
    assert_eq!(dir.receipt(1)["phase"], "accepted");
    assert_eq!(dir.receipt(1)["postconditionObserved"], false);
    assert_eq!(fixture.count("StopStream"), 1);
    fixture.joined().await;
}
#[tokio::test]
async fn rejection_is_terminal_but_lost_reply_blocks_every_new_safety_stop_without_replay() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let mut service = fixture.service(&dir, WAIT).await;
    let handle = service.handle();
    fixture.reject.lock().unwrap().insert("StopStream".into());
    let result = handle
        .generic(
            call("obs-stop-stream"),
            "fixture".into(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result["isError"], true);
    assert!(result.to_string().contains("PRIVATE exact Stop refusal"));
    assert_eq!(dir.receipt(1)["phase"], "rejected");
    fixture.lose.lock().unwrap().insert("StopVirtualCam".into());
    let result = handle
        .generic(
            call("obs-stop-virtual-cam"),
            "fixture".into(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(dir.receipt(2)["phase"], "unknown");
    for name in ["obs-stop-stream", "obs-stop-replay-buffer"] {
        assert!(
            handle
                .generic(call(name), "fixture".into(), Cancellation::new())
                .await
                .is_err()
        );
    }
    assert_eq!(fixture.count("StopStream"), 1);
    assert_eq!(fixture.count("StopReplayBuffer"), 0);
    assert!(service.shutdown().await.unwrap().joined);
    let store = service.take_store().unwrap();
    let broker = obs_mcp_native::receipt_broker::ReceiptBroker::new(store).unwrap();
    assert!(
        broker
            .scope(obs_mcp_native::receipt_broker::ScopeKind::SafetyStop(
                obs_mcp_native::session::SafetyStopRequest::ReplayBuffer
            ))
            .is_err()
    );
    fixture.joined().await;
}
#[tokio::test]
async fn known_ack_publication_failure_remains_sticky_and_never_grants_a_retry() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let mut service = fixture.service(&dir, WAIT).await;
    let handle = service.handle();
    fixture.hold("StopStream");
    let active = job(handle.clone(), call("obs-stop-stream"), Cancellation::new());
    fixture.seen("StopStream", 1).await;
    let (receipts, saved) = (
        dir.0.join("operation-receipts"),
        dir.0.join("saved-receipts"),
    );
    fs::rename(&receipts, &saved).unwrap();
    fs::create_dir(&receipts).unwrap();
    fs::write(receipts.join("human.txt"), "preserve").unwrap();
    fixture.release("StopStream").await;
    let result = active.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(result.to_string().contains("outcome is uncertain"));
    assert_eq!(fs::read(receipts.join("human.txt")).unwrap(), b"preserve");
    fs::remove_file(receipts.join("human.txt")).unwrap();
    fs::remove_dir(&receipts).unwrap();
    fs::rename(saved, &receipts).unwrap();
    assert!(
        handle
            .generic(
                call("obs-stop-virtual-cam"),
                "fixture".into(),
                Cancellation::new()
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.count("StopStream"), 1);
    assert_eq!(fixture.count("StopVirtualCam"), 0);
    assert_eq!(dir.receipt(1)["phase"], "dispatching");
    let result = service.shutdown().await.unwrap();
    assert!(result.joined);
    assert!(!result.journal_available);
    fixture.joined().await;
}
#[tokio::test]
async fn unpolled_frontend_does_not_stop_the_absolute_deadline_or_authorize_replay() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let duration = Duration::from_secs(2);
    let mut service = fixture.service(&dir, duration).await;
    fixture.hold("StopStream");
    let started = tokio::time::Instant::now();
    let frontend = job(
        service.handle(),
        call("obs-stop-stream"),
        Cancellation::new(),
    );
    fixture.seen("StopStream", 1).await;
    frontend.abort();
    let _ = frontend.await;
    let receipt = timeout(WAIT, service.shutdown()).await.unwrap().unwrap();
    assert!(started.elapsed() >= duration);
    assert!(receipt.joined);
    assert_eq!(dir.receipt(1)["phase"], "unknown");
    assert_eq!(fixture.count("StopStream"), 1);
    let broker =
        obs_mcp_native::receipt_broker::ReceiptBroker::new(service.take_store().unwrap()).unwrap();
    assert!(
        broker
            .scope(obs_mcp_native::receipt_broker::ScopeKind::SafetyStop(
                obs_mcp_native::session::SafetyStopRequest::Stream
            ))
            .is_err()
    );
    fixture.joined().await;
}
