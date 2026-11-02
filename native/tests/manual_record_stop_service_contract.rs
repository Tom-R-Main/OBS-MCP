// SPDX-License-Identifier: GPL-2.0-only
//! Actual Service ownership; fake OBS and host/media, no public MCP admission.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    clock,
    control::ControlDomain,
    custody::SharedControl,
    generic_requests::Call,
    observation::ReadRequest,
    operation_store::OperationStore,
    preflight::{self, Report},
    service::{
        ConnectionDescriptor, Options, REQUIRED_SUBSCRIPTIONS, Resources, Service, ServiceError,
        ServiceHandle, TakeServices,
    },
    session::{ObsSession, SessionHandle, SessionOptions},
    session_observation::ObservationBridge,
    stage_transport::Cancellation,
    take_monitor::Summary,
    take_transport::{Bounds, HostServices, PreflightReader},
    takes::{ClipCheck, IoFuture, Limits, Policy, StartOptions},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(8);
const EPOCH: u64 = 1_793_448_000_000;
fn policy() -> Policy {
    Policy {
        caller: "fixture".into(),
        ..Default::default()
    }
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let base = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"));
        fs::create_dir_all(&base).unwrap();
        let p = base.join(format!(
            "manual-service-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn receipts(&self) -> Vec<Value> {
        let p = self.0.join("operation-receipts");
        if !p.exists() {
            return vec![];
        }
        fs::read_dir(p)
            .unwrap()
            .map(|p| serde_json::from_slice(&fs::read(p.unwrap().path()).unwrap()).unwrap())
            .collect()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Default)]
struct Host {
    joined: AtomicBool,
    inspected: AtomicU64,
}
impl HostServices for Host {
    fn preflight(&self, _: PreflightReader, _: preflight::Options) -> IoFuture<Report> {
        Box::pin(async {
            Report {
                ready: true,
                checks: vec![preflight::Check {
                    id: "record-directory".into(),
                    status: preflight::Status::Pass,
                    message: "fake host".into(),
                }],
            }
        })
    }
    fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
        self.inspected.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            ClipCheck {
                skipped: Some("fake media".into()),
                ..Default::default()
            }
        })
    }
    fn write_log(&self, _: String, _: Summary) -> IoFuture<Option<String>> {
        Box::pin(async { None })
    }
    fn join_observers(&self) -> IoFuture<Result<(), String>> {
        self.joined.store(true, Ordering::Release);
        Box::pin(async { Ok(()) })
    }
}
enum PeerCommand {
    Frame(Value),
    Active(bool, oneshot::Sender<()>),
}
struct Fixture {
    url: String,
    trace: Arc<Mutex<Vec<Value>>>,
    hold: Arc<Mutex<HashSet<String>>>,
    held: Arc<Mutex<HashMap<String, Value>>>,
    supported: Arc<AtomicBool>,
    active: Arc<AtomicBool>,
    wake: Arc<Notify>,
    out: mpsc::Sender<PeerCommand>,
    peer: JoinHandle<()>,
    control: SharedControl,
    host: Arc<Host>,
    physical: Mutex<Option<SessionHandle>>,
}
impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let trace = Arc::new(Mutex::new(vec![]));
        let hold = Arc::new(Mutex::new(HashSet::<String>::new()));
        let held = Arc::new(Mutex::new(HashMap::new()));
        let supported = Arc::new(AtomicBool::new(true));
        let active = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Notify::new());
        let (out, mut commands) = mpsc::channel(16);
        let (history, holding, pending, support, recording, changed) = (
            trace.clone(),
            hold.clone(),
            held.clone(),
            supported.clone(),
            active.clone(),
            wake.clone(),
        );
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(socket).await.unwrap();
            peer.send(Message::Text(
                json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            assert!(matches!(peer.next().await, Some(Ok(Message::Text(_)))));
            peer.send(Message::Text(
                json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            loop {
                tokio::select! {
                 command=commands.recv()=>match command{Some(PeerCommand::Frame(frame))=>{peer.send(Message::Text(frame.to_string().into())).await.unwrap();},Some(PeerCommand::Active(active,done))=>{recording.store(active,Ordering::Release);done.send(()).unwrap();},None=>break},
                 message=peer.next()=>match message{
                  Some(Ok(Message::Text(text)))=>{
                   let request:Value=serde_json::from_str(&text).unwrap();assert_eq!(request["op"],6);history.lock().unwrap().push(request.clone());let kind=request["d"]["requestType"].as_str().unwrap();
                   let data=match kind{
                    "GetVersion"=>{let mut requests=vec!["GetVersion","GetRecordStatus","GetStats","GetCurrentProgramScene","GetInputList","GetProfileParameter","GetInputAudioTracks","StartRecord","CreateRecordChapter","SetInputMute","StopStream","StopVirtualCam","StopReplayBuffer"];if support.load(Ordering::Acquire){requests.push("StopRecord");}json!({"availableRequests":requests,"supportedImageFormats":[],"platform":"macos"})},
                    "GetInputList"=>json!({"inputs":[]}),"GetCurrentProgramScene"=>json!({"currentProgramSceneName":"Main","currentProgramSceneUuid":"main"}),"GetStats"=>json!({"availableDiskSpace":50000,"renderSkippedFrames":0,"renderTotalFrames":0}),"GetProfileParameter"=>json!({"parameterValue":if request["d"]["requestData"]["parameterCategory"]=="Output"&&request["d"]["requestData"]["parameterName"]=="Mode"{"Simple"}else{"1"}}),
                    "StartRecord"=>{recording.store(true,Ordering::Release);peer.send(Message::Text(json!({"op":5,"d":{"eventType":"RecordStateChanged","eventIntent":64,"eventData":{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputActive":true,"outputPath":"/fixture/same.mkv"}}}).to_string().into())).await.unwrap();json!({})},
                    "StopRecord"=>{recording.store(false,Ordering::Release);json!({"outputPath":"/fixture/same.mkv"})},
                    "GetRecordStatus"=>json!({"outputActive":recording.load(Ordering::Acquire),"outputPaused":false,"outputBytes":1048576}),"SetInputMute"|"StopStream"|"StopVirtualCam"|"StopReplayBuffer"|"CreateRecordChapter"=>json!({}),other=>panic!("unexpected RPC {other}")};
                   let reply=json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":kind,"requestStatus":{"result":true,"code":100},"responseData":data}});
                   if holding.lock().unwrap().remove(kind){pending.lock().unwrap().insert(kind.into(),reply);}else{peer.send(Message::Text(reply.to_string().into())).await.unwrap();}changed.notify_waiters();
                  },Some(Ok(Message::Close(_)))|Some(Err(_))|None=>{let _=peer.flush().await;break;},_=>{},
                 }
                }
            }
            let mut unexpected = Box::pin(listener.accept());
            assert!(
                poll!(unexpected.as_mut()).is_pending(),
                "second physical connection"
            );
        });
        Self {
            url,
            trace,
            hold,
            held,
            supported,
            active,
            wake,
            out,
            peer,
            control: Arc::new(Mutex::new(ControlDomain::default())),
            host: Arc::new(Host::default()),
            physical: Mutex::new(None),
        }
    }
    fn count(&self, kind: &str) -> usize {
        self.trace
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v["d"]["requestType"] == kind)
            .count()
    }
    fn hold(&self, kind: &str) {
        assert!(self.hold.lock().unwrap().insert(kind.into()));
    }
    async fn seen(&self, kind: &str, n: usize) {
        timeout(WAIT, async {
            loop {
                let wake = self.wake.notified();
                if self.count(kind) >= n && (!self.hold.lock().unwrap().contains(kind)) {
                    break;
                }
                wake.await;
            }
        })
        .await
        .unwrap();
    }
    async fn release(&self, kind: &str) {
        let reply = self.held.lock().unwrap().remove(kind).unwrap();
        self.out.send(PeerCommand::Frame(reply)).await.unwrap();
    }
    async fn active(&self, active: bool) {
        let (done, wait) = oneshot::channel();
        self.out
            .send(PeerCommand::Active(active, done))
            .await
            .unwrap();
        wait.await.unwrap();
    }
    async fn service(&self, dir: &Dir, duration: Duration, take: bool, warm: bool) -> Service {
        self.service_with_setup(dir, duration, take, warm, WAIT)
            .await
    }
    async fn service_with_setup(
        &self,
        dir: &Dir,
        duration: Duration,
        take: bool,
        warm: bool,
        setup: Duration,
    ) -> Service {
        let (session, events) = ObsSession::connect(
            &self.url,
            None,
            SessionOptions {
                event_subscriptions: REQUIRED_SUBSCRIPTIONS | (1 << 19),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        *self.physical.lock().unwrap() = Some(session.handle());
        let bridge = ObservationBridge::bind(session.handle(), events, None).unwrap();
        let mut next = 0;
        let service = Service::spawn(
            Resources {
                session,
                bridge,
                store: OperationStore::new(&dir.0, &self.url).unwrap(),
                control: self.control.clone(),
                clock: Arc::new(|| {
                    clock::from_epoch_duration(Duration::from_millis(EPOCH)).unwrap()
                }),
                identities: Box::new(move || {
                    next += 1;
                    format!("00000000-0000-4000-8000-{next:012x}")
                }),
            },
            Options {
                operation_timeout: duration,
                connection: Some(ConnectionDescriptor {
                    url: self.url.clone(),
                    has_password: false,
                }),
                state_root: Some(dir.0.clone()),
                take: take.then(|| TakeServices {
                    host: self.host.clone(),
                    exact_endpoint: self.url.clone(),
                    pending_settings: Arc::new(AtomicBool::new(false)),
                    bounds: Bounds {
                        rpc: WAIT,
                        ..Default::default()
                    },
                    limits: Limits {
                        rpc: WAIT,
                        setup,
                        stop: Duration::from_millis(200),
                        start_confirmation: WAIT,
                        inspection: WAIT,
                        file: WAIT,
                        stats_interval: Duration::from_secs(60),
                        picture_interval: Duration::from_secs(60),
                    },
                }),
                ..Default::default()
            },
        )
        .unwrap_or_else(|(e, _)| panic!("spawn {e:?}"));
        if warm {
            service
                .handle()
                .read(ReadRequest::Version, Value::Null, duration.min(WAIT))
                .await
                .unwrap();
        }
        service
    }
    async fn joined(self) {
        timeout(WAIT, self.peer).await.unwrap().unwrap();
    }
}
fn manual(h: ServiceHandle, c: Cancellation) -> JoinHandle<Result<Value, ServiceError>> {
    tokio::spawn(async move { h.manual_record_stop(policy(), c).await })
}
async fn queued(h: &ServiceHandle, n: u64) {
    timeout(WAIT, async {
        loop {
            if h.summary().await.unwrap()["queuedSafetyStops"] == n {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn joined(service: &mut Service) {
    let r = timeout(WAIT, service.shutdown()).await.unwrap().unwrap();
    assert!(
        r.joined && r.observer_joined && r.socket_joined && r.take_joined && r.workers_joined,
        "{r:?}"
    );
}

#[tokio::test]
async fn manual_effect_during_actual_take_init_get_version_cannot_rearm_admitted_start() {
    let dir = Dir::new();
    let f = Fixture::new().await;
    let mut s = f.service(&dir, WAIT, true, true).await;
    let h = s.handle();
    f.hold("GetVersion");
    let take = tokio::spawn({
        let h = h.clone();
        async move {
            h.start_take(
                StartOptions {
                    expect_silent: true,
                    max_duration_seconds: Some(30),
                    ..Default::default()
                },
                policy(),
            )
            .await
        }
    });
    f.seen("GetVersion", 2).await;
    let manual = manual(h.clone(), Cancellation::default())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(manual["isError"], true, "{manual}");
    assert_eq!(f.count("StopRecord"), 1);
    f.release("GetVersion").await;
    let start = timeout(WAIT, take).await.unwrap().unwrap().unwrap();
    // Always join the reached production schedule before the intended red assertion.
    f.active(false).await;
    joined(&mut s).await;
    let starts = f.count("StartRecord");
    f.joined().await;
    assert_eq!(
        starts, 0,
        "admitted Take silently captured a new epoch after manual effect: {start}"
    );
    assert_eq!(start["isError"], true, "{start}");
}

#[tokio::test]
async fn no_impact_delayed_bind_keeps_ready_ticket_even_after_impact_wait_cutoff() {
    let dir = Dir::new();
    let f = Fixture::new().await;
    let mut s = f
        .service_with_setup(&dir, WAIT, true, true, Duration::from_millis(100))
        .await;
    let h = s.handle();
    f.hold("GetVersion");
    let take = tokio::spawn(async move {
        h.start_take(
            StartOptions {
                expect_silent: true,
                max_duration_seconds: Some(30),
                ..Default::default()
            },
            policy(),
        )
        .await
    });
    f.seen("GetVersion", 2).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    f.release("GetVersion").await;
    let result = timeout(WAIT, take).await.unwrap().unwrap().unwrap();
    assert_ne!(result["isError"], true, "{result}");
    assert_eq!(f.count("StartRecord"), 1);
    f.active(false).await;
    joined(&mut s).await;
    f.joined().await;
}
#[tokio::test]
async fn capability_missing_is_zero_intent_and_confirmation_still_precedes_capability() {
    let dir = Dir::new();
    let f = Fixture::new().await;
    f.supported.store(false, Ordering::Release);
    let mut s = f.service(&dir, WAIT, false, true).await;
    let h = s.handle();
    let confirmed = h
        .manual_record_stop(
            Policy {
                confirmation_enabled: true,
                can_elicit: true,
                ..policy()
            },
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        confirmed,
        json!({"resultType":"input_required","inputRequests":{"confirm":{"method":"elicitation/create","params":{"mode":"form","message":"Stop the recording?","requestedSchema":{"type":"object","properties":{"confirm":{"type":"boolean","title":"Confirm","description":"Stop the recording?"}},"required":["confirm"]}}}}})
    );
    let result = h
        .manual_record_stop(policy(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(
        result,
        json!({"content":[{"type":"text","text":"Error stopping recording: OBS WebSocket does not advertise support for request 'StopRecord'"}],"isError":true})
    );
    assert_eq!(f.count("StopRecord"), 0);
    assert_eq!(f.count("GetVersion"), 1);
    assert!(dir.receipts().is_empty());
    joined(&mut s).await;
    f.joined().await;
}
#[tokio::test]
async fn foreign_lease_does_not_block_stop_and_unknown_capability_needs_no_bootstrap_rpc() {
    let dir = Dir::new();
    let f = Fixture::new().await;
    f.control
        .lock()
        .unwrap()
        .claim("other operator", 1, "fixture", EPOCH)
        .unwrap();
    let mut s = f.service(&dir, WAIT, false, false).await;
    let result = manual(s.handle(), Cancellation::default())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(result["isError"], true, "{result}");
    assert_eq!(f.count("GetVersion"), 0);
    assert_eq!(f.count("StopRecord"), 1);
    assert_eq!(dir.receipts()[0]["phase"], "accepted");
    joined(&mut s).await;
    f.joined().await;
}
#[tokio::test]
async fn policy_payload_refuses_before_enqueue_and_manual_generic_share_one_eight_item_queue() {
    let dir = Dir::new();
    let f = Fixture::new().await;
    let mut s = f.service(&dir, WAIT, false, true).await;
    let h = s.handle();
    assert_eq!(
        h.manual_record_stop(
            Policy {
                caller: "x".repeat(8192),
                ..Default::default()
            },
            Cancellation::default()
        )
        .await,
        Err(ServiceError::Capacity)
    );
    assert_eq!(
        h.manual_record_stop(
            Policy {
                input_responses: Some(json!({"confirm":"x".repeat(8192)})),
                ..policy()
            },
            Cancellation::default()
        )
        .await,
        Err(ServiceError::Capacity)
    );
    assert!(dir.receipts().is_empty());
    f.hold("StopRecord");
    let first = manual(h.clone(), Cancellation::default());
    f.seen("StopRecord", 1).await;
    // Probe only the existing closed owned-status transport admission; this
    // does not bypass the Take domain's mandatory pending-impact veto.
    let physical = f.physical.lock().unwrap().as_ref().unwrap().clone();
    let status = physical
        .stop_record_status_checked(physical.token(), WAIT, || Ok(()))
        .await
        .unwrap();
    assert_eq!(status["outputActive"], false);
    let mut pending = vec![];
    for n in 0..8 {
        let cancel = Cancellation::default();
        let hh = h.clone();
        let cc = cancel.clone();
        let job = tokio::spawn(async move {
            if n % 2 == 0 {
                hh.manual_record_stop(policy(), cc).await
            } else {
                hh.generic(
                    Call::parse("obs-stop-stream", &json!({})).unwrap(),
                    "fixture".into(),
                    cc,
                )
                .await
            }
        });
        pending.push((cancel, job));
        queued(&h, n + 1).await;
    }
    assert_eq!(
        h.manual_record_stop(policy(), Cancellation::default())
            .await,
        Err(ServiceError::Capacity)
    );
    assert_eq!(
        h.generic(
            Call::parse("obs-stop-replay-buffer", &json!({})).unwrap(),
            "fixture".into(),
            Cancellation::default()
        )
        .await,
        Err(ServiceError::Capacity)
    );
    for (c, _) in &pending {
        c.cancel();
    }
    f.release("StopRecord").await;
    assert_ne!(first.await.unwrap().unwrap()["isError"], true);
    for (_, job) in pending {
        let value = job.await.unwrap().unwrap();
        assert_eq!(value["isError"], true);
    }
    assert_eq!(f.count("StopRecord"), 1);
    assert_eq!(f.count("StopStream"), 0);
    assert_eq!(f.count("StopReplayBuffer"), 0);
    assert_eq!(dir.receipts().len(), 1);
    joined(&mut s).await;
    f.joined().await;
}
#[tokio::test]
async fn shutdown_joins_manual_ack_after_frontend_abort_and_never_duplicates_stop() {
    let dir = Dir::new();
    let f = Fixture::new().await;
    let mut s = f.service(&dir, WAIT, true, true).await;
    let h = s.handle();
    let start = h
        .start_take(
            StartOptions {
                expect_silent: true,
                max_duration_seconds: Some(30),
                ..Default::default()
            },
            policy(),
        )
        .await
        .unwrap();
    assert_ne!(start["isError"], true, "{start}");
    f.hold("StopRecord");
    let job = manual(h.clone(), Cancellation::default());
    f.seen("StopRecord", 1).await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    let mut shutdown = Box::pin(s.shutdown());
    assert!(poll!(shutdown.as_mut()).is_pending());
    f.release("StopRecord").await;
    let result = timeout(WAIT, shutdown).await.unwrap().unwrap();
    assert!(
        result.joined
            && result.take_joined
            && result.socket_joined
            && result.observer_joined
            && result.workers_joined,
        "{result:?}"
    );
    assert_eq!(f.count("StopRecord"), 1);
    assert!(
        dir.receipts()
            .iter()
            .any(|v| v["requestType"] == "StopRecord"
                && v["phase"] == "accepted"
                && v["postconditionObserved"] == false)
    );
    f.joined().await;
}
#[tokio::test]
async fn service_manual_ack_withholds_events_and_does_not_stop_same_path_replacement_at_old_deadline()
 {
    let dir = Dir::new();
    let f = Fixture::new().await;
    let mut s = f.service(&dir, WAIT, true, true).await;
    let h = s.handle();
    let start = h
        .start_take(
            StartOptions {
                expect_silent: true,
                max_duration_seconds: Some(1),
                ..Default::default()
            },
            policy(),
        )
        .await
        .unwrap();
    assert_ne!(start["isError"], true, "{start}");
    let result = manual(h.clone(), Cancellation::default())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(result["isError"], true, "{result}");
    f.active(true).await;
    assert_eq!(
        h.read(ReadRequest::RecordStatus, Value::Null, WAIT)
            .await
            .unwrap()["outputActive"],
        true
    );
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(f.count("StopRecord"), 1);
    assert!(f.active.load(Ordering::Acquire));
    assert_eq!(f.host.inspected.load(Ordering::Acquire), 0);
    let resource = h.take_resource().await.unwrap();
    assert_eq!(
        resource,
        json!({"running":false}),
        "revoked old Take must not claim the replacement: {resource}"
    );
    f.active(false).await;
    joined(&mut s).await;
    assert_eq!(f.count("StopRecord"), 1);
    f.joined().await;
}
#[tokio::test]
async fn aborted_frontend_keeps_original_manual_deadline_and_unknown_receipt() {
    let dir = Dir::new();
    let f = Fixture::new().await;
    let duration = Duration::from_millis(200);
    let mut s = f.service(&dir, duration, false, true).await;
    f.hold("StopRecord");
    let began = tokio::time::Instant::now();
    let job = manual(s.handle(), Cancellation::default());
    f.seen("StopRecord", 1).await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    let r = timeout(WAIT, s.shutdown()).await.unwrap().unwrap();
    assert!(r.joined);
    assert!(began.elapsed() >= duration);
    assert_eq!(f.count("StopRecord"), 1);
    assert_eq!(dir.receipts()[0]["phase"], "unknown");
    // A healthy journal can faithfully retain Unknown; it must block admission.
    let broker =
        obs_mcp_native::receipt_broker::ReceiptBroker::new(s.take_store().unwrap()).unwrap();
    assert!(
        broker
            .scope(obs_mcp_native::receipt_broker::ScopeKind::ManualRecordStop)
            .is_err()
    );
    f.joined().await;
}

#[tokio::test]
async fn suspended_admitted_start_uses_original_cutoff_and_never_rearms_after_late_manual_ack() {
    let dir = Dir::new();
    let f = Fixture::new().await;
    let mut s = f
        .service_with_setup(&dir, WAIT, true, true, Duration::from_millis(100))
        .await;
    let h = s.handle();
    f.hold("GetVersion");
    let take = tokio::spawn({
        let h = h.clone();
        async move {
            h.start_take(
                StartOptions {
                    expect_silent: true,
                    ..Default::default()
                },
                policy(),
            )
            .await
        }
    });
    f.seen("GetVersion", 2).await;
    f.hold("StopRecord");
    let stop = manual(h, Cancellation::default());
    f.seen("StopRecord", 1).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    f.release("GetVersion").await;
    let start = timeout(WAIT, take).await.unwrap().unwrap().unwrap();
    assert_eq!(start["isError"], true, "{start}");
    assert_eq!(f.count("StartRecord"), 0);
    assert!(f.held.lock().unwrap().contains_key("StopRecord"));
    f.release("StopRecord").await;
    assert_ne!(stop.await.unwrap().unwrap()["isError"], true);
    joined(&mut s).await;
    assert_eq!(f.count("StartRecord"), 0);
    assert_eq!(f.count("StopRecord"), 1);
    f.joined().await;
}
#[tokio::test]
async fn policy_bytes_share_the_same_eight_kib_queue_budget() {
    let dir = Dir::new();
    let f = Fixture::new().await;
    let mut s = f.service(&dir, WAIT, false, true).await;
    let h = s.handle();
    f.hold("StopRecord");
    let first = manual(h.clone(), Cancellation::default());
    f.seen("StopRecord", 1).await;
    let mut waiting = vec![];
    for n in 0..2 {
        let cancel = Cancellation::default();
        let hh = h.clone();
        let cc = cancel.clone();
        waiting.push((
            cancel,
            tokio::spawn(async move {
                hh.manual_record_stop(
                    Policy {
                        caller: "x".repeat(4090),
                        ..Default::default()
                    },
                    cc,
                )
                .await
            }),
        ));
        queued(&h, n + 1).await;
    }
    assert_eq!(
        h.summary().await.unwrap()["queuedSafetyArgumentBytes"],
        8184
    );
    assert_eq!(
        h.manual_record_stop(policy(), Cancellation::default())
            .await,
        Err(ServiceError::Capacity)
    );
    for (c, _) in &waiting {
        c.cancel();
    }
    f.release("StopRecord").await;
    first.await.unwrap().unwrap();
    for (_, job) in waiting {
        assert_eq!(job.await.unwrap().unwrap()["isError"], true);
    }
    assert_eq!(f.count("StopRecord"), 1);
    joined(&mut s).await;
    f.joined().await;
}
#[tokio::test]
async fn acknowledged_manual_stop_keeps_known_ack_and_blocks_service_after_namespace_failure() {
    let dir = Dir::new();
    let f = Fixture::new().await;
    let mut s = f.service(&dir, WAIT, true, true).await;
    let h = s.handle();
    f.hold("StopRecord");
    let stop = manual(h.clone(), Cancellation::default());
    f.seen("StopRecord", 1).await;
    let receipts = dir.0.join("operation-receipts");
    let saved = dir.0.join("saved-receipts");
    fs::rename(&receipts, &saved).unwrap();
    fs::create_dir(&receipts).unwrap();
    fs::write(receipts.join("operator.txt"), "preserve").unwrap();
    f.release("StopRecord").await;
    let result = stop.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(result.to_string().contains("was acknowledged"), "{result}");
    assert_eq!(
        h.manual_record_stop(policy(), Cancellation::default())
            .await,
        Err(ServiceError::Uncertain)
    );
    assert_eq!(
        h.start_take(StartOptions::default(), policy()).await,
        Err(ServiceError::Uncertain)
    );
    assert_eq!(
        fs::read(receipts.join("operator.txt")).unwrap(),
        b"preserve"
    );
    let r = s.shutdown().await.unwrap();
    assert!(r.joined);
    assert!(!r.journal_available);
    assert_eq!(f.count("StopRecord"), 1);
    assert_eq!(f.count("StartRecord"), 0);
    f.joined().await;
}
