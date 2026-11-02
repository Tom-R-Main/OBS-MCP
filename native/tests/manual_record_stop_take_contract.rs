// SPDX-License-Identifier: GPL-2.0-only
//! Library composition only: real Session/Bridge/Take/store owners, fake host/media.
//! This does not qualify a public manual route, frontend retention, or live OBS.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    clock,
    control::ControlDomain,
    custody::Clock,
    operation_store::{GenericPhase, OperationStore},
    preflight::{self, Report},
    receipt_broker::{ReceiptBroker, ScopeKind},
    record_impact::RecordImpactOwner,
    record_stop_transport::{BoundManualRecordStop, Request},
    session::{ObsSession, SessionOptions},
    session_observation::ObservationBridge,
    stage_transport::Cancellation,
    take_monitor::Summary,
    take_transport::{BoundTakeIo, Bounds, HostServices, PreflightReader},
    takes::{self, ClipCheck, Event, IoFuture, Limits, Loan, Policy, StartOptions, TakeSupervisor},
};
use serde_json::{Value, json};
use std::{
    future::Future,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(8);
const PATH: &str = "/fixture/same-path.mkv";
#[derive(Default)]
struct Host {
    joined: AtomicBool,
    inspections: AtomicU64,
}
impl HostServices for Host {
    fn preflight(&self, _: PreflightReader, _: preflight::Options) -> IoFuture<Report> {
        Box::pin(async {
            Report {
                ready: true,
                checks: vec![preflight::Check {
                    id: "record-directory".into(),
                    status: preflight::Status::Pass,
                    message: "explicit fake host".into(),
                }],
            }
        })
    }
    fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
        self.inspections.fetch_add(1, Ordering::Relaxed);
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
#[derive(Default)]
struct State {
    active: bool,
    replacement: bool,
    stops: usize,
    requests: Vec<Value>,
    joined: bool,
}
enum Operator {
    Replace(oneshot::Sender<()>),
    Inactive(oneshot::Sender<()>),
}
// Moves original permitted events linearly. It is not another physical receiver,
// observer, or relay task; every owner below is the production implementation.
async fn pump<T>(
    future: impl Future<Output = T>,
    source: &mut mpsc::Receiver<Event>,
    destination: &mpsc::Sender<Event>,
) -> T {
    tokio::pin!(future);
    let mut open = true;
    loop {
        tokio::select! {biased;r=&mut future=>return r,e=source.recv(),if open=>match e{Some(e)=>destination.send(e).await.unwrap(),None=>open=false}}
    }
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"));
        let p = root.join(format!(
            "manual-take-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[tokio::test]
async fn durable_manual_ack_revokes_old_take_without_events_and_never_stops_same_path_replacement()
{
    let directory = Dir::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(State::default()));
    let observed = state.clone();
    let (operator, mut commands) = mpsc::channel(2);
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
                command=commands.recv()=>match command{Some(Operator::Replace(done))=>{let mut s=observed.lock().unwrap();s.active=true;s.replacement=true;done.send(()).unwrap();},Some(Operator::Inactive(done))=>{observed.lock().unwrap().active=false;done.send(()).unwrap();},None=>break},
                packet=peer.next()=>match packet{
                    Some(Ok(Message::Text(text)))=>{
                        let request:Value=serde_json::from_str(&text).unwrap();assert_eq!(request["op"],6);observed.lock().unwrap().requests.push(request.clone());let kind=request["d"]["requestType"].as_str().unwrap();
                        let data=match kind{
                            "GetVersion"=>json!({"availableRequests":["GetVersion","GetRecordStatus","GetStats","GetCurrentProgramScene","GetInputList","GetProfileParameter","GetInputAudioTracks","StartRecord","StopRecord","CreateRecordChapter"],"supportedImageFormats":[],"platform":"macos"}),
                            "GetInputList"=>json!({"inputs":[]}),
                            "GetCurrentProgramScene"=>json!({"currentProgramSceneName":"Main","currentProgramSceneUuid":"main"}),
                            "GetStats"=>json!({"availableDiskSpace":50000,"renderSkippedFrames":0,"renderTotalFrames":0}),
                            "GetProfileParameter"=>{let simple=request["d"]["requestData"]["parameterCategory"]=="Output"&&request["d"]["requestData"]["parameterName"]=="Mode";json!({"parameterValue":if simple{"Simple"}else{"1"}})},
                            "StartRecord"=>{observed.lock().unwrap().active=true;peer.send(Message::Text(json!({"op":5,"d":{"eventType":"RecordStateChanged","eventIntent":64,"eventData":{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputActive":true,"outputPath":PATH}}}).to_string().into())).await.unwrap();json!({})},
                            "StopRecord"=>{let mut s=observed.lock().unwrap();s.stops+=1;s.active=false;assert!(!s.replacement,"old automatic ticket stopped a replacement recording");json!({"outputPath":PATH})},
                            "GetRecordStatus"=>json!({"outputActive":observed.lock().unwrap().active,"outputPaused":false,"outputBytes":1048576}),
                            "CreateRecordChapter"=>json!({}),other=>panic!("unexpected RPC {other}"),
                        };
                        // Stop/replacement events are deliberately never emitted.
                        peer.send(Message::Text(json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":kind,"requestStatus":{"result":true,"code":100},"responseData":data}}).to_string().into())).await.unwrap();
                    },Some(Ok(Message::Close(_)))|Some(Err(_))|None=>{let _=peer.flush().await;break;},_=>{},
                }
            }
        }
        let mut unexpected_connection = Box::pin(listener.accept());
        assert!(
            futures_util::poll!(unexpected_connection.as_mut()).is_pending(),
            "unexpected second physical connection"
        );
        observed.lock().unwrap().joined = true;
    });
    let (physical, events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            event_subscriptions: takes::REQUIRED_SUBSCRIPTIONS | (1 << 19),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let handle = physical.handle();
    let bridge = ObservationBridge::bind(handle.clone(), events, None).unwrap();
    let mut feed = bridge.attach_take_feed().unwrap();
    let fence = feed.fence();
    let mut source = feed.take_receiver().unwrap();
    let (destination, loan_events) = mpsc::channel(256);
    let host = Arc::new(Host::default());
    let io = Arc::new(
        BoundTakeIo::bind(
            handle.clone(),
            Arc::new(fence.clone()),
            host.clone(),
            &url,
            Arc::new(AtomicBool::new(false)),
            Bounds {
                rpc: WAIT,
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let clock: Clock =
        Arc::new(|| clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap());
    let control = Arc::new(Mutex::new(ControlDomain::default()));
    // Strong impact owner is retained until after Take.shutdown returns.
    let impact = RecordImpactOwner::bind(&handle).unwrap();
    let driver = BoundManualRecordStop::bind(handle.clone(), impact.manual_port()).unwrap();
    let broker = ReceiptBroker::new(OperationStore::new(&directory.0, &url).unwrap()).unwrap();
    let mut ids = 0;
    let mut take = TakeSupervisor::spawn_with_record_impact(
        Loan {
            session: handle.clone(),
            store: broker.scope(ScopeKind::Take).unwrap(),
            control: control.clone(),
            clock: clock.clone(),
            identities: Box::new(move || {
                ids += 1;
                format!("00000000-0000-4000-8000-{ids:012x}")
            }),
            io,
            events: loan_events,
        },
        Limits {
            rpc: WAIT,
            setup: WAIT,
            stop: Duration::from_millis(200),
            inspection: WAIT,
            file: WAIT,
            start_confirmation: WAIT,
            stats_interval: Duration::from_secs(60),
            picture_interval: Duration::from_secs(60),
        },
        impact.view(),
    )
    .unwrap_or_else(|f| panic!("spawn {:?}", f.reason));
    let take_handle = take.handle();
    let started = timeout(
        WAIT,
        pump(
            take_handle.start(
                StartOptions {
                    expect_silent: true,
                    max_duration_seconds: Some(1),
                    ..Default::default()
                },
                Policy {
                    caller: "fixture".into(),
                    ..Default::default()
                },
            ),
            &mut source,
            &destination,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_ne!(started["isError"], true, "{started}");
    assert_eq!(started["structuredContent"]["ownership"], "continuous");
    fence.synchronize(Instant::now() + WAIT).await.unwrap();
    let cursor = handle.event_cursor();
    let manual = timeout(
        WAIT,
        pump(
            driver.execute(
                broker.scope(ScopeKind::ManualRecordStop).unwrap(),
                Request {
                    operation_id: "bbbbbbbb-bbbb-4bbb-8bbb-000000000999".into(),
                    capabilities: None,
                    policy: Policy {
                        caller: "fixture".into(),
                        ..Default::default()
                    },
                    control,
                    clock,
                    cancellation: Cancellation::default(),
                    admitted_at: Instant::now(),
                    timeout: WAIT,
                },
            ),
            &mut source,
            &destination,
        ),
    )
    .await
    .unwrap();
    assert!(matches!(manual.transport, Some(Ok(()))));
    assert_eq!(manual.receipt.unwrap().phase(), GenericPhase::Accepted);
    assert_eq!(
        manual.result["structuredContent"]["message"],
        format!("Recording stopped, saved to: {PATH}")
    );
    let (done, wait) = oneshot::channel();
    operator.send(Operator::Replace(done)).await.unwrap();
    wait.await.unwrap();
    let fresh = handle
        .request(handle.token(), "GetRecordStatus", Value::Null, WAIT)
        .await
        .unwrap();
    assert_eq!(fresh["outputActive"], true);
    assert_eq!(state.lock().unwrap().stops, 1);
    bridge.synchronize(Instant::now() + WAIT).await.unwrap();
    assert_eq!(
        handle.event_cursor(),
        cursor,
        "counter requires no stop/replacement notifications"
    );
    // Zero-effect observation crosses the original one-second Take deadline.
    // No explicit Take stop is used to substitute for its automatic deadline.
    timeout(
        WAIT,
        pump(
            tokio::time::sleep(Duration::from_millis(1200)),
            &mut source,
            &destination,
        ),
    )
    .await
    .unwrap();
    assert_eq!(take_handle.snapshot()["ownership"], "unavailable");
    assert_eq!(state.lock().unwrap().stops, 1);
    assert!(state.lock().unwrap().active);
    assert_eq!(host.inspections.load(Ordering::Acquire), 0);
    // Explicit fake operator cleanup is separate from native Stop authority.
    let (done, wait) = oneshot::channel();
    operator.send(Operator::Inactive(done)).await.unwrap();
    wait.await.unwrap();
    let joined = timeout(WAIT, pump(take.shutdown(), &mut source, &destination))
        .await
        .unwrap()
        .unwrap();
    assert!(joined.joined);
    assert!(joined.observers_joined);
    assert!(take.take_store().is_some());
    assert!(host.joined.load(Ordering::Acquire));
    drop(impact);
    drop(feed);
    drop(destination);
    physical.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    drop(operator);
    timeout(WAIT, peer).await.unwrap().unwrap();
    assert!(state.lock().unwrap().joined);
    assert_eq!(state.lock().unwrap().stops, 1);
    assert!(broker.into_store().is_ok());
    let manual_disk: Value = serde_json::from_slice(
        &std::fs::read(
            directory
                .0
                .join("operation-receipts/bbbbbbbb-bbbb-4bbb-8bbb-000000000999.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(manual_disk["phase"], "accepted");
    assert_eq!(manual_disk["postconditionObserved"], false);
    assert!(manual_disk.get("outputPath").is_none());
}
