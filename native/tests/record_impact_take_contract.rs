// SPDX-License-Identifier: GPL-2.0-only
//! Isolated composition controls: actual native owners and one websocket.
//! Host preflight/media are explicit fakes; trusted raw manual RPC is not a
//! currently admitted public tool or a durable public-stop owner.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    clock,
    control::CallerContext,
    control::ControlDomain,
    custody::Clock,
    observation::ReadRequest,
    observation::Target,
    operation_store::OperationStore,
    preflight::{self, Report},
    receipt_broker::{ReceiptBroker, ScopeKind},
    record_impact::{ManualImpactAttempt, ManualRecordImpactPort, RecordImpactOwner},
    service_events::{TakeEventFeed, TakeEventFence},
    session::{ConnectionToken, ObsSession, RequestError, SessionHandle, SessionOptions},
    session_observation::ObservationBridge,
    stage::{Context as StageContext, StageMachine},
    stage_transport::{BoundCall, CallerBinding, Cancellation, StageTransport, TransportContext},
    take_monitor::Summary,
    take_transport::{BoundTakeIo, Bounds, HostServices, PreflightReader},
    takes::{
        self, ClipCheck, DispatchError, DispatchGuard, Effect, Event, IoFuture, Limits, Loan,
        MonitorSetup, Policy, StartOptions, TakeIo, TakeSupervisor,
    },
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
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const WATCH: Duration = Duration::from_secs(6);
const PATH: &str = "/fixture/same-path.mkv";
static NEXT: AtomicU64 = AtomicU64::new(0);
#[derive(Default)]
struct Host {
    joined: AtomicBool,
    preflight_gate: Mutex<Option<Arc<Gate>>>,
    inspection_gate: Mutex<Option<Arc<Gate>>>,
    inspections: AtomicU64,
    logs: AtomicU64,
}
#[derive(Default)]
struct Gate {
    entered: AtomicBool,
    released: AtomicBool,
    changed: Notify,
}
impl Gate {
    async fn wait(&self) {
        self.entered.store(true, Ordering::Release);
        self.changed.notify_waiters();
        loop {
            let changed = self.changed.notified();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
    async fn reached(&self) {
        loop {
            let changed = self.changed.notified();
            if self.entered.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}
struct DispatchHooks {
    port: ManualRecordImpactPort,
    pending: Mutex<Option<ManualImpactAttempt>>,
    intervene: AtomicBool,
    corrupt: AtomicBool,
    corrupted: Mutex<Option<PathBuf>>,
    directory: PathBuf,
    setup_gate: Mutex<Option<Arc<Gate>>>,
    status_gate: Mutex<Option<Arc<Gate>>>,
}
struct TestIo {
    bound: BoundTakeIo,
    hooks: Arc<DispatchHooks>,
}
impl TakeIo for TestIo {
    fn preflight(&self, o: preflight::Options) -> IoFuture<Report> {
        self.bound.preflight(o)
    }
    fn read(&self, r: ReadRequest, d: Duration) -> IoFuture<Result<Value, String>> {
        let gate = if matches!(r, ReadRequest::RecordStatus) {
            self.hooks.status_gate.lock().unwrap().clone()
        } else {
            None
        };
        let future = self.bound.read(r, d);
        Box::pin(async move {
            let value = future.await;
            if let Some(gate) = gate {
                gate.wait().await;
            }
            value
        })
    }
    fn stop_status(&self, d: Duration) -> IoFuture<Result<Value, String>> {
        self.bound.stop_status(d)
    }
    fn dispatch(
        &self,
        token: ConnectionToken,
        effect: Effect,
        d: Duration,
        guard: DispatchGuard,
    ) -> IoFuture<Result<Value, DispatchError>> {
        let hooks = self.hooks.clone();
        let intervene =
            matches!(effect, Effect::StopRecord) && hooks.intervene.swap(false, Ordering::AcqRel);
        let guard: DispatchGuard = if intervene {
            Box::new(move || {
                let attempt = hooks
                    .port
                    .begin_at_send(token)
                    .map_err(|_| "fixture impact admission refused".to_owned())?;
                *hooks.pending.lock().unwrap() = Some(attempt);
                if hooks.corrupt.load(Ordering::Acquire) {
                    for item in
                        std::fs::read_dir(hooks.directory.join("operation-receipts")).unwrap()
                    {
                        let path = item.unwrap().path();
                        if path.extension().is_some_and(|s| s == "json") {
                            let value: Value =
                                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                            if value["kind"] == "record-stop" {
                                std::fs::write(&path, b"{}").unwrap();
                                *hooks.corrupted.lock().unwrap() = Some(path);
                            }
                        }
                    }
                }
                guard()
            })
        } else {
            guard
        };
        self.bound.dispatch(token, effect, d, guard)
    }
    fn monitor_setup(&self) -> IoFuture<Result<MonitorSetup, String>> {
        let gate = self.hooks.setup_gate.lock().unwrap().clone();
        let future = self.bound.monitor_setup();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.wait().await;
            }
            future.await
        })
    }
    fn picture(&self, p: String) -> IoFuture<Result<Vec<u8>, String>> {
        self.bound.picture(p)
    }
    fn inspect(&self, p: String, l: bool) -> IoFuture<ClipCheck> {
        self.bound.inspect(p, l)
    }
    fn write_log(&self, p: String, s: Summary) -> IoFuture<Option<String>> {
        self.bound.write_log(p, s)
    }
    fn join_observers(&self) -> IoFuture<Result<(), String>> {
        self.bound.join_observers()
    }
}
impl HostServices for Host {
    fn preflight(&self, _: PreflightReader, _: preflight::Options) -> IoFuture<Report> {
        let gate = self.preflight_gate.lock().unwrap().clone();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.wait().await;
            }
            Report {
                ready: true,
                checks: vec![preflight::Check {
                    id: "record-directory".into(),
                    status: preflight::Status::Pass,
                    message: "explicit fixture host probe".into(),
                }],
            }
        })
    }
    fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
        self.inspections.fetch_add(1, Ordering::Relaxed);
        let gate = self.inspection_gate.lock().unwrap().clone();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.wait().await;
            }
            ClipCheck {
                skipped: Some("isolated fake media seam".into()),
                ..Default::default()
            }
        })
    }
    fn write_log(&self, _: String, _: Summary) -> IoFuture<Option<String>> {
        self.logs.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { None })
    }
    fn join_observers(&self) -> IoFuture<Result<(), String>> {
        self.joined.store(true, Ordering::Release);
        Box::pin(async { Ok(()) })
    }
}
#[derive(Default)]
struct State {
    requests: Vec<Value>,
    status_replies: Vec<Value>,
    stop_calls: usize,
    manual_ack: bool,
    replacement_installed: bool,
    automatic_stopped_replacement: bool,
    active: bool,
    event_states: Vec<String>,
    connections: usize,
    peer_joined: bool,
}
#[derive(Default)]
struct Hooks {
    state: Mutex<State>,
    changed: Notify,
    publish_stops: AtomicBool,
    hold_start_ack: AtomicBool,
}
impl Hooks {
    async fn until(&self, condition: impl Fn(&State) -> bool) {
        let deadline = Instant::now() + WATCH;
        loop {
            let changed = self.changed.notified();
            if condition(&self.state.lock().unwrap()) {
                return;
            }
            timeout(deadline.saturating_duration_since(Instant::now()), changed)
                .await
                .expect("actual schedule watchdog");
        }
    }
}
enum Command {
    Replace(bool, oneshot::Sender<()>),
    Release(oneshot::Sender<()>),
    Inactive(oneshot::Sender<()>),
    ReleaseStart(oneshot::Sender<()>),
}
async fn event(
    peer: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    hooks: &Hooks,
    state: &str,
) {
    peer.send(Message::Text(json!({"op":5,"d":{"eventType":"RecordStateChanged","eventIntent":64,"eventData":{"outputState":state,"outputActive":state=="OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":PATH}}}).to_string().into())).await.unwrap();
    hooks.state.lock().unwrap().event_states.push(state.into());
    hooks.changed.notify_one();
}
struct Fixture {
    physical: ObsSession,
    handle: SessionHandle,
    bridge: ObservationBridge,
    feed: TakeEventFeed,
    fence: TakeEventFence,
    source: mpsc::Receiver<Event>,
    destination: mpsc::Sender<Event>,
    take: TakeSupervisor,
    commands: mpsc::Sender<Command>,
    peer: JoinHandle<()>,
    hooks: Arc<Hooks>,
    host: Arc<Host>,
    directory: PathBuf,
    delivered: u64,
    _impact_owner: RecordImpactOwner,
    impact_port: ManualRecordImpactPort,
    dispatch_hooks: Arc<DispatchHooks>,
    io: Arc<TestIo>,
    broker: Option<ReceiptBroker>,
    control: obs_mcp_native::custody::SharedControl,
    clock: Clock,
}
// One linear, bounded test delivery gate. It moves original ObservedFrames,
// including their original permits. It is neither another physical receiver nor
// a duplicate observer/model, and spawns no relay task.
async fn pump<T>(
    future: impl Future<Output = T>,
    source: &mut mpsc::Receiver<Event>,
    destination: &mpsc::Sender<Event>,
    delivered: &mut u64,
) -> T {
    tokio::pin!(future);
    let mut open = true;
    loop {
        tokio::select! {biased;result=&mut future=>return result,event=source.recv(),if open=>match event{Some(event)=>{*delivered+=1;destination.send(event).await.unwrap();},None=>open=false,}}
    }
}
impl Fixture {
    async fn new(label: &str) -> Self {
        Self::configured(label, false).await
    }
    async fn configured(label: &str, standalone: bool) -> Self {
        Self::composed(label, standalone, false).await
    }
    async fn composed(label: &str, standalone: bool, scoped: bool) -> Self {
        let directory = evidence_dir().join(format!(
            "state-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let hooks = Arc::new(Hooks::default());
        let observed = hooks.clone();
        let (commands, mut receive) = mpsc::channel::<Command>(8);
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            observed.state.lock().unwrap().connections += 1;
            let mut peer = accept_async(stream).await.unwrap();
            peer.send(Message::Text(
                json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}})
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
            let mut withheld = Vec::<&'static str>::new();
            let mut held_start: Option<Value> = None;
            let mut preview = "Main";
            loop {
                tokio::select! {
                    command=receive.recv()=>match command{
                        Some(Command::Replace(publish,done))=>{
                            {let mut state=observed.state.lock().unwrap();state.active=true;state.replacement_installed=true;}
                            if publish{for state in withheld.drain(..){event(&mut peer,&observed,state).await;}event(&mut peer,&observed,"OBS_WEBSOCKET_OUTPUT_STARTED").await;}
                            else{withheld.push("OBS_WEBSOCKET_OUTPUT_STARTED");}
                            let _=done.send(());observed.changed.notify_one();
                        },
                        Some(Command::Release(done))=>{for state in withheld.drain(..){event(&mut peer,&observed,state).await;}let _=done.send(());},
                        Some(Command::Inactive(done))=>{observed.state.lock().unwrap().active=false;let _=done.send(());},
                        Some(Command::ReleaseStart(done))=>{if let Some(reply)=held_start.take(){peer.send(Message::Text(reply.to_string().into())).await.unwrap();}let _=done.send(());},
                        None=>break,
                    },
                    packet=peer.next()=>match packet{
                        Some(Ok(Message::Text(text)))=>{
                            let request:Value=serde_json::from_str(&text).unwrap();assert_eq!(request["op"],6,"unexpected batch or second transport");
                            observed.state.lock().unwrap().requests.push(request.clone());
                            let kind=request["d"]["requestType"].as_str().unwrap();
                            let mut status=None;
                            let data=match kind{
                                "GetVersion"=>json!({"availableRequests":["GetVersion","GetRecordStatus","GetStats","GetCurrentProgramScene","GetInputList","GetProfileParameter","GetInputAudioTracks","StartRecord","StopRecord","CreateRecordChapter","GetSceneCollectionList","GetSceneList","GetStudioModeEnabled","GetCurrentPreviewScene","GetVideoSettings","GetSceneItemList","GetSourceFilterList","GetSourceScreenshot","SetCurrentPreviewScene"],"supportedImageFormats":[],"platform":"macos"}),
                                "GetInputList"=>json!({"inputs":[]}),
                                "GetCurrentProgramScene"=>json!({"currentProgramSceneName":"Main","currentProgramSceneUuid":"main"}),
                                "GetStudioModeEnabled"=>json!({"studioModeEnabled":true}),
                                "GetSceneCollectionList"=>json!({"currentSceneCollectionName":"Fixture"}),
                                "GetSceneList"=>json!({"currentProgramSceneName":"Main","currentProgramSceneUuid":"main","scenes":[{"sceneName":"Main","sceneUuid":"main"},{"sceneName":"Review","sceneUuid":"review"}]}),
                                "GetCurrentPreviewScene"=>json!({"currentPreviewSceneName":preview,"currentPreviewSceneUuid":if preview=="Main"{"main"}else{"review"}}),
                                "GetVideoSettings"=>json!({"baseWidth":1920,"baseHeight":1080}),
                                "GetSceneItemList"=>json!({"sceneItems":[]}),
                                "GetSourceFilterList"=>json!({"filters":[]}),
                                "GetSourceScreenshot"=>json!({"imageData":"data:image/png;base64,AAAA"}),
                                "SetCurrentPreviewScene"=>{preview="Review";json!({})},
                                "GetStats"=>json!({"availableDiskSpace":50000,"renderSkippedFrames":0,"renderTotalFrames":0}),
                                "GetProfileParameter"=>{let category=request["d"]["requestData"]["parameterCategory"].as_str().unwrap_or("");let name=request["d"]["requestData"]["parameterName"].as_str().unwrap_or("");json!({"parameterValue":match(category,name){("Output","Mode")=>"Simple",(_,"RecTracks")=>"1",_=>"1"}})},
                                "StartRecord"=>{observed.state.lock().unwrap().active=true;event(&mut peer,&observed,"OBS_WEBSOCKET_OUTPUT_STARTED").await;json!({})},
                                "StopRecord"=>{
                                    let mut state=observed.state.lock().unwrap();state.stop_calls+=1;
                                    if state.stop_calls==1{state.manual_ack=true;}
                                    else{state.automatic_stopped_replacement|=state.active&&state.replacement_installed;}
                                    state.active=false;drop(state);
                                    // State notifications are explicitly withheld until the
                                    // test releases their historical order, including auto-stop.
                                    withheld.push("OBS_WEBSOCKET_OUTPUT_STOPPED");
                                    json!({"outputPath":PATH})
                                },
                                "GetRecordStatus"=>{let state=observed.state.lock().unwrap();let data=json!({"outputActive":state.active,"outputPaused":false,"outputBytes":1048576});status=Some(json!({"active":state.active,"stopCallsBeforeReply":state.stop_calls,"replacementInstalled":state.replacement_installed}));data},
                                "CreateRecordChapter"=>json!({}),other=>panic!("unexpected fixture RPC {other}"),
                            };
                            if kind=="StopRecord"&&observed.publish_stops.load(Ordering::Acquire){withheld.pop();event(&mut peer,&observed,"OBS_WEBSOCKET_OUTPUT_STOPPED").await;}
                            if kind=="SetCurrentPreviewScene"{peer.send(Message::Text(json!({"op":5,"d":{"eventType":"CurrentPreviewSceneChanged","eventIntent":4,"eventData":{"sceneName":preview}}}).to_string().into())).await.unwrap();}
                            let reply=json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":kind,"requestStatus":{"result":true,"code":100},"responseData":data}});
                            if kind=="StartRecord"&&observed.hold_start_ack.load(Ordering::Acquire) {held_start=Some(reply);} else {peer.send(Message::Text(reply.to_string().into())).await.unwrap();}
                            if let Some(status)=status{observed.state.lock().unwrap().status_replies.push(status);}
                            observed.changed.notify_one();
                        },
                        Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                        _=>{},
                    }
                }
            }
            observed.state.lock().unwrap().peer_joined = true;
            observed.changed.notify_one();
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
        let source = feed.take_receiver().unwrap();
        let (destination, loan_events) = mpsc::channel(256);
        let host = Arc::new(Host::default());
        let io = BoundTakeIo::bind(
            handle.clone(),
            Arc::new(fence.clone()),
            host.clone(),
            &url,
            Arc::new(AtomicBool::new(false)),
            Bounds {
                rpc: WATCH,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let clock: Clock = Arc::new(|| {
            clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap()
        });
        let mut ids = 0;
        let impact_owner = RecordImpactOwner::bind(&handle).unwrap();
        let impact_port = impact_owner.manual_port();
        let dispatch_hooks = Arc::new(DispatchHooks {
            port: impact_port.clone(),
            pending: Mutex::new(None),
            intervene: AtomicBool::new(false),
            corrupt: AtomicBool::new(false),
            corrupted: Mutex::new(None),
            directory: directory.clone(),
            setup_gate: Mutex::new(None),
            status_gate: Mutex::new(None),
        });
        let io = Arc::new(TestIo {
            bound: io,
            hooks: dispatch_hooks.clone(),
        });
        let control = Arc::new(Mutex::new(ControlDomain::default()));
        let direct = OperationStore::new(&directory, &url).unwrap();
        let (broker, store) = if scoped {
            let broker = ReceiptBroker::new(direct).unwrap();
            let store = broker.scope(ScopeKind::Take).unwrap();
            (Some(broker), store)
        } else {
            (None, direct)
        };
        let loan = Loan {
            session: handle.clone(),
            store,
            control: control.clone(),
            clock: clock.clone(),

            identities: Box::new(move || {
                ids += 1;
                format!("00000000-0000-4000-8000-{ids:012x}")
            }),
            io: io.clone(),
            events: loan_events,
        };
        let limits = Limits {
            rpc: WATCH,
            setup: WATCH,
            stop: Duration::from_millis(200),
            inspection: WATCH,
            file: WATCH,
            start_confirmation: WATCH,
            stats_interval: Duration::from_secs(60),
            picture_interval: Duration::from_secs(60),
        };
        let take = if standalone {
            TakeSupervisor::spawn(loan, limits)
        } else {
            TakeSupervisor::spawn_with_record_impact(loan, limits, impact_owner.view())
        }
        .unwrap_or_else(|failure| panic!("real take spawn failed {:?}", failure.reason));
        Self {
            physical,
            handle,
            bridge,
            feed,
            fence,
            source,
            destination,
            take,
            commands,
            peer,
            hooks,
            host,
            directory,
            delivered: 0,
            _impact_owner: impact_owner,
            impact_port,
            dispatch_hooks,
            io,
            broker,
            control,
            clock,
        }
    }
    async fn start(&mut self) -> Value {
        let handle = self.take.handle();
        let future = handle.start(
            StartOptions {
                expect_silent: true,
                max_duration_seconds: Some(1),
                ..Default::default()
            },
            Policy {
                caller: "fixture".into(),
                ..Default::default()
            },
        );
        let result = timeout(
            WATCH,
            pump(
                future,
                &mut self.source,
                &self.destination,
                &mut self.delivered,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(result["isError"], true, "{result}");
        assert_eq!(
            result["structuredContent"]["ownership"], "continuous",
            "{result}"
        );
        self.fence
            .synchronize(Instant::now() + WATCH)
            .await
            .unwrap();
        assert!(self.fence.ready());
        result
    }
    async fn drive<T>(&mut self, future: impl Future<Output = T>) -> T {
        timeout(
            WATCH,
            pump(
                future,
                &mut self.source,
                &self.destination,
                &mut self.delivered,
            ),
        )
        .await
        .unwrap()
    }
    async fn pause(&mut self, duration: Duration) {
        self.drive(tokio::time::sleep(duration)).await;
    }
    fn stop_receipts(&self) -> Vec<Value> {
        let root = self.directory.join("operation-receipts");
        if !root.exists() {
            return vec![];
        }
        std::fs::read_dir(root)
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                if path.extension().is_some_and(|s| s == "json") {
                    let value: Value =
                        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
                    (value["kind"] == "record-stop").then_some(value)
                } else {
                    None
                }
            })
            .collect()
    }
    async fn pending_refused_manual(&self) -> ManualImpactAttempt {
        let port = self.impact_port.clone();
        let token = self.handle.token();
        let admitted = Arc::new(Mutex::new(None));
        let capture = admitted.clone();
        let result = self
            .handle
            .request_checked(token, "StopRecord", Value::Null, WATCH, move || {
                *capture.lock().unwrap() = Some(
                    port.begin_at_send(token)
                        .map_err(|_| "fixture impact capacity".to_owned())?,
                );
                Err("controlled final-guard refusal after impact admission".into())
            })
            .await;
        assert!(
            matches!(result, Err(RequestError::BeforeDispatch(_))),
            "must be actual typed unsent fate: {result:?}"
        );
        admitted.lock().unwrap().take().unwrap()
    }
    async fn manual_stop(&self) -> Value {
        self.handle
            .request_checked(
                self.handle.token(),
                "StopRecord",
                Value::Null,
                WATCH,
                || Ok(()),
            )
            .await
            .unwrap()
    }
    async fn tracked_manual_stop(&self) -> Value {
        let port = self.impact_port.clone();
        let token = self.handle.token();
        let pending = Arc::new(Mutex::new(None));
        let admitted = pending.clone();
        let result = self
            .handle
            .request_checked(
                self.handle.token(),
                "StopRecord",
                Value::Null,
                WATCH,
                move || {
                    let attempt = port
                        .begin_at_send(token)
                        .map_err(|_| "Impact admission unavailable".to_owned())?;
                    *admitted.lock().unwrap() = Some(attempt);
                    Ok(())
                },
            )
            .await
            .unwrap();
        pending.lock().unwrap().take().unwrap().accepted().unwrap();
        result
    }
    async fn replace(&self, publish: bool) {
        let (done, wait) = oneshot::channel();
        self.commands
            .send(Command::Replace(publish, done))
            .await
            .unwrap();
        wait.await.unwrap();
    }
    async fn published(&self) -> u64 {
        // The follow-up same-socket RPC proves all prior physical frames were
        // read. Bridge synchronization proves its own publication prefix.
        self.handle
            .request(self.handle.token(), "GetVersion", Value::Null, WATCH)
            .await
            .unwrap();
        self.bridge
            .synchronize(Instant::now() + WATCH)
            .await
            .unwrap();
        self.handle.event_cursor()
    }
    async fn fresh_automatic_status(&mut self) {
        let hooks = self.hooks.clone();
        timeout(
            WATCH,
            pump(
                hooks.until(|state| !state.status_replies.is_empty()),
                &mut self.source,
                &self.destination,
                &mut self.delivered,
            ),
        )
        .await
        .unwrap();
    }
    fn snapshot(&self, published_through: u64) -> Value {
        let state = self.hooks.state.lock().unwrap();
        json!({"intrinsic":self.handle.event_cursor(),"bridgePublishedThrough":published_through,"linearDeliveredThrough":self.delivered,"takeReadyAtIntrinsic":self.fence.ready(),"canonicalRevision":self.bridge.evidence().borrow().revision,"stopCalls":state.stop_calls,"manualAck":state.manual_ack,"replacementInstalled":state.replacement_installed,"automaticStoppedReplacement":state.automatic_stopped_replacement,"active":state.active,"freshStatusReplies":state.status_replies,"eventStatesSent":state.event_states,"take":self.take.handle().snapshot()})
    }
    async fn cleanup(self) -> Value {
        let (done, wait) = oneshot::channel();
        self.commands.send(Command::Release(done)).await.unwrap();
        wait.await.unwrap();
        // Fake operator cleanup is explicit and separate from the observed
        // automatic effect. It neither writes a receipt nor replays native Stop.
        let (done, wait) = oneshot::channel();
        self.commands.send(Command::Inactive(done)).await.unwrap();
        wait.await.unwrap();
        self.join().await
    }
    async fn join(mut self) -> Value {
        let receipt = timeout(
            WATCH,
            pump(
                self.take.shutdown(),
                &mut self.source,
                &self.destination,
                &mut self.delivered,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let returned = self.take.take_store().is_some();
        drop(self.feed);
        drop(self.destination);
        self.physical.shutdown().await.unwrap();
        self.bridge.shutdown().await.unwrap();
        drop(self.commands);
        self.peer.await.unwrap();
        let broker_returned = self.broker.take().map(|broker| broker.into_store().is_ok());
        let state = self.hooks.state.lock().unwrap();
        json!({"brokerWriterReturned":broker_returned,"takeJoined":receipt.joined,"observersJoined":receipt.observers_joined,"stopConfirmedAtCleanup":receipt.stop_confirmed,"writerReturned":returned,"hostJoinCalled":self.host.joined.load(Ordering::Acquire),"connections":state.connections,"peerJoined":state.peer_joined,"finalStopCalls":state.stop_calls,"receiptsRetainedAt":self.directory})
    }
}
fn evidence_dir() -> PathBuf {
    let base = std::env::var_os("OBS_MCP_RECORD_IMPACT_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"))
                .join("record-impact-take-evidence")
        });
    std::fs::create_dir_all(&base).unwrap();
    std::fs::canonicalize(base).unwrap()
}
fn save(name: &str, value: &Value) {
    std::fs::write(
        evidence_dir().join(format!("{name}.json")),
        serde_json::to_string_pretty(value).unwrap() + "\n",
    )
    .unwrap();
}
#[tokio::test]
async fn manual_ack_with_unreceived_notifications_must_not_stop_same_path_replacement() {
    let mut f = Fixture::new("withheld").await;
    let start = f.start().await;
    let initial = f.handle.event_cursor();
    let ack = f.tracked_manual_stop().await;
    assert_eq!(ack["outputPath"], PATH);
    f.replace(false).await;
    let published = f.published().await;
    assert_eq!(
        published, initial,
        "withheld scenario unexpectedly received transitions"
    );
    assert!(f.fence.ready());
    // The veto may suppress the timer before any automatic status read.
    // Establish the same actual active replacement independently, then cross
    // the original one-second timer boundary while pumping the real owner.
    f.handle
        .request(f.handle.token(), "GetRecordStatus", Value::Null, WATCH)
        .await
        .unwrap();
    let fresh = f.hooks.state.lock().unwrap().status_replies[0].clone();
    assert_eq!(fresh["active"], true);
    assert_eq!(fresh["stopCallsBeforeReply"], 1);
    assert_eq!(fresh["replacementInstalled"], true);
    // Observe the actual automatic timer's next frame, with no explicit
    // take-stop call. Timeout is only the zero-effect observation window.
    let hooks = f.hooks.clone();
    let _ = timeout(
        Duration::from_millis(1200),
        pump(
            hooks.until(|state| state.stop_calls > 1),
            &mut f.source,
            &f.destination,
            &mut f.delivered,
        ),
    )
    .await;
    let snapshot = f.snapshot(published);
    let automatic_count = snapshot["stopCalls"].as_u64().unwrap() - 1;
    let schedule_reached =
        fresh["active"] == true && published == initial && snapshot["replacementInstalled"] == true;
    assert_eq!(snapshot["take"]["ownership"], "unavailable");
    let cleanup = f.cleanup().await;
    save(
        "withheld-notifications",
        &json!({"scheduleReached":schedule_reached,"samePath":PATH,"start":start,"manualAck":ack,"beforeCleanup":snapshot,"automaticStopFrames":automatic_count,"cleanup":cleanup}),
    );
    assert!(
        schedule_reached,
        "counter did not reach actual manual-ACK/replacement/fresh-status schedule"
    );
    assert_eq!(cleanup["connections"], 1);
    assert_eq!(cleanup["takeJoined"], true);
    assert_eq!(cleanup["peerJoined"], true);
    assert_eq!(cleanup["writerReturned"], true);
    assert_eq!(
        automatic_count, 0,
        "owned automatic Stop targeted replacement after manual ACK but before notifications arrived"
    );
}
#[tokio::test]
async fn received_but_unapplied_replacement_is_already_fenced() {
    let mut f = Fixture::new("unapplied").await;
    f.start().await;
    let initial = f.handle.event_cursor();
    f.manual_stop().await;
    f.replace(true).await;
    let published = f.published().await;
    assert_eq!(published, initial + 2);
    let first = timeout(WATCH, f.source.recv()).await.unwrap().unwrap();
    let second = timeout(WATCH, f.source.recv()).await.unwrap().unwrap();
    assert!(!f.fence.ready());
    assert_eq!(f.delivered, initial);
    // Do not pump these original frames: the real BoundTakeIo status adapter
    // must wait for actual Take application after its true status reply.
    let hooks = f.hooks.clone();
    timeout(WATCH, hooks.until(|state| !state.status_replies.is_empty()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let held = f.snapshot(published);
    assert_eq!(held["freshStatusReplies"][0]["active"], true);
    assert_eq!(held["stopCalls"], 1);
    f.destination.send(first).await.unwrap();
    f.delivered += 1;
    f.destination.send(second).await.unwrap();
    f.delivered += 1;
    let fence = f.fence.clone();
    timeout(
        WATCH,
        pump(
            fence.synchronize(Instant::now() + WATCH),
            &mut f.source,
            &f.destination,
            &mut f.delivered,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let handle = f.take.handle();
    let result = timeout(
        WATCH,
        pump(
            handle.stop(
                None,
                Policy {
                    caller: "fixture".into(),
                    ..Default::default()
                },
            ),
            &mut f.source,
            &f.destination,
            &mut f.delivered,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let settled = f.snapshot(published);
    let cleanup = f.cleanup().await;
    save(
        "received-unapplied",
        &json!({"scheduleReached":true,"initialAppliedConfirmedThrough":initial,"held":held,"settled":settled,"ownedResult":result,"cleanup":cleanup}),
    );
    assert_eq!(settled["stopCalls"], 1);
    assert_eq!(result["structuredContent"]["stopConfirmed"], false);
    assert_eq!(result["structuredContent"]["ownership"], "unavailable");
    assert_eq!(cleanup["connections"], 1);
    assert_eq!(cleanup["takeJoined"], true);
}
#[tokio::test]
async fn inactive_without_stopped_event_refuses_without_another_stop() {
    let mut f = Fixture::new("inactive").await;
    f.start().await;
    let initial = f.handle.event_cursor();
    f.manual_stop().await;
    let published = f.published().await;
    assert_eq!(published, initial);
    f.fresh_automatic_status().await;
    assert_eq!(
        f.hooks.state.lock().unwrap().status_replies[0]["active"],
        false
    );
    let handle = f.take.handle();
    let result = timeout(
        WATCH,
        pump(
            handle.stop(
                None,
                Policy {
                    caller: "fixture".into(),
                    ..Default::default()
                },
            ),
            &mut f.source,
            &f.destination,
            &mut f.delivered,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let snapshot = f.snapshot(published);
    let cleanup = f.cleanup().await;
    save(
        "inactive-without-stopped",
        &json!({"scheduleReached":true,"initialAppliedConfirmedThrough":initial,"beforeCleanup":snapshot,"ownedResult":result,"cleanup":cleanup}),
    );
    assert_eq!(snapshot["stopCalls"], 1);
    assert_eq!(result["structuredContent"]["stopConfirmed"], false);
    assert!(
        result["structuredContent"]["reason"]
            .as_str()
            .unwrap()
            .contains("Inactive status has no matching stopped event")
    );
    assert_eq!(cleanup["takeJoined"], true);
    assert_eq!(cleanup["connections"], 1);
}

#[tokio::test]
async fn preintent_expiry_keeps_no_stop_intent_and_exact_unsent_resumes_the_original_due_timer_once()
 {
    let mut f = Fixture::new("preintent").await;
    f.start().await;
    f.hooks.publish_stops.store(true, Ordering::Release);
    let attempt = f.pending_refused_manual().await;
    f.pause(Duration::from_millis(1350)).await;
    let blocked = f.take.handle().snapshot();
    let before = f.stop_receipts();
    let before_frames = f.hooks.state.lock().unwrap().stop_calls;
    let resumed = Instant::now();
    attempt.not_dispatched().unwrap();
    let hooks = f.hooks.clone();
    f.drive(hooks.until(|state| state.stop_calls == 1)).await;
    let elapsed = resumed.elapsed();
    let handle = f.take.handle();
    let result = f.drive(handle.stop(None, Policy::default())).await.unwrap();
    let after = f.stop_receipts();
    let cleanup = f.join().await;
    assert_eq!(blocked["ownership"], "suspended");
    assert_eq!(before_frames, 0);
    assert!(before.is_empty());
    assert!(
        elapsed < Duration::from_millis(700),
        "original timer was restarted: {elapsed:?}"
    );
    assert_eq!(result["structuredContent"]["stopConfirmed"], true);
    assert_eq!(after.len(), 1);
    assert_eq!(after[0]["phase"], "observed");
    assert_eq!(cleanup["finalStopCalls"], 1);
    assert_eq!(cleanup["takeJoined"], true);
}
#[tokio::test]
async fn coalesced_unsent_a_then_pending_b_never_rearms_through_b() {
    let mut f = Fixture::new("coalesced").await;
    f.start().await;
    f.hooks.publish_stops.store(true, Ordering::Release);
    let first = f.pending_refused_manual().await;
    f.pause(Duration::from_millis(1300)).await;
    let port = f.impact_port.clone();
    let token = f.handle.token();
    let pending = Arc::new(Mutex::new(None));
    let captured = pending.clone();
    let result = f
        .handle
        .request_checked(token, "StopRecord", Value::Null, WATCH, move || {
            first
                .not_dispatched()
                .map_err(|_| "first unsent settlement".to_owned())?;
            *captured.lock().unwrap() = Some(
                port.begin_at_send(token)
                    .map_err(|_| "second impact admission".to_owned())?,
            );
            Err("second controlled actual BeforeDispatch".into())
        })
        .await;
    assert!(matches!(result, Err(RequestError::BeforeDispatch(_))));
    f.pause(Duration::from_millis(250)).await;
    let before = f.hooks.state.lock().unwrap().stop_calls;
    pending
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .not_dispatched()
        .unwrap();
    let hooks = f.hooks.clone();
    f.drive(hooks.until(|state| state.stop_calls == 1)).await;
    let handle = f.take.handle();
    let result = f.drive(handle.stop(None, Policy::default())).await.unwrap();
    let cleanup = f.join().await;
    assert_eq!(before, 0);
    assert_eq!(result["structuredContent"]["stopConfirmed"], true);
    assert_eq!(cleanup["finalStopCalls"], 1);
}
#[tokio::test]
async fn final_guard_unsent_retires_full_tuple_and_continuation_uses_a_fresh_receipt() {
    let mut f = Fixture::new("final-guard").await;
    f.start().await;
    f.hooks.publish_stops.store(true, Ordering::Release);
    f.dispatch_hooks.intervene.store(true, Ordering::Release);
    let handle = f.take.handle();
    let result = f.drive(handle.stop(None, Policy::default())).await.unwrap();
    let old = f.stop_receipts();
    let before = f.hooks.state.lock().unwrap().stop_calls;
    let pending = f.dispatch_hooks.pending.lock().unwrap().take().unwrap();
    pending.not_dispatched().unwrap();
    f.pause(Duration::from_millis(1150)).await;
    let handle = f.take.handle();
    let completed = f.drive(handle.stop(None, Policy::default())).await.unwrap();
    let all = f.stop_receipts();
    let cleanup = f.join().await;
    assert_eq!(result["structuredContent"]["stopConfirmed"], false);
    assert_eq!(before, 0);
    assert_eq!(old.len(), 1);
    assert_eq!(old[0]["phase"], "not-dispatched");
    assert_eq!(old[0]["requestDispatched"], false);
    assert!(old[0]["requestAccepted"].is_null());
    assert_eq!(old[0]["postconditionObserved"], false);
    assert_eq!(completed["structuredContent"]["stopConfirmed"], true);
    assert_eq!(all.len(), 2);
    assert_ne!(all[0]["operationId"], all[1]["operationId"]);
    assert_eq!(
        all.iter()
            .filter(|v| v["phase"] == "not-dispatched")
            .count(),
        1
    );
    assert_eq!(cleanup["finalStopCalls"], 1);
}
#[tokio::test]
async fn failed_unsent_publication_never_grants_a_new_stop_permission() {
    let mut f = Fixture::new("publication-fault").await;
    f.start().await;
    f.dispatch_hooks.intervene.store(true, Ordering::Release);
    f.dispatch_hooks.corrupt.store(true, Ordering::Release);
    let handle = f.take.handle();
    let result = f.drive(handle.stop(None, Policy::default())).await.unwrap();
    let corrupted = f.dispatch_hooks.corrupted.lock().unwrap().clone().unwrap();
    f.dispatch_hooks
        .pending
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .not_dispatched()
        .unwrap();
    f.pause(Duration::from_millis(1250)).await;
    let handle = f.take.handle();
    let later = f.drive(handle.stop(None, Policy::default())).await.unwrap();
    let before = f.hooks.state.lock().unwrap().stop_calls;
    let cleanup = f.cleanup().await;
    assert_eq!(result["structuredContent"]["stopConfirmed"], false);
    assert_eq!(later["structuredContent"]["stopConfirmed"], false);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("could not be safely retired")
    );
    assert_eq!(std::fs::read(corrupted).unwrap(), b"{}");
    assert_eq!(before, 0);
    assert_eq!(cleanup["finalStopCalls"], 0);
}
#[tokio::test]
async fn ticket_revocation_before_inactive_stopped_completion_never_confirms_the_manual_file() {
    let mut f = Fixture::new("inactive-impact").await;
    f.start().await;
    f.tracked_manual_stop().await;
    let (done, wait) = oneshot::channel();
    f.commands.send(Command::Release(done)).await.unwrap();
    wait.await.unwrap();
    let fence = f.fence.clone();
    f.drive(fence.synchronize(Instant::now() + WATCH))
        .await
        .unwrap();
    let handle = f.take.handle();
    let result = f.drive(handle.stop(None, Policy::default())).await.unwrap();
    let receipts = f.stop_receipts();
    let inspections = f.host.inspections.load(Ordering::Acquire);
    let cleanup = f.join().await;
    assert_eq!(result["structuredContent"]["stopConfirmed"], false);
    assert_eq!(result["structuredContent"]["ownership"], "unavailable");
    assert!(receipts.is_empty());
    assert_eq!(inspections, 0);
    assert_eq!(cleanup["finalStopCalls"], 1);
}
#[tokio::test]
async fn impact_during_admitted_inspection_preserves_observed_stop_but_refuses_public_file_success()
{
    let mut f = Fixture::new("inspection-impact").await;
    f.start().await;
    f.hooks.publish_stops.store(true, Ordering::Release);
    let gate = Arc::new(Gate::default());
    *f.host.inspection_gate.lock().unwrap() = Some(gate.clone());
    let handle = f.take.handle();
    let future = handle.stop(None, Policy::default());
    tokio::pin!(future);
    f.drive(async {tokio::select!{biased;result=&mut future=>panic!("inspection not held: {result:?}"),_=gate.reached()=>{}}}).await;
    let before = f.stop_receipts();
    f.tracked_manual_stop().await;
    gate.release();
    let result = f.drive(future).await.unwrap();
    let after = f.stop_receipts();
    let log_attempts = f.host.logs.load(Ordering::Acquire);
    let cleanup = f.join().await;
    assert_eq!(
        log_attempts, 0,
        "revoked inspection must not admit a new log publication"
    );
    assert_eq!(before.len(), 1);
    assert_eq!(before[0]["phase"], "observed");
    assert_eq!(result["structuredContent"]["stopConfirmed"], false);
    assert!(result["structuredContent"].get("outputPath").is_none());
    assert_eq!(after[0]["requestAccepted"], true);
    assert_eq!(after[0]["phase"], "observed");
    assert_eq!(cleanup["finalStopCalls"], 2);
}
#[tokio::test]
async fn the_start_ticket_survives_preflight_before_any_start_intent() {
    let mut f = Fixture::new("preflight-impact").await;
    let gate = Arc::new(Gate::default());
    *f.host.preflight_gate.lock().unwrap() = Some(gate.clone());
    let handle = f.take.handle();
    let future = handle.start(StartOptions::default(), Policy::default());
    tokio::pin!(future);
    f.drive(async{tokio::select!{biased;result=&mut future=>panic!("preflight not held {result:?}"),_=gate.reached()=>{}}}).await;
    f.tracked_manual_stop().await;
    gate.release();
    let result = f.drive(future).await.unwrap();
    let starts = f
        .hooks
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|r| r["d"]["requestType"] == "StartRecord")
        .count();
    let cleanup = f.join().await;
    assert_eq!(result["isError"], true);
    assert_eq!(starts, 0);
    assert_eq!(cleanup["finalStopCalls"], 1);
}
#[tokio::test]
async fn the_same_start_ticket_survives_held_monitor_setup_and_never_recaptures_ready_epoch() {
    let mut f = Fixture::new("setup-impact").await;
    let gate = Arc::new(Gate::default());
    *f.dispatch_hooks.setup_gate.lock().unwrap() = Some(gate.clone());
    let handle = f.take.handle();
    let future = handle.start(StartOptions::default(), Policy::default());
    tokio::pin!(future);
    f.drive(async{tokio::select!{biased;result=&mut future=>panic!("setup not held {result:?}"),_=gate.reached()=>{}}}).await;
    f.tracked_manual_stop().await;
    gate.release();
    let result = f.drive(future).await.unwrap();
    let snapshot = f.take.handle().snapshot();
    let cleanup = f.join().await;
    assert_eq!(result["isError"], true);
    assert!(
        snapshot.is_null(),
        "inactive lost Take must retire rather than capture a fresh ticket"
    );
    assert_eq!(cleanup["finalStopCalls"], 1);
}
#[tokio::test]
async fn standalone_owner_survives_cancellation_safe_normal_shutdown_until_one_confirmed_stop() {
    let mut f = Fixture::configured("standalone-shutdown", true).await;
    f.start().await;
    f.hooks.publish_stops.store(true, Ordering::Release);
    let gate = Arc::new(Gate::default());
    *f.host.inspection_gate.lock().unwrap() = Some(gate.clone());
    {
        let shutdown = f.take.shutdown();
        tokio::pin!(shutdown);
        timeout(WATCH,pump(async {tokio::select!{biased;result=&mut shutdown=>panic!("shutdown should be held in inspection: {result:?}"),_=gate.reached()=>{}}},&mut f.source,&f.destination,&mut f.delivered)).await.unwrap();
        // Cancel only the frontend await; the supervisor retains the same task
        // and its private impact owner, then resumes that JoinHandle below.
    }
    gate.release();
    let cleanup = f.join().await;
    assert_eq!(cleanup["stopConfirmedAtCleanup"], true);
    assert_eq!(cleanup["finalStopCalls"], 1);
    assert_eq!(cleanup["writerReturned"], true);
}

#[tokio::test]
async fn closing_pending_impact_is_bounded_and_does_not_detach_a_late_stop() {
    let mut f = Fixture::new("closing-pending").await;
    f.start().await;
    let attempt = f.pending_refused_manual().await;
    let receipt = f
        .drive({
            let handle = f.take.handle();
            async move { handle.stop(None, Policy::default()).await }
        })
        .await
        .unwrap();
    let cleanup = f.join().await;
    let late = attempt.not_dispatched();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        late,
        Err(obs_mcp_native::record_impact::ImpactError::OwnerGone)
    );
    assert_eq!(receipt["structuredContent"]["ownership"], "suspended");
    assert_eq!(cleanup["takeJoined"], true);
    assert_eq!(cleanup["stopConfirmedAtCleanup"], false);
    assert_eq!(cleanup["finalStopCalls"], 0);
}
#[test]
fn an_exact_unsent_rollback_restores_later_unexpected_stop_warning() {
    use obs_mcp_native::take_monitor::{MonitorOptions, TakeMonitorCore};
    let mut core = TakeMonitorCore::new(
        0,
        "1970-01-01T00:00:00.000Z".into(),
        MonitorOptions::default(),
    );
    let expected = core.begin_stop_expectation();
    core.rollback_stop_expectation(expected);
    core.on_event(
        1,
        "RecordStateChanged",
        &json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED","outputActive":false}),
    );
    assert!(
        core.summary(1)
            .warnings
            .iter()
            .any(|w| w.message == "The recording stopped before it was asked to")
    );
}
#[test]
fn an_own_stop_event_before_ack_stays_expected_and_prior_expectation_is_preserved() {
    use obs_mcp_native::take_monitor::{MonitorOptions, TakeMonitorCore};
    let mut core = TakeMonitorCore::new(
        0,
        "1970-01-01T00:00:00.000Z".into(),
        MonitorOptions::default(),
    );
    core.expect_stop();
    let prior = core.begin_stop_expectation();
    core.rollback_stop_expectation(prior);
    core.on_event(
        1,
        "RecordStateChanged",
        &json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED","outputActive":false}),
    );
    assert!(core.summary(1).warnings.is_empty());
}

#[tokio::test]
async fn exact_unsent_within_the_original_wait_window_resumes_one_waiting_stop() {
    let mut f = Fixture::new("same-window").await;
    f.start().await;
    f.hooks.publish_stops.store(true, Ordering::Release);
    let attempt = f.pending_refused_manual().await;
    let handle = f.take.handle();
    let future = handle.stop(None, Policy::default());
    tokio::pin!(future);
    f.drive(async{tokio::select!{biased;result=&mut future=>panic!("pending impact escaped early {result:?}"),_=tokio::time::sleep(Duration::from_millis(40))=>{}}}).await;
    let no_intent = f.stop_receipts();
    let no_wire = f.hooks.state.lock().unwrap().stop_calls;
    attempt.not_dispatched().unwrap();
    let result = f.drive(future).await.unwrap();
    let cleanup = f.join().await;
    assert!(no_intent.is_empty());
    assert_eq!(no_wire, 0);
    assert_eq!(result["structuredContent"]["stopConfirmed"], true);
    assert_eq!(cleanup["finalStopCalls"], 1);
}
#[tokio::test]
async fn accepted_manual_impact_during_started_confirmation_cannot_restore_start_ownership() {
    let mut f = Fixture::new("confirmation-impact").await;
    f.hooks.hold_start_ack.store(true, Ordering::Release);
    let handle = f.take.handle();
    let future = handle.start(StartOptions::default(), Policy::default());
    tokio::pin!(future);
    let hooks = f.hooks.clone();
    f.drive(async{tokio::select!{biased;result=&mut future=>panic!("Start ACK should be held {result:?}"),_=hooks.until(|state|state.event_states.len()==1)=>{}}}).await;
    // Controlled capability transition while the ordinary Start request waits
    // for ACK: this is not a claim that an unadmitted manual public route can
    // bypass its eventual transport lane. The actual Session/Take are retained.
    f.impact_port
        .begin_at_send(f.handle.token())
        .unwrap()
        .accepted()
        .unwrap();
    let (done, wait) = oneshot::channel();
    f.commands.send(Command::ReleaseStart(done)).await.unwrap();
    wait.await.unwrap();
    let result = f.drive(future).await.unwrap();
    let snapshot = f.take.handle().snapshot();
    let cleanup = f.cleanup().await;
    assert_eq!(result["isError"], true);
    assert_eq!(snapshot["ownership"], "unavailable");
    assert_eq!(cleanup["finalStopCalls"], 0);
}
#[tokio::test]
async fn already_sent_owned_stop_keeps_accepted_fact_but_impact_never_promotes_its_file() {
    let mut f = Fixture::new("sent-owned").await;
    f.start().await;
    let handle = f.take.handle();
    let future = handle.stop(None, Policy::default());
    tokio::pin!(future);
    let hooks = f.hooks.clone();
    f.drive(async{tokio::select!{biased;result=&mut future=>panic!("STOPPED should be withheld {result:?}"),_=hooks.until(|state|state.stop_calls==1)=>{}}}).await;
    // The peer has already flushed the actual ACK; bridge/readback completion
    // may settle later. Do not recall or replay the physical first frame.
    let pending = timeout(WATCH, async {
        loop {
            let pending = f.stop_receipts();
            if pending[0]["phase"] != "dispatching" {
                break pending;
            }
            f.pause(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    f.impact_port
        .begin_at_send(f.handle.token())
        .unwrap()
        .unknown()
        .unwrap();
    let (done, wait) = oneshot::channel();
    f.commands.send(Command::Release(done)).await.unwrap();
    wait.await.unwrap();
    let result = f.drive(future).await.unwrap();
    let receipts = f.stop_receipts();
    let handle = f.take.handle();
    let restart = f
        .drive(handle.start(StartOptions::default(), Policy::default()))
        .await
        .unwrap();
    let starts = f
        .hooks
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|r| r["d"]["requestType"] == "StartRecord")
        .count();
    let cleanup = f.join().await;
    assert_eq!(restart["isError"], true);
    assert!(
        restart["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("receipts are unresolved")
    );
    assert_eq!(starts, 1);
    assert_eq!(pending[0]["phase"], "accepted");
    assert_eq!(receipts[0]["requestAccepted"], true);
    assert_ne!(receipts[0]["phase"], "observed");
    assert_eq!(result["structuredContent"]["stopConfirmed"], false);
    assert_eq!(cleanup["finalStopCalls"], 1);
}

#[tokio::test]
async fn explicit_constructor_rejects_another_physical_view_or_dead_owner_without_consuming_loan() {
    let mut f = Fixture::new("binding-first").await;
    let other = Fixture::new("binding-other").await;
    f.drive({
        let handle = f.take.handle();
        async move { handle.status().await }
    })
    .await
    .unwrap();
    f.take.shutdown().await.unwrap();
    let store = f.take.take_store().unwrap();
    let (_, events) = mpsc::channel(1);
    let loan = Loan {
        session: f.handle.clone(),
        store,
        control: Arc::new(Mutex::new(ControlDomain::default())),
        clock: Arc::new(|| {
            clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap()
        }),
        identities: Box::new(|| panic!("rejected binding must not allocate identity")),
        io: f.io.clone(),
        events,
    };
    let first = match TakeSupervisor::spawn_with_record_impact(
        loan,
        Limits::default(),
        other._impact_owner.view(),
    ) {
        Err(failure) => failure,
        Ok(_) => panic!("foreign physical view admitted"),
    };
    assert_eq!(first.reason, takes::OwnerError::Binding);
    let dead = RecordImpactOwner::bind(&f.handle).unwrap();
    let view = dead.view();
    drop(dead);
    let second =
        match TakeSupervisor::spawn_with_record_impact(*first.loan, Limits::default(), view) {
            Err(failure) => failure,
            Ok(_) => panic!("dead owner admitted"),
        };
    assert_eq!(second.reason, takes::OwnerError::Binding);
    f.take = TakeSupervisor::spawn_with_record_impact(
        *second.loan,
        Limits::default(),
        f._impact_owner.view(),
    )
    .unwrap_or_else(|_| panic!("same loan could not be recovered"));
    let first_cleanup = f.join().await;
    let other_cleanup = other.join().await;
    assert_eq!(first_cleanup["writerReturned"], true);
    assert_eq!(first_cleanup["finalStopCalls"], 0);
    assert_eq!(other_cleanup["finalStopCalls"], 0);
}
#[test]
fn rollback_does_not_claim_to_reconstruct_an_already_suppressed_warning() {
    use obs_mcp_native::take_monitor::{MonitorOptions, TakeMonitorCore};
    let mut core = TakeMonitorCore::new(
        0,
        "1970-01-01T00:00:00.000Z".into(),
        MonitorOptions::default(),
    );
    let token = core.begin_stop_expectation();
    let stopped = json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED","outputActive":false});
    core.on_event(1, "RecordStateChanged", &stopped);
    core.rollback_stop_expectation(token);
    assert!(
        core.summary(1).warnings.is_empty(),
        "rollback has no historical event reconstruction"
    );
    core.on_event(2, "RecordStateChanged", &stopped);
    assert_eq!(
        core.summary(2)
            .warnings
            .iter()
            .filter(|w| w.message == "The recording stopped before it was asked to")
            .count(),
        1
    );
}

#[tokio::test]
async fn status_projection_rechecks_impact_after_its_admitted_real_read_settles() {
    let mut f = Fixture::new("status-projection").await;
    f.start().await;
    let gate = Arc::new(Gate::default());
    *f.dispatch_hooks.status_gate.lock().unwrap() = Some(gate.clone());
    let handle = f.take.handle();
    let future = handle.status();
    tokio::pin!(future);
    f.drive(async{tokio::select!{biased;result=&mut future=>panic!("readback gate not reached: {result:?}"),_=gate.reached()=>{}}}).await;
    f.tracked_manual_stop().await;
    gate.release();
    let result = f.drive(future).await.unwrap();
    let cleanup = f.cleanup().await;
    assert_eq!(result["structuredContent"]["ownership"], "unavailable");
    assert_eq!(cleanup["finalStopCalls"], 1);
}

#[tokio::test]
async fn inactive_revoked_take_retires_only_old_ownership_then_healthy_history_allows_fresh_take() {
    let mut f = Fixture::new("inactive-retirement").await;
    f.start().await;
    f.tracked_manual_stop().await;
    let handle = f.take.handle();
    let result = f.drive(handle.stop(None, Policy::default())).await.unwrap();
    let old_cleared = f.take.handle().snapshot();
    let old_stop_intents = f.stop_receipts();
    // Drain the historical old STOPPED before a new Start reservation: it is
    // not a notification for the next owned recording.
    let (done, wait) = oneshot::channel();
    f.commands.send(Command::Release(done)).await.unwrap();
    wait.await.unwrap();
    let historical_published = f.published().await;
    let fence = f.fence.clone();
    f.drive(fence.synchronize(Instant::now() + WATCH))
        .await
        .unwrap();
    let handle = f.take.handle();
    let fresh = f
        .drive(handle.start(StartOptions::default(), Policy::default()))
        .await
        .unwrap();
    let starts = f
        .hooks
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|r| r["d"]["requestType"] == "StartRecord")
        .count();
    let file_calls = f.host.inspections.load(Ordering::Acquire);
    f.hooks.publish_stops.store(true, Ordering::Release);
    let cleanup = f.join().await;
    save(
        "inactive-retirement",
        &json!({"historicalPublishedThrough":historical_published,"retired":result,"oldCleared":old_cleared,"oldStopIntents":old_stop_intents,"fresh":fresh,"starts":starts,"fileCallsBeforeNewCleanup":file_calls,"cleanup":cleanup}),
    );
    assert_eq!(result["structuredContent"]["stopConfirmed"], false);
    assert_eq!(result["structuredContent"]["ownership"], "unavailable");
    assert!(
        result["structuredContent"]["reason"]
            .as_str()
            .unwrap()
            .contains("OBS is now inactive")
    );
    assert!(old_cleared.is_null());
    assert!(old_stop_intents.is_empty());
    assert_ne!(fresh["isError"], true);
    assert_eq!(fresh["structuredContent"]["ownership"], "continuous");
    assert_eq!(starts, 2);
    assert_eq!(file_calls, 0);
    assert_eq!(cleanup["finalStopCalls"], 2);
    assert_eq!(cleanup["writerReturned"], true);
}

#[tokio::test]
async fn a_historical_stopped_event_applied_during_preflight_does_not_taint_the_fresh_start() {
    let mut f = Fixture::new("historical-preflight").await;
    // This effect is settled BEFORE the logical Start reserves its ticket.
    // Only the old notification is delayed into the new preflight window.
    f.tracked_manual_stop().await;
    let gate = Arc::new(Gate::default());
    *f.host.preflight_gate.lock().unwrap() = Some(gate.clone());
    let handle = f.take.handle();
    let future = handle.start(StartOptions::default(), Policy::default());
    tokio::pin!(future);
    f.drive(async{tokio::select!{biased;result=&mut future=>panic!("preflight gate not reached {result:?}"),_=gate.reached()=>{}}}).await;
    let (done, wait) = oneshot::channel();
    f.commands.send(Command::Release(done)).await.unwrap();
    wait.await.unwrap();
    let historical = f.published().await;
    let fence = f.fence.clone();
    f.drive(fence.synchronize(Instant::now() + WATCH))
        .await
        .unwrap();
    gate.release();
    let fresh = f.drive(future).await.unwrap();
    let cleanup = f.cleanup().await;
    save(
        "historical-preflight",
        &json!({"historicalPublishedThrough":historical,"fresh":fresh,"cleanup":cleanup}),
    );
    assert_ne!(fresh["isError"], true);
    assert_eq!(fresh["structuredContent"]["ownership"], "continuous");
    assert_eq!(cleanup["takeJoined"], true);
}

#[tokio::test]
async fn the_unfired_original_timer_retires_an_inactive_revoked_take_without_sending_stop() {
    let mut f = Fixture::new("automatic-inactive-retirement").await;
    f.start().await;
    f.tracked_manual_stop().await;
    f.pause(Duration::from_millis(1350)).await;
    let retired = f.take.handle().snapshot();
    let stop_intents = f.stop_receipts();
    let probes = f.host.inspections.load(Ordering::Acquire);
    let frames = f.hooks.state.lock().unwrap().stop_calls;
    let cleanup = f.join().await;
    assert!(
        retired.is_null(),
        "original deadline must reconcile the inactive old Take"
    );
    assert!(stop_intents.is_empty());
    assert_eq!(probes, 0);
    assert_eq!(frames, 1, "only the original manual frame");
    assert_eq!(cleanup["finalStopCalls"], 1);
    assert_eq!(cleanup["stopConfirmedAtCleanup"], false);
    assert_eq!(cleanup["writerReturned"], true);
}

#[tokio::test]
async fn historical_observed_stop_allows_stage_during_inspection_but_later_impact_refuses_file_success()
 {
    let mut f = Fixture::composed("stage-inspection-impact", false, true).await;
    f.start().await;
    f.hooks.publish_stops.store(true, Ordering::Release);
    let gate = Arc::new(Gate::default());
    *f.host.inspection_gate.lock().unwrap() = Some(gate.clone());
    let handle = f.take.handle();
    let stop = handle.stop(None, Policy::default());
    tokio::pin!(stop);
    f.drive(async{tokio::select!{biased;result=&mut stop=>panic!("inspection was not retained {result:?}"),_=gate.reached()=>{}}}).await;
    let before = f.stop_receipts();
    let scope = f.broker.as_ref().unwrap().scope(ScopeKind::Stage);
    let scope_admitted = scope.is_ok();
    let stage_result = if let Ok(mut store) = scope {
        let target = Target {
            scene_collection: "Fixture".into(),
            scene_name: "Review".into(),
            canvas_uuid: None,
        };
        let caller = CallerBinding::resolve(
            CallerContext {
                http_client_id: Some(&json!("fixture")),
                ..Default::default()
            },
            None,
            false,
            None,
            None,
        );
        let mut transport = StageTransport::bind(
            TransportContext {
                handle: f.handle.clone(),
                bridge: &f.bridge,
                control: f.control.clone(),
                clock: f.clock.clone(),
                cancellation: Cancellation::new(),
            },
            BoundCall::stage(target.clone()).unwrap(),
            caller,
        )
        .unwrap();
        let mut machine = StageMachine::default();
        let mut context = StageContext {
            reader: &mut transport.reader,
            mutator: &mut transport.mutator,
            store: &mut store,
            clock: f.clock.clone(),
            deadline: Instant::now() + WATCH,
            screenshot_max_bytes: 1024,
        };
        let future = machine.stage(
            &mut context,
            target,
            "00000000-0000-4000-8000-000000000900",
            "00000000-0000-4000-8000-000000000901",
        );
        let result = timeout(
            WATCH,
            pump(future, &mut f.source, &f.destination, &mut f.delivered),
        )
        .await
        .unwrap();
        let value = match result {
            Ok(success) => success.tool_result(),
            Err(failure) => failure.tool_result(),
        };
        transport.shutdown().await.unwrap();
        drop(store);
        value
    } else {
        json!({"isError":true,"reason":"scope_refused"})
    };
    // Same physical Session and the real manual send guard; this later effect
    // can revoke current/file authority but cannot undo an earlier Stop fact.
    f.tracked_manual_stop().await;
    gate.release();
    let result = f.drive(stop).await.unwrap();
    let after = f.stop_receipts();
    let log_calls = f.host.logs.load(Ordering::Acquire);
    let cleanup = f.join().await;
    save(
        "stage-inspection-impact",
        &json!({"scopeAdmitted":scope_admitted,"before":before,"stage":stage_result,"take":result,"after":after,"logCalls":log_calls,"cleanup":cleanup}),
    );
    assert!(
        scope_admitted,
        "historically observed Stop must not block an independent Stage scope"
    );
    assert_ne!(stage_result["isError"], true);
    assert_eq!(stage_result["structuredContent"]["status"], "staged");
    assert_eq!(before[0]["phase"], "observed");
    assert_eq!(after[0]["phase"], "observed");
    assert_eq!(after[0]["operationId"], before[0]["operationId"]);
    assert_eq!(result["structuredContent"]["stopConfirmed"], false);
    assert_eq!(result["structuredContent"]["receiptStatus"], "observed");
    assert!(result["structuredContent"].get("outputPath").is_none());
    assert_eq!(log_calls, 0);
    assert_eq!(cleanup["brokerWriterReturned"], true);
    assert_eq!(cleanup["finalStopCalls"], 2);
}
