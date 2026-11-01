// SPDX-License-Identifier: GPL-2.0-only
//! Actual single-socket composition and receipt files. Host media/ACL probes
//! are injected; these controls do not qualify live OBS or MCP take routing.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    clock,
    control::{CallerContext, ControlDomain},
    custody::{Clock, Confirmation, Details, PolicyCall, TrackedRequest},
    observation::{ReadRequest, Target},
    operation_store::{OperationKind, OperationStore},
    preflight::{self, Report},
    service::{
        ConnectionDescriptor, Options, PreparationOwners, REQUIRED_SUBSCRIPTIONS, Resources,
        Service, ServiceError, TakeServices,
    },
    session::{ConnectionToken, ObsSession, SessionOptions},
    session_observation::ObservationBridge,
    stage_transport::{CallerBinding, Cancellation},
    take_monitor::Summary,
    take_transport::{Bounds, HostServices, PreflightReader},
    takes::{ClipCheck, IoFuture, Limits, Policy, StartOptions},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(6);
static NEXT: AtomicU64 = AtomicU64::new(0);
fn id(n: u64) -> String {
    format!("00000000-0000-4000-8000-{n:012x}")
}
fn clock() -> Clock {
    Arc::new(|| clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap())
}
fn caller() -> CallerBinding {
    CallerBinding::resolve(
        CallerContext {
            http_client_id: Some(&json!("codex")),
            ..Default::default()
        },
        None,
        false,
        None,
        None,
    )
}
fn target() -> Target {
    Target {
        scene_collection: "Fixture".into(),
        scene_name: "Review".into(),
        canvas_uuid: None,
    }
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "service-contract-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
struct Gate {
    name: String,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}
#[derive(Default)]
struct Host {
    preflight_gate: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    inspect_gate: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    joined: AtomicBool,
}
impl Host {
    fn hold(&self, inspect: bool) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, seen) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        *if inspect {
            self.inspect_gate.lock().unwrap()
        } else {
            self.preflight_gate.lock().unwrap()
        } = Some((entered, resume));
        (seen, release)
    }
}
impl HostServices for Host {
    fn preflight(&self, _: PreflightReader, _: preflight::Options) -> IoFuture<Report> {
        let gate = self.preflight_gate.lock().unwrap().take();
        Box::pin(async move {
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.await;
            }
            Report {
                ready: true,
                checks: vec![preflight::Check {
                    id: "record-directory".into(),
                    status: preflight::Status::Pass,
                    message: "fixture host probe".into(),
                }],
            }
        })
    }
    fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
        let gate = self.inspect_gate.lock().unwrap().take();
        Box::pin(async move {
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.await;
            }
            ClipCheck {
                duration_seconds: Some(1.0),
                video_streams: Some(1),
                audio_streams: Some(1),
                ..Default::default()
            }
        })
    }
    fn write_log(&self, _: String, _: Summary) -> IoFuture<Option<String>> {
        Box::pin(async { Some("fixture-log.json".into()) })
    }
    fn join_observers(&self) -> IoFuture<Result<(), String>> {
        self.joined.store(true, Ordering::Release);
        Box::pin(async { Ok(()) })
    }
}
struct Fixture {
    url: String,
    trace: Arc<Mutex<Vec<Value>>>,
    gate: Arc<Mutex<Option<Gate>>>,
    lose: Arc<AtomicBool>,
    meters: Arc<AtomicBool>,
    meter_count: Arc<AtomicU64>,
    rejections: Arc<Mutex<HashMap<String, (u64, String)>>>,
    control: obs_mcp_native::custody::SharedControl,
    advance_clock: Arc<AtomicBool>,
    read_epoch: Arc<AtomicU64>,
    task: JoinHandle<()>,
}
impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let trace = Arc::new(Mutex::new(vec![]));
        let gate = Arc::new(Mutex::new(None::<Gate>));
        let lose = Arc::new(AtomicBool::new(false));
        let history = trace.clone();
        let held = gate.clone();
        let lost = lose.clone();
        let meters = Arc::new(AtomicBool::new(false));
        let emitting = meters.clone();
        let meter_count = Arc::new(AtomicU64::new(0));
        let meter_history = meter_count.clone();
        let rejections = Arc::new(Mutex::new(HashMap::<String, (u64, String)>::new()));
        let rejected = rejections.clone();
        let advance_clock = Arc::new(AtomicBool::new(false));
        let advancing = advance_clock.clone();
        let read_epoch = Arc::new(AtomicU64::new(1_793_448_000_000));
        let peer_epoch = read_epoch.clone();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(socket).await.unwrap();
            peer.send(Message::Text(
                json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let identify: Value =
                serde_json::from_str(&peer.next().await.unwrap().unwrap().into_text().unwrap())
                    .unwrap();
            assert_eq!(identify["op"], 1);
            peer.send(Message::Text(
                json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let mut preview = "Main";
            let mut program = "Main";
            let mut active = false;
            loop {
                let message = tokio::select! {
                    next=peer.next()=>next,
                    _=tokio::time::sleep(Duration::from_millis(1)),if emitting.load(Ordering::Acquire)=>{
                        let frame=json!({"op":5,"d":{"eventType":"InputVolumeMeters","eventIntent":1<<16,"eventData":{"inputs":[]}}});
                        if peer.send(Message::Text(frame.to_string().into())).await.is_err(){return;}
                        meter_history.fetch_add(1,Ordering::Relaxed);
                        continue;
                    },
                };
                let Some(message) = message else {
                    return;
                };
                let Ok(message) = message else {
                    return;
                };
                let Message::Text(text) = message else {
                    if matches!(message, Message::Close(_)) {
                        let _ = peer.flush().await;
                        return;
                    }
                    continue;
                };
                let request: Value = serde_json::from_str(&text).unwrap();
                history.lock().unwrap().push(request.clone());
                if request["op"] == 8 {
                    let pause = {
                        let mut gate = held.lock().unwrap();
                        if gate
                            .as_ref()
                            .is_some_and(|gate| gate.name == "RequestBatch")
                        {
                            gate.take()
                        } else {
                            None
                        }
                    };
                    if let Some(gate) = pause {
                        let _ = gate.entered.send(());
                        let _ = gate.release.await;
                    }
                    let rows=request["d"]["requests"].as_array().unwrap().iter().map(|call| {let data=match call["requestType"].as_str().unwrap(){"GetSceneItemList"=>if advancing.load(Ordering::Acquire){json!({"sceneItems":[{"sceneItemId":7,"sceneItemIndex":0,"sourceName":"Window","sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemTransform":{"positionX":17}}]})}else{json!({"sceneItems":[]})},"GetInputSettings"=>json!({"inputKind":"screen_capture","inputSettings":{"window":77,"opaque":{"retained":true}}}),"GetInputMute"=>json!({"inputMuted":true}),"GetInputVolume"=>json!({"inputVolumeMul":0.5}),"GetInputAudioTracks"=>json!({"inputAudioTracks":{"1":true}}),other=>panic!("unexpected snapshot batch request {other}")};json!({"requestType":call["requestType"],"requestId":call["requestId"],"requestStatus":{"result":true,"code":100},"responseData":data})}).collect::<Vec<_>>();
                    if advancing.load(Ordering::Acquire) {
                        peer_epoch.fetch_add(1000, Ordering::AcqRel);
                    }
                    if peer.send(Message::Text(json!({"op":9,"d":{"requestId":request["d"]["requestId"],"results":rows}}).to_string().into())).await.is_err(){return;}
                    continue;
                }
                let kind = request["d"]["requestType"].as_str().unwrap();
                let rejection = rejected.lock().unwrap().get(kind).cloned();
                if let Some((code, comment)) = rejection {
                    let frame = json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":kind,"requestStatus":{"result":false,"code":code,"comment":comment}}});
                    if peer
                        .send(Message::Text(frame.to_string().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                let pause = {
                    let mut gate = held.lock().unwrap();
                    if gate.as_ref().is_some_and(|gate| gate.name == kind) {
                        gate.take()
                    } else {
                        None
                    }
                };
                if let Some(gate) = pause {
                    let _ = gate.entered.send(());
                    let _ = gate.release.await;
                }
                let data = match kind {
                    "GetVersion" => {
                        json!({"availableRequests":["SetProfileParameter","SetCurrentProfile","SetPersistentData","GetVersion","GetRecordStatus","GetStats","GetCurrentProgramScene","GetInputList","GetSceneItemList","GetProfileParameter","GetInputAudioTracks","StartRecord","StopRecord","CreateRecordChapter","GetSceneCollectionList","GetSceneList","GetStudioModeEnabled","GetCurrentPreviewScene","GetVideoSettings","GetStreamStatus","GetReplayBufferStatus","GetVirtualCamStatus","GetOutputList","GetOutputStatus"],"supportedImageFormats":[],"platform":"macos"})
                    }
                    "GetStudioModeEnabled" => json!({"studioModeEnabled":true}),
                    "GetSceneCollectionList" => json!({"currentSceneCollectionName":"Fixture"}),
                    "GetSceneList" => {
                        json!({"currentProgramSceneName":program,"currentProgramSceneUuid":if program=="Main"{"main"}else{"review"},"scenes":[{"sceneName":"Main","sceneUuid":"main"},{"sceneName":"Review","sceneUuid":"review"}]})
                    }
                    "GetCurrentPreviewScene" => {
                        json!({"currentPreviewSceneName":preview,"currentPreviewSceneUuid":if preview=="Main"{"main"}else{"review"}})
                    }
                    "GetCurrentProgramScene" => {
                        json!({"currentProgramSceneName":program,"currentProgramSceneUuid":if program=="Main"{"main"}else{"review"}})
                    }
                    "GetVideoSettings" => json!({"baseWidth":1920,"baseHeight":1080}),
                    "GetInputList" => {
                        if advancing.load(Ordering::Acquire) {
                            json!({"inputs":[{"inputName":"Window","inputKind":"screen_capture"}]})
                        } else {
                            json!({"inputs":[]})
                        }
                    }
                    "GetSceneItemList" => json!({"sceneItems":[]}),
                    "GetSourceFilterList" => json!({"filters":[]}),
                    "GetSourceScreenshot" => json!({"imageData":"data:image/png;base64,AAAA"}),
                    "GetStreamStatus" | "GetReplayBufferStatus" | "GetVirtualCamStatus" => {
                        json!({"outputActive":false,"outputReconnecting":false})
                    }
                    "GetOutputList" => json!({"outputs":[]}),
                    "GetRecordStatus" => {
                        json!({"outputActive":active,"outputPaused":false,"outputBytes":1048576})
                    }
                    "GetStats" => {
                        json!({"availableDiskSpace":50000,"renderSkippedFrames":0,"renderTotalFrames":0})
                    }
                    "GetProfileParameter" => json!({"parameterValue":"1"}),
                    "SetCurrentPreviewScene" => {
                        preview = "Review";
                        json!({})
                    }
                    "TriggerStudioModeTransition" => {
                        program = preview;
                        json!({})
                    }
                    "StartRecord" => {
                        active = true;
                        json!({})
                    }
                    "StopRecord" => {
                        active = false;
                        json!({"outputPath":"fixture.mkv"})
                    }
                    "SetInputMute"
                    | "SetProfileParameter"
                    | "SetCurrentProfile"
                    | "SetPersistentData" => json!({}),
                    "CreateRecordChapter" => json!({}),
                    other => panic!("unexpected service fixture request {other}"),
                };
                if advancing.load(Ordering::Acquire)
                    && matches!(kind, "GetCurrentProgramScene" | "GetInputList")
                {
                    peer_epoch.fetch_add(1000, Ordering::AcqRel);
                }
                let event = match kind {
                    "SetCurrentPreviewScene" => Some(("CurrentPreviewSceneChanged", json!({}), 4)),
                    "TriggerStudioModeTransition" => {
                        Some(("CurrentProgramSceneChanged", json!({}), 4))
                    }
                    "StartRecord" => Some((
                        "RecordStateChanged",
                        json!({"outputActive":true,"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED"}),
                        64,
                    )),
                    "StopRecord" => Some((
                        "RecordStateChanged",
                        json!({"outputActive":false,"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED","outputPath":"fixture.mkv"}),
                        64,
                    )),
                    _ => None,
                };
                if let Some((name, data, intent)) = event
                    && peer.send(Message::Text(json!({"op":5,"d":{"eventType":name,"eventIntent":intent,"eventData":data}}).to_string().into())).await.is_err(){return;}
                if kind == "SetCurrentPreviewScene" && lost.load(Ordering::Acquire) {
                    return;
                }
                if peer.send(Message::Text(json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":kind,"requestStatus":{"result":true,"code":100},"responseData":data}}).to_string().into())).await.is_err(){return;}
            }
        });
        Self {
            control: Arc::new(Mutex::new(ControlDomain::default())),
            advance_clock,
            read_epoch,
            url,
            trace,
            gate,
            lose,
            meters,
            meter_count,
            rejections,
            task,
        }
    }
    fn hold(&self, name: &str) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, seen) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        *self.gate.lock().unwrap() = Some(Gate {
            name: name.into(),
            entered,
            release: resume,
        });
        (seen, release)
    }
    fn count(&self, name: &str) -> usize {
        self.trace
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["d"]["requestType"] == name)
            .count()
    }
    async fn owners(&self, dir: &Dir, host: Option<Arc<Host>>) -> (Service, ConnectionToken) {
        self.owners_timeout(dir, host, Duration::from_secs(30))
            .await
    }
    async fn owners_timeout(
        &self,
        dir: &Dir,
        host: Option<Arc<Host>>,
        operation_timeout: Duration,
    ) -> (Service, ConnectionToken) {
        self.owners_clock(dir, host, operation_timeout, clock())
            .await
    }
    async fn owners_clock(
        &self,
        dir: &Dir,
        host: Option<Arc<Host>>,
        operation_timeout: Duration,
        clock: Clock,
    ) -> (Service, ConnectionToken) {
        self.owners_clock_pending(dir, host, operation_timeout, clock, None)
            .await
    }
    async fn owners_clock_pending(
        &self,
        dir: &Dir,
        host: Option<Arc<Host>>,
        operation_timeout: Duration,
        clock: Clock,
        pending: Option<Arc<AtomicBool>>,
    ) -> (Service, ConnectionToken) {
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
        let token = session.token();
        let bridge = ObservationBridge::bind(session.handle(), events, None).unwrap();
        let mut sequence = 0;
        let resources = Resources {
            session,
            bridge,
            store: OperationStore::new(&dir.0, &self.url).unwrap(),
            control: self.control.clone(),
            clock,
            identities: Box::new(move || {
                sequence += 1;
                id(sequence)
            }),
        };
        let take = host.map(|host| TakeServices {
            host,
            exact_endpoint: self.url.clone(),
            pending_settings: pending.unwrap_or_else(|| Arc::new(AtomicBool::new(false))),
            bounds: Bounds::default(),
            limits: Limits {
                inspection: Duration::from_millis(30),
                ..Default::default()
            },
        });
        (
            Service::spawn(
                resources,
                Options {
                    operation_timeout,
                    preparation: Some(PreparationOwners::new(&dir.0).unwrap()),
                    take,
                    connection: Some(ConnectionDescriptor {
                        url: self.url.clone(),
                        has_password: false,
                    }),
                    state_root: Some(dir.0.clone()),
                    ..Default::default()
                },
            )
            .unwrap_or_else(|_| panic!("service binding refused")),
            token,
        )
    }
    async fn joined(self) {
        timeout(WAIT, self.task).await.unwrap().unwrap();
    }
}
fn tracked(token: ConnectionToken, n: u64) -> TrackedRequest {
    TrackedRequest {
        operation_id: id(n),
        kind: OperationKind::StageScene,
        expected: token,
        policy: PolicyCall {
            tool_name: "obs-set-current-preview-scene".into(),
            arguments: json!({"sceneName":"Review"}),
            read_only_hint: false,
            caller: "codex".into(),
            confirmation: Confirmation {
                enabled: false,
                can_elicit: false,
                input_responses: None,
            },
        },
        details: Details::default(),
        request_type: "SetCurrentPreviewScene".into(),
        request_data: json!({"sceneName":"Review"}),
        timeout: Duration::from_secs(3),
    }
}
async fn idle(handle: &obs_mcp_native::service::ServiceHandle) {
    timeout(WAIT, async {
        loop {
            let summary = handle.summary().await.unwrap();
            if summary.get("jobsPending") != Some(&json!(true)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn empty_service_handoff_joins_observer_socket_and_returns_writer_once() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, None).await;
    let result = service.handle().handoff().await.unwrap();
    assert_eq!(result["nativeOwnersJoined"], true);
    let receipt = service.shutdown().await.unwrap();
    assert!(receipt.joined && receipt.handoff_allowed);
    assert!(service.take_store().is_some());
    assert!(service.take_store().is_none());
    fixture.joined().await;
}

#[tokio::test]
async fn stage_survives_frontend_drop_and_keeps_token_across_busy_take_and_raw() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, token) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("GetSourceScreenshot");
    let frontend = tokio::spawn({
        let handle = handle.clone();
        async move { handle.stage(target(), caller(), Cancellation::new()).await }
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    frontend.abort();
    assert_eq!(
        handle.tracked(tracked(token, 90)).await.unwrap_err(),
        ServiceError::Busy
    );
    assert_eq!(
        handle
            .start_take(StartOptions::default(), Policy::default())
            .await
            .unwrap_err(),
        ServiceError::Busy
    );
    release.send(()).unwrap();
    idle(&handle).await;
    assert_eq!(fixture.count("SetCurrentPreviewScene"), 1);
    assert_eq!(
        handle.handoff().await.unwrap_err(),
        ServiceError::StageToken
    );
    let take = handle
        .take_scene(id(1), caller(), Cancellation::new())
        .await
        .unwrap();
    assert_ne!(take["isError"], true);
    assert_eq!(fixture.count("TriggerStudioModeTransition"), 1);
    handle.handoff().await.unwrap();
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn dropped_raw_frontend_retains_writer_until_reply_and_acceptance_blocks_handoff() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, token) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("SetCurrentPreviewScene");
    let frontend = tokio::spawn({
        let handle = handle.clone();
        async move { handle.tracked(tracked(token, 91)).await }
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    frontend.abort();
    assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Busy);
    assert!(service.take_store().is_none());
    release.send(()).unwrap();
    idle(&handle).await;
    assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Uncertain);
    assert_eq!(
        handle
            .stage(target(), caller(), Cancellation::new())
            .await
            .unwrap_err(),
        ServiceError::Uncertain
    );
    assert_eq!(fixture.count("SetCurrentPreviewScene"), 1);
    assert!(service.shutdown().await.unwrap().joined);
    let store = service.take_store().unwrap();
    assert_eq!(
        store.read(&id(91)).unwrap().phase(),
        obs_mcp_native::operation_store::Phase::Accepted
    );
    fixture.joined().await;
}

#[tokio::test]
async fn lost_raw_reply_is_durable_unknown_and_never_replayed() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    fixture.lose.store(true, Ordering::Release);
    let (mut service, token) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let result = handle.tracked(tracked(token, 92)).await.unwrap();
    assert_eq!(result["status"], "unknown");
    assert_eq!(
        handle.tracked(tracked(token, 93)).await.unwrap_err(),
        ServiceError::Uncertain
    );
    assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Uncertain);
    service.shutdown().await.unwrap();
    assert_eq!(
        service.take_store().unwrap().read(&id(92)).unwrap().phase(),
        obs_mcp_native::operation_store::Phase::Unknown
    );
    assert_eq!(fixture.count("SetCurrentPreviewScene"), 1);
    fixture.joined().await;
}

#[tokio::test]
async fn take_inspection_retains_scope_after_frontend_drop_while_stage_is_independent() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let host = Arc::new(Host::default());
    let (seen, release) = host.hold(true);
    let (mut service, token) = fixture.owners(&dir, Some(host.clone())).await;
    let handle = service.handle();
    let started = handle
        .start_take(StartOptions::default(), Policy::default())
        .await
        .unwrap();
    assert_ne!(started["isError"], true, "{started}");
    assert_eq!(fixture.count("StartRecord"), 1);
    assert_eq!(
        handle.tracked(tracked(token, 95)).await.unwrap_err(),
        ServiceError::Busy
    );
    let stop = tokio::spawn({
        let handle = handle.clone();
        async move { handle.stop_take(None, Policy::default()).await }
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    stop.abort();
    let staged = handle
        .stage(target(), caller(), Cancellation::new())
        .await
        .unwrap();
    assert_ne!(
        staged["isError"], true,
        "permitted stage remains independent of held take inspection"
    );
    assert!(service.take_store().is_none());
    let mut shutdown = Box::pin(service.shutdown());
    assert!(poll!(&mut shutdown).is_pending());
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        poll!(&mut shutdown).is_pending(),
        "held inspection was abandoned after timeout"
    );
    release.send(()).unwrap();
    let receipt = timeout(WAIT, shutdown).await.unwrap().unwrap();
    assert!(receipt.joined && receipt.take_joined && receipt.stop_confirmed);
    assert!(host.joined.load(Ordering::Acquire));
    assert!(service.take_store().is_some());
    assert_eq!(fixture.count("StartRecord"), 1);
    assert_eq!(fixture.count("StopRecord"), 1);
    fixture.joined().await;
}

#[tokio::test]
async fn take_startup_loan_and_shutdown_remain_owned_while_preflight_is_held() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let host = Arc::new(Host::default());
    let (seen, release) = host.hold(false);
    let (mut service, token) = fixture.owners(&dir, Some(host)).await;
    let handle = service.handle();
    let start = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .start_take(StartOptions::default(), Policy::default())
                .await
        }
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    start.abort();
    assert_eq!(
        handle.tracked(tracked(token, 96)).await.unwrap_err(),
        ServiceError::Busy
    );
    assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Busy);
    let mut shutdown = Box::pin(service.shutdown());
    assert!(poll!(&mut shutdown).is_pending());
    release.send(()).unwrap();
    let receipt = timeout(WAIT, shutdown).await.unwrap().unwrap();
    assert!(receipt.joined && receipt.stop_confirmed);
    assert_eq!(fixture.count("StartRecord"), 1);
    assert_eq!(fixture.count("StopRecord"), 1);
    assert!(service.take_store().is_some());
    fixture.joined().await;
}

#[tokio::test]
async fn maximum_duration_stops_without_frontend_await_and_idempotent_result_survives_loan_return()
{
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, Some(Arc::new(Host::default()))).await;
    let handle = service.handle();
    handle
        .start_take(
            StartOptions {
                max_duration_seconds: Some(1),
                ..Default::default()
            },
            Policy::default(),
        )
        .await
        .unwrap();
    timeout(WAIT, async {
        loop {
            if fixture.count("StopRecord") == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    idle(&handle).await;
    let first = handle.stop_take(None, Policy::default()).await.unwrap();
    let second = handle.stop_take(None, Policy::default()).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(
        handle.handoff().await.unwrap_err(),
        ServiceError::RetainedState
    );
    assert_eq!(
        handle.stop_take(None, Policy::default()).await.unwrap(),
        first,
        "handoff refusal discarded the idempotent result"
    );
    assert_eq!(fixture.count("StopRecord"), 1);
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn emergency_drop_invalidates_owned_jobs_without_claiming_joined_shutdown() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (service, _) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("GetSourceScreenshot");
    let stage = tokio::spawn({
        let handle = handle.clone();
        async move { handle.stage(target(), caller(), Cancellation::new()).await }
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    drop(service);
    assert_eq!(handle.summary().await.unwrap_err(), ServiceError::Closed);
    release.send(()).unwrap();
    assert_eq!(
        timeout(WAIT, stage).await.unwrap().unwrap().unwrap_err(),
        ServiceError::Closed
    );
    fixture.joined().await;
}

#[tokio::test]
async fn reopened_unknown_journal_refuses_every_mutation_and_handoff_before_obs_reads() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    {
        let mut store = OperationStore::new(&dir.0, &fixture.url).unwrap();
        let receipt = store
            .prepare(
                &id(700),
                OperationKind::RecordStart,
                1,
                "2026-10-31T12:00:00.000Z",
                Default::default(),
            )
            .unwrap();
        store
            .update(
                &receipt,
                obs_mcp_native::operation_store::OperationUpdate::new(
                    obs_mcp_native::operation_store::Phase::Unknown,
                ),
                "2026-10-31T12:00:00.000Z",
            )
            .unwrap();
    }
    let (mut service, token) = fixture.owners(&dir, Some(Arc::new(Host::default()))).await;
    let handle = service.handle();
    assert_eq!(
        handle
            .stage(target(), caller(), Cancellation::new())
            .await
            .unwrap_err(),
        ServiceError::Uncertain
    );
    assert_eq!(
        handle.tracked(tracked(token, 701)).await.unwrap_err(),
        ServiceError::Uncertain
    );
    assert_eq!(
        handle
            .start_take(StartOptions::default(), Policy::default())
            .await
            .unwrap_err(),
        ServiceError::Uncertain
    );
    assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Uncertain);
    assert!(fixture.trace.lock().unwrap().is_empty());
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn incomplete_event_profile_returns_untouched_owners_before_spawning_service() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (session, events) = ObsSession::connect(&fixture.url, None, SessionOptions::default())
        .await
        .unwrap();
    let bridge = ObservationBridge::bind(session.handle(), events, None).unwrap();
    let resources = Resources {
        session,
        bridge,
        store: OperationStore::new(&dir.0, &fixture.url).unwrap(),
        control: Arc::new(Mutex::new(ControlDomain::default())),
        clock: clock(),
        identities: Box::new(|| id(1)),
    };
    let (reason, resources) = match Service::spawn(resources, Options::default()) {
        Ok(_) => panic!("incomplete profile admitted"),
        Err(error) => error,
    };
    assert_eq!(reason, ServiceError::Binding);
    assert!(fixture.trace.lock().unwrap().is_empty());
    resources.bridge.shutdown().await.unwrap();
    resources.session.shutdown().await.unwrap();
    fixture.joined().await;
}

#[tokio::test]
async fn sustained_meter_feed_allows_bounded_user_stop_and_preserves_one_physical_owner() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, Some(Arc::new(Host::default()))).await;
    let handle = service.handle();
    handle
        .start_take(StartOptions::default(), Policy::default())
        .await
        .unwrap();
    fixture.meters.store(true, Ordering::Release);
    timeout(WAIT, async {
        while fixture.meter_count.load(Ordering::Acquire) < 20 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let status = timeout(WAIT, handle.take_status()).await.unwrap().unwrap();
    assert_ne!(status["isError"], true, "{status}");
    let result = timeout(WAIT, handle.stop_take(None, Policy::default()))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(result["isError"], true, "{result}");
    assert_eq!(fixture.count("StartRecord"), 1);
    assert_eq!(fixture.count("StopRecord"), 1);
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn production_observe_uses_actual_connection_and_same_bridge_without_mutation() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, token) = fixture.owners(&dir, None).await;
    let report = service.handle().observe(None).await.unwrap();
    assert_eq!(report["status"], "observed", "{report}");
    assert_eq!(report["generation"], token.generation());
    assert_eq!(report["connection"]["generation"], token.generation());
    assert_eq!(report["connection"]["url"], fixture.url);
    assert_eq!(report["connection"]["connected"], true);
    assert_eq!(report["connection"]["identified"], true);
    assert_eq!(report["connection"]["hasPassword"], false);
    assert!(
        report["connection"]["availableRequestCount"]
            .as_u64()
            .unwrap()
            > 10
    );
    assert_eq!(report["connection"]["versionInfo"]["platform"], "macos");
    assert_eq!(report["requests"], 20);
    assert_eq!(report["control"], Value::Null);
    assert_eq!(report["activeTake"], Value::Null);
    assert_eq!(
        report["recovery"],
        json!({"status":"observed","operations":[]})
    );
    assert_eq!(
        report["operationReceipts"],
        json!({"status":"observed","operations":[],"total":0,"truncated":false})
    );
    assert_eq!(fixture.count("GetVersion"), 1);
    service.handle().observe(None).await.unwrap();
    assert_eq!(
        fixture.count("GetVersion"),
        1,
        "same connection metadata was re-probed"
    );
    assert!(fixture.trace.lock().unwrap().iter().all(|frame| {
        frame["d"]["requestType"]
            .as_str()
            .unwrap()
            .starts_with("Get")
    }));
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn production_observe_runs_while_take_owns_writer_and_keeps_automatic_deadline_live() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, Some(Arc::new(Host::default()))).await;
    let handle = service.handle();
    handle
        .start_take(
            StartOptions {
                max_duration_seconds: Some(1),
                expect_silent: true,
                ..Default::default()
            },
            Policy::default(),
        )
        .await
        .unwrap();
    fixture.meters.store(true, Ordering::Release);
    let report = timeout(WAIT, handle.observe(None)).await.unwrap().unwrap();
    assert_eq!(report["record"]["outputActive"], true, "{report}");
    assert_eq!(report["activeTake"]["running"], true);
    assert!(report["activeTake"]["id"].as_str().is_some());
    assert_eq!(report["activeTake"]["locked"], true);
    assert_eq!(report["activeTake"]["expectSilent"], true);
    assert_eq!(report["activeTake"]["maxDurationSeconds"], 1);
    assert_eq!(report["activeTake"]["ownershipLost"], Value::Null);
    assert_eq!(report["operationReceipts"]["status"], "observed");
    assert_eq!(
        report["operationReceipts"]["operations"][0]["kind"],
        "record-start"
    );
    assert_eq!(report["status"], "observed");
    timeout(WAIT, async {
        while fixture.count("StopRecord") != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(service.shutdown().await.unwrap().joined);
    assert_eq!(fixture.count("StopRecord"), 1);
    fixture.joined().await;
}

#[tokio::test]
async fn dropped_observe_frontend_keeps_same_read_job_until_joined_service_shutdown() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("GetRecordStatus");
    let frontend = tokio::spawn(async move { handle.observe(None).await });
    timeout(WAIT, seen).await.unwrap().unwrap();
    frontend.abort();
    let mut shutdown = Box::pin(service.shutdown());
    assert!(poll!(&mut shutdown).is_pending());
    release.send(()).unwrap();
    let receipt = timeout(WAIT, shutdown).await.unwrap().unwrap();
    assert!(receipt.joined && receipt.workers_joined);
    assert_eq!(fixture.count("GetRecordStatus"), 2);
    assert!(service.take_store().is_some());
    fixture.joined().await;
}

#[tokio::test]
async fn observe_reads_fresh_shared_control_twice_and_refuses_changed_local_authority() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (session, events) = ObsSession::connect(
        &fixture.url,
        None,
        SessionOptions {
            event_subscriptions: REQUIRED_SUBSCRIPTIONS,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bridge = ObservationBridge::bind(session.handle(), events, None).unwrap();
    let control = Arc::new(Mutex::new(ControlDomain::default()));
    control
        .lock()
        .unwrap()
        .claim("codex", 1, "before", 1_793_448_000_000)
        .unwrap();
    let resources = Resources {
        session,
        bridge,
        store: OperationStore::new(&dir.0, &fixture.url).unwrap(),
        control: control.clone(),
        clock: clock(),
        identities: Box::new(|| id(1)),
    };
    let mut service = Service::spawn(
        resources,
        Options {
            connection: Some(ConnectionDescriptor {
                url: fixture.url.clone(),
                has_password: false,
            }),
            state_root: Some(dir.0.clone()),
            ..Default::default()
        },
    )
    .unwrap_or_else(|_| panic!("binding failed"));
    let (seen, release) = fixture.hold("GetVideoSettings");
    let handle = service.handle();
    let observing = tokio::spawn(async move { handle.observe(None).await });
    timeout(WAIT, seen).await.unwrap().unwrap();
    control.lock().unwrap().release("codex", 1_793_448_000_000);
    control
        .lock()
        .unwrap()
        .claim("other", 1, "after", 1_793_448_000_000)
        .unwrap();
    release.send(()).unwrap();
    let report = timeout(WAIT, observing).await.unwrap().unwrap().unwrap();
    assert_eq!(report["status"], "unknown", "{report}");
    assert_eq!(report["reason"], "observation_invalidated");
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn production_observe_preserves_owned_obs_rejection_comments_in_authorized_report() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    fixture.rejections.lock().unwrap().insert(
        "GetVirtualCamStatus".into(),
        (604, "fixture comment retained in result".into()),
    );
    let (mut service, _) = fixture.owners(&dir, None).await;
    let report = service.handle().observe(None).await.unwrap();
    assert_eq!(report["status"], "incomplete");
    assert_eq!(report["outputs"]["virtualCamera"], Value::Null);
    let missing = report["missingEvidence"].as_array().unwrap();
    assert_eq!(missing.len(), 1);
    assert_eq!(
        missing[0],
        json!({"request":"GetVirtualCamStatus","reason":"capability_unavailable","message":"OBS request GetVirtualCamStatus failed with code 604: fixture comment retained in result","nextCheck":"Run obs-observe and inspect the named target before retrying"})
    );
    assert_eq!(fixture.count("GetVirtualCamStatus"), 2);
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn observe_detects_real_receipt_change_between_its_local_snapshots_without_second_writer() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    {
        let mut seed = OperationStore::new(&dir.0, &fixture.url).unwrap();
        seed.prepare(
            &id(800),
            OperationKind::RecordStart,
            1,
            "2026-10-31T12:00:00.000Z",
            Default::default(),
        )
        .unwrap();
    }
    let (mut service, _) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("GetVideoSettings");
    let observing = tokio::spawn({
        let handle = handle.clone();
        async move { handle.observe(None).await }
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    // Adversarial external file change, not a second service writer or mutation.
    let path = dir
        .0
        .join("operation-receipts")
        .join(format!("{}.json", id(800)));
    let mut receipt: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    receipt["phase"] = json!("unknown");
    receipt["updatedAt"] = json!("2026-10-31T12:00:01.000Z");
    receipt["requestDispatched"] = Value::Null;
    fs::write(path, serde_json::to_vec(&receipt).unwrap()).unwrap();
    release.send(()).unwrap();
    let report = timeout(WAIT, observing).await.unwrap().unwrap().unwrap();
    assert_eq!(report["status"], "unknown", "{report}");
    assert_eq!(report["reason"], "observation_invalidated");
    assert_eq!(
        report["operationReceipts"]["operations"][0]["phase"],
        "unknown"
    );
    assert_eq!(
        handle
            .stage(target(), caller(), Cancellation::new())
            .await
            .unwrap_err(),
        ServiceError::Uncertain
    );
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn observe_inspects_actual_recovery_files_and_never_clears_an_invalid_inventory() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, None).await;
    let path = dir.0.join("demo-recovery");
    fs::create_dir(&path).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(path.join(format!("{}.json", id(801))), b"{}").unwrap();
    let report = service.handle().observe(None).await.unwrap();
    assert_eq!(report["status"], "incomplete", "{report}");
    assert_eq!(
        report["recovery"],
        json!({"status":"unknown","reason":"Recovery journals could not be inspected; inspect obs-recover-demo"})
    );
    assert_eq!(report["missingEvidence"][0]["component"], "recovery");
    assert_eq!(
        service.handle().handoff().await.unwrap_err(),
        ServiceError::RetainedState
    );
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn observe_duration_includes_capability_admission_but_sample_time_starts_after_it() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (session, events) = ObsSession::connect(
        &fixture.url,
        None,
        SessionOptions {
            event_subscriptions: REQUIRED_SUBSCRIPTIONS,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bridge = ObservationBridge::bind(session.handle(), events, None).unwrap();
    let wall = Arc::new(AtomicU64::new(1_793_448_000_000));
    let time = wall.clone();
    let clock: Clock = Arc::new(move || {
        clock::from_epoch_duration(Duration::from_millis(time.load(Ordering::Acquire))).unwrap()
    });
    let resources = Resources {
        session,
        bridge,
        store: OperationStore::new(&dir.0, &fixture.url).unwrap(),
        control: Arc::new(Mutex::new(ControlDomain::default())),
        clock,
        identities: Box::new(|| id(1)),
    };
    let mut service = Service::spawn(
        resources,
        Options {
            connection: Some(ConnectionDescriptor {
                url: fixture.url.clone(),
                has_password: false,
            }),
            ..Default::default()
        },
    )
    .unwrap_or_else(|_| panic!("binding failed"));
    let (seen, release) = fixture.hold("GetVersion");
    let handle = service.handle();
    let observing = tokio::spawn(async move { handle.observe(None).await });
    timeout(WAIT, seen).await.unwrap().unwrap();
    wall.store(1_793_448_000_250, Ordering::Release);
    release.send(()).unwrap();
    let report = timeout(WAIT, observing).await.unwrap().unwrap().unwrap();
    assert_eq!(report["status"], "observed", "{report}");
    assert_eq!(report["durationMs"], 250);
    assert_eq!(report["observedAt"], "2026-10-31T12:00:00.250Z");
    assert_eq!(report["completedAt"], "2026-10-31T12:00:00.250Z");
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn a_live_control_lease_refuses_handoff_without_discarding_authority() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (session, events) = ObsSession::connect(
        &fixture.url,
        None,
        SessionOptions {
            event_subscriptions: REQUIRED_SUBSCRIPTIONS,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bridge = ObservationBridge::bind(session.handle(), events, None).unwrap();
    let control = Arc::new(Mutex::new(ControlDomain::default()));
    control
        .lock()
        .unwrap()
        .claim("codex", 1, "persistent authority", 1_793_448_000_000)
        .unwrap();
    let resources = Resources {
        session,
        bridge,
        store: OperationStore::new(&dir.0, &fixture.url).unwrap(),
        control: control.clone(),
        clock: clock(),
        identities: Box::new(|| id(1)),
    };
    let mut service = Service::spawn(
        resources,
        Options {
            connection: Some(ConnectionDescriptor {
                url: fixture.url.clone(),
                has_password: false,
            }),
            ..Default::default()
        },
    )
    .unwrap_or_else(|_| panic!("binding failed"));
    let handle = service.handle();
    assert_eq!(
        handle.handoff().await.unwrap_err(),
        ServiceError::RetainedState
    );
    assert_eq!(
        handle.observe(None).await.unwrap()["control"]["holder"],
        "codex"
    );
    control.lock().unwrap().release("codex", 1_793_448_000_000);
    handle.handoff().await.unwrap();
    assert!(service.shutdown().await.unwrap().handoff_allowed);
    fixture.joined().await;
}

#[tokio::test]
async fn retained_closed_read_survives_frontend_drop_and_joins_before_socket_shutdown() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("GetRecordStatus");
    let frontend = tokio::spawn(async move {
        handle
            .read(
                ReadRequest::RecordStatus,
                Value::Null,
                Duration::from_secs(3),
            )
            .await
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    frontend.abort();
    let mut shutdown = Box::pin(service.shutdown());
    assert!(poll!(&mut shutdown).is_pending());
    release.send(()).unwrap();
    assert!(timeout(WAIT, shutdown).await.unwrap().unwrap().joined);
    assert_eq!(fixture.count("GetRecordStatus"), 1);
    fixture.joined().await;
}

#[tokio::test]
async fn all_four_legacy_tracked_kinds_keep_their_critical_receipt_semantics() {
    for (kind, name) in [
        (OperationKind::StageScene, "SetCurrentPreviewScene"),
        (OperationKind::TakeScene, "TriggerStudioModeTransition"),
        (OperationKind::RecordStart, "StartRecord"),
        (OperationKind::RecordStop, "StopRecord"),
    ] {
        let dir = Dir::new();
        let fixture = Fixture::new().await;
        let (mut service, token) = fixture.owners(&dir, None).await;
        let handle = service.handle();
        let mut request = tracked(token, 80);
        request.kind = kind;
        request.request_type = name.into();
        let result = handle.tracked(request).await.unwrap();
        assert_eq!(result["status"], "accepted");
        assert_eq!(result["receipt"]["postconditionObserved"], false);
        assert_eq!(fixture.count(name), 1);
        assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Uncertain);
        assert!(service.shutdown().await.unwrap().joined);
        fixture.joined().await;
    }
}
#[tokio::test]
async fn active_take_policy_keeps_profile_lock_but_allows_closed_edits_and_stage() {
    use obs_mcp_native::control::{Disposition, GateRequest, RefusalKind};
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let host = Arc::new(Host::default());
    let (mut service, _) = fixture.owners(&dir, Some(host)).await;
    let handle = service.handle();
    assert_ne!(
        handle
            .start_take(
                StartOptions::default(),
                Policy {
                    caller: "codex".into(),
                    ..Default::default()
                }
            )
            .await
            .unwrap()["isError"],
        true
    );
    for tool in ["obs-set-input-mute", "obs-stage-scene"] {
        assert_eq!(
            fixture.control.lock().unwrap().evaluate(
                GateRequest {
                    tool_name: tool,
                    arguments: &json!({}),
                    read_only_hint: false,
                    caller: "codex",
                    confirmation: Default::default()
                },
                clock()().epoch_ms
            ),
            Disposition::Allow
        );
    }
    assert!(matches!(
        fixture.control.lock().unwrap().evaluate(
            GateRequest {
                tool_name: "obs-set-profile-parameter",
                arguments: &json!({}),
                read_only_hint: false,
                caller: "codex",
                confirmation: Default::default()
            },
            clock()().epoch_ms
        ),
        Disposition::Refused {
            kind: RefusalKind::TakeLock,
            ..
        }
    ));
    handle.stop_take(None, Policy::default()).await.unwrap();
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}
#[tokio::test]
async fn frontend_drop_and_shutdown_retain_same_accepted_generic_job_until_reply() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("SetInputMute");
    let caller = handle.clone();
    let frontend = tokio::spawn(async move {
        caller
            .generic(
                obs_mcp_native::generic_requests::Call::parse(
                    "obs-set-input-mute",
                    &json!({"inputName":"fixture","inputMuted":true}),
                )
                .unwrap(),
                "codex".into(),
                Cancellation::new(),
            )
            .await
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    frontend.abort();
    assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Busy);
    let mut shutdown = Box::pin(service.shutdown());
    assert!(poll!(&mut shutdown).is_pending());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(poll!(&mut shutdown).is_pending());
    release.send(()).unwrap();
    assert!(timeout(WAIT, shutdown).await.unwrap().unwrap().joined);
    let store = service.take_store().unwrap();
    let receipt = store.read_view().read_any(&id(1)).unwrap();
    assert_eq!(receipt["phase"], "accepted");
    assert_eq!(receipt["postconditionObserved"], false);
    assert_eq!(fixture.count("SetInputMute"), 1);
    fixture.joined().await;
}
#[tokio::test]
async fn configured_short_deadline_keeps_unknown_and_never_releases_a_replay_permission() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    // This is a positive-wire deadline test: allow receipt persistence and
    // dispatch to finish under load before withholding the actual OBS reply.
    // Two seconds remains below Source's 10s and Service's default 30s.
    let operation_deadline = Duration::from_secs(2);
    let (mut service, _) = fixture.owners_timeout(&dir, None, operation_deadline).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("SetInputMute");
    let caller = handle.clone();
    let admitted_at = tokio::time::Instant::now();
    let mut frontend = tokio::spawn(async move {
        caller
            .generic(
                obs_mcp_native::generic_requests::Call::parse(
                    "obs-set-input-mute",
                    &json!({"inputName":"fixture","inputMuted":true}),
                )
                .unwrap(),
                "codex".into(),
                Cancellation::new(),
            )
            .await
    });
    let observed = timeout(WAIT, seen).await;
    if !matches!(observed, Ok(Ok(()))) {
        let outcome = timeout(WAIT, &mut frontend).await;
        let summary = timeout(WAIT, handle.summary()).await;
        let wire_count = fixture.count("SetInputMute");
        let _ = release.send(());
        let cleanup = timeout(WAIT, service.shutdown()).await;
        let receipt = service
            .take_store()
            .map(|store| store.read_view().read_any(&id(1)));
        fixture.joined().await;
        panic!(
            "SetInputMute never reached its held wire gate: gate={observed:?}, frontend={outcome:?}, receipt={receipt:?}, summary={summary:?}, wire_count={wire_count}, cleanup={cleanup:?}"
        );
    }
    assert_eq!(fixture.count("SetInputMute"), 1);
    frontend.abort();
    let _ = frontend.await;
    idle(&handle).await;
    assert!(admitted_at.elapsed() >= operation_deadline);
    assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Uncertain);
    let next = handle
        .generic(
            obs_mcp_native::generic_requests::Call::parse(
                "obs-set-input-mute",
                &json!({"inputName":"fixture","inputMuted":false}),
            )
            .unwrap(),
            "codex".into(),
            Cancellation::new(),
        )
        .await;
    assert!(matches!(next, Err(ServiceError::Uncertain)));
    assert_eq!(fixture.count("SetInputMute"), 1);
    release.send(()).unwrap();
    assert!(
        timeout(WAIT, service.shutdown())
            .await
            .unwrap()
            .unwrap()
            .joined
    );
    let value = service
        .take_store()
        .unwrap()
        .read_view()
        .read_any(&id(1))
        .unwrap();
    assert_eq!(value["phase"], "unknown");
    assert_eq!(value["requestAccepted"], Value::Null);
    assert_eq!(value["postconditionObserved"], false);
    fixture.joined().await;
}

#[tokio::test]
async fn generic_fifo_bounds_retained_argument_bytes_and_cancels_without_new_intent() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("SetInputMute");
    let first_handle = handle.clone();
    let first = tokio::spawn(async move {
        first_handle
            .generic(
                obs_mcp_native::generic_requests::Call::parse(
                    "obs-set-input-mute",
                    &json!({"inputName":"first","inputMuted":true}),
                )
                .unwrap(),
                "codex".into(),
                Cancellation::new(),
            )
            .await
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    let mut pending = vec![];
    let mut cancellations = vec![];
    for _ in 0..2 {
        let cancel = Cancellation::new();
        cancellations.push(cancel.clone());
        let caller = handle.clone();
        let call = obs_mcp_native::generic_requests::Call::parse(
            "obs-set-input-mute",
            &json!({"inputName":"x".repeat(7*1024*1024),"inputMuted":true}),
        )
        .unwrap();
        pending.push(tokio::spawn(async move {
            caller.generic(call, "codex".into(), cancel).await
        }));
    }
    timeout(WAIT, async {
        loop {
            let summary = handle.summary().await.unwrap();
            if summary["queuedCommands"] == 2 {
                assert!(summary["queuedArgumentBytes"].as_u64().unwrap() > 14 * 1024 * 1024);
                assert!(summary["queuedArgumentBytes"].as_u64().unwrap() <= 16 * 1024 * 1024);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let rejected = handle
        .generic(
            obs_mcp_native::generic_requests::Call::parse(
                "obs-set-input-mute",
                &json!({"inputName":"z".repeat(7*1024*1024),"inputMuted":false}),
            )
            .unwrap(),
            "codex".into(),
            Cancellation::new(),
        )
        .await;
    assert!(matches!(rejected, Err(ServiceError::Capacity)));
    for cancel in cancellations {
        cancel.cancel();
    }
    release.send(()).unwrap();
    assert_ne!(first.await.unwrap().unwrap()["isError"], true);
    for job in pending {
        assert_eq!(job.await.unwrap().unwrap()["isError"], true);
    }
    idle(&handle).await;
    let summary = handle.summary().await.unwrap();
    assert_eq!(summary["total"], 1);
    assert_eq!(summary["queuedArgumentBytes"], 0);
    assert_eq!(fixture.count("SetInputMute"), 1);
    assert_eq!(
        handle.handoff().await.unwrap_err(),
        ServiceError::RetainedState
    );
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn queued_generic_keeps_original_deadline_through_take_preflight_and_creates_no_intent() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let host = Arc::new(Host::default());
    let (seen, release) = host.hold(false);
    let (mut service, _) = fixture
        .owners_timeout(&dir, Some(host), Duration::from_millis(150))
        .await;
    let handle = service.handle();
    let start_handle = handle.clone();
    let start = tokio::spawn(async move {
        start_handle
            .start_take(
                StartOptions::default(),
                Policy {
                    caller: "codex".into(),
                    ..Default::default()
                },
            )
            .await
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    let queued_handle = handle.clone();
    let queued = tokio::spawn(async move {
        queued_handle
            .generic(
                obs_mcp_native::generic_requests::Call::parse(
                    "obs-set-input-mute",
                    &json!({"inputName":"fixture","inputMuted":true}),
                )
                .unwrap(),
                "codex".into(),
                Cancellation::new(),
            )
            .await
    });
    timeout(WAIT, async {
        while handle.summary().await.unwrap()["queuedCommands"] != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(180)).await;
    release.send(()).unwrap();
    assert_ne!(start.await.unwrap().unwrap()["isError"], true);
    let refused = queued.await.unwrap().unwrap();
    assert_eq!(refused["isError"], true);
    assert!(refused.to_string().contains("expired before dispatch"));
    assert_eq!(fixture.count("SetInputMute"), 0);
    let summary = handle.summary().await.unwrap();
    assert!(
        summary["operations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|receipt| receipt["version"] == 1)
    );
    handle.stop_take(None, Policy::default()).await.unwrap();
    assert_eq!(fixture.count("StopRecord"), 1);
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}

#[tokio::test]
async fn snapshot_frontend_drop_retains_the_single_file_owner_until_batch_and_publication_join() {
    use obs_mcp_native::snapshot_tools::Request;
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("RequestBatch");
    let frontend = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .snapshot(
                    Request::parse(&json!({"label":"retained"})).unwrap(),
                    Cancellation::new(),
                )
                .await
        }
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    frontend.abort();
    let concurrent = handle
        .snapshot(
            Request::parse(&json!({"list":true})).unwrap(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(concurrent["isError"], true);
    assert!(!dir.0.join("snapshots.json").exists());
    release.send(()).unwrap();
    idle(&handle).await;
    let saved: Value =
        serde_json::from_slice(&fs::read(dir.0.join("snapshots.json")).unwrap()).unwrap();
    assert_eq!(saved[0]["label"], "retained");
    assert_eq!(saved.as_array().unwrap().len(), 1);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(dir.0.join("snapshots.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let list = handle
        .snapshot(
            Request::parse(&json!({"list":true})).unwrap(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        list["structuredContent"]["snapshots"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(fixture.count("GetCurrentProgramScene"), 1);
    assert!(service.shutdown().await.unwrap().workers_joined);
    fixture.joined().await;
}

#[tokio::test]
async fn cancelled_snapshot_joins_same_batch_before_shutdown_and_never_publishes() {
    use obs_mcp_native::snapshot_tools::Request;
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("RequestBatch");
    let cancel = Cancellation::new();
    let frontend = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            handle
                .snapshot(Request::parse(&json!({})).unwrap(), cancel)
                .await
        }
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    cancel.cancel();
    let mut shutdown = Box::pin(service.shutdown());
    assert!(poll!(&mut shutdown).is_pending());
    release.send(()).unwrap();
    assert!(
        timeout(WAIT, shutdown)
            .await
            .unwrap()
            .unwrap()
            .workers_joined
    );
    let result = frontend.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(!dir.0.join("snapshots.json").exists());
    fixture.joined().await;
}

#[tokio::test]
async fn snapshot_namespace_replacement_refuses_publication_into_either_directory() {
    use obs_mcp_native::snapshot_tools::Request;
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    let (seen, release) = fixture.hold("RequestBatch");
    let frontend = tokio::spawn(async move {
        handle
            .snapshot(Request::parse(&json!({})).unwrap(), Cancellation::new())
            .await
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    let retired = dir.0.with_extension("replaced");
    fs::rename(&dir.0, &retired).unwrap();
    fs::create_dir(&dir.0).unwrap();
    release.send(()).unwrap();
    let result = frontend.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(!dir.0.join("snapshots.json").exists());
    assert!(!retired.join("snapshots.json").exists());
    assert!(service.shutdown().await.unwrap().workers_joined);
    fixture.joined().await;
    fs::remove_dir_all(retired).unwrap();
}

#[tokio::test]
async fn active_take_stop_progresses_while_snapshot_publication_retains_its_independent_file_owner()
{
    use obs_mcp_native::snapshot_tools::Request;
    use std::io::Write;
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, _) = fixture.owners(&dir, Some(Arc::new(Host::default()))).await;
    let handle = service.handle();
    assert_ne!(
        handle
            .start_take(StartOptions::default(), Policy::default())
            .await
            .unwrap()["isError"],
        true
    );
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock = dir.0.join("snapshots.json.lock");
    options
        .open(&lock)
        .unwrap()
        .write_all(b"unproven fixture owner")
        .unwrap();
    let frontend = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .snapshot(
                    Request::parse(&json!({"scenes":[],"inputs":[],"label":"during-recording"}))
                        .unwrap(),
                    Cancellation::new(),
                )
                .await
        }
    });
    timeout(WAIT, async {
        while handle.summary().await.unwrap()["jobsPending"] != true {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    frontend.abort();
    let stopped = timeout(
        Duration::from_secs(1),
        handle.stop_take(None, Policy::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(stopped["structuredContent"]["stopConfirmed"], true);
    assert_eq!(fixture.count("StopRecord"), 1);
    assert_eq!(fs::read(&lock).unwrap(), b"unproven fixture owner");
    fs::remove_file(lock).unwrap();
    idle(&handle).await;
    let saved: Value =
        serde_json::from_slice(&fs::read(dir.0.join("snapshots.json")).unwrap()).unwrap();
    assert_eq!(saved[0]["label"], "during-recording");
    assert_eq!(fixture.count("StartRecord"), 1);
    assert_eq!(fixture.count("StopRecord"), 1);
    assert!(service.shutdown().await.unwrap().workers_joined);
    fixture.joined().await;
}

#[tokio::test]
async fn manual_snapshot_metadata_phase_matches_advancing_clock_full_source_callback() {
    use obs_mcp_native::snapshot_tools::Request;
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let dir = Dir::new();
    let source_dir = Dir::new();
    let fixture = Fixture::new().await;
    fixture.advance_clock.store(true, Ordering::Release);
    let epoch = fixture.read_epoch.clone();
    let controlled: Clock = Arc::new(move || {
        clock::from_epoch_duration(Duration::from_millis(epoch.load(Ordering::Acquire))).unwrap()
    });
    let (mut service, _) = fixture.owners_clock(&dir, None, WAIT, controlled).await;
    let actual = service
        .handle()
        .snapshot(Request::parse(&json!({})).unwrap(), Cancellation::new())
        .await
        .unwrap();
    let script = r#"import crypto from'node:crypto';import{EventEmitter}from'node:events';import{readFileSync}from'node:fs';import{join}from'node:path';import{McpServer,InMemoryTransport}from'@modelcontextprotocol/server';import{Client}from'@modelcontextprotocol/client';import{initialize}from'./build/tools/index.js';const chunks=[];for await(const b of process.stdin)chunks.push(b);const input=JSON.parse(Buffer.concat(chunks));let epoch=1793448000000;const RealDate=Date;globalThis.Date=class extends RealDate{constructor(...args){super(...(args.length?args:[epoch]))}static now(){return epoch}};crypto.randomUUID=()=> '00000000-0000-4000-8000-000000000001';const trace=[];function answer(name){switch(name){case'GetCurrentProgramScene':return{currentProgramSceneName:'Main',currentProgramSceneUuid:'main'};case'GetInputList':return{inputs:[{inputName:'Window',inputKind:'screen_capture'}]};case'GetSceneItemList':return{sceneItems:[{sceneItemId:7,sceneItemIndex:0,sourceName:'Window',sceneItemEnabled:true,sceneItemLocked:false,sceneItemTransform:{positionX:17}}]};case'GetInputSettings':return{inputKind:'screen_capture',inputSettings:{window:77,opaque:{retained:true}}};case'GetInputMute':return{inputMuted:true};case'GetInputVolume':return{inputVolumeMul:0.5};case'GetInputAudioTracks':return{inputAudioTracks:{1:true}};default:throw Error(name)}}class Fake extends EventEmitter{isConnected(){return true}getConnectionStatus(){return{url:input.endpoint,identified:true,hasPassword:false}}supportsRequest(){return true}async sendRequest(name,data){trace.push({op:6,name,data:data??null});const value=answer(name);epoch+=1000;return value}async sendBatch(requests){if(!requests.length)return[];trace.push({op:8,requests});const rows=requests.map(call=>({requestType:call.requestType,ok:true,code:100,responseData:answer(call.requestType)}));epoch+=1000;return rows}}const server=new McpServer({name:'snapshot-phase',version:'1'});initialize(server,new Fake(),{resourcesAndPrompts:false,sessionNotifications:false,confirmLive:false});const client=new Client({name:'phase',version:'1'},{versionNegotiation:{mode:'legacy'}}),[a,b]=InMemoryTransport.createLinkedPair();try{await Promise.all([server.connect(a),client.connect(b)]);const result=await client.callTool({name:'obs-snapshot',arguments:{}});console.log(JSON.stringify({result,history:JSON.parse(readFileSync(join(process.env.OBS_MCP_STATE_DIR,'snapshots.json'),'utf8')),trace,epoch}));}finally{await client.close();await server.close()}"#;
    let mut child = Command::new("node")
        .args(["--input-type=module", "-e", script])
        .current_dir(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap(),
        )
        .env("OBS_MCP_STATE_DIR", &source_dir.0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&json!({"endpoint":fixture.url})).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual, expected["result"]);
    assert_eq!(
        actual["structuredContent"]["takenAt"],
        "2026-10-31T12:00:03.000Z"
    );
    assert_eq!(
        fixture.read_epoch.load(Ordering::Acquire),
        1_793_448_004_000
    );
    assert_eq!(expected["epoch"], json!(1_793_448_004_000u64));
    let history: Value =
        serde_json::from_slice(&fs::read(dir.0.join("snapshots.json")).unwrap()).unwrap();
    // JSON has one number type; compare all fields with the existing JS-number
    // canonicalizer so serialized f64 7.0 and Source 7 remain the same value.
    assert_eq!(
        obs_mcp_native::observation::signature(&history),
        obs_mcp_native::observation::signature(&expected["history"])
    );
    let trace=fixture.trace.lock().unwrap().iter().map(|frame|if frame["op"]==8{json!({"op":8,"requests":frame["d"]["requests"].as_array().unwrap().iter().map(|call|json!({"requestType":call["requestType"],"requestData":call["requestData"]})).collect::<Vec<_>>()})}else{json!({"op":6,"name":frame["d"]["requestType"],"data":frame["d"].get("requestData").cloned().unwrap_or(Value::Null)})}).collect::<Vec<_>>();
    assert_eq!(trace, *expected["trace"].as_array().unwrap());
    assert!(service.shutdown().await.unwrap().workers_joined);
    fixture.joined().await;
}

fn profile_parameter(category: &str) -> obs_mcp_native::generic_requests::Call {
    obs_mcp_native::generic_requests::Call::parse(
        "obs-set-profile-parameter",
        &json!({"parameterCategory":category,"parameterName":"Mode","parameterValue":"Advanced"}),
    )
    .unwrap()
}
#[tokio::test]
async fn physical_pending_flag_exists_without_host_and_survives_profile_change_until_new_owner() {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let (mut service, token) = fixture.owners(&dir, None).await;
    let handle = service.handle();
    assert_eq!(
        handle.summary().await.unwrap()["outputSettingsPending"],
        false
    );
    handle
        .read(ReadRequest::Version, Value::Null, WAIT)
        .await
        .unwrap();
    for category in ["General", "output"] {
        let result = handle
            .generic(
                profile_parameter(category),
                "codex".into(),
                Cancellation::new(),
            )
            .await
            .unwrap();
        assert_ne!(result["isError"], true);
        assert_eq!(
            handle.summary().await.unwrap()["outputSettingsPending"],
            false
        );
    }
    let result = handle
        .generic(
            profile_parameter("Output"),
            "codex".into(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_ne!(result["isError"], true);
    assert_eq!(
        handle.summary().await.unwrap()["outputSettingsPending"],
        true
    );
    let changed = handle
        .generic(
            obs_mcp_native::generic_requests::Call::parse(
                "obs-set-current-profile",
                &json!({"profileName":"Another"}),
            )
            .unwrap(),
            "codex".into(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_ne!(changed["isError"], true);
    assert_eq!(
        handle.summary().await.unwrap()["outputSettingsPending"],
        true
    );
    let independent_dir = Dir::new();
    let independent = Fixture::new().await;
    let (mut next, next_token) = independent.owners(&independent_dir, None).await;
    assert_ne!(token, next_token);
    assert_eq!(
        next.handle().summary().await.unwrap()["outputSettingsPending"],
        false
    );
    assert!(next.shutdown().await.unwrap().joined);
    independent.joined().await;
    assert!(service.shutdown().await.unwrap().joined);
    fixture.joined().await;
}
#[tokio::test]
async fn acknowledged_profile_change_marks_flag_before_failed_receipt_publication_and_cannot_clear_custody()
 {
    let dir = Dir::new();
    let fixture = Fixture::new().await;
    let pending = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(AtomicBool::new(false));
    let saw_pending = Arc::new(AtomicBool::new(false));
    let checked_clock: Clock = {
        let pending = pending.clone();
        let released = released.clone();
        let observed = observed.clone();
        let saw = saw_pending.clone();
        Arc::new(move || {
            if released.load(Ordering::Acquire) {
                observed.store(true, Ordering::Release);
                saw.store(pending.load(Ordering::Acquire), Ordering::Release);
            }
            clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap()
        })
    };
    let (mut service, _) = fixture
        .owners_clock_pending(
            &dir,
            Some(Arc::new(Host::default())),
            Duration::from_secs(30),
            checked_clock,
            Some(pending.clone()),
        )
        .await;
    let handle = service.handle();
    handle
        .read(ReadRequest::Version, Value::Null, WAIT)
        .await
        .unwrap();
    let (seen, reply) = fixture.hold("SetProfileParameter");
    let call = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .generic(
                    profile_parameter("AdvOut"),
                    "codex".into(),
                    Cancellation::new(),
                )
                .await
        }
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    assert!(!pending.load(Ordering::Acquire));
    assert_eq!(
        handle.summary().await.unwrap()["outputSettingsPending"],
        false
    );
    let history = dir.0.join("operation-receipts");
    let saved = dir.0.join("saved-owned-operation-receipts");
    fs::rename(&history, &saved).unwrap();
    fs::create_dir(&history).unwrap();
    let human = history.join("human.txt");
    fs::write(&human, b"preserve replacement").unwrap();
    released.store(true, Ordering::Release);
    reply.send(()).unwrap();
    let result = timeout(WAIT, call).await.unwrap().unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(
        observed.load(Ordering::Acquire),
        "outcome timestamp reached before publication"
    );
    assert!(
        saw_pending.load(Ordering::Acquire),
        "known ACK must set readiness BEFORE the outcome timestamp/publication attempt"
    );
    assert!(pending.load(Ordering::Acquire));
    let summary = handle.summary().await.unwrap();
    assert_eq!(summary["outputSettingsPending"], true);
    assert_eq!(summary["status"], "unknown");
    let next = handle
        .generic(
            obs_mcp_native::generic_requests::Call::parse(
                "obs-set-persistent-data",
                &json!({"realm":"R","slotName":"must refuse","slotValue":true}),
            )
            .unwrap(),
            "codex".into(),
            Cancellation::new(),
        )
        .await;
    assert!(matches!(next, Err(ServiceError::Uncertain)));
    assert_eq!(fixture.count("SetPersistentData"), 0);
    assert_eq!(fixture.count("SetProfileParameter"), 1);
    assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Uncertain);
    let durable: Value =
        serde_json::from_slice(&fs::read(saved.join(format!("{}.json", id(1)))).unwrap()).unwrap();
    assert_eq!(durable["phase"], "dispatching");
    assert_eq!(durable["requestAccepted"], Value::Null);
    assert_eq!(durable["postconditionObserved"], false);
    assert_eq!(fs::read(&human).unwrap(), b"preserve replacement");
    assert!(service.shutdown().await.unwrap().joined);
    assert!(
        service.take_store().is_none(),
        "poison must not be erased by reclaim"
    );
    assert_eq!(fs::read(&human).unwrap(), b"preserve replacement");
    fixture.joined().await;
}
