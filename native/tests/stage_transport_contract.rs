// SPDX-License-Identifier: GPL-2.0-only
//! Real loopback transport plus real receipt files. No live OBS or MCP registration.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    clock::from_epoch_duration,
    control::{CallerContext, ControlDomain, Disposition, RefusalKind},
    custody::{Clock, SharedControl},
    observation::{Observation, ReadAdapter, ReadRequest, Target},
    operation_store::{OperationStore, Phase},
    session::{ALL_EVENTS, ObsSession, SessionHandle, SessionOptions},
    session_observation::ObservationBridge,
    stage::{Context, StageMachine, StageReader},
    stage_transport::{
        BoundCall, CallerBinding, Cancellation, REQUIRED_STAGE_EVENTS, StageTransport,
        TransportContext,
    },
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

const EPOCH: u64 = 1_793_448_000_000;
const WAIT: Duration = Duration::from_secs(5);
type Peer = WebSocketStream<TcpStream>;

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .unwrap();
        let path = std::env::temp_dir().canonicalize().unwrap().join(format!(
            "obs-stage-transport-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn root(&self) -> PathBuf {
        self.0.join("state")
    }
    fn path(&self, id: &str) -> PathBuf {
        self.root()
            .join("operation-receipts")
            .join(format!("{id}.json"))
    }
    fn store(&self, url: &str) -> OperationStore {
        OperationStore::new(self.root(), url).unwrap()
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn id(number: usize) -> String {
    format!("00000000-0000-4000-8000-{number:012x}")
}
fn target() -> Target {
    Target {
        scene_collection: "Fixture".into(),
        scene_name: "Review".into(),
        canvas_uuid: None,
    }
}
fn clock() -> Clock {
    Arc::new(|| from_epoch_duration(Duration::from_millis(EPOCH)).unwrap())
}
fn control() -> SharedControl {
    Arc::new(Mutex::new(ControlDomain::default()))
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
fn bind<'a>(
    handle: &SessionHandle,
    bridge: &'a ObservationBridge,
    control: SharedControl,
    call: BoundCall,
    cancellation: Cancellation,
) -> StageTransport<'a> {
    StageTransport::bind(
        TransportContext {
            handle: handle.clone(),
            bridge,
            control,
            clock: clock(),
            cancellation,
        },
        call,
        caller(),
    )
    .unwrap()
}
fn context<'a, 'b>(
    transport: &'a mut StageTransport<'b>,
    store: &'a mut OperationStore,
) -> Context<
    'a,
    obs_mcp_native::stage_transport::TransportReader<'b>,
    obs_mcp_native::stage_transport::TransportMutator,
> {
    Context {
        reader: &mut transport.reader,
        mutator: &mut transport.mutator,
        store,
        clock: clock(),
        deadline: Instant::now() + Duration::from_secs(15),
        screenshot_max_bytes: 4 * 1024 * 1024,
    }
}
fn ok(
    result: Result<obs_mcp_native::stage::Success, obs_mcp_native::stage::Failure>,
) -> obs_mcp_native::stage::Success {
    result.unwrap_or_else(|failure| panic!("{}", failure.report))
}
fn failure(
    result: Result<obs_mcp_native::stage::Success, obs_mcp_native::stage::Failure>,
) -> obs_mcp_native::stage::Failure {
    match result {
        Ok(value) => panic!("unexpected success {}", value.report),
        Err(error) => error,
    }
}

#[derive(Debug)]
enum Action {
    Continue,
    Event(&'static str),
    TransformChanged,
    Disconnect,
}
struct Pause {
    seen: Option<oneshot::Sender<()>>,
    release: oneshot::Receiver<Action>,
}
fn pause() -> (Pause, oneshot::Receiver<()>, oneshot::Sender<Action>) {
    let (seen, receive) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    (
        Pause {
            seen: Some(seen),
            release: resume,
        },
        receive,
        release,
    )
}
#[derive(Default)]
struct Hooks {
    last_studio: Option<Pause>,
    last_studio_at: usize,
    version: Option<Pause>,
    input: Option<Pause>,
    lose_transition: bool,
    event_before_audio_rejection: bool,
}
struct Fixture {
    url: String,
    trace: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
impl Fixture {
    async fn new(hooks: Hooks, connections: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let trace = Arc::new(Mutex::new(vec![]));
        let history = trace.clone();
        let world = Arc::new(Mutex::new(
            json!({"program":"Main","preview":"Main","positionX":0}),
        ));
        let task = tokio::spawn(async move {
            let mut connections_to_join = vec![];
            let mut initial_hooks = Some(hooks);
            for _ in 0..connections {
                let (socket, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
                let hooks = initial_hooks.take().unwrap_or_default();
                let history = history.clone();
                let world = world.clone();
                connections_to_join.push(tokio::spawn(async move {
                    serve(socket, history, world, hooks).await;
                }));
            }
            for task in connections_to_join {
                timeout(WAIT, task).await.unwrap().unwrap();
            }
        });
        Self { url, trace, task }
    }
    fn writes(&self) -> Vec<Value> {
        self.trace
            .lock()
            .unwrap()
            .iter()
            .filter(|request| {
                matches!(
                    request["d"]["requestType"].as_str(),
                    Some("SetCurrentPreviewScene" | "TriggerStudioModeTransition")
                )
            })
            .cloned()
            .collect()
    }
    async fn joined(self) {
        timeout(WAIT, self.task).await.unwrap().unwrap();
    }
}
async fn send(peer: &mut Peer, value: Value) {
    peer.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn event(peer: &mut Peer, name: &str) {
    let intent = match name {
        "SceneItemTransformChanged" => 1 << 19,
        name if name.starts_with("Input") => 8,
        _ => 4,
    };
    send(
        peer,
        json!({"op":5,"d":{"eventType":name,"eventIntent":intent,"eventData":{}}}),
    )
    .await;
}
async fn paused(
    peer: &mut Peer,
    mut pause: Pause,
    world: &Mutex<Value>,
    subscriptions: u64,
) -> bool {
    pause.seen.take().unwrap().send(()).unwrap();
    match pause.release.await.unwrap() {
        Action::Continue => false,
        Action::Event(name) => {
            event(peer, name).await;
            false
        }
        Action::TransformChanged => {
            world.lock().unwrap()["positionX"] = json!(120);
            // Real OBS emits this high-volume event only when bit 19 was
            // advertised in Identify. ALL alone must not mask the regression.
            if subscriptions & (1 << 19) != 0 {
                event(peer, "SceneItemTransformChanged").await;
            }
            false
        }
        Action::Disconnect => true,
    }
}
async fn serve(
    socket: TcpStream,
    history: Arc<Mutex<Vec<Value>>>,
    world: Arc<Mutex<Value>>,
    mut hooks: Hooks,
) {
    let mut peer = accept_async(socket).await.unwrap();
    send(
        &mut peer,
        json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}}),
    )
    .await;
    let identify = timeout(WAIT, peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap();
    let identify: Value = serde_json::from_str(&identify).unwrap();
    assert_eq!(identify["op"], 1);
    let subscriptions = identify["d"]["eventSubscriptions"].as_u64().unwrap();
    send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
    let mut studio = 0;
    while let Some(message) = timeout(WAIT, peer.next()).await.unwrap() {
        let message = message.unwrap();
        match message {
            Message::Close(_) => {
                let _ = peer.flush().await;
                return;
            }
            Message::Ping(_) => {
                peer.flush().await.unwrap();
                continue;
            }
            Message::Text(text) => {
                let request: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(request["op"], 6);
                history.lock().unwrap().push(request.clone());
                let kind = request["d"]["requestType"].as_str().unwrap();
                let pause = match kind {
                    "GetStudioModeEnabled" => {
                        studio += 1;
                        if studio
                            == if hooks.last_studio_at == 0 {
                                2
                            } else {
                                hooks.last_studio_at
                            }
                        {
                            hooks.last_studio.take()
                        } else {
                            None
                        }
                    }
                    "GetVersion" => hooks.version.take(),
                    "GetInputSettings" => hooks.input.take(),
                    _ => None,
                };
                if let Some(pause) = pause
                    && paused(&mut peer, pause, &world, subscriptions).await
                {
                    return;
                }
                if kind == "GetInputMute" && hooks.event_before_audio_rejection {
                    hooks.event_before_audio_rejection = false;
                    event(&mut peer, "InputSettingsChanged").await;
                }
                let (accepted, code, data) = {
                    let mut world = world.lock().unwrap();
                    let data = match kind {
                        "GetStudioModeEnabled" => json!({"studioModeEnabled":true}),
                        "GetSceneCollectionList" => json!({"currentSceneCollectionName":"Fixture"}),
                        "GetSceneList" => {
                            json!({"currentProgramSceneName":world["program"],"currentProgramSceneUuid":if world["program"]=="Review"{"review"}else{"main"},"scenes":[{"sceneName":"Main","sceneUuid":"main"},{"sceneName":"Review","sceneUuid":"review"}]})
                        }
                        "GetCurrentPreviewScene" => {
                            json!({"currentPreviewSceneName":world["preview"],"currentPreviewSceneUuid":if world["preview"]=="Review"{"review"}else{"main"}})
                        }
                        "GetCurrentProgramScene" => {
                            json!({"currentProgramSceneName":world["program"],"currentProgramSceneUuid":if world["program"]=="Review"{"review"}else{"main"}})
                        }
                        "GetVideoSettings" => json!({"baseWidth":1920,"baseHeight":1080}),
                        "GetInputList" => {
                            json!({"inputs":[{"inputName":"Fixture input","inputUuid":"input","inputKind":"color_source_v3"}]})
                        }
                        "GetSceneItemList" => {
                            json!({"sceneItems":[{"sceneItemId":1,"sourceName":"Fixture input","sourceUuid":"input","isGroup":false,"sceneItemEnabled":true}]})
                        }
                        "GetSceneItemTransform" => {
                            json!({"sceneItemTransform":{"positionX":world["positionX"],"positionY":0,"scaleX":1,"scaleY":1}})
                        }
                        "GetInputSettings" => {
                            json!({"inputKind":"color_source_v3","inputSettings":{"色":1}})
                        }
                        "GetSourceFilterList" => json!({"filters":[]}),
                        "GetInputMute" | "GetInputVolume" | "GetInputAudioTracks" => Value::Null,
                        "GetSourceScreenshot" => json!({"imageData":"data:image/png;base64,AAAA"}),
                        "GetVersion" => json!({"obsVersion":"fixture"}),
                        "SetCurrentPreviewScene" => {
                            world["preview"] =
                                json!(if request["d"]["requestData"]["sceneUuid"] == "review" {
                                    "Review"
                                } else {
                                    "Main"
                                });
                            json!({})
                        }
                        "TriggerStudioModeTransition" => {
                            let preview = world["preview"].clone();
                            world["preview"] = world["program"].clone();
                            world["program"] = preview;
                            json!({})
                        }
                        _ => panic!("unexpected fixture request {kind}"),
                    };
                    let accepted = !matches!(
                        kind,
                        "GetInputMute" | "GetInputVolume" | "GetInputAudioTracks"
                    );
                    (accepted, if accepted { 100 } else { 604 }, data)
                };
                if kind == "SetCurrentPreviewScene" {
                    event(&mut peer, "CurrentPreviewSceneChanged").await;
                }
                if kind == "TriggerStudioModeTransition" {
                    event(&mut peer, "CurrentProgramSceneChanged").await;
                    if hooks.lose_transition {
                        return;
                    }
                }
                send(&mut peer,json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":kind,"requestStatus":{"result":accepted,"code":code},"responseData":data}})).await;
            }
            other => panic!("unexpected socket frame {other:?}"),
        }
    }
}
async fn owners(fixture: &Fixture) -> (ObsSession, ObservationBridge) {
    let (owner, events) = ObsSession::connect(&fixture.url, None, stage_options())
        .await
        .unwrap();
    let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
    (owner, bridge)
}
fn stage_options() -> SessionOptions {
    SessionOptions {
        event_subscriptions: REQUIRED_STAGE_EVENTS,
        ..Default::default()
    }
}
async fn close(owner: ObsSession, bridge: ObservationBridge, fixture: Fixture) {
    owner.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    fixture.joined().await;
}

#[tokio::test]
async fn normal_stage_and_take_use_one_physical_session_and_record_independent_scene_observation() {
    let fixture = Fixture::new(Hooks::default(), 1).await;
    let (owner, bridge) = owners(&fixture).await;
    let handle = owner.handle();
    let control = control();
    let scratch = Scratch::new();
    let mut store = scratch.store(&fixture.url);
    let mut machine = StageMachine::default();
    let mut staging = bind(
        &handle,
        &bridge,
        control.clone(),
        BoundCall::stage(target()).unwrap(),
        Cancellation::new(),
    );
    let staged = ok(machine
        .stage(
            &mut context(&mut staging, &mut store),
            target(),
            &id(1),
            &id(2),
        )
        .await);
    assert_eq!(staged.report["status"], "staged");
    assert_eq!(staged.report["renderedComposition"], "unverified");
    assert_eq!(staged.report["generation"], handle.token().generation());
    staging.shutdown().await.unwrap();
    let mut taking = bind(
        &handle,
        &bridge,
        control,
        BoundCall::take(&id(1)).unwrap(),
        Cancellation::new(),
    );
    let taken = ok(machine
        .take(&mut context(&mut taking, &mut store), &id(1), &id(3))
        .await);
    assert_eq!(taken.report["status"], "verified");
    assert_eq!(taken.report["intendedSceneObservedOnProgram"], true);
    assert_eq!(taken.report["renderedComposition"], "unverified");
    assert_eq!(store.read(&id(3)).unwrap().phase(), Phase::Observed);
    assert_eq!(
        fixture
            .writes()
            .iter()
            .map(|frame| frame["d"]["requestType"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["SetCurrentPreviewScene", "TriggerStudioModeTransition"]
    );
    assert!(handle.invalidation().is_none());
    taking.shutdown().await.unwrap();
    close(owner, bridge, fixture).await;
}

#[tokio::test]
async fn queued_mutation_refuses_new_lease_events_subscribed_transform_disconnect_and_cancel() {
    for variant in ["lease", "event", "transform", "disconnect", "cancel"] {
        let (last_studio, studio_seen, release_studio) = pause();
        let (version, version_seen, release_version) = pause();
        let fixture = Fixture::new(
            Hooks {
                last_studio: Some(last_studio),
                version: Some(version),
                ..Default::default()
            },
            1,
        )
        .await;
        let (owner, bridge) = owners(&fixture).await;
        let handle = owner.handle();
        let control = control();
        let scratch = Scratch::new();
        let mut store = scratch.store(&fixture.url);
        let mut machine = StageMachine::default();
        let cancellation = Cancellation::new();
        let mut transport = bind(
            &handle,
            &bridge,
            control.clone(),
            BoundCall::stage(target()).unwrap(),
            cancellation.clone(),
        );
        let mut invocation = context(&mut transport, &mut store);
        let (stage_id, operation_id) = (id(1), id(2));
        let mut job = Box::pin(machine.stage(&mut invocation, target(), &stage_id, &operation_id));
        tokio::select! {value=&mut job=>panic!("workflow ended before queue fixture: {}",match value{Ok(s)=>s.report,Err(f)=>f.report}),_=studio_seen=>{}}
        let blocking_handle = handle.clone();
        let mut blocker = Box::pin(async move {
            blocking_handle
                .request(blocking_handle.token(), "GetVersion", Value::Null, WAIT)
                .await
        });
        assert!(poll!(&mut blocker).is_pending());
        release_studio.send(Action::Continue).unwrap();
        tokio::select! {value=&mut job=>panic!("workflow ended before blocker: {}",match value{Ok(s)=>s.report,Err(f)=>f.report}),_=version_seen=>{}}
        // Finish the ready inspection and synchronously publish intent, then
        // poll through request admission while the socket owner stays blocked.
        assert!(poll!(&mut job).is_pending());
        let queued: Value =
            serde_json::from_slice(&fs::read(scratch.path(&operation_id)).unwrap()).unwrap();
        assert_eq!(queued["phase"], "dispatching");
        assert!(queued["requestDispatched"].is_null());
        if variant == "lease" {
            control
                .lock()
                .unwrap()
                .claim("other", 1, "competing holder", EPOCH)
                .unwrap();
        }
        if variant == "cancel" {
            cancellation.cancel();
        }
        release_version
            .send(match variant {
                "event" => Action::Event("InputSettingsChanged"),
                "transform" => Action::TransformChanged,
                "disconnect" => Action::Disconnect,
                _ => Action::Continue,
            })
            .unwrap();
        // Poll the independent blocker too: its request future owns admission.
        let (result, _) = tokio::join!(job, blocker);
        let failed = failure(result);
        assert_eq!(failed.report["status"], "blocked");
        assert_eq!(failed.report["requestDispatched"], false);
        assert_eq!(
            store.read(&operation_id).unwrap().phase(),
            Phase::NotDispatched
        );
        if variant == "lease" {
            assert!(matches!(
                failed.policy,
                Some(Disposition::Refused {
                    kind: RefusalKind::Lease,
                    ..
                })
            ));
        } else {
            assert_eq!(
                failed.report["reason"],
                if variant == "cancel" {
                    "cancelled"
                } else {
                    "observation_invalidated"
                }
            );
        }
        assert!(fixture.writes().is_empty());
        transport.shutdown().await.unwrap();
        close(owner, bridge, fixture).await;
    }
}

#[tokio::test]
async fn a_lost_transition_reply_stays_unknown_with_one_dispatch_and_no_reusable_stage() {
    let fixture = Fixture::new(
        Hooks {
            lose_transition: true,
            ..Default::default()
        },
        1,
    )
    .await;
    let (owner, bridge) = owners(&fixture).await;
    let handle = owner.handle();
    let control = control();
    let scratch = Scratch::new();
    let mut store = scratch.store(&fixture.url);
    let mut machine = StageMachine::default();
    let mut staging = bind(
        &handle,
        &bridge,
        control.clone(),
        BoundCall::stage(target()).unwrap(),
        Cancellation::new(),
    );
    ok(machine
        .stage(
            &mut context(&mut staging, &mut store),
            target(),
            &id(1),
            &id(2),
        )
        .await);
    staging.shutdown().await.unwrap();
    let mut taking = bind(
        &handle,
        &bridge,
        control,
        BoundCall::take(&id(1)).unwrap(),
        Cancellation::new(),
    );
    let failed = failure(
        machine
            .take(&mut context(&mut taking, &mut store), &id(1), &id(3))
            .await,
    );
    assert_eq!(failed.report["status"], "unknown");
    assert!(failed.report["requestAccepted"].is_null());
    assert_eq!(machine.token_count(), 0);
    let receipt = store.read(&id(3)).unwrap();
    assert_eq!(receipt.phase(), Phase::Unknown);
    assert_eq!(receipt.to_value()["postconditionObserved"], false);
    assert_eq!(
        fixture
            .writes()
            .iter()
            .filter(|request| request["d"]["requestType"] == "TriggerStudioModeTransition")
            .count(),
        1
    );
    taking.shutdown().await.unwrap();
    close(owner, bridge, fixture).await;
}

#[tokio::test]
async fn late_take_policy_refusal_exposes_receipt_and_consumed_token_restaging_guidance() {
    let (last_studio, studio_seen, release_studio) = pause();
    let (version, version_seen, release_version) = pause();
    let fixture = Fixture::new(
        Hooks {
            last_studio: Some(last_studio),
            last_studio_at: 6,
            version: Some(version),
            ..Default::default()
        },
        1,
    )
    .await;
    let (owner, bridge) = owners(&fixture).await;
    let handle = owner.handle();
    let control = control();
    let scratch = Scratch::new();
    let mut store = scratch.store(&fixture.url);
    let mut machine = StageMachine::default();
    let mut staging = bind(
        &handle,
        &bridge,
        control.clone(),
        BoundCall::stage(target()).unwrap(),
        Cancellation::new(),
    );
    ok(machine
        .stage(
            &mut context(&mut staging, &mut store),
            target(),
            &id(1),
            &id(2),
        )
        .await);
    staging.shutdown().await.unwrap();
    let mut taking = bind(
        &handle,
        &bridge,
        control.clone(),
        BoundCall::take(&id(1)).unwrap(),
        Cancellation::new(),
    );
    let (stage_id, operation_id) = (id(1), id(3));
    let mut invocation = context(&mut taking, &mut store);
    let mut job = Box::pin(machine.take(&mut invocation, &stage_id, &operation_id));
    tokio::select! {value=&mut job=>panic!("take ended before queue fixture: {}",match value{Ok(s)=>s.report,Err(f)=>f.report}),_=studio_seen=>{}}
    let blocking_handle = handle.clone();
    let mut blocker = Box::pin(async move {
        blocking_handle
            .request(blocking_handle.token(), "GetVersion", Value::Null, WAIT)
            .await
    });
    assert!(poll!(&mut blocker).is_pending());
    release_studio.send(Action::Continue).unwrap();
    tokio::select! {value=&mut job=>panic!("take ended before blocker: {}",match value{Ok(s)=>s.report,Err(f)=>f.report}),_=version_seen=>{}}
    assert!(poll!(&mut job).is_pending());
    let intent: Value =
        serde_json::from_slice(&fs::read(scratch.path(&operation_id)).unwrap()).unwrap();
    assert_eq!(intent["phase"], "dispatching");
    control
        .lock()
        .unwrap()
        .claim("other", 1, "competing holder", EPOCH)
        .unwrap();
    release_version.send(Action::Continue).unwrap();
    let (result, _) = tokio::join!(job, blocker);
    let failed = failure(result);
    let projected = failed.tool_result();
    assert!(matches!(
        failed.policy,
        Some(Disposition::Refused {
            kind: RefusalKind::Lease,
            ..
        })
    ));
    assert_eq!(projected["isError"], true);
    assert_eq!(projected["structuredContent"]["operationId"], operation_id);
    assert_eq!(projected["structuredContent"]["requestDispatched"], false);
    assert_eq!(projected["structuredContent"]["stageTokenConsumed"], true);
    let next = projected["structuredContent"]["nextCheck"]
        .as_str()
        .unwrap();
    assert!(next.contains("consumed"));
    assert!(next.contains("stage again"));
    assert_eq!(
        projected["structuredContent"]["policyRefusal"]["content"],
        projected["content"]
    );
    assert!(
        projected["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("other has control")
    );
    assert_eq!(
        store.read(&operation_id).unwrap().phase(),
        Phase::NotDispatched
    );
    assert_eq!(machine.token_count(), 0);
    assert_eq!(fixture.writes().len(), 1);
    taking.shutdown().await.unwrap();
    control.lock().unwrap().release("other", EPOCH);
    let mut retry = bind(
        &handle,
        &bridge,
        control,
        BoundCall::take(&stage_id).unwrap(),
        Cancellation::new(),
    );
    assert_eq!(
        failure(
            machine
                .take(&mut context(&mut retry, &mut store), &stage_id, &id(4))
                .await
        )
        .report["reason"],
        "stage_unavailable"
    );
    assert_eq!(fixture.writes().len(), 1);
    retry.shutdown().await.unwrap();
    close(owner, bridge, fixture).await;
}

#[tokio::test]
async fn initial_policy_denial_keeps_the_original_raw_error_projection_and_creates_no_intent() {
    let fixture = Fixture::new(Hooks::default(), 1).await;
    let (owner, bridge) = owners(&fixture).await;
    let handle = owner.handle();
    let control = control();
    let scratch = Scratch::new();
    let mut store = scratch.store(&fixture.url);
    let mut machine = StageMachine::default();
    control
        .lock()
        .unwrap()
        .claim("other", 1, "fixture", EPOCH)
        .unwrap();
    let mut transport = bind(
        &handle,
        &bridge,
        control,
        BoundCall::stage(target()).unwrap(),
        Cancellation::new(),
    );
    let failed = failure(
        machine
            .stage(
                &mut context(&mut transport, &mut store),
                target(),
                &id(1),
                &id(2),
            )
            .await,
    );
    let projection = failed.tool_result();
    assert_eq!(projection["isError"], true);
    assert!(projection.get("structuredContent").is_none());
    assert!(
        projection["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("other has control")
    );
    assert!(!scratch.root().exists());
    assert!(fixture.trace.lock().unwrap().is_empty());
    transport.shutdown().await.unwrap();
    close(owner, bridge, fixture).await;
}

#[tokio::test]
async fn scoped_cancellation_and_events_fence_delivery_but_retain_healthy_reads_and_other_scopes() {
    for cancel_request in [false, true] {
        let (input, input_seen, release_input) = pause();
        let fixture = Fixture::new(
            Hooks {
                input: Some(input),
                ..Default::default()
            },
            1,
        )
        .await;
        let (owner, bridge) = owners(&fixture).await;
        let handle = owner.handle();
        let control = control();
        let cancellation = Cancellation::new();
        let mut a = bind(
            &handle,
            &bridge,
            control.clone(),
            BoundCall::stage(target()).unwrap(),
            cancellation.clone(),
        );
        let mut b = bind(
            &handle,
            &bridge,
            control,
            BoundCall::stage(target()).unwrap(),
            Cancellation::new(),
        );
        let deadline = Instant::now() + WAIT;
        let states = a.reader.synchronize(deadline).await.unwrap();
        let mut sample = Observation::new(
            &mut a.reader,
            states,
            handle.token().generation(),
            from_epoch_duration(Duration::from_millis(EPOCH))
                .unwrap()
                .utc,
            deadline,
        )
        .unwrap();
        let mut read =
            Box::pin(sample.read(ReadRequest::InputSettings, json!({"inputUuid":"input"})));
        tokio::select! {value=&mut read=>panic!("read completed before fixture: {value:?}"),_=input_seen=>{}}
        if cancel_request {
            cancellation.cancel();
        }
        assert!(poll!(&mut read).is_pending());
        let mut other = Box::pin(b.reader.read(ReadRequest::Version, Value::Null, deadline));
        assert!(poll!(&mut other).is_pending());
        release_input
            .send(Action::Event("InputSettingsChanged"))
            .unwrap();
        let (first, second) = tokio::join!(read, other);
        assert_eq!(
            first.unwrap_err().reason,
            if cancel_request {
                "cancelled"
            } else {
                "observation_invalidated"
            }
        );
        assert_eq!(second.unwrap()["obsVersion"], "fixture");
        assert!(!bridge.evidence().borrow().cancelled);
        assert!(handle.invalidation().is_none());
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
        close(owner, bridge, fixture).await;
    }
}

#[tokio::test]
async fn an_event_received_before_audio_604_cannot_be_tolerated_as_fresh_evidence() {
    let fixture = Fixture::new(
        Hooks {
            event_before_audio_rejection: true,
            ..Default::default()
        },
        1,
    )
    .await;
    let (owner, bridge) = owners(&fixture).await;
    let handle = owner.handle();
    let scratch = Scratch::new();
    let mut store = scratch.store(&fixture.url);
    let mut machine = StageMachine::default();
    let mut transport = bind(
        &handle,
        &bridge,
        control(),
        BoundCall::stage(target()).unwrap(),
        Cancellation::new(),
    );
    let failed = failure(
        machine
            .stage(
                &mut context(&mut transport, &mut store),
                target(),
                &id(1),
                &id(2),
            )
            .await,
    );
    assert_eq!(failed.report["reason"], "observation_invalidated");
    assert_eq!(failed.report["requestDispatched"], false);
    assert!(!scratch.root().exists());
    assert!(fixture.writes().is_empty());
    assert_eq!(machine.token_count(), 0);
    assert!(handle.invalidation().is_none());
    transport.shutdown().await.unwrap();
    close(owner, bridge, fixture).await;
}

#[tokio::test]
async fn human_confirmation_is_required_before_consumption_and_the_answer_is_bound_to_the_next_call()
 {
    let fixture = Fixture::new(Hooks::default(), 1).await;
    let (owner, bridge) = owners(&fixture).await;
    let handle = owner.handle();
    let control = control();
    let scratch = Scratch::new();
    let mut store = scratch.store(&fixture.url);
    let mut machine = StageMachine::default();
    let mut staging = bind(
        &handle,
        &bridge,
        control.clone(),
        BoundCall::stage(target()).unwrap(),
        Cancellation::new(),
    );
    ok(machine
        .stage(
            &mut context(&mut staging, &mut store),
            target(),
            &id(1),
            &id(2),
        )
        .await);
    staging.shutdown().await.unwrap();
    let name = json!("codex");
    let envelope =
        json!({"io.modelcontextprotocol/clientCapabilities":{"elicitation":{"form":{}}}});
    let caller_context = || CallerContext {
        http_client_id: Some(&name),
        envelope: Some(&envelope),
        legacy_name: None,
    };
    let mut asking = StageTransport::bind(
        TransportContext {
            handle: handle.clone(),
            bridge: &bridge,
            control: control.clone(),
            clock: clock(),
            cancellation: Cancellation::new(),
        },
        BoundCall::take(&id(1)).unwrap(),
        CallerBinding::resolve(caller_context(), None, true, None, Some(true)),
    )
    .unwrap();
    let reads = fixture.trace.lock().unwrap().len();
    let needed = failure(
        machine
            .take(&mut context(&mut asking, &mut store), &id(1), &id(3))
            .await,
    )
    .tool_result();
    assert_eq!(needed["resultType"], "input_required");
    assert_eq!(
        needed["inputRequests"]["confirm"]["params"]["message"],
        "Transition the staged preview scene to program?"
    );
    assert_eq!(machine.token_count(), 1);
    assert_eq!(fixture.trace.lock().unwrap().len(), reads);
    assert!(store.read(&id(3)).is_err());
    asking.shutdown().await.unwrap();
    let answer = json!({"confirm":{"action":"accept","content":{"confirm":true,"unrelated":"not retained"}}});
    let mut accepted = StageTransport::bind(
        TransportContext {
            handle: handle.clone(),
            bridge: &bridge,
            control,
            clock: clock(),
            cancellation: Cancellation::new(),
        },
        BoundCall::take(&id(1)).unwrap(),
        CallerBinding::resolve(caller_context(), None, true, Some(&answer), None),
    )
    .unwrap();
    assert_eq!(
        ok(machine
            .take(&mut context(&mut accepted, &mut store), &id(1), &id(4))
            .await)
        .report["status"],
        "verified"
    );
    assert_eq!(machine.token_count(), 0);
    assert_eq!(fixture.writes().len(), 2);
    accepted.shutdown().await.unwrap();
    close(owner, bridge, fixture).await;
}

#[tokio::test]
async fn bindings_reject_another_physical_session_custom_profiles_and_mismatched_arguments() {
    let fixture = Fixture::new(Hooks::default(), 3).await;
    let (owner_a, events_a) = ObsSession::connect(&fixture.url, None, stage_options())
        .await
        .unwrap();
    let (owner_b, events_b) = ObsSession::connect(&fixture.url, None, stage_options())
        .await
        .unwrap();
    let (owner_c, events_c) = ObsSession::connect(&fixture.url, None, stage_options())
        .await
        .unwrap();
    let bridge_a = ObservationBridge::bind(owner_a.handle(), events_a, None).unwrap();
    let bridge_b = ObservationBridge::bind(owner_b.handle(), events_b, None).unwrap();
    let bridge_c =
        ObservationBridge::bind_with_event_filter(owner_c.handle(), events_c, None, |_| false)
            .unwrap();
    assert!(
        StageTransport::bind(
            TransportContext {
                handle: owner_a.handle(),
                bridge: &bridge_b,
                control: control(),
                clock: clock(),
                cancellation: Cancellation::new()
            },
            BoundCall::stage(target()).unwrap(),
            caller()
        )
        .is_err()
    );
    assert!(
        StageTransport::bind(
            TransportContext {
                handle: owner_c.handle(),
                bridge: &bridge_c,
                control: control(),
                clock: clock(),
                cancellation: Cancellation::new()
            },
            BoundCall::stage(target()).unwrap(),
            caller()
        )
        .is_err()
    );
    let scratch = Scratch::new();
    let mut store = scratch.store(&fixture.url);
    let mut machine = StageMachine::default();
    let mut transport = bind(
        &owner_a.handle(),
        &bridge_a,
        control(),
        BoundCall::stage(target()).unwrap(),
        Cancellation::new(),
    );
    let mut wrong = target();
    wrong.scene_name = "Main".into();
    let denied = failure(
        machine
            .stage(
                &mut context(&mut transport, &mut store),
                wrong,
                &id(1),
                &id(2),
            )
            .await,
    );
    assert_eq!(denied.report["status"], "blocked");
    assert!(!scratch.root().exists());
    assert!(fixture.trace.lock().unwrap().is_empty());
    transport.shutdown().await.unwrap();
    owner_a.shutdown().await.unwrap();
    owner_b.shutdown().await.unwrap();
    owner_c.shutdown().await.unwrap();
    bridge_a.shutdown().await.unwrap();
    bridge_b.shutdown().await.unwrap();
    bridge_c.shutdown().await.unwrap();
    fixture.joined().await;
}

#[tokio::test]
async fn stage_admission_requires_default_and_transform_subscriptions_and_accepts_extra_events() {
    for (mask, admitted) in [
        (0, false),
        (ALL_EVENTS, false),
        (ALL_EVENTS & !(1 << 3), false),
        (ALL_EVENTS | (1 << 19), true),
        ((ALL_EVENTS | (1 << 19)) & !(1 << 3), false),
        (ALL_EVENTS | (1 << 19) | (1 << 16), true),
    ] {
        let fixture = Fixture::new(Hooks::default(), 1).await;
        let (owner, events) = ObsSession::connect(
            &fixture.url,
            None,
            SessionOptions {
                event_subscriptions: mask,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let handle = owner.handle();
        assert_eq!(handle.event_subscriptions(), mask);
        let bridge = ObservationBridge::bind(handle.clone(), events, None).unwrap();
        assert!(bridge.fence().full_profile());
        {
            let result = StageTransport::bind(
                TransportContext {
                    handle,
                    bridge: &bridge,
                    control: control(),
                    clock: clock(),
                    cancellation: Cancellation::new(),
                },
                BoundCall::stage(target()).unwrap(),
                caller(),
            );
            match result {
                Ok(transport) => {
                    assert!(admitted, "unexpected admission for mask {mask}");
                    transport.shutdown().await.unwrap();
                }
                Err(error) => {
                    assert!(!admitted, "unexpected refusal for mask {mask}");
                    assert_eq!(error.reason, "capability_unavailable");
                }
            }
        }
        assert!(fixture.trace.lock().unwrap().is_empty());
        close(owner, bridge, fixture).await;
    }
}

#[test]
fn bound_take_ids_follow_the_actual_pinned_zod_uuid_schema_including_sentinels() {
    let values = json!([
        "00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "00000000-0000-4000-8000-000000000001",
        "A0B0C0D0-1111-8AAA-BBBB-123456789ABC",
        "unknown",
        "00000000-0000-0000-8000-000000000001",
        "00000000-0000-4000-7000-000000000001"
    ]);
    let output=Command::new("node").args(["--input-type=module","-e","import{z}from'zod';console.log(JSON.stringify(JSON.parse(process.env.STAGE_UUID_FIXTURE).map(value=>z.uuid().safeParse(value).success)))"]).env("STAGE_UUID_FIXTURE",values.to_string()).current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()).output().unwrap();
    assert!(output.status.success());
    let source: Vec<bool> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        values
            .as_array()
            .unwrap()
            .iter()
            .map(|value| BoundCall::take(value.as_str().unwrap()).is_ok())
            .collect::<Vec<_>>(),
        source
    );
}
