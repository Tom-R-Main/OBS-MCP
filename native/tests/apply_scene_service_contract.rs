// SPDX-License-Identifier: GPL-2.0-only
//! Actual Service/Session workflows over one owned mock socket; no public route.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    apply_scene::Request,
    clock,
    control::ControlDomain,
    custody::SharedControl,
    observation::ReadRequest,
    operation_store::OperationStore,
    preflight::{self, Report},
    service::{
        ConnectionDescriptor, Options, PreparationOwners, REQUIRED_SUBSCRIPTIONS, Resources,
        Service, ServiceHandle, TakeServices,
    },
    session::{ObsSession, SessionOptions},
    session_observation::ObservationBridge,
    snapshot_tools,
    stage_transport::Cancellation,
    take_monitor::Summary,
    take_transport::{Bounds, HostServices, PreflightReader},
    takes::{ClipCheck, IoFuture, Limits, Policy, StartOptions},
    tool_validation::ApplySceneSettingsOrder,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
const WAIT: Duration = Duration::from_secs(8);
const EPOCH: u64 = 1_793_448_000_000;
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "apply-service-{}-{}",
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
            .filter_map(|e| {
                let p = e.unwrap().path();
                (p.extension().and_then(|s| s.to_str()) == Some("json"))
                    .then(|| serde_json::from_slice(&fs::read(p).unwrap()).unwrap())
            })
            .collect()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn current() -> Value {
    json!({"program":"Scene","nextId":3,"video":{"baseWidth":1920,"baseHeight":1080},"inputs":{"Input":{"inputKind":"image_source","inputSettings":{"file":"before"},"inputMuted":false,"inputVolumeMul":1,"inputAudioTracks":{"1":true}},"Other":{"inputKind":"image_source","inputSettings":{}}},"scenes":{"Scene":[{"sceneItemId":2,"sourceName":"Other","sceneItemIndex":0,"sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemTransform":{}},{"sceneItemId":1,"sourceName":"Input","sceneItemIndex":1,"sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemTransform":{"positionX":0}}]}})
}
fn request(args: Value) -> Request {
    let frame = json!({"params":{"arguments":args}}).to_string();
    let order = ApplySceneSettingsOrder::from_frame(&args, frame.as_bytes()).unwrap();
    Request::parse(&args, &order).unwrap()
}
fn key(frame: &Value) -> String {
    if frame["op"] == 6 {
        return frame["d"]["requestType"].as_str().unwrap().into();
    }
    let rows = frame["d"]["requests"].as_array().unwrap();
    if rows
        .iter()
        .any(|r| r["requestType"].as_str().unwrap().starts_with("Create"))
    {
        "create".into()
    } else if rows.iter().any(|r| {
        r["requestType"].as_str().unwrap().starts_with("Set")
            || r["requestType"] == "RemoveSceneItem"
    }) {
        "change".into()
    } else if rows.iter().any(|r| r["requestType"] == "GetInputSettings") {
        "input-read".into()
    } else {
        "scene-read".into()
    }
}
fn response(
    kind: &str,
    data: &Value,
    state: &mut Value,
    reject: &Option<String>,
) -> (bool, u64, Option<String>, Value) {
    let yes = |data| (true, 100, None, data);
    let no = |code, message: &str| (false, code, Some(message.into()), json!({}));
    if reject.as_deref() == Some(kind) {
        return no(601, "fixture rejected");
    }
    let name = data["inputName"].as_str().unwrap_or("");
    let scene = data["sceneName"].as_str().unwrap_or("");
    if matches!(
        kind,
        "GetInputSettings" | "GetInputMute" | "GetInputVolume" | "GetInputAudioTracks"
    ) {
        let Some(input) = state["inputs"].get(name) else {
            return no(600, "input absent");
        };
        if kind == "GetInputSettings" {
            return yes(
                json!({"inputKind":input["inputKind"],"inputSettings":input["inputSettings"]}),
            );
        }
        let field = match kind {
            "GetInputMute" => "inputMuted",
            "GetInputVolume" => "inputVolumeMul",
            _ => "inputAudioTracks",
        };
        return match input.get(field) {
            Some(value) => yes(json!({field:value})),
            None => no(604, "unsupported"),
        };
    }
    match kind {
        "GetVersion" => yes(
            json!({"platform":"macos","availableRequests":["GetVersion","GetSceneList","GetInputList","GetVideoSettings","GetSceneItemList","GetInputSettings","GetInputMute","GetInputVolume","GetInputAudioTracks","GetStats","GetRecordStatus","GetCurrentProgramScene","GetProfileParameter","StartRecord","StopRecord","CreateRecordChapter","CreateScene","CreateInput","CreateSceneItem","SetInputSettings","SetInputMute","SetInputVolume","SetInputAudioTracks","SetSceneItemTransform","SetSceneItemEnabled","SetSceneItemLocked","SetSceneItemIndex","RemoveSceneItem"],"supportedImageFormats":[]}),
        ),
        "GetSceneList" => yes(
            json!({"scenes":state["scenes"].as_object().unwrap().keys().map(|name|json!({"sceneName":name,"sceneUuid":name})).collect::<Vec<_>>(),"currentProgramSceneName":state["program"]}),
        ),
        "GetInputList" => yes(
            json!({"inputs":state["inputs"].as_object().unwrap().iter().map(|(name,value)|json!({"inputName":name,"inputKind":value["inputKind"]})).collect::<Vec<_>>()}),
        ),
        "GetVideoSettings" => yes(state["video"].clone()),
        "GetSceneItemList" => match state["scenes"].get(scene).and_then(Value::as_array) {
            Some(items) => yes(
                json!({"sceneItems":items.iter().enumerate().map(|(index,item)|{let mut item=item.clone();item["sceneItemIndex"]=json!(index);let kind=state["inputs"][item["sourceName"].as_str().unwrap()]["inputKind"].as_str().unwrap_or("");let blank=kind=="window_capture"||kind.ends_with("_input_capture")||matches!(kind,"ffmpeg_source"|"vlc_source");item["inputKind"]=json!(kind);item["sceneItemTransform"]["sourceWidth"]=json!(if blank{0}else{800});item["sceneItemTransform"]["sourceHeight"]=json!(if blank{0}else{600});item}).collect::<Vec<_>>()}),
            ),
            None => no(600, "scene absent"),
        },
        "GetStats" => {
            yes(json!({"availableDiskSpace":50000,"renderSkippedFrames":0,"renderTotalFrames":0}))
        }
        "GetRecordStatus" => yes(
            json!({"outputActive":state["recording"].as_bool().unwrap_or(false),"outputPaused":false,"outputBytes":1048576}),
        ),
        "GetCurrentProgramScene" => {
            yes(json!({"currentProgramSceneName":"Scene","currentProgramSceneUuid":"Scene"}))
        }
        "GetProfileParameter" => yes(
            json!({"parameterValue":if data["parameterCategory"]=="Output"&&data["parameterName"]=="Mode"{"Simple"}else{"1"}}),
        ),
        "StartRecord" | "CreateRecordChapter" => yes(json!({})),
        "StopRecord" => yes(json!({"outputPath":"/fixture/take.mkv"})),
        "CreateScene" => {
            if state["scenes"].get(scene).is_some() || state["inputs"].get(scene).is_some() {
                return no(601, "duplicate source");
            }
            state["scenes"][scene] = json!([]);
            yes(json!({}))
        }
        "CreateInput" | "CreateSceneItem" => {
            let input = if kind == "CreateInput" {
                name
            } else {
                data["sourceName"].as_str().unwrap()
            };
            if kind == "CreateInput" {
                if state["inputs"].get(input).is_some() {
                    return no(601, "duplicate source");
                }
                let input_kind = data["inputKind"].as_str().unwrap();
                let mut value = json!({"inputKind":input_kind,"inputSettings":data.get("inputSettings").cloned().unwrap_or_else(||json!({}))});
                if input_kind.contains("capture") {
                    value["inputMuted"] = json!(false);
                    value["inputVolumeMul"] = json!(1);
                    value["inputAudioTracks"] = json!({"1":true});
                }
                state["inputs"][input] = value;
            }
            if state["inputs"].get(input).is_none() {
                return no(600, "input absent");
            }
            let id = state["nextId"].as_u64().unwrap();
            state["nextId"] = json!(id + 1);
            let Some(items) = state["scenes"].get_mut(scene).and_then(Value::as_array_mut) else {
                return no(600, "scene absent");
            };
            items.push(json!({"sceneItemId":id,"sourceName":input,"sceneItemEnabled":data["sceneItemEnabled"]!=false,"sceneItemLocked":false,"sceneItemTransform":{"positionX":0,"positionY":0,"scaleX":1,"scaleY":1,"rotation":0,"alignment":5,"boundsType":"OBS_BOUNDS_NONE"}}));
            yes(json!({"sceneItemId":id}))
        }
        "SetInputSettings" | "SetInputMute" | "SetInputVolume" | "SetInputAudioTracks" => {
            let Some(input) = state["inputs"].get_mut(name) else {
                return no(600, "input absent");
            };
            match kind {
                "SetInputSettings" => {
                    for (k, v) in data["inputSettings"].as_object().unwrap() {
                        input["inputSettings"][k] = v.clone();
                    }
                }
                "SetInputMute" => input["inputMuted"] = data["inputMuted"].clone(),
                "SetInputVolume" => {
                    input["inputVolumeMul"] =
                        json!(10f64.powf(data["inputVolumeDb"].as_f64().unwrap() / 20.0))
                }
                _ => input["inputAudioTracks"] = data["inputAudioTracks"].clone(),
            };
            yes(json!({}))
        }
        "SetSceneItemTransform"
        | "SetSceneItemEnabled"
        | "SetSceneItemLocked"
        | "SetSceneItemIndex"
        | "RemoveSceneItem" => {
            let Some(items) = state["scenes"].get_mut(scene).and_then(Value::as_array_mut) else {
                return no(600, "scene absent");
            };
            let Some(index) = items
                .iter()
                .position(|item| item["sceneItemId"] == data["sceneItemId"])
            else {
                return no(600, "item absent");
            };
            match kind {
                "RemoveSceneItem" => {
                    items.remove(index);
                }
                "SetSceneItemIndex" => {
                    let item = items.remove(index);
                    let index = data["sceneItemIndex"].as_u64().unwrap() as usize;
                    items.insert(index.min(items.len()), item);
                }
                "SetSceneItemEnabled" => {
                    items[index]["sceneItemEnabled"] = data["sceneItemEnabled"].clone()
                }
                "SetSceneItemLocked" => {
                    items[index]["sceneItemLocked"] = data["sceneItemLocked"].clone()
                }
                _ => {
                    for (k, v) in data["sceneItemTransform"].as_object().unwrap() {
                        items[index]["sceneItemTransform"][k] = v.clone();
                    }
                }
            };
            yes(json!({}))
        }
        other => panic!("unexpected request {other}"),
    }
}
#[derive(Default)]
struct Host {
    inspected: AtomicU64,
    joined: AtomicBool,
}
impl HostServices for Host {
    fn preflight(&self, _: PreflightReader, _: preflight::Options) -> IoFuture<Report> {
        Box::pin(async {
            Report {
                ready: true,
                checks: vec![preflight::Check {
                    id: "record-directory".into(),
                    status: preflight::Status::Pass,
                    message: "fixture host".into(),
                }],
            }
        })
    }
    fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
        self.inspected.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            ClipCheck {
                skipped: Some("fixture media".into()),
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
struct Fixture {
    url: String,
    trace: Arc<Mutex<Vec<Value>>>,
    state: Arc<Mutex<Value>>,
    hold: Arc<Mutex<HashMap<String, usize>>>,
    held: Arc<Mutex<HashMap<String, Value>>>,
    reject: Arc<Mutex<Option<String>>>,
    lost: Arc<AtomicBool>,
    malformed: Arc<AtomicBool>,
    notify: Arc<Notify>,
    out: mpsc::Sender<Value>,
    task: JoinHandle<()>,
    control: SharedControl,
    host: Arc<Host>,
}
impl Fixture {
    async fn new() -> Self {
        Self::new_at("127.0.0.1:0").await
    }
    async fn new_at(bind: &str) -> Self {
        let listener = TcpListener::bind(bind).await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let trace = Arc::new(Mutex::new(vec![]));
        let state = Arc::new(Mutex::new(current()));
        let hold = Arc::new(Mutex::new(HashMap::new()));
        let held = Arc::new(Mutex::new(HashMap::new()));
        let reject = Arc::new(Mutex::new(None));
        let lost = Arc::new(AtomicBool::new(false));
        let malformed = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());
        let (out, mut commands) = mpsc::channel::<Value>(16);
        let (history, observed, holding, pending, rejections, lose, bad, wake) = (
            trace.clone(),
            state.clone(),
            hold.clone(),
            held.clone(),
            reject.clone(),
            lost.clone(),
            malformed.clone(),
            notify.clone(),
        );
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(socket).await.unwrap();
            peer.send(Message::Text(
                json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.6"}})
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
                                 command=commands.recv()=>match command{Some(frame)=>peer.send(Message::Text(frame.to_string().into())).await.unwrap(),None=>break},
                                 message=peer.next()=>match message{Some(Ok(Message::Text(text)))=>{let frame:Value=serde_json::from_str(&text).unwrap();history.lock().unwrap().push(frame.clone());let tag=key(&frame);if tag=="StartRecord"||tag=="StopRecord"{let active=tag=="StartRecord";observed.lock().unwrap()["recording"]=json!(active);peer.send(Message::Text(json!({"op":5,"d":{"eventType":"RecordStateChanged","eventIntent":64,"eventData":{"outputState":if active{"OBS_WEBSOCKET_OUTPUT_STARTED"}else{"OBS_WEBSOCKET_OUTPUT_STOPPED"},"outputActive":active,"outputPath":"/fixture/take.mkv"}}}).to_string().into())).await.unwrap();}let reply={let mut state=observed.lock().unwrap();let rejection=rejections.lock().unwrap().clone();if frame["op"]==6{let r=&frame["d"];let(ok,code,comment,data)=response(r["requestType"].as_str().unwrap(),&r["requestData"],&mut state,&rejection);json!({"op":7,"d":{"requestId":r["requestId"],"requestType":r["requestType"],"requestStatus":{"result":ok,"code":code,"comment":comment},"responseData":data}})}else{assert_eq!(frame["op"],8);let mut rows=Vec::new();for r in frame["d"]["requests"].as_array().unwrap(){let(ok,code,comment,data)=response(r["requestType"].as_str().unwrap(),&r["requestData"],&mut state,&rejection);rows.push(json!({"requestId":r["requestId"],"requestType":r["requestType"],"requestStatus":{"result":ok,"code":code,"comment":comment},"responseData":data}));if !ok&&frame["d"]["haltOnFailure"]==true{break;}}if (tag=="create"||tag=="change")&&bad.load(Ordering::Acquire){rows.pop();}json!({"op":9,"d":{"requestId":frame["d"]["requestId"],"results":rows}})}};if (tag=="create"||tag=="change")&&lose.load(Ordering::Acquire){break;}
                let count=history.lock().unwrap().iter().filter(|f|key(f)==tag).count();let should_hold=holding.lock().unwrap().get(&tag).copied()==Some(count);if should_hold{holding.lock().unwrap().remove(&tag);pending.lock().unwrap().insert(tag,reply);}else{peer.send(Message::Text(reply.to_string().into())).await.unwrap();}wake.notify_waiters();},Some(Ok(Message::Close(_)))|Some(Err(_))|None=>{let _=peer.flush().await;break;},_=>{}}
                                }
            }
            let mut second = Box::pin(listener.accept());
            assert!(poll!(second.as_mut()).is_pending(), "second socket");
        });
        Self {
            url,
            trace,
            state,
            hold,
            held,
            reject,
            lost,
            malformed,
            notify,
            out,
            task,
            control: Arc::new(Mutex::new(ControlDomain::default())),
            host: Arc::new(Host::default()),
        }
    }
    fn count(&self, tag: &str) -> usize {
        self.trace
            .lock()
            .unwrap()
            .iter()
            .filter(|f| key(f) == tag)
            .count()
    }
    fn hold(&self, tag: &str) {
        let next = self.count(tag) + 1;
        self.hold.lock().unwrap().insert(tag.into(), next);
    }
    async fn seen(&self, tag: &str, n: usize) {
        timeout(WAIT, async {
            loop {
                let wake = self.notify.notified();
                if self.count(tag) >= n && self.hold.lock().unwrap().get(tag).copied() != Some(n) {
                    break;
                }
                wake.await;
            }
        })
        .await
        .unwrap();
    }
    async fn release(&self, tag: &str) {
        let frame = self.held.lock().unwrap().remove(tag).unwrap();
        self.out.send(frame).await.unwrap();
    }
    async fn service(&self, dir: &Dir) -> Service {
        self.service_with_take(dir, false).await
    }
    async fn service_with_take(&self, dir: &Dir, take: bool) -> Service {
        self.service_with_limit(dir, take, WAIT).await
    }
    async fn service_with_limit(&self, dir: &Dir, take: bool, limit: Duration) -> Service {
        self.service_with_owners(dir, take, limit, PreparationOwners::new(&dir.0).unwrap())
            .await
    }
    async fn service_with_owners(
        &self,
        dir: &Dir,
        take: bool,
        limit: Duration,
        owners: PreparationOwners,
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
        let bridge = ObservationBridge::bind(session.handle(), events, None).unwrap();
        static FACTORIES: AtomicU64 = AtomicU64::new(1);
        let mut next = FACTORIES.fetch_add(1, Ordering::Relaxed) * 1000;
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
                    format!("{next:08x}-cccc-4ccc-8ccc-{next:012x}")
                }),
            },
            Options {
                operation_timeout: limit,
                connection: Some(ConnectionDescriptor {
                    url: self.url.clone(),
                    has_password: false,
                }),
                preparation: Some(owners),
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
                        setup: WAIT,
                        stop: WAIT,
                        inspection: WAIT,
                        file: WAIT,
                        start_confirmation: WAIT,
                        stats_interval: Duration::from_secs(60),
                        picture_interval: Duration::from_secs(60),
                    },
                }),
                ..Default::default()
            },
        )
        .unwrap_or_else(|(e, _)| panic!("spawn {e:?}"));
        service
            .handle()
            .read(ReadRequest::Version, Value::Null, limit.min(WAIT))
            .await
            .unwrap();
        service
    }
    async fn joined(self) {
        timeout(WAIT, self.task).await.unwrap().unwrap();
    }
}
fn apply(
    handle: ServiceHandle,
    args: Value,
    cancel: Cancellation,
) -> JoinHandle<Result<Value, obs_mcp_native::service::ServiceError>> {
    tokio::spawn(async move {
        handle
            .apply_scene(request(args), "fixture".into(), cancel)
            .await
    })
}
fn text(result: &Value) -> String {
    result["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}
fn spec() -> Value {
    json!({"sceneName":"Scene","sources":[{"name":"Input","kind":"image_source","settings":{"file":"after"},"locked":true}],"apply":true})
}
#[tokio::test]
async fn policy_precedes_every_read_even_plan_only_no_change_and_corrupt_snapshot_state() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    fs::write(dir.0.join("snapshots.json"), b"{ broken").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            dir.0.join("snapshots.json"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }
    f.control
        .lock()
        .unwrap()
        .claim("other-client", 1, "", EPOCH)
        .unwrap();
    let mut service = f.service(&dir).await;
    let before = f.trace.lock().unwrap().len();
    for args in [
        json!({"sceneName":"Scene","sources":[{"name":"Input"}]}),
        spec(),
    ] {
        let result = service
            .handle()
            .apply_scene(request(args), "fixture".into(), Cancellation::default())
            .await
            .unwrap();
        assert!(text(&result).starts_with("Not changed: other-client has control"));
        assert_eq!(f.trace.lock().unwrap().len(), before);
    }
    assert!(dir.receipts().is_empty());
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn initial_no_change_plan_only_and_business_errors_have_no_snapshot_or_effect_owner() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let h = service.handle();
    let unchanged = h
        .apply_scene(
            request(json!({"sceneName":"Scene","sources":[{"name":"Input"}],"apply":true})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(text(&unchanged), "Scene Scene already matches");
    assert_eq!(unchanged["structuredContent"]["converged"], true);
    let mut planned = spec();
    planned["apply"] = json!(false);
    let plan = h
        .apply_scene(request(planned), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(plan["structuredContent"]["applied"], false);
    assert!(text(&plan).contains("Plan only; call again with apply: true"));
    let bad=h.apply_scene(request(json!({"sceneName":"Scene","sources":[{"name":"Input"},{"name":"Input"}],"apply":true})),"fixture".into(),Cancellation::default()).await.unwrap();
    assert!(text(&bad).contains("Input is listed more than once"));
    assert_eq!(f.count("create") + f.count("change"), 0);
    assert!(dir.receipts().is_empty());
    assert!(!dir.0.join("snapshots.json").exists());
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn creates_then_changes_shared_inputs_with_joined_saved_snapshot_and_returns_owner() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let h = service.handle();
    let args = json!({"sceneName":"Review","sources":[{"name":"Input","settings":{"file":"after"},"visible":false},{"name":"Fresh","kind":"image_source","locked":true,"fit":"canvas"}],"order":true,"apply":true});
    let expected = source(args.clone(), current(), None, None);
    let result = h
        .apply_scene(request(args), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(
        normalized(result.clone(), &saved(&dir)),
        normalized(
            expected["result"].clone(),
            expected["snapshots"].as_array().unwrap()
        )
    );
    assert_eq!(writes(&f), expected["writes"]);
    assert_ne!(result["isError"], true, "{result}");
    assert_eq!(result["structuredContent"]["converged"], true);
    assert_eq!(f.count("create"), 1);
    assert_eq!(f.count("change"), 1);
    let snapshots: Vec<Value> =
        serde_json::from_slice(&fs::read(dir.0.join("snapshots.json")).unwrap()).unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0]["label"], "before obs-apply-scene");
    assert_eq!(snapshots[0]["auto"], true);
    assert_eq!(snapshots[0]["items"], json!([]));
    assert_eq!(snapshots[0]["inputs"][0]["inputName"], "Input");
    assert_eq!(snapshots[0]["inputs"][0]["inputSettings"]["file"], "before");
    let receipts = dir.receipts();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0]["version"], 4);
    assert_eq!(receipts[0]["phase"], "closed");
    assert_eq!(receipts[0]["postconditionObserved"], false);
    assert_eq!(receipts[0]["renderedComposition"], "unverified");
    let list = h
        .snapshot(
            snapshot_tools::Request::parse(&json!({"list":true})).unwrap(),
            Cancellation::default(),
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
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}

const SOURCE_ORACLE: &str = r###"import {pathToFileURL} from 'node:url';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';
const {initialize}=await import(pathToFileURL(process.argv[1]).href);
const payload=JSON.parse(readFileSync(0,'utf8'));const state=structuredClone(payload.state);const writes=[];let reads=0;let callback,schema;
function response(type,data={}) {
 if(type===payload.reject)return {ok:false,code:601,comment:'fixture rejected',responseData:{}};
 if(type==='GetSceneList'){
  reads++;
  if(reads===payload.hookRead){if(payload.hook==='wrong-kind')state.inputs.Input.inputKind='browser_source';if(payload.hook==='remove-input'){delete state.inputs.Input;state.scenes.Scene=state.scenes.Scene.filter(i=>i.sourceName!=='Input');}}
  return {ok:true,code:100,responseData:{scenes:Object.keys(state.scenes).map(sceneName=>({sceneName,sceneUuid:sceneName}))}};
 }
 if(type==='GetInputList')return {ok:true,code:100,responseData:{inputs:Object.entries(state.inputs).filter(([name])=>!(payload.hook==='omit-new'&&reads>=2&&name==='Fresh')).map(([inputName,v])=>({inputName,inputKind:v.inputKind}))}};
 if(type==='GetVideoSettings')return {ok:true,code:100,responseData:state.video};
 const yes=(data={})=>({ok:true,code:100,responseData:data});const no=(code,comment)=>({ok:false,code,comment,responseData:{}});
 if(type==='GetSceneItemList'){
  if(!state.scenes[data.sceneName])return no(600,'scene absent');
  return yes({sceneItems:state.scenes[data.sceneName].map((item,sceneItemIndex)=>{const kind=state.inputs[item.sourceName]?.inputKind??'';const blank=kind==='window_capture'||kind.endsWith('_input_capture')||['ffmpeg_source','vlc_source'].includes(kind);return {...item,sceneItemIndex,inputKind:kind,sceneItemTransform:{...item.sceneItemTransform,sourceWidth:blank?0:800,sourceHeight:blank?0:600}};})});
 }
 const input=state.inputs[data.inputName];
 if(type.startsWith('GetInput')){
  if(!input)return no(600,'input absent');
  if(type==='GetInputSettings')return yes({inputKind:input.inputKind,inputSettings:input.inputSettings});
  const field={GetInputMute:'inputMuted',GetInputVolume:'inputVolumeMul',GetInputAudioTracks:'inputAudioTracks'}[type];return field in input?yes({[field]:input[field]}):no(604,'unsupported');
 }
 if(type==='CreateScene'){if(state.scenes[data.sceneName]||state.inputs[data.sceneName])return no(601,'duplicate source');state.scenes[data.sceneName]=[];return yes();}
 if(type==='CreateInput'||type==='CreateSceneItem'){
  const name=type==='CreateInput'?data.inputName:data.sourceName;
  if(type==='CreateInput'){if(state.inputs[name])return no(601,'duplicate source');const kind=data.inputKind;state.inputs[name]={inputKind:kind,inputSettings:data.inputSettings??{},...(kind.includes('capture')?{inputMuted:false,inputVolumeMul:1,inputAudioTracks:{1:true}}:{})};}
  if(!state.inputs[name])return no(600,'input absent');if(!state.scenes[data.sceneName])return no(600,'scene absent');const id=state.nextId++;
  state.scenes[data.sceneName].push({sceneItemId:id,sourceName:name,sceneItemEnabled:data.sceneItemEnabled!==false,sceneItemLocked:false,sceneItemTransform:{positionX:0,positionY:0,scaleX:1,scaleY:1,rotation:0,alignment:5,boundsType:'OBS_BOUNDS_NONE'}});return yes({sceneItemId:id});
 }
 if(type.startsWith('SetInput')){
  if(!input)return no(600,'input absent');
  if(type==='SetInputSettings')Object.assign(input.inputSettings,data.inputSettings);
  if(type==='SetInputMute')input.inputMuted=data.inputMuted;
  if(type==='SetInputVolume')input.inputVolumeMul=10**(data.inputVolumeDb/20);
  if(type==='SetInputAudioTracks')input.inputAudioTracks=data.inputAudioTracks;
  return yes();
 }
 const items=state.scenes[data.sceneName];if(!items)return no(600,'scene absent');const index=items.findIndex(i=>i.sceneItemId===data.sceneItemId);if(index<0)return no(600,'item absent');
 if(type==='RemoveSceneItem')items.splice(index,1);
 if(type==='SetSceneItemIndex'){const [item]=items.splice(index,1);items.splice(Math.min(data.sceneItemIndex,items.length),0,item);}
 if(type==='SetSceneItemEnabled')items[index].sceneItemEnabled=data.sceneItemEnabled;
 if(type==='SetSceneItemLocked')items[index].sceneItemLocked=data.sceneItemLocked;
 if(type==='SetSceneItemTransform')Object.assign(items[index].sceneItemTransform,data.sceneItemTransform);
 return yes();
}
const client={getConnectionStatus(){return {url:payload.url};},async sendRequest(type,data){const row=response(type,data??{});if(!row.ok)throw Error(`OBS request ${type} failed with code ${row.code}: ${row.comment}`);return row.responseData;},async sendBatch(requests,options={}){if(requests.some(r=>!r.requestType.startsWith('Get')))writes.push({requests,haltOnFailure:options.haltOnFailure??false,executionType:0});const rows=[];for(const r of requests){const row=response(r.requestType,r.requestData);rows.push({requestType:r.requestType,...row});if(!row.ok&&options.haltOnFailure)break;}return rows;}};
initialize({registerTool(name,config,run){if(name==='obs-apply-scene'){callback=run;schema=config.inputSchema;}}},client);
const result=await callback(schema.parse(payload.args));let snapshots=[];try{snapshots=JSON.parse(readFileSync(join(process.env.OBS_MCP_STATE_DIR,'snapshots.json'),'utf8'));}catch{}
process.stdout.write(JSON.stringify({result,writes,state,snapshots}));
"###;

fn source(args: Value, state: Value, reject: Option<&str>, hook: Option<(&str, u64)>) -> Value {
    use std::io::Write;
    let dir = Dir::new();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned();
    let payload = json!({"args":args,"state":state,"url":"ws://source-fixture","reject":reject,"hook":hook.map(|v|v.0),"hookRead":hook.map(|v|v.1)});
    let mut child = Command::new("node")
        .args(["--input-type=module", "--eval", SOURCE_ORACLE])
        .arg(root.join("build/tools/apply-scene.js"))
        .env("OBS_MCP_STATE_DIR", &dir.0)
        .current_dir(&root)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(payload.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "Source oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn normalized(mut result: Value, snapshots: &[Value]) -> Value {
    for snapshot in snapshots {
        if let Some(id) = snapshot["id"].as_str() {
            for item in result["content"].as_array_mut().unwrap() {
                if let Some(text) = item["text"].as_str() {
                    item["text"] = json!(text.replace(id, "<SNAPSHOT_ID>"));
                }
            }
            if result["structuredContent"]["snapshotId"].as_str() == Some(id) {
                result["structuredContent"]["snapshotId"] = json!("<SNAPSHOT_ID>");
            }
        }
    }
    result
}
fn saved(dir: &Dir) -> Vec<Value> {
    if !dir.0.join("snapshots.json").exists() {
        return vec![];
    }
    serde_json::from_slice(&fs::read(dir.0.join("snapshots.json")).unwrap()).unwrap()
}
fn writes(f: &Fixture) -> Value {
    json!(f.trace.lock().unwrap().iter().filter(|frame|matches!(key(frame).as_str(),"create"|"change")).map(|frame|json!({"executionType":frame["d"]["executionType"],"haltOnFailure":frame["d"]["haltOnFailure"],"requests":frame["d"]["requests"].as_array().unwrap().iter().map(|r|json!({"requestType":r["requestType"],"requestData":r["requestData"]})).collect::<Vec<_>>()})).collect::<Vec<_>>())
}

#[tokio::test]
async fn wholly_new_graph_skips_snapshot_and_matches_source_two_pass_result() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let args = json!({"sceneName":"New","sources":[{"name":"Fresh","kind":"image_source","locked":true,"transform":{"positionX":12}}],"apply":true});
    let expected = source(args.clone(), current(), None, None);
    let result = service
        .handle()
        .apply_scene(request(args), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(result, expected["result"]);
    assert_eq!(writes(&f), expected["writes"]);
    assert!(!dir.0.join("snapshots.json").exists());
    assert_eq!(dir.receipts().len(), 1);
    assert_eq!(dir.receipts()[0]["phase"], "closed");
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn mixed_change_failures_exact_order_settings_overlay_and_remaining_match_source() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    *f.reject.lock().unwrap() = Some("SetInputMute".into());
    let mut service = f.service(&dir).await;
    let args = json!({"sceneName":"Scene","sources":[{"name":"Input","settings":{"file":"after"},"muted":true,"volumeDb":-6,"audioTracks":[2],"visible":false,"locked":true,"fit":"canvas","transform":{"positionX":5}}],"order":true,"removeOthers":true,"apply":true});
    let expected = source(args.clone(), current(), Some("SetInputMute"), None);
    let result = service
        .handle()
        .apply_scene(request(args), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(
        normalized(result.clone(), &saved(&dir)),
        normalized(
            expected["result"].clone(),
            expected["snapshots"].as_array().unwrap()
        )
    );
    assert_eq!(writes(&f), expected["writes"]);
    assert_eq!(result["isError"], true);
    assert_eq!(dir.receipts()[0]["phase"], "closed");
    assert_eq!(dir.receipts()[0]["changes"]["counts"]["failed"], 1);
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn required_snapshot_publication_failure_prevents_every_effect_and_receipt() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    fs::create_dir(dir.0.join("snapshots.json")).unwrap();
    let result = service
        .handle()
        .apply_scene(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(result["isError"], true);
    assert!(text(&result).starts_with("Nothing changed: could not save a recovery snapshot:"));
    assert_eq!(f.count("create") + f.count("change"), 0);
    assert!(dir.receipts().is_empty());
    let list = service
        .handle()
        .snapshot(
            snapshot_tools::Request::parse(&json!({"list":true})).unwrap(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(list["isError"], true);
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn auto_snapshot_retention_uses_the_checked_auto_lane() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    for n in 0..11 {
        let mut args = spec();
        args["sources"][0]["settings"]["file"] = json!(format!("after{n}"));
        let result = service
            .handle()
            .apply_scene(request(args), "fixture".into(), Cancellation::default())
            .await
            .unwrap();
        assert_ne!(result["isError"], true, "{result}");
    }
    let snapshots = saved(&dir);
    assert_eq!(snapshots.len(), 10);
    assert!(snapshots.iter().all(|s| s["auto"] == true));
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}

impl Fixture {
    fn hold_nth(&self, tag: &str, n: usize) {
        self.hold.lock().unwrap().insert(tag.into(), n);
    }
}
fn create_spec() -> Value {
    json!({"sceneName":"Review","sources":[{"name":"Input","kind":"image_source","settings":{"file":"after"}},{"name":"Fresh","kind":"image_source","locked":true}],"apply":true})
}
fn evidence(result: &Value) -> &Value {
    &result["structuredContent"]["errorEvidence"]
}
#[tokio::test]
async fn lease_refusal_after_saved_snapshot_surfaces_metadata_and_returns_original_owner() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold_nth("input-read", 2);
    let job = apply(service.handle(), spec(), Cancellation::default());
    f.seen("input-read", 2).await;
    f.control
        .lock()
        .unwrap()
        .claim("other-client", 1, "", EPOCH)
        .unwrap();
    f.release("input-read").await;
    let result = job.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(text(&result).starts_with("Not changed:"));
    assert_eq!(saved(&dir).len(), 1);
    assert_eq!(evidence(&result)["snapshotId"], saved(&dir)[0]["id"]);
    assert_eq!(evidence(&result)["postconditionObserved"], false);
    assert_eq!(f.count("create") + f.count("change"), 0);
    assert!(dir.receipts().is_empty());
    let list = service
        .handle()
        .snapshot(
            snapshot_tools::Request::parse(&json!({"list":true})).unwrap(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert!(list["structuredContent"]["snapshots"].is_array());
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn renewed_creation_and_second_plan_kind_error_match_corrected_source_with_known_prefix() {
    for wrong_kind in [false, true] {
        let f = Fixture::new().await;
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        let args = create_spec();
        let expected = source(
            args.clone(),
            current(),
            None,
            Some((if wrong_kind { "wrong-kind" } else { "omit-new" }, 2)),
        );
        f.hold("create");
        let job = apply(service.handle(), args, Cancellation::default());
        f.seen("create", 1).await;
        if wrong_kind {
            f.state.lock().unwrap()["inputs"]["Input"]["inputKind"] = json!("browser_source");
        } else {
            f.state.lock().unwrap()["inputs"]
                .as_object_mut()
                .unwrap()
                .remove("Fresh");
        }
        f.release("create").await;
        let result = job.await.unwrap().unwrap();
        assert_eq!(
            normalized(result.clone(), &saved(&dir)),
            normalized(
                expected["result"].clone(),
                expected["snapshots"].as_array().unwrap()
            )
        );
        assert!(text(&result).contains("Created before stopping:"));
        assert!(text(&result).contains("holds the previous state"));
        assert_eq!(f.count("create"), 1);
        assert_eq!(f.count("change"), 0);
        assert_eq!(dir.receipts()[0]["phase"], "aborted");
        assert_eq!(dir.receipts()[0]["creations"]["counts"]["successful"], 3);
        assert!(service.shutdown().await.unwrap().joined);
        f.joined().await;
    }
}
#[tokio::test]
async fn final_verification_kind_errors_preserve_exact_source_failures_and_false_convergence() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let args = spec();
    let expected = source(args.clone(), current(), None, Some(("wrong-kind", 3)));
    f.hold("change");
    let job = apply(service.handle(), args, Cancellation::default());
    f.seen("change", 1).await;
    f.state.lock().unwrap()["inputs"]["Input"]["inputKind"] = json!("browser_source");
    f.release("change").await;
    let result = job.await.unwrap().unwrap();
    assert_eq!(
        normalized(result.clone(), &saved(&dir)),
        normalized(
            expected["result"].clone(),
            expected["snapshots"].as_array().unwrap()
        )
    );
    assert_eq!(result["structuredContent"]["converged"], false);
    assert_eq!(result["structuredContent"]["failures"], json!([]));
    assert!(result["structuredContent"]["verificationErrors"].is_array());
    assert_eq!(dir.receipts()[0]["phase"], "closed");
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn lease_between_stages_aborts_known_continuation_without_claiming_nothing_changed() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("create");
    let job = apply(service.handle(), create_spec(), Cancellation::default());
    f.seen("create", 1).await;
    f.control
        .lock()
        .unwrap()
        .claim("other-client", 1, "", EPOCH)
        .unwrap();
    f.release("create").await;
    let result = job.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(!text(&result).contains("Nothing changed"));
    assert!(!text(&result).starts_with("Not changed"));
    assert_eq!(evidence(&result)["snapshotId"], saved(&dir)[0]["id"]);
    assert_eq!(evidence(&result)["knownBatches"][0]["stage"], "creations");
    assert_eq!(evidence(&result)["knownBatches"][0]["successful"], 3);
    assert_eq!(f.count("change"), 0);
    assert_eq!(dir.receipts()[0]["phase"], "aborted");
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn cancellation_and_shutdown_join_held_creation_ack_then_abort_without_second_batch() {
    for shutdown in [false, true] {
        let f = Fixture::new().await;
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        f.hold("create");
        let cancel = Cancellation::default();
        let job = apply(service.handle(), create_spec(), cancel.clone());
        f.seen("create", 1).await;
        if shutdown {
            let mut joining = Box::pin(service.shutdown());
            assert!(poll!(joining.as_mut()).is_pending());
            tokio::task::yield_now().await;
            assert!(poll!(joining.as_mut()).is_pending());
            f.release("create").await;
            assert!(joining.await.unwrap().joined);
        } else {
            cancel.cancel();
            f.release("create").await;
        }
        let result = job.await.unwrap().unwrap();
        assert_eq!(result["isError"], true);
        assert_eq!(evidence(&result)["knownBatches"][0]["successful"], 3);
        assert_eq!(f.count("create"), 1);
        assert_eq!(f.count("change"), 0);
        assert_eq!(dir.receipts()[0]["phase"], "aborted");
        if !shutdown {
            assert!(service.shutdown().await.unwrap().joined);
        }
        f.joined().await;
    }
}
#[tokio::test]
async fn dropped_frontend_await_keeps_admitted_job_and_returns_snapshot_owner_after_completion() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("change");
    let h = service.handle();
    let job = apply(h.clone(), spec(), Cancellation::default());
    f.seen("change", 1).await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    assert_eq!(dir.receipts()[0]["phase"], "dispatching-changes");
    f.release("change").await;
    timeout(WAIT, async {
        loop {
            let result = h
                .snapshot(
                    snapshot_tools::Request::parse(&json!({"list":true})).unwrap(),
                    Cancellation::default(),
                )
                .await
                .unwrap();
            if result["structuredContent"]["snapshots"].is_array() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(dir.receipts()[0]["phase"], "closed");
    assert_eq!(f.count("change"), 1);
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn read_failure_after_creation_preserves_source_text_plus_bounded_error_evidence_and_aborts()
{
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("create");
    let job = apply(service.handle(), create_spec(), Cancellation::default());
    f.seen("create", 1).await;
    *f.reject.lock().unwrap() = Some("GetInputList".into());
    f.release("create").await;
    let result = job.await.unwrap().unwrap();
    assert_eq!(
        text(&result),
        "Apply Scene failed: OBS request GetInputList failed with code 601: fixture rejected"
    );
    assert_eq!(evidence(&result)["snapshotId"], saved(&dir)[0]["id"]);
    assert_eq!(
        evidence(&result)["knownBatches"][0]["batchAcknowledged"],
        true
    );
    assert_eq!(evidence(&result)["knownBatches"][0]["successful"], 3);
    assert_eq!(dir.receipts()[0]["phase"], "aborted");
    assert_eq!(f.count("change"), 0);
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn initial_read_error_has_no_snapshot_receipt_or_mutation_and_matches_source() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    *f.reject.lock().unwrap() = Some("GetInputList".into());
    let mut service = f.service(&dir).await;
    let expected = source(spec(), current(), Some("GetInputList"), None);
    let result = service
        .handle()
        .apply_scene(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(result, expected["result"]);
    assert!(saved(&dir).is_empty());
    assert!(dir.receipts().is_empty());
    assert_eq!(f.count("create") + f.count("change"), 0);
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}

#[tokio::test]
async fn lost_or_malformed_reply_retains_unknown_and_restarted_service_has_no_effect_replay() {
    for second in [false, true] {
        for malformed in [false, true] {
            let f = Fixture::new().await;
            let dir = Dir::new();
            let endpoint = f.url.clone();
            let mut service = f.service(&dir).await;
            let job;
            if second {
                f.hold("create");
                job = apply(service.handle(), create_spec(), Cancellation::default());
                f.seen("create", 1).await;
            } else {
                job = apply(service.handle(), create_spec(), Cancellation::default());
            }
            if malformed {
                f.malformed.store(true, Ordering::Release);
            } else {
                f.lost.store(true, Ordering::Release);
            }
            if second {
                f.release("create").await;
            }
            let result = job.await.unwrap().unwrap();
            assert_eq!(result["isError"], true);
            assert!(text(&result).contains("uncertain"));
            assert_eq!(evidence(&result)["snapshotId"], saved(&dir)[0]["id"]);
            assert_eq!(dir.receipts()[0]["phase"], "unknown");
            if second {
                assert_eq!(evidence(&result)["knownBatches"][0]["stage"], "creations");
                assert_eq!(f.count("change"), 1);
            } else {
                assert_eq!(f.count("change"), 0);
            }
            assert_eq!(f.count("create"), 1);
            let joined = service.shutdown().await.unwrap();
            assert!(joined.workers_joined && joined.socket_joined);
            drop(service);
            f.joined().await;
            let next = Fixture::new_at(endpoint.strip_prefix("ws://").unwrap()).await;
            let mut restarted = next.service(&dir).await;
            let mut plan = create_spec();
            plan["apply"] = json!(false);
            let result = restarted
                .handle()
                .apply_scene(request(plan), "fixture".into(), Cancellation::default())
                .await
                .unwrap();
            assert_eq!(result["structuredContent"]["applied"], false);
            let result = restarted
                .handle()
                .apply_scene(
                    request(create_spec()),
                    "fixture".into(),
                    Cancellation::default(),
                )
                .await
                .unwrap();
            assert_eq!(result["isError"], true);
            assert!(text(&result).contains("Unresolved receipt custody"));
            assert_eq!(next.count("create") + next.count("change"), 0);
            assert_eq!(dir.receipts().len(), 1);
            assert_eq!(dir.receipts()[0]["phase"], "unknown");
            assert!(restarted.shutdown().await.unwrap().joined);
            next.joined().await;
        }
    }
}
#[tokio::test]
async fn receipt_publication_failure_preserves_known_ack_facts_and_saved_snapshot_without_retry() {
    for creation in [false, true] {
        let f = Fixture::new().await;
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        let tag = if creation { "create" } else { "change" };
        f.hold(tag);
        let job = apply(
            service.handle(),
            if creation { create_spec() } else { spec() },
            Cancellation::default(),
        );
        f.seen(tag, 1).await;
        let receipts = dir.receipts();
        assert_eq!(receipts.len(), 1);
        let path = dir.0.join("operation-receipts").join(format!(
            "{}.json",
            receipts[0]["operationId"].as_str().unwrap()
        ));
        fs::write(path, b"{}").unwrap();
        f.release(tag).await;
        let result = job.await.unwrap().unwrap();
        assert_eq!(result["isError"], true);
        assert_eq!(evidence(&result)["receiptUncertain"], true);
        assert_eq!(evidence(&result)["snapshotId"], saved(&dir)[0]["id"]);
        assert_eq!(
            evidence(&result)["knownBatches"][0]["stage"],
            if creation { "creations" } else { "changes" }
        );
        assert_eq!(
            evidence(&result)["knownBatches"][0]["batchAcknowledged"],
            true
        );
        assert_eq!(evidence(&result)["postconditionObserved"], false);
        assert_eq!(evidence(&result)["renderedComposition"], "unverified");
        assert!(!evidence(&result).to_string().contains("file"));
        assert!(!text(&result).contains("Nothing changed"));
        if creation {
            assert_eq!(f.count("change"), 0);
        }
        assert!(
            service
                .handle()
                .apply_scene(request(spec()), "fixture".into(), Cancellation::default())
                .await
                .is_err()
        );
        assert!(service.shutdown().await.unwrap().workers_joined);
        f.joined().await;
    }
}
#[tokio::test]
async fn receipt_intent_failure_after_snapshot_keeps_snapshot_evidence_and_emits_no_effect() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold_nth("input-read", 2);
    let job = apply(service.handle(), spec(), Cancellation::default());
    f.seen("input-read", 2).await;
    fs::write(dir.0.join("operation-receipts"), b"blocked namespace").unwrap();
    f.release("input-read").await;
    let result = job.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(evidence(&result)["snapshotId"], saved(&dir)[0]["id"]);
    assert_eq!(f.count("create") + f.count("change"), 0);
    assert!(service.shutdown().await.unwrap().workers_joined);
    f.joined().await;
}
#[tokio::test]
async fn held_change_ack_cancel_keeps_known_changes_and_final_read_failure_never_claims_no_change()
{
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("change");
    let cancel = Cancellation::default();
    let job = apply(service.handle(), spec(), cancel.clone());
    f.seen("change", 1).await;
    cancel.cancel();
    f.release("change").await;
    let result = job.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(evidence(&result)["knownBatches"][0]["stage"], "changes");
    assert_eq!(
        evidence(&result)["knownBatches"][0]["batchAcknowledged"],
        true
    );
    assert_eq!(dir.receipts()[0]["phase"], "closed");
    assert!(!text(&result).contains("Nothing changed"));
    assert_eq!(f.count("change"), 1);
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn creation_halt_failures_and_blank_audio_or_video_warnings_match_source() {
    for case in ["halt", "audio", "blank"] {
        let f = Fixture::new().await;
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        let (args, reject) = match case {
            "halt" => (create_spec(), Some("CreateInput")),
            "audio" => (
                json!({"sceneName":"Scene","sources":[{"name":"Fresh","kind":"ffmpeg_source","settings":{"local_file":"tone.wav"},"locked":true}],"apply":true}),
                None,
            ),
            _ => (
                json!({"sceneName":"Scene","sources":[{"name":"Fresh","kind":"window_capture","locked":true}],"apply":true}),
                None,
            ),
        };
        *f.reject.lock().unwrap() = reject.map(str::to_owned);
        let expected = source(args.clone(), current(), reject, None);
        let result = service
            .handle()
            .apply_scene(request(args), "fixture".into(), Cancellation::default())
            .await
            .unwrap();
        assert_eq!(
            normalized(result.clone(), &saved(&dir)),
            normalized(
                expected["result"].clone(),
                expected["snapshots"].as_array().unwrap()
            )
        );
        assert_eq!(writes(&f), expected["writes"]);
        if case == "halt" {
            assert_eq!(f.count("change"), 0);
            assert_eq!(dir.receipts()[0]["phase"], "aborted");
        }
        assert!(service.shutdown().await.unwrap().joined);
        f.joined().await;
    }
}
#[tokio::test]
async fn same_holder_actual_locked_take_keeps_recording_stop_and_file_ownership_separate() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service_with_take(&dir, true).await;
    let h = service.handle();
    f.control
        .lock()
        .unwrap()
        .claim("fixture", 1, "", EPOCH)
        .unwrap();
    let started = h
        .start_take(
            StartOptions {
                lock: true,
                max_duration_seconds: Some(30),
                ..Default::default()
            },
            Policy {
                caller: "fixture".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_ne!(started["isError"], true, "{started}");
    assert_eq!(started["structuredContent"]["locked"], true);
    let result = h
        .apply_scene(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    assert_ne!(result["isError"], true, "{result}");
    assert_eq!(f.count("StartRecord"), 1);
    assert_eq!(f.count("StopRecord"), 0);
    assert_eq!(f.host.inspected.load(Ordering::Relaxed), 0);
    let stopped = h
        .stop_take(
            None,
            Policy {
                caller: "fixture".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        stopped["structuredContent"]["stopConfirmed"], true,
        "{stopped}"
    );
    assert_eq!(f.count("StopRecord"), 1);
    assert!(service.shutdown().await.unwrap().joined);
    assert!(f.host.joined.load(Ordering::Acquire));
    f.joined().await;
}

#[tokio::test]
async fn queued_change_frame_rechecks_original_caller_after_snapshot_and_replan() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let h = service.handle();
    f.hold_nth("input-read", 3);
    let job = apply(h.clone(), spec(), Cancellation::default());
    f.seen("input-read", 3).await;
    f.hold("GetStats");
    let reader = h.clone();
    let reading =
        tokio::spawn(async move { reader.read(ReadRequest::Stats, Value::Null, WAIT).await });
    tokio::task::yield_now().await;
    f.release("input-read").await;
    f.seen("GetStats", 1).await;
    timeout(WAIT, async {
        loop {
            if dir
                .receipts()
                .iter()
                .any(|r| r["phase"] == "dispatching-changes")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.control
        .lock()
        .unwrap()
        .claim("other-client", 1, "", EPOCH)
        .unwrap();
    f.release("GetStats").await;
    reading.await.unwrap().unwrap();
    let result = job.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(text(&result).starts_with("Not changed: other-client has control"));
    assert_eq!(evidence(&result)["snapshotId"], saved(&dir)[0]["id"]);
    assert_eq!(f.count("change"), 0);
    let receipts = dir.receipts();
    assert_eq!(receipts[0]["phase"], "aborted");
    assert_eq!(receipts[0]["changes"]["batchAcknowledged"], Value::Null);
    assert_eq!(receipts[0]["changes"]["counts"], Value::Null);
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn original_deadline_joins_held_creation_outcome_as_unknown_without_a_new_budget_or_retry() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f
        .service_with_limit(&dir, false, Duration::from_secs(2))
        .await;
    f.hold("create");
    let job = apply(service.handle(), create_spec(), Cancellation::default());
    f.seen("create", 1).await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(text(&result).contains("uncertain"));
    assert_eq!(evidence(&result)["snapshotId"], saved(&dir)[0]["id"]);
    assert!(evidence(&result).get("knownBatches").is_none());
    assert_eq!(f.count("create"), 1);
    assert_eq!(f.count("change"), 0);
    assert_eq!(dir.receipts()[0]["phase"], "unknown");
    assert_eq!(dir.receipts()[0]["creations"]["counts"], Value::Null);
    let report = service.shutdown().await.unwrap();
    assert!(report.workers_joined && report.socket_joined);
    f.joined().await;
}
