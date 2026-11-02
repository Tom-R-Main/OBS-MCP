// SPDX-License-Identifier: GPL-2.0-only
//! Actual Service/Session workflows over one owned mock socket; no public route.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    capture_window::Request,
    clock,
    control::ControlDomain,
    custody::SharedControl,
    observation::ReadRequest,
    operation_store::OperationStore,
    preflight::{self, Report},
    scene_audit::ScreenshotLimit,
    service::{
        ConnectionDescriptor, Options, PreparationOwners, REQUIRED_SUBSCRIPTIONS, Resources,
        Service, ServiceHandle, TakeServices,
    },
    session::{ObsSession, SessionOptions},
    session_observation::ObservationBridge,
    stage_transport::Cancellation,
    take_monitor::Summary,
    take_transport::{Bounds, HostServices, PreflightReader},
    takes::{ClipCheck, IoFuture, Limits},
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
                "capture-service-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&p).unwrap();
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
    json!({"platform":"macos","program":"Scene","nextId":3,"video":{"baseWidth":1920,"baseHeight":1080},"properties":{"propertyItems":[{"itemName":"Editor","itemValue":42,"itemEnabled":true,"unknown":{"revision":1}}]},"inputs":{"Input":{"inputUuid":"input-original","inputKind":"screen_capture","inputSettings":{"type":1,"window":20,"untouched":true},"inputMuted":false,"inputVolumeMul":1,"inputAudioTracks":{"1":true}}},"scenes":{"Scene":[{"sceneItemId":1,"sourceName":"Input","sourceUuid":"input-original","sceneItemIndex":0,"sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemTransform":{"positionX":0}}],"Other":[]}})
}
fn request(args: Value) -> Request {
    Request::parse(&args).unwrap()
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
    if data.get("inputUuid").is_some()
        && state["inputs"].get(name).is_some()
        && data["inputUuid"] != state["inputs"][name]["inputUuid"]
    {
        return no(600, "UUID mismatch");
    }
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
            json!({"platform":state["platform"],"availableRequests":["GetInputKindList","GetInputPropertiesListPropertyItems","GetSceneItemId","GetSourceScreenshot","GetVersion","GetSceneList","GetInputList","GetVideoSettings","GetSceneItemList","GetInputSettings","GetInputMute","GetInputVolume","GetInputAudioTracks","GetStats","GetRecordStatus","GetCurrentProgramScene","GetProfileParameter","StartRecord","StopRecord","CreateRecordChapter","CreateScene","CreateInput","CreateSceneItem","SetInputSettings","SetInputMute","SetInputVolume","SetInputAudioTracks","SetSceneItemTransform","SetSceneItemEnabled","SetSceneItemLocked","SetSceneItemIndex","RemoveSceneItem"],"supportedImageFormats":[]}),
        ),
        "GetSceneList" => yes(
            json!({"scenes":state["scenes"].as_object().unwrap().keys().map(|name|json!({"sceneName":name,"sceneUuid":state["sceneIds"].get(name).cloned().unwrap_or_else(||json!(name))})).collect::<Vec<_>>(),"currentProgramSceneName":state["program"]}),
        ),
        "GetInputList" => yes(
            json!({"inputs":state["inputs"].as_object().unwrap().iter().map(|(name,value)|json!({"inputName":name,"inputUuid":value["inputUuid"],"inputKind":value["inputKind"]})).collect::<Vec<_>>()}),
        ),
        "GetVideoSettings" => yes(state["video"].clone()),
        "GetInputKindList" => yes(
            json!({"inputKinds":["xcomposite_input","pipewire-window-capture-source","pipewire-screen-capture-source"]}),
        ),
        "GetInputPropertiesListPropertyItems" => yes(state["properties"].clone()),
        "GetSceneItemId" => yes(
            json!({"sceneItemId":state["scenes"][scene].as_array().unwrap().iter().find(|r|r["sourceName"]==data["sourceName"]).map(|r|r["sceneItemId"].clone()).unwrap_or(Value::Null)}),
        ),
        "GetSourceScreenshot" => yes(
            json!({"imageData":state.get("screenshotData").cloned().unwrap_or_else(||json!("data:image/jpeg;base64,YQ=="))}),
        ),
        "GetSceneItemList" => match state["scenes"].get(scene).and_then(Value::as_array) {
            Some(items) => yes(
                json!({"sceneItems":items.iter().enumerate().map(|(index,item)|{let mut item=item.clone();item["sceneItemIndex"]=json!(index);let kind=state["inputs"][item["sourceName"].as_str().unwrap()]["inputKind"].as_str().unwrap_or("");let blank=kind=="window_capture"||kind.ends_with("_input_capture")||matches!(kind,"ffmpeg_source"|"vlc_source");item["inputKind"]=json!(kind);item["sceneItemTransform"]["sourceWidth"]=json!(if blank{0}else{state["sourceWidth"].as_u64().unwrap_or(800)});item["sceneItemTransform"]["sourceHeight"]=json!(if blank{0}else{state["sourceHeight"].as_u64().unwrap_or(600)});item}).collect::<Vec<_>>()}),
            ),
            None => no(600, "scene absent"),
        },
        "GetStats" => {
            yes(json!({"availableDiskSpace":50000,"renderSkippedFrames":0,"renderTotalFrames":0}))
        }
        "GetRecordStatus" => yes(
            json!({"outputActive":state["recording"].as_bool().unwrap_or(false),"outputPaused":false,"outputBytes":1048576}),
        ),
        "GetCurrentProgramScene" => yes(
            json!({"currentProgramSceneName":state["program"],"currentProgramSceneUuid":state["sceneIds"].get(state["program"].as_str().unwrap()).cloned().unwrap_or_else(||state["program"].clone())}),
        ),
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
                let mut value = json!({"inputUuid":"created-uuid","inputKind":input_kind,"inputSettings":data.get("inputSettings").cloned().unwrap_or_else(||json!({}))});
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
            let uuid = state["inputs"][input]["inputUuid"].clone();
            let Some(items) = state["scenes"].get_mut(scene).and_then(Value::as_array_mut) else {
                return no(600, "scene absent");
            };
            items.push(json!({"sceneItemId":id,"sourceName":input,"sourceUuid":uuid,"sceneItemEnabled":data["sceneItemEnabled"]!=false,"sceneItemLocked":false,"sceneItemTransform":{"positionX":0,"positionY":0,"scaleX":1,"scaleY":1,"rotation":0,"alignment":5,"boundsType":"OBS_BOUNDS_NONE"}}));
            yes(json!({"sceneItemId":id,"inputUuid":"created-uuid"}))
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
    after_snapshot: Arc<Mutex<Option<PathBuf>>>,
    screenshot_limit: Mutex<Option<ScreenshotLimit>>,
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
        let after_snapshot = Arc::new(Mutex::new(None::<PathBuf>));
        let snapshot_gate = after_snapshot.clone();
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
                let count=history.lock().unwrap().iter().filter(|f|key(f)==tag).count();let saved_gate=tag=="GetInputList"&&snapshot_gate.lock().unwrap().as_ref().is_some_and(|p|p.exists());if saved_gate{snapshot_gate.lock().unwrap().take();}let should_hold=saved_gate||holding.lock().unwrap().get(&tag).copied()==Some(count);if should_hold{holding.lock().unwrap().remove(&tag);pending.lock().unwrap().insert(tag,reply);}else{peer.send(Message::Text(reply.to_string().into())).await.unwrap();}wake.notify_waiters();},Some(Ok(Message::Close(_)))|Some(Err(_))|None=>{let _=peer.flush().await;break;},_=>{}}
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
            after_snapshot,
            screenshot_limit: Mutex::new(None),
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
                generic_readback_screenshot_limit: self.screenshot_limit.lock().unwrap().map(Ok),
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
fn call(
    handle: ServiceHandle,
    args: Value,
    cancel: Cancellation,
) -> JoinHandle<Result<Value, obs_mcp_native::service::ServiceError>> {
    tokio::spawn(async move {
        handle
            .capture_window(request(args), "fixture".into(), cancel)
            .await
    })
}
fn spec() -> Value {
    json!({"sceneName":"Scene","inputName":"Input","window":"Editor"})
}

// Match the actual Mac ordering: the retarget and audio ACKs arrive first,
// then delayed settings/geometry events precede a discovery-read reply.
async fn late_owned_capture_events(
    conflict: bool,
    reject_read: bool,
) -> (Value, Vec<Value>, Vec<Value>) {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("change");
    let job = call(service.handle(), spec(), Cancellation::default());
    f.seen("change", 1).await;
    let next = f.count("GetInputPropertiesListPropertyItems") + 1;
    f.hold("GetInputPropertiesListPropertyItems");
    f.release("change").await;
    f.seen("GetInputPropertiesListPropertyItems", next).await;
    if reject_read {
        let mut held = f.held.lock().unwrap();
        let reply = held.get_mut("GetInputPropertiesListPropertyItems").unwrap();
        reply["d"]["requestStatus"] =
            json!({"result":false,"code":601,"comment":"fixture property refusal"});
        reply["d"]["responseData"] = json!({});
    }
    if conflict {
        f.state.lock().unwrap()["inputs"]["Input"]["inputSettings"]["untouched"] = json!(false);
    }
    let settings = f.state.lock().unwrap()["inputs"]["Input"]["inputSettings"].clone();
    f.out.send(json!({"op":5,"d":{"eventType":"InputSettingsChanged","eventIntent":8,"eventData":{"inputName":"Input","inputUuid":"input-original","inputSettings":settings}}})).await.unwrap();
    f.out.send(json!({"op":5,"d":{"eventType":"SceneItemTransformChanged","eventIntent":524288,"eventData":{"sceneName":"Scene","sceneUuid":"Scene","sceneItemId":1,"sceneItemTransform":{"sourceWidth":960,"sourceHeight":540}}}})).await.unwrap();
    f.release("GetInputPropertiesListPropertyItems").await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    let history = effects(&f);
    let receipts = dir.receipts();
    let reads = f.count("GetInputPropertiesListPropertyItems");
    finish(&mut service, f).await;
    if reject_read {
        assert_eq!(
            reads, next,
            "OBS rejection must not become a resampling opportunity"
        );
    }
    (result, history, receipts)
}

#[tokio::test]
async fn late_owned_events_after_audio_ack_resample_without_replaying_effects() {
    let (result, history, receipts) = late_owned_capture_events(false, false).await;
    assert_ne!(result["isError"], true, "{result}");
    assert_eq!(
        history
            .iter()
            .map(|row| row["requestType"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "SetInputSettings",
            "SetInputMute",
            "SetInputAudioTracks",
            "SetSceneItemTransform"
        ]
    );
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0]["phase"], "closed");
    assert_eq!(receipts[0]["steps"][0]["counts"]["successful"], 1);
    assert_eq!(receipts[0]["steps"][1]["counts"]["successful"], 2);
    assert_eq!(receipts[0]["steps"][2]["counts"]["successful"], 1);
}

#[tokio::test]
async fn late_events_never_mask_an_actual_capture_settings_conflict() {
    let (result, history, receipts) = late_owned_capture_events(true, false).await;
    assert_eq!(result["isError"], true, "{result}");
    assert_eq!(
        history
            .iter()
            .map(|row| row["requestType"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["SetInputSettings", "SetInputMute", "SetInputAudioTracks"]
    );
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0]["phase"], "aborted");
    assert_eq!(receipts[0]["steps"][2]["phase"], "not-dispatched");
    assert_eq!(
        result["structuredContent"]["captureEvidence"]["receiptUncertain"],
        false
    );
}

#[tokio::test]
async fn continued_post_ack_revision_churn_retires_after_four_samples_without_effect_replay() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("change");
    let job = call(service.handle(), spec(), Cancellation::default());
    f.seen("change", 1).await;
    let next = f.count("GetInputPropertiesListPropertyItems") + 1;
    f.hold("GetInputPropertiesListPropertyItems");
    f.release("change").await;
    for offset in 0..4 {
        f.seen("GetInputPropertiesListPropertyItems", next + offset)
            .await;
        if offset < 3 {
            f.hold("GetInputPropertiesListPropertyItems");
        }
        f.out.send(json!({"op":5,"d":{"eventType":"InputSettingsChanged","eventIntent":8,"eventData":{"inputName":"Input","inputUuid":"input-original","inputSettings":{"type":1,"window":42,"untouched":true}}}})).await.unwrap();
        f.release("GetInputPropertiesListPropertyItems").await;
    }
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    let history = effects(&f);
    let reads = f.count("GetInputPropertiesListPropertyItems");
    let receipts = dir.receipts();
    finish(&mut service, f).await;
    assert_eq!(result["isError"], true, "{result}");
    assert_eq!(reads, next + 3);
    assert_eq!(
        history
            .iter()
            .map(|row| row["requestType"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["SetInputSettings", "SetInputMute", "SetInputAudioTracks"]
    );
    assert_eq!(receipts[0]["phase"], "aborted");
    assert_eq!(receipts[0]["steps"][2]["phase"], "not-dispatched");
    assert_eq!(
        result["structuredContent"]["captureEvidence"]["receiptUncertain"],
        false
    );
}
fn text(value: &Value) -> String {
    value["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}
fn effects(f: &Fixture) -> Vec<Value> {
    f.trace
        .lock()
        .unwrap()
        .iter()
        .flat_map(|frame| {
            if frame["op"] == 6 {
                vec![frame["d"].clone()]
            } else {
                frame["d"]["requests"].as_array().unwrap().clone()
            }
        })
        .filter(|r| !r["requestType"].as_str().unwrap().starts_with("Get"))
        .collect()
}
async fn finish(service: &mut Service, f: Fixture) {
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn ordinary_existing_capture_saves_before_three_acknowledged_stages_and_preserves_uuid_fields()
 {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let result = service
        .handle()
        .capture_window(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    let history = effects(&f);
    let receipts = dir.receipts();
    let state = f.state.lock().unwrap().clone();
    finish(&mut service, f).await;
    assert_ne!(result["isError"], true, "{result}");
    assert!(text(&result).contains("Muted the capture"));
    assert!(text(&result).contains("Fitted to the 1920×1080 canvas"));
    assert_eq!(
        history
            .iter()
            .map(|r| r["requestType"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "SetInputSettings",
            "SetInputMute",
            "SetInputAudioTracks",
            "SetSceneItemTransform"
        ]
    );
    for effect in &history[..3] {
        assert_eq!(effect["requestData"]["inputUuid"], "input-original");
    }
    assert_eq!(history[3]["requestData"]["sceneUuid"], "Scene");
    assert_eq!(
        state["inputs"]["Input"]["inputSettings"],
        json!({"type":1,"window":42,"untouched":true})
    );
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0]["version"], 5);
    assert_eq!(receipts[0]["phase"], "closed");
    assert_eq!(
        result["structuredContent"]["captureEvidence"]["knownSteps"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert!(dir.0.join("snapshots.json").exists());
    let receipt = receipts[0].to_string();
    for private in [
        "Editor",
        "input-original",
        "untouched",
        "capture_window",
        "portal",
    ] {
        assert!(!receipt.contains(private), "metadata leaked {private}");
    }
}
#[tokio::test]
async fn plan_only_and_portal_or_selector_errors_have_no_snapshot_or_effect() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let h = service.handle();
    let mut plan = spec();
    plan["apply"] = json!(false);
    let result = h
        .capture_window(request(plan), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    assert_eq!(result["structuredContent"]["applied"], false, "{result}");
    for args in [
        json!({"sceneName":"Scene","inputName":"Input"}),
        json!({"sceneName":"Scene","inputName":"Input","backend":"portal"}),
    ] {
        let r = h
            .capture_window(request(args), "fixture".into(), Cancellation::default())
            .await
            .unwrap();
        assert_eq!(r["isError"], true);
    }
    let empty = effects(&f).is_empty();
    finish(&mut service, f).await;
    assert!(empty);
    assert!(dir.receipts().is_empty());
    assert!(!dir.0.join("snapshots.json").exists());
}
#[tokio::test]
async fn create_uses_separate_inventory_then_owns_new_uuid_without_snapshot_or_replacement_authority()
 {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let mut args = spec();
    args["inputName"] = json!("Fresh");
    let result = service
        .handle()
        .capture_window(request(args), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    let history = effects(&f);
    finish(&mut service, f).await;
    assert_ne!(result["isError"], true, "{result}");
    assert_eq!(history[0]["requestType"], "CreateInput");
    assert_eq!(history[1]["requestData"]["inputUuid"], "created-uuid");
    assert!(!dir.0.join("snapshots.json").exists());
    assert_eq!(dir.receipts()[0]["plan"]["initial"], "create");
}
#[tokio::test]
async fn readonly_and_foreign_lease_policy_precede_discovery_even_plan_only() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.control
        .lock()
        .unwrap()
        .claim("human", 60, "", EPOCH)
        .unwrap();
    let before = f.trace.lock().unwrap().len();
    let mut args = spec();
    args["apply"] = json!(false);
    let result = service
        .handle()
        .capture_window(request(args), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    let after = f.trace.lock().unwrap().len();
    finish(&mut service, f).await;
    assert!(text(&result).contains("human has control"), "{result}");
    assert_eq!(before, after);
    assert!(dir.receipts().is_empty());
}
#[tokio::test]
async fn cancellation_during_initial_ack_joins_and_preserves_saved_snapshot_and_no_audio_or_fit() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("SetInputSettings");
    let cancel = Cancellation::default();
    let mut job = call(service.handle(), spec(), cancel.clone());
    f.seen("SetInputSettings", 1).await;
    cancel.cancel();
    assert!(poll!(&mut job).is_pending());
    assert!(dir.0.join("snapshots.json").exists());
    f.release("SetInputSettings").await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    let history = effects(&f);
    finish(&mut service, f).await;
    assert_eq!(history.len(), 1);
    assert_eq!(result["isError"], true);
    assert!(result["structuredContent"]["captureEvidence"]["snapshotId"].is_string());
    assert_eq!(
        result["structuredContent"]["captureEvidence"]["knownSteps"][0]["successful"],
        1
    );
    assert_eq!(dir.receipts()[0]["phase"], "aborted");
}
#[tokio::test]
async fn shutdown_during_audio_ack_joins_same_batch_and_skips_fit() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("change");
    let job = call(service.handle(), spec(), Cancellation::default());
    f.seen("change", 1).await;
    let mut stopping = Box::pin(service.shutdown());
    assert!(poll!(stopping.as_mut()).is_pending());
    f.release("change").await;
    let joined = timeout(WAIT, stopping).await.unwrap().unwrap();
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    let history = effects(&f);
    f.joined().await;
    assert!(joined.joined);
    assert_eq!(history.len(), 3);
    assert_eq!(result["isError"], true);
    assert_eq!(dir.receipts()[0]["steps"][1]["counts"]["successful"], 2);
    assert_eq!(dir.receipts()[0]["phase"], "aborted");
}
#[tokio::test]
async fn failed_audio_ack_retains_prior_retarget_and_snapshot_without_fit_or_silence_claim() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    *f.reject.lock().unwrap() = Some("SetInputAudioTracks".into());
    let result = service
        .handle()
        .capture_window(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    let history = effects(&f);
    finish(&mut service, f).await;
    assert_eq!(history.len(), 3);
    assert_eq!(result["isError"], true);
    assert!(!text(&result).contains("Muted the capture and removed"));
    assert!(result["structuredContent"]["captureEvidence"]["snapshotId"].is_string());
    let receipt = &dir.receipts()[0];
    assert_eq!(receipt["steps"][0]["counts"]["successful"], 1);
    assert_eq!(
        receipt["steps"][1]["counts"],
        json!({"successful":1,"failed":1,"skipped":0})
    );
    assert_eq!(receipt["phase"], "aborted");
}
#[tokio::test]
async fn replacement_while_initial_ack_is_held_prevents_any_replacement_settings_or_later_effect() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("SetInputSettings");
    let job = call(service.handle(), spec(), Cancellation::default());
    f.seen("SetInputSettings", 1).await;
    f.state.lock().unwrap()["inputs"]["Input"]["inputUuid"] = json!("human-replacement");
    let before = f.count("GetInputSettings");
    f.release("SetInputSettings").await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    let after = f.count("GetInputSettings");
    let history = effects(&f);
    finish(&mut service, f).await;
    assert_eq!(result["isError"], true);
    assert_eq!(history.len(), 1);
    assert_eq!(before, after, "replacement inspected");
    assert!(
        !text(&result).contains("No capture changed"),
        "prior ACK must not be erased by a discovery refusal"
    );
    assert_eq!(dir.receipts()[0]["steps"][0]["counts"]["successful"], 1);
}
#[tokio::test]
async fn empty_audio_ack_is_unknown_durable_and_never_fit_or_report_silenced() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.malformed.store(true, Ordering::Release);
    let result = service
        .handle()
        .capture_window(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    let history = effects(&f);
    let error = service
        .handle()
        .capture_window(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap_err();
    finish(&mut service, f).await;
    assert_eq!(result["isError"], true);
    assert!(!text(&result).contains("Muted the capture and removed"));
    assert_eq!(history.len(), 3);
    assert_eq!(error, obs_mcp_native::service::ServiceError::Uncertain);
    assert_eq!(dir.receipts()[0]["phase"], "unknown");
}
#[tokio::test]
async fn portal_video_only_noop_preserves_picker_settings_and_requires_no_receipt() {
    let f = Fixture::new().await;
    {
        let mut s = f.state.lock().unwrap();
        s["platform"] = json!("linux");
        s["inputs"]["Input"]["inputKind"] = json!("pipewire-window-capture-source");
        s["inputs"]["Input"]["inputSettings"] =
            json!({"RestoreToken":"configured","untouched":true});
    }
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let args = json!({"sceneName":"Scene","inputName":"Input","backend":"portal","fit":false});
    let result = service
        .handle()
        .capture_window(request(args), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    let history = effects(&f);
    let enumerations = f.count("GetInputPropertiesListPropertyItems");
    finish(&mut service, f).await;
    assert_ne!(result["isError"], true, "{result}");
    assert!(text(&result).contains("existing portal selection"));
    assert!(history.is_empty());
    assert_eq!(enumerations, 0);
    assert!(dir.receipts().is_empty());
    assert!(dir.0.join("snapshots.json").exists());
}
#[tokio::test]
async fn xcomposite_unconfigured_is_never_enumerated_while_zero_string_is_safe_and_full_xid_survives()
 {
    for configured in [false, true] {
        let f = Fixture::new().await;
        {
            let mut s = f.state.lock().unwrap();
            s["platform"] = json!("linux");
            s["inputs"]["Input"]["inputKind"] = json!("xcomposite_input");
            s["inputs"]["Input"]["inputSettings"] = if configured {
                json!({"capture_window":"0"})
            } else {
                json!({"capture_window":null})
            };
            s["properties"] = json!({"propertyItems":[{"itemName":"Editor","itemValue":"4294967295\r\nEditor\r\nClass","unknown":7}]});
        }
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        let result = service
            .handle()
            .capture_window(request(spec()), "fixture".into(), Cancellation::default())
            .await
            .unwrap();
        let count = f.count("GetInputPropertiesListPropertyItems");
        let history = effects(&f);
        finish(&mut service, f).await;
        if configured {
            assert_ne!(result["isError"], true, "{result}");
            assert!(count > 0);
            assert_eq!(
                history[0]["requestData"]["inputSettings"]["capture_window"],
                "4294967295\r\nEditor\r\nClass"
            );
            assert_eq!(history.len(), 2);
        } else {
            assert_eq!(result["isError"], true);
            assert_eq!(count, 0);
            assert!(history.is_empty());
        }
    }
}
#[tokio::test]
async fn actor_summary_during_capture_is_accessible_but_second_mutation_cannot_steal_owners() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("SetInputSettings");
    let job = call(service.handle(), spec(), Cancellation::default());
    f.seen("SetInputSettings", 1).await;
    let status = service.handle().summary().await.unwrap();
    let second = service
        .handle()
        .capture_window(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap_err();
    f.release("SetInputSettings").await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    finish(&mut service, f).await;
    assert!(status.is_object());
    assert_eq!(second, obs_mcp_native::service::ServiceError::Busy);
    assert_ne!(result["isError"], true);
    assert_eq!(dir.receipts().len(), 1);
}
#[tokio::test]
async fn original_deadline_crossed_during_held_initial_ack_is_not_renewed_for_audio() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f
        .service_with_limit(&dir, false, Duration::from_secs(2))
        .await;
    f.hold("SetInputSettings");
    let job = call(service.handle(), spec(), Cancellation::default());
    f.seen("SetInputSettings", 1).await;
    // ACK delivery deliberately crosses the existing operation deadline. This is
    // a deadline control, not a generic readiness wait or an increased timeout.
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let held = f.held.lock().unwrap().remove("SetInputSettings").unwrap();
    let _ = f.out.send(held).await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    let history = effects(&f);
    finish(&mut service, f).await;
    assert_eq!(result["isError"], true);
    assert_eq!(history.len(), 1);
    assert!(result["structuredContent"]["captureEvidence"]["snapshotId"].is_string());
    assert_eq!(dir.receipts()[0]["phase"], "unknown");
}
#[tokio::test]
async fn failed_fit_preserves_initial_and_audio_ack_facts_and_prior_saved_snapshot() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    *f.reject.lock().unwrap() = Some("SetSceneItemTransform".into());
    let result = service
        .handle()
        .capture_window(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    let history = effects(&f);
    finish(&mut service, f).await;
    assert_eq!(result["isError"], true);
    assert_eq!(history.len(), 4);
    let facts = &result["structuredContent"]["captureEvidence"];
    assert!(facts["snapshotId"].is_string());
    assert_eq!(facts["knownSteps"][1]["successful"], 2);
    assert_eq!(facts["knownSteps"][2]["failed"], 1);
    assert_eq!(dir.receipts()[0]["phase"], "aborted");
}
#[tokio::test]
async fn newly_created_target_without_uuid_never_runs_audio_or_fit_and_preserves_known_initial_ack()
{
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("CreateInput");
    let mut args = spec();
    args["inputName"] = json!("Fresh");
    let job = call(service.handle(), args, Cancellation::default());
    f.seen("CreateInput", 1).await;
    f.held.lock().unwrap().get_mut("CreateInput").unwrap()["d"]["responseData"]
        .as_object_mut()
        .unwrap()
        .remove("inputUuid");
    f.release("CreateInput").await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    let history = effects(&f);
    finish(&mut service, f).await;
    assert_eq!(history.len(), 1);
    assert!(text(&result).contains("did not report its UUID"));
    assert_eq!(
        result["structuredContent"]["captureEvidence"]["knownSteps"][0]["successful"],
        1
    );
    assert_eq!(dir.receipts()[0]["phase"], "aborted");
}
#[tokio::test]
async fn transport_loss_during_audio_preserves_initial_ack_and_durable_unknown_on_reopen() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.lost.store(true, Ordering::Release);
    let endpoint = f.url.clone();
    let result = service
        .handle()
        .capture_window(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    finish(&mut service, f).await;
    assert_eq!(result["isError"], true);
    let reopened = OperationStore::new(&dir.0, &endpoint).unwrap();
    assert!(reopened.read_view().admission().unwrap().blocking);
    assert_eq!(dir.receipts()[0]["phase"], "unknown");
    assert_eq!(dir.receipts()[0]["steps"][0]["counts"]["successful"], 1);
}
#[tokio::test]
async fn saved_snapshot_remains_known_when_cancellation_or_foreign_lease_refuses_first_effect() {
    for cancel_after_save in [false, true] {
        let f = Fixture::new().await;
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        *f.after_snapshot.lock().unwrap() = Some(dir.0.join("snapshots.json"));
        let cancellation = Cancellation::default();
        let job = call(service.handle(), spec(), cancellation.clone());
        let ready = timeout(WAIT, async {
            loop {
                let wake = f.notify.notified();
                if f.held.lock().unwrap().contains_key("GetInputList") {
                    break;
                }
                wake.await;
            }
        })
        .await
        .is_ok();
        if ready {
            if cancel_after_save {
                cancellation.cancel();
            } else {
                f.control
                    .lock()
                    .unwrap()
                    .claim("human", 60, "", EPOCH)
                    .unwrap();
            }
            f.release("GetInputList").await;
        } else {
            cancellation.cancel();
        }
        let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
        let history = effects(&f);
        finish(&mut service, f).await;
        assert!(ready, "post-snapshot read was never reached");
        assert_eq!(result["isError"], true);
        assert!(history.is_empty());
        assert!(dir.0.join("snapshots.json").exists());
        assert!(result["structuredContent"]["captureEvidence"]["snapshotId"].is_string());
    }
}
#[tokio::test]
async fn structural_event_after_saved_snapshot_refuses_before_any_capture_write() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    *f.after_snapshot.lock().unwrap() = Some(dir.0.join("snapshots.json"));
    let cancel = Cancellation::default();
    let job = call(service.handle(), spec(), cancel.clone());
    let ready = timeout(WAIT, async {
        loop {
            let wake = f.notify.notified();
            if f.held.lock().unwrap().contains_key("GetInputList") {
                break;
            }
            wake.await;
        }
    })
    .await
    .is_ok();
    if ready {
        f.out.send(json!({"op":5,"d":{"eventType":"InputSettingsChanged","eventIntent":8,"eventData":{"inputName":"Input","inputUuid":"input-original","inputSettings":{"window":20}}}})).await.unwrap();
        f.release("GetInputList").await;
    } else {
        cancel.cancel();
    }
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    let history = effects(&f);
    finish(&mut service, f).await;
    assert!(ready);
    assert_eq!(result["isError"], true);
    assert!(history.is_empty());
    assert!(result["structuredContent"]["captureEvidence"]["snapshotId"].is_string());
    assert!(dir.receipts().is_empty());
}
fn normalized(mut value: Value, id: Option<&str>) -> Value {
    if let Some(fields) = value["structuredContent"].as_object_mut() {
        fields.remove("captureEvidence");
    }
    if let Some(id) = id {
        value = serde_json::from_str(&value.to_string().replace(id, "<snapshot>")).unwrap();
    }
    obs_mcp_native::js_json::normalize_json_numbers(&mut value).unwrap();
    value
}
#[tokio::test]
async fn actual_service_matches_eight_checked_source_oracles_including_shared_nonprogram_and_portal()
 {
    let oracle: Value =
        serde_json::from_str(include_str!("fixtures/capture_window_source.json")).unwrap();
    for name in [
        "mac-plan-unknown-record",
        "mac-create",
        "mac-existing",
        "mac-no-audio-no-fit",
        "nonprogram-existing-shared",
        "x11-create",
        "pipewire-window-capture-source-plan",
        "pipewire-window-capture-source-apply",
    ] {
        let row = oracle["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["name"] == name)
            .unwrap();
        let before = &row["current"];
        let scene = row["args"]["sceneName"].as_str().unwrap();
        let f = Fixture::new().await;
        {
            let mut state = f.state.lock().unwrap();
            state["platform"] = row["platform"].clone();
            state["program"] = row["program"].clone();
            state["nextId"] = json!(7);
            state["inputs"] = json!({});
            state["scenes"] = json!({scene:before["membership"]["sceneItems"]});
            state["sceneIds"] = json!({scene:before["scenes"]["scenes"][0]["sceneUuid"]});
            state["sourceWidth"] = json!(1280);
            state["sourceHeight"] = json!(720);
            let program = state["program"].as_str().unwrap().to_owned();
            if program != scene {
                state["scenes"][&program] = json!([]);
                state["sceneIds"][&program] = json!("program-uuid");
            }
            for input in before["inputs"]["inputs"].as_array().unwrap() {
                let input_name = input["inputName"].as_str().unwrap();
                let candidate = before["candidates"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|r| r["inputUuid"] == input["inputUuid"])
                    .unwrap();
                state["inputs"][input_name] = json!({"inputUuid":input["inputUuid"],"inputKind":input["inputKind"],"inputSettings":candidate["settings"]["inputSettings"],"inputMuted":false,"inputVolumeMul":1,"inputAudioTracks":{"1":true}});
                state["properties"] = candidate["properties"].clone();
            }
        }
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        let result = service
            .handle()
            .capture_window(
                request(row["args"].clone()),
                "fixture".into(),
                Cancellation::default(),
            )
            .await
            .unwrap();
        let history = effects(&f);
        finish(&mut service, f).await;
        let id = result["structuredContent"]["captureEvidence"]["snapshotId"].as_str();
        assert_eq!(
            normalized(result.clone(), id),
            normalized(row["result"].clone(), None),
            "Source result {name}"
        );
        let mut actual = Value::Array(
            history
                .iter()
                .map(|r| json!({"requestType":r["requestType"],"requestData":r["requestData"]}))
                .collect(),
        );
        let mut expected = row["mutations"].clone();
        obs_mcp_native::js_json::normalize_json_numbers(&mut actual).unwrap();
        obs_mcp_native::js_json::normalize_json_numbers(&mut expected).unwrap();
        assert_eq!(actual, expected, "Source mutations {name}");
    }
}
#[tokio::test]
async fn startup_snapshot_rpc_cancellation_joins_without_publishing_snapshot_or_mutating() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("input-read");
    let cancellation = Cancellation::default();
    let mut job = call(service.handle(), spec(), cancellation.clone());
    f.seen("input-read", 1).await;
    cancellation.cancel();
    assert!(poll!(&mut job).is_pending());
    f.release("input-read").await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    let history = effects(&f);
    finish(&mut service, f).await;
    assert_eq!(result["isError"], true);
    assert!(history.is_empty());
    assert!(!dir.0.join("snapshots.json").exists());
    assert!(dir.receipts().is_empty());
}
#[tokio::test]
async fn shared_input_in_nonprogram_target_scene_is_supported_without_switching_program() {
    let f = Fixture::new().await;
    {
        let mut state = f.state.lock().unwrap();
        let mut shared = state["scenes"]["Scene"][0].clone();
        shared["sceneItemId"] = json!(3);
        state["scenes"]["Other"] = json!([shared]);
        state["program"] = json!("Other");
    }
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let result = service
        .handle()
        .capture_window(request(spec()), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    let history = effects(&f);
    let program = f.state.lock().unwrap()["program"].clone();
    finish(&mut service, f).await;
    assert_ne!(result["isError"], true, "{result}");
    assert_eq!(program, "Other");
    assert_eq!(
        result["structuredContent"]["appearances"][0]["sceneName"],
        "Other"
    );
    assert_eq!(history.last().unwrap()["requestData"]["sceneUuid"], "Scene");
    assert!(
        history
            .iter()
            .all(|r| r["requestType"] != "SetCurrentProgramScene")
    );
}
#[tokio::test]
async fn acknowledged_effect_facts_survive_terminal_receipt_publication_failure() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    f.hold("SetInputSettings");
    let job = call(service.handle(), spec(), Cancellation::default());
    f.seen("SetInputSettings", 1).await;
    let entry = fs::read_dir(dir.0.join("operation-receipts"))
        .unwrap()
        .map(|r| r.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "json"))
        .unwrap();
    let parked = entry.with_extension("parked");
    fs::rename(&entry, &parked).unwrap();
    f.release("SetInputSettings").await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    let history = effects(&f);
    finish(&mut service, f).await;
    assert_eq!(result["isError"], true);
    assert_eq!(history.len(), 1);
    assert_eq!(
        result["structuredContent"]["captureEvidence"]["receiptUncertain"],
        true
    );
    assert_eq!(
        result["structuredContent"]["captureEvidence"]["knownSteps"][0]["successful"],
        1
    );
    assert!(result["structuredContent"]["captureEvidence"]["snapshotId"].is_string());
}
#[tokio::test]
async fn post_snapshot_identity_settings_membership_scene_and_full_selection_are_fresh_before_first_effect()
 {
    for changed in ["identity", "settings", "membership", "scene", "selection"] {
        let f = Fixture::new().await;
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        *f.after_snapshot.lock().unwrap() = Some(dir.0.join("snapshots.json"));
        let cancellation = Cancellation::default();
        let job = call(service.handle(), spec(), cancellation.clone());
        let ready = timeout(WAIT, async {
            loop {
                let wake = f.notify.notified();
                if f.held.lock().unwrap().contains_key("GetInputList") {
                    break;
                }
                wake.await;
            }
        })
        .await
        .is_ok();
        let before = f.count("GetInputSettings");
        if ready {
            {
                let mut state = f.state.lock().unwrap();
                match changed {
                    "identity" => {
                        state["inputs"]["Input"]["inputUuid"] = json!("replacement");
                        state["scenes"]["Scene"][0]["sourceUuid"] = json!("replacement");
                        state["scenes"]["Scene"][0]["sceneItemId"] = json!(99);
                        f.held.lock().unwrap().get_mut("GetInputList").unwrap()["d"]["responseData"]
                            ["inputs"][0]["inputUuid"] = json!("replacement");
                    }
                    "settings" => {
                        state["inputs"]["Input"]["inputSettings"]["untouched"] = json!(false)
                    }
                    "membership" => state["scenes"]["Scene"][0]["sceneItemId"] = json!(99),
                    "scene" => state["sceneIds"] = json!({"Scene":"new-scene-uuid"}),
                    _ => state["properties"]["propertyItems"][0]["unknown"]["revision"] = json!(2),
                }
            }
            f.release("GetInputList").await;
        } else {
            cancellation.cancel();
        }
        let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
        let history = effects(&f);
        let after = f.count("GetInputSettings");
        finish(&mut service, f).await;
        assert!(ready, "{changed}");
        assert_eq!(result["isError"], true, "{changed}: {result}");
        assert!(history.is_empty(), "{changed}");
        assert!(
            result["structuredContent"]["captureEvidence"]["snapshotId"].is_string(),
            "{changed}"
        );
        assert!(dir.receipts().is_empty(), "{changed}");
        if changed == "identity" {
            assert_eq!(before, after, "replacement settings were requested");
        }
    }
}
#[tokio::test]
async fn default_allowed_image_larger_than_report_cap_keeps_image_and_separate_bounded_diagnostics()
{
    let f = Fixture::new().await;
    f.state.lock().unwrap()["screenshotData"] =
        json!(format!("data:image/jpeg;base64,{}", "YWFh".repeat(300_000)));
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let mut args = spec();
    args["includeScreenshot"] = json!(true);
    let result = service
        .handle()
        .capture_window(request(args), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    let history = effects(&f);
    finish(&mut service, f).await;
    assert_ne!(result["isError"], true, "{}", text(&result));
    assert!(text(&result).contains("OBS screenshot (image/jpeg, 900000 bytes)"));
    assert_eq!(history.len(), 4);
    let facts = &result["structuredContent"]["captureEvidence"];
    assert!(facts["snapshotId"].is_string());
    assert_eq!(facts["knownSteps"].as_array().unwrap().len(), 3);
    assert!(
        serde_json::to_vec(&result).unwrap().len()
            > obs_mcp_native::capture_window::MAX_REPORT_BYTES
    );
    let mut diagnostic = result.clone();
    let rows = diagnostic["content"].as_array_mut().unwrap();
    assert_eq!(rows.iter().filter(|r| r["type"] == "image").count(), 1);
    rows.retain(|r| r["type"] != "image");
    assert!(
        serde_json::to_vec(&diagnostic).unwrap().len()
            <= obs_mcp_native::capture_window::MAX_REPORT_BYTES
    );
    assert_eq!(dir.receipts()[0]["phase"], "closed");
}
#[tokio::test]
async fn fit_refuses_invalid_canvas_or_wrong_current_item_without_erasing_prior_ack() {
    for invalid_canvas in [false, true] {
        let f = Fixture::new().await;
        {
            let mut state = f.state.lock().unwrap();
            if invalid_canvas {
                state["video"]["baseWidth"] = json!(0);
            } else {
                state["scenes"]["Scene"][0]["sceneItemId"] = json!(-1);
            }
        }
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        let result = service
            .handle()
            .capture_window(request(spec()), "fixture".into(), Cancellation::default())
            .await
            .unwrap();
        let history = effects(&f);
        finish(&mut service, f).await;
        assert_eq!(result["isError"], true);
        assert_eq!(history.len(), 3);
        assert_eq!(
            result["structuredContent"]["captureEvidence"]["knownSteps"][1]["successful"],
            2
        );
        assert_eq!(dir.receipts()[0]["phase"], "aborted");
    }
}
#[tokio::test]
async fn validated_screenshot_preference_is_retained_and_default_still_admits_small_image() {
    for configured in [false, true] {
        let f = Fixture::new().await;
        f.state.lock().unwrap()["screenshotData"] = json!("data:image/jpeg;base64,YWFh");
        if configured {
            *f.screenshot_limit.lock().unwrap() = Some(ScreenshotLimit::parse(Some("1")).unwrap());
        }
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        let mut args = spec();
        args["includeScreenshot"] = json!(true);
        let result = service
            .handle()
            .capture_window(request(args), "fixture".into(), Cancellation::default())
            .await
            .unwrap();
        let image_requests = f.count("GetSourceScreenshot");
        let history = effects(&f);
        finish(&mut service, f).await;
        assert_ne!(
            result["isError"], true,
            "screenshot preference is a readback note: {result}"
        );
        assert_eq!(image_requests, 1);
        assert_eq!(history.len(), 4);
        let has_image = result["content"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["type"] == "image");
        assert_eq!(has_image, !configured);
        if configured {
            assert!(
                text(&result)
                    .contains("No screenshot: OBS screenshot is 3 bytes; limit is 1 bytes"),
                "{result}"
            );
        }
        assert!(
            serde_json::to_vec(&result).unwrap().len()
                < obs_mcp_native::capture_window::MAX_REPORT_BYTES
        );
        assert_eq!(dir.receipts()[0]["phase"], "closed");
    }
}
#[tokio::test]
async fn optional_screenshot_read_or_cumulative_input_budget_failure_is_a_bounded_note_after_known_effects()
 {
    for budget in [false, true] {
        let f = Fixture::new().await;
        if budget {
            *f.screenshot_limit.lock().unwrap() =
                Some(ScreenshotLimit::parse(Some("6291456")).unwrap());
            f.state.lock().unwrap()["screenshotData"] = json!(format!(
                "data:image/jpeg;base64,{}",
                "YWFh".repeat(2_097_000)
            ));
        } else {
            *f.reject.lock().unwrap() = Some("GetSourceScreenshot".into());
        }
        let dir = Dir::new();
        let mut service = f.service(&dir).await;
        let mut args = spec();
        args["includeScreenshot"] = json!(true);
        let result = service
            .handle()
            .capture_window(request(args), "fixture".into(), Cancellation::default())
            .await
            .unwrap();
        let history = effects(&f);
        let images = f.count("GetSourceScreenshot");
        finish(&mut service, f).await;
        assert_ne!(result["isError"], true, "{}", text(&result));
        assert!(text(&result).contains("No screenshot:"));
        if budget {
            assert!(
                text(&result).contains("cumulative read bytes"),
                "{}",
                text(&result)
            );
        }
        assert_eq!(history.len(), 4);
        assert_eq!(images, 1);
        assert!(
            result["content"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["type"] == "text")
        );
        assert!(
            serde_json::to_vec(&result).unwrap().len()
                <= obs_mcp_native::capture_window::MAX_REPORT_BYTES
        );
        assert_eq!(
            result["structuredContent"]["captureEvidence"]["knownSteps"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        assert_eq!(dir.receipts()[0]["phase"], "closed");
    }
}
#[tokio::test]
async fn zero_effect_portal_needs_no_receipt_scope_but_keeps_prior_unknown_and_refuses_later_fit() {
    use obs_mcp_native::operation_store::{
        CaptureInitial, CapturePlan, CaptureStep, CaptureUpdate,
    };
    let f = Fixture::new().await;
    {
        let mut s = f.state.lock().unwrap();
        s["platform"] = json!("linux");
        s["inputs"]["Input"]["inputKind"] = json!("pipewire-window-capture-source");
        s["inputs"]["Input"]["inputSettings"] = json!({"RestoreToken":"configured"});
    }
    let dir = Dir::new();
    let prior = {
        let mut store = OperationStore::new(&dir.0, &f.url).unwrap();
        let r = store
            .prepare_capture(
                "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                1,
                CapturePlan {
                    initial: CaptureInitial::Retarget,
                    audio: false,
                    fit: false,
                },
                "2026-11-01T00:00:00.000Z",
            )
            .unwrap();
        let r = store
            .update_capture(
                &r,
                CaptureUpdate::Dispatch(CaptureStep::Initial),
                "2026-11-01T00:00:00.000Z",
            )
            .unwrap();
        store
            .update_capture(&r, CaptureUpdate::Unknown, "2026-11-01T00:00:00.000Z")
            .unwrap()
            .to_value()
    };
    let mut service = f.service(&dir).await;
    let args = json!({"sceneName":"Scene","inputName":"Input","backend":"portal","fit":false});
    let result = service
        .handle()
        .capture_window(
            request(args.clone()),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    let summary = service.handle().summary().await.unwrap();
    let mut mutation = args;
    mutation["fit"] = json!(true);
    let refused = service
        .handle()
        .capture_window(request(mutation), "fixture".into(), Cancellation::default())
        .await
        .unwrap();
    let history = effects(&f);
    let endpoint = f.url.clone();
    finish(&mut service, f).await;
    assert_ne!(result["isError"], true, "{result}");
    assert!(result["structuredContent"]["captureEvidence"]["snapshotId"].is_string());
    assert_eq!(refused["isError"], true);
    assert!(text(&refused).contains("Unresolved receipt custody"));
    assert!(history.is_empty());
    assert_eq!(dir.receipts(), vec![prior]);
    assert_eq!(summary["operations"][0]["phase"], "unknown");
    let reopened = OperationStore::new(&dir.0, &endpoint).unwrap();
    assert!(reopened.read_view().admission().unwrap().blocking);
}

#[tokio::test]
async fn revision_loss_does_not_retry_an_obs_rejected_read() {
    let (result, history, receipts) = late_owned_capture_events(false, true).await;
    assert_eq!(result["isError"], true, "{result}");
    assert_eq!(history.len(), 3);
    assert_eq!(receipts[0]["phase"], "aborted");
    assert_eq!(receipts[0]["steps"][2]["phase"], "not-dispatched");
}
