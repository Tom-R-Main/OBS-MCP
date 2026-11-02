// SPDX-License-Identifier: GPL-2.0-only
//! Actual Service/Session workflows over one owned mock socket; no public route.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    clock,
    control::ControlDomain,
    custody::SharedControl,
    observation::ReadRequest,
    operation_store::OperationStore,
    preflight::{self, Report},
    restore_workflow::Request,
    service::{
        ConnectionDescriptor, Options, PreparationOwners, REQUIRED_SUBSCRIPTIONS, Resources,
        Service, ServiceHandle, TakeServices,
    },
    session::{ObsSession, SessionOptions},
    session_observation::ObservationBridge,
    snapshot_tools,
    snapshots::{Snapshot, SnapshotFile, SnapshotWriter},
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
                "restore-service-{}-{}",
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
    json!({"inputs":{"Input":{"inputKind":"image_source","inputSettings":{"file":"now"},"inputMuted":false,"inputVolumeMul":0.5,"inputAudioTracks":{"1":true}}},"items":[{"sceneItemId":2,"sourceName":"Other","sceneItemIndex":0,"sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemTransform":{}},{"sceneItemId":1,"sourceName":"Input","sceneItemIndex":1,"sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemTransform":{"positionX":0}}]})
}
fn snapshot(id: &str, endpoint: Option<&str>, before: bool) -> Snapshot {
    let mut v = json!({"id":id,"takenAt":"2026-11-01T12:00:00.000Z","inputs":[{"inputName":"Input","inputKind":"image_source","inputSettings":{"file":if before{"before"}else{"now"}},"muted":before,"volumeMul":if before{0.25}else{0.5},"audioTracks":{"1":!before}}],"items":[{"sceneName":"Scene","sceneItemId":1,"sourceName":"Input","index":if before{0}else{1},"enabled":!before,"locked":before,"transform":{"positionX":if before{10}else{0}}},{"sceneName":"Scene","sceneItemId":2,"sourceName":"Other","index":if before{1}else{0},"enabled":true,"locked":false,"transform":{}}]});
    if let Some(url) = endpoint {
        v["obsUrl"] = json!(url);
    }
    serde_json::from_value(v).unwrap()
}
fn save(dir: &Dir, rows: &[Snapshot]) {
    let mut writer = SnapshotFile::new(&dir.0, 8 * 1024 * 1024).unwrap();
    for row in rows {
        writer.save(row).unwrap();
    }
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
        .any(|r| r["requestType"].as_str().unwrap().starts_with("Set"))
    {
        "effects".into()
    } else if rows.iter().any(|r| r["requestType"] == "GetInputSettings") {
        "input-read".into()
    } else {
        "scene-read".into()
    }
}
fn response(
    kind: &str,
    data: &Value,
    state: &Value,
    reject: &Option<String>,
) -> (bool, u64, Option<String>, Value) {
    if reject.as_deref() == Some(kind) {
        return (false, 601, Some("fixture rejected".into()), json!({}));
    }
    let input = data["inputName"]
        .as_str()
        .and_then(|n| state["inputs"].get(n));
    match kind {
        "GetVersion" => (
            true,
            100,
            None,
            json!({"platform":"macos","availableRequests":["GetVersion","GetSceneItemList","GetInputSettings","GetInputMute","GetInputVolume","GetInputAudioTracks","GetStats","GetRecordStatus","GetCurrentProgramScene","GetInputList","GetProfileParameter","StartRecord","StopRecord","CreateRecordChapter","SetInputSettings","SetInputMute","SetInputVolume","SetInputAudioTracks","SetSceneItemTransform","SetSceneItemEnabled","SetSceneItemLocked","SetSceneItemIndex"],"supportedImageFormats":[]}),
        ),
        "GetSceneItemList" => {
            if data["sceneName"] == "Scene" {
                (true, 100, None, json!({"sceneItems":state["items"]}))
            } else {
                (false, 600, Some("scene absent".into()), json!({}))
            }
        }
        "GetInputSettings" | "GetInputMute" | "GetInputVolume" | "GetInputAudioTracks" => {
            match input {
                Some(input) => {
                    let keys = match kind {
                        "GetInputSettings" => vec!["inputKind", "inputSettings"],
                        "GetInputMute" => vec!["inputMuted"],
                        "GetInputVolume" => vec!["inputVolumeMul"],
                        _ => vec!["inputAudioTracks"],
                    };
                    let mut out = json!({});
                    for k in keys {
                        out[k] = input[k].clone();
                    }
                    (true, 100, None, out)
                }
                None => (false, 600, Some("input absent".into()), json!({})),
            }
        }
        "GetStats" => (
            true,
            100,
            None,
            json!({"availableDiskSpace":50000,"renderSkippedFrames":0,"renderTotalFrames":0}),
        ),
        "GetRecordStatus" => (
            true,
            100,
            None,
            json!({"outputActive":state["recording"].as_bool().unwrap_or(false),"outputPaused":false,"outputBytes":1048576}),
        ),
        "GetCurrentProgramScene" => (
            true,
            100,
            None,
            json!({"currentProgramSceneName":"Scene","currentProgramSceneUuid":"scene"}),
        ),
        "GetInputList" => (true, 100, None, json!({"inputs":[]})),
        "GetProfileParameter" => (
            true,
            100,
            None,
            json!({"parameterValue":if data["parameterCategory"]=="Output"&&data["parameterName"]=="Mode"{"Simple"}else{"1"}}),
        ),
        "StartRecord" | "CreateRecordChapter" => (true, 100, None, json!({})),
        "StopRecord" => (true, 100, None, json!({"outputPath":"/fixture/take.mkv"})),
        k if k.starts_with("Set") => (true, 100, None, json!({})),
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
    hold: Arc<Mutex<HashSet<String>>>,
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
        let hold = Arc::new(Mutex::new(HashSet::new()));
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
                                 message=peer.next()=>match message{Some(Ok(Message::Text(text)))=>{let frame:Value=serde_json::from_str(&text).unwrap();history.lock().unwrap().push(frame.clone());let tag=key(&frame);if tag=="StartRecord"||tag=="StopRecord"{let active=tag=="StartRecord";observed.lock().unwrap()["recording"]=json!(active);peer.send(Message::Text(json!({"op":5,"d":{"eventType":"RecordStateChanged","eventIntent":64,"eventData":{"outputState":if active{"OBS_WEBSOCKET_OUTPUT_STARTED"}else{"OBS_WEBSOCKET_OUTPUT_STOPPED"},"outputActive":active,"outputPath":"/fixture/take.mkv"}}}).to_string().into())).await.unwrap();}let state=observed.lock().unwrap().clone();let rejection=rejections.lock().unwrap().clone();let reply=if frame["op"]==6{let r=&frame["d"];let(ok,code,comment,data)=response(r["requestType"].as_str().unwrap(),&r["requestData"],&state,&rejection);json!({"op":7,"d":{"requestId":r["requestId"],"requestType":r["requestType"],"requestStatus":{"result":ok,"code":code,"comment":comment},"responseData":data}})}else{assert_eq!(frame["op"],8);let mut rows=frame["d"]["requests"].as_array().unwrap().iter().map(|r|{let(ok,code,comment,data)=response(r["requestType"].as_str().unwrap(),&r["requestData"],&state,&rejection);json!({"requestId":r["requestId"],"requestType":r["requestType"],"requestStatus":{"result":ok,"code":code,"comment":comment},"responseData":data})}).collect::<Vec<_>>();if tag=="effects"&&bad.load(Ordering::Acquire){rows.pop();}json!({"op":9,"d":{"requestId":frame["d"]["requestId"],"results":rows}})};if tag=="effects"&&lose.load(Ordering::Acquire){break;}
                if holding.lock().unwrap().remove(&tag){pending.lock().unwrap().insert(tag,reply);}else{peer.send(Message::Text(reply.to_string().into())).await.unwrap();}wake.notify_waiters();},Some(Ok(Message::Close(_)))|Some(Err(_))|None=>{let _=peer.flush().await;break;},_=>{}}
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
        self.hold.lock().unwrap().insert(tag.into());
    }
    async fn seen(&self, tag: &str, n: usize) {
        timeout(WAIT, async {
            loop {
                let wake = self.notify.notified();
                if self.count(tag) >= n && !self.hold.lock().unwrap().contains(tag) {
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
        let mut next = 0u64;
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
                    format!("cccccccc-cccc-4ccc-8ccc-{next:012x}")
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
fn restore(
    handle: ServiceHandle,
    args: Value,
    cancel: Cancellation,
) -> JoinHandle<Result<Value, obs_mcp_native::service::ServiceError>> {
    tokio::spawn(async move {
        handle
            .restore(request(args), "fixture".into(), cancel)
            .await
    })
}
#[test]
fn request_schema_defaults_empty_strings_and_unknown_fields_match_source() {
    assert!(!request(json!({})).dry_run);
    assert_eq!(
        request(json!({"snapshotId":"","ignored":7})).snapshot_id,
        Some("".into())
    );
    for args in [
        Value::Null,
        json!([]),
        json!({"snapshotId":null}),
        json!({"dryRun":null}),
        json!({"dryRun":0}),
    ] {
        assert!(Request::parse(&args).is_err());
    }
}
#[tokio::test]
async fn no_change_and_dry_run_use_original_snapshot_owner_without_effect_receipt() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(
        &dir,
        &[
            snapshot("current", Some(&f.url), false),
            snapshot("before", Some(&f.url), true),
        ],
    );
    let mut service = f.service(&dir).await;
    let h = service.handle();
    let unchanged = h
        .restore(
            request(json!({"snapshotId":"current"})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        unchanged,
        json!({"content":[{"type":"text","text":"OBS already matches snapshot current"}],"structuredContent":{"snapshotId":"current","changes":[],"missing":[],"applied":false}})
    );
    let plan = h
        .restore(
            request(json!({"dryRun":true})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(plan["structuredContent"]["snapshotId"], "before");
    assert_eq!(
        plan["structuredContent"]["changes"]
            .as_array()
            .unwrap()
            .len(),
        9
    );
    assert_eq!(plan["structuredContent"]["applied"], false);
    assert_eq!(f.count("effects"), 0);
    assert!(dir.receipts().is_empty());
    assert_eq!(
        h.snapshot(
            snapshot_tools::Request::parse(&json!({"list":true})).unwrap(),
            Cancellation::default()
        )
        .await
        .unwrap()["structuredContent"]["snapshots"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}

const SOURCE_ORACLE: &str = r#"
import {pathToFileURL} from 'node:url';
const {initialize}=await import(pathToFileURL(process.argv[1]).href);
const payload=JSON.parse(process.argv[2]);let callback;
const response=(r)=>{
 const type=r.requestType,data=r.requestData??{},input=payload.current.inputs[data.inputName];
 if(type==='GetSceneItemList')return data.sceneName==='Scene'?{ok:true,responseData:{sceneItems:payload.current.items}}:{ok:false,code:600,comment:'scene absent'};
 if(type.startsWith('GetInput')){
  if(!input)return{ok:false,code:600,comment:'input absent'};
  const keys={GetInputSettings:['inputKind','inputSettings'],GetInputMute:['inputMuted'],GetInputVolume:['inputVolumeMul'],GetInputAudioTracks:['inputAudioTracks']}[type];
  return{ok:true,responseData:Object.fromEntries(keys.map(k=>[k,input[k]]))};
 }
 return{ok:type!==payload.reject,code:type===payload.reject?601:100,...(type===payload.reject?{comment:'fixture rejected'}:{}),responseData:{}};
};
initialize({registerTool(name,config,run){if(name==='obs-restore')callback=run;}},{getConnectionStatus(){return{url:payload.url};},async sendBatch(requests){return requests.map(r=>({requestType:r.requestType,...response(r)}));}});
process.stdout.write(JSON.stringify(await callback(payload.args)));
"#;
fn source(dir: &Dir, url: &str, args: Value, current: Value, reject: Option<&str>) -> Value {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned();
    let output = Command::new("node")
        .args(["--input-type=module", "--eval", SOURCE_ORACLE])
        .arg(root.join("build/tools/snapshots.js"))
        .arg(json!({"url":url,"args":args,"current":current,"reject":reject}).to_string())
        .env("OBS_MCP_STATE_DIR", &dir.0)
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
#[tokio::test]
async fn latest_explicit_empty_and_endpoint_selection_match_real_source_callback() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(
        &dir,
        &[
            snapshot("legacy", None, true),
            snapshot("current", Some(&f.url), false),
            snapshot("foreign", Some("ws://elsewhere:4455"), true),
        ],
    );
    let mut service = f.service(&dir).await;
    let h = service.handle();
    for args in [
        json!({"dryRun":true}),
        json!({"snapshotId":"","dryRun":true}),
        json!({"snapshotId":"legacy","dryRun":true}),
        json!({"snapshotId":"foreign","dryRun":true}),
        json!({"snapshotId":"absent","dryRun":false}),
    ] {
        let expected = source(&dir, &f.url, args.clone(), current(), None);
        let actual = h
            .restore(request(args), "fixture".into(), Cancellation::default())
            .await
            .unwrap();
        assert_eq!(actual, expected);
    }
    assert_eq!(f.count("effects"), 0);
    assert!(dir.receipts().is_empty());
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn empty_history_and_missing_objects_are_source_notes_not_false_success_or_error() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut service = f.service(&dir).await;
    let h = service.handle();
    let actual = h
        .restore(
            request(json!({})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        actual,
        source(&dir, &f.url, json!({"dryRun":false}), current(), None)
    );
    assert!(actual["isError"] == true);
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
    let f = Fixture::new().await;
    let dir = Dir::new();
    let mut saved = snapshot("missing", Some(&f.url), true);
    saved.inputs[0].input_name = "Absent".into();
    for item in &mut saved.items {
        item.scene_name = "Deleted".into();
    }
    save(&dir, &[saved]);
    let mut service = f.service(&dir).await;
    let actual = service
        .handle()
        .restore(
            request(json!({})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        actual,
        source(&dir, &f.url, json!({"dryRun":false}), current(), None)
    );
    assert!(actual.get("isError").is_none());
    assert_eq!(actual["structuredContent"]["applied"], false);
    assert!(
        !actual["structuredContent"]["missing"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(dir.receipts().is_empty());
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn exact_closed_effect_order_and_mixed_failures_match_source_without_new_snapshot() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(&dir, &[snapshot("before", Some(&f.url), true)]);
    let history = fs::read(dir.0.join("snapshots.json")).unwrap();
    *f.reject.lock().unwrap() = Some("SetInputMute".into());
    let expected = source(
        &dir,
        &f.url,
        json!({"dryRun":false}),
        current(),
        Some("SetInputMute"),
    );
    let mut service = f.service(&dir).await;
    let actual = service
        .handle()
        .restore(
            request(json!({})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual["structuredContent"]["applied"], true);
    assert_eq!(
        actual["structuredContent"]["failures"],
        json!(["input Input: mute failed: fixture rejected"])
    );
    {
        let trace = f.trace.lock().unwrap();
        let effects = trace
            .iter()
            .filter(|frame| key(frame) == "effects")
            .collect::<Vec<_>>();
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0]["d"]["executionType"], 0);
        assert_eq!(effects[0]["d"]["haltOnFailure"], false);
        let methods = effects[0]["d"]["requests"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["requestType"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            vec![
                "SetInputSettings",
                "SetInputMute",
                "SetInputVolume",
                "SetInputAudioTracks",
                "SetSceneItemTransform",
                "SetSceneItemEnabled",
                "SetSceneItemLocked",
                "SetSceneItemIndex",
                "SetSceneItemIndex"
            ]
        );
    }
    let receipts = dir.receipts();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0]["version"], 3);
    assert_eq!(receipts[0]["phase"], "acknowledged");
    assert_eq!(receipts[0]["successfulRequests"], 8);
    assert_eq!(receipts[0]["failedRequests"], 1);
    assert_eq!(receipts[0]["postconditionObserved"], false);
    assert_eq!(fs::read(dir.0.join("snapshots.json")).unwrap(), history);
    assert_eq!(
        service
            .handle()
            .snapshot(
                snapshot_tools::Request::parse(&json!({"list":true})).unwrap(),
                Cancellation::default()
            )
            .await
            .unwrap()["structuredContent"]["snapshots"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn write_policy_precedes_corrupt_or_missing_lookup_including_dry_run() {
    for corrupt in [false, true] {
        let f = Fixture::new().await;
        let dir = Dir::new();
        if corrupt {
            fs::write(dir.0.join("snapshots.json"), "{ broken").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(
                    dir.0.join("snapshots.json"),
                    fs::Permissions::from_mode(0o600),
                )
                .unwrap();
            }
        }
        f.control
            .lock()
            .unwrap()
            .claim("other-client", 1, "private reason", EPOCH)
            .unwrap();
        let mut service = f.service(&dir).await;
        let h = service.handle();
        let before = f.trace.lock().unwrap().len();
        for args in [
            json!({"dryRun":true}),
            json!({"snapshotId":"absent","dryRun":false}),
        ] {
            let result = h
                .restore(request(args), "fixture".into(), Cancellation::default())
                .await
                .unwrap();
            assert_eq!(result["isError"], true);
            assert!(
                result["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .starts_with("Not changed: other-client has control")
            );
        }
        assert_eq!(f.trace.lock().unwrap().len(), before);
        assert!(dir.receipts().is_empty());
        if corrupt {
            assert_eq!(
                fs::read_to_string(dir.0.join("snapshots.json")).unwrap(),
                "{ broken"
            );
        }
        assert!(service.shutdown().await.unwrap().joined);
        f.joined().await;
    }
}
#[tokio::test]
async fn queued_batch_rechecks_lease_after_planning_and_returns_preparation_owner() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(&dir, &[snapshot("before", Some(&f.url), true)]);
    let mut service = f.service(&dir).await;
    let h = service.handle();
    f.hold("input-read");
    let restoring = restore(h.clone(), json!({}), Cancellation::default());
    f.seen("input-read", 1).await;
    f.hold("GetStats");
    let reader = h.clone();
    let reading =
        tokio::spawn(async move { reader.read(ReadRequest::Stats, Value::Null, WAIT).await });
    tokio::task::yield_now().await;
    f.release("input-read").await;
    f.seen("GetStats", 1).await;
    timeout(WAIT, async {
        loop {
            if dir.receipts().iter().any(|r| r["phase"] == "dispatching") {
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
    let result = restoring.await.unwrap().unwrap();
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("Not changed: other-client has control")
    );
    assert_eq!(f.count("effects"), 0);
    assert_eq!(dir.receipts()[0]["phase"], "not-dispatched");
    assert!(
        h.snapshot(
            snapshot_tools::Request::parse(&json!({"list":true})).unwrap(),
            Cancellation::default()
        )
        .await
        .unwrap()
        .get("isError")
        .is_none()
    );
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn cancelled_or_dropped_frontend_and_shutdown_retain_one_held_ack() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(&dir, &[snapshot("before", Some(&f.url), true)]);
    let mut service = f.service(&dir).await;
    f.hold("effects");
    let cancel = Cancellation::default();
    let frontend = restore(service.handle(), json!({}), cancel.clone());
    f.seen("effects", 1).await;
    cancel.cancel();
    frontend.abort();
    assert!(frontend.await.unwrap_err().is_cancelled());
    let mut shutdown = Box::pin(service.shutdown());
    assert!(poll!(shutdown.as_mut()).is_pending());
    f.release("effects").await;
    let joined = shutdown.await.unwrap();
    assert!(
        joined.joined && joined.workers_joined && joined.observer_joined && joined.socket_joined
    );
    assert_eq!(dir.receipts()[0]["phase"], "acknowledged");
    assert_eq!(f.count("effects"), 1);
    f.joined().await;
}
#[tokio::test]
async fn cancellation_during_planning_or_closing_never_begins_a_new_effect() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(&dir, &[snapshot("before", Some(&f.url), true)]);
    let mut service = f.service(&dir).await;
    f.hold("input-read");
    let cancel = Cancellation::default();
    let frontend = restore(service.handle(), json!({}), cancel.clone());
    f.seen("input-read", 1).await;
    cancel.cancel();
    let mut shutdown = Box::pin(service.shutdown());
    assert!(poll!(shutdown.as_mut()).is_pending());
    f.release("input-read").await;
    let joined = shutdown.await.unwrap();
    assert!(joined.joined && joined.workers_joined);
    let result = frontend.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(f.count("effects"), 0);
    assert!(dir.receipts().is_empty());
    f.joined().await;
}
#[tokio::test]
async fn lost_or_malformed_effect_reply_stays_unknown_and_restart_has_no_retry() {
    for malformed in [false, true] {
        let f = Fixture::new().await;
        let dir = Dir::new();
        save(&dir, &[snapshot("before", Some(&f.url), true)]);
        if malformed {
            f.malformed.store(true, Ordering::Release);
        } else {
            f.lost.store(true, Ordering::Release);
        }
        let mut service = f.service(&dir).await;
        let result = service
            .handle()
            .restore(
                request(json!({})),
                "fixture".into(),
                Cancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(result["isError"], true);
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("uncertain")
        );
        assert_eq!(dir.receipts()[0]["phase"], "unknown");
        assert_eq!(f.count("effects"), 1);
        let joined = service.shutdown().await.unwrap();
        assert!(joined.workers_joined && joined.socket_joined);
        let reopened = OperationStore::new(&dir.0, &f.url).unwrap();
        assert!(reopened.read_view().admission().unwrap().blocking);
        assert!(
            service
                .handle()
                .restore(
                    request(json!({})),
                    "fixture".into(),
                    Cancellation::default()
                )
                .await
                .is_err()
        );
        f.joined().await;
    }
}
#[tokio::test]
async fn post_ack_publication_failure_preserves_known_results_and_blocks_more_effects() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(&dir, &[snapshot("before", Some(&f.url), true)]);
    let mut service = f.service(&dir).await;
    f.hold("effects");
    let work = restore(service.handle(), json!({}), Cancellation::default());
    f.seen("effects", 1).await;
    let path = dir.0.join("operation-receipts").join(format!(
        "{}.json",
        dir.receipts()[0]["operationId"].as_str().unwrap()
    ));
    fs::write(path, b"{}").unwrap();
    f.release("effects").await;
    let result = work.await.unwrap().unwrap();
    assert_eq!(result["structuredContent"]["applied"], true);
    assert_eq!(result["structuredContent"]["failures"], json!([]));
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("Restored snapshot before: 9 of 9")
    );
    assert!(
        result["content"][1]["text"]
            .as_str()
            .unwrap()
            .contains("acknowledged")
    );
    assert!(
        service
            .handle()
            .restore(
                request(json!({})),
                "fixture".into(),
                Cancellation::default()
            )
            .await
            .is_err()
    );
    assert_eq!(f.count("effects"), 1);
    let _ = service.shutdown().await.unwrap();
    f.joined().await;
}

#[tokio::test]
async fn added_and_removed_items_stay_legacy_notes_in_exact_source_order() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(&dir, &[snapshot("before", Some(&f.url), true)]);
    let mut state = current();
    state["items"].as_array_mut().unwrap().remove(0);
    state["items"].as_array_mut().unwrap().insert(0,json!({"sceneItemId":9,"sourceName":"Added","sceneItemIndex":0,"sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemTransform":{}}));
    *f.state.lock().unwrap() = state.clone();
    let expected = source(&dir, &f.url, json!({"dryRun":true}), state, None);
    let mut service = f.service(&dir).await;
    let result = service
        .handle()
        .restore(
            request(json!({"dryRun":true})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(result, expected);
    assert_eq!(
        result["structuredContent"]["missing"],
        json!([
            "Scene › Other (#2) was removed",
            "Scene › Added (#9) was added since; it is left in place"
        ])
    );
    assert!(result.get("isError").is_none());
    assert_eq!(f.count("effects"), 0);
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}
#[tokio::test]
async fn actual_locked_take_keeps_its_supervisor_and_stop_authority_during_same_holder_restore() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(&dir, &[snapshot("before", Some(&f.url), true)]);
    f.control
        .lock()
        .unwrap()
        .claim("fixture", 1, "same holder", EPOCH)
        .unwrap();
    let mut service = f.service_with_take(&dir, true).await;
    let h = service.handle();
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
        .restore(
            request(json!({})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(result["structuredContent"]["applied"], true, "{result}");
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
async fn intent_persistence_failure_is_zero_send_and_keeps_snapshot_bytes() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(&dir, &[snapshot("before", Some(&f.url), true)]);
    let snapshot_bytes = fs::read(dir.0.join("snapshots.json")).unwrap();
    let mut service = f.service(&dir).await;
    f.hold("input-read");
    let work = restore(service.handle(), json!({}), Cancellation::default());
    f.seen("input-read", 1).await;
    let receipts = dir.0.join("operation-receipts");
    fs::create_dir_all(&receipts).unwrap();
    let staging = receipts.join(format!(
        "cccccccc-cccc-4ccc-8ccc-000000000001.{}.1.tmp",
        std::process::id()
    ));
    fs::write(staging, b"preexisting evidence").unwrap();
    f.release("input-read").await;
    let result = work.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("no OBS request was dispatched"),
        "{result}"
    );
    assert_eq!(f.count("effects"), 0);
    assert!(dir.receipts().is_empty());
    assert_eq!(
        fs::read(dir.0.join("snapshots.json")).unwrap(),
        snapshot_bytes
    );
    assert!(
        service
            .handle()
            .restore(
                request(json!({})),
                "fixture".into(),
                Cancellation::default()
            )
            .await
            .is_err()
    );
    let _ = service.shutdown().await.unwrap();
    f.joined().await;
}
#[tokio::test]
async fn restarted_service_can_plan_but_cannot_retry_unknown_batch_on_same_endpoint() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(&dir, &[snapshot("before", Some(&f.url), true)]);
    let endpoint = f.url.clone();
    f.lost.store(true, Ordering::Release);
    let mut service = f.service(&dir).await;
    let result = service
        .handle()
        .restore(
            request(json!({})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(result["isError"], true);
    let _ = service.shutdown().await.unwrap();
    drop(service);
    f.joined().await;
    let next = Fixture::new_at(endpoint.strip_prefix("ws://").unwrap()).await;
    let mut restarted = next.service(&dir).await;
    let plan = restarted
        .handle()
        .restore(
            request(json!({"dryRun":true})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(plan["structuredContent"]["applied"], false);
    let result = restarted
        .handle()
        .restore(
            request(json!({})),
            "fixture".into(),
            Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Unresolved receipt custody")
    );
    assert_eq!(next.count("effects"), 0);
    assert_eq!(dir.receipts().len(), 1);
    assert_eq!(dir.receipts()[0]["phase"], "unknown");
    assert!(restarted.shutdown().await.unwrap().joined);
    next.joined().await;
}

#[tokio::test]
async fn planning_consumes_original_deadline_instead_of_resetting_apply_budget() {
    let f = Fixture::new().await;
    let dir = Dir::new();
    save(&dir, &[snapshot("before", Some(&f.url), true)]);
    let limit = Duration::from_secs(2);
    let mut service = f.service_with_limit(&dir, false, limit).await;
    f.hold("input-read");
    let work = restore(service.handle(), json!({}), Cancellation::default());
    f.seen("input-read", 1).await;
    // Queue a fresh ordinary read behind the held planner read. It starts later
    // and has a later deadline, while Restore must spend its ORIGINAL budget.
    tokio::time::sleep(Duration::from_millis(500)).await;
    f.hold("GetStats");
    let handle = service.handle();
    let ordinary =
        tokio::spawn(async move { handle.read(ReadRequest::Stats, Value::Null, limit).await });
    tokio::task::yield_now().await;
    f.release("input-read").await;
    f.seen("GetStats", 1).await;
    let result = timeout(WAIT, work).await.unwrap().unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(f.count("effects"), 0);
    assert_eq!(dir.receipts()[0]["phase"], "not-dispatched");
    assert_eq!(dir.receipts()[0]["requestDispatched"], false);
    f.release("GetStats").await;
    ordinary.await.unwrap().unwrap();
    assert!(service.shutdown().await.unwrap().joined);
    f.joined().await;
}

#[test]
fn offline_preparation_lookup_returns_only_refusals_without_any_connection() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let empty = Dir::new();
    let mut owners = PreparationOwners::new(&empty.0).unwrap();
    assert_eq!(
        owners
            .local_restore_refusal(&endpoint, &request(json!({})))
            .unwrap(),
        json!({"content":[{"type":"text","text":"No snapshots saved; take one with obs-snapshot"}],"isError":true})
    );
    let dir = Dir::new();
    save(
        &dir,
        &[
            snapshot("local", Some(&endpoint), true),
            snapshot("foreign", Some("ws://elsewhere:4455"), true),
        ],
    );
    let bytes = fs::read(dir.0.join("snapshots.json")).unwrap();
    let mut owners = PreparationOwners::new(&dir.0).unwrap();
    for args in [
        json!({}),
        json!({"snapshotId":""}),
        json!({"snapshotId":"local","dryRun":true}),
    ] {
        assert!(
            owners
                .local_restore_refusal(&endpoint, &request(args))
                .is_none()
        );
    }
    let refused = owners
        .local_restore_refusal(&endpoint, &request(json!({"snapshotId":"foreign"})))
        .unwrap();
    assert_eq!(
        refused,
        json!({"content":[{"type":"text","text":"No snapshot foreign; list them with obs-snapshot list: true"}],"isError":true})
    );
    assert_eq!(fs::read(dir.0.join("snapshots.json")).unwrap(), bytes);
    assert!(dir.receipts().is_empty());
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let corrupt = Dir::new();
    let file = corrupt.0.join("snapshots.json");
    fs::write(&file, b"{ broken").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
    }
    let mut owners = PreparationOwners::new(&corrupt.0).unwrap();
    let result = owners
        .local_restore_refusal(&endpoint, &request(json!({})))
        .unwrap();
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("Restore failed:")
    );
    assert_eq!(fs::read(file).unwrap(), b"{ broken");
}

#[tokio::test]
async fn offline_validity_grants_no_authority_and_same_owner_rechecks_after_connect() {
    for replaced in [false, true] {
        let f = Fixture::new().await;
        let dir = Dir::new();
        save(&dir, &[snapshot("before", Some(&f.url), true)]);
        let mut owners = PreparationOwners::new(&dir.0).unwrap();
        assert!(
            owners
                .local_restore_refusal(&f.url, &request(json!({})))
                .is_none()
        );
        assert!(f.trace.lock().unwrap().is_empty());
        if replaced {
            let path = dir.0.join("snapshots.json");
            let replacement = dir.0.join("replacement");
            fs::copy(&path, &replacement).unwrap();
            fs::rename(replacement, path).unwrap();
        } else {
            f.control
                .lock()
                .unwrap()
                .claim("other-client", 1, "changed after offline lookup", EPOCH)
                .unwrap();
        }
        let mut service = f.service_with_owners(&dir, false, WAIT, owners).await;
        let h = service.handle();
        let result = h
            .restore(
                request(json!({})),
                "fixture".into(),
                Cancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(result["isError"], true);
        if replaced {
            assert!(
                result["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("replaced"),
                "{result}"
            );
        } else {
            assert!(
                result["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .starts_with("Not changed:")
            );
        }
        assert_eq!(f.count("effects"), 0);
        assert!(dir.receipts().is_empty());
        if !replaced {
            f.control.lock().unwrap().release("other-client", EPOCH);
            let applied = h
                .restore(
                    request(json!({})),
                    "fixture".into(),
                    Cancellation::default(),
                )
                .await
                .unwrap();
            assert_eq!(applied["structuredContent"]["applied"], true);
            assert_eq!(f.count("effects"), 1);
        }
        assert!(service.shutdown().await.unwrap().joined);
        f.joined().await;
    }
}
