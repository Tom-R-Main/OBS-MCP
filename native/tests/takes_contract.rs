// SPDX-License-Identifier: GPL-2.0-only
//! Injected-owner qualification. The binding socket is real; effect/file seams
//! are explicit fakes. This does not qualify native/live OBS recording.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    clock,
    control::ControlDomain,
    custody::{Clock, SharedControl},
    observation::ReadRequest,
    operation_store::OperationStore,
    preflight::{Check, Report, Status},
    session::{ConnectionToken, EventReceiver, ObsSession, SessionOptions},
    takes::{
        ClipCheck, DispatchError, DispatchGuard, Effect, Event, IoFuture, Limits, Loan,
        MonitorSetup, Policy, StartOptions, TakeHandle, TakeIo, TakeSupervisor,
    },
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(4);
const EPOCH: u64 = 1_793_448_000_000;
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "takes-contract-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Clone, Default)]
struct Plan {
    no_start_event: bool,
    start_error: bool,
    stop_mode: String,
    local: bool,
    clip: ClipCheck,
    chapter_error: bool,
    start_unknown: bool,
}
struct State {
    active: bool,
    paused: bool,
    effects: Vec<String>,
    plan: Plan,
    stop_calls: usize,
}
struct Flight {
    active: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
impl Drop for Flight {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct Io {
    token: ConnectionToken,
    events: mpsc::Sender<Event>,
    state: Arc<Mutex<State>>,
    active: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    joined: Arc<AtomicBool>,
    setup_gate: Arc<Notify>,
    hold_setup: AtomicBool,
    stop_gate: Arc<Notify>,
    hold_stop: AtomicBool,
    setup_entered: Arc<Notify>,
    stop_entered: Arc<Notify>,
    output: PathBuf,
    journal: PathBuf,
    intent_phases: Arc<Mutex<Vec<String>>>,
    fail_status: Arc<AtomicUsize>,
    hold_inspection: AtomicBool,
    inspection_gate: Arc<Notify>,
    hold_stop_reply: AtomicBool,
    stop_reply_gate: Arc<Notify>,
    observer_failure: Arc<AtomicBool>,
}
impl Io {
    fn own<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = T> + Send + 'static,
    ) -> IoFuture<T> {
        let active = self.active.clone();
        let dropped = self.dropped.clone();
        Box::pin(async move {
            active.fetch_add(1, Ordering::SeqCst);
            let _flight = Flight { active, dropped };
            future.await
        })
    }
    async fn emit(&self, name: &str, data: Value) {
        emit(self.events.clone(), self.token, name.to_owned(), data).await;
    }
}
async fn emit(
    events: mpsc::Sender<Event>,
    connection: ConnectionToken,
    event_type: String,
    data: Value,
) {
    events
        .send(Event::Obs {
            connection,
            event_type,
            data,
        })
        .await
        .unwrap();
    let (done, joined) = oneshot::channel();
    events
        .send(Event::Barrier { connection, done })
        .await
        .unwrap();
    joined.await.unwrap();
}
impl TakeIo for Io {
    fn preflight(&self, _options: obs_mcp_native::preflight::Options) -> IoFuture<Report> {
        let local = self.state.lock().unwrap().plan.local;
        let active = self.state.lock().unwrap().active;
        self.own(async move {
            Report {
                ready: !active,
                checks: [
                    "connection",
                    "record-idle",
                    "record-directory",
                    "disk-space",
                    "encoder",
                    "audio",
                    "scene-items",
                ]
                .into_iter()
                .map(|id| Check {
                    id: id.into(),
                    status: if id == "record-idle" && active {
                        Status::Fail
                    } else if id == "record-directory" && !local {
                        Status::Unknown
                    } else {
                        Status::Pass
                    },
                    message: if id == "record-idle" && active {
                        "Recording is already active"
                    } else {
                        "fixture"
                    }
                    .into(),
                })
                .collect(),
            }
        })
    }
    fn read(&self, request: ReadRequest, _duration: Duration) -> IoFuture<Result<Value, String>> {
        let state = self.state.clone();
        let failure = self.fail_status.clone();
        self.own(async move {
            if request==ReadRequest::RecordStatus&&failure.fetch_update(Ordering::AcqRel,Ordering::Acquire,|remaining|remaining.checked_sub(1)).is_ok() {return Err("fixture status unavailable".into());}
            let state=state.lock().unwrap();match request {
            ReadRequest::RecordStatus=>Ok(json!({"outputActive":state.active,"outputPaused":state.paused,"outputBytes":1_048_576})),
            ReadRequest::Stats=>Ok(json!({"availableDiskSpace":50000,"renderSkippedFrames":0,"renderTotalFrames":0})),
            _=>Err("Unexpected fixture read".into()),
        }})
    }
    fn dispatch(
        &self,
        expected: ConnectionToken,
        effect: Effect,
        _duration: Duration,
        guard: DispatchGuard,
    ) -> IoFuture<Result<Value, DispatchError>> {
        assert_eq!(expected, self.token);
        let state = self.state.clone();
        let events = self.events.clone();
        let token = self.token;
        let path = self.output.to_string_lossy().into_owned();
        let hold_stop = self.hold_stop.load(Ordering::Acquire);
        let stop_gate = self.stop_gate.clone();
        let entered = self.stop_entered.clone();
        let journal = self.journal.clone();
        let phases = self.intent_phases.clone();
        let hold_reply = self.hold_stop_reply.load(Ordering::Acquire);
        let reply_gate = self.stop_reply_gate.clone();
        self.own(async move {
            if matches!(effect,Effect::StopRecord)&&hold_stop {entered.notify_waiters();stop_gate.notified().await;}
            guard().map_err(DispatchError::BeforeDispatch)?;
            if !matches!(effect,Effect::Chapter(_)) {
                let kind=if matches!(effect,Effect::StartRecord) {"record-start"}else{"record-stop"};
                let receipts=fs::read_dir(journal.join("operation-receipts")).unwrap().filter_map(Result::ok).filter(|entry|entry.path().extension().is_some_and(|extension|extension=="json")).map(|entry|serde_json::from_slice::<Value>(&fs::read(entry.path()).unwrap()).unwrap()).collect::<Vec<_>>();
                assert!(receipts.iter().any(|receipt|receipt["kind"]==kind&&receipt["phase"]=="dispatching"&&receipt["requestDispatched"].is_null()),"effect preceded durable dispatching intent");
                phases.lock().unwrap().push(kind.into());
            }
            let plan={let mut state=state.lock().unwrap();state.effects.push(effect.request_type().into());if matches!(effect,Effect::StopRecord){state.stop_calls+=1;}state.plan.clone()};
            match effect {
                Effect::StartRecord=>{
                    if plan.start_error {return Err(DispatchError::Rejected {code:500,message:"OBS request StartRecord failed with code 500: fixture start rejected".into()});}
                    state.lock().unwrap().active=true;
                    if !plan.no_start_event {emit(events,token,"RecordStateChanged".into(),json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputActive":true,"outputPath":path})).await;}
                    if plan.start_unknown {Err(DispatchError::Unknown("fixture lost start reply".into()))}else{Ok(json!({}))}
                },
                Effect::StopRecord=>{
                    if plan.stop_mode=="rejected" {return Err(DispatchError::Rejected {code:500,message:"OBS request StopRecord failed with code 500: fixture stop rejected".into()});}
                    if plan.stop_mode=="unknown" {return Err(DispatchError::Unknown("fixture lost stop reply".into()));}
                    state.lock().unwrap().active=false;
                    emit(events,token,"RecordStateChanged".into(),json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED","outputActive":false,"outputPath":path})).await;
                    if hold_reply {reply_gate.notified().await;}
                    if plan.stop_mode=="unknown-stopped" {Err(DispatchError::Unknown("fixture lost stop reply".into()))}else{Ok(json!({}))}
                },
                Effect::Chapter(_)=>if plan.chapter_error {Err(DispatchError::Rejected {code:500,message:"OBS request CreateRecordChapter failed with code 500: fixture chapter rejected".into()})}else{Ok(json!({}))},
            }
        })
    }
    fn monitor_setup(&self) -> IoFuture<Result<MonitorSetup, String>> {
        let gate = self.setup_gate.clone();
        let held = self.hold_setup.load(Ordering::Acquire);
        let entered = self.setup_entered.clone();
        self.own(async move {
            if held {entered.notify_waiters();gate.notified().await;}
            Ok(MonitorSetup {record_tracks:vec![1],input_tracks:HashMap::new(),tracks_error:None,program:json!({"currentProgramSceneName":"Demo"}),stats:Some(json!({"availableDiskSpace":50000,"renderSkippedFrames":0,"renderTotalFrames":0})),supports_ppm:false})
        })
    }
    fn picture(&self, _program: String) -> IoFuture<Result<Vec<u8>, String>> {
        self.own(async { Ok(vec![40, 40]) })
    }
    fn inspect(&self, _path: String, _loudness: bool) -> IoFuture<ClipCheck> {
        let clip = self.state.lock().unwrap().plan.clip.clone();
        let held = self.hold_inspection.load(Ordering::Acquire);
        let gate = self.inspection_gate.clone();
        self.own(async move {
            if held {
                gate.notified().await;
            }
            clip
        })
    }
    fn write_log(
        &self,
        path: String,
        summary: obs_mcp_native::take_monitor::Summary,
    ) -> IoFuture<Option<String>> {
        self.own(async move {
            let log = Path::new(&path).with_extension("take.json");
            let mut value = serde_json::to_value(summary).unwrap();
            value["recording"] = json!(path);
            fs::write(&log, serde_json::to_vec(&value).unwrap())
                .ok()
                .map(|_| log.to_string_lossy().into_owned())
        })
    }
    fn join_observers(&self) -> IoFuture<Result<(), String>> {
        let active = self.active.clone();
        let joined = self.joined.clone();
        let failure = self.observer_failure.clone();
        Box::pin(async move {
            assert_eq!(
                active.load(Ordering::SeqCst),
                0,
                "result preceded IO worker exit"
            );
            if failure.load(Ordering::Acquire) {
                return Err("fixture observer cleanup unconfirmed".into());
            }
            joined.store(true, Ordering::Release);
            Ok(())
        })
    }
}
struct Fixture {
    dir: Dir,
    owner: TakeSupervisor,
    handle: TakeHandle,
    io: Arc<Io>,
    session: ObsSession,
    peer: JoinHandle<()>,
    clock: Arc<AtomicU64>,
    control: SharedControl,
    _transport_events: EventReceiver,
}
impl Fixture {
    async fn new(plan: Plan) -> Self {
        Self::with_limits(
            plan,
            Limits {
                rpc: Duration::from_millis(400),
                setup: Duration::from_secs(3),
                stop: Duration::from_millis(100),
                start_confirmation: Duration::from_millis(20),
                ..Limits::default()
            },
        )
        .await
    }
    async fn with_limits(plan: Plan, limits: Limits) -> Self {
        let dir = Dir::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            socket
                .send(Message::Text(
                    json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            assert!(matches!(socket.next().await, Some(Ok(Message::Text(_)))));
            socket
                .send(Message::Text(
                    json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            if let Some(next) = socket.next().await {
                match next.unwrap() {
                    Message::Close(_) => {
                        let _ = socket.flush().await;
                    }
                    other => panic!("injected test unexpectedly wrote binding socket: {other:?}"),
                }
            }
        });
        let (session, _events) = ObsSession::connect(
            &url,
            None,
            SessionOptions {
                event_subscriptions: obs_mcp_native::takes::REQUIRED_SUBSCRIPTIONS,
                ..SessionOptions::default()
            },
        )
        .await
        .unwrap();
        let clock = Arc::new(AtomicU64::new(EPOCH));
        let time = clock.clone();
        let wall: Clock = Arc::new(move || {
            clock::from_epoch_duration(Duration::from_millis(time.load(Ordering::Acquire))).unwrap()
        });
        let control = Arc::new(Mutex::new(ControlDomain::default()));
        let (sender, events) = mpsc::channel(64);
        let io = Arc::new(Io {
            token: session.token(),
            events: sender,
            state: Arc::new(Mutex::new(State {
                active: false,
                paused: false,
                effects: vec![],
                plan,
                stop_calls: 0,
            })),
            active: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicUsize::new(0)),
            joined: Arc::new(AtomicBool::new(false)),
            setup_gate: Arc::new(Notify::new()),
            hold_setup: AtomicBool::new(false),
            stop_gate: Arc::new(Notify::new()),
            hold_stop: AtomicBool::new(false),
            setup_entered: Arc::new(Notify::new()),
            stop_entered: Arc::new(Notify::new()),
            output: dir.path("clip.mkv"),
            journal: dir.path("native-state"),
            intent_phases: Arc::new(Mutex::new(vec![])),
            fail_status: Arc::new(AtomicUsize::new(0)),
            hold_inspection: AtomicBool::new(false),
            inspection_gate: Arc::new(Notify::new()),
            hold_stop_reply: AtomicBool::new(false),
            stop_reply_gate: Arc::new(Notify::new()),
            observer_failure: Arc::new(AtomicBool::new(false)),
        });
        let store = OperationStore::new(dir.path("native-state"), &url).unwrap();
        let mut sequence = 0_u64;
        let owner = TakeSupervisor::spawn(
            Loan {
                session: session.handle(),
                store,
                control: control.clone(),
                clock: wall,
                identities: Box::new(move || {
                    sequence += 1;
                    format!("{sequence:08x}-0000-4000-8000-000000000000")
                }),
                io: io.clone(),
                events,
            },
            limits,
        )
        .unwrap();
        let handle = owner.handle();
        Self {
            dir,
            owner,
            handle,
            io,
            session,
            peer,
            clock,
            control,
            _transport_events: _events,
        }
    }
    async fn close(mut self) -> obs_mcp_native::takes::ShutdownReceipt {
        let receipt = timeout(WAIT, self.owner.shutdown()).await.unwrap().unwrap();
        assert!(receipt.joined);
        assert!(receipt.observers_joined);
        assert!(self.io.joined.load(Ordering::Acquire));
        assert_eq!(self.io.active.load(Ordering::Acquire), 0);
        assert!(self.owner.take_store().is_some());
        assert!(self.owner.take_store().is_none());
        self.session.shutdown().await.unwrap();
        timeout(WAIT, self.peer).await.unwrap().unwrap();
        receipt
    }
}
fn policy() -> Policy {
    Policy {
        caller: "tester".into(),
        ..Policy::default()
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    timeout(WAIT, async {
        while !condition() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn start_mark_status_stop_and_repeat_preserve_owned_state_and_joined_workers() {
    let fixture = Fixture::new(Plan::default()).await;
    let start = timeout(
        WAIT,
        fixture.handle.start(StartOptions::default(), policy()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_ne!(start["isError"], true, "{start}");
    assert_eq!(start["structuredContent"]["ownership"], "continuous");
    assert!(fixture.handle.snapshot()["locked"] == true);
    let mark = fixture.handle.mark("step".into(), policy()).await.unwrap();
    assert_eq!(mark["structuredContent"]["atSeconds"], 0.0);
    let status = fixture.handle.status().await.unwrap();
    assert_eq!(status["structuredContent"]["record"]["outputActive"], true);
    let stop = timeout(WAIT, fixture.handle.stop(None, policy()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stop["structuredContent"]["stopConfirmed"], true, "{stop}");
    assert_eq!(
        stop["structuredContent"]["verification"]["status"],
        "unavailable"
    );
    assert_eq!(fixture.handle.stop(None, policy()).await.unwrap(), stop);
    assert_eq!(
        fixture.io.state.lock().unwrap().effects,
        ["StartRecord", "CreateRecordChapter", "StopRecord"]
    );
    assert!(fixture.io.dropped.load(Ordering::Acquire) > 0);
    fixture.close().await;
}

#[tokio::test]
async fn uncertain_stop_observation_reconciles_without_replaying_the_mutation() {
    let fixture = Fixture::new(Plan {
        stop_mode: "unknown-stopped".into(),
        ..Plan::default()
    })
    .await;
    fixture
        .handle
        .start(StartOptions::default(), policy())
        .await
        .unwrap();
    let first = fixture.handle.stop(None, policy()).await.unwrap();
    assert_eq!(first["structuredContent"]["stopConfirmed"], false);
    let second = fixture.handle.stop(None, policy()).await.unwrap();
    assert_eq!(
        second["structuredContent"]["stopConfirmed"], true,
        "{second}"
    );
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 1);
    fixture.close().await;
}

#[tokio::test]
async fn replacement_recording_is_never_stopped_and_only_inactive_observation_retires_lock() {
    let fixture = Fixture::new(Plan::default()).await;
    fixture
        .handle
        .start(StartOptions::default(), policy())
        .await
        .unwrap();
    fixture
        .io
        .emit(
            "RecordStateChanged",
            json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED","outputActive":false}),
        )
        .await;
    fixture.io.emit("RecordStateChanged",json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputActive":true,"outputPath":"/fixture/human.mkv"})).await;
    let result = fixture.handle.stop(None, policy()).await.unwrap();
    assert_eq!(result["structuredContent"]["stopConfirmed"], false);
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 0);
    assert_eq!(fixture.handle.snapshot()["ownership"], "unavailable");
    fixture.io.state.lock().unwrap().active = false;
    let retired = fixture.handle.stop(None, policy()).await.unwrap();
    assert_eq!(retired["structuredContent"]["stopConfirmed"], false);
    assert!(fixture.handle.snapshot().is_null());
    fixture.close().await;
}

#[tokio::test]
async fn deadline_keeps_running_after_frontend_start_wait_is_dropped_and_ignores_pause() {
    let fixture = Fixture::new(Plan::default()).await;
    fixture.io.hold_setup.store(true, Ordering::Release);
    let handle = fixture.handle.clone();
    let start = tokio::spawn(async move {
        handle
            .start(
                StartOptions {
                    max_duration_seconds: Some(1),
                    ..StartOptions::default()
                },
                policy(),
            )
            .await
    });
    until(|| fixture.io.active.load(Ordering::Acquire) > 0 && !fixture.handle.snapshot().is_null())
        .await;
    start.abort();
    let _ = start.await;
    fixture.clock.store(EPOCH + 1000, Ordering::Release);
    fixture.io.state.lock().unwrap().paused = true;
    fixture
        .io
        .emit(
            "RecordStateChanged",
            json!({"outputState":"OBS_WEBSOCKET_OUTPUT_PAUSED"}),
        )
        .await;
    fixture.clock.store(EPOCH + 2500, Ordering::Release);
    until(|| fixture.io.state.lock().unwrap().stop_calls == 1).await;
    assert!(
        fixture.io.active.load(Ordering::Acquire) > 0,
        "monitor setup was abandoned at emitted stop result"
    );
    fixture.io.setup_gate.notify_waiters();
    until(|| fixture.handle.snapshot().is_null()).await;
    assert!(!fixture.io.state.lock().unwrap().active);
    let stopped = fixture.handle.stop(None, policy()).await.unwrap();
    assert_eq!(stopped["structuredContent"]["take"]["durationSeconds"], 1.0);
    fixture.close().await;
}

#[tokio::test]
async fn overlapping_stop_waiters_share_explicit_rejection_and_a_new_observed_attempt_is_separate()
{
    let fixture = Fixture::new(Plan {
        stop_mode: "rejected".into(),
        ..Plan::default()
    })
    .await;
    fixture
        .handle
        .start(StartOptions::default(), policy())
        .await
        .unwrap();
    fixture.io.hold_stop.store(true, Ordering::Release);
    let a = fixture.handle.clone();
    let b = fixture.handle.clone();
    let first = tokio::spawn(async move { a.stop(None, policy()).await });
    let second = tokio::spawn(async move { b.stop(None, policy()).await });
    until(|| fixture.io.active.load(Ordering::Acquire) > 0).await;
    sleep(Duration::from_millis(10)).await;
    fixture.io.stop_gate.notify_waiters();
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(first, second);
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 1);
    fixture.io.hold_stop.store(false, Ordering::Release);
    fixture.io.state.lock().unwrap().plan.stop_mode = String::new();
    assert_eq!(
        fixture.handle.stop(None, policy()).await.unwrap()["structuredContent"]["stopConfirmed"],
        true
    );
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 2);
    fixture.close().await;
}

fn source(dir: &Dir, path: &Path, input: &Value) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let output=Command::new("node").arg("--input-type=module").arg("-e").arg(r#"
import {EventEmitter} from 'node:events';import crypto from 'node:crypto';import {PassThrough} from 'node:stream';
import cp from 'node:child_process';import {promisify} from 'node:util';import {syncBuiltinESMExports} from 'node:module';
const input=JSON.parse(process.env.TAKE_OWNER_TEST_INPUT),effects=[],processes=[];
let clock=1793448000000,sequence=0;Date.now=()=>clock;
crypto.randomUUID=()=>((++sequence).toString(16).padStart(8,'0')+'-0000-4000-8000-000000000000');
const clip=input.clip??{};
const execute=async(command,args)=>{
 processes.push({command,args});
 if(command==='ffprobe')return {stdout:JSON.stringify({format:{duration:String(clip.duration??0)},streams:[...Array(clip.video??1)].map(()=>({codec_type:'video'})).concat([...Array(clip.audio??0)].map(()=>({codec_type:'audio'})))}),stderr:''};
 if(command==='ffmpeg')return {stdout:'',stderr:'max_volume: '+(clip.max??'-inf')+' dB'};
 throw Error('Unexpected process '+command);
};
const exec=(command,args,options,callback)=>{execute(command,args).then(({stdout,stderr})=>callback(null,stdout,stderr),callback)};
exec[promisify.custom]=execute;cp.execFile=exec;
// Exercise the actual bounded streaming source with PCM and real event order.
// The old execFile stderr fixture cannot stand in for decoded sample evidence.
const children=new Set();
cp.spawn=(command,args,options)=>{
 if(command!=='ffmpeg')throw Error('Unexpected spawned process '+command);
 processes.push({command,args,options});
 const child=new EventEmitter();child.stdout=new PassThrough();child.stderr=new PassThrough();
 child.kill=()=>{throw Error('Unexpected fixture media cancellation')};children.add(child);
 queueMicrotask(()=>{
  const raw=clip.max??'-inf';const peak=raw==='-inf'?0:Math.min(32767,Math.round(32768*Math.pow(10,Number(raw)/20)));
  const pcm=Buffer.alloc(2);pcm.writeInt16LE(peak);
  // Split a sample over chunk boundaries; diagnostic text is not peak authority.
  child.stdout.write(pcm.subarray(0,1));child.stdout.end(pcm.subarray(1));child.stderr.end('max_volume: +20 dB spoof ignored');
  child.emit('exit',0,null);queueMicrotask(()=>{children.delete(child);child.emit('close',0,null)});
 });return child;
};syncBuiltinESMExports();
const {RECORD_OUTPUT}=await import('./build/tools/output-start.js');RECORD_OUTPUT.timeoutMs=20;
const {initialize,currentTake,stopTake,withTakeLock}=await import('./build/tools/takes.js');
let recording=false,paused=false,stopMode=input.stopMode??'',subscriptions=0;
class Client extends EventEmitter {
 async connect(){}
 getConnectionStatus(){return {url:input.local?'ws://localhost:4455':'ws://remote.example:4455',generation:7,versionInfo:{platform:'macos',supportedImageFormats:['png']}}}
 hasPendingOutputSettings(){return false}
 subscribeHighVolume(){subscriptions++;return ()=>{subscriptions--;}}
 async sendBatch(){return []}
 async sendRequest(type,data){
  if(type==='GetProfileParameter'){const values={'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'};return {parameterValue:values[data.parameterCategory+'/'+data.parameterName]??null};}
  if(type==='GetRecordDirectory')return {recordDirectory:input.directory};
  if(type==='GetStats')return {availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0};
  if(type==='GetInputList')return {inputs:[]};
  if(type==='GetCurrentProgramScene')return {currentProgramSceneName:'Demo'};
  if(type==='GetSceneItemList')return {sceneItems:[{sceneItemEnabled:true,sceneItemId:1,sourceName:'Color',inputKind:'color_source',sceneItemTransform:{sourceWidth:32,sourceHeight:32}}]};
  if(type==='GetRecordStatus')return {outputActive:recording,outputPaused:paused,outputBytes:1048576};
  if(type==='StartRecord'){
   effects.push(type);if(input.startError){const {OBSRequestError}=await import('./build/client.js');throw new OBSRequestError(type,500,'fixture start rejected')}
   recording=true;if(!input.noStartEvent)this.emit('RecordStateChanged',{outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputActive:true,outputPath:input.path});if(input.startUnknown)throw Error('fixture lost start reply');return {};
  }
  if(type==='StopRecord'){
   effects.push(type);
   if(stopMode==='rejected'){const {OBSRequestError}=await import('./build/client.js');throw new OBSRequestError(type,500,'fixture stop rejected')}
   if(stopMode==='unknown')throw Error('fixture lost stop reply');
   recording=false;this.emit('RecordStateChanged',{outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED',outputActive:false,outputPath:input.path});
   if(stopMode==='unknown-stopped')throw Error('fixture lost stop reply');return {};
  }
  if(type==='CreateRecordChapter'){
   effects.push(type);if(input.chapterError){const {OBSRequestError}=await import('./build/client.js');throw new OBSRequestError(type,500,'fixture chapter rejected')}return {};
  }
  throw Error('Unexpected fixture request '+type);
 }
}
const client=new Client(),handlers={};
const server={server:{getClientVersion(){return {name:'tester'}}},registerTool(name,_definition,callback){handlers[name]=callback}};
initialize(server,client);withTakeLock(server,client).registerTool('obs-set-video-settings',{},async()=>({content:[{type:'text',text:'allowed'}]}));
const results=[];
const save=result=>results.push(structuredClone(result));
try{
 for(const action of input.actions){
  if(action.kind==='clock'){clock=1793448000000+action.ms;continue;}
  if(action.kind==='mode'){stopMode=action.value;continue;}
  if(action.kind==='emit'){
   if(action.active!==undefined)recording=action.active;if(action.paused!==undefined)paused=action.paused;
   client.emit(action.event,action.data);continue;
  }
  if(action.kind==='start')save(await handlers['obs-take-start']({expectSilent:false,minFreeDiskMb:1024,lock:true,...action.options},{}));
  if(action.kind==='mark')save(await handlers['obs-take-mark']({name:action.name},{}));
  if(action.kind==='status')save(await handlers['obs-take-status']({},{}));
  if(action.kind==='stop')save(action.requested===undefined?await handlers['obs-take-stop']({},{}):await stopTake(client,currentTake(client),{requestedSeconds:action.requested}));
  if(action.kind==='lock')save(await handlers['obs-set-video-settings']({},{}));
 }
 console.log(JSON.stringify({results,effects,processes}));
}finally{
 const take=currentTake(client);if(take){recording=false;client.emit('RecordStateChanged',{outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED',outputActive:false,outputPath:input.path});await stopTake(client,take);}
 if(subscriptions!==0)throw Error('source monitor observers not released');
 if(children.size!==0)throw Error('source media children not closed');
}
"#).current_dir(root).env("OBS_MCP_STATE_DIR",dir.path("source-state")).env("TAKE_OWNER_TEST_INPUT",{
        let mut input=input.clone();input["path"]=json!(path);input["directory"]=json!(dir.0);input.to_string()
    }).output().unwrap();
    assert!(
        output.status.success(),
        "actual takes source oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn normalize(mut value: Value) -> Value {
    fn ids(value: &Value, takes: &mut Vec<String>) {
        if let Some(id) = value.get("takeId").and_then(Value::as_str)
            && !takes.iter().any(|known| known == id)
        {
            takes.push(id.into());
        }
        match value {
            Value::Object(object) => {
                for value in object.values() {
                    ids(value, takes);
                }
            }
            Value::Array(array) => {
                for value in array {
                    ids(value, takes);
                }
            }
            _ => {}
        }
    }
    fn visit(value: &mut Value, key: &str, takes: &[String]) {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    visit(value, key, takes);
                }
            }
            Value::Array(array) => {
                for value in array {
                    visit(value, key, takes);
                }
            }
            Value::String(text) if key == "operationId" => *text = "operation-id".into(),
            Value::String(text) if key == "takeId" || key == "text" => {
                for (index, id) in takes.iter().enumerate() {
                    *text = text.replace(id, &format!("take-{}", index + 1));
                }
            }
            Value::Number(number) => *value = json!(number.as_f64().unwrap()),
            _ => {}
        }
    }
    let mut takes = vec![];
    ids(&value, &mut takes);
    visit(&mut value, "", &takes);
    value
}
async fn compare(input: Value) -> Value {
    let plan = Plan {
        no_start_event: input["noStartEvent"] == true,
        start_error: input["startError"] == true,
        stop_mode: input["stopMode"].as_str().unwrap_or("").into(),
        local: input["local"] == true,
        chapter_error: input["chapterError"] == true,
        start_unknown: input["startUnknown"] == true,
        clip: ClipCheck {
            duration_seconds: input["clip"]["duration"].as_f64(),
            audio_streams: input["clip"]["audio"].as_u64().map(|n| n as usize),
            video_streams: input["clip"]["video"].as_u64().map(|n| n as usize),
            max_volume_db: input["clip"]["max"].as_str().map(|value| {
                if value == "-inf" {
                    // Source PCM verification projects digital silence to -91dB.
                    -91.0
                } else {
                    value.parse().unwrap()
                }
            }),
            ..ClipCheck::default()
        },
    };
    let fixture = Fixture::new(plan).await;
    let expected = source(&fixture.dir, &fixture.io.output, &input);
    for process in expected["processes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|process| process["command"] == "ffmpeg")
    {
        assert_eq!(
            process["options"],
            json!({"stdio":["ignore","pipe","pipe"]})
        );
        assert_eq!(
            process["args"],
            json!([
                "-v",
                "error",
                "-hide_banner",
                "-nostats",
                "-protocol_whitelist",
                "file,pipe",
                "-format_whitelist",
                "matroska,webm,mov,mp4,m4a,3gp,3g2,mj2,wav,avi,flv,mpegts,ogg,nut,yuv4mpegpipe",
                "-i",
                fixture.io.output,
                "-map",
                "0:a:0",
                "-c:a",
                "pcm_s16le",
                "-f",
                "s16le",
                "-"
            ])
        );
    }
    let source_log = fs::read(fixture.io.output.with_extension("take.json"))
        .ok()
        .map(|bytes| serde_json::from_slice::<Value>(&bytes).unwrap());
    let mut results = vec![];
    for action in input["actions"].as_array().unwrap() {
        match action["kind"].as_str().unwrap() {
            "clock" => fixture
                .clock
                .store(EPOCH + action["ms"].as_u64().unwrap(), Ordering::Release),
            "mode" => {
                fixture.io.state.lock().unwrap().plan.stop_mode =
                    action["value"].as_str().unwrap().into()
            }
            "emit" => {
                {
                    let mut state = fixture.io.state.lock().unwrap();
                    if let Some(active) = action["active"].as_bool() {
                        state.active = active;
                    }
                    if let Some(paused) = action["paused"].as_bool() {
                        state.paused = paused;
                    }
                }
                fixture
                    .io
                    .emit(action["event"].as_str().unwrap(), action["data"].clone())
                    .await;
            }
            "start" => {
                let options = &action["options"];
                results.push(
                    fixture
                        .handle
                        .start(
                            StartOptions {
                                expect_silent: options["expectSilent"] == true,
                                lock: options["lock"] != false,
                                chapter: options["chapter"].as_str().map(str::to_owned),
                                require_verified_preflight: options["requireVerifiedPreflight"]
                                    == true,
                                ..StartOptions::default()
                            },
                            policy(),
                        )
                        .await
                        .unwrap(),
                );
            }
            "mark" => results.push(
                fixture
                    .handle
                    .mark(action["name"].as_str().unwrap().into(), policy())
                    .await
                    .unwrap(),
            ),
            "status" => results.push(fixture.handle.status().await.unwrap()),
            "stop" => results.push(
                fixture
                    .handle
                    .stop(action["requested"].as_f64(), policy())
                    .await
                    .unwrap(),
            ),
            "lock" => {
                use obs_mcp_native::control::{ConfirmationContext, GateRequest};
                let value = fixture
                    .control
                    .lock()
                    .unwrap()
                    .evaluate(
                        GateRequest {
                            tool_name: "obs-set-video-settings",
                            arguments: &json!({}),
                            read_only_hint: false,
                            caller: "tester",
                            confirmation: ConfirmationContext::default(),
                        },
                        fixture.clock.load(Ordering::Acquire),
                    )
                    .tool_result()
                    .unwrap_or(json!({"content":[{"type":"text","text":"allowed"}]}));
                results.push(value);
            }
            _ => panic!("unknown action"),
        }
    }
    assert_eq!(
        normalize(json!(results)),
        normalize(expected["results"].clone()),
        "public result fixture: {input}"
    );
    assert_eq!(
        json!(fixture.io.state.lock().unwrap().effects),
        expected["effects"]
    );
    if let Some(source_log) = source_log {
        let actual: Value = serde_json::from_slice(
            &fs::read(fixture.io.output.with_extension("take.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            normalize(actual),
            normalize(source_log),
            "actual log-file payload"
        );
    }
    fixture.close().await;
    expected
}

#[tokio::test]
async fn actual_source_public_start_mark_status_lock_stop_and_completed_result_match() {
    compare(json!({"actions":[{"kind":"status"},{"kind":"start"},{"kind":"start"},{"kind":"mark","name":"step"},{"kind":"status"},{"kind":"lock"},{"kind":"stop"},{"kind":"stop"},{"kind":"lock"}]})).await;
    compare(json!({"chapterError":true,"actions":[{"kind":"start","options":{"lock":false,"chapter":"intro"}},{"kind":"mark","name":"step"},{"kind":"lock"},{"kind":"stop"}]})).await;
}

#[tokio::test]
async fn actual_source_lost_stop_reconciliation_and_replacement_refusal_match() {
    compare(json!({"stopMode":"unknown-stopped","actions":[{"kind":"start"},{"kind":"stop"},{"kind":"stop"}]})).await;
    compare(json!({"actions":[{"kind":"start"},{"kind":"emit","event":"RecordStateChanged","active":false,"data":{"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED","outputActive":false}},{"kind":"emit","event":"RecordStateChanged","active":true,"data":{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputActive":true,"outputPath":"/fixture/human.mkv"}},{"kind":"stop"},{"kind":"emit","event":"RecordStateChanged","active":false,"data":{"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED","outputActive":false}},{"kind":"stop"}]})).await;
    compare(json!({"noStartEvent":true,"actions":[{"kind":"start"},{"kind":"stop"}]})).await;
    compare(
        json!({"startError":true,"actions":[{"kind":"start"},{"kind":"status"},{"kind":"stop"}]}),
    )
    .await;
}

#[tokio::test]
async fn actual_source_local_file_report_log_and_exact_decimal_rounding_match() {
    compare(json!({"local":true,"clip":{"duration":2.25,"video":1,"audio":1,"max":"-inf"},"actions":[{"kind":"start","options":{"expectSilent":true}},{"kind":"clock","ms":5000},{"kind":"mark","name":"step"},{"kind":"stop","requested":10}]})).await;
    compare(json!({"local":true,"clip":{"duration":2.55,"video":0,"audio":1,"max":"-20"},"actions":[{"kind":"start","options":{"expectSilent":true}},{"kind":"stop"}]})).await;
}

#[tokio::test]
async fn lost_start_and_strict_preflight_refusal_match_source_without_mutation_replay() {
    compare(json!({"startUnknown":true,"actions":[{"kind":"start"},{"kind":"status"},{"kind":"stop"},{"kind":"start"}]})).await;
    let result=compare(json!({"actions":[{"kind":"start","options":{"requireVerifiedPreflight":true}},{"kind":"stop"}]})).await;
    assert_eq!(result["effects"], json!([]));
}

#[tokio::test]
async fn cancelled_stop_frontend_and_resumed_shutdown_keep_one_owned_effect_and_join() {
    let mut fixture = Fixture::new(Plan::default()).await;
    fixture
        .handle
        .start(StartOptions::default(), policy())
        .await
        .unwrap();
    fixture.io.hold_stop.store(true, Ordering::Release);
    let handle = fixture.handle.clone();
    let front = tokio::spawn(async move { handle.stop(None, policy()).await });
    until(|| fixture.io.active.load(Ordering::Acquire) > 0).await;
    front.abort();
    let _ = front.await;
    {
        let closing = fixture.owner.shutdown();
        tokio::pin!(closing);
        tokio::select! {result=&mut closing=>panic!("cleanup returned before held effect finished: {result:?}"),_=sleep(Duration::from_millis(20))=>{}}
    }
    assert!(!fixture.io.joined.load(Ordering::Acquire));
    fixture.io.stop_gate.notify_waiters();
    let receipt = timeout(WAIT, fixture.owner.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.stop_confirmed);
    assert!(receipt.observers_joined);
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 1);
    assert_eq!(
        *fixture.io.intent_phases.lock().unwrap(),
        ["record-start", "record-stop"]
    );
    assert_eq!(fixture.io.active.load(Ordering::Acquire), 0);
    fixture.close().await;
}

#[tokio::test]
async fn stop_guard_rejects_replacement_that_appeared_during_queue_wait() {
    let fixture = Fixture::new(Plan::default()).await;
    fixture
        .handle
        .start(StartOptions::default(), policy())
        .await
        .unwrap();
    fixture.io.hold_stop.store(true, Ordering::Release);
    let handle = fixture.handle.clone();
    let front = tokio::spawn(async move { handle.stop(None, policy()).await });
    until(|| fixture.io.active.load(Ordering::Acquire) > 0).await;
    fixture
        .io
        .emit(
            "RecordStateChanged",
            json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"}),
        )
        .await;
    fixture.io.emit("RecordStateChanged",json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":"/fixture/replacement.mkv"})).await;
    fixture.io.stop_gate.notify_waiters();
    let refused = front.await.unwrap().unwrap();
    assert_eq!(refused["structuredContent"]["stopConfirmed"], false);
    assert!(
        refused["structuredContent"]["reason"]
            .as_str()
            .unwrap()
            .contains("ownership changed before dispatch")
    );
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 0);
    fixture.io.state.lock().unwrap().active = false;
    fixture.handle.stop(None, policy()).await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn missing_durable_start_intent_dispatches_nothing_and_returns_the_writer_only_after_join() {
    let fixture = Fixture::new(Plan::default()).await;
    fs::write(
        fixture.dir.path("native-state"),
        b"fixture blocks state directory",
    )
    .unwrap();
    let refused = fixture
        .handle
        .start(StartOptions::default(), policy())
        .await
        .unwrap();
    assert_eq!(refused["isError"], true);
    assert!(
        refused["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("receipt could not be persisted")
    );
    assert!(fixture.io.state.lock().unwrap().effects.is_empty());
    assert!(fixture.io.intent_phases.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn an_early_unknown_status_keeps_the_independent_stop_deadline_armed() {
    let fixture = Fixture::new(Plan::default()).await;
    fixture
        .handle
        .start(
            StartOptions {
                max_duration_seconds: Some(1),
                ..StartOptions::default()
            },
            policy(),
        )
        .await
        .unwrap();
    fixture.io.fail_status.store(1, Ordering::Release);
    let unknown = fixture.handle.stop(None, policy()).await.unwrap();
    assert_eq!(
        unknown["structuredContent"]["reason"],
        "Recording status is unavailable"
    );
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 0);
    until(|| fixture.handle.snapshot().is_null()).await;
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 1);
    assert!(!fixture.io.state.lock().unwrap().active);
    fixture.close().await;
}

#[tokio::test]
async fn emergency_owner_drop_aborts_owned_workers_without_claiming_obs_stopped() {
    let fixture = Fixture::new(Plan::default()).await;
    fixture.io.hold_setup.store(true, Ordering::Release);
    let handle = fixture.handle.clone();
    let front = tokio::spawn(async move { handle.start(StartOptions::default(), policy()).await });
    until(|| !fixture.handle.snapshot().is_null() && fixture.io.active.load(Ordering::Acquire) > 0)
        .await;
    drop(fixture.owner);
    assert_eq!(fixture.handle.snapshot()["status"], "unknown");
    until(|| fixture.io.active.load(Ordering::Acquire) == 0).await;
    assert!(fixture.io.state.lock().unwrap().active);
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 0);
    assert!(!fixture.io.joined.load(Ordering::Acquire));
    assert!(front.await.unwrap().is_err());
    fixture.session.shutdown().await.unwrap();
    timeout(WAIT, fixture.peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn incomplete_physical_event_masks_return_the_untouched_loan_before_workers_or_writes() {
    let fixture = Fixture::new(Plan::default()).await;
    for mask in [0, 1 << 6, obs_mcp_native::session::ALL_EVENTS] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            socket
                .send(Message::Text(
                    json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let identify = socket.next().await.unwrap().unwrap();
            let Message::Text(identify) = identify else {
                panic!("expected Identify")
            };
            assert_eq!(
                serde_json::from_str::<Value>(&identify).unwrap()["d"]["eventSubscriptions"],
                mask
            );
            socket
                .send(Message::Text(
                    json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            if let Some(frame) = socket.next().await {
                assert!(
                    matches!(frame, Ok(Message::Close(_))),
                    "rejected take loan wrote a request"
                );
                let _ = socket.flush().await;
            }
        });
        let (session, events) = ObsSession::connect(
            &url,
            None,
            SessionOptions {
                event_subscriptions: mask,
                ..SessionOptions::default()
            },
        )
        .await
        .unwrap();
        let root = fixture.dir.path(&format!("mask-{mask}"));
        let store = OperationStore::new(&root, &url).unwrap();
        let (_, receiver) = mpsc::channel(1);
        let wall: Clock =
            Arc::new(|| clock::from_epoch_duration(Duration::from_millis(EPOCH)).unwrap());
        let rejected = TakeSupervisor::spawn(
            Loan {
                session: session.handle(),
                store,
                control: fixture.control.clone(),
                clock: wall,
                identities: Box::new(|| panic!("rejected admission allocated identity")),
                io: fixture.io.clone(),
                events: receiver,
            },
            Limits::default(),
        );
        let failure = match rejected {
            Err(failure) => failure,
            Ok(mut owner) => {
                owner.shutdown().await.unwrap();
                panic!("incomplete events were admitted")
            }
        };
        assert_eq!(
            failure.reason,
            obs_mcp_native::takes::OwnerError::IncompleteEvents
        );
        assert_eq!(
            failure.loan.store.endpoint_hash(),
            session.handle().endpoint_hash()
        );
        assert!(!root.exists(), "admission wrote the journal");
        assert_eq!(fixture.io.active.load(Ordering::Acquire), 0);
        drop(failure);
        session.shutdown().await.unwrap();
        drop(events);
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
    fixture.close().await;
}

#[tokio::test]
async fn held_timeout_work_blocks_readmission_frontend_completion_and_writer_return_until_settled()
{
    let mut observations = vec![];
    for lane in ["setup", "inspection"] {
        let mut fixture = Fixture::with_limits(
            Plan {
                local: lane == "inspection",
                clip: ClipCheck {
                    duration_seconds: Some(0.0),
                    video_streams: Some(1),
                    audio_streams: Some(0),
                    ..ClipCheck::default()
                },
                ..Plan::default()
            },
            Limits {
                setup: Duration::from_millis(20),
                inspection: Duration::from_millis(20),
                rpc: Duration::from_millis(400),
                stop: Duration::from_millis(400),
                start_confirmation: Duration::from_millis(20),
                ..Limits::default()
            },
        )
        .await;
        let front = if lane == "setup" {
            fixture.io.hold_setup.store(true, Ordering::Release);
            let handle = fixture.handle.clone();
            tokio::spawn(async move { handle.start(StartOptions::default(), policy()).await })
        } else {
            fixture
                .handle
                .start(StartOptions::default(), policy())
                .await
                .unwrap();
            fixture.io.hold_inspection.store(true, Ordering::Release);
            let handle = fixture.handle.clone();
            tokio::spawn(async move { handle.stop(None, policy()).await })
        };
        until(|| {
            !fixture.handle.snapshot().is_null() && fixture.io.active.load(Ordering::Acquire) > 0
        })
        .await;
        sleep(Duration::from_millis(60)).await; // exceed the explicit20ms IO deadline
        let retained = fixture.io.active.load(Ordering::Acquire) > 0;
        let frontend_pending = !front.is_finished();
        let readmission = fixture
            .handle
            .start(StartOptions::default(), policy())
            .await
            .unwrap();
        let refused = readmission["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("already recording");
        let starts = fixture
            .io
            .state
            .lock()
            .unwrap()
            .effects
            .iter()
            .filter(|effect| *effect == "StartRecord")
            .count();
        let early_join = {
            let close = fixture.owner.shutdown();
            tokio::pin!(close);
            tokio::select! {result=&mut close=>{result.unwrap();true},_=sleep(Duration::from_millis(20))=>false}
        };
        if lane == "setup" {
            fixture.io.setup_gate.notify_waiters();
        } else {
            fixture.io.inspection_gate.notify_waiters();
        }
        let _ = timeout(WAIT, front).await.unwrap().unwrap();
        fixture.close().await;
        observations.push((
            lane,
            retained,
            frontend_pending,
            refused,
            starts,
            early_join,
        ));
    }
    // Assert after explicit cleanup so the intended failing negative control
    // still joins every owned fixture task/socket and leaves no test helpers.
    assert!(
        observations
            .iter()
            .all(|(_, retained, pending, refused, starts, early)| *retained
                && *pending
                && *refused
                && *starts == 1
                && !*early),
        "timed-out work was discarded/re-admitted/released before actual settle: {observations:?}"
    );
}

#[tokio::test]
async fn timed_out_dispatched_stop_drains_late_reply_as_unknown_then_reconciles_without_replay() {
    let fixture = Fixture::with_limits(
        Plan::default(),
        Limits {
            rpc: Duration::from_millis(400),
            stop: Duration::from_millis(20),
            start_confirmation: Duration::from_millis(20),
            ..Limits::default()
        },
    )
    .await;
    fixture
        .handle
        .start(StartOptions::default(), policy())
        .await
        .unwrap();
    fixture.io.hold_stop_reply.store(true, Ordering::Release);
    let handle = fixture.handle.clone();
    let front = tokio::spawn(async move { handle.stop(None, policy()).await });
    until(|| fixture.io.state.lock().unwrap().stop_calls == 1).await;
    sleep(Duration::from_millis(60)).await;
    assert!(
        !front.is_finished(),
        "timeout discarded the dispatched effect observer"
    );
    assert!(fixture.io.active.load(Ordering::Acquire) > 0);
    fixture.io.stop_reply_gate.notify_waiters();
    let late = front.await.unwrap().unwrap();
    assert_eq!(
        late["structuredContent"]["stopConfirmed"], false,
        "late success must remain unknown"
    );
    let reconciled = fixture.handle.stop(None, policy()).await.unwrap();
    assert_eq!(reconciled["structuredContent"]["stopConfirmed"], true);
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 1);
    fixture.close().await;
}

#[tokio::test]
async fn failed_observer_cleanup_refuses_readmission_and_writer_extraction() {
    let mut fixture = Fixture::new(Plan::default()).await;
    fixture
        .handle
        .start(StartOptions::default(), policy())
        .await
        .unwrap();
    fixture.io.observer_failure.store(true, Ordering::Release);
    let stopped = fixture.handle.stop(None, policy()).await.unwrap();
    assert_eq!(stopped["structuredContent"]["stopConfirmed"], false);
    let refused = fixture
        .handle
        .start(StartOptions::default(), policy())
        .await
        .unwrap();
    assert!(
        refused["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("already recording")
    );
    let receipt = fixture.owner.shutdown().await.unwrap();
    assert!(!receipt.observers_joined);
    assert!(
        fixture.owner.take_store().is_none(),
        "unconfirmed observer cleanup released writer loan"
    );
    assert_eq!(fixture.io.state.lock().unwrap().stop_calls, 1);
    fixture.session.shutdown().await.unwrap();
    timeout(WAIT, fixture.peer).await.unwrap().unwrap();
}
