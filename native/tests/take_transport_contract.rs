// SPDX-License-Identifier: GPL-2.0-only
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    observation::ReadRequest,
    preflight,
    session::{ConnectionToken, ObsSession, SessionHandle, SessionOptions},
    take_monitor::Summary,
    take_transport::{
        BoundTakeIo, Bounds, EventFence, EventFuture, HostServices, LocalHost, PreflightReader,
        decode_ppm,
    },
    takes::{self, ClipCheck, DispatchError, Effect, IoFuture, TakeIo},
};
use serde_json::{Value, json};
use std::{
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(3);
type Peer = WebSocketStream<TcpStream>;
async fn send(peer: &mut Peer, value: Value) {
    peer.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn reply(peer: &mut Peer, request: &Value, data: Value) {
    send(peer,json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":request["d"]["requestType"],"requestStatus":{"result":true,"code":100},"responseData":data}})).await;
}
async fn event(peer: &mut Peer, name: &str) {
    send(peer,json!({"op":5,"d":{"eventType":name,"eventIntent":if name=="InputVolumeMeters"{1<<16}else{4},"eventData":{"sceneName":"Replacement"}}})).await;
}
struct Hooks {
    hold_stats: AtomicBool,
    stats_seen: AtomicBool,
    stats_gate: Notify,
    delay_stop: AtomicBool,
    lose_stop: AtomicBool,
    reject_stop: AtomicBool,
    picture_event: Mutex<Option<String>>,
    version_event: AtomicBool,
    trace: Mutex<Vec<Value>>,
    ppm: Mutex<String>,
    available: Mutex<Vec<String>>,
}
impl Default for Hooks {
    fn default() -> Self {
        Self {
            hold_stats: AtomicBool::new(false),
            stats_seen: AtomicBool::new(false),
            stats_gate: Notify::new(),
            delay_stop: AtomicBool::new(false),
            lose_stop: AtomicBool::new(false),
            reject_stop: AtomicBool::new(false),
            picture_event: Mutex::new(None),
            version_event: AtomicBool::new(false),
            trace: Mutex::new(vec![]),
            ppm: Mutex::new(STANDARD.encode(b"P6\n2 1\n255\n\xff\x00\x00\x00\xff\x00")),
            available: Mutex::new(
                [
                    "GetVersion",
                    "GetStats",
                    "GetCurrentProgramScene",
                    "GetInputList",
                    "GetInputAudioTracks",
                    "GetProfileParameter",
                    "GetSourceScreenshot",
                    "StartRecord",
                    "StopRecord",
                    "CreateRecordChapter",
                ]
                .map(str::to_owned)
                .to_vec(),
            ),
        }
    }
}
struct Fence {
    handle: SessionHandle,
    processed: AtomicU64,
    revision: AtomicU64,
    alive: AtomicBool,
    hold: AtomicBool,
    changed: Notify,
    sync_delay_ms: AtomicU64,
}
impl EventFence for Fence {
    fn token(&self) -> ConnectionToken {
        self.handle.token()
    }
    fn ready_at_cursor(&self, cursor: u64) -> bool {
        self.alive.load(Ordering::Acquire)
            && self.handle.invalidation().is_none()
            && self.processed.load(Ordering::Acquire) >= cursor
            && self.handle.event_cursor() == cursor
    }
    fn picture_revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
    fn synchronize(&self, deadline: Instant) -> EventFuture<'_> {
        Box::pin(async move {
            // Simulate a ready event consumer being resumed after its caller's
            // budget elapsed. Readiness must not create a fresh send budget.
            let delay = self.sync_delay_ms.swap(0, Ordering::AcqRel);
            if delay > 0 {
                sleep(Duration::from_millis(delay)).await;
            }
            loop {
                let changed = self.changed.notified();
                if self.ready_at_cursor(self.handle.event_cursor()) {
                    return Ok(());
                }
                if !self.alive.load(Ordering::Acquire) || self.handle.invalidation().is_some() {
                    return Err("test observation owner ended".into());
                }
                tokio::time::timeout_at(deadline, changed)
                    .await
                    .map_err(|_| "test event barrier deadline".to_owned())?;
            }
        })
    }
}
struct Host;
impl HostServices for Host {
    fn preflight(&self, _: PreflightReader, _: preflight::Options) -> IoFuture<preflight::Report> {
        Box::pin(async {
            preflight::Report {
                ready: false,
                checks: vec![],
            }
        })
    }
    fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
        Box::pin(async { ClipCheck::default() })
    }
    fn write_log(&self, _: String, _: Summary) -> IoFuture<Option<String>> {
        Box::pin(async { None })
    }
    fn join_observers(&self) -> IoFuture<Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}
struct Fixture {
    url: String,
    owner: Option<ObsSession>,
    handle: SessionHandle,
    fence: Arc<Fence>,
    hooks: Arc<Hooks>,
    pump: JoinHandle<()>,
    peer: JoinHandle<()>,
}
impl Fixture {
    async fn new(hooks: Arc<Hooks>, mask: u64, hold_events: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let state = hooks.clone();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            send(
                &mut peer,
                json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}}),
            )
            .await;
            let identify = peer.next().await.unwrap().unwrap();
            let identify: Value = serde_json::from_str(identify.to_text().unwrap()).unwrap();
            assert_eq!(identify["op"], 1);
            assert_eq!(identify["d"]["eventSubscriptions"], mask);
            send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
            while let Some(message) = peer.next().await {
                let Ok(message) = message else { break };
                match message {
                    Message::Close(_) => {
                        let _ = peer.flush().await;
                        break;
                    }
                    Message::Text(text) => {
                        let request: Value = serde_json::from_str(&text).unwrap();
                        state.trace.lock().unwrap().push(request.clone());
                        if request["op"] == 8 {
                            let results=request["d"]["requests"].as_array().unwrap().iter().map(|request|json!({"requestId":request["requestId"],"requestType":request["requestType"],"requestStatus":{"result":request["requestData"]["inputName"]!="video","code":if request["requestData"]["inputName"]=="video"{604}else{100}},"responseData":{"inputAudioTracks":{"1":false,"2":true}}})).collect::<Vec<_>>();
                            send(&mut peer,json!({"op":9,"d":{"requestId":request["d"]["requestId"],"results":results}})).await;
                            continue;
                        }
                        let name = request["d"]["requestType"].as_str().unwrap();
                        let data = match name {
                            "GetVersion" => {
                                if state.version_event.load(Ordering::Acquire) {
                                    event(&mut peer, "CurrentProgramSceneChanged").await;
                                }
                                json!({"availableRequests":state.available.lock().unwrap().clone(),"supportedImageFormats":["ppm","png"],"platform":"macos"})
                            }
                            "GetStats" => {
                                if state.hold_stats.swap(false, Ordering::AcqRel) {
                                    state.stats_seen.store(true, Ordering::Release);
                                    state.stats_gate.notified().await;
                                }
                                json!({"renderTotalFrames":12,"outputTotalFrames":12})
                            }
                            "GetCurrentProgramScene" => {
                                json!({"currentProgramSceneName":"Program"})
                            }
                            "GetInputList" => {
                                json!({"inputs":[{"inputName":"mic"},{"inputName":"video"}]})
                            }
                            "GetProfileParameter" => {
                                json!({"parameterValue":match request["d"]["requestData"]["parameterName"].as_str().unwrap(){"Mode"=>"Advanced","RecType"=>"Standard","RecTracks"=>"2",_=>panic!("unexpected profile parameter")}})
                            }
                            "GetSourceScreenshot" => {
                                let name = state.picture_event.lock().unwrap().take();
                                if let Some(name) = name {
                                    event(&mut peer, &name).await;
                                }
                                json!({"imageData":format!("data:image/x-portable-pixmap;base64,{}",state.ppm.lock().unwrap())})
                            }
                            "StopRecord" => {
                                if state.reject_stop.load(Ordering::Acquire) {
                                    send(&mut peer,json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":"StopRecord","requestStatus":{"result":false,"code":604,"comment":"concrete source rejection"}}})).await;
                                    continue;
                                }
                                if state.delay_stop.load(Ordering::Acquire) {
                                    sleep(Duration::from_millis(80)).await;
                                }
                                if state.lose_stop.load(Ordering::Acquire) {
                                    return;
                                }
                                json!({"outputPath":"/approved/take.mkv"})
                            }
                            "StartRecord" | "CreateRecordChapter" => json!({}),
                            _ => panic!("unexpected request {name}"),
                        };
                        reply(&mut peer, &request, data).await;
                    }
                    Message::Ping(_) => {
                        let _ = peer.flush().await;
                    }
                    _ => {}
                }
            }
        });
        let (owner, mut events) = ObsSession::connect(
            &url,
            None,
            SessionOptions {
                event_subscriptions: mask,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let handle = owner.handle();
        let fence = Arc::new(Fence {
            handle: handle.clone(),
            processed: AtomicU64::new(0),
            revision: AtomicU64::new(0),
            alive: AtomicBool::new(true),
            hold: AtomicBool::new(hold_events),
            changed: Notify::new(),
            sync_delay_ms: AtomicU64::new(0),
        });
        let observed = fence.clone();
        let pump = tokio::spawn(async move {
            while let Ok(event) = events.next().await {
                while observed.hold.load(Ordering::Acquire) {
                    observed.changed.notified().await;
                }
                if event.event_type != "InputVolumeMeters" {
                    observed.revision.fetch_add(1, Ordering::AcqRel);
                }
                observed.processed.store(event.sequence, Ordering::Release);
                observed.changed.notify_waiters();
            }
            observed.alive.store(false, Ordering::Release);
            observed.changed.notify_waiters();
        });
        Self {
            url,
            owner: Some(owner),
            handle,
            fence,
            hooks,
            pump,
            peer,
        }
    }
    async fn io(&self) -> Result<BoundTakeIo, String> {
        BoundTakeIo::bind(
            self.handle.clone(),
            self.fence.clone(),
            Arc::new(Host),
            &self.url,
            Arc::new(AtomicBool::new(false)),
            Bounds::default(),
        )
        .await
    }
    fn count(&self, name: &str) -> usize {
        self.hooks
            .trace
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| frame["d"]["requestType"] == name)
            .count()
    }
    async fn close(mut self) {
        self.fence.hold.store(false, Ordering::Release);
        self.fence.changed.notify_waiters();
        let _ = self.owner.take().unwrap().shutdown().await;
        timeout(WAIT, self.pump).await.unwrap().unwrap();
        timeout(WAIT, self.peer).await.unwrap().unwrap();
    }
}
async fn until(mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < deadline, "fixture condition timed out");
        sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::test]
async fn binds_real_capabilities_once_and_runs_closed_effects_on_one_socket() {
    let fixture = Fixture::new(
        Arc::new(Hooks::default()),
        takes::REQUIRED_SUBSCRIPTIONS,
        false,
    )
    .await;
    let io = fixture.io().await.unwrap();
    assert_eq!(fixture.count("GetVersion"), 1);
    for effect in [
        Effect::StartRecord,
        Effect::Chapter("A literal chapter".into()),
        Effect::StopRecord,
    ] {
        io.dispatch(fixture.handle.token(), effect, WAIT, Box::new(|| Ok(())))
            .await
            .unwrap();
    }
    assert_eq!(fixture.count("StartRecord"), 1);
    assert_eq!(fixture.count("StopRecord"), 1);
    assert_eq!(fixture.count("CreateRecordChapter"), 1);
    assert_eq!(
        fixture
            .hooks
            .trace
            .lock()
            .unwrap()
            .iter()
            .find(|r| r["d"]["requestType"] == "CreateRecordChapter")
            .unwrap()["d"]["requestData"],
        json!({"chapterName":"A literal chapter"})
    );
    fixture.close().await;
}

#[tokio::test]
async fn bootstraps_metadata_before_consumer_without_authorizing_pending_events() {
    let hooks = Arc::new(Hooks::default());
    hooks.version_event.store(true, Ordering::Release);
    let fixture = Fixture::new(hooks, takes::REQUIRED_SUBSCRIPTIONS, true).await;
    let io = timeout(Duration::from_millis(200), fixture.io())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.handle.event_cursor(), 1);
    assert!(!fixture.fence.ready_at_cursor(1));
    let mut dispatch = io.dispatch(
        fixture.handle.token(),
        Effect::StartRecord,
        WAIT,
        Box::new(|| Ok(())),
    );
    assert!(poll!(dispatch.as_mut()).is_pending());
    assert_eq!(fixture.count("StartRecord"), 0);
    fixture.fence.hold.store(false, Ordering::Release);
    fixture.fence.changed.notify_waiters();
    dispatch.await.unwrap();
    assert_eq!(fixture.count("StartRecord"), 1);
    fixture.close().await;
}

#[tokio::test]
async fn rejects_wrong_endpoint_masks_and_unadvertised_effect_before_send() {
    let fixture = Fixture::new(Arc::new(Hooks::default()), 0, false).await;
    assert!(fixture.io().await.is_err());
    assert_eq!(fixture.count("GetVersion"), 0);
    fixture.close().await;
    let hooks = Arc::new(Hooks::default());
    hooks
        .available
        .lock()
        .unwrap()
        .retain(|name| name != "StopRecord");
    let fixture = Fixture::new(hooks, takes::REQUIRED_SUBSCRIPTIONS, false).await;
    assert!(
        BoundTakeIo::bind(
            fixture.handle.clone(),
            fixture.fence.clone(),
            Arc::new(Host),
            "ws://127.0.0.1:9",
            Arc::new(AtomicBool::new(false)),
            Bounds::default()
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.count("GetVersion"), 0);
    let io = fixture.io().await.unwrap();
    assert!(matches!(
        io.dispatch(
            fixture.handle.token(),
            Effect::StopRecord,
            WAIT,
            Box::new(|| Ok(()))
        )
        .await,
        Err(DispatchError::BeforeDispatch(_))
    ));
    assert_eq!(fixture.count("StopRecord"), 0);
    fixture.close().await;
}

#[tokio::test]
async fn queued_policy_or_replacement_refusal_runs_at_actual_send() {
    for reason in ["lease changed", "replacement recording"] {
        let hooks = Arc::new(Hooks::default());
        let fixture = Fixture::new(hooks.clone(), takes::REQUIRED_SUBSCRIPTIONS, false).await;
        let io = fixture.io().await.unwrap();
        hooks.hold_stats.store(true, Ordering::Release);
        let blocking_handle = fixture.handle.clone();
        let blocker =
            blocking_handle.request(fixture.handle.token(), "GetStats", Value::Null, WAIT);
        tokio::pin!(blocker);
        assert!(poll!(blocker.as_mut()).is_pending());
        until(|| hooks.stats_seen.load(Ordering::Acquire)).await;
        let permission = Arc::new(AtomicBool::new(true));
        let check = permission.clone();
        let mut queued = io.dispatch(
            fixture.handle.token(),
            Effect::StopRecord,
            WAIT,
            Box::new(move || {
                if check.load(Ordering::Acquire) {
                    Ok(())
                } else {
                    Err(reason.into())
                }
            }),
        );
        assert!(poll!(queued.as_mut()).is_pending());
        permission.store(false, Ordering::Release);
        hooks.stats_gate.notify_one();
        blocker.await.unwrap();
        let failure = queued.await.unwrap_err();
        assert!(matches!(failure,DispatchError::BeforeDispatch(ref text) if text==reason));
        assert_eq!(fixture.count("StopRecord"), 0);
        fixture.close().await;
    }
}

#[tokio::test]
async fn retained_frontend_reads_and_stop_awaits_never_queue_a_second_request() {
    let hooks = Arc::new(Hooks::default());
    let fixture = Fixture::new(hooks.clone(), takes::REQUIRED_SUBSCRIPTIONS, false).await;
    let io = fixture.io().await.unwrap();
    hooks.hold_stats.store(true, Ordering::Release);
    let mut read = io.read(ReadRequest::Stats, WAIT);
    assert!(timeout(Duration::from_millis(20), &mut read).await.is_err());
    hooks.stats_gate.notify_one();
    read.await.unwrap();
    assert_eq!(fixture.count("GetStats"), 1);
    hooks.delay_stop.store(true, Ordering::Release);
    let mut stop = io.dispatch(
        fixture.handle.token(),
        Effect::StopRecord,
        WAIT,
        Box::new(|| Ok(())),
    );
    assert!(timeout(Duration::from_millis(10), &mut stop).await.is_err());
    stop.await.unwrap();
    assert_eq!(fixture.count("StopRecord"), 1);
    fixture.close().await;
}

#[tokio::test]
async fn lost_reply_remains_unknown_with_one_mutation() {
    let hooks = Arc::new(Hooks::default());
    hooks.lose_stop.store(true, Ordering::Release);
    let fixture = Fixture::new(hooks, takes::REQUIRED_SUBSCRIPTIONS, false).await;
    let io = fixture.io().await.unwrap();
    assert!(matches!(
        io.dispatch(
            fixture.handle.token(),
            Effect::StopRecord,
            WAIT,
            Box::new(|| Ok(()))
        )
        .await,
        Err(DispatchError::Unknown(_))
    ));
    assert_eq!(fixture.count("StopRecord"), 1);
    fixture.close().await;
}

#[tokio::test]
async fn explicit_rejection_keeps_source_reason_and_code_without_debug_payload() {
    let hooks = Arc::new(Hooks::default());
    hooks.reject_stop.store(true, Ordering::Release);
    let fixture = Fixture::new(hooks, takes::REQUIRED_SUBSCRIPTIONS, false).await;
    let io = fixture.io().await.unwrap();
    let error = io
        .dispatch(
            fixture.handle.token(),
            Effect::StopRecord,
            WAIT,
            Box::new(|| Ok(())),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&error,DispatchError::Rejected{code:604,message} if message=="OBS request StopRecord failed with code 604: concrete source rejection")
    );
    assert!(!format!("{error:?}").contains("concrete source rejection"));
    assert_eq!(fixture.count("StopRecord"), 1);
    fixture.close().await;
}

#[tokio::test]
async fn event_synchronization_cannot_extend_the_original_mutation_deadline() {
    let fixture = Fixture::new(
        Arc::new(Hooks::default()),
        takes::REQUIRED_SUBSCRIPTIONS,
        false,
    )
    .await;
    let io = fixture.io().await.unwrap();
    fixture.fence.sync_delay_ms.store(40, Ordering::Release);
    let result = io
        .dispatch(
            fixture.handle.token(),
            Effect::StartRecord,
            Duration::from_millis(10),
            Box::new(|| Ok(())),
        )
        .await;
    let count = fixture.count("StartRecord");
    fixture.close().await;
    assert!(
        matches!(result, Err(DispatchError::BeforeDispatch(_))),
        "event sync silently created another dispatch budget"
    );
    assert_eq!(count, 0);
}

#[tokio::test]
async fn setup_uses_real_serial_audio_batch_and_preserves_optional_read_failures() {
    let fixture = Fixture::new(
        Arc::new(Hooks::default()),
        takes::REQUIRED_SUBSCRIPTIONS,
        false,
    )
    .await;
    let io = fixture.io().await.unwrap();
    let setup = io.monitor_setup().await.unwrap();
    assert_eq!(setup.record_tracks, vec![2]);
    assert_eq!(
        setup.input_tracks.get("mic"),
        Some(json!({"1":false,"2":true}).as_object().unwrap())
    );
    assert!(!setup.input_tracks.contains_key("video"));
    assert!(setup.tracks_error.is_none());
    assert!(setup.supports_ppm);
    assert_eq!(setup.program, json!({"currentProgramSceneName":"Program"}));
    assert!(setup.stats.is_some());
    let batches = fixture
        .hooks
        .trace
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r["op"] == 8)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0]["d"]["executionType"], 0);
    assert_eq!(batches[0]["d"]["requests"].as_array().unwrap().len(), 2);
    fixture.close().await;
}

#[tokio::test]
async fn picture_fences_program_changes_but_accepts_meter_progress() {
    let hooks = Arc::new(Hooks::default());
    let fixture = Fixture::new(hooks.clone(), takes::REQUIRED_SUBSCRIPTIONS, false).await;
    let io = fixture.io().await.unwrap();
    *hooks.picture_event.lock().unwrap() = Some("CurrentProgramSceneChanged".into());
    assert!(
        io.picture("Program".into())
            .await
            .unwrap_err()
            .contains("source changed")
    );
    *hooks.picture_event.lock().unwrap() = Some("InputVolumeMeters".into());
    assert_eq!(io.picture("Program".into()).await.unwrap(), vec![54, 182]);
    let request = hooks
        .trace
        .lock()
        .unwrap()
        .iter()
        .find(|r| r["d"]["requestType"] == "GetSourceScreenshot")
        .cloned()
        .unwrap();
    assert_eq!(
        request["d"]["requestData"],
        json!({"sourceName":"Program","imageFormat":"ppm","imageWidth":96})
    );
    fixture.close().await;
}

fn source_ppm(bytes: &[u8]) -> Value {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let output=Command::new("node").args(["--input-type=module","-e","import {decodePpm} from './build/tools/frame-sample.js';try{const s=decodePpm(Buffer.from(process.env.PPM_FIXTURE,'base64'));console.log(JSON.stringify({width:s.width,height:s.height,luma:[...s.luma]}));}catch(e){console.log(JSON.stringify({error:e.message}));}"]).env("PPM_FIXTURE",STANDARD.encode(bytes)).current_dir(root).output().unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}
#[test]
fn bounded_ppm_luma_matches_actual_typescript_for_8_and_16_bit_samples() {
    let mut fixtures = vec![
        b"P6\n# comment\n2 1\n255\n\xff\x00\x00\x00\xff\x00".to_vec(),
        b"P6\t1\r1\n1\n\xff\xff\xff".to_vec(),
    ];
    let mut bytes = b"P6\n17 23\n65535\n".to_vec();
    let mut seed = 0x12345678u32;
    for _ in 0..17 * 23 * 3 {
        seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
        bytes.extend((seed as u16).to_be_bytes());
    }
    fixtures.push(bytes);
    // Valid16bit sample just below0.5: floor(x+0.5) incorrectly becomes1.
    fixtures.push(b"P6\n1 1\n17289\n\0\0\0\x05\x01\xa4".to_vec());
    for bytes in fixtures {
        let source = source_ppm(&bytes);
        let native = decode_ppm(&bytes, 4096).unwrap();
        assert_eq!(
            json!({"width":native.width,"height":native.height,"luma":native.luma}),
            source
        );
    }
}
#[test]
fn malformed_and_excessive_ppm_never_allocates_unbounded_pixels() {
    for bytes in [
        b"P5\n1 1\n255\n\0".as_slice(),
        b"P6\n# missing end",
        b"P6\n1 1\n255\n\0",
    ] {
        assert_eq!(
            json!({"error":decode_ppm(bytes,4096).unwrap_err()}),
            source_ppm(bytes)
        );
    }
    for bytes in [
        b"P6\n0 0\n255\n".as_slice(),
        b"P6\n1 1\n0\n\0\0\0",
        b"P6\n99999999999999999999999999999 1\n255\n",
        b"P6\n2048 2048\n255\n",
    ] {
        assert!(decode_ppm(bytes, 4096).is_err());
    }
}

struct Scratch(std::path::PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "take-host-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }
    fn path(&self, name: &str) -> std::path::PathBuf {
        self.0.join(name)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn tool(name: &str) -> std::path::PathBuf {
    let output = Command::new("which").arg(name).output().unwrap();
    assert!(
        output.status.success(),
        "concrete take host test requires installed {name}"
    );
    std::path::PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
        .canonicalize()
        .unwrap()
}
fn assert_json_same(actual: &Value, expected: &Value) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => assert_eq!(a.as_f64(), b.as_f64()),
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len());
            for (a, b) in a.iter().zip(b) {
                assert_json_same(a, b);
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(a.keys().collect::<Vec<_>>(), b.keys().collect::<Vec<_>>());
            for (key, a) in a {
                assert_json_same(a, &b[key]);
            }
        }
        _ => assert_eq!(actual, expected),
    }
}
#[tokio::test]
async fn concrete_host_inspects_every_audio_track_against_actual_typescript() {
    let scratch = Scratch::new();
    let file = scratch.path("clip; literal.mkv");
    let ffmpeg = tool("ffmpeg");
    let ffprobe = tool("ffprobe");
    let output = Command::new(&ffmpeg)
        .args([
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=red:s=32x32:r=5:d=1",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=48000:cl=mono",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:sample_rate=48000:duration=1",
            "-map",
            "0:v",
            "-map",
            "1:a",
            "-map",
            "2:a",
            "-t",
            "1",
            "-c:v",
            "ffv1",
            "-c:a",
            "pcm_s16le",
        ])
        .arg(&file)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture encoding failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let host = LocalHost::new(&scratch.0, Some(&ffmpeg), Some(&ffprobe)).unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    for loudness in [false, true] {
        let actual = host
            .inspect(file.to_string_lossy().into_owned(), loudness)
            .await;
        let output=Command::new("node").args(["--input-type=module","-e","import{inspectRecording}from'./build/tools/takes.js';console.log(JSON.stringify(await inspectRecording(process.env.TAKE_FIXTURE,process.env.TAKE_LOUD==='true')));"]).env("TAKE_FIXTURE",&file).env("TAKE_LOUD",loudness.to_string()).current_dir(root).output().unwrap();
        assert!(output.status.success());
        assert_json_same(
            &serde_json::to_value(&actual).unwrap(),
            &serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        );
        assert_eq!(actual.audio_streams, Some(2));
        assert_eq!(actual.video_streams, Some(1));
        if loudness {
            assert!(
                actual.max_volume_db.unwrap() > -30.0,
                "silent first track concealed audible second track"
            );
        }
    }
}

#[tokio::test]
async fn concrete_host_refuses_outside_symlink_hardlink_and_changed_root_authority() {
    use preflight::WritableDirectoryProbe;
    let scratch = Scratch::new();
    let outside = Scratch::new();
    let original = scratch.path("recording.mkv");
    std::fs::write(&original, b"fixture").unwrap();
    let host = LocalHost::new(&scratch.0, None, None).unwrap();
    host.check(scratch.0.to_str().unwrap()).unwrap();
    assert!(std::fs::read_dir(&scratch.0).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));
    assert!(host.check(outside.0.to_str().unwrap()).is_err());
    assert!(
        host.inspect(
            outside.path("unknown.mkv").to_string_lossy().into_owned(),
            true
        )
        .await
        .skipped
        .unwrap()
        .contains("approved")
    );
    #[cfg(unix)]
    {
        let alias = scratch.path("alias.mkv");
        std::os::unix::fs::symlink(&original, &alias).unwrap();
        assert!(
            host.inspect(alias.to_string_lossy().into_owned(), false)
                .await
                .skipped
                .unwrap()
                .contains("independent")
        );
        std::fs::remove_file(&alias).unwrap();
        std::fs::hard_link(&original, &alias).unwrap();
        assert!(
            host.inspect(original.to_string_lossy().into_owned(), false)
                .await
                .skipped
                .unwrap()
                .contains("independent")
        );
        std::fs::remove_file(alias).unwrap();
        let nested = scratch.path("outside");
        std::os::unix::fs::symlink(&outside.0, &nested).unwrap();
        assert!(host.check(nested.to_str().unwrap()).is_err());
    }
    assert_eq!(
        host.inspect(original.to_string_lossy().into_owned(), false)
            .await
            .skipped,
        Some("ffprobe is not installed".into())
    );
    // Replace the approved root while preserving its old inode in a sibling.
    let sibling = scratch
        .0
        .with_file_name(format!("old-take-host-{}", std::process::id()));
    assert!(!sibling.exists());
    std::fs::rename(&scratch.0, &sibling).unwrap();
    std::fs::create_dir(&scratch.0).unwrap();
    let refused = host.check(scratch.0.to_str().unwrap()).is_err();
    std::fs::remove_dir(&scratch.0).unwrap();
    std::fs::rename(&sibling, &scratch.0).unwrap();
    assert!(refused);
}

#[tokio::test]
async fn complete_take_log_matches_source_payload_and_preserves_existing_human_sidecars() {
    let scratch = Scratch::new();
    let recording = scratch.path("demo.mkv");
    std::fs::write(&recording, b"owned recording fixture").unwrap();
    let host = LocalHost::new(&scratch.0, None, None).unwrap();
    let monitor = obs_mcp_native::take_monitor::TakeMonitorCore::new(
        0,
        "1970-01-01T00:00:00.000Z".into(),
        Default::default(),
    );
    let summary = monitor.summary(1000);
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let output=Command::new("node").args(["--input-type=module","-e","import fs from'node:fs';import{writeTakeLog}from'./build/tools/take-monitor.js';const p=await writeTakeLog(process.env.TAKE_FIXTURE,JSON.parse(process.env.TAKE_SUMMARY));console.log(fs.readFileSync(p,'utf8'));fs.unlinkSync(p);"]).env("TAKE_FIXTURE",&recording).env("TAKE_SUMMARY",serde_json::to_string(&summary).unwrap()).current_dir(root).output().unwrap();
    assert!(output.status.success());
    let path = host
        .write_log(recording.to_string_lossy().into_owned(), summary.clone())
        .await
        .unwrap();
    let actual = std::fs::read(&path).unwrap();
    assert_json_same(
        &serde_json::from_slice::<Value>(&actual).unwrap(),
        &serde_json::from_slice::<Value>(&output.stdout).unwrap(),
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    std::fs::write(&path, b"human sidecar").unwrap();
    assert!(
        host.write_log(recording.to_string_lossy().into_owned(), summary.clone())
            .await
            .is_none()
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"human sidecar");
    #[cfg(unix)]
    {
        std::fs::remove_file(&path).unwrap();
        let target = scratch.path("human.txt");
        std::fs::write(&target, b"preserve").unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(
            host.write_log(recording.to_string_lossy().into_owned(), summary)
                .await
                .is_none()
        );
        assert_eq!(std::fs::read(target).unwrap(), b"preserve");
    }
}

#[tokio::test]
async fn file_and_metadata_diagnostics_cannot_spoof_silent_recording_verification() {
    let scratch = Scratch::new();
    let ffmpeg = tool("ffmpeg");
    let ffprobe = tool("ffprobe");
    let host = LocalHost::new(&scratch.0, Some(&ffmpeg), Some(&ffprobe)).unwrap();
    for scenario in ["filename", "metadata"] {
        let name = if scenario == "filename" {
            "claimed max_volume: -inf dB.wav"
        } else {
            "metadata.wav"
        };
        let file = scratch.path(name);
        let mut command = Command::new(&ffmpeg);
        command.args([
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:sample_rate=48000:duration=0.3",
            "-c:a",
            "pcm_s16le",
        ]);
        if scenario == "metadata" {
            command.args(["-metadata", "title=max_volume: -inf dB"]);
        }
        let output = command.arg(&file).output().unwrap();
        assert!(output.status.success());
        let native = host
            .inspect(file.to_string_lossy().into_owned(), true)
            .await;
        let output=Command::new("node").args(["--input-type=module","-e","import{inspectRecording}from'./build/tools/takes.js';console.log(JSON.stringify(await inspectRecording(process.env.TAKE_FIXTURE,true)));"]).env("TAKE_FIXTURE",&file).current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()).output().unwrap();
        assert!(output.status.success());
        let source: Value = serde_json::from_slice(&output.stdout).unwrap();
        eprintln!(
            "diagnostic counterexample {scenario}: source={source}; native={:?}",
            native.max_volume_db
        );
        assert!(
            source["maxVolumeDb"]
                .as_f64()
                .is_some_and(|level| level > -30.0),
            "TypeScript accepted untrusted diagnostic text as silence"
        );
        assert!(
            native.max_volume_db.is_some_and(|level| level > -30.0),
            "Native inspector accepted untrusted diagnostic text as silence"
        );
    }
}

#[tokio::test]
async fn local_recording_inspection_refuses_network_playlists_without_connecting() {
    let scratch = Scratch::new();
    let file = scratch.path("disguised-recording.mkv");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    std::fs::write(
        &file,
        format!(
            "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\nhttp://{}/segment.ts\n#EXT-X-ENDLIST\n",
            listener.local_addr().unwrap()
        ),
    )
    .unwrap();
    let ffmpeg = tool("ffmpeg");
    let ffprobe = tool("ffprobe");
    let host = LocalHost::new(&scratch.0, Some(&ffmpeg), Some(&ffprobe)).unwrap();
    assert!(
        host.inspect(file.to_string_lossy().into_owned(), true)
            .await
            .skipped
            .is_some()
    );
    let output=Command::new("node").args(["--input-type=module","-e","import{inspectRecording}from'./build/tools/takes.js';console.log(JSON.stringify(await inspectRecording(process.env.TAKE_FIXTURE,true)));"]).env("TAKE_FIXTURE",&file).current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()).output().unwrap();
    assert!(output.status.success());
    assert!(
        serde_json::from_slice::<Value>(&output.stdout)
            .unwrap()
            .get("skipped")
            .is_some()
    );
    assert!(
        timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "local media inspection opened a network URL"
    );
}
