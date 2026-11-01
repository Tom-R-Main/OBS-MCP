// SPDX-License-Identifier: GPL-2.0-only
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    control::{CallerContext, ControlDomain},
    custody::{Clock, ClockSnapshot},
    demo::{
        Delivery, Demo, DemoAdapter, DemoAdmission, DemoContext, DemoMutation, DispatchFence,
        JournalFile, JournalStore, PrepareMetadata, Selection,
    },
    demo_transport::{
        BoundDemoAdapter, DemoAction, DemoBounds, DemoPolicy, DemoTransportContext,
        REQUIRED_DEMO_EVENTS,
    },
    observation::{ReadAdapter, ReadRequest},
    session::{ObsSession, SessionHandle, SessionOptions},
    session_observation::ObservationBridge,
    snapshots::{CaptureMetadata, ReadCall, SnapshotFile, SnapshotReader},
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::Notify,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(3);
const ID: &str = "12345678-1234-4234-8234-123456789abc";
type Peer = WebSocketStream<tokio::net::TcpStream>;
async fn send(peer: &mut Peer, value: Value) {
    peer.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn event(peer: &mut Peer) {
    send(peer,json!({"op":5,"d":{"eventType":"SceneItemTransformChanged","eventIntent":1<<19,"eventData":{"sceneName":"Demo","sceneItemId":3}}})).await;
}
fn initial() -> Value {
    json!({"collection":"Private Demo","program":"Demo","input":{"inputName":"Fixture","inputUuid":"input-1","inputKind":"screen_capture","settings":{"type":1,"window":123,"secret":"SYNTHETIC PRIVATE SETTING"},"muted":false,"volume":0.7,"tracks":{"1":true,"2":false,"3":false,"4":false,"5":false,"6":false}},"scenes":[{"sceneName":"Demo","sceneUuid":"scene-1"}],"items":{"Demo":[{"sceneItemId":3,"sourceName":"Fixture","sourceUuid":"input-1","isGroup":false,"sourceType":"OBS_SOURCE_TYPE_INPUT","inputKind":"screen_capture","sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemIndex":0,"sceneItemTransform":{"boundsType":"OBS_BOUNDS_NONE","boundsWidth":600,"boundsHeight":400,"boundsAlignment":5,"alignment":0,"positionX":25,"positionY":35,"rotation":10,"sourceWidth":1512,"sourceHeight":982,"scaleX":0.75,"scaleY":0.75}}]},"video":{"baseWidth":1920,"baseHeight":1080}})
}
fn available() -> Vec<String> {
    [
        "GetVersion",
        "GetStats",
        "GetSceneCollectionList",
        "GetSceneList",
        "GetInputList",
        "GetSceneItemList",
        "GetInputSettings",
        "GetInputMute",
        "GetInputVolume",
        "GetInputAudioTracks",
        "GetVideoSettings",
        "GetStreamStatus",
        "GetRecordStatus",
        "GetVirtualCamStatus",
        "GetReplayBufferStatus",
        "GetOutputList",
        "GetOutputStatus",
        "SetInputMute",
        "SetInputAudioTracks",
        "SetSceneItemTransform",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}
struct Hooks {
    state: Mutex<Value>,
    trace: Mutex<Vec<Value>>,
    available: Mutex<Vec<String>>,
    platform: Mutex<String>,
    writes: AtomicUsize,
    reject_write: AtomicUsize,
    lose_write: AtomicUsize,
    reject_tracks: AtomicBool,
    hold_stats: AtomicBool,
    stats_seen: AtomicBool,
    stats_gate: Notify,
    event_release: AtomicBool,
    event_version: AtomicBool,
    late_transform: AtomicBool,
    pending_transform: AtomicBool,
    human_after_late: AtomicUsize,
    human_on_resample: AtomicBool,
    postwrite_collections: AtomicUsize,
}
impl Default for Hooks {
    fn default() -> Self {
        Self {
            state: Mutex::new(initial()),
            trace: Mutex::new(vec![]),
            available: Mutex::new(available()),
            platform: Mutex::new("macos".into()),
            writes: AtomicUsize::new(0),
            reject_write: AtomicUsize::new(0),
            lose_write: AtomicUsize::new(0),
            reject_tracks: AtomicBool::new(false),
            hold_stats: AtomicBool::new(false),
            stats_seen: AtomicBool::new(false),
            stats_gate: Notify::new(),
            event_release: AtomicBool::new(false),
            event_version: AtomicBool::new(false),
            late_transform: AtomicBool::new(false),
            pending_transform: AtomicBool::new(false),
            human_after_late: AtomicUsize::new(0),
            human_on_resample: AtomicBool::new(false),
            postwrite_collections: AtomicUsize::new(usize::MAX),
        }
    }
}
fn answer(hooks: &Hooks, name: &str, data: &Value) -> Value {
    let mut state = hooks.state.lock().unwrap();
    match name {
        "GetVersion" => {
            json!({"availableRequests":hooks.available.lock().unwrap().clone(),"platform":hooks.platform.lock().unwrap().clone()})
        }
        "GetStats" => json!({}),
        "GetSceneCollectionList" => json!({"currentSceneCollectionName":state["collection"]}),
        "GetSceneList" => {
            json!({"currentProgramSceneName":state["program"],"scenes":state["scenes"]})
        }
        "GetInputList" => {
            json!({"inputs":[{"inputName":state["input"]["inputName"],"inputUuid":state["input"]["inputUuid"],"inputKind":state["input"]["inputKind"]}]})
        }
        "GetSceneItemList" => {
            json!({"sceneItems":state["items"][data["sceneName"].as_str().unwrap()]})
        }
        "GetInputSettings" => {
            json!({"inputKind":state["input"]["inputKind"],"inputSettings":state["input"]["settings"]})
        }
        "GetInputMute" => json!({"inputMuted":state["input"]["muted"]}),
        "GetInputVolume" => json!({"inputVolumeMul":state["input"]["volume"]}),
        "GetInputAudioTracks" => json!({"inputAudioTracks":state["input"]["tracks"]}),
        "GetVideoSettings" => state["video"].clone(),
        "GetOutputList" => json!({"outputs":[]}),
        "GetStreamStatus"
        | "GetRecordStatus"
        | "GetVirtualCamStatus"
        | "GetReplayBufferStatus"
        | "GetOutputStatus" => json!({"outputActive":false,"outputReconnecting":false}),
        "SetInputMute" => {
            state["input"]["muted"] = data["inputMuted"].clone();
            json!({})
        }
        "SetInputAudioTracks" => {
            state["input"]["tracks"] = data["inputAudioTracks"].clone();
            json!({})
        }
        "SetSceneItemTransform" => {
            for (key, value) in data["sceneItemTransform"].as_object().unwrap() {
                state["items"]["Demo"][0]["sceneItemTransform"][key] = value.clone();
            }
            json!({})
        }
        _ => panic!("unexpected physical request: {name}"),
    }
}
fn apply_human_edit(hooks: &Hooks) {
    let mut state = hooks.state.lock().unwrap();
    match hooks.human_after_late.load(Ordering::Acquire) {
        1 => state["items"]["Demo"][0]["sceneItemTransform"]["positionX"] = json!(23),
        2 => state["items"]["Demo"][0]["sceneItemTransform"]["rotation"] = json!(23),
        3 => state["input"]["settings"]["capture_window"] = json!("456\r\nHuman"),
        _ => {}
    }
}
struct Fixture {
    url: String,
    owner: ObsSession,
    handle: SessionHandle,
    bridge: ObservationBridge,
    hooks: Arc<Hooks>,
    control: Arc<Mutex<ControlDomain>>,
    cancel: Cancellation,
    peer: JoinHandle<()>,
}
impl Fixture {
    async fn new(hooks: Arc<Hooks>, mask: u64, narrow: bool) -> Self {
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
            assert_eq!(identify["d"]["eventSubscriptions"], mask);
            send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
            while let Some(message) = peer.next().await {
                let Ok(message) = message else { break };
                let Message::Text(text) = message else {
                    let _ = peer.flush().await;
                    break;
                };
                let request: Value = serde_json::from_str(&text).unwrap();
                state.trace.lock().unwrap().push(request.clone());
                if request["op"] == 8 {
                    assert_eq!(request["d"]["executionType"], 0);
                    assert_eq!(request["d"]["haltOnFailure"], false);
                    let rows=request["d"]["requests"].as_array().unwrap().iter().map(|row|{
                        let name=row["requestType"].as_str().unwrap();let reject=name=="GetInputAudioTracks"&&state.reject_tracks.load(Ordering::Acquire);
                        json!({"requestId":row["requestId"],"requestType":name,"requestStatus":{"result":!reject,"code":if reject{604}else{100},"comment":if reject{"private optional track"}else{""}},"responseData":if reject{json!({})}else{answer(&state,name,&row["requestData"])}})
                    }).collect::<Vec<_>>();
                    send(
                        &mut peer,
                        json!({"op":9,"d":{"requestId":request["d"]["requestId"],"results":rows}}),
                    )
                    .await;
                    continue;
                }
                let name = request["d"]["requestType"].as_str().unwrap();
                if name == "GetSceneCollectionList"
                    && state.pending_transform.swap(false, Ordering::AcqRel)
                {
                    state.postwrite_collections.store(0, Ordering::Release);
                    if !state.human_on_resample.load(Ordering::Acquire) {
                        apply_human_edit(&state);
                    }
                    event(&mut peer).await;
                }
                if name == "GetSceneCollectionList"
                    && state.postwrite_collections.load(Ordering::Acquire) != usize::MAX
                {
                    let phase = state.postwrite_collections.fetch_add(1, Ordering::AcqRel);
                    if phase == 2 && state.human_on_resample.load(Ordering::Acquire) {
                        apply_human_edit(&state);
                        event(&mut peer).await;
                    }
                }
                if name == "GetStats" && state.hold_stats.swap(false, Ordering::AcqRel) {
                    state.stats_seen.store(true, Ordering::Release);
                    state.stats_gate.notified().await;
                    if state.event_release.load(Ordering::Acquire) {
                        event(&mut peer).await;
                    }
                }
                if name == "GetVersion" && state.event_version.load(Ordering::Acquire) {
                    event(&mut peer).await;
                }
                let write = name.starts_with("Set");
                let ordinal = if write {
                    state.writes.fetch_add(1, Ordering::AcqRel) + 1
                } else {
                    0
                };
                if write && ordinal == state.lose_write.load(Ordering::Acquire) {
                    let _ = answer(&state, name, &request["d"]["requestData"]);
                    peer.close(None).await.unwrap();
                    break;
                }
                let reject = write && ordinal == state.reject_write.load(Ordering::Acquire);
                let data = if reject {
                    json!({})
                } else {
                    answer(&state, name, &request["d"]["requestData"])
                };
                if write && !reject {
                    if name == "SetSceneItemTransform"
                        && state.late_transform.load(Ordering::Acquire)
                    {
                        state.pending_transform.store(true, Ordering::Release);
                    } else {
                        event(&mut peer).await;
                    }
                }
                send(&mut peer,json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":name,"requestStatus":{"result":!reject,"code":if reject{500}else{100},"comment":if reject{"PRIVATE PEER COMMENT"}else{""}},"responseData":data}})).await;
            }
        });
        let (owner, events) = ObsSession::connect(
            &url,
            None,
            SessionOptions {
                request_items: 4,
                event_subscriptions: mask,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let handle = owner.handle();
        let bridge = if narrow {
            ObservationBridge::bind_with_event_filter(handle.clone(), events, None, |_| false)
        } else {
            ObservationBridge::bind(handle.clone(), events, None)
        }
        .unwrap();
        Self {
            url,
            owner,
            handle,
            bridge,
            hooks,
            control: Arc::new(Mutex::new(ControlDomain::default())),
            cancel: Cancellation::new(),
            peer,
        }
    }
    fn context(&self) -> DemoTransportContext {
        DemoTransportContext {
            handle: self.handle.clone(),
            reader: self.bridge.reader(),
            fence: self.bridge.fence(),
            control: self.control.clone(),
            clock: Arc::new(|| ClockSnapshot {
                epoch_ms: 100,
                utc: "2026-10-31T12:00:00.000Z".into(),
            }) as Clock,
            cancellation: self.cancel.clone(),
        }
    }
    async fn adapter(
        &self,
        action: DemoAction,
    ) -> Result<BoundDemoAdapter, obs_mcp_native::demo_transport::BindingError> {
        BoundDemoAdapter::bind(
            self.context(),
            &self.url,
            DemoPolicy::resolve(
                action,
                CallerContext {
                    legacy_name: Some("agent"),
                    ..Default::default()
                },
            ),
            Instant::now() + WAIT,
            DemoBounds::default(),
        )
        .await
    }
    fn count(&self, name: &str) -> usize {
        self.hooks
            .trace
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row["d"]["requestType"] == name)
            .count()
    }
    async fn close(self) {
        let _ = self.owner.shutdown().await;
        self.bridge.shutdown().await.unwrap();
        timeout(WAIT, self.peer).await.unwrap().unwrap();
    }
}
fn fence(adapter: &BoundDemoAdapter) -> DispatchFence {
    let stamp = adapter.evidence();
    DispatchFence {
        generation: stamp.generation,
        revision: stamp.revision,
    }
}
fn mute() -> DemoMutation {
    DemoMutation::Mute {
        input_name: "Fixture".into(),
        muted: true,
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + WAIT;
    while !condition() {
        assert!(Instant::now() < end);
        sleep(Duration::from_millis(1)).await;
    }
}
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "demo-transport-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn selection() -> Selection {
    Selection {
        scene_collection: "Private Demo".into(),
        scene_name: "Demo".into(),
        input_name: "Fixture".into(),
    }
}
fn context(adapter: &BoundDemoAdapter, url: &str) -> DemoContext {
    DemoContext {
        endpoint: url.into(),
        host: adapter.host(),
        generation: adapter.evidence().generation,
        deadline: Instant::now() + WAIT,
        admission: DemoAdmission::default(),
    }
}
fn metadata(url: &str) -> PrepareMetadata {
    PrepareMetadata {
        operation_id: ID.into(),
        snapshot: CaptureMetadata {
            id: "12345678".into(),
            obs_url: url.into(),
            taken_at: "2026-10-31T12:00:00.000Z".into(),
        },
    }
}

#[tokio::test]
async fn real_capabilities_closed_reads_and_serial_batches_preserve_optional_rejections() {
    let hooks = Arc::new(Hooks::default());
    hooks.event_version.store(true, Ordering::Release);
    let fixture = Fixture::new(hooks, REQUIRED_DEMO_EVENTS, false).await;
    let mut adapter = fixture.adapter(DemoAction::Prepare).await.unwrap();
    assert_eq!(fixture.count("GetVersion"), 1);
    assert_eq!(adapter.platform(), "macos");
    assert_eq!(adapter.evidence().revision, 1);
    fixture.hooks.reject_tracks.store(true, Ordering::Release);
    let rows = adapter
        .batch(vec![
            ReadCall {
                request: ReadRequest::InputMute,
                data: json!({"inputName":"Fixture"}),
            },
            ReadCall {
                request: ReadRequest::InputAudioTracks,
                data: json!({"inputName":"Fixture"}),
            },
        ])
        .await
        .unwrap();
    assert!(rows[0].ok);
    assert!(!rows[1].ok);
    assert_eq!(rows[1].code, Some(604));
    assert_eq!(rows[1].comment.as_deref(), Some("private optional track"));
    assert!(!format!("{:?}", rows[1]).contains("private optional"));
    assert!(
        fixture
            .hooks
            .trace
            .lock()
            .unwrap()
            .iter()
            .all(|row| !row.to_string().contains("GetInputProperties"))
    );
    fixture.close().await;
}

#[tokio::test]
async fn refuses_unqualified_profiles_endpoints_metadata_and_physical_identity() {
    for (mask, narrow) in [
        (obs_mcp_native::session::ALL_EVENTS, false),
        (REQUIRED_DEMO_EVENTS, true),
    ] {
        let fixture = Fixture::new(Arc::new(Hooks::default()), mask, narrow).await;
        assert!(fixture.adapter(DemoAction::Prepare).await.is_err());
        assert_eq!(fixture.count("GetVersion"), 0);
        fixture.close().await;
    }
    let first = Fixture::new(Arc::new(Hooks::default()), REQUIRED_DEMO_EVENTS, false).await;
    let second = Fixture::new(Arc::new(Hooks::default()), REQUIRED_DEMO_EVENTS, false).await;
    let mut mixed = first.context();
    mixed.reader = second.bridge.reader();
    assert!(
        BoundDemoAdapter::bind(
            mixed,
            &first.url,
            DemoPolicy::resolve(DemoAction::Prepare, CallerContext::default()),
            Instant::now() + WAIT,
            DemoBounds::default()
        )
        .await
        .is_err()
    );
    assert!(
        BoundDemoAdapter::bind(
            first.context(),
            &second.url,
            DemoPolicy::resolve(DemoAction::Prepare, CallerContext::default()),
            Instant::now() + WAIT,
            DemoBounds::default()
        )
        .await
        .is_err()
    );
    *first.hooks.platform.lock().unwrap() = "windows".into();
    assert!(first.adapter(DemoAction::Prepare).await.is_err());
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn actual_send_rechecks_lease_revision_deadline_and_cancellation() {
    for reason in ["lease", "revision", "deadline", "cancel"] {
        let fixture = Fixture::new(Arc::new(Hooks::default()), REQUIRED_DEMO_EVENTS, false).await;
        let mut adapter = fixture.adapter(DemoAction::Prepare).await.unwrap();
        let expected = fence(&adapter);
        fixture.hooks.hold_stats.store(true, Ordering::Release);
        let handle = fixture.handle.clone();
        let blocker = handle.request(handle.token(), "GetStats", Value::Null, WAIT);
        tokio::pin!(blocker);
        assert!(poll!(blocker.as_mut()).is_pending());
        until(|| fixture.hooks.stats_seen.load(Ordering::Acquire)).await;
        let end = if reason == "deadline" {
            Instant::now() + Duration::from_millis(20)
        } else {
            Instant::now() + WAIT
        };
        let mutation = adapter.mutate(mute(), expected, end);
        tokio::pin!(mutation);
        assert!(poll!(mutation.as_mut()).is_pending());
        match reason {
            "lease" => {
                fixture
                    .control
                    .lock()
                    .unwrap()
                    .claim("human", 1, "human edit", 100)
                    .unwrap();
            }
            "revision" => fixture.hooks.event_release.store(true, Ordering::Release),
            "deadline" => sleep(Duration::from_millis(30)).await,
            "cancel" => fixture.cancel.cancel(),
            _ => unreachable!(),
        }
        fixture.hooks.stats_gate.notify_one();
        blocker.await.unwrap();
        let result = mutation.await;
        if reason == "lease" {
            assert!(
                result
                    .as_ref()
                    .unwrap_err()
                    .message
                    .contains("human has control")
            );
        }
        assert!(matches!(result,Err(ref error)if error.delivery==Delivery::NotDispatched));
        assert_eq!(fixture.hooks.writes.load(Ordering::Acquire), 0);
        fixture.close().await;
    }
}

#[tokio::test]
async fn explicit_peer_rejection_preserves_tool_comment_but_debug_stays_metadata() {
    let hooks = Arc::new(Hooks::default());
    hooks.reject_write.store(1, Ordering::Release);
    let fixture = Fixture::new(hooks, REQUIRED_DEMO_EVENTS, false).await;
    let mut adapter = fixture.adapter(DemoAction::Prepare).await.unwrap();
    let expected = fence(&adapter);
    let error = adapter
        .mutate(mute(), expected, Instant::now() + WAIT)
        .await
        .unwrap_err();
    assert_eq!(error.delivery, Delivery::Rejected);
    assert_eq!(
        error.message,
        "OBS request SetInputMute failed with code 500: PRIVATE PEER COMMENT"
    );
    assert!(!format!("{error:?}").contains("PRIVATE PEER"));
    fixture.close().await;
}

#[tokio::test]
async fn filesystem_prepare_restart_recover_and_human_conflict_use_one_physical_session() {
    for conflict in [false, true] {
        let scratch = Scratch::new();
        let fixture = Fixture::new(Arc::new(Hooks::default()), REQUIRED_DEMO_EVENTS, false).await;
        let before = fixture.hooks.state.lock().unwrap().clone();
        let mut adapter = fixture.adapter(DemoAction::Prepare).await.unwrap();
        let mut snapshots = SnapshotFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
        let mut journals = JournalFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
        let ctx = context(&adapter, &fixture.url);
        let prepared = Demo::new(&mut adapter, &mut snapshots, &mut journals, ctx)
            .prepare(selection(), true, metadata(&fixture.url))
            .await;
        assert_eq!(
            prepared["structuredContent"]["status"], "prepared",
            "{prepared}"
        );
        assert_eq!(journals.load(ID).unwrap().version(), 2);
        drop(journals);
        drop(snapshots);
        if conflict {
            fixture.hooks.state.lock().unwrap()["items"]["Demo"][0]["sceneItemTransform"]["rotation"] =
                json!(123);
        }
        let writes = fixture.hooks.writes.load(Ordering::Acquire);
        let mut adapter = fixture.adapter(DemoAction::Recover).await.unwrap();
        let mut snapshots = SnapshotFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
        let mut journals = JournalFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
        let ctx = context(&adapter, &fixture.url);
        let recovered = Demo::new(&mut adapter, &mut snapshots, &mut journals, ctx)
            .recover(ID, true)
            .await;
        if conflict {
            assert_eq!(
                recovered["structuredContent"]["status"], "conflict",
                "{recovered}"
            );
            assert_eq!(fixture.hooks.writes.load(Ordering::Acquire), writes);
        } else {
            assert_eq!(
                recovered["structuredContent"]["status"], "recovered",
                "{recovered}"
            );
            assert_eq!(*fixture.hooks.state.lock().unwrap(), before);
        }
        assert!(
            fixture
                .hooks
                .trace
                .lock()
                .unwrap()
                .iter()
                .all(|row| !row.to_string().contains("GetInputProperties"))
        );
        fixture.close().await;
    }
}

#[tokio::test]
async fn lost_reply_survives_restart_as_v2_uncertainty_without_replay() {
    let scratch = Scratch::new();
    let hooks = Arc::new(Hooks::default());
    hooks.lose_write.store(1, Ordering::Release);
    let fixture = Fixture::new(hooks.clone(), REQUIRED_DEMO_EVENTS, false).await;
    let mut adapter = fixture.adapter(DemoAction::Prepare).await.unwrap();
    let mut snapshots = SnapshotFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
    let mut journals = JournalFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
    let ctx = context(&adapter, &fixture.url);
    let prepared = Demo::new(&mut adapter, &mut snapshots, &mut journals, ctx)
        .prepare(selection(), true, metadata(&fixture.url))
        .await;
    assert_eq!(
        prepared["structuredContent"]["status"], "partial",
        "{prepared}"
    );
    assert_eq!(hooks.writes.load(Ordering::Acquire), 1);
    let journal = journals.load(ID).unwrap();
    assert_eq!(journal.version(), 2);
    drop(journals);
    fixture.close().await;
    // A restarted transport cannot reuse the old URL identity on a different
    // test listener. Use the same persisted endpoint with the existing Demo
    // owner and dead transport: uncertainty must refuse even before any read.
    let mut journals = JournalFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
    let endpoint = journal.to_value()["obsUrl"].as_str().unwrap().to_owned();
    let ctx = context(&adapter, &endpoint);
    let recovered = Demo::new(&mut adapter, &mut snapshots, &mut journals, ctx)
        .recover(ID, true)
        .await;
    assert_eq!(
        recovered["structuredContent"]["status"], "unknown",
        "{recovered}"
    );
    assert_eq!(hooks.writes.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn frontend_timeout_retains_a_read_and_abandonment_never_replays() {
    for abandon in [false, true] {
        let fixture = Fixture::new(Arc::new(Hooks::default()), REQUIRED_DEMO_EVENTS, false).await;
        let mut adapter = fixture.adapter(DemoAction::Prepare).await.unwrap();
        fixture.hooks.hold_stats.store(true, Ordering::Release);
        let mut read = adapter.read(ReadRequest::Stats, Value::Null, Instant::now() + WAIT);
        assert!(poll!(read.as_mut()).is_pending());
        until(|| fixture.hooks.stats_seen.load(Ordering::Acquire)).await;
        assert!(
            timeout(Duration::from_millis(10), read.as_mut())
                .await
                .is_err()
        );
        assert_eq!(fixture.count("GetStats"), 1);
        if abandon {
            drop(read);
            fixture.hooks.stats_gate.notify_one();
        } else {
            fixture.hooks.stats_gate.notify_one();
            read.await.unwrap();
        }
        fixture.close().await;
    }
}

#[tokio::test]
async fn advertised_capability_and_batch_limits_refuse_before_any_frame() {
    let hooks = Arc::new(Hooks::default());
    hooks
        .available
        .lock()
        .unwrap()
        .retain(|name| name != "GetInputMute" && name != "SetInputMute");
    let fixture = Fixture::new(hooks, REQUIRED_DEMO_EVENTS, false).await;
    let mut adapter = BoundDemoAdapter::bind(
        fixture.context(),
        &fixture.url,
        DemoPolicy::resolve(DemoAction::Prepare, CallerContext::default()),
        Instant::now() + WAIT,
        DemoBounds {
            max_batch: 1,
            max_evidence_bytes: 4096,
        },
    )
    .await
    .unwrap();
    assert_eq!(adapter.supports(ReadRequest::InputMute), Some(false));
    assert!(
        adapter
            .read(
                ReadRequest::InputMute,
                json!({"inputName":"Fixture"}),
                Instant::now() + WAIT
            )
            .await
            .is_err()
    );
    let expected = fence(&adapter);
    assert_eq!(
        adapter
            .mutate(mute(), expected, Instant::now() + WAIT)
            .await
            .unwrap_err()
            .delivery,
        Delivery::NotDispatched
    );
    for mutation in [
        DemoMutation::Tracks {
            input_name: "Fixture".into(),
            tracks: Default::default(),
        },
        DemoMutation::Fit {
            scene_name: "Demo".into(),
            item_id: 3.0,
            key: obs_mcp_native::demo::FitKey::Rotation,
            value: Value::Null,
        },
    ] {
        assert_eq!(
            adapter
                .mutate(mutation, expected, Instant::now() + WAIT)
                .await
                .unwrap_err()
                .delivery,
            Delivery::NotDispatched
        );
    }
    let call = ReadCall {
        request: ReadRequest::Stats,
        data: Value::Null,
    };
    assert!(adapter.batch(vec![call.clone(), call]).await.is_err());
    assert_eq!(fixture.count("GetInputMute"), 0);
    assert_eq!(fixture.hooks.writes.load(Ordering::Acquire), 0);
    assert_eq!(fixture.hooks.trace.lock().unwrap().len(), 1);
    fixture.close().await;
}

#[tokio::test]
async fn linux_xcomposite_and_authorized_pipewire_prepare_without_property_enumeration() {
    for (kind, settings) in [
        (
            "xcomposite_input",
            json!({"capture_window":"123\r\nFixture\r\nChrome"}),
        ),
        (
            "pipewire-window-capture-source",
            json!({"RestoreToken":"user-authorized-fixture-token"}),
        ),
    ] {
        let scratch = Scratch::new();
        let hooks = Arc::new(Hooks::default());
        *hooks.platform.lock().unwrap() = "linux".into();
        {
            let mut state = hooks.state.lock().unwrap();
            state["input"]["inputKind"] = json!(kind);
            state["input"]["settings"] = settings;
            state["items"]["Demo"][0]["inputKind"] = json!(kind);
        }
        let fixture = Fixture::new(hooks, REQUIRED_DEMO_EVENTS, false).await;
        let mut adapter = fixture.adapter(DemoAction::Prepare).await.unwrap();
        assert_eq!(adapter.platform(), "linux");
        let mut snapshots = SnapshotFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
        let mut journals = JournalFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
        let ctx = context(&adapter, &fixture.url);
        let result = Demo::new(&mut adapter, &mut snapshots, &mut journals, ctx)
            .prepare(selection(), true, metadata(&fixture.url))
            .await;
        assert_eq!(
            result["structuredContent"]["status"], "prepared",
            "{result}"
        );
        for row in fixture.hooks.trace.lock().unwrap().iter() {
            let text = row.to_string();
            assert!(!text.contains("GetInputProperties"));
            assert!(!text.contains("SetInputMute"));
            assert!(!text.contains("SetInputAudioTracks"));
            assert!(!text.contains("SetInputSettings"));
        }
        fixture.close().await;
    }
}

fn one_transform_fixture() -> Arc<Hooks> {
    let hooks = Arc::new(Hooks::default());
    *hooks.platform.lock().unwrap() = "linux".into();
    hooks.late_transform.store(true, Ordering::Release);
    {
        let mut state = hooks.state.lock().unwrap();
        state["input"]["inputKind"] = json!("xcomposite_input");
        state["input"]["settings"] = json!({"capture_window":"123\r\nFixture"});
        state["items"]["Demo"][0]["inputKind"] = json!("xcomposite_input");
        let transform = state["items"]["Demo"][0]["sceneItemTransform"]
            .as_object_mut()
            .unwrap();
        for(key,value)in json!({"boundsType":"OBS_BOUNDS_SCALE_INNER","boundsWidth":1920,"boundsHeight":1080,"boundsAlignment":0,"alignment":5,"positionX":17,"positionY":0,"rotation":0}).as_object().unwrap(){transform.insert(key.clone(),value.clone());}
    }
    hooks
}

#[tokio::test]
async fn accepted_transform_reply_then_late_event_resamples_reads_for_prepare_and_restore() {
    let scratch = Scratch::new();
    let fixture = Fixture::new(one_transform_fixture(), REQUIRED_DEMO_EVENTS, false).await;
    let mut adapter = fixture.adapter(DemoAction::Prepare).await.unwrap();
    let mut snapshots = SnapshotFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
    let mut journals = JournalFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
    let ctx = context(&adapter, &fixture.url);
    let prepared = Demo::new(&mut adapter, &mut snapshots, &mut journals, ctx)
        .prepare(selection(), true, metadata(&fixture.url))
        .await;
    let first_status = prepared["structuredContent"]["status"].clone();
    if first_status != "prepared" {
        fixture.close().await;
        assert_eq!(first_status, "prepared", "{prepared}");
        return;
    }
    assert_eq!(fixture.hooks.writes.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture.hooks.state.lock().unwrap()["items"]["Demo"][0]["sceneItemTransform"]["positionX"],
        0
    );
    let mut adapter = fixture.adapter(DemoAction::Recover).await.unwrap();
    let ctx = context(&adapter, &fixture.url);
    let recovered = Demo::new(&mut adapter, &mut snapshots, &mut journals, ctx)
        .recover(ID, true)
        .await;
    assert_eq!(
        recovered["structuredContent"]["status"], "recovered",
        "{recovered}"
    );
    assert_eq!(fixture.hooks.writes.load(Ordering::Acquire), 2);
    assert_eq!(
        fixture.hooks.state.lock().unwrap()["items"]["Demo"][0]["sceneItemTransform"]["positionX"],
        17
    );
    fixture.close().await;
}

#[tokio::test]
async fn late_notification_resampling_preserves_human_fields_and_stops_all_further_effects() {
    for phase in ["prepare", "recover"] {
        for edit in [1, 2, 3] {
            for resample in [false, true] {
                let scratch = Scratch::new();
                let hooks = one_transform_fixture();
                if phase == "prepare" {
                    hooks.human_after_late.store(edit, Ordering::Release);
                    hooks.human_on_resample.store(resample, Ordering::Release);
                }
                let fixture = Fixture::new(hooks, REQUIRED_DEMO_EVENTS, false).await;
                let mut adapter = fixture.adapter(DemoAction::Prepare).await.unwrap();
                let mut snapshots = SnapshotFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
                let mut journals = JournalFile::new(&scratch.0, 4 * 1024 * 1024).unwrap();
                let ctx = context(&adapter, &fixture.url);
                let prepared = Demo::new(&mut adapter, &mut snapshots, &mut journals, ctx)
                    .prepare(selection(), true, metadata(&fixture.url))
                    .await;
                let report = if phase == "recover" {
                    assert_eq!(
                        prepared["structuredContent"]["status"], "prepared",
                        "{prepared}"
                    );
                    fixture
                        .hooks
                        .human_after_late
                        .store(edit, Ordering::Release);
                    fixture
                        .hooks
                        .human_on_resample
                        .store(resample, Ordering::Release);
                    let mut adapter = fixture.adapter(DemoAction::Recover).await.unwrap();
                    let ctx = context(&adapter, &fixture.url);
                    Demo::new(&mut adapter, &mut snapshots, &mut journals, ctx)
                        .recover(ID, true)
                        .await
                } else {
                    prepared
                };
                assert_eq!(
                    report["structuredContent"]["status"], "partial",
                    "{phase} edit{edit}: {report}"
                );
                assert_eq!(
                    fixture.hooks.writes.load(Ordering::Acquire),
                    if phase == "prepare" { 1 } else { 2 }
                );
                {
                    let state = fixture.hooks.state.lock().unwrap();
                    match edit {
                        1 => assert_eq!(
                            state["items"]["Demo"][0]["sceneItemTransform"]["positionX"],
                            23
                        ),
                        2 => assert_eq!(
                            state["items"]["Demo"][0]["sceneItemTransform"]["rotation"],
                            23
                        ),
                        3 => {
                            assert_eq!(state["input"]["settings"]["capture_window"], "456\r\nHuman")
                        }
                        _ => unreachable!(),
                    };
                }
                assert_eq!(
                    fixture.hooks.postwrite_collections.load(Ordering::Acquire),
                    4,
                    "resampling exceeded one fresh read"
                );
                assert_ne!(journals.load(ID).unwrap().to_value()["recovered"], true);
                fixture.close().await;
            }
        }
    }
}
