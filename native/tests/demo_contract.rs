// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{
    capture_backend,
    demo::{
        Delivery, Demo, DemoAdapter, DemoAdmission, DemoContext, DemoError, DemoMutation,
        DispatchFence, EvidenceStamp, HostClass, Journal, JournalFile, JournalStore,
        MutationFailure, MutationFuture, PrepareMetadata, PublicationPoint, Selection,
        recovery_summary,
    },
    observation::{ObservationError, ReadAdapter, ReadFuture, ReadRequest},
    snapshots::{
        BatchReadResult, CaptureMetadata, ReadCall, Snapshot, SnapshotError, SnapshotFuture,
        SnapshotReader, SnapshotWriter,
    },
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::time::Instant;
const URL: &str = "ws://127.0.0.1:4455";
const ID: &str = "12345678-1234-4234-8234-123456789abc";
const CAP: usize = 4 * 1024 * 1024;
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("obs-demo-contract-{}-{id}", std::process::id()));
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
fn initial() -> Value {
    json!({"collection":"Private Demo","program":"Demo","input":{"inputName":"Chrome","inputUuid":"input-1","inputKind":"screen_capture","settings":{"type":1,"window":123,"fixtureSecret":"SYNTHETIC-DEMO-SECRET"},"muted":false,"volume":0.7,"tracks":{"1":true,"2":false,"3":false,"4":false,"5":false,"6":false}},"scenes":[{"sceneName":"Demo","sceneUuid":"scene-1"}],"items":{"Demo":[{"sceneItemId":3,"sourceName":"Chrome","sourceUuid":"input-1","isGroup":null,"sourceType":"OBS_SOURCE_TYPE_INPUT","inputKind":"screen_capture","sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemIndex":0,"sceneItemTransform":{"boundsType":"OBS_BOUNDS_NONE","boundsWidth":600,"boundsHeight":400,"boundsAlignment":5,"alignment":0,"positionX":25,"positionY":35,"rotation":10,"sourceWidth":1512,"sourceHeight":982,"scaleX":0.75,"scaleY":0.75,"cropTop":23}}]},"video":{"baseWidth":1920,"baseHeight":1080},"outputs":[{"outputName":"Auxiliary"}],"statuses":{},"failures":{}})
}
fn selection() -> Selection {
    Selection {
        scene_collection: "Private Demo".into(),
        scene_name: "Demo".into(),
        input_name: "Chrome".into(),
    }
}
fn metadata() -> PrepareMetadata {
    PrepareMetadata {
        operation_id: ID.into(),
        snapshot: CaptureMetadata {
            id: "12345678".into(),
            obs_url: URL.into(),
            taken_at: "2026-10-31T12:00:00.000Z".into(),
        },
    }
}
fn context() -> DemoContext {
    DemoContext {
        endpoint: URL.into(),
        host: HostClass::Loopback,
        generation: 7,
        deadline: Instant::now() + Duration::from_secs(20),
        admission: DemoAdmission::default(),
    }
}
// Test-owned predecessor fixtures bypass the native product writer, which must
// never publish V1. The predecessor itself also creates V1 in oracle runs.
fn write_legacy(root: &Path, value: &Value) {
    let directory = root.join("demo-recovery");
    if !directory.exists() {
        fs::create_dir(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
    let path = directory.join(format!("{}.json", value["operationId"].as_str().unwrap()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path).unwrap();
    file.write_all(&serde_json::to_vec(value).unwrap()).unwrap();
    file.sync_all().unwrap();
}
#[derive(Default)]
struct Snapshots(Vec<Snapshot>);
impl SnapshotWriter for Snapshots {
    fn save(&mut self, snapshot: &Snapshot) -> Result<(), SnapshotError> {
        self.0.push(snapshot.clone());
        Ok(())
    }
}
struct Peer {
    state: Value,
    trace: Vec<Value>,
    stamp: EvidenceStamp,
    writes: usize,
    item_reads: usize,
    fail_at: Option<usize>,
    delivery: Delivery,
    stale_on_dispatch: bool,
    reads: usize,
    invalidate_at: Option<usize>,
    batch_delay: Duration,
    batches_started: usize,
    batches_completed: usize,
    cancel_read: bool,
    generation_on_batch: Option<usize>,
    effect_root: Option<PathBuf>,
    crash_write_at: Option<usize>,
    mutation_delay: Duration,
    mutations_settled: usize,
}
impl Peer {
    fn new(state: Value) -> Self {
        let fail_at = state["failAt"].as_u64().map(|n| n as usize);
        Self {
            state,
            trace: vec![],
            stamp: EvidenceStamp {
                generation: 7,
                revision: 0,
                connected: true,
                cancelled: false,
            },
            writes: 0,
            item_reads: 0,
            fail_at,
            delivery: Delivery::Unknown,
            stale_on_dispatch: false,
            reads: 0,
            invalidate_at: None,
            batch_delay: Duration::ZERO,
            batches_started: 0,
            batches_completed: 0,
            cancel_read: false,
            generation_on_batch: None,
            effect_root: None,
            crash_write_at: None,
            mutation_delay: Duration::ZERO,
            mutations_settled: 0,
        }
    }
    fn answer(&mut self, kind: &str, data: &Value) -> BatchReadResult {
        let failure = &self.state["failures"][kind];
        if let Some(code) = failure["code"].as_u64() {
            return BatchReadResult {
                ok: false,
                code: Some(code),
                comment: failure["comment"].as_str().map(str::to_owned),
                response_data: json!({}),
            };
        }
        let input = &self.state["input"];
        let value = match kind {
            "GetSceneCollectionList" => {
                json!({"currentSceneCollectionName":self.state["collection"]})
            }
            "GetStreamStatus"
            | "GetRecordStatus"
            | "GetVirtualCamStatus"
            | "GetReplayBufferStatus"
            | "GetOutputStatus" => self.state["statuses"]
                .get(kind)
                .cloned()
                .unwrap_or(json!({"outputActive":false,"outputReconnecting":false})),
            "GetOutputList" => json!({"outputs":self.state["outputs"]}),
            "GetSceneList" => {
                json!({"currentProgramSceneName":self.state["program"],"scenes":self.state["scenes"]})
            }
            "GetInputList" => {
                json!({"inputs":[{"inputName":input["inputName"],"inputKind":input["inputKind"],"inputUuid":input["inputUuid"]}]})
            }
            "GetSceneItemList" => {
                self.item_reads += 1;
                if self.state["snapshotConflict"] == true && self.item_reads == 2 {
                    self.state["items"]["Demo"][0]["sceneItemTransform"]["rotation"] = json!(123);
                }
                json!({"sceneItems":self.state["items"][data["sceneName"].as_str().unwrap()]})
            }
            "GetInputSettings" => {
                json!({"inputKind":input["inputKind"],"inputSettings":input["settings"]})
            }
            "GetInputMute" => json!({"inputMuted":input["muted"]}),
            "GetInputVolume" => json!({"inputVolumeMul":input["volume"]}),
            "GetInputAudioTracks" => json!({"inputAudioTracks":input["tracks"]}),
            "GetVideoSettings" => self.state["video"].clone(),
            _ => panic!("unexpected read {kind}"),
        };
        BatchReadResult {
            ok: true,
            code: Some(100),
            comment: None,
            response_data: value,
        }
    }
    fn edit(&mut self, path: &str, value: Value) {
        *self.state.pointer_mut(path).unwrap() = value;
    }
}
impl ReadAdapter for Peer {
    fn supports(&self, _: ReadRequest) -> Option<bool> {
        None
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        _deadline: Instant,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            self.trace
                .push(json!({"kind":"single","requestType":request.as_str(),"requestData":data}));
            self.reads += 1;
            if self.cancel_read {
                self.stamp.cancelled = true;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if self.invalidate_at == Some(self.reads) {
                self.stamp.revision += 1;
            }
            let result = self.answer(request.as_str(), &data);
            if result.ok {
                Ok(result.response_data)
            } else {
                let code = result.code.unwrap();
                Err(ObservationError::rejected(
                    code,
                    "evidence_unavailable",
                    format!(
                        "OBS request {} failed with code {code}{}",
                        request.as_str(),
                        result.comment.map(|s| format!(": {s}")).unwrap_or_default()
                    ),
                ))
            }
        })
    }
}
impl SnapshotReader for Peer {
    fn batch<'a>(&'a mut self, calls: Vec<ReadCall>) -> SnapshotFuture<'a, Vec<BatchReadResult>> {
        Box::pin(async move {
            if calls.is_empty() {
                return Ok(vec![]);
            }
            self.trace.push(json!({"kind":"batch","requests":calls.iter().map(|call|json!({"requestType":call.request.as_str(),"requestData":call.data})).collect::<Vec<_>>() }));
            self.batches_started += 1;
            if !self.batch_delay.is_zero() {
                tokio::time::sleep(self.batch_delay).await;
            }
            self.batches_completed += 1;
            if self.generation_on_batch == Some(self.batches_completed) {
                self.stamp.generation += 1;
            }
            Ok(calls
                .into_iter()
                .map(|call| self.answer(call.request.as_str(), &call.data))
                .collect())
        })
    }
}
impl DemoAdapter for Peer {
    fn evidence(&self) -> EvidenceStamp {
        self.stamp
    }
    fn mutate<'a>(
        &'a mut self,
        mutation: DemoMutation,
        fence: DispatchFence,
        deadline: Instant,
    ) -> MutationFuture<'a> {
        Box::pin(async move {
            if self.stale_on_dispatch {
                self.stamp.revision += 1;
            }
            if !self.stamp.connected
                || self.stamp.cancelled
                || fence.generation != self.stamp.generation
                || fence.revision != self.stamp.revision
                || Instant::now() >= deadline
            {
                return Err(MutationFailure {
                    delivery: Delivery::NotDispatched,
                    message: "Concrete dispatch fence rejected changed evidence".into(),
                });
            }
            self.writes += 1;
            let call = mutation.request();
            self.trace.push(json!({"kind":"single","requestType":call["requestType"],"requestData":call["requestData"]}));
            if self.fail_at == Some(self.writes) && self.delivery != Delivery::Unknown {
                return Err(MutationFailure {
                    delivery: self.delivery,
                    message: "fixture mutation failed".into(),
                });
            }
            match mutation {
                DemoMutation::Mute { muted, .. } => self.state["input"]["muted"] = json!(muted),
                DemoMutation::Tracks { tracks, .. } => {
                    self.state["input"]["tracks"] = json!(tracks)
                }
                DemoMutation::Fit { key, value, .. } => {
                    self.state["items"]["Demo"][0]["sceneItemTransform"][key.as_str()] = value
                }
            }
            if self.state["editAfterWrite"]["at"].as_u64() == Some(self.writes as u64) {
                let edit = self.state["editAfterWrite"].clone();
                self.edit(edit["path"].as_str().unwrap(), edit["value"].clone());
            }
            self.stamp.revision += 1;
            if let Some(root) = &self.effect_root {
                durable_fixture(&root.join("peer-state.json"), &self.state);
                if self.crash_write_at == Some(self.writes) {
                    crash_barrier(root, "effect-before-reply");
                }
            }
            if !self.mutation_delay.is_zero() {
                tokio::time::sleep(self.mutation_delay).await;
            }
            self.mutations_settled += 1;
            if self.fail_at == Some(self.writes) {
                return Err(MutationFailure {
                    delivery: Delivery::Unknown,
                    message: "fixture mutation failed".into(),
                });
            }
            Ok(())
        })
    }
}
fn durable_fixture(path: &Path, value: &Value) {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).unwrap();
    file.write_all(&serde_json::to_vec(value).unwrap()).unwrap();
    file.sync_all().unwrap();
    fs::File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}
fn crash_barrier(root: &Path, label: &str) -> ! {
    durable_fixture(&root.join("crash-ready.json"), &json!({"point":label}));
    loop {
        std::thread::park_timeout(Duration::from_secs(1));
    }
}
struct CrashStore {
    inner: JournalFile,
    root: PathBuf,
    case: String,
}
impl JournalStore for CrashStore {
    fn poison(&mut self, id: &str) {
        self.inner.poison(id)
    }
    fn load(&mut self, id: &str) -> Result<Journal, DemoError> {
        self.inner.load(id)
    }
    fn list(&mut self) -> Result<Vec<Journal>, DemoError> {
        self.inner.list()
    }
    fn persist(&mut self, journal: &Journal) -> Result<(), DemoError> {
        self.inner.persist(journal)?;
        let value = journal.to_value();
        let fields = &value["provenance"]["fields"];
        let preparation = fields[0]["preparation"]["steps"]
            .as_array()
            .and_then(|s| s.last())
            .and_then(Value::as_str);
        let recovery = fields[0]["recovery"]
            .as_array()
            .and_then(|a| a.last())
            .and_then(|a| a["steps"].as_array())
            .and_then(|s| s.last())
            .and_then(Value::as_str);
        let stop = match self.case.as_str() {
            "initial" => value["provenance"]["revision"] == 0,
            "intent" => preparation == Some("intent"),
            "possible" => preparation == Some("possible-dispatch"),
            "accepted" => preparation == Some("accepted"),
            "not-dispatched" => preparation == Some("not-dispatched"),
            "unknown" => preparation == Some("unknown"),
            "recovery-possible" => recovery == Some("possible-dispatch"),
            "recovery-accepted" => recovery == Some("accepted"),
            "recovery-not-dispatched" => recovery == Some("not-dispatched"),
            _ => false,
        };
        if stop {
            crash_barrier(&self.root, "persist-returned");
        }
        Ok(())
    }
}
#[tokio::test]
#[ignore = "subprocess crash driver; invoked by the restart control"]
async fn preparation_crash_child() {
    let root = PathBuf::from(
        std::env::var_os("OBS_NATIVE_PREPARATION_CRASH_ROOT").expect("owned fixture root"),
    );
    let case = std::env::var("OBS_NATIVE_PREPARATION_CRASH_CASE").unwrap();
    let mut file = JournalFile::new(&root, CAP).unwrap();
    let point = match case.as_str() {
        "stage-synced" => Some(PublicationPoint::StageSynced),
        "renamed" => Some(PublicationPoint::Renamed),
        "directory-synced" => Some(PublicationPoint::DirectorySynced),
        _ => None,
    };
    if let Some(point) = point {
        let owned = root.clone();
        file.set_publication_observer(Some(Box::new(move |at, metadata| {
            if at == point && metadata.revision == 2 {
                crash_barrier(&owned, "publication-boundary");
            }
        })));
    }
    let mut store = CrashStore {
        inner: file,
        root: root.clone(),
        case: case.clone(),
    };
    let mut peer = Peer::new(initial());
    peer.effect_root = Some(root.clone());
    if case == "not-dispatched" {
        peer.stale_on_dispatch = true;
    }
    if case == "unknown" {
        peer.fail_at = Some(1);
    }
    if case == "effect" {
        peer.crash_write_at = Some(1);
    }
    if case == "recovery-effect" {
        peer.crash_write_at = Some(11);
    }
    durable_fixture(&root.join("peer-state.json"), &peer.state);
    let mut snapshots = Snapshots::default();
    let report = Demo::new(&mut peer, &mut snapshots, &mut store, context())
        .prepare(selection(), true, metadata())
        .await;
    assert_eq!(report["structuredContent"]["status"], "prepared");
    if case.starts_with("recovery-") {
        if case == "recovery-not-dispatched" {
            peer.stale_on_dispatch = true;
        }
        let _ = Demo::new(&mut peer, &mut snapshots, &mut store, context())
            .recover(ID, true)
            .await;
    }
    panic!("crash schedule did not reach its selected boundary");
}
struct CrashChild(Option<std::process::Child>);
impl Drop for CrashChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
impl CrashChild {
    fn kill_and_reap(&mut self) {
        let mut child = self.0.take().unwrap();
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
    }
}
#[tokio::test]
async fn actual_process_death_at_publication_send_and_recovery_boundaries_never_invents_ownership()
{
    for case in [
        "initial",
        "intent",
        "stage-synced",
        "renamed",
        "directory-synced",
        "possible",
        "effect",
        "accepted",
        "not-dispatched",
        "unknown",
        "recovery-possible",
        "recovery-effect",
        "recovery-accepted",
        "recovery-not-dispatched",
    ] {
        let root = Scratch::new();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "preparation_crash_child",
                "--nocapture",
            ])
            .env("OBS_NATIVE_PREPARATION_CRASH_ROOT", &root.0)
            .env("OBS_NATIVE_PREPARATION_CRASH_CASE", case)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut child = CrashChild(Some(child));
        let until = Instant::now() + Duration::from_secs(5);
        while !root.0.join("crash-ready.json").exists() {
            assert!(Instant::now() < until, "crash driver did not reach {case}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        child.kill_and_reap();
        let state: Value =
            serde_json::from_slice(&fs::read(root.0.join("peer-state.json")).unwrap()).unwrap();
        let mut peer = Peer::new(state);
        let mut store = JournalFile::new(&root.0, CAP).unwrap();
        let persisted = store.load(ID).unwrap();
        assert_eq!(persisted.version(), 2);
        let status = persisted.provenance().unwrap().fields[0].preparation_status();
        if matches!(
            case,
            "initial" | "intent" | "stage-synced" | "not-dispatched"
        ) {
            assert!(!persisted.provenance().unwrap().fields[0].owns_restore());
            peer.state["input"]["muted"] = json!(true);
        }
        if case == "recovery-accepted" {
            peer.state["input"]["muted"] = json!(true);
        }
        if matches!(case, "renamed" | "directory-synced" | "possible") {
            assert_eq!(
                status,
                Some(obs_mcp_native::preparation_journal::Step::PossibleDispatch)
            );
            peer.state["input"]["muted"] = json!(true);
        }
        let mut snapshots = Snapshots::default();
        let report = Demo::new(&mut peer, &mut snapshots, &mut store, context())
            .recover(ID, true)
            .await;
        let outcome = report["structuredContent"]["status"].as_str().unwrap();
        match case {
            "initial" | "intent" | "stage-synced" | "recovery-accepted" | "not-dispatched" => {
                assert_eq!(outcome, "conflict", "{case}");
                assert_eq!(peer.writes, 0);
                assert_eq!(peer.state["input"]["muted"], true);
            }
            "accepted" => {
                assert_eq!(outcome, "recovered");
                assert_eq!(peer.writes, 1);
                assert_eq!(peer.state["input"]["muted"], false);
                let closed = store.load(ID).unwrap();
                assert!(closed.provenance().unwrap().completed);
                assert!(
                    closed
                        .provenance()
                        .unwrap()
                        .fields
                        .iter()
                        .all(|field| field.restored())
                );
            }
            "recovery-not-dispatched" => {
                assert_eq!(outcome, "recovered");
                assert_eq!(peer.writes, 10);
                let closed = store.load(ID).unwrap();
                let attempts = &closed.provenance().unwrap().fields[0].recovery;
                assert_eq!(attempts.len(), 2);
                assert_eq!(
                    attempts[0].status(),
                    obs_mcp_native::preparation_journal::Step::NotDispatched
                );
                assert_eq!(
                    attempts[1].status(),
                    obs_mcp_native::preparation_journal::Step::Accepted
                );
            }
            _ => {
                assert_eq!(outcome, "unknown", "{case}");
                assert_eq!(peer.writes, 0);
                assert!(
                    peer.trace.is_empty(),
                    "uncertain {case} must refuse before fresh reads"
                );
            }
        }
    }
}
#[tokio::test]
async fn real_predecessor_refuses_v2_prepare_recover_and_status_without_writes() {
    let root = Scratch::new();
    let input = json!({"state":initial(),"steps":[{"kind":"prepare","apply":true}]});
    let product = run(&input, &root.0).await;
    assert_eq!(product["journals"][0]["version"], 2);
    for kind in ["prepare", "recover"] {
        let predecessor = node(
            &json!({"state":initial(),"steps":[{"kind":kind,"apply":true}]}),
            &root.0,
        );
        assert_eq!(
            predecessor["reports"][0]["structuredContent"]["status"],
            "failed"
        );
        assert_eq!(predecessor["reports"][0]["isError"], true);
        assert!(
            predecessor["trace"]
                .as_array()
                .unwrap()
                .iter()
                .all(|entry| !entry["requestType"]
                    .as_str()
                    .is_some_and(|request| request.starts_with("Set")))
        );
        assert_eq!(predecessor["summary"]["status"], "unknown");
    }
    assert_eq!(
        JournalFile::new(&root.0, CAP)
            .unwrap()
            .load(ID)
            .unwrap()
            .to_value(),
        product["journals"][0]
    );
}
#[tokio::test]
async fn legacy_attempted_counter_is_unknown_but_zero_attempt_import_closes_without_writes() {
    let source = Scratch::new();
    let legacy = node(
        &json!({"state":initial(),"steps":[{"kind":"prepare","apply":true}]}),
        &source.0,
    );
    let root = Scratch::new();
    write_legacy(&root.0, &legacy["journals"][0]);
    let mut peer = Peer::new(legacy["state"].clone());
    let mut snapshots = Snapshots::default();
    let mut store = JournalFile::new(&root.0, CAP).unwrap();
    let report = Demo::new(&mut peer, &mut snapshots, &mut store, context())
        .recover(ID, true)
        .await;
    assert_eq!(report["structuredContent"]["status"], "unknown");
    assert!(peer.trace.is_empty());
    assert_eq!(peer.writes, 0);
    assert_eq!(store.load(ID).unwrap().version(), 1);
    let unattempted = Scratch::new();
    let mut journal = legacy["journals"][0].clone();
    journal["attempted"] = json!(0);
    write_legacy(&unattempted.0, &journal);
    let mut peer = Peer::new(initial());
    let mut store = JournalFile::new(&unattempted.0, CAP).unwrap();
    let report = Demo::new(&mut peer, &mut snapshots, &mut store, context())
        .recover(ID, true)
        .await;
    assert_eq!(report["structuredContent"]["status"], "recovered");
    assert_eq!(peer.writes, 0);
    let imported = store.load(ID).unwrap();
    assert_eq!(imported.version(), 2);
    assert!(imported.provenance().unwrap().completed);
    assert!(
        imported
            .provenance()
            .unwrap()
            .fields
            .iter()
            .all(|field| field.recovery_status()
                == Some(obs_mcp_native::preparation_journal::Step::ObservedBefore))
    );
}
fn normalize(value: Value) -> Value {
    match value {
        Value::Object(mut object) => {
            if let Some(Value::Array(content)) = object.get_mut("content") {
                for block in content {
                    if let Some(text) = block.get_mut("text")
                        && let Some(raw) = text.as_str()
                        && let Ok(parsed) = serde_json::from_str::<Value>(raw)
                    {
                        *text = parsed;
                    }
                }
            }
            Value::Object(object.into_iter().map(|(k, v)| (k, normalize(v))).collect())
        }
        Value::Array(values) => Value::Array(values.into_iter().map(normalize).collect()),
        value => value,
    }
}
// Normalize valid JSON text object-key order only; arrays/errors/metadata remain
// ordered and exact. Numeric comparison accepts JSON's 3 and 3.0 representation.
fn assert_json(actual: &Value, expected: &Value) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => assert_eq!(a.as_f64(), b.as_f64()),
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len());
            for (a, b) in a.iter().zip(b) {
                assert_json(a, b)
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(a.keys().collect::<Vec<_>>(), b.keys().collect::<Vec<_>>());
            for (k, v) in a {
                assert_json(v, &b[k])
            }
        }
        _ => assert_eq!(actual, expected),
    }
}
fn node(input: &Value, root: &Path) -> Value {
    let script = r#"
import crypto from'node:crypto';import{readFileSync,readdirSync,existsSync}from'node:fs';import{join}from'node:path';
import{initialize,recoverySummary}from'./build/tools/demo.js';import{configuredWindow}from'./build/tools/capture-backend.js';import{OBSRequestError}from'./build/client.js';import{McpServer,InMemoryTransport}from'@modelcontextprotocol/server';import{Client}from'@modelcontextprotocol/client';
const chunks=[];for await(const c of process.stdin)chunks.push(c);const input=JSON.parse(Buffer.concat(chunks));
if(input.backends){console.log(JSON.stringify(input.backends.map(([kind,settings])=>configuredWindow(kind,settings))));process.exit(0);}
const RealDate=Date;globalThis.Date=class extends RealDate{constructor(...args){super(...(args.length?args:['2026-10-31T12:00:00.000Z']));}static now(){return RealDate.parse('2026-10-31T12:00:00.000Z');}};crypto.randomUUID=()=>"12345678-1234-4234-8234-123456789abc";
const state=input.state,trace=[],reports=[];let writes=0,itemReads=0;
function answer(type,data={}){const failure=state.failures[type];if(failure)throw new OBSRequestError(type,failure.code,failure.comment);const i=state.input;
switch(type){case'GetSceneCollectionList':return{currentSceneCollectionName:state.collection};case'GetStreamStatus':case'GetRecordStatus':case'GetVirtualCamStatus':case'GetReplayBufferStatus':case'GetOutputStatus':return state.statuses[type]??{outputActive:false,outputReconnecting:false};case'GetOutputList':return{outputs:state.outputs};case'GetSceneList':return{currentProgramSceneName:state.program,scenes:state.scenes};case'GetInputList':return{inputs:[{inputName:i.inputName,inputKind:i.inputKind,inputUuid:i.inputUuid}]};case'GetSceneItemList':itemReads++;if(state.snapshotConflict&&itemReads===2)state.items.Demo[0].sceneItemTransform.rotation=123;return{sceneItems:state.items[data.sceneName]};case'GetInputSettings':return{inputKind:i.inputKind,inputSettings:i.settings};case'GetInputMute':return{inputMuted:i.muted};case'GetInputVolume':return{inputVolumeMul:i.volume};case'GetInputAudioTracks':return{inputAudioTracks:i.tracks};case'GetVideoSettings':return state.video;default:throw Error('unexpected read');}}
const client={getConnectionStatus(){return{url:input.endpoint??'ws://127.0.0.1:4455'}},async sendRequest(type,data){trace.push({kind:'single',requestType:type,requestData:data??null});if(type.startsWith('Set')){writes++;if(type==='SetInputMute')state.input.muted=data.inputMuted;else if(type==='SetInputAudioTracks')state.input.tracks=structuredClone(data.inputAudioTracks);else if(type==='SetSceneItemTransform')Object.assign(state.items.Demo[0].sceneItemTransform,data.sceneItemTransform);else throw Error('unexpected write');if(state.editAfterWrite?.at===writes){const parts=state.editAfterWrite.path.split('/').slice(1);let parent=state;for(const p of parts.slice(0,-1))parent=parent[p];parent[parts.at(-1)]=state.editAfterWrite.value;}if(writes===state.failAt)throw Error('fixture mutation failed');return{};}return structuredClone(answer(type,data));},async sendBatch(requests){if(!requests.length)return[];trace.push({kind:'batch',requests});return requests.map(({requestType,requestData})=>{try{return{ok:true,code:100,responseData:structuredClone(answer(requestType,requestData))}}catch(error){return{ok:false,code:error.code,comment:error.message,responseData:{}}}})}};
const server=new McpServer({name:'demo-oracle',version:'1'});initialize(server,client);const consumer=new Client({name:'consumer',version:'1'});const[a,b]=InMemoryTransport.createLinkedPair();await Promise.all([server.connect(a),consumer.connect(b)]);
try{for(const step of input.steps){if(step.kind==='edit'){const parts=step.path.split('/').slice(1);let parent=state;for(const p of parts.slice(0,-1))parent=parent[p];parent[parts.at(-1)]=step.value;continue;}reports.push(await consumer.callTool({name:step.kind==='prepare'?'obs-prepare-demo':'obs-recover-demo',arguments:step.kind==='prepare'?{sceneCollection:'Private Demo',sceneName:'Demo',inputName:'Chrome',apply:step.apply??false}:{operationId:step.operationId??'12345678-1234-4234-8234-123456789abc',apply:step.apply??false}}));}
const dir=join(process.env.OBS_MCP_STATE_DIR,'demo-recovery');const journals=existsSync(dir)?readdirSync(dir).filter(n=>n.endsWith('.json')).sort().map(n=>JSON.parse(readFileSync(join(dir,n),'utf8'))):[];const path=join(process.env.OBS_MCP_STATE_DIR,'snapshots.json');console.log(JSON.stringify({reports,trace,state,journals,history:existsSync(path)?JSON.parse(readFileSync(path,'utf8')):[],summary:recoverySummary(client)}));}finally{await consumer.close();await server.close();}
"#;
    let mut child = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(script)
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
        .env("OBS_MCP_STATE_DIR", root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let write = child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(input).unwrap());
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "demo source oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    write.unwrap();
    serde_json::from_slice(&output.stdout).unwrap()
}
async fn run(input: &Value, root: &Path) -> Value {
    let mut peer = Peer::new(input["state"].clone());
    let mut snapshots = Snapshots::default();
    let mut journals = JournalFile::new(root, CAP).unwrap();
    let mut reports = vec![];
    let ctx = context();
    for step in input["steps"].as_array().unwrap() {
        if step["kind"] == "edit" {
            peer.edit(step["path"].as_str().unwrap(), step["value"].clone());
            continue;
        }
        let mut demo = Demo::new(
            &mut peer,
            &mut snapshots,
            &mut journals,
            DemoContext {
                endpoint: ctx.endpoint.clone(),
                host: ctx.host,
                generation: ctx.generation,
                deadline: ctx.deadline,
                admission: ctx.admission.clone(),
            },
        );
        reports.push(if step["kind"] == "prepare" {
            demo.prepare(selection(), step["apply"] == true, metadata())
                .await
        } else {
            demo.recover(
                step["operationId"].as_str().unwrap_or(ID),
                step["apply"] == true,
            )
            .await
        });
    }
    let saved = journals
        .list()
        .unwrap()
        .into_iter()
        .map(|j| j.to_value())
        .collect::<Vec<_>>();
    json!({"reports":reports,"trace":peer.trace,"state":peer.state,"journals":saved,"history":snapshots.0,"summary":recovery_summary(&mut journals,URL)})
}
async fn parity(input: Value) -> Value {
    let ts = Scratch::new();
    let rs = Scratch::new();
    let expected = normalize(node(&input, &ts.0));
    let actual = normalize(run(&input, &rs.0).await);
    // The V2 private format is deliberately different. Check its schema, then
    // compare the preserved V1 payload separately; tool reports/effect traces
    // remain exact (only valid JSON-text object key order is normalized).
    let mut payload = actual.clone();
    for journal in payload["journals"].as_array_mut().unwrap() {
        if journal["version"] == 2 {
            Journal::from_value(journal.clone()).unwrap();
            journal.as_object_mut().unwrap().remove("provenance");
            journal["version"] = json!(1);
        }
    }
    if let Some(operations) = payload["summary"]["operations"].as_array_mut() {
        for operation in operations {
            operation.as_object_mut().unwrap().remove("provenance");
        }
    }
    assert_json(&payload, &expected);
    actual
}

#[tokio::test]
async fn exact_source_full_prepare_idempotence_and_recovery_reports_and_effects() {
    let result=parity(json!({"state":initial(),"steps":[{"kind":"prepare"},{"kind":"prepare","apply":true},{"kind":"prepare","apply":true},{"kind":"recover"},{"kind":"recover","apply":true},{"kind":"recover","apply":true}]})).await;
    assert_eq!(
        result["reports"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["structuredContent"]["status"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "planned",
            "prepared",
            "already-prepared",
            "planned",
            "recovered",
            "already-recovered"
        ]
    );
    let writes = result["trace"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| {
            v["requestType"]
                .as_str()
                .is_some_and(|s| s.starts_with("Set"))
        })
        .collect::<Vec<_>>();
    assert_eq!(writes.len(), 20);
    assert_eq!(writes[0]["requestType"], "SetInputMute");
    assert_eq!(writes[1]["requestType"], "SetInputAudioTracks");
    assert_eq!(
        writes[2]["requestData"]["sceneItemTransform"]
            .as_object()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(result["history"].as_array().unwrap().len(), 1);
    assert_eq!(result["journals"][0]["version"], 2);
    assert_eq!(result["state"]["input"]["volume"], 0.7);
    assert_eq!(
        result["state"]["items"]["Demo"][0]["sceneItemTransform"]["cropTop"],
        23
    );
}
#[tokio::test]
async fn source_lost_reply_requires_inspection_in_v2_and_preserves_unowned_edits() {
    let mut state = initial();
    state["failAt"] = json!(2);
    let input = json!({"state":state,"steps":[{"kind":"prepare","apply":true},{"kind":"edit","path":"/input/volume","value":0.25},{"kind":"edit","path":"/items/Demo/0/sceneItemTransform/cropTop","value":70},{"kind":"edit","path":"/items/Demo/0/sceneItemLocked","value":true},{"kind":"recover","apply":true}]});
    let ts = Scratch::new();
    let rs = Scratch::new();
    let predecessor = node(&input, &ts.0);
    assert_eq!(
        predecessor["reports"][1]["structuredContent"]["status"],
        "recovered"
    );
    let result = run(&input, &rs.0).await;
    assert_eq!(
        result["reports"][0]["structuredContent"]["status"],
        "partial"
    );
    assert_eq!(result["journals"][0]["attempted"], 2);
    assert_eq!(
        result["reports"][1]["structuredContent"]["status"],
        "unknown"
    );
    assert_eq!(result["state"]["input"]["volume"], 0.25);
    assert_eq!(
        result["state"]["items"]["Demo"][0]["sceneItemTransform"]["cropTop"],
        70
    );
    assert_eq!(result["state"]["items"]["Demo"][0]["sceneItemLocked"], true);
}
#[tokio::test]
async fn source_unattempted_desired_value_and_third_value_operator_changes_refuse_recovery() {
    for (path, value) in [
        ("/items/Demo/0/sceneItemTransform/positionX", json!(0)),
        ("/input/tracks/2", json!(true)),
    ] {
        let mut state = initial();
        state["editAfterWrite"] =
            json!({"at":1,"path":"/items/Demo/0/sceneItemTransform/rotation","value":0});
        let result=parity(json!({"state":state,"steps":[{"kind":"prepare","apply":true},{"kind":"edit","path":path,"value":value},{"kind":"recover","apply":true}]})).await;
        assert_eq!(
            result["reports"][1]["structuredContent"]["status"],
            "conflict"
        );
        assert_eq!(
            result["trace"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|t| t["requestType"]
                    .as_str()
                    .is_some_and(|k| k.starts_with("Set")))
                .count(),
            1
        );
    }
}
#[tokio::test]
async fn source_inactive_outputs_exact_604_and_selection_guards() {
    let mut cases = vec![];
    let mut s = initial();
    s["statuses"]["GetRecordStatus"] = json!({"outputActive":true});
    cases.push(s);
    let mut s = initial();
    s["statuses"]["GetOutputStatus"] = json!({"outputActive":false});
    cases.push(s);
    let mut s = initial();
    s["failures"]["GetVirtualCamStatus"] = json!({"code":604,"comment":"unconfigured"});
    s["failures"]["GetReplayBufferStatus"] = json!({"code":604,"comment":"unconfigured"});
    cases.push(s);
    let mut s = initial();
    s["failures"]["GetRecordStatus"] = json!({"code":604,"comment":"unavailable"});
    cases.push(s);
    let mut s = initial();
    s["items"]["Demo"][0]["isGroup"] = json!(true);
    cases.push(s);
    let mut s = initial();
    s["scenes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"sceneName":"Other","sceneUuid":"other"}));
    s["items"]["Other"] = s["items"]["Demo"].clone();
    cases.push(s);
    let mut s = initial();
    s["input"]["settings"]["type"] = json!(0);
    cases.push(s);
    let mut s = initial();
    s["input"]["tracks"]["6"] = Value::Null;
    cases.push(s);
    for state in cases {
        let result = parity(json!({"state":state,"steps":[{"kind":"prepare"}]})).await;
        assert!(result["trace"].as_array().unwrap().iter().all(|t| {
            !t["requestType"]
                .as_str()
                .is_some_and(|k| k.starts_with("Set"))
        }));
        assert!(result["history"].as_array().unwrap().is_empty());
    }
}
#[tokio::test]
async fn source_mid_preparation_conflict_and_replaced_identity_stop_all_further_writes() {
    let mut state = initial();
    state["editAfterWrite"] =
        json!({"at":1,"path":"/items/Demo/0/sceneItemTransform/rotation","value":75});
    let output=parity(json!({"state":state,"steps":[{"kind":"prepare","apply":true},{"kind":"prepare","apply":true},{"kind":"recover","apply":true}]})).await;
    assert_eq!(
        output["reports"][0]["structuredContent"]["status"],
        "partial"
    );
    assert_eq!(
        output["reports"][0]["structuredContent"]["issues"][0],
        "State conflict during preparation; automatic writes stopped"
    );
    assert_eq!(
        output["reports"][1]["structuredContent"]["status"],
        "conflict"
    );
    assert_eq!(
        output["reports"][2]["structuredContent"]["status"],
        "conflict"
    );
    assert_eq!(output["journals"][0]["attempted"], 1);
    for (path, value) in [
        ("/scenes/0/sceneUuid", json!("replacement-scene")),
        ("/input/settings/window", json!(999)),
    ] {
        let output=parity(json!({"state":initial(),"steps":[{"kind":"prepare","apply":true},{"kind":"edit","path":path,"value":value},{"kind":"recover","apply":true}]})).await;
        assert_eq!(
            output["reports"][1]["structuredContent"]["status"],
            "conflict"
        );
        assert_eq!(
            output["reports"][1]["structuredContent"]["issues"][0],
            "Collection/object identity or capture settings changed"
        );
    }
}
#[tokio::test]
async fn source_conflict_during_recovery_stops_without_overwriting_human_field() {
    let mut state = initial();
    state["editAfterWrite"] = json!({"at":11,"path":"/input/tracks/2","value":true});
    let output=parity(json!({"state":state,"steps":[{"kind":"prepare","apply":true},{"kind":"recover","apply":true}]})).await;
    assert_eq!(
        output["reports"][1]["structuredContent"]["status"],
        "partial"
    );
    assert_eq!(
        output["reports"][1]["structuredContent"]["issues"][0],
        "State conflict during recovery; writes stopped"
    );
    assert_eq!(output["state"]["input"]["tracks"]["2"], true);
    assert_eq!(output["state"]["input"]["muted"], false);
    assert!(output["journals"][0].get("recovered").is_none());
}
#[tokio::test]
async fn source_interrupted_capture_creates_snapshot_but_never_journal_or_mutations() {
    let mut state = initial();
    state["snapshotConflict"] = json!(true);
    let result = parity(json!({"state":state,"steps":[{"kind":"prepare","apply":true}]})).await;
    assert_eq!(
        result["reports"][0]["structuredContent"]["issues"][0],
        "State changed during snapshot; nothing changed"
    );
    assert_eq!(result["history"].as_array().unwrap().len(), 1);
    assert!(result["journals"].as_array().unwrap().is_empty());
}
#[tokio::test]
async fn source_video_only_platforms_do_not_acquire_audio_ownership() {
    for (kind, settings) in [
        (
            "xcomposite_input",
            json!({"capture_window":"123\r\nChrome"}),
        ),
        (
            "pipewire-window-capture-source",
            json!({"RestoreToken":"synthetic-authorized"}),
        ),
        (
            "pipewire-screen-capture-source",
            json!({"RestoreToken":"synthetic-authorized"}),
        ),
    ] {
        let mut state = initial();
        state["input"]["inputKind"] = json!(kind);
        state["input"]["settings"] = settings;
        for k in ["GetInputMute", "GetInputVolume", "GetInputAudioTracks"] {
            state["failures"][k] = json!({"code":604,"comment":"unsupported"});
        }
        let result=parity(json!({"state":state,"steps":[{"kind":"prepare","apply":true},{"kind":"recover","apply":true}]})).await;
        assert!(
            result["trace"]
                .as_array()
                .unwrap()
                .iter()
                .all(|t| !matches!(
                    t["requestType"].as_str(),
                    Some("SetInputMute" | "SetInputAudioTracks")
                ))
        );
    }
}
#[test]
fn source_backend_classification_is_configuration_evidence_only() {
    let cases = json!([["screen_capture",{"type":1,"window":2}],["screen_capture",{"type":"1","window":2}],["screen_capture",{"type":1,"window":0}],["xcomposite_input",{"capture_window":"123\r\nChrome"}],["xcomposite_input",{"capture_window":"000"}],["xcomposite_input",{"capture_window":"1\nChrome"}],["xcomposite_input",{"capture_window":"+1"}],["xcomposite_input",{"capture_window":"0001\r\n"}],["pipewire-screen-capture-source",{"RestoreToken":"synthetic"}],["pipewire-window-capture-source",{"RestoreToken":""}],["other",{"RestoreToken":"synthetic"}]]);
    let root = Scratch::new();
    let expected = node(&json!({"backends":cases}), &root.0);
    let actual = json!(
        cases
            .as_array()
            .unwrap()
            .iter()
            .map(|v| capture_backend::configured_window(
                v[0].as_str().unwrap(),
                v[1].as_object().unwrap()
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(actual, expected);
}

struct FaultStore {
    journals: Vec<Journal>,
    poisoned: HashSet<String>,
    writes: usize,
    fail_at: Option<usize>,
    publish: bool,
}
impl FaultStore {
    fn new(fail_at: Option<usize>, publish: bool) -> Self {
        Self {
            journals: vec![],
            poisoned: HashSet::new(),
            writes: 0,
            fail_at,
            publish,
        }
    }
}
impl JournalStore for FaultStore {
    fn poison(&mut self, id: &str) {
        self.poisoned.insert(id.into());
    }
    fn list(&mut self) -> Result<Vec<Journal>, DemoError> {
        if !self.poisoned.is_empty() {
            Err(DemoError::StorageUncertain)
        } else {
            Ok(self.journals.clone())
        }
    }
    fn load(&mut self, id: &str) -> Result<Journal, DemoError> {
        if self.poisoned.contains(id) {
            return Err(DemoError::StorageUncertain);
        }
        self.journals
            .iter()
            .find(|j| j.operation_id() == id)
            .cloned()
            .ok_or(DemoError::Missing)
    }
    fn persist(&mut self, journal: &Journal) -> Result<(), DemoError> {
        self.writes += 1;
        if self.poisoned.contains(journal.operation_id()) {
            return Err(DemoError::StorageUncertain);
        }
        if self.fail_at != Some(self.writes) || self.publish {
            self.journals
                .retain(|j| j.operation_id() != journal.operation_id());
            self.journals.push(journal.clone());
        }
        if self.fail_at == Some(self.writes) {
            Err(DemoError::StorageUncertain)
        } else {
            Ok(())
        }
    }
}
#[tokio::test]
async fn concrete_dispatch_guard_records_durable_not_dispatched_and_quarantines_uncertain_publication()
 {
    for (uncertain, publish) in [(false, false), (true, false), (true, true)] {
        let mut peer = Peer::new(initial());
        peer.stale_on_dispatch = true;
        let mut snaps = Snapshots::default();
        let mut store = FaultStore::new(if uncertain { Some(4) } else { None }, publish);
        let report = Demo::new(&mut peer, &mut snaps, &mut store, context())
            .prepare(selection(), true, metadata())
            .await;
        assert_eq!(report["structuredContent"]["status"], "partial");
        assert_eq!(peer.writes, 0);
        if uncertain {
            assert_eq!(store.journals[0].attempted(), if publish { 0 } else { 1 });
            assert!(store.load(ID).is_err());
            assert_eq!(recovery_summary(&mut store, URL)["status"], "unknown");
            let report = Demo::new(&mut peer, &mut snaps, &mut store, context())
                .recover(ID, true)
                .await;
            assert_eq!(report["structuredContent"]["status"], "failed");
        } else {
            assert_eq!(store.journals[0].attempted(), 0);
            peer.stale_on_dispatch = false;
            peer.edit("/input/muted", json!(true));
            let report = Demo::new(&mut peer, &mut snaps, &mut store, context())
                .recover(ID, true)
                .await;
            assert_eq!(report["structuredContent"]["status"], "conflict");
        }
        assert_eq!(peer.writes, 0);
    }
}
#[tokio::test]
async fn uncertain_journal_publication_stops_before_effect_and_stays_unknown() {
    for publish in [false, true] {
        let mut peer = Peer::new(initial());
        let mut snaps = Snapshots::default();
        let mut store = FaultStore::new(Some(2), publish);
        let report = Demo::new(&mut peer, &mut snaps, &mut store, context())
            .prepare(selection(), true, metadata())
            .await;
        assert_eq!(report["structuredContent"]["status"], "failed");
        assert_eq!(peer.writes, 0);
        assert_eq!(recovery_summary(&mut store, URL)["status"], "unknown");
        assert!(store.load(ID).is_err());
    }
}
#[tokio::test]
async fn changed_read_evidence_deadlines_and_shared_admission_never_dispatch() {
    let mut peer = Peer::new(initial());
    peer.invalidate_at = Some(2);
    let mut snaps = Snapshots::default();
    let mut store = FaultStore::new(None, false);
    let report = Demo::new(&mut peer, &mut snaps, &mut store, context())
        .prepare(selection(), true, metadata())
        .await;
    assert_eq!(report["structuredContent"]["status"], "failed");
    assert_eq!(peer.writes, 0);
    assert!(snaps.0.is_empty());
    assert!(store.journals.is_empty());
    let mut ctx = context();
    let _permit = ctx.admission.try_enter().unwrap();
    let reads = peer.reads;
    let report = Demo::new(&mut peer, &mut snaps, &mut store, ctx)
        .prepare(selection(), true, metadata())
        .await;
    assert_eq!(report["structuredContent"]["status"], "conflict");
    assert_eq!(peer.reads, reads);
    ctx = context();
    ctx.deadline = Instant::now();
    let report = Demo::new(&mut peer, &mut snaps, &mut store, ctx)
        .prepare(selection(), true, metadata())
        .await;
    assert_eq!(report["structuredContent"]["status"], "failed");
    assert_eq!(peer.reads, reads);
}
#[tokio::test]
async fn bounded_batch_capture_and_cancelled_inflight_read_never_detach_or_write() {
    let mut peer = Peer::new(initial());
    peer.batch_delay = Duration::from_millis(100);
    let mut snapshots = Snapshots::default();
    let mut store = FaultStore::new(None, false);
    let mut ctx = context();
    ctx.deadline = Instant::now() + Duration::from_millis(10);
    let report = Demo::new(&mut peer, &mut snapshots, &mut store, ctx)
        .prepare(selection(), true, metadata())
        .await;
    assert_eq!(report["structuredContent"]["status"], "failed");
    assert_eq!(peer.batches_started, 1);
    assert_eq!(
        peer.batches_completed, 1,
        "deadline returned the demo journal owner before its admitted batch settled"
    );
    assert_eq!(peer.writes, 0);
    assert!(snapshots.0.is_empty());
    assert!(store.journals.is_empty());
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(peer.batches_completed, 1);
    let mut peer = Peer::new(initial());
    peer.cancel_read = true;
    let started = Instant::now();
    let report = Demo::new(&mut peer, &mut snapshots, &mut store, context())
        .prepare(selection(), true, metadata())
        .await;
    assert_eq!(report["structuredContent"]["status"], "failed");
    assert!(started.elapsed() >= Duration::from_millis(10));
    assert_eq!(peer.reads, 1);
    assert_eq!(peer.writes, 0);
}

#[tokio::test]
async fn mutation_deadline_drains_original_work_and_late_acceptance_cannot_grant_recovery() {
    let mut peer = Peer::new(initial());
    peer.mutation_delay = Duration::from_millis(500);
    let mut snapshots = Snapshots::default();
    let mut store = FaultStore::new(None, false);
    let mut ctx = context();
    ctx.deadline = Instant::now() + Duration::from_millis(200);
    let report = Demo::new(&mut peer, &mut snapshots, &mut store, ctx)
        .prepare(selection(), true, metadata())
        .await;
    assert_eq!(report["structuredContent"]["status"], "partial");
    assert_eq!(peer.writes, 1);
    assert_eq!(
        peer.mutations_settled, 1,
        "writer returned before admitted mutation settled"
    );
    let journal = store.load(ID).unwrap();
    let field = &journal.provenance().unwrap().fields[0];
    assert_eq!(
        field.preparation_status(),
        Some(obs_mcp_native::preparation_journal::Step::Unknown)
    );
    assert!(!field.owns_restore());
    let writes = peer.writes;
    let recovery = Demo::new(&mut peer, &mut snapshots, &mut store, context())
        .recover(ID, true)
        .await;
    assert_eq!(recovery["isError"], true);
    assert_eq!(
        peer.writes, writes,
        "late acceptance granted automatic restore authority"
    );
}
#[tokio::test]
async fn replaced_connection_during_automatic_capture_never_publishes_snapshot_or_journal() {
    let mut peer = Peer::new(initial());
    peer.generation_on_batch = Some(3);
    let mut snapshots = Snapshots::default();
    let mut store = FaultStore::new(None, false);
    let report = Demo::new(&mut peer, &mut snapshots, &mut store, context())
        .prepare(selection(), true, metadata())
        .await;
    assert_eq!(peer.batches_completed, 3);
    assert_eq!(report["structuredContent"]["status"], "failed");
    assert_eq!(peer.writes, 0);
    assert!(snapshots.0.is_empty());
    assert!(store.journals.is_empty());
}
#[tokio::test]
async fn canonical_host_and_metadata_providers_fail_closed_before_any_effect() {
    for host in [HostClass::Other, HostClass::Unqualified] {
        let mut peer = Peer::new(initial());
        let mut snapshots = Snapshots::default();
        let mut store = FaultStore::new(None, false);
        let mut ctx = context();
        ctx.host = host;
        let report = Demo::new(&mut peer, &mut snapshots, &mut store, ctx)
            .prepare(selection(), true, metadata())
            .await;
        assert_eq!(report["structuredContent"]["status"], "failed");
        assert_eq!(peer.reads, 0);
        assert_eq!(peer.writes, 0);
    }
    let mut peer = Peer::new(initial());
    let mut snapshots = Snapshots::default();
    let mut store = FaultStore::new(None, false);
    let mut meta = metadata();
    meta.snapshot.obs_url = "ws://localhost:9999".into();
    let report = Demo::new(&mut peer, &mut snapshots, &mut store, context())
        .prepare(selection(), true, meta)
        .await;
    assert_eq!(report["structuredContent"]["status"], "failed");
    assert_eq!(peer.writes, 0);
    assert!(snapshots.0.is_empty());
    assert!(store.journals.is_empty());
}
#[tokio::test]
async fn real_journal_v1_import_metadata_summary_and_corruption_are_fail_closed() {
    let ts = Scratch::new();
    let input = json!({"state":initial(),"steps":[{"kind":"prepare","apply":true}]});
    let source = node(&input, &ts.0);
    let mut files = JournalFile::new(&ts.0, CAP).unwrap();
    let imported = files.load(ID).unwrap();
    assert_json(&imported.to_value(), &source["journals"][0]);
    assert_json(&recovery_summary(&mut files, URL), &source["summary"]);
    assert_eq!(
        recovery_summary(&mut files, "ws://localhost:9999")["operations"],
        json!([])
    );
    assert!(!format!("{imported:?}").contains("SYNTHETIC-DEMO-SECRET"));
    assert!(
        !serde_json::to_string(&recovery_summary(&mut files, URL))
            .unwrap()
            .contains("SYNTHETIC-DEMO-SECRET")
    );
    let path = ts.0.join("demo-recovery").join(format!("{ID}.json"));
    fs::write(&path, b"{").unwrap();
    assert!(matches!(files.load(ID), Err(DemoError::InvalidJournal)));
    assert_eq!(recovery_summary(&mut files, URL)["status"], "unknown");
    assert_eq!(fs::read(&path).unwrap(), b"{");
    assert!(files.persist(&imported).is_err());
}
#[tokio::test]
async fn journal_bounds_filename_identity_replaced_directory_and_symlinks_are_refused() {
    let root = Scratch::new();
    let mut peer = Peer::new(initial());
    let mut snapshots = Snapshots::default();
    let mut files = JournalFile::new(&root.0, CAP).unwrap();
    Demo::new(&mut peer, &mut snapshots, &mut files, context())
        .prepare(selection(), true, metadata())
        .await;
    let journal = files.load(ID).unwrap();
    let path = root.0.join("demo-recovery").join(format!("{ID}.json"));
    let mut bounded = JournalFile::new(&root.0, 16).unwrap();
    assert!(matches!(bounded.load(ID), Err(DemoError::InvalidJournal)));
    assert!(files.load("../../outside").is_err());
    let mut mismatch = journal.to_value();
    mismatch["operationId"] = json!("22345678-1234-4234-8234-123456789abc");
    fs::write(&path, serde_json::to_vec(&mismatch).unwrap()).unwrap();
    assert!(files.load(ID).is_err());
    fs::write(&path, serde_json::to_vec(&journal.to_value()).unwrap()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let private = root.0.join("private-copy");
        fs::rename(&path, &private).unwrap();
        symlink(&private, &path).unwrap();
        assert!(matches!(files.load(ID), Err(DemoError::UnsafePath)));
        assert!(files.persist(&journal).is_err());
        fs::remove_file(&path).unwrap();
        fs::rename(&private, &path).unwrap();
    }
    let dir = root.0.join("demo-recovery");
    fs::rename(&dir, root.0.join("saved-owned-directory")).unwrap();
    assert!(files.list().is_err());
    assert_eq!(recovery_summary(&mut files, URL)["status"], "unknown");
    assert!(!dir.exists());
}
#[tokio::test]
async fn journal_inventory_is_bounded_and_known_missing_never_becomes_empty_success() {
    let root = Scratch::new();
    let mut peer = Peer::new(initial());
    let mut snapshots = Snapshots::default();
    let mut files = JournalFile::new(&root.0, CAP).unwrap();
    assert_eq!(
        recovery_summary(&mut files, URL),
        json!({"status":"observed","operations":[]})
    );
    Demo::new(&mut peer, &mut snapshots, &mut files, context())
        .prepare(selection(), true, metadata())
        .await;
    let dir = root.0.join("demo-recovery");
    let journal = files.load(ID).unwrap();
    let path = dir.join(format!("{ID}.json"));
    fs::rename(&path, root.0.join("task-owned-journal-copy")).unwrap();
    assert!(matches!(files.list(), Err(DemoError::Missing)));
    assert_eq!(recovery_summary(&mut files, URL)["status"], "unknown");
    fs::rename(root.0.join("task-owned-journal-copy"), &path).unwrap();
    for index in 0..1024 {
        fs::write(dir.join(format!("noise-{index}")), b"").unwrap();
    }
    assert!(matches!(files.list(), Err(DemoError::InventoryLimit)));
    assert_eq!(
        recovery_summary(&mut files, URL),
        json!({"status":"unknown","reason":"Recovery inventory exceeds limit"})
    );
    assert!(journal.to_value()["state"]["settings"].is_object());
}
#[tokio::test]
async fn source_v1_unknown_fields_and_recovered_truthiness_survive_roundtrip() {
    let ts = Scratch::new();
    let original = node(
        &json!({"state":initial(),"steps":[{"kind":"prepare","apply":true}]}),
        &ts.0,
    )["journals"][0]
        .clone();
    for recovered in [Value::Null, json!(false), json!("legacy-marker"), json!([])] {
        let mut value = original.clone();
        value["recovered"] = recovered.clone();
        value["extension"] = json!({"private":"SYNTHETIC-EXTENSION"});
        value["state"]["legacyExtra"] = json!({"private":"SYNTHETIC-STATE"});
        value["changes"][0]["legacyExtra"] = json!([1, 2]);
        let journal = Journal::from_value(value.clone()).unwrap();
        assert_json(&journal.to_value(), &value);
        let root = Scratch::new();
        let mut file = JournalFile::new(&root.0, CAP).unwrap();
        write_legacy(&root.0, &journal.to_value());
        let summary = recovery_summary(&mut file, URL);
        let source = node(&json!({"state":initial(),"steps":[]}), &root.0);
        assert_json(&summary, &source["summary"]);
        assert_json(&journal.to_value(), &source["journals"][0]);
        let expected_count = if recovered == Value::Null || recovered == json!(false) {
            1
        } else {
            0
        };
        assert_eq!(
            summary["operations"].as_array().unwrap().len(),
            expected_count
        );
        assert!(!format!("{journal:?}").contains("SYNTHETIC"));
    }
}
#[tokio::test]
async fn source_pending_and_summary_keep_uuid_filename_order_and_numeric_v1_compatibility() {
    let seed = Scratch::new();
    let original = node(
        &json!({"state":initial(),"steps":[{"kind":"prepare","apply":true}]}),
        &seed.0,
    )["journals"][0]
        .clone();
    let rs = Scratch::new();
    let ts = Scratch::new();
    for root in [&rs.0, &ts.0] {
        for id in [
            "b2345678-1234-4234-8234-123456789abc",
            "A2345678-1234-4234-8234-123456789abc",
        ] {
            let mut value = original.clone();
            value["operationId"] = json!(id);
            value["version"] = json!(1.0);
            value["attempted"] = json!(1.0);
            write_legacy(root, &value);
        }
    }
    let input = json!({"state":initial(),"steps":[{"kind":"prepare"}]});
    let actual = normalize(run(&input, &rs.0).await);
    let expected = normalize(node(&input, &ts.0));
    assert_json(&actual, &expected);
    assert_eq!(
        actual["reports"][0]["structuredContent"]["operationId"],
        "A2345678-1234-4234-8234-123456789abc"
    );
    for id in [
        "00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "A2345678-1234-4234-8234-123456789ABC",
    ] {
        let mut value = original.clone();
        value["operationId"] = json!(id);
        value["attempted"] = json!(0);
        let rs = Scratch::new();
        let ts = Scratch::new();
        for root in [&rs.0, &ts.0] {
            write_legacy(root, &value);
        }
        let input = json!({"state":initial(),"steps":[{"kind":"recover","operationId":id}]});
        assert_json(
            &normalize(run(&input, &rs.0).await),
            &normalize(node(&input, &ts.0)),
        );
    }
    // Zod permits mixed-case RFC UUIDs but its special MAX literal is lowercase.
    let mut invalid = original.clone();
    invalid["operationId"] = json!("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF");
    assert!(Journal::from_value(invalid).is_err());
    let root = Scratch::new();
    let source = node(
        &json!({"state":initial(),"steps":[{"kind":"recover","operationId":"FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"}]}),
        &root.0,
    );
    assert_eq!(source["reports"][0]["isError"], true);
    assert!(source["reports"][0].get("structuredContent").is_none());
}
