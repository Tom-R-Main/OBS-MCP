// SPDX-License-Identifier: GPL-2.0-only
use futures_util::poll;
use obs_mcp_native::{
    clock::from_epoch_duration,
    control::{Disposition, RefusalKind},
    custody::Clock,
    media_verification::{MediaProcess, ProcessOptions, run_bounded_media_process},
    observation::{EvidenceState, ObservationError, ReadAdapter, ReadFuture, ReadRequest, Target},
    operation_store::{OperationStore, Phase},
    stage::{
        AdmissionError, Context, Mutation, MutationError, STAGE_TTL_MS, StageAdmission,
        StageMachine, StageMutator, StageReader,
    },
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    ffi::OsString,
    fs,
    future::Future,
    path::PathBuf,
    pin::Pin,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};

const EPOCH: u64 = 1_793_448_000_000;
const ENDPOINT: &str = "ws://fixture.invalid:4455";
type World = Arc<Mutex<Value>>;
type Trace = Arc<Mutex<Vec<Value>>>;
type ResultFuture<'a, T, E = ObservationError> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

fn target() -> Target {
    Target {
        scene_collection: "Audit collection".into(),
        scene_name: "Review".into(),
        canvas_uuid: None,
    }
}
fn id(number: usize) -> String {
    format!("00000000-0000-4000-8000-{number:012x}")
}
fn endpoint() -> String {
    format!("{:x}", Sha256::digest(ENDPOINT))
}
fn initial() -> Value {
    json!({"program":"Main","preview":"Main","studio":true,"setting":1,"muted":true,"generation":7,"connected":true,"cancelled":false,"audio604":false,"audioUnavailable":false,"image":"data:image/png;base64,AAAA",
        "item":{"sceneItemId":7,"sceneItemIndex":0,"sourceName":"Application","sourceUuid":"application-source-uuid","inputKind":"screen_capture","isGroup":false,"sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemTransform":{"sourceWidth":1280,"sourceHeight":720,"width":1280,"height":720,"positionX":0,"positionY":0,"scaleX":1,"scaleY":1,"rotation":0,"alignment":5,"boundsType":"OBS_BOUNDS_NONE","boundsWidth":0,"boundsHeight":0,"boundsAlignment":0,"cropLeft":0,"cropRight":0,"cropTop":0,"cropBottom":0}}})
}
fn uuid(name: &str) -> &'static str {
    if name == "Review" {
        "review-scene-uuid"
    } else {
        "main-scene-uuid"
    }
}
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let number = SEQUENCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .unwrap();
        let path = std::env::temp_dir().canonicalize().unwrap().join(format!(
            "obs-stage-contract-{}-{number}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn root(&self) -> PathBuf {
        self.0.join("state")
    }
    fn store(&self) -> OperationStore {
        OperationStore::new(self.root(), ENDPOINT).unwrap()
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn retained_child(ready: &std::path::Path, release: &std::path::Path) -> MediaProcess {
    run_bounded_media_process(
        "node",
        &[
            OsString::from("-e"),
            OsString::from(
                "const fs=require('node:fs');fs.writeFileSync(process.argv[1],String(process.pid),{mode:0o600});const until=Date.now()+750;const timer=setInterval(()=>{if(Date.now()>=until&&fs.existsSync(process.argv[2]))clearInterval(timer)},5);",
            ),
            ready.as_os_str().to_owned(),
            release.as_os_str().to_owned(),
        ],
        ProcessOptions {
            timeout: Duration::from_secs(3),
            ..ProcessOptions::default()
        },
    )
    .unwrap()
}

struct Reader {
    world: World,
    trace: Trace,
    endpoint: String,
    states: watch::Sender<EvidenceState>,
    pause: Option<ReadRequest>,
    entered: Arc<AtomicBool>,
    resume: Arc<Notify>,
    advance_on_program: Option<Arc<AtomicU64>>,
    retained: Option<MediaProcess>,
    child_paths: Option<(PathBuf, PathBuf)>,
    settled: Arc<AtomicBool>,
}
impl ReadAdapter for Reader {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        Some(
            !(self.world.lock().unwrap()["audioUnavailable"] == true
                && request == ReadRequest::InputMute),
        )
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        _deadline: Instant,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            self.trace
                .lock()
                .unwrap()
                .push(json!({"type":request.as_str(),"data":data}));
            if self.pause == Some(request) {
                self.entered.store(true, Ordering::Release);
                self.resume.notified().await;
            }
            if request == ReadRequest::SourceScreenshot
                && let Some((ready, release)) = &self.child_paths
            {
                if self.retained.is_none() {
                    self.retained = Some(retained_child(ready, release));
                }
                self.entered.store(true, Ordering::Release);
                let result = self.retained.as_mut().unwrap().finish().await;
                self.settled.store(true, Ordering::Release);
                result.map_err(|_| {
                    ObservationError::new("evidence_unavailable", "owned fixture child failed")
                })?;
            }
            let world = self.world.lock().unwrap();
            if request == ReadRequest::CurrentProgramScene
                && let Some(clock) = &self.advance_on_program
            {
                clock.store(EPOCH + 6_000, Ordering::Release);
            }
            if world["audio604"] == true
                && matches!(
                    request,
                    ReadRequest::InputMute
                        | ReadRequest::InputVolume
                        | ReadRequest::InputAudioTracks
                )
            {
                return Err(ObservationError::rejected(
                    604,
                    "capability_unavailable",
                    "fixture audio is unsupported",
                ));
            }
            Ok(match request {
                ReadRequest::StudioModeEnabled => json!({"studioModeEnabled":world["studio"]}),
                ReadRequest::SceneCollectionList => {
                    json!({"currentSceneCollectionName":"Audit collection"})
                }
                ReadRequest::SceneList => {
                    json!({"currentProgramSceneName":world["program"],"currentProgramSceneUuid":uuid(world["program"].as_str().unwrap()),"scenes":[{"sceneName":"Main","sceneUuid":"main-scene-uuid"},{"sceneName":"Review","sceneUuid":"review-scene-uuid"}]})
                }
                ReadRequest::VideoSettings => {
                    json!({"baseWidth":1920,"baseHeight":1080,"outputWidth":1920,"outputHeight":1080,"fpsNumerator":30,"fpsDenominator":1})
                }
                ReadRequest::InputList => {
                    json!({"inputs":[{"inputName":"Application","inputUuid":"application-source-uuid","inputKind":"screen_capture"}]})
                }
                ReadRequest::SceneItemList | ReadRequest::GroupSceneItemList => {
                    json!({"sceneItems":[world["item"]]})
                }
                ReadRequest::SceneItemTransform => {
                    json!({"sceneItemTransform":world["item"]["sceneItemTransform"]})
                }
                ReadRequest::InputSettings => {
                    json!({"inputKind":"screen_capture","inputSettings":{"setting":world["setting"]}})
                }
                ReadRequest::SourceFilterList => {
                    json!({"filters":world.get("filters").cloned().unwrap_or_else(|| json!([]))})
                }
                ReadRequest::InputMute => json!({"inputMuted":world["muted"]}),
                ReadRequest::InputVolume => json!({"inputVolumeMul":1}),
                ReadRequest::InputAudioTracks => json!({"inputAudioTracks":{"1":false}}),
                ReadRequest::CurrentPreviewScene => {
                    json!({"currentPreviewSceneName":world["preview"],"currentPreviewSceneUuid":uuid(world["preview"].as_str().unwrap())})
                }
                ReadRequest::CurrentProgramScene => {
                    json!({"currentProgramSceneName":world["program"],"currentProgramSceneUuid":uuid(world["program"].as_str().unwrap())})
                }
                ReadRequest::SourceScreenshot => json!({"imageData":world["image"]}),
                _ => {
                    return Err(ObservationError::new(
                        "evidence_unavailable",
                        "Unexpected stage fixture read",
                    ));
                }
            })
        })
    }
}
impl StageReader for Reader {
    fn endpoint_hash(&self) -> &str {
        &self.endpoint
    }
    fn connection_generation(&self) -> Option<u64> {
        let state = self.world.lock().unwrap();
        (state["connected"] == true).then(|| state["generation"].as_u64().unwrap())
    }
    fn cancelled(&self) -> bool {
        self.world.lock().unwrap()["cancelled"] == true
    }
    fn synchronize<'a>(
        &'a mut self,
        _deadline: Instant,
    ) -> ResultFuture<'a, watch::Receiver<EvidenceState>> {
        Box::pin(async move {
            let world = self.world.lock().unwrap();
            self.states.send_modify(|state| {
                state.connected = world["connected"] == true;
                state.cancelled = world["cancelled"] == true;
                state.generation = world["generation"].as_u64().unwrap();
            });
            if world["connected"] != true {
                return Err(ObservationError::new(
                    "observation_invalidated",
                    "fixture disconnected",
                ));
            }
            Ok(self.states.subscribe())
        })
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Normal,
    LostReply,
    NoEffect,
    WaitAfterTransition,
    RetainedChild,
}
struct Mutator {
    world: World,
    trace: Trace,
    endpoint: String,
    mode: Mode,
    refuse: bool,
    entered: Arc<AtomicBool>,
    retained: Option<MediaProcess>,
    release: Option<PathBuf>,
    ready: Option<PathBuf>,
    settled: Arc<AtomicBool>,
}
impl StageMutator for Mutator {
    fn endpoint_hash(&self) -> &str {
        &self.endpoint
    }
    fn connection_generation(&self) -> Option<u64> {
        let world = self.world.lock().unwrap();
        (world["connected"] == true).then(|| world["generation"].as_u64().unwrap())
    }
    fn admit(&mut self, _call: StageAdmission<'_>) -> Result<(), AdmissionError> {
        if self.refuse {
            Err(AdmissionError::Policy(Disposition::Refused {
                kind: RefusalKind::Lease,
                message: "fixture lease refusal".into(),
            }))
        } else {
            Ok(())
        }
    }
    fn mutate<'a>(
        &'a mut self,
        mutation: Mutation,
        expected_generation: u64,
        _expected_revision: u64,
        _deadline: Instant,
    ) -> ResultFuture<'a, (), MutationError> {
        Box::pin(async move {
            {
                let mut world = self.world.lock().unwrap();
                if world["generation"].as_u64() != Some(expected_generation)
                    || world["connected"] != true
                {
                    return Err(MutationError::NotDispatched(ObservationError::new(
                        "observation_invalidated",
                        "fixture connection changed",
                    )));
                }
                match mutation {
                    Mutation::SetPreview { ref scene_uuid } => {
                        self.trace.lock().unwrap().push(json!({"type":"SetCurrentPreviewScene","data":{"sceneUuid":scene_uuid}}));
                        world["preview"] = json!(if scene_uuid == "review-scene-uuid" {
                            "Review"
                        } else {
                            "Main"
                        });
                    }
                    Mutation::Transition => {
                        self.trace
                            .lock()
                            .unwrap()
                            .push(json!({"type":"TriggerStudioModeTransition","data":{}}));
                        if !matches!(self.mode, Mode::NoEffect) {
                            let preview = world["preview"].clone();
                            world["preview"] = world["program"].clone();
                            world["program"] = preview;
                        }
                    }
                }
            }
            if matches!(self.mode, Mode::RetainedChild) {
                if self.retained.is_none() {
                    self.retained = Some(retained_child(
                        self.ready.as_ref().unwrap(),
                        self.release.as_ref().unwrap(),
                    ));
                }
                self.entered.store(true, Ordering::Release);
                let result = self.retained.as_mut().unwrap().finish().await;
                self.settled.store(true, Ordering::Release);
                result.map_err(|_| {
                    MutationError::Unknown(ObservationError::new(
                        "observation_invalidated",
                        "owned fixture child failed",
                    ))
                })?;
            }
            if matches!(mutation, Mutation::Transition) {
                if matches!(self.mode, Mode::LostReply) {
                    return Err(MutationError::Unknown(ObservationError::new(
                        "observation_invalidated",
                        "fixture reply was lost",
                    )));
                }
                if matches!(self.mode, Mode::WaitAfterTransition) {
                    self.entered.store(true, Ordering::Release);
                    std::future::pending::<()>().await;
                }
            }
            Ok(())
        })
    }
}
struct Fixture {
    reader: Reader,
    mutator: Mutator,
    now: Arc<AtomicU64>,
    world: World,
    trace: Trace,
}
impl Fixture {
    fn new(world: Value) -> Self {
        let world = Arc::new(Mutex::new(world));
        let trace = Arc::new(Mutex::new(vec![]));
        let (states, _) = watch::channel(EvidenceState {
            generation: 7,
            revision: 0,
            connected: true,
            cancelled: false,
        });
        Self {
            reader: Reader {
                world: world.clone(),
                trace: trace.clone(),
                endpoint: endpoint(),
                states,
                pause: None,
                entered: Arc::new(AtomicBool::new(false)),
                resume: Arc::new(Notify::new()),
                advance_on_program: None,
                retained: None,
                child_paths: None,
                settled: Arc::new(AtomicBool::new(false)),
            },
            mutator: Mutator {
                world: world.clone(),
                trace: trace.clone(),
                endpoint: endpoint(),
                mode: Mode::Normal,
                refuse: false,
                entered: Arc::new(AtomicBool::new(false)),
                retained: None,
                release: None,
                ready: None,
                settled: Arc::new(AtomicBool::new(false)),
            },
            now: Arc::new(AtomicU64::new(EPOCH)),
            world,
            trace,
        }
    }
    fn context<'a>(&'a mut self, store: &'a mut OperationStore) -> Context<'a, Reader, Mutator> {
        let now = self.now.clone();
        let clock: Clock = Arc::new(move || {
            from_epoch_duration(Duration::from_millis(now.load(Ordering::Acquire))).unwrap()
        });
        Context {
            reader: &mut self.reader,
            mutator: &mut self.mutator,
            store,
            clock,
            deadline: Instant::now() + Duration::from_secs(12),
            screenshot_max_bytes: 4 * 1024 * 1024,
        }
    }
    fn writes(&self) -> Vec<Value> {
        self.trace
            .lock()
            .unwrap()
            .iter()
            .filter(|request| {
                matches!(
                    request["type"].as_str(),
                    Some("SetCurrentPreviewScene" | "TriggerStudioModeTransition")
                )
            })
            .cloned()
            .collect()
    }
}
fn ok(
    result: Result<obs_mcp_native::stage::Success, obs_mcp_native::stage::Failure>,
) -> obs_mcp_native::stage::Success {
    result.unwrap_or_else(|error| panic!("{}", error.report))
}
fn failure(
    result: Result<obs_mcp_native::stage::Success, obs_mcp_native::stage::Failure>,
) -> obs_mcp_native::stage::Failure {
    match result {
        Ok(success) => panic!("unexpected success {}", success.report),
        Err(error) => error,
    }
}
fn normalize(mut report: Value) -> Value {
    let object = report.as_object_mut().unwrap();
    object.remove("operationId");
    object.remove("stageId");
    object.remove("verifiedAt");
    report
}

fn node_oracle(initial: Value) -> Value {
    let scratch = Scratch::new();
    let script = r#"
import {EventEmitter} from 'node:events';
import {McpServer,InMemoryTransport} from '@modelcontextprotocol/server';
import {Client} from '@modelcontextprotocol/client';
import {initialize} from './build/tools/stage.js';
import {OBSRequestError,OBSConnectionChangedError} from './build/client.js';
const state=JSON.parse(process.env.STAGE_ORACLE_FIXTURE),trace=[];
Date.now=()=>1793448000000;
const uuid=name=>name==='Review'?'review-scene-uuid':'main-scene-uuid';
class Adapter extends EventEmitter {
 async connect(){} isConnected(){return state.connected} getConnectionStatus(){return {url:'ws://fixture.invalid:4455',generation:state.generation}}
 supportsRequest(type){return !(state.audioUnavailable&&type==='GetInputMute')}
 async sendRequestForGeneration(generation,type,data){if(generation!==state.generation)throw new OBSConnectionChangedError();return this.sendRequest(type,data)}
 async sendRequest(type,data){
  trace.push({type,data:data??null});
  if(state.audio604&&['GetInputMute','GetInputVolume','GetInputAudioTracks'].includes(type))throw new OBSRequestError(type,604);
  switch(type){
   case 'GetStudioModeEnabled':return {studioModeEnabled:state.studio};
   case 'GetSceneCollectionList':return {currentSceneCollectionName:'Audit collection'};
   case 'GetSceneList':return {currentProgramSceneName:state.program,currentProgramSceneUuid:uuid(state.program),scenes:[{sceneName:'Main',sceneUuid:'main-scene-uuid'},{sceneName:'Review',sceneUuid:'review-scene-uuid'}]};
   case 'GetVideoSettings':return {baseWidth:1920,baseHeight:1080,outputWidth:1920,outputHeight:1080,fpsNumerator:30,fpsDenominator:1};
   case 'GetInputList':return {inputs:[{inputName:'Application',inputUuid:'application-source-uuid',inputKind:'screen_capture'}]};
   case 'GetSceneItemList':case 'GetGroupSceneItemList':return {sceneItems:[structuredClone(state.item)]};
   case 'GetSceneItemTransform':return {sceneItemTransform:structuredClone(state.item.sceneItemTransform)};
   case 'GetInputSettings':return {inputKind:'screen_capture',inputSettings:{setting:state.setting}};
   case 'GetSourceFilterList':return {filters:structuredClone(state.filters??[])};
   case 'GetInputMute':return {inputMuted:state.muted};
   case 'GetInputVolume':return {inputVolumeMul:1};
   case 'GetInputAudioTracks':return {inputAudioTracks:{1:false}};
   case 'GetCurrentPreviewScene':return {currentPreviewSceneName:state.preview,currentPreviewSceneUuid:uuid(state.preview)};
   case 'GetCurrentProgramScene':return {currentProgramSceneName:state.program,currentProgramSceneUuid:uuid(state.program)};
   case 'GetSourceScreenshot':return {imageData:state.image};
   case 'SetCurrentPreviewScene':state.preview=data.sceneUuid==='review-scene-uuid'?'Review':'Main';return {};
   case 'TriggerStudioModeTransition':[state.program,state.preview]=[state.preview,state.program];return {};
   default:throw Error('Unexpected oracle request');
  }
 }
}
const adapter=new Adapter(),server=new McpServer({name:'stage-oracle',version:'1'}),client=new Client({name:'stage-oracle-client',version:'1'});
initialize(server,adapter);const [a,b]=InMemoryTransport.createLinkedPair();
const normalize=report=>{report=structuredClone(report);delete report.operationId;delete report.stageId;delete report.verifiedAt;return report};
try {
 await Promise.all([server.connect(a),client.connect(b)]);
 const staged=await client.callTool({name:'obs-stage-scene',arguments:{sceneCollection:'Audit collection',sceneName:'Review'}});
 let taken=null;if(!staged.isError){if(state.afterStageSetting!==undefined)state.setting=state.afterStageSetting;taken=await client.callTool({name:'obs-take-scene',arguments:{stageId:staged.structuredContent.stageId}});}
 console.log(JSON.stringify({staged:normalize(staged.structuredContent),taken:taken?normalize(taken.structuredContent):null,trace}));
}finally{await client.close();await server.close();}
"#;
    let output = Command::new("node")
        .args(["--input-type=module", "-e", script])
        .current_dir(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap(),
        )
        .env("STAGE_ORACLE_FIXTURE", initial.to_string())
        .env("OBS_MCP_STATE_DIR", scratch.root())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "pinned TypeScript stage oracle failed"
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn actual_typescript_oracle_matches_staging_take_reports_and_ordered_reads_and_effects() {
    for (program, preview) in [("Main", "Main"), ("Main", "Review"), ("Review", "Main")] {
        let mut initial = initial();
        initial["program"] = json!(program);
        initial["preview"] = json!(preview);
        let oracle = node_oracle(initial.clone());
        let scratch = Scratch::new();
        let mut store = scratch.store();
        let mut fixture = Fixture::new(initial);
        let mut machine = StageMachine::default();
        let staged = ok(machine
            .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
            .await);
        assert_eq!(normalize(staged.report), oracle["staged"]);
        assert_eq!(staged.image.unwrap().size_bytes(), 3);
        let taken = ok(machine
            .take(&mut fixture.context(&mut store), &id(1), &id(3))
            .await);
        assert_eq!(normalize(taken.report), oracle["taken"]);
        assert_eq!(
            *fixture.trace.lock().unwrap(),
            oracle["trace"].as_array().unwrap().clone()
        );
        assert_eq!(store.read(&id(3)).unwrap().phase(), Phase::Observed);
        assert_eq!(
            store.read(&id(3)).unwrap().to_value()["renderedComposition"],
            "unverified"
        );
        assert_eq!(machine.token_count(), 0);
        assert_eq!(
            failure(
                machine
                    .take(&mut fixture.context(&mut store), &id(1), &id(4))
                    .await
            )
            .report["reason"],
            "stage_unavailable"
        );
    }
}

#[tokio::test]
async fn only_actual_604_audio_rejections_are_tolerated_and_match_the_typescript_oracle() {
    for (flag, succeeds) in [("audio604", true), ("audioUnavailable", false)] {
        let mut world = initial();
        world[flag] = json!(true);
        let oracle = node_oracle(world.clone());
        let scratch = Scratch::new();
        let mut store = scratch.store();
        let mut fixture = Fixture::new(world);
        let mut machine = StageMachine::default();
        let result = machine
            .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
            .await;
        if succeeds {
            assert_eq!(normalize(ok(result).report), oracle["staged"]);
        } else {
            assert_eq!(failure(result).report["reason"], oracle["staged"]["reason"]);
            assert!(fixture.writes().is_empty());
            assert!(!scratch.root().exists());
        }
    }
}

#[tokio::test]
async fn silent_preview_settings_transform_program_studio_and_audio_changes_consume_stage_without_a_transition()
 {
    for change in [
        "preview",
        "setting",
        "transform",
        "program",
        "studio",
        "audio",
    ] {
        let scratch = Scratch::new();
        let mut store = scratch.store();
        let mut fixture = Fixture::new(initial());
        let mut machine = StageMachine::default();
        ok(machine
            .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
            .await);
        fixture.trace.lock().unwrap().clear();
        {
            let mut world = fixture.world.lock().unwrap();
            match change {
                "preview" => world["preview"] = json!("Main"),
                "setting" => world["setting"] = json!(2),
                "transform" => world["item"]["sceneItemTransform"]["positionX"] = json!(10),
                "program" => world["program"] = json!("Review"),
                "studio" => world["studio"] = json!(false),
                "audio" => world["muted"] = json!(false),
                _ => unreachable!(),
            }
        }
        assert_eq!(
            failure(
                machine
                    .take(&mut fixture.context(&mut store), &id(1), &id(3))
                    .await
            )
            .report["status"],
            "blocked",
            "{change}"
        );
        assert_eq!(machine.token_count(), 0);
        assert!(fixture.writes().is_empty());
        assert!(store.read(&id(3)).is_err());
    }
}

#[tokio::test]
async fn expiry_is_exact_and_reconnect_consumes_the_old_generation_before_inspection() {
    for expiry in [true, false] {
        let scratch = Scratch::new();
        let mut store = scratch.store();
        let mut fixture = Fixture::new(initial());
        let mut machine = StageMachine::default();
        ok(machine
            .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
            .await);
        fixture.trace.lock().unwrap().clear();
        if expiry {
            fixture.now.store(EPOCH + STAGE_TTL_MS, Ordering::Release);
        } else {
            fixture.world.lock().unwrap()["generation"] = json!(8);
        }
        let failed = failure(
            machine
                .take(&mut fixture.context(&mut store), &id(1), &id(3))
                .await,
        );
        assert_eq!(
            failed.report["reason"],
            if expiry {
                "stage_unavailable"
            } else {
                "observation_invalidated"
            }
        );
        assert_eq!(machine.token_count(), 0);
        assert!(fixture.trace.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn a_new_stage_invalidates_old_tokens_even_when_its_inspection_fails() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut fixture = Fixture::new(initial());
    let mut machine = StageMachine::default();
    ok(machine
        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
        .await);
    fixture.world.lock().unwrap()["studio"] = json!(false);
    assert_eq!(
        failure(
            machine
                .stage(&mut fixture.context(&mut store), target(), &id(3), &id(4))
                .await
        )
        .report["status"],
        "blocked"
    );
    assert_eq!(machine.token_count(), 0);
    fixture.world.lock().unwrap()["studio"] = json!(true);
    assert_eq!(
        failure(
            machine
                .take(&mut fixture.context(&mut store), &id(1), &id(5))
                .await
        )
        .report["reason"],
        "stage_unavailable"
    );
}

#[tokio::test]
async fn admitted_take_consumes_token_even_when_its_deadline_is_already_expired() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut fixture = Fixture::new(initial());
    let mut machine = StageMachine::default();
    ok(machine
        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
        .await);
    fixture.trace.lock().unwrap().clear();
    let mut context = fixture.context(&mut store);
    context.deadline = Instant::now();
    let failed = failure(machine.take(&mut context, &id(1), &id(3)).await);
    assert_eq!(failed.report["status"], "blocked");
    assert_eq!(failed.report["reason"], "observation_limit");
    assert_eq!(machine.token_count(), 0);
    assert!(fixture.writes().is_empty());
    assert!(store.read(&id(3)).is_err());
}

#[tokio::test]
async fn lost_transition_reply_is_unknown_and_a_consumed_token_cannot_replay_it() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut fixture = Fixture::new(initial());
    let mut machine = StageMachine::default();
    ok(machine
        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
        .await);
    fixture.trace.lock().unwrap().clear();
    fixture.mutator.mode = Mode::LostReply;
    let failed = failure(
        machine
            .take(&mut fixture.context(&mut store), &id(1), &id(3))
            .await,
    );
    assert_eq!(failed.report["status"], "unknown");
    assert!(failed.report["requestAccepted"].is_null());
    assert_eq!(store.read(&id(3)).unwrap().phase(), Phase::Unknown);
    assert_eq!(
        store.read(&id(3)).unwrap().to_value()["postconditionObserved"],
        false
    );
    assert_eq!(
        failure(
            machine
                .take(&mut fixture.context(&mut store), &id(1), &id(4))
                .await
        )
        .report["reason"],
        "stage_unavailable"
    );
    assert_eq!(fixture.writes().len(), 1);
}

#[tokio::test]
async fn accepted_transition_without_observed_program_never_reports_verified() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut fixture = Fixture::new(initial());
    let mut machine = StageMachine::default();
    ok(machine
        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
        .await);
    fixture.mutator.mode = Mode::NoEffect;
    fixture.reader.advance_on_program = Some(fixture.now.clone());
    let failed = failure(
        machine
            .take(&mut fixture.context(&mut store), &id(1), &id(3))
            .await,
    );
    assert_eq!(failed.report["status"], "unknown");
    assert_eq!(failed.report["reason"], "verification_timeout");
    assert_eq!(failed.report["requestAccepted"], true);
    assert!(failed.report["intendedSceneObservedOnProgram"].is_null());
    assert_eq!(
        store.read(&id(3)).unwrap().to_value()["postconditionObserved"],
        false
    );
}

#[tokio::test]
async fn cancelled_workflow_future_consumes_token_and_preserves_dispatched_intent_without_replay() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut fixture = Fixture::new(initial());
    let mut machine = StageMachine::default();
    ok(machine
        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
        .await);
    fixture.trace.lock().unwrap().clear();
    fixture.mutator.mode = Mode::WaitAfterTransition;
    let entered = fixture.mutator.entered.clone();
    let mut context = fixture.context(&mut store);
    let stage_id = id(1);
    let operation_id = id(3);
    let mut workflow = Box::pin(machine.take(&mut context, &stage_id, &operation_id));
    assert!(poll!(&mut workflow).is_pending());
    assert!(entered.load(Ordering::Acquire));
    drop(workflow);
    assert_eq!(machine.token_count(), 0);
    assert_eq!(store.read(&id(3)).unwrap().phase(), Phase::Dispatching);
    assert_eq!(store.summary()["operations"][0]["status"], "unknown");
    assert_eq!(
        failure(
            machine
                .take(&mut fixture.context(&mut store), &id(1), &id(4))
                .await
        )
        .report["reason"],
        "stage_unavailable"
    );
    assert_eq!(fixture.writes().len(), 1);
}
#[tokio::test]
async fn stage_and_take_deadlines_retain_the_admitted_child_until_join_and_discard_late_success() {
    for take in [false, true] {
        let scratch = Scratch::new();
        let mut store = scratch.store();
        let mut fixture = Fixture::new(initial());
        let mut machine = StageMachine::default();
        if take {
            ok(machine
                .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
                .await);
            fixture.trace.lock().unwrap().clear();
        }
        fixture.mutator.mode = Mode::RetainedChild;
        let release = scratch.0.join("release");
        fixture.mutator.release = Some(release.clone());
        fixture.mutator.ready = Some(scratch.0.join("ready"));
        let settled = fixture.mutator.settled.clone();
        let inspector = store.read_view();
        let operation = id(3);
        let stage = id(1);
        let mut context = fixture.context(&mut store);
        context.deadline = Instant::now() + Duration::from_millis(250);
        let mut workflow: Pin<Box<dyn Future<Output = _>>> = if take {
            Box::pin(machine.take(&mut context, &stage, &operation))
        } else {
            Box::pin(machine.stage(&mut context, target(), &stage, &operation))
        };
        let early = tokio::time::timeout(Duration::from_millis(400), &mut workflow).await;
        let returned_before_child = early.is_ok();
        let phase_before_join = inspector.read(&operation).unwrap().phase();
        let settled_before_join = settled.load(Ordering::Acquire);
        fs::write(&release, b"release owned child").unwrap();
        let result = match early {
            Ok(result) => {
                drop(workflow);
                result
            }
            Err(_) => workflow.await,
        };
        drop(context);
        // Always finish the retained fixture, including the failing negative control;
        // an assertion must not leave the child or its stderr owner behind.
        fixture
            .mutator
            .retained
            .as_mut()
            .expect("mutation admitted one fixture child")
            .finish()
            .await
            .unwrap();
        let failed = failure(result);
        assert!(
            !returned_before_child,
            "deadline returned the stage writer while its admitted child was still owned"
        );
        assert!(!settled_before_join);
        assert_eq!(phase_before_join, Phase::Dispatching);
        assert!(settled.load(Ordering::Acquire));
        assert_eq!(failed.report["status"], "unknown");
        assert_eq!(failed.report["reason"], "observation_limit");
        assert!(failed.report["requestAccepted"].is_null());
        assert_eq!(store.read(&operation).unwrap().phase(), Phase::Unknown);
        assert_eq!(machine.token_count(), 0);
        assert_eq!(fixture.writes().len(), 1);
        if take {
            assert_eq!(
                failure(
                    machine
                        .take(&mut fixture.context(&mut store), &stage, &id(4))
                        .await
                )
                .report["reason"],
                "stage_unavailable"
            );
            assert_eq!(fixture.writes().len(), 1);
        }
    }
}

#[tokio::test]
async fn screenshot_deadline_drains_owned_read_and_rejects_late_evidence_without_further_reads() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut fixture = Fixture::new(initial());
    let release = scratch.0.join("release");
    fixture.reader.child_paths = Some((scratch.0.join("ready"), release.clone()));
    let settled = fixture.reader.settled.clone();
    let trace = fixture.trace.clone();
    let inspector = store.read_view();
    let mut machine = StageMachine::default();
    let stage = id(1);
    let operation = id(2);
    let mut context = fixture.context(&mut store);
    context.deadline = Instant::now() + Duration::from_millis(250);
    let mut workflow = Box::pin(machine.stage(&mut context, target(), &stage, &operation));
    let early = tokio::time::timeout(Duration::from_millis(400), &mut workflow).await;
    let returned_before_child = early.is_ok();
    let phase_before_join = inspector.read(&operation).unwrap().phase();
    let settled_before_join = settled.load(Ordering::Acquire);
    let reads_before_join = trace.lock().unwrap().len();
    fs::write(&release, b"release owned child").unwrap();
    let result = match early {
        Ok(result) => {
            drop(workflow);
            result
        }
        Err(_) => workflow.await,
    };
    drop(context);
    fixture
        .reader
        .retained
        .as_mut()
        .unwrap()
        .finish()
        .await
        .unwrap();
    let failed = failure(result);
    assert!(
        !returned_before_child,
        "read deadline released its admitted child"
    );
    assert!(!settled_before_join);
    assert_eq!(phase_before_join, Phase::Accepted);
    assert!(settled.load(Ordering::Acquire));
    assert_eq!(trace.lock().unwrap().len(), reads_before_join);
    assert_eq!(
        trace.lock().unwrap().last().unwrap()["type"],
        "GetSourceScreenshot"
    );
    assert_eq!(failed.report["status"], "unknown");
    assert_eq!(failed.report["reason"], "evidence_unavailable");
    assert_eq!(
        failed.report["message"],
        "Request GetSourceScreenshot timed out"
    );
    assert_eq!(failed.report["requestAccepted"], true);
    assert_eq!(store.read(&operation).unwrap().phase(), Phase::Unknown);
    assert_eq!(machine.token_count(), 0);
    assert_eq!(fixture.writes().len(), 1);
}

#[tokio::test]
async fn abandoned_frontend_keeps_service_task_and_dispatching_receipt_until_child_is_joined() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let inspector = store.read_view();
    let mut fixture = Fixture::new(initial());
    fixture.mutator.mode = Mode::RetainedChild;
    let release = scratch.0.join("release");
    fixture.mutator.release = Some(release.clone());
    fixture.mutator.ready = Some(scratch.0.join("ready"));
    let entered = fixture.mutator.entered.clone();
    let settled = fixture.mutator.settled.clone();
    let (sender, frontend) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut machine = StageMachine::default();
        let mut context = fixture.context(&mut store);
        context.deadline = Instant::now() + Duration::from_millis(250);
        let result = machine.stage(&mut context, target(), &id(1), &id(2)).await;
        drop(context);
        // A receiver going away cannot retire this service-owned future.
        let report = failure(result).report;
        let _ = sender.send(report.clone());
        fixture
            .mutator
            .retained
            .as_mut()
            .unwrap()
            .finish()
            .await
            .unwrap();
        (report, fixture.writes().len(), machine.token_count())
    });
    let wait_until = Instant::now() + Duration::from_secs(1);
    while !entered.load(Ordering::Acquire) && Instant::now() < wait_until {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let admitted = entered.load(Ordering::Acquire);
    drop(frontend);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let completed_before_join = task.is_finished();
    let phase_before_join = inspector.read(&id(2)).map(|receipt| receipt.phase());
    let settled_before_join = settled.load(Ordering::Acquire);
    fs::write(&release, b"release owned child").unwrap();
    // Retain and join the task even if a captured assertion will fail below.
    let (report, writes, tokens) = task.await.unwrap();
    assert!(admitted);
    assert!(!completed_before_join);
    assert!(!settled_before_join);
    assert_eq!(phase_before_join.unwrap(), Phase::Dispatching);
    assert!(settled.load(Ordering::Acquire));
    assert_eq!(report["status"], "unknown");
    assert_eq!(report["reason"], "observation_limit");
    assert!(report["requestAccepted"].is_null());
    assert_eq!(inspector.read(&id(2)).unwrap().phase(), Phase::Unknown);
    assert_eq!(writes, 1);
    assert_eq!(tokens, 0);
}

#[tokio::test]
async fn policy_refusal_keeps_a_stage_usable_and_receipt_failure_blocks_preview_effects() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut fixture = Fixture::new(initial());
    let mut machine = StageMachine::default();
    ok(machine
        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
        .await);
    fixture.mutator.refuse = true;
    let denied = failure(
        machine
            .take(&mut fixture.context(&mut store), &id(1), &id(3))
            .await,
    );
    assert!(denied.policy.is_some());
    assert_eq!(machine.token_count(), 1);
    fixture.mutator.refuse = false;
    ok(machine
        .take(&mut fixture.context(&mut store), &id(1), &id(4))
        .await);
    let fresh = Scratch::new();
    let mut blocked_store = fresh.store();
    fs::write(fresh.root(), b"blocked directory").unwrap();
    let mut next = Fixture::new(initial());
    let mut next_machine = StageMachine::default();
    let failed = failure(
        next_machine
            .stage(
                &mut next.context(&mut blocked_store),
                target(),
                &id(1),
                &id(2),
            )
            .await,
    );
    assert_eq!(failed.report["reason"], "receipt_unavailable");
    assert!(next.writes().is_empty());
    assert_eq!(next_machine.token_count(), 0);
}

#[tokio::test]
async fn cancellation_during_inspection_and_cycles_cannot_publish_or_mutate() {
    for cycle in [false, true] {
        let scratch = Scratch::new();
        let mut store = scratch.store();
        let mut fixture = Fixture::new(initial());
        let mut machine = StageMachine::default();
        if cycle {
            {
                let mut world = fixture.world.lock().unwrap();
                world["item"]["isGroup"] = json!(true);
                world["item"]["sourceUuid"] = json!("nested-group");
            }
            assert_eq!(
                failure(
                    machine
                        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
                        .await
                )
                .report["reason"],
                "evidence_unavailable"
            );
        } else {
            fixture.world.lock().unwrap()["cancelled"] = json!(true);
            assert_eq!(
                failure(
                    machine
                        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
                        .await
                )
                .report["reason"],
                "cancelled"
            );
        }
        assert!(fixture.writes().is_empty());
        assert!(!scratch.root().exists());
        assert_eq!(machine.token_count(), 0);
    }
}

#[tokio::test]
async fn invalid_screenshot_after_preview_effect_retains_unknown_and_never_publishes_stage() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut world = initial();
    world["image"] = json!("data:image/png;base64,not base64");
    let mut fixture = Fixture::new(world);
    let mut machine = StageMachine::default();
    let failed = failure(
        machine
            .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
            .await,
    );
    assert_eq!(failed.report["status"], "unknown");
    assert_eq!(failed.report["requestAccepted"], true);
    assert_eq!(store.read(&id(2)).unwrap().phase(), Phase::Unknown);
    assert_eq!(machine.token_count(), 0);
    assert_eq!(fixture.writes().len(), 1);
}

#[tokio::test]
async fn private_owner_local_fingerprints_support_unicode_settings_and_filter_keys_without_public_hashes()
 {
    let mut world = initial();
    world["setting"] = json!({"mañana":"private-fixture-value"});
    world["filters"] = json!([{"filterName":"Colour","filterSettings":{"🌙":"private-filter-value","a-b":2,"ø":true}}]);
    let source = node_oracle(world.clone());
    assert_eq!(source["staged"]["status"], "staged");
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut fixture = Fixture::new(world);
    let mut machine = StageMachine::default();
    let staged = ok(machine
        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
        .await);
    assert_eq!(normalize(staged.report.clone()), source["staged"]);
    assert!(!staged.report.to_string().contains("private-fixture-value"));
    assert!(!staged.report.to_string().contains("private-filter-value"));
    let receipt = store.read(&id(2)).unwrap().to_value().to_string();
    assert!(!receipt.contains("private-fixture-value"));
    assert!(!receipt.contains("fingerprint"));
    let taken = ok(machine
        .take(&mut fixture.context(&mut store), &id(1), &id(3))
        .await);
    assert_eq!(normalize(taken.report), source["taken"]);
    assert_eq!(
        *fixture.trace.lock().unwrap(),
        source["trace"].as_array().unwrap().clone()
    );
    assert_eq!(machine.token_count(), 0);
}

#[tokio::test]
async fn a_changed_unicode_setting_is_refused_before_transition_and_matches_source() {
    let mut world = initial();
    world["setting"] = json!({"mañana":1});
    world["afterStageSetting"] = json!({"mañana":2});
    let source = node_oracle(world.clone());
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut fixture = Fixture::new(world.clone());
    let mut machine = StageMachine::default();
    ok(machine
        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
        .await);
    fixture.world.lock().unwrap()["setting"] = world["afterStageSetting"].clone();
    let denied = failure(
        machine
            .take(&mut fixture.context(&mut store), &id(1), &id(3))
            .await,
    );
    assert_eq!(normalize(denied.report), source["taken"]);
    assert_eq!(source["taken"]["reason"], "target_changed");
    assert_eq!(fixture.writes().len(), 1);
    assert_eq!(
        *fixture.trace.lock().unwrap(),
        source["trace"].as_array().unwrap().clone()
    );
    assert!(store.read(&id(3)).is_err());
}

#[tokio::test]
async fn equivalent_js_numbers_do_not_invalidate_a_stage_across_integer_float_and_negative_zero_forms()
 {
    let mut world = initial();
    world["setting"] = json!({"数":1,"zero":-0.0,"large":9_007_199_254_740_992_u64});
    world["afterStageSetting"] = json!({"large":9_007_199_254_740_993_u64,"zero":0.0,"数":1.0});
    let source = node_oracle(world.clone());
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let mut fixture = Fixture::new(world.clone());
    let mut machine = StageMachine::default();
    ok(machine
        .stage(&mut fixture.context(&mut store), target(), &id(1), &id(2))
        .await);
    fixture.world.lock().unwrap()["setting"] = world["afterStageSetting"].clone();
    let taken = ok(machine
        .take(&mut fixture.context(&mut store), &id(1), &id(3))
        .await);
    assert_eq!(normalize(taken.report), source["taken"]);
    assert_eq!(source["taken"]["status"], "verified");
    assert_eq!(
        *fixture.trace.lock().unwrap(),
        source["trace"].as_array().unwrap().clone()
    );
}
