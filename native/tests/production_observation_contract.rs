// SPDX-License-Identifier: GPL-2.0-only
//! Full actual SDK tool/resource reports and ordered traces, frozen dates only.
use obs_mcp_native::{
    clock,
    custody::Clock,
    observation::{EvidenceState, ObservationError, ReadAdapter, ReadFuture, ReadRequest},
    production_observation::{self, Context, LocalState},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::PathBuf,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::watch, time::Instant};
const EPOCH: u64 = 1_793_448_000_000;
const NOW: &str = "2026-10-31T12:00:00.000Z";
const DONE: &str = "2026-10-31T12:00:01.000Z";
fn fixture() -> Value {
    json!({"GetSceneCollectionList":[{"currentSceneCollectionName":"Fixture"}],"GetSceneList":[{"currentProgramSceneName":"Main","currentProgramSceneUuid":"main","scenes":[{"sceneName":"Main","sceneUuid":"main"},{"sceneName":"Review","sceneUuid":"review"}]}],"GetStudioModeEnabled":[{"studioModeEnabled":true}],"GetCurrentPreviewScene":[{"currentPreviewSceneName":"Review","currentPreviewSceneUuid":"review"}],"GetVideoSettings":[{"baseWidth":1920,"baseHeight":1080}],"GetRecordStatus":[{"outputActive":false,"outputPaused":false}],"GetStreamStatus":[{"outputActive":false,"outputReconnecting":false}],"GetReplayBufferStatus":[{"outputActive":false}],"GetVirtualCamStatus":[{"outputActive":false}],"GetOutputList":[{"outputs":[{"outputName":"named"}]}],"GetOutputStatus":[{"outputActive":false,"outputReconnecting":false}],"GetInputList":[{"inputs":[]}],"GetStats":[{"availableDiskSpace":50000}],"GetProfileParameter":[{"parameterValue":"1"}],"GetCurrentProgramScene":[{"currentProgramSceneName":"Main"}]})
}
fn connection() -> Value {
    json!({"generation":7,"connected":true,"identified":true,"url":"ws://remote.example:4455","hasPassword":false,"availableRequestCount":null,"versionInfo":{"platform":"macos","supportedImageFormats":[]}})
}
fn local() -> Value {
    json!({"control":null,"activeTake":null,"recovery":{"status":"observed","operations":[]},"operationReceipts":{"status":"observed","operations":[],"total":0,"truncated":false}})
}
struct Provider {
    local: Arc<Mutex<Value>>,
    connection: Value,
}
impl LocalState for Provider {
    fn snapshot(&self) -> Value {
        self.local.lock().unwrap().clone()
    }
    fn connection(&self) -> Value {
        self.connection.clone()
    }
}
struct Reader {
    rows: HashMap<String, VecDeque<Value>>,
    trace: Vec<Value>,
    state: watch::Sender<EvidenceState>,
    local: Arc<Mutex<Value>>,
    options: Value,
}
impl ReadAdapter for Reader {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        Some(self.options["unsupported"] != request.as_str())
    }
    fn read<'a>(&'a mut self, request: ReadRequest, data: Value, _: Instant) -> ReadFuture<'a> {
        Box::pin(async move {
            self.trace
                .push(json!({"type":request.as_str(),"data":data}));
            let n = self.trace.len();
            if self.options["eventAt"] == n {
                self.state.send_modify(|state| state.revision += 1);
            }
            if self.options["localAt"] == n {
                self.local.lock().unwrap()["control"] = json!({"holder":"codex","reason":"changed","since":EPOCH,"expiresAt":EPOCH+60000});
            }
            let rows = self.rows.get_mut(request.as_str()).ok_or_else(|| {
                ObservationError::new("evidence_unavailable", "Unexpected fixture read")
            })?;
            let mut row = if rows.len() > 1 {
                rows.pop_front().unwrap()
            } else {
                rows.front().unwrap().clone()
            };
            if self.options["oversized"] == request.as_str() {
                row["oversized"] = json!("x".repeat(262145));
            }
            if let Some(code) = row["$error"].as_u64() {
                let comment = row["comment"]
                    .as_str()
                    .filter(|comment| !comment.is_empty())
                    .map(|comment| format!(": {comment}"))
                    .unwrap_or_default();
                return Err(ObservationError::rejected(
                    code,
                    if code == 604 {
                        "capability_unavailable"
                    } else {
                        "evidence_unavailable"
                    },
                    format!(
                        "OBS request {} failed with code {code}{comment}",
                        request.as_str()
                    ),
                ));
            }
            Ok(row)
        })
    }
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "observe-contract-{}-{}",
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
fn node(dir: &Dir, rows: &Value, options: &Value) -> Value {
    let output=Command::new("node").args(["--input-type=module","-e",r#"
import {EventEmitter} from 'node:events';import {McpServer,InMemoryTransport} from '@modelcontextprotocol/server';import {Client} from '@modelcontextprotocol/client';import {initialize} from './build/tools/observe.js';import {initialize as resources} from './build/tools/resources.js';import {claim} from './build/tools/lease.js';import {TakeMonitor} from './build/tools/take-monitor.js';import {OBSRequestError} from './build/client.js';
const input=JSON.parse(process.env.PRODUCTION_OBSERVE_TEST_INPUT),trace=[],RealDate=Date;const NOW=1793448000000,DONE=NOW+1000;let wall=NOW;
globalThis.Date=class extends RealDate{constructor(...args){super(...(args.length?args:[DONE]));}static now(){return wall;}};
class Reader extends EventEmitter {
 async connect(){if(input.options.offline)throw Error('fixture offline')}
 isConnected(){return true}
 getConnectionStatus(){return {generation:7,connected:true,identified:true,url:'ws://remote.example:4455',hasPassword:false,availableRequestCount:null,versionInfo:{platform:'macos',supportedImageFormats:[]}}}
 supportsRequest(type){return input.options.unsupported!==type}
 subscribeHighVolume(){return ()=>{}}
 async sendBatch(){return []}
 async sendRequest(type,data){trace.push({type,data:data??null});if(trace.length===input.options.eventAt)this.emit('event','SceneNameChanged',{});if(trace.length===input.options.localAt)claim(this,'codex',1,'changed');const rows=input.rows[type];if(!rows?.length)throw Error('Unexpected fixture read');const row=structuredClone(rows.length>1?rows.shift():rows[0]);if(input.options.durationMs&&trace.length===22)wall=NOW+input.options.durationMs;if(input.options.oversized===type)row.oversized="x".repeat(262145);if(row.$error)throw new OBSRequestError(type,row.$error,row.comment);return row;}
}
const reader=new Reader();if(input.options.lease)claim(reader,'codex',2,'observe');let monitor;
if(input.options.monitor){monitor=new TakeMonitor(reader,{expectSilent:true});await monitor.start();reader.emit('RecordStateChanged',{outputState:'OBS_WEBSOCKET_OUTPUT_PAUSED',outputActive:true,outputPaused:true});trace.length=0;}
const server=new McpServer({name:'observe-oracle',version:'1'});initialize(server,reader);resources(server,reader,{sessionNotifications:false});const client=new Client({name:'consumer',version:'1'});const[a,b]=InMemoryTransport.createLinkedPair();
try{await Promise.all([server.connect(a),client.connect(b)]);let report;if(input.options.resource){const result=await client.readResource({uri:'obs://status'});report=JSON.parse(result.contents[0].text);}else{const result=await client.callTool({name:'obs-observe',arguments:input.options.canvas?{canvasUuid:input.options.canvas}:{}});report=result.structuredContent;}console.log(JSON.stringify({report,trace}));}finally{if(monitor)await monitor.stop();await client.close();await server.close();}
"#]).current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap()).env("OBS_MCP_STATE_DIR",&dir.0).env("PRODUCTION_OBSERVE_TEST_INPUT",json!({"rows":rows,"options":options}).to_string()).output().unwrap();
    assert!(
        output.status.success(),
        "source oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
async fn run(rows: Value, options: Value) -> Value {
    let mut local = local();
    if options["lease"] == true {
        local["control"] =
            json!({"holder":"codex","reason":"observe","since":EPOCH,"expiresAt":EPOCH+120000});
    }
    if options["monitor"] == true {
        local["activeTake"] = json!({"id":null,"running":true,"startedAt":NOW,"expectSilent":true,"locked":null,"maxDurationSeconds":null,"ownershipLost":null});
    }
    let local = Arc::new(Mutex::new(local));
    let provider = Provider {
        local: local.clone(),
        connection: connection(),
    };
    let (state, states) = watch::channel(EvidenceState {
        generation: 7,
        revision: 0,
        connected: true,
        cancelled: false,
    });
    let mut reader = Reader {
        rows: rows
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, rows)| (key.clone(), rows.as_array().unwrap().clone().into()))
            .collect(),
        trace: vec![],
        state,
        local,
        options: options.clone(),
    };
    let count = Arc::new(AtomicU64::new(0));
    let duration = options["durationMs"].as_u64().unwrap_or(0);
    let clock: Clock = Arc::new(move || {
        let mut snapshot = clock::from_epoch_duration(Duration::from_millis(EPOCH)).unwrap();
        if count.fetch_add(1, Ordering::Relaxed) >= 2 {
            snapshot.utc = DONE.into();
            snapshot.epoch_ms += duration;
        }
        snapshot
    });
    let report = production_observation::observe(
        &mut reader,
        Context {
            state: states,
            generation: 7,
            local: &provider,
            clock,
            deadline: Instant::now() + Duration::from_secs(12),
            started: None,
            connect_error: if options["offline"] == true {
                Some(ObservationError::new(
                    "evidence_unavailable",
                    "fixture offline",
                ))
            } else {
                None
            },
        },
        options["canvas"].as_str(),
    )
    .await;
    json!({"report":report,"trace":reader.trace})
}

#[tokio::test]
async fn complete_source_tool_and_resource_reports_match_for_idle_studio_and_paused_monitor() {
    for mode in ["studio", "idle", "resource", "paused", "lease", "duration"] {
        let mut rows = fixture();
        let mut options = json!({});
        if mode == "idle" {
            rows["GetStudioModeEnabled"] = json!([{"studioModeEnabled":false}]);
        }
        if mode == "resource" {
            options["resource"] = json!(true);
        }
        if mode == "lease" {
            options["lease"] = json!(true);
        }
        if mode == "duration" {
            options["durationMs"] = json!(7);
        }
        if mode == "paused" {
            options["monitor"] = json!(true);
            rows["GetRecordStatus"] = json!([{"outputActive":true,"outputPaused":true}]);
        }
        let dir = Dir::new();
        assert_eq!(
            run(rows.clone(), options.clone()).await,
            node(&dir, &rows, &options),
            "{mode}"
        );
    }
}

#[tokio::test]
async fn optional_rejections_malformed_evidence_and_missing_studio_match_full_source_reports() {
    for mode in ["604", "comment", "output", "studio", "unsupported"] {
        let mut rows = fixture();
        let mut options = json!({});
        match mode {
            "604" => rows["GetReplayBufferStatus"] = json!([{"$error":604}]),
            "comment" => {
                rows["GetVirtualCamStatus"] =
                    json!([{"$error":500,"comment":"fixture unavailable"}])
            }
            "output" => rows["GetOutputStatus"] = json!([{"outputActive":"unknown"}]),
            "studio" => rows["GetStudioModeEnabled"] = json!([{}]),
            "unsupported" => options["unsupported"] = json!("GetStreamStatus"),
            _ => unreachable!(),
        };
        let dir = Dir::new();
        assert_eq!(
            run(rows.clone(), options.clone()).await,
            node(&dir, &rows, &options),
            "{mode}"
        );
    }
}

#[tokio::test]
async fn same_generation_and_local_changes_offline_and_default_canvas_refusal_match_source() {
    for mode in [
        "changed",
        "local",
        "event",
        "offline",
        "canvas",
        "identity",
        "dimensions",
        "inventory",
        "oversized",
    ] {
        let mut rows = fixture();
        let mut options = json!({});
        match mode {
            "changed" => {
                rows["GetRecordStatus"] = json!([{"outputActive":false},{"outputActive":true}])
            }
            "local" => options["localAt"] = json!(5),
            "event" => options["eventAt"] = json!(5),
            "offline" => options["offline"] = json!(true),
            "canvas" => options["canvas"] = json!("other"),
            "identity" => rows["GetSceneList"][0]["currentProgramSceneUuid"] = json!("replacement"),
            "dimensions" => rows["GetVideoSettings"][0]["baseWidth"] = json!(0),
            "oversized" => options["oversized"] = json!("GetVideoSettings"),
            "inventory" => {
                rows["GetOutputList"][0]["outputs"] = json!(
                    (0..33)
                        .map(|n| json!({"outputName":format!("output{n}")}))
                        .collect::<Vec<_>>()
                )
            }
            _ => unreachable!(),
        };
        let dir = Dir::new();
        assert_eq!(
            run(rows.clone(), options.clone()).await,
            node(&dir, &rows, &options),
            "{mode}"
        );
    }
}
