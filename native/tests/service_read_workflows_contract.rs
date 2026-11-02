// SPDX-License-Identifier: GPL-2.0-only
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    clock,
    custody::Clock,
    scene_audit::{AuditOptions, ScreenshotLimit},
    scene_plan::PlanRequest,
    service_read_workflows::{self, WorkflowBounds},
    session::{ObsSession, SessionOptions},
    session_observation::{ObservationBridge, REQUIRED_AUDIT_EVENTS},
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::Notify,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const NOW: u64 = 1793448000000;
const WAIT: Duration = Duration::from_secs(3);
const PNG: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
fn clock() -> Clock {
    Arc::new(|| clock::from_epoch_duration(Duration::from_millis(NOW)).unwrap())
}
fn plan_args() -> Value {
    json!({"sceneCollection":"Audit collection","sceneName":"Review","template":{"version":1,"name":"Presentation","canvas":{"width":1920,"height":1080},"roles":[{"role":"slides","inputKinds":["screen_capture","xcomposite_input"],"transform":{"positionX":12},"enabled":true,"locked":true,"index":0}]},"bindings":[{"role":"slides","sourceName":"Application","sourceUuid":"application-source-uuid","containerUuid":"review-scene-uuid","sceneItemId":7}]})
}
fn audit_args(screenshot: bool) -> Value {
    json!({"sceneCollection":"Audit collection","sceneName":"Review","requiredItems":[{"sourceName":"Application"}],"includeScreenshot":screenshot})
}
fn replies() -> Value {
    json!({
        "GetSceneCollectionList":[{"currentSceneCollectionName":"Audit collection","sceneCollections":[{"sceneCollectionName":"Audit collection"}]}],
        "GetSceneList":[{"currentProgramSceneName":"Other program scene","scenes":[{"sceneName":"Review","sceneUuid":"review-scene-uuid"}]}],
        "GetVideoSettings":[{"baseWidth":1920,"baseHeight":1080,"outputWidth":1920,"outputHeight":1080,"fpsNumerator":30,"fpsDenominator":1}],
        "GetInputList":[{"inputs":[{"inputName":"Application","inputUuid":"application-source-uuid","inputKind":"screen_capture","unversionedInputKind":"screen_capture"}]}],
        "GetSceneItemList":[{"sceneItems":[{"sceneItemId":7,"sceneItemIndex":0,"sourceName":"Application","sourceUuid":"application-source-uuid","inputKind":"screen_capture","isGroup":false,"sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemTransform":{"positionX":0,"positionY":0,"scaleX":1,"scaleY":1,"rotation":0,"alignment":5,"boundsType":"OBS_BOUNDS_NONE","boundsWidth":0,"boundsHeight":0,"boundsAlignment":0,"cropLeft":0,"cropRight":0,"cropTop":0,"cropBottom":0}}]}],
        "GetSceneItemTransform":[{"sceneItemTransform":{"sourceWidth":1280,"sourceHeight":720,"width":1280,"height":720,"positionX":0,"positionY":0,"scaleX":1,"scaleY":1,"rotation":0,"alignment":5,"boundsType":"OBS_BOUNDS_NONE","boundsWidth":0,"boundsHeight":0,"boundsAlignment":0,"cropLeft":0,"cropRight":0,"cropTop":0,"cropBottom":0}}],
        "GetSourceScreenshot":[{"imageData":PNG}]
    })
}
struct Hooks {
    replies: Mutex<Value>,
    trace: Mutex<Vec<Value>>,
    events: Vec<String>,
    inject_at: String,
    hold: AtomicBool,
    entered: Notify,
    release: Notify,
    settled: AtomicBool,
    reject: bool,
}
impl Hooks {
    fn new(events: Vec<&str>, inject_at: &str) -> Self {
        Self {
            replies: Mutex::new(replies()),
            trace: Mutex::new(vec![]),
            events: events.into_iter().map(str::to_owned).collect(),
            inject_at: inject_at.into(),
            hold: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            settled: AtomicBool::new(false),
            reject: false,
        }
    }
}
struct Fixture {
    owner: ObsSession,
    bridge: Arc<ObservationBridge>,
    hooks: Arc<Hooks>,
    peer: JoinHandle<()>,
}
impl Fixture {
    async fn new(hooks: Hooks) -> Self {
        let hooks = Arc::new(hooks);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let state = hooks.clone();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            peer.send(Message::Text(
                json!({"op":0,"d":{"obsWebSocketVersion":"5.7.4","rpcVersion":1}})
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
            while let Some(message) = peer.next().await {
                let Ok(Message::Text(text)) = message else {
                    let _ = peer.flush().await;
                    break;
                };
                let frame: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(frame["op"], 6);
                let name = frame["d"]["requestType"].as_str().unwrap();
                let data = frame["d"]
                    .get("requestData")
                    .cloned()
                    .unwrap_or(Value::Null);
                state
                    .trace
                    .lock()
                    .unwrap()
                    .push(json!({"type":name,"data":data}));
                if state.hold.swap(false, Ordering::AcqRel) {
                    state.entered.notify_one();
                    state.release.notified().await;
                    state.settled.store(true, Ordering::Release);
                }
                if state.inject_at == name {
                    for event in &state.events {
                        peer.send(Message::Text(json!({"op":5,"d":{"eventType":event,"eventIntent":if event=="InputVolumeMeters"{1<<16}else if event=="SceneItemTransformChanged"{1<<19}else{8},"eventData":{}}}).to_string().into())).await.unwrap();
                    }
                }
                let reply = {
                    let mut replies = state.replies.lock().unwrap();
                    let queue = replies[name]
                        .as_array_mut()
                        .expect("qualified physical read");
                    if queue.len() > 1 {
                        queue.remove(0)
                    } else {
                        queue[0].clone()
                    }
                };
                let reject = state.reject && name == state.inject_at;
                if peer.send(Message::Text(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":name,"requestStatus":{"result":!reject,"code":if reject{500}else{100},"comment":"PRIVATE WORKFLOW REJECTION"},"responseData":reply}}).to_string().into())).await.is_err(){break;}
            }
        });
        let (owner, events) = ObsSession::connect(
            &url,
            None,
            SessionOptions {
                event_subscriptions: REQUIRED_AUDIT_EVENTS | (1 << 16),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let bridge = Arc::new(ObservationBridge::bind(owner.handle(), events, None).unwrap());
        Self {
            owner,
            bridge,
            hooks,
            peer,
        }
    }
    async fn close(self) {
        let _ = self.owner.shutdown().await;
        Arc::try_unwrap(self.bridge)
            .ok()
            .expect("no workflow may extend owner")
            .shutdown()
            .await
            .unwrap();
        timeout(WAIT, self.peer).await.unwrap().unwrap();
    }
}
/// Reuse the actual SDK boundary and test fixture model used by existing
/// scene_plan_contract/scene_audit_contract, with fixed metadata and NO omitted
/// fields. JSON text blocks are parsed only to ignore object key ordering.
fn source(input: &Value) -> Value {
    let script = r#"
import {EventEmitter} from 'node:events';
import {McpServer,InMemoryTransport} from '@modelcontextprotocol/server';
import {Client} from '@modelcontextprotocol/client';
import {initialize as plan} from './build/tools/scene-template.js';
import {initialize as audit} from './build/tools/scene-audit.js';
const chunks=[];for await(const chunk of process.stdin)chunks.push(chunk);const input=JSON.parse(Buffer.concat(chunks).toString());
const RealDate=Date;globalThis.Date=class extends RealDate{constructor(...args){super(...(args.length?args:[1793448000000]));}static now(){return 1793448000000;}};
if(input.limit!=null)process.env.OBS_MCP_MAX_SCREENSHOT_BYTES=input.limit;else delete process.env.OBS_MCP_MAX_SCREENSHOT_BYTES;
const trace=[];class Reader extends EventEmitter{
connect(){return Promise.resolve()}
getConnectionStatus(){return {identified:true,generation:input.generation}}supportsRequest(type){return input.supported?input.supported.includes(type):true}isConnected(){return true}
async sendRequest(type,data){if(input.supported&&!input.supported.includes(type))throw Error(`OBS WebSocket does not advertise support for request '${type}'`);trace.push({type,data:data??null});if(type===input.injectAt)for(const event of input.events??[])this.emit('event',event);const queue=input.replies[type];if(!queue?.length)throw Error('Unexpected fixture read');return structuredClone(queue.length>1?queue.shift():queue[0]);}}
const reader=new Reader(),server=new McpServer({name:'service-read-oracle',version:'1'});(input.kind==='plan'?plan:audit)(server,reader);
const client=new Client({name:'read-consumer',version:'1'}),[a,b]=InMemoryTransport.createLinkedPair();
try{await Promise.all([server.connect(a),client.connect(b)]);const result=await client.callTool({name:input.kind==='plan'?'obs-plan-scene-template':'obs-audit-scene',arguments:input.args});for(const block of result.content??[])if(block.type==='text')block.text=JSON.parse(block.text);console.log(JSON.stringify({result,trace}));}finally{await client.close();await server.close();}
"#;
    let mut child = Command::new("node")
        .args(["--input-type=module", "-e", script])
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(input).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "source SDK oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn semantic(mut value: Value) -> Value {
    for block in value["content"].as_array_mut().unwrap() {
        if block["type"] == "text" {
            block["text"] = serde_json::from_str(block["text"].as_str().unwrap()).unwrap();
        }
    }
    value
}
async fn compare(kind: &str, args: Value, hooks: Hooks, limit: Option<&str>) -> Value {
    let original = hooks.replies.lock().unwrap().clone();
    let events = hooks.events.clone();
    let inject = hooks.inject_at.clone();
    let fixture = Fixture::new(hooks).await;
    let expected = source(
        &json!({"kind":kind,"args":args,"replies":original,"generation":fixture.owner.token().generation(),"events":events,"injectAt":inject,"limit":limit}),
    );
    let result = if kind == "plan" {
        service_read_workflows::plan(
            fixture.bridge.clone(),
            PlanRequest::from_value(&args).unwrap(),
            clock(),
            Cancellation::new(),
            WorkflowBounds::default(),
        )
        .await
    } else {
        service_read_workflows::audit(
            fixture.bridge.clone(),
            AuditOptions::from_value(&args).unwrap(),
            clock(),
            Cancellation::new(),
            WorkflowBounds::default(),
            ScreenshotLimit::parse(limit),
        )
        .await
    }
    .unwrap();
    assert_eq!(semantic(result.clone()), expected["result"]);
    assert_eq!(
        *fixture.hooks.trace.lock().unwrap(),
        *expected["trace"].as_array().unwrap()
    );
    fixture.close().await;
    result
}

#[tokio::test]
async fn complete_plan_and_audit_tool_results_and_ordered_reads_match_actual_sdk() {
    let plan = compare("plan", plan_args(), Hooks::new(vec![], ""), None).await;
    assert_eq!(plan["structuredContent"]["status"], "resolved");
    let audit = compare("audit", audit_args(false), Hooks::new(vec![], ""), None).await;
    assert_eq!(audit["structuredContent"]["status"], "pass");
}

#[tokio::test]
async fn narrow_audit_ignores_record_and_meter_events_while_plan_uses_canonical_profile() {
    let audit = compare(
        "audit",
        audit_args(false),
        Hooks::new(
            vec!["RecordStateChanged", "InputVolumeMeters"],
            "GetInputList",
        ),
        None,
    )
    .await;
    assert_eq!(audit["structuredContent"]["status"], "pass");
    let plan = compare(
        "plan",
        plan_args(),
        Hooks::new(vec!["InputVolumeMeters"], "GetInputList"),
        None,
    )
    .await;
    assert_eq!(plan["structuredContent"]["status"], "resolved");
    let plan = compare(
        "plan",
        plan_args(),
        Hooks::new(vec!["RecordStateChanged"], "GetInputList"),
        None,
    )
    .await;
    assert_eq!(plan["structuredContent"]["status"], "unknown");
}

#[tokio::test]
async fn queued_structural_changes_produce_source_exact_unknown_without_mutations() {
    for kind in ["plan", "audit"] {
        let result = compare(
            kind,
            if kind == "plan" {
                plan_args()
            } else {
                audit_args(false)
            },
            Hooks::new(vec!["SceneItemTransformChanged"], "GetInputList"),
            None,
        )
        .await;
        assert_eq!(result["structuredContent"]["status"], "unknown");
    }
}

#[tokio::test]
async fn screenshots_preserve_source_width_content_and_exact_byte_limit_reports() {
    for limit in [None, Some("2"), Some(" 4")] {
        let result = compare("audit", audit_args(true), Hooks::new(vec![], ""), limit).await;
        assert_eq!(
            result["content"].as_array().unwrap().len(),
            if limit.is_none() { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn frontend_timeout_retains_work_and_request_cancellation_drains_the_admitted_read() {
    for cancel in [false, true] {
        let hooks = Hooks::new(vec![], "");
        hooks.hold.store(true, Ordering::Release);
        let fixture = Fixture::new(hooks).await;
        let cancellation = Cancellation::new();
        let work = service_read_workflows::audit(
            fixture.bridge.clone(),
            AuditOptions::from_value(&audit_args(false)).unwrap(),
            clock(),
            cancellation.clone(),
            WorkflowBounds::default(),
            ScreenshotLimit::parse(None),
        );
        tokio::pin!(work);
        assert!(poll!(work.as_mut()).is_pending());
        fixture.hooks.entered.notified().await;
        assert!(
            timeout(Duration::from_millis(10), work.as_mut())
                .await
                .is_err()
        );
        if cancel {
            cancellation.cancel();
        }
        assert!(!fixture.hooks.settled.load(Ordering::Acquire));
        fixture.hooks.release.notify_one();
        let result = work.await;
        assert!(fixture.hooks.settled.load(Ordering::Acquire));
        if cancel {
            assert_eq!(result.unwrap_err().reason, "cancelled");
            assert_eq!(fixture.hooks.trace.lock().unwrap().len(), 1);
        } else {
            assert_eq!(result.unwrap()["structuredContent"]["status"], "pass");
        }
        fixture.close().await;
    }
}

#[tokio::test]
async fn unsupported_unicode_peer_errors_and_expired_work_refuse_explicitly() {
    for kind in ["plan", "audit"] {
        let hooks = Hooks::new(vec![], "");
        hooks.replies.lock().unwrap()["GetSceneList"][0]["scenes"][0]["évidence"] = json!(true);
        let fixture = Fixture::new(hooks).await;
        let result = if kind == "plan" {
            service_read_workflows::plan(
                fixture.bridge.clone(),
                PlanRequest::from_value(&plan_args()).unwrap(),
                clock(),
                Cancellation::new(),
                WorkflowBounds::default(),
            )
            .await
        } else {
            service_read_workflows::audit(
                fixture.bridge.clone(),
                AuditOptions::from_value(&audit_args(false)).unwrap(),
                clock(),
                Cancellation::new(),
                WorkflowBounds::default(),
                ScreenshotLimit::parse(None),
            )
            .await
        };
        assert_eq!(result.unwrap_err().reason, "native_projection_unavailable");
        fixture.close().await;
    }
    let mut hooks = Hooks::new(vec![], "GetInputList");
    hooks.reject = true;
    let fixture = Fixture::new(hooks).await;
    let error = service_read_workflows::audit(
        fixture.bridge.clone(),
        AuditOptions::from_value(&audit_args(false)).unwrap(),
        clock(),
        Cancellation::new(),
        WorkflowBounds::default(),
        ScreenshotLimit::parse(None),
    )
    .await
    .unwrap_err();
    assert_eq!(error.request_code(), Some(500));
    assert!(error.message.contains("PRIVATE WORKFLOW REJECTION"));
    assert!(!format!("{error:?}").contains("PRIVATE WORKFLOW"));
    let count = fixture.hooks.trace.lock().unwrap().len();
    let error = service_read_workflows::plan(
        fixture.bridge.clone(),
        PlanRequest::from_value(&plan_args()).unwrap(),
        clock(),
        Cancellation::new(),
        WorkflowBounds {
            deadline: Instant::now() - Duration::from_millis(1),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.reason, "observation_limit");
    assert_eq!(fixture.hooks.trace.lock().unwrap().len(), count);
    fixture.close().await;
}

#[tokio::test]
async fn admitted_rpc_deadline_is_bounded_and_cannot_publish_late_evidence_or_replay() {
    for kind in ["plan", "audit"] {
        let hooks = Hooks::new(vec![], "");
        hooks.hold.store(true, Ordering::Release);
        let fixture = Fixture::new(hooks).await;
        let bounds = WorkflowBounds {
            deadline: Instant::now() + Duration::from_millis(30),
        };
        let result = if kind == "plan" {
            service_read_workflows::plan(
                fixture.bridge.clone(),
                PlanRequest::from_value(&plan_args()).unwrap(),
                clock(),
                Cancellation::new(),
                bounds,
            )
            .await
        } else {
            service_read_workflows::audit(
                fixture.bridge.clone(),
                AuditOptions::from_value(&audit_args(false)).unwrap(),
                clock(),
                Cancellation::new(),
                bounds,
                ScreenshotLimit::parse(None),
            )
            .await
        };
        assert_eq!(result.unwrap()["structuredContent"]["status"], "unknown");
        assert_eq!(fixture.hooks.trace.lock().unwrap().len(), 1);
        assert!(!fixture.hooks.settled.load(Ordering::Acquire));
        // The returned unknown result is not a socket-shutdown receipt. Its
        // service-owned actor separately publishes terminal state and is joined
        // below; no replay or later evidence becomes workflow authority.
        timeout(WAIT, async {
            while fixture.owner.handle().invalidation().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        fixture.hooks.release.notify_one();
        fixture.close().await;
    }
}

#[tokio::test]
async fn canceled_admission_and_inconsistent_wall_clock_refuse_without_rpc() {
    let fixture = Fixture::new(Hooks::new(vec![], "")).await;
    let cancellation = Cancellation::new();
    cancellation.cancel();
    let error = service_read_workflows::audit(
        fixture.bridge.clone(),
        AuditOptions::from_value(&audit_args(false)).unwrap(),
        clock(),
        cancellation,
        WorkflowBounds::default(),
        ScreenshotLimit::parse(None),
    )
    .await
    .unwrap_err();
    assert_eq!(error.reason, "cancelled");
    let bad_clock: Clock = Arc::new(|| obs_mcp_native::custody::ClockSnapshot {
        epoch_ms: NOW,
        utc: "1970-01-01T00:00:00.000Z".into(),
    });
    let error = service_read_workflows::plan(
        fixture.bridge.clone(),
        PlanRequest::from_value(&plan_args()).unwrap(),
        bad_clock,
        Cancellation::new(),
        WorkflowBounds::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.reason, "clock_unavailable");
    assert!(fixture.hooks.trace.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn same_connection_capability_overlay_refuses_unadvertised_reads_without_dispatch() {
    for kind in ["plan", "audit"] {
        let hooks = Hooks::new(vec![], "");
        let original = hooks.replies.lock().unwrap().clone();
        let supported = original
            .as_object()
            .unwrap()
            .keys()
            .filter(|name| name.as_str() != "GetVideoSettings")
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let fixture = Fixture::new(hooks).await;
        let args = if kind == "plan" {
            plan_args()
        } else {
            audit_args(false)
        };
        let expected = source(
            &json!({"kind":kind,"args":args,"replies":original,"generation":fixture.owner.token().generation(),"events":[],"injectAt":"","supported":supported}),
        );
        let result = if kind == "plan" {
            service_read_workflows::plan_with_capabilities(
                fixture.bridge.clone(),
                PlanRequest::from_value(&args).unwrap(),
                clock(),
                Cancellation::new(),
                WorkflowBounds::default(),
                Some(Arc::new(supported)),
            )
            .await
        } else {
            service_read_workflows::audit_with_capabilities(
                fixture.bridge.clone(),
                AuditOptions::from_value(&args).unwrap(),
                clock(),
                Cancellation::new(),
                WorkflowBounds::default(),
                ScreenshotLimit::parse(None),
                Some(Arc::new(supported)),
            )
            .await
        }
        .unwrap();
        assert_eq!(semantic(result), expected["result"]);
        assert_eq!(
            *fixture.hooks.trace.lock().unwrap(),
            *expected["trace"].as_array().unwrap()
        );
        assert!(
            !fixture
                .hooks
                .trace
                .lock()
                .unwrap()
                .iter()
                .any(|read| read["type"] == "GetVideoSettings")
        );
        fixture.close().await;
    }
}
