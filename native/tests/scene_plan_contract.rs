// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{
    observation::{
        self, EvidenceState, Observation, ObservationError, ReadAdapter, ReadFuture, ReadRequest,
        Target,
    },
    scene_plan::{PlanRequest, plan_report, same_transform_value},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    path::Path,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};

const NOW: &str = "2026-10-31T12:00:00.000Z";
const DONE: &str = "2026-10-31T12:00:01.000Z";
fn state() -> EvidenceState {
    EvidenceState {
        generation: 7,
        revision: 0,
        connected: true,
        cancelled: false,
    }
}
fn arguments() -> Value {
    json!({"sceneCollection":"Audit collection","sceneName":"Review","template":{"version":1,"name":"Presentation","canvas":{"width":1920,"height":1080},"roles":[{"role":"slides","inputKinds":["screen_capture","xcomposite_input"],"transform":{"positionX":12},"enabled":true,"locked":true,"index":0}]},"bindings":[{"role":"slides","sourceName":"Application","sourceUuid":"application-source-uuid","containerUuid":"review-scene-uuid","sceneItemId":7}]})
}
fn replies() -> Value {
    json!({
        "GetSceneCollectionList":[{"currentSceneCollectionName":"Audit collection","sceneCollections":[{"sceneCollectionName":"Audit collection"}]}],
        "GetSceneList":[{"currentProgramSceneName":"Other program scene","scenes":[{"sceneName":"Review","sceneUuid":"review-scene-uuid"}]}],
        "GetVideoSettings":[{"baseWidth":1920,"baseHeight":1080,"outputWidth":1920,"outputHeight":1080,"fpsNumerator":30,"fpsDenominator":1}],
        "GetInputList":[{"inputs":[{"inputName":"Application","inputUuid":"application-source-uuid","inputKind":"screen_capture","unversionedInputKind":"screen_capture"}]}],
        "GetSceneItemList":[{"sceneItems":[{"sceneItemId":7,"sceneItemIndex":0,"sourceName":"Application","sourceUuid":"application-source-uuid","inputKind":"screen_capture","isGroup":false,"sceneItemEnabled":true,"sceneItemLocked":false,"sceneItemTransform":{"positionX":0,"positionY":0,"scaleX":1,"scaleY":1,"rotation":0,"alignment":5,"boundsType":"OBS_BOUNDS_NONE","boundsWidth":0,"boundsHeight":0,"boundsAlignment":0,"cropLeft":0,"cropRight":0,"cropTop":0,"cropBottom":0}}]}],
        "GetSceneItemTransform":[{"sceneItemTransform":{"sourceWidth":1280,"sourceHeight":720,"width":1280,"height":720,"positionX":0,"positionY":0,"scaleX":1,"scaleY":1,"rotation":0,"alignment":5,"boundsType":"OBS_BOUNDS_NONE","boundsWidth":0,"boundsHeight":0,"boundsAlignment":0,"cropLeft":0,"cropRight":0,"cropTop":0,"cropBottom":0}}]
    })
}
type Trace = Arc<Mutex<Vec<Value>>>;
struct Reader {
    replies: HashMap<String, VecDeque<Value>>,
    trace: Trace,
    unavailable: Option<ReadRequest>,
    delay: Duration,
    entered: Arc<Notify>,
    settled: Arc<AtomicBool>,
}
impl Reader {
    fn new(replies: Value) -> Self {
        Self {
            replies: replies
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, values)| (name.clone(), values.as_array().unwrap().clone().into()))
                .collect(),
            trace: Arc::new(Mutex::new(vec![])),
            unavailable: None,
            delay: Duration::ZERO,
            entered: Arc::new(Notify::new()),
            settled: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl ReadAdapter for Reader {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.unavailable.map(|unavailable| unavailable != request)
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
            self.entered.notify_one();
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.settled.store(true, Ordering::Release);
            let queue = self.replies.get_mut(request.as_str()).ok_or_else(|| {
                ObservationError::new("evidence_unavailable", "Unexpected fixture read")
            })?;
            if queue.len() > 1 {
                Ok(queue.pop_front().unwrap())
            } else {
                queue.front().cloned().ok_or_else(|| {
                    ObservationError::new("evidence_unavailable", "Missing fixture evidence")
                })
            }
        })
    }
}
fn node(body: &str, input: &Value) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let script = format!("const input=JSON.parse(process.env.SCENE_PLAN_TEST_INPUT);\n{body}");
    let output = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(script)
        .current_dir(root)
        .env("SCENE_PLAN_TEST_INPUT", input.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "isolated TypeScript scene oracle failed"
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn node_plan(args: &Value, replies: &Value) -> Value {
    node(
        r#"
import {EventEmitter} from "node:events";
import {McpServer,InMemoryTransport} from "@modelcontextprotocol/server";
import {Client} from "@modelcontextprotocol/client";
import {initialize} from "./build/tools/scene-template.js";
const trace=[];
class Reader extends EventEmitter {
  connect(){return Promise.resolve()}
  isConnected(){return true}
  getConnectionStatus(){return {generation:7}}
  supportsRequest(){return true}
  async sendRequest(type,data){trace.push({type,data:data??null});const queue=input.replies[type];if(!queue?.length)throw Error("Unexpected fixture read");return structuredClone(queue.length>1?queue.shift():queue[0]);}
}
const server=new McpServer({name:"scene-oracle",version:"1"});initialize(server,new Reader());
const client=new Client({name:"scene-consumer",version:"1"});const [a,b]=InMemoryTransport.createLinkedPair();
try {await Promise.all([server.connect(a),client.connect(b)]);const result=await client.callTool({name:"obs-plan-scene-template",arguments:input.args});const report=result.structuredContent;delete report.observedAt;delete report.completedAt;console.log(JSON.stringify({report,trace}));}
finally {await client.close();await server.close();}
"#,
        &json!({"args":args,"replies":replies}),
    )
}
async fn run(args: &Value, replies: Value) -> (Value, Vec<Value>) {
    let mut reader = Reader::new(replies);
    let trace = reader.trace.clone();
    let (_provider, states) = watch::channel(state());
    let mut sample = Observation::new(
        &mut reader,
        states,
        7,
        NOW.to_owned(),
        Instant::now() + Duration::from_secs(12),
    )
    .unwrap();
    let report = plan_report(&mut sample, &PlanRequest::from_value(args).unwrap(), || {
        DONE.to_owned()
    })
    .await;
    (report, trace.lock().unwrap().clone())
}
fn without_times(mut report: Value) -> Value {
    let object = report.as_object_mut().unwrap();
    object.remove("observedAt");
    object.remove("completedAt");
    report
}

#[tokio::test]
async fn exact_bindings_resolve_to_a_read_only_plan_with_ordered_rereads_matching_source() {
    let args = arguments();
    let fixture = replies();
    let (report, trace) = run(&args, fixture.clone()).await;
    assert_eq!(report["status"], "resolved");
    assert_eq!(report["applied"], false);
    assert_eq!(report["renderedContent"], "unverified");
    assert_eq!(
        report["changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|change| change["field"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["sceneItemLocked", "transform.positionX"]
    );
    assert_eq!(
        trace
            .iter()
            .map(|read| read["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "GetSceneCollectionList",
            "GetSceneList",
            "GetVideoSettings",
            "GetInputList",
            "GetSceneItemList",
            "GetSceneItemTransform",
            "GetSceneItemTransform",
            "GetSceneCollectionList",
            "GetSceneList",
            "GetVideoSettings",
            "GetInputList",
            "GetSceneItemList"
        ]
    );
    assert!(
        trace
            .iter()
            .all(|read| read["type"].as_str().unwrap().starts_with("Get"))
    );
    assert_eq!(
        node_plan(&args, &fixture),
        json!({"report":without_times(report),"trace":trace})
    );
}

#[tokio::test]
async fn independently_expected_binding_blockers_and_traces_match_source() {
    for (variant, reason) in [
        ("rename", "target_renamed"),
        ("replacement", "target_replaced"),
        ("canvas", "canvas_mismatch"),
        ("kind", "incompatible_kind"),
        ("duplicate", "duplicate_binding"),
        ("missing", "missing_binding"),
        ("container", "container_mismatch"),
        ("nested", "nested_layout"),
        ("unknown-role", "unknown_role"),
        ("duplicate-role", "duplicate_role"),
        ("order", "invalid_order"),
        ("transform-field", "evidence_unavailable"),
    ] {
        let mut args = arguments();
        let mut fixture = replies();
        match variant {
            "rename" => {
                fixture["GetSceneItemList"][0]["sceneItems"][0]["sourceName"] = json!("Renamed")
            }
            "replacement" => {
                fixture["GetSceneItemList"][0]["sceneItems"][0]["sourceUuid"] = json!("replacement")
            }
            "canvas" => args["template"]["canvas"]["width"] = json!(1280),
            "kind" => {
                fixture["GetInputList"][0]["inputs"][0]["inputKind"] = json!("browser_source")
            }
            "duplicate" => {
                let binding = args["bindings"][0].clone();
                args["bindings"].as_array_mut().unwrap().push(binding);
            }
            "missing" => args["bindings"] = json!([]),
            "container" => args["bindings"][0]["containerUuid"] = json!("another-scene"),
            "nested" => fixture["GetSceneItemList"][0]["sceneItems"][0]["isGroup"] = json!(true),
            "unknown-role" => {
                let mut binding = args["bindings"][0].clone();
                binding["role"] = json!("absent");
                binding["sceneItemId"] = json!(8);
                args["bindings"].as_array_mut().unwrap().push(binding);
            }
            "duplicate-role" => {
                let role = args["template"]["roles"][0].clone();
                args["template"]["roles"].as_array_mut().unwrap().push(role);
            }
            "order" => args["template"]["roles"][0]["index"] = json!(1),
            _ => {
                fixture["GetSceneItemTransform"][0]["sceneItemTransform"]
                    .as_object_mut()
                    .unwrap()
                    .remove("positionX");
            }
        }
        let (report, trace) = run(&args, fixture.clone()).await;
        assert_eq!(report["status"], "blocked", "{variant}");
        assert!(
            report["blockers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|blocker| blocker["reason"] == reason),
            "{variant}"
        );
        assert_eq!(
            node_plan(&args, &fixture),
            json!({"report":without_times(report),"trace":trace}),
            "{variant}"
        );
    }
}

#[tokio::test]
async fn source_identity_changes_and_final_rereads_produce_unknown_without_proposed_changes() {
    for variant in [
        "transform",
        "inventory",
        "collection",
        "duplicate-scenes",
        "ambiguous",
        "duplicate-items",
        "null-group",
    ] {
        let args = arguments();
        let mut fixture = replies();
        match variant {
            "transform" => {
                let mut after = fixture["GetSceneItemTransform"][0].clone();
                after["sceneItemTransform"]["positionX"] = json!(1);
                fixture["GetSceneItemTransform"]
                    .as_array_mut()
                    .unwrap()
                    .push(after);
            }
            "inventory" => {
                let mut after = fixture["GetSceneItemList"][0].clone();
                after["sceneItems"][0]["sceneItemLocked"] = json!(true);
                fixture["GetSceneItemList"]
                    .as_array_mut()
                    .unwrap()
                    .push(after);
            }
            "collection" => {
                fixture["GetSceneCollectionList"][0]["currentSceneCollectionName"] =
                    json!("another collection")
            }
            "duplicate-scenes" => {
                let mut scene = fixture["GetSceneList"][0]["scenes"][0].clone();
                scene["sceneName"] = json!("Other");
                fixture["GetSceneList"][0]["scenes"]
                    .as_array_mut()
                    .unwrap()
                    .push(scene);
            }
            "ambiguous" => {
                let mut scene = fixture["GetSceneList"][0]["scenes"][0].clone();
                scene["sceneUuid"] = json!("Other");
                fixture["GetSceneList"][0]["scenes"]
                    .as_array_mut()
                    .unwrap()
                    .push(scene);
            }
            "duplicate-items" => {
                let item = fixture["GetSceneItemList"][0]["sceneItems"][0].clone();
                fixture["GetSceneItemList"][0]["sceneItems"]
                    .as_array_mut()
                    .unwrap()
                    .push(item);
            }
            _ => fixture["GetSceneItemList"][0]["sceneItems"][0]["isGroup"] = Value::Null,
        }
        let (report, trace) = run(&args, fixture.clone()).await;
        assert_eq!(report["status"], "unknown", "{variant}");
        assert_eq!(report["changes"], json!([]));
        assert_eq!(
            node_plan(&args, &fixture),
            json!({"report":without_times(report),"trace":trace}),
            "{variant}"
        );
    }
}

#[tokio::test]
async fn explicit_input_null_group_normalizes_but_named_canvas_never_dispatches() {
    let mut args = arguments();
    let mut fixture = replies();
    fixture["GetSceneItemList"][0]["sceneItems"][0]["isGroup"] = Value::Null;
    fixture["GetSceneItemList"][0]["sceneItems"][0]["sourceType"] = json!("OBS_SOURCE_TYPE_INPUT");
    let (report, trace) = run(&args, fixture.clone()).await;
    assert_eq!(report["status"], "resolved");
    assert_eq!(
        node_plan(&args, &fixture),
        json!({"report":without_times(report),"trace":trace})
    );
    args["canvasUuid"] = json!("another-canvas");
    let (report, trace) = run(&args, fixture.clone()).await;
    assert_eq!(report["status"], "unknown");
    assert!(trace.is_empty());
    assert_eq!(report["blockers"][0]["reason"], "capability_unavailable");
    assert_eq!(
        node_plan(&args, &fixture),
        json!({"report":without_times(report),"trace":trace})
    );
}

#[test]
fn transforms_match_actual_source_half_pixel_f32_and_recursive_equality() {
    let cases = json!([
        ["positionX",0,0.24],["positionX",0,0.25],["positionY",0,-0.25],["positionY",-0.26,-0.5],
        ["positionX",204,204.5],["positionX",0,0.24999999999999997],
        ["scaleX",100.1,100.09999847],["rotation",1,1.0000001],["rotation",1,1.00001],
        ["scaleY",1e308,1e307],["boundsType","OBS_BOUNDS_NONE","OBS_BOUNDS_NONE"],
        ["other",{"a":[1.0,true]},{"a":[1.0000001,true]}]
    ]);
    let expected = node(
        "import {sameTransformValue} from './build/tools/snapshots.js';console.log(JSON.stringify(input.map(([field,a,b])=>sameTransformValue(field,a,b))));",
        &cases,
    );
    let actual = cases
        .as_array()
        .unwrap()
        .iter()
        .map(|case| same_transform_value(case[0].as_str().unwrap(), &case[1], &case[2]))
        .collect::<Vec<_>>();
    assert_eq!(json!(actual), expected);
}

#[test]
fn signatures_and_digests_match_source_for_verified_keys_number_forms_and_integer_key_order() {
    let values = json!([
        {"a":1.0,"A":2,"_a":3,"a_b":4,"ab":5,"Z":6,"z":7},
        {"10":"ten","2":"two","01":"leading","4294967294":"index","4294967295":"not-index","":"empty"},
        {"nested":{"value":"🧪\n\u{0}","array":[-0.0,1.0,1e-6,1e-7,1e20,1e21,5e-324,1.7976931348623157e308,1.2345678901234567,1000000000000000100.0]}}
    ]);
    let expected = node(
        "import {signature,digest} from './build/tools/observation.js';console.log(JSON.stringify(input.map(value=>({signature:signature(value),digest:digest(value)}))));",
        &values,
    );
    let actual=values.as_array().unwrap().iter().map(|value|json!({"signature":observation::signature(value).unwrap(),"digest":observation::digest(value).unwrap()})).collect::<Vec<_>>();
    assert_eq!(json!(actual), expected);
    for unsupported in [json!({"é":1}), json!({"a.b":1})] {
        assert_eq!(
            observation::signature(&unsupported).unwrap_err().reason,
            "evidence_unavailable"
        );
    }
}

#[test]
fn canonical_number_rendering_survives_a_deterministic_source_oracle_corpus() {
    let mut bits = 0x215c_a7e9_103b_6d8fu64;
    let mut numbers = Vec::new();
    for _ in 0..1_024 {
        bits = bits
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let number = f64::from_bits(bits);
        if number.is_finite() {
            numbers.push(json!(number));
        }
    }
    let input = json!(numbers);
    let expected = node(
        "import {signature} from './build/tools/observation.js';console.log(JSON.stringify(input.map(signature)));",
        &input,
    );
    let actual = input
        .as_array()
        .unwrap()
        .iter()
        .map(|value| observation::signature(value).unwrap())
        .collect::<Vec<_>>();
    let expected = expected.as_array().unwrap();
    assert_eq!(actual.len(), expected.len());
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        assert_eq!(
            actual,
            expected.as_str().unwrap(),
            "number corpus entry {index}"
        );
    }
}

#[tokio::test]
async fn source_kind_coercion_and_duplicate_numeric_identity_are_not_reinterpreted() {
    let mut args = arguments();
    let mut fixture = replies();
    args["template"]["roles"][0]["inputKinds"] = json!(["5"]);
    fixture["GetInputList"][0]["inputs"][0]["inputKind"] = json!(5);
    let (report, trace) = run(&args, fixture.clone()).await;
    assert_eq!(report["status"], "resolved");
    assert_eq!(
        node_plan(&args, &fixture),
        json!({"report":without_times(report),"trace":trace})
    );
    let args = arguments();
    let mut fixture = replies();
    let mut first = fixture["GetSceneItemList"][0]["sceneItems"][0].clone();
    first["sceneItemId"] = json!(-0.0);
    let mut second = first.clone();
    second["sceneItemId"] = json!(0);
    fixture["GetSceneItemList"][0]["sceneItems"] = json!([first, second]);
    let (report, trace) = run(&args, fixture.clone()).await;
    assert_eq!(
        report["blockers"][0]["message"],
        "Duplicate scene item identities"
    );
    assert_eq!(
        node_plan(&args, &fixture),
        json!({"report":without_times(report),"trace":trace})
    );
}

#[tokio::test]
async fn observation_limits_capability_identity_and_provider_lifetime_fence_admission() {
    let mut reader = Reader::new(json!({"GetSceneList":[{}]}));
    let trace = reader.trace.clone();
    let (provider, states) = watch::channel(state());
    let mut sample = Observation::new(
        &mut reader,
        states,
        7,
        NOW.to_owned(),
        Instant::now() + Duration::from_secs(12),
    )
    .unwrap();
    for _ in 0..observation::MAX_REQUESTS {
        sample
            .read(ReadRequest::SceneList, Value::Null)
            .await
            .unwrap();
    }
    assert_eq!(
        sample
            .read(ReadRequest::SceneList, Value::Null)
            .await
            .unwrap_err()
            .reason,
        "observation_limit"
    );
    assert_eq!(trace.lock().unwrap().len(), 256);
    sample.close();
    assert_eq!(
        sample.check().unwrap_err().reason,
        "observation_invalidated"
    );
    drop(sample);
    let states = provider.subscribe();
    drop(provider);
    assert!(
        matches!(Observation::new(&mut reader,states,7,NOW.to_owned(),Instant::now()+Duration::from_secs(12)),Err(error) if error.reason=="observation_invalidated")
    );
    assert!(observation::identity(&json!("🙂".repeat(128))).is_ok());
    assert!(observation::identity(&json!("🙂".repeat(129))).is_err());
    let mut reader = Reader::new(replies());
    reader.unavailable = Some(ReadRequest::VideoSettings);
    let trace = reader.trace.clone();
    let (_provider, states) = watch::channel(state());
    let mut sample = Observation::new(
        &mut reader,
        states,
        7,
        NOW.to_owned(),
        Instant::now() + Duration::from_secs(12),
    )
    .unwrap();
    assert_eq!(
        sample
            .read(ReadRequest::VideoSettings, Value::Null)
            .await
            .unwrap_err()
            .reason,
        "capability_unavailable"
    );
    assert!(trace.lock().unwrap().is_empty());
}

#[tokio::test]
async fn revisions_and_cancellation_drain_admitted_reads_then_reject_without_more_reads() {
    for cancel in [false, true] {
        let mut reader = Reader::new(json!({"GetSceneList":[{}]}));
        reader.delay = Duration::from_millis(15);
        let entered = reader.entered.clone();
        let settled = reader.settled.clone();
        let trace = reader.trace.clone();
        let (provider, states) = watch::channel(state());
        let mut sample = Observation::new(
            &mut reader,
            states,
            7,
            NOW.to_owned(),
            Instant::now() + Duration::from_secs(12),
        )
        .unwrap();
        let change = async {
            entered.notified().await;
            assert!(!settled.load(Ordering::Acquire));
            let mut changed = state();
            if cancel {
                changed.cancelled = true;
            } else {
                changed.revision = 1;
            }
            provider.send_replace(changed);
        };
        let (result, ()) = tokio::join!(sample.read(ReadRequest::SceneList, Value::Null), change);
        assert!(
            settled.load(Ordering::Acquire),
            "invalidation must not abort a healthy admitted RPC"
        );
        assert_eq!(
            result.unwrap_err().reason,
            if cancel {
                "cancelled"
            } else {
                "observation_invalidated"
            }
        );
        assert!(
            sample
                .read(ReadRequest::SceneList, Value::Null)
                .await
                .is_err()
        );
        assert_eq!(trace.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn deadline_and_closed_provider_discard_unsettled_evidence_without_further_reads() {
    for provider_closed in [false, true] {
        let mut reader = Reader::new(json!({"GetSceneList":[{}]}));
        reader.delay = Duration::from_millis(50);
        let entered = reader.entered.clone();
        let settled = reader.settled.clone();
        let trace = reader.trace.clone();
        let (provider, states) = watch::channel(state());
        let mut sample = Observation::new(
            &mut reader,
            states,
            7,
            NOW.to_owned(),
            Instant::now()
                + if provider_closed {
                    Duration::from_secs(12)
                } else {
                    Duration::from_millis(5)
                },
        )
        .unwrap();
        let change = async {
            entered.notified().await;
            if provider_closed {
                drop(provider);
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                drop(provider);
            }
        };
        let (result, ()) = tokio::join!(sample.read(ReadRequest::SceneList, Value::Null), change);
        assert_eq!(
            result.unwrap_err().reason,
            if provider_closed {
                "observation_invalidated"
            } else {
                "observation_limit"
            }
        );
        assert!(!settled.load(Ordering::Acquire));
        assert_eq!(trace.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn byte_and_inventory_limits_are_unknown_and_screenshots_keep_the_explicit_exception() {
    for excess in [false, true] {
        let data = "x".repeat(observation::MAX_EVIDENCE_BYTES - 11 + usize::from(excess));
        let mut reader = Reader::new(
            json!({"GetSceneList":[{"data":data}],"GetSourceScreenshot":[{"data":"x".repeat(observation::MAX_EVIDENCE_BYTES)}]}),
        );
        let (_provider, states) = watch::channel(state());
        let mut sample = Observation::new(
            &mut reader,
            states,
            7,
            NOW.to_owned(),
            Instant::now() + Duration::from_secs(12),
        )
        .unwrap();
        let result = sample.read(ReadRequest::SceneList, Value::Null).await;
        assert_eq!(result.is_err(), excess);
        sample
            .read(ReadRequest::SourceScreenshot, Value::Null)
            .await
            .unwrap();
    }
    for (method, field, count) in [
        ("GetSceneList", "scenes", 257),
        ("GetInputList", "inputs", 2049),
        ("GetSceneItemList", "sceneItems", 129),
    ] {
        let args = arguments();
        let mut fixture = replies();
        fixture[method][0][field] = json!(vec![json!({}); count]);
        let (report, trace) = run(&args, fixture.clone()).await;
        assert_eq!(report["status"], "unknown");
        assert_eq!(report["blockers"][0]["reason"], "evidence_unavailable");
        assert_eq!(
            node_plan(&args, &fixture),
            json!({"report":without_times(report),"trace":trace})
        );
    }
}

#[test]
fn schema_admission_and_event_prefixes_do_not_grant_mutation_or_guessing() {
    for variant in [
        "unknown-transform",
        "bad-alignment",
        "negative-crop",
        "too-many-bindings",
        "unsafe-id",
        "null-canvas",
    ] {
        let mut args = arguments();
        match variant {
            "unknown-transform" => args["template"]["roles"][0]["transform"]["dynamic"] = json!(1),
            "bad-alignment" => args["template"]["roles"][0]["transform"]["alignment"] = json!(3),
            "negative-crop" => args["template"]["roles"][0]["transform"]["cropLeft"] = json!(-1),
            "too-many-bindings" => args["bindings"] = json!(vec![args["bindings"][0].clone(); 65]),
            "unsafe-id" => args["bindings"][0]["sceneItemId"] = json!(9_007_199_254_740_992u64),
            _ => args["canvasUuid"] = Value::Null,
        }
        assert!(PlanRequest::from_value(&args).is_err(), "{variant}");
    }
    for event in [
        "InputNameChanged",
        "CurrentProgramSceneChanged",
        "SceneItemRemoved",
        "VirtualcamStateChanged",
    ] {
        assert!(observation::invalidates_event(event));
    }
    for event in ["InputVolumeMeters", "VendorEvent", "RecordFileChanged"] {
        assert!(!observation::invalidates_event(event), "{event}");
    }
    let target = Target {
        scene_collection: "x".into(),
        scene_name: "y".into(),
        canvas_uuid: None,
    };
    assert_eq!(target.canvas(), json!({}));
}
