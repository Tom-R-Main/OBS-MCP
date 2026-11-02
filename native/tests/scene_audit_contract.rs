// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{
    observation::{EvidenceState, ObservationError, ReadAdapter, ReadFuture, ReadRequest},
    scene_audit::{self, AuditCompletion, AuditOptions, AuditSample, ScreenshotLimit, audit},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::watch, time::Instant};
const NOW: &str = "2026-10-31T12:00:00.000Z";
const PNG: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
fn options() -> Value {
    json!({"sceneCollection":"Audit collection","sceneName":"Review","requiredItems":[{"sourceName":"Application"}],"includeScreenshot":false})
}
fn transform() -> Value {
    json!({"positionX":0,"positionY":0,"rotation":0,"scaleX":1,"scaleY":1,"sourceWidth":1280,"sourceHeight":720,"cropLeft":0,"cropRight":0,"cropTop":0,"cropBottom":0,"alignment":5,"boundsAlignment":0,"boundsWidth":0,"boundsHeight":0,"boundsType":"OBS_BOUNDS_NONE"})
}
fn item() -> Value {
    json!({"sourceName":"Application","sourceUuid":"application-source-uuid","sceneItemId":7,"sceneItemEnabled":true,"isGroup":false,"inputKind":"screen_capture"})
}
fn fixture() -> Value {
    json!({
        "GetSceneCollectionList":[{"currentSceneCollectionName":"Audit collection"}],
        "GetSceneList":[{"scenes":[{"sceneName":"Review","sceneUuid":"review-scene-uuid"}]}],
        "GetInputList":[{"inputs":[{"inputName":"Application","inputUuid":"application-source-uuid","inputKind":"screen_capture"}]}],
        "GetSceneItemList":[{"sceneItems":[item()]}],"GetSceneItemTransform":[{"sceneItemTransform":transform()}],
        "GetSourceScreenshot":[{"imageData":PNG}]
    })
}
fn evidence() -> EvidenceState {
    EvidenceState {
        generation: 7,
        revision: 0,
        connected: true,
        cancelled: false,
    }
}
type Trace = Arc<Mutex<Vec<Value>>>;
struct Reader {
    replies: HashMap<String, VecDeque<Value>>,
    trace: Trace,
    unavailable: Option<ReadRequest>,
    failure: Option<(ReadRequest, ObservationError)>,
    event: Option<(ReadRequest, watch::Sender<EvidenceState>)>,
    delay: Duration,
}
impl Reader {
    fn new(value: Value) -> Self {
        Self {
            replies: value
                .as_object()
                .unwrap()
                .iter()
                .map(|(key, value)| (key.clone(), value.as_array().unwrap().clone().into()))
                .collect(),
            trace: Arc::new(Mutex::new(vec![])),
            unavailable: None,
            failure: None,
            event: None,
            delay: Duration::ZERO,
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
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if let Some((method, provider)) = &self.event
                && *method == request
            {
                let mut next = evidence();
                next.revision = 1;
                provider.send_replace(next);
            }
            if let Some((method, error)) = &self.failure
                && *method == request
            {
                return Err(error.clone());
            }
            let scoped = format!(
                "{}|{}",
                request.as_str(),
                data["sceneUuid"].as_str().unwrap_or("")
            );
            let key = if self.replies.contains_key(&scoped) {
                scoped
            } else {
                request.as_str().to_owned()
            };
            let queue = self.replies.get_mut(&key).ok_or_else(|| {
                ObservationError::new("evidence_unavailable", "Unexpected fixture read")
            })?;
            Ok(if queue.len() > 1 {
                queue.pop_front().unwrap()
            } else {
                queue.front().unwrap().clone()
            })
        })
    }
}
fn node(input: &Value) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let script = r#"
import {EventEmitter} from "node:events";
import {McpServer,InMemoryTransport} from "@modelcontextprotocol/server";
import {Client} from "@modelcontextprotocol/client";
import {initialize} from "./build/tools/scene-audit.js";
const chunks=[];for await(const chunk of process.stdin)chunks.push(chunk);
const input=JSON.parse(Buffer.concat(chunks).toString("utf8"));
const RealDate=Date;const epoch=RealDate.parse("2026-10-31T12:00:00.000Z");
globalThis.Date=class extends RealDate{constructor(...args){super(...(args.length?args:[epoch]));}static now(){return epoch;}};
if(input.limit!==undefined)process.env.OBS_MCP_MAX_SCREENSHOT_BYTES=input.limit;else delete process.env.OBS_MCP_MAX_SCREENSHOT_BYTES;
const trace=[];
class Reader extends EventEmitter{
 getConnectionStatus(){return {identified:true,generation:7}}
 async sendRequest(type,data){
   if(input.unavailable===type)throw Error(`OBS WebSocket does not advertise support for request '${type}'`);
   trace.push({type,data:data??null});
   if(input.event===type)this.emit("event","SceneItemEnableStateChanged");
   if(input.failure?.type===type)throw Error(input.failure.message);
   const key=`${type}|${data?.sceneUuid??""}`;
   const queue=input.replies[key]??input.replies[type];if(!queue?.length)throw Error("Unexpected fixture read");
   return structuredClone(queue.length>1?queue.shift():queue[0]);
 }
}
const reader=new Reader();const server=new McpServer({name:"audit-oracle",version:"1"});initialize(server,reader);
const client=new Client({name:"audit-consumer",version:"1"});const[a,b]=InMemoryTransport.createLinkedPair();
try{await Promise.all([server.connect(a),client.connect(b)]);const result=await client.callTool({name:"obs-audit-scene",arguments:input.options});console.log(JSON.stringify({result,trace,listeners:reader.listenerCount("event")}));}
finally{await client.close();await server.close();}
"#;
    let mut child = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(script)
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let written = child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(input).unwrap());
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "isolated TypeScript structural audit oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
            .chars()
            .take(1600)
            .collect::<String>()
    );
    written.unwrap();
    serde_json::from_slice(&output.stdout).unwrap()
}
async fn native(input: &Value) -> (Value, Vec<Value>, bool) {
    let (provider, states) = watch::channel(evidence());
    let mut reader = Reader::new(input["replies"].clone());
    if let Some(method) = input["unavailable"].as_str() {
        reader.unavailable = Some(method_type(method));
    }
    if let Some(method) = input["failure"]["type"].as_str() {
        reader.failure = Some((
            method_type(method),
            ObservationError::new(
                "evidence_unavailable",
                input["failure"]["message"].as_str().unwrap(),
            ),
        ));
    }
    if let Some(method) = input["event"].as_str() {
        reader.event = Some((method_type(method), provider.clone()));
    }
    let trace = reader.trace.clone();
    let mut sample = AuditSample::new(
        &mut reader,
        states,
        7,
        NOW.to_owned(),
        Instant::now() + Duration::from_secs(12),
    );
    let result = audit(
        &mut sample,
        &AuditOptions::from_value(&input["options"]).unwrap(),
        ScreenshotLimit::parse(input["limit"].as_str()),
        || AuditCompletion {
            completed_at: NOW.to_owned(),
            duration_ms: 0,
        },
    )
    .await;
    (
        result,
        trace.lock().unwrap().clone(),
        sample.requires_compatibility(),
    )
}
fn method_type(method: &str) -> ReadRequest {
    match method {
        "GetSceneItemTransform" => ReadRequest::SceneItemTransform,
        "GetInputList" => ReadRequest::InputList,
        "GetSourceScreenshot" => ReadRequest::SourceScreenshot,
        "GetSceneItemList" => ReadRequest::SceneItemList,
        _ => panic!("unsupported test method"),
    }
}
fn result_semantics(mut result: Value) -> Value {
    // Only JSON text object key order is immaterial. Validate text/report
    // coherence first; preserve every other result field and image byte.
    let text = result["content"][0]["text"].as_str().expect("report text");
    let parsed: Value = serde_json::from_str(text).unwrap();
    assert_eq!(parsed, result["structuredContent"]);
    result["content"][0]["text"] = Value::Null;
    result["content"][0]["jsonText"] = parsed;
    result
}
async fn compare(input: &Value) -> Value {
    let (result, trace, compatibility) = native(input).await;
    let expected = node(input);
    assert!(
        !compatibility,
        "supported evidence must not require an SDK projection"
    );
    assert_eq!(trace, expected["trace"].as_array().unwrap().clone());
    assert_eq!(
        result_semantics(result.clone()),
        result_semantics(expected["result"].clone())
    );
    assert_eq!(expected["listeners"], 0);
    result
}

#[tokio::test]
async fn flat_named_canvas_and_screenshot_reports_match_source_without_rendered_or_audio_readiness_claims()
 {
    for canvas in [false, true] {
        let mut options = options();
        let mut replies = fixture();
        options["includeScreenshot"] = json!(true);
        if canvas {
            options["canvasUuid"] = json!("secondary-canvas");
            replies["GetSceneList"][0]["scenes"][0]["sceneUuid"] = json!("secondary-scene");
        }
        let result = compare(&json!({"options":options,"replies":replies})).await;
        assert_eq!(result["structuredContent"]["status"], "pass");
        assert_eq!(result["structuredContent"]["renderedContent"], "unverified");
        assert_eq!(result["content"].as_array().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn missing_hidden_repeated_and_empty_occurrences_keep_source_status_precedence() {
    for variant in [
        "missing",
        "hidden",
        "repeated",
        "exact-repeat",
        "empty",
        "unknown-enabled",
    ] {
        let mut options = options();
        let mut replies = fixture();
        let status = match variant {
            "missing" => {
                options["requiredItems"][0]["sourceName"] = json!("Missing");
                "fail"
            }
            "hidden" => {
                replies["GetSceneItemList"][0]["sceneItems"][0]["sceneItemEnabled"] = json!(false);
                "fail"
            }
            "empty" => {
                replies["GetSceneItemList"][0]["sceneItems"] = json!([]);
                "fail"
            }
            "unknown-enabled" => {
                replies["GetSceneItemList"][0]["sceneItems"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("sceneItemEnabled");
                "unknown"
            }
            _ => {
                let mut repeated = item();
                repeated["sceneItemId"] = json!(9);
                replies["GetSceneItemList"][0]["sceneItems"]
                    .as_array_mut()
                    .unwrap()
                    .push(repeated);
                if variant == "exact-repeat" {
                    options["requiredItems"][0]["sceneItemId"] = json!(9);
                    options["requiredItems"][0]["containerUuid"] = json!("review-scene-uuid");
                    "pass"
                } else {
                    "unknown"
                }
            }
        };
        let result = compare(&json!({"options":options,"replies":replies})).await;
        assert_eq!(result["structuredContent"]["status"], status, "{variant}");
    }
}

#[tokio::test]
async fn malformed_identity_transform_and_geometry_are_not_projected_into_a_pass() {
    for variant in [
        "scene-list",
        "input-list",
        "item-id",
        "null-group",
        "missing-transform",
        "fractional-width",
        "large-width",
        "zero-width",
        "zero-scale",
        "full-crop",
        "rotation",
        "bounds",
        "signed-scale",
        "extra-transform",
    ] {
        let mut replies = fixture();
        let status = match variant {
            "scene-list" => {
                replies["GetSceneList"][0] = json!({"scenes":[{}]});
                "unknown"
            }
            "input-list" => {
                replies["GetInputList"][0] = json!({"inputs":[{}]});
                "unknown"
            }
            "item-id" => {
                replies["GetSceneItemList"][0]["sceneItems"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("sceneItemId");
                "unknown"
            }
            "null-group" => {
                replies["GetSceneItemList"][0]["sceneItems"][0]["isGroup"] = Value::Null;
                "unknown"
            }
            "missing-transform" => {
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]
                    .as_object_mut()
                    .unwrap()
                    .remove("sourceWidth");
                "unknown"
            }
            "fractional-width" => {
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]["sourceWidth"] =
                    json!(0.5);
                "unknown"
            }
            "large-width" => {
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]["sourceWidth"] =
                    json!(4294967296u64);
                "unknown"
            }
            "zero-width" => {
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]["sourceWidth"] = json!(0);
                "fail"
            }
            "zero-scale" => {
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]["scaleX"] = json!(0);
                "fail"
            }
            "full-crop" => {
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]["cropLeft"] = json!(1280);
                "fail"
            }
            "rotation" => {
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]["rotation"] = json!(20);
                "unknown"
            }
            "bounds" => {
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]["boundsType"] =
                    json!("OBS_BOUNDS_SCALE_INNER");
                "unknown"
            }
            "signed-scale" => {
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]["scaleX"] = json!(-1);
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]["positionX"] =
                    json!(-9000);
                "pass"
            }
            _ => {
                replies["GetSceneItemTransform"][0]["sceneItemTransform"]["untrustedExtra"] =
                    json!("not evidence");
                "pass"
            }
        };
        let result = compare(&json!({"options":options(),"replies":replies})).await;
        assert_eq!(result["structuredContent"]["status"], status, "{variant}");
        assert!(
            !result["structuredContent"]
                .to_string()
                .contains("untrustedExtra")
        );
    }
}

#[tokio::test]
async fn audio_capture_kinds_are_evidence_but_media_filenames_and_mute_settings_are_not_read() {
    for (kind, status) in [
        ("coreaudio_input_capture", "pass"),
        ("wasapi_output_capture", "pass"),
        ("sck_audio_capture", "pass"),
        ("jack_fixture", "pass"),
        ("ffmpeg_source", "unknown"),
        ("vlc_source", "unknown"),
        ("screen_capture", "fail"),
    ] {
        let mut replies = fixture();
        replies["GetInputList"][0]["inputs"][0]["inputKind"] = json!(kind);
        replies["GetSceneItemTransform"][0]["sceneItemTransform"]["sourceWidth"] = json!(0);
        replies["GetSceneItemTransform"][0]["sceneItemTransform"]["sourceHeight"] = json!(0);
        let input = json!({"options":options(),"replies":replies});
        let result = compare(&input).await;
        assert_eq!(result["structuredContent"]["status"], status, "{kind}");
        let (_, trace, _) = native(&input).await;
        assert!(trace.iter().all(|read| !matches!(
            read["type"].as_str(),
            Some("GetInputSettings" | "GetInputMute" | "GetInputVolume")
        )));
    }
}

#[tokio::test]
async fn groups_nested_scenes_cycles_and_disabled_ancestry_keep_occurrence_paths() {
    for variant in [
        "group",
        "nested",
        "cycle",
        "nested-rename",
        "disabled-ancestor",
    ] {
        let mut replies = fixture();
        let mut parent = item();
        let mut child = item();
        child["sceneItemId"] = json!(2);
        if variant == "cycle" {
            parent["sourceUuid"] = json!("review-scene-uuid");
            parent["sourceName"] = json!("Review");
            replies["GetSceneItemList"][0]["sceneItems"] = json!([parent]);
        } else if variant == "group" {
            parent["sourceUuid"] = json!("group-uuid");
            parent["sourceName"] = json!("Group");
            parent["isGroup"] = json!(true);
            replies["GetSceneItemList"][0]["sceneItems"] = json!([parent]);
            replies["GetGroupSceneItemList"] = json!([{"sceneItems":[child]}]);
        } else {
            parent["sourceUuid"] = json!("child-scene");
            parent["sourceName"] = json!(if variant == "nested-rename" {
                "Old name"
            } else {
                "Child"
            });
            if variant == "disabled-ancestor" {
                parent["sceneItemEnabled"] = json!(false);
            }
            replies["GetSceneList"][0]["scenes"]
                .as_array_mut()
                .unwrap()
                .push(json!({"sceneName":"Child","sceneUuid":"child-scene"}));
            replies["GetSceneItemList"] = json!([]);
            replies["GetSceneItemList|review-scene-uuid"] = json!([{"sceneItems":[parent]}]);
            replies["GetSceneItemList|child-scene"] = json!([{"sceneItems":[child]}]);
        }
        let mut opts = options();
        opts["canvasUuid"] = json!("secondary-canvas");
        let result = compare(&json!({"options":opts,"replies":replies})).await;
        let report = &result["structuredContent"];
        assert_eq!(
            report["status"],
            if variant == "disabled-ancestor" {
                "fail"
            } else {
                "unknown"
            }
        );
        if variant == "disabled-ancestor" {
            assert_eq!(report["items"][1]["effectiveEnabled"], false);
            assert_eq!(report["items"][1]["path"].as_array().unwrap().len(), 2);
        }
    }
}

#[tokio::test]
async fn changed_membership_transform_collection_and_events_invalidate_all_conclusions() {
    for variant in [
        "membership",
        "transform",
        "collection",
        "scenes",
        "inputs",
        "event",
    ] {
        let mut replies = fixture();
        let mut input = json!({"options":options(),"replies":{}});
        match variant {
            "membership" => {
                let mut after = replies["GetSceneItemList"][0].clone();
                after["sceneItems"][0]["sourceUuid"] = json!("replacement");
                replies["GetSceneItemList"]
                    .as_array_mut()
                    .unwrap()
                    .push(after);
            }
            "transform" => {
                let mut after = replies["GetSceneItemTransform"][0].clone();
                after["sceneItemTransform"]["sourceWidth"] = json!(0);
                replies["GetSceneItemTransform"]
                    .as_array_mut()
                    .unwrap()
                    .push(after);
            }
            "collection" => {
                replies["GetSceneCollectionList"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"currentSceneCollectionName":"Other"}));
            }
            "scenes" => {
                let mut after = replies["GetSceneList"][0].clone();
                after["scenes"][0]["sceneName"] = json!("Renamed");
                replies["GetSceneList"].as_array_mut().unwrap().push(after);
            }
            "inputs" => {
                let mut after = replies["GetInputList"][0].clone();
                after["inputs"][0]["inputName"] = json!("Renamed");
                replies["GetInputList"].as_array_mut().unwrap().push(after);
            }
            _ => input["event"] = json!("GetSceneItemTransform"),
        }
        input["replies"] = replies;
        let result = compare(&input).await;
        assert_eq!(result["structuredContent"]["status"], "unknown");
        assert!(
            result["structuredContent"]["checks"]
                .as_array()
                .unwrap()
                .iter()
                .all(|check| check["status"] == "unknown")
        );
    }
}

#[tokio::test]
async fn screenshot_limits_and_invalid_configuration_preserve_source_failure_semantics() {
    for variant in [
        "missing",
        "invalid-url",
        "empty",
        "padding",
        "non-multiple",
        "too-large",
        "invalid-limit",
        "hard-limit",
        "invalid-unused",
    ] {
        let mut opts = options();
        opts["includeScreenshot"] = json!(true);
        let mut replies = fixture();
        let mut input = json!({"options":{},"replies":{}});
        match variant {
            "missing" => replies["GetSourceScreenshot"][0] = json!({}),
            "invalid-url" => replies["GetSourceScreenshot"][0]["imageData"] = json!("bad"),
            "empty" => {
                replies["GetSourceScreenshot"][0]["imageData"] = json!("data:image/png;base64,")
            }
            "padding" => {
                replies["GetSourceScreenshot"][0]["imageData"] = json!("data:image/png;base64,A===")
            }
            "non-multiple" => {
                replies["GetSourceScreenshot"][0]["imageData"] = json!("data:image/png;base64,AAA")
            }
            "too-large" => input["limit"] = json!("2"),
            "hard-limit" => input["limit"] = json!("6291457"),
            _ => input["limit"] = json!(" 4"),
        }
        if variant == "invalid-unused" {
            opts["includeScreenshot"] = json!(false);
        }
        input["options"] = opts;
        input["replies"] = replies;
        let result = compare(&input).await;
        assert_eq!(
            result["structuredContent"]["status"],
            if variant == "invalid-unused" {
                "pass"
            } else {
                "unknown"
            }
        );
        assert_eq!(result["content"].as_array().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn advertised_absence_and_peer_projection_are_not_conflated() {
    let mut input =
        json!({"options":options(),"replies":fixture(),"unavailable":"GetSceneItemTransform"});
    let result = compare(&input).await;
    assert_eq!(result["structuredContent"]["status"], "unknown");
    input.as_object_mut().unwrap().remove("unavailable");
    input["failure"] = json!({"type":"GetSceneItemTransform","message":"fixture peer unavailable"});
    let result = compare(&input).await;
    assert_eq!(result["structuredContent"]["status"], "unknown");
    let (provider, states) = watch::channel(evidence());
    let mut reader = Reader::new(fixture());
    reader.failure = Some((
        ReadRequest::SceneItemTransform,
        ObservationError::rejected(
            604,
            "capability_unavailable",
            "peer text requires qualified projection",
        ),
    ));
    let mut sample = AuditSample::new(
        &mut reader,
        states,
        7,
        NOW.to_owned(),
        Instant::now() + Duration::from_secs(12),
    );
    let _result = audit(
        &mut sample,
        &AuditOptions::from_value(&options()).unwrap(),
        Ok(ScreenshotLimit::default()),
        || AuditCompletion {
            completed_at: NOW.to_owned(),
            duration_ms: 0,
        },
    )
    .await;
    assert!(sample.requires_compatibility());
    drop(provider);
}

#[tokio::test]
async fn source_utf16_evidence_budget_and_512_read_limit_are_distinct_from_planning() {
    let mut replies = fixture();
    replies["GetSceneItemTransform"][0]["untrustedExtra"] = json!("🙂".repeat(80_000));
    let input = json!({"options":options(),"replies":replies});
    let result = compare(&input).await;
    assert_eq!(
        result["structuredContent"]["status"], "pass",
        "160k UTF16 units may exceed256k UTF8 bytes without failing source audit"
    );
    let mut reader = Reader::new(json!({"GetSceneItemList":[{}]}));
    let trace = reader.trace.clone();
    let (provider, states) = watch::channel(evidence());
    let mut sample = AuditSample::new(
        &mut reader,
        states,
        7,
        NOW.to_owned(),
        Instant::now() + Duration::from_secs(12),
    );
    for _ in 0..512 {
        sample
            .read(ReadRequest::SceneItemList, Value::Null)
            .await
            .unwrap();
    }
    assert_eq!(
        sample
            .read(ReadRequest::SceneItemList, Value::Null)
            .await
            .unwrap_err()
            .message,
        "Scene audit request or time limit reached"
    );
    assert_eq!(trace.lock().unwrap().len(), 512);
    drop(provider);
}

#[tokio::test]
async fn unsupported_signature_projection_and_lost_provider_fail_closed() {
    let mut replies = fixture();
    replies["GetSceneList"][0]["scenes"][0]["évidence"] = json!(true);
    let (result, _, compatibility) = native(&json!({"options":options(),"replies":replies})).await;
    assert_eq!(result["structuredContent"]["status"], "unknown");
    assert!(compatibility);
    let mut reader = Reader::new(fixture());
    let trace = reader.trace.clone();
    let (provider, states) = watch::channel(evidence());
    drop(provider);
    let mut sample = AuditSample::new(
        &mut reader,
        states,
        7,
        NOW.to_owned(),
        Instant::now() + Duration::from_secs(12),
    );
    let result = audit(
        &mut sample,
        &AuditOptions::from_value(&options()).unwrap(),
        Ok(ScreenshotLimit::default()),
        || AuditCompletion {
            completed_at: NOW.to_owned(),
            duration_ms: 0,
        },
    )
    .await;
    assert_eq!(result["structuredContent"]["status"], "unknown");
    assert!(trace.lock().unwrap().is_empty());
}

#[tokio::test]
async fn inventory_depth_and_report_limits_match_the_actual_source() {
    for variant in ["items", "scenes", "inputs", "depth", "report"] {
        let mut replies = fixture();
        let mut opts = options();
        match variant {
            "items" => {
                replies["GetSceneItemList"][0]["sceneItems"] = json!(
                    (0..129)
                        .map(|id| {
                            let mut item = item();
                            item["sceneItemId"] = json!(id);
                            item
                        })
                        .collect::<Vec<_>>()
                );
            }
            "scenes" => {
                replies["GetSceneList"][0]["scenes"] = json!((0..257).map(|id|json!({"sceneName":format!("scene-{id}"),"sceneUuid":format!("scene-{id}")})).collect::<Vec<_>>());
            }
            "inputs" => {
                replies["GetInputList"][0]["inputs"] = json!((0..2049).map(|id|json!({"inputName":format!("input-{id}"),"inputUuid":format!("input-{id}"),"inputKind":"screen_capture"})).collect::<Vec<_>>());
            }
            "depth" => {
                for id in 0..12 {
                    replies["GetSceneList"][0]["scenes"].as_array_mut().unwrap().push(json!({"sceneName":format!("Scene {id}"),"sceneUuid":format!("scene-{id}")}));
                    let mut next = item();
                    next["sourceName"] = json!(format!("Scene {}", id + 1));
                    next["sourceUuid"] = json!(format!("scene-{}", id + 1));
                    replies[format!("GetSceneItemList|scene-{id}")] =
                        json!([{"sceneItems":[next]}]);
                }
                let mut first = item();
                first["sourceName"] = json!("Scene 0");
                first["sourceUuid"] = json!("scene-0");
                replies["GetSceneItemList"][0]["sceneItems"] = json!([first]);
                opts["requiredItems"][0]["sourceName"] = json!("Missing");
            }
            _ => {
                let groups = (0..8)
                    .map(|id| {
                        (
                            format!("{id}{}", "g".repeat(255)),
                            format!("{id}{}", "u".repeat(255)),
                        )
                    })
                    .collect::<Vec<_>>();
                let mut first = item();
                first["sourceName"] = json!(groups[0].0);
                first["sourceUuid"] = json!(groups[0].1);
                first["isGroup"] = json!(true);
                replies["GetSceneItemList"][0]["sceneItems"] = json!([first]);
                for (index, (_name, uuid)) in groups.iter().enumerate() {
                    let members = if index == 7 {
                        (0..80)
                            .map(|id| {
                                let mut member = item();
                                member["sceneItemId"] = json!(id);
                                member
                            })
                            .collect::<Vec<_>>()
                    } else {
                        let mut child = item();
                        child["sourceName"] = json!(groups[index + 1].0);
                        child["sourceUuid"] = json!(groups[index + 1].1);
                        child["isGroup"] = json!(true);
                        vec![child]
                    };
                    replies[format!("GetGroupSceneItemList|{uuid}")] =
                        json!([{"sceneItems":members}]);
                }
            }
        }
        let result = compare(&json!({"options":opts,"replies":replies})).await;
        let report = &result["structuredContent"];
        assert_eq!(report["status"], "unknown", "{variant}");
        if variant == "depth" {
            assert!(report["items"].as_array().unwrap().len() <= 9);
        }
        if variant == "report" {
            assert_eq!(report["items"], json!([]));
            assert_eq!(report["checks"][0]["id"], "report-size");
            assert!(report.to_string().len() < 512 * 1024);
        }
    }
}

#[test]
fn audit_profile_and_option_schema_do_not_silently_inherit_broader_semantics() {
    assert!(scene_audit::invalidates_event("SceneItemTransformChanged"));
    assert!(scene_audit::invalidates_event("InputSettingsChanged"));
    assert!(!scene_audit::invalidates_event("InputMuteStateChanged"));
    assert!(!scene_audit::invalidates_event("RecordStateChanged"));
    for value in [
        json!({"sceneCollection":"a","sceneName":"b","requiredItems":null}),
        json!({"sceneCollection":"a","sceneName":"b","includeScreenshot":null}),
        json!({"sceneCollection":"a","sceneName":"b","requiredItems":[{"sourceName":"a","sceneItemId":-1}]}),
    ] {
        assert!(AuditOptions::from_value(&value).is_err());
    }
}
