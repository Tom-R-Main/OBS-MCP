// SPDX-License-Identifier: GPL-2.0-only
//! Pure-domain contracts with the actual corrected compiled Source as oracle.
//! No OBS socket, Service, operation store, recording or public admission.
use obs_mcp_native::{
    apply_scene::{self as apply, Canvas, Current, Effect, Request, Status},
    tool_validation::ApplySceneSettingsOrder,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

fn request(raw: &str) -> Request {
    let value: Value = serde_json::from_str(raw).unwrap();
    let frame = format!(r#"{{"params":{{"name":"obs-apply-scene","arguments":{raw}}}}}"#);
    let order = ApplySceneSettingsOrder::from_frame(&value, frame.as_bytes()).unwrap();
    Request::parse(&value, &order).unwrap()
}
fn parse(value: Value) -> Request {
    request(&value.to_string())
}
fn current() -> Current {
    Current{scene_exists:true,scene_names:HashSet::from(["Demo".into()]),input_kinds:HashMap::from([("Chrome".into(),"screen_capture".into()),("Background".into(),"color_source_v3".into())]),
        inputs:serde_json::from_value(json!([
            {"inputName":"Chrome","inputKind":"screen_capture","inputSettings":{"file":"old","alpha":1},"muted":true,"volumeMul":1,"audioTracks":{"1":false}},
            {"inputName":"Background","inputKind":"color_source_v3","inputSettings":{}}
        ])).unwrap(),
        items:serde_json::from_value(json!([
            {"sceneName":"Demo","sceneItemId":1,"sourceName":"Background","index":0,"enabled":true,"locked":true,"transform":{"positionX":0}},
            {"sceneName":"Demo","sceneItemId":2,"sourceName":"Chrome","index":1,"enabled":true,"locked":false,"transform":{"positionX":204,"rotation":0}}
        ])).unwrap(),video:Some(Canvas{width:1920.0,height:1080.0})}
}
fn projection(c: &Current) -> Value {
    json!({"sceneExists":c.scene_exists,"sceneNames":c.scene_names,"inputKinds":c.input_kinds,"items":c.items,"inputs":c.inputs,"video":c.video.map(|v|json!({"baseWidth":v.width,"baseHeight":v.height}))})
}
fn node(body: &str, input: Value) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    std::fs::create_dir_all(root.join("native/target")).unwrap();
    let script = format!(
        r#"
import {{readFileSync}} from "node:fs";
import {{initialize,planScene}} from "./build/tools/apply-scene.js";
const input=JSON.parse(readFileSync(0,"utf8"));
let schema;
initialize({{registerTool(_name,definition){{schema=definition.inputSchema;}}}},{{}});
const normalized=raw=>schema.parse(JSON.parse(raw));
const current=value=>({{...value,sceneNames:new Set(value.sceneNames),inputKinds:new Map(Object.entries(value.inputKinds))}});
{body}
"#
    );
    let mut child = Command::new("node")
        .args(["--input-type=module", "-e", &script])
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "corrected compiled Source oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn source_plan(raw: &str, current: &Current) -> Value {
    node(
        "const {apply,...spec}=normalized(input.raw);console.log(JSON.stringify(planScene(spec,current(input.current))));",
        json!({"raw":raw,"current":projection(current)}),
    )
}
fn source_arguments(raw: &str) -> Value {
    node(
        "const parsed=schema.safeParse(JSON.parse(input.raw));console.log(JSON.stringify(parsed.success?{ok:true,args:parsed.data}:{ok:false}));",
        json!({"raw":raw}),
    )
}
#[test]
fn schema_defaults_stripping_and_transform_declaration_order_match_source() {
    let raw = r#"{"sceneName":"Demo","sources":[{"name":"Chrome","unknown":"strip","kind":"","transform":{"cropTop":0,"scaleX":0.3333,"rotation":12.34,"positionX":100.1,"unknown":9}}],"unknown":true}"#;
    let r = request(raw);
    let source = source_arguments(raw);
    assert_eq!(r.arguments(), source["args"]);
    assert!(!r.apply());
    assert!(!r.needs_canvas());
    assert_eq!(
        apply::plan(&r, &current()).projection(),
        source_plan(raw, &current())
    );
    let empty = r#"{"sceneName":"Demo","sources":[{"name":"Chrome","transform":{"strip":1}}]}"#;
    assert_eq!(request(empty).arguments(), source_arguments(empty)["args"]);
}
#[test]
fn integer_index_duplicate_and_string_settings_order_matches_source() {
    let raw = r#"{"sceneName":"Demo","sources":[{"name":"Chrome","settings":{"z":1,"10":10,"2":2,"z":7,"01":1,"4294967295":1,"a":2}}]}"#;
    let plan = apply::plan(&request(raw), &current());
    assert_eq!(plan.projection(), source_plan(raw, &current()));
    assert_eq!(
        plan.actions()[0].change(),
        "settings (2, 10, z, 01, 4294967295, a)"
    );
    assert_eq!(
        plan.actions()[0].effect().unwrap().request()["requestData"]["inputSettings"]["z"],
        7
    );
    assert_eq!(
        plan.actions()[0].effect().unwrap().request()["requestData"]["overlay"],
        true
    );
}
#[test]
fn sidecar_compares_original_arguments_and_cannot_supply_values_or_dispatch() {
    let raw = r#"{"sceneName":"Demo","sources":[{"name":"Chrome","settings":{"b":2,"a":1},"strip":true}],"strip":1}"#;
    let original: Value = serde_json::from_str(raw).unwrap();
    let frame = format!(r#"{{"params":{{"arguments":{raw}}}}}"#);
    let order = ApplySceneSettingsOrder::from_frame(&original, frame.as_bytes()).unwrap();
    let normalized = request(raw).arguments();
    assert!(ApplySceneSettingsOrder::from_frame(&normalized, frame.as_bytes()).is_err());
    let mut retained = original.clone();
    retained["sources"][0]["settings"] = json!({"a":1});
    assert!(Request::parse(&retained, &order).is_err());
    let mut values = original.clone();
    values["sources"][0]["settings"]["a"] = json!(99);
    let r = Request::parse(&values, &order).unwrap();
    assert_eq!(
        apply::plan(&r, &current()).actions()[0]
            .effect()
            .unwrap()
            .request()["requestData"]["inputSettings"]["a"],
        99
    );
    // Metadata does not bind values or grant authority. Request reads its own
    // independent values and dispatch must be owned by a later scoped adapter.
    let debug = format!("{order:?} {r:?}");
    assert!(!debug.contains("Chrome"));
    assert!(!debug.contains("settings"));
    assert!(ApplySceneSettingsOrder::from_frame(&original, b"{}").is_err());
    assert!(
        ApplySceneSettingsOrder::from_frame(&original, &vec![b' '; 8 * 1024 * 1024 + 1]).is_err()
    );
}
#[test]
fn invalid_inputs_match_source_and_cannot_be_normalized_into_valid_effects() {
    let cases = vec![
        json!({"sceneName":"","sources":[{"name":"Chrome"}]}),
        json!({"sceneName":"Demo","sources":[]}),
        json!({"sceneName":"Demo","sources":vec![json!({"name":"Chrome"});51]}),
        json!({"sceneName":"Demo","sources":[{"name":"Chrome","visible":null}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Chrome","volumeDb":27}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Chrome","audioTracks":[0]}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Chrome","fit":"screen"}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Chrome","transform":{"alignment":9007199254740992u64}}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Chrome","transform":{"cropTop":-1}}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Chrome","transform":{"boundsType":"other"}}]}),
    ];
    for args in cases {
        let raw = args.to_string();
        let frame = json!({"params":{"arguments":args}}).to_string();
        let parsed = ApplySceneSettingsOrder::from_frame(&args, frame.as_bytes())
            .ok()
            .and_then(|order| Request::parse(&args, &order).ok());
        assert!(parsed.is_none(), "native admitted invalid fixture");
        assert_eq!(source_arguments(&raw)["ok"], false);
    }
}
#[test]
fn all_closed_effects_placeholders_audio_fit_and_shared_inputs_match_source() {
    let raw = r#"{"sceneName":"Review","sources":[{"name":"Chrome","settings":{"file":"new"},"visible":false,"locked":true,"muted":false,"volumeDb":-6,"audioTracks":[6,2,6],"fit":"canvas","transform":{"positionX":12.3,"boundsWidth":1000}},{"name":"Mic","kind":"coreaudio_input_capture","settings":{},"locked":true,"muted":true}],"order":true,"removeOthers":true,"apply":true}"#;
    let mut c = current();
    c.scene_exists = false;
    c.items.clear();
    let r = request(raw);
    let plan = apply::plan(&r, &c);
    assert_eq!(plan.projection(), source_plan(raw, &c));
    assert!(
        plan.actions()
            .iter()
            .any(|a| a.change() == "lock" && a.effect().is_none())
    );
    let selected = apply::snapshot_selection(&r, &c).unwrap();
    assert!(selected.scenes.is_empty());
    assert_eq!(selected.inputs, vec!["Chrome"]);
    let raw = r#"{"sceneName":"Demo","sources":[{"name":"Chrome","settings":{"file":"new"},"visible":false,"locked":true,"muted":false,"volumeDb":-6,"audioTracks":[],"fit":"canvas","transform":{"boundsType":"OBS_BOUNDS_NONE","rotation":3}}],"order":true,"removeOthers":true,"apply":true}"#;
    assert_eq!(
        apply::plan(&request(raw), &current()).projection(),
        source_plan(raw, &current())
    );
}
#[test]
fn duplicates_scene_collisions_empty_kind_and_missing_input_errors_match_source() {
    for args in [
        json!({"sceneName":"Demo","sources":[{"name":"Chrome"},{"name":"Chrome"}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Demo","kind":"image_source"}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Missing","kind":""}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Chrome","kind":"other"}]}),
    ] {
        let raw = args.to_string();
        assert_eq!(
            apply::plan(&request(&raw), &current()).projection(),
            source_plan(&raw, &current())
        );
    }
}
#[test]
fn first_matching_duplicate_item_and_removal_order_are_source_compatible() {
    let mut c = current();
    let mut duplicate = c.items[1].clone();
    duplicate.scene_item_id = 7.0;
    duplicate.index = 2.0;
    c.items.push(duplicate);
    let raw = r#"{"sceneName":"Demo","sources":[{"name":"Chrome","visible":false,"locked":true,"transform":{"positionX":10}}],"order":true,"removeOthers":true}"#;
    let plan = apply::plan(&request(raw), &c);
    assert_eq!(plan.projection(), source_plan(raw, &c));
    for action in plan.actions() {
        if let Some(
            Effect::Enabled { id, .. }
            | Effect::Locked { id, .. }
            | Effect::Transform { id, .. }
            | Effect::Index { id, .. },
        ) = action.effect()
        {
            assert_eq!(*id, 2.0);
        }
    }
}
#[test]
fn float32_half_pixel_volume_threshold_and_audio_defaults_match_source() {
    let mut c = current();
    c.items[1].transform =
        json!({"positionX":100.0,"scaleX":0.3333_f32 as f64,"rotation":12.34_f32 as f64})
            .as_object()
            .unwrap()
            .clone();
    c.inputs[0].volume_mul = None;
    c.inputs[0].muted = None;
    c.inputs[0].audio_tracks = None;
    for volume in [0.0, 0.1, 0.10000000001, -100.0] {
        let args = json!({"sceneName":"Demo","sources":[{"name":"Chrome","volumeDb":volume,"muted":false,"audioTracks":[],"transform":{"positionX":100.1,"scaleX":0.3333,"rotation":12.34}}]});
        let raw = args.to_string();
        assert_eq!(
            apply::plan(&request(&raw), &c).projection(),
            source_plan(&raw, &c)
        );
    }
}
#[test]
fn snapshot_selection_deduplicates_scene_then_requested_shared_inputs() {
    let mut c = current();
    c.input_kinds.insert("Shared".into(), "image_source".into());
    let r = parse(json!({"sceneName":"Demo","sources":[{"name":"Shared"},{"name":"Chrome"}]}));
    let selection = apply::snapshot_selection(&r, &c).unwrap();
    assert_eq!(selection.scenes, vec!["Demo"]);
    assert_eq!(selection.inputs, vec!["Background", "Chrome", "Shared"]);
    c.scene_exists = false;
    c.items.clear();
    c.input_kinds.clear();
    assert!(apply::snapshot_selection(&r, &c).is_none());
}

fn source_workflow(raw: &str, currents: &[Current], statuses: Option<Vec<Status>>) -> Value {
    let replies = statuses.map(|rows| {
        rows.into_iter()
            .map(|s| json!({"ok":s.ok,"code":s.code,"comment":s.comment}))
            .collect::<Vec<_>>()
    });
    node(
        r#"
import {applyScene} from "./build/tools/apply-scene.js";
import {mkdtempSync,rmSync} from "node:fs";
import {join} from "node:path";
const directory=mkdtempSync(join(process.cwd(),"native","target","apply-source-"));
process.env.OBS_MCP_STATE_DIR=directory;
let phase=-1;let value=input.currents[0];const writes=[];
const get=(type,data={})=>{
  if(type==="GetSceneList"){value=input.currents[Math.min(++phase,input.currents.length-1)];return {scenes:value.sceneNames.map(sceneName=>({sceneName}))};}
  if(type==="GetInputList")return {inputs:Object.entries(value.inputKinds).map(([inputName,inputKind])=>({inputName,inputKind}))};
  if(type==="GetVideoSettings")return value.video;
  if(type==="GetSceneItemList")return {sceneItems:value.items.map(item=>({sceneItemId:item.sceneItemId,sourceName:item.sourceName,sceneItemIndex:item.index,sceneItemEnabled:item.enabled,sceneItemLocked:item.locked,sceneItemTransform:{...item.transform,sourceWidth:800,sourceHeight:600}}))};
  const found=value.inputs.find(input=>input.inputName===data.inputName);
  if(!found)throw {code:600,message:"missing input"};
  if(type==="GetInputSettings")return {inputKind:found.inputKind,inputSettings:found.inputSettings};
  const [field,key]=({GetInputMute:["muted","inputMuted"],GetInputVolume:["volumeMul","inputVolumeMul"],GetInputAudioTracks:["audioTracks","inputAudioTracks"]})[type]??[];
  if(!field||!(field in found))throw {code:604,message:"unsupported"};return {[key]:found[field]};
};
const client={
  getConnectionStatus(){return {url:"ws://pure-source-oracle"}},
  async sendRequest(type,data){return get(type,data)},
  async sendBatch(requests,options={}){
    if(requests.every(r=>r.requestType.startsWith("Get")))return requests.map(r=>{try{return {ok:true,code:100,responseData:get(r.requestType,r.requestData)}}catch(e){return {ok:false,code:e.code,comment:e.message}}});
    writes.push({requests,haltOnFailure:options.haltOnFailure??false});
    if(input.statuses&&options.haltOnFailure){return input.statuses.map(s=>({...s,...(s.comment===null?{comment:undefined}:{})}));}
    return requests.map(()=>({ok:true,code:100,responseData:{}}));
  }
};
try {
  const {apply,...spec}=normalized(input.raw);
  const result=await applyScene(client,spec,apply);
  for(const block of result.content??[])if(block.type==="text")block.text=block.text.replace(/(snapshot )[0-9a-f]{8}\b/gi,"$1<SNAPSHOT_ID>");
  if(result.structuredContent?.snapshotId)result.structuredContent.snapshotId="<SNAPSHOT_ID>";
  console.log(JSON.stringify({result,writes}));
} finally {rmSync(directory,{recursive:true});delete process.env.OBS_MCP_STATE_DIR;}
"#,
        json!({"raw":raw,"currents":currents.iter().map(projection).collect::<Vec<_>>(),"statuses":replies}),
    )
}
#[test]
fn initial_no_change_plan_and_errors_match_actual_source_workflow() {
    for args in [
        json!({"sceneName":"Demo","sources":[{"name":"Chrome"}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Chrome","locked":true}]}),
        json!({"sceneName":"Demo","sources":[{"name":"Missing"}],"apply":true}),
    ] {
        let r = parse(args.clone());
        let c = current();
        let p = apply::plan(&r, &c);
        assert_eq!(
            apply::initial_result(&r, &p).unwrap(),
            source_workflow(&args.to_string(), &[c], None)["result"]
        );
    }
}
#[test]
fn continuation_refuses_omitted_acknowledged_creation_and_preserves_known_prefix() {
    let args = json!({"sceneName":"Demo","sources":[{"name":"Chrome","muted":false},{"name":"Slides","kind":"image_source"}],"apply":true});
    let r = parse(args.clone());
    let first = current();
    let creations = apply::plan(&r, &first).creations();
    let second = first.clone();
    let p = apply::plan(&r, &second);
    let source = source_workflow(&args.to_string(), &[first, second], None);
    let native = apply::continuation(&p, &creations, Some("<SNAPSHOT_ID>")).unwrap_err();
    assert_eq!(native, source["result"]);
    assert_eq!(source["writes"].as_array().unwrap().len(), 1);
    assert_eq!(
        source["writes"][0]["requests"][0]["requestType"],
        "CreateInput"
    );
}
fn check_continuation_refusal(wrong_kind: bool) {
    let args = json!({"sceneName":"Demo","sources":[{"name":"Chrome","kind":"screen_capture","muted":false}],"apply":true});
    let r = parse(args.clone());
    let first = current();
    let mut second = first.clone();
    if wrong_kind {
        second
            .input_kinds
            .insert("Chrome".into(), "browser_source".into());
        second.inputs[0].input_kind = "browser_source".into();
    } else {
        second.input_kinds.remove("Chrome");
        second.inputs.remove(0);
        second.items.retain(|i| i.source_name != "Chrome");
    }
    let plan = apply::plan(&r, &second);
    let source = source_workflow(&args.to_string(), &[first, second], None);
    let native = apply::continuation(&plan, &[], Some("<SNAPSHOT_ID>")).unwrap_err();
    assert_eq!(native, source["result"]);
    assert_eq!(source["writes"], json!([]));
}
#[test]
fn continuation_refuses_external_removal() {
    check_continuation_refusal(false);
}
#[test]
fn continuation_refuses_second_plan_kind_error() {
    check_continuation_refusal(true);
}
#[test]
fn final_kind_error_is_not_convergence_or_rpc_failure_in_actual_source() {
    let args = json!({"sceneName":"Demo","sources":[{"name":"Chrome","kind":"screen_capture","muted":false}],"apply":true});
    let r = parse(args.clone());
    let first = current();
    let second = first.clone();
    let changes = apply::continuation(&apply::plan(&r, &second), &[], None).unwrap();
    let mut last = second.clone();
    last.input_kinds
        .insert("Chrome".into(), "browser_source".into());
    last.inputs[0].input_kind = "browser_source".into();
    last.inputs[0].muted = Some(false);
    let native = apply::final_result(
        &r,
        &[],
        &changes,
        &[],
        &apply::plan(&r, &last),
        &[],
        Some("<SNAPSHOT_ID>"),
    );
    assert_eq!(
        native,
        source_workflow(&args.to_string(), &[first, second, last], None)["result"]
    );
    assert_eq!(native["structuredContent"]["converged"], false);
    assert_eq!(native["structuredContent"]["failures"], json!([]));
}
#[test]
fn creation_failure_prefix_skipped_suffix_and_empty_comment_match_source() {
    let args = json!({"sceneName":"New","sources":[{"name":"One","kind":"image_source"},{"name":"Two","kind":"image_source"}],"apply":true});
    let r = parse(args.clone());
    let mut c = current();
    c.scene_exists = false;
    c.items.clear();
    c.inputs.clear();
    c.input_kinds.clear();
    let creations = apply::plan(&r, &c).creations();
    let statuses = vec![
        Status {
            ok: true,
            code: 100,
            comment: None,
        },
        Status {
            ok: false,
            code: 601,
            comment: Some(String::new()),
        },
    ];
    let native = apply::creation_result(&creations, &statuses, None).unwrap();
    assert_eq!(
        native,
        source_workflow(&args.to_string(), &[c], Some(statuses))["result"]
    );
    assert!(
        native["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Two: create image_source failed (skipped)")
    );
}
#[test]
fn ordinary_final_result_and_snapshot_failure_keep_distinct_claims() {
    let args = json!({"sceneName":"Demo","sources":[{"name":"Chrome","muted":false}],"apply":true});
    let r = parse(args.clone());
    let first = current();
    let second = first.clone();
    let changes = apply::continuation(&apply::plan(&r, &second), &[], None).unwrap();
    let mut last = second.clone();
    last.inputs[0].muted = Some(false);
    let native = apply::final_result(
        &r,
        &[],
        &changes,
        &[],
        &apply::plan(&r, &last),
        &[],
        Some("<SNAPSHOT_ID>"),
    );
    assert_eq!(
        native,
        source_workflow(&args.to_string(), &[first, second, last], None)["result"]
    );
    assert!(
        native["structuredContent"]
            .get("verificationErrors")
            .is_none()
    );
    assert_eq!(
        apply::snapshot_failure("disk failed")["content"][0]["text"],
        "Nothing changed: could not save a recovery snapshot: disk failed"
    );
    assert_eq!(
        apply::failure("reply lost")["content"][0]["text"],
        "Apply Scene failed: reply lost"
    );
}

#[test]
fn record_prototype_stripping_and_large_json_numbers_match_source() {
    let raw = r#"{"sceneName":"Demo","sources":[{"name":"Chrome","settings":{"z":9007199254740993,"__proto__":{"removed":true},"constructor":1,"prototype":2,"nested":{"__proto__":{"kept":true}}}}]}"#;
    let r = request(raw);
    assert_eq!(r.arguments(), source_arguments(raw)["args"]);
    assert_eq!(
        apply::plan(&r, &current()).projection(),
        source_plan(raw, &current())
    );
    assert!(
        r.arguments()["sources"][0]["settings"]
            .get("__proto__")
            .is_none()
    );
    assert_eq!(
        r.arguments()["sources"][0]["settings"]["nested"]["__proto__"]["kept"],
        true
    );
}
#[test]
fn all_source_order_permutations_and_remove_other_modes_match_source() {
    fn permutations(values: Vec<usize>) -> Vec<Vec<usize>> {
        if values.len() <= 1 {
            return vec![values];
        }
        let mut all = Vec::new();
        for (index, head) in values.iter().enumerate() {
            let mut rest = values.clone();
            rest.remove(index);
            for tail in permutations(rest) {
                let mut item = vec![*head];
                item.extend(tail);
                all.push(item);
            }
        }
        all
    }
    let mut cases = Vec::new();
    let mut expected = Vec::new();
    for indices in permutations(vec![0, 1, 2, 3]) {
        for remove in [false, true] {
            let mut c = current();
            for name in ["Slides", "Webcam"] {
                c.input_kinds.insert(name.into(), "image_source".into());
                let mut input = c.inputs[1].clone();
                input.input_name = name.into();
                input.input_kind = "image_source".into();
                c.inputs.push(input);
                let mut item = c.items[0].clone();
                item.source_name = name.into();
                item.scene_item_id = c.items.len() as f64 + 1.0;
                c.items.push(item);
            }
            c.items = indices
                .iter()
                .enumerate()
                .map(|(index, source)| {
                    let mut item = c.items[*source].clone();
                    item.index = index as f64;
                    item
                })
                .collect();
            let args = json!({"sceneName":"Demo","sources":[{"name":"Webcam"},{"name":"Chrome"}],"order":true,"removeOthers":remove});
            let raw = args.to_string();
            cases.push(json!({"raw":raw,"current":projection(&c)}));
            expected.push(apply::plan(&request(&raw), &c).projection());
        }
    }
    let actual = node(
        "console.log(JSON.stringify(input.cases.map(c=>{const {apply,...spec}=normalized(c.raw);return planScene(spec,current(c.current));})));",
        json!({"cases":cases}),
    );
    assert_eq!(json!(expected), actual);
    assert!(expected.iter().any(|p| {
        p["actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["request"]["requestType"] == "SetSceneItemIndex")
    }));
}
