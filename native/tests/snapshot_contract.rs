// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{
    observation::{ObservationError, ReadAdapter, ReadFuture, ReadRequest},
    snapshots::{
        BatchReadResult, CaptureMetadata, CaptureOptions, Mutation, ReadCall, RequiredInputField,
        Snapshot, SnapshotError, SnapshotFile, SnapshotFuture, SnapshotReader, SnapshotWriter,
        plan_restore, read_inputs, take_snapshot,
    },
};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::time::Instant;
const NOW: &str = "2026-10-31T12:00:00.000Z";
const URL: &str = "ws://127.0.0.1:4455";
const CAPACITY: usize = 4 * 1024 * 1024;
fn state() -> Value {
    json!({"program":"Demo","inputs":{"Background":{"kind":"color_source_v3","settings":{"color":4278190080u64}},"Chrome":{"kind":"screen_capture","settings":{"type":1,"window":31875},"muted":true,"volumeMul":1,"tracks":{"1":false,"2":false,"3":false,"4":false,"5":false,"6":false}}},"scenes":{"Demo":[{"sceneItemId":1,"sourceName":"Background","enabled":true,"locked":true,"transform":{"positionX":0,"positionY":0,"scaleX":1,"scaleY":1,"rotation":0,"alignment":5,"boundsType":"OBS_BOUNDS_NONE","cropTop":0,"sourceWidth":1920}},{"sceneItemId":2,"sourceName":"Chrome","enabled":true,"locked":false,"transform":{"positionX":204,"positionY":65,"scaleX":1,"scaleY":1,"rotation":0,"alignment":5,"boundsType":"OBS_BOUNDS_NONE","cropTop":40,"sourceWidth":1512}}]},"failures":{}})
}
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .unwrap();
        let root = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("obs-snapshot-contract-{}-{id}", std::process::id()));
        fs::create_dir(&root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(root)
    }
    fn file(&self) -> PathBuf {
        self.0.join("snapshots.json")
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Default)]
struct Memory {
    saved: Vec<Snapshot>,
    fail: bool,
}
impl SnapshotWriter for Memory {
    fn save(&mut self, snapshot: &Snapshot) -> Result<(), SnapshotError> {
        if self.fail {
            return Err(SnapshotError::HistoryUnavailable);
        }
        self.saved.push(snapshot.clone());
        Ok(())
    }
}
struct Reader {
    state: Value,
    trace: Vec<Value>,
    delay: Duration,
}
impl Reader {
    fn new(state: Value) -> Self {
        Self {
            state,
            trace: vec![],
            delay: Duration::ZERO,
        }
    }
    fn answer(&self, request: ReadRequest, data: &Value) -> BatchReadResult {
        let kind = request.as_str();
        let name = data["inputName"].as_str().unwrap_or("");
        let failure = &self.state["failures"][format!("{kind}:{name}")];
        if let Some(code) = failure["code"].as_u64() {
            return BatchReadResult {
                ok: false,
                code: Some(code),
                comment: failure["comment"].as_str().map(str::to_owned),
                response_data: json!({}),
            };
        }
        let fail = |code, message: &str| BatchReadResult {
            ok: false,
            code: Some(code),
            comment: Some(message.to_owned()),
            response_data: json!({}),
        };
        let value = match request {
            ReadRequest::CurrentProgramScene => self
                .state
                .get("programReply")
                .cloned()
                .unwrap_or_else(|| json!({"currentProgramSceneName":self.state["program"]})),
            ReadRequest::InputList => {
                json!({"inputs":self.state["inputs"].as_object().unwrap().iter().map(|(name,input)|json!({"inputName":name,"inputKind":input["kind"]})).collect::<Vec<_>>()})
            }
            ReadRequest::SceneItemList => {
                let Some(items) =
                    self.state["scenes"][data["sceneName"].as_str().unwrap()].as_array()
                else {
                    return fail(600, "scene missing");
                };
                json!({"sceneItems":items.iter().enumerate().map(|(index,item)|json!({"sceneItemId":item["sceneItemId"],"sceneItemIndex":index,"sourceName":item["sourceName"],"sceneItemEnabled":item["enabled"],"sceneItemLocked":item["locked"],"sceneItemTransform":item["transform"]})).collect::<Vec<_>>()})
            }
            ReadRequest::InputSettings => {
                let input = &self.state["inputs"][name];
                if input.is_null() {
                    return fail(600, "input missing");
                }
                json!({"inputKind":input["kind"],"inputSettings":input["settings"]})
            }
            ReadRequest::InputMute | ReadRequest::InputVolume | ReadRequest::InputAudioTracks => {
                let input = &self.state["inputs"][name];
                if input.is_null() {
                    return fail(600, "input missing");
                }
                if input.get("muted").is_none() {
                    return fail(604, "audio unsupported");
                }
                match request {
                    ReadRequest::InputMute => json!({"inputMuted":input["muted"]}),
                    ReadRequest::InputVolume => json!({"inputVolumeMul":input["volumeMul"]}),
                    _ => json!({"inputAudioTracks":input["tracks"]}),
                }
            }
            _ => panic!("unexpected snapshot fixture read"),
        };
        BatchReadResult {
            ok: true,
            code: Some(100),
            comment: None,
            response_data: value,
        }
    }
}
impl ReadAdapter for Reader {
    fn supports(&self, _request: ReadRequest) -> Option<bool> {
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
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            let answer = self.answer(request, &data);
            if answer.ok {
                Ok(answer.response_data)
            } else {
                Err(ObservationError::rejected(
                    answer.code.unwrap(),
                    "evidence_unavailable",
                    answer.comment.unwrap_or_default(),
                ))
            }
        })
    }
}
impl SnapshotReader for Reader {
    fn batch<'a>(&'a mut self, calls: Vec<ReadCall>) -> SnapshotFuture<'a, Vec<BatchReadResult>> {
        Box::pin(async move {
            if calls.is_empty() {
                return Ok(vec![]);
            }
            self.trace.push(json!({"kind":"batch","requests":calls.iter().map(|call|json!({"requestType":call.request.as_str(),"requestData":call.data})).collect::<Vec<_>>()}));
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(calls
                .iter()
                .map(|call| self.answer(call.request, &call.data))
                .collect())
        })
    }
}
fn node(input: &Value, root: &Path) -> Value {
    let project = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let script = r#"
import crypto from "node:crypto";import{readFileSync,existsSync,readdirSync,utimesSync}from"node:fs";import{join}from"node:path";
import{readItems,readInputs,takeSnapshot,planRestore,initialize}from"./build/tools/snapshots.js";
import{McpServer,InMemoryTransport}from"@modelcontextprotocol/server";import{Client}from"@modelcontextprotocol/client";
const chunks=[];for await(const chunk of process.stdin)chunks.push(chunk);const input=JSON.parse(Buffer.concat(chunks));
const RealDate=Date,epoch=RealDate.parse("2026-10-31T12:00:00.000Z");globalThis.Date=class extends RealDate{constructor(...args){super(...(args.length?args:[epoch]));}static now(){return epoch;}};
crypto.randomUUID=()=>"12345678-1234-4234-8234-123456789abc";
const state=input.state,trace=[];
function answer(type,data={}){const name=data.inputName??"";const failure=state.failures?.[`${type}:${name}`];if(failure)throw failure;
switch(type){case"GetCurrentProgramScene":return state.programReply??{currentProgramSceneName:state.program};case"GetInputList":return{inputs:Object.entries(state.inputs).map(([inputName,input])=>({inputName,inputKind:input.kind}))};case"GetSceneItemList":{const items=state.scenes[data.sceneName];if(!items)throw{code:600,comment:"scene missing"};return{sceneItems:items.map((item,index)=>({sceneItemId:item.sceneItemId,sceneItemIndex:index,sourceName:item.sourceName,sceneItemEnabled:item.enabled,sceneItemLocked:item.locked,sceneItemTransform:item.transform}))};}default:{const input=state.inputs[name];if(!input)throw{code:600,comment:"input missing"};if(type==="GetInputSettings")return{inputKind:input.kind,inputSettings:input.settings};if(input.muted===undefined)throw{code:604,comment:"audio unsupported"};if(type==="GetInputMute")return{inputMuted:input.muted};if(type==="GetInputVolume")return{inputVolumeMul:input.volumeMul};if(type==="GetInputAudioTracks")return{inputAudioTracks:input.tracks};throw Error("unexpected fixture method");}}
}
const client={getConnectionStatus(){return{url:input.endpoint??"ws://127.0.0.1:4455"}},async sendRequest(type,data){trace.push({kind:"single",requestType:type,requestData:data??null});try{return structuredClone(answer(type,data));}catch(error){throw Error(error.comment??"fixture read failed")}},async sendBatch(requests){if(!requests.length)return[];trace.push({kind:"batch",requests});return requests.map(({requestType,requestData})=>{try{return{requestType,ok:true,code:100,responseData:structuredClone(answer(requestType,requestData))}}catch(error){return{requestType,ok:false,code:error.code,comment:error.comment,responseData:{}}}})}};
try{let result;if(input.action==="readInputs")result=await readInputs(client,input.names,input.required??[]);else if(input.action==="readItems")result=await readItems(client,input.scenes);else if(input.action==="plan")result=await planRestore(client,input.snapshot);else if(input.action==="list"){const server=new McpServer({name:"snapshot-oracle",version:"1"});initialize(server,client);const reader=new Client({name:"consumer",version:"1"});const[a,b]=InMemoryTransport.createLinkedPair();try{await Promise.all([server.connect(a),reader.connect(b)]);result=(await reader.callTool({name:"obs-snapshot",arguments:{list:true}})).structuredContent;}finally{await reader.close();await server.close();}}else{if(input.staleLock)utimesSync(join(process.env.OBS_MCP_STATE_DIR,"snapshots.json.lock"),new RealDate(epoch-20000),new RealDate(epoch-20000));result=await takeSnapshot(client,input.options??{});}
console.log(JSON.stringify({result,trace,history:existsSync(join(process.env.OBS_MCP_STATE_DIR,"snapshots.json"))?JSON.parse(readFileSync(join(process.env.OBS_MCP_STATE_DIR,"snapshots.json"),"utf8")):null,files:readdirSync(process.env.OBS_MCP_STATE_DIR)}));}catch(error){console.log(JSON.stringify({error:error.message,trace,files:readdirSync(process.env.OBS_MCP_STATE_DIR)}));}
"#;
    let mut child = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(script)
        .current_dir(project)
        .env("OBS_MCP_STATE_DIR", root)
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
    assert!(output.status.success(), "snapshot oracle failed");
    written.unwrap();
    serde_json::from_slice(&output.stdout).unwrap()
}
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
            for (key, value) in a {
                assert_json(value, &b[key])
            }
        }
        _ => assert_eq!(actual, expected),
    }
}
fn metadata() -> CaptureMetadata {
    CaptureMetadata {
        id: "12345678".to_owned(),
        obs_url: URL.to_owned(),
        taken_at: NOW.to_owned(),
    }
}
async fn capture(state: Value, options: CaptureOptions) -> (Snapshot, Vec<Value>) {
    let mut reader = Reader::new(state);
    let mut writer = Memory::default();
    let snapshot = take_snapshot(
        &mut reader,
        &mut writer,
        options,
        metadata(),
        Instant::now() + Duration::from_secs(12),
    )
    .await
    .unwrap();
    assert_eq!(writer.saved.len(), 1);
    (snapshot, reader.trace)
}
fn private_write(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[tokio::test]
async fn captures_default_and_explicit_selections_with_exact_fields_batches_and_private_format() {
    for variant in [
        "default",
        "inputs",
        "empty-scenes",
        "label",
        "fallback-program",
    ] {
        let scratch = Scratch::new();
        let mut data = state();
        let mut options = CaptureOptions::default();
        let mut source_options = json!({});
        match variant {
            "inputs" => {
                options.inputs = Some(vec!["Chrome".to_owned()]);
                source_options["inputs"] = json!(["Chrome"]);
            }
            "empty-scenes" => {
                options.scenes = Some(vec![]);
                source_options["scenes"] = json!([]);
            }
            "label" => {
                options.label = Some("before capture".to_owned());
                options.auto = true;
                options.scene_collection = Some("collection".to_owned());
                source_options =
                    json!({"label":"before capture","auto":true,"sceneCollection":"collection"});
                data["scenes"]["Demo"][1]["transform"]["cropToBounds"] = json!(true);
            }
            "fallback-program" => data["programReply"] = json!({"sceneName":"Demo"}),
            _ => {}
        }
        let (snapshot, trace) = capture(data.clone(), options).await;
        let expected = node(
            &json!({"action":"capture","state":data,"options":source_options}),
            &scratch.0,
        );
        assert_json(
            &serde_json::to_value(&snapshot).unwrap(),
            &expected["result"],
        );
        assert_json(&json!(trace), &expected["trace"]);
        assert_json(&json!([snapshot]), &expected["history"]);
    }
}

#[tokio::test]
async fn optional_audio_unsupported_is_distinct_from_failed_and_required_observation_refuses() {
    for required in [false, true] {
        let scratch = Scratch::new();
        let mut data = state();
        data["failures"]["GetInputVolume:Background"] = json!({"code":500,"comment":"read failed"});
        let names = vec!["Background".to_owned(), "Chrome".to_owned()];
        let mut reader = Reader::new(data.clone());
        let fields = if required {
            vec![RequiredInputField::Muted]
        } else {
            vec![]
        };
        let actual = read_inputs(&mut reader, &names, &fields).await;
        let expected = node(
            &json!({"action":"readInputs","state":data,"names":names,"required":if required{vec!["muted"]}else{vec![]}}),
            &scratch.0,
        );
        if required {
            assert_eq!(
                actual.unwrap_err().tool_message(),
                expected["error"].as_str().unwrap()
            );
        } else {
            let values = serde_json::to_value(actual.unwrap()).unwrap();
            assert_json(&values, &expected["result"]);
            assert_eq!(
                values[0]["unavailable"],
                json!({"muted":"unsupported","volumeMul":"failed","audioTracks":"unsupported"})
            );
        }
        assert_json(&json!(reader.trace), &expected["trace"]);
    }
}

#[tokio::test]
async fn required_settings_and_persistence_failures_never_publish_an_unsaved_capture() {
    let mut data = state();
    data["failures"]["GetInputSettings:Chrome"] = json!({"code":600,"comment":"unavailable"});
    let mut reader = Reader::new(data);
    let mut writer = Memory::default();
    let result = take_snapshot(
        &mut reader,
        &mut writer,
        CaptureOptions {
            inputs: Some(vec!["Chrome".to_owned()]),
            required_input_fields: vec![RequiredInputField::InputSettings],
            ..Default::default()
        },
        metadata(),
        Instant::now() + Duration::from_secs(12),
    )
    .await;
    assert!(result.is_err());
    assert!(writer.saved.is_empty());
    let mut reader = Reader::new(state());
    let mut writer = Memory {
        fail: true,
        ..Default::default()
    };
    assert!(matches!(
        take_snapshot(
            &mut reader,
            &mut writer,
            CaptureOptions::default(),
            metadata(),
            Instant::now() + Duration::from_secs(12)
        )
        .await,
        Err(SnapshotError::HistoryUnavailable)
    ));
    assert!(writer.saved.is_empty());
}

#[tokio::test]
async fn dropped_capture_has_no_detached_publication() {
    let mut reader = Reader::new(state());
    reader.delay = Duration::from_millis(50);
    let mut writer = Memory::default();
    let result = tokio::time::timeout(
        Duration::from_millis(5),
        take_snapshot(
            &mut reader,
            &mut writer,
            CaptureOptions::default(),
            metadata(),
            Instant::now() + Duration::from_secs(12),
        ),
    )
    .await;
    assert!(result.is_err());
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(writer.saved.is_empty());
    assert_eq!(reader.trace.len(), 1);
}

#[tokio::test]
async fn restore_plans_match_manual_source_and_preserve_added_item_positions() {
    let (snapshot, _) = capture(state(), CaptureOptions::default()).await;
    for variant in [
        "matches",
        "changed",
        "removed-added",
        "deleted-scene",
        "half-pixel",
        "replacement",
    ] {
        let scratch = Scratch::new();
        let mut now = state();
        let mut saved = snapshot.clone();
        match variant {
            "changed" => {
                now["inputs"]["Chrome"]["settings"] = json!({"type":2,"application":"com.fixture"});
                now["inputs"]["Chrome"]["muted"] = json!(false);
                now["inputs"]["Chrome"]["volumeMul"] = json!(0.5);
                now["inputs"]["Chrome"]["tracks"]["1"] = json!(true);
                now["scenes"]["Demo"][1]["transform"]["positionX"] = json!(0);
                now["scenes"]["Demo"][0]["enabled"] = json!(false);
                now["scenes"]["Demo"][0]["locked"] = json!(false);
                now["scenes"]["Demo"].as_array_mut().unwrap().reverse();
            }
            "removed-added" => {
                now["scenes"]["Demo"].as_array_mut().unwrap().remove(0);
                let mut added = now["scenes"]["Demo"][0].clone();
                added["sceneItemId"] = json!(3);
                added["sourceName"] = json!("Webcam");
                now["scenes"]["Demo"]
                    .as_array_mut()
                    .unwrap()
                    .insert(0, added);
            }
            "deleted-scene" => {
                let mut deleted = saved.items[0].clone();
                deleted.scene_name = "Deleted scene".to_owned();
                saved.items.push(deleted);
            }
            "half-pixel" => now["scenes"]["Demo"][1]["transform"]["positionX"] = json!(204.5),
            "replacement" => {
                now["scenes"]["Demo"][1]["sourceName"] = json!("Replaced source");
                now["scenes"]["Demo"][1]["transform"]["positionX"] = json!(12);
            }
            _ => {}
        }
        let mut reader = Reader::new(now.clone());
        let actual = plan_restore(&mut reader, &saved).await.unwrap();
        let expected = node(
            &json!({"action":"plan","state":now,"snapshot":saved}),
            &scratch.0,
        );
        assert_json(&serde_json::to_value(&actual).unwrap(), &expected["result"]);
        assert_json(&json!(reader.trace), &expected["trace"]);
        if variant == "replacement" {
            assert!(
                actual.changes.iter().any(|change| matches!(
                    change.mutation,
                    Mutation::ItemTransform { item_id: 2.0, .. }
                )),
                "legacy manual plan does not prove replacement identity or preserve conflicting transform edits"
            );
        }
        if variant == "removed-added" {
            assert!(
                actual
                    .missing
                    .iter()
                    .any(|note| note.contains("was added since; it is left in place"))
            );
        }
    }
}

#[tokio::test]
async fn private_file_roundtrip_endpoint_filtering_and_source_readback_are_compatible() {
    let scratch = Scratch::new();
    let (snapshot, _) = capture(state(), CaptureOptions::default()).await;
    let mut owner = SnapshotFile::new(&scratch.0, CAPACITY).unwrap();
    assert!(owner.load().unwrap().is_empty());
    owner.save(&snapshot).unwrap();
    assert_eq!(owner.load().unwrap(), vec![snapshot.clone()]);
    let expected = node(&json!({"action":"list","state":state()}), &scratch.0);
    assert_eq!(expected["result"]["snapshots"][0]["id"], snapshot.id);
    assert_eq!(expected["result"]["snapshots"][0]["inputs"], 2);
    let mut foreign = snapshot.clone();
    foreign.id = "foreign".to_owned();
    foreign.obs_url = Some("ws://127.0.0.1:4456".to_owned());
    owner.save(&foreign).unwrap();
    let mut legacy = snapshot.clone();
    legacy.id = "legacy".to_owned();
    legacy.obs_url = None;
    owner.save(&legacy).unwrap();
    let selected = owner
        .for_endpoint(URL)
        .unwrap()
        .iter()
        .map(|snapshot| snapshot.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(selected, vec![snapshot.id, "legacy".to_owned()]);
    let expected = node(&json!({"action":"list","state":state()}), &scratch.0);
    let source_ids = expected["result"]["snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .map(|snapshot| snapshot["id"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(selected, source_ids);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(scratch.file()).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert_eq!(fs::read_dir(&scratch.0).unwrap().count(), 1);
}

#[tokio::test]
async fn automatic_retention_cannot_evict_manual_history_and_unknown_extensions_survive() {
    let scratch = Scratch::new();
    let (base, _) = capture(state(), CaptureOptions::default()).await;
    let mut owner = SnapshotFile::new(&scratch.0, CAPACITY).unwrap();
    for index in 0..45 {
        let mut snapshot = base.clone();
        snapshot.id = format!("snapshot-{index}");
        snapshot.auto = Some(index % 2 == 0);
        snapshot
            .extra
            .insert("futureField".to_owned(), json!({"retained":true}));
        owner.save(&snapshot).unwrap();
    }
    let retained = owner.load().unwrap();
    assert_eq!(
        retained
            .iter()
            .map(|snapshot| snapshot.id.clone())
            .collect::<Vec<_>>(),
        (0..45)
            .filter(|index| index % 2 == 0 && *index >= 26 || index % 2 == 1 && *index >= 5)
            .map(|index| format!("snapshot-{index}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        retained
            .iter()
            .filter(|snapshot| snapshot.auto == Some(true))
            .count(),
        10
    );
    assert_eq!(
        retained
            .iter()
            .filter(|snapshot| snapshot.auto != Some(true))
            .count(),
        20
    );
    assert!(
        retained
            .iter()
            .all(|snapshot| snapshot.extra["futureField"] == json!({"retained":true}))
    );
    let expected = node(&json!({"action":"list","state":state()}), &scratch.0);
    assert_eq!(
        expected["result"]["snapshots"].as_array().unwrap().len(),
        30
    );
    let source = Scratch::new();
    private_write(&source.file(), &serde_json::to_vec(&retained).unwrap());
    owner.save(&base).unwrap();
    let expected = node(&json!({"action":"capture","state":state()}), &source.0);
    assert_json(
        &serde_json::to_value(owner.load().unwrap()).unwrap(),
        &expected["history"],
    );
}

#[tokio::test]
async fn corruption_and_unproven_stale_lock_refusals_have_real_legacy_counterexamples() {
    let scratch = Scratch::new();
    let (snapshot, _) = capture(state(), CaptureOptions::default()).await;
    private_write(&scratch.file(), b"{ not json");
    let mut owner = SnapshotFile::new(&scratch.0, CAPACITY)
        .unwrap()
        .with_lock_wait(Duration::ZERO);
    assert_eq!(owner.load(), Err(SnapshotError::HistoryCorrupt));
    assert_eq!(owner.save(&snapshot), Err(SnapshotError::HistoryCorrupt));
    assert_eq!(fs::read(scratch.file()).unwrap(), b"{ not json");
    assert_eq!(fs::read_dir(&scratch.0).unwrap().count(), 1);
    let legacy = Scratch::new();
    private_write(&legacy.file(), b"{ not json");
    let result = node(&json!({"action":"capture","state":state()}), &legacy.0);
    assert!(result.get("error").is_none());
    assert!(result["files"].as_array().unwrap().iter().any(|file| {
        file.as_str()
            .unwrap()
            .starts_with("snapshots.json.unreadable-")
    }));
    let locked = Scratch::new();
    private_write(&locked.0.join("snapshots.json.lock"), b"unproven owner");
    let mut owner = SnapshotFile::new(&locked.0, CAPACITY)
        .unwrap()
        .with_lock_wait(Duration::ZERO);
    assert_eq!(owner.save(&snapshot), Err(SnapshotError::LockBusy));
    assert_eq!(
        fs::read(locked.0.join("snapshots.json.lock")).unwrap(),
        b"unproven owner"
    );
    let result = node(
        &json!({"action":"capture","state":state(),"staleLock":true}),
        &locked.0,
    );
    assert!(
        result.get("error").is_none(),
        "legacy source breaks lock by age without proving owner exit"
    );
}

#[tokio::test]
async fn established_missing_replaced_capacity_and_symlinks_are_not_empty_history() {
    let (base, _) = capture(state(), CaptureOptions::default()).await;
    for variant in ["missing", "replaced", "capacity"] {
        let scratch = Scratch::new();
        let mut owner = SnapshotFile::new(&scratch.0, CAPACITY).unwrap();
        owner.save(&base).unwrap();
        if variant == "missing" {
            fs::remove_file(scratch.file()).unwrap();
            assert_eq!(owner.load(), Err(SnapshotError::HistoryMissing));
        } else if variant == "replaced" {
            let replacement = scratch.0.join("replacement");
            private_write(
                &replacement,
                &serde_json::to_vec(&vec![base.clone()]).unwrap(),
            );
            fs::rename(replacement, scratch.file()).unwrap();
            assert_eq!(owner.load(), Err(SnapshotError::HistoryReplaced));
        } else {
            let mut small = SnapshotFile::new(&scratch.0, 8).unwrap();
            assert_eq!(small.load(), Err(SnapshotError::Capacity));
        }
    }
    let scratch = Scratch::new();
    assert!(matches!(
        SnapshotFile::new(scratch.0.join("missing-root"), CAPACITY),
        Err(SnapshotError::MissingRoot)
    ));
    assert!(!scratch.0.join("missing-root").exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        private_write(&scratch.0.join("outside"), b"[]");
        symlink(scratch.0.join("outside"), scratch.file()).unwrap();
        assert!(matches!(
            SnapshotFile::new(&scratch.0, CAPACITY),
            Err(SnapshotError::UnsafePath)
        ));
        assert_eq!(fs::read(scratch.0.join("outside")).unwrap(), b"[]");
    }
}

#[tokio::test]
async fn diagnostic_debug_cannot_materialize_settings_endpoints_or_peer_comments() {
    let (mut snapshot, _) = capture(state(), CaptureOptions::default()).await;
    let secret = "fixture-sensitive-value";
    snapshot.obs_url = Some(format!("ws://fixture:{secret}@localhost"));
    snapshot.inputs[0]
        .input_settings
        .insert("password".to_owned(), json!(secret));
    let mutation = Mutation::InputSettings {
        input_name: secret.to_owned(),
        settings: snapshot.inputs[0].input_settings.clone(),
    };
    let result = BatchReadResult {
        ok: false,
        code: Some(500),
        comment: Some(secret.to_owned()),
        response_data: json!({"token":secret}),
    };
    let diagnostic = format!(
        "{snapshot:?} {:?} {mutation:?} {result:?} {:?}",
        snapshot.inputs[0],
        SnapshotError::Read(secret.to_owned())
    );
    assert!(!diagnostic.contains(secret));
}
