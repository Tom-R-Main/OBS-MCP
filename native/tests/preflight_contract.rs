// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{
    observation::ReadRequest,
    preflight::{
        Adapter, BatchResult, Call, ConnectionContext, Options, ReadError, ReadFuture,
        WritableDirectoryProbe, recorded_tracks, renders_no_video, run_preflight,
    },
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, process::Command, sync::Mutex};

fn healthy() -> Value {
    json!({
        "url":"ws://localhost:4455","platform":"macos",
        "profile":{"Output/Mode":"Simple","SimpleOutput/RecQuality":"Small","SimpleOutput/RecEncoder":"apple_h264","SimpleOutput/RecTracks":"1","SimpleOutput/RecFormat2":"mkv"},
        "responses":{
            "GetRecordStatus":{"outputActive":false},
            "GetRecordDirectory":{"recordDirectory":"/fixture/recordings"},
            "GetStats":{"availableDiskSpace":50000},
            "GetInputList":{"inputs":[{"inputName":"Chrome","inputKind":"screen_capture"},{"inputName":"Background","inputKind":"color_source_v3"}]},
            "GetCurrentProgramScene":{"currentProgramSceneName":"Demo"},
            "GetSceneItemList":{"sceneItems":[{"sceneItemId":1,"sourceName":"Background","inputKind":"color_source_v3","sceneItemEnabled":true,"sceneItemTransform":{"sourceWidth":1920,"sourceHeight":1080}},{"sceneItemId":2,"sourceName":"Chrome","inputKind":"screen_capture","sceneItemEnabled":true,"sceneItemTransform":{"sourceWidth":1512,"sourceHeight":949}}]}
        },
        "batchResults":{
            "GetInputMute:Chrome":{"ok":true,"responseData":{"inputMuted":true}},
            "GetInputAudioTracks:Chrome":{"ok":true,"responseData":{"inputAudioTracks":{"1":false}}},
            "GetInputMute:Background":{"ok":false,"code":604},
            "GetInputAudioTracks:Background":{"ok":true,"responseData":{"inputAudioTracks":{}}}
        },
        "options":{"minFreeDiskMb":1024,"expectSilent":false,"requireVerified":false},
        "host":{}
    })
}
fn oracle(input: &Value) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let output=Command::new("node").arg("--input-type=module").arg("-e").arg(r#"
import fs from 'node:fs';
import {syncBuiltinESMExports} from 'node:module';
const input=JSON.parse(process.env.PREFLIGHT_TEST_INPUT),trace=[],hostCalls=[];
const stat=fs.statSync,access=fs.accessSync;
fs.statSync=(path,...rest)=>{
  if(path!=='/fixture/recordings')return stat(path,...rest);
  hostCalls.push({kind:'stat',path});
  if(input.host?.statError)throw Error(input.host.statError);
  return {isDirectory:()=>input.host?.isDirectory!==false};
};
fs.accessSync=(path,mode)=>{
  if(path!=='/fixture/recordings')return access(path,mode);
  hostCalls.push({kind:'access',path,write:mode===fs.constants.W_OK});
  if(input.host?.accessError)throw Error(input.host.accessError);
};
syncBuiltinESMExports();
const {runPreflight,recordedTracks,initialize}=await import('./build/tools/preflight.js');
const {rendersNoVideo}=await import('./build/tools/audio-only.js');
const key=(type,data)=>type+':'+(data?.inputName??data?.sceneItemId??'');
function reply(type,data){
  if(type==='GetProfileParameter'){
    const name=data.parameterCategory+'/'+data.parameterName;
    const response=input.responses?.['GetProfileParameter:'+name]??{parameterValue:input.profile?.[name]??null,defaultParameterValue:null};
    if(response?.error)throw Error(response.error);return structuredClone(response);
  }
  const response=input.responses?.[key(type,data)]??input.responses?.[type]??null;
  if(response?.error)throw Error(response.error);return structuredClone(response);
}
const client={
  async connect(){trace.push({kind:'connect'});if(input.connectError)throw Error(input.connectError);},
  getConnectionStatus(){return {url:input.url,versionInfo:{platform:input.platform}}},
  hasPendingOutputSettings(){return input.pending===true},
  async sendRequest(type,data){trace.push({kind:'read',request:type,data:data??null});return reply(type,data);},
  async sendBatch(calls){
    trace.push({kind:'batch',calls:calls.map(call=>({request:call.requestType,data:call.requestData}))});
    const group=calls[0]?.requestType??'empty';
    if(input.batchErrors?.[group])throw Error(input.batchErrors[group]);
    const results=calls.map(call=>structuredClone(input.batchResults?.[key(call.requestType,call.requestData)]??{ok:false,code:600}));
    return input.batchLimits?.[group]===undefined?results:results.slice(0,input.batchLimits[group]);
  }
};
let hostname=null;try{hostname=new URL(input.url).hostname}catch{}
let result;
if(input.mode==='tracks'){
  try{result={tracks:await recordedTracks(client,input.verified===true)}}catch(error){result={error:error.message}}
}else if(input.mode==='renders'){
  result=input.cases.map(item=>rendersNoVideo(item.kind,item.settings));
}else if(input.mode==='tool'){
  let handler;initialize({registerTool(_name,_definition,callback){handler=callback}},client);
  result=await handler(input.options);
}else result=await runPreflight(client,input.options);
console.log(JSON.stringify({result,trace,hostCalls,hostname}));
"#).current_dir(root).env("PREFLIGHT_TEST_INPUT",input.to_string()).output().unwrap();
    assert!(
        output.status.success(),
        "actual TypeScript preflight oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
struct Reader {
    input: Value,
    hostname: Option<String>,
    trace: Mutex<Vec<Value>>,
}
impl Reader {
    fn response(&self, request: ReadRequest, data: &Option<Value>) -> Result<Value, ReadError> {
        let name = request.as_str();
        let value = if request == ReadRequest::ProfileParameter {
            let data = data.as_ref().unwrap();
            let key = format!(
                "{}/{}",
                data["parameterCategory"].as_str().unwrap(),
                data["parameterName"].as_str().unwrap()
            );
            self.input["responses"].get(format!("GetProfileParameter:{key}")).cloned().unwrap_or_else(||json!({"parameterValue":self.input["profile"].get(&key).unwrap_or(&Value::Null),"defaultParameterValue":null}))
        } else {
            self.input["responses"]
                .get(call_key(request, data.as_ref().unwrap_or(&Value::Null)))
                .or_else(|| self.input["responses"].get(name))
                .cloned()
                .unwrap_or(Value::Null)
        };
        if let Some(error) = value.get("error").and_then(Value::as_str) {
            Err(ReadError::new(error))
        } else {
            Ok(value)
        }
    }
}
fn call_key(request: ReadRequest, data: &Value) -> String {
    let key = data.get("inputName").or_else(|| data.get("sceneItemId"));
    let key = match key {
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    };
    format!("{}:{key}", request.as_str())
}
impl Adapter for Reader {
    fn connect(&self) -> ReadFuture<'_, ()> {
        self.trace.lock().unwrap().push(json!({"kind":"connect"}));
        let result = self
            .input
            .get("connectError")
            .and_then(Value::as_str)
            .map_or(Ok(()), |error| Err(ReadError::new(error)));
        Box::pin(async move { result })
    }
    fn connection_context(&self) -> ConnectionContext {
        ConnectionContext {
            hostname: self.hostname.clone(),
            platform: self.input["platform"].as_str().map(str::to_owned),
            pending_output_settings: self.input["pending"] == true,
        }
    }
    fn read(&self, request: ReadRequest, data: Option<Value>) -> ReadFuture<'_, Value> {
        self.trace
            .lock()
            .unwrap()
            .push(json!({"kind":"read","request":request.as_str(),"data":data}));
        let result = self.response(request, &data);
        Box::pin(async move { result })
    }
    fn batch(&self, calls: Vec<Call>) -> ReadFuture<'_, Vec<BatchResult>> {
        self.trace.lock().unwrap().push(json!({"kind":"batch","calls":calls.iter().map(|call|json!({"request":call.request.as_str(),"data":call.data})).collect::<Vec<_>>()}));
        let group = calls.first().map_or("empty", |call| call.request.as_str());
        let result = if let Some(error) =
            self.input["batchErrors"].get(group).and_then(Value::as_str)
        {
            Err(ReadError::new(error))
        } else {
            let mut rows = calls
                .iter()
                .map(|call| {
                    let value = self.input["batchResults"]
                        .get(call_key(call.request, &call.data))
                        .cloned()
                        .unwrap_or(json!({"ok":false,"code":600}));
                    BatchResult {
                        ok: value["ok"] == true,
                        code: value["code"].as_i64(),
                        response_data: value.get("responseData").cloned().unwrap_or(Value::Null),
                    }
                })
                .collect::<Vec<_>>();
            if let Some(limit) = self.input["batchLimits"].get(group).and_then(Value::as_u64) {
                rows.truncate(limit as usize);
            }
            Ok(rows)
        };
        Box::pin(async move { result })
    }
}
struct Host {
    input: Value,
    calls: Mutex<Vec<Value>>,
}
impl WritableDirectoryProbe for Host {
    fn check(&self, path: &str) -> Result<(), String> {
        self.calls
            .lock()
            .unwrap()
            .push(json!({"kind":"stat","path":path}));
        if let Some(error) = self.input["statError"].as_str() {
            return Err(error.into());
        }
        if self.input["isDirectory"] == false {
            return Err("not a directory".into());
        }
        self.calls
            .lock()
            .unwrap()
            .push(json!({"kind":"access","path":path,"write":true}));
        if let Some(error) = self.input["accessError"].as_str() {
            return Err(error.into());
        }
        Ok(())
    }
}
fn options(input: &Value) -> Options {
    Options {
        min_free_disk_mb: input["minFreeDiskMb"].as_f64().unwrap_or(1024.0),
        expect_silent: input["expectSilent"] == true,
        require_verified: input["requireVerified"] == true,
    }
}
fn canonical_trace(trace: &[Value]) -> Value {
    // Independent Promise.all checks have no global transport-order contract.
    // Preserve repeat ordering per exact read key, every batch boundary and
    // ordered batch vector, and connect count. Do not sort inside a batch.
    let mut reads: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut batches: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut connects = 0;
    for entry in trace {
        match entry["kind"].as_str().unwrap() {
            "connect" => connects += 1,
            "read" => {
                let key = format!("{}:{}", entry["request"], entry["data"]);
                reads.entry(key).or_default().push(entry.clone());
            }
            "batch" => {
                let key = entry["calls"][0]["request"]
                    .as_str()
                    .unwrap_or("empty")
                    .to_owned();
                batches.entry(key).or_default().push(entry.clone());
            }
            _ => panic!("unknown trace entry"),
        }
    }
    json!({"connects":connects,"reads":reads,"batches":batches})
}
async fn compare(input: Value) -> Value {
    let expected = oracle(&input);
    let reader = Reader {
        input: input.clone(),
        hostname: expected["hostname"].as_str().map(str::to_owned),
        trace: Mutex::new(vec![]),
    };
    let host = Host {
        input: input["host"].clone(),
        calls: Mutex::new(vec![]),
    };
    let result = if input["mode"] == "tracks" {
        match recorded_tracks(&reader, input["verified"] == true).await {
            Ok(tracks) => json!({"tracks":tracks}),
            Err(error) => json!({"error":error.message}),
        }
    } else if input["mode"] == "renders" {
        json!(
            input["cases"]
                .as_array()
                .unwrap()
                .iter()
                .map(|case| renders_no_video(&case["kind"], case.get("settings")))
                .collect::<Vec<_>>()
        )
    } else {
        let report = run_preflight(&reader, &host, &options(&input["options"])).await;
        if input["mode"] == "tool" {
            report.tool_result()
        } else {
            serde_json::to_value(report).unwrap()
        }
    };
    assert_eq!(result, expected["result"], "report fixture: {input}");
    let trace = reader.trace.into_inner().unwrap();
    let expected_trace = expected["trace"].as_array().unwrap();
    if input["mode"] == "tracks" {
        assert_eq!(trace, *expected_trace, "ordered shared-parser reads");
    } else {
        assert_eq!(
            canonical_trace(&trace),
            canonical_trace(expected_trace),
            "request/batch trace fixture: {input}"
        );
    }
    assert_eq!(
        host.calls.into_inner().unwrap(),
        expected["hostCalls"].as_array().unwrap().clone(),
        "host probe calls"
    );
    result
}
fn by_id<'a>(report: &'a Value, id: &str) -> &'a Value {
    report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["id"] == id)
        .unwrap()
}

#[tokio::test]
async fn healthy_report_and_connect_failure_have_actual_source_projection() {
    let good = compare(healthy()).await;
    assert_eq!(good["ready"], true);
    assert_eq!(by_id(&good, "settings-dialog")["status"], "unknown");
    let mut bad = healthy();
    bad["connectError"] = json!("connection fixture unavailable");
    let result = compare(bad).await;
    assert_eq!(result["checks"].as_array().unwrap().len(), 1);
    assert_eq!(result["ready"], false);
}

#[tokio::test]
async fn source_registered_tool_text_and_structured_report_are_exact() {
    for failure in [false, true] {
        let mut input = healthy();
        input["mode"] = json!("tool");
        if failure {
            input["responses"]["GetRecordStatus"]["outputActive"] = json!(true);
        }
        compare(input).await;
    }
}

#[tokio::test]
async fn recording_directory_probe_and_remote_hostname_boundaries_match_source() {
    for host in [
        json!({"statError":"missing fixture directory"}),
        json!({"isDirectory":false}),
        json!({"accessError":"ACL denied"}),
        json!({}),
    ] {
        let mut input = healthy();
        input["host"] = host;
        compare(input).await;
    }
    for url in [
        "ws://127.0.0.1:4455",
        "ws://[::1]:4455",
        "ws://LOCALHOST:4455",
        "ws://127.1:4455",
        "ws://remote.example:4455",
        "not a URL",
    ] {
        let mut input = healthy();
        input["url"] = json!(url);
        input["host"] = json!({"accessError":"must not infer remote access"});
        compare(input).await;
    }
    for directory in [Value::Null, json!(""), json!(false)] {
        let mut input = healthy();
        input["responses"]["GetRecordDirectory"]["recordDirectory"] = directory;
        assert_eq!(
            by_id(&compare(input).await, "record-directory")["status"],
            "fail"
        );
    }
}

#[tokio::test]
async fn recording_unknown_and_disk_rounding_failure_thresholds_match_source() {
    for active in [json!(true), Value::Null, json!("false"), json!(false)] {
        let mut input = healthy();
        input["responses"]["GetRecordStatus"]["outputActive"] = active;
        compare(input).await;
    }
    for free in [
        Value::Null,
        json!("4096"),
        json!(1023.49),
        json!(1023.5),
        json!(1024),
        json!(-0.5),
        json!(1e22),
    ] {
        let mut input = healthy();
        input["responses"]["GetStats"]["availableDiskSpace"] = free;
        compare(input).await;
    }
}

#[tokio::test]
async fn encoder_platform_shared_stream_fallback_and_pending_restart_match_source() {
    for platform in ["macos", "linux", "windows", ""] {
        for encoder in [
            "nvenc",
            "QSV",
            "jim_nvenc",
            "com.apple.videotoolbox.videoencoder.ave.avc",
            "obs_x264",
            "",
        ] {
            let mut input = healthy();
            input["platform"] = json!(platform);
            input["profile"]["SimpleOutput/RecEncoder"] = json!(encoder);
            compare(input).await;
        }
    }
    let mut stream = healthy();
    stream["profile"]["SimpleOutput/RecQuality"] = json!("Stream");
    stream["profile"]["SimpleOutput/StreamEncoder"] = json!("apple_h264");
    stream["profile"]["SimpleOutput/RecEncoder"] = json!("nvenc");
    compare(stream).await;
    for recorded in [Value::Null, json!(""), json!("none"), json!("obs_x264")] {
        let mut input = healthy();
        input["platform"] = json!("linux");
        input["profile"]["Output/Mode"] = json!("Advanced");
        input["profile"]["AdvOut/RecEncoder"] = recorded;
        input["profile"]["AdvOut/Encoder"] = json!("obs_x264");
        input["profile"]["AdvOut/RecTracks"] = json!("1");
        input["pending"] = json!(true);
        compare(input).await;
    }
}

#[tokio::test]
async fn audio_mute_rejection_604_routing_and_expect_silent_match_source() {
    for verified in [false, true] {
        for silent in [false, true] {
            for mute in [
                json!({"ok":false,"code":604}),
                json!({"ok":false,"code":600}),
                json!({"ok":true,"responseData":null}),
                json!({"ok":true,"responseData":{"inputMuted":"false"}}),
                json!({"ok":true,"responseData":{"inputMuted":true}}),
                json!({"ok":true,"responseData":{"inputMuted":false}}),
            ] {
                let mut input = healthy();
                input["options"]["requireVerified"] = json!(verified);
                input["options"]["expectSilent"] = json!(silent);
                input["batchResults"]["GetInputMute:Chrome"] = mute;
                input["batchResults"]["GetInputAudioTracks:Chrome"] =
                    json!({"ok":false,"code":500});
                compare(input).await;
            }
        }
    }
    for route in [
        json!({"1":false}),
        json!({"1":true}),
        json!({"1":"true"}),
        json!({}),
        json!({"1":true,"2":true}),
    ] {
        let mut input = healthy();
        input["batchResults"]["GetInputMute:Chrome"] =
            json!({"ok":true,"responseData":{"inputMuted":false}});
        input["batchResults"]["GetInputAudioTracks:Chrome"] =
            json!({"ok":true,"responseData":{"inputAudioTracks":route}});
        compare(input).await;
    }
    let mut short = healthy();
    short["batchLimits"] = json!({"GetInputMute":1});
    compare(short).await;
}

#[tokio::test]
async fn incomplete_lists_missing_dimensions_empty_and_disabled_scenes_match_source() {
    for inputs in [
        Value::Null,
        json!([null]),
        json!([{}]),
        json!([{"inputName":""}]),
        json!([]),
    ] {
        let mut input = healthy();
        input["responses"]["GetInputList"]["inputs"] = inputs;
        compare(input).await;
    }
    for items in [
        Value::Null,
        json!([null]),
        json!([{}]),
        json!([]),
        json!([{"sceneItemEnabled":false,"sourceName":"hidden"}]),
        json!([{"sceneItemEnabled":true,"sceneItemTransform":{"sourceWidth":-1,"sourceHeight":0}}]),
        json!([{"sceneItemEnabled":true,"sceneItemId":"7"}]),
    ] {
        let mut input = healthy();
        input["responses"]["GetSceneItemList"]["sceneItems"] = items;
        compare(input).await;
    }
    for program in [
        json!({}),
        json!({"currentProgramSceneName":false,"sceneName":"fallback not selected"}),
        json!({"currentProgramSceneName":null,"sceneName":"fallback selected"}),
        json!({"currentProgramSceneName":""}),
    ] {
        let mut input = healthy();
        input["responses"]["GetCurrentProgramScene"] = program;
        compare(input).await;
    }
}

#[tokio::test]
async fn transform_batch_results_stay_attached_to_each_item_even_with_duplicate_ids() {
    let mut input = healthy();
    input["responses"]["GetSceneItemList"]["sceneItems"] = json!([
        {"sceneItemEnabled":true,"sceneItemId":7,"sourceName":"first","inputKind":"screen_capture"},
        {"sceneItemEnabled":true,"sceneItemId":7,"sourceName":"second","inputKind":"screen_capture"},
        {"sceneItemEnabled":false,"sceneItemId":8,"sourceName":"hidden"}
    ]);
    input["batchResults"]["GetSceneItemTransform:7"] =
        json!({"ok":true,"responseData":{"sceneItemTransform":{"sourceWidth":0,"sourceHeight":0}}});
    let report = compare(input.clone()).await;
    assert!(
        by_id(&report, "scene-items")["message"]
            .as_str()
            .unwrap()
            .contains("first, second")
    );
    input["batchLimits"] = json!({"GetSceneItemTransform":1});
    assert_eq!(
        by_id(&compare(input).await, "scene-items")["status"],
        "unknown"
    );
}

#[tokio::test]
async fn media_settings_audio_only_and_batch_failures_match_source() {
    for (kind, path) in [
        ("ffmpeg_source", "voice.MP3"),
        ("vlc_source", "sound.aiff"),
        ("ffmpeg_source", "movie.mkv"),
        ("ffmpeg_source", "sound.wav?query"),
        ("vlc_source", "sound.mp3\n"),
    ] {
        let mut input = healthy();
        input["responses"]["GetSceneItemList"]["sceneItems"] = json!([{"sceneItemEnabled":true,"sceneItemId":1,"sourceName":"media","inputKind":kind,"sceneItemTransform":{"sourceWidth":0,"sourceHeight":0}}]);
        input["batchResults"]["GetInputSettings:media"] =
            json!({"ok":true,"responseData":{"inputSettings":{"local_file":path}}});
        compare(input.clone()).await;
        input["batchErrors"] = json!({"GetInputSettings":"settings fixture unavailable"});
        compare(input).await;
    }
    for request in [
        "GetStats",
        "GetRecordStatus",
        "GetRecordDirectory",
        "GetInputList",
        "GetCurrentProgramScene",
        "GetSceneItemList",
        "GetProfileParameter:Output/Mode",
    ] {
        let mut input = healthy();
        input["responses"][request] = json!({"error":"read fixture failed"});
        compare(input).await;
    }
    for request in ["GetInputMute", "GetSceneItemTransform"] {
        let mut input = healthy();
        input["batchErrors"] = json!({request:"batch fixture failed"});
        if request == "GetSceneItemTransform" {
            input["responses"]["GetSceneItemList"]["sceneItems"][0]
                .as_object_mut()
                .unwrap()
                .remove("sceneItemTransform");
        }
        compare(input).await;
    }
}

#[tokio::test]
async fn recorded_track_masks_strict_admission_and_ordered_reads_match_actual_typescript() {
    for verified in [false, true] {
        for mask in [
            Value::Null,
            json!(""),
            json!(" "),
            json!("\u{feff}3\u{feff}"),
            json!("\u{85}3\u{85}"),
            json!("0"),
            json!("1"),
            json!("2"),
            json!("63"),
            json!("64"),
            json!("-1"),
            json!("-1.5"),
            json!("3.9"),
            json!("0x3"),
            json!("0b10"),
            json!("0o7"),
            json!("1e1"),
            json!("NaN"),
            json!("Infinity"),
            json!("18446744073709551615"),
            json!("0x200000000000011"),
            json!("0x200000000000031"),
            json!("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"),
        ] {
            let mut input = healthy();
            input["mode"] = json!("tracks");
            input["verified"] = json!(verified);
            input["profile"]["SimpleOutput/RecTracks"] = mask;
            compare(input).await;
        }
        for mode in [Value::Null, json!(""), json!("unknown"), json!("Advanced")] {
            let mut input = healthy();
            input["mode"] = json!("tracks");
            input["verified"] = json!(verified);
            input["profile"]["Output/Mode"] = mode;
            compare(input).await;
        }
        for record_type in [
            Value::Null,
            json!("unknown"),
            json!("Standard"),
            json!("FFmpeg"),
        ] {
            let mut input = healthy();
            input["mode"] = json!("tracks");
            input["verified"] = json!(verified);
            input["profile"]["Output/Mode"] = json!("Advanced");
            input["profile"]["AdvOut/RecType"] = record_type;
            input["profile"]["AdvOut/RecTracks"] = json!("2");
            input["profile"]["AdvOut/FFAudioMixes"] = json!("4");
            compare(input).await;
        }
        for (quality, format) in [
            (Value::Null, Value::Null),
            (json!("Stream"), Value::Null),
            (Value::Null, json!("flv")),
            (json!(""), json!("mkv")),
        ] {
            let mut input = healthy();
            input["mode"] = json!("tracks");
            input["verified"] = json!(verified);
            input["profile"]["SimpleOutput/RecQuality"] = quality;
            input["profile"]["SimpleOutput/RecFormat2"] = format;
            compare(input).await;
        }
    }
}

#[tokio::test]
async fn audio_only_classifier_matches_actual_regex_boundaries() {
    let mut cases = vec![];
    for kind in [
        "coreaudio_input_capture",
        "wasapi_output_capture",
        "process_output_capture",
        "input_capture",
        "output_capture",
        "sck_audio_capture",
        "jack_device",
        "JACK_device",
        "screen_capture",
        "capture",
        "ffmpeg_source",
        "vlc_source",
        "ffmpeg_source_v2",
    ] {
        for path in [
            "voice.wav",
            "voice.aif",
            "voice.aiff",
            "voice.CAF",
            "clip.mp4",
            "voice.mp3\n",
            "voice.mp3\r\n",
            "voice.mp3\u{2028}",
            "voice.mp3\n\n",
            "voice.mp3?token",
            "voice.wav ",
        ] {
            cases.push(json!({"kind":kind,"settings":{"local_file":path}}));
        }
    }
    cases.extend([
        json!({"kind":null}),
        json!({"kind":[]}),
        json!({"kind":"ffmpeg_source","settings":{"local_file":false}}),
    ]);
    let mut input = healthy();
    input["mode"] = json!("renders");
    input["cases"] = json!(cases);
    compare(input).await;
}
