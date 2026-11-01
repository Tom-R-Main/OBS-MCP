// SPDX-License-Identifier: GPL-2.0-only
//! Readback oracle: invoke actual Source callbacks through the pinned SDK.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::session::{ObsSession, SessionOptions};
use obs_mcp_native::{
    custom_mutations::{Request, SessionReadback},
    observation::{ObservationError, ReadAdapter, ReadFuture, ReadRequest},
    scene_audit::ScreenshotLimit,
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    io::Write,
    process::{Command, Stdio},
    time::Duration,
};
use tokio::net::TcpListener;
use tokio::time::Instant;
use tokio_tungstenite::{accept_async, tungstenite::Message};

const ORACLE: &str = r#"
import {McpServer, InMemoryTransport} from '@modelcontextprotocol/server';
import {Client} from '@modelcontextprotocol/client';
import {EventEmitter} from 'node:events';
import {initialize} from './build/tools/index.js';
import {OBSRequestError} from './build/client.js';
import {createHash} from 'node:crypto';
let text='';for await (const chunk of process.stdin) text+=chunk;
const {name,args,spec}=JSON.parse(text), trace=[];
let samples=0;
const obs = new EventEmitter();
Object.assign(obs, {
  sendRequest: async (requestType,requestData) => {
    trace.push({requestType,...(requestData===undefined?{}:{requestData})});
    if(spec.fail?.[requestType]) throw new OBSRequestError(requestType,500,spec.fail[requestType]);
    switch(requestType) {
      case 'CreateInput': return spec.created ?? {sceneItemId:7};
      case 'SetInputSettings': return {};
      case 'SetSourceFilterIndex': return {};
      case 'GetInputSettings': return Object.hasOwn(spec,'settings')?spec.settings:{inputKind:'screen_capture',inputSettings:{type:1,window:1234}};
      case 'GetCurrentProgramScene': return Object.hasOwn(spec,'program')?spec.program:{currentProgramSceneName:'Demo'};
      case 'GetSceneItemList': { const lists=spec.lists??[{sceneItems:[{sceneItemId:2,sourceName:'Fixture',sceneItemTransform:{sourceWidth:1512,sourceHeight:949}}]}]; return lists[Math.min(samples++,lists.length-1)]; }
      case 'GetSourceScreenshot': return spec.screenshot ?? {imageData:'data:image/jpeg;base64,YWJjZA=='};
      case 'SetSceneItemTransform': return {};
      case 'GetSceneItemTransform': return {sceneItemTransform:{z:3,a:1}};
      default: throw new Error('Unexpected '+requestType);
    }
  },
  isConnected:()=>true,
  getConnectionStatus:()=>({generation:1,identified:true,url:'ws://localhost:4455',hasPassword:false}),
  supportsRequest:()=>true,
});
const server=new McpServer({name:'custom-input-source',version:'1'});
initialize(server,obs,{confirmLive:false,resourcesAndPrompts:false,sessionNotifications:false});
const client=new Client({name:'oracle',version:'1'}),[a,b]=InMemoryTransport.createLinkedPair();
try { await Promise.all([server.connect(a),client.connect(b)]); const result=await client.callTool({name,arguments:args}); const numericJson=typeof args.filterIndex==='number'?JSON.stringify(args.filterIndex):undefined; console.log(JSON.stringify({result,trace,numericJson,...(numericJson===undefined?{}:{numericDigest:createHash('sha256').update(numericJson).digest('hex')})})); }
finally { await client.close(); await server.close(); }
"#;

fn source(name: &str, args: &Value, spec: &Value) -> Value {
    let root = std::env::var_os("OBS_CUSTOM_SOURCE_ROOT").unwrap_or_else(|| {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .into_os_string()
    });
    let mut child = Command::new("node")
        .args(["--input-type=module", "-e", ORACLE])
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
        .write_all(&serde_json::to_vec(&json!({"name":name,"args":args,"spec":spec})).unwrap())
        .unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(
        result.status.success(),
        "Source oracle: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}
struct Reader {
    spec: Value,
    trace: Vec<Value>,
    samples: usize,
}
impl ReadAdapter for Reader {
    fn supports(&self, _: ReadRequest) -> Option<bool> {
        Some(true)
    }
    fn read<'a>(&'a mut self, request: ReadRequest, data: Value, _: Instant) -> ReadFuture<'a> {
        Box::pin(async move {
            let name = request.as_str();
            self.trace.push(if data.is_null() {
                json!({"requestType":name})
            } else {
                json!({"requestType":name,"requestData":data})
            });
            if let Some(message) = self.spec["fail"][name].as_str() {
                return Err(ObservationError::rejected(
                    500,
                    "fixture",
                    format!("OBS request {name} failed with code 500: {message}"),
                ));
            }
            Ok(match request {
                ReadRequest::InputSettings => self.spec.get("settings").cloned().unwrap_or_else(|| json!({"inputKind":"screen_capture","inputSettings":{"type":1,"window":1234}})),
                ReadRequest::CurrentProgramScene => self.spec.get("program").cloned().unwrap_or_else(|| json!({"currentProgramSceneName":"Demo"})),
                ReadRequest::SceneItemList => {
                    let defaults = json!([{"sceneItems":[{"sceneItemId":2,"sourceName":"Fixture","sceneItemTransform":{"sourceWidth":1512,"sourceHeight":949}}]}]);
                    let lists = self.spec.get("lists").unwrap_or(&defaults).as_array().unwrap();
                    let result = lists[self.samples.min(lists.len()-1)].clone(); self.samples+=1; result
                }
                ReadRequest::SourceScreenshot => self.spec.get("screenshot").cloned().unwrap_or_else(|| json!({"imageData":"data:image/jpeg;base64,YWJjZA=="})),
                _ => panic!("Unexpected read: {request:?}"),
            })
        })
    }
}
async fn compare(name: &str, args: Value, spec: Value) -> Value {
    let expected = source(name, &args, &spec);
    let request = Request::parse(name, &args).unwrap();
    let mut reader = Reader {
        spec: spec.clone(),
        trace: vec![
            json!({"requestType":request.kind().request_type(),"requestData":request.data()}),
        ],
        samples: 0,
    };
    let response = spec
        .get("created")
        .cloned()
        .unwrap_or_else(|| json!({"sceneItemId":7}));
    let (data, follow_up) = request.into_parts();
    let result = follow_up
        .finish(
            &data,
            &response,
            &mut reader,
            Ok(ScreenshotLimit::default()),
            &Cancellation::new(),
            Instant::now() + Duration::from_secs(3),
        )
        .await;
    assert_eq!(result, expected["result"], "{name} args={args} spec={spec}");
    assert_eq!(
        reader.trace,
        *expected["trace"].as_array().unwrap(),
        "{name} exact ordered effects"
    );
    result
}
fn create() -> Value {
    json!({"sceneName":"Demo","inputName":"Fixture","inputKind":"screen_capture"})
}
fn settings() -> Value {
    json!({"inputName":"Fixture","inputSettings":{"window":1234,"nested":{"secret":"fixture-only-private-settings"}}})
}

#[tokio::test]
async fn source_create_and_settings_callbacks_match_with_selectors_omission_and_screenshots() {
    for mut args in [
        create(),
        json!({"canvasUuid":"canvas","sceneName":"Demo","inputName":"Fixture","inputKind":"screen_capture","inputSettings":{},"sceneItemEnabled":false}),
    ] {
        for screenshot in [None, Some(false), Some(true)] {
            if let Some(value) = screenshot {
                args["includeScreenshot"] = json!(value);
            } else {
                args.as_object_mut().unwrap().remove("includeScreenshot");
            }
            compare("obs-create-input", args.clone(), json!({})).await;
        }
    }
    for overlay in [None, Some(false), Some(true)] {
        let mut args = settings();
        if let Some(value) = overlay {
            args["overlay"] = json!(value);
        }
        compare("obs-set-input-settings", args, json!({})).await;
    }
}
#[tokio::test]
async fn source_size_settling_and_audio_only_zero_size_match_exactly() {
    let blank = json!({"sceneItems":[{"sceneItemId":2,"sourceName":"Fixture","sceneItemTransform":{"sourceWidth":0,"sourceHeight":0}}]});
    let ready = json!({"sceneItems":[{"sceneItemId":2,"sourceName":"Fixture","sceneItemTransform":{"sourceWidth":640,"sourceHeight":480}}]});
    compare(
        "obs-create-input",
        create(),
        json!({"lists":[blank.clone(),blank.clone(),ready]}),
    )
    .await;
    compare(
        "obs-set-input-settings",
        settings(),
        json!({"lists":[blank.clone()]}),
    )
    .await;
    for kind in [
        "coreaudio_input_capture",
        "sck_audio_capture",
        "jack_output_capture",
        "process_output_capture",
    ] {
        compare(
            "obs-set-input-settings",
            settings(),
            json!({"settings":{"inputKind":kind,"inputSettings":{}},"lists":[blank.clone()]}),
        )
        .await;
    }
    for file in ["voice.MP3", "voice.aif", "voice.aiff", "voice.opus"] {
        compare("obs-set-input-settings",settings(),json!({"settings":{"inputKind":"ffmpeg_source","inputSettings":{"local_file":file}},"lists":[blank.clone()]})).await;
    }
}
#[tokio::test]
async fn source_failed_readback_and_screenshot_remain_warnings_after_acceptance() {
    for fail in [
        json!({"GetInputSettings":"settings unavailable"}),
        json!({"GetCurrentProgramScene":"program unavailable"}),
        json!({"GetSceneItemList":"scene unavailable"}),
        json!({"GetSourceScreenshot":"render unavailable"}),
    ] {
        let mut args = settings();
        args["includeScreenshot"] = json!(true);
        let result = compare("obs-set-input-settings", args, json!({"fail":fail})).await;
        assert!(result.get("isError").is_none());
    }
    for screenshot in [
        json!({}),
        json!({"imageData":"data:image/jpeg;base64,!"}),
        json!({"imageData":"data:image/jpeg;base64,"}),
    ] {
        let mut args = create();
        args["includeScreenshot"] = json!(true);
        compare("obs-create-input", args, json!({"screenshot":screenshot})).await;
    }
}
#[tokio::test]
async fn source_malformed_observations_preserve_filtering_defaults_and_create_id_coercion() {
    for program in [
        json!({}),
        Value::Null,
        json!({"currentProgramSceneName":17}),
    ] {
        compare("obs-create-input", create(), json!({"program":program})).await;
    }
    compare("obs-set-input-settings",settings(),json!({"settings":{"inputKind":false,"inputSettings":[]},"lists":[{"sceneItems":[null,[],{}, {"sourceName":"Other","sceneItemId":9},{"sourceName":"Fixture","sceneItemId":"2"},{"sourceName":"Fixture","sceneItemId":-0.0,"sceneItemEnabled":false,"sceneItemTransform":{"sourceWidth":0,"sourceHeight":0}},{"sourceName":"Fixture","sceneItemId":2.5,"sceneItemEnabled":true,"sceneItemTransform":{"sourceWidth":1e-7,"sourceHeight":1e21}}]}]})).await;
    for id in [
        Value::Null,
        json!(true),
        json!(3.5),
        json!("custom"),
        json!([1, null, [2]]),
        json!({"x":1}),
        json!({"toString":null}),
    ] {
        compare(
            "obs-create-input",
            create(),
            json!({"created":{"sceneItemId":id}}),
        )
        .await;
    }
    compare("obs-create-input", create(), json!({"created":{}})).await;
}
#[tokio::test]
async fn cancellation_and_original_deadline_bound_only_followup_reads() {
    for expired in [false, true] {
        let request = Request::parse("obs-set-input-settings", &settings()).unwrap();
        let (data, follow_up) = request.into_parts();
        let mut reader = Reader {
            spec: json!({}),
            trace: vec![],
            samples: 0,
        };
        let cancel = Cancellation::new();
        if !expired {
            cancel.cancel();
        }
        let result = follow_up
            .finish(
                &data,
                &json!({}),
                &mut reader,
                Ok(ScreenshotLimit::default()),
                &cancel,
                if expired {
                    Instant::now() - Duration::from_millis(1)
                } else {
                    Instant::now() + Duration::from_secs(3)
                },
            )
            .await;
        assert!(result.get("isError").is_none());
        assert_eq!(reader.trace, Vec::<Value>::new());
        assert!(
            result["structuredContent"]["warnings"]
                .as_array()
                .unwrap()
                .len()
                >= 2
        );
    }
}
#[test]
fn request_projection_preserves_only_callback_fields_and_never_debugs_payloads() {
    let mut args = settings();
    args["includeScreenshot"] = json!(false);
    args["ignored"] = json!("strip");
    let request = Request::parse("obs-set-input-settings", &args).unwrap();
    assert_eq!(request.data().get("includeScreenshot"), None);
    assert_eq!(
        request.arguments().get("includeScreenshot"),
        Some(&json!(false))
    );
    assert_eq!(request.arguments().get("ignored"), None);
    assert!(!format!("{request:?}").contains("fixture-only-private-settings"));
    for args in [
        json!({}),
        json!({"inputName":"Fixture","inputSettings":[]}),
        json!({"inputName":"Fixture","inputSettings":{},"overlay":null}),
        json!({"inputName":"Fixture","inputSettings":{},"includeScreenshot":"true"}),
    ] {
        assert!(Request::parse("obs-set-input-settings", &args).is_none());
    }
}

#[test]
fn transform_text_serialization_order_is_an_observed_admission_gap() {
    let expected = source(
        "obs-set-scene-item-transform",
        &json!({"sceneName":"Demo","sceneItemId":2,"positionX":10}),
        &json!({}),
    );
    let text = expected["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("Applied transform: {\"z\":3,\"a\":1}"),
        "{text}"
    );
    let response: Value = serde_json::from_str("{\"z\":3,\"a\":1}").unwrap();
    let sorted = serde_json::to_string(&response).unwrap();
    assert_eq!(sorted, "{\"a\":1,\"z\":3}");
    assert!(!text.contains(&format!("Applied transform: {sorted}")));
    assert!(
        Request::parse(
            "obs-set-scene-item-transform",
            &json!({"sceneName":"Demo","sceneItemId":2})
        )
        .is_none()
    );
}

#[tokio::test]
async fn direct_session_readback_has_no_frozen_sample_or_diagnostic_size_gate() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let expected_settings = json!({"opaque":"x".repeat(1024*1024)});
    let peer_settings = expected_settings.clone();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket
            .send(Message::Text(
                json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Text(
                json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let mut reads = vec![];
        while let Some(message) = socket.next().await {
            let Ok(Message::Text(text)) = message else {
                let _ = socket.flush().await;
                return reads;
            };
            let frame: Value = serde_json::from_str(&text).unwrap();
            let name = frame["d"]["requestType"].as_str().unwrap();
            reads.push(name.to_string());
            let data = match name {
                "GetInputSettings" => {
                    // A mutation's own structural event is not a frozen-sample
                    // veto on the Source callback's following plain read.
                    socket.send(Message::Text(json!({"op":5,"d":{"eventType":"InputSettingsChanged","eventIntent":8,"eventData":{"inputName":"Fixture"}}}).to_string().into())).await.unwrap();
                    json!({"inputKind":"screen_capture","inputSettings":peer_settings})
                }
                "GetCurrentProgramScene" => json!({}),
                _ => panic!("unexpected read {name}"),
            };
            socket.send(Message::Text(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":name,"requestStatus":{"result":true,"code":100},"responseData":data}}).to_string().into())).await.unwrap();
        }
        reads
    });
    let (session, events) = ObsSession::connect(&endpoint, None, SessionOptions::default())
        .await
        .unwrap();
    let mut reader = SessionReadback::new(session.handle(), None);
    let (data, follow_up) = Request::parse("obs-set-input-settings", &settings())
        .unwrap()
        .into_parts();
    let result = follow_up
        .finish(
            &data,
            &json!({}),
            &mut reader,
            Ok(ScreenshotLimit::default()),
            &Cancellation::new(),
            Instant::now() + Duration::from_secs(3),
        )
        .await;
    assert_eq!(
        result["structuredContent"]["inputSettings"],
        expected_settings
    );
    assert_eq!(result["structuredContent"]["warnings"], json!([]));
    session.shutdown().await.unwrap();
    drop(events);
    assert_eq!(
        peer.await.unwrap(),
        vec!["GetInputSettings", "GetCurrentProgramScene"]
    );
}

#[test]
fn owned_argument_shaping_moves_one_settings_payload_and_counts_local_flag() {
    let text = "private-large-owned-payload".repeat(1024);
    let args = json!({"inputName":"Fixture","inputSettings":{"opaque":text},"includeScreenshot":false,"discarded":[1,2,3]});
    let pointer = args["inputSettings"]["opaque"].as_str().unwrap().as_ptr();
    let request = Request::parse_owned("obs-set-input-settings", args).unwrap();
    assert_eq!(
        request.data()["inputSettings"]["opaque"]
            .as_str()
            .unwrap()
            .as_ptr(),
        pointer
    );
    let expected = obs_mcp_native::js_json::encoded_len(
        &request.arguments(),
        obs_mcp_native::js_json::Limits::default(),
    )
    .unwrap();
    let (data, follow_up) = request.into_parts();
    assert_eq!(
        follow_up
            .retained_argument_bytes(&data, obs_mcp_native::js_json::Limits::default())
            .unwrap(),
        expected
    );
    assert!(data.get("discarded").is_none());
}
#[tokio::test]
async fn source_record_boundary_drops_only_own_proto_key_and_readback_preserves_unknown_keys() {
    let mut args = settings();
    args["inputSettings"]=serde_json::from_str(r#"{"__proto__":{"discarded":"record root"},"constructor":"ordinary own key","nested":{"__proto__":{"kept":true},"integer":9007199254740993},"negative":-0.0}"#).unwrap();
    let readback:Value=serde_json::from_str(r#"{"inputKind":"screen_capture","inputSettings":{"__proto__":{"kept":"readback"},"nested":[-0,9007199254740993]}}"#).unwrap();
    let result = compare("obs-set-input-settings", args, json!({"settings":readback})).await;
    assert_eq!(
        result["structuredContent"]["inputSettings"]["__proto__"],
        json!({"kept":"readback"})
    );
    assert_eq!(
        result["structuredContent"]["inputSettings"]["nested"],
        json!([0, 9007199254740992u64])
    );
}

#[test]
fn actual_source_filter_index_callback_and_fingerprint_use_even_shortest_decimal_tie() {
    let n = f64::from_bits(0x42e073b841ec62b4);
    let result = source(
        "obs-set-source-filter-index",
        &json!({"sourceName":"Fixture","filterName":"Filter","filterIndex":n}),
        &json!({}),
    );
    assert!(
        result["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("144713588892437.62")
    );
    assert_eq!(result["numericJson"], "144713588892437.62");
    assert_eq!(
        obs_mcp_native::observation::signature(&json!(n)).unwrap(),
        result["numericJson"].as_str().unwrap()
    );
    assert_eq!(
        obs_mcp_native::observation::digest(&json!(n)).unwrap(),
        result["numericDigest"].as_str().unwrap()
    );
}
