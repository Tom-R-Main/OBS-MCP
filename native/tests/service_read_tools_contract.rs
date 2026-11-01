// SPDX-License-Identifier: GPL-2.0-only
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    clock,
    custody::Clock,
    preflight::ReadError,
    scene_audit::ScreenshotLimit,
    service::ConnectionDescriptor,
    service_read_tools::{self, ReadToolBounds, ReadToolContext, ReadToolRequest, StatusMetadata},
    session::{ObsSession, SessionOptions},
    session_observation::{ObservationBridge, REQUIRED_AUDIT_EVENTS},
    stage_transport::Cancellation,
    take_transport::LocalHost,
    tool_validation,
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
const WAIT: Duration = Duration::from_secs(3);
const PNG: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
fn clock() -> Clock {
    Arc::new(|| clock::from_epoch_duration(Duration::from_millis(1793448000000)).unwrap())
}
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "read-tools-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&p).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(p)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn settings(root: &Path) -> Value {
    json!({
    "responses":{"GetVersion":{"obsVersion":"32.1.0","platform":"macos","availableRequests":["GetVersion"]},"GetInputList":{"inputs":[{"inputName":"Silent","inputKind":"screen_capture"}]},"GetInputKindList":{"inputKinds":["screen_capture","xcomposite_input"]},"GetSourceScreenshot":{"imageData":PNG},"GetVideoSettings":{"fpsNumerator":2000,"fpsDenominator":1},"GetRecordStatus":{"outputActive":false},"GetRecordDirectory":{"recordDirectory":root},"GetStats":{"availableDiskSpace":2048.4},"GetCurrentProgramScene":{"currentProgramSceneName":"Fixture"},"GetSceneItemList":{"sceneItems":[{"sceneItemId":1,"sourceName":"Silent","isGroup":false}]},"GetInputMute":{"inputMuted":true}},
    "profile":{"Output/Mode":"Simple","SimpleOutput/RecQuality":"High Quality","SimpleOutput/RecEncoder":"x264","SimpleOutput/RecFormat2":"mkv","SimpleOutput/RecTracks":"1"},
    "batch":{"GetInputMute:Silent":{"ok":true,"code":100,"responseData":{"inputMuted":true}},"GetInputAudioTracks:Silent":{"ok":true,"code":100,"responseData":{"inputAudioTracks":{"1":true}}},"GetInputVolume:Silent":{"ok":true,"code":100,"responseData":{"inputVolumeMul":0.8}},"GetInputSettings:Silent":{"ok":true,"code":100,"responseData":{"inputKind":"screen_capture","inputSettings":{"type":1,"window":123}}},"GetSceneItemTransform:1":{"ok":true,"code":100,"responseData":{"sceneItemTransform":{"sourceWidth":640,"sourceHeight":480}}}},"platform":"macos","pending":false})
}
fn key(name: &str, data: &Value) -> String {
    format!(
        "{name}:{}",
        data["inputName"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| data["sceneItemId"].as_i64().map(|n| n.to_string()))
            .unwrap_or_default()
    )
}
fn response(config: &Value, name: &str, data: &Value) -> Value {
    if name == "GetProfileParameter" {
        return json!({"parameterValue":config["profile"][format!("{}/{}",data["parameterCategory"].as_str().unwrap(),data["parameterName"].as_str().unwrap())],"defaultParameterValue":null});
    }
    config["responses"]
        .get(key(name, data))
        .or_else(|| config["responses"].get(name))
        .cloned()
        .unwrap_or(Value::Null)
}
struct Hooks {
    config: Mutex<Value>,
    trace: Mutex<Vec<Value>>,
    hold: AtomicBool,
    entered: Notify,
    release: Notify,
}
struct Fixture {
    url: String,
    owner: ObsSession,
    bridge: ObservationBridge,
    hooks: Arc<Hooks>,
    peer: JoinHandle<()>,
}
impl Fixture {
    async fn new(config: Value) -> Self {
        let hooks = Arc::new(Hooks {
            config: Mutex::new(config),
            trace: Mutex::new(vec![]),
            hold: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let state = hooks.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
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
                if state.hold.swap(false, Ordering::AcqRel) {
                    state.entered.notify_one();
                    state.release.notified().await;
                }
                let config = state.config.lock().unwrap().clone();
                if frame["op"] == 8 {
                    assert_eq!(frame["d"]["executionType"], 0);
                    let calls = frame["d"]["requests"].as_array().unwrap();
                    let trace:Vec<_>=calls.iter().map(|v|json!({"request":v["requestType"],"data":v.get("requestData").cloned().unwrap_or(Value::Null)})).collect();
                    state
                        .trace
                        .lock()
                        .unwrap()
                        .push(json!({"kind":"batch","calls":trace}));
                    let rows:Vec<_>=calls.iter().map(|v|{let name=v["requestType"].as_str().unwrap();let data=&v["requestData"];let result=config["batch"].get(key(name,data)).cloned().unwrap_or(json!({"ok":false,"code":600,"responseData":{}}));json!({"requestId":v["requestId"],"requestType":name,"requestStatus":{"result":result["ok"],"code":result["code"]},"responseData":result["responseData"]})}).collect();
                    if peer.send(Message::Text(json!({"op":9,"d":{"requestId":frame["d"]["requestId"],"results":rows}}).to_string().into())).await.is_err(){break;}
                    continue;
                }
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
                    .push(json!({"kind":"read","request":name,"data":data}));
                let failure = config["failure"].as_str() == Some(name);
                if config["event"] == true {
                    peer.send(Message::Text(json!({"op":5,"d":{"eventType":"InputSettingsChanged","eventIntent":8,"eventData":{}}}).to_string().into())).await.unwrap();
                }
                let reply = response(&config, name, &data);
                if peer.send(Message::Text(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":name,"requestStatus":{"result":!failure,"code":if failure{500}else{100},"comment":"PRIVATE READ TOOL COMMENT"},"responseData":reply}}).to_string().into())).await.is_err(){break;}
            }
        });
        let (owner, events) = ObsSession::connect(
            &url,
            None,
            SessionOptions {
                event_subscriptions: REQUIRED_AUDIT_EVENTS,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
        Self {
            url,
            owner,
            bridge,
            hooks,
            peer,
        }
    }
    fn context(&self, root: Option<&Path>) -> ReadToolContext {
        let config = self.hooks.config.lock().unwrap();
        ReadToolContext {
            handle: self.owner.handle(),
            reader: self.bridge.reader(),
            capabilities: config["capabilities"].as_array().map(|v| {
                Arc::new(
                    v.iter()
                        .map(|v| v.as_str().unwrap().to_owned())
                        .collect::<HashSet<_>>(),
                )
            }),
            platform: config["platform"].as_str().map(str::to_owned),
            connection: ConnectionDescriptor {
                url: self.url.clone(),
                has_password: true,
            },
            clock: clock(),
            cancellation: Cancellation::new(),
            pending_output_settings: Arc::new(AtomicBool::new(config["pending"] == true)),
            writable: root.map(|root| Arc::new(LocalHost::new(root, None, None).unwrap())),
            screenshot_limit: ScreenshotLimit::parse(config["limit"].as_str()),
            bounds: ReadToolBounds::default(),
        }
    }
    async fn close(self) {
        let _ = self.owner.shutdown().await;
        self.bridge.shutdown().await.unwrap();
        timeout(WAIT, self.peer).await.unwrap().unwrap();
    }
}
/// Actual MCP SDK through the COMPLETE index middleware (lease/take guards and
/// ensureStructuredContent), not isolated module callbacks. Legacy transport
/// is selected explicitly: transport-owned modern _meta is tested by its owner.
fn source(input: &Value) -> Value {
    let script = r#"
import{EventEmitter}from'node:events';import{McpServer,InMemoryTransport}from'@modelcontextprotocol/server';import{Client}from'@modelcontextprotocol/client';import{initialize}from'./build/tools/index.js';
const chunks=[];for await(const chunk of process.stdin)chunks.push(chunk);const input=JSON.parse(Buffer.concat(chunks));const RealDate=Date;globalThis.Date=class extends RealDate{constructor(...args){super(...(args.length?args:[1793448000000]));}static now(){return 1793448000000;}};
if(input.config.limit!=null)process.env.OBS_MCP_MAX_SCREENSHOT_BYTES=input.config.limit;else delete process.env.OBS_MCP_MAX_SCREENSHOT_BYTES;
const trace=[],key=(type,data)=>type+':'+(data?.inputName??data?.sceneItemId??'');function reply(type,data){if(type==='GetProfileParameter')return{parameterValue:input.config.profile[data.parameterCategory+'/'+data.parameterName]??null,defaultParameterValue:null};return structuredClone(input.config.responses[key(type,data)]??input.config.responses[type]??null)}
class Fake extends EventEmitter{async connect(){}isConnected(){return input.connected??true}getConnectionStatus(){return{url:input.url,hasPassword:true,identified:input.identified??true,versionInfo:{platform:input.config.platform}}}hasPendingOutputSettings(){return input.config.pending===true}supportsRequest(name){return input.config.capabilities===undefined||input.config.capabilities.includes(name)}
async sendRequest(type,data){if(!this.supportsRequest(type))throw Error(`OBS WebSocket does not advertise support for request '${type}'`);trace.push({kind:'read',request:type,data:data??null});if(input.config.failure===type)throw Error(`OBS request ${type} failed with code 500: PRIVATE READ TOOL COMMENT`);return reply(type,data)}
async sendBatch(calls){for(const call of calls)if(!this.supportsRequest(call.requestType))throw Error(`OBS WebSocket does not advertise support for request '${call.requestType}'`);trace.push({kind:'batch',calls:calls.map(call=>({request:call.requestType,data:call.requestData}))});return calls.map(call=>structuredClone(input.config.batch[key(call.requestType,call.requestData)]??{ok:false,code:600,responseData:{}}))}}
const fake=new Fake(),server=new McpServer({name:'full-read-tool-oracle',version:'1'});initialize(server,fake,{resourcesAndPrompts:false,sessionNotifications:false,confirmLive:false});const client=new Client({name:'consumer',version:'1'},{versionNegotiation:{mode:'legacy'}}),[a,b]=InMemoryTransport.createLinkedPair();try{await Promise.all([server.connect(a),client.connect(b)]);const result=await client.callTool({name:input.name,arguments:input.args});console.log(JSON.stringify({result,trace}));}finally{await client.close();await server.close();}
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
        "full index oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn semantics(mut value: Value) -> Value {
    for block in value["content"].as_array_mut().unwrap() {
        if block["type"] == "text"
            && let Ok(json) = serde_json::from_str::<Value>(block["text"].as_str().unwrap())
        {
            block["text"] = json;
        }
    }
    value
}
fn preflight_trace(mut trace: Vec<Value>) -> Vec<Value> {
    trace.sort_by_key(Value::to_string);
    trace
}
async fn compare(root: &Path, name: &str, args: Value, config: Value) -> Value {
    let fixture = Fixture::new(config.clone()).await;
    let expected = source(&json!({"name":name,"args":args,"url":fixture.url,"config":config}));
    let parsed = ReadToolRequest::parse(name, &args);
    let result = match parsed {
        Ok(request) => service_read_tools::run(fixture.context(Some(root)), request)
            .await
            .unwrap(),
        Err(_) => {
            let catalog: Value =
                serde_json::from_str(include_str!("../assets/tools.json")).unwrap();
            let schema = &catalog["tools"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["name"] == name)
                .unwrap()["inputSchema"];
            tool_validation::tool_error(name, schema, &args).unwrap()
        }
    };
    assert_eq!(
        semantics(result.clone()),
        semantics(expected["result"].clone()),
        "{name} {args}"
    );
    let actual = fixture.hooks.trace.lock().unwrap().clone();
    let expected = expected["trace"].as_array().unwrap().clone();
    if name == "obs-preflight" {
        assert_eq!(
            preflight_trace(actual),
            preflight_trace(expected),
            "independent checks preserve every read and within-batch order"
        );
    } else {
        assert_eq!(actual, expected, "{name} wire requests");
    }
    fixture.close().await;
    result
}

#[tokio::test]
async fn status_and_fresh_connection_test_match_full_index_shapes() {
    let root = Scratch::new();
    for name in ["obs-get-status", "obs-test-connection"] {
        compare(&root.0, name, json!({}), settings(&root.0)).await;
    }
    let config = settings(&root.0);
    let connection = ConnectionDescriptor {
        url: "ws://127.0.0.1:4455".into(),
        has_password: true,
    };
    let expected = source(
        &json!({"name":"obs-get-status","args":{},"url":connection.url,"config":config,"connected":false,"identified":false}),
    );
    assert_eq!(
        semantics(
            service_read_tools::status(
                StatusMetadata {
                    connection,
                    connected: false,
                    identified: false
                },
                clock()
            )
            .unwrap()
        ),
        semantics(expected["result"].clone())
    );
}

#[tokio::test]
async fn optional_filters_preserve_absence_empty_values_and_false() {
    let root = Scratch::new();
    for (name, args) in [
        ("obs-get-input-list", json!({})),
        ("obs-get-input-list", json!({"inputKind":"","ignored":9})),
        ("obs-get-input-list", json!({"inputKind":"screen_capture"})),
        ("obs-get-input-kind-list", json!({})),
        ("obs-get-input-kind-list", json!({"unversioned":false})),
        ("obs-get-input-kind-list", json!({"unversioned":true})),
    ] {
        compare(&root.0, name, args, settings(&root.0)).await;
    }
}

#[tokio::test]
async fn screenshot_uses_shared_parser_and_preserves_image_bytes_and_bounds() {
    let root = Scratch::new();
    for variant in ["normal", "tiny", "invalidlimit", "malformed"] {
        let mut config = settings(&root.0);
        match variant {
            "tiny" => config["limit"] = json!("2"),
            "invalidlimit" => config["limit"] = json!(" 4"),
            "malformed" => {
                config["responses"]["GetSourceScreenshot"]["imageData"] =
                    json!("data:image/png;base64,A===")
            }
            _ => {}
        }
        compare(&root.0,"obs-get-source-screenshot",json!({"imageFormat":"png","sourceUuid":"fixture","imageWidth":960.0,"imageCompressionQuality":-1}),config).await;
    }
}

#[tokio::test]
async fn sleep_matches_exclusivity_js_round_coercion_fps_and_cap_errors() {
    let root = Scratch::new();
    for args in [
        json!({}),
        json!({"sleepMillis":0,"sleepFrames":0}),
        json!({"sleepMillis":0.0}),
        json!({"sleepFrames":0}),
        json!({"sleepFrames":1}),
    ] {
        compare(&root.0, "obs-sleep", args, settings(&root.0)).await;
    }
    for (numerator, denominator, frames) in [
        (json!("2000"), json!(1), 1),
        (json!(2000.0000000000005), json!(1), 1),
        (json!([2000]), json!(1), 1),
        (json!(true), json!(1), 0),
        (json!(1), json!(0), 1),
        (Value::Null, json!(1), 1),
        (json!({"toString":false}), json!(1), 1),
        (json!([{"toString":null}]), json!(1), 1),
        (json!(1), json!(1), 10_000),
    ] {
        let mut config = settings(&root.0);
        config["responses"]["GetVideoSettings"] =
            json!({"fpsNumerator":numerator,"fpsDenominator":denominator});
        compare(&root.0, "obs-sleep", json!({"sleepFrames":frames}), config).await;
    }
}

#[tokio::test]
async fn malformed_public_arguments_match_actual_sdk_errors_without_dispatch() {
    let root = Scratch::new();
    for (name, args) in [
        ("obs-get-input-list", json!({"inputKind":null})),
        ("obs-get-input-kind-list", json!({"unversioned":1})),
        ("obs-sleep", json!({"sleepMillis":-1})),
        ("obs-sleep", json!({"sleepFrames":0.5})),
        (
            "obs-get-source-screenshot",
            json!({"imageFormat":"png","imageWidth":7}),
        ),
        ("obs-get-source-screenshot", json!({"imageFormat":null})),
        ("obs-preflight", json!({"expectSilent":null})),
        ("obs-preflight", json!({"minFreeDiskMb":-1})),
        (
            "obs-preflight",
            json!({"minFreeDiskMb":9_007_199_254_740_992.0}),
        ),
    ] {
        compare(&root.0, name, args, settings(&root.0)).await;
    }
}

#[tokio::test]
async fn preflight_complete_results_cover_encoder_audio_capture_and_pending_settings() {
    let root = Scratch::new();
    for variant in [
        "normal",
        "encoder",
        "audio",
        "capture",
        "pending",
        "peererror",
    ] {
        let mut config = settings(&root.0);
        match variant {
            "encoder" => config["profile"]["SimpleOutput/RecEncoder"] = json!("nvenc"),
            "audio" => {
                config["batch"]["GetInputMute:Silent"]["responseData"]["inputMuted"] = json!(false)
            }
            "capture" => {
                config["batch"]["GetSceneItemTransform:1"]["responseData"]["sceneItemTransform"]["sourceWidth"] =
                    json!(0)
            }
            "pending" => config["pending"] = json!(true),
            "peererror" => config["failure"] = json!("GetRecordStatus"),
            _ => {}
        }
        compare(
            &root.0,
            "obs-preflight",
            json!({"expectSilent":true}),
            config,
        )
        .await;
        assert_eq!(fs::read_dir(&root.0).unwrap().count(), 0);
    }
}

#[tokio::test]
async fn capabilities_unknown_are_not_empty_and_no_root_never_fakes_preflight_pass() {
    let root = Scratch::new();
    let fixture = Fixture::new(settings(&root.0)).await;
    let error = service_read_tools::run(
        fixture.context(None),
        ReadToolRequest::parse("obs-preflight", &json!({})).unwrap(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.reason, "capability_unavailable");
    assert!(fixture.hooks.trace.lock().unwrap().is_empty());
    fixture.close().await;
    for name in [
        "obs-test-connection",
        "obs-get-input-list",
        "obs-get-input-kind-list",
    ] {
        let mut config = settings(&root.0);
        config["capabilities"] = json!([]);
        compare(&root.0, name, json!({}), config).await;
    }
    let e = ReadError::new("PRIVATE PREFLIGHT ERROR");
    assert!(!format!("{e:?} {e}").contains("PRIVATE"));
    assert_eq!(e.tool_message(), "PRIVATE PREFLIGHT ERROR");
}

#[tokio::test]
async fn retained_reads_preflight_pairs_and_sleep_timers_settle_before_owner_return() {
    let root = Scratch::new();
    for name in ["obs-get-input-list", "obs-preflight"] {
        let fixture = Fixture::new(settings(&root.0)).await;
        fixture.hooks.hold.store(true, Ordering::Release);
        let context = fixture.context(Some(&root.0));
        let cancellation = context.cancellation.clone();
        let mut work = Box::pin(service_read_tools::run(
            context,
            ReadToolRequest::parse(name, &json!({})).unwrap(),
        ));
        assert!(poll!(work.as_mut()).is_pending());
        fixture.hooks.entered.notified().await;
        assert!(
            timeout(Duration::from_millis(10), work.as_mut())
                .await
                .is_err()
        );
        cancellation.cancel();
        fixture.hooks.release.notify_one();
        let result = work.await;
        if name == "obs-preflight" {
            assert_eq!(result.unwrap_err().reason, "cancelled");
        } else {
            assert_eq!(result.unwrap()["isError"], true);
        }
        fixture.close().await;
    }
    let fixture = Fixture::new(settings(&root.0)).await;
    let mut context = fixture.context(None);
    context.bounds.deadline = Instant::now() + Duration::from_millis(15);
    let work = service_read_tools::run(
        context,
        ReadToolRequest::parse("obs-sleep", &json!({"sleepMillis":50_000})).unwrap(),
    );
    let result = timeout(WAIT, work).await.unwrap().unwrap();
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("deadline expired")
    );
    assert!(fixture.hooks.trace.lock().unwrap().is_empty());
    fixture.close().await;
}

#[tokio::test]
async fn detailed_peer_errors_preserve_full_index_body_and_opaque_diagnostics() {
    let root = Scratch::new();
    for (name, request, args) in [
        ("obs-test-connection", "GetVersion", json!({})),
        ("obs-get-input-list", "GetInputList", json!({})),
        ("obs-get-input-kind-list", "GetInputKindList", json!({})),
        (
            "obs-get-source-screenshot",
            "GetSourceScreenshot",
            json!({"imageFormat":"png"}),
        ),
    ] {
        let mut config = settings(&root.0);
        config["failure"] = json!(request);
        let result = compare(&root.0, name, args, config).await;
        assert_eq!(result["isError"], true);
        assert!(result.get("structuredContent").is_none());
    }
}

#[tokio::test]
async fn physical_binding_and_queued_pre_reply_event_barrier_are_checked() {
    let root = Scratch::new();
    let mut config = settings(&root.0);
    config["event"] = json!(true);
    let first = Fixture::new(config).await;
    let second = Fixture::new(settings(&root.0)).await;
    let mut wrong = first.context(None);
    wrong.reader = second.bridge.reader();
    assert!(
        service_read_tools::run(wrong, ReadToolRequest::InputList { input_kind: None })
            .await
            .is_err()
    );
    assert!(first.hooks.trace.lock().unwrap().is_empty());
    assert!(second.hooks.trace.lock().unwrap().is_empty());
    let result = service_read_tools::run(
        first.context(None),
        ReadToolRequest::InputList { input_kind: None },
    )
    .await
    .unwrap();
    assert_ne!(result["isError"], true);
    assert_eq!(first.bridge.evidence().borrow().revision, 1);
    assert!(
        first
            .bridge
            .ready_at_revision(first.owner.token().generation(), 1)
    );
    assert_eq!(first.hooks.trace.lock().unwrap().len(), 1);
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn rpc_deadline_discards_late_response_and_actor_owner_is_joined_separately() {
    let root = Scratch::new();
    let fixture = Fixture::new(settings(&root.0)).await;
    fixture.hooks.hold.store(true, Ordering::Release);
    let mut context = fixture.context(None);
    context.bounds.deadline = Instant::now() + Duration::from_millis(20);
    let mut work = Box::pin(service_read_tools::run(
        context,
        ReadToolRequest::InputList { input_kind: None },
    ));
    assert!(poll!(work.as_mut()).is_pending());
    fixture.hooks.entered.notified().await;
    let result = timeout(WAIT, work).await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(result.get("structuredContent").is_none());
    let hooks = fixture.hooks.clone();
    hooks.release.notify_one();
    fixture.close().await;
    assert_eq!(
        hooks.trace.lock().unwrap().len(),
        1,
        "unknown read was replayed"
    );
}
