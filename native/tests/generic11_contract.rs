// SPDX-License-Identifier: GPL-2.0-only
//! Actual pinned SDK callbacks versus one real Rust socket and scoped V2 owner.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    clock,
    control::ControlDomain,
    custody::Clock,
    generic_requests::{self, Call},
    operation_store::OperationStore,
    receipt_broker::{ReceiptBroker, ScopeKind},
    session::{ObsSession, SessionOptions},
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, time::Instant};
use tokio_tungstenite::{accept_async, tungstenite::Message};
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "generic11-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn source(name: &str, args: &Value, response: &Value, reject: bool) -> Value {
    source_with_era(name, args, response, reject, false)
}
fn source_with_era(
    name: &str,
    args: &Value,
    response: &Value,
    reject: bool,
    legacy: bool,
) -> Value {
    let script = r#"
import{Client}from'@modelcontextprotocol/client';import{StdioClientTransport,getDefaultEnvironment}from'@modelcontextprotocol/client/stdio';import{mkdtempSync,rmSync,chmodSync}from'node:fs';import{join}from'node:path';
let raw='';for await(const part of process.stdin)raw+=part;const input=JSON.parse(raw);
const serverCode=`import{McpServer}from'@modelcontextprotocol/server';import{serveStdio}from'@modelcontextprotocol/server/stdio';import{EventEmitter}from'node:events';import{initialize}from'./build/tools/index.js';import{OBSRequestError}from'./build/client.js';const obs=new EventEmitter();Object.assign(obs,{sendRequest:async(requestType,requestData)=>{process.stderr.write('TRACE '+JSON.stringify({requestType,...(requestData===undefined?{}:{requestData})})+'\\n');if(${input.reject})throw new OBSRequestError(requestType,500,'PRIVATE callback detail');return ${JSON.stringify(input.response??{})};},markOutputSettingsPending:()=>process.stderr.write('PENDING\\n'),isConnected:()=>true,getConnectionStatus:()=>({generation:1,identified:true,url:'ws://localhost:4455',hasPassword:false}),supportsRequest:()=>true});serveStdio(()=>{const server=new McpServer({name:'generic11-source',version:'1'});initialize(server,obs,{confirmLive:false,resourcesAndPrompts:false,sessionNotifications:false});return server});`;
const state=mkdtempSync(join(process.cwd(),'native/target/generic11-oracle-'));chmodSync(state,0o700);const client=new Client({name:'oracle',version:'1'},{versionNegotiation:{mode:process.argv[1]==='legacy'?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:process.execPath,args:['--input-type=module','-e',serverCode],cwd:process.cwd(),env:{...getDefaultEnvironment(),OBS_MCP_STATE_DIR:state,OBS_MCP_CONFIRM_LIVE:'false'},stderr:'pipe'});let diagnostics='';transport.stderr?.on('data',part=>diagnostics+=String(part));let result;try{await client.connect(transport);result=await client.callTool({name:input.name,arguments:input.args});delete result._meta;}finally{await client.close();await transport.close();rmSync(state,{recursive:true,force:true});}console.log(JSON.stringify({result,trace:diagnostics.split('\n').filter(line=>line.startsWith('TRACE ')).map(line=>JSON.parse(line.slice(6))),pending:diagnostics.split('PENDING').length-1}));
"#;
    let mut child = Command::new("node")
        .args([
            "--input-type=module",
            "-e",
            script,
            if legacy { "legacy" } else { "modern" },
        ])
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
        .write_all(
            &serde_json::to_vec(
                &json!({"name":name,"args":args,"response":response,"reject":reject}),
            )
            .unwrap(),
        )
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "Source oracle: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn semantic(mut value: Value) -> Value {
    if let Some(blocks) = value["content"].as_array_mut() {
        for block in blocks {
            if block["type"] == "text"
                && let Some(text) = block["text"].as_str()
                && let Ok(parsed) = serde_json::from_str::<Value>(text)
            {
                block["text"] = parsed;
            }
        }
    }
    value
}
async fn compare(name: &str, args: Value, response: Value, reject: bool) -> Value {
    let expected = source(name, &args, &response, reject);
    let scratch = Scratch::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let wire = Arc::new(Mutex::new(vec![]));
    let observed = wire.clone();
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
        while let Some(message) = socket.next().await {
            let Ok(Message::Text(text)) = message else {
                let _ = socket.flush().await;
                return;
            };
            let frame: Value = serde_json::from_str(&text).unwrap();
            let mut entry = frame["d"].clone();
            entry.as_object_mut().unwrap().remove("requestId");
            observed.lock().unwrap().push(entry);
            let reply = json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":frame["d"]["requestType"],"requestStatus":{"result":!reject,"code":if reject{500}else{100},"comment":"PRIVATE callback detail"},"responseData":response}});
            if socket
                .send(Message::Text(reply.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    });
    let (session, events) = ObsSession::connect(&endpoint, None, SessionOptions::default())
        .await
        .unwrap();
    let call = Call::parse(name, &args).unwrap();
    let broker = ReceiptBroker::new(OperationStore::new(&scratch.0, &endpoint).unwrap()).unwrap();
    let scope = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
    let clock: Clock =
        Arc::new(|| clock::from_epoch_duration(Duration::from_millis(1793448000000)).unwrap());
    let pending = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let result = generic_requests::execute_with_pending(
        scope,
        call,
        session.handle(),
        Arc::new(Mutex::new(ControlDomain::default())),
        clock,
        "oracle".into(),
        Cancellation::new(),
        "00000000-0000-4000-8000-000000000001".into(),
        Duration::from_secs(3),
        Instant::now(),
        pending.clone(),
    )
    .await;
    assert_eq!(
        pending.load(Ordering::Acquire),
        expected["pending"].as_u64().unwrap() != 0
    );
    assert_eq!(
        semantic(result.clone()),
        semantic(expected["result"].clone()),
        "{name} {args}"
    );
    assert_eq!(
        *wire.lock().unwrap(),
        *expected["trace"].as_array().unwrap(),
        "{name} exact wire"
    );
    let receipt = broker
        .read_view()
        .read_any("00000000-0000-4000-8000-000000000001")
        .unwrap();
    assert_eq!(
        receipt["phase"],
        if reject { "rejected" } else { "accepted" }
    );
    assert_eq!(receipt["postconditionObserved"], false);
    session.shutdown().await.unwrap();
    drop(events);
    peer.await.unwrap();
    result
}
fn cases() -> Vec<(&'static str, Value)> {
    vec![
        (
            "obs-set-persistent-data",
            json!({"realm":"arbitrary Realm","slotName":"Slot","slotValue":{"nested":[null,true,9007199254740993u64],"__proto__":{"kept":true},"constructor":"kept"}}),
        ),
        (
            "obs-set-current-scene-collection",
            json!({"sceneCollectionName":"Fixture collection"}),
        ),
        (
            "obs-create-scene-collection",
            json!({"sceneCollectionName":"日本語 collection"}),
        ),
        (
            "obs-set-current-profile",
            json!({"profileName":"Fixture profile"}),
        ),
        ("obs-create-profile", json!({"profileName":"New profile"})),
        ("obs-remove-profile", json!({"profileName":"Old profile"})),
        (
            "obs-set-stream-service-settings",
            json!({"streamServiceType":"rtmp_custom","streamServiceSettings":{"server":"fixture","key":"PRIVATE fixture","__proto__":"strip","nested":{"__proto__":"keep","number":18446744073709551615u64}}}),
        ),
        (
            "obs-set-record-directory",
            json!({"recordDirectory":"/remote/not-the-approved-local-root"}),
        ),
        (
            "obs-set-output-settings",
            json!({"outputName":"Fixture output","outputSettings":{"__proto__":"strip","constructor":"keep","value":f64::from_bits(0x42e0_73b8_41ec_62b4)}}),
        ),
        (
            "obs-set-video-settings",
            json!({"fpsNumerator":59.94,"fpsDenominator":1,"baseWidth":1920.5,"baseHeight":1080,"outputWidth":960,"outputHeight":540}),
        ),
        (
            "obs-set-profile-parameter",
            json!({"parameterCategory":"Output","parameterName":"Mode","parameterValue":null}),
        ),
    ]
}
#[tokio::test]
async fn eleven_actual_source_callback_results_wire_and_client_flags_match() {
    for (name, args) in cases() {
        compare(name, args, json!({}), false).await;
    }
}
#[tokio::test]
async fn eleven_explicit_rejections_preserve_errors_without_setting_pending() {
    for (name, args) in cases() {
        compare(name, args, json!({}), true).await;
    }
}
#[tokio::test]
async fn source_empty_video_nullable_profile_exact_categories_and_unknown_slot_shapes() {
    compare("obs-set-video-settings", json!({}), json!({}), false).await;
    for category in ["Output", "SimpleOutput", "AdvOut", "output", "General", ""] {
        compare(
            "obs-set-profile-parameter",
            json!({"parameterCategory":category,"parameterName":"","parameterValue":""}),
            json!({}),
            false,
        )
        .await;
    }
    for value in [
        Value::Null,
        json!(false),
        json!(42),
        json!("literal"),
        json!([null,{"__proto__":true}]),
    ] {
        compare(
            "obs-set-persistent-data",
            json!({"realm":"","slotName":"","slotValue":value}),
            json!({}),
            false,
        )
        .await;
    }
}

#[test]
fn actual_sdk_unknown_json_and_string_record_error_projection_matches_both_eras() {
    let catalog: Value = serde_json::from_str(include_str!("../assets/tools.json")).unwrap();
    let cases = [
        ("obs-set-persistent-data", json!({})),
        (
            "obs-set-persistent-data",
            json!({"realm":"R","slotName":"N"}),
        ),
        (
            "obs-set-persistent-data",
            json!({"realm":null,"slotName":42,"slotValue":null}),
        ),
        (
            "obs-set-stream-service-settings",
            json!({"streamServiceType":"rtmp_custom"}),
        ),
        (
            "obs-set-stream-service-settings",
            json!({"streamServiceType":"rtmp_custom","streamServiceSettings":null}),
        ),
        (
            "obs-set-stream-service-settings",
            json!({"streamServiceType":"rtmp_custom","streamServiceSettings":[]}),
        ),
        (
            "obs-set-stream-service-settings",
            json!({"streamServiceType":42,"streamServiceSettings":{"private":"never formatted","constructor":{"nested":[null,true]}}}),
        ),
        (
            "obs-set-output-settings",
            json!({"outputName":"N","outputSettings":[]}),
        ),
        ("obs-set-output-settings", json!({"outputName":"N"})),
        (
            "obs-set-video-settings",
            json!({"fpsNumerator":0.5,"baseWidth":0,"outputHeight":4096.5}),
        ),
        (
            "obs-set-video-settings",
            json!({"baseWidth":null,"fpsDenominator":"1"}),
        ),
        ("obs-set-current-profile", json!({"profileName":null})),
        ("obs-create-profile", json!({})),
        ("obs-set-record-directory", json!({"recordDirectory":false})),
        (
            "obs-set-profile-parameter",
            json!({"parameterCategory":"Output","parameterName":"N","parameterValue":42}),
        ),
        (
            "obs-set-profile-parameter",
            json!({"parameterCategory":"Output","parameterName":"N"}),
        ),
        (
            "obs-set-profile-parameter",
            json!({"parameterCategory":42,"parameterValue":[]}),
        ),
    ];
    for legacy in [false, true] {
        for (name, args) in &cases {
            let expected = source_with_era(name, args, &json!({}), false, legacy);
            assert_eq!(
                expected["trace"],
                json!([]),
                "invalidSourceargs sent {name}"
            );
            assert_eq!(expected["result"]["isError"], true);
            let schema = &catalog["tools"]
                .as_array()
                .unwrap()
                .iter()
                .find(|tool| tool["name"] == *name)
                .unwrap()["inputSchema"];
            assert_eq!(
                obs_mcp_native::tool_validation::tool_error(name, schema, args).unwrap(),
                expected["result"],
                "{name} {args}; legacy={legacy}"
            );
            assert!(Call::parse(name, args).is_none());
        }
    }
}

#[tokio::test]
async fn missing_required_pending_binding_refuses_before_intent_or_a_single_packet() {
    let scratch = Scratch::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let wire = Arc::new(Mutex::new(Vec::<Value>::new()));
    let observed = wire.clone();
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
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Text(text)) => observed
                    .lock()
                    .unwrap()
                    .push(serde_json::from_str(&text).unwrap()),
                _ => {
                    let _ = socket.flush().await;
                    return;
                }
            }
        }
    });
    let (session, events) = ObsSession::connect(&endpoint, None, SessionOptions::default())
        .await
        .unwrap();
    let call = Call::parse(
        "obs-set-profile-parameter",
        &json!({"parameterCategory":"Output","parameterName":"Mode","parameterValue":"Advanced"}),
    )
    .unwrap();
    let broker = ReceiptBroker::new(OperationStore::new(&scratch.0, &endpoint).unwrap()).unwrap();
    let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
    let clock: Clock =
        Arc::new(|| clock::from_epoch_duration(Duration::from_millis(1793448000000)).unwrap());
    let result = generic_requests::execute(
        port,
        call,
        session.handle(),
        Arc::new(Mutex::new(ControlDomain::default())),
        clock,
        "oracle".into(),
        Cancellation::new(),
        "00000000-0000-4000-8000-000000000001".into(),
        Duration::from_secs(3),
        Instant::now(),
    )
    .await;
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("state is unavailable")
    );
    assert_eq!(broker.read_view().summary()["total"], 0);
    assert!(!scratch.0.join("operation-receipts").exists());
    session.shutdown().await.unwrap();
    drop(events);
    peer.await.unwrap();
    assert!(wire.lock().unwrap().is_empty());
}
