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
    tool_validation,
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
                "generic20-{}-{}",
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
    let script = r#"
import {McpServer,InMemoryTransport} from '@modelcontextprotocol/server';import {Client} from '@modelcontextprotocol/client';import {EventEmitter} from 'node:events';import {initialize} from './build/tools/index.js';import {OBSRequestError} from './build/client.js';
let raw='';for await(const chunk of process.stdin)raw+=chunk;const input=JSON.parse(raw);const trace=[];const obs=new EventEmitter();Object.assign(obs,{sendRequest:async(requestType,requestData)=>{trace.push({requestType,...(requestData===undefined?{}:{requestData})});if(input.reject)throw new OBSRequestError(requestType,500,'PRIVATE callback detail');return input.response??{};},isConnected:()=>true,getConnectionStatus:()=>({generation:1,identified:true,url:'ws://localhost:4455',hasPassword:false}),supportsRequest:()=>true});
const server=new McpServer({name:'generic20-source',version:'1'});initialize(server,obs,{confirmLive:false,resourcesAndPrompts:false,sessionNotifications:false});const client=new Client({name:'oracle',version:'1'}),[a,b]=InMemoryTransport.createLinkedPair();try{await Promise.all([server.connect(a),client.connect(b)]);const result=await client.callTool({name:input.name,arguments:input.args});console.log(JSON.stringify({result,trace}));}finally{await client.close();await server.close();}
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
        .write_all(
            &serde_json::to_vec(
                &json!({"name":name,"args":args,"response":response,"reject":reject}),
            )
            .unwrap(),
        )
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "Source oracle: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
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
    let result = generic_requests::execute(
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
    )
    .await;
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
            "obs-create-scene",
            json!({"canvasUuid":"canvas","sceneName":"日本語"}),
        ),
        ("obs-remove-scene", json!({"sceneName":"Review"})),
        (
            "obs-remove-scene-item",
            json!({"sceneUuid":"scene","sceneItemId":7}),
        ),
        (
            "obs-remove-source-filter",
            json!({"sourceName":"App","filterName":"Filter"}),
        ),
        (
            "obs-set-source-filter-name",
            json!({"sourceName":"App","filterName":"Filter","newFilterName":"New"}),
        ),
        (
            "obs-set-source-filter-index",
            json!({"sourceName":"App","filterName":"Filter","filterIndex":0.5}),
        ),
        ("obs-remove-input", json!({"inputName":"App"})),
        (
            "obs-set-input-name",
            json!({"inputName":"App","newInputName":"New"}),
        ),
        (
            "obs-duplicate-scene-item",
            json!({"sceneUuid":"scene","sceneItemId":7,"destinationSceneUuid":"other"}),
        ),
        (
            "obs-set-scene-name",
            json!({"sceneUuid":"scene","newSceneName":"New"}),
        ),
        (
            "obs-create-scene-item",
            json!({"sceneUuid":"scene","sourceUuid":"input","enabled":false}),
        ),
        (
            "obs-set-scene-item-enabled",
            json!({"sceneUuid":"scene","sceneItemId":7,"enabled":false}),
        ),
        ("obs-set-current-scene", json!({"sceneName":"Review"})),
        ("obs-set-preview-scene", json!({"sceneName":"Review"})),
        ("obs-trigger-studio-transition", json!({"ignored":true})),
        (
            "obs-set-current-transition",
            json!({"transitionName":"Fade"}),
        ),
        ("obs-trigger-transition", json!({})),
        (
            "obs-set-scene-transition-override",
            json!({"sceneUuid":"scene","transitionName":null,"transitionDuration":null}),
        ),
        ("obs-set-tbar-position", json!({"position":0.25})),
        ("obs-set-transition-duration", json!({"duration":125.5})),
    ]
}
#[tokio::test]
async fn all_twenty_callbacks_and_wire_mappings_match_actual_sdk() {
    for (name, args) in cases() {
        compare(
            name,
            args,
            json!({"sceneItemId":42,"extension":{"keep":true}}),
            false,
        )
        .await;
    }
}
#[tokio::test]
async fn all_twenty_explicit_rejections_keep_source_error_prefix_and_no_false_acceptance() {
    for (name, args) in cases() {
        compare(name, args, json!({}), true).await;
    }
}
#[tokio::test]
async fn duplicate_validates_declared_output_and_keeps_all_extension_fields_without_observation() {
    for response in [
        json!({"sceneItemId":"wrong","privateExtra":"not a diagnostic"}),
        json!({"sceneItemId":null,"extra":[1,true,null]}),
        json!({}),
        json!(42),
        Value::Null,
    ] {
        let result = compare(
            "obs-duplicate-scene-item",
            json!({"sceneItemId":7}),
            response.clone(),
            false,
        )
        .await;
        if response["sceneItemId"].is_string() {
            assert_eq!(result["isError"], true);
            assert!(!result.to_string().contains("not a diagnostic"));
        }
    }
}
#[tokio::test]
async fn custom_defaults_undefined_labels_and_js_response_coercion_match_source() {
    for args in [
        json!({}),
        json!({"enabled":true}),
        json!({"sceneName":"Review","sourceName":"App"}),
    ] {
        compare("obs-create-scene-item", args, json!({}), false).await;
    }
    for id in [
        json!(null),
        json!([1, null, "text"]),
        json!({}),
        json!({"toString":"not-callable"}),
    ] {
        compare(
            "obs-create-scene-item",
            json!({}),
            json!({"sceneItemId":id}),
            false,
        )
        .await;
    }
}
#[test]
fn nullable_overrides_and_tbar_defaults_preserve_public_args_and_invalid_controls() {
    for args in [
        json!({}),
        json!({"transitionName":null}),
        json!({"transitionDuration":null}),
        json!({"transitionName":"Fade","transitionDuration":50}),
    ] {
        let call = Call::parse("obs-set-scene-transition-override", &args).unwrap();
        assert_eq!(call.arguments(), args);
    }
    let call = Call::parse("obs-set-tbar-position", &json!({"position":0.5})).unwrap();
    assert_eq!(call.arguments(), json!({"position":0.5,"release":true}));
    for (name, args) in [
        (
            "obs-set-tbar-position",
            json!({"position":0.5,"release":null}),
        ),
        (
            "obs-set-scene-transition-override",
            json!({"transitionDuration":49}),
        ),
        (
            "obs-set-scene-transition-override",
            json!({"transitionDuration":50.5}),
        ),
        ("obs-create-scene-item", json!({"enabled":null})),
        ("obs-set-scene-name", json!({"newSceneName":""})),
    ] {
        assert!(Call::parse(name, &args).is_none());
    }
}
#[test]
fn narrow_output_projection_rejects_unowned_schemas_and_remains_diagnostic_only() {
    let catalog: Value = serde_json::from_str(include_str!("../assets/tools.json")).unwrap();
    let schema = catalog["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "obs-duplicate-scene-item")
        .unwrap()["outputSchema"]
        .clone();
    assert!(
        tool_validation::tool_output_error("obs-create-scene-item", &schema, &json!({})).is_err()
    );
    assert_eq!(
        tool_validation::tool_output_error("obs-duplicate-scene-item", &json!({}), &json!({})),
        Err(tool_validation::ProjectionUnavailable::SchemaMismatch)
    );
    assert_eq!(
        tool_validation::tool_output_error(
            "obs-duplicate-scene-item",
            &schema,
            &json!({"sceneItemId":null})
        ),
        Err(tool_validation::ProjectionUnavailable::NoIssues)
    );
}
