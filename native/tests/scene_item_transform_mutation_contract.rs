// SPDX-License-Identifier: GPL-2.0-only
//! Actual Source callbacks versus the pure module over the existing typed socket.
//! This does not admit a Generic mutation route or claim receipt integration.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    scene_item_transform_mutation::{self as transform, Request, SessionReadback},
    session::{ObsSession, SessionOptions},
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    process::Command,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
type Peer = tokio_tungstenite::WebSocketStream<TcpStream>;
const WAIT: Duration = Duration::from_secs(6);
async fn receive(peer: &mut Peer) -> Value {
    loop {
        match timeout(WAIT, peer.next()).await.unwrap().unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) => peer.flush().await.unwrap(),
            other => panic!("unexpected fixture frame {other:?}"),
        }
    }
}
async fn send(peer: &mut Peer, value: Value) {
    peer.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn hello(stream: TcpStream) -> Peer {
    let mut peer = accept_async(stream).await.unwrap();
    send(
        &mut peer,
        json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}}),
    )
    .await;
    assert_eq!(receive(&mut peer).await["op"], 1);
    send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
    peer
}
async fn close(peer: &mut Peer) {
    while let Some(message) = timeout(WAIT, peer.next()).await.unwrap() {
        match message {
            Ok(Message::Close(_)) | Err(_) => {
                let _ = peer.flush().await;
                break;
            }
            _ => {}
        }
    }
}
async fn reply(peer: &mut Peer, request: &Value, body: &str, rejected: bool) {
    let head = format!(
        r#"{{"op":7,"d":{{"requestId":{},"requestType":{},"requestStatus":{{"result":{},"code":{},"comment":"PRIVATE transform fixture failure"}},"responseData":"#,
        request["d"]["requestId"],
        request["d"]["requestType"],
        !rejected,
        if rejected { 500 } else { 100 }
    );
    peer.send(Message::Text((head + body + "}}").into()))
        .await
        .unwrap();
}
fn source() -> Value {
    let output = Command::new("node")
        .args([
            "--input-type=module",
            "-e",
            include_str!("fixtures/transform_source_oracle.mjs"),
        ])
        .current_dir(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap(),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Source oracle: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn response_body(row: &Value) -> String {
    match row["case"].as_str().unwrap() {
        "numeric-request" => {
            r#"{"sceneItemTransform":{"huge":18446744073709551615,"tie":144713588892437.625}}"#
                .into()
        }
        "selected-null" => r#"{"sceneItemTransform":null}"#.into(),
        "response-null" => "null".into(),
        "response-false" => "false".into(),
        "response-array" => "[]".into(),
        _ => {
            let text = row["result"]["content"][0]["text"].as_str().unwrap();
            let applied = text
                .split('\n')
                .find_map(|line| line.strip_prefix("Applied transform: "))
                .unwrap_or("{}");
            format!(r#"{{"sceneItemTransform":{applied}}}"#)
        }
    }
}
#[tokio::test]
async fn sixty_two_actual_source_callbacks_match_parser_wire_projection_and_typed_readback_results()
{
    let expected = source();
    assert_eq!(expected["callbacks"], 62);
    for row in expected["results"].as_array().unwrap() {
        let Some(request) = Request::parse(&row["args"]) else {
            assert_eq!(row["result"]["isError"], true, "{}", row["case"]);
            assert_eq!(row["trace"], json!([]));
            continue;
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let trace = Arc::new(Mutex::new(Vec::new()));
        let seen = trace.clone();
        let body = response_body(row);
        let mutation_rejected = row["case"] == "mutation-rejected";
        let readback_rejected = row["case"] == "readback-rejected";
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut peer = hello(socket).await;
            let set = receive(&mut peer).await;
            assert_eq!(set["d"]["requestType"], "SetSceneItemTransform");
            seen.lock().unwrap().push(json!({"requestType":set["d"]["requestType"],"requestData":set["d"]["requestData"]}));
            reply(&mut peer, &set, "{}", mutation_rejected).await;
            if !mutation_rejected {
                let get = receive(&mut peer).await;
                assert_eq!(get["d"]["requestType"], "GetSceneItemTransform");
                seen.lock().unwrap().push(json!({"requestType":get["d"]["requestType"],"requestData":get["d"]["requestData"]}));
                reply(&mut peer, &get, &body, readback_rejected).await;
            }
            close(&mut peer).await;
        });
        let (owner, _events) = ObsSession::connect(&endpoint, None, SessionOptions::default())
            .await
            .unwrap();
        let set = owner
            .handle()
            .request_checked(
                owner.token(),
                "SetSceneItemTransform",
                request.request_data(),
                WAIT,
                || Ok(()),
            )
            .await;
        let result = match set {
            Ok(_) => {
                let (arguments, followup) = request.into_parts();
                let mut reader = SessionReadback::new(owner.handle(), None);
                followup
                    .finish(
                        &arguments,
                        &mut reader,
                        &Cancellation::new(),
                        Instant::now() + WAIT,
                    )
                    .await
            }
            Err(reason) => {
                transform::failure(&reason.obs_failure_message("SetSceneItemTransform").unwrap())
            }
        };
        assert_eq!(
            result, row["result"],
            "complete unnormalized callback result: {} legacy={}",
            row["case"], row["legacy"]
        );
        assert_eq!(
            *trace.lock().unwrap(),
            *row["trace"].as_array().unwrap(),
            "exact effects: {}",
            row["case"]
        );
        owner.shutdown().await.unwrap();
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
}
#[tokio::test]
async fn known_unsupported_cancelled_and_expired_followups_send_no_getter_or_version_probe() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut peer = hello(stream).await;
        let message = timeout(WAIT, peer.next()).await.unwrap().unwrap().unwrap();
        assert!(matches!(message, Message::Close(_)));
        let _ = peer.flush().await;
    });
    let (owner, _events) = ObsSession::connect(&endpoint, None, SessionOptions::default())
        .await
        .unwrap();
    for mode in 0..3 {
        let request = Request::parse(&json!({"sceneItemId":0,"sceneName":""})).unwrap();
        let (arguments, followup) = request.into_parts();
        let cancel = Cancellation::new();
        if mode == 1 {
            cancel.cancel();
        }
        let caps = (mode == 0).then(|| Arc::new(HashSet::new()));
        let mut reader = SessionReadback::new(owner.handle(), caps);
        let deadline = if mode == 2 {
            Instant::now() - Duration::from_millis(1)
        } else {
            Instant::now() + WAIT
        };
        let result = followup
            .finish(&arguments, &mut reader, &cancel, deadline)
            .await;
        assert_ne!(result["isError"], true);
        assert_eq!(result["structuredContent"]["sceneItemTransform"], json!({}));
        assert_eq!(
            result["structuredContent"]["warnings"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    owner.shutdown().await.unwrap();
    timeout(WAIT, peer).await.unwrap().unwrap();
}
#[tokio::test]
async fn cancelled_frontend_drains_the_same_typed_read_and_keeps_the_physical_socket_usable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (entered, seen) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut peer = hello(stream).await;
        let get = receive(&mut peer).await;
        assert_eq!(get["d"]["requestType"], "GetSceneItemTransform");
        entered.send(()).unwrap();
        resume.await.unwrap();
        reply(
            &mut peer,
            &get,
            r#"{"sceneItemTransform":{"z":3,"a":1}}"#,
            false,
        )
        .await;
        let independent = receive(&mut peer).await;
        assert_eq!(independent["d"]["requestType"], "GetVersion");
        reply(&mut peer, &independent, "{}", false).await;
        close(&mut peer).await;
    });
    let (owner, _events) = ObsSession::connect(&endpoint, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let request = Request::parse(&json!({"sceneName":"Demo","sceneItemId":2})).unwrap();
    let (arguments, followup) = request.into_parts();
    let cancel = Cancellation::new();
    let token = cancel.clone();
    let job = tokio::spawn(async move {
        let mut reader = SessionReadback::new(handle, None);
        followup
            .finish(&arguments, &mut reader, &token, Instant::now() + WAIT)
            .await
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    cancel.cancel();
    tokio::task::yield_now().await;
    assert!(
        !job.is_finished(),
        "admitted reply remains owned after cancellation"
    );
    release.send(()).unwrap();
    let result = timeout(WAIT, job).await.unwrap().unwrap();
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .ends_with("Applied transform: {\"z\":3,\"a\":1}")
    );
    owner
        .handle()
        .request(owner.token(), "GetVersion", Value::Null, WAIT)
        .await
        .unwrap();
    owner.shutdown().await.unwrap();
    timeout(WAIT, peer).await.unwrap().unwrap();
}
#[test]
fn raw_owned_arguments_round_like_source_and_debug_never_exposes_opaque_values() {
    let raw = r#"{"sceneItemId":-0.0,"positionX":144713588892437.625,"sceneItemTransform":{"positionY":7,"unsafe":9007199254740993,"u64":18446744073709551615,"secret":"fixture-only-private","nested":{"__proto__":true}}}"#;
    let request = Request::parse_owned(serde_json::from_str(raw).unwrap()).unwrap();
    assert!(!format!("{request:?}").contains("fixture-only-private"));
    let data = request.request_data();
    assert!(data["sceneItemTransform"].get("positionY").is_none());
    assert_eq!(
        data["sceneItemTransform"]["unsafe"],
        json!(9_007_199_254_740_992u64)
    );
    assert_eq!(data["sceneItemId"], 0);
    assert_eq!(data["sceneItemTransform"]["nested"]["__proto__"], true);
    let bytes = obs_mcp_native::js_json::encode_source_json(&data, Default::default()).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("\"u64\":18446744073709552000"));
    assert!(text.contains("\"positionX\":144713588892437.62"));
}
