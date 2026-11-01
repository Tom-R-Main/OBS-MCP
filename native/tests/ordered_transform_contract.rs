// SPDX-License-Identifier: GPL-2.0-only
//! Fixed read projection: actual Source OBS client and physical loopback frames.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    ordered_transform::{ProjectionError, SceneItemSelectors, TransformReadbackError},
    session::{BeforeDispatch, Invalidation, ObsSession, RequestError, SessionOptions},
};
use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(10);
type Peer = WebSocketStream<TcpStream>;
fn selectors() -> SceneItemSelectors {
    SceneItemSelectors {
        canvas_uuid: None,
        scene_name: Some("Demo".into()),
        scene_uuid: None,
        scene_item_id: 2,
    }
}
async fn receive(peer: &mut Peer) -> Value {
    loop {
        match timeout(WAIT, peer.next()).await.unwrap().unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            Message::Ping(_) => peer.flush().await.unwrap(),
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}
async fn send(peer: &mut Peer, value: Value) {
    peer.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn hello(socket: TcpStream) -> Peer {
    let mut peer = accept_async(socket).await.unwrap();
    send(
        &mut peer,
        json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}}),
    )
    .await;
    assert_eq!(receive(&mut peer).await["op"], 1);
    send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
    peer
}
async fn raw_reply(peer: &mut Peer, request: &Value, body: &str, success: bool) {
    let prefix = format!(
        r#"{{"op":7,"d":{{"requestId":{},"requestType":{},"requestStatus":{{"result":{},"code":{}}},"responseData":"#,
        request["d"]["requestId"],
        request["d"]["requestType"],
        success,
        if success { 100 } else { 602 }
    );
    peer.send(Message::Text((prefix + body + "}}").into()))
        .await
        .unwrap();
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
async fn source(url: &str) -> String {
    let script = r#"import{OBSWebSocketClient}from'./build/client.js';let text='';for await(const b of process.stdin)text+=b;const spec=JSON.parse(text),client=new OBSWebSocketClient(spec.url);try{await client.connect();const data=await client.sendRequest('GetSceneItemTransform',{sceneName:'Demo',sceneItemId:2});console.log(JSON.stringify({text:JSON.stringify(data?.sceneItemTransform??{})}));}finally{await client.disconnect();}"#;
    let root = std::env::var_os("OBS_CUSTOM_SOURCE_ROOT").unwrap_or_else(|| {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .into_os_string()
    });
    let mut child = tokio::process::Command::new("node")
        .args(["--input-type=module", "-e", script])
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&json!({"url":url})).unwrap())
        .await
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut bytes = Vec::new();
    let completed = timeout(WAIT, async {
        stdout.read_to_end(&mut bytes).await.unwrap();
        child.wait().await.unwrap()
    })
    .await;
    match completed {
        Ok(status) => assert!(status.success(), "Source OBS client failed"),
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            panic!("Source watchdog expired after joined cleanup");
        }
    }
    serde_json::from_slice::<Value>(&bytes).unwrap()["text"]
        .as_str()
        .unwrap()
        .to_owned()
}
#[tokio::test]
async fn actual_source_readback_order_duplicates_escapes_numeric_and_primitive_contract() {
    let cases = [
        r#"{"sceneItemTransform":{"z":3,"a":1}}"#,
        r#"{"sceneItemTransform":{"z":3,"10":"ten","2":"two","a":1,"z":4,"01":"leading","4294967294":"last-index","4294967295":"not-index","2":"TWO","nested":{"b":1,"a":2}}}"#,
        r#"{"sceneItemTransform":{"a":1,"\u0061":9,"__proto__":{"z":3,"a":1},"text":"line\n\u0000日本語\u2028\u2029"}}"#,
        r#"{"sceneItemTransform":{"bad":1},"sceneItemTransform":{"z":9,"a":1}}"#,
        r#"{"sceneItemTransform":{"negativeZero":-0.0,"positiveFloat":1.0,"safeLow":9007199254740991,"safeEdge":9007199254740992,"rounded":9007199254740993,"nextEven":9007199254740994,"negativeRounded":-9007199254740993,"fullU64":18446744073709551615,"fullI64":-9223372036854775808,"shortInteger":1000000000000000128,"minSubnormal":5e-324,"smallExponent":1e-7,"threshold":0.000001,"largeExponent":1e21}}"#,
        r#"{"sceneItemTransform":{"$serde_json::private::RawValue":"123","z":{"$serde_json::private::RawValue":"{\"owned\":true}"}}}"#,
        r#"{"sceneItemTransform":{"\u0024serde_json::private::RawValue":{"z":3,"a":1},"a":2}}"#,
        "{}",
        r#"{"sceneItemTransform":null}"#,
        r#"{"sceneItemTransform":false}"#,
        r#"{"sceneItemTransform":0}"#,
        r#"{"sceneItemTransform":""}"#,
        r#"{"sceneItemTransform":[{"z":3,"a":1},0,false]}"#,
        "false",
        "null",
        r#"{"sceneItemTransform":{"sourceWidth":0,"z":3,"a":1}}"#,
    ];
    for body in cases {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let body = body.to_owned();
        let peer = tokio::spawn(async move {
            for _ in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let mut peer = hello(socket).await;
                loop {
                    let request = receive(&mut peer).await;
                    match request["d"]["requestType"].as_str().unwrap() {
                        "GetVersion" => {
                            let version=json!({"obsVersion":"32.2.2","obsWebSocketVersion":"5.7.0","rpcVersion":1,"availableRequests":["GetVersion","GetSceneItemTransform"]}).to_string();
                            raw_reply(&mut peer, &request, &version, true).await;
                        }
                        "GetSceneItemTransform" => {
                            raw_reply(&mut peer, &request, &body, true).await;
                            close(&mut peer).await;
                            break;
                        }
                        other => panic!("unexpected request: {other}"),
                    }
                }
            }
        });
        let expected = source(&url).await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let result = owner
            .handle()
            .scene_item_transform_checked(owner.token(), selectors(), WAIT, || Ok(()))
            .await
            .unwrap();
        owner.shutdown().await.unwrap();
        timeout(WAIT, peer).await.unwrap().unwrap();
        assert_eq!(result.applied_json.as_str(), expected);
        assert!(!format!("{result:?}").contains("negativeZero"));
    }
}
#[tokio::test]
async fn stale_and_rejected_oversize_subtrees_never_allocate_projection_or_retire_usable_session() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut peer = hello(socket).await;
        let request = receive(&mut peer).await;
        let body = format!(
            r#"{{"sceneItemTransform":"{}"}}"#,
            "x".repeat(8 * 1024 * 1024)
        );
        let mut stale = request.clone();
        stale["d"]["requestId"] = json!("foreign");
        raw_reply(&mut peer, &stale, &body, true).await;
        raw_reply(&mut peer, &request, &body, false).await;
        let after = receive(&mut peer).await;
        raw_reply(
            &mut peer,
            &after,
            r#"{"sceneItemTransform":{"z":3,"a":1}}"#,
            true,
        )
        .await;
        close(&mut peer).await;
    });
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    assert!(matches!(
        handle
            .scene_item_transform_checked(owner.token(), selectors(), WAIT, || Ok(()))
            .await,
        Err(TransformReadbackError::Request(RequestError::Rejected {
            code: 602
        }))
    ));
    assert!(handle.invalidation().is_none());
    assert_eq!(
        handle
            .scene_item_transform_checked(owner.token(), selectors(), WAIT, || Ok(()))
            .await
            .unwrap()
            .applied_json
            .as_str(),
        r#"{"z":3,"a":1}"#
    );
    owner.shutdown().await.unwrap();
    timeout(WAIT, peer).await.unwrap().unwrap();
}
#[tokio::test]
async fn valid_oversize_projection_is_local_failure_after_read_reply_and_stop_stays_usable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut peer = hello(socket).await;
        let request = receive(&mut peer).await;
        let body = format!(
            r#"{{"sceneItemTransform":"{}"}}"#,
            "x".repeat(8 * 1024 * 1024)
        );
        raw_reply(&mut peer, &request, &body, true).await;
        let after = receive(&mut peer).await;
        assert_eq!(after["d"]["requestType"], "StopRecord");
        raw_reply(&mut peer, &after, "{}", true).await;
        close(&mut peer).await;
    });
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    assert!(matches!(
        handle
            .scene_item_transform_checked(owner.token(), selectors(), WAIT, || Ok(()))
            .await,
        Err(TransformReadbackError::Projection(
            ProjectionError::RawCapacity
        ))
    ));
    assert!(handle.invalidation().is_none());
    handle
        .request(owner.token(), "StopRecord", json!({}), WAIT)
        .await
        .unwrap();
    owner.shutdown().await.unwrap();
    timeout(WAIT, peer).await.unwrap().unwrap();
}
#[tokio::test]
async fn final_guard_and_wrong_response_type_precede_projection_allocation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut peer = hello(socket).await;
        let mut request = receive(&mut peer).await;
        assert_eq!(request["d"]["requestType"], "GetSceneItemTransform");
        request["d"]["requestType"] = json!("GetOther");
        let body = format!(
            r#"{{"sceneItemTransform":"{}"}}"#,
            "x".repeat(8 * 1024 * 1024)
        );
        raw_reply(&mut peer, &request, &body, true).await;
        close(&mut peer).await;
    });
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    assert!(matches!(
        handle
            .scene_item_transform_checked(owner.token(), selectors(), WAIT, || Err(
                "changed authority".into()
            ))
            .await,
        Err(TransformReadbackError::Request(
            RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(_))
        ))
    ));
    assert!(matches!(
        handle
            .scene_item_transform_checked(owner.token(), selectors(), WAIT, || Ok(()))
            .await,
        Err(TransformReadbackError::Request(RequestError::Unknown(
            Invalidation::ResponseIdentityMismatch
        )))
    ));
    owner.shutdown().await.unwrap();
    timeout(WAIT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn safe_selector_domain_matches_source_and_refuses_before_wire_or_guard() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut peer = hello(socket).await;
        for id in [0u64, 9_007_199_254_740_991] {
            let request = receive(&mut peer).await;
            assert_eq!(request["d"]["requestData"]["sceneItemId"], id);
            assert_eq!(request["d"]["requestData"]["canvasUuid"], "");
            assert!(request["d"]["requestData"].get("sceneUuid").is_none());
            raw_reply(&mut peer, &request, "{}", true).await;
        }
        close(&mut peer).await;
    });
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let calls = Arc::new(AtomicUsize::new(0));
    let guard = calls.clone();
    let mut invalid = selectors();
    invalid.scene_item_id = 9_007_199_254_740_992;
    assert!(matches!(
        handle
            .scene_item_transform_checked(owner.token(), invalid, WAIT, move || {
                guard.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await,
        Err(TransformReadbackError::Request(
            RequestError::BeforeDispatch(BeforeDispatch::InvalidRequest)
        ))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for id in [0u64, 9_007_199_254_740_991] {
        let mut value = selectors();
        value.scene_item_id = id;
        value.canvas_uuid = Some(String::new());
        handle
            .scene_item_transform_checked(owner.token(), value, WAIT, || Ok(()))
            .await
            .unwrap();
    }
    owner.shutdown().await.unwrap();
    timeout(WAIT, peer).await.unwrap().unwrap();
}
