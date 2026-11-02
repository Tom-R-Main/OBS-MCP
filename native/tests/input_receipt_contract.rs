// SPDX-License-Identifier: GPL-2.0-only
//! Real socket and durable receipt controls for the input mutation owner.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    clock,
    control::ControlDomain,
    custody::Clock,
    generic_requests::{self, Call, GenericServices, ReadbackServices},
    operation_store::OperationStore,
    receipt_broker::{ReceiptBroker, ScopeKind},
    scene_audit::ScreenshotLimit,
    session::{ObsSession, SessionOptions},
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, time::Instant};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const ID: &str = "00000000-0000-4000-8000-000000000001";
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "input-receipt-{}-{}",
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
#[derive(Clone, Copy)]
enum Mode {
    Normal,
    BadId,
    LoseAck,
    ReplaceReceiptNamespace,
}
struct Peer {
    endpoint: String,
    trace: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    async fn new(mode: Mode, root: PathBuf) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let trace = Arc::new(Mutex::new(Vec::new()));
        let seen = trace.clone();
        let task = tokio::spawn(async move {
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
                seen.lock().unwrap().push(frame.clone());
                let request = frame["d"]["requestType"].as_str().unwrap();
                if matches!(request, "CreateInput" | "SetInputSettings") {
                    if matches!(mode, Mode::LoseAck) {
                        let _ = socket.close(None).await;
                        return;
                    }
                    if matches!(mode, Mode::ReplaceReceiptNamespace) {
                        fs::rename(
                            root.join("operation-receipts"),
                            root.join("saved-operation-receipts"),
                        )
                        .unwrap();
                        fs::create_dir(root.join("operation-receipts")).unwrap();
                        fs::write(
                            root.join("operation-receipts/human.txt"),
                            b"preserve replacement",
                        )
                        .unwrap();
                    }
                }
                let data = match request {
                    "CreateInput" if matches!(mode, Mode::BadId) => {
                        json!({"sceneItemId":{"toString":null}})
                    }
                    "CreateInput" => json!({"sceneItemId":42}),
                    "SetInputSettings" => json!({}),
                    "GetInputSettings" => {
                        json!({"inputKind":"screen_capture","inputSettings":{"window":42}})
                    }
                    "GetCurrentProgramScene" => json!({"currentProgramSceneName":"Main"}),
                    "GetSceneItemList" => json!({"sceneItems":[]}),
                    other => panic!("unexpected input owner request {other}"),
                };
                if socket.send(Message::Text(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":request,"requestStatus":{"result":true,"code":100},"responseData":data}}).to_string().into())).await.is_err(){return;}
            }
        });
        Self {
            endpoint,
            trace,
            task,
        }
    }
}
fn call() -> Call {
    Call::parse("obs-create-input",&json!({"sceneName":"Main","inputName":"Fixture","inputKind":"screen_capture","inputSettings":{"window":42},"includeScreenshot":false})).unwrap()
}
fn services() -> GenericServices {
    GenericServices {
        readback: Some(ReadbackServices {
            capabilities: None,
            input_screenshot_limit: Some(Ok(ScreenshotLimit::default())),
        }),
        ..Default::default()
    }
}
fn clock() -> Clock {
    Arc::new(|| clock::from_epoch_duration(Duration::from_millis(1793448000000)).unwrap())
}
async fn execute(broker: &ReceiptBroker, session: &ObsSession, services: GenericServices) -> Value {
    let call = call();
    generic_requests::execute_with_services(
        broker.scope(ScopeKind::Generic(call.kind())).unwrap(),
        call,
        session.handle(),
        Arc::new(Mutex::new(ControlDomain::default())),
        clock(),
        "fixture".into(),
        Cancellation::new(),
        ID.into(),
        Duration::from_secs(3),
        Instant::now(),
        services,
    )
    .await
}
async fn connect(
    peer: &Peer,
    request_bytes: Option<usize>,
) -> (ObsSession, obs_mcp_native::session::EventReceiver) {
    let mut options = SessionOptions::default();
    if let Some(bytes) = request_bytes {
        options.request_bytes = bytes;
    }
    ObsSession::connect(&peer.endpoint, None, options)
        .await
        .unwrap()
}
#[tokio::test]
async fn required_input_context_refuses_before_any_receipt_or_wire() {
    let dir = Scratch::new();
    let peer = Peer::new(Mode::Normal, dir.0.clone()).await;
    let (session, events) = connect(&peer, None).await;
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &peer.endpoint).unwrap()).unwrap();
    let result = execute(&broker, &session, GenericServices::default()).await;
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("no OBS request was dispatched")
    );
    assert_eq!(broker.read_view().summary()["total"], 0);
    assert!(!dir.0.join("operation-receipts").exists());
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
    assert!(peer.trace.lock().unwrap().is_empty());
}
#[tokio::test]
async fn accepted_mutation_readback_never_promotes_postcondition_or_replays() {
    let dir = Scratch::new();
    let peer = Peer::new(Mode::Normal, dir.0.clone()).await;
    let (session, events) = connect(&peer, None).await;
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &peer.endpoint).unwrap()).unwrap();
    let result = execute(&broker, &session, services()).await;
    assert_ne!(result["isError"], true);
    let receipt = broker.read_view().read_any(ID).unwrap();
    assert_eq!(receipt["phase"], "accepted");
    assert_eq!(receipt["requestAccepted"], true);
    assert_eq!(receipt["postconditionObserved"], false);
    assert_eq!(
        peer.trace
            .lock()
            .unwrap()
            .iter()
            .map(|frame| frame["d"]["requestType"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        [
            "CreateInput",
            "GetInputSettings",
            "GetCurrentProgramScene",
            "GetSceneItemList"
        ]
    );
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
}
#[tokio::test]
async fn source_result_coercion_error_keeps_accepted_and_starts_no_readback() {
    let dir = Scratch::new();
    let peer = Peer::new(Mode::BadId, dir.0.clone()).await;
    let (session, events) = connect(&peer, None).await;
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &peer.endpoint).unwrap()).unwrap();
    assert_eq!(
        execute(&broker, &session, services()).await["isError"],
        true
    );
    let receipt = broker.read_view().read_any(ID).unwrap();
    assert_eq!(receipt["phase"], "accepted");
    assert_eq!(receipt["postconditionObserved"], false);
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
    assert_eq!(peer.trace.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn failed_accepted_publication_keeps_dispatching_and_never_starts_readback() {
    let dir = Scratch::new();
    let peer = Peer::new(Mode::ReplaceReceiptNamespace, dir.0.clone()).await;
    let (session, events) = connect(&peer, None).await;
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &peer.endpoint).unwrap()).unwrap();
    let result = execute(&broker, &session, services()).await;
    assert_eq!(result["isError"], true);
    assert!(broker.admission().is_err());
    let receipt: Value = serde_json::from_slice(
        &fs::read(
            dir.0
                .join("saved-operation-receipts")
                .join(format!("{ID}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(receipt["phase"], "dispatching");
    assert_eq!(receipt["postconditionObserved"], false);
    assert_eq!(
        fs::read(dir.0.join("operation-receipts/human.txt")).unwrap(),
        b"preserve replacement"
    );
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
    assert_eq!(peer.trace.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn lost_ack_stays_unknown_on_reopen_without_followup_or_replay_permission() {
    let dir = Scratch::new();
    let peer = Peer::new(Mode::LoseAck, dir.0.clone()).await;
    let (session, events) = connect(&peer, None).await;
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &peer.endpoint).unwrap()).unwrap();
    assert_eq!(
        execute(&broker, &session, services()).await["isError"],
        true
    );
    let receipt = broker.read_view().read_any(ID).unwrap();
    assert_eq!(receipt["phase"], "unknown");
    assert_eq!(receipt["requestAccepted"], Value::Null);
    assert_eq!(receipt["postconditionObserved"], false);
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
    drop(broker);
    let reopened =
        ReceiptBroker::new(OperationStore::new(&dir.0, &peer.endpoint).unwrap()).unwrap();
    assert!(reopened.admission().unwrap().blocking);
    assert!(reopened.scope(ScopeKind::Generic(call().kind())).is_err());
    assert_eq!(reopened.read_view().read_any(ID).unwrap(), receipt);
    assert_eq!(peer.trace.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn byte_policy_refusal_reports_known_no_send_and_keeps_not_dispatched() {
    let dir = Scratch::new();
    let peer = Peer::new(Mode::Normal, dir.0.clone()).await;
    let (session, events) = connect(&peer, Some(128)).await;
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &peer.endpoint).unwrap()).unwrap();
    let result = execute(&broker, &session, services()).await;
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("was not sent")
    );
    let receipt = broker.read_view().read_any(ID).unwrap();
    assert_eq!(receipt["phase"], "not-dispatched");
    assert_eq!(receipt["postconditionObserved"], false);
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
    assert!(peer.trace.lock().unwrap().is_empty());
}
