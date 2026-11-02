// SPDX-License-Identifier: GPL-2.0-only
//! Closed ACK-only public Stop callbacks; not the owned recording Stop lane.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    clock,
    control::{ClaimOutcome, ControlDomain},
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
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(5);
const EPOCH: u64 = 1_793_448_000_000;
fn now() -> Clock {
    Arc::new(|| clock::from_epoch_duration(Duration::from_millis(EPOCH)).unwrap())
}
fn id(index: u8) -> String {
    format!("00000000-0000-4000-8000-{index:012}")
}
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "stop3-contract-{}-{}",
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
fn cases() -> Vec<(&'static str, &'static str, Value, &'static str)> {
    vec![
        (
            "obs-stop-stream",
            "StopStream",
            json!({}),
            "Successfully stopped streaming",
        ),
        (
            "obs-stop-virtual-cam",
            "StopVirtualCam",
            json!({}),
            "Virtual camera stopped",
        ),
        (
            "obs-stop-replay-buffer",
            "StopReplayBuffer",
            json!({}),
            "Replay buffer stopped",
        ),
    ]
}
#[derive(Clone, Copy)]
enum Mode {
    Ack,
    LoseAck,
    HoldRead,
}
struct Peer {
    endpoint: String,
    wire: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
    seen: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}
impl Peer {
    async fn new(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let wire = Arc::new(Mutex::new(Vec::new()));
        let observed = wire.clone();
        let (seen_tx, seen) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
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
            let mut seen_tx = Some(seen_tx);
            let mut release_rx = Some(release_rx);
            while let Some(message) = socket.next().await {
                let Ok(Message::Text(text)) = message else {
                    let _ = socket.flush().await;
                    return;
                };
                let frame: Value = serde_json::from_str(&text).unwrap();
                observed.lock().unwrap().push(frame.clone());
                if matches!(mode, Mode::LoseAck) {
                    let _ = socket.close(None).await;
                    return;
                }
                if matches!(mode, Mode::HoldRead) && frame["d"]["requestType"] == "GetRecordStatus"
                {
                    seen_tx.take().unwrap().send(()).unwrap();
                    let _ = release_rx.take().unwrap().await;
                }
                let response = json!({"op":7,"d":{"requestType":frame["d"]["requestType"],"requestId":frame["d"]["requestId"],"requestStatus":{"result":true,"code":100},"responseData":{"ignored":true}}});
                if socket
                    .send(Message::Text(response.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Self {
            endpoint,
            wire,
            task,
            seen,
            release,
        }
    }
}
#[test]
fn three_fixed_ack_only_stops_have_no_argument_or_followup_authority() {
    for (name, request, args, _) in cases() {
        let call = Call::parse(name, &args).expect(name);
        assert_eq!(call.kind().request_type(), request);
        assert_eq!(serde_json::to_value(call.kind()).unwrap(), request);
        assert!(!call.requires_readback());
        assert_eq!(call.arguments(), json!({}));
        assert_eq!(
            Call::parse(name, &json!({"PRIVATE":{"requestType":"StopRecord"}}))
                .unwrap()
                .arguments(),
            json!({})
        );
        assert!(Call::parse(name, &Value::Null).is_none());
    }
    // The separately admitted atomic toggle has its own closed identity;
    // it does not become an explicit Stop or gain a confirmation/readback.
    let toggle = Call::parse("obs-toggle-stream", &json!({"discarded":true})).unwrap();
    assert_eq!(toggle.kind().request_type(), "ToggleStream");
    assert_eq!(toggle.arguments(), json!({}));
    assert!(!toggle.requires_readback());
    assert!(toggle.confirmed_output_kind().is_none());
    for name in [
        "obs-stop-record",
        "obs-stop-output",
        "obs-start-record",
        "obs-call-request",
        "StopStream",
    ] {
        assert!(Call::parse(name, &json!({})).is_none());
    }
}
#[tokio::test]
async fn exact_three_no_data_rpcs_publish_only_ack_acceptance_without_status_reads() {
    let scratch = Scratch::new();
    let peer = Peer::new(Mode::Ack).await;
    let (session, events) = ObsSession::connect(&peer.endpoint, None, SessionOptions::default())
        .await
        .unwrap();
    let broker =
        ReceiptBroker::new(OperationStore::new(&scratch.0, &peer.endpoint).unwrap()).unwrap();
    let control = Arc::new(Mutex::new(ControlDomain::default()));
    assert!(matches!(
        control
            .lock()
            .unwrap()
            .claim("other-client", 1, "foreign control", EPOCH),
        Ok(ClaimOutcome::Claimed(_))
    ));
    for (index, (name, _, args, message)) in cases().into_iter().enumerate() {
        let call = Call::parse(name, &args).unwrap();
        let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
        let result = generic_requests::execute(
            port,
            call,
            session.handle(),
            control.clone(),
            now(),
            "stop3-contract".into(),
            Cancellation::new(),
            id(index as u8 + 1),
            WAIT,
            Instant::now(),
        )
        .await;
        assert_eq!(
            result,
            json!({"content":[{"type":"text","text":message}],"structuredContent":{"message":message}})
        );
        let receipt = broker.read_view().read_any(&id(index as u8 + 1)).unwrap();
        assert_eq!(receipt["phase"], "accepted");
        assert_eq!(receipt["requestAccepted"], true);
        assert_eq!(receipt["postconditionObserved"], false);
    }
    let wire = peer
        .wire
        .lock()
        .unwrap()
        .iter()
        .map(|frame| {
            let mut data = frame["d"].clone();
            data.as_object_mut().unwrap().remove("requestId");
            data
        })
        .collect::<Vec<_>>();
    assert_eq!(
        wire,
        cases()
            .iter()
            .map(|(_, request, _, _)| json!({"requestType":request}))
            .collect::<Vec<_>>()
    );
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
}
#[tokio::test]
async fn lost_stop_ack_is_unknown_without_replay_or_uncertainty_bypass() {
    let scratch = Scratch::new();
    let peer = Peer::new(Mode::LoseAck).await;
    let (session, events) = ObsSession::connect(&peer.endpoint, None, SessionOptions::default())
        .await
        .unwrap();
    let broker =
        ReceiptBroker::new(OperationStore::new(&scratch.0, &peer.endpoint).unwrap()).unwrap();
    let call = Call::parse("obs-stop-stream", &json!({})).unwrap();
    let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
    let result = generic_requests::execute(
        port,
        call.clone(),
        session.handle(),
        Arc::new(Mutex::new(ControlDomain::default())),
        now(),
        "stop3-contract".into(),
        Cancellation::new(),
        id(1),
        WAIT,
        Instant::now(),
    )
    .await;
    assert_eq!(result["isError"], true);
    let receipt = broker.read_view().read_any(&id(1)).unwrap();
    assert_eq!(receipt["phase"], "unknown");
    assert_eq!(receipt["requestAccepted"], Value::Null);
    assert_eq!(receipt["postconditionObserved"], false);
    for (name, _, args, _) in cases() {
        assert!(
            broker
                .scope(ScopeKind::Generic(Call::parse(name, &args).unwrap().kind()))
                .is_err()
        );
    }
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
    assert_eq!(peer.wire.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn queued_generic_stop_preserves_foreign_lease_exemption_but_not_priority() {
    // Deliberately uses the ordinary Generic scope. Only Service's closed
    // SafetyStop scope may select the separate public transport lane.
    let scratch = Scratch::new();
    let peer = Peer::new(Mode::HoldRead).await;
    let (session, events) = ObsSession::connect(&peer.endpoint, None, SessionOptions::default())
        .await
        .unwrap();
    let read = tokio::spawn({
        let handle = session.handle();
        async move {
            handle
                .request(handle.token(), "GetRecordStatus", Value::Null, WAIT)
                .await
        }
    });
    timeout(WAIT, peer.seen).await.unwrap().unwrap();
    let broker =
        ReceiptBroker::new(OperationStore::new(&scratch.0, &peer.endpoint).unwrap()).unwrap();
    let call = Call::parse("obs-stop-stream", &json!({})).unwrap();
    let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
    let control = Arc::new(Mutex::new(ControlDomain::default()));
    let job = tokio::spawn(generic_requests::execute(
        port,
        call,
        session.handle(),
        control.clone(),
        now(),
        "stop3-contract".into(),
        Cancellation::new(),
        id(1),
        WAIT,
        Instant::now(),
    ));
    timeout(WAIT, async {
        loop {
            if broker
                .read_view()
                .read_any(&id(1))
                .ok()
                .is_some_and(|r| r["phase"] == "dispatching")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        control
            .lock()
            .unwrap()
            .claim("other-client", 1, "queue control", EPOCH),
        Ok(ClaimOutcome::Claimed(_))
    ));
    assert_eq!(
        peer.wire.lock().unwrap().len(),
        1,
        "the Generic Stop retains ordinary transport FIFO"
    );
    peer.release.send(()).unwrap();
    read.await.unwrap().unwrap();
    let result = job.await.unwrap();
    assert_ne!(result["isError"], true);
    assert_eq!(
        broker.read_view().read_any(&id(1)).unwrap()["phase"],
        "accepted"
    );
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
    assert_eq!(peer.wire.lock().unwrap().len(), 2);
}
