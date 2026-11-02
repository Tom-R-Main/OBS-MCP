// SPDX-License-Identifier: GPL-2.0-only
//! Consumer declarations for the closed timeline tranche; run after admission.
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
                "timeline-contract-{}-{}",
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
            "obs-pause-record",
            "PauseRecord",
            json!({}),
            "Recording paused",
        ),
        (
            "obs-resume-record",
            "ResumeRecord",
            json!({}),
            "Recording resumed",
        ),
        (
            "obs-toggle-record-pause",
            "ToggleRecordPause",
            json!({}),
            "Recording pause toggled",
        ),
        (
            "obs-toggle-record-pause",
            "ToggleRecordPause",
            json!({}),
            "Recording pause toggled",
        ),
        (
            "obs-split-record-file",
            "SplitRecordFile",
            json!({}),
            "Recording file split",
        ),
        (
            "obs-create-record-chapter",
            "CreateRecordChapter",
            json!({}),
            "Record chapter created",
        ),
        (
            "obs-create-record-chapter",
            "CreateRecordChapter",
            json!({"chapterName":""}),
            "Record chapter created",
        ),
        (
            "obs-create-record-chapter",
            "CreateRecordChapter",
            json!({"chapterName":"A \"named\" chapter"}),
            "Record chapter \"A \"named\" chapter\" created",
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
fn fixed_timeline_identity_optional_chapter_and_no_private_followup_contract() {
    for (name, request, args, _) in cases() {
        let call = Call::parse(name, &args).expect(name);
        assert_eq!(call.kind().request_type(), request);
        assert_eq!(serde_json::to_value(call.kind()).unwrap(), request);
        assert!(!call.requires_readback());
        assert_eq!(call.arguments(), args);
    }
    for (name, _, _, _) in cases().into_iter().take(5) {
        assert_eq!(
            Call::parse(
                name,
                &json!({"discarded":{"requestType":"StopRecord","data":true}})
            )
            .unwrap()
            .arguments(),
            json!({})
        );
    }
    for chapter in [Value::Null, json!(42), json!(false), json!([]), json!({})] {
        assert!(
            Call::parse("obs-create-record-chapter", &json!({"chapterName":chapter})).is_none()
        );
    }
    let private = Call::parse(
        "obs-create-record-chapter",
        &json!({"chapterName":"PRIVATE CHAPTER"}),
    )
    .unwrap();
    assert!(!format!("{private:?}").contains("PRIVATE CHAPTER"));
    for name in [
        "obs-stop-record",
        "obs-start-record",
        "obs-call-request",
        "PauseRecord",
    ] {
        assert!(Call::parse(name, &json!({})).is_none());
    }
}
#[tokio::test]
async fn eight_callbacks_emit_exact_no_data_or_chapter_objects_and_only_accepted_receipts() {
    // Source authority: timeline-source-controls-v2.json's exact eight effects.
    let calls = cases()
        .into_iter()
        .map(|(name, request, args, message)| {
            (
                Call::parse(name, &args).expect(name),
                request,
                args,
                message,
            )
        })
        .collect::<Vec<_>>();
    let scratch = Scratch::new();
    let peer = Peer::new(Mode::Ack).await;
    let (session, events) = ObsSession::connect(&peer.endpoint, None, SessionOptions::default())
        .await
        .unwrap();
    let broker =
        ReceiptBroker::new(OperationStore::new(&scratch.0, &peer.endpoint).unwrap()).unwrap();
    let control = Arc::new(Mutex::new(ControlDomain::default()));
    for (index, (call, _, _, message)) in calls.iter().enumerate() {
        let receipt_id = id(index as u8 + 1);
        let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
        let result = generic_requests::execute(
            port,
            call.clone(),
            session.handle(),
            control.clone(),
            now(),
            "timeline-contract".into(),
            Cancellation::new(),
            receipt_id.clone(),
            WAIT,
            Instant::now(),
        )
        .await;
        assert_eq!(
            result,
            json!({"content":[{"type":"text","text":message}],"structuredContent":{"message":message}})
        );
        let receipt = broker.read_view().read_any(&receipt_id).unwrap();
        assert_eq!(receipt["phase"], "accepted");
        assert_eq!(receipt["requestAccepted"], true);
        assert_eq!(receipt["postconditionObserved"], false);
    }
    let actual = peer
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
    let expected = calls
        .iter()
        .map(|(_, request, args, _)| {
            if *request == "CreateRecordChapter" {
                json!({"requestType":request,"requestData":args})
            } else {
                json!({"requestType":request})
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(broker.read_view().summary()["total"], 8);
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
}
#[tokio::test]
async fn lost_toggle_ack_is_unknown_and_never_replayed_or_observed() {
    let call = Call::parse("obs-toggle-record-pause", &json!({})).unwrap();
    let scratch = Scratch::new();
    let peer = Peer::new(Mode::LoseAck).await;
    let (session, events) = ObsSession::connect(&peer.endpoint, None, SessionOptions::default())
        .await
        .unwrap();
    let broker =
        ReceiptBroker::new(OperationStore::new(&scratch.0, &peer.endpoint).unwrap()).unwrap();
    let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
    let result = generic_requests::execute(
        port,
        call.clone(),
        session.handle(),
        Arc::new(Mutex::new(ControlDomain::default())),
        now(),
        "timeline-contract".into(),
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
    assert!(broker.scope(ScopeKind::Generic(call.kind())).is_err());
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
    assert_eq!(peer.wire.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn queued_timeline_effect_rechecks_foreign_lease_before_any_send() {
    let call = Call::parse("obs-split-record-file", &json!({})).unwrap();
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
    let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
    let control = Arc::new(Mutex::new(ControlDomain::default()));
    let job = tokio::spawn(generic_requests::execute(
        port,
        call,
        session.handle(),
        control.clone(),
        now(),
        "timeline-contract".into(),
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
                .is_some_and(|value| value["phase"] == "dispatching")
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
    peer.release.send(()).unwrap();
    read.await.unwrap().unwrap();
    let result = job.await.unwrap();
    assert_eq!(result["isError"], true);
    assert!(result.to_string().contains("other-client"));
    let receipt = broker.read_view().read_any(&id(1)).unwrap();
    assert_eq!(receipt["phase"], "not-dispatched");
    assert_eq!(receipt["requestDispatched"], false);
    assert_eq!(receipt["postconditionObserved"], false);
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
    assert_eq!(peer.wire.lock().unwrap().len(), 1);
}
