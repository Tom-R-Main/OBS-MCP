// SPDX-License-Identifier: GPL-2.0-only
//! Staged UI consumers; restore only after immutable148 capture.
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
                "ui6-contract-{}-{}",
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
            "obs-set-studio-mode",
            "SetStudioModeEnabled",
            json!({"studioModeEnabled":true}),
            "Studio Mode has been enabled",
        ),
        (
            "obs-set-studio-mode",
            "SetStudioModeEnabled",
            json!({"studioModeEnabled":false}),
            "Studio Mode has been disabled",
        ),
        (
            "obs-open-input-properties",
            "OpenInputPropertiesDialog",
            json!({}),
            "Properties dialog opened for input: undefined",
        ),
        (
            "obs-open-input-properties",
            "OpenInputPropertiesDialog",
            json!({"inputUuid":""}),
            "Properties dialog opened for input: ",
        ),
        (
            "obs-open-input-filters",
            "OpenInputFiltersDialog",
            json!({"inputName":"","inputUuid":"Uuid"}),
            "Filters dialog opened for input: Uuid",
        ),
        (
            "obs-open-input-interact",
            "OpenInputInteractDialog",
            json!({"inputName":"Name","inputUuid":"Uuid"}),
            "Interact dialog opened for input: Name",
        ),
        (
            "obs-open-video-mix-projector",
            "OpenVideoMixProjector",
            json!({"videoMixType":"OBS_WEBSOCKET_VIDEO_MIX_TYPE_PREVIEW","monitorIndex":-1.5,"projectorGeometry":""}),
            "Video mix projector opened for: OBS_WEBSOCKET_VIDEO_MIX_TYPE_PREVIEW",
        ),
        (
            "obs-open-source-projector",
            "OpenSourceProjector",
            json!({}),
            "Source projector opened for: undefined",
        ),
        (
            "obs-open-source-projector",
            "OpenSourceProjector",
            json!({"sourceName":"","sourceUuid":"","monitorIndex":0.0,"projectorGeometry":"PRIVATE GEOMETRY"}),
            "Source projector opened for: ",
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
fn ui_identity_is_closed_and_optional_arguments_remain_literal() {
    for (name, request, args, _) in cases() {
        let call = Call::parse(name, &args).expect(name);
        assert_eq!(call.kind().request_type(), request);
        assert!(!call.requires_readback());
        assert!(!format!("{call:?}").contains("PRIVATE GEOMETRY"));
        let mut expected = args;
        obs_mcp_native::js_json::normalize_with_limits(&mut expected, Default::default()).unwrap();
        assert_eq!(call.into_arguments(), expected);
    }
    for (name, args) in [
        ("obs-set-studio-mode", json!({})),
        ("obs-set-studio-mode", json!({"studioModeEnabled":1})),
        ("obs-open-input-properties", json!({"inputName":null})),
        (
            "obs-open-video-mix-projector",
            json!({"videoMixType":"preview"}),
        ),
        ("obs-open-source-projector", json!({"monitorIndex":"-1"})),
        ("obs-open-input-interact", json!({"inputUuid":false})),
    ] {
        assert!(Call::parse(name, &args).is_none());
    }
    for name in [
        "obs-trigger-hotkey-by-name",
        "obs-call-request",
        "OpenSourceProjector",
    ] {
        assert!(Call::parse(name, &json!({})).is_none());
    }
}
#[tokio::test]
async fn ui_wire_objects_and_source_falsy_labels_ignore_response_fields_without_observation() {
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
            "ui6-contract".into(),
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
        assert_eq!(receipt["postconditionObserved"], false);
    }
    let actual = peer
        .wire
        .lock()
        .unwrap()
        .iter()
        .map(|frame| {
            let mut d = frame["d"].clone();
            d.as_object_mut().unwrap().remove("requestId");
            d
        })
        .collect::<Vec<_>>();
    let expected = calls
        .iter()
        .map(|(call, request, _, _)| json!({"requestType":request,"requestData":call.arguments()}))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    session.shutdown().await.unwrap();
    drop(events);
    peer.task.await.unwrap();
}
#[tokio::test]
async fn lost_idempotent_hint_ui_ack_is_unknown_and_never_automatically_replayed() {
    let call = Call::parse("obs-open-input-properties", &json!({"inputName":"Fixture"})).unwrap();
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
        "ui6-contract".into(),
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
async fn queued_ui_effect_rechecks_foreign_lease_before_any_send() {
    let call = Call::parse("obs-open-source-projector", &json!({})).unwrap();
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
        "ui6-contract".into(),
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
