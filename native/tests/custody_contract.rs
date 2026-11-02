// SPDX-License-Identifier: GPL-2.0-only
//! Real loopback WebSocket plus real receipt files; no live OBS or MCP route promotion.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    control::{ControlDomain, Disposition, RefusalKind},
    custody::{
        BeginError, Clock, ClockSnapshot, Confirmation, CustodyCoordinator, Details, FinishError,
        Outcome, PolicyCall, SharedControl, TrackedRequest, TransportDisposition,
    },
    operation_store::{OperationKind, OperationStore, Phase, StoreError},
    session::{BeforeDispatch, Invalidation, ObsSession, SessionHandle, SessionOptions},
};
use serde_json::{Value, json};
use std::{
    fs,
    future::Future,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

const WAIT: Duration = Duration::from_secs(3);
const EPOCH: u64 = 1_793_448_000_000;
const UTC: &str = "2026-10-31T12:00:00.000Z";
type Peer = WebSocketStream<TcpStream>;

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let number = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |number| {
                number.checked_add(1)
            })
            .unwrap();
        let path = std::env::temp_dir().canonicalize().unwrap().join(format!(
            "obs-custody-contract-{}-{number}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn root(&self) -> PathBuf {
        self.0.join("state")
    }
    fn path(&self, number: usize) -> PathBuf {
        self.root()
            .join("operation-receipts")
            .join(format!("{}.json", id(number)))
    }
    fn coordinator(
        &self,
        url: &str,
        handle: SessionHandle,
        control: SharedControl,
    ) -> CustodyCoordinator {
        CustodyCoordinator::new(
            OperationStore::new(self.root(), url).unwrap(),
            handle,
            control,
            clock(),
        )
        .unwrap()
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn id(number: usize) -> String {
    format!("00000000-0000-4000-8000-{number:012x}")
}
fn clock() -> Clock {
    Arc::new(|| ClockSnapshot {
        epoch_ms: EPOCH,
        utc: UTC.to_owned(),
    })
}
fn control() -> SharedControl {
    Arc::new(Mutex::new(ControlDomain::default()))
}
fn request(handle: &SessionHandle, number: usize) -> TrackedRequest {
    TrackedRequest {
        operation_id: id(number),
        kind: OperationKind::StageScene,
        expected: handle.token(),
        policy: PolicyCall {
            tool_name: "obs-stage-scene".into(),
            arguments: json!({"sceneName":"Fixture"}),
            read_only_hint: false,
            caller: "codex".into(),
            confirmation: Confirmation::default(),
        },
        details: Details {
            scene_uuid: Some("fixture-scene".into()),
            ..Default::default()
        },
        request_type: "SetCurrentPreviewScene".into(),
        request_data: json!({"sceneUuid":"fixture-scene"}),
        timeout: WAIT,
    }
}
async fn send(peer: &mut Peer, value: Value) {
    peer.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn receive(peer: &mut Peer) -> Value {
    loop {
        match timeout(WAIT, peer.next()).await.unwrap().unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) => peer.flush().await.unwrap(),
            other => panic!("expected request, got {other:?}"),
        }
    }
}
async fn reply(peer: &mut Peer, request: &Value, accepted: bool) {
    send(peer, json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":request["d"]["requestType"],"requestStatus":{"result":accepted,"code":if accepted {100} else {601}},"responseData":{"fixtureAccepted":accepted}}})).await;
}
async fn closed(peer: &mut Peer) {
    while let Some(message) = timeout(WAIT, peer.next()).await.unwrap() {
        match message.unwrap() {
            Message::Close(_) => {
                let _ = peer.flush().await;
                return;
            }
            Message::Ping(_) => peer.flush().await.unwrap(),
            other => panic!("unexpected frame (possible extra dispatch): {other:?}"),
        }
    }
}
async fn fixture<F, Fut>(script: F) -> (String, JoinHandle<()>)
where
    F: FnOnce(Peer) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (socket, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
        let mut peer = accept_async(socket).await.unwrap();
        send(
            &mut peer,
            json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}}),
        )
        .await;
        assert_eq!(receive(&mut peer).await["op"], 1);
        send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
        script(peer).await;
    });
    (url, task)
}
async fn joined(task: JoinHandle<()>) {
    timeout(WAIT, task).await.unwrap().unwrap();
}

#[tokio::test]
async fn initial_policy_refusal_creates_no_files_and_sends_nothing() {
    let scratch = Scratch::new();
    let (url, peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let control = control();
    control
        .lock()
        .unwrap()
        .claim("other", 1, "fixture", EPOCH)
        .unwrap();
    let mut custody = scratch.coordinator(&url, handle.clone(), control);
    assert!(matches!(
        custody.begin(request(&handle, 1)),
        Err(BeginError::Policy(Disposition::Refused {
            kind: RefusalKind::Lease,
            ..
        }))
    ));
    assert!(!scratch.root().exists());
    assert!(!custody.has_pending());
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn endpoint_mismatch_and_stale_token_are_rejected_before_creating_receipts() {
    let scratch = Scratch::new();
    let (url_a, peer_a) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (url_b, peer_b) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (owner_a, _events_a) = ObsSession::connect(&url_a, None, SessionOptions::default())
        .await
        .unwrap();
    let (owner_b, _events_b) = ObsSession::connect(&url_b, None, SessionOptions::default())
        .await
        .unwrap();
    let handle_b = owner_b.handle();
    let wrong_endpoint = CustodyCoordinator::new(
        OperationStore::new(scratch.root(), &url_a).unwrap(),
        handle_b.clone(),
        control(),
        clock(),
    );
    assert!(matches!(wrong_endpoint, Err(StoreError::EndpointMismatch)));
    assert!(!scratch.root().exists());
    let mut custody = scratch.coordinator(&url_b, handle_b.clone(), control());
    let mut stale = request(&handle_b, 1);
    stale.expected = owner_a.token();
    assert!(matches!(
        custody.begin(stale),
        Err(BeginError::StaleConnection)
    ));
    assert!(!scratch.root().exists());
    owner_a.shutdown().await.unwrap();
    owner_b.shutdown().await.unwrap();
    joined(peer_a).await;
    joined(peer_b).await;
}

#[tokio::test]
async fn success_means_accepted_without_observed_or_rendered_evidence() {
    let scratch = Scratch::new();
    let path = scratch.path(1);
    let (url, peer) = fixture(move |mut peer| async move {
        let request = receive(&mut peer).await;
        let intent: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(intent["phase"], "dispatching");
        assert!(intent["requestDispatched"].is_null());
        assert!(!intent["postconditionObserved"].as_bool().unwrap());
        reply(&mut peer, &request, true).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut custody = scratch.coordinator(&url, handle.clone(), control());
    custody.begin(request(&handle, 1)).unwrap();
    assert_eq!(
        custody.pending_receipt().unwrap().phase(),
        Phase::Dispatching
    );
    let Outcome::Accepted { receipt, response } = custody.finish().await.unwrap() else {
        panic!("expected accepted")
    };
    assert_eq!(receipt.phase(), Phase::Accepted);
    assert_eq!(receipt.generation(), owner.token().generation());
    assert_eq!(response, json!({"fixtureAccepted":true}));
    assert_eq!(receipt.to_value()["requestAccepted"], true);
    assert_eq!(receipt.to_value()["postconditionObserved"], false);
    assert_eq!(receipt.to_value()["renderedComposition"], "unverified");
    assert_eq!(custody.summary()["operations"][0]["status"], "unknown");
    assert!(matches!(
        custody.finish().await,
        Err(FinishError::NoPending)
    ));
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn queued_policy_changes_are_checked_at_the_socket_and_mark_not_dispatched() {
    let scratch = Scratch::new();
    let (seen, seen_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (url, peer) = fixture(move |mut peer| async move {
        let blocking = receive(&mut peer).await;
        assert_eq!(blocking["d"]["requestType"], "GetVersion");
        seen.send(()).unwrap();
        release_rx.await.unwrap();
        reply(&mut peer, &blocking, true).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let blocking_handle = handle.clone();
    let blocking = tokio::spawn(async move {
        blocking_handle
            .request(blocking_handle.token(), "GetVersion", Value::Null, WAIT)
            .await
            .unwrap()
    });
    seen_rx.await.unwrap();
    let control = control();
    control
        .lock()
        .unwrap()
        .claim("codex", 1, "", EPOCH)
        .unwrap();
    let mut custody = scratch.coordinator(&url, handle.clone(), control.clone());
    custody.begin(request(&handle, 1)).unwrap();
    let mut finish = Box::pin(custody.finish());
    assert!(poll!(&mut finish).is_pending());
    control.lock().unwrap().release("codex", EPOCH);
    control
        .lock()
        .unwrap()
        .claim("other", 1, "", EPOCH)
        .unwrap();
    release.send(()).unwrap();
    let Outcome::NotDispatched {
        receipt,
        reason,
        policy,
    } = finish.await.unwrap()
    else {
        panic!("expected final guard refusal")
    };
    assert!(matches!(reason, BeforeDispatch::GuardRefused(_)));
    assert!(matches!(
        policy,
        Some(Disposition::Refused {
            kind: RefusalKind::Lease,
            ..
        })
    ));
    assert_eq!(receipt.phase(), Phase::NotDispatched);
    assert_eq!(receipt.to_value()["requestDispatched"], false);
    assert!(receipt.to_value()["requestAccepted"].is_null());
    blocking.await.unwrap();
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn lost_reply_records_unknown_and_dispatches_exactly_once() {
    let scratch = Scratch::new();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let (url, peer) = fixture(move |mut peer| async move {
        assert_eq!(
            receive(&mut peer).await["d"]["requestType"],
            "SetCurrentPreviewScene"
        );
        observed.fetch_add(1, Ordering::Relaxed);
        // Abrupt disconnect after delivery, without a reply.
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut custody = scratch.coordinator(&url, handle.clone(), control());
    custody.begin(request(&handle, 1)).unwrap();
    let Outcome::Unknown { receipt, .. } = custody.finish().await.unwrap() else {
        panic!("expected unknown")
    };
    assert_eq!(receipt.phase(), Phase::Unknown);
    assert!(receipt.to_value()["requestDispatched"].is_null());
    assert!(receipt.to_value()["requestAccepted"].is_null());
    assert_eq!(count.load(Ordering::Relaxed), 1);
    assert!(matches!(
        custody.finish().await,
        Err(FinishError::NoPending)
    ));
    assert!(matches!(
        custody.begin(request(&handle, 1)),
        Err(BeginError::Receipt { .. })
    ));
    assert_eq!(count.load(Ordering::Relaxed), 1);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn explicit_rejection_retains_dispatched_true_accepted_false_unknown_phase() {
    let scratch = Scratch::new();
    let (url, peer) = fixture(|mut peer| async move {
        let request = receive(&mut peer).await;
        reply(&mut peer, &request, false).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut custody = scratch.coordinator(&url, handle.clone(), control());
    custody.begin(request(&handle, 1)).unwrap();
    let Outcome::Rejected { receipt, code } = custody.finish().await.unwrap() else {
        panic!("expected rejection")
    };
    assert_eq!(code, 601);
    assert_eq!(receipt.phase(), Phase::Unknown);
    assert_eq!(receipt.to_value()["requestDispatched"], true);
    assert_eq!(receipt.to_value()["requestAccepted"], false);
    assert_eq!(receipt.to_value()["postconditionObserved"], false);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn receipt_failure_before_dispatch_sends_nothing() {
    let scratch = Scratch::new();
    let (url, peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut custody = scratch.coordinator(&url, handle.clone(), control());
    fs::write(scratch.root(), b"fixture blocks directory creation").unwrap();
    assert!(matches!(
        custody.begin(request(&handle, 1)),
        Err(BeginError::Receipt { .. })
    ));
    assert!(!custody.has_pending());
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn post_send_receipt_failure_is_custody_uncertainty_even_after_an_accepted_reply() {
    let scratch = Scratch::new();
    let path = scratch.path(1);
    let (url, peer) = fixture(move |mut peer| async move {
        let request = receive(&mut peer).await;
        fs::write(path, b"fixture corrupts receipt after possible effect").unwrap();
        reply(&mut peer, &request, true).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut custody = scratch.coordinator(&url, handle.clone(), control());
    custody.begin(request(&handle, 1)).unwrap();
    let Err(FinishError::CustodyUncertain {
        last_confirmed_receipt,
        transport: TransportDisposition::Accepted,
        ..
    }) = custody.finish().await
    else {
        panic!("must not report success")
    };
    assert_eq!(last_confirmed_receipt.phase(), Phase::Dispatching);
    assert_eq!(custody.summary()["status"], "unknown");
    assert!(matches!(
        custody.finish().await,
        Err(FinishError::NoPending)
    ));
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn delayed_first_finish_uses_the_begin_deadline_and_second_begin_writes_nothing() {
    let scratch = Scratch::new();
    let (url, peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut custody = scratch.coordinator(&url, handle.clone(), control());
    let mut first = request(&handle, 1);
    first.timeout = Duration::from_millis(20);
    custody.begin(first).unwrap();
    assert!(matches!(
        custody.begin(request(&handle, 2)),
        Err(BeginError::Busy)
    ));
    assert!(!scratch.path(2).exists());
    tokio::time::sleep(Duration::from_millis(30)).await;
    let Outcome::NotDispatched {
        receipt,
        reason: BeforeDispatch::Deadline,
        ..
    } = custody.finish().await.unwrap()
    else {
        panic!("original deadline must expire")
    };
    assert_eq!(receipt.phase(), Phase::NotDispatched);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn cancelled_frontend_await_resumes_the_same_observer_without_redispatch() {
    let scratch = Scratch::new();
    let (seen, seen_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let (url, peer) = fixture(move |mut peer| async move {
        let request = receive(&mut peer).await;
        observed.fetch_add(1, Ordering::Relaxed);
        seen.send(()).unwrap();
        release_rx.await.unwrap();
        reply(&mut peer, &request, true).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut custody = scratch.coordinator(&url, handle.clone(), control());
    custody.begin(request(&handle, 1)).unwrap();
    let mut first_await = Box::pin(custody.finish());
    assert!(poll!(&mut first_await).is_pending());
    seen_rx.await.unwrap();
    drop(first_await);
    assert!(custody.has_pending());
    assert!(matches!(
        custody.begin(request(&handle, 2)),
        Err(BeginError::Busy)
    ));
    release.send(()).unwrap();
    assert!(matches!(
        custody.finish().await.unwrap(),
        Outcome::Accepted { .. }
    ));
    assert_eq!(count.load(Ordering::Relaxed), 1);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn dropping_coordinator_invalidates_inflight_socket_and_retains_uncertain_intent() {
    let scratch = Scratch::new();
    let (seen, seen_rx) = oneshot::channel();
    let (url, peer) = fixture(move |mut peer| async move {
        receive(&mut peer).await;
        seen.send(()).unwrap();
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut custody = scratch.coordinator(&url, handle.clone(), control());
    custody.begin(request(&handle, 1)).unwrap();
    let mut frontend = Box::pin(custody.finish());
    assert!(poll!(&mut frontend).is_pending());
    seen_rx.await.unwrap();
    drop(frontend);
    drop(custody);
    timeout(WAIT, async {
        while handle.invalidation().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        handle.invalidation(),
        Some(Invalidation::CancelledAfterDispatch)
    );
    let reopened = OperationStore::new(scratch.root(), &url).unwrap();
    assert_eq!(reopened.read(&id(1)).unwrap().phase(), Phase::Dispatching);
    assert_eq!(reopened.summary()["operations"][0]["status"], "unknown");
    owner.shutdown().await.unwrap();
    joined(peer).await;
}
