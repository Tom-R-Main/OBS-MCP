// SPDX-License-Identifier: GPL-2.0-only
//! Closed library boundary over actual Session/Broker; no public Restore route.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    clock,
    control::{ControlDomain, TakeLock},
    operation_store::{
        GenericKind, OperationKind, OperationStore, PrepareDetails, RestoreCounts, RestorePhase,
        RestoreReceipt,
    },
    receipt_broker::{ReceiptBroker, ScopeKind},
    restore_transport::{BeginError, Policy, Request, RestoreBatchOwner},
    session::{BeforeDispatch, ObsSession, RequestError, SessionOptions},
    snapshots::Mutation,
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    fs,
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
    time::{Instant, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(8);
type Peer = WebSocketStream<TcpStream>;
fn id(n: u64) -> String {
    format!("bbbbbbbb-bbbb-4bbb-8bbb-{n:012x}")
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let base = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"));
        let p = base.join(format!(
            "restore-batch-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn receipt(&self, n: u64) -> PathBuf {
        self.0
            .join("operation-receipts")
            .join(format!("{}.json", id(n)))
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
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
            Message::Text(s) => return serde_json::from_str(&s).unwrap(),
            Message::Ping(_) => peer.flush().await.unwrap(),
            v => panic!("unexpected frame {v:?}"),
        }
    }
}
async fn reply(peer: &mut Peer, request: &Value, data: Value) {
    send(peer,json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":request["d"]["requestType"],"requestStatus":{"result":true,"code":100},"responseData":data}})).await;
}
async fn closed(peer: &mut Peer) {
    match timeout(WAIT, peer.next()).await.unwrap() {
        None | Some(Err(_)) | Some(Ok(Message::Close(_))) => {
            let _ = peer.flush().await;
        }
        v => panic!("unexpected extra request {v:?}"),
    }
}
async fn fixture<F, Fut>(script: F) -> (String, JoinHandle<()>)
where
    F: FnOnce(Peer) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut peer = accept_async(socket).await.unwrap();
        send(
            &mut peer,
            json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}}),
        )
        .await;
        assert_eq!(receive(&mut peer).await["op"], 1);
        send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
        script(peer).await;
        let mut unexpected_connection = Box::pin(listener.accept());
        assert!(
            poll!(unexpected_connection.as_mut()).is_pending(),
            "unexpected second connection"
        );
    });
    (url, task)
}

const UTC: &str = "2026-11-01T12:00:00.000Z";
fn shared_control() -> Arc<Mutex<ControlDomain>> {
    Arc::new(Mutex::new(ControlDomain::default()))
}
fn clock() -> obs_mcp_native::custody::Clock {
    Arc::new(|| clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap())
}
fn mutations() -> Vec<Mutation> {
    vec![
        Mutation::InputSettings {
            input_name: "PRIVATE_INPUT".into(),
            settings: serde_json::from_value(json!({"PRIVATE_SETTING":"PRIVATE_VALUE"})).unwrap(),
        },
        Mutation::InputMute {
            input_name: "PRIVATE_INPUT".into(),
            muted: true,
        },
        Mutation::InputVolume {
            input_name: "PRIVATE_INPUT".into(),
            volume: 0.5,
        },
        Mutation::InputAudioTracks {
            input_name: "PRIVATE_INPUT".into(),
            tracks: serde_json::from_value(json!({"1":false})).unwrap(),
        },
        Mutation::ItemTransform {
            scene_name: "PRIVATE_SCENE".into(),
            item_id: 3.0,
            transform: serde_json::from_value(json!({"positionX":5})).unwrap(),
        },
        Mutation::ItemEnabled {
            scene_name: "PRIVATE_SCENE".into(),
            item_id: 3.0,
            enabled: true,
        },
        Mutation::ItemLocked {
            scene_name: "PRIVATE_SCENE".into(),
            item_id: 3.0,
            locked: true,
        },
        Mutation::ItemIndex {
            scene_name: "PRIVATE_SCENE".into(),
            item_id: 3.0,
            index: 0,
        },
    ]
}
fn request(n: u64, cancel: Cancellation) -> Request {
    Request {
        operation_id: id(n),
        mutations: mutations(),
        policy: Policy {
            caller: "restore-client".into(),
            snapshot_id: Some("PRIVATE_SNAPSHOT".into()),
        },
        capabilities: None,
        cancellation: cancel,
        admitted_at: Instant::now(),
        timeout: WAIT,
    }
}
fn results(frame: &Value) -> Vec<Value> {
    frame["d"]["requests"].as_array().unwrap().iter().enumerate().map(|(i,r)|json!({"requestId":r["requestId"],"requestType":r["requestType"],"requestStatus":{"result":i!=1,"code":if i==1 {601}else{100},"comment":"PRIVATE_COMMENT"}})).collect()
}
async fn batch_reply(peer: &mut Peer, frame: &Value, rows: Vec<Value>) {
    send(
        peer,
        json!({"op":9,"d":{"requestId":frame["d"]["requestId"],"results":rows}}),
    )
    .await;
}
async fn finish_session(owner: ObsSession, peer: JoinHandle<()>) {
    owner.shutdown().await.unwrap();
    timeout(WAIT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn exact_one_closed_frame_mixed_statuses_and_identity_only_receipt() {
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        assert_eq!(frame["op"], 8);
        assert_eq!(frame["d"]["executionType"], 0);
        assert_eq!(frame["d"]["haltOnFailure"], false);
        let expected = [
            "SetInputSettings",
            "SetInputMute",
            "SetInputVolume",
            "SetInputAudioTracks",
            "SetSceneItemTransform",
            "SetSceneItemEnabled",
            "SetSceneItemLocked",
            "SetSceneItemIndex",
        ];
        for (i, r) in frame["d"]["requests"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
        {
            assert_eq!(r["requestType"], expected[i]);
            assert_eq!(r["requestId"], i.to_string());
        }
        assert_eq!(frame["d"]["requests"][0]["requestData"]["overlay"], false);
        let mut rows = results(&frame);
        rows.reverse();
        batch_reply(&mut peer, &frame, rows).await;
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = RestoreBatchOwner::new(
        broker.scope(ScopeKind::RestoreBatch).unwrap(),
        session.handle(),
        shared_control(),
        clock(),
    )
    .unwrap();
    owner.begin(request(1, Cancellation::default())).unwrap();
    let out = owner.finish().await.unwrap();
    assert!(out.receipt_error.is_none());
    let rows = out.transport.unwrap();
    assert_eq!(rows.len(), 8);
    assert!(!rows[1].ok);
    assert_eq!(rows[1].code, 601);
    let receipt = out.receipt.to_value();
    assert_eq!(receipt["version"], 3);
    assert_eq!(receipt["phase"], "acknowledged");
    assert_eq!(receipt["successfulRequests"], 7);
    assert_eq!(receipt["failedRequests"], 1);
    assert_eq!(receipt["postconditionObserved"], false);
    assert_eq!(receipt["renderedComposition"], "unverified");
    assert!(!receipt.to_string().contains("PRIVATE"));
    assert!(!broker.admission().unwrap().blocking);
    assert!(broker.admission().unwrap().generic_v2_present);
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}

#[test]
fn dedicated_receipt_scope_and_strict_schema_are_restart_safe() {
    let dir = Dir::new();
    let endpoint = "ws://127.0.0.1:4455";
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, endpoint).unwrap()).unwrap();
    let mut restore = broker.scope(ScopeKind::RestoreBatch).unwrap();
    assert!(
        restore
            .prepare_generic(&id(11), GenericKind::InputMute, 1, UTC)
            .is_err()
    );
    assert!(
        restore
            .prepare(
                &id(12),
                OperationKind::RecordStop,
                1,
                UTC,
                PrepareDetails::default()
            )
            .is_err()
    );
    let prepared = restore.prepare_restore(&id(1), 1, 8, UTC).unwrap();
    assert!(
        obs_mcp_native::operation_store::OperationReceipt::from_value(prepared.to_value()).is_err()
    );
    let dispatch = restore
        .update_restore(&prepared, RestorePhase::Dispatching, None, UTC)
        .unwrap();
    drop(restore);
    drop(broker);
    let reopened = OperationStore::new(&dir.0, endpoint).unwrap();
    assert!(reopened.read_view().admission().unwrap().blocking);
    let mut value = dispatch.to_value();
    value["postconditionObserved"] = json!(true);
    assert!(RestoreReceipt::from_value(value).is_err());
    assert_eq!(
        reopened.read_view().read_any(&id(1)).unwrap()["phase"],
        "dispatching"
    );
}

#[tokio::test]
async fn cancelled_wait_keeps_exact_admitted_batch_until_ack_and_join() {
    let (sent, seen) = oneshot::channel();
    let (release, held) = oneshot::channel();
    let (url, peer) = fixture(move |mut peer| async move {
        let frame = receive(&mut peer).await;
        sent.send(()).unwrap();
        held.await.unwrap();
        let rows = results(&frame);
        batch_reply(&mut peer, &frame, rows).await;
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = RestoreBatchOwner::new(
        broker.scope(ScopeKind::RestoreBatch).unwrap(),
        session.handle(),
        shared_control(),
        clock(),
    )
    .unwrap();
    let cancel = Cancellation::default();
    owner.begin(request(1, cancel.clone())).unwrap();
    {
        let mut frontend = Box::pin(owner.finish());
        assert!(poll!(frontend.as_mut()).is_pending());
        seen.await.unwrap();
    }
    cancel.cancel();
    assert!(owner.has_pending());
    owner = *owner
        .into_store()
        .err()
        .expect("pending effect retains the entire owner");
    let mut shutdown = Box::pin(async move {
        let out = owner.finish().await.unwrap();
        drop(owner.into_store().ok().unwrap());
        session.shutdown().await.unwrap();
        out
    });
    assert!(poll!(shutdown.as_mut()).is_pending());
    release.send(()).unwrap();
    let out = shutdown.await;
    assert!(out.transport.is_ok());
    assert_eq!(out.receipt.phase(), RestorePhase::Acknowledged);
    assert!(!broker.admission().unwrap().blocking);
    timeout(WAIT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn lease_change_or_cancel_while_queued_prevents_the_only_batch_send() {
    for case in ["lease", "cancel"] {
        let (sent, seen) = oneshot::channel();
        let (release, held) = oneshot::channel();
        let (url, peer) = fixture(move |mut peer| async move {
            let first = receive(&mut peer).await;
            assert_eq!(first["op"], 6);
            sent.send(()).unwrap();
            held.await.unwrap();
            reply(&mut peer, &first, json!({})).await;
            closed(&mut peer).await;
        })
        .await;
        let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let handle = session.handle();
        let mut held_rpc = Box::pin(handle.request(handle.token(), "GetStats", Value::Null, WAIT));
        assert!(poll!(held_rpc.as_mut()).is_pending());
        seen.await.unwrap();
        let dir = Dir::new();
        let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
        let control = shared_control();
        let mut owner = RestoreBatchOwner::new(
            broker.scope(ScopeKind::RestoreBatch).unwrap(),
            session.handle(),
            control.clone(),
            clock(),
        )
        .unwrap();
        let cancel = Cancellation::default();
        owner.begin(request(1, cancel.clone())).unwrap();
        {
            let mut waiting = Box::pin(owner.finish());
            assert!(poll!(waiting.as_mut()).is_pending());
        }
        if case == "lease" {
            control
                .lock()
                .unwrap()
                .claim("another-client", 1, "", 1_793_448_000_000)
                .unwrap();
        } else {
            cancel.cancel();
        }
        release.send(()).unwrap();
        held_rpc.await.unwrap();
        let out = owner.finish().await.unwrap();
        assert!(matches!(
            out.transport,
            Err(RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(
                _
            )))
        ));
        assert_eq!(out.receipt.phase(), RestorePhase::NotDispatched);
        assert_eq!(out.receipt.to_value()["requestDispatched"], false);
        assert_eq!(out.policy_refusal.is_some(), case == "lease");
        drop(owner.into_store().ok().unwrap());
        finish_session(session, peer).await;
    }
}

#[tokio::test]
async fn invoked_dispatch_guard_can_still_report_not_dispatched() {
    let (url, peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let cancel = Cancellation::default();
    let trigger = cancel.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let checked_clock: obs_mcp_native::custody::Clock = Arc::new(move || {
        let n = seen.fetch_add(1, Ordering::SeqCst);
        if n == 3 {
            trigger.cancel();
        }
        clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap()
    });
    let mut owner = RestoreBatchOwner::new(
        broker.scope(ScopeKind::RestoreBatch).unwrap(),
        session.handle(),
        shared_control(),
        checked_clock,
    )
    .unwrap();
    owner.begin(request(1, cancel)).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let out = owner.finish().await.unwrap();
    assert!(calls.load(Ordering::SeqCst) >= 5);
    assert_eq!(out.receipt.phase(), RestorePhase::NotDispatched);
    assert!(matches!(
        out.transport,
        Err(RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(
            _
        )))
    ));
    assert_eq!(out.receipt.to_value()["batchAcknowledged"], Value::Null);
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}

#[tokio::test]
async fn lost_or_malformed_reply_is_unknown_and_restart_blocks_without_retry() {
    for case in ["lost", "missing", "foreign", "bad-status"] {
        let (url, peer) = fixture(move |mut peer| async move {
            let frame = receive(&mut peer).await;
            assert_eq!(frame["op"], 8);
            if case == "lost" {
                peer.close(None).await.unwrap();
                return;
            }
            let mut rows = results(&frame);
            match case {
                "missing" => {
                    rows.pop();
                }
                "foreign" => rows[0]["requestId"] = json!("other"),
                _ => rows[0]["requestStatus"]["result"] = json!("true"),
            }
            batch_reply(&mut peer, &frame, rows).await;
            closed(&mut peer).await;
        })
        .await;
        let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let dir = Dir::new();
        let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
        let mut owner = RestoreBatchOwner::new(
            broker.scope(ScopeKind::RestoreBatch).unwrap(),
            session.handle(),
            shared_control(),
            clock(),
        )
        .unwrap();
        owner.begin(request(1, Cancellation::default())).unwrap();
        let out = owner.finish().await.unwrap();
        assert!(matches!(out.transport, Err(RequestError::Unknown(_))));
        assert_eq!(out.receipt.phase(), RestorePhase::Unknown);
        assert!(broker.admission().unwrap().blocking);
        assert!(owner.begin(request(2, Cancellation::default())).is_err());
        drop(owner.into_store().ok().unwrap());
        drop(broker);
        let restarted = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
        assert!(restarted.admission().unwrap().blocking);
        assert!(restarted.scope(ScopeKind::RestoreBatch).is_err());
        assert!(!dir.receipt(2).exists());
        finish_session(session, peer).await;
    }
}

#[tokio::test]
async fn unacknowledged_deadline_retires_session_without_replay() {
    let (url, peer) = fixture(|mut peer| async move {
        assert_eq!(receive(&mut peer).await["op"], 8);
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = RestoreBatchOwner::new(
        broker.scope(ScopeKind::RestoreBatch).unwrap(),
        session.handle(),
        shared_control(),
        clock(),
    )
    .unwrap();
    let mut r = request(1, Cancellation::default());
    r.timeout = Duration::from_secs(2);
    owner.begin(r).unwrap();
    let out = owner.finish().await.unwrap();
    assert!(matches!(out.transport, Err(RequestError::Unknown(_))));
    assert_eq!(out.receipt.phase(), RestorePhase::Unknown);
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}

#[tokio::test]
async fn pre_send_refusals_have_no_receipt_or_frame() {
    let (url, peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let control = shared_control();
    let mut owner = RestoreBatchOwner::new(
        broker.scope(ScopeKind::RestoreBatch).unwrap(),
        session.handle(),
        control.clone(),
        clock(),
    )
    .unwrap();
    let cancel = Cancellation::default();
    cancel.cancel();
    assert!(matches!(
        owner.begin(request(1, cancel)),
        Err(BeginError::Cancelled)
    ));
    let mut r = request(2, Cancellation::default());
    r.mutations.clear();
    assert!(matches!(owner.begin(r), Err(BeginError::Empty)));
    let mut r = request(3, Cancellation::default());
    r.mutations = vec![Mutation::InputVolume {
        input_name: "input".into(),
        volume: f64::NAN,
    }];
    assert!(matches!(owner.begin(r), Err(BeginError::InvalidMutation)));
    let mut r = request(4, Cancellation::default());
    r.timeout = Duration::ZERO;
    assert!(matches!(owner.begin(r), Err(BeginError::Deadline)));
    control
        .lock()
        .unwrap()
        .claim("another-client", 1, "", 1_793_448_000_000)
        .unwrap();
    assert!(matches!(
        owner.begin(request(5, Cancellation::default())),
        Err(BeginError::Policy(_))
    ));
    assert!(!dir.0.join("operation-receipts").exists());
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}

#[tokio::test]
async fn source_take_lock_allows_same_holder_input_item_restore_without_recording_authority() {
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        for r in frame["d"]["requests"].as_array().unwrap() {
            assert!(
                ![
                    "StartRecord",
                    "StopRecord",
                    "ToggleRecord",
                    "SetOutputSettings",
                    "SetVideoSettings",
                    "SetCurrentProfile"
                ]
                .contains(&r["requestType"].as_str().unwrap())
            );
        }
        let rows = results(&frame);
        batch_reply(&mut peer, &frame, rows).await;
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let control = shared_control();
    {
        let mut c = control.lock().unwrap();
        c.claim("restore-client", 1, "", 1_793_448_000_000).unwrap();
        c.set_take_lock(Some(TakeLock {
            id: "owned-take".into(),
            locked: true,
        }));
    }
    let impact = obs_mcp_native::record_impact::RecordImpactOwner::bind(&session.handle()).unwrap();
    let ticket = impact.view().reserve_take().unwrap();
    let mut owner = RestoreBatchOwner::new(
        broker.scope(ScopeKind::RestoreBatch).unwrap(),
        session.handle(),
        control,
        clock(),
    )
    .unwrap();
    owner.begin(request(1, Cancellation::default())).unwrap();
    assert!(owner.finish().await.unwrap().transport.is_ok());
    assert_eq!(
        ticket.check(),
        obs_mcp_native::record_impact::ImpactStatus::Ready
    );
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}

#[tokio::test]
async fn actual_persistence_failures_before_or_after_dispatch_never_authorize_retry() {
    for case in ["prepared", "dispatching", "settlement"] {
        let dir = Dir::new();
        let receipt_path = dir.receipt(1);
        let target = receipt_path.clone();
        let (url, peer) = fixture(move |mut peer| async move {
            if case != "settlement" {
                closed(&mut peer).await;
                return;
            }
            let frame = receive(&mut peer).await;
            fs::write(target, b"{}").unwrap();
            let rows = results(&frame);
            batch_reply(&mut peer, &frame, rows).await;
            closed(&mut peer).await;
        })
        .await;
        let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
        let clock_calls = Arc::new(AtomicUsize::new(0));
        let calls = clock_calls.clone();
        let clock_path = receipt_path.clone();
        let selected_clock: obs_mcp_native::custody::Clock = Arc::new(move || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if case == "prepared" && n == 1 {
                fs::write(clock_path.parent().unwrap(), b"blocking file").unwrap();
            }
            if case == "dispatching" && n == 2 {
                fs::write(&clock_path, b"{}").unwrap();
            }
            clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap()
        });
        let mut owner = RestoreBatchOwner::new(
            broker.scope(ScopeKind::RestoreBatch).unwrap(),
            session.handle(),
            shared_control(),
            selected_clock,
        )
        .unwrap();
        if case == "settlement" {
            owner.begin(request(1, Cancellation::default())).unwrap();
            let out = owner.finish().await.unwrap();
            assert!(out.transport.is_ok());
            assert!(out.receipt_error.is_some());
            assert_eq!(out.receipt.phase(), RestorePhase::Dispatching);
        } else {
            assert!(matches!(
                owner.begin(request(1, Cancellation::default())),
                Err(BeginError::Receipt(_))
            ));
        }
        assert!(broker.admission().is_err());
        assert!(owner.begin(request(2, Cancellation::default())).is_err());
        assert!(!dir.receipt(2).exists());
        drop(owner.into_store().ok().unwrap());
        finish_session(session, peer).await;
    }
}

#[test]
fn every_existing_scope_and_another_restore_scope_lack_this_receipt_capability() {
    use obs_mcp_native::session::SafetyStopRequest;
    let dir = Dir::new();
    let broker =
        ReceiptBroker::new(OperationStore::new(&dir.0, "ws://127.0.0.1:4455").unwrap()).unwrap();
    for scope in [
        ScopeKind::Take,
        ScopeKind::Stage,
        ScopeKind::CriticalRecord,
        ScopeKind::Generic(GenericKind::InputMute),
        ScopeKind::ManualRecordStop,
        ScopeKind::SafetyStop(SafetyStopRequest::Stream),
    ] {
        let mut port = broker.scope(scope).unwrap();
        assert!(port.prepare_restore(&id(1), 1, 8, UTC).is_err());
    }
    let mut first = broker.scope(ScopeKind::RestoreBatch).unwrap();
    let mut second = broker.scope(ScopeKind::RestoreBatch).unwrap();
    let r = first.prepare_restore(&id(1), 1, 8, UTC).unwrap();
    assert!(
        second
            .update_restore(&r, RestorePhase::Dispatching, None, UTC)
            .is_err()
    );
    for kind in [
        GenericKind::ManualRecordStop,
        GenericKind::InputMute,
        GenericKind::StreamStart,
    ] {
        assert!(first.prepare_generic(&id(2), kind, 1, UTC).is_err());
    }
    for kind in [
        OperationKind::StageScene,
        OperationKind::TakeScene,
        OperationKind::RecordStart,
        OperationKind::RecordStop,
    ] {
        assert!(
            first
                .prepare(&id(3), kind, 1, UTC, PrepareDetails::default())
                .is_err()
        );
    }
}

#[test]
fn version_three_dispositions_counts_schema_and_transitions_are_closed() {
    let dir = Dir::new();
    let mut store = OperationStore::new(&dir.0, "ws://127.0.0.1:4455").unwrap();
    assert!(store.prepare_restore(&id(1), 1, 0, UTC).is_err());
    let p = store.prepare_restore(&id(1), 1, 8, UTC).unwrap();
    assert!(
        store
            .update_restore(
                &p,
                RestorePhase::Acknowledged,
                Some(RestoreCounts {
                    successful: 8,
                    failed: 0
                }),
                UTC
            )
            .is_err()
    );
    let d = store
        .update_restore(&p, RestorePhase::Dispatching, None, UTC)
        .unwrap();
    assert!(
        store
            .update_restore(&p, RestorePhase::NotDispatched, None, UTC)
            .is_err()
    );
    assert!(
        store
            .update_restore(
                &d,
                RestorePhase::Acknowledged,
                Some(RestoreCounts {
                    successful: 7,
                    failed: 0
                }),
                UTC
            )
            .is_err()
    );
    let a = store
        .update_restore(
            &d,
            RestorePhase::Acknowledged,
            Some(RestoreCounts {
                successful: 7,
                failed: 1,
            }),
            UTC,
        )
        .unwrap();
    assert!(
        store
            .update_restore(&a, RestorePhase::Dispatching, None, UTC)
            .is_err()
    );
    for (field, value) in [
        ("version", json!(2)),
        ("kind", json!("request")),
        ("executionType", json!(1)),
        ("haltOnFailure", json!(true)),
        ("requestCount", json!(0)),
        ("successfulRequests", json!(8)),
        ("batchAcknowledged", json!(false)),
        ("requestDispatched", json!(false)),
        ("postconditionObserved", json!(true)),
        ("renderedComposition", json!("verified")),
        ("requestType", json!("StopRecord")),
    ] {
        let mut v = a.to_value();
        v[field] = value;
        assert!(RestoreReceipt::from_value(v).is_err(), "field {field}");
    }
    let mut v = a.to_value();
    v.as_object_mut().unwrap().remove("failedRequests");
    assert!(RestoreReceipt::from_value(v).is_err());
    let summary = store.summary();
    assert_eq!(summary["operations"][0]["status"], "acknowledged");
    assert!(summary["operations"][0].get("endpointHash").is_none());
}

#[tokio::test]
async fn cached_capability_identity_and_wrong_scope_refuse_before_intent() {
    use obs_mcp_native::restore_transport::Capabilities;
    let (url, peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (other_url, other_peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let (other, _other_events) = ObsSession::connect(&other_url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let wrong = RestoreBatchOwner::new(
        broker.scope(ScopeKind::Stage).unwrap(),
        session.handle(),
        shared_control(),
        clock(),
    );
    assert!(matches!(wrong, Err(BeginError::Binding)));
    let mut owner = RestoreBatchOwner::new(
        broker.scope(ScopeKind::RestoreBatch).unwrap(),
        session.handle(),
        shared_control(),
        clock(),
    )
    .unwrap();
    let mut r = request(1, Cancellation::default());
    r.capabilities = Capabilities::from_version(
        &session.handle(),
        &json!({"availableRequests":["GetStats"]}),
    );
    assert!(matches!(
        owner.begin(r),
        Err(BeginError::Unsupported("SetInputSettings"))
    ));
    let mut r = request(2, Cancellation::default());
    r.capabilities = Capabilities::from_version(&other.handle(), &json!({"availableRequests":[]}));
    assert!(matches!(owner.begin(r), Err(BeginError::Binding)));
    assert!(!dir.receipt(1).exists());
    assert!(!dir.receipt(2).exists());
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
    finish_session(other, other_peer).await;
}

#[tokio::test]
async fn dropping_pending_owner_is_emergency_uncertainty_not_join_or_retry() {
    let (sent, seen) = oneshot::channel();
    let (url, peer) = fixture(move |mut peer| async move {
        assert_eq!(receive(&mut peer).await["op"], 8);
        sent.send(()).unwrap();
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = RestoreBatchOwner::new(
        broker.scope(ScopeKind::RestoreBatch).unwrap(),
        session.handle(),
        shared_control(),
        clock(),
    )
    .unwrap();
    owner.begin(request(1, Cancellation::default())).unwrap();
    {
        let mut f = Box::pin(owner.finish());
        assert!(poll!(f.as_mut()).is_pending());
        seen.await.unwrap();
    }
    drop(owner);
    assert!(broker.admission().unwrap().blocking);
    assert_eq!(
        broker.read_view().read_any(&id(1)).unwrap()["phase"],
        "dispatching"
    );
    finish_session(session, peer).await;
}

#[tokio::test]
async fn restart_after_ack_keeps_known_counts_but_same_identity_cannot_replay() {
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        let rows = results(&frame);
        batch_reply(&mut peer, &frame, rows).await;
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = RestoreBatchOwner::new(
        broker.scope(ScopeKind::RestoreBatch).unwrap(),
        session.handle(),
        shared_control(),
        clock(),
    )
    .unwrap();
    owner.begin(request(1, Cancellation::default())).unwrap();
    owner.finish().await.unwrap();
    drop(owner.into_store().ok().unwrap());
    drop(broker);
    let restarted = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let v = restarted.read_view().read_any(&id(1)).unwrap();
    assert_eq!(v["successfulRequests"], 7);
    assert_eq!(v["failedRequests"], 1);
    assert!(!restarted.admission().unwrap().blocking);
    let mut owner = RestoreBatchOwner::new(
        restarted.scope(ScopeKind::RestoreBatch).unwrap(),
        session.handle(),
        shared_control(),
        clock(),
    )
    .unwrap();
    assert!(matches!(
        owner.begin(request(1, Cancellation::default())),
        Err(BeginError::Receipt(_))
    ));
    assert!(restarted.admission().is_err());
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}
