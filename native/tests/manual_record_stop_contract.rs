// SPDX-License-Identifier: GPL-2.0-only
//! Real socket/store library tests; no Service/frontend/manual tool admission.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    clock,
    control::ControlDomain,
    generic_requests::Call,
    operation_store::{GenericKind, GenericPhase, OperationStore},
    receipt_broker::{ReceiptBroker, ScopeKind},
    record_impact::{ImpactError, ImpactStatus, RecordImpactOwner},
    record_stop_transport::{BoundManualRecordStop, Request},
    session::{ObsSession, RequestError, SafetyStopRequest, SessionOptions},
    stage_transport::Cancellation,
    takes::Policy,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
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
            "manual-stop-{}-{}",
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
fn context(n: u64, cancel: Cancellation) -> Request {
    Request {
        operation_id: id(n),
        capabilities: None,
        policy: Policy {
            caller: "manual-library-client".into(),
            ..Default::default()
        },
        control: Arc::new(Mutex::new(ControlDomain::default())),
        clock: Arc::new(|| {
            clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap()
        }),
        cancellation: cancel,
        admitted_at: Instant::now(),
        timeout: WAIT,
    }
}
fn intent(store: &mut OperationStore, n: u64) -> obs_mcp_native::operation_store::GenericReceipt {
    let p = store
        .prepare_generic(
            &id(n),
            GenericKind::InputMute,
            1,
            "2026-11-01T12:00:00.000Z",
        )
        .unwrap();
    store
        .update_generic(&p, GenericPhase::Dispatching, "2026-11-01T12:00:00.000Z")
        .unwrap()
}
async fn finish(owner: ObsSession, peer: JoinHandle<()>) {
    owner.shutdown().await.unwrap();
    timeout(WAIT, peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn real_record_callback_bodies_and_durable_ack_fates_match_all_39_source_cases() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned();
    let source = Command::new("node")
        .arg(root.join("native/tests/fixtures/output_stop_source_oracle.mjs"))
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(
        source.status.success(),
        "{}",
        String::from_utf8_lossy(&source.stderr)
    );
    let source: Value = serde_json::from_slice(&source.stdout).unwrap();
    let cases = source["results"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["tool"] == "obs-stop-record")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 39);
    let (calls, mut next) = mpsc::channel::<Value>(1);
    let (trace, observed) = (
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicU64::new(0)),
    );
    let history = trace.clone();
    let count = observed.clone();
    let (url, peer) = fixture(move |mut peer| async move {
        while let Some(case) = next.recv().await {
            let request = receive(&mut peer).await;
            assert_eq!(request["op"], 6);
            assert_eq!(request["d"]["requestType"], "StopRecord");
            assert!(request["d"].get("requestData").is_none());
            history.lock().unwrap().push(request.clone());
            count.fetch_add(1, Ordering::SeqCst);
            let status = &case["ack"]["status"];
            let head = json!({
                "requestId":request["d"]["requestId"],
                "requestType":"StopRecord",
                "requestStatus":status
            })
            .to_string();
            let raw = case["responseDataRaw"].as_str().unwrap();
            let suffix = if raw == "<absent>" {
                String::new()
            } else {
                format!(",\"responseData\":{raw}")
            };
            // Keep the Source fixture's actual numeric and responseData
            // lexemes. An intermediate Rust Value would normalize the oracle.
            let frame = ["{\"op\":7,\"d\":", &head[..head.len() - 1], &suffix, "}}"].concat();
            peer.send(Message::Text(frame.into())).await.unwrap();
        }
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let impact = RecordImpactOwner::bind(&handle).unwrap();
    let driver = BoundManualRecordStop::bind(handle, impact.manual_port()).unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut format_errors = 0;
    for (index, case) in cases.into_iter().enumerate() {
        let ticket = impact.view().reserve_take().unwrap();
        let n = 100 + index as u64;
        calls.send(case.clone()).await.unwrap();
        let out = driver
            .clone()
            .execute(
                broker.scope(ScopeKind::ManualRecordStop).unwrap(),
                context(n, Cancellation::default()),
            )
            .await;
        assert_eq!(out.result, case["structuredResult"], "{}", case["name"]);
        assert!(out.impact_started);
        assert!(out.receipt_error.is_none());
        assert!(out.impact_error.is_none());
        let disk: Value = serde_json::from_slice(&fs::read(dir.receipt(n)).unwrap()).unwrap();
        assert_eq!(disk["requestType"], "StopRecord");
        assert_eq!(disk["postconditionObserved"], false);
        assert!(disk.get("outputPath").is_none());
        assert!(matches!(ticket.check(), ImpactStatus::Revoked(_)));
        if case["knownTransportAccepted"] == true {
            assert_eq!(out.transport, Some(Ok(())));
            assert_eq!(out.receipt.unwrap().phase(), GenericPhase::Accepted);
            assert_eq!(disk["requestAccepted"], true);
            if out.result["isError"] == true {
                format_errors += 1;
                assert_eq!(disk["phase"], "accepted");
            }
        } else {
            assert!(matches!(
                out.transport,
                Some(Err(RequestError::RejectedDetailed { code: 500, .. }))
            ));
            assert_eq!(out.receipt.unwrap().phase(), GenericPhase::Rejected);
            assert_eq!(disk["requestAccepted"], false);
        }
    }
    assert_eq!(format_errors, 8);
    assert_eq!(observed.load(Ordering::SeqCst), 39);
    assert_eq!(trace.lock().unwrap().len(), 39);
    assert!(
        Call::parse("obs-stop-record", &json!({})).is_none(),
        "deferred route was admitted"
    );
    drop(calls);
    assert!(broker.into_store().is_ok());
    finish(owner, peer).await;
}

#[tokio::test]
async fn binding_rejects_wrong_and_dead_owner_before_any_intent() {
    let (url, a) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (other_url, b) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let (other, _other_events) = ObsSession::connect(&other_url, None, SessionOptions::default())
        .await
        .unwrap();
    let impact = RecordImpactOwner::bind(&owner.handle()).unwrap();
    let port = impact.manual_port();
    assert_eq!(port.check_connection(owner.token()), Ok(()));
    assert_eq!(
        port.check_connection(other.token()),
        Err(ImpactError::WrongConnection)
    );
    assert!(matches!(
        BoundManualRecordStop::bind(other.handle(), port.clone()),
        Err(ImpactError::WrongConnection)
    ));
    drop(impact);
    assert!(matches!(
        BoundManualRecordStop::bind(owner.handle(), port),
        Err(ImpactError::OwnerGone)
    ));
    finish(owner, a).await;
    finish(other, b).await;
}

#[tokio::test]
async fn pre_cancel_and_actual_queued_guard_refusal_write_nothing_and_keep_ticket_ready() {
    for queued in [false, true] {
        let (url, peer) = fixture(|mut peer| async move {
            closed(&mut peer).await;
        })
        .await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let impact = RecordImpactOwner::bind(&owner.handle()).unwrap();
        let ticket = impact.view().reserve_take().unwrap();
        let driver = BoundManualRecordStop::bind(owner.handle(), impact.manual_port()).unwrap();
        let dir = Dir::new();
        let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
        let cancel = Cancellation::default();
        if !queued {
            cancel.cancel();
        }
        let mut job = Box::pin(driver.execute(
            broker.scope(ScopeKind::ManualRecordStop).unwrap(),
            context(1, cancel.clone()),
        ));
        if queued {
            assert!(poll!(job.as_mut()).is_pending());
            cancel.cancel();
        }
        let out = job.await;
        assert_eq!(ticket.check(), ImpactStatus::Ready);
        assert!(!out.impact_started);
        if queued {
            assert!(matches!(
                out.transport,
                Some(Err(RequestError::BeforeDispatch(_)))
            ));
            assert_eq!(out.receipt.unwrap().phase(), GenericPhase::NotDispatched);
            let disk: Value = serde_json::from_slice(&fs::read(dir.receipt(1)).unwrap()).unwrap();
            assert_eq!(disk["requestDispatched"], false);
        } else {
            assert!(out.transport.is_none());
            assert!(!dir.receipt(1).exists());
        }
        assert!(broker.into_store().is_ok());
        finish(owner, peer).await;
    }
}

#[tokio::test]
async fn retained_job_joins_known_ack_after_frontend_delivery_and_cancel_are_abandoned() {
    let (seen, seen_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (url, peer) = fixture(|mut peer| async move {
        let request = receive(&mut peer).await;
        seen.send(()).unwrap();
        release_rx.await.unwrap();
        reply(&mut peer, &request, json!({"outputPath":"fixture.mkv"})).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let impact = RecordImpactOwner::bind(&owner.handle()).unwrap();
    let ticket = impact.view().reserve_take().unwrap();
    let driver = BoundManualRecordStop::bind(owner.handle(), impact.manual_port()).unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let cancel = Cancellation::default();
    let request = context(1, cancel.clone());
    let scope = broker.scope(ScopeKind::ManualRecordStop).unwrap();
    let (deliver, frontend) = oneshot::channel();
    let job = tokio::spawn(async move {
        let result = driver.execute(scope, request).await;
        let _ = deliver.send(());
        result
    });
    seen_rx.await.unwrap();
    assert!(matches!(ticket.check(), ImpactStatus::Suspended(_)));
    drop(frontend);
    cancel.cancel();
    release.send(()).unwrap();
    let out = timeout(WAIT, job).await.unwrap().unwrap();
    assert_eq!(out.transport, Some(Ok(())));
    assert_eq!(out.receipt.unwrap().phase(), GenericPhase::Accepted);
    assert!(matches!(ticket.check(), ImpactStatus::Revoked(_)));
    assert!(broker.into_store().is_ok());
    finish(owner, peer).await;
}

#[tokio::test]
async fn unknown_reply_retires_one_connection_and_never_replays() {
    let (url, peer) = fixture(|mut peer| async move {
        let request = receive(&mut peer).await;
        assert_eq!(request["d"]["requestType"], "StopRecord");
        peer.close(None).await.unwrap();
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let impact = RecordImpactOwner::bind(&owner.handle()).unwrap();
    let ticket = impact.view().reserve_take().unwrap();
    let driver = BoundManualRecordStop::bind(owner.handle(), impact.manual_port()).unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let out = driver
        .execute(
            broker.scope(ScopeKind::ManualRecordStop).unwrap(),
            context(1, Cancellation::default()),
        )
        .await;
    assert!(matches!(out.transport, Some(Err(RequestError::Unknown(_)))));
    assert_eq!(out.receipt.unwrap().phase(), GenericPhase::Unknown);
    assert!(matches!(ticket.check(), ImpactStatus::Revoked(_)));
    assert!(broker.scope(ScopeKind::ManualRecordStop).is_err());
    finish(owner, peer).await;
}

#[tokio::test]
async fn missing_live_loan_poison_survives_scope_drop_and_forbids_manual_dispatch() {
    let (url, peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let impact = RecordImpactOwner::bind(&owner.handle()).unwrap();
    let ticket = impact.view().reserve_take().unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut ordinary = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    intent(&mut ordinary, 1);
    let manual = broker.scope(ScopeKind::ManualRecordStop).unwrap();
    fs::remove_file(dir.receipt(1)).unwrap();
    let driver = BoundManualRecordStop::bind(owner.handle(), impact.manual_port()).unwrap();
    let out = driver
        .execute(manual, context(2, Cancellation::default()))
        .await;
    assert!(out.transport.is_none());
    assert!(!out.impact_started);
    assert_eq!(ticket.check(), ImpactStatus::Ready);
    assert!(!dir.receipt(2).exists());
    drop(ordinary);
    assert!(broker.admission().is_err());
    assert!(broker.scope(ScopeKind::ManualRecordStop).is_err());
    assert!(broker.into_store().is_err());
    finish(owner, peer).await;
}

#[tokio::test]
async fn known_ack_is_not_downgraded_when_terminal_receipt_publication_is_poisoned() {
    let ack = Arc::new(AtomicBool::new(false));
    let sent = ack.clone();
    let (url, peer) = fixture(move |mut peer| async move {
        let request = receive(&mut peer).await;
        sent.store(true, Ordering::SeqCst);
        reply(&mut peer, &request, json!({"outputPath":"fixture.mkv"})).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let impact = RecordImpactOwner::bind(&owner.handle()).unwrap();
    let ticket = impact.view().reserve_take().unwrap();
    let dir = Dir::new();
    let path = dir.receipt(1);
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let once = Arc::new(AtomicBool::new(false));
    let gate = once.clone();
    let mut request = context(1, Cancellation::default());
    let normal = request.clock.clone();
    request.clock = Arc::new(move || {
        if ack.load(Ordering::SeqCst) && !gate.swap(true, Ordering::SeqCst) {
            fs::write(&path, b"{}").unwrap();
        }
        normal()
    });
    let driver = BoundManualRecordStop::bind(owner.handle(), impact.manual_port()).unwrap();
    let out = driver
        .execute(broker.scope(ScopeKind::ManualRecordStop).unwrap(), request)
        .await;
    assert_eq!(out.transport, Some(Ok(())));
    assert!(out.receipt_error.is_some());
    assert_eq!(out.result["isError"], true);
    assert!(
        out.result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("acknowledged")
    );
    assert!(matches!(ticket.check(), ImpactStatus::Revoked(_)));
    assert!(broker.admission().is_err());
    assert!(broker.into_store().is_err());
    finish(owner, peer).await;
}

#[tokio::test]
async fn shared_public_scope_and_closed_driver_never_accept_ordinary_or_other_stop_scope() {
    let (url, peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let impact = RecordImpactOwner::bind(&owner.handle()).unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let safety = broker
        .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
        .unwrap();
    assert!(broker.scope(ScopeKind::ManualRecordStop).is_err());
    let driver = BoundManualRecordStop::bind(owner.handle(), impact.manual_port()).unwrap();
    let out = driver
        .execute(safety, context(1, Cancellation::default()))
        .await;
    assert!(out.transport.is_none());
    assert!(!dir.receipt(1).exists());
    let manual = broker.scope(ScopeKind::ManualRecordStop).unwrap();
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err()
    );
    drop(manual);
    assert!(broker.into_store().is_ok());
    finish(owner, peer).await;
}

#[tokio::test]
async fn closed_manual_lane_bypasses_held_normal_and_leaves_owned_status_lane_free() {
    let (seen, normal_seen) = oneshot::channel();
    let (release, held) = oneshot::channel();
    let (manual_sent, manual_seen) = oneshot::channel();
    let (url, peer) = fixture(move |mut peer| async move {
        let normal = receive(&mut peer).await;
        assert_eq!(normal["d"]["requestType"], "SetPersistentData");
        seen.send(()).unwrap();
        let manual = receive(&mut peer).await;
        assert_eq!(manual["d"]["requestType"], "StopRecord");
        assert!(manual["d"].get("requestData").is_none());
        manual_sent.send(()).unwrap();
        let status = receive(&mut peer).await;
        assert_eq!(status["d"]["requestType"], "GetRecordStatus");
        assert_ne!(normal["d"]["requestId"], manual["d"]["requestId"]);
        assert_ne!(manual["d"]["requestId"], status["d"]["requestId"]);
        assert_ne!(normal["d"]["requestId"], status["d"]["requestId"]);
        reply(&mut peer, &status, json!({"outputActive":false})).await;
        held.await.unwrap();
        reply(&mut peer, &manual, json!({"outputPath":"fixture"})).await;
        reply(&mut peer, &normal, json!({})).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let impact = RecordImpactOwner::bind(&handle).unwrap();
    let ticket = impact.view().reserve_take().unwrap();
    let driver = BoundManualRecordStop::bind(handle.clone(), impact.manual_port()).unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut ordinary = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let live = intent(&mut ordinary, 800);
    let mut normal = Box::pin(handle.request(
        handle.token(),
        "SetPersistentData",
        json!({"slotName":"fixture","slotValue":1}),
        WAIT,
    ));
    tokio::select! {r=&mut normal=>panic!("normal request unexpectedly settled {r:?}"),r=normal_seen=>r.unwrap()}
    let manual = broker.scope(ScopeKind::ManualRecordStop).unwrap();
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err()
    );
    let mut stop = Box::pin(driver.execute(manual, context(801, Cancellation::default())));
    tokio::select! {r=&mut stop=>panic!("held manual ACK unexpectedly settled {r:?}"),r=manual_seen=>r.unwrap()}
    let status = handle.stop_record_status_checked(handle.token(), WAIT, || Ok(()));
    tokio::pin!(status);
    // Both futures are polled concurrently; actual socket receive ordering
    // establishes manual dispatch while the normal ACK remains withheld.
    let read = tokio::select! {r=&mut stop=>panic!("held manual ACK unexpectedly settled {r:?}"),r=&mut status=>r.unwrap()};
    assert_eq!(read["outputActive"], false);
    assert!(matches!(ticket.check(), ImpactStatus::Suspended(_)));
    release.send(()).unwrap();
    let (stop, normal) = tokio::join!(stop, normal);
    assert!(matches!(stop.transport, Some(Ok(()))));
    assert!(normal.is_ok());
    assert_eq!(stop.receipt.unwrap().phase(), GenericPhase::Accepted);
    ordinary
        .update_generic(&live, GenericPhase::Accepted, "2026-11-01T12:00:01.000Z")
        .unwrap();
    assert!(matches!(ticket.check(), ImpactStatus::Revoked(_)));
    drop(ordinary);
    assert!(broker.into_store().is_ok());
    finish(owner, peer).await;
}

#[test]
fn orphan_dispatching_and_unknown_receipts_remain_blocking_for_manual_scope() {
    for phase in [GenericPhase::Dispatching, GenericPhase::Unknown] {
        let dir = Dir::new();
        let mut store = OperationStore::new(&dir.0, "ws://127.0.0.1:4455").unwrap();
        let receipt = intent(&mut store, 950);
        if phase == GenericPhase::Unknown {
            store
                .update_generic(&receipt, phase, "2026-11-01T12:00:01.000Z")
                .unwrap();
        }
        drop(store);
        let broker =
            ReceiptBroker::new(OperationStore::new(&dir.0, "ws://127.0.0.1:4455").unwrap())
                .unwrap();
        assert!(
            broker.scope(ScopeKind::ManualRecordStop).is_err(),
            "orphan/unknown proof was excluded: {phase:?}"
        );
        assert!(broker.into_store().is_ok());
    }
}

#[tokio::test]
async fn capability_context_from_other_physical_session_refuses_before_intent() {
    let (url, peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (other_url, other_peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let (other, _other_events) = ObsSession::connect(&other_url, None, SessionOptions::default())
        .await
        .unwrap();
    let impact = RecordImpactOwner::bind(&owner.handle()).unwrap();
    let ticket = impact.view().reserve_take().unwrap();
    let driver = BoundManualRecordStop::bind(owner.handle(), impact.manual_port()).unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut request = context(990, Cancellation::default());
    request.capabilities =
        obs_mcp_native::record_stop_transport::StopRecordCapability::from_version(
            &other.handle(),
            &json!({"availableRequests":["StopRecord"]}),
        );
    assert!(request.capabilities.is_some());
    let result = driver
        .execute(broker.scope(ScopeKind::ManualRecordStop).unwrap(), request)
        .await;
    assert!(result.receipt.is_none());
    assert!(result.transport.is_none());
    assert!(!result.impact_started);
    assert_eq!(result.result["isError"], true);
    assert_eq!(ticket.check(), ImpactStatus::Ready);
    assert!(broker.into_store().is_ok());
    finish(owner, peer).await;
    finish(other, other_peer).await;
}
