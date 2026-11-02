// SPDX-License-Identifier: GPL-2.0-only
//! Actual Session/Broker contracts; no public Apply Scene admission.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    apply_scene::{Effect, Request as SceneRequest},
    apply_scene_transport::{ApplySceneOwner, BeginError, Capabilities, Stage, Start},
    clock,
    control::{ControlDomain, TakeLock},
    operation_store::{
        ApplyCounts, ApplyPhase, ApplyReceipt, ApplyUpdate, GenericKind, GenericPhase,
        OperationKind, OperationStore, PrepareDetails,
    },
    receipt_broker::{ReceiptBroker, ScopeKind},
    session::{BeforeDispatch, ObsSession, RequestError, SafetyStopRequest, SessionOptions},
    stage_transport::Cancellation,
    tool_validation::ApplySceneSettingsOrder,
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
            "apply-batch-{}-{}",
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
fn scene_request(apply: bool) -> SceneRequest {
    let args = json!({"sceneName":"PRIVATE_SCENE","sources":[{"name":"PRIVATE_INPUT","kind":"image_source","settings":{"PRIVATE_SETTING":"PRIVATE_VALUE"},"visible":false,"locked":true,"muted":true,"volumeDb":-6,"audioTracks":[2],"transform":{"positionX":5}},{"name":"PRIVATE_NEW","kind":"image_source"}],"order":true,"removeOthers":true,"apply":apply});
    let frame = json!({"params":{"arguments":args}}).to_string();
    let order = ApplySceneSettingsOrder::from_frame(&args, frame.as_bytes()).unwrap();
    SceneRequest::parse(&args, &order).unwrap()
}
fn creations() -> Vec<Effect> {
    vec![
        Effect::CreateScene {
            scene_name: "PRIVATE_SCENE".into(),
        },
        Effect::CreateInput {
            scene_name: "PRIVATE_SCENE".into(),
            input_name: "PRIVATE_NEW".into(),
            kind: "image_source".into(),
            settings: json!({}),
            enabled: true,
        },
        Effect::CreateItem {
            scene_name: "PRIVATE_SCENE".into(),
            input_name: "PRIVATE_INPUT".into(),
            enabled: false,
        },
    ]
}
fn changes() -> Vec<Effect> {
    vec![
        Effect::SettingsOverlay {
            input_name: "PRIVATE_INPUT".into(),
            settings: json!({"PRIVATE_SETTING":"PRIVATE_VALUE"}),
        },
        Effect::Mute {
            input_name: "PRIVATE_INPUT".into(),
            muted: true,
        },
        Effect::VolumeDb {
            input_name: "PRIVATE_INPUT".into(),
            db: -6.0,
        },
        Effect::AudioTracks {
            input_name: "PRIVATE_INPUT".into(),
            tracks: json!({"1":false,"2":true,"3":false,"4":false,"5":false,"6":false}),
        },
        Effect::Transform {
            scene_name: "PRIVATE_SCENE".into(),
            id: 3.0,
            transform: json!({"positionX":5}),
        },
        Effect::Enabled {
            scene_name: "PRIVATE_SCENE".into(),
            id: 3.0,
            enabled: false,
        },
        Effect::Locked {
            scene_name: "PRIVATE_SCENE".into(),
            id: 3.0,
            locked: true,
        },
        Effect::RemoveItem {
            scene_name: "PRIVATE_SCENE".into(),
            id: 9.0,
        },
        Effect::Index {
            scene_name: "PRIVATE_SCENE".into(),
            id: 3.0,
            index: 0,
        },
    ]
}
fn start(n: u64, cancel: Cancellation) -> Start {
    Start {
        operation_id: id(n),
        request: scene_request(true),
        caller: "apply-client".into(),
        creations: creations(),
        capabilities: None,
        cancellation: cancel,
        admitted_at: Instant::now(),
        timeout: WAIT,
    }
}
fn results(frame: &Value, failure: Option<usize>, halt: bool) -> Vec<Value> {
    frame["d"]["requests"].as_array().unwrap().iter().enumerate().take_while(|(i,_)|!halt||failure.is_none_or(|failure|*i<=failure)).map(|(i,r)|json!({"requestId":r["requestId"],"requestType":r["requestType"],"requestStatus":{"result":Some(i)!=failure,"code":if Some(i)==failure{601}else{100},"comment":"PRIVATE_COMMENT"}})).collect()
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
fn owner(
    broker: &ReceiptBroker,
    session: &ObsSession,
    control: Arc<Mutex<ControlDomain>>,
) -> ApplySceneOwner {
    ApplySceneOwner::new(
        broker.scope(ScopeKind::ApplyScene).unwrap(),
        session.handle(),
        control,
        clock(),
    )
    .unwrap()
}
fn assert_metadata(receipt: &ApplyReceipt) {
    let value = receipt.to_value();
    assert_eq!(value["version"], 4);
    assert_eq!(value["postconditionObserved"], false);
    assert_eq!(value["renderedComposition"], "unverified");
    assert!(!value.to_string().contains("PRIVATE"));
}

#[tokio::test]
async fn exact_two_frames_fixed_methods_options_mixed_changes_and_immutable_prefix() {
    let (url, peer) = fixture(|mut peer| async move {
        for (stage, methods) in [
            (true, vec!["CreateScene", "CreateInput", "CreateSceneItem"]),
            (
                false,
                vec![
                    "SetInputSettings",
                    "SetInputMute",
                    "SetInputVolume",
                    "SetInputAudioTracks",
                    "SetSceneItemTransform",
                    "SetSceneItemEnabled",
                    "SetSceneItemLocked",
                    "RemoveSceneItem",
                    "SetSceneItemIndex",
                ],
            ),
        ] {
            let frame = receive(&mut peer).await;
            assert_eq!(frame["op"], 8);
            assert_eq!(frame["d"]["executionType"], 0);
            assert_eq!(frame["d"]["haltOnFailure"], stage);
            for (i, row) in frame["d"]["requests"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
            {
                assert_eq!(row["requestType"], methods[i]);
                assert_eq!(row["requestId"], i.to_string());
            }
            if !stage {
                assert_eq!(frame["d"]["requests"][0]["requestData"]["overlay"], true);
            }
            let rows = results(&frame, (!stage).then_some(1), stage);
            batch_reply(&mut peer, &frame, rows).await;
        }
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = owner(&broker, &session, shared_control());
    owner.begin(start(1, Cancellation::default())).unwrap();
    let first = owner.finish().await.unwrap();
    assert!(first.receipt_error.is_none());
    assert_eq!(first.receipt.phase(), ApplyPhase::Continuation);
    let prefix = first.receipt.to_value()["creations"].clone();
    assert!(broker.admission().unwrap().blocking);
    owner = *owner.into_store().err().unwrap();
    owner.begin_changes(changes()).unwrap();
    assert_eq!(owner.receipt().unwrap().to_value()["creations"], prefix);
    let second = owner.finish().await.unwrap();
    assert_eq!(second.stage, Stage::Changes);
    assert_eq!(second.receipt.phase(), ApplyPhase::Closed);
    assert_eq!(second.receipt.to_value()["creations"], prefix);
    assert_eq!(
        second.acknowledged,
        Some(ApplyCounts {
            successful: 8,
            failed: 1,
            skipped: 0
        })
    );
    assert_metadata(&second.receipt);
    assert!(!broker.admission().unwrap().blocking);
    assert!(broker.admission().unwrap().generic_v2_present);
    assert!(owner.begin_changes(changes()).is_err());
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}
#[tokio::test]
async fn zero_creations_direct_change_or_explicit_zero_effect_closure() {
    for effects in [true, false] {
        let (url, peer) = fixture(move |mut peer| async move {
            if effects {
                let frame = receive(&mut peer).await;
                assert_eq!(frame["d"]["haltOnFailure"], false);
                let rows = results(&frame, None, false);
                batch_reply(&mut peer, &frame, rows).await;
            }
            closed(&mut peer).await;
        })
        .await;
        let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let dir = Dir::new();
        let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
        let mut owner = owner(&broker, &session, shared_control());
        let mut r = start(1, Cancellation::default());
        r.creations.clear();
        owner.begin(r).unwrap();
        assert!(!owner.has_pending());
        assert_eq!(
            owner.receipt().unwrap().to_value()["creations"]["phase"],
            "not-needed"
        );
        assert!(broker.admission().unwrap().blocking);
        let receipt = if effects {
            owner.begin_changes(changes()).unwrap();
            owner.finish().await.unwrap().receipt
        } else {
            owner.close().unwrap()
        };
        assert_eq!(receipt.phase(), ApplyPhase::Closed);
        assert_metadata(&receipt);
        if !effects {
            assert_eq!(receipt.to_value()["changes"]["phase"], "not-needed");
            assert_eq!(
                receipt.to_value()["changes"]["batchAcknowledged"],
                Value::Null
            );
        }
        drop(owner.into_store().ok().unwrap());
        finish_session(session, peer).await;
    }
}
#[tokio::test]
async fn creation_halt_failure_has_exact_acknowledged_skip_counts_and_no_second_frame() {
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        let rows = results(&frame, Some(1), true);
        batch_reply(&mut peer, &frame, rows).await;
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = owner(&broker, &session, shared_control());
    owner.begin(start(1, Cancellation::default())).unwrap();
    let out = owner.finish().await.unwrap();
    assert_eq!(out.receipt.phase(), ApplyPhase::Aborted);
    assert_eq!(
        out.acknowledged,
        Some(ApplyCounts {
            successful: 1,
            failed: 1,
            skipped: 1
        })
    );
    assert_eq!(out.not_attempted, None);
    assert_eq!(
        out.receipt.to_value()["creations"]["notAttempted"],
        Value::Null
    );
    assert!(owner.begin_changes(changes()).is_err());
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}

#[tokio::test]
async fn cancelled_frontend_and_refused_transfer_retain_each_exact_observer_until_join() {
    for second in [false, true] {
        let (sent, seen) = oneshot::channel();
        let (release, held) = oneshot::channel();
        let (url, peer) = fixture(move |mut peer| async move {
            if second {
                let first = receive(&mut peer).await;
                let rows = results(&first, None, true);
                batch_reply(&mut peer, &first, rows).await;
            }
            let frame = receive(&mut peer).await;
            sent.send(()).unwrap();
            held.await.unwrap();
            let rows = results(&frame, None, !second);
            batch_reply(&mut peer, &frame, rows).await;
            closed(&mut peer).await;
        })
        .await;
        let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let dir = Dir::new();
        let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
        let mut owner = owner(&broker, &session, shared_control());
        let cancel = Cancellation::default();
        owner.begin(start(1, cancel.clone())).unwrap();
        if second {
            owner.finish().await.unwrap();
            owner.begin_changes(changes()).unwrap();
        }
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
            .expect("refusal retains pending owner");
        let mut shutdown = Box::pin(async move {
            let out = owner.finish().await.unwrap();
            if !second {
                owner.abort().unwrap();
            }
            drop(owner.into_store().ok().unwrap());
            session.shutdown().await.unwrap();
            out
        });
        assert!(poll!(shutdown.as_mut()).is_pending());
        release.send(()).unwrap();
        let out = shutdown.await;
        assert!(out.transport.is_ok());
        assert_eq!(out.acknowledged.unwrap().failed, 0);
        assert!(!broker.admission().unwrap().blocking);
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
}
#[tokio::test]
async fn lease_or_cancel_while_either_stage_is_queued_prevents_effects_without_fabricated_ack() {
    for second in [false, true] {
        for lease in [false, true] {
            let (sent, seen) = oneshot::channel();
            let (release, held) = oneshot::channel();
            let (url, peer) = fixture(move |mut peer| async move {
                if second {
                    let first = receive(&mut peer).await;
                    let rows = results(&first, None, true);
                    batch_reply(&mut peer, &first, rows).await;
                }
                let blocker = receive(&mut peer).await;
                assert_eq!(blocker["op"], 6);
                sent.send(()).unwrap();
                held.await.unwrap();
                reply(&mut peer, &blocker, json!({})).await;
                closed(&mut peer).await;
            })
            .await;
            let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
                .await
                .unwrap();
            let dir = Dir::new();
            let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
            let control = shared_control();
            let mut owner = owner(&broker, &session, control.clone());
            let cancel = Cancellation::default();
            if second {
                owner.begin(start(1, cancel.clone())).unwrap();
                owner.finish().await.unwrap();
            }
            let handle = session.handle();
            let mut blocker =
                Box::pin(handle.request(handle.token(), "GetStats", Value::Null, WAIT));
            assert!(poll!(blocker.as_mut()).is_pending());
            seen.await.unwrap();
            if second {
                owner.begin_changes(changes()).unwrap();
            } else {
                owner.begin(start(1, cancel.clone())).unwrap();
            }
            {
                let mut frontend = Box::pin(owner.finish());
                assert!(poll!(frontend.as_mut()).is_pending());
            }
            if lease {
                control
                    .lock()
                    .unwrap()
                    .claim("other-client", 1, "", 1_793_448_000_000)
                    .unwrap();
            } else {
                cancel.cancel();
            }
            release.send(()).unwrap();
            blocker.await.unwrap();
            let out = owner.finish().await.unwrap();
            assert!(matches!(
                out.transport,
                Err(RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(
                    _
                )))
            ));
            assert_eq!(out.receipt.phase(), ApplyPhase::Aborted);
            assert_eq!(out.acknowledged, None);
            assert_eq!(out.not_attempted, Some(if second { 9 } else { 3 }));
            assert_eq!(out.policy_refusal.is_some(), lease);
            let stage = if second { "changes" } else { "creations" };
            assert_eq!(out.receipt.to_value()[stage]["counts"], Value::Null);
            assert_eq!(
                out.receipt.to_value()[stage]["batchAcknowledged"],
                Value::Null
            );
            if second {
                assert_eq!(out.receipt.creation_counts().unwrap().successful, 3);
            }
            drop(owner.into_store().ok().unwrap());
            finish_session(session, peer).await;
        }
    }
}
#[tokio::test]
async fn original_policy_and_deadline_cannot_be_replaced_between_stages() {
    for case in ["lease", "cancel", "deadline"] {
        let (url, peer) = fixture(|mut peer| async move {
            let frame = receive(&mut peer).await;
            let rows = results(&frame, None, true);
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
        let mut owner = owner(&broker, &session, control.clone());
        let cancel = Cancellation::default();
        let mut r = start(1, cancel.clone());
        if case == "deadline" {
            r.timeout = Duration::from_secs(2);
            r.admitted_at = Instant::now() - Duration::from_secs(1);
        }
        owner.begin(r).unwrap();
        let first = owner.finish().await.unwrap();
        if case == "deadline" {
            tokio::time::sleep(Duration::from_millis(1100)).await;
            assert!(matches!(
                owner.begin_changes(changes()),
                Err(BeginError::Deadline)
            ));
        } else if case == "cancel" {
            cancel.cancel();
            assert!(matches!(
                owner.begin_changes(changes()),
                Err(BeginError::Cancelled)
            ));
        } else {
            control
                .lock()
                .unwrap()
                .claim("other-client", 1, "", 1_793_448_000_000)
                .unwrap();
            assert!(matches!(
                owner.begin_changes(changes()),
                Err(BeginError::Policy(_))
            ));
        }
        assert_eq!(
            owner.receipt().unwrap().to_value(),
            first.receipt.to_value()
        );
        let terminal = owner.abort().unwrap();
        assert_eq!(terminal.phase(), ApplyPhase::Aborted);
        assert_eq!(terminal.creation_counts(), first.receipt.creation_counts());
        assert_eq!(terminal.to_value()["changes"], Value::Null);
        drop(owner.into_store().ok().unwrap());
        finish_session(session, peer).await;
    }
}
#[tokio::test]
async fn invoked_guard_can_still_prove_not_dispatched_without_acknowledged_skips() {
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
        if seen.fetch_add(1, Ordering::SeqCst) == 3 {
            trigger.cancel();
        }
        clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap()
    });
    let mut owner = ApplySceneOwner::new(
        broker.scope(ScopeKind::ApplyScene).unwrap(),
        session.handle(),
        shared_control(),
        checked_clock,
    )
    .unwrap();
    owner.begin(start(1, cancel)).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let out = owner.finish().await.unwrap();
    assert!(calls.load(Ordering::SeqCst) >= 5);
    assert!(matches!(
        out.transport,
        Err(RequestError::BeforeDispatch(_))
    ));
    assert_eq!(out.not_attempted, Some(3));
    assert_eq!(out.acknowledged, None);
    assert_eq!(out.receipt.to_value()["creations"]["counts"], Value::Null);
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}
#[tokio::test]
async fn lost_malformed_and_noncanonical_vectors_are_unknown_without_replay() {
    for second in [false, true] {
        for case in ["lost", "missing", "foreign", "bad-status", "bad-prefix"] {
            let (url, peer) = fixture(move |mut peer| async move {
                if second {
                    let first = receive(&mut peer).await;
                    let rows = results(&first, None, true);
                    batch_reply(&mut peer, &first, rows).await;
                }
                let frame = receive(&mut peer).await;
                if case == "lost" {
                    peer.close(None).await.unwrap();
                    return;
                }
                let mut rows = results(&frame, None, !second);
                match case {
                    "missing" => {
                        rows.pop();
                    }
                    "foreign" => rows[0]["requestId"] = json!("foreign"),
                    "bad-status" => rows[0]["requestStatus"]["result"] = json!("true"),
                    _ => {
                        if second {
                            rows.clear();
                        } else {
                            rows[0]["requestStatus"]["result"] = json!(false);
                            rows[0]["requestStatus"]["code"] = json!(601);
                        }
                    }
                };
                batch_reply(&mut peer, &frame, rows).await;
                closed(&mut peer).await;
            })
            .await;
            let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
                .await
                .unwrap();
            let dir = Dir::new();
            let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
            let mut owner = owner(&broker, &session, shared_control());
            owner.begin(start(1, Cancellation::default())).unwrap();
            if second {
                owner.finish().await.unwrap();
                owner.begin_changes(changes()).unwrap();
            }
            let out = owner.finish().await.unwrap();
            assert!(matches!(out.transport, Err(RequestError::Unknown(_))));
            assert_eq!(out.receipt.phase(), ApplyPhase::Unknown);
            assert_eq!(out.acknowledged, None);
            if second {
                assert_eq!(out.receipt.creation_counts().unwrap().successful, 3);
            }
            assert!(broker.admission().unwrap().blocking);
            assert!(owner.begin_changes(changes()).is_err());
            drop(owner.into_store().ok().unwrap());
            drop(broker);
            let restarted = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
            assert!(restarted.admission().unwrap().blocking);
            assert!(restarted.scope(ScopeKind::ApplyScene).is_err());
            assert!(
                restarted
                    .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
                    .is_err()
            );
            finish_session(session, peer).await;
        }
    }
}
#[tokio::test]
async fn held_ack_deadline_becomes_unknown_and_never_continues() {
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        assert_eq!(frame["op"], 8);
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = owner(&broker, &session, shared_control());
    let mut r = start(1, Cancellation::default());
    r.timeout = Duration::from_secs(2);
    owner.begin(r).unwrap();
    let out = owner.finish().await.unwrap();
    assert!(matches!(out.transport, Err(RequestError::Unknown(_))));
    assert_eq!(out.receipt.phase(), ApplyPhase::Unknown);
    assert!(owner.begin_changes(changes()).is_err());
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}
#[tokio::test]
async fn continuation_restart_is_an_interlock_not_an_automatic_second_stage() {
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        let rows = results(&frame, None, true);
        batch_reply(&mut peer, &frame, rows).await;
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = owner(&broker, &session, shared_control());
    owner.begin(start(1, Cancellation::default())).unwrap();
    let out = owner.finish().await.unwrap();
    drop(owner);
    drop(broker);
    let restarted = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    assert_eq!(
        restarted.read_view().read_any(&id(1)).unwrap(),
        out.receipt.to_value()
    );
    assert!(restarted.admission().unwrap().blocking);
    assert!(restarted.scope(ScopeKind::ApplyScene).is_err());
    assert!(
        restarted
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err()
    );
    finish_session(session, peer).await;
}

#[tokio::test]
async fn publication_faults_before_and_after_each_frame_are_sticky_and_keep_known_ack_facts() {
    for case in [
        "prepared",
        "dispatching",
        "creation-settlement",
        "change-settlement",
        "change-dispatching",
    ] {
        let dir = Dir::new();
        let receipt_path = dir.receipt(1);
        let target = receipt_path.clone();
        let (url, peer) = fixture(move |mut peer| async move {
            if case == "prepared" || case == "dispatching" {
                closed(&mut peer).await;
                return;
            }
            let first = receive(&mut peer).await;
            if case == "creation-settlement" {
                fs::write(&target, b"{}").unwrap();
            }
            let rows = results(&first, None, true);
            batch_reply(&mut peer, &first, rows).await;
            if case == "change-settlement" {
                let second = receive(&mut peer).await;
                fs::write(&target, b"{}").unwrap();
                let rows = results(&second, Some(1), false);
                batch_reply(&mut peer, &second, rows).await;
            }
            closed(&mut peer).await;
        })
        .await;
        let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let path = receipt_path.clone();
        let fault_clock: obs_mcp_native::custody::Clock = Arc::new(move || {
            let n = count.fetch_add(1, Ordering::SeqCst);
            if case == "prepared" && n == 1 {
                fs::write(path.parent().unwrap(), b"blocking file").unwrap();
            }
            if (case == "dispatching" && n == 2) || (case == "change-dispatching" && n == 6) {
                fs::write(&path, b"{}").unwrap();
            }
            clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap()
        });
        let mut owner = ApplySceneOwner::new(
            broker.scope(ScopeKind::ApplyScene).unwrap(),
            session.handle(),
            shared_control(),
            fault_clock,
        )
        .unwrap();
        if case == "change-dispatching" {
            owner.begin(start(1, Cancellation::default())).unwrap();
            let first = owner.finish().await.unwrap();
            assert_eq!(first.receipt.creation_counts().unwrap().successful, 3);
            assert!(matches!(
                owner.begin_changes(changes()),
                Err(BeginError::Receipt(_))
            ));
            assert_eq!(
                owner.receipt().unwrap().creation_counts(),
                first.receipt.creation_counts()
            );
        } else if case.ends_with("settlement") {
            owner.begin(start(1, Cancellation::default())).unwrap();
            let mut out = owner.finish().await.unwrap();
            if case == "change-settlement" {
                assert!(out.receipt_error.is_none());
                owner.begin_changes(changes()).unwrap();
                out = owner.finish().await.unwrap();
                assert_eq!(out.receipt.creation_counts().unwrap().successful, 3);
            }
            assert!(out.transport.is_ok());
            assert!(out.acknowledged.is_some());
            assert!(out.receipt_error.is_some());
            assert!(matches!(
                out.receipt.phase(),
                ApplyPhase::DispatchingCreations | ApplyPhase::DispatchingChanges
            ));
        } else {
            assert!(matches!(
                owner.begin(start(1, Cancellation::default())),
                Err(BeginError::Receipt(_))
            ));
        }
        assert!(owner.is_poisoned());
        assert!(broker.admission().is_err());
        assert!(owner.begin_changes(changes()).is_err());
        assert!(!dir.receipt(2).exists());
        drop(owner.into_store().ok().unwrap());
        drop(broker);
        assert!(
            OperationStore::new(&dir.0, &url).is_err()
                || OperationStore::new(&dir.0, &url)
                    .unwrap()
                    .read_view()
                    .admission()
                    .is_err()
        );
        finish_session(session, peer).await;
    }
}
#[test]
fn dedicated_scope_strict_schema_stale_and_foreign_updates_never_gain_other_authority() {
    let dir = Dir::new();
    let endpoint = "ws://127.0.0.1:4455";
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, endpoint).unwrap()).unwrap();
    for kind in [
        ScopeKind::Take,
        ScopeKind::Stage,
        ScopeKind::CriticalRecord,
        ScopeKind::RestoreBatch,
        ScopeKind::ManualRecordStop,
        ScopeKind::Generic(GenericKind::InputMute),
        ScopeKind::SafetyStop(SafetyStopRequest::Stream),
    ] {
        let mut port = broker.scope(kind).unwrap();
        assert!(port.prepare_apply(&id(1), 1, 3, UTC).is_err());
    }
    let mut first = broker.scope(ScopeKind::ApplyScene).unwrap();
    let mut other = broker.scope(ScopeKind::ApplyScene).unwrap();
    let prepared = first.prepare_apply(&id(1), 1, 3, UTC).unwrap();
    assert!(
        other
            .update_apply(&prepared, ApplyUpdate::DispatchCreations, UTC)
            .is_err()
    );
    for k in [
        GenericKind::InputMute,
        GenericKind::ManualRecordStop,
        GenericKind::StreamStart,
    ] {
        assert!(first.prepare_generic(&id(2), k, 1, UTC).is_err());
    }
    for k in [
        OperationKind::RecordStart,
        OperationKind::RecordStop,
        OperationKind::StageScene,
        OperationKind::TakeScene,
    ] {
        assert!(
            first
                .prepare(&id(2), k, 1, UTC, PrepareDetails::default())
                .is_err()
        );
    }
    assert!(first.prepare_restore(&id(2), 1, 3, UTC).is_err());
    let dispatch = first
        .update_apply(&prepared, ApplyUpdate::DispatchCreations, UTC)
        .unwrap();
    assert!(
        first
            .update_apply(&prepared, ApplyUpdate::Abort, UTC)
            .is_err()
    );
    let ack = first
        .update_apply(
            &dispatch,
            ApplyUpdate::CreationsAcknowledged(ApplyCounts {
                successful: 3,
                failed: 0,
                skipped: 0,
            }),
            UTC,
        )
        .unwrap();
    let prefix = ack.to_value()["creations"].clone();
    assert!(ack.phase() == ApplyPhase::Continuation);
    let change = first
        .update_apply(&ack, ApplyUpdate::DispatchChanges(2), UTC)
        .unwrap();
    assert_eq!(change.to_value()["creations"], prefix);
    let terminal = first
        .update_apply(
            &change,
            ApplyUpdate::ChangesAcknowledged(ApplyCounts {
                successful: 1,
                failed: 1,
                skipped: 0,
            }),
            UTC,
        )
        .unwrap();
    assert_eq!(terminal.to_value()["creations"], prefix);
    assert!(
        first
            .update_apply(&terminal, ApplyUpdate::DispatchChanges(1), UTC)
            .is_err()
    );
    assert!(!broker.admission().unwrap().blocking);
    assert!(broker.admission().unwrap().generic_v2_present);
    for (field, value) in [
        ("version", json!(3)),
        ("kind", json!("request")),
        ("postconditionObserved", json!(true)),
        ("renderedComposition", json!("verified")),
        ("requestType", json!("StopRecord")),
    ] {
        let mut v = terminal.to_value();
        v[field] = value;
        assert!(ApplyReceipt::from_value(v).is_err());
    }
    for (field, value) in [
        ("haltOnFailure", json!(true)),
        ("executionType", json!(1)),
        ("batchAcknowledged", Value::Null),
        ("counts", json!({"successful":1,"failed":0,"skipped":1})),
    ] {
        let mut v = terminal.to_value();
        v["changes"][field] = value;
        assert!(ApplyReceipt::from_value(v).is_err());
    }
    let mut v = terminal.to_value();
    v["creations"]
        .as_object_mut()
        .unwrap()
        .remove("notAttempted");
    assert!(ApplyReceipt::from_value(v).is_err());
    assert!(
        obs_mcp_native::operation_store::OperationReceipt::from_value(terminal.to_value()).is_err()
    );
    let summary = broker.read_view().summary();
    assert!(summary["operations"][0].get("endpointHash").is_none());
    assert_eq!(summary["operations"][0]["status"], "closed");
}
#[tokio::test]
async fn safety_stop_excludes_only_exact_live_valid_apply_phases_never_orphaned_or_unknown() {
    let (url, peer) = fixture(|mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = owner(&broker, &session, shared_control());
    let mut r = start(1, Cancellation::default());
    r.creations.clear();
    owner.begin(r).unwrap();
    let mut stop = broker
        .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
        .unwrap();
    let s = stop
        .prepare_generic(&id(2), GenericKind::StreamStop, 1, UTC)
        .unwrap();
    let s = stop
        .update_generic(&s, GenericPhase::Dispatching, UTC)
        .unwrap();
    assert!(stop.dispatch_fence().unwrap().permits(&id(2)).unwrap());
    assert!(!owner.has_pending());
    stop.update_generic(&s, GenericPhase::NotDispatched, UTC)
        .unwrap();
    drop(stop);
    drop(owner);
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err()
    );
    assert!(
        broker
            .scope(ScopeKind::Generic(GenericKind::InputMute))
            .is_err()
    );
    finish_session(session, peer).await;
    for unknown in [false, true] {
        let dir = Dir::new();
        let broker =
            ReceiptBroker::new(OperationStore::new(&dir.0, "ws://127.0.0.1:4455").unwrap())
                .unwrap();
        let mut port = broker.scope(ScopeKind::ApplyScene).unwrap();
        let r = port.prepare_apply(&id(1), 1, 3, UTC).unwrap();
        let r = port
            .update_apply(&r, ApplyUpdate::DispatchCreations, UTC)
            .unwrap();
        assert!(
            broker
                .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
                .is_ok()
        );
        if unknown {
            port.update_apply(&r, ApplyUpdate::CreationsUnknown, UTC)
                .unwrap();
        } else {
            fs::write(dir.receipt(1), b"{}").unwrap();
        }
        assert!(
            broker
                .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
                .is_err()
        );
    }
}
#[tokio::test]
async fn invalid_stage_subjects_apply_flag_caps_foreign_session_and_locked_take_are_bounded() {
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
    let (other, _events2) = ObsSession::connect(&other_url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let control = shared_control();
    assert!(matches!(
        ApplySceneOwner::new(
            broker.scope(ScopeKind::RestoreBatch).unwrap(),
            session.handle(),
            control.clone(),
            clock()
        ),
        Err(BeginError::Binding)
    ));
    let mut owner = owner(&broker, &session, control.clone());
    let mut r = start(1, Cancellation::default());
    r.request = scene_request(false);
    assert!(matches!(owner.begin(r), Err(BeginError::InvalidRequest)));
    let mut r = start(1, Cancellation::default());
    r.creations = changes();
    assert!(matches!(owner.begin(r), Err(BeginError::InvalidEffect)));
    let mut r = start(1, Cancellation::default());
    r.creations[0] = Effect::CreateScene {
        scene_name: "OTHER".into(),
    };
    assert!(matches!(owner.begin(r), Err(BeginError::InvalidEffect)));
    let mut r = start(1, Cancellation::default());
    r.capabilities = Capabilities::from_version(&other.handle(), &json!({"availableRequests":[]}));
    assert!(matches!(owner.begin(r), Err(BeginError::Binding)));
    let mut r = start(1, Cancellation::default());
    r.capabilities = Capabilities::from_version(
        &session.handle(),
        &json!({"availableRequests":["GetStats"]}),
    );
    assert!(matches!(
        owner.begin(r),
        Err(BeginError::Unsupported("CreateScene"))
    ));
    assert!(!dir.receipt(1).exists());
    control.lock().unwrap().set_take_lock(Some(TakeLock {
        id: "take".into(),
        locked: true,
    }));
    control
        .lock()
        .unwrap()
        .claim("apply-client", 1, "", 1_793_448_000_000)
        .unwrap();
    let mut r = start(1, Cancellation::default());
    r.creations.clear();
    owner.begin(r).unwrap();
    assert!(matches!(
        owner.begin_changes(creations()),
        Err(BeginError::InvalidEffect)
    ));
    let mut wrong = changes();
    wrong[0] = Effect::SettingsOverlay {
        input_name: "OTHER".into(),
        settings: json!({}),
    };
    assert!(matches!(
        owner.begin_changes(wrong),
        Err(BeginError::InvalidEffect)
    ));
    owner.abort().unwrap();
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
    finish_session(other, other_peer).await;
}

#[tokio::test]
async fn same_holder_locked_take_permits_actual_closed_change_frame_without_recording_authority() {
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        for row in frame["d"]["requests"].as_array().unwrap() {
            assert!(
                ![
                    "StartRecord",
                    "StopRecord",
                    "ToggleRecord",
                    "SetOutputSettings",
                    "SetVideoSettings",
                    "SetCurrentProfile"
                ]
                .contains(&row["requestType"].as_str().unwrap())
            );
        }
        let rows = results(&frame, None, false);
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
        c.claim("apply-client", 1, "", 1_793_448_000_000).unwrap();
        c.set_take_lock(Some(TakeLock {
            id: "take".into(),
            locked: true,
        }));
    }
    let mut owner = owner(&broker, &session, control);
    let mut r = start(1, Cancellation::default());
    r.creations.clear();
    owner.begin(r).unwrap();
    owner.begin_changes(changes()).unwrap();
    let out = owner.finish().await.unwrap();
    assert!(out.transport.is_ok());
    assert_eq!(out.receipt.phase(), ApplyPhase::Closed);
    assert_metadata(&out.receipt);
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}
#[tokio::test]
async fn dropping_a_pending_owner_is_emergency_uncertainty_not_graceful_closure() {
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
    let mut owner = owner(&broker, &session, shared_control());
    owner.begin(start(1, Cancellation::default())).unwrap();
    {
        let mut f = Box::pin(owner.finish());
        assert!(poll!(f.as_mut()).is_pending());
        seen.await.unwrap();
    }
    drop(owner);
    assert!(broker.admission().unwrap().blocking);
    assert_eq!(
        broker.read_view().read_any(&id(1)).unwrap()["phase"],
        "dispatching-creations"
    );
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err()
    );
    finish_session(session, peer).await;
}

const ARCHIVED_V4: &str = r#"{"version":4,"operationId":"bbbbbbbb-bbbb-4bbb-8bbb-000000000001","kind":"apply-scene-workflow","endpointHash":"0000000000000000000000000000000000000000000000000000000000000000","generation":1,"createdAt":"2026-11-01T12:00:00.000Z","updatedAt":"2026-11-01T12:00:00.000Z","phase":"continuation","creations":{"executionType":0,"haltOnFailure":true,"requestCount":3,"phase":"acknowledged","requestDispatched":true,"batchAcknowledged":true,"counts":{"successful":3,"failed":0,"skipped":0},"notAttempted":null},"changes":null,"postconditionObserved":false,"renderedComposition":"unverified"}"#;
#[test]
fn exact_archived_process_fixture_is_valid_v4_known_prefix_metadata() {
    let receipt = ApplyReceipt::from_value(serde_json::from_str(ARCHIVED_V4).unwrap()).unwrap();
    assert_eq!(receipt.phase(), ApplyPhase::Continuation);
    assert_eq!(
        receipt.creation_counts(),
        Some(ApplyCounts {
            successful: 3,
            failed: 0,
            skipped: 0
        })
    );
    assert_metadata(&receipt);
}
#[tokio::test]
async fn original_capability_list_and_already_cancelled_or_expired_admission_do_not_gain_a_frame() {
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        let rows = results(&frame, None, true);
        batch_reply(&mut peer, &frame, rows).await;
        closed(&mut peer).await;
    })
    .await;
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let mut owner = owner(&broker, &session, shared_control());
    let cancel = Cancellation::default();
    cancel.cancel();
    assert!(matches!(
        owner.begin(start(1, cancel)),
        Err(BeginError::Cancelled)
    ));
    let mut r = start(1, Cancellation::default());
    r.timeout = Duration::ZERO;
    assert!(matches!(owner.begin(r), Err(BeginError::Deadline)));
    let mut r = start(1, Cancellation::default());
    r.admitted_at = Instant::now() + Duration::from_secs(1);
    assert!(matches!(owner.begin(r), Err(BeginError::Deadline)));
    assert!(!dir.receipt(1).exists());
    let mut r = start(1, Cancellation::default());
    r.capabilities = Capabilities::from_version(
        &session.handle(),
        &json!({"availableRequests":["CreateScene","CreateInput","CreateSceneItem"]}),
    );
    owner.begin(r).unwrap();
    owner.finish().await.unwrap();
    assert!(matches!(
        owner.begin_changes(changes()),
        Err(BeginError::Unsupported("SetInputSettings"))
    ));
    owner.abort().unwrap();
    drop(owner.into_store().ok().unwrap());
    finish_session(session, peer).await;
}

#[tokio::test]
#[ignore = "owned subprocess helper, invoked by actual process-kill regression"]
async fn apply_batch_continuation_child() {
    let Some(root) = std::env::var_os("APPLY_BATCH_CHILD_ROOT") else {
        return;
    };
    let url = std::env::var("APPLY_BATCH_CHILD_URL").unwrap();
    let (session, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let broker =
        ReceiptBroker::new(OperationStore::new(PathBuf::from(&root), &url).unwrap()).unwrap();
    let mut owner = owner(&broker, &session, shared_control());
    owner.begin(start(1, Cancellation::default())).unwrap();
    let out = owner.finish().await.unwrap();
    assert_eq!(out.receipt.phase(), ApplyPhase::Continuation);
    fs::write(PathBuf::from(root).join("child-ready"), b"continuation").unwrap();
    let retained = (owner, session, broker);
    std::future::pending::<()>().await;
    drop(retained);
}
#[tokio::test]
async fn actual_process_kill_reopens_known_prefix_without_continuation_or_replay() {
    struct Child(Option<std::process::Child>);
    impl Drop for Child {
        fn drop(&mut self) {
            if let Some(mut child) = self.0.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        let rows = results(&frame, None, true);
        batch_reply(&mut peer, &frame, rows).await;
        closed(&mut peer).await;
    })
    .await;
    let dir = Dir::new();
    let mut child = Child(Some(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "apply_batch_continuation_child",
                "--ignored",
                "--nocapture",
            ])
            .env("APPLY_BATCH_CHILD_ROOT", &dir.0)
            .env("APPLY_BATCH_CHILD_URL", &url)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap(),
    ));
    timeout(WAIT, async {
        while !dir.0.join("child-ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut process = child.0.take().unwrap();
    process.kill().unwrap();
    let output = process.wait_with_output().unwrap();
    assert!(!output.status.success());
    timeout(WAIT, peer).await.unwrap().unwrap();
    let reopened = ReceiptBroker::new(OperationStore::new(&dir.0, &url).unwrap()).unwrap();
    let receipt = reopened.read_view().read_any(&id(1)).unwrap();
    assert_eq!(receipt["phase"], "continuation");
    assert_eq!(
        receipt["creations"]["counts"],
        json!({"successful":3,"failed":0,"skipped":0})
    );
    assert_eq!(receipt["changes"], Value::Null);
    assert!(reopened.admission().unwrap().blocking);
    assert!(reopened.scope(ScopeKind::ApplyScene).is_err());
    assert!(
        reopened
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err()
    );
}
