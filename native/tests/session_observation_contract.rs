// SPDX-License-Identifier: GPL-2.0-only
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    observation::{Observation, ReadAdapter, ReadRequest},
    session::{ObsSession, SessionOptions},
    session_observation::{ObservationBridge, REQUIRED_AUDIT_EVENTS},
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

type Peer = WebSocketStream<TcpStream>;

#[tokio::test]
async fn a_narrow_profile_still_publishes_every_event_watermark() {
    let (url, count, peer) = server(true, 1).await;
    let (owner, events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let bridge =
        ObservationBridge::bind_with_event_filter(owner.handle(), events, None, |_| false).unwrap();
    assert!(!bridge.fence().full_profile());
    let mut reader = bridge.reader();
    assert_eq!(reader.token(), owner.token());
    assert_eq!(reader.endpoint_hash(), owner.handle().endpoint_hash());
    let value = reader
        .read(
            ReadRequest::SceneList,
            json!({}),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(value, json!({"scenes":[]}));
    assert_eq!(bridge.evidence().borrow().revision, 0);
    assert_eq!(owner.handle().event_cursor(), 1);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    // synchronize must complete without asking OBS to emit another event/RPC.
    bridge
        .synchronize(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    reader
        .synchronize(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(reader.evidence().revision, 0);
    assert!(reader.evidence().connected);
    assert_eq!(
        reader.synchronize(Instant::now()).await.unwrap_err().reason,
        "observation_limit"
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn only_explicit_obs_rejections_carry_request_codes() {
    use obs_mcp_native::observation::ObservationError;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut peer = accept_async(stream).await.unwrap();
        send(
            &mut peer,
            json!({"op":0,"d":{"obsWebSocketVersion":"5.7.4","rpcVersion":1}}),
        )
        .await;
        assert_eq!(receive(&mut peer).await["op"], 1);
        send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
        for code in [604, 500] {
            let request = receive(&mut peer).await;
            assert_eq!(request["d"]["requestType"], "GetInputMute");
            send(&mut peer, json!({"op":5,"d":{"eventType":"InputSettingsChanged","eventIntent":8,"eventData":{"inputName":"fixture"}}})).await;
            send(
                &mut peer,
                json!({"op":7,"d":{
                    "requestId":request["d"]["requestId"],"requestType":"GetInputMute",
                    "requestStatus":{"result":false,"code":code,"comment":"PRIVATE PEER COMMENT"}
                }}),
            )
            .await;
        }
        assert!(matches!(
            timeout(Duration::from_secs(2), peer.next()).await.unwrap(),
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None
        ));
    });
    let (owner, events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
    let mut reader = bridge.reader();
    for (index, code) in [604, 500].into_iter().enumerate() {
        let error = reader
            .read(
                ReadRequest::InputMute,
                json!({"inputName":"fixture"}),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        assert_eq!(error.request_code(), Some(code));
        assert_eq!(
            error.reason,
            if code == 604 {
                "capability_unavailable"
            } else {
                "evidence_unavailable"
            }
        );
        assert!(!format!("{error:?}").contains("PRIVATE PEER COMMENT"));
        assert!(!error.to_string().contains("PRIVATE PEER COMMENT"));
        assert_eq!(
            error.message,
            format!("OBS request GetInputMute failed with code {code}: PRIVATE PEER COMMENT")
        );
        assert!(error.failure().get("requestCode").is_none());
        assert_eq!(bridge.evidence().borrow().revision, index as u64 + 1);
    }
    let absent = ObservationError::new("capability_unavailable", "Not advertised");
    assert_eq!(absent.request_code(), None);
    owner.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn publication_barrier_waits_for_an_unpolled_prior_event_without_another_rpc() {
    let (url, count, peer) = server(true, 1).await;
    let (owner, events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    handle
        .request(
            owner.token(),
            "GetSceneList",
            json!({}),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(handle.event_cursor(), 1);
    // No event consumer has been polled yet. The freshly spawned bridge cannot
    // run on this current-thread runtime during this first explicit poll.
    let bridge = ObservationBridge::bind(handle, events, None).unwrap();
    assert!(
        !bridge.ready_at_revision(owner.token().generation(), 0),
        "an unpolled event cannot authorize a queued mutation"
    );
    let state = {
        let ready = bridge.synchronize(Instant::now() + Duration::from_secs(2));
        tokio::pin!(ready);
        assert!(futures_util::poll!(ready.as_mut()).is_pending());
        ready.await.unwrap()
    };
    assert_eq!(state.borrow().revision, 1);
    assert!(bridge.ready_at_revision(owner.token().generation(), 1));
    assert!(!bridge.ready_at_revision(owner.token().generation(), 0));
    assert!(!bridge.ready_at_revision(0, 1));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    assert!(!bridge.ready_at_revision(state.borrow().generation, 1));
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}

async fn send(peer: &mut Peer, data: Value) {
    peer.send(Message::Text(data.to_string().into()))
        .await
        .unwrap();
}
async fn receive(peer: &mut Peer) -> Value {
    let frame = timeout(Duration::from_secs(2), peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(frame.to_text().unwrap()).unwrap()
}

async fn server(
    event_before_reply: bool,
    expected_requests: usize,
) -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let received = count.clone();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut peer = accept_async(stream).await.unwrap();
        send(
            &mut peer,
            json!({"op":0,"d":{"obsWebSocketVersion":"5.7.4","rpcVersion":1}}),
        )
        .await;
        assert_eq!(receive(&mut peer).await["op"], 1);
        send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
        for _ in 0..expected_requests {
            let request = receive(&mut peer).await;
            assert_eq!(request["d"]["requestType"], "GetSceneList");
            received.fetch_add(1, Ordering::SeqCst);
            if event_before_reply {
                send(&mut peer, json!({"op":5,"d":{"eventType":"InputCreated","eventIntent":8,"eventData":{"inputName":"changed"}}})).await;
            }
            send(
                &mut peer,
                json!({"op":7,"d":{
                    "requestId":request["d"]["requestId"],"requestType":"GetSceneList",
                    "requestStatus":{"result":true,"code":100},"responseData":{"scenes":[]}
                }}),
            )
            .await;
        }
        match timeout(Duration::from_secs(2), peer.next()).await.unwrap() {
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {}
            other => panic!("unexpected request after evidence admission ended: {other:?}"),
        }
    });
    (url, count, task)
}

#[tokio::test]
async fn queued_pre_reply_event_fences_real_rpc_evidence_without_extra_reads() {
    let (url, count, peer) = server(true, 1).await;
    let (owner, events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
    let mut reader = bridge.reader();
    let mut sample = Observation::new(
        &mut reader,
        bridge.evidence(),
        owner.token().generation(),
        "2026-10-31T00:00:00.000Z".into(),
        Instant::now() + Duration::from_secs(2),
    )
    .unwrap();
    let failure = sample
        .read(ReadRequest::SceneList, json!({}))
        .await
        .unwrap_err();
    assert_eq!(failure.reason, "observation_invalidated");
    assert!(
        sample
            .read(ReadRequest::SceneList, json!({}))
            .await
            .is_err()
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(owner.handle().event_cursor(), 1);
    owner.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn stable_real_reads_and_advertised_capabilities_preserve_one_socket() {
    let (url, count, peer) = server(false, 1).await;
    let (owner, events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let bridge = ObservationBridge::bind(
        owner.handle(),
        events,
        Some(HashSet::from(["GetSceneList".into()])),
    )
    .unwrap();
    let mut reader = bridge.reader();
    assert_eq!(reader.supports(ReadRequest::SceneList), Some(true));
    assert_eq!(reader.supports(ReadRequest::InputList), Some(false));
    let mut sample = Observation::new(
        &mut reader,
        bridge.evidence(),
        owner.token().generation(),
        "2026-10-31T00:00:00.000Z".into(),
        Instant::now() + Duration::from_secs(2),
    )
    .unwrap();
    assert_eq!(
        sample
            .read(ReadRequest::SceneList, json!({}))
            .await
            .unwrap(),
        json!({"scenes":[]})
    );
    assert_eq!(
        sample
            .read(ReadRequest::InputList, json!({}))
            .await
            .unwrap_err()
            .reason,
        "capability_unavailable"
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn dropping_observation_owner_ends_admission_for_retained_readers() {
    let (url, count, peer) = server(false, 0).await;
    let (owner, events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
    let mut reader = bridge.reader();
    let fence = bridge.fence();
    assert!(fence.full_profile());
    let cloned = fence.clone();
    assert_eq!(fence.token(), owner.token());
    assert_eq!(fence.endpoint_hash(), owner.handle().endpoint_hash());
    assert!(fence.ready_at_revision(owner.token().generation(), 0));
    drop(bridge);
    assert!(!fence.ready_at_revision(owner.token().generation(), 0));
    assert!(!cloned.ready_at_revision(owner.token().generation(), 0));
    assert!(!reader.evidence().connected);
    assert_eq!(
        reader
            .synchronize(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err()
            .reason,
        "observation_invalidated"
    );
    assert_eq!(
        reader
            .read(
                ReadRequest::SceneList,
                json!({}),
                Instant::now() + Duration::from_secs(1)
            )
            .await
            .unwrap_err()
            .reason,
        "observation_invalidated"
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    owner.shutdown().await.unwrap();
    peer.await.unwrap();
}

/// One accepted physical socket, exactly one caller RPC per scheduled event.
async fn profile_server(
    schedule: Vec<(&'static str, bool)>,
) -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut peer = accept_async(stream).await.unwrap();
        send(
            &mut peer,
            json!({"op":0,"d":{"obsWebSocketVersion":"5.7.4","rpcVersion":1}}),
        )
        .await;
        assert_eq!(receive(&mut peer).await["op"], 1);
        send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
        for (name, reject) in schedule {
            let request = receive(&mut peer).await;
            assert_eq!(request["d"]["requestType"], "GetSceneList");
            observed.fetch_add(1, Ordering::SeqCst);
            send(&mut peer,json!({"op":5,"d":{"eventType":name,"eventIntent":if name=="SceneItemTransformChanged"{1<<19}else if name=="InputVolumeMeters"{1<<16}else{8},"eventData":{"fixture":true}}})).await;
            let _=peer.send(Message::Text(json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":"GetSceneList","requestStatus":{"result":!reject,"code":if reject{604}else{100},"comment":"PRIVATE AUDIT REJECTION"},"responseData":{"scenes":[]}}}).to_string().into())).await;
        }
        match timeout(Duration::from_secs(2), peer.next()).await.unwrap() {
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {}
            other => panic!("unexpected extra audit RPC {other:?}"),
        }
    });
    (url, count, task)
}

#[tokio::test]
async fn fixed_audit_profile_has_source_exact_revisions_on_the_same_physical_reads() {
    let schedule = vec![
        ("RecordStateChanged", false),
        ("SceneItemTransformChanged", false),
        ("InputVolumeMeters", false),
        ("InputMuteStateChanged", false),
        ("InputSettingsChanged", false),
        ("CanvasCreated", false),
        ("CurrentProgramSceneChanged", false),
        ("SceneNameChanged", false),
    ];
    let (url, count, peer) = profile_server(schedule).await;
    let (owner, events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            event_subscriptions: REQUIRED_AUDIT_EVENTS | (1 << 16),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bridge = ObservationBridge::bind(
        owner.handle(),
        events,
        Some(HashSet::from(["GetSceneList".into()])),
    )
    .unwrap();
    let mut audit = bridge.audit_reader().unwrap();
    let canonical = bridge.reader();
    let audit_fence = bridge.audit_fence().unwrap();
    assert!(!audit_fence.full_profile());
    assert!(bridge.fence().full_profile());
    assert_eq!(audit.token(), owner.token());
    assert_eq!(audit.endpoint_hash(), owner.handle().endpoint_hash());
    assert_eq!(audit.supports(ReadRequest::InputList), Some(false));
    for (index, (expected_canonical, expected_audit)) in [
        (1, 0),
        (2, 1),
        (2, 1),
        (3, 1),
        (4, 2),
        (5, 3),
        (6, 3),
        (7, 4),
    ]
    .into_iter()
    .enumerate()
    {
        audit
            .read(
                ReadRequest::SceneList,
                json!({}),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(canonical.evidence().revision, expected_canonical);
        assert_eq!(audit.evidence().revision, expected_audit);
        assert_eq!(bridge.audit_evidence().borrow().revision, expected_audit);
        assert!(audit_fence.ready_at_revision(owner.token().generation(), expected_audit));
        assert_eq!(owner.handle().event_cursor(), index as u64 + 1);
    }
    assert_eq!(count.load(Ordering::SeqCst), 8);
    owner.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn audit_rejected_reply_cannot_outrun_its_pre_reply_structural_event() {
    let (url, count, peer) = profile_server(vec![("SceneItemTransformChanged", true)]).await;
    let (owner, events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            event_subscriptions: REQUIRED_AUDIT_EVENTS,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
    let mut reader = bridge.audit_reader().unwrap();
    let error = reader
        .read(
            ReadRequest::SceneList,
            json!({}),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert_eq!(error.request_code(), Some(604));
    assert_eq!(
        error.message,
        "OBS request GetSceneList failed with code 604: PRIVATE AUDIT REJECTION"
    );
    assert!(!format!("{error:?}").contains("PRIVATE AUDIT"));
    assert_eq!(reader.evidence().revision, 1);
    assert_eq!(bridge.evidence().borrow().revision, 1);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn unpolled_irrelevant_event_advances_both_barriers_without_another_rpc() {
    let (url, count, peer) = profile_server(vec![("RecordStateChanged", false)]).await;
    let (owner, events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            event_subscriptions: REQUIRED_AUDIT_EVENTS,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    owner
        .handle()
        .request(
            owner.token(),
            "GetSceneList",
            json!({}),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(owner.handle().event_cursor(), 1);
    let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
    let mut audit = bridge.audit_reader().unwrap();
    let fence = bridge.audit_fence().unwrap();
    assert!(!fence.ready_at_revision(owner.token().generation(), 0));
    {
        let pending = audit.synchronize(Instant::now() + Duration::from_secs(2));
        tokio::pin!(pending);
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        pending.await.unwrap();
    }
    assert_eq!(audit.evidence().revision, 0);
    assert_eq!(bridge.evidence().borrow().revision, 1);
    assert!(fence.ready_at_revision(owner.token().generation(), 0));
    assert!(bridge.ready_at_revision(owner.token().generation(), 1));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn audit_admission_refuses_incomplete_or_custom_profiles_and_drop_revokes_both() {
    for narrow in [false, true] {
        let (url, count, peer) = server(false, 0).await;
        let (owner, events) = ObsSession::connect(
            &url,
            None,
            SessionOptions {
                event_subscriptions: if narrow {
                    REQUIRED_AUDIT_EVENTS
                } else {
                    obs_mcp_native::session::ALL_EVENTS
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let bridge = if narrow {
            ObservationBridge::bind_with_event_filter(owner.handle(), events, None, |_| false)
        } else {
            ObservationBridge::bind(owner.handle(), events, None)
        }
        .unwrap();
        assert!(bridge.audit_reader().is_err());
        assert!(bridge.audit_fence().is_err());
        assert_eq!(count.load(Ordering::SeqCst), 0);
        owner.shutdown().await.unwrap();
        bridge.shutdown().await.unwrap();
        peer.await.unwrap();
    }
    for mode in ["drop", "invalidate", "cancel", "shutdown"] {
        let (url, count, peer) = server(false, 0).await;
        let (owner, events) = ObsSession::connect(
            &url,
            None,
            SessionOptions {
                event_subscriptions: REQUIRED_AUDIT_EVENTS,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
        let mut audit = bridge.audit_reader().unwrap();
        let mut canonical = bridge.reader();
        let audit_state = bridge.audit_evidence();
        let canonical_state = bridge.evidence();
        let fence = bridge.audit_fence().unwrap();
        let mut retain = Some(bridge);
        match mode {
            "drop" => drop(retain.take()),
            "invalidate" => retain.as_ref().unwrap().invalidate(),
            "cancel" => retain.as_ref().unwrap().cancel_observations(),
            "shutdown" => retain.take().unwrap().shutdown().await.unwrap(),
            _ => unreachable!(),
        }
        assert!(!fence.ready_at_revision(owner.token().generation(), 0));
        if mode == "cancel" {
            assert!(audit_state.borrow().cancelled);
            assert!(canonical_state.borrow().cancelled);
        } else {
            assert!(!audit_state.borrow().connected);
            assert!(!canonical_state.borrow().connected);
        }
        if mode != "cancel" {
            assert!(
                audit
                    .read(
                        ReadRequest::SceneList,
                        json!({}),
                        Instant::now() + Duration::from_secs(1)
                    )
                    .await
                    .is_err()
            );
            assert!(
                canonical
                    .read(
                        ReadRequest::SceneList,
                        json!({}),
                        Instant::now() + Duration::from_secs(1)
                    )
                    .await
                    .is_err()
            );
        }
        assert_eq!(count.load(Ordering::SeqCst), 0);
        owner.shutdown().await.unwrap();
        if let Some(bridge) = retain {
            bridge.shutdown().await.unwrap();
        }
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn held_original_take_frame_exhausts_socket_permits_and_revokes_both_profiles() {
    let (url, count, peer) = profile_server(vec![
        ("RecordStateChanged", false),
        ("SceneItemTransformChanged", false),
    ])
    .await;
    let (owner, events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            event_items: 1,
            event_subscriptions: REQUIRED_AUDIT_EVENTS | (1 << 16),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
    let mut feed = bridge.attach_take_feed().unwrap();
    assert!(bridge.attach_take_feed().is_err());
    let mut receiver = feed.take_receiver().unwrap();
    let mut audit = bridge.audit_reader().unwrap();
    let mut canonical = bridge.reader();
    let fence = bridge.audit_fence().unwrap();
    audit
        .read(
            ReadRequest::SceneList,
            json!({}),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    let held = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(held, obs_mcp_native::takes::Event::Observed(_)));
    assert_eq!(audit.evidence().revision, 0);
    assert_eq!(canonical.evidence().revision, 1);
    assert!(
        audit
            .read(
                ReadRequest::SceneList,
                json!({}),
                Instant::now() + Duration::from_secs(2)
            )
            .await
            .is_err()
    );
    assert!(!fence.ready_at_revision(owner.token().generation(), 0));
    assert!(
        canonical
            .read(
                ReadRequest::SceneList,
                json!({}),
                Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    assert_eq!(
        owner.handle().invalidation(),
        Some(obs_mcp_native::session::Invalidation::EventOverflow)
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
    drop(held);
    drop(feed);
    let _ = owner.shutdown().await;
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn producer_close_revokes_both_profiles_even_with_retained_reader_clones() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut peer = accept_async(stream).await.unwrap();
        send(
            &mut peer,
            json!({"op":0,"d":{"obsWebSocketVersion":"5.7.4","rpcVersion":1}}),
        )
        .await;
        assert_eq!(receive(&mut peer).await["op"], 1);
        send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
        assert_eq!(receive(&mut peer).await["d"]["requestType"], "GetSceneList");
        peer.close(None).await.unwrap();
    });
    let (owner, events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            event_subscriptions: REQUIRED_AUDIT_EVENTS,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
    let mut audit = bridge.audit_reader().unwrap();
    let mut detached_audit = audit.clone();
    let mut detached_canonical = bridge.reader();
    let fence = bridge.audit_fence().unwrap();
    let mut audit_state = bridge.audit_evidence();
    let mut canonical_state = bridge.evidence();
    assert!(
        audit
            .read(
                ReadRequest::SceneList,
                json!({}),
                Instant::now() + Duration::from_secs(2)
            )
            .await
            .is_err()
    );
    assert!(!fence.ready_at_revision(owner.token().generation(), 0));
    timeout(Duration::from_secs(2), async {
        while audit_state.borrow_and_update().connected {
            audit_state.changed().await.unwrap();
        }
        while canonical_state.borrow_and_update().connected {
            canonical_state.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(
        detached_canonical
            .synchronize(Instant::now() + Duration::from_secs(1))
            .await
            .is_err()
    );
    assert!(
        detached_audit
            .synchronize(Instant::now() + Duration::from_secs(1))
            .await
            .is_err()
    );
    let _ = owner.shutdown().await;
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn later_publication_cannot_restore_canceled_profile_permissions() {
    let (url, count, peer) = profile_server(vec![("SceneItemTransformChanged", false)]).await;
    let (owner, events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            event_subscriptions: REQUIRED_AUDIT_EVENTS,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
    let mut audit = bridge.audit_reader().unwrap();
    let mut canonical = bridge.reader();
    let audit_state = bridge.audit_evidence();
    let canonical_state = bridge.evidence();
    bridge.cancel_observations();
    assert!(
        audit
            .read(
                ReadRequest::SceneList,
                json!({}),
                Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    assert!(
        canonical
            .read(
                ReadRequest::SceneList,
                json!({}),
                Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    // The policy-free transport still receives an event after cancellation;
    // publishing it must preserve both revocations rather than replace them.
    owner
        .handle()
        .request(
            owner.token(),
            "GetSceneList",
            json!({}),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            if audit_state.borrow().revision == 1 && canonical_state.borrow().revision == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(audit_state.borrow().cancelled);
    assert!(canonical_state.borrow().cancelled);
    assert!(
        audit
            .synchronize(Instant::now() + Duration::from_secs(1))
            .await
            .is_err()
    );
    assert!(
        canonical
            .synchronize(Instant::now() + Duration::from_secs(1))
            .await
            .is_err()
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    peer.await.unwrap();
}
