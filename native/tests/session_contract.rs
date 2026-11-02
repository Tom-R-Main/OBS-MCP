// SPDX-License-Identifier: GPL-2.0-only
//! Consumer tests exercise real loopback WebSockets. No MCP routes or live OBS.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::session::{
    ALL_EVENTS, BatchOptions, BatchRequest, BeforeDispatch, ConnectError, Execution, Invalidation,
    ObsSession, RequestError, SUPPORTED_EVENT_SUBSCRIPTIONS, SessionHandle, SessionOptions,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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

type Peer = WebSocketStream<TcpStream>;
const WAIT: Duration = Duration::from_secs(2);
const RPC: Duration = Duration::from_secs(5);

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
            other => panic!("expected request, received {other:?}"),
        }
    }
}

async fn reply(peer: &mut Peer, request: &Value, success: bool, code: u64) {
    send(peer, json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":request["d"]["requestType"],"requestStatus":{"result":success,"code":code,"comment":"fixture-private-comment"},"responseData":{"ok":true}}})).await;
}

async fn event(peer: &mut Peer, sequence: u64) {
    send(peer, json!({"op":5,"d":{"eventType":"RecordStateChanged","eventIntent":64,"eventData":{"sequence":sequence}}})).await;
}

async fn closed(peer: &mut Peer) {
    while let Some(next) = timeout(WAIT, peer.next()).await.unwrap() {
        match next.unwrap() {
            Message::Close(_) => {
                let _ = peer.flush().await;
                return;
            }
            Message::Ping(_) => {
                let _ = peer.flush().await;
            }
            other => panic!("unexpected frame while awaiting close: {other:?}"),
        }
    }
}

async fn dropped_socket(peer: &mut Peer) {
    let end = timeout(WAIT, peer.next()).await.unwrap();
    let tcp_reset = matches!(&end, Some(Err(tokio_tungstenite::tungstenite::Error::Io(error)))
        if matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::UnexpectedEof));
    assert!(
        tcp_reset
            || matches!(
        &end,
        None | Some(Ok(Message::Close(_)))
            | Some(Err(tokio_tungstenite::tungstenite::Error::Protocol(
                tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake
            )))
    ),
        "socket was not closed: {end:?}"
    );
}

async fn unidentified_peer() -> (String, oneshot::Receiver<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (ready, seen) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (socket, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
        let mut peer = accept_async(socket).await.unwrap();
        ready.send(()).unwrap();
        // Deliberately never send Hello. A cancelled/timed-out connect must
        // destroy this socket without creating a detached session owner.
        dropped_socket(&mut peer).await;
    });
    (url, seen, task)
}

async fn fixture<F, Fut>(auth: bool, script: F) -> (String, JoinHandle<()>)
where
    F: FnOnce(Peer) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (socket, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
        let mut peer = accept_async(socket).await.unwrap();
        let mut hello = json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}});
        if auth {
            hello["d"]["authentication"] =
                json!({"salt":"fixture-salt","challenge":"fixture-challenge"});
        }
        send(&mut peer, hello).await;
        let identify = receive(&mut peer).await;
        assert_eq!(identify["op"], 1);
        assert_eq!(identify["d"]["rpcVersion"], 1);
        assert_eq!(identify["d"]["eventSubscriptions"], ALL_EVENTS);
        if auth {
            let secret = STANDARD.encode(Sha256::digest("fixture-passwordfixture-salt"));
            let answer = STANDARD.encode(Sha256::digest(format!("{secret}fixture-challenge")));
            assert_eq!(identify["d"]["authentication"], answer);
        }
        send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
        script(peer).await;
    });
    (url, task)
}

async fn joined(task: JoinHandle<()>) {
    timeout(WAIT, task).await.unwrap().unwrap();
}

async fn invalidated(handle: &SessionHandle) -> Invalidation {
    timeout(WAIT, async {
        loop {
            if let Some(reason) = handle.invalidation() {
                break reason;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn authenticates_and_pumps_ordered_events_idle_and_during_rpc() {
    let (url, peer) = fixture(true, |mut peer| async move {
        event(&mut peer, 1).await;
        let request = receive(&mut peer).await;
        assert_eq!(request["d"]["requestType"], "SetInputMute");
        event(&mut peer, 2).await;
        reply(&mut peer, &request, true, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, mut events) =
        ObsSession::connect(&url, Some("fixture-password"), SessionOptions::default())
            .await
            .unwrap();
    let handle = owner.handle();
    let diagnostics = format!("{owner:?} {handle:?}");
    assert!(!diagnostics.contains(&url));
    assert!(!diagnostics.contains("fixture-password"));
    let first = timeout(WAIT, events.next()).await.unwrap().unwrap();
    assert_eq!(first.connection, owner.token());
    assert_eq!(first.sequence, 1);
    assert_eq!(first.event_type, "RecordStateChanged");
    assert_eq!(first.intent, 64);
    assert_eq!(first.data, Some(json!({"sequence":1})));
    assert!(first.encoded_bytes > 0);
    assert_eq!(
        handle
            .request(
                owner.token(),
                "SetInputMute",
                json!({"inputName":"Fixture","inputMuted":true}),
                RPC
            )
            .await
            .unwrap(),
        json!({"ok":true})
    );
    let second = events.next().await.unwrap();
    assert_eq!(second.sequence, 2);
    assert_eq!(second.data, Some(json!({"sequence":2})));
    owner.shutdown().await.unwrap();
    assert_eq!(events.next().await.unwrap_err(), Invalidation::Shutdown);
    joined(peer).await;
}

#[tokio::test]
async fn rpc_completion_covers_earlier_queued_events_before_receiver_polling() {
    let (url, peer) = fixture(false, |mut peer| async move {
        event(&mut peer, 1).await;
        let request = receive(&mut peer).await;
        reply(&mut peer, &request, true, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    handle
        .request(owner.token(), "GetObservation", json!({}), RPC)
        .await
        .unwrap();
    // No receiver poll happened before the RPC response. A separate bridge may
    // still be behind, but the transport already exposes its exact watermark.
    let cursor = handle.event_cursor();
    assert_eq!(cursor, 1);
    assert_eq!(handle.clone().event_cursor(), cursor);
    let event = events.next().await.unwrap();
    assert_eq!((event.connection, event.sequence), (owner.token(), cursor));
    owner.shutdown().await.unwrap();
    assert_eq!(handle.event_cursor(), cursor);
    assert_eq!(events.next().await.unwrap_err(), Invalidation::Shutdown);
    joined(peer).await;
}

#[tokio::test]
async fn rejected_comment_is_bounded_and_private_without_truncating_the_tool_body() {
    for length in [65_536, 65_537] {
        let comment = format!("PRIVATE:{}", "x".repeat(length - 8));
        let wire_comment = comment.clone();
        let (url, peer) = fixture(false, move |mut peer| async move {
            let request = receive(&mut peer).await;
            send(
                &mut peer,
                json!({"op":7,"d":{
                    "requestId":request["d"]["requestId"],
                    "requestType":request["d"]["requestType"],
                    "requestStatus":{"result":false,"code":500,"comment":wire_comment}
                }}),
            )
            .await;
            if length == 65_536 {
                closed(&mut peer).await;
            } else {
                dropped_socket(&mut peer).await;
            }
        })
        .await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let result = owner
            .handle()
            .request(owner.token(), "GetInputMute", json!({}), RPC)
            .await
            .unwrap_err();
        assert!(!format!("{result:?} {result}").contains("PRIVATE:"));
        if length == 65_536 {
            assert_eq!(result.rejection_code(), Some(500));
            assert_eq!(
                result.obs_failure_message("GetInputMute"),
                Some(format!(
                    "OBS request GetInputMute failed with code 500: {comment}"
                ))
            );
        } else {
            assert_eq!(result.rejection_code(), None);
            assert_eq!(
                result,
                RequestError::Unknown(Invalidation::MalformedMessage)
            );
        }
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
}

#[tokio::test]
async fn six_mib_image_reply_fits_its_envelope_without_widening_request_or_event_budgets() {
    const IMAGE_BYTES: usize = 6 * 1024 * 1024;
    let image = format!(
        "data:image/png;base64,{}",
        STANDARD.encode(vec![0u8; IMAGE_BYTES])
    );
    let expected_len = image.len();
    let (url, peer) = fixture(false, move |mut peer| async move {
        let request = receive(&mut peer).await;
        let response = json!({"op":7,"d":{
            "requestId":request["d"]["requestId"],"requestType":"GetSourceScreenshot",
            "requestStatus":{"result":true,"code":100},"responseData":{"imageData":image}
        }});
        if peer
            .send(Message::Text(response.to_string().into()))
            .await
            .is_ok()
        {
            // The old receive cap closes during a legal-size reply. Either
            // close path is joined before the consumer asserts its outcome.
            let _ = timeout(RPC, peer.next()).await;
            let _ = peer.flush().await;
        }
    })
    .await;
    let (owner, _events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            request_bytes: 256,
            event_bytes: 256,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let response = handle
        .request(owner.token(), "GetSourceScreenshot", json!({}), RPC)
        .await;
    let too_large_request = handle
        .request(
            owner.token(),
            "GetStats",
            json!({"padding":"x".repeat(512)}),
            RPC,
        )
        .await;
    owner.shutdown().await.unwrap();
    joined(peer).await;
    assert!(
        response.is_ok(),
        "valid 6MiB image was rejected after base64/envelope expansion: {:?}",
        response.as_ref().err()
    );
    assert_eq!(
        response.unwrap()["imageData"].as_str().unwrap().len(),
        expected_len
    );
    assert_eq!(
        too_large_request,
        Err(RequestError::BeforeDispatch(BeforeDispatch::ByteCapacity))
    );
}

#[tokio::test]
async fn tokens_are_process_unique_and_stale_admission_has_no_write_or_guard() {
    let (url_a, peer_a) = fixture(false, |mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (url_b, peer_b) = fixture(false, |mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (owner_a, _events_a) = ObsSession::connect(&url_a, None, SessionOptions::default())
        .await
        .unwrap();
    let (owner_b, _events_b) = ObsSession::connect(&url_b, None, SessionOptions::default())
        .await
        .unwrap();
    assert_ne!(owner_a.token(), owner_b.token());
    let calls = Arc::new(AtomicUsize::new(0));
    let guard_calls = calls.clone();
    let result = owner_b
        .handle()
        .request_checked(owner_a.token(), "SetInputMute", json!({}), RPC, move || {
            guard_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;
    assert_eq!(
        result,
        Err(RequestError::BeforeDispatch(
            BeforeDispatch::StaleConnection
        ))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    owner_a.shutdown().await.unwrap();
    owner_b.shutdown().await.unwrap();
    joined(peer_a).await;
    joined(peer_b).await;
}

#[tokio::test]
async fn abandoned_queued_work_never_dispatches_or_runs_its_guard() {
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        let first = receive(&mut peer).await;
        assert_eq!(first["d"]["requestType"], "SetFirst");
        seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        reply(&mut peer, &first, true, 100).await;
        let third = receive(&mut peer).await;
        assert_eq!(third["d"]["requestType"], "SetThird");
        assert_ne!(first["d"]["requestId"], third["d"]["requestId"]);
        reply(&mut peer, &third, true, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut first = Box::pin(handle.request(owner.token(), "SetFirst", json!({}), RPC));
    assert!(poll!(first.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let guard_calls = Arc::new(AtomicUsize::new(0));
    {
        let calls = guard_calls.clone();
        let mut cancelled = Box::pin(handle.request_checked(
            owner.token(),
            "SetCancelled",
            json!({}),
            RPC,
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        assert!(poll!(cancelled.as_mut()).is_pending());
        // Drop the actual owned future while it is admitted behind the first.
    }
    let mut third = Box::pin(handle.request(owner.token(), "SetThird", json!({}), RPC));
    assert!(poll!(third.as_mut()).is_pending());
    release_tx.send(()).unwrap();
    first.await.unwrap();
    third.await.unwrap();
    assert_eq!(guard_calls.load(Ordering::SeqCst), 0);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn queue_deadline_returns_before_active_work_finishes_and_cannot_dispatch() {
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        let first = receive(&mut peer).await;
        seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        reply(&mut peer, &first, true, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut first = Box::pin(handle.request(owner.token(), "SetFirst", json!({}), RPC));
    assert!(poll!(first.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let queued = timeout(
        Duration::from_secs(1),
        handle.request(
            owner.token(),
            "SetExpired",
            json!({}),
            Duration::from_millis(20),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        queued,
        Err(RequestError::BeforeDispatch(BeforeDispatch::Deadline))
    );
    release_tx.send(()).unwrap();
    first.await.unwrap();
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn final_guard_rechecks_authority_after_queue_wait_before_any_send() {
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        let first = receive(&mut peer).await;
        seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        reply(&mut peer, &first, true, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let valid = Arc::new(AtomicBool::new(true));
    let guard_valid = valid.clone();
    let mut first = Box::pin(handle.request(owner.token(), "SetFirst", json!({}), RPC));
    assert!(poll!(first.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let mut queued =
        Box::pin(
            handle.request_checked(owner.token(), "SetGuarded", json!({}), RPC, move || {
                if guard_valid.load(Ordering::Acquire) {
                    Ok(())
                } else {
                    Err("lease expired".into())
                }
            }),
        );
    assert!(poll!(queued.as_mut()).is_pending());
    valid.store(false, Ordering::Release);
    release_tx.send(()).unwrap();
    first.await.unwrap();
    assert_eq!(
        queued.await,
        Err(RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(
            "lease expired".into()
        )))
    );
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn lost_reply_is_one_dispatch_unknown_outcome_and_a_retired_connection() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = seen.clone();
    let (url, peer) = fixture(false, move |mut peer| async move {
        let request = receive(&mut peer).await;
        recorded.lock().unwrap().push(request);
        peer.close(None).await.unwrap();
    })
    .await;
    let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    assert_eq!(
        handle
            .request(owner.token(), "SetInputMute", json!({}), RPC)
            .await,
        Err(RequestError::Unknown(Invalidation::Disconnected))
    );
    assert_eq!(
        handle
            .request(owner.token(), "SetInputMute", json!({}), RPC)
            .await,
        Err(RequestError::BeforeDispatch(BeforeDispatch::Closed(
            Invalidation::Disconnected
        )))
    );
    assert_eq!(events.next().await.unwrap_err(), Invalidation::Disconnected);
    assert_eq!(seen.lock().unwrap().len(), 1);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn active_cancellation_closes_the_uncertain_session_without_replay() {
    let (seen_tx, seen_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        let request = receive(&mut peer).await;
        assert_eq!(request["d"]["requestType"], "SetInputMute");
        seen_tx.send(()).unwrap();
        closed(&mut peer).await;
    })
    .await;
    let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut pending = Box::pin(handle.request(owner.token(), "SetInputMute", json!({}), RPC));
    assert!(poll!(pending.as_mut()).is_pending());
    seen_rx.await.unwrap();
    drop(pending);
    assert_eq!(
        invalidated(&handle).await,
        Invalidation::CancelledAfterDispatch
    );
    assert_eq!(
        events.next().await.unwrap_err(),
        Invalidation::CancelledAfterDispatch
    );
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn explicit_rejection_preserves_code_and_keeps_the_session_usable() {
    let (url, peer) = fixture(false, |mut peer| async move {
        let rejected = receive(&mut peer).await;
        reply(&mut peer, &rejected, false, 602).await;
        let accepted = receive(&mut peer).await;
        reply(&mut peer, &accepted, true, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let rejected = handle
        .request(owner.token(), "SetRejected", json!({}), RPC)
        .await
        .unwrap_err();
    assert_eq!(rejected.rejection_code(), Some(602));
    assert!(!rejected.to_string().contains("fixture-private-comment"));
    assert!(!format!("{rejected:?}").contains("fixture-private-comment"));
    assert_eq!(
        rejected.obs_failure_message("SetRejected").unwrap(),
        "OBS request SetRejected failed with code 602: fixture-private-comment"
    );
    handle
        .request(owner.token(), "SetAccepted", json!({}), RPC)
        .await
        .unwrap();
    assert_eq!(handle.invalidation(), None);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn retained_event_permits_bound_consumed_as_well_as_queued_items() {
    let (again_tx, again_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        event(&mut peer, 1).await;
        again_rx.await.unwrap();
        event(&mut peer, 2).await;
        closed(&mut peer).await;
    })
    .await;
    let options = SessionOptions {
        event_items: 1,
        ..SessionOptions::default()
    };
    let (owner, mut events) = ObsSession::connect(&url, None, options).await.unwrap();
    let handle = owner.handle();
    let retained = events.next().await.unwrap();
    again_tx.send(()).unwrap();
    assert_eq!(invalidated(&handle).await, Invalidation::EventOverflow);
    assert_eq!(handle.event_cursor(), 2);
    assert_eq!(
        events.next().await.unwrap_err(),
        Invalidation::EventOverflow
    );
    assert_eq!(retained.data, Some(json!({"sequence":1})));
    drop(retained);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn event_byte_overflow_invalidates_instead_of_delivering_success() {
    let (url, peer) = fixture(false, |mut peer| async move {
        event(&mut peer, 1).await;
        closed(&mut peer).await;
    })
    .await;
    let options = SessionOptions {
        event_bytes: 32,
        ..SessionOptions::default()
    };
    let (owner, mut events) = ObsSession::connect(&url, None, options).await.unwrap();
    assert_eq!(
        events.next().await.unwrap_err(),
        Invalidation::EventOverflow
    );
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn admitted_wire_bytes_are_charged_through_completion_before_next_allocation() {
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        let held = receive(&mut peer).await;
        seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        reply(&mut peer, &held, true, 100).await;
        let after = receive(&mut peer).await;
        assert_eq!(after["d"]["requestType"], "SetAfter");
        reply(&mut peer, &after, true, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let options = SessionOptions {
        request_bytes: 512,
        ..SessionOptions::default()
    };
    let (owner, _events) = ObsSession::connect(&url, None, options).await.unwrap();
    let handle = owner.handle();
    let mut held = Box::pin(handle.request(
        owner.token(),
        "SetHeld",
        json!({"payload":"x".repeat(350)}),
        RPC,
    ));
    assert!(poll!(held.as_mut()).is_pending());
    seen_rx.await.unwrap();
    assert_eq!(
        handle
            .request(
                owner.token(),
                "SetQueued",
                json!({"payload":"x".repeat(100)}),
                RPC
            )
            .await,
        Err(RequestError::BeforeDispatch(BeforeDispatch::ByteCapacity))
    );
    release_tx.send(()).unwrap();
    held.await.unwrap();
    assert_eq!(
        handle
            .request(
                owner.token(),
                "SetHuge",
                json!({"payload":"x".repeat(1024)}),
                RPC
            )
            .await,
        Err(RequestError::BeforeDispatch(BeforeDispatch::ByteCapacity))
    );
    handle
        .request(owner.token(), "SetAfter", json!({}), RPC)
        .await
        .unwrap();
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn item_capacity_refuses_before_dispatch_and_shutdown_joins_active_work() {
    let (seen_tx, seen_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        receive(&mut peer).await;
        seen_tx.send(()).unwrap();
        closed(&mut peer).await;
    })
    .await;
    let options = SessionOptions {
        request_items: 1,
        ..SessionOptions::default()
    };
    let (owner, mut events) = ObsSession::connect(&url, None, options).await.unwrap();
    let handle = owner.handle();
    let token = owner.token();
    let mut held = Box::pin(handle.request(token, "SetHeld", json!({}), RPC));
    assert!(poll!(held.as_mut()).is_pending());
    seen_rx.await.unwrap();
    assert_eq!(
        handle
            .request(token, "SetOverCapacity", json!({}), RPC)
            .await,
        Err(RequestError::BeforeDispatch(BeforeDispatch::Capacity))
    );
    owner.shutdown().await.unwrap();
    assert_eq!(
        held.await,
        Err(RequestError::Unknown(Invalidation::Shutdown))
    );
    assert_eq!(events.next().await.unwrap_err(), Invalidation::Shutdown);
    joined(peer).await;
}

#[tokio::test]
async fn dropping_event_consumer_invalidates_the_transport() {
    let (url, peer) = fixture(false, |mut peer| async move {
        closed(&mut peer).await;
    })
    .await;
    let (owner, events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    drop(events);
    assert_eq!(
        invalidated(&handle).await,
        Invalidation::EventConsumerDropped
    );
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn deadline_after_dispatch_reports_unknown_and_closes_the_connection() {
    let (url, peer) = fixture(false, |mut peer| async move {
        receive(&mut peer).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    assert_eq!(
        handle
            .request(
                owner.token(),
                "SetNoReply",
                json!({}),
                Duration::from_millis(20)
            )
            .await,
        Err(RequestError::Unknown(Invalidation::DeadlineAfterDispatch))
    );
    assert_eq!(
        invalidated(&handle).await,
        Invalidation::DeadlineAfterDispatch
    );
    assert_eq!(
        events.next().await.unwrap_err(),
        Invalidation::DeadlineAfterDispatch
    );
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn event_overflow_during_rpc_invalidates_the_inflight_outcome() {
    let (url, peer) = fixture(false, |mut peer| async move {
        receive(&mut peer).await;
        event(&mut peer, 1).await;
        event(&mut peer, 2).await;
        closed(&mut peer).await;
    })
    .await;
    let options = SessionOptions {
        event_items: 1,
        ..SessionOptions::default()
    };
    let (owner, mut events) = ObsSession::connect(&url, None, options).await.unwrap();
    let handle = owner.handle();
    assert_eq!(
        handle
            .request(owner.token(), "SetPending", json!({}), RPC)
            .await,
        Err(RequestError::Unknown(Invalidation::EventOverflow))
    );
    assert_eq!(
        events.next().await.unwrap_err(),
        Invalidation::EventOverflow
    );
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn dropping_owner_invalidates_handles_and_drops_the_socket() {
    let (url, peer) = fixture(false, |mut peer| async move {
        // Owner Drop deliberately aborts rather than promising an awaited
        // WebSocket close handshake. EOF/reset still proves socket destruction.
        dropped_socket(&mut peer).await;
    })
    .await;
    let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    drop(owner);
    assert_eq!(invalidated(&handle).await, Invalidation::OwnerDropped);
    assert_eq!(events.next().await.unwrap_err(), Invalidation::OwnerDropped);
    joined(peer).await;
}

#[tokio::test]
async fn connection_deadline_covers_waiting_for_hello_and_drops_the_socket() {
    let (url, _ready, peer) = unidentified_peer().await;
    let options = SessionOptions {
        connect_timeout: Duration::from_millis(200),
        ..SessionOptions::default()
    };
    assert!(matches!(
        ObsSession::connect(&url, None, options).await,
        Err(ConnectError::Deadline)
    ));
    joined(peer).await;
}

#[tokio::test]
async fn connection_cancellation_leaves_no_detached_reader_or_socket() {
    let (url, ready, peer) = unidentified_peer().await;
    let opening =
        tokio::spawn(
            async move { ObsSession::connect(&url, None, SessionOptions::default()).await },
        );
    timeout(WAIT, ready).await.unwrap().unwrap();
    opening.abort();
    assert!(opening.await.is_err());
    joined(peer).await;
}

#[tokio::test]
async fn endpoint_binding_distinguishes_receiver_tokens_across_same_endpoint_connections() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
            let mut peer = accept_async(socket).await.unwrap();
            send(
                &mut peer,
                json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}}),
            )
            .await;
            assert_eq!(receive(&mut peer).await["op"], 1);
            send(&mut peer, json!({"op":2,"d":{"negotiatedRpcVersion":1}})).await;
            closed(&mut peer).await;
        }
    });
    let (first, events_first) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let first_handle = first.handle();
    assert_eq!(events_first.connection_token(), first_handle.token());
    let expected = format!("{:x}", Sha256::digest(url.as_bytes()));
    assert_eq!(first_handle.endpoint_hash(), expected);
    assert_eq!(first_handle.clone().endpoint_hash(), expected);
    let generation = first.token().generation();
    assert!(generation > 0);
    first.shutdown().await.unwrap();
    let (second, events_second) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    assert_eq!(second.handle().endpoint_hash(), expected);
    assert!(second.token().generation() > generation);
    assert_eq!(events_second.connection_token(), second.handle().token());
    // Same endpoint, different physical connections: the stale receiver cannot
    // be used as the new handle's observation source.
    assert_ne!(events_first.connection_token(), second.handle().token());
    second.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn event_debug_omits_payload_values_and_connect_errors_omit_credentials() {
    let (url, peer) = fixture(false, |mut peer| async move {
        send(&mut peer, json!({"op":5,"d":{"eventType":"VendorEvent","eventIntent":512,"eventData":{"url":"ws://fixture-user:fixture-password@loopback/fixture-private-path","password":"fixture-private-password"}}})).await;
        closed(&mut peer).await;
    }).await;
    let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let event = events.next().await.unwrap();
    let diagnostic = format!("{event:?}");
    assert!(diagnostic.contains("VendorEvent"));
    assert!(!diagnostic.contains("fixture-password"));
    assert!(!diagnostic.contains("fixture-private"));
    assert!(!diagnostic.contains("fixture-user"));
    owner.shutdown().await.unwrap();
    joined(peer).await;
    let endpoint = "ws://127.0.0.1:0/fixture-private-path?password=fixture-private-password";
    let error = match ObsSession::connect(
        endpoint,
        Some("fixture-private-password"),
        SessionOptions::default(),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("port zero unexpectedly connected"),
    };
    let diagnostic = format!("{error:?} {error}");
    assert!(!diagnostic.contains(endpoint));
    assert!(!diagnostic.contains("fixture-private"));
}

#[test]
fn subscription_masks_match_the_pinned_protocol_enum() {
    let protocol: Value = serde_json::from_str(include_str!("../../docs/protocol.json")).unwrap();
    let entries = protocol["enums"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["enumType"] == "EventSubscription")
        .unwrap()["enumIdentifiers"]
        .as_array()
        .unwrap();
    let mut flags = std::collections::HashMap::new();
    for entry in entries {
        let name = entry["enumIdentifier"].as_str().unwrap();
        if name == "All" {
            continue;
        }
        let value = if let Some(value) = entry["enumValue"].as_u64() {
            value
        } else {
            let shift: u32 = entry["enumValue"]
                .as_str()
                .unwrap()
                .strip_prefix("(1 << ")
                .unwrap()
                .strip_suffix(')')
                .unwrap()
                .parse()
                .unwrap();
            1u64.checked_shl(shift).unwrap()
        };
        flags.insert(name, value);
    }
    assert_eq!(
        flags.values().fold(0, |mask, value| mask | value),
        SUPPORTED_EVENT_SUBSCRIPTIONS
    );
    let all_expression = entries
        .iter()
        .find(|entry| entry["enumIdentifier"] == "All")
        .unwrap()["enumValue"]
        .as_str()
        .unwrap();
    let all = all_expression
        .trim_matches(['(', ')'])
        .split('|')
        .map(str::trim)
        .fold(0, |mask, name| mask | flags[name]);
    assert_eq!(all, ALL_EVENTS);
}

#[tokio::test]
async fn invalid_options_and_encrypted_urls_fail_before_network_admission() {
    let options = [
        SessionOptions {
            request_items: 0,
            ..SessionOptions::default()
        },
        SessionOptions {
            event_items: tokio::sync::Semaphore::MAX_PERMITS + 1,
            ..SessionOptions::default()
        },
        SessionOptions {
            request_bytes: usize::MAX,
            ..SessionOptions::default()
        },
        SessionOptions {
            shutdown_timeout: Duration::MAX,
            ..SessionOptions::default()
        },
        SessionOptions {
            event_subscriptions: SUPPORTED_EVENT_SUBSCRIPTIONS | (1 << 63),
            ..SessionOptions::default()
        },
    ];
    for options in options {
        assert!(matches!(
            ObsSession::connect("ws://127.0.0.1:1", None, options).await,
            Err(ConnectError::InvalidOptions)
        ));
    }
    assert!(matches!(
        ObsSession::connect("wss://127.0.0.1:1", None, SessionOptions::default()).await,
        Err(ConnectError::UnsupportedTransport)
    ));
}

// Source OBS text evidence: full-rust-service/custom-input-raw-obs-first.log.
// Assert numeric leaf spelling at the physical Session boundary; parsing the
// frame back into Value alone would hide the integer/decimal token distinction.
#[tokio::test]
async fn source_number_tokens_reach_single_and_batch_wire_without_changing_identity() {
    let (captured_tx, captured_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        let mut captured = Vec::new();
        for operation in [6, 8] {
            let text = match timeout(WAIT, peer.next()).await.unwrap().unwrap().unwrap() {
                Message::Text(text) => text.to_string(),
                other => panic!("expected text frame: {other:?}"),
            };
            let frame: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(frame["op"], operation);
            if operation == 6 {
                reply(&mut peer, &frame, true, 100).await;
            } else {
                send(&mut peer, json!({"op":9,"d":{"requestId":frame["d"]["requestId"],"results":[{"requestId":"0","requestType":"SetInputSettings","requestStatus":{"result":true,"code":100},"responseData":{}}]}})).await;
            }
            captured.push(text);
        }
        captured_tx.send(captured).unwrap();
        closed(&mut peer).await;
    }).await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let data: Value = serde_json::from_str(r#"{"negativeZero":-0.0,"positiveFloat":1.0,"safeLow":9007199254740991,"safeEdge":9007199254740992,"rounded":9007199254740993,"nextEven":9007199254740994,"negativeRounded":-9007199254740993,"fullU64":18446744073709551615,"fullI64":-9223372036854775808,"shortInteger":1000000000000000128,"minSubnormal":5e-324,"smallExponent":1e-7,"threshold":0.000001,"largeExponent":1e21}"#).unwrap();
    handle
        .request(owner.token(), "SetInputSettings", data.clone(), RPC)
        .await
        .unwrap();
    handle
        .batch(
            owner.token(),
            vec![BatchRequest {
                request_type: "SetInputSettings".into(),
                request_data: Some(data),
            }],
            BatchOptions {
                execution: Execution::SerialFrame,
                timeout: Some(RPC),
                ..BatchOptions::default()
            },
        )
        .await
        .unwrap();
    let captured = captured_rx.await.unwrap();
    let generation = owner.token().generation();
    owner.shutdown().await.unwrap();
    joined(peer).await;
    for (index, text) in captured.iter().enumerate() {
        for (name, expected) in [
            ("negativeZero", "0"),
            ("positiveFloat", "1"),
            ("safeLow", "9007199254740991"),
            ("safeEdge", "9007199254740992"),
            ("rounded", "9007199254740992"),
            ("nextEven", "9007199254740994"),
            ("negativeRounded", "-9007199254740992"),
            ("fullU64", "18446744073709552000"),
            ("fullI64", "-9223372036854776000"),
            ("shortInteger", "1000000000000000100"),
            ("minSubnormal", "5e-324"),
            ("smallExponent", "1e-7"),
            ("threshold", "0.000001"),
            ("largeExponent", "1e+21"),
        ] {
            let marker = format!("\"{name}\":");
            let after = text.split_once(&marker).unwrap().1;
            let length = after
                .bytes()
                .take_while(|b| b.is_ascii_digit() || b"-+.eE".contains(b))
                .count();
            assert_eq!(&after[..length], expected, "operation {index} leaf {name}");
        }
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert_eq!(
            parsed["d"]["requestId"],
            format!("{}:{}", generation, index + 1)
        );
        assert!(text.contains(&format!("\"op\":{}", if index == 0 { 6 } else { 8 })));
    }
    assert!(captured[1].contains("\"executionType\":1"));
}

#[tokio::test]
async fn source_decimal_expansion_is_refused_before_guard_and_releases_admission() {
    let (dispatched_tx, dispatched_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        let mut dispatched = Vec::new();
        loop {
            let frame = receive(&mut peer).await;
            let kind = frame["d"]["requestType"].as_str().unwrap().to_owned();
            dispatched.push(kind.clone());
            reply(&mut peer, &frame, true, 100).await;
            if kind == "SetAfter" {
                break;
            }
        }
        dispatched_tx.send(dispatched).unwrap();
        closed(&mut peer).await;
    })
    .await;
    let options = SessionOptions {
        request_items: 1,
        request_bytes: 1024,
        ..SessionOptions::default()
    };
    let (owner, _events) = ObsSession::connect(&url, None, options).await.unwrap();
    let handle = owner.handle();
    let guards = Arc::new(AtomicUsize::new(0));
    let called = guards.clone();
    let data = json!({"values":vec![0.000001;160]});
    assert!(serde_json::to_vec(&data).unwrap().len() < 900);
    let result = handle
        .request_checked(owner.token(), "SetExpanding", data, RPC, move || {
            called.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;
    handle
        .request(owner.token(), "SetAfter", json!({}), RPC)
        .await
        .unwrap();
    let dispatched = dispatched_rx.await.unwrap();
    owner.shutdown().await.unwrap();
    joined(peer).await;
    assert_eq!(
        result,
        Err(RequestError::BeforeDispatch(BeforeDispatch::ByteCapacity))
    );
    assert_eq!(guards.load(Ordering::SeqCst), 0);
    assert_eq!(dispatched, ["SetAfter"]);
}

#[tokio::test]
async fn bounded_wire_depth_preserves_parsed_payload_and_refuses_unqualified_library_depth() {
    fn payload(array_depth: usize) -> Value {
        let mut value = json!(0);
        for _ in 0..array_depth {
            value = Value::Array(vec![value]);
        }
        json!({"nested":value})
    }
    let (dispatched_tx, dispatched_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        let mut dispatched = Vec::new();
        loop {
            // Inspect the controlled header without imposing the fixture's
            // default128-depth inbound parser on an outbound132-depth frame.
            let text = match timeout(WAIT, peer.next()).await.unwrap().unwrap().unwrap() {
                Message::Text(text) => text.to_string(),
                other => panic!("expected text frame: {other:?}"),
            };
            let request_id = text.split_once("\"requestId\":\"").unwrap().1.split_once('"').unwrap().0;
            let operation = if text.contains("\"op\":8") {8} else {6};
            dispatched.push(operation);
            if operation == 8 {
                send(&mut peer, json!({"op":9,"d":{"requestId":request_id,"results":[{"requestId":"0","requestType":"SetDeep","requestStatus":{"result":true,"code":100},"responseData":{}}]}})).await;
                break;
            }
            send(&mut peer,json!({"op":7,"d":{"requestId":request_id,"requestType":"SetTooDeep","requestStatus":{"result":true,"code":100},"responseData":{}}})).await;
        }
        dispatched_tx.send(dispatched).unwrap();
        closed(&mut peer).await;
    }).await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let calls = Arc::new(AtomicUsize::new(0));
    let guard = calls.clone();
    // Complete single-request frame depth: payload object +130 arrays +2
    // enclosing requestData wrappers =133, beyond the qualified132 bound.
    let refused = handle
        .request_checked(owner.token(), "SetTooDeep", payload(130), RPC, move || {
            guard.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;
    // A parser/domain-qualified opaque payload has depth128. The batch adds
    // exactly4 wrappers (d, requests, item, requestData), reaching132.
    let accepted = handle
        .batch(
            owner.token(),
            vec![BatchRequest {
                request_type: "SetDeep".into(),
                request_data: Some(payload(127)),
            }],
            BatchOptions {
                timeout: Some(RPC),
                ..BatchOptions::default()
            },
        )
        .await;
    let dispatched = dispatched_rx.await.unwrap();
    owner.shutdown().await.unwrap();
    joined(peer).await;
    assert_eq!(
        refused,
        Err(RequestError::BeforeDispatch(BeforeDispatch::InvalidRequest))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    accepted.unwrap();
    assert_eq!(dispatched, [8]);
}

#[tokio::test]
async fn projected_wire_permits_return_after_queued_cancel_and_final_guard_refusal() {
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (url, peer) = fixture(false, |mut peer| async move {
        let first = receive(&mut peer).await;
        assert_eq!(first["d"]["requestType"], "SetHeld");
        seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        reply(&mut peer, &first, true, 100).await;
        for kind in ["SetBarrier", "SetAfter"] {
            let request = receive(&mut peer).await;
            assert_eq!(request["d"]["requestType"], kind);
            reply(&mut peer, &request, true, 100).await;
        }
        closed(&mut peer).await;
    })
    .await;
    let options = SessionOptions {
        request_items: 2,
        request_bytes: 512,
        ..SessionOptions::default()
    };
    let (owner, _events) = ObsSession::connect(&url, None, options).await.unwrap();
    let handle = owner.handle();
    let mut held = Box::pin(handle.request(owner.token(), "SetHeld", json!({}), RPC));
    assert!(poll!(held.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let cancelled_guard = Arc::new(AtomicUsize::new(0));
    {
        let guard = cancelled_guard.clone();
        let mut cancelled = Box::pin(handle.request_checked(
            owner.token(),
            "SetCancelled",
            json!({"values":vec![0.000001;20]}),
            RPC,
            move || {
                guard.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        assert!(poll!(cancelled.as_mut()).is_pending());
        // The queued command retains its encoded byte permits until the sole
        // actor sees abandonment; dropping this frontend never sends it.
    }
    release_tx.send(()).unwrap();
    held.await.unwrap();
    handle
        .request(owner.token(), "SetBarrier", json!({}), RPC)
        .await
        .unwrap();
    let refused = handle
        .request_checked(
            owner.token(),
            "SetRefused",
            json!({"values":vec![0.000001;40]}),
            RPC,
            || Err("authority changed".into()),
        )
        .await;
    assert_eq!(
        refused,
        Err(RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(
            "authority changed".into()
        )))
    );
    // A retained cancelled/refused ~450-byte frame would exhaust this same
    // 512-byte pool. Successful follow-up proves both leases were returned.
    handle
        .request(
            owner.token(),
            "SetAfter",
            json!({"values":vec![0.000001;40]}),
            RPC,
        )
        .await
        .unwrap();
    assert_eq!(cancelled_guard.load(Ordering::SeqCst), 0);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}
