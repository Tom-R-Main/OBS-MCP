// SPDX-License-Identifier: GPL-2.0-only
//! Closed Stop lane, one physical writer/reader. All effects use loopback fixtures.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::session::{
    BeforeDispatch, Invalidation, ObsSession, RequestError, SessionOptions,
};
use serde_json::{Value, json};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
type Peer = WebSocketStream<TcpStream>;
const WAIT: Duration = Duration::from_secs(10);
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
            other => panic!("unexpected fixture frame {other:?}"),
        }
    }
}
async fn reply(peer: &mut Peer, request: &Value, code: u64) {
    send(peer,json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":request["d"]["requestType"],"requestStatus":{"result":code==100,"code":code,"comment":"controlled-peer-detail"},"responseData":{"outputPath":"fixture","ok":true}}})).await;
}
async fn closed(peer: &mut Peer) {
    match timeout(WAIT, peer.next()).await.unwrap() {
        None | Some(Err(_)) => {}
        Some(Ok(Message::Close(_))) => {
            let _ = peer.flush().await;
        }
        Some(Ok(other)) => panic!("unexpected extra write {other:?}"),
    }
}

async fn fixture<F, Fut>(script: F) -> (String, tokio::task::JoinHandle<()>)
where
    F: FnOnce(Peer) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
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
    });
    (url, peer)
}
async fn joined(peer: tokio::task::JoinHandle<()>) {
    timeout(WAIT, peer).await.unwrap().unwrap();
}
#[tokio::test]
async fn ordinary_capacity_full_still_admits_exactly_one_stop_before_held_ack() {
    let (seen_normal, normal_seen) = oneshot::channel();
    let (seen_stop, stop_seen) = oneshot::channel();
    let (release_stop, stop_release) = oneshot::channel();
    let (release_normal, normal_release) = oneshot::channel();
    let (url, peer) = fixture(|mut peer| async move {
        let normal = receive(&mut peer).await;
        assert_eq!(normal["d"]["requestType"], "SetHeld");
        seen_normal.send(()).unwrap();
        let stop = receive(&mut peer).await;
        assert_eq!(stop["d"]["requestType"], "StopRecord");
        assert!(stop["d"].get("requestData").is_none());
        assert_ne!(normal["d"]["requestId"], stop["d"]["requestId"]);
        seen_stop.send(()).unwrap();
        stop_release.await.unwrap();
        reply(&mut peer, &stop, 100).await;
        normal_release.await.unwrap();
        reply(&mut peer, &normal, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let options = SessionOptions {
        request_items: 1,
        request_bytes: 512,
        ..SessionOptions::default()
    };
    let (owner, _events) = ObsSession::connect(&url, None, options).await.unwrap();
    let handle = owner.handle();
    let empty_frame = json!({"op":6,"d":{"requestId":format!("{}:1",owner.token().generation()),"requestType":"SetHeld","requestData":{"payload":""}}});
    let overhead = obs_mcp_native::js_json::encoded_len(
        &empty_frame,
        obs_mcp_native::js_json::Limits::default(),
    )
    .unwrap();
    let mut normal = Box::pin(handle.request(
        owner.token(),
        "SetHeld",
        json!({"payload":"x".repeat(512-overhead)}),
        WAIT,
    ));
    assert!(poll!(normal.as_mut()).is_pending());
    normal_seen.await.unwrap();
    assert!(matches!(
        handle
            .request(owner.token(), "SetNoCapacity", json!({}), WAIT)
            .await,
        Err(RequestError::BeforeDispatch(BeforeDispatch::Capacity))
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let guard = calls.clone();
    let mut stop = Box::pin(handle.stop_record_checked(owner.token(), WAIT, move || {
        guard.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    assert!(poll!(stop.as_mut()).is_pending());
    stop_seen.await.unwrap();
    assert!(matches!(
        handle
            .stop_record_checked(owner.token(), WAIT, || panic!("second Stop guard ran"))
            .await,
        Err(RequestError::BeforeDispatch(BeforeDispatch::Capacity))
    ));
    release_stop.send(()).unwrap();
    assert_eq!(stop.await.unwrap()["outputPath"], "fixture");
    assert!(matches!(
        handle
            .request(owner.token(), "SetStillFull", json!({}), WAIT)
            .await,
        Err(RequestError::BeforeDispatch(BeforeDispatch::Capacity))
    ));
    release_normal.send(()).unwrap();
    normal.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}
#[tokio::test]
async fn normal_ack_is_delivered_before_stop_ack_and_next_normal_stays_serial() {
    let (seen_normal, normal_seen) = oneshot::channel();
    let (seen_stop, stop_seen) = oneshot::channel();
    let (release, release_stop) = oneshot::channel();
    let(url,peer)=fixture(|mut peer|async move{let first=receive(&mut peer).await;seen_normal.send(()).unwrap();let stop=receive(&mut peer).await;seen_stop.send(()).unwrap();reply(&mut peer,&first,100).await;
 tokio::select!{_ = release_stop=>{},early=peer.next()=>panic!("ordinary command passed pending Stop: {early:?}")}
 reply(&mut peer,&stop,100).await;let after=receive(&mut peer).await;assert_eq!(after["d"]["requestType"],"SetAfter");reply(&mut peer,&after,100).await;closed(&mut peer).await;}).await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut normal = Box::pin(handle.request(owner.token(), "SetHeld", json!({}), WAIT));
    assert!(poll!(normal.as_mut()).is_pending());
    normal_seen.await.unwrap();
    let mut stop = Box::pin(handle.stop_record_checked(owner.token(), WAIT, || Ok(())));
    assert!(poll!(stop.as_mut()).is_pending());
    stop_seen.await.unwrap();
    normal.await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let guard = calls.clone();
    let mut after =
        Box::pin(
            handle.request_checked(owner.token(), "SetAfter", json!({}), WAIT, move || {
                guard.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        );
    assert!(poll!(after.as_mut()).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    stop.await.unwrap();
    after.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}
#[tokio::test]
async fn either_reply_order_preserves_independent_rejection_comments_and_known_results() {
    for stop_first in [false, true] {
        let (seen_normal, normal_seen) = oneshot::channel();
        let (url, peer) = fixture(move |mut peer| async move {
            let normal = receive(&mut peer).await;
            seen_normal.send(()).unwrap();
            let stop = receive(&mut peer).await;
            if stop_first {
                reply(&mut peer, &stop, 701).await;
                reply(&mut peer, &normal, 704).await;
            } else {
                reply(&mut peer, &normal, 704).await;
                reply(&mut peer, &stop, 701).await;
            }
            closed(&mut peer).await;
        })
        .await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let handle = owner.handle();
        let mut normal = Box::pin(handle.request(owner.token(), "SetHeld", json!({}), WAIT));
        assert!(poll!(normal.as_mut()).is_pending());
        normal_seen.await.unwrap();
        let (stop, normal) = tokio::join!(
            handle.stop_record_checked(owner.token(), WAIT, || Ok(())),
            normal
        );
        assert_eq!(
            stop.unwrap_err().obs_failure_message("StopRecord").unwrap(),
            "OBS request StopRecord failed with code 701: controlled-peer-detail"
        );
        assert_eq!(normal.unwrap_err().rejection_code(), Some(704));
        assert!(handle.invalidation().is_none());
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
}
#[tokio::test]
async fn both_pending_follow_conservative_cancel_deadline_eof_and_shutdown_fates() {
    for cause in [
        "normal-deadline",
        "stop-deadline",
        "normal-cancel",
        "stop-cancel",
        "eof",
        "shutdown",
    ] {
        let (seen_normal, normal_seen) = oneshot::channel();
        let (seen_stop, stop_seen) = oneshot::channel();
        let (close_tx, close_rx) = oneshot::channel();
        let (url, peer) = fixture(move |mut peer| async move {
            receive(&mut peer).await;
            seen_normal.send(()).unwrap();
            receive(&mut peer).await;
            seen_stop.send(()).unwrap();
            if cause == "eof" {
                close_rx.await.unwrap();
                peer.close(None).await.unwrap();
            } else {
                let _keep = close_rx;
                closed(&mut peer).await;
            }
        })
        .await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let handle = owner.handle();
        let mut normal = Box::pin(handle.request(
            owner.token(),
            "SetHeld",
            json!({}),
            if cause == "normal-deadline" {
                Duration::from_secs(2)
            } else {
                WAIT
            },
        ));
        assert!(poll!(normal.as_mut()).is_pending());
        normal_seen.await.unwrap();
        let mut stop = Box::pin(handle.stop_record_checked(
            owner.token(),
            if cause == "stop-deadline" {
                Duration::from_secs(2)
            } else {
                WAIT
            },
            || Ok(()),
        ));
        assert!(poll!(stop.as_mut()).is_pending());
        stop_seen.await.unwrap();
        let mut owner = Some(owner);
        match cause {
            "normal-cancel" => {
                drop(normal);
                assert_eq!(
                    stop.await,
                    Err(RequestError::Unknown(Invalidation::CancelledAfterDispatch))
                );
            }
            "stop-cancel" => {
                drop(stop);
                assert_eq!(
                    normal.await,
                    Err(RequestError::Unknown(Invalidation::CancelledAfterDispatch))
                );
            }
            "eof" => {
                close_tx.send(()).unwrap();
                let (a, b) = tokio::join!(normal, stop);
                assert_eq!(a, Err(RequestError::Unknown(Invalidation::Disconnected)));
                assert_eq!(b, Err(RequestError::Unknown(Invalidation::Disconnected)));
            }
            "shutdown" => {
                let shutdown = owner.take().unwrap().shutdown();
                let (a, b, c) = tokio::join!(normal, stop, shutdown);
                assert_eq!(a, Err(RequestError::Unknown(Invalidation::Shutdown)));
                assert_eq!(b, Err(RequestError::Unknown(Invalidation::Shutdown)));
                c.unwrap();
            }
            _ => {
                let (a, b) = tokio::join!(normal, stop);
                assert_eq!(
                    a,
                    Err(RequestError::Unknown(Invalidation::DeadlineAfterDispatch))
                );
                assert_eq!(
                    b,
                    Err(RequestError::Unknown(Invalidation::DeadlineAfterDispatch))
                );
            }
        }
        if let Some(owner) = owner {
            owner.shutdown().await.unwrap();
        }
        joined(peer).await;
    }
}
#[tokio::test]
async fn known_stop_result_survives_later_normal_deadline_and_foreign_reply_ids() {
    let (seen_normal, normal_seen) = oneshot::channel();
    let (url, peer) = fixture(|mut peer| async move {
        let normal = receive(&mut peer).await;
        seen_normal.send(()).unwrap();
        let stop = receive(&mut peer).await;
        let mut foreign = stop.clone();
        foreign["d"]["requestId"] = json!("foreign");
        reply(&mut peer, &foreign, 100).await;
        reply(&mut peer, &stop, 100).await;
        let _held = normal;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut normal =
        Box::pin(handle.request(owner.token(), "SetHeld", json!({}), Duration::from_secs(2)));
    assert!(poll!(normal.as_mut()).is_pending());
    normal_seen.await.unwrap();
    let stop = handle
        .stop_record_checked(owner.token(), WAIT, || Ok(()))
        .await
        .unwrap();
    assert_eq!(stop["outputPath"], "fixture");
    assert_eq!(
        normal.await,
        Err(RequestError::Unknown(Invalidation::DeadlineAfterDispatch))
    );
    assert_eq!(stop["outputPath"], "fixture");
    owner.shutdown().await.unwrap();
    joined(peer).await;
}
#[tokio::test]
async fn matching_stop_id_with_wrong_type_invalidates_both_without_replay() {
    let (seen_normal, normal_seen) = oneshot::channel();
    let (url, peer) = fixture(|mut peer| async move {
        receive(&mut peer).await;
        seen_normal.send(()).unwrap();
        let mut stop = receive(&mut peer).await;
        stop["d"]["requestType"] = json!("SetHeld");
        reply(&mut peer, &stop, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut normal = Box::pin(handle.request(owner.token(), "SetHeld", json!({}), WAIT));
    assert!(poll!(normal.as_mut()).is_pending());
    normal_seen.await.unwrap();
    let (a, b) = tokio::join!(
        normal,
        handle.stop_record_checked(owner.token(), WAIT, || Ok(()))
    );
    assert_eq!(
        a,
        Err(RequestError::Unknown(
            Invalidation::ResponseIdentityMismatch
        ))
    );
    assert_eq!(
        b,
        Err(RequestError::Unknown(
            Invalidation::ResponseIdentityMismatch
        ))
    );
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn ordinary_batch_and_reserved_stop_keep_distinct_ids_and_event_cursor() {
    use obs_mcp_native::session::{BatchOptions, BatchRequest};
    let (seen, seen_rx) = oneshot::channel();
    let(url,peer)=fixture(|mut peer|async move{
        let batch=receive(&mut peer).await;assert_eq!(batch["op"],8);seen.send(()).unwrap();
        let stop=receive(&mut peer).await;assert_eq!(stop["op"],6);assert_ne!(stop["d"]["requestId"],batch["d"]["requestId"]);
        send(&mut peer,json!({"op":5,"d":{"eventType":"SceneItemTransformChanged","eventIntent":128,"eventData":{"privateMarker":"controlled"}}})).await;
        reply(&mut peer,&stop,100).await;
        send(&mut peer,json!({"op":9,"d":{"requestId":batch["d"]["requestId"],"results":[{"requestId":"0","requestType":"GetHeld","requestStatus":{"result":true,"code":100},"responseData":{"value":1}}]}})).await;
        closed(&mut peer).await;
    }).await;
    let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut batch = Box::pin(handle.batch(
        owner.token(),
        vec![BatchRequest {
            request_type: "GetHeld".into(),
            request_data: None,
        }],
        BatchOptions {
            timeout: Some(WAIT),
            ..BatchOptions::default()
        },
    ));
    assert!(poll!(batch.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let (a, b) = tokio::join!(
        batch,
        handle.stop_record_checked(owner.token(), WAIT, || Ok(()))
    );
    assert_eq!(a.unwrap()[0].response_data["value"], 1);
    b.unwrap();
    assert_eq!(handle.event_cursor(), 1);
    let event = events.next().await.unwrap();
    assert_eq!(event.connection, owner.token());
    assert_eq!(event.sequence, 1);
    assert!(event.encoded_bytes > 0);
    assert!(!format!("{event:?}").contains("controlled"));
    owner.shutdown().await.unwrap();
    joined(peer).await;
}
#[tokio::test]
async fn stop_matching_id_wrong_batch_shape_is_unknown_for_both() {
    let (seen, seen_rx) = oneshot::channel();
    let (url, peer) = fixture(|mut peer| async move {
        receive(&mut peer).await;
        seen.send(()).unwrap();
        let stop = receive(&mut peer).await;
        send(
            &mut peer,
            json!({"op":9,"d":{"requestId":stop["d"]["requestId"],"results":[]}}),
        )
        .await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut normal = Box::pin(handle.request(owner.token(), "GetHeld", json!({}), WAIT));
    assert!(poll!(normal.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let (a, b) = tokio::join!(
        normal,
        handle.stop_record_checked(owner.token(), WAIT, || Ok(()))
    );
    assert_eq!(
        a,
        Err(RequestError::Unknown(
            Invalidation::ResponseIdentityMismatch
        ))
    );
    assert_eq!(
        b,
        Err(RequestError::Unknown(
            Invalidation::ResponseIdentityMismatch
        ))
    );
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn queued_stop_cancel_expiry_and_guard_refusal_have_zero_wire_and_release_reserved_capacity()
{
    for mode in ["cancel", "deadline", "guard"] {
        let (seen, seen_rx) = oneshot::channel();
        let (go, go_rx) = oneshot::channel();
        let(url,peer)=fixture(move|mut peer|async move{
            let normal=receive(&mut peer).await;seen.send(()).unwrap();go_rx.await.unwrap();
            // Priority queue admission precedes this event's delivery. Seeing
            // it is the causal barrier after the abandoned/refused Stop drains.
            send(&mut peer,json!({"op":5,"d":{"eventType":"VendorEvent","eventIntent":512,"eventData":{"barrier":true}}})).await;
            let stop=receive(&mut peer).await;assert_eq!(stop["d"]["requestType"],"StopRecord");reply(&mut peer,&stop,100).await;reply(&mut peer,&normal,100).await;closed(&mut peer).await;
        }).await;
        let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let handle = owner.handle();
        let mut normal = Box::pin(handle.request(owner.token(), "GetHeld", json!({}), WAIT));
        assert!(poll!(normal.as_mut()).is_pending());
        seen_rx.await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let guard = calls.clone();
        if mode == "guard" {
            assert_eq!(
                handle
                    .stop_record_checked(owner.token(), WAIT, move || {
                        guard.fetch_add(1, Ordering::SeqCst);
                        Err("authority changed".into())
                    })
                    .await,
                Err(RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(
                    "authority changed".into()
                )))
            );
        } else {
            let mut abandoned = Box::pin(handle.stop_record_checked(
                owner.token(),
                if mode == "deadline" {
                    Duration::from_millis(200)
                } else {
                    WAIT
                },
                move || {
                    guard.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            ));
            assert!(
                poll!(abandoned.as_mut()).is_pending(),
                "Stop was not admitted before controlled expiry"
            );
            if mode == "deadline" {
                // Deliberately do not schedule the single-thread actor between
                // queue admission and expiry; the real watchdog remains WAIT.
                std::thread::sleep(Duration::from_millis(250));
                assert_eq!(
                    abandoned.await,
                    Err(RequestError::BeforeDispatch(BeforeDispatch::Deadline))
                );
            } else {
                drop(abandoned);
            }
        }
        go.send(()).unwrap();
        let event = events.next().await.unwrap();
        assert_eq!(event.event_type, "VendorEvent");
        drop(event);
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(mode == "guard"));
        handle
            .stop_record_checked(owner.token(), WAIT, || Ok(()))
            .await
            .unwrap();
        normal.await.unwrap();
        assert!(handle.invalidation().is_none());
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
}

#[tokio::test]
async fn only_stop_workflow_status_pre_and_postchecks_share_reserved_control_slot() {
    let (seen, seen_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let(url,peer)=fixture(|mut peer|async move{
        let normal=receive(&mut peer).await;assert_eq!(normal["d"]["requestType"],"GetHeld");seen.send(()).unwrap();
        for(name,active)in [("GetRecordStatus",true),("StopRecord",true),("GetRecordStatus",false)]{
            let request=receive(&mut peer).await;assert_eq!(request["d"]["requestType"],name);
            send(&mut peer,json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":name,"requestStatus":{"result":true,"code":100},"responseData":{"outputActive":active,"outputPath":"fixture"}}})).await;
        }
        release_rx.await.unwrap();reply(&mut peer,&normal,100).await;
        let ordinary_status=receive(&mut peer).await;assert_eq!(ordinary_status["d"]["requestType"],"GetRecordStatus");reply(&mut peer,&ordinary_status,100).await;
        closed(&mut peer).await;
    }).await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut normal = Box::pin(handle.request(owner.token(), "GetHeld", json!({}), WAIT));
    assert!(poll!(normal.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let guard = calls.clone();
    let mut ordinary_status = Box::pin(handle.request_checked(
        owner.token(),
        "GetRecordStatus",
        Value::Null,
        WAIT,
        move || {
            guard.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    ));
    assert!(poll!(ordinary_status.as_mut()).is_pending());
    assert_eq!(
        handle
            .stop_record_status_checked(owner.token(), WAIT, || Ok(()))
            .await
            .unwrap()["outputActive"],
        true
    );
    handle
        .stop_record_checked(owner.token(), WAIT, || Ok(()))
        .await
        .unwrap();
    assert_eq!(
        handle
            .stop_record_status_checked(owner.token(), WAIT, || Ok(()))
            .await
            .unwrap()["outputActive"],
        false
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    normal.await.unwrap();
    ordinary_status.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn foreign_batch_response_preserves_legacy_strict_identity_with_reserved_stop_pending() {
    use obs_mcp_native::session::{BatchOptions, BatchRequest};
    let (seen, seen_rx) = oneshot::channel();
    let(url,peer)=fixture(|mut peer|async move{
        receive(&mut peer).await;seen.send(()).unwrap();receive(&mut peer).await;
        send(&mut peer,json!({"op":9,"d":{"requestId":"foreign-batch","results":[{"requestId":"0","requestType":"GetHeld","requestStatus":{"result":true,"code":100},"responseData":{}}]}})).await;
        closed(&mut peer).await;
    }).await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut batch = Box::pin(handle.batch(
        owner.token(),
        vec![BatchRequest {
            request_type: "GetHeld".into(),
            request_data: None,
        }],
        BatchOptions {
            timeout: Some(WAIT),
            ..BatchOptions::default()
        },
    ));
    assert!(poll!(batch.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let (a, b) = tokio::join!(
        batch,
        handle.stop_record_checked(owner.token(), WAIT, || Ok(()))
    );
    assert!(matches!(
        a,
        Err(RequestError::Unknown(
            Invalidation::ResponseIdentityMismatch
        ))
    ));
    assert_eq!(
        b,
        Err(RequestError::Unknown(
            Invalidation::ResponseIdentityMismatch
        ))
    );
    owner.shutdown().await.unwrap();
    joined(peer).await;
}
#[tokio::test]
async fn known_normal_result_survives_later_reserved_stop_deadline() {
    let (seen, seen_rx) = oneshot::channel();
    let (url, peer) = fixture(|mut peer| async move {
        let normal = receive(&mut peer).await;
        seen.send(()).unwrap();
        receive(&mut peer).await;
        reply(&mut peer, &normal, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let mut normal = Box::pin(handle.request(owner.token(), "GetHeld", json!({}), WAIT));
    assert!(poll!(normal.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let mut stop =
        Box::pin(handle.stop_record_checked(owner.token(), Duration::from_secs(2), || Ok(())));
    assert!(poll!(stop.as_mut()).is_pending());
    let known = normal.await.unwrap();
    assert_eq!(known["ok"], true);
    assert_eq!(
        stop.await,
        Err(RequestError::Unknown(Invalidation::DeadlineAfterDispatch))
    );
    assert_eq!(known["ok"], true);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}
