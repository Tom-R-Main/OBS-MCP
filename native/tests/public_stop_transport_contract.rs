// SPDX-License-Identifier: GPL-2.0-only
//! Closed public safety Stop lane remains independent of ordinary and owned Take work.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::session::{
    BeforeDispatch, Invalidation, ObsSession, RequestError, SafetyStopRequest, SessionOptions,
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
const KINDS: [SafetyStopRequest; 3] = [
    SafetyStopRequest::Stream,
    SafetyStopRequest::VirtualCam,
    SafetyStopRequest::ReplayBuffer,
];

#[tokio::test]
async fn three_closed_names_omit_data_and_raw_same_names_stay_ordinary() {
    for kind in KINDS {
        let (seen, seen_rx) = oneshot::channel();
        let (raw_queued, raw_queued_rx) = oneshot::channel();
        let (url, peer) = fixture(move |mut peer| async move {
            let normal = receive(&mut peer).await;
            assert_eq!(normal["d"]["requestType"], "GetHeld");
            seen.send(()).unwrap();
            raw_queued_rx.await.unwrap();
            send(&mut peer,json!({"op":5,"d":{"eventType":"VendorEvent","eventIntent":512,"eventData":{"rawAdmissionBarrier":true}}})).await;
            let stop = receive(&mut peer).await;
            assert_eq!(stop["op"], 6);
            assert_eq!(stop["d"]["requestType"], kind.request_type());
            assert_eq!(stop["d"].as_object().unwrap().len(), 2);
            assert!(stop["d"].get("requestData").is_none());
            assert_ne!(normal["d"]["requestId"], stop["d"]["requestId"]);
            reply(&mut peer, &stop, 100).await;
            reply(&mut peer, &normal, 100).await;
            let raw = receive(&mut peer).await;
            assert_eq!(raw["d"]["requestType"], kind.request_type());
            assert!(raw["d"].get("requestData").is_none());
            reply(&mut peer, &raw, 100).await;
            closed(&mut peer).await;
        })
        .await;
        let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let h = owner.handle();
        let mut normal = Box::pin(h.request(owner.token(), "GetHeld", Value::Null, WAIT));
        assert!(poll!(normal.as_mut()).is_pending());
        seen_rx.await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let guard = calls.clone();
        let mut raw = Box::pin(h.request_checked(
            owner.token(),
            kind.request_type(),
            Value::Null,
            WAIT,
            move || {
                guard.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        assert!(poll!(raw.as_mut()).is_pending());
        raw_queued.send(()).unwrap();
        let event = events.next().await.unwrap();
        assert_eq!(event.event_type, "VendorEvent");
        drop(event);
        // The actor has processed a physical event after raw admission; this
        // is not merely an inline assertion before the actor can run.
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let (a, b, c) = tokio::join!(
            h.safety_stop_checked(owner.token(), kind, WAIT, || Ok(())),
            normal,
            raw
        );
        a.unwrap();
        b.unwrap();
        c.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(h.invalidation().is_none());
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
}

#[tokio::test]
async fn held_public_stop_leaves_owned_status_and_stop_independent() {
    for kind in KINDS {
        let (seen, seen_rx) = oneshot::channel();
        let (public_seen, public_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let (url, peer) = fixture(move |mut peer| async move {
            let normal = receive(&mut peer).await;
            seen.send(()).unwrap();
            let public = receive(&mut peer).await;
            assert_eq!(public["d"]["requestType"], kind.request_type());
            public_seen.send(()).unwrap();
            let status = receive(&mut peer).await;
            assert_eq!(status["d"]["requestType"], "GetRecordStatus");
            reply(&mut peer, &status, 100).await;
            let owned = receive(&mut peer).await;
            assert_eq!(owned["d"]["requestType"], "StopRecord");
            reply(&mut peer, &owned, 100).await;
            let ids =
                [&normal, &public, &status, &owned].map(|r| r["d"]["requestId"].as_str().unwrap());
            for a in 0..ids.len() {
                for b in 0..a {
                    assert_ne!(ids[a], ids[b]);
                }
            }
            release_rx.await.unwrap();
            reply(&mut peer, &public, 100).await;
            reply(&mut peer, &normal, 100).await;
            closed(&mut peer).await;
        })
        .await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let h = owner.handle();
        let mut normal = Box::pin(h.request(owner.token(), "GetHeld", Value::Null, WAIT));
        assert!(poll!(normal.as_mut()).is_pending());
        seen_rx.await.unwrap();
        let mut public = Box::pin(h.safety_stop_checked(owner.token(), kind, WAIT, || Ok(())));
        assert!(poll!(public.as_mut()).is_pending());
        public_rx.await.unwrap();
        let refused = Arc::new(AtomicUsize::new(0));
        let guard = refused.clone();
        assert_eq!(
            h.safety_stop_checked(
                owner.token(),
                SafetyStopRequest::ReplayBuffer,
                WAIT,
                move || {
                    guard.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            )
            .await,
            Err(RequestError::BeforeDispatch(BeforeDispatch::Capacity))
        );
        assert_eq!(refused.load(Ordering::SeqCst), 0);
        h.stop_record_status_checked(owner.token(), WAIT, || Ok(()))
            .await
            .unwrap();
        h.stop_record_checked(owner.token(), WAIT, || Ok(()))
            .await
            .unwrap();
        assert!(poll!(public.as_mut()).is_pending());
        assert!(poll!(normal.as_mut()).is_pending());
        release.send(()).unwrap();
        let (a, b) = tokio::join!(public, normal);
        a.unwrap();
        b.unwrap();
        assert!(h.invalidation().is_none());
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
}

#[tokio::test]
async fn all_six_three_pending_reply_orders_preserve_identity_and_known_rejections() {
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let (seen, seen_rx) = oneshot::channel();
        let (public_seen, public_rx) = oneshot::channel();
        let (url, peer) = fixture(move |mut peer| async move {
            let normal = receive(&mut peer).await;
            seen.send(()).unwrap();
            let public = receive(&mut peer).await;
            public_seen.send(()).unwrap();
            let owned = receive(&mut peer).await;
            assert_eq!(normal["d"]["requestType"], "GetHeld");
            assert_eq!(public["d"]["requestType"], "StopStream");
            assert_eq!(owned["d"]["requestType"], "StopRecord");
            let requests = [normal, public, owned];
            for index in order {
                reply(&mut peer, &requests[index], [701, 702, 703][index]).await;
            }
            closed(&mut peer).await;
        })
        .await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let h = owner.handle();
        let mut normal = Box::pin(h.request(owner.token(), "GetHeld", Value::Null, WAIT));
        assert!(poll!(normal.as_mut()).is_pending());
        seen_rx.await.unwrap();
        let mut public =
            Box::pin(
                h.safety_stop_checked(owner.token(), SafetyStopRequest::Stream, WAIT, || Ok(())),
            );
        assert!(poll!(public.as_mut()).is_pending());
        public_rx.await.unwrap();
        let (a, b, c) = tokio::join!(
            normal,
            public,
            h.stop_record_checked(owner.token(), WAIT, || Ok(()))
        );
        assert_eq!(a.unwrap_err().rejection_code(), Some(701));
        assert_eq!(b.unwrap_err().rejection_code(), Some(702));
        assert_eq!(c.unwrap_err().rejection_code(), Some(703));
        assert!(h.invalidation().is_none());
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
}

#[tokio::test]
async fn saturated_ordinary_items_or_encoded_bytes_cannot_consume_either_control_slot() {
    let default_bytes = SessionOptions::default().request_bytes;
    assert_eq!(default_bytes, 8 * 1024 * 1024);
    for (bytes_full, byte_limit) in [(false, 512), (true, 512), (true, default_bytes)] {
        let (seen, seen_rx) = oneshot::channel();
        let (public_seen, public_rx) = oneshot::channel();
        let (url, peer) = fixture(move |mut peer| async move {
            // A complete valid ordinary frame is observed before either
            // control RPC; this does not claim TCP-flush preemption.
            let normal = receive(&mut peer).await;
            assert_eq!(serde_json::to_vec(&normal).unwrap().len(), byte_limit);
            seen.send(()).unwrap();
            let public = receive(&mut peer).await;
            public_seen.send(()).unwrap();
            let owned = receive(&mut peer).await;
            assert_eq!(public["d"]["requestType"], "StopVirtualCam");
            assert_eq!(owned["d"]["requestType"], "StopRecord");
            reply(&mut peer, &owned, 100).await;
            reply(&mut peer, &public, 100).await;
            reply(&mut peer, &normal, 100).await;
            closed(&mut peer).await;
        })
        .await;
        let options = SessionOptions {
            request_items: if bytes_full { 2 } else { 1 },
            request_bytes: byte_limit,
            ..SessionOptions::default()
        };
        let (owner, _events) = ObsSession::connect(&url, None, options).await.unwrap();
        let h = owner.handle();
        let empty = json!({"op":6,"d":{"requestId":format!("{}:1",owner.token().generation()),"requestType":"SetHeld","requestData":{"payload":""}}});
        let overhead = obs_mcp_native::js_json::encoded_len(
            &empty,
            obs_mcp_native::js_json::Limits::default(),
        )
        .unwrap();
        let mut normal = Box::pin(h.request(
            owner.token(),
            "SetHeld",
            json!({"payload":"x".repeat(byte_limit-overhead)}),
            WAIT,
        ));
        assert!(poll!(normal.as_mut()).is_pending());
        seen_rx.await.unwrap();
        assert_eq!(
            h.request(owner.token(), "NoCapacity", Value::Null, WAIT)
                .await,
            Err(RequestError::BeforeDispatch(if bytes_full {
                BeforeDispatch::ByteCapacity
            } else {
                BeforeDispatch::Capacity
            }))
        );
        let mut public = Box::pin(h.safety_stop_checked(
            owner.token(),
            SafetyStopRequest::VirtualCam,
            WAIT,
            || Ok(()),
        ));
        assert!(poll!(public.as_mut()).is_pending());
        public_rx.await.unwrap();
        let (a, b, c) = tokio::join!(
            normal,
            public,
            h.stop_record_checked(owner.token(), WAIT, || Ok(()))
        );
        a.unwrap();
        b.unwrap();
        c.unwrap();
        assert!(h.invalidation().is_none());
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
}

#[tokio::test]
async fn queued_cancel_expiry_and_guard_refusal_have_no_wire_effect_and_release_capacity() {
    for mode in ["cancel", "deadline", "guard"] {
        let (seen, seen_rx) = oneshot::channel();
        let (go, go_rx) = oneshot::channel();
        let(url,peer)=fixture(move|mut peer|async move{
            let normal=receive(&mut peer).await;seen.send(()).unwrap();go_rx.await.unwrap();
            send(&mut peer,json!({"op":5,"d":{"eventType":"VendorEvent","eventIntent":512,"eventData":{"barrier":true}}})).await;
            let stop=receive(&mut peer).await;assert_eq!(stop["d"]["requestType"],"StopReplayBuffer");reply(&mut peer,&stop,100).await;reply(&mut peer,&normal,100).await;closed(&mut peer).await;
        }).await;
        let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let h = owner.handle();
        let mut normal = Box::pin(h.request(owner.token(), "GetHeld", Value::Null, WAIT));
        assert!(poll!(normal.as_mut()).is_pending());
        seen_rx.await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let guard = calls.clone();
        if mode == "guard" {
            assert_eq!(
                h.safety_stop_checked(owner.token(), SafetyStopRequest::Stream, WAIT, move || {
                    guard.fetch_add(1, Ordering::SeqCst);
                    Err("authority changed".into())
                })
                .await,
                Err(RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(
                    "authority changed".into()
                )))
            );
        } else {
            let mut abandoned = Box::pin(h.safety_stop_checked(
                owner.token(),
                SafetyStopRequest::Stream,
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
            assert!(poll!(abandoned.as_mut()).is_pending());
            if mode == "deadline" {
                // Keep this single-thread actor unscheduled between actual
                // queue admission and expiry; no timed wire-effect inference.
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
        h.safety_stop_checked(owner.token(), SafetyStopRequest::ReplayBuffer, WAIT, || {
            Ok(())
        })
        .await
        .unwrap();
        normal.await.unwrap();
        assert!(h.invalidation().is_none());
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
}

#[tokio::test]
async fn matching_wrong_type_or_batch_shape_retires_all_three_without_replay() {
    for cause in ["type", "batch", "foreign-batch", "status"] {
        let (seen, seen_rx) = oneshot::channel();
        let (public_seen, public_rx) = oneshot::channel();
        let(url,peer)=fixture(move|mut peer|async move{
            receive(&mut peer).await;seen.send(()).unwrap();let public=receive(&mut peer).await;public_seen.send(()).unwrap();receive(&mut peer).await;
            let mut frame=json!({"op":7,"d":{"requestId":public["d"]["requestId"],"requestType":"StopStream","requestStatus":{"result":true,"code":100},"responseData":{}}});
            match cause {"type"=>frame["d"]["requestType"]=json!("StopRecord"),"batch"|"foreign-batch"=>{frame=json!({"op":9,"d":{"requestId":if cause=="batch"{public["d"]["requestId"].clone()}else{json!("foreign")},"results":[]}});},_=>frame["d"]["requestStatus"]["result"]=json!("true")}
            send(&mut peer,frame).await;closed(&mut peer).await;
        }).await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let h = owner.handle();
        let mut normal = Box::pin(h.request(owner.token(), "GetHeld", Value::Null, WAIT));
        assert!(poll!(normal.as_mut()).is_pending());
        seen_rx.await.unwrap();
        let mut public =
            Box::pin(
                h.safety_stop_checked(owner.token(), SafetyStopRequest::Stream, WAIT, || Ok(())),
            );
        assert!(poll!(public.as_mut()).is_pending());
        public_rx.await.unwrap();
        let (a, b, c) = tokio::join!(
            normal,
            public,
            h.stop_record_checked(owner.token(), WAIT, || Ok(()))
        );
        let expected = if cause == "status" {
            Invalidation::MalformedMessage
        } else {
            Invalidation::ResponseIdentityMismatch
        };
        for result in [a, b, c] {
            assert_eq!(result, Err(RequestError::Unknown(expected.clone())));
        }
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
}

#[tokio::test]
async fn known_public_ack_survives_peer_deadline_and_foreign_single_id_does_not_settle_it() {
    let (seen, seen_rx) = oneshot::channel();
    let (public_seen, public_rx) = oneshot::channel();
    let (url, peer) = fixture(|mut peer| async move {
        receive(&mut peer).await;
        seen.send(()).unwrap();
        let public = receive(&mut peer).await;
        public_seen.send(()).unwrap();
        receive(&mut peer).await;
        let mut foreign = public.clone();
        foreign["d"]["requestId"] = json!("foreign");
        reply(&mut peer, &foreign, 701).await;
        reply(&mut peer, &public, 100).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let h = owner.handle();
    let mut normal = Box::pin(h.request(
        owner.token(),
        "GetHeld",
        Value::Null,
        Duration::from_secs(1),
    ));
    assert!(poll!(normal.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let mut public =
        Box::pin(h.safety_stop_checked(owner.token(), SafetyStopRequest::Stream, WAIT, || Ok(())));
    assert!(poll!(public.as_mut()).is_pending());
    public_rx.await.unwrap();
    let mut owned = Box::pin(h.stop_record_checked(owner.token(), WAIT, || Ok(())));
    assert!(poll!(owned.as_mut()).is_pending());
    let known = public.await.unwrap();
    assert_eq!(known["ok"], true);
    let (a, b) = tokio::join!(normal, owned);
    for result in [a, b] {
        assert_eq!(
            result,
            Err(RequestError::Unknown(Invalidation::DeadlineAfterDispatch))
        );
    }
    assert_eq!(known["ok"], true);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn default_thirty_two_ordinary_items_do_not_borrow_public_or_owned_capacity() {
    let (seen, seen_rx) = oneshot::channel();
    let (public_seen, public_rx) = oneshot::channel();
    let (url, peer) = fixture(|mut peer| async move {
        let first = receive(&mut peer).await;
        assert_eq!(first["d"]["requestType"], "GetHeld");
        seen.send(()).unwrap();
        let public = receive(&mut peer).await;
        assert_eq!(public["d"]["requestType"], "StopReplayBuffer");
        public_seen.send(()).unwrap();
        let owned = receive(&mut peer).await;
        assert_eq!(owned["d"]["requestType"], "StopRecord");
        reply(&mut peer, &owned, 100).await;
        reply(&mut peer, &public, 100).await;
        reply(&mut peer, &first, 100).await;
        for _ in 1..32 {
            let queued = receive(&mut peer).await;
            assert_eq!(queued["d"]["requestType"], "GetQueued");
            reply(&mut peer, &queued, 100).await;
        }
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let h = owner.handle();
    let mut first = Box::pin(h.request(owner.token(), "GetHeld", Value::Null, WAIT));
    assert!(poll!(first.as_mut()).is_pending());
    seen_rx.await.unwrap();
    let mut queued = Vec::new();
    for _ in 1..32 {
        let mut call = Box::pin(h.request(owner.token(), "GetQueued", Value::Null, WAIT));
        assert!(poll!(call.as_mut()).is_pending());
        queued.push(call);
    }
    assert_eq!(
        h.request(owner.token(), "NoCapacity", Value::Null, WAIT)
            .await,
        Err(RequestError::BeforeDispatch(BeforeDispatch::Capacity))
    );
    let mut public = Box::pin(h.safety_stop_checked(
        owner.token(),
        SafetyStopRequest::ReplayBuffer,
        WAIT,
        || Ok(()),
    ));
    assert!(poll!(public.as_mut()).is_pending());
    public_rx.await.unwrap();
    let (a, b, c) = tokio::join!(
        first,
        public,
        h.stop_record_checked(owner.token(), WAIT, || Ok(()))
    );
    a.unwrap();
    b.unwrap();
    c.unwrap();
    for call in queued {
        call.await.unwrap();
    }
    assert!(h.invalidation().is_none());
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn three_unresolved_commands_retire_on_public_cancel_deadline_disconnect_or_shutdown() {
    for cause in ["cancel", "deadline", "disconnect", "shutdown"] {
        let (seen, seen_rx) = oneshot::channel();
        let (public_seen, public_rx) = oneshot::channel();
        let (owned_seen, owned_rx) = oneshot::channel();
        let (close, close_rx) = oneshot::channel();
        let (url, peer) = fixture(move |mut peer| async move {
            receive(&mut peer).await;
            seen.send(()).unwrap();
            receive(&mut peer).await;
            public_seen.send(()).unwrap();
            receive(&mut peer).await;
            owned_seen.send(()).unwrap();
            if cause == "disconnect" {
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
        let h = owner.handle();
        let token = owner.token();
        let mut owner = Some(owner);
        let mut normal = Box::pin(h.request(token, "GetHeld", Value::Null, WAIT));
        assert!(poll!(normal.as_mut()).is_pending());
        seen_rx.await.unwrap();
        let mut public = Box::pin(h.safety_stop_checked(
            token,
            SafetyStopRequest::Stream,
            if cause == "deadline" {
                Duration::from_secs(1)
            } else {
                WAIT
            },
            || Ok(()),
        ));
        assert!(poll!(public.as_mut()).is_pending());
        public_rx.await.unwrap();
        let mut owned = Box::pin(h.stop_record_checked(token, WAIT, || Ok(())));
        assert!(poll!(owned.as_mut()).is_pending());
        owned_rx.await.unwrap();
        let expected = match cause {
            "cancel" => Invalidation::CancelledAfterDispatch,
            "deadline" => Invalidation::DeadlineAfterDispatch,
            "disconnect" => Invalidation::Disconnected,
            _ => Invalidation::Shutdown,
        };
        if cause == "cancel" {
            drop(public);
            let (a, b) = tokio::join!(normal, owned);
            for result in [a, b] {
                assert_eq!(result, Err(RequestError::Unknown(expected.clone())));
            }
        } else if cause == "shutdown" {
            let (a, b, c, shutdown) =
                tokio::join!(normal, public, owned, owner.take().unwrap().shutdown());
            for result in [a, b, c] {
                assert_eq!(result, Err(RequestError::Unknown(expected.clone())));
            }
            shutdown.unwrap();
        } else {
            if cause == "disconnect" {
                close.send(()).unwrap();
            }
            let (a, b, c) = tokio::join!(normal, public, owned);
            for result in [a, b, c] {
                assert_eq!(result, Err(RequestError::Unknown(expected.clone())));
            }
        }
        if let Some(owner) = owner {
            owner.shutdown().await.unwrap();
        }
        joined(peer).await;
    }
}

#[tokio::test]
async fn stale_generation_zero_and_overflowing_deadline_never_call_guard_or_write() {
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
    let h = owner.handle();
    assert_eq!(
        h.safety_stop_checked(other.token(), SafetyStopRequest::Stream, WAIT, || panic!(
            "stale guard ran"
        ))
        .await,
        Err(RequestError::BeforeDispatch(
            BeforeDispatch::StaleConnection
        ))
    );
    for duration in [Duration::ZERO, Duration::MAX] {
        assert_eq!(
            h.safety_stop_checked(
                owner.token(),
                SafetyStopRequest::VirtualCam,
                duration,
                || panic!("expired guard ran")
            )
            .await,
            Err(RequestError::BeforeDispatch(BeforeDispatch::Deadline))
        );
    }
    owner.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
    joined(peer).await;
    joined(other_peer).await;
}
