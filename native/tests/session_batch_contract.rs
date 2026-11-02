// SPDX-License-Identifier: GPL-2.0-only
//! Real loopback consumers; no live OBS and no promoted MCP routes.
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::session::{
    BatchOptions, BatchRequest, BeforeDispatch, Execution, Invalidation, ObsSession, RequestError,
    SessionHandle, SessionOptions,
};
use serde_json::{Value, json};
use std::{
    future::Future,
    path::Path,
    process::Command,
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
const WAIT: Duration = Duration::from_secs(3);
fn request(name: &str, data: Option<Value>) -> BatchRequest {
    BatchRequest {
        request_type: name.into(),
        request_data: data,
    }
}
fn options() -> BatchOptions {
    BatchOptions {
        timeout: Some(WAIT),
        ..BatchOptions::default()
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
            other => panic!("unexpected request frame: {other:?}"),
        }
    }
}
async fn closed(peer: &mut Peer) {
    while let Some(message) = timeout(WAIT, peer.next()).await.unwrap() {
        match message.unwrap() {
            Message::Close(_) => {
                let _ = peer.flush().await;
                return;
            }
            Message::Ping(_) => {
                let _ = peer.flush().await;
            }
            other => panic!("unexpected dispatch during cleanup: {other:?}"),
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
        let identify = receive(&mut peer).await;
        assert_eq!(identify["op"], 1);
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
fn rows(frame: &Value) -> Vec<Value> {
    frame["d"]["requests"].as_array().unwrap().iter().map(|request|json!({"requestId":request["requestId"],"requestType":request["requestType"],"requestStatus":{"result":true,"code":100},"responseData":{"private":"fixture-data"}})).collect()
}
async fn batch_reply(peer: &mut Peer, frame: &Value, rows: Vec<Value>) {
    send(
        peer,
        json!({"op":9,"d":{"requestId":frame["d"]["requestId"],"results":rows}}),
    )
    .await;
}
async fn rpc_reply(peer: &mut Peer, frame: &Value) {
    send(peer,json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":frame["d"]["requestType"],"requestStatus":{"result":true,"code":100},"responseData":{}}})).await;
}
async fn event(peer: &mut Peer, sequence: u64) {
    send(peer,json!({"op":5,"d":{"eventType":"InputMuteStateChanged","eventIntent":8,"eventData":{"sequence":sequence}}})).await;
}
fn source(requests: Value, options: Value, results: Value) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let output=Command::new("node").arg("--input-type=module").arg("-e").arg(r#"
import {OBSWebSocketClient} from './build/client.js';
const input=JSON.parse(process.env.SESSION_BATCH_TEST_INPUT),client=new OBSWebSocketClient(),socket={};
client.ws=socket;client.readySocket=async()=>socket;
let wire,budget;
client.sendTracked=async(_socket,_name,timeout,build)=>{
  wire=build('outer');budget=timeout;
  return new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>reject(Error('source batch response ignored')),100);
    client.pendingRequests.set('outer',{socket,resolve,reject,timeout:timer});
    client.handleMessage(socket,{op:9,d:{requestId:'outer',results:input.results}});
  });
};
const results=await client.sendBatch(input.requests,input.options);
console.log(JSON.stringify({wire,budget,results}));
"#).current_dir(root).env("SESSION_BATCH_TEST_INPUT",json!({"requests":requests,"options":options,"results":results}).to_string()).output().unwrap();
    assert!(
        output.status.success(),
        "actual TypeScript batch oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn exact_op8_serial_frame_wire_reordered_results_events_and_private_debug_match_source() {
    let captured = Arc::new(Mutex::new(None));
    let capture = captured.clone();
    let raw = Arc::new(Mutex::new(vec![]));
    let raw_peer = raw.clone();
    let (url, peer) = fixture(move |mut peer| async move {
        let frame = receive(&mut peer).await;
        assert_eq!(frame["op"], 8);
        let mut result = rows(&frame);
        result[1]["requestStatus"] =
            json!({"result":false,"code":604,"comment":"fixture-private-comment"});
        result[2]["responseData"] = Value::Null;
        result.reverse();
        *capture.lock().unwrap() = Some(frame.clone());
        *raw_peer.lock().unwrap() = result.clone();
        event(&mut peer, 1).await;
        batch_reply(&mut peer, &frame, result).await;
        event(&mut peer, 2).await;
        let rpc = receive(&mut peer).await;
        assert_eq!(rpc["op"], 6);
        rpc_reply(&mut peer, &rpc).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, mut events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let batch = vec![
        request("GetInputMute", Some(json!({"inputName":"Mic"}))),
        request("GetInputAudioTracks", Some(json!({"inputName":"Mic"}))),
        request("Sleep", Some(json!({"sleepMillis":125,"sleepFrames":3}))),
        request("GetInputList", Some(Value::Null)),
    ];
    let request_debug = format!("{batch:?}");
    assert!(!request_debug.contains("Mic"));
    assert!(!request_debug.contains("sleepMillis"));
    let result = handle
        .batch(
            handle.token(),
            batch,
            BatchOptions {
                execution: Execution::SerialFrame,
                ..BatchOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result[1].code, 604);
    assert!(!result[1].ok);
    assert_eq!(
        result[1].comment.as_deref(),
        Some("fixture-private-comment")
    );
    let debug = format!("{result:?}");
    assert!(!debug.contains("fixture-private-comment"));
    assert!(!debug.contains("fixture-data"));
    // The reply covers the earlier event, even before receiver polling. The
    // later event is observed by the continuing idle pump, not assumed ready.
    assert!(handle.event_cursor() >= 1);
    for sequence in 1..=2 {
        assert_eq!(events.next().await.unwrap().sequence, sequence);
    }
    assert_eq!(handle.event_cursor(), 2);
    handle
        .request(handle.token(), "GetVersion", Value::Null, WAIT)
        .await
        .unwrap();
    let mut wire = captured.lock().unwrap().take().unwrap();
    wire["d"]["requestId"] = json!("outer");
    let expected = source(
        json!([
            {"requestType":"GetInputMute","requestData":{"inputName":"Mic"}},
            {"requestType":"GetInputAudioTracks","requestData":{"inputName":"Mic"}},
            {"requestType":"Sleep","requestData":{"sleepMillis":125,"sleepFrames":3}},
            {"requestType":"GetInputList","requestData":null}
        ]),
        json!({"executionType":1}),
        json!(raw.lock().unwrap().clone()),
    );
    assert_eq!(wire, expected["wire"]);
    assert_eq!(serde_json::to_value(result).unwrap(), expected["results"]);
    assert_eq!(expected["budget"], 10225);
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn halt_on_failure_accepts_only_ordered_prefix_and_preserves_item_rejection() {
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        assert_eq!(frame["d"]["executionType"], 0);
        assert_eq!(frame["d"]["haltOnFailure"], true);
        assert!(frame["d"]["requests"][0].get("requestData").is_none());
        let mut result = rows(&frame);
        result.truncate(2);
        result[1]["requestStatus"] = json!({"result":false,"code":604.0});
        result.reverse();
        batch_reply(&mut peer, &frame, result).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let result = handle
        .batch(
            handle.token(),
            vec![
                request("GetInputMute", None),
                request("GetInputAudioTracks", None),
                request("GetStats", None),
            ],
            BatchOptions {
                halt_on_failure: true,
                ..options()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert!(result[0].ok);
    assert!(!result[1].ok);
    assert_eq!(result[1].code, 604);
    assert!(handle.invalidation().is_none());
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn malformed_partial_duplicate_foreign_and_mislabeled_results_invalidate_without_replay() {
    for case in [
        "missing-id",
        "duplicate-id",
        "leading-zero-id",
        "foreign-index",
        "wrong-type",
        "partial",
        "empty",
        "foreign-outer",
        "malformed-status",
        "fractional-code",
        "negative-code",
        "wrong-op",
        "halt-success-prefix",
        "halt-after-failure",
    ] {
        let (url, peer) = fixture(move |mut peer| async move {
            let frame = receive(&mut peer).await;
            let mut result = rows(&frame);
            match case {
                "missing-id" => {
                    result[0].as_object_mut().unwrap().remove("requestId");
                }
                "duplicate-id" => result[1]["requestId"] = json!("0"),
                "leading-zero-id" => result[0]["requestId"] = json!("00"),
                "foreign-index" => result[0]["requestId"] = json!("2"),
                "wrong-type" => result[0]["requestType"] = json!("GetVersion"),
                "partial" | "halt-success-prefix" => result.truncate(1),
                "empty" => result.clear(),
                "malformed-status" => {
                    result[0]["requestStatus"] = json!({"result":"true","code":100})
                }
                "fractional-code" => result[0]["requestStatus"]["code"] = json!(100.5),
                "negative-code" => result[0]["requestStatus"]["code"] = json!(-1),
                "halt-after-failure" => result[0]["requestStatus"]["result"] = json!(false),
                _ => {}
            }
            let mut reply =
                json!({"op":9,"d":{"requestId":frame["d"]["requestId"],"results":result}});
            if case == "foreign-outer" {
                reply["d"]["requestId"] = json!("other-connection:1");
            }
            if case == "wrong-op" {
                reply["op"] = json!(7);
            }
            send(&mut peer, reply).await;
            closed(&mut peer).await;
        })
        .await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let handle = owner.handle();
        let expected = if matches!(
            case,
            "malformed-status" | "fractional-code" | "negative-code"
        ) {
            Invalidation::MalformedMessage
        } else {
            Invalidation::ResponseIdentityMismatch
        };
        let result = handle
            .batch(
                handle.token(),
                vec![
                    request("GetInputMute", None),
                    request("GetInputAudioTracks", None),
                ],
                BatchOptions {
                    halt_on_failure: case.starts_with("halt-"),
                    ..options()
                },
            )
            .await;
        assert!(
            matches!(result,Err(RequestError::Unknown(ref reason)) if *reason==expected),
            "{case}: {result:?}"
        );
        assert_eq!(invalidated(&handle).await, expected);
        assert!(matches!(
            handle
                .batch(handle.token(), vec![request("GetStats", None)], options())
                .await,
            Err(RequestError::BeforeDispatch(BeforeDispatch::Closed(_)))
        ));
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
    // Confirm this is a deliberately stricter correspondence boundary: source
    // positional fallback accepts missing IDs and unexplained partial output.
    let accepted = source(
        json!([{"requestType":"GetInputMute"},{"requestType":"GetInputAudioTracks"}]),
        json!({}),
        json!([{"requestType":"GetInputMute","requestStatus":{"result":true,"code":100}}]),
    );
    assert_eq!(accepted["results"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn shared_fifo_rechecks_batch_policy_and_abandoned_queued_work_never_sends() {
    let (seen, ready) = oneshot::channel();
    let (release, unblock) = oneshot::channel();
    let (url, peer) = fixture(move |mut peer| async move {
        let first = receive(&mut peer).await;
        assert_eq!(first["op"], 6);
        seen.send(()).unwrap();
        unblock.await.unwrap();
        rpc_reply(&mut peer, &first).await;
        let next = receive(&mut peer).await;
        assert_eq!(next["op"], 6);
        assert_eq!(next["d"]["requestType"], "GetVersion");
        rpc_reply(&mut peer, &next).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = owner.handle();
    let first_handle = handle.clone();
    let first = tokio::spawn(async move {
        first_handle
            .request(first_handle.token(), "GetStats", Value::Null, WAIT)
            .await
    });
    ready.await.unwrap();
    let guard_calls = Arc::new(AtomicUsize::new(0));
    {
        let calls = guard_calls.clone();
        let abandoned = handle.batch_checked(
            handle.token(),
            vec![request("GetInputList", None)],
            options(),
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        );
        tokio::pin!(abandoned);
        assert!(poll!(&mut abandoned).is_pending());
    }
    let expired = handle
        .batch(
            handle.token(),
            vec![request("GetInputList", None)],
            BatchOptions {
                timeout: Some(Duration::from_millis(20)),
                ..BatchOptions::default()
            },
        )
        .await;
    assert_eq!(
        expired.unwrap_err(),
        RequestError::BeforeDispatch(BeforeDispatch::Deadline)
    );
    let valid = Arc::new(AtomicBool::new(true));
    let check = valid.clone();
    let guarded = handle.batch_checked(
        handle.token(),
        vec![request("GetInputList", None)],
        options(),
        move || {
            if check.load(Ordering::Acquire) {
                Ok(())
            } else {
                Err("policy changed while queued".into())
            }
        },
    );
    tokio::pin!(guarded);
    assert!(poll!(&mut guarded).is_pending());
    valid.store(false, Ordering::Release);
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    assert_eq!(
        guarded.await.unwrap_err(),
        RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(
            "policy changed while queued".into()
        ))
    );
    assert_eq!(guard_calls.load(Ordering::SeqCst), 0);
    handle
        .request(handle.token(), "GetVersion", Value::Null, WAIT)
        .await
        .unwrap();
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn active_batch_cancel_deadline_or_lost_reply_close_the_session_after_one_dispatch() {
    for reason in ["cancel", "deadline", "disconnect"] {
        let (seen, ready) = oneshot::channel();
        let (url, peer) = fixture(move |mut peer| async move {
            let frame = receive(&mut peer).await;
            assert_eq!(frame["op"], 8);
            seen.send(()).unwrap();
            if reason == "disconnect" {
                peer.close(None).await.unwrap();
            } else {
                closed(&mut peer).await;
            }
        })
        .await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let handle = owner.handle();
        let worker = handle.clone();
        let job = tokio::spawn(async move {
            worker
                .batch(
                    worker.token(),
                    vec![request("GetInputList", None)],
                    BatchOptions {
                        timeout: Some(if reason == "deadline" {
                            Duration::from_millis(60)
                        } else {
                            WAIT
                        }),
                        ..BatchOptions::default()
                    },
                )
                .await
        });
        ready.await.unwrap();
        let expected = match reason {
            "cancel" => Invalidation::CancelledAfterDispatch,
            "deadline" => Invalidation::DeadlineAfterDispatch,
            _ => Invalidation::Disconnected,
        };
        if reason == "cancel" {
            job.abort();
            assert!(job.await.unwrap_err().is_cancelled());
        } else {
            assert!(
                matches!(job.await.unwrap(),Err(RequestError::Unknown(reason)) if reason==expected)
            );
        }
        assert_eq!(invalidated(&handle).await, expected);
        assert!(matches!(
            handle
                .batch(
                    handle.token(),
                    vec![request("GetInputList", None)],
                    options()
                )
                .await,
            Err(RequestError::BeforeDispatch(BeforeDispatch::Closed(_)))
        ));
        owner.shutdown().await.unwrap();
        joined(peer).await;
    }
}

#[tokio::test]
async fn event_overflow_during_batch_prevents_observed_success() {
    let (url, peer) = fixture(|mut peer| async move {
        let frame = receive(&mut peer).await;
        event(&mut peer, 1).await;
        event(&mut peer, 2).await;
        let _ = peer
            .send(Message::Text(
                json!({"op":9,"d":{"requestId":frame["d"]["requestId"],"results":rows(&frame)}})
                    .to_string()
                    .into(),
            ))
            .await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, mut events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            event_items: 1,
            ..SessionOptions::default()
        },
    )
    .await
    .unwrap();
    let handle = owner.handle();
    assert!(matches!(
        handle
            .batch(handle.token(), vec![request("GetStats", None)], options())
            .await,
        Err(RequestError::Unknown(Invalidation::EventOverflow))
    ));
    assert!(matches!(
        events.next().await,
        Err(Invalidation::EventOverflow)
    ));
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn batch_wire_uses_same_aggregate_byte_budget_as_active_ordinary_rpc() {
    let (seen, ready) = oneshot::channel();
    let (release, unblock) = oneshot::channel();
    let (url, peer) = fixture(move |mut peer| async move {
        let first = receive(&mut peer).await;
        assert_eq!(first["op"], 6);
        seen.send(()).unwrap();
        unblock.await.unwrap();
        rpc_reply(&mut peer, &first).await;
        let frame = receive(&mut peer).await;
        assert_eq!(frame["op"], 8);
        batch_reply(&mut peer, &frame, rows(&frame)).await;
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            request_bytes: 512,
            ..SessionOptions::default()
        },
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let worker = handle.clone();
    let first = tokio::spawn(async move {
        worker
            .request(
                worker.token(),
                "GetStats",
                json!({"payload":"a".repeat(280)}),
                WAIT,
            )
            .await
    });
    ready.await.unwrap();
    let requests = vec![request(
        "GetInputList",
        Some(json!({"payload":"b".repeat(150)})),
    )];
    assert_eq!(
        handle
            .batch(handle.token(), requests.clone(), options())
            .await
            .unwrap_err(),
        RequestError::BeforeDispatch(BeforeDispatch::ByteCapacity)
    );
    let huge = vec![request(
        "GetInputList",
        Some(json!({"payload":"x".repeat(1024)})),
    )];
    assert_eq!(
        handle
            .batch(handle.token(), huge, options())
            .await
            .unwrap_err(),
        RequestError::BeforeDispatch(BeforeDispatch::ByteCapacity)
    );
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    handle
        .batch(handle.token(), requests, options())
        .await
        .unwrap();
    owner.shutdown().await.unwrap();
    joined(peer).await;
}

#[tokio::test]
async fn batch_item_capacity_is_shared_and_shutdown_joins_active_batch() {
    let (seen, ready) = oneshot::channel();
    let (url, peer) = fixture(move |mut peer| async move {
        let frame = receive(&mut peer).await;
        assert_eq!(frame["op"], 8);
        seen.send(()).unwrap();
        closed(&mut peer).await;
    })
    .await;
    let (owner, _events) = ObsSession::connect(
        &url,
        None,
        SessionOptions {
            request_items: 1,
            ..SessionOptions::default()
        },
    )
    .await
    .unwrap();
    let handle = owner.handle();
    let worker = handle.clone();
    let job = tokio::spawn(async move {
        worker
            .batch(
                worker.token(),
                vec![request("GetInputList", None)],
                options(),
            )
            .await
    });
    ready.await.unwrap();
    assert_eq!(
        handle
            .request(handle.token(), "GetStats", Value::Null, WAIT)
            .await
            .unwrap_err(),
        RequestError::BeforeDispatch(BeforeDispatch::Capacity)
    );
    owner.shutdown().await.unwrap();
    assert!(matches!(
        job.await.unwrap(),
        Err(RequestError::Unknown(Invalidation::Shutdown))
    ));
    joined(peer).await;
}

#[tokio::test]
async fn empty_stale_invalid_and_default_timeout_controls_dispatch_no_frames() {
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
    let handle = owner.handle();
    assert!(
        handle
            .batch(
                other.token(),
                vec![],
                BatchOptions {
                    timeout: Some(Duration::ZERO),
                    ..BatchOptions::default()
                }
            )
            .await
            .unwrap()
            .is_empty()
    );
    let result = handle
        .batch(
            other.token(),
            vec![request("GetInputList", None)],
            options(),
        )
        .await;
    assert_eq!(
        result.unwrap_err(),
        RequestError::BeforeDispatch(BeforeDispatch::StaleConnection)
    );
    assert_eq!(
        handle
            .batch(
                handle.token(),
                vec![request("Invalid-Type", None)],
                options()
            )
            .await
            .unwrap_err(),
        RequestError::BeforeDispatch(BeforeDispatch::InvalidRequest)
    );
    assert_eq!(
        handle
            .batch(
                handle.token(),
                vec![request("Sleep", Some(json!({"sleepMillis":-20000})))],
                BatchOptions::default()
            )
            .await
            .unwrap_err(),
        RequestError::BeforeDispatch(BeforeDispatch::Deadline)
    );
    let requests = vec![
        request("Sleep", Some(json!({"sleepMillis":250,"sleepFrames":30}))),
        request("GetStats", Some(json!({"sleepMillis":1000}))),
    ];
    assert_eq!(
        BatchOptions::default()
            .effective_timeout(&requests)
            .unwrap(),
        Duration::from_millis(11250)
    );
    owner.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
    joined(peer).await;
    joined(other_peer).await;
}
