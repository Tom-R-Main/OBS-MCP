// SPDX-License-Identifier: GPL-2.0-only
#[path = "support/output_start_three.rs"]
mod support;
use obs_mcp_native::{
    generic_requests,
    operation_store::OperationStore,
    output_events::OutputKind,
    output_transport::{self, Context},
    receipt_broker::{ReceiptBroker, ScopeKind},
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{collections::HashSet, fs, sync::Arc, time::Duration};
use support::*;
use tokio::time::{Instant, timeout};
fn context(
    session: obs_mcp_native::session::SessionHandle,
    events: obs_mcp_native::output_events::OutputEvents,
) -> Context {
    Context {
        session,
        events,
        control: control(),
        clock: clock(),
        capabilities: None,
    }
}
fn text(value: &Value) -> &str {
    value["content"][0]["text"].as_str().unwrap()
}
async fn run(socket: &Socket, dir: &Dir, name: &str, caps: Option<HashSet<String>>) -> Value {
    let (session, bridge) = socket.connect().await;
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &socket.url).unwrap()).unwrap();
    let call = call(name);
    let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
    let mut context = context(session.handle(), bridge.output_events().unwrap());
    context.capabilities = caps.map(Arc::new);
    let result = output_transport::execute(
        port,
        call,
        context,
        "fixture".into(),
        Cancellation::new(),
        id(1),
        Instant::now() + WAIT,
    )
    .await;
    bridge.shutdown().await.unwrap();
    session.shutdown().await.unwrap();
    result
}
#[tokio::test]
async fn three_closed_starts_use_same_socket_omit_data_and_publish_only_accepted() {
    for (name, request, _, label) in STARTS {
        let socket = Socket::new().await;
        let dir = Dir::new();
        let result = run(&socket, &dir, name, None).await;
        let trace = socket.trace.lock().unwrap().clone();
        let receipt = dir.receipt(1);
        socket.join().await;
        let message = format!("{label} started, writing to: /fixture/α.mkv");
        assert_eq!(
            result,
            json!({"content":[{"type":"text","text":message}],"structuredContent":{"message":message}})
        );
        assert_eq!(trace.len(), 1);
        assert_eq!(trace[0]["d"]["requestType"], request);
        assert!(trace[0]["d"].get("requestData").is_none());
        assert_eq!(receipt["requestType"], request);
        assert_eq!(receipt["phase"], "accepted");
        assert_eq!(receipt["requestAccepted"], true);
        assert_eq!(receipt["postconditionObserved"], false);
    }
}
#[tokio::test]
async fn status_is_conditional_and_strict_with_missing_fallback_capability_allowed() {
    for (name, request, _, label) in STARTS {
        for active in [json!(true), json!("true"), json!(1), Value::Null] {
            let socket = Socket::new().await;
            let dir = Dir::new();
            socket.plan(
                request,
                Plan {
                    states: vec![json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"})],
                    ..Default::default()
                },
            );
            let status = request.replacen("Start", "Get", 1) + "Status";
            socket.plan(
                &status,
                Plan {
                    response: json!({"outputActive":active}),
                    ..Default::default()
                },
            );
            let result = run(&socket, &dir, name, None).await;
            assert_eq!(socket.count(&status), 1);
            assert_eq!(dir.receipt(1)["phase"], "accepted");
            socket.join().await;
            if active == json!(true) {
                assert_eq!(text(&result), format!("{label} started"));
                assert!(result.get("isError").is_none());
            } else {
                assert_eq!(result["isError"], true);
                assert!(
                    text(&result).contains("OBS stopped the output immediately after starting it")
                );
            }
        }
        let socket = Socket::new().await;
        let dir = Dir::new();
        let result = run(&socket, &dir, name, Some([request.to_owned()].into())).await;
        assert!(result.get("isError").is_none());
        assert_eq!(socket.trace.lock().unwrap().len(), 1);
        socket.join().await;
        let socket = Socket::new().await;
        let dir = Dir::new();
        socket.plan(
            request,
            Plan {
                states: vec![json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"})],
                ..Default::default()
            },
        );
        let result = run(&socket, &dir, name, Some([request.to_owned()].into())).await;
        assert_eq!(result["isError"], true);
        assert_eq!(socket.trace.lock().unwrap().len(), 1);
        assert_eq!(dir.receipt(1)["phase"], "accepted");
        socket.join().await;
    }
}
#[tokio::test]
async fn explicit_rejection_and_lost_ack_never_upgrade_from_started_event_or_replay() {
    for lost in [false, true] {
        let socket = Socket::new().await;
        let dir = Dir::new();
        socket.plan(
            "StartStream",
            Plan {
                rejected: !lost,
                lost,
                ..Default::default()
            },
        );
        let result = run(&socket, &dir, "obs-start-stream", None).await;
        let receipt = dir.receipt(1);
        assert_eq!(socket.count("StartStream"), 1);
        assert_eq!(socket.count("GetStreamStatus"), 0);
        assert_eq!(socket.count("StopStream"), 0);
        socket.join().await;
        assert_eq!(result["isError"], true);
        assert_eq!(receipt["phase"], if lost { "unknown" } else { "rejected" });
        if !lost {
            assert!(text(&result).contains("PRIVATE precise start refusal"));
        }
    }
}
#[tokio::test]
async fn missing_start_capability_full_registration_and_expired_owner_have_no_intent() {
    for mode in 0..3 {
        let socket = Socket::new().await;
        let dir = Dir::new();
        let (session, bridge) = socket.connect().await;
        let provider = bridge.output_events().unwrap();
        let mut slots = vec![];
        if mode == 1 {
            for _ in 0..32 {
                slots.push(
                    provider
                        .subscribe(OutputKind::Stream, Instant::now() + WAIT)
                        .unwrap(),
                );
            }
        }
        let call = call("obs-start-stream");
        let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &socket.url).unwrap()).unwrap();
        let scope = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
        let mut ctx = context(session.handle(), provider);
        if mode == 0 {
            ctx.capabilities = Some(Arc::new(HashSet::new()));
        }
        let end = if mode == 2 {
            Instant::now() - Duration::from_millis(1)
        } else {
            Instant::now() + WAIT
        };
        let result = output_transport::execute(
            scope,
            call,
            ctx,
            "fixture".into(),
            Cancellation::new(),
            id(1),
            end,
        )
        .await;
        drop(slots);
        bridge.shutdown().await.unwrap();
        session.shutdown().await.unwrap();
        assert_eq!(socket.count("StartStream"), 0);
        socket.join().await;
        assert_eq!(result["isError"], true);
        assert!(!dir.path(1).exists());
    }
}
#[tokio::test]
async fn mismatched_provider_endpoint_and_direct_store_refuse_before_intent() {
    let first = Socket::new().await;
    let second = Socket::new().await;
    let dir = Dir::new();
    let (s1, b1) = first.connect().await;
    let (s2, b2) = second.connect().await;
    assert_ne!(
        b1.output_events().unwrap().connection_token(),
        b2.output_events().unwrap().connection_token()
    );
    for mode in 0..3 {
        let call = call("obs-start-stream");
        let store =
            OperationStore::new(&dir.0, if mode == 1 { &second.url } else { &first.url }).unwrap();
        let broker = if mode == 2 {
            None
        } else {
            Some(ReceiptBroker::new(store).unwrap())
        };
        let port = if let Some(broker) = broker.as_ref() {
            broker.scope(ScopeKind::Generic(call.kind())).unwrap()
        } else {
            OperationStore::new(&dir.0, &first.url).unwrap()
        };
        let ctx = context(
            s1.handle(),
            if mode == 0 {
                b2.output_events().unwrap()
            } else {
                b1.output_events().unwrap()
            },
        );
        let result = output_transport::execute(
            port,
            call,
            ctx,
            "fixture".into(),
            Cancellation::new(),
            id(1),
            Instant::now() + WAIT,
        )
        .await;
        assert_eq!(result["isError"], true);
        assert!(!dir.path(1).exists());
    }
    b1.shutdown().await.unwrap();
    b2.shutdown().await.unwrap();
    s1.shutdown().await.unwrap();
    s2.shutdown().await.unwrap();
    assert!(first.trace.lock().unwrap().is_empty());
    assert!(second.trace.lock().unwrap().is_empty());
    first.join().await;
    second.join().await;
}
#[tokio::test]
async fn generic_ack_only_executor_cannot_execute_any_confirmed_start() {
    let socket = Socket::new().await;
    let dir = Dir::new();
    let (session, bridge) = socket.connect().await;
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &socket.url).unwrap()).unwrap();
    for (name, _, _, _) in STARTS {
        let call = call(name);
        let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
        let result = generic_requests::execute(
            port,
            call,
            session.handle(),
            control(),
            clock(),
            "fixture".into(),
            Cancellation::new(),
            id(1),
            WAIT,
            Instant::now(),
        )
        .await;
        assert_eq!(result["isError"], true);
        assert!(text(&result).contains("confirmation services are unavailable"));
        assert!(!dir.path(1).exists());
    }
    bridge.shutdown().await.unwrap();
    session.shutdown().await.unwrap();
    assert!(socket.trace.lock().unwrap().is_empty());
    socket.join().await;
}
#[tokio::test]
async fn queued_lease_change_refuses_actual_send_and_preserves_whole_source_policy_body() {
    let socket = Socket::new().await;
    let dir = Dir::new();
    socket.plan(
        "GetVersion",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let (session, bridge) = socket.connect().await;
    let handle = session.handle();
    let held = tokio::spawn(async move {
        handle
            .request(handle.token(), "GetVersion", Value::Null, WAIT)
            .await
    });
    socket.seen("GetVersion", 1).await;
    let control = control();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &socket.url).unwrap()).unwrap();
    let call = call("obs-start-stream");
    let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
    let mut ctx = context(session.handle(), bridge.output_events().unwrap());
    ctx.control = control.clone();
    let job = tokio::spawn(output_transport::execute(
        port,
        call,
        ctx,
        "fixture".into(),
        Cancellation::new(),
        id(1),
        Instant::now() + WAIT,
    ));
    timeout(WAIT, async {
        while !dir.path(1).exists() || dir.receipt(1)["phase"] != "dispatching" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    control
        .lock()
        .unwrap()
        .claim("human", 5, "manual", EPOCH)
        .unwrap();
    socket.release("GetVersion").await;
    held.await.unwrap().unwrap();
    let result = job.await.unwrap();
    bridge.shutdown().await.unwrap();
    session.shutdown().await.unwrap();
    assert_eq!(socket.count("StartStream"), 0);
    socket.join().await;
    assert_eq!(dir.receipt(1)["phase"], "not-dispatched");
    assert_eq!(result["isError"], true);
    assert!(text(&result).contains("human"));
    assert!(!text(&result).starts_with("Streaming failed to start"));
}
#[tokio::test]
async fn known_ack_publication_fault_remains_uncertain_without_claiming_observed_or_replaying() {
    let socket = Socket::new().await;
    let dir = Dir::new();
    socket.plan(
        "StartStream",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let (session, bridge) = socket.connect().await;
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &socket.url).unwrap()).unwrap();
    let call = call("obs-start-stream");
    let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
    let job = tokio::spawn(output_transport::execute(
        port,
        call,
        context(session.handle(), bridge.output_events().unwrap()),
        "fixture".into(),
        Cancellation::new(),
        id(1),
        Instant::now() + WAIT,
    ));
    socket.seen("StartStream", 1).await;
    let original = dir.0.join("original-receipts");
    fs::rename(dir.0.join("operation-receipts"), &original).unwrap();
    fs::create_dir(dir.0.join("operation-receipts")).unwrap();
    socket.release("StartStream").await;
    let result = job.await.unwrap();
    let backup: Value =
        serde_json::from_slice(&fs::read(original.join(format!("{}.json", id(1)))).unwrap())
            .unwrap();
    fs::remove_dir(dir.0.join("operation-receipts")).unwrap();
    fs::rename(&original, dir.0.join("operation-receipts")).unwrap();
    assert!(broker.admission().is_err());
    bridge.shutdown().await.unwrap();
    session.shutdown().await.unwrap();
    assert_eq!(socket.count("StartStream"), 1);
    assert_eq!(socket.count("StopStream"), 0);
    socket.join().await;
    assert_eq!(backup["phase"], "dispatching");
    assert_eq!(backup["postconditionObserved"], false);
    assert_eq!(result["isError"], true);
    assert!(text(&result).contains("receipt outcome is uncertain"));
}

fn source_callback(name: &str, stopped: bool, rejected: bool, legacy: bool) -> Value {
    use std::{
        io::Write,
        path::Path,
        process::{Command, Stdio},
    };
    let script = r#"
import{Client}from'@modelcontextprotocol/client';import{StdioClientTransport,getDefaultEnvironment}from'@modelcontextprotocol/client/stdio';import{mkdtempSync,chmodSync,rmSync}from'node:fs';import{join}from'node:path';
let raw='';for await(const part of process.stdin)raw+=part;const input=JSON.parse(raw);const configuration=JSON.stringify(input);
const serverCode=`import{McpServer}from'@modelcontextprotocol/server';import{serveStdio}from'@modelcontextprotocol/server/stdio';import{EventEmitter}from'node:events';import{initialize}from'./build/tools/index.js';import{OBSRequestError}from'./build/client.js';const input=${configuration},obs=new EventEmitter(),events={StartStream:'StreamStateChanged',StartVirtualCam:'VirtualcamStateChanged',StartReplayBuffer:'ReplayBufferStateChanged'};Object.assign(obs,{sendRequest:async(requestType,requestData)=>{process.stderr.write('TRACE '+JSON.stringify({requestType,...(requestData===undefined?{}:{requestData})})+'\\n');if(events[requestType]){if(input.rejected)throw new OBSRequestError(requestType,500,'PRIVATE precise start refusal');obs.emit(events[requestType],{outputState:input.stopped?'OBS_WEBSOCKET_OUTPUT_STOPPED':'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:'/fixture/α.mkv'});return{};}return{outputActive:false};},supportsRequest:()=>true,isConnected:()=>true,getConnectionStatus:()=>({generation:1,identified:true,url:'ws://localhost:4455',hasPassword:false})});serveStdio(()=>{const server=new McpServer({name:'start-three-source',version:'1'});initialize(server,obs,{confirmLive:false,resourcesAndPrompts:false,sessionNotifications:false});return server;});`;
const state=mkdtempSync(join(process.cwd(),'native/target/start3-source-'));chmodSync(state,0o700);const client=new Client({name:'fixture',version:'1'},{versionNegotiation:{mode:input.legacy?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:process.execPath,args:['--input-type=module','-e',serverCode],cwd:process.cwd(),env:{...getDefaultEnvironment(),OBS_MCP_STATE_DIR:state,OBS_MCP_CONFIRM_LIVE:'false'},stderr:'pipe'});let diagnostic='';transport.stderr?.on('data',part=>diagnostic+=String(part));let result;try{await client.connect(transport);result=await client.callTool({name:input.name,arguments:{}});delete result._meta;}finally{await client.close();await transport.close();rmSync(state,{recursive:true,force:true});}console.log(JSON.stringify({result,trace:diagnostic.split('\n').filter(line=>line.startsWith('TRACE ')).map(line=>JSON.parse(line.slice(6)))}));
"#;
    let mut child = Command::new("node")
        .args(["--input-type=module", "-e", script])
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            serde_json::to_string(
                &json!({"name":name,"stopped":stopped,"rejected":rejected,"legacy":legacy}),
            )
            .unwrap()
            .as_bytes(),
        )
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "Source child {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
#[tokio::test]
async fn actual_source_sdk_whole_callback_results_match_for_three_modes_and_both_eras() {
    for legacy in [false, true] {
        for (name, request, _, _) in STARTS {
            for mode in 0..3 {
                let expected = source_callback(name, mode == 1, mode == 2, legacy);
                let socket = Socket::new().await;
                let dir = Dir::new();
                socket.plan(request,Plan{states:vec![json!({"outputState":if mode==1{"OBS_WEBSOCKET_OUTPUT_STOPPED"}else{"OBS_WEBSOCKET_OUTPUT_STARTED"},"outputPath":"/fixture/α.mkv"})],rejected:mode==2,..Default::default()});
                if mode == 1 {
                    socket.plan(
                        &(request.replacen("Start", "Get", 1) + "Status"),
                        Plan {
                            response: json!({"outputActive":false}),
                            ..Default::default()
                        },
                    );
                }
                let result = run(&socket, &dir, name, None).await;
                let trace = socket
                    .trace
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|frame| {
                        let mut data = frame["d"].clone();
                        data.as_object_mut().unwrap().remove("requestId");
                        data
                    })
                    .collect::<Vec<_>>();
                socket.join().await;
                assert_eq!(
                    result, expected["result"],
                    "{name} mode{mode} legacy{legacy}"
                );
                assert_eq!(json!(trace), expected["trace"]);
            }
        }
    }
}

#[tokio::test]
async fn actual_receipt_owner_retains_timely_prefix_after_ack_beyond_confirmation_cutoff() {
    use obs_mcp_native::{
        session::{ObsSession, SessionOptions},
        session_observation::ObservationBridge,
    };
    use std::sync::{Condvar, Mutex, OnceLock};
    use tokio::sync::{Notify, oneshot};
    struct Gate {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
        entered: Notify,
    }
    static GATE: OnceLock<Arc<Gate>> = OnceLock::new();
    fn filter(name: &str) -> bool {
        if name == "OutputStartThreeContractGate" {
            let gate = GATE.get().unwrap();
            let mut state = gate.state.lock().unwrap();
            state.0 = true;
            gate.entered.notify_waiters();
            while !state.1 {
                state = gate.changed.wait(state).unwrap();
            }
        }
        false
    }
    struct Thread {
        stop: Option<oneshot::Sender<()>>,
        join: Option<std::thread::JoinHandle<()>>,
        gate: Arc<Gate>,
    }
    impl Thread {
        fn release(&self) {
            let mut state = self.gate.state.lock().unwrap();
            state.1 = true;
            self.gate.changed.notify_all();
        }
    }
    impl Drop for Thread {
        fn drop(&mut self) {
            self.release();
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            if let Some(join) = self.join.take() {
                join.join().unwrap();
            }
        }
    }
    let gate = GATE
        .get_or_init(|| {
            Arc::new(Gate {
                state: Mutex::new((false, false)),
                changed: Condvar::new(),
                entered: Notify::new(),
            })
        })
        .clone();
    let socket = Socket::new().await;
    socket.plan(
        "StartVirtualCam",
        Plan {
            states: vec![],
            ..Default::default()
        },
    );
    let dir = Dir::new();
    let (session, events) = ObsSession::connect(&socket.url, None, SessionOptions::default())
        .await
        .unwrap();
    let handle = session.handle();
    let (stop, stopped) = oneshot::channel();
    let (send, ready) = oneshot::channel();
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let bridge =
                    ObservationBridge::bind_with_event_filter(handle, events, None, filter)
                        .unwrap();
                send.send(bridge)
                    .unwrap_or_else(|_| panic!("bridge receiver unavailable"));
                let _ = stopped.await;
            });
    });
    let thread = Thread {
        stop: Some(stop),
        join: Some(worker),
        gate: gate.clone(),
    };
    let bridge = ready.await.unwrap();
    let call = call("obs-start-virtual-cam");
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &socket.url).unwrap()).unwrap();
    let port = broker.scope(ScopeKind::Generic(call.kind())).unwrap();
    let at = Instant::now();
    let job = tokio::spawn(output_transport::execute(
        port,
        call,
        context(session.handle(), bridge.output_events().unwrap()),
        "fixture".into(),
        Cancellation::new(),
        id(1),
        at + Duration::from_secs(10),
    ));
    socket.seen("StartVirtualCam", 1).await;
    socket.emit("OutputStartThreeContractGate", json!({})).await;
    timeout(WAIT, async {
        loop {
            let entered = gate.entered.notified();
            if gate.state.lock().unwrap().0 {
                break;
            }
            entered.await;
        }
    })
    .await
    .unwrap();
    socket.emit("VirtualcamStateChanged",json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":"/fixture/physically-timely.mkv"})).await;
    let handle = session.handle();
    handle
        .request(handle.token(), "GetVersion", Value::Null, WAIT)
        .await
        .unwrap();
    tokio::time::sleep_until(at + Duration::from_millis(5_200)).await;
    let stayed_pending = !job.is_finished();
    thread.release();
    let result = timeout(WAIT, job).await.unwrap().unwrap();
    bridge.shutdown().await.unwrap();
    session.shutdown().await.unwrap();
    drop(thread);
    assert_eq!(socket.count("GetVirtualCamStatus"), 0);
    socket.join().await;
    assert!(
        stayed_pending,
        "confirmation cutoff must not cancel original prefix settlement"
    );
    assert_eq!(
        result["content"][0]["text"],
        "Virtual camera started, writing to: /fixture/physically-timely.mkv"
    );
    assert_eq!(dir.receipt(1)["phase"], "accepted");
    assert_eq!(dir.receipt(1)["postconditionObserved"], false);
}

#[tokio::test]
async fn standalone_start_consumes_only_current_slot_and_never_borrows_take_stop_headroom() {
    use obs_mcp_native::operation_store::{
        GenericKind, GenericPhase, OperationKind, OperationUpdate, Phase, PrepareDetails,
    };
    for reserved_take in [false, true] {
        let socket = Socket::new().await;
        let dir = Dir::new();
        let mut seed = OperationStore::new(&dir.0, &socket.url).unwrap();
        let first = seed
            .prepare_generic(
                &id(10_000),
                GenericKind::InputMute,
                1,
                "2026-10-31T12:00:00.000Z",
            )
            .unwrap();
        let first = seed
            .update_generic(
                &first,
                GenericPhase::Dispatching,
                "2026-10-31T12:00:00.000Z",
            )
            .unwrap();
        let template = seed
            .update_generic(&first, GenericPhase::Accepted, "2026-10-31T12:00:00.000Z")
            .unwrap()
            .to_value();
        drop(seed);
        let count = if reserved_take { 1022 } else { 1023 };
        for n in 1..count {
            let mut row = template.clone();
            row["operationId"] = json!(id(20_000 + n));
            fs::write(dir.path(20_000 + n), serde_json::to_vec(&row).unwrap()).unwrap();
        }
        let broker = ReceiptBroker::new(OperationStore::new(&dir.0, &socket.url).unwrap()).unwrap();
        let mut take = if reserved_take {
            Some(broker.scope(ScopeKind::Take).unwrap())
        } else {
            None
        };
        if let Some(take) = take.as_mut() {
            let started = take
                .prepare(
                    &id(50_000),
                    OperationKind::RecordStart,
                    1,
                    "2026-10-31T12:00:00.000Z",
                    PrepareDetails::default(),
                )
                .unwrap();
            let started = take
                .update(
                    &started,
                    OperationUpdate::new(Phase::Dispatching),
                    "2026-10-31T12:00:00.000Z",
                )
                .unwrap();
            let mut accepted = OperationUpdate::new(Phase::Accepted);
            accepted.request_dispatched = Some(Some(true));
            accepted.request_accepted = Some(Some(true));
            let started = take
                .update(&started, accepted, "2026-10-31T12:00:00.000Z")
                .unwrap();
            let mut observed = OperationUpdate::new(Phase::Observed);
            observed.postcondition_observed = Some(true);
            take.update(&started, observed, "2026-10-31T12:00:00.000Z")
                .unwrap();
        }
        let (session, bridge) = socket.connect().await;
        let start = call("obs-start-stream");
        let port = broker.scope(ScopeKind::Generic(start.kind())).unwrap();
        let result = output_transport::execute(
            port,
            start,
            context(session.handle(), bridge.output_events().unwrap()),
            "fixture".into(),
            Cancellation::new(),
            id(1),
            Instant::now() + WAIT,
        )
        .await;
        if let Some(take) = take.as_mut() {
            assert_eq!(result["isError"], true);
            assert!(!dir.path(1).exists());
            assert_eq!(socket.count("StartStream"), 0);
            take.prepare(
                &id(50_001),
                OperationKind::RecordStop,
                1,
                "2026-10-31T12:00:00.000Z",
                PrepareDetails::default(),
            )
            .unwrap();
        } else {
            assert!(result.get("isError").is_none());
            assert_eq!(dir.receipt(1)["phase"], "accepted");
            let stop = call("obs-stop-stream");
            let port = broker
                .scope(ScopeKind::SafetyStop(
                    obs_mcp_native::session::SafetyStopRequest::Stream,
                ))
                .unwrap();
            let refusal = generic_requests::execute(
                port,
                stop,
                session.handle(),
                control(),
                clock(),
                "fixture".into(),
                Cancellation::new(),
                id(2),
                WAIT,
                Instant::now(),
            )
            .await;
            assert_eq!(refusal["isError"], true);
            assert!(!dir.path(2).exists());
            assert_eq!(socket.count("StopStream"), 0);
        }
        assert_eq!(broker.read_view().summary()["total"], 1024);
        bridge.shutdown().await.unwrap();
        session.shutdown().await.unwrap();
        socket.join().await;
    }
}
