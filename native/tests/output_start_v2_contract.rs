// SPDX-License-Identifier: GPL-2.0-only
//! Additive helper composition controls; no Service or public Start admission.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    output_events::{
        MAX_SUBSCRIPTIONS, OutputEventError as Error, OutputKind, PATH_BYTES, Terminal,
    },
    session::{EventReceiver, ObsSession, SessionHandle, SessionOptions},
    session_observation::ObservationBridge,
};
use obs_mcp_native::{
    output_start::OutputFuture,
    output_start_v2::{
        self, Detail, OutputIo, RpcFailure, RpcFate, Timing, TransportFate, Unconfirmed,
    },
};
use serde_json::{Value, json};
use std::{path::Path, process::Command as ProcessCommand};
use std::{
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const WATCHDOG: Duration = Duration::from_secs(5);
enum Command {
    Emit(&'static str, Value, oneshot::Sender<()>),
    Close(oneshot::Sender<()>),
    Ack(oneshot::Sender<()>),
}
struct SocketFixture {
    owner: ObsSession,
    events: EventReceiver,
    commands: mpsc::Sender<Command>,
    peer: JoinHandle<()>,
    connections: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
}
impl SocketFixture {
    async fn new(options: SessionOptions, start_events: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let counted = connections.clone();
        let requests = Arc::new(AtomicUsize::new(0));
        let request_count = requests.clone();
        let (commands, mut receive) = mpsc::channel::<Command>(8);
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            counted.fetch_add(1, Ordering::SeqCst);
            let mut socket = accept_async(stream).await.unwrap();
            socket
                .send(Message::Text(
                    json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let identify = socket.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(identify.to_text().unwrap()).unwrap()["op"],
                1
            );
            socket
                .send(Message::Text(
                    json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let mut pending_start: Option<Value> = None;
            loop {
                tokio::select! {
                    command=receive.recv()=>match command {
                        Some(Command::Emit(name,data,done))=>{
                            let intent=if name=="InputVolumeMeters" {1<<16}else{1<<6};
                            if socket.send(Message::Text(json!({"op":5,"d":{"eventType":name,"eventIntent":intent,"eventData":data}}).to_string().into())).await.is_err(){break;}
                            let _=done.send(());
                        },
                        Some(Command::Ack(done))=>{let frame=pending_start.take().expect("actual Start ACK must be held");if socket.send(Message::Text(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":frame["d"]["requestType"],"requestStatus":{"result":true,"code":100},"responseData":{}}}).to_string().into())).await.is_err(){break;}let _=done.send(());},
                        Some(Command::Close(done))=>{let _=socket.send(Message::Close(None)).await;let _=done.send(());break;},
                        None=>break,
                    },
                    packet=socket.next()=>match packet {
                        Some(Ok(Message::Text(text)))=>{
                            let frame:Value=serde_json::from_str(&text).unwrap();assert_eq!(frame["op"],6);request_count.fetch_add(1,Ordering::SeqCst);
                            if start_events && frame["d"]["requestType"]=="StartRecord" {
                                for state in ["OBS_WEBSOCKET_OUTPUT_STARTED","OBS_WEBSOCKET_OUTPUT_STOPPED"] {
                                    socket.send(Message::Text(json!({"op":5,"d":{"eventType":"RecordStateChanged","eventIntent":64,"eventData":{"outputState":state,"outputPath":"/fixture/first.mkv"}}}).to_string().into())).await.unwrap();
                                }
                            }
                            if start_events && frame["d"]["requestType"]=="StartRecord" {pending_start=Some(frame);continue;}
                            if socket.send(Message::Text(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":frame["d"]["requestType"],"requestStatus":{"result":true,"code":100},"responseData":{}}}).to_string().into())).await.is_err(){break;}
                        },
                        Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                        _=>{},
                    }
                }
            }
        });
        let (owner, events) = ObsSession::connect(&url, None, options).await.unwrap();
        Self {
            owner,
            events,
            commands,
            peer,
            connections,
            requests,
        }
    }
}
struct Fixture {
    owner: ObsSession,
    bridge: ObservationBridge,
    commands: mpsc::Sender<Command>,
    peer: JoinHandle<()>,
    connections: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
}
impl Fixture {
    async fn new(options: SessionOptions, start_events: bool) -> Self {
        let SocketFixture {
            owner,
            events,
            commands,
            peer,
            connections,
            requests,
        } = SocketFixture::new(options, start_events).await;
        let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
        Self {
            owner,
            bridge,
            commands,
            peer,
            connections,
            requests,
        }
    }
    async fn emit(&self, name: &'static str, data: Value) {
        emit(&self.commands, name, data).await;
    }
    async fn observed(&self) {
        received(&self.owner.handle()).await;
        self.bridge
            .synchronize(Instant::now() + WATCHDOG)
            .await
            .unwrap();
    }
    async fn close(self) {
        self.owner.shutdown().await.unwrap();
        self.bridge.shutdown().await.unwrap();
        drop(self.commands);
        self.peer.await.unwrap();
        assert_eq!(self.connections.load(Ordering::SeqCst), 1);
    }
}
async fn emit(commands: &mpsc::Sender<Command>, name: &'static str, data: Value) {
    let (done, wait) = oneshot::channel();
    commands
        .send(Command::Emit(name, data, done))
        .await
        .unwrap();
    timeout(WATCHDOG, wait).await.unwrap().unwrap();
}
async fn received(handle: &SessionHandle) {
    handle
        .request(handle.token(), "GetVersion", json!({}), WATCHDOG)
        .await
        .unwrap();
}
fn started(path: Value) -> Value {
    json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":path})
}

struct Gate {
    entered: Notify,
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            open: Mutex::new(false),
            changed: Condvar::new(),
        }
    }
    fn hold(&self) {
        self.entered.notify_one();
        let guard = self.open.lock().unwrap();
        let (_guard, result) = self
            .changed
            .wait_timeout_while(guard, WATCHDOG, |open| !*open)
            .unwrap();
        assert!(
            !result.timed_out(),
            "owned test observer gate exceeded watchdog"
        );
    }
    fn arm(&self) {
        *self.open.lock().unwrap() = false;
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
static GATE: OnceLock<Arc<Gate>> = OnceLock::new();
fn held_filter(name: &str) -> bool {
    if name == "OutputStartV2ContractHold" {
        GATE.get().unwrap().hold();
    }
    false
}
struct BridgeThread {
    finish: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    gate: Arc<Gate>,
}
impl BridgeThread {
    fn join(mut self) {
        self.gate.release();
        self.finish.take().unwrap().send(()).unwrap();
        self.thread.take().unwrap().join().unwrap();
    }
}
impl Drop for BridgeThread {
    fn drop(&mut self) {
        self.gate.release();
        if let Some(finish) = self.finish.take() {
            let _ = finish.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(Clone)]
struct TestIo {
    handle: SessionHandle,
    commands: mpsc::Sender<Command>,
    input: Value,
    trace: Arc<Mutex<Vec<String>>>,
    deadlines: Arc<Mutex<Vec<Instant>>>,
    actual: bool,
}
impl TestIo {
    fn new(f: &Fixture, input: Value, actual: bool) -> Self {
        Self {
            handle: f.owner.handle(),
            commands: f.commands.clone(),
            input,
            trace: Arc::new(Mutex::new(vec![])),
            deadlines: Arc::new(Mutex::new(vec![])),
            actual,
        }
    }
}
fn rpc_error(kind: &str, error: obs_mcp_native::session::RequestError) -> RpcFailure {
    let fate = if let Some(code) = error.rejection_code() {
        RpcFate::Rejected { code }
    } else if matches!(
        error,
        obs_mcp_native::session::RequestError::BeforeDispatch(_)
    ) {
        RpcFate::NotDispatched
    } else {
        RpcFate::Unknown
    };
    let message = error
        .obs_failure_message(kind)
        .unwrap_or_else(|| format!("fixture {kind} transport unavailable"));
    RpcFailure::new(fate, message)
}
impl OutputIo for TestIo {
    fn start(&self, kind: OutputKind, deadline: Instant) -> OutputFuture<Result<(), RpcFailure>> {
        let this = self.clone();
        let def = output_start_v2::definition(kind);
        self.trace.lock().unwrap().push(def.request_type.into());
        self.deadlines.lock().unwrap().push(deadline);
        Box::pin(async move {
            if this.actual {
                let duration = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default();
                this.handle
                    .request(this.handle.token(), def.request_type, Value::Null, duration)
                    .await
                    .map(|_| ())
                    .map_err(|e| rpc_error(def.request_type, e))
            } else {
                if let Some(error) = this.input["error"].as_str() {
                    return Err(RpcFailure::new(RpcFate::Rejected { code: 500 }, error));
                }
                if let Some(events) = this.input["events"].as_array() {
                    for event in events {
                        emit(&this.commands, kind.event_name(), event.clone()).await;
                    }
                }
                if let Some(error) = this.input["errorAfterEvents"].as_str() {
                    return Err(RpcFailure::new(RpcFate::Unknown, error));
                }
                if this.input["ackLate"] == true {
                    tokio::time::sleep(Duration::from_millis(90)).await;
                }
                Ok(())
            }
        })
    }
    fn status(
        &self,
        kind: OutputKind,
        deadline: Instant,
    ) -> OutputFuture<Result<Value, RpcFailure>> {
        let this = self.clone();
        let def = output_start_v2::definition(kind);
        self.trace.lock().unwrap().push(def.status_request.into());
        self.deadlines.lock().unwrap().push(deadline);
        Box::pin(async move {
            if this.actual {
                let duration = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default();
                this.handle
                    .request(
                        this.handle.token(),
                        def.status_request,
                        Value::Null,
                        duration,
                    )
                    .await
                    .map_err(|e| rpc_error(def.status_request, e))
            } else if this.input["statusError"] == true {
                Err(RpcFailure::new(RpcFate::Unknown, "status fixture failed"))
            } else if this.input["statusUnsupported"] == true {
                Err(RpcFailure::new(
                    RpcFate::NotDispatched,
                    "status capability unavailable",
                ))
            } else {
                Ok(this.input["status"].clone())
            }
        })
    }
}
fn oracle(input: &Value, kind: OutputKind) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut input = input.clone();
    input["definitionKey"] = json!(match kind {
        OutputKind::Record => "RECORD_OUTPUT",
        OutputKind::Stream => "STREAM_OUTPUT",
        OutputKind::VirtualCam => "VIRTUAL_CAM_OUTPUT",
        OutputKind::ReplayBuffer => "REPLAY_BUFFER_OUTPUT",
    });
    let result=ProcessCommand::new("node").arg("--input-type=module").arg("-e").arg(r#"
import{EventEmitter}from'node:events';import*as output from'./build/tools/output-start.js';
const input=JSON.parse(process.env.OUTPUT_START_V2_TEST_INPUT),trace=[],sourceDefinition=output[input.definitionKey];input.definition={...sourceDefinition,timeoutMs:60};class Client extends EventEmitter{on(name,fn){trace.push('on:'+name);return super.on(name,fn)}async sendRequest(type){trace.push(type);if(type===input.definition.requestType){if(input.error)throw Error(input.error);for(const data of input.events??[])this.emit(input.definition.stateEvent,data);if(input.errorAfterEvents)throw Error(input.errorAfterEvents);if(input.ackLate)await new Promise(r=>setTimeout(r,90));return{};}if(input.statusError)throw Error('status fixture failed');if(input.statusUnsupported)throw Error('status capability unavailable');return input.status;}}
const client=new Client();const result=await output.startOutputAndConfirm(client,input.definition,()=>{trace.push('accepted');if(input.acceptError)throw Error(input.acceptError)});if(client.listenerCount(input.definition.stateEvent)!==0)throw Error('Source observer leaked');console.log(JSON.stringify({result,trace,sourceDefinition}));
"#).current_dir(root).env("OUTPUT_START_V2_TEST_INPUT",input.to_string()).output().unwrap();
    assert!(
        result.status.success(),
        "Source helper oracle failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}
#[tokio::test]
async fn whole_source_helper_results_and_effect_order_match_for_all_four_closed_kinds() {
    for kind in [
        OutputKind::Record,
        OutputKind::Stream,
        OutputKind::VirtualCam,
        OutputKind::ReplayBuffer,
    ] {
        for input in [
            json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":"/fixture/first.mkv"},{"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"}]}),
            json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":""}],"statusUnsupported":true}),
            json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":"0\n日本語💡"}]}),
            json!({"events":[null,{},false,{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTING"},{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":17}],"ackLate":true}),
            json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"},{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED"}],"status":{"outputActive":false}}),
            json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"}],"status":{"outputActive":true}}),
            json!({"events":[],"status":{"outputActive":true}}),
            json!({"events":[],"status":{"outputActive":"true"}}),
            json!({"events":[],"status":{"outputActive":1}}),
            json!({"events":[],"status":null}),
            json!({"events":[],"status":[]}),
            json!({"events":[],"statusError":true}),
            json!({"events":[],"statusUnsupported":true}),
            json!({"error":"fixture rejected"}),
            json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED"}],"errorAfterEvents":"fixture lost ACK"}),
            json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED"}],"acceptError":"fixture acceptance publication failed"}),
        ] {
            let expected = oracle(&input, kind);
            let def = output_start_v2::definition(kind);
            assert_eq!(def.label, expected["sourceDefinition"]["label"]);
            assert_eq!(
                def.request_type,
                expected["sourceDefinition"]["requestType"]
            );
            assert_eq!(
                def.status_request,
                expected["sourceDefinition"]["statusRequest"]
            );
            assert_eq!(def.state_event, expected["sourceDefinition"]["stateEvent"]);
            assert_eq!(
                kind.event_name(),
                expected["sourceDefinition"]["stateEvent"]
            );
            assert_eq!(
                def.timeout.as_millis(),
                expected["sourceDefinition"]["timeoutMs"].as_u64().unwrap() as u128
            );
            let f = Fixture::new(Default::default(), false).await;
            let events = f.bridge.output_events().unwrap();
            let io = TestIo::new(&f, input.clone(), false);
            let trace = io.trace.clone();
            let timing = Timing {
                operation_deadline: Instant::now() + WATCHDOG,
                confirmation_timeout: Duration::from_millis(60),
                rpc_timeout: Duration::from_secs(1),
            };
            let execution = output_start_v2::execute(&io, &events, kind, timing, || {
                trace.lock().unwrap().push("accepted".into());
                input["acceptError"]
                    .as_str()
                    .map_or(Ok(()), |e| Err(e.into()))
            })
            .await;
            assert_eq!(
                execution.result, expected["result"],
                "kind {kind:?}, input {input}"
            );
            let wanted = expected["trace"]
                .as_array()
                .unwrap()
                .iter()
                .skip(1)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(json!(*io.trace.lock().unwrap()), json!(wanted));
            if input.get("error").is_some() {
                assert_eq!(execution.fate, TransportFate::Rejected { code: 500 });
            } else if input.get("errorAfterEvents").is_some() {
                assert_eq!(execution.fate, TransportFate::Unknown);
            } else {
                assert_eq!(execution.fate, TransportFate::Accepted);
            }
            if input.get("acceptError").is_some() {
                assert_eq!(execution.detail, Detail::AckHookFailed);
            }
            let mut slots = vec![];
            for _ in 0..MAX_SUBSCRIPTIONS {
                slots.push(events.subscribe(kind, Instant::now() + WATCHDOG).unwrap());
            }
            drop(slots);
            f.close().await;
        }
    }
}
async fn threaded_fixture() -> (BridgeThread, Fixture) {
    let gate = GATE.get_or_init(|| Arc::new(Gate::new())).clone();
    let SocketFixture {
        owner,
        events,
        commands,
        peer,
        connections,
        requests,
    } = SocketFixture::new(Default::default(), false).await;
    let handle = owner.handle();
    let (finish, finished) = oneshot::channel();
    let (built, ready) = oneshot::channel();
    let worker_gate = gate.clone();
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let bridge =
                ObservationBridge::bind_with_event_filter(handle, events, None, held_filter)
                    .unwrap();
            built
                .send(bridge)
                .unwrap_or_else(|_| panic!("observer receiver dropped"));
            let _ = finished.await;
        });
        worker_gate.release();
    });
    let worker = BridgeThread {
        finish: Some(finish),
        thread: Some(thread),
        gate,
    };
    let bridge = ready.await.unwrap();
    (
        worker,
        Fixture {
            owner,
            bridge,
            commands,
            peer,
            connections,
            requests,
        },
    )
}
#[tokio::test]
async fn actual_helper_settles_timely_prefix_after_early_ack_or_reports_typed_original_bound() {
    for short in [false, true] {
        let (worker, f) = threaded_fixture().await;
        let gate = worker.gate.clone();
        let outputs = f.bridge.output_events().unwrap();
        let io = TestIo::new(&f, json!({}), true);
        let trace = io.trace.clone();
        gate.arm();
        f.emit("OutputStartV2ContractHold", json!({})).await;
        timeout(WATCHDOG, gate.entered.notified()).await.unwrap();
        let now = Instant::now();
        let confirmation = if short {
            Duration::from_millis(60)
        } else {
            Duration::from_millis(200)
        };
        let original = now
            + if short {
                Duration::from_millis(150)
            } else {
                WATCHDOG
            };
        let (accepted, ack) = oneshot::channel();
        let timing = Timing {
            operation_deadline: original,
            confirmation_timeout: confirmation,
            rpc_timeout: Duration::from_secs(1),
        };
        let job = tokio::spawn(async move {
            output_start_v2::execute(&io, &outputs, OutputKind::Record, timing, move || {
                let _ = accepted.send(());
                Ok(())
            })
            .await
        });
        timeout(WATCHDOG, ack).await.unwrap().unwrap();
        let ack_at = Instant::now();
        assert!(ack_at < now + confirmation, "fixture missed early ACK");
        f.emit("RecordStateChanged", started(json!("timely"))).await;
        received(&f.owner.handle()).await;
        assert!(
            Instant::now() < now + confirmation,
            "fixture missed timely physical event"
        );
        tokio::time::sleep_until(ack_at + confirmation + Duration::from_millis(10)).await;
        let remained_pending = !job.is_finished();
        let execution = if short {
            let execution = timeout(WATCHDOG, job).await.unwrap().unwrap();
            gate.release();
            execution
        } else {
            gate.release();
            timeout(WATCHDOG, job).await.unwrap().unwrap()
        };
        f.close().await;
        worker.join();
        assert!(
            remained_pending,
            "legacy outer confirmation timeout would discard this unsettled prefix"
        );
        assert_eq!(execution.fate, TransportFate::Accepted);
        assert_eq!(*trace.lock().unwrap(), vec!["StartRecord"]);
        if short {
            assert_eq!(
                execution.detail,
                Detail::ObservationUnavailable(Error::SettlementUnavailable)
            );
            assert_eq!(execution.result["isError"], true);
        } else {
            assert_eq!(execution.detail, Detail::StartedEvent);
            assert_eq!(
                execution.result["content"][0]["text"],
                "Recording started, writing to: timely"
            );
        }
    }
}
#[tokio::test]
async fn actual_predeadline_first_terminal_survives_late_physical_ack_without_fallback() {
    let f = Fixture::new(Default::default(), true).await;
    let io = TestIo::new(&f, json!({}), true);
    let trace = io.trace.clone();
    let outputs = f.bridge.output_events().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let marked = accepted.clone();
    let timing = Timing {
        operation_deadline: Instant::now() + WATCHDOG,
        confirmation_timeout: Duration::from_millis(60),
        rpc_timeout: Duration::from_secs(1),
    };
    let job = tokio::spawn(async move {
        output_start_v2::execute(&io, &outputs, OutputKind::Record, timing, move || {
            marked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
    });
    timeout(WATCHDOG, async {
        while f.owner.handle().event_cursor() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.bridge
        .synchronize(Instant::now() + WATCHDOG)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert_eq!(accepted.load(Ordering::SeqCst), 0);
    let (done, wait) = oneshot::channel();
    f.commands.send(Command::Ack(done)).await.unwrap();
    wait.await.unwrap();
    let execution = job.await.unwrap();
    assert_eq!(execution.fate, TransportFate::Accepted);
    assert_eq!(execution.detail, Detail::StartedEvent);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert_eq!(
        execution.result["content"][0]["text"],
        "Recording started, writing to: /fixture/first.mkv"
    );
    assert_eq!(*trace.lock().unwrap(), vec!["StartRecord"]);
    f.close().await;
}
#[tokio::test]
async fn registration_and_expired_timing_refuse_before_io_and_debug_never_formats_mutated_values() {
    for kind in [
        OutputKind::Record,
        OutputKind::Stream,
        OutputKind::VirtualCam,
        OutputKind::ReplayBuffer,
    ] {
        let timing = Timing::source(kind, Instant::now() + WATCHDOG);
        assert_eq!(
            timing.confirmation_timeout,
            Duration::from_secs(if kind == OutputKind::Stream { 15 } else { 5 })
        );
        assert_eq!(timing.rpc_timeout, Duration::from_secs(10));
    }
    let f = Fixture::new(Default::default(), false).await;
    let events = f.bridge.output_events().unwrap();
    let io = TestIo::new(&f, json!({}), false);
    let mut held = vec![];
    for _ in 0..MAX_SUBSCRIPTIONS {
        held.push(
            events
                .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
                .unwrap(),
        );
    }
    let accepted = AtomicUsize::new(0);
    let mut result = output_start_v2::execute(
        &io,
        &events,
        OutputKind::Record,
        Timing::source(
            OutputKind::Record,
            Instant::now() - Duration::from_millis(1),
        ),
        || {
            accepted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .await;
    assert_eq!(result.fate, TransportFate::NotDispatched);
    assert_eq!(result.detail, Detail::OperationDeadline);
    result = output_start_v2::execute(
        &io,
        &events,
        OutputKind::Record,
        Timing::source(OutputKind::Record, Instant::now() + WATCHDOG),
        || {
            accepted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .await;
    assert_eq!(result.detail, Detail::Registration(Error::Capacity));
    assert_eq!(result.fate, TransportFate::NotDispatched);
    assert_eq!(accepted.load(Ordering::SeqCst), 0);
    assert!(io.trace.lock().unwrap().is_empty());
    assert_eq!(f.requests.load(Ordering::SeqCst), 0);
    result.result["isError"] = json!("PRIVATE mutated diagnostic");
    assert!(!format!("{result:?}").contains("PRIVATE"));
    assert!(
        !format!(
            "{:?}",
            RpcFailure::new(RpcFate::Unknown, "PRIVATE RPC body")
        )
        .contains("PRIVATE")
    );
    drop(held);
    let timing = Timing {
        operation_deadline: Instant::now() + WATCHDOG,
        confirmation_timeout: Duration::from_millis(1),
        rpc_timeout: Duration::MAX,
    };
    let original = timing.operation_deadline;
    let result =
        output_start_v2::execute(&io, &events, OutputKind::Record, timing, || Ok(())).await;
    assert_eq!(result.fate, TransportFate::Accepted);
    assert!(matches!(
        result.detail,
        Detail::Unconfirmed {
            reason: Unconfirmed::ConfirmationExpired,
            ..
        }
    ));
    assert!(io.deadlines.lock().unwrap().iter().all(|d| *d == original));
    f.close().await;
}
#[tokio::test]
async fn post_ack_revocation_and_byte_capacity_are_typed_unavailability_not_false_timeout() {
    let f = Fixture::new(Default::default(), false).await;
    let events = f.bridge.output_events().unwrap();
    let io = TestIo::new(&f, json!({}), false);
    let result = output_start_v2::execute(
        &io,
        &events,
        OutputKind::Record,
        Timing::source(OutputKind::Record, Instant::now() + WATCHDOG),
        || {
            f.bridge.invalidate();
            Ok(())
        },
    )
    .await;
    assert_eq!(result.fate, TransportFate::Accepted);
    assert_eq!(
        result.detail,
        Detail::ObservationUnavailable(Error::Revoked)
    );
    assert_eq!(io.trace.lock().unwrap().len(), 1);
    f.close().await;
    let f = Fixture::new(Default::default(), false).await;
    let events = f.bridge.output_events().unwrap();
    let path = "x".repeat(PATH_BYTES / 2);
    let a = events
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    let b = events
        .subscribe(OutputKind::Record, Instant::now() + WATCHDOG)
        .unwrap();
    f.emit("RecordStateChanged", started(json!(path))).await;
    f.observed().await;
    let a = a.receive(Instant::now() + WATCHDOG).await.unwrap();
    let b = b.receive(Instant::now() + WATCHDOG).await.unwrap();
    let io = TestIo::new(
        &f,
        json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":"tiny"}]}),
        false,
    );
    let result = output_start_v2::execute(
        &io,
        &events,
        OutputKind::Stream,
        Timing::source(OutputKind::Stream, Instant::now() + WATCHDOG),
        || Ok(()),
    )
    .await;
    assert_eq!(result.fate, TransportFate::Accepted);
    assert_eq!(
        result.detail,
        Detail::ObservationUnavailable(Error::ByteCapacity)
    );
    assert_eq!(io.trace.lock().unwrap().len(), 1);
    drop(a);
    drop(b);
    f.close().await;
}
#[tokio::test]
async fn rendered_owned_result_is_outside_released_provider_path_budget() {
    let f = Fixture::new(Default::default(), false).await;
    let events = f.bridge.output_events().unwrap();
    let path = "p".repeat(PATH_BYTES / 2);
    let io = TestIo::new(
        &f,
        json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":path}]}),
        false,
    );
    let result = output_start_v2::execute(
        &io,
        &events,
        OutputKind::Record,
        Timing::source(OutputKind::Record, Instant::now() + WATCHDOG),
        || Ok(()),
    )
    .await;
    assert_eq!(result.detail, Detail::StartedEvent);
    assert!(result.result["content"][0]["text"].as_str().unwrap().len() > PATH_BYTES / 2);
    let a = events
        .subscribe(OutputKind::Stream, Instant::now() + WATCHDOG)
        .unwrap();
    let b = events
        .subscribe(OutputKind::Stream, Instant::now() + WATCHDOG)
        .unwrap();
    f.emit(
        "StreamStateChanged",
        started(json!("s".repeat(PATH_BYTES / 2))),
    )
    .await;
    f.observed().await;
    let a = a.receive(Instant::now() + WATCHDOG).await.unwrap();
    let b = b.receive(Instant::now() + WATCHDOG).await.unwrap();
    assert_eq!(
        a.output_path().unwrap().len() + b.output_path().unwrap().len(),
        PATH_BYTES
    );
    assert_eq!(a.terminal(), Terminal::Started);
    assert_eq!(result.fate, TransportFate::Accepted);
    drop(a);
    drop(b);
    drop(result);
    f.close().await;
}
#[tokio::test]
async fn timely_event_never_upgrades_a_lost_start_ack_when_original_rpc_bound_expires() {
    let f = Fixture::new(Default::default(), true).await;
    let io = TestIo::new(&f, json!({}), true);
    let trace = io.trace.clone();
    let events = f.bridge.output_events().unwrap();
    let accepted = AtomicUsize::new(0);
    let timing = Timing {
        operation_deadline: Instant::now() + Duration::from_secs(2),
        confirmation_timeout: Duration::from_secs(3),
        rpc_timeout: Duration::from_secs(10),
    };
    let result = output_start_v2::execute(&io, &events, OutputKind::Record, timing, || {
        accepted.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .await;
    assert_eq!(result.fate, TransportFate::Unknown);
    assert_eq!(result.detail, Detail::RequestFailed(RpcFate::Unknown));
    assert_eq!(accepted.load(Ordering::SeqCst), 0);
    assert_eq!(*trace.lock().unwrap(), vec!["StartRecord"]);
    assert_eq!(f.requests.load(Ordering::SeqCst), 1);
    f.close().await;
}
#[tokio::test]
async fn actual_disconnect_after_known_ack_remains_accepted_observation_unavailable() {
    let f = Fixture::new(Default::default(), false).await;
    let events = f.bridge.output_events().unwrap();
    let io = TestIo::new(&f, json!({}), true);
    let (done, closed) = oneshot::channel();
    let result = output_start_v2::execute(
        &io,
        &events,
        OutputKind::Record,
        Timing::source(OutputKind::Record, Instant::now() + WATCHDOG),
        || {
            f.commands.try_send(Command::Close(done)).unwrap();
            Ok(())
        },
    )
    .await;
    closed.await.unwrap();
    assert_eq!(result.fate, TransportFate::Accepted);
    assert_eq!(
        result.detail,
        Detail::ObservationUnavailable(Error::Revoked)
    );
    assert_eq!(*io.trace.lock().unwrap(), vec!["StartRecord"]);
    f.close().await;
}
