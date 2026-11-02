// SPDX-License-Identifier: GPL-2.0-only
use futures_util::{SinkExt, StreamExt, poll};
use obs_mcp_native::{
    property_list::{self, Context, Request},
    session::{ObsSession, SessionOptions},
    session_observation::{ObservationBridge, REQUIRED_AUDIT_EVENTS},
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::Notify,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const WAIT: Duration = Duration::from_secs(3);
struct Fixture {
    url: String,
    owner: ObsSession,
    bridge: ObservationBridge,
    trace: Arc<Mutex<Vec<Value>>>,
    peer: JoinHandle<()>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
impl Fixture {
    async fn new(config: Value) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let trace = Arc::new(Mutex::new(vec![]));
        let log = trace.clone();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let peer_entered = entered.clone();
        let peer_release = release.clone();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            peer.send(Message::Text(
                json!({"op":0,"d":{"obsWebSocketVersion":"5.7.4","rpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            peer.next().await.unwrap().unwrap();
            peer.send(Message::Text(
                json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            while let Some(Ok(Message::Text(text))) = peer.next().await {
                let frame: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(frame["op"], 6);
                let name = frame["d"]["requestType"].as_str().unwrap();
                let data = frame["d"]
                    .get("requestData")
                    .cloned()
                    .unwrap_or(Value::Null);
                log.lock()
                    .unwrap()
                    .push(json!({"request":name,"data":data}));
                if name == "GetStats" && config["holdStats"] == true {
                    peer_entered.notify_one();
                    peer_release.notified().await;
                }
                if config["eventAt"] == name {
                    peer.send(Message::Text(json!({"op":5,"d":{"eventType":"InputSettingsChanged","eventIntent":8,"eventData":{}}}).to_string().into())).await.unwrap();
                }
                if let Some(delay) = config["delayMillis"].as_u64() {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                let failed = config["failure"] == name;
                let response = if name == "GetInputSettings" {
                    config["settings"].clone()
                } else {
                    config["response"].clone()
                };
                if peer.send(Message::Text(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":name,"requestStatus":{"result":!failed,"code":if failed{500}else{100},"comment":"PRIVATE PROPERTY COMMENT"},"responseData":response}}).to_string().into())).await.is_err(){break;}
            }
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
        Self {
            url,
            owner,
            bridge,
            trace,
            peer,
            entered,
            release,
        }
    }
    fn context(&self) -> Context {
        Context {
            handle: self.owner.handle(),
            reader: self.bridge.reader(),
            fence: self.bridge.fence(),
            endpoint: self.url.clone(),
            capabilities: None,
            cancellation: Cancellation::new(),
            deadline: Instant::now() + WAIT,
        }
    }
    async fn close(self) {
        self.owner.shutdown().await.unwrap();
        self.bridge.shutdown().await.unwrap();
        timeout(WAIT, self.peer).await.unwrap().unwrap();
    }
}
fn source(args: &Value, config: &Value, era: &str) -> Value {
    let script = r#"import{EventEmitter}from'node:events';import{McpServer,InMemoryTransport}from'@modelcontextprotocol/server';import{Client}from'@modelcontextprotocol/client';import{initialize}from'./build/tools/index.js';const chunks=[];for await(const c of process.stdin)chunks.push(c);const input=JSON.parse(Buffer.concat(chunks)),trace=[];class Fake extends EventEmitter{isConnected(){return true}getConnectionStatus(){return{url:'ws://fixture',identified:true,hasPassword:false}}supportsRequest(n){return input.config.capabilities===undefined||input.config.capabilities.includes(n)}async sendRequest(name,data){if(!this.supportsRequest(name))throw Error(`OBS WebSocket does not advertise support for request '${name}'`);trace.push({request:name,data:data??null});if(input.config.failure===name)throw Error(`OBS request ${name} failed with code 500: PRIVATE PROPERTY COMMENT`);return name==='GetInputSettings'?input.config.settings:input.config.response}}const server=new McpServer({name:'property-oracle',version:'1'});initialize(server,new Fake(),{resourcesAndPrompts:false,sessionNotifications:false,confirmLive:false});const client=new Client({name:'oracle',version:'1'},{versionNegotiation:{mode:input.era}}),[a,b]=InMemoryTransport.createLinkedPair();try{await Promise.all([server.connect(a),client.connect(b)]);const result=await client.callTool({name:'obs-get-input-property-list-items',arguments:input.args});console.log(JSON.stringify({result,trace}));}finally{await client.close();await server.close()}"#;
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
        .write_all(&serde_json::to_vec(&json!({"args":args,"config":config,"era":era})).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn semantic(mut body: Value) -> Value {
    for block in body["content"].as_array_mut().unwrap() {
        if let Some(text) = block["text"].as_str()
            && let Ok(value) = serde_json::from_str::<Value>(text)
        {
            block["text"] = value;
        }
    }
    body
}
async fn compare(args: Value, config: Value) {
    let fixture = Fixture::new(config.clone()).await;
    let mut context = fixture.context();
    context.capabilities = config["capabilities"].as_array().map(|a| {
        Arc::new(
            a.iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect::<HashSet<_>>(),
        )
    });
    let actual = property_list::run(context, Request::parse(&args).unwrap())
        .await
        .unwrap();
    let expected = source(&args, &config, "legacy");
    assert_eq!(
        semantic(actual),
        semantic(expected["result"].clone()),
        "args={args} config={config}"
    );
    assert_eq!(
        *fixture.trace.lock().unwrap(),
        *expected["trace"].as_array().unwrap()
    );
    fixture.close().await;
}
fn config(kind: Value, window: Value) -> Value {
    json!({"settings":{"inputKind":kind,"inputSettings":{"capture_window":window,"display_uuid":"selected"}},"response":{"propertyItems":null,"pluginMetadata":{"nullable":true}}})
}
#[tokio::test]
async fn source_full_index_guard_and_success_shapes() {
    for name in ["display_uuid", "application"] {
        for selected in [Value::Null, json!("selected")] {
            compare(
                json!({"inputName":"","inputUuid":"uuid","propertyName":name,"stripped":true}),
                config(json!("screen_capture"), selected),
            )
            .await;
        }
    }
    for window in [
        Value::Null,
        json!(""),
        json!(0),
        json!(" "),
        json!("not-an-id"),
    ] {
        compare(
            json!({"propertyName":"capture_window"}),
            config(json!("xcomposite_input"), window),
        )
        .await;
    }
    for name in [
        "window",
        "display_uuid",
        "application",
        "capture_window",
        "",
    ] {
        compare(
            json!({"inputName":"cam","propertyName":name}),
            config(json!("other"), Value::Null),
        )
        .await;
    }
    let mut items = config(json!("screen_capture"), Value::Null);
    items["response"] = json!({"propertyItems":[{"itemName":"Fixture window","itemValue":23,"itemEnabled":true,"pluginExtra":null}],"opaquePluginMetadata":{"version":1}});
    compare(json!({"inputUuid":"id","propertyName":"window"}), items).await;
    let mut direct_unsupported = config(json!("screen_capture"), Value::Null);
    direct_unsupported["capabilities"] = json!(["GetInputSettings"]);
    compare(json!({"propertyName":"window"}), direct_unsupported).await;
    for kind in [Value::Null, json!(""), json!(4)] {
        compare(
            json!({"propertyName":"application"}),
            config(kind, Value::Null),
        )
        .await;
    }
    for name in ["GetInputSettings", "GetInputPropertiesListPropertyItems"] {
        let mut c = config(json!("other"), Value::Null);
        c["failure"] = json!(name);
        compare(json!({"propertyName":"display_uuid"}), c).await;
    }
    for caps in [json!([]), json!(["GetInputSettings"])] {
        let mut c = config(json!("other"), Value::Null);
        c["capabilities"] = caps;
        compare(json!({"propertyName":"display_uuid"}), c).await;
    }
}
#[test]
fn parser_distinguishes_null_and_omission() {
    for args in [
        json!({}),
        json!({"propertyName":null}),
        json!({"propertyName":1}),
        json!({"propertyName":"window","inputName":null}),
        json!({"propertyName":"window","inputUuid":false}),
    ] {
        assert!(Request::parse(&args).is_err());
    }
    assert!(
        Request::parse(&json!({"propertyName":"","inputName":"","inputUuid":"","unknown":1}))
            .is_ok()
    );
    assert!(
        !format!(
            "{:?}",
            Request::parse(&json!({"propertyName":"PRIVATE"})).unwrap()
        )
        .contains("PRIVATE")
    );
}
#[tokio::test]
async fn queued_source_event_invalidates_probe_and_rejected_reply() {
    for failure in [Value::Null, json!("GetInputSettings")] {
        let mut c = config(json!("other"), Value::Null);
        c["eventAt"] = json!("GetInputSettings");
        c["failure"] = failure.clone();
        let f = Fixture::new(c).await;
        let result = property_list::run(
            f.context(),
            Request::parse(&json!({"propertyName":"application"})).unwrap(),
        )
        .await;
        if failure.is_null() {
            assert_eq!(result.unwrap_err().reason, "property_list_invalidated");
        } else {
            assert_eq!(
                result.unwrap(),
                json!({"content":[{"type":"text","text":"Get Input Property List Items refused: Capture properties could not be checked safely; read obs-get-input-settings before listing them."}],"isError":true})
            );
        }
        assert_eq!(f.trace.lock().unwrap().len(), 1);
        f.close().await;
    }
}
#[tokio::test]
async fn endpoint_cancellation_and_expired_deadline_refuse_before_any_rpc() {
    for variant in 0..3 {
        let f = Fixture::new(config(json!("other"), Value::Null)).await;
        let mut ctx = f.context();
        match variant {
            0 => ctx.endpoint.push('/'),
            1 => ctx.cancellation.cancel(),
            _ => ctx.deadline = Instant::now(),
        };
        assert!(
            property_list::run(
                ctx,
                Request::parse(&json!({"propertyName":"window"})).unwrap()
            )
            .await
            .is_err()
        );
        assert!(f.trace.lock().unwrap().is_empty());
        f.close().await;
    }
}
#[tokio::test]
async fn frontend_timeout_retains_same_request_until_settled() {
    let mut c = config(json!("other"), Value::Null);
    c["delayMillis"] = json!(60);
    let f = Fixture::new(c).await;
    let mut job = Box::pin(property_list::run(
        f.context(),
        Request::parse(&json!({"propertyName":"window"})).unwrap(),
    ));
    assert!(timeout(Duration::from_millis(10), &mut job).await.is_err());
    assert!(job.await.unwrap()["structuredContent"].is_object());
    assert_eq!(f.trace.lock().unwrap().len(), 1);
    f.close().await;
}

#[tokio::test]
async fn final_actual_send_guard_refuses_queued_event_or_cancel_without_enum_frame() {
    for variant in 0..3 {
        let mut c = config(json!("other"), Value::Null);
        c["holdStats"] = json!(true);
        if variant == 0 {
            c["eventAt"] = json!("GetStats");
        }
        let f = Fixture::new(c).await;
        let handle = f.owner.handle();
        let mut blocker = Box::pin(handle.request(handle.token(), "GetStats", json!({}), WAIT));
        assert!(poll!(&mut blocker).is_pending());
        timeout(WAIT, f.entered.notified()).await.unwrap();
        let mut ctx = f.context();
        if variant == 2 {
            ctx.deadline = Instant::now() + Duration::from_millis(30);
        }
        let cancellation = ctx.cancellation.clone();
        let mut job = Box::pin(property_list::run(
            ctx,
            Request::parse(&json!({"propertyName":"window"})).unwrap(),
        ));
        assert!(poll!(&mut job).is_pending());
        if variant == 1 {
            cancellation.cancel();
        }
        if variant == 2 {
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        f.release.notify_one();
        blocker.await.unwrap();
        assert!(job.await.is_err());
        assert_eq!(
            *f.trace.lock().unwrap(),
            vec![json!({"request":"GetStats","data":{}})]
        );
        f.close().await;
    }
}

#[tokio::test]
async fn foreign_reader_and_owner_drop_never_authorize_dispatch() {
    let a = Fixture::new(config(json!("other"), Value::Null)).await;
    let b = Fixture::new(config(json!("other"), Value::Null)).await;
    let mut ctx = a.context();
    ctx.reader = b.bridge.reader();
    assert!(
        property_list::run(
            ctx,
            Request::parse(&json!({"propertyName":"window"})).unwrap()
        )
        .await
        .is_err()
    );
    let mut ctx = a.context();
    ctx.fence = b.bridge.fence();
    assert!(
        property_list::run(
            ctx,
            Request::parse(&json!({"propertyName":"window"})).unwrap()
        )
        .await
        .is_err()
    );
    assert!(a.trace.lock().unwrap().is_empty());
    assert!(b.trace.lock().unwrap().is_empty());
    a.close().await;
    b.close().await;
    let f = Fixture::new(config(json!("other"), Value::Null)).await;
    let ctx = f.context();
    f.bridge.shutdown().await.unwrap();
    assert!(
        property_list::run(
            ctx,
            Request::parse(&json!({"propertyName":"window"})).unwrap()
        )
        .await
        .is_err()
    );
    assert!(f.trace.lock().unwrap().is_empty());
    f.owner.shutdown().await.unwrap();
    timeout(WAIT, f.peer).await.unwrap().unwrap();
}

#[tokio::test]
async fn malformed_property_items_match_source_output_schema_boundary() {
    for items in [
        json!(3),
        json!({}),
        json!(true),
        json!("value"),
        json!([3]),
        json!([null]),
        json!([[]]),
        json!([false]),
        json!(["value"]),
        json!([{}, 3, null]),
        json!([{"arbitrary":{"nested":[1,null]}}]),
        Value::Null,
        json!([]),
    ] {
        let mut c = config(json!("other"), Value::Null);
        c["response"] = json!({"propertyItems":items,"extra":"retained"});
        compare(json!({"propertyName":"window"}), c).await;
    }
}
