// SPDX-License-Identifier: GPL-2.0-only
#![allow(dead_code)] // Shared by two independently compiled consumer binaries.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    clock,
    control::ControlDomain,
    custody::Clock,
    generic_requests::Call,
    observation::ReadRequest,
    operation_store::OperationStore,
    service::{ConnectionDescriptor, Options, REQUIRED_SUBSCRIPTIONS, Resources, Service},
    session::{ObsSession, SessionOptions},
    session_observation::ObservationBridge,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
pub const WAIT: Duration = Duration::from_secs(12);
pub const EPOCH: u64 = 1_793_448_000_000;
pub fn id(n: u64) -> String {
    format!("00000000-0000-4000-8000-{n:012x}")
}
pub fn clock() -> Clock {
    Arc::new(|| clock::from_epoch_duration(Duration::from_millis(EPOCH)).unwrap())
}
pub fn control() -> Arc<Mutex<ControlDomain>> {
    Arc::new(Mutex::new(ControlDomain::default()))
}
pub fn call(name: &str) -> Call {
    Call::parse(name, &json!({})).unwrap()
}
pub const STARTS: [(&str, &str, &str, &str); 3] = [
    (
        "obs-start-stream",
        "StartStream",
        "StreamStateChanged",
        "Streaming",
    ),
    (
        "obs-start-virtual-cam",
        "StartVirtualCam",
        "VirtualcamStateChanged",
        "Virtual camera",
    ),
    (
        "obs-start-replay-buffer",
        "StartReplayBuffer",
        "ReplayBufferStateChanged",
        "Replay buffer",
    ),
];
pub struct Dir(pub PathBuf);
impl Dir {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "output-start-owner-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&p).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(p)
    }
    pub fn path(&self, n: u64) -> PathBuf {
        self.0
            .join("operation-receipts")
            .join(format!("{}.json", id(n)))
    }
    pub fn receipt(&self, n: u64) -> Value {
        serde_json::from_slice(&fs::read(self.path(n)).unwrap()).unwrap()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Clone)]
pub struct Plan {
    pub states: Vec<Value>,
    pub response: Value,
    pub rejected: bool,
    pub lost: bool,
    pub held: bool,
}
impl Default for Plan {
    fn default() -> Self {
        Self {
            states: vec![
                json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":"/fixture/α.mkv"}),
            ],
            response: json!({"outputActive":true}),
            rejected: false,
            lost: false,
            held: false,
        }
    }
}
pub struct Socket {
    pub url: String,
    pub trace: Arc<Mutex<Vec<Value>>>,
    pub plans: Arc<Mutex<HashMap<String, Plan>>>,
    pub held: Arc<Mutex<HashMap<String, Value>>>,
    pub out: mpsc::Sender<Value>,
    pub caps: Arc<Mutex<HashSet<String>>>,
    notify: Arc<Notify>,
    task: JoinHandle<()>,
}
impl Socket {
    pub async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let trace = Arc::new(Mutex::new(vec![]));
        let plans = Arc::new(Mutex::new(HashMap::<String, Plan>::new()));
        let held = Arc::new(Mutex::new(HashMap::new()));
        let caps = Arc::new(Mutex::new(
            [
                "GetVersion",
                "GetInputList",
                "GetCurrentProgramScene",
                "GetStats",
                "GetProfileParameter",
                "StartStream",
                "StartVirtualCam",
                "StartReplayBuffer",
                "GetStreamStatus",
                "GetVirtualCamStatus",
                "GetReplayBufferStatus",
                "StopStream",
                "StopVirtualCam",
                "StopReplayBuffer",
                "GetRecordStatus",
                "StartRecord",
                "StopRecord",
                "SetInputMute",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect::<HashSet<_>>(),
        ));
        let notify = Arc::new(Notify::new());
        let (out, mut incoming) = mpsc::channel::<Value>(32);
        let (history, known, pending, capabilities, wake) = (
            trace.clone(),
            plans.clone(),
            held.clone(),
            caps.clone(),
            notify.clone(),
        );
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            peer.send(Message::Text(
                json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let hello = peer.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(hello.to_text().unwrap()).unwrap()["op"],
                1
            );
            peer.send(Message::Text(
                json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let mut record_active = false;
            loop {
                tokio::select! {
                 message=peer.next()=>{let Some(Ok(Message::Text(text)))=message else{let _=peer.flush().await;return;};let frame:Value=serde_json::from_str(&text).unwrap();if frame["op"]==8 {let results=frame["d"]["requests"].as_array().unwrap().iter().map(|call|json!({"requestType":call["requestType"],"requestId":call["requestId"],"requestStatus":{"result":true,"code":100},"responseData":{}})).collect::<Vec<_>>();if peer.send(Message::Text(json!({"op":9,"d":{"requestId":frame["d"]["requestId"],"results":results}}).to_string().into())).await.is_err(){return;}continue;}let kind=frame["d"]["requestType"].as_str().unwrap().to_owned();history.lock().unwrap().push(frame.clone());wake.notify_waiters();let plan=known.lock().unwrap().remove(&kind).unwrap_or_default();if matches!(kind.as_str(),"StartRecord"|"StopRecord"){record_active=kind=="StartRecord";let state=if record_active{"OBS_WEBSOCKET_OUTPUT_STARTED"}else{"OBS_WEBSOCKET_OUTPUT_STOPPED"};if peer.send(Message::Text(json!({"op":5,"d":{"eventType":"RecordStateChanged","eventIntent":64,"eventData":{"outputActive":record_active,"outputState":state,"outputPath":"/fixture/record.mkv"}}}).to_string().into())).await.is_err(){return;}}
                 if let Some((_,_,event,_))=STARTS.iter().find(|(_,request,_,_)|*request==kind){for data in &plan.states{if peer.send(Message::Text(json!({"op":5,"d":{"eventType":event,"eventIntent":64,"eventData":data}}).to_string().into())).await.is_err(){return;}}}
                 if plan.lost{let _=peer.close(None).await;return;}let data=if kind=="GetVersion"{json!({"obsVersion":"32.2.2","obsWebSocketVersion":"5.7.0","rpcVersion":1,"platform":"macos","availableRequests":capabilities.lock().unwrap().iter().collect::<Vec<_>>(),"supportedImageFormats":["png"]})}else{match kind.as_str(){"GetRecordStatus"=>json!({"outputActive":record_active,"outputPaused":false,"outputBytes":1000}),"GetInputList"=>json!({"inputs":[]}),"GetCurrentProgramScene"=>json!({"currentProgramSceneName":"Fixture"}),"GetStats"=>json!({"renderSkippedFrames":0,"renderTotalFrames":100,"outputSkippedFrames":0,"outputTotalFrames":100,"cpuUsage":1.0}),"GetProfileParameter"=>json!({"parameterValue":"1"}),"StopRecord"=>json!({"outputPath":"/fixture/record.mkv"}),_=>plan.response}};let reply=json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":kind,"requestStatus":{"result":!plan.rejected,"code":if plan.rejected{500}else{100},"comment":"PRIVATE precise start refusal"},"responseData":data}});if plan.held{pending.lock().unwrap().insert(kind,reply);wake.notify_waiters();continue;}
                 if peer.send(Message::Text(reply.to_string().into())).await.is_err(){return;}
                 },
                 value=incoming.recv()=>{let Some(value)=value else{return;};if peer.send(Message::Text(value.to_string().into())).await.is_err(){return;}}
                }
            }
        });
        Self {
            url,
            trace,
            plans,
            held,
            out,
            caps,
            notify,
            task,
        }
    }
    pub fn plan(&self, request: &str, plan: Plan) {
        self.plans.lock().unwrap().insert(request.into(), plan);
    }
    pub fn count(&self, request: &str) -> usize {
        self.trace
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| frame["d"]["requestType"] == request)
            .count()
    }
    pub async fn seen(&self, request: &str, n: usize) {
        timeout(WAIT, async {
            loop {
                let changed = self.notify.notified();
                if self.count(request) >= n {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "waiting for {request} count{n}; seen request types {:?}",
                self.trace
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|frame| frame["d"]["requestType"].clone())
                    .collect::<Vec<_>>()
            )
        });
    }
    pub async fn release(&self, request: &str) {
        timeout(WAIT, async {
            loop {
                let changed = self.notify.notified();
                let frame = self.held.lock().unwrap().remove(request);
                if let Some(frame) = frame {
                    self.out.send(frame).await.unwrap();
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
    pub async fn emit(&self, event: &str, data: Value) {
        self.out
            .send(json!({"op":5,"d":{"eventType":event,"eventIntent":64,"eventData":data}}))
            .await
            .unwrap();
    }
    pub async fn connect(&self) -> (ObsSession, ObservationBridge) {
        let (session, events) = ObsSession::connect(
            &self.url,
            None,
            SessionOptions {
                event_subscriptions: REQUIRED_SUBSCRIPTIONS,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let bridge = ObservationBridge::bind(session.handle(), events, None).unwrap();
        (session, bridge)
    }
    pub async fn service(&self, dir: &Dir, duration: Duration) -> Service {
        self.service_with_take(dir, duration, None).await
    }
    pub async fn service_with_take(
        &self,
        dir: &Dir,
        duration: Duration,
        take: Option<obs_mcp_native::service::TakeServices>,
    ) -> Service {
        self.service_with_features(dir, duration, take, None).await
    }
    pub async fn service_with_features(
        &self,
        dir: &Dir,
        duration: Duration,
        take: Option<obs_mcp_native::service::TakeServices>,
        media: Option<Arc<obs_mcp_native::media_tools::Config>>,
    ) -> Service {
        let (session, bridge) = self.connect().await;
        let mut n = 0;
        let service = Service::spawn(
            Resources {
                session,
                bridge,
                store: OperationStore::new(&dir.0, &self.url).unwrap(),
                control: control(),
                clock: clock(),
                identities: Box::new(move || {
                    n += 1;
                    id(n)
                }),
            },
            Options {
                operation_timeout: duration,
                take,
                media,
                state_root: Some(dir.0.clone()),
                connection: Some(ConnectionDescriptor {
                    url: self.url.clone(),
                    has_password: false,
                }),
                ..Default::default()
            },
        )
        .map_err(|(error, _)| error)
        .unwrap();
        service
            .handle()
            .read(ReadRequest::Version, Value::Null, duration.min(WAIT))
            .await
            .unwrap();
        service
    }
    pub async fn join(self) {
        timeout(WAIT, self.task).await.unwrap().unwrap();
    }
}
