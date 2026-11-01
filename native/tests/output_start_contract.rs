// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::output_start::{
    self, Definition, ObservedEvent, OutputFuture, OutputIo, Subscription,
};
use serde_json::{Value, json};
use std::{
    future::Future,
    path::Path,
    pin::Pin,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{Instant, sleep},
};
struct Observer {
    receiver: mpsc::UnboundedReceiver<ObservedEvent>,
    active: Arc<AtomicUsize>,
}
impl Subscription for Observer {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Option<ObservedEvent>> + Send + '_>> {
        Box::pin(self.receiver.recv())
    }
}
impl Drop for Observer {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}
struct Io {
    input: Value,
    sender: Mutex<Option<mpsc::UnboundedSender<ObservedEvent>>>,
    trace: Arc<Mutex<Vec<String>>>,
    active: Arc<AtomicUsize>,
}
impl OutputIo for Io {
    fn subscribe(&self, event: &'static str) -> Box<dyn Subscription> {
        self.trace.lock().unwrap().push(format!("on:{event}"));
        self.active.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = mpsc::unbounded_channel();
        *self.sender.lock().unwrap() = Some(sender);
        Box::new(Observer {
            receiver,
            active: self.active.clone(),
        })
    }
    fn request(&self, request: &'static str) -> OutputFuture<Result<Value, String>> {
        self.trace.lock().unwrap().push(request.into());
        let input = self.input.clone();
        let sender = self.sender.lock().unwrap().clone();
        let active = self.active.clone();
        Box::pin(async move {
            assert_eq!(
                active.load(Ordering::SeqCst),
                1,
                "observer did not live through request"
            );
            if request == "StartRecord" {
                if let Some(error) = input["error"].as_str() {
                    return Err(error.into());
                }
                if let Some(events) = input["events"].as_array() {
                    for data in events {
                        sender
                            .as_ref()
                            .unwrap()
                            .send(ObservedEvent {
                                at: Instant::now(),
                                data: data.clone(),
                            })
                            .unwrap();
                    }
                }
                if input["ackLate"] == true {
                    sleep(Duration::from_millis(35)).await;
                }
                Ok(json!({}))
            } else if input["statusError"] == true {
                Err("status fixture failed".into())
            } else {
                Ok(input["status"].clone())
            }
        })
    }
}
fn oracle(input: &Value) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let result=Command::new("node").arg("--input-type=module").arg("-e").arg(r#"
import {EventEmitter} from 'node:events';import {startOutputAndConfirm} from './build/tools/output-start.js';
const input=JSON.parse(process.env.OUTPUT_START_TEST_INPUT),trace=[];
class Client extends EventEmitter {
 on(name,fn){trace.push('on:'+name);return super.on(name,fn)}
 async sendRequest(type){
  trace.push(type);
  if(type==='StartRecord'){
   if(input.error)throw Error(input.error);
   for(const event of input.events??[])this.emit('RecordStateChanged',event);
   if(input.ackLate)await new Promise(resolve=>setTimeout(resolve,35));
   return {};
  }
  if(input.statusError)throw Error('status fixture failed');return input.status;
 }
}
const client=new Client();
const result=await startOutputAndConfirm(client,{label:'Recording',requestType:'StartRecord',statusRequest:'GetRecordStatus',stateEvent:'RecordStateChanged',timeoutMs:20},()=>{trace.push('accepted');if(input.acceptError)throw Error(input.acceptError)});
if(client.listenerCount('RecordStateChanged'))throw Error('source leaked observer');
console.log(JSON.stringify({result,trace}));
"#).current_dir(root).env("OUTPUT_START_TEST_INPUT",input.to_string()).output().unwrap();
    assert!(
        result.status.success(),
        "source output-start oracle failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}
#[tokio::test]
async fn actual_source_result_effect_order_pre_ack_events_and_cleanup_match() {
    for input in [
        json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":"/fixture/file.mkv"}]}),
        json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED","outputPath":""}]}),
        json!({"events":[null,{},{"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED"}],"ackLate":true}),
        json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"}],"status":{"outputActive":false}}),
        json!({"events":[{"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"}],"status":{"outputActive":true}}),
        json!({"events":[],"status":{"outputActive":true}}),
        json!({"events":[],"status":null}),
        json!({"events":[],"statusError":true}),
        json!({"error":"fixture request rejected"}),
        json!({"acceptError":"fixture acceptance callback failed"}),
    ] {
        let expected = oracle(&input);
        let trace = Arc::new(Mutex::new(vec![]));
        let active = Arc::new(AtomicUsize::new(0));
        let io = Io {
            input: input.clone(),
            sender: Mutex::new(None),
            trace: trace.clone(),
            active: active.clone(),
        };
        let result = output_start::start_output_and_confirm(
            &io,
            Definition {
                timeout: Duration::from_millis(20),
                ..output_start::RECORD_OUTPUT
            },
            || {
                trace.lock().unwrap().push("accepted".into());
                input["acceptError"]
                    .as_str()
                    .map_or(Ok(()), |error| Err(error.into()))
            },
        )
        .await;
        assert_eq!(result, expected["result"]);
        assert_eq!(json!(*trace.lock().unwrap()), expected["trace"]);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }
}
