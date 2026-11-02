// SPDX-License-Identifier: GPL-2.0-only
//! Actual pinned Source stdio parses raw JSON bytes before its tool callback.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::js_json::{
    Limits, NumericNormalizationError, encode_source_json, encoded_len, normalize_json_numbers,
    normalize_with_limits,
};
use obs_mcp_native::session::{ObsSession, SessionOptions};
use serde_json::{Value, json};
use std::{
    io::Write,
    process::{Command, Stdio},
};
use tokio::{io::AsyncWriteExt, net::TcpListener, time::Duration};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

const SOURCE_WIRE: &str = r#"
import {spawn} from 'node:child_process';
let raw='';for await(const chunk of process.stdin)raw+=chunk;
const {settingsText}=JSON.parse(raw);
const serverSource=String.raw`
import {McpServer} from '@modelcontextprotocol/server';
import {StdioServerTransport} from '@modelcontextprotocol/server/stdio';
import {EventEmitter} from 'node:events';
import {initialize} from './build/tools/index.js';
let settings={};const obs=new EventEmitter();
Object.assign(obs,{sendRequest:async(requestType,requestData)=>{
  process.stderr.write('TRACE '+JSON.stringify({requestType,...(requestData===undefined?{}:{requestData})})+'\n');
  switch(requestType){
    case 'SetInputSettings':settings=requestData.inputSettings;return{};
    case 'GetInputSettings':return{inputKind:'coreaudio_input_capture',inputSettings:settings};
    case 'GetCurrentProgramScene':return{};
    default:throw new Error('Unexpected '+requestType);
  }
},isConnected:()=>true,getConnectionStatus:()=>({generation:1,identified:true,url:'ws://localhost:4455',hasPassword:false}),supportsRequest:()=>true});
const server=new McpServer({name:'numeric-source-wire',version:'1'});
initialize(server,obs,{confirmLive:false,resourcesAndPrompts:false,sessionNotifications:false});
await server.connect(new StdioServerTransport());
`;
const child=spawn(process.execPath,['--input-type=module','-e',serverSource],{cwd:process.cwd(),stdio:['pipe','pipe','pipe']});
let diagnostics='',pending='',waiters=new Map(),exitCode;
const exit=new Promise(resolve=>child.once('exit',(code,signal)=>{exitCode={code,signal};resolve(exitCode);}));
child.stderr.on('data',chunk=>{diagnostics+=chunk;});
child.stdout.on('data',chunk=>{pending+=chunk;for(;;){const at=pending.indexOf('\n');if(at<0)break;const line=pending.slice(0,at);pending=pending.slice(at+1);if(!line.trim())continue;const frame=JSON.parse(line);waiters.get(frame.id)?.resolve(frame);}});
function response(id){return new Promise((resolve,reject)=>{const timer=setTimeout(()=>{waiters.delete(id);reject(new Error('Source wire reply timeout '+id));},5000);waiters.set(id,{resolve:frame=>{clearTimeout(timer);waiters.delete(id);resolve(frame);}});});}
let result;
try{
  const initialized=response(1);
  child.stdin.write('{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","clientInfo":{"name":"numeric-wire","version":"1"},"capabilities":{}}}\n');
  const init=await initialized;if(init.error)throw new Error(JSON.stringify(init));
  child.stdin.write('{"jsonrpc":"2.0","method":"notifications/initialized"}\n');
  const called=response(2);
  child.stdin.write('{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"obs-set-input-settings","arguments":{"inputName":"Fixture","inputSettings":'+settingsText+'}}}\n');
  result=await called;
}finally{
  child.stdin.end();
  const timer=setTimeout(()=>child.kill('SIGTERM'),1500);await exit;clearTimeout(timer);
}
if(exitCode.code!==0)throw new Error('Source wire exit '+JSON.stringify(exitCode)+' '+diagnostics);
const rawTrace=diagnostics.split('\n').filter(line=>line.startsWith('TRACE ')).map(line=>line.slice(6));
const trace=rawTrace.map(line=>JSON.parse(line));
console.log(JSON.stringify({result,trace,rawTrace,exit:exitCode}));
"#;
fn source_wire(settings: &str) -> Value {
    let root = std::env::var_os("OBS_CUSTOM_SOURCE_ROOT").unwrap_or_else(|| {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .into_os_string()
    });
    let mut child = Command::new("node")
        .args(["--input-type=module", "-e", SOURCE_WIRE])
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&json!({"settingsText":settings})).unwrap())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "wire fixture: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}
fn numbers() -> &'static str {
    r#"{"negativeZero":-0.0,"positiveFloat":1.0,"safeLow":9007199254740991,"safeEdge":9007199254740992,"rounded":9007199254740993,"nextEven":9007199254740994,"negativeRounded":-9007199254740993,"fullU64":18446744073709551615,"fullI64":-9223372036854775808,"shortInteger":1000000000000000128,"minSubnormal":5e-324,"smallExponent":1e-7,"threshold":0.000001,"largeExponent":1e21,"nested":[-0,9007199254740993,{"kept":"unchanged","flag":true,"nothing":null}]}"#
}
#[test]
fn raw_source_stdio_proves_pre_callback_rounding_and_post_callback_zero_projection() {
    let actual = source_wire(numbers());
    let parsed: Value = serde_json::from_str(numbers()).unwrap();
    let source = &actual["trace"][0]["requestData"]["inputSettings"];
    assert_eq!(actual["trace"][0]["requestType"], "SetInputSettings");
    assert_ne!(&parsed, source);
    assert_eq!(source["negativeZero"], json!(0));
    assert_eq!(source["rounded"], json!(9007199254740992u64));
    assert_eq!(source["negativeRounded"], json!(-9007199254740992i64));
    assert_eq!(source["shortInteger"], json!(1000000000000000100u64));
    assert_eq!(
        &actual["result"]["result"]["structuredContent"]["inputSettings"],
        source
    );
    assert_eq!(actual["exit"], json!({"code":0,"signal":null}));
    let raw = actual["rawTrace"][0].as_str().unwrap();
    assert!(raw.contains("\"negativeZero\":0,"));
    assert!(raw.contains("\"positiveFloat\":1,"));
    assert!(raw.contains("\"shortInteger\":1000000000000000100,"));
    assert!(raw.contains("\"fullI64\":-9223372036854776000,"));
    println!(
        "{}",
        json!({"scope":"actual Source stdio controlled numeric fixture","before":parsed,"sourceRequest":source,"sourceRequestRaw":raw,"sourceResult":actual["result"]["result"]["structuredContent"]["inputSettings"],"sourceExited":actual["exit"]})
    );
}
#[test]
fn proposed_numeric_normalizer_matches_the_controlled_source_wire_values() {
    let actual = source_wire(numbers());
    let mut value: Value = serde_json::from_str(numbers()).unwrap();
    normalize_json_numbers(&mut value).unwrap();
    assert_eq!(value, actual["trace"][0]["requestData"]["inputSettings"]);
    // No mutation authority or a numeric tolerance for snapshot comparisons is implied.
    assert_eq!(
        value["nested"][2],
        json!({"kept":"unchanged","flag":true,"nothing":null})
    );
}
#[test]
fn bounds_refuse_before_any_numeric_node_changes() {
    let mut value: Value =
        serde_json::from_str(r#"{"first":-0,"nested":{"big":9007199254740993}}"#).unwrap();
    let before = value.clone();
    assert_eq!(
        normalize_with_limits(
            &mut value,
            Limits {
                max_depth: 1,
                max_encoded_bytes: 8192
            }
        ),
        Err(NumericNormalizationError::Depth)
    );
    assert_eq!(value, before);
    assert_eq!(
        normalize_with_limits(
            &mut value,
            Limits {
                max_depth: 128,
                max_encoded_bytes: 8
            }
        ),
        Err(NumericNormalizationError::EncodedBytes)
    );
    assert_eq!(value, before);
}
#[test]
fn normalized_encoded_expansion_cannot_escape_the_bound_or_partially_change_values() {
    let mut value = json!({"zero":-0.0,"small":1e-6});
    let before = value.clone();
    let serde_bytes = serde_json::to_vec(&value).unwrap().len();
    let source = encode_source_json(&value, Limits::default()).unwrap();
    assert!(source.len() > serde_bytes);
    assert_eq!(
        normalize_with_limits(
            &mut value,
            Limits {
                max_depth: 128,
                max_encoded_bytes: serde_bytes
            }
        ),
        Err(NumericNormalizationError::EncodedBytes)
    );
    assert_eq!(value, before);
    assert_eq!(
        encoded_len(&value, Limits::default()).unwrap(),
        source.len()
    );
}

#[test]
fn diagnostic_limits_are_not_valid_opaque_input_limits() {
    let mut value = json!({"items":vec![0;5000],"text":"x".repeat(1024*1024+1)});
    normalize_json_numbers(&mut value).unwrap();
    assert_eq!(value["items"].as_array().unwrap().len(), 5000);
    let mut deep = json!(9007199254740993u64);
    for _ in 0..40 {
        deep = json!([deep]);
    }
    normalize_json_numbers(&mut deep).unwrap();
}
#[test]
fn source_overflow_and_existing_value_parser_are_explicitly_different() {
    let actual = source_wire(r#"{"positive":1e400,"negative":-1e400}"#);
    assert_eq!(
        actual["trace"][0]["requestData"]["inputSettings"],
        json!({"positive":null,"negative":null})
    );
    assert!(serde_json::from_str::<Value>(r#"{"positive":1e400}"#).is_err());
}

#[test]
fn encoder_preserves_non_numeric_property_data_and_counts_its_exact_bytes() {
    let mut value:Value=serde_json::from_str(r#"{"negativeZero":-0.0,"one":1.0,"large":18446744073709551615,"small":1e-6,"__proto__":{"kept":"own"},"nested":[9007199254740993,{"text":"line\n\u0000日本語","flag":true,"nothing":null}]}"#).unwrap();
    let before = encode_source_json(&value, Limits::default()).unwrap();
    normalize_json_numbers(&mut value).unwrap();
    let after = encode_source_json(&value, Limits::default()).unwrap();
    assert_eq!(before, after);
    let text = String::from_utf8(after).unwrap();
    for token in [
        "\"negativeZero\":0",
        "\"one\":1",
        "\"large\":18446744073709552000",
        "\"small\":0.000001",
    ] {
        assert!(text.contains(token), "{text}");
    }
    assert_eq!(value["__proto__"], json!({"kept":"own"}));
    let parsed: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["nested"][1]["text"], json!("line\n\u{0}日本語"));
    assert_eq!(encoded_len(&value, Limits::default()).unwrap(), text.len());
}

#[test]
fn prepared_borrow_reuses_exact_bound_and_does_not_debug_the_opaque_payload() {
    let value = json!({"private":"controlled-private-payload","small":1e-6,"large":u64::MAX});
    let prepared = obs_mcp_native::js_json::Prepared::new(&value, Limits::default()).unwrap();
    assert!(!prepared.is_empty());
    assert!(!format!("{prepared:?}").contains("controlled-private-payload"));
    let length = prepared.len();
    let mut written = vec![];
    prepared.write_to(&mut written).unwrap();
    assert_eq!(length, written.len());
    assert_eq!(written, prepared.into_buffer().unwrap());
    assert!(matches!(
        obs_mcp_native::js_json::Prepared::new(
            &value,
            Limits {
                max_depth: 128,
                max_encoded_bytes: length - 1
            }
        ),
        Err(NumericNormalizationError::EncodedBytes)
    ));
}
#[test]
fn deterministic_finite_f64_bits_match_actual_node_json_stringify() {
    let mut bits = vec![
        0f64.to_bits(),
        (-0f64).to_bits(),
        1,
        f64::MIN_POSITIVE.to_bits(),
        f64::MAX.to_bits(),
        f64::MIN.to_bits(),
        f64::EPSILON.to_bits(),
        ((1u64 << 52) - 1),
    ];
    for value in [
        1.0f64,
        -1.0,
        42.0,
        9_007_199_254_740_991.0,
        -9_007_199_254_740_991.0,
    ] {
        bits.push(value.to_bits());
    }
    for value in [
        1e-6f64,
        1e21,
        1e-7,
        1e18,
        9007199254740992.0,
        1000000000000000128.0,
        2.2250738585072014e-308,
    ] {
        for delta in [-1i64, 0, 1] {
            bits.push(value.to_bits().wrapping_add_signed(delta));
            bits.push((-value).to_bits().wrapping_add_signed(delta));
        }
    }
    let mut seed = 0x8e37_79b9_7f4a_7c15u64;
    for _ in 0..4096 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        if f64::from_bits(seed).is_finite() {
            bits.push(seed);
        }
    }
    let script = r#"let input='';for await(const chunk of process.stdin)input+=chunk;const bits=JSON.parse(input),buffer=Buffer.alloc(8);console.log(JSON.stringify(bits.map(value=>{buffer.writeBigUInt64LE(BigInt(value));return JSON.stringify(buffer.readDoubleLE());})));"#;
    let mut child = Command::new("node")
        .args(["--input-type=module", "-e", script])
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
            &serde_json::to_vec(&bits.iter().map(u64::to_string).collect::<Vec<_>>()).unwrap(),
        )
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "Node number oracle failed");
    let expected: Vec<String> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(bits.len(), expected.len());
    for (bits, expected) in bits.iter().zip(expected) {
        let n = f64::from_bits(*bits);
        let value = Value::Number(serde_json::Number::from_f64(n).unwrap());
        let actual =
            String::from_utf8(encode_source_json(&value, Limits::default()).unwrap()).unwrap();
        assert_eq!(actual, expected, "finite f64 bits={bits:016x} value={n:?}");
    }
    println!(
        "qualified {} deterministic finite f64 bit cases against actual Node JSON.stringify",
        bits.len()
    );
}

async fn raw_obs_capture(flavor: u8, raw: &str) -> Vec<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut frames = vec![];
        socket
            .send(Message::Text(
                json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        while let Some(message) = socket.next().await {
            let Ok(Message::Text(text)) = message else {
                let _ = socket.flush().await;
                return frames;
            };
            let frame: Value = serde_json::from_str(&text).unwrap();
            if frame["op"] == 1 {
                socket
                    .send(Message::Text(
                        json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                continue;
            }
            if frame["op"] != 6 {
                continue;
            }
            frames.push(text.to_string());
            let data = if frame["d"]["requestType"] == "GetVersion" {
                json!({"obsVersion":"32.2.2","obsWebSocketVersion":"5.7.0","rpcVersion":1,"availableRequests":["GetVersion","SetInputSettings"]})
            } else {
                json!({})
            };
            socket.send(Message::Text(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":frame["d"]["requestType"],"requestStatus":{"result":true,"code":100},"responseData":data}}).to_string().into())).await.unwrap();
        }
        frames
    });
    if flavor == 0 {
        // Real owning Source OBS client, not the mocked MCP callback adapter.
        let script = r#"import {OBSWebSocketClient} from './build/client.js';let text='';for await(const chunk of process.stdin)text+=chunk;const spec=JSON.parse(text),client=new OBSWebSocketClient(spec.url);try{await client.connect();await client.sendRequest('SetInputSettings',JSON.parse(spec.data));}finally{await client.disconnect();}"#;
        let source = std::env::var_os("OBS_CUSTOM_SOURCE_ROOT").unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .into_os_string()
        });
        let mut child = tokio::process::Command::new("node")
            .args(["--input-type=module", "-e", script])
            .current_dir(source)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&serde_json::to_vec(&json!({"url":endpoint,"data":raw})).unwrap())
            .await
            .unwrap();
        match tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
            Ok(result) => assert!(result.unwrap().success(), "Source OBS client failed"),
            Err(_) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                panic!("Source OBS client watchdog expired after joined cleanup");
            }
        }
    } else {
        let mut data: Value = serde_json::from_str(raw).unwrap();
        normalize_json_numbers(&mut data).unwrap();
        if flavor == 1 {
            let (session, events) = ObsSession::connect(&endpoint, None, SessionOptions::default())
                .await
                .unwrap();
            session
                .handle()
                .request(
                    session.token(),
                    "SetInputSettings",
                    data,
                    Duration::from_secs(3),
                )
                .await
                .unwrap();
            session.shutdown().await.unwrap();
            drop(events);
        } else {
            // Encoder proof only: the new routes are not integrated with the
            // checked production actor. Transport these controlled bytes to
            // observe exact lexemes, not to claim production admission.
            let (mut socket, _) = connect_async(&endpoint).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({"op":1,"d":{"rpcVersion":1}}).to_string().into(),
                ))
                .await
                .unwrap();
            socket.next().await.unwrap().unwrap();
            let frame = json!({"op":6,"d":{"requestId":"numeric-encoder-fixture","requestType":"SetInputSettings","requestData":data}});
            let bytes = encode_source_json(&frame, Limits::default()).unwrap();
            assert_eq!(bytes.len(), encoded_len(&frame, Limits::default()).unwrap());
            socket
                .send(Message::Text(String::from_utf8(bytes).unwrap().into()))
                .await
                .unwrap();
            socket.next().await.unwrap().unwrap();
            socket.close(None).await.unwrap();
        }
    }
    tokio::time::timeout(Duration::from_secs(4), peer)
        .await
        .unwrap()
        .unwrap()
}
fn token<'a>(frame: &'a str, key: &str) -> &'a str {
    let key = format!("\"{key}\":");
    let rest = frame.split_once(&key).expect("controlled numeric field").1;
    let length = rest
        .bytes()
        .take_while(|byte| byte.is_ascii_digit() || b"-+.eE".contains(byte))
        .count();
    assert!(length > 0);
    &rest[..length]
}
#[tokio::test]
async fn actual_obs_text_frames_match_source_after_scoped_encoder_integration() {
    let raw = format!(r#"{{"inputName":"Fixture","inputSettings":{}}}"#, numbers());
    let source = raw_obs_capture(0, &raw).await;
    let current = raw_obs_capture(1, &raw).await;
    let encoded = raw_obs_capture(2, &raw).await;
    let pick = |frames: Vec<String>| {
        frames
            .into_iter()
            .find(|text| text.contains("\"SetInputSettings\""))
            .unwrap()
    };
    let source = pick(source);
    let current = pick(current);
    let encoded = pick(encoded);
    for key in [
        "negativeZero",
        "positiveFloat",
        "safeLow",
        "safeEdge",
        "rounded",
        "nextEven",
        "negativeRounded",
        "fullU64",
        "fullI64",
        "shortInteger",
        "minSubnormal",
        "smallExponent",
        "threshold",
        "largeExponent",
    ] {
        assert_eq!(
            token(&encoded, key),
            token(&source, key),
            "borrowedencoder OBS token {key}"
        );
        assert_eq!(
            token(&current, key),
            token(&source, key),
            "productionactor OBS token {key}"
        );
    }
    for key in ["negativeZero", "positiveFloat", "rounded", "shortInteger"] {
        let text = token(&encoded, key);
        assert!(
            !text.contains(['.', 'e', 'E']),
            "Source integer token kind {key}: {text}"
        );
    }
    assert_eq!(token(&current, "fullU64"), token(&source, "fullU64"));
    assert_eq!(token(&current, "threshold"), token(&source, "threshold"));
    println!(
        "actual OBS scoped encoder: fullU64 {} matchesSource {}; borrowedEncoder {}",
        token(&current, "fullU64"),
        token(&source, "fullU64"),
        token(&encoded, "fullU64")
    );
}
