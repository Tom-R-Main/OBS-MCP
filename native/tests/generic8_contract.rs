// SPDX-License-Identifier: GPL-2.0-only
//! Closed audio/media admission and actual Source exhaustive-record diagnostics.
use obs_mcp_native::{
    generic_requests::Call,
    operation_store::GenericKind,
    tool_validation::{self, ProjectionUnavailable},
};
use serde_json::{Value, json};
use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::OnceLock,
};
fn schema() -> &'static Value {
    static CATALOG: OnceLock<Value> = OnceLock::new();
    &CATALOG.get_or_init(||serde_json::from_str(include_str!("../assets/tools.json")).unwrap())["tools"].as_array().unwrap().iter().find(|tool|tool["name"]=="obs-set-input-audio-tracks").unwrap()["inputSchema"]
}
fn frame(args: &str) -> Vec<u8> {
    format!(r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"obs-set-input-audio-tracks","arguments":{args}}}}}"#).into_bytes()
}
fn oracle(cases: &[&str], legacy: bool) -> Value {
    let script = r#"
import{Client}from'@modelcontextprotocol/client';import{StdioClientTransport,getDefaultEnvironment}from'@modelcontextprotocol/client/stdio';import{mkdtempSync,rmSync,chmodSync}from'node:fs';import{join}from'node:path';
let input='';for await(const part of process.stdin)input+=part;const cases=JSON.parse(input);
const serverCode=`import{McpServer}from'@modelcontextprotocol/server';import{serveStdio}from'@modelcontextprotocol/server/stdio';import{EventEmitter}from'node:events';import{initialize}from'./build/tools/index.js';const obs=new EventEmitter();Object.assign(obs,{isConnected:()=>false,getConnectionStatus:()=>({}),sendRequest:async()=>{process.stderr.write('UNEXPECTED_DISPATCH\\n');throw Error('unexpected OBS request')}});serveStdio(()=>{const server=new McpServer({name:'record-oracle',version:'1'});initialize(server,obs,{confirmLive:false,resourcesAndPrompts:false,sessionNotifications:false});return server});`;
const state=mkdtempSync(join(process.cwd(),'native/target/generic8-oracle-'));chmodSync(state,0o700);const client=new Client({name:'record-oracle',version:'1'},{versionNegotiation:{mode:process.argv[1]==='legacy'?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:process.execPath,args:['--input-type=module','-e',serverCode],cwd:process.cwd(),env:{...getDefaultEnvironment(),OBS_MCP_STATE_DIR:state,OBS_MCP_CONFIRM_LIVE:'false'},stderr:'pipe'});let diagnostics='';transport.stderr?.on('data',chunk=>diagnostics+=String(chunk));try{await client.connect(transport);const results=[];for(const args of cases){const result=await client.callTool({name:'obs-set-input-audio-tracks',arguments:JSON.parse(args)});delete result._meta;results.push(result);}console.log(JSON.stringify({dispatches:diagnostics.split('UNEXPECTED_DISPATCH').length-1,results}));}finally{await client.close();await transport.close();rmSync(state,{recursive:true,force:true});}
"#;
    let mut child = Command::new("node")
        .args([
            "--input-type=module",
            "-e",
            script,
            if legacy { "legacy" } else { "modern" },
        ])
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
        .write_all(&serde_json::to_vec(cases).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "Source record oracle: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
#[test]
fn exhaustive_record_errors_match_actual_source_including_original_order_and_reserved_keys() {
    let cases = [
        r#"{}"#,
        r#"{"inputAudioTracks":{}}"#,
        r#"{"inputAudioTracks":{"1":true}}"#,
        r#"{"inputAudioTracks":null}"#,
        r#"{"inputAudioTracks":[]}"#,
        r#"{"inputAudioTracks":"not record"}"#,
        r#"{"inputAudioTracks":{"1":"bad","2":false,"3":true,"4":false,"6":false,"z":true}}"#,
        r#"{"inputAudioTracks":{"1":true,"2":false,"3":true,"4":false,"5":true,"6":false,"z":true,"7":false,"a":true,"01":false,"4294967295":true,"9":true}}"#,
        r#"{"inputAudioTracks":{"1":true,"2":false,"3":true,"4":false,"5":true,"6":false,"z":true,"a":true,"z":false}}"#,
        r#"{"inputName":null,"inputUuid":42,"inputAudioTracks":{"1":true,"2":false,"3":true,"4":false,"5":true,"6":false,"constructor":true,"__proto__":true,"日本語":false,"quote\"key":true,"line\nkey":false}}"#,
    ];
    for legacy in [false, true] {
        let expected = oracle(&cases, legacy);
        assert_eq!(expected["dispatches"], 0);
        for (args, result) in cases.iter().zip(expected["results"].as_array().unwrap()) {
            assert_eq!(result["isError"], true);
            let parsed = serde_json::from_str(args).unwrap();
            assert_eq!(
                tool_validation::tool_error_from_frame(
                    "obs-set-input-audio-tracks",
                    schema(),
                    &parsed,
                    &frame(args)
                )
                .unwrap(),
                *result,
                "original JSON {args}; legacy={legacy}"
            );
        }
    }
}
#[test]
fn record_projection_cannot_admit_work_rebind_arguments_or_escape_its_bounds() {
    let valid =
        json!({"inputAudioTracks":{"1":true,"2":false,"3":true,"4":false,"5":true,"6":false}});
    let raw = serde_json::to_string(&valid).unwrap();
    assert_eq!(
        tool_validation::tool_error_from_frame(
            "obs-set-input-audio-tracks",
            schema(),
            &valid,
            &frame(&raw)
        ),
        Err(ProjectionUnavailable::NoIssues)
    );
    assert_eq!(
        tool_validation::tool_error_from_frame(
            "obs-set-input-volume",
            schema(),
            &json!({}),
            &frame("{}")
        ),
        Err(ProjectionUnavailable::UnknownTool)
    );
    assert_eq!(
        tool_validation::tool_error_from_frame(
            "obs-set-input-audio-tracks",
            schema(),
            &json!({}),
            &frame(&raw)
        ),
        Err(ProjectionUnavailable::UnsupportedSchema)
    );
    let huge = vec![b' '; 8 * 1024 * 1024 + 1];
    assert_eq!(
        tool_validation::tool_error_from_frame(
            "obs-set-input-audio-tracks",
            schema(),
            &json!({}),
            &huge
        ),
        Err(ProjectionUnavailable::InputLimit)
    );
    let changed = json!({"type":"object"});
    assert_eq!(
        tool_validation::tool_error_from_frame(
            "obs-set-input-audio-tracks",
            &changed,
            &json!({}),
            &frame("{}")
        ),
        Err(ProjectionUnavailable::SchemaMismatch)
    );
}
#[test]
fn closed_admission_preserves_exhaustive_tracks_and_non_schema_volume_business_validation() {
    let valid = json!({"inputUuid":"literal uuid","inputAudioTracks":{"1":true,"2":false,"3":true,"4":false,"5":true,"6":false}});
    let call = Call::parse("obs-set-input-audio-tracks", &valid).unwrap();
    assert_eq!(call.kind(), GenericKind::InputAudioTracks);
    assert_eq!(call.arguments(), valid);
    assert_eq!(
        call.retained_bytes().unwrap(),
        serde_json::to_vec(&valid).unwrap().len()
    );
    for invalid in [
        json!({}),
        json!({"inputAudioTracks":null}),
        json!({"inputAudioTracks":{"1":true}}),
        json!({"inputAudioTracks":{"1":true,"2":false,"3":true,"4":false,"5":true,"6":false,"7":true}}),
    ] {
        assert!(Call::parse("obs-set-input-audio-tracks", &invalid).is_none());
    }
    let missing = Call::parse("obs-set-input-volume", &json!({"inputName":"App"})).unwrap();
    assert_eq!(missing.kind(), GenericKind::InputVolume);
    assert_eq!(missing.arguments(), json!({"inputName":"App"}));
    assert_eq!(
        missing.unsupported(),
        json!({"content":[{"type":"text","text":"Error setting input volume: Either inputVolumeMul or inputVolumeDb must be provided"}],"isError":true})
    );
    for invalid in [
        json!({"inputName":"App","inputVolumeMul":null}),
        json!({"inputName":"App","inputVolumeDb":27}),
        json!({"inputName":"App","inputVolumeMul":-0.1}),
    ] {
        assert!(Call::parse("obs-set-input-volume", &invalid).is_none());
    }
    assert_eq!(
        Call::parse(
            "obs-set-input-volume",
            &json!({"inputName":"App","inputVolumeMul":20,"inputVolumeDb":-100})
        )
        .unwrap()
        .arguments(),
        json!({"inputName":"App","inputVolumeMul":20,"inputVolumeDb":-100})
    );
    let replay = Call::parse("obs-save-replay-buffer", &json!({"discarded":true})).unwrap();
    assert_eq!(replay.kind(), GenericKind::SaveReplayBuffer);
    assert_eq!(replay.arguments(), json!({}));
    assert_eq!(replay.retained_bytes(), Some(2));
}
