// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::tool_validation::{ProjectionUnavailable, tool_error};
use serde_json::{Value, json};
use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::OnceLock,
};
fn catalog() -> &'static Value {
    static CATALOG: OnceLock<Value> = OnceLock::new();
    CATALOG.get_or_init(|| serde_json::from_str(include_str!("../assets/tools.json")).unwrap())
}
fn schema(name: &str) -> &'static Value {
    &catalog()["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == name)
        .unwrap()["inputSchema"]
}
fn oracle(cases: &[(String, Value)], legacy: bool) -> Value {
    let script = r#"
import{Client}from'@modelcontextprotocol/client';import{StdioClientTransport,getDefaultEnvironment}from'@modelcontextprotocol/client/stdio';import{mkdtempSync,rmSync,chmodSync}from'node:fs';import{join}from'node:path';
const chunks=[];for await(const chunk of process.stdin)chunks.push(chunk);const cases=JSON.parse(Buffer.concat(chunks));
const serverCode=`import{McpServer}from'@modelcontextprotocol/server';import{serveStdio}from'@modelcontextprotocol/server/stdio';import{initialize}from'./build/tools/index.js';
const fake={isConnected:()=>false,getConnectionStatus:()=>({}),on(){},off(){},async sendRequest(){process.stderr.write('UNEXPECTED_DISPATCH\\n');throw Error('unexpected OBS request')}};
serveStdio(()=>{const server=new McpServer({name:'validation-oracle',version:'1'});initialize(server,fake,{resourcesAndPrompts:false,confirmLive:false});return server});`;
const client=new Client({name:'validation-consumer',version:'1'},{versionNegotiation:{mode:process.argv[1]==='legacy'?'legacy':{pin:'2026-07-28'}}});
const state=mkdtempSync(join(process.cwd(),'native/target/validation-oracle-'));chmodSync(state,0o700);
const transport=new StdioClientTransport({command:process.execPath,args:['--input-type=module','-e',serverCode],cwd:process.cwd(),env:{...getDefaultEnvironment(),OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_STATE_DIR:state},stderr:'pipe'});let diagnostics='';transport.stderr?.on('data',b=>diagnostics=(diagnostics+String(b)).slice(-65536));
try{await client.connect(transport);const results=[];for(const[name,args]of cases){const result=await client.callTool({name,arguments:args});
// Transport-owned modern serverInfo is tested by ReplyContext integration;
// this helper owns only the exact content/isError payload, with no metadata.
delete result._meta;results.push(result)}process.stdout.write(JSON.stringify({results,dispatches:diagnostics.split('UNEXPECTED_DISPATCH').length-1}));}finally{await client.close();await transport.close();rmSync(state,{recursive:true,force:true})}
"#;
    let mut child = Command::new("node")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
        .args([
            "--input-type=module",
            "-e",
            script,
            if legacy { "legacy" } else { "modern" },
        ])
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
        "oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn compare(cases: &[(String, Value)], legacy: bool) {
    let source = oracle(cases, legacy);
    assert_eq!(source["dispatches"], 0);
    for ((name, args), expected) in cases.iter().zip(source["results"].as_array().unwrap()) {
        assert_eq!(expected["isError"], true, "{name}: {expected}");
        assert_eq!(
            tool_error(name, schema(name), args).unwrap_or_else(|e| panic!("{name}: {e}")),
            *expected,
            "{name}: {args}"
        );
    }
}
#[test]
fn every_qualified_read_field_type_error_matches_actual_sdk_and_declared_field_order() {
    let mut cases = vec![];
    for (name, definition) in catalog()["requests"].as_object().unwrap() {
        let properties = definition["inputSchema"]["properties"].as_object().unwrap();
        if properties.is_empty() {
            assert_eq!(
                tool_error(name, &definition["inputSchema"], &json!({})),
                Err(ProjectionUnavailable::NoIssues)
            );
            continue;
        }
        cases.push((
            name.clone(),
            Value::Object(
                properties
                    .keys()
                    .map(|name| (name.clone(), Value::Null))
                    .collect(),
            ),
        ));
        if definition["inputSchema"]["required"]
            .as_array()
            .is_some_and(|r| !r.is_empty())
        {
            cases.push((name.clone(), json!({})));
        }
    }
    assert_eq!(catalog()["requests"].as_object().unwrap().len(), 59);
    for legacy in [false, true] {
        compare(&cases, legacy);
    }
}
#[test]
fn pinned_sdk_zod_scalar_enum_integer_uuid_and_array_issue_messages_match() {
    let cases=vec![
        ("obs-get-input-settings",json!({})),
        ("obs-get-input-settings",json!({"inputName":42,"unknown":"SYNTHETIC-PRIVATE-VALUE"})),
        ("obs-get-input-settings",json!({"inputName":false})),
        ("obs-get-input-settings",json!({"inputName":[]})),
        ("obs-get-scene-item-transform",json!({"sceneName":42,"sceneUuid":null,"canvasUuid":true,"sceneItemId":-1})),
        ("obs-get-scene-item-transform",json!({"sceneItemId":-1.5})),
        ("obs-get-scene-item-transform",json!({"sceneItemId":9007199254740992_u64})),
        ("obs-get-scene-item-transform",json!({"sceneItemId":-9007199254740992_i64})),
        ("obs-take-start",json!({"expectSilent":"private","lock":null,"minFreeDiskMb":-1,"maxDurationSeconds":3601,"chapter":""})),
        ("obs-take-start",json!({"maxDurationSeconds":9007199254740992_u64})),
        ("obs-claim-control",json!({"minutes":0,"reason":"x".repeat(201)})),
        ("obs-prepare-demo",json!({"sceneName":3,"inputName":true,"apply":[],"ignored":"SYNTHETIC-PRIVATE-VALUE"})),
        ("obs-take-scene",json!({"stageId":"SYNTHETIC-PRIVATE-VALUE"})),
        ("obs-take-scene",json!({"stageId":3})),
        ("obs-take-scene",json!({})),
        ("obs-recover-demo",json!({"operationId":"00000000-0000-0000-9000-000000000000","apply":"private"})),
        ("obs-contact-sheet",json!({"path":null,"times":[-1,"private",null],"columns":0,"width":641})),
        ("obs-contact-sheet",json!({"path":"private","times":vec![-1;25]})),
        ("obs-record-clip",json!({"durationSeconds":0,"chapters":[{"atSeconds":-1,"name":""},{"atSeconds":"private","name":null}],"expectSilent":"private"})),
        ("obs-capture-window",json!({"backend":42})),
        ("obs-capture-window",json!({"backend":"SYNTHETIC-PRIVATE-VALUE"})),
        ("obs-stage-scene",json!({"sceneCollection":"","sceneName":"😀".repeat(257)})),
    ].into_iter().map(|(name,args)|(name.to_owned(),args)).collect::<Vec<_>>();
    for legacy in [false, true] {
        compare(&cases, legacy);
    }
}
#[test]
fn unknown_fields_and_valid_arguments_never_become_invented_errors() {
    assert_eq!(
        tool_error(
            "obs-get-input-settings",
            schema("obs-get-input-settings"),
            &json!({"inputName":"fixture","ignored":"SYNTHETIC-PRIVATE-VALUE"})
        ),
        Err(ProjectionUnavailable::NoIssues)
    );
    for id in [
        "00000000-0000-0000-0000-000000000000",
        "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
        "12345678-1234-8123-a123-123456789abc",
    ] {
        assert_eq!(
            tool_error(
                "obs-take-scene",
                schema("obs-take-scene"),
                &json!({"stageId":id})
            ),
            Err(ProjectionUnavailable::NoIssues)
        );
    }
    assert_eq!(
        tool_error(
            "obs-get-version",
            schema("obs-get-version"),
            &json!({"ignored":true})
        ),
        Err(ProjectionUnavailable::NoIssues)
    );
    // Actual pinned Zod counts Unicode code points; do not invent an error
    // when an external UTF-16-based validator rejects this valid input.
    assert_eq!(
        tool_error(
            "obs-stage-scene",
            schema("obs-stage-scene"),
            &json!({"sceneCollection":"fixture","sceneName":"😀".repeat(256)})
        ),
        Err(ProjectionUnavailable::NoIssues)
    );
    let source=Command::new("node").current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()).args(["--input-type=module","-e","import{z}from'zod';console.log(z.string().max(256).safeParse('😀'.repeat(256)).success)"]).output().unwrap();
    assert!(source.status.success());
    assert_eq!(source.stdout, b"true\n");
}
#[test]
fn unsupported_contexts_drift_and_input_output_caps_are_explicit_and_redacted() {
    assert_eq!(
        tool_error("unknown-private-tool", &json!({}), &json!({})),
        Err(ProjectionUnavailable::UnknownTool)
    );
    let mut changed = schema("obs-get-input-settings").clone();
    changed["properties"]["inputName"]["description"] = json!("drift");
    assert_eq!(
        tool_error("obs-get-input-settings", &changed, &json!({})),
        Err(ProjectionUnavailable::SchemaMismatch)
    );
    assert_eq!(
        // Non-object RPC envelopes belong to the protocol owner. Diagnostic
        // schema support alone cannot project or authorize that envelope.
        tool_error(
            "obs-get-input-settings",
            schema("obs-get-input-settings"),
            &Value::Null
        ),
        Err(ProjectionUnavailable::UnsupportedSchema)
    );
    assert_eq!(
        tool_error(
            "obs-get-input-settings",
            schema("obs-get-input-settings"),
            &json!({"inputName":true,"ignored":"x".repeat(1024*1024+1)})
        ),
        Err(ProjectionUnavailable::InputLimit)
    );
    let mut deep = json!(false);
    for _ in 0..34 {
        deep = json!([deep]);
    }
    assert_eq!(
        tool_error(
            "obs-get-input-settings",
            schema("obs-get-input-settings"),
            &json!({"inputName":deep})
        ),
        Err(ProjectionUnavailable::InputLimit)
    );
    assert_eq!(
        tool_error(
            "obs-audit-scene",
            schema("obs-audit-scene"),
            &json!({"sceneCollection":"fixture","sceneName":"fixture","requiredItems":vec![json!({"sourceName":true,"sourceUuid":false});64]})
        ),
        Err(ProjectionUnavailable::OutputLimit)
    );
    let error=tool_error("obs-get-input-settings",schema("obs-get-input-settings"),&json!({"inputName":{"private":"SYNTHETIC-PRIVATE-VALUE"},"unknown-private-key":"SYNTHETIC-PRIVATE-VALUE"})).unwrap();
    assert!(!error.to_string().contains("SYNTHETIC-PRIVATE-VALUE"));
    assert!(!error.to_string().contains("unknown-private-key"));
    for unavailable in [
        ProjectionUnavailable::UnknownTool,
        ProjectionUnavailable::SchemaMismatch,
        ProjectionUnavailable::UnsupportedSchema,
        ProjectionUnavailable::InputLimit,
        ProjectionUnavailable::OutputLimit,
        ProjectionUnavailable::NoIssues,
    ] {
        assert!(!format!("{unavailable} {unavailable:?}").contains("SYNTHETIC-PRIVATE-VALUE"));
    }
}

#[test]
fn opaque_record_diagnostics_match_source_without_admitting_raw_batch_execution() {
    // Unknown-valued records are now supported for closed settings callbacks.
    // That also makes this schema diagnosable; route admission is independent.
    for legacy in [false, true] {
        compare(&[("obs-batch".into(), json!({}))], legacy);
    }
    for owner in ["requests", "locals"] {
        assert!(catalog()[owner].get("obs-batch").is_none());
    }
    assert!(catalog()["service"]["handlers"].get("obs-batch").is_none());
}
