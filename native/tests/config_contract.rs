// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::config::{self, Platform, Unsupported};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    collections::HashMap,
    path::{Path, PathBuf},
    process::Command,
};

#[test]
fn path_candidates_match_actual_typescript_join_and_selection() {
    let cases = json!([
     {"platform":"darwin","home":"/Users/fixture","env":{}},
     {"platform":"linux","home":"/home/fixture","env":{}},
     {"platform":"linux","home":"/home/fixture/../other","env":{"XDG_CONFIG_HOME":"relative/../config"}},
     {"platform":"linux","home":"/home/fixture","env":{"XDG_CONFIG_HOME":""}},
     {"platform":"win32","home":"C:/Users/fixture","env":{}},
     {"platform":"win32","home":"C:/Users/fixture","env":{"APPDATA":""}},
     {"platform":"win32","home":"C:/Users/fixture","env":{"APPDATA":"C:/Temp/../Roaming"}}
    ]);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let oracle=Command::new("node").args(["--input-type=module","-e","import {obsWebSocketConfigPaths as paths} from './build/obs-config.js';console.log(JSON.stringify(JSON.parse(process.env.CONFIG_TEST_INPUT).map(c=>paths(c.env,c.platform,c.home))));"])
  .current_dir(root).env("CONFIG_TEST_INPUT",cases.to_string()).output().unwrap();
    assert!(oracle.status.success());
    let expected: Value = serde_json::from_slice(&oracle.stdout).unwrap();
    for (i, case) in cases.as_array().unwrap().iter().enumerate() {
        let platform = match case["platform"].as_str().unwrap() {
            "darwin" => Platform::Mac,
            "win32" => Platform::Windows,
            _ => Platform::Linux,
        };
        let paths = config::config_paths(
            |name| case["env"][name].as_str().map(str::to_owned),
            platform,
            Path::new(case["home"].as_str().unwrap()),
        );
        assert_eq!(json!(paths), expected[i]);
    }
}
#[test]
fn resolution_precedence_and_diagnostics_match_actual_source_without_exposing_debug_secrets() {
    let cases = json!([
     {"env":{"OBS_WEBSOCKET_PASSWORD":"TEST_ONLY_EXPLICIT","OBS_MCP_READ_OBS_CONFIG":"true"},"files":{}},
     {"env":{},"files":{"/first":{"server_password":"TEST_ONLY_SAVED"}}},
     {"env":{"OBS_MCP_READ_OBS_CONFIG":"yes"},"files":{"/first":{"auth_required":false,"server_password":"TEST_ONLY_SAVED"}}},
     {"env":{"OBS_MCP_READ_OBS_CONFIG":" TRUE "},"files":{"/second":{"auth_required":true,"server_password":"TEST_ONLY_SAVED"}}},
     {"env":{"OBS_MCP_READ_OBS_CONFIG":"\u{feff}true\u{feff}"},"files":{"/first":{"server_password":"TEST_ONLY_SAVED"}}},
     {"env":{"OBS_MCP_READ_OBS_CONFIG":"\u{85}true\u{85}"},"files":{"/first":{"server_password":"TEST_ONLY_SAVED"}}},
     {"env":{"OBS_MCP_READ_OBS_CONFIG":"1"},"files":{"/first":{"server_password":""},"/second":{"server_password":"TEST_ONLY_SECOND"}}},
     {"env":{"OBS_MCP_READ_OBS_CONFIG":"yes"},"files":{"/first":[],"/second":false}}
    ]);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let oracle=Command::new("node").args(["--input-type=module","-e",r#"
import {resolveObsPassword,readObsConfigPassword} from './build/obs-config.js';
const results=[];console.error=(...parts)=>results.at(-1).diagnostic=parts.join(' ').replace(/^\[obs-mcp\] /,'');
for(const c of JSON.parse(process.env.CONFIG_TEST_INPUT)){const r={password:null,diagnostic:null,reads:[]};results.push(r);r.password=resolveObsPassword(c.env,()=>readObsConfigPassword(['/first','/second'],p=>{r.reads.push(p);if(!(p in c.files))throw Error('missing');return JSON.stringify(c.files[p]);}));}
console.log(JSON.stringify(results));
"#]).current_dir(root).env("CONFIG_TEST_INPUT",cases.to_string()).output().unwrap();
    assert!(oracle.status.success());
    let expected: Value = serde_json::from_slice(&oracle.stdout).unwrap();
    for (i, case) in cases.as_array().unwrap().iter().enumerate() {
        let mut reads = vec![];
        let result = config::resolve_password(
            |name| case["env"][name].as_str().map(str::to_owned),
            || Ok(vec![PathBuf::from("/first"), PathBuf::from("/second")]),
            |p| {
                let p = p.to_str().unwrap();
                reads.push(p.to_owned());
                Ok(case["files"].get(p).map(Value::to_string))
            },
        )
        .unwrap();
        assert_eq!(
            json!({"password":result.password(),"diagnostic":result.diagnostic(),"reads":reads}),
            expected[i]
        );
        assert!(!format!("{result:?}").contains("TEST_ONLY_"));
    }
}
#[test]
fn opt_in_never_bypasses_password_precedence_or_reads_without_permission() {
    let calls = Cell::new(0);
    for env in [
        HashMap::new(),
        HashMap::from([
            ("OBS_WEBSOCKET_PASSWORD", "TEST_ONLY_EXPLICIT"),
            ("OBS_MCP_READ_OBS_CONFIG", "true"),
        ]),
    ] {
        config::resolve_password(
            |k| env.get(k).map(|v| v.to_string()),
            || {
                calls.set(calls.get() + 1);
                Err(Unsupported::HomeUnavailable)
            },
            |_| panic!("must not read"),
        )
        .unwrap();
    }
    assert_eq!(calls.get(), 0);
}
#[test]
fn unsupported_json_and_bounded_encoding_select_the_source_owner() {
    let get = |k: &str| (k == "OBS_MCP_READ_OBS_CONFIG").then(|| "true".into());
    assert_eq!(
        config::resolve_password(
            get,
            || Ok(vec!["/fixture".into()]),
            |_| Ok(Some("{malformed".into()))
        )
        .unwrap_err(),
        Unsupported::EncodingOrSize
    );
    assert_eq!(
        config::resolve_password(
            get,
            || Ok(vec!["/fixture".into()]),
            |_| Err(Unsupported::EncodingOrSize)
        )
        .unwrap_err(),
        Unsupported::EncodingOrSize
    );
}
