// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::config::{Platform, state_directory};
use serde_json::json;
use std::{path::Path, process::Command};
#[test]
fn state_namespace_selection_matches_actual_node_without_creating_paths() {
    let fixtures = vec![
        (Platform::Mac, "darwin", json!({})),
        (Platform::Linux, "linux", json!({})),
        (Platform::Windows, "win32", json!({})),
        (
            Platform::Mac,
            "darwin",
            json!({"OBS_MCP_STATE_DIR":"relative/../state/"}),
        ),
        (Platform::Linux, "linux", json!({"OBS_MCP_STATE_DIR":" "})),
        (
            Platform::Linux,
            "linux",
            json!({"OBS_MCP_STATE_DIR":"","XDG_STATE_HOME":"/fixture/one/../state"}),
        ),
        (Platform::Linux, "linux", json!({"XDG_STATE_HOME":""})),
        (Platform::Windows, "win32", json!({"LOCALAPPDATA":""})),
        (
            Platform::Windows,
            "win32",
            json!({"LOCALAPPDATA":"/fixture/local/../private"}),
        ),
        (
            Platform::Mac,
            "darwin",
            json!({"XDG_STATE_HOME":"/ignored"}),
        ),
    ];
    let home = "/fixture/user";
    let project = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    for (platform, label, env) in fixtures {
        let input = json!({"env":env,"platform":label,"home":home});
        let output=Command::new("node").args(["--input-type=module","-e","import{stateDirectory}from'./build/state-dir.js';const x=JSON.parse(process.argv[1]);console.log(JSON.stringify(stateDirectory(x.env,x.platform,x.home)));",&input.to_string()]).current_dir(project).output().unwrap();
        assert!(
            output.status.success(),
            "stateDirectory source oracle failed"
        );
        let expected: String = serde_json::from_slice(&output.stdout).unwrap();
        let actual = state_directory(
            |key| env.get(key).and_then(|v| v.as_str()).map(str::to_owned),
            platform,
            Path::new(home),
        );
        assert_eq!(actual.to_str().unwrap(), expected, "{label} {env}");
    }
}
