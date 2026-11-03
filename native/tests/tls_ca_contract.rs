// SPDX-License-Identifier: GPL-2.0-only
//! Real native TLS and OBS-protocol authentication with generated certificate-only
//! configuration. This is a fixture, not a live OBS/platform release admission.
#![cfg(feature = "platform-tls-evaluation")]
use obs_mcp_native::{
    config, network_transport,
    session::{ObsSession, SessionOptions},
};
use serde_json::{Value, json};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::{Instant, timeout},
};
const WAIT: Duration = Duration::from_secs(5);
struct Fixture {
    child: Child,
    input: ChildStdin,
    output: tokio::io::Lines<BufReader<ChildStdout>>,
    port: u16,
    ca: PathBuf,
}
impl Fixture {
    async fn start(mode: &str) -> Self {
        let script =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/support/tls_ca_fixture.py");
        let mut child = Command::new("python3")
            .arg(script)
            .arg(mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
        let ready: Value = serde_json::from_str(
            &timeout(WAIT, output.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        Self {
            child,
            input,
            output,
            port: ready["port"].as_u64().unwrap() as u16,
            ca: ready["ca"].as_str().unwrap().into(),
        }
    }
    async fn command(&mut self, command: &str) -> Value {
        self.input
            .write_all(format!("{command}\n").as_bytes())
            .await
            .unwrap();
        self.input.flush().await.unwrap();
        serde_json::from_str(
            &timeout(WAIT, self.output.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        )
        .unwrap()
    }
    async fn finish(mut self) -> Value {
        let facts = self.command("stop").await;
        let status = timeout(WAIT, self.child.wait()).await.unwrap().unwrap();
        assert!(status.success());
        assert_eq!(facts["closed"], true);
        facts
    }
    fn owner(&self, additional: bool) -> network_transport::Owner {
        let roots = if additional {
            config::additional_tls_roots(Some(self.ca.as_os_str()), true).unwrap()
        } else {
            None
        };
        network_transport::Owner::fixture_with_executable(
            roots,
            PathBuf::from(env!("CARGO_BIN_EXE_obs-mcp-native")),
        )
        .unwrap()
    }
    fn url(&self, ip: bool) -> String {
        format!(
            "wss://{}:{}/",
            if ip { "127.0.0.1" } else { "localhost" },
            self.port
        )
    }
}
#[tokio::test]
async fn additive_ca_authenticates_and_reads_with_tls12_tls13_dns_and_ip() {
    for (mode, ip) in [("valid12", false), ("valid13", false), ("valid13", true)] {
        let fixture = Fixture::start(mode).await;
        let mut network = fixture.owner(true);
        let url = fixture.url(ip);
        // The immutable snapshot must survive removal of its source filename.
        std::fs::remove_file(&fixture.ca).unwrap();
        let established = ObsSession::connect_with_network_connector(
            &url,
            Some("non-user-fixture-password"),
            SessionOptions::default(),
            network.connector(),
            Instant::now() + Duration::from_secs(3),
        )
        .await;
        let establishment_error = format!("{:?}", established.as_ref().err());
        let mut response = None;
        let mut session_join = None;
        if let Ok((session, _events)) = established {
            let handle = session.handle();
            response = Some(
                handle
                    .request(
                        handle.token(),
                        "GetVersion",
                        json!({}),
                        Duration::from_secs(1),
                    )
                    .await,
            );
            session_join = Some(session.shutdown().await);
        }
        let joined = timeout(WAIT, network.shutdown()).await;
        let facts = fixture.finish().await;
        assert!(joined.unwrap().is_ok());
        assert!(
            session_join.is_some_and(|join| join.is_ok()),
            "{mode} establishment failed: {establishment_error}; peer={facts}"
        );
        assert_eq!(
            response.unwrap().unwrap()["obsVersion"],
            "fixture-native-tls"
        );
        assert_eq!(facts["accepted"], 1);
        assert_eq!(facts["upgrades"], 1);
        assert_eq!(facts["authenticated"], 1);
        assert_eq!(facts["reads"], 1);
        assert_eq!(
            facts["tls"][0],
            if mode == "valid12" {
                "TLSv1.2"
            } else {
                "TLSv1.3"
            }
        );
        assert_eq!(facts["alpn"][0], "http/1.1");
    }
}
#[tokio::test]
async fn ca_never_bypasses_issuer_hostname_ip_or_expiry() {
    for (mode, ip, additional) in [
        ("valid13", false, false),
        ("wrongdns", false, true),
        ("wrongip", true, true),
        ("expired", false, true),
    ] {
        let fixture = Fixture::start(mode).await;
        let mut network = fixture.owner(additional);
        let url = fixture.url(ip);
        let result = ObsSession::connect_with_network_connector(
            &url,
            Some("non-user-fixture-password"),
            SessionOptions::default(),
            network.connector(),
            Instant::now() + Duration::from_secs(3),
        )
        .await;
        let refused = matches!(
            &result,
            Err(obs_mcp_native::session::ConnectError::Handshake(_))
        );
        if let Ok((session, _)) = result {
            let _ = session.shutdown().await;
        }
        let joined = timeout(WAIT, network.shutdown()).await;
        let facts = fixture.finish().await;
        assert!(refused, "{mode} unexpectedly accepted");
        assert!(joined.unwrap().is_ok());
        assert_eq!(facts["accepted"], 1);
        assert_eq!(facts["upgrades"], 0);
        assert_eq!(facts["identifies"], 0);
    }
}
#[tokio::test]
async fn abandoned_tls_upgrade_and_identify_keep_cleanup_custody() {
    for (mode, phase) in [
        ("holdtls", "accepted"),
        ("holdupgrade", "upgrades"),
        ("holdidentify", "identifies"),
    ] {
        let mut fixture = Fixture::start(mode).await;
        let mut network = fixture.owner(true);
        let url = fixture.url(false);
        let observed = {
            let connect = ObsSession::connect_with_network_connector(
                &url,
                Some("non-user-fixture-password"),
                SessionOptions::default(),
                network.connector(),
                Instant::now() + Duration::from_secs(3),
            );
            tokio::pin!(connect);
            let command = format!("wait {phase}");
            tokio::select! {facts=fixture.command(&command)=>facts,r=&mut connect=>{drop(r);json!({"unexpectedEarlyCompletion":true})}}
            // The establishment consumer is abandoned here; network stays owned.
        };
        let joined = timeout(WAIT, network.shutdown()).await;
        let facts = fixture.finish().await;
        assert!(
            observed[phase].as_u64().unwrap_or(0) > 0,
            "fixture phase not reached"
        );
        assert!(joined.unwrap().is_ok());
        assert_eq!(facts["accepted"], 1);
        assert!(facts["upgrades"].as_u64().unwrap() <= 1);
    }
}
#[tokio::test]
async fn one_original_deadline_covers_tls_upgrade_identify_and_join() {
    for mode in ["holdtls", "holdupgrade", "holdidentify"] {
        let fixture = Fixture::start(mode).await;
        let mut network = fixture.owner(true);
        let url = fixture.url(false);
        let start = Instant::now();
        let deadline = start + Duration::from_millis(500);
        let result = ObsSession::connect_with_network_connector(
            &url,
            Some("non-user-fixture-password"),
            SessionOptions {
                connect_timeout: Duration::from_secs(10),
                ..SessionOptions::default()
            },
            network.connector(),
            deadline,
        )
        .await;
        let timed_out = matches!(
            &result,
            Err(obs_mcp_native::session::ConnectError::Deadline
                | obs_mcp_native::session::ConnectError::Handshake(_))
        );
        let error_kind = format!("{:?}", result.as_ref().err());
        if let Ok((session, _)) = result {
            let _ = session.shutdown().await;
        }
        let elapsed = start.elapsed();
        let joined = timeout(WAIT, network.shutdown()).await;
        let facts = fixture.finish().await;
        // The retained native connect and outer Session share the same absolute
        // deadline. Either timer may win; require the held phase and elapsed
        // budget rather than relabeling a generic transport error as a cert error.
        assert!(timed_out, "phase {mode}; {error_kind}");
        assert!(
            elapsed >= Duration::from_millis(450),
            "premature error: {error_kind}"
        );
        let phase = match mode {
            "holdtls" => "accepted",
            "holdupgrade" => "upgrades",
            _ => "identifies",
        };
        assert_eq!(facts[phase], 1);
        assert!(elapsed < Duration::from_secs(2));
        eprintln!(
            "{}",
            json!({"deadlinePhase":mode,"error":error_kind,"elapsedMs":elapsed.as_millis(),"joined":joined.as_ref().is_ok_and(|result|result.is_ok()),"accepted":facts["accepted"],"upgrades":facts["upgrades"]})
        );
        assert!(joined.unwrap().is_ok());
        assert_eq!(facts["accepted"], 1);
        assert!(facts["upgrades"].as_u64().unwrap() <= 1);
    }
}

#[tokio::test]
async fn configured_ca_reaches_real_native_mcp_service_and_invalid_setting_refuses() {
    let fixture = Fixture::start("valid13").await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_obs-mcp-native"))
        .env("OBS_MCP_TLS_CA_FILE", &fixture.ca)
        .env("OBS_WEBSOCKET_URL", fixture.url(false))
        .env("OBS_WEBSOCKET_PASSWORD", "non-user-fixture-password")
        .env("OBS_MCP_READ_OBS_CONFIG", "0")
        .env(
            "OBS_MCP_STATE_DIR",
            fixture.ca.parent().unwrap().join("state"),
        )
        .env("OBS_MCP_NATIVE_PROFILE", "production")
        .env("OBS_MCP_READ_ONLY", "1")
        .env_remove("OBS_MCP_NATIVE_RECORDING_ROOT")
        .env_remove("OBS_MCP_FORCE_COMPAT")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
    for message in [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"tls-ca-contract","version":"1"}}}),
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"obs-test-connection","arguments":{}}}),
    ] {
        input
            .write_all(format!("{message}\n").as_bytes())
            .await
            .unwrap();
    }
    input.flush().await.unwrap();
    let response = timeout(WAIT, async {
        loop {
            let line = output.next_line().await.unwrap().unwrap();
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["id"] == 2 {
                return value;
            }
        }
    })
    .await;
    drop(input);
    let status = timeout(WAIT, child.wait()).await;
    let facts = fixture.finish().await;
    assert!(status.unwrap().unwrap().success());
    let response = response.unwrap();
    assert!(response.get("error").is_none(), "{response}");
    assert_ne!(response["result"]["isError"], true, "{response}");
    assert_eq!(facts["accepted"], 1);
    assert_eq!(facts["authenticated"], 1);
    assert!(facts["reads"].as_u64().unwrap() >= 1);
    let output = Command::new(env!("CARGO_BIN_EXE_obs-mcp-native"))
        .env("OBS_MCP_TLS_CA_FILE", "/invalid-additional-ca-do-not-read")
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("OBS_MCP_TLS_CA_FILE"));
    assert!(!error.contains("invalid-additional-ca-do-not-read"));
}

#[tokio::test]
async fn ca_setting_does_not_admit_unsupported_endpoint_schemes() {
    for scheme in [
        "http://localhost:4455/",
        "file:///local/fixture",
        "ftp://localhost:4455/",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_obs-mcp-native"))
            .env("OBS_WEBSOCKET_URL", scheme)
            .env_remove("OBS_MCP_TLS_CA_FILE")
            .stdin(Stdio::null())
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("supported WS/WSS endpoint")
        );
    }
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn native_custom_ca_metadata_certificate_errors_and_callback_quiescence() {
    use obs_mcp_platform_tls::{AdditionalRoots, Supervisor};
    for mode in ["valid12", "valid13", "wrongdns", "wrongip", "expired"] {
        let fixture = Fixture::start(mode).await;
        let roots =
            AdditionalRoots::from_der(config::read_tls_ca_file(&fixture.ca).unwrap()).unwrap();
        let mut owner = Supervisor::with_additional_roots(roots).unwrap();
        let expected = if mode == "wrongip" {
            "127.0.0.1"
        } else {
            "localhost"
        };
        let result = owner
            .connect(
                expected,
                fixture.port,
                (Instant::now() + Duration::from_secs(2)).into_std(),
            )
            .unwrap()
            .await;
        let accepted = result.is_ok();
        drop(result);
        let snapshots = timeout(WAIT, owner.shutdown()).await.unwrap();
        let facts = fixture.finish().await;
        assert_eq!(snapshots.len(), 1);
        let snapshot = &snapshots[0];
        assert!(
            snapshot.cancelled
                && snapshot.released
                && snapshot.pending == 0
                && snapshot.captures == 0
        );
        if mode.starts_with("valid") {
            assert!(accepted);
            assert_eq!(
                snapshot.tls_version,
                Some(if mode == "valid12" { 0x0303 } else { 0x0304 })
            );
            assert_eq!(snapshot.alpn.as_deref(), Some("http/1.1"));
        } else {
            assert!(!accepted);
            assert!(
                snapshot.error.is_some_and(|(domain, _)| domain == 3),
                "expected native TLS certificate refusal, observed {:?}",
                snapshot.error
            );
            assert_eq!(facts["upgrades"], 0);
        }
        eprintln!(
            "{}",
            json!({"mode":mode,"nativeVersion":snapshot.tls_version,"nativeError":snapshot.error,"cancelled":snapshot.cancelled,"released":snapshot.released,"pending":snapshot.pending,"operationCaptures":snapshot.captures})
        );
    }
}

async fn guarded_mcp_preflight(tool: &str, args: Value) {
    let fixture = Fixture::start("valid13").await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_obs-mcp-native"))
        .env("OBS_MCP_TLS_CA_FILE", &fixture.ca)
        .env("OBS_WEBSOCKET_URL", fixture.url(false))
        .env("OBS_WEBSOCKET_PASSWORD", "non-user-fixture-password")
        .env("OBS_MCP_READ_OBS_CONFIG", "0")
        .env(
            "OBS_MCP_STATE_DIR",
            fixture.ca.parent().unwrap().join("state"),
        )
        .env(
            "OBS_MCP_NATIVE_RECORDING_ROOT",
            fixture.ca.parent().unwrap().join("state"),
        )
        .env("OBS_MCP_NATIVE_FFMPEG", "/fixture-no-media-tool")
        .env("OBS_MCP_NATIVE_FFPROBE", "/fixture-no-media-tool")
        .env("OBS_MCP_NATIVE_PROFILE", "production")
        .env("OBS_MCP_READ_ONLY", "0")
        .env_remove("OBS_MCP_FORCE_COMPAT")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
    for message in [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"tls-ca-guarded-contract","version":"1"}}}),
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":tool,"arguments":args}}),
    ] {
        input
            .write_all(format!("{message}\n").as_bytes())
            .await
            .unwrap();
    }
    input.flush().await.unwrap();
    let response = timeout(WAIT, async {
        loop {
            let line = output.next_line().await.unwrap().unwrap();
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["id"] == 2 {
                return value;
            }
        }
    })
    .await;
    drop(input);
    let status = timeout(WAIT, child.wait()).await;
    let facts = fixture.finish().await;
    assert!(status.unwrap().unwrap().success());
    let response = response.unwrap();
    assert!(response.get("error").is_none(), "{tool}: {response}");
    // GetVersion is the only advertised request. A second read is the
    // guarded adapter's own capability bootstrap after its scheme fence;
    // every recording/mutation capability remains absent and undispatched.
    assert!(
        facts["reads"].as_u64().unwrap() >= 2,
        "{tool} failed before adapter capability bootstrap: {response}; {facts}"
    );
    assert_eq!(facts["authenticated"], 1);
    assert_eq!(facts["accepted"], 1);
    assert!(
        facts["requestTypes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|name| name == "GetVersion"),
        "an effect or unadvertised request escaped its fence: {facts}"
    );
    assert_eq!(
        response["result"]["isError"], true,
        "missing guarded capabilities must still refuse: {response}"
    );
}
#[tokio::test]
async fn guarded_demo_reaches_capability_preflight_over_wss_without_effects() {
    guarded_mcp_preflight("obs-prepare-demo",json!({"sceneCollection":"Fixture","sceneName":"Fixture","inputName":"Fixture","apply":false})).await;
}
#[tokio::test]
async fn guarded_take_reaches_capability_preflight_over_wss_without_effects() {
    guarded_mcp_preflight(
        "obs-take-start",
        json!({"expectSilent":true,"minFreeDiskMb":0}),
    )
    .await;
}
