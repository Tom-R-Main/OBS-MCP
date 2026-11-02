// SPDX-License-Identifier: GPL-2.0-only
//! Library metadata against actual frozen Source controllers. No public mode.
#[path = "support/output_start_three.rs"]
mod support;
use obs_mcp_native::dynamic_visibility::{
    Availability, CaptureProfile, Contract, Effect, Error, Projection, Settings, Visibility,
};
use serde_json::{Value, json};
fn oracle() -> Value {
    serde_json::from_str(include_str!("fixtures/dynamic_visibility_source.json")).unwrap()
}
fn catalog() -> Value {
    serde_json::from_str(include_str!("../assets/tools.json")).unwrap()
}
fn contract() -> Contract {
    Contract::from_source(&catalog(), &oracle()["contract"]).unwrap()
}
fn settings(value: &Value) -> Settings {
    Settings::parse(|name| value.get(name).and_then(Value::as_str).map(str::to_owned))
}
fn owner(options: &Value, profile: CaptureProfile, masked: &[&str]) -> Visibility {
    let c = contract();
    let available = Availability::new(
        &c,
        c.known_names()
            .into_iter()
            .filter(|name| !masked.contains(name))
            .map(str::to_owned),
        profile,
    )
    .unwrap();
    Visibility::new(c, available, settings(options)).unwrap()
}
fn snapshot(owner: &Visibility) -> Value {
    json!({"registered":owner.registered_names(),"enabled":owner.enabled_names(),"list":owner.list().ok().map(|result|result.result)})
}
#[test]
fn actual_source_registry_defaults_filters_results_duplicates_order_and_startup_errors_match() {
    let oracle = oracle();
    for case in oracle["cases"].as_array().unwrap() {
        let c = contract();
        let available = Availability::new(
            &c,
            c.known_names().into_iter().map(str::to_owned),
            CaptureProfile::Standalone,
        )
        .unwrap();
        let built = Visibility::new(c, available, settings(&case["env"]));
        if let Some(expected) = case["error"].as_str() {
            assert_eq!(
                format!("Error: {}", built.unwrap_err()),
                expected,
                "{}",
                case["label"]
            );
            continue;
        }
        let mut owner = built.unwrap();
        assert_eq!(snapshot(&owner), case["initial"], "{}", case["label"]);
        for step in case["steps"].as_array().unwrap() {
            let command = &step["command"];
            let groups = command["groups"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect();
            let enable = command["enable"].as_bool().unwrap();
            let transition = owner.set_groups(groups, enable).unwrap();
            assert_eq!(
                transition.projection,
                Projection::CompleteSourceAvailability
            );
            assert_eq!(transition.result, step["result"], "{}", case["label"]);
            let expected = step["result"]["structuredContent"]["changed"]
                .as_array()
                .unwrap()
                .iter()
                .map(|name| Effect::VisibilityChanged {
                    name: name.as_str().unwrap().to_owned(),
                    enabled: enable,
                })
                .collect::<Vec<_>>();
            assert_eq!(transition.effects, expected);
            assert_eq!(snapshot(&owner), step["after"], "{}", case["label"]);
        }
        assert_eq!(case["obsRequests"], 0);
    }
}
#[test]
fn known_availability_registered_and_enabled_are_distinct_and_missing_names_refuse() {
    let owner = owner(
        &json!({"OBS_MCP_DYNAMIC_TOOLSETS":"true","OBS_MCP_READ_ONLY":"true"}),
        CaptureProfile::Standalone,
        &["obs-get-stats"],
    );
    assert_eq!(
        owner.state("obs-get-stats").unwrap(),
        &obs_mcp_native::dynamic_visibility::State {
            available: false,
            registered: false,
            enabled: false
        }
    );
    assert_eq!(
        owner.state("obs-stop-record").unwrap(),
        &obs_mcp_native::dynamic_visibility::State {
            available: true,
            registered: false,
            enabled: false
        }
    );
    assert_eq!(
        owner.state("obs-get-record-status").unwrap(),
        &obs_mcp_native::dynamic_visibility::State {
            available: true,
            registered: true,
            enabled: false
        }
    );
    assert!(owner.state("missing-tool").is_none());
    let c = contract();
    assert!(matches!(
        Availability::new(&c, ["missing-tool".into()], CaptureProfile::Standalone),
        Err(Error::Contract)
    ));
}
#[test]
fn masked_unsupported_and_capture_stream_names_cannot_be_enabled_even_when_explicit() {
    let mut visible = owner(
        &json!({"OBS_MCP_DYNAMIC_TOOLSETS":"true","OBS_MCP_TOOLSETS":"all","OBS_MCP_TOOLS":"obs-call-request,obs-stop-record,obs-start-stream"}),
        CaptureProfile::TakeOrMedia,
        &["obs-call-request", "obs-stop-record"],
    );
    for enable in [false, true, false, true] {
        let changed = visible
            .set_groups(
                vec!["stream".into(), "record".into(), "protocol".into()],
                enable,
            )
            .unwrap();
        assert_eq!(changed.projection, Projection::AvailabilityRestricted);
        for name in [
            "obs-start-stream",
            "obs-stop-stream",
            "obs-toggle-stream",
            "obs-call-request",
            "obs-stop-record",
        ] {
            assert_eq!(
                visible.state(name).unwrap(),
                &obs_mcp_native::dynamic_visibility::State {
                    available: false,
                    registered: false,
                    enabled: false
                }
            );
            assert!(!changed.effects.iter().any(
                |effect| matches!(effect,Effect::VisibilityChanged{name:found,..} if found==name)
            ));
        }
    }
    let facts = visible.group_facts();
    assert_eq!(
        facts
            .iter()
            .map(|g| g.counts.known - g.counts.available)
            .sum::<usize>(),
        5
    );
    assert!(facts.iter().all(|g| g.counts.enabled <= g.counts.registered
        && g.counts.registered <= g.counts.available
        && g.counts.available <= g.counts.known));
    let standalone = owner(
        &json!({"OBS_MCP_DYNAMIC_TOOLSETS":"true","OBS_MCP_TOOLSETS":"all"}),
        CaptureProfile::Standalone,
        &[],
    );
    for name in ["obs-start-stream", "obs-stop-stream", "obs-toggle-stream"] {
        assert!(standalone.state(name).unwrap().enabled);
    }
}
#[test]
fn invalid_group_requests_are_atomic_and_sdk_error_examples_remain_frontend_owned() {
    let mut owner = owner(
        &json!({"OBS_MCP_DYNAMIC_TOOLSETS":"true","OBS_MCP_TOOLSETS":"all"}),
        CaptureProfile::Standalone,
        &[],
    );
    let before = snapshot(&owner);
    for groups in [
        vec![],
        vec!["core".into()],
        vec!["record".into(), "missing".into()],
        vec!["all".into()],
    ] {
        assert!(matches!(
            owner.set_groups(groups, true),
            Err(Error::InvalidArguments)
        ));
        assert_eq!(snapshot(&owner), before);
    }
    let fixture = oracle();
    let errors = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["label"] == "dynamic-all")
        .unwrap()["invalid"]
        .as_array()
        .unwrap();
    assert_eq!(errors.len(), 5);
    assert!(
        errors
            .iter()
            .all(|entry| entry["result"]["isError"] == true)
    );
    // Typed rejection is not a claim to implement SDK JSON/schema projection.
}
#[test]
fn transition_payloads_are_owned_and_static_selection_has_no_controllers() {
    let mut visible = owner(
        &json!({"OBS_MCP_DYNAMIC_TOOLSETS":"true"}),
        CaptureProfile::Standalone,
        &[],
    );
    let first = visible
        .set_groups(vec!["record".into(), "record".into()], true)
        .unwrap();
    let saved = first.result.clone();
    visible.set_groups(vec!["record".into()], false).unwrap();
    assert_eq!(first.result, saved);
    assert!(!first.effects.is_empty());
    assert!(visible.controllers_available());
    let static_owner = owner(&json!({}), CaptureProfile::Standalone, &[]);
    assert!(!static_owner.controllers_available());
    assert!(matches!(
        static_owner.list(),
        Err(Error::UnavailableController)
    ));
}
#[test]
fn source_contract_mismatch_cannot_construct_an_owner() {
    for mutate in 0..4 {
        let mut source = oracle()["contract"].clone();
        let mut catalog = catalog();
        match mutate {
            0 => {
                source["groups"].as_array_mut().unwrap().pop();
            }
            1 => source["groups"][0]["group"] = json!("wrong"),
            2 => source["registryOrder"][1] = source["registryOrder"][0].clone(),
            _ => {
                catalog["toolsets"]["toolGroups"]
                    .as_object_mut()
                    .unwrap()
                    .remove("obs-stop-record");
            }
        }
        assert!(matches!(
            Contract::from_source(&catalog, &source),
            Err(Error::Contract)
        ));
    }
}
#[tokio::test]
async fn visibility_effects_cannot_consume_held_service_work_lease_receipt_or_writer() {
    use obs_mcp_native::{
        control::ControlDomain,
        generic_requests::Call,
        observation::ReadRequest,
        operation_store::OperationStore,
        service::{ConnectionDescriptor, Options, Resources, Service},
        stage_transport::Cancellation,
    };
    use std::{
        fs,
        sync::{Arc, Mutex},
        time::Duration,
    };
    use support::*;
    let mut visible = owner(
        &json!({"OBS_MCP_DYNAMIC_TOOLSETS":"true"}),
        CaptureProfile::TakeOrMedia,
        &[],
    );
    let socket = Socket::new().await;
    let fixture_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(format!(
            ".dynamic-visibility-fixture-{}",
            std::process::id()
        ));
    fs::create_dir(&fixture_path).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fixture_path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let dir = Dir(fixture_path);
    let (session, bridge) = socket.connect().await;
    let control = Arc::new(Mutex::new(ControlDomain::default()));
    control
        .lock()
        .unwrap()
        .claim("fixture", 7, "retained service work", EPOCH)
        .unwrap();
    let lease = serde_json::to_value(control.lock().unwrap().active_lease(EPOCH)).unwrap();
    let mut serial = 0;
    let mut service = Service::spawn(
        Resources {
            session,
            bridge,
            store: OperationStore::new(&dir.0, &socket.url).unwrap(),
            control: control.clone(),
            clock: clock(),
            identities: Box::new(move || {
                serial += 1;
                id(serial)
            }),
        },
        Options {
            operation_timeout: Duration::from_secs(3),
            state_root: Some(dir.0.clone()),
            connection: Some(ConnectionDescriptor {
                url: socket.url.clone(),
                has_password: false,
            }),
            ..Default::default()
        },
    )
    .map_err(|(error, _)| error)
    .unwrap();
    let handle = service.handle();
    handle
        .read(ReadRequest::Version, Value::Null, Duration::from_secs(3))
        .await
        .unwrap();
    socket.plan(
        "SetInputMute",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let worker = handle.clone();
    let job = tokio::spawn(async move {
        worker
            .generic(
                Call::parse(
                    "obs-set-input-mute",
                    &json!({"inputName":"Fixture","inputMuted":true}),
                )
                .unwrap(),
                "fixture".into(),
                Cancellation::new(),
            )
            .await
    });
    socket.seen("SetInputMute", 1).await;
    let journal = || {
        let mut files = fs::read_dir(dir.0.join("operation-receipts"))
            .unwrap()
            .map(|entry| {
                let p = entry.unwrap().path();
                (p.file_name().unwrap().to_owned(), fs::read(p).unwrap())
            })
            .collect::<Vec<_>>();
        files.sort_by(|a, b| a.0.cmp(&b.0));
        files
    };
    let before = journal();
    let outcomes = [
        visible.set_groups(vec!["inputs".into(), "record".into()], true),
        visible.set_groups(vec!["general".into(), "inputs".into()], false),
    ];
    tokio::task::yield_now().await;
    let still_pending = !job.is_finished();
    let after = journal();
    let still_custodied = service.take_store().is_none();
    let lease_after = serde_json::to_value(control.lock().unwrap().active_lease(EPOCH)).unwrap();
    let physical_count = socket.count("SetInputMute");
    // Settle actual work/socket ownership before any domain assertion can unwind.
    socket.release("SetInputMute").await;
    let result = tokio::time::timeout(WAIT, job)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let joined = service.shutdown().await.unwrap();
    let writer_returned = service.take_store().is_some();
    socket.join().await;
    assert!(outcomes.iter().all(Result::is_ok));
    assert!(still_pending && still_custodied && writer_returned);
    let active_receipts = before
        .iter()
        .filter(|(name, _)| name.to_string_lossy().ends_with(".json"))
        .map(|(_, bytes)| serde_json::from_slice::<Value>(bytes).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(active_receipts.len(), 1);
    assert_eq!(active_receipts[0]["phase"], "dispatching");
    assert_eq!(active_receipts[0]["requestType"], "SetInputMute");
    assert_eq!(before, after);
    assert_eq!(lease, lease_after);
    assert_eq!(physical_count, 1);
    assert_ne!(result["isError"], true);
    assert!(joined.joined && joined.workers_joined && joined.socket_joined);
}
