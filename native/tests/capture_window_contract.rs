// SPDX-License-Identifier: GPL-2.0-only
//! Actual approved compiled Source MCP/fake-OBS fixtures, not a second model.
//! These pure values neither connect OBS nor establish execution authority.
use obs_mcp_native::{
    capture_window::{
        self as capture, Backend, Candidate, Canvas, Discovery, Failure, FailureKind, Kind,
        Readings, Request, Status,
    },
    js_json,
};
use serde_json::{Value, json};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/capture_window_source.json")).unwrap()
}
fn row<'a>(fixture: &'a Value, name: &str) -> &'a Value {
    fixture["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == name)
        .unwrap()
}
fn readings<T>(world: &Value, body: impl FnOnce(Readings<'_>) -> T) -> T {
    let candidates = world["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| Candidate {
            input_uuid: row["inputUuid"].as_str().unwrap(),
            settings: &row["settings"],
            properties: &row["properties"],
        })
        .collect::<Vec<_>>();
    body(Readings {
        scenes: &world["scenes"],
        inputs: &world["inputs"],
        membership: &world["membership"],
        candidates: &candidates,
    })
}
fn discover(row: &Value) -> Result<Discovery, Failure> {
    let request = Request::parse(&row["args"])?;
    let kinds = row["current"]["kinds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    readings(&row["current"], |current| {
        capture::discover(request, row["platform"].as_str(), &kinds, current)
    })
}
fn normalized(mut value: Value) -> Value {
    js_json::normalize_json_numbers(&mut value).unwrap();
    value
}
fn equal(actual: Value, expected: &Value, case: &str) {
    assert_eq!(
        normalized(actual),
        normalized(expected.clone()),
        "Source case {case}"
    );
}
fn statuses(rows: &Value) -> Vec<Status> {
    rows.as_array()
        .unwrap()
        .iter()
        .map(|row| Status {
            request_type: row["requestType"].as_str().unwrap().into(),
            ok: row["requestStatus"]["result"].as_bool().unwrap(),
            code: row["requestStatus"]["code"].as_i64().unwrap(),
            comment: row["requestStatus"]["comment"].as_str().map(str::to_owned),
        })
        .collect()
}
fn audio_ok() -> Vec<Status> {
    ["SetInputMute", "SetInputAudioTracks"]
        .into_iter()
        .map(|name| Status {
            request_type: name.into(),
            ok: true,
            code: 100,
            comment: None,
        })
        .collect()
}

#[test]
fn actual_registered_source_schema_defaults_stripping_and_truthy_business_refusal() {
    let fixture = fixture();
    for schema in fixture["schemas"].as_array().unwrap() {
        let parsed = Request::parse(&schema["args"]);
        assert_eq!(
            parsed.is_ok(),
            schema["ok"].as_bool().unwrap(),
            "schema {}",
            schema["args"]
        );
        if let Ok(request) = parsed {
            equal(request.arguments(), &schema["normalized"], "schema");
        }
    }
    let empty = Request::parse(&json!({"sceneName":"","inputName":"","window":""})).unwrap();
    assert!(empty.selector_refusal().is_some());
    assert_eq!(empty.backend(), Backend::Auto);
    assert!(empty.apply() && empty.fit() && empty.silent() && !empty.include_screenshot());
    let spaces =
        Request::parse(&json!({"sceneName":"Demo","inputName":"Capture","window":" "})).unwrap();
    assert!(spaces.selector_refusal().is_none());
    for name in [
        "windows-refused",
        "mac-linux-backend-refused",
        "mac-application-refused",
    ] {
        let case = row(&fixture, name);
        let request = Request::parse(&case["args"]).unwrap();
        equal(
            request
                .platform_refusal(case["platform"].as_str())
                .unwrap()
                .result(),
            &case["result"],
            name,
        );
    }
}
#[test]
fn actual_source_plan_and_discovery_refusals_preserve_selected_plugin_record() {
    let fixture = fixture();
    let mut compared = 0;
    for case in fixture["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["mode"] == "discover")
    {
        let discovered = discover(case);
        assert_eq!(
            discovered.is_err(),
            case["result"]["isError"].as_bool().unwrap_or(false),
            "discovery admission {}",
            case["name"]
        );
        match discovered {
            Err(error) => {
                equal(
                    error.result(),
                    &case["result"],
                    case["name"].as_str().unwrap(),
                );
                compared += 1;
            }
            Ok(discovery) if !discovery.request().apply() => {
                equal(
                    discovery.plan_result().unwrap(),
                    &case["result"],
                    case["name"].as_str().unwrap(),
                );
                compared += 1;
            }
            Ok(_) => {}
        }
    }
    assert_eq!(compared, 31);
    let discovery = discover(row(&fixture, "mac-plan-unknown-record")).unwrap();
    assert_eq!(
        discovery.selected().unwrap()["pluginMetadata"],
        json!({"opaque":["keep",3.5]})
    );
    assert!(discovery.initial_intent().is_none() && discovery.snapshot_intent().is_none());
}
#[test]
fn actual_source_success_mutation_sequences_and_full_result_messages_match() {
    let fixture = fixture();
    let mut compared = 0;
    for case in fixture["rows"].as_array().unwrap().iter().filter(|row| {
        row["mode"] == "discover" && !row["result"]["isError"].as_bool().unwrap_or(false)
    }) {
        let discovery = discover(case).unwrap();
        if !discovery.request().apply() {
            continue;
        }
        let mut effects = vec![];
        if let Some(effect) = discovery.initial_intent() {
            effects.push(effect.projection());
        }
        let target = discovery.existing().cloned().unwrap_or_else(|| {
            discovery
                .created_target(&json!({"inputUuid":"created-uuid"}))
                .unwrap()
        });
        effects.extend(
            discovery
                .audio_intents(&target)
                .unwrap()
                .iter()
                .map(|effect| effect.projection()),
        );
        let canvas = Canvas::parse(&json!({"baseWidth":1920,"baseHeight":1080})).unwrap();
        let fit = discovery
            .fit_intent(
                &target,
                &case["after"]["scenes"],
                &case["after"]["membership"],
                &json!({"baseWidth":1920,"baseHeight":1080}),
                &json!({"sceneItemId":7}),
            )
            .unwrap();
        if let Some(fit) = fit {
            assert_eq!(fit.target().unwrap(), &target);
            effects.push(fit.projection());
        }
        equal(
            Value::Array(effects),
            &case["mutations"],
            case["name"].as_str().unwrap(),
        );
        let snapshot = discovery.existing().map(|_| "<snapshot>");
        equal(
            discovery
                .result(
                    snapshot,
                    &case["state"],
                    &audio_ok(),
                    discovery.request().fit().then_some(canvas),
                )
                .unwrap(),
            &case["result"],
            case["name"].as_str().unwrap(),
        );
        if let Some(snapshot) = discovery.snapshot_intent() {
            assert_eq!(
                snapshot.projection(),
                json!({"inputs":["Capture"],"scenes":["Demo"],"label":"before obs-capture-window","auto":true})
            );
        }
        compared += 1;
    }
    assert_eq!(compared, 7);
}
#[test]
fn created_uuid_and_exact_silence_ack_failures_match_corrected_source() {
    let fixture = fixture();
    let case = row(&fixture, "created-uuid-required");
    let discovery = discover(case).unwrap();
    equal(
        discovery
            .created_target(&json!({"sceneItemId":7}))
            .unwrap_err()
            .result(),
        &case["result"],
        "missing created UUID",
    );
    assert_eq!(
        discovery.initial_intent().unwrap().projection(),
        case["mutations"][0]
    );
    for case in fixture["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["mode"] == "silence")
    {
        let error = capture::validate_silence(&statuses(&case["ack"])).unwrap_err();
        equal(
            error.result(),
            &case["result"],
            case["name"].as_str().unwrap(),
        );
    }
    let mut wrong = audio_ok();
    wrong.swap(0, 1);
    assert!(capture::validate_silence(&wrong).is_err());
    let mut extra = audio_ok();
    extra.push(extra[0].clone());
    assert!(capture::validate_silence(&extra).is_err());
    assert!(capture::validate_silence(&audio_ok()).is_ok());
}
#[test]
fn post_snapshot_full_identity_comparison_and_persisted_fact_match_source() {
    let fixture = fixture();
    let mut compared = 0;
    for case in fixture["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["mode"] == "compare")
    {
        let discovery = discover(case).unwrap();
        let error = readings(&case["fresh"], |fresh| discovery.compare(fresh)).unwrap_err();
        equal(
            error.after_snapshot("<snapshot>").unwrap(),
            &case["result"],
            case["name"].as_str().unwrap(),
        );
        compared += 1;
    }
    assert_eq!(compared, 6);
}
#[test]
fn unknown_selected_fields_settings_and_membership_are_not_reduced_to_display_names() {
    let fixture = fixture();
    let case = row(&fixture, "mac-existing");
    let discovery = discover(case).unwrap();
    let mut changed = case["current"].clone();
    changed["candidates"][0]["properties"]["propertyItems"][0]["pluginMetadata"]["opaque"][0] =
        json!("replaced");
    assert!(readings(&changed, |fresh| discovery.compare(fresh)).is_err());
    let mut changed = case["current"].clone();
    changed["candidates"][0]["settings"]["inputSettings"]["newPluginField"] = json!(true);
    assert!(readings(&changed, |fresh| discovery.compare(fresh)).is_err());
    let mut duplicate = case["current"].clone();
    duplicate["membership"]["sceneItems"]
        .as_array_mut()
        .unwrap()
        .push(json!({"sceneItemId":8,"sourceName":"Capture","sourceUuid":"target-uuid"}));
    let mut duplicate_case = case.clone();
    duplicate_case["current"] = duplicate.clone();
    let discovery = discover(&duplicate_case).unwrap();
    duplicate["membership"]["sceneItems"]
        .as_array_mut()
        .unwrap()
        .reverse();
    assert!(readings(&duplicate, |fresh| discovery.compare(fresh)).is_ok());
    duplicate["membership"]["sceneItems"][0]["sourceUuid"] = json!("replacement");
    assert!(readings(&duplicate, |fresh| discovery.compare(fresh)).is_err());
}
#[test]
fn stronger_fit_description_refuses_same_name_uuid_or_scene_replacement() {
    let fixture = fixture();
    let case = row(&fixture, "mac-existing");
    let discovery = discover(case).unwrap();
    let target = discovery.existing().unwrap();
    let mut members = case["current"]["membership"].clone();
    members["sceneItems"][0]["sourceUuid"] = json!("replacement");
    assert!(
        discovery
            .fit_intent(
                target,
                &case["current"]["scenes"],
                &members,
                &json!({"baseWidth":1920,"baseHeight":1080}),
                &json!({"sceneItemId":7})
            )
            .is_err()
    );
    let mut scene = case["current"]["scenes"].clone();
    scene["scenes"][0]["sceneUuid"] = json!("replacement");
    assert!(
        discovery
            .fit_intent(
                target,
                &scene,
                &case["current"]["membership"],
                &json!({"baseWidth":1920,"baseHeight":1080}),
                &json!({"sceneItemId":7})
            )
            .is_err()
    );
    for video in [
        json!({"baseWidth":0,"baseHeight":1080}),
        json!({"baseWidth":1920}),
        json!({"baseWidth":-1,"baseHeight":1080}),
    ] {
        assert!(Canvas::parse(&video).is_err());
    }
}
#[test]
fn source_safe_x11_inventory_is_distinct_from_selected_window_identity() {
    let fixture = fixture();
    let case = row(&fixture, "x11-zero-inventory-application");
    let discovery = discover(case).unwrap();
    assert_eq!(discovery.kind(), Kind::XComposite);
    assert_eq!(
        discovery.selected().unwrap()["itemValue"],
        "42\r\nOwned Window\r\nEditor"
    );
    let mut configured = case.clone();
    configured["current"]["candidates"][0]["settings"]["inputSettings"]["capture_window"] =
        json!("");
    assert!(discover(&configured).is_err());
    let mut selected = case.clone();
    selected["args"]["application"] = json!("Owned");
    selected["current"]["candidates"][0]["properties"]["propertyItems"][0]["itemValue"] =
        json!("0");
    assert!(discover(&selected).is_err());
}
#[test]
fn native_evidence_bounds_refuse_before_retaining_large_graphs_and_are_labeled() {
    let oversized =
        json!({"sceneName":"Demo","inputName":"Capture","window":"x".repeat(capture::MAX_BYTES)});
    assert_eq!(
        Request::parse(&oversized).unwrap_err().kind(),
        FailureKind::NativeLimit
    );
    let fixture = fixture();
    let mut too_many = row(&fixture, "mac-plan-unknown-record").clone();
    too_many["current"]["candidates"][0]["properties"]["propertyItems"] = Value::Array(vec![
            json!({"itemName":"Owned","itemValue":22});
            capture::MAX_RECORDS
        ]);
    assert_eq!(
        discover(&too_many).unwrap_err().kind(),
        FailureKind::NativeLimit
    );
    let mut deep = Value::Null;
    for _ in 0..=capture::MAX_DEPTH {
        deep = json!({"child":deep});
    }
    let mut input = json!({"sceneName":"Demo","inputName":"Capture","window":"Owned"});
    input["opaque"] = deep;
    assert_eq!(
        Request::parse(&input).unwrap_err().kind(),
        FailureKind::NativeLimit
    );
    let fields = (0..capture::MAX_NODES)
        .map(|n| (n.to_string(), Value::Null))
        .collect::<serde_json::Map<_, _>>();
    input["opaque"] = Value::Object(fields);
    assert_eq!(
        Request::parse(&input).unwrap_err().kind(),
        FailureKind::NativeLimit
    );
}

#[test]
fn escaped_ambiguous_candidate_failure_obeys_encoded_report_cap() {
    let fixture = fixture();
    let mut case = row(&fixture, "mac-plan-unknown-record").clone();
    let controls = "\u{0001}".repeat(100_000);
    case["current"]["candidates"][0]["properties"]["propertyItems"] = json!([
        {"itemName":format!("Owned {controls} A"),"itemValue":22},
        {"itemName":format!("Owned {controls} B"),"itemValue":23},
    ]);
    assert!(serde_json::to_vec(&case["current"]).unwrap().len() < capture::MAX_BYTES);
    let failure = discover(&case).unwrap_err();
    let result = failure.result();
    let encoded = serde_json::to_vec(&result).unwrap().len();
    assert!(
        encoded <= capture::MAX_REPORT_BYTES,
        "ambiguous diagnostic encoded {encoded} bytes"
    );
    assert_eq!(failure.kind(), FailureKind::NativeLimit);
    assert!(result["isError"].as_bool().unwrap());
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("exceeds native limits")
    );
}
#[test]
fn escaped_direct_kind_failure_obeys_encoded_report_cap() {
    let fixture = fixture();
    let mut case = row(&fixture, "mac-existing").clone();
    let name = format!("Capture {}", "\u{0001}".repeat(200_000));
    case["args"]["inputName"] = json!(name);
    case["current"]["inputs"]["inputs"][0]["inputName"] = json!(name);
    case["current"]["inputs"]["inputs"][0]["inputKind"] = json!("browser_source");
    assert!(serde_json::to_vec(&case["args"]).unwrap().len() < capture::MAX_BYTES);
    assert!(serde_json::to_vec(&case["current"]).unwrap().len() < capture::MAX_BYTES);
    let failure = discover(&case).unwrap_err();
    let result = failure.result();
    let encoded = serde_json::to_vec(&result).unwrap().len();
    assert!(
        encoded <= capture::MAX_REPORT_BYTES,
        "direct diagnostic encoded {encoded} bytes"
    );
    assert_eq!(failure.kind(), FailureKind::NativeLimit);
    assert!(result["isError"].as_bool().unwrap());
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("exceeds native limits")
    );
}
