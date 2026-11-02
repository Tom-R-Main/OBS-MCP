// SPDX-License-Identifier: GPL-2.0-only
//! Opaque callbacks remain closed metadata identities with one charged payload.
use obs_mcp_native::{
    generic_requests::Call,
    js_json::{self, Limits},
};
use serde_json::{Value, json};

#[test]
fn opaque_six_keep_fixed_request_identities_and_redacted_debug() {
    for (name, request, args) in [
        (
            "obs-broadcast-custom-event",
            "BroadcastCustomEvent",
            json!({"eventData":{"private":"PRIVATE_ARGUMENT"}}),
        ),
        (
            "obs-set-transition-settings",
            "SetCurrentSceneTransitionSettings",
            json!({"transitionSettings":{}}),
        ),
        (
            "obs-create-source-filter",
            "CreateSourceFilter",
            json!({"sourceName":"S","filterName":"F","filterKind":"K"}),
        ),
        (
            "obs-set-source-filter-settings",
            "SetSourceFilterSettings",
            json!({"sourceName":"S","filterName":"F","filterSettings":{},"overlay":false}),
        ),
        (
            "obs-press-input-property-button",
            "PressInputPropertiesButton",
            json!({"propertyName":"P"}),
        ),
        (
            "obs-save-source-screenshot",
            "SaveSourceScreenshot",
            json!({"imageFormat":"png","imageFilePath":"PRIVATE_ARGUMENT"}),
        ),
    ] {
        let call = Call::parse(name, &args).expect(name);
        assert_eq!(call.kind().request_type(), request);
        assert_eq!(serde_json::to_value(call.kind()).unwrap(), request);
        assert_eq!(call.name(), name);
        assert!(!call.requires_readback());
        assert!(!format!("{call:?}").contains("PRIVATE_ARGUMENT"));
        assert_eq!(
            call.retained_bytes(),
            js_json::encoded_len(&call.arguments(), Limits::default()).ok()
        );
    }
    for name in [
        "obs-call-request",
        "obs-call-vendor-request",
        "obs-start-record",
        "SaveSourceScreenshot",
    ] {
        assert!(
            Call::parse(
                name,
                &json!({"requestType":"SaveSourceScreenshot","eventData":{}})
            )
            .is_none()
        );
    }
}

#[test]
fn record_boundaries_strip_only_outer_proto_and_keep_unknown_sentinel_objects() {
    let record: Value = serde_json::from_str(r#"{"__proto__":"outer-only","constructor":"keep","prototype":"keep","nested":{"__proto__":"keep","$serde_json::private::RawValue":"123","$serde_json::private::Number":"123"},"values":[null,false,{},[]]}"#).unwrap();
    for (name, field, mut args) in [
        ("obs-broadcast-custom-event", "eventData", json!({})),
        (
            "obs-set-transition-settings",
            "transitionSettings",
            json!({}),
        ),
        (
            "obs-create-source-filter",
            "filterSettings",
            json!({"sourceName":"S","filterName":"F","filterKind":"K"}),
        ),
        (
            "obs-set-source-filter-settings",
            "filterSettings",
            json!({"sourceName":"S","filterName":"F"}),
        ),
    ] {
        args[field] = record.clone();
        args["discarded"] = json!("root unknown");
        let parsed = Call::parse(name, &args).unwrap().into_arguments();
        let mut expected = record.clone();
        expected.as_object_mut().unwrap().remove("__proto__");
        assert_eq!(parsed[field], expected);
        assert!(parsed.get("discarded").is_none());
        for invalid in [
            Value::Null,
            json!([]),
            json!(true),
            json!(42),
            json!("string"),
        ] {
            args[field] = invalid;
            assert!(Call::parse(name, &args).is_none(), "{name}");
        }
    }
}

#[test]
fn optional_fields_and_source_numeric_range_are_preserved_without_invented_defaults() {
    let bare = json!({"sourceName":"S","filterName":"F","filterKind":"K"});
    assert!(
        Call::parse("obs-create-source-filter", &bare)
            .unwrap()
            .arguments()
            .get("filterSettings")
            .is_none()
    );
    let mut with_settings = bare;
    with_settings["filterSettings"] = json!({});
    assert_eq!(
        Call::parse("obs-create-source-filter", &with_settings)
            .unwrap()
            .arguments()["filterSettings"],
        json!({})
    );
    for overlay in [None, Some(false), Some(true)] {
        let mut args = json!({"sourceName":"S","filterName":"F","filterSettings":{}});
        if let Some(overlay) = overlay {
            args["overlay"] = json!(overlay);
        }
        assert_eq!(
            Call::parse("obs-set-source-filter-settings", &args)
                .unwrap()
                .arguments(),
            args
        );
    }
    let args = json!({"imageFormat":"","imageFilePath":"","imageWidth":-1.5,"imageHeight":0,"imageCompressionQuality":1e21});
    assert!(Call::parse("obs-save-source-screenshot", &args).is_some());
    for (name, args) in [
        (
            "obs-press-input-property-button",
            json!({"propertyName":""}),
        ),
        (
            "obs-press-input-property-button",
            json!({"inputName":"","inputUuid":"","propertyName":""}),
        ),
    ] {
        assert_eq!(Call::parse(name, &args).unwrap().arguments(), args);
    }
    assert!(
        Call::parse(
            "obs-set-source-filter-settings",
            &json!({"sourceName":"S","filterName":"F","filterSettings":{},"overlay":null})
        )
        .is_none()
    );
    assert!(
        Call::parse(
            "obs-save-source-screenshot",
            &json!({"imageFormat":"png","imageFilePath":"P","imageWidth":null})
        )
        .is_none()
    );
}

#[test]
fn opaque_payloads_are_charged_after_source_number_projection_and_fail_closed_at_bounds() {
    let args: Value = serde_json::from_str(
        r#"{"eventData":{"n":18446744073709551615,"zero":-0.0,"integer":1.0}}"#,
    )
    .unwrap();
    let call = Call::parse("obs-broadcast-custom-event", &args).unwrap();
    let normalized = call.arguments();
    let encoded = js_json::encode_source_json(&normalized, Limits::default()).unwrap();
    let encoded = String::from_utf8(encoded).unwrap();
    assert!(encoded.contains("18446744073709552000"));
    assert!(encoded.contains("\"zero\":0"));
    assert!(encoded.contains("\"integer\":1"));
    assert_eq!(call.retained_bytes(), Some(encoded.len()));
    let oversized =
        json!({"eventData":{"payload":"x".repeat(Limits::default().max_encoded_bytes)}});
    assert!(Call::parse("obs-broadcast-custom-event", &oversized).is_none());
    let mut nested = json!({});
    for _ in 0..130 {
        nested = json!({"nested":nested});
    }
    assert!(Call::parse("obs-broadcast-custom-event", &json!({"eventData":nested})).is_none());
}
