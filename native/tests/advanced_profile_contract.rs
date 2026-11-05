// SPDX-License-Identifier: MIT
#[path = "support/output_start_three.rs"]
mod support;
use obs_mcp_native::{
    advanced_commands::Request,
    generic_requests::Call,
    native_profile::NativeProfile,
    operation_store::{GenericKind, GenericPhase, OperationStore},
};
use serde_json::json;
use support::{Dir, id};
const UTC: &str = "2026-10-31T12:00:00.000Z";
const ENDPOINT: &str = "ws://advanced-profile-fixture:4455";
const COMMANDS: [(&str, GenericKind); 5] = [
    ("obs-start-record", GenericKind::ManualRecordStart),
    ("obs-toggle-record", GenericKind::ManualRecordToggle),
    ("obs-start-output", GenericKind::NamedOutputStart),
    ("obs-stop-output", GenericKind::NamedOutputStop),
    ("obs-toggle-output", GenericKind::NamedOutputToggle),
];
fn dir() -> Dir {
    std::fs::create_dir_all(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target"))
        .unwrap();
    Dir::new()
}

#[test]
fn immutable_profiles_and_closed_commands_do_not_create_raw_or_guarded_authority() {
    assert_eq!(NativeProfile::parse(None), Ok(NativeProfile::Production));
    assert_eq!(
        NativeProfile::parse(Some(" advanced ")),
        Ok(NativeProfile::Advanced)
    );
    assert!(NativeProfile::parse(Some("all")).is_err());
    for (name, kind) in COMMANDS {
        assert!(!NativeProfile::Production.allows(name));
        assert!(NativeProfile::Advanced.allows(name));
        let call=Call::parse(name,&json!({"outputName":"private alias","requestType":"StopRecord","requestData":{"foo":1}})).unwrap();
        assert_eq!(call.kind(), kind);
        assert!(kind.advanced_only());
        assert!(kind.incompatible_with_guarded_reentry());
        assert!(!format!("{call:?}").contains("private alias"));
        assert_eq!(
            call.arguments(),
            if name.ends_with("output") {
                json!({"outputName":"private alias"})
            } else {
                json!({})
            }
        );
    }
    for name in [
        "obs-take-start",
        "obs-take-stop",
        "obs-take-mark",
        "obs-take-status",
        "obs-record-clip",
        "obs-contact-sheet",
        "obs-trim-take",
        "obs-save-replay-verified",
        "obs-stage-scene",
        "obs-take-scene",
        "obs-prepare-demo",
        "obs-recover-demo",
        "obs-apply-scene",
        "obs-capture-window",
        "obs-snapshot",
        "obs-restore",
    ] {
        assert!(!NativeProfile::Advanced.allows(name), "{name}");
    }
    assert!(NativeProfile::Advanced.allows("obs-stop-record"));
    for name in ["obs-start-output", "obs-stop-output", "obs-toggle-output"] {
        assert!(Request::parse(name, &json!({"outputName":""})).is_some());
        for args in [
            json!({}),
            json!({"outputName":1}),
            json!({"outputName":null}),
        ] {
            assert!(Request::parse(name, &args).is_none());
        }
    }
    assert!(Request::parse("obs-call-request", &json!({"requestType":"StartRecord"})).is_none());
}

#[test]
fn every_advanced_phase_preserves_endpoint_barrier_except_exact_not_dispatched() {
    for (_, kind) in COMMANDS.into_iter().chain([
        ("opaque", GenericKind::OpaqueRequest),
        ("batch", GenericKind::OpaqueBatch),
        ("vendor", GenericKind::VendorRequest),
        ("hotkey-name", GenericKind::HotkeyName),
        ("hotkey-sequence", GenericKind::HotkeySequence),
        ("plugin-button", GenericKind::InputPropertiesButton),
        ("stream", GenericKind::StreamStart),
        ("stream-stop", GenericKind::StreamStop),
        ("stream-toggle", GenericKind::StreamToggle),
    ]) {
        for phase in [
            GenericPhase::Prepared,
            GenericPhase::Dispatching,
            GenericPhase::Accepted,
            GenericPhase::Rejected,
            GenericPhase::Unknown,
            GenericPhase::NotDispatched,
        ] {
            let dir = dir();
            let mut store = OperationStore::new(&dir.0, ENDPOINT).unwrap();
            let prepared = store.prepare_generic(&id(1), kind, 1, UTC).unwrap();
            let final_receipt = if phase == GenericPhase::Prepared {
                prepared
            } else {
                let pending = store
                    .update_generic(&prepared, GenericPhase::Dispatching, UTC)
                    .unwrap();
                if phase == GenericPhase::Dispatching {
                    pending
                } else {
                    store.update_generic(&pending, phase, UTC).unwrap()
                }
            };
            let allows = phase == GenericPhase::NotDispatched;
            assert_eq!(
                store.read_view().guarded_reentry_allowed(),
                Ok(allows),
                "{kind:?}/{phase:?}"
            );
            assert_eq!(
                store.read_view().production_connection_allowed(),
                Ok(allows || !kind.advanced_only()),
                "{kind:?}/{phase:?}"
            );
            // Accepted compatibility history is not transport uncertainty.
            if phase == GenericPhase::Accepted {
                assert!(!store.read_view().admission().unwrap().blocking);
                assert_eq!(final_receipt.to_value()["requestAccepted"], true);
            }
            drop(store);
            let fresh = OperationStore::new(&dir.0, ENDPOINT).unwrap();
            assert_eq!(fresh.read_view().guarded_reentry_allowed(), Ok(allows));
            assert_eq!(
                fresh.read_view().production_connection_allowed(),
                Ok(allows || !kind.advanced_only())
            );
            let other = OperationStore::new(&dir.0, "ws://other-fixture:4455").unwrap();
            assert!(other.read_view().guarded_reentry_allowed().unwrap());
        }
    }
}

#[test]
fn payloads_are_absent_and_corrupt_barrier_evidence_never_becomes_compatible() {
    let dir = dir();
    let mut store = OperationStore::new(&dir.0, ENDPOINT).unwrap();
    let receipt = store
        .prepare_generic(&id(1), GenericKind::NamedOutputStart, 1, UTC)
        .unwrap();
    let body = receipt.to_value();
    assert_eq!(body["requestType"], "StartOutput");
    assert!(body.get("outputName").is_none());
    assert!(body.get("requestData").is_none());
    std::fs::write(dir.path(1), b"{broken").unwrap();
    assert!(store.read_view().guarded_reentry_allowed().is_err());
    assert!(store.read_view().production_connection_allowed().is_err());
}
