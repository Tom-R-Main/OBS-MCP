// SPDX-License-Identifier: MIT
use obs_mcp_native::{
    operation_store::{
        CaptureCounts, CaptureInitial, CapturePhase, CapturePlan, CaptureReceipt, CaptureStep,
        CaptureUpdate, OperationStore,
    },
    receipt_broker::{ReceiptBroker, ScopeKind},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
const ID: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const UTC: &str = "2026-11-01T00:00:00.000Z";
const ENDPOINT: &str = "ws://127.0.0.1:4455";
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "capture-receipt-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn plan() -> CapturePlan {
    CapturePlan {
        initial: CaptureInitial::Retarget,
        audio: true,
        fit: true,
    }
}
fn start(dir: &Dir) -> (ReceiptBroker, OperationStore, CaptureReceipt) {
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, ENDPOINT).unwrap()).unwrap();
    let mut scope = broker.scope(ScopeKind::CaptureWindow).unwrap();
    let receipt = scope.prepare_capture(ID, 7, plan(), UTC).unwrap();
    (broker, scope, receipt)
}
#[test]
fn exact_stages_ack_and_halt_tail_are_validated_and_never_render_proof() {
    let dir = Dir::new();
    let (broker, mut scope, mut r) = start(&dir);
    assert!(broker.admission().unwrap().blocking);
    assert!(
        scope
            .update_capture(&r, CaptureUpdate::Dispatch(CaptureStep::Fit), UTC)
            .is_err()
    );
    for (step, count) in [
        (CaptureStep::Initial, 1),
        (CaptureStep::Audio, 2),
        (CaptureStep::Fit, 1),
    ] {
        r = scope
            .update_capture(&r, CaptureUpdate::Dispatch(step), UTC)
            .unwrap();
        assert!(scope.dispatch_fence().unwrap().permits(ID).unwrap());
        r = scope
            .update_capture(
                &r,
                CaptureUpdate::Acknowledged(CaptureCounts {
                    successful: count,
                    failed: 0,
                    skipped: 0,
                }),
                UTC,
            )
            .unwrap();
    }
    r = scope.update_capture(&r, CaptureUpdate::Close, UTC).unwrap();
    assert_eq!(r.phase(), CapturePhase::Closed);
    assert!(!broker.admission().unwrap().blocking);
    assert_eq!(r.to_value()["postconditionObserved"], false);
    assert_eq!(r.to_value()["renderedTarget"], "unverified");
    assert_eq!(CaptureReceipt::from_value(r.to_value()).unwrap(), r);
}
#[test]
fn unknown_and_orphaned_continuation_block_restart_and_other_endpoint_does_not() {
    for unknown in [false, true] {
        let dir = Dir::new();
        let (broker, mut scope, r) = start(&dir);
        let r = scope
            .update_capture(&r, CaptureUpdate::Dispatch(CaptureStep::Initial), UTC)
            .unwrap();
        let _ = scope
            .update_capture(
                &r,
                if unknown {
                    CaptureUpdate::Unknown
                } else {
                    CaptureUpdate::Acknowledged(CaptureCounts {
                        successful: 1,
                        failed: 0,
                        skipped: 0,
                    })
                },
                UTC,
            )
            .unwrap();
        drop(scope);
        drop(broker);
        let reopened = OperationStore::new(&dir.0, ENDPOINT).unwrap();
        assert!(reopened.read_view().admission().unwrap().blocking);
        drop(reopened);
        let foreign = OperationStore::new(&dir.0, "ws://127.0.0.1:4456").unwrap();
        assert!(!foreign.read_view().admission().unwrap().blocking);
    }
}
#[test]
fn failed_audio_preserves_prior_ack_and_exact_skipped_tail_with_no_continuation() {
    let dir = Dir::new();
    let (broker, mut scope, mut r) = start(&dir);
    for (step, c) in [
        (
            CaptureStep::Initial,
            CaptureCounts {
                successful: 1,
                failed: 0,
                skipped: 0,
            },
        ),
        (
            CaptureStep::Audio,
            CaptureCounts {
                successful: 0,
                failed: 1,
                skipped: 1,
            },
        ),
    ] {
        r = scope
            .update_capture(&r, CaptureUpdate::Dispatch(step), UTC)
            .unwrap();
        r = scope
            .update_capture(&r, CaptureUpdate::Acknowledged(c), UTC)
            .unwrap();
    }
    assert_eq!(r.phase(), CapturePhase::Aborted);
    assert_eq!(r.to_value()["steps"][0]["counts"]["successful"], 1);
    assert_eq!(r.to_value()["steps"][2]["phase"], "not-dispatched");
    assert!(!broker.admission().unwrap().blocking);
    assert!(
        scope
            .update_capture(&r, CaptureUpdate::Dispatch(CaptureStep::Fit), UTC)
            .is_err()
    );
}
#[test]
fn missing_null_facts_unknown_fields_wrong_counts_and_private_payload_cannot_be_admitted() {
    let dir = Dir::new();
    let (_, _, r) = start(&dir);
    let original = r.to_value();
    let mut invalid = vec![];
    for path in ["activeStep", "postconditionObserved", "renderedTarget"] {
        let mut v = original.clone();
        v.as_object_mut().unwrap().remove(path);
        invalid.push(v);
    }
    for field in ["requestDispatched", "replyAcknowledged", "counts"] {
        let mut v = original.clone();
        v["steps"][0].as_object_mut().unwrap().remove(field);
        invalid.push(v);
    }
    let mut private = original.clone();
    private["inputSettings"] = json!({"RestoreToken":"do-not-store"});
    invalid.push(private);
    let mut wrong = original.clone();
    wrong["steps"][1]["requestCount"] = json!(3);
    invalid.push(wrong);
    let mut fake = original.clone();
    fake["postconditionObserved"] = json!(true);
    invalid.push(fake);
    for value in invalid {
        assert!(CaptureReceipt::from_value(value).is_err());
    }
    assert!(CaptureReceipt::from_value(original).is_ok());
}
#[test]
fn foreign_scope_cannot_prepare_capture_or_alter_original_and_stale_receipt_cannot_overwrite() {
    let dir = Dir::new();
    let broker = ReceiptBroker::new(OperationStore::new(&dir.0, ENDPOINT).unwrap()).unwrap();
    let mut generic = broker.scope(ScopeKind::ApplyScene).unwrap();
    assert!(generic.prepare_capture(ID, 1, plan(), UTC).is_err());
    drop(generic);
    let mut capture = broker.scope(ScopeKind::CaptureWindow).unwrap();
    let old = capture.prepare_capture(ID, 1, plan(), UTC).unwrap();
    let live = capture
        .update_capture(&old, CaptureUpdate::Dispatch(CaptureStep::Initial), UTC)
        .unwrap();
    assert!(
        capture
            .update_capture(&old, CaptureUpdate::Abort, UTC)
            .is_err()
    );
    let next = capture
        .update_capture(&live, CaptureUpdate::NotDispatched, UTC)
        .unwrap();
    assert_eq!(next.phase(), CapturePhase::Aborted);
    assert_eq!(next.to_value()["steps"][0]["requestDispatched"], false);
    assert_eq!(
        next.to_value()["steps"][0]["replyAcknowledged"],
        Value::Null
    );
}
