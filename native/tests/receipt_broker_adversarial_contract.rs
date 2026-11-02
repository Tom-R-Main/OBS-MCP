// SPDX-License-Identifier: GPL-2.0-only
//! Independent consumer tests of receipt custody. These do not dispatch OBS RPCs
//! or prove that a receipt grants authority to replay a setter or stop recording.
use obs_mcp_native::{
    operation_store::{
        GenericKind, GenericPhase, GenericReceipt, OperationKind, OperationReceipt, OperationStore,
        OperationUpdate, Phase, PrepareDetails,
    },
    receipt_broker::{ReceiptBroker, ScopeKind},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const ENDPOINT: &str = "ws://127.0.0.1:4455";
const OLD: &str = "2026-10-31T12:00:00.000Z";
const NEW: &str = "2026-11-01T12:00:00.000Z";

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "receipt-broker-adversarial-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(path)
    }
    fn root(&self) -> PathBuf {
        self.0.join("state")
    }
    fn receipt(&self, id: &str) -> PathBuf {
        self.root()
            .join("operation-receipts")
            .join(format!("{id}.json"))
    }
    fn store(&self) -> OperationStore {
        OperationStore::new(self.root(), ENDPOINT).unwrap()
    }
    fn broker(&self) -> ReceiptBroker {
        ReceiptBroker::new(self.store()).unwrap()
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn id(n: usize) -> String {
    format!("11111111-1111-4111-8111-{n:012x}")
}

fn legacy(store: &mut OperationStore, n: usize, phase: Phase) -> OperationReceipt {
    let prepared = store
        .prepare(
            &id(n),
            OperationKind::RecordStart,
            1,
            OLD,
            PrepareDetails::default(),
        )
        .unwrap();
    let mut intent = OperationUpdate::new(Phase::Dispatching);
    intent.request_dispatched = Some(None);
    let dispatching = store.update(&prepared, intent, OLD).unwrap();
    let mut result = OperationUpdate::new(phase);
    result.request_dispatched = Some(Some(true));
    result.request_accepted = Some(if phase == Phase::Accepted {
        Some(true)
    } else {
        None
    });
    store.update(&dispatching, result, OLD).unwrap()
}
fn generic(store: &mut OperationStore, n: usize, terminal: GenericPhase) -> GenericReceipt {
    let prepared = store
        .prepare_generic(&id(n), GenericKind::InputMute, 1, NEW)
        .unwrap();
    let dispatching = store
        .update_generic(&prepared, GenericPhase::Dispatching, NEW)
        .unwrap();
    store.update_generic(&dispatching, terminal, NEW).unwrap()
}
fn sixty_five_accepted(broker: &ReceiptBroker) {
    let mut port = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    for n in 1..=65 {
        generic(&mut port, n, GenericPhase::Accepted);
    }
}
fn assert_no_observation_or_replay(receipt: &Value) {
    assert_ne!(receipt["phase"], "observed");
    assert_eq!(
        receipt.get("postconditionObserved"),
        Some(&Value::Bool(false))
    );
    assert_eq!(receipt["renderedComposition"], "unverified");
    assert_ne!(receipt.get("status"), Some(&json!("observed")));
    for field in [
        "replayAuthorized",
        "retryAuthorized",
        "mayReplay",
        "mayRetry",
    ] {
        assert_ne!(receipt.get(field), Some(&Value::Bool(true)));
    }
}

fn historical_generic_files(scratch: &Scratch, count: usize, endpoint: &str) {
    assert!(count >= 1);
    let mut seed = OperationStore::new(scratch.root(), endpoint).unwrap();
    let template = generic(&mut seed, 10_000, GenericPhase::Accepted).to_value();
    drop(seed);
    for number in 1..count {
        let mut value = template.clone();
        value["operationId"] = json!(id(number));
        // Historical immutable fixtures, rooted in an actual persisted API
        // receipt; the consumer must independently validate all of these files.
        fs::write(
            scratch.receipt(&id(number)),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
    }
}

#[test]
fn take_admission_requires_both_start_and_future_stop_receipt_capacity() {
    for endpoint in [ENDPOINT, "ws://127.0.0.1:4456"] {
        let scratch = Scratch::new();
        historical_generic_files(&scratch, 1_023, endpoint);
        let broker = scratch.broker();
        assert!(
            broker.admission().is_ok(),
            "validated bounded history is readable"
        );
        assert!(
            broker.scope(ScopeKind::Take).is_err(),
            "one free physical slot cannot fund Start and its future Stop, including foreign-endpoint history"
        );
        assert_eq!(
            fs::read_dir(scratch.root().join("operation-receipts"))
                .unwrap()
                .count(),
            1_023
        );
    }
}

#[test]
fn admitted_generic_scope_cannot_steal_the_continuous_take_stop_receipt_slot() {
    let scratch = Scratch::new();
    historical_generic_files(&scratch, 1_022, ENDPOINT);
    let broker = scratch.broker();
    let mut mute = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let mut take = broker.scope(ScopeKind::Take).unwrap();
    let start = legacy(&mut take, 50_000, Phase::Accepted);
    let mut observation = OperationUpdate::new(Phase::Observed);
    observation.postcondition_observed = Some(true);
    take.update(&start, observation, NEW).unwrap();
    assert!(!broker.admission().unwrap().blocking);
    assert!(
        mute.prepare_generic(&id(60_000), GenericKind::InputMute, 1, NEW)
            .is_err(),
        "last slot belongs to the already-owned Stop"
    );
    assert!(!scratch.receipt(&id(60_000)).exists());
    let stop = take
        .prepare(
            &id(50_001),
            OperationKind::RecordStop,
            1,
            NEW,
            PrepareDetails::default(),
        )
        .unwrap();
    assert_eq!(stop.phase(), Phase::Prepared);
    assert_eq!(broker.admission().unwrap().total, 1_024);
    assert_eq!(
        fs::read_dir(scratch.root().join("operation-receipts"))
            .unwrap()
            .count(),
        1_024
    );
}

#[test]
fn sixty_five_accepted_generic_receipts_are_terminal_without_becoming_observed() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    sixty_five_accepted(&broker);
    let view = broker.read_view();
    let admission = view.admission().unwrap();
    assert_eq!(admission.total, 65);
    assert!(!admission.blocking);
    assert!(admission.generic_v2_present);
    let summary = view.summary();
    assert_eq!(summary["total"], 65);
    assert_eq!(summary["truncated"], true);
    assert_eq!(summary["operations"].as_array().unwrap().len(), 64);
    for operation in summary["operations"].as_array().unwrap() {
        assert_no_observation_or_replay(operation);
    }
    let bytes = fs::read(scratch.receipt(&id(65))).unwrap();
    let persisted: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(persisted["version"], 2);
    assert_eq!(persisted["phase"], "accepted");
    assert_no_observation_or_replay(&persisted);
    drop(view);
    drop(broker);
    let restarted = scratch.broker();
    let admission = restarted.admission().unwrap();
    assert_eq!(admission.total, 65);
    assert!(!admission.blocking);
}

#[test]
fn full_inventory_keeps_older_unknown_blocking_after_sixty_five_new_acceptances() {
    let scratch = Scratch::new();
    let mut seed = scratch.store();
    for n in 1..=65 {
        generic(&mut seed, n, GenericPhase::Accepted);
    }
    let old = legacy(&mut seed, 999, Phase::Unknown);
    drop(seed);
    let broker = scratch.broker();
    assert!(
        broker
            .scope(ScopeKind::Generic(GenericKind::InputMute))
            .is_err()
    );
    let summary = broker.read_view().summary();
    assert_eq!(summary["operations"].as_array().unwrap().len(), 64);
    assert_eq!(summary["total"], 66);
    assert!(broker.admission().unwrap().blocking);
    assert_eq!(broker.admission().unwrap().total, 66);
    assert_eq!(
        broker.read_view().read(old.operation_id()).unwrap().phase(),
        Phase::Unknown
    );
    drop(broker);
    assert!(scratch.broker().admission().unwrap().blocking);
}

#[test]
fn legacy_accepted_recording_custody_still_blocks_after_sixty_five_generic_acceptances() {
    let scratch = Scratch::new();
    let mut seed = scratch.store();
    for n in 1..=65 {
        generic(&mut seed, n, GenericPhase::Accepted);
    }
    let old = legacy(&mut seed, 999, Phase::Accepted);
    drop(seed);
    let broker = scratch.broker();
    assert!(
        broker
            .scope(ScopeKind::Generic(GenericKind::InputMute))
            .is_err()
    );
    assert_eq!(broker.read_view().summary()["total"], 66);
    assert!(broker.admission().unwrap().blocking);
    let persisted = broker
        .read_view()
        .read(old.operation_id())
        .unwrap()
        .to_value();
    assert_eq!(persisted["version"], 1);
    assert_eq!(persisted["requestAccepted"], true);
    assert_no_observation_or_replay(&persisted);
}

#[test]
fn a_scope_cannot_publish_another_workflows_receipt_or_another_generic_kind() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut take = broker.scope(ScopeKind::Take).unwrap();
    let mut stage = broker.scope(ScopeKind::Stage).unwrap();
    let mut mute = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let mut filter = broker
        .scope(ScopeKind::Generic(GenericKind::SourceFilterEnabled))
        .unwrap();
    let mut other_mute = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let prepared = take
        .prepare(
            &id(1),
            OperationKind::RecordStart,
            1,
            OLD,
            PrepareDetails::default(),
        )
        .unwrap();
    let original = fs::read(scratch.receipt(&id(1))).unwrap();
    assert!(
        stage
            .update(&prepared, OperationUpdate::new(Phase::Dispatching), NEW)
            .is_err()
    );
    assert!(
        mute.update(&prepared, OperationUpdate::new(Phase::Dispatching), NEW)
            .is_err()
    );
    assert_eq!(fs::read(scratch.receipt(&id(1))).unwrap(), original);
    assert!(
        stage
            .prepare(
                &id(2),
                OperationKind::RecordStop,
                1,
                OLD,
                PrepareDetails::default()
            )
            .is_err()
    );
    assert!(!scratch.receipt(&id(2)).exists());
    assert!(
        take.prepare_generic(&id(3), GenericKind::InputMute, 1, OLD)
            .is_err()
    );
    assert!(!scratch.receipt(&id(3)).exists());
    let muted = mute
        .prepare_generic(&id(4), GenericKind::InputMute, 1, OLD)
        .unwrap();
    let original = fs::read(scratch.receipt(&id(4))).unwrap();
    assert!(
        filter
            .update_generic(&muted, GenericPhase::Dispatching, NEW)
            .is_err()
    );
    assert!(
        other_mute
            .update_generic(&muted, GenericPhase::Dispatching, NEW)
            .is_err()
    );
    assert!(
        take.update_generic(&muted, GenericPhase::Dispatching, NEW)
            .is_err()
    );
    assert_eq!(fs::read(scratch.receipt(&id(4))).unwrap(), original);
    assert!(
        mute.prepare_generic(&id(5), GenericKind::SourceFilterEnabled, 1, OLD)
            .is_err()
    );
    assert!(!scratch.receipt(&id(5)).exists());
}

#[test]
fn terminal_generic_acceptance_cannot_be_rewound_to_create_replay_authority() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut mute = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let receipt = generic(&mut mute, 1, GenericPhase::Accepted);
    let original = fs::read(scratch.receipt(&id(1))).unwrap();
    assert!(
        !mute
            .dispatch_fence()
            .unwrap()
            .permits(receipt.operation_id())
            .unwrap(),
        "a completed acceptance cannot be reused as before-dispatch authority"
    );
    for rewind in [
        GenericPhase::Prepared,
        GenericPhase::Dispatching,
        GenericPhase::NotDispatched,
        GenericPhase::Rejected,
        GenericPhase::Unknown,
    ] {
        assert!(mute.update_generic(&receipt, rewind, NEW).is_err());
        assert_eq!(fs::read(scratch.receipt(&id(1))).unwrap(), original);
    }
    assert!(!broker.admission().unwrap().blocking);
}

#[test]
fn unrelated_unknown_does_not_revoke_existing_take_scope_publication_custody() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut take = broker.scope(ScopeKind::Take).unwrap();
    let mut mute = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let stop = take
        .prepare(
            &id(100),
            OperationKind::RecordStop,
            1,
            OLD,
            PrepareDetails::default(),
        )
        .unwrap();
    generic(&mut mute, 1, GenericPhase::Unknown);
    assert!(broker.admission().unwrap().blocking);
    // Service owns whether StopRecord may be sent; this verifies only retained
    // scope custody can publish the already-owned operation's uncertain outcome.
    let mut update = OperationUpdate::new(Phase::Unknown);
    update.request_dispatched = Some(None);
    take.update(&stop, update, NEW).unwrap();
    assert_eq!(
        broker.read_view().read(&id(100)).unwrap().phase(),
        Phase::Unknown
    );
    assert!(broker.admission().unwrap().blocking);
}

#[test]
fn corrupt_inventory_beyond_display_cap_is_never_empty_success_and_does_not_steal_take_scope() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    sixty_five_accepted(&broker);
    let mut take = broker.scope(ScopeKind::Take).unwrap();
    let stop = take
        .prepare(
            &id(100),
            OperationKind::RecordStop,
            1,
            OLD,
            PrepareDetails::default(),
        )
        .unwrap();
    fs::write(scratch.receipt(&id(999)), b"{corrupt").unwrap();
    assert!(broker.admission().is_err());
    assert!(broker.read_view().admission().is_err());
    assert_eq!(broker.read_view().summary()["status"], "unknown");
    let mut result = OperationUpdate::new(Phase::Unknown);
    result.request_dispatched = Some(None);
    take.update(&stop, result, NEW).unwrap();
    assert_eq!(fs::read(scratch.receipt(&id(999))).unwrap(), b"{corrupt");
    assert!(broker.admission().is_err());
}

#[test]
fn replaced_namespace_refuses_new_publication_and_preserves_replacement_files() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut mute = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let prepared = mute
        .prepare_generic(&id(1), GenericKind::InputMute, 1, OLD)
        .unwrap();
    let original = fs::read(scratch.receipt(&id(1))).unwrap();
    fs::rename(scratch.root(), scratch.0.join("retired-state")).unwrap();
    fs::create_dir(scratch.root()).unwrap();
    fs::write(
        scratch.root().join("human-note"),
        b"replacement must survive",
    )
    .unwrap();
    assert!(broker.admission().is_err());
    assert!(
        mute.update_generic(&prepared, GenericPhase::Dispatching, NEW)
            .is_err()
    );
    assert!(broker.admission().is_err());
    assert_eq!(
        fs::read(scratch.root().join("human-note")).unwrap(),
        b"replacement must survive"
    );
    assert!(!scratch.root().join("operation-receipts").exists());
    assert_eq!(
        fs::read(
            scratch
                .0
                .join("retired-state/operation-receipts")
                .join(format!("{}.json", id(1)))
        )
        .unwrap(),
        original
    );
    assert_eq!(broker.read_view().summary()["status"], json!("unknown"));
}

#[test]
fn publication_poison_survives_restored_path_identity_and_cannot_be_erased_by_reclaim() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut mute = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let prepared = mute
        .prepare_generic(&id(1), GenericKind::InputMute, 1, OLD)
        .unwrap();
    fs::rename(scratch.root(), scratch.0.join("original-state")).unwrap();
    fs::create_dir(scratch.root()).unwrap();
    assert!(
        mute.update_generic(&prepared, GenericPhase::Dispatching, NEW)
            .is_err()
    );
    // Restore the original directory inode, preserving the replacement as well.
    // Repairing the path does not prove publication durability or revoke poison.
    fs::rename(scratch.root(), scratch.0.join("replacement-retained")).unwrap();
    fs::rename(scratch.0.join("original-state"), scratch.root()).unwrap();
    drop(mute);
    assert!(broker.admission().is_err());
    assert!(
        broker
            .scope(ScopeKind::Generic(GenericKind::InputMute))
            .is_err()
    );
    match broker.into_store() {
        Ok(_) => panic!("reclaim discarded durable-publication uncertainty"),
        Err(retained) => assert!(retained.admission().is_err()),
    }
    assert!(scratch.0.join("replacement-retained").is_dir());
}

#[test]
fn dropped_scope_revokes_its_cloned_dispatch_fence_without_deleting_receipt() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut mute = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let prepared = mute
        .prepare_generic(&id(1), GenericKind::InputMute, 1, OLD)
        .unwrap();
    let dispatching = mute
        .update_generic(&prepared, GenericPhase::Dispatching, NEW)
        .unwrap();
    let fence = mute.dispatch_fence().unwrap();
    assert!(fence.permits(dispatching.operation_id()).unwrap());
    let bytes = fs::read(scratch.receipt(&id(1))).unwrap();
    drop(mute);
    assert!(fence.permits(dispatching.operation_id()).is_err());
    assert_eq!(fs::read(scratch.receipt(&id(1))).unwrap(), bytes);
    assert!(broker.admission().unwrap().blocking);
}

#[test]
fn previously_admitted_second_scope_cannot_dispatch_after_first_becomes_unknown() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut mute = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let mut filter = broker
        .scope(ScopeKind::Generic(GenericKind::SourceFilterEnabled))
        .unwrap();
    let mut take = broker.scope(ScopeKind::Take).unwrap();
    let first = mute
        .prepare_generic(&id(1), GenericKind::InputMute, 1, OLD)
        .unwrap();
    let second = filter
        .prepare_generic(&id(2), GenericKind::SourceFilterEnabled, 1, OLD)
        .unwrap();
    let first = mute
        .update_generic(&first, GenericPhase::Dispatching, NEW)
        .unwrap();
    mute.update_generic(&first, GenericPhase::Unknown, NEW)
        .unwrap();
    let second = filter
        .update_generic(&second, GenericPhase::Dispatching, NEW)
        .unwrap();
    assert!(
        !filter
            .dispatch_fence()
            .unwrap()
            .permits(second.operation_id())
            .unwrap()
    );
    assert!(
        broker
            .scope(ScopeKind::Generic(GenericKind::InputMute))
            .is_err()
    );
    // Existing Take scope remains a custody owner; actual Stop RPC ownership is
    // separately qualified through the public service integration controls.
    let stop = take
        .prepare(
            &id(3),
            OperationKind::RecordStop,
            1,
            NEW,
            PrepareDetails::default(),
        )
        .unwrap();
    assert_eq!(stop.phase(), Phase::Prepared);
    assert!(broker.admission().unwrap().blocking);
}

#[test]
fn syntactically_valid_old_generic_observed_forgery_beyond_page_fails_closed() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    sixty_five_accepted(&broker);
    let mut corrupt: Value =
        serde_json::from_slice(&fs::read(scratch.receipt(&id(1))).unwrap()).unwrap();
    corrupt["operationId"] = json!(id(999));
    corrupt["createdAt"] = json!(OLD);
    corrupt["phase"] = json!("observed");
    corrupt["postconditionObserved"] = json!(true);
    fs::write(
        scratch.receipt(&id(999)),
        serde_json::to_vec(&corrupt).unwrap(),
    )
    .unwrap();
    assert!(broker.admission().is_err());
    assert!(broker.read_view().try_summary().is_err());
    assert_eq!(broker.read_view().summary()["status"], "unknown");
    assert!(
        broker
            .scope(ScopeKind::Generic(GenericKind::InputMute))
            .is_err()
    );
}

#[test]
fn unrelated_corrupt_history_does_not_prevent_preparing_the_owned_stop_intent() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut take = broker.scope(ScopeKind::Take).unwrap();
    // Establish this namespace through the real writer before the independent
    // history fault. This tests publication custody, not actual OBS ownership.
    take.prepare(
        &id(1),
        OperationKind::RecordStart,
        1,
        OLD,
        PrepareDetails::default(),
    )
    .unwrap();
    fs::write(scratch.receipt(&id(999)), b"{corrupt").unwrap();
    assert!(broker.admission().is_err());
    assert!(
        broker
            .scope(ScopeKind::Generic(GenericKind::InputMute))
            .is_err()
    );
    let stop = take
        .prepare(
            &id(2),
            OperationKind::RecordStop,
            1,
            NEW,
            PrepareDetails::default(),
        )
        .unwrap();
    assert_eq!(stop.phase(), Phase::Prepared);
    let mut dispatching = OperationUpdate::new(Phase::Dispatching);
    dispatching.request_dispatched = Some(None);
    take.update(&stop, dispatching, NEW).unwrap();
    assert_eq!(
        broker.read_view().read(&id(2)).unwrap().phase(),
        Phase::Dispatching
    );
    assert_eq!(fs::read(scratch.receipt(&id(999))).unwrap(), b"{corrupt");
    assert!(broker.admission().is_err());
}
