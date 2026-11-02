// SPDX-License-Identifier: GPL-2.0-only
//! Closed public Stop live-loan exclusions never remove durable uncertainty.
use obs_mcp_native::{
    operation_store::{
        GenericKind, GenericPhase, GenericReceipt, OperationKind, OperationReceipt, OperationStore,
        OperationUpdate, Phase, PrepareDetails,
    },
    receipt_broker::{ReceiptBroker, ScopeKind},
    session::SafetyStopRequest,
};
use serde_json::json;
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
                "public-stop-broker-{}-{}",
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

fn safety(broker: &ReceiptBroker) -> OperationStore {
    broker
        .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
        .unwrap()
}
fn intent(store: &mut OperationStore, n: usize, kind: GenericKind) -> GenericReceipt {
    let prepared = store.prepare_generic(&id(n), kind, 1, NEW).unwrap();
    store
        .update_generic(&prepared, GenericPhase::Dispatching, NEW)
        .unwrap()
}
#[test]
fn only_closed_safety_scope_can_exclude_a_live_ordinary_dispatching_loan() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut ordinary = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let other = intent(&mut ordinary, 1, GenericKind::InputMute);
    let normal_fence = ordinary.dispatch_fence().unwrap();
    assert!(normal_fence.permits(&id(1)).unwrap());
    let mut stop = safety(&broker);
    let stopped = intent(&mut stop, 2, GenericKind::StreamStop);
    let fence = stop.dispatch_fence().unwrap();
    assert!(fence.permits(&id(2)).unwrap());
    assert!(!normal_fence.permits(&id(1)).unwrap());
    assert!(
        broker
            .scope(ScopeKind::Generic(GenericKind::SourceFilterEnabled))
            .is_err()
    );
    assert!(broker.admission().unwrap().blocking);
    let accepted = stop
        .update_generic(&stopped, GenericPhase::Accepted, NEW)
        .unwrap();
    assert_eq!(accepted.to_value()["postconditionObserved"], false);
    assert!(fence.permits(&id(2)).is_err());
    assert!(normal_fence.permits(&id(1)).unwrap());
    ordinary
        .update_generic(&other, GenericPhase::Accepted, NEW)
        .unwrap();
    assert!(!broker.admission().unwrap().blocking);
}
#[test]
fn v1_accepted_exclusion_ends_exactly_when_its_live_loan_is_dropped() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut take = broker.scope(ScopeKind::Take).unwrap();
    let accepted = legacy(&mut take, 1, Phase::Accepted);
    let mut stop = safety(&broker);
    let pending = intent(&mut stop, 2, GenericKind::StreamStop);
    let fence = stop.dispatch_fence().unwrap();
    assert!(fence.permits(&id(2)).unwrap());
    assert_eq!(
        broker.read_view().read(&id(1)).unwrap().phase(),
        Phase::Accepted
    );
    drop(take);
    assert!(!fence.permits(&id(2)).unwrap());
    stop.update_generic(&pending, GenericPhase::NotDispatched, NEW)
        .unwrap();
    drop(stop);
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::VirtualCam))
            .is_err()
    );
    assert_eq!(broker.read_view().read(&id(1)).unwrap(), accepted);
    drop(fence);
    drop(broker);
    assert!(
        scratch
            .broker()
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err()
    );
}
#[test]
fn even_a_still_live_unknown_loan_blocks_an_admitted_safety_dispatch() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut ordinary = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let pending = intent(&mut ordinary, 1, GenericKind::InputMute);
    let mut stop = safety(&broker);
    let own = intent(&mut stop, 2, GenericKind::StreamStop);
    let fence = stop.dispatch_fence().unwrap();
    assert!(fence.permits(&id(2)).unwrap());
    ordinary
        .update_generic(&pending, GenericPhase::Unknown, NEW)
        .unwrap();
    assert!(!fence.permits(&id(2)).unwrap());
    stop.update_generic(&own, GenericPhase::NotDispatched, NEW)
        .unwrap();
    drop(stop);
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::ReplayBuffer))
            .is_err()
    );
    assert_eq!(
        broker.read_view().read_any(&id(1)).unwrap()["phase"],
        "unknown"
    );
}
#[test]
fn missing_live_receipt_cannot_become_an_exclusion_proof() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut ordinary = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let _pending = intent(&mut ordinary, 1, GenericKind::InputMute);
    fs::remove_file(scratch.receipt(&id(1))).unwrap();
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err(),
        "the retained loan cannot outlive its required durable intent"
    );
    drop(ordinary);
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err(),
        "dropping the loan must not erase detected durable-proof loss"
    );
    assert!(
        broker.into_store().is_err(),
        "reclaim cannot erase sticky missing-intent uncertainty"
    );
}
#[test]
fn own_missing_intent_and_dropped_scope_revoke_retained_safety_fences() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut stop = safety(&broker);
    let _own = intent(&mut stop, 1, GenericKind::StreamStop);
    let fence = stop.dispatch_fence().unwrap();
    assert!(fence.permits(&id(1)).unwrap());
    assert!(fence.permits(&id(99)).is_err());
    fs::remove_file(scratch.receipt(&id(1))).unwrap();
    assert!(fence.permits(&id(1)).is_err());
    drop(stop);
    assert!(fence.permits(&id(1)).is_err());
}
#[test]
fn safety_scope_is_closed_single_owner_and_cannot_publish_other_kinds_or_scopes() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut ordinary = broker
        .scope(ScopeKind::Generic(GenericKind::StreamStop))
        .unwrap();
    let prepared = ordinary
        .prepare_generic(&id(1), GenericKind::StreamStop, 1, NEW)
        .unwrap();
    let original = fs::read(scratch.receipt(&id(1))).unwrap();
    let mut stop = safety(&broker);
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::VirtualCam))
            .is_err()
    );
    assert!(
        stop.update_generic(&prepared, GenericPhase::Dispatching, NEW)
            .is_err()
    );
    assert_eq!(fs::read(scratch.receipt(&id(1))).unwrap(), original);
    assert!(
        stop.prepare_generic(&id(2), GenericKind::VirtualCamStop, 1, NEW)
            .is_err()
    );
    assert!(
        stop.prepare_generic(&id(3), GenericKind::InputMute, 1, NEW)
            .is_err()
    );
    assert!(!scratch.receipt(&id(2)).exists());
    assert!(!scratch.receipt(&id(3)).exists());
}
#[test]
fn corrupt_or_replaced_namespace_and_publication_poison_never_get_live_exclusions() {
    let scratch = Scratch::new();
    let broker = scratch.broker();
    let mut ordinary = broker
        .scope(ScopeKind::Generic(GenericKind::InputMute))
        .unwrap();
    let pending = intent(&mut ordinary, 1, GenericKind::InputMute);
    let directory = scratch.root().join("operation-receipts");
    let saved = scratch.root().join("saved-receipts");
    fs::rename(&directory, &saved).unwrap();
    fs::create_dir(&directory).unwrap();
    let human = directory.join("human.txt");
    fs::write(&human, "preserve").unwrap();
    assert!(
        ordinary
            .update_generic(&pending, GenericPhase::Accepted, NEW)
            .is_err()
    );
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err()
    );
    assert_eq!(fs::read(&human).unwrap(), b"preserve");
    fs::remove_file(human).unwrap();
    fs::remove_dir(&directory).unwrap();
    fs::rename(saved, &directory).unwrap();
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err(),
        "restoring paths cannot clear sticky publication uncertainty"
    );
}
#[test]
fn full_inventory_unknown_and_orphan_accepted_remain_blocking_past_display_size() {
    for phase in [Phase::Unknown, Phase::Accepted] {
        let scratch = Scratch::new();
        let mut seed = scratch.store();
        for n in 1..=65 {
            generic(&mut seed, n, GenericPhase::Accepted);
        }
        let _legacy = legacy(&mut seed, 999, phase);
        drop(seed);
        let broker = scratch.broker();
        assert_eq!(broker.read_view().summary()["total"], 66);
        assert_eq!(
            broker.read_view().summary()["operations"]
                .as_array()
                .unwrap()
                .len(),
            64
        );
        assert!(
            broker
                .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
                .is_err()
        );
    }
}
#[test]
fn a_public_stop_cannot_consume_the_continuous_take_stop_receipt_reservation() {
    let scratch = Scratch::new();
    let mut seed = scratch.store();
    let template = generic(&mut seed, 10_000, GenericPhase::Accepted).to_value();
    drop(seed);
    for n in 1..1022 {
        let mut value = template.clone();
        value["operationId"] = json!(id(n));
        fs::write(scratch.receipt(&id(n)), serde_json::to_vec(&value).unwrap()).unwrap();
    }
    let broker = scratch.broker();
    let mut take = broker.scope(ScopeKind::Take).unwrap();
    let started = legacy(&mut take, 50_000, Phase::Accepted);
    let mut update = OperationUpdate::new(Phase::Observed);
    update.postcondition_observed = Some(true);
    take.update(&started, update, NEW).unwrap();
    let mut stop = safety(&broker);
    assert!(
        stop.prepare_generic(&id(60_000), GenericKind::StreamStop, 1, NEW)
            .is_err()
    );
    assert!(!scratch.receipt(&id(60_000)).exists());
    assert!(
        broker.admission().is_ok(),
        "ordinary receipt capacity refusal is not proof-loss poison"
    );
    let stopped = take
        .prepare(
            &id(50_001),
            OperationKind::RecordStop,
            1,
            NEW,
            PrepareDetails::default(),
        )
        .unwrap();
    assert_eq!(stopped.phase(), Phase::Prepared);
    assert_eq!(broker.read_view().summary()["total"], 1024);
}

#[test]
fn unvalidated_full_inventory_limit_poison_survives_in_process_reclaim() {
    let scratch = Scratch::new();
    let mut seed = scratch.store();
    let template = generic(&mut seed, 10_000, GenericPhase::Accepted).to_value();
    drop(seed);
    for n in 1..1025 {
        let mut value = template.clone();
        value["operationId"] = json!(id(n));
        fs::write(scratch.receipt(&id(n)), serde_json::to_vec(&value).unwrap()).unwrap();
    }
    let broker = scratch.broker();
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err()
    );
    fs::remove_file(scratch.receipt(&id(1))).unwrap();
    assert!(
        broker
            .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
            .is_err(),
        "repairing bounded inventory does not clear detected in-process uncertainty"
    );
    assert!(broker.into_store().is_err());
}
