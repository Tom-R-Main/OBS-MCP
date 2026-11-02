// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::operation_store::{
    OperationKind, OperationStore, OperationUpdate, Phase, PrepareDetails, StoreError,
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
const ENDPOINT: &str = "ws://127.0.0.1:4455";
const ID: &str = "00000000-0000-4000-8000-000000000001";
const NOW: &str = "2026-10-31T12:00:00.000Z";
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let parent = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .canonicalize()
            .unwrap();
        let root = parent.join(format!(
            "operation-view-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
    fn root(&self) -> PathBuf {
        self.0.join("state")
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn a_view_created_before_first_write_observes_live_phases_without_becoming_a_writer() {
    let scratch = Scratch::new();
    let mut writer = OperationStore::new(scratch.root(), ENDPOINT).unwrap();
    let view = writer.read_view();
    let other = view.clone();
    assert_eq!(view.state_root(), writer.state_root());
    assert_eq!(view.endpoint_hash(), writer.endpoint_hash());
    assert_eq!(view.summary()["total"], 0);
    assert!(!scratch.root().exists(), "inspection created the namespace");
    let receipt = writer
        .prepare(
            ID,
            OperationKind::StageScene,
            1,
            NOW,
            PrepareDetails::default(),
        )
        .unwrap();
    assert_eq!(view.read(ID).unwrap().phase(), Phase::Prepared);
    let writer = std::thread::spawn(move || {
        writer
            .update(&receipt, OperationUpdate::new(Phase::Dispatching), NOW)
            .unwrap();
        writer
    })
    .join()
    .unwrap();
    assert_eq!(other.read(ID).unwrap().phase(), Phase::Dispatching);
    assert_eq!(view.summary()["operations"][0]["status"], "unknown");
    drop(writer);
    assert_eq!(view.read(ID).unwrap().phase(), Phase::Dispatching);
}

#[test]
fn first_creation_upgrades_the_views_directory_identity_and_replacement_is_unknown() {
    let scratch = Scratch::new();
    let mut writer = OperationStore::new(scratch.root(), ENDPOINT).unwrap();
    let view = writer.read_view();
    writer
        .prepare(
            ID,
            OperationKind::StageScene,
            1,
            NOW,
            PrepareDetails::default(),
        )
        .unwrap();
    let directory = scratch.root().join("operation-receipts");
    fs::rename(&directory, scratch.root().join("retired-owned-history")).unwrap();
    fs::create_dir(&directory).unwrap();
    assert!(matches!(view.try_summary(), Err(StoreError::UnsafePath)));
    assert_eq!(view.summary()["status"], "unknown");
    assert_eq!(writer.summary()["status"], "unknown");
}

#[test]
fn external_record_edits_and_corruption_are_read_fresh_not_cached_as_clear() {
    let scratch = Scratch::new();
    let mut writer = OperationStore::new(scratch.root(), ENDPOINT).unwrap();
    let view = writer.read_view();
    writer
        .prepare(
            ID,
            OperationKind::StageScene,
            1,
            NOW,
            PrepareDetails::default(),
        )
        .unwrap();
    let path = scratch
        .root()
        .join("operation-receipts")
        .join(format!("{ID}.json"));
    let mut record: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    record["phase"] = serde_json::json!("unknown");
    fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
    assert_eq!(view.read(ID).unwrap().phase(), Phase::Unknown);
    fs::write(&path, b"{").unwrap();
    assert_eq!(view.summary()["status"], "unknown");
}
