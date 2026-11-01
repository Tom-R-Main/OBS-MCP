// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{
    snapshot_tools::{self, Request},
    snapshots::{Snapshot, SnapshotFile, SnapshotWriter},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "snapshot-tools-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn snapshot(id: &str, endpoint: Option<&str>, label: Option<&str>) -> Snapshot {
    serde_json::from_value(json!({"id":id,"obsUrl":endpoint,"label":label,"takenAt":"2026-10-31T12:00:00.000Z","inputs":[],"items":[]})).unwrap()
}
#[test]
fn source_optional_fields_distinguish_omitted_null_empty_and_unknown() {
    for args in [
        json!({}),
        json!({"scenes":[],"inputs":["", "A", "A"],"label":"","list":false,"ignored":{"kept":false}}),
        json!({"list":true}),
    ] {
        assert!(Request::parse(&args).is_ok());
    }
    for args in [
        Value::Null,
        json!([]),
        json!({"scenes":null}),
        json!({"scenes":[1]}),
        json!({"inputs":false}),
        json!({"label":null}),
        json!({"list":null}),
        json!({"list":1}),
    ] {
        assert!(Request::parse(&args).is_err(), "{args}");
    }
    assert!(!Request::parse(&json!({})).unwrap().list);
    assert!(Request::parse(&json!({"list":true})).unwrap().list);
}
#[test]
fn list_preserves_oldest_order_optional_label_and_legacy_endpoint_without_obs() {
    let dir = Scratch::new();
    let mut writer = SnapshotFile::new(&dir.0, 1024 * 1024).unwrap();
    writer.save(&snapshot("legacy", None, None)).unwrap();
    writer
        .save(&snapshot("other", Some("ws://other"), None))
        .unwrap();
    writer
        .save(&snapshot(
            "local",
            Some("ws://fixture"),
            Some("quoted \" label"),
        ))
        .unwrap();
    let result = snapshot_tools::list(&mut writer, "ws://fixture");
    assert_eq!(
        result,
        json!({"content":[{"type":"text","text":"legacy at 2026-10-31T12:00:00.000Z: 0 input(s), 0 item(s) in no scenes\nlocal \"quoted \" label\" at 2026-10-31T12:00:00.000Z: 0 input(s), 0 item(s) in no scenes"}],"structuredContent":{"snapshots":[{"id":"legacy","takenAt":"2026-10-31T12:00:00.000Z","inputs":0,"items":0},{"id":"local","label":"quoted \" label","takenAt":"2026-10-31T12:00:00.000Z","inputs":0,"items":0}]}})
    );
}
#[test]
fn established_history_replacement_refuses_explicitly_and_preserves_both_files() {
    let dir = Scratch::new();
    let mut writer = SnapshotFile::new(&dir.0, 1024 * 1024).unwrap();
    writer
        .save(&snapshot("one", Some("ws://fixture"), None))
        .unwrap();
    let original = dir.0.join("snapshots.json");
    let replacement = dir.0.join("replacement");
    fs::copy(&original, &replacement).unwrap();
    fs::rename(replacement, &original).unwrap();
    let before = fs::read(&original).unwrap();
    let result = snapshot_tools::list(&mut writer, "ws://fixture");
    assert_eq!(result["isError"], true);
    assert_eq!(
        result["content"][0]["text"],
        "Snapshot failed: Established snapshot history was replaced"
    );
    assert_eq!(fs::read(original).unwrap(), before);
}
