// SPDX-License-Identifier: GPL-2.0-only
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use obs_mcp_native::operation_store::{
    DISPLAY_RECEIPTS, MAX_RECEIPT_BYTES, MAX_RECEIPTS, OperationKind, OperationReceipt,
    OperationStore, OperationUpdate, Phase, PrepareDetails, StoreError,
};
use serde_json::{Value, json};

const ENDPOINT: &str = "ws://127.0.0.1:4455";
const CREATED: &str = "2026-10-31T12:00:00.000Z";
const UPDATED: &str = "2026-10-31T12:00:01.000Z";

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let sequence = SEQUENCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .unwrap();
        // macOS's /var is a symlink; the approved base is its canonical target.
        let parent = std::env::temp_dir().canonicalize().unwrap();
        let path = parent.join(format!(
            "obs-operation-store-contract-{}-{sequence}",
            std::process::id()
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
    fn directory(&self) -> PathBuf {
        self.root().join("operation-receipts")
    }
    fn path(&self, id: &str) -> PathBuf {
        self.directory().join(format!("{id}.json"))
    }
    fn store(&self) -> OperationStore {
        OperationStore::new(self.root(), ENDPOINT).unwrap()
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn id(number: usize) -> String {
    format!("00000000-0000-4000-8000-{number:012x}")
}
fn prepare(store: &mut OperationStore, number: usize) -> OperationReceipt {
    store
        .prepare(
            &id(number),
            OperationKind::TakeScene,
            3,
            CREATED,
            PrepareDetails::default(),
        )
        .unwrap()
}
fn node(root: &Path, endpoint: &str, body: &str) -> Value {
    let project = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let script = format!(
        r#"
import {{OBSWebSocketClient}} from "./build/client.js";
import {{operationSummary,prepareOperation,updateOperation}} from "./build/tools/operation-receipts.js";
const client=new OBSWebSocketClient(process.env.OPERATION_TEST_ENDPOINT);
{body}
"#
    );
    let output = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(script)
        .current_dir(project)
        .env("OBS_MCP_STATE_DIR", root)
        .env("OPERATION_TEST_ENDPOINT", endpoint)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "isolated TypeScript receipt oracle failed"
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn fresh_missing_inventory_and_named_missing_receipt_are_distinct() {
    let scratch = Scratch::new();
    let store = scratch.store();
    assert_eq!(
        store.summary(),
        json!({"status":"observed","operations":[],"total":0,"truncated":false})
    );
    assert_eq!(store.read(&id(1)), Err(StoreError::Missing));
    assert!(
        !scratch.root().exists(),
        "read-only inspection must not create state"
    );
}

#[test]
fn durable_prepare_dispatch_and_restart_preserve_unknown_without_reconciliation() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let prepared = prepare(&mut store, 1);
    assert_eq!(store.read(&id(1)).unwrap(), prepared);
    let mut update = OperationUpdate::new(Phase::Dispatching);
    update.request_dispatched = Some(None);
    let dispatched = store.update(&prepared, update, UPDATED).unwrap();
    assert_eq!(dispatched.phase(), Phase::Dispatching);
    assert_eq!(dispatched.generation(), 3);
    drop(store);
    let restarted = scratch.store();
    let summary = restarted.summary();
    assert_eq!(summary["operations"][0]["status"], "unknown");
    assert_eq!(summary["operations"][0]["requestDispatched"], Value::Null);
    assert_eq!(summary["operations"][0]["requestAccepted"], Value::Null);
    assert_eq!(summary["operations"][0]["postconditionObserved"], false);
    assert_eq!(
        summary["operations"][0]["renderedComposition"],
        "unverified"
    );
    assert!(summary["operations"][0].get("endpointHash").is_none());
    assert_eq!(
        summary,
        node(
            &scratch.root(),
            ENDPOINT,
            "console.log(JSON.stringify(operationSummary(client)));"
        )
    );
}

#[test]
fn typescript_receipts_are_read_and_updated_without_runtime_or_format_migration() {
    let scratch = Scratch::new();
    let value = node(
        &scratch.root(),
        ENDPOINT,
        r#"
const receipt=prepareOperation(client,"record-start",{sceneCollection:"fixture",sceneUuid:"scene-id",takeId:"take-id"});
console.log(JSON.stringify(updateOperation(receipt,{phase:"accepted",requestDispatched:true,requestAccepted:true})));
"#,
    );
    let mut store = scratch.store();
    let receipt = store.read(value["operationId"].as_str().unwrap()).unwrap();
    assert_eq!(receipt.to_value(), value);
    assert_eq!(
        store.summary(),
        node(
            &scratch.root(),
            ENDPOINT,
            "console.log(JSON.stringify(operationSummary(client)));"
        )
    );
    let mut update = OperationUpdate::new(Phase::Observed);
    update.postcondition_observed = Some(true);
    let observed = store
        .update(&receipt, update, "2026-11-01T00:00:00Z")
        .unwrap();
    assert_eq!(observed.to_value()["postconditionObserved"], true);
    assert_eq!(
        store.summary(),
        node(
            &scratch.root(),
            ENDPOINT,
            "console.log(JSON.stringify(operationSummary(client)));"
        )
    );
}

#[test]
fn raw_endpoint_and_collection_values_never_enter_receipts_summaries_or_errors() {
    let scratch = Scratch::new();
    let endpoint = "ws://fixture-user:fixture-password@127.0.0.1:4455/?token=fixture-token";
    let mut store = OperationStore::new(scratch.root(), endpoint).unwrap();
    let receipt = store
        .prepare(
            &id(1),
            OperationKind::StageScene,
            0,
            CREATED,
            PrepareDetails {
                scene_collection: Some("private fixture collection"),
                scene_uuid: Some("scene-id"),
                stage_id: Some(&id(2)),
                take_id: Some("take-id"),
            },
        )
        .unwrap();
    let bytes = fs::read(scratch.path(&id(1))).unwrap();
    let serialized = String::from_utf8(bytes).unwrap();
    let summary = store.summary().to_string();
    for sensitive in [
        "fixture-password",
        "fixture-token",
        "fixture-user",
        "private fixture collection",
        "ws://",
    ] {
        assert!(!serialized.contains(sensitive));
        assert!(!summary.contains(sensitive));
        assert!(!StoreError::Unavailable.to_string().contains(sensitive));
    }
    assert_eq!(
        receipt.to_value()["endpointHash"].as_str().unwrap().len(),
        64
    );
    assert_eq!(
        store.summary(),
        node(
            &scratch.root(),
            endpoint,
            "console.log(JSON.stringify(operationSummary(client)));"
        )
    );
}

#[test]
fn endpoint_isolation_and_foreign_corruption_match_the_predecessor() {
    let scratch = Scratch::new();
    let mut first = scratch.store();
    let a = prepare(&mut first, 1);
    let mut other = OperationStore::new(scratch.root(), "ws://127.0.0.1:4456").unwrap();
    other
        .prepare(
            &id(2),
            OperationKind::RecordStop,
            0,
            CREATED,
            PrepareDetails::default(),
        )
        .unwrap();
    assert_eq!(first.summary()["total"], 1);
    assert_eq!(other.summary()["total"], 1);
    assert_eq!(first.read(&id(2)), Err(StoreError::EndpointMismatch));
    assert_eq!(
        other.update(&a, OperationUpdate::new(Phase::Unknown), UPDATED),
        Err(StoreError::EndpointMismatch)
    );
    fs::write(scratch.path(&id(2)), "{}").unwrap();
    assert_eq!(
        first.summary()["status"],
        "unknown",
        "foreign entries are validated before filtering"
    );
    assert_eq!(
        first.summary(),
        node(
            &scratch.root(),
            ENDPOINT,
            "console.log(JSON.stringify(operationSummary(client)));"
        )
    );
}

#[test]
fn stale_updates_duplicate_prepares_and_corrupt_current_records_never_overwrite() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let prepared = prepare(&mut store, 1);
    assert_eq!(
        store.prepare(
            &id(1),
            OperationKind::RecordStart,
            0,
            CREATED,
            PrepareDetails::default()
        ),
        Err(StoreError::Conflict)
    );
    let accepted = store
        .update(&prepared, OperationUpdate::new(Phase::Accepted), UPDATED)
        .unwrap();
    assert_eq!(
        store.update(&prepared, OperationUpdate::new(Phase::Observed), UPDATED),
        Err(StoreError::Conflict)
    );
    assert_eq!(store.read(&id(1)).unwrap(), accepted);
    fs::write(scratch.path(&id(1)), "{}").unwrap();
    assert_eq!(
        store.update(&accepted, OperationUpdate::new(Phase::Observed), UPDATED),
        Err(StoreError::Corrupt)
    );
    assert_eq!(fs::read_to_string(scratch.path(&id(1))).unwrap(), "{}");
    assert_eq!(store.summary()["status"], "unknown");
}

#[test]
fn strict_schema_rejects_invalid_fields_while_preserving_valid_json_integer_and_date_forms() {
    let scratch = Scratch::new();
    let base = prepare(&mut scratch.store(), 1).to_value();
    for (field, invalid) in [
        ("version", json!(2)),
        ("payload", json!({})),
        ("operationId", json!("../outside")),
        ("kind", json!("other")),
        ("endpointHash", json!("A".repeat(64))),
        ("collectionHash", Value::Null),
        ("sceneUuid", json!("")),
        ("sceneUuid", json!("🙂".repeat(129))),
        ("takeId", Value::Null),
        ("stageId", json!("00000000-0000-0000-8000-000000000000")),
        ("generation", json!(-1)),
        ("generation", json!(1.5)),
        ("generation", json!(9_007_199_254_740_992_u64)),
        ("createdAt", json!("2026-02-29T12:00:00Z")),
        ("createdAt", json!("2026-10-31T12:00Z")),
        ("createdAt", json!("2026-10-31T24:00:00Z")),
        ("createdAt", json!("2026-10-31T12:00:00+00:00")),
        ("createdAt", json!("2026-10-31T12:00:00.Z")),
        ("phase", json!("failed")),
        ("requestDispatched", json!("unknown")),
        ("requestAccepted", json!(1)),
        ("postconditionObserved", Value::Null),
        ("renderedComposition", json!("verified")),
    ] {
        let mut invalid_record = base.clone();
        invalid_record[field] = invalid;
        assert!(
            OperationReceipt::from_value(invalid_record).is_err(),
            "field {field} must fail admission"
        );
    }
    for field in [
        "requestDispatched",
        "requestAccepted",
        "version",
        "postconditionObserved",
    ] {
        let mut missing = base.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            OperationReceipt::from_value(missing).is_err(),
            "required field {field} must be present even when nullable"
        );
    }
    for valid in [
        "0000-02-29T00:00:00Z",
        "2000-02-29T23:59:59.1Z",
        "2026-10-31T12:00:00.123456789Z",
    ] {
        let mut value = base.clone();
        value["createdAt"] = json!(valid);
        value["generation"] = json!(1.0);
        value["version"] = json!(1.0);
        assert!(OperationReceipt::from_value(value).is_ok());
    }
    let mut max = base;
    max["generation"] = json!(9_007_199_254_740_991_u64);
    assert!(OperationReceipt::from_value(max).is_ok());
}

#[test]
fn pending_first_pagination_preserves_sorting_and_cannot_hide_old_unknown_operations() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let base = prepare(&mut store, 0).to_value();
    for number in 0..71 {
        let mut value = base.clone();
        value["operationId"] = json!(id(number));
        value["createdAt"] = json!(format!(
            "2026-10-31T12:{:02}:{:02}.000Z",
            number / 60,
            number % 60
        ));
        value["phase"] = json!(if number == 0 {
            "dispatching"
        } else {
            "observed"
        });
        fs::write(
            scratch.path(&id(number)),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
    }
    let summary = store.try_summary().unwrap();
    assert_eq!(summary.total, 71);
    assert_eq!(summary.operations.len(), DISPLAY_RECEIPTS);
    assert!(summary.truncated);
    assert_eq!(summary.operations[0]["operationId"], id(0));
    assert_eq!(summary.operations[0]["status"], "unknown");
    assert_eq!(summary.operations[1]["operationId"], id(70));
    assert_eq!(summary.operations.last().unwrap()["operationId"], id(8));
    assert_eq!(
        store.summary(),
        node(
            &scratch.root(),
            ENDPOINT,
            "console.log(JSON.stringify(operationSummary(client)));"
        )
    );
}

#[test]
fn tie_breaking_matches_javascript_locale_order_for_uppercase_valid_identifiers() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let base = prepare(&mut store, 1).to_value();
    // These differ in their last digit as well as case, so both can coexist on
    // case-insensitive macOS filesystems while challenging case-first sorting.
    for operation_id in [
        "AAAAAAAA-0000-4000-8000-000000000002",
        "aaaaaaaa-0000-4000-8000-000000000001",
    ] {
        let mut value = base.clone();
        value["operationId"] = json!(operation_id);
        fs::write(
            scratch.path(operation_id),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
    }
    assert_eq!(
        store.summary(),
        node(
            &scratch.root(),
            ENDPOINT,
            "console.log(JSON.stringify(operationSummary(client)));"
        )
    );
}

#[test]
fn fractional_utc_strings_retain_the_established_locale_sort_instead_of_becoming_time_arithmetic() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let base = prepare(&mut store, 0).to_value();
    for (number, utc) in [
        "2026-10-31T12:00:00Z",
        "2026-10-31T12:00:00.0Z",
        "2026-10-31T12:00:00.001Z",
        "2026-10-31T12:00:00.01Z",
        "2026-10-31T12:00:00.1Z",
        "2026-10-31T12:00:00.10Z",
        "2026-10-31T12:00:00.100Z",
        "2026-10-31T12:00:01Z",
    ]
    .into_iter()
    .enumerate()
    {
        let mut value = base.clone();
        value["operationId"] = json!(id(number));
        value["createdAt"] = json!(utc);
        fs::write(
            scratch.path(&id(number)),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
    }
    assert_eq!(
        store.summary(),
        node(
            &scratch.root(),
            ENDPOINT,
            "console.log(JSON.stringify(operationSummary(client)));"
        )
    );
}

#[test]
fn corrupt_oversize_and_mismatched_filenames_are_unknown_instead_of_empty_success() {
    for mode in ["corrupt", "oversize", "mismatch", "invalid-name"] {
        let scratch = Scratch::new();
        let mut store = scratch.store();
        let receipt = prepare(&mut store, 1);
        match mode {
            "corrupt" => fs::write(scratch.path(&id(1)), "{}").unwrap(),
            "oversize" => fs::File::create(scratch.path(&id(1)))
                .unwrap()
                .set_len(MAX_RECEIPT_BYTES as u64 + 1)
                .unwrap(),
            "mismatch" => {
                fs::write(scratch.path(&id(2)), serde_json::to_vec(&receipt).unwrap()).unwrap()
            }
            _ => fs::write(
                scratch.directory().join("not-an-id.json"),
                serde_json::to_vec(&receipt).unwrap(),
            )
            .unwrap(),
        }
        assert!(store.try_summary().is_err());
        assert_eq!(store.summary()["status"], "unknown");
        assert_eq!(store.summary()["reason"], "receipt_unavailable");
        assert!(store.summary().get("total").is_none());
        assert_eq!(
            store.summary(),
            node(
                &scratch.root(),
                ENDPOINT,
                "console.log(JSON.stringify(operationSummary(client)));"
            )
        );
    }
}

#[test]
fn inventory_admission_stops_at_the_item_limit() {
    let scratch = Scratch::new();
    let mut store = scratch.store();
    prepare(&mut store, 0);
    for number in 1..=MAX_RECEIPTS {
        fs::write(scratch.path(&id(number)), b"{}").unwrap();
    }
    assert!(matches!(
        store.try_summary(),
        Err(StoreError::InventoryLimit)
    ));
    assert_eq!(store.summary()["status"], "unknown");
}

#[test]
fn create_new_staging_never_overwrites_an_existing_temporary_file() {
    let scratch = Scratch::new();
    fs::create_dir_all(scratch.directory()).unwrap();
    let existing = scratch
        .directory()
        .join(format!("{}.{}.1.tmp", id(1), std::process::id()));
    fs::write(&existing, b"preexisting temporary evidence").unwrap();
    let mut store = scratch.store();
    assert_eq!(
        store.prepare(
            &id(1),
            OperationKind::TakeScene,
            0,
            CREATED,
            PrepareDetails::default()
        ),
        Err(StoreError::Unavailable)
    );
    assert_eq!(
        fs::read(&existing).unwrap(),
        b"preexisting temporary evidence"
    );
    assert!(!scratch.path(&id(1)).exists());
}

#[test]
fn path_escape_invalid_timestamps_and_lost_inventory_fail_closed() {
    let scratch = Scratch::new();
    assert!(matches!(
        OperationStore::new("relative/path", ENDPOINT),
        Err(StoreError::UnsafePath)
    ));
    assert!(matches!(
        OperationStore::new(scratch.root().join("../outside"), ENDPOINT),
        Err(StoreError::UnsafePath)
    ));
    let mut store = scratch.store();
    assert_eq!(
        store.prepare(
            "../outside",
            OperationKind::TakeScene,
            0,
            CREATED,
            PrepareDetails::default()
        ),
        Err(StoreError::InvalidIdentifier)
    );
    assert_eq!(
        store.prepare(
            &id(1),
            OperationKind::TakeScene,
            0,
            "invalid",
            PrepareDetails::default()
        ),
        Err(StoreError::InvalidTimestamp)
    );
    prepare(&mut store, 1);
    fs::remove_dir_all(scratch.directory()).unwrap();
    assert!(matches!(store.try_summary(), Err(StoreError::UnsafePath)));
    assert_eq!(store.summary()["status"], "unknown");
}

#[cfg(unix)]
#[test]
fn symlink_roots_ancestors_receipts_and_hardlinks_cannot_escape_the_owned_store() {
    use std::os::unix::fs::symlink;
    let scratch = Scratch::new();
    let mut store = scratch.store();
    prepare(&mut store, 1);
    symlink(scratch.root(), scratch.0.join("alias")).unwrap();
    assert!(matches!(
        OperationStore::new(scratch.0.join("alias"), ENDPOINT),
        Err(StoreError::UnsafePath)
    ));
    assert!(matches!(
        OperationStore::new(scratch.0.join("alias/nested"), ENDPOINT),
        Err(StoreError::UnsafePath)
    ));
    let outside = scratch.0.join("outside.json");
    fs::rename(scratch.path(&id(1)), &outside).unwrap();
    let original = fs::read(&outside).unwrap();
    symlink(&outside, scratch.path(&id(1))).unwrap();
    assert_eq!(store.read(&id(1)), Err(StoreError::UnsafePath));
    assert_eq!(store.summary()["status"], "unknown");
    assert_eq!(fs::read(&outside).unwrap(), original);
    fs::remove_file(scratch.path(&id(1))).unwrap();
    fs::hard_link(&outside, scratch.path(&id(1))).unwrap();
    assert_eq!(store.read(&id(1)), Err(StoreError::UnsafePath));
    assert_eq!(fs::read(&outside).unwrap(), original);
}

#[cfg(unix)]
#[test]
fn symlink_inventory_and_replaced_directory_are_rejected_without_outside_writes() {
    use std::os::unix::fs::symlink;
    let scratch = Scratch::new();
    fs::create_dir_all(scratch.root()).unwrap();
    let outside = scratch.0.join("outside");
    fs::create_dir(&outside).unwrap();
    symlink(&outside, scratch.directory()).unwrap();
    assert!(matches!(
        OperationStore::new(scratch.root(), ENDPOINT),
        Err(StoreError::UnsafePath)
    ));
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
    fs::remove_file(scratch.directory()).unwrap();
    let mut store = scratch.store();
    prepare(&mut store, 1);
    fs::rename(scratch.root(), scratch.0.join("old-state")).unwrap();
    fs::create_dir(scratch.root()).unwrap();
    assert!(matches!(store.try_summary(), Err(StoreError::UnsafePath)));
    assert_eq!(
        store.prepare(
            &id(2),
            OperationKind::TakeScene,
            0,
            CREATED,
            PrepareDetails::default()
        ),
        Err(StoreError::UnsafePath)
    );
}

#[cfg(unix)]
#[test]
fn prepared_files_are_private_and_publication_leaves_no_temporary_files() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new();
    let mut store = scratch.store();
    let receipt = prepare(&mut store, 1);
    store
        .update(&receipt, OperationUpdate::new(Phase::Unknown), UPDATED)
        .unwrap();
    assert_eq!(
        fs::metadata(scratch.directory())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(scratch.path(&id(1)))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(fs::read_dir(scratch.directory()).unwrap().count(), 1);
}
