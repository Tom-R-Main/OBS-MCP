// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{
    demo::{DemoError, Journal, JournalFile, JournalStore, inspect_recovery_summary},
    preparation_journal::{Fence, Provenance, Step},
};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
const ID: &str = "12345678-1234-4234-8234-123456789abc";
const URL: &str = "ws://127.0.0.1:4455?fixture=SYNTHETIC-SECRET";
fn fence() -> Fence {
    Fence {
        generation: 7,
        evidence_revision: 11,
    }
}
#[test]
fn metadata_inspector_is_readonly_bounded_and_fresh_across_writer_updates() {
    let root = Scratch::new();
    let missing = root.0.join("not-created");
    assert_eq!(
        inspect_recovery_summary(&missing, 64 * 1024, URL)["status"],
        "unknown"
    );
    assert!(!missing.exists());
    assert_eq!(
        inspect_recovery_summary(&root.0, 64 * 1024, URL),
        json!({"status":"observed","operations":[]})
    );
    assert!(!root.0.join("demo-recovery").exists());
    let mut store = JournalFile::new(&root.0, 64 * 1024).unwrap();
    let mut journal = initial();
    store.persist(&journal).unwrap();
    let first = inspect_recovery_summary(&root.0, 64 * 1024, URL);
    assert_eq!(
        first["operations"][0]["provenance"]["delivery"]["revision"],
        0
    );
    journal.record_intent(0, false, fence()).unwrap();
    store.persist(&journal).unwrap();
    let second = inspect_recovery_summary(&root.0, 64 * 1024, URL);
    assert_eq!(
        second["operations"][0]["provenance"]["delivery"]["revision"],
        1
    );
    assert!(
        !serde_json::to_string(&second)
            .unwrap()
            .contains("SYNTHETIC")
    );
    assert_eq!(
        inspect_recovery_summary(&root.0, 16, URL)["status"],
        "unknown"
    );
    assert_eq!(
        inspect_recovery_summary(&root.0, 64 * 1024, "ws://127.0.0.1:9999")["operations"],
        json!([])
    );
}
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "preparation-contract-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(path.canonicalize().unwrap())
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn legacy() -> Value {
    json!({"version":1,"operationId":ID,"obsUrl":URL,"snapshotId":"12345678","state":{"sceneCollection":"Fixture","sceneName":"Scene","inputName":"Window","sceneUuid":"scene-1","inputUuid":"input-1","sceneItemId":3,"settings":{"type":1,"window":123,"private":"SYNTHETIC-SETTINGS"},"muted":false,"tracks":{},"transform":{}},"changes":[{"field":"muted","before":false,"after":true}],"attempted":0})
}
fn initial() -> Journal {
    Journal::from_value(legacy())
        .unwrap()
        .import_unattempted()
        .unwrap()
}
#[test]
fn state_machine_never_upgrades_uncertain_dispatch_by_readback_and_requires_terminal_recovery() {
    let subject = json!({"subject":"fixture"});
    let mut provenance = Provenance::new(URL, &subject, 1);
    provenance.validate(URL, &subject, 1).unwrap();
    let empty = provenance.clone();
    provenance.begin_preparation(0, fence()).unwrap();
    empty.successor(&provenance).unwrap();
    assert!(!provenance.fields[0].owns_restore());
    provenance
        .advance(0, false, Step::PossibleDispatch)
        .unwrap();
    assert!(provenance.uncertain());
    assert!(provenance.observed_before(0, fence()).is_err());
    assert!(provenance.complete().is_err());
    provenance.advance(0, false, Step::Unknown).unwrap();
    assert!(provenance.advance(0, false, Step::Accepted).is_err());
    assert!(provenance.begin_recovery(0, fence()).is_err());
    let mut accepted = Provenance::new(URL, &subject, 1);
    accepted.begin_preparation(0, fence()).unwrap();
    accepted.advance(0, false, Step::PossibleDispatch).unwrap();
    accepted.advance(0, false, Step::Accepted).unwrap();
    assert!(accepted.fields[0].owns_restore());
    assert!(accepted.complete().is_err());
    accepted.begin_recovery(0, fence()).unwrap();
    accepted.advance(0, true, Step::PossibleDispatch).unwrap();
    assert!(accepted.complete().is_err());
    accepted.advance(0, true, Step::Accepted).unwrap();
    accepted.complete().unwrap();
    accepted.validate(URL, &subject, 1).unwrap();
    assert!(accepted.begin_recovery(0, fence()).is_err());
    assert!(accepted.advance(0, true, Step::PossibleDispatch).is_err());
    assert!(
        !serde_json::to_string(&accepted.summary())
            .unwrap()
            .contains("SYNTHETIC")
    );
}
#[test]
fn known_not_dispatched_stays_unowned_and_explicit_recovery_retries_append_new_fenced_attempts() {
    let subject = json!({});
    for terminal in [Step::NotDispatched, Step::Rejected] {
        let mut provenance = Provenance::new(URL, &subject, 1);
        provenance.begin_preparation(0, fence()).unwrap();
        provenance
            .advance(0, false, Step::PossibleDispatch)
            .unwrap();
        provenance.advance(0, false, terminal).unwrap();
        assert!(!provenance.fields[0].owns_restore());
        provenance.observed_before(0, fence()).unwrap();
        provenance.complete().unwrap();
        provenance.validate(URL, &subject, 1).unwrap();
    }
    let mut value = Provenance::new(URL, &subject, 1);
    value.begin_preparation(0, fence()).unwrap();
    value.advance(0, false, Step::PossibleDispatch).unwrap();
    value.advance(0, false, Step::Accepted).unwrap();
    value.begin_recovery(0, fence()).unwrap();
    value.advance(0, true, Step::NotDispatched).unwrap();
    let previous = value.clone();
    value
        .begin_recovery(
            0,
            Fence {
                generation: 8,
                evidence_revision: 1,
            },
        )
        .unwrap();
    previous.successor(&value).unwrap();
    value.advance(0, true, Step::PossibleDispatch).unwrap();
    value.advance(0, true, Step::Accepted).unwrap();
    value.complete().unwrap();
    value.validate(URL, &subject, 1).unwrap();
    assert_eq!(value.fields[0].recovery.len(), 2);
}
#[test]
fn provenance_binds_endpoint_subject_revision_and_complete_history() {
    let subject = json!({"identity":{"uuid":"source-1"}});
    let mut value = Provenance::new(URL, &subject, 1);
    value.begin_preparation(0, fence()).unwrap();
    value.advance(0, false, Step::PossibleDispatch).unwrap();
    value.advance(0, false, Step::Accepted).unwrap();
    assert!(value.validate("ws://127.0.0.1:9999", &subject, 1).is_err());
    assert!(
        value
            .validate(URL, &json!({"identity":{"uuid":"replacement"}}), 1)
            .is_err()
    );
    let mut corrupt = value.clone();
    corrupt.revision = 1;
    assert!(corrupt.validate(URL, &subject, 1).is_err());
    corrupt = value.clone();
    corrupt.completed = true;
    corrupt.revision += 1;
    assert!(corrupt.validate(URL, &subject, 1).is_err());
    corrupt = value.clone();
    corrupt.fields[0].preparation.as_mut().unwrap().steps = vec![Step::Accepted];
    corrupt.revision = 1;
    assert!(corrupt.validate(URL, &subject, 1).is_err());
    assert!(value.successor(&Provenance::new(URL, &subject, 1)).is_err());
}
#[test]
fn real_file_writer_is_v2_only_and_rejects_rewinds_jumps_and_changed_subjects() {
    let root = Scratch::new();
    let mut store = JournalFile::new(&root.0, 64 * 1024).unwrap();
    let old = Journal::from_value(legacy()).unwrap();
    assert!(matches!(
        store.persist(&old),
        Err(DemoError::LegacyJournalReadOnly)
    ));
    let mut journal = initial();
    store.persist(&journal).unwrap();
    let empty = journal.clone();
    journal.record_intent(0, false, fence()).unwrap();
    store.persist(&journal).unwrap();
    assert!(matches!(
        store.persist(&empty),
        Err(DemoError::JournalTransition)
    ));
    let mut skipped = journal.clone();
    skipped
        .record_step(0, false, Step::PossibleDispatch)
        .unwrap();
    skipped.record_step(0, false, Step::Accepted).unwrap();
    assert!(matches!(
        store.persist(&skipped),
        Err(DemoError::JournalTransition)
    ));
    journal
        .record_step(0, false, Step::PossibleDispatch)
        .unwrap();
    store.persist(&journal).unwrap();
    journal.record_step(0, false, Step::Accepted).unwrap();
    store.persist(&journal).unwrap();
    let mut changed = journal.to_value();
    changed["state"]["inputUuid"] = json!("replacement");
    assert!(Journal::from_value(changed).is_err());
    let mut changed = journal.to_value();
    changed["obsUrl"] = json!("ws://127.0.0.1:9999");
    assert!(Journal::from_value(changed).is_err());
    let mut changed = journal.to_value();
    changed["recovered"] = json!(true);
    assert!(Journal::from_value(changed).is_err());
    journal.record_intent(0, true, fence()).unwrap();
    store.persist(&journal).unwrap();
    journal
        .record_step(0, true, Step::PossibleDispatch)
        .unwrap();
    store.persist(&journal).unwrap();
    journal.record_step(0, true, Step::Accepted).unwrap();
    store.persist(&journal).unwrap();
    journal.complete_recovery().unwrap();
    store.persist(&journal).unwrap();
    let reopened = JournalFile::new(&root.0, 64 * 1024)
        .unwrap()
        .load(ID)
        .unwrap();
    assert_eq!(reopened.version(), 2);
    assert_eq!(reopened.to_value()["recovered"], true);
    assert!(reopened.provenance().unwrap().completed);
}
#[test]
fn reopened_corrupt_or_symlink_journal_refuses_and_active_owner_detects_valid_replacement() {
    let root = Scratch::new();
    let mut store = JournalFile::new(&root.0, 64 * 1024).unwrap();
    let journal = initial();
    store.persist(&journal).unwrap();
    let path = root.0.join("demo-recovery").join(format!("{ID}.json"));
    let replacement = root.0.join("replacement");
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&replacement).unwrap();
    file.write_all(&serde_json::to_vec(&journal.to_value()).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    fs::rename(&replacement, &path).unwrap();
    assert!(matches!(store.load(ID), Err(DemoError::UnsafePath)));
    let mut reopened = JournalFile::new(&root.0, 64 * 1024).unwrap();
    reopened.load(ID).unwrap();
    fs::write(&path, b"{").unwrap();
    assert!(matches!(
        JournalFile::new(&root.0, 64 * 1024).unwrap().load(ID),
        Err(DemoError::InvalidJournal)
    ));
    assert_eq!(fs::read(&path).unwrap(), b"{");
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let other = root.0.join("task-owned-private-fixture");
        fs::write(&other, b"must survive").unwrap();
        fs::remove_file(&path).unwrap();
        symlink(&other, &path).unwrap();
        assert!(matches!(
            JournalFile::new(&root.0, 64 * 1024).unwrap().load(ID),
            Err(DemoError::UnsafePath)
        ));
        assert_eq!(fs::read(&other).unwrap(), b"must survive");
    }
}
