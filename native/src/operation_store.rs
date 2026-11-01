// SPDX-License-Identifier: GPL-2.0-only
//! Durable identity-only operation receipts. The caller supplies operation IDs
//! and UTC timestamps and retains the sole mutable writer for this store.
//!
//! Roots must be absolute, approved, non-symlink paths. Static symlinks,
//! hardlinked receipts and replaced directory identities fail closed. These
//! standard-library path checks are not a filesystem sandbox against a process
//! with the same privileges racing ancestor-directory replacement. Such a
//! threat requires descriptor-relative platform APIs before this owner is used
//! in a shared untrusted directory.

use std::{
    cell::Cell,
    fmt,
    fs::{self, DirBuilder, File, Metadata, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const MAX_RECEIPT_BYTES: usize = 16_384;
pub const MAX_RECEIPTS: usize = 1_024;
pub const DISPLAY_RECEIPTS: usize = 64;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const PENDING_CHECK: &str =
    "Inspect fresh OBS state; this receipt does not authorize replay or prove a rendered result";
const SEPARATE_CHECK: &str =
    "Current OBS state and rendered composition require separate inspection";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    InvalidIdentifier,
    InvalidTimestamp,
    InvalidReceipt,
    UnsafePath,
    Missing,
    Corrupt,
    ReceiptTooLarge,
    InventoryLimit,
    EndpointMismatch,
    Conflict,
    IdentityExhausted,
    Unavailable,
    /// Rename completed: inspect the supplied operation ID before any retry.
    DurabilityUncertain,
}

impl fmt::Display for StoreError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(match self {
            Self::InvalidIdentifier => "Operation receipt identifier is invalid",
            Self::InvalidTimestamp => "Operation receipt UTC timestamp is invalid",
            Self::InvalidReceipt => "Operation receipt does not match the version 1 contract",
            Self::UnsafePath => "Operation receipt storage path cannot be trusted",
            Self::Missing => "The requested operation receipt is missing",
            Self::Corrupt => "An operation receipt could not be validated",
            Self::ReceiptTooLarge => "An operation receipt exceeds its byte limit",
            Self::InventoryLimit => "Operation receipt inventory exceeds its item limit",
            Self::EndpointMismatch => "Operation receipt belongs to a different endpoint",
            Self::Conflict => "Operation receipt changed or its identifier is already in use",
            Self::IdentityExhausted => "Operation receipt temporary identity is exhausted",
            Self::Unavailable => "Operation receipt storage is unavailable",
            Self::DurabilityUncertain => {
                "Operation receipt publication occurred but durability is unconfirmed"
            }
        })
    }
}
impl std::error::Error for StoreError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationKind {
    StageScene,
    TakeScene,
    RecordStart,
    RecordStop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    Prepared,
    Dispatching,
    Accepted,
    Observed,
    NotDispatched,
    Unknown,
}
impl Phase {
    pub fn is_uncertain(self) -> bool {
        matches!(self, Self::Dispatching | Self::Accepted | Self::Unknown)
    }
    fn name(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Dispatching => "dispatching",
            Self::Accepted => "accepted",
            Self::Observed => "observed",
            Self::NotDispatched => "not-dispatched",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Record {
    version: u8,
    operation_id: String,
    kind: OperationKind,
    endpoint_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    collection_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scene_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stage_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    take_id: Option<String>,
    generation: u64,
    created_at: String,
    updated_at: String,
    phase: Phase,
    request_dispatched: Option<bool>,
    request_accepted: Option<bool>,
    postcondition_observed: bool,
    rendered_composition: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationReceipt(Record);

impl OperationReceipt {
    pub fn operation_id(&self) -> &str {
        &self.0.operation_id
    }
    pub fn phase(&self) -> Phase {
        self.0.phase
    }
    pub fn generation(&self) -> u64 {
        self.0.generation
    }
    pub fn to_value(&self) -> Value {
        serde_json::to_value(&self.0).expect("validated receipt is JSON serializable")
    }
    pub fn from_json(bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(StoreError::ReceiptTooLarge);
        }
        let value = serde_json::from_slice(bytes).map_err(|_| StoreError::Corrupt)?;
        Self::from_value(value)
    }
    pub fn from_value(mut value: Value) -> Result<Self, StoreError> {
        let object = value.as_object_mut().ok_or(StoreError::InvalidReceipt)?;
        for required in [
            "version",
            "operationId",
            "kind",
            "endpointHash",
            "generation",
            "createdAt",
            "updatedAt",
            "phase",
            "requestDispatched",
            "requestAccepted",
            "postconditionObserved",
            "renderedComposition",
        ] {
            if !object.contains_key(required) {
                return Err(StoreError::InvalidReceipt);
            }
        }
        for optional in ["collectionHash", "sceneUuid", "stageId", "takeId"] {
            if object.get(optional).is_some_and(Value::is_null) {
                return Err(StoreError::InvalidReceipt);
            }
        }
        // JSON numbers such as 1.0 remain valid JavaScript integers. Normalize
        // them only after checking the same safe-integer bounds as Zod.
        for (field, maximum) in [("version", 1), ("generation", MAX_SAFE_INTEGER)] {
            let number = object[field]
                .as_u64()
                .or_else(|| {
                    object[field].as_f64().and_then(|number| {
                        (number.is_finite()
                            && number >= 0.0
                            && number.fract() == 0.0
                            && number <= maximum as f64)
                            .then_some(number as u64)
                    })
                })
                .filter(|number| *number <= maximum)
                .ok_or(StoreError::InvalidReceipt)?;
            object.insert(field.to_owned(), json!(number));
        }
        Self::checked(serde_json::from_value(value).map_err(|_| StoreError::InvalidReceipt)?)
    }
    fn checked(record: Record) -> Result<Self, StoreError> {
        if record.version != 1
            || !valid_uuid(&record.operation_id)
            || !valid_hash(&record.endpoint_hash)
            || record
                .collection_hash
                .as_deref()
                .is_some_and(|value| !valid_hash(value))
            || record
                .stage_id
                .as_deref()
                .is_some_and(|value| !valid_uuid(value))
            || [record.scene_uuid.as_deref(), record.take_id.as_deref()]
                .into_iter()
                .flatten()
                .any(|value| !bounded_identity(value))
            || record.generation > MAX_SAFE_INTEGER
            || !valid_timestamp(&record.created_at)
            || !valid_timestamp(&record.updated_at)
            || record.rendered_composition != "unverified"
        {
            return Err(StoreError::InvalidReceipt);
        }
        Ok(Self(record))
    }
}
impl Serialize for OperationReceipt {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for OperationReceipt {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::from_value(Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Default)]
pub struct PrepareDetails<'a> {
    pub scene_collection: Option<&'a str>,
    pub scene_uuid: Option<&'a str>,
    pub stage_id: Option<&'a str>,
    pub take_id: Option<&'a str>,
}

pub struct OperationUpdate {
    pub phase: Phase,
    /// None retains the old value; Some(None) explicitly records uncertainty.
    pub request_dispatched: Option<Option<bool>>,
    pub request_accepted: Option<Option<bool>>,
    pub postcondition_observed: Option<bool>,
}
impl OperationUpdate {
    pub fn new(phase: Phase) -> Self {
        Self {
            phase,
            request_dispatched: None,
            request_accepted: None,
            postcondition_observed: None,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ObservedSummary {
    status: &'static str,
    pub operations: Vec<Value>,
    pub total: usize,
    pub truncated: bool,
}

#[derive(Clone)]
struct LayoutIdentity {
    root: Option<Metadata>,
    directory: Option<Metadata>,
}
struct ReadNamespace {
    poisoned: AtomicBool,
    root: PathBuf,
    directory: PathBuf,
    endpoint_hash: String,
    identities: Mutex<LayoutIdentity>,
}
/// A fresh filesystem inspector bound to this writer's actual namespace.
/// It has no writer constructor, mutation methods or publication sequence.
/// Clones do not extend any process/transport owner or authorize replay.
#[derive(Clone)]
pub struct OperationView {
    inner: Arc<ReadNamespace>,
}
impl OperationView {
    pub(crate) fn mark_uncertain(&self) {
        self.inner.poisoned.store(true, Ordering::Release);
    }
    pub fn state_root(&self) -> &Path {
        &self.inner.root
    }
    pub fn endpoint_hash(&self) -> &str {
        &self.inner.endpoint_hash
    }
    pub fn read(&self, operation_id: &str) -> Result<OperationReceipt, StoreError> {
        if !valid_uuid(operation_id) {
            return Err(StoreError::InvalidIdentifier);
        }
        if !self.validate_layout()? {
            return Err(StoreError::Missing);
        }
        let receipt = read_receipt(&self.inner.directory.join(format!("{operation_id}.json")))?;
        if receipt.operation_id() != operation_id {
            return Err(StoreError::Corrupt);
        }
        self.check_endpoint(&receipt)?;
        self.validate_layout()?;
        Ok(receipt)
    }

    fn inventory(&self) -> Result<Vec<StoredReceipt>, StoreError> {
        if self.inner.poisoned.load(Ordering::Acquire) {
            return Err(StoreError::DurabilityUncertain);
        }
        if !self.validate_layout()? {
            return Ok(vec![]);
        }
        let mut receipts = Vec::new();
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.inner.directory).map_err(|_| StoreError::Unavailable)? {
            let name = entry
                .map_err(|_| StoreError::Unavailable)?
                .file_name()
                .into_string()
                .map_err(|_| StoreError::Corrupt)?;
            if !name.ends_with(".json") {
                continue;
            }
            names.push(name);
            if names.len() > MAX_RECEIPTS {
                return Err(StoreError::InventoryLimit);
            }
        }
        for name in names {
            let id = name.strip_suffix(".json").ok_or(StoreError::Corrupt)?;
            if !valid_uuid(id) {
                return Err(StoreError::Corrupt);
            }
            let receipt = read_stored(&self.inner.directory.join(&name))?;
            if receipt.id() != id {
                return Err(StoreError::Corrupt);
            }
            if receipt.endpoint() == self.inner.endpoint_hash {
                receipts.push(receipt);
            }
        }
        self.validate_layout()?;
        receipts.sort_by(|a, b| {
            b.created()
                .cmp(a.created())
                .then_with(|| uuid_collation(a.id(), b.id()))
        });
        Ok(receipts)
    }
    pub(crate) fn capacity_used(&self) -> Result<usize, StoreError> {
        if !self.validate_layout()? {
            return Ok(0);
        }
        let mut count = 0;
        for entry in fs::read_dir(&self.inner.directory).map_err(|_| StoreError::Unavailable)? {
            let name = entry
                .map_err(|_| StoreError::Unavailable)?
                .file_name()
                .into_string()
                .map_err(|_| StoreError::Corrupt)?;
            if name.ends_with(".json") {
                count += 1;
                if count > MAX_RECEIPTS {
                    return Err(StoreError::InventoryLimit);
                }
            }
        }
        self.validate_layout()?;
        Ok(count)
    }
    pub fn read_any(&self, id: &str) -> Result<Value, StoreError> {
        if !valid_uuid(id) {
            return Err(StoreError::InvalidIdentifier);
        }
        self.inventory()?
            .into_iter()
            .find(|r| r.id() == id)
            .map(|r| match r {
                StoredReceipt::V1(r) => r.to_value(),
                StoredReceipt::Generic(r) => r.to_value(),
            })
            .ok_or(StoreError::Missing)
    }
    /// Admission always validates the entire bounded inventory, never a UI page.
    pub fn admission(&self) -> Result<Admission, StoreError> {
        self.admission_except(None)
    }
    pub(crate) fn is_dispatching(&self, id: &str) -> Result<bool, StoreError> {
        Ok(self.inventory()?.iter().any(|receipt| {
            receipt.id() == id
                && match receipt {
                    StoredReceipt::V1(r) => r.phase() == Phase::Dispatching,
                    StoredReceipt::Generic(r) => r.phase() == GenericPhase::Dispatching,
                }
        }))
    }
    pub(crate) fn admission_except(&self, own: Option<&str>) -> Result<Admission, StoreError> {
        let inventory = self.inventory()?;
        Ok(Admission {
            blocking: inventory
                .iter()
                .any(|entry| Some(entry.id()) != own && entry.blocking()),
            generic_v2_present: inventory
                .iter()
                .any(|entry| matches!(entry, StoredReceipt::Generic(_))),
            total: inventory.len(),
        })
    }
    pub fn try_summary(&self) -> Result<ObservedSummary, StoreError> {
        let inventory = self.inventory()?;
        let total = inventory.len();
        let operations = inventory
            .iter()
            .filter(|receipt| receipt.blocking())
            .chain(inventory.iter().filter(|receipt| !receipt.blocking()))
            .take(DISPLAY_RECEIPTS)
            .map(StoredReceipt::summary)
            .collect::<Vec<_>>();
        Ok(ObservedSummary {
            status: "observed",
            truncated: total > operations.len(),
            total,
            operations,
        })
    }
    /// Match the established wire summary: inspection failure is uncertainty,
    /// never an observed empty history. Call try_summary for the typed cause.
    pub fn summary(&self) -> Value {
        match self.try_summary() {
            Ok(summary) => serde_json::to_value(summary).expect("summary is JSON serializable"),
            Err(_) => {
                json!({ "status": "unknown", "operations": [], "reason": "receipt_unavailable", "nextCheck": "Operation receipts could not be inspected; do not infer that no operation is pending" })
            }
        }
    }

    fn check_endpoint(&self, receipt: &OperationReceipt) -> Result<(), StoreError> {
        if receipt.0.endpoint_hash == self.inner.endpoint_hash {
            Ok(())
        } else {
            Err(StoreError::EndpointMismatch)
        }
    }
    fn validate_layout(&self) -> Result<bool, StoreError> {
        validate_chain(&self.inner.directory, true)?;
        let expected = self
            .inner
            .identities
            .lock()
            .map_err(|_| StoreError::Unavailable)?
            .clone();
        let root = directory_metadata(&self.inner.root)?;
        let directory = directory_metadata(&self.inner.directory)?;
        for (expected, current) in [(&expected.root, &root), (&expected.directory, &directory)] {
            if let Some(expected) = expected
                && current
                    .as_ref()
                    .is_none_or(|current| !same_identity(expected, current))
            {
                return Err(StoreError::UnsafePath);
            }
        }
        Ok(directory.is_some())
    }
}

/// No raw endpoint URL is retained, and this writer is deliberately not Clone
/// or Sync. The enclosing server must retain one mutable owner per state root.
pub(crate) struct BackendStore {
    root: PathBuf,
    directory: PathBuf,
    endpoint_hash: String,
    root_identity: Option<Metadata>,
    directory_identity: Option<Metadata>,
    sequence: u64,
    _single_owner: Cell<()>,
    read_namespace: Arc<ReadNamespace>,
}

impl BackendStore {
    /// Identity-only binding for the session owner; the original URL is never retained.
    pub fn endpoint_hash(&self) -> &str {
        &self.endpoint_hash
    }

    pub fn new(root: impl AsRef<Path>, endpoint: &str) -> Result<Self, StoreError> {
        let root = root.as_ref().to_path_buf();
        validate_chain(&root, true)?;
        let directory = root.join("operation-receipts");
        validate_chain(&directory, true)?;
        let root_identity = directory_metadata(&root)?;
        let directory_identity = directory_metadata(&directory)?;
        let endpoint_hash = hash(endpoint);
        let read_namespace = Arc::new(ReadNamespace {
            poisoned: AtomicBool::new(false),
            root: root.clone(),
            directory: directory.clone(),
            endpoint_hash: endpoint_hash.clone(),
            identities: Mutex::new(LayoutIdentity {
                root: root_identity.clone(),
                directory: directory_identity.clone(),
            }),
        });
        Ok(Self {
            root,
            directory,
            endpoint_hash,
            root_identity,
            directory_identity,
            sequence: 0,
            _single_owner: Cell::new(()),
            read_namespace,
        })
    }

    /// Persist intent before dispatch. On DurabilityUncertain the supplied ID
    /// may already exist; inspect it instead of preparing or dispatching again.
    pub fn prepare(
        &mut self,
        operation_id: &str,
        kind: OperationKind,
        generation: u64,
        utc: &str,
        details: PrepareDetails<'_>,
    ) -> Result<OperationReceipt, StoreError> {
        if !valid_uuid(operation_id) {
            return Err(StoreError::InvalidIdentifier);
        }
        if !valid_timestamp(utc) {
            return Err(StoreError::InvalidTimestamp);
        }
        let receipt = OperationReceipt::checked(Record {
            version: 1,
            operation_id: operation_id.to_owned(),
            kind,
            endpoint_hash: self.endpoint_hash.clone(),
            collection_hash: details.scene_collection.map(hash),
            scene_uuid: details.scene_uuid.map(str::to_owned),
            stage_id: details.stage_id.map(str::to_owned),
            take_id: details.take_id.map(str::to_owned),
            generation,
            created_at: utc.to_owned(),
            updated_at: utc.to_owned(),
            phase: Phase::Prepared,
            request_dispatched: Some(false),
            request_accepted: None,
            postcondition_observed: false,
            rendered_composition: "unverified".to_owned(),
        })?;
        self.persist(&receipt, false)?;
        Ok(receipt)
    }

    /// A stale receipt is a conflict. A post-publication error preserves the
    /// visible file and never rewinds the old state or authorizes replay.
    pub fn update(
        &mut self,
        receipt: &OperationReceipt,
        update: OperationUpdate,
        utc: &str,
    ) -> Result<OperationReceipt, StoreError> {
        if !valid_timestamp(utc) {
            return Err(StoreError::InvalidTimestamp);
        }
        self.check_endpoint(receipt)?;
        if self.read(receipt.operation_id())? != *receipt {
            return Err(StoreError::Conflict);
        }
        let mut record = receipt.0.clone();
        record.phase = update.phase;
        record.updated_at = utc.to_owned();
        if let Some(value) = update.request_dispatched {
            record.request_dispatched = value;
        }
        if let Some(value) = update.request_accepted {
            record.request_accepted = value;
        }
        if let Some(value) = update.postcondition_observed {
            record.postcondition_observed = value;
        }
        let receipt = OperationReceipt::checked(record)?;
        self.persist(&receipt, true)?;
        Ok(receipt)
    }

    pub fn prepare_generic(
        &mut self,
        id: &str,
        kind: GenericKind,
        generation: u64,
        utc: &str,
    ) -> Result<GenericReceipt, StoreError> {
        let receipt = GenericReceipt::checked(GenericRecord {
            version: 2,
            operation_id: id.into(),
            kind: "request".into(),
            request_type: kind,
            endpoint_hash: self.endpoint_hash.clone(),
            generation,
            created_at: utc.into(),
            updated_at: utc.into(),
            phase: GenericPhase::Prepared,
            request_dispatched: Some(false),
            request_accepted: None,
            postcondition_observed: false,
            rendered_composition: "unverified".into(),
        })?;
        self.persist_value(id, &receipt.to_value(), false)?;
        Ok(receipt)
    }
    pub fn update_generic(
        &mut self,
        receipt: &GenericReceipt,
        phase: GenericPhase,
        utc: &str,
    ) -> Result<GenericReceipt, StoreError> {
        if receipt.0.endpoint_hash != self.endpoint_hash {
            return Err(StoreError::EndpointMismatch);
        }
        let actual = read_stored(
            &self
                .directory
                .join(format!("{}.json", receipt.operation_id())),
        )?;
        if !matches!(actual,StoredReceipt::Generic(ref actual) if actual==receipt) {
            return Err(StoreError::Conflict);
        }
        if !matches!(
            (receipt.phase(), phase),
            (
                GenericPhase::Prepared,
                GenericPhase::Dispatching | GenericPhase::NotDispatched
            ) | (
                GenericPhase::Dispatching,
                GenericPhase::Accepted
                    | GenericPhase::Rejected
                    | GenericPhase::NotDispatched
                    | GenericPhase::Unknown
            )
        ) {
            return Err(StoreError::Conflict);
        }
        let mut record = receipt.0.clone();
        record.phase = phase;
        record.updated_at = utc.into();
        (record.request_dispatched, record.request_accepted) = match phase {
            GenericPhase::Prepared | GenericPhase::NotDispatched => (Some(false), None),
            GenericPhase::Dispatching | GenericPhase::Unknown => (None, None),
            GenericPhase::Accepted => (Some(true), Some(true)),
            GenericPhase::Rejected => (Some(true), Some(false)),
        };
        let receipt = GenericReceipt::checked(record)?;
        self.persist_value(receipt.operation_id(), &receipt.to_value(), true)?;
        Ok(receipt)
    }
    pub fn read_view(&self) -> OperationView {
        OperationView {
            inner: self.read_namespace.clone(),
        }
    }
    pub fn state_root(&self) -> &Path {
        &self.root
    }
    pub fn read(&self, operation_id: &str) -> Result<OperationReceipt, StoreError> {
        self.read_view().read(operation_id)
    }
    fn check_endpoint(&self, receipt: &OperationReceipt) -> Result<(), StoreError> {
        if receipt.0.endpoint_hash == self.endpoint_hash {
            Ok(())
        } else {
            Err(StoreError::EndpointMismatch)
        }
    }

    fn validate_layout(&self) -> Result<bool, StoreError> {
        validate_chain(&self.directory, true)?;
        let root = directory_metadata(&self.root)?;
        let directory = directory_metadata(&self.directory)?;
        for (expected, current) in [
            (&self.root_identity, &root),
            (&self.directory_identity, &directory),
        ] {
            if let Some(expected) = expected
                && current
                    .as_ref()
                    .is_none_or(|current| !same_identity(expected, current))
            {
                return Err(StoreError::UnsafePath);
            }
        }
        Ok(directory.is_some())
    }

    fn ensure_layout(&mut self) -> Result<(), StoreError> {
        self.validate_layout()?;
        let mut builder = DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&self.directory)
            .map_err(|_| StoreError::Unavailable)?;
        validate_chain(&self.directory, false)?;
        self.root_identity = directory_metadata(&self.root)?;
        self.directory_identity = directory_metadata(&self.directory)?;
        *self
            .read_namespace
            .identities
            .lock()
            .map_err(|_| StoreError::Unavailable)? = LayoutIdentity {
            root: self.root_identity.clone(),
            directory: self.directory_identity.clone(),
        };
        Ok(())
    }

    fn persist(&mut self, receipt: &OperationReceipt, replacing: bool) -> Result<(), StoreError> {
        self.check_endpoint(receipt)?;
        self.persist_value(receipt.operation_id(), &receipt.to_value(), replacing)
    }
    fn persist_value(
        &mut self,
        id: &str,
        value: &Value,
        replacing: bool,
    ) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec(value).map_err(|_| StoreError::InvalidReceipt)?;
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(StoreError::ReceiptTooLarge);
        }
        self.ensure_layout()?;
        let destination = self.directory.join(format!("{id}.json"));
        match fs::symlink_metadata(&destination) {
            Ok(metadata) => {
                validate_regular(&metadata)?;
                if !replacing {
                    return Err(StoreError::Conflict);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if replacing {
                    return Err(StoreError::Missing);
                }
            }
            Err(_) => return Err(StoreError::Unavailable),
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(StoreError::IdentityExhausted)?;
        let temporary = self.directory.join(format!(
            "{}.{}.{}.tmp",
            id,
            std::process::id(),
            self.sequence
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&temporary)
            .map_err(|_| StoreError::Unavailable)?;
        let identity = file.metadata().map_err(|_| StoreError::Unavailable)?;
        let mut staged = StagedFile {
            path: temporary,
            file: Some(file),
            identity,
            published: false,
        };
        let file = staged.file.as_mut().ok_or(StoreError::Unavailable)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| StoreError::Unavailable)?;
        self.validate_layout()?;
        drop(staged.file.take());
        fs::rename(&staged.path, &destination).map_err(|_| StoreError::Unavailable)?;
        staged.published = true;
        // Failure from here means publication may already be visible. Do not
        // remove it or pretend a retry is safe.
        self.validate_layout()
            .map_err(|_| StoreError::DurabilityUncertain)?;
        for (directory, expected) in [
            (&self.directory, &self.directory_identity),
            (&self.root, &self.root_identity),
        ] {
            let handle = File::open(directory).map_err(|_| StoreError::DurabilityUncertain)?;
            let opened = handle
                .metadata()
                .map_err(|_| StoreError::DurabilityUncertain)?;
            if expected
                .as_ref()
                .is_none_or(|expected| !same_identity(expected, &opened))
            {
                return Err(StoreError::DurabilityUncertain);
            }
            handle
                .sync_all()
                .map_err(|_| StoreError::DurabilityUncertain)?;
        }
        self.validate_layout()
            .map_err(|_| StoreError::DurabilityUncertain)?;
        Ok(())
    }
}

struct StagedFile {
    path: PathBuf,
    file: Option<File>,
    identity: Metadata,
    published: bool,
}
impl Drop for StagedFile {
    fn drop(&mut self) {
        drop(self.file.take());
        if !self.published
            && fs::symlink_metadata(&self.path)
                .is_ok_and(|current| same_identity(&self.identity, &current))
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn read_bytes(path: &Path) -> Result<Vec<u8>, StoreError> {
    let before = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            StoreError::Missing
        } else {
            StoreError::Unavailable
        }
    })?;
    validate_regular(&before)?;
    if before.len() > MAX_RECEIPT_BYTES as u64 {
        return Err(StoreError::ReceiptTooLarge);
    }
    let file = File::open(path).map_err(|_| StoreError::Unavailable)?;
    let opened = file.metadata().map_err(|_| StoreError::Unavailable)?;
    validate_regular(&opened)?;
    if !same_identity(&before, &opened) {
        return Err(StoreError::UnsafePath);
    }
    let mut bytes = Vec::with_capacity((opened.len() as usize).min(MAX_RECEIPT_BYTES));
    file.take(MAX_RECEIPT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| StoreError::Unavailable)?;
    if bytes.len() > MAX_RECEIPT_BYTES {
        return Err(StoreError::ReceiptTooLarge);
    }
    let after = fs::symlink_metadata(path).map_err(|_| StoreError::Unavailable)?;
    validate_regular(&after)?;
    if !same_identity(&opened, &after)
        || opened.len() != after.len()
        || opened.modified().ok() != after.modified().ok()
    {
        return Err(StoreError::Conflict);
    }
    Ok(bytes)
}

fn directory_metadata(path: &Path) -> Result<Option<Metadata>, StoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(StoreError::UnsafePath);
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o022 != 0 {
                    return Err(StoreError::UnsafePath);
                }
            }
            Ok(Some(metadata))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(StoreError::Unavailable),
    }
}

fn validate_chain(path: &Path, missing_allowed: bool) -> Result<(), StoreError> {
    if !path.is_absolute() {
        return Err(StoreError::UnsafePath);
    }
    let mut current = PathBuf::new();
    let mut missing = false;
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str())
            }
            _ => return Err(StoreError::UnsafePath),
        }
        if missing {
            continue;
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(StoreError::UnsafePath),
            Err(error) if missing_allowed && error.kind() == io::ErrorKind::NotFound => {
                missing = true
            }
            Err(_) => return Err(StoreError::Unavailable),
        }
    }
    Ok(())
}

fn validate_regular(metadata: &Metadata) -> Result<(), StoreError> {
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(StoreError::UnsafePath);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 || metadata.mode() & 0o022 != 0 {
            return Err(StoreError::UnsafePath);
        }
    }
    Ok(())
}

fn same_identity(a: &Metadata, b: &Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        a.dev() == b.dev() && a.ino() == b.ino()
    }
    #[cfg(not(unix))]
    {
        a.is_dir() == b.is_dir()
            && a.is_file() == b.is_file()
            && a.created().ok() == b.created().ok()
    }
}

fn hash(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
fn bounded_identity(value: &str) -> bool {
    (1..=256).contains(&value.encode_utf16().count())
}
fn valid_uuid(value: &str) -> bool {
    if value == "00000000-0000-0000-0000-000000000000"
        || value == "ffffffff-ffff-ffff-ffff-ffffffffffff"
    {
        return true;
    }
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                *byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
        && (b'1'..=b'8').contains(&bytes[14])
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b' | b'A' | b'B')
}
fn valid_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 20 || bytes.last() != Some(&b'Z') {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate().take(19) {
        if let Some(expected) = match index {
            4 | 7 => Some(b'-'),
            10 => Some(b'T'),
            13 | 16 => Some(b':'),
            _ => None,
        } {
            if *byte != expected {
                return false;
            }
        } else if !byte.is_ascii_digit() {
            return false;
        }
    }
    if bytes.len() != 20
        && (bytes[19] != b'.'
            || bytes.len() < 22
            || !bytes[20..bytes.len() - 1].iter().all(u8::is_ascii_digit))
    {
        return false;
    }
    let number = |start: usize, end: usize| {
        bytes[start..end]
            .iter()
            .fold(0u16, |value, byte| value * 10 + (byte - b'0') as u16)
    };
    let year = number(0, 4);
    let month = number(5, 7);
    let day = number(8, 10);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(400) || year.is_multiple_of(4) && !year.is_multiple_of(100) => 29,
        2 => 28,
        _ => 0,
    };
    day >= 1 && day <= days && number(11, 13) <= 23 && number(14, 16) <= 59 && number(17, 19) <= 59
}

fn uuid_collation(a: &str, b: &str) -> std::cmp::Ordering {
    a.to_ascii_lowercase()
        .cmp(&b.to_ascii_lowercase())
        .then_with(|| {
            a.bytes()
                .zip(b.bytes())
                .find_map(|(a, b)| {
                    (a != b).then(|| a.is_ascii_uppercase().cmp(&b.is_ascii_uppercase()))
                })
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}
fn summary_receipt(receipt: &OperationReceipt) -> Value {
    let mut result = receipt.to_value();
    let object = result.as_object_mut().expect("receipt is an object");
    object.remove("endpointHash");
    object.insert(
        "status".to_owned(),
        json!(if receipt.phase().is_uncertain() {
            "unknown"
        } else {
            receipt.phase().name()
        }),
    );
    object.insert(
        "nextCheck".to_owned(),
        json!(if receipt.phase().is_uncertain() {
            PENDING_CHECK
        } else {
            SEPARATE_CHECK
        }),
    );
    result
}

fn read_receipt(path: &Path) -> Result<OperationReceipt, StoreError> {
    OperationReceipt::from_json(&read_bytes(path)?).map_err(|_| StoreError::Corrupt)
}
/// Closed metadata identity; payloads and settings are never journaled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GenericKind {
    #[serde(rename = "BroadcastCustomEvent")]
    BroadcastCustomEvent,
    #[serde(rename = "SetCurrentSceneTransitionSettings")]
    CurrentSceneTransitionSettings,
    #[serde(rename = "CreateSourceFilter")]
    CreateSourceFilter,
    #[serde(rename = "SetSourceFilterSettings")]
    SourceFilterSettings,
    #[serde(rename = "PressInputPropertiesButton")]
    InputPropertiesButton,
    #[serde(rename = "SaveSourceScreenshot")]
    SaveSourceScreenshot,
    #[serde(rename = "CreateInput")]
    CreateInput,
    #[serde(rename = "SetInputSettings")]
    InputSettings,
    #[serde(rename = "SetPersistentData")]
    PersistentData,
    #[serde(rename = "SetCurrentSceneCollection")]
    CurrentSceneCollection,
    #[serde(rename = "CreateSceneCollection")]
    CreateSceneCollection,
    #[serde(rename = "SetCurrentProfile")]
    CurrentProfile,
    #[serde(rename = "CreateProfile")]
    CreateProfile,
    #[serde(rename = "RemoveProfile")]
    RemoveProfile,
    #[serde(rename = "SetStreamServiceSettings")]
    StreamServiceSettings,
    #[serde(rename = "SetRecordDirectory")]
    RecordDirectory,
    #[serde(rename = "SetOutputSettings")]
    OutputSettings,
    #[serde(rename = "SetVideoSettings")]
    VideoSettings,
    #[serde(rename = "SetProfileParameter")]
    ProfileParameter,
    #[serde(rename = "SetMediaInputCursor")]
    MediaInputCursor,
    #[serde(rename = "OffsetMediaInputCursor")]
    MediaInputCursorOffset,
    #[serde(rename = "TriggerMediaInputAction")]
    MediaInputAction,
    #[serde(rename = "SetInputAudioTracks")]
    InputAudioTracks,
    #[serde(rename = "ToggleInputMute")]
    InputMuteToggle,
    #[serde(rename = "SetInputVolume")]
    InputVolume,
    #[serde(rename = "SendStreamCaption")]
    StreamCaption,
    #[serde(rename = "SaveReplayBuffer")]
    SaveReplayBuffer,
    #[serde(rename = "CreateScene")]
    CreateScene,
    #[serde(rename = "RemoveScene")]
    RemoveScene,
    #[serde(rename = "RemoveSceneItem")]
    RemoveSceneItem,
    #[serde(rename = "RemoveSourceFilter")]
    RemoveSourceFilter,
    #[serde(rename = "SetSourceFilterName")]
    SourceFilterName,
    #[serde(rename = "SetSourceFilterIndex")]
    SourceFilterIndex,
    #[serde(rename = "RemoveInput")]
    RemoveInput,
    #[serde(rename = "SetInputName")]
    InputName,
    #[serde(rename = "DuplicateSceneItem")]
    DuplicateSceneItem,
    #[serde(rename = "SetSceneName")]
    SceneName,
    #[serde(rename = "CreateSceneItem")]
    CreateSceneItem,
    #[serde(rename = "SetSceneItemEnabled")]
    SceneItemEnabled,
    #[serde(rename = "SetCurrentProgramScene")]
    CurrentProgramScene,
    #[serde(rename = "SetCurrentPreviewScene")]
    CurrentPreviewScene,
    #[serde(rename = "TriggerStudioModeTransition")]
    TriggerStudioModeTransition,
    #[serde(rename = "SetCurrentSceneTransition")]
    CurrentSceneTransition,
    #[serde(rename = "SetSceneSceneTransitionOverride")]
    SceneSceneTransitionOverride,
    #[serde(rename = "SetTBarPosition")]
    TBarPosition,
    #[serde(rename = "SetCurrentSceneTransitionDuration")]
    CurrentSceneTransitionDuration,
    #[serde(rename = "SetInputMute")]
    InputMute,
    #[serde(rename = "SetInputAudioBalance")]
    InputAudioBalance,
    #[serde(rename = "SetInputAudioSyncOffset")]
    InputAudioSyncOffset,
    #[serde(rename = "SetInputAudioMonitorType")]
    InputAudioMonitorType,
    #[serde(rename = "SetInputDeinterlaceFieldOrder")]
    InputDeinterlaceFieldOrder,
    #[serde(rename = "SetInputDeinterlaceMode")]
    InputDeinterlaceMode,
    #[serde(rename = "SetSceneItemIndex")]
    SceneItemIndex,
    #[serde(rename = "SetSceneItemLocked")]
    SceneItemLocked,
    #[serde(rename = "SetSceneItemBlendMode")]
    SceneItemBlendMode,
    #[serde(rename = "SetSourceFilterEnabled")]
    SourceFilterEnabled,
}
impl GenericKind {
    pub fn request_type(self) -> &'static str {
        match self {
            Self::BroadcastCustomEvent => "BroadcastCustomEvent",
            Self::CurrentSceneTransitionSettings => "SetCurrentSceneTransitionSettings",
            Self::CreateSourceFilter => "CreateSourceFilter",
            Self::SourceFilterSettings => "SetSourceFilterSettings",
            Self::InputPropertiesButton => "PressInputPropertiesButton",
            Self::SaveSourceScreenshot => "SaveSourceScreenshot",
            Self::CreateInput => "CreateInput",
            Self::InputSettings => "SetInputSettings",
            Self::PersistentData => "SetPersistentData",
            Self::CurrentSceneCollection => "SetCurrentSceneCollection",
            Self::CreateSceneCollection => "CreateSceneCollection",
            Self::CurrentProfile => "SetCurrentProfile",
            Self::CreateProfile => "CreateProfile",
            Self::RemoveProfile => "RemoveProfile",
            Self::StreamServiceSettings => "SetStreamServiceSettings",
            Self::RecordDirectory => "SetRecordDirectory",
            Self::OutputSettings => "SetOutputSettings",
            Self::VideoSettings => "SetVideoSettings",
            Self::ProfileParameter => "SetProfileParameter",
            Self::MediaInputCursor => "SetMediaInputCursor",
            Self::MediaInputCursorOffset => "OffsetMediaInputCursor",
            Self::MediaInputAction => "TriggerMediaInputAction",
            Self::InputAudioTracks => "SetInputAudioTracks",
            Self::InputMuteToggle => "ToggleInputMute",
            Self::InputVolume => "SetInputVolume",
            Self::StreamCaption => "SendStreamCaption",
            Self::SaveReplayBuffer => "SaveReplayBuffer",
            Self::CreateScene => "CreateScene",
            Self::RemoveScene => "RemoveScene",
            Self::RemoveSceneItem => "RemoveSceneItem",
            Self::RemoveSourceFilter => "RemoveSourceFilter",
            Self::SourceFilterName => "SetSourceFilterName",
            Self::SourceFilterIndex => "SetSourceFilterIndex",
            Self::RemoveInput => "RemoveInput",
            Self::InputName => "SetInputName",
            Self::DuplicateSceneItem => "DuplicateSceneItem",
            Self::SceneName => "SetSceneName",
            Self::CreateSceneItem => "CreateSceneItem",
            Self::SceneItemEnabled => "SetSceneItemEnabled",
            Self::CurrentProgramScene => "SetCurrentProgramScene",
            Self::CurrentPreviewScene => "SetCurrentPreviewScene",
            Self::TriggerStudioModeTransition => "TriggerStudioModeTransition",
            Self::CurrentSceneTransition => "SetCurrentSceneTransition",
            Self::SceneSceneTransitionOverride => "SetSceneSceneTransitionOverride",
            Self::TBarPosition => "SetTBarPosition",
            Self::CurrentSceneTransitionDuration => "SetCurrentSceneTransitionDuration",
            Self::InputMute => "SetInputMute",
            Self::InputAudioBalance => "SetInputAudioBalance",
            Self::InputAudioSyncOffset => "SetInputAudioSyncOffset",
            Self::InputAudioMonitorType => "SetInputAudioMonitorType",
            Self::InputDeinterlaceFieldOrder => "SetInputDeinterlaceFieldOrder",
            Self::InputDeinterlaceMode => "SetInputDeinterlaceMode",
            Self::SceneItemIndex => "SetSceneItemIndex",
            Self::SceneItemLocked => "SetSceneItemLocked",
            Self::SceneItemBlendMode => "SetSceneItemBlendMode",
            Self::SourceFilterEnabled => "SetSourceFilterEnabled",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GenericPhase {
    Prepared,
    Dispatching,
    Accepted,
    Rejected,
    NotDispatched,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GenericRecord {
    version: u8,
    operation_id: String,
    kind: String,
    request_type: GenericKind,
    endpoint_hash: String,
    generation: u64,
    created_at: String,
    updated_at: String,
    phase: GenericPhase,
    request_dispatched: Option<bool>,
    request_accepted: Option<bool>,
    postcondition_observed: bool,
    rendered_composition: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericReceipt(GenericRecord);
impl GenericReceipt {
    pub fn operation_id(&self) -> &str {
        &self.0.operation_id
    }
    pub fn phase(&self) -> GenericPhase {
        self.0.phase
    }
    pub fn kind(&self) -> GenericKind {
        self.0.request_type
    }
    pub fn to_value(&self) -> Value {
        serde_json::to_value(&self.0).expect("validated metadata")
    }
    fn checked(record: GenericRecord) -> Result<Self, StoreError> {
        let valid_disposition = match record.phase {
            GenericPhase::Prepared | GenericPhase::NotDispatched => {
                record.request_dispatched == Some(false) && record.request_accepted.is_none()
            }
            GenericPhase::Dispatching | GenericPhase::Unknown => {
                record.request_dispatched.is_none() && record.request_accepted.is_none()
            }
            GenericPhase::Accepted => {
                record.request_dispatched == Some(true) && record.request_accepted == Some(true)
            }
            GenericPhase::Rejected => {
                record.request_dispatched == Some(true) && record.request_accepted == Some(false)
            }
        };
        if record.version != 2
            || record.kind != "request"
            || !valid_uuid(&record.operation_id)
            || !valid_hash(&record.endpoint_hash)
            || record.generation > MAX_SAFE_INTEGER
            || !valid_timestamp(&record.created_at)
            || !valid_timestamp(&record.updated_at)
            || record.updated_at < record.created_at
            || !valid_disposition
            || record.postcondition_observed
            || record.rendered_composition != "unverified"
        {
            return Err(StoreError::InvalidReceipt);
        }
        Ok(Self(record))
    }
    fn from_value(value: Value) -> Result<Self, StoreError> {
        // Option fields must be present, even when null.
        if ["requestDispatched", "requestAccepted"]
            .iter()
            .any(|key| value.get(key).is_none())
        {
            return Err(StoreError::InvalidReceipt);
        }
        Self::checked(serde_json::from_value(value).map_err(|_| StoreError::InvalidReceipt)?)
    }
    fn blocking(&self) -> bool {
        matches!(
            self.phase(),
            GenericPhase::Dispatching | GenericPhase::Unknown
        )
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Admission {
    pub blocking: bool,
    pub generic_v2_present: bool,
    pub total: usize,
}
enum StoredReceipt {
    V1(OperationReceipt),
    Generic(GenericReceipt),
}
impl StoredReceipt {
    fn id(&self) -> &str {
        match self {
            Self::V1(r) => r.operation_id(),
            Self::Generic(r) => r.operation_id(),
        }
    }
    fn endpoint(&self) -> &str {
        match self {
            Self::V1(r) => &r.0.endpoint_hash,
            Self::Generic(r) => &r.0.endpoint_hash,
        }
    }
    fn created(&self) -> &str {
        match self {
            Self::V1(r) => &r.0.created_at,
            Self::Generic(r) => &r.0.created_at,
        }
    }
    fn blocking(&self) -> bool {
        match self {
            Self::V1(r) => r.phase().is_uncertain(),
            Self::Generic(r) => r.blocking(),
        }
    }
    fn summary(&self) -> Value {
        match self {
            Self::V1(r) => summary_receipt(r),
            Self::Generic(r) => {
                let mut value = r.to_value();
                value.as_object_mut().unwrap().remove("endpointHash");
                value["status"] = json!(if r.blocking() {
                    "unknown"
                } else {
                    match r.phase() {
                        GenericPhase::Prepared => "prepared",
                        GenericPhase::Accepted => "accepted",
                        GenericPhase::Rejected => "rejected",
                        GenericPhase::NotDispatched => "not-dispatched",
                        _ => unreachable!(),
                    }
                });
                value["nextCheck"] = json!(
                    "Acceptance is not observation or replay authority; inspect current OBS state independently"
                );
                value
            }
        }
    }
}
fn read_stored(path: &Path) -> Result<StoredReceipt, StoreError> {
    let value: Value =
        serde_json::from_slice(&read_bytes(path)?).map_err(|_| StoreError::Corrupt)?;
    if value["version"] == 2 {
        GenericReceipt::from_value(value)
            .map(StoredReceipt::Generic)
            .map_err(|_| StoreError::Corrupt)
    } else {
        OperationReceipt::from_value(value)
            .map(StoredReceipt::V1)
            .map_err(|_| StoreError::Corrupt)
    }
}

/// One direct backend, or a nonclone scoped port into the service's unique
/// backend. Scoped stores never construct a second filesystem writer.
pub struct OperationStore {
    pub(crate) mode: StoreMode,
}
pub(crate) enum StoreMode {
    Direct(Box<BackendStore>),
    Scoped(crate::receipt_broker::ReceiptPort),
}
impl OperationStore {
    pub fn dispatch_fence(&self) -> Option<crate::receipt_broker::DispatchFence> {
        match &self.mode {
            StoreMode::Direct(_) => None,
            StoreMode::Scoped(port) => Some(port.fence()),
        }
    }
    pub fn new(root: impl AsRef<Path>, endpoint: &str) -> Result<Self, StoreError> {
        BackendStore::new(root, endpoint).map(|store| Self {
            mode: StoreMode::Direct(Box::new(store)),
        })
    }
    pub fn endpoint_hash(&self) -> &str {
        match &self.mode {
            StoreMode::Direct(s) => s.endpoint_hash(),
            StoreMode::Scoped(p) => p.endpoint_hash(),
        }
    }
    pub fn state_root(&self) -> &Path {
        match &self.mode {
            StoreMode::Direct(s) => s.state_root(),
            StoreMode::Scoped(p) => p.state_root(),
        }
    }
    pub fn read_view(&self) -> OperationView {
        match &self.mode {
            StoreMode::Direct(s) => s.read_view(),
            StoreMode::Scoped(p) => p.read_view(),
        }
    }
    pub fn read(&self, id: &str) -> Result<OperationReceipt, StoreError> {
        self.read_view().read(id)
    }
    pub fn try_summary(&self) -> Result<ObservedSummary, StoreError> {
        self.read_view().try_summary()
    }
    pub fn summary(&self) -> Value {
        self.read_view().summary()
    }
    pub fn prepare(
        &mut self,
        id: &str,
        kind: OperationKind,
        generation: u64,
        utc: &str,
        details: PrepareDetails<'_>,
    ) -> Result<OperationReceipt, StoreError> {
        match &mut self.mode {
            StoreMode::Direct(s) => s.prepare(id, kind, generation, utc, details),
            StoreMode::Scoped(p) => p.prepare(id, kind, generation, utc, details),
        }
    }
    pub fn update(
        &mut self,
        r: &OperationReceipt,
        u: OperationUpdate,
        utc: &str,
    ) -> Result<OperationReceipt, StoreError> {
        match &mut self.mode {
            StoreMode::Direct(s) => s.update(r, u, utc),
            StoreMode::Scoped(p) => p.update(r, u, utc),
        }
    }
    pub fn prepare_generic(
        &mut self,
        id: &str,
        kind: GenericKind,
        generation: u64,
        utc: &str,
    ) -> Result<GenericReceipt, StoreError> {
        match &mut self.mode {
            StoreMode::Direct(s) => s.prepare_generic(id, kind, generation, utc),
            StoreMode::Scoped(p) => p.prepare_generic(id, kind, generation, utc),
        }
    }
    pub fn update_generic(
        &mut self,
        r: &GenericReceipt,
        phase: GenericPhase,
        utc: &str,
    ) -> Result<GenericReceipt, StoreError> {
        match &mut self.mode {
            StoreMode::Direct(s) => s.update_generic(r, phase, utc),
            StoreMode::Scoped(p) => p.update_generic(r, phase, utc),
        }
    }
}
