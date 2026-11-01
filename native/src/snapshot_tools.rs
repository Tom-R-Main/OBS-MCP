// SPDX-License-Identifier: GPL-2.0-only
//! Manual snapshot reads on the service's existing physical connection. This
//! module neither owns a socket nor publishes files or OBS mutations. Legacy
//! snapshots are ordered observations, not an atomic or field-owned receipt.
use crate::{
    observation::{ObservationError, ReadAdapter, ReadFuture, ReadRequest},
    session::{ALL_EVENTS, BatchOptions, BatchRequest, RequestError, SessionHandle},
    session_observation::{ObservationFence, SessionReader},
    snapshots::{
        self, BatchReadResult, CaptureMetadata, CaptureOptions, ReadCall, Snapshot, SnapshotError,
        SnapshotFile, SnapshotFuture, SnapshotReader,
    },
    stage_transport::Cancellation,
};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, sync::Arc};
use tokio::time::Instant;

pub struct Request {
    pub list: bool,
    options: CaptureOptions,
}
impl std::fmt::Debug for Request {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("SnapshotRequest")
            .field("list", &self.list)
            .finish_non_exhaustive()
    }
}
impl Request {
    pub fn parse(args: &Value) -> Result<Self, ObservationError> {
        let invalid = || ObservationError::new("invalid_arguments", "Invalid snapshot arguments");
        let object = args.as_object().ok_or_else(invalid)?;
        let strings = |key: &str| -> Result<Option<Vec<String>>, ObservationError> {
            object
                .get(key)
                .map(|value| {
                    value
                        .as_array()
                        .ok_or_else(invalid)?
                        .iter()
                        .map(|value| value.as_str().map(str::to_owned).ok_or_else(invalid))
                        .collect()
                })
                .transpose()
        };
        Ok(Self {
            list: object
                .get("list")
                .map(|value| value.as_bool().ok_or_else(invalid))
                .transpose()?
                .unwrap_or(false),
            options: CaptureOptions {
                scenes: strings("scenes")?,
                inputs: strings("inputs")?,
                label: object
                    .get("label")
                    .map(|value| value.as_str().map(str::to_owned).ok_or_else(invalid))
                    .transpose()?,
                ..Default::default()
            },
        })
    }
}
pub fn failure(message: &str) -> Value {
    json!({"content":[{"type":"text","text":format!("Snapshot failed: {message}")}],"isError":true})
}
fn describe(snapshot: &Snapshot) -> String {
    let mut scenes = Vec::new();
    for item in &snapshot.items {
        if !scenes.contains(&item.scene_name) {
            scenes.push(item.scene_name.clone());
        }
    }
    format!(
        "{}{} at {}: {} input(s), {} item(s) in {}",
        snapshot.id,
        snapshot
            .label
            .as_ref()
            .filter(|label| !label.is_empty())
            .map(|label| format!(" \"{label}\""))
            .unwrap_or_default(),
        snapshot.taken_at,
        snapshot.inputs.len(),
        snapshot.items.len(),
        if scenes.is_empty() {
            "no scenes".into()
        } else {
            scenes.join(", ")
        }
    )
}
pub fn saved(snapshot: &Snapshot) -> Value {
    json!({"content":[{"type":"text","text":format!("Saved snapshot {}. Undo later changes with obs-restore",describe(snapshot))}],
        "structuredContent":{"snapshotId":snapshot.id,"takenAt":snapshot.taken_at,"inputs":snapshot.inputs.iter().map(|input|&input.input_name).collect::<Vec<_>>(),"items":snapshot.items.len()}})
}
/// Uses the sole history owner even offline. Corrupt/replaced history is an
/// explicit refusal; it is never renamed away or silently treated as empty.
pub fn list(writer: &mut SnapshotFile, endpoint: &str) -> Value {
    match writer.for_endpoint(endpoint) {
        Err(error) => failure(error.tool_message()),
        Ok(saved) => {
            let rows = saved
                .iter()
                .map(|snapshot| {
                    let mut row = Map::from_iter([
                        ("id".into(), json!(snapshot.id)),
                        ("takenAt".into(), json!(snapshot.taken_at)),
                        ("inputs".into(), json!(snapshot.inputs.len())),
                        ("items".into(), json!(snapshot.items.len())),
                    ]);
                    if let Some(label) = &snapshot.label {
                        row.insert("label".into(), json!(label));
                    }
                    Value::Object(row)
                })
                .collect::<Vec<_>>();
            json!({"content":[{"type":"text","text":if saved.is_empty(){"No snapshots saved".into()}else{saved.iter().map(describe).collect::<Vec<_>>().join("\n")}}],"structuredContent":{"snapshots":rows}})
        }
    }
}
pub struct Context {
    pub handle: SessionHandle,
    pub reader: SessionReader,
    pub fence: ObservationFence,
    pub endpoint: String,
    pub capabilities: Option<Arc<HashSet<String>>>,
    pub cancellation: Cancellation,
    pub deadline: Instant,
}
fn invalidated() -> ObservationError {
    ObservationError::new(
        "snapshot_invalidated",
        "Snapshot session evidence is unavailable",
    )
}
fn remaining(deadline: Instant) -> Result<std::time::Duration, ObservationError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|value| !value.is_zero())
        .ok_or_else(|| ObservationError::new("snapshot_deadline", "Snapshot deadline elapsed"))
}
fn observed(error: RequestError, name: &str) -> ObservationError {
    if let Some(code) = error.rejection_code() {
        ObservationError::rejected(
            code,
            "snapshot_read",
            error.obs_failure_message(name).expect("typed rejection"),
        )
    } else {
        invalidated()
    }
}
struct Reader(Context);
impl Reader {
    fn check(&self) -> Result<(), ObservationError> {
        remaining(self.0.deadline)?;
        let state = self.0.reader.evidence();
        if self.0.cancellation.is_cancelled() {
            return Err(ObservationError::new(
                "snapshot_cancelled",
                "Snapshot request cancelled",
            ));
        }
        if self.0.handle.invalidation().is_some()
            || !state.connected
            || state.cancelled
            || state.generation != self.0.handle.token().generation()
        {
            return Err(invalidated());
        }
        Ok(())
    }
    fn capability(&self, request: ReadRequest) -> Result<(), ObservationError> {
        if self.supports(request) == Some(false) {
            Err(ObservationError::new(
                "capability_unavailable",
                format!(
                    "OBS WebSocket does not advertise support for request '{}'",
                    request.as_str()
                ),
            ))
        } else {
            Ok(())
        }
    }
    fn guard(&self) -> impl FnOnce() -> Result<(), String> + Send + 'static {
        let fence = self.0.fence.clone();
        let state = self.0.reader.evidence();
        let cancel = self.0.cancellation.clone();
        let deadline = self.0.deadline;
        move || {
            if cancel.is_cancelled()
                || Instant::now() >= deadline
                || !fence.ready_at_revision(state.generation, state.revision)
            {
                Err("Snapshot read permission expired".into())
            } else {
                Ok(())
            }
        }
    }
}
impl ReadAdapter for Reader {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.0
            .capabilities
            .as_ref()
            .map(|set| set.contains(request.as_str()))
            .or_else(|| self.0.reader.supports(request))
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        deadline: Instant,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            self.check()?;
            self.capability(request)?;
            let deadline = deadline.min(self.0.deadline);
            self.0.reader.synchronize(deadline).await?;
            let result = self
                .0
                .handle
                .request_checked(
                    self.0.handle.token(),
                    request.as_str(),
                    data,
                    remaining(deadline)?,
                    self.guard(),
                )
                .await;
            self.0.reader.synchronize(deadline).await?;
            self.check()?;
            result.map_err(|error| observed(error, request.as_str()))
        })
    }
}
impl SnapshotReader for Reader {
    fn batch<'a>(&'a mut self, calls: Vec<ReadCall>) -> SnapshotFuture<'a, Vec<BatchReadResult>> {
        Box::pin(async move {
            self.check()?;
            if calls.len() > 4096 {
                return Err(SnapshotError::Capacity);
            }
            if calls.is_empty() {
                return Ok(vec![]);
            }
            for call in &calls {
                self.capability(call.request)?;
            }
            self.0.reader.synchronize(self.0.deadline).await?;
            let result = self
                .0
                .handle
                .batch_checked(
                    self.0.handle.token(),
                    calls
                        .into_iter()
                        .map(|call| BatchRequest {
                            request_type: call.request.as_str().into(),
                            request_data: Some(call.data),
                        })
                        .collect(),
                    BatchOptions {
                        timeout: Some(remaining(self.0.deadline)?),
                        ..Default::default()
                    },
                    self.guard(),
                )
                .await;
            self.0.reader.synchronize(self.0.deadline).await?;
            self.check()?;
            let rows = result.map_err(|error| observed(error, "RequestBatch"))?;
            Ok(rows
                .into_iter()
                .map(|row| BatchReadResult {
                    ok: row.ok,
                    code: Some(row.code),
                    comment: row.comment,
                    response_data: row.response_data,
                })
                .collect())
        })
    }
}
/// The retained service job owns this future until all admitted RPCs settle.
/// Publication happens afterwards through its existing SnapshotFile owner.
pub async fn capture(
    context: Context,
    request: Request,
    metadata: CaptureMetadata,
) -> Result<Snapshot, SnapshotError> {
    if metadata.obs_url != context.endpoint {
        return Err(invalidated().into());
    }
    capture_with_metadata(context, request, || metadata).await
}

/// The host factory is deliberately invoked at the same phase as Source.
/// The service retains its ID/clock owners; the callback gains no file writer.
pub async fn capture_with_metadata<F: FnOnce() -> CaptureMetadata + Send>(
    context: Context,
    request: Request,
    metadata: F,
) -> Result<Snapshot, SnapshotError> {
    let hash = format!("{:x}", Sha256::digest(context.endpoint.as_bytes()));
    if request.list
        || context.handle.token() != context.reader.token()
        || context.handle.token() != context.fence.token()
        || hash != context.handle.endpoint_hash()
        || hash != context.reader.endpoint_hash()
        || hash != context.fence.endpoint_hash()
        || !context.fence.full_profile()
        || context.handle.event_subscriptions() & ALL_EVENTS != ALL_EVENTS
    {
        return Err(invalidated().into());
    }
    let deadline = context.deadline;
    let mut reader = Reader(context);
    reader.check()?;
    let snapshot =
        snapshots::capture_snapshot_with_metadata(&mut reader, request.options, metadata, deadline)
            .await?;
    if snapshot.obs_url.as_deref() != Some(reader.0.endpoint.as_str()) {
        return Err(invalidated().into());
    }
    reader.check()?;
    reader.0.reader.synchronize(deadline).await?;
    reader.check()?;
    Ok(snapshot)
}
