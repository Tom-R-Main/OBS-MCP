// SPDX-License-Identifier: GPL-2.0-only
//! Source-shaped manual Restore workflow. Its legacy name/item-ID plan is not
//! guarded recovery, recording ownership, a rendered result or replay authority.
use crate::{
    control::{Disposition, GateRequest},
    custody::{Clock, SharedControl},
    observation::ObservationError,
    operation_store::OperationStore,
    restore_transport::{self, RestoreBatchOwner},
    session::{BatchResult, RequestError, SessionHandle},
    snapshot_tools,
    snapshots::{RestoreChange, RestorePlan, Snapshot, SnapshotFile},
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::Instant;
#[derive(Clone)]
pub struct Request {
    pub snapshot_id: Option<String>,
    pub dry_run: bool,
}
impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestoreRequest")
            .field("dry_run", &self.dry_run)
            .finish_non_exhaustive()
    }
}
impl Request {
    pub fn parse(args: &Value) -> Result<Self, ObservationError> {
        let invalid =
            || ObservationError::new("invalid_restore_arguments", "Invalid restore arguments");
        let object = args.as_object().ok_or_else(invalid)?;
        let snapshot_id = object
            .get("snapshotId")
            .map(|v| v.as_str().map(str::to_owned).ok_or_else(invalid))
            .transpose()?;
        let dry_run = object
            .get("dryRun")
            .map(|v| v.as_bool().ok_or_else(invalid))
            .transpose()?
            .unwrap_or(false);
        Ok(Self {
            snapshot_id,
            dry_run,
        })
    }
    pub fn arguments(&self) -> Value {
        let mut args = json!({"dryRun":self.dry_run});
        if let Some(id) = &self.snapshot_id {
            args["snapshotId"] = json!(id);
        }
        args
    }
}
pub(crate) fn policy(
    request: &Request,
    caller: &str,
    control: &SharedControl,
    clock: &Clock,
) -> Result<Disposition, ()> {
    Ok(control.lock().map_err(|_| ())?.evaluate(
        GateRequest {
            tool_name: "obs-restore",
            arguments: &request.arguments(),
            read_only_hint: false,
            caller,
            confirmation: Default::default(),
        },
        clock().epoch_ms,
    ))
}
pub(crate) fn failure(message: &str) -> Value {
    json!({"content":[{"type":"text","text":format!("Restore failed: {message}")}],"isError":true})
}
/// Only called after Source policy, on the SAME retained file owner.
pub(crate) fn select(
    writer: &mut SnapshotFile,
    endpoint: &str,
    request: &Request,
) -> Result<Snapshot, Value> {
    let mut saved = writer
        .for_endpoint(endpoint)
        .map_err(|e| failure(e.tool_message()))?;
    let explicit = request.snapshot_id.as_deref().filter(|id| !id.is_empty());
    let selected = match explicit {
        Some(id) => saved.into_iter().find(|row| row.id == id),
        None => saved.pop(),
    };
    selected.ok_or_else(||json!({"content":[{"type":"text","text":explicit.map(|id|format!("No snapshot {id}; list them with obs-snapshot list: true")).unwrap_or_else(||"No snapshots saved; take one with obs-snapshot".into())}],"isError":true}))
}
pub(crate) struct ApplyPlan {
    snapshot_id: String,
    changes: Vec<RestoreChange>,
    missing: Vec<String>,
}
pub(crate) enum Planned {
    Finished(Value),
    Apply(ApplyPlan),
}
fn listed(changes: &[(String, String)]) -> Value {
    json!(
        changes
            .iter()
            .map(|(target, field)| json!({"target":target,"field":field}))
            .collect::<Vec<_>>()
    )
}
fn lines(changes: &[(String, String)]) -> Vec<String> {
    changes
        .iter()
        .map(|(target, field)| format!("- {target}: {field}"))
        .collect()
}
fn notes(missing: &[String]) -> Vec<String> {
    missing
        .iter()
        .map(|note| format!("Not restored: {note}"))
        .collect()
}
pub(crate) async fn plan(
    context: snapshot_tools::Context,
    snapshot: Snapshot,
    request: &Request,
) -> Planned {
    let RestorePlan { changes, missing } =
        match snapshot_tools::restore_plan(context, &snapshot).await {
            Ok(plan) => plan,
            Err(error) => return Planned::Finished(failure(error.tool_message())),
        };
    if changes.is_empty() {
        let mut text = vec![format!("OBS already matches snapshot {}", snapshot.id)];
        text.extend(notes(&missing));
        return Planned::Finished(
            json!({"content":[{"type":"text","text":text.join("\n")}],"structuredContent":{"snapshotId":snapshot.id,"changes":[],"missing":missing,"applied":false}}),
        );
    }
    if request.dry_run {
        let description = changes
            .iter()
            .map(|c| (c.target.clone(), c.field.clone()))
            .collect::<Vec<_>>();
        let mut text = vec![format!("Restoring snapshot {} would change:", snapshot.id)];
        text.extend(lines(&description));
        text.extend(notes(&missing));
        return Planned::Finished(
            json!({"content":[{"type":"text","text":text.join("\n")}],"structuredContent":{"snapshotId":snapshot.id,"changes":listed(&description),"missing":missing,"applied":false}}),
        );
    }
    Planned::Apply(ApplyPlan {
        snapshot_id: snapshot.id,
        changes,
        missing,
    })
}
pub(crate) struct Execution {
    pub store: Option<OperationStore>,
    pub result: Value,
    pub broken: bool,
}
pub(crate) struct ApplyContext {
    pub session: SessionHandle,
    pub control: SharedControl,
    pub clock: Clock,
    pub cancellation: Cancellation,
    pub admitted_at: Instant,
    pub timeout: Duration,
    pub operation_id: String,
    pub caller: String,
    pub request: Request,
    pub capabilities: Option<restore_transport::Capabilities>,
}
fn acknowledged(
    id: &str,
    description: &[(String, String)],
    missing: &[String],
    rows: &[BatchResult],
) -> Value {
    let failures = description
        .iter()
        .zip(rows)
        .filter(|(_, row)| !row.ok)
        .map(|((target, field), row)| {
            format!(
                "{target}: {field} failed{}",
                row.comment
                    .as_deref()
                    .filter(|c| !c.is_empty())
                    .map(|c| format!(": {c}"))
                    .unwrap_or_default()
            )
        })
        .collect::<Vec<_>>();
    let mut text = vec![format!(
        "Restored snapshot {id}: {} of {} change(s)",
        description.len() - failures.len(),
        description.len()
    )];
    text.extend(lines(description));
    text.extend(failures.iter().map(|f| format!("Failed: {f}")));
    text.extend(notes(missing));
    let mut result = json!({"content":[{"type":"text","text":text.join("\n")}],"structuredContent":{"snapshotId":id,"changes":listed(description),"missing":missing,"failures":failures,"applied":true}});
    if !failures.is_empty() {
        result["isError"] = json!(true);
    }
    result
}
fn begin_failure(error: restore_transport::BeginError) -> Value {
    use restore_transport::BeginError as E;
    match error {
        E::Policy(policy) => policy
            .tool_result()
            .unwrap_or_else(|| failure("Control policy refused Restore")),
        E::Receipt(_) => failure(
            "Operation receipt could not be persisted; no OBS request was dispatched; inspect obs-observe before another change",
        ),
        E::Unsupported(name) => failure(&format!(
            "OBS WebSocket does not advertise support for request '{name}'"
        )),
        E::Cancelled | E::Deadline => {
            failure("Restore cancelled or original deadline expired before dispatch")
        }
        _ => failure(
            "Restore custody is unavailable; no OBS request was dispatched; inspect obs-observe before another change",
        ),
    }
}
/// The Service owns this exact future. Never select-drop an admitted batch.
pub(crate) async fn apply(
    store: OperationStore,
    plan: ApplyPlan,
    context: ApplyContext,
) -> Execution {
    let mut owner =
        match RestoreBatchOwner::new(store, context.session, context.control, context.clock) {
            Ok(owner) => owner,
            Err(error) => {
                return Execution {
                    store: None,
                    result: begin_failure(error),
                    broken: true,
                };
            }
        };
    let mut description = Vec::with_capacity(plan.changes.len());
    let mut mutations = Vec::with_capacity(plan.changes.len());
    for change in plan.changes {
        description.push((change.target, change.field));
        mutations.push(change.mutation);
    }
    let request = restore_transport::Request {
        operation_id: context.operation_id,
        mutations,
        policy: restore_transport::Policy {
            caller: context.caller,
            snapshot_id: context.request.snapshot_id,
        },
        capabilities: context.capabilities,
        cancellation: context.cancellation,
        admitted_at: context.admitted_at,
        timeout: context.timeout,
    };
    let (result, broken) = match owner.begin(request) {
        Err(error) => {
            let broken = matches!(
                error,
                restore_transport::BeginError::Receipt(_) | restore_transport::BeginError::Binding
            );
            (begin_failure(error), broken)
        }
        Ok(()) => match owner.finish().await {
            Err(_) => (
                failure(
                    "Restore custody is unavailable; inspect obs-observe before another change",
                ),
                true,
            ),
            Ok(outcome) => {
                let acknowledged_batch = outcome.transport.is_ok();
                let mut result = match outcome.transport {
                    Ok(rows) => acknowledged(&plan.snapshot_id, &description, &plan.missing, &rows),
                    Err(RequestError::BeforeDispatch(_)) => outcome
                        .policy_refusal
                        .and_then(|p| p.tool_result())
                        .unwrap_or_else(|| {
                            failure("Restore cancelled, expired, or refused before dispatch")
                        }),
                    Err(_) => failure(
                        "Restore batch outcome is uncertain; inspect current OBS state and obs-observe before another change; no retry was made",
                    ),
                };
                if outcome.receipt_error.is_some() {
                    result["content"].as_array_mut().expect("tool content").push(json!({"type":"text","text":if acknowledged_batch{"The Restore batch was acknowledged, but its operation receipt could not be settled. Individual results above remain known; inspect obs-observe before another change."}else{"Restore receipt custody is uncertain; inspect obs-observe before another change."}}));
                    result["isError"] = json!(true);
                }
                (result, outcome.receipt_error.is_some())
            }
        },
    };
    // Reclaim refuses by returning the entire pending owner. Drain that same
    // observer if the invariant ever changes; never drop it on transfer failure.
    let mut broken = broken;
    let store = loop {
        match owner.into_store() {
            Ok(store) => break store,
            Err(pending) => {
                owner = *pending;
                broken = true;
                let _ = owner.finish().await;
            }
        }
    };
    Execution {
        store: Some(store),
        result,
        broken,
    }
}
