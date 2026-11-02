// SPDX-License-Identifier: GPL-2.0-only
//! Retained Apply Scene composition. It borrows no new socket, raw read,
//! supervisor, recording/file authority or automatic recovery/replay capability.
use crate::{
    apply_scene::{self as domain, Action, Request, Status},
    apply_scene_transport::{self as transport, ApplySceneOwner, Stage},
    control::{Disposition, GateRequest},
    custody::{Clock, SharedControl},
    operation_store::{ApplyCounts, OperationStore},
    session::RequestError,
    snapshot_tools,
    snapshots::{CaptureMetadata, Snapshot},
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::Instant;
pub(crate) fn policy(
    request: &Request,
    caller: &str,
    control: &SharedControl,
    clock: &Clock,
) -> Result<Disposition, ()> {
    let arguments = request.arguments();
    let now = clock().epoch_ms;
    Ok(control.lock().map_err(|_| ())?.evaluate(
        GateRequest {
            tool_name: "obs-apply-scene",
            arguments: &arguments,
            read_only_hint: false,
            caller,
            confirmation: Default::default(),
        },
        now,
    ))
}
pub(crate) struct Ready {
    creations: Vec<Action>,
    snapshot: Option<String>,
}
pub(crate) enum Planned {
    Finished(Value),
    Ready(Ready),
    Save(Ready, Box<Snapshot>),
}
pub(crate) async fn plan<F: FnOnce() -> CaptureMetadata + Send>(
    context: snapshot_tools::Context,
    request: &Request,
    metadata: F,
) -> Planned {
    let before = match snapshot_tools::apply_current(context.clone(), request).await {
        Ok(current) => current,
        Err(error) => return Planned::Finished(domain::failure(error.tool_message())),
    };
    let first = domain::plan(request, &before);
    if let Some(result) = domain::initial_result(request, &first) {
        return Planned::Finished(result);
    }
    let mut ready = Ready {
        creations: first.creations(),
        snapshot: None,
    };
    if let Some(selection) = domain::snapshot_selection(request, &before) {
        let snapshot = match snapshot_tools::capture_apply(context, selection, metadata).await {
            Ok(snapshot) => snapshot,
            Err(error) => return Planned::Finished(domain::snapshot_failure(error.tool_message())),
        };
        ready.snapshot = Some(snapshot.id.clone());
        Planned::Save(ready, Box::new(snapshot))
    } else {
        Planned::Ready(ready)
    }
}
pub(crate) struct Context {
    pub reads: snapshot_tools::Context,
    pub control: SharedControl,
    pub clock: Clock,
    pub cancellation: Cancellation,
    pub admitted_at: Instant,
    pub timeout: Duration,
    pub operation_id: String,
    pub caller: String,
    pub request: Request,
    pub capabilities: Option<transport::Capabilities>,
}
pub(crate) struct Execution {
    pub store: Option<OperationStore>,
    pub result: Value,
    pub broken: bool,
}
fn statuses(rows: &[crate::session::BatchResult]) -> Vec<Status> {
    rows.iter()
        .map(|row| Status {
            ok: row.ok,
            code: row.code,
            comment: row.comment.clone(),
        })
        .collect()
}
fn begin_failure(error: transport::BeginError, previous: &[(Stage, ApplyCounts)]) -> Value {
    use transport::BeginError as E;
    if let E::Policy(policy) = &error {
        if previous.is_empty() {
            return policy
                .tool_result()
                .unwrap_or_else(|| domain::failure("Control policy refused Apply Scene"));
        }
        return domain::failure(
            "Control policy refused continuation; earlier acknowledged results remain known",
        );
    }
    let message = match error {
        E::Unsupported(name) => {
            format!("OBS WebSocket does not advertise support for request '{name}'")
        }
        E::Cancelled | E::Deadline => {
            "Apply Scene cancelled or original deadline expired before the next batch".into()
        }
        E::Receipt(_) => {
            "Apply Scene receipt could not be persisted; inspect obs-observe before another change"
                .into()
        }
        _ => "Apply Scene custody is unavailable; inspect obs-observe before another change".into(),
    };
    domain::failure(&message)
}
fn with_evidence(mut result: Value, ready: &Ready, facts: &[(Stage, ApplyCounts)]) -> Value {
    let mut evidence = json!({"postconditionObserved":false,"renderedComposition":"unverified"});
    if let Some(id) = &ready.snapshot {
        evidence["snapshotId"] = json!(id);
    }
    if !facts.is_empty() {
        evidence["knownBatches"]=json!(facts.iter().map(|(stage,c)|json!({"stage":if *stage==Stage::Creations{"creations"}else{"changes"},"batchAcknowledged":true,"successful":c.successful,"failed":c.failed,"skipped":c.skipped})).collect::<Vec<_>>());
    }
    if !result["structuredContent"].is_object() {
        result["structuredContent"] = json!({});
    }
    result["structuredContent"]["errorEvidence"] = evidence;
    result
}
pub(crate) fn refusal(ready: &Ready, message: &str) -> Value {
    with_evidence(domain::failure(message), ready, &[])
}
fn cancelled(context: &Context) -> bool {
    context.cancellation.is_cancelled() || context.admitted_at.elapsed() >= context.timeout
}
fn abort(owner: &mut ApplySceneOwner, broken: &mut bool) {
    if owner.has_pending() {
        return;
    }
    if matches!(
        owner.receipt().map(|r| r.phase()),
        Some(
            crate::operation_store::ApplyPhase::Prepared
                | crate::operation_store::ApplyPhase::Continuation
        )
    ) && owner.abort().is_err()
    {
        *broken = true;
    }
}
/// The Service owns this exact future and observer until every admitted RPC and
/// blocking publication settles. A dropped frontend await is not cancellation.
pub(crate) async fn apply(store: OperationStore, ready: Ready, context: Context) -> Execution {
    let mut owner = match ApplySceneOwner::new(
        store,
        context.reads.handle.clone(),
        context.control.clone(),
        context.clock.clone(),
    ) {
        Ok(owner) => owner,
        Err(error) => {
            return Execution {
                store: None,
                result: with_evidence(begin_failure(error, &[]), &ready, &[]),
                broken: true,
            };
        }
    };
    let mut broken = false;
    let mut facts = Vec::new();
    let mut result = run(&mut owner, &ready, &context, &mut facts, &mut broken).await;
    // No unbounded finish-only loop: known continuation has no observer. It
    // must explicitly abort before transfer, while admitted work must settle.
    abort(&mut owner, &mut broken);
    let mut receipt_uncertain = owner.is_poisoned();
    let store = loop {
        match owner.into_store() {
            Ok(store) => break store,
            Err(returned) => {
                owner = *returned;
                broken = true;
                if owner.has_pending()
                    && let Ok(outcome) = owner.finish().await
                    && let Some(counts) = outcome.acknowledged
                {
                    facts.push((outcome.stage, counts));
                }
                abort(&mut owner, &mut broken);
                receipt_uncertain |= owner.is_poisoned();
            }
        }
    };
    if receipt_uncertain {
        result = with_evidence(result, &ready, &facts);
        result["structuredContent"]["errorEvidence"]["receiptUncertain"] = json!(true);
        result["isError"] = json!(true);
    }
    broken |= context.reads.handle.invalidation().is_some();
    Execution {
        store: Some(store),
        result,
        broken,
    }
}
async fn run(
    owner: &mut ApplySceneOwner,
    ready: &Ready,
    context: &Context,
    facts: &mut Vec<(Stage, ApplyCounts)>,
    broken: &mut bool,
) -> Value {
    let start = transport::Start {
        operation_id: context.operation_id.clone(),
        request: context.request.clone(),
        caller: context.caller.clone(),
        creations: ready
            .creations
            .iter()
            .filter_map(|a| a.effect().cloned())
            .collect(),
        capabilities: context.capabilities.clone(),
        cancellation: context.cancellation.clone(),
        admitted_at: context.admitted_at,
        timeout: context.timeout,
    };
    if let Err(error) = owner.begin(start) {
        *broken |= owner.is_poisoned();
        return with_evidence(begin_failure(error, facts), ready, facts);
    }
    if owner.has_pending() {
        let outcome = owner.finish().await.expect("admitted creation observer");
        if let Some(counts) = outcome.acknowledged {
            facts.push((outcome.stage, counts));
        }
        if outcome.receipt_error.is_some() {
            *broken = true;
            return with_evidence(
                domain::failure(
                    "The creation batch settled, but its operation receipt could not be published; no continuation was attempted",
                ),
                ready,
                facts,
            );
        }
        match outcome.transport {
            Ok(rows)=>if let Some(result)=domain::creation_result(&ready.creations,&statuses(&rows),ready.snapshot.as_deref()){return result;},
            Err(RequestError::BeforeDispatch(_))=>return with_evidence(outcome.policy_refusal.and_then(|p|p.tool_result()).unwrap_or_else(||domain::failure("Apply Scene creation batch was cancelled, expired or refused before dispatch")),ready,facts),
            Err(_)=>{*broken=true;return with_evidence(domain::failure("Creation batch outcome is uncertain; inspect obs-observe and current OBS state; no retry was made"),ready,facts);},
        }
    }
    if cancelled(context) {
        return with_evidence(
            domain::failure(
                "Apply Scene cancelled or original deadline expired before continuation",
            ),
            ready,
            facts,
        );
    }
    let current = match snapshot_tools::apply_current(context.reads.clone(), &context.request).await
    {
        Ok(current) => current,
        Err(error) => return with_evidence(domain::failure(error.tool_message()), ready, facts),
    };
    let second = domain::plan(&context.request, &current);
    let changes = match domain::continuation(&second, &ready.creations, ready.snapshot.as_deref()) {
        Ok(changes) => changes,
        Err(result) => return result,
    };
    let mut problems = Vec::new();
    if changes.is_empty() {
        if let Err(error) = owner.close() {
            *broken |= owner.is_poisoned();
            return with_evidence(begin_failure(error, facts), ready, facts);
        }
    } else {
        if let Err(error) =
            owner.begin_changes(changes.iter().filter_map(|a| a.effect().cloned()).collect())
        {
            *broken |= owner.is_poisoned();
            return with_evidence(begin_failure(error, facts), ready, facts);
        }
        let outcome = owner.finish().await.expect("admitted changes observer");
        if let Some(counts) = outcome.acknowledged {
            facts.push((outcome.stage, counts));
        }
        if outcome.receipt_error.is_some() {
            *broken = true;
            return with_evidence(
                domain::failure(
                    "The changes batch settled, but its operation receipt could not be published; known acknowledgements remain available",
                ),
                ready,
                facts,
            );
        }
        match outcome.transport {
            Ok(rows) => problems = domain::failures(&changes, &statuses(&rows)),
            Err(RequestError::BeforeDispatch(_)) => {
                if facts.is_empty() {
                    return with_evidence(outcome.policy_refusal.and_then(|p|p.tool_result()).unwrap_or_else(||domain::failure("Apply Scene changes batch was cancelled, expired or refused before dispatch")),ready,facts);
                }
                return with_evidence(
                    domain::failure(
                        "Changes batch was cancelled, expired or refused before dispatch; earlier acknowledged results remain known",
                    ),
                    ready,
                    facts,
                );
            }
            Err(_) => {
                *broken = true;
                return with_evidence(
                    domain::failure(
                        "Changes batch outcome is uncertain; inspect obs-observe and current OBS state; no retry was made",
                    ),
                    ready,
                    facts,
                );
            }
        }
    }
    let current = match snapshot_tools::apply_current(context.reads.clone(), &context.request).await
    {
        Ok(current) => current,
        Err(error) => return with_evidence(domain::failure(error.tool_message()), ready, facts),
    };
    let final_plan = domain::plan(&context.request, &current);
    let blank = snapshot_tools::apply_blank(context.reads.clone(), &context.request)
        .await
        .unwrap_or_default();
    domain::final_result(
        &context.request,
        &ready.creations,
        &changes,
        &problems,
        &final_plan,
        &blank,
        ready.snapshot.as_deref(),
    )
}
