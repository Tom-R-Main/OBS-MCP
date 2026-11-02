// SPDX-License-Identifier: GPL-2.0-only
//! Retained read-only jobs for the service's existing physical OBS session.
//! The caller owns the complete future. No socket, observer, relay task or
//! mutator is created, and no native-only failure launches a compatibility lane.
use crate::{
    clock,
    custody::{Clock, ClockSnapshot},
    observation::{
        EvidenceState, Observation, ObservationError, ReadAdapter, ReadFuture, ReadRequest,
    },
    scene_audit::{self, AuditCompletion, AuditOptions, AuditSample, ScreenshotLimit},
    scene_plan::{PlanRequest, plan_report},
    session_observation::ObservationBridge,
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::{sync::watch, time::Instant};

#[derive(Clone, Copy, Debug)]
pub struct WorkflowBounds {
    pub deadline: Instant,
}
impl Default for WorkflowBounds {
    fn default() -> Self {
        Self {
            deadline: Instant::now() + Duration::from_secs(12),
        }
    }
}
impl WorkflowBounds {
    fn admitted(self) -> Result<Instant, ObservationError> {
        let now = Instant::now();
        if self.deadline <= now {
            return Err(ObservationError::new(
                "observation_limit",
                "Read workflow deadline expired",
            ));
        }
        Ok(self.deadline.min(now + Duration::from_secs(12)))
    }
}
fn projection() -> ObservationError {
    ObservationError::new(
        "native_projection_unavailable",
        "Native read workflow cannot preserve this evidence projection; inspect the reported input before retrying",
    )
}
fn cancelled() -> ObservationError {
    ObservationError::new(
        "cancelled",
        "Operation cancelled; inspect OBS before any further action",
    )
}
fn checked_clock(clock: &Clock) -> Result<ClockSnapshot, ObservationError> {
    let snapshot = clock();
    let derived =
        clock::from_epoch_duration(Duration::from_millis(snapshot.epoch_ms)).map_err(|_| {
            ObservationError::new("clock_unavailable", "Read workflow clock is unavailable")
        })?;
    if derived.utc != snapshot.utc {
        return Err(ObservationError::new(
            "clock_unavailable",
            "Read workflow epoch and UTC do not match",
        ));
    }
    Ok(snapshot)
}
/// Per-call capability evidence from this same physical connection's cached
/// GetVersion. It adds no profile/revision/connection authority.
#[derive(Clone)]
struct CapabilityReader<R> {
    inner: R,
    capabilities: Option<Arc<HashSet<String>>>,
}
impl<R: ReadAdapter> ReadAdapter for CapabilityReader<R> {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.capabilities
            .as_ref()
            .map(|set| set.contains(request.as_str()))
            .or_else(|| self.inner.supports(request))
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        deadline: Instant,
    ) -> ReadFuture<'a> {
        self.inner.read(request, data, deadline)
    }
}
type OwnedRead = Pin<Box<dyn Future<Output = Result<Value, ObservationError>> + Send + 'static>>;
/// The sample can time out its borrowed polling future without destroying an
/// admitted RPC. Exactly one independently deadline-bounded RPC remains here;
/// the workflow drains it before returning its report or unsupported result.
struct ScopedReader<R> {
    inner: R,
    source: watch::Receiver<EvidenceState>,
    local: watch::Sender<EvidenceState>,
    cancellation: Cancellation,
    pending: Option<OwnedRead>,
    failure: Option<ObservationError>,
}
impl<R: ReadAdapter + Clone + 'static> ScopedReader<R> {
    fn new(inner: R, source: watch::Receiver<EvidenceState>, cancellation: Cancellation) -> Self {
        let initial = *source.borrow();
        let (local, _) = watch::channel(initial);
        Self {
            inner,
            source,
            local,
            cancellation,
            pending: None,
            failure: None,
        }
    }
    fn refresh(&self) {
        let source = *self.source.borrow();
        let alive = self.source.has_changed().is_ok();
        self.local.send_modify(|state| {
            state.revision = state.revision.max(source.revision);
            state.connected &= source.connected && alive && state.generation == source.generation;
            state.cancelled |= source.cancelled || self.cancellation.is_cancelled();
        });
    }
    async fn drain(&mut self) {
        if let Some(work) = self.pending.take()
            && let Err(error) = work.await
        {
            self.failure = Some(error);
        }
        self.refresh();
    }
}
impl<R: ReadAdapter + Clone + 'static> ReadAdapter for ScopedReader<R> {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.inner.supports(request)
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        deadline: Instant,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            self.refresh();
            if self.cancellation.is_cancelled() {
                return Err(cancelled());
            }
            if self.pending.is_some() {
                return Err(ObservationError::new(
                    "operation_in_progress",
                    "Previous workflow read has not settled",
                ));
            }
            let mut inner = self.inner.clone();
            self.pending = Some(Box::pin(async move {
                inner.read(request, data, deadline).await
            }));
            let result = self.pending.as_mut().expect("admitted owned RPC").await;
            self.pending.take();
            self.refresh();
            if let Err(error) = &result {
                self.failure = Some(error.clone());
            }
            result
        })
    }
}
fn body(report: Value) -> Result<Value, ObservationError> {
    let text = serde_json::to_string(&report).map_err(|_| projection())?;
    Ok(json!({"content":[{"type":"text","text":text}],"structuredContent":report}))
}
/// Plan on canonical evidence. Meter telemetry is irrelevant; recording and
/// other structural changes retain the canonical source invalidation policy.
pub async fn plan(
    bridge: Arc<ObservationBridge>,
    arguments: PlanRequest,
    clock: Clock,
    cancellation: Cancellation,
    bounds: WorkflowBounds,
) -> Result<Value, ObservationError> {
    plan_with_capabilities(bridge, arguments, clock, cancellation, bounds, None).await
}

pub async fn plan_with_capabilities(
    bridge: Arc<ObservationBridge>,
    arguments: PlanRequest,
    clock: Clock,
    cancellation: Cancellation,
    bounds: WorkflowBounds,
    capabilities: Option<Arc<HashSet<String>>>,
) -> Result<Value, ObservationError> {
    let deadline = bounds.admitted()?;
    if cancellation.is_cancelled() {
        return Err(cancelled());
    }
    // Planning fingerprints transforms, so require the full canonical observer
    // plus the transform event subscription before admitting this sample.
    bridge.audit_fence()?;
    let mut inner = bridge.reader();
    inner.synchronize(deadline).await?;
    let started = checked_clock(&clock)?;
    let mut reader = ScopedReader::new(
        CapabilityReader {
            inner,
            capabilities,
        },
        bridge.evidence(),
        cancellation.clone(),
    );
    reader.refresh();
    let state = reader.local.subscribe();
    let mut completion_error = None;
    let report = {
        let mut sample = Observation::new(
            &mut reader,
            state,
            bridge.fence().token().generation(),
            started.utc,
            deadline,
        )?;
        let report = plan_report(&mut sample, &arguments, || match checked_clock(&clock) {
            Ok(now) => now.utc,
            Err(error) => {
                completion_error = Some(error);
                String::new()
            }
        })
        .await;
        sample.close();
        report
    };
    reader.drain().await;
    if let Some(error) = completion_error {
        return Err(error);
    }
    if cancellation.is_cancelled() {
        return Err(cancelled());
    }
    if report["blockers"].as_array().is_some_and(|blockers| {
        blockers.iter().any(|blocker| {
            blocker["message"] == "Observation signature property collation is unavailable"
        })
    }) {
        return Err(projection());
    }
    body(report)
}

/// Audit on the fixed source-exact audit profile, independent of recording or
/// meter events. Unsupported projections return an explicit native-only error.
pub async fn audit(
    bridge: Arc<ObservationBridge>,
    arguments: AuditOptions,
    clock: Clock,
    cancellation: Cancellation,
    bounds: WorkflowBounds,
    screenshot_limit: Result<ScreenshotLimit, ObservationError>,
) -> Result<Value, ObservationError> {
    audit_with_capabilities(
        bridge,
        arguments,
        clock,
        cancellation,
        bounds,
        screenshot_limit,
        None,
    )
    .await
}

pub async fn audit_with_capabilities(
    bridge: Arc<ObservationBridge>,
    arguments: AuditOptions,
    clock: Clock,
    cancellation: Cancellation,
    bounds: WorkflowBounds,
    screenshot_limit: Result<ScreenshotLimit, ObservationError>,
    capabilities: Option<Arc<HashSet<String>>>,
) -> Result<Value, ObservationError> {
    let deadline = bounds.admitted()?;
    if cancellation.is_cancelled() {
        return Err(cancelled());
    }
    let mut inner = bridge.audit_reader()?;
    inner.synchronize(deadline).await?;
    let started = checked_clock(&clock)?;
    let mut reader = ScopedReader::new(
        CapabilityReader {
            inner,
            capabilities,
        },
        bridge.audit_evidence(),
        cancellation.clone(),
    );
    reader.refresh();
    let state = reader.local.subscribe();
    let mut completion_error = None;
    let (report, unsupported) = {
        let mut sample = AuditSample::new(
            &mut reader,
            state,
            bridge.fence().token().generation(),
            started.utc,
            deadline,
        );
        let report =
            scene_audit::audit(
                &mut sample,
                &arguments,
                screenshot_limit,
                || match checked_clock(&clock) {
                    Ok(now) if now.epoch_ms >= started.epoch_ms => AuditCompletion {
                        completed_at: now.utc,
                        duration_ms: now.epoch_ms - started.epoch_ms,
                    },
                    Ok(_) => {
                        completion_error = Some(ObservationError::new(
                            "clock_unavailable",
                            "Read workflow clock moved backwards",
                        ));
                        AuditCompletion {
                            completed_at: String::new(),
                            duration_ms: 0,
                        }
                    }
                    Err(error) => {
                        completion_error = Some(error);
                        AuditCompletion {
                            completed_at: String::new(),
                            duration_ms: 0,
                        }
                    }
                },
            )
            .await;
        let unsupported = sample.requires_compatibility();
        sample.close();
        (report, unsupported)
    };
    reader.drain().await;
    if let Some(error) = completion_error {
        return Err(error);
    }
    if cancellation.is_cancelled() {
        return Err(cancelled());
    }
    if unsupported {
        return Err(reader.failure.unwrap_or_else(projection));
    }
    Ok(report)
}
