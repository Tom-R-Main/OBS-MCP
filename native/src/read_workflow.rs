// SPDX-License-Identifier: GPL-2.0-only
//! Ephemeral owner for qualified read-only planning and structural audit. The
//! caller keeps this owner outside its cancellable work future and always joins
//! actor + observer before replying or starting the compatibility runtime.
use std::{collections::HashSet, io, time::Duration};

use obs_mcp_native::{
    clock,
    observation::{EvidenceState, Observation, ReadAdapter, ReadFuture, ReadRequest},
    scene_audit::{self, AuditCompletion, AuditOptions, AuditSample, ScreenshotLimit},
    scene_plan::{PlanRequest, plan_report},
    session::{EventReceiver, ObsSession, SessionHandle, SessionOptions},
    session_observation::{ObservationBridge, SessionReader},
};
use serde_json::{Value, json};
use tokio::{sync::watch, time::Instant};

pub(crate) enum ReadWorkflowRequest {
    Plan(PlanRequest),
    Audit(AuditOptions),
}
impl ReadWorkflowRequest {
    pub(crate) fn parse(name: &str, arguments: &Value) -> Option<Self> {
        match name {
            "obs-plan-scene-template" => PlanRequest::from_value(arguments).ok().map(Self::Plan),
            "obs-audit-scene" => AuditOptions::from_value(arguments).ok().map(Self::Audit),
            _ => None,
        }
    }
    pub(crate) fn handler(&self) -> &'static str {
        match self {
            Self::Plan(_) => "scene-template-plan",
            Self::Audit(_) => "scene-audit",
        }
    }
}

pub(crate) struct ReadWorkflow {
    url: String,
    password: Option<String>,
    actor: Option<ObsSession>,
    events: Option<EventReceiver>,
    bridge: Option<ObservationBridge>,
}

/// The transport keeps peer-controlled error comments private. Until their
/// public projection moves to Rust, retain the SDK's exact interpretation of
/// adapter failures after closing every native owner. Semantic planning errors
/// and ordinary invalidating events already have qualified native projections.
struct WorkflowReader {
    inner: SessionReader,
    evidence: watch::Receiver<EvidenceState>,
    sdk_error: bool,
}

struct PendingRead<'a> {
    sdk_error: &'a mut bool,
    completed: bool,
}
impl Drop for PendingRead<'_> {
    fn drop(&mut self) {
        // Observation can cancel this read future at its own deadline before
        // SessionReader returns. Preserve that unsupported error projection too.
        if !self.completed {
            *self.sdk_error = true;
        }
    }
}

impl ReadAdapter for WorkflowReader {
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
            let before = *self.evidence.borrow();
            let mut pending = PendingRead {
                sdk_error: &mut self.sdk_error,
                completed: false,
            };
            let result = self.inner.read(request, data, deadline).await;
            pending.completed = true;
            if let Err(error) = &result {
                let after = *self.evidence.borrow();
                // Only a published event revision has the already-qualified
                // invalidation projection. A transport deadline can report an
                // unknown result before its actor publishes terminal status.
                let event_invalidation = error.reason == "observation_invalidated"
                    && after.connected
                    && after.generation == before.generation
                    && after.revision != before.revision;
                if !event_invalidation {
                    *pending.sdk_error = true;
                }
            }
            result
        })
    }
}

impl ReadWorkflow {
    pub(crate) fn new(url: String, password: Option<String>) -> Self {
        Self {
            url,
            password,
            actor: None,
            events: None,
            bridge: None,
        }
    }

    pub(crate) async fn run(&mut self, request: &ReadWorkflowRequest) -> Result<Value, ()> {
        match request {
            ReadWorkflowRequest::Plan(arguments) => self.plan(arguments).await,
            ReadWorkflowRequest::Audit(arguments) => self.audit(arguments).await,
        }
    }

    async fn setup(&mut self, invalidates: fn(&str) -> bool) -> Result<SessionHandle, ()> {
        let setup_deadline = Instant::now() + Duration::from_secs(10);
        let (actor, events) = ObsSession::connect(
            &self.url,
            self.password.as_deref(),
            SessionOptions::default(),
        )
        .await
        .map_err(|_| ())?;
        let handle = actor.handle();
        let token = handle.token();
        // Transfer each asynchronous owner into self before the next await.
        self.actor = Some(actor);
        self.events = Some(events);
        let remaining = setup_deadline
            .checked_duration_since(Instant::now())
            .ok_or(())?;
        let version = handle
            .request(token, "GetVersion", Value::Null, remaining)
            .await
            .map_err(|_| ())?;
        if !version.is_object() {
            return Err(());
        }
        let supported: Option<HashSet<String>> =
            version["availableRequests"].as_array().map(|requests| {
                requests
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            });
        // Events collected during capability discovery remain owned and ordered.
        // Synchronize their cursor before freezing the observation revision.
        let bridge = ObservationBridge::bind_with_event_filter(
            handle.clone(),
            self.events.take().ok_or(())?,
            supported,
            invalidates,
        )
        .map_err(|_| ())?;
        self.bridge = Some(bridge);
        Ok(handle)
    }

    /// Unsupported setup/adapter projections return to the SDK after cleanup.
    async fn plan(&mut self, arguments: &PlanRequest) -> Result<Value, ()> {
        let handle = self
            .setup(obs_mcp_native::observation::invalidates_event)
            .await?;
        let token = handle.token();
        let bridge = self.bridge.as_ref().ok_or(())?;
        let deadline = Instant::now() + Duration::from_secs(12);
        let state = bridge.synchronize(deadline).await.map_err(|_| ())?;
        let observed_at = clock::snapshot().map_err(|_| ())?.utc;
        let mut reader = WorkflowReader {
            inner: bridge.reader(),
            evidence: bridge.evidence(),
            sdk_error: false,
        };
        let mut clock_failed = false;
        let report = {
            let mut sample = Observation::new(
                &mut reader,
                state,
                token.generation(),
                observed_at,
                deadline,
            )
            .map_err(|_| ())?;
            let report = plan_report(&mut sample, arguments, || match clock::snapshot() {
                Ok(snapshot) => snapshot.utc,
                Err(_) => {
                    clock_failed = true;
                    String::new()
                }
            })
            .await;
            sample.close();
            report
        };
        if clock_failed || reader.sdk_error || handle.invalidation().is_some() {
            return Err(());
        }
        Ok(
            json!({"content":[{"type":"text","text":serde_json::to_string(&report).map_err(|_| ())?}],"structuredContent":report}),
        )
    }

    async fn audit(&mut self, arguments: &AuditOptions) -> Result<Value, ()> {
        let started = clock::snapshot().map_err(|_| ())?;
        let handle = self.setup(scene_audit::invalidates_event).await?;
        let bridge = self.bridge.as_ref().ok_or(())?;
        let deadline = Instant::now() + Duration::from_secs(12);
        let state = bridge.synchronize(deadline).await.map_err(|_| ())?;
        let mut reader = WorkflowReader {
            inner: bridge.reader(),
            evidence: bridge.evidence(),
            sdk_error: false,
        };
        let mut clock_failed = false;
        let (report, requires_compatibility) = {
            let mut sample = AuditSample::new(
                &mut reader,
                state,
                handle.token().generation(),
                started.utc,
                deadline,
            );
            let report =
                scene_audit::audit(&mut sample, arguments, ScreenshotLimit::parse(None), || {
                    match clock::snapshot() {
                        Ok(now) if now.epoch_ms >= started.epoch_ms => AuditCompletion {
                            completed_at: now.utc,
                            duration_ms: now.epoch_ms - started.epoch_ms,
                        },
                        _ => {
                            clock_failed = true;
                            AuditCompletion {
                                completed_at: String::new(),
                                duration_ms: 0,
                            }
                        }
                    }
                })
                .await;
            let requires_compatibility = sample.requires_compatibility();
            sample.close();
            (report, requires_compatibility)
        };
        if clock_failed
            || requires_compatibility
            || reader.sdk_error
            || handle.invalidation().is_some()
        {
            return Err(());
        }
        Ok(report)
    }

    /// Attempt both joins even if one reports failure. EventReceiver remains
    /// alive until actor shutdown, including when bridge construction failed.
    pub(crate) async fn cleanup(&mut self) -> io::Result<()> {
        let actor = self.actor.take();
        let bridge = self.bridge.take();
        let events = self.events.take();
        let actor_result = match actor {
            Some(actor) => actor
                .shutdown()
                .await
                .map_err(|_| io::Error::other("Read workflow actor shutdown failed")),
            None => Ok(()),
        };
        let bridge_result = match bridge {
            Some(bridge) => bridge
                .shutdown()
                .await
                .map_err(|_| io::Error::other("Read workflow observer shutdown failed")),
            None => Ok(()),
        };
        drop(events);
        actor_result.and(bridge_result)
    }
}
