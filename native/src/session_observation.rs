// SPDX-License-Identifier: GPL-2.0-only
//! One event observer per physical session, with cloneable typed read adapters.
//! The RPC/event cursor barrier prevents queued, unprocessed events from being
//! mistaken for unchanged evidence. This observer does not own OBS effects.
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::{
    observation::{
        EvidenceState, ObservationError, ReadAdapter, ReadFuture, ReadRequest, invalidates_event,
    },
    service_events::{EventHub, TakeEventFeed},
    session::{ALL_EVENTS, EventReceiver, SessionHandle},
};

pub const REQUIRED_AUDIT_EVENTS: u64 = ALL_EVENTS | (1 << 19);

fn invalidated() -> ObservationError {
    ObservationError::new(
        "observation_invalidated",
        "OBS changed during observation; run obs-observe again",
    )
}

struct EvidenceLifetime(
    watch::Sender<EvidenceState>,
    watch::Sender<EvidenceState>,
    Arc<EventHub>,
);
impl Drop for EvidenceLifetime {
    fn drop(&mut self) {
        self.0.send_modify(|state| state.connected = false);
        self.1.send_modify(|state| state.connected = false);
        self.2.revoke();
    }
}

/// Retain this owner for the whole session, not just one sample. Dropping it
/// ends evidence admission and aborts its observer; that is not an awaited
/// transport-shutdown receipt. Shut down the ObsSession owner separately.
pub struct ObservationBridge {
    handle: SessionHandle,
    state: watch::Sender<EvidenceState>,
    processed: Arc<AtomicU64>,
    audit_state: watch::Sender<EvidenceState>,
    audit_processed: Arc<AtomicU64>,
    supported: Option<Arc<HashSet<String>>>,
    task: Option<JoinHandle<()>>,
    full_profile: bool,
    event_hub: Arc<EventHub>,
}

/// Cloneable dispatch evidence, with no socket task, join handle or authority
/// to extend the observation owner's lifetime. Owner shutdown/drop revokes it.
#[derive(Clone)]
pub struct ObservationFence {
    handle: SessionHandle,
    state: watch::Receiver<EvidenceState>,
    processed: Arc<AtomicU64>,
    full_profile: bool,
}
impl ObservationFence {
    /// True only for the canonical full observation constructor. A custom
    /// filter is not proof that all stage-relevant events were counted.
    pub fn full_profile(&self) -> bool {
        self.full_profile
    }
    pub fn token(&self) -> crate::session::ConnectionToken {
        self.handle.token()
    }
    pub fn endpoint_hash(&self) -> &str {
        self.handle.endpoint_hash()
    }
    pub fn ready_at_revision(&self, generation: u64, revision: u64) -> bool {
        let cursor = self.handle.event_cursor();
        if self.processed.load(Ordering::Acquire) < cursor
            || self.handle.invalidation().is_some()
            || self.state.has_changed().is_err()
        {
            return false;
        }
        let state = *self.state.borrow();
        state.connected
            && !state.cancelled
            && state.generation == generation
            && state.revision == revision
            && self.handle.event_cursor() == cursor
    }
}

impl ObservationBridge {
    pub fn bind(
        handle: SessionHandle,
        events: EventReceiver,
        supported: Option<HashSet<String>>,
    ) -> Result<Self, ObservationError> {
        Self::bind_profile(handle, events, supported, invalidates_event, true)
    }

    /// An ephemeral workflow may have a narrower source-defined invalidation
    /// profile. The same owned event task still accounts for EVERY sequence,
    /// even when the profile does not advance its evidence revision. Callers
    /// must select the profile before the first sample, never change it during
    /// a sample or share this evidence with another workflow's profile.
    pub fn bind_with_event_filter(
        handle: SessionHandle,
        events: EventReceiver,
        supported: Option<HashSet<String>>,
        invalidates: fn(&str) -> bool,
    ) -> Result<Self, ObservationError> {
        Self::bind_profile(handle, events, supported, invalidates, false)
    }

    fn bind_profile(
        handle: SessionHandle,
        mut events: EventReceiver,
        supported: Option<HashSet<String>>,
        invalidates: fn(&str) -> bool,
        full_profile: bool,
    ) -> Result<Self, ObservationError> {
        if handle.token() != events.connection_token() || handle.invalidation().is_some() {
            return Err(invalidated());
        }
        let token = handle.token();
        let (state, _) = watch::channel(EvidenceState {
            generation: token.generation(),
            revision: 0,
            connected: true,
            cancelled: false,
        });
        let processed = Arc::new(AtomicU64::new(0));
        let (audit_state, _) = watch::channel(*state.borrow());
        let audit_processed = Arc::new(AtomicU64::new(0));
        let progress = processed.clone();
        let publish = state.clone();
        let audit_progress = audit_processed.clone();
        let audit_publish = audit_state.clone();
        let event_hub = Arc::new(EventHub::default());
        let delivery = event_hub.clone();
        let task = tokio::spawn(async move {
            let _lifetime =
                EvidenceLifetime(publish.clone(), audit_publish.clone(), delivery.clone());
            while let Ok(event) = events.next().await {
                let prior = progress.load(Ordering::Acquire);
                if event.connection != token || prior.checked_add(1) != Some(event.sequence) {
                    break;
                }
                let mut next = *publish.borrow();
                if invalidates(&event.event_type) {
                    let Some(revision) = next.revision.checked_add(1) else {
                        break;
                    };
                    next.revision = revision;
                }
                let mut audit_next = *audit_publish.borrow();
                if crate::scene_audit::invalidates_event(&event.event_type) {
                    let Some(revision) = audit_next.revision.checked_add(1) else {
                        break;
                    };
                    audit_next.revision = revision;
                }
                // Publish revision BEFORE advancing the cursor. Readers load
                // cursor then state; acquire/release preserves that ordering.
                let sequence = event.sequence;
                if delivery
                    .publish(event, || {
                        publish.send_modify(|current| {
                            next.connected &= current.connected;
                            next.cancelled |= current.cancelled;
                            *current = next;
                        });
                        audit_publish.send_modify(|current| {
                            audit_next.connected &= current.connected;
                            audit_next.cancelled |= current.cancelled;
                            *current = audit_next;
                        });
                        progress.store(sequence, Ordering::Release);
                        audit_progress.store(sequence, Ordering::Release);
                        // Also wake readers for unchanged structural revisions.
                        publish.send_modify(|_| {});
                        audit_publish.send_modify(|_| {});
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Self {
            handle,
            state,
            processed,
            audit_state,
            audit_processed,
            supported: supported.map(Arc::new),
            task: Some(task),
            full_profile,
            event_hub,
        })
    }

    pub fn evidence(&self) -> watch::Receiver<EvidenceState> {
        self.state.subscribe()
    }

    /// Source-exact scene-audit revisions, published by the same sole observer.
    /// This channel is read-only and cannot keep admission alive after owner drop.
    /// A channel alone is not proof of complete event subscriptions; obtaining
    /// an admitted audit_reader/audit_fence verifies the canonical constructor.
    pub fn audit_evidence(&self) -> watch::Receiver<EvidenceState> {
        self.audit_state.subscribe()
    }

    fn admit_audit(&self) -> Result<(), ObservationError> {
        if !self.full_profile
            || self.handle.event_subscriptions() & REQUIRED_AUDIT_EVENTS != REQUIRED_AUDIT_EVENTS
        {
            return Err(ObservationError::new(
                "capability_unavailable",
                "Scene audit requires the canonical observer and default plus scene-item-transform event subscriptions",
            ));
        }
        let state = *self.audit_state.borrow();
        if self.handle.invalidation().is_some() || !state.connected || state.cancelled {
            return Err(invalidated());
        }
        Ok(())
    }

    pub fn audit_fence(&self) -> Result<ObservationFence, ObservationError> {
        self.admit_audit()?;
        Ok(ObservationFence {
            handle: self.handle.clone(),
            state: self.audit_state.subscribe(),
            processed: self.audit_processed.clone(),
            full_profile: false,
        })
    }

    /// One take consumer shares this observer and the socket's existing event
    /// byte/item permits. Its owner must outlive the take supervisor's workers.
    pub fn attach_take_feed(&self) -> Result<TakeEventFeed, ObservationError> {
        if !self.full_profile
            || self.handle.event_subscriptions() & crate::takes::REQUIRED_SUBSCRIPTIONS
                != crate::takes::REQUIRED_SUBSCRIPTIONS
        {
            return Err(ObservationError::new(
                "capability_unavailable",
                "Take feed requires the canonical structural profile and meter subscriptions",
            ));
        }
        self.event_hub.attach(
            self.handle.clone(),
            self.state.subscribe(),
            self.processed.clone(),
        )
    }

    /// Revoke admission synchronously, before queued jobs or Arc references can
    /// extend the observer task. Joining the task remains a separate duty.
    pub fn invalidate(&self) {
        self.state.send_modify(|state| state.connected = false);
        self.audit_state
            .send_modify(|state| state.connected = false);
        self.event_hub.revoke();
    }

    pub fn fence(&self) -> ObservationFence {
        ObservationFence {
            handle: self.handle.clone(),
            state: self.state.subscribe(),
            processed: self.processed.clone(),
            full_profile: self.full_profile,
        }
    }

    /// A synchronous dispatch guard may use this after completing inspection.
    /// It never upgrades an older inspection to a newer event revision. Events
    /// already received but not published refuse admission, even if unrelated.
    /// An OBS change arriving after this check remains outside atomic proof.
    pub fn ready_at_revision(&self, generation: u64, revision: u64) -> bool {
        self.fence().ready_at_revision(generation, revision)
    }

    /// Finish publication of already-received events before starting a new
    /// sample. This issues no OBS requests and does not freeze future state.
    pub async fn synchronize(
        &self,
        deadline: Instant,
    ) -> Result<watch::Receiver<EvidenceState>, ObservationError> {
        let cursor = self.handle.event_cursor();
        let mut reader = self.reader();
        reader.cursor_barrier(cursor, deadline).await?;
        Ok(self.evidence())
    }

    pub fn reader(&self) -> SessionReader {
        SessionReader {
            handle: self.handle.clone(),
            state: self.state.subscribe(),
            processed: self.processed.clone(),
            supported: self.supported.clone(),
        }
    }

    /// A distinct read-only type prevents audit evidence from being supplied as
    /// the canonical SessionReader required by demo/stage transport admission.
    pub fn audit_reader(&self) -> Result<AuditReader, ObservationError> {
        self.admit_audit()?;
        Ok(AuditReader(SessionReader {
            handle: self.handle.clone(),
            state: self.audit_state.subscribe(),
            processed: self.audit_processed.clone(),
            supported: self.supported.clone(),
        }))
    }

    /// Service-wide cancellation. Request-scoped cancellation needs its own
    /// observation provider; it must not silently cancel another caller's sample.
    pub fn cancel_observations(&self) {
        self.state.send_modify(|state| state.cancelled = true);
        self.audit_state.send_modify(|state| state.cancelled = true);
    }

    pub async fn shutdown(mut self) -> Result<(), ObservationError> {
        self.invalidate();
        if let Some(task) = self.task.take() {
            task.abort();
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(_) => return Err(invalidated()),
            }
        }
        Ok(())
    }
}

impl Drop for ObservationBridge {
    fn drop(&mut self) {
        self.invalidate();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Fixed audit-profile view. It has no conversion to the canonical reader and
/// no socket, task, event receiver, write authority or producer ownership.
#[derive(Clone)]
pub struct AuditReader(SessionReader);
impl AuditReader {
    pub fn token(&self) -> crate::session::ConnectionToken {
        self.0.token()
    }
    pub fn endpoint_hash(&self) -> &str {
        self.0.endpoint_hash()
    }
    pub fn evidence(&self) -> EvidenceState {
        self.0.evidence()
    }
    pub async fn synchronize(&mut self, deadline: Instant) -> Result<(), ObservationError> {
        self.0.synchronize(deadline).await
    }
}
impl ReadAdapter for AuditReader {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.0.supports(request)
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: serde_json::Value,
        deadline: Instant,
    ) -> ReadFuture<'a> {
        self.0.read(request, data, deadline)
    }
}

/// Contains no socket or independent event task. Only the closed enum of safe
/// read requests is available; property enumeration and effects are excluded.
#[derive(Clone)]
pub struct SessionReader {
    handle: SessionHandle,
    state: watch::Receiver<EvidenceState>,
    processed: Arc<AtomicU64>,
    supported: Option<Arc<HashSet<String>>>,
}

impl SessionReader {
    pub fn token(&self) -> crate::session::ConnectionToken {
        self.handle.token()
    }

    pub fn endpoint_hash(&self) -> &str {
        self.handle.endpoint_hash()
    }

    /// An owned snapshot, not a channel that can publish or extend admission.
    pub fn evidence(&self) -> EvidenceState {
        *self.state.borrow()
    }

    /// Settle every event already received by this physical connection. A
    /// caller must still freeze/check revision for subsequent work: this is a
    /// publication barrier, not an atomic snapshot of OBS.
    pub async fn synchronize(&mut self, deadline: Instant) -> Result<(), ObservationError> {
        loop {
            if Instant::now() >= deadline {
                return Err(ObservationError::new(
                    "observation_limit",
                    "Observation event barrier exceeded its read deadline",
                ));
            }
            let cursor = self.handle.event_cursor();
            self.cursor_barrier(cursor, deadline).await?;
            if self.handle.event_cursor() == cursor {
                return Ok(());
            }
        }
    }

    async fn cursor_barrier(
        &mut self,
        cursor: u64,
        deadline: Instant,
    ) -> Result<(), ObservationError> {
        loop {
            // Cursor acquire must precede state read: producer publishes the
            // revision before releasing this cursor watermark.
            let processed = self.processed.load(Ordering::Acquire);
            let state = *self.state.borrow_and_update();
            if self.state.has_changed().is_err()
                || self.handle.invalidation().is_some()
                || !state.connected
                || state.cancelled
                || state.generation != self.handle.token().generation()
            {
                return Err(invalidated());
            }
            if processed >= cursor {
                return Ok(());
            }
            match timeout_at(deadline, self.state.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(invalidated()),
                Err(_) => {
                    return Err(ObservationError::new(
                        "observation_limit",
                        "Observation event barrier exceeded its read deadline",
                    ));
                }
            }
        }
    }
}

impl ReadAdapter for SessionReader {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.supported
            .as_ref()
            .map(|requests| requests.contains(request.as_str()))
    }

    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: serde_json::Value,
        deadline: Instant,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            let state = *self.state.borrow();
            if !state.connected
                || state.cancelled
                || self.handle.invalidation().is_some()
                || self.state.has_changed().is_err()
            {
                return Err(invalidated());
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|duration| !duration.is_zero())
                .ok_or_else(|| {
                    ObservationError::new("observation_limit", "Observation read deadline expired")
                })?;
            let response = self
                .handle
                .request(self.handle.token(), request.as_str(), data, remaining)
                .await;
            // Rejections are observations too: code 604 must not outrun a
            // relevant event already received before its reply. Callers can
            // then check their frozen sample before tolerating that error.
            let cursor = self.handle.event_cursor();
            self.cursor_barrier(cursor, deadline).await?;
            response.map_err(|error| {
                if let Some(code) = error.rejection_code() {
                    ObservationError::rejected(
                        code,
                        if code == 604 {
                            "capability_unavailable"
                        } else {
                            "evidence_unavailable"
                        },
                        error
                            .obs_failure_message(request.as_str())
                            .expect("typed rejection has a code"),
                    )
                } else {
                    invalidated()
                }
            })
        })
    }
}
