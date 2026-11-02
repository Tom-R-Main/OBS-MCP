// SPDX-License-Identifier: GPL-2.0-only
//! An external recording-impact veto, not recording identity or OBS state.
//!
//! One owner must span the composing Service lifetime. Views, manual ports,
//! tickets, attempts and change receivers cannot keep that owner admitted.
//! The owner retains a clone of the existing SessionHandle, never a second
//! socket or reader. During graceful shutdown, seal new manual admission at the
//! composing Service first, retain this owner through normal owned Take cleanup
//! and all admitted manual settlements, then drop it. Dropping a still-Ready
//! owner earlier would deliberately revoke that cleanup permission.
//! There is one pending actual-send attempt and constant-sized history: an
//! increasing identity and a revocation watermark. No task, RPC, OBS consumer,
//! path or journal writer is created. Callers must begin inside the physical
//! writer's final guard and settle from its actual transport disposition.
use crate::session::{ConnectionToken, SessionHandle};
use std::{
    fmt,
    sync::{Arc, Mutex, Weak},
};
use tokio::{
    sync::watch,
    time::{Instant, timeout_at},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AttemptId(u64);
impl fmt::Debug for AttemptId {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("AttemptId { .. }")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Revocation {
    ExternalEffect,
    UnknownEffect,
    UnqualifiedRejection,
    AbandonedAttempt,
    OwnerGone,
    ConnectionClosed,
    Poisoned,
    IdentityExhausted,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImpactStatus {
    Ready,
    Suspended(AttemptId),
    Revoked(Revocation),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImpactError {
    OwnerGone,
    ConnectionClosed,
    WrongConnection,
    Capacity,
    StaleAttempt,
    Poisoned,
    IdentityExhausted,
    Deadline,
}
impl fmt::Display for ImpactError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "{self:?}")
    }
}
impl std::error::Error for ImpactError {}
struct State {
    alive: bool,
    terminal: Option<Revocation>,
    last_id: u64,
    pending: Option<AttemptId>,
    revoked_through: u64,
    reason: Revocation,
    revision: u64,
}
struct Inner {
    session: SessionHandle,
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}
impl Inner {
    // The sole lock is never held across await or an external callback. Watch
    // publication is synchronous while holding it, so revisions cannot reorder.
    fn advance(&self, state: &mut State) {
        if let Some(next) = state.revision.checked_add(1) {
            state.revision = next;
        } else {
            state.terminal = Some(Revocation::IdentityExhausted);
            state.pending = None;
        }
        self.changed.send_replace(state.revision);
    }
    fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> Result<R, ImpactError> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poison) => {
                let mut state = poison.into_inner();
                state.terminal = Some(Revocation::Poisoned);
                state.pending = None;
                self.advance(&mut state);
                return Err(ImpactError::Poisoned);
            }
        };
        if !state.alive {
            return Err(ImpactError::OwnerGone);
        }
        if self.session.invalidation().is_some() && state.terminal.is_none() {
            state.terminal = Some(Revocation::ConnectionClosed);
            state.pending = None;
            self.advance(&mut state);
        }
        if let Some(reason) = state.terminal {
            return Err(match reason {
                Revocation::Poisoned => ImpactError::Poisoned,
                Revocation::IdentityExhausted => ImpactError::IdentityExhausted,
                _ => ImpactError::ConnectionClosed,
            });
        }
        Ok(f(&mut state))
    }
    fn settle(&self, id: AttemptId, fate: Option<Revocation>) -> Result<(), ImpactError> {
        self.with(|state| {
            if state.pending != Some(id) {
                return Err(ImpactError::StaleAttempt);
            }
            state.pending = None;
            if let Some(reason) = fate {
                state.revoked_through = id.0;
                state.reason = reason;
            }
            self.advance(state);
            Ok(())
        })?
    }
}

/// The only strong owner of the impact registry. Drop invalidates borrowed
/// admissions synchronously; a transient upgraded borrow cannot defeat Drop.
pub struct RecordImpactOwner {
    inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct RecordImpactView {
    inner: Weak<Inner>,
    token: ConnectionToken,
}
#[derive(Clone)]
pub struct ManualRecordImpactPort {
    inner: Weak<Inner>,
    token: ConnectionToken,
}
pub struct TakeImpactTicket {
    inner: Weak<Inner>,
    epoch: u64,
    token: ConnectionToken,
}
/// A linear admitted attempt. Dropping it without a known fate revokes all
/// older tickets; exact NotDispatched is the only release without revocation.
pub struct ManualImpactAttempt {
    inner: Weak<Inner>,
    id: AttemptId,
    settled: bool,
}
/// One bounded watch cell, not a queue of historical transitions. The next
/// check must inspect the ticket: several transitions may coalesce.
pub struct ImpactChanges {
    inner: Weak<Inner>,
    receiver: watch::Receiver<u64>,
}
macro_rules! opaque_debug {
    ($($name:ty),*) => {$(impl fmt::Debug for $name {
        fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
            out.write_str(concat!(stringify!($name), " { .. }"))
        }
    })*};
}
opaque_debug!(
    RecordImpactOwner,
    RecordImpactView,
    ManualRecordImpactPort,
    TakeImpactTicket,
    ManualImpactAttempt,
    ImpactChanges
);
fn upgrade(inner: &Weak<Inner>) -> Result<Arc<Inner>, ImpactError> {
    inner.upgrade().ok_or(ImpactError::OwnerGone)
}
impl RecordImpactOwner {
    pub fn bind(session: &SessionHandle) -> Result<Self, ImpactError> {
        if session.invalidation().is_some() {
            return Err(ImpactError::ConnectionClosed);
        }
        let (changed, _) = watch::channel(0);
        Ok(Self {
            inner: Arc::new(Inner {
                session: session.clone(),
                changed,
                state: Mutex::new(State {
                    alive: true,
                    terminal: None,
                    last_id: 0,
                    pending: None,
                    revoked_through: 0,
                    reason: Revocation::ExternalEffect,
                    revision: 0,
                }),
            }),
        })
    }
    pub fn view(&self) -> RecordImpactView {
        RecordImpactView {
            inner: Arc::downgrade(&self.inner),
            token: self.inner.session.token(),
        }
    }
    pub fn manual_port(&self) -> ManualRecordImpactPort {
        ManualRecordImpactPort {
            inner: Arc::downgrade(&self.inner),
            token: self.inner.session.token(),
        }
    }
}
impl Drop for RecordImpactOwner {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.alive = false;
        state.pending = None;
        self.inner.advance(&mut state);
    }
}
impl RecordImpactView {
    pub fn token(&self) -> ConnectionToken {
        self.token
    }
    /// Registry identity is stricter than physical connection identity: two
    /// independent owners may bind the same socket but grant different epochs.
    pub(crate) fn owns_ticket(&self, ticket: &TakeImpactTicket) -> bool {
        self.token == ticket.token && Weak::ptr_eq(&self.inner, &ticket.inner)
    }
    pub fn reserve_take(&self) -> Result<TakeImpactTicket, ImpactError> {
        let inner = upgrade(&self.inner)?;
        let epoch = inner.with(|state| {
            if state.pending.is_some() {
                Err(ImpactError::Capacity)
            } else {
                Ok(state.last_id)
            }
        })??;
        Ok(TakeImpactTicket {
            inner: self.inner.clone(),
            epoch,
            token: self.token,
        })
    }
    pub fn changes(&self) -> Result<ImpactChanges, ImpactError> {
        let inner = upgrade(&self.inner)?;
        inner.with(|_| ())?;
        Ok(ImpactChanges {
            inner: self.inner.clone(),
            receiver: inner.changed.subscribe(),
        })
    }
}
impl TakeImpactTicket {
    pub fn token(&self) -> ConnectionToken {
        self.token
    }
    pub fn check(&self) -> ImpactStatus {
        let inner = match upgrade(&self.inner) {
            Ok(inner) => inner,
            Err(_) => return ImpactStatus::Revoked(Revocation::OwnerGone),
        };
        match inner.with(|state| {
            if state.revoked_through > self.epoch {
                ImpactStatus::Revoked(state.reason)
            } else if let Some(id) = state.pending {
                ImpactStatus::Suspended(id)
            } else {
                ImpactStatus::Ready
            }
        }) {
            Ok(status) => status,
            Err(error) => ImpactStatus::Revoked(match error {
                ImpactError::Poisoned => Revocation::Poisoned,
                ImpactError::IdentityExhausted => Revocation::IdentityExhausted,
                ImpactError::OwnerGone => Revocation::OwnerGone,
                _ => Revocation::ConnectionClosed,
            }),
        }
    }
}
impl ManualRecordImpactPort {
    /// Validate the exact live physical owner without allocating an attempt.
    /// A prior ticket's historical revocation is not new manual-call authority;
    /// ordinary policy/custody and the actual writer guard remain required.
    pub fn check_connection(&self, expected: ConnectionToken) -> Result<(), ImpactError> {
        if expected != self.token {
            return Err(ImpactError::WrongConnection);
        }
        upgrade(&self.inner)?.with(|_| ())
    }

    /// Call only as the final synchronous step of the same physical writer's
    /// checked dispatch guard, after ordinary policy/custody checks succeed.
    pub fn begin_at_send(
        &self,
        expected: ConnectionToken,
    ) -> Result<ManualImpactAttempt, ImpactError> {
        if expected != self.token {
            return Err(ImpactError::WrongConnection);
        }
        let inner = upgrade(&self.inner)?;
        let id = inner.with(|state| {
            if state.pending.is_some() {
                return Err(ImpactError::Capacity);
            }
            let Some(next) = state.last_id.checked_add(1) else {
                state.terminal = Some(Revocation::IdentityExhausted);
                inner.advance(state);
                return Err(ImpactError::IdentityExhausted);
            };
            let id = AttemptId(next);
            state.last_id = next;
            state.pending = Some(id);
            inner.advance(state);
            if state.terminal.is_some() {
                Err(ImpactError::IdentityExhausted)
            } else {
                Ok(id)
            }
        })??;
        Ok(ManualImpactAttempt {
            inner: self.inner.clone(),
            id,
            settled: false,
        })
    }
}
impl ManualImpactAttempt {
    pub fn id(&self) -> AttemptId {
        self.id
    }
    fn finish(mut self, fate: Option<Revocation>) -> Result<(), ImpactError> {
        self.settled = true;
        upgrade(&self.inner)?.settle(self.id, fate)
    }
    pub fn not_dispatched(self) -> Result<(), ImpactError> {
        self.finish(None)
    }
    pub fn accepted(self) -> Result<(), ImpactError> {
        self.finish(Some(Revocation::ExternalEffect))
    }
    pub fn unknown(self) -> Result<(), ImpactError> {
        self.finish(Some(Revocation::UnknownEffect))
    }
    /// Until an exact rejection has a qualified no-effect proof, it cannot
    /// establish that a recording's identity or lifetime is unchanged.
    pub fn rejected_unqualified(self) -> Result<(), ImpactError> {
        self.finish(Some(Revocation::UnqualifiedRejection))
    }
}
impl Drop for ManualImpactAttempt {
    fn drop(&mut self) {
        if !self.settled
            && let Some(inner) = self.inner.upgrade()
        {
            let _ = inner.settle(self.id, Some(Revocation::AbandonedAttempt));
        }
    }
}
impl ImpactChanges {
    /// A coordination wake, never identity/state proof. A wait owns no task,
    /// strong owner or physical receiver and can be dropped without an effect.
    /// Session EOF is checked at entry and wake/deadline; the existing OBS feed
    /// remains the immediate disconnect wake source for future composition.
    pub async fn changed_until(&mut self, deadline: Instant) -> Result<(), ImpactError> {
        upgrade(&self.inner)?.with(|_| ())?;
        if Instant::now() >= deadline {
            return Err(ImpactError::Deadline);
        }
        match timeout_at(deadline, self.changed()).await {
            Ok(result) => result,
            Err(_) => {
                upgrade(&self.inner)?.with(|_| ())?;
                Err(ImpactError::Deadline)
            }
        }
    }
    /// Cancellable coordination wake for an owner actor that already selects
    /// its own shutdown and OBS-disconnect feed. No deadline or liveness poller
    /// is created here; signal payload is never authority without rechecking.
    pub async fn changed(&mut self) -> Result<(), ImpactError> {
        upgrade(&self.inner)?.with(|_| ())?;
        let changed = self.receiver.changed().await;
        upgrade(&self.inner)?.with(|_| ())?;
        changed.map_err(|_| ImpactError::OwnerGone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{EventReceiver, ObsSession, SessionOptions};
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use std::time::Duration;
    use tokio::{net::TcpListener, task::JoinHandle};
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    async fn physical() -> (ObsSession, EventReceiver, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(socket).await.unwrap();
            peer.send(Message::Text(
                json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            assert!(matches!(peer.next().await, Some(Ok(Message::Text(_)))));
            peer.send(Message::Text(
                json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            while let Some(frame) = peer.next().await {
                if matches!(frame, Ok(Message::Close(_)) | Err(_)) {
                    break;
                }
                assert!(!matches!(frame, Ok(Message::Text(_))), "no module RPC");
            }
        });
        let (session, events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        (session, events, peer)
    }
    async fn joined(session: ObsSession, events: EventReceiver, peer: JoinHandle<()>) {
        session.shutdown().await.unwrap();
        drop(events);
        tokio::time::timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn connection_check_does_not_allocate_attempts_revisions_or_change_ready_permission() {
        let (session, events, peer) = physical().await;
        let token = session.token();
        let owner = RecordImpactOwner::bind(&session.handle()).unwrap();
        let ticket = owner.view().reserve_take().unwrap();
        let port = owner.manual_port();
        let before = {
            let state = owner.inner.state.lock().unwrap();
            (
                state.last_id,
                state.revision,
                state.revoked_through,
                state.pending,
            )
        };
        for _ in 0..100 {
            assert_eq!(port.check_connection(token), Ok(()));
        }
        let after = {
            let state = owner.inner.state.lock().unwrap();
            (
                state.last_id,
                state.revision,
                state.revoked_through,
                state.pending,
            )
        };
        assert_eq!(before, after);
        assert_eq!(ticket.check(), ImpactStatus::Ready);
        drop(owner);
        assert_eq!(port.check_connection(token), Err(ImpactError::OwnerGone));
        joined(session, events, peer).await;
    }
    #[tokio::test]
    async fn a_stale_exact_no_dispatch_cannot_clear_a_later_suspension() {
        let (session, events, peer) = physical().await;
        let handle = session.handle();
        let owner = RecordImpactOwner::bind(&handle).unwrap();
        let ticket = owner.view().reserve_take().unwrap();
        let first = owner.manual_port().begin_at_send(handle.token()).unwrap();
        let stale = first.id();
        first.not_dispatched().unwrap();
        let second = owner.manual_port().begin_at_send(handle.token()).unwrap();
        let result = owner.inner.settle(stale, None);
        let status = ticket.check();
        let wanted = ImpactStatus::Suspended(second.id());
        second.not_dispatched().unwrap();
        drop(owner);
        joined(session, events, peer).await;
        assert_eq!(result, Err(ImpactError::StaleAttempt));
        assert_eq!(status, wanted);
    }
    #[tokio::test]
    async fn finite_identity_exhaustion_revokes_instead_of_wrapping_into_old_permission() {
        let (session, events, peer) = physical().await;
        let handle = session.handle();
        let owner = RecordImpactOwner::bind(&handle).unwrap();
        let view = owner.view();
        let ticket = view.reserve_take().unwrap();
        owner.inner.state.lock().unwrap().last_id = u64::MAX;
        let result = owner
            .manual_port()
            .begin_at_send(handle.token())
            .unwrap_err();
        let status = ticket.check();
        let reserve = view.reserve_take().unwrap_err();
        drop(owner);
        joined(session, events, peer).await;
        assert_eq!(result, ImpactError::IdentityExhausted);
        assert_eq!(status, ImpactStatus::Revoked(Revocation::IdentityExhausted));
        assert_eq!(reserve, ImpactError::IdentityExhausted);
    }
    #[tokio::test]
    async fn revision_exhaustion_cannot_publish_ready_after_a_new_attempt() {
        let (session, events, peer) = physical().await;
        let handle = session.handle();
        let owner = RecordImpactOwner::bind(&handle).unwrap();
        let ticket = owner.view().reserve_take().unwrap();
        owner.inner.state.lock().unwrap().revision = u64::MAX;
        let result = owner
            .manual_port()
            .begin_at_send(handle.token())
            .unwrap_err();
        let status = ticket.check();
        drop(owner);
        joined(session, events, peer).await;
        assert_eq!(result, ImpactError::IdentityExhausted);
        assert_eq!(status, ImpactStatus::Revoked(Revocation::IdentityExhausted));
    }
    #[tokio::test]
    async fn poisoned_coordination_never_releases_an_outstanding_attempt() {
        let (session, events, peer) = physical().await;
        let handle = session.handle();
        let owner = RecordImpactOwner::bind(&handle).unwrap();
        let ticket = owner.view().reserve_take().unwrap();
        let attempt = owner.manual_port().begin_at_send(handle.token()).unwrap();
        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _locked = owner.inner.state.lock().unwrap();
            panic!("controlled registry poison");
        }));
        assert!(poison.is_err());
        let result = attempt.not_dispatched();
        let status = ticket.check();
        let admission = owner.view().reserve_take().unwrap_err();
        drop(owner);
        joined(session, events, peer).await;
        assert_eq!(result, Err(ImpactError::Poisoned));
        assert_eq!(status, ImpactStatus::Revoked(Revocation::Poisoned));
        assert_eq!(admission, ImpactError::Poisoned);
    }
}
