// SPDX-License-Identifier: GPL-2.0-only
//! Bounded first-terminal projections from the existing sole event observer.
//! No socket, event consumer, task, RPC or mutation authority is created here.
//! The byte pool charges owned UTF-8 path copies, including consumed events.
//! It excludes the original parsed event, caller copies, formatting and RSS.
//! Lock order: EventHub take-slot -> registry state -> terminal cell. Physical
//! receipt advances an atomic cursor; registration reads it under registry state
//! without locking Publisher. Revocation never acquires a publisher/Hub lock.
//! Receive releases cell/watch guards before unregistering; no lock crosses await.
use crate::session::{ConnectionToken, ObsEvent, SessionHandle};
use std::{
    fmt,
    sync::{Arc, Mutex, Weak},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, watch},
    time::{Instant, timeout_at},
};

pub const MAX_SUBSCRIPTIONS: usize = 32;
pub const PATH_BYTES: usize = 8 * 1024 * 1024;
const OUTPUT_EVENTS: u64 = 1 << 6;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputKind {
    Record,
    Stream,
    VirtualCam,
    ReplayBuffer,
}
impl OutputKind {
    pub fn event_name(self) -> &'static str {
        match self {
            Self::Record => "RecordStateChanged",
            Self::Stream => "StreamStateChanged",
            Self::VirtualCam => "VirtualcamStateChanged",
            Self::ReplayBuffer => "ReplayBufferStateChanged",
        }
    }
    fn from_event(name: &str) -> Option<Self> {
        [
            Self::Record,
            Self::Stream,
            Self::VirtualCam,
            Self::ReplayBuffer,
        ]
        .into_iter()
        .find(|kind| kind.event_name() == name)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Terminal {
    Started,
    Stopped,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputEventError {
    Capacity,
    ByteCapacity,
    PathTooLarge,
    IdentityExhausted,
    IncompleteEvents,
    Unavailable,
    Revoked,
    ConfirmationExpired,
    SettlementUnavailable,
}
impl fmt::Display for OutputEventError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "Output observation {:?}", self)
    }
}
impl std::error::Error for OutputEventError {}

struct Path {
    text: String,
    _permit: OwnedSemaphorePermit,
}
/// One observed transition, not request acceptance, output ownership or a file
/// existence claim. Non-Clone: its path byte lease moves with the last use.
pub struct OutputEvent {
    connection: ConnectionToken,
    sequence: u64,
    received_at: Instant,
    terminal: Terminal,
    path: Option<Path>,
}
impl OutputEvent {
    pub fn connection(&self) -> ConnectionToken {
        self.connection
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn received_at(&self) -> Instant {
        self.received_at
    }
    pub fn terminal(&self) -> Terminal {
        self.terminal
    }
    pub fn output_path(&self) -> Option<&str> {
        self.path.as_ref().map(|path| path.text.as_str())
    }
}
impl fmt::Debug for OutputEvent {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("OutputEvent")
            .field("connection", &self.connection)
            .field("sequence", &self.sequence)
            .field("terminal", &self.terminal)
            .field(
                "path_bytes",
                &self.path.as_ref().map(|path| path.text.len()),
            )
            .finish_non_exhaustive()
    }
}
enum Outcome {
    Pending,
    Ready(Result<OutputEvent, OutputEventError>),
    Consumed,
}
struct Cell {
    outcome: Mutex<Outcome>,
}
impl Cell {
    fn take(&self) -> Result<Option<Result<OutputEvent, OutputEventError>>, OutputEventError> {
        let mut state = self
            .outcome
            .lock()
            .map_err(|_| OutputEventError::Unavailable)?;
        if !matches!(*state, Outcome::Ready(_)) {
            return Ok(None);
        }
        match std::mem::replace(&mut *state, Outcome::Consumed) {
            Outcome::Ready(value) => Ok(Some(value)),
            _ => unreachable!("ready outcome checked under the same lock"),
        }
    }
}
struct Slot {
    id: u64,
    kind: OutputKind,
    after: u64,
    registered_at: Instant,
    confirmation_deadline: Instant,
    cell: Arc<Cell>,
}
struct State {
    active: bool,
    next_id: Option<u64>,
    slots: [Option<Slot>; MAX_SUBSCRIPTIONS],
}
impl State {
    fn identity(&mut self) -> Result<u64, OutputEventError> {
        let id = self.next_id.ok_or(OutputEventError::IdentityExhausted)?;
        self.next_id = id.checked_add(1);
        Ok(id)
    }
}
pub(crate) struct Registry {
    token: ConnectionToken,
    state: Mutex<State>,
    paths: Arc<Semaphore>,
    progress: watch::Sender<u64>,
}
impl Registry {
    pub(crate) fn new(token: ConnectionToken) -> Self {
        let (progress, _) = watch::channel(0);
        Self {
            token,
            state: Mutex::new(State {
                active: true,
                next_id: Some(1),
                slots: std::array::from_fn(|_| None),
            }),
            paths: Arc::new(Semaphore::new(PATH_BYTES)),
            progress,
        }
    }
    pub(crate) fn view(
        self: &Arc<Self>,
        handle: SessionHandle,
    ) -> Result<OutputEvents, OutputEventError> {
        if handle.token() != self.token || handle.invalidation().is_some() {
            return Err(OutputEventError::Revoked);
        }
        if handle.event_subscriptions() & OUTPUT_EVENTS != OUTPUT_EVENTS {
            return Err(OutputEventError::IncompleteEvents);
        }
        if !self
            .state
            .lock()
            .map_err(|_| OutputEventError::Unavailable)?
            .active
        {
            return Err(OutputEventError::Revoked);
        }
        Ok(OutputEvents {
            registry: Arc::downgrade(self),
            handle,
        })
    }
    /// Called only by EventHub before the original event moves into its take
    /// feed. Registration and projection serialize on the same registry lock.
    pub(crate) fn publish(&self, event: &ObsEvent) {
        let Ok(mut state) = self.state.lock() else {
            self.progress.send_modify(|_| {});
            return;
        };
        if !state.active || event.connection != self.token {
            return;
        }
        if let Some(kind) = OutputKind::from_event(&event.event_type)
            && let Some(terminal) = event
                .data
                .as_ref()
                .and_then(|data| data.get("outputState"))
                .and_then(|state| state.as_str())
                .and_then(|state| match state {
                    "OBS_WEBSOCKET_OUTPUT_STARTED" => Some(Terminal::Started),
                    "OBS_WEBSOCKET_OUTPUT_STOPPED" => Some(Terminal::Stopped),
                    _ => None,
                })
        {
            let path = (terminal == Terminal::Started)
                .then(|| {
                    event
                        .data
                        .as_ref()
                        .and_then(|data| data.get("outputPath"))
                        .and_then(|path| path.as_str())
                })
                .flatten();
            for slot in state.slots.iter_mut().flatten().filter(|slot| {
                slot.kind == kind
                    && event.sequence > slot.after
                    && event.received_at() >= slot.registered_at
                    && event.received_at() <= slot.confirmation_deadline
            }) {
                let Ok(mut outcome) = slot.cell.outcome.lock() else {
                    continue;
                };
                if !matches!(*outcome, Outcome::Pending) {
                    continue;
                }
                let selected = path
                    .map(|text| {
                        if text.len() > PATH_BYTES {
                            return Err(OutputEventError::PathTooLarge);
                        }
                        let permit = self
                            .paths
                            .clone()
                            .try_acquire_many_owned(text.len() as u32)
                            .map_err(|_| OutputEventError::ByteCapacity)?;
                        Ok(Path {
                            text: text.to_owned(),
                            _permit: permit,
                        })
                    })
                    .transpose();
                *outcome = Outcome::Ready(selected.map(|path| OutputEvent {
                    connection: event.connection,
                    sequence: event.sequence,
                    received_at: event.received_at(),
                    terminal,
                    path,
                }));
            }
        }
        // Even an irrelevant event advances the projection watermark. This is
        // independent of, and never waits for, take application.
        self.progress.send_replace(event.sequence);
    }
    pub(crate) fn revoke(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.active = false;
            for slot in state.slots.iter_mut().flatten() {
                if let Ok(mut outcome) = slot.cell.outcome.lock()
                    && matches!(*outcome, Outcome::Pending)
                {
                    *outcome = Outcome::Ready(Err(OutputEventError::Revoked));
                }
            }
        }
        // First terminal publication wins over later revocation. Pending
        // subscriptions become revoked; neither can be overwritten afterward.
        self.progress.send_modify(|_| {});
    }
    fn unregister(&self, index: usize, id: u64) {
        if let Ok(mut state) = self.state.lock()
            && state.slots[index]
                .as_ref()
                .is_some_and(|slot| slot.id == id)
        {
            state.slots[index] = None;
        }
    }
}

/// Non-owning clone: keeping it cannot keep the observation producer alive or
/// restore admission after owner invalidation. Offers no RPC or writer API.
#[derive(Clone)]
pub struct OutputEvents {
    registry: Weak<Registry>,
    handle: SessionHandle,
}
impl fmt::Debug for OutputEvents {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("OutputEvents")
            .field("connection", &self.handle.token())
            .finish_non_exhaustive()
    }
}
impl OutputEvents {
    /// Intrinsic identity of this non-owning provider; no publication rights.
    pub fn connection_token(&self) -> ConnectionToken {
        self.handle.token()
    }
    /// Eligibility is fixed before dispatch. Events beyond this immutable
    /// confirmation clock never copy a path, spend budget or report a path-projection error.
    pub fn subscribe(
        &self,
        kind: OutputKind,
        confirmation_deadline: Instant,
    ) -> Result<OutputSubscription, OutputEventError> {
        let registry = self.registry.upgrade().ok_or(OutputEventError::Revoked)?;
        let mut state = registry
            .state
            .lock()
            .map_err(|_| OutputEventError::Unavailable)?;
        if !state.active
            || self.handle.invalidation().is_some()
            || self.handle.token() != registry.token
        {
            return Err(OutputEventError::Revoked);
        }
        let index = state
            .slots
            .iter()
            .position(Option::is_none)
            .ok_or(OutputEventError::Capacity)?;
        let id = state.identity()?;
        // Capture intrinsic receipt, not a lagging bridge-published cursor.
        let after = self.handle.event_cursor();
        let registered_at = Instant::now();
        let cell = Arc::new(Cell {
            outcome: Mutex::new(Outcome::Pending),
        });
        state.slots[index] = Some(Slot {
            id,
            kind,
            after,
            registered_at,
            confirmation_deadline,
            cell: cell.clone(),
        });
        Ok(OutputSubscription {
            registry: Arc::downgrade(&registry),
            handle: self.handle.clone(),
            progress: registry.progress.subscribe(),
            confirmation_deadline,
            cell,
            index,
            id,
        })
    }
}

/// One registered first-terminal waiter. Consuming/dropping it unregisters its
/// fixed slot. A consumed OutputEvent independently retains any path byte lease.
pub struct OutputSubscription {
    registry: Weak<Registry>,
    handle: SessionHandle,
    progress: watch::Receiver<u64>,
    confirmation_deadline: Instant,
    cell: Arc<Cell>,
    index: usize,
    id: u64,
}
impl fmt::Debug for OutputSubscription {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("OutputSubscription")
            .field("id", &self.id)
            .field("connection", &self.handle.token())
            .finish_non_exhaustive()
    }
}
impl OutputSubscription {
    /// Confirmation and settlement are distinct absolute clocks. At expiry,
    /// capture the intrinsic cursor once and drain only that projection prefix
    /// under the original settlement bound. An early terminal may be projected
    /// after the confirmation clock; failure to settle is not a false timeout.
    /// No take-applied fence or RPC is involved. Already known terminal outcomes
    /// are inspected first, including when an ACK delayed this initial poll.
    pub async fn receive(
        mut self,
        settlement_deadline: Instant,
    ) -> Result<OutputEvent, OutputEventError> {
        let confirmation_deadline = self.confirmation_deadline;
        let mut cutoff = None;
        loop {
            if let Some(result) = self.cell.take()? {
                return result;
            }
            if self.handle.invalidation().is_some() || self.registry.upgrade().is_none() {
                return Err(OutputEventError::Revoked);
            }
            let now = Instant::now();
            if now >= confirmation_deadline {
                let cursor = *cutoff.get_or_insert_with(|| self.handle.event_cursor());
                let projected = *self.progress.borrow();
                if projected >= cursor {
                    // Projection precedes watermark publication; close the race
                    // between the first cell check and the watermark acquire.
                    if let Some(result) = self.cell.take()? {
                        return result;
                    }
                    return Err(OutputEventError::ConfirmationExpired);
                }
            }
            if now >= settlement_deadline {
                return Err(OutputEventError::SettlementUnavailable);
            }
            let deadline = if cutoff.is_some() {
                settlement_deadline
            } else {
                confirmation_deadline.min(settlement_deadline)
            };
            match timeout_at(deadline, self.progress.changed()).await {
                Ok(Ok(())) | Err(_) => {}
                Ok(Err(_)) => {
                    if let Some(result) = self.cell.take()? {
                        return result;
                    }
                    return Err(OutputEventError::Revoked);
                }
            }
        }
    }
}
impl Drop for OutputSubscription {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.unregister(self.index, self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exhausted_subscription_identity_never_wraps_or_restarts() {
        let mut state = State {
            active: true,
            next_id: Some(u64::MAX),
            slots: std::array::from_fn(|_| None),
        };
        assert_eq!(state.identity().unwrap(), u64::MAX);
        assert_eq!(
            state.identity().unwrap_err(),
            OutputEventError::IdentityExhausted
        );
        assert_eq!(
            state.identity().unwrap_err(),
            OutputEventError::IdentityExhausted
        );
        assert_eq!(state.next_id, None);
    }
}
