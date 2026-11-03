// SPDX-License-Identifier: GPL-2.0-only
//! Bounded saved-file observations from the sole physical OBS event owner.
//! A filename event has no request identity and never proves attribution.
use crate::session::{ConnectionToken, ObsEvent, SessionHandle};
use std::sync::{Arc, Mutex, Weak};
use tokio::{
    sync::watch,
    time::{Instant, timeout_at},
};

const SLOTS: usize = 8;
const MAX_PATH: usize = 4096;
struct Cell {
    armed: Option<(u64, Instant)>,
    seen: u8,
    path: Option<String>,
    issue: Option<&'static str>,
}
struct Slot {
    id: u64,
    deadline: Instant,
    cell: Arc<Mutex<Cell>>,
}
struct State {
    active: bool,
    next: Option<u64>,
    slots: [Option<Slot>; SLOTS],
}
pub(crate) struct Registry {
    token: ConnectionToken,
    state: Mutex<State>,
    progress: watch::Sender<u64>,
}
impl Registry {
    pub(crate) fn new(token: ConnectionToken) -> Self {
        let (progress, _) = watch::channel(0);
        Self {
            token,
            state: Mutex::new(State {
                active: true,
                next: Some(1),
                slots: std::array::from_fn(|_| None),
            }),
            progress,
        }
    }
    pub(crate) fn view(self: &Arc<Self>, handle: SessionHandle) -> Result<ReplayEvents, String> {
        if handle.token() != self.token
            || handle.invalidation().is_some()
            || handle.event_subscriptions() & (1 << 6) == 0
        {
            return Err("Replay saved-event observation is unavailable".into());
        }
        Ok(ReplayEvents {
            registry: Arc::downgrade(self),
            handle,
        })
    }
    pub(crate) fn publish(&self, event: &ObsEvent) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if !state.active || event.connection != self.token {
            return;
        }
        if event.event_type == "ReplayBufferSaved" {
            for slot in state.slots.iter_mut().flatten() {
                let Ok(mut cell) = slot.cell.lock() else {
                    continue;
                };
                let Some((after, armed)) = cell.armed else {
                    continue;
                };
                if event.sequence <= after
                    || event.received_at() < armed
                    || event.received_at() > slot.deadline
                {
                    continue;
                }
                cell.seen = cell.seen.saturating_add(1).min(2);
                if cell.seen > 1 {
                    cell.path.take();
                    cell.issue =
                        Some("Multiple replay-save events make this observation ambiguous");
                } else {
                    match event
                        .data
                        .as_ref()
                        .and_then(|data| data.get("savedReplayPath"))
                        .and_then(|path| path.as_str())
                    {
                        Some(path)
                            if !path.is_empty()
                                && path.len() <= MAX_PATH
                                && !path.contains('\0') =>
                        {
                            cell.path = Some(path.to_owned())
                        }
                        _ => {
                            cell.issue = Some("Replay saved event has an invalid or oversized path")
                        }
                    }
                }
            }
        }
        self.progress.send_replace(event.sequence);
    }
    pub(crate) fn revoke(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.active = false;
            for slot in state.slots.iter_mut().flatten() {
                if let Ok(mut cell) = slot.cell.lock() {
                    cell.issue = Some("Replay observation connection ended");
                }
            }
        }
        self.progress.send_modify(|_| {});
    }
}
#[derive(Clone)]
pub struct ReplayEvents {
    registry: Weak<Registry>,
    handle: SessionHandle,
}
pub struct Subscription {
    registry: Weak<Registry>,
    handle: SessionHandle,
    index: usize,
    id: u64,
    cell: Arc<Mutex<Cell>>,
    deadline: Instant,
}
impl ReplayEvents {
    pub fn subscribe(&self, deadline: Instant) -> Result<Arc<Subscription>, String> {
        let registry = self.registry.upgrade().ok_or("Replay observer ended")?;
        let mut state = registry
            .state
            .lock()
            .map_err(|_| "Replay observer unavailable")?;
        if !state.active || self.handle.invalidation().is_some() || Instant::now() >= deadline {
            return Err("Replay observer is not live".into());
        }
        let index = state
            .slots
            .iter()
            .position(Option::is_none)
            .ok_or("Replay observation capacity exhausted")?;
        let id = state.next.ok_or("Replay observation identity exhausted")?;
        state.next = id.checked_add(1);
        let cell = Arc::new(Mutex::new(Cell {
            armed: None,
            seen: 0,
            path: None,
            issue: None,
        }));
        state.slots[index] = Some(Slot {
            id,
            deadline,
            cell: cell.clone(),
        });
        Ok(Arc::new(Subscription {
            registry: Arc::downgrade(&registry),
            handle: self.handle.clone(),
            index,
            id,
            cell,
            deadline,
        }))
    }
}
impl Subscription {
    /// Called synchronously inside the socket's final dispatch guard. Queued
    /// pre-arm events are excluded by the physical cursor, even if unpublished.
    pub fn arm(&self) -> Result<(), String> {
        let registry = self.registry.upgrade().ok_or("Replay observer ended")?;
        let state = registry
            .state
            .lock()
            .map_err(|_| "Replay observer unavailable")?;
        if !state.active || self.handle.invalidation().is_some() || Instant::now() >= self.deadline
        {
            return Err("Replay observer is not live".into());
        }
        let mut cell = self
            .cell
            .lock()
            .map_err(|_| "Replay observation unavailable")?;
        if cell.armed.is_some() {
            return Err("Replay observation was already armed; save cannot be replayed".into());
        }
        cell.armed = Some((self.handle.event_cursor(), Instant::now()));
        Ok(())
    }
    pub fn observed(&self) -> bool {
        self.cell.lock().is_ok_and(|cell| cell.seen > 0)
    }
    pub async fn candidate(&self) -> Result<String, String> {
        let registry = self.registry.upgrade().ok_or("Replay observer ended")?;
        let mut progress = registry.progress.subscribe();
        loop {
            if self.handle.invalidation().is_some() || Instant::now() >= self.deadline {
                return Err(
                    "Replay saved-event confirmation did not complete before the original deadline"
                        .into(),
                );
            }
            {
                let cell = self
                    .cell
                    .lock()
                    .map_err(|_| "Replay observation unavailable")?;
                if let Some(issue) = cell.issue {
                    return Err(issue.into());
                }
                if let Some(path) = &cell.path {
                    return Ok(path.clone());
                }
            }
            timeout_at(self.deadline, progress.changed())
                .await
                .map_err(|_| "Replay saved-event confirmation timed out")?
                .map_err(|_| "Replay observer ended")?;
        }
    }
    /// Drain events already received by the physical session, then recheck the
    /// same bounded cell. No new consumer, polling RPC or effect is admitted.
    pub async fn confirm(&self, expected: &str) -> Result<(), String> {
        let registry = self.registry.upgrade().ok_or("Replay observer ended")?;
        let cursor = self.handle.event_cursor();
        let mut progress = registry.progress.subscribe();
        while *progress.borrow_and_update() < cursor {
            timeout_at(self.deadline, progress.changed())
                .await
                .map_err(|_| "Replay observer barrier timed out")?
                .map_err(|_| "Replay observer ended")?;
        }
        if self.candidate().await? != expected {
            return Err("Replay saved-file observation changed".into());
        }
        Ok(())
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade()
            && let Ok(mut state) = registry.state.lock()
            && state.slots[self.index]
                .as_ref()
                .is_some_and(|slot| slot.id == self.id)
        {
            state.slots[self.index] = None;
        }
    }
}
