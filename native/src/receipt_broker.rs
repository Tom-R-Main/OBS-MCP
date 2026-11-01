// SPDX-License-Identifier: GPL-2.0-only
//! One synchronous receipt writer, shared only through bounded typed scopes.
//! Guards never cross an OBS await. Dropping a port ends publication rights;
//! callers retain each port until its effect and cleanup have actually settled.
use crate::operation_store::{
    Admission, BackendStore, GenericKind, GenericPhase, GenericReceipt, OperationKind,
    OperationReceipt, OperationStore, OperationUpdate, OperationView, PrepareDetails,
    RestoreCounts, RestorePhase, RestoreReceipt, StoreError, StoreMode,
};
use crate::session::SafetyStopRequest;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
const MAX_SCOPES: usize = 34;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeKind {
    Take,
    Stage,
    CriticalRecord,
    Generic(GenericKind),
    SafetyStop(SafetyStopRequest),
    ManualRecordStop,
    RestoreBatch,
}
impl ScopeKind {
    fn is_public_stop(self) -> bool {
        matches!(self, Self::SafetyStop(_) | Self::ManualRecordStop)
    }
}
struct Scope {
    kind: ScopeKind,
    ids: HashSet<String>,
    stop_reserved: bool,
}
struct State {
    store: Box<BackendStore>,
    scopes: HashMap<u64, Scope>,
    next: u64,
    poisoned: bool,
}
struct Shared {
    state: Mutex<State>,
    view: OperationView,
    endpoint: String,
    root: PathBuf,
}
pub struct ReceiptBroker {
    shared: Arc<Shared>,
}
pub(crate) struct ReceiptPort {
    shared: Arc<Shared>,
    id: u64,
}
#[derive(Clone)]
pub struct DispatchFence {
    shared: Arc<Shared>,
    id: u64,
}
impl ReceiptBroker {
    pub fn new(store: OperationStore) -> Result<Self, StoreError> {
        let StoreMode::Direct(store) = store.mode else {
            return Err(StoreError::Conflict);
        };
        let view = store.read_view();
        let endpoint = store.endpoint_hash().into();
        let root = store.state_root().into();
        Ok(Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    store,
                    scopes: HashMap::new(),
                    next: 0,
                    poisoned: false,
                }),
                view,
                endpoint,
                root,
            }),
        })
    }
    pub fn read_view(&self) -> OperationView {
        self.shared.view.clone()
    }
    pub fn admission(&self) -> Result<Admission, StoreError> {
        let state = self
            .shared
            .state
            .lock()
            .map_err(|_| StoreError::Unavailable)?;
        if state.poisoned {
            return Err(StoreError::DurabilityUncertain);
        }
        self.shared.view.admission()
    }
    pub fn scope(&self, kind: ScopeKind) -> Result<OperationStore, StoreError> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| StoreError::Unavailable)?;
        if state.poisoned {
            return Err(StoreError::DurabilityUncertain);
        }
        if state.scopes.len() >= MAX_SCOPES {
            return Err(StoreError::Conflict);
        }
        if kind.is_public_stop() {
            if state
                .scopes
                .values()
                .any(|scope| scope.kind.is_public_stop())
                || safety_admission(&self.shared.view, &mut state, None)?.blocking
            {
                return Err(StoreError::Conflict);
            }
        } else if self.shared.view.admission()?.blocking {
            return Err(StoreError::Conflict);
        }
        if kind == ScopeKind::Take
            && (state
                .scopes
                .values()
                .any(|scope| scope.kind == ScopeKind::Take)
                || self.shared.view.capacity_used()?.saturating_add(2)
                    > crate::operation_store::MAX_RECEIPTS)
        {
            return Err(StoreError::InventoryLimit);
        }
        state.next = state
            .next
            .checked_add(1)
            .ok_or(StoreError::IdentityExhausted)?;
        let id = state.next;
        state.scopes.insert(
            id,
            Scope {
                kind,
                ids: HashSet::new(),
                stop_reserved: kind == ScopeKind::Take,
            },
        );
        Ok(OperationStore {
            mode: StoreMode::Scoped(ReceiptPort {
                shared: self.shared.clone(),
                id,
            }),
        })
    }
    pub fn into_store(self) -> Result<OperationStore, Self> {
        match Arc::try_unwrap(self.shared) {
            Ok(shared) => match shared.state.into_inner() {
                Ok(state) if !state.poisoned => Ok(OperationStore {
                    mode: StoreMode::Direct(state.store),
                }),
                Ok(state) => Err(Self {
                    shared: Arc::new(Shared {
                        state: Mutex::new(state),
                        ..shared
                    }),
                }),
                Err(poison) => {
                    let state = poison.into_inner();
                    Err(Self {
                        shared: Arc::new(Shared {
                            state: Mutex::new(State {
                                poisoned: true,
                                ..state
                            }),
                            ..shared
                        }),
                    })
                }
            },
            Err(shared) => Err(Self { shared }),
        }
    }
}
fn safety_kind(kind: SafetyStopRequest) -> GenericKind {
    match kind {
        SafetyStopRequest::Stream => GenericKind::StreamStop,
        SafetyStopRequest::VirtualCam => GenericKind::VirtualCamStop,
        SafetyStopRequest::ReplayBuffer => GenericKind::ReplayBufferStop,
    }
}
fn scope_matches_generic(scope: ScopeKind, kind: GenericKind) -> bool {
    match scope {
        ScopeKind::Generic(expected) => expected == kind,
        ScopeKind::SafetyStop(expected) => safety_kind(expected) == kind,
        ScopeKind::ManualRecordStop => kind == GenericKind::ManualRecordStop,
        _ => false,
    }
}
fn safety_admission(
    view: &OperationView,
    state: &mut State,
    own: Option<&str>,
) -> Result<Admission, StoreError> {
    let result = {
        let live = state
            .scopes
            .values()
            .flat_map(|scope| scope.ids.iter().map(String::as_str))
            .collect::<HashSet<_>>();
        view.admission_with_live_ids(own, &live)
    };
    // This in-process proof-loss flag survives loan drop and broker reclaim.
    // It is not a new durable marker for deletion after process restart.
    if matches!(&result, Err(error) if !matches!(error, StoreError::Conflict)) {
        state.poisoned = true;
        view.mark_uncertain();
    }
    result
}

impl DispatchFence {
    pub(crate) fn is_restore_batch(&self) -> Result<bool, StoreError> {
        let state = self
            .shared
            .state
            .lock()
            .map_err(|_| StoreError::Unavailable)?;
        if state.poisoned {
            return Err(StoreError::DurabilityUncertain);
        }
        Ok(state.scopes.get(&self.id).ok_or(StoreError::Conflict)?.kind == ScopeKind::RestoreBatch)
    }
    pub(crate) fn is_manual_record_stop(&self) -> Result<bool, StoreError> {
        let state = self
            .shared
            .state
            .lock()
            .map_err(|_| StoreError::Unavailable)?;
        if state.poisoned {
            return Err(StoreError::DurabilityUncertain);
        }
        Ok(state.scopes.get(&self.id).ok_or(StoreError::Conflict)?.kind
            == ScopeKind::ManualRecordStop)
    }
    pub(crate) fn safety_stop_kind(&self) -> Result<Option<SafetyStopRequest>, StoreError> {
        let state = self
            .shared
            .state
            .lock()
            .map_err(|_| StoreError::Unavailable)?;
        if state.poisoned {
            return Err(StoreError::DurabilityUncertain);
        }
        let scope = state.scopes.get(&self.id).ok_or(StoreError::Conflict)?;
        Ok(match scope.kind {
            ScopeKind::SafetyStop(kind) => Some(kind),
            _ => None,
        })
    }
    pub(crate) fn admission(&self) -> Result<Admission, StoreError> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| StoreError::Unavailable)?;
        if state.poisoned {
            return Err(StoreError::DurabilityUncertain);
        }
        let scope = state.scopes.get(&self.id).ok_or(StoreError::Conflict)?;
        if scope.kind.is_public_stop() {
            safety_admission(&self.shared.view, &mut state, None)
        } else {
            self.shared.view.admission()
        }
    }
    /// Ordinary scopes exclude only their own durable Dispatching intent.
    /// Closed public-stop scopes (including the manual Record library scope)
    /// may also exclude exact currently retained live loans in known Dispatching
    /// phases (or V1 Accepted), rechecked under the
    /// writer lock. Unknown, orphaned, missing or poisoned proof never qualifies.
    /// Exclusions establish neither an observed postcondition nor replay rights.
    pub fn permits(&self, own: &str) -> Result<bool, StoreError> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| StoreError::Unavailable)?;
        if state.poisoned {
            return Err(StoreError::DurabilityUncertain);
        }
        if !state
            .scopes
            .get(&self.id)
            .is_some_and(|scope| scope.ids.contains(own))
        {
            return Err(StoreError::Conflict);
        }
        if state.scopes[&self.id].kind.is_public_stop() {
            Ok(!safety_admission(&self.shared.view, &mut state, Some(own))?.blocking)
        } else {
            Ok(self.shared.view.is_dispatching(own)?
                && !self.shared.view.admission_except(Some(own))?.blocking)
        }
    }
}
impl ReceiptPort {
    pub(crate) fn endpoint_hash(&self) -> &str {
        &self.shared.endpoint
    }
    pub(crate) fn state_root(&self) -> &Path {
        &self.shared.root
    }
    pub(crate) fn read_view(&self) -> OperationView {
        self.shared.view.clone()
    }
    pub(crate) fn fence(&self) -> DispatchFence {
        DispatchFence {
            shared: self.shared.clone(),
            id: self.id,
        }
    }
    fn apply<T>(
        &self,
        action: impl FnOnce(&mut State) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| StoreError::Unavailable)?;
        let result = action(&mut state);
        if result.is_err()
            && (matches!(
                result,
                Err(StoreError::DurabilityUncertain
                    | StoreError::Unavailable
                    | StoreError::UnsafePath
                    | StoreError::Corrupt)
            ) || self.shared.view.admission().is_err())
        {
            state.poisoned = true;
            self.shared.view.mark_uncertain();
        }
        result
    }
    pub(crate) fn prepare(
        &mut self,
        id: &str,
        kind: OperationKind,
        generation: u64,
        utc: &str,
        details: PrepareDetails<'_>,
    ) -> Result<OperationReceipt, StoreError> {
        self.apply(|state| {
            let scope = state.scopes.get(&self.id).ok_or(StoreError::Conflict)?;
            if !matches!(
                (scope.kind, kind),
                (
                    ScopeKind::Take | ScopeKind::CriticalRecord,
                    OperationKind::RecordStart | OperationKind::RecordStop
                ) | (
                    ScopeKind::Stage,
                    OperationKind::StageScene | OperationKind::TakeScene
                )
            ) {
                return Err(StoreError::Conflict);
            }
            let stop = kind == OperationKind::RecordStop;
            if state.poisoned && !stop {
                return Err(StoreError::DurabilityUncertain);
            }
            let reserved = state
                .scopes
                .values()
                .filter(|scope| scope.stop_reserved)
                .count();
            let own_reserved = usize::from(stop && scope.stop_reserved);
            let restarting = scope.kind == ScopeKind::Take
                && kind == OperationKind::RecordStart
                && !scope.stop_reserved;
            if self
                .shared
                .view
                .capacity_used()?
                .saturating_add(reserved)
                .saturating_add(1 - own_reserved + usize::from(restarting))
                > crate::operation_store::MAX_RECEIPTS
            {
                return Err(StoreError::InventoryLimit);
            }
            if scope.ids.len() >= crate::operation_store::MAX_RECEIPTS {
                return Err(StoreError::InventoryLimit);
            }
            let receipt = state.store.prepare(id, kind, generation, utc, details)?;
            let scope = state.scopes.get_mut(&self.id).unwrap();
            scope.ids.insert(id.into());
            if stop {
                scope.stop_reserved = false;
            } else if restarting {
                scope.stop_reserved = true;
            }
            Ok(receipt)
        })
    }
    pub(crate) fn update(
        &mut self,
        receipt: &OperationReceipt,
        update: OperationUpdate,
        utc: &str,
    ) -> Result<OperationReceipt, StoreError> {
        self.apply(|state| {
            if !state
                .scopes
                .get(&self.id)
                .is_some_and(|scope| scope.ids.contains(receipt.operation_id()))
            {
                return Err(StoreError::Conflict);
            }
            state.store.update(receipt, update, utc)
        })
    }
    pub(crate) fn prepare_generic(
        &mut self,
        id: &str,
        kind: GenericKind,
        generation: u64,
        utc: &str,
    ) -> Result<GenericReceipt, StoreError> {
        self.apply(|state| {
            if !state
                .scopes
                .get(&self.id)
                .is_some_and(|scope| scope_matches_generic(scope.kind, kind))
            {
                return Err(StoreError::Conflict);
            }
            if state.poisoned {
                return Err(StoreError::DurabilityUncertain);
            }
            if state.scopes[&self.id].kind.is_public_stop()
                && safety_admission(&self.shared.view, state, None)?.blocking
            {
                return Err(StoreError::Conflict);
            }
            let reserved = state
                .scopes
                .values()
                .filter(|scope| scope.stop_reserved)
                .count();
            if self
                .shared
                .view
                .capacity_used()?
                .saturating_add(reserved)
                .saturating_add(1)
                > crate::operation_store::MAX_RECEIPTS
            {
                return Err(StoreError::InventoryLimit);
            }
            if state.scopes[&self.id].ids.len() >= crate::operation_store::MAX_RECEIPTS {
                return Err(StoreError::InventoryLimit);
            }
            let receipt = state.store.prepare_generic(id, kind, generation, utc)?;
            state
                .scopes
                .get_mut(&self.id)
                .unwrap()
                .ids
                .insert(id.into());
            Ok(receipt)
        })
    }
    pub(crate) fn update_generic(
        &mut self,
        receipt: &GenericReceipt,
        phase: GenericPhase,
        utc: &str,
    ) -> Result<GenericReceipt, StoreError> {
        self.apply(|state| {
            if !state.scopes.get(&self.id).is_some_and(|scope| {
                scope_matches_generic(scope.kind, receipt.kind())
                    && scope.ids.contains(receipt.operation_id())
            }) {
                return Err(StoreError::Conflict);
            }
            state.store.update_generic(receipt, phase, utc)
        })
    }
    pub(crate) fn prepare_restore(
        &mut self,
        id: &str,
        generation: u64,
        request_count: u64,
        utc: &str,
    ) -> Result<RestoreReceipt, StoreError> {
        self.apply(|state| {
            let scope = state.scopes.get(&self.id).ok_or(StoreError::Conflict)?;
            if scope.kind != ScopeKind::RestoreBatch {
                return Err(StoreError::Conflict);
            }
            if state.poisoned {
                return Err(StoreError::DurabilityUncertain);
            }
            let reserved = state.scopes.values().filter(|s| s.stop_reserved).count();
            if self
                .shared
                .view
                .capacity_used()?
                .saturating_add(reserved)
                .saturating_add(1)
                > crate::operation_store::MAX_RECEIPTS
                || scope.ids.len() >= crate::operation_store::MAX_RECEIPTS
            {
                return Err(StoreError::InventoryLimit);
            }
            let receipt = state
                .store
                .prepare_restore(id, generation, request_count, utc)?;
            state
                .scopes
                .get_mut(&self.id)
                .unwrap()
                .ids
                .insert(id.into());
            Ok(receipt)
        })
    }
    pub(crate) fn update_restore(
        &mut self,
        receipt: &RestoreReceipt,
        phase: RestorePhase,
        counts: Option<RestoreCounts>,
        utc: &str,
    ) -> Result<RestoreReceipt, StoreError> {
        self.apply(|state| {
            if !state.scopes.get(&self.id).is_some_and(|s| {
                s.kind == ScopeKind::RestoreBatch && s.ids.contains(receipt.operation_id())
            }) {
                return Err(StoreError::Conflict);
            }
            state.store.update_restore(receipt, phase, counts, utc)
        })
    }
}
impl Drop for ReceiptPort {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.scopes.remove(&self.id);
        }
    }
}
