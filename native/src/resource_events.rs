// SPDX-License-Identifier: GPL-2.0-only
//! Bounded session notification lifetime. Publishers retain no OBS frame,
//! socket, Service, Take, journal, or stdout ownership.
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

const MAX_LISTENERS: usize = 1024;
const MAX_IDS: usize = 256 * 1024;
const MAX_ID: usize = 1024;
const MAX_URIS: usize = 4096;
const MAX_URI: usize = 256 * 1024;
const MAX_URI_BYTES: usize = 1024 * 1024;
const QUEUE_ITEMS: usize = 64;
const QUEUE_BYTES: usize = 8 * 1024 * 1024;
const MAX_SCENE_NAME: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    SubscriptionLimit,
    Capacity,
    InvalidFilter,
    Unavailable,
}
impl Failure {
    pub fn message(self) -> &'static str {
        match self {
            Self::SubscriptionLimit => "Subscription limit reached",
            Self::Capacity => {
                "Native resource notification capacity exceeded; session publication is unavailable"
            }
            Self::InvalidFilter => {
                "Invalid params: 'notifications' is required and must be a valid SubscriptionFilter"
            }
            Self::Unavailable => "Native resource notification owner is unavailable",
        }
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for Failure {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    Updated(String),
    ListChanged,
}

fn component(value: &str) -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut result = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            result.push(char::from(byte));
        } else {
            result.push('%');
            result.push(char::from(HEX[usize::from(byte >> 4)]));
            result.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
    result
}
fn items(value: Option<&str>) -> Result<Option<Change>, Failure> {
    value
        .map(|name| {
            if name.len() > MAX_SCENE_NAME {
                return Err(Failure::Capacity);
            }
            Ok(Change::Updated(format!(
                "obs://scene/{}/items",
                component(name)
            )))
        })
        .transpose()
}
/// Actual Source callback order, including old-name invalidation after list change.
pub fn project(event: &str, data: &Value) -> Result<Vec<Change>, Failure> {
    project_selected(event, data, true)
}
fn project_selected(event: &str, data: &Value, scene_items: bool) -> Result<Vec<Change>, Failure> {
    // Unknown high-volume events must not allocate a notification vector.
    let mut result = Vec::new();
    if matches!(
        event,
        "RecordStateChanged"
            | "StreamStateChanged"
            | "ReplayBufferStateChanged"
            | "VirtualcamStateChanged"
            | "CurrentProgramSceneChanged"
            | "CurrentProfileChanged"
            | "CurrentSceneCollectionChanged"
            | "CurrentPreviewSceneChanged"
            | "StudioModeStateChanged"
            | "VideoSettingsChanged"
            | "CanvasCreated"
            | "CanvasRemoved"
            | "SceneNameChanged"
    ) {
        result.push(Change::Updated("obs://status".into()));
    }
    if matches!(
        event,
        "SceneCreated"
            | "SceneRemoved"
            | "SceneNameChanged"
            | "SceneListChanged"
            | "CurrentProgramSceneChanged"
            | "CurrentPreviewSceneChanged"
            | "CurrentSceneCollectionChanged"
    ) {
        result.push(Change::Updated("obs://scenes".into()));
    }
    if matches!(
        event,
        "SceneItemCreated"
            | "SceneItemRemoved"
            | "SceneItemListReindexed"
            | "SceneItemEnableStateChanged"
            | "SceneItemLockStateChanged"
    ) && scene_items
        && let Some(change) = items(data.get("sceneName").and_then(Value::as_str))?
    {
        result.push(change);
    }
    if matches!(event, "SceneCreated" | "SceneRemoved" | "SceneNameChanged") {
        result.push(Change::ListChanged);
    }
    if event == "SceneNameChanged"
        && scene_items
        && let Some(change) = items(data.get("oldSceneName").and_then(Value::as_str))?
    {
        result.push(change);
    }
    Ok(result)
}

fn encoded_len(value: &Value, limit: usize) -> Result<usize, Failure> {
    struct Count {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Count {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(b.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| std::io::Error::other("identity capacity"))?;
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count { bytes: 0, limit };
    serde_json::to_writer(&mut count, value).map_err(|_| Failure::Capacity)?;
    Ok(count.bytes)
}
#[derive(Clone)]
struct Listener {
    id: Value,
    tools: bool,
    list: bool,
    uris: Vec<String>,
    id_bytes: usize,
    uri_bytes: usize,
}
struct Routing {
    revision: u64,
    legacy: bool,
    tools: bool,
    listeners: Vec<Listener>,
    legacy_uris: HashSet<String>,
    id_bytes: usize,
    uri_bytes: usize,
    uri_entries: usize,
}
struct Inner {
    active: AtomicBool,
    routing: Mutex<Routing>,
    sender: mpsc::Sender<Batch>,
    bytes: Arc<Semaphore>,
    failure: watch::Sender<Option<Failure>>,
    direct: Mutex<Option<Weak<dyn DeliverySink>>>,
}
/// A weak publisher cannot prolong notification admission or any physical owner.
#[derive(Clone, Default)]
pub struct Publisher {
    inner: Weak<Inner>,
}
pub struct Owner {
    inner: Arc<Inner>,
    receiver: mpsc::Receiver<Batch>,
    failure: watch::Receiver<Option<Failure>>,
    failed: bool,
}
pub struct ListenPlan {
    revision: u64,
    listener: Listener,
    prior: Option<usize>,
    honored: Value,
}
impl ListenPlan {
    pub fn honored(&self) -> &Value {
        &self.honored
    }
}
pub struct UriPlan {
    revision: u64,
    uri: String,
    add: bool,
}

/// One Source callback publication, with recipients frozen at event application.
pub struct Delivery {
    pub change: Change,
    pub audiences: Vec<Option<Value>>,
}
/// URI/identity payload budget remains charged until stdout encoding is reserved.
pub struct Batch {
    pub deliveries: Vec<Delivery>,
    _bytes: OwnedSemaphorePermit,
}

/// A bounded frontend publication route. It owns no OBS/Service authority and
/// must never block awaiting a consumer. The Publisher retains it only weakly.
pub trait DeliverySink: Send + Sync {
    fn deliver(&self, batch: Batch);
}
#[derive(Clone)]
pub struct CancelPort {
    inner: Weak<Inner>,
}
impl CancelPort {
    pub fn cancel(&self, id: &Value) -> Result<bool, Failure> {
        let inner = self.inner.upgrade().ok_or(Failure::Unavailable)?;
        cancel_inner(&inner, id)
    }
}
fn cancel_inner(inner: &Inner, id: &Value) -> Result<bool, Failure> {
    let mut state = inner.routing.lock().map_err(|_| Failure::Unavailable)?;
    let Some(i) = state.listeners.iter().position(|s| s.id == *id) else {
        return Ok(false);
    };
    state.revision = state.revision.checked_add(1).ok_or(Failure::Unavailable)?;
    let old = state.listeners.remove(i);
    state.id_bytes -= old.id_bytes;
    state.uri_bytes -= old.uri_bytes;
    state.uri_entries -= old.uris.len();
    Ok(true)
}
impl Owner {
    pub fn new(tools: bool) -> Self {
        let (sender, receiver) = mpsc::channel(QUEUE_ITEMS);
        let (failed, failure) = watch::channel(None);
        let inner = Arc::new(Inner {
            active: AtomicBool::new(true),
            routing: Mutex::new(Routing {
                revision: 0,
                legacy: false,
                tools,
                listeners: vec![],
                legacy_uris: HashSet::new(),
                id_bytes: 0,
                uri_bytes: 0,
                uri_entries: 0,
            }),
            sender,
            bytes: Arc::new(Semaphore::new(QUEUE_BYTES)),
            failure: failed,
            direct: Mutex::new(None),
        });
        Self {
            inner,
            receiver,
            failure,
            failed: false,
        }
    }
    /// Install once, before registering listeners or binding an observation
    /// bridge. HTTP can publish independently of a held request's host borrow.
    pub fn bind_delivery(&self, sink: &Arc<dyn DeliverySink>) -> Result<(), Failure> {
        let state = self
            .inner
            .routing
            .lock()
            .map_err(|_| Failure::Unavailable)?;
        if !self.inner.active.load(Ordering::Acquire)
            || !state.listeners.is_empty()
            || !state.legacy_uris.is_empty()
            || state.legacy
            || state.tools
        {
            return Err(Failure::Unavailable);
        }
        let mut direct = self.inner.direct.lock().map_err(|_| Failure::Unavailable)?;
        if direct.is_some() {
            return Err(Failure::Unavailable);
        }
        *direct = Some(Arc::downgrade(sink));
        Ok(())
    }
    pub fn cancel_port(&self) -> CancelPort {
        CancelPort {
            inner: Arc::downgrade(&self.inner),
        }
    }
    pub fn publisher(&self) -> Publisher {
        Publisher {
            inner: Arc::downgrade(&self.inner),
        }
    }
    pub fn set_legacy(&self, legacy: bool) -> Result<(), Failure> {
        let mut state = self
            .inner
            .routing
            .lock()
            .map_err(|_| Failure::Unavailable)?;
        if state.legacy != legacy {
            state.revision = state.revision.checked_add(1).ok_or(Failure::Unavailable)?;
            state.legacy = legacy;
        }
        Ok(())
    }
    pub fn tool_targets(&self) -> Result<Vec<Value>, Failure> {
        Ok(self
            .inner
            .routing
            .lock()
            .map_err(|_| Failure::Unavailable)?
            .listeners
            .iter()
            .filter(|s| s.tools)
            .map(|s| s.id.clone())
            .collect())
    }
    /// No mutation until the frontend has reserved the acknowledged frame.
    pub fn plan_listen(
        &self,
        id: &Value,
        filter: &Value,
        valid: bool,
    ) -> Result<ListenPlan, Failure> {
        if !self.inner.active.load(Ordering::Acquire) {
            return Err(Failure::Unavailable);
        }
        let state = self
            .inner
            .routing
            .lock()
            .map_err(|_| Failure::Unavailable)?;
        if state.listeners.len() >= MAX_LISTENERS {
            return Err(Failure::SubscriptionLimit);
        }
        if !valid {
            return Err(Failure::InvalidFilter);
        }
        if !(id.is_string() || id.is_i64() || id.is_u64()) {
            return Err(Failure::Unavailable);
        }
        let id_bytes = encoded_len(id, MAX_ID)?;
        let uris = filter
            .get("resourceSubscriptions")
            .and_then(Value::as_array);
        let entries = uris.map_or(0, Vec::len);
        if id_bytes > MAX_ID || entries > MAX_LISTENERS {
            return Err(Failure::Capacity);
        }
        let uri_bytes = uris.into_iter().flatten().try_fold(0usize, |total, uri| {
            let uri = uri.as_str().ok_or(Failure::InvalidFilter)?;
            if uri.len() > MAX_URI {
                return Err(Failure::Capacity);
            }
            total.checked_add(uri.len()).ok_or(Failure::Capacity)
        })?;
        let prior = state
            .listeners
            .iter()
            .position(|listener| listener.id == *id);
        let old = prior.map(|i| &state.listeners[i]);
        if state.id_bytes - old.map_or(0, |s| s.id_bytes) + id_bytes > MAX_IDS
            || state.uri_bytes - old.map_or(0, |s| s.uri_bytes) + uri_bytes > MAX_URI_BYTES
            || state.uri_entries - old.map_or(0, |s| s.uris.len()) + entries > MAX_URIS
        {
            return Err(Failure::Capacity);
        }
        let tools = state.tools && filter["toolsListChanged"] == true;
        let list = filter["resourcesListChanged"] == true;
        let uris = uris
            .into_iter()
            .flatten()
            .map(|u| u.as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        let mut honored = json!({});
        if tools {
            honored["toolsListChanged"] = json!(true)
        }
        if list {
            honored["resourcesListChanged"] = json!(true)
        }
        if !uris.is_empty() {
            honored["resourceSubscriptions"] = json!(uris)
        }
        Ok(ListenPlan {
            revision: state.revision,
            listener: Listener {
                id: id.clone(),
                tools,
                list,
                uris,
                id_bytes,
                uri_bytes,
            },
            prior,
            honored,
        })
    }
    pub fn commit_listen(&self, plan: ListenPlan) -> Result<(), Failure> {
        let mut state = self
            .inner
            .routing
            .lock()
            .map_err(|_| Failure::Unavailable)?;
        if !self.inner.active.load(Ordering::Acquire) || state.revision != plan.revision {
            return Err(Failure::Unavailable);
        }
        state.revision = state.revision.checked_add(1).ok_or(Failure::Unavailable)?;
        if let Some(i) = plan.prior {
            let old = &state.listeners[i];
            let (ids, bytes, entries) = (old.id_bytes, old.uri_bytes, old.uris.len());
            state.id_bytes -= ids;
            state.uri_bytes -= bytes;
            state.uri_entries -= entries;
        }
        state.id_bytes += plan.listener.id_bytes;
        state.uri_bytes += plan.listener.uri_bytes;
        state.uri_entries += plan.listener.uris.len();
        if let Some(i) = plan.prior {
            state.listeners[i] = plan.listener
        } else {
            state.listeners.push(plan.listener)
        }
        Ok(())
    }
    pub fn cancel(&self, id: &Value) -> Result<bool, Failure> {
        cancel_inner(&self.inner, id)
    }
    pub fn plan_uri(&self, uri: &str, add: bool) -> Result<UriPlan, Failure> {
        if !self.inner.active.load(Ordering::Acquire) {
            return Err(Failure::Unavailable);
        }
        if uri.len() > MAX_URI {
            return Err(Failure::Capacity);
        }
        let state = self
            .inner
            .routing
            .lock()
            .map_err(|_| Failure::Unavailable)?;
        if add
            && !state.legacy_uris.contains(uri)
            && (state.legacy_uris.len() >= MAX_LISTENERS
                || state.uri_bytes + uri.len() > MAX_URI_BYTES
                || state.uri_entries >= MAX_URIS)
        {
            return Err(Failure::Capacity);
        }
        Ok(UriPlan {
            revision: state.revision,
            uri: uri.into(),
            add,
        })
    }
    pub fn commit_uri(&self, plan: UriPlan) -> Result<(), Failure> {
        let mut state = self
            .inner
            .routing
            .lock()
            .map_err(|_| Failure::Unavailable)?;
        if !self.inner.active.load(Ordering::Acquire) || state.revision != plan.revision {
            return Err(Failure::Unavailable);
        }
        state.revision = state.revision.checked_add(1).ok_or(Failure::Unavailable)?;
        if plan.add {
            if state.legacy_uris.insert(plan.uri.clone()) {
                state.uri_bytes += plan.uri.len();
                state.uri_entries += 1;
            }
        } else if state.legacy_uris.remove(&plan.uri) {
            state.uri_bytes -= plan.uri.len();
            state.uri_entries -= 1;
        }
        Ok(())
    }
    pub async fn next(&mut self) -> Option<Result<Batch, Failure>> {
        if self.failed {
            return None;
        }
        tokio::select! {
            biased;
            batch = self.receiver.recv() => match batch {
                Some(batch) => Some(Ok(batch)),
                None => {
                    self.failed = true;
                    self.terminal_failure().map(Err)
                }
            },
            changed = self.failure.changed() => {
                self.failed = true;
                Some(Err(if changed.is_err() { Failure::Unavailable } else {
                    (*self.failure.borrow()).unwrap_or(Failure::Unavailable)
                }))
            }
        }
    }
    pub fn drain_ready(&mut self) -> Vec<Batch> {
        let mut batches = Vec::new();
        for _ in 0..QUEUE_ITEMS {
            match self.receiver.try_recv() {
                Ok(batch) => batches.push(batch),
                Err(_) => break,
            }
        }
        batches
    }
    /// Retained producer failure survives queue drain and frontend EOF.
    pub fn terminal_failure(&self) -> Option<Failure> {
        *self.failure.borrow()
    }
    /// End this frontend's projection lifetime; does not touch OBS/Service jobs.
    pub fn close(&mut self) {
        let _state = self.inner.routing.lock();
        self.inner.active.store(false, Ordering::Release);
        self.receiver.close();
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.close()
    }
}
enum Origin<'a> {
    Obs(&'a str, &'a Value),
    Take,
}
impl Publisher {
    pub(crate) fn obs(&self, event: &str, data: &Value) {
        self.publish(Origin::Obs(event, data));
    }
    pub(crate) fn take_changed(&self) {
        self.publish(Origin::Take);
    }
    fn publish(&self, origin: Origin<'_>) {
        let Some(inner) = self
            .inner
            .upgrade()
            .filter(|p| p.active.load(Ordering::Acquire))
        else {
            return;
        };
        let fail = |reason| {
            inner.failure.send_if_modified(|slot| {
                if slot.is_none() {
                    *slot = Some(reason);
                    true
                } else {
                    false
                }
            });
        };
        if inner.failure.borrow().is_some() {
            return;
        }
        let direct = match inner.direct.lock() {
            Ok(slot) => slot.clone(),
            Err(_) => {
                fail(Failure::Unavailable);
                return;
            }
        };
        let state = match inner.routing.lock() {
            Ok(state) => state,
            Err(_) => {
                fail(Failure::Unavailable);
                return;
            }
        };
        if !inner.active.load(Ordering::Acquire) {
            return;
        }
        // Interest and recipients share the same routing boundary as event
        // projection. A later listener cannot capture an earlier publication.
        let interested =
            state.legacy || state.listeners.iter().any(|s| s.list || !s.uris.is_empty());
        if !interested {
            return;
        }
        let item_uri = |uri: &str| uri.starts_with("obs://scene/") && uri.ends_with("/items");
        let scene_items = if state.legacy {
            state.legacy_uris.iter().any(|uri| item_uri(uri))
        } else {
            state
                .listeners
                .iter()
                .flat_map(|s| s.uris.iter())
                .any(|uri| item_uri(uri))
        };
        let changes = match origin {
            Origin::Obs(event, data) => project_selected(event, data, scene_items),
            Origin::Take => Ok(vec![Change::Updated("obs://take/current".into())]),
        };
        let changes = match changes {
            Ok(changes) => changes,
            Err(reason) => {
                fail(reason);
                return;
            }
        };
        if changes.is_empty() {
            return;
        }
        let mut deliveries = Vec::with_capacity(changes.len());
        let mut bytes = 0usize;
        for change in changes {
            let audiences = if state.legacy {
                match &change {
                    Change::ListChanged => vec![None],
                    Change::Updated(uri) if state.legacy_uris.contains(uri) => vec![None],
                    _ => vec![],
                }
            } else {
                state
                    .listeners
                    .iter()
                    .filter(|s| match &change {
                        Change::ListChanged => s.list,
                        Change::Updated(uri) => s.uris.contains(uri),
                    })
                    .map(|s| Some(s.id.clone()))
                    .collect()
            };
            if audiences.is_empty() {
                continue;
            }
            // Account for amplified URI payload before it can become encoded frames.
            let uri_bytes = match &change {
                Change::ListChanged => 0,
                Change::Updated(uri) => uri.len(),
            };
            if direct.is_some() {
                bytes = match bytes.checked_add(uri_bytes + 256) {
                    Some(n) if n <= QUEUE_BYTES => n,
                    _ => {
                        fail(Failure::Capacity);
                        return;
                    }
                };
            }
            for id in &audiences {
                let id_bytes = id.as_ref().map_or(0, |id| {
                    serde_json::to_vec(id).map_or(MAX_ID + 1, |b| b.len())
                });
                bytes = match bytes.checked_add(if direct.is_some() {
                    id_bytes + 256
                } else {
                    uri_bytes + id_bytes + 256
                }) {
                    Some(n) if n <= QUEUE_BYTES => n,
                    _ => {
                        fail(Failure::Capacity);
                        return;
                    }
                };
            }
            deliveries.push(Delivery { change, audiences });
        }
        if deliveries.is_empty() {
            return;
        }
        drop(state);
        let permit = match inner
            .bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(bytes).unwrap())
        {
            Ok(p) => p,
            Err(_) => {
                fail(Failure::Capacity);
                return;
            }
        };
        let batch = Batch {
            deliveries,
            _bytes: permit,
        };
        if let Some(direct) = direct {
            if let Some(sink) = direct.upgrade() {
                sink.deliver(batch)
            } else {
                fail(Failure::Unavailable)
            }
        } else if inner.sender.try_send(batch).is_err() {
            fail(Failure::Capacity)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn subscribe(owner: &Owner, uri: &str) {
        owner
            .commit_uri(owner.plan_uri(uri, true).unwrap())
            .unwrap();
    }
    fn listen(owner: &Owner, id: Value, filter: Value) {
        owner
            .commit_listen(owner.plan_listen(&id, &filter, true).unwrap())
            .unwrap();
    }
    #[test]
    fn all33_actual_source_projections_and_encoding_match() {
        let oracle: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/resource_projection_source.json"
        ))
        .unwrap();
        let cases = oracle["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 33);
        for row in cases {
            let changes = project(row["event"].as_str().unwrap(), &row["data"]).unwrap();
            let actual = changes
                .into_iter()
                .map(|change| match change {
                    Change::Updated(uri) => json!({"kind":"updated","uri":uri}),
                    Change::ListChanged => json!({"kind":"list-changed"}),
                })
                .collect::<Vec<_>>();
            assert_eq!(json!(actual), row["projection"], "{}", row["event"]);
        }
    }
    #[tokio::test]
    async fn admission_freezes_recipients_and_cancellation_only_ends_future_delivery() {
        let mut owner = Owner::new(true);
        let publisher = owner.publisher();
        listen(
            &owner,
            json!("one"),
            json!({"resourceSubscriptions":["obs://status","obs://status"]}),
        );
        publisher.obs("RecordStateChanged", &json!({}));
        assert!(owner.cancel(&json!("one")).unwrap());
        publisher.obs("RecordStateChanged", &json!({}));
        let batches = owner.drain_ready();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].deliveries.len(), 1);
        assert_eq!(batches[0].deliveries[0].audiences, vec![Some(json!("one"))]);
        assert!(owner.tool_targets().unwrap().is_empty());
    }
    #[tokio::test]
    async fn queue_failure_retains_all64_accepted_batches_without_coalescing() {
        let mut owner = Owner::new(false);
        owner.set_legacy(true).unwrap();
        subscribe(&owner, "obs://status");
        let publisher = owner.publisher();
        for _ in 0..65 {
            publisher.obs("RecordStateChanged", &json!({}));
        }
        assert_eq!(owner.terminal_failure(), Some(Failure::Capacity));
        owner.close();
        for _ in 0..64 {
            let batch = owner.next().await.unwrap().unwrap();
            assert_eq!(batch.deliveries.len(), 1);
            assert_eq!(
                batch.deliveries[0].change,
                Change::Updated("obs://status".into())
            );
        }
        assert!(matches!(owner.next().await, Some(Err(Failure::Capacity))));
        assert!(owner.next().await.is_none());
    }
    #[test]
    fn plan_revision_and_closed_lifetime_refuse_stale_commit() {
        let mut owner = Owner::new(true);
        let plan = owner
            .plan_listen(&json!(1), &json!({"toolsListChanged":true}), true)
            .unwrap();
        listen(&owner, json!(2), json!({"toolsListChanged":true}));
        assert!(matches!(
            owner.commit_listen(plan),
            Err(Failure::Unavailable)
        ));
        assert_eq!(owner.tool_targets().unwrap(), vec![json!(2)]);
        let publisher = owner.publisher();
        owner.close();
        publisher.take_changed();
        assert!(owner.drain_ready().is_empty());
        assert!(matches!(
            owner.plan_uri("obs://status", true),
            Err(Failure::Unavailable)
        ));
        assert!(matches!(
            owner.plan_listen(&json!(3), &json!({}), true),
            Err(Failure::Unavailable)
        ));
    }
    #[test]
    fn count_identity_uri_and_revision_budgets_fail_before_install() {
        let owner = Owner::new(true);
        assert!(matches!(
            owner.plan_listen(&json!("x".repeat(1025)), &json!({}), true),
            Err(Failure::Capacity)
        ));
        assert!(matches!(
            owner.plan_uri(&"x".repeat(MAX_URI + 1), true),
            Err(Failure::Capacity)
        ));
        for n in 0..MAX_LISTENERS {
            listen(&owner, json!(n), json!({}));
        }
        assert!(matches!(
            owner.plan_listen(&json!(0), &json!({}), true),
            Err(Failure::SubscriptionLimit)
        ));
        assert!(matches!(
            owner.plan_listen(&json!(2), &json!({}), false),
            Err(Failure::SubscriptionLimit)
        ));
        let mut state = owner.inner.routing.lock().unwrap();
        state.revision = u64::MAX;
        drop(state);
        assert!(matches!(owner.cancel(&json!(0)), Err(Failure::Unavailable)));
    }
    #[test]
    fn weak_producer_cannot_retain_or_revive_a_frontend() {
        let publisher = {
            let owner = Owner::new(false);
            owner.publisher()
        };
        assert!(publisher.inner.upgrade().is_none());
        publisher.obs("SceneNameChanged", &json!({"oldSceneName":"retired"}));
    }

    #[test]
    fn retained_uri_bytes_exhaust_before_item_budget_and_release_on_batch_drop() {
        let mut owner = Owner::new(false);
        let name = "é".repeat(MAX_SCENE_NAME / 2);
        let change = project("SceneItemCreated", &json!({"sceneName":name}))
            .unwrap()
            .remove(0);
        let Change::Updated(uri) = change else {
            panic!("items URI")
        };
        listen(&owner, json!(1), json!({"resourceSubscriptions":[uri]}));
        let publisher = owner.publisher();
        for _ in 0..QUEUE_ITEMS {
            publisher.obs("SceneItemCreated", &json!({"sceneName":name}));
        }
        let batches = owner.drain_ready();
        assert!(!batches.is_empty() && batches.len() < QUEUE_ITEMS);
        assert_eq!(*owner.inner.failure.borrow(), Some(Failure::Capacity));
        assert!(owner.inner.bytes.available_permits() < QUEUE_BYTES);
        drop(batches);
        assert_eq!(owner.inner.bytes.available_permits(), QUEUE_BYTES);
    }

    #[tokio::test]
    async fn absent_uri_interest_neither_allocates_queued_publication_nor_fails_session() {
        let mut owner = Owner::new(true);
        let publisher = owner.publisher();
        let oversized = json!({"oldSceneName":"x".repeat(MAX_SCENE_NAME+1)});
        publisher.obs("SceneNameChanged", &oversized);
        assert!(owner.drain_ready().is_empty());
        assert_eq!(*owner.inner.failure.borrow(), None);
        owner.set_legacy(true).unwrap();
        publisher.obs("SceneNameChanged", &oversized);
        let batches = owner.drain_ready();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].deliveries.len(), 1);
        assert_eq!(batches[0].deliveries[0].change, Change::ListChanged);
        drop(batches);
        assert_eq!(*owner.inner.failure.borrow(), None);
        owner.close();
        assert!(owner.next().await.is_none());
        publisher.obs("SceneCreated", &json!({}));
        assert!(owner.drain_ready().is_empty());
    }
    #[test]
    fn direct_http_binding_refuses_dynamic_session_tool_authority() {
        struct Sink;
        impl DeliverySink for Sink {
            fn deliver(&self, _batch: Batch) {}
        }
        let sink: Arc<dyn DeliverySink> = Arc::new(Sink);
        assert!(matches!(
            Owner::new(true).bind_delivery(&sink),
            Err(Failure::Unavailable)
        ));
    }
    #[test]
    fn direct_sink_can_reenter_cancel_without_holding_router_lock() {
        use std::sync::{atomic::AtomicUsize, mpsc as blocking};
        struct Reentrant {
            cancel: CancelPort,
            count: AtomicUsize,
        }
        impl DeliverySink for Reentrant {
            fn deliver(&self, batch: Batch) {
                for delivery in batch.deliveries {
                    for id in delivery.audiences.into_iter().flatten() {
                        assert!(self.cancel.cancel(&id).unwrap());
                        self.count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }
        let owner = Owner::new(false);
        let sink = Arc::new(Reentrant {
            cancel: owner.cancel_port(),
            count: AtomicUsize::new(0),
        });
        let erased: Arc<dyn DeliverySink> = sink.clone();
        owner.bind_delivery(&erased).unwrap();
        let id = json!(1);
        owner
            .commit_listen(
                owner
                    .plan_listen(
                        &id,
                        &json!({"resourceSubscriptions":["obs://status"]}),
                        true,
                    )
                    .unwrap(),
            )
            .unwrap();
        let publisher = owner.publisher();
        let (done, wait) = blocking::channel();
        let task = std::thread::spawn(move || {
            publisher.obs("CurrentProgramSceneChanged", &json!({}));
            done.send(()).unwrap();
        });
        if wait
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_err()
        {
            // A broken callback can hold routing forever; do not deadlock this
            // negative control again in Owner::drop during panic unwinding.
            std::mem::forget(owner);
            panic!("direct sink deadlocked while cancelling its own listener");
        }
        task.join().unwrap();
        assert_eq!(sink.count.load(Ordering::SeqCst), 1);
        assert!(!owner.cancel(&id).unwrap());
        assert!(owner.terminal_failure().is_none());
    }
    #[test]
    fn direct_delivery_is_weak_and_cannot_rebind_or_extend_projection_lifetime() {
        struct Empty;
        impl DeliverySink for Empty {
            fn deliver(&self, _: Batch) {}
        }
        let mut owner = Owner::new(false);
        let sink: Arc<dyn DeliverySink> = Arc::new(Empty);
        owner.bind_delivery(&sink).unwrap();
        assert_eq!(Arc::strong_count(&sink), 1);
        assert!(owner.bind_delivery(&sink).is_err());
        owner
            .commit_listen(
                owner
                    .plan_listen(
                        &json!(1),
                        &json!({"resourceSubscriptions":["obs://status"]}),
                        true,
                    )
                    .unwrap(),
            )
            .unwrap();
        drop(sink);
        owner
            .publisher()
            .obs("CurrentProgramSceneChanged", &json!({}));
        assert_eq!(owner.terminal_failure(), Some(Failure::Unavailable));
        owner.close();
    }
}
