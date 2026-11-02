// SPDX-License-Identifier: GPL-2.0-only
//! One bounded, non-copying take feed attached to the physical observation owner.
//! A frame keeps the socket's existing byte/item permits until take application.
//! Only the take actor may apply it; dropping an unapplied frame revokes the feed.
use crate::{
    observation::{EvidenceState, ObservationError},
    session::{ConnectionToken, ObsEvent, SessionHandle},
    takes::Event,
};
use std::{
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, timeout_at},
};

const FEED_ITEMS: usize = 256;
fn invalidated() -> ObservationError {
    ObservationError::new(
        "observation_invalidated",
        "Take event feed is unavailable; inspect OBS before another action",
    )
}
struct FeedState {
    active: AtomicBool,
    token: ConnectionToken,
    applied: watch::Sender<u64>,
    sender: Mutex<Option<mpsc::Sender<Event>>>,
}
impl FeedState {
    fn revoke(&self) {
        self.active.store(false, Ordering::Release);
        // Remove the channel's producer even if non-owning fence clones remain.
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
        self.applied.send_modify(|_| {});
    }
}

/// The service holds this owner through take-supervisor shutdown. Fence clones
/// own no observer task and cannot keep admission alive after this owner drops.
pub struct TakeEventFeed {
    state: Arc<FeedState>,
    fence: TakeEventFence,
    receiver: Option<mpsc::Receiver<Event>>,
}
impl TakeEventFeed {
    pub fn fence(&self) -> TakeEventFence {
        self.fence.clone()
    }
    pub fn take_receiver(&mut self) -> Option<mpsc::Receiver<Event>> {
        self.receiver.take()
    }
    pub fn revoke(&self) {
        self.state.revoke();
    }
}
impl Drop for TakeEventFeed {
    fn drop(&mut self) {
        self.state.revoke();
    }
}

#[derive(Clone)]
pub struct TakeEventFence {
    scope: Arc<FeedState>,
    handle: SessionHandle,
    evidence: watch::Receiver<EvidenceState>,
    published: Arc<AtomicU64>,
}
impl fmt::Debug for TakeEventFence {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("TakeEventFence")
            .field("token", &self.scope.token)
            .field("active", &self.scope.active.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
impl TakeEventFence {
    pub fn token(&self) -> ConnectionToken {
        self.scope.token
    }
    pub fn endpoint_hash(&self) -> &str {
        self.handle.endpoint_hash()
    }
    pub fn picture_revision(&self) -> u64 {
        self.evidence.borrow().revision
    }
    fn live(&self) -> bool {
        let evidence = *self.evidence.borrow();
        self.scope.active.load(Ordering::Acquire)
            && self.handle.token() == self.scope.token
            && self.handle.invalidation().is_none()
            && self.evidence.has_changed().is_ok()
            && evidence.connected
            && !evidence.cancelled
            && evidence.generation == self.scope.token.generation()
    }
    pub fn ready(&self) -> bool {
        self.ready_at_cursor(self.handle.event_cursor())
    }
    pub fn ready_at_cursor(&self, cursor: u64) -> bool {
        self.live()
            && *self.scope.applied.borrow() >= cursor
            && self.published.load(Ordering::Acquire) >= cursor
            && self.handle.event_cursor() == cursor
    }
    /// Project only after received events are applied, with one unchanged
    /// intrinsic cursor across the synchronous callback. A cursor change retries
    /// this read-only projection; callers must not put effects in the callback.
    /// The retained caller owns the callback/result, with no extra task or RPC.
    /// Events received after the final cursor check belong to a later snapshot.
    pub(crate) async fn project_after_applied<F>(
        &self,
        deadline: Instant,
        mut project: F,
    ) -> Result<serde_json::Value, ObservationError>
    where
        F: FnMut() -> Result<serde_json::Value, ObservationError>,
    {
        loop {
            // synchronize() may already be settled; expiration must still deny
            // projection, including a settled feed with no received events.
            if Instant::now() >= deadline {
                return Err(ObservationError::new(
                    "observation_limit",
                    "Take event application exceeded its deadline",
                ));
            }
            self.synchronize(deadline).await?;
            let cursor = self.handle.event_cursor();
            if Instant::now() >= deadline {
                return Err(ObservationError::new(
                    "observation_limit",
                    "Take event application exceeded its deadline",
                ));
            }
            if self.ready_at_cursor(cursor) {
                let value = project();
                if !self.live() {
                    return Err(invalidated());
                }
                if Instant::now() >= deadline {
                    return Err(ObservationError::new(
                        "observation_limit",
                        "Take event application exceeded its deadline",
                    ));
                }
                if self.ready_at_cursor(cursor) {
                    return value;
                }
                // Never retain a stale projected Value across an await. Only
                // this read-only callback may be retried, never a take effect.
                drop(value);
            }
            tokio::task::yield_now().await;
        }
    }

    pub async fn synchronize(&self, deadline: Instant) -> Result<(), ObservationError> {
        let cursor = self.handle.event_cursor();
        let mut applied = self.scope.applied.subscribe();
        let mut evidence = self.evidence.clone();
        loop {
            if !self.live() {
                return Err(invalidated());
            }
            if *applied.borrow() >= cursor && self.published.load(Ordering::Acquire) >= cursor {
                return Ok(());
            }
            let changed = async {
                tokio::select! {
                    value = applied.changed() => value,
                    value = evidence.changed() => value,
                }
            };
            match timeout_at(deadline, changed).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(invalidated()),
                Err(_) => {
                    return Err(ObservationError::new(
                        "observation_limit",
                        "Take event application exceeded its deadline",
                    ));
                }
            }
        }
    }
}
impl crate::take_transport::EventFence for TakeEventFence {
    fn token(&self) -> ConnectionToken {
        self.token()
    }
    fn ready_at_cursor(&self, cursor: u64) -> bool {
        self.ready_at_cursor(cursor)
    }
    fn picture_revision(&self) -> u64 {
        self.picture_revision()
    }
    fn synchronize(&self, deadline: Instant) -> crate::take_transport::EventFuture<'_> {
        Box::pin(async move {
            self.synchronize(deadline)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

/// Opaque application permission. The source frame and its queue budgets are
/// retained here; there is no public constructor or public acknowledgement API.
pub struct ObservedFrame {
    event: ObsEvent,
    scope: Arc<FeedState>,
    completed: bool,
}
impl fmt::Debug for ObservedFrame {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("ObservedFrame")
            .field("connection", &self.event.connection)
            .field("sequence", &self.event.sequence)
            .finish_non_exhaustive()
    }
}
impl ObservedFrame {
    pub(crate) fn apply(
        mut self,
        observe: impl FnOnce(ConnectionToken, String, serde_json::Value),
    ) {
        let prior = *self.scope.applied.borrow();
        if !self.scope.active.load(Ordering::Acquire)
            || self.event.connection != self.scope.token
            || prior.checked_add(1) != Some(self.event.sequence)
        {
            self.scope.revoke();
            return;
        }
        observe(
            self.event.connection,
            std::mem::take(&mut self.event.event_type),
            self.event.data.take().unwrap_or(serde_json::Value::Null),
        );
        // Publish only after the take owner's synchronous state update finishes.
        // The original event's permits stay held through this point.
        if self.scope.active.load(Ordering::Acquire) {
            self.scope.applied.send_replace(self.event.sequence);
        }
        self.completed = true;
    }
}
impl Drop for ObservedFrame {
    fn drop(&mut self) {
        if !self.completed {
            self.scope.revoke();
        }
    }
}

pub(crate) struct EventHub {
    slot: Mutex<Option<Weak<FeedState>>>,
    outputs: Arc<crate::output_events::Registry>,
}
impl EventHub {
    pub(crate) fn new(token: ConnectionToken) -> Self {
        Self {
            slot: Mutex::new(None),
            outputs: Arc::new(crate::output_events::Registry::new(token)),
        }
    }
    pub(crate) fn output_events(
        &self,
        handle: SessionHandle,
    ) -> Result<crate::output_events::OutputEvents, crate::output_events::OutputEventError> {
        self.outputs.view(handle)
    }
    /// Call under this same lock as revision/progress publication so a newly
    /// attached consumer starts at a published cursor, never a mixed interval.
    pub(crate) fn publish(
        &self,
        event: ObsEvent,
        update: impl FnOnce(),
    ) -> Result<(), ObservationError> {
        let mut slot = self.slot.lock().map_err(|_| invalidated())?;
        update();
        self.outputs.publish(&event);
        let scope = slot
            .as_ref()
            .and_then(Weak::upgrade)
            .filter(|scope| scope.active.load(Ordering::Acquire));
        if let Some(scope) = scope {
            let sender = scope.sender.lock().map_err(|_| invalidated())?.clone();
            let sender = sender.ok_or_else(invalidated)?;
            let frame = ObservedFrame {
                event,
                scope: scope.clone(),
                completed: false,
            };
            if sender.try_send(Event::Observed(frame)).is_err() {
                scope.revoke();
                return Err(invalidated());
            }
        } else {
            slot.take();
        }
        Ok(())
    }
    pub(crate) fn attach(
        &self,
        handle: SessionHandle,
        evidence: watch::Receiver<EvidenceState>,
        published: Arc<AtomicU64>,
    ) -> Result<TakeEventFeed, ObservationError> {
        let mut slot = self.slot.lock().map_err(|_| invalidated())?;
        if slot
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|scope| scope.active.load(Ordering::Acquire))
        {
            return Err(ObservationError::new(
                "operation_in_progress",
                "A take event consumer already owns this connection",
            ));
        }
        let state = *evidence.borrow();
        if !state.connected
            || state.cancelled
            || evidence.has_changed().is_err()
            || handle.invalidation().is_some()
            || state.generation != handle.token().generation()
        {
            return Err(invalidated());
        }
        let (sender, receiver) = mpsc::channel(FEED_ITEMS);
        let (applied, _) = watch::channel(published.load(Ordering::Acquire));
        let scope = Arc::new(FeedState {
            active: AtomicBool::new(true),
            token: handle.token(),
            applied,
            sender: Mutex::new(Some(sender)),
        });
        let fence = TakeEventFence {
            scope: scope.clone(),
            handle,
            evidence,
            published,
        };
        *slot = Some(Arc::downgrade(&scope));
        Ok(TakeEventFeed {
            state: scope,
            fence,
            receiver: Some(receiver),
        })
    }
    pub(crate) fn revoke(&self) {
        self.outputs.revoke();
        if let Ok(mut slot) = self.slot.lock()
            && let Some(scope) = slot.take().and_then(|scope| scope.upgrade())
        {
            scope.revoke();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        session::{Invalidation, ObsSession, SessionOptions},
        session_observation::ObservationBridge,
    };
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use std::time::Duration;
    use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    type Command = (Value, oneshot::Sender<()>);
    struct Fixture {
        owner: ObsSession,
        bridge: ObservationBridge,
        commands: mpsc::Sender<Command>,
        peer: JoinHandle<()>,
    }
    impl Fixture {
        async fn new(event_items: usize) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let (commands, mut receiver) = mpsc::channel::<Command>(8);
            let peer = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                socket
                    .send(Message::Text(
                        json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                let identify = socket.next().await.unwrap().unwrap();
                let identify: Value = serde_json::from_str(identify.to_text().unwrap()).unwrap();
                assert_eq!(
                    identify["d"]["eventSubscriptions"],
                    json!(
                        crate::takes::REQUIRED_SUBSCRIPTIONS
                            | crate::stage_transport::REQUIRED_STAGE_EVENTS
                    )
                );
                socket
                    .send(Message::Text(
                        json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                loop {
                    tokio::select! {
                        command=receiver.recv()=>match command {
                            Some((frame, done))=>{ if socket.send(Message::Text(frame.to_string().into())).await.is_err(){break;} let _=done.send(()); },
                            None=>break,
                        },
                        message=socket.next()=>match message {
                            Some(Ok(Message::Text(text)))=>{
                                let frame:Value=serde_json::from_str(text.as_ref()).unwrap();
                                assert_eq!(frame["op"],6);
                                let data=&frame["d"];
                                if socket.send(Message::Text(json!({"op":7,"d":{"requestId":data["requestId"],"requestType":data["requestType"],"requestStatus":{"result":true,"code":100},"responseData":{"scenes":[]}}}).to_string().into())).await.is_err(){break;}
                            },
                            Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,
                            _=>{},
                        }
                    }
                }
            });
            let (owner, events) = ObsSession::connect(
                &url,
                None,
                SessionOptions {
                    event_items,
                    event_subscriptions: crate::takes::REQUIRED_SUBSCRIPTIONS
                        | crate::stage_transport::REQUIRED_STAGE_EVENTS,
                    ..SessionOptions::default()
                },
            )
            .await
            .unwrap();
            let bridge = ObservationBridge::bind(owner.handle(), events, None).unwrap();
            Self {
                owner,
                bridge,
                commands,
                peer,
            }
        }
        async fn emit(&self, event_type: &str) {
            let intent = match event_type {
                "InputVolumeMeters" => 1 << 16,
                "SceneItemTransformChanged" => 1 << 19,
                _ => 8,
            };
            let (done, wait) = oneshot::channel();
            self.commands.send((json!({"op":5,"d":{"eventType":event_type,"eventIntent":intent,"eventData":{"marker":7}}}),done)).await.unwrap();
            wait.await.unwrap();
        }
        async fn received(&self) {
            self.owner
                .handle()
                .request(
                    self.owner.token(),
                    "GetSceneList",
                    json!({}),
                    Duration::from_secs(2),
                )
                .await
                .unwrap();
            self.bridge
                .synchronize(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
        }
        async fn close(self) {
            self.owner.shutdown().await.unwrap();
            self.bridge.shutdown().await.unwrap();
            drop(self.commands);
            self.peer.await.unwrap();
        }
    }
    fn apply(event: Event, callback: impl FnOnce(ConnectionToken, String, Value)) {
        match event {
            Event::Observed(frame) => frame.apply(callback),
            _ => panic!("unowned event feed"),
        }
    }

    #[tokio::test]
    async fn current_take_projection_waits_for_actual_frame_application() {
        let fixture = Fixture::new(8).await;
        let mut feed = fixture.bridge.attach_take_feed().unwrap();
        let fence = feed.fence();
        let mut receiver = feed.take_receiver().unwrap();
        fixture.emit("RecordStateChanged").await;
        fixture.received().await;
        let held_frame = receiver.recv().await.unwrap();
        assert!(!fence.ready());
        let projected = AtomicU64::new(0);
        let monitor_revision = AtomicU64::new(0);
        let projection =
            fence.project_after_applied(Instant::now() + Duration::from_secs(2), || {
                projected.fetch_add(1, Ordering::AcqRel);
                Ok(json!({"revision":monitor_revision.load(Ordering::Acquire)}))
            });
        tokio::pin!(projection);
        let before_application = futures_util::poll!(projection.as_mut());
        let premature_calls = projected.load(Ordering::Acquire);
        apply(held_frame, |_, _, _| {
            monitor_revision.store(1, Ordering::Release);
        });
        let output = match before_application {
            std::task::Poll::Ready(result) => result.unwrap(),
            std::task::Poll::Pending => projection.await.unwrap(),
        };
        drop(feed);
        fixture.close().await;
        assert_eq!(
            premature_calls, 0,
            "projection ran before actual take application"
        );
        assert_eq!(output, json!({"revision":1}));
    }

    #[tokio::test]
    async fn expired_current_take_projection_never_calls_even_a_settled_feed() {
        let fixture = Fixture::new(8).await;
        let feed = fixture.bridge.attach_take_feed().unwrap();
        let fence = feed.fence();
        let calls = AtomicU64::new(0);
        assert!(fence.ready());
        let error = fence
            .project_after_applied(Instant::now(), || {
                calls.fetch_add(1, Ordering::AcqRel);
                Ok(json!({"running":false}))
            })
            .await
            .unwrap_err();
        assert_eq!(error.reason, "observation_limit");
        assert_eq!(calls.load(Ordering::Acquire), 0);
        drop(feed);
        fixture.close().await;
    }

    #[tokio::test]
    async fn unapplied_current_take_frame_expires_without_projection() {
        let fixture = Fixture::new(8).await;
        let mut feed = fixture.bridge.attach_take_feed().unwrap();
        let fence = feed.fence();
        let mut receiver = feed.take_receiver().unwrap();
        fixture.emit("RecordStateChanged").await;
        fixture.received().await;
        let held_frame = receiver.recv().await.unwrap();
        let calls = AtomicU64::new(0);
        let error = fence
            .project_after_applied(Instant::now() + Duration::from_millis(20), || {
                calls.fetch_add(1, Ordering::AcqRel);
                Ok(json!({"running":true}))
            })
            .await
            .unwrap_err();
        assert_eq!(error.reason, "observation_limit");
        assert_eq!(calls.load(Ordering::Acquire), 0);
        apply(held_frame, |_, _, _| {});
        assert!(fence.ready(), "read expiration revoked a healthy feed");
        drop(feed);
        fixture.close().await;
    }

    #[tokio::test]
    async fn revoked_pending_current_take_projection_never_calls() {
        let fixture = Fixture::new(8).await;
        let mut feed = fixture.bridge.attach_take_feed().unwrap();
        let fence = feed.fence();
        let mut receiver = feed.take_receiver().unwrap();
        fixture.emit("RecordStateChanged").await;
        fixture.received().await;
        let held_frame = receiver.recv().await.unwrap();
        let calls = AtomicU64::new(0);
        let projection =
            fence.project_after_applied(Instant::now() + Duration::from_secs(2), || {
                calls.fetch_add(1, Ordering::AcqRel);
                Ok(json!({"running":true}))
            });
        tokio::pin!(projection);
        assert!(futures_util::poll!(projection.as_mut()).is_pending());
        feed.revoke();
        assert_eq!(
            projection.await.unwrap_err().reason,
            "observation_invalidated"
        );
        assert_eq!(calls.load(Ordering::Acquire), 0);
        drop(held_frame);
        drop(feed);
        fixture.close().await;
    }

    #[tokio::test]
    async fn revocation_during_current_take_projection_refuses_stale_success() {
        let fixture = Fixture::new(8).await;
        let feed = fixture.bridge.attach_take_feed().unwrap();
        let fence = feed.fence();
        let calls = AtomicU64::new(0);
        let error = fence
            .project_after_applied(Instant::now() + Duration::from_secs(2), || {
                calls.fetch_add(1, Ordering::AcqRel);
                feed.revoke();
                Ok(json!({"stale":true}))
            })
            .await
            .unwrap_err();
        assert_eq!(error.reason, "observation_invalidated");
        assert_eq!(calls.load(Ordering::Acquire), 1);
        drop(feed);
        fixture.close().await;
    }

    #[tokio::test]
    async fn cursor_change_during_projection_discards_value_until_new_frame_applies() {
        // Existing project Tokio features provide a single-thread runtime. Keep
        // this real socket fixture on a separately owned test runtime so an
        // event can arrive while the synchronous callback runs on this thread.
        struct RuntimeThread {
            finish: Option<oneshot::Sender<()>>,
            thread: Option<std::thread::JoinHandle<()>>,
        }
        impl RuntimeThread {
            fn join(mut self) {
                self.finish.take().unwrap().send(()).unwrap();
                self.thread.take().unwrap().join().unwrap();
            }
        }
        impl Drop for RuntimeThread {
            fn drop(&mut self) {
                if let Some(finish) = self.finish.take() {
                    let _ = finish.send(());
                }
                if let Some(thread) = self.thread.take() {
                    let _ = thread.join();
                }
            }
        }
        let (constructed, fixture_ready) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let runtime_thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let fixture = Fixture::new(8).await;
                constructed
                    .send((fixture, tokio::runtime::Handle::current()))
                    .unwrap_or_else(|_| panic!("fixture receiver dropped"));
                finished.await.unwrap();
            });
        });
        // Declared before the fixture so unwind drops fixture handles before
        // signaling the runtime and joining its thread.
        let runtime_thread = RuntimeThread {
            finish: Some(finish),
            thread: Some(runtime_thread),
        };
        let (fixture, fixture_runtime) = fixture_ready.await.unwrap();
        let mut feed = fixture.bridge.attach_take_feed().unwrap();
        let fence = feed.fence();
        let mut receiver = feed.take_receiver().unwrap();
        let commands = fixture.commands.clone();
        let handle = fixture.owner.handle();
        let (trigger, start) = oneshot::channel();
        let (received, receive) = std::sync::mpsc::sync_channel(1);
        let emitter = fixture_runtime.spawn(async move {
            start.await.unwrap();
            let (done, wait) = oneshot::channel();
            commands.send((json!({"op":5,"d":{"eventType":"RecordStateChanged","eventIntent":8,"eventData":{"marker":8}}}),done)).await.unwrap();
            wait.await.unwrap();
            handle
                .request(
                    handle.token(),
                    "GetSceneList",
                    json!({}),
                    Duration::from_secs(2),
                )
                .await
                .unwrap();
            received.send(()).unwrap();
        });
        let mut trigger = Some(trigger);
        let calls = AtomicU64::new(0);
        let applied_revision = AtomicU64::new(0);
        let projection =
            fence.project_after_applied(Instant::now() + Duration::from_secs(5), || {
                let revision = applied_revision.load(Ordering::Acquire);
                calls.fetch_add(1, Ordering::AcqRel);
                if let Some(trigger) = trigger.take() {
                    trigger.send(()).unwrap();
                    // This bounded test-only callback allows the other runtime
                    // thread to deliver a real event during synchronous projection.
                    receive.recv_timeout(Duration::from_secs(2)).unwrap();
                }
                Ok(json!({"revision":revision}))
            });
        tokio::pin!(projection);
        assert!(futures_util::poll!(projection.as_mut()).is_pending());
        assert_eq!(calls.load(Ordering::Acquire), 1);
        apply(receiver.recv().await.unwrap(), |_, _, _| {
            applied_revision.store(1, Ordering::Release);
        });
        assert_eq!(projection.await.unwrap(), json!({"revision":1}));
        assert_eq!(calls.load(Ordering::Acquire), 2);
        emitter.await.unwrap();
        drop(feed);
        fixture.close().await;
        runtime_thread.join();
    }

    #[tokio::test]
    async fn received_meter_is_structurally_stable_but_pending_until_take_applies_it() {
        let fixture = Fixture::new(8).await;
        let mut feed = fixture.bridge.attach_take_feed().unwrap();
        let fence = feed.fence();
        let mut receiver = feed.take_receiver().unwrap();
        assert!(fence.ready());
        fixture.emit("InputVolumeMeters").await;
        fixture.received().await;
        assert_eq!(fence.picture_revision(), 0);
        assert!(
            fixture
                .bridge
                .ready_at_revision(fixture.owner.token().generation(), 0)
        );
        assert!(!fence.ready(), "delivery alone granted take admission");
        let waiting = fence.synchronize(Instant::now() + Duration::from_secs(2));
        tokio::pin!(waiting);
        assert!(futures_util::poll!(waiting.as_mut()).is_pending());
        apply(receiver.recv().await.unwrap(), |token, name, data| {
            assert_eq!(token, fixture.owner.token());
            assert_eq!(name, "InputVolumeMeters");
            assert_eq!(data, json!({"marker":7}));
            assert!(
                !fence.ready(),
                "permission advanced before the consumer update completed"
            );
        });
        waiting.await.unwrap();
        assert!(fence.ready());
        fixture.emit("InputMuteStateChanged").await;
        fixture.received().await;
        assert_eq!(fence.picture_revision(), 1);
        assert!(!fence.ready());
        apply(receiver.recv().await.unwrap(), |_, _, _| {});
        assert!(fence.ready());
        drop(feed);
        fixture.close().await;
    }

    #[tokio::test]
    async fn owner_drop_revokes_retained_clones_and_allows_a_fresh_exclusive_scope() {
        let fixture = Fixture::new(8).await;
        let feed = fixture.bridge.attach_take_feed().unwrap();
        let retained = feed.fence();
        assert!(fixture.bridge.attach_take_feed().is_err());
        assert!(retained.ready());
        drop(feed);
        assert!(!retained.ready());
        assert!(
            retained
                .synchronize(Instant::now() + Duration::from_secs(1))
                .await
                .is_err()
        );
        let replacement = fixture.bridge.attach_take_feed().unwrap();
        assert!(replacement.fence().ready());
        fixture.bridge.invalidate();
        assert!(!replacement.fence().ready());
        assert!(!retained.ready());
        drop(replacement);
        fixture.close().await;
    }

    #[tokio::test]
    async fn abandoning_an_unapplied_frame_revokes_without_acknowledging() {
        let fixture = Fixture::new(8).await;
        let mut feed = fixture.bridge.attach_take_feed().unwrap();
        let fence = feed.fence();
        let mut receiver = feed.take_receiver().unwrap();
        fixture.emit("InputVolumeMeters").await;
        fixture.received().await;
        drop(receiver.recv().await.unwrap());
        assert!(!fence.ready());
        assert!(
            fence
                .synchronize(Instant::now() + Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(receiver.recv().await.is_none());
        drop(feed);
        fixture.close().await;
    }

    #[tokio::test]
    async fn dequeued_unapplied_frame_keeps_original_transport_admission_charged() {
        let fixture = Fixture::new(1).await;
        let mut feed = fixture.bridge.attach_take_feed().unwrap();
        let fence = feed.fence();
        let mut receiver = feed.take_receiver().unwrap();
        fixture.emit("InputVolumeMeters").await;
        fixture.received().await;
        let retained = receiver.recv().await.unwrap();
        assert!(fixture.owner.handle().invalidation().is_none());
        fixture.emit("InputVolumeMeters").await;
        let result = fixture
            .owner
            .handle()
            .request(
                fixture.owner.token(),
                "GetSceneList",
                json!({}),
                Duration::from_secs(2),
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            fixture.owner.handle().invalidation(),
            Some(Invalidation::EventOverflow)
        );
        assert!(!fence.ready());
        drop(retained);
        drop(feed);
        fixture.close().await;
    }
}
