// SPDX-License-Identifier: GPL-2.0-only
//! Bounded response/event ownership, independent of TCP/HTTP framing.
use super::Class;
use futures_util::task::AtomicWaker;
use obs_mcp_native::{http_protocol::Response, stage_transport::Cancellation};
use std::{
    io,
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
const GLOBAL_BYTES: usize = 32 * 1024 * 1024;
const STREAM_BYTES: usize = 1024 * 1024;
const STREAM_ITEMS: usize = 16;
const RESPONSE_ITEMS: usize = 128;
// Two host-wide emergency classes; ordinary responses/listeners cannot borrow
// these pools. The transport must separately qualify pre-parse admission.
pub(super) const STOP_BODY_BYTES: usize = 16 * 1024;
pub(super) const STOP_OUTPUT_BYTES: usize = 64 * 1024;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    Closed,
    Capacity,
    Encoding,
}
/// Permits survive queue removal and stay charged while a transport borrows
/// bytes for its actual write/flush. Copying bytes leaves this pool's scope.
pub(crate) struct Packet {
    response: Response,
    _global: OwnedSemaphorePermit,
    _local: OwnedSemaphorePermit,
    _item: OwnedSemaphorePermit,
    _stop: Option<StopLease>,
}
impl Packet {
    pub(crate) fn response(&self) -> &Response {
        &self.response
    }
    /// The pinned graph already reexports bytes1.12.1 through tungstenite.
    /// A future Hyper Frame must move this owner-backed Bytes directly; copying
    /// its data into another queue would leave this permit accounting scope.
    pub(super) fn into_bytes(self) -> tokio_tungstenite::tungstenite::Bytes {
        struct OwnedPacket(Packet);
        impl AsRef<[u8]> for OwnedPacket {
            fn as_ref(&self) -> &[u8] {
                &self.0.response.body
            }
        }
        tokio_tungstenite::tungstenite::Bytes::from_owner(OwnedPacket(self))
    }
}
#[derive(Clone)]
struct StopPool {
    slot: Arc<Semaphore>,
    body: Arc<Semaphore>,
    output: Arc<Semaphore>,
}
impl StopPool {
    fn new() -> Self {
        Self {
            slot: Arc::new(Semaphore::new(1)),
            body: Arc::new(Semaphore::new(STOP_BODY_BYTES)),
            output: Arc::new(Semaphore::new(STOP_OUTPUT_BYTES)),
        }
    }
    fn reserve(&self, bytes: usize) -> Result<StopLease, Failure> {
        if bytes > STOP_BODY_BYTES {
            return Err(Failure::Capacity);
        }
        let slot = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Failure::Capacity)?;
        let body = self
            .body
            .clone()
            .try_acquire_many_owned(u32::try_from(bytes).map_err(|_| Failure::Capacity)?)
            .map_err(|_| Failure::Capacity)?;
        Ok(StopLease(Arc::new(StopCredits {
            _slot: slot,
            _body: body,
            output: self.output.clone(),
        })))
    }
}
struct StopCredits {
    _body: OwnedSemaphorePermit,
    output: Arc<Semaphore>,
    // Release raw custody before another exchange can acquire this class slot.
    _slot: OwnedSemaphorePermit,
}
/// Byte custody only: this grants no Host invocation, effect slot or consent.
/// The class slot/body credits outlive cancellation through every actual Packet.
#[derive(Clone)]
pub(super) struct StopLease(Arc<StopCredits>);
#[derive(Clone)]
pub(super) struct Budget {
    ordinary: Arc<Semaphore>,
    take: StopPool,
    safety: StopPool,
}
impl Budget {
    pub(super) fn new() -> Self {
        Self {
            ordinary: Arc::new(Semaphore::new(GLOBAL_BYTES)),
            take: StopPool::new(),
            safety: StopPool::new(),
        }
    }
    pub(super) fn reserve_stop(
        &self,
        class: Class,
        bytes: usize,
    ) -> Result<Option<StopLease>, Failure> {
        match class {
            Class::Ordinary => Ok(None),
            Class::TakeStop => self.take.reserve(bytes).map(Some),
            Class::SafetyStop => self.safety.reserve(bytes).map(Some),
        }
    }
    #[cfg(test)]
    pub(super) fn available(&self) -> usize {
        self.ordinary.available_permits()
    }
    #[cfg(test)]
    pub(super) fn stop_available(&self, class: Class) -> (usize, usize, usize) {
        let pool = match class {
            Class::TakeStop => &self.take,
            Class::SafetyStop => &self.safety,
            Class::Ordinary => panic!("not a reserved class"),
        };
        (
            pool.slot.available_permits(),
            pool.body.available_permits(),
            pool.output.available_permits(),
        )
    }
}
pub(super) struct Receiver {
    queue: mpsc::UnboundedReceiver<Packet>,
    closed: watch::Receiver<Option<Failure>>,
    wake: Arc<AtomicWaker>,
}
impl Receiver {
    pub(super) async fn next(&mut self) -> Option<Packet> {
        loop {
            if self.closed.borrow().is_some() {
                return self.queue.try_recv().ok();
            }
            tokio::select! {biased;packet=self.queue.recv()=>return packet,changed=self.closed.changed()=>if changed.is_err(){return self.queue.try_recv().ok();}}
        }
    }
    pub(super) fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Packet>> {
        // Register before observing closure; a failure with an empty queue must
        // wake a transport even while its Sender is still retained elsewhere.
        self.wake.register(cx.waker());
        if self.closed.borrow().is_some() {
            return std::task::Poll::Ready(self.queue.try_recv().ok());
        }
        self.queue.poll_recv(cx)
    }
    #[cfg(test)]
    pub(super) fn try_next(&mut self) -> Option<Packet> {
        self.queue.try_recv().ok()
    }
    pub(super) fn failure(&self) -> Option<Failure> {
        *self.closed.borrow()
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.queue.close();
    }
}
pub(super) struct Sender {
    queue: mpsc::UnboundedSender<Packet>,
    global: Arc<Semaphore>,
    local: Arc<Semaphore>,
    items: Arc<Semaphore>,
    max: usize,
    failure: watch::Sender<Option<Failure>>,
    departed: Cancellation,
    closed: Mutex<bool>,
    wake: Arc<AtomicWaker>,
    stop: Option<StopLease>,
}
pub(super) struct Reservation {
    sender: Arc<Sender>,
    packet: Packet,
}
impl Reservation {
    pub(super) fn commit(self) -> Result<(), Failure> {
        let open = self.sender.closed.lock().map_err(|_| Failure::Closed)?;
        if *open || self.sender.departed.is_cancelled() {
            return Err(Failure::Closed);
        }
        self.sender
            .queue
            .send(self.packet)
            .map_err(|_| Failure::Closed)
    }
}
impl Sender {
    pub(super) fn channel(
        budget: &Budget,
        stream: bool,
        departed: Cancellation,
    ) -> (Arc<Self>, Receiver) {
        Self::channel_reserved(budget, stream, departed, None)
    }
    pub(super) fn channel_reserved(
        budget: &Budget,
        stream: bool,
        departed: Cancellation,
        stop: Option<StopLease>,
    ) -> (Arc<Self>, Receiver) {
        let (queue, receiver) = mpsc::unbounded_channel();
        let (failure, closed) = watch::channel(None);
        let wake = Arc::new(AtomicWaker::new());
        let (max, items) = if stop.is_some() {
            (STOP_OUTPUT_BYTES, 1)
        } else if stream {
            (STREAM_BYTES, STREAM_ITEMS)
        } else {
            (GLOBAL_BYTES, RESPONSE_ITEMS)
        };
        (
            Arc::new(Self {
                queue,
                global: stop
                    .as_ref()
                    .map_or_else(|| budget.ordinary.clone(), |lease| lease.0.output.clone()),
                local: Arc::new(Semaphore::new(max)),
                items: Arc::new(Semaphore::new(items)),
                max,
                failure,
                departed,
                closed: Mutex::new(false),
                wake: wake.clone(),
                stop,
            }),
            Receiver {
                queue: receiver,
                closed,
                wake,
            },
        )
    }
    pub(super) fn reserved_stop(&self) -> bool {
        self.stop.is_some()
    }
    pub(super) fn available(&self) -> bool {
        !self.departed.is_cancelled()
            && !self.queue.is_closed()
            && self.closed.lock().is_ok_and(|open| !*open)
    }
    pub(super) fn fail(&self, reason: Failure) {
        if let Ok(mut closed) = self.closed.lock() {
            *closed = true;
        }
        self.failure.send_if_modified(|value| {
            if value.is_none() {
                *value = Some(reason);
                true
            } else {
                false
            }
        });
        self.departed.cancel();
        self.wake.wake();
    }
    fn credits(
        &self,
        bytes: usize,
    ) -> Result<
        (
            OwnedSemaphorePermit,
            OwnedSemaphorePermit,
            OwnedSemaphorePermit,
        ),
        Failure,
    > {
        if bytes > self.max || !self.available() {
            return Err(if bytes > self.max {
                Failure::Capacity
            } else {
                Failure::Closed
            });
        }
        let n = u32::try_from(bytes).map_err(|_| Failure::Capacity)?;
        let global = self
            .global
            .clone()
            .try_acquire_many_owned(n)
            .map_err(|_| Failure::Capacity)?;
        let local = self
            .local
            .clone()
            .try_acquire_many_owned(n)
            .map_err(|_| Failure::Capacity)?;
        let item = self
            .items
            .clone()
            .try_acquire_owned()
            .map_err(|_| Failure::Capacity)?;
        Ok((global, local, item))
    }
    pub(super) fn reserve(self: &Arc<Self>, response: Response) -> Result<Reservation, Failure> {
        let (global, local, item) = self.credits(response.body.len())?;
        Ok(Reservation {
            sender: self.clone(),
            packet: Packet {
                response,
                _global: global,
                _local: local,
                _item: item,
                _stop: self.stop.clone(),
            },
        })
    }
    pub(super) fn send(self: &Arc<Self>, response: Response) -> Result<(), Failure> {
        self.reserve(response)?.commit()
    }
    /// Count borrowed notification fields, acquire shared/per-stream credits,
    /// then allocate exactly one encoded frame. No URI/Value clone per listener.
    pub(super) fn event(
        self: &Arc<Self>,
        change: &obs_mcp_native::resource_events::Change,
        id: &serde_json::Value,
    ) -> Result<(), Failure> {
        struct Count {
            n: usize,
            max: usize,
        }
        impl io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.n = self
                    .n
                    .checked_add(bytes.len())
                    .filter(|n| *n <= self.max)
                    .ok_or_else(|| io::Error::other("notification frame bound"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let prefix = b"event: message\ndata: ";
        let suffix = b"\n\n";
        let mut count = Count {
            n: prefix.len() + suffix.len(),
            max: self.max,
        };
        crate::resource_notifications::write_event(&mut count, change, Some(id))
            .map_err(|_| Failure::Capacity)?;
        let (global, local, item) = self.credits(count.n)?;
        let mut body = Vec::with_capacity(count.n);
        body.extend_from_slice(prefix);
        crate::resource_notifications::write_event(&mut body, change, Some(id))
            .map_err(|_| Failure::Encoding)?;
        body.extend_from_slice(suffix);
        if body.len() != count.n {
            return Err(Failure::Encoding);
        }
        let response = Response {
            status: 200,
            content_type: Some("text/event-stream"),
            authenticate_bearer: false,
            body,
        };
        Reservation {
            sender: self.clone(),
            packet: Packet {
                response,
                _global: global,
                _local: local,
                _item: item,
                _stop: self.stop.clone(),
            },
        }
        .commit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
    };
    #[test]
    fn empty_stream_failure_wakes_reader_while_sender_is_retained() {
        struct Wake(AtomicUsize);
        impl futures_util::task::ArcWake for Wake {
            fn wake_by_ref(this: &Arc<Self>) {
                this.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let budget = Budget::new();
        let (sender, mut receiver) = Sender::channel(&budget, true, Cancellation::new());
        let counter = Arc::new(Wake(AtomicUsize::new(0)));
        let waker = futures_util::task::waker(counter.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(receiver.poll_next(&mut cx).is_pending());
        sender.fail(Failure::Capacity);
        assert!(counter.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(receiver.poll_next(&mut cx), Poll::Ready(None)));
        assert_eq!(receiver.failure(), Some(Failure::Capacity));
    }
    #[tokio::test]
    async fn dequeued_packet_keeps_shared_bytes_and_item_credits_until_drop() {
        let budget = Budget::new();
        let (sender, mut receiver) = Sender::channel(&budget, true, Cancellation::new());
        let frame = || Response {
            status: 200,
            content_type: None,
            authenticate_bearer: false,
            body: vec![b'x'; 65536],
        };
        for _ in 0..STREAM_ITEMS {
            sender.send(frame()).unwrap();
        }
        assert_eq!(budget.available(), GLOBAL_BYTES - STREAM_BYTES);
        let retained = receiver.next().await.unwrap();
        assert_eq!(budget.available(), GLOBAL_BYTES - STREAM_BYTES);
        assert!(matches!(sender.send(frame()), Err(Failure::Capacity)));
        drop(retained);
        sender.send(frame()).unwrap();
        drop(receiver);
        assert_eq!(budget.available(), GLOBAL_BYTES);
    }
    #[tokio::test]
    async fn cancellation_keeps_stop_slot_body_and_output_until_actual_packet_drop() {
        let budget = Budget::new();
        let lease = budget
            .reserve_stop(Class::SafetyStop, 128)
            .unwrap()
            .unwrap();
        assert!(budget.reserve_stop(Class::SafetyStop, 1).is_err());
        let (sender, mut receiver) =
            Sender::channel_reserved(&budget, false, Cancellation::new(), Some(lease.clone()));
        sender
            .send(Response {
                status: 200,
                content_type: None,
                authenticate_bearer: false,
                body: vec![0; STOP_OUTPUT_BYTES],
            })
            .unwrap();
        let packet = receiver.next().await.unwrap();
        sender.fail(Failure::Closed);
        drop(receiver);
        drop(sender);
        drop(lease);
        assert_eq!(
            budget.stop_available(Class::SafetyStop),
            (0, STOP_BODY_BYTES - 128, 0)
        );
        assert!(budget.reserve_stop(Class::SafetyStop, 1).is_err());
        let take = budget.reserve_stop(Class::TakeStop, 1).unwrap().unwrap();
        drop(take);
        drop(packet);
        assert_eq!(
            budget.stop_available(Class::SafetyStop),
            (1, STOP_BODY_BYTES, STOP_OUTPUT_BYTES)
        );
        assert_eq!(budget.available(), GLOBAL_BYTES);
        assert!(
            budget
                .reserve_stop(Class::TakeStop, STOP_BODY_BYTES + 1)
                .is_err()
        );
        assert_eq!(
            budget.stop_available(Class::TakeStop),
            (1, STOP_BODY_BYTES, STOP_OUTPUT_BYTES)
        );
    }
    #[tokio::test]
    async fn owner_backed_bytes_clones_and_slices_keep_stop_credits_until_last_write_owner_drops() {
        let budget = Budget::new();
        let lease = budget
            .reserve_stop(Class::SafetyStop, 128)
            .unwrap()
            .unwrap();
        let (sender, mut reader) =
            Sender::channel_reserved(&budget, false, Cancellation::new(), Some(lease.clone()));
        sender
            .send(Response {
                status: 200,
                content_type: None,
                authenticate_bearer: false,
                body: vec![b'x'; STOP_OUTPUT_BYTES],
            })
            .unwrap();
        let packet = reader.next().await.unwrap();
        let original = packet.response().body.as_ptr();
        let bytes = packet.into_bytes();
        assert_eq!(bytes.as_ptr(), original);
        let clone = bytes.clone();
        let tail = clone.slice(1..);
        assert_eq!(tail.as_ptr(), bytes.as_ptr().wrapping_add(1));
        sender.fail(Failure::Closed);
        drop(reader);
        drop(sender);
        drop(lease);
        drop(bytes);
        drop(clone);
        assert_eq!(
            budget.stop_available(Class::SafetyStop),
            (0, STOP_BODY_BYTES - 128, 0)
        );
        assert!(budget.reserve_stop(Class::SafetyStop, 1).is_err());
        drop(tail);
        assert_eq!(
            budget.stop_available(Class::SafetyStop),
            (1, STOP_BODY_BYTES, STOP_OUTPUT_BYTES)
        );
    }
    #[test]
    fn independent_streams_share_one_global_encoded_budget() {
        let budget = Budget::new();
        let mut receivers = Vec::new();
        let mut senders = Vec::new();
        for _ in 0..32 {
            let (sender, receiver) = Sender::channel(&budget, true, Cancellation::new());
            sender
                .send(Response {
                    status: 200,
                    content_type: None,
                    authenticate_bearer: false,
                    body: vec![0; STREAM_BYTES],
                })
                .unwrap();
            receivers.push(receiver);
            senders.push(sender);
        }
        assert_eq!(budget.available(), 0);
        let (sender, _receiver) = Sender::channel(&budget, true, Cancellation::new());
        assert!(matches!(
            sender.send(Response {
                status: 200,
                content_type: None,
                authenticate_bearer: false,
                body: vec![0]
            }),
            Err(Failure::Capacity)
        ));
        receivers.pop();
        assert_eq!(budget.available(), STREAM_BYTES);
        sender
            .send(Response {
                status: 200,
                content_type: None,
                authenticate_bearer: false,
                body: vec![0],
            })
            .unwrap();
        drop(receivers);
        drop(_receiver);
        assert_eq!(budget.available(), GLOBAL_BYTES);
    }
}
