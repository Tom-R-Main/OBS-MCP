// SPDX-License-Identifier: GPL-2.0-only
//! Transport-neutral admission and retained connection/body ownership.
//! No sockets, HTTP framing, URL normalization, Host or effect capability lives
//! here. A mature HTTP driver supplies validated heads and decoded body data.
use crate::{
    http_protocol::{self, HeadRefusal, Policy, RequestHead},
    stage_transport::Cancellation,
};
use futures_util::{Stream, StreamExt};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::Instant,
};
pub const ORDINARY_CONNECTIONS: usize = 32;
pub const EMERGENCY_CONNECTIONS: usize = 2;
pub const ORDINARY_BODY_BYTES: usize = 32 * 1024 * 1024;
pub const EMERGENCY_BODY_BYTES: usize = 16 * 1024;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Ordinary,
    Emergency,
}
#[derive(Debug)]
pub enum Failure {
    Closed,
    Capacity,
    TooLarge,
    LengthMismatch,
    Deadline,
    Cancelled,
    Transport,
    DriverPanicked,
    EmergencyUsed,
    Head(HeadRefusal),
}
struct State {
    ordinary: Arc<Semaphore>,
    emergency: Arc<Semaphore>,
    ordinary_body: Arc<Semaphore>,
    emergency_body: Arc<Semaphore>,
    closing: AtomicBool,
    shutdown: Cancellation,
}
pub struct Ingress {
    state: Arc<State>,
}
impl Default for Ingress {
    fn default() -> Self {
        Self::new()
    }
}
impl Ingress {
    pub fn new() -> Self {
        Self {
            state: Arc::new(State {
                ordinary: Arc::new(Semaphore::new(ORDINARY_CONNECTIONS)),
                emergency: Arc::new(Semaphore::new(EMERGENCY_CONNECTIONS)),
                ordinary_body: Arc::new(Semaphore::new(ORDINARY_BODY_BYTES)),
                emergency_body: Arc::new(Semaphore::new(
                    EMERGENCY_CONNECTIONS * EMERGENCY_BODY_BYTES,
                )),
                closing: AtomicBool::new(false),
                shutdown: Cancellation::new(),
            }),
        }
    }
    /// Nonblocking: no caller-declared emergency flag and no ingress queue.
    pub fn admit(&self) -> Result<Connection, Failure> {
        if self.state.closing.load(Ordering::Acquire) {
            return Err(Failure::Closed);
        }
        let (kind, permit) = match self.state.ordinary.clone().try_acquire_owned() {
            Ok(permit) => (Kind::Ordinary, permit),
            Err(_) => (
                Kind::Emergency,
                self.state
                    .emergency
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Failure::Capacity)?,
            ),
        };
        if self.state.closing.load(Ordering::Acquire) {
            return Err(Failure::Closed);
        }
        Ok(Connection {
            state: Arc::new(ConnectionState {
                kind,
                _connection: permit,
                reader: Arc::new(Semaphore::new(1)),
                gate: Arc::downgrade(&self.state),
                shutdown: self.state.shutdown.clone(),
                departed: Cancellation::new(),
                emergency_used: AtomicBool::new(false),
            }),
        })
    }
    pub fn close(&self) {
        self.state.closing.store(true, Ordering::Release);
        self.state.shutdown.cancel();
    }
    /// Wait for actual leases, not just a socket leader or task's result.
    /// The separate Connections owner additionally joins every driver future.
    pub async fn drained(&self) -> Result<(), Failure> {
        if !self.state.closing.load(Ordering::Acquire) {
            return Err(Failure::Closed);
        }
        let ordinary = self
            .state
            .ordinary
            .clone()
            .acquire_many_owned(ORDINARY_CONNECTIONS as u32)
            .await
            .map_err(|_| Failure::Closed)?;
        let emergency = self
            .state
            .emergency
            .clone()
            .acquire_many_owned(EMERGENCY_CONNECTIONS as u32)
            .await
            .map_err(|_| Failure::Closed)?;
        drop((ordinary, emergency));
        Ok(())
    }
    #[cfg(test)]
    fn available(&self) -> (usize, usize, usize, usize) {
        (
            self.state.ordinary.available_permits(),
            self.state.emergency.available_permits(),
            self.state.ordinary_body.available_permits(),
            self.state.emergency_body.available_permits(),
        )
    }
}
impl Drop for Ingress {
    fn drop(&mut self) {
        self.close();
    }
}
struct ConnectionState {
    kind: Kind,
    _connection: OwnedSemaphorePermit,
    reader: Arc<Semaphore>,
    gate: Weak<State>,
    shutdown: Cancellation,
    departed: Cancellation,
    emergency_used: AtomicBool,
}
/// A reader or completed body retains the connection lease. A detached body
/// cannot refund admission while another owner still uses its bytes.
pub struct Connection {
    state: Arc<ConnectionState>,
}
impl Connection {
    pub fn kind(&self) -> Kind {
        self.state.kind
    }
    pub fn depart(&self) {
        self.state.departed.cancel();
    }
    pub async fn shutdown_requested(&self) {
        self.state.shutdown.cancelled().await;
    }
    /// `length` comes only from the mature decoder's validated size hint. None
    /// reserves the whole class maximum; known length reserves exact bytes.
    /// `deadline` is the original absolute transport deadline, never restamped.
    pub async fn collect<S, B, E>(
        &self,
        head: &RequestHead<'_>,
        policy: &Policy,
        length: Option<usize>,
        deadline: Instant,
        stream: &mut S,
    ) -> Result<Body, Failure>
    where
        S: Stream<Item = Result<B, E>> + Unpin,
        B: AsRef<[u8]>,
    {
        http_protocol::preflight(head, policy).map_err(Failure::Head)?;
        let reader = self.reader(length, deadline)?;
        reader.collect(stream).await
    }
    fn reader(&self, length: Option<usize>, deadline: Instant) -> Result<Reader, Failure> {
        if self.state.shutdown.is_cancelled() {
            return Err(Failure::Closed);
        }
        if self.state.departed.is_cancelled() {
            return Err(Failure::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(Failure::Deadline);
        }
        let max = match self.state.kind {
            Kind::Ordinary => http_protocol::MAX_BODY_BYTES,
            Kind::Emergency => EMERGENCY_BODY_BYTES,
        };
        let capacity = length.unwrap_or(max);
        if capacity > max {
            return Err(Failure::TooLarge);
        }
        let gate = self.state.gate.upgrade().ok_or(Failure::Closed)?;
        let item = self
            .state
            .reader
            .clone()
            .try_acquire_owned()
            .map_err(|_| Failure::Capacity)?;
        let pool = match self.state.kind {
            Kind::Ordinary => gate.ordinary_body.clone(),
            Kind::Emergency => gate.emergency_body.clone(),
        };
        let bytes = pool
            .try_acquire_many_owned(u32::try_from(capacity).map_err(|_| Failure::TooLarge)?)
            .map_err(|_| Failure::Capacity)?;
        if self.state.kind == Kind::Emergency
            && self.state.emergency_used.swap(true, Ordering::AcqRel)
        {
            return Err(Failure::EmergencyUsed);
        }
        Ok(Reader {
            connection: self.state.clone(),
            _item: item,
            _bytes: bytes,
            capacity,
            length,
            deadline,
        })
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.depart();
    }
}
struct Reader {
    connection: Arc<ConnectionState>,
    _item: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    capacity: usize,
    length: Option<usize>,
    deadline: Instant,
}
impl Reader {
    fn live(&self) -> Result<(), Failure> {
        if self.connection.shutdown.is_cancelled() {
            return Err(Failure::Closed);
        }
        if self.connection.departed.is_cancelled() {
            return Err(Failure::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(Failure::Deadline);
        }
        Ok(())
    }
    async fn collect<S, B, E>(self, stream: &mut S) -> Result<Body, Failure>
    where
        S: Stream<Item = Result<B, E>> + Unpin,
        B: AsRef<[u8]>,
    {
        self.live()?;
        // Reserve before allocation; fixed capacity avoids geometric growth
        // exceeding the body's admitted byte custody for non-power-of-two sizes.
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.capacity)
            .map_err(|_| Failure::Capacity)?;
        loop {
            self.live()?;
            let next = tokio::select! {biased;
                ()=self.connection.shutdown.cancelled()=>return Err(Failure::Closed),
                ()=self.connection.departed.cancelled()=>return Err(Failure::Cancelled),
                ()=tokio::time::sleep_until(self.deadline)=>return Err(Failure::Deadline),
                next=stream.next()=>next,
            };
            match next {
                Some(Ok(chunk)) => {
                    let chunk = chunk.as_ref();
                    let length = bytes
                        .len()
                        .checked_add(chunk.len())
                        .filter(|n| *n <= self.capacity)
                        .ok_or(Failure::TooLarge)?;
                    debug_assert!(length <= self.capacity);
                    bytes.extend_from_slice(chunk);
                }
                Some(Err(_)) => return Err(Failure::Transport),
                None => {
                    if self.length.is_some_and(|n| n != bytes.len()) {
                        return Err(Failure::LengthMismatch);
                    }
                    return Ok(Body {
                        bytes,
                        reader: self,
                    });
                }
            }
            // Empty/one-byte decoded frames must not monopolize the Host's
            // runtime. This yields cooperatively without resetting a deadline.
            tokio::task::consume_budget().await;
        }
    }
}
/// Handoff is synchronous and retains reader credits until the next owner has
/// admitted the body. Emergency bytes are not authority to dispatch a call.
pub struct Body {
    bytes: Vec<u8>,
    reader: Reader,
}
impl Body {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn deadline(&self) -> Instant {
        self.reader.deadline
    }
    pub fn handoff<T>(self, admit: impl FnOnce(Vec<u8>, Kind, Instant) -> T) -> Result<T, Failure> {
        self.reader.live()?;
        let Self { bytes, reader } = self;
        let result = admit(bytes, reader.connection.kind, reader.deadline);
        drop(reader);
        Ok(result)
    }
}
/// Owns only bounded transport drivers, never a Runtime/Service/Take owner.
/// The server owns one independent retained Host loop and always awaits both
/// its global close and this joined driver shutdown.
pub struct Connections {
    ingress: Ingress,
    jobs: JoinSet<Result<(), Failure>>,
    failure: Option<Failure>,
}
impl Default for Connections {
    fn default() -> Self {
        Self::new()
    }
}
impl Connections {
    pub fn new() -> Self {
        Self {
            ingress: Ingress::new(),
            jobs: JoinSet::new(),
            failure: None,
        }
    }
    pub fn spawn<F, U>(&mut self, driver: F) -> Result<(), Failure>
    where
        F: FnOnce(Connection) -> U + Send + 'static,
        U: std::future::Future<Output = Result<(), Failure>> + Send + 'static,
    {
        // Retire completed driver results before admitting another connection;
        // bounded active sockets alone do not bound an unpolled JoinSet.
        while let Some(result) = self.jobs.try_join_next() {
            if let Err(reason) = result.unwrap_or(Err(Failure::DriverPanicked)) {
                self.failure.get_or_insert(reason);
            }
        }
        let connection = self.ingress.admit()?;
        let held = connection.state.clone();
        self.jobs.spawn(async move {
            let result = driver(connection).await;
            drop(held);
            result
        });
        Ok(())
    }
    pub async fn join_next(&mut self) -> Option<Result<(), Failure>> {
        self.jobs
            .join_next()
            .await
            .map(|result| result.unwrap_or(Err(Failure::DriverPanicked)))
    }
    pub async fn shutdown(&mut self) -> Result<(), Failure> {
        self.ingress.close();
        let mut failed = self.failure.take();
        while let Some(result) = self.join_next().await {
            if let Err(reason) = result {
                failed.get_or_insert(reason);
            }
        }
        self.ingress.drained().await?;
        failed.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_protocol::Response;
    use futures_util::{poll, stream};
    use std::{sync::atomic::AtomicUsize, task::Poll, time::Duration};
    use tokio::sync::oneshot;
    fn head() -> RequestHead<'static> {
        RequestHead {
            method: "POST",
            target: "/mcp",
            headers: &[
                ("host", "127.0.0.1:4455"),
                ("content-type", "application/json"),
            ],
        }
    }
    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }
    fn restored(gate: &Ingress) {
        assert_eq!(
            gate.available(),
            (32, 2, ORDINARY_BODY_BYTES, 2 * EMERGENCY_BODY_BYTES)
        );
    }
    #[tokio::test]
    async fn global32_plus2_connection_scope_is_derived_from_capacity_and_drains() {
        let gate = Ingress::new();
        let mut ordinary = Vec::new();
        let mut emergency = Vec::new();
        for _ in 0..32 {
            let connection = gate.admit().unwrap();
            assert_eq!(connection.kind(), Kind::Ordinary);
            ordinary.push(connection);
        }
        for _ in 0..2 {
            let connection = gate.admit().unwrap();
            assert_eq!(connection.kind(), Kind::Emergency);
            emergency.push(connection);
        }
        assert!(matches!(gate.admit(), Err(Failure::Capacity)));
        ordinary.pop();
        let replacement = gate.admit().unwrap();
        assert_eq!(replacement.kind(), Kind::Ordinary);
        gate.close();
        assert!(matches!(gate.admit(), Err(Failure::Closed)));
        drop(replacement);
        drop(ordinary);
        drop(emergency);
        gate.drained().await.unwrap();
        restored(&gate);
    }
    #[test]
    fn body_pools_are_aggregate_and_emergency_readers_never_borrow_ordinary_bytes() {
        let gate = Ingress::new();
        let connections = (0..34).map(|_| gate.admit().unwrap()).collect::<Vec<_>>();
        let readers = connections[..8]
            .iter()
            .map(|connection| connection.reader(None, deadline()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(gate.available().2, 0);
        assert!(matches!(
            connections[8].reader(None, deadline()),
            Err(Failure::Capacity)
        ));
        let a = connections[32].reader(None, deadline()).unwrap();
        let b = connections[33].reader(None, deadline()).unwrap();
        assert_eq!(gate.available().3, 0);
        assert!(matches!(
            connections[32].reader(Some(0), deadline()),
            Err(Failure::Capacity)
        ));
        drop(a);
        assert_eq!(gate.available().3, EMERGENCY_BODY_BYTES);
        drop(b);
        drop(readers);
        drop(connections);
        restored(&gate);
    }
    #[tokio::test]
    async fn path_host_origin_auth_method_and_header_refusals_do_not_poll_body() {
        let gate = Ingress::new();
        let connection = gate.admit().unwrap();
        let polls = AtomicUsize::new(0);
        let policy = Policy::new(Some("fixture-http-token"));
        let cases = [
            ("POST", "/other", vec![("host", "example.invalid")], 404),
            ("POST", "/mcp", vec![], 403),
            (
                "POST",
                "/mcp",
                vec![("host", "127.0.0.1"), ("origin", "http://example.invalid")],
                403,
            ),
            ("GET", "/mcp", vec![("host", "127.0.0.1")], 401),
            (
                "GET",
                "/mcp",
                vec![
                    ("host", "127.0.0.1"),
                    ("authorization", "Bearer fixture-http-token"),
                ],
                405,
            ),
            (
                "POST",
                "/mcp",
                vec![
                    ("host", "127.0.0.1"),
                    ("authorization", "Bearer fixture-http-token"),
                ],
                415,
            ),
        ];
        for (method, target, headers, status) in cases {
            let head = RequestHead {
                method,
                target,
                headers: &headers,
            };
            let mut body = stream::poll_fn(|_| {
                polls.fetch_add(1, Ordering::SeqCst);
                Poll::<Option<Result<Vec<u8>, ()>>>::Ready(None)
            });
            let result = connection
                .collect(&head, &policy, Some(usize::MAX), Instant::now(), &mut body)
                .await;
            let Err(Failure::Head(HeadRefusal::Reply(reply))) = result else {
                panic!("head refusal did not precede body bounds/deadline")
            };
            assert_eq!(reply.status, status);
        }
        let value = "x".repeat(http_protocol::MAX_HEADER_BYTES);
        let headers = [("host", "127.0.0.1"), ("x-bound", value.as_str())];
        let mut body = stream::poll_fn(|_| {
            polls.fetch_add(1, Ordering::SeqCst);
            Poll::<Option<Result<Vec<u8>, ()>>>::Ready(None)
        });
        assert!(matches!(
            connection
                .collect(
                    &RequestHead {
                        method: "POST",
                        target: "/mcp",
                        headers: &headers
                    },
                    &policy,
                    None,
                    deadline(),
                    &mut body
                )
                .await,
            Err(Failure::Head(HeadRefusal::Reply(Response {
                status: 431,
                ..
            })))
        ));
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(connection);
        restored(&gate);
    }
    #[tokio::test]
    async fn unqualified_url_and_duplicate_headers_refuse_before_body_polling() {
        let gate = Ingress::new();
        let connection = gate.admit().unwrap();
        let policy = Policy::new(None);
        for (headers, reason) in [
            (
                vec![
                    ("host", "%31%32%37.0.0.1"),
                    ("content-type", "application/json"),
                ],
                http_protocol::Unqualified::UrlForm,
            ),
            (
                vec![("host", "127.0.0.1"), ("Host", "127.0.0.1")],
                http_protocol::Unqualified::DuplicateHeader,
            ),
        ] {
            let mut body = stream::poll_fn(|_| -> Poll<Option<Result<Vec<u8>, ()>>> {
                panic!("unqualified head polled a body")
            });
            let result = connection
                .collect(
                    &RequestHead {
                        method: "POST",
                        target: "/mcp",
                        headers: &headers,
                    },
                    &policy,
                    None,
                    deadline(),
                    &mut body,
                )
                .await;
            assert!(
                matches!(result,Err(Failure::Head(HeadRefusal::Unqualified(actual)))if actual==reason)
            );
        }
        drop(connection);
        restored(&gate);
    }
    #[tokio::test]
    async fn exact_body_lengths_deadlines_and_handoff_keep_original_bytes_and_custody() {
        let gate = Ingress::new();
        let connection = gate.admit().unwrap();
        let original = br#"{ "z":1, "a":2 }"#.to_vec();
        let stamp = deadline();
        let mut chunks = stream::iter([
            Ok::<_, ()>(original[..5].to_vec()),
            Ok(original[5..].to_vec()),
        ]);
        let body = connection
            .collect(
                &head(),
                &Policy::new(None),
                Some(original.len()),
                stamp,
                &mut chunks,
            )
            .await
            .unwrap();
        assert_eq!(body.deadline(), stamp);
        assert_eq!(body.bytes(), original);
        assert_eq!(gate.available().2, ORDINARY_BODY_BYTES - original.len());
        assert!(matches!(
            connection.reader(Some(0), deadline()),
            Err(Failure::Capacity)
        ));
        let result = body
            .handoff(|bytes, kind, deadline| {
                assert_eq!(kind, Kind::Ordinary);
                assert_eq!(deadline, stamp);
                assert_eq!(bytes, original);
                assert_eq!(gate.available().2, ORDINARY_BODY_BYTES - original.len());
                7
            })
            .unwrap();
        assert_eq!(result, 7);
        assert_eq!(gate.available().2, ORDINARY_BODY_BYTES);
        drop(connection);
        restored(&gate);
    }
    #[tokio::test]
    async fn declared_limits_truncation_chunk_overflow_and_stream_errors_refund_without_handoff() {
        let gate = Ingress::new();
        let connections = (0..33).map(|_| gate.admit().unwrap()).collect::<Vec<_>>();
        let policy = Policy::new(None);
        let mut never = stream::poll_fn(|_| -> Poll<Option<Result<Vec<u8>, ()>>> {
            panic!("oversized declared body was polled")
        });
        assert!(matches!(
            connections[0]
                .collect(
                    &head(),
                    &policy,
                    Some(http_protocol::MAX_BODY_BYTES + 1),
                    deadline(),
                    &mut never
                )
                .await,
            Err(Failure::TooLarge)
        ));
        assert!(matches!(
            connections[32]
                .collect(
                    &head(),
                    &policy,
                    Some(EMERGENCY_BODY_BYTES + 1),
                    deadline(),
                    &mut never
                )
                .await,
            Err(Failure::TooLarge)
        ));
        let mut short = stream::iter([Ok::<_, ()>(vec![1, 2, 3])]);
        assert!(matches!(
            connections[0]
                .collect(&head(), &policy, Some(4), deadline(), &mut short)
                .await,
            Err(Failure::LengthMismatch)
        ));
        let mut overflow = stream::iter([Ok::<_, ()>(vec![0; EMERGENCY_BODY_BYTES]), Ok(vec![0])]);
        assert!(matches!(
            connections[32]
                .collect(&head(), &policy, None, deadline(), &mut overflow)
                .await,
            Err(Failure::TooLarge)
        ));
        let mut broken = stream::iter([Err::<Vec<u8>, _>(())]);
        assert!(matches!(
            connections[0]
                .collect(&head(), &policy, None, deadline(), &mut broken)
                .await,
            Err(Failure::Transport)
        ));
        drop(connections);
        restored(&gate);
    }
    #[tokio::test]
    async fn cancelled_or_expired_completed_body_cannot_call_handoff_and_keeps_lease_until_drop() {
        let gate = Ingress::new();
        let connection = gate.admit().unwrap();
        let mut stream = stream::iter([Ok::<_, ()>(vec![1, 2, 3])]);
        let policy = Policy::new(None);
        let body = connection
            .collect(&head(), &policy, Some(3), deadline(), &mut stream)
            .await
            .unwrap();
        connection.depart();
        drop(connection);
        assert_eq!(gate.available().0, 31);
        assert_eq!(gate.available().2, ORDINARY_BODY_BYTES - 3);
        assert!(matches!(
            body.handoff(|_, _, _| panic!("canceled body handed off")),
            Err(Failure::Cancelled)
        ));
        restored(&gate);
        let connection = gate.admit().unwrap();
        let mut stream = stream::iter([Ok::<_, ()>(vec![1])]);
        let mut body = connection
            .collect(&head(), &policy, Some(1), deadline(), &mut stream)
            .await
            .unwrap();
        body.reader.deadline = Instant::now() - Duration::from_secs(1);
        assert!(matches!(
            body.handoff(|_, _, _| panic!("expired body handed off")),
            Err(Failure::Deadline)
        ));
        drop(connection);
        restored(&gate);
    }
    #[tokio::test]
    async fn departure_global_close_and_future_drop_release_pending_readers_without_deadline_reset()
    {
        let gate = Ingress::new();
        let connection = gate.admit().unwrap();
        let policy = Policy::new(None);
        let head = head();
        let stamp = deadline();
        let mut pending = stream::pending::<Result<Vec<u8>, ()>>();
        let mut collecting =
            Box::pin(connection.collect(&head, &policy, None, stamp, &mut pending));
        assert!(poll!(&mut collecting).is_pending());
        assert_eq!(
            gate.available().2,
            ORDINARY_BODY_BYTES - http_protocol::MAX_BODY_BYTES
        );
        drop(collecting);
        assert_eq!(gate.available().2, ORDINARY_BODY_BYTES);
        let mut collecting =
            Box::pin(connection.collect(&head, &policy, None, stamp, &mut pending));
        assert!(poll!(&mut collecting).is_pending());
        connection.depart();
        assert!(matches!(collecting.await, Err(Failure::Cancelled)));
        drop(connection);
        let connection = gate.admit().unwrap();
        let mut collecting =
            Box::pin(connection.collect(&head, &policy, None, stamp, &mut pending));
        assert!(poll!(&mut collecting).is_pending());
        gate.close();
        assert!(matches!(collecting.await, Err(Failure::Closed)));
        drop(connection);
        gate.drained().await.unwrap();
        restored(&gate);
    }
    #[tokio::test]
    async fn expired_original_deadline_refuses_without_polling_or_reserving_body() {
        let gate = Ingress::new();
        let connection = gate.admit().unwrap();
        let mut body = stream::poll_fn(|_| -> Poll<Option<Result<Vec<u8>, ()>>> {
            panic!("expired request polled body")
        });
        assert!(matches!(
            connection
                .collect(
                    &head(),
                    &Policy::new(None),
                    None,
                    Instant::now() - Duration::from_secs(1),
                    &mut body
                )
                .await,
            Err(Failure::Deadline)
        ));
        assert_eq!(gate.available().2, ORDINARY_BODY_BYTES);
        drop(connection);
        restored(&gate);
    }
    #[tokio::test]
    async fn joined_shutdown_waits_for_retained_driver_write_and_joins_peers_after_error() {
        let mut owner = Connections::new();
        let (started_tx, started_rx) = oneshot::channel();
        let (write_tx, write_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let finished = Arc::new(AtomicUsize::new(0));
        let done = finished.clone();
        owner
            .spawn(move |connection| async move {
                started_tx.send(()).unwrap();
                connection.shutdown_requested().await;
                write_tx.send(()).unwrap();
                release_rx.await.unwrap();
                drop(connection);
                done.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        let done = finished.clone();
        owner
            .spawn(move |_| async move {
                done.fetch_add(1, Ordering::SeqCst);
                Err(Failure::Transport)
            })
            .unwrap();
        started_rx.await.unwrap();
        let mut closing = Box::pin(owner.shutdown());
        assert!(poll!(&mut closing).is_pending());
        write_rx.await.unwrap();
        assert!(poll!(&mut closing).is_pending());
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        release_tx.send(()).unwrap();
        assert!(matches!(closing.await, Err(Failure::Transport)));
        assert_eq!(finished.load(Ordering::SeqCst), 2);
        restored(&owner.ingress);
    }
    #[tokio::test]
    async fn supervisor_keeps_connection_credit_after_driver_drops_guard_but_write_is_pending() {
        let mut owner = Connections::new();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        owner
            .spawn(move |connection| async move {
                drop(connection);
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Ok(())
            })
            .unwrap();
        started_rx.await.unwrap();
        assert_eq!(owner.ingress.available().0, 31);
        let mut closing = Box::pin(owner.shutdown());
        assert!(poll!(&mut closing).is_pending());
        release_tx.send(()).unwrap();
        closing.await.unwrap();
        restored(&owner.ingress);
    }
    #[tokio::test]
    async fn connection_driver_panics_do_not_skip_other_joins_or_leak_admission() {
        let mut owner = Connections::new();
        let (done_tx, done_rx) = oneshot::channel();
        owner
            .spawn(|_| async {
                panic!("fixture driver panic");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();
        owner
            .spawn(|connection| async move {
                connection.shutdown_requested().await;
                done_tx.send(()).unwrap();
                Ok(())
            })
            .unwrap();
        assert!(matches!(
            owner.shutdown().await,
            Err(Failure::DriverPanicked)
        ));
        done_rx.await.unwrap();
        restored(&owner.ingress);
    }
    #[tokio::test]
    async fn emergency_connection_allows_one_decoded_body_and_cannot_keep_alive_as_ordinary() {
        let gate = Ingress::new();
        let ordinary = (0..32).map(|_| gate.admit().unwrap()).collect::<Vec<_>>();
        let emergency = gate.admit().unwrap();
        let mut chunks = stream::iter([Ok::<_, ()>(vec![1])]);
        let body = emergency
            .collect(
                &head(),
                &Policy::new(None),
                Some(1),
                deadline(),
                &mut chunks,
            )
            .await
            .unwrap();
        drop(body);
        assert!(matches!(
            emergency.reader(Some(0), deadline()),
            Err(Failure::EmergencyUsed)
        ));
        drop(emergency);
        drop(ordinary);
        restored(&gate);
    }
    #[tokio::test]
    async fn completed_driver_results_are_reaped_before_admission_without_losing_failures() {
        let mut owner = Connections::new();
        for index in 0..256 {
            let (done_tx, done_rx) = oneshot::channel();
            owner
                .spawn(move |_| async move {
                    done_tx.send(()).unwrap();
                    if index == 0 {
                        Err(Failure::Transport)
                    } else {
                        Ok(())
                    }
                })
                .unwrap();
            done_rx.await.unwrap();
            assert!(owner.jobs.len() <= ORDINARY_CONNECTIONS + EMERGENCY_CONNECTIONS);
        }
        assert!(matches!(owner.shutdown().await, Err(Failure::Transport)));
        assert!(owner.jobs.is_empty());
        restored(&owner.ingress);
    }
    #[tokio::test]
    async fn draining_counts_body_custody_after_connection_guard_and_driver_are_gone() {
        let gate = Ingress::new();
        let connection = gate.admit().unwrap();
        let mut chunks = stream::iter([Ok::<_, ()>(vec![1])]);
        let body = connection
            .collect(
                &head(),
                &Policy::new(None),
                Some(1),
                deadline(),
                &mut chunks,
            )
            .await
            .unwrap();
        drop(connection);
        gate.close();
        let mut draining = Box::pin(gate.drained());
        assert!(poll!(&mut draining).is_pending());
        drop(body);
        draining.await.unwrap();
        restored(&gate);
    }
}
