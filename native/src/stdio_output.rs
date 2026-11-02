// SPDX-License-Identifier: GPL-2.0-only
//! One retained writer. Admission, enqueue and local flush are distinct facts.
//! Cancellation of a completion waiter cannot cancel or replay owned pipe I/O.
use serde_json::Value;
use std::{
    future::Future,
    io,
    io::Write,
    mem::size_of,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
};
const ITEMS: usize = 128;
const BYTES: usize = 32 * 1024 * 1024;
const TEMPLATE_ENTRIES: usize = 4096;
const HEADER_BYTES: usize = 256 * 1024;
const FRAMES: usize = 256 * 1024;
const AMPLIFIED_BYTES: usize = 256 * 1024 * 1024;

/// A finite pattern, repeated without materializing each emitted copy. Values
/// are borrowed only during reservation; no parsed Value graph is retained.
pub(crate) struct Cycle<'a> {
    pub(crate) values: &'a [Value],
    pub(crate) repetitions: usize,
}
#[derive(Clone, Copy)]
struct Limits {
    items: usize,
    bytes: usize,
    template_entries: usize,
    header_bytes: usize,
    frames: usize,
    amplified_bytes: usize,
}
impl Limits {
    fn ordinary(items: usize, bytes: usize) -> Self {
        Self {
            items,
            bytes,
            template_entries: TEMPLATE_ENTRIES,
            header_bytes: HEADER_BYTES,
            frames: FRAMES,
            amplified_bytes: AMPLIFIED_BYTES,
        }
    }
}
struct TemplateCredits {
    _entries: OwnedSemaphorePermit,
    _headers: OwnedSemaphorePermit,
}
struct Segment {
    lines: Vec<Vec<u8>>,
    repetitions: usize,
}
enum Payload {
    Single(Vec<u8>),
    Repeated(Vec<Segment>),
}
struct Cursor {
    payload: Payload,
    segment: usize,
    repetition: usize,
    line: usize,
    frames: usize,
}
impl Cursor {
    fn current(&self) -> Option<&[u8]> {
        match &self.payload {
            Payload::Single(line) => (self.segment == 0).then_some(line.as_slice()),
            Payload::Repeated(segments) => segments
                .get(self.segment)
                .map(|segment| segment.lines[self.line].as_slice()),
        }
    }
    fn advance(&mut self) {
        match &self.payload {
            Payload::Single(_) => self.segment = 1,
            Payload::Repeated(segments) => {
                let segment = &segments[self.segment];
                self.line += 1;
                if self.line == segment.lines.len() {
                    self.line = 0;
                    self.repetition += 1;
                    if self.repetition == segment.repetitions {
                        self.repetition = 0;
                        self.segment += 1;
                    }
                }
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
    Queued,
    Writing,
    Flushing,
    Complete,
    Failed(io::ErrorKind),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Progress {
    pub(crate) stage: Stage,
    pub(crate) frame: usize,
    pub(crate) byte: usize,
}
struct Signal {
    progress: Progress,
    waiter: Option<Waker>,
}
impl Signal {
    fn update(shared: &Mutex<Self>, progress: Progress) {
        let wake = {
            let mut state = shared.lock().unwrap();
            state.progress = progress;
            if matches!(progress.stage, Stage::Complete | Stage::Failed(_)) {
                state.waiter.take()
            } else {
                None
            }
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
/// Polling/dropping this observer never drives the writer. Borrow it again to
/// resume a cancelled wait; committed publication remains independently owned.
pub(crate) struct Ticket {
    shared: Arc<Mutex<Signal>>,
}
impl Ticket {
    pub(crate) fn progress(&self) -> Progress {
        self.shared.lock().unwrap().progress
    }
}
impl Future for Ticket {
    type Output = io::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared.lock().unwrap();
        match state.progress.stage {
            Stage::Complete => Poll::Ready(Ok(())),
            Stage::Failed(kind) => Poll::Ready(Err(io::Error::new(
                kind,
                format!(
                    "Native publication failed at frame {} byte {}",
                    state.progress.frame, state.progress.byte
                ),
            ))),
            _ => {
                if state
                    .waiter
                    .as_ref()
                    .is_none_or(|w| !w.will_wake(cx.waker()))
                {
                    state.waiter = Some(cx.waker().clone());
                }
                Poll::Pending
            }
        }
    }
}
struct Entry {
    cursor: Cursor,
    progress: Progress,
    signal: Arc<Mutex<Signal>>,
    max_line: usize,
    _item: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    _templates: Option<TemplateCredits>,
}
impl Entry {
    fn report(&self) {
        Signal::update(&self.signal, self.progress);
    }
    fn fail(&mut self, kind: io::ErrorKind) {
        self.progress.stage = Stage::Failed(kind);
        self.report();
    }
}
impl Drop for Entry {
    fn drop(&mut self) {
        if !matches!(self.progress.stage, Stage::Complete | Stage::Failed(_)) {
            self.fail(io::ErrorKind::BrokenPipe);
        }
    }
}
/// Owns all admission before an external visibility commit. It occupies no
/// queue placeholder: refusal/drop returns capacity and enqueues nothing.
pub(crate) struct Reservation {
    permit: mpsc::OwnedPermit<Entry>,
    entry: Entry,
    closed: Arc<Mutex<bool>>,
}
impl Reservation {
    /// Infallible transfer of already-admitted ownership, not a flush/peer ACK.
    /// A concurrently failed writer yields a failed ticket, never a false flush.
    pub(crate) fn commit(self) -> Ticket {
        let Self {
            permit,
            entry,
            closed,
        } = self;
        let ticket = Ticket {
            shared: entry.signal.clone(),
        };
        // OwnedPermit alone may send AFTER Receiver::drop has drained the queue.
        // Serialize this bounded enqueue with receiver destruction, never I/O.
        let closed = closed.lock().unwrap();
        if *closed {
            drop(entry);
            drop(permit);
        } else {
            drop(permit.send(entry));
        }
        ticket
    }
}
// Created before spawn, so even abort-before-first-poll destroys the receiver
// under the same short lock used by commit. No lock spans an await or pipe I/O.
struct Queue {
    receiver: Option<mpsc::Receiver<Entry>>,
    closed: Arc<Mutex<bool>>,
}
impl Drop for Queue {
    fn drop(&mut self) {
        let mut closed = self.closed.lock().unwrap();
        *closed = true;
        drop(self.receiver.take());
    }
}
pub(crate) struct Output {
    sender: mpsc::Sender<Entry>,
    items: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    template_entries: Arc<Semaphore>,
    header_bytes: Arc<Semaphore>,
    limits: Limits,
    closed: Arc<Mutex<bool>>,
}
struct Count {
    length: usize,
    limit: usize,
}
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.length = self
            .length
            .checked_add(bytes.len())
            .filter(|length| *length <= self.limit)
            .ok_or_else(capacity)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn capacity() -> io::Error {
    io::Error::other("Native output admission exhausted; live state retained until owned shutdown")
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "Native publication must have a finite nonempty cursor within declared bounds",
    )
}
fn acquire(pool: &Arc<Semaphore>, count: usize) -> io::Result<OwnedSemaphorePermit> {
    pool.clone()
        .try_acquire_many_owned(u32::try_from(count).map_err(|_| capacity())?)
        .map_err(|_| capacity())
}
fn encoded_length(value: &Value, limit: usize) -> io::Result<usize> {
    let mut count = Count {
        length: 0,
        limit: limit.saturating_sub(1),
    };
    serde_json::to_writer(&mut count, value).map_err(io::Error::other)?;
    count
        .length
        .checked_add(1)
        .filter(|n| *n <= limit)
        .ok_or_else(capacity)
}
struct Encoding {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Encoding {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.limit)
        {
            return Err(capacity());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode(value: &Value, length: usize) -> io::Result<Vec<u8>> {
    let mut output = Encoding {
        bytes: Vec::with_capacity(length),
        limit: length - 1,
    };
    serde_json::to_writer(&mut output, value).map_err(io::Error::other)?;
    if output.bytes.len() + 1 != length {
        return Err(invalid());
    }
    output.bytes.push(b'\n');
    if output.bytes.capacity() > length {
        return Err(capacity());
    }
    Ok(output.bytes)
}
impl Output {
    pub(crate) fn spawn<W: AsyncWrite + Unpin + Send + 'static>(
        writer: W,
    ) -> (Self, JoinHandle<io::Result<()>>) {
        Self::with_limits(writer, ITEMS, BYTES)
    }
    fn with_limits<W: AsyncWrite + Unpin + Send + 'static>(
        writer: W,
        items: usize,
        bytes: usize,
    ) -> (Self, JoinHandle<io::Result<()>>) {
        Self::with_budgets(writer, Limits::ordinary(items, bytes))
    }
    fn with_budgets<W: AsyncWrite + Unpin + Send + 'static>(
        mut writer: W,
        limits: Limits,
    ) -> (Self, JoinHandle<io::Result<()>>) {
        let (sender, receiver) = mpsc::channel::<Entry>(limits.items);
        let closed = Arc::new(Mutex::new(false));
        let mut queue = Queue {
            receiver: Some(receiver),
            closed: closed.clone(),
        };
        let output = Self {
            sender,
            items: Arc::new(Semaphore::new(limits.items)),
            bytes: Arc::new(Semaphore::new(limits.bytes)),
            template_entries: Arc::new(Semaphore::new(limits.template_entries)),
            header_bytes: Arc::new(Semaphore::new(limits.header_bytes)),
            limits,
            closed: closed.clone(),
        };
        let worker = tokio::spawn(async move {
            while let Some(mut entry) = queue.receiver.as_mut().unwrap().recv().await {
                if let Err(error) = write_entry(&mut writer, &mut entry).await {
                    entry.fail(error.kind());
                    return Err(error);
                }
                drop(entry);
            }
            Ok(())
        });
        (output, worker)
    }
    /// Nonblocking admission of an entire finite cursor. Reserve BEFORE changing
    /// external visibility. One existing item and ALL retained encoded template
    /// bytes stay charged throughout ALL cursor flushes. The maximum line's
    /// progress credit is included in that sum: later frames cannot steal it.
    /// Template headers/entries and amplified output have additional bounds.
    /// These limits do not bound caller-owned Value graphs or process RSS.
    pub(crate) fn reserve_publication(&self, cycles: &[Cycle<'_>]) -> io::Result<Reservation> {
        let item = acquire(&self.items, 1)?;
        let permit = self
            .sender
            .clone()
            .try_reserve_owned()
            .map_err(|_| capacity())?;
        let single = cycles.len() == 1 && cycles[0].values.len() == 1 && cycles[0].repetitions == 1;
        if cycles.is_empty() || (!single && cycles.len() > self.limits.template_entries) {
            return Err(invalid());
        }
        let mut frames = 0usize;
        let mut amplification = 0usize;
        let mut templates = 0usize;
        let mut headers = cycles
            .len()
            .checked_mul(size_of::<Segment>())
            .ok_or_else(capacity)?;
        let mut maximum = 0usize;
        let mut encoded_sum = 0usize;
        for cycle in cycles {
            if cycle.values.is_empty() || cycle.repetitions == 0 {
                return Err(invalid());
            }
            templates = templates
                .checked_add(cycle.values.len())
                .ok_or_else(capacity)?;
            if !single
                && templates
                    .checked_add(cycles.len())
                    .is_none_or(|n| n > self.limits.template_entries)
            {
                return Err(capacity());
            }
            frames = frames
                .checked_add(
                    cycle
                        .values
                        .len()
                        .checked_mul(cycle.repetitions)
                        .ok_or_else(capacity)?,
                )
                .filter(|n| single || *n <= self.limits.frames)
                .ok_or_else(capacity)?;
            headers = headers
                .checked_add(
                    cycle
                        .values
                        .len()
                        .checked_mul(size_of::<Vec<u8>>())
                        .ok_or_else(capacity)?,
                )
                .ok_or_else(capacity)?;
            for value in cycle.values {
                let length = encoded_length(value, self.limits.bytes)?;
                maximum = maximum.max(length);
                encoded_sum = encoded_sum
                    .checked_add(length)
                    .filter(|n| *n <= self.limits.bytes)
                    .ok_or_else(capacity)?;
                amplification = amplification
                    .checked_add(length.checked_mul(cycle.repetitions).ok_or_else(capacity)?)
                    .filter(|n| single || *n <= self.limits.amplified_bytes)
                    .ok_or_else(capacity)?;
            }
        }
        // Single frames keep their original byte/item limits without new quotas.
        // Multi-frame headers consume separate credits; encoded buffers NEVER do.
        let templates_credit = if frames == 1 {
            None
        } else {
            if headers > self.limits.header_bytes {
                return Err(capacity());
            }
            Some(TemplateCredits {
                _entries: acquire(&self.template_entries, templates + cycles.len())?,
                _headers: acquire(&self.header_bytes, headers)?,
            })
        };
        let bytes = acquire(&self.bytes, encoded_sum)?;
        let payload_data = if frames == 1 {
            Payload::Single(encode(&cycles[0].values[0], maximum)?)
        } else {
            let mut segments = Vec::with_capacity(cycles.len());
            for cycle in cycles {
                let mut lines = Vec::with_capacity(cycle.values.len());
                for value in cycle.values {
                    let length = encoded_length(value, self.limits.bytes)?;
                    lines.push(encode(value, length)?);
                }
                segments.push(Segment {
                    lines,
                    repetitions: cycle.repetitions,
                });
            }
            Payload::Repeated(segments)
        };
        let progress = Progress {
            stage: Stage::Queued,
            frame: 0,
            byte: 0,
        };
        let signal = Arc::new(Mutex::new(Signal {
            progress,
            waiter: None,
        }));
        Ok(Reservation {
            permit,
            closed: self.closed.clone(),
            entry: Entry {
                cursor: Cursor {
                    payload: payload_data,
                    segment: 0,
                    repetition: 0,
                    line: 0,
                    frames,
                },
                progress,
                signal,
                max_line: maximum,
                _item: item,
                _bytes: bytes,
                _templates: templates_credit,
            },
        })
    }
    /// Existing immediate fast path: count/encode the borrowed Value, never clone
    /// it, never await pipe credits or flush, and keep128 items/32MiB unchanged.
    pub(crate) fn try_send(&self, value: &Value) -> io::Result<()> {
        let cycles = [Cycle {
            values: std::slice::from_ref(value),
            repetitions: 1,
        }];
        let ticket = self.reserve_publication(&cycles)?.commit();
        if matches!(ticket.progress().stage, Stage::Failed(_)) {
            return Err(capacity());
        }
        Ok(())
    }
}
async fn write_entry<W: AsyncWrite + Unpin>(writer: &mut W, entry: &mut Entry) -> io::Result<()> {
    while let Some(line) = entry.cursor.current() {
        let length = line.len();
        if length > entry.max_line || entry.progress.frame >= entry.cursor.frames {
            return Err(invalid());
        }
        entry.progress.stage = Stage::Writing;
        entry.report();
        while entry.progress.byte < length {
            let count = writer
                .write(&entry.cursor.current().unwrap()[entry.progress.byte..])
                .await?;
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            entry.progress.byte = entry
                .progress
                .byte
                .checked_add(count)
                .filter(|n| *n <= length)
                .ok_or_else(invalid)?;
            entry.report();
        }
        entry.progress.stage = Stage::Flushing;
        entry.report();
        writer.flush().await?;
        entry.cursor.advance();
        entry.progress.frame += 1;
        entry.progress.byte = 0;
    }
    if entry.progress.frame != entry.cursor.frames {
        return Err(invalid());
    }
    entry.progress.stage = Stage::Complete;
    entry.report();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn partial_write_keeps_byte_budget_and_preserves_both_frames() {
        let (writer, mut reader) = tokio::io::duplex(1);
        let first = json!({"id": 1, "result": "é\n"});
        let second = json!({"id": 2});
        let first_len = serde_json::to_vec(&first).unwrap().len() + 1;
        let second_len = serde_json::to_vec(&second).unwrap().len() + 1;
        let (output, worker) = Output::with_limits(writer, 8, first_len + second_len);
        output.try_send(&first).unwrap();
        let mut prefix = [0];
        reader.read_exact(&mut prefix).await.unwrap();
        // The channel slot has been taken, but the partially written frame
        // still consumes its bytes. Even a tiny third line cannot be admitted.
        assert_eq!(output.bytes.available_permits(), second_len);
        output.try_send(&second).unwrap();
        assert!(output.try_send(&Value::Null).is_err());
        let bytes = output.bytes.clone();
        let items = output.items.clone();
        drop(output);
        let mut result = prefix.to_vec();
        reader.read_to_end(&mut result).await.unwrap();
        worker.await.unwrap().unwrap();
        let lines = std::str::from_utf8(&result)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(lines, [first, second]);
        assert_eq!(bytes.available_permits(), first_len + second_len);
        assert_eq!(items.available_permits(), 8);
    }

    #[tokio::test]
    async fn active_frame_keeps_item_and_oversize_refusal_returns_capacity() {
        let (writer, mut reader) = tokio::io::duplex(1);
        let (output, worker) = Output::with_limits(writer, 1, 64);
        assert!(output.try_send(&json!("x".repeat(64))).is_err());
        assert_eq!(output.items.available_permits(), 1);
        assert_eq!(output.bytes.available_permits(), 64);
        output.try_send(&json!({"id": 1})).unwrap();
        reader.read_u8().await.unwrap();
        assert!(output.try_send(&Value::Null).is_err());
        let items = output.items.clone();
        let bytes = output.bytes.clone();
        drop(reader);
        assert!(worker.await.unwrap().is_err());
        assert_eq!(items.available_permits(), 1);
        assert_eq!(bytes.available_permits(), 64);
        assert!(output.try_send(&Value::Null).is_err());
        assert_eq!(items.available_permits(), 1);
        assert_eq!(bytes.available_permits(), 64);
    }

    #[tokio::test]
    async fn writer_failure_releases_active_and_queued_frames_without_replay() {
        let (writer, mut reader) = tokio::io::duplex(1);
        let (output, worker) = Output::with_limits(writer, 3, 128);
        for id in 1..=3 {
            output.try_send(&json!({"id": id})).unwrap();
        }
        reader.read_u8().await.unwrap();
        assert_eq!(output.items.available_permits(), 0);
        assert!(output.try_send(&Value::Null).is_err());
        drop(reader);
        assert!(worker.await.unwrap().is_err());
        assert_eq!(output.items.available_permits(), 3);
        assert_eq!(output.bytes.available_permits(), 128);
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    use serde_json::json;
    use tokio::{
        io::AsyncReadExt,
        time::{Duration, timeout},
    };
    fn line(value: &Value) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(value).unwrap();
        bytes.push(b'\n');
        bytes
    }
    fn counters(output: &Output) -> [usize; 4] {
        [
            output.items.available_permits(),
            output.bytes.available_permits(),
            output.template_entries.available_permits(),
            output.header_bytes.available_permits(),
        ]
    }
    fn full(limits: Limits) -> [usize; 4] {
        [
            limits.items,
            limits.bytes,
            limits.template_entries,
            limits.header_bytes,
        ]
    }
    async fn stage(ticket: &Ticket, expected: impl Fn(Progress) -> bool) -> bool {
        timeout(Duration::from_secs(3), async {
            while !expected(ticket.progress()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok()
    }
    async fn join_worker(mut worker: JoinHandle<io::Result<()>>) -> io::Result<()> {
        match timeout(Duration::from_secs(3), &mut worker).await {
            Ok(result) => result.map_err(io::Error::other)?,
            Err(_) => {
                worker.abort();
                let _ = worker.await;
                Err(io::ErrorKind::TimedOut.into())
            }
        }
    }
    #[tokio::test]
    async fn actual178_notifications_then_reply_cross128_without_coalescing_or_extra_writer() {
        let notifications = [
            json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{"label":"日本語\n💡"}}),
        ];
        let reply = [json!({"jsonrpc":"2.0","id":7,"result":{"changed":178}})];
        let limits = Limits::ordinary(ITEMS, BYTES);
        let (writer, mut reader) = tokio::io::duplex(1);
        let (output, worker) = Output::with_budgets(writer, limits);
        let before = counters(&output);
        let reservation = output
            .reserve_publication(&[
                Cycle {
                    values: &notifications,
                    repetitions: 178,
                },
                Cycle {
                    values: &reply,
                    repetitions: 1,
                },
            ])
            .unwrap();
        let admitted = counters(&output);
        let ticket = reservation.commit();
        let pools = (
            output.items.clone(),
            output.bytes.clone(),
            output.template_entries.clone(),
            output.header_bytes.clone(),
        );
        drop(output);
        let reading = tokio::spawn(async move {
            let mut received = Vec::new();
            reader.read_to_end(&mut received).await.unwrap();
            received
        });
        let completion = timeout(Duration::from_secs(5), ticket).await;
        let joined = join_worker(worker).await;
        let received = reading.await.unwrap();
        let mut expected = line(&notifications[0]).repeat(178);
        expected.extend(line(&reply[0]));
        assert!(matches!(completion, Ok(Ok(()))) && joined.is_ok());
        assert_eq!(before, full(limits));
        assert_eq!(admitted[0], ITEMS - 1);
        assert_eq!(received, expected);
        assert_eq!(std::str::from_utf8(&received).unwrap().lines().count(), 179);
        assert_eq!(
            [
                pools.0.available_permits(),
                pools.1.available_permits(),
                pools.2.available_permits(),
                pools.3.available_permits()
            ],
            full(limits)
        );
    }
    #[tokio::test]
    async fn queued_partial_wait_cancellation_resumes_exact_cursor_with_later_credits_occupied() {
        let templates = [
            json!({"n":"A"}),
            json!({"notification":"B💡日本語 and a longer later line"}),
        ];
        let reply = [json!({"reply":true})];
        let later = Value::Null;
        let maximum = templates
            .iter()
            .chain(&reply)
            .map(|v| line(v).len())
            .max()
            .unwrap();
        let retained: usize = templates.iter().chain(&reply).map(|v| line(v).len()).sum();
        let limits = Limits::ordinary(2, retained + line(&later).len());
        let (writer, mut reader) = tokio::io::duplex(1);
        let (output, worker) = Output::with_budgets(writer, limits);
        let mut ticket = output
            .reserve_publication(&[
                Cycle {
                    values: &templates,
                    repetitions: 89,
                },
                Cycle {
                    values: &reply,
                    repetitions: 1,
                },
            ])
            .unwrap()
            .commit();
        let initial = std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut ticket).poll(cx))).await;
        let queued = ticket.progress();
        let greedier = output.try_send(&json!("x".repeat(maximum - 3))).is_err();
        output.try_send(&later).unwrap();
        let exhausted = counters(&output);
        let first = reader.read_u8().await.unwrap();
        let cancelled = timeout(Duration::from_millis(10), &mut ticket)
            .await
            .is_err();
        let partial = ticket.progress();
        let pools = (
            output.items.clone(),
            output.bytes.clone(),
            output.template_entries.clone(),
            output.header_bytes.clone(),
        );
        drop(output);
        let reading = tokio::spawn(async move {
            let mut received = vec![first];
            reader.read_to_end(&mut received).await.unwrap();
            received
        });
        let completion = timeout(Duration::from_secs(5), &mut ticket).await;
        let joined = join_worker(worker).await;
        let received = reading.await.unwrap();
        let mut expected = Vec::new();
        for _ in 0..89 {
            expected.extend(line(&templates[0]));
            expected.extend(line(&templates[1]));
        }
        expected.extend(line(&reply[0]));
        expected.extend(line(&later));
        assert!(initial.is_pending());
        assert_eq!(queued.stage, Stage::Queued);
        assert!(greedier && cancelled && matches!(completion, Ok(Ok(()))) && joined.is_ok());
        assert_eq!(exhausted[0], 0);
        assert_eq!(exhausted[1], 0);
        assert_eq!(partial.stage, Stage::Writing);
        assert!(partial.byte > 0);
        assert_eq!(partial.frame, 0);
        assert_eq!(received, expected);
        assert_eq!(
            [
                pools.0.available_permits(),
                pools.1.available_permits(),
                pools.2.available_permits(),
                pools.3.available_permits()
            ],
            full(limits)
        );
    }
    #[derive(Default)]
    struct WireState {
        bytes: Vec<u8>,
        flushes: usize,
        hold_flush: bool,
        hold_after_first_flush: bool,
        fail_write_at: Option<usize>,
        fail_flush_at: Option<usize>,
        waiter: Option<Waker>,
    }
    struct Wire(Arc<Mutex<WireState>>);
    impl AsyncWrite for Wire {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            let mut state = self.0.lock().unwrap();
            if state.hold_after_first_flush && state.flushes > 0 {
                state.waiter = Some(cx.waker().clone());
                return Poll::Pending;
            }
            if state
                .fail_write_at
                .is_some_and(|at| state.bytes.len() >= at)
            {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            let count = bytes.len().min(2).min(
                state
                    .fail_write_at
                    .map_or(usize::MAX, |at| at - state.bytes.len()),
            );
            state.bytes.extend_from_slice(&bytes[..count]);
            Poll::Ready(Ok(count))
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let mut state = self.0.lock().unwrap();
            if state.hold_flush {
                state.waiter = Some(cx.waker().clone());
                return Poll::Pending;
            }
            if state.fail_flush_at == Some(state.flushes) {
                return Poll::Ready(Err(io::ErrorKind::Other.into()));
            }
            state.flushes += 1;
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    fn release(state: &Mutex<WireState>, flush: bool) {
        let wake = {
            let mut state = state.lock().unwrap();
            if flush {
                state.hold_flush = false;
            } else {
                state.hold_after_first_flush = false;
            }
            state.waiter.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
    #[tokio::test]
    async fn cancelled_wait_at_flush_and_between_frames_never_restarts_or_ends_ownership() {
        let state = Arc::new(Mutex::new(WireState {
            hold_flush: true,
            hold_after_first_flush: true,
            ..Default::default()
        }));
        let limits = Limits::ordinary(1, 128);
        let (output, worker) = Output::with_budgets(Wire(state.clone()), limits);
        let values = [json!({"frame":"日本\n"}), json!({"reply":"💡"})];
        let mut ticket = output
            .reserve_publication(&[Cycle {
                values: &values,
                repetitions: 1,
            }])
            .unwrap()
            .commit();
        let reached_flush = stage(&ticket, |p| p.stage == Stage::Flushing).await;
        let flushing = ticket.progress();
        let credits_flushing = counters(&output);
        let cancelled_flush = timeout(Duration::from_millis(10), &mut ticket)
            .await
            .is_err();
        release(&state, true);
        let reached_boundary = stage(&ticket, |p| {
            p.stage == Stage::Writing && p.frame == 1 && p.byte == 0
        })
        .await;
        let boundary = ticket.progress();
        let cancelled_boundary = timeout(Duration::from_millis(10), &mut ticket)
            .await
            .is_err();
        release(&state, false);
        drop(output);
        let complete = timeout(Duration::from_secs(3), &mut ticket).await;
        let joined = join_worker(worker).await;
        assert!(
            reached_flush
                && reached_boundary
                && cancelled_flush
                && cancelled_boundary
                && matches!(complete, Ok(Ok(())))
                && joined.is_ok()
        );
        assert_eq!(flushing.frame, 0);
        assert_eq!(flushing.byte, line(&values[0]).len());
        assert_eq!(boundary.frame, 1);
        assert_eq!(credits_flushing[0], 0);
        let mut expected = line(&values[0]);
        expected.extend(line(&values[1]));
        assert_eq!(state.lock().unwrap().bytes, expected);
        assert_eq!(state.lock().unwrap().flushes, 2);
    }
    #[tokio::test]
    async fn dropping_observer_keeps_publication_owned_through_flush_and_eof() {
        let state = Arc::new(Mutex::new(WireState {
            hold_flush: true,
            ..Default::default()
        }));
        let (output, worker) = Output::with_limits(Wire(state.clone()), 1, 128);
        let value = [json!({"retained":"é"})];
        let ticket = output
            .reserve_publication(&[Cycle {
                values: &value,
                repetitions: 178,
            }])
            .unwrap()
            .commit();
        let reached_flush = stage(&ticket, |p| p.stage == Stage::Flushing).await;
        drop(ticket);
        drop(output);
        release(&state, true);
        let result = join_worker(worker).await;
        assert!(reached_flush && result.is_ok());
        assert_eq!(state.lock().unwrap().bytes, line(&value[0]).repeat(178));
        assert_eq!(state.lock().unwrap().flushes, 178);
    }
    #[tokio::test]
    async fn partial_write_and_flush_failure_release_all_credits_without_replaying_or_reply() {
        for flush_failure in [false, true] {
            let state = Arc::new(Mutex::new(WireState {
                fail_write_at: (!flush_failure).then_some(3),
                fail_flush_at: flush_failure.then_some(0),
                ..Default::default()
            }));
            let limits = Limits::ordinary(2, 256);
            let (output, worker) = Output::with_budgets(Wire(state.clone()), limits);
            let values = [json!({"first":"日本"}), json!({"never-reply":true})];
            let mut active = output
                .reserve_publication(&[Cycle {
                    values: &values,
                    repetitions: 1,
                }])
                .unwrap()
                .commit();
            let queued = output
                .reserve_publication(&[Cycle {
                    values: &values,
                    repetitions: 2,
                }])
                .unwrap()
                .commit();
            let pools = (
                output.items.clone(),
                output.bytes.clone(),
                output.template_entries.clone(),
                output.header_bytes.clone(),
            );
            drop(output);
            let result = worker.await.unwrap();
            let failed = (&mut active).await;
            let later = queued.await;
            let progress = active.progress();
            assert!(result.is_err() && failed.is_err() && later.is_err());
            assert_eq!(progress.frame, 0);
            assert_eq!(
                progress.byte,
                if flush_failure {
                    line(&values[0]).len()
                } else {
                    3
                }
            );
            assert_eq!(
                state.lock().unwrap().bytes,
                if flush_failure {
                    line(&values[0])
                } else {
                    line(&values[0])[..3].to_vec()
                }
            );
            assert_eq!(
                [
                    pools.0.available_permits(),
                    pools.1.available_permits(),
                    pools.2.available_permits(),
                    pools.3.available_permits()
                ],
                full(limits)
            );
        }
    }
    #[tokio::test]
    async fn dropped_or_refused_reservation_never_places_a_queue_placeholder() {
        let limits = Limits::ordinary(2, 128);
        let (writer, mut reader) = tokio::io::duplex(1);
        let (output, worker) = Output::with_budgets(writer, limits);
        let values = [json!({"not-committed":true})];
        let reservation = output
            .reserve_publication(&[Cycle {
                values: &values,
                repetitions: 178,
            }])
            .unwrap();
        let admitted = counters(&output);
        output.try_send(&Value::Null).unwrap();
        let mut ordinary = vec![0; 5];
        timeout(Duration::from_secs(3), reader.read_exact(&mut ordinary))
            .await
            .unwrap()
            .unwrap();
        drop(reservation);
        let restored = counters(&output);
        drop(output);
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).await.unwrap();
        worker.await.unwrap().unwrap();
        assert_eq!(admitted[0], 1);
        assert_eq!(ordinary, b"null\n");
        assert!(rest.is_empty());
        assert_eq!(restored, full(limits));
    }
    #[tokio::test]
    async fn reservation_binds_exact_templates_and_cannot_substitute_values_at_commit() {
        let (writer, mut reader) = tokio::io::duplex(1);
        let (output, worker) = Output::with_limits(writer, 2, 128);
        let mut value = [json!({"captured":"old日本\n"})];
        let expected = line(&value[0]).repeat(2);
        let reservation = output
            .reserve_publication(&[Cycle {
                values: &value,
                repetitions: 2,
            }])
            .unwrap();
        value[0] = json!({"replacement":"not the admitted payload"});
        let ticket = reservation.commit();
        drop(value);
        drop(output);
        let reading = tokio::spawn(async move {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            bytes
        });
        ticket.await.unwrap();
        worker.await.unwrap().unwrap();
        assert_eq!(reading.await.unwrap(), expected);
    }
    #[tokio::test]
    async fn closed_receiver_between_reservation_and_commit_fails_ticket_and_releases_capacity() {
        let (writer, reader) = tokio::io::duplex(1);
        let limits = Limits::ordinary(2, 128);
        let (output, worker) = Output::with_budgets(writer, limits);
        let values = [json!({"reserved":true})];
        let reserved = output
            .reserve_publication(&[Cycle {
                values: &values,
                repetitions: 178,
            }])
            .unwrap();
        output.try_send(&Value::Null).unwrap();
        drop(reader);
        assert!(worker.await.unwrap().is_err());
        let ticket = reserved.commit();
        let result = timeout(Duration::from_secs(3), ticket).await;
        assert!(matches!(result, Ok(Err(_))));
        assert_eq!(counters(&output), full(limits));
        assert!(output.try_send(&Value::Null).is_err());
        assert_eq!(counters(&output), full(limits));
    }
    #[tokio::test]
    async fn worker_abort_before_first_poll_refuses_late_commit_and_returns_all_credits() {
        let limits = Limits::ordinary(2, 128);
        let (output, worker) = Output::with_budgets(tokio::io::sink(), limits);
        let values = [json!({"reserved":true})];
        let reservation = output
            .reserve_publication(&[Cycle {
                values: &values,
                repetitions: 178,
            }])
            .unwrap();
        worker.abort();
        let joined = worker.await;
        let failed = timeout(Duration::from_secs(3), reservation.commit()).await;
        assert!(joined.unwrap_err().is_cancelled());
        assert!(matches!(failed, Ok(Err(_))));
        assert_eq!(counters(&output), full(limits));
    }
    #[tokio::test]
    async fn finite_entry_templates_and_amplification_refusals_are_atomic_and_return_permits() {
        let mut limits = Limits::ordinary(2, 128);
        limits.template_entries = 4;
        limits.header_bytes = 256;
        limits.frames = 180;
        limits.amplified_bytes = 4096;
        let (output, worker) = Output::with_budgets(tokio::io::sink(), limits);
        let values = [json!({"line":1}), json!({"line":2})];
        let initial = counters(&output);
        let amplified = [json!({"payload":"x".repeat(64)})];
        let amplification_refused = output
            .reserve_publication(&[Cycle {
                values: &amplified,
                repetitions: 100,
            }])
            .is_err();
        let bad = [
            output.reserve_publication(&[]).is_err(),
            output
                .reserve_publication(&[Cycle {
                    values: &values,
                    repetitions: 0,
                }])
                .is_err(),
            output
                .reserve_publication(&[Cycle {
                    values: &values,
                    repetitions: usize::MAX,
                }])
                .is_err(),
            output
                .reserve_publication(&[Cycle {
                    values: &values,
                    repetitions: 91,
                }])
                .is_err(),
            output
                .reserve_publication(&[Cycle {
                    values: &values,
                    repetitions: 180,
                }])
                .is_err(),
        ];
        let refused = counters(&output);
        let reservation = output
            .reserve_publication(&[Cycle {
                values: &values,
                repetitions: 2,
            }])
            .unwrap();
        let exhausted = output
            .reserve_publication(&[Cycle {
                values: &values,
                repetitions: 2,
            }])
            .is_err();
        output.try_send(&Value::Null).unwrap();
        drop(reservation);
        drop(output);
        worker.await.unwrap().unwrap();
        assert!(amplification_refused && bad.into_iter().all(|b| b) && exhausted);
        assert_eq!(initial, full(limits));
        assert_eq!(refused, full(limits));
    }
    #[tokio::test]
    async fn distinct_templates_charge_their_full_encoded_sum_before_commit() {
        let limits = Limits::ordinary(2, 64);
        let state = Arc::new(Mutex::new(WireState::default()));
        let (output, worker) = Output::with_budgets(Wire(state.clone()), limits);
        let values = [json!("A".repeat(37)), json!("B".repeat(37))];
        assert!(values.iter().all(|v| line(v).len() <= limits.bytes));
        assert!(values.iter().map(|v| line(v).len()).sum::<usize>() > limits.bytes);
        let refused = output
            .reserve_publication(&[Cycle {
                values: &values,
                repetitions: 2,
            }])
            .is_err();
        let restored = counters(&output);
        drop(output);
        worker.await.unwrap().unwrap();
        assert!(refused);
        assert_eq!(restored, full(limits));
        assert!(state.lock().unwrap().bytes.is_empty());
    }
    #[tokio::test]
    async fn template_header_and_entry_quotas_are_independent_of_encoded_bytes() {
        for entry_limited in [false, true] {
            let mut limits = Limits::ordinary(3, 256);
            limits.template_entries = if entry_limited { 2 } else { 100 };
            limits.header_bytes = if entry_limited { 10000 } else { 100 };
            let (output, worker) = Output::with_budgets(tokio::io::sink(), limits);
            let values = [json!(0)];
            let reservation = output
                .reserve_publication(&[Cycle {
                    values: &values,
                    repetitions: 2,
                }])
                .unwrap();
            let held = counters(&output);
            let refused = output
                .reserve_publication(&[Cycle {
                    values: &values,
                    repetitions: 2,
                }])
                .is_err();
            let pools = (
                output.items.clone(),
                output.bytes.clone(),
                output.template_entries.clone(),
                output.header_bytes.clone(),
            );
            drop(reservation);
            drop(output);
            worker.await.unwrap().unwrap();
            assert!(refused);
            assert_eq!(held[0], 2);
            assert_eq!(held[1], 254);
            if entry_limited {
                assert_eq!(held[2], 0);
            } else {
                assert_eq!(held[2], 98);
                assert!(held[3] < 50);
            }
            assert_eq!(
                [
                    pools.0.available_permits(),
                    pools.1.available_permits(),
                    pools.2.available_permits(),
                    pools.3.available_permits()
                ],
                full(limits)
            );
        }
    }
    #[tokio::test]
    async fn emitted_line_and_frame_count_guards_reject_corrupt_internal_cursor_before_unpaid_output()
     {
        for corrupt_line in [false, true] {
            let values = [json!({"line":true})];
            let (writer, mut reader) = tokio::io::duplex(64);
            let (output, worker) = Output::with_limits(writer, 1, 128);
            let mut reservation = output
                .reserve_publication(&[Cycle {
                    values: &values,
                    repetitions: 2,
                }])
                .unwrap();
            if corrupt_line {
                if let Payload::Repeated(segments) = &mut reservation.entry.cursor.payload {
                    segments[0].lines[0].extend_from_slice(b"unpaid");
                }
            } else {
                reservation.entry.cursor.frames = 1;
            }
            let ticket = reservation.commit();
            drop(output);
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert!(worker.await.unwrap().is_err());
            assert!(ticket.await.is_err());
            assert_eq!(
                bytes,
                if corrupt_line {
                    vec![]
                } else {
                    line(&values[0])
                }
            );
        }
    }
    #[tokio::test]
    async fn ordinary_image_and_exact32mib_lines_ignore_new_publication_templates_quotas() {
        let mut limits = Limits::ordinary(ITEMS, BYTES);
        limits.template_entries = 0;
        limits.header_bytes = 0;
        limits.frames = 0;
        limits.amplified_bytes = 0;
        let (output, worker) = Output::with_budgets(tokio::io::sink(), limits);
        let image = json!({"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"image","mimeType":"image/png","data":"A".repeat(8*1024*1024)}]}});
        let image_result = output.try_send(&image);
        drop(image);
        drop(output);
        let image_join = worker.await.unwrap();
        let (output, worker) = Output::with_budgets(tokio::io::sink(), limits);
        let exact = Value::String("x".repeat(BYTES - 3));
        let accepted = output.try_send(&exact);
        let used = counters(&output);
        drop(exact);
        drop(output);
        let exact_join = worker.await.unwrap();
        let (output, worker) = Output::with_budgets(tokio::io::sink(), limits);
        let above = Value::String("x".repeat(BYTES - 2));
        let refused = output.try_send(&above).is_err();
        let returned = counters(&output);
        drop(above);
        drop(output);
        let failed_join = worker.await.unwrap();
        assert!(
            image_result.is_ok()
                && image_join.is_ok()
                && accepted.is_ok()
                && exact_join.is_ok()
                && refused
                && failed_join.is_ok()
        );
        assert_eq!(used[1], 0);
        assert_eq!(returned, full(limits));
    }
}
