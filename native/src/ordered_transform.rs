// SPDX-License-Identifier: GPL-2.0-only
//! A fixed transform-read projection. Pools charge owned raw/canonical strings,
//! not Value allocations, caller copies, tool bodies, frontend output or RSS.
use crate::{observation::js_string, session::RequestError};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::HashMap,
    fmt,
    io::{self, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
    task::JoinSet,
    time::{Instant, timeout_at},
};
const LIMIT: usize = 8 * 1024 * 1024;
const DEPTH: usize = 128;

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SceneItemSelectors {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canvas_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scene_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scene_uuid: Option<String>,
    pub scene_item_id: u64,
}
impl fmt::Debug for SceneItemSelectors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SceneItemSelectors")
            .field("has_canvas_uuid", &self.canvas_uuid.is_some())
            .field("has_scene_name", &self.scene_name.is_some())
            .field("has_scene_uuid", &self.scene_uuid.is_some())
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionError {
    RawCapacity,
    TextCapacity,
    WorkerCapacity,
    Malformed,
    Depth,
    Deadline,
    Cancelled,
    Closed,
    WorkerFailed,
}
impl fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Transform projection {self:?}")
    }
}
impl std::error::Error for ProjectionError {}
pub enum TransformReadbackError {
    Request(RequestError),
    Projection(ProjectionError),
}
impl fmt::Debug for TransformReadbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(_) => f.write_str("TransformReadbackError::Request"),
            Self::Projection(e) => f
                .debug_tuple("TransformReadbackError::Projection")
                .field(e)
                .finish(),
        }
    }
}
impl fmt::Display for TransformReadbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for TransformReadbackError {}
impl TransformReadbackError {
    /// Explicit authorized tool text. Never use this projection for diagnostics.
    pub fn tool_message(&self) -> String {
        match self {
            Self::Request(e) => e
                .obs_failure_message("GetSceneItemTransform")
                .unwrap_or_else(|| "Transform readback request unavailable".into()),
            Self::Projection(e) => e.to_string(),
        }
    }
}
/// Non-Clone transport-owned canonical text. Caller copies leave this pool's scope.
pub struct SourceJsonText {
    text: String,
    _permit: OwnedSemaphorePermit,
}
impl SourceJsonText {
    pub fn as_str(&self) -> &str {
        &self.text
    }
    pub fn len(&self) -> usize {
        self.text.len()
    }
    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }
}
impl fmt::Debug for SourceJsonText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceJsonText")
            .field("bytes", &self.text.len())
            .finish_non_exhaustive()
    }
}
pub struct SceneItemTransformReadback {
    pub response_data: Value,
    pub applied_json: SourceJsonText,
}
impl fmt::Debug for SceneItemTransformReadback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SceneItemTransformReadback")
            .field("applied_json", &self.applied_json)
            .finish_non_exhaustive()
    }
}
pub(crate) struct RawReadback {
    pub value: Value,
    raw: String,
    _permit: OwnedSemaphorePermit,
    deadline: Instant,
}
struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct Control {
    job: Arc<AtomicBool>,
    owner: Arc<AtomicBool>,
    deadline: Instant,
}
impl Control {
    fn check(&self) -> Result<(), ProjectionError> {
        if self.owner.load(Ordering::Acquire) {
            Err(ProjectionError::Closed)
        } else if self.job.load(Ordering::Acquire) {
            Err(ProjectionError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ProjectionError::Deadline)
        } else {
            Ok(())
        }
    }
}
struct Jobs {
    set: Option<JoinSet<()>>,
}
pub(crate) struct ProjectionOwner {
    raw: Arc<Semaphore>,
    text: Arc<Semaphore>,
    raw_limit: usize,
    text_limit: usize,
    items: usize,
    jobs: Mutex<Jobs>,
    closed: Arc<AtomicBool>,
    #[cfg(test)]
    next_gate: Mutex<Option<Arc<TestGate>>>,
    #[cfg(test)]
    next_return_gate: Mutex<Option<Arc<TestGate>>>,
}
impl ProjectionOwner {
    pub(crate) fn new(items: usize) -> Self {
        Self::with_limits(items, LIMIT, LIMIT)
    }
    fn with_limits(items: usize, raw: usize, text: usize) -> Self {
        Self {
            raw: Arc::new(Semaphore::new(raw)),
            text: Arc::new(Semaphore::new(text)),
            raw_limit: raw,
            text_limit: text,
            items,
            jobs: Mutex::new(Jobs {
                set: Some(JoinSet::new()),
            }),
            closed: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            next_gate: Mutex::new(None),
            #[cfg(test)]
            next_return_gate: Mutex::new(None),
        }
    }
    #[cfg(test)]
    pub(crate) fn hold_next(&self) -> Arc<TestGate> {
        let gate = Arc::new(TestGate::new());
        *self.next_gate.lock().unwrap() = Some(gate.clone());
        gate
    }
    #[cfg(test)]
    fn hold_next_return(&self) -> Arc<TestGate> {
        let gate = Arc::new(TestGate::new());
        *self.next_return_gate.lock().unwrap() = Some(gate.clone());
        gate
    }
    pub(crate) fn cancel(&self) {
        self.closed.store(true, Ordering::Release);
    }
    // Called only after the actor validated the exact ID/type/success status.
    pub(crate) fn capture(
        &self,
        frame: &str,
        value: Value,
        deadline: Instant,
    ) -> Result<RawReadback, ProjectionError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ProjectionError::Closed);
        }
        if Instant::now() >= deadline {
            return Err(ProjectionError::Deadline);
        }
        // Session already validated the complete frame and reply identity.
        // This private scanner retains spans; it never replaces global parsing.
        let root = RawSlice(frame.trim());
        if !root.get().starts_with('{') {
            return Err(ProjectionError::Malformed);
        }
        let d = field(root, "d")?.ok_or(ProjectionError::Malformed)?;
        if !d.get().starts_with('{') {
            return Err(ProjectionError::Malformed);
        }
        let data = field(d, "responseData")?;
        let selected = match data {
            Some(data) => field(data, "sceneItemTransform")?,
            None => None,
        };
        let raw = selected
            .filter(|v| v.get() != "null")
            .map_or("{}", RawSlice::get);
        if raw.len() > self.raw_limit {
            return Err(ProjectionError::RawCapacity);
        }
        if Instant::now() >= deadline {
            return Err(ProjectionError::Deadline);
        }
        let permit = self
            .raw
            .clone()
            .try_acquire_many_owned(raw.len() as u32)
            .map_err(|_| ProjectionError::RawCapacity)?;
        Ok(RawReadback {
            value,
            raw: raw.to_owned(),
            _permit: permit,
            deadline,
        })
    }
    pub(crate) async fn project(
        &self,
        raw: RawReadback,
    ) -> Result<SceneItemTransformReadback, ProjectionError> {
        let deadline = raw.deadline;
        let (send, mut receive) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(cancelled.clone());
        let control = Control {
            job: cancelled.clone(),
            owner: self.closed.clone(),
            deadline,
        };
        control.check()?;
        let text = self.text.clone();
        let text_limit = self.text_limit;
        {
            let mut jobs = self.jobs.lock().unwrap();
            let Some(set) = jobs.set.as_mut() else {
                return Err(ProjectionError::Closed);
            };
            while set.try_join_next().is_some() {}
            // Completed-but-unreaped entries count too, not merely live workers.
            if set.len() >= self.items {
                return Err(ProjectionError::WorkerCapacity);
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(ProjectionError::Closed);
            }
            #[cfg(test)]
            let gate = self.next_gate.lock().unwrap().take();
            #[cfg(test)]
            let return_gate = self.next_return_gate.lock().unwrap().take();
            set.spawn_blocking(move || {
                #[cfg(test)]
                if let Some(gate) = gate {
                    gate.wait();
                }
                let result = canonical(raw, &text, text_limit, &control);
                let _ = send.send(result);
                #[cfg(test)]
                if let Some(gate) = return_gate {
                    gate.wait();
                }
            });
        }
        let result = match timeout_at(deadline, &mut receive).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ProjectionError::WorkerFailed),
            Err(_) => {
                cancelled.store(true, Ordering::Release);
                // Retain and drain the SAME admitted formatter after timeout.
                let _ = receive.await;
                Err(ProjectionError::Deadline)
            }
        };
        {
            let mut jobs = self.jobs.lock().unwrap();
            if let Some(set) = jobs.set.as_mut() {
                while set.try_join_next().is_some() {}
            }
        }
        result
    }
    pub(crate) async fn shutdown(&self) {
        // Seal admission and move the whole set atomically; no mutex spans await.
        let set = {
            let mut jobs = self.jobs.lock().unwrap();
            self.cancel();
            jobs.set.take()
        };
        if let Some(mut set) = set {
            while set.join_next().await.is_some() {}
        }
    }
}

/// Private spans refer only to already parsed frames or their selected subtree.
/// The scanner still defends all boundaries; no unchecked indexing or unsafe.
#[derive(Clone, Copy)]
struct RawSlice<'a>(&'a str);
impl<'a> RawSlice<'a> {
    fn get(self) -> &'a str {
        self.0
    }
}
struct Scanner<'a, 'c> {
    text: &'a str,
    at: usize,
    nodes: usize,
    control: Option<&'c Control>,
}
impl<'a, 'c> Scanner<'a, 'c> {
    fn new(text: &'a str, control: Option<&'c Control>) -> Self {
        Self {
            text,
            at: 0,
            nodes: crate::obs::MAX_INCOMING_BYTES,
            control,
        }
    }
    fn check(&self) -> Result<(), ProjectionError> {
        if let Some(control) = self.control {
            control.check()?;
        }
        Ok(())
    }
    fn byte(&self) -> Option<u8> {
        self.text.as_bytes().get(self.at).copied()
    }
    fn ws(&mut self) {
        while self
            .byte()
            .is_some_and(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.at += 1;
        }
    }
    fn take(&mut self, expected: u8) -> Result<(), ProjectionError> {
        self.ws();
        if self.byte() != Some(expected) {
            return Err(ProjectionError::Malformed);
        }
        self.at += 1;
        Ok(())
    }
    fn string(&mut self) -> Result<&'a str, ProjectionError> {
        self.take(b'"')?;
        let start = self.at - 1;
        loop {
            if self.at.is_multiple_of(1024) {
                self.check()?;
            }
            match self.byte().ok_or(ProjectionError::Malformed)? {
                b'"' => {
                    self.at += 1;
                    return Ok(&self.text[start..self.at]);
                }
                b'\\' => {
                    self.at += 1;
                    match self.byte().ok_or(ProjectionError::Malformed)? {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => self.at += 1,
                        b'u' => {
                            self.at += 1;
                            for _ in 0..4 {
                                if !self.byte().is_some_and(|b| b.is_ascii_hexdigit()) {
                                    return Err(ProjectionError::Malformed);
                                }
                                self.at += 1;
                            }
                        }
                        _ => return Err(ProjectionError::Malformed),
                    }
                }
                0..=31 => return Err(ProjectionError::Malformed),
                _ => self.at += 1,
            }
        }
    }
    fn value(&mut self, depth: usize) -> Result<RawSlice<'a>, ProjectionError> {
        self.check()?;
        if depth > DEPTH {
            return Err(ProjectionError::Depth);
        }
        self.nodes = self
            .nodes
            .checked_sub(1)
            .ok_or(ProjectionError::Malformed)?;
        self.ws();
        let start = self.at;
        match self.byte().ok_or(ProjectionError::Malformed)? {
            b'"' => {
                self.string()?;
            }
            b'{' => {
                self.at += 1;
                self.ws();
                if self.byte() == Some(b'}') {
                    self.at += 1;
                } else {
                    loop {
                        self.string()?;
                        self.take(b':')?;
                        self.value(depth + 1)?;
                        self.ws();
                        match self.byte() {
                            Some(b',') => self.at += 1,
                            Some(b'}') => {
                                self.at += 1;
                                break;
                            }
                            _ => return Err(ProjectionError::Malformed),
                        }
                    }
                }
            }
            b'[' => {
                self.at += 1;
                self.ws();
                if self.byte() == Some(b']') {
                    self.at += 1;
                } else {
                    loop {
                        self.value(depth + 1)?;
                        self.ws();
                        match self.byte() {
                            Some(b',') => self.at += 1,
                            Some(b']') => {
                                self.at += 1;
                                break;
                            }
                            _ => return Err(ProjectionError::Malformed),
                        }
                    }
                }
            }
            b't' | b'f' | b'n' => {
                let token = match self.byte() {
                    Some(b't') => "true",
                    Some(b'f') => "false",
                    _ => "null",
                };
                if !self.text[self.at..].starts_with(token) {
                    return Err(ProjectionError::Malformed);
                }
                self.at += token.len();
            }
            b'-' | b'0'..=b'9' => {
                while self
                    .byte()
                    .is_some_and(|b| b.is_ascii_digit() || b"-+.eE".contains(&b))
                {
                    self.at += 1;
                }
                let token = &self.text[start..self.at];
                let _: serde_json::Number =
                    serde_json::from_str(token).map_err(|_| ProjectionError::Malformed)?;
            }
            _ => return Err(ProjectionError::Malformed),
        }
        Ok(RawSlice(&self.text[start..self.at]))
    }
    fn end(&mut self) -> Result<(), ProjectionError> {
        self.ws();
        if self.at == self.text.len() {
            Ok(())
        } else {
            Err(ProjectionError::Malformed)
        }
    }
}
fn object<'a>(
    raw: RawSlice<'a>,
    control: Option<&Control>,
    mut visit: impl FnMut(String, RawSlice<'a>),
) -> Result<(), ProjectionError> {
    let mut scan = Scanner::new(raw.get(), control);
    scan.take(b'{')?;
    scan.ws();
    if scan.byte() == Some(b'}') {
        scan.at += 1;
        return scan.end();
    }
    loop {
        scan.check()?;
        let key = serde_json::from_str::<String>(scan.string()?)
            .map_err(|_| ProjectionError::Malformed)?;
        scan.take(b':')?;
        let value = scan.value(1)?;
        visit(key, value);
        scan.ws();
        match scan.byte() {
            Some(b',') => scan.at += 1,
            Some(b'}') => {
                scan.at += 1;
                break;
            }
            _ => return Err(ProjectionError::Malformed),
        }
    }
    scan.end()
}
fn field<'a>(raw: RawSlice<'a>, key: &str) -> Result<Option<RawSlice<'a>>, ProjectionError> {
    if !raw.get().starts_with('{') {
        return Ok(None);
    }
    let mut found = None;
    object(raw, None, |name, value| {
        if name == key {
            found = Some(value);
        }
    })?;
    Ok(found)
}
fn fields<'a>(
    raw: RawSlice<'a>,
    control: &Control,
) -> Result<Vec<(String, RawSlice<'a>)>, ProjectionError> {
    let mut output: Vec<(String, RawSlice<'a>)> = Vec::new();
    let mut slots: HashMap<String, usize> = HashMap::new();
    object(raw, Some(control), |key, value| {
        if let Some(index) = slots.get(&key).copied() {
            output[index].1 = value;
        } else {
            slots.insert(key.clone(), output.len());
            output.push((key, value));
        }
    })?;
    fn index(key: &str) -> Option<u32> {
        key.parse::<u32>()
            .ok()
            .filter(|n| *n != u32::MAX && n.to_string() == key)
    }
    output.sort_by(|(a, _), (b, _)| {
        if control.check().is_err() {
            return std::cmp::Ordering::Equal;
        }
        match (index(a), index(b)) {
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    });
    control.check()?;
    Ok(output)
}
fn array<'a>(raw: RawSlice<'a>, control: &Control) -> Result<Vec<RawSlice<'a>>, ProjectionError> {
    let mut scan = Scanner::new(raw.get(), Some(control));
    let mut values = Vec::new();
    scan.take(b'[')?;
    scan.ws();
    if scan.byte() == Some(b']') {
        scan.at += 1;
        scan.end()?;
        return Ok(values);
    }
    loop {
        values.push(scan.value(1)?);
        scan.ws();
        match scan.byte() {
            Some(b',') => scan.at += 1,
            Some(b']') => {
                scan.at += 1;
                break;
            }
            _ => return Err(ProjectionError::Malformed),
        }
    }
    scan.end()?;
    Ok(values)
}

struct Count {
    used: usize,
    limit: usize,
}
impl Write for Count {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.used = self
            .used
            .checked_add(b.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| io::Error::other("text capacity"))?;
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Checked<'a, W> {
    out: &'a mut W,
    control: &'a Control,
}
impl<W: Write> Write for Checked<'_, W> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.control.check().map_err(io::Error::other)?;
        self.out.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}
fn write(out: &mut impl Write, bytes: &[u8], control: &Control) -> Result<(), ProjectionError> {
    control.check()?;
    out.write_all(bytes)
        .map_err(|_| ProjectionError::TextCapacity)
}
fn emit(
    raw: RawSlice<'_>,
    out: &mut impl Write,
    depth: usize,
    nodes: &mut usize,
    control: &Control,
) -> Result<(), ProjectionError> {
    control.check()?;
    if depth > DEPTH {
        return Err(ProjectionError::Depth);
    }
    *nodes = nodes.checked_sub(1).ok_or(ProjectionError::TextCapacity)?;
    match raw
        .get()
        .as_bytes()
        .first()
        .copied()
        .ok_or(ProjectionError::Malformed)?
    {
        b'{' => {
            write(out, b"{", control)?;
            for (index, (key, value)) in fields(raw, control)?.iter().enumerate() {
                if index != 0 {
                    write(out, b",", control)?;
                }
                serde_json::to_writer(&mut Checked { out, control }, key).map_err(|_| {
                    control
                        .check()
                        .err()
                        .unwrap_or(ProjectionError::TextCapacity)
                })?;
                write(out, b":", control)?;
                emit(*value, out, depth + 1, nodes, control)?;
            }
            write(out, b"}", control)
        }
        b'[' => {
            let values = array(raw, control)?;
            write(out, b"[", control)?;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    write(out, b",", control)?;
                }
                emit(*value, out, depth + 1, nodes, control)?;
            }
            write(out, b"]", control)
        }
        b'"' => {
            let value: String =
                serde_json::from_str(raw.get()).map_err(|_| ProjectionError::Malformed)?;
            serde_json::to_writer(Checked { out, control }, &value).map_err(|_| {
                control
                    .check()
                    .err()
                    .unwrap_or(ProjectionError::TextCapacity)
            })
        }
        b't' | b'f' | b'n' => write(out, raw.get().as_bytes(), control),
        _ => {
            let number: f64 = raw.get().parse().map_err(|_| ProjectionError::Malformed)?;
            let value = serde_json::Number::from_f64(number).ok_or(ProjectionError::Malformed)?;
            write(
                out,
                js_string(Some(&Value::Number(value))).as_bytes(),
                control,
            )
        }
    }
}
fn canonical(
    raw: RawReadback,
    pool: &Arc<Semaphore>,
    limit: usize,
    control: &Control,
) -> Result<SceneItemTransformReadback, ProjectionError> {
    control.check()?;
    let source = RawSlice(&raw.raw);
    let mut count = Count { used: 0, limit };
    let mut nodes = limit;
    emit(source, &mut count, 0, &mut nodes, control)?;
    let permit = pool
        .clone()
        .try_acquire_many_owned(count.used as u32)
        .map_err(|_| ProjectionError::TextCapacity)?;
    let mut bytes = Vec::with_capacity(count.used);
    let mut nodes = limit;
    emit(source, &mut bytes, 0, &mut nodes, control)?;
    control.check()?;
    debug_assert_eq!(bytes.len(), count.used);
    Ok(SceneItemTransformReadback {
        response_data: raw.value,
        applied_json: SourceJsonText {
            text: String::from_utf8(bytes).expect("JSON UTF-8"),
            _permit: permit,
        },
    })
}

#[cfg(test)]
pub(crate) struct TestGate {
    pub(crate) started: AtomicBool,
    release: Mutex<bool>,
    wake: std::sync::Condvar,
}
#[cfg(test)]
impl TestGate {
    fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            release: Mutex::new(false),
            wake: std::sync::Condvar::new(),
        }
    }
    fn wait(&self) {
        self.started.store(true, Ordering::Release);
        let mut open = self.release.lock().unwrap();
        while !*open {
            let (next, timeout) = self
                .wake
                .wait_timeout(open, std::time::Duration::from_secs(5))
                .unwrap();
            open = next;
            if timeout.timed_out() {
                break;
            }
        }
    }
    pub(crate) fn release(&self) {
        *self.release.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
#[cfg(test)]
impl Drop for TestGate {
    fn drop(&mut self) {
        *self.release.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::poll;
    use serde_json::json;
    use std::{
        io::Write,
        process::{Command, Stdio},
        time::Duration,
    };
    fn frame(raw: &str) -> String {
        format!(r#"{{"op":7,"d":{{"responseData":{{"sceneItemTransform":{raw}}}}}}}"#)
    }
    async fn started(gate: &TestGate) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !gate.started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    async fn join_current_workers(owner: &ProjectionOwner) -> Result<(), ProjectionError> {
        let mut healthy = true;
        while let Some(joined) = std::future::poll_fn(|cx| {
            let mut jobs = owner.jobs.lock().unwrap();
            match jobs.set.as_mut() {
                Some(set) => set.poll_join_next(cx),
                None => std::task::Poll::Ready(None),
            }
        })
        .await
        {
            healthy &= joined.is_ok();
        }
        if healthy {
            Ok(())
        } else {
            Err(ProjectionError::WorkerFailed)
        }
    }
    #[tokio::test]
    async fn published_result_does_not_release_physical_worker_before_owner_join() {
        let owner = ProjectionOwner::with_limits(1, 64, 64);
        let gate = owner.hold_next_return();
        let deadline = Instant::now() + Duration::from_secs(5);
        let first = owner
            .project(owner.capture(&frame("0"), json!({}), deadline).unwrap())
            .await;
        let reached_gate = tokio::time::timeout(Duration::from_secs(2), async {
            while !gate.started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        let canonical = first
            .as_ref()
            .ok()
            .map(|row| row.applied_json.as_str().to_owned());
        drop(first);
        let raw_free = owner.raw.available_permits();
        let text_free = owner.text.available_permits();
        let charged_workers = owner.jobs.lock().unwrap().set.as_ref().unwrap().len();
        let next = match owner.capture(&frame("false"), json!({}), deadline) {
            Ok(raw) => owner.project(raw).await,
            Err(error) => Err(error),
        };
        // A distinct owner has independent physical-worker admission. This is
        // not interference from other concurrently running unit tests.
        let independent = ProjectionOwner::with_limits(1, 64, 64);
        let other = match independent.capture(&frame("false"), json!({}), deadline) {
            Ok(raw) => independent.project(raw).await,
            Err(error) => Err(error),
        };
        let independent_text = other
            .as_ref()
            .ok()
            .map(|row| row.applied_json.as_str().to_owned());
        drop(other);
        independent.shutdown().await;
        let mut shutdown = Box::pin(owner.shutdown());
        let join_pending = poll!(shutdown.as_mut()).is_pending();
        // Always release and join the same held worker BEFORE assertions.
        gate.release();
        shutdown.await;
        assert!(reached_gate);
        assert_eq!(canonical.as_deref(), Some("0"));
        assert_eq!((raw_free, text_free, charged_workers), (64, 64, 1));
        assert_eq!(independent_text.as_deref(), Some("false"));
        assert!(join_pending);
        assert!(matches!(next, Err(ProjectionError::WorkerCapacity)));
        assert_eq!(owner.raw.available_permits(), 64);
        assert_eq!(owner.text.available_permits(), 64);
        assert!(owner.jobs.lock().unwrap().set.is_none());
    }
    #[tokio::test]
    async fn canonical_matches_actual_node_for_order_duplicates_escapes_types_and_numeric_leaves() {
        let raws = vec![
            r#"{"z":3,"a":1}"#,
            r#"{"z":3,"10":"ten","2":"two","a":1,"z":4,"01":"leading","4294967294":"last","4294967295":"string","2":"TWO","nested":{"b":1,"a":2}}"#,
            r#"{"a":1,"\u0061":9,"__proto__":{"z":3,"a":1},"text":"line\n\u0000日本語\u2028\u2029"}"#,
            r#"{"negativeZero":-0.0,"positiveFloat":1.0,"safeLow":9007199254740991,"safeEdge":9007199254740992,"rounded":9007199254740993,"nextEven":9007199254740994,"negativeRounded":-9007199254740993,"fullU64":18446744073709551615,"fullI64":-9223372036854775808,"shortInteger":1000000000000000128,"minSubnormal":5e-324,"smallExponent":1e-7,"threshold":0.000001,"largeExponent":1e21}"#,
            r#"{"$serde_json::private::RawValue":"123","z":{"$serde_json::private::RawValue":"{\"owned\":true}"}}"#,
            r#"{"\u0024serde_json::private::RawValue":{"z":3,"a":1},"a":2}"#,
            "false",
            "0",
            "\"\"",
            r#"[{"z":3,"a":1},0,false]"#,
        ];
        let mut child=Command::new("node").args(["--input-type=module","-e","let text='';for await(const b of process.stdin)text+=b;console.log(JSON.stringify(JSON.parse(text).map(raw=>JSON.stringify(JSON.parse(raw)))));"]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&serde_json::to_vec(&raws).unwrap())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let expected: Vec<String> = serde_json::from_slice(&output.stdout).unwrap();
        for (raw, expected) in raws.iter().zip(expected) {
            // Each independent semantic vector closes and joins its own worker.
            let owner = ProjectionOwner::new(2);
            let captured = owner
                .capture(
                    &frame(raw),
                    json!({}),
                    Instant::now() + Duration::from_secs(5),
                )
                .unwrap();
            let result = owner.project(captured).await;
            let observed = result
                .as_ref()
                .ok()
                .map(|row| row.applied_json.as_str().to_owned());
            let diagnostic = format!("{result:?}");
            drop(result);
            owner.shutdown().await;
            assert_eq!(observed.as_deref(), Some(expected.as_str()));
            assert!(!diagnostic.contains("negativeZero"));
            assert_eq!(owner.raw.available_permits(), LIMIT);
            assert_eq!(owner.text.available_permits(), LIMIT);
        }
    }
    #[tokio::test]
    async fn selected_path_duplicates_last_wins_and_null_or_missing_defaults_to_object() {
        for (raw, expected) in [
            (
                r#"{"d":{"responseData":{"sceneItemTransform":{"bad":1}}},"d":{"responseData":{"sceneItemTransform":{"z":1,"a":2}}}}"#,
                r#"{"z":1,"a":2}"#,
            ),
            (
                r#"{"d":{"responseData":{"sceneItemTransform":0},"responseData":{"sceneItemTransform":false}}}"#,
                "false",
            ),
            (
                r#"{"d":{"responseData":{"sceneItemTransform":false,"sceneItemTransform":null}}}"#,
                "{}",
            ),
            (r#"{"d":{"responseData":{}}}"#, "{}"),
            (r#"{"d":{"responseData":null}}"#, "{}"),
            (r#"{"d":{"responseData":false}}"#, "{}"),
        ] {
            let owner = ProjectionOwner::new(1);
            let capture = owner
                .capture(raw, json!({}), Instant::now() + Duration::from_secs(3))
                .unwrap();
            let result = owner.project(capture).await;
            let observed = result
                .as_ref()
                .ok()
                .map(|row| row.applied_json.as_str().to_owned());
            drop(result);
            owner.shutdown().await;
            assert_eq!(observed.as_deref(), Some(expected));
        }
    }
    #[tokio::test]
    async fn exact_raw_and_text_pools_are_independent_and_release_after_failure_and_retention() {
        let owner = ProjectionOwner::with_limits(2, 32, 16);
        let deadline = Instant::now() + Duration::from_secs(5);
        let held = owner
            .capture(&frame(r#""12345678901234567890""#), json!({}), deadline)
            .unwrap();
        assert!(matches!(
            owner.capture(&frame(r#""12345678901234567890""#), json!({}), deadline),
            Err(ProjectionError::RawCapacity)
        ));
        drop(held);
        let expansion = owner
            .capture(&frame("[1e-6,1e-6]"), json!({}), deadline)
            .unwrap();
        assert!(matches!(
            owner.project(expansion).await,
            Err(ProjectionError::TextCapacity)
        ));
        join_current_workers(&owner).await.unwrap();
        assert_eq!(owner.raw.available_permits(), 32);
        let first = owner
            .project(
                owner
                    .capture(&frame(r#"{"z":1,"a":2}"#), json!({}), deadline)
                    .unwrap(),
            )
            .await
            .unwrap();
        join_current_workers(&owner).await.unwrap();
        assert!(matches!(
            owner
                .project(
                    owner
                        .capture(&frame(r#"{"z":1,"a":2}"#), json!({}), deadline)
                        .unwrap()
                )
                .await,
            Err(ProjectionError::TextCapacity)
        ));
        join_current_workers(&owner).await.unwrap();
        drop(first);
        owner
            .project(
                owner
                    .capture(&frame(r#"{"z":1,"a":2}"#), json!({}), deadline)
                    .unwrap(),
            )
            .await
            .unwrap();
        owner.shutdown().await;
        assert_eq!(owner.raw.available_permits(), 32);
        assert_eq!(owner.text.available_permits(), 16);
    }
    #[tokio::test]
    async fn dropped_frontend_and_shutdown_retain_join_and_seal_worker_admission() {
        let owner = ProjectionOwner::with_limits(1, 64, 64);
        let gate = owner.hold_next();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut job = Box::pin(
            owner.project(
                owner
                    .capture(&frame(r#"{"z":1,"a":2}"#), json!({}), deadline)
                    .unwrap(),
            ),
        );
        assert!(poll!(job.as_mut()).is_pending());
        started(&gate).await;
        assert!(matches!(
            owner
                .project(owner.capture(&frame("0"), json!({}), deadline).unwrap())
                .await,
            Err(ProjectionError::WorkerCapacity)
        ));
        drop(job);
        assert!(owner.raw.available_permits() < 64);
        let mut shutdown = Box::pin(owner.shutdown());
        assert!(poll!(shutdown.as_mut()).is_pending());
        assert!(matches!(
            owner.capture(&frame("0"), json!({}), deadline),
            Err(ProjectionError::Closed)
        ));
        gate.release();
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap();
        assert_eq!(owner.raw.available_permits(), 64);
        assert_eq!(owner.text.available_permits(), 64);
        assert!(owner.jobs.lock().unwrap().set.is_none());
    }
    #[tokio::test]
    async fn timeout_drains_same_formatter_and_returns_projection_deadline() {
        let owner = ProjectionOwner::with_limits(1, 64, 64);
        let gate = owner.hold_next();
        let deadline = Instant::now() + Duration::from_millis(100);
        let mut job =
            Box::pin(owner.project(owner.capture(&frame("0"), json!({}), deadline).unwrap()));
        assert!(poll!(job.as_mut()).is_pending());
        started(&gate).await;
        tokio::time::sleep_until(deadline).await;
        assert!(
            poll!(job.as_mut()).is_pending(),
            "timeout discarded admitted formatter"
        );
        gate.release();
        assert!(matches!(job.await, Err(ProjectionError::Deadline)));
        owner.shutdown().await;
        assert_eq!(owner.raw.available_permits(), 64);
    }
    #[tokio::test]
    async fn completed_entries_remain_bounded_and_are_reaped_before_new_admission() {
        let owner = ProjectionOwner::with_limits(1, 64, 64);
        for _ in 0..64 {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let raw = owner
                        .capture(
                            &frame("0"),
                            serde_json::json!({}),
                            Instant::now() + Duration::from_secs(2),
                        )
                        .unwrap();
                    match owner.project(raw).await {
                        Ok(result) => {
                            assert_eq!(result.applied_json.as_str(), "0");
                            break;
                        }
                        Err(ProjectionError::WorkerCapacity) => tokio::task::yield_now().await,
                        Err(error) => panic!("unexpected projection: {error:?}"),
                    }
                }
            })
            .await
            .unwrap();
            assert!(owner.jobs.lock().unwrap().set.as_ref().unwrap().len() <= 1);
        }
        owner.shutdown().await;
        assert_eq!(owner.raw.available_permits(), 64);
        assert_eq!(owner.text.available_permits(), 64);
    }
    #[test]
    fn raw_value_feature_must_not_reinterpret_ordinary_json_object_keys() {
        let raw = r#"{"$serde_json::private::RawValue":"123"}"#;
        let parsed: Value = serde_json::from_str(raw).unwrap();
        assert!(
            parsed.is_object(),
            "ordinary JSON object was reinterpreted: {parsed}"
        );
        assert_eq!(parsed["$serde_json::private::RawValue"], "123");
        let nested: Value = serde_json::from_str(
            r#"{"outer":{"$serde_json::private::RawValue":"{\"owned\":true}"}}"#,
        )
        .unwrap();
        assert!(nested["outer"].is_object());
        assert_eq!(
            nested["outer"]["$serde_json::private::RawValue"],
            r#"{"owned":true}"#
        );
    }
    #[test]
    fn span_scanner_defends_malformed_boundaries_and_cancel_depth_nodes() {
        let owner = ProjectionOwner::new(1);
        let deadline = Instant::now() + Duration::from_secs(3);
        for bad in [
            "",
            "{",
            r#"{"d":}"#,
            r#"{"d" 1}"#,
            r#"{"d":{"responseData":{"sceneItemTransform":"unterminated}}}"#,
            r#"{"d":{"responseData":{"sceneItemTransform":"bad\q"}}}"#,
            r#"{"d":{"responseData":{"sceneItemTransform":[1,]}}}"#,
        ] {
            assert!(
                owner.capture(bad, serde_json::json!({}), deadline).is_err(),
                "accepted malformed private span {bad:?}"
            );
        }
        let control = Control {
            job: Arc::new(AtomicBool::new(false)),
            owner: Arc::new(AtomicBool::new(false)),
            deadline,
        };
        let raw = r#"{"a\"b":{"back\\slash":3},"a\u0022b":{"z":4,"a":2}}"#;
        let entries = fields(RawSlice(raw), &control).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "a\"b");
        assert_eq!(entries[0].1.get(), r#"{"z":4,"a":2}"#);
        let deep = format!("{}0{}", "[".repeat(129), "]".repeat(129));
        let mut scan = Scanner::new(&deep, Some(&control));
        assert!(matches!(scan.value(0), Err(ProjectionError::Depth)));
        let mut scan = Scanner::new("[0,1]", Some(&control));
        scan.nodes = 1;
        assert!(matches!(scan.value(0), Err(ProjectionError::Malformed)));
        control.job.store(true, Ordering::Release);
        let mut scan = Scanner::new("[0]", Some(&control));
        assert!(matches!(scan.value(0), Err(ProjectionError::Cancelled)));
    }
}
