// SPDX-License-Identifier: GPL-2.0-only
//! Injected-observation port of `src/tools/take-monitor.ts`.
//!
//! This owner neither starts nor stops recording. Its caller owns the take's
//! deadline, connection, subscriptions, decoding, sample tasks, and file checks.
//! Millisecond timestamps are injected wall-clock readings, including backward
//! jumps: elapsed time intentionally matches the TypeScript monitor. A deadline
//! owner calls `begin_stop` synchronously before awaiting its workers, then joins
//! or cancels their sample tickets before `finish_cleanup`. Dropping a ticket
//! cancels that admission; no tasks, sockets, or timers are spawned here.
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
};

const MAX_EVENTS: usize = 500;
const MAX_WARNINGS_PER_KIND: usize = 20;

#[derive(Clone, Debug)]
pub struct MonitorOptions {
    pub expect_silent: bool,
    pub silence_threshold_db: f64,
    pub sample_interval_ms: f64,
    pub stats_interval_ms: f64,
    pub still_after_seconds: f64,
}
impl Default for MonitorOptions {
    fn default() -> Self {
        Self {
            expect_silent: false,
            silence_threshold_db: -60.0,
            sample_interval_ms: 1_000.0,
            stats_interval_ms: 2_000.0,
            still_after_seconds: 3.0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WarningKind {
    Audio,
    Blank,
    Frames,
    Output,
    Connection,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Warning {
    pub at_seconds: f64,
    pub kind: WarningKind,
    pub message: String,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub at_seconds: f64,
    pub r#type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Chapter {
    pub at_seconds: f64,
    pub name: String,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    pub start_seconds: f64,
    pub end_seconds: f64,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InputLevel {
    pub input_name: String,
    /// Negative infinity represents an observed zero multiplier; JSON is null,
    /// just as JSON.stringify serializes the TypeScript summary.
    pub peak_db: f64,
    pub input_peak_db: f64,
    pub recorded: bool,
}
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Frames {
    pub render_skipped: f64,
    pub render_total: f64,
    pub output_skipped: f64,
    pub output_total: f64,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub running: bool,
    pub started_at: String,
    pub duration_seconds: f64,
    pub expect_silent: bool,
    pub silence_threshold_db: f64,
    pub warnings: Vec<Warning>,
    pub events: Vec<Event>,
    pub chapters: Vec<Chapter>,
    pub audio: Vec<InputLevel>,
    pub frames: Frames,
    pub stills: Vec<Span>,
    pub blanks: Vec<Span>,
    pub frames_sampled: u64,
    pub picture_checks: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Starting,
    Running,
    Stopping,
    Stopped,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SampleKind {
    Stats,
    Picture,
}
impl SampleKind {
    fn slot(self) -> usize {
        match self {
            Self::Stats => 0,
            Self::Picture => 1,
        }
    }
}
/// Admission to one non-overlapping periodic check. This is not take authority.
/// It cannot be cloned or created externally. Drop releases the admission;
/// cancellation/join of the external sample task remains the adapter's duty.
#[derive(Debug)]
pub struct SampleTicket {
    owner: Arc<()>,
    picture_history: Arc<()>,
    active: Arc<AtomicBool>,
    kind: SampleKind,
}
impl Drop for SampleTicket {
    fn drop(&mut self) {
        self.active.store(false, AtomicOrdering::Release);
    }
}
pub enum SampleObservation<'a> {
    Stats(&'a Value),
    /// Already decoded grayscale pixels. PPM decoding belongs to the adapter.
    Picture(&'a [u8]),
    Failed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupError {
    NotStopping,
    SamplesPending,
}

pub struct TakeMonitorCore {
    options: MonitorOptions,
    started_at_ms: i64,
    started_at: String,
    stopped_at_ms: Option<i64>,
    phase: Phase,
    stop_expected: bool,
    warnings: Vec<Warning>,
    events: Vec<Event>,
    chapters: Vec<Chapter>,
    levels: Vec<InputLevel>,
    audible_now: HashSet<String>,
    input_tracks: HashMap<String, Map<String, Value>>,
    record_tracks: Vec<u8>,
    frames: Frames,
    last_stats: Option<Value>,
    last_frame_warning_ms: [Option<i64>; 2],
    stills: Vec<Span>,
    blanks: Vec<Span>,
    tracker: StillTracker,
    blank_since: Option<f64>,
    blank_run: u64,
    frames_sampled: u64,
    picture_checks: bool,
    program_scene: Option<String>,
    recording_path: Option<String>,
    paused_ms: f64,
    paused_since: Option<i64>,
    sample_owner: Arc<()>,
    picture_history: Arc<()>,
    busy: [Weak<AtomicBool>; 2],
}
impl TakeMonitorCore {
    /// `started_at` is the UTC representation of the injected start reading;
    /// formatting/clock failure is the caller's responsibility, not fabricated.
    pub fn new(started_at_ms: i64, started_at: String, options: MonitorOptions) -> Self {
        let tracker = StillTracker::new(options.still_after_seconds);
        Self {
            options,
            started_at_ms,
            started_at,
            stopped_at_ms: None,
            phase: Phase::Starting,
            stop_expected: false,
            warnings: vec![],
            events: vec![],
            chapters: vec![],
            levels: vec![],
            audible_now: HashSet::new(),
            input_tracks: HashMap::new(),
            record_tracks: vec![1],
            frames: Frames::default(),
            last_stats: None,
            last_frame_warning_ms: [None; 2],
            stills: vec![],
            blanks: vec![],
            tracker,
            blank_since: None,
            blank_run: 0,
            frames_sampled: 0,
            picture_checks: false,
            program_scene: None,
            recording_path: None,
            paused_ms: 0.0,
            paused_since: None,
            sample_owner: Arc::new(()),
            picture_history: Arc::new(()),
            busy: [Weak::new(), Weak::new()],
        }
    }
    pub fn phase(&self) -> Phase {
        self.phase
    }
    pub fn now(&self, now_ms: i64) -> f64 {
        let end = self.stopped_at_ms.unwrap_or(now_ms) as f64;
        let paused = self.paused_ms + self.paused_since.map_or(0.0, |start| end - start as f64);
        round((end - self.started_at_ms as f64 - paused) / 1_000.0)
    }
    pub fn is_paused(&self) -> bool {
        self.paused_since.is_some()
    }
    pub fn output_path(&self) -> Option<&str> {
        self.recording_path.as_deref()
    }
    /// Scheduling hints only; the caller owns timers and their monotonic clock.
    pub fn interval_ms(&self, kind: SampleKind) -> f64 {
        match kind {
            SampleKind::Stats => self.options.stats_interval_ms,
            SampleKind::Picture => self.options.sample_interval_ms,
        }
    }
    fn listening(&self) -> bool {
        matches!(self.phase, Phase::Starting | Phase::Running)
    }
    /// Inject the output of `recordedTracks` and successful input-track reads.
    /// The source defaults to recording track 1; unknown input tracks are
    /// considered recorded. Profile parsing remains owned by preflight.
    pub fn load_tracks(&mut self, recorded: Vec<u8>, tracks: HashMap<String, Map<String, Value>>) {
        if self.listening() {
            self.record_tracks = recorded;
            self.input_tracks.extend(tracks);
        }
    }
    pub fn tracks_unknown(&mut self, now_ms: i64, message: &str) {
        if self.listening() {
            self.log(now_ms, "tracks-unknown", Some(message.to_owned()));
        }
    }
    pub fn load_program_scene(&mut self, response: &Value) {
        if self.listening() {
            self.program_scene = response
                .get("currentProgramSceneName")
                .filter(|v| !v.is_null())
                .or_else(|| response.get("sceneName"))
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
    }
    /// A deadline can stop the owner while setup is pending. Late setup cannot
    /// arm periodic work or subscribe again after `begin_stop`.
    pub fn complete_setup(&mut self, now_ms: i64, supports_ppm: bool) -> bool {
        if self.phase != Phase::Starting {
            return false;
        }
        self.phase = Phase::Running;
        self.picture_checks = supports_ppm;
        if !supports_ppm {
            self.log(
                now_ms,
                "picture-checks-off",
                Some("OBS cannot return PPM screenshots".into()),
            );
        }
        true
    }
    pub fn mark(&mut self, now_ms: i64, name: String) {
        self.chapters.push(Chapter {
            at_seconds: self.now(now_ms),
            name,
        });
    }
    pub fn expect_stop(&mut self) {
        self.stop_expected = true;
    }
    /// Detach listeners/timers in the adapter immediately after this call.
    /// Closing the monitor does not issue StopRecord or imply the file stopped.
    pub fn begin_stop(&mut self, now_ms: i64) -> bool {
        if self.stopped_at_ms.is_some() {
            return false;
        }
        self.stopped_at_ms = Some(now_ms);
        self.stop_expected = true;
        self.phase = Phase::Stopping;
        true
    }
    pub fn samples_pending(&self) -> bool {
        self.busy.iter().any(|slot| {
            slot.upgrade()
                .is_some_and(|active| active.load(AtomicOrdering::Acquire))
        })
    }
    /// Join or cancel admitted sample tasks first. This synchronous owner can
    /// check outstanding tickets, not certify an external task's physical exit.
    /// Final GetStats, if desired, is injected via `observe_stats` before this
    /// call; failure is ignorable.
    pub fn finish_cleanup(&mut self) -> Result<(), CleanupError> {
        if self.phase == Phase::Stopped {
            return Ok(());
        }
        if self.phase != Phase::Stopping {
            return Err(CleanupError::NotStopping);
        }
        if self.samples_pending() {
            return Err(CleanupError::SamplesPending);
        }
        let end = self.now(self.stopped_at_ms.unwrap());
        self.close_still(end);
        self.close_blank(end);
        self.phase = Phase::Stopped;
        Ok(())
    }
    pub fn begin_sample(&mut self, kind: SampleKind) -> Option<SampleTicket> {
        if self.phase != Phase::Running
            || (kind == SampleKind::Picture
                && (!self.picture_checks
                    || self.program_scene.as_deref().is_none_or(str::is_empty)
                    || self.is_paused()))
        {
            return None;
        }
        let slot = &mut self.busy[kind.slot()];
        if slot
            .upgrade()
            .is_some_and(|active| active.load(AtomicOrdering::Acquire))
        {
            return None;
        }
        let active = Arc::new(AtomicBool::new(true));
        *slot = Arc::downgrade(&active);
        Some(SampleTicket {
            owner: self.sample_owner.clone(),
            picture_history: self.picture_history.clone(),
            active,
            kind,
        })
    }
    pub fn finish_sample(
        &mut self,
        ticket: SampleTicket,
        now_ms: i64,
        observation: SampleObservation<'_>,
    ) -> bool {
        if !Arc::ptr_eq(&self.sample_owner, &ticket.owner)
            || !ticket.active.swap(false, AtomicOrdering::AcqRel)
            || (ticket.kind == SampleKind::Picture
                && !Arc::ptr_eq(&self.picture_history, &ticket.picture_history))
        {
            return false;
        }
        match (ticket.kind, observation) {
            (SampleKind::Stats, SampleObservation::Stats(stats)) => {
                self.observe_stats(now_ms, stats)
            }
            (SampleKind::Picture, SampleObservation::Picture(luma)) => {
                self.observe_picture(now_ms, luma)
            }
            (_, SampleObservation::Failed) => {}
            _ => return false,
        }
        true
    }
    pub fn on_disconnect(&mut self, now_ms: i64) {
        if self.listening() {
            self.warn(
                now_ms,
                WarningKind::Connection,
                "Lost the connection to OBS; checks paused until it reconnects".into(),
            );
        }
    }
    pub fn on_event(&mut self, now_ms: i64, event: &str, data: &Value) {
        if !self.listening() {
            return;
        }
        if event == "InputVolumeMeters" {
            self.on_meters(now_ms, data);
            return;
        }
        let detail = match event {
            "InputAudioTracksChanged" => {
                if let (Some(name), Some(tracks)) = (
                    data.get("inputName").and_then(Value::as_str),
                    data.get("inputAudioTracks").and_then(Value::as_object),
                ) {
                    self.input_tracks.insert(name.into(), tracks.clone());
                }
                string_or_empty(data.get("inputName"))
            }
            "CurrentProgramSceneChanged" => {
                if let Some(name) = data.get("sceneName").and_then(Value::as_str) {
                    self.program_scene = Some(name.into());
                }
                self.close_still(self.now(now_ms));
                string_or_empty(data.get("sceneName"))
            }
            "RecordStateChanged" => {
                if let Some(path) = data
                    .get("outputPath")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    self.recording_path = Some(path.into());
                }
                match data.get("outputState").and_then(Value::as_str) {
                    Some("OBS_WEBSOCKET_OUTPUT_PAUSED") if self.paused_since.is_none() => {
                        self.close_still(self.now(now_ms));
                        self.paused_since = Some(now_ms)
                    }
                    Some("OBS_WEBSOCKET_OUTPUT_RESUMED") => {
                        if let Some(since) = self.paused_since.take() {
                            self.paused_ms += now_ms as f64 - since as f64;
                        }
                    }
                    _ => {}
                }
                if data.get("outputActive") == Some(&Value::Bool(false))
                    && !self.stop_expected
                    && data.get("outputState").and_then(Value::as_str)
                        == Some("OBS_WEBSOCKET_OUTPUT_STOPPED")
                {
                    self.warn(
                        now_ms,
                        WarningKind::Output,
                        "The recording stopped before it was asked to".into(),
                    );
                }
                string_or_empty(data.get("outputState"))
            }
            "StreamStateChanged" => string_or_empty(data.get("outputState")),
            "RecordFileChanged" => string_or_empty(data.get("newOutputPath")),
            "SceneItemEnableStateChanged" => format!(
                "{} #{} {}",
                js_string(data.get("sceneName")),
                js_string(data.get("sceneItemId")),
                if truthy(data.get("sceneItemEnabled")) {
                    "shown"
                } else {
                    "hidden"
                }
            ),
            "InputMuteStateChanged" => format!(
                "{} {}",
                js_string(data.get("inputName")),
                if truthy(data.get("inputMuted")) {
                    "muted"
                } else {
                    "unmuted"
                }
            ),
            _ => return,
        };
        self.log(now_ms, event, Some(detail));
    }
    fn on_meters(&mut self, now_ms: i64, data: &Value) {
        if self.stopped_at_ms.is_some() || self.is_paused() {
            return;
        }
        let Some(inputs) = data.get("inputs").and_then(Value::as_array) else {
            return;
        };
        for input in inputs {
            let (Some(name), Some(channels)) = (
                input.get("inputName").and_then(Value::as_str),
                input.get("inputLevelsMul").and_then(Value::as_array),
            ) else {
                continue;
            };
            let (mut peak, mut input_peak) = (0.0_f64, 0.0_f64);
            for channel in channels.iter().filter_map(Value::as_array) {
                let level = |index| {
                    let n = js_number(channel.get(index));
                    if n.is_nan() || n == 0.0 { 0.0 } else { n }
                };
                peak = js_max(peak, level(1));
                input_peak = js_max(input_peak, level(2));
            }
            let recorded = self.input_tracks.get(name).is_none_or(|tracks| {
                self.record_tracks
                    .iter()
                    .any(|track| tracks.get(&track.to_string()) == Some(&Value::Bool(true)))
            });
            let peak_db = to_db(peak);
            if let Some(level) = self
                .levels
                .iter_mut()
                .find(|level| level.input_name == name)
            {
                level.peak_db = js_max(level.peak_db, peak_db);
                level.input_peak_db = js_max(level.input_peak_db, to_db(input_peak));
                level.recorded = recorded;
            } else {
                self.levels.push(InputLevel {
                    input_name: name.into(),
                    peak_db,
                    input_peak_db: to_db(input_peak),
                    recorded,
                });
            }
            let audible = recorded && peak_db > self.options.silence_threshold_db;
            if audible && self.audible_now.insert(name.into()) {
                self.log(
                    now_ms,
                    "audio-start",
                    Some(format!("{name} at {} dB", number_string(peak_db))),
                );
                if self.options.expect_silent {
                    self.warn(now_ms, WarningKind::Audio, format!("{name} is audible in the recording at {} dB (expected silence). Mute it or clear its recorded tracks", number_string(peak_db)));
                }
            } else if !audible && self.audible_now.remove(name) {
                self.log(now_ms, "audio-end", Some(name.into()));
            }
        }
    }
    /// Initial and final samples are injected directly. Missing/null fields
    /// mean zero, counter resets clamp to zero, and malformed values retain
    /// JavaScript's NaN propagation instead of inventing known totals.
    pub fn observe_stats(&mut self, now_ms: i64, stats: &Value) {
        if self.phase == Phase::Stopped {
            return;
        }
        let Some(previous) = self.last_stats.replace(stats.clone()) else {
            return;
        };
        // A null response throws when the TypeScript method indexes it; its
        // periodic/initial/final callers ignore that failed sample, after
        // lastStats was replaced. A falsey previous response is a new baseline.
        if !truthy(Some(&previous)) || stats.is_null() {
            return;
        }
        let delta = |field| {
            js_max(
                0.0,
                js_number(stats.get(field)) - js_number(previous.get(field)),
            )
        };
        let render = delta("renderSkippedFrames");
        let output = delta("outputSkippedFrames");
        self.frames.render_skipped += render;
        self.frames.render_total += delta("renderTotalFrames");
        self.frames.output_skipped += output;
        self.frames.output_total += delta("outputTotalFrames");
        for (index, skipped, message) in [
            (
                0,
                render,
                format!(
                    "OBS missed {} frame(s) while rendering; the GPU or CPU is overloaded",
                    number_string(render)
                ),
            ),
            (
                1,
                output,
                format!(
                    "The encoder skipped {} frame(s); lower the encoder preset or resolution",
                    number_string(output)
                ),
            ),
        ] {
            if skipped == 0.0
                || self.last_frame_warning_ms[index]
                    .is_some_and(|last| (now_ms as f64 - last as f64) < 10_000.0)
            {
                continue;
            }
            self.last_frame_warning_ms[index] = Some(now_ms);
            self.warn(now_ms, WarningKind::Frames, message);
        }
    }
    /// Injected after a successful screenshot decode. Admission checks scene
    /// availability/pause. Sample tickets reject responses from a history
    /// retired by pause or program switch; no picture publishes after stop.
    pub fn observe_picture(&mut self, now_ms: i64, luma: &[u8]) {
        if self.stopped_at_ms.is_some() || self.is_paused() {
            return;
        }
        let at = self.now(now_ms);
        self.frames_sampled = self.frames_sampled.saturating_add(1);
        if luma.iter().all(|value| *value < 16) {
            self.blank_run = self.blank_run.saturating_add(1);
            if self.blank_run == 2 {
                self.blank_since = Some(round(at - self.options.sample_interval_ms / 1_000.0));
                self.warn(now_ms, WarningKind::Blank, format!("The program output ({}) is black. A capture may have lost its window or screen recording permission", self.program_scene.as_deref().unwrap_or("null")));
            }
        } else {
            self.blank_run = 0;
            self.close_blank(at);
        }
        self.tracker.push(at, luma.to_vec());
    }
    pub fn summary(&self, now_ms: i64) -> Summary {
        let mut audio = self.levels.clone();
        audio.sort_by(|a, b| b.peak_db.partial_cmp(&a.peak_db).unwrap_or(Ordering::Equal));
        let mut stills = self.stills.clone();
        stills.extend(self.tracker.spans.iter().map(|span| Span {
            start_seconds: round(span.start_seconds),
            end_seconds: round(span.end_seconds),
        }));
        if self.stopped_at_ms.is_none()
            && let Some(start) = self.tracker.still_since()
        {
            stills.push(Span {
                start_seconds: start,
                end_seconds: self.now(now_ms),
            });
        }
        let mut blanks = self.blanks.clone();
        if self.stopped_at_ms.is_none()
            && let Some(start) = self.blank_since
        {
            blanks.push(Span {
                start_seconds: start,
                end_seconds: self.now(now_ms),
            });
        }
        Summary {
            running: self.stopped_at_ms.is_none(),
            started_at: self.started_at.clone(),
            duration_seconds: self.now(now_ms),
            expect_silent: self.options.expect_silent,
            silence_threshold_db: self.options.silence_threshold_db,
            warnings: self.warnings.clone(),
            events: self.events.clone(),
            chapters: self.chapters.clone(),
            audio,
            frames: self.frames.clone(),
            stills,
            blanks,
            frames_sampled: self.frames_sampled,
            picture_checks: if self.picture_checks { "on" } else { "off" },
        }
    }
    fn log(&mut self, now_ms: i64, event: &str, detail: Option<String>) {
        if self.events.len() < MAX_EVENTS {
            self.events.push(Event {
                at_seconds: self.now(now_ms),
                r#type: event.into(),
                detail: detail.filter(|detail| !detail.is_empty()),
            });
        }
    }
    fn warn(&mut self, now_ms: i64, kind: WarningKind, message: String) {
        if self
            .warnings
            .iter()
            .filter(|warning| warning.kind == kind)
            .count()
            < MAX_WARNINGS_PER_KIND
        {
            self.warnings.push(Warning {
                at_seconds: self.now(now_ms),
                kind,
                message,
            });
        }
    }
    fn close_still(&mut self, at: f64) {
        self.picture_history = Arc::new(());
        self.stills
            .extend(self.tracker.finish(at).iter().map(|span| Span {
                start_seconds: round(span.start_seconds),
                end_seconds: round(span.end_seconds),
            }));
        self.tracker = StillTracker::new(self.options.still_after_seconds);
    }
    fn close_blank(&mut self, at: f64) {
        if let Some(start) = self.blank_since.take() {
            self.blanks.push(Span {
                start_seconds: start,
                end_seconds: at,
            });
        }
    }
}

struct Frame {
    at: f64,
    luma: Vec<u8>,
}
/// Shared median-window tracker. Times and completed spans remain unrounded;
/// monitor presentation rounds only at its report projection. Holds at most
/// six input frames plus accumulated spans; callers must bound stream history.
pub struct StillTracker {
    min_seconds: f64,
    samples: VecDeque<Frame>,
    still_from: Option<f64>,
    last_time: Option<f64>,
    comparison_qualified: bool,
    spans: Vec<Span>,
}
impl StillTracker {
    pub fn new(min_seconds: f64) -> Self {
        Self {
            min_seconds,
            samples: VecDeque::new(),
            still_from: None,
            last_time: None,
            comparison_qualified: false,
            spans: vec![],
        }
    }
    pub fn still_since(&self) -> Option<f64> {
        self.still_from.filter(|start| {
            self.comparison_qualified
                && self
                    .last_time
                    .is_some_and(|last| last - start >= self.min_seconds)
        })
    }
    pub fn spans(&self) -> &[Span] {
        &self.spans
    }
    fn close(&mut self, at: f64) {
        if self.comparison_qualified
            && let Some(start) = self
                .still_from
                .filter(|start| at - start >= self.min_seconds)
        {
            self.spans.push(Span {
                start_seconds: start,
                end_seconds: at,
            });
        }
    }
    pub fn finish(&mut self, at: f64) -> &[Span] {
        self.close(at);
        self.still_from = None;
        self.last_time = None;
        self.comparison_qualified = false;
        self.samples.clear();
        &self.spans
    }
    pub fn push(&mut self, at: f64, luma: Vec<u8>) {
        if self
            .samples
            .front()
            .is_some_and(|sample| sample.luma.len() != luma.len())
        {
            if let Some(last) = self.last_time {
                self.close(last);
            }
            self.samples.clear();
            self.still_from = None;
            self.last_time = None;
            self.comparison_qualified = false;
        }
        self.still_from.get_or_insert(at);
        self.last_time = Some(at);
        self.samples.push_back(Frame { at, luma });
        if self.samples.len() < 6 {
            return;
        }
        let median = |offset: usize| -> Vec<u8> {
            (0..self.samples[offset].luma.len())
                .map(|i| {
                    let a = self.samples[offset].luma[i];
                    let b = self.samples[offset + 1].luma[i];
                    let c = self.samples[offset + 2].luma[i];
                    a.min(b).max(a.max(b).min(c))
                })
                .collect()
        };
        let before = median(0);
        let after = median(3);
        if !same_luma(&before, &after) {
            // A first transition can close a genuine old stable window. Do
            // not backdate that inference before its first matching sample.
            if !self.comparison_qualified {
                let matching = self
                    .samples
                    .iter()
                    .take(3)
                    .filter(|sample| same_luma(&before, &sample.luma))
                    .map(|sample| sample.at)
                    .collect::<Vec<_>>();
                if matching.len() >= 2 {
                    self.still_from = self.still_from.map(|start| js_max(start, matching[0]));
                    self.comparison_qualified = true;
                }
            }
            let changed_at = [1, 2, 3, 4]
                .into_iter()
                .find(|index| !same_luma(&before, &self.samples[*index].luma))
                .map_or(self.samples[3].at, |index| self.samples[index].at);
            let mut settled = self.samples[3].at;
            for index in [2, 1] {
                if !same_luma(&after, &self.samples[index].luma) {
                    break;
                }
                settled = self.samples[index].at;
            }
            self.close(changed_at);
            self.still_from = Some(js_max(changed_at, settled));
            // Observed motion does not qualify its trailing candidate as
            // unchanged. Await a later equal-median comparison; a majority
            // inside one after-window alone also occurs in full-frame ABA/BAB
            // alternation. The median/noise comparison metric is unchanged.
            self.comparison_qualified = false;
        } else {
            self.comparison_qualified = !before.is_empty();
        }
        self.samples.pop_front();
    }
}
fn same_luma(a: &[u8], b: &[u8]) -> bool {
    !a.is_empty()
        && a.len() == b.len()
        && a.iter()
            .zip(b)
            .filter(|(a, b)| a.abs_diff(**b) > 12)
            .take(2)
            .count()
            < 2
}
fn round(n: f64) -> f64 {
    (n * 100.0 + 0.5).floor() / 100.0
}
fn to_db(n: f64) -> f64 {
    if n > 0.0 {
        (20.0 * n.log10() * 10.0 + 0.5).floor() / 10.0
    } else {
        f64::NEG_INFINITY
    }
}
fn js_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}
fn number_string(n: f64) -> String {
    if n.is_nan() {
        "NaN".into()
    } else if n == f64::INFINITY {
        "Infinity".into()
    } else if n == f64::NEG_INFINITY {
        "-Infinity".into()
    } else if n == 0.0 {
        "0".into()
    } else if n.abs() >= 1e21 || n.abs() < 1e-6 {
        let scientific = format!("{n:e}");
        let (mantissa, exponent) = scientific.split_once('e').unwrap();
        format!(
            "{mantissa}e{}{exponent}",
            if exponent.starts_with('-') { "" } else { "+" }
        )
    } else {
        n.to_string()
    }
}
fn string_or_empty(value: Option<&Value>) -> String {
    value
        .filter(|v| !v.is_null())
        .map_or_else(String::new, |v| js_string(Some(v)))
}
fn js_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".into(),
        Some(Value::Null) => "null".into(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => number_string(n.as_f64().unwrap_or(f64::NAN)),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(a)) => a
            .iter()
            .map(|v| {
                if v.is_null() {
                    String::new()
                } else {
                    js_string(Some(v))
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(Value::Object(_)) => "[object Object]".into(),
    }
}
fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|n| n != 0.0 && !n.is_nan()),
        Some(Value::String(s)) => !s.is_empty(),
        _ => true,
    }
}
fn js_number(value: Option<&Value>) -> f64 {
    match value {
        None | Some(Value::Null) => 0.0,
        Some(Value::Bool(b)) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Value::Object(_)) => f64::NAN,
        Some(Value::Array(_)) => js_number(Some(&Value::String(js_string(value)))),
        Some(Value::String(s)) => {
            let s =
                s.trim_matches(|c: char| c == '\u{feff}' || (c.is_whitespace() && c != '\u{85}'));
            if s.is_empty() {
                return 0.0;
            }
            for (prefix, base) in [
                ("0x", 16),
                ("0X", 16),
                ("0b", 2),
                ("0B", 2),
                ("0o", 8),
                ("0O", 8),
            ] {
                if let Some(digits) = s.strip_prefix(prefix) {
                    if digits.is_empty() {
                        return f64::NAN;
                    }
                    return digits
                        .chars()
                        .try_fold(0.0, |n, digit| {
                            digit
                                .to_digit(base)
                                .map(|digit| n * f64::from(base) + f64::from(digit))
                        })
                        .unwrap_or(f64::NAN);
                }
            }
            if matches!(
                s,
                "inf" | "-inf" | "+inf" | "infinity" | "-infinity" | "+infinity"
            ) {
                f64::NAN
            } else {
                s.parse().unwrap_or(f64::NAN)
            }
        }
    }
}
