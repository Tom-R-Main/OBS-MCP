// SPDX-License-Identifier: GPL-2.0-only
//! Pure Capture Window values. A plan, identity or intent grants NO Session,
//! fence, mutation, replay, snapshot, receipt, lease or rendered-target rights.
//! Service must acquire/recheck its own live authority and retain admitted I/O.
use crate::{capture_backend, js_json, observation::js_string};
use serde_json::{Map, Value, json};
use std::fmt;

pub const MAX_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_NODES: usize = 16_384;
pub const MAX_RECORDS: usize = 4096;
pub const MAX_DEPTH: usize = 64;
pub const MAX_REPORT_BYTES: usize = 1024 * 1024;
const MAX_SAFE: f64 = 9_007_199_254_740_991.0;
const NO_INVENTORY: &str = "capability_unavailable: no safe existing capture inventory with an observable input UUID is available. Configure a window capture in OBS, then rerun; use obs-prepare-demo for an existing bound capture. No capture changed";
const PORTAL: &str = "capability_unavailable: create and authorize a PipeWire window capture using OBS's desktop portal picker, then call with backend:portal and that inputName. No capture changed";
const SOURCE_CHANGED: &str = "Capture source or selection changed during discovery; rerun with a stable window. No capture changed";
const UUID: &str = "Capture discovery requires an observable nonempty input UUID; configure a capture in a UUID-capable OBS instance. No capture changed";
const MEMBERSHIP: &str =
    "Existing capture must appear in the requested target scene; no capture changed";
const XCOMPOSITE: &str = "Listing windows on an unconfigured XComposite input can crash OBS. Select a window in OBS first, then inspect or plan its binding; discovery never seeds this property.";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind {
    Direct,
    Thrown,
    NativeLimit,
}
#[derive(Clone)]
pub struct Failure {
    kind: FailureKind,
    message: String,
}
impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureFailure")
            .field("kind", &self.kind)
            .field("bytes", &self.message.len())
            .finish()
    }
}
impl Failure {
    fn direct(message: impl Into<String>) -> Self {
        Self::new(FailureKind::Direct, message.into())
    }
    fn thrown(message: impl Into<String>) -> Self {
        Self::new(FailureKind::Thrown, message.into())
    }
    fn new(kind: FailureKind, message: String) -> Self {
        if message.len() > MAX_REPORT_BYTES / 2 {
            return limit();
        }
        let failure = Self { kind, message };
        // Raw characters do not bound their escaped JSON representation.
        if report(failure.body()).is_err() {
            return limit();
        }
        failure
    }
    pub fn kind(&self) -> FailureKind {
        self.kind
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn result(&self) -> Value {
        // Keep this boundary checked independently of constructor admission.
        // The fixed ASCII fallback uses body(), never result()/new(), so an
        // oversized diagnostic cannot recurse or escape the report budget.
        report(self.body()).unwrap_or_else(|_| limit().body())
    }
    fn body(&self) -> Value {
        let text = if self.kind == FailureKind::Thrown {
            format!("Capture Window failed: {}", self.message)
        } else {
            self.message.clone()
        };
        json!({"content":[{"type":"text","text":text}],"isError":true})
    }
    /// A known persisted snapshot is an independent fact, never rollback rights.
    pub fn after_snapshot(&self, id: &str) -> Result<Value, Self> {
        bounded_strings(&[id])?;
        let mut result = self.result();
        result["content"].as_array_mut().unwrap().push(json!({"type":"text","text":format!("Saved the previous state as snapshot {id} before detecting the conflict; this call applied no capture changes. Inspect OBS before retrying")}));
        report(result)
    }
}
fn limit() -> Failure {
    Failure {
        kind: FailureKind::NativeLimit,
        message:
            "Capture Window domain evidence exceeds native limits; no execution authority granted"
                .into(),
    }
}
fn invalid() -> Failure {
    Failure::thrown("Invalid capture-window arguments")
}
/// These are selective native admission bounds, not Source schema constraints or RSS bounds.
fn bounded(values: &[&Value]) -> Result<(), Failure> {
    fn walk(
        value: &Value,
        depth: usize,
        nodes: &mut usize,
        records: &mut usize,
    ) -> Result<(), Failure> {
        if depth > MAX_DEPTH || *nodes == 0 {
            return Err(limit());
        }
        *nodes -= 1;
        match value {
            Value::Array(rows) => {
                *records = records
                    .checked_add(rows.len())
                    .filter(|n| *n <= MAX_RECORDS)
                    .ok_or_else(limit)?;
                for row in rows {
                    walk(row, depth + 1, nodes, records)?;
                }
            }
            Value::Object(fields) => {
                for v in fields.values() {
                    walk(v, depth + 1, nodes, records)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let (mut nodes, mut records, mut bytes) = (MAX_NODES, 0usize, 0usize);
    for value in values {
        walk(value, 0, &mut nodes, &mut records)?;
        bytes = bytes
            .checked_add(
                js_json::encoded_len(
                    value,
                    js_json::Limits {
                        max_depth: MAX_DEPTH,
                        max_encoded_bytes: MAX_BYTES,
                    },
                )
                .map_err(|_| limit())?,
            )
            .filter(|n| *n <= MAX_BYTES)
            .ok_or_else(limit)?;
    }
    Ok(())
}
fn bounded_strings(values: &[&str]) -> Result<(), Failure> {
    let mut total = 0usize;
    for value in values {
        total = total
            .checked_add(value.len())
            .filter(|n| *n <= MAX_REPORT_BYTES / 2)
            .ok_or_else(limit)?;
    }
    Ok(())
}
fn report(value: Value) -> Result<Value, Failure> {
    js_json::encoded_len(
        &value,
        js_json::Limits {
            max_depth: MAX_DEPTH,
            max_encoded_bytes: MAX_REPORT_BYTES,
        },
    )
    .map_err(|_| limit())?;
    Ok(value)
}
fn owned(value: &Value) -> Result<Value, Failure> {
    bounded(&[value])?;
    let mut value = value.clone();
    js_json::normalize_json_numbers(&mut value).map_err(|_| limit())?;
    Ok(value)
}
fn same(a: &Value, b: &Value) -> Result<bool, Failure> {
    Ok(owned(a)? == owned(b)?)
}
fn blank(value: &str) -> bool {
    value.trim_matches(|c| matches!(c, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')).is_empty()
}
fn truthy(value: Option<&str>) -> bool {
    value.is_some_and(|s| !s.is_empty())
}
fn safe(value: &Value) -> Option<i64> {
    let n = value.as_f64()?;
    (n.is_finite() && n.fract() == 0.0 && n.abs() <= MAX_SAFE).then_some(n as i64)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    Auto,
    X11,
    Portal,
}
impl Backend {
    fn name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::X11 => "x11",
            Self::Portal => "portal",
        }
    }
}
#[derive(Clone)]
pub struct Request {
    backend: Backend,
    scene: String,
    input: String,
    window: Option<String>,
    application: Option<String>,
    display: Option<String>,
    silent: bool,
    fit: bool,
    screenshot: bool,
    apply: bool,
}
impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureRequest")
            .field("backend", &self.backend)
            .field("apply", &self.apply)
            .finish_non_exhaustive()
    }
}
impl Request {
    pub fn parse(args: &Value) -> Result<Self, Failure> {
        bounded(&[args])?;
        let fields = args.as_object().ok_or_else(invalid)?;
        let required = |name| {
            fields
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(invalid)
        };
        let optional = |name| {
            fields
                .get(name)
                .map(|v| v.as_str().map(str::to_owned).ok_or_else(invalid))
                .transpose()
        };
        let boolean = |name, default| {
            fields
                .get(name)
                .map(|v| v.as_bool().ok_or_else(invalid))
                .transpose()
                .map(|v| v.unwrap_or(default))
        };
        let backend = match fields.get("backend").map(Value::as_str) {
            None | Some(Some("auto")) => Backend::Auto,
            Some(Some("x11")) => Backend::X11,
            Some(Some("portal")) => Backend::Portal,
            _ => return Err(invalid()),
        };
        Ok(Self {
            backend,
            scene: required("sceneName")?,
            input: required("inputName")?,
            window: optional("window")?,
            application: optional("application")?,
            display: optional("displayUuid")?,
            silent: boolean("silent", true)?,
            fit: boolean("fit", true)?,
            screenshot: boolean("includeScreenshot", false)?,
            apply: boolean("apply", true)?,
        })
    }
    pub fn backend(&self) -> Backend {
        self.backend
    }
    pub fn scene_name(&self) -> &str {
        &self.scene
    }
    pub fn input_name(&self) -> &str {
        &self.input
    }
    pub fn apply(&self) -> bool {
        self.apply
    }
    pub fn fit(&self) -> bool {
        self.fit
    }
    pub fn silent(&self) -> bool {
        self.silent
    }
    pub fn include_screenshot(&self) -> bool {
        self.screenshot
    }
    pub fn arguments(&self) -> Value {
        let mut v = json!({"backend":self.backend.name(),"sceneName":self.scene,"inputName":self.input,"silent":self.silent,"fit":self.fit,"includeScreenshot":self.screenshot,"apply":self.apply});
        for (k, s) in [
            ("window", &self.window),
            ("application", &self.application),
            ("displayUuid", &self.display),
        ] {
            if let Some(s) = s {
                v[k] = json!(s);
            }
        }
        v
    }
    pub fn selector_refusal(&self) -> Option<Failure> {
        (self.backend != Backend::Portal
            && truthy(self.window.as_deref()) == truthy(self.application.as_deref()))
        .then(|| Failure::direct("Give exactly one of window or application"))
    }
    /// Source's platform refusal can run before collecting scene/input evidence.
    /// The platform must come from actual OBS metadata, not the MCP host.
    pub fn platform_refusal(&self, platform: Option<&str>) -> Option<Failure> {
        if platform == Some("macos") {
            if self.backend != Backend::Auto {
                return Some(Failure::direct(
                    "capability_unavailable: the requested Linux backend is unavailable on macOS; use backend:auto for ScreenCaptureKit",
                ));
            }
            if truthy(self.application.as_deref()) {
                return Some(Failure::direct(
                    "capability_unavailable: Mac application auto-discovery is unsafe because native application string-list serialization can crash OBS. Configure the intended application capture in OBS and bind that existing source; no capture changed",
                ));
            }
        }
        (platform == Some("windows")).then(|| Failure::direct("OBS is not on macOS or Linux; this workflow supports ScreenCaptureKit, X11 and PipeWire"))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    ScreenCapture,
    XComposite,
    PipeWireWindow,
    PipeWireScreen,
}
impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Self::ScreenCapture => "screen_capture",
            Self::XComposite => "xcomposite_input",
            Self::PipeWireWindow => "pipewire-window-capture-source",
            Self::PipeWireScreen => "pipewire-screen-capture-source",
        }
    }
    pub fn portal(self) -> bool {
        matches!(self, Self::PipeWireWindow | Self::PipeWireScreen)
    }
    pub fn video_only(self) -> bool {
        capture_backend::video_only(self.name())
    }
    fn parse(name: &str) -> Option<Self> {
        match name {
            "screen_capture" => Some(Self::ScreenCapture),
            "xcomposite_input" => Some(Self::XComposite),
            "pipewire-window-capture-source" => Some(Self::PipeWireWindow),
            "pipewire-screen-capture-source" => Some(Self::PipeWireScreen),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SceneIdentity {
    name: String,
    uuid: String,
}
impl SceneIdentity {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn uuid(&self) -> &str {
        &self.uuid
    }
    pub fn projection(&self) -> Value {
        json!({"sceneName":self.name,"sceneUuid":self.uuid})
    }
}
#[derive(Clone, PartialEq, Eq)]
pub struct InputIdentity {
    name: String,
    uuid: String,
    kind: Kind,
}
impl fmt::Debug for InputIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureInputIdentity")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}
impl InputIdentity {
    fn parse(value: &Value) -> Result<Self, Failure> {
        Ok(Self {
            name: value["inputName"]
                .as_str()
                .ok_or_else(|| Failure::thrown(UUID))?
                .into(),
            uuid: value["inputUuid"]
                .as_str()
                .filter(|s| !blank(s))
                .ok_or_else(|| Failure::thrown(UUID))?
                .into(),
            kind: Kind::parse(value["inputKind"].as_str().unwrap_or("")).ok_or_else(|| {
                Failure::thrown(
                    "Capture source identity or settings are unavailable; no capture changed",
                )
            })?,
        })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn uuid(&self) -> &str {
        &self.uuid
    }
    pub fn kind(&self) -> Kind {
        self.kind
    }
    pub fn projection(&self) -> Value {
        json!({"inputName":self.name,"inputUuid":self.uuid,"inputKind":self.kind.name()})
    }
    fn selector(&self) -> Value {
        json!({"inputName":self.name,"inputUuid":self.uuid})
    }
}
pub struct Candidate<'a> {
    pub input_uuid: &'a str,
    pub settings: &'a Value,
    pub properties: &'a Value,
}
pub struct Readings<'a> {
    pub scenes: &'a Value,
    pub inputs: &'a Value,
    pub membership: &'a Value,
    pub candidates: &'a [Candidate<'a>],
}
impl Readings<'_> {
    fn check(&self) -> Result<(), Failure> {
        if self.candidates.len() > MAX_RECORDS {
            return Err(limit());
        }
        let mut values = vec![self.scenes, self.inputs, self.membership];
        let mut selector_bytes = 0usize;
        for candidate in self.candidates {
            selector_bytes = selector_bytes
                .checked_add(candidate.input_uuid.len())
                .filter(|n| *n <= MAX_BYTES)
                .ok_or_else(limit)?;
            bounded_strings(&[candidate.input_uuid])?;
            values.push(candidate.settings);
            values.push(candidate.properties);
        }
        bounded(&values)
    }
    fn candidate(&self, id: &InputIdentity) -> Result<&Candidate<'_>, Failure> {
        self.candidates
            .iter()
            .find(|c| c.input_uuid == id.uuid)
            .ok_or_else(|| {
                Failure::thrown(
                    "Capture source identity or settings are unavailable; no capture changed",
                )
            })
    }
}
fn scene(request: &Request, value: &Value) -> Result<SceneIdentity, Failure> {
    let matches = value["scenes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|row| row.is_object() && row["sceneName"].as_str() == Some(&request.scene))
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(Failure::thrown(
            "Target scene must exist uniquely with an observable UUID; no capture changed",
        ));
    }
    let uuid = matches[0]["sceneUuid"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Failure::thrown(
                "Target scene must exist uniquely with an observable UUID; no capture changed",
            )
        })?;
    Ok(SceneIdentity {
        name: request.scene.clone(),
        uuid: uuid.into(),
    })
}
fn inputs(value: &Value) -> Result<&[Value], Failure> {
    value["inputs"]
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| Failure::thrown("Capture inventory is unavailable; no capture changed"))
}
fn found<'a>(rows: &'a [Value], name: &str) -> Option<&'a Value> {
    rows.iter()
        .find(|row| row.is_object() && row["inputName"].as_str() == Some(name))
}
fn membership(input: &InputIdentity, value: &Value) -> Result<Vec<i64>, Failure> {
    let rows = value["sceneItems"].as_array().ok_or_else(|| {
        Failure::thrown("Target-scene membership is unavailable; no capture changed")
    })?;
    let mut ids = Vec::new();
    for row in rows.iter().filter(|row| {
        row.is_object()
            && row["sourceName"].as_str() == Some(&input.name)
            && row["sourceUuid"].as_str() == Some(&input.uuid)
    }) {
        ids.push(safe(&row["sceneItemId"]).ok_or_else(|| Failure::thrown(MEMBERSHIP))?);
    }
    if ids.is_empty() {
        return Err(Failure::thrown(MEMBERSHIP));
    }
    ids.sort();
    Ok(ids)
}
fn settings(kind: Kind, response: &Value) -> Result<&Map<String, Value>, Failure> {
    if response["inputKind"] != kind.name() {
        return Err(Failure::thrown(
            "Capture source identity or settings are unavailable; no capture changed",
        ));
    }
    response["inputSettings"].as_object().ok_or_else(|| {
        Failure::thrown("Capture source identity or settings are unavailable; no capture changed")
    })
}
fn inventory_guard(kind: Kind, settings: &Map<String, Value>) -> Result<(), Failure> {
    if kind.portal() {
        if !capture_backend::configured_window(kind.name(), settings) {
            return Err(Failure::thrown(
                "capability_unavailable: PipeWire capture has no persisted portal selection; authorize it in OBS first",
            ));
        }
    } else if kind == Kind::XComposite
        && settings
            .get("capture_window")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return Err(Failure::thrown(XCOMPOSITE));
    }
    Ok(())
}
fn item_string(value: Option<&Value>) -> Result<String, Failure> {
    match value {
        Some(Value::Object(fields)) if fields.contains_key("toString") => {
            Err(Failure::thrown("Cannot convert object to primitive value"))
        }
        Some(Value::Array(rows)) => {
            let mut parts = Vec::with_capacity(rows.len());
            for row in rows {
                parts.push(if row.is_null() {
                    String::new()
                } else {
                    item_string(Some(row))?
                });
            }
            Ok(parts.join(","))
        }
        _ => Ok(js_string(value)),
    }
}
fn select(request: &Request, kind: Kind, response: &Value) -> Result<Value, Failure> {
    let wanted = request
        .window
        .as_deref()
        .or(request.application.as_deref())
        .unwrap_or("")
        .to_lowercase();
    let property = if kind == Kind::ScreenCapture {
        "window"
    } else {
        "capture_window"
    };
    let items = response["propertyItems"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|row| {
            row.is_object()
                && row["itemName"].as_str().is_some_and(|s| !blank(s))
                && row.get("itemEnabled") != Some(&Value::Bool(false))
        })
        .collect::<Vec<_>>();
    let mut matched = Vec::new();
    for item in &items {
        let name = item["itemName"].as_str().unwrap();
        let yes = if name.to_lowercase().contains(&wanted) {
            true
        } else {
            let value = item_string(item.get("itemValue"))?;
            value.to_lowercase() == wanted
                || (kind != Kind::ScreenCapture
                    && truthy(request.window.as_deref())
                    && value.split("\r\n").next() == Some(wanted.as_str()))
                || (kind != Kind::ScreenCapture
                    && truthy(request.application.as_deref())
                    && value
                        .to_lowercase()
                        .split("\r\n")
                        .skip(1)
                        .any(|part| part.contains(&wanted)))
        };
        if yes {
            matched.push(*item);
        }
    }
    if matched.len() != 1 {
        let names = if matched.len() > 1 { &matched } else { &items };
        let names = names
            .iter()
            .take(15)
            .map(|row| format!("- {}", row["itemName"].as_str().unwrap()))
            .collect::<Vec<_>>()
            .join("\n");
        let query = request
            .window
            .as_deref()
            .or(request.application.as_deref())
            .unwrap_or("undefined");
        return Err(Failure::thrown(format!(
            "{} {property} matches \"{query}\". {}:\n{names}",
            if matched.is_empty() {
                "No"
            } else {
                "More than one"
            },
            if matched.len() > 1 {
                "Matches"
            } else {
                "Available"
            }
        )));
    }
    let selected = matched[0];
    if kind == Kind::ScreenCapture {
        if safe(&selected["itemValue"]).is_none_or(|id| id <= 0) {
            return Err(Failure::thrown(
                "The selected Mac window has no positive integer identity; no capture changed",
            ));
        }
    } else {
        let valid = selected["itemValue"].as_str().is_some_and(|s| {
            let parts = s.split("\r\n").collect::<Vec<_>>();
            matches!(parts.len(), 1 | 3)
                && parts.iter().all(|p| !p.contains(['\r', '\n']))
                && !parts[0].is_empty()
                && parts[0].bytes().all(|b| b.is_ascii_digit())
                && parts[0]
                    .parse::<u64>()
                    .is_ok_and(|n| n > 0 && n <= u32::MAX as u64)
        });
        if !valid {
            return Err(Failure::thrown(
                "The selected X11 window has no well-formed positive XID; no capture changed",
            ));
        }
    }
    owned(selected)
}
#[derive(Clone)]
pub struct Discovery {
    request: Request,
    kind: Kind,
    scene: SceneIdentity,
    existing: Option<InputIdentity>,
    inventory: InputIdentity,
    settings: Value,
    members: Vec<i64>,
    selected: Option<Value>,
}
impl fmt::Debug for Discovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureDiscovery")
            .field("kind", &self.kind)
            .field("existing", &self.existing.is_some())
            .finish_non_exhaustive()
    }
}
pub fn discover(
    request: Request,
    platform: Option<&str>,
    advertised: &[String],
    readings: Readings<'_>,
) -> Result<Discovery, Failure> {
    if let Some(error) = request.selector_refusal() {
        return Err(error);
    }
    if let Some(error) = request.platform_refusal(platform) {
        return Err(error);
    }
    readings.check()?;
    if advertised.len() > MAX_RECORDS {
        return Err(limit());
    }
    bounded_strings(&advertised.iter().map(String::as_str).collect::<Vec<_>>())?;
    let mac = platform == Some("macos");
    let scene = scene(&request, readings.scenes)?;
    let rows = inputs(readings.inputs)?;
    let existing = found(rows, &request.input);
    let existing_kind = existing
        .and_then(|r| r["inputKind"].as_str())
        .and_then(Kind::parse);
    let kind = if mac {
        Kind::ScreenCapture
    } else {
        let portal = existing_kind.filter(|kind| kind.portal());
        if request.backend == Backend::Portal && portal.is_none() {
            return Err(Failure::direct(PORTAL));
        }
        let kind = if request.backend == Backend::X11 {
            Kind::XComposite
        } else if request.backend == Backend::Portal {
            portal.unwrap()
        } else if let Some(kind) = existing_kind.filter(|kind| kind.video_only()) {
            kind
        } else if advertised.iter().any(|name| name == "xcomposite_input") {
            Kind::XComposite
        } else {
            Kind::PipeWireScreen
        };
        if !advertised.iter().any(|name| name == kind.name()) && existing_kind != Some(kind) {
            return Err(Failure::direct(
                "capability_unavailable: OBS advertises no requested Linux window backend. On Wayland, configure a PipeWire window capture in OBS through the desktop portal; on X11 enable a window manager and XComposite. No capture changed",
            ));
        }
        if kind.portal() {
            if existing_kind != Some(kind) {
                return Err(Failure::direct(PORTAL));
            }
            if truthy(request.window.as_deref()) || truthy(request.application.as_deref()) {
                return Err(Failure::direct(
                    "capability_unavailable: the Wayland portal owns window selection. Select the intended window in OBS, inspect its image, then call with backend:portal and omit window/application. No capture changed",
                ));
            }
        }
        kind
    };
    if let Some(existing) = existing
        && existing["inputKind"] != kind.name()
    {
        return Err(Failure::direct(format!(
            "Input {} exists but is a {}, not a {}",
            request.input,
            js_string(existing.get("inputKind")),
            kind.name()
        )));
    }
    let existing = existing.map(InputIdentity::parse).transpose()?;
    let members = existing
        .as_ref()
        .map(|id| membership(id, readings.membership))
        .transpose()?
        .unwrap_or_default();
    let candidates = if let Some(existing) = &existing {
        vec![existing.clone()]
    } else {
        rows.iter()
            .filter(|row| row["inputKind"] == kind.name())
            .filter_map(|row| InputIdentity::parse(row).ok())
            .collect::<Vec<_>>()
    };
    let mut inventory = None;
    for identity in candidates {
        let result = (|| {
            let read = readings.candidate(&identity)?;
            let settings = settings(kind, read.settings)?;
            inventory_guard(kind, settings)?;
            Ok::<_, Failure>((
                owned(&read.settings["inputSettings"])?,
                if kind.portal() {
                    None
                } else {
                    Some(read.properties)
                },
            ))
        })();
        match result {
            Ok((settings, properties)) => {
                inventory = Some((identity, settings, properties));
                break;
            }
            Err(error) if existing.is_some() => return Err(error),
            Err(_) => {}
        }
    }
    let (inventory, settings, properties) =
        inventory.ok_or_else(|| Failure::direct(NO_INVENTORY))?;
    let selected = properties
        .map(|properties| select(&request, kind, properties))
        .transpose()?;
    Ok(Discovery {
        request,
        kind,
        scene,
        existing,
        inventory,
        settings,
        members,
        selected,
    })
}
impl Discovery {
    pub fn request(&self) -> &Request {
        &self.request
    }
    pub fn kind(&self) -> Kind {
        self.kind
    }
    pub fn scene(&self) -> &SceneIdentity {
        &self.scene
    }
    pub fn inventory(&self) -> &InputIdentity {
        &self.inventory
    }
    pub fn existing(&self) -> Option<&InputIdentity> {
        self.existing.as_ref()
    }
    pub fn selected(&self) -> Option<&Value> {
        self.selected.as_ref()
    }
    pub fn membership(&self) -> &[i64] {
        &self.members
    }
    pub fn compare(&self, readings: Readings<'_>) -> Result<(), Failure> {
        readings.check()?;
        let rows = inputs(readings.inputs)?;
        let matches = |identity: Option<&InputIdentity>, row: Option<&Value>| match (identity, row)
        {
            (None, None) => true,
            (Some(identity), Some(row)) => {
                row["inputName"] == identity.name
                    && row["inputUuid"] == identity.uuid
                    && row["inputKind"] == identity.kind.name()
            }
            _ => false,
        };
        if !matches(self.existing.as_ref(), found(rows, &self.request.input))
            || !matches(Some(&self.inventory), found(rows, &self.inventory.name))
        {
            return Err(Failure::direct(SOURCE_CHANGED));
        }
        let read = readings.candidate(&self.inventory)?;
        settings(self.kind, read.settings)?;
        if !same(&self.settings, &read.settings["inputSettings"])? {
            return Err(Failure::direct(SOURCE_CHANGED));
        }
        if !self.kind.portal()
            && !same(
                self.selected.as_ref().unwrap(),
                &select(&self.request, self.kind, read.properties)?,
            )?
        {
            return Err(Failure::direct(
                "Window selection changed during discovery; rerun with a stable window. No capture changed",
            ));
        }
        if scene(&self.request, readings.scenes)? != self.scene {
            return Err(Failure::direct(
                "Target scene or capture membership changed during discovery; no capture changed",
            ));
        }
        if let Some(id) = &self.existing
            && membership(id, readings.membership)? != self.members
        {
            return Err(Failure::direct(
                "Target scene or capture membership changed during discovery; no capture changed",
            ));
        }
        Ok(())
    }
    pub fn plan_result(&self) -> Result<Value, Failure> {
        report(
            json!({"content":[{"type":"text","text":"Capture plan only; no capture changed. Rendered target is unverified; inspect the image after applying."}],"structuredContent":{"applied":false,"rendered":"unverified","sceneName":self.request.scene,"scene":self.scene.projection(),"inputName":self.request.input,"existing":self.existing.as_ref().map(InputIdentity::projection),"inventory":self.inventory.projection(),"selected":self.selected.clone().unwrap_or_else(||json!({"inputName":self.request.input,"kind":"existing-portal-selection","persistedSelection":true})),"changes":{"create":self.existing.is_none(),"retarget":!self.kind.portal(),"silent":self.request.silent,"fit":self.request.fit}}}),
        )
    }
    pub fn snapshot_intent(&self) -> Option<SnapshotIntent> {
        (self.request.apply && self.existing.is_some()).then(|| SnapshotIntent {
            scene: self.request.scene.clone(),
            input: self.request.input.clone(),
        })
    }
    fn selected_settings(&self) -> Value {
        let value = self
            .selected
            .as_ref()
            .and_then(|row| row.get("itemValue"))
            .cloned()
            .unwrap_or(Value::Null);
        if self.kind == Kind::ScreenCapture {
            json!({"type":1,"window":value})
        } else {
            json!({"capture_window":value})
        }
    }
    pub fn initial_intent(&self) -> Option<Effect> {
        if !self.request.apply {
            return None;
        }
        if let Some(target) = &self.existing {
            (!self.kind.portal()).then(|| {
                Effect(EffectData::Retarget {
                    target: target.clone(),
                    settings: self.selected_settings(),
                })
            })
        } else {
            let mut settings = self.selected_settings();
            settings["show_cursor"] = json!(true);
            Some(Effect(EffectData::Create {
                scene: self.scene.clone(),
                input: self.request.input.clone(),
                kind: self.kind,
                settings,
            }))
        }
    }
    /// Interprets a known ACK body only. This function cannot establish that any
    /// create was sent, accepted, still exists, or belongs to a live Session.
    pub fn created_target(&self, response: &Value) -> Result<InputIdentity, Failure> {
        bounded(&[response])?;
        if self.existing.is_some() {
            return Err(Failure::thrown("Capture target already existed"));
        }
        let uuid=response["inputUuid"].as_str().filter(|s|!blank(s)).ok_or_else(||Failure::direct("OBS created the configured capture but did not report its UUID. No further changes were made; inspect OBS before retrying"))?;
        Ok(InputIdentity {
            name: self.request.input.clone(),
            uuid: uuid.into(),
            kind: self.kind,
        })
    }
    fn target(&self, target: &InputIdentity) -> Result<(), Failure> {
        if target.name != self.request.input
            || target.kind != self.kind
            || self.existing.as_ref().is_some_and(|old| old != target)
        {
            return Err(Failure::direct(SOURCE_CHANGED));
        }
        Ok(())
    }
    pub fn audio_intents(&self, target: &InputIdentity) -> Result<Vec<Effect>, Failure> {
        self.target(target)?;
        Ok(
            if self.request.apply && self.request.silent && !self.kind.video_only() {
                vec![
                    Effect(EffectData::Mute {
                        target: target.clone(),
                    }),
                    Effect(EffectData::TracksOff {
                        target: target.clone(),
                    }),
                ]
            } else {
                vec![]
            },
        )
    }
    /// Deliberately stronger than Source's name-only GetSceneItemId result.
    /// The composing Service must supply current scene/member UUID evidence.
    pub fn fit_intent(
        &self,
        target: &InputIdentity,
        scene_value: &Value,
        members: &Value,
        video: &Value,
        item: &Value,
    ) -> Result<Option<Effect>, Failure> {
        self.target(target)?;
        bounded(&[scene_value, members, video, item])?;
        if !self.request.apply || !self.request.fit {
            return Ok(None);
        }
        if scene(&self.request, scene_value)? != self.scene {
            return Err(Failure::direct(SOURCE_CHANGED));
        }
        let id = safe(&item["sceneItemId"])
            .filter(|id| *id >= 0)
            .ok_or_else(|| {
                Failure::thrown("Capture fit requires a safe current scene item identity")
            })?;
        if !membership(target, members)?.contains(&id) {
            return Err(Failure::direct(SOURCE_CHANGED));
        }
        let dimensions = Canvas::parse(video)?;
        Ok(Some(Effect(EffectData::Fit {
            scene: self.scene.clone(),
            target: target.clone(),
            item: id,
            canvas: dimensions,
        })))
    }
    pub fn result(
        &self,
        snapshot: Option<&str>,
        state: &Value,
        silence: &[Status],
        fitted: Option<Canvas>,
    ) -> Result<Value, Failure> {
        if !self.request.apply {
            return self.plan_result();
        }
        bounded(&[state])?;
        if !state["inputSettings"].is_object()
            || !state["appearances"].is_array()
            || !state["warnings"].is_array()
        {
            return Err(Failure::thrown(
                "Capture input-state evidence is unavailable",
            ));
        }
        if let Some(id) = snapshot {
            bounded_strings(&[id])?;
        }
        let mut notes = vec![if self.kind.portal() {
            "Using the existing portal selection; inspect the screenshot for target correctness"
                .into()
        } else {
            format!(
                "{} {} {}",
                if self.kind == Kind::ScreenCapture {
                    "Capturing"
                } else {
                    "Selected"
                },
                if self.kind == Kind::ScreenCapture {
                    "window"
                } else {
                    "capture_window"
                },
                self.selected.as_ref().unwrap()["itemName"]
                    .as_str()
                    .unwrap()
            )
        }];
        if let Some(id) = snapshot {
            notes.push(format!(
                "Saved the previous state as snapshot {id}; obs-restore undoes this change"
            ));
        }
        if self.request.silent {
            if self.kind.video_only() {
                notes.push("This video-only capture has no audio; inspect other audio inputs with obs-preflight".into());
            } else {
                validate_silence(silence)?;
                notes.push("Muted the capture and removed it from every audio track".into());
            }
        }
        if self.request.fit {
            let canvas = fitted
                .ok_or_else(|| Failure::thrown("Capture fit acknowledgement is unavailable"))?;
            notes.push(format!(
                "Fitted to the {}×{} canvas",
                js_string(Some(&json!(canvas.width))),
                js_string(Some(&json!(canvas.height)))
            ));
        }
        let message = notes.join(". ");
        let mut lines = vec![message.clone()];
        let mut sizes = vec![];
        for appearance in state["appearances"].as_array().unwrap() {
            if !appearance["sceneName"].is_string() {
                return Err(Failure::thrown(
                    "Capture appearance evidence is unavailable",
                ));
            }
            sizes.push(format!(
                "{} item {}: {}×{}",
                appearance["sceneName"].as_str().unwrap(),
                js_string(appearance.get("sceneItemId")),
                js_string(appearance.get("sourceWidth")),
                js_string(appearance.get("sourceHeight"))
            ));
        }
        lines.push(if sizes.is_empty() {
            "Not in the program scene".into()
        } else {
            format!("Size in program scene: {}", sizes.join("; "))
        });
        for warning in state["warnings"].as_array().unwrap() {
            lines.push(format!(
                "Warning: {}",
                warning
                    .as_str()
                    .ok_or_else(|| Failure::thrown("Capture warning evidence is unavailable"))?
            ));
        }
        report(
            json!({"content":[{"type":"text","text":lines.join("\n")}],"structuredContent":{"message":message,"inputSettings":state["inputSettings"],"appearances":state["appearances"],"warnings":state["warnings"]}}),
        )
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Canvas {
    width: f64,
    height: f64,
}
impl Canvas {
    pub fn parse(value: &Value) -> Result<Self, Failure> {
        let dimension = |key| {
            value
                .get(key)
                .and_then(Value::as_f64)
                .filter(|n| n.is_finite() && *n > 0.0)
                .ok_or_else(|| Failure::thrown("Capture canvas dimensions are unavailable"))
        };
        Ok(Self {
            width: dimension("baseWidth")?,
            height: dimension("baseHeight")?,
        })
    }
}
pub struct SnapshotIntent {
    scene: String,
    input: String,
}
impl SnapshotIntent {
    pub fn projection(&self) -> Value {
        json!({"inputs":[self.input],"scenes":[self.scene],"label":"before obs-capture-window","auto":true})
    }
}
#[derive(Clone, Debug)]
pub struct Status {
    pub request_type: String,
    pub ok: bool,
    pub code: i64,
    pub comment: Option<String>,
}
pub fn validate_silence(rows: &[Status]) -> Result<(), Failure> {
    if rows.len() > MAX_RECORDS {
        return Err(limit());
    }
    for row in rows {
        bounded_strings(&[&row.request_type, row.comment.as_deref().unwrap_or("")])?;
        if !row.ok {
            return Err(Failure::thrown(format!(
                "{} failed with code {}{}",
                row.request_type,
                row.code,
                row.comment
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .map(|s| format!(": {s}"))
                    .unwrap_or_default()
            )));
        }
    }
    if rows.len() != 2
        || rows[0].request_type != "SetInputMute"
        || rows[1].request_type != "SetInputAudioTracks"
    {
        return Err(Failure::thrown(
            "Capture silence was not fully acknowledged; inspect OBS before retrying",
        ));
    }
    Ok(())
}
/// Closed inert descriptions. projection() is data, never dispatch authority.
#[derive(Clone, Debug)]
enum EffectData {
    Create {
        scene: SceneIdentity,
        input: String,
        kind: Kind,
        settings: Value,
    },
    Retarget {
        target: InputIdentity,
        settings: Value,
    },
    Mute {
        target: InputIdentity,
    },
    TracksOff {
        target: InputIdentity,
    },
    Fit {
        scene: SceneIdentity,
        target: InputIdentity,
        item: i64,
        canvas: Canvas,
    },
}
/// The description is constructed only by this checked pure domain. It is still
/// ordinary data: the later transport must independently bind/recheck authority.
/// ```compile_fail
/// use obs_mcp_native::capture_window::Effect;
/// let arbitrary = Effect::new("StartRecord", serde_json::json!({}));
/// ```
#[derive(Debug)]
pub struct Effect(EffectData);
impl Effect {
    pub fn target(&self) -> Option<&InputIdentity> {
        match &self.0 {
            EffectData::Create { .. } => None,
            EffectData::Retarget { target, .. }
            | EffectData::Mute { target }
            | EffectData::TracksOff { target }
            | EffectData::Fit { target, .. } => Some(target),
        }
    }
    pub fn projection(&self) -> Value {
        match &self.0 {
            EffectData::Create {
                scene,
                input,
                kind,
                settings,
            } => {
                json!({"requestType":"CreateInput","requestData":{"sceneName":scene.name,"sceneUuid":scene.uuid,"inputName":input,"inputKind":kind.name(),"inputSettings":settings}})
            }
            EffectData::Retarget { target, settings } => {
                let mut data = target.selector();
                data["inputSettings"] = settings.clone();
                json!({"requestType":"SetInputSettings","requestData":data})
            }
            EffectData::Mute { target } => {
                let mut data = target.selector();
                data["inputMuted"] = json!(true);
                json!({"requestType":"SetInputMute","requestData":data})
            }
            EffectData::TracksOff { target } => {
                let mut data = target.selector();
                data["inputAudioTracks"] =
                    json!({"1":false,"2":false,"3":false,"4":false,"5":false,"6":false});
                json!({"requestType":"SetInputAudioTracks","requestData":data})
            }
            EffectData::Fit {
                scene,
                target: _,
                item,
                canvas,
            } => {
                json!({"requestType":"SetSceneItemTransform","requestData":{"sceneName":scene.name,"sceneUuid":scene.uuid,"sceneItemId":item,"sceneItemTransform":{"boundsType":"OBS_BOUNDS_SCALE_INNER","boundsWidth":canvas.width,"boundsHeight":canvas.height,"boundsAlignment":0,"alignment":5,"positionX":0,"positionY":0}}})
            }
        }
    }
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;
    #[test]
    fn every_failure_result_has_an_independent_encoded_guard_and_fixed_fallback() {
        for kind in [
            FailureKind::Direct,
            FailureKind::Thrown,
            FailureKind::NativeLimit,
        ] {
            // Exercise result() even if an internal future path bypasses new().
            let failure = Failure {
                kind,
                message: "\u{0001}".repeat(200_000),
            };
            let result = failure.result();
            assert!(serde_json::to_vec(&result).unwrap().len() <= MAX_REPORT_BYTES);
            assert_eq!(result, limit().body());
        }
    }
}
