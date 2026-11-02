// SPDX-License-Identifier: GPL-2.0-only
//! Pure Source-shaped Apply Scene planning and formatting. These owned values
//! hold no socket, writer, receipt, replay, recovery or recording authority.
use crate::{
    observation::{ObservationError, js_string},
    scene_plan::same_transform_value,
    snapshots::{self, InputState, ItemState},
    tool_validation::ApplySceneSettingsOrder,
};
use serde_json::{Map, Value, json};
use std::{
    collections::{HashMap, HashSet},
    fmt,
};

const TRANSFORM: &[&str] = &[
    "positionX",
    "positionY",
    "scaleX",
    "scaleY",
    "rotation",
    "alignment",
    "boundsType",
    "boundsWidth",
    "boundsHeight",
    "boundsAlignment",
    "cropLeft",
    "cropRight",
    "cropTop",
    "cropBottom",
];
const BOUNDS: &[&str] = &[
    "OBS_BOUNDS_NONE",
    "OBS_BOUNDS_STRETCH",
    "OBS_BOUNDS_SCALE_INNER",
    "OBS_BOUNDS_SCALE_OUTER",
    "OBS_BOUNDS_SCALE_TO_WIDTH",
    "OBS_BOUNDS_SCALE_TO_HEIGHT",
    "OBS_BOUNDS_MAX_ONLY",
];
fn invalid() -> ObservationError {
    ObservationError::new(
        "invalid_apply_scene_arguments",
        "Invalid apply-scene arguments",
    )
}
#[derive(Clone)]
pub struct SourceSpec {
    name: String,
    kind: Option<String>,
    settings: Option<Vec<(String, Value)>>,
    visible: Option<bool>,
    locked: Option<bool>,
    muted: Option<bool>,
    volume_db: Option<f64>,
    audio_tracks: Option<Vec<u8>>,
    fit: bool,
    transform: Vec<(String, Value)>,
    has_transform: bool,
}
impl fmt::Debug for SourceSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplySceneSource").finish_non_exhaustive()
    }
}
impl SourceSpec {
    pub fn name(&self) -> &str {
        &self.name
    }
    fn arguments(&self) -> Value {
        let mut value = json!({"name":self.name});
        if let Some(kind) = &self.kind {
            value["kind"] = json!(kind);
        }
        if let Some(settings) = &self.settings {
            value["settings"] = object(settings);
        }
        for (key, v) in [
            ("visible", self.visible),
            ("locked", self.locked),
            ("muted", self.muted),
        ] {
            if let Some(v) = v {
                value[key] = json!(v);
            }
        }
        if let Some(v) = self.volume_db {
            value["volumeDb"] = source_number(v);
        }
        if let Some(v) = &self.audio_tracks {
            value["audioTracks"] = json!(v);
        }
        if self.fit {
            value["fit"] = json!("canvas");
        }
        if self.has_transform {
            value["transform"] = object(&self.transform);
        }
        value
    }
}
#[derive(Clone)]
pub struct Request {
    scene_name: String,
    sources: Vec<SourceSpec>,
    order: bool,
    remove_others: bool,
    apply: bool,
}
impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplySceneRequest")
            .field("sources", &self.sources.len())
            .field("apply", &self.apply)
            .finish_non_exhaustive()
    }
}
impl Request {
    /// The key sidecar is metadata only; all values are independently parsed and
    /// normalized here. Unknown object fields are stripped like Source Zod.
    pub fn parse(args: &Value, order: &ApplySceneSettingsOrder) -> Result<Self, ObservationError> {
        let mut normalized = args.clone();
        crate::js_json::normalize_json_numbers(&mut normalized).map_err(|_| invalid())?;
        let args = normalized.as_object().ok_or_else(invalid)?;
        let scene_name = nonempty(args.get("sceneName"))?;
        let rows = args
            .get("sources")
            .and_then(Value::as_array)
            .filter(|rows| (1..=50).contains(&rows.len()) && rows.len() == order.sources())
            .ok_or_else(invalid)?;
        let mut sources = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            let row = row.as_object().ok_or_else(invalid)?;
            let name = nonempty(row.get("name"))?;
            let kind = row
                .get("kind")
                .map(|v| v.as_str().map(str::to_owned).ok_or_else(invalid))
                .transpose()?;
            let settings = match row.get("settings") {
                None if order.keys(index).is_none() => None,
                Some(value) => {
                    let values = value.as_object().ok_or_else(invalid)?;
                    let keys = order.keys(index).ok_or_else(invalid)?;
                    let distinct = keys.iter().collect::<HashSet<_>>();
                    if keys.len() != values.len()
                        || distinct.len() != keys.len()
                        || keys.iter().any(|k| !values.contains_key(k))
                    {
                        return Err(invalid());
                    }
                    // Zod's record parser strips this own property, while
                    // preserving constructor/prototype and nested unknown data.
                    let retained = values
                        .iter()
                        .filter(|(k, _)| k.as_str() != "__proto__")
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<Map<_, _>>();
                    let projected = keys
                        .iter()
                        .filter(|k| k.as_str() != "__proto__")
                        .collect::<Vec<_>>();
                    if projected.len() != retained.len()
                        || projected.iter().any(|k| !retained.contains_key(*k))
                    {
                        return Err(invalid());
                    }
                    Some(
                        projected
                            .into_iter()
                            .map(|k| (k.clone(), retained[k].clone()))
                            .collect(),
                    )
                }
                _ => return Err(invalid()),
            };
            let volume_db = row
                .get("volumeDb")
                .map(|v| {
                    finite(v)
                        .filter(|v| (-100.0..=26.0).contains(v))
                        .ok_or_else(invalid)
                })
                .transpose()?;
            let audio_tracks = row
                .get("audioTracks")
                .map(|v| {
                    v.as_array()
                        .ok_or_else(invalid)?
                        .iter()
                        .map(|v| {
                            finite(v)
                                .filter(|v| v.fract() == 0.0 && (1.0..=6.0).contains(v))
                                .map(|v| v as u8)
                                .ok_or_else(invalid)
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?;
            let fit = row
                .get("fit")
                .map(|v| {
                    if v.as_str() == Some("canvas") {
                        Ok(true)
                    } else {
                        Err(invalid())
                    }
                })
                .transpose()?
                .unwrap_or(false);
            let mut transform = Vec::new();
            if let Some(value) = row.get("transform") {
                let values = value.as_object().ok_or_else(invalid)?;
                for key in TRANSFORM {
                    let Some(value) = values.get(*key) else {
                        continue;
                    };
                    let valid = if *key == "boundsType" {
                        value.as_str().is_some_and(|v| BOUNDS.contains(&v))
                    } else {
                        finite(value).is_some_and(|v| {
                            if key.starts_with("crop") {
                                v.fract() == 0.0 && (0.0..=9_007_199_254_740_991.0).contains(&v)
                            } else if matches!(*key, "alignment" | "boundsAlignment") {
                                v.fract() == 0.0 && v.abs() <= 9_007_199_254_740_991.0
                            } else {
                                true
                            }
                        })
                    };
                    if !valid {
                        return Err(invalid());
                    }
                    transform.push(((*key).into(), value.clone()));
                }
            }
            sources.push(SourceSpec {
                name,
                kind,
                settings,
                visible: boolean(row, "visible")?,
                locked: boolean(row, "locked")?,
                muted: boolean(row, "muted")?,
                volume_db,
                audio_tracks,
                fit,
                transform,
                has_transform: row.contains_key("transform"),
            });
        }
        Ok(Self {
            scene_name,
            sources,
            order: boolean(args, "order")?.unwrap_or(false),
            remove_others: boolean(args, "removeOthers")?.unwrap_or(false),
            apply: boolean(args, "apply")?.unwrap_or(false),
        })
    }
    pub fn scene_name(&self) -> &str {
        &self.scene_name
    }
    pub fn sources(&self) -> &[SourceSpec] {
        &self.sources
    }
    pub fn apply(&self) -> bool {
        self.apply
    }
    pub fn needs_canvas(&self) -> bool {
        self.sources.iter().any(|s| s.fit)
    }
    pub fn arguments(&self) -> Value {
        json!({"sceneName":self.scene_name,"sources":self.sources.iter().map(SourceSpec::arguments).collect::<Vec<_>>(),"order":self.order,"removeOthers":self.remove_others,"apply":self.apply})
    }
}
fn nonempty(v: Option<&Value>) -> Result<String, ObservationError> {
    v.and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .ok_or_else(invalid)
}
fn boolean(row: &Map<String, Value>, key: &str) -> Result<Option<bool>, ObservationError> {
    row.get(key)
        .map(|v| v.as_bool().ok_or_else(invalid))
        .transpose()
}
fn finite(v: &Value) -> Option<f64> {
    v.as_f64().filter(|v| v.is_finite())
}
// Scalar normalization uses the existing Source number contract. A single
// JSON number (or JSON null for an invalid external scalar) fits its bounds;
// this presentation helper does not validate an effect for dispatch.
fn source_number(number: f64) -> Value {
    let mut value = json!(number);
    crate::js_json::normalize_json_numbers(&mut value).expect("bounded JSON scalar");
    value
}
fn object(values: &[(String, Value)]) -> Value {
    Value::Object(values.iter().cloned().collect())
}
#[derive(Clone, Copy)]
pub struct Canvas {
    pub width: f64,
    pub height: f64,
}
#[derive(Clone)]
pub struct Current {
    pub scene_exists: bool,
    pub scene_names: HashSet<String>,
    pub input_kinds: HashMap<String, String>,
    pub items: Vec<ItemState>,
    pub inputs: Vec<InputState>,
    pub video: Option<Canvas>,
}
impl fmt::Debug for Current {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplySceneCurrent")
            .field("items", &self.items.len())
            .field("inputs", &self.inputs.len())
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Create,
    Change,
}
/// Closed effect descriptions only. No raw/vendor/lifecycle/output-control
/// variant exists, and serializing an effect grants no right to send it.
/// ```compile_fail
/// use obs_mcp_native::apply_scene::Effect;
/// let effect = Effect::Raw(serde_json::json!({"requestType":"StopRecord"}));
/// ```
#[derive(Clone, PartialEq)]
pub enum Effect {
    CreateScene {
        scene_name: String,
    },
    CreateInput {
        scene_name: String,
        input_name: String,
        kind: String,
        settings: Value,
        enabled: bool,
    },
    CreateItem {
        scene_name: String,
        input_name: String,
        enabled: bool,
    },
    SettingsOverlay {
        input_name: String,
        settings: Value,
    },
    Mute {
        input_name: String,
        muted: bool,
    },
    VolumeDb {
        input_name: String,
        db: f64,
    },
    AudioTracks {
        input_name: String,
        tracks: Value,
    },
    Enabled {
        scene_name: String,
        id: f64,
        enabled: bool,
    },
    Locked {
        scene_name: String,
        id: f64,
        locked: bool,
    },
    Transform {
        scene_name: String,
        id: f64,
        transform: Value,
    },
    RemoveItem {
        scene_name: String,
        id: f64,
    },
    Index {
        scene_name: String,
        id: f64,
        index: usize,
    },
}
impl fmt::Debug for Effect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplySceneEffect")
            .field("request_type", &self.request_type())
            .finish_non_exhaustive()
    }
}
impl Effect {
    pub fn request_type(&self) -> &'static str {
        match self {
            Self::CreateScene { .. } => "CreateScene",
            Self::CreateInput { .. } => "CreateInput",
            Self::CreateItem { .. } => "CreateSceneItem",
            Self::SettingsOverlay { .. } => "SetInputSettings",
            Self::Mute { .. } => "SetInputMute",
            Self::VolumeDb { .. } => "SetInputVolume",
            Self::AudioTracks { .. } => "SetInputAudioTracks",
            Self::Enabled { .. } => "SetSceneItemEnabled",
            Self::Locked { .. } => "SetSceneItemLocked",
            Self::Transform { .. } => "SetSceneItemTransform",
            Self::RemoveItem { .. } => "RemoveSceneItem",
            Self::Index { .. } => "SetSceneItemIndex",
        }
    }
    pub fn request(&self) -> Value {
        let data = match self {
            Self::CreateScene { scene_name } => json!({"sceneName":scene_name}),
            Self::CreateInput {
                scene_name,
                input_name,
                kind,
                settings,
                enabled,
            } => {
                json!({"sceneName":scene_name,"inputName":input_name,"inputKind":kind,"inputSettings":settings,"sceneItemEnabled":enabled})
            }
            Self::CreateItem {
                scene_name,
                input_name,
                enabled,
            } => json!({"sceneName":scene_name,"sourceName":input_name,"sceneItemEnabled":enabled}),
            Self::SettingsOverlay {
                input_name,
                settings,
            } => json!({"inputName":input_name,"inputSettings":settings,"overlay":true}),
            Self::Mute { input_name, muted } => json!({"inputName":input_name,"inputMuted":muted}),
            Self::VolumeDb { input_name, db } => {
                json!({"inputName":input_name,"inputVolumeDb":source_number(*db)})
            }
            Self::AudioTracks { input_name, tracks } => {
                json!({"inputName":input_name,"inputAudioTracks":tracks})
            }
            Self::Enabled {
                scene_name,
                id,
                enabled,
            } => {
                json!({"sceneName":scene_name,"sceneItemId":source_number(*id),"sceneItemEnabled":enabled})
            }
            Self::Locked {
                scene_name,
                id,
                locked,
            } => {
                json!({"sceneName":scene_name,"sceneItemId":source_number(*id),"sceneItemLocked":locked})
            }
            Self::Transform {
                scene_name,
                id,
                transform,
            } => {
                json!({"sceneName":scene_name,"sceneItemId":source_number(*id),"sceneItemTransform":transform})
            }
            Self::RemoveItem { scene_name, id } => {
                json!({"sceneName":scene_name,"sceneItemId":source_number(*id)})
            }
            Self::Index {
                scene_name,
                id,
                index,
            } => {
                json!({"sceneName":scene_name,"sceneItemId":source_number(*id),"sceneItemIndex":index})
            }
        };
        json!({"requestType":self.request_type(),"requestData":data})
    }
}
#[derive(Clone)]
pub struct Action {
    target: String,
    change: String,
    phase: Phase,
    effect: Option<Effect>,
}
impl fmt::Debug for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplySceneAction")
            .field("phase", &self.phase)
            .field("has_effect", &self.effect.is_some())
            .finish_non_exhaustive()
    }
}
impl Action {
    pub fn target(&self) -> &str {
        &self.target
    }
    pub fn change(&self) -> &str {
        &self.change
    }
    pub fn phase(&self) -> Phase {
        self.phase
    }
    pub fn effect(&self) -> Option<&Effect> {
        self.effect.as_ref()
    }
    pub fn projection(&self) -> Value {
        let mut v = json!({"target":self.target,"change":self.change,"phase":if self.phase==Phase::Create{"create"}else{"change"}});
        if let Some(effect) = &self.effect {
            v["request"] = effect.request();
        }
        v
    }
    fn new(target: &str, change: impl Into<String>, phase: Phase, effect: Option<Effect>) -> Self {
        Self {
            target: target.into(),
            change: change.into(),
            phase,
            effect,
        }
    }
    fn description(&self) -> String {
        format!(
            "- {}: {}{}",
            self.target,
            self.change,
            if self.effect.is_some() {
                ""
            } else {
                " (once it exists)"
            }
        )
    }
    fn listed(&self) -> Value {
        json!({"target":self.target,"change":self.change})
    }
}
#[derive(Clone)]
pub struct Plan {
    actions: Vec<Action>,
    errors: Vec<String>,
}
impl fmt::Debug for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplyScenePlan")
            .field("actions", &self.actions.len())
            .field("errors", &self.errors.len())
            .finish()
    }
}
impl Plan {
    pub fn actions(&self) -> &[Action] {
        &self.actions
    }
    pub fn errors(&self) -> &[String] {
        &self.errors
    }
    pub fn projection(&self) -> Value {
        json!({"actions":self.actions.iter().map(Action::projection).collect::<Vec<_>>(),"errors":self.errors})
    }
    pub fn creations(&self) -> Vec<Action> {
        self.actions
            .iter()
            .filter(|a| a.phase == Phase::Create)
            .cloned()
            .collect()
    }
}

pub fn plan(request: &Request, current: &Current) -> Plan {
    let mut actions = Vec::new();
    let mut errors = Vec::new();
    let scene = &request.scene_name;
    if !current.scene_exists {
        actions.push(Action::new(
            &format!("scene {scene}"),
            "create",
            Phase::Create,
            Some(Effect::CreateScene {
                scene_name: scene.clone(),
            }),
        ));
    }
    let mut seen = HashSet::new();
    for source in &request.sources {
        if !seen.insert(&source.name) {
            errors.push(format!("{} is listed more than once", source.name));
        }
        if current.scene_names.contains(&source.name) || source.name == *scene {
            errors.push(format!(
                "{} is a scene, not an input; nest scenes with obs-create-scene-item",
                source.name
            ));
        }
    }
    for source in &request.sources {
        let name = &source.name;
        let kind = current.input_kinds.get(name);
        let item = current.items.iter().find(|item| item.source_name == *name);
        let input = current
            .inputs
            .iter()
            .find(|input| input.input_name == *name);
        let effect_scene = || scene.clone();
        let effect_name = || name.clone();
        if let Some(kind) = kind {
            if let Some(wanted) = source.kind.as_ref().filter(|s| !s.is_empty())
                && wanted != kind
            {
                errors.push(format!(
                    "{name} is a {kind}, not a {wanted}; choose another name"
                ));
                continue;
            }
            if item.is_none() {
                actions.push(Action::new(
                    name,
                    "add to the scene",
                    Phase::Create,
                    Some(Effect::CreateItem {
                        scene_name: effect_scene(),
                        input_name: effect_name(),
                        enabled: source.visible.unwrap_or(true),
                    }),
                ));
            }
            if let (Some(settings), Some(input)) = (&source.settings, input) {
                let changed = settings
                    .iter()
                    .filter(|(key, value)| {
                        !input
                            .input_settings
                            .get(key)
                            .is_some_and(|old| snapshots::same(value, old))
                    })
                    .map(|(key, _)| key.as_str())
                    .collect::<Vec<_>>();
                if !changed.is_empty() {
                    actions.push(Action::new(
                        name,
                        format!("settings ({})", changed.join(", ")),
                        Phase::Change,
                        Some(Effect::SettingsOverlay {
                            input_name: effect_name(),
                            settings: object(settings),
                        }),
                    ));
                }
            }
        } else {
            let Some(kind) = source.kind.as_ref().filter(|s| !s.is_empty()) else {
                errors.push(format!("{name} does not exist; give its kind to create it"));
                continue;
            };
            actions.push(Action::new(
                name,
                format!(
                    "create {kind}{}",
                    if source.settings.is_some() {
                        " with settings"
                    } else {
                        ""
                    }
                ),
                Phase::Create,
                Some(Effect::CreateInput {
                    scene_name: effect_scene(),
                    input_name: effect_name(),
                    kind: kind.clone(),
                    settings: source
                        .settings
                        .as_ref()
                        .map(|s| object(s))
                        .unwrap_or_else(|| json!({})),
                    enabled: source.visible.unwrap_or(true),
                }),
            ));
        }
        if let Some(muted) = source.muted
            && input.is_none_or(|input| input.muted != Some(muted))
        {
            actions.push(Action::new(
                name,
                if muted { "mute" } else { "unmute" },
                Phase::Change,
                Some(Effect::Mute {
                    input_name: effect_name(),
                    muted,
                }),
            ));
        }
        if let Some(wanted) = source.volume_db {
            let differs = input.is_none_or(|input| {
                let current = input
                    .volume_mul
                    .map(|mul| {
                        if mul > 0.0 {
                            20.0 * mul.log10()
                        } else {
                            -100.0
                        }
                    })
                    .unwrap_or(0.0);
                (current - wanted).abs() > 0.1
            });
            if differs {
                actions.push(Action::new(
                    name,
                    format!("volume {} dB", js_string(Some(&json!(wanted)))),
                    Phase::Change,
                    Some(Effect::VolumeDb {
                        input_name: effect_name(),
                        db: wanted,
                    }),
                ));
            }
        }
        if let Some(tracks) = &source.audio_tracks {
            let wanted = (1u8..=6)
                .map(|track| (track.to_string(), json!(tracks.contains(&track))))
                .collect::<Map<_, _>>();
            let differs = input.is_none_or(|input| {
                (1u8..=6).any(|track| {
                    (input
                        .audio_tracks
                        .as_ref()
                        .and_then(|v| v.get(&track.to_string()))
                        == Some(&Value::Bool(true)))
                        != tracks.contains(&track)
                })
            });
            if differs {
                let label = if tracks.is_empty() {
                    "no audio tracks".into()
                } else {
                    format!(
                        "audio tracks {}",
                        tracks
                            .iter()
                            .map(u8::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                actions.push(Action::new(
                    name,
                    label,
                    Phase::Change,
                    Some(Effect::AudioTracks {
                        input_name: effect_name(),
                        tracks: Value::Object(wanted),
                    }),
                ));
            }
        }
        if let Some(visible) = source.visible
            && item.is_some_and(|item| item.enabled != visible)
        {
            actions.push(Action::new(
                name,
                if visible { "show" } else { "hide" },
                Phase::Change,
                item.map(|item| Effect::Enabled {
                    scene_name: effect_scene(),
                    id: item.scene_item_id,
                    enabled: visible,
                }),
            ));
        }
        if let Some(locked) = source.locked
            && item.is_none_or(|item| item.locked != locked)
        {
            actions.push(Action::new(
                name,
                if locked { "lock" } else { "unlock" },
                Phase::Change,
                item.map(|item| Effect::Locked {
                    scene_name: effect_scene(),
                    id: item.scene_item_id,
                    locked,
                }),
            ));
        }
        let mut transform = if let Some(video) = current.video.filter(|_| source.fit) {
            vec![
                ("boundsType".into(), json!("OBS_BOUNDS_SCALE_INNER")),
                ("boundsWidth".into(), source_number(video.width)),
                ("boundsHeight".into(), source_number(video.height)),
                ("boundsAlignment".into(), json!(0)),
                ("alignment".into(), json!(5)),
                ("positionX".into(), json!(0)),
                ("positionY".into(), json!(0)),
            ]
        } else {
            Vec::new()
        };
        for (key, value) in &source.transform {
            if let Some((_, old)) = transform.iter_mut().find(|(k, _)| k == key) {
                *old = value.clone();
            } else {
                transform.push((key.clone(), value.clone()));
            }
        }
        let changed = transform
            .iter()
            .filter(|(key, value)| {
                item.is_none_or(|item| {
                    !item
                        .transform
                        .get(key)
                        .is_some_and(|old| same_transform_value(key, value, old))
                })
            })
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>();
        if !changed.is_empty() {
            let label = if source.fit && changed.contains(&"boundsType") {
                "fit to the canvas".into()
            } else {
                format!("transform ({})", changed.join(", "))
            };
            actions.push(Action::new(
                name,
                label,
                Phase::Change,
                item.map(|item| Effect::Transform {
                    scene_name: effect_scene(),
                    id: item.scene_item_id,
                    transform: object(&transform),
                }),
            ));
        }
    }
    let wanted = request
        .sources
        .iter()
        .map(|s| s.name.as_str())
        .collect::<HashSet<_>>();
    if request.remove_others {
        for item in current
            .items
            .iter()
            .filter(|item| !wanted.contains(item.source_name.as_str()))
        {
            actions.push(Action::new(
                &item.source_name,
                "remove from the scene",
                Phase::Change,
                Some(Effect::RemoveItem {
                    scene_name: scene.clone(),
                    id: item.scene_item_id,
                }),
            ));
        }
    }
    if request.order {
        let all_exist = request.sources.iter().all(|source| {
            current
                .items
                .iter()
                .any(|item| item.source_name == source.name)
        });
        let mut stack = current.items.iter().collect::<Vec<_>>();
        stack.sort_by(|a, b| {
            a.index
                .partial_cmp(&b.index)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let top = &stack[stack.len().saturating_sub(request.sources.len())..];
        if !all_exist {
            actions.push(Action::new(
                &format!("scene {scene}"),
                "order the sources back to front as listed",
                Phase::Change,
                None,
            ));
        } else if request
            .sources
            .iter()
            .enumerate()
            .any(|(index, s)| top.get(index).is_none_or(|item| item.source_name != s.name))
        {
            let index = if request.remove_others {
                request.sources.len()
            } else {
                current.items.len()
            } - 1;
            for source in &request.sources {
                let item = current
                    .items
                    .iter()
                    .find(|item| item.source_name == source.name)
                    .expect("all sources have items");
                actions.push(Action::new(
                    &source.name,
                    "order (to the front, as listed)",
                    Phase::Change,
                    Some(Effect::Index {
                        scene_name: scene.clone(),
                        id: item.scene_item_id,
                        index,
                    }),
                ));
            }
        }
    }
    Plan { actions, errors }
}

/// Selection description only: capturing/saving remains with PreparationOwners.
#[derive(Clone)]
pub struct SnapshotSelection {
    pub scenes: Vec<String>,
    pub inputs: Vec<String>,
}
impl fmt::Debug for SnapshotSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplySceneSnapshotSelection")
            .field("scenes", &self.scenes.len())
            .field("inputs", &self.inputs.len())
            .finish()
    }
}
pub fn snapshot_selection(request: &Request, current: &Current) -> Option<SnapshotSelection> {
    let mut inputs = Vec::new();
    let mut seen = HashSet::new();
    for name in current
        .items
        .iter()
        .map(|i| &i.source_name)
        .chain(request.sources.iter().map(|s| &s.name))
    {
        if current.input_kinds.contains_key(name) && seen.insert(name) {
            inputs.push(name.clone());
        }
    }
    (current.scene_exists || !inputs.is_empty()).then(|| SnapshotSelection {
        scenes: if current.scene_exists {
            vec![request.scene_name.clone()]
        } else {
            vec![]
        },
        inputs,
    })
}
fn result(text: String) -> Value {
    json!({"content":[{"type":"text","text":text}]})
}
fn error(text: String) -> Value {
    json!({"content":[{"type":"text","text":text}],"isError":true})
}
fn listed(actions: &[Action]) -> Vec<Value> {
    actions.iter().map(Action::listed).collect()
}
fn descriptions(actions: &[Action]) -> Vec<String> {
    actions.iter().map(Action::description).collect()
}
pub fn failure(message: &str) -> Value {
    error(format!("Apply Scene failed: {message}"))
}
pub fn snapshot_failure(message: &str) -> Value {
    error(format!(
        "Nothing changed: could not save a recovery snapshot: {message}"
    ))
}
/// Some is a terminal pure result; None means effects are requested, not allowed.
pub fn initial_result(request: &Request, plan: &Plan) -> Option<Value> {
    if !plan.errors.is_empty() {
        return Some(error(format!(
            "Nothing changed:\n{}",
            plan.errors
                .iter()
                .map(|e| format!("- {e}"))
                .collect::<Vec<_>>()
                .join("\n")
        )));
    }
    if plan.actions.is_empty() {
        let mut v = result(format!("Scene {} already matches", request.scene_name));
        v["structuredContent"] = json!({"actions":[],"applied":false,"converged":true});
        return Some(v);
    }
    if !request.apply {
        let mut lines = vec![format!("To make {} match:", request.scene_name)];
        lines.extend(descriptions(&plan.actions));
        lines.push("Plan only; call again with apply: true".into());
        let mut v = result(lines.join("\n"));
        v["structuredContent"] = json!({"actions":listed(&plan.actions),"applied":false});
        return Some(v);
    }
    None
}
/// Presentation of known statuses only. Missing entries retain Source's skipped
/// wording; the physical owner must reject malformed vectors before this API.
#[derive(Clone, Debug)]
pub struct Status {
    pub ok: bool,
    pub code: u64,
    pub comment: Option<String>,
}
pub fn failures(actions: &[Action], results: &[Status]) -> Vec<String> {
    actions
        .iter()
        .enumerate()
        .filter_map(|(index, action)| {
            let status = results.get(index);
            if status.is_some_and(|s| s.ok) {
                return None;
            }
            let detail = status
                .map(|s| {
                    format!(
                        ": {}",
                        s.comment
                            .clone()
                            .unwrap_or_else(|| format!("code {}", s.code))
                    )
                })
                .unwrap_or_else(|| " (skipped)".into());
            Some(format!(
                "{}: {} failed{detail}",
                action.target, action.change
            ))
        })
        .collect()
}
fn snapshot_note(snapshot: Option<&str>) -> Vec<String> {
    snapshot
        .map(|id| {
            vec![format!(
                "Snapshot {id} holds the previous state of existing sources"
            )]
        })
        .unwrap_or_default()
}
pub fn creation_result(
    creations: &[Action],
    results: &[Status],
    snapshot: Option<&str>,
) -> Option<Value> {
    let problems = failures(creations, results);
    if problems.is_empty() {
        return None;
    }
    let created = creations
        .iter()
        .enumerate()
        .filter(|(index, _)| results.get(*index).is_some_and(|s| s.ok))
        .map(|(_, a)| a.clone())
        .collect::<Vec<_>>();
    let mut lines = vec!["Stopped while creating:".into()];
    lines.extend(problems);
    if !created.is_empty() {
        lines.push("Created before stopping:".into());
        lines.extend(descriptions(&created));
    }
    lines.extend(snapshot_note(snapshot));
    Some(error(lines.join("\n")))
}
/// Corrected Source continuation: a new creation requirement is a refusal,
/// never permission to retry an ACK or recreate an externally removed object.
pub fn continuation(
    second: &Plan,
    creations: &[Action],
    snapshot: Option<&str>,
) -> Result<Vec<Action>, Value> {
    let mut problems = second.errors.clone();
    problems.extend(
        second
            .actions
            .iter()
            .filter(|a| a.phase == Phase::Create)
            .map(|a| {
                format!(
                    "{}: {} is still needed; it was not retried",
                    a.target, a.change
                )
            }),
    );
    if !problems.is_empty() {
        let mut lines = vec!["Stopped before applying changes:".into()];
        lines.extend(problems.iter().map(|e| format!("- {e}")));
        if !creations.is_empty() {
            lines.push("Created before stopping:".into());
            lines.extend(descriptions(creations));
        }
        lines.extend(snapshot_note(snapshot));
        return Err(error(lines.join("\n")));
    }
    Ok(second
        .actions
        .iter()
        .filter(|a| a.phase == Phase::Change && a.effect.is_some())
        .cloned()
        .collect())
}
pub fn final_result(
    request: &Request,
    creations: &[Action],
    changes: &[Action],
    problems: &[String],
    final_plan: &Plan,
    blank: &[String],
    snapshot: Option<&str>,
) -> Value {
    // Source's string count follows its action/result projection; it is not a
    // durable effect counter, postcondition, rendered claim or replay proof.
    let count = creations.len() as i128 + changes.len() as i128 - problems.len() as i128;
    let mut lines = vec![format!(
        "Applied {count} change(s) to {}:",
        request.scene_name
    )];
    lines.extend(descriptions(creations));
    lines.extend(descriptions(changes));
    lines.extend(problems.iter().map(|p| format!("Failed: {p}")));
    if !final_plan.errors.is_empty() {
        lines.push("Could not verify afterwards:".into());
        lines.extend(final_plan.errors.iter().map(|e| format!("- {e}")));
    }
    if !final_plan.actions.is_empty() {
        lines.push("Still different afterwards:".into());
        lines.extend(descriptions(&final_plan.actions));
    } else if final_plan.errors.is_empty() {
        lines.push("OBS now matches the spec".into());
    }
    lines.extend(blank.iter().map(|name| {
        format!(
            "Warning: {name} renders at 0×0; a capture may need a window, display, or permission"
        )
    }));
    if let Some(id) = snapshot {
        lines.push(format!("Saved the previous state as snapshot {id}; obs-restore undoes changes to existing sources"));
    }
    let actions = creations
        .iter()
        .chain(changes)
        .map(Action::listed)
        .collect::<Vec<_>>();
    let mut v = result(lines.join("\n"));
    v["structuredContent"] = json!({"actions":actions,"applied":true,"failures":problems,"converged":final_plan.actions.is_empty()&&final_plan.errors.is_empty(),"remaining":listed(&final_plan.actions),"blank":blank});
    if !final_plan.errors.is_empty() {
        v["structuredContent"]["verificationErrors"] = json!(final_plan.errors);
    }
    if let Some(id) = snapshot {
        v["structuredContent"]["snapshotId"] = json!(id);
    }
    if !problems.is_empty() || !final_plan.actions.is_empty() || !final_plan.errors.is_empty() {
        v["isError"] = json!(true);
    }
    v
}
