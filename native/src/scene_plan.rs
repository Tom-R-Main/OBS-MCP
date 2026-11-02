// SPDX-License-Identifier: GPL-2.0-only
//! Portable flat layout resolution. This owner can only read; it has no
//! mutation, snapshot or persistence interface.
use crate::observation::{
    Observation, ObservationError, ReadAdapter, ReadRequest, Target, flat_scene, identity,
    safe_integer, signature,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;

const TRANSFORMS: &[&str] = &[
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Binding {
    role: String,
    source_name: String,
    source_uuid: String,
    container_uuid: String,
    scene_item_id: u64,
}
#[derive(Clone, Debug)]
struct Role {
    role: String,
    input_kinds: Vec<String>,
    transform: Value,
    enabled: bool,
    locked: bool,
    index: u64,
}
#[derive(Clone, Debug)]
pub struct PlanRequest {
    target: Target,
    width: u64,
    height: u64,
    roles: Vec<Role>,
    bindings: Vec<Binding>,
}

impl PlanRequest {
    /// Mirrors target/template/binding schema admission. Unknown outer fields
    /// are stripped; literal transform fields are strict, as in the source.
    pub fn from_value(value: &Value) -> Result<Self, ObservationError> {
        let invalid =
            || ObservationError::new("evidence_unavailable", "Invalid scene template arguments");
        let object = value.as_object().ok_or_else(invalid)?;
        let scene_collection = identity(&value["sceneCollection"])?.to_owned();
        let scene_name = identity(&value["sceneName"])?.to_owned();
        let canvas_uuid = object
            .get("canvasUuid")
            .map(|canvas| identity(canvas).map(str::to_owned))
            .transpose()?;
        let template = value["template"].as_object().ok_or_else(invalid)?;
        if template.get("version").and_then(Value::as_f64) != Some(1.0) {
            return Err(invalid());
        }
        identity(&value["template"]["name"])?;
        let positive = |value: &Value| {
            safe_integer(value)
                .filter(|number| *number > 0.0)
                .map(|number| number as u64)
                .ok_or_else(invalid)
        };
        let integer = |value: &Value| {
            safe_integer(value)
                .filter(|number| *number >= 0.0)
                .map(|number| number as u64)
                .ok_or_else(invalid)
        };
        let width = positive(&value["template"]["canvas"]["width"])?;
        let height = positive(&value["template"]["canvas"]["height"])?;
        let role_values = value["template"]["roles"]
            .as_array()
            .filter(|roles| !roles.is_empty() && roles.len() <= 64)
            .ok_or_else(invalid)?;
        let mut roles = Vec::with_capacity(role_values.len());
        for role in role_values {
            let name = identity(&role["role"])?.to_owned();
            let input_kinds = role["inputKinds"]
                .as_array()
                .filter(|kinds| !kinds.is_empty() && kinds.len() <= 32)
                .ok_or_else(invalid)?
                .iter()
                .map(|kind| identity(kind).map(str::to_owned))
                .collect::<Result<Vec<_>, _>>()?;
            let transform = role["transform"].as_object().ok_or_else(invalid)?;
            if transform
                .keys()
                .any(|key| !TRANSFORMS.contains(&key.as_str()))
            {
                return Err(invalid());
            }
            for (field, value) in transform {
                let valid = match field.as_str() {
                    "boundsType" => value.as_str().is_some_and(|value| BOUNDS.contains(&value)),
                    "alignment" => safe_integer(value).is_some_and(|value| {
                        [0., 1., 2., 4., 5., 6., 8., 9., 10.].contains(&value)
                    }),
                    "boundsWidth" | "boundsHeight" => value
                        .as_f64()
                        .is_some_and(|value| value.is_finite() && value >= 0.0),
                    "boundsAlignment" => {
                        safe_integer(value).is_some_and(|value| (0.0..=10.0).contains(&value))
                    }
                    "cropLeft" | "cropRight" | "cropTop" | "cropBottom" => {
                        safe_integer(value).is_some_and(|value| value >= 0.0)
                    }
                    _ => value.as_f64().is_some_and(f64::is_finite),
                };
                if !valid {
                    return Err(invalid());
                }
            }
            roles.push(Role {
                role: name,
                input_kinds,
                transform: role["transform"].clone(),
                enabled: role["enabled"].as_bool().ok_or_else(invalid)?,
                locked: role["locked"].as_bool().ok_or_else(invalid)?,
                index: integer(&role["index"])?,
            });
        }
        let binding_values = value["bindings"]
            .as_array()
            .filter(|bindings| bindings.len() <= 64)
            .ok_or_else(invalid)?;
        let mut bindings = Vec::with_capacity(binding_values.len());
        for binding in binding_values {
            bindings.push(Binding {
                role: identity(&binding["role"])?.to_owned(),
                source_name: identity(&binding["sourceName"])?.to_owned(),
                source_uuid: identity(&binding["sourceUuid"])?.to_owned(),
                container_uuid: identity(&binding["containerUuid"])?.to_owned(),
                scene_item_id: integer(&binding["sceneItemId"])?,
            });
        }
        Ok(Self {
            target: Target {
                scene_collection,
                scene_name,
                canvas_uuid,
            },
            width,
            height,
            roles,
            bindings,
        })
    }
    pub fn target(&self) -> &Target {
        &self.target
    }
}

fn blocker(reason: &str, message: impl Into<String>, role: Option<&str>) -> Value {
    let mut result = json!({"reason":reason,"message":message.into(),"nextCheck":"Inspect obs-get-scene-items and provide an exact binding"});
    if let Some(role) = role.filter(|role| !role.is_empty()) {
        result["role"] = json!(role);
    }
    result
}

/// Source-compatible half-pixel positions and f32 transform storage equality.
pub fn same_transform_value(field: &str, a: &Value, b: &Value) -> bool {
    if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
        if matches!(field, "positionX" | "positionY") {
            return half_pixel(a) == half_pixel(b);
        }
        return (a as f32) == (b as f32) || (a - b).abs() < 1e-6;
    }
    same(a, b)
}
fn half_pixel(value: f64) -> f64 {
    let value = value * 2.0;
    // Math.round breaks ties toward +infinity; adding 0.5 first can round an
    // immediately-below-half value incorrectly due to binary precision.
    let floor = value.floor();
    (if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }) / 2.0
}
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            (a.as_f64().unwrap() - b.as_f64().unwrap()).abs() < 1e-6
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, a)| b.get(key).is_some_and(|b| same(a, b)))
        }
        _ => a == b,
    }
}

pub async fn plan_report<R: ReadAdapter>(
    sample: &mut Observation<'_, R>,
    args: &PlanRequest,
    completed_utc: impl FnOnce() -> String,
) -> Value {
    match resolve(sample, args, completed_utc).await {
        Ok(report) => report,
        Err(error) => {
            json!({"version":1,"status":"unknown","applied":false,"changes":[],"blockers":[error.failure()]})
        }
    }
}

async fn resolve<R: ReadAdapter>(
    sample: &mut Observation<'_, R>,
    args: &PlanRequest,
    completed_utc: impl FnOnce() -> String,
) -> Result<Value, ObservationError> {
    let before = flat_scene(sample, &args.target).await?;
    let mut blockers = Vec::new();
    let mut changes = Vec::new();
    let mut resolved = Vec::new();
    if before.video["baseWidth"].as_f64() != Some(args.width as f64)
        || before.video["baseHeight"].as_f64() != Some(args.height as f64)
    {
        blockers.push(blocker(
            "canvas_mismatch",
            "Template canvas dimensions differ from the selected canvas",
            None,
        ));
    }
    let role_names = args
        .roles
        .iter()
        .map(|role| role.role.as_str())
        .collect::<Vec<_>>();
    if role_names.iter().copied().collect::<HashSet<_>>().len() != role_names.len() {
        blockers.push(blocker(
            "duplicate_role",
            "Template role names must be unique",
            None,
        ));
    }
    let indices = args
        .roles
        .iter()
        .map(|role| role.index)
        .collect::<HashSet<_>>();
    if indices.len() != args.roles.len()
        || indices
            .iter()
            .any(|index| *index >= before.items.len() as u64)
    {
        blockers.push(blocker(
            "invalid_order",
            "Role indices must be unique and within this scene's item count",
            None,
        ));
    }
    if args
        .bindings
        .iter()
        .any(|binding| !role_names.contains(&binding.role.as_str()))
    {
        blockers.push(blocker(
            "unknown_role",
            "Binding names a role absent from the template",
            None,
        ));
    }
    let occurrences = args
        .bindings
        .iter()
        .map(|binding| format!("{}:{}", binding.container_uuid, binding.scene_item_id))
        .collect::<HashSet<_>>();
    if occurrences.len() != args.bindings.len() {
        blockers.push(blocker(
            "duplicate_occurrence",
            "An occurrence cannot fill more than one role",
            None,
        ));
    }
    let scene_ids = before.scenes["scenes"]
        .as_array()
        .expect("validated scene list")
        .iter()
        .filter_map(|scene| scene["sceneUuid"].as_str())
        .collect::<HashSet<_>>();
    if before.items.iter().any(|item| {
        item["isGroup"] == true
            || item["sourceUuid"]
                .as_str()
                .is_some_and(|uuid| scene_ids.contains(uuid))
    }) {
        blockers.push(blocker(
            "nested_layout",
            "Flat layouts cannot bind groups or nested scenes",
            None,
        ));
    }
    for role in &args.roles {
        let bindings = args
            .bindings
            .iter()
            .filter(|binding| binding.role == role.role)
            .collect::<Vec<_>>();
        if bindings.len() != 1 {
            blockers.push(blocker(
                if bindings.is_empty() {
                    "missing_binding"
                } else {
                    "duplicate_binding"
                },
                format!("Provide exactly one binding for {}", role.role),
                Some(&role.role),
            ));
            continue;
        }
        let binding = bindings[0];
        if binding.container_uuid != before.selected.scene_uuid {
            blockers.push(blocker(
                "container_mismatch",
                "Binding container differs from selected scene",
                Some(&role.role),
            ));
            continue;
        }
        let item = before
            .items
            .iter()
            .find(|item| item["sceneItemId"].as_f64() == Some(binding.scene_item_id as f64));
        let Some(item) = item.filter(|item| item["sourceUuid"] == binding.source_uuid) else {
            blockers.push(blocker(
                "target_replaced",
                "Bound occurrence or source UUID was removed/replaced",
                Some(&role.role),
            ));
            continue;
        };
        if item["sourceName"] != binding.source_name {
            blockers.push(blocker(
                "target_renamed",
                format!(
                    "Source is now named {}; confirm a fresh binding",
                    item["sourceName"].as_str().expect("validated source name")
                ),
                Some(&role.role),
            ));
            continue;
        }
        let inputs = before
            .inputs
            .iter()
            .filter(|input| {
                input["inputUuid"] == binding.source_uuid
                    && input["inputName"] == binding.source_name
            })
            .collect::<Vec<_>>();
        if inputs.len() != 1
            || !role
                .input_kinds
                .contains(&crate::observation::js_string(inputs[0].get("inputKind")))
        {
            blockers.push(blocker(
                "incompatible_kind",
                "Bound source kind is missing, ambiguous or incompatible",
                Some(&role.role),
            ));
            continue;
        }
        let query =
            json!({"sceneUuid":before.selected.scene_uuid,"sceneItemId":binding.scene_item_id});
        let reply = sample
            .read(ReadRequest::SceneItemTransform, query.clone())
            .await?;
        let current = &reply["sceneItemTransform"];
        if !current.is_object()
            || !item["sceneItemEnabled"].is_boolean()
            || !item["sceneItemLocked"].is_boolean()
            || safe_integer(&item["sceneItemIndex"]).is_none()
        {
            blockers.push(blocker(
                "evidence_unavailable",
                "Transform, order, enabled or lock state is unavailable",
                Some(&role.role),
            ));
            continue;
        }
        let mut resolution = serde_json::to_value(binding).expect("validated binding");
        if let Some(kind) = inputs[0].get("inputKind") {
            resolution["inputKind"] = kind.clone();
        }
        resolved.push(resolution);
        for (field, desired) in [
            ("sceneItemEnabled", json!(role.enabled)),
            ("sceneItemLocked", json!(role.locked)),
            ("sceneItemIndex", json!(role.index)),
        ] {
            let equal = match (item[field].as_f64(), desired.as_f64()) {
                (Some(a), Some(b)) => a == b,
                _ => item[field] == desired,
            };
            if !equal {
                changes.push(json!({"role":role.role,"binding":binding,"field":field,"before":item[field],"after":desired}));
            }
        }
        for field in TRANSFORMS {
            let Some(desired) = role.transform.get(*field) else {
                continue;
            };
            if let Some(value) = current.get(*field) {
                if !same_transform_value(field, value, desired) {
                    changes.push(json!({"role":role.role,"binding":binding,"field":format!("transform.{field}"),"before":value,"after":desired}));
                }
            } else {
                blockers.push(blocker(
                    "evidence_unavailable",
                    format!("Transform {field} is unavailable"),
                    Some(&role.role),
                ));
            }
        }
        let reread = sample.read(ReadRequest::SceneItemTransform, query).await?;
        if signature(current)? != signature(&reread["sceneItemTransform"])? {
            return Err(ObservationError::new(
                "observation_invalidated",
                "Transform changed during resolution",
            ));
        }
    }
    let after = flat_scene(sample, &args.target).await?;
    if signature(&serde_json::to_value(&before).expect("flat scene"))?
        != signature(&serde_json::to_value(&after).expect("flat scene"))?
    {
        return Err(ObservationError::new(
            "observation_invalidated",
            "Selected inventory changed during resolution",
        ));
    }
    sample.check()?;
    let mut target = serde_json::to_value(&args.target).expect("target");
    target["sceneUuid"] = json!(before.selected.scene_uuid);
    Ok(
        json!({"version":1,"status":if blockers.is_empty(){"resolved"}else{"blocked"},"applied":false,"target":target,"generation":sample.generation(),"observedAt":sample.observed_at(),"completedAt":completed_utc(),"resolved":resolved,"changes":changes,"blockers":blockers,"renderedContent":"unverified","nextCheck":if blockers.is_empty(){"Inspect proposed changes; this tool does not apply layouts"}else{"Resolve blockers and plan again"}}),
    )
}
