// SPDX-License-Identifier: GPL-2.0-only
//! Source-generated resource metadata and closed list/completion projections.
//! Resource notifications/subscriptions are separate, unqualified interfaces.
use serde_json::Value;

const MAX_RESULT_BYTES: usize = 4 * 1024 * 1024;

fn component(value: &str) -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
    encoded
}

pub(crate) fn listing(scene_reply: &Value) -> Option<Value> {
    let asset = crate::prompt_catalog::asset();
    let mut result = asset["resourceListing"].clone();
    let resources = result["resources"].as_array_mut()?;
    let scenes = scene_reply.get("scenes").and_then(Value::as_array);
    let mut remaining = MAX_RESULT_BYTES.checked_sub(serde_json::to_vec(resources).ok()?.len())?;
    if scenes.is_some_and(|scenes| scenes.len() > 4096) {
        return None;
    }
    for scene in scenes.into_iter().flatten() {
        let Some(name) = scene.get("sceneName").and_then(Value::as_str) else {
            continue;
        };
        // Selective native admission bounds, not limits imposed on the SDK.
        if name.len() > 64 * 1024 {
            return None;
        }
        let mut resource = asset["sceneItemResource"].clone();
        resource["name"] = Value::String(format!("{name} items"));
        resource["uri"] = Value::String(format!("obs://scene/{}/items", component(name)));
        remaining = remaining.checked_sub(serde_json::to_vec(&resource).ok()?.len() + 1)?;
        resources.push(resource);
    }
    (serde_json::to_vec(&result).ok()?.len() <= MAX_RESULT_BYTES).then_some(result)
}

pub(crate) enum Completion<'a> {
    Reply(Value),
    Missing(String),
    Scenes(&'a str),
}
/// Closed Source completion callbacks. Invalid/extended schemas remain outside
/// this admission surface; context is validated without retaining unused data.
pub(crate) fn completion(params: &Value) -> Option<Completion<'_>> {
    let reference = params.get("ref")?.as_object()?;
    let argument = params.get("argument")?.as_object()?;
    let name = argument.get("name")?.as_str()?;
    let prefix = argument.get("value")?.as_str()?;
    if name.len() > 64 * 1024 || prefix.len() > 64 * 1024 {
        return None;
    }
    if let Some(context) = params.get("context") {
        let context = context.as_object()?;
        if let Some(arguments) = context.get("arguments")
            && !arguments.as_object()?.values().all(Value::is_string)
        {
            return None;
        }
    }
    let empty =
        || Completion::Reply(serde_json::json!({"completion":{"values":[],"hasMore":false}}));
    match reference.get("type")?.as_str()? {
        "ref/prompt" => {
            let prompt = reference.get("name")?.as_str()?;
            if prompt.len() > 64 * 1024 {
                return None;
            }
            let found = crate::prompt_catalog::asset()["listing"]["prompts"]
                .as_array()?
                .iter()
                .any(|entry| entry["name"] == prompt);
            Some(if found {
                empty()
            } else {
                Completion::Missing(format!("Prompt {prompt} not found"))
            })
        }
        "ref/resource" => {
            let uri = reference.get("uri")?.as_str()?;
            if uri.len() > 64 * 1024 {
                return None;
            }
            Some(match uri {
                "obs://scene/{sceneName}/items" if name == "sceneName" => {
                    Completion::Scenes(prefix)
                }
                "obs://scene/{sceneName}/items"
                | "obs://scene/{sceneName}/screenshot"
                | "obs://status"
                | "obs://scenes"
                | "obs://take/current" => empty(),
                _ => Completion::Missing(format!("Resource template {uri} not found")),
            })
        }
        _ => None,
    }
}
pub(crate) fn complete_scenes(scene_reply: &Value, prefix: &str) -> Option<Value> {
    let scenes = scene_reply.get("scenes").and_then(Value::as_array);
    if scenes.is_some_and(|rows| rows.len() > 4096) {
        return None;
    }
    let mut values = Vec::new();
    let mut total = 0;
    for scene in scenes.into_iter().flatten() {
        let Some(name) = scene.get("sceneName").and_then(Value::as_str) else {
            continue;
        };
        if name.len() > 64 * 1024 {
            return None;
        }
        if name.starts_with(prefix) {
            total += 1;
            if values.len() < 100 {
                values.push(name);
            }
        }
    }
    let result =
        serde_json::json!({"completion":{"values":values,"total":total,"hasMore":total>100}});
    (serde_json::to_vec(&result).ok()?.len() <= MAX_RESULT_BYTES).then_some(result)
}
