// SPDX-License-Identifier: GPL-2.0-only
//! Source-shaped obs-observe / obs://status report. The caller supplies the
//! physical connection projection and fresh local metadata; this module owns
//! no socket, writer or task. Agreement is repeated observation, never atomicity.
use crate::{
    custody::Clock,
    observation::{
        EvidenceState, Observation, ObservationError, ReadAdapter, ReadRequest, identity, list,
        safe_integer, signature,
    },
};
use serde_json::{Value, json};
use tokio::{sync::watch, time::Instant};

pub trait LocalState: Send + Sync {
    fn snapshot(&self) -> Value;
    fn connection(&self) -> Value;
}
pub struct Context<'a> {
    pub state: watch::Receiver<EvidenceState>,
    pub generation: u64,
    pub local: &'a dyn LocalState,
    pub clock: Clock,
    pub deadline: Instant,
    /// A failed connection attempt is represented without issuing evidence reads.
    pub connect_error: Option<ObservationError>,
    /// Observe starts before connection/capability admission; sample time starts after.
    pub started: Option<crate::custody::ClockSnapshot>,
}
fn error(reason: &'static str, message: &str) -> ObservationError {
    ObservationError::new(reason, message)
}
fn failure(error: &ObservationError) -> Value {
    if let Some(code) = error.request_code() {
        // Rejection comments are owned public error data, never Debug output.
        let reason = if code == 604 {
            "capability_unavailable"
        } else {
            "evidence_unavailable"
        };
        let mut value = error.failure();
        value["reason"] = json!(reason);
        value
    } else {
        error.failure()
    }
}
async fn optional<R: ReadAdapter>(
    sample: &mut Observation<'_, R>,
    request: ReadRequest,
    data: Value,
    missing: &mut Vec<Value>,
) -> Result<Option<Value>, ObservationError> {
    match sample.read(request, data).await {
        Ok(value) => Ok(Some(value)),
        Err(error) => {
            sample.check()?;
            if error.reason == "observation_limit" {
                return Err(error);
            }
            let mut failed = failure(&error);
            failed["request"] = json!(request.as_str());
            missing.push(failed);
            Ok(None)
        }
    }
}
fn resolve(
    inventory: &[Value],
    name: &Value,
    uuid: Option<&Value>,
) -> Result<Value, ObservationError> {
    let matches = inventory
        .iter()
        .filter(|scene| {
            scene.get("sceneName") == Some(name)
                && uuid.is_none_or(|uuid| scene.get("sceneUuid") == Some(uuid))
        })
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(error(
            "evidence_unavailable",
            "Scene identity is missing or ambiguous",
        ));
    }
    Ok(matches[0].clone())
}
async fn read<R: ReadAdapter>(
    sample: &mut Observation<'_, R>,
    missing: &mut Vec<Value>,
) -> Result<Value, ObservationError> {
    let collection = sample
        .read(ReadRequest::SceneCollectionList, Value::Null)
        .await?;
    let collection = identity(&collection["currentSceneCollectionName"])?;
    let scenes = sample.read(ReadRequest::SceneList, json!({})).await?;
    let inventory=list(&scenes["scenes"],256)?.into_iter().map(|scene|Ok(json!({"sceneName":identity(&scene["sceneName"])? ,"sceneUuid":identity(&scene["sceneUuid"])?}))).collect::<Result<Vec<_>,ObservationError>>()?;
    let program = resolve(
        &inventory,
        &scenes["currentProgramSceneName"],
        scenes.get("currentProgramSceneUuid"),
    )?;
    let studio = optional(sample, ReadRequest::StudioModeEnabled, Value::Null, missing).await?;
    let mut preview = Value::Null;
    if studio
        .as_ref()
        .and_then(|value| value.get("studioModeEnabled"))
        == Some(&Value::Bool(true))
    {
        if let Some(response) =
            optional(sample, ReadRequest::CurrentPreviewScene, json!({}), missing).await?
        {
            preview = resolve(
                &inventory,
                &response["currentPreviewSceneName"],
                response.get("currentPreviewSceneUuid"),
            )?;
        }
    } else if studio
        .as_ref()
        .and_then(|value| value.get("studioModeEnabled"))
        != Some(&Value::Bool(false))
    {
        missing.push(json!({"request":"GetStudioModeEnabled","reason":"evidence_unavailable","nextCheck":"Inspect Studio Mode"}));
    }
    let video = sample.read(ReadRequest::VideoSettings, json!({})).await?;
    if ["baseWidth", "baseHeight"]
        .iter()
        .any(|field| safe_integer(&video[*field]).is_none_or(|number| number <= 0.0))
    {
        return Err(error(
            "evidence_unavailable",
            "Canvas dimensions are unavailable",
        ));
    }
    let mut outputs = serde_json::Map::new();
    for (name, request) in [
        ("record", ReadRequest::RecordStatus),
        ("stream", ReadRequest::StreamStatus),
        ("replayBuffer", ReadRequest::ReplayBufferStatus),
        ("virtualCamera", ReadRequest::VirtualCamStatus),
    ] {
        let response = optional(sample, request, Value::Null, missing).await?;
        let mut value = Value::Null;
        if let Some(response) = response {
            if let Some(active) = response["outputActive"].as_bool() {
                value = json!({"outputActive":active});
                for field in ["outputPaused", "outputReconnecting"] {
                    if let Some(boolean) = response[field].as_bool() {
                        value[field] = json!(boolean);
                    }
                }
            } else {
                missing.push(json!({"request":request.as_str(),"reason":"evidence_unavailable","nextCheck":format!("Inspect {}",request.as_str())}));
            }
        }
        outputs.insert(name.into(), value);
    }
    let other = optional(sample, ReadRequest::OutputList, Value::Null, missing).await?;
    let mut named = vec![];
    if let Some(other) = other {
        for entry in list(&other["outputs"], 32)? {
            let name = identity(&entry["outputName"])?;
            let response = optional(
                sample,
                ReadRequest::OutputStatus,
                json!({"outputName":name}),
                missing,
            )
            .await?;
            let active = response
                .as_ref()
                .and_then(|response| response["outputActive"].as_bool());
            let reconnecting = response
                .as_ref()
                .and_then(|response| response["outputReconnecting"].as_bool());
            named.push(
                json!({"outputName":name,"outputActive":active,"outputReconnecting":reconnecting}),
            );
            if active.is_none() || reconnecting.is_none() {
                missing.push(json!({"request":"GetOutputStatus","outputName":name,"reason":"evidence_unavailable","nextCheck":"Inspect this output before changing production"}));
            }
        }
    }
    Ok(
        json!({"collection":collection,"canvas":{"selection":"default","baseWidth":video["baseWidth"],"baseHeight":video["baseHeight"]},"program":program,"preview":preview,"studioModeEnabled":studio.as_ref().and_then(|value|value.get("studioModeEnabled")).unwrap_or(&Value::Null),"outputs":outputs,"namedOutputs":named}),
    )
}

pub async fn observe<R: ReadAdapter>(
    reader: &mut R,
    context: Context<'_>,
    canvas_uuid: Option<&str>,
) -> Value {
    let started = context.started.clone().unwrap_or_else(|| (context.clock)());
    let observed = (context.clock)();
    let mut sample = match context.connect_error.clone().map_or_else(
        || {
            Observation::new(
                reader,
                context.state,
                context.generation,
                observed.utc,
                context.deadline,
            )
        },
        Err,
    ) {
        Ok(sample) => sample,
        Err(error) => return failed_report(&error, context.local, &context.clock, &started, 0),
    };
    let result=async {
        if canvas_uuid.is_some() {return Err(error("capability_unavailable","obs-observe currently requires the default canvas; the video-settings contract is not canvas-scoped. Use obs-audit-scene for a named canvas structural audit"));}
        let mut missing=vec![];
        let before_local=context.local.snapshot();
        let before=read(&mut sample,&mut missing).await?;
        let after=read(&mut sample,&mut missing).await?;
        let after_local=context.local.snapshot();
        if signature(&before)?!=signature(&after)? || signature(&before_local)?!=signature(&after_local)? {return Err(error("observation_invalidated","Relevant production state changed within the observation interval"));}
        sample.check()?;
        if after_local["recovery"]["status"]=="unknown" {
            let mut item=json!({"component":"recovery","reason":"evidence_unavailable","nextCheck":"Inspect local recovery journals before preparing more changes"});
            if let Some(reason)=after_local["recovery"].get("reason") {item["message"]=reason.clone();}
            missing.push(item);
        }
        let mut unique=vec![];let mut signatures=std::collections::HashSet::new();
        for item in missing {if signatures.insert(signature(&item)?){unique.push(item);}}
        let record=&after["outputs"]["record"];
        let status=if record.is_null(){"unknown"}else if record["outputActive"]==true{"active"}else{"idle"};
        let program=after["program"]["sceneName"].as_str().unwrap_or("");
        let preview=if let Some(name)=after["preview"]["sceneName"].as_str(){format!("“{name}” is in preview")}else{"no preview was observed".into()};
        let completed=(context.clock)();
        let mut report=json!({"version":1,"status":if unique.is_empty(){"observed"}else{"incomplete"},"summary":format!("Recording is {status}. “{program}” is on program; {preview}."),"connection":context.local.connection(),"generation":sample.generation(),"record":record,"stream":after["outputs"]["stream"],"currentProgramSceneName":program,"observedAt":sample.observed_at(),"completedAt":completed.utc,"durationMs":i128::from(completed.epoch_ms)-i128::from(started.epoch_ms),"requests":sample.requests(),"missingEvidence":unique,"availableActions":["obs-audit-scene","obs-plan-scene-template"],"limitations":["Repeated observations agree; OBS does not provide an atomic snapshot.","Rendered composition and recording readiness require separate checks.","Control lease describes cooperating MCP clients; the OBS operator can still edit."]});
        if after["studioModeEnabled"]==true {report["availableActions"].as_array_mut().unwrap().push(json!("obs-stage-scene"));}
        report.as_object_mut().unwrap().extend(after.as_object().unwrap().clone());
        report.as_object_mut().unwrap().extend(after_local.as_object().ok_or_else(||error("evidence_unavailable","Local production metadata is unavailable"))?.clone());
        Ok::<Value,ObservationError>(report)
    }.await;
    let requests = sample.requests();
    sample.close();
    result.unwrap_or_else(|error| {
        failed_report(&error, context.local, &context.clock, &started, requests)
    })
}
fn failed_report(
    error: &ObservationError,
    local: &dyn LocalState,
    clock: &Clock,
    started: &crate::custody::ClockSnapshot,
    requests: usize,
) -> Value {
    let completed = clock();
    let mut report = json!({"version":1,"status":"unknown","connection":local.connection(),"operationReceipts":local.snapshot()["operationReceipts"],"observedAt":started.utc,"completedAt":completed.utc,"durationMs":i128::from(completed.epoch_ms)-i128::from(started.epoch_ms),"requests":requests,"availableActions":["obs-observe"]});
    report
        .as_object_mut()
        .unwrap()
        .extend(failure(error).as_object().unwrap().clone());
    report
}
