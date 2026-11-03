// SPDX-License-Identifier: GPL-2.0-only
//! One retained save attempt. Transport fate and file evidence remain separate.
//! OBS save events carry no request ID; even a verified file is not attributed
//! exclusively to this request, and no failure authorizes replay.
use crate::{
    custody::{Clock, SharedControl},
    generic_requests::{self, GenericServices},
    operation_store::OperationStore,
    replay_events::ReplayEvents,
    replay_files::Verifier,
    session::SessionHandle,
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    fs::{self, Metadata, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct Request {
    pub timeout: Duration,
}
impl Request {
    pub fn parse(value: &Value) -> Option<Self> {
        value.as_object()?;
        let seconds = value
            .get("timeoutSeconds")
            .map_or(Some(15), Value::as_u64)?;
        (1..=50).contains(&seconds).then_some(Self {
            timeout: Duration::from_secs(seconds),
        })
    }
}
#[cfg(unix)]
fn same(a: &Metadata, b: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}
#[cfg(not(unix))]
fn same(_: &Metadata, _: &Metadata) -> bool {
    false
}
fn metadata(path: &Path) -> Result<Metadata, String> {
    fs::symlink_metadata(path)
        .map_err(|_| "Replay verification journal identity is unavailable".into())
}
struct Journal {
    root: PathBuf,
    root_identity: Metadata,
    dir: PathBuf,
    dir_identity: Metadata,
    active: PathBuf,
    active_identity: Metadata,
    id: String,
    endpoint: String,
}
fn valid_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}
fn read_record(path: &Path) -> Result<Value, String> {
    let before = metadata(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before.nlink() != 1 {
            return Err("Replay journal cannot share a hardlink".into());
        }
    }
    if !before.is_file() || before.file_type().is_symlink() || before.len() > 32 * 1024 {
        return Err("Replay verification history is unsafe".into());
    }
    let file = fs::File::open(path).map_err(|_| "Replay verification history is unreadable")?;
    if !same(
        &before,
        &file
            .metadata()
            .map_err(|_| "Replay journal descriptor unavailable")?,
    ) {
        return Err("Replay history changed before read".into());
    }
    let mut bytes = Vec::new();
    file.take(32 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "Replay journal read failed")?;
    if bytes.len() > 32 * 1024 || !same(&before, &metadata(path)?) {
        return Err("Replay verification history changed or exceeds its limit".into());
    }
    let record: Value =
        serde_json::from_slice(&bytes).map_err(|_| "Replay verification history is corrupt")?;
    if record["version"] != 1
        || record["kind"] != "replay-verification"
        || !record["operationId"].as_str().is_some_and(valid_id)
        || !record["endpointHash"].is_string()
        || record["requestAttribution"] != "unverified"
    {
        return Err("Replay verification history is invalid".into());
    }
    Ok(record)
}
impl Journal {
    fn begin(root: &Path, endpoint: &str, id: &str) -> Result<Self, String> {
        if !valid_id(id) {
            return Err("Invalid replay operation identity".into());
        }
        let root_identity = metadata(root)?;
        let dir = root.join("replay-verifications");
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err("Replay verification journal cannot be created".into()),
        }
        let dir_identity = metadata(&dir)?;
        if !dir_identity.is_dir()
            || dir_identity.file_type().is_symlink()
            || dir.canonicalize().ok().as_deref() != Some(dir.as_path())
        {
            return Err("Replay verification journal namespace is unsafe".into());
        }
        let mut count = 0;
        for entry in fs::read_dir(&dir).map_err(|_| "Replay verification history is unavailable")? {
            count += 1;
            if count > 1024 {
                return Err("Replay verification history capacity reached".into());
            }
            let path = entry
                .map_err(|_| "Replay verification history is incomplete")?
                .path();
            if path.file_name().is_some_and(|name| name == "active") {
                return Err("An unfinished replay verification is retained; inspect its journal, do not replay the save".into());
            }
            let record = read_record(&path)?;
            let operation = record["operationId"].as_str().unwrap();
            let started = path.file_name().and_then(|s| s.to_str())
                == Some(format!("{operation}.started.json").as_str());
            let terminal_name = path.file_name().and_then(|s| s.to_str())
                == Some(format!("{operation}.result.json").as_str());
            if (!started && !terminal_name)
                || (started && record["status"] != "pending")
                || (terminal_name
                    && record["status"] != "verified"
                    && record["status"] != "unverified")
            {
                return Err("Replay journal filename or status is invalid".into());
            }
            if path.to_string_lossy().ends_with(".started.json")
                && record["endpointHash"] == endpoint
            {
                let terminal = dir.join(format!(
                    "{}.result.json",
                    record["operationId"].as_str().unwrap()
                ));
                let terminal=read_record(&terminal).map_err(|_|"Unfinished or corrupt replay file evidence blocks another verified save; no replay was dispatched")?;
                if terminal["operationId"] != record["operationId"]
                    || terminal["endpointHash"] != record["endpointHash"]
                    || !matches!(terminal["status"].as_str(), Some("verified" | "unverified"))
                {
                    return Err(
                        "Replay terminal evidence does not match its original attempt".into(),
                    );
                }
            }
        }
        if count + 3 > 1024 {
            return Err("Replay verification history lacks capacity for a complete attempt".into());
        }
        let active = dir.join("active");
        builder.create(&active).map_err(|_|"Another replay verification owns this namespace or an interrupted owner is retained")?;
        let active_identity = metadata(&active)?;
        let this = Self {
            root: root.to_owned(),
            root_identity,
            dir,
            dir_identity,
            active,
            active_identity,
            id: id.into(),
            endpoint: endpoint.into(),
        };
        this.write(
            "started",
            &json!({"status":"pending","requestAttribution":"unverified"}),
        )?;
        Ok(this)
    }
    fn check(&self) -> Result<(), String> {
        for (path, before) in [
            (&self.root, &self.root_identity),
            (&self.dir, &self.dir_identity),
            (&self.active, &self.active_identity),
        ] {
            let info = metadata(path)?;
            if !same(before, &info) || !info.is_dir() || info.file_type().is_symlink() {
                return Err("Replay journal namespace identity changed".into());
            }
        }
        Ok(())
    }
    fn write(&self, phase: &str, result: &Value) -> Result<(), String> {
        self.check()?;
        let mut body = result.clone();
        body["version"] = json!(1);
        body["kind"] = json!("replay-verification");
        body["operationId"] = json!(self.id);
        body["endpointHash"] = json!(self.endpoint);
        body["requestAttribution"] = json!("unverified");
        let data = serde_json::to_vec(&body).map_err(|_| "Replay journal serialization failed")?;
        if data.len() > 32 * 1024 {
            return Err("Replay journal result exceeds its byte limit".into());
        }
        let path = self.dir.join(format!("{}.{}.json", self.id, phase));
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(path)
            .map_err(|_| "Replay journal publication refused")?;
        file.write_all(&data)
            .and_then(|_| file.sync_all())
            .map_err(|_| "Replay journal publication is uncertain")?;
        fs::File::open(&self.dir)
            .and_then(|file| file.sync_all())
            .map_err(|_| "Replay journal directory sync failed")?;
        self.check()
    }
    fn complete(self, result: &Value) -> Result<(), String> {
        self.write("result", result)?;
        self.check()?;
        fs::remove_dir(&self.active).map_err(|_| "Replay active journal cleanup is unconfirmed")?;
        fs::File::open(&self.dir)
            .and_then(|file| file.sync_all())
            .map_err(|_| "Replay journal completion sync is uncertain".to_owned())
    }
}
pub struct Outcome {
    pub result: Value,
    pub cleanup_unconfirmed: bool,
}
fn result(facts: Value, issue: Option<String>) -> Outcome {
    let text=issue.unwrap_or_else(||"A new local replay file was observed and verified during this save attempt. OBS supplies no request identity for saved events; attribution remains unverified.".into());
    let failed = facts["status"] != "verified";
    Outcome {
        result: json!({"content":[{"type":"text","text":text}],"structuredContent":facts,"isError":failed}),
        cleanup_unconfirmed: false,
    }
}
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    writer: OperationStore,
    request: Request,
    session: SessionHandle,
    events: ReplayEvents,
    verifier: Arc<Verifier>,
    control: SharedControl,
    clock: Clock,
    caller: String,
    cancel: Cancellation,
    id: String,
    admitted: Instant,
) -> Outcome {
    let view = writer.read_view();
    let root = view.state_root().to_owned();
    let deadline = admitted + request.timeout;
    let mut facts = json!({"status":"unverified","operationId":id,"requestAccepted":false,"saveEventObserved":false,"newPathObserved":false,"fileStableDuringVerification":false,"strictDecode":false,"requestAttribution":"unverified"});
    if cancel.is_cancelled() || Instant::now() >= deadline {
        return result(
            facts,
            Some("Replay operation cancelled or expired before admission".into()),
        );
    }
    let endpoint = session.endpoint_hash().to_owned();
    let operation = id.clone();
    let journal =
        match tokio::task::spawn_blocking(move || Journal::begin(&root, &endpoint, &operation))
            .await
        {
            Ok(Ok(journal)) => journal,
            Ok(Err(issue)) => return result(facts, Some(issue)),
            Err(_) => {
                return result(
                    facts,
                    Some("Replay journal admission worker did not join".into()),
                );
            }
        };
    let mut cleanup_unconfirmed = false;
    // Retain explicit cleanup custody across final OBS/event readback awaits.
    let mut verified_snapshot = None;
    let workflow=async {
        if cancel.is_cancelled()||Instant::now()>=deadline{return Err("Replay operation cancelled or original deadline expired before dispatch".into());}
        let read=|name:&'static str|{let session=session.clone();async move {let remaining=deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));if remaining.is_zero(){return Err("Replay original deadline expired".to_owned());}session.request(session.token(),name,Value::Null,remaining).await.map_err(|e|format!("Replay prerequisite/readback {name} failed: {e}"))}};
        if read("GetReplayBufferStatus").await?["outputActive"]!=true{return Err("Replay buffer must already be active; this workflow never starts it".into());}
        let previous=read("GetLastReplayBufferReplay").await?;
        let previous=previous["savedReplayPath"].as_str().filter(|s|s.len()<=4096).ok_or("Previous replay path is unavailable")?.to_owned();
        let baseline=verifier.baseline(deadline,cancel.clone()).await?;
        let subscription=events.subscribe(deadline)?;
        let arm=subscription.clone();
        let save=generic_requests::Call::parse("obs-save-replay-buffer",&json!({})).expect("fixed replay save arguments");
        let save_admitted=Instant::now();
        let remaining=deadline.saturating_duration_since(save_admitted).min(Duration::from_secs(10));
        if remaining.is_zero(){return Err("Replay original deadline expired before save admission".into());}
        let _transport=generic_requests::execute_with_dispatch_hook(writer,save,session.clone(),control,clock,caller,cancel.clone(),id.clone(),remaining,save_admitted,GenericServices::default(),Some(Arc::new(move||arm.arm()))).await;
        facts["saveEventObserved"]=json!(subscription.observed());
        let receipt=view.read_any(&id).map_err(|_|"Replay transport receipt could not be inspected")?;
        facts["requestAccepted"]=receipt["requestAccepted"].clone();
        if facts["requestAccepted"]!=true{return Err("Replay save was rejected, not dispatched, or has an Unknown acknowledgement; no retry or verified file is granted".into());}
        let path=tokio::select!{path=subscription.candidate()=>path?,_=cancel.cancelled()=>return Err("Replay verification cancelled after save dispatch; its acknowledgement remains retained".into())};facts["saveEventObserved"]=json!(true);
        if path==previous{return Err("Replay saved path did not change from the previous replay; freshness is unverified".into());}
        let snapshot=match verifier.snapshot(PathBuf::from(&path),baseline,id.clone(),deadline,cancel.clone()).await{Ok(snapshot)=>snapshot,Err(error)=>{cleanup_unconfirmed|=error.cleanup_unconfirmed;return Err(error.message);}};
        facts["newPathObserved"]=json!(true);
        verified_snapshot=Some(match verifier.verify(snapshot,deadline,cancel.clone()).await{Ok(file)=>file,Err(error)=>{cleanup_unconfirmed|=error.cleanup_unconfirmed;return Err(error.message);}});
        facts["strictDecode"]=json!(true);
        if read("GetLastReplayBufferReplay").await?["savedReplayPath"]!=path{return Err("Current replay filename disagrees with the saved event".into());}
        subscription.confirm(&path).await?;
        let file=match verifier.finish(verified_snapshot.take().expect("retained verified snapshot"),deadline,cancel.clone()).await{Ok(file)=>file,Err(error)=>{cleanup_unconfirmed|=error.cleanup_unconfirmed;return Err(error.message);}};
        facts["file"]=file;facts["fileStableDuringVerification"]=json!(true);facts["status"]=json!("verified");
        Ok::<(),String>(())
    }.await;
    let mut issue = workflow.err();
    if let Some(snapshot) = verified_snapshot.take()
        && let Err(cleanup) = verifier.discard(snapshot)
    {
        cleanup_unconfirmed |= cleanup.cleanup_unconfirmed;
        issue = Some(match issue {
            Some(primary) => format!("{primary}; {}", cleanup.message),
            None => cleanup.message,
        });
    }
    if cleanup_unconfirmed {
        let mut outcome = result(
            facts,
            Some(format!(
                "{}; private snapshot and unfinished verification journal retained because cleanup is unconfirmed",
                issue.unwrap_or_else(|| "Replay helper cleanup is unconfirmed".into())
            )),
        );
        outcome.cleanup_unconfirmed = true;
        return outcome;
    }
    let recorded = facts.clone();
    match tokio::task::spawn_blocking(move || journal.complete(&recorded)).await {
        Ok(Ok(())) => {}
        outcome => {
            facts["status"] = json!("unverified");
            facts.as_object_mut().unwrap().remove("file");
            return result(
                facts,
                Some(match outcome {
                    Ok(Err(error)) => error,
                    _ => "Replay journal completion worker did not join".into(),
                }),
            );
        }
    }
    result(facts, issue)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scratch() -> PathBuf {
        let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("target");
        fs::create_dir_all(&parent).unwrap();
        let path = parent.join(format!(
            "replay-journal-{}",
            crate::runtime_identity::uuid_v4().unwrap()
        ));
        fs::create_dir(&path).unwrap();
        path
    }
    #[test]
    fn interrupted_verification_cannot_be_reentered_or_replayed() {
        let root = scratch();
        let id = crate::runtime_identity::uuid_v4().unwrap();
        let journal = Journal::begin(&root, "endpoint-a", &id).unwrap();
        drop(journal);
        assert!(
            Journal::begin(
                &root,
                "endpoint-a",
                &crate::runtime_identity::uuid_v4().unwrap()
            )
            .is_err()
        );
        let path = root
            .join("replay-verifications")
            .join(format!("{id}.started.json"));
        let before = fs::read(&path).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&before).unwrap()["status"],
            "pending"
        );
        assert_eq!(fs::read(&path).unwrap(), before);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn completed_file_facts_do_not_promote_transport_fate_and_old_journals_survive() {
        let root = scratch();
        let id = crate::runtime_identity::uuid_v4().unwrap();
        let journal = Journal::begin(&root, "endpoint-a", &id).unwrap();
        journal.complete(&json!({"status":"unverified","requestAccepted":true,"requestAttribution":"unverified"})).unwrap();
        let terminal = root
            .join("replay-verifications")
            .join(format!("{id}.result.json"));
        let before = fs::read(&terminal).unwrap();
        assert!(!root.join("replay-verifications/active").exists());
        let next = Journal::begin(
            &root,
            "endpoint-a",
            &crate::runtime_identity::uuid_v4().unwrap(),
        )
        .unwrap();
        next.complete(&json!({"status":"unverified","requestAccepted":false}))
            .unwrap();
        assert_eq!(fs::read(&terminal).unwrap(), before);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn original_budget_parser_never_accepts_fractional_zero_or_unbounded_time() {
        assert_eq!(
            Request::parse(&json!({})).unwrap().timeout,
            Duration::from_secs(15)
        );
        for value in [
            json!({"timeoutSeconds":0}),
            json!({"timeoutSeconds":51}),
            json!({"timeoutSeconds":1.5}),
            json!({"timeoutSeconds":null}),
        ] {
            assert!(Request::parse(&value).is_none());
        }
    }
}
