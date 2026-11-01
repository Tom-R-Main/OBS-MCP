// SPDX-License-Identifier: GPL-2.0-only
//! Explicit qualification lane. There is no SDK/compatibility entry point in
//! this module: unsupported features retain the persistent owner's state.
use crate::{
    compat::Frames,
    protocol::{Admission, OwnedReplyContext, ProtocolSession},
    schema,
    tool_filter::ToolFilter,
};
use obs_mcp_native::{
    clock, config,
    control::{CallerContext, ControlDomain, GateRequest, caller_name},
    custody::{Clock, SharedControl},
    demo::Selection,
    demo_transport::{DemoAction, DemoPolicy},
    media_tools,
    media_verification::Cancellation as MediaCancellation,
    observation::{ReadRequest, Target},
    operation_store::OperationStore,
    property_list, record_clip, restore_workflow,
    scene_audit::ScreenshotLimit,
    service::{
        ConnectionDescriptor, Options, PreparationOwners, Resources, Service, ServiceError,
        TakeServices,
    },
    service_read_tools::{self, ReadToolRequest, StatusMetadata},
    session::{ObsSession, SessionOptions},
    session_observation::ObservationBridge,
    snapshot_tools,
    stage_transport::{CallerBinding, Cancellation},
    take_transport::{Bounds, LocalHost},
    takes::{Limits, Policy, StartOptions},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs, io,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{io::BufReader, task::JoinSet};

fn error(text: impl Into<String>) -> Value {
    json!({"content":[{"type":"text","text":text.into()}],"isError":true})
}
fn service_error(reason: ServiceError) -> Value {
    error(format!(
        "Native persistent capability unavailable ({reason:?}); native state is retained. No compatibility runtime was started"
    ))
}
fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
// Mirrors the actual registry's withStructuredToolResults wrapper.
fn ensure_structured(mut result: Value) -> Value {
    if result["isError"] == true || result.get("structuredContent").is_some() {
        return result;
    }
    let text = result["content"]
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter(|block| block["type"] == "text")
                .filter_map(|block| block["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    result["structuredContent"] = if text.is_empty() {
        json!({})
    } else {
        match serde_json::from_str::<Value>(&text) {
            Ok(value) if value.is_object() => value,
            Ok(value) => json!({"result":value}),
            Err(_) => json!({"message":text}),
        }
    };
    result
}
fn no_take() -> Value {
    error("No take is recording; start one with obs-take-start")
}
fn read_kind(name: &str) -> Option<ReadRequest> {
    ReadRequest::from_name(name)
}

fn handler(name: &str, takes: bool, media: bool) -> Option<&'static str> {
    if (takes || media)
        && matches!(
            name,
            "obs-start-stream" | "obs-stop-stream" | "obs-toggle-stream"
        )
    {
        return None;
    }
    let handler = schema::service_handler(name)?;
    let expected = match name {
        "obs-observe" => "observe-production",
        "obs-stop-record" => "manual-record-stop",
        "obs-start-stream" | "obs-start-virtual-cam" | "obs-start-replay-buffer" => {
            "confirmed-output-start"
        }
        "obs-create-scene"
        | "obs-remove-scene"
        | "obs-remove-scene-item"
        | "obs-remove-source-filter"
        | "obs-set-source-filter-name"
        | "obs-set-source-filter-index"
        | "obs-remove-input"
        | "obs-set-input-name"
        | "obs-duplicate-scene-item"
        | "obs-set-scene-name"
        | "obs-create-scene-item"
        | "obs-set-scene-item-enabled"
        | "obs-set-current-scene"
        | "obs-set-preview-scene"
        | "obs-trigger-studio-transition"
        | "obs-set-current-transition"
        | "obs-trigger-transition"
        | "obs-set-scene-transition-override"
        | "obs-set-tbar-position"
        | "obs-set-transition-duration"
        | "obs-set-input-mute"
        | "obs-set-input-audio-balance"
        | "obs-set-input-audio-sync-offset"
        | "obs-set-input-audio-monitor-type"
        | "obs-set-input-deinterlace-field-order"
        | "obs-set-input-deinterlace-mode"
        | "obs-set-scene-item-index"
        | "obs-set-scene-item-locked"
        | "obs-set-scene-item-blend-mode"
        | "obs-set-source-filter-enabled"
        | "obs-set-media-input-cursor"
        | "obs-offset-media-input-cursor"
        | "obs-trigger-media-input-action"
        | "obs-set-input-audio-tracks"
        | "obs-toggle-input-mute"
        | "obs-set-input-volume"
        | "obs-send-stream-caption"
        | "obs-save-replay-buffer"
        | "obs-set-persistent-data"
        | "obs-set-current-scene-collection"
        | "obs-create-scene-collection"
        | "obs-set-current-profile"
        | "obs-create-profile"
        | "obs-remove-profile"
        | "obs-set-stream-service-settings"
        | "obs-set-record-directory"
        | "obs-set-output-settings"
        | "obs-set-video-settings"
        | "obs-set-profile-parameter"
        | "obs-create-input"
        | "obs-set-input-settings"
        | "obs-broadcast-custom-event"
        | "obs-set-transition-settings"
        | "obs-create-source-filter"
        | "obs-set-source-filter-settings"
        | "obs-press-input-property-button"
        | "obs-save-source-screenshot"
        | "obs-set-scene-item-transform"
        | "obs-pause-record"
        | "obs-resume-record"
        | "obs-toggle-record-pause"
        | "obs-split-record-file"
        | "obs-create-record-chapter"
        | "obs-toggle-stream"
        | "obs-toggle-virtual-cam"
        | "obs-toggle-replay-buffer"
        | "obs-stop-stream"
        | "obs-stop-virtual-cam"
        | "obs-stop-replay-buffer"
        | "obs-set-studio-mode"
        | "obs-open-input-properties"
        | "obs-open-input-filters"
        | "obs-open-input-interact"
        | "obs-open-video-mix-projector"
        | "obs-open-source-projector" => "literal-setter",
        "obs-get-status" => "get-status",
        "obs-test-connection" => "test-connection",
        "obs-sleep" => "sleep",
        "obs-get-input-list" => "input-list",
        "obs-get-input-kind-list" => "input-kind-list",
        "obs-get-source-screenshot" => "source-screenshot",
        "obs-preflight" => "preflight",
        "obs-snapshot" => "snapshot",
        "obs-restore" => "restore",
        "obs-get-input-property-list-items" => "property-list",
        "obs-stage-scene" => "stage-scene",
        "obs-take-scene" => "take-scene",
        "obs-record-clip" if takes => "record-clip",
        "obs-take-start" if takes => "take-start",
        "obs-take-stop" if takes => "take-stop",
        "obs-take-mark" if takes => "take-mark",
        "obs-take-status" if takes => "take-status",
        "obs-claim-control" => "claim-control",
        "obs-release-control" => "release-control",
        "obs-control-status" => "control-status",
        "obs-prepare-demo" => "prepare-demo",
        "obs-recover-demo" => "recover-demo",
        "obs-plan-scene-template" => "scene-template-plan",
        "obs-audit-scene" => "scene-audit",
        "obs-contact-sheet" if media => "contact-sheet",
        "obs-trim-take" if media => "trim-take",
        _ => return None,
    };
    (handler == expected).then_some(handler)
}
// Clip's closed parser owns the Source positive/fractional duration and
// chapter contract. The transitional schema parser intentionally does not
// admit exclusiveMinimum or normalize object-array items generically.
fn validated_clip_arguments(mut args: Value) -> Option<Value> {
    record_clip::Request::parse(&args)?;
    let fields = args.as_object_mut()?;
    fields.retain(|name, _| {
        matches!(
            name.as_str(),
            "durationSeconds" | "chapters" | "expectSilent"
        )
    });
    fields.entry("chapters").or_insert_with(|| json!([]));
    fields.entry("expectSilent").or_insert_with(|| json!(false));
    for chapter in fields.get_mut("chapters")?.as_array_mut()? {
        chapter
            .as_object_mut()?
            .retain(|name, _| matches!(name.as_str(), "atSeconds" | "name"));
    }
    Some(args)
}
fn read_definition(name: &str) -> Option<ReadRequest> {
    read_kind(schema::catalog()["requests"][name]["requestType"].as_str()?)
}
fn owned(name: &str, takes: bool, media: bool) -> bool {
    handler(name, takes, media).is_some()
        || read_definition(name).is_some()
        || name == "obs-describe-request"
}
fn inventory(filter: &ToolFilter, takes: bool, media: bool) -> Value {
    let manifest = filter.manifest();
    json!({"tools":manifest["tools"].as_array().unwrap().iter().filter(|tool|tool["name"].as_str().is_some_and(|name|owned(name,takes,media))).cloned().collect::<Vec<_>>()})
}
fn tool_path(variable: &str, name: &str) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(variable).filter(|path| !path.is_empty()) {
        return PathBuf::from(path).canonicalize().ok();
    }
    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|path| path.join(name))
                .find(|path| path.is_file())
        })
        .and_then(|path| path.canonicalize().ok())
}
fn admitted_clock(
    snapshot: impl Fn() -> Result<obs_mcp_native::custody::ClockSnapshot, clock::ClockError>
    + Send
    + Sync
    + 'static,
) -> io::Result<Clock> {
    snapshot().map_err(|_| io::Error::other("Native wall clock unavailable before activation"))?;
    Ok(Arc::new(move || {
        snapshot().unwrap_or_else(|_| {
            // Clock is an infallible legacy domain seam. Once admitted, a broken
            // wall-clock invariant must end this process rather than invent time,
            // renew/revive authority, or claim joined cleanup. Existing durable
            // dispatching receipts remain uncertain; restart never authorizes replay.
            eprintln!(
                "Native wall-clock invariant failed; custody is unknown and OBS stop is unconfirmed"
            );
            std::process::exit(70)
        })
    }))
}
struct MediaWork {
    path: PathBuf,
    identity: fs::Metadata,
}
impl MediaWork {
    fn new(root: &std::path::Path) -> io::Result<Self> {
        let id = obs_mcp_native::runtime_identity::uuid_v4()
            .map_err(|_| io::Error::other("Media work identity is unavailable"))?;
        let path = root.join(format!(".native-media-{id}"));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path)?;
        let identity = fs::symlink_metadata(&path)?;
        Ok(Self { path, identity })
    }
    fn cleanup(&self) -> io::Result<()> {
        let current = match fs::symlink_metadata(&self.path) {
            Ok(current) => current,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if current.dev() != self.identity.dev() || current.ino() != self.identity.ino() {
                return Err(io::Error::other("Media work namespace was replaced"));
            }
        }
        if !current.is_dir() || current.file_type().is_symlink() {
            return Err(io::Error::other("Media work namespace is unavailable"));
        }
        // Do not recursively erase unexpected contents. Joined operations must
        // remove their own files; a foreign/nonempty directory remains intact.
        fs::remove_dir(&self.path)
    }
}
impl Drop for MediaWork {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}
struct Runtime {
    owner: Option<Service>,
    url: String,
    password: Option<String>,
    root: PathBuf,
    host: Option<Arc<LocalHost>>,
    control: SharedControl,
    clock: Clock,
    preparation: Option<PreparationOwners>,
    media: Option<Arc<media_tools::Config>>,
    media_work: Option<MediaWork>,
    screenshot_limit: Result<ScreenshotLimit, obs_mcp_native::observation::ObservationError>,
}
impl Runtime {
    fn configure() -> io::Result<Self> {
        let enabled = |name: &str| std::env::var(name).is_ok_and(|value| config::enabled(&value));
        if std::env::args_os().len() > 1
            || ["OBS_MCP_HTTP_PORT", "OBS_MCP_HTTP_TOKEN"]
                .iter()
                .any(|name| std::env::var_os(name).is_some_and(|value| !value.is_empty()))
            || [
                "OBS_MCP_FORCE_COMPAT",
                "OBS_MCP_DYNAMIC_TOOLSETS",
                "OBS_MCP_CONFIRM_LIVE",
            ]
            .iter()
            .any(|name| enabled(name))
        {
            return Err(io::Error::other(
                "Native stateful qualification supports static stdio only; requested feature is unavailable and no SDK runtime will start",
            ));
        }
        let url = std::env::var("OBS_WEBSOCKET_URL")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "ws://localhost:4455".into());
        if !url.trim().to_ascii_lowercase().starts_with("ws://") {
            return Err(io::Error::other(
                "Native stateful qualification requires an already-qualified plaintext ws endpoint; unsupported transports cannot activate",
            ));
        }
        let resolved = config::resolve_environment()
            .map_err(|_| io::Error::other("Native authentication configuration is unavailable"))?;
        let root = config::resolve_state_root()
            .map_err(|_| io::Error::other("Native state namespace is unavailable"))?;
        let host = std::env::var_os("OBS_MCP_NATIVE_RECORDING_ROOT")
            .filter(|root| !root.is_empty())
            .map(|root| {
                LocalHost::new(
                    &PathBuf::from(root),
                    tool_path("OBS_MCP_NATIVE_FFMPEG", "ffmpeg").as_deref(),
                    tool_path("OBS_MCP_NATIVE_FFPROBE", "ffprobe").as_deref(),
                )
                .map(Arc::new)
                .map_err(|_| {
                    io::Error::other(
                        "Explicit native recording root or media tool binding is unavailable",
                    )
                })
            })
            .transpose()?;
        let clock = admitted_clock(clock::snapshot)?;
        let preparation=Some(PreparationOwners::new(&root).map_err(|_|io::Error::other("Native demo qualification requires an existing approved OBS_MCP_STATE_DIR with safe snapshot/journal storage"))?);
        let (media, media_work) = if host.is_some() {
            match (
                std::env::var_os("OBS_MCP_NATIVE_RECORDING_ROOT"),
                tool_path("OBS_MCP_NATIVE_FFMPEG", "ffmpeg"),
                tool_path("OBS_MCP_NATIVE_FFPROBE", "ffprobe"),
            ) {
                (Some(recording), Some(ffmpeg), Some(ffprobe)) => {
                    let work = MediaWork::new(&root)?;
                    let config = media_tools::Config::new(
                        &PathBuf::from(recording),
                        &ffmpeg,
                        &ffprobe,
                        &work.path,
                    )
                    .map_err(|_| {
                        io::Error::other("Native recording media configuration is unavailable")
                    })?;
                    (Some(Arc::new(config)), Some(work))
                }
                _ => (None, None),
            }
        } else {
            (None, None)
        };
        Ok(Self {
            owner: None,
            url,
            password: resolved.password().map(str::to_owned),
            root,
            host,
            control: Arc::new(Mutex::new(ControlDomain::default())),
            clock,
            preparation,
            media,
            media_work,
            screenshot_limit: ScreenshotLimit::parse(
                std::env::var("OBS_MCP_MAX_SCREENSHOT_BYTES")
                    .ok()
                    .as_deref(),
            ),
        })
    }
    async fn connect(&mut self) -> Result<obs_mcp_native::service::ServiceHandle, Value> {
        if let Some(owner) = &self.owner {
            return Ok(owner.handle());
        }
        let store = OperationStore::new(&self.root, &self.url).map_err(|_| {
            error("Native operation receipt namespace is unavailable; no OBS effect was dispatched")
        })?;
        let (session, events) = ObsSession::connect(
            &self.url,
            self.password.as_deref(),
            SessionOptions {
                event_subscriptions: obs_mcp_native::service::REQUIRED_SUBSCRIPTIONS,
                ..Default::default()
            },
        )
        .await
        .map_err(|_| {
            error("Native OBS connection is unavailable; no compatibility runtime was started")
        })?;
        let bridge = match ObservationBridge::bind(session.handle(), events, None) {
            Ok(bridge) => bridge,
            Err(_) => {
                let _ = session.shutdown().await;
                return Err(error("Native observation ownership is unavailable"));
            }
        };
        let take = self.host.as_ref().map(|host| TakeServices {
            host: host.clone(),
            exact_endpoint: self.url.clone(),
            pending_settings: Arc::new(AtomicBool::new(false)),
            bounds: Bounds::default(),
            limits: Limits::default(),
        });
        let resources = Resources {
            session,
            bridge,
            store,
            control: self.control.clone(),
            clock: self.clock.clone(),
            identities: Box::new(|| {
                obs_mcp_native::runtime_identity::uuid_v4().unwrap_or_default()
            }),
        };
        let owner = match Service::spawn(
            resources,
            Options {
                connection: Some(ConnectionDescriptor {
                    url: self.url.clone(),
                    has_password: self.password.is_some(),
                }),
                state_root: Some(self.root.clone()),
                take,
                preparation: self.preparation.take(),
                media: self.media.clone(),
                read_tools_host: self.host.clone(),
                generic_readback_screenshot_limit: Some(self.screenshot_limit.clone()),
                // Invalid screenshot policy blocks Stage independently; unrelated read
                // tools still own the session and project their exact policy error.
                screenshot_max_bytes: self.screenshot_limit.clone().unwrap_or_default().bytes(),
                stage_screenshot_error: self.screenshot_limit.as_ref().err().cloned(),
                ..Default::default()
            },
        ) {
            Ok(owner) => owner,
            Err((_, resources)) => {
                let _ = resources.bridge.shutdown().await;
                let _ = resources.session.shutdown().await;
                return Err(error("Native persistent ownership admission failed"));
            }
        };
        let handle = owner.handle();
        self.owner = Some(owner);
        // Source connect caches GetVersion before any public request. Persist
        // this same owner's capabilities; no extra fast/workflow socket exists.
        let _ = handle
            .read(ReadRequest::Version, Value::Null, Duration::from_secs(10))
            .await;
        Ok(handle)
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        if let Some(owner) = self.owner.as_mut() {
            let receipt = owner.shutdown().await.map_err(|_| {
                io::Error::other("Native owner could not establish joined shutdown")
            })?;
            if !receipt.joined {
                return Err(io::Error::other(
                    "Native cleanup is unconfirmed; no SDK runtime was started",
                ));
            }
        }
        if let Some(work) = &self.media_work {
            work.cleanup()
                .map_err(|_| io::Error::other("Native media work cleanup is unconfirmed"))?;
        }
        Ok(())
    }
}
struct Pending {
    cancel: Cancellation,
    frontend_cancelled: Arc<AtomicBool>,
    media: Option<MediaCancellation>,
    reserved_stop: bool,
    reserved_safety_stop: bool,
}
const ORDINARY_FRONTEND_JOBS: usize = 32;
struct ResourceFailure {
    code: i64,
    message: String,
}
type Completed = (
    String,
    OwnedReplyContext,
    Result<Value, ResourceFailure>,
    Arc<AtomicBool>,
    bool,
);

// Encoded URI-template variables undergo exactly one decodeURIComponent pass.
fn resource_request(uri: &str) -> Result<(ReadRequest, Value, String), ResourceFailure> {
    let missing = || ResourceFailure {
        code: -32602,
        message: format!("Resource not found: {uri}"),
    };
    if uri == "obs://scenes" {
        return Ok((ReadRequest::SceneList, Value::Null, uri.into()));
    }
    let rest = uri.strip_prefix("obs://scene/").ok_or_else(missing)?;
    let (encoded, request) = if let Some(name) = rest.strip_suffix("/items") {
        (name, ReadRequest::SceneItemList)
    } else if let Some(name) = rest.strip_suffix("/screenshot") {
        (name, ReadRequest::SourceScreenshot)
    } else {
        return Err(missing());
    };
    if encoded.is_empty() || encoded.bytes().any(|b| matches!(b, b'/' | b'?' | b'#')) {
        return Err(missing());
    }
    let mut decoded = Vec::with_capacity(encoded.len());
    let bytes = encoded.as_bytes();
    let mut index = 0;
    let malformed = || ResourceFailure {
        code: -32603,
        message: "URI malformed".into(),
    };
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let digit = |byte: u8| (byte as char).to_digit(16).map(|v| v as u8);
            let high = bytes
                .get(index + 1)
                .copied()
                .and_then(digit)
                .ok_or_else(malformed)?;
            let low = bytes
                .get(index + 2)
                .copied()
                .and_then(digit)
                .ok_or_else(malformed)?;
            decoded.push(high * 16 + low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    let name = String::from_utf8(decoded).map_err(|_| malformed())?;
    let data = if request == ReadRequest::SourceScreenshot {
        json!({"sourceName":name,"imageFormat":"jpeg","imageWidth":960})
    } else {
        json!({"sceneName":name})
    };
    // Match URL.href percent encoding for raw spaces/Unicode, retaining escapes.
    let mut canonical = String::with_capacity(uri.len());
    for byte in uri.bytes() {
        if byte <= 0x20 || byte >= 0x7f {
            use std::fmt::Write;
            write!(canonical, "%{byte:02X}").unwrap();
        } else {
            canonical.push(byte as char);
        }
    }
    Ok((request, data, canonical))
}
async fn read_resource(
    service: &obs_mcp_native::service::ServiceHandle,
    uri: &str,
    limit: Result<ScreenshotLimit, obs_mcp_native::observation::ObservationError>,
) -> Result<Value, ResourceFailure> {
    let (request, data, canonical) = resource_request(uri)?;
    let response = service
        .read(request, data, Duration::from_secs(10))
        .await
        .map_err(|reason| ResourceFailure {
            code: reason
                .request_code()
                .and_then(|code| i64::try_from(code).ok())
                .unwrap_or(-32603),
            message: reason.message,
        })?;
    if request == ReadRequest::SourceScreenshot {
        let image = obs_mcp_native::scene_audit::screenshot_content(
            &response,
            limit.map_err(|reason| ResourceFailure {
                code: -32603,
                message: reason.message,
            })?,
        )
        .map_err(|reason| ResourceFailure {
            code: -32603,
            message: reason.message,
        })?;
        Ok(
            json!({"contents":[{"uri":canonical,"mimeType":image["mimeType"],"blob":image["data"]}]}),
        )
    } else {
        Ok(
            json!({"contents":[{"uri":canonical,"mimeType":"application/json","text":serde_json::to_string_pretty(&response).unwrap()}]}),
        )
    }
}

pub(crate) async fn run() -> io::Result<()> {
    let mut runtime = Runtime::configure()?;
    let filter = ToolFilter::parse(|name| std::env::var(name).ok(), schema::catalog())
        .map_err(io::Error::other)?;
    let manifest = inventory(&filter, runtime.host.is_some(), runtime.media.is_some());
    let mut protocol = ProtocolSession::native_service();
    let mut input = Frames::new(BufReader::new(tokio::io::stdin()));
    let (output, mut output_worker) = crate::stdio_output::Output::spawn(tokio::io::stdout());
    let mut output_result = None;
    let stop = crate::shutdown_signal();
    tokio::pin!(stop);
    let mut jobs: JoinSet<Completed> = JoinSet::new();
    let mut pending = HashMap::<String, Pending>::new();
    let mut fatal = None;
    'frontend: loop {
        macro_rules! send_reply {
            ($target:expr,$response:expr $(,)?) => {
                if let Err(error) = $target.try_send($response) {
                    fatal = Some(error);
                    break 'frontend;
                }
            };
        }
        tokio::select! {
            ()=&mut stop=>break,
            result=&mut output_worker=>{output_result=Some(result);break;},
            result=jobs.join_next(),if !jobs.is_empty()=>{
                match result {Some(Ok((key,reply,result,cancelled,cacheable)))=>{pending.remove(&key);if !cancelled.load(Ordering::Acquire) && let Err(error)=output.try_send(&match result {Ok(result)=>reply.complete(result,cacheable),Err(reason)=>reply.error(reason.code,&reason.message)}) {fatal=Some(error);break;}},_=>{fatal=Some(io::Error::other("Native frontend worker failed; state was not handed off"));break;}}
            },
            next=input.next()=>{
                let frame=match next {Ok(Some(frame))=>frame,Ok(None)=>break,Err(error)=>{fatal=Some(error);break;}};
                if frame.iter().all(u8::is_ascii_whitespace){continue;}
                let value:Value=match serde_json::from_slice(&frame){Ok(value)=>value,Err(_)=>{send_reply!(&output,&rpc_error(Value::Null,-32700,"Parse error"));continue;}};
                if value["jsonrpc"]=="2.0" && value.get("id").is_none() && value["method"]=="notifications/cancelled" {
                    if let Some(id)=value.pointer("/params/requestId") {let key=id.to_string();if let Some(pending)=pending.get(&key){pending.frontend_cancelled.store(true,Ordering::Release);pending.cancel.cancel();if let Some(media)=&pending.media {media.cancel();}}}
                    continue;
                }
                let (reply,name,args,resource)=match protocol.admit(&value,&frame) {
                    Admission::Reply(response)=>{send_reply!(&output,&response);continue;},
                    Admission::Initialized=>continue,
                    Admission::List(reply)=>{send_reply!(&output,&reply.complete(manifest.clone(),true));continue;},
                    Admission::ResourceList(reply)=>(reply.owned(),"native-resource-list".to_owned(),json!({}),true),
                    Admission::ResourceCompletion {reply,prefix}=>(reply.owned(),"native-resource-completion".to_owned(),json!({"prefix":prefix}),true),
                    Admission::ResourceRead {reply,uri:"obs://take/current"}=>{
                        if runtime.owner.is_none(){send_reply!(&output,&reply.complete(json!({"contents":[{"uri":"obs://take/current","mimeType":"application/json","text":"{\n  \"running\": false\n}"}]}),true));continue;}
                        (reply.owned(),"native-take-resource".to_owned(),json!({}),true)
                    },
                    Admission::ResourceRead {reply,uri:"obs://status"}=>(reply.owned(),"obs-observe".to_owned(),json!({}),true),
                    Admission::ResourceRead {reply,uri}=>{if let Err(reason)=resource_request(uri) {send_reply!(&output,&reply.owned().error(reason.code,&reason.message));continue;} (reply.owned(),"native-resource".to_owned(),json!({"uri":uri}),true)},
                    Admission::Call {reply,name,arguments}=>(reply.owned(),name.to_owned(),arguments,false),
                    Admission::Compatibility=>{if let Some(id)=value.get("id"){send_reply!(&output,&rpc_error(id.clone(),-32601,"Native capability unavailable; session state retained and SDK handoff disabled"));}continue;},
                };
                let key=value["id"].to_string();
                // The owned Take and the four closed public Stops each have
                // one extra slot beyond ordinary work. Neither can consume
                // the other's reservation; transport and journal guards remain.
                let reserved_active=pending.values().any(|job|job.reserved_stop);
                let safety_reserved_active=pending.values().any(|job|job.reserved_safety_stop);
                let ordinary_jobs=jobs.len().saturating_sub(usize::from(reserved_active)+usize::from(safety_reserved_active));
                let reserved_stop=ordinary_jobs>=ORDINARY_FRONTEND_JOBS && !reserved_active && !resource && name=="obs-take-stop";
                let reserved_safety_stop=ordinary_jobs>=ORDINARY_FRONTEND_JOBS && !safety_reserved_active && !resource && matches!(name.as_str(),"obs-stop-stream"|"obs-stop-virtual-cam"|"obs-stop-replay-buffer"|"obs-stop-record");
                if pending.contains_key(&key)||(ordinary_jobs>=ORDINARY_FRONTEND_JOBS && !reserved_stop && !reserved_safety_stop) {send_reply!(&output,&rpc_error(value["id"].clone(),-32000,"Native request admission unavailable; live state retained"));continue;}
                if !resource && (!filter.includes(&name)||!owned(&name,runtime.host.is_some(),runtime.media.is_some())) {send_reply!(&output,&rpc_error(value["id"].clone(),-32601,"Tool is unavailable in the native-only qualification lane"));continue;}
                let read=read_definition(&name);
                let args=if resource {Some(args)}else if name=="obs-record-clip" {validated_clip_arguments(args)}else if read.is_some(){schema::native_request(&name,&args).ok().flatten().map(|(_,data)|data)}else if name=="obs-describe-request" {schema::validated_local_arguments(&name,"describe-request",&args)}else{schema::service_arguments(&name,&args)};
                let Some(args)=args else {
                    let tool=schema::catalog()["tools"].as_array().unwrap().iter().find(|tool|tool["name"]==name).unwrap();
                    let validation=if name=="obs-set-input-audio-tracks" {obs_mcp_native::tool_validation::tool_error_from_frame(&name,&tool["inputSchema"],value.pointer("/params/arguments").unwrap_or(&json!({})),&frame)}else{obs_mcp_native::tool_validation::tool_error(&name,&tool["inputSchema"],value.pointer("/params/arguments").unwrap_or(&json!({})))};
                    match validation {
                        Ok(result)=>send_reply!(&output,&reply.complete(result,false)),
                        Err(_)=>send_reply!(&output,&rpc_error(value["id"].clone(),-32602,"Native argument validation projection unavailable; no dispatch or SDK fallback")),
                    }
                    continue;
                };
                let envelope=value.pointer("/params/_meta");
                let caller=caller_name(CallerContext {envelope,legacy_name:protocol.legacy_name(),..Default::default()});
                let binding=CallerBinding::resolve(CallerContext {envelope,legacy_name:protocol.legacy_name(),..Default::default()},None,false,None,None);
                let control=runtime.control.clone();let clock=runtime.clock.clone();
                let read_only=resource||schema::catalog()["tools"].as_array().unwrap().iter().find(|tool|tool["name"]==name).is_some_and(|tool|tool["annotations"]["readOnlyHint"]==true);
                let refusal=control.lock().unwrap().evaluate(GateRequest {tool_name:&name,arguments:&args,read_only_hint:read_only,caller:&caller,confirmation:Default::default()},(clock)().epoch_ms).tool_result();
                if let Some(result)=refusal {send_reply!(&output,&reply.complete(result,false));continue;}
                // Volume's missing-value callback can finish without OBS.
                // Other literal callbacks have no early business refusal, so
                // avoid re-cloning/counting their validated opaque arguments.
                if !resource && name=="obs-set-input-volume" && let Some(result)=obs_mcp_native::generic_requests::Call::parse(&name,&args).and_then(|call|call.business_refusal()) {send_reply!(&output,&reply.complete(result,false));continue;}
                // Source performs late-chapter business validation before any
                // connection/preflight, after the common caller-control policy.
                if name=="obs-record-clip" && let Some(result)=record_clip::Request::parse(&args).expect("validated clip").refusal() {send_reply!(&output,&reply.complete(result,false));continue;}
                if name=="obs-stage-scene" && let Err(reason)=runtime.screenshot_limit.as_ref(){send_reply!(&output,&reply.complete(error(reason.message.clone()),false));continue;}
                if name=="obs-snapshot" && args["list"]==true && runtime.owner.is_none() {
                    let Some(mut owners)=runtime.preparation.take() else {send_reply!(&output,&reply.complete(service_error(ServiceError::Unsupported),false));continue;};
                    let endpoint=runtime.url.clone();
                    let (owners,result)=match tokio::task::spawn_blocking(move || {let result=owners.snapshot_list(&endpoint);(owners,result)}).await {Ok(result)=>result,Err(_)=>{fatal=Some(io::Error::other("Snapshot listing worker failed to join"));break;}};
                    runtime.preparation=Some(owners);send_reply!(&output,&reply.complete(result,false));continue;
                }
                if name=="obs-restore" && runtime.owner.is_none() {
                    let Some(mut owners)=runtime.preparation.take() else {send_reply!(&output,&reply.complete(service_error(ServiceError::Unsupported),false));continue;};
                    let endpoint=runtime.url.clone();let request=restore_workflow::Request::parse(&args).expect("validated restore");
                    // Borrow the SAME offline file owner, after caller policy.
                    // A valid selection grants no authority; Service reselects
                    // and checks policy again after lazy physical connection.
                    let (owners,refusal)=match tokio::task::spawn_blocking(move || {let refusal=owners.local_restore_refusal(&endpoint,&request);(owners,refusal)}).await {Ok(result)=>result,Err(_)=>{fatal=Some(io::Error::other("Restore lookup worker failed to join"));break;}};
                    runtime.preparation=Some(owners);
                    if let Some(result)=refusal {send_reply!(&output,&reply.complete(result,false));continue;}
                }
                let sleep=if name=="obs-sleep" {ReadToolRequest::parse(&name,&args).ok()}else{None};
                let local_sleep=matches!(&sleep,Some(ReadToolRequest::Sleep{frames:None,..})|Some(ReadToolRequest::Sleep{millis:Some(_),frames:Some(_)}));
                let status=name=="obs-get-status";
                let local=status||local_sleep||matches!(name.as_str(),"obs-describe-request"|"obs-claim-control"|"obs-release-control"|"obs-control-status");
                if name=="obs-recover-demo" && runtime.owner.is_none() && let Some(result)=runtime.preparation.as_mut().and_then(|owners|owners.local_recovery_refusal(&runtime.url,args["operationId"].as_str().unwrap())) {send_reply!(&output,&reply.complete(result,false));continue;}
                let service=if status {runtime.owner.as_ref().map(Service::handle)}else if local {None}else {match runtime.connect().await {Ok(handle)=>Some(handle),Err(result)=>{send_reply!(&output,&reply.complete(result,false));continue;}}};
                let descriptor=ConnectionDescriptor{url:runtime.url.clone(),has_password:runtime.password.is_some()};let screenshot_limit=runtime.screenshot_limit.clone();
                let cancel=Cancellation::new();let media_cancel=matches!(name.as_str(),"obs-contact-sheet"|"obs-trim-take").then(MediaCancellation::default);let cancelled=Arc::new(AtomicBool::new(false));pending.insert(key.clone(),Pending {cancel:cancel.clone(),frontend_cancelled:cancelled.clone(),media:media_cancel.clone(),reserved_stop,reserved_safety_stop});
                jobs.spawn(async move {
                    if name=="native-take-resource" {
                        let result=service.as_ref().unwrap().take_resource().await.map(|summary|json!({"contents":[{"uri":"obs://take/current","mimeType":"application/json","text":serde_json::to_string_pretty(&summary).unwrap()}]})).map_err(|_|ResourceFailure{code:-32603,message:"Native current-take resource is unavailable".into()});
                        return (key,reply,result,cancelled,true);
                    }
                    if name=="native-resource-list"||name=="native-resource-completion" {
                        let result=match service.as_ref().unwrap().read(ReadRequest::SceneList,Value::Null,Duration::from_secs(10)).await {
                            Ok(scenes)=>if name=="native-resource-list"{crate::resource_catalog::listing(&scenes)}else{crate::resource_catalog::complete_scenes(&scenes,args["prefix"].as_str().unwrap())}.ok_or_else(||ResourceFailure{code:-32603,message:"Native resource inventory limit exceeded; live state retained".into()}),
                            Err(reason)=>Err(ResourceFailure {code:reason.request_code().and_then(|code|i64::try_from(code).ok()).unwrap_or(-32603),message:reason.message}),
                        };
                        return (key,reply,result,cancelled,name=="native-resource-list");
                    }
                    if name=="native-resource" {let result=read_resource(service.as_ref().unwrap(),args["uri"].as_str().unwrap(),screenshot_limit).await;return (key,reply,result,cancelled,true);}
                    let mut result=match name.as_str() {
                        "obs-get-status"=>if let Some(service)=service.as_ref(){service.read_tool(ReadToolRequest::Status,screenshot_limit,cancel).await.unwrap_or_else(service_error)}else{service_read_tools::status(StatusMetadata {connection:descriptor,connected:false,identified:false},clock.clone()).unwrap_or_else(|reason|error(reason.message))},
                        "obs-sleep" if local_sleep=>match sleep.unwrap(){ReadToolRequest::Sleep{millis:Some(millis),frames:None}=>service_read_tools::sleep_millis(millis,cancel,tokio::time::Instant::now()+Duration::from_secs(60)).await,_=>service_read_tools::sleep_selection_error()},
                        "obs-describe-request"=>crate::protocol_catalog::describe(&args),
                        "obs-claim-control"=>control.lock().unwrap().claim(&caller,args["minutes"].as_u64().unwrap_or(30) as u16,args["reason"].as_str().unwrap_or(""),(clock)().epoch_ms).map(|result|result.tool_result()).unwrap_or_else(|_|error("Invalid control lease arguments")),
                        "obs-release-control"=>control.lock().unwrap().release(&caller,(clock)().epoch_ms).tool_result(),
                        "obs-control-status"=>control.lock().unwrap().status_result(&caller,(clock)().epoch_ms),
                        _=>dispatch(service.as_ref().unwrap(),&name,args,read,binding,caller,cancel,media_cancel,resource,screenshot_limit).await,
                    };
                    if !resource {result=ensure_structured(result);}
                    if resource && !result.get("isError").is_some_and(|value|value==true) {let report=result["structuredContent"].clone();result=json!({"contents":[{"uri":"obs://status","mimeType":"application/json","text":serde_json::to_string_pretty(&report).unwrap()}]});}
                    (key,reply,Ok(result),cancelled,resource)
                });
            },
        }
    }
    // EOF, cancellation, signals and write failures cannot drop jobs or the loan.
    // Frontend replies may be gone; actual Service work remains owned until join.
    for pending in pending.values() {
        pending.cancel.cancel();
        if let Some(media) = &pending.media {
            media.cancel();
        }
    }
    // Signal the owning Service while retained frontend work drains. Waiting
    // for an unrelated reply first would postpone its reserved owned Stop.
    // Both futures still settle before either owner is released.
    drop(output); // Close admission; the retained writer drains already-owned frames.
    let (frontend_joined, shutdown, output_joined) = tokio::join!(
        async {
            let mut joined = true;
            while let Some(result) = jobs.join_next().await {
                joined &= result.is_ok();
            }
            joined
        },
        runtime.shutdown(),
        async {
            match output_result {
                Some(result) => result,
                None => output_worker.await,
            }
        }
    );
    if !frontend_joined {
        fatal.get_or_insert_with(|| {
            io::Error::other("Native frontend worker did not join successfully")
        });
    }
    match output_joined {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            fatal.get_or_insert(error);
        }
        Err(_) => {
            fatal.get_or_insert_with(|| {
                io::Error::other("Native output worker did not join successfully")
            });
        }
    }
    shutdown?;
    match fatal {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
#[allow(clippy::too_many_arguments)]
async fn dispatch(
    service: &obs_mcp_native::service::ServiceHandle,
    name: &str,
    args: Value,
    read: Option<ReadRequest>,
    binding: CallerBinding,
    caller: String,
    cancel: Cancellation,
    media_cancel: Option<MediaCancellation>,
    _resource: bool,
    screenshot_limit: Result<ScreenshotLimit, obs_mcp_native::observation::ObservationError>,
) -> Value {
    if let Some(request) = read {
        return match service.read(request, args, Duration::from_secs(10)).await {
            Ok(response) => {
                let response = if response.is_object() {
                    response
                } else {
                    json!({})
                };
                if !schema::valid_response(name, &response) {
                    return error(
                        "Native response schema validation failed; no compatibility runtime was started",
                    );
                }
                json!({"content":[{"type":"text","text":serde_json::to_string_pretty(&response).unwrap()}],"structuredContent":response})
            }
            Err(reason) => {
                let title = schema::catalog()["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|tool| tool["name"] == name)
                    .and_then(|tool| tool["title"].as_str())
                    .unwrap_or(name);
                error(format!("{title} failed: {}", reason.message))
            }
        };
    }
    if name == "obs-snapshot" {
        return service
            .snapshot(
                snapshot_tools::Request::parse(&args).expect("validated snapshot"),
                cancel,
            )
            .await
            .unwrap_or_else(service_error);
    }
    if name == "obs-get-input-property-list-items" {
        return service
            .property_list(
                property_list::Request::parse(&args).expect("validated property list"),
                cancel,
            )
            .await
            .unwrap_or_else(service_error);
    }
    if name == "obs-restore" {
        let request = restore_workflow::Request::parse(&args).expect("validated restore");
        drop(args);
        return service
            .restore(request, caller, cancel)
            .await
            .unwrap_or_else(service_error);
    }
    if name == "obs-record-clip" {
        let request = record_clip::Request::parse(&args).expect("validated clip");
        // The retained Service owns only the typed schedule, not another raw
        // JSON chapter payload throughout the potentially50-second workflow.
        drop(args);
        return service
            .record_clip(
                request,
                Policy {
                    caller,
                    ..Default::default()
                },
                cancel,
            )
            .await
            .unwrap_or_else(service_error);
    }
    if let Some(call) = obs_mcp_native::generic_requests::Call::parse(name, &args) {
        drop(args);
        return service
            .generic(call, caller, cancel)
            .await
            .unwrap_or_else(service_error);
    }
    if matches!(
        name,
        "obs-test-connection"
            | "obs-sleep"
            | "obs-get-input-list"
            | "obs-get-input-kind-list"
            | "obs-get-source-screenshot"
            | "obs-preflight"
    ) {
        return service
            .read_tool(
                ReadToolRequest::parse(name, &args).expect("validated read tool"),
                screenshot_limit,
                cancel,
            )
            .await
            .unwrap_or_else(service_error);
    }
    let policy = Policy {
        caller,
        ..Default::default()
    };
    let result=match name {
        "obs-observe"=>service.observe(args["canvasUuid"].as_str().map(str::to_owned)).await.map(|report|json!({"content":[{"type":"text","text":report.to_string()}],"structuredContent":report})),
        "obs-stage-scene"=>service.stage(Target {scene_collection:args["sceneCollection"].as_str().unwrap().into(),scene_name:args["sceneName"].as_str().unwrap().into(),canvas_uuid:args["canvasUuid"].as_str().map(str::to_owned)},binding,cancel).await,
        "obs-take-scene"=>service.take_scene(args["stageId"].as_str().unwrap().into(),binding,cancel).await,
        "obs-plan-scene-template"=>service.plan_scene(obs_mcp_native::scene_plan::PlanRequest::from_value(&args).expect("validated plan"),cancel).await,
        "obs-audit-scene"=>service.audit_scene(obs_mcp_native::scene_audit::AuditOptions::from_value(&args).expect("validated audit"),screenshot_limit,cancel).await,
        "obs-contact-sheet"|"obs-trim-take"=>service.recording_media(media_tools::Request::parse(name,&args).expect("validated media request"),media_cancel.unwrap_or_default()).await,
        "obs-prepare-demo"=>service.prepare_demo(Selection {scene_collection:args["sceneCollection"].as_str().unwrap().into(),scene_name:args["sceneName"].as_str().unwrap().into(),input_name:args["inputName"].as_str().unwrap().into()},args["apply"]==true,DemoPolicy::resolve(DemoAction::Prepare,CallerContext {legacy_name:Some(&policy.caller),..Default::default()}),cancel).await,
        "obs-recover-demo"=>service.recover_demo(args["operationId"].as_str().unwrap().into(),args["apply"]==true,DemoPolicy::resolve(DemoAction::Recover,CallerContext {legacy_name:Some(&policy.caller),..Default::default()}),cancel).await,
        "obs-take-start"=>service.start_take(StartOptions {expect_silent:args["expectSilent"]==true,min_free_disk_mb:args["minFreeDiskMb"].as_f64().unwrap_or(1024.0),lock:args["lock"]!=false,chapter:args["chapter"].as_str().map(str::to_owned),max_duration_seconds:args["maxDurationSeconds"].as_u64().map(|value|value as u16),require_verified_preflight:args["requireVerifiedPreflight"]==true},policy).await,
        "obs-stop-record"=>service.manual_record_stop(policy,cancel).await,
        "obs-take-stop"=>service.stop_take(None,policy).await,
        "obs-take-mark"=>service.mark_take(args["name"].as_str().unwrap().into(),policy).await,
        "obs-take-status"=>service.take_status().await,
        _=>return error("Unsupported native service handler; no SDK handoff"),
    };
    match result {
        Ok(Value::Null) if name == "obs-take-status" => no_take(),
        Ok(result) => result,
        Err(ServiceError::Unsupported) if name == "obs-take-stop" || name == "obs-take-mark" => {
            no_take()
        }
        Err(reason) => service_error(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clip_closed_validation_preserves_fractional_fields_defaults_and_source_stripping() {
        assert_eq!(
            validated_clip_arguments(json!({"durationSeconds":0.25,"extra":true})),
            Some(json!({"durationSeconds":0.25,"chapters":[],"expectSilent":false}))
        );
        assert_eq!(
            validated_clip_arguments(
                json!({"durationSeconds":0.25,"chapters":[{"atSeconds":0.125,"name":" ","extra":{"request":"StopRecord"}}],"expectSilent":true})
            ),
            Some(
                json!({"durationSeconds":0.25,"chapters":[{"atSeconds":0.125,"name":" "}],"expectSilent":true})
            )
        );
        for malformed in [
            json!({"durationSeconds":0}),
            json!({"durationSeconds":50.1}),
            json!({"durationSeconds":"1"}),
            json!({"durationSeconds":1,"chapters":[null]}),
            json!({"durationSeconds":1,"chapters":[{"atSeconds":-1,"name":"bad"}]}),
            json!({"durationSeconds":1,"chapters":[{"atSeconds":0,"name":""}]}),
            json!({"durationSeconds":1,"expectSilent":null}),
        ] {
            assert!(validated_clip_arguments(malformed).is_none());
        }
    }
    #[test]
    fn initial_bad_clock_refuses_admission() {
        assert!(admitted_clock(|| Err(clock::ClockError::BeforeEpoch)).is_err());
    }
    #[test]
    #[ignore]
    fn clock_failure_child() {
        if std::env::var("NATIVE_CLOCK_FAILURE_CHILD").as_deref() != Ok("1") {
            return;
        }
        let root = PathBuf::from(std::env::var_os("NATIVE_CLOCK_FAILURE_ROOT").unwrap());
        let mut store = OperationStore::new(root, "ws://clock-fixture:4455").unwrap();
        let receipt = store
            .prepare(
                "00000000-0000-4000-8000-000000000001",
                obs_mcp_native::operation_store::OperationKind::StageScene,
                1,
                "2026-10-31T12:00:00.000Z",
                Default::default(),
            )
            .unwrap();
        store
            .update(
                &receipt,
                obs_mcp_native::operation_store::OperationUpdate::new(
                    obs_mcp_native::operation_store::Phase::Dispatching,
                ),
                "2026-10-31T12:00:00.000Z",
            )
            .unwrap();
        let count = std::sync::atomic::AtomicUsize::new(0);
        let clock = admitted_clock(move || {
            if count.fetch_add(1, Ordering::Relaxed) == 0 {
                clock::from_epoch_duration(Duration::ZERO)
            } else {
                Err(clock::ClockError::OutOfRange)
            }
        })
        .unwrap();
        clock();
        panic!("failed clock returned fabricated metadata");
    }
    #[test]
    fn later_bad_clock_fails_closed_and_preserves_uncertain_receipt() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!("native-clock-failure-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_service::tests::clock_failure_child",
                "--ignored",
                "--nocapture",
            ])
            .env("NATIVE_CLOCK_FAILURE_CHILD", "1")
            .env("NATIVE_CLOCK_FAILURE_ROOT", &root)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(70));
        assert!(String::from_utf8_lossy(&output.stderr).contains("custody is unknown"));
        let store = OperationStore::new(&root, "ws://clock-fixture:4455").unwrap();
        assert_eq!(
            store
                .read("00000000-0000-4000-8000-000000000001")
                .unwrap()
                .phase(),
            obs_mcp_native::operation_store::Phase::Dispatching
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
