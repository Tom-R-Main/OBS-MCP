// SPDX-License-Identifier: MIT
//! Explicit qualification lane. There is no SDK/compatibility entry point in
//! this module: unsupported features retain the persistent owner's state.
#[path = "confirmation.rs"]
mod confirmation;
#[path = "native_connect.rs"]
mod native_connect;
#[path = "native_host.rs"]
mod native_host;
use native_connect::{ConnectAttempt, ServiceSource};
// Both transports lend frontend identities to the same retained Host.
#[path = "http_bridge.rs"]
mod http_bridge;
#[path = "http_startup.rs"]
mod http_startup;
use native_host::{Admissions, Class, ExchangeId, FrontendId, Host};

use crate::{
    dynamic_tools::{self, DynamicTools},
    protocol::{Admission, OwnedReplyContext, ProtocolSession},
    schema,
    stdio_input::Frames,
    tool_filter::ToolFilter,
};
use obs_mcp_native::{
    clock, config,
    control::{
        CallerContext, ConfirmationContext, ControlDomain, Disposition, GateRequest, caller_name,
        client_can_elicit,
    },
    custody::{Clock, SharedControl},
    demo::Selection,
    demo_transport::{DemoAction, DemoPolicy},
    media_tools,
    media_verification::Cancellation as MediaCancellation,
    native_profile::NativeProfile,
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
    collections::VecDeque,
    fs, io,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::io::BufReader;

fn error(text: impl Into<String>) -> Value {
    json!({"content":[{"type":"text","text":text.into()}],"isError":true})
}
fn service_error(reason: ServiceError) -> Value {
    if reason == ServiceError::Profile {
        return error(
            "Native profile does not grant this operation. Advanced commands never create Take/media/Stage/recovery owners; retained advanced dispatch history blocks guarded reentry until separately qualified OBS incarnation/reset evidence. Current inactivity or restarting MCP does not clear that history",
        );
    }
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
        "obs-call-request"
        | "obs-batch"
        | "obs-call-vendor-request"
        | "obs-trigger-hotkey-by-name"
        | "obs-trigger-hotkey-by-key-sequence" => "advanced-opaque-command",
        "obs-start-record" | "obs-toggle-record" | "obs-start-output" | "obs-stop-output"
        | "obs-toggle-output" => "advanced-manual-command",
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
        "obs-apply-scene" => "apply-scene",
        "obs-capture-window" => "capture-window",
        "obs-get-input-property-list-items" => "property-list",
        "obs-stage-scene" => "stage-scene",
        "obs-take-scene" => "take-scene",
        "obs-record-clip" if takes => "record-clip",
        "obs-save-replay-verified" if media => "save-replay-verified",
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
fn owned(name: &str, takes: bool, media: bool, profile: NativeProfile) -> bool {
    profile.allows(name)
        && (handler(name, takes, media).is_some()
            || read_definition(name).is_some()
            || name == "obs-describe-request")
}
fn inventory(filter: &ToolFilter, takes: bool, media: bool, profile: NativeProfile) -> Value {
    let manifest = filter.manifest();
    json!({"tools":manifest["tools"].as_array().unwrap().iter().filter(|tool|tool["name"].as_str().is_some_and(|name|owned(name,takes,media,profile))).cloned().collect::<Vec<_>>()})
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
    cleanup_allowed: bool,
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
        Ok(Self {
            path,
            identity,
            cleanup_allowed: true,
        })
    }
    fn cleanup(&self) -> io::Result<()> {
        if !self.cleanup_allowed {
            return Err(io::Error::other(
                "Media work is retained after unconfirmed backend cleanup",
            ));
        }
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
    profile: NativeProfile,
    notifications: obs_mcp_native::resource_events::Owner,
    owner: Option<Service>,
    initialization: Option<ConnectAttempt>,
    initialization_broken: bool,
    network: Option<obs_mcp_native::network_transport::Owner>,
    tls_ca: Option<Vec<Vec<u8>>>,
    url: String,
    password: Option<String>,
    root: PathBuf,
    host: Option<Arc<LocalHost>>,
    control: SharedControl,
    clock: Clock,
    preparation: Option<PreparationOwners>,
    media: Option<Arc<media_tools::Config>>,
    replay: Option<Arc<obs_mcp_native::replay_files::Verifier>>,
    media_work: Option<MediaWork>,
    screenshot_limit: Result<ScreenshotLimit, obs_mcp_native::observation::ObservationError>,
    confirm_live: bool,
}
impl Runtime {
    fn configure(dynamic_requested: bool) -> io::Result<Self> {
        let enabled = |name: &str| std::env::var(name).is_ok_and(|value| config::enabled(&value));
        if std::env::args_os().len() > 1
            || ["OBS_MCP_FORCE_COMPAT"].iter().any(|name| enabled(name))
        {
            return Err(io::Error::other(
                "Native service refuses unsupported CLI or compatibility mode; no SDK runtime will start",
            ));
        }
        let url = std::env::var("OBS_WEBSOCKET_URL")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "ws://localhost:4455".into());
        if !(url.trim().to_ascii_lowercase().starts_with("ws://")
            || cfg!(feature = "platform-tls-evaluation")
                && url.trim().to_ascii_lowercase().starts_with("wss://"))
        {
            return Err(io::Error::other(
                "Native configuration requires a supported WS/WSS endpoint; unavailable transports cannot activate",
            ));
        }
        let tls_ca = config::additional_tls_roots(
            std::env::var_os("OBS_MCP_TLS_CA_FILE").as_deref(),
            cfg!(feature = "platform-tls-evaluation"),
        )?;
        let resolved = config::resolve_environment()
            .map_err(|_| io::Error::other("Native authentication configuration is unavailable"))?;
        let root = config::resolve_state_root()
            .map_err(|_| io::Error::other("Native state namespace is unavailable"))?;
        let profile = NativeProfile::parse(std::env::var("OBS_MCP_NATIVE_PROFILE").ok().as_deref())
            .map_err(|_| {
                io::Error::other(
                    "Native profile must be production or advanced; no runtime was started",
                )
            })?;
        let host = if profile == NativeProfile::Advanced {
            None
        } else {
            std::env::var_os("OBS_MCP_NATIVE_RECORDING_ROOT")
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
                .transpose()?
        };
        let clock = admitted_clock(clock::snapshot)?;
        let preparation = if profile == NativeProfile::Advanced {
            None
        } else {
            Some(PreparationOwners::new(&root).map_err(|_|io::Error::other("Native demo qualification requires an existing approved OBS_MCP_STATE_DIR with safe snapshot/journal storage"))?)
        };
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
        let replay = match (
            &media_work,
            std::env::var_os("OBS_MCP_NATIVE_RECORDING_ROOT"),
            tool_path("OBS_MCP_NATIVE_FFMPEG", "ffmpeg"),
            tool_path("OBS_MCP_NATIVE_FFPROBE", "ffprobe"),
        ) {
            (Some(work), Some(recording), Some(ffmpeg), Some(ffprobe)) => Some(Arc::new(
                obs_mcp_native::replay_files::Verifier::new(
                    &PathBuf::from(recording),
                    &ffmpeg,
                    &ffprobe,
                    &work.path,
                )
                .map_err(|_| {
                    io::Error::other("Replay verification configuration is unavailable")
                })?,
            )),
            _ => None,
        };
        Ok(Self {
            profile,
            notifications: obs_mcp_native::resource_events::Owner::new(dynamic_requested),
            owner: None,
            initialization: None,
            initialization_broken: false,
            network: None,
            tls_ca,
            url,
            password: resolved.password().map(str::to_owned),
            root,
            host,
            control: Arc::new(Mutex::new(ControlDomain::default())),
            clock,
            preparation,
            media,
            replay,
            media_work,
            confirm_live: enabled("OBS_MCP_CONFIRM_LIVE"),
            screenshot_limit: ScreenshotLimit::parse(
                std::env::var("OBS_MCP_MAX_SCREENSHOT_BYTES")
                    .ok()
                    .as_deref(),
            ),
        })
    }
    fn service_source(&mut self, guarded: bool) -> ServiceSource {
        if let Some(attempt) = &self.initialization {
            return ServiceSource::Waiting(attempt.receiver());
        }
        if let Some(owner) = &self.owner {
            return ServiceSource::Ready(Ok(Some(owner.handle())));
        }
        if self.initialization_broken {
            return ServiceSource::Ready(Err(error(
                "Native connection cleanup is unconfirmed; no new initialization was admitted",
            )));
        }
        // Preserve the existing pre-network receipt guard. A future resolver
        // Owner::resolve may register work only after this guard succeeds.
        let store = match OperationStore::new(&self.root, &self.url) {
            Ok(store) => store,
            Err(_) => {
                return ServiceSource::Ready(Err(error(
                    "Native operation receipt namespace is unavailable; no OBS effect was dispatched",
                )));
            }
        };
        if self.profile == NativeProfile::Production {
            match store.read_view().guarded_reentry_allowed() {
                Ok(true) => {}
                Ok(false) => match store.read_view().production_connection_allowed() {
                    Ok(true) if !guarded => {}
                    Ok(_) => {
                        return ServiceSource::Ready(Err(service_error(ServiceError::Profile)));
                    }
                    Err(_) => {
                        return ServiceSource::Ready(Err(service_error(ServiceError::Uncertain)));
                    }
                },
                Err(_) => {
                    return ServiceSource::Ready(Err(error(
                        "Native operation receipt history is unavailable; inspect retained evidence before another change",
                    )));
                }
            }
        }
        // Reentry guard above precedes all helper/DNS admission. Retain the
        // sole owner here; the static bootstrap carries only its Connector.
        if self.network.is_none() {
            match obs_mcp_native::network_transport::Owner::with_additional_roots(
                self.tls_ca.clone(),
            ) {
                Ok(owner) => self.network = Some(owner),
                Err(_) => {
                    return ServiceSource::Ready(Err(error(
                        "Native network ownership is unavailable",
                    )));
                }
            }
        }
        let connector = self.network.as_ref().unwrap().connector();
        let url = self.url.clone();
        let password = self.password.clone();
        // A future platform transport supplies its establishment-only Connector
        // here. Its non-Clone TLS Owner remains in Runtime through global join.
        let mut options = SessionOptions {
            event_subscriptions: obs_mcp_native::service::REQUIRED_SUBSCRIPTIONS,
            ..Default::default()
        };
        let deadline = tokio::time::Instant::now() + options.connect_timeout;
        let establish = Box::pin(async move {
            options.connect_timeout =
                deadline.saturating_duration_since(tokio::time::Instant::now());
            if options.connect_timeout.is_zero() {
                return Err(obs_mcp_native::session::ConnectError::Deadline);
            }
            ObsSession::connect_with_network_connector(
                &url,
                password.as_deref(),
                options,
                connector,
                deadline,
            )
            .await
        });
        let attempt = ConnectAttempt::start(
            native_connect::Inputs {
                store,
                control: self.control.clone(),
                clock: self.clock.clone(),
                publisher: self.notifications.publisher(),
                deadline,
            },
            establish,
        );
        let source = ServiceSource::Waiting(attempt.receiver());
        self.initialization = Some(attempt);
        source
    }
    fn initializing(&self) -> bool {
        self.initialization.is_some()
    }
    async fn progress_initialization(&mut self) -> io::Result<()> {
        let Some(attempt) = self.initialization.as_mut() else {
            return Ok(());
        };
        // The JoinSet owns the actual attempt; this borrowed wait is safe to
        // drop when a peer frame/catalog request wins the server's select.
        let result = attempt.join_next().await;
        self.accept_initialization(result)
    }
    fn accept_initialization(
        &mut self,
        result: Option<Result<native_connect::Outcome, tokio::task::JoinError>>,
    ) -> io::Result<()> {
        let mut attempt = self.initialization.take().expect("retained initialization");
        match result {
            Some(Ok(native_connect::Outcome::Ready(resources))) => {
                match self.adopt_connection(*resources) {
                    Ok(handle) => attempt.publish(Ok(handle)),
                    Err(resources) => {
                        attempt.cleanup(resources);
                        self.initialization = Some(attempt);
                    }
                }
            }
            Some(Ok(native_connect::Outcome::Failed { value, joined })) => {
                self.initialization_broken |= !joined;
                attempt.publish(Err(value));
            }
            _ => {
                self.initialization_broken = true;
                attempt.publish(Err(error(
                    "Native initialization worker failed; cleanup is unconfirmed",
                )));
            }
        }
        Ok(())
    }
    #[cfg(test)]
    async fn connect(&mut self) -> Result<obs_mcp_native::service::ServiceHandle, Value> {
        let source = self.service_source(false);
        while self.initializing() {
            self.progress_initialization()
                .await
                .map_err(|_| error("Native initialization could not join"))?;
        }
        source
            .resolve(Cancellation::new())
            .await?
            .ok_or_else(|| error("Native connection is unavailable"))
    }
    fn adopt_connection(
        &mut self,
        resources: obs_mcp_native::service::BootstrappedResources,
    ) -> Result<obs_mcp_native::service::ServiceHandle, Box<Resources>> {
        let take = self.host.as_ref().map(|host| TakeServices {
            host: host.clone(),
            exact_endpoint: self.url.clone(),
            pending_settings: Arc::new(AtomicBool::new(false)),
            bounds: Bounds::default(),
            limits: Limits::default(),
        });
        let owner = match Service::spawn_bootstrapped(
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
                replay: self.replay.clone(),
                read_tools_host: self.host.clone(),
                generic_readback_screenshot_limit: Some(self.screenshot_limit.clone()),
                // Invalid screenshot policy blocks Stage independently; unrelated read
                // tools still own the session and project their exact policy error.
                screenshot_max_bytes: self.screenshot_limit.clone().unwrap_or_default().bytes(),
                stage_screenshot_error: self.screenshot_limit.as_ref().err().cloned(),
                ..Default::default()
            },
            self.profile,
        ) {
            Ok(owner) => owner,
            Err((_, resources)) => return Err(resources),
        };
        let handle = owner.handle();
        self.owner = Some(owner);
        Ok(handle)
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        let mut failure = None;
        while self.initializing() {
            if let Err(error) = self.progress_initialization().await {
                failure.get_or_insert(error);
                break;
            }
        }
        if self.initialization_broken {
            failure.get_or_insert_with(|| {
                io::Error::other("Native initialization cleanup is unconfirmed")
            });
        }
        let mut backend_joined = !self.initialization_broken;
        if let Some(owner) = self.owner.as_mut() {
            match owner.shutdown().await {
                Ok(receipt) if receipt.joined => {}
                _ => {
                    backend_joined = false;
                    failure.get_or_insert_with(|| {
                        io::Error::other("Native owner could not establish joined shutdown")
                    });
                }
            }
        }
        // Do not return a backend error before draining both independent
        // outer owners. The aggregate retains the backend/TLS/resolver failures.
        if let Some(network) = self.network.as_mut() {
            let backend = failure.take().map_or(Ok(()), Err);
            failure = network.shutdown_after_backend(backend).await.err();
        }
        if let Some(work) = self.media_work.as_mut() {
            if !backend_joined {
                work.cleanup_allowed = false;
            } else if work.cleanup().is_err() {
                failure.get_or_insert_with(|| {
                    io::Error::other("Native media work cleanup is unconfirmed")
                });
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
// These are already-admitted Source routes, not a new raw/output capability.
enum ConfirmedCommand {
    Generic(obs_mcp_native::generic_requests::Call),
    TakeScene(String),
    TakeStop,
    RecordStop,
}
impl ConfirmedCommand {
    fn parse(name: &str, args: &Value) -> Option<Self> {
        Some(match name {
            "obs-take-scene" => Self::TakeScene(args["stageId"].as_str()?.into()),
            "obs-take-stop" => Self::TakeStop,
            "obs-stop-record" => Self::RecordStop,
            "obs-start-record" | "obs-toggle-record" | "obs-start-output" | "obs-stop-output"
            | "obs-toggle-output" | "obs-start-stream" | "obs-stop-stream"
            | "obs-toggle-stream" | "obs-call-request" | "obs-batch" => {
                Self::Generic(obs_mcp_native::generic_requests::Call::parse(name, args)?)
            }
            _ => return None,
        })
    }
    fn retained_bytes(&self) -> Option<usize> {
        match self {
            Self::Generic(call) => call.retained_bytes(),
            Self::TakeScene(id) => Some(id.len()),
            Self::TakeStop | Self::RecordStop => Some(0),
        }
    }
    async fn execute(
        self,
        service: &obs_mcp_native::service::ServiceHandle,
        caller: String,
        binding: CallerBinding,
        cancel: Cancellation,
    ) -> Value {
        let take_stop = matches!(&self, Self::TakeStop);
        let result = match self {
            Self::Generic(call) => service.generic(call, caller, cancel).await,
            Self::TakeScene(id) => service.take_scene(id, binding, cancel).await,
            Self::TakeStop => {
                service
                    .stop_take(
                        None,
                        Policy {
                            caller,
                            ..Default::default()
                        },
                    )
                    .await
            }
            Self::RecordStop => {
                service
                    .manual_record_stop(
                        Policy {
                            caller,
                            ..Default::default()
                        },
                        cancel,
                    )
                    .await
            }
        };
        match result {
            Ok(value) => value,
            Err(ServiceError::Unsupported) if take_stop => no_take(),
            Err(error) => service_error(error),
        }
    }
}
struct PreparedCall {
    frontend: FrontendId,
    reply: OwnedReplyContext,
    name: String,
    args: Value,
    resource: bool,
    controller: bool,
    read: Option<ReadRequest>,
    binding: CallerBinding,
    caller: String,
    apply_request: Option<obs_mcp_native::apply_scene::Request>,
    capture_request: Option<obs_mcp_native::capture_window::Request>,
    confirmed_command: Option<ConfirmedCommand>,
    consent_binding: [u8; 32],
    explicit_confirm: Option<bool>,
    can_elicit: bool,
    form_capable: bool,
}
impl PreparedCall {
    // Logical retained JSON/string accounting, not an RSS bound. All waiting
    // commands are the closed variants above, with no opaque body/observer.
    fn initialization_bytes(&self) -> Option<usize> {
        let mut bytes = self.retained_bytes()?;
        if let Some(request) = &self.apply_request {
            bytes = bytes.checked_add(
                serde_json::to_vec(&request.arguments())
                    .ok()?
                    .len()
                    .checked_mul(2)?,
            )?;
        }
        if let Some(request) = &self.capture_request {
            bytes = bytes.checked_add(
                serde_json::to_vec(&request.arguments())
                    .ok()?
                    .len()
                    .checked_mul(2)?,
            )?;
        }
        Some(bytes)
    }
    fn retained_bytes(&self) -> Option<usize> {
        serde_json::to_vec(&self.args)
            .ok()?
            .len()
            .checked_add(
                serde_json::to_vec(self.reply.id())
                    .ok()?
                    .len()
                    .checked_mul(2)?,
            )?
            .checked_add(self.caller.len().checked_mul(4)?)?
            .checked_add(
                self.confirmed_command
                    .as_ref()
                    .map_or(Some(0), ConfirmedCommand::retained_bytes)?,
            )?
            .checked_add(512)
    }
}
fn confirmed_manifest(mut manifest: Value, enabled: bool) -> Value {
    if enabled && let Some(tools) = manifest["tools"].as_array_mut() {
        for tool in tools {
            tool["inputSchema"] = confirmation::input_schema(
                tool["name"].as_str().unwrap_or(""),
                &tool["inputSchema"],
                true,
            );
        }
    }
    manifest
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
    ExchangeId,
    OwnedReplyContext,
    Result<Value, ResourceFailure>,
    Arc<AtomicBool>,
    bool,
);

type LocalReply = (
    ExchangeId,
    Arc<AtomicBool>,
    crate::stdio_output::Reservation,
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
    let http = http_startup::Configuration::environment()?;
    let dynamic_requested =
        std::env::var("OBS_MCP_DYNAMIC_TOOLSETS").is_ok_and(|value| config::enabled(&value));
    let runtime = Runtime::configure(dynamic_requested && http.is_none())?;
    if let Some(configuration) = http {
        if dynamic_requested {
            eprintln!(
                "OBS_MCP_DYNAMIC_TOOLSETS is ignored over HTTP; registering the OBS_MCP_TOOLSETS groups instead"
            );
        }
        let filter = ToolFilter::parse(|name| std::env::var(name).ok(), schema::catalog())
            .map_err(io::Error::other)?;
        return http_bridge::run(runtime, filter, configuration.port, configuration.token).await;
    }
    let mut host = Host::from_runtime(runtime, true)?;
    let frontend_guard = host
        .frontend()
        .map_err(|_| io::Error::other("Native frontend identity unavailable"))?;
    let frontend = frontend_guard.id();
    let filter = ToolFilter::parse(
        |name| {
            if dynamic_requested {
                None
            } else {
                std::env::var(name).ok()
            }
        },
        schema::catalog(),
    )
    .map_err(io::Error::other)?;
    let manifest = confirmed_manifest(
        inventory(
            &filter,
            host.runtime.host.is_some(),
            host.runtime.media.is_some(),
            host.runtime.profile,
        ),
        host.runtime.confirm_live,
    );
    let mut dynamic = if dynamic_requested {
        let available = schema::catalog()["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .filter(|name| {
                owned(
                    name,
                    host.runtime.host.is_some(),
                    host.runtime.media.is_some(),
                    host.runtime.profile,
                )
            })
            .map(str::to_owned)
            .collect();
        Some(DynamicTools::new(
            available,
            host.runtime.host.is_some() || host.runtime.media.is_some(),
            |name| std::env::var(name).ok(),
        )?)
    } else {
        None
    };
    let mut protocol = if dynamic.is_some() {
        ProtocolSession::native_dynamic_service()
    } else {
        ProtocolSession::native_service()
    };
    protocol.enable_confirmation(host.runtime.confirm_live);
    let mut input = Frames::new(BufReader::new(tokio::io::stdin()));
    let (output, mut output_worker) = crate::stdio_output::Output::spawn(tokio::io::stdout());
    let mut output_result = None;
    let stop = crate::shutdown_signal();
    tokio::pin!(stop);
    let mut local_replies = VecDeque::<LocalReply>::with_capacity(ORDINARY_FRONTEND_JOBS);
    let mut fatal = None;
    'frontend: loop {
        macro_rules! send_reply {
            ($target:expr,$response:expr $(,)?) => {{
                if let Err(error) =
                    crate::resource_notifications::flush(&mut host.runtime.notifications, $target)
                {
                    fatal = Some(error);
                    break 'frontend;
                }
                if let Err(error) = $target.try_send($response) {
                    fatal = Some(error);
                    break 'frontend;
                }
            }};
        }
        let confirmation_deadline = host.confirmations.next_deadline();
        tokio::select! {
            biased;
            ()=&mut stop=>break,
            ()=async {if let Some(at)=confirmation_deadline {tokio::time::sleep_until(at).await;}},if confirmation_deadline.is_some()=>{
                for expired in host.confirmations.expire(tokio::time::Instant::now()) {
                    if let confirmation::Challenge::Legacy(id)=expired.challenge {send_reply!(&output,&confirmation::legacy_cancel(id));send_reply!(&output,&expired.call.reply.complete(error("Confirmation expired; nothing was changed"),false));}
                }
            },
            result=&mut output_worker=>{output_result=Some(result);break;},
            next=input.next()=>{
                let frame=match next {Ok(Some(frame))=>frame,Ok(None)=>break,Err(error)=>{fatal=Some(error);break;}};
                if frame.iter().all(u8::is_ascii_whitespace){continue;}
                let value:Value=match serde_json::from_slice(&frame){Ok(value)=>value,Err(_)=>{send_reply!(&output,&rpc_error(Value::Null,-32700,"Parse error"));continue;}};
                if value["jsonrpc"]=="2.0" && value.get("id").is_none() && value["method"]=="notifications/cancelled" {
                    if let Some(id)=value.pointer("/params/requestId") {for removed in host.confirmations.cancel(&Admissions::continuation_key(frontend,id)){if let confirmation::Challenge::Legacy(server_id)=removed.challenge{send_reply!(&output,&confirmation::legacy_cancel(server_id));}}match host.runtime.notifications.cancel(id){Ok(true)=>continue,Ok(false)=>{},Err(reason)=>{fatal=Some(io::Error::other(reason));break;}}host.admissions.cancel(frontend,id);}
                    continue;
                }
                let legacy_response=host.runtime.confirm_live && value.get("method").is_none() && value.get("id").is_some() && (value.get("result").is_some()||value.get("error").is_some());
                let (prepared,answers)=if legacy_response {
                    match host.confirmations.take_legacy_for(&value,tokio::time::Instant::now(),|call|call.frontend==frontend){Some((call,answer))=>(call,Some(answer)),None=>continue}
                }else{ 'prepare:{
                let admitted=protocol.admit(&value,&frame);
                if let Err(reason)=host.resource_scope(frontend,protocol.is_legacy()){fatal=Some(io::Error::other(reason));break 'frontend;}
                let (reply,name,args,resource)=match admitted {
                    Admission::Reply(response)=>{send_reply!(&output,&response);continue 'frontend;},
                    Admission::Initialized=>continue 'frontend,
                    Admission::List(reply)=>{let current=dynamic.as_ref().map_or_else(||manifest.clone(),|state|confirmed_manifest(state.manifest().clone(),host.runtime.confirm_live));send_reply!(&output,&reply.complete(current,true));continue 'frontend;},
                    Admission::Listen{reply,notifications,valid_filter}=>{if let Err(error)=crate::resource_notifications::flush(&mut host.runtime.notifications,&output){fatal=Some(error);break 'frontend;}match crate::resource_notifications::listen(&host.runtime.notifications,&reply.owned(),notifications,valid_filter,&output){Ok(Some(result))=>send_reply!(&output,&result),Ok(None)=>{},Err(error)=>{fatal=Some(error);break 'frontend;}}continue 'frontend;},
                    Admission::ResourceSubscription{reply,uri,add}=>{
                        if host.admissions.check(frontend,&value["id"],Class::Ordinary).is_err(){send_reply!(&output,&rpc_error(value["id"].clone(),-32000,"Native request admission unavailable; live state retained"));continue 'frontend;}
                        if let Err(error)=crate::resource_notifications::flush(&mut host.runtime.notifications,&output){fatal=Some(error);break 'frontend;}
                        let reserved=match crate::resource_notifications::uri(&host.runtime.notifications,&reply.owned(),uri.as_deref().map_err(String::as_str),add,&output){Ok(reserved)=>reserved,Err(error)=>{fatal=Some(error);break 'frontend;}};
                        let cancelled=Arc::new(AtomicBool::new(false));let key=host.admissions.admit(frontend,&value["id"],Class::Ordinary,Pending{cancel:Cancellation::new(),frontend_cancelled:cancelled.clone(),media:None,reserved_stop:false,reserved_safety_stop:false}).expect("checked local admission without yield");local_replies.push_back((key,cancelled,reserved));continue 'frontend;
                    },
                    Admission::ResourceList(reply)=>(reply.owned(),"native-resource-list".to_owned(),json!({}),true),
                    Admission::ResourceCompletion {reply,prefix}=>(reply.owned(),"native-resource-completion".to_owned(),json!({"prefix":prefix}),true),
                    Admission::ResourceRead {reply,uri:"obs://take/current"}=>{
                        if host.runtime.owner.is_none(){send_reply!(&output,&reply.complete(json!({"contents":[{"uri":"obs://take/current","mimeType":"application/json","text":"{\n  \"running\": false\n}"}]}),true));continue 'frontend;}
                        (reply.owned(),"native-take-resource".to_owned(),json!({}),true)
                    },
                    Admission::ResourceRead {reply,uri:"obs://status"}=>(reply.owned(),"obs-observe".to_owned(),json!({}),true),
                    Admission::ResourceRead {reply,uri}=>{if let Err(reason)=resource_request(uri) {send_reply!(&output,&reply.owned().error(reason.code,&reason.message));continue 'frontend;} (reply.owned(),"native-resource".to_owned(),json!({"uri":uri}),true)},
                    Admission::Call {reply,name,arguments}=>(reply.owned(),name.to_owned(),arguments,false),
                    Admission::Compatibility=>{if let Some(id)=value.get("id"){send_reply!(&output,&rpc_error(id.clone(),-32601,"Native capability unavailable; session state retained and SDK handoff disabled"));}continue 'frontend;},
                };
                // A retry never reconstructs its original typed command. Verify
                // only its envelope binding, consume once, then reconcile fresh
                // visibility/capacity/policy on the retained original below.
                if host.runtime.confirm_live && (value.pointer("/params/requestState").is_some()||value.pointer("/params/inputResponses").is_some()) {
                    match host.resume(frontend,reply,&name,&value,protocol.legacy_name()) {Ok((original,response))=>break 'prepare (original,Some(response)),Err(response)=>{send_reply!(&output,&response);continue 'frontend;}}

                }
                // Both reserved Stop lanes belong to the server, not each client.
                if host.admissions.check(frontend,&value["id"],Class::for_tool(&name,resource)).is_err() {send_reply!(&output,&rpc_error(value["id"].clone(),-32000,"Native request admission unavailable; live state retained"));continue 'frontend;}
                if !resource && let Some((code,message))=dynamic.as_ref().and_then(|state|state.refusal(&name)){send_reply!(&output,&rpc_error(value["id"].clone(),code,&message));continue 'frontend;}
                let controller=dynamic.is_some()&&dynamic_tools::controller(&name);
                let selected=dynamic.as_ref().map_or_else(||filter.includes(&name),|state|state.includes(&name));
                if !resource && !host.runtime.profile.allows(&name) {send_reply!(&output,&rpc_error(value["id"].clone(),-32601,if host.runtime.profile==NativeProfile::Advanced{"Tool is unavailable in the native advanced profile: this profile never constructs Take, media, Stage or recovery owners"}else{"Tool is unavailable in the native production profile: explicit advanced profile is required for manual recording starts/toggles and arbitrary named outputs"}));continue 'frontend;}
                if !resource && !selected {send_reply!(&output,&rpc_error(value["id"].clone(),-32602,&format!("Tool {name} not found")));continue 'frontend;}
                if !resource && !controller&&!owned(&name,host.runtime.host.is_some(),host.runtime.media.is_some(),host.runtime.profile) {send_reply!(&output,&rpc_error(value["id"].clone(),-32601,"Tool is unavailable in the native-only qualification lane"));continue 'frontend;}
                match host.prepare(frontend,native_host::Incoming{reply,name,args,resource,controller,value:&value,frame:&frame,legacy_name:protocol.legacy_name(),legacy_capabilities:protocol.legacy_capabilities()}){Ok(prepared)=>(prepared,None),Err(response)=>{send_reply!(&output,&response);continue 'frontend;}}
                }};
                // Reconcile capacity/visibility after waiting without borrowing an effect owner.
                if host.admissions.check(frontend,prepared.reply.id(),Class::for_tool(&prepared.name,prepared.resource)).is_err(){send_reply!(&output,&prepared.reply.error(-32000,"Native request admission unavailable; live state retained"));continue;}
                if !prepared.resource && let Some((code,message))=dynamic.as_ref().and_then(|state|state.refusal(&prepared.name)){send_reply!(&output,&prepared.reply.error(code,&message));continue;}
                let selected=dynamic.as_ref().map_or_else(||filter.includes(&prepared.name),|state|state.includes(&prepared.name));
                if !prepared.resource&&!selected {send_reply!(&output,&prepared.reply.error(-32602,&format!("Tool {} not found",prepared.name)));continue;}
                if !prepared.resource&&!prepared.controller&&!owned(&prepared.name,host.runtime.host.is_some(),host.runtime.media.is_some(),host.runtime.profile){send_reply!(&output,&prepared.reply.error(-32601,"Tool is unavailable in the native-only qualification lane"));continue;}
                let invocation=match host.authorize(prepared,answers.as_ref()) {native_host::Authorization::Ok(invocation)=>invocation,native_host::Authorization::Err(response)=>{send_reply!(&output,&response);continue;}};
                if invocation.is_controller() {
                        let (reply,name,args)=invocation.into_controller();
                        if let Err(error)=crate::resource_notifications::flush(&mut host.runtime.notifications,&output){fatal=Some(error);break;}
                        let targets=match host.runtime.notifications.tool_targets(){Ok(targets)=>targets,Err(reason)=>{fatal=Some(io::Error::other(reason));break;}};
                        let cancelled=Arc::new(AtomicBool::new(false));
                        let reserved=match dynamic.as_mut().unwrap().call(&name,&args,&reply,&output,&targets){Ok(reserved)=>reserved,Err(error)=>{fatal=Some(error);break;}};
                        let key=host.admissions.admit(frontend,reply.id(),Class::Ordinary,Pending{cancel:Cancellation::new(),frontend_cancelled:cancelled.clone(),media:None,reserved_stop:false,reserved_safety_stop:false}).expect("checked controller admission without yield");
                        local_replies.push_back((key,cancelled,reserved));continue;
                }
                match host.dispatch(invocation).await {Ok(Some(response))=>send_reply!(&output,&response),Ok(None)=>{},Err(error)=>{fatal=Some(error);break;}}

            },
            event=host.runtime.notifications.next()=>{
                match event{Some(Ok(batch))=>if let Err(error)=crate::resource_notifications::publish(batch,&output){fatal=Some(error);break;},Some(Err(reason))=>{fatal=Some(io::Error::other(reason));break;},None=>{fatal=Some(io::Error::other("Native resource notification owner ended"));break;}}
            },
            result=async{host.runtime.initialization.as_mut().expect("checked initialization").join_next().await},if host.runtime.initialization.is_some()=>{
                if let Err(error)=host.runtime.accept_initialization(result){fatal=Some(error);break;}
            },
            result=host.jobs.join_next(),if !host.jobs.is_empty()=>{
                match result {Some(Ok(completed))=>{let finished=host.complete(completed).expect("retained completion admission");debug_assert_eq!(finished.frontend,frontend);if let Some(response)=finished.response && let Err(error)=output.try_send(&response){fatal=Some(error);break;}},_=>{fatal=Some(io::Error::other("Native frontend worker failed; state was not handed off"));break;}}
            },
            ()=tokio::task::yield_now(),if !local_replies.is_empty()=>{
                let (key,cancelled,reserved)=local_replies.pop_front().unwrap();host.admissions.retire(key);
                if !cancelled.load(Ordering::Acquire){let ticket=reserved.commit();if matches!(ticket.progress().stage,crate::stdio_output::Stage::Failed(_)){fatal=Some(io::Error::other("Dynamic controller reply writer failed"));break;}}
            },
        }
    }
    // Uncommitted canceled/EOF controller replies own queue permits. Release
    // them before dropping the final sender; committed notifications stay owned.
    local_replies.clear();
    // Actual Source stdin EOF closes the registry without synthetic listener
    // completion frames. Committed notification cursors remain with the writer.
    drop(dynamic.take());
    host.runtime.notifications.close();
    if let Err(error) =
        crate::resource_notifications::flush(&mut host.runtime.notifications, &output)
    {
        fatal.get_or_insert(error);
    }

    // Only the server lifetime shuts down the shared Service. A transport
    // frontend departure itself cancels/suppresses its own retained responses.
    host.disconnect(frontend);
    drop(output);
    let (shutdown, output_joined) = tokio::join!(host.shutdown(), async {
        match output_result {
            Some(result) => result,
            None => output_worker.await,
        }
    });
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
    admitted_at: tokio::time::Instant,
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
    if name == "obs-save-replay-verified" {
        return service
            .save_replay_verified(
                obs_mcp_native::replay_save::Request::parse(&args)
                    .expect("validated replay arguments"),
                caller,
                cancel,
                admitted_at,
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
    #[tokio::test]
    async fn unconfirmed_initialization_retains_media_namespace_through_runtime_drop() {
        let dir = native_host::support::Dir::new();
        let work = MediaWork::new(&dir.0).unwrap();
        let path = work.path.clone();
        let mut runtime = Runtime {
            profile: NativeProfile::Production,
            notifications: obs_mcp_native::resource_events::Owner::new(false),
            owner: None,
            initialization: None,
            initialization_broken: true,
            network: None,
            tls_ca: None,
            url: "ws://unused-fixture.invalid:4455".into(),
            password: None,
            root: dir.0.clone(),
            host: None,
            control: Arc::new(Mutex::new(Default::default())),
            clock: native_host::support::clock(),
            preparation: None,
            media: None,
            replay: None,
            media_work: Some(work),
            screenshot_limit: Ok(ScreenshotLimit::default()),
            confirm_live: false,
        };
        assert!(runtime.shutdown().await.is_err());
        assert!(path.is_dir());
        drop(runtime);
        assert!(path.is_dir());
        let normal = MediaWork::new(&dir.0).unwrap();
        let normal_path = normal.path.clone();
        drop(normal);
        assert!(!normal_path.exists());
    }
    #[tokio::test]
    async fn retained_controller_reply_ownership_consumes_ordinary_admission_until_retired() {
        use crate::stdio_output::{Cycle, Output};
        let (output, worker) = Output::spawn(tokio::io::sink());
        let mut ledger = Admissions::for_test();
        let frontend = ledger.frontend().unwrap();
        let mut replies = VecDeque::<LocalReply>::new();
        let frame = json!({"jsonrpc":"2.0","id":1,"result":{}});
        let pending = || Pending {
            cancel: Cancellation::new(),
            frontend_cancelled: Arc::new(AtomicBool::new(false)),
            media: None,
            reserved_stop: false,
            reserved_safety_stop: false,
        };
        for n in 0..ORDINARY_FRONTEND_JOBS {
            let state = pending();
            let cancelled = state.frontend_cancelled.clone();
            let id = ledger
                .admit(frontend, &json!(n), Class::Ordinary, state)
                .unwrap();
            let reserved = output
                .reserve_publication(&[Cycle {
                    values: std::slice::from_ref(&frame),
                    repetitions: 1,
                }])
                .unwrap();
            replies.push_back((id, cancelled, reserved));
        }
        assert_eq!(ledger.ordinary(), ORDINARY_FRONTEND_JOBS);
        ledger.cancel(frontend, &json!(0));
        assert!(replies.front().unwrap().1.load(Ordering::Acquire));
        assert!(
            ledger
                .check(frontend, &json!(100), Class::Ordinary)
                .is_err()
        );
        let stop = ledger
            .admit(frontend, &json!(101), Class::TakeStop, pending())
            .unwrap();
        let safety = ledger
            .admit(frontend, &json!(102), Class::SafetyStop, pending())
            .unwrap();
        assert_eq!(ledger.ordinary(), ORDINARY_FRONTEND_JOBS);
        let (id, _, reservation) = replies.pop_front().unwrap();
        drop(reservation);
        ledger.retire(id);
        assert_eq!(ledger.ordinary(), ORDINARY_FRONTEND_JOBS - 1);
        for (id, _, reservation) in replies {
            drop(reservation);
            ledger.retire(id);
        }
        ledger.retire(stop);
        ledger.retire(safety);
        assert_eq!(ledger.ordinary(), 0);
        drop(output);
        assert!(worker.await.unwrap().is_ok());
    }

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
        let parent = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target");
        std::fs::create_dir_all(&parent).unwrap();
        let root = parent.join(format!("native-clock-failure-{}", std::process::id()));
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
