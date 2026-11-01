// SPDX-License-Identifier: GPL-2.0-only
#[path = "support/output_start_three.rs"]
mod support;
use futures_util::poll;
use obs_mcp_native::{
    generic_requests::Call, service::ServiceError, stage_transport::Cancellation,
};
use serde_json::json;
use std::time::Duration;
use support::*;
use tokio::time::{Instant, sleep, timeout};
#[tokio::test]
async fn service_uses_actual_cached_capabilities_and_all_three_starts_share_one_session() {
    let socket = Socket::new().await;
    let dir = Dir::new();
    for status in [
        "GetStreamStatus",
        "GetVirtualCamStatus",
        "GetReplayBufferStatus",
    ] {
        socket.caps.lock().unwrap().remove(status);
    }
    let mut service = socket.service(&dir, Duration::from_secs(30)).await;
    let handle = service.handle();
    for (n, (name, request, _, _)) in STARTS.into_iter().enumerate() {
        let result = handle
            .generic(call(name), "fixture".into(), Cancellation::new())
            .await
            .unwrap();
        assert!(result.get("isError").is_none(), "{result}");
        assert_eq!(socket.count(request), 1);
        assert_eq!(dir.receipt(n as u64 + 1)["phase"], "accepted");
    }
    assert_eq!(socket.trace.lock().unwrap().len(), 4);
    assert_eq!(
        handle.handoff().await.unwrap_err(),
        ServiceError::RetainedState
    );
    let receipt = service.shutdown().await.unwrap();
    assert!(
        receipt.joined
            && receipt.socket_joined
            && receipt.observer_joined
            && receipt.workers_joined
    );
    socket.join().await;
}
#[tokio::test]
async fn dropped_frontend_retains_start_ack_scope_until_join_and_public_stop_progresses() {
    for (name, request, _, _) in STARTS {
        let socket = Socket::new().await;
        let dir = Dir::new();
        socket.plan(
            request,
            Plan {
                held: true,
                ..Default::default()
            },
        );
        let mut service = socket.service(&dir, Duration::from_secs(30)).await;
        let handle = service.handle();
        let front = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle
                    .generic(call(name), "fixture".into(), Cancellation::new())
                    .await
            }
        });
        socket.seen(request, 1).await;
        front.abort();
        let _ = front.await;
        assert_eq!(handle.handoff().await.unwrap_err(), ServiceError::Busy);
        let stop = timeout(
            WAIT,
            handle.generic(
                call("obs-stop-stream"),
                "foreign".into(),
                Cancellation::new(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(stop.get("isError").is_none(), "{stop}");
        assert_eq!(socket.count("StopStream"), 1);
        assert!(socket.held.lock().unwrap().contains_key(request));
        let mut shutdown = Box::pin(service.shutdown());
        assert!(poll!(&mut shutdown).is_pending());
        socket.release(request).await;
        let receipt = timeout(WAIT, shutdown).await.unwrap().unwrap();
        assert!(
            receipt.joined
                && receipt.workers_joined
                && receipt.observer_joined
                && receipt.socket_joined
        );
        assert_eq!(dir.receipt(1)["phase"], "accepted");
        assert_eq!(dir.receipt(1)["postconditionObserved"], false);
        assert_eq!(socket.count(request), 1);
        assert_eq!(socket.count("StopRecord"), 0);
        socket.join().await;
    }
}
#[tokio::test]
async fn queued_cancelled_start_has_no_intent_and_short_original_rpc_deadline_stays_unknown() {
    let socket = Socket::new().await;
    let dir = Dir::new();
    socket.plan(
        "SetInputMute",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let mut service = socket.service(&dir, Duration::from_secs(30)).await;
    let handle = service.handle();
    let mute = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .generic(
                    Call::parse(
                        "obs-set-input-mute",
                        &json!({"inputName":"Fixture","inputMuted":true}),
                    )
                    .unwrap(),
                    "fixture".into(),
                    Cancellation::new(),
                )
                .await
        }
    });
    socket.seen("SetInputMute", 1).await;
    let cancel = Cancellation::new();
    let start = tokio::spawn({
        let handle = handle.clone();
        let cancel = cancel.clone();
        async move {
            handle
                .generic(call("obs-start-stream"), "fixture".into(), cancel)
                .await
        }
    });
    timeout(WAIT, async {
        while handle.summary().await.unwrap()["queuedCommands"] != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    socket.release("SetInputMute").await;
    let mute_result = mute.await.unwrap().unwrap();
    assert!(mute_result.get("isError").is_none());
    let result = start.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(socket.count("StartStream"), 0);
    assert!(!dir.path(2).exists());
    assert!(service.shutdown().await.unwrap().joined);
    socket.join().await;
    let socket = Socket::new().await;
    let dir = Dir::new();
    socket.plan(
        "StartReplayBuffer",
        Plan {
            states: vec![],
            held: true,
            ..Default::default()
        },
    );
    let mut service = socket.service(&dir, Duration::from_millis(220)).await;
    let handle = service.handle();
    let at = Instant::now();
    let result = handle
        .generic(
            call("obs-start-replay-buffer"),
            "fixture".into(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert!(at.elapsed() >= Duration::from_millis(220));
    assert_eq!(result["isError"], true);
    assert_eq!(dir.receipt(1)["phase"], "unknown");
    assert_eq!(socket.count("StartReplayBuffer"), 1);
    assert!(service.shutdown().await.unwrap().workers_joined);
    socket.join().await;
}
#[tokio::test]
async fn original_stream_job_can_confirm_after_ten_seconds_and_short_kind_can_fallback_after_ten() {
    let streaming = async {
        let socket = Socket::new().await;
        let dir = Dir::new();
        socket.plan(
            "StartStream",
            Plan {
                states: vec![],
                ..Default::default()
            },
        );
        let mut service = socket.service(&dir, Duration::from_secs(30)).await;
        let handle = service.handle();
        let at = Instant::now();
        let start = tokio::spawn(async move {
            handle
                .generic(
                    call("obs-start-stream"),
                    "fixture".into(),
                    Cancellation::new(),
                )
                .await
        });
        socket.seen("StartStream", 1).await;
        sleep(Duration::from_millis(10_200)).await;
        assert!(
            !start.is_finished(),
            "old blanket10s job bound must not retire stream confirmation"
        );
        socket
            .emit(
                "StreamStateChanged",
                json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STARTED"}),
            )
            .await;
        let result = timeout(WAIT, start).await.unwrap().unwrap().unwrap();
        assert!(at.elapsed() >= Duration::from_secs(10));
        assert_eq!(result["content"][0]["text"], "Streaming started");
        assert_eq!(socket.count("GetStreamStatus"), 0);
        assert_eq!(dir.receipt(1)["phase"], "accepted");
        assert!(service.shutdown().await.unwrap().joined);
        socket.join().await;
    };
    let virtual_cam = async {
        let socket = Socket::new().await;
        let dir = Dir::new();
        socket.plan(
            "StartVirtualCam",
            Plan {
                states: vec![json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"})],
                held: true,
                ..Default::default()
            },
        );
        socket.plan(
            "GetVirtualCamStatus",
            Plan {
                held: true,
                ..Default::default()
            },
        );
        let mut service = socket.service(&dir, Duration::from_secs(30)).await;
        let handle = service.handle();
        let at = Instant::now();
        let start = tokio::spawn(async move {
            handle
                .generic(
                    call("obs-start-virtual-cam"),
                    "fixture".into(),
                    Cancellation::new(),
                )
                .await
        });
        socket.seen("StartVirtualCam", 1).await;
        sleep(Duration::from_millis(6_000)).await;
        socket.release("StartVirtualCam").await;
        socket.seen("GetVirtualCamStatus", 1).await;
        sleep(Duration::from_millis(4_200)).await;
        assert!(
            !start.is_finished(),
            "fallback needs original25s job budget, each RPC still≤10s"
        );
        socket.release("GetVirtualCamStatus").await;
        let result = timeout(WAIT, start).await.unwrap().unwrap().unwrap();
        assert!(at.elapsed() >= Duration::from_secs(10));
        assert_eq!(result["content"][0]["text"], "Virtual camera started");
        assert_eq!(socket.count("StartVirtualCam"), 1);
        assert_eq!(socket.count("GetVirtualCamStatus"), 1);
        assert_eq!(dir.receipt(1)["phase"], "accepted");
        assert!(service.shutdown().await.unwrap().joined);
        socket.join().await;
    };
    tokio::join!(streaming, virtual_cam);
}
#[tokio::test]
async fn accepted_unconfirmed_does_not_become_unknown_and_missing_start_capability_has_zero_intent()
{
    let socket = Socket::new().await;
    let dir = Dir::new();
    socket.caps.lock().unwrap().remove("GetReplayBufferStatus");
    socket.caps.lock().unwrap().remove("StartVirtualCam");
    socket.plan(
        "StartReplayBuffer",
        Plan {
            states: vec![json!({"outputState":"OBS_WEBSOCKET_OUTPUT_STOPPED"})],
            ..Default::default()
        },
    );
    let mut service = socket.service(&dir, Duration::from_secs(30)).await;
    let handle = service.handle();
    let result = handle
        .generic(
            call("obs-start-replay-buffer"),
            "fixture".into(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(dir.receipt(1)["phase"], "accepted");
    assert_eq!(socket.count("GetReplayBufferStatus"), 0);
    let result = handle
        .generic(
            call("obs-start-virtual-cam"),
            "fixture".into(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("Virtual camera failed to start: OBS WebSocket does not advertise")
    );
    assert_eq!(socket.count("StartVirtualCam"), 0);
    assert!(!dir.path(2).exists());
    assert!(service.shutdown().await.unwrap().joined);
    socket.join().await;
}

#[tokio::test]
async fn continuously_owned_take_deadline_physically_stops_during_held_virtual_cam_start_ack() {
    use obs_mcp_native::{
        preflight::{self, Report},
        service::TakeServices,
        take_monitor::Summary,
        take_transport::{Bounds, HostServices, PreflightReader},
        takes::{ClipCheck, IoFuture, Limits, Policy, StartOptions},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Host(AtomicBool);
    impl HostServices for Host {
        fn preflight(&self, _: PreflightReader, _: preflight::Options) -> IoFuture<Report> {
            Box::pin(async {
                Report {
                    ready: true,
                    checks: vec![preflight::Check {
                        id: "record-directory".into(),
                        status: preflight::Status::Pass,
                        message: "injected host fixture".into(),
                    }],
                }
            })
        }
        fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
            Box::pin(async {
                ClipCheck {
                    duration_seconds: Some(1.0),
                    video_streams: Some(1),
                    audio_streams: Some(1),
                    ..Default::default()
                }
            })
        }
        fn write_log(&self, _: String, _: Summary) -> IoFuture<Option<String>> {
            Box::pin(async { None })
        }
        fn join_observers(&self) -> IoFuture<Result<(), String>> {
            self.0.store(true, Ordering::Release);
            Box::pin(async { Ok(()) })
        }
    }
    let socket = Socket::new().await;
    let dir = Dir::new();
    let host = Arc::new(Host(AtomicBool::new(false)));
    let take = TakeServices {
        host: host.clone(),
        exact_endpoint: socket.url.clone(),
        pending_settings: Arc::new(AtomicBool::new(false)),
        bounds: Bounds::default(),
        limits: Limits {
            inspection: Duration::from_millis(50),
            ..Default::default()
        },
    };
    let mut service = socket
        .service_with_take(&dir, Duration::from_secs(30), Some(take))
        .await;
    let handle = service.handle();
    let started = handle
        .start_take(
            StartOptions {
                max_duration_seconds: Some(2),
                expect_silent: true,
                ..Default::default()
            },
            Policy {
                caller: "fixture".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(started.get("isError").is_none(), "{started}");
    assert_eq!(
        started["structuredContent"]["ownership"], "continuous",
        "{started}"
    );
    assert_eq!(
        socket.count("StopRecord"),
        0,
        "setup must precede own deadline"
    );
    socket.plan(
        "StartVirtualCam",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let stream = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .generic(
                    call("obs-start-virtual-cam"),
                    "fixture".into(),
                    Cancellation::new(),
                )
                .await
        }
    });
    socket.seen("StartVirtualCam", 1).await;
    assert_eq!(
        socket.count("StopRecord"),
        0,
        "physical virtual camera must be held before deadline Stop"
    );
    socket.seen("StopRecord", 1).await;
    assert!(socket.held.lock().unwrap().contains_key("StartVirtualCam"));
    assert!(!stream.is_finished());
    socket.release("StartVirtualCam").await;
    let result = stream.await.unwrap().unwrap();
    assert!(result.get("isError").is_none(), "{result}");
    let stopped = handle
        .stop_take(
            None,
            Policy {
                caller: "fixture".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        stopped["structuredContent"]["stopConfirmed"], true,
        "{stopped}"
    );
    assert_eq!(
        stopped["structuredContent"]["receiptStatus"], "observed",
        "{stopped}"
    );
    let receipt = service.shutdown().await.unwrap();
    assert!(
        receipt.joined && receipt.take_joined && receipt.workers_joined && receipt.socket_joined
    );
    assert!(host.0.load(Ordering::Acquire));
    assert_eq!(socket.count("StartRecord"), 1);
    assert_eq!(socket.count("StopRecord"), 1);
    assert_eq!(socket.count("StartVirtualCam"), 1);
    socket.join().await;
}

#[tokio::test]
async fn queued_start_keeps_original_admission_deadline_through_held_take_initialization() {
    use obs_mcp_native::{
        preflight::{self, Report},
        service::TakeServices,
        take_monitor::Summary,
        take_transport::{Bounds, HostServices, PreflightReader},
        takes::{ClipCheck, IoFuture, Limits, Policy, StartOptions},
    };
    use std::sync::{Arc, Mutex, atomic::AtomicBool};
    use tokio::sync::oneshot;
    struct Host(Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>);
    impl HostServices for Host {
        fn preflight(&self, _: PreflightReader, _: preflight::Options) -> IoFuture<Report> {
            let gate = self.0.lock().unwrap().take();
            Box::pin(async move {
                if let Some((entered, release)) = gate {
                    let _ = entered.send(());
                    let _ = release.await;
                }
                Report {
                    ready: true,
                    checks: vec![preflight::Check {
                        id: "record-directory".into(),
                        status: preflight::Status::Pass,
                        message: "held injected preflight".into(),
                    }],
                }
            })
        }
        fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
            Box::pin(async {
                ClipCheck {
                    duration_seconds: Some(1.0),
                    video_streams: Some(1),
                    audio_streams: Some(1),
                    ..Default::default()
                }
            })
        }
        fn write_log(&self, _: String, _: Summary) -> IoFuture<Option<String>> {
            Box::pin(async { None })
        }
        fn join_observers(&self) -> IoFuture<Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
    }
    let socket = Socket::new().await;
    let dir = Dir::new();
    let (entered, seen) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let take = TakeServices {
        host: Arc::new(Host(Mutex::new(Some((entered, resume))))),
        exact_endpoint: socket.url.clone(),
        pending_settings: Arc::new(AtomicBool::new(false)),
        bounds: Bounds::default(),
        limits: Limits {
            inspection: Duration::from_millis(50),
            ..Default::default()
        },
    };
    let mut service = socket
        .service_with_take(&dir, Duration::from_millis(500), Some(take))
        .await;
    let handle = service.handle();
    let taking = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .start_take(
                    StartOptions::default(),
                    Policy {
                        caller: "fixture".into(),
                        ..Default::default()
                    },
                )
                .await
        }
    });
    timeout(WAIT, seen).await.unwrap().unwrap();
    let queued = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .generic(
                    call("obs-start-virtual-cam"),
                    "fixture".into(),
                    Cancellation::new(),
                )
                .await
        }
    });
    timeout(WAIT, async {
        while handle.summary().await.unwrap()["queuedCommands"] != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    sleep(Duration::from_millis(650)).await;
    assert!(!queued.is_finished());
    release.send(()).unwrap();
    let take_result = taking.await.unwrap().unwrap();
    assert!(take_result.get("isError").is_none(), "{take_result}");
    let result = queued.await.unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("cancelled or expired before dispatch")
    );
    assert_eq!(socket.count("StartVirtualCam"), 0);
    assert_eq!(handle.summary().await.unwrap()["total"], 1);
    let stopped = handle
        .stop_take(
            None,
            Policy {
                caller: "fixture".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        stopped["structuredContent"]["stopConfirmed"], true,
        "{stopped}"
    );
    assert!(service.shutdown().await.unwrap().joined);
    socket.join().await;
}

#[derive(Default)]
struct SeparationHost;
impl obs_mcp_native::take_transport::HostServices for SeparationHost {
    fn preflight(
        &self,
        _: obs_mcp_native::take_transport::PreflightReader,
        _: obs_mcp_native::preflight::Options,
    ) -> obs_mcp_native::takes::IoFuture<obs_mcp_native::preflight::Report> {
        Box::pin(async {
            obs_mcp_native::preflight::Report {
                ready: true,
                checks: vec![obs_mcp_native::preflight::Check {
                    id: "record-directory".into(),
                    status: obs_mcp_native::preflight::Status::Pass,
                    message: "fixture host".into(),
                }],
            }
        })
    }
    fn inspect(
        &self,
        _: String,
        _: bool,
    ) -> obs_mcp_native::takes::IoFuture<obs_mcp_native::takes::ClipCheck> {
        Box::pin(async {
            obs_mcp_native::takes::ClipCheck {
                skipped: Some("fixture media".into()),
                ..Default::default()
            }
        })
    }
    fn write_log(
        &self,
        _: String,
        _: obs_mcp_native::take_monitor::Summary,
    ) -> obs_mcp_native::takes::IoFuture<Option<String>> {
        Box::pin(async { None })
    }
    fn join_observers(&self) -> obs_mcp_native::takes::IoFuture<Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}
fn separation_take(socket: &Socket) -> obs_mcp_native::service::TakeServices {
    obs_mcp_native::service::TakeServices {
        host: std::sync::Arc::new(SeparationHost),
        exact_endpoint: socket.url.clone(),
        pending_settings: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        bounds: Default::default(),
        limits: obs_mcp_native::takes::Limits {
            inspection: Duration::from_millis(50),
            ..Default::default()
        },
    }
}
fn separation_media(dir: &Dir) -> std::sync::Arc<obs_mcp_native::media_tools::Config> {
    let root = dir.0.join("media-root");
    let work = dir.0.join("media-work");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&work).unwrap();
    let executable = dir.0.join("media-fixture");
    std::fs::write(&executable, b"#!/bin/sh\nexit 73\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for path in [&root, &work, &executable] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
    std::sync::Arc::new(
        obs_mcp_native::media_tools::Config::new(&root, &executable, &executable, &work).unwrap(),
    )
}
const STREAM_CONTROLS: [&str; 3] = ["obs-start-stream", "obs-stop-stream", "obs-toggle-stream"];
#[tokio::test]
async fn immutable_stream_separation_refuses_before_capability_intent_and_send_for_each_constructor()
 {
    let mut observed = Vec::new();
    let mut invariants = Vec::new();
    for (take, media) in [(true, false), (false, true), (true, true)] {
        for advertised in [true, false] {
            let socket = Socket::new().await;
            let dir = Dir::new();
            for kind in ["StartStream", "StopStream", "ToggleStream"] {
                if advertised {
                    socket.caps.lock().unwrap().insert(kind.into());
                } else {
                    socket.caps.lock().unwrap().remove(kind);
                }
            }
            let mut service = socket
                .service_with_features(
                    &dir,
                    WAIT,
                    take.then(|| separation_take(&socket)),
                    media.then(|| separation_media(&dir)),
                )
                .await;
            let handle = service.handle();
            let before = socket.trace.lock().unwrap().len();
            for name in STREAM_CONTROLS {
                observed.push((
                    take,
                    media,
                    advertised,
                    name,
                    handle
                        .generic(call(name), "fixture".into(), Cancellation::new())
                        .await,
                ));
            }
            let after = socket.trace.lock().unwrap().len();
            let total = handle.summary().await.unwrap()["total"].clone();
            let status = handle
                .read(
                    obs_mcp_native::observation::ReadRequest::StreamStatus,
                    serde_json::Value::Null,
                    WAIT,
                )
                .await
                .unwrap();
            assert!(status.get("outputActive").is_some());
            let receipt = service.shutdown().await.unwrap();
            assert!(receipt.joined && receipt.workers_joined && receipt.socket_joined);
            socket.join().await;
            eprintln!(
                "constructor take={take} media={media} advertised={advertised}: RPCs before={before} after={after}, receipts={total}"
            );
            invariants.push((take, media, advertised, before, after, total));
        }
    }
    for (take, media, advertised, before, after, total) in invariants {
        assert_eq!(
            before, after,
            "take={take} media={media} advertised={advertised}: refused Stream controls must not request capabilities/status or mutate OBS"
        );
        assert_eq!(total, 0, "no refusal may allocate receipt intent");
    }
    for (take, media, advertised, name, result) in observed {
        assert!(
            matches!(result, Err(ServiceError::Unsupported)),
            "constructor take={take} media={media} advertised={advertised}, {name}: {result:?}"
        );
    }
}
#[tokio::test]
async fn immutable_stream_separation_stays_closed_after_take_options_are_consumed_and_take_retires()
{
    use obs_mcp_native::takes::{Policy, StartOptions};
    let socket = Socket::new().await;
    socket.caps.lock().unwrap().insert("ToggleStream".into());
    let dir = Dir::new();
    let mut service = socket
        .service_with_take(&dir, WAIT, Some(separation_take(&socket)))
        .await;
    let handle = service.handle();
    let mut refused = Vec::new();
    let policy = || Policy {
        caller: "fixture".into(),
        ..Default::default()
    };
    for name in STREAM_CONTROLS {
        refused.push(
            handle
                .generic(call(name), "fixture".into(), Cancellation::new())
                .await,
        );
    }
    let started = handle
        .start_take(
            StartOptions {
                expect_silent: true,
                ..Default::default()
            },
            policy(),
        )
        .await
        .unwrap();
    assert!(started.get("isError").is_none(), "{started}");
    let stopped = handle.stop_take(None, policy()).await.unwrap();
    assert_eq!(stopped["structuredContent"]["stopConfirmed"], true);
    for name in STREAM_CONTROLS {
        refused.push(
            handle
                .generic(call(name), "fixture".into(), Cancellation::new())
                .await,
        );
    }
    let counts = [
        socket.count("StartStream"),
        socket.count("StopStream"),
        socket.count("ToggleStream"),
    ];
    let receipt = service.shutdown().await.unwrap();
    assert!(receipt.joined && receipt.take_joined && receipt.socket_joined);
    socket.join().await;
    assert_eq!(counts, [0, 0, 0]);
    assert!(
        refused
            .iter()
            .all(|result| matches!(result, Err(ServiceError::Unsupported)))
    );
}
#[tokio::test]
async fn immutable_stream_separation_allows_standalone_controls_even_with_manual_stop_api() {
    let socket = Socket::new().await;
    socket.caps.lock().unwrap().insert("ToggleStream".into());
    let dir = Dir::new();
    let mut service = socket.service(&dir, WAIT).await;
    let handle = service.handle();
    let manual = handle
        .manual_record_stop(
            obs_mcp_native::takes::Policy {
                caller: "fixture".into(),
                ..Default::default()
            },
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert!(manual.get("isError").is_none(), "{manual}");
    for name in STREAM_CONTROLS {
        let result = handle
            .generic(call(name), "fixture".into(), Cancellation::new())
            .await
            .unwrap();
        assert!(result.get("isError").is_none(), "{name}: {result}");
    }
    assert_eq!(socket.count("StartStream"), 1);
    assert_eq!(socket.count("StopStream"), 1);
    assert_eq!(socket.count("ToggleStream"), 1);
    assert_eq!(handle.summary().await.unwrap()["total"], 4);
    assert!(service.shutdown().await.unwrap().joined);
    socket.join().await;
}
