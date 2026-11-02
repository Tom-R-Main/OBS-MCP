// SPDX-License-Identifier: GPL-2.0-only
//! Library-only Clip ownership through the actual Service/Session actors.
//! Fake OBS and explicit host seam; no live capture or public route.
#[path = "support/output_start_three.rs"]
mod support;
use obs_mcp_native::{
    control::ControlDomain,
    custody::SharedControl,
    operation_store::OperationStore,
    preflight::{self, Check, Report, Status},
    record_clip::Request,
    service::{
        ConnectionDescriptor, Options, Resources, Service, ServiceError, ServiceHandle,
        TakeServices,
    },
    stage_transport::Cancellation,
    take_monitor::Summary,
    take_transport::{Bounds, HostServices, PreflightReader},
    takes::{ClipCheck, IoFuture, Limits, Policy, StartOptions},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use tokio::{
    sync::Notify,
    time::{Instant, sleep, timeout},
};
#[derive(Default)]
struct Host {
    preflights: Mutex<Vec<preflight::Options>>,
    refuse: AtomicBool,
    hold: AtomicBool,
    entered: AtomicBool,
    release: Arc<Notify>,
    inspected: AtomicUsize,
}
impl HostServices for Host {
    fn preflight(&self, _: PreflightReader, options: preflight::Options) -> IoFuture<Report> {
        self.preflights.lock().unwrap().push(options);
        let refuse = self.refuse.load(Ordering::Acquire);
        let held = self.hold.load(Ordering::Acquire);
        let release = self.release.clone();
        self.entered.store(true, Ordering::Release);
        Box::pin(async move {
            if held {
                release.notified().await;
            }
            Report {
                ready: !refuse,
                checks: vec![
                    Check {
                        id: "record-directory".into(),
                        status: Status::Unknown,
                        message: "remote fixture".into(),
                    },
                    Check {
                        id: "audio".into(),
                        status: if refuse { Status::Fail } else { Status::Pass },
                        message: if refuse {
                            "audible fixture"
                        } else {
                            "quiet fixture"
                        }
                        .into(),
                    },
                ],
            }
        })
    }
    fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
        self.inspected.fetch_add(1, Ordering::AcqRel);
        Box::pin(async { panic!("remote clip inspected a file") })
    }
    fn write_log(&self, _: String, _: Summary) -> IoFuture<Option<String>> {
        Box::pin(async { panic!("remote clip wrote a file") })
    }
    fn join_observers(&self) -> IoFuture<Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}
struct Fixture {
    socket: Socket,
    _dir: Dir,
    service: Service,
    control: SharedControl,
    host: Arc<Host>,
}
impl Fixture {
    async fn new() -> Self {
        Self::configured(false).await
    }
    async fn configured(media_enabled: bool) -> Self {
        let socket = Socket::new().await;
        socket
            .caps
            .lock()
            .unwrap()
            .insert("CreateRecordChapter".into());
        let dir = Dir::new();
        let mut media = None;
        #[cfg(unix)]
        if media_enabled {
            use std::{fs, os::unix::fs::PermissionsExt};
            let root = dir.0.join("recordings");
            let work = dir.0.join("media-work");
            let helper = dir.0.join("media-helper");
            fs::create_dir(&root).unwrap();
            fs::create_dir(&work).unwrap();
            fs::set_permissions(&work, fs::Permissions::from_mode(0o700)).unwrap();
            fs::write(root.join("source.mkv"), b"fixture source").unwrap();
            fs::write(&helper, b"#!/bin/sh\nexit 1\n").unwrap();
            fs::set_permissions(&helper, fs::Permissions::from_mode(0o700)).unwrap();
            media = Some(Arc::new(
                obs_mcp_native::media_tools::Config::new(&root, &helper, &helper, &work).unwrap(),
            ));
            socket
                .caps
                .lock()
                .unwrap()
                .insert("GetRecordDirectory".into());
            socket.plan(
                "GetRecordDirectory",
                Plan {
                    response: json!({"recordDirectory":root}),
                    ..Default::default()
                },
            );
        }
        let (session, bridge) = socket.connect().await;
        let control = Arc::new(Mutex::new(ControlDomain::default()));
        let host = Arc::new(Host::default());
        let mut n = 0;
        let service = Service::spawn(
            Resources {
                session,
                bridge,
                store: OperationStore::new(&dir.0, &socket.url).unwrap(),
                control: control.clone(),
                clock: clock(),
                identities: Box::new(move || {
                    n += 1;
                    format!("{n:08x}-0000-4000-8000-{n:012x}")
                }),
            },
            Options {
                operation_timeout: Duration::from_secs(3),
                media,
                take: Some(TakeServices {
                    host: host.clone(),
                    exact_endpoint: socket.url.clone(),
                    pending_settings: Arc::new(AtomicBool::new(false)),
                    bounds: Bounds::default(),
                    limits: Limits {
                        start_confirmation: Duration::from_millis(40),
                        setup: Duration::from_secs(3),
                        stop: Duration::from_millis(200),
                        inspection: Duration::from_millis(200),
                        ..Default::default()
                    },
                }),
                connection: Some(ConnectionDescriptor {
                    url: socket.url.clone(),
                    has_password: false,
                }),
                state_root: Some(dir.0.clone()),
                ..Default::default()
            },
        )
        .map_err(|(e, _)| e)
        .unwrap();
        Self {
            socket,
            _dir: dir,
            service,
            control,
            host,
        }
    }
    fn handle(&self) -> ServiceHandle {
        self.service.handle()
    }
    async fn running(&self) {
        let h = self.handle();
        timeout(WAIT, async {
            loop {
                if h.take_resource().await.unwrap()["running"] == true
                    && !h.take_status().await.unwrap()["structuredContent"]["takeId"].is_null()
                {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn close(mut self) {
        let r = timeout(WAIT, self.service.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(r.joined && r.observer_joined && r.socket_joined && r.workers_joined);
        self.socket.join().await;
    }
}
fn policy() -> Policy {
    Policy {
        caller: "fixture".into(),
        ..Default::default()
    }
}
fn request(seconds: f64, chapters: Value) -> Request {
    Request::parse(&json!({"durationSeconds":seconds,"chapters":chapters})).unwrap()
}
#[tokio::test]
async fn fractional_clip_core_preserves_preexisting_lease_and_source_defaults() {
    let f = Fixture::new().await;
    f.control
        .lock()
        .unwrap()
        .claim("fixture", 7, "preexisting", EPOCH)
        .unwrap();
    let before =
        serde_json::to_value(f.control.lock().unwrap().active_lease(EPOCH).unwrap()).unwrap();
    let result = f
        .handle()
        .record_clip(request(0.05, json!([])), policy(), Cancellation::new())
        .await
        .unwrap();
    assert_eq!(
        result["structuredContent"]["stopConfirmed"], true,
        "{result}"
    );
    assert_eq!(
        (f.socket.count("StartRecord"), f.socket.count("StopRecord")),
        (1, 1)
    );
    assert_eq!(
        serde_json::to_value(f.control.lock().unwrap().active_lease(EPOCH)).unwrap(),
        before,
        "Clip must preserve the preexisting control lease without claiming or releasing it"
    );
    {
        let p = f.host.preflights.lock().unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].min_free_disk_mb, 1024.0);
        assert!(!p[0].expect_silent && !p[0].require_verified);
    }
    f.close().await;
}
#[tokio::test]
async fn public_start_still_claims_and_releases_its_240_minute_lease() {
    let f = Fixture::new().await;
    let h = f.handle();
    let result = h
        .start_take(StartOptions::default(), policy())
        .await
        .unwrap();
    assert_ne!(result["isError"], true, "{result}");
    assert_eq!(
        f.control
            .lock()
            .unwrap()
            .active_lease(EPOCH)
            .unwrap()
            .expires_at,
        EPOCH + 240 * 60 * 1000
    );
    assert_eq!(
        h.stop_take(None, policy()).await.unwrap()["structuredContent"]["stopConfirmed"],
        true
    );
    assert!(f.control.lock().unwrap().active_lease(EPOCH).is_none());
    f.close().await;
}
#[tokio::test]
async fn late_chapter_refusal_precedes_preflight_and_all_effects() {
    let f = Fixture::new().await;
    let result = f
        .handle()
        .record_clip(
            request(
                0.5,
                json!([{"atSeconds":1,"name":"late"},{"atSeconds":0.5,"name":"equal"}]),
            ),
            policy(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        result["content"][0]["text"],
        "Not recording: chapter(s) \"late\", \"equal\" start at or after the 0.5s clip ends"
    );
    assert!(f.host.preflights.lock().unwrap().is_empty());
    assert_eq!(
        (f.socket.count("StartRecord"), f.socket.count("StopRecord")),
        (0, 0)
    );
    f.close().await;
}
#[tokio::test]
async fn preflight_failure_never_enters_clip_stop() {
    let f = Fixture::new().await;
    f.host.refuse.store(true, Ordering::Release);
    let result = f
        .handle()
        .record_clip(request(0.1, json!([])), policy(), Cancellation::new())
        .await
        .unwrap();
    assert_eq!(result["isError"], true);
    assert!(result.to_string().contains("audible fixture"));
    assert_eq!(
        (f.socket.count("StartRecord"), f.socket.count("StopRecord")),
        (0, 0)
    );
    f.close().await;
}
#[tokio::test]
async fn existing_take_refusal_never_stops_or_replaces_it() {
    let f = Fixture::new().await;
    let h = f.handle();
    let old = h
        .start_take(StartOptions::default(), policy())
        .await
        .unwrap();
    let refused = h
        .record_clip(request(0.1, json!([])), policy(), Cancellation::new())
        .await
        .unwrap();
    assert!(
        refused.to_string().contains("already recording"),
        "{refused}"
    );
    assert_eq!(
        (f.socket.count("StartRecord"), f.socket.count("StopRecord")),
        (1, 0)
    );
    assert_eq!(
        h.take_status().await.unwrap()["structuredContent"]["takeId"],
        old["structuredContent"]["takeId"]
    );
    h.stop_take(None, policy()).await.unwrap();
    f.close().await;
}
#[tokio::test]
async fn chapter_rejection_becomes_note_and_recording_continues() {
    let f = Fixture::new().await;
    f.socket.plan(
        "CreateRecordChapter",
        Plan {
            rejected: true,
            ..Default::default()
        },
    );
    let result = f
        .handle()
        .record_clip(
            request(0.1, json!([{"atSeconds":0,"name":"chapter"}])),
            policy(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        result["structuredContent"]["stopConfirmed"], true,
        "{result}"
    );
    assert!(
        result["structuredContent"]["chapterNotes"][0]
            .as_str()
            .unwrap()
            .contains("not added")
    );
    assert_eq!(f.socket.count("StopRecord"), 1);
    f.close().await;
}
#[tokio::test]
async fn held_chapter_does_not_restart_final_wait_and_equal_times_keep_order() {
    let f = Fixture::new().await;
    f.socket.plan(
        "CreateRecordChapter",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let h = f.handle();
    let job = tokio::spawn(async move {
        h.record_clip(request(1.0,json!([{"atSeconds":0,"name":"zero"},{"atSeconds":0.1,"name":"a"},{"atSeconds":0.1,"name":"b"}])),policy(),Cancellation::new()).await
    });
    f.socket.seen("CreateRecordChapter", 1).await;
    sleep(Duration::from_millis(1100)).await;
    f.socket.release("CreateRecordChapter").await;
    let result = timeout(Duration::from_millis(700), job)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        result["structuredContent"]["stopConfirmed"], true,
        "{result}"
    );
    let names = f
        .socket
        .trace
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r["d"]["requestType"] == "CreateRecordChapter")
        .map(|r| r["d"]["requestData"]["chapterName"].clone())
        .collect::<Vec<_>>();
    assert_eq!(names, vec![json!("zero"), json!("a"), json!("b")]);
    f.close().await;
}
#[tokio::test]
async fn public_stop_and_status_remain_reachable_during_long_clip() {
    let f = Fixture::new().await;
    let h = f.handle();
    let cancel = Cancellation::new();
    let signal = cancel.clone();
    let job = tokio::spawn(async move {
        h.record_clip(request(50.0, json!([])), policy(), signal)
            .await
    });
    f.running().await;
    let h = f.handle();
    assert_ne!(
        timeout(Duration::from_millis(500), h.take_status())
            .await
            .unwrap()
            .unwrap()["isError"],
        true
    );
    assert_eq!(
        timeout(Duration::from_millis(700), h.stop_take(None, policy()))
            .await
            .unwrap()
            .unwrap()["structuredContent"]["stopConfirmed"],
        true
    );
    cancel.cancel();
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    assert_eq!(result["structuredContent"]["stopConfirmed"], true);
    assert_eq!(f.socket.count("StopRecord"), 1);
    f.close().await;
}
#[tokio::test]
async fn cancellation_wakes_long_sleep_and_skips_future_chapters() {
    let f = Fixture::new().await;
    let h = f.handle();
    let cancel = Cancellation::new();
    let signal = cancel.clone();
    let job = tokio::spawn(async move {
        h.record_clip(
            request(50.0, json!([{"atSeconds":49,"name":"late"}])),
            policy(),
            signal,
        )
        .await
    });
    f.running().await;
    cancel.cancel();
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    assert_eq!(
        result["structuredContent"]["stopConfirmed"], true,
        "{result}"
    );
    assert_eq!(
        (
            f.socket.count("CreateRecordChapter"),
            f.socket.count("StopRecord")
        ),
        (0, 1)
    );
    f.close().await;
}
#[tokio::test]
async fn service_shutdown_wakes_clip_before_joining_its_take_job() {
    let mut f = Fixture::new().await;
    let h = f.handle();
    let job = tokio::spawn(async move {
        h.record_clip(request(50.0, json!([])), policy(), Cancellation::new())
            .await
    });
    f.running().await;
    let receipt = timeout(WAIT, f.service.shutdown()).await.unwrap().unwrap();
    assert!(receipt.joined && receipt.stop_confirmed);
    assert_eq!(
        timeout(WAIT, job).await.unwrap().unwrap().unwrap()["structuredContent"]["stopConfirmed"],
        true
    );
    assert_eq!(f.socket.count("StopRecord"), 1);
    f.socket.join().await;
}
#[tokio::test]
async fn old_clip_cannot_mark_stop_inspect_or_unlock_replacement_take() {
    let f = Fixture::new().await;
    let h = f.handle();
    let job = tokio::spawn(async move {
        h.record_clip(
            request(0.6, json!([{"atSeconds":0.4,"name":"old clip"}])),
            policy(),
            Cancellation::new(),
        )
        .await
    });
    f.running().await;
    let h = f.handle();
    let stopped = h.stop_take(None, policy()).await.unwrap();
    let replacement = h
        .start_take(StartOptions::default(), policy())
        .await
        .unwrap();
    assert_ne!(replacement["isError"], true, "{replacement}");
    let id = replacement["structuredContent"]["takeId"].clone();
    assert_ne!(id, stopped["structuredContent"]["takeId"]);
    let lease =
        serde_json::to_value(f.control.lock().unwrap().active_lease(EPOCH).unwrap()).unwrap();
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    assert_eq!(
        result["structuredContent"]["takeId"],
        stopped["structuredContent"]["takeId"]
    );
    assert_eq!(
        (
            f.socket.count("CreateRecordChapter"),
            f.socket.count("StopRecord")
        ),
        (0, 1)
    );
    assert_eq!(
        h.take_status().await.unwrap()["structuredContent"]["takeId"],
        id
    );
    assert_eq!(
        serde_json::to_value(f.control.lock().unwrap().active_lease(EPOCH).unwrap()).unwrap(),
        lease
    );
    assert!(
        result["structuredContent"]["chapterNotes"][0]
            .as_str()
            .unwrap()
            .contains("ownership")
    );
    {
        use obs_mcp_native::control::{Disposition, GateRequest, RefusalKind};
        let lock = f.control.lock().unwrap().evaluate(
            GateRequest {
                tool_name: "obs-set-profile-parameter",
                arguments: &json!({"parameterName":"RecFormat"}),
                read_only_hint: false,
                caller: "fixture",
                confirmation: Default::default(),
            },
            EPOCH,
        );
        assert!(
            matches!(
                lock,
                Disposition::Refused {
                    kind: RefusalKind::TakeLock,
                    ..
                }
            ),
            "{lock:?}"
        );
    }
    assert_eq!(f.host.inspected.load(Ordering::Acquire), 0);
    h.stop_take(None, policy()).await.unwrap();
    f.close().await;
}
#[tokio::test]
async fn clip_origin_is_after_monitor_setup_instead_of_start_ack() {
    let f = Fixture::new().await;
    f.socket.plan(
        "GetCurrentProgramScene",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let h = f.handle();
    let job = tokio::spawn(async move {
        h.record_clip(request(0.2, json!([])), policy(), Cancellation::new())
            .await
    });
    f.socket.seen("GetCurrentProgramScene", 1).await;
    sleep(Duration::from_millis(300)).await;
    assert_eq!(f.socket.count("StopRecord"), 0);
    let released = Instant::now();
    f.socket.release("GetCurrentProgramScene").await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    assert!(released.elapsed() >= Duration::from_millis(150));
    assert_eq!(result["structuredContent"]["stopConfirmed"], true);
    f.close().await;
}
#[tokio::test]
async fn canceled_preflight_is_drained_without_start_or_clip_stop() {
    let f = Fixture::new().await;
    f.host.hold.store(true, Ordering::Release);
    let h = f.handle();
    let cancel = Cancellation::new();
    let signal = cancel.clone();
    let job = tokio::spawn(async move {
        h.record_clip(request(1.0, json!([])), policy(), signal)
            .await
    });
    timeout(WAIT, async {
        while !f.host.entered.load(Ordering::Acquire) {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    assert!(!job.is_finished());
    f.host.release.notify_one();
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(
        (f.socket.count("StartRecord"), f.socket.count("StopRecord")),
        (0, 0)
    );
    f.close().await;
}
#[tokio::test]
async fn original_clip_ticket_is_not_recaptured_after_held_bind_and_manual_ack() {
    let f = Fixture::new().await;
    f.socket.plan(
        "GetVersion",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let h = f.handle();
    let job = tokio::spawn(async move {
        h.record_clip(request(0.1, json!([])), policy(), Cancellation::new())
            .await
    });
    f.socket.seen("GetVersion", 1).await;
    let manual = f
        .handle()
        .manual_record_stop(policy(), Cancellation::new())
        .await
        .unwrap();
    assert_ne!(manual["isError"], true, "{manual}");
    f.socket.release("GetVersion").await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(
        (f.socket.count("StartRecord"), f.socket.count("StopRecord")),
        (0, 1)
    );
    assert!(f.host.preflights.lock().unwrap().is_empty());
    assert_eq!(f.host.inspected.load(Ordering::Acquire), 0);
    f.close().await;
}

#[tokio::test]
async fn rejected_core_start_is_returned_without_clip_stop_or_monitor_setup() {
    let f = Fixture::new().await;
    f.socket.plan(
        "StartRecord",
        Plan {
            rejected: true,
            ..Default::default()
        },
    );
    let result = f
        .handle()
        .record_clip(request(0.1, json!([])), policy(), Cancellation::new())
        .await
        .unwrap();
    assert_eq!(result["isError"], true, "{result}");
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("Recording failed to start:"),
        "{result}"
    );
    assert_eq!(
        (
            f.socket.count("StartRecord"),
            f.socket.count("StopRecord"),
            f.socket.count("GetCurrentProgramScene")
        ),
        (1, 0, 0)
    );
    // This fixture emits a Start event even with the rejection. That cannot
    // authorize Clip cleanup: core start did not produce an owned Take.
    f.close().await;
}
#[tokio::test]
async fn canceled_held_chapter_is_joined_before_same_take_stop_without_replay() {
    let f = Fixture::new().await;
    f.socket.plan(
        "CreateRecordChapter",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let h = f.handle();
    let cancel = Cancellation::new();
    let signal = cancel.clone();
    let job = tokio::spawn(async move {
        h.record_clip(
            request(
                50.0,
                json!([{"atSeconds":0,"name":"held"},{"atSeconds":1,"name":"skipped"}]),
            ),
            policy(),
            signal,
        )
        .await
    });
    f.socket.seen("CreateRecordChapter", 1).await;
    cancel.cancel();
    sleep(Duration::from_millis(50)).await;
    assert!(!job.is_finished());
    assert_eq!(f.socket.count("StopRecord"), 0);
    f.socket.release("CreateRecordChapter").await;
    let result = timeout(WAIT, job).await.unwrap().unwrap().unwrap();
    assert_eq!(
        result["structuredContent"]["stopConfirmed"], true,
        "{result}"
    );
    assert_eq!(
        (
            f.socket.count("CreateRecordChapter"),
            f.socket.count("StopRecord")
        ),
        (1, 1)
    );
    f.close().await;
}
#[tokio::test]
async fn shutdown_joins_held_stop_ack_without_replaying_or_abandoning_it() {
    let mut f = Fixture::new().await;
    f.socket.plan(
        "StopRecord",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let h = f.handle();
    let clip = tokio::spawn(async move {
        h.record_clip(request(0.05, json!([])), policy(), Cancellation::new())
            .await
    });
    f.socket.seen("StopRecord", 1).await;
    let shutdown = f.service.shutdown();
    tokio::pin!(shutdown);
    assert!(
        timeout(Duration::from_millis(50), &mut shutdown)
            .await
            .is_err()
    );
    assert!(!clip.is_finished());
    assert_eq!(f.socket.count("StopRecord"), 1);
    f.socket.release("StopRecord").await;
    let receipt = timeout(WAIT, &mut shutdown).await.unwrap().unwrap();
    assert!(receipt.joined && receipt.stop_confirmed);
    assert_eq!(
        timeout(WAIT, clip).await.unwrap().unwrap().unwrap()["structuredContent"]["stopConfirmed"],
        true
    );
    assert_eq!(f.socket.count("StopRecord"), 1);
    f.socket.join().await;
}
#[tokio::test]
async fn canceled_monitor_setup_drains_before_owned_cleanup_and_skips_schedule() {
    let f = Fixture::new().await;
    f.socket.plan(
        "GetCurrentProgramScene",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let h = f.handle();
    let cancel = Cancellation::new();
    let signal = cancel.clone();
    let clip = tokio::spawn(async move {
        h.record_clip(
            request(50.0, json!([{"atSeconds":0,"name":"never scheduled"}])),
            policy(),
            signal,
        )
        .await
    });
    f.socket.seen("GetCurrentProgramScene", 1).await;
    cancel.cancel();
    sleep(Duration::from_millis(50)).await;
    assert!(!clip.is_finished());
    assert_eq!(f.socket.count("StopRecord"), 0);
    f.socket.release("GetCurrentProgramScene").await;
    let result = timeout(WAIT, clip).await.unwrap().unwrap().unwrap();
    assert_eq!(result["isError"], true, "{result}");
    assert_eq!(
        result["structuredContent"]["stopConfirmed"], true,
        "{result}"
    );
    assert_eq!(
        (
            f.socket.count("StartRecord"),
            f.socket.count("StopRecord"),
            f.socket.count("CreateRecordChapter")
        ),
        (1, 1, 0)
    );
    f.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn active_media_refuses_clip_before_preflight_and_start() {
    use obs_mcp_native::{
        media_tools::Request as MediaRequest, media_verification::Cancellation as MediaCancel,
    };
    let f = Fixture::configured(true).await;
    f.socket.plan(
        "GetRecordDirectory",
        Plan {
            held: true,
            response: json!({"recordDirectory":f._dir.0.join("recordings")}),
            ..Default::default()
        },
    );
    let h = f.handle();
    let cancel = MediaCancel::default();
    let signal = cancel.clone();
    let media = tokio::spawn(async move {
        h.recording_media(
            MediaRequest::parse("obs-contact-sheet", &json!({"path":"source.mkv"})).unwrap(),
            signal,
        )
        .await
    });
    f.socket.seen("GetRecordDirectory", 1).await;
    let result = f
        .handle()
        .record_clip(request(0.1, json!([])), policy(), Cancellation::new())
        .await;
    cancel.cancel();
    f.socket.release("GetRecordDirectory").await;
    assert!(timeout(WAIT, media).await.unwrap().unwrap().is_ok());
    assert!(matches!(result, Err(ServiceError::Busy)), "{result:?}");
    assert!(f.host.preflights.lock().unwrap().is_empty());
    assert_eq!(f.socket.count("StartRecord"), 0);
    assert_eq!(
        std::fs::read(f._dir.0.join("recordings/source.mkv")).unwrap(),
        b"fixture source"
    );
    f.close().await;
}
#[cfg(unix)]
#[tokio::test]
async fn held_clip_binding_keeps_media_excluded_through_startup_handoff() {
    use obs_mcp_native::{
        media_tools::Request as MediaRequest, media_verification::Cancellation as MediaCancel,
    };
    let f = Fixture::configured(true).await;
    f.socket.plan(
        "GetVersion",
        Plan {
            held: true,
            ..Default::default()
        },
    );
    let h = f.handle();
    let cancel = Cancellation::new();
    let signal = cancel.clone();
    let clip = tokio::spawn(async move {
        h.record_clip(request(50.0, json!([])), policy(), signal)
            .await
    });
    f.socket.seen("GetVersion", 1).await;
    let during = f
        .handle()
        .recording_media(
            MediaRequest::parse("obs-contact-sheet", &json!({"path":"source.mkv"})).unwrap(),
            MediaCancel::default(),
        )
        .await;
    assert!(matches!(during, Err(ServiceError::Busy)), "{during:?}");
    assert_eq!(f.socket.count("GetRecordDirectory"), 0);
    f.socket.release("GetVersion").await;
    f.running().await;
    let running = f
        .handle()
        .recording_media(
            MediaRequest::parse("obs-contact-sheet", &json!({"path":"source.mkv"})).unwrap(),
            MediaCancel::default(),
        )
        .await;
    assert!(matches!(running, Err(ServiceError::Busy)), "{running:?}");
    assert_eq!(f.socket.count("GetRecordDirectory"), 0);
    cancel.cancel();
    assert_eq!(
        timeout(WAIT, clip).await.unwrap().unwrap().unwrap()["structuredContent"]["stopConfirmed"],
        true
    );
    f.close().await;
}

#[tokio::test]
async fn abandoned_frontend_does_not_abandon_clip_and_service_shutdown_joins_it() {
    let mut f = Fixture::new().await;
    let h = f.handle();
    let frontend = tokio::spawn(async move {
        h.record_clip(request(50.0, json!([])), policy(), Cancellation::new())
            .await
    });
    f.running().await;
    frontend.abort();
    assert!(frontend.await.unwrap_err().is_cancelled());
    let receipt = timeout(WAIT, f.service.shutdown()).await.unwrap().unwrap();
    assert!(receipt.joined && receipt.stop_confirmed);
    assert_eq!(f.socket.count("StopRecord"), 1);
    f.socket.join().await;
}

#[tokio::test]
async fn standalone_take_actor_shutdown_wakes_its_retained_clip_and_joins_stop() {
    use obs_mcp_native::{
        take_transport::BoundTakeIo,
        takes::{Loan, TakeSupervisor},
    };
    let socket = Socket::new().await;
    socket
        .caps
        .lock()
        .unwrap()
        .insert("CreateRecordChapter".into());
    let dir = Dir::new();
    let (session, bridge) = socket.connect().await;
    let mut feed = bridge.attach_take_feed().unwrap();
    let host = Arc::new(Host::default());
    let io = BoundTakeIo::bind(
        session.handle(),
        Arc::new(feed.fence()),
        host,
        &socket.url,
        Arc::new(AtomicBool::new(false)),
        Bounds::default(),
    )
    .await
    .unwrap();
    let mut serial = 0;
    let mut owner = TakeSupervisor::spawn(
        Loan {
            session: session.handle(),
            store: OperationStore::new(&dir.0, &socket.url).unwrap(),
            control: control(),
            clock: clock(),
            identities: Box::new(move || {
                serial += 1;
                format!("{serial:08x}-0000-4000-8000-{serial:012x}")
            }),
            io: Arc::new(io),
            events: feed.take_receiver().unwrap(),
        },
        Limits {
            setup: Duration::from_secs(3),
            stop: Duration::from_millis(200),
            start_confirmation: Duration::from_millis(40),
            ..Default::default()
        },
    )
    .map_err(|failure| failure.reason)
    .unwrap();
    let h = owner.handle();
    let clip = tokio::spawn(async move {
        h.record_clip(
            request(
                50.0,
                json!([{"atSeconds":0,"name":"ready"},{"atSeconds":49,"name":"skipped"}]),
            ),
            policy(),
            Cancellation::new(),
        )
        .await
    });
    socket.seen("CreateRecordChapter", 1).await;
    let receipt = timeout(WAIT, owner.shutdown()).await.unwrap().unwrap();
    assert!(receipt.joined && receipt.observers_joined && receipt.stop_confirmed);
    assert_eq!(
        timeout(WAIT, clip).await.unwrap().unwrap().unwrap()["structuredContent"]["stopConfirmed"],
        true
    );
    assert_eq!(
        (
            socket.count("CreateRecordChapter"),
            socket.count("StopRecord")
        ),
        (1, 1)
    );
    drop(owner);
    drop(feed);
    bridge.shutdown().await.unwrap();
    session.shutdown().await.unwrap();
    socket.join().await;
}
