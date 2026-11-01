// SPDX-License-Identifier: GPL-2.0-only
//! Existing Service API; durable same-endpoint uncertainty is not cleared by
//! an inactive recording-status read. Fake media executable, no live OBS.
#[path = "support/output_start_three.rs"]
mod support;
use obs_mcp_native::{
    media_tools::{Config, Request},
    media_verification::Cancellation,
    operation_store::{
        GenericKind, GenericPhase, OperationKind, OperationStore, OperationUpdate, Phase,
        PrepareDetails,
    },
    service::ServiceError,
};
use serde_json::json;
use std::{fs, sync::Arc, time::Duration};
use support::*;
#[cfg(unix)]
fn media(dir: &Dir) -> Arc<Config> {
    use std::os::unix::fs::PermissionsExt;
    let root = dir.0.join("recordings");
    let work = dir.0.join("media-work");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&work).unwrap();
    fs::set_permissions(&work, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(root.join("source.mkv"), b"fixture source bytes").unwrap();
    let helper = dir.0.join("media-helper");
    fs::write(
        &helper,
        b"#!/bin/sh\nprintf invoked > \"$(dirname \"$0\")/spawned\"\nexit 1\n",
    )
    .unwrap();
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        std::process::Command::new(&helper).status().unwrap().code(),
        Some(1)
    );
    assert_eq!(fs::read(dir.0.join("spawned")).unwrap(), b"invoked");
    fs::remove_file(dir.0.join("spawned")).unwrap();
    Arc::new(Config::new(&root, &helper, &helper, &work).unwrap())
}
fn unknown(dir: &Dir, url: &str, generic: bool) {
    let mut store = OperationStore::new(&dir.0, url).unwrap();
    let utc = "2026-10-31T12:00:00.000Z";
    if generic {
        let r = store
            .prepare_generic(&id(800), GenericKind::InputMute, 1, utc)
            .unwrap();
        let r = store
            .update_generic(&r, GenericPhase::Dispatching, utc)
            .unwrap();
        store
            .update_generic(&r, GenericPhase::Unknown, utc)
            .unwrap();
    } else {
        let r = store
            .prepare(
                &id(800),
                OperationKind::RecordStart,
                1,
                utc,
                PrepareDetails {
                    take_id: Some("unknown-start"),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut dispatching = OperationUpdate::new(Phase::Dispatching);
        dispatching.request_dispatched = Some(None);
        let r = store.update(&r, dispatching, utc).unwrap();
        let mut unknown = OperationUpdate::new(Phase::Unknown);
        unknown.request_dispatched = Some(None);
        unknown.request_accepted = Some(None);
        store.update(&r, unknown, utc).unwrap();
    }
}
#[cfg(unix)]
async fn refuses(generic: bool) {
    let socket = Socket::new().await;
    socket
        .caps
        .lock()
        .unwrap()
        .insert("GetRecordDirectory".into());
    let dir = Dir::new();
    let config = media(&dir);
    unknown(&dir, &socket.url, generic);
    socket.plan(
        "GetRecordDirectory",
        Plan {
            response: json!({"recordDirectory":dir.0.join("recordings")}),
            ..Default::default()
        },
    );
    let mut service = socket
        .service_with_features(&dir, Duration::from_secs(3), None, Some(config))
        .await;
    let result = service
        .handle()
        .recording_media(
            Request::parse("obs-contact-sheet", &json!({"path":"source.mkv"})).unwrap(),
            Cancellation::default(),
        )
        .await;
    let status = socket.count("GetRecordStatus");
    let directory = socket.count("GetRecordDirectory");
    let spawned = dir.0.join("spawned").exists();
    let after = fs::read(dir.0.join("recordings/source.mkv")).unwrap();
    let joined = service.shutdown().await.unwrap();
    socket.join().await;
    assert!(
        joined.joined && joined.socket_joined && joined.observer_joined && joined.workers_joined
    );
    assert!(
        matches!(result, Err(ServiceError::Uncertain)),
        "same-endpoint unknown admitted media: {result:?}; status={status},directory={directory},spawned={spawned}"
    );
    assert_eq!((status, directory, spawned), (0, 0, false));
    assert_eq!(after, b"fixture source bytes");
    let saved = fs::read_to_string(
        dir.0
            .join("operation-receipts")
            .join(format!("{}.json", id(800))),
    )
    .unwrap();
    assert!(saved.contains("unknown"));
}
#[cfg(unix)]
#[tokio::test]
async fn critical_start_unknown_refuses_media_before_fresh_status_directory_or_process() {
    refuses(false).await;
}
#[cfg(unix)]
#[tokio::test]
async fn generic_unknown_refuses_media_before_fresh_status_directory_or_process() {
    refuses(true).await;
}
#[cfg(unix)]
#[tokio::test]
async fn foreign_endpoint_unknown_does_not_blanket_disable_current_endpoint_media() {
    let socket = Socket::new().await;
    socket
        .caps
        .lock()
        .unwrap()
        .insert("GetRecordDirectory".into());
    let dir = Dir::new();
    let config = media(&dir);
    unknown(&dir, "ws://127.0.0.1:1", false);
    socket.plan(
        "GetRecordDirectory",
        Plan {
            response: json!({"recordDirectory":dir.0.join("recordings")}),
            ..Default::default()
        },
    );
    let mut service = socket
        .service_with_features(&dir, Duration::from_secs(3), None, Some(config))
        .await;
    let result = service
        .handle()
        .recording_media(
            Request::parse("obs-contact-sheet", &json!({"path":"source.mkv"})).unwrap(),
            Cancellation::default(),
        )
        .await;
    let status = socket.count("GetRecordStatus");
    let directory = socket.count("GetRecordDirectory");
    let joined = service.shutdown().await.unwrap();
    socket.join().await;
    assert!(joined.joined);
    assert!(
        result.is_ok(),
        "foreign history blocked media admission: {result:?}"
    );
    assert_eq!((status, directory), (1, 1));
}
