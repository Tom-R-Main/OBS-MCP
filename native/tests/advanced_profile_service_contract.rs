// SPDX-License-Identifier: MIT
#[path = "support/output_start_three.rs"]
mod support;
use obs_mcp_native::{
    generic_requests::Call,
    native_profile::NativeProfile,
    observation::{ReadRequest, Target},
    operation_store::{GenericKind, GenericPhase, OperationStore},
    service::{Options, PreparationOwners, Resources, Service, ServiceError},
    stage_transport::{CallerBinding, Cancellation},
    takes::{Policy, StartOptions},
};
use serde_json::{Value, json};
use std::time::Duration;
use support::*;
fn dir() -> Dir {
    std::fs::create_dir_all(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target"))
        .unwrap();
    Dir::new()
}
async fn advanced(socket: &Socket, dir: &Dir) -> Service {
    socket.caps.lock().unwrap().extend(
        ["ToggleRecord", "StartOutput", "StopOutput", "ToggleOutput"]
            .into_iter()
            .map(str::to_owned),
    );
    let (session, bridge) = socket.connect().await;
    let mut n = 0;
    let service = Service::spawn_in_profile(
        Resources {
            session,
            bridge,
            store: OperationStore::new(&dir.0, &socket.url).unwrap(),
            control: control(),
            clock: clock(),
            identities: Box::new(move || {
                n += 1;
                id(n)
            }),
        },
        Options::default(),
        NativeProfile::Advanced,
    )
    .map_err(|(error, _)| error)
    .unwrap();
    service
        .handle()
        .read(ReadRequest::Version, Value::Null, WAIT)
        .await
        .unwrap();
    service
}

#[tokio::test]
async fn advanced_actual_service_denies_guarded_apis_and_owns_no_automatic_record_stop() {
    let socket = Socket::new().await;
    let dir = dir();
    let mut service = advanced(&socket, &dir).await;
    let handle = service.handle();
    assert_eq!(handle.native_profile(), NativeProfile::Advanced);
    assert!(matches!(
        handle
            .start_take(StartOptions::default(), Policy::default())
            .await,
        Err(ServiceError::Profile)
    ));
    assert!(matches!(
        handle
            .stage(
                Target {
                    scene_collection: "Fixture".into(),
                    scene_name: "Scene".into(),
                    canvas_uuid: None
                },
                CallerBinding::resolve(Default::default(), None, false, None, None),
                Cancellation::new()
            )
            .await,
        Err(ServiceError::Profile)
    ));
    let started = handle
        .generic(
            call("obs-start-record"),
            "fixture".into(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        started["structuredContent"]["message"],
        "Recording started, writing to: /fixture/record.mkv"
    );
    assert_eq!(dir.receipt(1)["phase"], "accepted");
    assert_eq!(handle.profile_metadata()["guardedReentryAllowed"], false);
    assert!(
        !OperationStore::new(&dir.0, &socket.url)
            .unwrap()
            .read_view()
            .admission()
            .unwrap()
            .blocking
    );
    let independent = handle
        .generic(
            Call::parse("obs-stop-output", &json!({"outputName":"recording alias"})).unwrap(),
            "fixture".into(),
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert!(
        !independent
            .get("isError")
            .is_some_and(|value| value == true)
    );
    assert!(service.shutdown().await.unwrap().joined);
    assert_eq!(socket.count("StartRecord"), 1);
    assert_eq!(socket.count("StopRecord"), 0);
    assert_eq!(socket.count("StopOutput"), 1);
    socket.join().await;
}

#[tokio::test]
async fn production_service_rejects_closed_advanced_calls_even_without_frontend_filter() {
    let socket = Socket::new().await;
    let dir = dir();
    let mut service = socket.service(&dir, Duration::from_secs(30)).await;
    for name in [
        "obs-start-record",
        "obs-toggle-record",
        "obs-start-output",
        "obs-stop-output",
        "obs-toggle-output",
    ] {
        let call = Call::parse(name, &json!({"outputName":"recording alias"})).unwrap();
        assert!(matches!(
            service
                .handle()
                .generic(call, "fixture".into(), Cancellation::new())
                .await,
            Err(ServiceError::Profile)
        ));
    }
    assert_eq!(
        std::fs::read_dir(dir.0.join("operation-receipts"))
            .map(|entries| entries.count())
            .unwrap_or(0),
        0
    );
    assert!(service.shutdown().await.unwrap().joined);
    assert_eq!(socket.count("StartRecord"), 0);
    assert_eq!(socket.count("StopOutput"), 0);
    socket.join().await;
}

#[tokio::test]
async fn known_advanced_ack_refuses_new_guarded_binding_without_relabeling_receipt_unknown() {
    let socket = Socket::new().await;
    let dir = dir();
    let mut store = OperationStore::new(&dir.0, &socket.url).unwrap();
    let prepared = store
        .prepare_generic(
            &id(99),
            GenericKind::NamedOutputToggle,
            1,
            "2026-10-31T12:00:00.000Z",
        )
        .unwrap();
    let sent = store
        .update_generic(
            &prepared,
            GenericPhase::Dispatching,
            "2026-10-31T12:00:00.000Z",
        )
        .unwrap();
    store
        .update_generic(&sent, GenericPhase::Accepted, "2026-10-31T12:00:00.000Z")
        .unwrap();
    drop(store);
    let (session, bridge) = socket.connect().await;
    let result = Service::spawn(
        Resources {
            session,
            bridge,
            store: OperationStore::new(&dir.0, &socket.url).unwrap(),
            control: control(),
            clock: clock(),
            identities: Box::new(|| id(1)),
        },
        Options {
            preparation: Some(PreparationOwners::new(&dir.0).unwrap()),
            ..Default::default()
        },
    );
    let Err((error, resources)) = result else {
        panic!("guarded binding must be refused")
    };
    assert_eq!(error, ServiceError::Profile);
    assert_eq!(dir.receipt(99)["phase"], "accepted");
    assert!(!resources.store.read_view().admission().unwrap().blocking);
    resources.bridge.shutdown().await.unwrap();
    resources.session.shutdown().await.unwrap();
    socket.join().await;
}
