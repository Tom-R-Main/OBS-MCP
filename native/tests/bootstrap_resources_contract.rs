// SPDX-License-Identifier: MIT
#[path = "support/output_start_three.rs"]
mod support;
use obs_mcp_native::{
    native_profile::NativeProfile,
    observation::ReadRequest,
    operation_store::OperationStore,
    service::{ConnectionDescriptor, Options, Resources, Service},
};
use serde_json::Value;
use std::time::Duration;
use tokio::time::Instant;
async fn resources(socket: &support::Socket, dir: &support::Dir) -> Resources {
    let (session, bridge) = socket.connect().await;
    Resources {
        session,
        bridge,
        store: OperationStore::new(&dir.0, &socket.url).unwrap(),
        control: support::control(),
        clock: support::clock(),
        identities: Box::new(|| support::id(1)),
    }
}
#[tokio::test]
async fn expired_original_connect_deadline_cannot_restart_the_capability_read_budget() {
    let socket = support::Socket::new().await;
    let dir = support::Dir::new();
    let resources = resources(&socket, &dir).await;
    let Err(resources) =
        Service::bootstrap_resources(resources, Instant::now() - Duration::from_secs(1)).await
    else {
        panic!("expired initialization was adopted")
    };
    assert_eq!(socket.count("GetVersion"), 0);
    resources.bridge.shutdown().await.unwrap();
    resources.session.shutdown().await.unwrap();
    socket.join().await;
}
#[tokio::test]
async fn bound_bootstrap_primes_the_same_private_cache_before_service_adoption_without_a_second_read()
 {
    let socket = support::Socket::new().await;
    socket.caps.lock().unwrap().remove("GetStats");
    let dir = support::Dir::new();
    let resources = resources(&socket, &dir).await;
    let bootstrap = match Service::bootstrap_resources(
        resources,
        Instant::now() + Duration::from_secs(3),
    )
    .await
    {
        Ok(bootstrap) => bootstrap,
        Err(_) => panic!("bootstrap failed"),
    };
    assert_eq!(socket.count("GetVersion"), 1);
    let mut service = Service::spawn_bootstrapped(
        bootstrap,
        Options {
            state_root: Some(dir.0.clone()),
            connection: Some(ConnectionDescriptor {
                url: socket.url.clone(),
                has_password: false,
            }),
            ..Default::default()
        },
        NativeProfile::Production,
    )
    .map_err(|(error, _)| error)
    .unwrap();
    assert!(
        service
            .handle()
            .read(ReadRequest::Stats, Value::Null, Duration::from_secs(3))
            .await
            .is_err()
    );
    assert_eq!(socket.count("GetStats"), 0);
    assert_eq!(socket.count("GetVersion"), 1);
    assert!(service.shutdown().await.unwrap().joined);
    socket.join().await;
}
