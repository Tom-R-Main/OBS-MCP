// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::stage_transport::Cancellation;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn monotonic_wait_handles_cancel_before_poll_and_wakes_every_waiter() {
    let signal = Cancellation::new();
    let before_poll = signal.cancelled();
    signal.cancel();
    timeout(Duration::from_secs(1), before_poll).await.unwrap();
    timeout(Duration::from_secs(1), signal.cancelled())
        .await
        .unwrap();

    let signal = Cancellation::new();
    let mut waiters = Vec::new();
    for _ in 0..3 {
        let signal = signal.clone();
        waiters.push(tokio::spawn(async move { signal.cancelled().await }));
    }
    tokio::task::yield_now().await;
    signal.cancel();
    for waiter in waiters {
        timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn abandoning_a_wait_or_another_clone_does_not_cancel_the_owner() {
    let signal = Cancellation::new();
    drop(signal.clone());
    assert!(
        timeout(Duration::from_millis(10), signal.cancelled())
            .await
            .is_err()
    );
    assert!(!signal.is_cancelled());
    signal.cancel();
    assert!(signal.is_cancelled());
    timeout(Duration::from_secs(1), signal.cancelled())
        .await
        .unwrap();
}
