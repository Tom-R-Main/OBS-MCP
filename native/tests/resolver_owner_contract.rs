// SPDX-License-Identifier: MIT
//! Durable owner/process controls. Fixed self-helper only; no external DNS or OBS.
#![cfg(feature = "resolver-fixture")]
use obs_mcp_resolver::{Error, ExpectedPeer, Fixture, MAX_JOBS, Owner, Receipt};
use std::{future::poll_fn, path::PathBuf, process::Stdio, task::Poll, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::{Instant, sleep, timeout},
};

const WAIT: Duration = Duration::from_secs(3);
const HELPER: &str = env!("CARGO_BIN_EXE_obs-mcp-native");

fn owner() -> Owner {
    Owner::fixture_with_executable(PathBuf::from(HELPER)).unwrap()
}
async fn ready(owner: &Owner, count: usize) -> bool {
    timeout(WAIT, async {
        loop {
            let jobs = owner.snapshots();
            if jobs.len() == count && jobs.iter().all(|j| j.spawned && j.fixture_ready) {
                return;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .is_ok()
}
async fn drain(owner: &mut Owner) -> Vec<Receipt> {
    timeout(WAIT, owner.shutdown()).await.unwrap().unwrap()
}
async fn assert_killed_and_reaped(receipts: &[Receipt], count: usize) {
    assert_eq!(receipts.len(), count);
    for receipt in receipts {
        assert!(
            receipt.fixture_ready,
            "the fixture never proved it was running"
        );
        assert!(receipt.kill_requested && receipt.joined && receipt.reaped);
        assert!(receipt.pid.is_some());
        #[cfg(unix)]
        {
            assert_eq!(receipt.exit_signal, Some(9));
            // Independent read-only observation: a zombie is still visible to ps.
            // We never signal a numeric PID or claim an exited parent reaped it.
            let observed = Command::new("/bin/ps")
                .args(["-o", "stat=", "-p", &receipt.pid.unwrap().to_string()])
                .kill_on_drop(true)
                .output();
            let observed = timeout(WAIT, observed).await.unwrap().unwrap();
            assert!(
                observed.stdout.is_empty(),
                "owned helper is still observable"
            );
            assert!(observed.stderr.is_empty());
        }
        #[cfg(windows)]
        {
            assert_eq!(receipt.exit_signal, None);
            assert!(receipt.exit_code.is_some());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn original_deadline_retires_a_ready_helper_before_result_publication() {
    let mut owner = owner();
    let deadline = Instant::now() + Duration::from_secs(2);
    let consumer = owner.fixture(Fixture::Stall, deadline).unwrap();
    let was_ready = ready(&owner, 1).await;
    let result = timeout(WAIT, consumer).await;
    let returned_after_deadline = Instant::now() >= deadline;
    let receipts = drain(&mut owner).await;
    assert!(
        was_ready,
        "helper readiness was absent at the original deadline: {receipts:?}"
    );
    assert!(matches!(result, Ok(Err(Error::Deadline))));
    assert!(returned_after_deadline);
    assert_eq!(owner.retained_jobs(), 0);
    assert_killed_and_reaped(&receipts, 1).await;
    assert_eq!(receipts[0].class, Err(Error::Deadline));
}

#[tokio::test(flavor = "current_thread")]
async fn abandoning_a_consumer_does_not_free_active_job_capacity() {
    let mut owner = owner();
    let deadline = Instant::now() + WAIT;
    let first = owner.fixture(Fixture::Stall, deadline).unwrap();
    let second = owner.fixture(Fixture::Stall, deadline).unwrap();
    let were_ready = ready(&owner, MAX_JOBS).await;
    drop(first);
    // No await here: cancellation cannot run its driver on this current-thread
    // runtime before the next admission. Consumer departure is not joined work.
    let third = owner.fixture(Fixture::Stall, deadline);
    let capacity_refused = matches!(third, Err(Error::Capacity));
    drop(third);
    let retained_before_join = owner.retained_jobs();
    let receipts = drain(&mut owner).await;
    drop(second);
    assert!(were_ready && capacity_refused);
    assert_eq!(retained_before_join, MAX_JOBS);
    assert_eq!(owner.retained_jobs(), 0);
    assert_killed_and_reaped(&receipts, MAX_JOBS).await;
    assert!(receipts.iter().all(|r| r.class == Err(Error::Cancelled)));
}

#[tokio::test(flavor = "current_thread")]
async fn interrupted_shutdown_retains_join_handles_and_permanently_stops_admission() {
    let mut owner = owner();
    let deadline = Instant::now() + WAIT;
    let consumer = owner.fixture(Fixture::Stall, deadline).unwrap();
    let was_ready = ready(&owner, 1).await;
    let mut closing = Box::pin(owner.shutdown());
    let was_pending = poll_fn(|cx| Poll::Ready(closing.as_mut().poll(cx).is_pending())).await;
    drop(closing);
    let retained_after_abandonment = owner.retained_jobs();
    let rejected = matches!(
        owner.resolve(ExpectedPeer::parse("127.0.0.1").unwrap(), 4455, deadline),
        Err(Error::OwnerStopped)
    );
    let receipts = drain(&mut owner).await;
    drop(consumer);
    let repeated = drain(&mut owner).await;
    assert!(was_ready && was_pending && rejected);
    assert_eq!(retained_after_abandonment, 1);
    assert_eq!(owner.retained_jobs(), 0);
    assert_eq!(repeated.len(), receipts.len());
    assert_killed_and_reaped(&receipts, 1).await;
    assert!(matches!(
        owner.fixture(Fixture::Stall, deadline),
        Err(Error::OwnerStopped)
    ));
    assert!(matches!(
        Owner::fixture_with_executable(PathBuf::from("relative-helper")),
        Err(Error::Spawn)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn exact_frame_reads_leave_lifeline_eof_and_unexpected_bytes_visible() {
    for trailing in [false, true] {
        let mut child = Command::new(HELPER)
            .arg("--obs-mcp-internal-resolver-stall-v1")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        // Independent fixed request vector: ASCII localhost, port4455. Stall
        // fixtures parse the frame but never call the external resolver.
        let mut request = b"ODR1\x11\x67\x09localhost".to_vec();
        if trailing {
            request.push(b'X');
        }
        let wrote = timeout(WAIT, stdin.write_all(&request)).await;
        let mut marker = [0; 4];
        let observed_ready = if trailing {
            true // The unexpected byte can end the helper before RDY1 is emitted.
        } else {
            timeout(WAIT, stdout.read_exact(&mut marker))
                .await
                .is_ok_and(|r| r.is_ok())
                && &marker == b"RDY1"
        };
        if !trailing {
            drop(stdin);
        }
        let outcome = timeout(WAIT, child.wait()).await;
        // If the regression stalls the helper, close only this owned process
        // and explicitly wait before any test assertion can panic.
        let status = match outcome {
            Ok(Ok(status)) => Some(status),
            _ => {
                let _ = child.start_kill();
                let _ = timeout(WAIT, child.wait()).await;
                None
            }
        };
        assert!(wrote.is_ok_and(|r| r.is_ok()) && observed_ready);
        assert_eq!(
            status.and_then(|s| s.code()),
            Some(if trailing { 76 } else { 75 })
        );
        // This is a live parent's explicit wait. Abrupt-parent-death/orphan
        // reaping remains a separate OS-supervisor observation, not this proof.
    }
}
