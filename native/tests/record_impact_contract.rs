// SPDX-License-Identifier: GPL-2.0-only
//! Library prerequisite only: real Session binding, no public critical routes.
use futures_util::{SinkExt, StreamExt};
use obs_mcp_native::{
    record_impact::{ImpactError, ImpactStatus, RecordImpactOwner, Revocation},
    session::{EventReceiver, ObsSession, SessionHandle, SessionOptions},
};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
const BOUND: Duration = Duration::from_secs(3);
struct Fixture {
    session: ObsSession,
    handle: SessionHandle,
    events: EventReceiver,
    close: Option<oneshot::Sender<()>>,
    peer: JoinHandle<()>,
    rpc: Arc<AtomicUsize>,
}
impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (close, mut closed) = oneshot::channel();
        let rpc = Arc::new(AtomicUsize::new(0));
        let count = rpc.clone();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            peer.send(Message::Text(
                json!({"op":0,"d":{"obsWebSocketVersion":"5.7.0","rpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let identify = peer.next().await.unwrap().unwrap();
            assert!(matches!(identify, Message::Text(_)));
            peer.send(Message::Text(
                json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            loop {
                tokio::select! {
                    _=&mut closed=>{let _=peer.close(None).await;break;},
                    frame=peer.next()=>match frame {
                        Some(Ok(Message::Text(_)))=>{count.fetch_add(1,Ordering::SeqCst);},
                        Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                        _=>{},
                    }
                }
            }
        });
        let (session, events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let handle = session.handle();
        Self {
            session,
            handle,
            events,
            close: Some(close),
            peer,
            rpc,
        }
    }
    async fn disconnected(&mut self) {
        self.close.take().unwrap().send(()).unwrap();
        assert!(timeout(BOUND, self.events.next()).await.unwrap().is_err());
        assert!(self.handle.invalidation().is_some());
    }
    async fn finish(self) {
        self.session.shutdown().await.unwrap();
        drop(self.events);
        drop(self.close);
        timeout(BOUND, self.peer).await.unwrap().unwrap();
        assert_eq!(
            self.rpc.load(Ordering::SeqCst),
            0,
            "impact registry must not send RPCs"
        );
    }
}
#[tokio::test]
async fn only_exact_no_dispatch_restores_the_suspended_take_without_reusing_identity() {
    let f = Fixture::new().await;
    let owner = RecordImpactOwner::bind(&f.handle).unwrap();
    let view = owner.view();
    let port = owner.manual_port();
    let ticket = view.reserve_take().unwrap();
    let mut changes = view.changes().unwrap();
    let attempt = port.begin_at_send(f.handle.token()).unwrap();
    let first_id = attempt.id();
    assert_eq!(ticket.check(), ImpactStatus::Suspended(first_id));
    assert_eq!(view.reserve_take().unwrap_err(), ImpactError::Capacity);
    assert_eq!(
        port.begin_at_send(f.handle.token()).unwrap_err(),
        ImpactError::Capacity
    );
    changes.changed_until(Instant::now() + BOUND).await.unwrap();
    attempt.not_dispatched().unwrap();
    changes.changed_until(Instant::now() + BOUND).await.unwrap();
    assert_eq!(ticket.check(), ImpactStatus::Ready);
    let second = port.begin_at_send(f.handle.token()).unwrap();
    assert_ne!(second.id(), first_id);
    second.not_dispatched().unwrap();
    assert_eq!(ticket.check(), ImpactStatus::Ready);
    drop(owner);
    f.finish().await;
}
#[tokio::test]
async fn accepted_unknown_rejected_and_abandoned_effects_never_revive_old_tickets() {
    let f = Fixture::new().await;
    let owner = RecordImpactOwner::bind(&f.handle).unwrap();
    let view = owner.view();
    let port = owner.manual_port();
    let mut old = Vec::new();
    for (index, reason) in [
        Revocation::ExternalEffect,
        Revocation::UnknownEffect,
        Revocation::UnqualifiedRejection,
        Revocation::AbandonedAttempt,
    ]
    .into_iter()
    .enumerate()
    {
        let ticket = view.reserve_take().unwrap();
        assert_eq!(ticket.token(), f.handle.token());
        assert_eq!(ticket.check(), ImpactStatus::Ready);
        let attempt = port.begin_at_send(f.handle.token()).unwrap();
        match index {
            0 => attempt.accepted().unwrap(),
            1 => attempt.unknown().unwrap(),
            2 => attempt.rejected_unqualified().unwrap(),
            _ => drop(attempt),
        }
        assert_eq!(ticket.check(), ImpactStatus::Revoked(reason));
        old.push(ticket);
        let no_effect = port.begin_at_send(f.handle.token()).unwrap();
        no_effect.not_dispatched().unwrap();
        assert!(
            old.iter()
                .all(|ticket| matches!(ticket.check(), ImpactStatus::Revoked(_)))
        );
    }
    // Fresh explicit admission after settled impact is possible; old admissions
    // remain revoked. This is not OBS active/inactive or restart evidence.
    assert_eq!(view.reserve_take().unwrap().check(), ImpactStatus::Ready);
    drop(owner);
    f.finish().await;
}
#[tokio::test]
async fn retained_views_ports_attempts_and_waiters_cannot_extend_owner_admission() {
    let f = Fixture::new().await;
    let owner = RecordImpactOwner::bind(&f.handle).unwrap();
    let view = owner.view();
    let port = owner.manual_port();
    let ticket = view.reserve_take().unwrap();
    let attempt = port.begin_at_send(f.handle.token()).unwrap();
    let mut changes = view.changes().unwrap();
    let waiting = changes.changed_until(Instant::now() + BOUND);
    tokio::pin!(waiting);
    tokio::select! {biased; result=&mut waiting=>panic!("unchanged owner resolved early: {result:?}"), _=tokio::task::yield_now()=>{}}
    drop(owner);
    assert_eq!(
        timeout(BOUND, waiting).await.unwrap(),
        Err(ImpactError::OwnerGone)
    );
    assert_eq!(ticket.check(), ImpactStatus::Revoked(Revocation::OwnerGone));
    assert_eq!(view.reserve_take().unwrap_err(), ImpactError::OwnerGone);
    assert_eq!(
        port.begin_at_send(f.handle.token()).unwrap_err(),
        ImpactError::OwnerGone
    );
    assert_eq!(attempt.not_dispatched(), Err(ImpactError::OwnerGone));
    f.finish().await;
}
#[tokio::test]
async fn another_physical_connection_cannot_begin_an_impact_or_suspend_a_ticket() {
    let first = Fixture::new().await;
    let second = Fixture::new().await;
    let owner = RecordImpactOwner::bind(&first.handle).unwrap();
    let ticket = owner.view().reserve_take().unwrap();
    assert_eq!(owner.view().token(), first.handle.token());
    assert_eq!(
        owner
            .manual_port()
            .begin_at_send(second.handle.token())
            .unwrap_err(),
        ImpactError::WrongConnection
    );
    assert_eq!(ticket.check(), ImpactStatus::Ready);
    drop(owner);
    first.finish().await;
    second.finish().await;
}
#[tokio::test]
async fn eof_refuses_all_new_admissions_and_pending_wait_checks_its_original_bound() {
    let mut f = Fixture::new().await;
    let owner = RecordImpactOwner::bind(&f.handle).unwrap();
    let view = owner.view();
    let port = owner.manual_port();
    let ticket = view.reserve_take().unwrap();
    let attempt = port.begin_at_send(f.handle.token()).unwrap();
    let mut changes = view.changes().unwrap();
    let deadline = Instant::now() + Duration::from_millis(40);
    let waiting = changes.changed_until(deadline);
    tokio::pin!(waiting);
    tokio::select! {biased; result=&mut waiting=>panic!("no transition yet: {result:?}"), _=tokio::task::yield_now()=>{}}
    f.disconnected().await;
    // No terminal subscription or liveness-poller task exists in this module:
    // a waiting listener notices at its original bound, or another caller's
    // synchronous liveness check may publish the terminal signal sooner.
    assert_eq!(
        timeout(BOUND, waiting).await.unwrap(),
        Err(ImpactError::ConnectionClosed)
    );
    assert_eq!(
        ticket.check(),
        ImpactStatus::Revoked(Revocation::ConnectionClosed)
    );
    assert_eq!(
        view.reserve_take().unwrap_err(),
        ImpactError::ConnectionClosed
    );
    assert_eq!(
        port.begin_at_send(f.handle.token()).unwrap_err(),
        ImpactError::ConnectionClosed
    );
    assert_eq!(attempt.not_dispatched(), Err(ImpactError::ConnectionClosed));
    assert_eq!(
        RecordImpactOwner::bind(&f.handle).unwrap_err(),
        ImpactError::ConnectionClosed
    );
    drop(owner);
    f.finish().await;
}
#[tokio::test]
async fn wait_deadline_does_not_settle_or_release_a_pending_manual_effect() {
    let f = Fixture::new().await;
    let owner = RecordImpactOwner::bind(&f.handle).unwrap();
    let ticket = owner.view().reserve_take().unwrap();
    let mut changes = owner.view().changes().unwrap();
    let attempt = owner.manual_port().begin_at_send(f.handle.token()).unwrap();
    changes.changed_until(Instant::now() + BOUND).await.unwrap();
    assert_eq!(
        changes
            .changed_until(Instant::now() + Duration::from_millis(5))
            .await,
        Err(ImpactError::Deadline)
    );
    assert_eq!(ticket.check(), ImpactStatus::Suspended(attempt.id()));
    assert_eq!(
        changes.changed_until(Instant::now()).await,
        Err(ImpactError::Deadline)
    );
    drop(attempt);
    assert_eq!(
        ticket.check(),
        ImpactStatus::Revoked(Revocation::AbandonedAttempt)
    );
    drop(owner);
    f.finish().await;
}
#[tokio::test]
async fn diagnostics_expose_no_owner_identity_endpoint_or_attempt_number() {
    let f = Fixture::new().await;
    let owner = RecordImpactOwner::bind(&f.handle).unwrap();
    let view = owner.view();
    let port = owner.manual_port();
    let ticket = view.reserve_take().unwrap();
    let changes = view.changes().unwrap();
    let attempt = port.begin_at_send(f.handle.token()).unwrap();
    let text = format!(
        "{owner:?} {view:?} {port:?} {ticket:?} {changes:?} {attempt:?} {:?}",
        attempt.id()
    );
    assert!(!text.contains(f.handle.endpoint_hash()));
    assert!(!text.contains(&f.handle.token().generation().to_string()));
    assert!(!text.contains("127.0.0.1"));
    assert_eq!(format!("{:?}", attempt.id()), "AttemptId { .. }");
    drop(attempt);
    drop(owner);
    f.finish().await;
}
