// SPDX-License-Identifier: MIT
//! One retained initialization attempt. No frontend owns its task or physical
//! resources, and no preparation/snapshot writer crosses the network await.
use super::*;
use obs_mcp_native::{
    resource_events::Publisher,
    service::{BootstrappedResources, ServiceHandle},
    session::{ConnectError, EventReceiver},
};
use std::{future::Future, pin::Pin};
use tokio::{
    sync::watch,
    task::{JoinError, JoinSet},
};
/// The platform TLS integration supplies an establishment-only Connector to
/// this owned future, while its non-Clone Owner stays in Runtime.
pub(super) type Establish = Pin<
    Box<dyn Future<Output = Result<(ObsSession, EventReceiver), ConnectError>> + Send + 'static>,
>;
pub(super) struct Inputs {
    pub(super) store: OperationStore,
    pub(super) control: SharedControl,
    pub(super) clock: Clock,
    pub(super) publisher: Publisher,
    pub(super) deadline: tokio::time::Instant,
}
#[derive(Clone)]
pub(super) enum State {
    Pending,
    Ready(ServiceHandle),
    Failed(Value),
}
pub(super) enum ServiceSource {
    Ready(Result<Option<ServiceHandle>, Value>),
    Waiting(watch::Receiver<State>),
}
impl ServiceSource {
    pub(super) async fn resolve(
        self,
        cancel: Cancellation,
    ) -> Result<Option<ServiceHandle>, Value> {
        match self {
            Self::Ready(result) => result,
            Self::Waiting(mut status) => loop {
                if cancel.is_cancelled() {
                    return Err(error(
                        "Native initialization waiter was cancelled; no OBS effect was dispatched",
                    ));
                }
                match status.borrow_and_update().clone() {
                    State::Ready(handle) => return Ok(Some(handle)),
                    State::Failed(value) => return Err(value),
                    State::Pending => {}
                }
                tokio::select! {biased;()=cancel.cancelled()=>return Err(error("Native initialization waiter was cancelled; no OBS effect was dispatched")),changed=status.changed()=>if changed.is_err(){return Err(error("Native initialization owner ended without a result"));}}
            },
        }
    }
}
pub(super) enum Outcome {
    Ready(Box<BootstrappedResources>),
    Failed { value: Value, joined: bool },
}
pub(super) struct ConnectAttempt {
    jobs: JoinSet<Outcome>,
    status: watch::Sender<State>,
}
impl ConnectAttempt {
    pub(super) fn start(inputs: Inputs, establish: Establish) -> Self {
        let (status, _) = watch::channel(State::Pending);
        let mut jobs = JoinSet::new();
        jobs.spawn(initialize(inputs, establish));
        Self { jobs, status }
    }
    pub(super) fn receiver(&self) -> watch::Receiver<State> {
        self.status.subscribe()
    }
    pub(super) async fn join_next(&mut self) -> Option<Result<Outcome, JoinError>> {
        self.jobs.join_next().await
    }
    pub(super) fn publish(&self, result: Result<ServiceHandle, Value>) {
        self.status.send_replace(match result {
            Ok(handle) => State::Ready(handle),
            Err(value) => State::Failed(value),
        });
    }
    pub(super) fn cleanup(&mut self, resources: Box<Resources>) {
        self.jobs.spawn(async move {
            let bridge = resources.bridge.shutdown().await.is_ok();
            let session = resources.session.shutdown().await.is_ok();
            Outcome::Failed {
                value: error("Native persistent ownership admission failed"),
                joined: bridge && session,
            }
        });
    }
}
async fn initialize(inputs: Inputs, establish: Establish) -> Outcome {
    let deadline = inputs.deadline;
    let (session, events) = match establish.await {
        Ok(parts) => parts,
        Err(_) => {
            return Outcome::Failed {
                value: error(
                    "Native OBS connection is unavailable; no compatibility runtime was started",
                ),
                joined: true,
            };
        }
    };
    let bridge = match ObservationBridge::bind_with_resource_events(
        session.handle(),
        events,
        None,
        inputs.publisher,
    ) {
        Ok(bridge) => bridge,
        Err(_) => {
            let joined = session.shutdown().await.is_ok();
            return Outcome::Failed {
                value: error("Native observation ownership is unavailable"),
                joined,
            };
        }
    };
    let resources = Resources {
        session,
        bridge,
        store: inputs.store,
        control: inputs.control,
        clock: inputs.clock,
        identities: Box::new(|| obs_mcp_native::runtime_identity::uuid_v4().unwrap_or_default()),
    };
    match Service::bootstrap_resources(resources, deadline).await {
        Ok(resources) => Outcome::Ready(Box::new(resources)),
        Err(resources) => {
            let bridge = resources.bridge.shutdown().await.is_ok();
            let session = resources.session.shutdown().await.is_ok();
            Outcome::Failed {
                value: error(
                    "Native OBS connection deadline expired during capability discovery; no OBS effect was dispatched",
                ),
                joined: bridge && session,
            }
        }
    }
}
