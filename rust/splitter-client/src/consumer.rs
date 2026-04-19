//! Consumer client: drives the `ConsumerService.Join` bidi stream.
//!
//! Phase-1 scope: open the stream, drive the embedded session protocol, send
//! `Register`, surface inbound cluster updates via a `watch` channel and
//! inbound grant-related `ClientMessage`s via a `broadcast` channel. The
//! full grant/ownership state machine lives in Phase 2.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use splitter_proto::pb;
use splitter_proto::pb::consumer_service_client::ConsumerServiceClient;
use splitter_proto::pb::{client_message, consumer_message, join_message, ClientMessage};
use splitter_proto::pb::{ConsumerMessage, JoinMessage};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle as TaskHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::{debug, info, warn};

use crate::cluster::ClusterMap;
use crate::error::{ClientError, Result};
use crate::ids::{DomainKeyName, Instance, QualifiedServiceName};
use crate::latch::{Latch, LatchReader};
use crate::session;

/// User-facing options for `ConsumerClient::join`.
#[derive(Clone, Debug, Default)]
pub struct ConsumerOptions {
    pub key_names: Vec<DomainKeyName>,
    pub capacity_limit: u64,
}

impl ConsumerOptions {
    pub fn with_key_name(mut self, name: DomainKeyName) -> Self {
        self.key_names.push(name);
        self
    }
    pub fn with_capacity_limit(mut self, limit: u64) -> Self {
        self.capacity_limit = limit;
        self
    }
}

/// A single decoded inbound `ClientMessage` variant, surfaced to the Dispatcher
/// layer in Phase 2 and to the `join` example in Phase 1.
#[derive(Clone, Debug)]
pub enum ClientMessageEvent {
    Assign(pb::client_message::Assign),
    Promote(pb::client_message::Promote),
    Revoke(pb::client_message::Revoke),
    Released(pb::client_message::Released),
    Update(pb::client_message::Update),
    Notify(pb::client_message::Notify),
    Extend(pb::client_message::Extend),
    Deregister(pb::client_message::Deregister),
    Register(pb::client_message::Register),
}

/// Handle to a running Join stream.
pub struct JoinHandle {
    pub cluster: watch::Receiver<Arc<ClusterMap>>,
    pub grants: broadcast::Receiver<ClientMessageEvent>,
    closed: LatchReader,
    shutdown: Latch,
    task: Option<TaskHandle<Result<()>>>,
}

impl JoinHandle {
    pub fn closed(&self) -> LatchReader {
        self.closed.clone()
    }

    /// Signal the driver to send Deregister + session Closed and exit.
    /// Returns once the task has exited.
    pub async fn shutdown(mut self) -> Result<()> {
        self.shutdown.close();
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(res) => res,
                Err(e) if e.is_cancelled() => Ok(()),
                Err(e) => Err(ClientError::SessionClosed(e.to_string())),
            }
        } else {
            Ok(())
        }
    }

    /// Subscribe a new receiver to grant-related inbound messages.
    pub fn subscribe_grants(&self) -> broadcast::Receiver<ClientMessageEvent> {
        self.grants.resubscribe()
    }
}

/// Thin wrapper around the generated tonic `ConsumerServiceClient`.
#[derive(Clone)]
pub struct ConsumerClient {
    inner: ConsumerServiceClient<Channel>,
}

impl ConsumerClient {
    pub fn new(channel: Channel) -> Self {
        Self {
            inner: ConsumerServiceClient::new(channel),
        }
    }

    /// Start a Join stream. Returns a `JoinHandle` whose channels surface
    /// cluster updates and grant-related messages.
    pub async fn join(
        &self,
        consumer: Instance,
        service: QualifiedServiceName,
        opts: ConsumerOptions,
    ) -> Result<JoinHandle> {
        let (outbound_tx, outbound_rx) = mpsc::channel::<JoinMessage>(64);
        let (cluster_tx, cluster_rx) = watch::channel(Arc::new(ClusterMap::empty()));
        let (grants_tx, grants_rx) = broadcast::channel::<ClientMessageEvent>(128);
        let closed = Latch::new();
        let shutdown = Latch::new();

        // Kick off the bidi stream. tonic owns the outbound half via the stream
        // we give it; we keep the receiver side to pump inbound frames.
        let request_stream = ReceiverStream::new(outbound_rx);
        let mut client = self.inner.clone();
        let response = client.join(request_stream).await?;
        let inbound: Streaming<JoinMessage> = response.into_inner();

        // Send Establish as the first frame so the server can set up the session.
        let session_id = uuid::Uuid::new_v4().to_string();
        outbound_tx
            .send(session::establish_frame(
                consumer.clone().into(),
                session_id,
            ))
            .await
            .map_err(|_| ClientError::SessionClosed("outbound closed before Establish".into()))?;

        // Followed by Register so we appear in the assignment rotation.
        outbound_tx
            .send(build_register(consumer.clone(), service.clone(), &opts))
            .await
            .map_err(|_| ClientError::SessionClosed("outbound closed before Register".into()))?;

        let driver = Driver {
            outbound: outbound_tx,
            inbound,
            cluster: cluster_tx,
            grants: grants_tx,
            closed: closed.clone(),
            shutdown: shutdown.clone(),
        };

        let task = tokio::spawn(driver.run());

        Ok(JoinHandle {
            cluster: cluster_rx,
            grants: grants_rx,
            closed: closed.reader(),
            shutdown,
            task: Some(task),
        })
    }
}

struct Driver {
    outbound: mpsc::Sender<JoinMessage>,
    inbound: Streaming<JoinMessage>,
    cluster: watch::Sender<Arc<ClusterMap>>,
    grants: broadcast::Sender<ClientMessageEvent>,
    closed: Latch,
    shutdown: Latch,
}

impl Driver {
    async fn run(mut self) -> Result<()> {
        // Start with a long delay; first Established frame replaces it.
        let mut heartbeat = tokio::time::interval(Duration::from_secs(60));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate first tick — we want the server to drive pacing.
        heartbeat.tick().await;

        let result = loop {
            tokio::select! {
                // Shutdown requested: send Deregister + Closed, then drain.
                _ = self.shutdown.closed(), if !self.shutdown.is_closed() => {
                    let _ = self.outbound.send(build_deregister()).await;
                    let _ = self.outbound.send(session::closed_frame("client shutdown")).await;
                    break Ok(());
                }
                _ = heartbeat.tick() => {
                    if self.outbound.send(session::heartbeat_frame()).await.is_err() {
                        break Err(ClientError::SessionClosed("outbound dropped".into()));
                    }
                }
                msg = self.inbound.next() => {
                    match msg {
                        Some(Ok(join)) => {
                            if let Some(next) = self.handle_inbound(join).await? {
                                let delay = session::heartbeat_delay(next, SystemTime::now());
                                heartbeat = tokio::time::interval(delay);
                                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                                // Consume the immediate tick.
                                heartbeat.tick().await;
                            }
                        }
                        Some(Err(status)) => {
                            break Err(ClientError::Transport(status));
                        }
                        None => {
                            break Err(ClientError::SessionClosed("stream ended".into()));
                        }
                    }
                }
            }
        };

        self.closed.close();
        result
    }

    /// Dispatch an inbound frame. Returns a new TTL if a session frame reset
    /// the heartbeat cadence.
    async fn handle_inbound(&mut self, join: JoinMessage) -> Result<Option<SystemTime>> {
        let Some(msg) = join.msg else {
            return Ok(None);
        };
        match msg {
            join_message::Msg::Session(session_msg) => {
                match session::classify(&session_msg) {
                    Some(session::SessionEvent::Established { ttl }) => {
                        info!(?ttl, "session established");
                        Ok(Some(ttl))
                    }
                    Some(session::SessionEvent::HeartbeatAck { ttl }) => {
                        debug!(?ttl, "heartbeat ack");
                        Ok(Some(ttl))
                    }
                    Some(session::SessionEvent::Closed { error }) => {
                        Err(ClientError::SessionClosed(error))
                    }
                    None => Ok(None),
                }
            }
            join_message::Msg::Consumer(ConsumerMessage { msg: Some(sub) }) => {
                match sub {
                    consumer_message::Msg::Cluster(cm) => {
                        // Apply the cluster update on top of the current snapshot.
                        let current = self.cluster.borrow().clone();
                        match current.apply(cm) {
                            Ok(next) => {
                                // watch::Sender::send_if_modified would require PartialEq; we just replace.
                                let _ = self.cluster.send(Arc::new(next));
                            }
                            Err(e) => {
                                warn!(error = %e, "failed to apply cluster update, continuing with previous snapshot");
                            }
                        }
                        Ok(None)
                    }
                    consumer_message::Msg::Client(client) => {
                        if let Some(event) = decode_client_message(client) {
                            let _ = self.grants.send(event);
                        }
                        Ok(None)
                    }
                }
            }
            join_message::Msg::Consumer(ConsumerMessage { msg: None }) => Ok(None),
        }
    }
}

fn decode_client_message(msg: ClientMessage) -> Option<ClientMessageEvent> {
    match msg.msg? {
        client_message::Msg::Register(r) => Some(ClientMessageEvent::Register(r)),
        client_message::Msg::Deregister(d) => Some(ClientMessageEvent::Deregister(d)),
        client_message::Msg::Extend(e) => Some(ClientMessageEvent::Extend(e)),
        client_message::Msg::Assign(a) => Some(ClientMessageEvent::Assign(a)),
        client_message::Msg::Promote(p) => Some(ClientMessageEvent::Promote(p)),
        client_message::Msg::Revoke(r) => Some(ClientMessageEvent::Revoke(r)),
        client_message::Msg::Released(r) => Some(ClientMessageEvent::Released(r)),
        client_message::Msg::Update(u) => Some(ClientMessageEvent::Update(u)),
        client_message::Msg::Notify(n) => Some(ClientMessageEvent::Notify(n)),
    }
}

fn build_register(
    consumer: Instance,
    service: QualifiedServiceName,
    opts: &ConsumerOptions,
) -> JoinMessage {
    let register = pb::client_message::Register {
        consumer: Some(consumer.into()),
        service: Some(service.into()),
        domains: Vec::new(),
        active: Vec::new(),
        options: Some(pb::client_message::register::Options {
            names: opts.key_names.iter().cloned().map(Into::into).collect(),
            capacity_limit: opts.capacity_limit,
        }),
    };
    wrap_client(pb::client_message::Msg::Register(register))
}

fn build_deregister() -> JoinMessage {
    wrap_client(pb::client_message::Msg::Deregister(
        pb::client_message::Deregister {},
    ))
}

fn wrap_client(msg: pb::client_message::Msg) -> JoinMessage {
    JoinMessage {
        msg: Some(join_message::Msg::Consumer(ConsumerMessage {
            msg: Some(consumer_message::Msg::Client(ClientMessage {
                msg: Some(msg),
            })),
        })),
    }
}
