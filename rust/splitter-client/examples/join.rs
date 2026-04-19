//! Phase-1 smoke test: connect to a splitter, join a service, print cluster
//! updates and grant-related messages until Ctrl-C.
//!
//! Usage:
//!     cargo run --example join -- <endpoint> <tenant/service> [region] [node]
//!
//! Defaults:
//!     endpoint: http://localhost:50051
//!     tenant/service: (required)
//!     region:   local
//!     node:     local

use std::time::SystemTime;

use splitter_client::{
    ConsumerClient, ConsumerOptions, Instance, InstanceId, Location, QualifiedServiceName,
};
use tonic::transport::Channel;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,splitter_client=debug".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let endpoint = args
        .next()
        .unwrap_or_else(|| "http://localhost:50051".to_string());
    let svc_arg = args
        .next()
        .ok_or("second arg must be tenant/service")?;
    let service = QualifiedServiceName::parse(&svc_arg)
        .ok_or("tenant/service format required, e.g. acme/widgets")?;
    let region = args.next().unwrap_or_else(|| "local".to_string());
    let node = args.next().unwrap_or_else(|| "local".to_string());

    tracing::info!(%endpoint, %service, %region, %node, "connecting");

    let channel = Channel::from_shared(endpoint.clone())?.connect().await?;
    let client = ConsumerClient::new(channel);

    let consumer = Instance {
        id: InstanceId(uuid::Uuid::new_v4().to_string()),
        location: Location::new(region, node),
        name: "splitter-rs/join-example".to_string(),
        created: SystemTime::now(),
        endpoint: String::new(),
    };

    let handle = client
        .join(consumer, service, ConsumerOptions::default())
        .await?;

    let mut cluster = handle.cluster.clone();
    let mut grants = handle.subscribe_grants();
    let closed = handle.closed();

    loop {
        tokio::select! {
            res = cluster.changed() => {
                if res.is_err() {
                    tracing::info!("cluster channel closed");
                    break;
                }
                let snap = cluster.borrow_and_update();
                tracing::info!(
                    version = snap.id.version,
                    consumers = snap.consumers.len(),
                    grants = snap.grants.len(),
                    shards = snap.shards.len(),
                    "cluster update"
                );
            }
            evt = grants.recv() => {
                match evt {
                    Ok(e) => tracing::info!("grant event: {:?}", kind_name(&e)),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(dropped = n, "grant broadcast lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = closed.closed() => {
                tracing::info!("join closed");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT received, draining");
                handle.shutdown().await?;
                return Ok(());
            }
        }
    }

    Ok(())
}

fn kind_name(e: &splitter_client::ClientMessageEvent) -> &'static str {
    use splitter_client::ClientMessageEvent::*;
    match e {
        Assign(_) => "Assign",
        Promote(_) => "Promote",
        Revoke(_) => "Revoke",
        Released(_) => "Released",
        Update(_) => "Update",
        Notify(_) => "Notify",
        Extend(_) => "Extend",
        Deregister(_) => "Deregister",
        Register(_) => "Register",
    }
}
