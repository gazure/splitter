//! Rust client for splitter data plane.
//!
//! Phase 1 surface: Latch, ClientError, domain IDs, session driver, ClusterMap,
//! ConsumerClient::join. The Dispatcher/Processor/Proxy layers land in later
//! phases.

// ClientError carries a tonic::Status which is ~176 bytes; boxing it would
// complicate the #[from] conversions without real benefit. Results carrying
// ClientError are our standard return type, so accept the size.
#![allow(clippy::result_large_err)]

pub mod cluster;
pub mod consumer;
pub mod error;
pub mod ids;
pub mod latch;
pub mod session;
pub mod wrap;

pub use cluster::{ClusterId, ClusterMap, ConsumerEntry, GrantEntry};
pub use consumer::{ClientMessageEvent, ConsumerClient, ConsumerOptions, JoinHandle};
pub use error::{ClientError, Result};
pub use ids::{
    ConsumerId, DomainKey, GrantId, Instance, InstanceId, Location, QualifiedDomainKey,
    QualifiedDomainName, QualifiedServiceName, Shard,
};
pub use latch::{Latch, LatchReader};
