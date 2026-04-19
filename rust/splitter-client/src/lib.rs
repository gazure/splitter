//! Rust client for splitter data plane.

// ClientError carries a tonic::Status which is ~176 bytes; boxing it would
// complicate the #[from] conversions without real benefit. Results carrying
// ClientError are our standard return type, so accept the size.
#![allow(clippy::result_large_err)]

pub mod cluster;
pub mod consumer;
pub mod error;
pub mod grant_map;
pub mod ids;
pub mod latch;
pub mod ownership;
pub mod session;
pub mod wrap;

pub use cluster::{ClusterId, ClusterMap, ConsumerEntry, GrantEntry};
pub use consumer::{ConsumerClient, ConsumerOptions, GrantEvent, JoinHandle};
pub use error::{ClientError, Result};
pub use grant_map::GrantMap;
pub use ids::{
    ConsumerId, DomainKey, DomainKeyName, DomainType, GrantId, GrantState, Instance, InstanceId,
    Location, QualifiedDomainKey, QualifiedDomainName, QualifiedServiceName, Shard,
};
pub use latch::{Latch, LatchReader};
pub use ownership::{Loader, Ownership, Unloader};
