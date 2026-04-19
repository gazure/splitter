//! Peer connection pool.
//!
//! Port of `pkg/model/pool.go`, simplified to lean on tonic's already-lazy
//! `Channel::connect_lazy()` for dial/reconnect. The pool's job is to:
//!
//! 1. Keep a `HashMap<InstanceId, Channel>` in sync with the set of consumer
//!    instances visible in the latest `ClusterMap`.
//! 2. Reject self-lookups via `ClientError::NoResolution` (pool.go:59).
//! 3. Expose cheap `resolve(id)` for the forwarding path.
//!
//! We skip Go's 19s dial-delay / 2min idle-eviction machinery — tonic's
//! `Channel` already handles lazy connect + reconnect; when a consumer leaves
//! the cluster we drop the channel, and tonic tears down the underlying
//! connection on the last clone's drop.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use tonic::transport::{Channel, Endpoint};
use tracing::warn;

use crate::cluster::ClusterMap;
use crate::error::ClientError;
use crate::ids::InstanceId;

struct PoolEntry {
    endpoint: String,
    channel: Channel,
}

/// Dial configuration: accepts a "host:port" or full URL and returns a tonic
/// [`Channel`]. Defaults to insecure HTTP/2. Users can customize via
/// [`ConnectionPool::with_tls`].
type DialFn = dyn Fn(&str) -> Result<Channel, ClientError> + Send + Sync + 'static;

pub struct ConnectionPool {
    self_id: InstanceId,
    entries: RwLock<HashMap<InstanceId, PoolEntry>>,
    dial: Arc<DialFn>,
}

impl ConnectionPool {
    pub fn new(self_id: InstanceId) -> Arc<Self> {
        Arc::new(Self {
            self_id,
            entries: RwLock::new(HashMap::new()),
            dial: Arc::new(default_dial),
        })
    }

    /// Build a pool with a user-supplied dial function. Useful for TLS /
    /// custom interceptors; the default dials insecure HTTP/2.
    pub fn with_dial<F>(self_id: InstanceId, dial: F) -> Arc<Self>
    where
        F: Fn(&str) -> Result<Channel, ClientError> + Send + Sync + 'static,
    {
        Arc::new(Self {
            self_id,
            entries: RwLock::new(HashMap::new()),
            dial: Arc::new(dial),
        })
    }

    pub fn self_id(&self) -> &InstanceId {
        &self.self_id
    }

    /// Resolve a peer by instance id.
    pub fn resolve(&self, id: &InstanceId) -> Result<Channel, ClientError> {
        if id == &self.self_id {
            return Err(ClientError::NoResolution);
        }
        let entries = self.entries.read().expect("pool poisoned");
        entries
            .get(id)
            .map(|e| e.channel.clone())
            .ok_or(ClientError::NoResolution)
    }

    /// Apply the current cluster membership: dial new peers, drop departed
    /// ones, rebuild channels whose advertised endpoint has changed.
    pub fn apply_cluster(&self, cluster: &ClusterMap) {
        let mut seen: HashSet<InstanceId> = HashSet::new();
        let mut to_add: Vec<(InstanceId, String)> = Vec::new();
        let mut to_rebuild: Vec<(InstanceId, String)> = Vec::new();

        {
            let entries = self.entries.read().expect("pool poisoned");
            for consumer in cluster.consumers.values() {
                let id = consumer.consumer.id.clone();
                if id == self.self_id {
                    continue;
                }
                let endpoint = consumer.consumer.endpoint.clone();
                if endpoint.is_empty() {
                    continue; // peer didn't advertise a server; skip silently
                }
                seen.insert(id.clone());
                match entries.get(&id) {
                    Some(existing) if existing.endpoint == endpoint => {}
                    Some(_) => to_rebuild.push((id, endpoint)),
                    None => to_add.push((id, endpoint)),
                }
            }
        }

        if to_add.is_empty() && to_rebuild.is_empty() {
            // Still need to drop departed peers.
            let mut entries = self.entries.write().expect("pool poisoned");
            entries.retain(|id, _| seen.contains(id));
            return;
        }

        let mut entries = self.entries.write().expect("pool poisoned");
        for (id, endpoint) in to_add.into_iter().chain(to_rebuild.into_iter()) {
            match (self.dial)(&endpoint) {
                Ok(channel) => {
                    entries.insert(id, PoolEntry { endpoint, channel });
                }
                Err(e) => {
                    warn!(peer = %endpoint, error = %e, "failed to dial peer, skipping");
                }
            }
        }
        entries.retain(|id, _| seen.contains(id));
    }

    pub fn len(&self) -> usize {
        self.entries.read().expect("pool poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn default_dial(endpoint: &str) -> Result<Channel, ClientError> {
    Endpoint::from_shared(normalize_endpoint(endpoint))
        .map(|ep| ep.connect_lazy())
        .map_err(ClientError::TransportSetup)
}

fn normalize_endpoint(s: &str) -> String {
    if s.starts_with("http://") || s.starts_with("https://") {
        s.to_string()
    } else {
        format!("http://{s}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // We construct a minimal ClusterMap by hand; apply_cluster should dial /
    // evict correctly without needing a real gRPC server.
    use crate::cluster::{ClusterId, ConsumerEntry};
    use crate::ids::Location;
    use std::time::SystemTime;

    fn mk_instance(id: &str, endpoint: &str) -> crate::ids::Instance {
        crate::ids::Instance {
            id: InstanceId(id.into()),
            location: Location::default(),
            name: id.into(),
            created: SystemTime::UNIX_EPOCH,
            endpoint: endpoint.into(),
        }
    }

    fn mk_cluster(consumers: Vec<crate::ids::Instance>) -> ClusterMap {
        let mut map = std::collections::HashMap::new();
        for c in consumers {
            map.insert(
                c.id.clone(),
                ConsumerEntry {
                    consumer: c,
                    grants: Vec::new(),
                },
            );
        }
        ClusterMap {
            id: ClusterId {
                version: 1,
                origin: None,
                timestamp: SystemTime::UNIX_EPOCH,
            },
            consumers: map,
            grants: std::collections::HashMap::new(),
            shards: Vec::new(),
        }
    }

    #[test]
    fn self_resolve_returns_no_resolution() {
        let pool = ConnectionPool::new(InstanceId("me".into()));
        assert!(matches!(
            pool.resolve(&InstanceId("me".into())),
            Err(ClientError::NoResolution)
        ));
    }

    #[test]
    fn unknown_id_returns_no_resolution() {
        let pool = ConnectionPool::new(InstanceId("me".into()));
        assert!(matches!(
            pool.resolve(&InstanceId("other".into())),
            Err(ClientError::NoResolution)
        ));
    }

    #[tokio::test]
    async fn apply_cluster_adds_known_peers() {
        let pool = ConnectionPool::new(InstanceId("me".into()));
        let cluster = mk_cluster(vec![
            mk_instance("me", "1.1.1.1:50052"),
            mk_instance("peer-a", "2.2.2.2:50052"),
            mk_instance("peer-b", "3.3.3.3:50052"),
        ]);
        pool.apply_cluster(&cluster);
        assert_eq!(pool.len(), 2);
        assert!(pool.resolve(&InstanceId("peer-a".into())).is_ok());
        assert!(pool.resolve(&InstanceId("peer-b".into())).is_ok());
        // self still rejects
        assert!(matches!(
            pool.resolve(&InstanceId("me".into())),
            Err(ClientError::NoResolution)
        ));
    }

    #[tokio::test]
    async fn apply_cluster_drops_departed_peers() {
        let pool = ConnectionPool::new(InstanceId("me".into()));
        pool.apply_cluster(&mk_cluster(vec![
            mk_instance("peer-a", "2.2.2.2:50052"),
            mk_instance("peer-b", "3.3.3.3:50052"),
        ]));
        assert_eq!(pool.len(), 2);

        pool.apply_cluster(&mk_cluster(vec![mk_instance("peer-a", "2.2.2.2:50052")]));
        assert_eq!(pool.len(), 1);
        assert!(pool.resolve(&InstanceId("peer-a".into())).is_ok());
        assert!(matches!(
            pool.resolve(&InstanceId("peer-b".into())),
            Err(ClientError::NoResolution)
        ));
    }

    #[tokio::test]
    async fn apply_cluster_skips_empty_endpoints() {
        let pool = ConnectionPool::new(InstanceId("me".into()));
        pool.apply_cluster(&mk_cluster(vec![mk_instance("peer-a", "")]));
        assert_eq!(pool.len(), 0);
    }
}
