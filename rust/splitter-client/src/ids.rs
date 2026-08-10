//! Rust-native domain types used throughout the client.
//!
//! These intentionally don't share structure with the generated `splitter-proto`
//! types — they derive `Hash`/`Eq` cleanly, use rust-native time, and keep the
//! public API free of prost dependencies. Conversions live in [`crate::wrap`].

use std::fmt;
use std::time::SystemTime;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InstanceId(pub String);

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Consumer identity equals instance identity (see `model.Consumer` in Go).
pub type ConsumerId = InstanceId;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GrantId(pub String);

impl fmt::Display for GrantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct Location {
    pub region: String,
    pub node: String,
}

impl Location {
    pub fn new(region: impl Into<String>, node: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            node: node.into(),
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.region, self.node)
    }
}

/// Addressable consumer instance (inner `location.Instance` + peer endpoint).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instance {
    pub id: InstanceId,
    pub location: Location,
    pub name: String,
    pub created: SystemTime,
    pub endpoint: String,
}

impl fmt::Display for Instance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.location)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QualifiedServiceName {
    pub tenant: String,
    pub service: String,
}

impl QualifiedServiceName {
    pub fn new(tenant: impl Into<String>, service: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            service: service.into(),
        }
    }

    /// Parse `tenant/service`.
    pub fn parse(s: &str) -> Option<Self> {
        let (tenant, service) = s.split_once('/')?;
        if tenant.is_empty() || service.is_empty() {
            return None;
        }
        Some(Self::new(tenant, service))
    }
}

impl fmt::Display for QualifiedServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.tenant, self.service)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QualifiedDomainName {
    pub service: QualifiedServiceName,
    pub name: String,
}

impl fmt::Display for QualifiedDomainName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.service, self.name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DomainKey {
    pub region: String,
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QualifiedDomainKey {
    pub domain: QualifiedDomainName,
    pub key: DomainKey,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum DomainType {
    Invalid,
    Unit,
    Global,
    Regional,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum GrantState {
    Unknown,
    Active,
    Allocated,
    Revoked,
    AllocatedLoaded,
    RevokedUnloaded,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Shard {
    pub domain: QualifiedDomainName,
    pub kind: DomainType,
    pub region: String,
    pub from: String,
    pub to: String,
}

impl fmt::Display for Shard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}..{}]", self.domain, self.from, self.to)
    }
}

/// Named key variant used by the `Register.Options.names` feature
/// (`model.proto` DomainKeyName).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DomainKeyName {
    pub domain: String,
    pub name: String,
}
