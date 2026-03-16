# Domain Glossary

## Couchbase Services

| Service | Description | Test directory |
|---------|-------------|----------------|
| **KV** (Key-Value) | Core data storage, CRUD, expiry, durability | `pytests/setgettests.py`, `pytests/buckettests.py` |
| **Query** (N1QL) | SQL-like query language for Couchbase | `pytests/tuqquery/` |
| **GSI** (Global Secondary Index) | Secondary index service backing N1QL queries | `pytests/gsi/` |
| **FTS** (Full Text Search) | Full-text, vector, and composite search indexes | `pytests/fts/` |
| **Eventing** | JavaScript event-driven data processing | `pytests/eventing/` |
| **CBAS** (Analytics) | Large-scale analytics with SQL++ syntax | `pytests/cbas/` |
| **Backup** | Enterprise backup and restore | `pytests/ent_backup_restore/` |
| **XDCR** | Cross Datacenter Replication (active-active) | `pytests/xdcr/` |

## Additional Test Areas

| Area | Description | Test location |
|------|-------------|---------------|
| **Views** | Legacy MapReduce indexing | `pytests/view/` |
| **Sub-Doc** | Partial document get/set operations | `pytests/subdoc/` |
| **Security/RBAC** | Role-based access, LDAP, SSL, x.509 | `pytests/security/` |
| **Rebalance** | Cluster topology change operations | `pytests/rebalance/`, `pytests/rebalancetests.py` |
| **Failover** | Automatic/graceful failover testing | `pytests/failover/` |
| **Upgrade** | Cross-version upgrade validation | `pytests/upgrade/` |
| **DCP** | Database Change Protocol (internal replication) | `pytests/dcp/` |

## Test Environments

| Environment | Description |
|-------------|-------------|
| **cluster_run** | Local sandbox; multiple Couchbase nodes on one machine via `../ns_server/` |
| **Remote cluster** | Real multi-machine cluster accessed via SSH; configured in INI files |
| **Capella** | Couchbase managed cloud service |
| **Cloud (AWS/Azure/GCP)** | Self-managed cloud deployments; provisioned via `scripts/cloud_provision.py` |

## Configuration Files

| Type | Format | Location | Purpose |
|------|--------|----------|---------|
| **INI** | `[global]`, `[servers]`, `[server_N]` sections | `b/resources/` | Cluster topology, IPs, credentials, SSH keys, service assignments |
| **Conf** | `test.Module.method param1=val1,param2=val2` per line | `conf/` | Test suite definitions with parameters |

## Test Types

| Type | Description |
|------|-------------|
| **Integration** | Primary type; all tests require a running cluster |
| **Smoke** | Quick sanity: `make e2e-kv-single-node` |
| **Upgrade** | Cross-version compatibility (data migration, service compat) |
| **Performance** | Load/stress testing via `lib/perf_engines/` |
| **Longevity** | Extended-duration stability tests in `longevity/` |

## Framework Components

| Component | Path | Purpose |
|-----------|------|---------|
| `basetestcase.py` | `pytests/` | Base class for all tests (setUp/tearDown, bucket ops, logging) |
| `TestInputParser` | `TestInput.py` | CLI argument and configuration parsing |
| `RestConnection` | `lib/membase/api/rest_client.py` | Couchbase REST API client |
| `RemoteMachineShellConnection` | `lib/remote/remote_util.py` | SSH/SCP for remote operations |
| `cluster_run_manager` | `lib/cluster_run_manager.py` | Local cluster_run lifecycle |
| `sdk_client3` | `lib/sdk_client3.py` | Python SDK wrapper |
| `Cluster` | `lib/couchbase_helper/cluster.py` | Cluster operation helpers |

## Test Artifacts

| Artifact | Location | Format |
|----------|----------|--------|
| Test logs | `tmp-<timestamp>/<test>.log` | Plain text |
| XUnit reports | `tmp-<timestamp>/report-<timestamp>.xml-<suite>.xml` | XML |
| cbcollect_info | Collected on failure | Couchbase diagnostics bundle |

## Key Acronyms

| Acronym | Meaning |
|---------|---------|
| GSI | Global Secondary Index |
| N1QL | Non-first Normal Form Query Language (Couchbase SQL) |
| FTS | Full Text Search |
| CBAS | Couchbase Analytics Service |
| XDCR | Cross Datacenter Replication |
| DCP | Database Change Protocol |
| RBAC | Role-Based Access Control |
| KV | Key-Value (data service) |
| SDK | Software Development Kit (Couchbase client library) |
| INI | Initialization file (cluster config format) |
