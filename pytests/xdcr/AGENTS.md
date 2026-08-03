# AGENTS.md - XDCR Tests

## Project Purpose and Scope

XDCR (Cross Data Center Replication) test suite validates Couchbase replication between clusters. Tests cover all major XDCR features including replication topologies, filtering, collections, security, conflict resolution, and performance tuning.

## XDCR Features Covered

| Feature | Test File | Description |
|---------|-----------|-------------|
| **Replication Direction** | `uniXDCR.py`, `biXDCR.py` | Unidirectional and bidirectional replication |
| **Topologies** | `xdcrTopologyTests.py` | Chain, star, ring cluster topologies |
| **Collections** | `collectionsXDCR.py`, `collectionsMappingXDCR.py` | Scope/collection mapping, mirroring mode, migration mode |
| **Filtering** | `filterXDCR.py`, `advFilteringXDCR.py`, `filterDelExpXDCR.py`, `xdcrFilterChangeTests.py`, `testXdcrFilterSkipRestream.py` | Key/regex filtering, deletion/expiration filtering, filter changes; `filterDelExpXDCR.py` also asserts `/settings/replications` payload integrity — no blank key leaked from the internal `filterExpDelType` bitmask, and `filterDeletionsWithExpression`/`filterExpirationsWithExpression` rejected without their base flag (`-p GROUP=settings`) |
| **LWW (Last Writer Wins)** | `lww.py`, `lwwXDCR.py` | Conflict resolution with timestamp-based LWW |
| **CCV (Custom Conflict Resolution)** | `ccvTestXDCR.py` | Custom conflict resolution with mobile import simulation |
| **Conflict Logging** | `conflictLoggingTests.py` | Logging conflicts to specified collections |
| **Compression** | `compressionXDCR.py` | XDCR data compression. `compressionType` only accepts `Auto` or `None` over REST -- goxdcr keeps `Snappy` as an internal-only value and rejects it as user input, so conf rows must never set it |
| **Priority** | `prioritizationXDCR.py` | DCP stream prioritization (High, Medium, Low) |
| **Network Bandwidth** | `nwusageXDCR.py` | Network usage throttling and bandwidth limits |
| **Security/TLS** | `secureXDCR.py` | TLS encryption, n2n encryption, multiple CA support |
| **Staged Credentials** | `stagedCredentialsXDCR.py` | Credential rotation without replication interruption |
| **Checkpoints** | `checkpointXDCR.py` | Checkpoint intervals, persistence, recovery |
| **P2P (Peer-to-Peer)** | `p2pXDCR.py`, `xdcrP2PCheckpointTests.py` | P2P discovery, VB master checks |
| **Backfill** | `backfillXDCR.py` | Backfill pipeline for collections, rebalance during backfill |
| **Pause/Resume** | `pauseResumeXDCR.py` | Replication pause and resume lifecycle |
| **Rebalance** | `rebalanceXDCR.py` | Replication during cluster rebalance/failover |
| **Upgrade** | `upgradeXDCR.py` | XDCR upgrade path validation |
| **Target Awareness** | `targetAwarenessXDCR.py` | Topology heartbeats: incoming/outgoing replication tracking, plus `SourceClusterNodes` correctness -- non-KV source nodes never advertised (including when the non-KV node is the source orchestrator, i.e. the heartbeat sender: MB-71771), failed-over and rebalanced nodes reflected, every target node agreeing (P2P heartbeat proxying), source cluster name propagation, heartbeats surviving an orchestrator crash, and the target-side `xdcr_number_of_source_nodes_total` gauge. Tests shorten the heartbeat cadence via `SrcHeartbeatMinInterval`/`SrcHeartbeatMaxIntervalFactor` (shipped defaults put the next heartbeat up to 300s away; these are metakv-backed, so one POST per cluster covers every node and restarts goxdcr cluster-wide), assert the values read back before deriving any wait from them, and restore the shipped values in `tearDown`. Needs an ini with spare nodes -- `b/resources/6-nodes-template-xdcr.ini` |
| **Variable vBuckets** | `variableVbucketXDCR.py` | Dynamic vBucket configuration |
| **XATTR** | `xdcr_xattr_sdk.py` | Extended attributes replication |
| **forwardLocalOnly** | `forwardLocalOnlyXDCR.py` | AA bidirectional ECCV replication of local-only mutations; covers Type-1/2/3 mutation classification, foreign-HLV skip, `disableHlvBasedShortCircuit`, deletes/tombstones/expiry, advanced filtering, creation-time validation (source/target ECCV checks, FLO+mobile mutual exclusion), 3-cluster mixed-ECCV, full-mesh exactly-once delivery, ring-topology data-loss callout (with leg-liveness control), mixed-mode setting persistence; verifies `xdcr_non_local_mutations_skipped_total` and appends a post-scenario FLO-enforcement probe to lifecycle/topology tests |
| **JWT Auth** | `xdcr_jwt_auth.py` | JWT bearer-token auth for XDCR's REST admin surface (create/pause/resume/delete replication, remote cluster reference CRUD), delegated entirely to cbauth; RBAC-negative, malformed-token, Basic/JWT coexistence, and P2P non-interference coverage |
| **Statistics** | `statsXDCR.py` | XDCR replication statistics and metrics |
| **Memory** | `memoryTestXDCR.py` | Memory usage during replication |
| **xdcrDiffer** | `xdcrDiffer.py` | Data comparison tool with encryption support |
| **Elasticsearch** | `esXDCR.py`, `esbasetests.py` | Elasticsearch integration |
| **CAPI** | `capiXDCR.py` | Legacy CAPI protocol tests |

## Core Commands

### Run XDCR Tests
```bash
# Run unidirectional replication tests
./testrunner -i b/resources/dev-4-nodes-xdcr.ini -c conf/xdcr/py-xdcr-unidirectional-1.conf

# Run collections XDCR tests
./testrunner -i b/resources/dev-4-nodes-xdcr.ini -c conf/xdcr/py-xdcr-collections-P0.conf

# Run specific test with parameters
./testrunner -i b/resources/dev-4-nodes-xdcr.ini -t xdcr.uniXDCR.unidirectional.load_with_ops -p items=100000,rdirection=unidirection,ctopology=chain

# Run bidirectional tests
./testrunner -i b/resources/dev-4-nodes-xdcr.ini -c conf/xdcr/py-xdcr-bidirectional.conf
```

### INI File Templates
INI files in `b/resources/` define cluster topology with dynamic IP placeholders:
- `b/resources/dev-4-nodes-xdcr.ini` - 4 node XDCR setup
- `b/resources/6-nodes-template-xdcr.ini` - 6 node template
- `b/resources/8-nodes-template-xdcr.ini` - 8 node template with 3 clusters

## Repo Layout

- `AGENTS.md` - This file
- `xdcrnewbasetests.py` - Base class for all XDCR tests (XDCRNewBaseTest)
- `collectionsXDCR.py` - Scope/collection mapping validation
- `uniXDCR.py` - Unidirectional replication tests
- `biXDCR.py` - Bidirectional replication tests
- `lww.py` - Last Writer Wins conflict resolution
- `upgradeXDCR.py` - XDCR upgrade path validation
- `secureXDCR.py` - TLS/SSL encryption tests
- `stagedCredentialsXDCR.py` - Staged credential management
- `variableVbucketXDCR.py` - Dynamic vbucket configuration
- `filterXDCR.py`, `testXdcrFilterSkipRestream.py`, `xdcrFilterChangeTests.py` - Replication filtering
- `pauseResumeXDCR.py` - Replication pause/resume lifecycle
- `rebalanceXDCR.py` - Replication during cluster rebalance
- `compressionXDCR.py` - Compression over XDCR
- `forwardLocalOnlyXDCR.py` - forwardLocalOnly replication setting tests (AA bidirectional ECCV)
- `xdcr_jwt_auth.py` - JWT bearer-token auth coverage for the XDCR REST admin surface
- `xdcrDiffer.py` - xdcrDiffer binary tests
- `conf/xdcr/*.conf` - 40+ test configuration files

## Development Patterns and Constraints

### Test Structure
- All tests inherit from `XDCRNewBaseTest` in `xdcrnewbasetests.py`
- Use `setUp()` for cluster initialization and XDCR replication setup
- Access parameters via `self._input.param(name, default)`
- Common cluster retrieval: `self.get_cb_cluster_by_name('C1')` (source), `self.get_cb_cluster_by_name('C2')` (target)
- Pattern: `setup_xdcr_and_load()` → `perform_update_delete()` → `verify_results()`

### Configuration Format
- `.conf` files use `module.Class.method,param1=value1,param2=value2` format
- Common parameters: `rdirection=unidirection|bidirection`, `ctopology=chain|star|ring`, `items=100000`
- Comments with `#` disable specific tests

### Base Class
- `XDCRNewBaseTest` in `xdcrnewbasetests.py` - The only active base class for XDCR tests
- Note: `xdcrbasetests.py` is deprecated and should not be used for new tests
- Topology helpers worth reusing instead of re-deriving:
  - `get_orchestrator_node(cluster)` - the ns_server orchestrator as a server object, **not** the same thing as `cluster.get_master_node()`; it is the only node that sends topology heartbeats, and leadership moves on its own after restarts/rebalances/failovers
  - `get_active_kv_nodes(cluster)` - `"<host>:<port>"` of active data nodes, matching how XDCR advertises source nodes
  - `get_live_rest(cluster)` - `RestConnection` to the first responsive node, for tests that take nodes down
  - `query_prometheus_metric_series(cluster, metric)` - per-series metric read with labels; cluster-level goxdcr gauges are exported by every node, so `query_prometheus_metric()`'s sum multiplies them by the node count
  - `CouchbaseCluster.rebalance_in_with_services(["index,n1ql"])` - rebalance in service-only (non-KV) nodes; `rebalance_in()` always adds data nodes
- goxdcr's admin surface is proxied by ns_server on 8091: `GET`/`POST /xdcr/internalSettings` and `GET /xdcr/sourceClusters` all work through `RestConnection`, so tests do not need SSH + `curl localhost:9998` (which also puts the admin password into the logs and returns no usable status)
- Remote commands: `execute_command(..., use_channel=True)` does **not** surface stderr or a failure - it logged "command executed successfully" for a command that died with `nft: command not found`, which is how a firewall test blocked nothing and then blamed the product. Use `get_exit_code=True` and assert on the exit code, and where the command is supposed to change behaviour, verify the effect too
- Orchestrator placement: there is no supported way to hand orchestration to a chosen node. Stopping the current orchestrator moves leadership within seconds, but the node **reclaims it as soon as it is started again** (4/4 on 8.1.0-2570); rebalancing a node *in* does land leadership on the new node. A test that needs a node which is not the orchestrator should read the orchestrator and choose relative to it (`kv_node_other_than_orchestrator()` in `targetAwarenessXDCR.py`) rather than trying to move leadership

### Adding New Test Files
- When creating a new test file, ensure it is tracked in git (`git add <filename>`)
- Update the "XDCR Features Covered" table in this AGENTS.md with the new test file
- Update the "Repo Layout" section if the file covers a major new feature

### Key Libraries
- `lib/couchbase_helper/cluster.py` - Cluster REST API operations
- `lib/couchbase_helper/documentgenerator.py` - Data generators (BlobGenerator, DocumentGenerator, SDKDataLoader)
- `lib/sdk_client3.py` - Couchbase SDK client wrapper
- `lib/remote/remote_util.py` - SSH connections for remote server operations
- `membase/api/rest_client.py` - REST API for cluster/bucket configuration

### Document Loading Methods

**BlobGenerator** - Primary method for generating test documents with binary blob values:
```python
from couchbase_helper.documentgenerator import BlobGenerator
gen = BlobGenerator('prefix', 'prefix-', value_size=512, start=0, end=1000)
self.src_cluster.load_all_buckets_from_generator(kv_gen=gen)
```

**DocumentGenerator** - For JSON documents with specific templates:
```python
from couchbase_helper.documentgenerator import DocumentGenerator
gen = DocumentGenerator('doc_name', template, field1, field2, start=0, end=1000)
```

**SDKDataLoader** - Java SDK-based loader for high-performance loading:
```python
from couchbase_helper.documentgenerator import SDKDataLoader
gen = SDKDataLoader(num_ops=10000, percent_create=100, percent_update=0, 
                    percent_delete=0, doc_expiry=0, all_collections=True)
```

**cbc-pillowfight** - CLI tool for high-throughput binary document loading:
```python
def load_docs_with_pillowfight(self, server, items, bucket, batch=1000, docsize=100, 
                                rate_limit=100000, scope="_default", collection="_default"):
    cmd = f"/opt/couchbase/bin/cbc-pillowfight -u Administrator -P password " \
          f"-U couchbase://localhost/{bucket} -I {items} -m {docsize} -M {docsize} " \
          f"-B {batch} --rate-limit={rate_limit} --populate-only --collection {scope}.{collection}"
```

**Cluster loading methods:**
- `cluster.load_all_buckets(num_items, value_size, exp)` - Sync load to all buckets
- `cluster.async_load_all_buckets(num_items, value_size, exp)` - Async load
- `cluster.load_all_buckets_from_generator(kv_gen, ops, exp)` - Load using generator
- `cluster.load_all_buckets_till_dgm(active_resident_threshold, value_size, expiry)` - Load until DGM state

## Validation and Evidence Required

### Before Code Changes to XDCR Tests
- Run relevant test conf files to validate changes
- Verify test logs in `logs/` for failures

### Test Output Analysis
- Test results: logs and xunit XML in `tmp-{timestamp}/` directories
- Console summary: "summary so far suite {name}, pass X, fail Y"
- XDCR-specific logs: `goxdcr.log`, `xdcr.log` in logs directory

### Data Verification Patterns
- Use `verify_results()` to compare source/destination data
- Use `self.verify_cluster()` to validate cluster health
- Use `RestConnection` to query bucket stats via REST API

## Security and Sensitive Path Guidance

### Credential Management
- `.ini` files in `resources/` contain REST usernames, passwords, and SSH keys
- Never commit hardcoded credentials to test files
- Staged credentials tests involve rotating credentials without replication interruption

### Sensitive Information
- TLS certificates for secureXDCR.py tests
- SSH keys for remote server connections in `lib/remote/remote_util.py`
- xdcrDiffer encryption passphrases (test-pass, `P@ssw0rd!#$123`)

### Access Control
- Some tests require non-root SSH access to remote clusters
- Security tests (TLS, RBAC) require appropriate cluster configuration
- xdcrDiffer at-rest encryption requires `encryptionPassphrase` parameter

## Supporting Documentation

### Internal Docs
- [Domain Glossary](../../docs/agent-context/domain-glossary.md) - Couchbase terminology and XDCR concepts
- [Build Test Matrix](../../docs/agent-context/build-test-matrix.md) - Test execution commands by feature
- [Repo Inventory](../../docs/agent-context/repo-inventory.md) - XDCR directory and file structure
- [Troubleshooting](../../docs/agent-context/troubleshooting.md) - Common failures and escalation points
- [Architecture](../../docs/architecture.agents.md) - Testrunner architecture and runtime flows

### External Couchbase Documentation
- [XDCR Overview](https://docs.couchbase.com/server/current/learn/clusters-and-availability/xdcr-overview.html) - Conceptual overview of XDCR
- [XDCR REST API](https://docs.couchbase.com/server/current/rest-api/rest-xdcr-intro.html) - REST API for XDCR operations
- [XDCR Reference](https://docs.couchbase.com/server/current/xdcr-reference/xdcr-reference-intro.html) - Advanced settings and filtering expressions
- [XDCR Advanced Settings](https://docs.couchbase.com/server/current/xdcr-reference/xdcr-advanced-settings.html) - Compression, nozzles, checkpoints, network limits
- [XDCR Advanced Filtering](https://docs.couchbase.com/server/current/xdcr-reference/xdcr-filtering-reference-intro.html) - Regular expressions and filtering expressions
- [XDCR Management Overview](https://docs.couchbase.com/server/current/manage/manage-xdcr/xdcr-management-overview.html) - Managing XDCR replications
- [Enable Secure Replications](https://docs.couchbase.com/server/current/manage/manage-xdcr/enable-full-secure-replication.html) - TLS/security configuration
