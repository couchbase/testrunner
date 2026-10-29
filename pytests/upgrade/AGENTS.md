# Upgrade Tests - Agent Context

## Overview

The upgrade test framework validates Couchbase Server upgrades by running various operations before, during, and after cluster upgrades. Tests support online, offline, and failover-based upgrades across multiple Couchbase versions.

## Core Test Files

| File | Purpose |
|------|---------|
| `newupgradebasetest.py` | Base class with shared upgrade test logic |
| `upgrade_tests.py` | Main upgrade test implementation |
| `upgrade_tests_collections.py` | Collection-specific upgrade tests |
| `upgrade_cluster_ops.py` | NServ cluster-level upgrade checks: health, orchestration, settings persistence, compat/join, LDAP TLS |

## Test Execution Flow

### `test_upgrade()` (upgrade_tests.py)

```
test_upgrade()
  ├─> FTSCallable(nodes=self.servers, es_validate=True)  # ES validation initialized
  ├─> rebalance_load_setup()
  ├─> run_event(initialize_events)                      # pre-upgrade operations
  ├─> upgrade_event()                                    # perform upgrade
  ├─> run_event(after_events)                            # post-upgrade operations
  └─> cluster_stats() + verify_data()
```

## Elasticsearch Integration

### When is Elasticsearch Used?

Elasticsearch is used **only when `es_validate=True`** is passed to `FTSCallable`. It serves as a reference/baseline system to validate Couchbase FTS query correctness.

### Unique ES Resource Naming for Concurrent Test Execution

**Important:** All ES resources (indexes, aliases, and ingest pipelines) now use unique names derived from `FTSBaseTest.get_es_index_name()` to enable safe concurrent test execution on shared ES clusters.

**Index Name Format:** `{uuid_short}_{ddmmmyy_hh_mm_ss}` (e.g., `a3f4e2c1_28mar26_14_59_01`)

**Note:** Elasticsearch requires all index names to be lowercase. The code automatically applies `.lower()` to the generated index name to ensure compliance.

**ES Resources Generated:**
- Primary index: `{UUID_short}_{DDMMMYY_HH_MM_SS}`
- Multi-dataset indexes: `{bucket}_es_index_{UUID_short}_{DDMMMYY_HH_MM_SS}`
- ES aliases: `{name}_es_alias_{UUID_short}_{DDMMMYY_HH_MM_SS}`
- Ingest pipelines: `polygonize_{UUID_short}_{DDMMMYY_HH_MM_SS}`

**Example Names:**
- Main index: `a3f4e2c1_28mar26_14_59_01`
- Emp bucket index: `emp_es_index_a3f4e2c1_28mar26_14_59_01`
- Wiki bucket index: `wiki_es_index_a3f4e2c1_28mar26_14_59_01`
- Multi-bucket alias: `emp_wiki_es_alias_a3f4e2c1_28mar26_14_59_01`
- Ingest pipeline: `polygonize_a3f4e2c1_28mar26_14_59_01`

**Key Methods:**
- `FTSBaseTest.get_es_index_name()` - Returns the unique job-level index name (lazy-initialized class attribute)
- `wait_for_indexing_complete()` - Auto-resolves ES index name when `compare_es=True`
- `create_index_es()` - Uses dynamic ES index name
- `add_circle_ingest_pipeline()` - Accepts configurable pipeline name

### Required INI Configuration

```ini
[elastic]
ip:172.23.219.184
port:9200
es_username:      # optional, if ES requires auth
es_password:      # optional, if ES requires auth
```

**Failure without ES config:** If `es_validate=True` is set but `[elastic]` section is missing, the test will raise an exception at FTSCallable initialization.

### FTSCallable Setup - Call Map

#### 1. Direct Creation in `test_upgrade()`

```
upgrade_tests.py:test_upgrade()
  └─> FTSCallable(nodes=self.servers, es_validate=True)
```

#### 2. Via Event Functions (Inherited from NewUpgradeBaseTest)

##### Event: `create_fts_index_query_compare` (pre-upgrade)

```
test_upgrade()
  └─> run_event(initialize_events)
       └─> create_fts_index_query_compare() [newupgradebasetest.py]
            ├─> FTSCallable(nodes=self.servers, es_validate=True)
            ├─> create_default_index() on each bucket
            ├─> load_data(num_items)
            │   └─> Parallel load to CB and ES
            ├─> wait_for_indexing_complete()
            └─> run_query_and_compare() (20 random queries per index)
```

##### Event: `run_fts_query_and_compare` (post-upgrade)

```
test_upgrade()
  └─> run_event(after_events)
       └─> run_fts_query_and_compare() [newupgradebasetest.py]
            ├─> FTSCallable(nodes=self.servers, es_validate=True)
            ├─> load_data(1000)
            ├─> delete_doc_by_key() (20% of docs)
            ├─> push_vector_data() (if vector_upgrade enabled)
            └─> update_delete_fts_data_run_queries()
```

### Elasticsearch Operations (Pure ES Perspective)

#### FTSCallable.__init__() [fts_callable.py:44-74]

```
FTSCallable.__init__(es_validate=True)
  └─> self.es_index_name = FTSBaseTest.get_es_index_name()
       └─> Generates unique job-level index: {uuid_short}_{ddmmmyy_hh_mm_ss}
            Example: "a3f4e2c1_28mar26_14_59_01"
       └─> Automatically converts to lowercase for ES compliance
  └─> ElasticSearchBase(self.elastic_node)
       └─> create_empty_index_with_bleve_equivalent_std_analyzer(self.es_index_name)
            ├─> DELETE {es_index_name} (cleanup)
            ├─> PUT {es_index_name} with BLEVE.STD_ANALYZER settings
            │   ├── Custom analyzer: "custom_standard_analyzer"
            │   │   ├── tokenizer: "standard"
            │   │   └── filters: ["lowercase", "custom_stop_filter"]
            │   └─> BLEVE.STOPWORDS (stopword filter: 100+ words)
            └─> enable_scroll(index_name=self.es_index_name, max_result_window: 1000000)
```

**Unique Index Naming:**
- Format: `{uuid_short}_{ddmmmyy_hh_mm_ss}` (e.g., `a3f4e2c1_28mar26_14_59_01`)
- Automatically lowercased to meet Elasticsearch naming requirements
- Job-level sharing: Tests within the same job share the same unique index
- Parallel isolation: Different automated jobs use different indices to avoid conflicts
- Reduces to `FTSBaseTest.es_index_name` class attribute for reuse across test methods in the same job

#### Data Loading Operations

```
load_data(num_items)
  └─> async_load_data()
       └─> ES operations:
            ├── es.async_bulk_load_ES(index_name=self.es_index_name)
            │   └─> ElasticSearchBase.async_bulk_load_ES()
            │        └─> ESBulkLoadGeneratorTask (async bulk HTTP POST)
            │             └─> JSON generator: "emp" template
            │                  (name, email, address, dept, skills)
```

#### Query Validation Operations

```
run_query_and_compare(index, num_queries=20)
  ├─> __generate_random_queries(index, num_queries)  # Query types:
  │    ├── match
  │    ├── bool
  │    ├── match_phrase
  │    ├── prefix
  │    ├── fuzzy
  │    ├── conjunction
  │    ├── disjunction
  │    ├── wildcard
  │    ├── regexp
  │    ├── query_string
  │    ├── numeric_range
  │    └── date_range
  ├─> cb_cluster.async_run_fts_query_compare() [fts_base.py]
  │    ├── Run query on Couchbase FTS → get CB doc IDs
  │    ├── Run query on Elasticsearch (es.search()) → get ES doc IDs
  │    └─> Compare doc_id sets
  └─> Fail if mismatch (raise Exception with failed query indices)
```

#### CRUD Sync Operations (During/After Upgrade)

```
async_perform_update_delete()
  └─> Update operations:
        └─> es.async_bulk_load_ES(index_name=self.es_index_name, op_type='update')
  └─> Delete operations:
        └─> es.async_bulk_load_ES(index_name=self.es_index_name, op_type='delete')
  └─> Expire operations (CB expiry → ES delete):
        └─> es.async_bulk_load_ES(index_name=self.es_index_name, op_type='delete')
```

#### Elasticsearch API Operations Summary

| Operation | ES API Call | Description |
|-----------|-------------|-------------|
| **Create Index** | `PUT /{es_index_name}` | Create with custom analyzer settings |
| **Delete Index** | `DELETE /{es_index_name}` | Cleanup before test |
| **Bulk Load** | `POST /{es_index_name}/_bulk` | Load 1000-10000+ JSON documents |
| **Search/Query** | `POST /{es_index_name}/_search` | Run full-text queries with size=1000000 |
| **Refresh Index** | `POST /{es_index_name}/_refresh` | Force index refresh after updates |
| **Get Count** | `GET /{es_index_name}/_count` | Verify document count |
| **Enable Scroll** | `PUT /{es_index_name}/_settings` | Increase max_result_window for large result sets |

#### Elasticsearch Cluster Management

**IMPORTANT:** The test code **does NOT create or initialize Elasticsearch clusters**. It assumes ES is pre-installed, configured, and running.

```
ElasticSearchBase.__init__()
  └─> Only: HTTP connection to existing ES cluster
       └─> self.__connection_url = 'http://{ip}:{port}/'
       └─> NO cluster creation/initialization

(Optional) restart_es() [es_base.py]
  └─> Called by: fts_base.py (in FTSBaseTest.setUp)
  └─> RemoteMachineShellConnection(self.__host)
  └─> shell.execute_non_sudo_command("/etc/init.d/elasticsearch restart")
  └─> Purpose: Restart ES SERVICE on given node (NOT cluster)

is_running() [es_base.py]
  └─> Health check: GET {host}:9200/
  └─> Purpose: Verify ES is reachable before operations
  └─> NO cluster creation/initialization
```

#### Cluster Management: What Code Does vs. Does NOT Do

| Operation | Code Does It? | Notes |
|-----------|---------------|-------|
| Create ES cluster | ❌ NO | Assumes existing cluster |
| Initialize ES cluster | ❌ NO | Assumes cluster is running |
| Configure cluster settings | ❌ NO | Uses default settings |
| Add/remove ES nodes | ❌ NO | Assumes static node list |
| Set up shards/replicas | ❌ NO | Uses ES defaults |
| Create ES indices | ✅ YES | Within existing cluster |
| Delete ES indices | ✅ YES | Cleanup operations |
| Restart ES service | ✅ YES | Optional per-node restart |
| Load data to ES | ✅ YES | Via HTTP bulk API |
| Check ES health | ✅ YES | Health check only |

#### Elasticsearch Prerequisites

Before running tests with `es_validate=True`, ensure:

```bash
# Elasticsearch must be installed and running
systemctl status elasticsearch   # or: /etc/init.d/elasticsearch status

# ES node must be accessible
curl http://<es_ip>:9200/

# Sufficient memory allocation
ES_JAVA_OPTS="-Xms4g -Xmx4g"    # Default 1GB may be insufficient

# Network connectivity
telnet <es_ip> 9200
```

The `[elastic]` INI configuration points to an **existing, running ES instance** - the code does not provision or manage the ES cluster infrastructure.

## Event System

### Event Parameters

```python
initialize_events = "kv_ops_initialize-create_fts_index_query_compare"
before_events = ""  # Optional events before upgrade
in_between_events = ""  # Optional events during upgrade (online only)
after_events = "rebalance_in-run_fts_query_and_compare"
```

### Event Separator: `-` (dash)

Events can be chained (run in sequence): `event1-event2-event3`

### Common Upgrade Events

| Event | Function | Location |
|-------|----------|----------|
| `kv_ops_initialize` | Load KV data | newupgradebasetest.py |
| `create_fts_index_query_compare` | Create FTS index, load data, validate queries | newupgradebasetest.py |
| `run_fts_query_and_compare` | Post-upgrade query validation | newupgradebasetest.py |
| `rebalance_in` | Add node to cluster | newupgradebasetest.py |
| `rebalance_out` | Remove node from cluster | newupgradebasetest.py |
| `create_index` | Create GSI/N1QL index | upgrade_tests.py |
| `create_views` | Create MapReduce views | upgrade_tests.py |
| `create_eventing_services` | Setup eventing functions | upgrade_tests.py |
| `create_cbas_services` | Setup Analytics services | upgrade_tests.py |

### Event Execution

```
run_event(events)
  └─> Split events by "-"
  └─> For each event:
       ├── If contains "-" → run_event_in_sequence()
       └─> If single event → find_function(event) + thread
  └─> return thread_list
```

## Upgrade Types

### Online Upgrade

```python
upgrade_type = "online"
online_upgrade_type = "swap"  # or "incremental"
```

- **Swap Rebalance:** Remove old nodes, add new upgraded nodes simultaneously
- **Incremental:** Upgrade nodes one by one, keeping cluster quorum

### Offline Upgrade

```python
upgrade_type = "offline"
offline_upgrade_type = "offline_shutdown"  # or "offline_failover"
```

- **Offline Shutdown:** Stop cluster, upgrade all nodes, restart
- **Offline Failover:** Graceful failover nodes, upgrade, add back

## Cluster Ops Upgrade Checks (`upgrade_cluster_ops.py`)

`UpgradeClusterOpsTests(UpgradeTests)` adds events for `test_upgrade` and the standalone `test_node_join_compat`.

| Event | Use as | Checks |
|-------|--------|--------|
| `capture_cluster_snapshot` | initialize | Records the service topology (per-node service sets) and bucket list |
| `configure_cluster_settings` | initialize | Sets n2n encryption (`n2n_level`), auto-failover, service quotas, `tlsMinVersion`, retry-rebalance, and stores what was read back |
| `verify_cluster_settings_persisted` | in_between / after | Settings and topology match what was stored; n2n is on for every node; strict mode passes `check_if_services_obey_tls` |
| `verify_mixed_mode_compat` | in_between | Compat stays at the source version until every node is upgraded; data writes still work |
| `verify_cluster_health_post_upgrade` | after | All nodes active and healthy on the target version, compat at the target, no rebalance running, orchestrator present, topology and buckets unchanged |
| `verify_rebalance_orchestration` | after | Rebalance out, rebalance back in, then a swap with a free target-version node |
| `verify_autofailover_orchestration` | in_between / after | A stopped kv node is auto-failed-over within timeout + `af_tolerance`, then recovered (`af_recovery_type`) |
| `verify_fts_index_post_upgrade` | after | The index from `create_fts_index` still indexes every doc in its bucket and `match_all` returns them all. Used instead of `run_fts_query_and_compare`, which needs an `[elastic]` node |
| `setup_ldap_tls12_endpoint` | initialize | Starts a TLS 1.2-only openldap container (docker on `ldap_host`, default the initial master), proves it refuses TLS 1.3, and points the cluster's LDAP at it |
| `verify_ldap_connectivity` | in_between / after | Checks the LDAP config left applied last time persisted, then applies each `ldap_variants` config (`default`, `tls12`, `tls12_middlebox`) and runs `settings/ldap/validate/connectivity` on every node. `tls12` / `tls12_middlebox` failures on affected builds (7.6.8-7.6.10, 8.0.0-8.0.2: middlebox_comp_mode is always passed, and Erlang 27+ rejects it with TLS 1.2 only) are logged, not failed |
| `verify_old_node_join_rejected` | after | Installs `join_version` (default: the source version) on a free node; addNode must fail with `expected_join_error` (default "compatibility mismatch") and leave the topology unchanged |
| `configure_rbac_roles` | initialize | Creates groups and local/external users with the roles ns_server migrates (`security_admin_local`, `security_admin_external`, `ro_admin`, ...) plus controls (`admin`, `data_reader`). On 7.9+ sources the deprecated roles are created in their substituted form |
| `verify_rbac_roles` | in_between / after | Roles match `menelaus_users:upgrade_props` for the current compat: unchanged in mixed mode; at 7.9 (EE) `security_admin_local` -> `security_admin`+`user_admin_local`, `security_admin_external` -> `security_admin`+`user_admin_external`, `ro_admin` -> +`ro_security_admin`; at 8.5 `ui_access` is added to holders of a pre-8.5 UI role (not `admin`). The local user must still be able to list users |

- A failed event is recorded and the rest of its `-` chain still runs, so one upgrade reports every check. Failures are asserted when the test ends; on its own, an event inside the upgrade thread cannot fail the test.
- `verify_cluster_settings_persisted` also handshakes TLS 1.1/1.2/1.3 against 18091 on every node to check `tlsMinVersion` is still enforced.
- Put `configure_cluster_settings` in `initialize_events`, not `before_events`. A single `before_events` entry runs concurrently with the upgrade.
- `online_upgrade_type=failover_recovery`: upgrades one node at a time with graceful failover (hard for non-kv nodes), an in-place package upgrade and an add back with `upgrade_recovery_type` (default `delta`). The test master is upgraded last.
- `online_upgrade_type=incremental` (overridden here): one node at a time, rebalance out, in-place package upgrade, rebalance back in with the same services, master last. The base `online_upgrade_incremental` re-adds nodes as kv only and drops the master.
- Multi-hop: `upgrade_path=<hop>` is upgraded before `upgrade_version`. This works for swap, failover_recovery, incremental and offline, with a version and compat check after each hop.
- `test_node_join_compat`: no upgrade. Installs `join_version` (default: the upgrade version) on a free node and calls addNode against an `initial_version` cluster. With `expect_join=False` the join must be rejected with `expected_join_error` (default "is not supported", the below-compat-floor message).
- Design: an upgrade is the expensive step, so each conf row is one scenario (source x upgrade mode x n2n level) with all the cheap checks stacked on it, rather than one row per check.
- Confs: `conf/upgrade/upgrade_cluster_ops_from_76x_80x.conf` (P0, every build), `upgrade_cluster_ops_from_76x_80x_extended.conf` (P1), `upgrade_min_path_compat.conf` (join compat floor, no upgrade), `upgrade_ce_cluster_ops_from_76x_80x.conf`. Pass the target with `-p upgrade_version=` (latest Totoro: `python3 ../TAF/scripts/latest_builds.py --branches totoro --top 1`).
- Community edition: `upgrade_build_type=community` (or `initial_build_type=community`) makes `_get_build` fetch CE packages, so offline CE upgrades work. `ce_to_ee=True` still fetches EE.

## Required Test Parameters

```python
# Minimum required for FTS with ES validation
upgrade_test=True
skip_init_check_cbserver=True  # Bypass ns_server check
initialize_events="create_fts_index_query_compare"
after_events="run_fts_query_and_compare"

# FTS-specific
initial-services-setting="kv,fts-kv,n1ql"
num_indexes=2
num_items=10000

# Elasticsearch: Must include in INI file
# [elastic]
# ip:x.x.x.x
# port:9200
```

## Common Test Patterns

### FTS Upgrade with ES Validation

```python
-t upgrade.upgrade_tests.UpgradeTests.test_upgrade \
  ,items=10000 \
  ,initial_version=5.5.0-2958 \
  ,nodes_init=3 \
  ,initialize_events=create_fts_index_query_compare \
  ,initial-services-setting=kv,fts-kv,fts \
  ,upgrade_services_in=same \
  ,after_events=rebalance_in-run_fts_query_and_compare \
  ,after_upgrade_services_in=fts \
  ,upgrade_type=online \
  ,es_validate=True \
  ,upgrade_test=True \
  ,released_upgrade_version=6.5.0-3265 \
  ,skip_init_check_cbserver=true
```

### Vector Search Upgrade

```python
,vector_upgrade=true \
,target_bucket=default \
,target_scope=_default \
,target_collection=_default
```

## Inheritance Chain

```
NewUpgradeBaseTest
  ├─> create_fts_index_query_compare()
  ├─> run_fts_query_and_compare()
  ├─> create_fts_vector_index_query_compare()
  ├─> run_event(), run_event_in_sequence()
  ├─> find_function()
  └─> cluster operations, rebalance helpers

UpgradeTests extends NewUpgradeBaseTest
  ├─> setUp() - FTSCallable with es_validate=True
  ├─> test_upgrade() - Main test flow
  └─> Event implementations

UpgradeTestsCollections extends NewUpgradeBaseTest
  └─> Collection-aware upgrade tests
```

## Key Classes

| Class | File | Purpose |
|-------|------|---------|
| `UpgradeTests` | upgrade_tests.py | Main upgrade test class |
| `NewUpgradeBaseTest` | newupgradebasetest.py | Base class with shared logic |
| `FTSCallable` | fts/fts_callable.py | FTS wrapper with ES integration |
| `ElasticSearchBase` | fts/es_base.py | ES HTTP client and operations |

## Common Issues

### Elasticsearch Connection Failures

**Symptom:** Test fails with "For ES result validation, pls add elastic search node in the .ini file."

**Fix:** Ensure INI file contains:
```ini
[elastic]
ip:<es_server_ip>
port:9200
```

### Query Comparison Failures

**Symptom:** "X out of 20 queries failed! - [1, 5, 7]"

**Causes:**
- CB FTS and ES analyzer configuration mismatch
- Schema/index definition differences
- Tokenization behavior variations

**Debug:** Check logs for:
- FTS query vs ES query JSON
- Document IDs returned by each system
- Analyzer settings on both systems

### Indexing Timeout

**Symptom:** Index count never stabilizes

**Fix:**
- Increase `index_retry` parameter
- Check FTS node health
- Verify sufficient cluster resources

## Testing Best Practices

1. **Always verify ES connectivity** before running `es_validate=True` tests
2. **Clear ES indices** before test runs (FTSCallable does this on init)
3. **Use consistent analyzers** between CB FTS and ES for fair comparison
4. **Monitor document counts** in both systems after each operation
5. **Run without ES first** to isolate Couchbase-side issues

## Validation Commands

```bash
# Run upgrade test with FTS ES validation
./testrunner -i b/resources/dev.ini \
  -t upgrade.upgrade_tests.UpgradeTests.test_upgrade \
  ,initialize_events=create_fts_index_query_compare \
  ,after_events=run_fts_query_and_compare \
  ,es_validate=true \
  ,upgrade_test=true

# Check ES index
curl -X GET "http://<es_ip>:9200/{es_index_name}/_count?pretty"

# Run query on ES
curl -X GET "http://<es_ip>:9200/{es_index_name}/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match": {"name": "John"}}}'
```

## References

- Main AGENTS.md: `AGENTS.md`
- FTS Tests: `pytests/fts/`
- ES Client: `pytests/fts/es_base.py`
- FTS Callable: `pytests/fts/fts_callable.py`
- FTS Base: `pytests/fts/fts_base.py`
