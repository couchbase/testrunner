# AGENTS.md — Eventing Tests

**Eventing Service**: https://www.couchbase.com/products/eventing/
**REST API**: https://docs.couchbase.com/server/current/eventing-rest-api/

---

## Naming & Suites

Files: `eventing_<suite>.py` — e.g. `eventing_sanity.py`, `eventing_rebalance.py`

Suites: sanity, rebalance, failover, recovery, lifecycle, timers, n1ql, analytics, curl, bucket, collections, ondeploy, security, upgrade

---

## Eventing Upgrade Tests

### Overview

Eventing upgrade tests validate eventing service functionality before, during, and after Couchbase server upgrades. Tests support online and offline upgrades with various rebalance strategies.

### Test Classes

| Class | File | Purpose |
|-------|------|---------|
| `EventingUpgrade` | `eventing_upgrade.py` | Eventing upgrade tests |
| `NewUpgradeBaseTest` | `upgrade/newupgradebasetest.py` | Base upgrade class |
| `EventingBaseTest` | `eventing_base.py` | Base eventing class |

**Note:** `EventingUpgrade` uses **multiple inheritance**: `eventing_upgrade.EventingUpgrade(NewUpgradeBaseTest, EventingBaseTest)`

### Available Upgrade Tests

```
eventing_upgrade.py:test_offline_upgrade_with_eventing
eventing_upgrade.py:test_online_upgrade_with_regular_rebalance_with_eventing
eventing_upgrade.py:test_online_upgrade_with_swap_rebalance_with_eventing
eventing_upgrade.py:test_online_upgrade_with_failover_rebalance_with_eventing
eventing_upgrade.py:test_offline_upgrade_with_eventing_pause_resume
eventing_upgrade.py:test_online_upgrade_with_regular_rebalance_with_eventing_pause_resume
eventing_upgrade.py:test_online_upgrade_with_failover_rebalance_with_eventing_base64_xattrs
```

### Test Call Map: Offline Upgrade with Eventing

```
test_offline_upgrade_with_eventing()
  ├─> _install(servers[:nodes_init])  # Install initial version
  ├─> operations(servers, services="kv,kv,eventing,index,n1ql")  # Initialize cluster
  ├─> create_buckets()
  ├─> load(gens_load, buckets=src_bucket)  # Load data in old version
  ├─> pre_upgrade_handlers()  # Deploy functions before upgrade
  │   ├─> create_handler("bucket_op", "handler_code/delete_doc_bucket_op.js")
  │   ├─> create_handler("timers", "handler_code/bucket_op_with_timers_upgrade.js")
  │   └─> deploy_handler_by_name("timers")
  ├─> print_eventing_stats_from_all_eventing_nodes()  # Capture pre-upgrade stats
  ├─> _async_update(upgrade_version, servers)  # Perform upgrade
  │   └─> _upgrade() on each node (stops server, installs, starts)
  ├─> wait_for_handler_state("timers", "deployed")
  ├─> deploy_handler_by_name("bucket_op")
  ├─> verify_doc_count_collections()  # Validate data integrity
  ├─> post_upgrade_handlers()  # Create post-upgrade functions
  │   ├─> create_function_with_collection("sbm", ...)  # Source bucket mutation
  │   ├─> create_function_with_collection("curl", ...)  # CURL operations
  │   └─> create_function_with_collection("n1ql", ...)  # N1QL operations
  ├─> load_data_to_collection() / delete operations
  ├─> pause_handler_by_name() all handlers
  ├─> load_data_to_collection() during pause
  ├─> resume_handler_by_name() all handlers
  ├─> verify_count()  # Final validation
  └─> print_eventing_stats_from_all_eventing_nodes()
```

### Test Call Map: Online Upgrade with Regular Rebalance

```
test_online_upgrade_with_regular_rebalance_with_eventing()
  ├─> _install(servers[:nodes_init])  # Install initial version on first nodes
  ├─> _install(servers[nodes_init:])  # Install upgrade version on swap nodes
  ├─> operations(servers, services="kv,kv,eventing,index,n1ql")
  ├─> create_buckets()
  ├─> load(gens_load, buckets=src_bucket)
  ├─> pre_upgrade_handlers()
  ├── print_eventing_stats_from_all_eventing_nodes()
  ├─> online_upgrade(services=["kv","kv","eventing","index","n1ql"])
  │   └─> NewUpgradeBaseTest.online_upgrade_swap_rebalance()
  │        ├── Rebalance out old nodes 1 by 1
  │        ├── Start upgrade nodes (already has new version)
  │        └─> Rebalance in new nodes 1 by 1
  ├─> add_built_in_server_user()
  ├─> wait_for_handler_state("timers", "deployed")
  ├─> deploy_handler_by_name("bucket_op")
  ├─> verify_doc_count_collections()
  ├─> post_upgrade_handlers()  # Install collection-based handlers
  ├─> CRUD operations with data validation
  ├─> pause/resume cycles with validation
  └─> finalize and cleanup
```

### Pre-Upgrade Handlers

```
pre_upgrade_handlers()
  └─> Non-collection scoped handlers (bucket-level)
       ├─> create_handler("bucket_op", "handler_code/delete_doc_bucket_op.js")
       │   └─> Basic bucket operations: src_bucket → dst_bucket
       └─> create_handler("timers", "handler_code/bucket_op_with_timers_upgrade.js")
            └─> Timer-based operations with doc timers
```

### Post-Upgrade Handlers

```
post_upgrade_handlers()
  └─> Collection-scoped handlers (post-upgrade feature)
       ├─> create_function_with_collection("sbm", "handler_code/ABO/insert_sbm.js")
       │   └─> Source bucket mutation (SBM) mode
       │   └─> Namespace: source_bucket_mutation.event.coll_0
       ├─> create_function_with_collection("curl", "handler_code/ABO/curl_get.js")
       │   └─> CURL HTTP operations
       │   └─> Namespace: src_bucket.event.coll_0
       └─> create_function_with_collection("n1ql", "handler_code/collections/n1ql_insert_update.js")
            └─> N1QL INSERT/UPDATE operations
            └─> Namespace: src_bucket.event.coll_0
```

### Key Eventing Upgrade Methods

| Method | Location | Purpose |
|---------|----------|---------|
| `pre_upgrade_handlers()` | eventing_upgrade.py | Deploy non-collection handlers before upgrade |
| `post_upgrade_handlers()` | eventing_upgrade.py | Deploy collection handlers after upgrade |
| `deploy_handler_by_name(name)` | eventing_base.py | Deploy function by name |
| `pause_handler_by_name(name)` | eventing_base.py | Pause eventing function |
| `resume_handler_by_name(name)` | eventing_base.py | Resume paused function |
| `wait_for_handler_state(name, status)` | eventing_base.py | Poll until handler reaches status |
| `print_eventing_stats_from_all_eventing_nodes()` | eventing_base.py | Dump stats from all eventing nodes |
| `refresh_rest_server()` | eventing_base.py | Refresh connection after rebalance |

### Handler State Transitions During Upgrade

```
Pre-Upgrade State:
  bucket_op → deployed
  timers → deployed

During Offline Upgrade:
  bucket_op → undeployed (server stopped)
  timers → undeployed (server stopped)

After Upgrade:
  timers → deployed (auto-resumed or explicitly deployed)
  bucket_op → deployed (explicitly deployed)

Post-Upgrade (Collection SC):
  sbm → deployed
  curl → deployed
  n1ql → deployed

Pause/Resume Cycles:
  All handlers → paused
  [Load data while paused]
  All handlers → deployed
  [Verify data after resume]
```

### Upgrade Parameters

```ini
# Upgrade-specific
upgrade_version=7.0.0-5000
exported_handler_version=6.6.1
enable_n2n_encryption_and_tls=False

# Services (must include eventing)
services_init=kv,kv,eventing,index,n1ql

# Buckets
src_bucket_name=src_bucket
dst_bucket_name=dst_bucket
metadata_bucket_name=metadata
sbm=source_bucket_mutation  # SBM bucket
n1ql_op_dst=n1ql_op_dst     # N1QL ops destination
dst_bucket_curl=dst_bucket_curl

# Collections
use_single_bucket=True
non_default_collection=True
global_function_scope=True

# Eventing settings
eventing_log_level=INFO
dcp_stream_boundary=everything
commit_retry_timeout=20
checkpoint_interval=20000
```

### Running Eventing Upgrade Tests

```bash
# Offline upgrade with eventing
python3 testrunner.py -i b/resources/4-nodes-template.ini \
  -t pytests.eventing.eventing_upgrade.EventingUpgrade.test_offline_upgrade_with_eventing \
  ,nodes_init=4,upgrade_version=7.0.0-5000,initial_version=6.6.1-9202 \
  ,services_init=kv,kv,eventing,index,n1ql,doc-per-day=1

# Online upgrade with regular rebalance
python3 testrunner.py -i b/resources/4-nodes-template.ini \
  -t pytests.eventing.eventing_upgrade.EventingUpgrade.test_online_upgrade_with_regular_rebalance_with_eventing \
  ,nodes_init=4,upgrade_version=7.0.0-5000,initial_version=6.6.1-9202 \
  ,services_init=kv,kv,eventing,index,n1ql

# Offline upgrade with pause/resume
python3 testrunner.py -i b/resources/4-nodes-template.ini \
  -t pytests.eventing.eventing_upgrade.EventingUpgrade.test_offline_upgrade_with_eventing_pause_resume \
  ,nodes_init=4,upgrade_version=7.0.0-5000,pause_resume=True
```

### Inheritance

```
BaseTestCase (basetestcase.py)
└── QueryHelperTests (pytests/query_tests_helper.py)
    ├── NewUpgradeBaseTest (upgrade/newupgradebasetest.py)
    │   └─> upgrade workflow methods: _upgrade(), _async_update(), online_upgrade(), etc.
    └── EventingBaseTest (pytests/eventing/eventing_base.py)
         └─> eventing methods: deploy_function(), verify_eventing_results(), etc.
             └── EventingUpgrade (pytests/eventing/eventing_upgrade.py)
                  └─> Multiple inheritance: EventingUpgrade(NewUpgradeBaseTest, EventingBaseTest)
                       └─> Combines upgrade + eventing capabilities
```

---

## Key Files

| File | Role |
|------|------|
| `eventing_base.py` | All shared methods: deploy, verify, load data, debug |
| `eventing_upgrade.py` | Eventing upgrade tests (offline/online) |
| `eventing_constants.py` | `HANDLER_CODE*` classes mapping constants to `.js` paths |
| `upgrade/newupgradebasetest.py` | Base class for upgrade tests |
| `lib/membase/api/on_prem_rest_client.py` | REST API calls to eventing endpoints |
| `lib/membase/api/rest_client.py` | `RestConnection` used across all tests |
| `handler_code/*.js` | JavaScript handler functions (160+ files) |
| `conf/eventing/*.conf` | Test configuration files (~50 suites) |

---

## Handler Code

Every eventing function is a JavaScript file under `pytests/eventing/handler_code/`.

### Constants (`eventing_constants.py`)

```python
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL, HANDLER_CODE_ONDEPLOY, HANDLER_CODE_ANALYTICS

body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
```

Constant classes: `HANDLER_CODE`, `HANDLER_CODE_CURL`, `HANDLER_CODE_ONDEPLOY`, `HANDLER_CODE_FTS_QUERY_SUPPORT`, `HANDLER_CODE_ANALYTICS`, `HANDLER_CODE_ERROR`

**Adding a new handler**: create `.js` in `handler_code/`, add a constant in the relevant class in `eventing_constants.py`.

---

## Handler Types

### Callbacks

| Callback | Triggered by |
|----------|-------------|
| `OnUpdate(doc, meta)` | KV create/update mutation |
| `OnDelete(meta, options)` | KV delete or expiry |
| `OnDeploy(action)` | Function deploy/resume |

### Basic Bucket Op
```javascript
// HANDLER_CODE.BUCKET_OPS_ON_UPDATE
function OnUpdate(doc, meta) { dst_bucket[meta.id] = doc; }
function OnDelete(meta, options) { delete dst_bucket[meta.id]; }
```

### Advanced Bucket Accessors (ABO)
ABO uses `couchbase.*` functions instead of bracket notation and returns a result object `{success, doc, meta: {cas}}`.

```javascript
// handler_code/ABO/upsert.js  (no constant defined — reference file directly)
function OnUpdate(doc, meta) {
    var result = couchbase.get(src_bucket, meta);  // returns {success, doc, meta: {cas}}
    if (result.success && result.meta.cas != undefined) {
        couchbase.upsert(dst_bucket, meta, doc);
    }
}
function OnDelete(meta, options) {
    couchbase.delete(dst_bucket, {id: meta.id});
}
```

### Doc Timer
```javascript
// HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER
function OnUpdate(doc, meta) {
    var expiry = new Date(); expiry.setSeconds(expiry.getSeconds() + 5);
    createTimer(timerCallback, expiry, meta.id, {docID: meta.id});
}
function timerCallback(ctx) { dst_bucket[ctx.docID] = "fired"; }
```

### Cron Timer
```javascript
// HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMER  — createTimer with a cron-style Date
```

### CURL
```javascript
// HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_GET
function OnUpdate(doc, meta) {
    var res = curl("GET", server, {path: '/endpoint'});
    if (res.status == 200) dst_bucket[meta.id] = res.body;
}
```

### N1QL
```javascript
// HANDLER_CODE.N1QL_INSERT_ON_UPDATE
function OnUpdate(doc, meta) {
    var q = INSERT INTO dst_bucket (KEY, VALUE) VALUES ($meta.id, 'value');
}
```

### Analytics
```javascript
// HANDLER_CODE_ANALYTICS.ANALYTICS_BASIC_SELECT
function OnDeploy(action) {
    for (let row of couchbase.analyticsQuery("SELECT * FROM `b`.`s`.`c` LIMIT 10")) { log(row); }
}
```

### crc64 / Base64
```javascript
couchbase.crc_64_go_iso(value)          // HANDLER_CODE.BASIC_BUCKET_ACCESSORS_CRC
couchbase.base64Encode(str)             // HANDLER_CODE.BASIC_BUCKET_ACCESSORS_BASE64
```

### Source Bucket Mutation (SBM)
Handler modifies its own source bucket. Requires `src_bucket` alias bound `rw` in `depcfg`.
```javascript
// HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION
function OnUpdate(doc, meta) { src_bucket[meta.id] = {updated: true}; }
```

### Other handler paths
| Type | Path |
|------|------|
| Subdoc | `handler_code/ABO/basic_bucket_acessors_subdoc_operations.js` |
| XATTRs | `handler_code/ondeploy_user_xattrs.js` |
| Constant binding | `handler_code/fts_query_support/match_query_constant_binding.js` |
| Bucket cache | `handler_code/fts_query_support/match_query_with_bucket_cache_getop.js` |
| Counter | `handler_code/fts_query_support/match_query_counter.js` |

---

## Test Pattern

```python
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.eventing.eventing_constants import HANDLER_CODE

class MyEventingTest(EventingBaseTest):
    def setUp(self):
        super(MyEventingTest, self).setUp()

    def test_basic_eventing(self):
        body = self.create_save_function_body(
            self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_TIMERS,
            dcp_stream_boundary="from_now", worker_count=3, cpp_worker_thread_count=1
        )
        self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * 2016, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)
```

### `EventingBaseTest` Methods

| Method | Description |
|--------|-------------|
| `create_save_function_body(appname, appcode, ...)` | Build + save function with bindings/settings |
| `deploy_function(body)` | Deploy and wait for bootstrap |
| `undeploy_and_delete_function(body)` | Undeploy then delete |
| `pause_function(body)` / `resume_function(body)` | Pause/resume lifecycle |
| `wait_for_handler_state(name, status)` | Poll until `deployed` / `undeployed` / `paused` |
| `verify_eventing_results(name, expected_count)` | Wait and verify DCP mutation count |
| `verify_doc_count_collections(keyspace, expected)` | Verify doc count in `bucket.scope.collection` |
| `load_data_to_collection(doc_count, keyspace)` | Load docs into a collection |
| `print_execution_and_failure_stats(name)` | Print OnUpdate success/failure stats |
| `print_eventing_stats_from_all_eventing_nodes()` | Dump full eventing stats from all nodes |
| `check_eventing_logs_for_panic()` | Scan eventing.log for panics/core dumps |
| `print_app_logs(name)` | Print handler application logs |
| `handler_status_map()` | Return `{name: composite_status}` for all handlers |
| `refresh_rest_server()` | Refresh REST connection (call after rebalance) |

---

## Configuration Parameters

```ini
# Cluster
nodes_init=2                      # total nodes
services_init=kv-eventing         # e.g. kv-eventing, kv-eventing-index:n1ql
num_nodes_running=1               # eventing nodes (pins functions)
reset_services=True

# Buckets / Collections
use_single_bucket=True            # default.scope0.collection{0,1,2}
non_default_collection=True       # src_bucket.src_bucket.src_bucket layout
global_function_scope=True        # function scope = (*,*)
src_bucket_name=src_bucket
dst_bucket_name=dst_bucket
metadata_bucket_name=metadata

# Function
dcp_stream_boundary=from_now      # from_now | everything
doc-per-day=1                     # total docs = value * 2016
worker_count=3
eventing_log_level=INFO           # INFO | DEBUG | TRACE
java_sdk_client=True

# Advanced
source_bucket_mutation=True       # SBM mode
curl=True                         # enable CURL binding
pause_resume=True
binary_doc=True
skip_cleanup=False
```

---

## Running Tests

```bash
# Single test
python3 testrunner.py -i b/resources/2-nodes-template.ini \
  -t pytests.eventing.eventing_sanity.EventingSanity.test_create_mutation_for_dcp_stream_boundary_from_beginning,nodes_init=2,services_init=kv-eventing,dataset=default,groups=simple,reset_services=True,java_sdk_client=True,use_single_bucket=True

# Full suite
python3 testrunner.py -i b/resources/2-nodes-template.ini -c conf/eventing/eventing_sanity.conf
python3 testrunner.py -i b/resources/4-nodes-template.ini -c conf/eventing/eventing_rebalance.conf
```

**INI templates**: `b/resources/<n>-nodes-template.ini` 

---

## Security

- `.ini` files contain credentials — never commit production `.ini` files
- CURL handlers: set `validate_ssl=True` for HTTPS endpoints
- N2N encryption: `enable_n2n_encryption_and_tls` param + `setup_nton_cluster()` in base
