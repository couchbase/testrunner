# AGENTS.md — Eventing Tests

**Eventing Service**: https://www.couchbase.com/products/eventing/
**REST API**: https://docs.couchbase.com/server/current/eventing-rest-api/

---

## Naming & Suites

Files: `eventing_<suite>.py` — e.g. `eventing_sanity.py`, `eventing_rebalance.py`

Suites: sanity, rebalance, failover, recovery, lifecycle, timers, n1ql, analytics, curl, bucket, collections, ondeploy, security, upgrade

---

## Inheritance

```
BaseTestCase (basetestcase.py)
└── QueryHelperTests (pytests/query_tests_helper.py)
    └── EventingBaseTest (pytests/eventing/eventing_base.py)
        └── Eventing Test Classes (pytests/eventing/*.py)
```

---

## Key Files

| File | Role |
|------|------|
| `eventing_base.py` | All shared methods: deploy, verify, load data, debug |
| `eventing_constants.py` | `HANDLER_CODE*` classes mapping constants to `.js` paths |
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
