# AGENTS.md - N1QL Query Test Suite

## Subdirectory Purpose

`pytests/tuqquery/` contains 125+ test files (~120 test classes) for comprehensive Couchbase Query Service (N1QL/SQL++) integration testing. Coverage spans core query operations, indexing, DML/DDL, joins, window functions, UDF, vector search, collections, security, RBAC, FTS integration, and performance monitoring.

## Core Commands

```bash
# Run all tuqquery tests with meta configuration
cd testrunner
python testrunner.py -C conf/tuq/meta.conf

# Run specific configuration
python testrunner.py -c conf/tuq/py-tuq-ext-sanity.conf
python testrunner.py -c conf/tuq/py-tuq-joins.conf

# Run specific test class
python testrunner.py -t tuqquery.tuq_sanity.QuerySanityTests
python testrunner.py -t tuqquery.tuq_vectorsearch.VectorSearchTests.test_ann_search

# Run by priority group
python testrunner.py -C conf/tuq/meta.conf -group=P0
python testrunner.py -C conf/tuq/meta.conf -group=P1

# Run with parameters
python testrunner.py -t tuqquery.tuq_vectorsearch.VectorSearchTests.test_ann_search -p distance=L2,nprobes=5
```

## Repository Layout

| Path | Description |
|------|-------------|
| `pytests/tuqquery/` | 125+ test files for all Query Service features |
| `conf/tuq/` | 185+ configuration files defining test suites and parameters |
| `tuq.py` | Base test class `QueryTests` extending `BaseTestCase` |
| `tuq_sanity.py` | `QuerySanityTests` for core sanity testing |
| `tuq_vectorsearch.py` | Vector search (KNN/ANN) single-file comprehensive suite |
| `n1ql_collections_ddl.py` | Collections DDL operations |
| `n1ql_window_functions.py` | Window functions (RANK, DENSE_RANK, etc.) |
| `serverless/` | Serverless-specific query tests (metering, throttling) |

**Test inheritance:** `BaseTestCase` → `QueryTests` → `QuerySanityTests` or feature-specific class

## Development Patterns and Constraints

- **Python:** 3.10.13 (same as main testrunner)
- **All tests require running Couchbase cluster** with query service enabled
- **Working directory must be testrunner root** for imports (`lib/`, `pytests/` on sys.path)
- **No linting or type checking** configured
- **Configuration-driven testing:** Test behavior controlled via `conf/tuq/*.conf` files
- **Test naming convention:** `tuq_<feature>.py` or `n1ql_<feature>.py`
- **Parameter access:** Use `self.input.param("key", default)` to retrieve config values
- **Priority system:** P0 (critical), P1 (high), P2 (medium), P3 (low)
- **Group labels:** CE/NON_CE, feature tags (LIKE, DATE, SCALAR, GROUP, P0)

## Configuration System

Tests are driven by INI-style configuration files in `conf/tuq/`:

```ini
tuqquery.tuq_sanity.QuerySanityTests:
    test_meta_basic,skip_index=True,doc-per-day=1,covering_index=True,GROUP=P0
    test_meta,doc-per-day=1,covering_index=True,GROUP=P0
```

**Common parameters:**
- `skip_index`: Skip index creation
- `doc-per-day`: Documents generated per day
- `covering_index`: Use covering index optimization
- `GROUP`: Priority level (P0, P1, P2, P3)
- `BUGS`: Associated bug IDs
- `reload_data`: Reload test data before execution
- `distance`, `nprobes`, `description`: Vector search parameters

## Base Classes and Helpers

### QueryTests (tuq.py)
Main base class for all query tests. Provides:
- Query execution: `self.run_cbq_query(query="SELECT ...")`
- Index management: `self.create_index()`, `self.drop_index()`, `self.wait_for_all_indexes_online()`
- Result verification: `self.assertEqual()`, DeepDiff for JSON comparison
- Data loading: `self.load_data()`
- Utility methods: `self.get_index_list()`, `self.verify_results()`

### QuerySanityTests (tuq_sanity.py)
Extends QueryTests for sanity testing. Provides:
- Basic query operations and META() function coverage
- Sample dataset loading (travel-sample, beer-sample)
- Setup/teardown for test environment

### Common Test Patterns

```python
# Execute query
result = self.run_cbq_query(query="SELECT * FROM bucket")
self.assertEqual(result['results'], expected)

# Create index
self.run_cbq_query(query="CREATE INDEX idx ON bucket(field)")
self.wait_for_all_indexes_online()

# Verify index count
index_list = self.get_index_list()
self.assertEqual(len(index_list), expected_count)
```

## Validation and Evidence Required

Before completing changes:

1. Run sanity tests: `python testrunner.py -c conf/tuq/meta.conf`
2. Run extended sanity: `python testrunner.py -c conf/tuq/py-tuq-ext-sanity.conf`
3. For feature-specific changes: Run matching conf file (e.g., `py-tuq-joins.conf` for joins)
4. Verify `tmp-<timestamp>/` contains xunit XML and test logs
5. Check pass/fail summary in test runner output
6. For index-related changes: Run `py-tuq-gsi.conf` or `py-tuq-index.conf`
7. For security changes: Run `py-n1ql-rbac.conf`
8. For vector search: Run `py-tuq-vector-ann.conf`, `py-tuq-vector-knn.conf`

**Code quality rules:**
- Follow existing naming: `tuq_<feature>.py` or `n1ql_<feature>.py`
- Extend `QueryTests` or `QuerySanityTests`; don't invent new base classes
- Reuse methods from base classes; avoid reinventing common patterns
- Match coding style of surrounding test files

## Security and Sensitive Paths

**Never commit:**
- INI files with real cluster IPs or credentials (use templates in `b/resources/`)
- Test logs containing cluster data
- API keys or cloud credentials

**Sensitive operations:**
- `n1ql_rbac*.py` -- RBAC tests requiring special cluster configs
- `tuq_n1ql_audit.py` -- Audit logging with sensitive event data
- `serverless/tuq_metering.py` -- Billing/cost-related data
- `tuq_curl*.py` -- External service calls requiring careful parameter sanitization

## Unknowns

- No documented CI/CD pipeline for tuqquery-specific tests
- Vector search dataset requirements (SIFT, sparse vectors) location unspecified
- AiQG (AI Query Generator) integration scripts in `scripts/AiQG/` not fully documented
- Test data generator locations and configurations not cataloged

## Supporting Context

**Comprehensive documentation within tuqquery:**
- `README_Query_Tests.md` -- Full test suite guide (125 test files, 185 configs, test categories, parameters, running tests)
- `README_Query_Analysis.md` -- Coverage analysis with gaps, strengths, weaknesses, recommendations, action plans

**Documentation in main testrunner (from project root):**
- `AGENTS.md` -- Overall testrunner framework, core commands, repository layout, submodule orchestration
- `docs/agent-context/architecture.agents.md` -- Module boundaries and runtime flows
- `docs/agent-context/repo-inventory.md` -- Languages, packages, key directories
- `docs/agent-context/build-test-matrix.md` -- Exact validation commands per service
- `docs/agent-context/domain-glossary.md` -- Couchbase services, test types, terminology

**Key test utilities:**
- `couchbase_helper/tuq_generators.py` -- N1QL data generators
- `couchbase_helper/documentgenerator.py` -- Document generators
- `lib/collection/collections_n1ql_client.py` -- Collection utilities
- `lib/vector/vector.py` -- Vector utilities

**External data for testing:**
- Sample datasets: travel-sample, beer-sample (loadable via `self.rest.load_sample()`)
- Vector datasets: SIFT (dense vectors), Sparse (sparse vectors) -- paths not documented
- AiQG: `scripts/AiQG/AiQG.py` -- AI-powered query generation using LangChain and OpenAI

## Test Configuration Categories

Major configuration buckets in `conf/tuq/`:
- **Sanity:** `meta.conf`, `py-tuq-ext-sanity.conf`, `py-tuq-ext-sanity-2.conf`, `py-tuq-ext-sanity-3.conf`
- **Indexing:** `py-tuq-index.conf`, `py-tuq-gsi.conf`, `py-covering-index.conf`, `py-tuq-array-flattening.conf`
- **Joins/DML:** `py-tuq-joins.conf`, `py-tuq-ansi-joins.conf`, `py-tuq-ansi-merge.conf`, `py-tuq-dml.conf`
- **Security:** `py-n1ql-rbac.conf`, `py-n1ql-rbac-collections.conf`, `py-tuq-n1ql-audit.conf`
- **Vector Search:** 8 specialized confs (`py-tuq-vector-ann.conf`, `py-tuq-vector-knn.conf`, `py-tuq-vector-ann-pushdown.conf`, etc.)
- **UDF:** `py-tuq-udf-n1ql.conf`, `py-tuq-udf-analytics.conf`, `py-tuq-backup-udf.conf`
- **Window Functions:** `py-tuq-window_functions.conf`, `py-tuq-window_clause.conf`
- **FTS Integration:** `n1ql_fts_integration_phase1_P1.conf`, `n1ql_fts_integration_phase2_functional_moss_P1.conf`
- **Collections:** `py-n1ql-collections_ddl.conf`, `py-n1ql-collections_end2end.conf`
- **Performance:** `py-tuq-monitoring.conf`, `py-tuq-profiling.conf`, `py-tuq-awr.conf`
- **AliQG:** `py-tuq-aiqg.conf` -- AI-generated query testing (CBO, UDF, prepared statement validation)

## Quick Reference for Common Operations

```bash
# Vector search tests (8 conf files for different scenarios)
python testrunner.py -c conf/tuq/py-tuq-vector-knn.conf       # KNN search

# Iceberg external collection tests (requires AWS credentials)
python testrunner.py -c conf/tuq/py-tuq-iceberg.conf
```

## Additional Documentation

- **Iceberg Tests**: See [README_ICEBERG.md](README_ICEBERG.md) for external Iceberg collection test details
- **Encryption at Rest**: See [README_Encryption_At_Rest.md](README_Encryption_At_Rest.md) for key-rotation / re-encryption background

---

## Encryption at Rest Tests

**File:** `n1ql_encryption_at_rest.py`  
**Class:** `QueryEncryptionAtRestTests(BaseSecondaryIndexingTests)`  
**Conf:** `conf/tuq/py-tuq-encryption-at-rest.conf`

### Inheritance chain
```
BaseTestCase → QueryTests (tuq.py) → BaseSecondaryIndexingTests (gsi/base_gsi.py)
    → QueryEncryptionAtRestTests
```
`BaseSecondaryIndexingTests` provides `get_nodes_from_services_map`, `gsi_util_obj`,
`prepare_collection_for_indexing`, `wait_until_indexes_online`, `encryption_helper`.

### Cluster topology
```
nodes_init=3
services_init=kv:n1ql:index-kv:n1ql:index-kv:n1ql:index
num_index_replica=1
skip_load=True        # prevents base class loading Employee/Person dataset
```

### Critical design: per-node DEK dict
`_get_query_in_use_key_ids()` returns `{node_ip: [key_id, ...]}` — **not a flat list**.
Each query node independently manages its own DEKs via `GET :8093/admin/encryption_at_rest`.
Always use `_build_node_key_ids_map()` to normalise before passing to helpers, then call
the encryption helper one node at a time:
```python
node_key_ids_map = self._build_node_key_ids_map(query_nodes, expected_key_ids)
for node in query_nodes:
    self.encryption_helper.verify_query_log_files_encrypted(
        [node], node_key_ids_map.get(node.ip, [])
    )
```

### File types
| File | Created when | Validated by |
|------|-------------|-------------|
| `rlstream.*` | `completed-stream-size > 0` and queries run | `_assert_rlstream_files_encrypted` (immediate) |
| `local_request_log.*` | rlstream idle timeout or 100 MiB | `_assert_local_request_log_files_encrypted` (polls 120 s) |
| `query_ffdc_MAN_*` | `trigger_query_ffdc()` API call | `_trigger_ffdc_and_verify` (polls 180 s) |

### Key helpers

| Method | Purpose |
|--------|---------|
| `_build_node_key_ids_map(nodes, key_ids)` | Normalises flat list or per-node dict → `{node_ip:[ids]}` |
| `_assert_rlstream_files_encrypted(nodes, key_ids, label)` | Validates rlstream.* immediately |
| `_assert_local_request_log_files_encrypted(nodes, key_ids, label)` | Polls 120 s then validates local_request_log.* |
| `_get_query_in_use_key_ids()` | Returns `{node_ip:[key_id,...]}` from every query node |
| `_wait_for_new_query_key_ids(baseline, timeout, label)` | Polls until any node has new key_id; returns full current dict |
| `_set_query_completed_settings(stream_size, threshold)` | Applies to all nodes, sleeps 10 s, verifies |
| `_run_select_scans(queries, nodes)` | Broadcasts every query to every node (not round-robin) |
| `_generate_concurrent_load_until_archive(queries, nodes)` | 20 × 5 batches; breaks when archives appear |
| `_setup_bucket_indexes_scans(prefix)` | Full setup: settings → bucket → data → indexes → wait → scans |
| `_install_tools()` | Installs xxd on all servers if missing (suite_setUp) |

### Query admin settings
| Setting | Test value | tearDown default |
|---------|-----------|-----------------|
| `completed-stream-size` | 500 | 0 |
| `completed-threshold` | 0 | 1000 |

Applied and restored per-node via:
- `self.rest.set_completed_stream_size(node, size)`
- `self.rest.set_completed_requests_collection_duration(node, threshold)`

### Encryption setup (base class, not in this file)
`BaseTestCase.suite_setUp()` sets log encryption when `enable_log_encryption_at_rest=True`.
`self.log_encryption_at_rest_id` = the KEK ID.  Validated by `_verify_log_encryption_prerequisites()`.

### Key rotation test pattern
Pin `dekRotationInterval` high (60 000 s) before setup → baseline → reduce to 120 s →
poll `_wait_for_new_query_key_ids` → **immediately** re-pin to 60 000 s → re-run scans → validate.
Used by both `test_query_log_encryption_key_rotation` and `test_query_ffdc_encryption_key_rotation`.
