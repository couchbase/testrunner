# Architecture Agent Document - N1QL Query Test Suite

**Location:** `pytests/tuqquery/`
**Purpose:** Integration testing for Couchbase Query Service (N1QL/SQL++)
**Base Class:** `QueryTests` → `BaseTestCase` → `unittest.TestCase`

## Module Boundaries and Ownership

### Core Test Layer (mos/tuqquery/)
```
tuq.py (QueryTests)              # Main base class for all query tests
tuq_sanity.py (QuerySanityTests) # Core sanity tests
tuq_vectorsearch.py              # Vector search comprehensive suite
n1ql_window_functions.py        # Window functions
n1ql_collections_ddl.py         # Collections DDL operations
n1ql_fts_integration.py         # FTS service integration
tuq_UDF_N1QL.py                 # User Defined Functions
tuq_join.py                     # JOIN operations
tuq_dml.py                      # Data Manipulation Language
tuq_ddl.py                      # Data Definition Language
n1ql_rbac_2.py                  # Security/RBAC tests
... (125+ total test files)
```

### Helper Layer (lib/couchbase_helper/)
```
tuq_helper.py (N1QLHelper)      # Main query execution helper
tuq_generators.py (TuqGenerators) # Query generation and expected results
documentgenerator.py             # JSON document generation
data_analysis_helper.py         # Result verification
```

### Collection Layer (lib/collection/)
```
collections_n1ql_client.py (CollectionsN1QL) # Collections via N1QL
collections_cli_client.py                    # Collections via CLI
collections_stats.py                         # Collection statistics
```

### Vector Layer (lib/vector/)
```
vector.py (UtilVector)  # Vector distance calculations, sparse/dense operations
```

### Service Client Layer (lib/membase/api/)
```
rest_client.py (RestConnection)  # REST API for cluster/bucket ops
exception.py (CBQError)          # Query exception handling
```

### Framework Layer (pytests/)
```
basetestcase.py (BaseTestCase)  # Framework-level test infrastructure
```

## Runtime Flows

### Test Execution Flow
```
Configuration (conf/tuq/*.conf)
    ↓
QueryTests.setUp()
    ↓
Cluster/Bucket setup (via RestConnection)
    ↓
Index creation (via run_cbq_query → N1QLHelper)
    ↓
Data loading (via DocumentGenerator)
    ↓
wait_for_all_indexes_online()
    ↓
Query execution:
    self.run_cbq_query(query="SELECT ...")
        ↓
    N1QLHelper.run_cbq_query()
        ↓
    Couchbone SDK → Query Service (port 8093)
        ↓
    Result collection
    ↓
Result verification (DeepDiff for JSON comparison)
    ↓
QueryTests.tearDown()
    ↓
Cleanup (drop indexes, clear buckets)
```

### Vector Search Flow
```
VectorSearchTests.test_ann_search()
    ↓
vector.py: UtilVector.check_distance()
    ↓
Sparse/dense vector operations
    ↓
N1QL query: SELECT id, META().id, score FROM bucket USE INDEX ... ORDER BY VECTOR_SEARCH(...)
    ↓
Query Service via GSI with vector index
    ↓
Result verification with distance calculations
```

### FTS Integration Flow
```
N1qlFTSIntegrationTest.test_fts_search()
    ↓
tuq.py: QueryTests → FTSIndex (from pytests/fts/fts_base.py)
    ↓
Create FTS index via FTS Integration
    ↓
Query: SELECT ... FROM bucket WHERE SEARCH(...)
    ↓
Query Service delegates to FTS Service
    ↓
Result verification
```

### Collections Flow
```
QueryCollectionsDDLTests.test_create_collection()
    ↓
collections_n1ql_client.py: CollectionsN1QL.create_collection()
    ↓
N1QLHelper.create_collection()
    ↓
N1QL query: CREATE COLLECTION bucket.scope.collection
    ↓
wait_till_collection_created()
    ↓
Verify via system:scopes and system:collections
```

## Data Contracts and External Systems

### Couchbase Query Service (N1QL)
- **Protocol:** HTTP REST (port 8093) + Couchbase SDK
- **Query execution:** `N1QLHelper.run_cbq_query()` → SDK
- **Index management:** Via Query Service CREATE/DROP INDEX
- **System catalogs:** system:scopes, system:collections, system:indexes, system:keyspaces
- **Error handling:** `CBQError` exceptions

### Couchbase KV Service
- **Protocol:** memcached binary protocol
- **Bucket operations:** Via `RestConnection`
- **Document loading:** Via `DocumentGenerator` → KV
- **Needed for:** Test data, document operations

### Couchbase GSI Service
- **Role:** Global Secondary Index (GSI) management
- **Index creation:** Via N1QL CREATE INDEX
- **Index operations:** FLATTEN_KEYS for array indexing
- **Index monitoring:** system:indexes catalog

### Full-Text Search (FTS) Service
- **Integration:** Via N1QL SEARCH() function
- **Index management:** `FTSIndex` from `pytests/fts/fts_base.py`
- **Features:** Flex index, FTS integration, Phase 1/2 features

### Analytics Service (CBAS)
- **Integration:** Via tuq_aus.py (Analytics integration tests)
- **Data synchronization:** Query-CBAS cross-service queries

### Eventing Service
- **Integration:** Limited (infrequent cross-service tests in tuqquery)

### Security Services
- **RBAC:** Via `RbacBase` from `pytests/security/rbac_base.py`
- **Audit:** Query audit logging
- **Encryption:** Encryption at rest (KMIP)

## Validation Clues by Layer

### Test → Service Mapping
| Test Category | Validates | Configuration File |
|--------------|-----------|-------------------|
| Sanity (tuq_sanity.py) | Core query operations | `conf/tuq/meta.conf` |
| Indexing (tuq_gsi_index.py) | GSI operations | `conf/tuq/py-tuq-gsi.conf` |
| Joins (tuq_join.py) | JOIN semantics | `conf/tuq/py-tuq-joins.conf` |
| Vector Search (tuq_vectorsearch.py) | Vector similarity search | `conf/tuq/py-tuq-vector-*.conf` (8 files) |
| Window Functions (n1ql_window_functions.py) | Window function execution | `conf/tuq/py-tuq-window_functions.conf` |
| UDF (tuq_UDF_N1QL.py) | User defined functions | `conf/tuq/py-tuq-udf-n1ql.conf` |
| Collections (n1ql_collections_ddl.py) | Collections/scopes DDL | `conf/tuq/py-n1ql-collections_ddl.conf` |
| Security (n1ql_rbac_2.py) | RBAC/permissions | `conf/tuq/py-n1ql-rbac.conf` |
| FTS Integration (n1ql_fts_integration.py) | FTS-Query service | `conf/tuq/n1ql_fts_integration_phase1_P1.conf` |

### Helper → Service Mapping
| Helper | Service | Validation Point |
|--------|---------|----------------|
| `N1QLHelper.run_cbq_query()` | Query Service | Query execution, result collection |
| `N1QLHelper.create_index()` | GSI Service | Index creation |
| `CollectionsN1QL.create_collection()` | Query Service | Collection DDL |
| `UtilVector.check_distance()` | None (calculation verification) | Vector search accuracy |
| `TuqGenerators.generate_expected_result()` | None (client-side) | Expected result calculation |

### Configuration Parameter → Test Behavior
| Parameter | Affects | Example Values |
|-----------|---------|---------------|
| `skip_index=True` | Skips index creation | `test_meta_basic` |
| `doc-per-day=N` | Documents generated | `1`, `10`, `100` |
| `covering_index=True` | Uses covering index optimization | `test_meta` |
| `GROUP=P0/P1/P2/P3` | Priority level | `P0` (critical), `P1` (high) |
| `distance=L2/DOT/COSINE` | Vector search distance metric | Vector search tests |
| `nprobes=N` | ANN search nprobes parameter | `5`, `10`, `20` |
| `compare_cbo=True` | AiQG CBO comparison | AiQG tests |
| `reload_data=True` | Reload test data before execution | Data-dependent tests |

## Common Traps for Agents

### 1. Working Directory Assumptions
**Trap:** Tests assume `sys.path` includes project root (`lib/`, `pytests/`)
**Solution:** Always change to `testrunner` before running tests
**Trap:** Importing from `tuqquery` instead of using relative imports
**Solution:** In tuqquery, use `.tuq` for relative imports to QueryTests

### 2. Index Build Timing
**Trap:** Query fails because index not fully online yet
**Solution:** Always call `self.wait_for_all_indexes_online()` after index creation
**Trap:** Vector indexes require special timeout handling
**Solution:** Vector search tests have longer waits for large datasets

### 3. Configuration Parameter Access
**Trap:** Using literal values instead of `self.input.param()`
**Solution:** Always retrieve via `self.input.param("key", default_value)`
**Trap:** Assuming parameter exists without default
**Solution:** Provide sensible defaults: `self.input.param("skip_index", False)`

### 4. Collection System Catalogs
**Trap:** Querying system:collections before collections populated
**Solution:** Wait for `wait_till_collection_created()` after CREATE COLLECTION
**Trap:** Assuming collection exists in bucket
**Solution:** Check via `check_if_scope_exists()` or query system:scopes

### 5. Vector Data Dependencies
**Trap:** Vector tests fail without SIFT/sparse datasets
**Solution:** Vector datasets (SIFT, sparse) must be pre-loaded before test execution
**Trap:** Sparse vs dense vector confusion
**Solution:** Check `vector_type='sparse'` vs `'dense'` in UtilVector methods

### 6. FTS Integration Dependencies
**Trap:** FTS tests require existing sample buckets (travel-sample, beer-sample)
**Solution:** Load via `self.rest.load_sample("travel-sample")` in setUp
**Trap:** FTS index creation fails without proper bucket configuration
**Solution:** Ensure bucket has primary key before creating FTS index

### 7. Security Test Configuration
**Trap:** RBAC tests fail without proper user/collection setup
**Solution:** RBAC tests require admin users and scope-level permissions
**Trap:** Audit tests fail without audit logging enabled
**Solution:** Enable audit logs via REST before audit tests

### 8. DeepDiff JSON Comparison
**Trap:** False failures from floating-point precision or order differences
**Solution:** Use `DeepDiff` with appropriate ignore types: `DeepDiff(actual, expected, ignore_order=True, significant_digits=6)`
**Trap:** Comparison fails on Couchbase internal metadata fields
**Solution:** Strip META() fields or ignore in comparison

### 9. Test Priority and Grouping
**Trap:** Adding tests without GROUP parameter
**Solution:** Always assign GROUP for priority: `GROUP=P0` or `GROUP=feature`
**Trap:** Incorrect GROUP format (semicolon-separated)
**Solution:** Use `GROUP=P0;LIKE` for multiple tags

### 10. AiQG Configuration
**Trap:** AiQG tests require OpenAI API key
**Solution:** Set `OPENAI_API_KEY` environment variable before running AiQG
**Trap:** AiQG query generation fails without proper prompt files
**Solution:** AiQG prompts must be valid in `scripts/AiQG/` directory

### 11. Concurrent Test Execution
**Trap:** Multiple tests modifying same bucket cause conflicts
**Solution:** Use unique bucket names per test class or test isolation
**Trap:** Index creation conflicts in concurrent tests
**Solution:** Use unique index names: `f"idx_{self._testMethodName}"`

### 12. Cleanup and Teardown
**Trap:** Tests leave orphaned indexes/buckets
**Solution:** Always cleanup in `tearDown()` even if test fails
**Trap:** Cleanup fails if cluster is in unhealthy state
**Solution:** Wrap cleanup in try-except to avoid masking test failures

### 13. External Service Dependencies
**Trap:** Tests assume Query Service is enabled
**Solution:** Ensure cluster config includes "query" service
**Trap:** Tests assume specific build version features
**Solution:** Check `self.version` for version-specific features

### 14. Join and Subquery Performance
**Trap:** Large join operations timeout
**Solution:** Use LIMIT or appropriate dataset sizes in join tests
**Trap:** Correlated subqueries cause infinite loops
**Solution:** Bind subquery execution limits via query timeout

### 15. Window Function Edge Cases
**Trap:** Window functions with empty result sets fail
**Solution:** Validate window function behavior with empty datasets
**Trap:** ORDER BY in window clause vs outer query confusion
**Solution:** Clarify window frame vs query-level ordering

## Dependencies by Type

### Python Standard Library
- unittest - Test framework
- json, re, datetime, random, string, time - Utilities
- logging, traceback - Debugging
- copy - Result comparison

### Third-Party Libraries
- couchbase - Couchbase Python SDK
- deepdiff - JSON comparison
- boto3 - AWS S3 integration (for dataset download)
- faiss - Vector similarity search (ANN)
- numpy - Vector operations

### Framework Dependencies (testrunner/lib/)
- couchbase_helper/**/* - Query and document helpers
- lib/membase/api/**/* - REST API and exceptions
- lib/collection/**/* - Collections operations
- lib/remote/**/* - SSH operations
- lib/Cb_constants/**/* - Server constants

### Test Runner Dependencies
- testrunner.py - Main test runner
- scripts/start_cluster_and_run_tests.py - Cluster orchestration
- conf/tuq/*.conf - 185+ configuration files

### Couchbase Services (External)
- Query Service (N1QL) - Primary test target
- GSI Service - Index operations
- KV Service - Document operations
- FTS Service - Full-text search
- Analytics Service (CBAS) - Query-analytics integration
- Eventing Service - Limited integration

## Test Inheritance Tree

```
unittest.TestCase
└── BaseTestCase (pytests/basetestcase.py)
    ├── Cluster setup/teardown
    ├── Bucket management
    ├── SSH connections
    ├── Encryption at rest
    └── QueryTests (pytests/tuqquery/tuq.py)
        ├── N1QL query execution
        ├── Index management
        ├── Result verification
        ├── Data loading
        └── QuerySanityTests (tuq_sanity.py)
        │   ├── Basic query operations
        │   ├── META() function tests
        │   └── Sample dataset loading
        ├── VectorSearchTests (tuq_vectorsearch.py)
        │   └── Vector search (KNN/ANN)
        ├── WindowFunctionsTest (n1ql_window_functions.py)
        │   └── Window functions
        ├── QueryUDFN1QLTests (tuq_UDF_N1QL.py)
        │   └── User Defined Functions
        ├── QueryCollectionsDDLTests (n1ql_collections_ddl.py)
        │   └── Collections DDL
        ├── N1qlFTSIntegrationTest (n1ql_fts_integration.py)
        │   └── FTS integration
        ├── JoinTests (tuq_join.py)
        │   └── JOIN operations
        ├── QuerySubqueryTests (tuq_subquery.py)
        │   └── Subquery tests
        ├── DMLQueryTests (tuq_dml.py)
        │   └── DML operations
        ├── QueryDDLTests (tuq_ddl.py)
        │   └── DDL operations
        ├── RbacN1QL (n1ql_rbac_2.py)
        │   └── RBAC tests
        └── ... (120+ test classes)
```

## Key Configuration Files

| Configuration | Purpose | Key Tests |
|--------------|---------|-----------|
| `conf/tuq/meta.conf` | Main sanity tests | `QuerySanityTests` |
| `conf/tuq/py-tuq.conf` | Extended core query | Core operations |
| `conf/tuq/py-tuq-ext-sanity.conf` | Extended sanity | `QuerySanityTests` |
| `conf/tuq/py-tuq-joins.conf` | JOIN operations | `JoinTests` |
| `conf/tuq/py-tuq-ansi-joins.conf` | ANSI JOIN syntax | `QueryANSIJOINSTests` |
| `conf/tuq/py-tuq-gsi.conf` | GSI index tests | `QueriesIndexTests` |
| `conf/tuq/py-tuq-dml.conf` | DML operations | `DMLQueryTests` |
| `conf/tuq/py-tuq-udf-n1ql.conf` | UDF tests | `QueryUDFN1QLTests` |
| `conf/tuq/py-tuq-window_functions.conf` | Window functions | `WindowFunctionsTest` |
| `conf/tuq/py-n1ql-collections_ddl.conf` | Collections DDL | `QueryCollectionsDDLTests` |
| `conf/tuq/py-n1ql-rbac.conf` | RBAC tests | `RbacN1QL` |
| `conf/tuq/n1ql_fts_integration_phase1_P1.conf` | FTS integration | `N1qlFTSIntegrationTest` |
| `conf/tuq/py-tuq-vector-knn.conf` | KNN search | `VectorSearchTests` |
| `conf/tuq/py-tuq-vector-ann.conf` | ANN search | `VectorSearchTests` |
| `conf/tuq/py-tuq-vector-ann-pushdown.conf` | Vector pushdown | `VectorSearchTests` |
| `conf/tuq/py-tuq-aiqg.conf` | AI query generation | `QueryAiQGTests` |

## External Data Sources

### Sample Datasets
- **travel-sample**: Travel itinerary data (JSON documents)
- **beer-sample**: Beer brewery data (JSON documents)
- Loadable via: `self.rest.load_sample("dataset_name")`

### Vector Datasets
- **SIFT**: Dense vector dataset for KNN testing
- **Sparse**: Sparse vector dataset
- Location: Not documented in AGENTS.md (unknown)
- Required for: Vector search tests

### Generated Test Data
- **Random documents**: Via `DocumentGenerator`
- **JSON test data**: Via `TuqGenerators`
- **Custom datasets**: Via test-specific generators

## Validation Commands for Agents

### Validate Query Service Connectivity
```python
# Run basic query to verify Query Service is up
result = self.run_cbq_query(query="SELECT 1")
self.assertEqual(result['results'], [{'1': 1}])
```

### Validate Index Build
```python
# Check if index is online
index_list = self.get_index_list()
for index in index_list:
    self.assertEqual(index['state'], 'online')
```

### Validate Collection Creation
```python
# Query system catalog to verify collection
scope_query = f"SELECT COUNT(*) as cnt FROM system:scopes WHERE `bucket`='{bucket}' AND name='{scope}'"
res = self.run_cbq_query(query=scope_query)
self.assertTrue(res['results'][0]['cnt'] == 1)
```

### Validate Vector Search Accuracy
```python
# Verify distance calculations
util = UtilVector()
fail_count = util.check_distance(query_vector, xb, vector_results, vector_distances, distance="L2")
self.assertEqual(fail_count, 0)
```

## Common Entry Points

### Test Execution
```bash
cd testrunner
python testrunner.py -c conf/tuq/meta.conf               # Sanity tests
python testrunner.py -c conf/tuq/py-tuq-ext-sanity.conf  # Extended sanity
python testrunner.py -t tuqquery.tuq_sanity.QuerySanityTests  # Specific class
python testrunner.py -t tuqquery.tuq_vectorsearch.VectorSearchTests.test_ann_search -p distance=L2  # Specific test with params
```

### Debugging
```python
# Enable query logging
self.log.setLevel(logging.DEBUG)

# Print query results
result = self.run_cbq_query(query="SELECT * FROM bucket")
self.log.info(f"Query results: {result['results']}")

# Print explain plan
explain_result = self.run_cbq_query(query="EXPLAIN SELECT * FROM bucket WHERE field = 'value'")
self.log.info(f"Explain plan: {explain_result['results']}")
```

### Test Development
```python
# New test class template
from .tuq import QueryTests

class MyFeatureTests(QueryTests):
    def setUp(self):
        super(MyFeatureTests, self).setUp()
        self.index_name = self.input.param("index_name", "my_feature_idx")
        self.skip_index = self.input.param("skip_index", False)

    def test_my_new_feature(self):
        if not self.skip_index:
            self.run_cbq_query(query=f"CREATE INDEX {self.index_name} ON bucket(field)")
            self.wait_for_all_indexes_online()

        result = self.run_cbq_query(query="SELECT * FROM bucket WHERE field = 'value'")
        self.assertEqual(len(result['results']), 1)

    def tearDown(self):
        super(MyFeatureTests, self).tearDown()
        if not self.skip_index:
            self.run_cbq_query(query=f"DROP INDEX bucket.{self.index_name}")
```

## Unknowns and Assumptions

### Unknowns
- Vector dataset (SIFT, sparse) file locations and loading mechanisms
- AiQG OpenAI API key storage and security
- Test data generator configuration and customization
- CI/CD pipeline integration for tuqquery-specific tests
- External dataset download mechanisms (S3, FTP)

### Assumptions Made
- All tests require running Couchbase cluster with Query Service enabled
- Working directory must be `testrunner` for imports
- Python 3.10.13 is the minimum required version
- Configuration files in `conf/tuq/` follow INI-style format
- Test parameters are retrieved via `self.input.param()`
- Index build times vary based on dataset size and cluster resources

## Component Ownership

### Owned by tuqquery Team
- All test files in `pytests/tuqquery/`
- Configuration files in `conf/tuq/`
- Documentation in `pytests/tuqquery/README_*.md`

### Owned by Framework Team
- `pytests/basetestcase.py` - Base test infrastructure
- `testrunner.py` - Main test runner
- Framework-level helpers in `lib/`

### Owned by Services Teams
- Query Service behavior (N1QL, query execution)
- GSI Service behavior (indexing)
- FTS Service behavior (full-text search)
- KV Service behavior (document operations)

### Owned by External Projects
- `couchbase` Python SDK - Couchbase Inc.
- `deepdiff` - Third-party library
- `faiss` - Facebook AI Research
- `numpy` - NumPy project
