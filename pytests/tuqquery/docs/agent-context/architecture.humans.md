# Architecture Human-Facing Document - N1QL Query Test Suite

**Location:** `pytests/tuqquery/`
**Purpose:** Human-readable architecture overview for Couchbase Query Service integration testing
**Target Audience:** Test engineers, QA engineers, developers, system architects

## Narrative Overview

The N1QL Query Test Suite (`pytests/tuqquery/`) is a comprehensive integration testing framework designed to validate the Couchbase Query Service (N1QL/SQL++) functionality. The suite consists of 125+ test files organized into feature-specific modules, executing against running Couchbase clusters to ensure Query Service reliability across diverse use cases.

### Architecture Philosophy

The architecture follows a **layered, inheritance-based testing framework** pattern:

1. **Framework Layer:** Base infrastructure providing cluster management, bucket operations, and test lifecycle hooks
2. **Query Layer:** Specialized layer for query execution, result verification, and index management
3. **Feature Layer:** Domain-specific test classes covering particular Query Service features (vector search, window functions, UDF, etc.)
4. **Configuration Layer:** INI-style configuration system that drives test behavior without code changes

This design enables **test behavior parameterization** through configuration files rather than code modification, making the suite flexible for different cluster topologies, data volumes, and test priorities.

### Test Execution Model

Tests execute in an **integration testing environment** where:

1. A Couchbase cluster is provisioned (either physical, cluster_run sandbox, or cloud deployment)
2. Query Service is enabled along with required auxiliary services (GSI for indexing, KV for documents, FTS for full-text search)
3. Test configuration is parsed from `conf/tuq/*.conf` files defining parameters like dataset size, index behavior, priority
4. Test setup creates buckets, collections, indexes, and loads test data
5. Query execution runs N1QL queries via Couchbase Python SDK port 8093
6. Results are verified using DeepDiff for JSON comparison
7. Cleanup removes indexes and buckets to restore test environment

## Major Components and Responsibilities

### 1. Test Framework Core

**Component:** `BaseTestCase` (from `pytests/basetestcase.py`)
**Responsibility:** Framework-level test infrastructure
**Key Capabilities:**
- Cluster initialization and cleanup
- Bucket creation and management
- SSH connections for remote operations
- Encryption at rest configuration
- Test lifecycle management (setup/teardown)
- Logging configuration

**Tradeoffs:** High complexity but provides comprehensive cluster management; assumes testrunner is run from project root

### 2. Query Test Infrastructure

**Component:** `QueryTests` (from `tuq.py`)
**Responsibility:** Main query test base class providing Query Service testing capabilities
**Key Capabilities:**
- Query execution via `N1QLHelper.run_cbq_query()`
- Index management (create/drop/wait for online)
- Result verification using DeepDiff
- Data loading via `DocumentGenerator` or sample datasets
- Utility methods for common test patterns

**Design Decision:** Inherits from BaseTestCase to leverage cluster management while adding Query Service-specific operations

### 3. Sanity Test Layer

**Component:** `QuerySanityTests` (from `tuq_sanity.py`)
**Responsibility:** Core sanity testing for essential query operations
**Key Capabilities:**
- Basic SELECT operations
- META() function testing
- Aggregate functions (SUM, AVG, COUNT, MIN, MAX)
- Sample dataset loading (travel-sample, beer-sample)
- Result counting and verification

**Priority:** P0 (critical) - these are the most important tests for Query Service basic functionality

### 4. Feature-Specific Test Modules

Each feature area has dedicated test classes inheriting from `QueryTests`:

#### Vector Search (`tuq_vectorsearch.py`)
- **Responsibility:** Vector similarity search testing (KNN/ANN)
- **Key Features:** Distance metrics (L2, COSINE, DOT), ANN index types (IVF, PQ, SQ8), pushdown optimization
- **Configuration:** 8 specialized conf files for different scenarios
- **Maturity:** High - comprehensive coverage including edge cases
- **Tradeoff:** Single-file approach consolidates tests but requires large configuration file count

#### Window Functions (`n1ql_window_functions.py`)
- **Responsibility:** Window function testing (RANK, DENSE_RANK, ROW_NUMBER, NTILE, etc.)
- **Key Features:** PARTITION BY, window frames (ROWS, RANGE, GROUPS)
- **Coverage:** 70% - moderate, needs expansion for complex edge cases

#### User Defined Functions (`tuq_UDF_N1QL.py`, `tuq_UDF.py`)
- **Responsibility:** UDF creation, execution, and management testing
- **Key Features:** Inline SQL++ UDFs, JavaScript UDFs, error handling
- **Coverage:** 65% - medium, needs performance and resource limit testing

#### Collections (`n1ql_collections_ddl.py`, `n1ql_collections_end2end.py`)
- **Responsibility:** Collections and scopes DDL operations testing
- **Key Features:** Scope/collection creation/deletion, cross-collection queries
- **Coverage:** 85% - high, modern Couchbase architecture support
- **Tradeoff:** Tests depend on system catalog consistency

#### Security/RBAC (`n1ql_rbac_2.py`, `tuq_n1ql_audit.py`)
- **Responsibility:** Role-Based Access Control and audit logging testing
- **Key Features:** Permission testing, RBAC edge cases, audit log verification
- **Coverage:** 75% - medium, needs injection attack testing
- **Constraint:** Tests require complex cluster configurations

#### Join Operations (`tuq_join.py`, `tuq_ansi_joins.py`)
- **Responsibility:** JOIN operation testing
- **Key Features:** Implicit/explicit joins, ANSI syntax, optimization
- **Coverage:** 75% - medium, needs multi-table join testing

### 5. Helper Libraries

#### N1QL Helper (`lib/couchbase_helper/tuq_helper.py`)
**Component:** `N1QLHelper` class
**Responsibility:** Primary query execution and Query Service interaction
**Key Methods:**
- `run_cbq_query()` - Execute N1QL queries via Couchbase SDK
- `create_index()` / `drop_index()` - Index management
- `wait_for_all_indexes_online()` - Index build synchronization
- `create_collection()` / `delete_collection()` - Collection DDL via N1QL

**Design Decision:** Helper class reused across all test classes to avoid code duplication

#### Query Generator (`lib/couchbase_helper/tuq_generators.py`)
**Component:** `TuqGenerators` class
**Responsibility**: Query generation and expected result calculation
**Key Methods:**
- `generate_query()` - Generate queries from templates
- `generate_expected_result()` - Calculate expected results client-side

**Tradeoff:** Client-side expected result calculation is flexible but complex; may not match server behavior for edge cases

#### Vector Utilities (`lib/vector/vector.py`)
**Component:** `UtilVector` class
**Responsibility:** Vector distance calculations and verification
**Key Methods:**
- `check_distance()` - Verify vector search results
- `l2_dist()`, `dot_product_dist()`, `cosine_dist()` - Distance metrics

**Capability:** Supports both sparse and dense vector operations

#### Collections N1QL Client (`lib/collection/collections_n1ql_client.py`)
**Component:** `CollectionsN1QL` class
**Responsibility:** Collection operations via N1QL
**Key Methods:**
- `create_collection()` / `delete_collection()` - Collection management
- `create_scope()` / `delete_scope()` - Scope management

**Design Decision:** Uses N1QL queries for collection operations to validate Query Service integration

### 6. Configuration System

**Component:** INI-style configuration files in `conf/tuq/`
**Responsibility:** Drive test behavior without code modification
**Total Files:** 185+ configuration files

**Configuration Format:**
```ini
tuqquery.tuq_sanity.QuerySanityTests:
    test_meta_basic,skip_index=True,doc-per-day=1,covering_index=True,GROUP=P0
    test_meta,doc-per-day=1,covering_index=True,GROUP=P0
```

**Key Parameters:**
- `skip_index` - Skip index creation for performance testing
- `doc-per-day` - Documents generated per day (controls dataset size)
- `covering_index` - Use covering index optimization
- `GROUP` - Priority level (P0, P1, P2, P3) and feature tags
- `distance`, `nprobes`, `description` - Vector search parameters
- `reload_data` - Reload test data before execution

**Tradeoff:** Configuration flexibility vs. configuration complexity; 185+ config files to manage

**Categories of Configuration:**
- Sanity tests (meta.conf, py-tuq-ext-sanity.conf, etc.)
- Indexing (py-tuq-gsi.conf, py-covering-index.conf)
- Joins/DML (py-tuq-joins.conf, py-tuq-dml.conf)
- Vector Search (8 conf files for different scenarios)
- UDF (py-tuq-udf-n1ql.conf, py-tuq-udf-analytics.conf)
- Security (py-n1ql-rbac.conf, py-tuq-n1ql-audit.conf)
- Collections (py-n1ql-collections_ddl.conf)
- Window Functions (py-tuq-window_functions.conf)
- FTS Integration (n1ql_fts_integration_phase1_P1.conf)

## How Test Workflows Map to Architecture

### Test Development Workflow

```
1. Developer identifies new Query Service feature to test
   ↓
2. Developer creates test file: pytests/tuqquery/tuq_new_feature.py
   ↓
3. Developer creates test class inheriting from QueryTests
   ↓
4. Developer writes test methods using pattern:
   def test_new_feature(self):
       if not self.skip_index:
           self.create_index()
           self.wait_for_all_indexes_online()
       result = self.run_cbq_query(query="SELECT ...")
       self.assertEqual(result['results'], expected)
   ↓
5. Developer creates configuration: conf/tuq/py-tuq-new-feature.conf
   ↓
6. Developer assigns priority: GROUP=P0 or GROUP=P1
   ↓
7. Test executed via: python testrunner.py -c conf/tuq/py-tuq-new-feature.conf
```

### Test Execution Workflow

```
Configuration Parsing
   ↓
BaseTestCase.setUp() (cluster setup, buckets)
   ↓
QueryTests.setUp() (Query Service initialization)
   ↓
Feature-specific setUp() (feature-specific prep)
   ↓
Index Creation → wait_for_all_indexes_online()
   ↓
Data Loading (DocumentGenerator or sample datasets)
   ↓
Query Execution: run_cbq_query() → N1QLHelper → Couchbase SDK → Query Service
   ↓
Result Verification: DeepDiff(actual, expected)
   ↓
QueryTests.tearDown() (cleanup indexes)
   ↓
BaseTestCase.tearDown() (cleanup buckets, cluster)
```

### Vector Search Workflow (Complex Example)

```
VectorSearchTests.test_ann_search()
   ↓
Vector Data Loading (SIFT or sparse dataset)
   ↓
UtilVector.check_distance() (distance metric calculation) - CLIENT-SIDE
   ↓
Index Creation with vector configuration
   ↓
wait_for_all_indexes_online() - GSI Service coordination
   ↓
Query Execution:
   SELECT id, META().id, score FROM bucket
   USE INDEX idx USING GSI
   WHERE VECTOR_SEARCH(field, query_vector, params)
   LIMIT 10
   ↓
Query Service → GSI Service (ANN index lookup)
   ↓
Result Collection with distances
   ↓
UtilVector.check_distance() (verify client-side calculation matches server)
   ↓
Pass/Fail based on distance accuracy
```

### FTS Integration Workflow (Cross-Service Example)

```
N1qlFTSIntegrationTest.test_fts_search()
   ↓
FTS Index Creation: FTSIndex.create_index() (via pytests/fts/fts_base.py)
   ↓
Query Execution: SELECT ... WHERE SEARCH(index, query)
   ↓
Query Service delegates to FTS Service
   ↓
FTS Service executes full-text search
   ↓
Results returned to Query Service
   ↓
Query Service combines results with other query clauses
   ↓
Result verification
```

## Tradeoffs, Constraints, and Known Weak Spots

### Architecture Tradeoffs

#### 1. Single-File vs. Multi-File Test Organization
**Tradeoff:** Vector search uses single-file approach (tuq_vectorsearch.py) with multiple configuration files (8 conf files)
**Benefit:** Easier test maintenance, centralized vector search logic
**Cost:** Confusing for developers unfamiliar with configuration-driven testing
**Alternative:** Split into multiple test files (tuq_vector_knn.py, tuq_vector_ann.py)
**Decision:** Single-file chosen for vector search due to high interdependency of tests

#### 2. Configuration-Driven vs. Parameterized Code
**Tradeoff:** Configuration file system (185+ files) vs. code parameters
**Benefit:** Test behavior changes without code modification, flexible for different clusters
**Cost:** Configuration complexity, difficult to understand test behavior without reading conf files
**Alternative:** Use Python test parameters via pytest fixtures
**Decision:** Configuration system chosen for integration test flexibility

#### 3. Client-Side vs. Server-Side Expected Results
**Tradeoff:** TuqGenerators calculates expected results client-side vs. querying database for expected values
**Benefit:** No need for "golden dataset" maintenance, flexible expected result generation
**Cost:** Calculation may not match server behavior for complex queries or edge cases
**Alternative:** Query master cluster or trusted dataset for expected results
**Decision:** Client-side calculation for flexibility, but manual verification for complex cases

### Constraints

#### 1. Working Directory Constraint
**Constraint:** Tests must be run from project root (`testrunner`)
**Reason:** Python `sys.path` must include `lib/` and `pytests/` for imports to work
**Impact:** Cannot run tests from arbitrary directories
**Mitigation:** Documentation emphasizes working directory requirement

#### 2. Cluster Requirement Constraint
**Constraint:** All tests require running Couchbase cluster with Query Service enabled
**Reason:** Integration testing, not unit testing
**Impact:** No offline test execution possible, requires infrastructure
**Mitigation:** cluster_run sandbox for local development

#### 3. External Dataset Dependency
**Constraint:** Vector search tests require SIFT/sparse datasets, FTS tests require sample buckets
**Reason:** Tests validate specific vector operations and search functionality
**Impact:** Tests fail if datasets not available
**Mitigation:** Document dataset loading process, provide fallback tests

#### 4. Python Version Constraint
**Constraint:** Python 3.10.13 required
**Reason:** Dependencies (couchbase SDK, deepdiff) have version compatibility
**Impact:** Cannot use newer Python features
**Mitigation:** Version specification in requirements.txt

### Known Weak Spots

#### 1. Performance and Optimization Testing (Weakness: 60%Coverage)
**Gap:** Limited query plan verification, no performance regression testing, missing optimizer hint testing
**Impact:** Performance regressions can go undetected
**Priority:** P1 (High) - Performance critical for production
**Recommendation:** Create `tuq_performance_regression_tests.py` with baseline performance metrics

#### 2. Security Testing Depth (Weakness: Insufficient Comprehensive Coverage)
**Gap:** Limited privilege escalation testing, missing injection attack vectors, no comprehensive data privacy tests
**Impact:** Security vulnerabilities may not be caught
**Priority:** P0 (Critical) - Security issues are critical
**Recommendation:** Create `tuq_security_injection_tests.py`, `tuq_security_privilege_tests.py`

#### 3. Window Functions Testing (Weakness: 70%Coverage)
**Gap:** Missing edge case testing for complex window frames, no window function performance testing
**Impact:** Window function edge cases may cause production issues
**Priority:** P1 (High) - Window functions critical for analytical queries
**Recommendation:** Add complex window frame edge case tests

#### 4. Error Handling and Edge Cases (Weakness: Insufficient Coverage)
**Gap:** Limited boundary condition testing, no systematic out-of-bound testing, missing error message validation
**Impact:** Poor error handling affects user experience
**Priority:** P1 (High) - Error handling critical for user experience
**Recommendation:** Create systematic boundary condition and error validation tests

#### 5. Join Operations Coverage (Weakness: 75%Coverage)
**Gap:** Missing complex multi-table join tests, no join optimization verification, limited join error handling
**Impact:** Complex joins may fail in production
**Priority:** P2 (Medium) - Joins fundamental but edge cases less common
**Recommendation:** Add complex multi-table join tests

#### 6. Subquery and CTE Testing (Weakness: Medium Coverage)
**Gap:** Missing recursive CTE tests, limited correlated subquery performance tests, no subquery optimization verification
**Impact:** Complex query patterns may fail
**Priority:** P2 (Medium) - Advanced query patterns used but less frequently
**Recommendation:** Add recursive CTE tests

#### 7. Test Maintainability (Weakness: Documentation Gaps)
**Gap:** Inconsistent test method documentation, limited setUp/tearDown documentation, no parameter documentation
**Impact:** Difficult for new developers to understand test behavior
**Priority:** P2 (Medium) - Affects onboarding but not execution
**Recommendation:** Add comprehensive test method docstrings

#### 8. Serverless Coverage (Weakness: 50%Coverage)
**Gap:** Limited serverless configuration testing, no serverless scale testing, missing serverless-specific error handling
**Impact:** Serverless cloud deployments may have undetected issues
**Priority:** P2 (Medium) - Serverless growing but smaller than on-prem
**Recommendation:** Expand serverless test coverage

### Architectural Debt

#### 1. No Linting or Type Checking
**Debt:** No enforced code quality standards (flake8, pylint, mypy, ruff, black)
**Impact:** Risk of inconsistent code style, potential bugs from type errors
**Mitigation:** Manual code review, developer discipline
**Recommendation:** Introduce linting and type checking tools

#### 2. No CI/CD Pipeline
**Debt:** No automated test execution, no test result aggregation
**Impact:** Tests require manual triggering, no continuous integration
**Mitigation:** External test triggering (mechanism unknown)
**Recommendation:** Implement CI/CD pipeline

#### 3. No Dev Container Configuration
**Debt:** No reproducible development environment setup
**Impact:** Different developers may have inconsistent environments
**Mitigation:** Manual environment setup documentation
**Recommendation:** Create devcontainer and environment setup guide

#### 4. Configuration File Proliferation
**Debt:** 185+ configuration files, difficult to maintain
**Impact:** Configuration complexity, potential for inconsistencies
**Mitigation:** Configuration templates, documentation
**Recommendation:** Consolidate similar configurations, use configuration inheritance

### Scalability Concerns

#### 1. Large Dataset Execution
**Concern:** Vector search tests with large SIFT datasets (millions of vectors) may timeout
**Mitigation:** Dataset size limits, extended timeouts for vector tests
**Status:** Handled via configuration parameters

#### 2. Concurrent Test Execution
**Concern:** Multiple tests modifying same bucket cause conflicts
**Mitigation:** Unique bucket names per test class or test isolation
**Status:** Not systematically implemented, relies on developer discipline

#### 3. Index Build Time
**Concern:** Large indexes require long build times, tests may timeout waiting for indexes online
**Mitigation:** `wait_for_all_indexes_online()` with extended timeouts
**Status:** Implemented, but timeout values not documented

### Cross-Service Integration Complexity

#### 1. FTS Integration
**Complexity:** Requires FTS Service coordination, FTS index creation, QUERY Service delegation
**Mitigation:** FTSBase class from pytests/fts for consistency
**Status:** Well-handled via FTSBase abstraction

#### 2. Analytics Integration
**Complexity:** Requires CBAS Service coordination, data synchronization, Query-CBAS joins
**Mitigation:** Dedicated test files (tuq_aus.py)
**Status:** Limited but functional

#### 3. Eventing Integration
**Complexity:** Limited cross-service tests
**Mitigation:** Minimal integration tests
**Status:** Gap - missing comprehensive Eventing integration tests

### Dependency Management Challenges

#### 1. Couchbase SDK Version Compatibility
**Challenge:** Tight coupling to couchbase==4.4.0, may not work with newer SDK versions
**Mitigation:** Version pinning in requirements.txt
**Status:** Handled via requirements.txt

#### 2. Third-Party Library Compatibility
**Challenge:** FAISS, numpy, deepdiff version compatibility with Python 3.10.13
**Mitigation:** Version pinning in requirements.txt
**Status:** Handled via requirements.txt

#### 3. AI Query Generator (AiQG) Dependencies
**Challenge:** Requires OpenAI API key, internet connectivity, LangChain
**Mitigation:** Environment variable configuration
**Status:** Not documented for developers

## Future Architectural Improvements

### High Priority (P0 - Critical)

1. **Security Testing Strengthening**
   - Implement comprehensive RBAC edge case testing
   - Add injection attack vector tests
   - Create data privacy and encryption tests
   - Implement cross-tenant isolation tests

2. **Performance and Optimization Testing**
   - Implement systematic query plan verification
   - Create query performance regression suite
   - Add optimizer hint testing
   - Verify index usage across all query types

### Medium Priority (P1 - High)

3. **Enhanced Window Functions Testing**
   - Add complex window frame edge case tests
   - Implement window function performance tests
   - Test window function integration with other features

4. **Improved UDF Testing**
   - Add UDF performance testing
   - Implement UDF error handling edge cases
   - Test UDF with large datasets
   - Add UDF timeout and resource limit tests

5. **Error Handling Improvement**
   - Add boundary condition tests
   - Implement error message validation
   - Create systematic out-of-bound testing

### Low Priority (P2 - Medium)

6. **Linting and Type Checking**
   - Introduce flake8, pylint, or ruff
   - Add mypy type checking
   - Implement code formatting with black
   - Configure pre-commit hooks

7. **CI/CD Pipeline**
   - Implement automated test execution
   - Add test result aggregation
   - Create test failure escalation

8. **Documentation Improvement**
   - Add comprehensive test method docstrings
   - Document test dependencies
   - Improve setUp/tearDown documentation

## Conclusion

The N1QL Query Test Suite provides a robust, layered architecture for comprehensive Query Service integration testing. The inheritance-based design, configuration-driven testing, and helper libraries enable flexible test development and execution. However, critical gaps exist in security testing, performance validation, and code quality enforcement. Addressing these gaps through architectural improvements and expanded test coverage will strengthen the suite's ability to catch production issues before deployment.

The architecture successfully balances flexibility (configuration system) with consistency (base classes and helpers), enabling ~2,500+ test methods across 125+ test files to validate Query Service functionality effectively. Future improvements should focus on closing coverage gaps, strengthening security and performance testing, and introducing automated quality enforcement tools.

**Next Steps:**
1. Review and prioritize P0 improvements (security, performance)
2. Implement linting and type checking (P2 but low effort, high impact)
3. Expand test coverage for weak spots (window functions, error handling)
4. Create CI/CD pipeline for automated test execution
5. Improve documentation for developer onboarding
