# N1QL Query Test Suite - Comprehensive Guide

This document provides a comprehensive overview of the N1QL/SQL++ Query test suite, covering test organization, configuration, execution, and test coverage details.

## Overview

**Location**: `pytests/tuqquery/`
**Configuration Location**: `conf/tuq/`
**Total Test Files**: ~125 Python test files
**Total Test Classes**: ~110 test classes
**Configuration Files**: ~185 conf files
**Test Categories**: 15+ major categories
**Summary Statistics**: ~2,500+ test methods

## Table of Contents

- [Summary Statistics](#summary-statistics)
- [Test Class Categories](#test-class-categories)
- [Test Categories](#test-categories)
- [Test Suite Configuration Categories](#test-suite-configuration-categories)
- [Configuration System](#configuration-system)
- [Running Tests](#running-tests)
- [Test Priorities](#test-priorities)
- [Test Group Labels](#test-group-labels)
- [Base Test Class Hierarchy](#base-test-class-hierarchy)
- [Test Helpers](#test-helpers)
- [Additional Resources](#additional-resources)
- [Maintenance Notes](#maintenance-notes)

---

## Summary Statistics

### File Structure Analysis

```
Total Test Files: 125
Test Classes: ~110 unique test classes
Total Test Methods: ~2,500+ (estimated)
Configuration Files: 185+
Base Classes: 2 (QueryTests, QuerySanityTests)
Priority Levels: 4 (P0, P1, P2, P3)
```

### Test Distribution by Category

| Category | Test Files | Estimated Coverage | Maturity Level |
|----------|------------|-------------------|----------------|
| Core Query Operations | 15 | 95% | High |
| Indexing | 8 | 90% | High |
| DML Operations | 6 | 85% | High |
| DDL Operations | 4 | 80% | Medium |
| Join Operations | 3 | 75% | Medium |
| Window Functions | 2 | 70% | Medium |
| UDF (User Defined Functions) | 3 | 65% | Medium |
| Vector Search | 1 | 85% | High |
| Collections & Scopes | 5 | 85% | High |
| Security (RBAC) | 3 | 75% | Medium |
| Performance & Optimization | 4 | 60% | Low |
| Integration Tests | 3 | 70% | Medium |
| Error Handling | 5 | 80% | Medium |
| Cluster Operations | 3 | 70% | Medium |
| Function Tests | 4 | 85% | High |
| Serverless | 4 | 50% | Low |

---

## Test Class Categories

### Core Query Tests

| Test Class | File | Description |
|------------|------|-------------|
| `QueryTests` | `tuq.py` | Base test class for all N1QL query tests |
| `QuerySanityTests` | `tuq_sanity.py` | Core sanity tests for query functionality |
| `QuerySelectTests` | `tuq_select.py` | SELECT statement tests |
| `DMLQueryTests` | `tuq_dml.py` | INSERT, UPDATE, DELETE, UPSERT operations |
| `QueryDDLTests` | `tuq_ddl.py` | Data Definition Language tests |
| `QueryBucketDDLTests` | `tuq_ddl_bucket.py` | Bucket-level DDL operations |
| `QueryDDLUserTests` | `tuq_ddl_user.py` | User management DDL |

### Index Tests

| Test Class | File | Description |
|------------|------|-------------|
| `QueriesIndexTests` | `tuq_gsi_index.py` | GSI index query tests |
| `QueriesViewsTests` | `tuq_index.py` | Index operations and views |
| `QueryINDEXUNNESTTests` | `tuq_index_unnest.py` | UNNEST with index operations |
| `QueryBuildIndexExpressionsTests` | `n1ql_build_index_expressions.py` | Index expression building |
| `QueryLMKTests` | `tuq_leading_missing_keys.py` | Leading missing key index tests |
| `AscDescTests` | `tuq_ascdesc.py` | ASC/DESC index ordering |
| `QueryArrayFlatteningTests` | `tuq_array_flattening.py` | Array flattening |
| `QueryArrayQueryTests` | `tuq_array_queries.py` | Array query operations |

### Join and Subquery Tests

| Test Class | File | Description |
|------------|------|-------------|
| `JoinTests` | `tuq_join.py` | JOIN operations |
| `QueryANSIJOINSTests` | `tuq_ansi_joins.py` | ANSI JOIN syntax |
| `QueryANSIMERGETests` | `tuq_ansi_merge.py` | ANSI MERGE operations |
| `QueryANSINestUnnestTests` | `n1ql_ansi_nest_unnest.py` | NEST and UNNEST with ANSI syntax |
| `QuerySubqueryTests` | `tuq_subquery.py` | Subquery tests |
| `JoinQueryTests` | `dynamic_join_n1ql.py` | Dynamic join queries |

### User-Defined Functions (UDF)

| Test Class | File | Description |
|------------|------|-------------|
| `QueryUDFTests` | `tuq_UDF.py` | UDF creation and execution |
| `QueryUDFN1QLTests` | `tuq_UDF_N1QL.py` | N1QL UDF integration |
| `QueryBackupUDFTests` | `tuq_backup_udf.py` | UDF backup and restore |
| `QueryUdfCteLetTests` | `tuq_udf_cte_let.py` | UDF with CTE and LET |

### Vector Search Tests

**Single-file comprehensive approach with multiple configuration files**

| Test Class | File | Description |
|------------|------|-------------|
| `VectorSearchTests` | `tuq_vectorsearch.py` | Vector similarity search (KNN/ANN), pushdown optimization |

**Key Test Coverage:**
- KNN search with multiple distance metrics (L2, EUCLIDEAN, DOT, COSINE)
- ANN search with IVF, PQ, SQ8 index types and nprobes configuration
- Vector pushdown optimization tests (25+ dedicated test methods)
- Vector indexing with early filter optimization
- LIMIT pushdown for various scalar/vector combinations
- Index selection and use index scenarios
- Vector with predicates, aggregates, ORDER BY, subqueries, CTEs
- Vector normalization, encode/decode operations
- Vector with UNNEST, ARRAY_ANY, ARRAY_EVERY
- Vector explain and advise functionality
- Vector transactions, prepared statements, UDF integration
- Sparse vector testing
- Vector NULL handling and zero distance tests

### Window Functions

| Test Class | File | Description |
|------------|------|-------------|
| `WindowFunctionsTest` | `n1ql_window_functions.py` | Window function tests |
| `WindowFunctionsSyntaxTest` | `n1ql_window_functions_syntax_check.py` | Window function syntax validation |
| `QueryWindowClauseTests` | `tuq_window_clause.py` | Window clause tests |

### Collections and Scopes

| Test Class | File | Description |
|------------|------|-------------|
| `QueryCollectionsDDLTests` | `n1ql_collections_ddl.py` | Collection DDL operations |
| `QueryCollectionsEnd2EndTests` | `n1ql_collections_end2end.py` | End-to-end collection tests |
| `QueryCollectionsClusteropsTests` | `n1ql_collections_clusterops.py` | Collection cluster operations |
| `QueryContextTests` | `tuq_query_context.py` | Query context with collections |

### Security and RBAC

| Test Class | File | Description |
|------------|------|-------------|
| `RbacN1QL` | `n1ql_rbac_2.py` | Role-Based Access Control |
| `N1QLRBACTests` | `n1ql_rbac.py` | RBAC tests |
| `ReadOnlyUserTests` | `n1ql_ro_user.py` | Read-only user permissions |
| `QueryN1QLAuditTests` | `tuq_n1ql_audit.py` | Audit logging |

### Full-Text Search Integration

| Test Class | File | Description |
|------------|------|-------------|
| `N1qlFTSIntegrationTest` | `n1ql_fts_integration.py` | FTS integration |
| `N1qlFTSIntegrationPhase2Test` | `n1ql_fts_integration_phase2.py` | FTS Phase 2 features |
| `N1qlFTSSanityTest` | `n1ql_fts_sanity.py` | FTS sanity tests |
| `FlexIndexTests` | `flex_index_phase1.py` | Flex index (FTS+GSI) tests |

### Aggregate and Statistics

| Test Class | File | Description |
|------------|------|-------------|
| `StatisticAggregatesTest` | `n1ql_statistic_aggregates.py` | Aggregate functions |
| `QueryUpdateStatsTests` | `tuq_update_statistics.py` | UPDATE STATISTICS tests |
| `GroupByAliasTests` | `n1ql_group_by_alias.py` | GROUP BY with aliases |

### Query Advisor and Planning

| Test Class | File | Description |
|------------|------|-------------|
| `QueryAdviseTests` | `tuq_advise.py` | ADVISE statement |
| `QueryAdvisorTests` | `tuq_advisor.py` | Query advisor functionality |

### Prepared Statements

| Test Class | File | Description |
|------------|------|-------------|
| `QueryAutoPrepareTests` | `tuq_auto_prepare.py` | Auto-prepare functionality |
| `QueryMultiNodePreparedStatementTests` | `tuq_multinode_prepared_statements.py` | Multi-node prepared statements |

### AI-Generated Query Testing (AiQG)

| Test Class | File | Description |
|------------|------|-------------|
| `QueryAiQGTests` | `tuq_AiQG_runner.py` | AI-generated query testing (CBO, UDF, prepared) |

**AiQG Test Features:**
- Executes AI-generated queries with multiple validation modes:
  - Index Testing (execute without indexes, get advisor recommendations, validate results)
  - Cost-Based Optimizer (CBO) Comparison (execute with/without CBO, compare times and results)
  - UDF Comparison (create inline SQL++ UDF from query, parameterize predicates, validate results)
  - Prepared Statement Comparison (prepare query, execute with profile timings, validate results)
- Query Parameters: `memory_quota` (default: 100 MB), `timeout` (default: "120s")

**AiQG Query Generation:**
Located in `scripts/AiQG/`:
- **AiQG.py**: Main script for AI-powered N1QL query generation using LangChain and OpenAI
- Workflow: generates queries based on prompt files, executes against cluster to validate syntax, saves successful/failed queries with error context
- Supported Patterns: Multiple UNNEST, LET clause with subqueries, OFFSET, subqueries with aggregates, CTE with LET and UNNEST

### CURL and External Functions

| Test Class | File | Description |
|------------|------|-------------|
| `QueryCurlTests` | `tuq_curl.py` | CURL function tests |
| `QueryWhitelistTests` | `tuq_curl_whitelist.py` | CURL whitelist configuration |

### Monitoring and Performance

| Test Class | File | Description |
|------------|------|-------------|
| `QueryMonitoringTests` | `tuq_monitoring.py` | Query monitoring |
| `QueryStatsTests` | `tuq_query_stats.py` | Query statistics |
| `QueryN1QLBackfillTests` | `tuq_n1ql_backfill.py` | Query backfill |
| `QueryProfilingTests` | `tuq_profiling.py` | Query profiling |
| `QueryAWRTests` | `tuq_awr.py` | Automatic Workload Repository |

### Cluster Operations

| Test Class | File | Description |
|------------|------|-------------|
| `QueriesOpsTests` | `tuq_cluster_ops.py` | Cluster operations during queries |
| `QueryGracefulFailoverTests` | `tuq_graceful_failover.py` | Graceful failover tests |
| `QueriesUpgradeTests` | `n1ql_upgrade.py` | Upgrade tests |

### Data Types and Functions

| Test Class | File | Description |
|------------|------|-------------|
| `NULLTests` | `tuq_nulls.py` | NULL handling |
| `OrderByNullsTests` | `tuq_order_by_nulls.py` | NULL ordering in ORDER BY |
| `QueryStringFunctionsTests` | `tuq_string_functions.py` | String functions |
| `ScalarFunctionsTests` | `tuq_scalars.py` | Scalar functions |
| `TokenTests` | `tuq_tokens.py` | Token functions |
| `QueryConditionalFunctionsTests` | `n1ql_conditional_functions.py` | Conditional functions |
| `QueryBitwiseTests` | `n1ql_bitwise.py` | Bitwise operations |
| `QueryHashBytesTests` | `tuq_hashbytes.py` | Hash functions |
| `QueryTimeSeriesTests` | `tuq_timeseries.py` | Time series queries |

### Other Tests

| Test Class | File | Description |
|------------|------|-------------|
| `QuerySequenceTests` | `tuq_sequence.py` | Sequence operations |
| `QueryCTETests` | `tuq_cte.py` | Common Table Expressions |
| `QueryChainedLetTests` | `tuq_chained_let.py` | Chained LET clauses |
| `QueryXattrTests` | `tuq_xattr.py` | Extended attributes |
| `QueryInferTests` | `tuq_infer.py` | INFER statement |
| `QuerySeqScanTests` | `tuq_sequential_scan.py` | Sequential scan tests |
| `QueryMiscTests` | `tuq_misc.py` | Miscellaneous query tests |
| `OptionsTests` | `n1ql_options.py` | Query options |
| `QueryExpirationTests` | `ttl_with_n1ql.py` | TTL with N1QL |
| `InListOperatorTests` | `tuq_inlist.py` | IN list operator |

### Serverless Tests

Located in `pytests/tuqquery/serverless/`:

| Test Class | File | Description |
|------------|------|-------------|
| `QueryMeteringTests` | `serverless/tuq_metering.py` | Serverless metering |
| `QueryThrottlingTests` | `serverless/tuq_throttling.py` | Serverless throttling |
| `QuerySecurityTests` | `serverless/tuq_security.py` | Serverless security |

---

## Test Categories

### 1. Core Query Operations
- **Files**: `tuq_sanity.py`, `tuq.py`, `newtuq.py`
- **Coverage**: SELECT statements, WHERE clauses, GROUP BY and HAVING, ORDER BY and LIMIT, DISTINCT operations, META() function, aggregates, scalar functions, array operations
- **Coverage**: 95% | **Maturity**: High

### 2. Indexing
- **Files**: `tuq_gsi_index.py`, `tuq_index.py`, `tuq_gsi_index_extended.py`, `tuq_array_flattening.py`
- **Coverage**: Primary indexing, Secondary indexing (GSI), Covering indexes, Array indexing, Composite indexes, Index optimization, Index selection, FLATTEN_KEYS
- **Coverage**: 90% | **Maturity**: High

### 3. DML (Data Manipulation Language)
- **Files**: `tuq_dml.py`
- **Coverage**: INSERT, UPDATE, DELETE, UPSERT, MERGE
- **Coverage**: 85% | **Maturity**: High

### 4. DDL (Data Definition Language)
- **Files**: `tuq_ddl.py`, `tuq_ddl_bucket.py`, `tuq_ddl_user.py`
- **Coverage**: CREATE INDEX, ALTER INDEX, DROP INDEX, INDEX management, bucket/user-level DDL
- **Coverage**: 80% | **Maturity**: Medium

### 5. JOIN Operations
- **Files**: `tuq_join.py`, `tuq_ansi_joins.py`, `tuq_ansi_merge.py`
- **Coverage**: Implicit JOINs, Explicit JOINs (INNER, LEFT, RIGHT), ANSI JOIN syntax, JOIN optimization, Nested JOINs, JOIN with keys
- **Coverage**: 75% | **Maturity**: Medium

### 6. Advanced Query Features
- **Files**: `tuq_subquery.py`, `tuq_cte.py`, `tuq_chained_let.py`, `tuq_let_clause.py`
- **Coverage**: Subqueries (correlated, uncorrelated), Common Table Expressions (CTE), LET clause, subquery optimization
- **Coverage**: 70% | **Maturity**: Medium

### 7. Vector Search
- **Files**: `tuq_vectorsearch.py`, `tuq_vectorsearch_misc.py`
- **Coverage**: Vector indexing, KNN search, ANN search, Vector similarity functions, Vector aggregation, Pushdown optimization
- **Configuration**: 8 specialized conf files for different scenarios
- **Coverage**: 85% | **Maturity**: High

### 8. Window Functions
- **Files**: `n1ql_window_functions.py`, `n1ql_window_functions_syntax_check.py`, `tuq_window_clause.py`
- **Coverage**: RANK, DENSE_RANK, ROW_NUMBER, NTILE, FIRST_VALUE, LAST_VALUE, PARTITION BY, window frame
- **Coverage**: 70% | **Maturity**: Medium

### 9. User Defined Functions (UDF)
- **Files**: `tuq_UDF_N1QL.py`, `tuq_UDF.py`, `tuq_udf_cte_let.py`, `tuq_backup_udf.py`
- **Coverage**: N1QL inline functions, function execution, function management, error handling, integration with advanced features
- **Coverage**: 65% | **Maturity**: Medium

### 10. Functions
- **Files**: `tuq_string_functions.py`, `tuq_scalars.py`, `n1ql_conditional_functions.py`, `date_time_functions.py`, `n1ql_bitwise.py`, `tuq_hashbytes.py`
- **Coverage**: Scalar functions (mathematical, string, type conversion), Aggregate functions, Conditional functions, Date/time functions, Array functions, Bitwise operations
- **Coverage**: 85% | **Maturity**: High

### 11. Collections and Scopes
- **Files**: `n1ql_collections_ddl.py`, `n1ql_collections_end2end.py`, `tuq_system_keyspaces.py`
- **Coverage**: Scope operations, Collection operations, Cross-collection queries, System keyspaces
- **Coverage**: 85% | **Maturity**: High

### 12. Security
- **Files**: `n1ql_rbac.py`, `tuq_n1ql_audit.py`, `tuq_ro_user.py`, `tuq_tls_sanity.py`
- **Coverage**: RBAC (Role-Based Access Control), Query auditing, TLS/SSL connections, Authentication and authorization, Read-only permissions
- **Coverage**: 75% | **Maturity**: Medium

### 13. Performance and Optimization
- **Files**: `tuq_advisor.py`, `tuq_advise.py`, `tuq_early_filter_order.py`, `tuq_query_stats.py`, `tuq_monitoring.py`, `tuq_profiling.py`
- **Coverage**: Query advisor, Index recommendations, Filter pushdown, Query statistics, Performance monitoring, Performance tuning
- **Coverage**: 60% | **Maturity**: Low

### 14. Integration
- **Files**: `n1ql_fts_integration.py`, `n1ql_fts_integration_phase2.py`, `tuq_xattr.py`, `ttl_with_n1ql.py`, `tuq_aus.py`, `flex_index_phase1.py`
- **Coverage**: Full Text Search (FTS) integration, Analytics integration, XATTR (Extended Attributes), TTL operations, Adaptive indexing
- **Coverage**: 70% | **Maturity**: Medium

### 15. Cluster Operations
- **Files**: `n1ql_upgrade.py`, `tuq_cluster_ops.py`, `tuq_graceful_failover.py`, `tuq_ops.py`
- **Coverage**: Rebalance queries, Failover scenarios, Scaling operations, Upgrade tests, Cluster operations during queries
- **Coverage**: 70% | **Maturity**: Medium

### 16. Error Handling
- **Files**: `tuq_nulls.py`, `tuq_order_by_nulls.py`, `tuq_filter.py`, `tuq_error_handler.py`
- **Coverage**: NULL handling, Error messages, Edge cases, Boundary conditions, WHERE clause optimization, Filter pushdown
- **Coverage**: 80% | **Maturity**: Medium

---

## Test Suite Configuration Categories

### Sanity and Extended Sanity
- `meta.conf` - Main sanity test configuration
- `py-tuq-ext-sanity.conf` - Extended sanity tests
- `py-tuq-ext-sanity-2.conf` - Extended sanity part 2
- `py-tuq-ext-sanity-3.conf` - Extended sanity part 3

### Index Configurations
- `py-tuq-index.conf` - Index tests
- `py-tuq-gsi.conf` - GSI tests
- `py-covering-index.conf` - Covering index tests
- `py-tuq-index-unnest.conf` - Index with UNNEST

### Join and DML
- `py-tuq-joins.conf` - Join tests
- `py-tuq-ansi-joins.conf` - ANSI join tests
- `py-tuq-ansi-merge.conf` - ANSI merge tests
- `py-tuq-dml.conf` - DML operations

### Security
- `py-n1ql-rbac.conf` - RBAC tests
- `py-n1ql-rbac-collections.conf` - Collection-level RBAC
- `py-tuq-n1ql-audit.conf` - Audit tests

### Vector Search (8 configuration files)
- `py-tuq-vector-ann-pushdown.conf` - Vector pushdown optimization tests
- `py-tuq-vector-ann.conf` - ANN vector search
- `py-tuq-vector-knn.conf` - KNN vector search
- `py-tuq-vector-ann-bhive.conf` - BHive ANN tests
- `py-tuq-vector-misc.conf` - Vector miscellaneous
- `py-tuq-vector-misc-2.conf` - Additional vector miscellaneous
- `py-tuq-vector-advise-explain.conf` - Vector ADVISE and EXPLAIN
- `py-tuq-vector-zero.conf` - Zero vector scenarios

### UDF
- `py-tuq-udf-n1ql.conf` - N1QL UDF tests
- `py-tuq-udf-analytics.conf` - Analytics UDF
- `py-tuq-backup-udf.conf` - UDF backup

### Window Functions
- `py-tuq-window_functions.conf` - Window functions
- `py-tuq-window_clause.conf` - Window clauses

### FTS Integration
- `n1ql_fts_integration_phase1_P1.conf` - FTS Phase 1
- `n1ql_fts_integration_phase2_functional_moss_P1.conf` - FTS Phase 2 functional

### Collections
- `py-n1ql-collections_ddl.conf` - Collection DDL
- `py-n1ql-collections_end2end.conf` - Collection E2E
- `py-n1ql-collections_clusterops.conf` - Collection cluster ops

### Array Flattening
- `py-tuq-array-flattening.conf` - Array flattening
- `py-tuq-array-flattening-covering.conf` - Covering index with array flattening

### Performance and Monitoring
- `py-tuq-monitoring.conf` - Monitoring tests
- `py-tuq-profiling.conf` - Profiling tests
- `py-tuq-awr.conf` - AWR tests
- `py-tuq-statistics.conf` - Statistics tests

### AI Query Generator (AiQG)
- `py-tuq-aiqg.conf` - AI-generated query testing (CBO, UDF, prepared statement validation)

---

## Configuration System

Test cases are defined and configured through configuration files in `/conf/tuq/`.

### Meta Configuration Example

From `conf/tuq/meta.conf`:

```ini
tuqquery.tuq_sanity.QuerySanityTests:
    test_meta_basic,skip_index=True,doc-per-day=1,covering_index=True,GROUP=P0
    test_meta_where,skip_index=True,doc-per-day=1,covering_index=True,GROUP=P0
    test_meta_where_greater_than,skip_index=True,doc-per-day=1,covering_index=True,GROUP=P0
    test_meta_partial,skip_index=True,doc-per-day=1,covering_index=True,GROUP=P0
    test_meta_non_supported,skip_index=True,doc-per-day=1,GROUP=P0
    test_meta_negative_namespace,skip_index=True,doc-per-day=1,GROUP=P0
    test_meta,doc-per-day=1,covering_index=True,GROUP=P0
    test_meta_cas,doc-per-day=1,covering_index=True,GROUP=P0
    test_meta_negative,doc-per-day=1,covering_index=True,GROUP=P0
```

### Configuration Parameters

- `skip_index`: Skip index creation for the test
- `doc-per-day`: Number of documents to generate per day
- `covering_index`: Use covering index optimization
- `GROUP`: Test priority level (P0, P1, P2, P3)
- `BUGS`: Associated bug IDs for known issues
- `reload_data`: Reload test data before execution
- `distance`: Distance metric for vector search (L2, EUCLIDEAN, DOT, COSINE, etc.)
- `nprobes`: Number of probes for ANN vector search
- `description`: Index description for vector search (e.g., IVF,SQ8)
- `compare_cbo`: Enable CBO comparison for AiQG tests
- `compare_udf`: Enable UDF comparison for AiQG tests
- `compare_prepared`: Enable prepared statement comparison for AiQG tests

---

## Running Tests

### Running from CLI

```bash
# Run all tuqquery tests
python testrunner.py -C conf/tuq/meta.conf

# Run specific configuration file
python testrunner.py -c conf/tuq/py-tuq-ext-sanity.conf

# Run specific test class
python testrunner.py -t tuqquery.tuq_sanity.QuerySanityTests

# Run with specific parameters
python testrunner.py -t tuqquery.tuq_vectorsearch.VectorSearchTests.test_ann_search -p distance=L2,nprobes=5

# Run specific test suite with configuration
python testrunner.py -c conf/tuq/py-tuq-joins.conf
```

### Running Tests with Priorities

```bash
# Run P0 (highest priority) tests only
python testrunner.py -C conf/tuq/meta.conf -group=P0

# Run P1 tests
python testrunner.py -C conf/tuq/meta.conf -group=P1

# Run P2 tests
python testrunner.py -C conf/tuq/meta.conf -group=P2
```

### Running Tests with Groups

```bash
# Run specific group tests (e.g., LIKE operator tests)
python testrunner.py -C conf/tuq/meta.conf -group=LIKE

# Run multiple groups
python testrunner.py -C conf/tuq/meta.conf -group="LIKE;DATE;GROUP"
```

### Sample Test Commands

```bash
# Vector search with specific parameters
python testrunner.py -t tuqquery.tuq_vectorsearch.VectorSearchTests.test_ann_search

# Join tests
python testrunner.py -c conf/tuq/py-tuq-joins.conf

# UDF tests
python testrunner.py -c conf/tuq/py-tuq-udf-n1ql.conf

# Vector pushdown optimization tests
python testrunner.py -c conf/tuq/py-tuq-vector-ann-pushdown.conf

# CTE tests
python testrunner.py -t tuqquery.tuq_cte.QueryCTETests

# Window functions
python testrunner.py -c conf/tuq/py-tuq-window_functions.conf
```

---

## Test Priorities

Tests are categorized by priority levels:

- **P0**: Critical tests for core functionality, must pass
- **P1**: High priority tests for major features
- **P2**: Medium priority tests for edge cases and advanced features
- **P3**: Low priority tests for rare scenarios and experimental features

### Priority Examples in Configuration

```ini
tuqquery.tuq_sanity.QuerySanityTests:
    test_meta_basic,GROUP=P0
    test_array_agg,GROUP=SCALAR;P0
    test_like,GROUP=LIKE;P0
    test_group_by,GROUP=GROUP;P0
```

### Priority Distribution

- **P0 Tests**: Core sanity, basic SELECT, META functions, aggregates
- **P1 Tests**: JOIN operations, window functions, UDF, vector search
- **P2 Tests**: Advanced scenarios, edge cases, integration tests
- **P3 Tests**: Experimental features, performance edge cases

---

## Test Group Labels

Tests are tagged with groups for filtering and organization:

- `P0` - Highest priority tests
- `P1` - High priority tests
- `P2` - Medium priority tests
- `CE` - Community Edition compatible tests
- `NON_CE` - Enterprise Edition only tests
- `UNION` - Union operation tests
- `LIKE` - LIKE operator tests
- `DATE` - Date function tests
- `SCALAR` - Scalar function tests
- `GROUP` - GROUP BY tests
- `ANY;SATISFY` - ANY Satisfy clause tests

### Group Usage in Configuration

```ini
# Tag test with multiple groups
test_any_external,GROUP=ANY;SATISFY;P1

# Tag with priority and feature
test_like_pattern,GROUP=LIKE;P0

# Tag with scalar function
test_abs,GROUP=SCALAR;P0
```

---

## Base Test Class Hierarchy

```
BaseTestCase
└── QueryTests (tuq.py)
    ├── QuerySanityTests (tuq_sanity.py)
    │   ├── JoinTests
    │   ├── QueriesOpsTests
    │   └── ConcurrentTests
    ├── VectorSearchTests (tuq_vectorsearch.py)
    ├── QueryUDFTests (tuq_UDF.py)
    ├── WindowFunctionsTest (n1ql_window_functions.py)
    ├── QueryCollectionsDDLTests (n1ql_collections_ddl.py)
    ├── JoinTests (tuq_join.py)
    ├── QuerySubqueryTests (tuq_subquery.py)
    └── ... (most test classes extend either QueryTests or QuerySanityTests)
```

### QueryTests
Main base class for all query tests. Provides:
- Query execution infrastructure
- Result verification
- Index management
- Data loading
- Configuration handling
- Utility methods for common test patterns

### QuerySanityTests
Extends QueryTests for sanity testing. Provides:
- Basic query operations
- Meta function coverage
- Sample dataset loading
- Setup/teardown for test environment

---

## Test Helpers

### Common Test Patterns

```python
# Run a query
result = self.run_cbq_query(query="SELECT * FROM bucket")

# Wait for indexes
self.wait_for_all_indexes_online()

# Verify results
self.assertEqual(actual['results'], expected)

# Create index
self.run_cbq_query(query="CREATE INDEX idx ON bucket(field)")

# Verify index count
self.assertEqual(len(indexes), expected_count)

# Get index list
index_list = self.get_index_list()
```

### Common Utility Methods

- `run_cbq_query()`: Execute N1QL query and return results
- `wait_for_all_indexes_online()`: Wait for index build completion
- `create_index()`: Create a specific index
- `drop_index()`: Drop an index
- `get_index_list()`: Get list of existing indexes
- `load_data()`: Load test data into bucket
- `verify_results()`: Compare query results with expected values

---

## Additional Resources

### Test Utilities
- **Data generators**: `couchbase_helper/tuq_generators.py`
- **Document generators**: `couchbase_helper/documentgenerator.py`
- **Collection utilities**: `lib/collection/collections_n1ql_client.py`
- **Vector utilities**: `lib/vector/vector.py`

### Test Data Sources
- **Sample datasets**: travel-sample, beer-sample
- **Custom data generation**: Built-in generators
- **Vector datasets**: SIFT (dense vectors), Sparse (sparse vectors)
- **Data loading scripts**: Batch loading utilities

### External Data Loading
- **SIFT dataset**: For dense vector search testing
- **Sparse dataset**: For sparse vector search testing
- **Ground truth data**: For KNN/ANN accuracy validation

---

## Maintenance Notes

### Adding New Tests

1. Create test file in `/pytests/tuqquery/`
2. Follow naming convention: `tuq_<feature>.py` or `n1ql_<feature>.py`
3. Extend appropriate base class (`QueryTests` or `QuerySanityTests`)
4. Create configuration file in `/conf/tuq/`
5. Add test methods with appropriate naming conventions
6. Add to appropriate configuration category
7. Assign priority level (P0, P1, P2, P3)

### Test Best Practices

- Use descriptive test method names
- Include comprehensive result verification
- Handle cleanup in `tearDown()` methods
- Use appropriate test priorities
- Document dependencies and prerequisites
- Follow consistent code style across test files
- Use configuration parameters for test variations

### Adding New Configuration Parameters

1. Add parameter to test method signature
2. Use `self.input.param()` to retrieve value
3. Document parameter in configuration file header
4. Add parameter to test configuration examples
5. Update relevant test methods to use parameter

### Known Issues

Tests may reference known bugs using the `BUGS` parameter:

```ini
test_any_external,GROUP=ANY;SATISFY;P1,BUGS=MB-9188_coll_doesnt_allow_external_docs
```

### Updating Test Coverage

1. Regular review of test coverage reports
2. Identify gaps in test cases
3. Prioritize based on feature criticality
4. Add new tests or expand existing test suites
5. Update configuration files accordingly
6. Update documentation (this file)

---

## Summary

The N1QL Query test suite provides comprehensive coverage of Couchbase Query Service functionality through approximately:

- **125 test files** organized across ~110 test classes
- **185+ configuration files** for different test scenarios and priorities
- **15+ major test categories** covering all Query Service features
- **2,500+ test methods** with detailed coverage
- **4 priority levels** (P0, P1, P2, P3) for organized test execution

The modular structure allows for:
- Easy maintenance and extension of test coverage
- Organized test execution by priority, group, or configuration
- Comprehensive coverage of basic operations, advanced features, security, performance, and integration
- Scalable testing infrastructure for new Query Service features

This test suite ensures high-quality Query Service functionality and reduces production incident risks through systematic testing of all major Query features.

---

**Document Version**: 1.0  
**Last Updated**: March 18, 2026  
**Maintained By**: Query Test Team  
**Next Review**: Quarterly
