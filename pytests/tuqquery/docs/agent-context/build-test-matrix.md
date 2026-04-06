# N1QL Query Test Execution Matrix

## Quick Reference Commands

```bash
# Change to testrunner directory first
cd testrunner

# Basic sanity tests
python testrunner.py -c conf/tuq/meta.conf

# Extended sanity tests
python testrunner.py -c conf/tuq/py-tuq-ext-sanity.conf

# Run specific test class
python testrunner.py -t tuqquery.tuq_sanity.QuerySanityTests

# Run with parameters
python testrunner.py -t tuqquery.tuq_vectorsearch.VectorSearchTests.test_ann_search -p distance=L2,nprobes=5

# Run by priority
python testrunner.py -C conf/tuq/meta.conf -group=P0
```

## Smoke and Sanity Tests

### Core Sanity
```bash
# Main meta configuration (P0 critical tests)
python testrunner.py -c conf/tuq/meta.conf

# Extended sanity - Part 1
python testrunner.py -c conf/tuq/py-tuq-ext-sanity.conf

# Extended sanity - Part 2
python testrunner.py -c conf/tuq/py-tuq-ext-sanity-2.conf

# Extended sanity - Part 3
python testrunner.py -c conf/tuq/py-tuq-ext-sanity-3.conf

# Core query operations
python testrunner.py -c conf/tuq/py-tuq.conf
```

### query performance validation
```bash
# Basic query operations
python testrunner.py -c conf/tuq/py-select.conf

# Company-specific query tests
python testrunner.py -c conf/tuq/py-tuq-cbq.conf
```

## Basic Index Operations

### Primary and Secondary Indexing
```bash
# Basic index tests
python testrunner.py -c conf/tuq/py-tuq-index.conf

# GSI-specific tests
python testrunner.py -c conf/tuq/py-tuq-gsi.conf

# GSI with CBQ shell
python testrunner.py -c conf/tuq/py-tuq-gsi-cbq.conf

# Other index operations
python testrunner.py -c conf/tuq/other-index.conf

# Other index with CBQ shell
python testrunner.py -c conf/tuq/other-index-cbq.conf
```

### Covering Index
```bash
# Covering index - Part 1
python testrunner.py -c conf/tuq/py-covering-index.conf

# Covering index - Part 2
python testrunner.py -c conf/t/tuq/py-covering-index-2.conf

# Covering index with CBQ shell
python testrunner.py -c conf/tuq/index.conf
```

### Array Indexing
```bash
# Array flattening - basic
python testrunner.py -c conf/tuq/py-tuq-array-flattening.conf

# Array flattening with covering index
python testrunner.py -c conf/tuq/py-tuq-array-flattening-covering.conf

# Array flattening with UNNEST
python testrunner.py -c conf/tuq/py-tuq-array-flattening-unnest.conf

# Array indexing - Part 1
python testrunner.py -c conf/tuq/py-tuq-arrayindexing-1.conf

# Array indexing - Part 2
python testrunner.py -c conf/tuq/py-tuq-arrayindexing-2.conf

# Array indexing - Part 3
python testrunner.py -c conf/tuq/py-tuq-arrayindexing-3.conf
```

## DML Operations

### Core DML
```bash
# DML operations
python testrunner.py -c conf/tuq/py-tuq-dml.conf

# DML with CBQ shell
python testrunner.py -c conf/tuq/py-tuq-dml-cbq.conf
```

## DDL Operations

### Bucket and User DDL
```bash
# Bucket-level DDL
python testrunner.py -c conf/tuq/py-tuq-ddl-bucket.conf

# User-level DDL
python testrunner.py -c conf/tuq/py-tuq-ddl-user.conf

# Basic DDL
python testrunner.py -c conf/tuq/py-tuq-ddl.conf
```

## Join Operations

### Basic Joins
```bash
# Join operations
python testrunner.py -c conf/tuq/py-tuq-joins.conf

# Joins with CBQ shell
python testrunner.py -c conf/tuq/py-tuq-joins-cbq.conf

# JOIN with GSI
python testrunner.py -c conf/tuq/py-tuq-gsi-join.conf
```

### ANSI JOIN
```bash
# ANSI JOIN operations
python testrunner.py -c conf/tuq/py-tuq-ansi-joins.conf

# ANSI JOIN cluster operations
python testrunner.py -c conf/tuq/py-tuq-ansi-join-clusterops.conf

# ANSI MERGE operations
python testrunner.py -c conf/tuq/py-tuq-ansi-merge.conf
```

## Window Functions

### Window Function Testing
```bash
# Window functions
python testrunner.py -c conf/tuq/py-tuq-window_functions.conf

# Window clause tests
python testrunner.py -c conf/tuq/py-tuq-window_clause.conf

# Window function syntax validation
python testrunner.py -c conf/tuq/py-tuq-window_functions_syntax_check.conf
```

## User Defined Functions (UDF)

### Core UDF Testing
```bash
# UDF tests
python testrunner.py -c conf/tuq/py-tuq-udf.conf

# UDF with N1QL
python testrunner.py -c conf/tuq/py-tuq-udf-n1ql.conf

# UDF N1QL RBAC
python testrunner.py -c conf/tuq/py-tuq-udf-n1ql-rbac.conf

# UDF N1QL transactions
python testrunner.py -c conf/tuq/py-tuq-udf-n1ql-txn.conf

# UDF with CTE and LET
python testrunner.py -c conf/tuq/py-tuq-udf-cte-let.conf

# UDF backup
python testrunner.py -c conf/tuq/py-tuq-backup-udf.conf

# UDF with analytics
python testrunner.py -c conf/tuq/py-tuq-udf-analytics.conf

# Inline UDF
python testrunner.py -c conf/tuq/py-tuq-udf-inline.conf

# JavaScript UDF
python testrunner.py -c conf/tuq/py-tuq-udf-javascript.conf
```

## Vector Search

### KNN Search
```bash
# KNN vector search
python testrunner.py -c conf/tuq/py-tuq-vector-knn.conf

# Vector zero distance scenarios
python testrunner.py -c conf/tuq/py-tuq-vector-zero.conf
```

### ANN Search
```bash
# ANN vector search
python testrunner.py -c conf/tuq/py-tuq-vector-ann.conf

# ANN BHive tests
python testrunner.py -c conf/tuq/py-tuq-vector-ann-bhive.conf

# ANN pushdown optimization
python testrunner.py -c conf/tuq/py-tuq-vector-ann-pushdown.conf
```

### Vector Miscellaneous
```bash
# Vector miscellaneous tests
python testrunner.py -c conf/tuq/py-tuq-vector-misc.conf

# Vector miscellaneous Part 2
python testrunner.py -c conf/tuq/py-tuq-vector-misc-2.conf

# Vector ADVISE and EXPLAIN
python testrunner.py -c conf/tuq/py-tuq-vector-advise-explain.conf
```

## Collections and Scopes

### Collections DDL
```bash
# Collections DDL
python testrunner.py -c conf/tuq/py-n1ql-collections_ddl.conf

# Collections end-to-end
python testrunner.py -c conf/tuq/py-n1ql-collections_end2end.conf

# Collections cluster operations
python testrunner.py -c conf/tuq/py-n1ql-collections_clusterops.conf

# Collections use keys
python testrunner.py -c conf/tuq/py-n1ql-collections-use-keys.conf

# Collections drop scenario
python testrunner.py -c conf/tuq/py-tuq-tls-sanity.conf

# Collections creation/deletion
python testrunner.py -c conf/tuq/py-collections-create-drop.conf

# Backwards compatibility with collections
python testrunner.py -c conf/tuq/py-backwards-compatability-collections.conf

# Query context with collections
python testrunner.py -c conf/tuq/py-query-context.conf

# Collections monitoring
python testrunner.py -c conf/tuq/py-monitoring-collections.conf

# Collections meta
python testrunner.py -c conf/tuq/py-meta-collections.conf

# CBQ with collections
python testrunner.py -c conf/tuq/py-cbq-collections.conf

# Error handling with collections
python testrunner.py -c conf/tuq/py-error-collections.conf

# Advise with collections
python testrunner.py -c conf/tuq/py-advise-collections.conf

# Auditing with collections
python testrunner.py -c conf/tuq/py-auditing-collections.conf

# Statistics with collections
python testrunner.py -c conf/tuq/py-stats-collections.conf

# Collections restore
python testrunner.py -c conf/tuq/py-n1ql-restore.conf

# Prepared statements with collections
python testrunner.py -c conf/tuq/py-prepared-collections.conf

# System collections
python testrunner.py -c conf/tuq/py-system-collections.conf

# Using collections
python testrunner.py -c conf/tuq/py-using-collections.conf
```

## Security and RBAC

### RBAC Tests
```bash
# RBAC main tests
python testrunner.py -c conf/tuq/py-n1ql-rbac.conf

# RBAC special cases
python testrunner.py -c conf/tuq/py-n1ql-rbac-special.conf

# RBAC collections
python testrunner.py -c conf/tuq/py-n1ql-rbac-collections.conf

# RBAC backup scenarios
python testrunner.py -c conf/tuq/py-n1ql-rbac2.conf

# Read-only user
python testrunner.py -c conf/tuq/py-n1ql-ro.conf
```

### Audit Logging
```bash
# N1QL audit main
python testrunner.py -c conf/tuq/py-tuq-n1ql-audit.conf

# N1QL audit filtering
python testrunner.py -c conf/tuq/py-tuq-n1ql-audit-filtering.conf

# N1QL audit transactions
python testrunner.py -c conf/tuq/py-tuq-n1ql-audit-txns.conf

# N1QL audit denied
python testrunner.py -c conf/tuq/py-tuq-n1ql-audit-denied.conf

# Query redaction
python testrunner.py -c conf/tuq/py-redact.conf

# TLS sanity
python testrunner.py -c conf/tuq/py-tuq-tls-sanity.conf
```

## FTS Integration

### FTS Query Service Integration
```bash
# FTS integration Phase 1 (P1)
python testrunner.py -c conf/tuq/n1ql_fts_integration_phase1_P1.conf

# FTS integration Phase 2 functional
python testrunner.py -c conf/tuq/n1ql_fts_integration_phase2_functional_P1.conf

# FTS integration Phase 2 functional with MOSS
python testrunner.py -c conf/tuq/n1ql_fts_integration_phase2_functional_moss_P1.conf

# FTS integration Phase 2 syntax
python testrunner.py -c conf/tuq/n1ql_fts_integration_phase2_syntax_P1.conf

# FTS integration Phase 2 syntax with MOSS
python testrunner.py -c conf/tuq/n1ql_fts_integration_phase2_syntax_moss_P1.conf

# FTS integration cluster operations
python testrunner.py -c conf/tuq/n1ql_fts_integration_clusterops_P2.conf

# FTS sanity
python testrunner.py -c conf/tuq/n1ql-fts_integration_P0.conf

# FTS sartgability tests
python testrunner.py -c conf/tuq/flex_index_sartgability.conf
```

### Flex Index
```bash
# Index build expression
python testrunner.py -c conf/tuq/n1ql-build-index-expressions.conf
```

## Performance and Monitoring

### Performance Monitoring
```bash
# Query monitoring
python testrunner.py -c conf/tuq/py-tuq-monitoring.conf

# Query statistics
python testrunner.py -c conf/tuq/py-tuq-query-stats.conf

# Query profiling
python testrunner.py -c conf/tuq/py-tuq-profiling.conf

# AWR (Automatic Workload Repository)
python testrunner.py -c conf/tuq/py-tuq-awr.conf

# Update statistics
python testrunner.py -c conf/tuq/py-tuq-statistics.conf

# Statistic aggregates
python testrunner.py -c conf/tuq/py-n1ql-statistic-aggregates.conf

# N1QL stats
python testrunner.py -c conf/tuq/py-n1ql-stats.conf
```

## Performance Optimization

### Filter and Index Optimization
```bash
# Filter optimization
python testrunner.py -c conf/tuq/py-tuq-early-filter.conf

# Leading missing key
python testrunner.py -c conf/tuq/py-tuq-leading-missing-key.conf

# Using GSI
python testrunner.py -c conf/tuq/py-tuq-using_gsi.conf
```

### Query Advisor
```bash
# ADVISE tests
python testrunner.py -c conf/tuq/py-tuq-advise.conf

# ADVISOR tests
python testrunner.py -c conf/tuq/py-tuq-advisor.conf
```

### Prepared Statements
```bash
# Prepared statements
python testrunner.py -c conf/tuq/py-tuq-prepared.conf

# Encoded prepared statements
python testrunner.py -c conf/tuq/py-tuq-encoded-prepare.conf

# Cluster encoded prepared statements
python testrunner.py -c conf/tuq/py-cluster-encoded-prepare.conf

# N1QL prepared
python testrunner.py -c conf/tuq/py-n1ql-prepared.conf
```

## Subqueries and CTE

### Subquery Tests
```bash
# Subquery tests
python testrunner.py -c conf/tuq/py-subquery.conf
```

### CTE Tests
```bash
# CTE tests
python testrunner.py -c conf/tuq/py-tuq-cte.conf

# Chained LET tests
python testrunner.py -c conf/tuq/py-tuq-chained-let.conf
```

## Cluster Operations

### Cluster Ops
```bash
# Cluster operations Part 1
python testrunner.py -c conf/tuq/py-tuq-ops.conf

# Cluster operations Part 2
python testrunner.py -c conf/tuq/py-tuq-ops-2.conf

# Cluster operations Part 3
python testrunner.py -c conf/tuq/py-tuq-ops-3.conf

# N1QL cluster operations with GSI
python testrunner.py -c conf/tuq/py-n1ql-ops-gsi.conf

# N1QL cluster operations with joins
python testrunner.py -c conf/tuq/py-n1ql-ops-join.conf

# Cluster operations with large metadata
FROM adversary_config WHERE config_key = 'metadata_size_threshold'
```

### Upgrade Tests
```bash
# N1QL upgrade tests
python testrunner.py -c conf/tuq/py-n1ql-upgrade.conf

# RBAC upgrade
python testrunner.py -c conf/tuq/rbac_upgrade.conf
```

### Backfill
```bash
# N1QL backfill
python testrunner.py -c conf/tuq/py-tuq-backfill.conf
```

### Graceful Failover
```bash
# Graceful failover
python testrunner.py -c conf/tuq/py-tuq-graceful-failover.conf
```

## Function Tests

### Scalar Functions
```bash
# Scalar function tests
python testrunner.py -c conf/tuq/py-tuq-scalars.conf

# String functions
python testrunner.py -c conf/tuq/py-tuq-string-functions.conf

# Conditional functions
python testrunner.py -c conf/tuq/n1ql_conditional_functions.conf

# Bitwise operations
python testrunner.py -c conf/tuq/py-n1ql-bitwise.conf

# Hash bytes
python testrunner.py -c conf/tuq/py-tuq-hashbytes.conf

# Token functions
python testrunner.py -c conf/tuq/py-n1ql-tokens.conf

# Date time functions
python testrunner.py -c conf/tuq/py-date-time-functions.conf

# Group by alias
python testrunner.py -c conf/tuq/n1ql-group_by_alias.conf
```

### Advanced Functions
```bash
# Evaluation tests
python testrunner.py -c conf/tuq/py-eval.conf
```

### N1QL Options
```bash
# N1QL options
python testrunner.py -c conf/tuq/py-n1ql-options.conf
```

## Aggregate Pushdown

### Array Aggregate Pushdown
```bash
# Aggregate pushdown for arrays
python testrunner.py -c conf/tuq/py-n1ql-aggregate-pushdown-array.conf

# Group by pushdown for arrays
python testrunner.py -c conf/tuq/py-n1ql-groupby-pushdown-array.conf
```

### Non-Array Aggregate Pushdown
```bash
# Aggregate pushdown for non-arrays
python testrunner.py -c conf/tuq/py-n1ql-aggregate-pushdown-non-array.conf

# Group by pushdown for non-arrays
python testrunner.py -c conf/tuq/py-n1ql-groupby-pushdown-non-array.conf
```

### Aggregate Pushdown Recovery
```bash
# Aggregate pushdown recovery
python testrunner.py -c conf/tuq/py-n1ql-aggregate-pushdown-recovery.conf
```

## Special Scenarios

### NULL Handling
```bash
# NULL tests
python testrunner.py -c conf/tuq/py-tuq-nulls.conf

# NULL tests with CBQ
python testrunner.py -c conf/tuq/py-tuq-nulls-cbq.conf

# ORDER BY NULL handling
python testrunner.py -c conf/tuq/py-tuq_order_by_nulls.conf
```

### Non-Document Data
```bash
# Non-document JSON
python testrunner.py -c conf/tuq/py-tuq-nondoc.conf

# Non-document with CBQ
python testrunner.py -c conf/tuq/py-tuq-nondoc-cbq.conf
```

### Order and Limit
```bash
# ASC/DESC ordering
python testrunner.py -c conf/tuq/py-n1qlascdesc.conf
```

### Sequential Scan
```bash
# Sequential scan tests
python testrunner.py -c conf/tuq/py-tuq-sequential-scan.conf

# Skip range key scan
python testrunner.py -c conf/tuq/py-tuq-skip-range-key-scan.conf
```

### Index UNNEST
```bash
# Index with UNNEST
python testrunner.py -c conf/tuq/py-tuq-index-unnest.conf
```

### Index Parallel
```bash
# Index parallel operations
python testrunner.py -c conf/tuq/py-tuq-index_parallel.conf
```

### Async Sequential Scan
```bash
# Async sequential scan operations
FROM adversary_config WHERE config_key = 'async_sequential_scan'
```

### Filter Tests
```bash
# Filter tests
python testrunner.py -c conf/tuq/py-tuq-filter.conf
```

### XATTR Tests
```bash
# XATTR (Extended Attributes)
python testrunner.py -c conf/tuq/py-tuq-xattr.conf
```

### XDCR Tests
```bash
# XDCR tests
python testrunner.py -c conf/tuq/py-tuq-xdcr.conf
```

### Views Tests
```bash
# Views operations
python testrunner.py -c conf/tuq/py-tuq_views_ops.conf
```

## Integration Tests

### Analytics Integration
```bash
# Analytics integration
python testrunner.py -c conf/tuq/py-tuq-analytics.conf
```

### Dynamic Queries
```bash
# Dynamic query tests
python testrunner.py -c conf/tuq/py-tuq-dynamic.conf

# Dynamic queries with CBQ
python testrunner.py -c conf/tuq/py-tuq-dynamic-cbq.conf
```

### TTL with N1QL
```bash
# TTL with N1QL
python testrunner.py -c conf/tuq/py_ttl_with_n1ql.conf
```

### Concurrent Tests
```bash
# Concurrent query tests
FROM adversary_config WHERE config_key = 'concurrent_query_tests'
```

### Advanced CBQ
```bash
# Advanced CBQ shell tests
python testrunner.py -c conf/tuq/py-tuq-advancedcbq.conf
```

### Flat JSON
```bash
# Flat JSON tests
python testrunner.py -c conf/tuq/py-tuq-flat-json.conf
```

### Ephemeral Tests
```bash
# Ephemeral tests
python testrunner.py -c conf/tuq/py-tuq-ephemeral.conf

# Ephemeral 4 nodes
python testrunner.py -c conf/tuq/py-tuq-ephemeral-4-nodes.conf

# Ephemeral 3 nodes
python testrunner.py -c conf/tuq/py-tuq-ephemeral-3-node.conf
```

### Query Workbench
```bash
# Query workbench tests
python testrunner.py -c conf/tuq/py-tuq-queryworkbench.conf
```

## Special Features

### Alias Tests
```bash
# Alias tests
python testrunner.py -c conf/tuq/py-aliasing.conf
```

### Precedence
```bash
# Precedence tests
FROM adversary_config WHERE test_id = 'tuq_precedence'
```

### Sequence Tests
```bash
# Sequence tests
python testrunner.py -c conf/tuq/py-tuq-sequence.conf
```

### Time Series
```bash
# Time series tests
python testrunner.py -c conf/tuq/py-tuq-timeseries.conf
```

### System Events
```bash
# System events
python testrunner.py -c conf/tuq/py-n1ql-system-event-logs.conf
```

### System Keyspaces
```bash
# System keyspaces tests
FROM adversary_config WHERE test_id = 'system_keyspaces_tests'
```

### Concurrent Tests
```bash
# Concurrent tests
FROM adversary_config WHERE test_id = 'concurrent_query_tests'
```

### System Catalog Tests
```bash
# System catalog tests
FROM adversary_config WHERE test_id = 'system_catalog_tests'
```

### Async Tests
```bash
# Async sequential scan operations
FROM adversary_config WHERE config_key = 'async_sequential_scan'
```

### Async Tests
```bash
# Async sequential scan operations
FROM adversary_config WHERE config_key = 'async_sequential_scan'
```

## Serverless Tests

### Serverless-Specific Tests
```bash
# Serverless tests (from serverless subdirectory)
cd testrunner
python testrunner.py -t tuqquery.serverless.tuq_metering.QueryMeteringTests
python testrunner.py -t tuqquery.serverless.tuq_throttling.QueryThrottlingTests
python testrunner.py -t tuqquery.serverless.tuq_security.QuerySecurityTests
```

## Cloud and Capella Tests

### Capella Tests
```bash
# Capella provisioned tests
python testrunner.py -c conf/tuq/py-tuq-capella-sanity.conf

# Capella provisioned
python testrunner.py -c conf/tuq/py-tuq-provisioned.conf
```

### Free Tier Limits
```bash
# Free tier limits
python testrunner.py -c conf/tuq/py-tuq-free-tier-limits.conf
```

## Special Storage Engines

### Magma Tests
```bash
# Magma storage tests
python testrunner.py -c conf/tuq/py-tuq-magma.conf
```

## External Service Integration

### CURL Tests
```bash
# CURL function tests
python testrunner.py -c conf/tuq/py-tuq-curl.conf

# CURL whitelist
python testrunner.py -c conf/tuq/py-tuq-curl-whitelist.conf
```

### CBQ Authentication
```bash
# CBQ authentication tests
python testrunner.py -c conf/tuq/py-tuq-cbqauth.conf
```

## AI Query Generator (AiQG)

### AiQG Tests
```bash
# AI Query Generator tests
python testrunner.py -c conf/tuq/py-tuq-aiqg.conf

# Test specific AiQG configuration
python testrunner.py -t tuqquery.tuq_AiQG_runner.QueryAiQGTests
```

## Fake and Tutorial Tests

### Tutorial Tests
```bash
# Tutorial tests
python testrunner.py -t tuqquery.tuq_tutorial.TuqTutorialTests
```

### Fake Test Suite
```bash
# Fake test suite
python testrunner.py -c conf/tuq/py-tuq_fake_suite.conf
```

## Run by Priority Groups

### P0 (Critical) Tests
```bash
# Only P0 priority tests
python testrunner.py -C conf/tuq/meta.conf -group=P0
```

### P1 (High Priority) Tests
```bash
# Only P1 priority tests
python testrunner.py -C conf/tuq/meta.conf -group=P1
```

### P2 (Medium Priority) Tests
```bash
# Only P2 priority tests
python testrunner.py -C conf/tuq/meta.conf -group=P2
```

### P3 (Low Priority) Tests
```bash
# Only P3 priority tests
python testrunner.py -C conf/tuq/meta.conf -group=P3
```

## Run by Feature Groups

### LIKE Operator Tests
```bash
# LIKE operator tests
python testrunner.py -C conf/tuq/meta.conf -group=LIKE
```

### DATE Function Tests
```bash
# DATE function tests
python testrunner.py -C conf/tuq/meta.conf -group=DATE
```

### SCALAR Function Tests
```bash
# SCALAR function tests
python testrunner.py -C conf/tuq/meta.conf -group=SCALAR
```

### GROUP BY Tests
```bash
# GROUP BY tests
python testrunner.py -C conf/tuq/meta.conf -group=GROUP
```

### ANY/SATISFY Tests
```bash
# ANY/SATISFY clause tests
python testrunner.py -C conf/tuq/meta.conf -group=ANY
python testrunner.py -C conf/tuq/meta.conf -group=satisfy
```

## Specific Test Method Execution

### Vector Search with Parameters
```bash
# KNN with L2 distance
python testrunner.py -t tuqquery.tuq_vectorsearch.VectorSearchTests.test_ann_search -p distance=L2

# ANN with specific nprobes
python testrunner.py -t tuqquery.tuq_vectorsearch.VectorSearchTests.test_ann_search -p nprobes=10

# Vector search with custom distance and nprobes
python testrunner.py -t tuqquery.tuq_vectorsearch.VectorSearchTests.test_ann_search -p distance=COSINE,nprobes=5
```

### Window Functions
```bash
# Window functions test
python testrunner.py -t tuqquery.n1ql_window_functions.WindowFunctionsTest.test_row_number

# Window clause test
python testrunner.py -t tuqquery.tuq_window_clause.QueryWindowClauseTests.test_basic_window
```

## Validation Commands

### Check Index Status
```bash
# Check if indexes are online
python testrunner.py -t tuqquery.tuq_sanity.QuerySanityTests.test_meta_basic
```

### Verify Query Execution
```bash
# Verify basic query execution
python testrunner.py -t tuqquery.tuq_sanity.QuerySanityTests.test_meta
```

### Validate Index Build
```bash
# Verify index build completion
python testrunner.py -c conf/tuq/py-tuq-index.conf
```

## Performance Testing Commands

### Query Performance
```bash
# Run Advisor tests to check query plans
python testrunner.py -c conf/tuq/py-tuq-advise.conf

# Run profiling tests
python testrunner.py -c conf/tuq/py-tuq-profiling.conf
```

### Index Performance
```bash
# Test covering index performance
python testrunner.py -c conf/tuq/py-covering-index.conf
```

## Integration Validation

### Cross-Service Integration
```bash
# FTS integration tests
python testrunner.py -c conf/tuq/n1ql_fts_integration_phase1_P1.conf

# Analytics integration
python testrunner.py -c conf/tuq/py-tuq-analytics.conf
```

## Advanced Testing Scenarios

### Edge Cases
```bash
# Large metadata handling
python testrunner.py -c conf/tuq/py-n1qlDGM.conf
```

### Stress Tests
```bash
# Concurrent operations
FROM adversary_config WHERE test_id = 'concurrent_query_tests'
```

## Test Results and Logs

### Check Test Results
```bash
# Test results are in tmp-<timestamp>/ directory
ls -lh tmp-*/

# View test logs
cat tmp-*/test.log

# View xunit XML results
cat tmp-*/xunit-results.xml
```

### Pass/Fail Summary
```bash
# Summary is in test runner output
# Look for lines like:
# "passed: 150, failed: 2, skipped: 5"
```

## Troubleshooting Commands

### Debug Mode
```bash
# Run tests with verbose logging
python testrunner.py -c conf/tuq/meta.conf -v

# Debug mode for specific test
python testrunner.py -t tuqquery.tuq_sanity.QuerySanityTests.test_meta -v
```

### Check Query Service Status
```bash
# Verify Query Service is running
curl http://localhost:8093/pools
```

### Check Index Service Status
```bash
# Verify GSI service is running
curl http://localhost:8091/pools/default
```

## Configuration Parameters Reference

### Common Parameters
```bash
# Skip index creation
-python testrunner.py -t <test> -p skip_index=True

# Set document count
python testrunner.py -t <test> -p doc-per-day=100

# Enable covering index
python testrunner.py -t <test> -p covering_index=True

# Run specific group
python testrunner.py -c <conf> -group=P0

# Multiple groups
python testrunner.py -c <conf> -group="P0;LIKE;DATE"
```

### Vector Search Parameters
```bash
# Set distance metric
python testrunner.py -t <test> -p distance=L2

# Set ANN nprobes
python testrunner.py -t <test> -p nprobes=10

# Set quantization description
python testrunner.py -t <test> -p description="IVF,SQ8"

# Set train list size
python testrunner.py -t <test> -p trainlist=50000
```

## Working Directory Reminder

**CRITICAL:** All test commands must be run from the testrunner directory:
```bash
cd testrunner
```

**Failure to do so will cause import errors** because the framework expects `lib/` and `pytests/` directories to be on `sys.path`.

## Test Execution Time Estimates

| Category | Estimated Time | Notes |
|----------|---------------|-------|
| Sanity tests | 10-20 minutes | Fast sanity checks |
| Extended sanity | 30-60 minutes | Comprehensive coverage |
| Vector search | 60-120 minutes | Large dataset loading |
| FTS integration | 30-45 minutes | Requires sample buckets |
| RBAC tests | 20-40 minutes | Complex permission setup |
| Collections tests | 25-35 minutes | Scope/collection operations |
| Window functions | 15-25 minutes | Analytical queries |

## Resource Requirements

### Typical Cluster Configuration
```ini
nodes_init=4
services_init=kv,n1ql,index
bucket_size=256MB
num_buckets=1
```

### Vector Search Requirements
```ini
nodes_init=4
services_init=kv,n1ql,index
bucket_size=1GB  # Larger for vector datasets
num_buckets=1
# Requires SIFT dataset installation
```

### FTS Integration Requirements
```ini
nodes_init=4
services_init=kv,n1ql,index,fts
bucket_size=256MB
num_buckets=2  # Multiple buckets for FTS
# Requires sample buckets: travel-sample, beer-sample
```

## Test Execution Checklist

Before running tests:
- [ ] Change to `testrunner` directory
- [ ] Ensure Couchbase cluster is running
- [ ] Verify Query Service is enabled (port 8093)
- [ ] Verify GSI service is enabled
- [ ] Check cluster has sufficient memory
- [ ] For vector tests: ensure SIFT dataset is available
- [ ] For FTS tests: load sample buckets
- [ ] Verify Python 3.10.13 is installed
- [ ] Verify dependencies: `pip3 list | grep "couchbase deepdiff numpy"`

After test completion:
- [ ] Check `tmp-<timestamp>/` for xunit XML and logs
- [ ] Verify pass/fail summary
- [ ] Review test.log for errors
- [ ] Check index status if index-related tests failed
- [ ] Clean up: check for orphaned buckets or indexes
