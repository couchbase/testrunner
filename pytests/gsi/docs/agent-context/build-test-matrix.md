# GSI Test Execution Matrix

## Smoke Tests

```bash
# Fastest GSI sanity (4-node cluster)
python3 testrunner.py -i b/resources/7-nodes-index-template.ini -c conf/gsi/py-gsi_sanity.conf

# Integration via make target
make test-gsi-integrations-tests PARAMS=<additional_params>
```

## Service-Specific GSI Configuration

**Required services in INI file for GSI tests:**
- `n1ql` (Query service)
- `index` (GSI index service)
- `kv` (Key-Value service)

**Common service patterns:**
```
4-node: services_init=kv:n1ql-kv-index-index
2 nodes: services_init=kv:n1ql:index-index
1 node:  services_init=kv:n1ql:index
```

## Category-Specific Commands

### Basic Index Operations

```bash
# Plasma/ForestDB tests
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/py-basicindexops.conf \\
    -p gsi_type=forestdb

# Memory-Optimized (MOI) tests
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/py-basicindexops.conf \\
    -p gsi_type=memory_optimized
```

### Collections Index Tests

```bash
# Collection index basics
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/collection-index-basics.conf

# Concurrent index creation
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/collection-concurrent-indexes.conf

# Index rebalance on collections
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/collection-rebalance-improvements.conf
```

### Rebalance Tests

```bash
# GSI rebalance
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/py-gsi-rebalance.conf

# File-based rebalance (newer approach)
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/gsi_file_based_rebalance.conf

# BHive index rebalance
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/bhive-index-rebalance.conf
```

### Recovery Tests

```bash
# Index recovery during rebalance
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/py-index_add_based_recovery.conf

# Recovery operations with disk flush (MOI snapshot testing)
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/py-index_add_based_recovery_plasma_dgm.conf

# Recovery during rebalance with plasma DGM
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/py-index_drop_based_recovery_plasma_dgm.conf

# Rollback recovery scenarios
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/py-gsi-rollback-recovery.conf

# Collections rollback recovery
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/collections-gsi-rollback-recovery.conf

# BHIVE rollback recovery
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/bhive-index-rollback-recovery.conf
```

### Restore Tests

```bash
# Full backup and restore using cbbackupmgr or REST API
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/backup_and_restore.conf

# BHIVE index backup/restore testing
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/bhive-index-backup-restore.conf
```

### Vector Index Tests

```bash
# Composite Vector Index suite (vector + scalar combined indexing)
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/vector-index.conf

# BHIVE Vector Index sanity (chaos/integration testing)
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/bhive-sanity.conf

# BHIVE E2E integration tests (indexer resilience, train_list validation)
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/bhive-e2e-tests.conf

# BHIVE chaos tests (kill indexer during phases, drop during graph build)
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/bhive-chaos.conf

# BHIVE rebalance tests (index behavior during rebalance, stats API)
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/bhive-index-rebalance.conf

# BHIVE backup/restore tests
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/bhive-index-backup-restore.conf

# BHIVE rollback recovery tests
python3 testrunner.py -i b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini \\
    -c conf/gsi/bhive-index-rollback-recovery.conf
```

### Upgrade Tests

```bash
# GSI upgrade
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/py-upgradegsi.conf

# Aggregate pushdown upgrade
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/py-upgrade_gsi_aggregate.conf

# INT64 upgrade
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/py-upgrade_gsi_int64.conf

# GSI upgrade on Capella
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/upgrade_gsi_on_capella.conf
```

## Index Partitioning

```bash
# Partitioned index tests
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/py-gsi-index-partitioning.conf \\
    -p partitioned_index=True

# Partial partitioned index
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/py-partial-partitioned.conf
```

## Replica Indexes

```bash
# Replica index creation and failover
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/py-gsi-replica-indexes.conf

# Alter index replicas
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/alter_index_replicas.conf
```

## Index Stats and Settings

```bash
# Index statistics
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/indexer_stats.conf

# Index configuration
python3 testrunner.py -i b/resources/7-nodes-index-template.ini \\
    -c conf/gsi/index_config_stats_gsi.conf
```

## Test Parameters

**Scan consistency levels:**
- `request_plus` - Consistent scan including request time
- `at_plus` - Consistent scan including a specific time
- `statement_plus` - Consistent scan within statement
- `not_bounded` - No scan consistency guarantees

**Common parameter combos:**
```bash
# Simple index operations
-p groups=simple,dataset=default,doc-per-day=20,gsi_type=forestdb

# Composite indexes
-p groups=composite,dataset=default,gsi_type=forestdb

# With document operations during test
-p doc_ops=True,create_ops_per=.5,delete_ops_per=.2,update_ops_per=.2

# Deferred builds
-p defer_build=True,build_index_after_create=True
```

## Validation Checklist

After running GSI tests:
1. Check `tmp-<timestamp>/report-<timestamp>.xml-*.xml` for xunit results
2. Verify for panics if any
3. Check for index errors or warnings in test logs
4. Confirm no stuck index builds
   - Check test logs for error from `_wait_for_index_online` method (in lib/couchbase_helper/tuq_helper.py)
   - Error message: `index <index_name> is not online. last response is {'metrics': {...}, 'requestID': ..., 'results': [...], 'status': 'success'}`
   - Search logs: `grep "index.*is not online" tmp-*/test.log`
