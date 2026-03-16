# GSI Test Suite Inventory

## Module Structure

**Total test files:** 65+ Python modules

| Category | Files | Examples | Largest files |
|----------|-------|----------|--------------|
| Basic operations | 5+ | `plasma_basic_ops.py`, `indexcreatedrop_gsi.py`, `indexscans_gsi.py` | `composite_vector_index.py` (292KB), `upgrade_gsi.py` (249KB) |
| Collections | 12+ | `collections_index_basics.py`, `collections_concurrent_indexes.py` | `collections_concurrent_indexes.py` (74KB), `collections_index_basics.py` (79KB) |
| Rebalance/Failover | 8+ | `gsi_rebalance_gsi.py`, `gsi_file_based_rebalance.py`, `gsi_autofailover.py` | `gsi_rebalance_gsi.py` (180KB), `gsi_file_based_rebalance.py` (129KB) |
| Recovery | 3 | `recovery_gsi.py`, `plasma_recovery.py` | `recovery_gsi.py` (94KB), `plasma_recovery.py` (30KB) |
| Restore | 1 | `backup_and_restore.py` | 156KB |
| Replicas/Partitioning | 6+ | `gsi_replica_indexes.py`, `gsi_alter_index_replicas.py`, `gsi_index_partitioning.py` | `gsi_replica_indexes.py` (167KB), `gsi_index_partitioning.py` (229KB) |
| Vector | 2 | `bhive_e2e_tests.py`, `composite_vector_index.py` | 292KB (composite), 120KB (BHIVE E2E) |
| Stats/Monitoring | 3+ | `indexer_stats.py`, `index_config_stats_gsi.py` | `indexer_stats.py` (54KB) |
| Upgrade | 4+ | `upgrade_gsi.py`, `int64_upgrade_gsi.py`, `upgrade_gsi_on_capella.py` | `upgrade_gsi.py` (249KB) |
| Enterprise/Security | 5+ | `gsi_free_tier.py`, `tenant_management.py`, `gsi_n2nencryption.py` | `tenant_management.py` (67KB) |

## Base Classes

| File | Size | Lines | Purpose |
|------|------|-------|---------|
| `base_gsi.py` | 248KB | 4813 | BaseSecondaryIndexingTests - core GSI test utilities |
| `newtuq.py` | 18KB | - | QueryTests - N1QL query base class |

## Configuration Files

**Location:** `conf/gsi/` (104+ files)

| Configuration category | Examples |
|------------------------|----------|
| Sanity suites | `py-gsi_sanity.conf`, `simple_gsi_n1ql.conf` |
| BHIVE Vector | `bhive-sanity.conf`, `bhive-e2e-tests.conf`, `bhive-index.conf`, `bhive-index-rebalance.conf`, `bhive-chaos.conf`, `bhive-index-backup-restore.conf` (10+ files) |
| Composite Vector | `vector-index.conf`, `vector-sanity.conf` |
| Rebalance | `gsi_rebalance_encryption.conf`, `bhive-index-rebalance.conf` |
| Recovery | `bhive-index-rollback-recovery.conf` |
| File-based rebalance | `gsi_file_based_rebalance.conf`, `gsi_file_based_rebalance_upgrade_test.conf` |
| Collections | `collection-index-basics.conf`, `collection-rebalance-improvements.conf` |

## Key Functions (base_gsi.py)

| Function | Purpose |
|----------|---------|
| `run_cbq_query` | Execute N1QL queries via n1ql_helper |
| `multi_create_index` | Batch create secondary indexes |
| `verify_index_drop` | Confirm index removal |
| `verify_index_build` | Wait for index to complete building |
| `run_query_with_covering_index` | Query with index covering validation |
| `get_index_using_rest` | Fetch index metadata via REST API |

## Storage Engines

| Type | Parameter | Notes |
|------|-----------|-------|
| Plasma (ForestDB) | `gsi_type=forestdb` | Default storage, persistent |
| Memory-Optimized | `gsi_type=memory_optimized` | In-memory storage |

## Common Test Parameters

| Parameter | Values | Purpose |
|-----------|--------|---------|
| `services_init` | `kv:n1ql-kv-index-index` | Cluster services layout |
| `nodes_init` | 1-4 | Number of nodes |
| `scan_consistency` | `request_plus`, `at_plus`, `statement_plus`, `not_bounded` | Query scan consistency level |
| `defer_build` | True/False | Defer index build until build command |
| `build_index_after_create` | True/False | Auto-build after CREATE INDEX |
| `groups` | `simple`, `composite`, `partitioned`, `range` | Query group patterns |

## Vector/AI Dependencies

| Package | Purpose |
|---------|---------|
| `faiss-cpu` | Facebook AI Similarity Search for vector indexing |
| `numpy` | Numerical operations for vector data |
| `sentence-transformers` | Generate embeddings via SentenceTransformer (used in bhive_e2e_tests.py) |
| `huggingface-hub` | Download pre-trained models for embeddings |
| `deepdiff` | Compare query results |

## Missing Areas

No tests found for:
- GSI compression (mentioned in conf)
- GSI three-pass planner (conf exists, but test unknown)

## Unknowns

- `gsi_file_based_rebalance.py` purpose and difference from standard rebalance

## Vector Index Types

| Type | Description | Test file | Configs |
|------|-------------|-----------|---------|
| **BHIVE Vector** | Integration/chaos testing for vector similarity search with IVF+SQ8 quantization, train_list validation, indexer resilience during phases | `bhive_e2e_tests.py` (120KB, BhiveVectorIndex class) | `conf/gsi/bhive-*.conf` (10+ files) |
| **Composite Vector** | Vector + scalar field combined indexing with PQ (product quantization) for enhanced filtering and partition elimination | `composite_vector_index.py` (292KB, CompositeVectorIndex class) | `conf/gsi/vector-*.conf` (2 files) |

**Key differences:**
- BHIVE: Chaos/integration focus, `with {"description": "IVF,SQ8", "train_list": 50000}`, parameter `bhive_index=True`
- Composite: Performance/scalability focus, `with {"quantization": "PQ32x8"}`, parameter `bhive_index=False` when comparing
