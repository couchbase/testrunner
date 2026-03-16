# GSI Domain Glossary

## Core Concepts

| Term | Description |
|------|-------------|
| **GSI** (Global Secondary Index) | Couchbase secondary index service that backs N1QL queries |
| **GSI Type (Storage Engine)** | Storage backend for indexes: `forestdb` (Plasma, persistent) or `memory_optimized` (MOI, in-memory) |
| **Primary Index** | Required index for all queries; covers all document IDs |
| **Secondary Index** | User-defined index on document fields for query optimization |
| **Covering Index** | Index that includes all fields used in SELECT, WHERE, ORDER BY |
| **Index Replica** | Redundant copy of an index for high availability (replica indexes) |
| **Partitioned Index** | Index split across multiple partitions for scalability |

## Scan Consistency

| Level | Description |
|-------|-------------|
| `request_plus` | Guarantees consistency with all mutations up to the request time |
| `at_plus` | Guarantees consistency with mutations up to a specified point in time |
| `not_bounded` | No consistency guarantees; fastest performance |

## Index Operation Modes

| Mode | Description |
|------|-------------|
| **Deferred Build** | Create index without immediate data population; build later with BUILD INDEX |
| **Immediate Build** | Create and build index in one operation |
| **Async Build** | Index builds asynchronously in background |
| **Sync Build** | Index builds synchronously, blocking until complete |

## Test Categories

| Category | Purpose | Examples |
|----------|---------|----------|
| **Basic Operations** | Create, drop, query indexes | `plasma_basic_ops.py`, `indexcreatedrop_gsi.py` |
| **Scan Consistency** | Test different scan consistency levels | `indexscans_gsi.py` |
| **Rebalance** | Index behavior during cluster topology changes | `gsi_rebalance_gsi.py` |
| **Failover** | Index behavior during automatic failover | `gsi_autofailover.py` |
| **Recovery** | Index recovery from node failures, disk flush, rebalance operations, DGM scenarios | `recovery_gsi.py` (CollectionsSecondaryIndexingRecoveryTests), `plasma_recovery.py` (SecondaryIndexingPlasmaDGMRecoveryTests) |
| **Restore** | Full backup and restore of indexes using cbbackupmgr or REST API | `backup_and_restore.py` (BackupRestoreTests) |
| **Replica** | Test index replicas and failover with replicas | `gsi_replica_indexes.py` |
| **Collections** | Scopes and collections (Couchbase 6.5+) | `collections_*.py` |
| **Vector** | Vector similarity search indexes | `bhive_e2e_tests.py`, `composite_vector_index.py` |

## Vector Indexing

| Term | Description |
|------|-------------|
| **BHIVE Vector Index** | Integration and chaos testing framework for vector similarity search indexes; tests indexer resilience, train_list validation, and recovery scenarios |
| **Composite Vector Index** | Vector index combined with scalar fields for enhanced filtering and query performance |
| **FAISS** | Facebook AI Similarity Search library used for vector operations |
| **Embedding** | Numerical representation of data (documents, images) for similarity |
| **IVF (Inverted File Index)** | Vector quantization algorithm that partitions vectors into clusters (centroids) for faster search |
| **SQ8 (Scalar Quantization)** | 8-bit scalar quantization that compresses vector data to reduce memory |
| **PQ (Product Quantization)** | Compression technique that splits vectors into sub-vectors and quantizes each separately (PQ32x8, PQ128x8) |
| **train_list** | Number of documents/centroids to train IVF quantization model (e.g., 50000 for 100k documents) |
| **dimension** | Vector embedding dimension (e.g., 128 for small models, 4096 for large models) |
| **similarity** | Distance metric for vector comparison: L2_SQUARED, COSINE, DOT, EUCLIDEAN_SQUARED, L2 |

## SYNTAX EXAMPLES (SQL):

```sql
-- BHIVE Vector Index (chaos/integration testing)
CREATE VECTOR INDEX idx_name ON bucket(vec_field VECTOR) USING GSI
  WITH {"dimension": 128, "similarity": "L2_SQUARED",
         "description": "IVF,SQ8", "train_list": 50000}

-- Composite Vector Index (vector + scalar fields)
CREATE VECTOR INDEX idx_name ON bucket(scalar_field, vec_field VECTOR) USING GSI
  WITH {"dimension": 128, "similarity": "L2_SQUARED"}
```

## BHIVE Test Methods

| Test Method | Description |
|-------------|-------------|
| `test_kill_indexer_process_at_different_stages` | Kill indexer during sampling, training, building, or graph building phases; validate recovery |
| `test_index_drop_during_graph_build` | Drop index while in "Graph build" phase |
| `test_drop_multiple_indexes_in_training_state` | Test concurrent drops during training |
| `test_index_statistics_during_rebalance` | Poll index statistics API during rebalance operations |
| `test_backup_api_during_indexer_crash` | Test backup API endpoint after indexer termination |
| `test_measure_query_latencies_before_after_crash` | Measure query performance before and after indexer crash |
| `test_train_list_threshold_validation` | Validate train_list > total documents fails appropriately |
| `test_query_performance_with_different_similarity_metrics` | Test COSINE, L2_SQUARED, L2, DOT metrics |
| `test_index_metadata_statistics` | Validate index metadata and statistics endpoints |
| `test_bulk_create_drop_indexes` | Test batch index create/drop operations |

## BHIVE-Specific Test Parameters

| Parameter | Values | Purpose |
|-----------|--------|---------|
| `bhive_index` | True/False | Enable BHIVE vector index tests (vs composite) |
| `quantization_algo_description_vector` | PQ32x8, PQ128x8, SQ8 | Quantization algorithm for description embeddings |
| `quantization_algo_color_vector` | SQ8 | Quantization algorithm for scalar/vector colors |
| `trainlist|train_list` | 50000, 10000, 1000 | Number of documents for IVF training |
| `dimension` | 128, 4096 | Vector embedding dimensions |
| `scan_nprobes` | 25 | Number of probes during vector scan (IVF) |
| `json_template` | Cars, Hotel | Document template for test data |
| `vector_backup_filename` | backup_zips/100K_car.zip | Pre-loaded vector data backup file |

## Recovery Modes

| Mode | Description | Test file | Use cases |
|------|-------------|-----------|----------|
| **Add-Based Recovery** | Recover indexes by re-adding missing indexes after failure | `py-index_add_based_recovery.conf` | Index loss during rebalance_in |
| **Drop-Based Recovery** | Recover by dropping and recreating corrupted indexes | `py-index_drop_based_recovery.conf` | Corrupted index state |
| **Query-Based Recovery** | Validate index completeness and rebuild missing indexes | `py-index_query_based_recovery.conf` | Index query failures |
| **Rollback Recovery** | Recover index state after cluster rollback operation | `py-gsi-rollback-recovery.conf`, `collections-gsi-rollback-recovery.conf`, `bhive-index-rollback-recovery.conf` | Cluster rollback scenarios |
| **DGM Recovery** | Index recovery in Data Growth Monitor (out-of-memory) scenarios with plasma storage | `py-index_*_recovery_plasma_dgm.conf` | Memory pressure with plasma |

**Recovery vs Restore:**
- **Recovery**: Self-healing capabilities of the indexer service to recover from transient failures (rebalance, flush, DGM, rollback) without external backup; tests validate indexer resilience
- **Restore**: Full backup and restore operations using cbbackupmgr or REST API to save/completey restore index state; tests backup/restore tools and data integrity

## Restore Operations

| Operation | Description | Test file |
|-----------|-------------|-----------|
| **Full Backup** | Create complete backup of GSI indexes using cbbackupmgr or REST API | `backup_and_restore.conf` |
| **Full Restore** | Restore GSI indexes from backup to recover cluster state | `backup_and_restore.conf` |
| **Index Backup API** | Test backup API endpoints (e.g., `/api/v1/bucket/{bucket}/backup`) during indexer unavailability | `bhive-index-backup-restore.conf` |

## Special Parameters

| Parameter | Description |
|-----------|-------------|
| `defer_build` | Create index without immediate data population |
| `build_index_after_create` | Auto-build index after CREATE INDEX if True |
| `verify_using_index_status` | Verify index build by polling index status endpoint |
| `timeout_for_index_online` | Maximum time to wait for index to go online |
| `use_replica_when_active_down` | Use replica index when active index is unavailable |
| `use_where_clause_in_index` | Include WHERE clause in index definition |

## Acronyms

| Acronym | Meaning |
|---------|---------|
| **GSI** | Global Secondary Index |
| **N1QL** | Non-first Normal Form Query Language (Couchbase SQL) |
| **MOI** | Memory Optimized Index (in-memory storage) |
| **DGM** | Data Greater than memory (database growth tracking) |
| **FTS** | Full Text Search (can use GSI indexes) |
| **E2E** | End-to-End testing |
