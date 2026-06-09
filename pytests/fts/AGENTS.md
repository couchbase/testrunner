# AGENTS.md — `pytests/fts`

## Module Overview

This directory contains the **Full-Text Search (FTS / Couchbase Search)** integration test suite for Couchbase Server. It validates the entire FTS service lifecycle — index management, query execution, vector search (kNN), N1QL Flex integration, RBAC, backup/restore, topology changes, upgrades, serverless tier limits, geo-spatial queries, synonyms, and disk compaction — against on-premises clusters, `cluster_run` sandboxes, and cloud (Capella/serverless) deployments.

---

## Directory Layout

```
pytests/fts/
├── fts_base.py                         # Core base class + FTS model classes
├── stable_topology_fts.py              # Comprehensive FTS tests on a stable cluster
├── stable_topology_extended_fts.py     # Extended: IP/CIDR and geo-spatial queries
├── moving_topology_fts.py              # FTS tests during rebalance / topology changes
├── fts_vector_search.py                # Vector search (kNN) with FAISS validation
├── fts_fastmerge_vector_search.py      # FastMerge index optimisation for vector search
├── fts_bq_vector_search.py             # Binary Quantization (BQ) vector search
├── vector_moving_topology_fts.py       # Vector search during rebalance
├── using_fts.py                        # N1QL USING FTS clause integration
├── fts_flex_features.py                # N1QL Flex queries with FTS/GSI pushdown
├── rbac_fts.py                         # RBAC and user-permission tests
├── fts_backup_restore.py               # FTS index backup, restore, and remap
├── upgrade_fts.py                      # Offline upgrade and version compatibility
├── fts_pause_resume.py                 # Pause / resume indexing operations
├── fts_server_groups.py                # Server groups and partition topology
├── fts_free_tier_limits.py             # Serverless free-tier quota enforcement
├── fts_reclaimable_disk_space.py       # Compaction and disk-space reclamation
├── tenant_management_fts.py            # Serverless multi-tenant limits
├── fts_callable.py                     # FTSCallable utility (non-TestCase helper)
└── __init__.py
```

---

## Class Hierarchy

```
BaseTestCase  (lib/)
└── FTSBaseTest                              # fts_base.py — all FTS tests inherit from here
    ├── StableTopFTS                         # stable_topology_fts.py
    │   └── StableTopExtendedFTS             # stable_topology_extended_fts.py
    ├── MovingTopFTS                         # moving_topology_fts.py
    ├── VectorSearch                         # fts_vector_search.py
    │   ├── FastMergeVectorSearch            # fts_fastmerge_vector_search.py
    │   └── BQVectorSearch                   # fts_bq_vector_search.py
    ├── VectorSearchMovingTopFTS             # vector_moving_topology_fts.py
    ├── USINGFTS                             # using_fts.py
    ├── FlexFeaturesFTS                      # fts_flex_features.py
    ├── RbacFTS                              # rbac_fts.py
    ├── BackupRestore                        # fts_backup_restore.py
    ├── PauseResume                          # fts_pause_resume.py
    ├── FTSServerGroups                      # fts_server_groups.py
    ├── FtsFreeTierLimits                    # fts_free_tier_limits.py
    ├── FTSReclaimableDiskSpace              # fts_reclaimable_disk_space.py
    └── TenantManagementFTS                  # tenant_management_fts.py

NewUpgradeBaseTest  (pytests/)
└── UpgradeFTS                               # upgrade_fts.py
    FTSServerGroups also inherits NewUpgradeBaseTest

Utility (not a TestCase):
    FTSCallable                              # fts_callable.py — callable helper for non-test contexts
```

---

## Key Files and Responsibilities

### `fts_base.py`
- **Purpose:** Central base class and FTS domain model. Every test class inherits from `FTSBaseTest`.
- **Key model classes:**
  | Class | Description |
  |-------|-------------|
  | `FTSIndex` | Represents an FTS index — definition, source bucket/scope/collection, type mappings, analyzers |
  | `CouchbaseCluster` | Wraps REST/SSH access to a Couchbase cluster node |
  | `INDEX_DEFAULTS` | Default FTS index parameters (partition count, replicas, etc.) |
  | `FTSException` | Domain exception for FTS operation failures |
- **Key base helpers:**
  - `create_fts_index()` / `delete_fts_index()` — index CRUD via REST
  - `run_query()` / `run_n1ql_query()` — execute FTS / N1QL queries
  - `load_data()` — load documents into source bucket
  - `wait_for_indexing_complete()` — polls index doc count until stable
  - `validate_query_results()` — compares FTS results against expected output
  - `get_fts_node()` / `get_fts_rest_client()` — locate the FTS service node

### `stable_topology_fts.py`
- **Purpose:** Comprehensive FTS query tests on a cluster that does not change topology during the test.
- **Key class:** `StableTopFTS`
- **Key test areas:**
  - Match, phrase, fuzzy, prefix, wildcard, boolean, regex, numeric range, date range queries
  - Custom analyzers and tokenizers
  - Synonym search
  - Multi-field and nested field mappings
  - Default and custom type mappings
  - Collections-scoped indexes
  - Index aliases
  - Concurrent query load

### `stable_topology_extended_fts.py`
- **Purpose:** Extended topology-stable tests for advanced query types.
- **Key class:** `StableTopExtendedFTS` (inherits `StableTopFTS`)
- **Key test areas:**
  - IP address (CIDR) queries
  - Geo-spatial point-in-radius and bounding-box queries
  - Geo-polygon queries

### `moving_topology_fts.py`
- **Purpose:** FTS correctness under cluster topology changes.
- **Key class:** `MovingTopFTS`
- **Key test areas:**
  - Rebalance-in / rebalance-out of FTS nodes
  - Swap rebalance
  - Failover and recovery
  - Index partition redistribution
  - Query correctness during rebalance

### `fts_vector_search.py`
- **Purpose:** Vector search (approximate nearest-neighbour / kNN) tests.
- **Key class:** `VectorSearch`
- **Key test areas:**
  - kNN query execution with Euclidean, dot-product, and cosine similarity metrics
  - FAISS-based ground-truth validation of recall and precision
  - Vector index creation with dimension and similarity settings
  - Hybrid text + vector queries
  - GPU-accelerated index building
  - Pre-filtering with FTS queries on vector indexes

### `fts_fastmerge_vector_search.py`
- **Purpose:** Tests the FastMerge index optimisation path for vector indexes.
- **Key class:** `FastMergeVectorSearch` (inherits `VectorSearch`)
- **Key test areas:** Index build time, merge policy configuration, recall under FastMerge

### `fts_bq_vector_search.py`
- **Purpose:** Binary Quantization (BQ) compression for vector indexes.
- **Key class:** `BQVectorSearch` (inherits `VectorSearch`)
- **Key test areas:** BQ-compressed index creation, recall vs. full-precision comparison, memory footprint

### `vector_moving_topology_fts.py`
- **Purpose:** Vector search correctness during cluster topology changes.
- **Key class:** `VectorSearchMovingTopFTS`
- **Key test areas:** kNN recall during rebalance, failover, and node addition

### `using_fts.py`
- **Purpose:** Tests the N1QL `USING FTS` clause that routes N1QL queries through the FTS engine.
- **Key class:** `USINGFTS`
- **Key test areas:**
  - `SELECT ... WHERE ... USING FTS` query execution
  - Result consistency between FTS and N1QL engines
  - Index selection when multiple FTS indexes exist

### `fts_flex_features.py`
- **Purpose:** N1QL Flex index queries — automatic pushdown of N1QL predicates to FTS or GSI.
- **Key class:** `FlexFeaturesFTS`
- **Key test areas:**
  - Flex query pushdown to FTS vs. GSI selection
  - Mixed FTS+GSI query plans
  - Flex index creation and covering queries

### `rbac_fts.py`
- **Purpose:** RBAC and security validation for FTS operations.
- **Key class:** `RbacFTS`
- **Key test areas:**
  - FTS Admin, FTS Searcher role enforcement
  - Bucket-level vs. cluster-level permission scoping
  - Unauthorized access rejection
  - Collections-scoped RBAC

### `fts_backup_restore.py`
- **Purpose:** Backup and restore of FTS index definitions.
- **Key class:** `BackupRestore`
- **Key test areas:**
  - Index definition included in cbbackupmgr backups
  - Restore with same and remapped bucket names
  - Index state after restore (active, ready-to-query)

### `upgrade_fts.py`
- **Purpose:** FTS service behaviour across Couchbase version upgrades.
- **Key class:** `UpgradeFTS` (inherits `NewUpgradeBaseTest`)
- **Key test areas:**
  - Offline upgrade with existing FTS indexes
  - Index availability post-upgrade
  - Query results consistency before and after upgrade
  - Vector index compatibility across versions

### `fts_pause_resume.py`
- **Purpose:** Pause and resume FTS indexing operations.
- **Key class:** `PauseResume`
- **Key test areas:**
  - Pause indexing via REST API
  - Resume indexing and verify doc count catches up
  - Query behaviour while indexing is paused

### `fts_server_groups.py`
- **Purpose:** FTS partition placement in server groups (rack/zone awareness).
- **Key class:** `FTSServerGroups` (inherits `FTSBaseTest`, `NewUpgradeBaseTest`)
- **Key test areas:**
  - Server group topology setup
  - FTS partition replica placement across groups
  - Rebalance with server groups

### `fts_free_tier_limits.py`
- **Purpose:** Serverless free-tier FTS quota and limit enforcement.
- **Key class:** `FtsFreeTierLimits`
- **Key test areas:**
  - Max index count per database enforcement
  - Query throttling at quota limits
  - Error messaging on limit breach

### `fts_reclaimable_disk_space.py`
- **Purpose:** Disk space compaction and reclamation for FTS indexes.
- **Key class:** `FTSReclaimableDiskSpace`
- **Key test areas:**
  - Compaction API trigger and completion
  - Reclaimable bytes metric before/after compaction
  - Disk usage reduction validation

### `tenant_management_fts.py`
- **Purpose:** Serverless multi-tenant FTS limit management.
- **Key class:** `TenantManagementFTS`
- **Key test areas:**
  - Per-tenant index quotas
  - Cross-tenant isolation
  - Admin override of tenant limits

### `fts_callable.py`
- **Purpose:** `FTSCallable` — a non-TestCase utility class for invoking FTS operations from other test modules (e.g., upgrade or backup tests that need to verify FTS as part of a broader scenario).

---

## Configuration Files

All FTS conf files live under `conf/fts/`:

| Conf File | Description |
|-----------|-------------|
| `py-fts-simpletopology.conf` | Default stable-topology FTS tests (used by `make test-fts`) |
| `py-fts-simpletopology-multicollections.conf` | Collections-scoped FTS tests |
| `py-fts-movingtopology.conf` | Rebalance / topology-change FTS tests |
| `py-fts-vector-search-movingtopology.conf` | Vector search during topology changes |
| `py-fts-fastmerge-vector-search.conf` | FastMerge vector index tests |
| `py-fts-flex_index.conf` | N1QL Flex index pushdown tests |
| `py-fts-backup-restore.conf` | FTS index backup/restore tests |
| `py-fts-vector-upgrade.conf` | Vector index upgrade compatibility |
| `py-fts-rbac.conf` | RBAC permission tests |
| `py-fts-geo_spatial.conf` | Geo-spatial query tests |
| `py-fts-geo_polygon.conf` | Geo-polygon query tests |
| `py-fts-synonym-search.conf` | Synonym analyser tests |
| `py-fts-scan-plus.conf` | Scan + FTS hybrid tests |
| `py-fts-index-management.conf` | Index CRUD management tests |
| `py-fts-defmap-rqg-queries.conf` | Default mapping RQG-driven queries |
| `py-fts-custmap-rqg-queries.conf` | Custom mapping RQG-driven queries |
| `py-fts-vector-custmap-rqg-queries.conf` | Vector custom-mapping RQG queries |
| `py-fts-compaction-api.conf` | Compaction API and disk reclamation |

---

## Running Tests

```bash
# Default FTS suite — stable topology (4-node, fts service, 1000 MB FTS quota)
make test-fts

# Single test via testrunner CLI
./testrunner -i b/resources/4-nodes-template.ini \
  -t pytests.fts.stable_topology_fts.StableTopFTS.test_query_type_match_phrases

# Vector search tests
./testrunner -i b/resources/4-nodes-template.ini \
  -t pytests.fts.fts_vector_search.VectorSearch.test_vector_search_sanity

# Moving-topology tests
./testrunner -i b/resources/4-nodes-template.ini \
  -t pytests.fts.moving_topology_fts.MovingTopFTS.test_rebalance_in_during_index_building

# Flex index tests
./testrunner -i b/resources/4-nodes-template.ini \
  -t pytests.fts.fts_flex_features.FlexFeaturesFTS.test_flex_pushdown_fts_vs_gsi

# Full conf suite
./testrunner -i b/resources/4-nodes-template.ini -c conf/fts/py-fts-simpletopology.conf
```

---

## Key Dependencies

| Dependency | Source |
|-----------|--------|
| `FTSBaseTest` / `BaseTestCase` | `lib/basetestcase.py` |
| `NewUpgradeBaseTest` | `pytests/newupgradebasetest.py` |
| `RestConnection` | `lib/membase/api/rest_client.py` |
| `RemoteMachineShellConnection` | `lib/remote/remote_util.py` |
| `BlobGenerator` / `DocumentGenerator` | `lib/couchbase_helper/documentgenerator.py` |
| `FTSCallable` | `pytests/fts/fts_callable.py` |
| `SearchServiceEvents` | `lib/SystemEventLogLib/fts_service_events.py` |
| `faiss` | Vector ground-truth validation (optional, installed separately) |
| `numpy` | Vector data generation and recall computation |

---

## Couchbase Features Covered

| Feature | Coverage |
|---------|----------|
| FTS Index CRUD | Create, update, clone, delete indexes |
| Type Mappings | Default mapping, custom field/type mappings |
| Analyzers & Tokenizers | Standard, keyword, custom analyzers, synonym tokens |
| Query Types | Match, phrase, fuzzy, prefix, wildcard, bool, regex, numeric range, date range, geo, IP/CIDR |
| Geo-spatial Queries | Point-in-radius, bounding box, polygon |
| Vector Search (kNN) | kNN with Euclidean / dot-product / cosine, recall validation via FAISS |
| FastMerge | Vector index FastMerge optimisation |
| Binary Quantization | BQ-compressed vector indexes |
| Hybrid Search | Combined text + vector queries |
| N1QL USING FTS | N1QL queries routed through FTS engine |
| N1QL Flex Indexes | Automatic FTS/GSI pushdown from N1QL |
| Index Aliases | Multi-index query aliases |
| Collections | Scope/collection-scoped FTS indexes |
| RBAC | FTS Admin, FTS Searcher roles, bucket/collection scoping |
| Backup / Restore | Index definition backup and restore with remapping |
| Rebalance | Index partition redistribution, query correctness during rebalance |
| Failover | FTS node failover and recovery |
| Pause / Resume | Indexing control via REST API |
| Server Groups | Rack-zone aware partition placement |
| Upgrade | Offline upgrade with index compatibility checks |
| Serverless Limits | Free-tier quota, tenant isolation, throttling |
| Compaction | Disk space reclamation via compaction API |
| System Events | FTS service event log validation |

---

## Development Guidelines

1. **Always inherit from `FTSBaseTest`** for new FTS test classes; never instantiate `RestConnection` for FTS operations directly — use `fts_base.py` helpers.
2. **For vector tests**, subclass `VectorSearch` rather than `FTSBaseTest` directly to inherit kNN query helpers and FAISS recall validation.
3. **For upgrade tests**, subclass `NewUpgradeBaseTest` (as `UpgradeFTS` does) and use `FTSCallable` to invoke FTS operations from within upgrade hooks.
4. **Use `wait_for_indexing_complete()`** before running queries in any test — FTS indexes build asynchronously.
5. **Validation:** Use `validate_query_results()` from `FTSBaseTest` for result assertions rather than manual JSON diffing.
6. **Conf coverage:** Register new test methods in the appropriate `conf/fts/*.conf` file.
7. **Python 3.10+:** All code must be Python 3.10 compatible.
8. **Cleanup:** Always delete FTS indexes in `tearDown`; leaked indexes can pollute subsequent tests sharing the same cluster.

---

## Security Notes

- RBAC tests (`rbac_fts.py`) require a cluster with at least one non-admin user configured — use the INI `[membase]` credentials section.
- Never log raw query responses that may contain PII from test documents.
- Cloud / Capella tests require credentials injected via environment variables or INI `[cloud]` section — do **not** hardcode tokens.

