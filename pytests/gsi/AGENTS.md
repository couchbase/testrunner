# AGENTS.md: GSI Test Suite

## Project Scope

Global Secondary Index (GSI) test suite for Couchbase Server. Tests secondary index creation, querying, rebalance, failover, upgrade, and vector indexing across different storage engines (Plasma/ForestDB/BHIVE and Memory-Optimized).

## Core Commands

```bash
# GSI integration tests (4-node cluster with n1ql+index services)
make test-gsi-integrations-tests PARAMS=<additional_params>

# GSI sanity test suite
python3 testrunner.py -i b/resources/7-nodes-index-template.ini -c conf/py-gsi_sanity.conf

# Vector index tests
python3 testrunner.py -i b/resources/7-nodes-index-template.ini -c conf/gsi/vector-index.conf

# File-based rebalance tests
python3 testrunner.py -i b/resources/7-nodes-index-template.ini -c conf/gsi/gsi_file_based_rebalance.conf
```

## Test Structure

**Inheritance hierarchy:**
- `pytests/basetestcase.py` -> `gsi/newtuq.py` -> `gsi/base_gsi.py` (248KB, 4813 lines) -> individual tests

**Test categories:**
| Category | Representative files | Purpose |
|----------|---------------------|---------|
| Basic operations | `plasma_basic_ops.py`, `indexcreatedrop_gsi.py`, `indexscans_gsi.py` | Create, drop, query indexes |
| Collections | `collections_index_basics.py`, `collections_concurrent_indexes.py`, `collections_index_scans.py` | Scopes and collections (Couchbase 6.5+) |
| Rebalance | `gsi_rebalance_gsi.py`, `collections_indexes_rebalance.py`, `gsi_file_based_rebalance.py` | Index behavior during cluster topology changes |
| Recovery | `recovery_gsi.py`, `plasma_recovery.py` | Index recovery from node failures, disk flush, rebalance, DGM |
| Restore | `backup_and_restore.py` | Full backup/restore using cbbackupmgr or REST API |
| Replicas | `gsi_replica_indexes.py`, `gsi_alter_index_replicas.py` | Index replicas and failover with replicas |
| Vector | `bhive_e2e_tests.py` (120KB), `composite_vector_index.py` (292KB) | BHIVE chaos/e2e, composite vector + scalar indexing |

**Configuration files:** `conf/gsi/` has 104+ test suite conf files (including `bhive-*.conf` for BHIVE chaos tests and `vector-*.conf` for composite vector indexes) |

## Development Patterns

**Index storage engines:**
- `gsi_type=forestdb` (Plasma storage - default)
- `gsi_type=memory_optimized` (MOI - in-memory)

**Common test parameters:**
- `services_init=kv:n1ql-kv-index-index` (4-node cluster with index services)
- `nodes_init=4` (typical node count)
- `scan_consistency=request_plus|at_plus|statement_plus|not_bounded`
- `defer_build=True`, `build_index_after_create=True`
- `groups=simple|composite|partitioned|range`

**Base class utilities (base_gsi.py):**
- `run_cbq_query` - N1QL execution
- `multi_create_index` - Bulk index creation
- `verify_index_drop` - Index cleanup validation
- Index replication and partitioning support

**Vector indexing:**
- **BHIVE Vector Indexes** (`bhive_e2e_tests.py` - 120KB, 2450 lines)
  - Integration and chaos testing for vector similarity search
  - Syntax: `CREATE VECTOR INDEX <name> ON <bucket>(<field> VECTOR) USING GSI WITH {"dimension": N, "similarity": "L2_SQUARED", "description": "IVF,SQ8", "train_list": N}`
  - Quantization: IVF + SQ8 (scalar quantization)
  - Test configs: `conf/gsi/bhive-*.conf` (10+ conf files: bhive-sanity.conf, bhive-e2e-tests.conf, bhive-index-rebalance.conf, bhive-chaos.conf, etc.)
  - Tests: indexer kill during phases, drop during graph build, rebalance statistics, backup API, crash recovery, train_list validation, large result sets, distance functions, replica repair, inline filtering
  - Parameters: `bhive_index=True`, `quantization_algo_*`, `trainlist`, `scan_nprobes`
- **Composite Vector Indexes** (`composite_vector_index.py` - 292KB)
  - Vector index combined with scalar fields for enhanced filtering
  - Syntax: Similar to BHIVE but with additional scalar fields
  - Quantization: PQ32x8, PQ128x8 (product quantization), SQ8
  - Test configs: `conf/gsi/vector-index.conf`, `vector-sanity.conf`
  - Tests: build scenarios, concurrent builds, partitioning, comparison between partitioned/non-partitioned, mutation workloads, expiry workloads, replica repair
- Required packages: `faiss-cpu`, `numpy`, `sentence-transformers` (for embeddings via SentenceTransformer), `deepdiff`

## Validation Requirements

Before completing GSI test changes:
1. Run `make test-gsi-integrations-tests` for integration validation
2. Run `conf/gsi/py-gsi_sanity.conf` for smoke tests
3. Check `tmp-<timestamp>/` for xunit reports and logs
4. Verify index builds complete (`verify_using_index_status=True`)
5. For rebalance/failover changes, run `conf/gsi/gsi_rebalance_gsi.conf`

**Code quality:**
- Inherit from `BaseSecondaryIndexingTests`
- Use existing index creation/query patterns in `base_gsi.py`
- Follow parameter naming: `gsi_type`, `services_init`, `nodes_init`

## Dependencies

**Python modules (from base_gsi.py imports):**
```python
# Standard
import json, math, random, re, threading, time, base64, subprocess

# Couchbase SDK and framework
from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition

# ML/Vector search
import faiss
import numpy as np
import requests
from deepdiff import DeepDiff

# Collections
from collections_rest_client import CollectionsRest
from collections_collections_cli_client import CollectionsCLI
```

**Git submodules:**
- `gsi_utils` - GSI utilities

## Security Considerations

- Tests may expose index metadata and query patterns
- No sensitive credentials in test code
- REST API credentials from INI files (use templates)

## Unknowns

- `gsi_file_based_rebalance.py` - recently modified, purpose unclear
- MOI vs Plasma behavior differences not documented
- GSI upgrade testing edge cases

## Supporting Context

- `docs/agent-context/repo-inventory.md` - GSI module inventory
- `docs/agent-context/build-test-matrix.md` - GSI test execution patterns
- `docs/agent-context/domain-glossary.md` - GSI terminology (replicas, partitions, storage types)
- `docs/agent-context/troubleshooting.md` - GSI-specific failures
- Parent context: `/Users/pavan.pb/Workstuff/testrunner/AGENTS.md`
