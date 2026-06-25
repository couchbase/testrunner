# Encryption at Rest Tests

**File:** `n1ql_encryption_at_rest.py`  
**Class:** `QueryEncryptionAtRestTests(BaseSecondaryIndexingTests)`  
**Conf:** `conf/tuq/py-tuq-encryption-at-rest.conf`

## Inheritance chain
```
BaseTestCase → QueryTests (tuq.py) → BaseSecondaryIndexingTests (gsi/base_gsi.py)
    → QueryEncryptionAtRestTests
```
`BaseSecondaryIndexingTests` provides `get_nodes_from_services_map`, `gsi_util_obj`,
`prepare_collection_for_indexing`, `wait_until_indexes_online`, `encryption_helper`.

## Cluster topology
```
nodes_init=3
services_init=kv:n1ql:index-kv:n1ql:index-kv:n1ql:index
num_index_replica=1
skip_load=True        # prevents base class loading Employee/Person dataset
```

## Critical design: per-node DEK dict
`_get_query_in_use_key_ids()` returns `{node_ip: [key_id, ...]}` — **not a flat list**.
Each query node independently manages its own DEKs via `GET :8093/admin/encryption_at_rest`.
Always use `_build_node_key_ids_map()` to normalise, then call the encryption helper
one node at a time:
```python
node_key_ids_map = self._build_node_key_ids_map(query_nodes, expected_key_ids)
for node in query_nodes:
    self.encryption_helper.verify_query_log_files_encrypted(
        [node], node_key_ids_map.get(node.ip, [])
    )
```

## File types
| File | Created when | Validated by |
|------|-------------|-------------|
| `rlstream.*` | `completed-stream-size > 0` and queries run | `_assert_rlstream_files_encrypted` (immediate) |
| `local_request_log.*` | rlstream idle timeout or 100 MiB | `_assert_local_request_log_files_encrypted` (polls 120 s) |
| `query_ffdc_MAN_*` | `trigger_query_ffdc()` API call | `_trigger_ffdc_and_verify` (polls 180 s) |

## Key helpers
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

## Query admin settings
| Setting | Test value | tearDown default |
|---------|-----------|-----------------|
| `completed-stream-size` | 500 | 0 |
| `completed-threshold` | 0 | 1000 |

Applied and restored per-node via `self.rest.set_completed_stream_size(node, size)` and
`self.rest.set_completed_requests_collection_duration(node, threshold)`.

## Encryption setup (base class)
`BaseTestCase.suite_setUp()` sets log encryption when `enable_log_encryption_at_rest=True`.
`self.log_encryption_at_rest_id` = the KEK ID. Validated by `_verify_log_encryption_prerequisites()`.

## Key rotation test pattern
Pin `dekRotationInterval` high (60 000 s) before setup → baseline → reduce to 120 s →
poll `_wait_for_new_query_key_ids` → **immediately** re-pin to 60 000 s → re-run scans → validate.
Used by both `test_query_log_encryption_key_rotation` and `test_query_ffdc_encryption_key_rotation`.
