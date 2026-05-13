# EncryptionAtRestHelper

**File:** `encryption_at_rest_helper.py`  
**Class:** `EncryptionAtRestHelper`  
**Instantiated as:** `self.encryption_helper` in `BaseSecondaryIndexingTests` (`pytests/gsi/base_gsi.py`)

## Core detection
```python
get_file_header_text(node, file_path, bytes_to_read=512)
# → (header_read: bool, decoded: str, printable: str)
```
Runs `xxd -l <N> -p <path> 2>/dev/null` remotely, joins hex lines, decodes
hex → bytes → ASCII (`errors='replace'`).

When xxd returns no output: runs `ls -la`, `xxd -l 64 2>&1`, `file <path>` for
diagnostics and returns `(False, "", "No output from xxd. ls:[...] | ...")`.

**Encrypted file header format** (observed from live rlstream.* files):
```
\x00 + "Couchbase Encrypted" + \x00 + 3 version bytes + \x00\x00\x00\x24 + 36-char UUID
```
Total = 64 bytes. `bytes_to_read=64` is the minimum to capture the key ID.

`ENCRYPTION_MAGIC_BYTES = "Couchbase Encrypted"` — checked with `in actual`.

`xxd -p` = plain hex; `cat` shows readable text because terminals render ASCII bytes
directly — both show identical data. `xxd` ships in `vim-common` (apt/yum).

## Query log methods
```python
verify_query_log_files_encrypted(query_nodes, expected_key_ids=None)
# → {node_ip: {"rlstream": {path: result}, "local_request_log": {path: result}}}
# result = {"encrypted": bool, "details": str, "key_id_found": bool}

verify_query_ffdc_files_encrypted(query_nodes, expected_key_ids=None)
# → {node_ip: {path: {"encrypted": bool, "details": str, "key_id_found": bool}}}

verify_query_log_files_decrypted(query_nodes)   # same shape, no key_ids
verify_query_ffdc_files_decrypted(query_nodes)  # same shape, no key_ids
```

**`expected_key_ids` is a flat list** applied uniformly to every node. When key IDs
differ per node, **call one node at a time**:
```python
for node in query_nodes:
    results = self.encryption_helper.verify_query_log_files_encrypted(
        [node], per_node_key_ids_map.get(node.ip, [])
    )
```

FFDC files: `query_ffdc_MAN_areq*`, `query_ffdc_MAN_creq*`, `query_ffdc_MAN_vita*`.

## Log path
```python
get_log_path(node)  # Linux: /opt/couchbase/var/lib/couchbase/logs
```

## GSI methods (not used by query encryption tests)
- `verify_gsi_storage_files_encrypted(index_nodes, gsi_type, expected_key_id)`
- `verify_gsi_snapshot_files_encrypted(index_nodes, expected_key_id, encrypted_bucket_names)`
- `verify_gsi_indexer_stats_log_encrypted(index_nodes, expected_key_id)`
- `verify_file_encryption_magic_bytes(node, file_path)` → `(is_encrypted, details)`
- `verify_file_header_contains(node, file_path, text)` → `(found, details)`
- `verify_file_header_contains_any(node, file_path, values)` → `(found, matched/details)`
