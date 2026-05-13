# AGENTS.md — lib/couchbase_helper/

## EncryptionAtRestHelper

**File:** `encryption_at_rest_helper.py`  
**Class:** `EncryptionAtRestHelper`  
**Instantiated as:** `self.encryption_helper` in `BaseSecondaryIndexingTests` (`pytests/gsi/base_gsi.py`)

Used by GSI encryption tests (`pytests/gsi/`) and query encryption tests
(`pytests/tuqquery/n1ql_encryption_at_rest.py`).

### Core detection

```python
get_file_header_text(node, file_path, bytes_to_read=512)
# → (header_read: bool, decoded: str, printable: str)
```
Runs `xxd -l <N> -p <path> 2>/dev/null` remotely, joins hex output, decodes
hex → bytes → ASCII (`errors='replace'`).

When xxd returns no output, diagnostics run (`ls -la`, `xxd -l 64 2>&1`, `file <path>`)
and `(False, "", "No output from xxd. ls:[...] | file:[...] | ...")` is returned.

**Encrypted file header format** (observed from live rlstream.* files):
```
\x00  +  "Couchbase Encrypted"  +  \x00  +  3 version bytes  +  \x00\x00\x00\x24  +  36-char UUID key ID
```
Total = 64 bytes exactly.  `bytes_to_read=64` is the minimum to capture the key ID.

`ENCRYPTION_MAGIC_BYTES = "Couchbase Encrypted"` — checked with `in actual`.

`xxd -p` outputs plain hex (no formatting); `cat` on the same file shows readable text
because terminals render ASCII bytes directly — both show identical data, just different
presentation.  `xxd` ships in `vim-common` (apt/yum) and may need installing on nodes.

### Query log methods

```python
verify_query_log_files_encrypted(query_nodes, expected_key_ids=None)
# → {node_ip: {"rlstream": {path: result}, "local_request_log": {path: result}}}
# result = {"encrypted": bool, "details": str, "key_id_found": bool}

verify_query_ffdc_files_encrypted(query_nodes, expected_key_ids=None)
# → {node_ip: {path: {"encrypted": bool, "details": str, "key_id_found": bool}}}

verify_query_log_files_decrypted(query_nodes)   # same shape, no key_ids
verify_query_ffdc_files_decrypted(query_nodes)  # same shape, no key_ids
```

**`expected_key_ids` is a flat list** — the same list is used for every node in
`query_nodes`.  When key IDs differ per node (which they do — each node has its own DEKs),
**call these methods one node at a time**:
```python
for node in query_nodes:
    results = self.encryption_helper.verify_query_log_files_encrypted(
        [node], per_node_key_ids_map.get(node.ip, [])
    )
```

FFDC files searched: `query_ffdc_MAN_areq*`, `query_ffdc_MAN_creq*`, `query_ffdc_MAN_vita*`.

### Log path

```python
get_log_path(node)  # → LINUX_COUCHBASE_LOGS_PATH or WIN_COUCHBASE_LOGS_PATH
# Linux: /opt/couchbase/var/lib/couchbase/logs
```
From `lib/testconstants`.

### GSI methods (not used by query encryption tests)

- `verify_gsi_storage_files_encrypted(index_nodes, gsi_type, expected_key_id)` — checks `@2i/` subtree
- `verify_gsi_snapshot_files_encrypted(index_nodes, expected_key_id, encrypted_bucket_names)` — checks `*.index/snapshot.*`
- `verify_gsi_indexer_stats_log_encrypted(index_nodes, expected_key_id)` — checks `stats.log`
- `verify_file_encryption_magic_bytes(node, file_path)` → `(is_encrypted: bool, details: str)`
- `verify_file_header_contains(node, file_path, expected_text)` → `(found: bool, details: str)`
- `verify_file_header_contains_any(node, file_path, expected_values)` → `(found: bool, matched/details: str)`
