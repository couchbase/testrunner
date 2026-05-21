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

## GSI methods (orchestrated by `base_gsi.validate_file_encryption`)

Header-mode (magic bytes + key ID in first 64 bytes):
- `verify_gsi_snapshot_files_encrypted(index_nodes, expected_key_id, encrypted_bucket_names)`
- `verify_gsi_error_files_encrypted(index_nodes, expected_key_id)` — only present on induced corruption; skipped otherwise
- `verify_gsi_indexer_stats_log_encrypted(index_nodes, expected_key_id)` — `indexer_stats.log` (+ `.enc` variant)
- `verify_gsi_projector_stats_log_encrypted(index_nodes, expected_key_id)` — `projector_stats.log` (+ `.enc` variant)
- `verify_gsi_request_handler_cache_encrypted(index_nodes, expected_key_id)` — files under `@2i/cache/{stats,meta}`

Body-grep mode (key ID lives in file body, NOT header):
- `verify_file_contains_key_id(node, file_path, key_ids)` → `(found, matched_key_id, details)` — uses `grep -aFl`
- `verify_gsi_data_files_key_id(index_nodes, expected_key_id, encrypted_bucket_names)` — `@2i/*.index/{docIndex,mainIndex,keyindex}/*.data`

TODO:
- `verify_gsi_metadata_repo_encrypted(index_nodes, expected_key_id)` — stub returning `status=todo`; spec pending for `metadata_repo_v2/*.wal` and `*.sstable`.

Per-bucket key map (used by snapshot / data_files / error_files / storage_stats_rest):
- `build_bucket_key_map(rest, bucket_names)` → `{bucket_name: {"uuid": str, "key_ids": [str]}}`. Combines `/pools/default/buckets/<bucket>` (UUID) with `<indexer>:9102/encryption/GetInUseKeys` (parses `{service_bucket <uuid>}` → key list).
- **Path layout (current):** `@2i/<bucket-uuid>_<index-def-id>_<partition-id>.index/` and `@2i/@bhive/<bucket-uuid>_..._.index/`. The bucket *name* never appears on disk — only the UUID does. Path-side attribution therefore matches `/<uuid>_`, not `/<name>_`.
- Verifiers attribute each file via `_bucket_for_path(path, bucket_key_map)` (looks for `/<uuid>_` substring) and accept only that bucket's `key_ids`. Falls back to flat `expected_key_id` when the map is empty or a file's UUID isn't in the map.
- `_bucket_uuid_prefix_filter(paths, bucket_key_map)` filters paths to only those whose UUID appears in the map; used for `encrypted_bucket_names`-based scoping.

REST-based (no SSH):
- `verify_storage_stats_rest(index_nodes, expected_key_id, encrypted_bucket_names, bucket_key_map)` — calls `<indexer>/stats/storage` per node. Walks **MainStore** and **BackStore** per index independently (the encryption fields live inside each store, not at `Stats` top level).
- `snapshot_storage_stats_counters(index_nodes)` → `{node_ip: {label: {field: int}}}`. Captures the four crypt-byte counters (`lss_blk_written_crypt`, `recovery_lss_blk_written_crypt`, `lss_blk_read_bs_crypt`, `recovery_lss_blk_read_bs_crypt`) per `<bucket>:<index>/<store>` label.
- `assert_storage_stats_counters_delta(before, after, mode)` — diffs two snapshots. `mode="must_increase"` for encryption-enabled, `"must_not_increase"` for encryption-disabled / upgrade-not-yet-done.

Orchestration / failure aggregation:
- `validate_file_encryption(..., fail_on_error=True)` in `pytests/gsi/base_gsi.py` wraps every verifier in `_safe_run` — a verifier crash is captured as a synthetic failure for that category, the run continues, and a single `AssertionError` with a multi-line consolidated summary is raised AFTER every category has executed. Pass `fail_on_error=False` to make it return-only.
- `results["_overall"] = {"failed": bool, "passed_summary": [str, ...], "failure_summary": [str, ...]}` is the post-run aggregate.
- `_build_encryption_summary_lines(results)` produces two high-level lists — one PASSED, one FAILED. Each entry is `"[category] node=IP status=X passed=P/T failed=F"` (counts only — no file paths or per-entry details, those stay in the per-file `warning`/`error` logs the verifiers emit during the run). Soft-fail counts (e.g. `partially_encrypted` stores) are tagged on the passed line so they remain visible without inflating the failure list.
- `_encryption_node_counts(result)` reduces a single node-result dict to `(passed, failed, total)` by sniffing whichever counter the verifier exposes (`passed_count`/`encrypted_count`, `failed_files`/`failed_stores`/`failed_indexes`/`violations`, `total_checked`/`checked_stores`, plaintext-leakage's `total_leaks` + `files_checked`, stats-log single-file shape).

Logging conventions (mirrors `n1ql_encryption_at_rest.py`):
- `RemoteMachineShellConnection(node, verbose=False)` everywhere.
- `RestConnection.query_tool(..., verbose=False)` for N1QL sampling.
- Per-file PASS logs are `self.log.debug` (not info) — only failures/warnings stay at `warning`/`error`. Bucket-key map and plaintext-sample tokens are debug-only (they carry secrets).

Plaintext leakage:
- `collect_plaintext_samples_via_n1ql(query_node, bucket_names, sample_size=10)` — `SELECT * LIMIT N` per bucket, recursively extract string values, drop tokens shorter than 6 chars, cap at 25 tokens.
- `verify_no_plaintext_in_encrypted_files(index_nodes, plaintext_samples, categories=None, encrypted_bucket_names, bucket_key_map, per_category_limit=50)` — `grep -aFl` each token against every must-be-encrypted file across the requested categories. `categories=None` → all six (`snapshot`, `data`, `error`, `codebook`, `request_handler_cache`, `stats_logs`). Per-category result includes `files_checked`, `leak_count`, `leaks`.
- `verify_no_plaintext_in_snapshot_files(...)` — backwards-compat wrapper calling the generalized verifier with `categories=("snapshot",)`.

Codebook:
- `verify_gsi_codebook_encrypted(index_nodes, expected_key_id, encrypted_bucket_names, bucket_key_map)` — header magic + key ID on every file under `@2i/*.index/codebook/`.
  - `encryption_status == "encrypted"` → pass
  - `encryption_status == "partially_encrypted"` → soft fail (logged + tracked in `soft_failed`, does not flip overall status)
  - any other status (incl. `"unencrypted"`) → hard fail
  - stray key IDs not in `expected_key_id` → hard fail (strict subset)

Naming-leak (no bucket/scope/collection/index/field name on disk):
- `_collect_forbidden_names(rest)` — pulls names from raw `/indexStatus` JSON (the parsed map drops `scope`/`collection`/`secExprs`); filters out `_default`/`default`/needles `<3` chars/all-digit.
- `verify_no_sensitive_names_in_paths(index_nodes, forbidden_substrings)` — recursively lists `@2i` + log dir, flags any path **component** containing a needle.

Legacy / unused by the new orchestrator (kept for backward compatibility):
- `verify_gsi_storage_files_encrypted(index_nodes, gsi_type, expected_key_id)` — broader/older sweep; superseded by the per-category methods above.

Low-level primitives:
- `verify_file_encryption_magic_bytes(node, file_path)` → `(is_encrypted, details)`
- `verify_file_header_contains(node, file_path, text)` → `(found, details)`
- `verify_file_header_contains_any(node, file_path, values)` → `(found, matched/details)`
- `_normalize_key_ids(expected_key_id)` — accepts None/str/list, returns list of str
