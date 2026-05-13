# Query Service — Encryption at Rest Tests

**Test file:** `n1ql_encryption_at_rest.py`  
**Test class:** `QueryEncryptionAtRestTests`

---

## Background: three distinct operations

The terms "key rotation", "force encryption", and "full re-encryption" refer to three
separate cluster-manager-driven workflows. They are **not** interchangeable. The
distinction matters when reading test names and understanding what each test exercises.

### 1. Key Rotation

Two independent sub-processes that run in parallel:

| Sub-process | Trigger | Callback | Effect |
|---|---|---|---|
| New active key generation | Periodic (default: monthly) | `RefreshKeysCallback` | Cluster manager generates a new active key and pushes it to all services. All **new** data written to disk uses the new active key. Previous (historical) keys remain available for **decryption** of existing data. |
| Expired-key removal | Key exceeds max lifetime (default: 1 year) | `DropKeysCallback` | Cluster manager initiates a "drop key" procedure for the expired key. All data encrypted with that key is **re-encrypted** using the current active key. Once re-encryption completes the old key is deleted. |

Key rotation is therefore a **gradual** process: new writes use the new key while old data
is lazily re-encrypted as keys expire.

**How the test exercises this:**  
`test_query_log_encryption_key_rotation` uses the **original** log KEK
(`log_encryption_at_rest_id`) — no new secret is created.

1. Before any setup, pin `log.dekRotationInterval` and `log.dekLifetime` to **60 000 s
   (1 000 min)** so no accidental background rotation fires during bucket/index creation.
2. Run setup (bucket, indexes, scans) and confirm baseline DEK IDs.
3. Reduce `log.dekRotationInterval` / `log.dekLifetime` to **120 s (2 min)** to trigger
   the first rotation.
4. Poll `GET :8093/admin/encryption_at_rest` until new DEK IDs appear (set difference from
   baseline).
5. **Immediately** re-pin the interval back to 60 000 s so a second rotation cannot fire
   before file validation completes.
6. Re-run scans, then verify log files carry the new DEK IDs.

---

### 2. Force Encryption

Triggered by the cluster manager when the encryption state of stored data needs to be
brought in line with the current encryption policy. There are two scenarios:

| Scenario | What happens |
|---|---|
| **Encryption being enabled** | Cluster manager runs `DropKeysCallback` for the **empty key** (`key_id == ""`). This instructs services to encrypt any data that is not yet encrypted, using the current active key. |
| **Encryption being disabled** | Cluster manager runs `DropKeysCallback` for **all keys**. Because the current active key is `""` (empty), this is effectively a decryption pass — data is re-written without encryption. |

Force encryption is **not** user-initiated; it is a side-effect of changing the
cluster-level encryption policy.

> **Note:** Force encryption is not explicitly covered by the current test suite. A
> future test should toggle `log.encryptionMethod` from `encryptionKey` → `disabled` →
> `encryptionKey` and verify that files are correctly decrypted and re-encrypted.

---

### 3. Full Re-encryption

A **user-initiated** operation that combines both sub-processes of key rotation at once:

1. Generate a new active key immediately (`RefreshKeysCallback` is called).
2. Mark **all** existing keys as expired (including the empty key `""`) and initiate
   `DropKeysCallback` for each of them. Any data encrypted with an old key — or not yet
   encrypted — is re-encrypted with the newly generated active key.

This is the most thorough operation: after it completes, every byte on disk is encrypted
with a single, freshly generated key.

**How the test exercises this:**  
`test_query_log_encryption_force_reencryption` calls `trigger_log_reencryption()` which
triggers `POST /controller/dropEncryptionAtRestDeks/log`.

1. Before any setup, pin `log.dekRotationInterval` and `log.dekLifetime` to **60 000 s
   (1 000 min)** so no accidental background rotation fires during setup.
2. Run setup (bucket, indexes, scans) and confirm baseline DEK IDs.
3. Call `trigger_log_reencryption` — this instructs the cluster manager to drop all
   existing log DEKs and re-encrypt log data with a newly generated active key.
4. Poll `GET :8093/admin/encryption_at_rest` until new DEK IDs appear.
5. **Immediately** re-pin the interval back to 60 000 s so a subsequent background
   rotation cannot fire before file validation completes.
6. Re-run scans, then verify log files carry the new DEK IDs.

---

## Test summary

| Test | Operation exercised | What changes |
|---|---|---|
| `test_query_request_log_files_encrypted` | Sanity — encryption already active | Verifies `rlstream.*` and `local_request_log.*` files carry "Couchbase Encrypted" header and the current in-use key ID |
| `test_query_log_encryption_key_rotation` | Key rotation (new active DEK sub-process) | Reduces `dekRotationInterval`/`dekLifetime` on original KEK → new DEK promoted → files use new DEK IDs; KEK unchanged |
| `test_query_log_encryption_force_reencryption` | Full re-encryption | `trigger_log_reencryption` (`dropEncryptionAtRestDeks/log`) drops all log DEKs → log files re-encrypted with newly generated key IDs |

---

## Running the tests

All three tests require:

```
enable_log_encryption_at_rest=True
```

passed as a test parameter. `BaseTestCase.suite_setUp` handles the actual cluster
configuration — it creates the log KEK secret and calls:

```
POST /settings/security/encryptionAtRest
  {"log.encryptionMethod": "encryptionKey", "log.encryptionKeyId": <id>}
```

The query service admin settings used by the tests:

| Setting | Value set by tests | Purpose |
|---|---|---|
| `completed-threshold` | `0` | Log every completed query request (no minimum duration filter) |
| `completed-stream-size` | `500` | Rotate the active `rlstream.*` file after 500 entries, producing a `local_request_log.*` archive |

Both settings are restored to their defaults (`1000` and `0` respectively) in `tearDown`.
