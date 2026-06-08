# AGENTS.md — `pytests/ent_backup_restore`

## Module Overview

This directory contains the Enterprise Backup & Restore (cbbackupmgr) integration test suite for Couchbase Server. It covers the full lifecycle of backup, restore, and merge operations against on-premises clusters, `cluster_run` sandboxes, and cloud deployments (S3, GCP, Azure). Tests exercise both the legacy `cbbackupmgr` CLI tool and the newer **Backup Service** REST API introduced in Couchbase 7.x.

---

## Directory Layout

```
pytests/ent_backup_restore/
├── enterprise_backup_restore_base.py       # Core base class (EnterpriseBackupRestoreBase)
├── enterprise_backup_restore_test.py       # Main test class (EnterpriseBackupRestoreTest)
├── enterprise_backup_merge_tests.py        # Merge-specific tests (EnterpriseBackupMergeTest)
├── enterprise_backup_restore_bwc.py        # Backward-compatibility tests (EnterpriseBackupRestoreBWCTest)
├── enterprise_bkrs_collection_base.py      # Collections base class (EnterpriseBackupRestoreCollectionBase)
├── enterprise_bkrs_collection.py           # Collections test class (EnterpriseBackupRestoreCollectionTest)
├── backup_service_base.py                  # Backup Service API base class (BackupServiceBase)
├── backup_service_test.py                  # Backup Service REST API tests (BackupServiceTest)
├── backup_service_upgrade.py               # Upgrade integration hooks (BackupServiceHook)
├── backup_examine_test.py                  # cbbackupmgr examine command tests
├── provider/
│   ├── provider.py                         # Abstract cloud storage provider base
│   ├── s3.py                               # Amazon S3 implementation
│   ├── gcp.py                              # Google Cloud Platform implementation
│   └── azure.py                            # Azure Blob Storage implementation
└── validation_helpers/
    ├── backup_restore_validations.py        # BackupRestoreValidations: post-op assertions
    ├── validation_base.py                   # Shared validation utilities
    ├── files_validations.py                 # File-system level validations
    ├── directory_structure_validations.py   # Backup archive directory checks
    └── json_generator.py                    # JSON document generator for test data
```

---

## Class Hierarchy

```
BaseTestCase  (lib/)
└── EnterpriseBackupRestoreBase              # enterprise_backup_restore_base.py
    ├── EnterpriseBackupRestoreTest          # enterprise_backup_restore_test.py
    ├── EnterpriseBackupMergeTest            # enterprise_backup_merge_tests.py
    └── EnterpriseBackupRestoreBWCTest       # enterprise_backup_restore_bwc.py

BaseTestCase + NewUpgradeBaseTest
└── EnterpriseBackupRestoreCollectionBase    # enterprise_bkrs_collection_base.py
    └── EnterpriseBackupRestoreCollectionTest# enterprise_bkrs_collection.py

BaseTestCase
└── BackupServiceBase                        # backup_service_base.py
    ├── BackupServiceTest                    # backup_service_test.py
    └── BackupServiceUpgradeTest             # backup_service_upgrade.py
```

---

## Key Files and Responsibilities

### `enterprise_backup_restore_base.py`
- **Purpose:** Core base class for all cbbackupmgr CLI-based tests.
- **Key class:** `EnterpriseBackupRestoreBase` — sets up backup repositories, `Backupset` data class, cluster credentials, and helper wrappers around `cbbackupmgr` shell commands.
- **Important helpers:**
  - `backup_create()` — creates a backup repository
  - `backup_cluster()` — triggers a full or incremental backup
  - `backup_restore()` — restores from a backup repository
  - `backup_merge()` — merges backup increments
  - `backup_list()` — lists backup contents
  - `backup_compact()` — compacts backup data
  - `verify_backup_create()`, `verify_cluster_restore_bucket_data()` — post-operation assertions
- **`Backupset` class:** Holds all state for a backup set (archive path, repo name, directory, host, username, etc.)

### `enterprise_backup_restore_test.py`
- **Purpose:** Main integration tests for `cbbackupmgr` CLI.
- **Key class:** `EnterpriseBackupRestoreTest`
- **Key test methods:**
  | Method | Description |
  |--------|-------------|
  | `test_backup_create` | Validates backup repository creation |
  | `test_backup_restore_sanity` | End-to-end backup and restore |
  | `test_backup_restore_with_ops` | Backup/restore under concurrent KV ops |
  | `test_backup_restore_with_rbac` | RBAC permission enforcement |
  | `test_backup_restore_with_fts` | Full-Text Search index backup/restore |
  | `test_backup_restore_with_views` | Views backup/restore |
  | `test_backup_restore_with_gsi` | GSI index backup/restore |
  | `test_backup_restore_with_eventing` | Eventing functions backup/restore |
  | `test_backup_restore_with_audit` | Audit log behaviour during backup |
  | `test_backup_restore_with_expiry` | Document TTL/expiration handling |
  | `test_backup_restore_with_xattr` | Extended attributes (XATTRs) |
  | `test_backup_restore_after_rebalance` | Backup after cluster rebalance |
  | `test_backup_restore_after_failover` | Backup after node failover |
  | `test_backup_restore_cloud_storage` | Cloud object-store targets (S3/GCP/Azure) |

### `enterprise_backup_merge_tests.py`
- **Purpose:** Tests for merging multiple backup increments.
- **Key class:** `EnterpriseBackupMergeTest`
- **Key test methods:**
  - `test_multiple_backups_merges` — merges N incremental backups into one
  - `test_multiple_backups_merge_with_tombstoning` — merge with deleted-document tombstones

### `enterprise_backup_restore_bwc.py`
- **Purpose:** Backward-compatibility (BWC) tests — verifies restoring backups taken on older Couchbase versions.
- **Key class:** `EnterpriseBackupRestoreBWCTest`
- **Inherits from:** `EnterpriseBackupRestoreBase`, `NewUpgradeBaseTest`

### `enterprise_bkrs_collection_base.py` / `enterprise_bkrs_collection.py`
- **Purpose:** Backup/restore tests scoped to Couchbase Collections (scopes, collections, keyspaces).
- **Key class:** `EnterpriseBackupRestoreCollectionTest`
- **Key test methods:**
  - `test_backup_restore_collection_sanity` — end-to-end backup/restore targeting a specific collection
  - Collection-level filter and mapping tests

### `backup_service_base.py` / `backup_service_test.py`
- **Purpose:** Tests for the Couchbase **Backup Service** REST API (introduced in CB 7.x). Uses HTTP-based `BackupServiceClient` instead of CLI.
- **Key class:** `BackupServiceTest`
- **Key test areas:**
  - Plan CRUD (create, read, update, delete backup plans)
  - Repository management
  - Scheduled task trigger/pause/resume
  - One-time backup/restore via REST
  - Health check and task history APIs

### `backup_service_upgrade.py`
- **Purpose:** Provides `BackupServiceHook` for upgrade scenario integration — hooks into the upgrade test harness to run backup/restore assertions before and after version upgrades.

### `backup_examine_test.py`
- **Purpose:** Tests for `cbbackupmgr examine` — introspects a backup archive without restoring.
- **Key data classes:** `Document`, `Collection`, `Tag` (represent the examine output model).
- **Key test methods:** verify document-level, collection-level, and metadata-level examine output.

### `provider/`
- **Purpose:** Abstracts cloud object-store access for backup targets.
- **`provider.py`:** Abstract `Provider` base — defines interface: `setup()`, `teardown()`, `get_path()`.
- **`s3.py`:** AWS S3 via `boto3` — manages bucket creation, path construction, and cleanup.
- **`gcp.py`:** GCP Cloud Storage via `google-cloud-storage`.
- **`azure.py`:** Azure Blob Storage via `azure-storage-blob`.

### `validation_helpers/`
- **Purpose:** Reusable post-operation assertion helpers decoupled from test logic.
- **`BackupRestoreValidations`:** Central class with:
  - `validate_backup_create()` — checks backup archive structure
  - `validate_restore()` — compares source and restored cluster data
  - `validate_merge()` — asserts merged backup integrity
- **`files_validations.py`:** Low-level file existence and size checks.
- **`directory_structure_validations.py`:** Validates cbbackupmgr archive directory layout.
- **`json_generator.py`:** Generates deterministic JSON documents for test data.

---

## Configuration Files

| File | Description |
|------|-------------|
| `conf/py-backuprestore.conf` | Primary test suite configuration — maps test classes/methods to cluster topology params |
| `conf/py-backuprestore-win.conf` | Windows-specific backup/restore test suite |

---

## Running Tests

```bash
# Full backup/restore suite (requires 4-node cluster)
make test-backuprestore PARAMS="..."

# Single test via testrunner CLI
./testrunner -i b/resources/4-nodes-template.ini \
  -t pytests.ent_backup_restore.enterprise_backup_restore_test.EnterpriseBackupRestoreTest.test_backup_restore_sanity

# Backup Service tests
./testrunner -i b/resources/4-nodes-template.ini \
  -t pytests.ent_backup_restore.backup_service_test.BackupServiceTest.test_backup_service_sanity

# Collections backup tests
./testrunner -i b/resources/4-nodes-template.ini \
  -t pytests.ent_backup_restore.enterprise_bkrs_collection.EnterpriseBackupRestoreCollectionTest.test_backup_restore_collection_sanity
```

---

## Key Dependencies

| Dependency | Source |
|-----------|--------|
| `BaseTestCase` | `lib/basetestcase.py` |
| `NewUpgradeBaseTest` | `pytests/upgrade/newupgradebasetest.py` |
| `RestConnection` | `lib/membase/api/rest_client.py` |
| `RemoteMachineShellConnection` | `lib/remote/remote_util.py` |
| `BucketOperationHelper` | `lib/membase/helper/bucket_helper.py` |
| `DocumentGenerator` / `BlobGenerator` | `lib/couchbase_helper/documentgenerator.py` |
| `BackupServiceClient` | Backup Service REST API wrapper (within `backup_service_base.py`) |
| `boto3` | AWS S3 cloud provider |
| `google-cloud-storage` | GCP cloud provider |
| `azure-storage-blob` | Azure cloud provider |

---

## Couchbase Features Covered

| Feature | Coverage |
|---------|----------|
| KV (Couchbase & Ephemeral buckets) | Full backup/restore, incremental, merge |
| Collections & Scopes | Scoped backup, filter mapping |
| Full-Text Search (FTS) | Index definition backup/restore |
| GSI (Global Secondary Index) | Index backup/restore |
| Views | Design document backup/restore |
| Eventing | Function definition backup/restore |
| XDCR | Backup under active replication |
| RBAC | Permission checks for cbbackupmgr operations |
| Audit Logging | Audit event emission during backup/restore |
| XATTRs | Extended attribute preservation |
| Document Expiry (TTL) | TTL preservation across restore |
| Cloud Storage | S3, GCP, Azure object-store targets |
| Rebalance / Failover | Backup/restore under topology changes |
| Backward Compatibility | Restore archives from older CB versions |
| Backup Service REST API | Plan management, scheduled/one-time backup |
| Upgrade | Pre/post-upgrade backup integrity via `BackupServiceHook` |

---

## Development Guidelines

1. **Inherit correctly:** CLI-based tests → `EnterpriseBackupRestoreBase`; REST API tests → `BackupServiceBase`; collection-scoped tests → `EnterpriseBackupRestoreCollectionBase`.
2. **Reuse base helpers:** Never shell out to `cbbackupmgr` directly in test methods — call the helpers defined in the base classes (e.g., `self.backup_create()`, `self.backup_restore()`).
3. **Validation:** Use `validation_helpers/backup_restore_validations.py` for assertions instead of ad-hoc checks in test methods.
4. **Cloud tests:** Instantiate the appropriate `Provider` subclass from `provider/` for cloud object-store targets; don't hardcode S3/GCP/Azure paths.
5. **Cleanup:** Always call `self.backup_remove()` (or the equivalent tearDown helper) to clean up backup archives; backup data can be large.
6. **Conf coverage:** Add new tests to `conf/py-backuprestore.conf` so they are included in CI suite runs.
7. **Python 3.10+:** All code must be Python 3.10 compatible; avoid deprecated APIs.

---

## Security Notes

- **Never commit** real cloud credentials (AWS keys, GCP service account JSON, Azure SAS tokens) into provider configs or test parameters.
- Cloud provider credentials should be injected via environment variables or the cluster INI file's `[cloud]` section.
- Backup archives can contain sensitive cluster data — do not log archive paths or contents in test output.

