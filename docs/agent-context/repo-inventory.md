# Repository Inventory: Couchbase TestRunner

## Summary

Python-based integration test framework for Couchbase Server. Validates 8+ services across on-premises clusters, `cluster_run` sandboxes, and cloud deployments.

## Languages and Tooling

| Item | Detail |
|------|--------|
| Language | Python 3.10.13 |
| Test framework | Custom runner (`testrunner.py`) based on `unittest` |
| Package management | `pip3` + `requirements.txt` |
| Build orchestration | `Makefile` -> `scripts/start_cluster_and_run_tests.sh/.py` -> `testrunner.py` |
| Legacy build | `buildout.cfg` (optional) |
| Linting | None configured |
| Type checking | None configured |
| CI/CD | None visible in repo |

## Git Submodules

| Submodule | Path | URL |
|-----------|------|-----|
| Java SDK client | `java_sdk_client` | github.com/couchbaselabs/java_sdk_client |
| Capella REST APIs | `lib/capellaAPI` | github.com/couchbaselabs/CapellaRESTAPIs |
| DocLoader | `magma_loader/DocLoader` | github.com/couchbaselabs/DocLoader.git |
| GSI utilities | `gsi_utils` | github.com/couchbaselabs/gsi_utils.git |

Initialize with `make init-submodules` or `git submodule update --init --force --remote`.

## Key Dependencies (from requirements.txt)

| Category | Packages |
|----------|----------|
| Couchbase SDK | couchbase==4.4.0 |
| HTTP/REST | requests, httplib2, urllib3 |
| SSH/Remote | paramiko==2.7.1, PyNaCl, bcrypt |
| Cloud (AWS) | boto3, botocore, s3transfer |
| Cloud (Azure) | azure-identity, azure-mgmt-resource, azure-storage-blob |
| Cloud (GCP) | google-cloud-compute, google-cloud-storage |
| ML/Vector | torch==2.4.0, sentence-transformers, transformers, faiss-cpu, numpy, scipy |
| Data/Test | deepdiff, Faker, beautifulsoup4, PyYAML, Geohash |
| Infrastructure | docker, psutil |
| Database | mysql-connector-python, psycopg2-binary |

## Directory Structure

### Test Suites (`pytests/` -- 80+ directories/modules)

| Directory | Service | Estimated modules |
|-----------|---------|-------------------|
| `gsi/` | Global Secondary Indexes | 65 |
| `tuqquery/` | N1QL/Query | 120+ |
| `fts/` | Full Text Search | 30+ |
| `eventing/` | Eventing | 40+ |
| `xdcr/` | Cross Datacenter Replication | 35+ |
| `security/` | RBAC, SSL, x.509, LDAP | 50+ |
| `cbas/` | Analytics | 15+ |
| `ent_backup_restore/` | Backup & Restore | 13+ |
| `rebalance/` | Rebalance operations | 12+ |
| `failover/` | Failover operations | 10+ |
| `upgrade/` | Upgrade tests | 5+ |

Top-level test files: `basetestcase.py` (186KB, 3579 lines), `setgettests.py`, `buckettests.py`, `rebalancetests.py`, `newupgradetests.py`.

### Framework Libraries (`lib/` -- 48 entries)

| Directory/File | Purpose |
|----------------|---------|
| `membase/api/` | REST API clients for cluster management |
| `remote/` | SSH, SCP, remote command execution |
| `couchbase_helper/` | SDK utilities, data generators, cluster helpers |
| `tasks/` | Concurrent task execution |
| `collection/` | Collections API wrappers |
| `Cb_constants/` | Couchbase server constants |
| `perf_engines/` | Performance engines (mcsoda, cbsoda, obs) |
| `sdk_client3.py` | Python SDK client wrapper (56KB) |
| `cluster_run_manager.py` | Local cluster_run lifecycle management |
| `vector/` | Vector search utilities |
| `SystemEventLogLib/` | System event log helpers |

### Configuration (`conf/` -- 147 files + 28 subdirectories)

| Path pattern | Purpose |
|--------------|---------|
| `conf/gsi/` | GSI test suites (104 files) |
| `conf/tuq/` | Query test suites (188 files) |
| `conf/fts/` | FTS test suites |
| `conf/eventing/` | Eventing test suites |
| `conf/xdcr/` | XDCR test suites |
| `conf/security/` | Security test suites |
| `conf/simple_gsi_n1ql.conf` | GSI integration smoke suite |
| `conf/py-all-dev.conf` | KV dev smoke suite |

### Cluster Templates (`b/resources/` -- 153 INI files)

Common patterns: `dev.ini`, `dev-4-nodes.ini`, `dev-4-nodes-xdcr_n1ql_gsi.ini`, `dev-4-nodes-xdcr_n1ql_fts.ini`.

### Scripts (`scripts/` -- 79 entries)

| File | Purpose |
|------|---------|
| `start_cluster_and_run_tests.sh` | Shell orchestrator: build ns_server, start cluster_run, run tests |
| `start_cluster_and_run_tests.py` | Python orchestrator with parameter handling |
| `install.py` | Remote Couchbase installation via SSH |
| `cloud_provision.py` | Cloud infrastructure provisioning (AWS, Azure, GCP) |
| `testDispatcher_sdk4.py` | SDK4-based parallel test dispatcher |
| `populateIni.py` | INI file generation |
| `merge_reports.py` | XUnit report merging |

## Evidence Gaps

- No CI/CD configuration visible (Jenkins, GitHub Actions, GitLab CI)
- No linting, formatting, or type checking tools configured
- No `.devcontainer/` or `docker-compose.yml` for dev environment
- No pre-commit hooks
- No `pytest.ini` or `tox.ini` (custom test runner only)
- `cluster_run` setup procedure beyond `../ns_server/` presence is undocumented
