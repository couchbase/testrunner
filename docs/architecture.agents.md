# TestRunner Architecture

## Component Boundaries

**Test Runner Entry Points:**
- `testrunner.py` - Primary command-line test executor
- `Makefile` - High-level orchestration with service-specific targets
- `scripts/start_cluster_and_run_tests.sh` - Shell script wrapper (cluster_run focus)
- `scripts/start_cluster_and_run_tests.py` - Python orchestration (parameter handling)

**Test Framework Core (`lib/`):**
- `basetestcase.py` - Base test class hierarchy
- `membase/api/` - REST API clients (REST communication boundary)
- `remote/` - SSH/SCP operations (remote boundary)
- `couchbase_helper/` - SDK and data operations

**Service Test Suites (`pytests/`):**
- Service isolation: each service (`tuqquery/`, `gsi/`, etc.) has independent tests
- Common base: All inherit from `basetestcase.py`
- Service-specific: Each has service base classes and utilities

**Performance Engines (`lib/perf_engines/`):**
- mcsoda, cbsoda - KV workload generators
- obs - Observer-based performance testing
- Separate from test suites, used by performance tests

## Runtime Flows

**Test Execution Flow:**
1. Parse CLI args and `-i` INI file (testrunner.py)
2. Parse `-c` conf file for test suite
3. Initialize cluster (cluster_run or remote)
4. For each test in suite:
   - Create test instance
   - Call setUp (cluster setup, bucket creation)
   - Run test method
   - Call tearDown (cleanup)
5. Generate xunit report
6. Create tmp-<timestamp>/ with logs

**Cluster Setup Flow:**
1. Parse INI file `[servers]` section
2. Initialize cluster_run or connect to remote cluster
3. Create buckets if specified
4. Enable services if specified
5. Load initial data if specified

**Test Discovery Flow:**
1. Read conf file for test module paths
2. Import test modules (Python unittest discovery)
3. Parse test parameters from conf file
4. Build test suite with parameters

**Result Reporting Flow:**
1. Test execution via unittest framework
2. XUnitTestResult collects results
3. Write xunit XML to tmp-<timestamp>/
4. Log test summaries (pass/fail counts)

**Cloud Provisioning Flow:**
1. Parse cloud credentials and configuration
2. Call cloud provider API (AWS EC2, Azure, etc.)
3. Wait for instance provisioning
4. Install Couchbase via scripts/install.py
5. Generate INI file for cluster
6. Execute tests against cloud cluster

## Key Validation Points

**Before Test Execution:**
- Verify cluster connectivity (REST API reachable)
- Check cluster health (`/pools/default` endpoint)
- Verify required services enabled
- Ensure bucket quota available

**During Test Execution:**
- Cluster operations succeed (rebalance, failover, etc.)
- SDK operations complete without errors
- Query results validated against expectations
- Index creation and query completion

**After Test Execution:**
- Cluster in expected state
- Test logs created successfully
- xunit report generated
- Cleanup executed (if skip_cleanup=False)

## Service Boundaries

**Query Service (tuqquery/):**
- REST API: /query/service, /indexerStatus
- SDK: query() method
- Index management via gsi/ suite or built-in operations

**FTS Service (fts/):**
- REST API: /api/index, /api/pindex
- Search endpoints: /api/bucket/<bucket>/index/<index>/query
- Indexing pipeline: ingestion → indexing → search

**Eventing Service (eventing/):**
- REST API: /api/v1/functions, /api/v1/eventing
- Function: create JavaScript handlers
- Processing: document changes → event → function execution

**GSI Service (gsi/):**
- REST API: /indexStatus, /setIndexingMemQuota
- Index types: primary, secondary, vector, array
- Query integration via Query Service

**XDCR Service (xdcr/):**
- REST API: /pools/default/remoteClusters, /pools/default/buckets/<bucket>/replications
- Replication: source bucket → target bucket changes
- Conflict resolution: last-write-wins or custom

**Backup Service (ent_backup_restore/):**
- REST API: /api/v1/backup (backup service specific)
- Cloud providers: S3, Azure Blob, Google Cloud Storage
- Operations: full backup, incremental backup, restore

## Integration Points

**External Cluster Communication:**
- REST API for all cluster operations (port 8091)
- SDK for data operations (port 11210)
- SSH for remote installation and log collection

**Service Interdependencies:**
- Query depends on GSI for secondary indexes
- FTS can use GSI for some query types
- XDCR replicates all services
- Eventing can trigger queries and document updates

**External Tools:**
- cbcollect_info - Diagnostics collection
- cbbackupmgr - Backup utility integration
- couchbase-cli - CLI wrapper testing
- SDK clients (Python, Java, Go) for multi-language testing

## Validation Clues

**Framework Health Indicators:**
- basetestcase.py imports successfully
- cluster_run starts without errors
- REST API clients connect and authenticate
- Test suite discovery works

**Service-Specific Clues:**
- Query service: N1QL queries execute, indexes build
- GSI: indexes build, query results return
- FTS: indexes create, search returns results
- Eventing: functions deploy and execute
- XDCR: replication starts and progresses
- Backup: backup completes, restore works

**Performance Health Indicators:**
- Performance engines (mcsoda, cbsoda) start
- Load tests complete with measurable statistics
- No resource exhaustion during tests
- Consistent performance between runs

**Failure Patterns:**
- Import errors: Python path issues or missing dependencies
- Connection errors: Network or authentication issues
- Timeout errors: Resource constraints or slow cluster
- Assertion failures: Test expectations not met

## Extension Points

**New Service Tests:**
- Create `pytests/<service>/` directory
- Create service base class inheriting from basetestcase
- Add test methods with unittest framework
- Create conf file in `conf/<service>/` for test suite

**New Performance Tests:**
- Extend lib/perf_engines/ engines
- Use existing workload generation patterns
- Add performance-specific validation
- Use perf/ conf files for execution

**New Cloud Provider:**
- Extend scripts/cloud_provision.py
- Add provider-specific API calls
- Create resource templates
- Add provider credential handling

**New Test Framework Features:**
- Extend basetestcase.py for common functionality
- Add lib/lib for reusable utilities
- Create helper functions in couchbase_helper/
- Extend rest clients in membase/api/
