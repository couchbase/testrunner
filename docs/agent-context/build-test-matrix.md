# Build and Test Matrix

All tests are integration tests requiring a running Couchbase cluster. There is no unit test framework, linting, or type checking configured.

## Smoke Tests

```bash
# KV single-node smoke (fastest, ~minutes)
make e2e-kv-single-node
# Runs: scripts/start_cluster_and_run_tests.sh b/resources/dev.ini conf/py-all-dev.conf

# Single test execution
./testrunner -i b/resources/dev.ini -t setgettests.MembaseBucket.value_100b
```

## Service-Specific Make Targets

All targets auto-run `git submodule init && git submodule update` before test execution.

### GSI (Global Secondary Indexes)

```bash
make test-gsi-integrations-tests PARAMS=<additional_params>
# INI:  b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini
# Conf: conf/simple_gsi_n1ql.conf
# Flags: VERBOSE=1 DEBUG=1
```

### FTS (Full Text Search)

```bash
make test-fts
# INI:  b/resources/dev-4-nodes-xdcr_n1ql_fts.ini
# Conf: conf/fts/py-fts-simpletopology.conf
# Params: get-cbcollect-info=False,GROUP=PS,fts_quota=1000,index_type=scorch,skip_log_scan=False,skip_disable_nton=True,validate_index_partition=False
```

### Eventing

```bash
make test-eventing-sanity-tests
# Uses Python orchestrator (not shell script)
# INI:  b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini
# Conf: conf/eventing/eventing_sanity.conf
```

### Views

```bash
make test-viewquery
# INI:  b/resources/dev-4-nodes.ini
# Conf: conf/view-conf/py-viewquery.conf

make test-views-pre-merge       # Required before view engine merges
make test-viewmerge              # Single-node + 4-node view merge tests
```

### XDCR

```bash
make test-xdcr-merge
# INI:  b/resources/dev-4-nodes-xdcr.ini
# Conf: conf/py-xdcrmerge.conf
```

### DCP

```bash
make dcp-test TEST=<test_name>
# INI: b/resources/dev-4-nodes.ini
# Conf: conf/py-dcp.conf
```

## Flexible Execution

```bash
# Run any single test
make any-test NODES=<ini_file> TEST=<test_name>

# Run any test suite
make any-suite NODES=<ini_file> SUITE=<conf_file>

# Direct testrunner invocation
python3 testrunner.py -i <ini_file> -t <test_module.TestClass.test_method>
python3 testrunner.py -i <ini_file> -c <conf_file>

# Python orchestrator with cluster management
python3 scripts/start_cluster_and_run_tests.py MAKE <ini_file> <conf_file> <VERBOSE> <DEBUG>
```

## Dependency Verification

```bash
pip3 install -r requirements.txt

# Verify critical imports
python3 -c "import couchbase, paramiko, requests, yaml"

# Verify Python syntax on a file
python3 -m py_compile <path_to_file>
```

## Test Artifacts

```bash
# Tests create:
tmp-<timestamp>/
  report-<timestamp>.xml-<suite>.xml   # XUnit results
  <test_name>.log                      # Execution logs

# Artifacts are NOT auto-cleaned
```

## Parallel Test Dispatchers

```bash
python3 scripts/testDispatcher_sdk4.py   # SDK4 (most recent)
python3 scripts/testDispatcher_sdk3.py   # SDK3
python3 scripts/testDispatcher.py        # Legacy
```

## Execution Flow

```
Makefile target
  -> git submodule update
  -> scripts/start_cluster_and_run_tests.sh (or .py)
     -> build ../ns_server (if cluster_run)
     -> start cluster_run with N nodes
     -> python3 testrunner.py -i <ini> -c <conf>
        -> parse INI for cluster topology
        -> parse conf for test suite
        -> for each test: setUp -> test -> tearDown
        -> write xunit XML to tmp-<timestamp>/
     -> stop cluster_run
```
