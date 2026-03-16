# GSI Test Troubleshooting

## Index Build Failures

**Index stuck in building state:**
```bash
# Check index status via REST
curl http://<host>:8091/indexStatus

# Look for specific error message in test logs (from _wait_for_index_online method)
grep "index.*is not online" tmp-*/test.log

# Error message to search for:
# "index <index_name> is not online. last response is {'metrics': {...}, 'requestID': ..., 'results': [...], 'status': 'success'}"

# Increase timeout if needed
-p timeout_for_index_online=1200  # 20 minutes
```

**Index creation fails with "service not available":**
- Verify index service is enabled: `curl http://<host>:8091/pools/default`
- Check `services_init=kv:n1ql-kv-index-index` includes `index`
- Restart cluster if index service failed

**DGM (Data Growth Monitor) errors:**
- Reduce document count: `-p doc-per-day=10`
- Increase memory quota for index service
- Check `py-index_create_scan_drop_dgm.conf` for proper DGM configuration

## Rebalance Failures

**Indexes lost during rebalance:**
```bash
# Check base_gsi.py for index_lost_during_move_out tracking
grep "index_lost_during_move_out" tmp-*/test.log
```

**Rebalance timeout:**
```bash
# Increase rebalance timeout
-p rebalance_timeout=900  # 15 minutes

# Use file-based rebalance (newer, more stable)
-c conf/gsi/gsi_file_based_rebalance.conf
```

**Index node down during rebalance:**
- Check `index_load_balancing_gsi.py` tests for coverage
- Verify replica indexes are created: `-p use_replica_when_active_down=True`

## Vector Index Issues

**FAISS import errors:**
```bash
# Verify faiss-cpu is installed
pip3 install -r /Users/pavan.pb/Workstuff/testrunner/requirements.txt --force-reinstall

# Check for numpy conflicts
python3 -c "import faiss, numpy; print(faiss.__version__, numpy.__version__)"
```

**Vector similarity search fails:**
- Verify embedding dimensions match index definition
- Check `composite_vector_index.py` for embedding generation
- Reducing embedding size can help with memory issues

## Storage Engine Issues

**Plasma (forestdb) crashes:**
- Check for DGM state: `-p plasma_dgm=False`
- Reduce document count: `-p num_of_docs_per_collection=5000`
- Verify sufficient index memory quota

**MOI (memory_optimized) out of memory:**
```bash
# Increase index memory quota via REST
curl -u admin:password -X POST http://<host>:8091/pools/default \\
  -d indexMemoryQuota=<larger_value>

# Or use less data
-p num_of_docs_per_collection=1000
```

## Collection Test Failures

**Scope/collection not found:**
```bash
# Verify scope and collection prefixes
-p scope_prefix=test_scope
-p collection_prefix=test_collection
-p num_scopes=1
-p num_collections=1
```

**Collection index scan consistency failures:**
- Check `collection-index-scan-consistency.conf`
- Adjust scan consistency: `-p scan_consistency=request_plus`

## Query Failures

**N1QL query errors:**
```bash
# Verify scan consistency level is valid
-p scan_consistency=request_plus  # not "REQUEST_PLUS"

# Check query syntax in test code
grep "run_cbq_query" <test_file>.py
```

**Covering index not used:**
- Add `use_gsi_for_secondary=True` parameter
- Check EXPLAIN output: verify index is in plan
- Ensure all SELECT and WHERE fields are indexed

## Upgrade Test Failures

**Index not present after upgrade:**
```bash
# Verify gsi_utils submodule is initialized
make init-submodules

# Check upgrade conf for proper version ranges
cat conf/gsi/py-upgradegsi.conf
```

**INT64 upgrade issues:**
- Test config: `conf/gsi/py-upgrade_gsi_int64.conf`
- Verify large integer values are handled correctly

## Parameter Parsing Errors

**Invalid parameter format:**
- Use comma-separated key=value pairs: `-p key1=val1,key2=val2`
- Quote values with spaces: `-p param="value with spaces"`

**Boolean parameter issues:**
- Use Python boolean values: `-p defer_build=True` (not `true`)

## Index Replica Issues

**Replica index not used after failover:**
```bash
# Check replica coverage
curl http://<host>:8091/indexStatus | grep -i replica

# Verify replica count
# Usually: create index with num_replicas=N
```

**Alter replica index failures:**
- Conf file: `conf/gsi/alter_index_replicas.conf`
- Check `gsi_alter_index_replicas.py` for alter logic

## Common Error Messages

| Error | Cause | Fix |
|-------|-------|-----|
| `Index cannot be built` | Bucket quota exceeded or index memory low | Increase index memory, reduce data |
| `GSI not available` | Index service not enabled or crashed | Enable index service, restart cluster |
| `Scan consistency not satisfied` | Vector scan incomplete | Increase vector timeout, use `not_bounded` |
| `Cannot find scope` | Collection not created | Check scope/collection creation in setUp |
| `File-based rebalance failed` | Index partition metadata corrupt | Use standard rebalance test as fallback |

## When to Escalate

**Escalate if:**
- GSI index service crashes repeatedly
- Smoke tests (`py-gsi_sanity.conf`) fail >3 times
- New vector index test fails consistently
- Upgrade tests fail with index loss

**Information to collect:**
- Test logs from `tmp-<timestamp>/`
- Index status: `curl http://<host>:8091/indexStatus`
- Cluster services: `curl http://<host>:8091/pools/default`
- Git log of recent GSI changes: `git log --oneline -10 -- pytests/gsi/`
