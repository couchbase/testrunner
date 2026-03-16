# Troubleshooting

## cluster_run Issues

**cluster_run not found or fails to start:**
```bash
# Verify ns_server exists
ls ../ns_server/

# Build cluster_run
cd ../ns_server && make cluster_run

# Check port conflicts (default: 9000+)
lsof -ti:9000

# Kill stuck processes
pkill -f cluster_run
```

**Multi-node cluster_run issues:**
- Ensure unique ports per node
- Check process count: `ps aux | grep cluster_run`
- Verify sufficient CPU/memory for node count

## Import and Path Errors

**ModuleNotFoundError or ImportError:**
```bash
# Must run from project root
cd /path/to/testrunner

# Verify Python path includes lib/ and pytests/
export PYTHONPATH=lib:pytests:$PYTHONPATH

# Verify critical imports
python3 -c "import couchbase, paramiko, requests, yaml"

# Reinstall dependencies
pip3 install -r requirements.txt --force-reinstall
```

**couchbase SDK installation failure:**
- Requires libcouchbase development libraries
- macOS: usually bundled with SDK pip package
- Linux: may need `libcouchbase2-libevent` or `libcouchbase-devel`

## Remote Cluster Connection

**SSH connection failures:**
```bash
# Verify SSH key path in INI: ssh_key=/path/to/private_key
ssh -i <key_path> user@host

# Check network connectivity
ping <cluster_ip>
```

**REST API connection failures:**
```bash
# Verify cluster is accessible
curl http://<host>:8091/pools

# Check cluster status
curl http://<host>:8091/pools/default
```

**Certificate/TLS errors:**
- Check SSL config in INI file `[ssl]` section
- For testing, use `[ssl] verify=False` (insecure mode)

## Test Execution Failures

**Port already in use:**
```bash
pkill -f cluster_run
# Or kill specific port:
lsof -ti:9000 | xargs kill -9
```

**Test timeout:**
- Increase with `-p timeout=600` (seconds)
- Reduce dataset size with `-p num_items=1000`
- Check cluster resources and network latency

**Bucket creation failures:**
- Verify cluster has sufficient memory quota
- Check existing buckets: `curl http://<host>:8091/pools/default/buckets`
- Ensure previous bucket delete completed before new create

**Dataset not cleaned up between runs:**
- Use `-p skip_cleanup=False` to enforce cleanup
- Manually flush bucket or recreate

## Log and Report Issues

**Logs not generated:**
- Check for `tmp-<timestamp>/` in project root
- Verify test runner has write permissions
- Confirm test actually completed (look for summary output)

**XUnit report issues:**
- Report location: `tmp-<timestamp>/report-<timestamp>.xml-<suite>.xml`
- Verify XML is complete and well-formed

## Dependency Issues

```bash
# Upgrade pip first
pip3 install --upgrade pip

# Force reinstall all
pip3 install -r requirements.txt --force-reinstall

# If buildout needed (legacy)
pip3 install zc.buildout
rm -rf bin/ develop-eggs/ parts/
buildout
```

## Cloud Testing Issues

| Provider | Verification command |
|----------|---------------------|
| AWS | `aws ec2 describe-instances` |
| Azure | `az account show` |
| GCP | `gcloud auth list` |

Common issues: expired credentials, insufficient IAM permissions, resource quota limits.

## Configuration Issues

**INI file parse errors:**
- Verify required sections: `[global]`, `[servers]`
- Check for encoding issues or stray characters

**Conf file not found:**
- Path must be relative to project root
- Verify file exists: `ls conf/<path>`

**Parameter parsing errors:**
- Format: `-p key1=value1,key2=value2`
- Quote values with spaces: `-p param="value with spaces"`

## Flaky Test Patterns

| Pattern | Mitigation |
|---------|------------|
| Intermittent timeouts | Increase `-p timeout`, check network stability |
| Data inconsistency | Ensure cleanup between runs, use deterministic data |
| Cluster state leakage | Verify cluster healthy before test, check for ongoing rebalance |
| Race conditions | Reduce parallelism, add explicit waits |

## When to Escalate

- cluster_run consistently fails to start after rebuild
- >3 consecutive critical smoke test failures
- Significant increase in test execution time
- Multiple tests in same service failing simultaneously

**Information to collect:**
- Test output and logs from `tmp-<timestamp>/`
- Cluster status: `curl http://<host>:8091/pools/default`
- System resources: `top`, `df -h`
- Git branch and recent changes: `git log --oneline -5`
