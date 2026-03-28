# Elasticsearch Integration - Agent Context

## Overview

Elasticsearch is used in the Couchbase TestRunner in **two distinct ways**:

1. **FTS Query Validation** (Primary Use): ES serves as a reference/truth system to validate Couchbase FTS query results
2. **XDCR Replication**: ES acts as a replication destination for XDCR tests

This document covers the FTS Query Validation use case. See `pytests/xdcr/esXDCR.py` for XDCR-to-ES replication.

## Elasticsearch Cluster Requirements

### Prerequisites

Before running tests with `es_validate=True`, ensure:

```bash
# Elasticsearch must be installed and running
systemctl status elasticsearch   # or: /etc/init.d/elasticsearch status

# ES node must be accessible
curl http://<es_ip>:9200/

# Verify ES is responding
curl -X GET "http://<es_ip>:9200/_cluster/health?pretty"

# Sufficient memory allocation (adjust based on test load)
ES_JAVA_OPTS="-Xms4g -Xmx4g"    # Default 1GB may be insufficient for large tests

# Network connectivity from all test nodes
telnet <es_ip> 9200
ping <es_ip>
```

### Recommended ES Configuration

**ES Version:** 6.x, 7.x, or 8.x (multiple versions supported)

**Settings:**
```yaml
# elasticsearch.yml (recommended settings)
cluster.name: couchbase-testrunner
node.name: node-1

# Network
network.host: 0.0.0.0
http.port: 9200
transport.port: 9300

# Memory (adjust based on test data volume)
# Set to 50% of available RAM, max 31GB due to JVM pointer compression
bootstrap.memory_lock: false  # Set to true with ulimit for production

# Security (optional, not required by default)
xpack.security.enabled: false
# xpack.security.http.ssl.enabled: true  # If SSL enabled
# xpack.security.transport.ssl.enabled: true

# Index settings
index.max_result_window: 1000000  # Required for large result sets
index.number_of_shards: 1         # For consistency
index.number_of_replicas: 0       # For performance in tests
```

**JVM Heap Configuration:**
```bash
# Set in jvm.options or environment
-Xms4g
-Xmx4g
```

**System Requirements:**
- OS: Linux (Ubuntu, CentOS, RHEL)
- Java: OpenJDK 8 or 11
- RAM: 8GB minimum (4GB heap + 4GB system)
- Disk: At least 10GB free space for indices
- CPU: 2+ cores recommended

### Authentication (Optional)

Some test environments may require ES authentication:

```ini
[elastic]
ip:172.23.219.184
port:9200
es_username=elastic
es_password=changeme
```

Security can be enabled in ES:
```yaml
# elasticsearch.yml
xpack.security.enabled: true
xpack.security.authc.realms.native.native1:
  order: 0
```

## Test Configuration (INI Files)

ES configuration is specified in test `.ini` files:

```ini
[elastic]
ip:172.23.219.184
port:9200
es_username=      # optional, if ES requires authentication
es_password=      # optional, if ES requires authentication
```

**Es_validate parameter:** Set via test parameter, not INI file:
```bash
-t pytests.fts.fts_base.FTSBaseTest.test_fts_query_skip,es_validate=true
```

## How Tests Use Elasticsearch

### Test Workflow

```
1. FTSCallable.__init__(es_validate=True)
   └─> Reads [elastic] from INI file
   └─> HTTP connection test (is_running())
   └─> DELETE es_index (cleanup)
   └─> PUT es_index with BLEVE.STD_ANALYZER settings
   └─> enable_scroll(max_result_window: 1000000)

2. load_data()  # Called by test
   └─> Generate documents (e.g., "emp" template: name, email, dept, address)
   └─> Parallel load to both Couchbase and ES
   └─> ES: async_bulk_load_ES(index='es_index')
        └─> POST /es_index/_bulk (1000-10000 documents)

3. wait_for_indexing_complete()
   └─> Poll CB index count
   └─> Poll ES index count (if es_validate=True)
   └─> Wait for counts to match

4. run_query_and_compare()
   └─> Generate random queries (match, fuzzy, wildcard, regexp, etc.)
   └─> Run query on Couchbase FTS → get CB doc IDs
   └─> Run same query on ES → get ES doc IDs
   └─> Compare doc_id sets
   └─> Pass if match, Fail if mismatch

5. Operations During Test
   └─> Updates: load_data() with create_gen / update_gen
   └─> Deletes: load_data() with delete_gen
   └─> ES stays in sync via async_bulk_load_ES

6. Cleanup (delete_all())
   └─> DELETE all CB indices
   └─> DELETE es_index
```

### ES Index Settings Used

**Index Name:** Always `es_index` (hardcoded in FTSCallable)

**Analyzer Configuration:** BLEVE.STD_ANALYZER

```json
{
  "settings": {
    "analysis": {
      "analyzer": {
        "custom_standard_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["lowercase", "custom_stop_filter"]
        }
      },
      "filter": {
        "custom_stop_filter": {
          "type": "stop",
          "stopwords": ["i", "me", "my", "ourselves", "you", "yours", "he", "him", "his", "she", "her", "hers", "it", "its", "they", "them", "their", "theirs", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "would", "should", "could", "ought", "\"i'm\"", "\"you're\"", "\"he's\"", "\"she's\"", "\"it's\"", "\"we're\"", "\"they're\"", "\"i've\"", "\"you've\"", "\"we've\"", "\"they've\"", "\"i'd\"", "\"you'd\"", "\"he'd\"", "\"she'd\"", "\"we'd\"", "\"they'd\"", "\"i'll\"", "\"you'll\"", "\"he'll\"", "\"she'll\"", "\"we'll\"", "\"they'll\"", "\"isn't\"", "\"aren't\"", "\"wasn't\"", "\"weren't\"", "\"hasn't\"", "\"haven't\"", "\"hadn't\"", "\"doesn't\"", "\"don't\"", "\"didn't\"", "\"won't\"", "\"wouldn't\"", "\"shan't\"", "\"shouldn't\"", "\"can't\"", "\"cannot\"", "\"couldn't\"", "\"mustn't\"", "\"let's\"", "\"that's\"", "\"who's\"", "\"what's\"", "\"here's\"", "\"there's\"", "\"when's\"", "\"where's\"", "\"why's\"", "\"how's\"", "\"a\"", "\"an\"", "\"the\"", "\"and\"", "\"but\"", "\"if\"", "\"or\"", "\"because\"", "\"as\"", "\"until\"", "\"while\"", "\"of\"", "\"at\"", "\"by\"", "\"for\"", "\"with\"", "\"about\"", "\"against\"", "\"between\"", "\"into\"", "\"through\"", "\"during\"", "\"before\"", "\"after\"", "\"above\"", "\"below\"", "\"to\"", "\"from\"", "\"up\"", "\"down\"", "\"in\"", "\"out\"", "\"on\"", "\"off\"", "\"over\"", "\"under\"", "\"again\"", "\"further\"", "\"then\"", "\"once\"", "\"here\"", "\"there\"", "\"when\"", "\"where\"", "\"why\"", "\"how\"", "\"all\"", "\"any\"", "\"both\"", "\"each\"", "\"few\"", "\"more\"", "\"most\"", "\"other\"", "\"some\"", "\"such\"", "\"no\"", "\"nor\"", "\"not\"", "\"only\"", "\"own\"", "\"same\"", "\"so\"", "\"than\"", "\"too\"", "\"very\""]
        }
      },
      "tokenizer": {
        "standard": {
          "type": "standard"
        }
      }
    },
    "index": {
      "max_ngram_diff": 2
    },
    "index.max_shingle_diff": 4
  }
}
```

### ES API Operations Summary

| Operation | HTTP Method | Endpoint | Purpose |
|-----------|-------------|----------|---------|
| **Health Check** | GET | `/{host}:9200/` | Verify ES is running |
| **Create Index** | PUT | `/{host}:9200/es_index` | Create with custom analyzer |
| **Delete Index** | DELETE | `/{host}:9200/es_index` | Cleanup before test |
| **Bulk Load** | POST | `/{host}:9200/es_index/_bulk` | Load 1000-10000+ documents |
| **Search** | POST | `/{host}:9200/es_index/_search?size=1000000` | Run full-text queries |
| **Get Count** | GET | `/{host}:9200/es_index/_count` | Verify document count |
| **Refresh Index** | POST | `/{host}:9200/es_index/_refresh` | Force refresh after updates |
| **Scroll Settings** | PUT | `/{host}:9200/es_index/_settings` | Increase max_result_window |

### Data Flow Example

```python
# Test code
fts_callable = FTSCallable(nodes=servers, es_validate=True)
fts_callable.load_data(10000)  # Load 10K docs
fts_callable.wait_for_indexing_complete()
fts_callable.run_query_and_compare(index=ft, num_queries=20)

# What happens:
# 1. ES connection initialized: http://172.23.219.184:9200/
# 2. 10K documents loaded in parallel:
#    - CB: KV operations
#    - ES: POST /es_index/_bulk (JSON documents)
# 3. 20 random queries generated:
#    - Query 1: {"match": {"name": "John"}}
#    - Query 2: {"bool": {"must": [{"match": {"dept": "Engineering"}}]}}
#    - ...
# 4. For each query:
#    - Run on CB FTS: POST /_fts/api/index/{name}/_search
#    - Run on ES: POST /es_index/_search
#    - Compare result doc_id sets
#    - Report pass/fail
```

## Test Files Using Elasticsearch

### FTS Tests (Primary)

| File | ES Purpose | es_validate Setting |
|------|------------|---------------------|
| `pytests/fts/fts_callable.py` | Core FTSCallable class | Parameter-driven |
| `pytests/fts/fts_base.py` | Base FTS test class, ES validation setup | **True** (most tests) |
| `pytests/fts/es_base.py` | ES HTTP client, all ES operations | N/A (utility) |
| `pytests/fts/stable_topology_fts.py` | FTS on stable topology | **True** |
| `pytests/fts/moving_topology_fts.py` | FTS during rebalance/failover | **True** |
| `pytests/fts/vector_moving_topology_fts.py` | Vector search topology tests | **False** (vector) |
| `pytests/fts/rbac_fts.py` | RBAC with FTS | **True** |

### Upgrade Tests

| File | ES Purpose | es_validate Setting |
|------|------------|---------------------|
| `pytests/upgrade/newupgradebasetest.py` | Pre/post-upgrade FTS validation | **True** |
| `pytests/upgrade/upgrade_tests.py` | Main upgrade test with FTS | **True** |
| `pytests/upgrade/upgrade_tests_collections.py` | Collection upgrade with FTS | Varies |
| `pytests/fts/upgrade_fts.py` | FTS-specific upgrade tests | **False** (upgrade-only) |

### Eventing Tests (FTS Integration)

| File | ES Purpose | es_validate Setting |
|------|------------|---------------------|
| `pytests/eventing/eventing_fts_query_support.py` | Eventing with FTS queries | **False** |
| `pytests/eventing/eventing_recovery.py` | Eventing recovery with FTS | **False** |
| `pytests/eventing/eventing_rebalance.py` | Eventing + FTS during rebalance | **False** |
| `pytests/eventing/eventing_security.py` | RBAC + Eventing + FTS | **False** |
| `pytests/eventing/eventing_failover.py` | Eventing failover with FTS | **False** |

### Serverless FTS Tests

| File | ES Purpose | es_validate Setting |
|------|------------|---------------------|
| `pytests/fts/serverless/sanity.py` | Elixir-based serverless FTS | **False** (Elixir) |
| `pytests/fts/serverless/throttling.py` | FTS throttling tests | **False** |
| `pytests/fts/serverless/pause_resume.py` | FTS pause/resume | **False** |
| `pytests/fts/serverless/metering.py` | FTS metering validation | **False** |

### XDCR to Elasticsearch Tests

| File | ES Purpose | Type |
|------|------------|------|
| `pytests/xdcr/esXDCR.py` | XDCR replication to ES | Separate feature |
| `pytests/xdcr/esbasetests.py` | ES document verification | Separate feature |
| `lib/membase/api/esrest_client.py` | ES REST API client for XDCR | Utility |

### Other Tests

| File | ES Purpose |
|------|------------|
| `pytests/ent_backup_restore/enterprise_backup_restore_test.py` | FTS index backup/restore |
| `pytests/ent_backup_restore/backup_service_test.py` | Backup with FTS |
| `pytests/security/ntonencryptionTests.py` | N2N encryption with FTS |
| `pytests/gsi/base_gsi.py` | GSI with ES references |

## INI Templates with ES Configuration

### FTS-Specific Templates

```bash
b/resources/1-node-fts-cust-map-template.ini
b/resources/2-nodes-fts-es-template-new.ini
b/resources/3-nodes-fts-es-template-new.ini
b/resources/3-nodes-fts-es7-template.ini
b/resources/b/resources/rqg/2-nodes-fts-rqgp0-template.ini
b/resources/b/resources/rqg/3-node-fts-n1ql-rqg-template-new-elastic.ini
```

### Upgrade Templates with ES

```bash
b/resources/5-nodes-template-os_certify-upgrade.ini
b/resources/8-nodes-template-upgrade-with-fts.ini
b/resources/4-nodes-template-upgrade.ini
b/resources/4-nodes-template-upgrade-community.ini
```

### FTS INI Directory

```bash
b/resources/fts/ini/2-node.ini
b/resources/fts/ini/3-node.ini
b/resources/fts/ini/4-node.ini
b/resources/fts/ini/5-node.ini
b/resources/fts/ini/6-node.ini
b/resources/fts/ini/8-node.ini
```

**Total:** 45+ INI files with `[elastic]` configuration

## Running Tests with ES

### Single Test

```bash
# FTS test with ES validation
python3 testrunner.py \
  -i b/resources/2-nodes-fts-es-template-new.ini \
  -t pytests.fts.fts_base.FTSBaseTest.test_fts_query_skip \
  ,num_collections=5,es_validate=true,index_type=fulltext-index

# Upgrade test with ES validation
python3 testrunner.py \
  -i b/resources/4-nodes-template-upgrade.ini \
  -t pytests.upgrade.upgrade_tests.UpgradeTests.test_upgrade \
  ,es_validate=true \
  ,initialize_events=create_fts_index_query_compare \
  ,after_events=run_fts_query_and_compare
```

### Full Suite

```bash
# Run full FTS suite with ES
python3 testrunner.py \
  -i b/resources/2-nodes-fts-es-template-new.ini \
  -c conf/fts/fts_sanity.conf \
  ,es_validate=true
```

## Common Issues and Troubleshooting

### Issue: "For ES result validation, pls add elastic search node in the .ini file."

**Cause:** `[elastic]` section missing in INI file or `es_validate=True` without ES config

**Fix:** Add to INI:
```ini
[elastic]
ip:172.23.219.184
port:9200
```

### Issue: "X out of 20 queries failed!"

**Cause:** Query results between CB FTS and ES don't match

**Possible reasons:**
- Analyzer configuration mismatch
- Index definition differences
- Tokenization behavior variations
- Query format differences

**Debug steps:**
```bash
# Check ES index mapping
curl -X GET "http://<es_ip>:9200/es_index/_mapping?pretty"

# Check document count
curl -X GET "http://<es_ip>:9200/es_index/_count?pretty"

# Run a test query on ES
curl -X POST "http://<es_ip>:9200/es_index/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match": {"name": "John"}}}'
```

### Issue: Index count never stabilizes

**Cause:** Indexing timeout or insufficient resources

**Fixes:**
- Increase `index_retry` parameter: `index_retry=30`
- Check FTS node health
- Verify ES has sufficient heap memory
- Check network latency

### Issue: Socket error connecting to ES

**Cause:** ES not running, network issues, or firewall blocking

**Fix:**
```bash
# Check if ES is running
curl http://<es_ip>:9200/

# Check network connectivity
telnet <es_ip> 9200
ping <es_ip>

# Check firewall
sudo iptables -L | grep 9200
```

### Issue: ES heap memory errors

**Cause:** Insufficient JVM heap for data volume

**Fix:**
```bash
# Increase heap in jvm.options
-Xms8g
-Xmx8g

# Or via environment
export ES_JAVA_OPTS="-Xms8g -Xmx8g"
```

## ES Cluster: What Code Does NOT Manage

| Operation | Code Does It? | Notes |
|-----------|---------------|-------|
| Create ES cluster | ❌ NO | Assumes existing cluster |
| Initialize ES cluster | ❌ NO | Assumes cluster is running |
| Configure cluster settings | ❌ NO | Uses default settings |
| Add/remove ES nodes | ❌ NO | Assumes static node list |
| Set up shards/replicas | ❌ NO | Uses ES defaults |
| Create ES indices | ✅ YES | Within existing cluster (es_index only) |
| Delete ES indices | ✅ YES | Cleanup operations (es_index only) |
| Restart ES service | ✅ YES | Optional per-node restart (FTSBaseTest) |
| Load data to ES | ✅ YES | Via HTTP bulk API |
| Check ES health | ✅ YES | Health check only |

**Bottom Line:** The test runner does **not** provision or manage ES clusters. It only manages **indices** within an existing, pre-configured ES cluster.

## Validation Commands

### Verify ES Cluster

```bash
# Cluster health
curl -X GET "http://<es_ip>:9200/_cluster/health?pretty"

# Node info
curl -X GET "http://<es_ip>:9200/_cat/nodes?v"

# Index stats
curl -X GET "http://<es_ip>:9200/_cat/indices?v"
```

### Verify Test Index

```bash
# Check es_index exists
curl -X GET "http://<es_ip>:9200/es_index?pretty"

# Get document count
curl -X GET "http://<es_ip>:9200/es_index/_count?pretty"

# Get index mapping
curl -X GET "http://<es_ip>:9200/es_index/_mapping?pretty"

# Search all docs
curl -X POST "http://<es_ip>:9200/es_index/_search?pretty&size=10"
```

### Clean ES Index

```bash
# Delete es_index
curl -X DELETE "http://<es_ip>:9200/es_index"

# Check if deleted
curl -X GET "http://<es_ip>:9200/_cat/indices?v"
```

## References

- **Elasticsearch Documentation:** https://www.elastic.co/guide/en/elasticsearch/reference/
- **FTS Callable:** `pytests/fts/fts_callable.py`
- **ES Base Client:** `pytests/fts/es_base.py`
- **FTS Base Tests:** `pytests/fts/fts_base.py`
- **Upgrade Tests:** `pytests/upgrade/newupgradebasetest.py`
- **Eventing Tests:** `pytests/eventing/eventing_*.py`
- **XDCR to ES:** `pytests/xdcr/esXDCR.py`
