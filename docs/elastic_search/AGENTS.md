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

### Test Workflow (Parallel Job Support)

```
1. FTSCallable.__init__(es_validate=True) OR FTSBaseTest.setUp()
   └─> Reads [elastic] from INI file
   └─> HTTP connection test (is_running())
   └─> Generate unique ES index name per job (shared across all tests):
        Format: {uuid_short}_{ddmmmyy_hh_mm_ss}
        Example: "a3f4e2c1_28mar26_14_59_01"
        Implementation: class-level FTSBaseTest.es_index_name
        └─> FTSBaseTest.setUp() checks if cls.es_index_name is None
        └─> If None: generates and stores as class attribute (lowercased for ES compliance)
        └─> If exists: reuses (subsequent tests in same job)
   └─> DELETE {index_name} (cleanup)
   └─> PUT {index_name} with BLEVE.STD_ANALYZER settings
   └─> enable_scroll(max_result_window: 1000000)

2. load_data()  # Called by test
   └─> Generate documents (e.g., "emp" template: name, email, dept, address)
   └─> Parallel load to both Couchbase and ES
   └─> ES: async_bulk_load_ES(index=FTSBaseTest.es_index_name)
        └─> POST /{index_name}/_bulk (1000-10000 documents)

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
   └─> DELETE FTSBaseTest.es_index_name

Job Execution Model:
- Job-A, Test-1: FTSBaseTest.es_index_name = "a3f4e2c1_28mar26_10_00_01"
- Job-A, Test-2: Uses same FTSBaseTest.es_index_name = "a3f4e2c1_28mar26_10_00_01"
- Job-B (parallel), Test-1: FTSBaseTest.es_index_name = "b8d5f9e3_28mar26_10_01_02"
- Job-B, Test-2: Uses same FTSBaseTest.es_index_name = "b8d5f9e3_28mar26_10_01_02"

FTSCallable Mode:
- FTSCallable(nodes, es_validate=True) → generates its own unique es_index_name (lowercased)
- FTSCallable(nodes, es_validate=True, es_index_name="custom") → uses provided name
- FTSCallable can share name with FTSBaseTest: FTSCallable(..., es_index_name=FTSBaseTest.es_index_name)
```

### Unique Index Naming (Job-Level)

**Index Name Format:** `{uuid_short}_{ddmmmyy_hh_mm_ss}`

**Examples:**
- `a3f4e2c1_28mar26_14_59_01`
- `b8d5f9e3_28mar26_15_02_23`
- `c7e4a8d2_28mar26_16_05_47`

**Components:**
- `uuid_short`: First 8 chars of UUID4 for uniqueness across jobs
- `ddmmmyy_hh_mm_ss`: Human-readable timestamp for cleanup tracking (day, 3-char lowercase month name, 2-digit year, hour, minute, second)
- **Note:** Elasticsearch requires all index names to be lowercase. The generation method automatically applies `.lower()` to ensure compliance.

**Purpose:** 
- **Job-Level Sharing**: Each parallel job generates a unique ES index name
- **Code-Reuse Isolation**: Tests within the same job share the same index unless explicitly customized
- **Conflict Prevention**: Different automated jobs running simultaneously on the same ES cluster don't conflict

**Implementation:**

```python
# In FTSBaseTest (pytests/fts/fts_base.py)
class FTSBaseTest(unittest.TestCase):
    es_index_name = None  # Class-level attribute
    
    def setUp(self):
        if self.compare_es:
            # Generate unique ES index name per job (shared across all tests in job)
            if FTSBaseTest.es_index_name is None:
                timestamp = datetime.datetime.now().strftime("%d%b%y_%H_%M_%S")
                FTSBaseTest.es_index_name = f"{uuid.uuid4().hex[:8]}_{timestamp}".lower()
                self.log.info(f"Job-level unique ES index generated: {FTSBaseTest.es_index_name}")
            else:
                self.log.info(f"Using existing job-level ES index: {FTSBaseTest.es_index_name}")
```

**Usage Patterns:**

1. **FTSBaseTest-based Tests** (Primary):
```python
# Job-A, Test-1 setUp: generates FTSBaseTest.es_index_name = "a3f4e2c1_28mar26_10_00_01"
# Job-A, Test-2 setUp: reuses FTSBaseTest.es_index_name
# Job-A, Test-3 setUp: reuses FTSBaseTest.es_index_name
```

2. **FTSCallable Standalone** (Separate process):
```python
# Always generates unique name unless es_index_name is passed
fts = FTSCallable(nodes, es_validate=True)
# Generates: "b8d5f9e3_28mar26_10_01_02"
```

3. **FTSCallable Shared with FTSBaseTest** (Same job):
```python
# In a test that uses both FTSBaseTest and FTSCallable:
# FTSBaseTest.setUp() generates: "a3f4e2c1_28mar26_10_00_01"
# Pass the same name to FTSCallable:
fts = FTSCallable(nodes, es_validate=True, es_index_name=FTSBaseTest.es_index_name)
# Now both use same ES index: "a3f4e2c1_28mar26_10_00_01"
```

### ES Index Settings Used

**Unique Index Naming - All ES Resources:**

All ES resources now use unique names to enable safe concurrent test execution on shared ES clusters. Names are derived from `FTSBaseTest.get_es_index_name()` which generates a job-level unique identifier.

**Primary Index Name:** `{UUID_short}_{DDMMMYY_HH_MM_SS}`

**Additional ES Resources:**
- Multi-bucket indexes: `{bucket}_es_index_{UUID_short}_{DDMMMYY_HH_MM_SS}`
- ES aliases: `{name}_es_alias_{UUID_short}_{DDMMMYY_HH_MM_SS}`
- Ingest pipelines: `polygonize_{UUID_short}_{DDMMMYY_HH_MM_SS}`

**Examples:**
- Main index: `a3f4e2c1_28mar26_14_59_01`
- Emp bucket index: `emp_es_index_a3f4e2c1_28mar26_14_59_01`
- Wiki bucket index: `wiki_es_index_a3f4e2c1_28mar26_14_59_01`
- Multi-bucket alias: `emp_wiki_es_alias_a3f4e2c1_28apr26_10_30_45`
- Ingest pipeline: `polygonize_a3f4e2c1_28mar26_14_59_01`

**Directive:** Use unique index names (e.g., with test ID suffix) to avoid conflicts when multiple tests run concurrently on the same ES cluster.

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
| **Create Index** | PUT | `/{host}:9200/{uuid_short}_{ddmmmyy_hh_mm_ss}` | Create with custom analyzer (unique per job, lowercased) |
| **Create Multi-Bucket Index** | PUT | `/{host}:9200/{bucket}_es_index_{uuid_short}_{ddmmmyy_hh_mm_ss}` | For multi-bucket tests |
| **Delete Index** | DELETE | `/{host}:9200/{name}` | Cleanup any ES resource |
| **Bulk Load** | POST | `/{host}:9200/{name}/_bulk` | Load 1000-10000+ documents |
| **Search** | POST | `/{host}:9200/{name}/_search?size=1000000` | Run full-text queries |
| **Get Count** | GET | `/{host}:9200/{name}/_count` | Verify document count |
| **Refresh Index** | POST | `/{host}:9200/{name}/_refresh` | Force refresh after updates |
| **Create Alias** | POST | `/{host}:9200/_alias/{name}_es_alias_{uuid_short}_{ddmmmyy_hh_mm_ss}` | Create ES alias |
| **Create Ingest Pipeline** | PUT | `/{host}:9200/_ingest/pipeline/polygonize_{uuid_short}_{ddmmmyy_hh_mm_ss}` | Create geo pipeline |
| **Scroll Settings** | PUT | `/{host}:9200/{name}/_settings` | Increase max_result_window |

**Note:** `{name}` represents any ES resource name (main index, multi-bucket index, alias).

### Benefits of Unique Index Naming

### Parallel Job Execution

Multiple test jobs can now run simultaneously on the same ES cluster:

```bash
# Job 1, Job 2, Job 3 running in parallel on same ES cluster
Job 1 → Index: a3f4e2c1_28mar26_14_59_01
Job 2 → Index: b8d5f9e3_28mar26_15_02_23
Job 3 → Index: c7e4a8d2_28mar26_16_05_47

# No conflicts - each job has its own isolated index
```

### Failed Job Debugging

If a job fails, you can inspect its specific index:

```bash
# Check the failed job's index
curl -X GET "http://<es_ip>:9200/a3f4e2c1_28mar26_14_59_01/_count?pretty"

# View documents in the failed job's index
curl -X POST "http://<es_ip>:9200/a3f4e2c1_28mar26_14_59_01/_search?pretty&size=10"
```

### Automated Cleanup for Stale Indices

Timestamp in index name enables easy cleanup:

```bash
# Find indices older than 72 hours
curl -s "http://<es_ip>:9200/_cat/indices?h=index" | \
  grep -P '^[a-f0-9]{8}_\d{2}[a-z]{3}\d{2}_\d{2}_\d{2}_\d{2}$' > /tmp/indices.txt

while read index_name; do
  # Extract date part from index name: <uuid>_<date>
  date_part=$(echo $index_name | cut -d'_' -f2)
  
  # Parse the date: ddmmmyy_hh_mm_ss (e.g., 28mar26_14_59_01) - lowercase for ES compliance
  if [[ $date_part =~ ^([0-9]{2})([A-Z][a-z]{2})([0-9]{2})_([0-9]{2})_([0-9]{2})_([0-9]{2})$ ]]; then
    day=${BASH_REMATCH[1]}
    month_name=${BASH_REMATCH[2]}
    year=${BASH_REMATCH[3]}
    hour=${BASH_REMATCH[4]}
    minute=${BASH_REMATCH[5]}
    second=${BASH_REMATCH[6]}
    
    # Convert month name to number (Jan=1, Feb=2, ..., Dec=12)
    declare -A months=(["Jan"]=01 ["Feb"]=02 ["Mar"]=03 ["Apr"]=04 ["May"]=05 ["Jun"]=06 
                       ["Jul"]=07 ["Aug"]=08 ["Sep"]=09 ["Oct"]=10 ["Nov"]=11 ["Dec"]=12)
    month=${months[$month_name]}

    # Create timestamp from parsed date (assume year 2000+ for YY)
    full_year="20${year}"
    index_ts=$(date -d "${full_year}-${month}-${day} ${hour}:${minute}:${second}" +%s 2>/dev/null || echo "0")
    CURRENT_TIME=$(date +%s)
    age_hours=$(( ($CURRENT_TIME - $index_ts) / 3600 ))

    if [ $age_hours -gt $MAX_AGE_DAYS ]; then
      echo "Deleting old index: $index_name (age: ${age_hours}h)"
      curl -s -X DELETE "http://${ES_HOST}:${ES_PORT}/$index_name"
    fi
  fi
done < /tmp/indices.txt
```

**Cleanup Pattern for Multi-Bucket Tests:**
```bash
# Find and delete multi-bucket ES resources (indexes and aliases)
curl -s "http://<es_ip>:9200/_cat/indices?h=index" | \
  grep -P '^[a-z]+_es_index_[a-f0-9]{8}_\d{2}[a-z]{3}\d{2}_\d{2}_\d{2}_\d{2}$' | \
  while read index_name; do
    echo "Deleting multi-bucket ES resource: $index_name"
    curl -s -X DELETE "http://${ES_HOST}:${ES_PORT}/$index_name"
  done

# Find and delete ES aliases
curl -s "http://<es_ip>:9200/_cat/aliases?h=alias,index" | \
  grep -P '^[a-z_]+_es_alias_[a-f0-9]{8}_\d{2}[a-z]{3}\d{2}_\d{2}_\d{2}_\d{2}' | \
  while read alias_name; do
    echo "Deleting ES alias: $alias_name (cleanup only, recreated per job)"
    # Note: ES aliases don't need explicit deletion if index is deleted
  done

# Find and delete ingest pipelines
curl -s "http://<es_ip>:9200/_ingest/pipeline" | \
  grep -oP '"polygonize_[a-f0-9]{8}_\d{2}[a-z]{3}\d{2}_\d{2}_\d{2}_\d{2}' | \
  while read pipeline_name; do
    echo "Deleting ES ingest pipeline: $pipeline_name"
    curl -s -X DELETE "http://${ES_HOST}:${ES_PORT}/_ingest/pipeline/$pipeline_name"
  done
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
# Find the running job's ES index (check test logs for index name)
# Example index name from logs: "a3f4e2c1_28mar26_14_59_01"

# Check ES index mapping
curl -X GET "http://<es_ip>:9200/{es_index_name}/_mapping?pretty"

# Check document count
curl -X GET "http://<es_ip>:9200/{es_index_name}/_count?pretty"

# Run a test query on ES
curl -X POST "http://<es_ip>:9200/{es_index_name}/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match": {"name": "John"}}}'

# For multi-bucket tests, check the specific bucket's index
curl -X GET "http://<es_ip>:9200/{bucket}_es_index_{UUID}_{timestamp}/_count?pretty"
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
| Create ES indices | ✅ YES | Within existing cluster (unique per job) |
| Create ES aliases | ✅ YES | Multi-bucket test support |
| Create ingest pipelines | ✅ YES | Geo processing support |
| Delete ES indices | ✅ YES | All ES resources cleanup |
| Delete ES aliases | ✅ YES | Cleanup operations |
| Delete ingest pipelines | ✅ YES | Cleanup operations |
| Restart ES service | ✅ YES | Optional per-node restart (FTSBaseTest) |
| Load data to ES | ✅ YES | Via HTTP bulk API |
| Check ES health | ✅ YES | Health check only |

**Bottom Line:** The test runner does **not** provision or manage ES clusters. It only manages **indices, aliases, and pipelines** within an existing, pre-configured ES cluster using unique job-level names for safe concurrent execution.

## Validation Commands

### Verify ES Cluster

```bash
# Cluster health
curl -X GET "http://<es_ip>:9200/_cluster/health?pretty"

# Node info
curl -X GET "http://<es_ip>:9200/_cat/nodes?v"

# List all indices (including test indices)
curl -X GET "http://<es_ip>:9200/_cat/indices?v"

# List all aliases (including multi-bucket test aliases)
curl -X GET "http://<es_ip>:9200/_cat/aliases?v"

# List all ingest pipelines (including geo processing pipelines)
curl -X GET "http://<es_ip>:9200/_ingest/pipeline?pretty"
```

### Verify Test Index

```bash
# Check specific index exists (replace {es_index_name} with actual name from logs)
curl -X GET "http://<es_ip>:9200/{es_index_name}?pretty"

# Get document count
curl -X GET "http://<es_ip>:9200/{es_index_name}/_count?pretty"

# Get index mapping
curl -X GET "http://<es_ip>:9200/{es_index_name}/_mapping?pretty"

# Search all docs
curl -X POST "http://<es_ip>:9200/{es_index_name}/_search?pretty&size=10"

# For multi-bucket tests, check bucket-specific indices
curl -X GET "http://<es_ip>:9200/{bucket}_es_index_{UUID}_{timestamp}?pretty"
curl -X GET "http://<es_ip>:9200/{name}_es_alias_{UUID}_{timestamp}?pretty"
```

### Clean ES Index

```bash
# Delete specific test index (replace with actual index name from logs)
curl -X DELETE "http://<es_ip>:9200/{es_index_name}"

# Delete all test indices matching the naming pattern
curl -X DELETE "http://<es_ip>:9200/{bucket}_es_index_{UUID}_{timestamp}"
curl -X DELETE "http://<es_ip>:9200/{name}_es_alias_{UUID}_{timestamp}"

# Delete ingest pipeline
curl -X DELETE "http://<es_ip>:9200/_ingest/pipeline/polygonize_{UUID}_{timestamp}"

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
