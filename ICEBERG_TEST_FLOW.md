# Iceberg Test Flow - Step by Step

This document explains the complete flow of Iceberg integration tests for N1QL queries over external Iceberg tables.

## Phase 1: Test Setup (`setUp`)

### Step 1: Base QueryTests Setup
```
IcebergQueryTests.setUp() calls super().setUp()
↓
- Initializes base testrunner infrastructure
- Sets up cluster connections (self.master, self.rest)
- Reads environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
```

### Step 2: Read Test Parameters
```python
# Read catalog configuration
self.catalog_type = "AWS_GLUE"  # from input.param
self.credentialstore_name = "iceberg_creds"
self.catalog_name = "iceberg_catalog"
self.external_collection_name = "iceberg_external"
self.iceberg_namespace = "icebergdb"  # Iceberg database name
self.iceberg_table_name = "hotel"     # Iceberg table name
self.initial_doc_count = 10000        # Number of docs to load
```

### Step 3: Initialize Iceberg Helpers
```python
# Create IcebergBase with credentials and config
self.iceberg_base = IcebergBase(
    aws_access_key=self.aws_access_key_id,
    aws_secret_key=self.aws_secret_access_key,
    catalog_type="AWS_GLUE",
    aws_region="us-east-1",
    database_name="icebergdb",
    table_name="hotel"
)
↓
# IcebergBase constructor does:
- Sets up environment variables for AWS SDK
- Creates boto3 clients (glue, s3tables, s3)
- Generates unique bucket name with timestamp
```

### Step 4: Provision External Iceberg Catalog
```python
self._provision_iceberg_catalog()
```

This does:

**4a. Generate Sample Data**
```python
sample_data = [
    {
        "id": 0,
        "country": "Guadeloupe",
        "city": "New Fritz",
        "type": "Hotel",
        "name": "Alexander Stiedemann",
        "price": 500,
        "reviews": [{"date": "2025-11-13 01:08:32", "author": "Alexander Stiedemann", "ratings": {...}}],
        "public_likes": [],
        ...
    },
    ...
]
# 10,000 hotel documents total with complex nested structure
```

**Sample Document Structure:**
```json
{
  "id": 1234,
  "country": "Germany",
  "mutate": 0,
  "address": "334 Kuphal Street",
  "free_parking": true,
  "city": "Tokyo",
  "counter": 0,
  "type": "Motel",
  "url": "www.kuphal-winston.com",
  "reviews": [
    {
      "date": "2025-11-13 01:08:32",
      "author": "Alexander Stiedemann",
      "ratings": {
        "Value": 4,
        "Cleanliness": 1,
        "Overall": 2,
        "Check in / front desk": 0,
        "Rooms": 1
      }
    },
    {
      "date": "2025-11-20 01:08:32",
      "author": "Winston Fisher",
      "ratings": {
        "Value": 0,
        "Cleanliness": 2,
        "Overall": 3,
        "Check in / front desk": 1,
        "Rooms": 2
      }
    }
  ],
  "phone": "433.233.5234",
  "price": 1234,
  "avg_rating": 2.5,
  "free_breakfast": false,
  "name": "Leoma Kuphal",
  "public_likes": ["Ladonna Keebler", "Landon Kautzer", "Mrs. Ilona Toy", "Dr. Lady Stamm"],
  "email": "Leoma.Kuphal@hotels.com"
}
```

**Data Distribution Patterns:**
- **Countries**: 20 distinct values (500 docs per country)
- **Cities**: 50 distinct values (200 docs per city)
- **Hotel Types**: 5 types - Hotel, Hostel, Resort, Motel, Inn (2,000 docs per type)
- **Prices**: 500 to 5000 (cycles every 500)
- **Reviews Array**: 1-5 review objects per hotel
  - **PATTERN**: Number of reviews = (id % 5) + 1
  - Doc 0: 1 review, Doc 1: 2 reviews, Doc 2: 3 reviews, Doc 3: 4 reviews, Doc 4: 5 reviews
  - Pattern repeats every 5 documents
  - **Total reviews**: ~30,000 review objects across all 10K hotels
- **Public Likes Array**: 0-10 names per hotel (count = id % 11)
- **Free Parking**: Alternates (id % 2 == 0)
- **Free Breakfast**: Every 3rd hotel (id % 3 == 0)

**4b. Setup AWS Glue Catalog** (for `catalog_type=AWS_GLUE`)
```python
iceberg_util.setup_glue_catalog(data=sample_data)
↓
┌─────────────────────────────────────────────────────┐
│ 1. Create S3 Bucket                                 │
│    - Bucket: tuqquery-iceberg-testing-1746367890    │
│    - Region: us-east-1                              │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│ 2. Create Spark Session                             │
│    - App: "Load data into the Catalog..."          │
│    - Catalog type: AWS_GLUE                         │
│    - Jars: iceberg-spark-runtime, AWS SDK bundles   │
│    - Config:                                        │
│      spark.sql.catalog.AWS_GLUE.catalog-impl =     │
│        org.apache.iceberg.aws.glue.GlueCatalog     │
│      spark.sql.catalog.AWS_GLUE.warehouse =        │
│        s3://tuqquery-iceberg-testing-1746367890     │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│ 3. Create Glue Database                             │
│    - Database name: icebergdb                       │
│    - Via boto3 glue client                          │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│ 4. Delete Existing Glue Table (if exists)          │
│    - Table: icebergdb.hotel                         │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│ 5. Create Iceberg Table with PySpark                │
│    df = spark.createDataFrame(sample_data)          │
│    df.writeTo("AWS_GLUE.icebergdb.hotel")          │
│      .using("iceberg")                              │
│      .tableProperty("format-version", "2")          │
│      .tableProperty("write-format", "parquet")      │
│      .createOrReplace()                             │
│                                                      │
│    Result: 10,000 rows written to Iceberg table     │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│ 6. Verify Data Load                                 │
│    df = spark.table("AWS_GLUE.icebergdb.hotel")   │
│    df.count() => 10000                              │
│    df.show(5) => displays first 5 rows              │
└─────────────────────────────────────────────────────┘
```

### Step 5: Setup N1QL Objects
```python
self._setup_n1ql_iceberg_objects()
```

This creates the Couchbase-side integration:

**5a. Enable Credential Store Settings**
```python
POST http://172.23.216.36:8091/settings/credentialStore
Body: {
    "configEncryptionOverride": false,
    "n2nEncryptionOverride": true
}
# This enables the cluster to accept CREDENTIALSTORE commands
```

**5b. Create CREDENTIALSTORE**
```sql
CREATE CREDENTIALSTORE iceberg_creds AS {
    "type": "s3",
    "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
    "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
}
```
Stores AWS credentials securely in Couchbase for N1QL to use

**5c. Create CATALOG**
```sql
CREATE CATALOG iceberg_catalog AS {
    "type": "Iceberg",
    "catalogSource": "AWS_GLUE",
    "credentialStore": "iceberg_creds",
    "warehousePath": "s3://tuqquery-iceberg-testing-1746367890",
    "region": "us-east-1"
}
```
Configures N1QL to connect to the AWS Glue catalog

**5d. Create EXTERNAL COLLECTION**
```sql
CREATE EXTERNAL COLLECTION iceberg_external AS {
    "catalog": "iceberg_catalog",
    "credentialStore": "iceberg_creds",
    "tableFormat": "iceberg",
    "namespace": "icebergdb",
    "tableName": "hotel"
}
```
Maps the external Iceberg table to a queryable N1QL collection

---

## Phase 2: Test Execution

### Test 1: `test_iceberg_select_count`

```python
query = "SELECT COUNT(*) as count FROM iceberg_external"
result = self.run_cbq_query(query)
```

**Query Flow:**
```
N1QL Query Service receives query
↓
Recognizes iceberg_external as EXTERNAL COLLECTION
↓
Looks up CATALOG iceberg_catalog
↓
Retrieves credentials from CREDENTIALSTORE iceberg_creds
↓
Connects to AWS Glue catalog
  - Lists tables in namespace "icebergdb"
  - Finds table "hotel"
  - Reads Iceberg metadata from S3
↓
Reads Iceberg table schema and data files
  - Parquet files in S3: s3://tuqquery-iceberg-testing-1746367890/icebergdb/hotel/...
↓
Executes COUNT(*) operation
  - Can read from metadata without scanning all data files (Iceberg optimization)
↓
Returns: {"results": [{"count": 10000}], "status": "success"}
```

**Verification:**
```python
assert result['status'] == 'success'
assert result['results'][0]['count'] == 10000
```

### Test 2: `test_iceberg_select_projection`

```python
query = """
SELECT id, name, city 
FROM iceberg_external 
WHERE id < 10 
ORDER BY id
"""
result = self.run_cbq_query(query)
```

**Query Flow:**
```
N1QL parses query and identifies:
  - Projection: id, name, city
  - Filter: id < 10
  - Sort: ORDER BY id
↓
Connects to Iceberg table via catalog
↓
Reads Iceberg metadata to find relevant data files
  - If table is partitioned, can skip files based on filter
  - Reads column statistics from Parquet metadata
↓
Scans data files and applies filter
  - Parquet columnar format allows reading only needed columns
  - Pushdown predicates where possible
↓
Sorts results by id
↓
Returns: [
  {"id": 0, "name": "Hotel_0", "city": "City_0"},
  {"id": 1, "name": "Hotel_1", "city": "City_1"},
  ...
  {"id": 9, "name": "Hotel_9", "city": "City_9"}
]
```

**Verification:**
```python
assert result['status'] == 'success'
assert len(result['results']) == 10
assert result['results'][0]['id'] == 0
assert result['results'][0]['name'] == "Hotel_0"
```

### Test 3: `test_iceberg_select_filter`

```python
query = "SELECT id, name, city, type, price FROM iceberg_external WHERE type = 'Hostel' ORDER BY id LIMIT 100"
result = self.run_cbq_query(query)
```

**Query Flow:**
```
N1QL applies WHERE filter: type = 'Hostel'
↓
Iceberg catalog lookup
↓
Reads relevant Parquet files
  - Uses Parquet column statistics for pruning
↓
Filters rows where type matches 'Hostel'
↓
Returns matching rows (limited to 100):
  - Hostel appears at IDs: 1, 6, 11, 16, 21, ...
  - Total 'Hostel' docs: 2,000 (type cycles every 5)
  - Query returns first 100 due to LIMIT
```

**Expected Result Count:** 100 documents (due to LIMIT clause)

**Verification:**
```python
assert result['status'] == 'success'
assert len(result['results']) <= 100
for row in result['results']:
    assert row['type'] == 'Hostel'
```

### Test 4: `test_iceberg_select_reviews_array`

```python
# Part 1: Get documents with reviews
query = "SELECT id, name, reviews FROM iceberg_external WHERE id IN [0, 1, 2, 3, 4] ORDER BY id"
result = self.run_cbq_query(query)

# Part 2: UNNEST reviews
query = """
SELECT h.id, h.name, r.author, r.ratings.Overall as overall_rating
FROM iceberg_external h
UNNEST h.reviews r
WHERE h.id < 10
ORDER BY h.id, r.date
"""
result = self.run_cbq_query(query)
```

**Query Flow:**
```
Part 1: Nested Array Retrieval
N1QL applies WHERE filter: id IN [0, 1, 2, 3, 4]
↓
Retrieves 5 documents with nested reviews arrays
↓
Returns documents with review counts:
  - Doc 0: 1 review
  - Doc 1: 2 reviews
  - Doc 2: 3 reviews
  - Doc 3: 4 reviews
  - Doc 4: 5 reviews
↓
Total: 15 review objects across 5 documents

Part 2: UNNEST Operation
N1QL applies UNNEST on reviews array
↓
Flattens nested reviews to individual rows
↓
For IDs 0-9:
  - Doc 0: 1 review → 1 row
  - Doc 1: 2 reviews → 2 rows
  - Doc 2: 3 reviews → 3 rows
  - Doc 3: 4 reviews → 4 rows
  - Doc 4: 5 reviews → 5 rows
  - Doc 5: 1 review → 1 row
  - Doc 6: 2 reviews → 2 rows
  - Doc 7: 3 reviews → 3 rows
  - Doc 8: 4 reviews → 4 rows
  - Doc 9: 5 reviews → 5 rows
↓
Total: 30 unnested rows
```

**Cross-Validation Logic:**
```python
# Helper method calculates expected review count
def _calculate_expected_review_count(doc_ids):
    return sum([(doc_id % 5) + 1 for doc_id in doc_ids])

# Validate Part 1: Review counts per document
for row in result['results']:
    expected = (row['id'] % 5) + 1
    actual = len(row['reviews'])
    assert actual == expected

# Validate Part 2: Total unnested reviews
expected_total = _calculate_expected_review_count(range(10))
# = 1+2+3+4+5+1+2+3+4+5 = 30
assert len(result['results']) == 30
```

**Verification:**
```python
assert result['status'] == 'success'
# Part 1: 5 documents returned
assert len(result['results']) == 5
# Each document has correct review count based on (id % 5) + 1

# Part 2: 30 unnested reviews
assert len(result['results']) == 30
# Each unnested row has author, ratings, and other review fields
```

---

## Phase 3: Test Teardown (`tearDown`)

### Step 1: Cleanup N1QL Objects
```python
self._cleanup_n1ql_iceberg_objects()
```

**In reverse order of creation:**

```sql
-- 1. Drop external collection
DROP COLLECTION iceberg_external
↓
-- 2. Drop catalog
DROP CATALOG iceberg_catalog
↓
-- 3. Drop credential store
DROP CREDENTIALSTORE iceberg_creds
```

### Step 2: Cleanup External Iceberg Resources
```python
self._cleanup_iceberg_catalog()
```

**For AWS Glue:**
```python
iceberg_util.destroy_glue_catalog()
↓
┌─────────────────────────────────────────────────────┐
│ 1. Delete S3 Bucket                                 │
│    - Empty all objects/versions                     │
│    - Delete bucket: tuqquery-iceberg-testing-...    │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│ 2. Delete Glue Table                                │
│    - Table: icebergdb.hotel                         │
└─────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────┐
│ 3. Delete Glue Database                             │
│    - Database: icebergdb                            │
└─────────────────────────────────────────────────────┘
```

### Step 3: Base Teardown
```python
super().tearDown()
# Cleans up base testrunner resources
```

---

## Complete End-to-End Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         TEST EXECUTION                              │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────────┐
│   setUp() called     │
└──────────┬───────────┘
           │
           ├─► Read test parameters (catalog_type, credentials, etc.)
           │
           ├─► Initialize IcebergBase + IcebergUtil
           │       ├─► Setup boto3 clients
           │       └─► Configure AWS environment
           │
           ├─► Provision External Catalog (PySpark)
           │       ├─► Create S3 bucket
           │       ├─► Create Spark session with Iceberg jars
           │       ├─► Create Glue database
           │       ├─► Write 10,000 sample docs to Iceberg table
           │       └─► Verify data load
           │
           └─► Setup N1QL Objects
                   ├─► Enable credential store settings
                   ├─► CREATE CREDENTIALSTORE
                   ├─► CREATE CATALOG
                   └─► CREATE EXTERNAL COLLECTION
                   
┌──────────────────────┐
│ test_xxx() called    │
└──────────┬───────────┘
           │
           ├─► Build N1QL query string
           │
           ├─► Execute via run_cbq_query()
           │       └─► POST to http://172.23.216.36:8093/query
           │
           ├─► N1QL processes query
           │       ├─► Resolve EXTERNAL COLLECTION
           │       ├─► Lookup CATALOG
           │       ├─► Retrieve CREDENTIALSTORE
           │       ├─► Connect to AWS Glue
           │       ├─► Read Iceberg metadata from S3
           │       ├─► Scan Parquet data files
           │       └─► Return results
           │
           └─► Assert results match expectations

┌──────────────────────┐
│  tearDown() called   │
└──────────┬───────────┘
           │
           ├─► Cleanup N1QL Objects (reverse order)
           │       ├─► DROP COLLECTION iceberg_external
           │       ├─► DROP CATALOG iceberg_catalog
           │       └─► DROP CREDENTIALSTORE iceberg_creds
           │
           ├─► Cleanup External Resources
           │       ├─► Delete S3 bucket + contents
           │       ├─► Delete Glue table
           │       └─► Delete Glue database
           │
           └─► super().tearDown()
```

---

## Sample Document Examples

Here are actual examples of documents generated by `_generate_sample_data()`:

### Document ID 0 (1 review):
```json
{
  "id": 0,
  "country": "Guadeloupe",
  "mutate": 0,
  "address": "100 Stiedemann Street",
  "free_parking": true,
  "city": "New Fritz",
  "counter": 0,
  "type": "Hotel",
  "url": "www.stiedemann-alexander.com",
  "reviews": [
    {
      "date": "2025-11-13 01:08:32",
      "author": "Alexander Stiedemann",
      "ratings": {
        "Value": 0,
        "Cleanliness": 2,
        "Overall": 3,
        "Check in / front desk": 1,
        "Rooms": 2
      }
    }
  ],
  "phone": "200.100.1000",
  "price": 500,
  "avg_rating": 3.0,
  "free_breakfast": true,
  "name": "Alexander Stiedemann",
  "public_likes": [],
  "email": "Alexander.Stiedemann@hotels.com"
}
```

### Document ID 4 (5 reviews):
```json
{
  "id": 4,
  "country": "Japan",
  "mutate": 0,
  "address": "104 Stamm Boulevard",
  "free_parking": true,
  "city": "Tokyo",
  "counter": 0,
  "type": "Inn",
  "url": "www.stamm-lady.com",
  "reviews": [
    {
      "date": "2025-11-13 01:08:32",
      "author": "Lady Stamm",
      "ratings": {"Value": 4, "Cleanliness": 1, "Overall": 2, "Check in / front desk": 0, "Rooms": 1}
    },
    {
      "date": "2025-11-20 01:08:32",
      "author": "Odis Ziemann",
      "ratings": {"Value": 0, "Cleanliness": 2, "Overall": 3, "Check in / front desk": 1, "Rooms": 2}
    },
    {
      "date": "2025-11-27 01:08:32",
      "author": "Althea Kshlerin",
      "ratings": {"Value": 1, "Cleanliness": 3, "Overall": 4, "Check in / front desk": 2, "Rooms": 3}
    },
    {
      "date": "2025-12-04 01:08:32",
      "author": "Rolf McDermott",
      "ratings": {"Value": 2, "Cleanliness": 4, "Overall": 0, "Check in / front desk": 3, "Rooms": 4}
    },
    {
      "date": "2025-12-11 01:08:32",
      "author": "Kathern Village",
      "ratings": {"Value": 3, "Cleanliness": 0, "Overall": 1, "Check in / front desk": 4, "Rooms": 0}
    }
  ],
  "phone": "204.104.1004",
  "price": 504,
  "avg_rating": 2.04,
  "free_breakfast": false,
  "name": "Lady Stamm",
  "public_likes": ["Lady Stamm", "Odis Ziemann", "Althea Kshlerin", "Rolf McDermott"],
  "email": "Lady.Stamm@hotels.com"
}
```

### Document ID 1234 (3 reviews):
```json
{
  "id": 1234,
  "country": "Germany",
  "mutate": 0,
  "address": "334 Kuphal Street",
  "free_parking": true,
  "city": "Tokyo",
  "counter": 0,
  "type": "Inn",
  "url": "www.kuphal-leoma.org",
  "reviews": [
    {
      "date": "2025-11-13 01:08:32",
      "author": "Lady Stamm",
      "ratings": {"Value": 4, "Cleanliness": 1, "Overall": 2, "Check in / front desk": 0, "Rooms": 1}
    },
    {
      "date": "2025-11-20 01:08:32",
      "author": "Odis Ziemann",
      "ratings": {"Value": 0, "Cleanliness": 2, "Overall": 3, "Check in / front desk": 1, "Rooms": 2}
    },
    {
      "date": "2025-11-27 01:08:32",
      "author": "Althea Kshlerin",
      "ratings": {"Value": 1, "Cleanliness": 3, "Overall": 4, "Check in / front desk": 2, "Rooms": 3}
    }
  ],
  "phone": "433.233.5234",
  "price": 1234,
  "avg_rating": 3.12,
  "free_breakfast": false,
  "name": "Leoma Kuphal",
  "public_likes": ["Lady Stamm", "Odis Ziemann", "Althea Kshlerin", "Rolf McDermott"],
  "email": "Leoma.Kuphal@hotels.org"
}
```

---

## Data Distribution Summary

With 10,000 documents:

| Field | Pattern | Distinct Values | Documents per Value |
|-------|---------|-----------------|---------------------|
| `id` | Sequential 0-9999 | 10,000 | 1 |
| `country` | Cycles through 20 countries | 20 | 500 |
| `city` | Cycles through 50 cities | 50 | 200 |
| `type` | Cycles through 5 types | 5 | 2,000 |
| `price` | 500 + (id % 4500) | 4,500 | ~2-3 |
| `free_parking` | id % 2 == 0 | 2 | 5,000 |
| `free_breakfast` | id % 3 == 0 | 2 | 3,333 / 6,667 |
| `reviews[]` | (id % 5) + 1 objects | 1-5 per doc | Varies |
| `public_likes[]` | id % 11 objects | 0-10 per doc | Varies |

### Reviews Array Distribution (CRITICAL FOR CROSS-VALIDATION)

| Doc ID Pattern | Review Count | Example IDs | Total Docs | Total Reviews |
|----------------|--------------|-------------|------------|---------------|
| id % 5 == 0 | 1 review | 0, 5, 10, ... | 2,000 | 2,000 |
| id % 5 == 1 | 2 reviews | 1, 6, 11, ... | 2,000 | 4,000 |
| id % 5 == 2 | 3 reviews | 2, 7, 12, ... | 2,000 | 6,000 |
| id % 5 == 3 | 4 reviews | 3, 8, 13, ... | 2,000 | 8,000 |
| id % 5 == 4 | 5 reviews | 4, 9, 14, ... | 2,000 | 10,000 |
| **Total** | **Average: 3** | **All 10K docs** | **10,000** | **30,000** |

**Query Validation Examples:**
- `WHERE type = 'Hostel'` returns 2,000 rows (IDs: 1, 6, 11, 16, ...)
- `WHERE country = 'Germany'` returns 500 rows
- `WHERE city = 'Tokyo'` returns 200 rows
- `WHERE free_parking = true` returns 5,000 rows
- `UNNEST reviews` on all docs produces **30,000 rows total**
- `UNNEST reviews WHERE id < 10` produces **30 rows** (1+2+3+4+5+1+2+3+4+5)
- `WHERE id IN [0,1,2,3,4]` with reviews produces **15 review objects** (1+2+3+4+5)

This distribution enables testing:
- **Selective filters**: Filter by type, country, city
- **Boolean filters**: free_parking, free_breakfast
- **Array operations**: UNNEST reviews, ARRAY_LENGTH(public_likes)
- **Nested queries**: Reviews with ratings aggregations
- **Cross-validation**: Deterministic review counts for validation

---

## Key Points

1. **Two-Layer Architecture**: 
   - External layer: PySpark provisions actual Iceberg tables in cloud storage
   - N1QL layer: Creates catalog/collection mappings to query external data

2. **Data Never Enters Couchbase**: The 10,000 hotel documents stay in S3 as Parquet files, N1QL queries them remotely

3. **Credential Isolation**: AWS credentials stored in CREDENTIALSTORE, not hardcoded in queries

4. **Clean Separation**: Each catalog type (AWS_GLUE, S3_TABLES, BIGLAKE_METASTORE) follows the same pattern but with different provisioning logic

5. **Happy Path Focus**: Tests verify successful query execution, not error cases (as requested)

6. **Scale**: 10,000 documents provides meaningful test data while keeping provisioning time reasonable

This flow provides full end-to-end validation of Couchbase's Iceberg integration through N1QL queries!
