# Iceberg External Collection Tests

## Overview

`tuq_iceberg.py` tests N1QL queries over external Apache Iceberg tables stored in AWS S3. Tests provision Iceberg catalogs via PySpark, create Couchbase external collections pointing to them, and validate query operations.

## Prerequisites

- **AWS Credentials**: Set environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- **PySpark**: Installed via `requirements.txt`
- **Java 8+**: Required for Spark

## Running Tests

```bash
python testrunner.py -c conf/tuq/py-tuq-iceberg.conf
```

## Test Architecture

```
IcebergQueryTests (tuq_iceberg.py)
    └── setUp()
        ├── Create unique S3 bucket (tuqquery-iceberg-{timestamp}-{uuid})
        ├── Provision Iceberg table via PySpark + AWS Glue
        ├── Create Couchbase bucket/scope
        ├── CREATE CREDENTIALSTORE (AWS credentials)
        ├── CREATE CATALOG (Iceberg catalog)
        └── CREATE EXTERNAL COLLECTION (points to Iceberg table)
    └── test_*()
        └── Run N1QL queries on external collection
    └── tearDown()
        ├── DROP COLLECTION, CATALOG, CREDENTIALSTORE
        ├── Stop Spark session
        └── Delete S3 bucket
```

## Hotel Document Schema

The test loads 10,000 hotel documents into Iceberg with this structure:

```json
{
  "id": 0,
  "country": "Guadeloupe",
  "city": "New Fritz",
  "type": "Hotel",
  "name": "Alexander Stiedemann",
  "address": "100 Stiedemann Street",
  "phone": "200.100.1000",
  "email": "Alexander.Stiedemann@hotels.com",
  "url": "www.stiedemann-alexander.com",
  "price": 500,
  "avg_rating": 3.0,
  "free_breakfast": true,
  "free_parking": true,
  "counter": 0,
  "mutate": 0,
  "public_likes": ["Dr. Alexander Stiedemann"],
  "reviews": [
    {
      "date": "2025-11-13 01:08:32",
      "author": "Alexander Stiedemann",
      "ratings": {
        "Value": 0, "Cleanliness": 2, "Overall": 3,
        "Check in / front desk": 1, "Rooms": 2
      }
    }
  ]
}
```

**Data patterns for validation:**
- Reviews per doc: `(id % 5) + 1` (1-5 reviews)
- Public likes per doc: `id % 11` (0-10 names)
- Country: Cycles through 20 countries
- City: Cycles through 50 cities
- Type: Hotel, Hostel, Resort, Motel, Inn

## N1QL Syntax for Iceberg Objects

```sql
-- Create credential store for AWS
CREATE CREDENTIALSTORE iceberg_creds WITH {
  "type": "aws",
  "fields": {
    "accessKeyId": "...",
    "secretAccessKey": "...",
    "region": "us-east-1",
    "endpoint": "https://s3.amazonaws.com"
  },
  "guardrails": {"allowedServices": ["n1ql"]},
  "description": "AWS credential for iceberg"
}

-- Create catalog pointing to AWS Glue
CREATE CATALOG iceberg_catalog TYPE ICEBERG SOURCE AWS_GLUE AT iceberg_creds WITH {}

-- Create external collection (requires bucket.scope.collection path)
CREATE EXTERNAL COLLECTION default.iceberg.external_hotel 
  ON iceberg_catalog AT iceberg_creds 
  WITH {"namespace": "icebergdb", "tableName": "hotel"}

-- Query external collection
SELECT COUNT(*) FROM default.iceberg.external_hotel
```

## Troubleshooting

**Hostname resolution errors (Spark):**
- Error: `java.net.UnknownHostException: hostname: Temporary failure in name resolution`
- Fix: The test auto-adds hostname to `/etc/hosts` and sets `SPARK_LOCAL_IP`

**S3 bucket not found errors:**
- Each test creates a unique S3 bucket with UUID suffix
- Spark session is stopped between tests to prevent caching issues

**Couchbase bucket not found:**
- Test creates Couchbase bucket `default` if it doesn't exist
- Uses `default_bucket=False` in conf to avoid conflicts
