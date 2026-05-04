# Iceberg Reviews Array - Cross-Validation Guide

## Overview

The Iceberg test data includes a complex nested `reviews` array field. This document explains how reviews are generated and how to cross-validate query results against expected counts.

## Review Generation Pattern

### Deterministic Formula
```python
num_reviews_per_document = (document_id % 5) + 1
```

This creates a predictable pattern:
- Document ID 0, 5, 10, 15, ... → **1 review**
- Document ID 1, 6, 11, 16, ... → **2 reviews**
- Document ID 2, 7, 12, 17, ... → **3 reviews**
- Document ID 3, 8, 13, 18, ... → **4 reviews**
- Document ID 4, 9, 14, 19, ... → **5 reviews**

### Review Count Distribution (10,000 documents)

| Pattern | Review Count | # of Documents | Total Reviews |
|---------|--------------|----------------|---------------|
| id % 5 == 0 | 1 | 2,000 | 2,000 |
| id % 5 == 1 | 2 | 2,000 | 4,000 |
| id % 5 == 2 | 3 | 2,000 | 6,000 |
| id % 5 == 3 | 4 | 2,000 | 8,000 |
| id % 5 == 4 | 5 | 2,000 | 10,000 |
| **TOTAL** | **Avg: 3** | **10,000** | **30,000** |

## Review Object Structure

Each review object has this structure:
```json
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
```

### Review Attributes

1. **Date**: Weekly intervals starting from 2025-11-13
   - Review 0: 2025-11-13 01:08:32
   - Review 1: 2025-11-20 01:08:32 (+1 week)
   - Review 2: 2025-11-27 01:08:32 (+2 weeks)
   - And so on...

2. **Author**: Deterministic based on `(document_id + review_index)`
   - Cycles through first/last name combinations
   - Example: Doc 0 Review 0 → "Alexander Stiedemann"
   - Example: Doc 1 Review 1 → "Leoma Kuphal"

3. **Ratings**: 5 rating categories with values 0-4
   - Deterministic based on `(document_id + review_index) % 5`
   - Each rating field offset by different amounts
   - Overall rating is most important for aggregations

## Cross-Validation Examples

### Example 1: Simple Document Query
```sql
SELECT id, name, reviews 
FROM iceberg_external 
WHERE id IN [0, 1, 2, 3, 4]
ORDER BY id
```

**Expected Results:**
```
Doc 0: 1 review  (array length = 1)
Doc 1: 2 reviews (array length = 2)
Doc 2: 3 reviews (array length = 3)
Doc 3: 4 reviews (array length = 4)
Doc 4: 5 reviews (array length = 5)

Total: 5 documents, 15 review objects (1+2+3+4+5)
```

**Validation Code:**
```python
for row in results:
    expected = (row['id'] % 5) + 1
    actual = len(row['reviews'])
    assert actual == expected, f"Doc {row['id']}: expected {expected}, got {actual}"
```

### Example 2: UNNEST Reviews Query
```sql
SELECT h.id, h.name, r.author, r.ratings.Overall as overall_rating
FROM iceberg_external h
UNNEST h.reviews r
WHERE h.id < 10
ORDER BY h.id, r.date
```

**Expected Results:**
```
30 rows total (1+2+3+4+5+1+2+3+4+5)

Doc 0: 1 unnested row
Doc 1: 2 unnested rows
Doc 2: 3 unnested rows
Doc 3: 4 unnested rows
Doc 4: 5 unnested rows
Doc 5: 1 unnested row
Doc 6: 2 unnested rows
Doc 7: 3 unnested rows
Doc 8: 4 unnested rows
Doc 9: 5 unnested rows
```

**Validation Code:**
```python
def calculate_expected_review_count(doc_ids):
    return sum([(doc_id % 5) + 1 for doc_id in doc_ids])

expected = calculate_expected_review_count(range(10))  # = 30
actual = len(results)
assert actual == expected, f"Expected {expected} unnested reviews, got {actual}"
```

### Example 3: Aggregate Reviews by Type
```sql
SELECT h.type, COUNT(r) as review_count
FROM iceberg_external h
UNNEST h.reviews r
WHERE h.id < 100
GROUP BY h.type
ORDER BY h.type
```

**Expected Results:**
```
For IDs 0-99 (100 documents):
- Total reviews: sum((i % 5) + 1 for i in range(100)) = 300

By type (type cycles every 5):
- Hotel (id % 5 == 0): 20 docs × 1 review = 20 reviews
- Hostel (id % 5 == 1): 20 docs × 2 reviews = 40 reviews
- Resort (id % 5 == 2): 20 docs × 3 reviews = 60 reviews
- Motel (id % 5 == 3): 20 docs × 4 reviews = 80 reviews
- Inn (id % 5 == 4): 20 docs × 5 reviews = 100 reviews

Total: 20+40+60+80+100 = 300 reviews ✓
```

### Example 4: Review Rating Analysis
```sql
SELECT h.id, AVG(r.ratings.Overall) as avg_overall
FROM iceberg_external h
UNNEST h.reviews r
WHERE h.id BETWEEN 0 AND 4
GROUP BY h.id
ORDER BY h.id
```

**Expected Results:**
```
Doc 0: 1 review with Overall=3 → avg = 3.0
Doc 1: 2 reviews with Overall=[4,0] → avg = 2.0
Doc 2: 3 reviews with Overall=[0,1,2] → avg = 1.0
Doc 3: 4 reviews with Overall=[1,2,3,4] → avg = 2.5
Doc 4: 5 reviews with Overall=[2,3,4,0,1] → avg = 2.0
```

## Test Helper Methods

The test class provides helper methods for validation:

```python
def _get_expected_reviews_for_doc(self, doc_id):
    """Get expected number of reviews for a specific document ID."""
    return (doc_id % 5) + 1

def _calculate_expected_review_count(self, doc_ids):
    """Calculate expected total review count for given document IDs."""
    return sum([(doc_id % 5) + 1 for doc_id in doc_ids])
```

## Common Validation Queries

### Query 1: Count reviews per document
```sql
SELECT id, ARRAY_LENGTH(reviews) as review_count
FROM iceberg_external
WHERE id < 20
ORDER BY id
```

### Query 2: All reviews with author names
```sql
SELECT h.id, r.author, r.date
FROM iceberg_external h
UNNEST h.reviews r
WHERE h.id < 5
ORDER BY h.id, r.date
```

### Query 3: High-rated reviews only
```sql
SELECT h.id, h.name, r.ratings.Overall
FROM iceberg_external h
UNNEST h.reviews r
WHERE r.ratings.Overall >= 4 AND h.id < 100
ORDER BY h.id
```

### Query 4: Documents with specific review count
```sql
SELECT id, name, ARRAY_LENGTH(reviews) as review_count
FROM iceberg_external
WHERE ARRAY_LENGTH(reviews) = 5 AND id < 100
ORDER BY id
```
Expected: IDs 4, 9, 14, 19, 24, 29, ... (20 documents)

## Troubleshooting

### Mismatch in Review Counts

If you get unexpected review counts:

1. **Check document ID range**: Make sure you're querying the expected IDs
2. **Verify UNNEST syntax**: Ensure proper UNNEST and alias usage
3. **Check filter conditions**: WHERE clauses can change expected counts
4. **Validate calculation**: Use helper methods to compute expected values

### Example Debug Query
```sql
-- This should return exactly 30 reviews for IDs 0-9
SELECT COUNT(*) as total_reviews
FROM iceberg_external h
UNNEST h.reviews r
WHERE h.id < 10
```

If result ≠ 30, check:
- Data generation logic
- Iceberg table creation
- Query syntax

## Summary

The reviews array provides rich data for testing:
- **Deterministic**: Predictable counts based on document ID
- **Nested structure**: Tests complex JSON handling
- **Cross-validation**: Easy to verify correctness
- **Realistic**: Mimics real hotel review data

Total across all 10,000 documents: **30,000 review objects**
