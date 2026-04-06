# N1QL Query Test Suite Domain Glossary

## Core Concepts

| Term | Description |
|------|-------------|
| **N1QL** (Non-Intrusive Query Language) | Couchbase's SQL++ query language for JSON documents |
| **SQL++** | SQL-compatible query language for Couchbase, also called N1QL |
| **Query Service** | Couchbase service that executes N1QL queries (port 8093) |
| **GSI** (Global Secondary Index) | Index service backing Query Service operations |
| **Primary Index** | Required index (`#primary`) for simple queries without WHERE clauses |
| **Secondary Index** | User-defined index on document fields for query optimization |
| **Covering Index** | Index that includes all fields used in SELECT, WHERE, ORDER BY |
| **Array Indexing** | Index on array fields with FLATTEN_KEYS option |
| **Meta Index** | Special covering index used by default schemaless queries |
| **Bucket** | Document container in Couchbase (analogous to database) |
| **Scope** | Logical grouping of collections within a bucket (Couchbase 6.5+) |
| **Collection** | Document subset within a scope (Couchbase 6.5+) |
| **Keyspace** | Query terminology for bucket, scope.collection, or system catalog |

## Query Execution Phases

| Phase | Description |
|-------|-------------|
| **Parsing** | Parse query string into abstract syntax tree (AST) |
| **Planning** | Generate query execution plan, select indexes |
| **Optimization** | Apply optimizer rules, choose best access path |
| **Execution** | Execute the plan, read data, compute results |
| **Result Formatting** | Format results according to client request |

## Request Levels and Scan Consistency

| Level | Description |
|-------|-------------|
| `not_bounded` | No consistency guarantees; fastest performance |
| `request_plus` | Guarantees consistency with all mutations up to the request time |
| `statement_plus` | Guarantees consistency with mutations up to the statement execution |
| `at_plus` | Guarantees consistency with mutations up to a specified point in time |

## Query Types

| Type | Description |
|------|-------------|
| **SELECT** | Retrieve documents with filtering, projection, aggregation |
| **INSERT** | Create new documents |
| **UPDATE** | Modify existing documents |
| **DELETE** | Remove documents |
| **UPSERT** | Insert or update document (create if not exists, update if exists) |
| **MERGE** | Conditional insert/update based on matching conditions |
| **Explain** | View query execution plan without executing |
| **Advise** | Get index recommendations |
| **Infer** | Infer schema from documents |

## Join Types

| Type | Description |
|------|-------------|
| **Implicit Join** | Comma-separated FROM clause with lookup in WHERE |
| **Explicit Join** | ANSI JOIN syntax with ON clause |
| **INNER JOIN** | Default join, returns matching rows |
| **LEFT OUTER JOIN** | Returns all left table rows, matching right rows, NULL for mismatches |
| **RIGHT OUTER JOIN** | Returns all right table rows, matching left rows, NULL for mismatches |
| **Nest** | Right-side documents nested in left-side results |
| **Unnest** | Flatten nested arrays for querying |
| **ANSI JOIN** | Standard SQL JOIN syntax (Couchbase 5.5+) |
| **ANSI MERGE** | Standard SQL MERGE syntax |

## Window Functions

| Function | Description |
|----------|-------------|
| **ROW_NUMBER()** | Sequential row order within partition |
| **RANK()** | Ranking with ties resulting in gaps |
| **DENSE_RANK()** | Ranking with ties without gaps |
| **NTILE(N)** | Divide rows into N evenly ranked buckets |
| **FIRST_VALUE()** | First value in window frame |
| **LAST_VALUE()** | Last value in window frame |
| **LAG()** | Value from preceding row |
| **LEAD()** | Value from following row |
| **Aggregate Over** | SUM, AVG, COUNT over window frame |

## Window Frames

| Frame | Description |
|-------|-------------|
| **ROWS** | Physical rows offset from current row |
| **RANGE** | Logical value range offset from current row |
| **GROUPS** | Groups with identical value keys |
| **UNBOUNDED PRECEDING** | From first row of partition |
| **CURRENT ROW** | Current row |
| **UNBOUNDED FOLLOWING** | To last row of partition |

## User Defined Functions (UDF)

| Term | Description |
|------|-------------|
| **Inline UDF** | UDF defined inline using FUNCTION clause in N1QL |
| **External UDF** | UDF defined via JavaScript outside query |
| **Function Library** | Collection of UDFs for reuse |
| **Define UDF** | CREATE FUNCTION statement for UDF creation |
| **Execute UDF** | Call UDF within query expression |
| **Drop UDF** | Remove UDF with DROP FUNCTION |

## Vector Search

| Term | Description |
|------|-------------|
| **Vector Search** | Similarity search based on vector embeddings |
| **KNN (K-Nearest Neighbors)** | Find K most similar vectors |
| **ANN (Approximate Nearest Neighbor)** | Fast approximate vector search |
| **FAISS** | Facebook AI Similarity Search library for ANN |
| **Distance Metric** | Similarity measure (L2, DOT, COSINE, EUCLIDEAN) |
| **IVF (Inverted File Index)** | Vector quantization algorithm for fast search |
| **SQ8 (Scalar Quantization)** | 8-bit scalar quantization for compression |
| **PQ (Product Quantization)** | Advanced compression for vectors |
| **Embedding** | Numerical representation of documents/images |
| **Dimension** | Vector embedding size (e.g., 128, 4096) |
| **nprobes** | Number of clusters to probe in ANN search |
| **Vector Index** | Specialized GSI index for vector similarity search |
| **Pushdown Optimization** | Execute vector similarity in GSI service |

## Test Categories and Labels

| Category | Purpose | Label/Group |
|----------|---------|-------------|
| **Sanity Tests** | Core query functionality | P0 |
| **Core Operations** | SELECT, aggregation, functions | P0, feature tags |
| **Indexing** | Index creation, usage, optimization | P0 |
| **DML** | INSERT, UPDATE, DELETE, UPSERT, MERGE | P1 |
| **DDL** | CREATE/DROP INDEX, bucket ops | P1 |
| **Joins** | Implicit and explicit joins | P1 |
| **Subqueries** | Correlated and uncorrelated subqueries | P1 |
| **CTE** | Common Table Expressions | P1 |
| **Window Functions** | Analytical queries | P1 |
| **UDF** | User defined functions | P1 |
| **Vector Search** | KNN/ANN vector similarity | P1 |
| **Collections** | Scopes and collections | P1 |
| **RBAC** | Role-based access control | P1 |
| **Security** | RBAC, audit, encryption | P0 |
| **FTS Integration** | Full-text search via Query | P2 |
| **Performance** | Query performance and optimization | P2 |

## Priority Levels

| Level | Purpose | Criteria |
|-------|---------|----------|
| **P0** | Critical tests | Core functionality, must pass for each build |
| **P1** | High priority | Major features, important edge cases |
| **P2** | Medium priority | Advanced features, less common scenarios |
| **P3** | Low priority | Experimental features, rare edge cases |

## Configuration Parameters

| Parameter | Description | Example Values |
|-----------|-------------|----------------|
| `skip_index` | Skip index creation for performance testing | `True`, `False` |
| `doc-per-day` | Documents generated per day | `1`, `10`, `100` |
| `covering_index` | Use covering index optimization | `True`, `False` |
| `GROUP` | Priority level and feature tags | `P0`, `P1`, `LIKE`, `DATE`, `SCALAR` |
| `distance` | Vector search distance metric | `L2`, `DOT`, `COSINE`, `EUCLIDEAN` |
| `nprobes` | ANN search cluster probes | `5`, `10`, `20` |
| `description` | Vector index quantization | `IVF,SQ8`, `PQ32x8`, `SQ8` |
| `reload_data` | Reload test data before execution | `True`, `False` |
| `compare_cbo` | AiQG CBO comparison | `True`, `False` |
| `compare_udf` | AiQG UDF comparison | `True`, `False` |
| `compare_prepared` | AiQG prepared statement comparison | `True`, `False` |

## Query Advisor

| Term | Description |
|------|-------------|
| **ADVISE Statement** | N1QL statement to get index recommendations |
| **Advice** | Suggested indexes to improve query performance |
| **Advisor** | Tool that analyzes queries and recommends indexes |
| **Query Plan** | Execution plan generated by Query Service |
| **Index Selection** | Process of choosing optimal index for query |

## Test File Naming Conventions

| Prefix | Purpose | Examples |
|--------|---------|----------|
| `tuq_` | General Query Service tests | `tuq.py`, `tuq_sanity.py`, `tuq_join.py` |
| `n1ql_` | N1QL-specific feature tests | `n1ql_window_functions.py`, `n1ql_rbac_2.py` |
| `newtuq` | New Query Service format | `newtuq.py`, `newtuq_tests.py` |

## System Catalogs

| Catalog | Description |
|---------|-------------|
| `system:keyspaces` | List all keyspaces (buckets, scopes.collections, system catalogs) |
| `system:scopes` | List all scopes in all buckets |
| `system:collections` | List all collections in all scopes |
| `system:indexes` | List all indexes including metadata |
| `system:dictionaries` | Document field schemas |
| `system:user_info` | User information and roles |

## Query Operators

| Operator | Category | Description |
|----------|----------|-------------|
| `ANY` | Array | Evaluate if any element satisfies condition |
| `EVERY` | Array | Evaluate if all elements satisfy condition |
| `SATISFIES` | Array | Condition clause for ANY/EVERY |
| `IN` | Membership | Check if value in list |
| `LIKE` | Pattern | String pattern matching |
| `BETWEEN` | Range | Value in inclusive range |
| `IS NULL` | Null check | Check for NULL value |
| `IS MISSING` | Missing check | Check for missing field |

## Query Clauses

| Clause | Purpose |
|--------|---------|
| `SELECT` | Specify projection (fields to return) |
| `FROM` | Specify data source (keyspace) |
| `WHERE` | Filter rows |
| `GROUP BY` | Aggregate groups |
| `HAVING` | Filter groups |
| `ORDER BY` | Sort results |
| `LIMIT` | Restrict result count |
| `OFFSET` | Skip initial results |
| `LET` | Define variables for reuse |
| `UNNEST` | Flatten nested arrays |
| `NEST` | Nest right side in left |
| `JOIN` | Join keyspaces |
| `USE INDEX` | Explicitly select index |

## Result Verification

| Method | Description |
|--------|-------------|
| `DeepDiff` | JSON comparison library for result verification |
| `Self.assertEqual` | Standard Python test assertion |
| `Self.assertTrue` | Boolean assertion |
| `Self.assertIn` | Membership assertion |
| `run_cbq_query` | Execute N1QL query and return results |
| `verify_results` | Custom result verification method |

## Query Patterns

| Pattern | Description |
|---------|-------------|
| **Subquery** | Query within another query |
| **Correlated Subquery** | Subquery referencing outer query values |
| **Common Table Expression (CTE)** | Named temporary result set |
| **Chained LET** | Multiple LET clauses for complex calculations |
| **Prepared Statement** | Pre-compiled query for reuse |
| **Auto Prepare** | Automatic query preparation |
| **Parameterized Query** | Query with placeholders |

## AiQG (AI Query Generator)

| Term | Description |
|------|-------------|
| **AiQG** | AI-powered Query Generator |
| **OpenAI** | AI model API for query generation |
| **LangChain** | Framework for LLM applications |
| **Prompt Engineering** | Designing prompts for query generation |
| **Query Validation** | Testing generated queries against cluster |
| **Index Testing** | Test queries without indexes, get advisor recommendations |
| **CBO Comparison** | Compare query plans with/without Cost-Based Optimizer |
| **UDF Transformation** | Convert query parameters to UDF for testing |
| **Memory Quota** | Memory allocation for query execution |
| **Timeout** | Maximum query execution time |

## Error Handling

| Exception | Description | Source |
|-----------|-------------|--------|
| `CBQError` | Query execution error | `membase/api/exception.py` |
| `ReadDocumentException` | Document read error | `membase/api/exception.py` |
| `ServerUnavailableException` | Server not available | `membase/api/exception.py` |

## Query Timeout

| Parameter | Description |
|-----------|-------------|
| `timeout` | Query execution timeout in seconds |
| `n1ql_timeout` | N1QL service timeout |
| `request_timeout` | REST API request timeout |

## Query Monitoring

| Metric | Description |
|--------|-------------|
| `query_time` | Query execution time |
| `service_time` | Service processing time |
| `result_count` | Number of results returned |
| `result_size` | Result set size in bytes |
| `index_used` | Index used in query execution |

## Performance Optimization

| Technique | Description |
|-----------|-------------|
| **Covering Index** | Index includes all query fields |
| **Pushdown** | Execute predicate in index service |
| **Filter Order** | Optimize filter order for early elimination |
| **Limit Pushdown** | Push LIMIT to index service |
| **Advisory** | Use ADVISE to recommend indexes |

## Testing Terminology

| Term | Description |
|------|-------------|
| **Sanity Test** | Basic functionality test |
| **Smoke Test** | Quick validation test |
| **Integration Test** | Test against full system |
| **Regression Test** | Prevent breaking existing functionality |
| **Performance Test** | Validate query performance |
| **Security Test** | Test authentication, authorization, RBAC |
| **Edge Case Test** | Test boundary conditions |
| **Negative Test** | Test error conditions |

## Data generators

| Generator | Description |
|-----------|-------------|
| `DocumentGenerator` | JSON document generator |
| `JsonDocGenerator` | JSON document generator |
| `WikiJSONGenerator` | Wiki-style JSON generator |
| `BlobGenerator` | Binary blob generator |
| `SDKDataLoader` | SDK-based data loader |

## Query Constraints

| Constraint | Description |
|-----------|-------------|
| `PRIMARY KEY` | Unique document identifier |
| `UNIQUE` | Unique index constraint |
| `NOT NULL` | Non-null field constraint |
| `CHECK` | Custom constraint expression |

## Decision/Solution:

I will now create the `domain-glossary.md` file in the appropriate directory.<tool_call>Create<arg_key>file_path</arg_key><arg_value>testrunner/pytests/tuqquery/docs/agent-context/domain-glossary.md
