# N1QL Query Test Generator - LAST UPDATED 05/09/2025

This tool helps generate and test complex N1QL/SQL++ queries for Couchbase using AI-powered query generation. The workflow involves generating queries with AI, executing them against a Couchbase cluster, and saving the successful queries to a sql file.

## Overview

The N1QL Query Test Generator consists of:

1. A query generation and execution script (`scripts/AiQG/AiQG.py`) that uses AI to create and test complex N1QL queries
2. Uses sample data templates, prompt files, and a data generator (`resources/AiQG/data/gen.py`) provided to the script to guide query generation
3. Support for reprompting failed queries with error context

## Script Workflow

### 1. Generate and Execute Queries

Use the `AiQG.py` script to generate complex N1QL queries:
```
python scripts/AiQG/AiQG.py --generate \
  --openai-key YOUR_OPENAI_API_KEY \
  --schema-file scripts/AiQG/sampledata.txt \
  --prompts-file scripts/AiQG/samplepromptfile.txt \
  --username Administrator \
  --password password \
  --bucket default \
  --output-file-name sample_queries \
  --output-dir resources/AiQG/sql
```

#### Parameters:

- `--generate`: Generate queries using LangChain and OpenAI
- `--reprompt-file`: Path to file containing failed queries to reprompt
- `--sql-file`: Path to the SQL file containing queries we wish to execute if we are not using the --generate or reprompt-file options
- `--openai-key`: OpenAI API key (required if --generate or --reprompt-file is used)
- `--schema-file`: Schema template file (required if --generate or --reprompt-file is used)
- `--prompts-file`: Text file containing query generation prompts (required if --generate is used)
- `--query-rules`: Optional text file containing additional query rules for generation
- `--seed`: Random seed for query generation (default: 42) 
- `--ip`: Couchbase server IP address (default: 127.0.0.1)
- `--port`: Couchbase server port (default: 8091)
- `--username`: Couchbase username (default: Administrator)
- `--password`: Couchbase password (default: password)
- `--bucket`: Couchbase bucket name (default: default)
- `--query-context`: Query context (bucket.scope) (default: default._default)
- `--output-file-name`: Output file prefix for the sql files i.e (output_file_name)_successful_queries_42.sql (optional)
- `--output-dir`: Output directory for generated query files (optional)

### 2. Generate Queries from prompt file

The script will generate queries based on a prompt file and execute those queries to make sure they are valid. It will then generate two files, one for successful queries and one for failed queries. The script will also dump a list of if each query was successful or not to the console, with execution times, result counts, and error messages if any.

### 3. Review Generated Queries

The script will save generated queries to the specified output file (e.g., `resources/AiQG/sql/sample_queries_successful_queries_42.sql`). Review these queries to ensure they meet your testing requirements. If an output file is not provided, the script will save the queries to a file named after the prompt file (e.g., `resources/AiQG/sql/samplepromptfile_successful_queries_42.sql`). Review the failed queries to ensure there are no bugs present.

### 4. Reprompt Failed Queries

The script has an option to reprompt failed queries to try to fix them. You will pass in the old failed queries file, it will analyze the errors and regenerate new queries based on those errors to try to fix them. It will then append successful queries to the previous successful queries file and genearte a new failed queries file.

```
python scripts/AiQG/AiQG.py --reprompt-file resources/AiQG/sql/samplepromptfile_failed_queries_42.txt \
  --openai-key YOUR_OPENAI_API_KEY \
  --schema-file scripts/AiQG/sampledata.txt \
  --username Administrator \
  --password password \
  --bucket default \
  --output-file-name sample_queries \
  --output-dir resources/AiQG/sql
```
## Testing Workflow with testrunner

### 1. Generate conf file with the queries obtained from AiQG.py

Create or update a test configuration file (e.g., `conf/tuq/py-tuq-aiqg.conf`) to include test cases for each query:

```
tuqquery.tuq_AiQG_runner.QueryAiQGTests:
    test_AiQG_runner,sql_file_path=resources/AiQG/sql/sample_queries.sql,query_number=1
    test_AiQG_runner,sql_file_path=resources/AiQG/sql/sample_queries.sql,query_number=2
    # Add more test cases as needed
```

### 2. Run Tests

Execute the tests using the test runner:

```
python testrunner.py -c conf/tuq/py-tuq-aiqg.conf
```

## Components

### Query Generation Script (`AiQG.py`)

The script uses OpenAI's models to generate complex N1QL queries based on:
- Database schema (collections and their fields)
- Prompt instructions that specify query complexity and features to include

### Sample Data Template

The sample data template (`scripts/AiQG/sampledata.txt`) defines the collections and their fields, along with sample documents. This helps the AI understand the database structure.

### Data Generator

The data generator (`resources/AiQG/data/gen.py`) is used to generate data that we use in our testing. It is provided so that the script can generate queries that are valid and can return results based on the actual values used in our testing.

### Prompt File

The prompt file (`scripts/AiQG/samplepromptfile.txt`) contains instructions for the AI about what kinds of queries to generate. You can specify:
- Query complexity
- N1QL features to use (UNNEST, LET, subqueries, etc.)
- Number of queries to generate

### Test Runner

The test runner (`pytests/tuqquery/tuq_AiQG_runner.py`) executes the generated queries and validates them by:
1. Running the query without indexes to get expected results
2. Getting index recommendations from the advisor
3. Creating recommended indexes
4. Running the query with indexes and optional parameters:
   - `memory_quota`: Memory quota in MB for query execution (default: 100)
   - `timeout`: Query timeout duration (default: "120s")
   - `compare_cbo`: Enable comparison between CBO and non-CBO execution (default: false)
   - `compare_udf`: Enable comparison using inline SQL++ UDF (default: false)
   - `compare_prepared`: Enable comparison using prepared statements (default: false)
5. When `compare_cbo=true`:
   - Executes query with and without CBO
   - Compares execution times and results
   - Validates CBO plan contains required optimization metrics
6. When `compare_udf=true`:
   - Creates inline SQL++ UDF from query
   - If query has string predicate (e.g. field = "value"), converts it to parameterized form
   - Executes UDF with original predicate value as parameter
   - Validates UDF results match original query results
7. When `compare_prepared=true`:
   - Prepares query using PREPARE statement
   - Executes prepared query with profile timings
   - Validates prepared query results match original results
   - Checks explain plan for index usage
8. Comparing results with and without indexes to ensure they match

## Customization

### Creating Custom Query Sets

1. Create a new prompt file with specific instructions
2. Run the generator with your custom prompt file
3. Save to a new SQL file
4. Create a new test configuration pointing to your SQL file

### Adding Test Data

The test runner automatically creates collections and loads test data during setup. To modify test data:
1. Update the JSON files in `resources/AiQG/data/`
2. Adjust the schema file to match your data structure

## Troubleshooting

- If queries fail during testing, check the test logs for specific errors
- For query generation issues, try adjusting the prompts or providing more context in the schema file
- Use the `--seed` parameter to reproduce specific query generation results

## Example Prompt File

```
Use Multiple UNNEST of same array in a single query. Give me 5 queries
Usage of LET clause with subqueries and UNNEST with ORDER BY clause. Give me 5 queries
Usage of OFFSET with ORDER BY clause. Give me 5 queries
```

Each line represents a different prompt, and the AI will generate the requested number of queries for each prompt.

## Best Practices

1. **Start Simple**: Begin with simple query patterns and gradually increase complexity
2. **Review Generated Queries**: Always review AI-generated queries before testing
3. **Use Specific Prompts**: More specific prompts yield better query generation results
4. **Maintain Test Data**: Ensure test data covers all query scenarios
5. **Version Control**: Keep generated queries in version control for regression testing

## Limitations

- AI-generated queries may occasionally contain syntax errors
- Complex queries might need manual adjustment
- Query performance can vary based on data size and distribution