# N1QL Query Test Generator

This tool helps generate and test complex N1QL/SQL++ queries for Couchbase using AI-powered query generation. The workflow involves generating queries with AI, saving them to a test file, and then running tests against those queries.

## Overview

The N1QL Query Test Generator consists of:

1. A query generation script (`scripts/AiQG/AiQG.py`) that uses AI to create complex N1QL queries
2. Sample data templates and prompt files to guide query generation
3. A test runner (`pytests/tuqquery/tuq_AiQG_runner.py`) that executes and validates the generated queries

## Workflow

### 1. Generate Queries

Use the `AiQG.py` script to generate complex N1QL queries:

```
python scripts/AiQG/AiQG.py --generate \
  --openai-key YOUR_OPENAI_API_KEY \
  --schema-file scripts/AiQG/sampledata.txt \
  --prompts-file scripts/AiQG/samplepromptfile.txt \
  --username Administrator \
  --password password \
  --bucket default \
  --save-queries resources/AiQG/sql/sample_queries.sql
```

#### Parameters:

- `--generate`: Use AI to generate queries
- `--openai-key`: Your OpenAI API key
- `--schema-file`: File containing the database schema (collections and fields)
- `--prompts-file`: File containing query generation prompts
- `--username`: Couchbase username
- `--password`: Couchbase password
- `--bucket`: Couchbase bucket name
- `--save-queries`: Output file to save generated queries
- `--seed`: (Optional) Random seed for reproducible query generation

### 2. Review Generated Queries

The script will save generated queries to the specified output file (e.g., `resources/AiQG/sql/sample_queries.sql`). Review these queries to ensure they meet your testing requirements.

### 3. Create Test Configuration

Create or update a test configuration file (e.g., `conf/tuq/py-tuq-aiqg.conf`) to include test cases for each query:

```
tuqquery.tuq_AiQG_runner.QueryAiQGTests:
    test_AiQG_runner,sql_file_path=resources/AiQG/sql/sample_queries.sql,query_number=1
    test_AiQG_runner,sql_file_path=resources/AiQG/sql/sample_queries.sql,query_number=2
    # Add more test cases as needed
```

### 4. Run Tests

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
4. Running the query with indexes
5. Comparing results to ensure they match

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