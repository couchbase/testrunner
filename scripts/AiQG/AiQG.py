#!/usr/bin/env python3
import argparse
import os
import time
import json
from typing import List, Dict, Any, Tuple
from couchbase.cluster import Cluster, ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
import re

# LangChain imports
from langchain_core.messages import HumanMessage, SystemMessage
from langchain.chat_models import init_chat_model

def connect_to_couchbase(ip: str, port: int, username: str, password: str, bucket_name: str) -> Tuple[Cluster, Any]:
    """Connect to Couchbase and return the cluster and bucket objects."""
    # Format the connection string with the provided IP and port
    connection_string = f"couchbase://{ip}:{port}"

    auth = PasswordAuthenticator(username, password)
    options = ClusterOptions(auth)

    cluster = Cluster(connection_string, options)
    bucket = cluster.bucket(bucket_name)

    return cluster, bucket

def load_schema_template(file_path: str) -> str:
    """Load a sample data template from a file."""
    try:
        with open(file_path, 'r') as f:
            schema_content = f.read()
            return schema_content
    except Exception as e:
        print(f"Error loading schema template: {str(e)}")
        return {}

def load_data_generation() -> str:
    """Load data generation context from file."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        gen_path = os.path.join(script_dir, '..', '..', 'resources', 'AiQG', 'data', 'gen.py')
        with open(gen_path, 'r') as f:
            data_gen_content = f.read()
        return data_gen_content
    except Exception as e:
        print(f"Error loading data generation file: {str(e)}")
        return {}

def load_query_rules(query_rules_file: str) -> str:
    """Load additional query rules from file."""
    try:
        with open(query_rules_file, 'r') as f:
            return f.read()
    except Exception as e:
        print(f"Error loading rules file: {str(e)}")
        return {}

def generate_queries_with_langchain(api_key: str, schema_template: str, prompts_file: str, seed: int, query_rules_file: str = None) -> List[str]:
    """Generate SQL++ queries using LangChain and OpenAI."""
    # Set the OpenAI API key
    os.environ["OPENAI_API_KEY"] = api_key

    if seed is None:
        seed = 42
        
    # Initialize the chat model
    model = init_chat_model("gpt-4o", model_provider="openai", **{"seed": seed })

    # Load prompts from file
    try:
        with open(prompts_file, 'r') as f:
            prompt_content = f.read()
    except Exception as e:
        print(f"Error loading prompts file: {str(e)}")
        return []

    # Load data generation context and query rules
    data_gen_content = load_data_generation()
    additional_query_rules = load_query_rules(query_rules_file) if query_rules_file else ""
    
    if not data_gen_content:
        return [], seed
    
    all_queries = []

    messages = [
        SystemMessage("You are a Couchbase SQL++/N1QL expert. You are capable of writing complex SQL++/N1QL queries to fetch data from Couchbase."),
        HumanMessage(f"Please use this JSON template to write the Couchbase SQL++/N1QL queries: {schema_template}"),
        HumanMessage(f"Here is how the test data is generated: {data_gen_content}"),
        HumanMessage(prompt_content),
        HumanMessage(additional_query_rules),
        HumanMessage("Generate one query per single line ending with semicolon and no extra text, no bullet points, no quotes, no title/summary. No query numbers.")
    ]

    try:
        # Get the response from the model
        response = model.invoke(messages)

        # Extract the generated queries
        generated_text = response.content.strip()

        # Split the text into individual queries
        queries = []
        current_query = ""

        for line in generated_text.split('\n'):
            line = line.strip()
            if not line or line.startswith('--') or line.startswith('#'):
                continue

            current_query += " " + line

            if line.endswith(';'):
                queries.append(current_query.strip())
                current_query = ""

        # Add the last query if it doesn't end with a semicolon
        if current_query.strip():
            if not current_query.strip().endswith(';'):
                current_query += ";"
            queries.append(current_query.strip())

        all_queries.extend(queries)

    except Exception as e:
        print(f"Error generating queries with LangChain: {str(e)}")

    return all_queries, seed  # Limit to the requested number of queries

def reprompt_with_langchain(api_key: str, reprompt_file: str, schema_template: str, seed: int, cluster: Cluster, query_bucket: str, query_rules_file: str = None) -> List[str]:
    """Reprompt OpenAI to fix failed queries using error messages."""
    os.environ["OPENAI_API_KEY"] = api_key

    bucket = cluster.bucket(query_bucket)

    if seed is None:
        seed = 42
    model = init_chat_model("gpt-4", model_provider="openai", **{"seed": seed})
    fixed_queries = []

    # Load data generation context and query rules
    data_gen_content = load_data_generation()
    additional_query_rules = load_query_rules(query_rules_file) if query_rules_file else ""

    if not data_gen_content:
        return [], seed

    try:
        with open(reprompt_file, 'r') as f:
            content = f.read()
            query_blocks = content.split('\n\n')

        for block in query_blocks:
            lines = block.strip().split('\n')
            if len(lines) >= 3:
                # Extract error and query from the block
                error_line = lines[1].replace('-- Error: ', '')
                query = lines[2]

                # Skip if timeout error
                if 'LCB_ERR_TIMEOUT' in error_line:
                    print(f"Skipping query due to timeout error: {query}")
                    continue

                # Parse error details
                error_code = None
                error_msg = None
                additional_info = None
                
                if 'first_error_code' in error_line:
                    # Extract error code and message from the error context
                    error_match = re.search(r"'first_error_code': (\d+)", error_line)
                    if error_match:
                        error_code = error_match.group(1)
                        try:
                            if error_code != '0':
                                finderr_query = f"select e.reason, e.user_action from finderr({error_code}) e"
                                # Execute the query using the scope
                                query_result = bucket.query(finderr_query)
                                # Materialize the results to ensure query execution completes
                                rows = [row for row in query_result]
                                additional_info = rows[0]['user_action']
                                print(f"Additional info: {additional_info}")
                        except CouchbaseException as e:
                            raise e
                        except Exception as e:
                            raise Exception(f"Unexpected error: {str(e)}")
                    
                    msg_match = re.search(r"'first_error_message': \"([^\"]+)\"", error_line)
                    if msg_match:
                        error_msg = msg_match.group(1)
                else:
                    # For simpler error messages, use the whole line
                    error_msg = error_line

                messages = [
                    SystemMessage("You are a Couchbase SQL++/N1QL expert. Fix the following failed query based on the error message."),
                    HumanMessage(f"Schema template: {schema_template}"),
                    HumanMessage(f"Here is how the test data is generated: {data_gen_content}"),
                    HumanMessage(additional_query_rules),
                    HumanMessage(f"Failed query: {query}"),
                    HumanMessage(f"Error code: {error_code}"),
                    HumanMessage(f"Error message: {error_msg}"),
                    HumanMessage(f"Additional info: {additional_info}"),
                    HumanMessage("Return only the fixed query with no additional text or explanation. One query per line.")
                ]

                response = model.invoke(messages)
                fixed_query = response.content.strip()
                
                if not fixed_query.endswith(';'):
                    fixed_query += ';'
                    
                fixed_queries.append(fixed_query)

    except Exception as e:
        print(f"Error reprompting queries: {str(e)}")

    return fixed_queries,seed

def execute_queries(cluster: Cluster, queries: List[str], query_context: str = None) -> List[Dict[str, Any]]:
    """Execute each query and track results."""
    results = []
    query_context_contents = query_context.split('.')
    query_bucket = query_context_contents[0]
    query_scope = query_context_contents[1]
    # Get travel-sample bucket and inventory collection
    bucket = cluster.bucket(query_bucket)
    scope = bucket.scope(query_scope)

    for i, query in enumerate(queries, 1):
        if not query.strip():
            continue

        result = {
            "query_number": i,
            "query": query,
            "status": "FAILED",
            "error": None,
            "execution_time": 0,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            # Execute the query using the scope
            query_result = scope.query(query, QueryOptions(metrics=True))
            # Materialize the results to ensure query execution completes
            rows = [row for row in query_result]

            result["status"] = "SUCCESS"
            result["execution_time"] = query_result.metadata().metrics().execution_time()
            result["result_count"] = query_result.metadata().metrics().result_count()
        except CouchbaseException as e:
            result["error"] = str(e)
        except Exception as e:
            result["error"] = f"Unexpected error: {str(e)}"

        results.append(result)

    return results

def generate_report(results: List[Dict[str, Any]], output_file: str = None, prompts_file: str = None, seed: int = None, reprompt: str = None, output_dir: str = None) -> None:
    """Generate a report of query execution results."""
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    total_count = len(results)

    if total_count == 0:
        report = "No queries were executed."
        print(report)
        return

    # Use output_dir if provided, otherwise use current directory
    if output_dir:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = os.path.dirname(os.path.abspath(__file__))

    report = "Detailed Results:\n----------------"

    # Create files for successful and failed queries based on prompt file name
    if prompts_file:
        prompt_base = os.path.splitext(prompts_file)[0]
        prompt_base = prompt_base.split('/')[-1]
        if output_file:
            successful_queries = os.path.join(output_dir, f"{output_file}_successful_{seed}.sql")
        else:
            successful_queries = os.path.join(output_dir, f"{prompt_base}_successful_queries_{seed}.sql")
        if reprompt:
            if output_file:
                failed_queries = os.path.join(output_dir, f"{output_file}_failed_{seed}.txt")
            else:
                failed_queries = os.path.join(output_dir, f"{prompt_base}_reprompt_queries_failed_{seed}.txt")
        else:
            if output_file:
                failed_queries = os.path.join(output_dir, f"{output_file}_failed_{seed}.txt")
            else:
                failed_queries = os.path.join(output_dir, f"{prompt_base}_reprompt_queries_{seed}.txt")
    else:
        successful_queries = os.path.join(output_dir, "successful_queries.sql" if not output_file else f"{output_file}_successful.sql")
        failed_queries = os.path.join(output_dir, "failed_queries.txt" if not output_file else f"{output_file}_failed.txt")

    # Open files in append mode to add to existing files or create new ones
    with open(successful_queries, 'a+') as sf, open(failed_queries, 'a+') as ff:
        for result in results:
            report += f"\nQuery #{result['query_number']} - {result['status']}\n"
            report += f"Timestamp: {result['timestamp']}\n"
            report += f"Query: {result['query']}\n"

            if result["status"] == "SUCCESS":
                # Convert timedelta to seconds
                execution_time_seconds = result['execution_time'].total_seconds()
                report += f"Execution Time: {execution_time_seconds} seconds\n"
                # Convert UnsignedInt64 to integer for result count
                result_count = int(result.get('result_count', 'N/A'))
                report += f"Rows Returned: {result_count}\n"
                # Format query to remove new lines and extra spaces
                formatted_query = ' '.join(result['query'].split())
                # Write successful query to stable queries file
                sf.write(f"{formatted_query}\n")
            else:
                report += f"Error: {result['error']}\n"
                # Write failed query and error to reprompt queries file
                ff.write(f"-- Query #{result['query_number']}\n")
                ff.write(f"-- Error: {result['error']}\n")
                ff.write(f"{result['query']}\n\n")

    report += f'''
    N1QL Query Execution Summary
    ===========================
    Total Queries: {total_count}
    Successful: {success_count}
    Failed: {total_count - success_count}
    Success Rate: {(success_count / total_count) * 100:.2f}%
    Successful queries saved to: {successful_queries}
    Failed queries saved to: {failed_queries}\n'''

    print(report)

    if output_file:
        output_path = os.path.join(output_dir, output_file)
        with open(output_path, 'w') as f:
            f.write(report)
        print(f"Report saved to {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Generate and execute SQL++ queries against Couchbase")

    # Input options - either file or AI generation
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--sql-file", help="Path to the SQL file containing queries")
    input_group.add_argument("--generate", action="store_true", help="Generate queries using LangChain and OpenAI")
    input_group.add_argument("--reprompt-file", help="Path to file containing failed queries to reprompt")

    # Generation options
    parser.add_argument("--openai-key", help="OpenAI API key (required if --generate or --reprompt-file is used)")
    parser.add_argument("--schema-file", help="schema template file (required if --generate or --reprompt-file is used)")
    parser.add_argument("--prompts-file", help="textfile containing query generation prompts (required if --generate is used)")
    parser.add_argument("--query-rules", help="Optional text file containing additional query rules")
    parser.add_argument("--seed", type=int, help="Random seed for query generation (optional)")

    # Couchbase connection options
    parser.add_argument("--ip", default="127.0.0.1", help="Couchbase server IP address")
    parser.add_argument("--port", type=int, default=8091, help="Couchbase server port")
    parser.add_argument("--username", default="Administrator", required=True, help="Couchbase username")
    parser.add_argument("--password", default="password", required=True, help="Couchbase password")
    parser.add_argument("--bucket", default="default", required=True, help="Couchbase bucket name")
    parser.add_argument("--query-context", default="default._default", 
                        help="Query context (bucket.scope)")

    # Output options
    parser.add_argument("--output-file-name", help="Output file prefix for the sql files i.e (output_file_name)_successful_queries_42.sql (optional)")
    parser.add_argument("--output-dir", help="Output directory for generated query files (optional)")

    args = parser.parse_args()

    # Validate arguments
    if args.generate:
        if not args.openai_key:
            print("Error: --openai-key is required when using --generate")
            return
        if not args.schema_file:
            print("Error: --schema-file is required when using --generate")
            return
        if not args.prompts_file:
            print("Error: --prompts-file is required when using --generate")
            return
        if not os.path.exists(args.schema_file):
            print(f"Error: Schema file '{args.schema_file}' not found")
            return
        if not os.path.exists(args.prompts_file):
            print(f"Error: Prompts file '{args.prompts_file}' not found")
            return
        if args.query_rules and not os.path.exists(args.query_rules):
            print(f"Error: Query rules file '{args.query_rules}' not found")
            return
    elif args.reprompt_file:
        if not args.openai_key:
            print("Error: --openai-key is required when using --reprompt-file")
            return
        if not args.schema_file:
            print("Error: --schema-file is required when using --reprompt-file") 
            return
        if not os.path.exists(args.reprompt_file):
            print(f"Error: Reprompt file '{args.reprompt_file}' not found")
            return
    elif args.sql_file and not os.path.exists(args.sql_file):
        print(f"Error: SQL file '{args.sql_file}' not found")
        return

    try:
        # Connect to Couchbase
        print(f"Connecting to Couchbase at {args.ip}:{args.port}...")
        cluster, bucket = connect_to_couchbase(args.ip, args.port, args.username, args.password, args.bucket)
        print("Connected successfully")
        
        # Get queries either from file or by generating them
        if args.generate:
            print(f"Loading schema template from {args.schema_file}...")
            schema_template = load_schema_template(args.schema_file)

            print(f"Generating queries using LangChain and OpenAI...")
            queries, seed = generate_queries_with_langchain(
                args.openai_key,
                schema_template,
                args.prompts_file,
                seed=args.seed,
                query_rules_file=args.query_rules
            )

            if not queries:
                print("No queries were generated. Exiting.")
                return

            print(f"Generated {len(queries)} queries.")

        elif args.reprompt_file:
            print(f"Loading schema template from {args.schema_file}...")
            schema_template = load_schema_template(args.schema_file)

            print(f"Reprompting failed queries using LangChain and OpenAI...")
            queries,seed = reprompt_with_langchain(
                args.openai_key,
                args.reprompt_file,
                schema_template,
                seed=args.seed,
                cluster=cluster,
                query_bucket=args.bucket,
                query_rules_file=args.query_rules
            )

            if not queries:
                print("No queries were regenerated. Exiting.")
                return

            print(f"Regenerated {len(queries)} queries.")
        else:
            # Parse SQL file
            with open(args.sql_file, 'r') as f:
                content = f.read()

            # Simple query splitting by semicolons
            queries = [q.strip() for q in content.split(';') if q.strip()]
            print(f"Found {len(queries)} queries in {args.sql_file}")

        # Execute queries
        print(f"Executing queries with context: {args.query_context}...")
        results = execute_queries(cluster, queries, args.query_context)

        file_name = None
        if args.generate:
            file_name = args.prompts_file
        elif args.reprompt_file:
            file_name = args.reprompt_file.split('_reprompt')[0]
        else:
            file_name = None

        # Generate report
        generate_report(results, output_file=args.output_file_name, prompts_file=file_name, seed=seed, reprompt=args.reprompt_file, output_dir=args.output_dir)

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()