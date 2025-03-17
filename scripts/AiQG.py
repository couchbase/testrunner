#!/usr/bin/env python3
import argparse
import os
import time
import json
from typing import List, Dict, Any, Tuple
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException

# LangChain imports
from langchain_core.messages import HumanMessage, SystemMessage
from langchain.chat_models import init_chat_model

def load_schema_template(file_path: str) -> Dict:
    """Load a JSON schema template from a file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading schema template: {str(e)}")
        return {}

def generate_queries_with_langchain(api_key: str, schema_template: Dict, query_type: str = "join", num_queries: int = 5) -> List[str]:
    """Generate SQL++ queries using LangChain and OpenAI."""
    # Set the OpenAI API key
    os.environ["OPENAI_API_KEY"] = api_key
    
    # Initialize the chat model
    model = init_chat_model("gpt-4o", model_provider="openai")
    
    # Prepare the messages based on query type
    if query_type.lower() == "join":
        prompt = f"Please provide {num_queries} join queries using the provided hotel collection sample document. Return only the queries, with no explanations or additional text in a sql file format. Each query should end with a semicolon."
    elif query_type.lower() == "subquery":
        prompt = f"Please suggest {num_queries} SQL++ queries with subqueries using the provided schema template. Return only the queries, one per line, with no explanations or additional text. Each query should end with a semicolon."
    else:
        prompt = f"Please provide {num_queries} complex SQL++ queries using the provided schema template. Return only the queries, one per line, with no explanations or additional text. Each query should end with a semicolon."
    
    messages = [
        SystemMessage("You are a Couchbase SQL++ expert. You are capable of writing complex SQL++ queries to fetch data from Couchbase."),
        HumanMessage(f"Please use this JSON template to write the Couchbase SQL++ queries: {json.dumps(schema_template)}"),
        HumanMessage(prompt)
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
        
        return queries[:num_queries]  # Limit to the requested number of queries
    
    except Exception as e:
        print(f"Error generating queries with LangChain: {str(e)}")
        return []

def connect_to_couchbase(ip: str, port: int, username: str, password: str, bucket_name: str) -> Tuple[Cluster, Any]:
    """Connect to Couchbase and return the cluster and bucket objects."""
    # Format the connection string with the provided IP and port
    connection_string = f"couchbase://{ip}:{port}"
    
    auth = PasswordAuthenticator(username, password)
    options = ClusterOptions(auth)
    
    cluster = Cluster(connection_string, options)
    bucket = cluster.bucket(bucket_name)
    
    return cluster, bucket

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
            start_time = time.time()
            # Execute the query using the scope
            query_result = scope.query(query)
            # Materialize the results to ensure query execution completes
            rows = [row for row in query_result]
            end_time = time.time()
            
            result["status"] = "SUCCESS"
            result["execution_time"] = end_time - start_time
            result["row_count"] = len(rows)
        except CouchbaseException as e:
            result["error"] = str(e)
        except Exception as e:
            result["error"] = f"Unexpected error: {str(e)}"
        
        results.append(result)
        
    return results

def generate_report(results: List[Dict[str, Any]], output_file: str = None) -> None:
    """Generate a report of query execution results."""
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    total_count = len(results)
    
    if total_count == 0:
        report = "No queries were executed."
        print(report)
        return
    
    report = f"""
Detailed Results:
----------------
"""
    
    for result in results:
        report += f"\nQuery #{result['query_number']} - {result['status']}\n"
        report += f"Timestamp: {result['timestamp']}\n"
        report += f"Query: {result['query']}\n"
        
        if result["status"] == "SUCCESS":
            report += f"Execution Time: {result['execution_time']:.4f} seconds\n"
            report += f"Rows Returned: {result.get('row_count', 'N/A')}\n"
        else:
            report += f"Error: {result['error']}\n"
    
    report += f'''
N1QL Query Execution Summary
===========================
Total Queries: {total_count}
Successful: {success_count}
Failed: {total_count - success_count}
Success Rate: {(success_count / total_count) * 100:.2f}%\n'''
    
    print(report)
    
    if output_file:
        with open(output_file, 'w') as f:
            f.write(report)
        print(f"Report saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Generate and execute SQL++ queries against Couchbase")
    
    # Input options - either file or AI generation
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--sql-file", help="Path to the SQL file containing queries")
    input_group.add_argument("--generate", action="store_true", help="Generate queries using LangChain and OpenAI")
    
    # Generation options
    parser.add_argument("--openai-key", help="OpenAI API key (required if --generate is used)")
    parser.add_argument("--schema-file", help="JSON schema template file (required if --generate is used)")
    parser.add_argument("--query-type", choices=["join", "subquery", "complex"], default="join", 
                        help="Type of queries to generate (default: join)")
    parser.add_argument("--num-queries", type=int, default=5, help="Number of queries to generate (default: 5)")
    
    # Couchbase connection options
    parser.add_argument("--ip", default="127.0.0.1", help="Couchbase server IP address")
    parser.add_argument("--port", type=int, default=8091, help="Couchbase server port")
    parser.add_argument("--username", required=True, help="Couchbase username")
    parser.add_argument("--password", required=True, help="Couchbase password")
    parser.add_argument("--bucket", required=True, help="Couchbase bucket name")
    parser.add_argument("--query-context", default="default._default", 
                        help="Query context (bucket.scope)")
    
    # Output options
    parser.add_argument("--output", help="Output file for the report (optional)")
    parser.add_argument("--save-queries", help="Save generated queries to a file (optional)")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.generate:
        if not args.openai_key:
            print("Error: --openai-key is required when using --generate")
            return
        if not args.schema_file:
            print("Error: --schema-file is required when using --generate")
            return
        if not os.path.exists(args.schema_file):
            print(f"Error: Schema file '{args.schema_file}' not found")
            return
    elif args.sql_file and not os.path.exists(args.sql_file):
        print(f"Error: SQL file '{args.sql_file}' not found")
        return
    
    try:
        # Get queries either from file or by generating them
        if args.generate:
            print(f"Loading schema template from {args.schema_file}...")
            schema_template = load_schema_template(args.schema_file)
            
            print(f"Generating {args.num_queries} {args.query_type} queries using LangChain and OpenAI...")
            queries = generate_queries_with_langchain(
                args.openai_key, 
                schema_template, 
                args.query_type, 
                args.num_queries
            )
            
            if not queries:
                print("No queries were generated. Exiting.")
                return
                
            print(f"Generated {len(queries)} queries.")
            
            # Save queries to file if requested
            if args.save_queries:
                with open(args.save_queries, 'w') as f:
                    for query in queries:
                        f.write(f"{query}\n")
                print(f"Saved generated queries to {args.save_queries}")
        else:
            # Parse SQL file
            with open(args.sql_file, 'r') as f:
                content = f.read()
            
            # Simple query splitting by semicolons
            queries = [q.strip() for q in content.split(';') if q.strip()]
            print(f"Found {len(queries)} queries in {args.sql_file}")
        
        # Connect to Couchbase
        print(f"Connecting to Couchbase at {args.ip}:{args.port}...")
        cluster, bucket = connect_to_couchbase(args.ip, args.port, args.username, args.password, args.bucket)
        print("Connected successfully")
        
        # Execute queries
        print(f"Executing queries with context: {args.query_context}...")
        results = execute_queries(cluster, queries, args.query_context)
        
        # Generate report
        generate_report(results, args.output)
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()