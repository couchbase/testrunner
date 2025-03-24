import json
import time
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
import os
from .tuq import QueryTests
from deepdiff import DeepDiff

class QueryAiQGTests(QueryTests):

    def setUp(self):
        super(QueryAiQGTests, self).setUp()
        self.log.info("==============  QueryAiQGTests setup has started ==============")
        self.sql_file_path = self.input.param("sql_file_path", "resources/AiQG/sql/sample_queries.sql")
        self.query_number = self.input.param("query_number", 1) # Changed default to 1
        self.query_context = self.input.param("query_context", "default._default")
        self.log.info("==============  QueryAiQGTests setup has ended ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryAiQGTests, self).suite_setUp()
        self.log.info("==============  QueryAiQGTests suite_setup has started ==============")
        # Create and load collections in default._default scope
        try:
            collections_config = {
                'hotel': {'key_field': 'hotel_id', 'prefix': 'hotel'},
                'reviews': {'key_field': 'review_id', 'prefix': 'review'}, 
                'bookings': {'key_field': 'booking_id', 'prefix': 'booking'},
                'users': {'key_field': 'user_id', 'prefix': 'user'}
            }

            shell = RemoteMachineShellConnection(self.master)

            for collection, config in collections_config.items():
                try:
                    # Create collection
                    self.run_cbq_query(f"CREATE COLLECTION default._default.{collection} IF NOT EXISTS")
                    self.log.info(f"Created collection: {collection}")

                    # Copy data file to remote
                    src_file = f"resources/AiQG/data/{collection}.json"
                    des_file = f"/tmp/{collection}.json"
                    shell.copy_file_local_to_remote(src_file, des_file)
                    self.log.info(f"Copied {collection} data file to remote")

                    # Import data using cbimport
                    import_cmd = f"/opt/couchbase/bin/cbimport json --cluster {self.master.ip} "\
                               f"--username {self.master.rest_username} "\
                               f"--password {self.master.rest_password} "\
                               f"--bucket default "\
                               f"--scope-collection-exp _default.{collection} "\
                               f"--format list "\
                               f"--generate-key \"{config['prefix']}::%{config['key_field']}%\" "\
                               f"--dataset file:///tmp/{collection}.json"
                    
                    output, error = shell.execute_command(import_cmd)
                    if error:
                        self.log.error(f"Error importing {collection} data: {error}")
                        raise Exception(f"Failed to import {collection} data")
                    self.log.info(f"Successfully imported {collection} data: {output}")

                    # Create primary index and update statistics
                    self.run_cbq_query(f"CREATE PRIMARY INDEX idx_primary_{collection} IF NOT EXISTS ON {collection}", query_context=self.query_context)
                    self.run_cbq_query(f"UPDATE STATISTICS FOR {collection} INDEX(`idx_primary_{collection}`)", query_context=self.query_context)
                    self.log.info(f"Created primary index and updated statistics for {collection}")

                except Exception as e:
                    self.log.error(f"Error processing collection {collection}: {str(e)}")
                    raise

            shell.disconnect()

        except Exception as e:
            self.log.error(f"Error creating collections: {str(e)}")
            raise
        self.log.info("==============  QueryAiQGTests suite_setup has ended ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryAiQGTests tearDown has started ==============")
        self.log.info("==============  QueryAiQGTests tearDown has ended ==============")
        super(QueryAiQGTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryAiQGTests suite_tearDown has started ==============")
        self.log.info("==============  QueryAiQGTests suite_tearDown has ended ==============")
        super(QueryAiQGTests, self).suite_tearDown()

    def test_AiQG_runner(self):
        # Read queries from file
        try:
            with open(self.sql_file_path, 'r') as f:
                queries = f.readlines()
        except FileNotFoundError:
            self.log.error(f"SQL file not found: {self.sql_file_path}")
            raise
        except Exception as e:
            self.log.error(f"Error reading SQL file: {str(e)}")
            raise

        # Validate query number (adjusted for 1-based indexing)
        if self.query_number < 1 or self.query_number > len(queries):
            raise ValueError(f"Invalid query number {self.query_number}. File contains {len(queries)} queries.")

        # Get the specified query (adjusted for 1-based indexing)
        query = queries[self.query_number - 1].strip()

        if query:  # Ensure query isn't empty
            try:
                # Drop any existing indexes and use sequential scan for first query run
                try:
                    result = self.run_cbq_query("SELECT name,keyspace_id,scope_id,bucket_id FROM system:indexes")
                    for index in result['results']:
                        if 'scope_id' not in index or 'bucket_id' not in index:
                            self.run_cbq_query(f"DROP INDEX default:{index['keyspace_id']}.{index['name']}")
                        else:
                            self.run_cbq_query(f"DROP INDEX default:{index['bucket_id']}.{index['scope_id']}.{index['keyspace_id']}.{index['name']}")
                    self.log.info("Dropped all existing indexes")
                except Exception as e:
                    self.log.error(f"Error dropping indexes: {str(e)}")
                    raise

                # Execute the query
                expected_result = self.run_cbq_query(query,query_context=self.query_context)
                self.log.info(f"Successfully executed the query {self.query_number}: {query} without any indexes")

                # Get index recommendations from advisor and create indexes
                main_index_name, subquery_index_names = self._create_recommended_indexes(query)

                # Get explain plan and validate indexes are being used
                explain_result = self.run_cbq_query(f"EXPLAIN {query}",query_context=self.query_context)
                self.log.info(f"Explain plan for query {self.query_number}: {explain_result}")
                self._validate_indexes_in_plan(explain_result, main_index_name, subquery_index_names)

                # Execute the query
                memory_quota = self.input.param("memory_quota", 100)
                timeout = self.input.param("timeout", "120s")
                compare_cbo = self.input.param("compare_cbo", False)
                
                if compare_cbo:
                    # Get explain plan without CBO
                    explain_no_cbo = self.run_cbq_query(f"EXPLAIN {query}", query_context=self.query_context,
                                                       query_params={'use_cbo': False})
                    explain_str = str(explain_no_cbo)
                    # Check that CBO keywords are NOT present when CBO is disabled
                    if any(keyword in explain_str for keyword in ['optimizer_estimates', 'cardinality', 'fr_cost']):
                        self.log.error(f"Explain plan without CBO: {explain_no_cbo}")
                        self.fail(f"Explain plan without CBO should not contain CBO keywords for query {self.query_number}")

                    # Get explain plan with CBO
                    explain_cbo = self.run_cbq_query(f"EXPLAIN {query}", query_context=self.query_context,
                                                    query_params={'use_cbo': True})
                    explain_str = str(explain_cbo)
                    # Check that CBO keywords are present when CBO is enabled
                    if not all(keyword in explain_str for keyword in ['optimizer_estimates', 'cardinality', 'fr_cost']):
                        self.log.error(f"Explain plan with CBO: {explain_cbo}")
                        self.fail(f"Explain plan with CBO missing required keywords for query {self.query_number}")

                    # Run with CBO disabled
                    self.log.info(f"Executing query {self.query_number} without CBO...")
                    result_no_cbo = self.run_cbq_query(query, query_context=self.query_context, 
                                                      query_params={'memory_quota': memory_quota, 
                                                                  'timeout': timeout,
                                                                  'use_cbo': False})
                    # Parse execution time to milliseconds
                    time_str_no_cbo = result_no_cbo['metrics']['executionTime']
                    if time_str_no_cbo.endswith('ms'):
                        time_no_cbo = float(time_str_no_cbo[:-2])
                    elif time_str_no_cbo.endswith('s'):
                        time_no_cbo = float(time_str_no_cbo[:-1]) * 1000
                    elif time_str_no_cbo.endswith('m'):
                        time_no_cbo = float(time_str_no_cbo[:-1]) * 60 * 1000

                    self.log.info(f"Executing query {self.query_number} with CBO...")
                    result_cbo = self.run_cbq_query(query, query_context=self.query_context,
                                                   query_params={'memory_quota': memory_quota,
                                                               'timeout': timeout, 
                                                               'use_cbo': True})
                    time_str_cbo = result_cbo['metrics']['executionTime']
                    if time_str_cbo.endswith('ms'):
                        time_cbo = float(time_str_cbo[:-2])
                    elif time_str_cbo.endswith('s'):
                        time_cbo = float(time_str_cbo[:-1]) * 1000
                    elif time_str_cbo.endswith('m'):
                        time_cbo = float(time_str_cbo[:-1]) * 60 * 1000
                    
                    # Compare results
                    self.log.info("Comparing results between CBO and non-CBO runs...")
                    diff = DeepDiff(result_cbo['results'], result_no_cbo['results'], 
                                  ignore_order=True, significant_digits=5)
                    if diff:
                        if len(str(diff)) < 5000:
                            self.log.error(f"Results do not match between CBO and non-CBO runs for query {self.query_number}. Differences: {diff}")
                        else:
                            self.log.error(f"Results do not match between CBO and non-CBO runs for query {self.query_number}. Differences too large to display.")
                        self.fail(f"Results do not match between CBO and non-CBO runs for query {self.query_number}")
                    
                    # Compare execution times from metrics
                    time_diff_percent = ((time_no_cbo - time_cbo) / time_no_cbo) * 100
                    self.log.info(f"Query {self.query_number} execution times - No CBO: {time_str_no_cbo}, With CBO: {time_str_cbo}")
                    self.log.info(f"CBO improved execution time by {time_diff_percent:.1f}%")
                    
                    if time_cbo > time_no_cbo * 1.33:  # Allow 33% margin
                        self.log.error(f"CBO execution was significantly slower for query {self.query_number}")
                        self.fail(f"CBO execution was {((time_cbo - time_no_cbo) / time_no_cbo) * 100:.1f}% slower than non-CBO for query {self.query_number}")
                    
                    actual_result = result_cbo  # Use CBO result for further validation
                else:
                    # Original execution path
                    actual_result = self.run_cbq_query(query, query_context=self.query_context, 
                                                     query_params={'memory_quota': memory_quota, 'timeout': timeout})
                    self.log.info(f"Successfully executed query {self.query_number}: {query} with secondary indexes, memory_quota={memory_quota} and timeout={timeout}")

                # Validate actual result matches expected result
                diff = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True, significant_digits=5)
                if diff:
                    # Only log diff if it's not too large
                    if len(str(diff)) < 5000:
                        self.log.error(f"Results do not match for query {self.query_number}. Differences: {diff}")
                    else:
                        self.log.error(f"Results do not match for query {self.query_number}. Differences too large to display.")
                    self.fail(f"Results do not match for query {self.query_number}")
            except Exception as e:
                self.log.error(f"Error details for query {self.query_number}: {str(e)}")
                self.fail(f"Error executing query {self.query_number}: {query}")

    def _validate_indexes_in_plan(self, explain_result, main_index_name, subquery_index_names):
        # Check main index usage
        if main_index_name:
            self.assertTrue(main_index_name in str(explain_result['results'][0]['plan']), f"Main index {main_index_name} is not being used in the query plan please check the plan{explain_result['results'][0]['plan']}")

        # Check subquery indexes usage
        if '~subqueries' in explain_result['results'][0]:
            if subquery_index_names:
                for subquery_index in subquery_index_names:
                    self.assertTrue(subquery_index in str(explain_result['results'][0]['~subqueries']), f"Subquery index {subquery_index} is not being used in the query plan please check the plan{explain_result['results'][0]['plan']}")
            else:
                self.fail("No subquery indexes are being used in the query plan")

    def _create_recommended_indexes(self, query):
        """Helper method to get index recommendations and create indexes"""
        # Get index recommendations from advisor
        advise_query = f"ADVISE {query}"
        advise_result = self.run_cbq_query(advise_query)
        self.log.info(f"Index advice for query {self.query_number}: {advise_result}")

        # Variables to store and return index names
        main_index_name = None
        subquery_index_names = []

        # Create recommended indexes if any
        if 'advice' in advise_result['results'][0]:
            advice = advise_result['results'][0]['advice']['adviseinfo']
            # Handle main query indexes
            if 'recommended_indexes' in advice:
                # Handle covering indexes
                if 'covering_indexes' in advice['recommended_indexes']:
                    index = advice['recommended_indexes']['covering_indexes'][0]
                    try:
                        create_index_result = self.run_cbq_query(index['index_statement'],query_context=self.query_context)
                        main_index_name = create_index_result['results'][0]['name']
                        self.log.info(f"Created recommended covering index: {main_index_name}")
                    except Exception as e:
                        self.log.error(f"Error creating covering index: {str(e)}")

                # Handle regular indexes if no covering indexes are found
                elif 'indexes' in advice['recommended_indexes']:
                    index = advice['recommended_indexes']['indexes'][0]
                    try:
                        create_index_result = self.run_cbq_query(index['index_statement'],query_context=self.query_context)
                        main_index_name = create_index_result['results'][0]['name']
                        self.log.info(f"Created recommended index: {main_index_name}")
                    except Exception as e:
                        self.log.error(f"Error creating index: {str(e)}")

        # Handle subquery indexes
        if '~subqueries' in advise_result['results'][0]:
            for subquery in advise_result['results'][0]['~subqueries']:
                if 'recommended_indexes' in subquery['adviseinfo']:
                    # Handle covering indexes for subquery
                    if 'covering_indexes' in subquery['adviseinfo']['recommended_indexes']:
                        index = subquery['adviseinfo']['recommended_indexes']['covering_indexes'][0]
                        try:
                            create_index_result = self.run_cbq_query(index['index_statement'],query_context=self.query_context)
                            index_name = create_index_result['results'][0]['name']
                            subquery_index_names.append(index_name)
                            self.log.info(f"Created recommended covering index for subquery: {index_name}")
                        except Exception as e:
                            self.log.error(f"Error creating covering index for subquery: {str(e)}")

                    # Handle regular indexes for subquery if no covering indexes
                    elif 'indexes' in subquery['adviseinfo']['recommended_indexes']:
                        index = subquery['adviseinfo']['recommended_indexes']['indexes'][0]
                        try:
                            create_index_result = self.run_cbq_query(index['index_statement'],query_context=self.query_context)
                            index_name = create_index_result['results'][0]['name']
                            subquery_index_names.append(index_name)
                            self.log.info(f"Created recommended index for subquery: {index_name}")
                        except Exception as e:
                            self.log.error(f"Error creating index for subquery: {str(e)}")

        return main_index_name, subquery_index_names