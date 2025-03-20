import json
import time
from lib.membase.api.rest_client import RestConnection
import os
from .tuq import QueryTests
from deepdiff import DeepDiff

class QueryAiQGTests(QueryTests):

    def setUp(self):
        super(QueryAiQGTests, self).setUp()
        self.log.info("==============  QueryAiQGTests setup has started ==============")
        self.sql_file_path = self.input.param("sql_file_path", "path/to/your/queries.sql")
        self.query_number = self.input.param("query_number", 0)
        self.query_context = self.input.param("query_context", "default._default")
        self.log.info("==============  QueryAiQGTests setup has started ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryAiQGTests, self).suite_setUp()
        self.log.info("==============  QueryAiQGTests suite_setup has started ==============")
        # Create collections in default._default scope
        try:
            self.run_cbq_query("CREATE COLLECTION default._default.hotel IF NOT EXISTS")
            self.run_cbq_query("CREATE COLLECTION default._default.reviews IF NOT EXISTS") 
            self.run_cbq_query("CREATE COLLECTION default._default.bookings IF NOT EXISTS")
            self.run_cbq_query("CREATE COLLECTION default._default.users IF NOT EXISTS")
            self.log.info("Created collections: hotel, reviews, bookings, users")
        except Exception as e:
            self.log.error(f"Error creating collections: {str(e)}")
            raise
        self.log.info("==============  QueryAiQGTests suite_setup has started ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryAiQGTests tearDown has started ==============")
        self.log.info("==============  QueryAiQGTests tearDown has started ==============")
        super(QueryAiQGTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryAiQGTests suite_tearDown has started ==============")
        self.log.info("==============  QueryAiQGTests suite_tearDown has started ==============")
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

        # Validate query number
        if self.query_number < 0 or self.query_number > len(queries):
            raise ValueError(f"Invalid query number {self.query_number}. File contains {len(queries)} queries.")

        # Get the specified query
        query = queries[self.query_number].strip()

        if query:  # Ensure query isn't empty
            try:
                # Drop any existing indexes first
                try:
                    result = self.run_cbq_query("SELECT name,keyspace_id,scope_id,bucket_id FROM system:indexes")
                    for index in result['results']:
                        if 'scope_id' not in index or 'bucket_id' not in index:
                            self.run_cbq_query(f"DROP INDEX {index['keyspace_id']}.{index['name']}")
                        else:
                            self.run_cbq_query(f"DROP INDEX {index['bucket_id']}.{index['scope_id']}.{index['keyspace_id']}.{index['name']}")
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
                self._validate_indexes_in_plan(explain_result, main_index_name, subquery_index_names)

                # Execute the query
                actual_result = self.run_cbq_query(query,query_context=self.query_context)
                self.log.info(f"Successfully executed query {self.query_number}: {query} with secondary indexes")

                # Validate actual result matches expected result
                if 'ORDER BY' not in query.upper():
                    diff = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                else:
                    diff = DeepDiff(actual_result['results'], expected_result['results'])
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
        result = self.run_cbq_query(advise_query)

        # Variables to store and return index names
        main_index_name = None
        subquery_index_names = []

        # Create recommended indexes if any
        if 'advice' in result['results'][0]:
            advice = result['results'][0]['advice']['adviseinfo']
            # Handle main query indexes
            if 'recommended_indexes' in advice:
                # Handle covering indexes
                if 'covering_indexes' in advice['recommended_indexes']:
                    index = advice['recommended_indexes']['covering_indexes'][0]
                    try:
                        result = self.run_cbq_query(index['index_statement'],query_context=self.query_context)
                        main_index_name = result['results'][0]['name']
                        self.log.info(f"Created recommended covering index: {main_index_name}")
                    except Exception as e:
                        self.log.error(f"Error creating covering index: {str(e)}")

                # Handle regular indexes if no covering indexes are found
                elif 'indexes' in advice['recommended_indexes']:
                    index = advice['recommended_indexes']['indexes'][0]
                    try:
                        result = self.run_cbq_query(index['index_statement'],query_context=self.query_context)
                        main_index_name = result['results'][0]['name']
                        self.log.info(f"Created recommended index: {main_index_name}")
                    except Exception as e:
                        self.log.error(f"Error creating index: {str(e)}")

        # Handle subquery indexes
        if '~subqueries' in result['results'][0]:
            for subquery in result['results'][0]['~subqueries']:
                if 'recommended_indexes' in subquery['adviseinfo']:
                    # Handle covering indexes for subquery
                    if 'covering_indexes' in subquery['adviseinfo']['recommended_indexes']:
                        index = subquery['adviseinfo']['recommended_indexes']['covering_indexes'][0]
                        try:
                            result = self.run_cbq_query(index['index_statement'],query_context=self.query_context)
                            index_name = result['results'][0]['name']
                            subquery_index_names.append(index_name)
                            self.log.info(f"Created recommended covering index for subquery: {index_name}")
                        except Exception as e:
                            self.log.error(f"Error creating covering index for subquery: {str(e)}")

                    # Handle regular indexes for subquery if no covering indexes
                    elif 'indexes' in subquery['adviseinfo']['recommended_indexes']:
                        index = subquery['adviseinfo']['recommended_indexes']['indexes'][0]
                        try:
                            result = self.run_cbq_query(index['index_statement'],query_context=self.query_context)
                            index_name = result['results'][0]['name']
                            subquery_index_names.append(index_name)
                            self.log.info(f"Created recommended index for subquery: {index_name}")
                        except Exception as e:
                            self.log.error(f"Error creating index for subquery: {str(e)}")

        return main_index_name, subquery_index_names