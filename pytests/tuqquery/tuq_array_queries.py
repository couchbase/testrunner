import json
import time
from lib.membase.api.rest_client import RestConnection
import os
from .tuq import QueryTests
from scripts.array_query_generator import ArrayGenerator
from scripts.array_query_generator_links import ArrayGeneratorLinks



class QueryArrayQueryTests(QueryTests):

    def setUp(self):
        super(QueryArrayQueryTests, self).setUp()
        self.log.info("==============  QueryArrayQueryTests setup has started ==============")
        self.num_queries = self.input.param("num_queries", 1)
        self.compare_primary = self.input.param("compare_primary", False)
        self.advisor_node = self.input.advisor
        self.doc_count = self.input.param("doc_count",50000)
        self.log.info("==============  QueryArrayQueryTests setup has started ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryArrayQueryTests, self).suite_setUp()
        self.log.info("==============  QueryArrayQueryTests suite_setup has started ==============")
        self.log.info("==============  QueryArrayQueryTests suite_setup has started ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryArrayQueryTests tearDown has started ==============")
        self.log.info("==============  QueryArrayQueryTests tearDown has started ==============")
        super(QueryArrayQueryTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryArrayQueryTests suite_tearDown has started ==============")
        self.log.info("==============  QueryArrayQueryTests suite_tearDown has started ==============")
        super(QueryArrayQueryTests, self).suite_tearDown()


    ''' This will generate n number of queries based on an input param. It will run the advisor to detemine the indexes
        needed for the query, create the index, run the query, run the same query on analytics, and compare the results'''
    def test_array_queries(self):
        final_query_list = []
        total_passed_queries = 0
        total_failed_queries = 0
        failed_queries = []

        # Load travel-sample
        self.rest.load_sample("travel-sample")
        time.sleep(60)

        # Second server is the cbas server
        cbas_connection = RestConnection(self.servers[1])

        if not self.compare_primary:
            # Create analytics dataset on travel-sample
            cmd_create_dataset = "create dataset `travel-sample` on `travel-sample`;"
            response = cbas_connection.execute_statement_on_cbas(cmd_create_dataset, None,
                                                                 username=cbas_connection.username,
                                                                 password=cbas_connection.password)
            response = json.loads(response)
            self.assertTrue(response['status'] == 'success', "Dataverse failed to be created with error: %s" % response)

            cmd_connect_bucket = "connect link Local;"
            response = cbas_connection.execute_statement_on_cbas(cmd_connect_bucket, None,
                                                                 username=cbas_connection.username,
                                                                 password=cbas_connection.password)
            response = json.loads(response)
            self.assertTrue(response['status'] == 'success', "Dataverse failed to link with error: %s" % response)

        # Sleep to let dataverse load all items
        time.sleep(60)

        # Get the list of indexes to drop and create a fresh system
        list_of_indexes = self.run_cbq_query(query="select raw name from system:indexes where keyspace_id='travel-sample' and namespace_id='default'")

        for index in list_of_indexes['results']:
            if index == "#primary":
                continue
            else:
                # Leave the primary index for within comparisons
                if index == "def_primary":
                    continue
                else:
                    self.run_cbq_query(query="drop index `travel-sample`.`%s`" % index)

        # Generate final list of queries, total number of queries is the number * ~190
        for i in range(self.num_queries):
            query_list = ArrayGenerator().generate_query_pairs()
            final_query_list += query_list
        x = 1
        # Execute queries sequentially and compare against analytics results, capture results of comparison
        for query in final_query_list:
            # Generate primary_index query we wish to run
            split_query = query.split("WHERE")
            primary_index_query= split_query[0] + "USE INDEX (def_primary) WHERE" + split_query[1]
            required_index = ''
            cbas_query = ''
            try:
                self.log.info("=" * 50 + " Running Query %s " % str(x) + "=" * 50)
                # Get the covering index required to run the query (we want to test optimized query paths
                advise_query = "advise " + query
                advise_query = advise_query.replace("`travel-sample`", "bucket_01")
                advise_result = self.run_cbq_query(query= advise_query, server=self.master, username=self.rest.username, password= self.rest.password)
                required_index = advise_result['results'][0]['advice']['adviseinfo'][0]['recommended_indexes']['covering_indexes'][0]['index_statement']
            except Exception as e:
                # Sometimes the advisor will not advise a covering index, so in this case we need to grab a different index recommendation
                if str(e) == "'covering_indexes'":
                    advise_result = self.run_cbq_query(query=advise_query, server=self.master, username=self.rest.username, password= self.rest.password)
                    required_index = advise_result['results'][0]['advice']['adviseinfo'][0]['recommended_indexes']['indexes'][0]['index_statement']
            try:
                if required_index:
                    required_index = required_index.replace("bucket_01", "travel-sample")
                    self.run_cbq_query(query=required_index)
                    self.wait_for_all_indexes_online()
                    index_name = required_index.split("INDEX")[1].split("ON")[0].strip()
                    # Ensure the created queries are being run against the correct index
                    explain_query = "explain " + query
                    explain_plan= self.run_cbq_query(query=explain_query)
                    if not (index_name in str(explain_plan)):
                        total_failed_queries += 1
                        failed_queries.append(
                            ("N1QL Query: %s , CBAS Query: %s" % (query, cbas_query), "Query is not using the created index"))

                actual_result = self.run_cbq_query(query=query)

                if self.compare_primary or "within" in query:
                    expected_result = self.run_cbq_query(query=primary_index_query)
                    self.log.info("=" * 100)
                else:
                    # Need to slightly rewrite queries to run against analytics
                    cbas_query = query
                    cbas_query = cbas_query.replace("ANY ", "(ANY ")
                    cbas_query = cbas_query.replace("EVERY ", "(EVERY ")
                    cbas_query = cbas_query.replace("SOME ", "(SOME ")
                    cbas_query = cbas_query.replace("END", "END)")
                    cbas_query = cbas_query.replace("`travel-sample`", "`travel-sample` t")
                    cbas_query = cbas_query.replace("schedule", "t.schedule")
                    cbas_query = cbas_query.replace("reviews", "t.reviews")
                    cbas_query = cbas_query.replace("public_likes", "t.public_likes")
                    self.log.info("RUN cbas query: %s" % cbas_query)
                    response = cbas_connection.execute_statement_on_cbas(cbas_query, None, username=cbas_connection.username, password=cbas_connection.password)
                    expected_result = json.loads(response)
                    self.log.info("=" * 100)
                if actual_result['results'] != expected_result['results']:
                    total_failed_queries += 1
                    failed_queries.append(("N1QL Query: %s , CBAS Query: %s" % (query,cbas_query), "Mismatch of results"))
                else:
                    total_passed_queries += 1
            except Exception as e:
                total_failed_queries+=1
                failed_queries.append(("N1QL Query: %s , CBAS Query: %s" % (query,cbas_query), "Exception %s" % str(e)))
                self.log.error("Query failed to run! %s" % str(e))
            finally:
                if required_index:
                    try:
                        self.run_cbq_query(query="DROP INDEX `travel-sample`.%s" % index_name)
                    except Exception as e:
                        self.log.error("Something went wrong while dropping index!: "  + str(e))
            x+=1

        self.log.info("=" * 100)
        self.log.info("Queries Passed: %s , Queries Failed: %s" % (total_passed_queries, total_failed_queries))
        self.log.info("=" * 100)

        if total_failed_queries != 0:
            for query in failed_queries:
                self.log.info("Query failed : %s , Reason : %s" % (query[0],query[1]))
                self.log.info("-" * 50)
            self.fail("Some queries failed, see detailed summary above.")

    ''' This will generate n number of queries based on an input param. It will run the advisor to detemine the indexes
        needed for the query, create the index, run the query, run the same query on analytics, and compare the results'''
    def test_array_queries_links(self):
        final_query_list = []
        total_passed_queries = 0
        total_failed_queries = 0
        failed_queries = []

        #shell = RemoteMachineShellConnection(self.master)
        cmd = "fakeit couchbase --server http://%s:%s --bucket default --count %s --username Administrator --password password --verbose resources/yaml/links.yaml" % (self.master.ip, self.master.port, self.doc_count)
        #output, error = shell.execute_command(cmd)
        os.system(cmd)


        # Second server is the cbas server
        cbas_connection = RestConnection(self.servers[1])

        if not self.compare_primary:
            # Create analytics dataset on travel-sample
            cmd_create_dataset = "create dataset `default` on `default`;"
            response = cbas_connection.execute_statement_on_cbas(cmd_create_dataset, None,
                                                                 username=cbas_connection.username,
                                                                 password=cbas_connection.password)
            response = json.loads(response)
            self.assertTrue(response['status'] == 'success', "Dataverse failed to be created with error: %s" % response)

            cmd_connect_bucket = "connect link Local;"
            response = cbas_connection.execute_statement_on_cbas(cmd_connect_bucket, None,
                                                                 username=cbas_connection.username,
                                                                 password=cbas_connection.password)
            response = json.loads(response)
            self.assertTrue(response['status'] == 'success', "Dataverse failed to link with error: %s" % response)

        # Sleep to let dataverse load all items
        time.sleep(60)

        # Get the list of indexes to drop and create a fresh system
        list_of_indexes = self.run_cbq_query(query= "select raw name from system:indexes")

        for index in list_of_indexes['results']:
            if index == "#primary":
                continue
            else:
                # Leave the primary index for within comparisons
                if index == "def_primary":
                    continue
                else:
                    self.run_cbq_query(query = "drop index `default`.`%s`" % index)

        # Generate final list of queries, total number of queries is the number * ~190
        for i in range(self.num_queries):
            query_list = ArrayGeneratorLinks().generate_query_pairs()
            final_query_list += query_list
        x = 1
        # Execute queries sequentially and compare against analytics results, capture results of comparison
        for query in final_query_list:
            # Generate primary_index query we wish to run
            query = query.replace("b.", "b.bdayinfo.")
            query = query.replace("Number", "`Number`")
            query = query.replace("E.Emails", "E")
            query = query.replace("o.occur", "o")
            query = query.replace("p.prefixes", "p")
            query = query.replace("last", "`last`")
            query = query.replace("first", "`first`")
            split_query = query.split("WHERE")
            primary_index_query= split_query[0] + "USE INDEX (`#primary`) WHERE" + split_query[1]
            required_index = ''
            cbas_query = ''
            try:
                self.log.info("=" * 50 + " Running Query %s " % str(x) + "=" * 50)
                # Get the covering index required to run the query (we want to test optimized query paths
                advise_query = "advise " + query
                advise_query = advise_query.replace("default", "bucket_01")
                advise_result = self.run_cbq_query(query= advise_query, server=self.master, username=self.rest.username, password= self.rest.password)
                required_index = advise_result['results'][0]['advice']['adviseinfo'][0]['recommended_indexes']['covering_indexes'][0]['index_statement']
            except Exception as e:
                # Sometimes the advisor will not advise a covering index, so in this case we need to grab a different index recommendation
                if str(e) == "'covering_indexes'":
                    advise_result = self.run_cbq_query(query=advise_query, server=self.master, username=self.rest.username, password= self.rest.password)
                    required_index = advise_result['results'][0]['advice']['adviseinfo'][0]['recommended_indexes']['indexes'][0]['index_statement']
            try:
                if required_index:
                    required_index = required_index.replace("bucket_01", "default")
                    self.run_cbq_query(query=required_index)
                    self.wait_for_all_indexes_online()
                    index_name = required_index.split("INDEX")[1].split("ON")[0].strip()
                    # Ensure the created queries are being run against the correct index
                    explain_query = "explain " + query
                    explain_plan= self.run_cbq_query(query=explain_query)
                    if not (index_name in str(explain_plan)):
                        total_failed_queries += 1
                        failed_queries.append(
                            ("N1QL Query: %s , CBAS Query: %s" % (query, cbas_query), "Query is not using the created index"))

                actual_result = self.run_cbq_query(query=query)

                if self.compare_primary or "within" in query:
                    expected_result = self.run_cbq_query(query=primary_index_query)
                    self.log.info("=" * 100)
                else:
                    # Need to slightly rewrite queries to run against analytics
                    cbas_query = query
                    cbas_query = cbas_query.replace("ANY ", "(ANY ")
                    cbas_query = cbas_query.replace("EVERY ", "(EVERY ")
                    cbas_query = cbas_query.replace("SOME ", "(SOME ")
                    cbas_query = cbas_query.replace("END", "END)")
                    cbas_query = cbas_query.replace("`default`", "`default` d")
                    cbas_query = cbas_query.replace("ident", "d.ident")
                    cbas_query = cbas_query.replace("type", "`type`")
                    self.log.info("RUN cbas query: %s" % cbas_query)
                    response = cbas_connection.execute_statement_on_cbas(cbas_query, None, username=cbas_connection.username, password=cbas_connection.password)
                    expected_result = json.loads(response)
                    self.log.info("=" * 100)
                if actual_result['results'] != expected_result['results']:
                    total_failed_queries += 1
                    failed_queries.append(("N1QL Query: %s , CBAS Query: %s" % (query,cbas_query), "Mismatch of results"))
                else:
                    total_passed_queries += 1
            except Exception as e:
                total_failed_queries+=1
                failed_queries.append(("N1QL Query: %s , CBAS Query: %s" % (query,cbas_query), "Exception %s" % str(e)))
                self.log.error("Query failed to run! %s" % str(e))
            finally:
                if required_index:
                    try:
                        self.run_cbq_query(query="DROP INDEX `default`.%s" % index_name)
                    except Exception as e:
                        self.log.error("Something went wrong while dropping index!: "  + str(e))
            x+=1

        self.log.info("=" * 100)
        self.log.info("Queries Passed: %s , Queries Failed: %s" % (total_passed_queries, total_failed_queries))
        self.log.info("=" * 100)

        if total_failed_queries != 0:
            for query in failed_queries:
                self.log.info("Query failed : %s , Reason : %s" % (query[0],query[1]))
                self.log.info("-" * 50)
            self.fail("Some queries failed, see detailed summary above.")