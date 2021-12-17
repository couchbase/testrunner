from lib.Cb_constants.CBServer import CbServer
import requests
from .tuq import QueryTests
from .flex_index_phase1 import FlexIndexTests

class QuerySanityTLSTests(QueryTests):
    def setUp(self):
        super(QuerySanityTLSTests, self).setUp()
        self.custom_map = self.input.param("custom_map", False)
        self.fts_index_type = self.input.param("fts_index_type", "scorch")
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql",get_all_nodes=True)[1]
        self.enable_dp = self.input.param("enable_dp", True)
        self.use_https = self.input.param("use_https", True)
        self.enforce_tls = self.input.param("enforce_tls", True)
        if self.use_https:
            CbServer.use_https = True

    def tearDown(self):
        pass

    def suite_setUp(self):
        super(QuerySanityTLSTests, self).suite_setUp()

    def suite_tearDown(self):
        super(QuerySanityTLSTests, self).suite_tearDown()

    def test_dml_bucket(self):
        try:
            _ = self.run_cbq_query("CREATE INDEX adv_job_title ON `default`(`job_title`)", server=self.query_node)
            queries = {
                'select': 'SELECT name FROM default WHERE job_title = "Engineer" AND join_yr = 2011 AND join_mo = 10',
                'update': 'UPDATE default SET job_title = "ENGINEER" WHERE join_yr = 2011 AND join_mo = 10 AND lower(job_title) = "engineer" RETURNING name, job_title',
                'insert': 'INSERT INTO default (KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" }) RETURNING *',
                'upsert': 'UPSERT INTO default (KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" }) RETURNING *',
                'delete': 'DELETE FROM default WHERE name = "new hotel" RETURNING *'
            }
            for type, query in queries.items():
                    result = self.run_cbq_query(query, server=self.query_node)
                    self.log.info(result)
        finally:
            self.run_cbq_query('DROP INDEX default.adv_job_title', server=self.query_node)
 
    def test_create_index(self):
        try:
            result = self.run_cbq_query('CREATE INDEX adv_job_title ON default(job_title)', server=self.query_node)
        finally:
            self.run_cbq_query('DROP INDEX default.adv_job_title', server=self.query_node)

    def test_prepared_statement(self):
        prepared_query = 'PREPARE engineer_count as SELECT COUNT(*) as count_engineer FROM default WHERE job_title = "Engineer"'
        self.run_cbq_query(prepared_query, server=self.query_node)
        results = self.run_cbq_query(query="EXECUTE engineer_count", server=self.query_node)
        self.assertEqual(results['results'], [{'count_engineer': 644}])

    def test_curl(self):
        url = "https://jsonplaceholder.typicode.com/todos"
        self.rest.create_whitelist(self.master, {"all_access": True})
        response = requests.get(url)
        expected_curl = response.json()        
        results = self.run_cbq_query("SELECT CURL('%s')" % url, server=self.query_node)
        actual_curl = results['results'][0]['$1']
        self.assertEqual(actual_curl, expected_curl)

    def test_fts_search(self):
        search_query = 'SELECT SEARCH_META() AS meta FROM default \
            AS t1 WHERE SEARCH(t1, { \
                "query": {"match": "ubuntu", "fields": "VMs.os", "analyzer": "standard"}, \
                "includeLocations": true }) LIMIT 3'
        results = self.run_cbq_query(search_query, server=self.query_node)
        self.log.info(results)

    def test_flex_index(self):
        ft_object = FlexIndexTests()
        ft_object.init_flex_object(self)
        flex_query_list = ["select meta().id from default {0} where name = 'employee-6'"]
        self.create_fts_index(
            name="default_index", source_name="default", doc_count=2017, index_storage_type=self.fts_index_type)
        failed_to_run_query, not_found_index_in_response, result_mismatch = ft_object.run_query_and_validate(flex_query_list)
        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                        "or flex query and gsi query results not matching: {2}"
                        .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All queries passed")
