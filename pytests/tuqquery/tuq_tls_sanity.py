from .tuq import QueryTests
from .flex_index_phase1 import FlexIndexTests
import time
import json
import requests
from pytests.security.x509_multiple_CA_util import x509main
from security.ntonencryptionBase import ntonencryptionBase
from lib.Cb_constants.CBServer import CbServer

class QuerySanityTLSTests(QueryTests):

    def setUp(self):
        super(QuerySanityTLSTests, self).setUp()
        self.custom_map = self.input.param("custom_map", False)
        self.multi_root = self.input.param("multi_root", True)
        self.rotate_cert = self.input.param("rotate_cert", False)
        self.fts_index_type = self.input.param("fts_index_type", "scorch")
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql",get_all_nodes=True)[1]
        self.log.info(f'####### Query Node: {self.query_node}')
        self.standard = self.input.param("standard", "pkcs8")
        self.passphrase_type = self.input.param("passphrase_type", "script")
        self.encryption_type = self.input.param("encryption_type", "aes256")
        self.spec_file = self.input.param("spec_file", "default")
        self.x509 = x509main(host=self.master, standard=self.standard,
                            encryption_type=self.encryption_type,
                            passphrase_type=self.passphrase_type)
        if self.rotate_cert and self.multi_root:
            ntonencryptionBase().disable_nton_cluster([self.master])
            self.use_https = False
            CbServer.use_https = False
            self.log.info("###### ROTATING certificate")
            self.x509.rotate_certs(self.servers, "all")
            self.log.info("Manifest after rotating certs #########\n {0}".format(json.dumps(self.x509.manifest, indent=4)))
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
            self.use_https = True
            CbServer.use_https = True

    def tearDown(self):
        pass

    def suite_setUp(self):
        super(QuerySanityTLSTests, self).suite_setUp()
        # For setup multi root CA
        if self.multi_root:
            self.log.info("##### SETUP Multi Root CA ########")
            for server in self.servers:
                self.x509.delete_inbox_folder_on_server(server=server)
            self.x509.generate_multiple_x509_certs(servers=self.servers, spec_file_name=self.spec_file)
            self.log.info("Manifest #########\n {0}".format(json.dumps(self.x509.manifest, indent=4)))
            for server in self.servers:
                _ = self.x509.upload_root_certs(server)
            self.x509.upload_node_certs(servers=self.servers)
            self.x509.delete_unused_out_of_the_box_CAs(server=self.master)

        # Enforce TLS
        self.log.info("##### Enforcing TLS ########")
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        self.use_https = True
        CbServer.use_https = True

    def suite_tearDown(self):
        ntonencryptionBase().disable_nton_cluster([self.master])

    def test_dml_bucket(self):
        try:
            _ = self.run_cbq_query("CREATE INDEX adv_job_title ON `default`(`job_title`)", server=self.query_node)
            queries = {
                'select': f'SELECT name FROM default WHERE job_title = "Engineer" AND join_yr = 2011 AND join_mo = 10',
                'update': f'UPDATE default SET job_title = "ENGINEER" WHERE join_yr = 2011 AND join_mo = 10 AND lower(job_title) = "engineer" RETURNING name, job_title',
                'insert': f'INSERT INTO default (KEY, VALUE) VALUES ("key1", {{ "type" : "hotel", "name" : "new hotel" }}) RETURNING *',
                'upsert': f'UPSERT INTO default (KEY, VALUE) VALUES ("key1", {{ "type" : "hotel", "name" : "new hotel" }}) RETURNING *',
                'delete': f'DELETE FROM default WHERE name = "new hotel" RETURNING *',
                'merge': 'MERGE INTO default t USING [{"job_title":"Engineer"}] source ON t.job_title = source.job_title ' \
                    'WHEN MATCHED THEN UPDATE SET t.old_tile = "Engineer", t.job_title = "Ingenieur" ' \
                    'WHERE t.join_yr = 2011 AND t.join_mo = 11 LIMIT 2 RETURNING *'
            }
            for type, query in queries.items():
                with self.subTest(type):
                    result = self.run_cbq_query(query, server=self.query_node)
                    self.log.info(result)
        finally:
            self.run_cbq_query('DROP INDEX adv_job_title ON default', server=self.query_node)

    def test_dml_scope_collection(self):
        try:
            self.run_cbq_query('DROP SCOPE default.company IF EXISTS', server=self.query_node)
            self.sleep(3)
            self.run_cbq_query('CREATE SCOPE default.company', server=self.query_node)
            self.sleep(3)
            self.run_cbq_query('CREATE COLLECTION default.company.employees', server=self.query_node)
            self.sleep(3)
            self.run_cbq_query('CREATE PRIMARY INDEX on default.company.employees', server=self.query_node)
            self.run_cbq_query('INSERT INTO default.company.employees (KEY UUID(), VALUE _employee) SELECT _employee FROM default _employee', server=self.query_node)
            queries = {
                'select': f'SELECT name FROM default.company.employees WHERE job_title = "Engineer" AND join_yr = 2011 AND join_mo = 10',
                'update': f'UPDATE default.company.employees SET job_title = "ENGINEER" WHERE join_yr = 2011 AND join_mo = 10 AND lower(job_title) = "engineer" RETURNING name, job_title',
                'insert': f'INSERT INTO default.company.employees (KEY, VALUE) VALUES ("key1", {{ "type" : "hotel", "name" : "new hotel" }}) RETURNING *',
                'upsert': f'UPSERT INTO default.company.employees (KEY, VALUE) VALUES ("key1", {{ "type" : "hotel", "name" : "new hotel" }}) RETURNING *',
                'delete': f'DELETE FROM default.company.employees WHERE name = "new hotel" RETURNING *',
            }
            for type, query in queries.items():
                with self.subTest(type):
                    result = self.run_cbq_query(query, server=self.query_node)
                    self.log.info(result)
        finally:
            self.run_cbq_query('DROP SCOPE default.company IF EXISTS', server=self.query_node)

 
    def test_create_index(self):
        try:
            result = self.run_cbq_query('CREATE INDEX adv_job_title ON default(job_title)', server=self.query_node)
        finally:
            self.run_cbq_query('DROP INDEX adv_job_title ON default', server=self.query_node)

    def test_create_scope_collection(self):
        try:
            result = self.run_cbq_query('CREATE SCOPE default.scope1', server=self.query_node)
            result = self.run_cbq_query('CREATE SCOPE default.scope2', server=self.query_node)
            result = self.run_cbq_query('CREATE COLLECTION default.scope1.collection1', server=self.query_node)
            result = self.run_cbq_query('CREATE COLLECTION default.scope1.collection2', server=self.query_node)
        finally:
            self.run_cbq_query('DROP SCOPE default.scope1', server=self.query_node)
            self.run_cbq_query('DROP SCOPE default.scope2', server=self.query_node)

    def test_transaction(self):
        results = self.run_cbq_query(query='BEGIN WORK', server=self.query_node)
        txid = results['results'][0]['txid']
        result = self.run_cbq_query(query=f'SELECT job_title, count(*) FROM default GROUP BY job_title', txnid=txid, server=self.query_node)
        self.log.info(result)
        self.run_cbq_query(query='COMMIT', txnid=txid, server=self.query_node)

    def test_udf(self):
        functions = 'function adder(a, b, c) { for (i=0; i< b; i++){a = a + c;} return a; }'
        function_names = ["adder"]
        self.log.info("Create math library")
        self.use_https = True
        self.create_library("math", functions, function_names)
        try:
            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b,c) LANGUAGE JAVASCRIPT AS "adder" AT "math"', server=self.query_node)
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3,5)", server=self.query_node)
            self.assertEqual(results['results'], [16])
        finally:
            try:
                self.log.info("Delete math library")
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1", server=self.query_node)
            except Exception as e:
                self.log.error(str(e))

    def test_prepared_statement(self):
        prepared_query = f'PREPARE engineer_count as SELECT COUNT(*) as count_engineer FROM default WHERE job_title = "Engineer"'
        self.run_cbq_query(prepared_query, server=self.query_node)
        results = self.run_cbq_query(query="EXECUTE engineer_count", server=self.query_node)
        self.assertEqual(results['results'], [{'count_engineer': 642}])

    def test_advisor(self):
        results = self.run_cbq_query(query="SELECT ADVISOR({'action': 'start', 'duration': '1h', 'query_count': 2 })", server=self.query_node)
        session = results['results'][0]['$1']['session']
        results = self.run_cbq_query(query=f'SELECT * FROM default WHERE job_title = "Engineer" LIMIT 3', server=self.query_node)
        results = self.run_cbq_query(query=f'SELECT * FROM default WHERE job_title = "Engineer" LIMIT 3', server=self.query_node)
        self.sleep(3)
        results = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'stop', 'session': '{session}'}})", server=self.query_node)
        # Stop a second time to ensure no side effect (see MB-48576)
        results = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'stop', 'session': '{session}'}})", server=self.query_node)
        results = self.run_cbq_query(query=f"SELECT ADVISOR({{'action': 'get', 'session': '{session}'}})", server=self.query_node)
        self.assertTrue('recommended_indexes' in results['results'][0]['$1'][0][0], f"There are no recommended index: {results['results'][0]['$1'][0][0]}")

    def test_curl(self):
        url = "https://jsonplaceholder.typicode.com/todos"
        self.rest.create_whitelist(self.master, {"all_access": True})
        response = requests.get(url)
        expected_curl = response.json()        
        results = self.run_cbq_query(f"SELECT CURL('{url}')", server=self.query_node)
        actual_curl = results['results'][0]['$1']
        self.assertEqual(actual_curl, expected_curl)

    def test_fts_search(self):
        search_query = f'SELECT SEARCH_META() AS meta FROM default \
            AS t1 WHERE SEARCH(t1, {{ \
                "query": {{"match": "ubuntu", "fields": "VMs.os", "analyzer": "standard"}}, \
                "includeLocations": true }}) LIMIT 3'
        results = self.run_cbq_query(search_query, server=self.query_node)
        self.log.info(results)

    def test_flex_index(self):
        ft_object = FlexIndexTests()
        ft_object.init_flex_object(self)
        flex_query_list = ["select meta().id from default {0} where name = 'employee-6'"]
        ft_object.fts_index = self.create_fts_index(
            name="default_index", source_name="default", doc_count=2016, index_storage_type=self.fts_index_type)        
        failed_to_run_query, not_found_index_in_response, result_mismatch = ft_object.run_query_and_validate(flex_query_list)
        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                        "or flex query and gsi query results not matching: {2}"
                        .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All queries passed")
