from basetestcase import BaseTestCase
from membase.helper.cluster_helper import ClusterOperationHelper
from sdk_client import SDKClient
from mysql_client import MySQLClient
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.tuq_helper import N1QLHelper
from couchbase_helper.query_helper import QueryHelper

class RQGTests(BaseTestCase):
    """ Class for defining tests for python sdk """

    def setUp(self):
        super(RQGTests, self).setUp()
        self.log.info("==============  RQGTests setup was finished for test #{0} {1} =============="\
                      .format(self.case_number, self._testMethodName))
        self.database= self.input.param("database","flightstats")
        self.user_id= self.input.param("user_id","root")
        self.password= self.input.param("password","")
        self.mysql_url= self.input.param("mysql_url","localhost")
        self.n1ql_server = self.get_nodes_from_services_map(service_type = "n1ql")
        self.query_helper = QueryHelper()
        self._initialize_mysql_client()
        self._setup_and_load_buckets()
        self._initialize_n1ql_helper()
        self._build_primary_indexes()

    def tearDown(self):
        super(RQGTests, self).tearDown()

    def test_rqg_example(self):
        sql_query = "SELECT a1.* FROM ( `ontime_mysiam`  AS a1 INNER JOIN `carriers`  AS a2 ON ( a1.`carrier` = a2.`code` ) )"
        n1ql_query = "SELECT a1.* FROM `ontime_mysiam`  AS a1 INNER JOIN `carriers`  AS a2 ON KEYS [ a1.`carrier` ]"
        # Run n1ql query
        check, msg = self._run_queries_compare(n1ql_query = n1ql_query , sql_query = sql_query)
        self.assertTrue(check, msg)

    def test_rqg_from_list(self):
        self.n1ql_file_path= self.input.param("n1ql_file_path","default")
        self.sql_file_path= self.input.param("sql_file_path","default")
        with open(self.n1ql_file_path) as f:
            n1ql_query_list = f.readlines()
        with open(self.sql_file_path) as f:
            sql_query_list = f.readlines()
        i = 0
        check = True
        pass_case = 0
        total =0
        fail_case = 0
        failure_map = {}
        for n1ql_query in n1ql_query_list:
            sql_query = sql_query_list[i]
            i+=1
            # Run n1ql query
            hints = self.query_helper._find_hints(n1ql_query)
            success, msg = self._run_queries_compare(n1ql_query = n1ql_query , sql_query = sql_query)
            total += 1
            check = check and success
            if success:
                pass_case += 1
            else:
                fail_case +=  1
                failure_map[str[1]] = { "sql_query":sql_query, "n1ql_query": n1ql_query, "reason for failure": msg}
        self.log.info(" Total Queries Run = {0}, Pass Percentage = {1}".format(total, (100*(pass_case/total))))
        self.assertTrue(check, failure_map)

    def test_n1ql_queries_only(self):
        self.n1ql_file_path= self.input.param("n1ql_file_path","default")
        with open(self.n1ql_file_path) as f:
            n1ql_query_list = f.readlines()
        failure_list = []
        check = True
        for n1ql_query in n1ql_query_list:
            try:
                self._run_n1ql_queries(n1ql_query = n1ql_query)
            except Exception, ex:
                self.log.info(ex)
                check = False
                failure_list.append({"n1ql_query":n1ql_query, "reason":ex})
        self.assertTrue(check, failure_list)

    def _run_n1ql_queries(self, n1ql_query = None):
        # Run n1ql query
        actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server)

    def _run_queries_compare(self, n1ql_query = None, sql_query = None):
        # Run n1ql query
        hints = self.query_helper._find_hints(n1ql_query)
        actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server)
        n1ql_result = actual_result["results"]
        # Run SQL Query
        columns, rows = self.client._execute_query(query = sql_query)
        sql_result = self.client._gen_json_from_results(columns, rows)
        self.log.info(" result from n1ql query returns {0} items".format(len(n1ql_result)))
        self.log.info(" result from sql query returns {0} items".format(len(sql_result)))
        try:
            self.log.info("Sample SQL Result {0}".format(sql_result[0]))
            self.log.info("N1QL Result {0}".format(n1ql_result[0]))
            self.n1ql_helper._verify_results_rqg(sql_result = sql_result, n1ql_result = n1ql_result, hints = hints)
        except Exception, ex:
            self.log.info(ex)
            return False, ex
        return True, "Pass"

    def _build_primary_indexes(self, using_gsi= True, bucket_list = []):
        self.n1ql_helper.create_primary_index(using_gsi = using_gsi,server = self.n1ql_server)

    def _load_data_in_buckets(self, bucket_name, data_set):
        scheme = "couchbase"
        host=self.master.ip
        if self.master.ip == "127.0.0.1":
            scheme = "http"
            host="{0}:{1}".format(self.master.ip,self.master.port)
        client = SDKClient(scheme=scheme,hosts = [host], bucket = bucket_name)
        client.upsert_multi(data_set)
        client.close()

    def _initialize_n1ql_helper(self):
        self.n1ql_helper = N1QLHelper(version = "sherlock", shell = None,
            use_rest = True, max_verify = self.max_verify,
            buckets = self.buckets, item_flag = None,
            n1ql_port = self.n1ql_server.n1ql_port, full_docs_list = [],
            log = self.log, input = self.input, master = self.master)

    def _initialize_mysql_client(self):
        self.client = MySQLClient(database = self.database, host = self.mysql_url,
            user_id = self.user_id, password = self.password)

    def _setup_and_load_buckets(self):
        # Remove any previous buckets
        rest = RestConnection(self.master)
        for bucket in self.buckets:
            rest.delete_bucket(bucket.name)
        self.buckets = []
        # Pull information about tables from mysql database and interpret them as no-sql dbs
        table_key_map = self.client._get_primary_key_map_for_tables()
        # Make a list of buckets that we want to create for querying
        bucket_list = table_key_map.keys()
        # Create New Buckets
        self._create_buckets(self.master, bucket_list, server_id=None, bucket_size=None)
        # Wait till the buckets are up
        self.sleep(15)

        # Create Schema in MYSQL Database - TBD : So far working with pre-populated Schema

        # Load Data in MYSQL Database - TBD : So far working with pre-populated data

        # Read Data from mysql database and populate the couchbase server
        for bucket_name in bucket_list:
            query = "select * from {0}".format(bucket_name)
            columns, rows = self.client._execute_query(query = query)
            dict = self.client._gen_json_from_results_with_primary_key(columns, rows, table_key_map[bucket_name])
            self._load_data_in_buckets(bucket_name, dict)





