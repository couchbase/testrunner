import sys
import paramiko
import re
from basetestcase import BaseTestCase
import json
import os
import zipfile
import pprint
import queue
import json
from membase.helper.cluster_helper import ClusterOperationHelper
import mc_bin_client
import threading
from memcached.helper.data_helper import  VBucketAwareMemcached
from .rqg_mysql_client import RQGMySQLClient as MySQLClient
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.analytics_helper import AnalyticsHelper
from couchbase_helper.query_helper import QueryHelper
from remote.remote_util import RemoteMachineShellConnection
from lib.membase.helper.bucket_helper import BucketOperationHelper
import random

class RQGASTERIXTests(BaseTestCase):
    """ Class for defining tests for RQG base testing """

    def setUp(self):
        super(RQGASTERIXTests, self).setUp()
        self.client_map={}
        self.log.info("==============  RQGTests setup was finished for test #{0} {1} =============="\
                      .format(self.case_number, self._testMethodName))
        self.skip_setup_cleanup = True
        self.remove_alias = self.input.param("remove_alias", True)
        self.number_of_buckets = self.input.param("number_of_buckets", 5)
        self.crud_type = self.input.param("crud_type", "update")
        self.populate_with_replay = self.input.param("populate_with_replay", False)
        self.crud_batch_size = self.input.param("crud_batch_size", 1)
        self.skip_cleanup = self.input.param("skip_cleanup", False)
        self.record_failure= self.input.param("record_failure", False)
        self.failure_record_path= self.input.param("failure_record_path", "/tmp")
        self.use_mysql= self.input.param("use_mysql", True)
        self.joins = self.input.param("joins", False)
        self.ansi_joins = self.input.param("ansi_joins", False)
        self.subquery = self.input.param("subquery", False)
        self.initial_loading_to_cb= self.input.param("initial_loading_to_cb", True)
        self.change_bucket_properties = self.input.param("change_bucket_properties", False)
        self.database= self.input.param("database", "flightstats")
        self.merge_operation= self.input.param("merge_operation", False)
        self.load_copy_table= self.input.param("load_copy_table", False)
        self.user_id= self.input.param("user_id", "root")
        self.user_cluster = self.input.param("user_cluster", "Administrator")
        self.password= self.input.param("password", "")
        self.password_cluster = self.input.param("password_cluster", "password")
        self.generate_input_only = self.input.param("generate_input_only", False)
        self.using_gsi= self.input.param("using_gsi", True)
        self.reset_database = self.input.param("reset_database", True)
        self.items = self.input.param("items", 1000)
        self.mysql_url= self.input.param("mysql_url", "localhost")
        self.mysql_url=self.mysql_url.replace("_", ".")
        self.n1ql_server = self.get_nodes_from_services_map(service_type = "n1ql")
        self.concurreny_count= self.input.param("concurreny_count", 10)
        self.total_queries= self.input.param("total_queries", None)
        self.run_query_with_primary= self.input.param("run_query_with_primary", False)
        self.run_query_with_secondary= self.input.param("run_query_with_secondary", False)
        self.run_explain_with_hints= self.input.param("run_explain_with_hints", False)
        self.test_file_path= self.input.param("test_file_path", None)
        self.db_dump_path= self.input.param("db_dump_path", None)
        self.input_rqg_path= self.input.param("input_rqg_path", None)
        self.set_limit = self.input.param("set_limit", 0)
        self.query_count= 0
        self.use_rest = self.input.param("use_rest", True)
        self.ram_quota = self.input.param("ram_quota", 512)
        self.drop_bucket = self.input.param("drop_bucket", False)
        if self.input_rqg_path != None:
            self.db_dump_path = self.input_rqg_path+"/db_dump/database_dump.zip"
            self.test_file_path = self.input_rqg_path+"/input/source_input_rqg_run.txt"
        self.query_helper = QueryHelper()
        self.keyword_list = self.query_helper._read_keywords_from_file("b/resources/rqg/n1ql_info/keywords.txt")
        self._initialize_analytics_helper()
        self.rest = RestConnection(self.master)
        self.indexer_memQuota = self.input.param("indexer_memQuota", 1024)
        if self.initial_loading_to_cb:
            self._initialize_cluster_setup()
        if not(self.use_rest):
            self._ssh_client = paramiko.SSHClient()
            self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                self.os = self.shell.extract_remote_info().type.lower()
            except Exception as ex:
                self.log.error('SETUP FAILED')
                self.tearDown()



    def tearDown(self):
        super(RQGASTERIXTests, self).tearDown()
        bucket_username = "cbadminbucket"
        bucket_password = "password"
        
        data = 'disconnect link Local;'
        filename = "file.txt"
        f = open(filename, 'w')
        f.write(data)
        f.close()
        url = 'http://{0}:8095/analytics/service'.format(self.master.ip)
        cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url + " -u " + bucket_username + ":" + bucket_password
        os.system(cmd)
        os.remove(filename)
        
        for bucket in self.buckets:
            data = 'drop dataset {0}'.format(bucket.name + "_shadow")
            filename = "file.txt"
            f = open(filename, 'w')
            f.write(data)
            f.close()
            url = 'http://{0}:8095/analytics/service'.format(self.master.ip)
            cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url + " -u " + bucket_username + ":" + bucket_password
            os.system(cmd)
            os.remove(filename)

        
        if hasattr(self, 'reset_database'):
            #self.skip_cleanup= self.input.param("skip_cleanup",False)
            if self.use_mysql and self.reset_database and (not self.skip_cleanup):
                try:
                    self.client.drop_database(self.database)
                except Exception as ex:
                    self.log.info(ex)


    def _initialize_cluster_setup(self):
        if self.use_mysql:
            self.log.info(" Will load directly from mysql")
            self._initialize_mysql_client()
            if not self.generate_input_only:
                self._setup_and_load_buckets()
        else:
            self.log.info(" Will load directly from file snap-shot")
            if self.populate_with_replay:
                self._initialize_mysql_client()
            self._setup_and_load_buckets_from_files()

        self._initialize_analytics_helper()
        #create copy of simple table if this is a merge operation
        self.sleep(10)
        if self.gsi_type ==  "memory_optimized":
            os.system("curl -X POST  http://Administrator:password@{1}:8091/pools/default -d memoryQuota={0} -d indexMemoryQuota={2}".format(self.ram_quota, self.n1ql_server.ip, self.indexer_memQuota))
            self.sleep(10)

            # self.log.info("Increasing Indexer Memory Quota to {0}".format(self.indexer_memQuota))
            # self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=self.indexer_memQuota)
            # self.sleep(120)
        if self.change_bucket_properties:
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_command("curl -X POST -u {0}:{1} -d maxBucketCount=25 http://{2}:{3}/internalSettings".format(self.user_cluster, self.password_cluster, self.master.ip, self.master.port))
            self.sleep(10, "Updating maxBucket count to 15")

    def _initialize_mysql_client(self):
        if self.reset_database:
            self.client = MySQLClient(host = self.mysql_url,
                user_id = self.user_id, password = self.password)
            path  = "b/resources/rqg/{0}/database_definition/definition.sql".format(self.database)
            self.database = self.database+"_"+str(self.query_helper._random_int())
            populate_data = False
            if not self.populate_with_replay:
                populate_data = True
            self.client.reset_database_add_data(database = self.database, items= self.items, sql_file_definiton_path = path, populate_data = populate_data, number_of_tables  = self.number_of_buckets)
            self._copy_table_for_merge()
        else:
            self.client = MySQLClient(database = self.database, host = self.mysql_url,
                user_id = self.user_id, password = self.password)

    def _copy_table_for_merge(self):
        table_list = self.client._get_table_list()
        reference_table = table_list[0]
        if self.merge_operation:
            path  = "b/resources/rqg/crud_db/database_definition/table_definition.sql"
            self.client.database_add_data(database = self.database, sql_file_definiton_path = path)
            table_list = self.client._get_table_list()
            for table_name in table_list:
                if table_name != reference_table:
                    sql = "INSERT INTO {0} SELECT * FROM {1}".format(table_name, reference_table)
                    self.client._insert_execute_query(sql)
        table_list = self.client._get_table_list()
        for table_name in table_list:
            self.client_map[table_name] = MySQLClient(database = self.database, host = self.mysql_url, user_id = self.user_id, password = self.password)

    def _setup_and_load_buckets(self):
        # Remove any previous buckets
        #rest = RestConnection(self.master)
        if (self.skip_setup_cleanup):
            for bucket in self.buckets:
                self.rest.delete_bucket(bucket.name)
        self.buckets = []
        if self.change_bucket_properties or self.gsi_type == "memory_optimized":
            bucket_size = 100
        else:
            bucket_size = None
        if self.change_bucket_properties:
            shell = RemoteMachineShellConnection(self.master)
            #print "master is {0}".format(self.master)
            shell.execute_command("curl -X POST -u {0}:{1} -d maxBucketCount=25 http://{2}:{3}/internalSettings".format(self.user_cluster, self.password_cluster, self.master.ip, self.master.port))
            self.sleep(10, "Updating maxBucket count to 25")
        # Pull information about tables from mysql database and interpret them as no-sql dbs
        table_key_map = self.client._get_primary_key_map_for_tables()
        # Make a list of buckets that we want to create for querying
        bucket_list = list(table_key_map.keys())
        print("database used is {0}".format(self.database))
        new_bucket_list =[]
        for bucket in bucket_list:
            if (bucket.find("copy_simple_table")>0):
                new_bucket_list.append(self.database+"_"+"copy_simple_table")
            else:
                new_bucket_list.append(self.database + "_" + bucket)


        # Create New Buckets
        self._create_buckets(self.master, new_bucket_list, server_id=None, bucket_size=bucket_size)
        print("buckets created")
        # Wait till the buckets are up
        self.sleep(5)
        self.buckets = self.rest.get_buckets()
        self.newbuckets = []
        for bucket in self.buckets:
            if bucket.name in new_bucket_list:
                self.newbuckets.append(bucket)

        print("safe to start another job")
        self.record_db = {}
        self.buckets = self.newbuckets
        # Read Data from mysql database and populate the couchbase server
        for bucket_name in bucket_list:
            query = "select * from {0}".format(bucket_name)
            columns, rows = self.client._execute_query(query = query)
            self.record_db[bucket_name] = self.client._gen_json_from_results_with_primary_key(columns, rows,
                primary_key = table_key_map[bucket_name])
            for bucket in self.newbuckets:
                if bucket.name == self.database+"_"+bucket_name:
                    self._load_bulk_data_in_buckets_using_n1ql(bucket, self.record_db[bucket_name])

        data = 'use Default;'
        bucket_username = "cbadminbucket"
        bucket_password = "password"
        for bucket in self.buckets:
            data = 'create dataset {1} on {0}; '.format(bucket.name,
                                                                bucket.name + "_shadow")
            filename = "file.txt"
            f = open(filename, 'w')
            f.write(data)
            f.close()
            url = 'http://{0}:8095/analytics/service'.format(self.cbas_node.ip)
            cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url + " -u " + bucket_username + ":" + bucket_password
            os.system(cmd)
            os.remove(filename)
        data = 'connect link Local;'
        filename = "file.txt"
        f = open(filename, 'w')
        f.write(data)
        f.close()
        url = 'http://{0}:8095/analytics/service'.format(self.cbas_node.ip)
        cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url + " -u " + bucket_username + ":" + bucket_password
        os.system(cmd)
        os.remove(filename)


    def unzip_template(self, template_path):
        if "zip" not in template_path:
            return template_path
        tokens =  template_path.split("/")
        file_name = tokens[len(tokens)-1]
        output_path = template_path.replace(file_name, "")
        with zipfile.ZipFile(template_path, "r") as z:
            z.extractall(output_path)
        template_path = template_path.replace(".zip", "")
        return template_path

    def _initialize_analytics_helper(self):
        self.n1ql_helper = AnalyticsHelper(version = "spock", shell = None,
            use_rest = True, max_verify = self.max_verify,
            buckets = self.buckets, item_flag = None,
            analytics_port=8095, full_docs_list = [],
            log = self.log, input = self.input, master = self.master, database = self.database)

    def _load_bulk_data_in_buckets_using_n1ql(self, bucket, data_set):
        try:
            count=0
            n1ql_query = self.query_helper._builk_insert_statement_n1ql(bucket.name, data_set)
            actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server, verbose = False)
        except Exception as ex:
            print('WARN=======================')
            print(ex)

    def test_rqg_concurrent(self):
        # Get Data Map
        table_map = self.client._get_values_with_type_for_fields_in_table()
        check = True
        failure_map = {}
        batches = []
        batch = []
        test_case_number = 1
        count = 1
        inserted_count = 0
        # Load All the templates
        self.test_file_path= self.unzip_template(self.test_file_path)
        with open(self.test_file_path) as f:
            query_list = f.readlines()
        if self.total_queries  == None:
            self.total_queries = len(query_list)
        for n1ql_query_info in query_list:
            data = n1ql_query_info
            batch.append({str(test_case_number):data})
            if count == self.concurreny_count:
                inserted_count += len(batch)
                batches.append(batch)
                count = 1
                batch = []
            else:
                count +=1
            test_case_number += 1
            if test_case_number > self.total_queries:
                break
        if inserted_count != len(query_list):
            batches.append(batch)
        result_queue = queue.Queue()
        input_queue = queue.Queue()
        failure_record_queue = queue.Queue()
        # Run Test Batches
        test_case_number = 1
        thread_list = []
        for i in range(self.concurreny_count):
            t = threading.Thread(target=self._testrun_worker, args = (input_queue, result_queue, failure_record_queue))
            t.daemon = True
            t.start()
            thread_list.append(t)
        for test_batch in batches:
            # Build all required secondary Indexes
            list = [data[list(data.keys())[0]] for data in test_batch]
            list = self.client._convert_template_query_info(
                    table_map = table_map,
                    n1ql_queries = list,
                    ansi_joins = self.ansi_joins,
                    gen_expected_result = False)

            # Create threads and run the batch
            for test_case in list:
                test_case_input = test_case
                input_queue.put({"test_case_number":test_case_number, "test_data":test_case_input})
                test_case_number += 1
            # Capture the results when done
            check = False
        for t in thread_list:
            t.join()

            for bucket in self.buckets:
                BucketOperationHelper.delete_bucket_or_assert(serverInfo=self.master, bucket=bucket)
        # Analyze the results for the failure and assert on the run
        success, summary, result = self._test_result_analysis(result_queue)
        self.log.info(result)
        #self.dump_failure_data(failure_record_queue)
        self.assertTrue(success, summary)

    def _testrun_worker(self, input_queue, result_queue, failure_record_queue = None):
        count = 0
        while True:
            if self.total_queries <= (self.query_count):
                break
            if not input_queue.empty():
                data = input_queue.get()
                test_data = data["test_data"]
                test_case_number = data["test_case_number"]
                self._run_basic_test(test_data, test_case_number, result_queue, failure_record_queue)
                count = 0
            else:
                count += 1
                if count > 1000:
                    return

    def _gen_expected_result(self, sql = ""):
        sql_result = []
        try:
            client = MySQLClient(database = self.database, host = self.mysql_url,
            user_id = self.user_id, password = self.password)
            columns, rows = client._execute_query(query = sql)
            sql_result = self.client._gen_json_from_results(columns, rows)
            client._close_connection()
            client = None
        except Exception as ex:
            self.log.info(ex)
            if ex.message.__contains__("SQL syntax") or ex.message.__contains__("ERROR"):
                print("Error in sql syntax")

    def _run_basic_test(self, test_data, test_case_number, result_queue, failure_record_queue = None):
        data = test_data
        n1ql_query = data["n1ql"]
        #LOCK = threading.Lock()

        if (self.joins or self.subquery):
            n1ql_query = data["sql"]
            #import pdb;pdb.set_trace()
            #if LOCK.acquire(False):
            #i = n1ql_query.find("t_")
            #temp = n1ql_query[i:i+4]
            #print "temp is {0}".format(temp)
            #n1ql_query = n1ql_query.replace("t_","VALUE t_",1)
            #print "n1ql query before replace is %s" %n1ql_query
            #n1ql_query = n1ql_query.replace("t_",temp,1)
            #print "n1ql query after replace  is %s" %n1ql_query
            if ("IN" in n1ql_query):
                    index = n1ql_query.find("IN (")
                    temp1 = n1ql_query[0:index] + " IN [ "
                    temp2 = n1ql_query[index+4:].replace(")", "]", 1)
                    n1ql_query = temp1 + temp2
                    print("n1ql query after in replace  is %s"%n1ql_query)
            #LOCK.release()



        if (n1ql_query.find("simple_table")>0) and ((self.database+"_"+"simple_table") not in n1ql_query):
            n1ql_query = n1ql_query.replace("simple_table", self.database+"_"+"simple_table")

        sql_query = data["sql"]
        table_name = data["bucket"]
        expected_result = data["expected_result"]
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< BEGIN RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(test_case_number))
        result_run = {}
        result_run["n1ql_query"] = n1ql_query
        result_run["sql_query"] = sql_query
        result_run["test_case_number"] = test_case_number
        if self.set_limit > 0 and n1ql_query.find("DISTINCT") > 0:
            result_limit = self.query_helper._add_limit_to_query(n1ql_query, self.set_limit)
            query_index_run = self._run_queries_and_verify(n1ql_query = result_limit, sql_query = sql_query, expected_result = expected_result)
            result_run["run_query_with_limit"] = query_index_run
        if  expected_result == None:
            expected_result = self._gen_expected_result(sql_query)
            data["expected_result"] = expected_result
        query_index_run = self._run_queries_and_verify(n1ql_query = n1ql_query, sql_query = sql_query, expected_result = expected_result)
        result_run["run_query_without_index_hint"] = query_index_run
        if self.run_query_with_primary:
            index_info = {"name":"`#primary`","type":"GSI"}
            query = self.query_helper._add_index_hints_to_query(n1ql_query, [index_info])
            query_index_run = self._run_queries_and_verify(n1ql_query = query, sql_query = sql_query, expected_result = expected_result)
            result_run["run_query_with_primary"] = query_index_run

        result_queue.put(result_run)
        self._check_and_push_failure_record_queue(result_run, data, failure_record_queue)
        self.query_count += 1
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< END RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(test_case_number))


    def _run_queries_and_verify(self, n1ql_query = None, sql_query = None, expected_result = None):
        if "NUMERIC_VALUE1" in n1ql_query:
            limit = random.randint(2, 30)
            n1ql_query = n1ql_query.replace("NUMERIC_VALUE1", str(limit))
            sql_query = sql_query.replace("NUMERIC_VALUE1", str(limit))
            if limit < 10:
                offset = limit - 2
            else:
                offset = limit - 10
            n1ql_query = n1ql_query.replace("NUMERIC_VALUE2", str(offset))
            sql_query = sql_query.replace("NUMERIC_VALUE2", str(offset))
            self.log.info(" SQL QUERY :: {0}".format(sql_query))
            self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
            
        self.log.info(" SQL QUERY :: {0}".format(sql_query))
        result_run = {}
        # Run n1ql query
        hints = self.query_helper._find_hints(sql_query)

        for i, item in enumerate(hints):
            if "simple_table" in item:
                hints[i] = hints[i].replace("simple_table", self.database+"_"+"simple_table")
        try:
            actual_result = self.n1ql_helper.run_analytics_query(query = n1ql_query, server = self.n1ql_server, scan_consistency="request_plus")
            n1ql_result = actual_result["results"]
            #self.log.info(actual_result)
            # Run SQL Query
            sql_result = expected_result
            if expected_result == None:
                columns, rows = self.client._execute_query(query = sql_query)
                sql_result = self.client._gen_json_from_results(columns, rows)
            #self.log.info(sql_result)
            self.log.info(" result from CBAS query returns {0} items".format(len(n1ql_result)))
            self.log.info(" result from sql query returns {0} items".format(len(sql_result)))

            if(len(n1ql_result)!=len(sql_result)):
                self.log.info("number of results returned from sql and n1ql are different")
                if (len(sql_result) == 0 and len(n1ql_result) ==1) or (len(n1ql_result) == 0 and len(sql_result) == 1):
                        return {"success":True, "result": "Pass"}
                return {"success":False, "result": str("different results")}
            try:
                self.n1ql_helper._verify_results_rqg_new(sql_result = sql_result, n1ql_result = n1ql_result, hints = hints)
            except Exception as ex:
                self.log.info(ex)
                return {"success":False, "result": str(ex)}
            return {"success":True, "result": "Pass"}
        except Exception as ex:
            return {"success":False, "result": str(ex)}

    def _test_result_analysis(self, queue):
        result_list = []
        pass_case = 0
        fail_case = 0
        total= 0
        failure_map = {}
        keyword_map = {}
        failure_reason_map = {}
        success = True
        while not queue.empty():
            result_list.append(queue.get())
        total = len(result_list)
        for result_run in result_list:
            test_case_number = result_run["test_case_number"]
            sql_query = result_run["sql_query"]
            n1ql_query = result_run["n1ql_query"]
            check, message, failure_types = self._analyze_result(result_run)
            success = success and check
            if check:
                pass_case += 1
            else:
                fail_case +=  1
                for failure_reason_type in failure_types:
                    if failure_reason_type not in list(failure_reason_map.keys()):
                        failure_reason_map[failure_reason_type] = 1
                    else:
                        failure_reason_map[failure_reason_type] += 1
                keyword_list = self.query_helper.find_matching_keywords(n1ql_query, self.keyword_list)
                for keyword in keyword_list:
                    if keyword not in list(keyword_map.keys()):
                        keyword_map[keyword] = 1
                    else:
                        keyword_map[keyword] += 1
                failure_map[test_case_number] = {"sql_query":sql_query, "n1ql_query": n1ql_query,
                 "run_result" : message, "keyword_list": keyword_list}
        summary = " Total Queries Run = {0}, Pass = {1}, Fail = {2}, Pass Pecentage = {3} %".format(total, pass_case, fail_case, ((pass_case*100)/total))
        if len(keyword_map) > 0:
            summary += "\n [ KEYWORD FAILURE DISTRIBUTION ] \n"
        for keyword in list(keyword_map.keys()):
            summary  += keyword+" :: " + str((keyword_map[keyword]*100)/total)+"%\n "
        if len(failure_reason_map)  > 0:
            summary += "\n [ FAILURE TYPE DISTRIBUTION ] \n"
            for keyword in list(failure_reason_map.keys()):
                summary  += keyword+" :: " + str((failure_reason_map[keyword]*100)/total)+"%\n "
        self.log.info(" Total Queries Run = {0}, Pass = {1}, Fail = {2}, Pass Pecentage = {3} %".format(total, pass_case, fail_case, ((pass_case*100)/total)))
        result = self._generate_result(failure_map)
        return success, summary, result

    def _analyze_result(self, result):
        check = True
        failure_types = []
        message = "\n ____________________________________________________\n "
        for key in list(result.keys()):
            if key != "test_case_number" and key != "n1ql_query" and key != "sql_query":
                check = check and result[key]["success"]
                if not result[key]["success"]:
                    failure_types.append(key)
                    message += " Scenario ::  {0} \n".format(key)
                    message += " Reason :: "+result[key]["result"]+"\n"
        return check, message, failure_types

    def _generate_result(self, data):
        result = ""
        for key in list(data.keys()):
            result +="<<<<<<<<<< TEST {0} >>>>>>>>>>> \n".format(key)
            for result_key in list(data[key].keys()):
                result += "{0} :: {1} \n".format(result_key, data[key][result_key])
        return result

    def _check_and_push_failure_record_queue(self, result, data, failure_record_queue):
        if not self.record_failure:
            return
        check = True
        for key in list(result.keys()):
            if key != "test_case_number" and key != "n1ql_query" and key != "sql_query":
                check = check and result[key]["success"]
                if not result[key]["success"]:
                    failure_record_queue.put(data)


    def test_rqg_concurrent_new(self):
        # Get Data Map
        table_list = self.client._get_table_list()
        table_map = self.client._get_values_with_type_for_fields_in_table()
        if self.remove_alias:
            for key in list(table_map.keys()):
                if "alias_name" in list(table_map[key].keys()):
                    table_map[key].pop("alias_name", None)
        check = True
        failure_map = {}
        batches = queue.Queue()
        batch = []
        test_case_number = 1
        count = 1
        inserted_count = 0
        self.use_secondary_index = self.run_query_with_secondary or self.run_explain_with_hints
        # Load All the templates
        self.test_file_path= self.unzip_template(self.test_file_path)
        with open(self.test_file_path) as f:
            query_list = f.readlines()
        if self.total_queries  == None:
            self.total_queries = len(query_list)
        for n1ql_query_info in query_list:
            data = n1ql_query_info
            batch.append({str(test_case_number):data})
            if count == self.concurreny_count:
                inserted_count += len(batch)
                batches.put(batch)
                count = 1
                batch = []
            else:
                count +=1
            test_case_number += 1
            if test_case_number > self.total_queries:
                break
        if inserted_count != len(query_list):
            batches.put(batch)
        result_queue = queue.Queue()
        input_queue = queue.Queue()
        failure_record_queue = queue.Queue()
        # Run Test Batches
        test_case_number = 1
        thread_list = []
        start_test_case_number = 1
        table_queue_map = {}
        for table_name in table_list:
            table_queue_map[table_name] = queue.Queue()
        self.log.info("CREATE BACTHES")
        while not batches.empty():
            # Build all required secondary Indexes
            for table_name in table_list:
                if batches.empty():
                    break
                test_batch = batches.get()

                list = [data[list(data.keys())[0]] for data in test_batch]
                table_queue_map[table_name].put({"table_name":table_name, "table_map":table_map,"list":list, "start_test_case_number":start_test_case_number })
                start_test_case_number += len(list)
        self.log.info("SPAWNING THREADS")
        for table_name in table_list:
            t = threading.Thread(target=self._testrun_worker_new, args = (table_queue_map[table_name], result_queue, failure_record_queue))
            t.daemon = True
            t.start()
            thread_list.append(t)
            # Drop all the secondary Indexes
        for t in thread_list:
            t.join()

        if self.drop_bucket == True:
            #import pdb;pdb.set_trace()
            for bucket in self.buckets:
                BucketOperationHelper.delete_bucket_or_assert(serverInfo=self.master, bucket=bucket)
        # Analyze the results for the failure and assert on the run
        success, summary, result = self._test_result_analysis(result_queue)
        self.log.info(result)
#         self.dump_failure_data(failure_record_queue)
        self.assertTrue(success, summary)

    def _testrun_worker_new(self, input_queue , result_queue, failure_record_queue = None):
        while not input_queue.empty():
            data = input_queue.get()
            table_name = data["table_name"]
            table_map = data["table_map"]
            list = data["list"]
            start_test_case_number = data["start_test_case_number"]
            list_info = self.client._convert_template_query_info(
                    table_map = table_map,
                    n1ql_queries = list,
                    define_gsi_index = self.use_secondary_index)
            thread_list = []
            test_case_number = start_test_case_number
            for test_case_input in list_info:
                t = threading.Thread(target=self._run_basic_test, args = (test_case_input,  test_case_number, result_queue, failure_record_queue))
                test_case_number += 1
                t.daemon = True
                t.start()
                thread_list.append(t)
                # Drop all the secondary Indexes
            for t in thread_list:
                t.join()

    def dump_failure_data(self, failure_record_queue):
        if not self.record_failure:
            return
        import uuid
        sub_dir = str(uuid.uuid4()).replace("-", "")
        self.data_dump_path= self.failure_record_path+"/"+sub_dir
        os.mkdir(self.data_dump_path)
        input_file_path=self.data_dump_path+"/input"
        os.mkdir(input_file_path)
        f_write_file = open(input_file_path+"/source_input_rqg_run.txt", 'w')
        secondary_index_path=self.data_dump_path+"/index"
        os.mkdir(secondary_index_path)
        database_dump = self.data_dump_path+"/db_dump"
        os.mkdir(database_dump)
        f_write_index_file = open(secondary_index_path+"/secondary_index_definitions.txt", 'w')
        client = MySQLClient(database = self.database, host = self.mysql_url,
            user_id = self.user_id, password = self.password)
        client.dump_database(data_dump_path = database_dump)
        client._close_connection()
        f_write_index_file.write(json.dumps(self.sec_index_map))
        f_write_index_file.close()
        while not failure_record_queue.empty():
            f_write_file.write(json.dumps(failure_record_queue.get())+"\n")
        f_write_file.close()

