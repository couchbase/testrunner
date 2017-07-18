import sys
import paramiko
from basetestcase import BaseTestCase
import json
import os
import zipfile
import pprint
import Queue
import json
from membase.helper.cluster_helper import ClusterOperationHelper
import mc_bin_client
import threading
from memcached.helper.data_helper import  VBucketAwareMemcached
from mysql_client import MySQLClient
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.tuq_helper import N1QLHelper
from couchbase_helper.query_helper import QueryHelper
from remote.remote_util import RemoteMachineShellConnection
from lib.membase.helper.bucket_helper import BucketOperationHelper
import random
from itertools import combinations


class RQGTests(BaseTestCase):
    """ Class for defining tests for RQG base testing """

    def setUp(self):
        super(RQGTests, self).setUp()
        self.client_map={}
        self.log.info("==============  RQGTests setup was finished for test #{0} {1} =============="\
                      .format(self.case_number, self._testMethodName))
        self.check_covering_index = self.input.param("check_covering_index",True)
        self.skip_setup_cleanup = True
        self.remove_alias = self.input.param("remove_alias",True)
        self.skip_cleanup = self.input.param("skip_cleanup",False)
        self.build_secondary_index_in_seq = self.input.param("build_secondary_index_in_seq",False)
        self.number_of_buckets = self.input.param("number_of_buckets",5)
        self.crud_type = self.input.param("crud_type","update")
        self.populate_with_replay = self.input.param("populate_with_replay",False)
        self.crud_batch_size = self.input.param("crud_batch_size",1)
        self.record_failure= self.input.param("record_failure",False)
        self.failure_record_path= self.input.param("failure_record_path","/tmp")
        self.use_mysql= self.input.param("use_mysql",True)
        self.initial_loading_to_cb= self.input.param("initial_loading_to_cb",True)
        self.change_bucket_properties = self.input.param("change_bucket_properties",False)
        self.database= self.input.param("database","flightstats")
        self.merge_operation= self.input.param("merge_operation",False)
        self.load_copy_table= self.input.param("load_copy_table",False)
        self.user_id= self.input.param("user_id","root")
        self.user_cluster = self.input.param("user_cluster","Administrator")
        self.password= self.input.param("password","")
        self.password_cluster = self.input.param("password_cluster","password")
        self.generate_input_only = self.input.param("generate_input_only",False)
        self.using_gsi= self.input.param("using_gsi",True)
        self.reset_database = self.input.param("reset_database",True)
        self.create_primary_index = self.input.param("create_primary_index", True)
        self.create_secondary_indexes = self.input.param("create_secondary_indexes",False)
        self.items = self.input.param("items",1000)
        self.mysql_url= self.input.param("mysql_url","localhost")
        self.mysql_url=self.mysql_url.replace("_",".")
        self.gen_secondary_indexes= self.input.param("gen_secondary_indexes",False)
        self.gen_gsi_indexes= self.input.param("gen_gsi_indexes",True)
        self.n1ql_server = self.get_nodes_from_services_map(service_type = "n1ql")
        self.create_all_indexes= self.input.param("create_all_indexes",False)
        self.concurreny_count= self.input.param("concurreny_count",10)
        self.total_queries= self.input.param("total_queries",None)
        self.run_query_without_index_hint= self.input.param("run_query_without_index_hint",True)
        self.run_query_with_primary= self.input.param("run_query_with_primary",False)
        self.create_primary_index=self.input.param("create_primary_index",True)
        self.run_query_with_secondary= self.input.param("run_query_with_secondary",False)
        self.run_explain_with_hints= self.input.param("run_explain_with_hints",False)
        self.test_file_path= self.input.param("test_file_path",None)
        self.secondary_index_info_path= self.input.param("secondary_index_info_path",None)
        self.db_dump_path= self.input.param("db_dump_path",None)
        self.input_rqg_path= self.input.param("input_rqg_path",None)
        self.set_limit = self.input.param("set_limit",0)
        self.build_index_batch_size= self.input.param("build_index_batch_size",1000)
        self.query_count= 0
        self.use_rest = self.input.param("use_rest",True)
        self.ram_quota = self.input.param("ram_quota",512)
        self.drop_index = self.input.param("drop_index",False)
        self.drop_bucket = self.input.param("drop_bucket",False)
        self.dynamic_indexing = self.input.param("dynamic_indexing", False)
        self.pushdown = self.input.param("pushdown",False)
        self.subquery = self.input.param("subquery",False)
        if self.input_rqg_path != None:
            self.secondary_index_info_path = self.input_rqg_path+"/index/secondary_index_definitions.txt"
            self.db_dump_path = self.input_rqg_path+"/db_dump/database_dump.zip"
            self.test_file_path = self.input_rqg_path+"/input/source_input_rqg_run.txt"
        self.query_helper = QueryHelper()
        self.keyword_list = self.query_helper._read_keywords_from_file("b/resources/rqg/n1ql_info/keywords.txt")
        self._initialize_n1ql_helper()
        self.rest = RestConnection(self.master)
        self.indexer_memQuota = self.input.param("indexer_memQuota",1024)
        if self.initial_loading_to_cb:
            self._initialize_cluster_setup()
        if self.subquery:
            self.items = 500
        if not(self.use_rest):
            self._ssh_client = paramiko.SSHClient()
            self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                self.os = self.shell.extract_remote_info().type.lower()
            except Exception, ex:
                self.log.error('SETUP FAILED')
                self.tearDown()



    def tearDown(self):
        super(RQGTests, self).tearDown()
        if hasattr(self, 'reset_database'):
            print "cleanup is %s" %(self.skip_cleanup)
            if self.use_mysql and self.reset_database and (not self.skip_cleanup):
                try:
                    self.client.drop_database(self.database)
                except Exception, ex:
                    self.log.info(ex)

    def test_rqg_concurrent_with_predefined_input(self):
        check = True
        failure_map = {}
        batches = []
        batch = []
        test_case_number = 1
        count = 1
        inserted_count = 0
        self.use_secondary_index = self.run_query_with_secondary or self.run_explain_with_hints
        with open(self.test_file_path) as f:
            n1ql_query_list = f.readlines()
        if self.total_queries  == None:
            self.total_queries = len(n1ql_query_list)
        for n1ql_query_info in n1ql_query_list:
            data = json.loads(n1ql_query_info)
            batch.append({str(test_case_number):data})
            if count == self.concurreny_count:
                inserted_count += len(batch)
                batches.append(batch)
                count = 1
                batch = []
            else:
                count +=1
            test_case_number += 1
            if test_case_number >= self.total_queries:
                break
        if inserted_count != len(n1ql_query_list):
            batches.append(batch)
        result_queue = Queue.Queue()
        for test_batch in batches:
            # Build all required secondary Indexes
            list = [data[data.keys()[0]] for data in test_batch]
            if self.use_secondary_index:
                self._generate_secondary_indexes_in_batches(list)
            thread_list = []
            # Create threads and run the batch
            for test_case in test_batch:
                test_case_number = test_case.keys()[0]
                data = test_case[test_case_number]
                t = threading.Thread(target=self._run_basic_test, args = (data, test_case_number, result_queue))
                t.daemon = True
                t.start()
                thread_list.append(t)
            # Capture the results when done
            check = False
            for t in thread_list:
                t.join()
            # Drop all the secondary Indexes
            if self.use_secondary_index:
                self._drop_secondary_indexes_in_batches(list)
        # Analyze the results for the failure and assert on the run
        success, summary, result = self._test_result_analysis(result_queue)
        self.log.info(result)
        self.assertTrue(success, summary)

    def test_rqg_generate_input(self):
        self.data_dump_path= self.input.param("data_dump_path","b/resources/rqg/data_dump")
        input_file_path=self.data_dump_path+"/input"
        os.mkdir(input_file_path)
        f_write_file = open(input_file_path+"/source_input_rqg_run.txt",'w')
        secondary_index_path=self.data_dump_path+"/index"
        os.mkdir(secondary_index_path)
        database_dump = self.data_dump_path+"/db_dump"
        os.mkdir(database_dump)
        f_write_index_file = open(secondary_index_path+"/secondary_index_definitions.txt",'w')
        self.client.dump_database(data_dump_path = database_dump)
        f_write_index_file.write(json.dumps(self.sec_index_map))
        f_write_index_file.close()
        # Get Data Map
        table_map = self.client._get_values_with_type_for_fields_in_table()
        check = True
        failure_map = {}
        batches = []
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
                batches.append(batch)
                count = 1
                batch = []
            else:
                count +=1
            test_case_number += 1
            if test_case_number >= self.total_queries:
                break
        if inserted_count != len(query_list):
            batches.append(batch)
        # Run Test Batches
        test_case_number = 1
        for test_batch in batches:
            # Build all required secondary Indexes
            list = [data[data.keys()[0]] for data in test_batch]
            list = self.client._convert_template_query_info(
                    table_map = table_map,
                    n1ql_queries = list,
                    define_gsi_index = self.use_secondary_index,
                    gen_expected_result = True)
            # Create threads and run the batch
            for test_case in list:
                test_case_input = test_case
                data = self._generate_test_data(test_case_input)
                f_write_file.write(json.dumps(data)+"\n")
        f_write_file.close()

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
        result_queue = Queue.Queue()
        input_queue = Queue.Queue()
        failure_record_queue = Queue.Queue()
        # Run Test Batches
        test_case_number = 1
        thread_list = []
        for i in xrange(self.concurreny_count):
            t = threading.Thread(target=self._testrun_worker, args = (input_queue, result_queue, failure_record_queue))
            t.daemon = True
            t.start()
            thread_list.append(t)
        for test_batch in batches:
            # Build all required secondary Indexes
            list = [data[data.keys()[0]] for data in test_batch]
            list = self.client._convert_template_query_info(
                    table_map = table_map,
                    n1ql_queries = list,
                    define_gsi_index = self.use_secondary_index,
                    gen_expected_result = False)
            if (self.subquery==False):
                if self.use_secondary_index:
                    self._generate_secondary_indexes_in_batches(list)
            # Create threads and run the batch
            for test_case in list:
                test_case_input = test_case
                input_queue.put({"test_case_number":test_case_number, "test_data":test_case_input})
                test_case_number += 1
            # Capture the results when done
            check = False
            # Drop all the secondary Indexes
            if self.use_secondary_index:
                self._drop_secondary_indexes_in_batches(list)
        for t in thread_list:
            t.join()

        if self.drop_index:
            query = 'select * from system:indexes where keyspace_id like "{0}%"'.format(self.database)
            actual_result = self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server)
            #print actual_result['results']
            index_names = []
            keyspaces = []
            for indexes in actual_result['results']:
                index_names.append(indexes['indexes']['name'])
                keyspaces.append(indexes['indexes']['keyspace_id'])
            i=0
            for name in index_names:
                keyspace = keyspaces[i]
                if (name =='#primary'):
                    query = 'drop primary index on {0}'.format(keyspace)
                else:
                    query = 'drop index {0}.{1}'.format(keyspace,name)
                i+=1
                self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server,query_params={'timeout' : '900s'})
        if self.drop_bucket:
            for bucket in self.buckets:
                BucketOperationHelper.delete_bucket_or_assert(serverInfo=self.n1ql_server,bucket=bucket)
        # Analyze the results for the failure and assert on the run
        success, summary, result = self._test_result_analysis(result_queue)
        self.log.info(result)
        self.dump_failure_data(failure_record_queue)
        self.assertTrue(success, summary)

    def test_rqg_concurrent_new(self):
        # Get Data Map
        table_list = self.client._get_table_list()
        table_map = self.client._get_values_with_type_for_fields_in_table()
        if self.remove_alias:
            for key in table_map.keys():
                if "alias_name" in table_map[key].keys():
                    table_map[key].pop("alias_name",None)
        check = True
        failure_map = {}
        batches = Queue.Queue()
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
        result_queue = Queue.Queue()
        input_queue = Queue.Queue()
        failure_record_queue = Queue.Queue()
        # Run Test Batches
        test_case_number = 1
        thread_list = []
        start_test_case_number = 1
        table_queue_map = {}
        for table_name in table_list:
            table_queue_map[table_name] = Queue.Queue()
        self.log.info("CREATE BACTHES")
        while not batches.empty():
            # Build all required secondary Indexes
            for table_name in table_list:
                if batches.empty():
                    break
                test_batch = batches.get()

                list = [data[data.keys()[0]] for data in test_batch]
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

        if self.drop_index == True:
            query = 'select * from system:indexes where keyspace_id like "{0}%"'.format(self.database)
            actual_result = self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server)
            #print actual_result['results']
            index_names = []
            keyspaces = []
            for indexes in actual_result['results']:
                index_names.append(indexes['indexes']['name'])
                keyspaces.append(indexes['indexes']['keyspace_id'])
            i=0
            for name in index_names:
                keyspace = keyspaces[i]
                if (name =='#primary'):
                    query = 'drop primary index on {0}'.format(keyspace)
                else:
                    query = 'drop index {0}.{1}'.format(keyspace,name)
                i+=1
                self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server,query_params={'timeout' : '900s'})
        if self.drop_bucket == True:
            for bucket in self.buckets:
                BucketOperationHelper.delete_bucket_or_assert(serverInfo=self.master,bucket=bucket)
        # Analyze the results for the failure and assert on the run
        success, summary, result = self._test_result_analysis(result_queue)
        self.log.info(result)
        self.dump_failure_data(failure_record_queue)
        self.assertTrue(success, summary)

    def test_rqg_concurrent_with_secondary(self):
        # Get Data Map
        table_list = self.client._get_table_list()
        table_map = self.client._get_values_with_type_for_fields_in_table()
        for key in table_map.keys():
            if "alias_name" in table_map[key].keys():
                table_map[key].pop("alias_name")
        check = True
        failure_map = {}
        batches = Queue.Queue()
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
        result_queue = Queue.Queue()
        input_queue = Queue.Queue()
        failure_record_queue = Queue.Queue()
        # Run Test Batches
        test_case_number = 1
        thread_list = []
        start_test_case_number = 1
        while not batches.empty():
            # Build all required secondary Indexes
            for table_name in table_list:
                test_batch = batches.get()
                list = [data[data.keys()[0]] for data in test_batch]
                # Create threads and run the batch
                t = threading.Thread(target=self._testrun_secondary_index_worker, args = (table_name, table_map, list, start_test_case_number, result_queue, failure_record_queue))
                start_test_case_number += len(list)
                t.daemon = True
                t.start()
                thread_list.append(t)
                # Drop all the secondary Indexes
            for t in thread_list:
                t.join()
        # Analyze the results for the failure and assert on the run
        success, summary, result = self._test_result_analysis(result_queue)
        self.log.info(result)
        self.dump_failure_data(failure_record_queue)
        self.assertTrue(success, summary)

    def test_rqg_crud_ops(self):
        # Get Data Map
        table_list = self.client._get_table_list()
        table_map = self.client._get_values_with_type_for_fields_in_table()
        for key in table_map.keys():
            if "alias_name" in table_map[key]:
                table_map[key].pop("alias_name")
        failure_map = {}
        batch = []
        test_case_number = 1
        count = 1
        inserted_count = 0
        # Load All the templates
        self.test_file_path= self.unzip_template(self.test_file_path)
        table_list.remove("copy_simple_table")
        with open(self.test_file_path) as f:
            query_list = f.readlines()
        if self.total_queries  == None:
            self.total_queries = len(query_list)
        batches = {}
        for table_name in table_list:
            batches[table_name] = []
        test_case_number = 0
        for n1ql_query_info in query_list:
            data = n1ql_query_info
            batches[table_list[test_case_number%(len(table_list))]].append({str(test_case_number):data})
            test_case_number += 1
            if test_case_number >= self.total_queries:
                break
        result_queue = Queue.Queue()
        failure_record_queue = Queue.Queue()
        # Build all required secondary Indexes
        thread_list =[]
        for table_name in table_list:
            if len(batches[table_name]) > 0:
                test_batch = batches[table_name]
                t = threading.Thread(target=self._testrun_crud_worker, args = (batches[table_name], table_name, table_map, result_queue, failure_record_queue))
                t.daemon = True
                t.start()
                thread_list.append(t)
                # Capture the results when done
        for t in thread_list:
            t.join()
        # Analyze the results for the failure and assert on the run
        success, summary, result = self._test_result_analysis(result_queue)
        self.log.info(result)
        self.dump_failure_data(failure_record_queue)
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

    def _testrun_secondary_index_worker(self, table_name, table_map, list_info , start_test_case_number, result_queue, failure_record_queue = None):
        # map = {self.database+"_"+table_name:table_map[self.database+"_"+table_name]}
        #
        # for info in list_info:
        #     info.replace("simple_table",self.database)
        map = {table_name:table_map[table_name]}

        list_info = self.client._convert_template_query_info(
                    table_map = map,
                    n1ql_queries = list_info,
                    define_gsi_index = self.use_secondary_index)

        for info in list_info:
            info["n1ql"] = info['n1ql'].replace("simple_table",self.database+"_"+"simple_table")

        print list_info
        if self.use_secondary_index:
                self._generate_secondary_indexes_in_batches(list_info)
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

        if self.use_secondary_index:
            self._drop_secondary_indexes_in_batches(list_info)

    def _testrun_crud_worker(self, list_info, table_name, table_map, result_queue = None, failure_record_queue = None):
        map = {table_name:table_map[table_name]}
        for test_data in list_info:
            test_case_number = test_data.keys()[0]
            test_data = test_data[test_case_number]
            data_info = [test_data]
            if self.crud_type == "update":
                data_info = self.client_map[table_name]._convert_update_template_query_info(
                            table_map = map,
                            n1ql_queries = data_info)
            elif self.crud_type == "delete":
                data_info = self.client_map[table_name]._convert_delete_template_query_info(
                            table_map = map,
                            n1ql_queries = data_info)
            elif self.crud_type == "merge_update":
                data_info = self.client_map[table_name]._convert_update_template_query_info_with_merge(
                            source_table = self.database+"_"+"copy_simple_table",
                            target_table = table_name,
                            table_map = map,
                            n1ql_queries = data_info)
            elif self.crud_type == "merge_delete":
                data_info = self.client_map[table_name]._convert_delete_template_query_info_with_merge(
                            source_table = self.database+"_"+"copy_simple_table",
                            target_table = table_name,
                            table_map = map,
                            n1ql_queries = data_info)
            verification_query = "SELECT * from {0} ORDER by primary_key_id".format(table_name)
            self._run_basic_crud_test(data_info[0], verification_query,  test_case_number, result_queue, failure_record_queue = failure_record_queue, table_name= table_name)
            self._populate_delta_buckets(table_name)


    def _run_basic_test(self, test_data, test_case_number, result_queue, failure_record_queue = None):
        data = test_data
        n1ql_query = data["n1ql"]
        sql_query = data["sql"]
        aggregate = False
        subquery=self.subquery

        #import pdb;pdb.set_trace()
        if (n1ql_query.find("simple_table")>0) and ((self.database+"_"+"simple_table") not in n1ql_query):
            n1ql_query = n1ql_query.replace("simple_table",self.database+"_"+"simple_table")
        if (subquery == True):
             n1ql_query = n1ql_query.replace(self.database+"_"+"simple_table_2","t_5.simple_table_2")
             n1ql_query = n1ql_query.replace("t_5.t_5.simple_table_2","t_5.simple_table_2")

        if (subquery == True):
            if "qty" in n1ql_query:
                n1ql_query = n1ql_query.replace("t_2.qty","qty")
                n1ql_query = n1ql_query.replace("qty","t_2.qty")
            if "sum" in n1ql_query:
                n1ql_query = n1ql_query.replace("sum(t_1.productId)","sum(t_1.qty)")
                sql_query = sql_query.replace("sum(t_1.productId)","sum(t_1.qty)")
            n1ql_query = n1ql_query.replace("t_5.simple_table_2 t_1.price","t_1.price")
            sql_query = sql_query.replace("simple_table_2 t_1.price","t_1.price")
            n1ql_query = n1ql_query + " order by meta().id limit 5"
            sql_query = sql_query + " order by t_5.primary_key_id limit 5"

            if ("sum" in n1ql_query or "min" in n1ql_query or "max" in n1ql_query or "count" in n1ql_query):
                aggregate = True


        indexes = data["indexes"]
        table_name = data["bucket"]
        expected_result = data["expected_result"]
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< BEGIN RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(test_case_number))
        result_run = {}
        result_run["n1ql_query"] = n1ql_query
        result_run["sql_query"] = sql_query
        result_run["test_case_number"] = test_case_number
        if self.set_limit > 0 and n1ql_query.find("DISTINCT") > 0:
            result_limit = self.query_helper._add_limit_to_query(n1ql_query,self.set_limit)
            query_index_run = self._run_queries_and_verify(aggregate,subquery,n1ql_query = result_limit , sql_query = sql_query, expected_result = expected_result)
            result_run["run_query_with_limit"] = query_index_run
        if  expected_result == None:
            expected_result = self._gen_expected_result(sql_query,test_case_number)
            data["expected_result"] = expected_result
        query_index_run = self._run_queries_and_verify(aggregate,subquery,n1ql_query = n1ql_query , sql_query = sql_query, expected_result = expected_result)
        result_run["run_query_without_index_hint"] = query_index_run
        if self.run_query_with_primary:
            index_info = {"name":"`#primary`","type":"GSI"}
            query = self.query_helper._add_index_hints_to_query(n1ql_query, [index_info])
            query_index_run = self._run_queries_and_verify(aggregate,subquery,n1ql_query = query , sql_query = sql_query, expected_result = expected_result)
            result_run["run_query_with_primary"] = query_index_run
        if self.run_query_with_secondary:
            for index_name in indexes.keys():
                query = self.query_helper._add_index_hints_to_query(n1ql_query, [indexes[index_name]])
                query_index_run = self._run_queries_and_verify(aggregate,subquery,n1ql_query = query , sql_query = sql_query, expected_result = expected_result)
                key = "run_query_with_index_name::{0}".format(index_name)
                result_run[key] = query_index_run
        if self.run_explain_with_hints:
            result = self._run_queries_with_explain(n1ql_query , indexes)
            result_run.update(result)
        result_queue.put(result_run)
        self._check_and_push_failure_record_queue(result_run, data, failure_record_queue)
        self.query_count += 1
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< END RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(test_case_number))

    def _generate_test_data(self, test_data):
        query_info_list = []
        data = test_data
        n1ql_query = data["n1ql"]
        sql_query = data["sql"]
        indexes = data["indexes"]
        table_name = data["bucket"]
        expected_result = data["expected_result"]
        if  expected_result == None:
            data["expected_result"] = self._gen_expected_result(sql_query)
        return data

    def _run_basic_crud_test(self, test_data, verification_query, test_case_number, result_queue, failure_record_queue = None, table_name = None):
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< BEGIN RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(test_case_number))
        if table_name != None:
            client = self.client_map[table_name]
        else:
            client = self.client
        result_run = {}
        n1ql_query = test_data["n1ql_query"]
        if (n1ql_query.find("copy_simple_table")>0):
             n1ql_query = n1ql_query.replace("simple_table",self.database+"_"+"simple_table")
             #print ("n1ql query before copy replace {0}").format(n1ql_query)
             n1ql_query = n1ql_query.replace("copy_"+self.database+"_"+"simple_table","copy_simple_table")
             #print ("n1ql query after copy replace {0}").format(n1ql_query)
             n1ql_query = n1ql_query.replace("ON KEY copy_simple_table","ON KEY "+ self.database+"_"+"copy_simple_table")
             #print ("n1ql query after on key replace {0}").format(n1ql_query)
        else :
            #print ("n1ql query before simple replace {0}").format(n1ql_query)
            n1ql_query = n1ql_query.replace("simple_table",self.database+"_"+"simple_table")
            #print ("n1ql query after simple replace {0}").format(n1ql_query)

        test_data["n1ql_query"] = n1ql_query
        sql_query = test_data["sql_query"]
        result_run["n1ql_query"] = n1ql_query
        result_run["sql_query"] = sql_query
        result_run["test_case_number"] = test_case_number
        self.log.info("SQL :: {0}".format(sql_query))
        self.log.info("N1QL :: {0}".format(n1ql_query))
        crud_ops_run_result = None
        query_index_run = self._run_queries_and_verify_crud(n1ql_query = verification_query , sql_query = verification_query, expected_result = None, table_name = table_name)
        try:
            self.n1ql_helper.run_cbq_query(n1ql_query, self.n1ql_server)
            client._db_execute_query(query = sql_query)
        except Exception, ex:
            self.log.info(ex)
            crud_ops_run_result ={"success":False, "result": str(ex)}
        if crud_ops_run_result == None:
            #self.sleep(5)
            query_index_run = self._run_queries_and_verify_crud(n1ql_query = verification_query , sql_query = verification_query, expected_result = None, table_name = table_name)
        else:
            query_index_run = crud_ops_run_result
        result_run["crud_verification_test"] = query_index_run
        result_queue.put(result_run)
        self._check_and_push_failure_record_queue(result_run, test_data, failure_record_queue)
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< END RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(test_case_number))

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
                    if failure_reason_type not in failure_reason_map.keys():
                        failure_reason_map[failure_reason_type] = 1
                    else:
                        failure_reason_map[failure_reason_type] += 1
                keyword_list = self.query_helper.find_matching_keywords(n1ql_query, self.keyword_list)
                for keyword in keyword_list:
                    if keyword not in keyword_map.keys():
                        keyword_map[keyword] = 1
                    else:
                        keyword_map[keyword] += 1
                failure_map[test_case_number] = {"sql_query":sql_query, "n1ql_query": n1ql_query,
                 "run_result" : message, "keyword_list": keyword_list}
        summary = " Total Queries Run = {0}, Pass = {1}, Fail = {2}, Pass Pecentage = {3} %".format(total, pass_case, fail_case, ((pass_case*100)/total))
        if len(keyword_map) > 0:
            summary += "\n [ KEYWORD FAILURE DISTRIBUTION ] \n"
        for keyword in keyword_map.keys():
            summary  += keyword+" :: " + str((keyword_map[keyword]*100)/total)+"%\n "
        if len(failure_reason_map)  > 0:
            summary += "\n [ FAILURE TYPE DISTRIBUTION ] \n"
            for keyword in failure_reason_map.keys():
                summary  += keyword+" :: " + str((failure_reason_map[keyword]*100)/total)+"%\n "
        self.log.info(" Total Queries Run = {0}, Pass = {1}, Fail = {2}, Pass Pecentage = {3} %".format(total, pass_case, fail_case, ((pass_case*100)/total)))
        result = self._generate_result(failure_map)
        return success, summary, result

    def test_rqg_from_file(self):
        self.n1ql_file_path= self.input.param("n1ql_file_path","default")
        with open(self.n1ql_file_path) as f:
            n1ql_query_list = f.readlines()
        self._generate_secondary_indexes(n1ql_query_list)
        i = 0
        check = True
        pass_case = 0
        total =0
        fail_case = 0
        failure_map = {}
        for n1ql_query_info in n1ql_query_list:
            # Run n1ql query
            data = json.loads(n1ql_query_info)
            case_number = data["test case number"]
            n1ql_query = data["n1ql_query"]
            sql_query = data["sql_query"]
            expected_result = data["expected_result"]
            hints = self.query_helper._find_hints(n1ql_query)
            self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< BEGIN RUNNING QUERY  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(case_number))
            success, msg = self._run_queries_from_file_and_compare(n1ql_query = n1ql_query , sql_query = sql_query, sql_result = expected_result)
            total += 1
            check = check and success
            if success:
                pass_case += 1
            else:
                fail_case +=  1
                failure_map[case_number] = { "sql_query":sql_query, "n1ql_query": n1ql_query, "reason for failure": msg}
            self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< END RUNNING QUERY CASE NUMBER {0} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(case_number))
        self.log.info(" Total Queries Run = {0}, Pass = {1}, Fail = {2}, Pass Pecentage = {3} %".format(total, pass_case, fail_case,((100*pass_case)/total) ))
        self.assertTrue(check, failure_map)


    def test_bootstrap_with_data(self):
        self.log.info(" Data has been bootstrapped !!")
        self.skip_cleanup=True

    def test_take_mysql_query_response_snap_shot(self):
        self._initialize_mysql_client()
        self.file_prefix= self.input.param("file_prefix","default_rqg_test")
        self.data_dump_path= self.input.param("data_dump_path","/tmp")
        self.queries_per_dump_file= self.input.param("queries_per_dump_file",10000)
        self.n1ql_file_path= self.input.param("n1ql_file_path","default")
        self.sql_file_path= self.input.param("sql_file_path","default")
        with open(self.n1ql_file_path) as f:
            n1ql_query_list = f.readlines()
        with open(self.sql_file_path) as f:
            sql_query_list = f.readlines()
        self._generate_secondary_indexes(n1ql_query_list)
        i = 0
        queries =0
        file_number=0
        f = open(self.data_dump_path+"/"+self.file_prefix+"_"+str(file_number)+".txt","w")
        for n1ql_query in n1ql_query_list:
            n1ql_query = n1ql_query.replace("\n","")
            sql_query = sql_query_list[i].replace("\n","")
            hints = self.query_helper._find_hints(n1ql_query)
            columns, rows = self.client._execute_query(query = sql_query)
            sql_result = self.client._gen_json_from_results(columns, rows)
            if hints == "FUN":
                sql_result = self._convert_fun_result(sql_result)
            dump_data = {
              "test case number":(i+1),
              "n1ql_query":n1ql_query,
              "sql_query":sql_query,
              "expected_result":sql_result
               }
            i+=1
            queries += 1
            f.write(json.dumps(dump_data)+"\n")
            if queries > self.queries_per_dump_file:
                queries = 0
                file_number = 1
                f.close()
                f = open(self.data_dump_path+"/"+self.file_prefix+"_"+file_number+".txt","w")
        f.close()

    def test_take_snapshot_of_database(self):
        self._take_snapshot_of_database()

    def test_load_data_of_database(self):
        self._setup_and_load_buckets_from_files()

    def _run_n1ql_queries(self, n1ql_query = None):
        # Run n1ql query
        actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server)

    def _gen_expected_result(self, sql = "",test = 49):
        sql_result = []
        try:
            client = MySQLClient(database = self.database, host = self.mysql_url,
            user_id = self.user_id, password = self.password)
            if (test == 51):
                 columns = []
                 rows = []
            else:
                columns, rows = client._execute_query(query = sql)
            sql_result = self.client._gen_json_from_results(columns, rows)
            client._close_mysql_connection()
            client = None
        except Exception, ex:
            self.log.info(ex)
            if ex.message.__contains__("SQL syntax") or ex.message.__contains__("ERROR"):
                print "Error in sql syntax"
        #print sql_result

        return sql_result

    def _run_no_change_queries_and_verify(self, n1ql_query = None, sql_query = None, expected_result = None):
        self.log.info(" SQL QUERY :: {0}".format(sql_query))
        self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        result_run = {}
        # Run n1ql query
        try:
            actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server)
            n1ql_result = actual_result["results"]
            #self.log.info(actual_result)
            # Run SQL Query
            sql_result = expected_result
            if expected_result == None:
                columns, rows = self.client._execute_query(query = sql_query)
                sql_result = self.client._gen_json_from_results(columns, rows)
            #self.log.info(sql_result)
            self.log.info(" result from n1ql query returns {0} items".format(len(n1ql_result)))
            self.log.info(" result from sql query returns {0} items".format(len(sql_result)))
            try:
                self.n1ql_helper._verify_results_rqg(subquery=False,aggregate=False,sql_result = sql_result, n1ql_result = n1ql_result, hints = hints)
            except Exception, ex:
                self.log.info(ex)
                return {"success":False, "result": str(ex)}
            return {"success":True, "result": "Pass"}
        except Exception, ex:
            return {"success":False, "result": str(ex)}


    def _run_queries_and_verify(self, agggregate ,subquery, n1ql_query=None, sql_query=None, expected_result=None):
        if not self.create_primary_index:
            n1ql_query = n1ql_query.replace("USE INDEX(`#primary` USING GSI)", " ")
        self.log.info(" SQL QUERY :: {0}".format(sql_query))
        self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        if("NUMERIC_VALUE1" in n1ql_query):
            limit = random.randint(1,30)
            n1ql_query = n1ql_query.replace("NUMERIC_VALUE1",str(limit))
            sql_query = sql_query.replace("NUMERIC_VALUE1",str(limit))
            if (limit < 10):
                offset = limit - 2
            else:
                offset = limit - 10
            n1ql_query = n1ql_query.replace("NUMERIC_VALUE2",str(offset))
            sql_query = sql_query.replace("NUMERIC_VALUE2",str(offset))
            self.log.info(" SQL QUERY :: {0}".format(sql_query))
            self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        result_run = {}
        # Run n1ql query
        hints = self.query_helper._find_hints(sql_query)
        #import pdb;pdb.set_trace()
        for i,item in enumerate(hints):
            if "simple_table" in item:
                hints[i] = hints[i].replace("simple_table",self.database+"_"+"simple_table")
        try:
            if subquery:
                query_params={'timeout' : '1200s'}
            else:
                query_params={}
            actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server,query_params=query_params, scan_consistency="request_plus")
            n1ql_result = actual_result["results"]
            #self.log.info(actual_result)
            # Run SQL Query
            sql_result = expected_result
            if expected_result == None:
                columns, rows = self.client._execute_query(query = sql_query)
                sql_result = self.client._gen_json_from_results(columns, rows)
            #self.log.info(sql_result)
            self.log.info(" result from n1ql query returns {0} items".format(len(n1ql_result)))
            self.log.info(" result from sql query returns {0} items".format(len(sql_result)))

            if(len(n1ql_result)!=len(sql_result)):
                self.log.info("number of results returned from sql and n1ql are different")
                self.log.info("sql query is {0}".format(sql_query))
                self.log.info("n1ql query is {0}".format(n1ql_query))
                if (len(sql_result) == 0 and len(n1ql_result) ==1) or (len(n1ql_result) == 0 and len(sql_result) == 1) or (len(sql_result) == 0):
                        return {"success":True, "result": "Pass"}
                return {"success":False, "result": str("different results")}
            try:
                self.n1ql_helper._verify_results_rqg(subquery,agggregate,sql_result = sql_result, n1ql_result = n1ql_result, hints = hints)
            except Exception, ex:
                self.log.info(ex)
                return {"success":False, "result": str(ex)}
            return {"success":True, "result": "Pass"}
        except Exception, ex:
            return {"success":False, "result": str(ex)}

    def _run_queries_and_verify_crud(self, n1ql_query = None, sql_query = None, expected_result = None, table_name = None):
        self.log.info(" SQL QUERY :: {0}".format(sql_query))
        self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        n1ql_query = n1ql_query.replace("simple_table",self.database+"_"+"simple_table")
        result_run = {}
        if table_name != None:
            client = self.client_map[table_name]
        else:
            client = self.client
        # Run n1ql query
        hints = self.query_helper._find_hints(sql_query)
        for i,item in enumerate(hints):
            if "simple_table" in item:
                hints[i] = hints[i].replace("simple_table",self.database+"_"+"simple_table")
        try:
            actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server, scan_consistency="request_plus")
            n1ql_result = actual_result["results"]
            #self.log.info(actual_result)
            # Run SQL Query
            sql_result = expected_result
            if expected_result == None:
                columns, rows = client._execute_query(query = sql_query)
                sql_result = client._gen_json_from_results(columns, rows)
            #self.log.info(sql_result)
            self.log.info(" result from n1ql query returns {0} items".format(len(n1ql_result)))
            self.log.info(" result from sql query returns {0} items".format(len(sql_result)))


            if(len(n1ql_result)!=len(sql_result)):
                self.log.info("number of results returned from sql and n1ql are different")
                self.log.info("sql query is {0}".format(sql_query))
                self.log.info("n1ql query is {0}".format(n1ql_query))
                if (len(sql_result) == 0 and len(n1ql_result) ==1) or (len(n1ql_result) == 0 and len(sql_result) == 1) or (len(sql_result) == 0):
                    return {"success":True, "result": "Pass"}
            try:
                self.n1ql_helper._verify_results_crud_rqg(sql_result = sql_result, n1ql_result = n1ql_result, hints = hints)
            except Exception, ex:
                self.log.info(ex)
                return {"success":False, "result": str(ex)}
            return {"success":True, "result": "Pass"}
        except Exception, ex:
            return {"success":False, "result": str(ex)}

    def _run_queries_compare(self, n1ql_query = None, sql_query = None, expected_result = None):
        self.log.info(" SQL QUERY :: {0}".format(sql_query))
        self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        # Run n1ql query
        hints = self.query_helper._find_hints(n1ql_query)
        try:
            actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server)
            n1ql_result = actual_result["results"]
            #self.log.info(actual_result)
            # Run SQL Query
            sql_result = expected_result
            if expected_result == None:
                columns, rows = self.client._execute_query(query = sql_query)
                sql_result = self.client._gen_json_from_results(columns, rows)
            #self.log.info(sql_result)
            self.log.info(" result from n1ql query returns {0} items".format(len(n1ql_result)))
            self.log.info(" result from sql query returns {0} items".format(len(sql_result)))
            try:
                self.n1ql_helper._verify_results_rqg(subquery=False,aggregate=False,sql_result = sql_result, n1ql_result = n1ql_result, hints = hints)
            except Exception, ex:
                self.log.info(ex)
                return False, ex
            return True, "Pass"
        except Exception, ex:
            return False, ex

    def _run_explain_and_print_result(self, n1ql_query):
        explain_query = "EXPLAIN "+n1ql_query
        if "not" in explain_query or "NOT" in explain_query:
            self.log.info("Not executing queries with covering indexes and using not keyword")
        else:
            try:
                actual_result = self.n1ql_helper.run_cbq_query(query=explain_query, server=self.n1ql_server)
                self.log.info(explain_query)
            except Exception, ex:
                self.log.info(ex)

    def _run_queries_with_explain(self, n1ql_query = None, indexes = {}):
        run_result = {}
        # Run n1ql query
        for index_name in indexes:
            hint = "USE INDEX({0} USING {1})".format(index_name,indexes[index_name]["type"])
            n1ql = self.query_helper._add_explain_with_hints(n1ql_query, hint)
            self.log.info(n1ql)
            message = "Pass"
            check = True
            fieldsnotcovered = False
            if self.check_covering_index:
                query = "select * from system:indexes where name = '%s'" % index_name
                actual_result = self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server)
                n1ql_result = actual_result["results"]
                fields = n1ql_result[0]["indexes"]["index_key"]
                fieldsnotcovered = self.query_helper.check_groupby_orderby(n1ql_query, fields)

            if "NOT" in n1ql or "not" in n1ql or fieldsnotcovered and self.check_covering_index:
                key = "Explain for index {0}".format(index_name)
                run_result[key] = {"success":check, "result":message}
            else:
                try:
                    actual_result = self.n1ql_helper.run_cbq_query(query=n1ql, server=self.n1ql_server)
                    self.log.info(actual_result)
                    check = self.n1ql_helper.verify_index_with_explain(actual_result, index_name,
                                                                       self.check_covering_index)
                    if not check:
                        message = " query {0} failed explain result, index {1} not found".format(n1ql_query, index_name)
                        self.log.info(message)
                except Exception, ex:
                    self.log.info(ex)
                    message = ex
                    check = False
                finally:
                    key = "Explain for index {0}".format(index_name)
                    run_result[key] = {"success":check, "result":message}


        return run_result

    def _run_queries_from_file_and_compare(self, n1ql_query = None, sql_query = None, sql_result = None):
        self.log.info(" SQL QUERY :: {0}".format(sql_query))
        self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        # Run n1ql query
        hints = self.query_helper._find_hints(n1ql_query)
        actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server)
        n1ql_result = actual_result["results"]
        self.log.info(actual_result)
        self.log.info(sql_result)
        self.log.info(" result from n1ql query returns {0} items".format(len(n1ql_result)))
        self.log.info(" result from sql query returns {0} items".format(len(sql_result)))
        try:
            self.n1ql_helper._verify_results_rqg(subquery=False,aggregate=False,sql_result = sql_result, n1ql_result = n1ql_result, hints = hints)
        except Exception, ex:
            self.log.info(ex)
            return False, ex
        return True, "Pass"

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

        self._initialize_n1ql_helper()
        #create copy of simple table if this is a merge operation
        self.sleep(10)
        if self.gsi_type ==  "memory_optimized":
            os.system("curl -X POST  http://Administrator:password@{1}:8091/pools/default -d memoryQuota={0} -d indexMemoryQuota={2}".format(self.ram_quota, self.n1ql_server.ip,self.indexer_memQuota))
            self.sleep(10)

            # self.log.info("Increasing Indexer Memory Quota to {0}".format(self.indexer_memQuota))
            # self.rest.set_service_memoryQuota(service='indexMemoryQuota', MemoryQuota=self.indexer_memQuota)
            # self.sleep(120)
        if self.change_bucket_properties:
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_command("curl -X POST -u {0}:{1} -d maxBucketCount=25 http://{2}:{3}/internalSettings".format(self.user_cluster,self.password_cluster,self.master.ip,self.master.port))
            self.sleep(10,"Updating maxBucket count to 15")
        self._build_indexes()

    def _build_indexes(self):
        self.sec_index_map = {}
        if self.create_secondary_indexes:
            if self.use_mysql:
                self.sec_index_map  = self.client._gen_index_combinations_for_tables()
            else:
                self.sec_index_map  = self._extract_secondary_index_map_from_file(self.secondary_index_info_path)
        if not self.generate_input_only:
            if self.create_primary_index:
                self._build_primary_indexes(self.using_gsi)
            if self.create_secondary_indexes:
                thread_list = []
                if self.build_secondary_index_in_seq:
                    for table_name in self.sec_index_map.keys():
                        self._gen_secondary_indexes_per_table(self.database+"_"+table_name, self.sec_index_map[table_name], 0)
                else:
                    for table_name in self.sec_index_map.keys():
                        t = threading.Thread(target=self._gen_secondary_indexes_per_table, args = (self.database+"_"+table_name, self.sec_index_map[table_name]))
                        t.daemon = True
                        t.start()
                        thread_list.append(t)
                    for t in thread_list:
                        t.join()

    def _build_primary_indexes(self, using_gsi= True):
        if (self.create_primary_index==True):
            self.n1ql_helper.create_primary_index(using_gsi = using_gsi, server = self.n1ql_server)

    def _load_data_in_buckets_using_mc_bin_client(self, bucket, data_set):
        client = VBucketAwareMemcached(RestConnection(self.master), bucket)
        try:
            for key in data_set.keys():
                o, c, d = client.set(key, 0, 0, json.dumps(data_set[key]))
        except Exception, ex:
            print 'WARN======================='
            print ex

    def _load_data_in_buckets_using_n1ql(self, bucket, data_set):
        try:
            count=0
            for key in data_set.keys():
                if count%2 == 0:
                    n1ql_query = self.query_helper._insert_statement_n1ql(bucket.name, "\""+key+"\"", json.dumps(data_set[key]))
                else:
                    n1ql_query = self.query_helper._upsert_statement_n1ql(bucket.name, "\""+key+"\"", json.dumps(data_set[key]))
                actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server)
                count+=1
        except Exception, ex:
            print 'WARN======================='
            print ex

    def _load_bulk_data_in_buckets_using_n1ql(self, bucket, data_set):
        try:
            count=0
            n1ql_query = self.query_helper._builk_insert_statement_n1ql(bucket.name,data_set)
            actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_query, server = self.n1ql_server, verbose = False)
        except Exception, ex:
            print 'WARN======================='
            print ex

    def _load_data_in_buckets_using_mc_bin_client_json(self, bucket, data_set):
        client = VBucketAwareMemcached(RestConnection(self.master), bucket)
        try:
            for key in data_set.keys():
                o, c, d = client.set(key.encode("utf8"), 0, 0, json.dumps(data_set[key]))
        except Exception, ex:
            print 'WARN======================='
            print ex

    def _load_data_in_buckets(self, bucket_name, data_set):
        from sdk_client import SDKClient
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
            max_verify = self.max_verify,
            buckets = self.buckets, item_flag = None,
            n1ql_port = self.n1ql_server.n1ql_port, full_docs_list = [],
            log = self.log, input = self.input, master = self.master,database = self.database,use_rest=self.use_rest)

    def _initialize_mysql_client(self):
        if self.reset_database:
            self.client = MySQLClient(host = self.mysql_url,
                user_id = self.user_id, password = self.password)
            print self.subquery
            if self.subquery:
                path  = "b/resources/rqg/{0}/database_definition/definition-subquery.sql".format(self.database)
            else:
                path  = "b/resources/rqg/{0}/database_definition/definition.sql".format(self.database)
            self.database = self.database+"_"+str(self.query_helper._random_int())
            populate_data = False
            if not self.populate_with_replay:
                populate_data = True
            if self.subquery:
                self.client.reset_database_add_data(database = self.database, items= self.items, sql_file_definiton_path = path, populate_data = populate_data, number_of_tables  = 1)
            else:
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


    def _zipdir(self, path, zip_path):
        self.log.info(zip_path)
        zipf = zipfile.ZipFile(zip_path, 'w')
        for root, dirs, files in os.walk(path):
            for file in files:
                zipf.write(os.path.join(root, file))

    def _calculate_secondary_indexing_information(self, query_list = []):
        secondary_index_table_map = {}
        table_field_map = self.client._get_field_list_map_for_tables()
        for table_name in table_field_map.keys():
            field_list = table_field_map[table_name]
            secondary_index_list = set([])
            for query in query_list:
                tokens = query.split(" ")
                check_for_table_name = False
                check_for_as = False
                table_name_alias = None
                for token in tokens:
                    if (not check_for_table_name) and (token == table_name):
                        check = True
                    if (not check_for_as) and check_for_table_name and (token == "AS" or token == "as"):
                        check_for_table_name = True
                    if check_for_table_name and token != " ":
                        table_name_alias  = token
                if table_name in query:
                    list = []
                    for field in table_field_map[table_name]:
                        field_name = field
                        if table_name_alias:
                            field_name = table_name_alias+"."+field_name
                        if field_name in query:
                            list.append(field)
                    if len(list) > 0:
                        secondary_index_list = set(secondary_index_list).union(set(list))
            list = []
            index_map ={}
            if len(secondary_index_list) > 0:
                list = [element for element in secondary_index_list]
                index_name = "{0}_{1}".format(table_name,"_".join(list))
                index_map = {index_name:list}
            for field in list:
                index_name = "{0}_{1}".format(table_name,field)
                index_map[index_name] = [field]
            if len(index_map) > 0:
                secondary_index_table_map[table_name] = index_map
        return secondary_index_table_map

    def _generate_result(self, data):
        result = ""
        for key in data.keys():
            result +="<<<<<<<<<< TEST {0} >>>>>>>>>>> \n".format(key)
            for result_key in data[key].keys():
                result += "{0} :: {1} \n".format(result_key, data[key][result_key])
        return result

    def _generate_secondary_indexes(self, query_list):
        if not self.gen_secondary_indexes:
            return
        secondary_index_table_map = self._calculate_secondary_indexing_information(query_list)
        if self.dynamic_indexing:
            for table_name in secondary_index_table_map.keys():
                index_name = "idx_" + str(table_name)
                bucket_name = self.database+ "_" + table_name
                query = "Create Index {0} on {1}(DISTINCT ARRAY v FOR v IN PAIRS(SELF) END)".format(
                    index_name, bucket_name)
                if self.gen_gsi_indexes:
                    query += " using gsi"
                self.log.info(" Running Query {0} ".format(query))
                try:
                    actual_result = self.n1ql_helper.run_cbq_query(
                        query=query, server=self.n1ql_server)
                    check = self.n1ql_helper.is_index_online_and_in_list(
                        bucket_name, index_name, server=self.n1ql_server, timeout=240)
                except Exception, ex:
                    self.log.info(ex)
                    raise
        else:
            for table_name in secondary_index_table_map.keys():
                #table_name = self.database+"_" + table_name
                self.log.info(" Building Secondary Indexes for Bucket {0}".format(self.database+"_" + table_name))
                for index_name in secondary_index_table_map[table_name].keys():
                    query = "Create Index {0} on {1}({2}) ".format(index_name, self.database+"_" + table_name,
                        ",".join(secondary_index_table_map[table_name][index_name]))
                    if self.gen_gsi_indexes:
                        query += " using gsi"
                    self.log.info(" Running Query {0} ".format(query))
                    try:
                        actual_result = self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server)
                        table_name = self.database+"_" + table_name
                        check = self.n1ql_helper.is_index_online_and_in_list(table_name, index_name,
                            server = self.n1ql_server, timeout = 240)
                    except Exception, ex:
                        self.log.info(ex)
                        raise

    def _generate_secondary_indexes_during_initialize(self, index_map = {}):
        if self.generate_input_only:
            return
        thread_list = []
        self.index_map = index_map
        for table_name in index_map.keys():
            t = threading.Thread(target=self._gen_secondary_indexes_per_table, args = (self.database+"_"+table_name))
            t.daemon = True
            t.start()
            thread_list.append(t)
        for t in thread_list:
                t.join()

    def _gen_secondary_indexes_per_table(self, table_name = "", index_map = {}, sleep_time = 2):
        defer_mode = str({"defer_build":"true"})
        build_index_list = []
        batch_index_definitions = {}
        batch_index_definitions = index_map
        if self.pushdown:
           table_field_map = self.client._get_field_list_map_for_tables()
           fields = table_field_map['simple_table']
           combination_fields = sum([map(list, combinations(fields, i)) for i in range(len(fields) + 1)], [])
           for x in xrange(1,len(combination_fields)):
               input = combination_fields[x]
               if len(input)==1:
                   fields_indexed = str(input[0])
                   index_name = "ix_" + str(0)+str(x)
               else:
                 fields_indexed = str(input[0])
                 for i in xrange(1,len(input)):
                   index_name = "ix_" + str(i)+str(x)
                   fields_indexed = fields_indexed+"," + str(x[i])
               query = "CREATE INDEX {0} ON {1}({2})".format(
                     index_name, table_name, fields_indexed)
               build_index_list.append(index_name)
               self.log.info(" Running Query {0} ".format(query))
               try:
                    self.n1ql_helper.run_cbq_query(
                    query=query, server=self.n1ql_server, verbose=False)
                    build_index_list.append(index_name)
                    check = self.n1ql_helper.is_index_online_and_in_list(table_name, index_name,
                            server = self.n1ql_server, timeout = 240)
               except Exception, ex:
                    self.log.info(ex)

        if self.dynamic_indexing:
            index_name = "idx_" + table_name
            query = "CREATE INDEX {0} ON {1}(DISTINCT ARRAY v FOR v IN PAIRS(SELF) END) WITH {2}".format(
                    index_name, table_name, defer_mode)
            build_index_list.append(index_name)
            self.log.info(" Running Query {0} ".format(query))
            try:
                actual_result = self.n1ql_helper.run_cbq_query(
                    query=query, server=self.n1ql_server, verbose=False)
                build_index_list.append(index_name)
            except Exception, ex:
                self.log.info(ex)
                raise
        else:
            for index_name in batch_index_definitions.keys():
                query = "{0} WITH {1}".format(
                    batch_index_definitions[index_name]["definition"], defer_mode)
                build_index_list.append(index_name)
                self.log.info(" Running Query {0} ".format(query))
                try:
                    actual_result = self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server, verbose = False)
                    build_index_list.append(index_name)
                except Exception, ex:
                    self.log.info(ex)
                    raise
        # Run Build Query
        if build_index_list != None and len(build_index_list) > 0:
            batch_size = 0
            start_index_batch = 0
            end_index_batch = 0
            total_indexes = 0
            while total_indexes < len(build_index_list):
                start_index_batch = end_index_batch
                end_index_batch = min(end_index_batch+self.build_index_batch_size,len(build_index_list))
                batch_size += 1
                if start_index_batch == end_index_batch:
                    break
                list_build_index_list = build_index_list[start_index_batch:end_index_batch]
                total_indexes += len(list_build_index_list)
                try:
                    build_query = "BUILD INDEX on {0}({1}) USING GSI".format(table_name,",".join(list_build_index_list))
                    actual_result = self.n1ql_helper.run_cbq_query(query = build_query, server = self.n1ql_server)
                    self.log.info(actual_result)
                    self.sleep(15,"sleep after building index")
                except Exception, ex:
                    self.log.info(ex)
                    raise
                self.sleep(sleep_time)
                # Monitor till the index is built
                tasks = []
                # try:
                #     check = self.n1ql_helper.is_index_online_and_in_list_bulk(table_name, list_build_index_list, server = self.n1ql_server, index_state = "online", timeout = 1200.00)
                #     if not check:
                #         raise Exception(" Index build timed out \n {0}".format(list_build_index_list))
                #     #for index_name in list_build_index_list:
                #     #    tasks.append(self.async_monitor_index(bucket = table_name, index_name = index_name))
                #     #for task in tasks:
                #     #    task.result()
                # except Exception, ex:
                #     self.log.info(ex)

    def _generate_secondary_indexes(self, index_map = {}):
        defer_mode = str({"defer_build":"true"})
        for table_name in index_map.keys():
            build_index_list = []
            batch_index_definitions = {}
            batch_index_definitions = index_map[table_name]
            for index_name in batch_index_definitions.keys():
                query = "{0} WITH {1}".format(
                    batch_index_definitions[index_name]["definition"],
                    defer_mode)
                build_index_list.append(index_name)
                self.log.info(" Running Query {0} ".format(query))
                try:
                    actual_result = self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server)
                    build_index_list.append(index_name)
                except Exception, ex:
                    self.log.info(ex)
                    raise
            # Run Build Query
            if build_index_list != None and len(build_index_list) > 0:
                try:
                    build_query = "BUILD INDEX on {0}({1}) USING GSI".format(table_name,",".join(build_index_list))
                    actual_result = self.n1ql_helper.run_cbq_query(query = build_query, server = self.n1ql_server)
                    self.log.info(actual_result)
                except Exception, ex:
                    self.log.info(ex)
                    raise
                # Monitor till the index is built
                tasks = []
                try:
                    for index_name in build_index_list:
                        tasks.append(self.async_monitor_index(bucket = table_name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

    def _extract_secondary_index_map_from_file(self, file_path= "/tmp/index.txt"):
        with open(file_path) as data_file:
            return json.load(data_file)

    def _generate_secondary_indexes_in_batches(self, batches):
        if self.generate_input_only:
            return
        defer_mode = str({"defer_build":"true"})
        batch_index_definitions = {}
        build_index_list = []
        for info in batches:
            table_name = info["bucket"]
            n1ql = info["n1ql"]
            batch_index_definitions.update(info["indexes"])
        for index_name in batch_index_definitions.keys():
            fail_index_name = index_name
            query = "{0} WITH {1}".format(
                batch_index_definitions[index_name]["definition"],
                defer_mode)
            query = query.replace("ON simple_table","ON "+self.database+"_"+"simple_table")
            self.log.info(" Running Query {0} ".format(query))
            try:
                print index_name
                actual_result = self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server)
                build_index_list.append(index_name)
            except Exception, ex:
                self.log.info(ex)
                raise
        # Run Build Query
        if build_index_list != None and len(build_index_list) > 0:
            try:
                build_query = "BUILD INDEX on {0}({1}) USING GSI".format(self.database+"_"+table_name,",".join(build_index_list))
                actual_result = self.n1ql_helper.run_cbq_query(query = build_query, server = self.n1ql_server)
                self.log.info(actual_result)
            except Exception, ex:
                self.log.info(ex)
                raise
            # Monitor till the index is built
            tasks = []
            try:
                for info in batches:
                    table_name = info["bucket"]
                    table_name = self.database+"_"+table_name
                    for index_name in info["indexes"]:
                        if index_name in build_index_list:
                            tasks.append(self.async_monitor_index(bucket = table_name, index_name = index_name))
                for task in tasks:
                    task.result()
            except Exception, ex:
                self.log.info(ex)

    def async_monitor_index(self, bucket, index_name = None):
        monitor_index_task = self.cluster.async_monitor_index(
                 server = self.n1ql_server, bucket = bucket,
                 n1ql_helper = self.n1ql_helper,
                 index_name = index_name)
        return monitor_index_task

    def _drop_secondary_indexes_in_batches(self, batches):
        for info in batches:
            table_name = info["bucket"]
            table_name =self.database+"_"+table_name
            for index_name in info["indexes"].keys():
                query ="DROP INDEX {0}.{1} USING {2}".format(table_name, index_name, info["indexes"][index_name]["type"])
                try:
                    self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server,query_params={'timeout' : '900s'},timeout = '900s')
                    self.sleep(10,"Sleep to make sure index dropped properly")
                except Exception, ex:
                    self.log.info(ex)

    def _drop_secondary_indexes_with_index_map(self, index_map = {}, table_name = "simple_table"):
        table_name = self.database + "_" +"simple_table"
        self.log.info(" Dropping Secondary Indexes for Bucket {0}".format(table_name))
        for index_name in index_map.keys():
            query ="DROP INDEX {0}.{1} USING {2}".format(table_name, index_name, index_map[index_name]["type"])
            try:
                self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server,query_params={'timeout' : '900s'},timeout = '900s')

            except Exception, ex:
                self.log.info(ex)
                raise

    def _analyze_result(self, result):
        check = True
        failure_types = []
        message = "\n ____________________________________________________\n "
        for key in result.keys():
            if key != "test_case_number" and key != "n1ql_query" and key != "sql_query":
                check = check and result[key]["success"]
                if not result[key]["success"]:
                    failure_types.append(key)
                    message += " Scenario ::  {0} \n".format(key)
                    message += " Reason :: "+result[key]["result"]+"\n"
        return check, message, failure_types

    def _check_and_push_failure_record_queue(self, result, data, failure_record_queue):
        if not self.record_failure:
            return
        check = True
        for key in result.keys():
            if key != "test_case_number" and key != "n1ql_query" and key != "sql_query":
                check = check and result[key]["success"]
                if not result[key]["success"]:
                    failure_record_queue.put(data)

    def dump_failure_data(self, failure_record_queue):
        if not self.record_failure:
            return
        import uuid
        sub_dir = str(uuid.uuid4()).replace("-","")
        self.data_dump_path= self.failure_record_path+"/"+sub_dir
        os.mkdir(self.data_dump_path)
        input_file_path=self.data_dump_path+"/input"
        os.mkdir(input_file_path)
        f_write_file = open(input_file_path+"/source_input_rqg_run.txt",'w')
        secondary_index_path=self.data_dump_path+"/index"
        os.mkdir(secondary_index_path)
        database_dump = self.data_dump_path+"/db_dump"
        os.mkdir(database_dump)
        f_write_index_file = open(secondary_index_path+"/secondary_index_definitions.txt",'w')
        client = MySQLClient(database = self.database, host = self.mysql_url,
            user_id = self.user_id, password = self.password)
        client.dump_database(data_dump_path = database_dump)
        client._close_mysql_connection()
        f_write_index_file.write(json.dumps(self.sec_index_map))
        f_write_index_file.close()
        while not failure_record_queue.empty():
            f_write_file.write(json.dumps(failure_record_queue.get())+"\n")
        f_write_file.close()

    def _check_for_failcase(self, result):
        check=True
        for key in result.keys():
            if key != "test_case_number" and key != "n1ql_query" and key != "sql_query":
                check = check and result[key]["success"]
        return check

    def _convert_fun_result(self, result_set):
        list = []
        for data in result_set:
            map = {}
            for key in data.keys():
                val = data[key]
                if val == None:
                    val =0
                if not isinstance(val, int):
                    val = str(val)
                    if val == "":
                        val = 0
                map[key] =  val
            list.append(map)
        return list

    def unzip_template(self, template_path):
        if "zip" not in template_path:
            return template_path
        tokens =  template_path.split("/")
        file_name = tokens[len(tokens)-1]
        output_path = template_path.replace(file_name,"")
        with zipfile.ZipFile(template_path, "r") as z:
            z.extractall(output_path)
        template_path = template_path.replace(".zip","")
        return template_path

    def _setup_and_load_buckets_from_files(self):
        bucket_list =[]
        import shutil
        #Unzip the files and get bucket list
        tokens = self.db_dump_path.split("/")
        data_file_path = self.db_dump_path.replace(tokens[len(tokens)-1],"data_dump")
        os.mkdir(data_file_path)
        with zipfile.ZipFile(self.db_dump_path, "r") as z:
            z.extractall(data_file_path)
        from os import listdir
        from os.path import isfile, join
        onlyfiles = [ f for f in listdir(data_file_path) if isfile(join(data_file_path,f))]
        for file in onlyfiles:
            bucket_list.append(file.split(".")[0])
        # Remove any previous buckets
        rest = RestConnection(self.master)
        for bucket in self.buckets:
            self.rest.delete_bucket(bucket.name)
        self.buckets = []
        # Create New Buckets
        self._create_buckets(self.master, bucket_list, server_id=None, bucket_size=None)
        # Wait till the buckets are up
        self.sleep(15)
        # Read Data from mysql database and populate the couchbase server
        for bucket_name in bucket_list:
             for bucket in self.buckets:
                if bucket.name == bucket_name:
                    file_path = data_file_path+"/"+bucket_name+".txt"
                    with open(file_path) as data_file:
                        data = json.load(data_file)
                        self._load_data_in_buckets_using_mc_bin_client_json(bucket, data)
                        if self.populate_with_replay:
                            for key in data.keys():
                                insert_sql = self.query_helper._generate_insert_statement_from_data(bucket_name,data[key])
                                self.client._insert_execute_query(insert_sql)
        shutil.rmtree(data_file_path, ignore_errors=True)


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
            shell.execute_command("curl -X POST -u {0}:{1} -d maxBucketCount=25 http://{2}:{3}/internalSettings".format(self.user_cluster,self.password_cluster,self.master.ip,self.master.port))
            self.sleep(10,"Updating maxBucket count to 25")
        # Pull information about tables from mysql database and interpret them as no-sql dbs
        table_key_map = self.client._get_primary_key_map_for_tables()
        # Make a list of buckets that we want to create for querying
        bucket_list = table_key_map.keys()
        print "database used is {0}".format(self.database)
        new_bucket_list =[]
        for bucket in bucket_list:
            if (bucket.find("copy_simple_table")>0):
                new_bucket_list.append(self.database+"_"+"copy_simple_table")
            else:
                new_bucket_list.append(self.database + "_" + bucket)
                if self.subquery:
                    break;

        # Create New Buckets
        self._create_buckets(self.master, new_bucket_list, server_id=None, bucket_size=bucket_size)
        print "buckets created"

        # Wait till the buckets are up
        self.sleep(5)
        self.buckets = self.rest.get_buckets()
        self.newbuckets = []
        for bucket in self.buckets:
            if bucket.name in new_bucket_list:
                self.newbuckets.append(bucket)

        print "safe to start another job"
        self.record_db = {}
        self.buckets = self.newbuckets
        # Read Data from mysql database and populate the couchbase server
        for bucket_name in bucket_list:
            query = "select * from {0}".format(bucket_name)
            columns, rows = self.client._execute_query(query = query)
            self.record_db[bucket_name] = self.client._gen_json_from_results_with_primary_key(columns, rows,
                primary_key = table_key_map[bucket_name])
            if self.subquery:
               for bucket in self.newbuckets:
                 if bucket.name == self.database+"_"+bucket_name:
                     self.load_subquery_test_data(bucket)
            else:
              for bucket in self.newbuckets:
                  if bucket.name == self.database+"_"+bucket_name:
                         self._load_bulk_data_in_buckets_using_n1ql(bucket, self.record_db[bucket_name])

    def _populate_delta_buckets(self, table_name = "simple_table"):
        if table_name != "simple_table":
            client = self.client_map[table_name]
        else:
            client = self.client
        query = "delete from {0}".format(table_name)
        client._insert_execute_query(query = query)
        query = "delete from {0}".format(self.database+"_"+table_name)
        self.n1ql_helper.run_cbq_query(query = query, server = self.n1ql_server, verbose=True)
        insert_sql= "insert into {0}(KEY k ,VALUE b) SELECT meta(b).id as k, b from {1} b".format(self.database+"_"+table_name,self.database+"_"+"copy_simple_table")
        try:
            self.log.info("n1ql query is {0}".format(insert_sql))
            self.n1ql_helper.run_cbq_query(query = insert_sql, server = self.n1ql_server, verbose=True)
            insert_sql= "INSERT INTO {0} SELECT * FROM copy_simple_table".format(table_name)
            client._insert_execute_query(insert_sql)
        except Exception, ex:
            self.log.info(ex)

    def load_subquery_test_data(self, bucket):
        query = 'select primary_key_id from simple_table_1'
        result = self.client._execute_sub_query(query)
        primary_key_values = result
        query = 'CREATE TABLE IF NOT EXISTS {0}.`simple_table_2` ' \
                '(`order_id` VARCHAR(100) NOT NULL,`qty` INT(11) NULL DEFAULT NULL,`productId` VARCHAR(1000) NOT NULL' \
                ',`price` DECIMAL(10,0) NOT NULL,`primary_key_id` VARCHAR(100) NOT NULL,PRIMARY KEY (`order_id`),' \
                'FOREIGN KEY (`primary_key_id`) REFERENCES `simple_table_1`(`primary_key_id`))'.format(self.database)
        self.client._db_execute_query(query)
        for primary_key_value in primary_key_values:
            query = 'select varchar_field1 from simple_table_1 where primary_key_id = {0}'.format(primary_key_value)
            result = self.client._execute_sub_query(query)
            varchar_field = result
            query = 'select decimal_field1 from simple_table_1 where primary_key_id = {0}'.format(primary_key_value)
            result = self.client._execute_sub_query(query)
            decimal_field_value = result
            query = 'select int_field1 from simple_table_1 where primary_key_id = {0}'.format(primary_key_value)
            result = self.client._execute_sub_query(query)
            int_field_value = result
            query = 'select datetime_field1 from simple_table_1 where primary_key_id  = {0}'.format(primary_key_value)
            result = self.client._execute_sub_query(query)
            datetime_field_value = result
            query = 'select bool_field1 from simple_table_1 where primary_key_id  = {0}'.format(primary_key_value)
            result = self.client._execute_sub_query(query)
            bool_field_value = bool(result)
            # if bool_field_value == 1:
            #     bool_field_value = True
            # else:
            #     bool_field_value = False
            query = 'select varchar_field1 from simple_table_1  where primary_key_id  = {0}'.format(primary_key_value)
            result = self.client._execute_sub_query(query)
            varchar_value = result
            query = 'select char_field1 from simple_table_1  where primary_key_id  = {0}'.format(primary_key_value)
            result = self.client._execute_sub_query(query)
            char_value = result
            orderid1="order-"+ varchar_field
            orderid2="order-"+(self.query_helper._random_char()+"_"+str(self.query_helper._random_int()))+varchar_field
            price1 = self.query_helper._random_float()+10
            price2 = self.query_helper._random_float()+100
            qty1 =  self.query_helper._random_int()
            qty2 =  self.query_helper._random_int()
            query = 'insert into simple_table_2 (order_id,qty,productId,price,primary_key_id) values ("%s",%s,"snack",%s,%s)'%(orderid1,qty1,
                                                                                                                             price1,primary_key_value)
            self.client._insert_execute_query(query)
            query = 'insert into simple_table_2 (order_id,qty,productId,price,primary_key_id) values ("%s",%s,"lunch",%s,%s)'%(orderid2,qty2,
                                                                                                                             price2,primary_key_value)
            self.client._insert_execute_query(query)
            n1ql_insert_template = 'INSERT INTO %s (KEY, VALUE) VALUES' \
            ' ("%s", {"primary_key_id": "%s" ,"decimal_field1":%s,"int_field1":%s,"datetime_field1":"%s","bool_field1":%s,"varchar_field1":"%s","char_field1":"%s","simple_table_2":[{"order_id":"%s","qty":%s,"productId":"snack","price":%s,"primary_key_id":"%s"},' \
                                   '{"order_id":"%s","qty":%s,"productId":"lunch","price":%s,"primary_key_id":"%s"}] } )'\
                                   %(bucket.name,primary_key_value,
                                    primary_key_value,decimal_field_value,int_field_value,
                                    datetime_field_value,bool_field_value,varchar_value,char_value,orderid1,qty1,price1,primary_key_value,
                                    orderid2,qty2,price2,primary_key_value)
            actual_result = self.n1ql_helper.run_cbq_query(query = n1ql_insert_template, server = self.n1ql_server)


