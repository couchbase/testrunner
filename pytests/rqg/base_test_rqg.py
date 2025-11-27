import paramiko
from basetestcase import BaseTestCase
import os
import zipfile
import queue
import json
import threading
from memcached.helper.data_helper import VBucketAwareMemcached
from .rqg_mysql_client import RQGMySQLClient
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.tuq_helper import N1QLHelper
from .rqg_query_helper import RQGQueryHelper
from remote.remote_util import RemoteMachineShellConnection
import random
from itertools import combinations
import shutil
from os import listdir
from os.path import isfile, join
import traceback
from .rqg_postgres_client import RQGPostgresClient
from membase.api.exception import CBQError
from fts.random_query_generator.rand_query_gen import FTSFlexQueryGenerator
from pytests.fts.fts_base import FTSIndex
from pytests.fts.random_query_generator.rand_query_gen import DATASET
from deepdiff import DeepDiff
from pytests.fts.fts_base import CouchbaseCluster
import uuid
import re

class BaseRQGTests(BaseTestCase):
    def setUp(self):
        try:
            super(BaseRQGTests, self).setUp()
            self.log.info("==============  RQG Setup Has Started ==============")
            self.client_map = {}
            self.check_covering_index = self.input.param("check_covering_index", True)
            self.skip_setup_cleanup = True
            self.crud_ops = self.input.param("crud_ops", False)
            self.use_fts = self.input.param("use_fts", False)
            self.use_sdk = self.input.param("use_sdk", False)
            self.use_analytics = self.input.param("use_analytics", False)
            self.use_udf = self.input.param("use_udf", False)
            self.shell = RemoteMachineShellConnection(self.master)
            self.ansi_joins = self.input.param("ansi_joins", False)
            self.with_let = self.input.param("with_let", False)
            self.ansi_transform = self.input.param("ansi_transform", False)
            self.prepared = self.input.param("prepared", False)
            self.hash_joins = self.input.param("hash_joins", False)
            self.create_secondary_meta_indexes = self.input.param("create_secondary_meta_indexes", False)
            self.aggregate_pushdown = self.input.param("aggregate_pushdown", False)
            self.create_secondary_ansi_join_indexes = self.input.param("create_secondary_ansi_join_indexes", False)
            self.remove_alias = self.input.param("remove_alias", True)
            self.skip_cleanup = self.input.param("skip_cleanup", False)
            self.build_secondary_index_in_seq = self.input.param("build_secondary_index_in_seq", False)
            self.number_of_buckets = self.input.param("number_of_buckets", 5)
            self.crud_type = self.input.param("crud_type", "update")
            self.populate_with_replay = self.input.param("populate_with_replay", False)
            self.use_default_collection = self.input.param("use_default_collection", False)
            self.use_query_context = self.input.param("use_query_context", False)
            self.crud_batch_size = self.input.param("crud_batch_size", 1)
            self.record_failure = self.input.param("record_failure", False)
            self.failure_record_path = self.input.param("failure_record_path", "/tmp")
            self.use_mysql = self.input.param("use_mysql", False)
            self.use_postgres = self.input.param("use_postgres", False)
            self.initial_loading_to_cb = self.input.param("initial_loading_to_cb", True)
            self.change_bucket_properties = self.input.param("change_bucket_properties", False)
            self.database = self.input.param("database", "flightstats")
            self.merge_operation = self.input.param("merge_operation", False)
            self.load_copy_table = self.input.param("load_copy_table", False)
            self.user_id = self.input.param("user_id", "root")
            self.user_cluster = self.input.param("user_cluster", "Administrator")
            self.password = self.input.param("password", "")
            self.password_cluster = self.input.param("password_cluster", "password")
            self.generate_input_only = self.input.param("generate_input_only", False)
            self.using_gsi = self.input.param("using_gsi", True)
            self.use_txns = self.input.param("use_txns", False)
            self.num_txns = self.input.param("num_txns", 1)
            self.use_cbo = self.input.param("use_cbo", False)
            self.reset_database = self.input.param("reset_database", True)
            self.create_primary_index = self.input.param("create_primary_index", False)
            self.create_secondary_indexes = self.input.param("create_secondary_indexes", False)
            self.use_advisor = self.input.param("use_advisor", False)
            self.items = self.input.param("items", 1000)
            self.mysql_url = self.input.param("mysql_url", "localhost")
            self.mysql_url = self.mysql_url.replace("_", ".")
            self.postgres_url = self.input.param("postgres_url", self.mysql_url)
            self.postgres_url = self.postgres_url.replace("_", ".")
            self.gen_secondary_indexes = self.input.param("gen_secondary_indexes", False)
            self.gen_gsi_indexes = self.input.param("gen_gsi_indexes", True)
            self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql")
            self.create_all_indexes = self.input.param("create_all_indexes", False)
            self.concurreny_count = self.input.param("concurreny_count", 10)
            self.total_queries = self.input.param("total_queries", None)
            self.run_query_without_index_hint = self.input.param("run_query_without_index_hint", True)
            self.run_query_with_primary = self.input.param("run_query_with_primary", False)
            self.run_query_with_secondary = self.input.param("run_query_with_secondary", False)
            self.run_explain_with_hints = self.input.param("run_explain_with_hints", False)
            self.test_file_path = self.input.param("test_file_path", None)
            self.secondary_index_info_path = self.input.param("secondary_index_info_path", None)
            self.db_dump_path = self.input.param("db_dump_path", None)
            self.input_rqg_path = self.input.param("input_rqg_path", None)
            self.set_limit = self.input.param("set_limit", 0)
            self.build_index_batch_size = self.input.param("build_index_batch_size", 1000)
            self.query_count = 0
            self.use_rest = self.input.param("use_rest", True)
            self.ram_quota = self.input.param("ram_quota", 512)
            self.drop_index = self.input.param("drop_index", False)
            self.drop_bucket = self.input.param("drop_bucket", False)
            self.dynamic_indexing = self.input.param("dynamic_indexing", False)
            self.partitioned_indexes = self.input.param("partitioned_indexes", False)
            self.pushdown = self.input.param("pushdown", False)
            self.subquery = self.input.param("subquery", False)
            self.drop_secondary_indexes = self.input.param("drop_secondary_indexes", True)
            self.query_helper = self._initialize_rqg_query_helper()
            self.n1ql_helper = self._initialize_n1ql_helper()
            self.rest = RestConnection(self.master)
            self.indexer_memQuota = self.input.param("indexer_memQuota", 1024)
            self.teardown_mysql = self.use_mysql and self.reset_database and (not self.skip_cleanup)
            self.keyword_list = self.query_helper._read_keywords_from_file("b/resources/rqg/n1ql_info/keywords.txt")
            self.use_secondary_index = self.run_query_with_secondary or self.run_explain_with_hints
            self.check_explain_plan = self.input.param("explain_plan", False)
            self.index_limit = self.input.param("index_limit", 15)
            self.float_round_level = self.input.param("float_round_level", 0)
            self.delta = self.input.param("delta", 0)
            self.window_function_test = self.input.param("window_function_test", False)
            self.randomize = self.input.param("randomize", False)
            self.advise_server = self.input.advisor
            self.advise_buckets = ["bucket_01", "bucket_02", "bucket_03", "bucket_04", "bucket_05", "bucket_06", "bucket_07", "bucket_08", "bucket_09", "bucket_10"]
            self.advise_dict={}

            if self.input_rqg_path is not None:
                self.secondary_index_info_path = self.input_rqg_path+"/index/secondary_index_definitions.txt"
                self.db_dump_path = self.input_rqg_path+"/db_dump/database_dump.zip"
                self.test_file_path = self.input_rqg_path+"/input/source_input_rqg_run.txt"
            if self.initial_loading_to_cb:
                self._initialize_cluster_setup()
            if self.subquery:
                self.items = 500
            if not self.use_rest:
                self._ssh_client = paramiko.SSHClient()
                self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self.os = self.shell.extract_remote_info().type.lower()
        except Exception as ex:
            self.log.info("==============  RQG Setup Has Failed ==============")
            traceback.print_exc()
            self.assertTrue(False)
            self.tearDown()
        self.log.info("==============  RQG Setup Has Completed ==============")

    def tearDown(self):
        try:
            self.log.info("==============  RQG BasTestCase Teardown Has Started ==============")
            super(BaseRQGTests, self).tearDown()
            self.log.info("==============  RQG BasTestCase Teardown Has Completed ==============")
            self.log.info("==============  RQG Teardown Has Started ==============")
            if hasattr(self, 'reset_database'):
                if self.teardown_mysql:
                    client = RQGMySQLClient(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)
                    self.kill_mysql_processes(client)
                    client.drop_database(self.database)
        except Exception as ex:
            self.log.info("==============  RQG Teardown Has Failed ==============")
            self.log.info(ex)
        self.log.info("==============  RQG Teardown Has Completed ==============")

    def kill_mysql_processes(self, client):
        columns, rows = client._execute_query(query="select concat('KILL ',id,';') from information_schema.processlist where user='root' and time > 0;")
        sql_result = client._gen_json_from_results(columns, rows)
        for result in sql_result:
            for key in list(result.keys()):
                query = result[key]
                # execute kill query
                client._db_execute_query(query=query)
        client.drop_database(self.database)

    def create_fts_index(self, name, source_type='couchbase',
                         source_name=None, index_type='fulltext-index',
                         index_params=None, plan_params=None,
                         source_params=None, source_uuid=None, doc_count=1000):
        """Create fts index/alias
        @param node: Node on which index is created
        @param name: name of the index/alias
        @param source_type : 'couchbase' or 'files'
        @param source_name : name of couchbase bucket or "" for alias
        @param index_type : 'fulltext-index' or 'fulltext-alias'
        @param index_params :  to specify advanced index mapping;
                                dictionary overriding params in
                                INDEX_DEFAULTS.BLEVE_MAPPING or
                                INDEX_DEFAULTS.ALIAS_DEFINITION depending on
                                index_type
        @param plan_params : dictionary overriding params defined in
                                INDEX_DEFAULTS.PLAN_PARAMS
        @param source_params: dictionary overriding params defined in
                                INDEX_DEFAULTS.SOURCE_CB_PARAMS or
                                INDEX_DEFAULTS.SOURCE_FILE_PARAMS
        @param source_uuid: UUID of the source, may not be used
        """
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        index_params = {"default_analyzer": "keyword",
            "default_mapping": {
                "enabled": True,
                "dynamic": True,
            }
        }

        plan_params = {'numReplicas': 0}

        fts_index = FTSIndex(
            self.cbcluster,
            name,
            source_type,
            source_name,
            index_type,
            index_params,
            plan_params,
            source_params,
            source_uuid)
        fts_index.create()

        indexed_doc_count = 0
        retry_count = 10
        while indexed_doc_count < doc_count and retry_count > 0:
            try:
                self.sleep(10)
                indexed_doc_count = fts_index.get_indexed_doc_count()
            except KeyError as k:
                continue
            retry_count -= 1

        if indexed_doc_count != doc_count:
            self.fail(
                "FTS indexing did not complete. FTS index count : {0}, Bucket count : {1}".format(indexed_doc_count,
                                                                                                  doc_count))

    def test_rqg(self):
        try:
            # Get Data Map
            run_seed = random.seed(uuid.uuid4().int)
            self.log.info("SEED: {0}".format(run_seed))
            table_list = self.client._get_table_list()
            table_map = self.client._get_values_with_type_for_fields_in_table()
            if self.use_txns:
                failures = {}
                passes = 0
                fails = 0
                update_query_template_list = []
                delete_query_template_list = []
                select_query_template_list = []
            i = 1
            for table_name in table_list:
                if self.use_fts:
                    fts_index = self.create_fts_index(name="default_index" + str(i), source_name=self.database+"_"+table_name)
                    i = i + 1
                if self.use_txns:
                    results = self.n1ql_helper.run_cbq_query(query="CREATE PRIMARY INDEX ON {0}".format(self.database + "_" + table_name))
            if self.remove_alias:
                table_map = self.remove_aliases_from_table_map(table_map)
            if self.use_udf:
                bucket_username = "cbadminbucket"
                bucket_password = "password"
                # Register udf library
                udf_code = 'function execute_query(statement) {\
                    var query = N1QL(statement, {});\
                    var acc = [];\
                    for (const row of query) { acc.push(row); }\
                    return acc;\
                }'
                filename = "udf.js"
                f = open(filename, 'w')
                f.write(udf_code)
                f.close()
                url = f'http://{self.master.ip}:8093/evaluator/v1/libraries/n1ql'
                cmd = f"curl -s -k -X POST -u {bucket_username}:{bucket_password} {url} -H 'content-type: application/json' -d @{filename}"
                os.system(cmd)
                os.remove(filename)
                # Create execute_query udf
                self.n1ql_helper.run_cbq_query(query='CREATE OR REPLACE FUNCTION execute_query(s) LANGUAGE JAVASCRIPT AS  "execute_query" AT "n1ql"')
            if self.use_analytics:
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
                    url = 'http://{0}:8095/analytics/service'.format(self.master.ip)
                    cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url + " -u " + bucket_username + ":" + bucket_password
                    os.system(cmd)
                    os.remove(filename)
                data = 'connect link Local;'
                filename = "file.txt"
                f = open(filename, 'w')
                f.write(data)
                f.close()
                url = 'http://{0}:8095/analytics/service'.format(self.master.ip)
                cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url + " -u " + bucket_username + ":" + bucket_password
                os.system(cmd)
                os.remove(filename)

            if self.crud_ops:
                table_list.remove("copy_simple_table")

            query_template_list = self.extract_query_templates()
            if self.use_txns:
                for query in query_template_list:
                    if "fields_asc_desc" in query[0]:
                        select_query_template_list.append(query[1])
                    elif "update" in query[0]:
                        update_query_template_list.append(query[1])
                    elif "delete" in query[0]:
                        delete_query_template_list.append(query[1])

                random.shuffle(select_query_template_list)
                random.shuffle(update_query_template_list)
                random.shuffle(delete_query_template_list)
            # Generate the query batches based on the given template file and the concurrency count
            else:
                if self.randomize:
                    random.shuffle(query_template_list)
                batches = self.generate_batches(table_list, query_template_list)

            result_queue = queue.Queue()
            failure_queue = queue.Queue()
            input_queue = queue.Queue()
            if self.use_cbo:
                for table in table_list:
                    self.n1ql_helper.run_cbq_query(query=f"UPDATE STATISTICS FOR {self.database}_{table}(bool_field1,char_field1,datetime_field1,decimal_field1,int_field1,primary_key_id,varchar_field1)")
            # Run Test Batches
            thread_list = []
            start_test_case_number = 1
            if self.crud_ops:
                i = 1
                for table_name in table_list:
                    if i <= self.number_of_buckets:
                        if len(batches[table_name]) > 0:
                            self._crud_ops_worker(batches[table_name], table_name, table_map, result_queue, failure_queue)
                    i = i + 1
            elif self.use_txns:
                for x in range(0, self.num_txns):
                    rollback_exists = False
                    savepoints = []
                    txn_queries = []
                    savepoint = 0
                    test_batch = []
                    rollback_point = 0

                    select_batches = self.generate_batches(table_list, select_query_template_list)
                    update_batches = self.generate_batches(table_list, update_query_template_list)
                    delete_batches = self.generate_batches(table_list, delete_query_template_list)

                    i = 0
                    self.log.info("-----------------------------------------------------------------STARTING TRANSACTION txn {0}-----------------------------------------------------------------".format(x))
                    results = self.n1ql_helper.run_cbq_query(query="START TRANSACTION", txtimeout="4m")
                    txn_id = results['results'][0]['txid']
                    self.log.info("TXN ID {0}".format(txn_id))
                    txn_queries.append("START TRANSACTION")
                    while not (select_batches.empty() and update_batches.empty() and delete_batches.empty()):
                        # Split up the batches and send them to the worker threads
                        try:
                            select_batch = select_batches.get(False)
                            update_batch = update_batches.get(False)
                            delete_batch = delete_batches.get(False)
                        except Exception as ex:
                            self.log.error(str(ex))
                            break
                        random.seed(uuid.uuid4().int)
                        percentage = random.randint(1, 100)
                        if percentage <= 50:
                            query_type = random.choice(['select','select','select','select','select','select','update','update','update', 'update'])
                            if query_type == 'select':
                                test_batch = select_batch
                            elif query_type == 'update':
                                test_batch = update_batch
                                self.crud_type = "update"
                            elif query_type == 'delete':
                                test_batch = delete_batch
                                self.crud_type = "delete"
                            test_query_template_list = [test_data[list(test_data.keys())[0]] for test_data in test_batch]
                            table_name_description_map = {'simple_table': table_map['simple_table']}
                            # create strings for queries and indexes but doesnt send indexes to Couchbase
                            if query_type == 'update' or query_type == 'delete':
                                sql_n1ql_index_map_list = self.convert_crud_ops_query(table_name, test_query_template_list, table_name_description_map)
                            else:
                                sql_n1ql_index_map_list = self.client._convert_template_query_info(table_map=table_name_description_map,
                                                                                               n1ql_queries=test_query_template_list,
                                                                                               define_gsi_index=self.use_secondary_index,
                                                                                               aggregate_pushdown=self.aggregate_pushdown,
                                                                                               partitioned_indexes=self.partitioned_indexes,
                                                                                               ansi_joins=self.ansi_joins,
                                                                                               with_let=self.with_let)

                            for sql_n1ql_index_map in sql_n1ql_index_map_list:
                                if not query_type == 'select':
                                    sql_n1ql_index_map["n1ql_query"] = sql_n1ql_index_map['n1ql_query'].replace("simple_table",
                                                                                                    self.database + "_" + "simple_table")
                                    if query_type == 'delete':
                                        sql_n1ql_index_map['n1ql_query'] = sql_n1ql_index_map['n1ql_query'].split(";")[0] + " LIMIT 10;" + sql_n1ql_index_map['n1ql_query'].split(";")[1]
                                        sql_n1ql_index_map['sql_query'] = sql_n1ql_index_map['sql_query'].split(";")[0] + " LIMIT 10;" + sql_n1ql_index_map['sql_query'].split(";")[1]

                                else:
                                    sql_n1ql_index_map["n1ql"] = sql_n1ql_index_map['n1ql'].replace("simple_table", self.database+"_"+"simple_table")
                            # build indexes
                            if self.use_secondary_index:
                                self._generate_secondary_indexes_in_batches(sql_n1ql_index_map_list)
                            for test_case_input in sql_n1ql_index_map_list:
                                # if self.use_advisor:
                                #     self.create_secondary_index(n1ql_query=test_case_input['n1ql'])
                                if not query_type == 'select':
                                    self.n1ql_helper.run_cbq_query(query=test_case_input['n1ql_query'], txnid=txn_id)
                                    txn_queries.append(test_case_input['sql_query'])
                                else:
                                    self.n1ql_helper.run_cbq_query(query=test_case_input['n1ql'], txnid=txn_id)
                                    txn_queries.append(test_case_input['sql'])
                            percentage2 = random.randint(1, 100)
                            if percentage2 <= 10:
                                savepoint = i
                                self.log.info("CREATING SAVEPOINT s{0}".format(savepoint))
                                results = self.n1ql_helper.run_cbq_query(query="SAVEPOINT s{0}".format(savepoint), txnid=txn_id)
                                txn_queries.append("SAVEPOINT s{0}".format(savepoint))
                                savepoints.append("s{0}".format(savepoint))
                                i = i + 1
                        if savepoints:
                            percentage3 = random.randint(1, 100)
                            if percentage3 <= 10:
                                rollback_point = random.randint(0, savepoint)
                                for txns in txn_queries:
                                    if "ROLLBACK" in txns:
                                        rollback_exists = True
                                        break
                                if not rollback_exists:
                                    self.log.info("ROLLING BACK")
                                    results = self.n1ql_helper.run_cbq_query(query=f"ROLLBACK TO SAVEPOINT s{savepoint}", txnid=txn_id)
                                    txn_queries.append(f"ROLLBACK TO SAVEPOINT s{savepoint}")

                    self.log.info("-----------------------------------------------------------------COMMITING TRANSACTION {0}-----------------------------------------------------------------".format(x))
                    results = self.n1ql_helper.run_cbq_query(query="COMMIT TRANSACTION", txnid=txn_id)
                    txn_queries.append("COMMIT")
                    self.log.info("txn {0} : {1}".format(x, txn_queries))

                    client = RQGMySQLClient(database=self.database, host=self.mysql_url, user_id=self.user_id,
                                                password=self.password)
                    cursor = client.mysql_connector_client.cursor(buffered=True)
                    for sql_query in txn_queries:
                        cursor.execute(sql_query)
                    client.mysql_connector_client.commit()

                    n1ql_result = self.n1ql_helper.run_cbq_query('select bool_field1,char_field1,datetime_field1,decimal_field1,int_field1,primary_key_id,varchar_field1 FROM {0}'.format(self.database+"_"+"simple_table"))
                    columns, rows = client._execute_query(query='SELECT * FROM simple_table')
                    sql_result = client._gen_json_from_results(columns, rows)
                    client._close_connection()
                    diffs = DeepDiff(sql_result, n1ql_result['results'], ignore_order=True,
                                     ignore_numeric_type_changes=True)
                    if diffs:
                        txn_no = 'txn {0}'.format(x)
                        failures[txn_no] = (diffs, txn_queries)
                        fails += 1
                    else:
                        passes += 1
            else:
                while not batches.empty():
                    # Split up the batches and send them to the worker threads
                    try:
                        test_batch = batches.get(False)
                    except Exception as ex:
                        break
                    test_query_template_list = [test_data[list(test_data.keys())[0]] for test_data in test_batch]
                    input_queue.put({"start_test_case_number": start_test_case_number,
                                     "query_template_list": test_query_template_list})
                    start_test_case_number += len(test_query_template_list)
                i = 1
                for table_name in table_list:
                    if i <= self.number_of_buckets:
                        # Create threads based on number of tables (each table has its own thread)
                        self._rqg_worker(table_name, table_map, input_queue, result_queue,
                                                   failure_queue)
                    i = i + 1
            if not self.use_txns:
                # Analyze the results for the failure and assert on the run
                self.analyze_test(result_queue, failure_queue)
            else:
                self.log.info("Txns Passed: {0} Txn Failed: {1}".format(passes, fails))
                if not fails == 0:
                    self.log.error("Failures were seen, the number of documents in the bucket is not the same in mysql and n1ql after transactions!")
                    for failure in failures:
                        self.log.info("----------------------------------------------------------------{0}-----------------------------------------------------------------------------".format(failure))
                        self.log.info("diffs: {0}  txn: {1}".format(failures[failure][0], failure))
                    self.fail()
        except Exception as ex:
            traceback.print_exc()
            self.log.info(ex)
            self.assertFalse(True)

    def _rqg_worker(self, table_name, table_map, input_queue, result_queue, failure_record_queue=None):
        count = 0
        table_name_description_map = {table_name: table_map[table_name]}
        print(f'{table_name_description_map}')

        while True:
            if self.total_queries <= self.query_count:
                break
            if not input_queue.empty():
                data = input_queue.get()
                start_test_case_number = data["start_test_case_number"]
                query_template_list = data["query_template_list"]
                # create strings for queries and indexes but doesnt send indexes to Couchbase
                sql_n1ql_index_map_list = self.client._convert_template_query_info(table_map=table_name_description_map,
                                                                                   n1ql_queries=query_template_list,
                                                                                   define_gsi_index=self.use_secondary_index,
                                                                                   aggregate_pushdown=self.aggregate_pushdown,
                                                                                   partitioned_indexes=self.partitioned_indexes,
                                                                                   ansi_joins=self.ansi_joins,
                                                                                   with_let=self.with_let)

                for sql_n1ql_index_map in sql_n1ql_index_map_list:
                    sql_n1ql_index_map["n1ql"] = sql_n1ql_index_map['n1ql'].replace("simple_table", self.database+"_"+"simple_table")

                # build indexes
                if self.use_secondary_index:
                    self._generate_secondary_indexes_in_batches(sql_n1ql_index_map_list)
                thread_list = []
                test_case_number = start_test_case_number
                for test_case_input in sql_n1ql_index_map_list:
                    t = threading.Thread(target=self._run_basic_test, args=(test_case_input, test_case_number, result_queue, failure_record_queue))
                    #self._run_basic_test(test_case_input, test_case_number, result_queue, failure_record_queue)
                    test_case_number += 1
                    t.daemon = True
                    t.start()
                    thread_list.append(t)
                    # Drop all the secondary Indexes

                for t in thread_list:
                    t.join()
                if self.use_secondary_index and self.drop_secondary_indexes:
                    self._drop_secondary_indexes_in_batches(sql_n1ql_index_map_list)
            else:
                count += 1
                if count > 1000:
                    return

    def n1ql_query_runner_wrapper(self, n1ql_query="", server=None, query_params={}, scan_consistency=None, verbose=True, query_context=None):
        if self.use_advisor:
            if "UNION ALL" in n1ql_query:
                queries = n1ql_query.split(' UNION ALL ')
            else:
                queries = n1ql_query.split(' UNION ')
            for q in queries:
                query = re.sub(r"^\((.*?)\)$", r"\1", q.strip())
                self.create_secondary_index(n1ql_query=query)
        if self.use_query_context:
            result = self.n1ql_helper.run_cbq_query(query=n1ql_query, server=server, query_params=query_params,
                                                    scan_consistency=scan_consistency, verbose=verbose,query_context=query_context,use_sdk=self.use_sdk)
        else:
            result = self.n1ql_helper.run_cbq_query(query=n1ql_query, server=server, query_params=query_params, scan_consistency=scan_consistency, verbose=verbose,use_sdk=self.use_sdk)
        return result

    def prepare_advise_query(self, n1ql_query=""):
        for bucket in self.advise_dict.keys():
            n1ql_query = n1ql_query.replace(bucket, self.advise_dict[bucket])
            n1ql_query = n1ql_query.replace("._default._default", '')
            if self.use_query_context:
                n1ql_query = n1ql_query.replace("_default", self.advise_dict[bucket])
            n1ql_query = n1ql_query.replace("PREPARE", "")
        return n1ql_query

    def translate_index_statement(self, n1ql_query=""):
        index_name = ""
        index_bucket = ""
        for key in self.advise_dict.keys():
            n1ql_query = n1ql_query.replace(self.advise_dict[key], key)
        # remote util shell strips all spaces out of returns, need to readd them
        if not self.use_rest:
            n1ql_query = n1ql_query.replace("CREATE", "CREATE ")
            n1ql_query = n1ql_query.replace("INDEX", "INDEX ")
            n1ql_query = n1ql_query.replace("ON", " ON ")
            n1ql_query = n1ql_query.replace("(", " ( ")
            n1ql_query = n1ql_query.replace(")", " ) ")
            n1ql_query = n1ql_query.replace("`", "\`")
        index_parts = n1ql_query.split("ON")
        for statement in index_parts:
            if "adv" in statement:
                index_name = statement.replace("CREATE INDEX ", "").strip()
            elif "`" in statement:
                if not self.use_rest:
                    index_bucket = statement.split("(")[0].replace("\`", "").strip()
                else:
                    index_bucket = statement.split("(")[0].replace("`", "").strip()
        return n1ql_query,index_name,index_bucket

    def create_secondary_index(self, n1ql_query=""):
        use_partitioned = False
        if "EXECUTE" not in n1ql_query:
            if self.count_secondary_indexes() >= self.index_limit:
                self.remove_all_secondary_indexes()
            self.n1ql_helper.wait_for_all_indexes_online()
            advise_query = self.prepare_advise_query(n1ql_query=n1ql_query)

            advise_result = self.n1ql_helper.run_cbq_query(query="ADVISE " + advise_query,
                                                           server=self.n1ql_server)
            if len(advise_result["results"][0]["advice"]["adviseinfo"]) == 0:
                self.log.info("No advise for index")
                return
            if "index recommendation at this time" not in str(advise_result):
                if "indexes" in advise_result["results"][0]["advice"]["adviseinfo"][
                    "recommended_indexes"].keys():
                    for index_statement_array in advise_result["results"][0]["advice"]["adviseinfo"]["recommended_indexes"]["indexes"]:
                        index_statement = index_statement_array["index_statement"]
                        if index_statement != "":
                            try:
                                prepared_index_statement, index_name, index_bucket = self.translate_index_statement(index_statement)
                                # insert randomization logic for partitioned vs non_partitioned indexes
                                chance_of_partitioned = random.randint(1, 100)
                                if chance_of_partitioned <= 30:
                                    use_partitioned = True
                                    self.log.info("Using partitioned index for this query: {0}".format(n1ql_query))
                                if self.use_txns:
                                    if use_partitioned:
                                        prepared_index_statement = prepared_index_statement + " PARTITION BY HASH(META().id)"
                                    self.n1ql_helper.run_cbq_query(prepared_index_statement)
                                    self.n1ql_helper._wait_for_index_online(index_bucket,index_name)
                                else:
                                    if use_partitioned:
                                        prepared_index_statement = prepared_index_statement + " PARTITION BY HASH(META().id)"
                                        self.log.info(prepared_index_statement)
                                    self.n1ql_helper.run_cbq_query(prepared_index_statement)
                                    self.n1ql_helper._wait_for_index_online(index_bucket,index_name)
                            except CBQError as ex:
                                if "already exists" in str(ex) or "alreadyexists" in str(ex):
                                    continue

                if "covering_indexes" in advise_result["results"][0]["advice"]["adviseinfo"][
                    "recommended_indexes"].keys():
                    for index_statement_array in advise_result["results"][0]["advice"]["adviseinfo"]["recommended_indexes"]["covering_indexes"]:
                        index_statement = index_statement_array["index_statement"]
                        if index_statement != "":
                            try:
                                prepared_index_statement, index_name, index_bucket = self.translate_index_statement(index_statement)
                                chance_of_partitioned = random.randint(1, 100)
                                if chance_of_partitioned <= 30:
                                    use_partitioned = True
                                    self.log.info("Using partitioned index for this query: {0}".format(n1ql_query))
                                if self.use_txns:
                                    if use_partitioned:
                                        prepared_index_statement = prepared_index_statement + " PARTITION BY HASH(META().id)"
                                    self.n1ql_helper.run_cbq_query(prepared_index_statement)
                                    self.n1ql_helper._wait_for_index_online(index_bucket,index_name)
                                else:
                                    if use_partitioned:
                                        prepared_index_statement = prepared_index_statement + " PARTITION BY HASH(META().id)"
                                        self.log.info(prepared_index_statement)
                                    self.n1ql_helper.run_cbq_query(prepared_index_statement)
                                    self.n1ql_helper._wait_for_index_online(index_bucket,index_name)
                            except CBQError as ex:
                                if "already exists" in str(ex) or "alreadyexists" in str(ex):
                                    continue

    def count_secondary_indexes(self):
        count = self.n1ql_helper.run_cbq_query("select count(*) from system:indexes")
        return int(count["results"][0]["$1"])

    def remove_all_secondary_indexes(self):
        self.n1ql_helper.drop_all_indexes()

    def _run_basic_test(self, query_test_map, test_case_number, result_queue, failure_record_queue=None):
        n1ql_query = query_test_map["n1ql"]
        sql_query = query_test_map["sql"]
        indexes = query_test_map["indexes"]
        expected_result = query_test_map["expected_result"]
        sql_query, n1ql_query = self.handle_limit_offset(sql_query, n1ql_query)
        n1ql_query = self.handle_n1ql_table_name(n1ql_query)
        sql_query, n1ql_query, aggregate = self.handle_subquery(sql_query, n1ql_query)
        n1ql_query = self.handle_hash_join(n1ql_query)
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< BEGIN RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(
            test_case_number))

        # results dict
        result_run = dict()
        result_run["n1ql_query"] = n1ql_query
        result_run["sql_query"] = sql_query
        result_run["test_case_number"] = test_case_number

        # create a randomized set of params to pass to run_queries_and verify, decide prepared or not, partitioned or not, and execution type
        # store these params in a dictionary (possibly extend result_run to contain these choices

        if self.ansi_transform:
            result = self._run_explain_queries(n1ql_query=n1ql_query, keyword="u'outer':u'True'", present=False)
            result_run.update(result)

        if self.check_explain_plan:

            result_run['check_explain_plan'] = self._check_explain_plan_for_secondary_index(n1ql_query=n1ql_query);

        # run the query
        result_run["run_query_without_index_hint"] = self._run_queries_and_verify(aggregate=aggregate,
                                                                                  subquery=self.subquery,
                                                                                  n1ql_query=n1ql_query,
                                                                                  sql_query=sql_query,
                                                                                  expected_result=expected_result)

        if expected_result is None:
            expected_result = self._gen_expected_result(sql_query, test_case_number)
            query_test_map["expected_result"] = expected_result

        if self.set_limit > 0 and n1ql_query.find("DISTINCT") > 0:
            result_limit = self.query_helper._add_limit_to_query(n1ql_query, self.set_limit)
            result_run["run_query_with_limit"] = self._run_queries_and_verify(aggregate=aggregate,
                                                                              subquery=self.subquery,
                                                                              n1ql_query=result_limit,
                                                                              sql_query=sql_query,
                                                                              expected_result=expected_result)

        if self.run_query_with_primary:
            index_info = [{"name": "`#primary`", "type": "GSI"}]
            n1ql_query_with_hints = self.query_helper._add_index_hints_to_query(n1ql_query, index_info)
            result_run["run_query_with_primary"] = self._run_queries_and_verify(aggregate=aggregate,
                                                                                subquery=self.subquery,
                                                                                n1ql_query=n1ql_query_with_hints,
                                                                                sql_query=sql_query,
                                                                                expected_result=expected_result)

            if self.aggregate_pushdown == "primary" and not self.with_let:
                result_run["aggregate_explain_check::#primary"] = self._run_query_with_pushdown_check(n1ql_query,
                                                                                                      index_info)

        if self.run_query_with_secondary:
            for index_name in list(indexes.keys()):
                n1ql_query_with_hints = self.query_helper._add_index_hints_to_query(n1ql_query, [indexes[index_name]])
                result_run["run_query_with_index_name::{0}" + str(index_name)] = self._run_queries_and_verify(
                    aggregate=aggregate,
                    subquery=self.subquery,
                    n1ql_query=n1ql_query_with_hints,
                    sql_query=sql_query,
                    expected_result=expected_result)

        if self.run_explain_with_hints:
            result = self._run_queries_with_explain(n1ql_query, indexes)
            result_run.update(result)

        if self.aggregate_pushdown and not self.with_let:
            for index_name in list(indexes.keys()):
                result_run["aggregate_explain_check::" + str(index_name)] = self._run_query_with_pushdown_check(
                    n1ql_query,
                    indexes[index_name])

        if self.ansi_joins and self.hash_joins:
            self._verify_query_with_hash_joins(n1ql_query)

        result_queue.put(result_run)
        self._check_and_push_failure_record_queue(result_run, query_test_map, failure_record_queue)
        self.query_count += 1
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< END RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(test_case_number))

    def _crud_ops_worker(self, list_info, table_name, table_map, result_queue=None, failure_record_queue=None):
        table_name_map = {table_name: table_map[table_name]}
        for test_data in list_info:
            test_case_number = list(test_data.keys())[0]
            test_data = test_data[test_case_number]
            data_info = self.convert_crud_ops_query(table_name, [test_data], table_name_map)
            verification_query = "SELECT * from {0} where primary_key_id is not null ORDER by primary_key_id".format(table_name)
            self._run_basic_crud_test(data_info[0], verification_query,  test_case_number, result_queue, failure_record_queue, table_name=table_name)
            self._populate_delta_buckets(table_name)
            self.wait_for_num_items(table_name, 1000)

    def remove_aliases_from_table_map(self, table_map):
        for key in list(table_map.keys()):
            if "alias_name" in list(table_map[key].keys()):
                table_map[key].pop("alias_name")
        return table_map

    def extract_query_templates(self):
        file_paths = self.test_file_path.split(":")
        query_template_list = []
        for file_path in file_paths:
            file_path = self.unzip_template(file_path)
            cur_queries_list = []
            with open(file_path) as f:
                cur_queries_list = f.readlines()
            for q in cur_queries_list:
                if self.use_txns:
                    query_template_list.append((file_path,q))
                else:
                    query_template_list.append(q)
        if self.total_queries is None:
            self.total_queries = len(query_template_list)
        return query_template_list

    def generate_batches(self, table_list, query_template_list):
        if self.crud_ops:
            batches = {}
            for table_name in table_list:
                batches[table_name] = []
        else:
            batches = queue.Queue()
            batch = []
            count = 1
            inserted_count = 0

        test_case_number = 1

        for template_query in query_template_list:
            if self.crud_ops:
                batches[table_list[test_case_number % (len(table_list))]].append({str(test_case_number): template_query})
            else:
                batch.append({str(test_case_number): template_query})
                if count == self.concurreny_count:
                    inserted_count += len(batch)
                    batches.put(batch)
                    count = 1
                    batch = []
                else:
                    count += 1
            test_case_number += 1
            if test_case_number > self.total_queries:
                break

        if not self.crud_ops:
            if len(batch) > 0:
                batches.put(batch)

        return batches

    def analyze_test(self, result_queue, failure_queue):
        success, summary, result = self._test_result_analysis(result_queue)
        self.log.info(result)
        self.dump_failure_data(failure_queue)
        self.assertTrue(success, summary)

    def convert_crud_ops_query(self, table_name, data_info, table_name_map):
        if self.crud_type == "update":
            data_info = self.client_map[table_name]._convert_update_template_query_info(
                table_map=table_name_map,
                n1ql_queries=data_info)
        elif self.crud_type == "delete":
            data_info = self.client_map[table_name]._convert_delete_template_query_info(
                table_map=table_name_map,
                n1ql_queries=data_info)
        elif self.crud_type == "merge_update":
            data_info = self.client_map[table_name]._convert_update_template_query_info_with_merge(
                source_table=self.database+"_"+"copy_simple_table",
                target_table=table_name,
                table_map=table_name_map,
                n1ql_queries=data_info)
        elif self.crud_type == "merge_delete":
            data_info = self.client_map[table_name]._convert_delete_template_query_info_with_merge(
                source_table=self.database+"_"+"copy_simple_table",
                target_table=table_name,
                table_map=table_name_map,
                n1ql_queries=data_info)
        return data_info

    def wait_for_num_items(self, table, num_items):
        num_items_reached = False
        while not num_items_reached:
            self.sleep(1)
            query = "SELECT COUNT(*) from {0}".format(self.database+"_"+table)
            result = self.n1ql_query_runner_wrapper(n1ql_query=query, server=self.n1ql_server)
            if result["results"][0]["$1"] == num_items:
                num_items_reached = True

    def handle_limit_offset(self, sql_query, n1ql_query):
        if "NUMERIC_VALUE1" in n1ql_query:
            limit = random.randint(1, 30)
            n1ql_query = n1ql_query.replace("NUMERIC_VALUE1", str(limit))
            sql_query = sql_query.replace("NUMERIC_VALUE1", str(limit))
            if limit < 10:
                offset = limit - 2
            else:
                offset = limit - 10
            if offset < 0:
                offset = 0
            n1ql_query = n1ql_query.replace("NUMERIC_VALUE2", str(offset))
            sql_query = sql_query.replace("NUMERIC_VALUE2", str(offset))
            self.log.info(" SQL QUERY :: {0}".format(sql_query))
            self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        return sql_query, n1ql_query

    def handle_n1ql_table_name(self, n1ql_query):
        if (n1ql_query.find("simple_table") > 0) and ((self.database+"_"+"simple_table") not in n1ql_query):
            n1ql_query = n1ql_query.replace("simple_table", self.database+"_"+"simple_table")
        return n1ql_query

    def handle_subquery(self, sql_query, n1ql_query):
        aggregate = False
        if self.subquery:
            n1ql_query = n1ql_query.replace(self.database+"_"+"simple_table_2", "t_5.simple_table_2")
            n1ql_query = n1ql_query.replace("t_5.t_5.simple_table_2", "t_5.simple_table_2")
            if "qty" in n1ql_query:
                n1ql_query = n1ql_query.replace("t_2.qty", "qty")
                n1ql_query = n1ql_query.replace("qty", "t_2.qty")
            if "sum" in n1ql_query:
                n1ql_query = n1ql_query.replace("sum(t_1.productId)", "sum(t_1.qty)")
                sql_query = sql_query.replace("sum(t_1.productId)", "sum(t_1.qty)")
            n1ql_query = n1ql_query.replace("t_5.simple_table_2 t_1.price", "t_1.price")
            sql_query = sql_query.replace("simple_table_2 t_1.price", "t_1.price")
            n1ql_query = n1ql_query + " order by primary_key_id limit 5"
            sql_query = sql_query + " order by t_5.primary_key_id limit 5"
            if "sum" in n1ql_query or "min" in n1ql_query or "max" in n1ql_query or "count" in n1ql_query:
                aggregate = True
        return sql_query, n1ql_query, aggregate

    def handle_hash_join(self, n1ql_query):
        if self.ansi_joins and self.hash_joins:
            hash_join_template_list = ["HASH(build)", "HASH(probe)"]
            n1ql_query.replace(" ON ", "{0} ON ".random.choice(hash_join_template_list))
        return n1ql_query

    def _run_query_with_pushdown_check(self, n1ql_query, index):
        message = "Pass"
        explain_check = False
        if isinstance(index, dict):
            index = [index]
        query = self.query_helper._add_index_hints_to_query(n1ql_query, index)
        explain_n1ql = "EXPLAIN " + query
        try:
            actual_result = self.n1ql_helper.run_cbq_query(query=explain_n1ql, server=self.n1ql_server)
            if "index_group_aggs" in str(actual_result):
                explain_check = True
            if not explain_check:
                message = "aggregate query {0} with index {1} failed explain result, index_group_aggs not found".format(n1ql_query, index)
                self.log.info(message)
                self.log.info(str(actual_result))
        except Exception as ex:
            self.log.info(ex)
            message = ex
            explain_check = False
        finally:
            return {"success": explain_check, "result": message}

    def _verify_query_with_hash_joins(self, n1ql_query):
        message = "Pass"
        explain_check = True
        explain_n1ql = "EXPLAIN " + n1ql_query
        hash_query_count = n1ql_query.count("HASH")
        try:
            actual_result = self.n1ql_helper.run_cbq_query(query=explain_n1ql, server=self.n1ql_server)
            hash_explain_count = str(actual_result).count("HashJoin")
            explain_check = (hash_query_count == hash_explain_count)
            if not explain_check:
                message = "Join query {0} with failed explain result, HashJoins not found".format(n1ql_query)
                self.log.info(message)
                self.log.info(str(actual_result))
        except Exception as ex:
            self.log.info(ex)
            message = ex
            explain_check = False
        finally:
            return {"success": explain_check, "result": message}


    def _run_basic_crud_test(self, test_data, verification_query, test_case_number, result_queue, failure_record_queue=None, table_name=None):
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< BEGIN RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(test_case_number))
        result_run = {}
        n1ql_query = test_data["n1ql_query"]
        if n1ql_query.find("copy_simple_table") > 0:
             n1ql_query = n1ql_query.replace("simple_table", self.database+"_"+"simple_table")
             n1ql_query = n1ql_query.replace("copy_"+self.database+"_"+"simple_table", "copy_simple_table")
             n1ql_query = n1ql_query.replace("ON KEY copy_simple_table", "ON KEY " + self.database+"_"+"copy_simple_table")
        else:
            n1ql_query = n1ql_query.replace("simple_table", self.database+"_"+"simple_table")

        test_data["n1ql_query"] = n1ql_query
        sql_query = test_data["sql_query"]

        result_run["n1ql_query"] = n1ql_query
        result_run["sql_query"] = sql_query
        result_run["test_case_number"] = test_case_number

        self.log.info("SQL :: {0}".format(sql_query))
        self.log.info("N1QL :: {0}".format(n1ql_query))

        crud_ops_run_result = None
        client = RQGMySQLClient(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)
        try:
            self.n1ql_query_runner_wrapper(n1ql_query=n1ql_query, server=self.n1ql_server)
            client._insert_execute_query(query=sql_query)
        except Exception as ex:
            self.log.info(ex)
            crud_ops_run_result = {"success": False, "result": str(ex)}
            client._close_connection()
        client._close_connection()
        if crud_ops_run_result is None:
            query_index_run = self._run_queries_and_verify_crud(n1ql_query=verification_query, sql_query=verification_query, expected_result=None, table_name=table_name)
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
                fail_case += 1
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
                failure_map[test_case_number] = {"sql_query": sql_query, "n1ql_query": n1ql_query,
                                                 "run_result": message, "keyword_list": keyword_list}
        pass_percent = 0
        if total > 0:
            summary = " Total Queries Run = {0}, Pass = {1}, Fail = {2}, Pass Percentage = {3} %".format(total, pass_case, fail_case, ((pass_case*100)/total))
        else:
            summary = " No Query Results Found"
        if len(keyword_map) > 0:
            summary += "\n [ KEYWORD FAILURE DISTRIBUTION ] \n"
        for keyword in list(keyword_map.keys()):
            summary += keyword+" :: " + str((keyword_map[keyword]*100)/total)+"%\n "
        if len(failure_reason_map) > 0:
            summary += "\n [ FAILURE TYPE DISTRIBUTION ] \n"
            for keyword in list(failure_reason_map.keys()):
                summary += keyword+" :: " + str((failure_reason_map[keyword]*100)/total)+"%\n "
        if total > 0:
            self.log.info(" Total Queries Run = {0}, Pass = {1}, Fail = {2}, Pass Percentage = {3} %".format(total, pass_case, fail_case, ((pass_case*100)/total)))
        else:
            self.log.info(" Total Queries Run = 0, No results in queue")
        result = self._generate_result(failure_map)
        return success, summary, result

    def _gen_expected_result(self, sql="", test=49):
        sql_result = []
        try:
            client = None
            if self.use_mysql:
                client = RQGMySQLClient(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)
            elif self.use_postgres:
                client = RQGPostgresClient(host=self.postgres_url, user=self.user_id, password=self.password, database=self.database)

            if test == 51:
                columns = []
                rows = []
            else:
                columns, rows = client._execute_query(query=sql)
            if self.aggregate_pushdown:
                sql_result = client._gen_json_from_results_repeated_columns(columns, rows)
            else:
                sql_result = client._gen_json_from_results(columns, rows)
            client._close_connection()
        except Exception as ex:
            self.log.info(ex)
            traceback.print_exc()
            if ex.message.__contains__("SQL syntax") or ex.message.__contains__("ERROR"):
                print("Error in sql syntax")
        return sql_result

    def _check_explain_plan_for_secondary_index(self, n1ql_query=None):
        self.n1ql_query_runner_wrapper(n1ql_query=n1ql_query, server=self.n1ql_server)
        actual_result = self.n1ql_helper.run_cbq_query(query="EXPLAIN "+n1ql_query, server=self.n1ql_server)
        self.log.info("EXPLAIN PLAN :: "+str(actual_result))
        if "PrimaryScan" in str(actual_result['results'][0]['plan']):
            return {"success": False, "result": "Fail"}
        else:
            return {"success": True, "result": "Pass"}

    def _run_queries_and_verify(self, aggregate=False, subquery=False, n1ql_query=None, sql_query=None, expected_result=None):
        query_context = ""
        skip = False
        use_prepared = False
        fts_explain_query = ""
        chance_of_prepared = random.randint(1, 100)
        if chance_of_prepared <= 20:
            use_prepared = True
            self.log.info("Preparing query {0}".format(n1ql_query))
        if not self.create_primary_index:
            n1ql_query = n1ql_query.replace("USE INDEX(`#primary` USING GSI)", " ")
        if self.prepared or use_prepared:
            n1ql_query = "PREPARE " + n1ql_query
            self.log.info("Prepared query {0}".format(n1ql_query))
        if self.use_default_collection:
            n1ql_query = n1ql_query.replace("table_10", "table_010")
            for bucket in self.advise_dict.keys():
                n1ql_query = n1ql_query.replace(bucket, bucket+"._default._default")
            n1ql_query = n1ql_query.replace("table_010", "table_10._default._default")
        if self.use_query_context:
            n1ql_query = n1ql_query.replace("table_10", "table_010")
            # So that query context works for multiple buckets, replace the first matching bucket and leave the rest as full paths
            for bucket in self.advise_dict.keys():
                if bucket in n1ql_query and not skip:
                    n1ql_query = n1ql_query.replace(bucket, "_default")
                    query_context = "default:"+bucket+"._default"
                    skip = True
                else:
                    n1ql_query = n1ql_query.replace(bucket, bucket + "._default._default")
            n1ql_query = n1ql_query.replace("table_010", "table_10._default._default")

        fts_query = n1ql_query
        cbas_query = n1ql_query
        udf_n1ql_query = n1ql_query.replace('"',"'")
        udf_query = f'EXECUTE FUNCTION execute_query("{udf_n1ql_query}")'
        if self.use_fts:
            if not "JOIN" in n1ql_query:
                add_hint = n1ql_query.split("WHERE")
                fts_query = add_hint[0] + " USE INDEX (USING FTS, USING GSI) WHERE " + add_hint[1]
                if "BETWEEN" in n1ql_query:
                    fts_query = add_hint[0] + " USE INDEX (USING FTS) WHERE " + add_hint[1]
            else:
                split_list = n1ql_query.split("JOIN")
                i = 0
                new_n1ql = ""
                for items in split_list:
                    if "ON" in items:
                        items = items.replace("ON", "USE INDEX (USING FTS) ON")
                    else:
                        items = items.replace("INNER",  "USE INDEX (USING FTS) INNER")
                        items = items.replace("LEFT",  "USE INDEX (USING FTS) LEFT")
                    if i == 0:
                        new_n1ql = new_n1ql + items
                    else:
                        new_n1ql = new_n1ql + " JOIN " + items
                    i = i + 1
                fts_query = new_n1ql
            fts_explain_query = fts_query
            self.log.info(" FTS QUERY :: {0}".format(fts_query))
        elif self.use_analytics:
            cbas_query = cbas_query.replace("ANY ", "(ANY ")
            cbas_query = cbas_query.replace("EVERY ", "(EVERY ")
            cbas_query = cbas_query.replace("SOME ", "(SOME ")
            cbas_query = cbas_query.replace("END", "END)")
            cbas_query = cbas_query.replace("type", "`type`")
        else:
            self.log.info(" SQL QUERY :: {0}".format(sql_query))
        self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        # Run n1ql query
        hints = self.query_helper._find_hints(sql_query)
        for i, item in enumerate(hints):
            if "simple_table" in item:
                hints[i] = hints[i].replace("simple_table", self.database+"_"+"simple_table")
        try:
            if subquery:
                query_params = {'timeout': '1200s'}
            else:
                query_params={}
            if self.use_fts:
                fts_result = self.n1ql_query_runner_wrapper(n1ql_query=fts_query, server=self.n1ql_server,
                                                               query_params=query_params,query_context=query_context)
                if self.use_query_context:
                    if "PREPARE" in fts_query:
                        fts_explain_query = fts_query.replace("PREPARE","")
                    fts_explain = self.n1ql_helper.run_cbq_query(query="EXPLAIN " + fts_explain_query, server=self.n1ql_server,
                                                                 query_params=query_params,query_context=query_context,use_sdk=self.use_sdk)
                else:
                    if "PREPARE" in fts_query:
                        fts_explain_query = fts_query.replace("PREPARE","")
                    fts_explain = self.n1ql_helper.run_cbq_query(query="EXPLAIN " + fts_explain_query, server=self.n1ql_server,query_params=query_params,use_sdk=self.use_sdk)
                if not (fts_explain['results'][0]['plan']['~children'][0]['#operator'] == 'IndexFtsSearch' or fts_explain['results'][0]['plan']['~children'][0]['~children'][0]['#operator'] == 'IndexFtsSearch'):
                    return {"success": False, "result": str("Query does not use fts index {0}".format(fts_explain))}
            elif self.use_analytics:
                analytics_result = self.n1ql_query_runner_wrapper(n1ql_query=cbas_query, server=self.n1ql_server,
                                                               query_params=query_params,query_context=query_context)
            elif self.use_udf:
                if "PREPARE" in udf_query:
                    udf_query = udf_query.replace("PREPARE ","")
                udf_result = self.n1ql_query_runner_wrapper(n1ql_query=udf_query, server=self.n1ql_server, query_params=query_params, scan_consistency="request_plus",query_context=query_context)

            actual_result = self.n1ql_query_runner_wrapper(n1ql_query=n1ql_query, server=self.n1ql_server, query_params=query_params, scan_consistency="request_plus",query_context=query_context)
            if self.prepared or use_prepared:
                name = actual_result["results"][0]['name']
                prepared_query = "EXECUTE '%s'" % name
                self.log.info(" N1QL QUERY :: {0}".format(prepared_query))
                actual_result = self.n1ql_query_runner_wrapper(n1ql_query=prepared_query, server=self.n1ql_server, query_params=query_params, scan_consistency="request_plus",query_context=query_context)
                if self.use_fts:
                    fts_result = self.n1ql_query_runner_wrapper(n1ql_query=prepared_query, server=self.n1ql_server, query_params=query_params, scan_consistency="request_plus",query_context=query_context)
            n1ql_result = actual_result["results"]
            if self.use_fts:
                fts_result = fts_result["results"]
            elif self.use_analytics:
                analytics_result = analytics_result['results']
            elif self.use_udf:
                udf_result = udf_result['results'][0]

            # Run SQL Query
            if not self.use_fts and not self.use_analytics and not self.use_udf:
                sql_result = expected_result
                client = None
                if self.use_mysql:
                    client = RQGMySQLClient(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)
                elif self.use_postgres:
                    client = RQGPostgresClient(host=self.postgres_url, user=self.user_id, password=self.password, database=self.database)

                if expected_result is None:
                    columns, rows = client._execute_query(query=sql_query)
                    if self.aggregate_pushdown:
                        sql_result = client._gen_json_from_results_repeated_columns(columns, rows)
                    else:
                        sql_result = client._gen_json_from_results(columns, rows)
                client._close_connection()
                self.log.info(" result from sql query returns {0} items".format(len(sql_result)))
            elif self.use_fts:
                self.log.info(" result from fts query returns {0} items".format(len(fts_result)))
            elif self.use_analytics:
                self.log.info(" result from analytics query returns {0} items".format(len(analytics_result)))
            elif self.use_udf:
                self.log.info(" result from udf query returns {0} items".format(len(udf_result)))
            self.log.info(" result from n1ql query returns {0} items".format(len(n1ql_result)))

            if self.use_fts:
                if len(n1ql_result) != len(fts_result):
                    self.log.info("number of results returned from sql and n1ql are different")
                    self.log.info("fts query is {0}".format(fts_query))
                    self.log.info("n1ql query is {0}".format(n1ql_query))

                    if (len(fts_result) == 0 and len(n1ql_result) == 1) or (
                            len(n1ql_result) == 0 and len(fts_result) == 1) or (len(fts_result) == 0):
                        return {"success": True, "result": "Pass"}
                    return {"success": False, "result": str("different results")}
            elif self.use_analytics:
                if len(n1ql_result) != len(analytics_result):
                    self.log.info("number of results returned from analytics and n1ql are different")
                    self.log.info("analytics query is {0}".format(cbas_query))
                    self.log.info("n1ql query is {0}".format(n1ql_query))

                    if (len(analytics_result) == 0 and len(n1ql_result) == 1) or (
                            len(n1ql_result) == 0 and len(analytics_result) == 1) or (len(analytics_result) == 0):
                        return {"success": True, "result": "Pass"}
                    return {"success": False, "result": str("different results")}
            elif self.use_udf:
                if len(n1ql_result) != len(udf_result):
                    self.log.info("number of results returned from sql and n1ql are different")
                    self.log.info("udf query is {0}".format(udf_query))
                    self.log.info("n1ql query is {0}".format(n1ql_query))

                    if (len(udf_result) == 0 and len(n1ql_result) == 1) or (len(n1ql_result) == 0 and len(udf_result) == 1) or (len(udf_result) == 0):
                            return {"success": True, "result": "Pass"}
                    return {"success": False, "result": str("different results")}
            else:
                if len(n1ql_result) != len(sql_result):
                    self.log.info("number of results returned from sql and n1ql are different")
                    self.log.info("sql query is {0}".format(sql_query))
                    self.log.info("n1ql query is {0}".format(n1ql_query))

                    if (len(sql_result) == 0 and len(n1ql_result) == 1) or (len(n1ql_result) == 0 and len(sql_result) == 1) or (len(sql_result) == 0):
                            return {"success": True, "result": "Pass"}
                    return {"success": False, "result": str("different results")}
            try:
                if self.use_fts:
                    self.n1ql_helper._verify_results_rqg(subquery, aggregate, sql_result=fts_result, n1ql_result=n1ql_result, hints=hints, aggregate_pushdown=self.aggregate_pushdown,use_fts=self.use_fts)
                elif self.use_analytics:
                    self.n1ql_helper._verify_results_rqg(subquery, aggregate, sql_result=analytics_result, n1ql_result=n1ql_result, hints=hints, aggregate_pushdown=self.aggregate_pushdown)
                elif self.use_udf:
                    self.n1ql_helper._verify_results_rqg(subquery, aggregate, sql_result=udf_result, n1ql_result=n1ql_result, hints=hints, aggregate_pushdown=self.aggregate_pushdown)
                else:
                    self.n1ql_helper._verify_results_rqg(subquery, aggregate, sql_result=sql_result, n1ql_result=n1ql_result, hints=hints, aggregate_pushdown=self.aggregate_pushdown)
            except Exception as ex:
                self.log.info(ex)
                traceback.print_exc()
                return {"success": False, "result": str(ex) }
            return {"success": True, "result": "Pass"}
        except Exception as ex:
            self.log.info(ex)
            traceback.print_exc()
            return {"success": False, "result": str(ex)}

    def _run_queries_and_verify_crud(self, n1ql_query=None, sql_query=None, expected_result=None, table_name=None):
        self.log.info(" SQL QUERY :: {0}".format(sql_query))
        self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        if n1ql_query.find(self.database) <= 0:
            n1ql_query = n1ql_query.replace("simple_table", self.database+"_"+"simple_table")
        hints = self.query_helper._find_hints(sql_query)
        for i, item in enumerate(hints):
            if "simple_table" in item:
                hints[i] = hints[i].replace("simple_table", self.database+"_"+"simple_table")

        try:
            actual_result = self.n1ql_query_runner_wrapper(n1ql_query=n1ql_query, server=self.n1ql_server, scan_consistency="request_plus")
            n1ql_result = actual_result["results"]
            # Run SQL Query
            sql_result = expected_result
            client = RQGMySQLClient(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)
            if expected_result is None:
                columns, rows = client._execute_query(query=sql_query)
                sql_result = client._gen_json_from_results(columns, rows)
            client._close_connection()
            self.log.info(" result from n1ql query returns {0} items".format(len(n1ql_result)))
            self.log.info(" result from sql query returns {0} items".format(len(sql_result)))

            if len(n1ql_result) != len(sql_result):
                self.log.info("number of results returned from sql and n1ql are different")
                self.log.info("sql query is {0}".format(sql_query))
                self.log.info("n1ql query is {0}".format(n1ql_query))

                if (len(sql_result) == 0 and len(n1ql_result) == 1) or (len(n1ql_result) == 0 and len(sql_result) == 1) or (len(sql_result) == 0):
                    return {"success": True, "result": "Pass"}
            try:
                self.n1ql_helper._verify_results_crud_rqg(sql_result=sql_result, n1ql_result=n1ql_result, hints=hints)
            except Exception as ex:
                self.log.info(ex)
                return {"success": False, "result": str(ex)}
            return {"success": True, "result": "Pass"}
        except Exception as ex:
            return {"success": False, "result": str(ex)}

    def _run_queries_with_explain(self, n1ql_query=None, indexes={}):
        run_result = {}
        # Run n1ql query
        for index_name in indexes:
            hint = "USE INDEX({0} USING {1})".format(index_name, indexes[index_name]["type"])
            n1ql = self.query_helper._add_explain_with_hints(n1ql_query, hint)
            self.log.info(n1ql)
            message = "Pass"
            check = True
            fieldsnotcovered = False
            if self.check_covering_index:
                query = "select * from system:indexes where name = '%s'" % index_name
                actual_result = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
                n1ql_result = actual_result["results"]
                fields = n1ql_result[0]["indexes"]["index_key"]
                fieldsnotcovered = self.query_helper.check_groupby_orderby(n1ql_query, fields)

            if "NOT" in n1ql or "not" in n1ql or fieldsnotcovered and self.check_covering_index:
                key = "Explain for index {0}".format(index_name)
                run_result[key] = {"success": check, "result": message}
            else:
                try:
                    actual_result = self.n1ql_query_runner_wrapper(n1ql_query=n1ql, server=self.n1ql_server)
                    self.log.info(actual_result)
                    check = self.n1ql_helper.verify_index_with_explain(actual_result, index_name,
                                                                       self.check_covering_index)
                    if not check:
                        message = " query {0} failed explain result, index {1} not found".format(n1ql_query, index_name)
                        self.log.info(message)
                except Exception as ex:
                    self.log.info(ex)
                    message = ex
                    check = False
                finally:
                    key = "Explain for index {0}".format(index_name)
                    run_result[key] = {"success": check, "result": message}
        return run_result

    def _run_explain_queries(self, n1ql_query=None, keyword ="", present=True):
        run_result = {}
        # Run n1ql query
        n1ql = self.query_helper._add_explain_with_hints(n1ql_query)
        self.log.info("Running query: " + n1ql)
        message = "Pass"
        try:
            actual_result = self.n1ql_query_runner_wrapper(n1ql_query=n1ql, server=self.n1ql_server)
            self.log.info(actual_result)
            check = self.n1ql_helper.verify_explain(actual_result, keyword, present)
            if not check:
                if present:
                    message = " query {0} failed explain result, keyword {1} not found".format(n1ql_query, keyword)
                else:
                    message = " query {0} failed explain result, keyword {1} was found but should not be present".format(n1ql_query, keyword)
                self.log.info(message)
        except Exception as ex:
            self.log.info(ex)
            message = ex
            check = False
        finally:
            key = "Explain for query:  {0}".format(n1ql)
            run_result[key] = {"success": check, "result": message}
        return run_result

    def _initialize_cluster_setup(self):
        if self.use_mysql:
            self.log.info(" Will load directly from mysql")
            self._initialize_mysql_client()
            if not self.generate_input_only:
                self._setup_and_load_buckets()
        elif self.use_postgres:
            self._initialize_postgres_client()
            if not self.generate_input_only:
                self._setup_and_load_buckets()
        else:
            self.log.info(" Will load directly from file snap-shot")
            if self.populate_with_replay:
                self._initialize_mysql_client()
            self._setup_and_load_buckets_from_files()

        self.n1ql_helper = self._initialize_n1ql_helper()
        # create copy of simple table if this is a merge operation
        self.sleep(10)
        if self.gsi_type == "memory_optimized":
            os.system("curl -X POST  http://Administrator:password@{1}:8091/pools/default -d memoryQuota={0} -d indexMemoryQuota={2}".format(self.ram_quota, self.n1ql_server.ip, self.indexer_memQuota))
            self.sleep(10)
        if self.change_bucket_properties:
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_command("curl -X POST -u {0}:{1} -d maxBucketCount=25 http://{2}:{3}/internalSettings".format(self.user_cluster, self.password_cluster, self.master.ip, self.master.port))
            self.sleep(10, "Updating maxBucket count to 15")
        self._build_indexes()

    def _build_indexes(self):
        self.sec_index_map = {}
        fields = ['primary_key_id', 'bool_field1', 'char_field1', 'datetime_field1', 'decimal_field1',
                  'int_field1', 'varchar_field1']
        if self.create_secondary_indexes:
            if self.use_mysql or self.use_postgres:
                self.sec_index_map = self.client._gen_index_combinations_for_tables(partitioned_indexes=self.partitioned_indexes)
            else:
                self.sec_index_map = self._extract_secondary_index_map_from_file(self.secondary_index_info_path)
        if not self.generate_input_only:
            if self.create_primary_index:
                self._build_primary_indexes(self.using_gsi)
            if self.create_secondary_meta_indexes:
                index_name = ""
                for table_name in list(self.sec_index_map.keys()):
                    queries = {}
                    index_name = table_name
                    query = "CREATE INDEX {0} ON {1}(primary_key_id,bool_field1,char_field1," \
                            "datetime_field1," \
                            "decimal_field1,int_field1,varchar_field1)".format(table_name, self.database + "_" + table_name)
                    queries[index_name] = query
                    if self.create_secondary_ansi_join_indexes:
                        for field in fields:
                            index_name = table_name+"_"+field
                            query = "CREATE INDEX {0} ON {1}({2})".format(table_name+"_"+field, self.database+"_"+table_name, field)
                            queries[index_name] = query
                    for index_name in list(queries.keys()):
                        try:
                            self.n1ql_helper.run_cbq_query(query=queries[index_name],
                                                           server=self.n1ql_server, verbose=False)
                            check = self.n1ql_helper.is_index_online_and_in_list(self.database+"_"+table_name,
                                                                                 index_name ,
                                                                                 server=self.n1ql_server,
                                                                                 timeout=240)
                        except Exception as ex:
                            self.log.info(ex)
            if self.create_secondary_indexes and (not self.create_secondary_meta_indexes):
                thread_list = []
                if self.build_secondary_index_in_seq:
                    for table_name in list(self.sec_index_map.keys()):
                        self._gen_secondary_indexes_per_table(self.database+"_"+table_name, self.sec_index_map[table_name], 0)
                else:
                    for table_name in list(self.sec_index_map.keys()):
                        t = threading.Thread(target=self._gen_secondary_indexes_per_table, args=(self.database+"_"+table_name, self.sec_index_map[table_name]))
                        t.daemon = True
                        t.start()
                        thread_list.append(t)
                    for t in thread_list:
                        t.join()

    def _build_primary_indexes(self, using_gsi=True):
        if self.create_primary_index:
            if not self.partitioned_indexes:
                self.n1ql_helper.create_primary_index(using_gsi=using_gsi, server=self.n1ql_server)
            else:
                self.n1ql_helper.create_partitioned_primary_index(using_gsi=using_gsi, server=self.n1ql_server)

    def _load_bulk_data_in_buckets_using_n1ql(self, bucket, data_set):
        try:
            n1ql_query = self.query_helper._builk_insert_statement_n1ql(bucket.name, data_set)
            if not self.use_rest:
                new_query_helper = N1QLHelper(version="sherlock", shell=self.shell, max_verify=self.max_verify,
                           buckets=self.buckets, item_flag=None, n1ql_port=getattr(self.n1ql_server, 'n1ql_port', 8093),
                           full_docs_list=[], log=self.log, input=self.input, master=self.master,
                           database=self.database, use_rest=True)
                new_query_helper.run_cbq_query(query=n1ql_query, server=self.n1ql_server, verbose=False)
            else:
                self.n1ql_helper.run_cbq_query(query=n1ql_query, server=self.n1ql_server, verbose=False)
        except Exception as ex:
            self.log.info('WARN=======================')
            self.log.info(ex)

    def _load_data_in_buckets_using_mc_bin_client_json(self, bucket, data_set):
        client = VBucketAwareMemcached(RestConnection(self.master), bucket)
        try:
            for key in list(data_set.keys()):
                client.set(key.encode("utf8"), 0, 0, json.dumps(data_set[key]))
        except Exception as ex:
            self.log.info('WARN=======================')
            self.log.info(ex)

    def _initialize_rqg_query_helper(self):
        return RQGQueryHelper()

    def _initialize_n1ql_helper(self):
        use_engine = random.randint(1, 100)
        if use_engine <= 20 and not self.use_analytics:
            self.use_sdk = True
        elif use_engine <= 40 and not self.use_analytics:
            self.use_rest = False
            self.log.info("We are using the CBQ engine to run queries for this run")
        return N1QLHelper(version="sherlock", shell=self.shell, max_verify=self.max_verify,
                                      buckets=self.buckets, item_flag=None, n1ql_port=getattr(self.n1ql_server, 'n1ql_port', 8903),
                                      full_docs_list=[], log=self.log, input=self.input, master=self.master,
                                      database=self.database, use_rest=self.use_rest)

    def _initialize_mysql_client(self):
        if self.reset_database:
            self.client = RQGMySQLClient(host=self.mysql_url, user_id=self.user_id, password=self.password)
            if self.subquery:
                path = "b/resources/rqg/{0}/database_definition/definition-subquery.sql".format(self.database)
            else:
                path = "b/resources/rqg/{0}/database_definition/definition.sql".format(self.database)
            self.database = self.database+"_"+str(self.query_helper._random_int())
            populate_data = False
            if not self.populate_with_replay:
                populate_data = True
            if self.subquery:
                self.client.reset_database_add_data(database=self.database, items=self.items, sql_file_definiton_path=path, populate_data=populate_data, number_of_tables=1)
            else:
                 self.client.reset_database_add_data(database=self.database, items=self.items, sql_file_definiton_path=path, populate_data=populate_data, number_of_tables=self.number_of_buckets)
            self._copy_table_for_merge()
        else:
            self.client = RQGMySQLClient(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)

    def _initialize_postgres_client(self):
        self.client = RQGPostgresClient(host=self.postgres_url, user=self.user_id, password=self.password, database=self.database)
        self.client.reset_database_add_data(items=self.items)

    def _copy_table_for_merge(self):
        table_list = self.client._get_table_list()
        reference_table = table_list[0]
        if self.merge_operation:
            path = "b/resources/rqg/crud_db/database_definition/table_definition.sql"
            self.client.database_add_data(database=self.database, sql_file_definiton_path=path)
            table_list = self.client._get_table_list()
            for table_name in table_list:
                if table_name != reference_table:
                    sql = "INSERT INTO {0} SELECT * FROM {1}".format(table_name, reference_table)
                    self.client._insert_execute_query(sql)
        table_list = self.client._get_table_list()
        for table_name in table_list:
            self.client_map[table_name] = RQGMySQLClient(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)

    def _generate_result(self, data):
        result = ""
        for key in list(data.keys()):
            result +="<<<<<<<<<< TEST {0} >>>>>>>>>>> \n".format(key)
            for result_key in list(data[key].keys()):
                result += "{0} :: {1} \n".format(result_key, data[key][result_key])
        return result

    def _gen_secondary_indexes_per_table(self, table_name="", index_map={}, sleep_time=2):
        if self.partitioned_indexes:
            defer_mode = str({"defer_build": "true", "num_partition":2})
        else:
            defer_mode = str({"defer_build": "true"})
        build_index_list = []
        batch_index_definitions = index_map
        if self.pushdown:
            table_field_map = self.client._get_field_list_map_for_tables()
            fields = table_field_map['simple_table']
            combination_fields = sum([list(map(list, combinations(fields, i))) for i in range(len(fields) + 1)], [])
            for x in range(1, len(combination_fields)):
                input = combination_fields[x]
                if len(input) == 1:
                    fields_indexed = str(input[0])
                    index_name = "ix_" + str(0) + str(x)
                else:
                    fields_indexed = str(input[0])
                    #TODO: this code is really weird!
                    for i in range(1, len(input)):
                        index_name = "ix_" + str(i) + str(x)
                    fields_indexed = fields_indexed+"," + str(x[i])
                if self.partitioned_indexes:
                    query = "CREATE INDEX {0} ON {1}({2}) PARTITION BY HASH(meta().id)".format(
                        index_name, table_name, fields_indexed)
                else:
                    query = "CREATE INDEX {0} ON {1}({2})".format(index_name,
                                                                  table_name,
                                                                  fields_indexed)
                build_index_list.append(index_name)
                self.log.info(" Running Query {0} ".format(query))
                try:
                    self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server, verbose=False)
                    build_index_list.append(index_name)
                    self.n1ql_helper.is_index_online_and_in_list(table_name, index_name, server=self.n1ql_server, timeout=240)
                except Exception as ex:
                    self.log.info(ex)

        if self.dynamic_indexing:
            index_name = "idx_" + table_name
            query = "CREATE INDEX {0} ON {1}(DISTINCT ARRAY v FOR v IN PAIRS(SELF) END) WITH {2}".format(index_name, table_name, defer_mode)
            build_index_list.append(index_name)
            self.log.info(" Running Query {0} ".format(query))
            try:
                self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server, verbose=False)
                build_index_list.append(index_name)
            except Exception as ex:
                self.log.info(ex)
                raise
        else:
            for index_name in list(batch_index_definitions.keys()):
                query = "{0} WITH {1}".format(
                        batch_index_definitions[index_name]["definition"],
                        defer_mode)
                build_index_list.append(index_name)
                self.log.info(" Running Query {0} ".format(query))
                try:
                    self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server, verbose=False)
                    build_index_list.append(index_name)
                except Exception as ex:
                    self.log.info(ex)
                    traceback.print_exc()
                    raise
        # Run Build Query
        if build_index_list is not None and len(build_index_list) > 0:
            batch_size = 0
            end_index_batch = 0
            total_indexes = 0
            while total_indexes < len(build_index_list):
                start_index_batch = end_index_batch
                end_index_batch = min(end_index_batch+self.build_index_batch_size, len(build_index_list))
                batch_size += 1
                if start_index_batch == end_index_batch:
                    break
                list_build_index_list = build_index_list[start_index_batch:end_index_batch]
                total_indexes += len(list_build_index_list)
                try:
                    build_query = "BUILD INDEX on {0}({1}) USING GSI".format(table_name, ",".join(list_build_index_list))
                    actual_result = self.n1ql_helper.run_cbq_query(query=build_query, server=self.n1ql_server)
                    self.log.info(actual_result)
                    self.sleep(15, "sleep after building index")
                except Exception as ex:
                    self.log.info(ex)
                    traceback.print_exc()
                    raise
                self.sleep(sleep_time)

    def _extract_secondary_index_map_from_file(self, file_path="/tmp/index.txt"):
        with open(file_path) as data_file:
            return json.load(data_file)

    def _generate_secondary_indexes_in_batches(self, batches):
        if self.generate_input_only:
            return
        defer_mode = str({"defer_build": "true"})
        if self.partitioned_indexes:
            defer_mode = str({"defer_build": "true", "num_partition":2})
        batch_index_definitions = {}
        build_index_list = []
        # add indexes to batch_index_definitions
        for info in batches:
            table_name = info["bucket"]
            batch_index_definitions.update(info["indexes"])
        for index_name in list(batch_index_definitions.keys()):
            query = "{0} WITH {1}".format(batch_index_definitions[index_name]["definition"], defer_mode)
            query = query.replace("ON simple_table", "ON "+self.database+"_"+"simple_table")
            if self.aggregate_pushdown:
                query = query.replace("limit 10 offset 4", "")
            self.log.info(" Running Query {0} ".format(query))
            try:
                self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server)
                if index_name not in build_index_list:
                    build_index_list.append(index_name)
            except Exception as ex:
                self.log.info(ex)
                traceback.print_exc()
                raise
        # Run Build Query
        if build_index_list is not None and len(build_index_list) > 0:
            try:
                build_query = "BUILD INDEX on {0}({1}) USING GSI".format(self.database+"_"+table_name, ",".join(build_index_list))
                actual_result = self.n1ql_helper.run_cbq_query(query=build_query, server=self.n1ql_server)
                self.log.info(actual_result)
            except Exception as ex:
                self.log.info(ex)
                traceback.print_exc()
                raise
            # Monitor till the index is built
            tasks = []
            try:
                for info in batches:
                    table_name = info["bucket"]
                    table_name = self.database+"_"+table_name
                    for index_name in info["indexes"]:
                        if index_name in build_index_list:
                            tasks.append(self.async_monitor_index(bucket=table_name, index_name=index_name))
                for task in tasks:
                    task.result()
            except Exception as ex:
                traceback.print_exc()
                self.log.info(ex)

    def async_monitor_index(self, bucket, index_name=None):
        monitor_index_task = self.cluster.async_monitor_index(server=self.n1ql_server, bucket=bucket,
                                                              n1ql_helper=self.n1ql_helper, index_name=index_name)
        return monitor_index_task

    def are_any_indexes_present(self, index_name_list):
        query_response = self.n1ql_helper.run_cbq_query("SELECT * FROM system:indexes")
        current_indexes = [i['indexes']['name'] for i in query_response['results']]
        for index_name in index_name_list:
            if index_name in current_indexes:
                return True
        return False

    def wait_for_index_drop(self, index_name_list):
        self.with_retry(lambda: self.are_any_indexes_present(index_name_list), eval=False, delay=1, tries=30)

    def with_retry(self, func, eval=True, delay=5, tries=10):
        attempts = 0
        while attempts < tries:
            attempts = attempts + 1
            res = func()
            if res == eval:
                return res
            else:
                self.sleep(delay, 'incorrect results, sleeping for %s' % delay)
        raise Exception('timeout, invalid results: %s' % res)

    def _drop_secondary_indexes_in_batches(self, batches):
        dropped_indexes = []
        for info in batches:
            table_name = info["bucket"]
            table_name = self.database+"_"+table_name
            for index_name in list(info["indexes"].keys()):
                if index_name not in dropped_indexes:
                    query = "DROP INDEX {0}.{1} USING {2}".format(table_name, index_name,
                                                                  info["indexes"][index_name]["type"])
                    try:
                        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_server,
                                                       query_params={'timeout': '900s'})
                        dropped_indexes.append(index_name)
                    except Exception as ex:
                        self.log.info("Error: " + str(ex))
            self.wait_for_index_drop(list(info["indexes"].keys()))

    def _analyze_result(self, result):
        check = True
        failure_types = []
        message = "\n ____________________________________________________\n "
        for key in list(result.keys()):
            if key != "test_case_number" and key != "n1ql_query" and key != "sql_query" and key!="check_explain_plan":
                check = check and result[key]["success"]
                if not result[key]["success"]:
                    failure_types.append(key)
                    message += " Scenario ::  {0} \n".format(key)
                    message += " Reason :: " + str(result[key]["result"]) + "\n"
            if key == "check_explain_plan":
                check = check and result[key]["success"]
                if not result[key]["success"]:
                    failure_types.append(key)
                    message += " Scenario ::  {0} \n".format(key)
                    message += " Reason :: Secondary index is not in use\n"

        return check, message, failure_types

    def _check_and_push_failure_record_queue(self, result, data, failure_record_queue):
        if not self.record_failure:
            return
        for key in list(result.keys()):
            if key != "test_case_number" and key != "n1ql_query" and key != "sql_query" and not result[key]["success"]:
                failure_record_queue.put(data)

    def dump_failure_data(self, failure_record_queue):
        if not self.record_failure:
            return
        import uuid
        sub_dir = str(uuid.uuid4()).replace("-", "")
        self.data_dump_path = self.failure_record_path+"/"+sub_dir
        os.mkdir(self.data_dump_path)
        input_file_path = self.data_dump_path+"/input"
        os.mkdir(input_file_path)
        f_write_file = open(input_file_path+"/source_input_rqg_run.txt", 'w')
        secondary_index_path = self.data_dump_path+"/index"
        os.mkdir(secondary_index_path)
        database_dump = self.data_dump_path+"/db_dump"
        os.mkdir(database_dump)
        f_write_index_file = open(secondary_index_path+"/secondary_index_definitions.txt", 'w')
        client = None
        if self.use_mysql:
            client = RQGMySQLClient(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)
        elif self.use_postgres:
            client = RQGPostgresClient(host=self.postgres_url, user=self.user_id, password=self.password, database=self.database)
        client.dump_database(data_dump_path=database_dump)
        client._close_connection()
        f_write_index_file.write(json.dumps(self.sec_index_map))
        f_write_index_file.close()
        while not failure_record_queue.empty():
            f_write_file.write(json.dumps(failure_record_queue.get())+"\n")
        f_write_file.close()

    def unzip_template(self, template_path):
        if "zip" not in template_path:
            return template_path
        tokens = template_path.split("/")
        file_name = tokens[len(tokens)-1]
        output_path = template_path.replace(file_name, "")
        with zipfile.ZipFile(template_path, "r") as z:
            z.extractall(output_path)
        template_path = template_path.replace(".zip", "")
        return template_path

    def _setup_and_load_buckets_from_files(self):
        bucket_list = []
        #Unzip the files and get bucket list
        tokens = self.db_dump_path.split("/")
        data_file_path = self.db_dump_path.replace(tokens[len(tokens)-1], "data_dump")
        os.mkdir(data_file_path)
        with zipfile.ZipFile(self.db_dump_path, "r") as z:
            z.extractall(data_file_path)
        onlyfiles = [f for f in listdir(data_file_path) if isfile(join(data_file_path, f))]
        for file in onlyfiles:
            bucket_list.append(file.split(".")[0])
        # Remove any previous buckets
        for bucket in self.buckets:
            self.rest.delete_bucket(bucket.name)
        self.buckets = []
        # Create New Buckets
        self._create_buckets(self.master, bucket_list, server_id=None, bucket_size=256)
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
                            for key in list(data.keys()):
                                insert_sql = self.query_helper._generate_insert_statement_from_data(bucket_name, data[key])
                                self.client._insert_execute_query(insert_sql)
        shutil.rmtree(data_file_path, ignore_errors=True)

    def fill_advise_dict(self, bucket_list=[]):
        for bucket in bucket_list:
            if bucket not in self.advise_dict.keys():
                self.advise_dict[bucket] = self.advise_buckets[0]
                self.advise_buckets.remove(self.advise_buckets[0])


    def _setup_and_load_buckets(self):
        # Remove any previous buckets
        if self.skip_setup_cleanup:
            for bucket in self.buckets:
                self.rest.delete_bucket(bucket.name)
        self.buckets = []
        bucket_size = 256

        if self.change_bucket_properties:
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_command("curl -X POST -u {0}:{1} -d maxBucketCount=25 http://{2}:{3}/internalSettings".format(self.user_cluster, self.password_cluster, self.master.ip, self.master.port))
            self.sleep(10, "Updating maxBucket count to 25")
        # Pull information about tables from mysql database and interpret them as no-sql dbs
        table_key_map = self.client._get_primary_key_map_for_tables()
        # Make a list of buckets that we want to create for querying
        bucket_list = list(table_key_map.keys())
        self.log.info("database used is {0}".format(self.database))
        new_bucket_list = []
        i = 1
        for bucket in bucket_list:
            if bucket.find("copy_simple_table") > 0:
                new_bucket_list.append(self.database+"_"+"copy_simple_table")
            else:
                if i <= self.number_of_buckets:
                    new_bucket_list.append(self.database + "_" + bucket)
                    if self.subquery:
                        break
                i = i + 1
        # Create New Buckets
        self.fill_advise_dict(new_bucket_list)
        self._create_buckets(self.master, new_bucket_list, server_id=None, bucket_size=bucket_size)
        self.log.info("buckets created")

        # Wait till the buckets are up
        self.sleep(5)
        self.buckets = self.rest.get_buckets()
        self.newbuckets = []
        for bucket in self.buckets:
            if bucket.name in new_bucket_list:
                self.newbuckets.append(bucket)

        self.log.info("safe to start another job")
        self.record_db = {}
        self.buckets = self.newbuckets
        # Read Data from mysql database and populate the couchbase server
        for bucket_name in bucket_list:
            query = "select * from {0}".format(bucket_name)
            columns, rows = self.client._execute_query(query=query)
            self.record_db[bucket_name] = self.client._gen_json_from_results_with_primary_key(columns, rows, primary_key=table_key_map[bucket_name])
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
        query = "delete from {0} where primary_key_id is not null".format(table_name)
        client._insert_execute_query(query=query)
        query = "delete from {0} where primary_key_id is not null".format(self.database+"_"+table_name)
        self.n1ql_query_runner_wrapper(n1ql_query=query, server=self.n1ql_server, verbose=True)
        insert_sql = "insert into {0}(KEY k ,VALUE b) SELECT meta(b).id as k, b from {1} b where primary_key_id is not null".format(self.database+"_"+table_name,self.database+"_"+"copy_simple_table")
        if self.use_advisor:
            self.create_secondary_index("SELECT meta(b).id as k, b from {0} b where primary_key_id is not null".format(self.database+"_"+"copy_simple_table"))

        try:
            self.log.info("n1ql query is {0}".format(insert_sql))
            self.n1ql_helper.run_cbq_query(query=insert_sql, server=self.n1ql_server, verbose=True)
            insert_sql = "INSERT INTO {0} SELECT * FROM copy_simple_table".format(table_name)
            client._insert_execute_query(insert_sql)
        except Exception as ex:
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
            query = 'select varchar_field1 from simple_table_1  where primary_key_id  = {0}'.format(primary_key_value)
            result = self.client._execute_sub_query(query)
            varchar_value = result
            query = 'select char_field1 from simple_table_1  where primary_key_id  = {0}'.format(primary_key_value)
            result = self.client._execute_sub_query(query)
            char_value = result
            orderid1 = "order-" + varchar_field
            orderid2 = "order-" + str(self.query_helper._random_char()) + "_"+str(self.query_helper._random_int()) + varchar_field
            price1 = self.query_helper._random_float()+10
            price2 = self.query_helper._random_float()+100
            qty1 = self.query_helper._random_int()
            qty2 = self.query_helper._random_int()
            query = 'insert into simple_table_2 (order_id, qty, productId, price, primary_key_id) values ("%s", %s, "snack", %s, %s)' % (orderid1, qty1, price1, primary_key_value)
            self.client._insert_execute_query(query)
            query = 'insert into simple_table_2 (order_id, qty, productId, price, primary_key_id) values ("%s", %s, "lunch", %s, %s)' % (orderid2, qty2, price2, primary_key_value)
            self.client._insert_execute_query(query)
            n1ql_insert_template = 'INSERT INTO %s (KEY, VALUE) VALUES ' \
                                   '("%s", {"primary_key_id": "%s" ,"decimal_field1":%s,"int_field1":%s,' \
                                   '"datetime_field1":"%s","bool_field1":%s,"varchar_field1":"%s",' \
                                   '"char_field1":"%s","simple_table_2":[{"order_id":"%s","qty":%s,' \
                                   '"productId":"snack","price":%s,"primary_key_id":"%s"},' \
                                   '{"order_id":"%s","qty":%s,"productId":"lunch","price":%s,' \
                                   '"primary_key_id":"%s"}] } )'\
                                   % (bucket.name, primary_key_value, primary_key_value, decimal_field_value,
                                      int_field_value, datetime_field_value, bool_field_value, varchar_value,
                                      char_value, orderid1, qty1, price1, primary_key_value, orderid2, qty2,
                                      price2, primary_key_value)
            self.n1ql_helper.run_cbq_query(query=n1ql_insert_template, server=self.n1ql_server)

    def _round_float_results(self, results, round_level=0):
        if round_level > 0:
            for res in results:
                for key in res.keys():
                    if isinstance(res[key], float):
                        res[key] = round(res[key], round_level)
        return results
