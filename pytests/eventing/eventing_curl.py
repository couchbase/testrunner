import copy

from lib.membase.api.rest_client import RestConnection
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.couchbase_helper.tuq_helper import N1QLHelper
from string import Template
import os
import logging
import datetime

log = logging.getLogger()

class EventingCurl(EventingBaseTest):
        def setUp(self):
            super(EventingCurl, self).setUp()
            self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=700)
            if self.create_functions_buckets:
                self.replicas = self.input.param("replicas", 0)
                self.bucket_size = 100
                # This is needed as we have increased the context size to 93KB. If this is not increased the metadata
                # bucket goes into heavy DGM
                self.metadata_bucket_size = 400
                log.info(self.bucket_size)
                bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                           replicas=self.replicas)
                bucket_params_meta = self._create_bucket_params(server=self.server, size=self.metadata_bucket_size,
                                                                replicas=self.replicas)
                self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                    bucket_params=bucket_params)
                self.src_bucket = RestConnection(self.master).get_buckets()
                self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                    bucket_params=bucket_params)
                self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                    bucket_params=bucket_params_meta)
                self.buckets = RestConnection(self.master).get_buckets()
            self.gens_load = self.generate_docs(self.docs_per_day)
            self.expiry = 3
            self.handler_code = self.input.param('handler_code', 'bucket_op_curl')
            self.curl_username= self.input.param('curl_user', None)
            self.curl_password= self.input.param('curl_password', None)
            self.auth_type= self.input.param('auth_type', 'no-auth')
            self.url= self.input.param('path', None)
            if self.handler_code == 'bucket_op_curl':
                self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL

        def tearDown(self):
            self.delete_temp_handler_code()
            super(EventingCurl, self).tearDown()

        def test_curl_get(self):
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            temp_handler=self.get_handler_code(self.handler_code, self.url)
            body = self.create_save_function_body(self.function_name, temp_handler, worker_count=3)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)

        def get_handler_code(self, handler_code, url):
            script_dir = os.path.dirname(__file__)
            abs_file_path = os.path.join(script_dir, handler_code)
            fh = open(abs_file_path, "r")
            code = Template(fh.read()).substitute(url=url)
            fh.close()
            ts = datetime.datetime.now().strftime('%m%d%y%H%M%S%f')
            temp_file_path = HANDLER_CODE.N1QL_TEMP_PATH + "f_" + ts + ".js"
            abs_file_path = os.path.join(script_dir, temp_file_path)
            fw = open(abs_file_path, "w+")
            fw.write(code)
            fw.close()
            return temp_file_path

        def delete_temp_handler_code(self, path=HANDLER_CODE.N1QL_TEMP_PATH):
            log.info("deleting all the handler codes")
            script_dir = os.path.dirname(__file__)
            dirPath = os.path.join(script_dir, path)
            fileList = os.listdir(dirPath)
            for fileName in fileList:
                os.remove(dirPath + "/" + fileName)

        def test_bearer_auth(self):
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_BEARER, worker_count=3)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
            self.undeploy_and_delete_function(body)

        def test_curl_with_different_handlers(self):
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            # delete json documents
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)


        def test_curl_with_different_handlers_pause_resume(self):
            gen_load_del = copy.deepcopy(self.gens_load)
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            self.deploy_function(body)
            # load some data
            task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                    self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
            self.pause_function(body)
            task.result()
            self.resume_function(body)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            # delete json documents
            self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.pause_resume_n(body, 1)
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)


        def test_curl_with_different_handlers_n1ql(self):
            self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.n1ql_helper = N1QLHelper(shell=self.shell, max_verify=self.max_verify, buckets=self.buckets,
                                          item_flag=self.item_flag, n1ql_port=self.n1ql_port,
                                          full_docs_list=self.full_docs_list, log=self.log, input=self.input,
                                          master=self.master, use_rest=True)
            self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            # delete json documents
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)

        def test_curl_takes_more_time(self):
            body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=3, execution_timeout=5)
            self.deploy_function(body)
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.sleep(20)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)