import copy

from lib.membase.api.rest_client import RestConnection
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL
from lib.testconstants import STANDARD_BUCKET_PORT
from string import Template
import os
import logging
import datetime, json

log = logging.getLogger()

class EventingCurl(EventingBaseTest):
        def setUp(self):
            super(EventingCurl, self).setUp()
            self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
            self.gens_load = self.generate_docs(self.docs_per_day)
            self.handler_code = self.input.param('handler_code', 'bucket_op_curl')
            self.curl_username= self.input.param('curl_user', None)
            self.curl_password= self.input.param('curl_password', None)
            self.auth_type= self.input.param('auth_type', 'no-auth')
            self.url= self.input.param('path', None)
            if self.handler_code == 'bucket_op_curl':
                self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL
            elif self.handler_code == 'curl_timeout_param':
                self.handler_code = HANDLER_CODE_CURL.CURL_TIMEOUT_PARAM
            elif self.handler_code == 'curl_small_timeout_param':
                self.handler_code = HANDLER_CODE_CURL.CURL_SMALL_TIMEOUT_PARAM

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
            self.verify_eventing_results(self.function_name, 0,skip_stats_validation=True)

        def test_curl_with_different_handlers_pause_resume_n1ql(self):
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
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016,skip_stats_validation=True)
            # delete json documents
            self.load(gen_load_del, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.pause_resume_n(body,1)
            self.verify_eventing_results(self.function_name, 0,skip_stats_validation=True)
            self.undeploy_and_delete_function(body)

        #MB-35455
        def test_curl_with_double_slash(self):
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.hostname=self.hostname[:-1]
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016,skip_stats_validation=True)
            # delete json documents
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0,skip_stats_validation=True)
            self.undeploy_and_delete_function(body)

        #MB-35455
        def test_curl_with_no_slash(self):
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.hostname=self.hostname[:-1]
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016,skip_stats_validation=True)
            # delete json documents
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0,skip_stats_validation=True)
            self.undeploy_and_delete_function(body)

        #MB-35455
        def test_curl_with_leading_slash(self):
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.hostname=self.hostname[:-1]
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016,skip_stats_validation=True)
            # delete json documents
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0,skip_stats_validation=True)
            self.undeploy_and_delete_function(body)

        def test_curl_parent_binding(self):
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.hostname = self.hostname[:-1]
            body = self.create_save_function_body(self.function_name, "handler_code/curl/bucket_op_curl_get_parent.js", worker_count=3,hostpath="/a/b/")
            self.deploy_function(body)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            # delete json documents
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)

        def test_bad_ssl_cert(self):
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, "handler_code/curl/bad_ssl_cert.js",validate_ssl=True)
            self.deploy_function(body)
            # Wait for eventing to catch up with all the create mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            # delete json documents
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)

        def test_curl_restrict_access_to_diag_eval_endpoint_via_eventing(self):
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, self.handler_code,
                                                  worker_count=3)
            self.deploy_function(body)
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            self.undeploy_and_delete_function(body)

        # MB-46304
        def test_curl_enable_disable_feature(self):
            self.load(self.gens_load, buckets=self.src_bucket,
                      flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.rest.disable_curl()
            body = self.create_save_function_body(
                self.function_name, "handler_code/curl/curl_enable_disable.js")
            self.deploy_function(body)
            self.verify_doc_count_collections("dst_bucket._default._default",
                                              self.docs_per_day * self.num_docs)
            self.rest.enable_curl()
            self.load(self.gens_load, buckets=self.src_bucket,
                      flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
            self.undeploy_and_delete_function(body)

        # MB-55045
        def test_curl_url_encoding_behaviour_based_on_language_compatibility(self):
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.import_function_from_directory(self.import_function)
            self.deploy_function(body)
            self.verify_doc_count_collections("dst_bucket._default._default",
                                              self.docs_per_day * self.num_docs * 3)
            self.undeploy_and_delete_function(body)

        # MB-59742
        def test_curl_access_to_diag_eval_via_multiple_slashes_in_the_path(self):
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
            body = self.create_save_function_body(self.function_name, self.handler_code)
            self.deploy_function(body)
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True)
            self.verify_doc_count_collections("dst_bucket._default._default", 0)
            self.undeploy_and_delete_function(body)

        def test_curl_with_different_timeouts_for_individual_requests(self):
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
            body = self.create_save_function_body(self.function_name, self.handler_code)
            self.deploy_function(body)
            self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
            self.undeploy_and_delete_function(body)