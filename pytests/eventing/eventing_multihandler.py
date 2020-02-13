import datetime
import json
import random

from couchbase_helper.tuq_helper import N1QLHelper
from eventing.eventing_base import EventingBaseTest
from eventing.eventing_constants import HANDLER_CODE
from membase.api.rest_client import RestConnection
from testconstants import STANDARD_BUCKET_PORT


class EventingMultiHandler(EventingBaseTest):
    def setUp(self):
        super(EventingMultiHandler, self).setUp()
        self.num_src_buckets=self.input.param('num_src_buckets', 1)
        self.num_dst_buckets=self.input.param('num_dst_buckets', 1)
        self.num_handlers=self.input.param('num_handlers', 1)
        self.deploy_handler=self.input.param('deploy_handler',1)
        self.sequential=self.input.param('sequential',True)
        self.num_pause=self.input.param('num_pause',0)
        self.worker_count=self.input.param('worker_count',1)
        self.handler_code=self.input.param('handler_code','handler_code/delete_doc_bucket_op.js')
        self.gens_load = self.generate_docs(self.docs_per_day)
        quota=(self.num_src_buckets+self.num_dst_buckets)*100+400
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=quota)
        self.metadata_bucket_size = 400
        bucket_params_meta = self._create_bucket_params(server=self.server, size=self.metadata_bucket_size,
                                                        replicas=self.num_replicas)
        self.create_n_buckets(self.src_bucket_name,self.num_src_buckets)
        self.buckets = RestConnection(self.master).get_buckets()
        if self.num_dst_buckets > 0:
            self.create_n_buckets(self.dst_bucket_name,self.num_dst_buckets)
        self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                            bucket_params=bucket_params_meta)
        self.deploying=[]
        self.pausing=[]
        if "n1ql" in self.handler_code:
            self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.n1ql_helper = N1QLHelper(shell=self.shell, max_verify=self.max_verify, buckets=self.buckets,
                                          item_flag=self.item_flag, n1ql_port=self.n1ql_port,
                                          full_docs_list=self.full_docs_list, log=self.log, input=self.input,
                                          master=self.master, use_rest=True)
            self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)

    def create_n_buckets(self,name,number):
        self.bucket_size = 100
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas)
        for i in range(number):
            self.cluster.create_standard_bucket(name+"_"+str(i), port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)

    def create_n_handler(self,num_handler,num_src,num_dst,handler_code):
        src_bucket=self.src_bucket_name
        dst_bucket=self.dst_bucket_name
        for i in range(num_handler):
            self.src_bucket_name=src_bucket+"_"+str(i%num_src)
            if num_dst > 0:
                self.dst_bucket_name=dst_bucket+"_"+str(i%num_dst)
            body = self.create_save_function_body(self.function_name+"_"+str(i),handler_code,
                                                  worker_count=self.worker_count,deployment_status=False,processing_status=False)
            if num_dst == 0 and not self.is_sbm :
                del body['depcfg']['buckets'][0]
            self.log.info("Creating the following handler code : {0} with {1}".format(body['appname'], body['depcfg']))
            self.log.info("\n{0}".format(body['appcode']))
            self.rest.create_function(body["appname"],body)

    def deploy_n_handler(self,num,sequential=True):
        funcs = self.handler_status_map()
        if num > len(funcs):
            num=len(funcs)
        deployed=0
        for key in funcs:
            if deployed == num:
                break
            self.log.info("Deploying the following handler code : {0}".format(key))
            self.deploy_handler_by_name(key,wait_for_bootstrap=sequential)
            self.deploying.append(key)
            deployed=deployed+1

    def undeploy_delete_all_handler(self):
        for key in self.deploying:
            self.log.info("Deploying the following handler code : {0}".format(key))
            self.rest.undeploy_function(key)
        for key in self.deploying:
            self.wait_for_handler_state(key,"undeployed")
        self.rest.delete_all_function()

    def pause_n_functions(self,num,sequential=True):
        if num > len(self.deploying):
            num = len(self.deploying)
        paused = 0
        for key in self.deploying:
            if paused == num:
                break
            self.log.info("Deploying the following handler code : {0}".format(key))
            self.pause_handler_by_name(key,wait_for_pause=sequential)
            self.pausing.append(key)
            paused = paused + 1

    def wait_for_handlers_to_deployed(self):
        for name in self.deploying:
            self.wait_for_handler_state(name,"deployed")

    def wait_for_handlers_to_paused(self):
        for name in self.pausing:
            self.wait_for_handler_state(name,"paused")

    def print_handlers_state(self):
        self.log.info("==========================================================================")
        self.log.info("handler status: \n {}".format(self.handler_status_map()))

    def reset_param(self):
        self.src_bucket_name = "src_bucket"
        self.dst_bucket_name = "dst_bucket"
        self.sleep(3)
        random.seed(datetime.datetime.now())
        function_name = "Function_{0}_{1}".format(random.randint(1, 1000000000), self._testMethodName)
        # See MB-28447, From now function name can only be max of 100 chars
        self.function_name = function_name[0:90]

    def test_multiple_handle_multiple_buckets_preload(self):
        # load data
        self.load(self.gens_load, buckets=self.buckets, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.create_n_handler(self.num_handlers,self.num_src_buckets,self.num_dst_buckets,self.handler_code)
        self.deploy_n_handler(self.deploy_handler,sequential=self.sequential)
        self.wait_for_handlers_to_deployed()
        if self.num_pause > 0:
            self.pause_n_functions(self.num_pause)
            self.wait_for_handlers_to_paused()
        self.undeploy_delete_all_handler()

    def test_multiple_handle_multiple_buckets(self):
        self.create_n_handler(self.num_handlers,self.num_src_buckets,self.num_dst_buckets,self.handler_code)
        self.deploy_n_handler(self.deploy_handler,sequential=self.sequential)
        # load data
        self.load(self.gens_load, buckets=self.buckets, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.wait_for_handlers_to_deployed()
        self.print_handlers_state()
        if self.num_pause > 0:
            self.pause_n_functions(self.num_pause)
            self.wait_for_handlers_to_paused()
        self.undeploy_delete_all_handler()

    def test_multiple_handle_multiple_create_only(self):
        # load data
        self.load(self.gens_load, buckets=self.buckets, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.create_n_handler(self.num_handlers,self.num_src_buckets,self.num_dst_buckets,self.handler_code)
        self.print_handlers_state()
        self.undeploy_delete_all_handler()

    def test_mix_handlers(self):
        # load data
        self.load(self.gens_load, buckets=self.buckets, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.create_n_handler(self.num_handlers, self.num_src_buckets, self.num_dst_buckets, "handler_code/no_op.js")
        #self.deploy_n_handler(self.deploy_handler, sequential=self.sequential)
        self.reset_param()
        self.create_n_handler(self.num_handlers, self.num_src_buckets, self.num_dst_buckets, "handler_code/delete_doc_bucket_op.js")
        #self.deploy_n_handler(self.deploy_handler, sequential=self.sequential)
        self.reset_param()
        self.create_n_handler(self.num_handlers, self.num_src_buckets, self.num_dst_buckets, "handler_code/bucket_op_with_timers.js")
        #self.deploy_n_handler(self.deploy_handler, sequential=self.sequential)
        self.reset_param()
        self.create_n_handler(self.num_handlers, self.num_src_buckets, self.num_dst_buckets, "handler_code/n1ql_op_without_timers.js")
        #self.deploy_n_handler(self.deploy_handler, sequential=self.sequential)
        self.reset_param()
        self.create_n_handler(self.num_handlers, self.num_src_buckets, self.num_dst_buckets, "handler_code/n1ql_op_with_timers.js")
        self.deploy_n_handler(self.deploy_handler, sequential=self.sequential)
        self.wait_for_handlers_to_deployed()
        self.print_handlers_state()
        self.undeploy_delete_all_handler()

