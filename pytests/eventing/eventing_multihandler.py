import datetime
import json
import random
from string import Template

from couchbase_helper.tuq_helper import N1QLHelper
from eventing.eventing_base import EventingBaseTest
from eventing.eventing_constants import HANDLER_CODE
from membase.api.rest_client import RestConnection
from testconstants import STANDARD_BUCKET_PORT

from lib.membase.api.rest_client import RestHelper


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
        self.handler_code=self.input.param('handler_code','handler_code/ABO/insert.js')
        self.gens_load = self.generate_docs(self.docs_per_day)
        memory_quota = (self.num_src_buckets+self.num_dst_buckets+3) * self.bucket_size
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=memory_quota)
        self.create_n_buckets(self.src_bucket_name,self.num_src_buckets)
        self.buckets = self.rest.get_buckets()
        if self.num_dst_buckets > 0:
            self.create_n_buckets(self.dst_bucket_name,self.num_dst_buckets)
        self.create_scope_collection(bucket=self.metadata_bucket_name, scope=self.metadata_bucket_name,
                                     collection=self.metadata_bucket_name)
        self.deploying=[]
        self.pausing=[]
        if self.n1ql_server:
            self.n1ql_helper.drop_primary_index(using_gsi=True, server=self.n1ql_server)
            self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_server)
        self.binding_map={}

    def create_n_buckets(self,name,number):
        self.bucket_size = 100
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas)
        for i in range(number):
            self.cluster.create_standard_bucket(name+"_"+str(i), port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.create_scope_collection(bucket=name+"_"+str(i), scope="scope_0",
                                         collection="coll_0")


    def create_n_handler(self,num_handler,num_src,num_dst,handler_code):
        src_bucket=self.src_bucket_name
        dst_bucket=self.dst_bucket_name
        for i in range(num_handler):
            self.src_bucket_name=src_bucket+"_"+str(i%num_src)
            source_namespace=self.src_bucket_name+".scope_0.coll_0"
            meta_namespace=self.metadata_bucket_name+"."+self.metadata_bucket_name+"."+self.metadata_bucket_name
            dst_namespace=[]
            if num_dst > 0:
                self.dst_bucket_name=dst_bucket+"_"+str(i%num_dst)
                binding=self.dst_bucket_name + ".scope_0.coll_0"
                dst_namespace.append("dst_bucket."+binding+".rw")
                if binding not in self.binding_map.keys():
                    self.binding_map[binding]=1
                else:
                    self.binding_map[binding] = self.binding_map[binding]+1
            body = self.create_function_with_collection(self.function_name+"_"+str(i),handler_code,
                                                        src_namespace=source_namespace,meta_namespace=meta_namespace,collection_bindings=dst_namespace)
            if "template" in handler_code:
                body['appcode'] = Template(body['appcode']).substitute(key=str("func_"+str(i)))
            # if num_dst == 0 and not self.is_sbm and len(body['depcfg']['buckets']) > 0:
            #     del body['depcfg']['buckets'][0]
            self.log.info("Creating the following handler code : {0} with {1}".format(body['appname'], body['depcfg']))
            self.log.info("\n{0}".format(body['appcode']))
            self.rest.create_function(body["appname"],body)
            print(self.binding_map)

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
        self.refresh_rest_server()
        for key in self.deploying:
            self.log.info("Undeploying the following handler code : {0}".format(key))
            self.undeploy_function_by_name(key,wait_for_undeployment=False)
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
        random.seed(datetime.datetime.now().timestamp())
        function_name = "Function_{0}_{1}".format(random.randint(1, 1000000000), self._testMethodName)
        # See MB-28447, From now function name can only be max of 100 chars
        self.function_name = function_name[0:90]

    def test_multiple_handle_multiple_buckets_preload(self):
        # load data
        self.load_data_to_all_source_collections()
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
        self.load_data_to_all_source_collections()
        self.wait_for_handlers_to_deployed()
        self.print_handlers_state()
        if self.num_pause > 0:
            self.pause_n_functions(self.num_pause)
            self.wait_for_handlers_to_paused()
        self.undeploy_delete_all_handler()

    def test_multiple_handle_multiple_create_only(self):
        # load data
        self.load_data_to_all_source_collections()
        self.create_n_handler(self.num_handlers,self.num_src_buckets,self.num_dst_buckets,self.handler_code)
        self.print_handlers_state()
        self.undeploy_delete_all_handler()

    def test_mix_handlers(self):
        # load data
        self.load_data_to_all_source_collections()
        self.create_n_handler(self.num_handlers, self.num_src_buckets, self.num_dst_buckets, "handler_code/no_op.js")
        #self.deploy_n_handler(self.deploy_handler, sequential=self.sequential)
        self.reset_param()
        self.create_n_handler(self.num_handlers, self.num_src_buckets, self.num_dst_buckets, "handler_code/ABO/insert.js")
        #self.deploy_n_handler(self.deploy_handler, sequential=self.sequential)
        self.reset_param()
        self.create_n_handler(self.num_handlers, self.num_src_buckets, self.num_dst_buckets, "handler_code/ABO/insert_timer.js")
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

    def load_data_to_all_source_collections(self,is_delete=False):
        buckets = RestConnection(self.master).get_buckets()
        task=[]
        for bucket in buckets:
            if "src_bucket_" in bucket.name:
                task.append(self.load_data_to_collection(self.docs_per_day * self.num_docs, bucket.name+".scope_0.coll_0"
                                                         ,is_delete=is_delete,wait_for_loading=False))
        for tk in task:
            tk.result()

    def verify_destination_buckets(self,num_docs):
        for bind in self.binding_map:
            self.verify_doc_count_collections(bind,num_docs *self.binding_map[bind])

    def test_multiple_handle_multiple_collections_rebalance_in(self):
        # load data
        self.load_data_to_all_source_collections()
        self.create_n_handler(self.num_handlers,self.num_src_buckets,self.num_dst_buckets,self.handler_code)
        self.deploy_n_handler(self.deploy_handler,sequential=self.sequential)
        self.wait_for_handlers_to_deployed()
        # rebalance in a eventing node when eventing is processing mutations
        services_in = ["eventing"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_destination_buckets(self.docs_per_day * self.num_docs)
        # delete load data
        self.load_data_to_all_source_collections(is_delete=True)
        self.verify_destination_buckets(0)
        self.undeploy_delete_all_handler()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_multiple_handle_multiple_collections_rebalance_out(self):
        # load data
        self.load_data_to_all_source_collections()
        self.create_n_handler(self.num_handlers,self.num_src_buckets,self.num_dst_buckets,self.handler_code)
        self.deploy_n_handler(self.deploy_handler,sequential=self.sequential)
        self.wait_for_handlers_to_deployed()
        # rebalance out a eventing node when eventing is processing mutations
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_ev])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_destination_buckets(self.docs_per_day * self.num_docs)
        # delete load data
        self.load_data_to_all_source_collections(is_delete=True)
        self.verify_destination_buckets(0)
        self.undeploy_delete_all_handler()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_multiple_handle_multiple_collections_swap_rebalance(self):
        # load data
        self.load_data_to_all_source_collections()
        self.create_n_handler(self.num_handlers,self.num_src_buckets,self.num_dst_buckets,self.handler_code)
        self.deploy_n_handler(self.deploy_handler,sequential=self.sequential)
        self.wait_for_handlers_to_deployed()
        # swap rebalance an eventing node when eventing is processing mutations
        services_in = ["eventing"]
        nodes_out_ev = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 nodes_out_ev, services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=200)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_destination_buckets(self.docs_per_day * self.num_docs)
        # delete load data
        self.load_data_to_all_source_collections(is_delete=True)
        self.verify_destination_buckets(0)
        self.undeploy_delete_all_handler()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_multiple_handle_multiple_collections_rebalance_in_kv(self):
        # load data
        self.load_data_to_all_source_collections()
        self.create_n_handler(self.num_handlers,self.num_src_buckets,self.num_dst_buckets,self.handler_code)
        self.deploy_n_handler(self.deploy_handler,sequential=self.sequential)
        self.wait_for_handlers_to_deployed()
        # rebalance in a kv node when eventing is processing mutations
        services_in = ["kv"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_destination_buckets(self.docs_per_day * self.num_docs)
        # delete load data
        self.load_data_to_all_source_collections(is_delete=True)
        self.verify_destination_buckets(0)
        self.undeploy_delete_all_handler()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_multiple_handle_multiple_collections_rebalance_out_kv(self):
        # load data
        self.load_data_to_all_source_collections()
        self.create_n_handler(self.num_handlers,self.num_src_buckets,self.num_dst_buckets,self.handler_code)
        self.deploy_n_handler(self.deploy_handler,sequential=self.sequential)
        self.wait_for_handlers_to_deployed()
        # rebalance out a kv node when eventing is processing mutations
        nodes_out_kv = self.servers[1]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [nodes_out_kv])
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_destination_buckets(self.docs_per_day * self.num_docs)
        # delete load data
        self.load_data_to_all_source_collections(is_delete=True)
        self.verify_destination_buckets(0)
        self.undeploy_delete_all_handler()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

    def test_multiple_handle_multiple_collections_swap_rebalance_kv(self):
        # load data
        self.load_data_to_all_source_collections()
        self.create_n_handler(self.num_handlers,self.num_src_buckets,self.num_dst_buckets,self.handler_code)
        self.deploy_n_handler(self.deploy_handler,sequential=self.sequential)
        self.wait_for_handlers_to_deployed()
        # swap rebalance an kv node when eventing is processing mutations
        services_in = ["kv"]
        nodes_out_kv = self.servers[1]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                 [nodes_out_kv], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.verify_destination_buckets(self.docs_per_day * self.num_docs)
        # delete load data
        self.load_data_to_all_source_collections(is_delete=True)
        self.verify_destination_buckets(0)
        self.undeploy_delete_all_handler()
        # Get all eventing nodes
        nodes_out_list = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        # rebalance out all eventing nodes
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], nodes_out_list)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
