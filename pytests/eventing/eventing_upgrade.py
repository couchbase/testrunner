import queue

from couchbase_helper.tuq_helper import N1QLHelper
from eventing.eventing_base import EventingBaseTest
from membase.api.rest_client import RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.membase.api.rest_client import RestConnection
import logging
from pytests.security.ntonencryptionBase import ntonencryptionBase
from lib.Cb_constants.CBServer import CbServer
import os
import json
import time

from upgrade.newupgradebasetest import NewUpgradeBaseTest

log = logging.getLogger()


class EventingUpgrade(NewUpgradeBaseTest,EventingBaseTest):
    def setUp(self):
        log.info("==============  EventingUpgrade setup has started ==============")
        super(EventingUpgrade, self).setUp()
        self.rest = RestConnection(self.master)
        self.server = self.master
        self.queue = queue.Queue()
        self.src_bucket_name = self.input.param('src_bucket_name', 'src_bucket')
        self.eventing_log_level = self.input.param('eventing_log_level', 'INFO')
        self.dst_bucket_name = self.input.param('dst_bucket_name', 'dst_bucket')
        self.dst_bucket_name1 = self.input.param('dst_bucket_name1', 'dst_bucket1')
        self.dst_bucket_curl = self.input.param('dst_bucket_curl', 'dst_bucket_curl')
        self.source_bucket_mutation = self.input.param('source_bucket_mutation', 'source_bucket_mutation')
        self.metadata_bucket_name = self.input.param('metadata_bucket_name', 'metadata')
        self.n1ql_op_dst=self.input.param('n1ql_op_dst', 'n1ql_op_dst')
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.upgrade_version = self.input.param("upgrade_version")
        self.exported_handler_version = self.input.param("exported_handler_version", '6.6.1')
        self.enable_n2n_encryption_and_tls = self.input.param("enable_n2n_encryption_and_tls", False)
        log.info("==============  EventingUpgrade setup has completed ==============")

    def tearDown(self):
        log.info("==============  EventingUpgrade tearDown has started ==============")
        super(EventingUpgrade, self).tearDown()
        log.info("==============  EventingUpgrade tearDown has completed ==============")

    ### for this to work upgrade_version > 5.5
    def test_offline_upgrade_with_eventing(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init], services="kv,kv,eventing,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op function
        log.info("Deploy the function in the initial version")
        self.pre_upgrade_handlers()
        # offline upgrade all the nodes
        self.print_eventing_stats_from_all_eventing_nodes()
        upgrade_threads = self._async_update(self.upgrade_version, self.servers)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        self.sleep(120)
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Upgrade failed!")
        if self.enable_n2n_encryption_and_tls:
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
            CbServer.use_https = True
            self.wait_for_handler_state("timers", "deployed")
            self.pause_handler_by_name("timers")
            self.resume_handler_by_name("timers")
        self.wait_for_handler_state("bucket_op", "undeployed")
        self.wait_for_handler_state("timers", "deployed")
        self.deploy_handler_by_name("bucket_op")
        # Validate the data
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_collections_on_all_buckets()
        self.post_upgrade_handlers()
        self.add_built_in_server_user()
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Load the data in collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 2)
        ### delete data in all collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0",
                                     is_delete=True)
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0", is_delete=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Delete the data on SBM bucket
        self.load(self.gens_load, buckets=self.sbm, verify_data=False, op_type='delete')
        self.verify_count(0)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        ## pause handler
        self.pause_handler_by_name("bucket_op")
        self.pause_handler_by_name("timers")
        self.pause_handler_by_name("n1ql")
        self.pause_handler_by_name("curl")
        self.pause_handler_by_name("sbm")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket._default._default")
        # resume function
        self.resume_handler_by_name("bucket_op")
        self.resume_handler_by_name("timers")
        self.resume_handler_by_name("n1ql")
        self.resume_handler_by_name("curl")
        self.resume_handler_by_name("sbm")
        # Validate the data for both the functions
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 3)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.sleep(300)  ## wait for timers to fire
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_and_test_eventing_manage_functions_role()
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check = True

    def test_online_upgrade_with_regular_rebalance_with_eventing(self):
        self._install(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_version
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.operations(self.servers[:self.nodes_init], services="kv,kv,eventing,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op function
        log.info("Deploy the function in the initial version")
        self.pre_upgrade_handlers()
        self.print_eventing_stats_from_all_eventing_nodes()
        # swap and rebalance the servers
        self.online_upgrade(services=["kv","kv", "eventing", "index", "n1ql"])
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.add_built_in_server_user()
        if self.enable_n2n_encryption_and_tls:
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
            CbServer.use_https = True
            self.wait_for_handler_state("timers", "deployed")
            self.pause_handler_by_name("timers")
            self.resume_handler_by_name("timers")
        self.wait_for_handler_state("bucket_op", "undeployed")
        self.wait_for_handler_state("timers", "deployed")
        self.deploy_handler_by_name("bucket_op")
        # Validate the data
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_collections_on_all_buckets()
        self.post_upgrade_handlers()
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Load the data in collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 2)
        ### delete data in all collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0",
                                     is_delete=True)
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0", is_delete=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Delete the data on SBM bucket
        self.load(self.gens_load, buckets=self.sbm, verify_data=False, op_type='delete')
        self.verify_count(0)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        ## pause handler
        self.pause_handler_by_name("bucket_op")
        self.pause_handler_by_name("timers")
        self.pause_handler_by_name("n1ql")
        self.pause_handler_by_name("curl")
        self.pause_handler_by_name("sbm")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket._default._default")
        # resume function
        self.resume_handler_by_name("bucket_op")
        self.resume_handler_by_name("timers")
        self.resume_handler_by_name("n1ql")
        self.resume_handler_by_name("curl")
        self.resume_handler_by_name("sbm")
        # Validate the data for both the functions
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 3)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.sleep(300)  ## wait for timers to fire
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_and_test_eventing_manage_functions_role()
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check=True

    def test_online_upgrade_with_swap_rebalance_with_eventing(self):
        self._install(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_version
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.operations(self.servers[:self.nodes_init], services="kv,kv,eventing,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op function
        log.info("Deploy the function in the initial version")
        self.pre_upgrade_handlers()
        # offline upgrade all the nodes
        self.print_eventing_stats_from_all_eventing_nodes()
        # swap and rebalance the servers
        self.online_upgrade_swap_rebalance(services=["kv","kv", "eventing","index","n1ql"])
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.add_built_in_server_user()
        if self.enable_n2n_encryption_and_tls:
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
            CbServer.use_https = True
            self.wait_for_handler_state("timers", "deployed")
            self.pause_handler_by_name("timers")
            self.resume_handler_by_name("timers")
        self.wait_for_handler_state("bucket_op", "undeployed")
        self.wait_for_handler_state("timers", "deployed")
        self.deploy_handler_by_name("bucket_op")
        # Validate the data
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_collections_on_all_buckets()
        self.post_upgrade_handlers()
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Load the data in collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 2)
        ### delete data in all collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0",
                                     is_delete=True)
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0", is_delete=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Delete the data on SBM bucket
        self.load(self.gens_load, buckets=self.sbm, verify_data=False, op_type='delete')
        self.verify_count(0)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.sleep(300)  ## wait for timers to fire
        self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        ## pause handler
        self.pause_handler_by_name("bucket_op")
        self.pause_handler_by_name("timers")
        self.pause_handler_by_name("n1ql")
        self.pause_handler_by_name("curl")
        self.pause_handler_by_name("sbm")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket._default._default")
        # resume function
        self.resume_handler_by_name("bucket_op")
        self.resume_handler_by_name("timers")
        self.resume_handler_by_name("n1ql")
        self.resume_handler_by_name("curl")
        self.resume_handler_by_name("sbm")
        # Validate the data for both the functions
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 3)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.sleep(300)  ## wait for timers to fire
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_and_test_eventing_manage_functions_role()
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check = True

    def test_online_upgrade_with_failover_rebalance_with_eventing(self):
        self._install(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_version
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.operations(self.servers[:self.nodes_init], services="kv,kv,eventing,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op function
        log.info("Deploy the function in the initial version")
        self.pre_upgrade_handlers()
        # offline upgrade all the nodes
        self.print_eventing_stats_from_all_eventing_nodes()
        # online upgrade with failover
        self.online_upgrade_with_failover(services=["kv","kv", "eventing", "index", "n1ql"])
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.add_built_in_server_user()
        if self.enable_n2n_encryption_and_tls:
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
            CbServer.use_https = True
            self.wait_for_handler_state("timers", "deployed")
            self.pause_handler_by_name("timers")
            self.resume_handler_by_name("timers")
        self.wait_for_handler_state("bucket_op", "undeployed")
        self.wait_for_handler_state("timers", "deployed")
        self.deploy_handler_by_name("bucket_op")
        # Validate the data
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_collections_on_all_buckets()
        self.post_upgrade_handlers()
        self.add_built_in_server_user()
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Load the data in collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 2)
        ### delete data in all collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0",
                                     is_delete=True)
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0", is_delete=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Delete the data on SBM bucket
        self.load(self.gens_load, buckets=self.sbm, verify_data=False, op_type='delete')
        self.verify_count(0)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.sleep(300)
        self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        ## pause handler
        self.pause_handler_by_name("bucket_op")
        self.pause_handler_by_name("timers")
        self.pause_handler_by_name("n1ql")
        self.pause_handler_by_name("curl")
        self.pause_handler_by_name("sbm")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket._default._default")
        # resume function
        self.resume_handler_by_name("bucket_op")
        self.resume_handler_by_name("timers")
        self.resume_handler_by_name("n1ql")
        self.resume_handler_by_name("curl")
        self.resume_handler_by_name("sbm")
        # Validate the data for both the functions
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 3)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.sleep(300)  ## wait for timers to fire
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_and_test_eventing_manage_functions_role()
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check = True

    def test_cross_version_import_export_of_handlers(self):
        self.create_buckets()
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.n1ql_helper = N1QLHelper(shell=self.shell, max_verify=self.max_verify, buckets=self.buckets,
                                      item_flag=self.item_flag, n1ql_port=self.n1ql_port,
                                      full_docs_list=self.full_docs_list, log=self.log, input=self.input,
                                      master=self.master, use_rest=True)
        self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket._default._default")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket_sbm._default._default")
        # import handlers exported from older version
        self.import_function("exported_functions/" + self.exported_handler_version + "/bucket_op.json")
        self.import_function("exported_functions/" + self.exported_handler_version + "/curl.json")
        self.import_function("exported_functions/" + self.exported_handler_version + "/n1ql.json")
        self.import_function("exported_functions/" + self.exported_handler_version + "/sbm.json")
        self.import_function("exported_functions/" + self.exported_handler_version + "/timer.json")
        # deploy handlers
        self.deploy_handler_by_name("bucket_op")
        self.deploy_handler_by_name("timer")
        self.deploy_handler_by_name("n1ql")
        self.deploy_handler_by_name("curl")
        self.deploy_handler_by_name("sbm")
        # validate mutations for all handlers
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)
        self.validate_eventing(self.dst_bucket_name1, self.docs_per_day * 2016)
        self.validate_eventing(self.dst_bucket_curl, self.docs_per_day * 2016)
        self.validate_eventing(self.n1ql_op_dst, self.docs_per_day * 2016)
        self.validate_eventing(self.source_bucket_mutation, 2 * self.docs_per_day * 2016)
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket._default._default", is_delete=True)
        self.validate_eventing(self.dst_bucket_name, 0)
        self.validate_eventing(self.dst_bucket_name1, 0)
        self.validate_eventing(self.dst_bucket_curl, 0)
        self.validate_eventing(self.n1ql_op_dst, 0)
        self.pause_handler_by_name("bucket_op")
        self.pause_handler_by_name("timer")
        self.pause_handler_by_name("n1ql")
        self.pause_handler_by_name("curl")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket._default._default")
        self.resume_handler_by_name("bucket_op")
        self.resume_handler_by_name("timer")
        self.resume_handler_by_name("n1ql")
        self.resume_handler_by_name("curl")
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)
        self.validate_eventing(self.dst_bucket_name1, self.docs_per_day * 2016)
        self.validate_eventing(self.dst_bucket_curl, self.docs_per_day * 2016)
        self.validate_eventing(self.n1ql_op_dst, self.docs_per_day * 2016)
        self.undeploy_and_delete_function("bucket_op")
        self.undeploy_and_delete_function("timer")
        self.undeploy_and_delete_function("n1ql")
        self.undeploy_and_delete_function("curl")
        self.undeploy_and_delete_function("sbm")

    def test_offline_upgrade_with_eventing_pause_resume(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init], services="kv,kv,eventing,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy and pause bucket_op and timers handler
        log.info("Deploy and pause the functions in the initial version")
        self.pre_upgrade_handlers_pause_resume()
        # offline upgrade all the nodes
        self.print_eventing_stats_from_all_eventing_nodes()
        upgrade_threads = self._async_update(self.upgrade_version, self.servers)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        self.sleep(120)
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Upgrade failed!")
        if self.enable_n2n_encryption_and_tls:
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
            CbServer.use_https = True
        self.wait_for_handler_state("bucket_op", "paused")
        self.wait_for_handler_state("timers", "paused")
        self.resume_handler_by_name("bucket_op")
        self.resume_handler_by_name("timers")
        # Validate the data
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_collections_on_all_buckets()
        self.post_upgrade_handlers()
        self.add_built_in_server_user()
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Load the data in collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 2)
        ### delete data in all collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0",
                                     is_delete=True)
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0", is_delete=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Delete the data on SBM bucket
        self.load(self.gens_load, buckets=self.sbm, verify_data=False, op_type='delete')
        self.verify_count(0)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        ## pause handler
        self.pause_handler_by_name("bucket_op")
        self.pause_handler_by_name("timers")
        self.pause_handler_by_name("n1ql")
        self.pause_handler_by_name("curl")
        self.pause_handler_by_name("sbm")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket._default._default")
        # resume function
        self.resume_handler_by_name("bucket_op")
        self.resume_handler_by_name("timers")
        self.resume_handler_by_name("n1ql")
        self.resume_handler_by_name("curl")
        self.resume_handler_by_name("sbm")
        # Validate the data for both the functions
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 3)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.sleep(300)  ## wait for timers to fire
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_and_test_eventing_manage_functions_role()
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check = True

    def test_online_upgrade_with_regular_rebalance_with_eventing_pause_resume(self):
        self._install(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_version
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.operations(self.servers[:self.nodes_init], services="kv,kv,eventing,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op function
        log.info("Deploy the function in the initial version")
        self.pre_upgrade_handlers_pause_resume()
        self.print_eventing_stats_from_all_eventing_nodes()
        # swap and rebalance the servers
        self.online_upgrade(services=["kv","kv", "eventing", "index", "n1ql"])
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.add_built_in_server_user()
        if self.enable_n2n_encryption_and_tls:
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
            CbServer.use_https = True
        self.wait_for_handler_state("bucket_op", "paused")
        self.wait_for_handler_state("timers", "paused")
        self.resume_handler_by_name("bucket_op")
        self.resume_handler_by_name("timers")
        # Validate the data
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_collections_on_all_buckets()
        self.post_upgrade_handlers()
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Load the data in collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 2)
        ### delete data in all collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0",
                                     is_delete=True)
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0", is_delete=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Delete the data on SBM bucket
        self.load(self.gens_load, buckets=self.sbm, verify_data=False, op_type='delete')
        self.verify_count(0)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        ## pause handler
        self.pause_handler_by_name("bucket_op")
        self.pause_handler_by_name("timers")
        self.pause_handler_by_name("n1ql")
        self.pause_handler_by_name("curl")
        self.pause_handler_by_name("sbm")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket._default._default")
        # resume function
        self.resume_handler_by_name("bucket_op")
        self.resume_handler_by_name("timers")
        self.resume_handler_by_name("n1ql")
        self.resume_handler_by_name("curl")
        self.resume_handler_by_name("sbm")
        # Validate the data for both the functions
        self.verify_count(self.docs_per_day * 2016)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", self.docs_per_day * 2016 * 3)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.sleep(300)  ## wait for timers to fire
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)
        self.create_and_test_eventing_manage_functions_role()
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check=True

    def import_function(self, function):
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, function)
        fh = open(abs_file_path, "r")
        body = fh.read()
        # import the previously exported function
        self.rest.import_function(body)

    def online_upgrade(self, services=None):
        servers_in = self.servers[self.nodes_init:self.num_servers]
        servers_out = self.servers[:self.nodes_init]
        self.cluster.rebalance(self.servers[:self.nodes_init], servers_in, servers_out, services=services)
        self.sleep(300)
        self.log.info("Rebalance in all {0} nodes" \
                      .format(self.input.param("upgrade_version", "")))
        self.log.info("Rebalanced out all old version nodes")

        status, content = ClusterOperationHelper.find_orchestrator(servers_in[0])
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}". \
                        format(status, content))
        FIND_MASTER = False
        for new_server in servers_in:
            if content.find(new_server.ip) >= 0:
                self._new_master(new_server)
                FIND_MASTER = True
                self.log.info("%s node %s becomes the master" \
                              % (self.input.param("upgrade_version", ""), new_server.ip))
                self.master=new_server
                break

    def online_upgrade_swap_rebalance(self, services=None):
        servers_in = self.servers[self.nodes_init:self.num_servers]
        self.sleep(self.sleep_time)
        status, content = ClusterOperationHelper.find_orchestrator(self.master)
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}". \
                        format(status, content))
        i = 1
        for server_in, service_in in zip(servers_in[1:], services[1:]):
            log.info(
                "Swap rebalance nodes : server_in: {0} service_in:{1} service_out:{2}".format(server_in, service_in,
                                                                                              self.servers[i]))
            self.cluster.rebalance(self.servers[:self.nodes_init], [server_in], [self.servers[i]],
                                   services=[service_in])
            i += 1
        self._new_master(self.servers[self.nodes_init + 1])
        self.cluster.rebalance(self.servers[self.nodes_init + 1:self.num_servers], [servers_in[0]], [self.servers[0]],
                               services=[services[0]])

    def online_upgrade_with_failover(self, services=None):
        servers_in = self.servers[self.nodes_init:self.num_servers]
        self.cluster.rebalance(self.servers[:self.nodes_init], servers_in, [], services=services)
        log.info("Rebalance in all {0} nodes" \
                 .format(self.input.param("upgrade_version", "")))
        self.sleep(self.sleep_time)
        status, content = ClusterOperationHelper.find_orchestrator(self.master)
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}". \
                        format(status, content))
        FIND_MASTER = False
        for new_server in servers_in:
            if content.find(new_server.ip) >= 0:
                self._new_master(new_server)
                FIND_MASTER = True
                self.log.info("%s node %s becomes the master" \
                              % (self.input.param("upgrade_version", ""), new_server.ip))
                break
        servers_out = self.servers[:self.nodes_init]
        self._new_master(self.servers[self.nodes_init])
        log.info("failover and rebalance nodes")
        self.cluster.failover(self.servers[:self.num_servers], failover_nodes=servers_out, graceful=False)
        self.cluster.rebalance(self.servers[:self.num_servers], [], servers_out)
        self.sleep(180)

    def _new_master(self, server):
        self.master = server
        self.rest = RestConnection(self.master)
        self.rest_helper = RestHelper(self.rest)

    def create_buckets(self):
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=1000)
        self.rest.delete_bucket("default")
        self.bucket_size = 256
        log.info("Create the required buckets in the initial version")
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas)
        self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                            bucket_params=bucket_params)
        self.src_bucket = RestConnection(self.master).get_buckets()
        self.sleep(60)
        self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 2,
                                            bucket_params=bucket_params)
        self.sleep(60)
        self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 3,
                                            bucket_params=bucket_params)
        self.sleep(60)
        self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 4,
                                            bucket_params=bucket_params)
        self.sleep(60)
        self.cluster.create_standard_bucket(name=self.dst_bucket_curl, port=STANDARD_BUCKET_PORT + 5,
                                            bucket_params=bucket_params)
        self.sleep(60)
        self.cluster.create_standard_bucket(name=self.source_bucket_mutation, port=STANDARD_BUCKET_PORT + 6,
                                            bucket_params=bucket_params)
        self.sleep(60)
        self.cluster.create_standard_bucket(name=self.n1ql_op_dst, port=STANDARD_BUCKET_PORT + 7,
                                            bucket_params=bucket_params)
        self.buckets = RestConnection(self.master).get_buckets()
        self.sbm = RestConnection(self.master).get_bucket_by_name(self.source_bucket_mutation)

    def validate_eventing(self, bucket_name, no_of_docs):
        count = 0
        stats_dst = self.rest.get_bucket_stats(bucket_name)
        while stats_dst["curr_items"] != no_of_docs and count < 20:
            message = "Waiting for handler code to complete bucket operations... Current : {0} Expected : {1}". \
                format(stats_dst["curr_items"], no_of_docs)
            self.sleep(30, message=message)
            count += 1
            stats_dst = self.rest.get_bucket_stats(bucket_name)
        if stats_dst["curr_items"] != no_of_docs:
            log.info("Eventing is not working as expected after upgrade")
            raise Exception(
                "Bucket operations from handler code took lot of time to complete or didn't go through. Current : {0} "
                "Expected : {1} ".format(stats_dst["curr_items"], no_of_docs))

    def deploy_function(self, body, deployment_fail=False, wait_for_bootstrap=True):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        content1 = self.rest.create_function(body['appname'], body)
        log.info("deploy Application : {0}".format(content1))
        if deployment_fail:
            res = json.loads(content1)
            if not res["compile_success"]:
                return
            else:
                raise Exception("Deployment is expected to be failed but no message of failure")
        if wait_for_bootstrap:
            # wait for the function to come out of bootstrap state
            self.wait_for_handler_state(body['appname'], "deployed")

    def undeploy_and_delete_function(self, function):
        self.undeploy_function_by_name(function)
        content1 = self.rest.delete_single_function(function)

    def pause_function(self, function):
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, function)
        fh = open(abs_file_path, "r")
        body = json.loads(fh.read())
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = False
        self.refresh_rest_server()
        # save the function so that it is visible in UI
        #content = self.rest.save_function(body['appname'], body)
        # undeploy the function
        content1 = self.rest.set_settings_for_function(body['appname'], body['settings'])
        log.info("Pause Application : {0}".format(body['appname']))
        self.wait_for_handler_state(body['appname'], "paused")

    def resume_function(self,function):
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, function)
        fh = open(abs_file_path, "r")
        body = json.loads(fh.read())
        ### resume function
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        if "dcp_stream_boundary" in body['settings']:
            body['settings'].pop('dcp_stream_boundary')
        log.info("Settings after deleting dcp_stream_boundary : {0}".format(body['settings']))
        self.rest.set_settings_for_function(body['appname'], body['settings'])
        log.info("Resume Application : {0}".format(body['appname']))
        self.wait_for_handler_state(body['appname'], "deployed")

    def wait_for_handler_state(self, name,status,iterations=20):
        self.refresh_rest_server()
        self.sleep(20, message="Waiting for {} to {}...".format(name,status))
        result = self.rest.get_composite_eventing_status()
        count = 0
        composite_status = None
        while composite_status != status and count < iterations:
            self.sleep(20,"Waiting for {} to {}...".format(name,status))
            result = self.rest.get_composite_eventing_status()
            for i in range(len(result['apps'])):
                if result['apps'][i]['name'] == name:
                    composite_status = result['apps'][i]['composite_status']
            count+=1
        if count == iterations:
            raise Exception('Eventing took lot of time for handler {} to {}'.format(name,status))

    def _create_primary_index(self):
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        n1ql_helper = N1QLHelper(shell=self.shell, max_verify=self.max_verify, buckets=self.buckets,
                                      item_flag=self.item_flag, n1ql_port=self.n1ql_port,
                                      full_docs_list=self.full_docs_list, log=self.log, input=self.input,
                                      master=self.master, use_rest=True)
        # primary index is required as we run some queries from handler code
        n1ql_helper.create_primary_index(using_gsi=True, server=n1ql_node)

    def pre_upgrade_handlers(self):
        self.create_handler("bucket_op", "handler_code/delete_doc_bucket_op.js")
        self.create_handler("timers", "handler_code/bucket_op_with_timers_upgrade.js",bucket_bindings=["dst_bucket.dst_bucket1.rw"])
        self.deploy_handler_by_name("timers")

    def post_upgrade_handlers(self):
        self.create_function_with_collection("sbm","handler_code/ABO/insert_sbm.js",src_namespace="source_bucket_mutation.event.coll_0",
                                             collection_bindings=["src_bucket.source_bucket_mutation.event.coll_0.rw"])
        self.hostname="http://qa.sc.couchbase.com/"
        self.create_function_with_collection("curl","handler_code/ABO/curl_get.js",src_namespace="src_bucket.event.coll_0",
                                             collection_bindings=["dst_bucket.dst_bucket_curl.event.coll_0.rw"],is_curl=True)
        self.create_function_with_collection("n1ql", "handler_code/collections/n1ql_insert_update.js",
                                             src_namespace="src_bucket.event.coll_0")
        self.deploy_handler_by_name("sbm")
        self.deploy_handler_by_name("curl")
        self.deploy_handler_by_name("n1ql")

    def create_collections_on_all_buckets(self):
        buckets=RestConnection(self.master).get_buckets()
        for bucket in buckets:
            self.create_scope_collection(bucket.name,"event","coll_0")

    def create_handler(self,appname,appcode,meta_bucket="metadata",src_bucket="src_bucket",
                       bucket_bindings=["dst_bucket.dst_bucket.rw"],dcp_stream_boundary="everything"):
        body = {}
        body['appname'] = appname
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, appcode)
        fh = open(abs_file_path, "r")
        body['appcode'] = fh.read()
        fh.close()
        body['depcfg'] = {}
        body['depcfg']['metadata_bucket'] = meta_bucket
        body['depcfg']['source_bucket'] = src_bucket
        body['settings'] = {}
        body['settings']['dcp_stream_boundary'] = dcp_stream_boundary
        body['settings']['deployment_status'] = False
        body['settings']['processing_status'] = False
        body['settings']['log_level'] = self.eventing_log_level
        body['depcfg']['curl'] = []
        body['depcfg']['buckets'] = []
        body['function_scope'] = {"bucket": "*",
                                  "scope": "*"}
        for binding in bucket_bindings:
            bind_map=binding.split(".")
            if  len(bind_map)< 3:
                raise Exception("Binding {} doesn't have all the fields".format(binding))
            body['depcfg']['buckets'].append(
                {"alias": bind_map[0], "bucket_name": bind_map[1], "access": bind_map[2]})
        self.rest.create_function(body['appname'], body)
        self.log.info("saving function {}".format(body['appname']))
        return body

    def verify_count(self,count):
        self.verify_doc_count_collections("dst_bucket_curl.event.coll_0",count)
        self.verify_doc_count_collections("n1ql_op_dst.event.coll_0", count)

    def pre_upgrade_handlers_pause_resume(self):
        self.create_handler("bucket_op", "handler_code/delete_doc_bucket_op.js")
        self.create_handler("timers", "handler_code/bucket_op_with_timers_upgrade.js",bucket_bindings=["dst_bucket.dst_bucket1.rw"])
        self.deploy_handler_by_name("bucket_op")
        self.deploy_handler_by_name("timers")
        self.pause_handler_by_name("bucket_op")
        self.pause_handler_by_name("timers")

    def create_and_test_eventing_manage_functions_role(self):
        payload = "name=" + "john" + "&roles=" + '''data_reader[metadata],data_writer[metadata],data_writer[dst_bucket],
                                  data_dcp_reader[src_bucket],eventing_manage_functions[src_bucket:_default]''' + "&password=" + "asdasd"
        self.rest.add_set_builtin_user(user_id="john", payload=payload)
        self.create_handler("test", "handler_code/no_op.js")
        self.deploy_handler_by_name("test")
        self.pause_handler_by_name("test")
        self.resume_handler_by_name("test")
        self.undeploy_and_delete_function("test")
        
    def test_online_upgrade_with_failover_rebalance_with_eventing_base64_xattrs(self):
        self._install(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_version
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.operations(self.servers[:self.nodes_init], services="kv,kv,eventing,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op function
        log.info("Deploy the function in the initial version")
        self.create_handler("pre_upgrade_function", "handler_code/ABO/insert.js",bucket_bindings=["dst_bucket.dst_bucket.rw"])
        self.deploy_handler_by_name("pre_upgrade_function")
        self.print_eventing_stats_from_all_eventing_nodes()
        # online upgrade with failover
        self.online_upgrade_swap_rebalance(services=["kv", "kv", "eventing", "index", "n1ql"])
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.add_built_in_server_user()
        self.wait_for_handler_state("pre_upgrade_function", "deployed")
        # Validate the data
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        # TODO
        self.create_collections_on_all_buckets()
        time.sleep(60)
        self.create_function_with_collection("xattrs_2", "handler_code/xattrs_handler1.js",src_namespace="src_bucket.event.coll_0",collection_bindings=["dst_bucket.dst_bucket.event.coll_0.rw"])
        self.create_function_with_collection("base64_2", "handler_code/base64_post_upgrade.js",src_namespace="src_bucket.event.coll_0",collection_bindings=["dst_bucket.dst_bucket1.event.coll_0.rw"])
        self.deploy_handler_by_name("xattrs_2")
        self.deploy_handler_by_name("base64_2")
        self.add_built_in_server_user()
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Load the data in collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.verify_doc_count_collections("dst_bucket.event.coll_0", self.docs_per_day * 2016 * 2)
        self.verify_doc_count_collections("dst_bucket1.event.coll_0", self.docs_per_day * 2016)
        # delete data in all collections
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0",is_delete=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,batch_size=self.batch_size, op_type='delete')
        # Delete the data on SBM bucket
        self.verify_doc_count_collections("src_bucket.event.coll_0", 0)
        self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.verify_doc_count_collections("dst_bucket.event.coll_0", 0)
        self.verify_doc_count_collections("dst_bucket1.event.coll_0", 0)
        ## pause handler
        self.pause_handler_by_name("pre_upgrade_function")
        self.pause_handler_by_name("xattrs_2")
        self.pause_handler_by_name("base64_2")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket.event.coll_0")
        self.load_data_to_collection(self.docs_per_day * 2016, namespace="src_bucket._default._default")
        # resume function
        self.resume_handler_by_name("pre_upgrade_function")
        self.resume_handler_by_name("xattrs_2")
        self.resume_handler_by_name("base64_2")
        # Validate the data for both the functions
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016 )
        self.verify_doc_count_collections("dst_bucket.event.coll_0", self.docs_per_day * 2016 * 2)
        self.verify_doc_count_collections("dst_bucket1.event.coll_0", self.docs_per_day * 2016)
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check = True
