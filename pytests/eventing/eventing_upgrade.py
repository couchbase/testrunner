import queue
import copy
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, RestHelper
from membase.api.exception import RebalanceFailedException
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.kvstore import KVStore
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.basetestcase import BaseTestCase
from lib.couchbase_helper.stats_tools import StatsCommon
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.couchbase_helper.documentgenerator import JSONNonDocGenerator, BlobGenerator
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
from lib.membase.api.rest_client import RestConnection
import logging
from pytests.eventing.eventing_constants import HANDLER_CODE, EXPORTED_FUNCTION
from testconstants import COUCHBASE_VERSION_2
import os
import json

log = logging.getLogger()


class EventingUpgrade(NewUpgradeBaseTest, BaseTestCase):
    def setUp(self):
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
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.upgrade_version = self.input.param("upgrade_version")

    def tearDown(self):
        super(EventingUpgrade, self).tearDown()

    def test_offline_upgrade_with_eventing_pre_vulcan(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init], services="kv,kv,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        # upgrade all the nodes
        upgrade_threads = self._async_update(self.upgrade_version, self.servers)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        self.sleep(120)
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Upgrade failed!")
        self.add_built_in_server_user()
        # Add eventing node to the cluster after upgrade
        self.cluster.rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                               services=["eventing"])
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.import_function(EXPORTED_FUNCTION.BUCKET_OP)
        self.sleep(180)
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)

    def test_offline_upgrade_with_eventing(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init], services="kv,eventing,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op function
        log.info("Deploy the function in the initial version")
        self.import_function(EXPORTED_FUNCTION.BUCKET_OP)
        # Validate the data
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)
        # offline upgrade all the nodes
        upgrade_threads = self._async_update(self.upgrade_version, self.servers)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        self.sleep(120)
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Upgrade failed!")
        self.add_built_in_server_user()
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op with timer function
        self.import_function(EXPORTED_FUNCTION.BUCKET_OP_WITH_TIMER)
        # Validate the data
        self.validate_eventing(self.dst_bucket_name1, self.docs_per_day * 2016)
        #Deploy the Source bucket handler
        self.import_function(EXPORTED_FUNCTION.SBM_BUCKET_OP)
        # Validate the data
        self.validate_eventing(self.source_bucket_mutation, 2*self.docs_per_day * 2016)
        # Deploy the Source bucket handler
        self.import_function(EXPORTED_FUNCTION.CURL_BUCKET_OP)
        # Validate the data
        self.validate_eventing(self.dst_bucket_curl, self.docs_per_day * 2016)
        # Delete the data on source bucket
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False, op_type='delete')
        # Validate the data for both the functions
        self.validate_eventing(self.dst_bucket_name, 0)
        self.validate_eventing(self.dst_bucket_name1, 0)
        self.validate_eventing(self.source_bucket_mutation, 0)
        self.validate_eventing(self.dst_bucket_curl, 0)
        # add data to source bucket
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        # Validate the data for both the functions
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)
        self.validate_eventing(self.dst_bucket_name1, self.docs_per_day * 2016)
        # Undeploy and delete both the functions
        self.undeploy_and_delete_function("test_import_function_1")
        self.undeploy_and_delete_function("test_import_function_2")
        self.undeploy_function('bucket_op_sbm')
        self.undeploy_function('bucket_op_curl')

    def test_online_upgrade_with_regular_rebalance_with_eventing(self):
        self._install(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_version
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.operations(self.servers[:self.nodes_init], services="kv,eventing,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        log.info("Load the data in older version in the initial version")
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op function
        log.info("Deploy the function in the initial version")
        self.import_function(EXPORTED_FUNCTION.BUCKET_OP)
        # Do validations
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)
        # swap and rebalance the servers
        self.online_upgrade(services=["kv", "eventing", "index", "n1ql"])
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.add_built_in_server_user()
        # Deploy the bucket op with timer function
        self.import_function(EXPORTED_FUNCTION.BUCKET_OP_WITH_TIMER)
        # Do validations
        self.validate_eventing(self.dst_bucket_name1, self.docs_per_day * 2016)
        # Delete the data on source bucket
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False, op_type='delete')
        # Validate the data for both the functions
        self.validate_eventing(self.dst_bucket_name, 0)
        self.validate_eventing(self.dst_bucket_name1, 0)
        # add data to source bucket
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        # Validate the data for both the functions
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)
        self.validate_eventing(self.dst_bucket_name1, self.docs_per_day * 2016)
        # Undeploy and delete both the functions
        self.undeploy_and_delete_function("test_import_function_1")
        self.undeploy_and_delete_function("test_import_function_2")

    def test_online_upgrade_with_swap_rebalance_with_eventing(self):
        self._install(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_version
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.operations(self.servers[:self.nodes_init], services="kv,kv,eventing,eventing")
        self.create_buckets()
        # Load the data in older version
        log.info("Load the data in older version in the initial version")
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op function
        log.info("Deploy the function in the initial version")
        self.import_function(EXPORTED_FUNCTION.BUCKET_OP)
        # Do validations
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)
        # swap and rebalance the servers
        self.online_upgrade_swap_rebalance(services=["kv", "kv", "eventing", "eventing"])
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.add_built_in_server_user()
        # Deploy the bucket op with timer function
        self.import_function(EXPORTED_FUNCTION.BUCKET_OP_WITH_TIMER)
        # Do validations
        self.validate_eventing(self.dst_bucket_name1, self.docs_per_day * 2016)
        # Delete the data on source bucket
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False, op_type='delete')
        # Validate the data for both the functions
        self.validate_eventing(self.dst_bucket_name, 0)
        self.validate_eventing(self.dst_bucket_name1, 0)
        # add data to source bucket
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        # Validate the data for both the functions
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)
        self.validate_eventing(self.dst_bucket_name1, self.docs_per_day * 2016)
        # Undeploy and delete both the functions
        self.undeploy_and_delete_function("test_import_function_1")
        self.undeploy_and_delete_function("test_import_function_2")

    def test_online_upgrade_with_failover_rebalance_with_eventing(self):
        self._install(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_version
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.operations(self.servers[:self.nodes_init], services="kv,eventing,index,n1ql")
        self.create_buckets()
        # Load the data in older version
        log.info("Load the data in older version in the initial version")
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Deploy the bucket op function
        log.info("Deploy the function in the initial version")
        self.import_function(EXPORTED_FUNCTION.BUCKET_OP)
        # Do validations
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)
        # swap and rebalance the servers
        self.online_upgrade_with_failover(services=["kv", "eventing", "index", "n1ql"])
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.add_built_in_server_user()
        # Deploy the bucket op with timer function
        self.import_function(EXPORTED_FUNCTION.BUCKET_OP_WITH_TIMER)
        # Do validations
        self.validate_eventing(self.dst_bucket_name1, self.docs_per_day * 2016)
        # Delete the data on source bucket
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False, op_type='delete')
        # Validate the data for both the functions
        self.validate_eventing(self.dst_bucket_name, 0)
        self.validate_eventing(self.dst_bucket_name1, 0)
        # add data to source bucket
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)
        # Validate the data for both the functions
        self.validate_eventing(self.dst_bucket_name, self.docs_per_day * 2016)
        self.validate_eventing(self.dst_bucket_name1, self.docs_per_day * 2016)
        # Undeploy and delete both the functions
        self.undeploy_and_delete_function("test_import_function_1")
        self.undeploy_and_delete_function("test_import_function_2")

    def import_function(self, function):
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, function)
        fh = open(abs_file_path, "r")
        body = json.loads(fh.read())
        # import the previously exported function
        self.rest.save_function(body["appname"], body)
        self.rest.deploy_function(body["appname"], body)
        self.sleep(180)

    def online_upgrade(self, services=None):
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
        if self.input.param("initial_version", "")[:5] in COUCHBASE_VERSION_2 \
                and not FIND_MASTER:
            raise Exception( \
                "After rebalance in {0} nodes, {0} node doesn't become master" \
                    .format(self.input.param("upgrade_version", "")))
        servers_out = self.servers[:self.nodes_init]
        log.info("Rebalanced out all old version nodes")
        self.cluster.rebalance(self.servers[:self.num_servers], [], servers_out)
        self._new_master(self.servers[self.nodes_init])

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
        if self.input.param("initial_version", "")[:5] in COUCHBASE_VERSION_2 \
                and not FIND_MASTER:
            raise Exception( \
                "After rebalance in {0} nodes, {0} node doesn't become master" \
                    .format(self.input.param("upgrade_version", "")))
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
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=700)
        self.rest.delete_bucket("default")
        self.bucket_size = 100
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
        self.cluster.create_standard_bucket(name=self.dst_bucket_curl, port=STANDARD_BUCKET_PORT + 4,
                                            bucket_params=bucket_params)
        self.sleep(60)
        self.cluster.create_standard_bucket(name=self.source_bucket_mutation, port=STANDARD_BUCKET_PORT + 4,
                                            bucket_params=bucket_params)
        self.buckets = RestConnection(self.master).get_buckets()

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

    def undeploy_and_delete_function(self, function):
        log.info("Undeploying function : {0}".format(function))
        content = self.rest.undeploy_function(function)
        self.sleep(180)
        log.info("Deleting function : {0}".format(function))
        content1 = self.rest.delete_single_function(function)