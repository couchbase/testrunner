import Queue
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
import os
import json
log = logging.getLogger()


class EventingUpgrade(NewUpgradeBaseTest, BaseTestCase):
    def setUp(self):
        super(EventingUpgrade, self).setUp()
        self.rest = RestConnection(self.master)
        self.server = self.master
        self.queue = Queue.Queue()
        self.src_bucket_name = self.input.param('src_bucket_name', 'src_bucket')
        self.eventing_log_level = self.input.param('eventing_log_level', 'INFO')
        self.dst_bucket_name = self.input.param('dst_bucket_name', 'dst_bucket')
        self.metadata_bucket_name = self.input.param('metadata_bucket_name', 'metadata')
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.upgrade_version = self.input.param("upgrade_version")

    def tearDown(self):
        super(EventingUpgrade, self).tearDown()

    def test_offline_upgrade_with_eventing(self):
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init], services="kv,kv,index,n1ql")
        self.rest.delete_bucket("default")
        self.bucket_size = 100
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
        self.buckets = RestConnection(self.master).get_buckets()
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
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, EXPORTED_FUNCTION.BUCKET_OP)
        fh = open(abs_file_path, "r")
        body = json.loads(fh.read())
        # import the previously exported function
        log.info("Deploying a function after upgrade")
        self.rest.save_function("test_import_function_1", body)
        self.rest.deploy_function("test_import_function_1", body)
        self.sleep(180)
        # Do validations
        count = 0
        expected_dcp_mutations = self.docs_per_day * 2016
        stats_dst = self.rest.get_bucket_stats(self.dst_bucket_name)
        while stats_dst["curr_items"] != expected_dcp_mutations and count < 20:
            self.sleep(30, message="Waiting for handler code to complete all bucket operations...")
            count += 1
            stats_dst = self.rest.get_bucket_stats(self.dst_bucket_name)
        if stats_dst["curr_items"] != expected_dcp_mutations:
            log.info("Eventing is not working as expected after upgrade")
            raise Exception(
                "Bucket operations from handler code took lot of time to complete or didn't go through. Current : {0} "
                "Expected : {1} ".format(stats_dst["curr_items"], expected_dcp_mutations))