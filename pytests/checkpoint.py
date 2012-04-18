import unittest
import time
import uuid

from TestInput import TestInputSingleton
from tasks.taskmanager import TaskManager
from tasks.taskscheduler import TaskScheduler
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper


class CheckpointTests(unittest.TestCase):

    def setUp(self):
        self.task_manager = TaskManager()
        self.task_manager.start()

        self.servers = TestInputSingleton.input.servers
        self.master = self.servers[0]
        self.quota = TaskScheduler.init_node(self.task_manager, self.master)
        self.old_vbuckets = self._get_vbuckets()

        # Start: Should be in a before class function
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        ClusterOperationHelper.wait_for_ns_servers_or_assert([self.master], self)
        # End: Should be in a before class function

        TaskScheduler.rebalance(self.task_manager, self.servers, self.servers[1:], [])
        ClusterOperationHelper.set_vbuckets(self.master, 1)

    def tearDown(self):
        ClusterOperationHelper.set_vbuckets(self.master, self.old_vbuckets)

        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        ClusterOperationHelper.wait_for_ns_servers_or_assert([self.master], self)

        self.task_manager.stop()

    def test_temporary(self):
        pass

    def _get_vbuckets(self):
        rest = RestConnection(self.master)
        command = "ns_config:search(couchbase_num_vbuckets_default)"
        status, content = rest.diag_eval(command)

        try:
            vbuckets = int(re.sub('[^\d]', '', content))
        except:
            vbuckets = 1024
        return vbuckets
