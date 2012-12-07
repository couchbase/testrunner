import logger
import unittest
import copy
import datetime
import time

from couchbase.cluster import Cluster
from couchbase.document import View
from couchbase.documentgenerator import DocumentGenerator
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection, Bucket
from memcached.helper.kvstore import KVStore
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
try:
    import logging
    logging.config.fileConfig("scripts.logging.conf")
    self.log = logging.getLogger()
except:
     pass


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.buckets = []
        self.master = self.servers[0]
        self.cluster = Cluster()
        try:
            self.wait_timeout = self.input.param("wait_timeout", 60)
            #number of case that is performed from testrunner( increment each time)
            self.case_number = self.input.param("case_number", 0)
            self.default_bucket = self.input.param("default_bucket", True)
            if self.default_bucket:
                self.default_bucket_name = "default"
            self.standard_buckets = self.input.param("standard_buckets", 0)
            self.sasl_buckets = self.input.param("sasl_buckets", 0)
            self.total_buckets = self.sasl_buckets + self.default_bucket + self.standard_buckets
            self.num_servers = self.input.param("servers", len(self.servers))
            #initial number of items in the cluster
            self.nodes_init = self.input.param("nodes_init", 1)

            self.num_replicas = self.input.param("replicas", 1)
            self.num_items = self.input.param("items", 1000)
            self.dgm_run = self.input.param("dgm_run", False)
            #max items number to verify in ValidateDataTask, None - verify all
            self.max_verify = self.input.param("max_verify", None)
            #we don't change consistent_view on server by default
            self.disabled_consistent_view = self.input.param("disabled_consistent_view", None)
            self.rebalanceIndexWaitingDisabled = self.input.param("rebalanceIndexWaitingDisabled", None)
            self.rebalanceIndexPausingDisabled = self.input.param("rebalanceIndexPausingDisabled", None)
            self.maxParallelIndexers = self.input.param("maxParallelIndexers", None)
            self.maxParallelReplicaIndexers = self.input.param("maxParallelReplicaIndexers", None)
            self.log.info("==============  basetestcase setup was started for test #{0} {1}=============="\
                          .format(self.case_number, self._testMethodName))
            #avoid clean up if the previous test has been tear down
            if not self.input.param("skip_cleanup", True) or self.case_number == 1:
                self.tearDown()
                self.cluster = Cluster()
            if str(self.__class__).find('rebalanceout.RebalanceOutTests') != -1:
                #rebalance all nodes into the cluster before each test
                self.cluster.rebalance(self.servers[:self.num_servers], self.servers[1:self.num_servers], [])
            elif self.nodes_init > 1:
                self.cluster.rebalance(self.servers[:1], self.servers[1:self.nodes_init], [])
            self.quota = self._initialize_nodes(self.cluster, self.servers, self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled, self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers, self.maxParallelReplicaIndexers)
            if self.dgm_run:
                self.quota = 256
            if self.total_buckets > 0:
                self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)

            if self.default_bucket:
                self.cluster.create_default_bucket(self.master, self.bucket_size, self.num_replicas)
                self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                           num_replicas=self.num_replicas, bucket_size=self.bucket_size))

            self._create_sasl_buckets(self.master, self.sasl_buckets)
            self._create_standard_buckets(self.master, self.standard_buckets)
            self.log.info("==============  basetestcase setup was finished for test #{0} {1} =============="\
                          .format(self.case_number, self._testMethodName))
            self._log_start(self)
        except Exception, e:
            self.cluster.shutdown()
            self.fail(e)

    def tearDown(self):
            try:
                if (hasattr(self, '_resultForDoCleanups') and len(self._resultForDoCleanups.failures) > 0 \
                    and TestInputSingleton.input.param("stop-on-failure", False))\
                        or self.input.param("skip_cleanup", False):
                    self.log.warn("CLEANUP WAS SKIPPED")
                else:
                    self.log.info("==============  basetestcase cleanup was started for test #{0} {1} =============="\
                          .format(self.case_number, self._testMethodName))
                    rest = RestConnection(self.master)
                    alerts = rest.get_alerts()
                    if alerts is not None and len(alerts) != 0:
                        self.log.warn("Alerts were found: {0}".format(alerts))
                    if rest._rebalance_progress_status() == 'running':
                        self.log.warning("rebalancing is still running, test should be verified")
                        stopped = rest.stop_rebalance()
                        self.assertTrue(stopped, msg="unable to stop rebalance")
                    BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
                    ClusterOperationHelper.cleanup_cluster(self.servers)
                    time.sleep(10)
                    ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
                    self.log.info("==============  basetestcase cleanup was finished for test #{0} {1} =============="\
                          .format(self.case_number, self._testMethodName))
            finally:
                #stop all existing task manager threads
                self.cluster.shutdown()
                self._log_finish(self)

    @staticmethod
    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    @staticmethod
    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    def _initialize_nodes(self, cluster, servers, disabled_consistent_view=None, rebalanceIndexWaitingDisabled=None,
                          rebalanceIndexPausingDisabled=None, maxParallelIndexers=None, maxParallelReplicaIndexers=None):
        quota = 0
        init_tasks = []
        for server in servers:
            init_tasks.append(cluster.async_init_node(server, disabled_consistent_view, rebalanceIndexWaitingDisabled,
                          rebalanceIndexPausingDisabled, maxParallelIndexers, maxParallelReplicaIndexers))
        for task in init_tasks:
            node_quota = task.result()
            if node_quota < quota or quota == 0:
                quota = node_quota
        return quota

    def _get_bucket_size(self, quota, num_buckets, ratio=2.0 / 3.0):
        ip = self.servers[0]
        for server in self.servers:
            if server.ip == ip:
                return int(ratio / float(self.num_servers) / float(num_buckets) * float(quota))
        return int(ratio / float(num_buckets) * float(quota))

    def _create_sasl_buckets(self, server, num_buckets):
        bucket_tasks = []
        for i in range(num_buckets):
            name = 'bucket' + str(i)
            bucket_tasks.append(self.cluster.async_create_sasl_bucket(server, name,
                                                                      'password',
                                                                      self.bucket_size,
                                                                      self.num_replicas))
            self.buckets.append(Bucket(name=name, authType="sasl", saslPassword='password',
                                       num_replicas=self.num_replicas, bucket_size=self.bucket_size));
        for task in bucket_tasks:
            task.result()

    def _create_standard_buckets(self, server, num_buckets):
        bucket_tasks = []
        for i in range(num_buckets):
            name = 'standard_bucket' + str(i)
            bucket_tasks.append(self.cluster.async_create_standard_bucket(server, name,
                                                                          11214 + i,
                                                                          self.bucket_size,
                                                                          self.num_replicas))

            self.buckets.append(Bucket(name=name, authType=None, saslPassword=None, num_replicas=self.num_replicas,
                                       bucket_size=self.bucket_size, port=11214 + i));
        for task in bucket_tasks:
            task.result()

    def _all_buckets_delete(self, server):
        delete_tasks = []
        for bucket in self.buckets:
            delete_tasks.append(self.cluster.async_bucket_delete(server, bucket.name))

        for task in delete_tasks:
            task.result()
        self.buckets = []

    def _verify_stats_all_buckets(self, servers):
        stats_tasks = []
        for bucket in self.buckets:
            items = sum([len(kv_store) for kv_store in bucket.kvs.values()])
            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                               'curr_items', '==', items))
            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                               'vb_active_curr_items', '==', items))

            available_replicas = self.num_replicas
            if len(servers) == self.num_replicas:
                available_replicas = len(servers) - 1
            elif len(servers) <= self.num_replicas:
                available_replicas = len(servers) - 1

            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                                   'vb_replica_curr_items', '==', items * available_replicas))
            stats_tasks.append(self.cluster.async_wait_for_stats(servers, bucket, '',
                                   'curr_items_tot', '==', items * (available_replicas + 1)))

        for task in stats_tasks:
            task.result(60)


    """Asynchronously applys load generation to all bucekts in the cluster.
 bucket.name, gen,
                                                          bucket.kvs[kv_store],
                                                          op_type, exp
    Args:
        server - A server in the cluster. (TestInputServer)
        kv_gen - The generator to use to generate load. (DocumentGenerator)
        op_type - "create", "read", "update", or "delete" (String)
        exp - The expiration for the items if updated or created (int)
        kv_store - The index of the bucket's kv_store to use. (int)

    Returns:
        A list of all of the tasks created.
    """
    def _async_load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True, batch_size=1, pause_secs=1, timeout_secs=30):
        tasks = []
        for bucket in self.buckets:
            gen = copy.deepcopy(kv_gen)
            tasks.append(self.cluster.async_load_gen_docs(server, bucket.name, gen,
                                                          bucket.kvs[kv_store],
                                                          op_type, exp, flag, only_store_hash, batch_size, pause_secs, timeout_secs))
        return tasks

    """Synchronously applys load generation to all bucekts in the cluster.

    Args:
        server - A server in the cluster. (TestInputServer)
        kv_gen - The generator to use to generate load. (DocumentGenerator)
        op_type - "create", "read", "update", or "delete" (String)
        exp - The expiration for the items if updated or created (int)
        kv_store - The index of the bucket's kv_store to use. (int)
    """
    def _load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True, batch_size=1000, pause_secs=1, timeout_secs=30):
        tasks = self._async_load_all_buckets(server, kv_gen, op_type, exp, kv_store, flag, only_store_hash, batch_size, pause_secs, timeout_secs)
        for task in tasks:
            task.result()

    """Waits for queues to drain on all servers and buckets in a cluster.

    A utility function that waits for all of the items loaded to be persisted
    and replicated.

    Args:
        servers - A list of all of the servers in the cluster. ([TestInputServer])
    """
    def _wait_for_stats_all_buckets(self, servers):
        tasks = []
        for server in servers:
            for bucket in self.buckets:
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_queue_size', '==', 0))
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_flusher_todo', '==', 0))
        for task in tasks:
            task.result()

    """Verifies data on all of the nodes in a cluster.

    Verifies all of the data in a specific kv_store index for all buckets in
    the cluster.

    Args:
        server - A server in the cluster. (TestInputServer)
        kv_store - The kv store index to check. (int)
    """
    def _verify_all_buckets(self, server, kv_store=1, timeout=180, max_verify=None, only_store_hash=True, batch_size=1000):
        tasks = []
        for bucket in self.buckets:
            tasks.append(self.cluster.async_verify_data(server, bucket, bucket.kvs[kv_store], max_verify, only_store_hash, batch_size))
        for task in tasks:
            task.result(timeout)


    def disable_compaction(self, server=None, bucket="default"):

        server = server or self.servers[0]
        new_config = {"viewFragmntThresholdPercentage" : None,
                      "dbFragmentThresholdPercentage" :  None,
                      "dbFragmentThreshold" : None,
                      "viewFragmntThreshold" : None}
        self.cluster.modify_fragmentation_config(server, new_config, bucket)

    def async_create_views(self, server, design_doc_name, views, bucket="default", with_query=True):
        tasks = []
        if len(views):
            for view in views:
                t_ = self.cluster.async_create_view(server, design_doc_name, view, bucket, with_query)
                tasks.append(t_)
        else:
            t_ = self.cluster.async_create_view(server, design_doc_name, None, bucket, with_query)
            tasks.append(t_)
        return tasks

    def create_views(self, server, design_doc_name, views, bucket="default", timeout=None):
        if len(views):
            for view in views:
                self.cluster.create_view(server, design_doc_name, view, bucket, timeout)
        else:
            self.cluster.create_view(server, design_doc_name, None, bucket, timeout)

    def make_default_views(self, prefix, count, is_dev_ddoc=False):
        ref_view = self.default_view
        ref_view.name = (prefix, ref_view.name)[prefix is None]
        return [View(ref_view.name + str(i), ref_view.map_func, None, is_dev_ddoc) for i in xrange(count)]

    def _load_doc_data_all_buckets(self, data_op="create", batch_size=1000):
        #initialize the template for document generator
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('test_docs', template, age, first, start=0, end=self.num_items)

        self.log.info("%s %s documents..." % (data_op, self.num_items))
        self._load_all_buckets(self.master, gen_load, data_op, 0, batch_size=batch_size)

    def verify_cluster_stats(self, servers=None, master=None, max_verify=None, timeout=None):
        if servers is None:
            servers = self.servers
        if master is None:
            master = self.master
        if max_verify is None:
            max_verify = self.max_verify
        self._wait_for_stats_all_buckets(servers)
        self._verify_all_buckets(master, timeout=timeout, max_verify=max_verify)
        self._verify_stats_all_buckets(servers)
        #verify that curr_items_tot corresponds to sum of curr_items from all nodes
        verified = True
        for bucket in self.buckets:
            verified &= RebalanceHelper.wait_till_total_numbers_match(master, bucket)
        self.assertTrue(verified, "Lost items!!! Replication was completed but sum(curr_items) don't match the curr_items_total")
