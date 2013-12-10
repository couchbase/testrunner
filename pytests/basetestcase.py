import logger
import unittest
import copy
import datetime
import time
import string
import random

from couchbase.documentgenerator import BlobGenerator
from couchbase.cluster import Cluster
from couchbase.document import View
from couchbase.documentgenerator import DocumentGenerator
from couchbase.stats_tools import StatsCommon
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection, Bucket
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from membase.api.exception import ServerUnavailableException


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.buckets = []
        self.master = self.servers[0]
        self.cluster = Cluster()
        self.pre_warmup_stats = {}
        try:
            self.auth_mech = self.input.param("auth_mech", "PLAIN")
            self.wait_timeout = self.input.param("wait_timeout", 60)
            # number of case that is performed from testrunner( increment each time)
            self.case_number = self.input.param("case_number", 0)
            self.default_bucket = self.input.param("default_bucket", True)
            if self.default_bucket:
                self.default_bucket_name = "default"
            self.standard_buckets = self.input.param("standard_buckets", 0)
            self.sasl_buckets = self.input.param("sasl_buckets", 0)
            self.num_buckets = self.input.param("num_buckets", 0)
            self.memcached_buckets = self.input.param("memcached_buckets", 0)
            self.total_buckets = self.sasl_buckets + self.default_bucket + self.standard_buckets + self.memcached_buckets
            self.num_servers = self.input.param("servers", len(self.servers))
            # initial number of items in the cluster
            self.nodes_init = self.input.param("nodes_init", 1)
            self.nodes_in = self.input.param("nodes_in", 1)
            self.nodes_out = self.input.param("nodes_out", 1)

            self.num_replicas = self.input.param("replicas", 1)
            self.num_items = self.input.param("items", 1000)
            self.value_size = self.input.param("value_size", 512)
            self.dgm_run = self.input.param("dgm_run", False)
            self.active_resident_threshold = int(self.input.param("active_resident_threshold", 0))
            # max items number to verify in ValidateDataTask, None - verify all
            self.max_verify = self.input.param("max_verify", None)
            # we don't change consistent_view on server by default
            self.disabled_consistent_view = self.input.param("disabled_consistent_view", None)
            self.rebalanceIndexWaitingDisabled = self.input.param("rebalanceIndexWaitingDisabled", None)
            self.rebalanceIndexPausingDisabled = self.input.param("rebalanceIndexPausingDisabled", None)
            self.maxParallelIndexers = self.input.param("maxParallelIndexers", None)
            self.maxParallelReplicaIndexers = self.input.param("maxParallelReplicaIndexers", None)
            self.quota_percent = self.input.param("quota_percent", None)
            self.port = None
            self.log_info=self.input.param("log_info", None)
            self.log_location=self.input.param("log_location", None)
            self.stat_info=self.input.param("stat_info", None)
            self.port_info=self.input.param("port_info", None)

            if self.input.param("log_info", None):
                self.change_log_info()
            if self.input.param("log_location", None):
                self.change_log_location()
            if self.input.param("stat_info", None):
                self.change_stat_info()
            if self.input.param("port_info", None):
                self.change_port_info()

            if self.input.param("port", None):
                self.port = str(self.input.param("port", None))
            self.log.info("==============  basetestcase setup was started for test #{0} {1}=============="\
                          .format(self.case_number, self._testMethodName))
            # avoid any cluster operations in setup for new upgrade & upgradeXDCR tests
            if str(self.__class__).find('newupgradetests') != -1 or \
                    str(self.__class__).find('upgradeXDCR') != -1 or \
                    hasattr(self, 'skip_buckets_handle') and self.skip_buckets_handle:
                self.log.info("any cluster operation in setup will be skipped")
                self.log.info("==============  basetestcase setup was finished for test #{0} {1} =============="\
                          .format(self.case_number, self._testMethodName))
                return
            # avoid clean up if the previous test has been tear down
            if not self.input.param("skip_cleanup", True) or self.case_number == 1 or self.case_number > 1000:
                if self.case_number > 1000:
                    self.log.warn("teardDown for previous test failed. will retry..")
                    self.case_number -= 1000
                self.tearDown()
                self.cluster = Cluster()

            self.quota = self._initialize_nodes(self.cluster, self.servers, self.disabled_consistent_view,
                                            self.rebalanceIndexWaitingDisabled, self.rebalanceIndexPausingDisabled,
                                            self.maxParallelIndexers, self.maxParallelReplicaIndexers, self.port)

            try:
                if (str(self.__class__).find('rebalanceout.RebalanceOutTests') != -1) or \
                    (str(self.__class__).find('memorysanitytests.MemorySanity') != -1) or \
                    str(self.__class__).find('negativetests.NegativeTests') != -1:
                    # rebalance all nodes into the cluster before each test
                    self.cluster.rebalance(self.servers[:self.num_servers], self.servers[1:self.num_servers], [])
                elif self.nodes_init > 1:
                    self.cluster.rebalance(self.servers[:1], self.servers[1:self.nodes_init], [])
                elif str(self.__class__).find('ViewQueryTests') != -1 and \
                        not self.input.param("skip_rebalance", False):
                    self.cluster.rebalance(self.servers, self.servers[1:], [])
            except BaseException, e:
                # increase case_number to retry tearDown in setup for the next test
                self.case_number += 1000
                self.fail(e)
            if self.dgm_run:
                self.quota = 256
            if self.total_buckets > 10:
                self.log.info("================== changing max buckets from 10 to {0} =================".format\
                                                            (self.total_buckets))
                self.change_max_buckets(self, self.total_buckets)
            if self.total_buckets > 0:
                self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)
            if str(self.__class__).find('newupgradetests') == -1:
                self._bucket_creation()
            self.log.info("==============  basetestcase setup was finished for test #{0} {1} =============="\
                          .format(self.case_number, self._testMethodName))
            self._log_start(self)
        except Exception, e:
            self.cluster.shutdown()
            self.fail(e)

    def tearDown(self):
            try:
                if hasattr(self, 'skip_buckets_handle') and self.skip_buckets_handle:
                    return
                test_failed = (hasattr(self, '_resultForDoCleanups') and len(self._resultForDoCleanups.failures or self._resultForDoCleanups.errors)) \
                    or (hasattr(self, '_exc_info') and self._exc_info()[1] is not None)
                if test_failed and TestInputSingleton.input.param("stop-on-failure", False)\
                        or self.input.param("skip_cleanup", False):
                    self.log.warn("CLEANUP WAS SKIPPED")
                else:
                    if test_failed and TestInputSingleton.input.param('get_trace', None):
                        for server in self.servers:
                            try:
                                shell = RemoteMachineShellConnection(server)
                                output, _ = shell.execute_command("ps -aef|grep %s" %
                                        TestInputSingleton.input.param('get_trace', None))
                                output = shell.execute_command("pstack %s" % output[0].split()[1].strip())
                                print output[0]
                            except:
                                pass
                    if test_failed and self.input.param('BUGS', False):
                        self.log.warn("Test failed. Possible reason is: {0}".format(self.input.param('BUGS', False)))

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
                    if self.input.param("forceEject", False):
                        for server in self.servers:
                            if server != self.servers[0]:
                                try:
                                    rest = RestConnection(server)
                                    rest.force_eject_node()
                                except BaseException, e:
                                    self.log.error(e)
                    ClusterOperationHelper.cleanup_cluster(self.servers)
                    self.sleep(10)
                    ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
                    self.log.info("==============  basetestcase cleanup was finished for test #{0} {1} =============="\
                          .format(self.case_number, self._testMethodName))
            except BaseException:
                # increase case_number to retry tearDown in setup for the next test
                self.case_number += 1000
            finally:
                # stop all existing task manager threads
                self.cluster.shutdown(force=True)
                self._log_finish(self)

    @staticmethod
    def change_max_buckets(self, total_buckets):
        command = "curl -X POST -u {0}:{1} -d maxBucketCount={2} http://{3}:{4}/internalSettings".format\
            (self.servers[0].rest_username,
            self.servers[0].rest_password,
            total_buckets,
            self.servers[0].ip,
            self.servers[0].port)
        shell = RemoteMachineShellConnection(self.servers[0])
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()

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

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def _initialize_nodes(self, cluster, servers, disabled_consistent_view=None, rebalanceIndexWaitingDisabled=None,
                          rebalanceIndexPausingDisabled=None, maxParallelIndexers=None, maxParallelReplicaIndexers=None,
                          port=None, quota_percent=None):
        quota = 0
        init_tasks = []
        for server in servers:
            init_port = port or server.port or '8091'
            init_tasks.append(cluster.async_init_node(server, disabled_consistent_view, rebalanceIndexWaitingDisabled,
                          rebalanceIndexPausingDisabled, maxParallelIndexers, maxParallelReplicaIndexers, init_port,
                          quota_percent))
        for task in init_tasks:
            node_quota = task.result()
            if node_quota < quota or quota == 0:
                quota = node_quota
        if quota < 100 and not len(set([server.ip for server in self.servers])) == 1:
            self.log.warn("RAM quota was defined less than 100 MB:")
            for server in servers:
                remote_client = RemoteMachineShellConnection(server)
                ram = remote_client.extract_remote_info().ram
                self.log.info("{0}: {1} MB".format(server.ip, ram))
                remote_client.disconnect()
        return quota

    def _bucket_creation(self):
        if self.default_bucket:
            self.cluster.create_default_bucket(self.master, self.bucket_size, self.num_replicas)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                       num_replicas=self.num_replicas, bucket_size=self.bucket_size))

        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self._create_standard_buckets(self.master, self.standard_buckets)
        self._create_memcached_buckets(self.master, self.memcached_buckets)

    def _get_bucket_size(self, mem_quota, num_buckets, ratio=2.0 / 3.0):
        #min size is 100MB now
        return max(100, int(ratio / float(num_buckets) * float(mem_quota)))

    def _create_sasl_buckets(self, server, num_buckets, server_id=None, bucket_size=None, password='password'):
        if not num_buckets:
            return
        if server_id is None:
            server_id = RestConnection(server).get_nodes_self().id
        if bucket_size is None:
            bucket_size = self.bucket_size
        bucket_tasks = []
        for i in range(num_buckets):
            name = 'bucket' + str(i)
            bucket_tasks.append(self.cluster.async_create_sasl_bucket(server, name,
                                                                      password,
                                                                      bucket_size,
                                                                      self.num_replicas))
            self.buckets.append(Bucket(name=name, authType="sasl", saslPassword=password,
                                       num_replicas=self.num_replicas, bucket_size=bucket_size,
                                       master_id=server_id));
        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)

    def _create_standard_buckets(self, server, num_buckets, server_id=None, bucket_size=None):
        if not num_buckets:
            return
        if server_id is None:
            server_id = RestConnection(server).get_nodes_self().id
        if bucket_size is None:
            bucket_size = self.bucket_size
        bucket_tasks = []
        for i in range(num_buckets):
            name = 'standard_bucket' + str(i)
            bucket_tasks.append(self.cluster.async_create_standard_bucket(server, name,
                                                                          11214 + i,
                                                                          bucket_size,
                                                                          self.num_replicas))
            self.buckets.append(Bucket(name=name, authType=None, saslPassword=None,
                                       num_replicas=self.num_replicas,
                                       bucket_size=bucket_size, port=11214 + i, master_id=server_id));
        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)

    def _create_memcached_buckets(self, server, num_buckets, server_id=None, bucket_size=None):
        if not num_buckets:
            return
        if server_id is None:
            server_id = RestConnection(server).get_nodes_self().id
        if bucket_size is None:
            bucket_size = self.bucket_size
        bucket_tasks = []
        for i in range(num_buckets):
            name = 'memcached_bucket' + str(i)
            bucket_tasks.append(self.cluster.async_create_memcached_bucket(server, name,
                                                                          11214 + \
                                                                          self.standard_buckets + i,
                                                                          bucket_size,
                                                                          self.num_replicas))
            self.buckets.append(Bucket(name=name, authType=None, saslPassword=None,
                                       num_replicas=self.num_replicas,
                                       bucket_size=bucket_size, port=11214 + \
                                                        self.standard_buckets + i,
                                       master_id=server_id, type='memcached'));
        for task in bucket_tasks:
            task.result()

    def _all_buckets_delete(self, server):
        delete_tasks = []
        for bucket in self.buckets:
            delete_tasks.append(self.cluster.async_bucket_delete(server, bucket.name))

        for task in delete_tasks:
            task.result()
        self.buckets = []

    def _verify_stats_all_buckets(self, servers, timeout=60):
        stats_tasks = []
        for bucket in self.buckets:
            items = sum([len(kv_store) for kv_store in bucket.kvs.values()])
            if bucket.type == 'memcached':
                items_actual = 0
                for server in servers:
                    client = MemcachedClientHelper.direct_client(server, bucket)
                    items_actual += int(client.stats()["curr_items"])
                self.assertEqual(items, items_actual, "Items are not correct")
                continue
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
        try:
            for task in stats_tasks:
                task.result(timeout)
        except Exception as e:
            print e;
            for task in stats_tasks:
                task.cancel()
            self.log.error("unable to get expected stats for any node! Print taps for all nodes:")
            rest = RestConnection(self.master)
            for bucket in self.buckets:
                RebalanceHelper.print_taps_from_all_nodes(rest, bucket)
            raise Exception("unable to get expected stats during {0} sec".format(timeout))

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
            if bucket.type != 'memcached':
                tasks.append(self.cluster.async_load_gen_docs(server, bucket.name, gen,
                                                          bucket.kvs[kv_store],
                                                          op_type, exp, flag, only_store_hash, batch_size, pause_secs, timeout_secs))
            else:
                self._load_memcached_bucket(server, gen, bucket.name)
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
        if self.active_resident_threshold:
            stats_all_buckets = {}
            for bucket in self.buckets:
                stats_all_buckets[bucket.name] = StatsCommon()

            for bucket in self.buckets:
                threshold_reached = False
                while not threshold_reached :
                    active_resident = stats_all_buckets[bucket.name].get_stats([self.master], bucket, '', 'vb_active_perc_mem_resident')[server]
                    if int(active_resident) > self.active_resident_threshold:
                        self.log.info("resident ratio is %s greater than %s for %s in bucket %s. Continue loading to the cluster" %
                                      (active_resident, self.active_resident_threshold, self.master.ip, bucket.name))
                        random_key = self.key_generator()
                        generate_load = BlobGenerator(random_key, '%s-' % random_key, self.value_size, end=20000)
                        self._load_all_buckets(self.master, generate_load, "create", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                    else:
                        threshold_reached = True
                        self.log.info("DGM state achieved for %s in bucket %s!" % (self.master.ip, bucket.name))
                        break

    def key_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))

    """Waits for queues to drain on all servers and buckets in a cluster.

    A utility function that waits for all of the items loaded to be persisted
    and replicated.

    Args:
        servers - A list of all of the servers in the cluster. ([TestInputServer])
        ep_queue_size - expected ep_queue_size (int)
        ep_queue_size_cond - condition for comparing (str)
        timeout - Waiting the end of the thread. (str)
    """
    def _wait_for_stats_all_buckets(self, servers, ep_queue_size=0, \
                                     ep_queue_size_cond='==', timeout=360):
        tasks = []
        for server in servers:
            for bucket in self.buckets:
                if bucket.type == 'memcached':
                    continue
                tasks.append(self.cluster.async_wait_for_stats([server], bucket, '',
                                   'ep_queue_size', ep_queue_size_cond, ep_queue_size))
        for task in tasks:
            task.result(timeout)

    """Verifies data on all of the nodes in a cluster.

    Verifies all of the data in a specific kv_store index for all buckets in
    the cluster.

    Args:
        server - A server in the cluster. (TestInputServer)
        kv_store - The kv store index to check. (int)
        timeout - Waiting the end of the thread. (str)
    """
    def _verify_all_buckets(self, server, kv_store=1, timeout=180, max_verify=None, only_store_hash=True, batch_size=1000,
                            replica_to_read=None):
        tasks = []
        if len(self.buckets) > 1:
            batch_size = 1
        for bucket in self.buckets:
            if bucket.type == 'memcached':
                continue
            tasks.append(self.cluster.async_verify_data(server, bucket, bucket.kvs[kv_store], max_verify,
                                                        only_store_hash, batch_size, replica_to_read))
        for task in tasks:
            task.result(timeout)


    def disable_compaction(self, server=None, bucket="default"):

        server = server or self.servers[0]
        new_config = {"viewFragmntThresholdPercentage" : None,
                      "dbFragmentThresholdPercentage" :  None,
                      "dbFragmentThreshold" : None,
                      "viewFragmntThreshold" : None}
        self.cluster.modify_fragmentation_config(server, new_config, bucket)

    def async_create_views(self, server, design_doc_name, views, bucket="default", with_query=True, check_replication=False):
        tasks = []
        if len(views):
            for view in views:
                t_ = self.cluster.async_create_view(server, design_doc_name, view, bucket, with_query, check_replication=check_replication)
                tasks.append(t_)
        else:
            t_ = self.cluster.async_create_view(server, design_doc_name, None, bucket, with_query, check_replication=check_replication)
            tasks.append(t_)
        return tasks

    def create_views(self, server, design_doc_name, views, bucket="default", timeout=None, check_replication=False):
        if len(views):
            for view in views:
                self.cluster.create_view(server, design_doc_name, view, bucket, timeout, check_replication=check_replication)
        else:
            self.cluster.create_view(server, design_doc_name, None, bucket, timeout, check_replication=check_replication)

    def make_default_views(self, prefix, count, is_dev_ddoc=False, different_map=False):
        ref_view = self.default_view
        ref_view.name = (prefix, ref_view.name)[prefix is None]
        if different_map:
            views = []
            for i in xrange(count):
                views.append(View(ref_view.name + str(i),
                                  'function (doc, meta) {'
                                  'emit(meta.id, "emitted_value%s");}' % str(i),
                                  None, is_dev_ddoc))
            return views
        else:
            return [View(ref_view.name + str(i), ref_view.map_func, None, is_dev_ddoc) for i in xrange(count)]

    def _load_doc_data_all_buckets(self, data_op="create", batch_size=1000, gen_load=None):
        # initialize the template for document generator
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "mutated" : 0, "age": {0}, "first_name": "{1}" }}'
        if gen_load is None:
            gen_load = DocumentGenerator('test_docs', template, age, first, start=0, end=self.num_items)

        self.log.info("%s %s documents..." % (data_op, self.num_items))
        self._load_all_buckets(self.master, gen_load, data_op, 0, batch_size=batch_size)
        return gen_load

    def verify_cluster_stats(self, servers=None, master=None, max_verify=None, timeout=None, check_items=True,
                             only_store_hash=True, replica_to_read=None, batch_size=1000):
        if servers is None:
            servers = self.servers
        if master is None:
            master = self.master
        if max_verify is None:
            max_verify = self.max_verify

        self._wait_for_stats_all_buckets(servers, timeout=(timeout or 120))
        if check_items:
            try:
                self._verify_all_buckets(master, timeout=timeout, max_verify=max_verify,
                                     only_store_hash=only_store_hash, replica_to_read=replica_to_read,
                                     batch_size=batch_size)
            except ValueError, e:
                # get/verify stats if 'ValueError: Not able to get values for following keys' was gotten
                self._verify_stats_all_buckets(servers, timeout=(timeout or 120))
                raise e
            self._verify_stats_all_buckets(servers, timeout=(timeout or 120))
            # verify that curr_items_tot corresponds to sum of curr_items from all nodes
            verified = True
            for bucket in self.buckets:
                verified &= RebalanceHelper.wait_till_total_numbers_match(master, bucket)
            self.assertTrue(verified, "Lost items!!! Replication was completed but sum(curr_items) don't match the curr_items_total")
        else:
            self.log.warn("verification of items was omitted")

    def _stats_befor_warmup(self, bucket_name):
        self.pre_warmup_stats[bucket_name] = {}
        self.stats_monitor = self.input.param("stats_monitor", "")
        self.warmup_stats_monitor = self.input.param("warmup_stats_monitor", "")
        if self.stats_monitor is not '':
            self.stats_monitor = self.stats_monitor.split(";")
        if self.warmup_stats_monitor is not '':
            self.warmup_stats_monitor = self.warmup_stats_monitor.split(";")
        for server in self.servers:
            mc_conn = MemcachedClientHelper.direct_client(server, bucket_name, self.timeout)
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)] = {}
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"] = mc_conn.stats("")["uptime"]
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["curr_items_tot"] = mc_conn.stats("")["curr_items_tot"]
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["curr_items"] = mc_conn.stats("")["curr_items"]
            for stat_to_monitor in self.stats_monitor:
                self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)][stat_to_monitor] = mc_conn.stats('')[stat_to_monitor]
            if self.without_access_log:
                for stat_to_monitor in self.warmup_stats_monitor:
                    self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)][stat_to_monitor] = mc_conn.stats('warmup')[stat_to_monitor]
            mc_conn.close()

    def _kill_nodes(self, nodes, servers, bucket_name):
        self.reboot = self.input.param("reboot", False)
        if not self.reboot:
            for node in nodes:
                _node = {"ip": node.ip, "port": node.port, "username": self.servers[0].rest_username,
                  "password": self.servers[0].rest_password}
                node_rest = RestConnection(_node)
                _mc = MemcachedClientHelper.direct_client(_node, bucket_name)
                self.log.info("restarted the node %s:%s" % (node.ip, node.port))
                pid = _mc.stats()["pid"]
                command = "os:cmd(\"kill -9 {0} \")".format(pid)
                self.log.info(command)
                killed = node_rest.diag_eval(command)
                self.log.info("killed ??  {0} ".format(killed))
                _mc.close()
        else:
            for server in servers:
                shell = RemoteMachineShellConnection(server)
                command = "reboot"
                output, error = shell.execute_command(command)
                shell.log_command_output(output, error)
                shell.disconnect()
                time.sleep(self.wait_timeout * 8)
                shell = RemoteMachineShellConnection(server)
                command = "/sbin/iptables -F"
                output, error = shell.execute_command(command)
                shell.log_command_output(output, error)
                shell.disconnect()

    def _restart_memcache(self, bucket_name):
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        is_partial = self.input.param("is_partial", "True")
        _nodes = []
        if len(self.servers) > 1 :
            skip = 2
        else:
            skip = 1
        if is_partial:
            _nodes = nodes[:len(nodes):skip]
        else:
            _nodes = nodes

        _servers = []
        for server in self.servers:
            for _node in _nodes:
                if server.ip == _node.ip:
                    _servers.append(server)

        self._kill_nodes(_nodes, _servers, bucket_name)

        start = time.time()
        memcached_restarted = False

        for server in _servers:
            while time.time() - start < (self.wait_timeout * 2):
                mc = None
                try:
                    mc = MemcachedClientHelper.direct_client(server, bucket_name)
                    stats = mc.stats()
                    new_uptime = int(stats["uptime"])
                    self.log.info("New warmup uptime %s:%s" % (new_uptime, self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"]))
                    if new_uptime < self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"]:
                        self.log.info("memcached restarted...")
                        memcached_restarted = True
                        break;
                except Exception:
                    self.log.error("unable to connect to %s:%s for bucket %s" % (server.ip, server.port, bucket_name))
                    if mc:
                        mc.close()
                    time.sleep(5)
            if not memcached_restarted:
                self.fail("memcached did not start %s:%s for bucket %s" % (server.ip, server.port, bucket_name))

    def perform_verify_queries(self, num_views, prefix, ddoc_name, query, wait_time=120,
                               bucket="default", expected_rows=None, retry_time=2, server=None):
        tasks = []
        if server is None:
            server = self.master
        if expected_rows is None:
            expected_rows = self.num_items
        for i in xrange(num_views):
            tasks.append(self.cluster.async_query_view(server, prefix + ddoc_name,
                                                       self.default_view_name + str(i), query,
                                                       expected_rows, bucket, retry_time))
        try:
            for task in tasks:
                task.result(wait_time)
        except Exception as e:
            print e;
            for task in tasks:
                task.cancel()
            raise Exception("unable to get expected results for view queries during {0} sec".format(wait_time))

    def wait_node_restarted(self, server, wait_time=120, wait_if_warmup=False, check_service=False):
        now = time.time()
        if check_service:
            self.wait_service_started(server, wait_time)
            wait_time = now + wait_time - time.time()
        num = 0
        while num < wait_time / 10:
            try:
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                                            [server], self, wait_time=wait_time - num * 10, wait_if_warmup=wait_if_warmup)
                break
            except ServerUnavailableException:
                num += 1
                self.sleep(10)

    def wait_service_started(self, server, wait_time=120):
        shell = RemoteMachineShellConnection(server)
        type = shell.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            cmd = "sc query CouchbaseServer | grep STATE"
        else:
            cmd = "service couchbase-server status"
        now = time.time()
        while time.time() - now < wait_time:
            output, error = shell.execute_command(cmd)
            if str(output).lower().find("running") != -1:
                self.log.info("Couchbase service is running")
                shell.disconnect()
                return
            else:
                self.log.warn("couchbase service is not running. {0}".format(output))
                self.sleep(10)
        shell.disconnect()
        self.fail("Couchbase service is not running after {0} seconds".format(wait_time))

    def _load_memcached_bucket(self, server, gen_load, bucket_name):
        num_tries = 0
        while num_tries < 6:
            try:
                num_tries += 1
                client = MemcachedClientHelper.direct_client(server, bucket_name)
                break
            except Exception as ex:
                if num_tries < 5:
                    self.log.info("unable to create memcached client due to {0}. Try again".format(ex))
                else:
                    self.log.error("unable to create memcached client due to {0}.".format(ex))
        while gen_load.has_next():
            key, value = gen_load.next()
            for v in xrange(1024):
                try:
                    client.set(key, 0, 0, value, v)
                    break
                except:
                    pass
        client.close()

    def change_password(self, new_password="new_password"):
        nodes = RestConnection(self.master).node_statuses()
        remote_client = RemoteMachineShellConnection(self.master)
        options = "--cluster-init-password=%s" % new_password
        cli_command = "cluster-init"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options,
                                                            cluster_host="localhost:8091",
                                                            user=self.master.rest_username,
                                                            password=self.master.rest_password)
        self.log.info(output)
        if error:
            raise Exception("Password didn't change! %s" % error)
        for node in nodes:
            for server in self.servers:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    server.rest_password = new_password
                    break

    def change_port(self, new_port="9090", current_port='8091'):
        nodes = RestConnection(self.master).node_statuses()
        remote_client = RemoteMachineShellConnection(self.master)
        options = "--cluster-init-port=%s" % new_port
        cli_command = "cluster-init"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options,
                                                            cluster_host="localhost:%s" % current_port,
                                                            user=self.master.rest_username,
                                                            password=self.master.rest_password)
        self.log.info(output)
        if error:
            raise Exception("Port didn't change! %s" % error)
        self.port = new_port
        for node in nodes:
            for server in self.servers:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    server.port = new_port
                    break

    def change_log_info(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_log_level(self.log_info)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED LOG LEVELS ===========")

    def change_log_location(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.configure_log_location(self.log_location)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED LOG LOCATION ===========")

    def change_stat_info(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_stat_periodicity(self.stat_info)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED STAT PERIODICITY ===========")

    def change_port_info(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_port_static(self.port_info)
            server.port = self.port_info
            self.log.info("New REST port %s" % server.port)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED ALL PORTS ===========")