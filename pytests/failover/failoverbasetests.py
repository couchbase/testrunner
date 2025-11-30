from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.document import View
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper


class FailoverBaseTest(BaseTestCase):

    def setUp(self):
        self._cleanup_nodes = []
        self._failed_nodes = []
        super(FailoverBaseTest, self).setUp()
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view_name = "default_view"
        self.default_view = View(self.default_view_name, self.defaul_map_func, None)
        self.failoverMaster = self.input.param("failoverMaster", False)
        self.total_vbuckets = self.input.param("total_vbuckets", 128)
        self.compact = self.input.param("compact", False)
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", 20)
        self.withMutationOps = self.input.param("withMutationOps", False)
        self.withViewsOps = self.input.param("withViewsOps", False)
        self.createIndexesDuringFailover = self.input.param("createIndexesDuringFailover", False)
        self.upr_check = self.input.param("upr_check", True)
        self.withQueries = self.input.param("withQueries", False)
        self.numberViews = self.input.param("numberViews", False)
        self.gracefulFailoverFail = self.input.param("gracefulFailoverFail", False)
        self.runRebalanceAfterFailover = self.input.param("runRebalanceAfterFailover", True)
        self.failoverMaster = self.input.param("failoverMaster", False)
        self.check_verify_failover_type = self.input.param("check_verify_failover_type", True)
        self.recoveryType = self.input.param("recoveryType", "delta")
        self.bidirectional = self.input.param("bidirectional", False)
        self.stopGracefulFailover = self.input.param("stopGracefulFailover", False)
        self._value_size = self.input.param("value_size", 256)
        self.victim_type = self.input.param("victim_type", "other")
        self.victim_count = self.input.param("victim_count", 1)
        self.stopNodes = self.input.param("stopNodes", False)
        self.killNodes = self.input.param("killNodes", False)
        self.doc_ops = self.input.param("doc_ops", [])
        self.firewallOnNodes = self.input.param("firewallOnNodes", False)
        self.deltaRecoveryBuckets = self.input.param("deltaRecoveryBuckets", None)
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(":")
        self.num_failed_nodes = self.input.param("num_failed_nodes", 0)
        self.dgm_run = self.input.param("dgm_run", True)
        credentials = self.input.membase_settings
        self.add_back_flag = False
        self.during_ops = self.input.param("during_ops", None)
        self.graceful = self.input.param("graceful", True)
        if self.recoveryType:
            self.recoveryType = self.recoveryType.split(":")
        if self.deltaRecoveryBuckets:
            self.deltaRecoveryBuckets = self.deltaRecoveryBuckets.split(":")

        # To validate MB-34173
        self.sleep_before_rebalance = \
            self.input.param("sleep_before_rebalance", None)
        self.flusher_total_batch_limit = \
            self.input.param("flusher_total_batch_limit", None)

        if self.flusher_total_batch_limit:
            self.set_flusher_total_batch_limit(
                self.flusher_total_batch_limit, self.buckets)

        # Defintions of Blod Generator used in tests
        self.gen_initial_create = BlobGenerator('failover', 'failover', self.value_size, end=self.num_items)
        self.gen_create = BlobGenerator('failover', 'failover', self.value_size, start=self.num_items + 1,
                                        end=self.num_items * 1.5)
        self.gen_update = BlobGenerator('failover', 'failover', self.value_size, start=self.num_items // 2,
                                        end=self.num_items)
        self.gen_delete = BlobGenerator('failover', 'failover', self.value_size, start=self.num_items // 4,
                                        end=self.num_items // 2 - 1)
        self.afterfailover_gen_create = BlobGenerator('failover', 'failover', self.value_size,
                                                      start=self.num_items * 1.6, end=self.num_items * 2)
        self.afterfailover_gen_update = BlobGenerator('failover', 'failover', self.value_size, start=1,
                                                      end=self.num_items / 4)
        self.afterfailover_gen_delete = BlobGenerator('failover', 'failover', self.value_size,
                                                      start=self.num_items * .5, end=self.num_items * 0.75)
        if self.vbuckets is not None:
            if (self.bucket_storage != "magma" or
                    (self.total_vbuckets not in [128, 1024])):
                # Consider test input param total_vbuckets only if it's not 128 or 1024
                # Because magma officially supports only 128 and 1024 vbuckets
                # So we should not override in other cases
                self.total_vbuckets = self.vbuckets
        self.log.info("==============  FailoverBaseTest setup was finished for test #{0} {1} ==============" \
                      .format(self.case_number, self._testMethodName))

    def tearDown(self):
        if hasattr(self, '_resultForDoCleanups') and len(self._resultForDoCleanups.failures) > 0 \
                and 'stop-on-failure' in TestInputSingleton.input.test_params and \
                str(TestInputSingleton.input.test_params['stop-on-failure']).lower() == 'true':
            # supported starting with python2.7
            self.log.warning("CLEANUP WAS SKIPPED")
            self.cluster.shutdown(force=True)
            self._log_finish(self)
        else:
            try:
                self.log.info("==============  tearDown was started for test #{0} {1} ==============" \
                              .format(self.case_number, self._testMethodName))
                RemoteUtilHelper.common_basic_setup(self.servers)
                BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
                for node in self.servers:
                    master = node
                    try:
                        ClusterOperationHelper.cleanup_cluster(self.servers,
                                                               master=master)
                    except:
                        continue
                self.log.info("==============  tearDown was finished for test #{0} {1} ==============" \
                              .format(self.case_number, self._testMethodName))
            finally:
                super(FailoverBaseTest, self).tearDown()
