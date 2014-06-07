from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.api.rest_client import RestConnection, RestHelper
from couchbase.documentgenerator import BlobGenerator
from couchbase.document import View
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper

class FailoverBaseTest(BaseTestCase):

    @staticmethod
    def setUp(self):
        self._cleanup_nodes = []
        self._failed_nodes = []
        super(FailoverBaseTest, self).setUp()
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view_name = "default_view"
        self.default_view = View(self.default_view_name, self.defaul_map_func, None)
        self.failoverMaster = self.input.param("failoverMaster", False)
        self.withOps = self.input.param("withOps", False)
        self.runViews = self.input.param("runViews", False)
        self.upr_check = self.input.param("upr_check", True)
        self.withQueries = self.input.param("withQueries", False)
        self.numberViews = self.input.param("numberViews", False)
        self.gracefulFailoverFail = self.input.param("gracefulFailoverFail", False)
        self.failoverMaster = self.input.param("failoverMaster", False)
        self.recoveryType = self.input.param("recoveryType", "delta")
        self.bidirectional = self.input.param("bidirectional", False)
        self._value_size = self.input.param("value_size", 256)
        self.doc_ops = self.input.param("doc_ops", [])
        self.runViewsDuringFailover = self.input.param("runViewsDuringFailover", False)
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(":")
        self.num_failed_nodes = self.input.param("num_failed_nodes", 0)
        self.dgm_run = self.input.param("dgm_run", True)
        credentials = self.input.membase_settings
        self.add_back_flag = False
        self.during_ops = self.input.param("during_ops", None)
        self.graceful = self.input.param("graceful", True)
        if self.recoveryType:
            self.recoveryType=self.recoveryType.split(":")

        # Defintions of Blod Generator used in tests
        self.gen_initial_create = BlobGenerator('failover', 'failover', self.value_size, end=self.num_items)
        self.gen_create = BlobGenerator('failover', 'failover', self.value_size, start=self.num_items + 1 , end=self.num_items * 1.5)
        self.gen_update = BlobGenerator('failover', 'failover', self.value_size, start=self.num_items / 2, end=self.num_items)
        self.gen_delete = BlobGenerator('failover', 'failover', self.value_size, start=self.num_items / 4, end=self.num_items / 2 - 1)

        self.log.info("==============  FailoverBaseTest setup was finished for test #{0} {1} =============="\
                      .format(self.case_number, self._testMethodName))

    @staticmethod
    def tearDown(self):
        if self.enable_flow_control and self.verify_max_unacked_bytes:
            servers  = self.get_nodes_in_cluster()
            self.wait_for_max_unacked_bytes_all_buckets(servers = servers, timeout = 60)
        if hasattr(self, '_resultForDoCleanups') and len(self._resultForDoCleanups.failures) > 0 \
                    and 'stop-on-failure' in TestInputSingleton.input.test_params and \
                    str(TestInputSingleton.input.test_params['stop-on-failure']).lower() == 'true':
                    # supported starting with python2.7
                    log.warn("CLEANUP WAS SKIPPED")
                    self.cluster.shutdown(force=True)
                    self._log_finish(self)
        else:
            try:
                self.log.info("==============  tearDown was started for test #{0} {1} =============="\
                              .format(self.case_number, self._testMethodName))
                RemoteUtilHelper.common_basic_setup(self.servers)
                self.log.info("==============  tearDown was finished for test #{0} {1} =============="\
                              .format(self.case_number, self._testMethodName))
            finally:
                super(FailoverBaseTest, self).tearDown()