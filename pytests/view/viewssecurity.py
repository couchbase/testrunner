import copy
import logger
from lib.Cb_constants.CBServer import CbServer
from threading import Thread, Event
from pytests.view import createdeleteview
from basetestcase import BaseTestCase
from couchbase_helper.document import DesignDocument, View
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.helper.cluster_helper import ClusterOperationHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.cb_tools.cb_cli import CbCli
from lib.membase.api.rest_client import RestConnection, RestHelper

class ViewsSecurity(createdeleteview.CreateDeleteViewTests,BaseTestCase):
    def setUp(self):
        try:
            super(ViewsSecurity, self).setUp()
            self.bucket_ddoc_map = {}
            self.ddoc_ops = self.input.param("ddoc_ops", None)
            self.boot_op = self.input.param("boot_op", None)
            self.test_with_view = self.input.param("test_with_view", False)
            self.num_views_per_ddoc = self.input.param("num_views_per_ddoc", 1)
            self.num_ddocs = self.input.param("num_ddocs", 1)
            self.gen = None
            self.is_crashed = Event()
            self.default_design_doc_name = "Doc1"
            self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
            self.updated_map_func = 'function (doc) { emit(null, doc);}'
            self.default_view = View("View", self.default_map_func, None, False)
            self.fragmentation_value = self.input.param("fragmentation_value", 80)
            self.rest = RestConnection(self.master)
        except Exception as ex:
            self.input.test_params["stop-on-failure"] = True
            self.log.error("SETUP WAS FAILED. ALL TESTS WILL BE SKIPPED")
            self.fail(ex)

    def suite_setUp(self):
        self.log.info("---------------Suite Setup---------------")

    def suite_tearDown(self):
        self.log.info("---------------Suite Teardown---------------")

    def tearDown(self):
        super(ViewsSecurity, self).tearDown()

    def test_view_ops_n2n_encryption_enabled(self):
        shell_conn = RemoteMachineShellConnection(self.master)
        cb_cli = CbCli(shell_conn, no_ssl_verify=True)
        cb_cli.enable_n2n_encryption()
        self._load_doc_data_all_buckets()
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc,
                                   bucket=bucket)

        self._wait_for_stats_all_buckets([self.master])

        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

        self._execute_ddoc_ops("update", self.test_with_view,
                               self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)
        self._wait_for_stats_all_buckets([self.master])

        cb_cli.set_n2n_encryption_level(level="control")
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

        status, content = self.rest.set_encryption_level(level="strict")
        if status:
            self.log.info("Encryption level set to Strict")
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()
        assert ClusterOperationHelper.check_if_services_obey_tls(
            servers=[self.master]), "Port binding after enforcing TLS incorrect"

        cb_cli.disable_n2n_encryption()

    def test_view_ops_n2n_encrypytion_enabled_all(self):
        shell_conn = RemoteMachineShellConnection(self.master)
        cb_cli = CbCli(shell_conn, no_ssl_verify=True)

        self._load_doc_data_all_buckets()
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc,
                                   bucket=bucket)

        self._wait_for_stats_all_buckets([self.master])
        cb_cli.enable_n2n_encryption()
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()
        self._execute_ddoc_ops("update", self.test_with_view,
                               self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)
        self._wait_for_stats_all_buckets([self.master])
        cb_cli.set_n2n_encryption_level(level="all")
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()
        cb_cli.disable_n2n_encryption()

    def test_n2n_encryption_enabled_rebalance_in_with_ddoc_ops(self):

        self.assertTrue(self.num_servers > self.nodes_in + self.nodes_out,
                        "ERROR: Not enough nodes to do rebalance in and out")
        shell_conn = RemoteMachineShellConnection(self.master)
        cb_cli = CbCli(shell_conn, no_ssl_verify=True)
        cb_cli.enable_n2n_encryption()

        self._load_doc_data_all_buckets()

        servs_in = self.servers[1:self.nodes_in + 1]
        rebalance = self.cluster.async_rebalance(self.servers[:1], servs_in, [])

        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc,
                                   bucket=bucket)
        self._execute_ddoc_ops("update", self.test_with_view,
                               self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)
        self._execute_ddoc_ops("delete", self.test_with_view,
                               self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_in + 1])
        max_verify = None
        if self.num_items > 500000:
            max_verify = 100000

        status, content = self.rest.set_encryption_level(level="strict")
        if status:
            self.log.info("Encryption level set to Strict")

        self._verify_stats_all_buckets(self.servers[:self.nodes_in + 1],
                                       timeout=self.wait_timeout if not self.dgm_run else None)
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()
        assert ClusterOperationHelper.check_if_services_obey_tls(
            servers=[self.master]), "Port binding after enforcing TLS incorrect"
        cb_cli.disable_n2n_encryption()
    def test_n2n_encryption_enabled_rebalance_in_and_out_with_ddoc_ops(self):
        self.assertTrue(self.num_servers > self.nodes_in + self.nodes_out,
                        "ERROR: Not enough nodes to do rebalance in and out")

        servs_in = self.servers[self.num_servers - self.nodes_in:]
        servs_for_rebal = [serv for serv in self.servers if serv not in servs_in]
        servs_out = servs_for_rebal[self.num_servers - self.nodes_in - self.nodes_out:]

        # list of server which will be available after the in/out operation
        servs_after_rebal = [serv for serv in self.servers if serv not in servs_out]

        self.log.info("create a cluster of all the available servers except nodes_in")
        self.cluster.rebalance(servs_for_rebal[:1],
                               servs_for_rebal[1:], [])
        # load initial documents
        self._load_doc_data_all_buckets()

        # start the rebalance in/out operation
        rebalance = self.cluster.async_rebalance(servs_for_rebal, servs_in, servs_out)

        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc,
                                   bucket=bucket)
            if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs // 2,
                                       self.num_views_per_ddoc // 2, bucket=bucket)
        rebalance.result()
        self._wait_for_stats_all_buckets(servs_after_rebal)
        max_verify = None
        if self.num_items > 500000:
            max_verify = 100000

        shell_conn = RemoteMachineShellConnection(self.master)
        cb_cli = CbCli(shell_conn, no_ssl_verify=True)
        cb_cli.enable_n2n_encryption()
        status, content = self.rest.set_encryption_level(level="strict")
        if status:
            self.log.info("Encryption level set to Strict")
        self.cluster.async_rebalance(servs_for_rebal, servs_in, servs_out)

        self._verify_stats_all_buckets(servs_after_rebal, timeout=self.wait_timeout if not self.dgm_run else None)
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()
        assert ClusterOperationHelper.check_if_services_obey_tls(
            servers=[self.master]), "Port binding after enforcing TLS incorrect"
        cb_cli.disable_n2n_encryption()