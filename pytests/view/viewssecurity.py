import copy
from threading import Thread, Event
from couchbase_helper.document import DesignDocument, View
from membase.helper.cluster_helper import ClusterOperationHelper
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.security.x509main import x509main
from pytests.view import createdeleteview
from basetestcase import BaseTestCase


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

        except Exception as ex:
            self.input.test_params["stop-on-failure"] = True
            self.log.error("SETUP WAS FAILED. ALL TESTS WILL BE SKIPPED")
            self.fail(ex)

    def tearDown(self):
        super(ViewsSecurity, self).tearDown()

    def test_view_ops_n2n_encryption_enabled(self):

        ntonencryptionBase().disable_nton_cluster([self.master])
        self.log.info("###### Generating x509 certificate#####")
        self.generate_x509_certs(self.servers)
        self.log.info("###### uploading x509 certificate#####")
        self.upload_x509_certs(self.servers)

        self._load_doc_data_all_buckets()
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc,
                                   bucket=bucket)

        self._wait_for_stats_all_buckets([self.master])
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        self.x509enable = True

        encryption_result = ntonencryptionBase().setup_nton_cluster(self.servers, 'enable', self.ntonencrypt_level)
        self.assertTrue(encryption_result, "Retries Exceeded. Cannot enable n2n encryption")

        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

        self._execute_ddoc_ops("update", self.test_with_view,
                               self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)
        self._wait_for_stats_all_buckets([self.master])

        ntonencryptionBase().disable_nton_cluster([self.master])

        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

        self._execute_ddoc_ops("delete", self.test_with_view,
                               self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)

        self._wait_for_stats_all_buckets([self.master])
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

        assert ClusterOperationHelper.check_if_services_obey_tls(
            servers=[self.master]), "Port binding after enforcing TLS incorrect"


    def test_n2n_encryption_enabled_rebalance_in_with_ddoc_ops(self):


        self.assertTrue(self.num_servers > self.nodes_in + self.nodes_out,
                        "ERROR: Not enough nodes to do rebalance in and out")


        ntonencryptionBase().disable_nton_cluster([self.master])

        self._load_doc_data_all_buckets()


        servs_in = self.servers[1:self.nodes_in + 1]
        rebalance = self.cluster.async_rebalance(self.servers[:1], servs_in, [])


        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, bucket=bucket)
        self._execute_ddoc_ops("update", self.test_with_view,
                               self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)
        self._execute_ddoc_ops("delete", self.test_with_view,
                               self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_in + 1])
        max_verify = None
        if self.num_items > 500000:
            max_verify = 100000

        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")

        self._verify_all_buckets(server=self.master, timeout=self.wait_timeout * 15 if not self.dgm_run else None, max_verify=max_verify)
        self._verify_stats_all_buckets(self.servers[:self.nodes_in + 1], timeout=self.wait_timeout if not self.dgm_run else None)
        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()
        assert ClusterOperationHelper.check_if_services_obey_tls(
            servers=[self.master]), "Port binding after enforcing TLS incorrect"


    def test_n2n_encryption_enabled_rebalance_in_and_out_with_ddoc_ops(self):
        #assert if number of nodes_in and nodes_out are not sufficient
        self.assertTrue(self.num_servers > self.nodes_in + self.nodes_out,
                            "ERROR: Not enough nodes to do rebalance in and out")
        ntonencryptionBase().disable_nton_cluster([self.master])

        servs_in = self.servers[self.num_servers - self.nodes_in:]
        #subtract the servs_in from the list of servers
        servs_for_rebal = [serv for serv in self.servers if serv not in servs_in]
        servs_out = servs_for_rebal[self.num_servers - self.nodes_in - self.nodes_out:]

        x509main().setup_cluster_nodes_ssl(servs_out)
        #list of server which will be available after the in/out operation
        servs_after_rebal = [serv for serv in self.servers if serv not in servs_out]

        self.log.info("create a cluster of all the available servers except nodes_in")
        self.cluster.rebalance(servs_for_rebal[:1],
                               servs_for_rebal[1:], [])
        #ntonencryptionBase().disable_nton_cluster([self.master])
        # load initial documents
        self._load_doc_data_all_buckets()

        #start the rebalance in/out operation
        rebalance = self.cluster.async_rebalance(servs_for_rebal, servs_in, servs_out)

        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, bucket=bucket)
            if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)
        rebalance.result()
        self._wait_for_stats_all_buckets(servs_after_rebal)
        max_verify = None
        if self.num_items > 500000:
            max_verify = 100000

        encryption_result = ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        self.assertTrue(encryption_result, "Retries Exceeded. Cannot enable n2n encryption")
       
        self.cluster.async_rebalance(servs_for_rebal, servs_in, servs_out)

        self._verify_all_buckets(server=self.master, timeout=self.wait_timeout * 15 if not self.dgm_run else None, max_verify=max_verify)
        self._verify_stats_all_buckets(servs_after_rebal, timeout=self.wait_timeout if not self.dgm_run else None)
        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()
        assert ClusterOperationHelper.check_if_services_obey_tls(
            servers=[self.master]), "Port binding after enforcing TLS incorrect"
