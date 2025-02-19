import copy

from lib.couchbase_helper.stats_tools import StatsCommon
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.couchbase_helper.documentgenerator import JSONNonDocGenerator, BlobGenerator
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_ONDEPLOY
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.security.ntonencryptionBase import ntonencryptionBase
from lib.membase.helper.cluster_helper import ClusterOperationHelper
import logging
import json

log = logging.getLogger()


class EventingSecurity(EventingBaseTest):
    def setUp(self):
        super(EventingSecurity, self).setUp()
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = "handler_code/ABO/insert_rebalance.js"
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'ondeploy_test':
            self.handler_code = HANDLER_CODE_ONDEPLOY.ONDEPLOY_TLS_N2N

    def tearDown(self):
        super(EventingSecurity, self).tearDown()

    '''
    Test steps -
    1. Disable n2n encryption.
    2. Create and deploy handler, load docs into src bucket and verify mutations are processed or not.
    3. Pause/undeploy handler, enable n2n encryption, deploy/resume handler, delete docs from src bucket and verify mutations are processed or not.
    4. Pause/undeploy handler, change encryption level to all, deploy/resume handler, load docs into src bucket and verify mutations are processed or not.
    5. Pause/undeploy handler, disable n2n encryption, deploy/resume handler, delete docs from src bucket and verify mutations are processed or not.
    '''
    def test_eventing_with_n2n_encryption_enabled(self):
        ntonencryptionBase().disable_nton_cluster([self.master])
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0", is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel="all")
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().disable_nton_cluster([self.master])
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        self.undeploy_and_delete_function(body)

    '''
    Test steps -
    1. Disable enforce tls, n2n encryption.
    2. Create and deploy handler, load docs into src bucket and verify mutations are processed or not.
    3. Pause/undeploy handler, enable n2n encryption, deploy/resume handler, load docs into src bucket and verify mutations are processed or not.
    4. Pause/undeploy handler, enforce tls, deploy/resume handler, load docs into src bucket and verify mutations are processed or not.
    5. Verify that all processes bind to ssl ports only after enforcing tls.
    6. Pause/undeploy handler, disable enforce tls by changing encryption level to control, load docs into src bucket and verify mutations are processed or not.
    7. Pause/undeploy handler, disable n2n encryption, deploy/resume handler, delete docs from src bucket and verify mutations are processed or not.
    '''
    def test_eventing_with_enforce_tls_feature(self):
        ntonencryptionBase().disable_nton_cluster([self.master])
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        assert ClusterOperationHelper.check_if_services_obey_tls(servers=[self.master]), "Port binding after enforcing TLS incorrect"
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().disable_nton_cluster([self.master])
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    '''
    Test steps -
    1. Disable enforce tls.
    2. Create and deploy handler, load docs into src bucket and verify mutations are processed or not.
    3. Enforce tls, delete docs from src bucket and verify mutations are not processed.
    4. Pause/undeploy handler, resume/deploy handler and verify that mutations are processed.
    5. Verify that all processes bind to ssl ports only after enforcing tls.
    6. Pause/undeploy handler, disable enforce tls and n2n encryption.
    7. Resume/deploy handler, load docs into src bucket and verify mutations are processed or not.
    '''
    # MB-47707
    def test_verify_mutations_are_not_processed_after_enforce_tls_and_before_any_lifecycle_operation(self):
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        assert ClusterOperationHelper.check_if_services_obey_tls(servers=[self.master]), "Port binding after enforcing TLS incorrect"
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        ntonencryptionBase().disable_nton_cluster([self.master])
        if self.pause_resume:
            self.resume_function(body)
        else:
            self.deploy_function(body)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)

    '''
    Test steps -
    1. Disable enforce tls.
    2. Create and deploy handler, load docs into src bucket and verify mutations are processed or not.
    3. Pause/undeploy handler, resume/deploy handler and while handler is undergoing lifecycle operation enforce tls.
    4. Validate that handler isn't stuck in deploying state.
    5. Delete docs from src bucket and  verify mutations are processed or not.
    6. Verify that all processes bind to ssl ports only after enforcing tls.
    '''
    # MB-47946
    def test_enforcing_tls_during_handler_lifecycle_operation(self):
        ntonencryptionBase().setup_nton_cluster(self.servers, clusterEncryptionLevel=self.ntonencrypt_level)
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        if self.pause_resume:
            self.pause_function(body)
        else:
            self.undeploy_function(body)
        if self.pause_resume:
            self.resume_function(body, wait_for_resume=False)
        else:
            self.deploy_function(body, wait_for_bootstrap=False)
        ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
        self.wait_for_handler_state(body['appname'], "deployed")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
        self.verify_doc_count_collections("default.scope0.collection1", 0)
        assert ClusterOperationHelper.check_if_services_obey_tls(servers=[self.master]), "Port binding after enforcing TLS incorrect"
        self.undeploy_and_delete_function(body)

    '''
    Test steps -
    1. Disable n2n encryption.
    2. Create and deploy handler, load docs into src bucket and verify mutations are processed or not.
    3. Pause/undeploy handler, enable n2n encryption with encryption level control and rebalance in eventing node.
    4. Resume/deploy handler, delete docs from src bucket and verify mutations are processed or not.
    5. Rebalance out eventing node, load docs into src bucket and verify mutations are processed or not.
    6. Repeat steps 3-5 for encryption level all and strict.
    '''
    def test_eventing_rebalance_with_n2n_encryption_and_enforce_tls(self):
        ntonencryptionBase().disable_nton_cluster([self.master])
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_rebalance.js")
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        for level in ["control", "all", "strict"]:
            if self.pause_resume:
                self.pause_function(body)
            else:
                self.undeploy_function(body)
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel=level)
            if self.x509enable:
                self.upload_x509_certs(self.servers[self.nodes_init])
            services_in = ["eventing"]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [],
                                                     services=services_in)
            reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            if self.pause_resume:
                self.resume_function(body)
            else:
                self.deploy_function(body)
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0",is_delete=True)
            self.verify_doc_count_collections("default.scope0.collection1", 0)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [self.servers[self.nodes_init]])
            reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
            self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)
