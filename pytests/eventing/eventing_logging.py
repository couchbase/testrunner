from pytests.eventing.eventing_base import EventingBaseTest
from logredaction.log_redaction_base import LogRedactionBase
from pytests.security.auditmain import audit
from lib.testconstants import STANDARD_BUCKET_PORT
import logging
import copy
import json
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.memcached.helper.data_helper import MemcachedClientHelper
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.security.rbac_base import RbacBase

log = logging.getLogger()


class EventingLogging(EventingBaseTest, LogRedactionBase):
    def setUp(self):
        super(EventingLogging, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=1500)
        if self.create_functions_buckets:
            self.bucket_size = 100
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=self.num_replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3
        auditing = audit(host=self.master)
        log.info("Enabling Audit")
        auditing.setAuditEnable('true')
        self.sleep(30)
        if self.non_default_collection:
            self.create_scope_collection(bucket=self.src_bucket_name,scope=self.src_bucket_name,collection=self.src_bucket_name)
            self.create_scope_collection(bucket=self.metadata_bucket_name,scope=self.metadata_bucket_name,collection=self.metadata_bucket_name)
            self.create_scope_collection(bucket=self.dst_bucket_name,scope=self.dst_bucket_name,collection=self.dst_bucket_name)
            expected_results_create_collection = {"real_userid:source": "builtin", "real_userid:user": "Administrator",
                                       "id": 8261, "name": "create collection","description": "Collection was created",
                                       "bucket_name": self.dst_bucket_name,"collection_name": self.dst_bucket_name,
                                       "new_manifest_uid": RestConnection(self.master).get_bucket_manifest(self.dst_bucket_name)['uid'],
                                       "scope_name": self.dst_bucket_name}
            self.check_config(8261, self.master, expected_results_create_collection)

    def tearDown(self):
        super(EventingLogging, self).tearDown()

    def check_config(self, event_id, host, expected_results):
        auditing = audit(eventID=event_id, host=host)
        _, value_verification = auditing.validateEvents(expected_results)
        self.assertTrue(value_verification, "Values for one of the fields is not matching")

    def test_eventing_audit_logging(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        expected_results_deploy = {"real_userid:source": "builtin", "real_userid:user": "Administrator",
                                   "context": self.function_name, "id": 32768, "name": "Create Function",
                                   "description": "Eventing function definition was created or updated"}
        # check audit log if the deploy operation is present in audit log
        self.check_config(32768, eventing_node, expected_results_deploy)
        # Wait for eventing to catch up with all the create mutations and verify results
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)
        expected_results_undeploy = {"real_userid:source": "builtin", "real_userid:user": "Administrator",
                                     "context": self.function_name, "id": 32779, "name": "Set Settings",
                                     "description": "Save settings for a given app"}
        expected_results_delete_draft = {"real_userid:source": "builtin", "real_userid:user": "Administrator",
                                         "context": self.function_name, "id": 32773, "name": "Delete Drafts",
                                         "description": "Eventing function draft definitions were deleted"}
        expected_results_delete = {"real_userid:source": "builtin", "real_userid:user": "Administrator",
                                   "context": self.function_name, "id": 32769, "name": "Delete Function",
                                   "description": "Eventing function definition was deleted"}
        # check audit log if the un deploy operation is present in audit log
        self.check_config(32779, eventing_node, expected_results_undeploy)
        # check audit log if the delete operation is present in audit log
        self.check_config(32773, eventing_node, expected_results_delete_draft)
        self.check_config(32769, eventing_node, expected_results_delete)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(60)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_with_log_redaction(self):
        self.log_redaction_level = self.input.param("redaction_level", "partial")
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        log.info("eventing_node : {0}".format(eventing_node))
        if self.non_default_collection:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        else:
            self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        if self.non_default_collection:
            self.verify_doc_count_collections("src_bucket.src_bucket.src_bucket", self.docs_per_day * self.num_docs)
        else:
            self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
        self.log.info("cb collect result: {}".format(result))
        node = "ns_1@"+eventing_node.ip
        if result["perNode"][node]["path"] == "failed":
            raise Exception("log collection failed")
        logs_path = result["perNode"][node]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/') + 1]
        log.info("redactFileName : {0}".format(redactFileName))
        log.info("nonredactFileName : {0}".format(nonredactFileName))
        log.info("remotepath : {0}".format(remotepath))
        self.sleep(120)
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
        self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.eventing.log")


    def test_log_rotation(self):
        self.load_sample_buckets(self.server, "travel-sample")
        self.src_bucket_name="travel-sample"
        body = self.create_save_function_body(self.function_name, "handler_code/logger.js")
        body['settings']['app_log_max_size']=3768300
        self.rest.create_function(body['appname'], body)
        # deploy a function without any alias
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", 31591)
        number=self.check_number_of_files()
        if number ==1:
            raise Exception("Files not rotated")
        matched, count=self.check_word_count_eventing_log(self.function_name,"docId:",31591)
        self.skip_metabucket_check = True
        if not matched:
            raise Exception("Not all data logged in file")

    def test_audit_event_for_authentication_failure_and_authorization_failure(self):
        # create a cluster admin user
        user = [{'id': 'test', 'password': 'password', 'name': 'test'}]
        RbacBase().create_user_source(user, 'builtin', self.master)
        user_role_list = [{'id': 'test', 'name': 'test', 'roles': 'cluster_admin'}]
        RbacBase().add_user_role(user_role_list, self.rest, 'builtin')
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        shell = RemoteMachineShellConnection(eventing_node)
        # audit event for authentication failure
        shell.execute_command("curl -s -XGET http://Administrator:wrongpassword@localhost:8096/api/v1/stats")
        expected_results_authentication_failure = {"real_userid:source": "internal", "real_userid:user": "unknown",
                                   "context": "<nil>", "id": 32787, "name": "Authentication Failure",
                                   "description": "Authentication failed", "method": "GET", "url": "/api/v1/stats"}
        self.check_config(32787, eventing_node, expected_results_authentication_failure)
        # audit event for authorisation failure
        shell.execute_command("curl -s -XGET http://test:password@localhost:8096/api/v1/stats")
        expected_results_authorization_failure = {"real_userid:source": "local", "real_userid:user": "test",
                                   "context": "<nil>", "id": 32788, "name": "Authorization Failure",
                                   "description": "Authorization failed", "method": "GET", "url": "/api/v1/stats"}
        self.check_config(32788, eventing_node, expected_results_authorization_failure)
        shell.disconnect()
        self.undeploy_and_delete_function(body)
