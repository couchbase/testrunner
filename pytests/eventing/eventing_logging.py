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
        auditing = audit(host=self.master)
        log.info("Enabling Audit")
        auditing.setAuditEnable('true')
        self.sleep(30)

    def tearDown(self):
        super(EventingLogging, self).tearDown()

    def check_config(self, event_id, host, expected_results):
        auditing = audit(eventID=event_id, host=host)
        _, value_verification = auditing.validateEvents(expected_results)
        self.assertTrue(value_verification, "Values for one of the fields is not matching")

    def test_eventing_audit_logging(self):
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        expected_results_deploy = {"real_userid:source": "builtin", "real_userid:user": "Administrator",
                                   "context": "Create Function", "id": 32768, "name": "Create Function",
                                   "description": "Request to create or update eventing function definition",
                                   "method": "POST", "url": "/api/v1/functions/" + self.function_name,
                                   "error": None}
        # check audit log if the deploy operation is present in audit log
        self.check_config(32768, eventing_node, expected_results_deploy)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs)
        self.undeploy_and_delete_function(body)
        expected_results_undeploy = {"real_userid:source": "builtin", "real_userid:user": "Administrator",
                                     "context": "Undeploy function", "id": 32790, "name": "Undeploy Function",
                                     "description": "Request to undeploy eventing function",
                                     "method": "POST", "url": "/api/v1/functions/" + self.function_name + "/undeploy",
                                     "error": None, "request": None}
        expected_results_delete = {"real_userid:source": "builtin", "real_userid:user": "Administrator",
                                   "context": "Delete Function", "id": 32769, "name": "Delete Function",
                                   "description": "Request to delete eventing function definition",
                                   "method": "DELETE", "url": "/api/v1/functions/" + self.function_name,
                                   "error": None, "request": None}
        # check audit log if the un deploy operation is present in audit log
        self.check_config(32790, eventing_node, expected_results_undeploy)
        # check audit log if the delete operation is present in audit log
        self.check_config(32769, eventing_node, expected_results_delete)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(60)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_with_log_redaction(self):
        self.log_redaction_level = self.input.param("redaction_level", "partial")
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        log.info("eventing_node : {0}".format(eventing_node))
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection0", self.docs_per_day * self.num_docs)
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
        self.function_scope = {"bucket": self.src_bucket_name, "scope": "_default"}
        body = self.create_save_function_body(self.function_name, "handler_code/logger.js")
        body['settings']['app_log_max_size']=3768300
        self.rest.create_function(body['appname'], body, self.function_scope)
        # deploy a function without any alias
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", 31591)
        number=self.check_number_of_files(self.src_bucket_name, self.function_scope["scope"], self.function_name)
        if number ==1:
            raise Exception("Files not rotated")
        matched, count=self.check_word_count_eventing_log(self.function_name, "docId:", 31591, False, self.src_bucket_name, self.function_scope["scope"])
        self.skip_metabucket_check = True
        if not matched:
            raise Exception("Not all data logged in file")

    def test_audit_event_for_authentication_failure_and_authorization_failure(self):
        # create a cluster admin user
        user = [{'id': 'test', 'password': 'password', 'name': 'test'}]
        RbacBase().create_user_source(user, 'builtin', self.master)
        user_role_list = [{'id': 'test', 'name': 'test', 'roles': 'views_admin[*]'}]
        RbacBase().add_user_role(user_role_list, self.rest, 'builtin')
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        self.verify_doc_count_collections("default.scope0.collection1", self.docs_per_day * self.num_docs)
        eventing_node = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=False)
        shell = RemoteMachineShellConnection(eventing_node)
        # audit event for authentication failure
        shell.execute_command("curl -s -XGET http://Administrator:wrongpassword@localhost:8096/api/v1/stats")
        expected_results_authentication_failure = {"real_userid:source": "internal", "real_userid:user": "unknown",
                                   "context": "<nil>", "id": 32787, "name": "Authentication Failure",
                                   "description": "Authentication failed", "method": "GET", "url": "/api/v1/stats"}
        self.check_config(32787, eventing_node, expected_results_authentication_failure)
        # audit event for authorisation failure
        shell.execute_command("curl -s -XGET http://test:password@localhost:8096/api/v1/config")
        expected_results_authorization_failure = {"real_userid:source": "local", "real_userid:user": "test",
                                   "context": "<nil>", "id": 32788, "name": "Authorization Failure",
                                   "description": "Authorization failed", "method": "GET", "url": "/api/v1/config"}
        self.check_config(32788, eventing_node, expected_results_authorization_failure)
        shell.disconnect()
        self.undeploy_and_delete_function(body)
