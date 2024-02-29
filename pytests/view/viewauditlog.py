from multiprocessing import Event

from lib.couchbase_helper.document import View
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.security.auditmain import audit
from pytests.security.rbac_base import RbacBase
from pytests.view import createdeleteview
import logging

log = logging.getLogger()

class ViewAuditLog(createdeleteview.CreateDeleteViewTests):
    def setUp(self):
        try:
            super(ViewAuditLog, self).setUp()
            self.rest = RestConnection(self.master)
            self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=1500)
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
            auditing = audit(host=self.master)
            log.info("Enabling Audit")
            auditing.setAuditEnable('true')
            self.sleep(30)
        except Exception as ex:
            self.input.test_params["stop-on-failure"] = True
            self.log.error("SETUP WAS FAILED. ALL TESTS WILL BE SKIPPED")
            self.fail(ex)

    def tearDown(self):
        super(ViewAuditLog, self).tearDown()

    def check_config(self, event_id, host, expected_results):
        auditing = audit(eventID=event_id, host=host)
        _, value_verification = auditing.validateEvents(expected_results)
        self.assertTrue(value_verification, "Values for one of the fields is not matching")

    def test_audit_event_for_authentication_failure_and_authorization_failure(self):

        self._load_doc_data_all_buckets()
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc,
                                   bucket=bucket)

        self._wait_for_stats_all_buckets([self.master])
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

        shell = RemoteMachineShellConnection(self.master)
        command = "curl --version | awk 'NR==1{print $2}'"
        result = shell.execute_command(command)

        # By default views audit logs are not enabled, Run below curl command to enable all view audit events
        shell.execute_command("curl -v  POST -u Administrator:password http://localhost:8091/settings/audit -d auditdEnabled=true -d disabled=8255,20485,20488,20489,20490,20491,20493,28672,28673,28674,28675,28676,28677,28678,28679,28680,28681,28682,28683,28684,28685,28686,28687,28688,28689,28690,28691,28692,28693,28694,28695,28697,28698,28699,28700,28701,28702,28704,28705,28706,28707,28708,28709,28710,28711,28712,28713,28714,28715,28716,28717,28718,28719,28720,28721,28722,28723,28724,28725,28726,28727,28728,32770,32771,32772,32780,32783,32784,32785,32786,36867,36868,36869,36870,36871,36872,36873,36877,36878,36879,36880,45057,45061,45066,45070")

        # audit event for authentication failure
        shell.execute_command("curl -s -XGET http://Administrator:wrongpassword@localhost:8092/default/_design/dev_ddoc0/_view/views0")
        expected_results_authentication_failure = {
                                                     "auth":"Administrator",
                                                     "error":"unauthorized",
                                                     "user_agent": "curl/"+result[0][0],
                                                     "id": 40966,
                                                     "name": "Access denied",
                                                     "description": "Access denied to the REST API due to invalid permissions or credentials",
                                                     "method": "GET",
                                                     "url": "/default/_design/dev_ddoc0/_view/views0"
                                                   }

        self.check_config(40966, self.master, expected_results_authentication_failure)

        # create a cluster admin user
        user = [{'id': 'test', 'password': 'password', 'name': 'test'}]
        RbacBase().create_user_source(user, 'builtin', self.master)
        user_role_list = [{'id': 'test', 'name': 'test', 'roles': 'cluster_admin'}]
        RbacBase().add_user_role(user_role_list, self.rest, 'builtin')

        # audit event for authorisation failure
        shell.execute_command("curl -s -XGET  http://test:password@localhost:8092/default/_design/dev_ddoc0/_view/views0")
        expected_results_authorization_failure = {
                                                     "auth": "test",
                                                     "error": "forbidden",
                                                     "user_agent": "curl/"+result[0][0],
                                                     "id": 40966,
                                                     "name": "Access denied",
                                                     "description": "Access denied to the REST API due to invalid permissions or credentials",
                                                     "method": "GET",
                                                     "url": "/default/_design/dev_ddoc0/_view/views0"
                                                  }
        self.check_config(40966, self.master, expected_results_authorization_failure)
        shell.disconnect()
