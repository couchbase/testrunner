"""gsi_audit_event.py: These tests validate auditing of events for GSI

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "08/17/20 12:31 pm"

"""
from remote.remote_util import RemoteMachineShellConnection
from security.rbac_base import RbacBase
from security.audittest import auditTest

from .base_gsi import BaseSecondaryIndexingTests


class GSIAuditEvent(BaseSecondaryIndexingTests, auditTest):
    def setUp(self):
        super(GSIAuditEvent, self).setUp()
        self.log.info("==============  GSIAuditEvent setup has started ==============")
        self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.audit_url = "http://%s:%s/settings/audit" % (self.master.ip, self.master.port)
        self.shell = RemoteMachineShellConnection(self.master)
        curl_output = self.shell.execute_command(f"curl -u Administrator:password -X POST "
                                                 f"-d 'auditdEnabled=true' {self.audit_url}")
        if "errors" in str(curl_output):
            self.log.error("Auditing settings were not set correctly")
        self.sleep(10)
        self.log.info("==============  GSIAuditEvent setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  GSIAuditEvent tearDown has started ==============")
        super(GSIAuditEvent, self).tearDown()
        self.log.info("==============  GSIAuditEvent tearDown has completed ==============")

    def test_audit_of_forbidden_access_denied_event(self):
        # create a cluster admin user
        user = [{'id': 'test', 'password': 'password', 'name': 'test'}]
        RbacBase().create_user_source(user, 'builtin', self.master)
        user_role_list = [{'id': 'test', 'name': 'test', 'roles': 'query_manage_index[*]'}]
        RbacBase().add_user_role(user_role_list, self.rest, 'builtin')

        shell = RemoteMachineShellConnection(self.master)
        curl_cmd = 'curl -u test:password http://localhost:9102/settings/storageMode'
        try:
            shell.execute_command(curl_cmd)
        except Exception as err:
            self.log.info(err)

        expected_results = {'description': 'The user does not have permission to access the requested resource',
                            'enabled': True, 'id': 49153, 'name': 'HTTP 403: Forbidden', 'sync': False,
                            'real_userid': {'domain': 'local', 'user': 'test'},
                            'method': 'request_handler::isAllowed',
                            'service': 'Index', 'url': '/settings/storageMode',
                            'message': 'Called by RequestHandler::handleIndexStorageModeRequest'}
        self.checkConfig(self.eventID, self.master, expected_results, n1ql_audit=False)

    def test_audit_of_unauthorised_access_denied_event(self):
        # create a cluster admin user
        user = [{'id': 'test', 'password': 'password', 'name': 'test'}]
        RbacBase().create_user_source(user, 'builtin', self.master)
        user_role_list = [{'id': 'test', 'name': 'test', 'roles': 'query_manage_index[*]'}]
        RbacBase().add_user_role(user_role_list, self.rest, 'builtin')

        shell = RemoteMachineShellConnection(self.master)
        curl_cmd = 'curl -u test:wrong_password http://localhost:9102/settings/storageMode'
        try:
            shell.execute_command(curl_cmd)
        except Exception as err:
            self.log.info(err)
        expected_results = {'description': 'Authentication is required to access the requested resource',
                            'enabled': True, 'id': 49152, 'name': 'HTTP 401: Unauthorized', 'sync': False,
                            'real_userid': {'domain': 'internal', 'user': 'unknown'},
                            'method': 'request_handler::doAuth',
                            'message': 'Called by RequestHandler::handleIndexStorageModeRequest',
                            'service': 'Index', 'url': '/settings/storageMode'}
        self.checkConfig(self.eventID, self.master, expected_results, n1ql_audit=False)
