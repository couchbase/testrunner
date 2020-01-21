from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
from security.rbac_base import RbacBase
import logging

log = logging.getLogger()


class EventingRBAC(EventingBaseTest):
    def setUp(self):
        super(EventingRBAC, self).setUp()
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
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'bucket_op_with_cron_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMERS
        elif handler_code == 'n1ql_op_with_timers':
            # index is required for delete operation through n1ql
            self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.n1ql_helper = N1QLHelper(shell=self.shell,
                                          max_verify=self.max_verify,
                                          buckets=self.buckets,
                                          item_flag=self.item_flag,
                                          n1ql_port=self.n1ql_port,
                                          full_docs_list=self.full_docs_list,
                                          log=self.log, input=self.input,
                                          master=self.master,
                                          use_rest=True
                                          )
            self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITH_TIMERS
        else:
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE

    def tearDown(self):
        super(EventingRBAC, self).tearDown()

    def test_eventing_with_read_only_user(self):
        # create a read only admin user
        user = [{'id': 'ro_admin', 'password': 'password', 'name': 'Read Only Admin'}]
        RbacBase().create_user_source(user, 'builtin', self.master)
        user_role_list = [{'id': 'ro_admin', 'name': 'Read Only Admin', 'roles': 'ro_admin'}]
        RbacBase().add_user_role(user_role_list, self.rest, 'builtin')
        body = self.create_save_function_body(self.function_name, self.handler_code)
        body['settings']['rbacpass'] = 'password'
        body['settings']['rbacrole'] = 'ro_admin'
        body['settings']['rbacuser'] = 'ro_admin'
        try:
            # deploy the function with ro_admin user
            self.deploy_function(body)
        except Exception as ex:
            if "did not successfully bootstrap" not in str(ex):
                self.fail("Function deploy succeeded even when function was using read only credentials")

    def test_eventing_with_cluster_admin_user(self):
        # create a cluster admin user
        user = [{'id': 'cluster_admin', 'password': 'password', 'name': 'Cluster Admin'}]
        RbacBase().create_user_source(user, 'builtin', self.master)
        user_role_list = [{'id': 'cluster_admin', 'name': 'Cluster Admin', 'roles': 'cluster_admin'}]
        RbacBase().add_user_role(user_role_list, self.rest, 'builtin')
        body = self.create_save_function_body(self.function_name, self.handler_code)
        body['settings']['rbacpass'] = 'password'
        body['settings']['rbacrole'] = 'cluster_admin'
        body['settings']['rbacuser'] = 'cluster_admin'
        # Deploy the function with cluster admin user
        self.deploy_function(body, wait_for_bootstrap=False)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # Wait for bootstrap to complete
        self.wait_for_handler_state(body['appname'], "deployed")
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
