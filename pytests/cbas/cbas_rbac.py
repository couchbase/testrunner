from .cbas_base import *
from remote.remote_util import RemoteMachineShellConnection


class CBASRBACTests(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket": False})
        super(CBASRBACTests, self).setUp()

    def tearDown(self):
        super(CBASRBACTests, self).tearDown()

    def test_cbas_rbac(self):
        self.load_sample_buckets(servers=[self.master],
                                 bucketName=self.cb_bucket_name,
                                 total_items=self.travel_sample_docs_count)

        users = [{"username": "analytics_manager1",
                  "roles": "bucket_full_access[travel-sample]:analytics_manager[travel-sample]"},
                 {"username": "analytics_manager2",
                  "roles": "bucket_admin[travel-sample]:analytics_manager[travel-sample]"},
                 {"username": "analytics_manager3",
                  "roles": "analytics_manager[travel-sample]"},
                 {"username": "analytics_manager4",
                  "roles": "data_reader[travel-sample]:analytics_manager[travel-sample]"},
                 {"username": "analytics_reader1",
                  "roles": "bucket_admin[travel-sample]:analytics_reader"},
                 {"username": "analytics_reader2", "roles": "analytics_reader"},
                 {"username": "analytics_reader3",
                  "roles": "bucket_full_access[travel-sample]:analytics_reader"},
                 {"username": "analytics_reader4",
                  "roles": "data_reader[travel-sample]:analytics_reader"},
                 {"username": "ro_admin", "roles": "ro_admin"},
                 {"username": "cluster_admin", "roles": "cluster_admin"},
                 {"username": "admin", "roles": "admin"}, ]

        operation_map = [
            {"operation": "create_bucket",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager2",
                                       "analytics_manager4",
                                       "analytics_reader1", "cluster_admin",
                                       "admin"],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4", "ro_admin"]},
            {"operation": "create_dataset",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager2",
                                       "analytics_manager4",
                                       "analytics_reader1", "cluster_admin",
                                       "admin"],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4", "ro_admin"]},
            {"operation": "connect_bucket",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager2",
                                       "analytics_manager4",
                                       "analytics_reader1", "cluster_admin",
                                       "admin"],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4", "ro_admin"]},
            {"operation": "disconnect_bucket",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager2",
                                       "analytics_manager4",
                                       "analytics_reader1", "cluster_admin",
                                       "admin"],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4", "ro_admin"]},
            {"operation": "drop_dataset",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager2",
                                       "analytics_manager4",
                                       "analytics_reader1", "cluster_admin",
                                       "admin"],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4", "ro_admin"]},
            {"operation": "drop_bucket",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager2",
                                       "analytics_manager4",
                                       "analytics_reader1", "cluster_admin",
                                       "admin"],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4", "ro_admin"]},
            {"operation": "create_index",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager2",
                                       "analytics_manager4",
                                       "analytics_reader1", "cluster_admin",
                                       "admin"],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4", "ro_admin"]},
            {"operation": "drop_index",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager2",
                                       "analytics_manager4",
                                       "analytics_reader1", "cluster_admin",
                                       "admin"],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4", "ro_admin"]},
            {"operation": "execute_query",
             "should_work_for_users": ["analytics_manager3",
                                       "analytics_reader2", "ro_admin",
                                       "cluster_admin", "admin"]},
            {"operation": "execute_metadata_query",
             "should_work_for_users": ["analytics_manager3",
                                       "analytics_reader2", "ro_admin",
                                       "cluster_admin", "admin"]}]

        for user in users:
            self.log.info("Creating user %s", user["username"])
            self._create_user_and_grant_role(user["username"], user["roles"])
            self.sleep(2)

        status = True

        for operation in operation_map:
            self.log.info(
                "============ Running tests for operation %s ============",
                operation["operation"])
            for user in operation["should_work_for_users"]:
                result = self._run_operation(operation["operation"], user)
                if not result:
                    self.log.info(
                        "=== Operation {0} failed for user {1} while it should have worked".format(
                            operation["operation"], user))
                    status = False
                else:
                    self.log.info(
                        "Operation : {0}, User : {1} = Works as expected".format(
                            operation["operation"], user))
            if "should_not_work_for_users" in operation:
                for user in operation["should_not_work_for_users"]:
                    result = self._run_operation(operation["operation"], user)
                    if result:
                        self.log.info(
                            "=== Operation {0} worked for user {1} while it should not have worked".format(
                                operation["operation"], user))
                        status = False
                    else:
                        self.log.info(
                            "Operation : {0}, User : {1} = Works as expected".format(
                                operation["operation"], user))

        self.assertTrue(status,
                        "=== Some operations have failed for some users. Pls check the log above.")

    def _run_operation(self, operation, username):
        if operation:

            if operation == "create_bucket":
                status = self.create_bucket_on_cbas(self.cbas_bucket_name,
                                                    self.cb_bucket_name,
                                                    username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "create_dataset":
                self.create_bucket_on_cbas(self.cbas_bucket_name,
                                           self.cb_bucket_name)
                status = self.create_dataset_on_bucket(self.cbas_bucket_name,
                                                       self.cbas_dataset_name,
                                                       username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "connect_bucket":
                self.create_bucket_on_cbas(self.cbas_bucket_name,
                                           self.cb_bucket_name)
                self.create_dataset_on_bucket(self.cbas_bucket_name,
                                              self.cbas_dataset_name)
                status = self.connect_to_bucket(self.cbas_bucket_name,
                                                username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "disconnect_bucket":
                self.create_bucket_on_cbas(self.cbas_bucket_name,
                                           self.cb_bucket_name)
                self.create_dataset_on_bucket(self.cbas_bucket_name,
                                              self.cbas_dataset_name)
                self.connect_to_bucket(self.cbas_bucket_name)
                status = self.disconnect_from_bucket(self.cbas_bucket_name,
                                                     username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "drop_dataset":
                self.create_bucket_on_cbas(self.cbas_bucket_name,
                                           self.cb_bucket_name)
                self.create_dataset_on_bucket(self.cbas_bucket_name,
                                              self.cbas_dataset_name)
                status = self.drop_dataset(self.cbas_dataset_name,
                                           username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "drop_bucket":
                self.create_bucket_on_cbas(self.cbas_bucket_name,
                                           self.cb_bucket_name)
                status = self.drop_cbas_bucket(self.cbas_bucket_name,
                                               username=username)
                self.log.info(
                    "^^^^^^^^^^^^^^ Status of drop bucket for user {0}: {1}".format(
                        username, status))

                # Cleanup
                self.cleanup_cbas()

            elif operation == "create_index":
                self.create_bucket_on_cbas(self.cbas_bucket_name,
                                           self.cb_bucket_name)
                self.create_dataset_on_bucket(self.cbas_bucket_name,
                                              self.cbas_dataset_name)
                create_idx_statement = "create index idx1 on {0}(city:String);".format(
                    self.cbas_dataset_name)
                status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
                    create_idx_statement, username=username)
                status = False if status != "success" else True

                # Cleanup
                drop_idx_statement = "drop index {0}.idx1;".format(
                    self.cbas_dataset_name)
                self.execute_statement_on_cbas_via_rest(drop_idx_statement)
                self.cleanup_cbas()

            elif operation == "drop_index":
                self.create_bucket_on_cbas(self.cbas_bucket_name,
                                           self.cb_bucket_name)
                self.create_dataset_on_bucket(self.cbas_bucket_name,
                                              self.cbas_dataset_name)
                create_idx_statement = "create index idx1 on {0}(city:String);".format(
                    self.cbas_dataset_name)
                self.execute_statement_on_cbas_via_rest(create_idx_statement)
                self.sleep(10)
                drop_idx_statement = "drop index {0}.idx1;".format(
                    self.cbas_dataset_name)
                status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
                    drop_idx_statement, username=username)
                status = False if status != "success" else True

                # Cleanup
                drop_idx_statement = "drop index {0}.idx1;".format(
                    self.cbas_dataset_name)
                self.execute_statement_on_cbas_via_rest(drop_idx_statement)
                self.cleanup_cbas()

            elif operation == "execute_query":
                self.create_bucket_on_cbas(self.cbas_bucket_name,
                                           self.cb_bucket_name)
                self.create_dataset_on_bucket(self.cbas_bucket_name,
                                              self.cbas_dataset_name)
                self.connect_to_bucket(self.cbas_bucket_name)
                query_statement = "select count(*) from {0};".format(
                    self.cbas_dataset_name)
                status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
                    query_statement, username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "execute_metadata_query":
                query_statement = "select Name from Metadata.`Bucket`;".format(
                    self.cbas_dataset_name)
                status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
                    query_statement, username=username)

        return status

    def test_rest_api_authorization_version_api_no_authentication(self):
        api_url = "http://{0}:8095/analytics/version".format(self.cbas_node.ip)
        shell = RemoteMachineShellConnection(self.master)

        roles = ["analytics_manager[*]", "analytics_reader", "ro_admin",
                 "cluster_admin", "admin"]

        for role in roles:
            self._create_user_and_grant_role("testuser", role)

            output, error = shell.execute_command(
                """curl -i {0} 2>/dev/null | head -n 1 | cut -d$' ' -f2""".format(
                    api_url))
            response = ""
            for line in output:
                response = response + line
            response = json.loads(response)
            self.log.info(response)

            self.assertEqual(response, 200)
        shell.disconnect()

    def test_rest_api_authorization_cbas_cluster_info_api(self):
        validation_failed = False

        api_authentication = [{
            "api_url": "http://{0}:8095/analytics/cluster".format(
                self.cbas_node.ip),
            "roles": [{"role": "ro_admin",
                       "expected_status": 200},
                      {"role": "cluster_admin",
                       "expected_status": 200},
                      {"role": "admin",
                       "expected_status": 200},
                      {"role": "analytics_manager[*]",
                       "expected_status": 401},
                      {"role": "analytics_reader",
                       "expected_status": 401}]},
            {
                "api_url": "http://{0}:8095/analytics/cluster/cc".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 200},
                          {"role": "cluster_admin",
                           "expected_status": 200},
                          {"role": "admin",
                           "expected_status": 200},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}]},
            {
                "api_url": "http://{0}:8095/analytics/diagnostics".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 200},
                          {"role": "cluster_admin",
                           "expected_status": 200},
                          {"role": "admin",
                           "expected_status": 200},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}]},
            {
                "api_url": "http://{0}:8095/analytics/node/diagnostics".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 200},
                          {"role": "cluster_admin",
                           "expected_status": 200},
                          {"role": "admin",
                           "expected_status": 200},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}]},
            {
                "api_url": "http://{0}:8095/analytics/cc/config".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 401},
                          {"role": "cluster_admin",
                           "expected_status": 200},
                          {"role": "admin",
                           "expected_status": 200},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}]},
            {
                "api_url": "http://{0}:8095/analytics/node/config".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 401},
                          {"role": "cluster_admin",
                           "expected_status": 200},
                          {"role": "admin",
                           "expected_status": 200},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}]},
            {
                "api_url": "http://{0}:8095/analytics/cluster/restart".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 401},
                          {"role": "cluster_admin",
                           "expected_status": 202},
                          {"role": "admin",
                           "expected_status": 202},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}],
                "method": "POST"}

        ]

        shell = RemoteMachineShellConnection(self.master)

        for api in api_authentication:
            for role in api["roles"]:
                self._create_user_and_grant_role("testuser", role["role"])
                self.sleep(5)

                if "method" in api:
                    output, error = shell.execute_command(
                        """curl -i {0} -X {1} -u {2}:{3} 2>/dev/null | head -n 1 | cut -d$' ' -f2""".format(
                            api["api_url"], api["method"], "testuser",
                            "password"))
                else:
                    output, error = shell.execute_command(
                        """curl -i {0} -u {1}:{2} 2>/dev/null | head -n 1 | cut -d$' ' -f2""".format(
                            api["api_url"], "testuser", "password"))
                response = ""
                for line in output:
                    response = response + line
                response = json.loads(response)

                if response != role["expected_status"]:
                    self.log.info(
                        "Error accessing {0} as user with {1} role. Response = {2}".format(
                            api["api_url"], role["role"], response))
                    validation_failed = True
                else:
                    self.log.info(
                        "Accessing {0} as user with {1} role worked as expected".format(
                        api["api_url"], role["role"]))

                self._drop_user("testuser")

        shell.disconnect()

        self.assertFalse(validation_failed,
                         "Authentication errors with some APIs. Check the test log above.")
