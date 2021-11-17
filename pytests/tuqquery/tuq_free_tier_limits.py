from .tuq import QueryTests
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from security.rbac_base import RbacBase
from deepdiff import DeepDiff
import threading
import time
import json

class QueryFreeLimits(QueryTests):
    def setUp(self):
        super(QueryFreeLimits, self).setUp()
        self.log.info("==============  QueryFreeLimits setup has started ==============")
        self.shell = RemoteMachineShellConnection(self.master)
        self.info = self.shell.extract_remote_info()
        if self.info.type.lower() == 'windows':
            self.curl_path = f"{self.path}curl"
        else:
            self.curl_path = "curl"
        self.rest = RestConnection(self.master)
        self.query_service_url = f"'http://{self.master.ip}:{self.n1ql_port}/query/service'"
        self.api_port = self.input.param("api_port", 8094)
        self.run_cbq_query('delete from system:prepareds')
        self.enforce_limits = self.input.param("enforce_limits", True)
        self.expected_error = self.input.param("expected_error", False)
        self.crash = self.input.param("crash", True)
        # Create the users necessary for the RBAC tests in curl
        self.log.info("==============  Creating Users ==============")
        self.delete_user("limited1")
        self.delete_user("limited2")
        testuser = [{'id': 'limited1', 'name': 'limited1', 'password': 'password'},
                    {'id': 'limited2', 'name': 'limited2', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)
        full_permissions = 'bucket_full_access[*]:query_select[*]:query_update[*]:' \
                           'query_insert[*]:query_delete[*]:query_manage_index[*]:' \
                           'query_system_catalog:query_external_access:query_execute_global_external_functions'
        # Assign user to role
        role_list = [
            {'id': 'limited1', 'name': 'limited1', 'roles': f'{full_permissions}', 'password': 'password'},
            {'id': 'limited2', 'name': 'limited2', 'roles': f'{full_permissions}', 'password': 'password'}]
        RbacBase().add_user_role(role_list, self.rest, 'builtin')
        self.cbqpath_user1 = f'{self.path}cbq -e {self.master.ip}:{self.n1ql_port} -q -u "limited1" -p {self.rest.password}'
        self.cbqpath_user2 = f'{self.path}cbq -e {self.master.ip}:{self.n1ql_port} -q -u "limited2" -p {self.rest.password}'
        self.log.info("==============  Users Created ==============")
        self.log.info("==============  QueryFreeLimits setup has completed ==============")
        self.log_config_info()
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        self.query_bucket = self.query_buckets[0]

    def suite_setUp(self):
        super(QueryFreeLimits, self).suite_setUp()
        self.log.info("==============  QueryFreeLimits suite_setup has started ==============")
        functions = 'function sleep(delay) { var start = new Date().getTime(); while (new Date().getTime() < start + delay); return delay; }'
        function_names = ["sleep"]
        self.create_library("sleep", functions, function_names)
        self.run_cbq_query(query="CREATE OR REPLACE FUNCTION sleep(t) LANGUAGE JAVASCRIPT AS \"sleep\" AT \"sleep\"")
        self.log.info("==============  QueryFreeLimits suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryFreeLimits tearDown has started ==============")
        self.log.info("==============  QueryFreeLimits tearDown has completed ==============")
        super(QueryFreeLimits, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryFreeLimits suite_tearDown has started ==============")
        self.log.info("==============  QueryFreeLimits suite_tearDown has completed ==============")
        super(QueryFreeLimits, self).suite_tearDown()

    def test_set_limits(self):
        limits_user_1 = '{"query":{"num_concurrent_requests": 2, "num_queries_per_min": 5, "ingress_mib_per_min": 10, "egress_mib_per_min": 10}}'
        self.set_query_limits(username="limited1", limits=limits_user_1)
        limits_user_2 = '{"query":{"num_concurrent_requests": 1, "num_queries_per_min": 3, "ingress_mib_per_min": 12, "egress_mib_per_min": 14}}'
        self.set_query_limits(username="limited2", limits=limits_user_2)

        curl_output = self.shell.execute_command(f"{self.curl_path} -X GET -u {self.rest.username}:{self.rest.password} http://{self.master.ip}:{self.master.port}/settings/rbac/users/local/limited1")
        self.log.info(curl_output)
        expected_settings = json.loads(limits_user_1)
        actual_settings = self.convert_list_to_json(curl_output[0])
        diffs = DeepDiff(actual_settings['limits'], expected_settings, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        curl_output = self.shell.execute_command(f"{self.curl_path} -X GET -u {self.rest.username}:{self.rest.password} http://{self.master.ip}:{self.master.port}/settings/rbac/users/local/limited2")
        self.log.info(curl_output)
        expected_settings = json.loads(limits_user_2)
        actual_settings = self.convert_list_to_json(curl_output[0])
        diffs = DeepDiff(actual_settings['limits'], expected_settings, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_concurrent_queries_negative(self):
        limits_user_1 = '{"query":{"num_concurrent_requests": -1}}'
        self.set_query_limits(username="limited1", limits=limits_user_1)
        limits_user_2 = '{"query":{"num_concurrent_requests": 0}}'
        self.set_query_limits(username="limited2", limits=limits_user_2)

    def test_enforce_limits_negative(self):
        limits_user_1 = '{"query":{"num_concurrent_requests": 2}}'
        self.set_query_limits(username="limited1", limits=limits_user_1)
        query = "EXECUTE FUNCTION sleep(10000)"

        # Run 2 queries for user 1 and 2 queries for user 2 in a different thread
        t51 = threading.Thread(name='run_first', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t51.start()
        t52 = threading.Thread(name='run_second', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t52.start()
        time.sleep(1)
        # User 1 should be allowed to run a query because enforce limits is off
        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        self.log.info(cbq_user1)
        results = self.convert_list_to_json(cbq_user1)
        self.assertEqual(results['status'], "success")
        self.assertEqual(results['results'], [10000])

    '''Both users running queries at the same time should not interfere with each other, and each should have their own limits'''
    def test_concurrent_queries(self):
        limits_user_1 = '{"query":{"num_concurrent_requests": 2}}'
        self.set_query_limits(username="limited1", limits=limits_user_1)
        limits_user_2 = '{"query":{"num_concurrent_requests": 3}}'
        self.set_query_limits(username="limited2", limits=limits_user_2)
        query = "EXECUTE FUNCTION sleep(10000)"

        # Run 2 queries for user 1 and 2 queries for user 2 in a different thread
        t51 = threading.Thread(name='run_first', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t51.start()
        t52 = threading.Thread(name='run_second', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t52.start()
        t61 = threading.Thread(name='run_fourth', target=self.run_dummy_query_thread, args=("limited2", 20000))
        t61.start()
        t62 = threading.Thread(name='run_fifth', target=self.run_dummy_query_thread, args=("limited2", 20000))
        t62.start()
        time.sleep(1)
        # User 1 should not be allowed to run another query
        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        self.log.info(cbq_user1)
        self.assertTrue("429 Too Many Requests" in cbq_user1, f"The statement should have error'd {cbq_user1}")
        # User 2 should be allowed to run another query
        cbq_user2 = self.execute_commands_inside(self.cbqpath_user2, query, '', '', '', '', '')
        self.log.info(cbq_user2)
        results = self.convert_list_to_json(cbq_user2)
        self.assertEqual(results['status'], "success")
        self.assertEqual(results['results'], [10000])
        # After a query finishes, we should be able to run another query
        t51.join()
        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        results = self.convert_list_to_json(cbq_user1)
        self.assertEqual(results['status'], "success")
        self.assertEqual(results['results'], [10000])

        # Unset limits on user one and try to exceed the limit
        limits_user_1 = '{}'
        self.set_query_limits(username="limited1", limits=limits_user_1, skip_verify=True)

        t71 = threading.Thread(name='run_sixth', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t71.start()
        t72 = threading.Thread(name='run_seventh', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t72.start()

        time.sleep(1)
        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        results = self.convert_list_to_json(cbq_user1)
        self.assertEqual(results['status'], "success")
        self.assertEqual(results['results'], [10000])

        if self.crash:
            limits_user_1 = '{"query":{"num_concurrent_requests": 2}}'
            self.set_query_limits(username="limited1", limits=limits_user_1)
            try:
                remote = RemoteMachineShellConnection(self.master)
                remote.stop_server()
            finally:
                remote = RemoteMachineShellConnection(self.master)
                remote.start_server()

            time.sleep(30)

            curl_output = self.shell.execute_command(f"{self.curl_path} -X GET -u {self.rest.username}:{self.rest.password} http://{self.master.ip}:{self.master.port}/settings/rbac/users/local/limited1")
            self.log.info(curl_output)
            curl_output = self.shell.execute_command(f"{self.curl_path} -X GET -u {self.rest.username}:{self.rest.password} http://{self.master.ip}:{self.master.port}/settings/rbac/users/local/limited2")
            self.log.info(curl_output)

            # Run 2 queries for user 1 and 2 queries for user 2 in a different thread
            t51 = threading.Thread(name='run_first', target=self.run_dummy_query_thread, args=("limited1", 20000))
            t51.start()
            t52 = threading.Thread(name='run_second', target=self.run_dummy_query_thread, args=("limited1", 20000))
            t52.start()
            t61 = threading.Thread(name='run_fourth', target=self.run_dummy_query_thread, args=("limited2", 20000))
            t61.start()
            t62 = threading.Thread(name='run_fifth', target=self.run_dummy_query_thread, args=("limited2", 20000))
            t62.start()
            time.sleep(1)
            # User 1 should not be allowed to run another query
            cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
            self.log.info(cbq_user1)
            self.assertTrue("429 Too Many Requests" in cbq_user1,
                            f"The statement should have error'd {cbq_user1}")
            # User 2 should be allowed to run another query
            cbq_user2 = self.execute_commands_inside(self.cbqpath_user2, query, '', '', '', '', '')
            self.log.info(cbq_user2)
            results = self.convert_list_to_json(cbq_user2)
            self.assertEqual(results['status'], "success")
            self.assertEqual(results['results'], [10000])
            # After a query finishes, we should be able to run another query
            t51.join()
            cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
            results = self.convert_list_to_json(cbq_user1)
            self.assertEqual(results['status'], "success")
            self.assertEqual(results['results'], [10000])

            # Unset limits on user one and try to exceed the limit
            limits_user_1 = '{}'
            self.set_query_limits(username="limited1", limits=limits_user_1, skip_verify=True)

            t71 = threading.Thread(name='run_sixth', target=self.run_dummy_query_thread, args=("limited1", 20000))
            t71.start()
            t72 = threading.Thread(name='run_seventh', target=self.run_dummy_query_thread, args=("limited1", 20000))
            t72.start()

            time.sleep(1)
            cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
            results = self.convert_list_to_json(cbq_user1)
            self.assertEqual(results['status'], "success")
            self.assertEqual(results['results'], [10000])


    '''Set num_queries_per_min'''
    def test_queries_per_min(self):
        limits_user_1 = '{"query":{"num_queries_per_min": 50}}'
        self.set_query_limits(username="limited1", limits=limits_user_1)
        limits_user_2 = '{"query":{"num_queries_per_min": 60}}'
        self.set_query_limits(username="limited2", limits=limits_user_2)
        query = "EXECUTE FUNCTION sleep(10000)"

        # Run 2 queries for user 1 and 2 queries for user 2 in a different thread
        t51 = threading.Thread(name='run_first', target=self.run_dummy_query_thread, args=("limited1", 30000))
        t51.start()
        time.sleep(1)
        t52 = threading.Thread(name='run_second', target=self.run_dummy_query_thread, args=("limited1", 30000))
        t52.start()
        t61 = threading.Thread(name='run_fourth', target=self.run_dummy_query_thread, args=("limited2", 30000))
        t61.start()
        # User 1 should not be allowed to run another query
        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        self.log.info(cbq_user1)
        self.assertTrue("429 Too Many Requests" in cbq_user1, f"The statement should have error'd please check it {cbq_user1}")
        # User 2 should be allowed to run another query
        time.sleep(1)
        cbq_user2 = self.execute_commands_inside(self.cbqpath_user2, query, '', '', '', '', '')
        self.log.info(cbq_user2)
        results = self.convert_list_to_json(cbq_user2)
        self.assertEqual(results['status'], "success")
        self.assertEqual(results['results'], [10000])

        # In a minute we should be able to execute more queries
        time.sleep(60)
        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        results = self.convert_list_to_json(cbq_user1)
        self.assertEqual(results['status'], "success")
        self.assertEqual(results['results'], [10000])

        # Unset limits on user one and try to exceed the limit
        limits_user_1 = '{}'
        self.set_query_limits(username="limited1", limits=limits_user_1, skip_verify=True)

        t71 = threading.Thread(name='run_sixth', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t71.start()
        t72 = threading.Thread(name='run_seventh', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t72.start()

        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        results = self.convert_list_to_json(cbq_user1)
        self.assertEqual(results['status'], "success")
        self.assertEqual(results['results'], [10000])

        if self.crash:
            limits_user_1 = '{"query":{"num_queries_per_min": 50}}'
            self.set_query_limits(username="limited1", limits=limits_user_1)
            try:
                remote = RemoteMachineShellConnection(self.master)
                remote.stop_server()
            finally:
                remote = RemoteMachineShellConnection(self.master)
                remote.start_server()

            time.sleep(30)

            # Run 2 queries for user 1 and 2 queries for user 2 in a different thread
            t51 = threading.Thread(name='run_first', target=self.run_dummy_query_thread, args=("limited1", 30000))
            t51.start()
            time.sleep(1)
            t52 = threading.Thread(name='run_second', target=self.run_dummy_query_thread, args=("limited1", 30000))
            t52.start()
            t61 = threading.Thread(name='run_fourth', target=self.run_dummy_query_thread, args=("limited2", 30000))
            t61.start()
            # User 1 should not be allowed to run another query
            cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
            self.log.info(cbq_user1)
            self.assertTrue("429 Too Many Requests" in cbq_user1,
                            f"The statement should have error'd please check it {cbq_user1}")
            # User 2 should be allowed to run another query
            time.sleep(1)
            cbq_user2 = self.execute_commands_inside(self.cbqpath_user2, query, '', '', '', '', '')
            self.log.info(cbq_user2)
            results = self.convert_list_to_json(cbq_user2)
            self.assertEqual(results['status'], "success")
            self.assertEqual(results['results'], [10000])

            # In a minute we should be able to execute more queries
            time.sleep(60)
            cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
            results = self.convert_list_to_json(cbq_user1)
            self.assertEqual(results['status'], "success")
            self.assertEqual(results['results'], [10000])

            # Unset limits on user one and try to exceed the limit
            limits_user_1 = '{}'
            self.set_query_limits(username="limited1", limits=limits_user_1, skip_verify=True)

            t71 = threading.Thread(name='run_sixth', target=self.run_dummy_query_thread, args=("limited1", 20000))
            t71.start()
            t72 = threading.Thread(name='run_seventh', target=self.run_dummy_query_thread, args=("limited1", 20000))
            t72.start()

            cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
            results = self.convert_list_to_json(cbq_user1)
            self.assertEqual(results['status'], "success")
            self.assertEqual(results['results'], [10000])

    def test_ingress(self):
        limits_user_1 = '{"query":{"ingress_mib_per_min": 1}}'
        self.set_query_limits(username="limited1", limits=limits_user_1)
        limits_user_2 = '{"query":{"ingress_mib_per_min": 2}}'
        self.set_query_limits(username="limited2", limits=limits_user_2)

        query = "select * from default limit 1 union select * from default limit 1 union select * from default limit 1 " \
                "union select * from default limit 1 union select * from default limit 1 union select * from default limit 1"

        for i in range(0, 1000):
            self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')

        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        self.assertTrue("413 Request Entity Too Large" in cbq_user1, f"The statement should have error'd check the output {cbq_user1}")

        # User 2 should be allowed to run another query
        cbq_user2 = self.execute_commands_inside(self.cbqpath_user2, query, '', '', '', '', '')
        self.log.info(cbq_user2)
        results = self.convert_list_to_json(cbq_user2)
        self.assertEqual(results['status'], "success")

        if self.crash:
            try:
                remote = RemoteMachineShellConnection(self.master)
                remote.stop_server()
            finally:
                remote = RemoteMachineShellConnection(self.master)
                remote.start_server()

            time.sleep(30)


            limits_user_1 = '{"query":{"ingress_mib_per_min": 1}}'
            self.set_query_limits(username="limited1", limits=limits_user_1)
            limits_user_2 = '{"query":{"ingress_mib_per_min": 2}}'
            self.set_query_limits(username="limited2", limits=limits_user_2)

            query = "select * from default limit 1 union select * from default limit 1 union select * from default limit 1 " \
                    "union select * from default limit 1 union select * from default limit 1 union select * from default limit 1"

            for i in range(0, 1000):
                self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')

            cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
            self.assertTrue("413 Request Entity Too Large" in cbq_user1, f"The statement should have error'd check the output {cbq_user1}")

            # User 2 should be allowed to run another query
            cbq_user2 = self.execute_commands_inside(self.cbqpath_user2, query, '', '', '', '', '')
            self.log.info(cbq_user2)
            results = self.convert_list_to_json(cbq_user2)
            self.assertEqual(results['status'], "success")



    def test_egress(self):
        limits_user_1 = '{"query":{"num_concurrent_requests": 5, "num_queries_per_min": 60, "egress_mib_per_min": 4}}'
        self.set_query_limits(username="limited1", limits=limits_user_1)
        limits_user_2 = '{"query":{"num_concurrent_requests": 10, "num_queries_per_min": 60, "ingress_mib_per_min": 10, "egress_mib_per_min": 100}}'
        self.set_query_limits(username="limited2", limits=limits_user_2)

        query = "select * from default"

        for i in range(0, 2):
            self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')

        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        self.log.info(cbq_user1)
        self.assertTrue("413 Request Entity Too Large" in cbq_user1, f"The statement should have error'd check the output {cbq_user1}")
        # User 2 should be allowed to run another query
        for i in range(0, 10):
            cbq_user2 = self.execute_commands_inside(self.cbqpath_user2, query, '', '', '', '', '')
            results = self.convert_list_to_json(cbq_user2)
            self.assertEqual(results['status'], "success")
            self.assertEqual(results['metrics']['resultCount'], self.docs_per_day * 2016)

        # After 60 second should be able to run queries again
        time.sleep(60)
        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        results = self.convert_list_to_json(cbq_user1)
        self.assertEqual(results['status'], "success")
        self.assertEqual(results['metrics']['resultCount'], self.docs_per_day * 2016)

        if self.crash:
            try:
                remote = RemoteMachineShellConnection(self.master)
                remote.stop_server()
            finally:
                remote = RemoteMachineShellConnection(self.master)
                remote.start_server()
            time.sleep(30)

            for i in range(0, 3):
                self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')

            cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
            self.log.info(cbq_user1)
            self.assertTrue("413 Request Entity Too Large" in cbq_user1, f"The statement should have error'd check the output {cbq_user1}")
            # User 2 should be allowed to run another query
            for i in range(0, 10):
                cbq_user2 = self.execute_commands_inside(self.cbqpath_user2, query, '', '', '', '', '')
                results = self.convert_list_to_json(cbq_user2)
                self.assertEqual(results['status'], "success")
                self.assertEqual(results['metrics']['resultCount'], self.docs_per_day * 2016)

            # After 60 second should be able to run queries again
            time.sleep(60)
            cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
            results = self.convert_list_to_json(cbq_user1)
            self.assertEqual(results['status'], "success")
            self.assertEqual(results['metrics']['resultCount'], self.docs_per_day * 2016)



    '''Rebalanced in node shouldn't have any carried over limits'''
    def test_rebalance(self):
        limits_user_1 = '{"query":{"num_concurrent_requests": 2, "num_queries_per_min": 100, "ingress_mib_per_min": 10, "egress_mib_per_min": 15}}'
        self.set_query_limits(username="limited1", limits=limits_user_1)
        limits_user_2 = '{"query":{"num_concurrent_requests": 3, "num_queries_per_min": 100, "ingress_mib_per_min": 10, "egress_mib_per_min": 100}}'
        self.set_query_limits(username="limited2", limits=limits_user_2)

        self.log.info("Now rebalancing in")

        try:
            reb1 = self.cluster.rebalance(self.servers[:self.nodes_init],
                                         [self.servers[self.nodes_init]], [],services=["kv"])
            if reb1:
                result = self.rest.monitorRebalance()
                msg = "successfully rebalanced cluster {0}"
                self.log.info(msg.format(result))
        except Exception as e:
                self.fail("Rebalance in failed with {0}".format(str(e)))

        curl_output = self.shell.execute_command(
            f"{self.curl_path} -X GET -u {self.rest.username}:{self.rest.password} http://{self.servers[self.nodes_init].ip}:{self.servers[self.nodes_init].port}/settings/rbac/users/local/limited1")
        self.log.info(curl_output)
        actual_settings = self.convert_list_to_json(curl_output[0])
        expected_settings = json.loads(limits_user_1)
        diffs = DeepDiff(actual_settings['limits'], expected_settings, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        self.set_query_limits(username="limited2", limits=limits_user_2, server=self.servers[self.nodes_init])
        query = "EXECUTE FUNCTION sleep(10000)"

        # Run 2 queries for user 1 and 2 queries for user 2 in a different thread
        t51 = threading.Thread(name='run_first', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t51.start()
        t52 = threading.Thread(name='run_second', target=self.run_dummy_query_thread, args=("limited1", 20000))
        t52.start()
        t61 = threading.Thread(name='run_fourth', target=self.run_dummy_query_thread, args=("limited2", 20000))
        t61.start()
        t62 = threading.Thread(name='run_fifth', target=self.run_dummy_query_thread, args=("limited2", 20000))
        t62.start()
        time.sleep(1)
        # User 1 should not be allowed to run another query
        cbq_user1 = self.execute_commands_inside(self.cbqpath_user1, query, '', '', '', '', '')
        self.log.info(cbq_user1)
        self.assertTrue("429 Too Many Requests" in cbq_user1, f"The statement should have error'd {cbq_user1}")
        # User 2 should be allowed to run another query
        cbq_user2 = self.execute_commands_inside(self.cbqpath_user2, query, '', '', '', '', '')
        self.log.info(cbq_user2)
        results = self.convert_list_to_json(cbq_user2)
        self.assertEqual(results['status'], "success")
        self.assertEqual(results['results'], [10000])

    def run_dummy_query_thread(self, username='', delay=5000):
        try:
            cbqpath = f'{self.path}cbq -e {self.master.ip}:{self.n1ql_port} -q -u {username} -p {self.rest.password}'
            query = f"EXECUTE FUNCTION sleep({delay})"
            curl = self.execute_commands_inside(cbqpath, query, '', '', '', '', '')
            self.log.info(curl)
        finally:
            return


    def set_query_limits(self, username="", limits="", server=None, skip_verify=False):
        if server is None:
            server = self.master
        if self.enforce_limits:
            curl_output = self.shell.execute_command(
                f"{self.curl_path} -X POST -u {self.rest.username}:{self.rest.password} http://{server.ip}:{server.port}/internalSettings "
                "-d 'enforceLimits=true' ")
            self.log.info(curl_output)
            curl_output = self.shell.execute_command(
                f"{self.curl_path} -X GET -u {self.rest.username}:{self.rest.password} http://{server.ip}:{server.port}/internalSettings")
            self.log.info(curl_output)
            enforce_limits_setting = self.convert_list_to_json(curl_output[0])
            self.assertEqual(enforce_limits_setting['enforceLimits'], True)
        full_permissions = 'bucket_full_access[*],query_select[*],query_update[*],' \
                           'query_insert[*],query_delete[*],query_manage_index[*],' \
                           'query_system_catalog,query_external_access,query_execute_global_external_functions'
        curl_output = self.shell.execute_command(f"{self.curl_path} -X PUT -u {self.rest.username}:{self.rest.password} http://{server.ip}:{server.port}/settings/rbac/users/local/{username} "
                                                 f"-d 'limits={limits}' -d 'roles={full_permissions}' ")
        self.log.info(curl_output)
        actual_output = self.convert_list_to_json(curl_output[0])
        if self.expected_error:
            self.assertTrue("Thevaluemustbeinrangefrom1toinfinity" in str(actual_output), f"The error is not right {actual_output}")
        else:
            curl_output = self.shell.execute_command(f"{self.curl_path} -X GET -u {self.rest.username}:{self.rest.password} http://{server.ip}:{server.port}/settings/rbac/users/local/{username}")
            self.log.info(curl_output)
            expected_settings = json.loads(limits)
            actual_settings = self.convert_list_to_json(curl_output[0])
            if not skip_verify:
                diffs = DeepDiff(actual_settings['limits'], expected_settings, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

    def delete_user(self, username="", server=None):
        if server is None:
            server = self.master
        curl_output = self.shell.execute_command(f"{self.curl_path} -X DELETE -u {self.rest.username}:{self.rest.password} http://{server.ip}:{server.port}/settings/rbac/users/local/{username}")
        self.log.info(curl_output)