import logging
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.tuq_helper import N1QLHelper
from newupgradebasetest import NewUpgradeBaseTest
from pytests.tuqquery.n1ql_rbac_2 import RbacN1QL
from remote.remote_util import RemoteMachineShellConnection

log = logging.getLogger(__name__)

class UpgradeN1QLRBAC(RbacN1QL, NewUpgradeBaseTest):
    def setUp(self):
        self.array_indexing = False
        super(UpgradeN1QLRBAC, self).setUp()
        self.initial_version = self.input.param('initial_version', '4.6.0-3653')
        self.upgrade_to = self.input.param("upgrade_to")
        self.n1ql_helper = N1QLHelper(version=self.version, shell=self.shell,
            use_rest=self.use_rest, max_verify=self.max_verify,
            buckets=self.buckets, item_flag=self.item_flag,
            n1ql_port=self.n1ql_port, full_docs_list=[],
            log=self.log, input=self.input, master=self.master)
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        log.info(self.n1ql_node)
        if self.ddocs_num:
            self.create_ddocs_and_views()
            gen_load = BlobGenerator('pre-upgrade', 'preupgrade-', self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen_load, "create", self.expire_time, flag=self.item_flag)

    def tearDown(self):
        super(UpgradeN1QLRBAC, self).tearDown()

    def test_offline_upgrade_with_rbac(self):
        """
        1. This test is run with sasl bucket.Secondary and primay indexes are
        created before upgrade.
        2. After offline upgrade we make sure that queries can use these indexes for
        sasl and non sasl buckets.
        3. We also use pre-upgrade users for the query with indexes.
        """

        for bucket in self.buckets:
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.run_cbq_query(query = self.query)
        # create users before upgrade via couchbase-cli
        self.create_users_before_upgrade_non_ldap()
        self._perform_offline_upgrade()
        #verify number of buckets after upgrade
        self.assertTrue(len(self.buckets)==2)
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        # verify number of users after upgrade
        self.log.error(actual_result['metrics']['resultCount'])
        self.assertEqual(actual_result['metrics']['resultCount'], 9)
        self.create_users(users=[{'id': 'john',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("admin", 'john')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.log.error(actual_result['metrics']['resultCount'])
        self.assertTrue(actual_result['metrics']['resultCount'] == 10)

        self.create_users(users=[{'id': 'johnClusterAdmin',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("cluster_admin", 'johnClusterAdmin')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['metrics']['resultCount'] == 11)
        self.query = "GRANT {0} on {2} to {1}".format("bucket_admin", 'standard_bucket0', 'standard_bucket0')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        self.shell = RemoteMachineShellConnection(self.master)
        for bucket in self.buckets:
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('johnClusterAdmin', 'password', self.master.ip, bucket.name, self.curl_path)
            self.sleep(10)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'johnClusterAdmin'))
            # use pre-upgrade users
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('john', 'password', self.master.ip, bucket.name, self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                          format(bucket.name, 'john_admin'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('standard_bucket0', 'password', self.master.ip, bucket.name, self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'standard_bucket0'))

    def test_offline_upgrade_with_new_users(self):
        """
        1. This test creates different users with different query permissions
        and validates the specific permissions after upgrade.
        2. We use pre-upgrade users for different queries and then change
        permissions on them and verify various queries accordingly.
        3. We also change permissions on new users and verify queries accordingly.
        :return:
        """

        for bucket in self.buckets:
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.run_cbq_query(query = self.query)
        # create users before upgrade via couchbase-cli
        self.create_users_before_upgrade_non_ldap()
        self._perform_offline_upgrade()
        self.sleep(20)
        #verify number of buckets after upgrade
        self.assertTrue(len(self.buckets)==2)
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['metrics']['resultCount'] == 16)
        self.check_permissions_helper()
        self.create_users(users=[{'id': 'johnClusterAdmin',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("cluster_admin", 'johnClusterAdmin')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('johnClusterAdmin', 'password', self.master.ip, 'standard_bucket0', self.curl_path)
        output, error = self.shell.execute_command(cmd)
        self.shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format('standard_bucket0', 'johnClusterAdmin'))
        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:my_user_info'".format('johnClusterAdmin', 'password', self.master.ip, self.curl_path)
        output, error = self.shell.execute_command(cmd)
        self.shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format('my_user_info', 'johnClusterAdmin'))
        self.use_pre_upgrade_users_post_upgrade()
        self.change_permissions_and_verify_pre_upgrade_users()
        self.change_permissions_and_verify_new_users()

    def test_offline_upgrade_with_system_catalog(self):
        """
        1. This test does offline upgrade and checks various system catalog users
        2. It might fail based on implementation details from dev.
        :return:
        """
        for bucket in self.buckets:
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.run_cbq_query(query = self.query)
        self._perform_offline_upgrade()
        self.create_and_verify_system_catalog_users_helper()
        self.check_system_catalog_helper()

    def test_offline_upgrade_check_ldap_users_before_upgrade(self):
        """
        This test does offline upgrade and tests if users created before upgrade are working correctly after upgrade.
        The users created before upgrade are verified for functionality in verify_pre_upgrade_users_permissions_helper.
        Permissions for the users created before upgrade are changed after upgrade to new query based permissions in
        change_pre_upgrade_users_permissions.
        """
        for bucket in self.buckets:
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.run_cbq_query(query = self.query)
        self._perform_offline_upgrade()
        self.sleep(20)
        self.query = 'select * from system:user_info'
        actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertTrue(actual_result['metrics']['resultCount'] == 5)
        self.verify_pre_upgrade_users_permissions_helper()
        self.change_and_verify_pre_upgrade_ldap_users_permissions()
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['metrics']['resultCount'] == 12)
        self.check_permissions_helper()
        self.change_permissions_and_verify_new_users()

    def test_online_upgrade_with_rebalance_with_rbac(self):
        """
        # This test does the online upgrade ,validates the specific
        # permissions after upgrade and verifies the number of users created are correct.
        # It also verifies the queries use the correct index for sasl buckets after online upgrade.
        """
        for bucket in self.buckets:
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.run_cbq_query(query = self.query)
         # create users before upgrade via couchbase-cli
        self.create_users_before_upgrade_non_ldap()
        self._perform_online_upgrade_with_rebalance()
        self.sleep(20)
        #verify number of buckets after upgrade
        self.assertTrue(len(self.buckets)==1)
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.log.error(actual_result['metrics']['resultCount'])
        #verify number of users after upgrade
        self.assertTrue(actual_result['metrics']['resultCount'] == 20)
        self.shell = RemoteMachineShellConnection(self.master)
        self.create_users(users=[{'id': 'johnClusterAdmin',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("cluster_admin", 'johnClusterAdmin')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        self.create_users(users=[{'id': 'john_admin',
                                           'name': 'john_admin',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("cluster_admin", 'john_admin')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        for bucket in self.buckets:
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('johnClusterAdmin', 'password', self.master.ip, bucket.name, self.curl_path)
            self.sleep(10)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'johnClusterAdmin'))
            # use pre-upgrade users
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('john_admin', 'password', self.master.ip, bucket.name, self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                          format(bucket.name, 'john_admin'))


    def test_online_upgrade_with_rebalance_with_system_catalog(self):
        """
        This test does online upgrade and checks various system catalog users
        It might fail based on implementation details from dev.
        :return:
        """
        self._perform_online_upgrade_with_rebalance()
        self.create_and_verify_system_catalog_users_helper()
        self.check_system_catalog_helper()

    def test_online_upgrade_with_rebalance_check_ldap_users_before_upgrade(self):
        """
        This test does online upgrade and tests if users created before upgrade are working correctly after upgrade.
        The users created before upgrade are verified for functionality in verify_pre_upgrade_users_permissions_helper.
        Permissions for the users created before upgrade are changed after upgrade to new query based permissions in
        change_pre_upgrade_users_permssions.
        """
        for bucket in self.buckets:
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.run_cbq_query(query = self.query)
        # create ldap users before upgrade
        self.create_ldap_auth_helper()
        self.sleep(20)
        self._perform_online_upgrade_with_rebalance()
        self.sleep(20)
        for bucket in self.buckets:
         self.query = 'create primary index on {0}'.format(bucket.name)
         self.run_cbq_query(query = self.query)
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.assertEqual(actual_result['metrics']['resultCount'], 7,
                         "actual result is {0}".format(actual_result))
        self.verify_pre_upgrade_users_permissions_helper(test= 'online_upgrade')
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.assertEqual(actual_result['metrics']['resultCount'], 14,
                         "actual result is {0}".format(actual_result))

    def test_online_upgrade_with_failover_with_rbac(self):
        """
        # This test does the online upgrade ,validates the specific
        # permissions after upgrade and verifies the number of users created are correct.
        # It also verifies the queries use the correct index for sasl buckets after online upgrade.
        """
         # create users before upgrade via couchbase-cli
        self.create_users_before_upgrade_non_ldap()
        self._perform_online_upgrade_with_failover()
        self.sleep(20)
        #verify number of buckets after upgrade
        self.assertTrue(len(self.buckets)==2)
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.assertEqual(actual_result['metrics']['resultCount'], 23,
                           "actual result is {0}".format(actual_result))
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.assertEqual(actual_result['metrics']['resultCount'], 23,
                           "actual result is {0}".format(actual_result))
        self.check_permissions_helper()
        self.change_permissions_and_verify_new_users()


    def test_online_upgrade_with_failover_with_system_catalog(self):
        """
        This test does online upgrade and checks various system catalog users
        It might fail based on implementation details from dev.
        :return:
        """
        for bucket in self.buckets:
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.run_cbq_query(query = self.query, server = self.n1ql_node)
        self._perform_online_upgrade_with_rebalance()
        self.sleep(20)
        self.create_and_verify_system_catalog_users_helper()
        self.check_system_catalog_helper()

    def test_online_upgrade_with_failover_check_ldap_users_before_upgrade(self):
        """
        This test does online upgrade and tests if users created before upgrade are working correctly after upgrade.
        The users created before upgrade are verified for functionality in verify_pre_upgrade_users_permissions_helper.
        Permissions for the users created before upgrade are changed after upgrade to new query based permissions in
        change_pre_upgrade_users_permssions.
        """
        for bucket in self.buckets:
            self.query = 'create index idx on {0}(meta().id)'.format(bucket.name)
            self.run_cbq_query(query = self.query)
        # create ldap users before upgrade
        self.create_ldap_auth_helper()
        self._perform_online_upgrade_with_failover()
        self.sleep(20)
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query, server = self.n1ql_node)
        self.assertEqual(actual_result['metrics']['resultCount'], 23)
        self.change_and_verify_pre_upgrade_ldap_users_permissions()
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.assertEqual(actual_result['metrics']['resultCount'], 23)
        self.check_permissions_helper()
        self.change_permissions_and_verify_new_users()
