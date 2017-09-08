import logging

from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.rest_client import RestConnection
from newupgradebasetest import NewUpgradeBaseTest
from pytests.security.rbac_base import RbacBase
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
        self.assertEqual(actual_result['metrics']['resultCount'] , 9)
        self.create_users(users=[{'id': 'john',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("admin",'john')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.log.error(actual_result['metrics']['resultCount'])
        self.assertTrue(actual_result['metrics']['resultCount'] == 10)

        self.create_users(users=[{'id': 'johnClusterAdmin',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("cluster_admin",'johnClusterAdmin')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['metrics']['resultCount'] == 11)
        self.query = "GRANT {0} on {2} to {1}".format("bucket_admin",'standard_bucket0','standard_bucket0')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        self.shell = RemoteMachineShellConnection(self.master)
        for bucket in self.buckets:
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('johnClusterAdmin','password', self.master.ip, bucket.name,self.curl_path)
            self.sleep(10)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'johnClusterAdmin'))
            # use pre-upgrade users
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('john','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                          format(bucket.name, 'john_admin'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('standard_bucket0','password', self.master.ip, bucket.name,self.curl_path)
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
        self.query = "GRANT {0} to {1}".format("cluster_admin",'johnClusterAdmin')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('johnClusterAdmin','password', self.master.ip, 'standard_bucket0',self.curl_path)
        output, error = self.shell.execute_command(cmd)
        self.shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format('standard_bucket0', 'johnClusterAdmin'))
        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:my_user_info'".format('johnClusterAdmin','password', self.master.ip, self.curl_path)
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
        self.query = "GRANT {0} to {1}".format("cluster_admin",'johnClusterAdmin')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        self.create_users(users=[{'id': 'john_admin',
                                           'name': 'john_admin',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("cluster_admin",'john_admin')
        actual_result = self.run_cbq_query(query = self.query)
        self.assertTrue(actual_result['status'] == 'success')
        for bucket in self.buckets:
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('johnClusterAdmin','password', self.master.ip, bucket.name,self.curl_path)
            self.sleep(10)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'johnClusterAdmin'))
            # use pre-upgrade users
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} use index(idx) where meta().id > 0 " \
                  "LIMIT 10'".\
                format('john_admin','password', self.master.ip, bucket.name,self.curl_path)
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
        self.assertEqual(actual_result['metrics']['resultCount'],23)
        self.change_and_verify_pre_upgrade_ldap_users_permissions()
        self.query_select_insert_update_delete_helper()
        self.query = 'select * from system:user_info'
        actual_result = self.run_cbq_query(query = self.query)
        self.assertEqual(actual_result['metrics']['resultCount'], 23)
        self.check_permissions_helper()
        self.change_permissions_and_verify_new_users()

    def query_select_insert_update_delete_helper(self):
        self.create_users(users=[{'id': 'john_insert',
                                           'name': 'johnInsert',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_update',
                                           'name': 'johnUpdate',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_delete',
                                           'name': 'johnDelete',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_select',
                                           'name': 'johnSelect',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_select2',
                                           'name': 'johnSelect2',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_rep',
                                           'name': 'johnRep',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_bucket_admin',
                                           'name': 'johnBucketAdmin',
                                           'password':'password'}])
        for bucket in self.buckets:
            self.query = "GRANT {0} on {2} to {1}".format("query_insert",'john_insert',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = "GRANT {0} on {2} to {1}".format("query_update",'john_update',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = "GRANT {0} on {2} to {1}".format("query_delete",'john_delete',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = "GRANT {0} on {2} to {1}".format("query_select",'john_select',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = "GRANT {0} on {2} to {1}".format("bucket_admin",'john_bucket_admin',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = "GRANT {0} to {1}".format("replication_admin",'john_rep')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            self.query = "GRANT {0} on {2} to {1}".format("query_select",'john_select2',bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)

    def query_select_insert_update_delete_helper_default(self):
        self.create_users(users=[{'id': 'john_insert',
                                           'name': 'johnInsert',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_update',
                                           'name': 'johnUpdate',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_delete',
                                           'name': 'johnDelete',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_select',
                                           'name': 'johnSelect',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_select2',
                                           'name': 'johnSelect2',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_rep',
                                           'name': 'johnRep',
                                           'password':'password'}])
        self.create_users(users=[{'id': 'john_bucket_admin',
                                           'name': 'johnBucketAdmin',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("replication_admin",'john_rep')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)


    def check_permissions_helper(self):
      for bucket in self.buckets:
         cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"test\", { \"value1\": \"one1\" })'"%\
                (self.curl_path,'john_insert', 'password', self.master.ip, bucket.name)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                        format(bucket.name, 'johnInsert'))
         log.info("Query executed successfully")
         old_name = "employee-14"
         new_name = "employee-14-2"
         cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'".\
                format('john_update', 'password', self.master.ip, bucket.name, new_name, old_name,self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to update into {0} as user {1}".
                        format(bucket.name, 'johnUpdate'))
         log.info("Query executed successfully")
         del_name = "employee-14"
         cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                format('john_delete', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                        format(bucket.name, 'john_delete'))
         log.info("Query executed successfully")
         cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format('john_select2', 'password', self.master.ip,bucket.name,self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_select2'))

    def create_and_verify_system_catalog_users_helper(self):
        self.create_users(users=[{'id': 'john_system',
                                           'name': 'john',
                                           'password':'password'}])
        self.query = "GRANT {0} to {1}".format("query_system_catalog",'john_system')
        self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
        for bucket in self.buckets:
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:namespaces'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:datastores'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:indexes'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:completed_requests'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:active_requests'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:prepareds'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:my_user_info'".\
                format('john_system','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, 'john_system'))

    def check_system_catalog_helper(self):
        """
        These test might fail for now as system catalog tables are not
        fully implemented based on query PM's doc.
        :return:
        """
        self.system_catalog_helper_delete_for_upgrade()
        self.system_catalog_helper_select_for_upgrade()

    def system_catalog_helper_select_for_upgrade(self):
        query = 'select * from system:datastores'
        res = self.run_cbq_query()
        self.assertEqual(res['status'],'success')
        self.query = 'select * from system:namespaces'
        res = self.run_cbq_query(query=query)
        self.assertEqual(res['status'],'success')
        self.query = 'select * from system:keyspaces'
        res = self.run_cbq_query(query=query)
        self.assertEqual(res['status'],'success')
        self.query = 'create index idx1 on {0}(name)'.format(self.buckets[0].name)
        res = self.run_cbq_query(query=query)
        self.sleep(10)
        self.query = 'select * from system:indexes'
        res = self.run_cbq_query(query=query)
        self.assertEqual(res['status'],'success')
        self.query = 'select * from system:dual'
        res = self.run_cbq_query(query=query)
        self.assertEqual(res['status'],'success')
        self.query = "prepare st1 from select * from {0} union select * from {0} union select * from {0}".format(self.buckets[0].name)
        res = self.run_cbq_query(query=query)
        self.assertEqual(res['status'],'success')
        self.query = 'execute st1'
        res = self.run_cbq_query(query=query)
        self.assertEqual(res['status'],'success')

    def system_catalog_helper_delete_for_upgrade(self):
        self.queries = ['delete from system:datastores',
                        'delete from system:namespaces',
                        'delete from system:keyspaces',
                        'delete from system:indexes',
                        'delete from system:user_info',
                        'delete from system:nodes',
                        'delete from system:applicable_roles']
        for query in self.queries:
            try:
                self.run_cbq_query(query=query)
            except Exception, ex:
                log.error(ex)
                self.assertNotEqual(str(ex).find("'code': 11003"), -1)
        try:
            query = 'delete from system:dual'
            self.run_cbq_query(query=query)
        except Exception,ex:
            self.log.error(ex)
            self.assertNotEqual(str(ex).find("'code': 11000"), -1)

        queries = ['delete from system:completed_requests',
                   'delete from system:active_requests where state!="running"',
                   'delete from system:prepareds']
        for query in queries:
            res = self.run_cbq_query(query=query)
            self.assertEqual(res['status'], 'success')

        queries = ['select * from system:completed_requests',
                   'select * from system:active_requests',
                   'select * from system:prepareds']
        for query in queries:
            res = self.run_cbq_query(query=query)
            self.assertEqual(res['status'], 'success')


    def change_and_verify_pre_upgrade_ldap_users_permissions(self):
        for bucket in self.buckets:
            # change permission of john_bucketadmin1 and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_select",bucket.name,'bucket0')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'".\
                    format('bucket0', 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, 'bucket0'))

            # change permission of john_bucketadminAll and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_insert",bucket.name,'bucket0')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "%s -u %s:%s http://%s:8093/query/service -d 'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"1\", { \"value1\": \"one1\" })'" %(self.curl_path,'bucket0', 'password',self.master.ip,bucket.name)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                            format(bucket.name, 'bucket0'))

            # change permission of cluster_user and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_update",bucket.name,'bucket0')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d 'statement=UPDATE {3} a set name = '{4}' where " \
                  "name = '{5}' limit 1'".format('bucket0', 'password',self.master.ip,bucket.name,new_name, old_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to update  {0} as user {1}".
                            format(bucket.name, 'bucket0'))
            #change permission of bucket0 and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_delete",bucket.name,'bucket0')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                format('bucket0', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                       format(bucket.name, 'bucket0'))
            log.info("Query executed successfully")

            # change permission of cbadminbucket user and verify its able to execute the correct query.
            self.query = "GRANT {0} to {1}".format("query_system_catalog",'cbadminbucket')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'".\
                format('cbadminbucket','password', self.master.ip, bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from system:keyspaces as user {0}".format('cbadminbucket'))


    def create_ldap_auth_helper(self):
        """
        Helper function for creating ldap users pre-upgrade
        :return:
        """
        # not able to create bucket admin on passwordless bucket pre upgrade
        users = [
                 {'id': 'john_bucketadminAll', 'name': 'john_bucketadminAll', 'password': 'password'},
                 {'id': 'cluster_user','name':'cluster_user','password':'password'},
                 {'id': 'read_user','name':'read_user','password':'password'},
                 {'id': 'cadmin','name':'cadmin','password':'password'},]
        RbacBase().create_user_source(users, 'ldap', self.master)
        rolelist = [{'id': 'john_bucketadminAll', 'name': 'john_bucketadminAll','roles': 'bucket_admin[*]'},
                    {'id': 'cluster_user', 'name': 'cluster_user','roles': 'cluster_admin'},
                    {'id': 'read_user', 'name': 'read_user','roles': 'ro_admin'},
                    {'id': 'cadmin', 'name': 'cadmin','roles': 'admin'}]
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'ldap')


    def verify_pre_upgrade_users_permissions_helper(self,test = ''):
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                    format('bucket0', 'password', self.master.ip,'bucket0',self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format('bucket0', 'bucket0'))
            if test == 'online_upgrade':
                cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                    format('cbadminbucket', 'password', self.master.ip,'default',self.curl_path)
            else:
                cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                    format('cbadminbucket', 'password', self.master.ip,'bucket0',self.curl_path)

            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                             format('bucket0', 'cbadminbucket'))
            cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'".\
                    format('cbadminbucket', 'password', self.master.ip,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                             format('system:keyspaces', 'cbadminbucket'))

            for bucket in self.buckets:
             cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
                  "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"5\", { \"value1\": \"one1\" })'"%\
                    (self.curl_path,'bucket0', 'password', self.master.ip, bucket.name)
             output, error = self.shell.execute_command(cmd)
             self.shell.log_command_output(output, error)
             self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                            format(bucket.name, 'bucket0'))
             log.info("Query executed successfully")
             old_name = "employee-14"
             new_name = "employee-14-2"
             cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
                  "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'".\
                    format('bucket0', 'password', self.master.ip, bucket.name, new_name, old_name,self.curl_path)
             output, error = self.shell.execute_command(cmd)
             self.shell.log_command_output(output, error)
             self.assertTrue(any("success" in line for line in output), "Unable to update into {0} as user {1}".
                            format(bucket.name, 'bucket0'))
             log.info("Query executed successfully")
             del_name = "employee-14"
             cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
                  "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                    format('bucket0', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
             output, error = self.shell.execute_command(cmd)
             self.shell.log_command_output(output, error)
             self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                           format(bucket.name, 'bucket0'))
             log.info("Query executed successfully")

    def use_pre_upgrade_users_post_upgrade(self):
        for bucket in self.buckets:
         cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"test2\", { \"value1\": \"one1\" })'"%\
                (self.curl_path,'cbadminbucket', 'password', self.master.ip, bucket.name)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                        format(bucket.name, 'johnInsert'))
         log.info("Query executed successfully")
         old_name = "employee-14"
         new_name = "employee-14-2"
         cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'".\
                format('cbadminbucket', 'password', self.master.ip, bucket.name, new_name, old_name,self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to update into {0} as user {1}".
                        format(bucket.name, 'johnUpdate'))
         log.info("Query executed successfully")
         cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format(bucket.name, 'password', self.master.ip,bucket.name,self.curl_path)
         output, error = self.shell.execute_command(cmd)
         self.shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(bucket.name, bucket.name))


    def change_permissions_and_verify_pre_upgrade_users(self):
        for bucket in self.buckets:
            # change permission of john_cluster and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_select",bucket.name,bucket.name)
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'".\
                    format(bucket.name, 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, bucket.name))

            # change permission of ro_non_ldap and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_update",bucket.name,'cbadminbucket')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d 'statement=UPDATE {3} a set name = '{4}' where " \
                  "name = '{5}' limit 1'".format('cbadminbucket', 'readonlypassword',self.master.ip,bucket.name,new_name, old_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to update  {0} as user {1}".
                            format(bucket.name, 'cbadminbucket'))

            # change permission of john_admin and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_delete",bucket.name,'cbadminbucket')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                format('cbadminbucket', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                        format(bucket.name, 'cbadminbucket'))
            log.info("Query executed successfully")
            # change permission of bob user and verify its able to execute the correct query.
            self.query = "GRANT {0} to {1}".format("query_system_catalog","cbadminbucket")
            self.run_cbq_query(query = self.query)
            cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from system:keyspaces'".\
                format('cbadminbucket','password', self.master.ip, self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to select from system:keyspaces as user {0}".
                        format('cbadminbucket'))

    def change_permissions_and_verify_new_users(self):
        for bucket in self.buckets:
            # change permission of john_insert and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("bucket_admin",bucket.name,'john_insert')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'".\
                    format('john_insert', 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, 'john_insert'))

            # change permission of john_update and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_insert",bucket.name,'john_update')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=INSERT INTO {3} values(\"k055\", 123  )' " \
                  .format('john_update', 'password',self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                            format(bucket.name, 'john_update'))

            # change permission of john_select and verify its able to execute the correct query.
            self.query = "GRANT {0} to {1}".format("cluster_admin",'john_select')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            old_name = "employee-14"
            new_name = "employee-14-2"
            cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d 'statement=UPDATE {3} a set name = '{4}' where " \
                  "name = '{5}' limit 1'".format('john_select', 'password',self.master.ip,bucket.name,new_name, old_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to update  {0} as user {1}".
                            format(bucket.name, 'john_select'))

            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'".\
                    format('john_select', 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, 'john_select'))

            # change permission of john_select2 and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_delete",bucket.name,'john_select2')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            del_name = "employee-14"
            cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                format('john_select2', 'password', self.master.ip, bucket.name, del_name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                        format(bucket.name, 'john_select2'))
            log.info("Query executed successfully")

            # change permission of john_delete and verify its able to execute the correct query.
            self.query = "GRANT {0} on {1} to {2}".format("query_select",bucket.name,'john_delete')
            self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} limit 1'".\
                    format('john_delete', 'password', self.master.ip,bucket.name,self.curl_path)
            output, error = self.shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                            format(bucket.name, 'john_delete'))

    def create_users(self, users=None):
        """
        :param user: takes a list of {'id': 'xxx', 'name': 'some_name ,
                                        'password': 'passw0rd'}
        :return: Nothing
        """
        if not users:
            users = self.users
        RbacBase().create_user_source(users, 'builtin',self.master)
        log.info("SUCCESS: User(s) %s created"
                      % ','.join([user['name'] for user in users]))

    def create_users_before_upgrade_non_ldap(self):
        """
        password needs to be added statically for these users
        on the specific machine where ldap is enabled.
        """
        log.info("create a read only user account")
        cli_cmd = "{0}couchbase-cli -c {1}:8091 -u Administrator " \
                      "-p password".format(self.path, self.master.ip)
        ro_set_cmd = cli_cmd + " user-manage --set --ro-username=ro_non_ldap " \
                                   "--ro-password=readonlypassword"
        self.shell.execute_command(ro_set_cmd)
        log.info("create a bucket admin on bucket0 user account")
        bucket0_admin_cmd = cli_cmd + " admin-role-manage --set-users=bob " \
                                      "--set-names=Bob " \
                                      "--roles=bucket_admin[bucket0]"
        self.shell.execute_command(bucket0_admin_cmd)
        log.info("create a bucket admin on all buckets user account")
        all_bucket_admin_cmd = cli_cmd + " admin-role-manage --set-users=mary " \
                                         "--set-names=Mary --roles=bucket_admin[*]"
        self.shell.execute_command(all_bucket_admin_cmd)
        log.info("create a cluster admin user account")
        cluster_admin_cmd = cli_cmd + "admin-role-manage --set-users=john_cluster " \
                                      "--set-names=john_cluster --roles=cluster_admin"
        self.shell.execute_command(cluster_admin_cmd)
        log.info("create a admin user account")
        admin_user_cmd = cli_cmd + " admin-role-manage --set-users=john_admin " \
                                   "--set-names=john_admin --roles=admin"
        self.shell.execute_command(admin_user_cmd)

        users = [{'id': 'Bob', 'name': 'Bob', 'password': 'password', 'roles': 'admin'},
                 {'id': 'mary', 'name': 'Mary', 'password': 'password', 'roles': 'cluster_admin'},
                 {'id': 'john_cluster','name':'john_cluster','password':'password', 'roles': 'cluster_admin'},
                 {'id': 'ro_non_ldap','name':'ro_non_ldap','password':'readonlypassword', 'roles': 'ro_admin'},
                 {'id': 'john_admin','name':'john_admin','password':'password', 'roles': 'admin'}]
        RbacBase().create_user_source(users, 'ldap', self.master)
        RbacBase().add_user_role(users, RestConnection(self.master), 'ldap')

    def _perform_offline_upgrade(self):
        for server in self.servers:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()
            self.upgrade_servers.append(server)
        upgrade_threads = self._async_update(self.upgrade_to, self.servers)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        self.sleep(20)
        self.add_built_in_server_user()
        self.sleep(20)
        self.upgrade_servers = self.servers



    def _perform_online_upgrade_with_rebalance(self):
        self.nodes_upgrade_path = self.input.param("nodes_upgrade_path", "").split("-")
        for service in self.nodes_upgrade_path:
            nodes = self.get_nodes_from_services_map(service_type=service, get_all_nodes=True)
            log.info("----- Upgrading all {0} nodes -----".format(service))
            for node in nodes:
                node_rest = RestConnection(node)
                node_info = "{0}:{1}".format(node.ip, node.port)
                node_services_list = node_rest.get_nodes_services()[node_info]
                node_services = [",".join(node_services_list)]
                if "n1ql" in node_services_list:
                    n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql",
                                                              get_all_nodes=True)
                    if len(n1ql_nodes) > 1:
                        for n1ql_node in n1ql_nodes:
                            if node.ip != n1ql_node.ip:
                                self.n1ql_node = n1ql_node
                                break
                log.info("Rebalancing the node out...")
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],[], [node])
                rebalance.result()
                active_nodes = []
                for active_node in self.servers:
                    if active_node.ip != node.ip:
                        active_nodes.append(active_node)
                log.info("Upgrading the node...")
                upgrade_th = self._async_update(self.upgrade_to, [node])
                for th in upgrade_th:
                     th.join()
                log.info("==== Upgrade Complete ====")
                log.info("Adding node back to cluster...")
                rebalance = self.cluster.async_rebalance(active_nodes,
                                                 [node], [],
                                                 services=node_services)
                rebalance.result()
                self.sleep(60)
                node_version = RestConnection(node).get_nodes_versions()
                log.info("{0} node {1} Upgraded to: {2}".format(service, node.ip, node_version))

    def _perform_online_upgrade_with_failover(self):
        self.nodes_upgrade_path = self.input.param("nodes_upgrade_path", "").split("-")
        for service in self.nodes_upgrade_path:
            nodes = self.get_nodes_from_services_map(service_type=service, get_all_nodes=True)
            log.info("----- Upgrading all {0} nodes -----".format(service))
            for node in nodes:
                node_rest = RestConnection(node)
                node_info = "{0}:{1}".format(node.ip, node.port)
                node_services_list = node_rest.get_nodes_services()[node_info]
                node_services = [",".join(node_services_list)]
                log.info("Rebalancing the node out...")
                failover_task = self.cluster.async_failover([self.master],
                                                            failover_nodes=[node],
                                                            graceful=False)
                failover_task.result()
                active_nodes = []
                for active_node in self.servers:
                    if active_node.ip != node.ip:
                        active_nodes.append(active_node)
                log.info("Upgrading the node...")
                upgrade_th = self._async_update(self.upgrade_to, [node])
                for th in upgrade_th:
                     th.join()
                log.info("==== Upgrade Complete ====")
                self.sleep(30)
                log.info("Adding node back to cluster...")
                rest = RestConnection(self.master)
                nodes_all = rest.node_statuses()
                for cluster_node in nodes_all:
                    if cluster_node.ip == node.ip:
                        log.info("Adding Back: {0}".format(node))
                        rest.add_back_node(cluster_node.id)
                        rest.set_recovery_type(otpNode=cluster_node.id,
                                               recoveryType="full")
                log.info("Adding node back to cluster...")
                rebalance = self.cluster.async_rebalance(active_nodes, [], [])
                rebalance.result()
                self.sleep(60)
                node_version = RestConnection(node).get_nodes_versions()
                log.info("{0} node {1} Upgraded to: {2}".format(service, node.ip,
                                                                node_version))