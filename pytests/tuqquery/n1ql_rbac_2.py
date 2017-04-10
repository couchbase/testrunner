from tuq import QueryTests
from TestInput import TestInputSingleton
from security.rbac_base import RbacBase
from lib.membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class RbacN1QL(QueryTests):

    def setUp(self):
        super(RbacN1QL, self).setUp()
        users = TestInputSingleton.input.param("users", None)
        self.all_buckets = TestInputSingleton.input.param("all_buckets", False)
        print users
        self.inp_users = []
        if users:
            self.inp_users = eval(eval(users))
        self.users = self.get_user_list()
        self.roles = self.get_user_role_list()

    def tearDown(self):
        super(RbacN1QL, self).tearDown()

    def create_users(self, users=None):
        """
        :param user: takes a list of {'id': 'xxx', 'name': 'some_name ,
                                        'password': 'passw0rd'}
        :return: Nothing
        """
        if not users:
            users = self.users
        RbacBase().create_user_source(users,'builtin',self.master)
        self.log.info("SUCCESS: User(s) %s created"
                      % ','.join([user['name'] for user in users]))

    def assign_role(self, rest=None, roles=None):
        if not rest:
            rest = RestConnection(self.master)
        #Assign roles to users
        if not roles:
            roles = self.roles
        RbacBase().add_user_role(roles, rest,'builtin')
        for user_role in roles:
            self.log.info("SUCCESS: Role(s) %s assigned to %s"
                          %(user_role['roles'], user_role['id']))

    def delete_role(self, rest=None, user_ids=None):
        if not rest:
            rest = RestConnection(self.master)
        if not user_ids:
            user_ids = [user['id'] for user in self.roles]
        RbacBase().remove_user_role(user_ids, rest)
        self.sleep(20, "wait for user to get deleted...")
        self.log.info("user roles revoked for %s" % ", ".join(user_ids))

    def get_user_list(self):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
        'password': 'passw0rd'}
        """
        user_list = []
        for user in self.inp_users:
            user_list.append({att: user[att] for att in ('id',
                                                         'name',
                                                         'password')})
        return user_list

    def get_user_role_list(self):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
         'roles': 'admin:fts_admin[default]'}
        """
        user_role_list = []
        for user in self.inp_users:
            user_role_list.append({att: user[att] for att in ('id',
                                                              'name',
                                                              'roles')})
        return user_role_list

    def grant_role(self):
        role = self.roles[0]['roles']
        if self.all_buckets:
            role += "(`*`)"
        self.query = "GRANT ROLE {0} to {1}".format(role, self.users[0]['id'])
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['status'] == 'success', "Unable to grant role {0} to {1}".
                                                                format(self.buckets[0].name, self.users[0]['id']))

    def test_select(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "curl -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_update(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        old_name = "employee-14"
        new_name = "employee-14-2"
        cmd = "curl -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1 returning a.name'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, old_name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_insert(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "curl -u %s:%s http://%s:8093/query/service -d " \
              "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"1\", { \"value1\": \"one1\" }) RETURNING *'"%\
                (self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_delete(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        del_name = "employee-14"
        cmd = "curl -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name = '{4}' RETURNING a.name'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, del_name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")
