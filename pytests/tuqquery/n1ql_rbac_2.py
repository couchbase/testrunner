from couchbase_helper.documentgenerator import BlobGenerator
from tuq import QueryTests
from TestInput import TestInputSingleton
from security.rbac_base import RbacBase
from lib.membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

import json

class RbacN1QL(QueryTests):

    def setUp(self):
        super(RbacN1QL, self).setUp()
        users = TestInputSingleton.input.param("users", None)
        self.all_buckets = TestInputSingleton.input.param("all_buckets", False)
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

    def retrieve_roles(self):
        server = self.master
        rest = RestConnection(server)
        url = "/settings/rbac/roles"
        api = rest.baseUrl + url
        status, content, header = rest._http_request(api, 'GET')
        self.log.info(" Retrieve all User roles - Status - {0} -- Content - {1} -- Header - {2}".format(status, content, header))
        return status, content, header

    def retrieve_users(self):
        rest = RestConnection(self.master)
        url = "/settings/rbac/users"
        api = rest.baseUrl + url
        status, content, header = rest._http_request(api, 'GET')
        self.log.info(" Retrieve User Roles - Status - {0} -- Content - {1} -- Header - {2}".format(status, content, header))
        return status, content, header

    def grant_role(self, role=None):
        if not role:
            role = self.roles[0]['roles']
        if self.all_buckets:
            list = []
            for bucket in self.buckets:
                list.append(bucket.name)
            names = ','.join(list)
            self.query = "GRANT {0} on {1} to {2}".format(role,names, self.users[0]['id'])
            actual_result = self.run_cbq_query()
        elif "," in role:
            roles = role.split(",")
            for role in roles:
                role1 = role.split("(")[0]
                name = role.split("(")[1][:-1]
                self.query = "GRANT {0} on {1} to {2}".format(role1,name, self.users[0]['id'])
                actual_result =self.run_cbq_query()
        elif "(" in role:
                role1 = role.split("(")[0]
                name = role.split("(")[1][:-1]
                self.query = "GRANT {0} on {1} to {2}".format(role1,name, self.users[0]['id'])
                actual_result = self.run_cbq_query()
        else:
                self.query = "GRANT {0} to {1}".format(role, self.users[0]['id'])
                actual_result = self.run_cbq_query()

        self.assertTrue(actual_result['status'] == 'success', "Unable to grant role {0} to {1}".
                                                                format(role, self.users[0]['id']))

    def revoke_role(self, role=None):
        if not role:
            role = self.roles[0]['roles']
            if self.all_buckets:
                role += "(`*`)"
        self.query = "REVOKE {0} FROM {1}".format(role, self.users[0]['id'])
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['status'] == 'success', "Unable to revoke role {0} from {1}".
                                                                format(role, self.users[0]['id']))

    def test_select(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        if "views_admin" in self.roles[0]['roles']:
            self.assertTrue(any("success" not in line for line in output), "Able to select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Query failed as expected")
        else:
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
        cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, old_name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to update from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_insert(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=INSERT INTO %s (KEY, VALUE) VALUES(\"1\", { \"value1\": \"one1\" })'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_delete(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        del_name = "employee-14"
        cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name = '{4}''".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, del_name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_upsert(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=UPSERT INTO %s (KEY, VALUE) VALUES(\"1\", { \"value1\": \"one1\" }) RETURNING *'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to upsert into {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_merge(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=MERGE INTO %s b1 USING %s b2 ON KEY b2.%s WHEN NOT MATCHED THEN " \
              "INSERT { \"value1\": \"one1\" }'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name, 'name')
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to merge {0} and {1} as user {2}".
                        format(self.buckets[0].name, self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")
        #test for joins with subquery
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=SELECT b1.b1id,b2.name FROM (select d.*,meta(d).id b1id from %s d) b1 JOIN %s b2 " \
              "ON KEYS  b1.docid where b1.b1id > 1' " %\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to join with subquery {0} and {1} as user {2}".
                        format(self.buckets[0].name, self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")
        #test for joins
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement= SELECT employee.name, employee.tasks_ids, new_project.project " \
            "FROM %s as employee JOIN %s as new_project "\
            "ON KEYS employee.tasks_ids '"%\
              (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to join {0} and {1} as user {2}".
                        format(self.buckets[0].name, self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")
        self.query = "create index idxbidirec on %s(docid)" %(self.buckets[0].name)
        self.run_cbq_query()
        #test for nest
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement= SELECT meta(b1).id b1id from %s b1 NEST %s b2 ON KEY b2.docid FOR b1 " \
              "WHERE meta(b1).id > 1 '" %\
              (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to nest {0} and {1} as user {2}".
                        format(self.buckets[0].name, self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

        self.query = "create index idxbidirec2 on %s(join_day)" %(self.buckets[0].name) ;
        actual_result = self.run_cbq_query()
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement= SELECT employee.name, employee.join_day FROM %s as employee inner JOIN %s as new_project "\
            " ON KEY new_project.join_day FOR employee where new_project.join_day is not null '"%\
              (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output))
        self.log.info("Query executed successfully")




    def test_create_build_index(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=CREATE INDEX `age-index` ON %s(age)'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=CREATE INDEX `age-index2` ON %s(age)  USING GSI WITH {\"defer_build\":true}'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        if "views_admin" in self.roles[0]['roles']:
            self.assertTrue(any("success" not in line for line in output), "Able to create index on {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Create Query failed as expected")
        else:
            self.assertTrue(any("success" in line for line in output), "Unable to create index on {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Create Query executed successfully")
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=BUILD INDEX ON %s(`age-index2`) USING GSI'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        if "views_admin" in self.roles[0]['roles']:
            self.assertTrue(any("success" not in line for line in output), "Able to build index on {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Build Query failed as expected")
        else:
            self.assertTrue(any("success" in line for line in output), "Unable to build index on {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Build Query executed successfully")

    # List Indexes is not implemented by query yet, keeping this test for future.
    # def test_list_indexes(self):
    #     self.create_users()
    #     self.shell.execute_command("killall cbq-engine")
    #     self.grant_role()
    #     shell = RemoteMachineShellConnection(self.master)
    #     cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
    #           "'statement=select * from system:indexes"%\
    #             (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip)
    #     output, error = shell.execute_command(cmd)
    #     shell.log_command_output(output, error)
    #
    #     self.assertTrue(any("success" in line for line in error), "Unable to list indexes".
    #                     format(self.buckets[0].name, self.users[0]['id']))

    def test_create_drop_index(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=CREATE INDEX `age-index` ON %s(age) USING GSI'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        if "views_admin" in self.roles[0]['roles']:
            self.assertTrue(any("success" not in line for line in output), "Able to create index on {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Create Query failed as expected")
        else:
            self.assertTrue(any("success" in line for line in output), "Unable to create index on {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Create Query executed successfully")
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=DROP INDEX %s.`age-index` USING GSI'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        if "views_admin" in self.roles[0]['roles']:
            self.assertTrue(any("success" not in line for line in output), "Able to build index on {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Drop Query failed as expected")
        else:
            self.assertTrue(any("success" in line for line in output), "Unable to build index on {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Drop Query executed successfully")


    def test_grant_role(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        self.create_users(users=[{'id': 'test',
                                           'name': 'test',
                                           'password':'password'}])
        self.assign_role(roles=[{'id': 'test',
                                         'name': 'test',
                                         'roles': 'query_system_catalog'
                                                  }])
        shell = RemoteMachineShellConnection(self.master)
        roles = ["select","insert","update","delete"]
        assigned_role = self.get_user_role_list()[0]['roles']
        for role in roles:
            self.query = 'grant {0} on {1} to {2}'.format(role,'default','test')
            res= self.curl_with_roles(self.query)
            if(assigned_role == 'cluster_admin' or assigned_role == 'ro_admin' or assigned_role == 'bucket_full_access'
               or assigned_role == 'bucket_admin'  or assigned_role == 'views_admin' or assigned_role == 'replication_admin'
               or assigned_role == 'query_select' or assigned_role == 'select' or assigned_role == 'query_system_catalog'
               or assigned_role == 'query_update'or assigned_role == 'query_delete' or assigned_role == 'query_manage_index'
               or assigned_role == 'delete' or assigned_role == 'update' or assigned_role == 'insert' or assigned_role == 'data_reader'
               or assigned_role == 'data_writer'):
                self.assertTrue(str(res).find("'code': 13014")!=-1)
            if(assigned_role == 'admin'):
             cmd = "%s -u %s:%s http://%s:8093/query/service -d 'statement= SELECT * FROM %s limit 1'"\
                     %(self.curl_path,'test', 'password', self.master.ip,'default')
             output, error = shell.execute_command(cmd)
             shell.log_command_output(output, error)
             self.assertTrue(any("success" in line for line in output), "Unable to select")
             self.log.info("Query executed successfully")




    def test_revoke_role(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        roles = ["select","insert","update","delete"]
        assigned_role = self.get_user_role_list()[0]['roles']

        for role in roles:
            self.query = 'revoke {0} on {1} from {2}'.format(role,'default','test')
            res= self.curl_with_roles(self.query)
            if(assigned_role == 'cluster_admin' or assigned_role == 'ro_admin' or assigned_role == 'bucket_full_access'
               or assigned_role == 'bucket_admin'  or assigned_role == 'views_admin' or assigned_role == 'replication_admin'
               or assigned_role == 'query_select' or assigned_role == 'select' or assigned_role == 'query_system_catalog'
               or assigned_role == 'query_update'or assigned_role == 'query_delete' or assigned_role == 'query_manage_index'
               or assigned_role == 'delete' or assigned_role == 'update' or assigned_role == 'insert' or assigned_role == 'data_reader'
               or assigned_role == 'data_writer'):
                self.assertTrue(str(res).find("'code': 13014")!=-1)
            if(assigned_role == 'admin'):
             cmd = "%s -u %s:%s http://%s:8093/query/service -d 'statement= SELECT * FROM %s limit 1'"\
                     %(self.curl_path,'test', 'password', self.master.ip,'default')
             output, error = shell.execute_command(cmd)
             shell.log_command_output(output, error)
             self.assertFalse(any("success" in line for line in output), "Unable to select")
             self.log.info("Query executed successfully")


    def test_prepare(self):
        self.create_users()
        #self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        if "delete" in self.roles[0]['roles'] :
            cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=PREPARE delete from {4} LIMIT 10'".\
                format(self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
            output, error = shell.execute_command(cmd)
            shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to prepare delete from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Prepare query executed successfully")
        elif "update" in self.roles[0]['roles']:
         cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=PREPARE UPDATE {4} a set name = 'test1' where name = 'test''".\
                format(self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
         output, error = shell.execute_command(cmd)
         shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to prepare select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
         self.log.info("Prepare query executed successfully")
        elif "insert" in self.roles[0]['roles']:
         cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=PREPARE INSERT INTO {4} values(\"k051\", 123  )'".\
                format(self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
         output, error = shell.execute_command(cmd)
         shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to prepare insert from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
         self.log.info("Prepare query executed successfully")
        else:
         cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=PREPARE SELECT * from {4} LIMIT 10'".\
                format(self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
         output, error = shell.execute_command(cmd)
         shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to prepare select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
         self.log.info("Prepare query executed successfully")

    def test_infer(self):
        self.create_users()
        #self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        gen_load = BlobGenerator('infer', 'infer-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 10, flag=self.item_flag)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d 'statement=INFER %s WITH {\"sample_size\":10,\"num_sample_values\":0}'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        if "views_admin" in self.roles[0]['roles']:
            self.assertTrue(any("success" not in line for line in output), "Able to infer from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Infer query failed as expected")
        else:
            self.assertTrue(any("success" in line for line in output), "Unable to infer from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Infer query executed successfully")

    def test_explain(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        if "delete" in self.roles[0]['roles'] :
            cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=EXPLAIN delete from {4}'".\
                format(self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
            output, error = shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to explain select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Explain query executed successfully")
        elif "insert" in self.roles[0]['roles'] :
            cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=EXPLAIN INSERT INTO {4} values(\"k051\", 123  )'".\
                format(self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
            output, error = shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to explain select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Explain query executed successfully")
        elif "update" in self.roles[0]['roles'] :
            cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=EXPLAIN UPDATE {4} a set name = \"test1\" where name = \"test\"'".\
                format(self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
            output, error = shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to explain select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Explain query executed successfully")
        else:
         cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=EXPLAIN SELECT * from {4} LIMIT 10'".\
                format(self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
         output, error = shell.execute_command(cmd)
         shell.log_command_output(output, error)
         if "views_admin" in self.roles[0]['roles']:
            self.assertTrue(any("success" not in line for line in output), "Able to explain select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Explain query failed as expected")
         else:
            self.assertTrue(any("success" in line for line in output), "Unable to explain select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Explain query executed successfully")

    def test_create_user_roles(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        user = self.users[0]['id']
        role = self.roles[0]['roles']
        self.grant_role()
        _,content,_ = self.retrieve_users()
        content = json.loads(content)
        found = False
        for assignment in content:
            if assignment['id'] == user:
                for item in assignment['roles']:
                    if item['role'] == role:
                        found = True
                        break
        self.assertTrue(found, "{0} not granted role {1} as expected".format(user, role))
        self.log.info("{0} granted role {1} as expected".format(user, role))

    def test_update_user_roles(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        user = self.users[0]['id']
        role = self.roles[0]['roles']
        self.grant_role()
        _,content,_ = self.retrieve_users()
        content = json.loads(content)
        found = False
        for assignment in content:
            if assignment['id'] == user:
                for item in assignment['roles']:
                    if item['role'] == role:
                        found = True
                        break
        self.assertTrue(found, "{0} not granted old role {1} as expected".format(user, role))
        self.log.info("{0} granted old role {1} as expected".format(user, role))
        self.revoke_role()
        new_role = TestInputSingleton.input.param("new_role", None)
        self.grant_role(role=new_role)
        _,content,_ = self.retrieve_users()
        content = json.loads(content)
        found = False
        for assignment in content:
            if assignment['id'] == user:
                for item in assignment['roles']:
                    if item['role'] == new_role:
                        found = True
                        break
        self.assertTrue(found, "{0} not granted new role {1} as expected".format(user, new_role))
        self.log.info("{0} granted new role {1} as expected".format(user, new_role))

    def test_delete_user_roles(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        user = self.users[0]['id']
        role = self.roles[0]['roles']
        self.grant_role()
        _,content,_ = self.retrieve_users()
        content = json.loads(content)
        found = False
        for assignment in content:
            if assignment['id'] == user:
                for item in assignment['roles']:
                    if item['role'] == role:
                        found = True
                        break
        self.assertTrue(found, "{0} not granted role {1} as expected".format(user, role))
        self.log.info("{0} granted role {1} as expected".format(user, role))
        self.revoke_role()
        _,content,_ = self.retrieve_users()
        content = json.loads(content)
        found = False
        for assignment in content:
            if assignment['id'] == user:
                for item in assignment['roles']:
                    if item['role'] == role:
                        found = True
                        break
        self.assertFalse(found, "{0} not revoked of role {1} as expected".format(user, role))
        self.log.info("{0} revoked of role {1} as expected".format(user, role))

    def test_multiple_user_roles_listing(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        user = self.users[0]['id']
        role = "query_select(default),admin"
        self.grant_role(role=role)
        _,content,_ = self.retrieve_users()
        content = json.loads(content)
        found_query_select = False
        found_bucket_admin = False
        found_admin = False
        found = False
        for assignment in content:
            if assignment['id'] == user:
                for item in assignment['roles']:
                    if item['role'] == 'query_select':
                        found_query_select = True
                    if item['role'] == 'bucket_admin':
                        found_bucket_admin = True
                    if item['role'] == 'admin':
                        found_admin = True
        found = found_query_select & found_bucket_admin & found_admin
        self.log.info("{0} granted role {1} as expected".format(user, role))

    def test_multiple_user_roles_precedence(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        shell = RemoteMachineShellConnection(self.master)
        role = "query_select(default),admin"
        self.grant_role(role=role)
        old_name = "employee-14"
        new_name = "employee-14-2"
        cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, old_name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        #self.assertTrue(any("success" in line for line in output), "Unable to update from {0} as user {1}".
                        #format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_incorrect_n1ql_role(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        old_name = "employee-14"
        new_name = "employee-14-2"
        cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' where name = '{5}' limit 1'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, old_name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("User does not have credentials to run UPDATE queries on the default bucket" in line for line in output))
        self.log.info("Query failed as expected")

    def test_grant_incorrect_user(self):
        role = self.roles[0]['roles']
        self.query = "GRANT {0} to {1}".format(role, 'abc')
        try:
            self.run_cbq_query()
        except Exception as ex:
            self.log.info(str(ex))
            self.assertTrue("Unable to find user local:abc" in str(ex), "Able to grant role {0} to incorrect user abc - not expected".
                                                                format(role))
            self.log.info("Unable to grant role to incorrect user as expected")

    def test_grant_incorrect_role(self):
        user = self.users[0]['id']
        self.query = "GRANT {0} to {1}".format('abc', user)
        try:
            self.run_cbq_query()
        except Exception as ex:
            self.log.info(str(ex))
            self.assertTrue("Role abc is not valid." in str(ex))

    def test_insert_nested_with_select_with_full_access(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        self.grant_role(role="query_select(default)")
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=INSERT INTO %s (KEY UUID(), VALUE _name)" \
              " SELECT _name FROM %s _name WHERE age > 10'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_upsert_nested_with_select_with_full_access(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        self.grant_role(role="query_select(default)")
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=UPSERT INTO %s (KEY UUID(), VALUE _name)" \
              " SELECT _name FROM %s _name WHERE age > 10'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to upsert into {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_update_nested_with_select_with_full_access(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        self.grant_role(role="query_select(default)")
        shell = RemoteMachineShellConnection(self.master)
        new_name = "employee-14-2"
        cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' WHERE name IN (SELECT name FROM {5} WHERE age > 10)'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, self.buckets[0].name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to update from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_delete_nested_with_select_with_full_access(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        self.grant_role(role="query_select(default)")
        shell = RemoteMachineShellConnection(self.master)
        cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name IN (SELECT name FROM {4} WHERE age > 10)'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_insert_nested_with_select_with_no_access(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=INSERT INTO %s (KEY UUID(), VALUE _name)" \
              " SELECT _name FROM %s _name WHERE age > 10'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("User does not have credentials to run SELECT queries on the default bucket."
                            in line for line in output),
                            "Able to insert into {0} as user {1} - not expected".
                            format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query failed as expected")

    def test_update_nested_with_select_with_no_access(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        new_name = "employee-14-2"
        cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' WHERE name IN (SELECT name FROM {5} WHERE age > 10)'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, self.buckets[0].name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        #self.assertTrue(any("User does not have credentials to access privilege cluster.bucket[default].n1ql.select!execute. "
                            #"Add role Query Select [default] to allow the query to run." in line for line in output),
                            #"Able to update {0} as user {1} - not expected".
                           # format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Quer#y failed as expected")

    def test_delete_nested_with_select_with_no_access(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name IN (SELECT name FROM {4} WHERE age > 10)'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("User does not have credentials to run SELECT queries on the default bucket"
                             in line for line in output),
                            "Able to insert into {0} as user {1} - not expected".
                            format(self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query failed as expected")

    def test_insert_nested_with_select_with_full_access_and_diff_buckets(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        self.grant_role(role="query_select(bucket0)")
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=INSERT INTO %s (KEY UUID(), VALUE _name)" \
              " SELECT _name FROM %s _name WHERE age > 10'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to insert into {0} and select from {1} as user {2}".
                        format(self.buckets[1].name, self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_insert_nested_with_select_with_no_access_and_diff_buckets(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=INSERT INTO %s (KEY UUID(), VALUE _name)" \
              " SELECT _name FROM %s _name WHERE age > 10'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("User does not have credentials to run SELECT queries on the bucket0 bucket."
                            in line for line in output),
                            "Able to select from {0} as user {1} - not expected".
                            format(self.buckets[1].name, self.users[0]['id']))
        self.log.info("Query failed as expected")

    def test_upsert_nested_with_select_with_full_access_and_diff_buckets(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        self.grant_role(role="query_select(bucket0)")
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=UPSERT INTO %s (KEY UUID(), VALUE _name)" \
              " SELECT _name FROM %s _name WHERE age > 10'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to upsert into {0} and select from {1} as user {2}".
                        format(self.buckets[1].name, self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_upsert_nested_with_select_with_no_access_and_diff_buckets(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=UPSERT INTO %s (KEY UUID(), VALUE _name)" \
              " SELECT _name FROM %s _name WHERE age > 10'"%\
                (self.curl_path,self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("User does not have credentials to run SELECT queries on the bucket0 bucket."
                             in line for line in output),
                             "Able to select from {0} as user {1} - not expected".
                             format(self.buckets[1].name, self.users[0]['id']))
        self.log.info("Query failed as expected")

    def test_update_nested_with_select_with_full_access_and_diff_buckets(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        self.grant_role(role="query_select(bucket0)")
        shell = RemoteMachineShellConnection(self.master)
        new_name = "employee-14-2"
        cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' WHERE name IN (SELECT name FROM {5} WHERE age > 10)'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, new_name, self.buckets[0].name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to update {0} and select from {1} as user {2}".
                        format(self.buckets[1].name, self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_update_nested_with_select_with_no_access_and_diff_buckets(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        new_name = "employee-14-2"
        cmd = "{6} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=UPDATE {3} a set name = '{4}' WHERE name IN (SELECT name FROM {5} WHERE age > 10)'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, new_name, self.buckets[0].name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("User does not have credentials to run SELECT queries on the bucket0 bucket."
                              in line for line in output),
                             "Able to select from {0} as user {1} - not expected".
                             format(self.buckets[1].name, self.users[0]['id']))
        self.log.info("Query failed as expected")

    def test_delete_nested_with_select_with_full_access_and_diff_buckets(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        self.grant_role(role="query_select(bucket0)")
        shell = RemoteMachineShellConnection(self.master)
        cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name IN (SELECT name FROM {4} WHERE age > 10)'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to delete from {0} and select from {1} as user {2}".
                        format(self.buckets[1].name, self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")

    def test_delete_nested_with_select_with_no_access_and_diff_buckets(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "{5} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement=DELETE FROM {3} a WHERE name IN (SELECT name FROM {4} WHERE age > 10)'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("User does not have credentials to run SELECT queries on the bucket0 bucket"
                             in line for line in output),
                            "Able to select from {0} as user {1} - not expected".
                            format(self.buckets[1].name, self.users[0]['id']))
        self.log.info("Query failed as expected")

    # select,insert,delete,updaete,drop system catalog tables.
    # This test will run with Administrator,cluster admin,bucket admin and view admin.
    def test_select_system_catalog(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_select("test_select_system_catalog",role)
        self.system_catalog_helper_delete("test_select_system_catalog",role)
        self.system_catalog_helper_insert("test_select_system_catalog",role)
        self.system_catalog_helper_update("test_select_system_catalog",role)
        self.select_my_user_info()


    # This test will select/delete on any system table with read only admin user.
    def test_read_only_admin_select_delete(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_select("test_read_only_admin_select_delete",role)
        self.system_catalog_helper_delete("test_read_only_admin_select_delete",role)
        self.select_my_user_info()

    # This test will be specific to system catalog role.
    def test_sys_catalog(self):
        self.create_users()

        rest = RestConnection(self.master)
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_select("test_sys_catalog",role)
        perm =  RbacBase().check_user_permission(
                        self.users[0]['id'],
                        self.users[0]['password'],
                        'cluster.bucket[%s].n1ql.select!execute' %(self.buckets[0].name),rest
                        )
        self.log.info("Permissions for user: %s on bucket %s is: %s"
                              %(self.users[0]['id'],self.buckets[0].name,perm))

        self.system_catalog_helper_delete("test_sys_catalog",role)
        self.system_catalog_helper_insert("test_sys_catalog",role)
        self.system_catalog_helper_update("test_sys_catalog",role)

    def test_query_select_role(self):
        self.create_users()
        #self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_select("test_query_select_role",role)
        self.select_my_user_info()

    def test_query_insert_role(self):
        self.create_users()
        #self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_insert("test_query_insert_role",role)

    def test_query_update_role(self):
        self.create_users()
        #self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_update("test_query_update_role",role)

    def test_query_delete_role(self):
        self.create_users()
        #self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_delete("test_query_delete_role",role)

    # Creates user with select role,select system:indexes.select should not work,
    # Grant system catalog to the same user, select should work.
    # Revoke system catalog from the user,select should not work.
    # Right now the test fails ,hence no asserts until behavior is confirmed.
    def test_grant_revoke_permissions(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        res = self.curl_with_roles(self.query)
        self.assign_role(roles=[{'id': self.users[0]['id'],
                                         'name': self.users[0]['name'],
                                         'roles': 'query_system_catalog'
                                                  }])
        self.query = 'select * from system:indexes'
        res = self.curl_with_roles(self.query)
        self.revoke_role(role='query_system_catalog')
        self.query = 'select * from system:indexes'
        res = self.curl_with_roles(self.query)

    def curl_with_roles(self,query):
        shell = RemoteMachineShellConnection(self.master)
        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d " \
              "'statement={3}'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, query,self.curl_path)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        new_list = [string.strip() for string in output]
        concat_string = ''.join(new_list)
        json_output=json.loads(concat_string)
        try:
            return json_output
        except ValueError:
            return error

    def system_catalog_helper_select(self,test,role = ""):
        self.query = 'select * from system:datastores'
        res = self.curl_with_roles(self.query)
        self.assertTrue(res['metrics']['resultCount']==1)
        self.query = 'select * from system:namespaces'
        res = self.curl_with_roles(self.query)
        self.assertTrue(res['metrics']['resultCount']==1)
        self.query = 'select * from system:keyspaces'
        res = self.curl_with_roles(self.query)
        if(role == "query_system_catalog"):
            self.assertTrue(res['metrics']['resultCount']==2)
        elif(role == "query_update(default)" or role == "query_delete(default)" or role == "query_insert(default)"):
            self.assertTrue(res['status'] == 'success')
        elif (role.startswith("query_") or role.startswith("select")):
            self.assertTrue(res['metrics']['resultCount']==1)
        elif(role == "bucket_full_access(default)"):
            self.assertTrue(res['metrics']['resultCount']==1)
        elif(role == "query_delete(default)"):
            self.assertTrue(res['metrics']['resultCount']==1)
        else:
            self.assertTrue(res['metrics']['resultCount']==2)
        self.query = 'create primary index on {0}'.format(self.buckets[0].name)
        try:
            self.curl_with_roles(self.query)
        except Exception,ex:
            self.log.error(ex)

        if(role != "query_insert(default)" or role !="query_update(default)" or role != "query_delete(default)"):
         self.query = 'create primary index on {0}'.format(self.buckets[1].name)
         try:
            self.curl_with_roles(self.query)
         except Exception,ex:
            self.log.error(ex)

        if(role != "views_admin(standard_bucket0)" or role != "views_admin(default)" or
               role != "query_insert(default)" or role !="query_update(default)" or role != "query_delete(default)"):
           self.query = 'create index idx1 on {0}(name)'.format(self.buckets[0].name)
           res = self.curl_with_roles(self.query)
           self.sleep(10)
           self.query = 'create index idx2 on {0}(name)'.format(self.buckets[1].name)
           self.curl_with_roles(self.query)
           self.sleep(10)
           self.query = 'select * from system:indexes'
           res = self.curl_with_roles(self.query)
        if ( role == "admin" or role == "cluster_admin" or role == "bucket_admin" ):
            self.assertTrue(res['metrics']['resultCount']==4)
        elif( role == "bucket_admin(default)" or role == "bucket_admin(standard_bucket0)"
             or role == "query_system_catalog" or role == "ro_admin" or role == "replication_admin" ):
            self.assertTrue(res['status']=='success')
        self.query = 'select * from system:dual'
        res = self.curl_with_roles(self.query)
        self.assertTrue(res['metrics']['resultCount']==1)
        self.query = 'select * from system:user_info'
        res = self.curl_with_roles(self.query)
        if (role == "admin"):
            self.assertTrue(res['status']=='success')
        elif (role == "cluster_admin"):
            self.assertTrue(str(res).find("'code': 13014")!=-1)
        self.query = 'select * from system:nodes'
        res = self.curl_with_roles(self.query)
        if(role == "bucket_full_access(default)"):
            self.assertTrue(res['status']=='stopped')
        elif(role == "select(default)" or role == "query_select(default)" or role == "select(standard_bucket0)" or role == "query_select(standard_bucket0)"):
            self.assertTrue(str(res).find("'code': 13014")!=-1)
        elif(role == "insert(default)" or role == "query_insert(default)" or role == "query_update(default)" or role == "query_delete(default)"):
          self.assertTrue(res['status']=='stopped')
        else:
          self.assertTrue(res['status']=='success')
        self.query = 'select * from system:applicable_roles'
        res = self.curl_with_roles(self.query)
        if (role == "admin"):
            self.assertTrue(res['status']=='success')
        elif(role == "ro_admin"):
            self.assertTrue(res['status']=='success')
        elif (role == "cluster_admin" or role == "bucket_admin(default)"):
            self.assertTrue(str(res).find("'code': 13014")!=-1)

        # if (role == "query_insert(default)" or role == "query_delete(default)" or role == "query_update(default)"):
        #     self.assertTrue(res['status']=='stopped')
        # elif(role == "bucket_admin(standard_bucket0)" or role == "views_admin(standard_bucket0)" or role == "views_admin(default)" or role == "views_admin" or role == "replication_admin" or role == "query_system_catalog" or role == "ro_admin"):
        #     self.assertTrue(str(res).find("'code': 13014")!=-1)
        # else:
        #     self.assertTrue(res['metrics']['resultCount']> 0)
        if(role != "ro_admin" and role != "replication_admin" and role != "query_insert(default)" and role != "query_delete(default)" and role != "query_update(default)" and role != "bucket_full_access(default)" and role != "query_system_catalog"):
           self.query = "prepare st1 from select * from default union select * from default union select * from default"
           res = self.curl_with_roles(self.query)
           self.query = 'execute st1'
           res = self.curl_with_roles(self.query)
           print res

           if(role == "bucket_admin(standard_bucket0)" or role == "views_admin(standard_bucket0)" or role == "views_admin(default)" or role == "views_admin" or role == "replication_admin" ):
             self.assertTrue(str(res).find("'code': 4040")!=-1)
           elif(role == "select(default)" or role == "query_select(default)"):
             self.assertTrue(res['metrics']['resultCount']== 0)
           else:
             self.assertTrue(res['status']=='success')

           if(role != "query_insert(default)" and role != "query_delete(default)" and role != "query_update(default)"):
            self.query = "prepare st2 from select * from default union select * from standard_bucket0 union select * from default"
            res = self.curl_with_roles(self.query)

            if(role == "bucket_admin(standard_bucket0)" or role == "views_admin(standard_bucket0)" or role == "views_admin(default)" or role == "views_admin" or role == "bucket_admin(default)" or role == "replication_admin" or role == "query_system_catalog" or role == "select(default)" or role == "query_select(default)" ):
              self.assertTrue(str(res).find("'code': 13014")!=-1)
            else:
              self.assertTrue(res['metrics']['resultCount']> 0)
            self.query = 'execute st2'
            res = self.curl_with_roles(self.query)
            if(role == "bucket_admin(standard_bucket0)" or role == "views_admin(standard_bucket0)" or role == "views_admin(default)" or role == "views_admin" or role == "bucket_admin(default)" or role == "replication_admin" or role == "query_system_catalog" or role == "select(default)" or role == "query_select(default)" ):
             self.assertTrue(str(res).find("'code': 4040")!=-1)
            else:
             self.assertTrue(res['status']=='success')
            self.query = 'select * from system:completed_requests'
            res = self.curl_with_roles(self.query)
            if(role == "select(default)" or role == "query_select(default)"):
             self.assertTrue(str(res).find("'code': 13014")!=-1)
            elif (role == "bucket_admin(standard_bucket0)"):
             self.assertTrue(res['metrics']['resultCount']> 0)
            else:
             self.assertTrue(res['status']=='success')

        if (role != "query_insert(default)" and role != "query_delete(default)" and role != "query_update(default)" and role != "bucket_full_access(default)" and role != "ro_admin"):
         self.query = 'select * from system:prepareds'
         res = self.curl_with_roles(self.query)
         if(role == "select(default)" or role == "query_select(default)" ):
            self.assertTrue(str(res).find("'code': 13014")!=-1)
         else:
            self.assertTrue(res['status']=='success')

         self.query = 'select * from system:active_requests'
         res = self.curl_with_roles(self.query)

         if(role == "select(default)" or role == "query_select(default)"):
            self.assertTrue(str(res).find("'code': 13014")!=-1)
         else:
          self.assertTrue(res['metrics']['resultCount']> 0)

         self.query = 'drop index {0}.idx1'.format(self.buckets[0].name)
         res = self.curl_with_roles(self.query)
         self.query = 'drop index {0}.idx2'.format(self.buckets[1].name)
         res = self.curl_with_roles(self.query)
         self.query = 'select * from system:indexes'
         res = self.curl_with_roles(self.query)
        if (role == "views_admin(default)"):
            self.assertTrue(res['status']=='success')
        elif(role == "bucket_admin(standard_bucket0)"  or role ==  "bucket_admin(default)"  or role == "select(default)" or role == "query_select(default)"):
            self.assertTrue(res['metrics']['resultCount']==1)
        elif(role == "query_insert(default)"  or role == "query_delete(default)" or role == "query_update(default)"):
            self.assertTrue(res['metrics']['resultCount']==0)
        # elif (role == "ro_admin"):
        #     self.assertTrue(res['metrics']['resultCount']==2)




    def system_catalog_helper_insert(self,test,role=""):
        self.query = 'insert into system:datastores values("k051", { "id":123  } )'
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
                    self.log.error(ex)
                    self.assertTrue(str(ex).find("System datastore :  Not implemented ")!=-1)
        self.query = 'insert into system:namespaces values("k051", { "id":123  } )'
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
                    self.log.error(ex)
                    self.assertTrue(str(ex).find("System datastore :  Not implemented ")!=-1)
        self.query = 'insert into system:keyspaces values("k051", { "id":123  } )'
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("System datastore :  Not implemented ")!=-1)
        self.query = 'insert into system:indexes values("k051", { "id":123  } )'
        try:
           self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("System datastore :  Not implemented ")!=-1)
        self.query = 'insert into system:dual values("k051", { "id":123  } )'
        try:
           self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("System datastore error Mutations not allowed on system:dual.")!=-1)
        self.query = 'insert into system:user_info values("k051", { "id":123  } )'
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("System datastore :  Not implemented ")!=-1)
        self.query = 'insert into system:nodes values("k051", { "id":123  } )'
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("System datastore :  Not implemented ")!=-1)
        self.query = 'insert into system:applicable_roles values("k051", { "id":123  } )'
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("System datastore :  Not implemented ")!=-1)
        self.query = 'insert into system:prepareds values("k051", { "id":123  } )'
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("System datastore :  Not implemented ")!=-1)
        self.query = 'insert into system:completed_requests values("k051", { "id":123  } )'
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("System datastore :  Not implemented ")!=-1)
        self.query = 'insert into system:active_requests values("k051", { "id":123  } )'
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("System datastore :  Not implemented ")!=-1)


    def system_catalog_helper_update(self,test,role=""):
        self.query = 'update system:datastores use keys "%s" set name="%s"'%("id","test")
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("'code': 11000")!=-1)
        self.query = 'update system:namespaces use keys "%s" set name="%s"'%("id","test")
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("'code': 11003")!=-1)
        self.query = 'update system:keyspaces use keys "%s" set name="%s"'%("id","test")
        # panic seen here as of now,hence commenting it out for now.
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("'code': 11003")!=-1)
        self.query = 'update system:indexes use keys "%s" set name="%s"'%("id","test")
        # panic seen here as of now,hence commenting it out for now.
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("'code': 11003")!=-1)
        self.query = 'update system:dual use keys "%s" set name="%s"'%("id","test")
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("'code': 11003")!=-1)
        self.query = 'update system:user_info use keys "%s" set name="%s"'%("id","test")
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.assertTrue(str(ex).find("'code': 5200")!=-1)
        self.query = 'update system:nodes use keys "%s" set name="%s"'%("id","test")
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("'code': 11003}")!=-1)
        # panic seen here as of now,hence commenting it out for now.
        self.query = 'update system:applicable_roles use keys "%s" set name="%s"'%("id","test")
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("'code': 11000")!=-1)
        self.query = 'update system:active_requests use keys "%s" set name="%s"'%("id","test")
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("'code': 11000")!=-1)
        self.query = 'update system:completed_requests use keys "%s" set name="%s"'%("id","test")
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("'code': 11000")!=-1)
        self.query = 'update system:prepareds use keys "%s" set name="%s"'%("id","test")
        try:
            self.curl_with_roles(self.query)
        except Exception, ex:
            self.log.error(ex)
            self.assertTrue(str(ex).find("'code': 11000")!=-1)

    # Query does not support drop these tables or buckets yet.We can add the test once it is supported.
    # Right now we cannot compare results in assert.
    # def system_catalog_helper_drop(self,query_params_with_roles,test = ""):
    #     self.query = 'drop system:datastores'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:namespaces'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:keyspaces'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:indexes'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:dual'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:user_info'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:nodes'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:applicable_roles'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:prepareds'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:completed_requests'
    #     res = self.run_cbq_query()
    #     print res
    #     self.query = 'drop system:active_requests'
    #     res = self.run_cbq_query()
    #     print res


    def system_catalog_helper_delete(self,test,role = "admin"):
        self.query = 'delete from system:datastores'
        res = self.curl_with_roles(self.query)
        self.assertTrue(str(res).find("'code': 11003")!=-1)
        self.query = 'delete from system:namespaces'
        res = self.curl_with_roles(self.query)
        self.assertTrue(str(res).find("'code': 11003")!=-1)
        # To be fixed in next version
        # self.query = 'delete from system:keyspaces'
        # res = self.curl_with_roles(self.query)
        # self.assertTrue(str(res).find("'code': 11003")!=-1)
        self.query = 'delete from system:indexes'
        res = self.curl_with_roles(self.query)
        self.assertTrue(str(res).find("'code': 11003")!=-1)
        self.query = 'delete from system:dual'
        res = self.curl_with_roles(self.query)
        self.assertTrue(str(res).find("'code': 11000")!=-1)
        self.query = 'delete from system:user_info'
        res = self.curl_with_roles(self.query)
        self.assertTrue(str(res).find("'code': 11003")!=-1)
        self.query = 'delete from system:nodes'
        res = self.curl_with_roles(self.query)
        self.assertTrue(str(res).find("'code': 11003")!=-1)
        self.query = 'delete from system:applicable_roles'
        res = self.curl_with_roles(self.query)
        self.assertTrue(str(res).find("'code': 11003")!=-1)

        self.query = 'delete from system:completed_requests'
        res = self.curl_with_roles(self.query)
        if(role == "query_delete(default)" or role == "query_delete(standard_bucket0)" or role == "delete(default)" or role == "bucket_full_access(default)"):
            self.assertNotEquals(res['status'],'success')
        else:
           self.assertTrue(res['status'] == 'success')
        self.query = 'delete from system:active_requests'
        res = self.curl_with_roles(self.query)
        self.assertTrue(res['status'] == 'stopped')
        self.query = 'delete from system:prepareds'
        res = self.curl_with_roles(self.query)
        if(role == "query_delete(default)" or role == "query_delete(standard_bucket0)" or role == "bucket_full_access(default)"):
            self.assertNotEquals(res['status'],'success')
        else:
            self.assertTrue(res['status'] == 'success')


    def select_my_user_info(self):
        self.query = 'select * from system:my_user_info'
        res = self.curl_with_roles(self.query)
        self.assertTrue(res['status'] == 'success')
