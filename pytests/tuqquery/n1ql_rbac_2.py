from couchbase_helper.documentgenerator import BlobGenerator
from .tuq import QueryTests
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

    def test_select(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "{4} -u {0}:{1} http://{2}:8093/query/service -d 'statement=SELECT * from {3} LIMIT 10'".\
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.curl_path)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, old_name, self.curl_path)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, del_name, self.curl_path)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name, 'name')
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        self.assertTrue(any("success" in line for line in output), "Unable to merge {0} and {1} as user {2}".
                        format(self.buckets[0].name, self.buckets[0].name, self.users[0]['id']))
        self.log.info("Query executed successfully")
        #test for joins with subquery
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=SELECT b1.b1id,b2.name FROM (select d.*,meta(d).id b1id from %s d) b1 JOIN %s b2 " \
              "ON KEYS  b1.docid where b1.b1id > 1' " %\
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
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
              (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
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
              (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
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
              (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=CREATE INDEX `age-index2` ON %s(age)  USING GSI WITH {\"defer_build\":true}'"%\
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
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

    def test_create_alter_index(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=CREATE INDEX `age-index` ON %s(age) USING GSI WITH {\"nodes\":\"%s\"}'" % \
              (self.curl_path, self.master.rest_username, self.master.rest_password,
               self.master.ip, self.buckets[0].name, self.master.ip+":"+self.master.port)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        if "views_admin" in self.roles[0]['roles']:
            self.assertTrue(any("success" not in line for line in output),
                            "Able to create index on {0} as user {1}".
                            format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Create Query failed as expected")
        else:
            self.assertTrue(any("success" in line for line in output),
                            "Unable to create index on {0} as user {1}".
                            format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Create Query executed successfully")
        cmd = "%s -u %s:%s http://%s:8093/query/service -d " \
              "'statement=ALTER INDEX %s.`age-index` WITH {\"action\":\"move\", \"nodes\":\"%s\"}'" % \
              (self.curl_path, self.users[0]['id'], self.users[0]['password'],
               self.master.ip, self.buckets[0].name, self.servers[1].ip+":"+self.servers[1].port)
        output, error = shell.execute_command(cmd)
        shell.log_command_output(output, error)
        valid_roles = ["admin", "bucket_admin", "views_admin", "query_manage_index"]
        role_list = self.roles[0]['roles']
        if any((True for x in valid_roles if x in role_list)):
            self.assertTrue(any("success" not in line for line in output),
                            "Unable to alter index on {0} as user {1}".
                            format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Alter Query executed successfully")
            self.sleep(120, "Allowing alter index to complete in the background before test cleanup")
        else:
            self.assertFalse(any("success" in line for line in output),
                            "Able to alter index on {0} as user {1}".
                            format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Alter Query failed as expected")


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
        roles = ["select", "insert", "update", "delete"]
        assigned_role = self.get_user_role_list()[0]['roles']
        for role in roles:
            self.query = 'grant {0} on {1} to {2}'.format(role, 'default', 'test')
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
                     %(self.curl_path, 'test', 'password', self.master.ip, 'default')
             output, error = shell.execute_command(cmd)
             shell.log_command_output(output, error)
             self.assertTrue(any("success" in line for line in output), "Unable to select")
             self.log.info("Query executed successfully")




    def test_revoke_role(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        shell = RemoteMachineShellConnection(self.master)
        roles = ["select", "insert", "update", "delete"]
        assigned_role = self.get_user_role_list()[0]['roles']

        for role in roles:
            self.query = 'revoke {0} on {1} from {2}'.format(role, 'default', 'test')
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
                     %(self.curl_path, 'test', 'password', self.master.ip, 'default')
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
                format(self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
            output, error = shell.execute_command(cmd)
            shell.log_command_output(output, error)
            self.assertTrue(any("success" in line for line in output), "Unable to prepare delete from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Prepare query executed successfully")
        elif "update" in self.roles[0]['roles']:
         cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=PREPARE UPDATE {4} a set name = 'test1' where name = 'test''".\
                format(self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
         output, error = shell.execute_command(cmd)
         shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to prepare select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
         self.log.info("Prepare query executed successfully")
        elif "insert" in self.roles[0]['roles']:
         cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=PREPARE INSERT INTO {4} values(\"k051\", 123  )'".\
                format(self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
         output, error = shell.execute_command(cmd)
         shell.log_command_output(output, error)
         self.assertTrue(any("success" in line for line in output), "Unable to prepare insert from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
         self.log.info("Prepare query executed successfully")
        else:
         cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=PREPARE SELECT * from {4} LIMIT 10'".\
                format(self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
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
                format(self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
            output, error = shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to explain select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Explain query executed successfully")
        elif "insert" in self.roles[0]['roles'] :
            cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=EXPLAIN INSERT INTO {4} values(\"k051\", 123  )'".\
                format(self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
            output, error = shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to explain select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Explain query executed successfully")
        elif "update" in self.roles[0]['roles'] :
            cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=EXPLAIN UPDATE {4} a set name = \"test1\" where name = \"test\"'".\
                format(self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
            output, error = shell.execute_command(cmd)
            self.assertTrue(any("success" in line for line in output), "Unable to explain select from {0} as user {1}".
                        format(self.buckets[0].name, self.users[0]['id']))
            self.log.info("Explain query executed successfully")
        else:
         cmd = "{0} -u {1}:{2} http://{3}:8093/query/service -d 'statement=EXPLAIN SELECT * from {4} LIMIT 10'".\
                format(self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name)
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
        _, content, _ = self.retrieve_users()
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
        _, content, _ = self.retrieve_users()
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
        _, content, _ = self.retrieve_users()
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
        _, content, _ = self.retrieve_users()
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
        _, content, _ = self.retrieve_users()
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
        _, content, _ = self.retrieve_users()
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, old_name, self.curl_path)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, old_name, self.curl_path)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, self.buckets[0].name, self.curl_path)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name, self.curl_path)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, new_name, self.buckets[0].name, self.curl_path)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[0].name, self.buckets[0].name, self.curl_path)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name)
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
                (self.curl_path, self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, new_name, self.buckets[0].name, self.curl_path)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, new_name, self.buckets[0].name, self.curl_path)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name, self.curl_path)
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
                format(self.users[0]['id'], self.users[0]['password'], self.master.ip, self.buckets[1].name, self.buckets[0].name, self.curl_path)
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
        self.system_catalog_helper_select("test_select_system_catalog", role)
        self.system_catalog_helper_delete("test_select_system_catalog", role)
        self.system_catalog_helper_insert("test_select_system_catalog", role)
        self.system_catalog_helper_update("test_select_system_catalog", role)
        self.select_my_user_info()


    # This test will select/delete on any system table with read only admin user.
    def test_read_only_admin_select_delete(self):
        self.create_users()
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_select("test_read_only_admin_select_delete", role)
        self.system_catalog_helper_delete("test_read_only_admin_select_delete", role)
        self.select_my_user_info()

    # This test will be specific to system catalog role.
    def test_sys_catalog(self):
        self.create_users()

        rest = RestConnection(self.master)
        self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_select("test_sys_catalog", role)
        perm =  RbacBase().check_user_permission(
                        self.users[0]['id'],
                        self.users[0]['password'],
                        'cluster.bucket[%s].n1ql.select!execute' %(self.buckets[0].name), rest
                        )
        self.log.info("Permissions for user: %s on bucket %s is: %s"
                              %(self.users[0]['id'], self.buckets[0].name, perm))

        self.system_catalog_helper_delete("test_sys_catalog", role)
        self.system_catalog_helper_insert("test_sys_catalog", role)
        self.system_catalog_helper_update("test_sys_catalog", role)

    def test_query_select_role(self):
        self.create_users()
        #self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_select("test_query_select_role", role)
        self.select_my_user_info()

    def test_query_insert_role(self):
        self.create_users()
        #self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_insert("test_query_insert_role", role)

    def test_query_update_role(self):
        self.create_users()
        #self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_update("test_query_update_role", role)

    def test_query_delete_role(self):
        self.create_users()
        #self.shell.execute_command("killall cbq-engine")
        self.grant_role()
        role = self.roles[0]['roles']
        self.system_catalog_helper_delete("test_query_delete_role", role)

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
