import json
from .tuq import QueryTests
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

class N1QLRBACTests(QueryTests):

    def setUp(self):
        super(N1QLRBACTests, self).setUp()
        self.shell = RemoteMachineShellConnection(self.master)
        self.rest = RestConnection(self.master)
        self.port = self.input.param("port", "8091")
        self.cbqpath = '%scbq' % self.path + " -u %s -p %s" % (self.rest.username, self.rest.password)
        self.query_service_url = "'http://%s:%s/query/service'" % (self.master.ip, self.n1ql_port)

    def suite_setUp(self):
        super(N1QLRBACTests, self).suite_setUp()

        cmd = 'curl -X POST -u %s:%s -d name=bucket1 -d ramQuotaMB=100 -d authType=sasl -d password=pwd1' \
              ' http://%s:%s/pools/default/buckets'% (self.rest.username, self.rest.password, self.master.ip, self.port)
        o =self.shell.execute_command(cmd)
        cmd = 'curl -X POST -u %s:%s -d name=bucket2 -d ramQuotaMB=100 -d authType=sasl -d password=pwd2' \
              ' http://%s:%s/pools/default/buckets'% (self.rest.username, self.rest.password, self.master.ip, self.port)
        o =self.shell.execute_command(cmd)
        cmd = 'curl -X POST -u %s:%s -d name=bucket3 -d ramQuotaMB=100 -d authType=None' \
              ' http://%s:%s/pools/default/buckets'% (self.rest.username, self.rest.password, self.master.ip, self.port)
        o =self.shell.execute_command(cmd)

    def tearDown(self):
        super(N1QLRBACTests, self).tearDown()

    def suite_tearDown(self):
        super(N1QLRBACTests, self).suite_tearDown()

    def test_users_built_in_sasl_auth(self):
        #Query the list of users. It should be empty, returning an empty list, [ ].
        cmd = 'curl http://%s:%s/settings/rbac/users -u %s:%s'  % (self.master.ip, self.port, self.rest.username, self.rest.password)
        o =self.shell.execute_command(cmd)
        self.assertTrue("cbadminbucket" not in str(o))
        #Create an internal user with one role
        cmd = 'curl -X PUT http://%s:%s/settings/rbac/users/builtin/user1 -d "name=User1&roles=data_reader_writer[bucket2]&' \
               'password=pwd1" -u %s:%s'%(self.master.ip, self.port, self.username, self.password)
        self.shell.execute_command(cmd)
        #Create an external user with one role
        cmd = 'curl -X PUT http://%s:%s/settings/rbac/users/builtin/intuser -d "name=User2&roles=data_reader_writer[bucket2]' \
              '&password=pwd2" -u %s:%s'%(self.master.ip, self.port, self.username, self.password)
        self.shell.execute_command(cmd)
        #Query the list of users again
        cmd = 'curl http://%s:%s/settings/rbac/users -u %s:%s'%(self.master.ip, self.port, self.username, self.password)
        self.shell.execute_command(cmd)
        cmd = 'curl http://%s:%s/settings/rbac/users -u %s:%s'%(self.master.ip, self.port, 'User1', 'pwd1')
        self.shell.execute_command(cmd)
        cmd = 'curl http://%s:%s/settings/rbac/users -u %s:%s'%(self.master.ip, self.port, 'User2', 'pwd2')
        self.shell.execute_command(cmd)

    def test_users_crud_operation(self):
        #Query the list of users. It should be empty, returning an empty list, [ ]
        cmd = 'curl http://%s:%s/settings/rbac/users -u %s:%s'%(self.master.ip, self.port, self.username, self.password)
        self.shell.execute_command(cmd)
        #Create an internal user with one role:
        cmd = 'curl -X PUT http://%s:%s/settings/rbac/users/builtin/intuser -d "name=TestInternalUser&roles=' \
              'data_reader[bucket2]&password=pwintuser" -u %s:%s'%(self.master.ip, self.port, self.username, self.password)
        self.shell.execute_command(cmd)
        #Read the internal user
        cmd = 'curl http://%s:%s/settings/rbac/users/builtin/intuser -u %s:%s'%(self.master.ip, self.port, self.username, self.password)
        o =self.shell.execute_command(cmd)
        new_curl = json.dumps(o)
        string_curl = json.loads(new_curl)







