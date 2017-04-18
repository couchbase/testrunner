import httplib2
import httplib
import base64
import json
import urllib
import urllib2
import ssl
import socket
import paramiko


from security.rbacTest import rbacTest
from security.rbacmain import rbacmain
from remote.remote_util import RemoteMachineShellConnection
from clitest.cli_base import CliBaseTest

class ServerInfo():
    def __init__(self,
                 ip,
                 port,
                 ssh_username,
                 ssh_password,
                 ssh_key=''):

        self.ip = ip
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.port = port
        self.ssh_key = ssh_key



class rbacclitest(rbacTest):

    def setUp(self):
        super(rbacclitest, self).setUp()
        self.ldapUser = self.input.param('ldapuser','Administrator')
        self.ldapPass = self.input.param('ldappass','password')
        self.user_name = self.input.param("user_name")

    def tearDown(self):
        super(rbacclitest, self).tearDown()

    def execute_admin_role_manage(self, options):
        cli_command = 'user-manage'
        options = options
        remote_client = RemoteMachineShellConnection(self.master)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
        return output, error

    def _get_user_role(self):
        final_user_id = rbacmain().returnUserList(self.user_id)
        final_roles = rbacmain()._return_roles(self.user_role)
        return final_user_id, final_roles

    def test_create_user_without_auth(self):
        users, roles = self._get_user_role()
        options = "--set " + "--rbac-username " + users[0][0] + " --rbac-password " + users[0][1] + " --roles " + roles
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: --auth-type is required with the --set option" in output[0], "Issue with command without auth")

    def test_create_user_without_rbac_user(self):
        users, roles = self._get_user_role()
        options = "--set " + " --rbac-password " + users[0][1] + " --roles " + roles
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: --rbac-username is required with the --set option" in output[0], "Issue with command without rbacusername")

    def test_create_user_without_rbac_pass(self):
        users, roles = self._get_user_role()
        options = "--set " + "--rbac-username " + users[0][0] + " --roles " + roles + " --auth-type builtin"
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: --rbac-password is required with the --set option" in output[0], "Issue with command without rbac_pass")

    def test_create_user_without_role(self):
        users, roles = self._get_user_role()
        options = "--set " + "--rbac-username " + users[0][0] + " --rbac-password " + users[0][1] +  \
                  " --auth-type " + self.auth_type
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: --roles is required with the --set option" in output[0],"Issue with command without role")

    def test_create_user(self):
        users, roles = self._get_user_role()
        options = "--set " + "--rbac-username " + users[0][0] + " --roles " + roles \
                  + " --auth-type " + self.auth_type
        if self.auth_type == "builtin":
            options += " --rbac-password " + users[0][1]
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("SUCCESS: RBAC user set" in output[0],"Issue with command create_user")

    def test_create_user_name(self):
        users, roles = self._get_user_role()
        options = "--set " + "--rbac-username " + users[0][0] + " --rbac-password " + users[0][1] + " --roles " + roles \
                  + " --auth-type " + self.auth_type + " --rbac-name " + self.user_name
        if self.auth_type == "builtin":
            options += " --rbac-password " + users[0][1]
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("SUCCESS: RBAC user set" in output[0],"Issue with command create user name")

    def test_delete_user(self):
        self.test_create_user()
        users, roles = self._get_user_role()
        options = "--delete " + "--rbac-username " + users[0][0] + " --auth-type=" + self.auth_type
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("SUCCESS: RBAC user removed" in output[0], "Issue with command of delete user")

    def test_delete_user_noexist(self):
        users, roles = self._get_user_role()
        options = "--delete " + "--rbac-username userdoesexist --auth-type=" + self.auth_type
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: \"User was not found." in output[0], "Issue with delete user that does not exist")

    def test_my_role(self):
        final_out = ''
        options = "--my-roles "
        self.test_create_user()
        users, roles = self._get_user_role()
        self.ldapUser = users[0][0]
        self.ldapPass = users[0][1]
        output, error = self.execute_admin_role_manage(options)
        for out in output:
            final_out = final_out + out
        test = json.loads(final_out)
        for role in test['roles']:
            self.assertTrue(role['role'] in roles,"Issue with --my-roles")

    def test_list_roles(self):
        final_out = ''
        options = "--list "
        self.test_create_user()
        users, roles = self._get_user_role()
        self.ldapUser = users[0][0]
        self.ldapPass = users[0][1]
        output, error = self.execute_admin_role_manage(options)
        for out in output:
            final_out = final_out + out
        test = json.loads(final_out)
        for role in test['roles']:
            self.assertTrue(role['role'] in roles,"Issue with --my-roles")