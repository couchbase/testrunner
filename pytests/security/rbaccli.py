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
        cli_command = 'admin-role-manage'
        options = options
        remote_client = RemoteMachineShellConnection(self.master)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
        return output, error

    def check_role_assignment(self,final_user_id, user_role,output,msg=None ):
        if msg == None:
            self.assertTrue("SUCCESS: set roles for" in output[0],"Issue with role assignment")
        else:
            self.assertTrue(msg in output[0],"Issue with role assignment")

    def setup_user_roles(self):
        final_user_id = rbacmain().returnUserList(self.user_id)
        final_roles = rbacmain()._return_roles(self.user_role)
        payload = "name=" + self.user_name + "&roles=" + final_roles
        for final_user in final_user_id:
            status, content, header =  rbacmain(self.master)._set_user_roles(user_name=final_user[0],payload=payload)
            self.assertTrue(status,"Issue with setting role")
            status = rbacmain()._parse_get_user_response(json.loads(content),final_user[0],self.user_name,final_roles)
            self.assertTrue(status,"Role assignment not matching")

    def test_get_roles(self):
        self.setup_user_roles()
        output, error = self.execute_admin_role_manage("--get-roles")
        final_user_id = rbacmain().returnUserList(self.user_id)
        self.check_role_assignment(final_user_id,self.user_role,output,"SUCCESS: user/role list:")

    def test_set_roles(self):
        final_user_id = rbacmain().returnUserList(self.user_id)
        print final_user_id
        user_list = ""
        if len(final_user_id) == 1:
            user_list = str(final_user_id[0])
        else:
            for final_user in final_user_id:
                user_list = user_list + "," + str(final_user[0])
            user_list = user_list[1:]

        final_roles = rbacmain()._return_roles(self.user_role)
        options = "--set-users=" + user_list + " --roles=" + final_roles
        output, error = self.execute_admin_role_manage(options)
        self.check_role_assignment(final_user_id,self.user_role,output)

    def test_delete_user_role(self):
        self.setup_user_roles()
        final_user_id = rbacmain().returnUserList(self.user_id)
        print final_user_id
        user_list = ""
        if len(final_user_id) == 1:
            user_list = str(final_user_id[0][0])
        else:
            for final_user in final_user_id:
                user_list = user_list + "," + str(final_user[0])
            user_list = user_list[1:]

        options = "--delete-users=" + user_list
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("SUCCESS: removed users" in output[0],"Issue with user deletion")


    def test_delete_user_role_not_exits(self):
        user_list = "temp1"
        options = "--delete-users=" + user_list
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("User was not found." in output[0],"Incorrect error message for incorrect user to delete")

    def test_without_roles(self):
        user_list = "temp1"
        options = "--set-users=" + user_list
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: You must specify lists of both users and roles for those users." in output[0],"Incorrect error message for missing roles in cli")
        self.assertTrue(" --set-users=[comma delimited user list] --roles=[comma-delimited list of one or more from full_admin, readonly_admin, cluster_admin, replication_admin, bucket_admin(opt bucket name), view_admin]" in output[1],"Incorrect error message for missing roles in cli")


    def test_incorrect_option(self):
        user_list = "temp1"
        options = "--roles=" + user_list
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: You must specify lists of both users and roles for those users." in output[0],"for incorrect switch")


    def test_set_roles_with_name(self):
        self.user_name = self.input.param("user_name",None)
        final_user_name = rbacmain().returnUserList(self.user_name)
        final_user_id = rbacmain().returnUserList(self.user_id)

        user_list = ""
        user_name = ""
        if len(final_user_id) == 1:
            user_list = str(final_user_id[0])
            user_name = str(final_user_name[0])
        else:
            for final_user in final_user_id:
                user_list = user_list + "," + str(final_user[0])

            for final_name in final_user_name:
                user_name = user_name + "," + str(final_name)

            user_list = user_list[1:]
            user_name = user_name[3:-2]
            user_name = "\"" + user_name + "\""

        final_roles = rbacmain()._return_roles(self.user_role)
        options = "--set-users=" + user_list + " --roles=" + final_roles + " --set-names=" + user_name
        output, error = self.execute_admin_role_manage(options)
        self.check_role_assignment(final_user_id,self.user_role,output)