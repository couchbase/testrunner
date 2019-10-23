import json
import httplib2
import http.client
import base64
import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
import ssl
import socket
import paramiko


from security.rbacTest import rbacTest
from security.rbacmain import rbacmain
from remote.remote_util import RemoteMachineShellConnection
from security.ldapGroupBase import ldapGroupBase
from ast import literal_eval


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
        self.user_name = self.input.param("user_name","")
        self.user_id = self.input.param('user_id','')
        self.auth_type = self.input.param('auth_type','local')
        self.user_role = self.input.param('user_role','admin')


    def tearDown(self):
        super(rbacclitest, self).tearDown()

    def execute_admin_role_manage(self, options):
        cli_command = 'user-manage'
        options = options
        remote_client = RemoteMachineShellConnection(self.master)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user=self.ldapUser, password=self.ldapPass)
        return output, error

    def execute_password_policy(self, options):
        cli_command = 'setting-password-policy'
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
        self.assertTrue("ERROR: --auth-domain is required with the --set option" in output[0], "Issue with command without auth")

    def test_create_user_without_rbac_user(self):
        users, roles = self._get_user_role()
        options = "--set " + " --rbac-password " + users[0][1] + " --roles " + roles
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: --rbac-username is required with the --set option" in output[0], "Issue with command without rbacusername")

    def test_create_user_without_rbac_pass(self):
        users, roles = self._get_user_role()
        options = "--set " + "--rbac-username " + users[0][0] + " --roles " + roles + " --auth-domain local"
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: --rbac-password is required with the --set option" in output[0], "Issue with command without rbac_pass")

    def test_create_user_without_role(self):
        users, roles = self._get_user_role()
        options = "--set " + "--rbac-username " + users[0][0] + " --rbac-password " + users[0][1] +  \
                  " --auth-domain " + self.auth_type
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: --roles is required with the --set option" in output[0],"Issue with command without role")

    def test_create_user(self):
        users, roles = self._get_user_role()
        options = "--set " + "--rbac-username " + users[0][0] + " --roles " + roles \
                  + " --auth-domain " + self.auth_type
        if self.auth_type == "local":
            options += " --rbac-password " + users[0][1]
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("SUCCESS: User " in output[0],"Issue with command create_user")

    def test_create_user_name(self):
        users, roles = self._get_user_role()
        options = "--set " + "--rbac-username " + users[0][0] + " --rbac-password " + users[0][1] + " --roles " + roles \
                  + " --auth-domain " + self.auth_type + " --rbac-name " + self.user_name
        if self.auth_type == "local":
            options += " --rbac-password " + users[0][1]
        output, error = self.execute_admin_role_manage(options)
        if 'WARNING' in output[0]:
            output= output[1:]
        self.assertTrue("SUCCESS: User " in output[0],"Issue with command create user name")

    def test_delete_user(self):
        self.test_create_user()
        users, roles = self._get_user_role()
        options = "--delete " + "--rbac-username " + users[0][0] + " --auth-domain " + self.auth_type
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("SUCCESS: User " in output[0], "Issue with command of delete user")

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



    def test_create_user_without_rbac_pass_value(self):
        users, roles = self._get_user_role()
        options = "--set " + " --rbac-username " + users[0][0] + " --rbac-password --roles " + roles
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("ERROR: argument --rbac-password: expected one argument" in output[0], "Issue with command without" + \
                        " rbac-password value")


    def test_create_user_invalid_character(self):
        self.auth_type='local'
        final_users = [["\"r;itam\"",'password'],["\"r(itam\"",'password'],["\"r)itam\"",'password'], ["\"r<itam\"",'password'], \
            ["\"r>itam\"", 'password'], ["\"r@itam\"",'password'], ["\"r,itam\"",'password'], ["\"r;itam\"",'password'], \
                       ["\"r:itam\"", 'password'], ["\"r\itam\"",'password'], ["\"r[itam\"",'password'], \
                       ["\"r]itam\"", 'password'],  ["\"r[itam\"", 'password'], ["\"r]itam\"", 'password'], \
                       ["\"r=itam\"", 'password'], ["\"r{itam\"", 'password'], ["\"r}itam\"", 'password']]
        #["\"r/itam\"",'password'], ["\"r?itam\"", 'password'],
        roles = 'admin'
        for users in final_users:
            self.log.info ("------Username tested is ------------{0}".format(users[0]))
            options = "--set " + "--rbac-username " + users[0] + " --roles " + roles \
                      + " --auth-domain " + self.auth_type + " --rbac-name " + self.user_name
            if self.auth_type == "local":
                options += " --rbac-password " + users[1]
            output, error = self.execute_admin_role_manage(options)
            self.assertTrue("ERROR: _ - Username must not contain spaces, control or any" in output[0],
                            "Issue with command without" + \
                            " rbac-password value")


    def test_invalid_password_less_6_char(self):
        if self.auth_type=='local':
            users, roles = self._get_user_role()
            options = "--set " + " --rbac-username " + users[0][0] + " --rbac-password " + users[0][1] + " --roles " + \
                    roles + " --auth-domain " + self.auth_type
            output, error = self.execute_admin_role_manage(options)
            self.assertTrue("ERROR: password - The password must be at least 6 characters long." in output[0],
                            "Issue with password < 6 characters")

    def test_change_password(self):
        self.new_password = self.input.param("new_password")
        users, roles = self._get_user_role()
        options = "--set " + "--rbac-username " + users[0][0] + " --roles " + roles \
                  + " --auth-domain " + self.auth_type
        if self.auth_type == "local":
            options += " --rbac-password " + users[0][1]
        output, error = self.execute_admin_role_manage(options)
        self.assertTrue("SUCCESS: User {0} set".format(users[0][0]) in output[0],
                        "Issue with command create_user")
        if self.new_password is not None:
            options = ""
            options = "--set " + "--rbac-username " + users[0][0] + " --roles " + roles \
                      + " --auth-domain " + self.auth_type
            if self.auth_type == "local":
                options += " --rbac-password " + self.new_password
            output, error = self.execute_admin_role_manage(options)
            self.assertTrue("SUCCESS: User {0} set".format(users[0][0]) in output[0],
                            "Issue with command create_user")

    def test_invalid_passwords(self):
        final_policy = ""
        users, roles = self._get_user_role()
        correct_pass = self.input.param("correctpass",False)
        policy_type = self.input.param("policy_type")
        if ":" in policy_type:
            policy_type = policy_type.split(":")
            for policy in policy_type:
                final_policy = final_policy + " --" + policy
            policy_type = final_policy
        if policy_type == "uppercase":
            error_msg = "ERROR: argument --uppercase: expected one argument"
        elif policy_type == "lowercase":
            error_msg = "ERROR: password - The password must contain at least one lowercase letter"
        elif policy_type == "digit":
            error_msg = "ERROR: password - The password must contain at least one digit"
        elif policy_type == "special-char":
            error_msg = "ERROR: password - The password must contain at least one of the following characters: @%+\/'\"!#$^?:,(){}[]~`-_"
        elif policy_type == " --special-char --digit":
            error_msg = "ERROR: password - The password must contain at least one digit"
        elif policy_type == " --uppercase --lowercase":
            error_msg = "ERROR: password - The password must contain at least one lowercase letter"
        elif policy_type == " --uppercase --lowercase --digit --special-char":
            error_msg = "ERROR: password - The password must contain at least one lowercase letter"
        try:
            if "--" in policy_type:
                options = "--set --min-length 6 " + policy_type
            else:
                options = "--set --min-length 6 --" + policy_type
            output, error = self.execute_password_policy(options)
            options = "--set " + " --rbac-username " + users[0][0] + " --rbac-password " + users[0][1] + " --roles " + \
                    roles + " --auth-domain " + self.auth_type
            output, error = self.execute_admin_role_manage(options)
            if 'WARNING' in output[0]:
                output = output[1:]
            if correct_pass:
                self.assertTrue("SUCCESS: User {0} set".format(users[0][0]) in output[0],
                                 "Issue with command create_user")
            else:
                if 'ERROR' in output[0]:
                    self.assertTrue(error_msg in output[0],
                            "Issue with password < 6 characters")
                else:
                    self.assertTrue("SUCCESS: User " in output[0], "Issue with command create_user")
        finally:
            options = "--set --min-length 6"
            output, error = self.execute_password_policy(options)
            self.assertTrue("SUCCESS: Password policy updated" in output[0], "Issue with setting the policy ")

    def test_delete_user_noexist(self):
        users, roles = self._get_user_role()
        options = "--delete " + "--rbac-username userdoesexist --auth-type " + self.auth_type
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
        final_out =""
        for out in output:
            final_out = final_out + out
        if roles not in ['admin','ro_admin','security_admin']:
            if type(final_out) == str :
                self.log.info("The test failed as expected")
        else:

            test = json.loads(final_out)
            usrTest =[]
            for usr in test:
                if usr['id'] ==users[0][0]:
                    usrTest=usr
                    break
            for role in usrTest['roles']:
                self.assertTrue(role['role'] in roles,"Issue with --my-roles")

    def get_group_roles(self, group_name):
        # ./ couchbase-cli user-manage --get-group --group-name trial --username Administrator --password password --cluster localhost
        final_out = ''
        options = '--get-group --group-name '+group_name+" --username Administrator --password password --cluster localhost --auth-domain "+self.auth_type
        output, error = self.execute_admin_role_manage(options)
        o=''
        for i in output:
           o=o+i
        output = literal_eval(o)
        roles = output['roles']
        for role in roles:
            final_out = final_out+","+role['role']
        if(final_out[0]==","):
            final_out= final_out[1:]
        return  final_out

    def test_create_group(self):
        options = ''
        group_name = self.input.param('group_name','testGrp')
        group_roles = self.input.param('group_roles','admin')
        group_roles = group_roles.replace('-',',')
        options = '--set-group --group-name '+group_name+' --roles '+group_roles +' --auth-domain '+self.auth_type
        output, error = self.execute_admin_role_manage(options)
        content = ldapGroupBase().check_grp_created(group_name,self.master)
        self.assertTrue(content)
        roles = self.get_group_roles(group_name)
        group_roles = group_roles.split(',')
        roles = roles.split(',')
        for role in roles:
            self.assertTrue(role in group_roles, "Issue with --my-roles")

    def test_update_group(self):
        group_name = self.input.param('group_name', 'testGrp')
        group_roles = self.input.param('group_roles', 'admin')
        group_roles = group_roles.replace('-',',')
        options = '--set-group --group-name ' + group_name + ' --roles ' + group_roles+' --auth-domain '+self.auth_type
        output, error = self.execute_admin_role_manage(options)
        roles = self.get_group_roles(group_name)
        group_roles = group_roles.split(',')
        roles = roles.split(',')
        for role in roles:
            self.assertTrue(role in group_roles, "Issue with --my-roles")

    def test_delete_group(self):
        group_name = self.input.param('group_name', 'testGrp')
        # ./couchbase-cli user-manage --delete-group --group-name trial
        options = '--delete-group --group-name '+group_name
        output, error = self.execute_admin_role_manage(options)
        content = ldapGroupBase().check_grp_created(group_name, self.master)
        self.assertFalse(content)

    def test_add_user_to_group(self):
        group_name = self.input.param('group_name', 'testGrp')
        content = ldapGroupBase().check_grp_created(group_name, self.master)
        if content == False:
            #default group_roles = admin
            self.test_create_group()
        self.test_create_user()
        users, roles = self._get_user_role()
        options = '--set --user-groups '+group_name+' --rbac-username '+ users[0][0]+ ' --auth-domain '+self.auth_type
        output, error = self.execute_admin_role_manage(options)
        options='--get --rbac-username '+users[0][0]
        output, error = self.execute_admin_role_manage(options)

        group_roles = self.get_group_roles( group_name)
        o = ''
        for i in output:
            o = o + i
        output = literal_eval(o)
        # if self.auth_type=='InternalGrp'or self.auth_type=='builtin':
        #     self.auth_type='local'
        # if self.auth_type=='LDAPGrp'or self.auth_type=='ldap':
        #     self.auth_type='external'
        for usr in output:
            if usr['id']== users[0][0]:
                if usr['domain']==self.auth_type:
                    role = usr['roles']
                    for ro in role:
                        origin = ro['origins']
                        for orig in origin:
                            if 'name' in orig.keys():
                                if orig['name']==group_name:
                                    self.assertTrue(ro['role']==group_roles)
    #
    # def test_del_user_from_group(self):
    #     group_name = self.input.param('group_name', 'testGrp')
    #     content = self.get_group_roles( group_name)
    #     print"\n\n\n", content ,"\n\n\n"
    #     users, roles = self._get_user_role()
    #     options = '--get --rbac-username ' + users[0][0]
    #
    #     output, error = self.execute_admin_role_manage(options)
    #
    #     o=''
    #     for i in output:
    #         o = o+i
    #     output= literal_eval(o)
    #     if self.auth_type == 'InternalGrp':
    #         self.auth_type = 'local'
    #     if self.auth_type == 'LDAPGrp':
    #         self.auth_type = 'external'
    #     final_roles =''
    #     for usr in output:
    #         if usr['id'] == users[0][0]:
    #             if usr['domain'] == self.auth_type:
    #                 role = usr['roles']
    #                 for ro in role:
    #                     final_roles=final_roles+","+ro['role']
    #     print"finalroles: ",final_roles,"\n\n\n"




