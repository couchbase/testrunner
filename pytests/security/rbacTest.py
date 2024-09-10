from pytests.security.ldapGroupBase import ldapGroupBase
from security.ldaptest import ldaptest
from lib.membase.api.rest_client import RestConnection
import urllib.request, urllib.parse, urllib.error
from security.rbacmain import rbacmain
import json
from remote.remote_util import RemoteMachineShellConnection
from newupgradebasetest import NewUpgradeBaseTest
from security.auditmain import audit
import subprocess
import socket
import fileinput
import sys
from subprocess import Popen, PIPE
from security.rbac_base import RbacBase
from pytests.failover.AutoFailoverBaseTest import AutoFailoverBaseTest


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


class rbacTest(ldaptest, AutoFailoverBaseTest):

    def setUp(self):
        super(rbacTest, self).setUp()
        rest = RestConnection(self.master)
        self.auth_type = self.input.param('auth_type', 'ldap')
        self.user_id = self.input.param("user_id", None)
        self.user_role = self.input.param("user_role", None)
        self.bucket_name = self.input.param("bucket_name", 'default')
        self.role_map = self.input.param("role_map", None)
        self.incorrect_bucket = self.input.param("incorrect_bucket", False)
        self.new_role = self.input.param("new_role", None)
        self.new_role_map = self.input.param("new_role_map", None)
        self.no_bucket_access = self.input.param("no_bucket_access", False)
        self.no_access_bucket_name = self.input.param("no_access_bucket_name", None)
        self.ldap_users = rbacmain().returnUserList(self.user_id)
        if self.auth_type == 'ldap' or self.auth_type == 'pam':
            rbacmain(self.master, 'builtin')._delete_user('cbadminbucket')
            RbacBase().enable_ldap(rest)
        # rbacmain(self.master, self.auth_type)._delete_user_from_roles(self.master)
        if self.auth_type == 'ldap' or self.auth_type == 'LDAPGroup':
            rest = RestConnection(self.master)
            param = {
                'hosts': '{0}'.format("172.23.120.175"),
                'port': '{0}'.format("389"),
                'encryption': '{0}'.format("None"),
                'bindDN': '{0}'.format("cn=admin,dc=couchbase,dc=com"),
                'bindPass': '{0}'.format("p@ssw0rd"),
                'authenticationEnabled': '{0}'.format("true"),
                'userDNMapping': '{0}'.format('{"template":"cn=%u,ou=Users,dc=couchbase,dc=com"}')
            }
            rest.setup_ldap(param, '')
            # rbacmain().setup_auth_mechanism(self.servers,'ldap',rest)
            RbacBase().enable_ldap(rest)
            self._removeLdapUserRemote(self.ldap_users)
            self._createLDAPUser(self.ldap_users)
        elif self.auth_type == "pam":
            RbacBase().enable_ldap(rest)
            rbacmain().setup_auth_mechanism(self.servers, 'pam', rest)
            rbacmain().add_remove_local_user(self.servers, self.ldap_users, 'deluser')
            rbacmain().add_remove_local_user(self.servers, self.ldap_users, 'adduser')
        elif self.auth_type == "builtin" or self.auth_type == 'InternalGroup':
            for user in self.ldap_users:
                testuser = [{'id': user[0], 'name': user[0], 'password': user[1]}]
                RbacBase().create_user_source(testuser, 'builtin', self.master)
                self.sleep(10)
        elif self.auth_type == 'LDAPGroup':
            self.group_name = self.input.param('group_name', 'testgrp')
            LDAP_GROUP_DN = "ou=Groups,dc=couchbase,dc=com"
            ldapGroupBase().create_group_ldap(self.group_name, self.ldap_users[0], self.master)
            group_dn = 'cn=' + self.group_name + ',' + LDAP_GROUP_DN
            ldapGroupBase().add_role_group(self.group_name, [self.user_role], group_dn, self.master)
        elif self.auth_type == "ExternalGroup":
            self.group_name = self.input.param("group_name", "testgrp")
            ldapGroupBase().create_group_ldap(self.group_name, self.ldap_users[0], self.master)
            LDAP_GROUP_DN = "ou=Groups,dc=couchbase,dc=com"
            group_dn = 'cn=' + self.group_name + ',' + LDAP_GROUP_DN
            print(self.user_role)
            ldapGroupBase().add_role_group(self.group_name, self.user_role, group_dn, self.master)
            ldapGroupBase().create_grp_usr_external([self.ldap_users[0][0]], self.master, [''],
                                                    self.group_name)
        self.ldap_server = ServerInfo(self.ldapHost, self.ldapPort, 'root', 'couchbase')
        self.ipAddress = self.getLocalIPAddress()

    def tearDown(self):
        super(rbacTest, self).tearDown()

    def getLocalIPAddress(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('couchbase.com', 0))
        return s.getsockname()[0]
        '''
        status, ipAddress = subprocess.getstatusoutput("ifconfig en0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        if '1' not in ipAddress:
            status, ipAddress = subprocess.getstatusoutput("ifconfig eth0 | grep  -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | awk '{print $2}'")
        return ipAddress
        '''

    def test_compare_orig_roles(self):
        status, content, header = rbacmain(self.master)._retrive_all_user_role(self.user_id)
        orig_role_list = [{"role": "admin", "name": "Admin",
                           "desc": "Can manage ALL cluster features including security."},
                          {"role": "ro_admin", "name": "Read Only Admin",
                           "desc": "Can view ALL cluster features."},
                          {"role": "cluster_admin", "name": "Cluster Admin",
                           "desc": "Can manage all cluster features EXCEPT security."},
                          {"role": "bucket_admin", "bucket_name": "*", "name": "Bucket Admin",
                           "desc": "Can manage ALL bucket features for specified buckets (incl. start/stop XDCR)"},
                          {"role": "views_admin", "bucket_name": "*", "name": "Views Admin",
                           "desc": "Can manage views for specified buckets"},
                          {"role": "replication_admin", "name": "Replication Admin",
                           "desc": "Can manage ONLY XDCR features (cluster AND bucket level)"}]
        content = json.loads(content)
        if orig_role_list == content:
            self.assertTrue(True, "Issue in comparison of original roles with expected")

    def test_role_assign_check_rest_api(self):
        user_name = self.input.param("user_name")
        final_test_role_assign_check_end_to_end = self.user_id.split("?")
        final_roles = rbacmain()._return_roles(self.user_role)
        payload = "name=" + user_name + "&roles=" + final_roles
        if len(final_user_id) == 1:
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=self.user_id, payload=payload)
            self.assertTrue(status, "Issue with setting role")
        else:
            for final_user in final_user_id:
                status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                    user_name=final_user[0], payload=payload)
                self.assertTrue(status, "Issue with setting role")

    def test_role_assign_check_end_to_end(self):
        user_name = self.input.param("user_name")
        final_user_id = rbacmain().returnUserList(self.user_id)
        final_roles = rbacmain()._return_roles(self.user_role)
        payload = "name=" + user_name + "&roles=" + final_roles
        if len(final_user_id) == 1:
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=(self.user_id.split(":"))[0], payload=payload)
            self.assertTrue(status, "Issue with setting role")
            status = rbacmain()._parse_get_user_response(json.loads(content),
                                                         (self.user_id.split(":"))[0], user_name,
                                                         final_roles)
            self.assertTrue(status, "Role assignment not matching")
        else:
            for final_user in final_user_id:
                status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                    user_name=final_user[0], payload=payload)
                self.assertTrue(status, "Issue with setting role")
                status = rbacmain()._parse_get_user_response(json.loads(content), final_user[0],
                                                             user_name, final_roles)
                self.assertTrue(status, "Role assignment not matching")

    def test_role_assign_incorrect_role_name(self):
        msg = self.input.param("msg", None)
        payload = "name=" + self.user_id + "&roles=" + self.user_role
        user_list = self.returnUserList(self.user_id)
        for user in user_list:
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=user[0], payload=payload)
            self.assertFalse(status, "Incorrect status for incorrect role name")
            content = json.loads(content)
            if msg not in content:
                self.assertFalse(True, "Message shown is incorrect")

    def test_role_assign_incorrect_bucket_name(self):
        msg = self.input.param("msg", None)
        payload = "name=" + self.user_id + "&roles=" + self.user_role
        user_list = self.returnUserList(self.user_id)
        for user in user_list:
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=user[0], payload=payload)
            self.assertFalse(status, "Incorrect status for incorrect role name")
            content = json.loads(content)
            if msg not in content:
                self.assertFalse(True, "Message shown is incorrect")

    '''
    def test_role_assign_retrieve(self):
        status, content, header = rbacmain(self.master)._retrieve_user_roles()
        content = json.loads(content)
        id, name, roles = self._parse_get_user_response(content,'ritam')
    '''

    def test_role_permission_validate_multiple(self):
        result = rbacmain(master_ip=self.master,
                          auth_type=self.auth_type)._check_role_permission_validate_multiple(
            self.user_id, self.user_role, self.bucket_name, self.role_map)
        self.assertTrue(result, "Issue with role assignment and comparision with permission set")

    def test_change_role(self):
        rbacmain(self.master, self.auth_type)._check_role_permission_validate_multiple(self.user_id,
                                                                                       self.user_role,
                                                                                       self.bucket_name,
                                                                                       self.role_map)
        result = rbacmain(self.master, self.auth_type)._check_role_permission_validate_multiple(
            self.user_id, self.new_role, self.bucket_name, self.new_role_map)
        self.assertTrue(result, "Issue with role assignment and comparision with permission set")

    def test_user_role_cluster(self):
        servers_count = self.servers[:self.nodes_init]
        user_list = self.returnUserList(self.user_id)
        final_roles = rbacmain()._return_roles(self.user_role)
        for user_id in user_list:
            payload = "name=" + user_id[0] + "&roles=" + final_roles
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=user_id[0], payload=payload)
            for server in servers_count:
                status, content, header = rbacmain(server)._retrieve_user_roles()
                content = json.loads(content)
                temp = rbacmain()._parse_get_user_response(content, user_id[0], user_id[0],
                                                           self.user_role)
                self.assertTrue(temp, "Roles are not matching for user")

    def test_user_role_cluster_rebalance_in(self):
        user_list = self.returnUserList(self.user_id)
        final_roles = rbacmain()._return_roles(self.user_role)
        for user_id in user_list:
            payload = "name=" + user_id[0] + "&roles=" + final_roles
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=user_id[0], payload=payload)
        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])
        for server in self.servers:
            status, content, header = rbacmain(server)._retrieve_user_roles()
            content = json.loads(content)
            for user_id in user_list:
                temp = rbacmain()._parse_get_user_response(content, user_id[0], user_id[0],
                                                           self.user_role)
                self.assertTrue(temp, "Roles are not matching for user")

    def test_user_role_cluster_rebalance_out(self):
        user_list = self.returnUserList(self.user_id)
        final_roles = rbacmain()._return_roles(self.user_role)
        for user_id in user_list:
            payload = "name=" + user_id[0] + "&roles=" + final_roles
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=user_id[0], payload=payload)
        servers_out = self.servers[2:]
        self.cluster.rebalance(self.servers, [], servers_out)
        for server in self.servers[:2]:
            status, content, header = rbacmain(server)._retrieve_user_roles()
            content = json.loads(content)
            for user_id in user_list:
                temp = rbacmain()._parse_get_user_response(content, user_id[0], user_id[0],
                                                           self.user_role)
                self.assertTrue(temp, "Roles are not matching for user")

    def test_role_permission_validate_multiple_cluster(self):
        for server in self.servers[:self.nodes_init]:
            rbacmain(server, self.auth_type)._check_role_permission_validate_multiple(self.user_id,
                                                                                      self.user_role,
                                                                                      self.bucket_name,
                                                                                      self.role_map,
                                                                                      self.incorrect_bucket)

    def test_role_permission_multiple_buckets(self):
        rest = RestConnection(self.master)
        rest.create_bucket(bucket='default', ramQuotaMB=256)
        rest1 = RestConnection(self.master)
        rest1.create_bucket(bucket='default1', ramQuotaMB=256, proxyPort=11212)
        bucket_name = self.bucket_name.split(":")
        for server in self.servers[:self.nodes_init]:
            if (len(bucket_name) > 1):
                for bucket in bucket_name:
                    rbacmain(server, self.auth_type)._check_role_permission_validate_multiple(
                        self.user_id, self.user_role, bucket, self.role_map, self.incorrect_bucket)
            else:
                rbacmain(server, self.auth_type)._check_role_permission_validate_multiple(
                    self.user_id, self.user_role, self.bucket_name, self.role_map,
                    self.incorrect_bucket)

    def test_role_permission_noaccess_bucket(self):
        rest = RestConnection(self.master)
        rest.create_bucket(bucket='default', ramQuotaMB=256)
        # rest1=RestConnection(self.master)
        # rest1.create_bucket(bucket='default1', ramQuotaMB=256,proxyPort=11212)
        bucket_name = self.bucket_name.split(":")
        for server in self.servers[:self.nodes_init]:
            if (len(bucket_name) > 1):
                for bucket in bucket_name:
                    rbacmain(server, self.auth_type)._check_role_permission_validate_multiple(
                        self.user_id, self.user_role, bucket, self.role_map, self.incorrect_bucket)
            else:
                rbacmain(server, self.auth_type)._check_role_permission_validate_multiple(
                    self.user_id, self.user_role, self.bucket_name, self.role_map,
                    no_bucket_access=self.no_bucket_access,
                    no_access_bucket_name=self.no_access_bucket_name)

    def test_add_remove_users(self):
        final_roles = ""
        user_list = self.returnUserList(self.user_id)
        user_role_param = self.user_role.split(":")
        if len(user_role_param) == 1:
            final_roles = user_role_param[0]
        else:
            for role in user_role_param:
                final_roles = role + "," + final_roles
        for user_id in user_list:
            payload = "name=" + user_id[0] + "&roles=" + final_roles
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=user_id[0], payload=payload)
        delete_user = user_list[1:]
        for user in delete_user:
            status, content, header = rbacmain(self.master, self.auth_type)._delete_user(user[0])
            self.assertTrue(status, "Issue with deleting users")

    def test_add_remove_user_check_permission(self):
        final_roles = ""
        user_list = self.returnUserList(self.user_id)
        user_role_param = self.user_role.split(":")
        if len(user_role_param) == 1:
            final_roles = user_role_param[0]
        else:
            for role in user_role_param:
                final_roles = role + "," + final_roles
        for user_id in user_list:
            payload = "name=" + user_id[0] + "&roles=" + final_roles
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=user_id[0], payload=payload)
        delete_user = user_list[1:]
        for user in delete_user:
            rbacmain(self.master, self.auth_type)._delete_user(user[0])
        permission_str = "cluster.pools!read,cluster.nodes!read"
        for user in delete_user:
            status, content, header = rbacmain(self.master, self.auth_type)._check_user_permission(
                user_id[0], user_id[1], permission_str)
            self.assertFalse(status, "Deleted user can access couchase server")

    def test_add_remove_some_user_check_permission(self):
        final_roles = ""
        user_list = self.returnUserList(self.user_id)
        user_role_param = self.user_role.split(":")
        if len(user_role_param) == 1:
            final_roles = user_role_param[0]
        else:
            for role in user_role_param:
                final_roles = role + "," + final_roles
        for user_id in user_list:
            payload = "name=" + user_id[0] + "&roles=" + final_roles
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=user_id[0], payload=payload)
        rbacmain(self.master)._delete_user(user_list[0][0])
        permission_str = "cluster.pools!read,cluster.nodes!read"
        for user in user_list[1:]:
            status, content, header = rbacmain(self.master, self.auth_type)._check_user_permission(
                user[0], user[1], permission_str)
            self.assertTrue(status,
                            "Users cannot login if one of the user is deleted from couchbase")

    def test_ldapDeleteUser(self):
        rbacmain(self.master, self.auth_type)._check_role_permission_validate_multiple(self.user_id,
                                                                                       self.user_role,
                                                                                       self.bucket_name,
                                                                                       self.role_map)
        user_name = rbacmain().returnUserList(self.user_id)
        self._removeLdapUserRemote(user_name)
        status, content, header = rbacmain(self.master, self.auth_type)._check_user_permission(
            user_name[0][0], user_name[0][1], self.user_role)
        self.assertFalse(status, "Not getting 401 for users that are deleted in LDAP")

    def test_checkInvalidISASLPW(self):
        shell = RemoteMachineShellConnection(self.master)
        try:
            result = rbacmain(self.master, self.auth_type)._check_role_permission_validate_multiple(
                self.user_id, self.user_role, self.bucket_name, self.role_map)
            self.assertTrue(result,
                            "Issue with role assignment and comparision with permission set")
            command = "mv /opt/couchbase/var/lib/couchbase/isasl.pw /tmp"
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
            result = rbacmain(self.master, self.auth_type)._check_role_permission_validate_multiple(
                self.user_id, self.user_role, self.bucket_name, self.role_map)
            self.assertTrue(result,
                            "Issue with role assignment and comparision with permission set")
        finally:
            command = "mv /tmp/isasl.pw /opt/couchbase/var/lib/couchbase"
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)
            shell.disconnect()

    def test_checkPasswordChange(self):
        result = rbacmain(self.master, self.auth_type)._check_role_permission_validate_multiple(
            self.user_id, self.user_role, self.bucket_name, self.role_map)
        self.assertTrue(result, "Issue with role assignment and comparision with permission set")
        user_list = self.returnUserList(self.user_id)
        temp_id = ""
        for i in range(len(user_list)):
            self._changeLdapPassRemote(user_list[i][0], 'password1')
            temp_id = str(user_list[i][0]) + ":" + str('password1?')
        result = rbacmain(self.master, self.auth_type)._check_role_permission_validate_multiple(
            temp_id[:-1], self.user_role, self.bucket_name, self.role_map)
        self.assertTrue(result, "Issue with role assignment and comparision with permission set")

    def test_role_permission_validate_multiple_rest_api(self):
        result = rbacmain(self.master, self.auth_type, servers=self.servers,
                          cluster=self.cluster)._check_role_permission_validate_multiple_rest_api(
            self.user_id, self.user_role, self.bucket_name, self.role_map)
        self.assertTrue(result, "Issue with role assignment and comparision with permission set")

    def test_role_assignment_audit(self):
        ops = self.input.param("ops", 'assign')
        if ops in ['assign', 'edit']:
            eventID = rbacmain.AUDIT_ROLE_ASSIGN
        elif ops == 'remove':
            eventID = rbacmain.AUDIT_REMOVE_ROLE
        Audit = audit(eventID=eventID, host=self.master)
        currentState = Audit.getAuditStatus()
        self.log.info(
            "Current status of audit on ip - {0} is {1}".format(self.master.ip, currentState))
        if currentState:
            Audit.setAuditEnable('false')
        self.log.info("Enabling Audit ")
        Audit.setAuditEnable('true')
        self.sleep(30)
        user_name = self.input.param("user_name")
        final_roles = rbacmain()._return_roles(self.user_role)
        payload = "name=" + user_name + "&roles=" + final_roles
        userid = self.user_id.split(":")
        status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
            user_name=userid[0], payload=payload)
        if self.auth_type == 'builtin':
            source = 'local'
        else:
            source = 'external'
        expectedResults = {"roles": ["admin"], "identity:source": source,
                           "identity:user": userid[0],
                           "real_userid:source": "ns_server", "real_userid:user": "Administrator",
                           "groups": [], "reason": "updated",
                           "ip": self.ipAddress, "port": 123456, "full_name": "'RitamSharma'"}
        if ops == 'edit':
            payload = "name=" + user_name + "&roles=" + 'admin,cluster_admin'
            status, content, header = rbacmain(self.master, self.auth_type)._set_user_roles(
                user_name=userid[0], payload=payload)
            expectedResults = {"roles": ["admin", "cluster_admin"], "identity:source": source,
                               "identity:user": userid[0],
                               "real_userid:source": "ns_server",
                               "real_userid:user": "Administrator", "groups": [],
                               "reason": "updated",
                               "ip": self.ipAddress, "port": 123456, "full_name": "'RitamSharma'"}
        elif ops == 'remove':
            status, content, header = rbacmain(self.master, self.auth_type)._delete_user(userid[0])
            expectedResults = {"identity:source": source, "identity:user": userid[0],
                               "real_userid:source": "ns_server",
                               "real_userid:user": "Administrator",
                               "ip": self.ipAddress, "port": 123456}
        fieldVerification, valueVerification = Audit.validateEvents(expectedResults)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")

    def test_update_password_http_patch(self):
        """
        Verify update of local user password using PATCH endpoint:
        /settings/rbac/users/local/<userid>
        """
        rest = RestConnection(self.master)
        user_id = "test_user"
        role = "admin"
        password = "password"
        payload = "name={0}&roles={1}&password={2}".format(user_id, role, password)
        rest.add_set_builtin_user(user_id, payload)
        newpassword = "newpassword"
        rest.update_password(user_id, newpassword)

        # Verify new password works
        try:
            rest.check_user_permission(user_id, newpassword, "cluster!admin")
        except Exception as ex:
            self.fail("Access not granted with the updated password. Failed with exception: {0}"
                      .format(ex))
        else:
            self.log.info("Access granted as expected with the updated password")

        # Verify old password doesn't work, api will return nothing
        try:
            rest.check_user_permission(user_id, password, "cluster!admin")
        except Exception as ex:
            self.assertTrue("b''" == str(ex), msg="Unexpected exception {0}".format(ex))
            self.log.info("Failed as excepted with the old password")
        else:
            self.fail("Unauthorized access granted with the old password")

        # Verify password update fails for non-existent user
        try:
            rest.update_password("non_existent_user", newpassword)
        except Exception as ex:
            self.assertTrue("User was not found" in str(ex),
                            msg="Unexpected exception {0}".format(ex))
            self.log.info("Update password failed as expected with invalid user name")
        else:
            self.fail("Password updated for a non-existent user")

        # Verify users with only certain roles authorized to update password
        roles = ['admin', 'cluster_admin', 'ro_admin', 'bucket_admin[*]', 'bucket_full_access[*]',
                 'data_backup[*]', 'security_admin_local', 'security_admin_external',
                 'data_reader[*]', 'data_writer[*]', 'data_dcp_reader[*]', 'data_monitoring[*]']
        allowed_roles = ['admin', 'security_admin_local', 'security_admin_external']
        for role in roles:
            rest.update_password(user_id, password)  # Reset password
            user_name = "user_" + role.split("[", 1)[0]
            user_role = role
            password = "password"
            payload = "name={0}&roles={1}&password={2}".format(user_name, user_role, password)
            rest.add_set_builtin_user(user_name, payload)
            try:
                rest.update_password(user_id, newpassword, user_name, password)
            except Exception as ex:
                if role in allowed_roles:
                    self.fail("User: {0} should have permission to update the password"
                              .format(user_name))
                else:
                    self.assertTrue("Forbidden. User needs the following permissions" in str(ex),
                                    msg="Unexpected exception {0}".format(ex))
                    self.log.info("User: {0} unauthorized to update password as expected"
                                  .format(user_name))
            else:
                if role in allowed_roles:
                    self.log.info("User: {0} authorized to update password as expected"
                                  .format(user_name))
                else:
                    self.fail("User: {0} should not have permission to update the password"
                              .format(user_name))

        #  Verify previous roles are intact after password update
        expected_user_roles = {'user_data_backup': ['data_backup[*]'],
                               'user_ro_admin': ['ro_admin'], 'user_admin': ['admin'],
                               'user_bucket_full_access': ['bucket_full_access[*]'],
                               'user_cluster_admin': ['cluster_admin'],
                               'user_bucket_admin': ['bucket_admin[*]'],
                               'user_security_admin_external': ['security_admin_external'],
                               'user_security_admin_local': ['security_admin_local'],
                               'user_data_reader': ['data_reader[*]'],
                               'user_data_writer': ['data_writer[*]'],
                               'user_data_monitoring': ['data_monitoring[*]'],
                               'user_data_dcp_reader': ['data_dcp_reader[*]']}
        for role in roles:
            user_id = "user_" + role.split("[", 1)[0]
            rest.update_password(user_id, newpassword)
        content = rest.retrieve_user_roles()
        observed_user_roles = {}
        for items in content:
            roles_list = []
            for role in items['roles']:
                roles_str = role['role']
                if 'bucket_name' in role:
                    roles_str = roles_str + "[" + role['bucket_name'] + "]"
                    if role['bucket_name'] == "*":
                        roles_list.append(roles_str)
                        observed_user_roles[items['id']] = roles_list
                        continue
                    if 'scope_name' in role:
                        roles_str = roles_str + "[" + role['scope_name'] + "]"
                        if 'collection_name' in role:
                            roles_str = roles_str + "[" + role['collection_name'] + "]"
                roles_list.append(roles_str)
                observed_user_roles[items['id']] = roles_list

        for user in expected_user_roles:
            if set(expected_user_roles[user]) != set(observed_user_roles[user]):
                self.fail(
                    "Validation of RBAC user roles post password update failed. "
                    "Mismatch in the roles of {0}.".format(user))
        self.log.info("All user roles after password update is intact")

        user_id = "test_user"
        rest.update_password(user_id, password)  # Reset password
        
        # no password provided
        try:
            rest.update_password(user_id)
        except Exception as ex:
            self.assertTrue("missing 1 required positional argument" in str(ex),
                            msg="Unexpected exception {0}".format(ex))
            self.log.info("Failed as expected as no password provided")
        else:
            self.fail("Should fail as no password provided")

        # empty password
        try:
            rest.update_password(user_id, "")
        except Exception as ex:
            self.assertTrue("The password must be at least 6 characters long" in str(ex),
                            msg="Unexpected exception {0}".format(ex))
            self.log.info("Failed as expected as empty password provided")
        else:
            self.fail("Should fail as empty password provided")

        # invalid password -> less than 6 chars
        try:
            rest.update_password(user_id, "aa")
        except Exception as ex:
            self.assertTrue("The password must be at least 6 characters long" in str(ex),
                            msg="Unexpected exception {0}".format(ex))
            self.log.info("Failed as expected as weak password(< 6 chars) provided")
        else:
            self.fail("Should fail as weak password(< 6 chars) provided")

        # new passwd same as old password
        try:
            rest.update_password(user_id, password)
        except Exception as ex:
            self.fail("Fails when new password provided same as old password with exception: {0}"
                      .format(ex))
        else:
            self.log.info("Works when new password provided same as old password")

        self.cluster.async_rebalance(self.servers,
                                     self.servers_to_add,
                                     self.servers_to_remove)
        self.sleep(5)
        #  update password during rebalance
        try:
            rest.update_password(user_id, newpassword)
        except Exception as ex:
            self.fail("Update password fails during rebalance with exception: {0}".format(ex))
        else:
            self.log.info("Update password works during rebalance")

        self.enable_autofailover_and_validate()
        self.sleep(5)
        self.failover_actions[self.failover_action](self)
        #  update password when a node is down
        try:
            rest.update_password(user_id, newpassword)
        except Exception as ex:
            self.fail("Update password fails when a node is down with exception: {0}".format(ex))
        else:
            self.log.info("Update password works when a node is down")

        self.sleep(300)
        # update password after autofailover
        try:
            rest.update_password(user_id, newpassword)
        except Exception as ex:
            self.fail("Update password fails after autofailover with exception: {0}".format(ex))
        else:
            self.log.info("Update password works after autofailover")

        self.bring_back_failed_nodes_up()


'''
class rbac_upgrade(NewUpgradeBaseTest, ldaptest):

    def setUp(self):
        super(rbac_upgrade, self).setUp()
        self.initial_version = self.input.param("initial_version", '4.1.0-5005')
        self.upgrade_version = self.input.param("upgrade_version", "4.5.0-2047")


    def setup_4_1_settings(self):
        rest = RestConnection(self.master)
        self._setupLDAPAuth(rest, self.authRole, self.authState, self.fullAdmin, self.ROAdmin)

    def tearDown(self):
        super(rbac_upgrade, self).tearDown()

    def upgrade_all_nodes(self):
        servers_in = self.servers[1:]
        self._install(self.servers)
        self.cluster.rebalance(self.servers, servers_in, [])
        self.user_role = self.input.param('user_role', None)
        self.setup_4_1_settings()


        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version, servers=self.servers)
        for threads in upgrade_threads:
            threads.join()


        for server in self.servers:
            status, content, header = rbacmain(server)._retrieve_user_roles()
            content = json.loads(content)
            for user_id in self.fullAdmin:
                temp = rbacmain()._parse_get_user_response(content, user_id[0], user_id[0], self.user_role)
                self.assertTrue(temp, "Roles are not matching for user")



    def upgrade_half_nodes(self):
        serv_upgrade = self.servers[2:4]
        servers_in = self.servers[1:]
        self._install(self.servers)
        self.cluster.rebalance(self.servers, servers_in, [])
        self.user_role = self.input.param('user_role', None)
        self.setup_4_1_settings()

        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version, servers=serv_upgrade)
        for threads in upgrade_threads:
            threads.join()

        status, content, header = rbacmain(self.master)._retrieve_user_roles()
        self.assertFalse(status, "Incorrect status for rbac cluster in mixed cluster {0} - {1} - {2}".format(status, content, header))

        for server in serv_upgrade:
            status, content, header = rbacmain(server)._retrieve_user_roles()
            self.assertFalse(status, "Incorrect status for rbac cluster in mixed cluster {0} - {1} - {2}".format(status, content, header))
'''
