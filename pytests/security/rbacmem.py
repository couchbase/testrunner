from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from security.testmemcached import TestMemcachedClient
from security.testmemcached import TestSDK
from security.rbacmain import rbacmain
from security.rbac_base import RbacBase
import time


class dataRoles():

    @staticmethod
    def _datareader_role_master():
        per_set = {
            "name": "Data Reader Role",
            "permissionSet": "read!True,write!False,statsRead!False,ReadMeta!True,WriteMeta!False,ReadXattr!False,WriteXattr!False"}
        return per_set

    @staticmethod
    def _datareaderwrite_role_master():
        per_set = {
            "name": "Data Reader Writer Role",
            "permissionSet": "read!False,write!True,statsRead!False,ReadMeta!False,WriteMeta!False,ReadXattr!False,WriteXattr!False"}
        return per_set

    @staticmethod
    def _view_admin_role_master():
        per_set = {
            "name": "View Admin Role",
            "permissionSet": "read!True,write!False,statsRead!True,ReadMeta!True,WriteMeta!False"}
        return per_set

    @staticmethod
    def _replication_admin_role_master():
        per_set = {
            "name": "Replication Admin Role",
            "permissionSet": "read!True,write!False,statsRead!True,ReadMeta!True,WriteMeta!False"}
        return per_set

    @staticmethod
    def _bucket_admin_role_master():
        per_set = {
            "name": "Bucket Admin Role",
            "permissionSet": "read!False,write!False,statsRead!True,ReadMeta!False,WriteMeta!False"}
        return per_set

    @staticmethod
    def _cluster_admin_role_master():
        per_set = {
            "name": "Cluster Admin Role",
            "permissionSet": "read!False,write!False,statsRead!True,ReadMeta!False,WriteMeta!False"}
        return per_set

    @staticmethod
    def _full_admin_role_master():
        per_set = {
            "name": "Full Admin Role",
            "permissionSet": "read!True,write!True,statsRead!True,ReadMeta!True,WriteMeta!True"}
        return per_set

    @staticmethod
    def _dcp_data_reader_role_master():
        per_set = {
            "name": "DCP Data Reader Role",
            "permissionSet": "read!True,write!False,statsRead!False,ReadMeta!True,WriteMeta!False"}
        return per_set

    @staticmethod
    def _read_only_role_master():
        per_set = {
            "name": "Read Only Role",
            "permissionSet": "read!False,write!False,statsRead!False,ReadMeta!False,WriteMeta!False"}
        return per_set

    @staticmethod
    def _no_bucket_access():
        per_set = {
            "name": "No Bucket Access",
            "permissionSet": "read!False,write!False,statsRead!False,ReadMeta!False,WriteMeta!False"}
        return per_set

    @staticmethod
    def _data_backup_master():
        per_set = {
            "name": "Data Backup",
            "permissionSet": "read!True,write!True,statsRead!True,ReadMeta!True,WriteMeta!True"}
        return per_set

    @staticmethod
    def _data_monitoring_master():
        per_set = {
            "name": "Data Monitoring",
            "permissionSet": "read!False,write!False,statsRead!True,ReadMeta!False,WriteMeta!False"}
        return per_set

    @staticmethod
    def _get_role_hierarchy():
        per_set = {
            "data_reader":0,
            "data_writer":1
        }

    @staticmethod
    def _return_permission_set(role=None):
        return_role_master = []
        return_role_expected = []
        return_role_expected_negative = []
        return_role_hierarchy = dataRoles._get_role_hierarchy()

        if role == "data_reader":
            return_role_master = dataRoles._datareader_role_master()

        if role == "data_writer":
            return_role_master = dataRoles._datareaderwrite_role_master()

        if role == "views_admin":
            return_role_master = dataRoles._view_admin_role_master()

        if role == "replication_admin":
            return_role_master = dataRoles._replication_admin_role_master()

        if role == "bucket_admin":
            return_role_master = dataRoles._bucket_admin_role_master()

        if role == "cluster_admin":
            return_role_master = dataRoles._cluster_admin_role_master()

        if role == "admin":
            return_role_master = dataRoles._full_admin_role_master()

        if role == "readonly":
            return_role_master = dataRoles._read_only_role_master()

        if role == "readonly":
            return_role_master = dataRoles._read_only_role_master()

        if role == "data_dcp_reader":
            return_role_master = dataRoles._dcp_data_reader_role_master()

        if role == "data_backup":
            return_role_master = dataRoles._data_backup_master()

        if role == "data_monitoring":
            return_role_master = dataRoles._data_monitoring_master()

        if role == "no_bucket_access":
            return_role_master = dataRoles._no_bucket_access()

        return return_role_master


class RbacTestMemcached(BaseTestCase):
    LDAP_GROUP_DN = "ou=Groups,dc=couchbase,dc=com"

    def setUp(self):
        super(RbacTestMemcached, self).setUp()
        rest = RestConnection(self.master)
        self.auth_type = self.input.param('auth_type', 'builtin')
        self.user_id = self.input.param("user_id", None)
        self.user_role = self.input.param("user_role", None)
        self.bucket_name = self.input.param("bucket_name", None)
        rest.create_bucket(bucket=self.bucket_name, ramQuotaMB=256, lww=True, numVBuckets=self.vbuckets)
        self.role_map = self.input.param("role_map", None)
        self.incorrect_bucket = self.input.param("incorrect_bucket", False)
        self.new_role = self.input.param("new_role", None)
        self.new_role_map = self.input.param("new_role_map", None)
        self.no_bucket_access = self.input.param("no_bucket_access", False)
        self.no_access_bucket_name = self.input.param("no_access_bucket_name", "noaccess")
        self.all_buckets = self.input.param("all_buckets", None)
        self.ldap_users = rbacmain().returnUserList(self.user_id)
        if self.no_bucket_access:
            rest.create_bucket(bucket=self.no_access_bucket_name, ramQuotaMB=256, lww=True, numVBuckets=self.vbuckets)
        if self.auth_type == 'ldap':
            rbacmain(self.master, 'builtin')._delete_user('cbadminbucket')
        if self.auth_type == 'ldap':
            rbacmain().setup_auth_mechanism(self.servers, 'ldap', rest)
            for user in self.ldap_users:
                testuser = [{'id': user[0], 'name': user[0], 'password': user[1]}]
                RbacBase().create_user_source(testuser, 'ldap', self.master)
                self.sleep(10)
        elif self.auth_type == "pam":
            rbacmain().setup_auth_mechanism(self.servers, 'pam', rest)
            rbacmain().add_remove_local_user(self.servers, self.ldap_users, 'deluser')
            rbacmain().add_remove_local_user(self.servers, self.ldap_users, 'adduser')
        elif self.auth_type == "builtin":
            for user in self.ldap_users:
                testuser = [{'id': user[0], 'name': user[0], 'password': user[1]}]
                RbacBase().create_user_source(testuser, 'builtin', self.master)
                self.sleep(10)
        elif self.auth_type == 'LDAPGrp':
            from security.ldapGroupBase import ldapGroupBase
            self.group_name = self.input.param('group_name','testgrp')
            ldapGroupBase().create_group_ldap(self.group_name,self.ldap_users[0],self.master)
            group_dn = 'cn=' + self.group_name + ',' + self.LDAP_GROUP_DN
            final_role = self._return_roles(self.user_role)
            ldapGroupBase().add_role_group(self.group_name,final_role,group_dn,self.master)
            ldapGroupBase().create_ldap_config(self.master)
        elif self.auth_type == 'InternalGrp':
            from security.ldapGroupBase import ldapGroupBase
            self.group_name = self.input.param('group_name','testgrp')
            final_role = self._return_roles(self.user_role)
            ldapGroupBase().create_int_group(self.group_name,self.ldap_users[0],final_role,final_role,self.master)


    def _return_roles(self, user_role):
        final_roles = ''
        user_role_param = user_role.split(":")
        if len(user_role_param) == 1:
            if user_role_param[0] in ('data_reader', 'data_writer', 'bucket_admin', 'views_admin', 'data_dcp_reader', 'data_monitoring', 'data_backup') and (bool(self.all_buckets)):
                final_roles = user_role_param[0] + "[*]"
            else:
                final_roles = user_role_param[0]
        else:
            for role in user_role_param:
                if role in ('data_reader', 'data_writer', 'bucket_admin', 'views_admin', 'data_dcp_reader', 'data_monitoring', 'data_backup') and bool((self.all_bucket)):
                    role = role + "[*]"
                final_roles = final_roles + ":" + role
        return final_roles

    def _assign_user_role(self):
        rest = RestConnection(self.master)
        final_role = self._return_roles(self.user_role)
        for user in self.ldap_users:
            final_user_role = [{'id': user[0], 'name': user[0], 'roles': final_role}]
            RbacBase().add_user_role(final_user_role, rest, self.auth_type)
            self.sleep(10)

    def _return_actions(self, permission_set):
        permission_set = dataRoles()._return_permission_set(permission_set)
        action_list = permission_set['permissionSet']
        return action_list.split(",")

    def rbac_test_memcached(self):
        time.sleep(30)
        self.log.info ("Current role assignment is - {0}".format(self.user_role))
        if self.auth_type != 'InternalGrp' and self.auth_type != 'LDAPGrp':
            self._assign_user_role()
        self.log.info("Current role mapping is - {0}".format(self.role_map))
        action_list = self._return_actions(self.role_map)
        for action in action_list:
            temp_action = action.split("!")
            for users in self.ldap_users:
                    if self.no_bucket_access:
                        mc, result = TestMemcachedClient().connection(self.master.ip, self.no_access_bucket_name, users[0], users[1],
                                                                      vbucket_count=self.vbuckets)
                    else:
                        mc, result = TestMemcachedClient().connection(self.master.ip, self.bucket_name, users[0], users[1],
                                                                      vbucket_count=self.vbuckets)
                        sdk_conn, result = TestSDK().connection(self.master.ip, self.bucket_name, users[0], users[1])
                    time.sleep(10)
                    if (result):
                        result_action = None
                        if temp_action[0] == 'write':
                            result_action = TestMemcachedClient().write_data(mc)
                        elif temp_action[0] == 'read':
                            result_action = TestMemcachedClient().read_data(self.master.ip, mc, self.bucket_name,
                                                                            vbucket_count=self.vbuckets)
                        elif temp_action[0] == 'statsRead':
                            result_action = TestMemcachedClient().read_stats(mc)
                        elif temp_action[0] == 'ReadMeta':
                            result_action = TestMemcachedClient().get_meta(self.master.ip, mc, self.bucket_name,
                                                                            vbucket_count=self.vbuckets)
                        elif temp_action[0] == 'WriteMeta':
                            result_action = TestMemcachedClient().set_meta(self.master.ip, mc, self.bucket_name,
                                                                            vbucket_count=self.vbuckets)
                        elif temp_action[0] == 'WriteXattr':
                            if self.no_bucket_access:
                                self.log.info ("No access to bucket via SDK")
                                result_action = True
                            else:
                                result_action = TestSDK().set_xattr(sdk_conn)
                        elif temp_action[0] == 'ReadXattr':
                            if self.no_bucket_access:
                                self.log.info ("No access to bucket via SDK")
                                result_action = True
                            else:
                                result_action = TestSDK().get_xattr(self.master.ip, sdk_conn, self.bucket_name)
                        self.log.info ("Result of action - {0} is {1}".format(action, result_action))
                        if temp_action[1] == 'False':
                            self.assertFalse(result_action)
                        else:
                            self.assertTrue(result_action)