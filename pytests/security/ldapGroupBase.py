from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from security.ldap_group import LdapGroup
from security.ldap_user import LdapUser
from security.internal_user import InternalUser
from security.external_user import ExternalUser
from security.rbacmain import rbacmain
import time
import json
import logger
log = logger.Logger.get_logger()


class ldapGroupBase:
    LDAP_GROUP_DN = "ou=Groups,dc=couchbase,dc=com"
    LDAP_HOST = '172.23.124.20'
    LDAP_PORT = "389"
    LDAP_DN = "ou=Users,dc=couchbase,dc=com"
    LDAP_OBJECT_CLASS = "inetOrgPerson"
    LDAP_ADMIN_USER = "cn=admin,dc=couchbase,dc=com"
    LDAP_ADMIN_PASS = "p@ssw0rd"
    LDAP_GROUP_QUERY =  "ou=Groups,dc=couchbase,dc=com??one?(member=cn=%u,ou=Users,dc=couchbase,dc=com)"
    LDAP_USER_DN_MAPPING = "ou=Users,dc=couchbase,dc=com??one?(uid=%u)"
    #LDAP_USER_DN_MAPPING = "ou=Users,dc=couchbase,dc=com??one?(uid:caseExactMatch:={0})"
    LDAP_GROUP_QUERY_NESTED_GRP = "ou=Groups,dc=couchbase,dc=com??one?(member=%D)"



    def __init__(self,
                 group_name=None,
                 user_list=None,
                 host=None):

        self.host = host
        self.group_name = group_name
        self.user_list = user_list

    #Setup ldap server:
    def create_ldap_config(self,host):
        cli_command = 'setting-ldap'
        options = "--hosts={0} --port={1} --user-dn-query='{2}' --bind-dn='{3}' --bind-password='{4}' --authentication-enabled={5} \
                 --authorization-enabled={6} --group-query='{7}' --enable-nested-groups=0".format(self.LDAP_HOST,389,
                                                                         self.LDAP_USER_DN_MAPPING,
                                                                         self.LDAP_ADMIN_USER,
                                                                         self.LDAP_ADMIN_PASS,1,1,
                                                                         self.LDAP_GROUP_QUERY)
        
        log.info (" Value of option is - {0}".format(options))
        remote_client = RemoteMachineShellConnection(host)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user='Administrator', password='password')
        log.info("Output of create ldap config command is {0}".format(output))
        log.info("Error of create ldap config command is {0}".format(error))
    
    def _update_ldap_config(self,options,host):
        cli_command = 'setting-ldap'
        options = "--hosts={0} --port={1} --user-dn-query='{2}' --bind-dn='{3}' --bind-password='{4}' --authentication-enabled={5} \
                         --authorization-enabled={6} --group-query='{7}' --nested-group-max-depth={8} --enable-nested-groups=1"\
                                                                                 .format(self.LDAP_HOST, 389,
                                                                                 self.LDAP_USER_DN_MAPPING,
                                                                                 self.LDAP_ADMIN_USER,
                                                                                 self.LDAP_ADMIN_PASS, 1, 1,
                                                                                 self.LDAP_GROUP_QUERY_NESTED_GRP,
                                                                                 options["nestedGroupsMaxDepth"])
        remote_client = RemoteMachineShellConnection(host)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user='Administrator', password='password')
        log.info("Output of create ldap config command is {0}".format(output))
        log.info("Error of create ldap config command is {0}".format(error))
        
        
    #Update Ldap Config
    def _update_ldap_config_rest(self,change_config,host):
        rest = RestConnection(host)
        rest.setup_ldap(change_config,'')


    #Create external ldap user
    def create_grp_usr_ldap(self,user_list,host):
        if type(user_list) == list:
            if len(user_list) == 1:
                LdapUser(user_list[0],'password',host).user_setup()
            else:
                for user in user_list:
                    LdapUser(user,'password',host).user_setup()
        else:
            LdapUser(user_list,'password',host).user_setup()

    #Create External User
    def create_grp_usr_external(self,user_list,host,roles='', groups=''):
        if len(user_list) == 1:
            user_list = user_list[0]
            payload = "name=" + user_list + "&roles=" + roles[0] + "&groups=" + groups
            ExternalUser(user_list, payload, host).user_setup()
        else:
            for index, user in enumerate(user_list):
                payload = "name=" + user + "&roles=" + roles[index] + "&groups=" + groups
                ExternalUser(user, payload, host).user_setup()

    def remove_external_user(self, users, host):
        for user in users:
            ExternalUser(user_id=user, host=host).delete_user()

    #Create internal user
    def create_grp_usr_internal(self,user_list,host,roles='', groups=''):
        if len(user_list) == 1:
            user_list = user_list[0]

            payload = "name=" + user_list + "&roles=" + roles[0] + "&password=password" + "&groups=" + groups

            InternalUser(user_list, payload, host).user_setup()
        else:

            for index, user in enumerate(user_list):
                payload = "name=" + user + "&roles=" + roles[index] + "&password=password" + "&groups=" + groups
                InternalUser(user, payload, host).user_setup()

    # Create LDAP users and groups

    def create_group_ldap(self, group_name, user_list, host, user_creation=True):
        if user_creation:
            self.create_grp_usr_ldap(user_list, host)
        LdapGroup().group_setup(group_name, user_list, host)

    # Create internal group - Create internal users and then create a group and assign role
    def create_int_group(self, group_name, user_list, grp_role, usr_role, host,  user_creation=True):
        if(len(grp_role)==1):
            grp_role = grp_role[0]
        time.sleep(2)
        self.add_role_group(group_name, grp_role, None, host)
        time.sleep(2)
        if user_creation:
            self.create_grp_usr_internal(user_list,host,usr_role, group_name)
        grole = ""
        for role in [grp_role]:
            grole = grole+","+role
        if(grole[0]==","):
            grole= grole[1:]
        # for index, urole in enumerate(usr_role):
            # usr_role[index] = urole + "," + grole



        # # self.create_grp_usr_internal(user_list, host, usr_role, group_name)
        # ldapGroupBase().add_group_role(self,group_name,description,roles,ldap_group_ref=None)

    # Create group, if internal user then ldap_ref=None, else for external group pass on the ldap_ref
    def add_role_group(self, group_name, roles, ldap_ref, host):
        log.info("Group name -- {0} :: Roles -- {1} :: LDAP REF -- {2}".format(group_name, roles, ldap_ref))
        rest = RestConnection(host)
        # group_name, description, roles, ldap_group_ref = None)
        if ldap_ref ==None:
            status, content = rest.add_group_role(group_name, group_name, roles, None)
        if ldap_ref !=None:
            status, content = rest.add_group_role(group_name, group_name, roles, ldap_ref)
        log.info("Output of adding group to roles is {0} - {1}".format(status, content))

    # if internal user then ldap_ref=None,
    # roles =['cluster_admin,admin']
    def add_role_existing_grp(self, group_names, groles, ldap_ref, host):
        for index, group_name in enumerate(group_names):
            Gcontent = self.get_group_detail(group_name, host)
            Gcontent = Gcontent[1]
            Groles = Gcontent['roles']
            roles = ""
            for role in Groles:
                roles = roles + "," + role['role']
            if (roles[0] == ","):
                roles = roles[1:]
            roles = roles + "," + groles[index]

            log.info("Group name -- {0} :: Roles -- {1} :: LDAP REF -- {2}".format(group_name, roles, ldap_ref))
            rest = RestConnection(host)
            status, content = rest.add_group_role(group_name, group_name, roles, ldap_ref)
            log.info("Output of adding group to roles is {0} - {1}".format(status, content))
            rest = RestConnection(host)
            content = rbacmain(master_ip=host, auth_type='builtin')._retrieve_user_roles()
            return content

    def remove_role_existing_grp(self, group_names, groles, ldap_ref, host):
        for index, group_name in enumerate(group_names):
            Gcontent = self.get_group_detail(group_name, host)
            Gcontent = Gcontent[1]
            Groles = Gcontent['roles']
            roles = ""
            for role in Groles:
                roles = roles + "," + role['role']
            if (roles[0] == ","):
                roles = roles[1:]
            final_roles = roles.split("," + groles[index])
            if(len(final_roles)==1 and final_roles[0]==groles[0]):
                final_roles=""
            roles = ""
            for i in final_roles:
                roles = roles + i


            log.info("Group name -- {0} :: Roles -- {1} :: LDAP REF -- {2}".format(group_name, roles, ldap_ref))
            rest = RestConnection(host)
            status, content = rest.add_group_role(group_name, group_name, roles, ldap_ref)
            log.info("Output of adding group to roles is {0} - {1}".format(status, content))
            rest = RestConnection(host)
            content = rbacmain(master_ip=host, auth_type='builtin')._retrieve_user_roles()
            content = rbacmain(master_ip=host, auth_type='builtin')._retrieve_user_roles()
            return content

    def add_int_user_role(self, user, group_name, host):
        final_group = ''
        rest = RestConnection(host)
        status, content = rest.get_user_group(user)
        for group in content['groups']:
            final_group = group + ',' + final_group
        final_group = final_group + ',' + group_name
        status, content = rest.add_user_group(final_group, user)
        log.info("Output of adding internal user to roles is {0} - {1}".format(status, content))
        return content

    def add_local_user_role(self, user, group_name, host):
        group_details = self.get_group_detail(group_name, host)
        group_details = group_details[1]
        final_group = ''
        rest = RestConnection(host)
        content = rbacmain(master_ip=host, auth_type='builtin')._retrieve_user_details(user)
        user_roles = content['roles']
        finalroles = ""
        for role in user_roles:
            finalroles = finalroles + "," + role['role']
        group_roles = ""
        for role in group_details['roles']:
            group_roles = group_roles + "," + role['role']
        if (group_roles[0] == ','):
            group_roles = group_roles[1:]
        if (finalroles[0] == ','):
            finalroles = finalroles[1:]
        final_group = finalroles + ',' + group_roles
        content = self.create_grp_usr_internal([user], host, [final_group], group_name)
        return content

    def remove_ext_user_from_grp(self,user,group,host):
        rest = RestConnection(host)

        payload = 'name=' + user + '&roles=' + roles
        rest.add_external_user(user,payload)

    def remove_int_user_role(self, user, group_name, host):
        rest = RestConnection(host)
        content = rbacmain(master_ip=host)._retrieve_user_roles()
        User_roles = json.loads(content[1])
        for user in User_roles:
            usr_role = user['roles']
            finalroles = ""
            for Urole in usr_role:
                for item in Urole['origins']:
                    if 'name' in item.keys():
                        if item['name'] == group_name:
                            continue
                    if item['type'] == 'user':
                        finalroles = finalroles + "," + Urole['role']
            if (finalroles != "" and finalroles[0] == ","):
                finalroles = finalroles[1:]
            payload = 'name=' + user['id'] + '&roles=' + finalroles + "&password=password"
            content = rest.add_set_builtin_user(user['id'], payload)
        return content

    # Delete group from couchbase
    def delete_group(self, group_name, host):
        rest = RestConnection(host)
        content = rbacmain(master_ip=host)._retrieve_user_roles()
        User_roles = json.loads(content[1])
        for user in User_roles:
            flag =0
            usr_role = user['roles']
            finalroles = ""
            for Urole in usr_role:
                for item in Urole['origins']:
                    if 'name' in item.keys():
                        if item['name'] == group_name:
                            flag = 1
                            continue
                    if item['type']=='user':
                        finalroles = finalroles +","+ Urole['role']
            if(finalroles!="" and finalroles[0]==","):
                finalroles= finalroles[1:]
            payload = 'name=' + user['id'] + '&roles=' + finalroles + "&password=password"
            if(flag ==1):
                log.info(payload)
                content = rest.add_set_builtin_user(user['id'], payload)
        rest.delete_group(group_name)

    def delete_int_group(self, group_name, host):
        rest = RestConnection(host)
        content = rbacmain(master_ip=host, auth_type='builtin')._retrieve_user_roles()

        User_roles = json.loads(content[1])
        for user in User_roles:
            flag =0
            usr_role = user['roles']
            finalroles = ""
            for Urole in usr_role:
                for item in Urole['origins']:
                    if 'name' in item.keys():
                        if item['name'] == group_name:
                            flag =1
                            continue
                    if item['type'] == 'user':
                        finalroles = finalroles + "," + Urole['role']
            if (finalroles != "" and finalroles[0] == ","):
                finalroles = finalroles[1:]

            payload = 'name=' + user['id'] + '&roles=' + finalroles + "&password=password"
            if(flag ==1):
                content = rest.add_set_builtin_user(user['id'], payload)
        rest.delete_group(group_name)

    #Update LDAP group with add/delete users
    def update_group(self,group_name,user_name,action,host,user_creation=True,external=None):
        if user_creation:
            self.create_grp_usr_ldap(user_name, host)
        LdapGroup().update_user_group(group_name,user_name,action,host,False,group_name)

        if external and action=="Add":
            if external == "ExternalUser":
                ldapGroupBase().create_grp_usr_external(user_name, host,['admin'],"")
            else:
                ldapGroupBase().create_grp_usr_external(user_name, host,[''],group_name)
        elif external:
            ldapGroupBase().remove_external_user(user_name,host)

    #Setup LDAP Config for nested ldap group
    def update_ldap_config_nested_grp(self,host,group_depth=10):
        update_ldap_config = {"nestedGroupsEnabled": 'true',
                "nestedGroupsMaxDepth": group_depth,
                'groupsQuery': self.LDAP_GROUP_QUERY_NESTED_GRP}
        self._update_ldap_config(update_ldap_config, host)

    #Create nested LDAP group
    def create_nested_grp(self, group_prefix=None, user_prefix=None, level=None,host=None):
        UsersCreated = []
        GroupsCreated = []
        first_user = [user_prefix + str(0)]
        first_group = group_prefix + str(0)
        self.create_grp_usr_ldap(first_user, host)
        GroupsCreated.append(first_group)
        UsersCreated.append(first_user[0])
        LdapGroup().group_setup(first_group, first_user,host)
        last_group = group_prefix + str(0)
        for i in range(1,level):
            current_user = [user_prefix + str(i)]
            current_group = group_prefix + str(i)
            UsersCreated.append(current_user[0])
            GroupsCreated.append(current_group)
            self.create_group_ldap(current_group, current_user,host)
            LdapGroup().update_user_group(current_group,current_user,'Add',host,True,last_group)
            last_group = current_group
            last_user = current_user
        return first_group, last_group, first_user, last_user , UsersCreated,GroupsCreated

    #Check if the group was created
    def check_grp_created(self, group_name, host):
        rest = RestConnection(host)
        status, content = rest.get_group_list()
        for group in content:
            if group['id'] == group_name:
                return True
        return False

    def get_user_detail(self, user_name, host):
        rest = RestConnection(host)
        status, content = rest.get_user_group(user_name)
        return content

    def get_group_detail(self, group_name, host):
        rest = RestConnection(host)
        status, content = rest.get_group_details(group_name)
        return status, content

    def check_permission(self, user_name, host,external=False):
        rest = RestConnection(host)
        if external:
            content = rest.check_user_permission(user_name, 'password', 'cluster.admin.external!all')
            return content['cluster.admin.external!all']
        else:
            content = rest.check_user_permission(user_name, 'password', 'cluster.admin.internal!all')
            return content['cluster.admin.internal!all']

    def check_permission_external(self, user_name, host):
        rest = RestConnection(host)
        content = rest.check_user_permission(user_name, 'password', 'cluster.admin.external!all')
        return content['cluster.admin.external!all']

    # Create LDAP Groups and Users in a sequence for no. of groups and users
    def create_ldap_grp_user(self, group_no, user_no, roles, host, grp_prefix='grp', usr_prefix='usr',external=False):
        user_list = []
        grp_list = []
        final_usr_list = []
        for i in range(0, int(group_no)):
            for j in range(0, len(user_no)):
                user_name = usr_prefix + str(i) + str(j)
                self.create_grp_usr_ldap(user_name, host)
                user_list.append(user_name)
            LdapGroup().group_setup(grp_prefix + str(i), user_list, host)
            grp_list.append(grp_prefix + str(i))
            final_usr_list.append(user_list)
            user_list = []
        final_roles = ""
        for i in range(0, int(group_no)):
            if '?' in roles[i]:
                current_role = roles[i].split('?')
            else:
                current_role = [roles[i]]
            if len(current_role) == 1:
                final_roles = current_role[0]
            else:
                for role in current_role:
                    final_roles = role + "," + final_roles
            group_dn = 'cn=' + grp_list[i] + ',' + self.LDAP_GROUP_DN

            rest = RestConnection(host)
            status, content = rest.add_group_role(grp_list[i], grp_list[i], final_roles, group_dn)

        if external:
            for i,group in enumerate(grp_list):
                self.create_grp_usr_external(final_usr_list[i], host, ['']*len(final_usr_list[i]), group)

        return grp_list, final_usr_list

    def create_multiple_group_user(self, users, user_role, group_no, group_name, group_role, host):

        group_name = group_name.split(":")
        names = []
        if (group_name[0] == 'testgrp'):
            for num in range(1, int(group_no) + 1, 1):
                names.append('testgrp' + str(num))
            group_name = names

        group_role = [group_role.replace(":", ",") for group_role in group_role.split("?")]
        final_role=[]
        if isinstance(user_role,list)==False:
            final_role = (user_role).split('?')
            final_role = [(role.replace('-', ",")).split(":") for role in final_role]
        else:
            final_role = user_role
        for index, grp_name in enumerate(group_name):
            ldapGroupBase().create_int_group(grp_name, users[index], group_role[index], final_role[index], host)
        # ldapGroupBase().delete_int_group("testgrp2", host)
        return group_name
    def _return_roles(self, user_role):
        final_roles = ''
        user_role_param = user_role.split(":")
        if len(user_role_param) == 1:
            if user_role_param[0] in (
            'data_reader', 'data_writer', 'bucket_admin', 'views_admin', 'data_dcp_reader', 'data_monitoring',
            'data_backup') and (bool(self.all_buckets)):
                final_roles = user_role_param[0] + "[*]"
            else:
                final_roles = user_role_param[0]
        else:
            for role in user_role_param:
                if role in (
                'data_reader', 'data_writer', 'bucket_admin', 'views_admin', 'data_dcp_reader', 'data_monitoring',
                'data_backup') and bool((self.all_bucket)):
                    role = role + "[*]"
                final_roles = final_roles + ":" + role
        if (final_roles[0] == ":"):
            final_roles = final_roles[1:]
        return final_roles