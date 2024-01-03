from membase.api.rest_client import RestConnection
from security.rbacmain import rbacmain
from security.ldapGroupBase import ldapGroupBase
from basetestcase import BaseTestCase
import random
import math
from security.ldap_user import LdapUser
from security.ldap_group import LdapGroup
from security.internal_user import InternalUser
from security.external_user import ExternalUser
import mc_bin_client

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





class ldapGroup(BaseTestCase):
    LDAP_GROUP_DN = "ou=Groups,dc=couchbase,dc=com"
    Users = []
    Groups = []

    def setUp(self):
        super(ldapGroup, self).setUp()
        self.Users = []
        self.Groups = []
        self.group_name = self.input.param('group_name','testgrp')
        self.group_no = self.input.param('group_no',None)
        self.user_no = self.input.param('user_no','')
        self.user_list = self.input.param('users','oel1:oel2')

        self.user_roles = self.input.param('user_roles','admin')
        if self.user_no !='':
            self.user_list=[]
            user_list = []
            number = self.user_no.split(":")
            count = 0
            for num in number:
                for user_num in range(int(num)):
                    user_list.append("usr"+str(count))
                    count=count+1
                self.user_list.append(user_list)
                user_list=[]
        if("?" in self.user_list):
            self.user_list= self.user_list.split("?")
            usr_list= []
            for user in self.user_list:
                temp= user.split(":")
                usr_list.append(temp)
            self.user_list= usr_list
        else:
            if(":" in self.user_list):
                self.user_list= self.user_list.split(":")
        self.group_roles = self.input.param('group_roles','admin')
        # if("?" not in self.group_roles):
        self.group_roles= self.group_roles.split(":")
        self.auth_type = self.input.param('auth_type','LDAPGroup')
        if self.auth_type in ['LDAPGroup','ExternalGroup','ExternalUser']:
            ldapGroupBase().create_ldap_config(self.master)
        if("?" not in self.user_roles):
            self.user_roles = self.user_roles.split(":")
        self.group_prefix = self.input.param('group_prefix','newgrp')
        self.user_prefix = self.input.param('user_prefix','newusr')
        self.nested_level = self.input.param('nested_level',10)
        self.access_type = self.input.param('access_type','last')
        self.multiple_setup = self.input.param('multiple_setup',False)
        if(self.multiple_setup):
            self.multiple_grp_setup()
        self.initial_setup = self.input.param('initial_setup',False)
        self.rest = RestConnection(self.master)
        self.rest.invalidate_ldap_cache()
        if self.initial_setup:
            self.do_initial_setup()

    def AfterTestExec(self):
        for grp in self.Groups:
            LdapGroup(group_name=grp,host=self.master).delete_group()
            ldapGroupBase().delete_group(grp, self.master)
        users = []
        for subl in self.Users:
            if isinstance(subl,list):
                users.extend([user for user in subl])
            else:
                users.append(subl)
        print(self.Groups)
        print(users)
        for usr in users:
            LdapUser(user_name=usr, password='password', host=self.master).delete_user()
            InternalUser(user_id=usr,host=self.master).delete_user()
            ExternalUser(user_id=usr,host=self.master).delete_user()

    def tearDown(self):
        print(self.Groups)
        print(self.Users)
        if isinstance(self,ldapGroup)==True and self.Groups!=[] and self.Users!=[]:
            self.AfterTestExec()

        super(ldapGroup, self).tearDown()

    def addToUserList(self,users):
        if isinstance(users,list)==True:
            for user in users :
                self.Users.append(user)


    def addToGroupList(self,groups):
        if isinstance(groups,list)==True:
            for group in groups :
                self.Groups.append(group)

    def do_initial_setup(self):

        ldapGroupBase().delete_group(self.group_name,self.master)
        self.addToUserList(self.user_list)
        self.addToGroupList([self.group_name])

        if self.auth_type == "ExternalGroup":
            ldapGroupBase().create_group_ldap(self.group_name, self.user_list, self.master)
            group_dn = 'cn=' + self.group_name + ',' + self.LDAP_GROUP_DN
            if len(self.user_list) > len(self.user_roles):
                for num in range(0, len(self.user_list) - 1):
                    self.user_roles.append(self.user_roles[0])
            ldapGroupBase().add_role_group(self.group_name, self.group_roles[0], group_dn, self.master)
            self.log.info(self.user_list)
            ldapGroupBase().create_grp_usr_external(self.user_list,self.master,['']*len(self.user_list),self.group_name)
        elif self.auth_type == "ExternalUser":
            ldapGroupBase().create_group_ldap(self.group_name, self.user_list, self.master)
            group_dn = 'cn=' + self.group_name + ',' + self.LDAP_GROUP_DN
            if len(self.user_list) > len(self.user_roles):
                for num in range(0, len(self.user_list) - 1):
                    self.user_roles.append(self.user_roles[0])
            ldapGroupBase().create_grp_usr_external(self.user_list,self.master, self.user_roles, "")
        elif self.auth_type == 'LDAPGroup':
            ldapGroupBase().create_group_ldap(self.group_name,self.user_list,self.master)
            group_dn = 'cn=' + self.group_name + ',' + self.LDAP_GROUP_DN
            ldapGroupBase().add_role_group(self.group_name,self.group_roles[0],group_dn,self.master)
        elif self.auth_type == 'InternalGroup':
            for index , usr in enumerate(self.user_list):
                ldapGroupBase().create_grp_usr_internal([usr],self.master,[self.user_roles[index]])
                content= rbacmain(master_ip=self.master, auth_type='builtin')._retrieve_user_roles()
                if (len(self.user_list) > len(self.user_roles)):
                    for num in range(0, len(self.user_list) - 1):
                        self.user_roles.append(self.user_roles[0])
                final_role = self.user_roles[index]+","+ self.group_roles[0]
                ldapGroupBase().create_int_group(self.group_name, [usr], self.group_roles, [self.user_roles[index]],self.master)

        self.rest.invalidate_ldap_cache()

    def test_external_user_group(self):
        for user in self.user_list:
            result = ldapGroupBase().check_permission(user, self.master, external=self.auth_type in ["ExternalUser","ExternalGroup"])
            self.assertTrue(result)


    def testLdap(self):
        result = ldapGroupBase().check_grp_created(self.group_name, self.master)
        content = ldapGroupBase().get_user_detail('oel1',self.master)
        self.assertTrue(result)

    def test_add_ldap_group(self):
        result = ldapGroupBase().check_grp_created(self.group_name,self.master)
        self.assertTrue(result)

    def test_delete_group(self):
        ldapGroupBase().delete_group(self.group_name,self.master)
        result = ldapGroupBase().check_grp_created(self.group_name,self.master)
        self.assertFalse(result)

    def test_add_user(self):

        self.add_user = self.input.param('add_user').split(':')
        self.Users.append(self.add_user)
        if self.auth_type == "ExternalGroup":
            ldapGroupBase().update_group(self.group_name, self.add_user, 'Add', self.master, external=self.auth_type)
        elif self.auth_type == "ExternalUser":
            ldapGroupBase().update_group(self.group_name, self.add_user, 'Add', self.master, external=self.auth_type)
        self.sleep(10)
        if self.auth_type == 'LDAPGroup':
            ldapGroupBase().update_group(self.group_name, self.add_user,'Add', self.master)
            self.sleep(10)
        if self.auth_type == "InternalGroup":
            content = ldapGroupBase().get_group_detail(self.group_name, self.master)
            final_grp = ""
            content = content[1]['roles']
            for role in content:
                final_grp = final_grp+","+role['role']
            if final_grp[0] == ",":
                final_grp = final_grp[1:]
            final_roles = self.user_roles[0] + "," + final_grp
            ldapGroupBase().create_grp_usr_internal(self.add_user,self.master,[final_roles], self.group_name)
            self.sleep(10)

        self.rest.invalidate_ldap_cache()
        self.sleep(10)
        result = ldapGroupBase().check_permission(self.add_user[0],self.master, external = (self.auth_type in ["ExternalUser","ExternalGroup"]))
        self.assertTrue(result)

    def test_remove_user(self):
        self.remove_user = self.input.param('remove_user').split(':')
        self.Users.append(self.remove_user)
        if self.auth_type in ["ExternalUser","ExternalGroup"]:
            ldapGroupBase().update_group(self.group_name, self.remove_user,'Remove', self.master, external = self.auth_type)
        else:
            ldapGroupBase().update_group(self.group_name, self.remove_user, 'Remove', self.master)
        self.sleep(10)
        self.rest.invalidate_ldap_cache()
        self.sleep(10)
        user = ldapGroupBase().get_user_detail(self.remove_user[0], self.master)
        result = ldapGroupBase().check_permission(self.remove_user[0],self.master, external = self.auth_type in ["ExternalUser","ExternalGroup"])
        self.sleep(10)
        self.assertFalse(result)

    # Simple test case setting up depth and < configured group depth
    # Add more than 10 - 30 nested groups
    # Check for random number
    def test_nested_grp(self):
        nested_grp_depth = self.input.param('nested_grp_depth',3)
        self.log.info("Current nested depth is:  - {0}".format(nested_grp_depth))
        config_nested_grp_depth = self.input.param('config_nested_grp_depth',10)
        self.log.info("Current config nested group depth is:  - {0}".format(config_nested_grp_depth))
        random_user = self.input.param('random_user',False)
        ldapGroupBase().update_ldap_config_nested_grp(self.master,config_nested_grp_depth)
        self.sleep(3)
        first_group, last_group, first_user, last_user, listOfUsers, listOfGrps = ldapGroupBase().create_nested_grp \
            ('newgrp', 'newusr', nested_grp_depth,self.master)
        self.addToUserList(listOfUsers)
        self.addToGroupList(listOfGrps)
        result=''
        group_dn = 'cn=' + last_group +  ',' + self.LDAP_GROUP_DN
        if(self.auth_type=='InternalGroup'):
             ldapGroupBase().add_role_group(last_group,'admin',group_dn,self.master)
             result = ldapGroupBase().check_permission(first_user[0], self.master)
        else:
             ldapGroupBase().add_role_group(last_group, 'admin', group_dn, self.master)
             self.sleep(30)
             self.rest.invalidate_ldap_cache()
             self.sleep(30)
             result = ldapGroupBase().check_permission(first_user[0],self.master)
             self.assertTrue(result)

        result = ldapGroupBase().check_permission(last_user[0],self.master)
        self.assertTrue(result)
        if random_user:
            level_no = random.randrange(1,nested_grp_depth)
            result = ldapGroupBase().check_permission('newusr' + str(level_no) ,self.master)
            self.assertTrue(result)

        add_layer = self.input.param('add_layer',None)
        if(add_layer!=None):
            ldapGroupBase().create_group_ldap('topGrp', [last_group], self.master)
            ldapGroupBase().add_role_group('topGrp', 'admin', group_dn, self.master)
            Gcontent = ldapGroupBase().get_group_detail(self.group_name, self.master)
            content = ldapGroupBase().check_permission(last_user[0],self.master)
            self.assertTrue(content)

    def test_nested_grp_diff_level(self):
        self.sleep(30)
        ldapGroupBase().update_ldap_config_nested_grp(self.master, 10)
        first_group, last_group, first_user, last_user, listOfUsers, listOfGrps = \
            ldapGroupBase().create_nested_grp(self.group_prefix, self.user_prefix, self.nested_level, self.master)
        self.addToUserList(listOfUsers)
        self.addToGroupList(listOfGrps)
        role = ""
        for i in self.group_roles:
            role = role+","+i
        if role[0] == ',' :
            role = role[1:]
        self.group_roles= role
        add_usr = self.input.param('add_user',None)

        if self.access_type == 'last':
            group_dn = 'cn=' + last_group +  ',' + self.LDAP_GROUP_DN
            ldapGroupBase().add_role_group(last_group,self.group_roles,group_dn,self.master)
            self.rest.invalidate_ldap_cache()
            self.sleep(30)
            result = ldapGroupBase().check_permission(first_user[0] ,self.master)
            self.sleep(30)
            self.assertTrue(result)
            result = ldapGroupBase().check_permission(last_user[0] ,self.master)
            self.assertTrue(result)
            #Choose a user with the same group and first group
        elif (self.access_type == 'middle'):
            level_no = int(math.floor(self.nested_level/2))
            group_dn = 'cn=' + self.group_prefix + str(level_no) + ',' +  self.LDAP_GROUP_DN
            ldapGroupBase().add_role_group(self.group_prefix + str(level_no),self.group_roles,group_dn,self.master)
            self.rest.invalidate_ldap_cache()
            self.sleep(30)
            result = ldapGroupBase().check_permission(self.user_prefix + str(level_no) ,self.master)
            self.assertTrue(result)
            result = ldapGroupBase().check_permission(first_user[0] ,self.master)
            self.assertTrue(result)
            #Choose a user with the same group and first group
        elif self.access_type == 'random':
            level_no = random.randrange(1,self.nested_level)
            group_dn = 'cn=' + self.group_prefix + str(level_no) + ',' + self.LDAP_GROUP_DN
            ldapGroupBase().add_role_group(self.group_prefix + str(level_no), self.group_roles, group_dn, self.master)
            self.rest.invalidate_ldap_cache()
            self.sleep(30)
            result = ldapGroupBase().check_permission(self.user_prefix + str(level_no), self.master)
            self.assertTrue(result)
            result = ldapGroupBase().check_permission(first_user[0], self.master)
            self.assertTrue(result)
            add_grp = self.input.param('add_grp', None)
            del_grp = self.input.param('del_grp', None)
            del_usr = self.input.param('del_usr', None)
            if (add_grp != None):
                ldapGroupBase().create_group_ldap('addedGrp', ['jyotsna3'], self.master)
                ldapGroupBase().create_group_ldap(self.group_prefix + str(level_no), ['addedGrp'], self.master)
                group_dn = 'cn=' + 'addedGrp' + ',' + self.LDAP_GROUP_DN
                ldapGroupBase().add_role_group('addedGrp', 'admin', group_dn, self.master)
                self.rest.invalidate_ldap_cache()
                self.sleep(30)
                content = ldapGroupBase().check_permission('jyotsna3', self.master)
                self.assertTrue(content)
                if del_grp!=None:
                    ldapGroupBase().delete_group('addedGrp', self.master)
                    self.rest.invalidate_ldap_cache()
                    self.sleep(30)
                    result = ldapGroupBase().check_grp_created('addedGrp', self.master)
                    self.assertFalse(result)

            if (add_usr!= None):
                ldapGroupBase().create_group_ldap(self.group_name, self.user_list, self.master)
                ldapGroupBase().update_user_group(self.group_prefix + str(level_no),['jeremy'],'Add', self.master,True,last_group)
                self.rest.invalidate_ldap_cache()
                self.sleep(30)
                group_dn = 'cn=' + self.group_prefix + str(level_no) + ',' + self.LDAP_GROUP_DN
                content = ldapGroupBase().check_permission('jyotsna3', self.master)
                self.assertTrue(content)
                if del_usr!=None:
                    ldapGroupBase().update_group(self.group_name, self.remove_user, 'Remove', self.master)
                    self.rest.invalidate_ldap_cache()
                    self.sleep(30)
                    result = ldapGroupBase().check_permission(self.remove_user[0], self.master)
                    self.assertFalse(result)


        elif self.access_type == 'first':
            level_no = random.randrange(1,self.nested_level)
            group_dn = 'cn=' + first_group + ',' + self.LDAP_GROUP_DN
            ldapGroupBase().add_role_group(first_group, self.group_roles,group_dn,self.master)
            self.rest.invalidate_ldap_cache()
            self.sleep(10)
            result = ldapGroupBase().check_permission(first_user[0] ,self.master)
            self.assertTrue(result)

    #First Assign the last group role
    #choose a random group and then assign a different role. Check that the higher role wins
    def test_nested_grp_ind_level(self):
        ldapGroupBase().update_ldap_config_nested_grp(self.master)
        self.sleep(10)
        new_role = self.input.param('new_role').split(':')
        first_group, last_group, first_user, last_user, listOfUsers, listOfGrps = \
            ldapGroupBase().create_nested_grp(self.group_prefix, self.user_prefix, self.nested_level, self.master)
        self.sleep(3)
        self.addToUserList(listOfUsers)
        self.addToGroupList(listOfGrps)
        group_dn = 'cn=' + last_group +  ',' + self.LDAP_GROUP_DN
        self.sleep(3)
        # ldapGroupBase().check_permission(last_user[0], self.master)
        ldapGroupBase().add_role_group(last_group,self.group_roles[0],group_dn,self.master)
        self.rest.invalidate_ldap_cache()
        self.sleep(10)
        # ldapGroupBase().check_permission(last_user[0] ,self.master)
        level_no = random.randrange(1,self.nested_level)
        group_dn = 'cn=' + self.group_prefix + str(level_no) + ',' +  self.LDAP_GROUP_DN
        ldapGroupBase().add_role_group(self.group_prefix + str(level_no),new_role[0],group_dn,self.master)
        self.rest.invalidate_ldap_cache()
        self.sleep(10)
        result = ldapGroupBase().check_permission(self.user_prefix + str(level_no) ,self.master)
        self.assertTrue(result)
        result = ldapGroupBase().check_permission(self.user_prefix + str(level_no-1) ,self.master)
        self.rest.invalidate_ldap_cache()
        self.assertTrue(result)


    #Different user in different group, add multiple groups and check permission
    def test_multi_grp_usr(self):
        #ldapGroupBase().update_ldap_config_nested_grp(self.master,10)
        self.sleep(3)
        if len(self.group_roles) == 1:
            grp = []
            for i in range(self.group_no):
                grp.append(self.group_roles[0])
            self.group_roles= grp
        grp_list, usr_list = ldapGroupBase().create_ldap_grp_user(self.group_no,self.user_no,
                                                                  self.group_roles, self.master,
                                                                  external=self.auth_type in ["ExternalGroup","ExternalUser"])
        self.addToUserList(usr_list)
        self.addToGroupList(grp_list)
        self.rest.invalidate_ldap_cache()
        self.sleep(30)
        for i in range(0,int(self.group_no)):
            for usr in usr_list[i]:
                result = ldapGroupBase().check_permission(usr, self.master, external=self.auth_type in ["ExternalUser","ExternalGroup"])
                if 'admin' in self.group_roles[i]:
                    self.assertTrue(result)
                elif 'cluster_admin' in self.group_roles[i]:
                    self.assertFalse(result)

    #Same user in multiple group - group having same role and group having higher role
    def test_usr_in_multi_grp(self):
        grp_list, usr_list = ldapGroupBase().create_ldap_grp_user(self.group_no,self.user_no, self.group_roles,
                                                                  self.master,external=self.auth_type in ["ExternalUser","ExternalGroup"])
        self.addToGroupList(grp_list)
        self.addToUserList(usr_list)
        common_user = ['comusr']
        ldapGroupBase().create_grp_usr_ldap(common_user[0], self.master)
        self.addToUserList(common_user)
        for i in range(0, int(self.group_no) - 1):
            ldapGroupBase().update_group(grp_list[i], common_user,'Add',self.master)
            self.sleep(5)
        if self.auth_type in ["ExternalUser","ExternalGroup"]:
            groups = ','.join(grp_list)
            ldapGroupBase().create_grp_usr_external(common_user,self.master,[''],groups)
            self.sleep(10)
        result = ldapGroupBase().check_permission('comusr', self.master, external=self.auth_type in ["ExternalUser","ExternalGroup"])
        self.assertTrue(result)

    ''' -- Local Group Starts here -----'''

    def test_add_local_group(self):
        result = ldapGroupBase().check_grp_created(self.group_name,self.master)
        self.assertTrue(result)

    #Add users  to local group
    def test_add_grp_local_usr(self):
        final_role = ''
        final_grp = ''
        add_grp = self.input.param('add_grp').split(':')
        add_role = self.input.param('add_role').split(':')
        for i in range (0, len(add_grp)):
            # add_role_group(self, group_name, roles, ldap_ref, host)
            ldapGroupBase().add_role_group(add_grp[i],add_role[i],None,self.master)
            self.sleep(3)
            final_grp = add_grp[i] + ',' + final_grp
        for user in self.user_list:
            ldapGroupBase().add_int_user_role(user,final_grp,self.master)
            self.sleep(3)
        for grp in add_grp:
            result = ldapGroupBase().check_grp_created(self.group_name, self.master)
            self.assertTrue(result)

        #Validate the user group and roles here

    # Add external/ldap users  to local group
    def test_add_grp_local_usr_ldap(self):
        final_role = ''
        final_grp = ''
        add_grp = self.input.param('add_grp').split(':')
        add_role = self.input.param('add_role').split(':')
        for i in range (0, len(add_grp)):
            # add_role_group(self, group_name, roles, ldap_ref, host)
            ldapGroupBase().add_role_group(add_grp[i],add_role[i],None,self.master)
            self.sleep(3)
            final_grp = add_grp[i] + ',' + final_grp
        for user in self.user_list:
            ldapGroupBase().add_int_user_role(user,final_grp,self.master)
            self.sleep(3)
        for grp in add_grp:
            result = ldapGroupBase().check_grp_created(self.group_name, self.master)
            self.assertTrue(result)

        #Validate the user group and roles here

    #Remove users from local group
    def test_rem_grp_local_usr(self):
        remove_usr = self.input.param('remove_usr').split(':')
        remove_grp = self.input.param('remove_grp').split(':')
        for i in range(0,len(remove_usr)):
            ldapGroupBase().remove_int_user_role(remove_usr[i], remove_grp[i], self.master)
            self.sleep(3)
        for user in self.user_list:
                ldapGroupBase().get_user_detail(user,self.master)

    def multiple_grp_setup(self):
        group_roles = ""
        if isinstance(self.group_roles, list):
            for role in self.group_roles:
                group_roles = group_roles+":"+role
            if group_roles[0]==":":
                group_roles= group_roles[1:]
        else:
            group_roles= self.group_roles
        group_no = self.input.param('group_no',"1")
        group_no = int(group_no)
        user_role = []
        final_roles=[]
        number = self.user_no.split(":")
        self.user_roles
        count = 0
        roleIndex =0
        if ':' not in self.user_roles:
            for num in number:
                for user_num in range(int(num)):
                    user_role.append(self.user_roles[roleIndex])
                final_roles.append(user_role)
                user_role = []
                roleIndex = roleIndex+1
        else:
            final_roles = self.user_roles
        self.user_roles= final_roles
        group_list = ldapGroupBase().create_multiple_group_user(self.user_list, self.user_roles, group_no, self.group_name, group_roles, self.master)
        self.addToUserList(self.user_list)
        self.addToGroupList(group_list)
        return  self.user_list, self.user_roles, group_no, self.group_name, group_roles

    def test_multiple_user_and_group_int(self):
        self.assertFalse(self.multiple_setup,"Please set the multiple_setup parameter to False")
        user_list,user_roles,group_no,group_name,group_roles= self.multiple_grp_setup()
        group_roles = [grpRoles.replace(":",",") for grpRoles in group_roles.split("?")]

        if isinstance(self.user_roles,list)==False:
            user_roles = (self.user_roles).split('?')
            user_roles = [(role.replace('-', ",")).split(":") for role in user_roles]
        for num in range(0,group_no):
            grp= group_name+str(num+1)
            result = ldapGroupBase().check_grp_created(grp,self.master)
            self.sleep(3)
            self.assertTrue(result,'Group '+ grp+' not created properly')
            users = user_list[num]
            uRoles = user_roles[num]
            grp_roles = group_roles[num]
            roles=[]


            for uRole in uRoles:
                role = uRole+","+grp_roles
                roles.append(role)
            for num ,user in enumerate(users):
                if  'admin' not in roles[num].split(','):
                    result=ldapGroupBase().check_permission(user,self.master)

                    self.assertFalse(result,"User "+user+" has admin permission when it should have permission for "+roles[num])
                elif 'admin'  in roles[num].split(','):
                    result = ldapGroupBase().check_permission(user, self.master)
                    self.assertTrue(result,"User " + user + " has non Admin permission when it should have permission for " + roles[num])



    def test_single_user_multiple_groups(self):
        add_group = self.input.param('add_group','addGrp')
        add_user = self.input.param('add_user_to_grp', 'oel1')
        content = rbacmain(master_ip=self.master, auth_type='builtin')._retrieve_user_details(add_user)
        #contains all the group names for the user
        grp = []
        for role in content['roles']:
            for type in role['origins']:
                if 'name' in type.keys():
                    grp.append(type['name'])
        grp.append(add_group)
        content = ldapGroupBase().add_int_user_role(add_user,add_group,self.master)
        content = rbacmain(master_ip=self.master, auth_type='builtin')._retrieve_user_details(add_user)
        for role in content['roles']:
            for type in role['origins']:
                if 'name' in type.keys():
                    self.assertTrue(type['name'] in grp , "The user roles doesnt have the grp: "+type['name'])



    def test_add_role_to_grp(self):
        add_role = self.input.param('add_role','').split('?')
        add_to_grp = self.input.param('add_to_grp','')
        ldapGroupBase().add_role_existing_grp([add_to_grp], add_role, None, self.master)


    def test_del_mult_grp(self):
        group_names = self.input.param('remove_grp_name', '')
        ldapGroupBase().delete_int_group(group_names,self.master)
        result = ldapGroupBase().check_grp_created(group_names,self.master)
        self.assertFalse(result)

    def test_del_role_from_grp(self):
        group_names = self.input.param('remove_grp_name','')
        groles = self.input.param('remove_grp_role','')
        num = group_names[-1]
        user_name= self.user_list[int(num)-1][0]

        content = ldapGroupBase().get_group_detail(group_names, self.master)
        ldapGroupBase().remove_role_existing_grp( [group_names], groles, None, self.master)
        result = ldapGroupBase().check_permission(user_name ,self.master)
        self.assertFalse(result)

    def test_add_remove_user(self):
        self.rem_usr = self.input.param('rem_usr',"")
        self.rem_grp = self.input.param('rem_from_grp', "")
        content =ldapGroupBase().remove_int_user_role( self.rem_usr, self.rem_grp, self.master)
        result = ldapGroupBase().check_permission(self.rem_usr, self.master)
        self.assertTrue(result)
        result= ldapGroupBase().add_local_user_role( self.rem_usr, self.rem_grp, self.master)
        content = ldapGroupBase().check_permission(self.rem_usr,self.master)
        self.assertTrue(content)

    def test_add_remove_ext_user(self):
        self.rem_usr = self.input.param('rem_usr', "")


    def test_ldap_group_cache(self):
        ldapGroupBase().create_group_ldap(self.group_name, self.user_list, self.master)
        group_dn = 'cn=' + self.group_name + ',' + self.LDAP_GROUP_DN
        ldapGroupBase().add_role_group(self.group_name, self.group_roles[0], group_dn, self.master)
        self.sleep(3)
        content = ldapGroupBase().check_permission(self.user_list[0],self.master)
        ldapGroupBase().remove_role_existing_grp( [self.group_name],['admin'], group_dn, self.master)
        self.sleep(3)
        Gcontent = ldapGroupBase().get_group_detail(self.group_name, self.master)
        content = ldapGroupBase().check_permission(self.user_list[0], self.master)
        ldapGroupBase().add_role_group(self.group_name, 'cluster_admin', group_dn, self.master)
        self.sleep(3)
        content = ldapGroupBase().check_permission(self.user_list[0], self.master)
        ldapGroupBase().add_role_group(self.group_name, 'admin', group_dn, self.master)
        self.sleep(3)
        content = ldapGroupBase().check_permission(self.user_list[0], self.master)

    def test_rebal_in_out(self):
        if len(self.servers) > 1:
            servers_in = self.servers[1:]
            self.cluster.rebalance(self.servers, servers_in, [])
            self.sleep(5)
            for server in self.servers[:2]:
                content = ldapGroupBase().check_permission(self.user_list[0], server)
                self.assertTrue(content)
            self.sleep(3)
            self.cluster.rebalance(self.servers, [], servers_in)
            self.sleep(5)
            content = ldapGroupBase().check_permission(self.user_list[0], self.master)
            self.assertTrue(content)

    # Test with one cluster LDAP enabled and setup on the other non LDAP cluster with half encryption using local user MB-39570
    def test_XDCR_with_ldap_setup_half_encryption(self):
        rest2 = RestConnection(self.servers[1])
        rest1 = RestConnection(self.servers[0])

        rest2.remove_all_replications()
        rest2.remove_all_remote_clusters()

        rest2.create_bucket("default",ramQuotaMB=256)
        rest1.create_bucket("default",ramQuotaMB=256)
        remote_cluster2 = 'C2'
        remote_server01 = self.servers[0]

        remote_id = rest2.add_remote_cluster(remote_server01.ip, 8091, 'cbadminbucket', 'password',
                                            remote_cluster2,demandEncryption="on",encryptionType="half")
        replication_id = rest2.start_replication('continuous', 'default', remote_cluster2)
        if replication_id is not None:
            self.assertTrue(True, "Replication was not created successfully")

        rest2.remove_all_replications()
        rest2.remove_all_remote_clusters()

    # Test to check if the server supports SCRAM-SHA variants of SASL Mechanisms in addition to PLAIN MB-39570
    def test_list_sasl_mechanisms(self):
        mc = mc_bin_client.MemcachedClient(self.master.ip, 11210)
        mechanisms = mc.sasl_mechanisms()
        self.log.info("Supported SASL Mechanisms {0}".format(mechanisms))
        self.assertTrue(b"PLAIN" in mechanisms)
        found = False
        for mech in mechanisms:
            if b"SCRAM-SHA" in mech:
                found = True

        self.assertTrue(found)

    # Test with one cluster LDAP enabled and setup on the other non LDAP cluster with no encryption using ldap user MB-39570
    def test_XDCR_with_ldap_setup_no_encryption(self):
        rest2 = RestConnection(self.servers[1])
        rest1 = RestConnection(self.servers[0])

        rest2.remove_all_replications()
        rest2.remove_all_remote_clusters()

        rest2.create_bucket("default", ramQuotaMB=256)
        rest1.create_bucket("default", ramQuotaMB=256)
        remote_cluster2 = 'C2'
        remote_server01 = self.servers[0]

        remote_id = rest2.add_remote_cluster(remote_server01.ip, 8091, self.user_list[0], 'password',
                                             remote_cluster2)
        replication_id = rest2.start_replication('continuous', 'default', remote_cluster2)
        if replication_id is not None:
            self.assertTrue(True, "Replication was not created successfully")

        rest2.remove_all_replications()
        rest2.remove_all_remote_clusters()















