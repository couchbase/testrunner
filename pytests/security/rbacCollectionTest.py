from membase.api.rest_client import RestConnection
import urllib.request, urllib.parse, urllib.error
import json
from remote.remote_util import RemoteMachineShellConnection
import subprocess
import socket
import fileinput
import sys
from subprocess import Popen, PIPE
from basetestcase import BaseTestCase
from sdk_client3 import SDKClient
from collection.collections_cli_client import CollectionsCLI
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats
from security.rbacmain import rbacmain
from lib.membase.api.rest_client import RestHelper
from security.x509main import x509main
from copy import deepcopy
from threading import Thread, Event
from security.ldap_user import LdapUser
from security.ldap_group import LdapGroup
from security.internal_user import InternalUser
from security.external_user import ExternalUser
from security.ldapGroupBase import ldapGroupBase

class ServerInfo():
    def __init__(self,
                 ip,
                 port,
                 ssh_username,
                 ssh_password,
                 ssh_key=''):

        self.ip = ip
        self.rest_username = ssh_username
        self.rest_password = ssh_password
        self.port = port
        self.ssh_key = ssh_key


class rbacCollectionTest(BaseTestCase):
    def wait(self):
        self.sleep(2)

    def setUp(self):
        super(rbacCollectionTest, self).setUp()
        self.rest = CollectionsRest(self.master)
        self.cli = CollectionsCLI(self.master)
        self.rest_client = RestConnection(self.master)
        self.test_scope = self.input.param("test_scope",'collection')
        self.update_role = self.input.param("update_role",False)
        self.auth_type = self.input.param("auth_type",'users')
        self.sasl_mech = None
        self.certpath = None
        if self.auth_type == 'InternalGroup':
            ldapGroupBase().create_int_group("group1", ['user1'], '', [''], self.master,  user_creation=True)
            ldapGroupBase().create_int_group("group2", ['user2'], '', [''], self.master,  user_creation=True)
        elif self.auth_type == "externalUser":
            ldapGroupBase().create_ldap_config(self.master)
            LdapUser('user1','password',self.master).user_setup()
            LdapUser('user2','password',self.master).user_setup()
            self.sasl_mech = 'PLAIN'
            temp_cert = self.rest_client.get_cluster_ceritificate()
            test_file = open("/tmp/server.crt",'w')
            test_file.write(temp_cert)
            test_file.close()
            self.certpath = '/tmp/server.crt'


    def tearDown(self):
        super(rbacCollectionTest, self).tearDown()
        self.user_cleanup()

    #Remove all users and groups after very test case run
    def user_cleanup(self):
        rest_client = RestConnection(self.master)
        self.user_delete(rest_client)

    def download_cert(self):
        tmp_path = "/tmp/server.pem"
        for servers in self.servers:
            cli_command = "ssl-manage"
            remote_client = RemoteMachineShellConnection(servers)
            options = "--regenerate-cert={0}".format(tmp_path)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options,
                                                                cluster_host=servers.ip, user="Administrator",
                                                                password="password")
        return tmp_path

    #Remove users - based on domain
    def user_delete_username(self,rest_client,domain=None,username=None):
        try:
            if domain == 'local':
                rest_client.delete_builtin_user(username)
            else:
                rest_client.delete_external_user(username)
        except:
            self.log.info ("User deletion failed, probably first time run")

    #Remove groups
    def group_delete_username(self,rest_client,username=None):
        try:
            rest_client.delete_group(username)
        except:
            self.log.info ("Group deletion failed, probably first time run")

    #Delete all groups and users in the system
    def user_delete(self,rest_client):
        status, content, header = rbacmain(self.master)._retrieve_user_roles()
        content = json.loads(content)
        for item in content:
            self.user_delete_username(rest_client,domain=item['domain'],username=item['id'])
        status, content = rest_client.get_group_list()
        for item in content:
            if 'ldap_group_ref' in item:
                self.log.info("ldap group")
            else:
                self.group_delete_username(rest_client,username=item['id'])

    #Create a single user
    def create_collection_read_write_user(self, user, bucket, scope, collection, role=None):
        if self.auth_type  == 'users':
            self.user_delete_username(RestConnection(self.master),'local',user)
        else:
            self.user_delete_username(RestConnection(self.master),'external',user)
        if collection is None:
            payload = "name=" + user + "&roles=" + role + "[" + bucket + ":" + scope
        elif collection is None and scope in None:
            payload = "name=" + user + "&roles=" + role + "[" + bucket
        else:
            payload = "name=" + user + "&roles=" + role + "[" + bucket + ":" + scope + ":" + collection

        if self.auth_type == 'extenalUser':
            payload = payload + "]"
            ExternalUser(user, payload, self.master).user_setup()
        else:
            payload = payload + "]&password=password"
            status, content, header =  rbacmain(self.master, "builtin")._set_user_roles(user_name=user, payload=payload)


    #Create user using a list, update user roles
    def create_collection_read_write_user_list(self, user_details, scope_scope, scope_collection,update=False):
        self.log.info ("User details are -= {0}".format(user_details))
        user_list = []
        user_list_role = []
        for item in user_details:
            if scope_collection is None and scope_scope is None:
                payload = item['role'] + "[" + item['bucket']  + "]"
            elif scope_collection is None:
                payload = item['role'] + "[" + item['bucket'] + ":" + item['scope']  + "]"
            else:
                payload = item['role'] + "[" + item['bucket'] + ":" + item['scope'] + ":" + item['collection'] + "]"

            if item['user'] in user_list:
                user_index = user_list.index(item['user'])
                final_role = (user_list_role[user_index])['role'] + ',' + payload
                user_list_role[user_index]['role'] = final_role
            else:
                user_list.append(item['user'])
                user_list_role.append({"user":item['user'],"role":payload})

        for item in user_list_role:
            if update:
                user, final_role = self.get_current_roles(item['user'])
                final_payload = "name=" + item['user'] + "&roles=" + item['role'] + "," + final_role
            else:
                final_payload = "name=" + item['user'] + "&roles=" + item['role']
            self.log.info (final_payload)
            if self.auth_type == 'users':
                final_payload = final_payload + "&password=password"
                self.user_delete_username(RestConnection(self.master), 'local', item['user'])
                status, content, header =  rbacmain(self.master, "builtin")._set_user_roles(user_name=item['user'], payload=final_payload)
            elif self.auth_type == 'InternalGroup':
                self.group_delete_username(RestConnection(self.master), item['user'])
                ldapGroupBase().add_role_group(item['user'], item['role'], None, self.master)
                if item['user'] == 'group1':
                    ldapGroupBase().create_grp_usr_internal(['user1'],self.master,roles=[''], groups=item['user'])
                elif item['user'] == 'group2':
                    ldapGroupBase().create_grp_usr_internal(['user2'],self.master,roles=[''], groups=item['user'])
            elif self.auth_type == 'externalUser':
                self.user_delete_username(RestConnection(self.master), 'external', item['user'])
                ExternalUser(item['user'], final_payload, self.master).user_setup()


    #Update roles for users
    def update_roles(self,user_details, scope_scope, scope_collection,update=False):
        user_list = []
        user_list_role = []
        for item in user_details:
            if scope_collection is None:
                payload = item['role'] + "[" + item['bucket'] + ":" + item['scope']  + "]"
            elif scope_collection is None and scope_scope in None:
                payload = item['role'] + "[" + item['bucket']  + "]"
            else:
                payload = item['role'] + "[" + item['bucket'] + ":" + item['scope'] + ":" + item['collection'] + "]"

            if item['user'] in user_list:
                user_index = user_list.index(item['user'])
                final_role = (user_list_role[user_index])['role'] + ',' + payload
                user_list_role[user_index]['role'] = final_role
            else:
                user_list.append(item['user'])
                user_list_role.append({"user":item['user'],"role":payload})

        for item in user_list_role:
            role_list = item['role'].split(',')
            for role_item in role_list:
                user, final_role = self.get_current_roles(item['user'])
                if ":*" in final_role:
                    final_role = final_role.replace(":*","")
                final_role = final_role + role_item
                final_payload = "name=" + item['user'] + "&roles=" + final_role + "&password=password"
                status, content, header =  rbacmain(self.master, "builtin")._set_user_roles(user_name=item['user'], payload=final_payload)
            user, final_role = self.get_current_roles(item['user'])

    #Get current roles
    def get_current_roles(self, user):
        status, content, header = rbacmain(self.master)._retrieve_user_roles()
        content = json.loads(content)
        final_role = ''
        for item in content:
            if item['id'] == user:
                for item_list in item['roles']:
                    final_role = item_list['role'] + "[" + item_list['bucket_name'] + ":" + item_list['scope_name'] + ":" + item_list['collection_name'] + "]" + "," + final_role
        return user, final_role

    #Create SDK connection
    def collectionConnection(self, scope, collection, bucket='default', username=None):
        self.log.info("Scope {0} Collection {1} username {2}".format(scope,collection,username))
        if self.certpath is None:
            scheme = "couchbase"
        else:
            scheme = "couchbases"
        host=self.master.ip
        if self.master.ip == "127.0.0.1":
            scheme = "http"
            host="{0}:{1}".format(self.master.ip, self.master.port)
        try:
            if username is None:
                client = SDKClient(scheme=scheme, hosts = [host], bucket = bucket, username=collection, password = 'password', certpath=self.certpath, sasl_mech=self.sasl_mech)
            else:
                client = SDKClient(scheme=scheme, hosts = [host], bucket = bucket, username=username, password = 'password', certpath= self.certpath, sasl_mech=self.sasl_mech)
            return client
        except Exception as e:
            self.log.info (e.result.err)

    #Input role details. Creates bucket, scope and collection.
    #Creates roles based on [bucket:*],[bucket:scope:*] bucket[bucket:scope:collection] based on access
    #Creates users and based on role - data_reader and data_writer validates the operation
    def check_permission_multiple_roles(self, role_details, access, update=False):
        self.log.info ("Roles details are --- {0}".format(role_details))
        for details in role_details:
            scope = details['scope']
            collection = details['collection']
            bucket = details['bucket']
            self.rest_client.delete_bucket(bucket)
            self.rest_client.create_bucket(bucket=bucket, ramQuotaMB=256)
            self.wait()
            self.rest.create_scope_collection(bucket=bucket, scope=scope, collection=collection)
        count = 1
        if (access['collection'] is True) and (access['scope'] is True):
            if update:
                self.update_roles(role_details, True,True,True)
            else:
                self.create_collection_read_write_user_list(role_details, True,True)
        elif (access['collection'] is False) and (access['scope'] is True):
            if update:
                self.update_roles(role_details, True,None,True)
            else:
                self.create_collection_read_write_user_list(role_details, True,None)
        elif (access['collection'] is False) and (access['scope'] is False):
            if update:
                self.update_roles(role_details, None,None,True)
            else:
                self.create_collection_read_write_user_list(role_details, None,None)

        for details in role_details:
            count = count + 1
            scope = details['scope']
            collection = details['collection']
            bucket = details['bucket']
            role = details['role']
            user = details['user']
            if user == 'group1':
                user = 'user1'
            elif user == 'group2':
                user = 'user2'

            try:
                if details['role'] == 'data_writer':
                    client = self.collectionConnection(scope, collection, bucket, user)
                    result = client.insert(str(count), "{1:2}",scope=scope,collection=collection)
                    errorResult = True if result.error == 0 else False
                    self.assertTrue(errorResult,'Issue with insertion')

                if details['role'] == 'data_reader':
                    client = self.collectionConnection(scope, collection, bucket, 'Administrator')
                    result = client.insert(str(count), "{2:2}",scope=scope,collection=collection)
                    client = self.collectionConnection(scope, collection, bucket, user)
                    result =  client.get(str(count), scope=scope, collection=collection)
                    self.log.info (result[2])
            except Exception as e:
                self.log.info (e.result.errstr)
                self.assertFalse(True,"Error message is -{0}".format(e.result.errstr))

    #Negative test case. Check with inverted scope and collections name
    #Validate error messages for incorrect scope and collections
    def check_permission_multiple_roles_negative(self, role_details, access, updateScope=True, updateCollection=True):
        self.log.info(" Scope value is -{0} and Collection value is {1}".format(updateScope,updateCollection))
        self.log.info ("Original Role details are -{0}".format(role_details))
        scopes = ['scope1','scope2']
        collections = ['collection1','collection2']
        roles = ['data_writer','data_reader']
        original_role_details = deepcopy(role_details)

        for details in role_details:
            scope = details['scope']
            collection = details['collection']
            bucket = details['bucket']
            self.rest_client.delete_bucket(bucket)
            self.rest_client.create_bucket(bucket=bucket, ramQuotaMB=256)
            self.wait()
            self.rest.create_scope_collection(bucket=bucket, scope=scope, collection=collection)

        count = 1
        for details in role_details:
            if updateScope == True:
                for scope_item in scopes:
                    if details['scope'] != scope_item:
                        details['scope'] = scope_item
                        break

            if updateCollection == True:
                for collections_items in collections:
                    if details['collection'] != collections_items:
                        details['collection'] = collections_items
                        break
            if updateScope == False and updateCollection == False:
                for roles_items in roles:
                    if details['role'] != roles_items:
                        details['role'] = roles_items
                        break

        self.log.info ("Updated Role details are -{0}".format(role_details))

        if (access['collection'] is True) and (access['scope'] is True):
            self.create_collection_read_write_user_list(original_role_details, True,True,True)
        elif (access['collection'] is False) and (access['scope'] is True):
            self.create_collection_read_write_user_list(original_role_details, True,True,None)
        elif (access['collection'] is False) and (access['scope'] is False):
            self.create_collection_read_write_user_list(original_role_details, True,None,None)

        for details in role_details:
            count = count + 1
            scope = details['scope']
            collection = details['collection']
            bucket = details['bucket']
            role = details['role']
            user = details['user']

            try:
                if details['role'] == 'data_writer':
                    client = self.collectionConnection(scope, collection, bucket, user)
                    result = client.insert(str(count), "{1:2}",scope=scope,collection=collection)

                if details['role'] == 'data_reader':
                    client = self.collectionConnection(scope, collection, bucket, 'Administrator')
                    result = client.insert(str(count), "{2:2}",scope=scope,collection=collection)
                    client = self.collectionConnection(scope, collection, bucket, user)
                    result =  client.get(str(count), scope=scope, collection=collection)
            except Exception as e:
                self.log.info (e.result.errstr)
                if (updateScope is True ):
                    errorResult = True if (e.result.errstr == 'LCB_ERR_SCOPE_NOT_FOUND (217)') else False
                if (updateCollection is True and updateScope is False) and (updateCollection is False and updateScope is False):
                    errorResult = True if (e.result.errstr == 'LCB_ERR_COLLECTION_NOT_FOUND (211)') else False
                self.assertTrue(errorResult,"Error")

    def check_incorrect_scope_collection(self):
        '''
        This test is for insertion to incorrect scope and collection.
        Validate error messages for scope and collection
        '''
        if not RestHelper(self.rest_client).bucket_exists("testbucket"):
                self.rest_client.create_bucket(bucket="testbucket", ramQuotaMB=256)
        self.rest.delete_collection("testbucket", "testscope", "testcollection")
        self.rest.delete_scope("testbucket", "testscope")
        self.wait()
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope", collection="testcollection")
        self.create_collection_read_write_user("testuser", "testbucket", "testscope", "testcollection", role='data_writer')

        try:
            client = self.collectionConnection("testscope1", "testcollection1", "testbucket", 'testuser')
            result = client.insert("testdoc", "{1:2}",scope="testscope1",collection="testcollection1")
        except Exception as e:
            self.log.info (e.result.errstr)
            errorResult = True if (e.result.errstr == 'LCB_ERR_SCOPE_NOT_FOUND (217)') else False
            self.assertTrue(errorResult,"Error")

        try:
            client = self.collectionConnection("testscope", "testcollection1", "testbucket", 'testuser')
            result = client.insert("testdoc", "{1:2}",scope="testscope",collection="testcollection1")
        except Exception as e:
            errorResult = True if (e.result.errstr == 'LCB_ERR_COLLECTION_NOT_FOUND (211)') else False
            self.assertTrue(errorResult,"Error")

        try:
            client = self.collectionConnection("testscope", "testcollection", "testbucket", 'testuser')
            result = client.insert("testdoc", "{1:2}",scope="testscope",collection="testcollection")
            errorResult = True if result.error == 0 else False
            self.assertTrue(errorResult,'Issue with insertion')
        except Exception as e:
            self.assertFalse(True, 'Issue with insertion with correct - {0}'.format(e.result.errstr))

    #Create list of permission with just 1 role attached
    def create_roles_collections_single(self):
        scopes = ['scope1','scope2']
        collections = ['collection1','collection2']
        roles = ['data_writer','data_reader']
        if self.auth_type == 'users' or self.auth_type == 'externalUser':
            users = ['user1','user2']
        elif self.auth_type == 'InternalGroup':
            users = ['group1','group2']
        buckets = ['default']
        final_roles = []
        access = {}
        final_return_roles=[]

        final_roles=[]
        for scope in scopes:
            for collection in collections:
                for user in users:
                    for role in roles:
                        for bucket in buckets:
                            final_role = {"scope":scope,"collection":collection,'user':user,'role':role,'bucket':bucket}
                            final_roles.append([final_role])
        final_return_roles.extend(final_roles)
        return final_return_roles

    #Create a list of roles with multiple roles and combination of users and roles
    def create_roles_collections_multiple(self):
        scopes = ['scope1','scope2']
        collections = ['collection1','collection2']
        roles = ['data_writer','data_reader']
        if self.auth_type == 'users' or self.auth_type == 'externalUser':
            users = ['user1','user2']
        elif self.auth_type == 'InternalGroup':
            users = ['group1','group2']
        buckets = ['default']
        final_roles = []
        access = {}
        final_return_roles=[]

        final_roles=[]
        for scope in scopes:
            for collection in collections:
                for user in users:
                    for role in roles:
                        for bucket in buckets:
                            final_role = {"scope":scope,"collection":collection,'user':user,'role':role,'bucket':bucket}
                            final_roles.append(final_role)


#         [{'scope': 'scope1', 'collection': 'collection1', 'user': 'user1', 'role': 'data_writer', 'bucket': 'default'},
#         {'scope': 'scope1', 'collection': 'collection1', 'user': 'user2', 'role': 'data_writer', 'bucket': 'default'}]
        final_list1=[]
        for collection in collections:
            for role in roles:
                for scope in scopes:
                    final_list = []
                    for user in users:
                        for final in final_roles:
                            if (role in final['role'] and collection in final['collection'] and scope in final['scope'] and user in final['user']):
                                final_list.append(final)
                    final_list1.append(final_list)
        final_return_roles.extend(final_list1)


#         [{'scope': 'scope1', 'collection': 'collection1', 'user': 'user1', 'role': 'data_writer', 'bucket': 'default'},
#          {'scope': 'scope1', 'collection': 'collection1', 'user': 'user1', 'role': 'data_reader', 'bucket': 'default'}]
        final_list1=[]
        for collection in collections:
            for user in users:
                for scope in scopes:
                    final_list = []
                    for role in roles:
                        for final in final_roles:
                            if (role in final['role'] and collection in final['collection'] and scope in final['scope'] and user in final['user']):
                                final_list.append(final)
                    final_list1.append(final_list)
        final_return_roles.extend(final_list1)

#         [{'scope': 'scope1', 'collection': 'collection2', 'user': 'user1', 'role': 'data_reader', 'bucket': 'default'},
#          {'scope': 'scope1', 'collection': 'collection2', 'user': 'user2', 'role': 'data_writer', 'bucket': 'default'}]
        final_list1=[]
        for user in users:
            final_list = []
            for role in roles:
                for final in final_roles:
                    if (role in final['role'] and user in final['user']):
                        final_list.append((final))
                    if (role not in final['role'] and user not in final['user']):
                        final_list.append((final))
            final_list1.append(final_list)
        final_list_return1 = []
        final_list_return2 = []
        final_return_list1 = []
        for list1 in final_list1:
            for i in range(0,len(list1),2):
                if (i % 2) == 0:
                    final_list_return1.append(list1[i])
            for i in range(1,len(list1),2):
                if (i % 2) != 0:
                    final_list_return2.append(list1[i])

        for i in range(0,len(final_list_return1)):
            final_return_list1.append([final_list_return1[i],final_list_return2[i]])

        final_return_roles.extend(final_return_list1[0:8])

        return(final_return_roles)

    #Defines the scope of the role - bucket, bucket:scope, bucket:scope:collection
    def getRoles(self,target):
        access = {}
        if target == 'collection':
            access = {'bucket':True, 'scope':True, 'collection':True}

        if target == 'scope':
            access = {'bucket':True, 'scope':True, 'collection':False}

        if target == 'bucket':
            access = {'bucket':True, 'scope':False, 'collection':False}

        return access

    #Function to create roles for multiple buckets
    def testDiffBucket(self):
        scopes = ['scope1','scope2']
        collections = ['collection1','collection2']
        roles = ['data_writer','data_reader']
        if self.auth_type == 'users' or self.auth_type == 'externalUser':
            users = ['user1','user2']
        else:
            users = ['group1','group2']
        buckets = ['first','second']
        final_roles = []
        access = {}
        final_return_roles=[]

        for scope in scopes:
            for collection in collections:
                for user in users:
                    for role in roles:
                        for bucket in buckets:
                            final_role = {"scope":scope,"collection":collection,'user':user,'role':role,'bucket':bucket}
                            final_roles.append(final_role)

        final_list1=[]
        for role in roles:
            for collection in collections:
                for role in roles:
                    for scope in scopes:
                        final_list = []
                        for bucket in buckets:
                            for final in final_roles:
                                if (role in final['role'] and bucket in final['bucket'] and scope in final['scope'] and user in final['user'] and collection in final['collection']):
                                    final_list.append(final)
                            final_list1.append(final_list)

        final_return_roles.extend(final_list1)

        return final_return_roles

    #Helper function to run doc ops in parallel and capture exceptions during writes
    def createBulkDocuments(self, client, scope, collection,start_num=0, end_num=10000, key1='demo_key'):
        value1 = {
          "name":"demo_value",
          "lastname":'lastname',
          "areapin":'',
          "preference":'veg',
          "type":''
        }
        for x in range (start_num, end_num):
            value = value1.copy()
            key = 'demo_key'
            key = key + str(x)
            for key1 in value:
                if value[key1] == 'type' and x % 2 == 0:
                    value['type'] = 'odd'
                else:
                    value['type'] = 'even'
                value[key1] = value[key1] + str(x)
            value['id'] = str(x)
            try:
                result = client.insert(key, value,scope=scope,collection=collection)
            except Exception as e:
                errorResult = True if (e.result.errstr == 'LCB_ERR_AUTHENTICATION_FAILURE (206)') else False
                self.assertTrue(errorResult,"Incorrect error message for connection")
                raise

    #Helper function for creating bucket, collection, scope and users
    def create_users_collections(self, bucket, number_collections, prefix='test'):
        final_list = []
        self.rest_client.create_bucket(bucket=bucket, ramQuotaMB=256)
        try:
            for i in range(0, number_collections):
                self.rest.create_scope_collection(bucket=bucket, scope=prefix + "scope" + str(i),
                                                  collection=prefix + "collection" + str(i))
                self.create_collection_read_write_user(prefix + "user" + str(i), bucket, prefix + "scope" + str(i),
                                                       prefix + "collection" + str(i), role="data_writer")
                self.wait()
                final_list.append(
                    {'bucket': bucket, 'scope': prefix + 'scope' + str(i), 'collection': prefix + 'collection' + str(i),
                     'user': prefix + 'user' + str(i)})
            return final_list
        except Exception as e:
            self.assertTrue(False, "Issue with creation of 1000 collection and users")

    #Test for testing single role assignment per user
    def test_check_single_role_collection_scope(self):
        roleSet = self.create_roles_collections_single()
        getScope = self.getRoles(self.test_scope)
        for role in roleSet:
            self.check_permission_multiple_roles(role,getScope)

    #Test for testing multiple role assignment per user
    def test_check_multiple_role_collection_scope(self):
        roleSet = self.create_roles_collections_multiple()
        getScope = self.getRoles(self.test_scope)
        for role in roleSet:
            self.check_permission_multiple_roles(role,getScope)

    #Test for multiple roles per and update the role one by one
    def test_check_multiple_role_collection_scope_update_roles(self):
        roleSet = self.create_roles_collections_multiple()
        getScope = self.getRoles(self.test_scope)
        for role in roleSet:
            self.check_permission_multiple_roles(role,getScope,True)

    #Test for mutliple roles for multiple buckets
    def test_check_multiple_buckets(self):
        roleSet = self.testDiffBucket()
        getScope = self.getRoles(self.test_scope)
        for role in roleSet:
            self.check_permission_multiple_roles(role,getScope)

    #Test for error messages when user tries to access scope and collections
    #the user does not have access to
    def test_incorrect_scope_collection(self):
        self.check_incorrect_scope_collection()

    #Defect - PYCBC-1014
    #Test for error message when user tries to access scope and collections
    #the user does not roles on. This test is for multiple roles on the same user
    def test_negative_scope_collection(self):
        roleSet = self.testDiffBucket()
        getScope = self.getRoles(self.test_scope)
        updateScope = self.input.param('updateScope',False)
        updateCollection = self.input.param('updateCollection',False)
        for role in roleSet:
            self.check_permission_multiple_roles_negative(role, getScope, updateScope, updateCollection)

    #Test that roles for uses are updated when collection and scope are deleted
    #User is not deleted after scope and collections are deleted
    def test_delete_collection_check_roles(self):
        self.rest_client.create_bucket(bucket="testbucket", ramQuotaMB=256)
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope", collection="testcollection")
        self.create_collection_read_write_user("testuser", "testbucket", "testscope", "testcollection", role="data_writer")
        self.rest.delete_collection("testbucket", "testscope", "testcollection")
        self.rest.delete_scope("testbucket", "testscope")
        user, final_role = self.get_current_roles('testuser')
        result = True if (final_role == '') else False
        self.assertTrue(result,'Role is not empty after deletion of collection ')
        result = True if (user == 'testuser') else False
        self.assertTrue(result,'user is deleted after deletion of collection')

    #Test uesr roles are cleaned up after collection is deleted
    def test_delete_recreated_collection_check_roles(self):
        self.rest_client.create_bucket(bucket="testbucket", ramQuotaMB=256)
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope", collection="testcollection")
        self.create_collection_read_write_user("testuser", "testbucket", "testscope", "testcollection", role="data_writer")
        self.rest.delete_collection("testbucket", "testscope", "testcollection")
        self.rest.delete_scope("testbucket", "testscope")
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope", collection="testcollection")
        self.create_collection_read_write_user("testuser", "testbucket", "testscope", "testcollection", role="data_writer")
        user, final_role = self.get_current_roles('testuser')
        result = True if (final_role == '') else False
        self.assertTrue(result,'Role is not empty after deletion of collection ')
        result = True if (user == 'testuser') else False
        self.assertTrue(result,'user is deleted after deletion of collection')


    #Check for errors when collections is deleted in parallel
    def test_collection_deletion_while_ops(self):
        self.rest_client.create_bucket(bucket="testbucket", ramQuotaMB=256)
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope", collection="testcollection")
        self.create_collection_read_write_user("testuser", "testbucket", "testscope", "testcollection", role="data_writer")
        client = self.collectionConnection("testscope", "testcollection", "testbucket", "testuser")
        self.wait()
        try:
            create_docs = Thread(name='create_docs', target=self.createBulkDocuments, args=(client, "testscope", "testcollection",))
            create_docs.start()
        except Exception as e:
            self.log.info (e)

        self.rest.delete_collection("testbucket", "testscope", "testcollection")
        self.rest.delete_scope("testbucket", "testscope")

        create_docs.join()

    #Create 1000 collections and users
    def create_hunderd_users_collections(self):
        self.rest_client.create_bucket(bucket="testbucket", ramQuotaMB=self.bucket_size)
        try:
            for i in range(0,1000):
                self.rest.create_scope_collection(bucket="testbucket", scope="testscope" + str(i), collection="testcollection"  + str(i))

                self.create_collection_read_write_user("testuser" + str(i), "testbucket" , "testscope" + str(i), "testcollection" + str(i), role="data_writer")
                self.wait()
                client = self.collectionConnection("testscope" + str(i), "testcollection" + str(i), "testbucket", "testuser" + str(i))
                result = client.insert("key", "{2:2}",scope="testscope" + str(i),collection="testcollection" + str(i))
        except Exception as e:
            self.assertTrue(False,"Issue with creation of 1000 collection and users")


    #Create collectioins and users during rebalance in
    def rebalance_in(self):
        try:
            final_items = self.create_users_collections('testbucket',10)
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                result = client.insert("key", "{2:2}", scope=item['scope'], collection=item['collection'])
            servs_inout = self.servers[self.nodes_init:]
            self.log.info("{0}".format(servs_inout))
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [])
            final_items = self.create_users_collections('testbucket1', 10,'test2')
            rebalance.result()
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                result = client.insert("key1", "{2:2}", scope=item['scope'], collection=item['collection'])
        except Exception as e:
            self.assertTrue(False,"Issue with creation of 1000 collection and users")

    #Create collections and users during rebalance out
    def rebalance_out(self):
        try:
            final_items = self.create_users_collections('testbucket',10)
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                result = client.insert("key", "{2:2}", scope=item['scope'], collection=item['collection'])
            servs_inout = self.servers[-1]
            self.log.info ("Value of server_in_out is {0}".format(servs_inout))
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],servs_inout)
            final_items = self.create_users_collections('testbucket1', 10,'test2')
            rebalance.result()
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                result = client.insert("key1", "{2:2}", scope=item['scope'], collection=item['collection'])
        except Exception as e:
            self.assertTrue(False,"Issue with creation of 1000 collection and users")

    #Create collectios and userss during failover
    def failover_out(self):
        try:
            final_items = self.create_users_collections('testbucket',1)
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                result = client.insert("key", "{2:2}", scope=item['scope'], collection=item['collection'])
            servs_inout = self.servers[-1]
            self.log.info ("Value fo server_in_out is {0}".format(servs_inout))
            fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[servs_inout], graceful=False)
            final_items = self.create_users_collections('testbucket1', 1,'test2')
            fail_over_task.result()
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                result = client.insert("key1", "{2:2}", scope=item['scope'], collection=item['collection'])
            self.rest_client.set_recovery_type('ns_1@' + servs_inout.ip, "full")
            self.rest_client.add_back_node('ns_1@' + servs_inout.ip)
            final_items = self.create_users_collections('testbucket3', 1, 'test3')
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                result = client.insert("key3", "{2:2}", scope=item['scope'], collection=item['collection'])
        except Exception as e:
            self.assertTrue(False,"Issue with creation of 1000 collection and users")

    #Delete users while data loading in parallel
    def test_user_deletion_while_ops(self):
        self.rest_client.create_bucket(bucket="testbucket", ramQuotaMB=256)
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope", collection="testcollection")
        self.create_collection_read_write_user("testuser", "testbucket", "testscope", "testcollection",
                                               role="data_writer")
        client = self.collectionConnection("testscope", "testcollection", "testbucket", "testuser")
        self.wait()
        try:
            create_docs = Thread(name='create_docs', target=self.createBulkDocuments,
                                 args=(client, "testscope", "testcollection",))
            create_docs.start()
        except Exception as e:
            self.log.info(e)

        self.rest_client.delete_user_roles("testuser")

        create_docs.join()

    #Delete user and check for error when insert happens
    def test_user_deletion_recreation(self):
        self.rest_client.create_bucket(bucket="testbucket", ramQuotaMB=256)
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope", collection="testcollection")
        self.create_collection_read_write_user("testuser", "testbucket", "testscope", "testcollection",
                                               role="data_writer")
        client = self.collectionConnection("testscope", "testcollection", "testbucket", "testuser")
        self.wait()
        result = client.insert("key1", "{2:2}", scope="testscope", collection="testcollection")
        self.rest_client.delete_user_roles("testuser")
        self.rest_client.add_set_builtin_user("testuser","name=testuser&roles=cluster_admin&password=password")
        try:
            result = client.insert("key1", "{2:2}", scope="testscope", collection="testcollection")
        except Exception as e:
            errorResult = True if (e.result.errstr == 'LCB_ERR_AUTHENTICATION_FAILURE (206)') else False
            self.assertTrue(errorResult, "Incorrect error message for connection")

    #Create users during rebalance and and other data loading is happening
    def rebalance_in_create_users(self):
        num_threads = []
        i = 1
        try:
            final_items = self.create_users_collections('testbucket',10)
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                result = client.insert("key", "{2:2}", scope=item['scope'], collection=item['collection'])
            servs_inout = self.servers[self.nodes_init:]
            print ("Value fo server_in_out is {0}".format(servs_inout))
            self.log.info("{0}".format(servs_inout))
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                create_docs = Thread(name='create_docs', target=self.createBulkDocuments,
                                     args=(client, item['scope'], item['collection'],i, i+ 1000,'demo_key'+item['collection'],))
                num_threads.append(create_docs)
                create_docs.start()
                i = (i * 2) + 1000
            for item in num_threads:
                item.join()

            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [])
            for i in range(1, 10):
                for j in range(1,20):
                    self.create_collection_read_write_user("testusernew" + str(j), "testbucket",
                                    scope="testscope" + str(i), collection= "testcollection"  + str(i),
                                                           role="data_writer")
            rebalance.result()
        except Exception as e:
            self.assertTrue(False, "Issue with creating users during rebalance - Error message {0}".format(e))

    #Delete users while rebalance in and data loading in parallel
    def rebalance_in_delete_users(self):
        num_threads = []
        i = 1
        try:
            final_items = self.create_users_collections('testbucket',10)
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                result = client.insert("key", "{2:2}", scope=item['scope'], collection=item['collection'])
            for i in range(1, 10):
                for j in range(1,20):
                    self.create_collection_read_write_user("testusernew" + str(j), "testbucket",
                                    scope="testscope" + str(i), collection= "testcollection"  + str(i),
                                                           role="data_writer")

            servs_inout = self.servers[self.nodes_init:]
            print ("Value fo server_in_out is {0}".format(servs_inout))
            self.log.info("{0}".format(servs_inout))
            for item in final_items:
                client = self.collectionConnection(item['scope'], item['collection'], item['bucket'],
                                                   item['user'])
                create_docs = Thread(name='create_docs', target=self.createBulkDocuments,
                                     args=(client, item['scope'], item['collection'],i, i+ 1000,'demo_key'+item['collection'],))
                num_threads.append(create_docs)
                create_docs.start()
                i = (i * 2) + 1000
            for item in num_threads:
                item.join()

            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [])
            for j in range(1,10):
                self.rest_client.delete_user_roles("testusernew" + str(j))
            rebalance.result()
        except Exception as e:
            self.assertTrue(False, "Issue with deleting users during rebalance - Error message {0}".format(e))

    #Remove users from group and check
    def add_remove_users_groups(self):
        self.rest_client.delete_bucket("testbucket")
        self.rest_client.create_bucket(bucket="testbucket", ramQuotaMB=256)
        self.wait()
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope", collection="testcollection")
        ldapGroupBase().add_role_group("testgroup", 'data_writer[testbucket:testscope:testcollection]', None, self.master)
        ldapGroupBase().create_grp_usr_internal(["user1"], self.master, roles=[''], groups="testgroup")
        ldapGroupBase().create_grp_usr_internal(["user2"], self.master, roles=[''], groups="testgroup")
        client = self.collectionConnection("testscope", "testcollection", "testbucket","user1")
        result = client.insert("user1key", "{2:2}", scope="testscope", collection="testcollection")
        client = self.collectionConnection("testscope", "testcollection", "testbucket", "user2")
        result = client.insert("user2key", "{2:2}", scope="testscope", collection="testcollection")
        self.rest_client.delete_user_roles("user2")
        self.wait()
        try:
            result = client.insert("user2key_error", "{2:2}", scope="testscope", collection="testcollection")
        except Exception as e:
            errorResult = True if (e.result.errstr == 'LCB_ERR_AUTHENTICATION_FAILURE (206)') else False
            self.assertTrue(errorResult, "User deleted but still able to insert data")

    # Remove groups from CB and check if user has access
    def add_remove_groups(self):
        self.rest_client.delete_bucket("testbucket")
        self.rest_client.create_bucket(bucket="testbucket", ramQuotaMB=256)
        self.wait()
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope", collection="testcollection")
        ldapGroupBase().add_role_group("testgroup", 'data_writer[testbucket:testscope:testcollection]', None, self.master)
        ldapGroupBase().add_role_group("testgroup1", 'data_writer[testbucket:testscope:testcollection]', None,
                                       self.master)
        ldapGroupBase().create_grp_usr_internal(["user1"], self.master, roles=[''], groups="testgroup")
        ldapGroupBase().create_grp_usr_internal(["user2"], self.master, roles=[''], groups="testgroup1")
        client = self.collectionConnection("testscope", "testcollection", "testbucket","user1")
        result = client.insert("user1key", "{2:2}", scope="testscope", collection="testcollection")
        client = self.collectionConnection("testscope", "testcollection", "testbucket", "user2")
        result = client.insert("user2key", "{2:2}", scope="testscope", collection="testcollection")
        self.rest_client.delete_group('testgroup1')
        self.wait()
        try:
            result = client.insert("user2key_error", "{2:2}", scope="testscope", collection="testcollection")
        except Exception as e:
            errorResult = True if (e.result.errstr == 'LCB_ERR_AUTHENTICATION_FAILURE (206)') else False
            self.assertTrue(errorResult, "Group deleted but still able to insert data")


    #Tests for scope admin
    def rest_execute(self, bucket, scope, collection, username, password, method='POST'):
        rest = RestConnection(self.master)
        api = "http://10.112.191.101:8091" + '/pools/default/buckets/%s/collections/%s' % (bucket, scope)
        body = {'name': collection}
        params = urllib.parse.urlencode(body)
        import base64
        authorization = base64.encodebytes(('%s:%s' % (username, password)).encode()).decode()
        header =  {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}
        self.wait()
        status, content, header = rest._http_request(api, 'POST', params=params, headers=header)
        return status, content, header


    def test_scope_admin_add_collection(self):
        self.rest_client.delete_bucket("testbucket")
        self.rest_client.create_bucket(bucket="testbucket", ramQuotaMB=256)
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope", collection="testcollection")
        self.rest.create_scope_collection(bucket="testbucket", scope="testscope1", collection="testcollection")
        self.create_collection_read_write_user('user_scope_admin', 'testbucket', 'testscope', None, role='scope_admin')
        self.wait()
        status, content, header = self.rest_execute('testbucket', 'testscope1', 'collection1','user_scope_admin','password')
        if status == True:
            self.assertTrue(False,'Scope admin can create collection for scope  it does not have access to')
        status, content, header = self.rest_execute('testbucket', 'testscope', 'collection1','user_scope_admin','password')
        if status == False:
            self.assertTrue(False,'Scope admin cannot create collection in scope it has access to')
        self.create_collection_read_write_user('user_scope_admin_full', 'testbucket', None, None, role='scope_admin')
        self.wait()
        status, content, header = self.rest_execute('testbucket', 'testscope', 'collection2','user_scope_admin_full','password')
        if status == False:
            self.assertTrue(False,'Scope admin can create collection for scope  it does not have access to')




    #Defect - PYCBC-1005
    import copy
    def test_x509(self):
        x509main(self.master)._generate_cert(self.servers, type='openssl', encryption="", key_length=1024, client_ip='172.16.1.174',wildcard_dns=None)
        x509main(self.master).setup_master("enable", ["subject.cn","san.dnsname","san.uri"], ['www.cb-','us.','www.'], [".",".","."], "rest")
        x509main().setup_cluster_nodes_ssl(self.servers)
        self.connection_sdk(self.master.ip)
        '''
        roleSet = self.checkCollections_x509()
        getScope = self.getRoles('scope')
        print (type(getScope))
        for role in roleSet:
            self.check_permission_multiple_roles(role,getScope)
        '''
