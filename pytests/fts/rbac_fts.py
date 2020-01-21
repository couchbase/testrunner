from .fts_base import FTSBaseTest, FTSIndex
from TestInput import TestInputSingleton
from security.rbac_base import RbacBase
from lib.membase.api.rest_client import RestConnection
import json
import httplib2

class RbacFTS(FTSBaseTest):

    def setUp(self):
        super(RbacFTS, self).setUp()
        users = TestInputSingleton.input.param("users", None)
        self.inp_users = []
        if users:
            self.inp_users = eval(eval(users))
        self.master = self._cb_cluster.get_master_node()
        self.users = self.get_user_list()
        self.roles = self.get_user_role_list()

    def tearDown(self):
        self.delete_role()
        super(RbacFTS, self).tearDown()

    def suite_setUp(self):
        self.log.info("==============   RbacFTS suite_setup has started ==============")
        self.log.info("==============   RbacFTS suite_setup has finished ==============")

    def suite_tearDown(self):
        self.log.info("==============   RbacFTS suite_tearDown has started ==============")
        self.log.info("==============   RbacFTS suite_tearDown has finished ==============")


    def create_users(self, users=None):
        """
        :param user: takes a list of {'id': 'xxx', 'name': 'some_name ,
                                        'password': 'passw0rd'}
        :return: Nothing
        """
        if not users:
            users = self.users
        RbacBase().create_user_source(users, 'builtin', self.master)
        self.log.info("SUCCESS: User(s) %s created"
                      % ','.join([user['name'] for user in users]))

    def assign_role(self, rest=None, roles=None):
        if not rest:
            rest = RestConnection(self.master)
        #Assign roles to users
        if not roles:
            roles = self.roles
        RbacBase().add_user_role(roles, rest, 'builtin')
        for user_role in roles:
            self.log.info("SUCCESS: Role(s) %s assigned to %s"
                          %(user_role['roles'], user_role['id']))

    def delete_role(self, rest=None, user_ids=None):
        if not rest:
            rest = RestConnection(self.master)
        if not user_ids:
            user_ids = [user['id'] for user in self.roles]
        RbacBase().remove_user_role(user_ids, rest)
        self.sleep(20, "wait for user to get deleted...")
        self.log.info("user roles revoked for %s" % ", ".join(user_ids))

    def get_user_list(self):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
        'password': 'passw0rd'}
        """
        user_list = []
        for user in self.inp_users:
            user_list.append({att: user[att] for att in ('id',
                                                         'name',
                                                         'password')})
        return user_list

    def get_user_role_list(self):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
         'roles': 'admin:fts_admin[default]'}
        """
        user_role_list = []
        for user in self.inp_users:
            user_role_list.append({att: user[att] for att in ('id',
                                                              'name',
                                                              'roles')})
        return user_role_list

    def get_rest_handle_for_credentials(self, user, password):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        rest.username = user
        rest.password = password
        return rest

    def create_index_with_no_credentials(self):
        server = self._cb_cluster.get_random_fts_node()
        self.load_sample_buckets(server=server, bucketName="travel-sample")

        api = "http://{0}:8094/api/index/travel?sourceType=couchbase&" \
              "indexType=fulltext-index&sourceName=travel-sample".\
            format(server.ip)
        response, content = httplib2.Http(timeout=120).request(api,
                                                               "PUT",
                                                               headers=None)
        try:
            content = content.decode()
        except AttributeError:
            pass
        self.log.info("Creating index returned : {0}".format(str(response)+ content))
        if response['status'] in ['200', '201', '202']:
            self.log.error(content)
            self.fail("FAIL: Index creation passed without credentials!")
        else:
            self.log.info("Index creation without credentials failed as expected!")

    def delete_index_with_no_credentials(self):
        server = self._cb_cluster.get_random_fts_node()
        self.load_sample_buckets(server=server, bucketName="travel-sample")
        index = FTSIndex(self._cb_cluster,
                         name="travel",
                         source_name="travel-sample")
        rest = self.get_rest_handle_for_credentials("Administrator", "password")
        index.create(rest)

        api = "http://{0}:8094/api/index/travel".format(server.ip)
        response, content = httplib2.Http(timeout=120).request(api,
                                                              "DELETE",
                                                              headers=None)
        try:
            content = content.decode()
        except AttributeError:
            pass
        self.log.info("Deleting index definition returned : {0}".format(str(response)+content))
        if response['status'] in ['200', '201', '202']:
            self.log.error(content)
            self.fail("FAIL: Index deletion passed without credentials!")
        else:
            self.log.info("Index deletion without credentials failed as expected!")

    def get_index_with_no_credentials(self):
        server = self._cb_cluster.get_random_fts_node()
        self.load_sample_buckets(server=server, bucketName="travel-sample")
        index = FTSIndex(self._cb_cluster,
                         name="travel",
                         source_name="travel-sample")
        rest = self.get_rest_handle_for_credentials("Administrator", "password")
        index.create(rest)

        api = "http://{0}:8094/api/index/travel".format(server.ip)
        response, content = httplib2.Http(timeout=120).request(api,
                                                               "GET",
                                                               headers=None)
        try:
            content = content.decode()
        except AttributeError:
            pass
        self.log.info("Getting index definition returned : {0}".format(str(response)+content))
        if response['status'] in ['200', '201', '202']:
            self.log.error(content)
            self.fail("FAIL: Get index definition passed without credentials!")
        else:
            self.log.info("Get index definition without credentials failed as expected!")

    def query_index_with_no_credentials(self):
        server = self._cb_cluster.get_random_fts_node()
        self.load_sample_buckets(server=server, bucketName="travel-sample")
        index = FTSIndex(self._cb_cluster,
                         name="travel",
                         source_name="travel-sample")
        rest = self.get_rest_handle_for_credentials("Administrator", "password")
        index.create(rest)
        self.wait_for_indexing_complete()

        api = "http://{0}:8094/api/index/travel/query?".format(server.ip)
        query = {"match": "Wick", "field": "city"}
        import urllib.request, urllib.parse, urllib.error
        api = api + urllib.parse.urlencode(query)
        response, content = httplib2.Http(timeout=120).request(api,
                                                               "POST",
                                                               headers=None)
        try:
            content = content.decode()
        except AttributeError:
            pass
        self.log.info("Querying returned : {0}".format(str(response) + content))
        if response['status'] in ['200', '201', '202']:
            self.log.error(content)
            self.fail("FAIL: Querying possible without credentials!")
        else:
            self.log.info("Querying without credentials failed as expected!")

    def create_index_with_credentials(self, username, password, index_name,
                                      bucket_name="default"):
        index = FTSIndex(self._cb_cluster, name=index_name,
                         source_name=bucket_name)
        rest = self.get_rest_handle_for_credentials(username, password)
        index.create(rest)
        return index

    def create_alias_with_credentials(self, username, password, alias_name,
                                      target_indexes):
        alias_def = {"targets": {}}
        for index in target_indexes:
            alias_def['targets'][index.name] = {}
            alias_def['targets'][index.name]['indexUUID'] = index.get_uuid()
        alias = FTSIndex(self._cb_cluster, name=alias_name,
                         index_type='fulltext-alias', index_params=alias_def)
        rest = self.get_rest_handle_for_credentials(username, password)
        alias.create(rest)
        return alias

    def edit_index_with_credentials(self, index, username, password):
        rest = self.get_rest_handle_for_credentials(username, password)
        _, defn = index.get_index_defn(rest)
        self.log.info("Old definition: %s" %defn['indexDef'])
        new_plan_param = {"maxPartitionsPerPIndex": 10}
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        index.update(rest)
        _, defn = index.get_index_defn()
        self.log.info("New definition: %s" %defn['indexDef'])

    def edit_index_different_source(self, index, new_source,
                                    username, password):
        rest = self.get_rest_handle_for_credentials(username, password)
        _, defn = index.get_index_defn(rest)
        self.log.info("Old definition: %s" % defn['indexDef'])
        index.index_definition['sourceName'] = new_source
        index.index_definition['uuid'] = index.get_uuid()
        index.update(rest)
        _, defn = index.get_index_defn()
        self.log.info("New definition: %s" % defn['indexDef'])


    def delete_index_with_credentials(self, index, username, password):
        rest = self.get_rest_handle_for_credentials(username, password)
        index.delete(rest)

    def query_index_with_credentials(self, index, username, password):
        rest = self.get_rest_handle_for_credentials(username, password)
        self.log.info("Now querying with credentials %s:%s" %(username,
                                                              password))
        hits, _, _, _ = rest.run_fts_query(index.name,
                                           {"query": self.sample_query})
        self.log.info("Hits: %s" %hits)

    def show_user_permissions(self):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        for user in self.users:
            for bucket in self._cb_cluster.get_buckets():
                perm =  RbacBase().check_user_permission(
                        user['id'],
                        user['password'],
                        'cluster.bucket[%s].fts!create,'
                        'cluster.bucket[%s].fts!manage,'
                        'cluster.bucket[%s].fts!read,'
                        'cluster.bucket[%s].fts!write'
                        %(bucket.name, bucket.name, bucket.name, bucket.name),
                        rest)
                self.log.info("Permissions for user: %s on bucket %s is: %s"
                              %(user['id'], bucket.name, perm))

    def test_fts_admin_permissions(self):
        """
        Tests if all created fts admin users are able to create and manage
        index and aliases
        :return: nothing
        """
        self.load_data()
        self.create_users()
        self.assign_role()
        self.show_user_permissions()

        for user in self.users:
            for bucket in self._cb_cluster.get_buckets():
                try:

                    index = self.create_index_with_credentials(
                        username= user['id'],
                        password=user['password'],
                        index_name="%s_%s_idx" %(user['id'], bucket.name),
                        bucket_name=bucket.name)

                    self.wait_for_indexing_complete()
                    alias = self.create_alias_with_credentials(
                        username= user['id'],
                        password=user['password'],
                        target_indexes=[index],
                        alias_name="%s_%s_alias" %(user['id'], bucket.name))
                    try:
                        self.edit_index_with_credentials(
                            index=index,
                            username=user['id'],
                            password=user['password'])
                        self.sleep(60, "Waiting for index rebuild after "
                                       "update...")
                        self.query_index_with_credentials(
                            index=index,
                            username=user['id'],
                            password=user['password'])
                        self.delete_index_with_credentials(
                            alias,
                            user['id'],
                            user['password'])
                        self.delete_index_with_credentials(
                            index=index,
                            username=user['id'],
                            password=user['password'])
                    except Exception as e:
                        self.fail("The user failed to edit/query/delete fts "
                                  "index %s : %s" % (user['id'], e))
                except Exception as e:
                        self.fail("The user failed to create fts index/alias"
                                  " %s : %s" % (user['id'], e))

    def test_grant_revoke_permissions(self):
        """
        Creates an fts admin, creates index with his credentials, revokes
        his role, limits his permissions to that of an 'fts_searcher'.
        Tries to see if he can now delete the index he created.
        Again updates his privileges to a user with no roles.
        Tests if he is able to get an index count using his new role.
        """
        self.load_data()
        self.create_users()
        self.assign_role()
        self.show_user_permissions()
        for user in self.users:
            for bucket in self._cb_cluster.get_buckets():
                index = self.create_index_with_credentials(
                    username= user['id'],
                    password=user['password'],
                    index_name="%s_%s_idx" %(user['id'], bucket.name),
                    bucket_name=bucket.name)
                self.delete_role(user_ids=[user['id']])
                # Temporary work around for deleting role now also deletes user
                #  in RbacBase()
                self.create_users(users=[{'id': user['id'],
                                          'name': user['name'],
                                          'password': user['password']}])
                self.assign_role(roles=[{'id': user['id'],
                                         'name': user['name'],
                                         'roles': 'fts_searcher[%s]'
                                                  %bucket.name}])
                try:
                    self.delete_index_with_credentials(
                        index=index,
                        username=user['id'],
                        password=user['password'])
                    self.fail("FAIL: User %s with revoked fts_admin privileges"
                              " is able to delete index" %(user['id']))
                except Exception as e:
                    self.log.info("Expected exception: %s" % e)
                self.delete_role(user_ids=[user['id']])
                # Temporary work around for deleting role now also deletes user
                #  in RbacBase()
                self.create_users(users=[{'id': user['id'],
                                          'name': user['name'],
                                          'password': user['password']}])
                self.assign_role(roles=[{'id': user['id'],
                                         'name': user['name'],
                                         'roles': 'replication_admin'}])
                try:
                    rest = self.get_rest_handle_for_credentials(
                        user=user['id'], password=user['password'])
                    self.log.info("Index count: %s"
                                  %index.get_indexed_doc_count(rest))
                    self.fail("FAIL: Able to get index count even after"
                              " fts_searcher role was revoked")
                except Exception as e:
                    self.log.info("Expected exception: %s" % e)

    def test_fts_searcher_permissions(self):
        """
        Tests if an 'fts_searcher' is able to create index, alias (negative test)
        and if he can get an index count and is able to run a query on a
        pre-existing index (positive case)
        """
        self.load_data()
        self.create_users()
        self.assign_role()
        self.show_user_permissions()

        for user in self.users:
            for bucket in self._cb_cluster.get_buckets():
                # creating an index
                try:

                    self.create_index_with_credentials(
                        username= user['id'],
                        password=user['password'],
                        index_name="%s_%s_idx" %(user['id'], bucket.name),
                        bucket_name=bucket.name)
                except Exception as e:
                    self.log.info("Expected exception: %s" %e)
                else:
                    self.fail("An fts_searcher is able to create index!")

                # creating an alias
                try:
                    self.log.info("Creating index as administrator...")
                    index = self.create_index_with_credentials(
                        username='Administrator',
                        password='password',
                        index_name="%s_%s_idx" % ('Admin', bucket.name),
                        bucket_name=bucket.name)
                    self.wait_for_indexing_complete()
                    self.log.info("Creating alias as fts_searcher...")
                    self.create_alias_with_credentials(
                        username=user['id'],
                        password=user['password'],
                        target_indexes=[index],
                        alias_name="%s_%s_alias" % (user['id'], bucket.name))
                except Exception as e:
                    self.log.info("Expected exception: %s" %e)
                else:
                    self.fail("An fts_searcher is able to create alias!")

                # editing an index
                try:
                    self.edit_index_with_credentials(index=index,
                                                     username=user['id'],
                                                     password=user['password'])
                except Exception as e:
                    self.log.info("Expected exception while updating index: %s"
                                  % e)
                    self.log.info("Index count: %s"
                                  %index.get_indexed_doc_count())
                    self.query_index_with_credentials(index=index,
                                                     username=user['id'],
                                                     password=user['password'])
                else:
                    self.fail("An fts searcher is able to edit index!")

                # deleting an index
                try:
                    self.delete_index_with_credentials(index=index,
                                                     username=user['id'],
                                                     password=user['password'])
                except Exception as e:
                    self.log.info("Expected exception: %s" % e)
                else:
                    self.fail("An fts searcher is able to delete index!")

    def test_fts_alias_creation_multiple_buckets(self):
        """
        A slightly complicated input based testcase that will require
        unnecessarily long manipulation if user info is not hardcoded
        This test creates 2 users - One(Will) is fts_admin of just one bucket,
        other(John) of two. We try to create an alias of the indexes using
        Will's credentials. Ideally this should fail. We then try to create an
        alias using John's credentials. This should pass.
        :return: Nothing
        """
        self.inp_users = [{"id": "willSmith",
                            "name": "William Smith",
                            "password": "password2",
                            "roles": "fts_admin[default]"},
                          {"id": "johnDoe",
                            "name": "Jonathan Downing",
                            "password": "password1",
                            "roles": "fts_admin[sasl_bucket_1]:"
                                     "fts_admin[default]"}]
        self.users = self.get_user_list()
        self.roles = self.get_user_role_list()
        self.load_data()
        self.create_users()
        self.assign_role()
        self.show_user_permissions()
        indexes = []

        # create indexes for those buckets for which the user is fts admin
        def_index = self.create_index_with_credentials(
            username='willSmith',
            password='password2',
            index_name='def_index',
            bucket_name='default')
        indexes.append(def_index)

        sasl_index = self.create_index_with_credentials(
            username='johnDoe',
            password='password1',
            index_name='sasl_index',
            bucket_name='sasl_bucket_1')
        indexes.append(sasl_index)

        # Try creating composite alias using credentials of a user who is
        #  fts_admin only for 1 bucket
        try:
            self.create_alias_with_credentials(username='willSmith',
                                               password='password2',
                                               target_indexes=indexes,
                                               alias_name="wills_alias")
        except Exception as e:
            self.log.info("Expected exception: %s" %e)

            # Now try the same with user having fts_admin permissions on
            # both buckets
            try:
                self.create_alias_with_credentials(username='johnDoe',
                                               password='password1',
                                               target_indexes=indexes,
                                               alias_name="johns_alias")
                self.log.info("SUCCESS: Alias successfully created")
            except Exception as e:
                self.fail("The user failed to create a composite alias on "
                          "indexes that he has permissions to: %s"%e)

    def test_alias_pointing_new_source(self):
        """
        This test creates 2 users - One(Will) is fts_admin of just one bucket,
        other(John) of two. We try to create an index on default bucket using
        Will's credentials. Then we try to point the index to a different
        bucket that Will does not have access to. This attempt should fail.
        Then we have Jonathan change the index source to sasl_bucket_1. And
        using Will's credentials, we try to get an index count, this should
        fail as permissions as based on source bucket.
        :return: Nothing
        """
        self.inp_users = [{"id": "willSmith",
                            "name": "William Smith",
                            "password": "password2",
                            "roles": "fts_admin[default]"},
                          {"id": "johnDoe",
                            "name": "Jonathan Downing",
                            "password": "password1",
                            "roles": "fts_admin[sasl_bucket_1]:"
                                     "fts_admin[default]"}]
        self.users = self.get_user_list()
        self.roles = self.get_user_role_list()
        self.load_data()
        self.create_users()
        self.assign_role()
        self.show_user_permissions()

        # create indexes for those buckets for which the user is fts admin
        def_index = self.create_index_with_credentials(
            username='willSmith',
            password='password2',
            index_name='def_index',
            bucket_name='default')
        try:
            self.edit_index_different_source(index = def_index,
                                         new_source="sasl_bucket_1",
                                         username='willSmith',
                                         password='password2')
            self.fail("User is able to point his index to bucket he does "
                      "not have access to!")
        except Exception as e:
            self.log.info("Expected exception: %s" %e)

        try:
            self.edit_index_different_source(index=def_index,
                                             new_source="sasl_bucket_1",
                                             username='johnDoe',
                                             password='password1')
            try:
                # Have Will get an index count on the index he created
                # but is now pointed to a different bucket that he has no
                # access to
                rest = self.get_rest_handle_for_credentials('willSmith',
                                                            'password1')
                index_count = def_index.get_indexed_doc_count(rest)
                self.fail("User with no permissions on source bucket is able to"
                          " get an index count on the index")
            except Exception as e:
                self.log.info("Expected exception: %s" %e)
        except Exception as e:
            self.fail("User with permissions is unable to change the "
                      "index source bucket %s" %e)

