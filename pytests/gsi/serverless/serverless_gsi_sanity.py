"""tenant_management.py: "This class test cluster affinity  for GSI"

__author__ = "Pavan PB"
__maintainer = Pavan PB"
__email__ = "pavan.pb@couchbase.com"
__git_user__ = "pavan-couchbase"
"""
import random
import time

from gsi.serverless.base_gsi_serverless import BaseGSIServerless
from membase.api.serverless_rest_client import ServerlessRestConnection as RestConnection
from couchbase_helper.query_definitions import QueryDefinition

REPLICA_INDEX_ONLINE_CHECK = False

class ServerlessGSISanity(BaseGSIServerless):
    def setUp(self):
        super(ServerlessGSISanity, self).setUp()
        self.log.info("==============  ServerlessGSISanity  setup has started ==============")
        self.provision_databases(count=self.num_of_tenants)

    def tearDown(self):
        self.log.info("==============  ServerlessGSISanity  tearDown has started ==============")
        super(ServerlessGSISanity, self).tearDown()
        self.log.info("==============  ServerlessGSISanity  tearDown has completed ==============")

    def suite_tearDown(self):
        pass

    def suite_setUp(self):
        pass

    def test_create_primary_index(self):
        """
        tests creation/drop of primary and named primary indexes. Also tests build/defer functionality
        """
        for named_primary_index in [True, False]:
            for counter, database in enumerate(self.databases.values()):
                self.cleanup_database(database_obj=database)
                scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
                collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
                if named_primary_index:
                    index_name = f'`#named_{random.randint(0, 1000)}_db_{counter}`'
                else:
                    index_name = f'`#primary`'
                query_gen = QueryDefinition(index_name=index_name)
                self.create_scope(database_obj=database, scope_name=scope_name)
                self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
                namespace = f"default:`{database.id}`.`{scope_name}`.`{collection_name}`"
                keyspace = f"`{database.id}`.`{scope_name}`.`{collection_name}`"
                self.load_databases(load_all_databases=False, num_of_docs=self.num_of_docs_per_collection,
                                    database_obj=database, scope=scope_name, collection=collection_name)
                query = query_gen.generate_primary_index_create_query(defer_build=self.defer_build, namespace=namespace)
                self.run_query(database=database, query=query)
                if self.defer_build:
                    build_query = query_gen.generate_build_query(namespace=namespace)
                    self.run_query(database=database, query=build_query)
                self.rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
                nodes_obj = self.rest_obj.get_all_dataplane_nodes()
                self.log.debug(f"Dataplane nodes object {nodes_obj}")
                for node in nodes_obj:
                    if 'index' in node['services']:
                        self.wait_until_indexes_online(database=database, index_name=index_name,
                                                       indexer_node=node['hostname'].split(":")[0],
                                                       keyspace=keyspace, replica_check=REPLICA_INDEX_ONLINE_CHECK)
                        break
                count_query = f'SELECT COUNT(*) from {namespace}'
                resp = self.run_query(database=database, query=count_query)['results']
                count = resp[0]['$1']
                self.assertEqual(count, self.num_of_docs_per_collection, "Docs count mismatch")
                drop_query = query_gen.generate_index_drop_query(namespace=keyspace)
                self.run_query(database=database, query=drop_query)
                if self.check_if_index_exists(database_obj=database, index_name=index_name):
                    self.fail(f"Index {index_name} not dropped on {database.id}")

    def test_create_secondary_index(self):
        """
        tests creation/drop of secondary indexes. Perform CRUD operations and check that the count of
        indexed items gets updated.
        Also tests build/defer functionality
        """
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            index_name = f'gsi_index_db_{counter}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            namespace = f"default:`{database.id}`.`{scope_name}`.`{collection_name}`"
            keyspace = f"`{database.id}`.`{scope_name}`.`{collection_name}`"
            self.load_databases(load_all_databases=False, num_of_docs=self.num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            query_gen = QueryDefinition(index_name=index_name, index_fields=['age'], keyspace=keyspace)
            # create indexes
            query = query_gen.generate_index_create_query(namespace=keyspace, defer_build=self.defer_build)
            self.run_query(database=database, query=query)
            if self.defer_build:
                build_query = query_gen.generate_build_query(namespace=namespace)
                self.run_query(database=database, query=build_query)
            self.rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
            nodes_obj = self.rest_obj.get_all_dataplane_nodes()
            self.log.debug(f"Dataplane nodes object {nodes_obj}")
            for node in nodes_obj:
                if 'index' in node['services']:
                    self.wait_until_indexes_online(database=database, index_name=index_name,
                                                   indexer_node=node['hostname'].split(":")[0],
                                                   keyspace=keyspace, replica_check=REPLICA_INDEX_ONLINE_CHECK)
                    break
            # insert docs
            select_query = f'SELECT meta().id from {keyspace} where age > 30 and age < 60'
            result = self.run_query(database=database, query=select_query)['results']
            doc_count_start = len(result)
            self.assertNotEqual(doc_count_start, 0, f'Actual : {result}')
            value = {
                "city": "Test Dee",
                "country": "Test Verde",
                "firstName": "Test name",
                "lastName": "Test Funk",
                "streetAddress": "66877 Williamson Terrace",
                "suffix": "V",
                "title": "International Solutions Coordinator"
            }
            exptime = 0
            count_docs_to_insert, count_docs_to_delete = 20, 10
            for key_id in range(count_docs_to_insert):
                doc_id = f'new_doc_{key_id}'
                doc_body = value
                doc_body['age'] = random.randint(31, 59)
                insert_query = f"INSERT into {keyspace} (KEY, VALUE) VALUES('{doc_id}', {doc_body}," \
                               f" {{'expiration': {exptime}}}) "
                self.run_query(database=database, query=insert_query)
            select_query = f'SELECT meta().id from {keyspace} where age > 30 and age < 60'
            result = self.run_query(database=database, query=select_query)['results']
            doc_count_after_insert = len(result)
            expected_doc_count_after_insert = doc_count_start + count_docs_to_insert
            if doc_count_after_insert != expected_doc_count_after_insert:
                self.fail(f"Doc count mismatch after insert. Actual count {doc_count_after_insert}. "
                          f"Expected count {expected_doc_count_after_insert}")
            # delete docs
            query = f'SELECT meta().id FROM {keyspace} where age=35'
            doc_ids = self.run_query(database=database, query=query)['results']
            count_docs_to_delete = len(doc_ids)
            # doc_ids = ", ".join([f'"{item}"' for item in docs_to_delete])
            delete_query = f'DELETE FROM {keyspace} WHERE age=35'
            deleted_doc_ids = self.run_query(database=database, query=delete_query)
            expected_doc_count_after_delete = doc_count_after_insert - count_docs_to_delete
            result = self.run_query(database=database, query=select_query)['results']
            doc_count_after_delete = len(result)
            if doc_count_after_delete != expected_doc_count_after_delete:
                self.fail(f"Doc count mismatch after delete. Actual count {doc_count_after_delete}. "
                          f"Expected count {expected_doc_count_after_delete}")
            expected_doc_count_before_update = len(self.run_query(database=database, query=select_query)['results'])
            update_query = f'UPDATE {namespace} SET updated = true WHERE age > 30 and age < 60'
            self.run_query(database=database, query=update_query)
            query = f'SELECT meta().id FROM {keyspace} WHERE age > 30 and age < 60'
            queried_docs = self.run_query(database=database, query=query)['results']
            queried_doc_ids = sorted([item['id'] for item in queried_docs])
            query = f'SELECT meta().id FROM {namespace} WHERE updated = true'
            updated_docs_ids = self.run_query(database=database, query=query)['results']
            updated_docs_ids = sorted([item['id'] for item in updated_docs_ids])
            self.assertEqual(queried_doc_ids, updated_docs_ids,
                             f"Actual: {queried_doc_ids}, Expected: {updated_docs_ids}")
            doc_count_after_update = len(self.run_query(database=database, query=select_query)['results'])
            self.assertEqual(doc_count_after_update, expected_doc_count_before_update,
                             f"Actual: {doc_count_after_update}, Expected: {expected_doc_count_before_update}")

            # upsert docs
            upsert_count_query = f'SELECT meta().id FROM {namespace} WHERE age > 90'
            docs_before_upsert = self.run_query(database=database, query=upsert_count_query)['results']
            count_before_upsert = len(docs_before_upsert)
            upsert_doc_list = ['upsert-1', 'upsert-2']
            upsert_query = f'UPSERT INTO {namespace} (KEY, VALUE) VALUES ' \
                           f'("upsert-1", {{ "firstName": "Michael", "age": 92}}),' \
                           f'("upsert-2", {{"firstName": "George", "age": 95}})' \
                           f' RETURNING VALUE name'
            self.run_query(database=database, query=upsert_query)
            upsert_doc_ids = self.run_query(database=database, query=upsert_count_query)['results']
            upsert_doc_ids = sorted([item['id'] for item in upsert_doc_ids])
            self.assertEqual(upsert_doc_ids, upsert_doc_list,
                             f"Actual: {upsert_doc_ids}, Expected: {upsert_doc_list}")
            expected_count_after_upsert = count_before_upsert + len(upsert_doc_list)
            docs_after_upsert = self.run_query(database=database, query=upsert_count_query)['results']
            self.assertEqual(len(docs_after_upsert), expected_count_after_upsert,
                             f"Actual: {len(docs_after_upsert)}, Expected: {expected_count_after_upsert}")

            drop_query = query_gen.generate_index_drop_query(namespace=keyspace)
            self.run_query(database=database, query=drop_query)
            if self.check_if_index_exists(database_obj=database, index_name=index_name):
                self.fail(f"Index {index_name} not dropped on {database.id}")

    def test_create_missing_key_index(self):
        """
        tests creation/drop of missing key indexes. Perform CRUD operations and check that the count of
        indexed items gets updated.
        Also tests build/defer functionality
        """
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            index_name = f'gsi_index_db_{counter}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            namespace = f"default:`{database.id}`.`{scope_name}`.`{collection_name}`"
            keyspace = f"`{database.id}`.`{scope_name}`.`{collection_name}`"
            self.load_databases(load_all_databases=False, num_of_docs=self.num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            query_gen = QueryDefinition(index_name=index_name, index_fields=['age', 'city', 'country'],
                                        keyspace=keyspace)
            # create indexes
            query = query_gen.generate_index_create_query(namespace=keyspace, defer_build=self.defer_build,
                                                          missing_indexes=True,
                                                          missing_field_desc=random.choice([True, False]))
            self.run_query(database=database, query=query)
            if self.defer_build:
                build_query = query_gen.generate_build_query(namespace=namespace)
                self.run_query(database=database, query=build_query)
            self.rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
            nodes_obj = self.rest_obj.get_all_dataplane_nodes()
            self.log.debug(f"Dataplane nodes object {nodes_obj}")
            for node in nodes_obj:
                if 'index' in node['services']:
                    self.wait_until_indexes_online(database=database, index_name=index_name,
                                                   indexer_node=node['hostname'].split(":")[0],
                                                   keyspace=keyspace, replica_check=REPLICA_INDEX_ONLINE_CHECK)
                    break
            # insert docs
            select_query = f'SELECT meta().id from {keyspace} where city like "C%"'
            result = self.run_query(database=database, query=select_query)['results']
            doc_count_start = len(result)
            self.assertNotEqual(doc_count_start, 0, f'Actual : {result}')
            value = {
                "city": "CityNest Dee",
                "country": "Test Verde",
                "firstName": "Test name",
                "lastName": "Test Funk",
                "streetAddress": "66877 Williamson Terrace",
                "suffix": "V",
                "title": "International Solutions Coordinator"
            }
            exptime = 0
            count_docs_to_insert = 1000
            for key_id in range(count_docs_to_insert):
                doc_id = f'new_doc_{key_id}'
                doc_body = value
                doc_body['age'] = None
                insert_query = f"INSERT into {keyspace} (KEY, VALUE) VALUES('{doc_id}', {doc_body}," \
                               f" {{'expiration': {exptime}}}) "
                self.run_query(database=database, query=insert_query)
            select_query = f'SELECT meta().id from {keyspace} where city like "C%"'
            result = self.run_query(database=database, query=select_query)['results']
            doc_count_after_insert = len(result)
            expected_doc_count_after_insert = doc_count_start + count_docs_to_insert
            if doc_count_after_insert != expected_doc_count_after_insert:
                self.fail(f"Doc count mismatch after insert. Actual count {doc_count_after_insert}. "
                          f"Expected count {expected_doc_count_after_insert}")
            # delete docs
            query = f'SELECT meta().id FROM {keyspace} where city like "A%"'
            doc_ids = self.run_query(database=database, query=query)['results']
            count_docs_to_delete = len(doc_ids)
            delete_query = f'DELETE FROM {keyspace} WHERE city like "A%"'
            self.run_query(database=database, query=delete_query)
            expected_doc_count_after_delete = doc_count_after_insert - count_docs_to_delete
            result = self.run_query(database=database, query=select_query)['results']
            doc_count_after_delete = len(result)
            if doc_count_after_delete != expected_doc_count_after_delete:
                self.fail(f"Doc count mismatch after delete. Actual count {doc_count_after_delete}. "
                          f"Expected count {expected_doc_count_after_delete}")
            expected_doc_count_before_update = len(self.run_query(database=database, query=select_query)['results'])
            update_query = f'UPDATE {namespace} SET updated = true WHERE country like "A%"'
            self.run_query(database=database, query=update_query)
            query = f'SELECT meta().id FROM {keyspace} WHERE country like "A%"'
            queried_docs = self.run_query(database=database, query=query)['results']
            queried_doc_ids = sorted([item['id'] for item in queried_docs])
            query = f'SELECT meta().id FROM {namespace} WHERE updated = true'
            updated_docs_ids = self.run_query(database=database, query=query)['results']
            updated_docs_ids = sorted([item['id'] for item in updated_docs_ids])
            self.assertEqual(queried_doc_ids, updated_docs_ids,
                             f"Actual: {queried_doc_ids}, Expected: {updated_docs_ids}")
            doc_count_after_update = len(self.run_query(database=database, query=select_query)['results'])
            self.assertEqual(doc_count_after_update, expected_doc_count_before_update,
                             f"Actual: {doc_count_after_update}, Expected: {expected_doc_count_before_update}")

            # upsert docs
            upsert_count_query = f'SELECT meta().id FROM {namespace} WHERE country like Z%'
            docs_before_upsert = self.run_query(database=database, query=upsert_count_query)['results']
            count_before_upsert = len(docs_before_upsert)
            upsert_doc_list = ['upsert-1', 'upsert-2']
            upsert_query = f'UPSERT INTO {namespace} (KEY, VALUE) VALUES ' \
                           f'("upsert-1", {{ "firstName": "Michael", "country": "Zimbabwe"}}),' \
                           f'("upsert-2", {{"firstName": "George", "country": "Zambia"}})' \
                           f' RETURNING VALUE name'
            self.run_query(database=database, query=upsert_query)
            upsert_doc_ids = self.run_query(database=database, query=upsert_count_query)['results']
            upsert_doc_ids = sorted([item['id'] for item in upsert_doc_ids])
            self.assertEqual(upsert_doc_ids, upsert_doc_list,
                             f"Actual: {upsert_doc_ids}, Expected: {upsert_doc_list}")
            expected_count_after_upsert = count_before_upsert + len(upsert_doc_list)
            docs_after_upsert = self.run_query(database=database, query=upsert_count_query)['results']
            self.assertEqual(len(docs_after_upsert), expected_count_after_upsert,
                             f"Actual: {len(docs_after_upsert)}, Expected: {expected_count_after_upsert}")

            drop_query = query_gen.generate_index_drop_query(namespace=keyspace)
            self.run_query(database=database, query=drop_query)
            if self.check_if_index_exists(database_obj=database, index_name=index_name):
                self.fail(f"Index {index_name} not dropped on {database.id}")

    def test_disallow_alter_index(self):
        """
        Alter index should be disallowed ( Move, replica_count, Drop)
        """
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            index_name = f'gsi_index_db_{counter}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            namespace = f"default:`{database.id}`.`{scope_name}`.`{collection_name}`"
            keyspace = f"`{database.id}`.`{scope_name}`.`{collection_name}`"
            self.load_databases(load_all_databases=False, num_of_docs=self.num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            query_gen = QueryDefinition(index_name=index_name, index_fields=['age', 'city', 'country'],
                                        keyspace=keyspace)
            # create index
            query = query_gen.generate_index_create_query(namespace=keyspace, defer_build=self.defer_build)
            self.run_query(database=database, query=query)
            if self.defer_build:
                build_query = query_gen.generate_build_query(namespace=namespace)
                self.run_query(database=database, query=build_query)
            self.rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
            nodes_obj = self.rest_obj.get_all_dataplane_nodes()
            self.log.debug(f"Dataplane nodes object {nodes_obj}")
            index_node_list = []
            for node in nodes_obj:
                if 'index' in node['services']:
                    index_node_list.append(node['hostname'].split(":")[0])
            # alter index drop
            alter_drop_index_query = f'ALTER INDEX {index_name} ON {keyspace} with ' \
                                     f'{"action": "drop_replica", "replicaId":1}'
            try:
                self.run_query(database=database, query=alter_drop_index_query)
                self.fail(f"{alter_drop_index_query} works. Test failure")
            except:
                self.log.info(f"{alter_drop_index_query} did not work as expected")
            # alter index increase replica count
            replica_count = len(index_node_list)
            alter_replica_count_query = f'ALTER INDEX {index_name} ON {keyspace} with ' \
                                        f'{"action": "replica_count", "num_replica": {replica_count}}'
            try:
                self.run_query(database=database, query=alter_replica_count_query)
                self.fail(f"{alter_replica_count_query} works. Test failure")
            except:
                self.log.info(f"{alter_replica_count_query} did not work as expected")
            # alter index move
            alter_move_query = f'ALTER INDEX {index_name} ON {keyspace} with ' \
                               f'{"action": "move", "nodes": {index_node_list[1]}}'
            try:
                self.run_query(database=database, query=alter_move_query)
                self.fail(f"{alter_move_query} works. Test failure")
            except:
                self.log.info(f"{alter_move_query} did not work as expected")