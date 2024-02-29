from couchbase_helper.query_definitions import QueryDefinition
from gsi.serverless.base_gsi_serverless import BaseGSIServerless
from membase.api.serverless_rest_client import ServerlessRestConnection as RestConnection
from testconstants import INDEX_MAX_CAP_PER_TENANT
from serverless.gsi_utils import GSIUtils
import random
import string
from lib.capella.utils import ServerlessDataPlane
import time

class Pause_Resume_GSI_Serverless(BaseGSIServerless):
    def setUp(self):
        super(Pause_Resume_GSI_Serverless, self).setUp()
        self.MAX_INDEXES = self.input.param('MAX_INDEXES', False)
        self.pause_no_of_tenants = self.input.param('pause_no_of_tenants', 1)
        self.ddl = self.input.param('ddl', False)
        self.log.info("==============  TenantManagement serverless setup has started ==============")

    def tearDown(self):
        self.log.info("==============  TenantManagement serverless tearDown has started ==============")
        super(Pause_Resume_GSI_Serverless, self).tearDown()
        self.log.info("==============  TenantManagement serverless tearDown has completed ==============")

    def non_hibernated_bucket_validation(self):
        #Skip the first few buckets which have been hibernated and run some scans on non hibernted buckets
        for counter, database in enumerate(self.databases.values()):
            if counter < self.pause_no_of_tenants:
                continue
            for scope in database.collections:
                for collection in database.collections[scope]:
                    definition_list = self.gsi_util_obj.get_index_definition_list(dataset=self.dataset)
                    create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definition_list, namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    select_queries = self.gsi_util_obj.get_select_queries(definition_list=definition_list, namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definition_list, namespace=f"`{database.id}`.`{scope}`.`{collection}`")

                    self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=database,
                                                         capella_run=True)
                    self.log.info('Sleeping for 30s post indexes creation')
                    time.sleep(30)
                    for query in select_queries:
                        self.run_query(database=database, query=query)
                        time.sleep(2)
                    for query in drop_queries:
                        self.run_query(database=database, query=query)
                        time.sleep(2)

    def post_resume_validations(self, indexed_item_count_before_pause, indexed_item_count_after_resume):
        self.assertEqual(len(indexed_item_count_before_pause),len(indexed_item_count_after_resume),'No of indexes do not match post resume')
        for idx in indexed_item_count_before_pause:
            self.assertEqual(indexed_item_count_before_pause[idx], indexed_item_count_after_resume[idx],'Num items differs post resume')
        #Running queries and dropping existing indexes post resume
        for database in self.databases.values():
            for scope in database.collections:
                for collection in database.collections[scope]:
                    select_queries = self.gsi_util_obj.get_select_queries(definition_list=self.definition_list, namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=self.definition_list, namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    for query in select_queries:
                        self.run_query(database=database, query=query)
                        time.sleep(2)
                    for query in drop_queries:
                        self.run_query(database=database, query=query)
                        time.sleep(2)

        # Creating new indexes post, running some queries and dropping them post resume
        for database in self.databases.values():
            for scope in database.collections:
                for collection in database.collections[scope]:
                    create_queries = self.gsi_util_obj.get_create_index_list(definition_list=self.definition_list, namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    select_queries = self.gsi_util_obj.get_select_queries(definition_list=self.definition_list, namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=self.definition_list, namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=database,
                                                         capella_run=True)

                    self.log.info('Sleeping for 30s post indexes creation')
                    time.sleep(30)
                    for query in select_queries:
                        self.run_query(database=database, query=query)
                        time.sleep(2)
                    for query in drop_queries:
                        self.run_query(database=database, query=query)
                        time.sleep(2)

    def get_reqd_metadata_stats(self, rest_info):
        stats_required = ['items_count']
        indexer_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
        result = {}
        for node in indexer_nodes:
            stat_data = self.get_index_stats(node, rest_info=rest_info)
            if self.MAX_INDEXES:
                for stat, value in stat_data.items():
                    for req_stat in stats_required:
                        if f':{req_stat}' in stat:
                            result[stat.split(':')[1]] = value
            else:
                for stat, value in stat_data.items():
                    for req_stat in stats_required:
                        if f':{req_stat}' in stat:
                            result[stat.split(':')[3]] = value
        return result


    def test_basic_pause_resume(self):

        self.provision_databases(count=self.num_of_tenants, dataplane_id=self.dataplane_id)
        self.create_scopes_collections(databases=self.databases.values())
        if self.MAX_INDEXES:
            for database in self.databases.values():

                for scope in database.collections:
                    for collection in database.collections[scope]:
                        self.load_databases(database_obj=database, num_of_docs=self.total_doc_count, scope=scope,
                                            collection=collection, doc_template=self.dataset)
            for database in self.databases.values():

                for index_num in range(INDEX_MAX_CAP_PER_TENANT):
                    try:
                        self.create_index(database,
                                          query_statement=f"create index idx_db_{index_num + 1} on _default(b)",
                                          use_sdk=self.use_sdk)
                    except Exception as e:
                        self.fail(str(e))
                try:
                    num_of_indexes = self.get_count_of_indexes_for_tenant(database_obj=database)
                    self.create_index(database,
                                      query_statement=f"create index idx{INDEX_MAX_CAP_PER_TENANT} on _default(b)",
                                      use_sdk=self.use_sdk)
                    self.fail(
                        f"Index creation still works despite reaching limit of {INDEX_MAX_CAP_PER_TENANT}."
                        f" No of indexes already created{num_of_indexes}")
                except Exception as e:
                    self.log.info(
                        f"Error seen {str(e)} while trying to create index number {INDEX_MAX_CAP_PER_TENANT} as expected")
        else:
            for database in self.databases.values():

                for scope in database.collections:
                    for collection in database.collections[scope]:
                        self.load_databases(database_obj=database, num_of_docs=self.total_doc_count, scope=scope,
                                            collection=collection, doc_template=self.dataset)

                        create_list = self.gsi_util_obj.get_create_index_list(definition_list=self.definition_list,
                                                                              namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                        self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, database=database,
                                                             capella_run=True)
                        self.log.info(f"Iteration for {database.id} complete. Dataload and index creation done")
        self.log.info('Sleeping for 310s to clear ddl tokens')
        time.sleep(310)
        indexer_stats_before_pause = self.get_reqd_metadata_stats(rest_info=self.dp_obj)
        self.log.info(f'Indexer stats {indexer_stats_before_pause}')
        for database in self.databases.values():
            self.api.pause_operation(database_id=database.id,state='paused')
        self.log.info('Sleeping 30s post sucessful pause')
        time.sleep(30)
        for database in self.databases.values():
            self.api.resume_operation(database_id=database.id,state='healthy')
        self.log.info('Sleep for 30s post resume')
        time.sleep(30)
        indexer_stats_after_resume = self.get_reqd_metadata_stats(rest_info=self.dp_obj)
        self.log.info(f'Indexer logs post resume {indexer_stats_after_resume}')
        self.post_resume_validations(indexed_item_count_before_pause=indexer_stats_before_pause, indexed_item_count_after_resume=indexer_stats_after_resume)

    def test_pause_resume_multiple_tenants_across_cluster(self):

        self.provision_databases(count=self.num_of_tenants, dataplane_id=self.dataplane_id)
        self.create_scopes_collections(databases=self.databases.values())
        for database in self.databases.values():

            for scope in database.collections:
                for collection in database.collections[scope]:
                    self.load_databases(database_obj=database, num_of_docs=self.total_doc_count, scope=scope,
                                        collection=collection, doc_template=self.dataset)

                    create_list = self.gsi_util_obj.get_create_index_list(definition_list=self.definition_list,
                                                                          namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, database=database,
                                                         capella_run=True)
                    self.log.info(f"Iteration for {database.id} complete. Dataload and index creation done")
        indexer_stats_before_pause = self.get_reqd_metadata_stats(rest_info=self.dp_obj)
        self.log.info(f'Indexer stats {indexer_stats_before_pause}')
        self.log.info('Sleeping for 310s to clear ddl tokens')
        time.sleep(310)
        for counter, database in enumerate(self.databases.values()):
            if counter >= self.pause_no_of_tenants:
                break
            self.api.pause_operation(database_id=database.id, state='paused')
        self.log.info('Sleeping 30s post sucessful pause')
        time.sleep(30)
        self.non_hibernated_bucket_validation()
        for counter, database in enumerate(self.databases.values()):
            if counter >= self.pause_no_of_tenants:
                break
            self.api.resume_operation(database_id=database.id, state='healthy')
        self.log.info('Sleep for 30s post resume')
        time.sleep(30)
        indexer_stats_after_resume = self.get_reqd_metadata_stats(rest_info=self.dp_obj)
        self.log.info(f'Indexer logs post resume {indexer_stats_after_resume}')
        self.post_resume_validations(indexed_item_count_before_pause=indexer_stats_before_pause,
                                     indexed_item_count_after_resume=indexer_stats_after_resume)

    def test_ddl_and_scan_operations_during_pause(self):

        self.provision_databases(count=self.num_of_tenants, dataplane_id=self.dataplane_id)
        self.create_scopes_collections(databases=self.databases.values())
        select_queries = []
        create_list_ddl = []
        definition_list = []
        for database in self.databases.values():

            for scope in database.collections:
                for collection in database.collections[scope]:
                    self.load_databases(database_obj=database, num_of_docs=self.total_doc_count, scope=scope,
                                        collection=collection, doc_template=self.dataset)
                    definition_list = self.gsi_util_obj.get_index_definition_list(dataset=self.dataset)
                    create_list_ddl = self.gsi_util_obj.get_create_index_list(definition_list=definition_list,
                                                                          namespace=f"`{database.id}`.`{scope}`.`{collection}`")

                    create_list = self.gsi_util_obj.get_create_index_list(definition_list=self.definition_list,
                                                                          namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    select_queries = self.gsi_util_obj.get_select_queries(definition_list=self.definition_list,
                                                                          namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, database=database,
                                                         capella_run=True)
                    self.log.info(f"Iteration for {database.id} complete. Dataload and index creation done")
        indexer_stats_before_pause = self.get_reqd_metadata_stats(rest_info=self.dp_obj)
        self.log.info(f'Indexer stats {indexer_stats_before_pause}')
        self.log.info('Sleeping for 310s to clear ddl tokens')
        time.sleep(310)
        for database in self.databases.values():
            self.api.pause_operation(database_id=database.id, state='paused', pause_complete=False)
        try:
            if self.ddl:
                for database in self.databases.values():
                    for query in create_list_ddl:
                        self.run_query(database=database, query=query)
            else:
                for database in self.databases.values():
                    for query in select_queries:
                        self.run_query(database=database, query=query)
        except Exception as e:
            self.log.info(f'Error msg recieved {str(e)}')
            self.assertIn('401 Client Error: Unauthorized for url:', str(e), f'Got error msg {str(e)} instead of: 401 Client Error: Unauthorized for url')
        else:
            self.fail('Exception did not occur as expected')

    def test_ddl_and_scan_operations_during_resume(self):

        self.provision_databases(count=self.num_of_tenants, dataplane_id=self.dataplane_id)
        self.create_scopes_collections(databases=self.databases.values())
        select_queries = []
        for database in self.databases.values():

            for scope in database.collections:
                for collection in database.collections[scope]:
                    self.load_databases(database_obj=database, num_of_docs=self.total_doc_count, scope=scope,
                                        collection=collection, doc_template=self.dataset)
                    definition_list = self.gsi_util_obj.get_index_definition_list(dataset=self.dataset)
                    create_list_ddl = self.gsi_util_obj.get_create_index_list(definition_list=definition_list,
                                                                              namespace=f"`{database.id}`.`{scope}`.`{collection}`")

                    create_list = self.gsi_util_obj.get_create_index_list(definition_list=self.definition_list,
                                                                          namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    select_queries = self.gsi_util_obj.get_select_queries(definition_list=self.definition_list,
                                                                          namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                    self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, database=database,
                                                         capella_run=True)
                    self.log.info(f"Iteration for {database.id} complete. Dataload and index creation done")
        indexer_stats_before_pause = self.get_reqd_metadata_stats(rest_info=self.dp_obj)
        self.log.info(f'Indexer stats {indexer_stats_before_pause}')
        self.log.info('Sleeping for 310s to clear ddl tokens')
        time.sleep(310)
        for database in self.databases.values():
            self.api.pause_operation(database_id=database.id, state='paused', pause_complete=True)
        for database in self.databases.values():
            self.api.resume_operation(database_id=database.id, state='healthy', resume_complete=False)
        try:
            if self.ddl:
                for database in self.databases.values():
                    for query in create_list_ddl:
                        self.run_query(database=database, query=query)
            else:
                for database in self.databases.values():
                    for query in select_queries:
                        self.run_query(database=database, query=query)
        except Exception as e:
            self.log.info(f'Error msg recieved {str(e)}')
            self.assertIn('401 Client Error: Unauthorized for url:', str(e),
                          f'Got error msg {str(e)} instead of: 401 Client Error: Unauthorized for url')
        else:
            self.fail('Exception did not occur as expected')
