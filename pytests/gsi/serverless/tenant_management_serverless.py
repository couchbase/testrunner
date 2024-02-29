"""tenant_management.py: "This class test cluster affinity  for GSI"

__author__ = "Pavan PB"
__maintainer = Pavan PB"
__email__ = "pavan.pb@couchbase.com"
__git_user__ = "pavan-couchbase"
"""
import random
import time
import math
import requests

from gsi.serverless.base_gsi_serverless import BaseGSIServerless
from membase.api.serverless_rest_client import ServerlessRestConnection as RestConnection
from testconstants import INDEX_MAX_CAP_PER_TENANT
from couchbase_helper.query_definitions import QueryDefinition
from lib.capella.utils import ServerlessDataPlane
from concurrent.futures import ThreadPoolExecutor
from threading import Event


class TenantManagement(BaseGSIServerless):
    def setUp(self):
        super(TenantManagement, self).setUp()
        self.log.info("==============  TenantManagement serverless setup has started ==============")

    def tearDown(self):
        self.log.info("==============  TenantManagement serverless tearDown has started ==============")
        super(TenantManagement, self).tearDown()
        self.log.info("==============  TenantManagement serverless tearDown has completed ==============")

    def test_cluster_affinity(self):
        self.provision_databases(count=self.num_of_tenants)
        self.create_scopes_collections(databases=self.databases.values())
        for counter, database in enumerate(self.databases.values()):
            for scope in database.collections:
                for collection in database.collections[scope]:
                    self.log.info(f"Iteration on db {database.id}. scope {scope} collection {collection}")
                    index1 = f'idx_a_db{counter}_{scope}_{collection}_{random.randint(0, 10000)}'
                    index2 = f'idx_b_db{counter}_{scope}_{collection}_{random.randint(0, 10000)}'
                    self.log.info(f"Index names index1: {index1} and index2: {index2}")
                    index1_replica, index2_replica = f'{index1} (replica 1)', f'{index2} (replica 1)'
                    self.log.info(f"Index replica names index1 replica {index1_replica} and index2 replica {index2_replica}")
                    self.create_index(database, query_statement=f"create index {index1} on `{database.id}`.{scope}.{collection}(a)",
                                      use_sdk=self.use_sdk)
                    self.create_index(database, query_statement=f"create index {index2} on `{database.id}`.{scope}.{collection}(b)",
                                      use_sdk=self.use_sdk)
                    time.sleep(30)
                    self.rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
                    rest_info = self.create_rest_info_obj(username=database.admin_username,
                                                          password=database.admin_password,
                                                          rest_host=database.rest_host)
                    nodes_obj = self.rest_obj.get_all_dataplane_nodes()
                    self.log.debug(f"Dataplane nodes object {nodes_obj}")
                    for node in nodes_obj:
                        if 'index' in node['services']:
                            host1 = self.get_resident_host_for_index(index_name=index1, rest_info=rest_info,
                                                                     indexer_node=node['hostname'].split(":")[0])
                            host1_replica = self.get_resident_host_for_index(index_name=index1_replica,
                                                                             rest_info=rest_info,
                                                                             indexer_node=node['hostname'].split(":")[0])
                            host2 = self.get_resident_host_for_index(index_name=index2, rest_info=rest_info,
                                                                     indexer_node=node['hostname'].split(":")[0])
                            host2_replica = self.get_resident_host_for_index(index_name=index2_replica,
                                                                             rest_info=rest_info,
                                                                             indexer_node=node['hostname'].split(":")[0])
                            break
                    self.log.info(
                        f"Hosts on which indexes reside. index1 {host1} and index1 replica {host1_replica}. "
                        f"index2 {host2}and index2 replica {host2_replica}")
                    if all(item is not None for item in [host1, host2, host1_replica, host2_replica]):
                        if host1 != host2 and host1 != host2_replica:
                            self.fail(
                                f"Index tenant affinity not honoured. Index1 is on host {host1}. Index2 is on host {host2}. "
                                f"Index1 replica is on host {host2}. Index2 replica is on host {host2_replica}")
                        if host2 != host1 and host2 != host1_replica:
                            self.fail(
                                f"Index tenant affinity not honoured. Index1 is on host {host1}. Index2 is on host {host2}. "
                                f"Index1 replica is on host {host2}. Index2 replica is on host {host2_replica}")
                    else:
                        self.fail("Not all indexes (or their replicas) have been created. Test failure")

    def test_max_limit_indexes_per_tenant(self):
        self.provision_databases(count=self.num_of_tenants)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            for index_num in range(INDEX_MAX_CAP_PER_TENANT):
                try:
                    self.create_index(database,
                                      query_statement=f"create index idx_db_{counter}_{index_num + 1} on _default(b)",
                                      use_sdk=self.use_sdk)
                except Exception as e:
                    self.fail(
                        f"Index creation fails despite not reaching the limit of {INDEX_MAX_CAP_PER_TENANT}."
                        f" No of indexes created: {index_num}")
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

    def test_query_node_not_co_located(self):
        self.provision_databases(count=self.num_of_tenants)
        for counter, database in enumerate(self.databases.values()):
            self.rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
            nodes_obj = self.rest_obj.get_all_dataplane_nodes()
            for node in nodes_obj:
                if 'n1ql' in node['services'] and len(node['services']) > 1:
                    self.fail(f"Node {node['hostname']} has multiple services {node['services']}")

    def test_run_queries_against_all_query_nodes(self):
        self.provision_databases(count=self.num_of_tenants)
        for counter, database in enumerate(self.databases.values()):
            index_name = f'#primary'
            namespace = f"default:`{database.id}`._default._default"
            self.rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
            nodes_obj = self.rest_obj.get_all_dataplane_nodes()
            self.load_databases(load_all_databases=False, num_of_docs=10,
                                database_obj=database, scope="_default", collection="_default")
            query_gen = QueryDefinition(index_name=index_name)
            query = query_gen.generate_primary_index_create_query(defer_build=self.defer_build, namespace=namespace)
            self.run_query(database=database, query=query)
            for node in nodes_obj:
                if 'n1ql' in node['services']:
                    resp_json = self.run_query(database=database, query="select count(*) from _default",
                                               query_node=node['hostname'].split(":")[0])
                    if resp_json['status'] != 'success' or resp_json['results'][0]['$1'] != 10:
                        self.fail(f"Count query errored out. Response is {resp_json}")
                    self.log.info(f"Response is {resp_json}")

    def test_run_queries_against_different_tenant(self):
        self.provision_databases(count=self.num_of_tenants)
        index_fields = ['age', 'country']
        user1, password1, user2, password2 = None, None, None, None
        queries = ['select * from _default where age>20', 'select * from _default where country like "A%"']
        for counter, database in enumerate(self.databases.values()):
            index_name = f'idx_db{counter}'
            self.create_index(database,
                              query_statement=f"create index {index_name} on _default({index_fields[counter]})")
            self.rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
            self.load_databases(load_all_databases=False, num_of_docs=10,
                                database_obj=database, scope="_default", collection="_default")
            if counter == 0:
                user1, password1 = database.access_key, database.secret_key
            else:
                user2, password2 = database.access_key, database.secret_key
        for counter, database in enumerate(self.databases.values()):
            test_pass = False
            if counter == 0:
                user, password = user2, password2
            else:
                user, password = user1, password1
            try:
                self.run_query(database=database, username=user, password=password, query=queries[counter])
                print("Query ran without errors. Test failure")
                test_pass = False
            except requests.exceptions.HTTPError as err:
                print(str(err))
                if '401 Client Error: Unauthorized' in str(err):
                    test_pass = True
            if not test_pass:
                self.fail(
                    f"User {user} with password {password} able to run queries on {database.id} a database the user is unauthorised for")

    def test_scale_up_number_of_tenants(self):
        try:
            if not self.new_dataplane_id:
                self.fail("This test needs a dataplane_id parameter in the conf file to run")
            dataplane = ServerlessDataPlane(self.new_dataplane_id)
            rest_api_info = self.api.get_access_to_serverless_dataplane_nodes(dataplane_id=self.new_dataplane_id)
            dataplane.populate(rest_api_info)
            rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                                  password=dataplane.admin_password,
                                                  rest_host=dataplane.rest_host)
            num_of_batches = math.ceil(self.num_of_tenants / 20)
            num_of_tenants = self.num_of_tenants
            for batch in range(num_of_batches):
                index_nodes_before = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
                # Need to check if self.databases gets overwritten (possible bug)
                db_count = 20 if num_of_tenants >= 20 else num_of_tenants
                self.provision_databases(count=db_count, seed=f"test-scale-up-number-of-tenants-{batch}",
                                         dataplane_id=self.new_dataplane_id)
                self.create_scopes_collections(databases=self.databases.values())
                for counter, database in enumerate(self.databases.values()):
                    self.load_databases(database_obj=database, num_of_docs=10)
                    self.log.info(f"Iteration number {counter+1}")
                    for scope in database.collections:
                        for collection in database.collections[scope]:
                            index1 = f'idx_db{counter}_{random.randint(0, 1000)}'
                            namespace = f"default:`{database.id}`.`{scope}`.`{collection}`"
                            self.create_index(database, query_statement=f"create index {index1} on {namespace}(b{counter})",
                                              use_sdk=self.use_sdk)
                            self.log.info(f"Iteration number {counter+1}. Index creation successful")
                            index_exists = self.check_if_index_exists(database_obj=database, index_name=index1)
                            self.log.info(f"Iteration number {counter+1}. index {index1} exists? {index_exists}")
                            index_nodes_after = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
                            for node in index_nodes_after:
                                num_tenant_stat = self.get_index_stats(indexer_node=index_nodes_before[0],
                                                                       rest_info=rest_info)['num_tenants']
                                self.log.info(f"Num_tenant stat from /stats endpoint: {num_tenant_stat} on index node {node}")
                            if index_nodes_after != index_nodes_before:
                                self.fail("Scaling has happened even when the num of tenants has not reached 20")
                time.sleep(300)
                index_nodes_after = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
                self.log.info(f"Index nodes after iteration {batch} run:{index_nodes_after}")
                if index_nodes_before == index_nodes_after:
                    self.fail(f"Index sub-cluster did not scale despite creating indexes for {self.num_of_tenants} tenants")
                num_of_tenants = num_of_tenants - 20
        finally:
            # TODO uncomment this after getting the appropriate keys needed to work with the s3 bucket
            pass
            # indexer_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            # storage_scheme, bucket, storage_prefix = self.get_fast_rebalance_config(rest_info=rest_info,
            #                                                                         indexer_node=indexer_nodes[0])
            # self.s3_utils_obj.check_s3_cleanup(bucket=bucket, folder=storage_prefix)

    def test_scale_indexer_sub_cluster(self):
        try:
            if not self.new_dataplane_id:
                self.fail("This test needs a new_dataplane_id parameter")
            dataplane = ServerlessDataPlane(self.new_dataplane_id)
            rest_api_info = self.api.get_access_to_serverless_dataplane_nodes(dataplane_id=self.new_dataplane_id)
            dataplane.populate(rest_api_info)
            rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                                  password=dataplane.admin_password,
                                                  rest_host=dataplane.rest_host)
            self.log.info(
                f"New dataplane credentials: User: {dataplane.admin_username} Password: {dataplane.admin_password}"
                f" Rest host: {dataplane.rest_host}")
            self.set_log_level_query_service(rest_info=rest_info, level='debug')
            index_nodes_before = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            self.log.info(f"Index nodes before the test run:{index_nodes_before}")
            time.sleep(60)
            self.log.info("Waiting 60 seconds after the dataplane is ready")
            self.provision_databases(count=self.num_of_tenants, seed="test-scale-up", dataplane_id=self.new_dataplane_id)
            self.create_scopes_collections(databases=self.databases.values())
            hosts = {}
            for counter, database in enumerate(self.databases.values()):
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        create_index_list = self.gsi_util_obj.get_create_index_list(self.definition_list,
                                                                                    f"`{database.id}`.{scope}.{collection}")
                        self.log.info(f"Create index list {create_index_list}")
                        for index_create_query in create_index_list:
                            self.run_query(database=database, query=index_create_query)
                            time.sleep(2)
                        index_list = self.get_all_index_names(database=database)
                        self.log.info(f"Index list from system:indexes query {index_list}")
                        self.rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
                        rest_info = self.create_rest_info_obj(username=database.admin_username,
                                                              password=database.admin_password,
                                                              rest_host=database.rest_host)
                        nodes_obj = self.rest_obj.get_all_dataplane_nodes()
                        hosts[database.id] = set()
                        for node in nodes_obj:
                            for index_name in index_list:
                                if 'index' in node['services']:
                                    host1 = self.get_resident_host_for_index(index_name=index_name, rest_info=rest_info,
                                                                     indexer_node=node['hostname'].split(":")[0])
                                    host1_replica = self.get_resident_host_for_index(index_name=f'{index_name} (replica 1)', rest_info=rest_info,
                                                                     indexer_node=node['hostname'].split(":")[0])
                                    if not host1:
                                        self.fail(f"Index {index_name} not found on any of the hosts")
                                    if not host1_replica:
                                        self.fail(f"Index {index_name} not found on any of the hosts")
                                    if not hosts[database.id]:
                                        if host1:
                                            hosts[database.id].add(host1[0])
                                        if host1_replica:
                                            hosts[database.id].add(host1_replica[0])
                                    elif host1[0] not in hosts[database.id] and host1_replica[0] not in hosts[database.id]:
                                        self.fail(f"Index tenant affinity not honored for index name {index_name}. Host it resides on"
                                                  f"{host1}. Replica host {host1_replica}. Indexes have so far only been created on these hosts {hosts}")
            self.load_data_new_doc_loader(databases=self.databases.values(), doc_start=0, doc_end=self.total_doc_count)
            with ThreadPoolExecutor() as executor:
                event = Event()
                future = executor.submit(self.run_parallel_workloads, event)
                self.scale_up_index_subcluster(dataplane=dataplane)
                event.set()
                future.result()
            self.log.info("============== cbcollect after scale up ==========================")
            self.collect_log_on_dataplane_nodes()
            index_nodes_after = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            if index_nodes_before == index_nodes_after:
                self.fail(f"Index sub-cluster did not scale despite creating indexes for {self.num_of_tenants} tenants")
            self.log.info(f"Index nodes after scale up:{index_nodes_after}")
            for counter, database in enumerate(self.databases.values()):
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        index1 = f'idx__db0_{random.randint(0, 1000)}'
                        index1_replica = f'{index1} (replica 1)'
                        self.create_index(database, query_statement=f"create index {index1}_db{counter} on `{database.id}`.{scope}.{collection}(a)",
                                              use_sdk=self.use_sdk)
                        time.sleep(10)
                        self.rest_obj = RestConnection(dataplane.admin_username, dataplane.admin_password, dataplane.rest_host)
                        nodes_obj = self.rest_obj.get_all_dataplane_nodes()
                        for node in nodes_obj:
                            if 'index' in node['services']:
                                host1_after_scale = self.get_resident_host_for_index(index_name=index1, rest_info=rest_info,
                                                                         indexer_node=node['hostname'].split(":")[0])
                                host1_replica_after_scale = self.get_resident_host_for_index(index_name=index1_replica,
                                                                                 rest_info=rest_info,
                                                                                 indexer_node=node['hostname'].split(":")[0])
                                break
                        self.log.info(
                            f"Hosts on which indexes reside. index1 {host1_after_scale} and index1 replica {host1_replica_after_scale}")
                        if all(item is not None for item in [host1_after_scale, host1_replica_after_scale]):
                            if host1_after_scale[0] not in hosts[database.id] and host1_replica_after_scale[0] not in hosts[database.id]:
                                self.fail(f"Tenant affinity not honored after scale down. After scaling, the index {index1} resides on"
                                          f"host {host1_after_scale}. Replica resides on {host1_replica_after_scale}, but the "
                                          f"indexes created before reside on {hosts[database.id]}")
            # run queries and mutations after scale-up
            with ThreadPoolExecutor() as executor:
                event = Event()
                future = executor.submit(self.run_parallel_workloads, event)
                time.sleep(120)
                event.set()
                future.result()
            with ThreadPoolExecutor() as executor:
                event = Event()
                future = executor.submit(self.run_parallel_workloads, event)
                self.scale_down_index_subcluster(dataplane=dataplane)
                event.set()
                future.result()
            self.collect_log_on_dataplane_nodes()
            index_nodes_after_2 = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            if index_nodes_after == index_nodes_after_2:
                self.fail(f"Index sub-cluster did not scale down despite creating indexes for {self.num_of_tenants} tenants")
            self.log.info(f"Index nodes after scale down:{index_nodes_after_2}")
            for counter, database in enumerate(self.databases.values()):
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        index1 = f'idx__db0_{random.randint(0, 1000)}'
                        index1_replica = f'{index1} (replica 1)'
                        self.create_index(database, query_statement=f"create index {index1} on `{database.id}`.{scope}.{collection}(a)",
                                          use_sdk=self.use_sdk)
                        time.sleep(10)
                        self.rest_obj = RestConnection(dataplane.admin_username, dataplane.admin_password, dataplane.rest_host)
                        nodes_obj = self.rest_obj.get_all_dataplane_nodes()
                        for node in nodes_obj:
                            if 'index' in node['services']:
                                host1_after_scale_down = self.get_resident_host_for_index(index_name=index1, rest_info=rest_info,
                                                                                     indexer_node=node['hostname'].split(":")[0])
                                host1_replica_after_scale_down = self.get_resident_host_for_index(index_name=index1_replica,
                                                                                             rest_info=rest_info,
                                                                                             indexer_node=
                                                                                             node['hostname'].split(":")[0])
                                break
                        self.log.info(
                            f"Hosts on which indexes reside. index1 {host1_after_scale_down} and index1 replica {host1_replica_after_scale_down}")
                        if all(item is not None for item in [host1_after_scale_down, host1_replica_after_scale_down]):
                            if host1_after_scale_down[0] not in hosts[database.id] and host1_replica_after_scale_down[0] not in hosts[
                                database.id]:
                                self.fail(f"Tenant affinity not honored after scaling. After scaling, the index {index1} resides on"
                                          f"host {host1_after_scale}. Replica resides on {host1_replica_after_scale}, but the "
                                          f"indexes created before reside on {hosts[database.id]}")
            # run queries and mutations after scale down
            self.collect_log_on_dataplane_nodes()
            self.log.info("============== cbcollect after scale down ==========================")
            with ThreadPoolExecutor() as executor:
                event = Event()
                future = executor.submit(self.run_parallel_workloads, event)
                time.sleep(120)
                event.set()
                future.result()
        finally:
            self.set_log_level_query_service(rest_info=rest_info, level='info')
            # indexer_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            # storage_scheme, bucket, storage_prefix = self.get_fast_rebalance_config(rest_info=rest_info,
            #                                                                         indexer_node=indexer_nodes[0])
            # self.s3_utils_obj.check_s3_cleanup(bucket=bucket, folder=storage_prefix)

    def test_scale_query_sub_cluster(self):
        try:
            if not self.new_dataplane_id:
                self.fail("This test needs a new_dataplane_id parameter")
            dataplane = ServerlessDataPlane(self.new_dataplane_id)
            rest_api_info = self.api.get_access_to_serverless_dataplane_nodes(dataplane_id=self.new_dataplane_id)
            dataplane.populate(rest_api_info)
            rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                                  password=dataplane.admin_password,
                                                  rest_host=dataplane.rest_host)
            self.set_log_level_query_service(rest_info=rest_info, level='debug')
            query_nodes_before = self.get_nodes_from_services_map(rest_info=rest_info, service="n1ql")
            self.log.info(f"Query nodes before the test run:{query_nodes_before}")
            self.provision_databases(count=self.num_of_tenants, seed="test-scale-query", dataplane_id=self.new_dataplane_id)
            self.create_scopes_collections(databases=self.databases.values())
            hosts = {}
            for counter, database in enumerate(self.databases.values()):
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        create_index_list = self.gsi_util_obj.get_create_index_list(self.definition_list,
                                                                                    f"`{database.id}`.{scope}.{collection}")
                        for index_create_query in create_index_list:
                            self.run_query(database=database, query=index_create_query)
            self.load_data_new_doc_loader(databases=self.databases.values(), doc_start=0, doc_end=self.total_doc_count)
            with ThreadPoolExecutor() as executor:
                event = Event()
                future = executor.submit(self.run_parallel_workloads, event)
                self.scale_up_query_subcluster(dataplane=dataplane)
                event.set()
                future.result()
            query_nodes_after = self.get_all_query_nodes(rest_info=rest_info)
            if query_nodes_before == query_nodes_after:
                self.fail(f"Query sub-cluster did not scale despite loadfactor being over the limit")
            self.log.info("============== cbcollect after scale up ==========================")
            self.collect_log_on_dataplane_nodes()
            self.log.info(f"Query nodes after the scale up:{query_nodes_after}")
            # run queries and mutations after scale-up
            with ThreadPoolExecutor() as executor:
                event = Event()
                future = executor.submit(self.run_parallel_workloads, event)
                time.sleep(120)
                event.set()
                future.result()
            for counter, database in enumerate(self.databases.values()):
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        index1 = f'idx_db0_{random.randint(0, 1000)}'
                        self.create_index(database, query_statement=f"create index {index1}_db{counter} on `{database.id}`.`{scope}`.`{collection}`(a)",
                                              use_sdk=self.use_sdk)
            dataplane = ServerlessDataPlane(self.new_dataplane_id)
            rest_api_info = self.api.get_access_to_serverless_dataplane_nodes(dataplane_id=self.new_dataplane_id)
            dataplane.populate(rest_api_info)
            rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                                  password=dataplane.admin_password,
                                                  rest_host=dataplane.rest_host)
            with ThreadPoolExecutor() as executor:
                event = Event()
                future = executor.submit(self.run_parallel_workloads, event)
                self.scale_down_query_subcluster(dataplane=dataplane)
                event.set()
                future.result()
            self.log.info("============== cbcollect after scale down ==========================")
            self.collect_log_on_dataplane_nodes()
            # run queries and mutations after scale-down
            query_nodes_after_2 = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            if query_nodes_after == query_nodes_after_2:
                self.fail(f"Query sub-cluster did not scale down despite loadfactor being below the threshold limit")
            self.log.info(f"Query nodes after the test run:{query_nodes_after}")
            # run queries and mutations after scale down
            with ThreadPoolExecutor() as executor:
                event = Event()
                future = executor.submit(self.run_parallel_workloads, event)
                time.sleep(120)
                event.set()
                future.result()
            for counter, database in enumerate(self.databases.values()):
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        index1 = f'idx_db0_{random.randint(0, 1000)}'
                        self.create_index(database, query_statement=f"create index {index1}_db{counter} on `{database.id}`.`{scope}`.`{collection}`(a)",
                                              use_sdk=self.use_sdk)
            self.collect_log_on_dataplane_nodes()
        finally:
            self.set_log_level_query_service(rest_info=rest_info, level='info')

    def test_rebalance_first_ddl_queued(self):
        try:
            if not self.new_dataplane_id:
                self.fail("This test needs a new_dataplane_id parameter to run")
            dataplane = ServerlessDataPlane(self.new_dataplane_id)
            rest_api_info = self.api.get_access_to_serverless_dataplane_nodes(dataplane_id=self.new_dataplane_id)
            dataplane.populate(rest_api_info)
            rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                                  password=dataplane.admin_password,
                                                  rest_host=dataplane.rest_host)
            index_nodes_before = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            self.provision_databases(count=20, seed=f"test-rebalance-first-ddl-queued",
                                     dataplane_id=self.new_dataplane_id)
            scope_collection_map = {"scope1": ["coll1", "coll2"]}
            self.create_scopes_collections(databases=self.databases.values(), scope_collection_map=scope_collection_map)
            for counter, database in enumerate(self.databases.values()):
                self.log.info(f"Iteration number {counter+1}")
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        self.load_databases(database_obj=database, num_of_docs=self.total_doc_count, scope=scope,
                                            collection=collection, doc_template=self.dataset)
                        if collection == "coll1":
                            create_list = self.gsi_util_obj.get_create_index_list(definition_list=self.definition_list,
                                                                                  namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                            self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, database=database,
                                                                 capella_run=True)
                        self.log.info(f"Iteration for {database.id} complete. Dataload and index creation done")
            time_now, scale_complete = time.time(), False
            while time.time() - time_now < 300:
                index_nodes_after = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
                self.log.info(f"Index nodes after scale up :{index_nodes_after}")
                if len(index_nodes_after) == len(index_nodes_before) + 2:
                    scale_complete = True
                    break
                time.sleep(30)
            if not scale_complete:
                self.fail(f"Index sub-cluster did not scale despite creating indexes for {self.num_of_tenants} tenants")
            sub_cluster_2_ready, batch = False, 0
            time.sleep(60)
            while not sub_cluster_2_ready:
                self.provision_databases(count=2, seed=f"new-test-{batch}-rebalance-first-ddl-queued",
                                         dataplane_id=self.new_dataplane_id)
                new_db_list = []
                for database in self.databases.values():
                    if f"new-test-{batch}" in database.id:
                        new_db_list.append(database)
                self.create_scopes_collections(databases=new_db_list,
                                               scope_collection_map=scope_collection_map)
                for counter, database in enumerate(new_db_list):
                    self.log.info(f"Iteration number {counter + 1}")
                    for scope in database.collections:
                        for collection in database.collections[scope]:
                            self.load_databases(database_obj=database, num_of_docs=self.total_doc_count, scope=scope,
                                                collection=collection, doc_template=self.dataset)
                            if collection == "coll1":
                                create_list = self.gsi_util_obj.get_create_index_list(definition_list=self.definition_list,
                                                                                      namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                                self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, database=database,
                                                                     capella_run=True)
                index_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
                num_tenants_dict = {}
                for node in index_nodes:
                    num_tenant_stat = self.get_index_stats(indexer_node=node,
                                                           rest_info=rest_info)['num_tenants']
                    num_tenants_dict[node] = num_tenant_stat
                if all(i >= 2 for i in num_tenants_dict.values()):
                    sub_cluster_2_ready = True
                batch += 1
            rev_dict = {}
            for key in num_tenants_dict:
                if num_tenants_dict[key] not in rev_dict:
                    rev_dict[num_tenants_dict[key]] = [key]
                else:
                    rev_dict[num_tenants_dict[key]].append(key)
            ddl_list, id_ddl_list = [], []
            for item in rev_dict.values():
                node = item[0]
                tenant_list = self.get_all_tenants_on_host(node, rest_info=rest_info)
                for tenant in tenant_list[:2]:
                    ddl_list.append(self.databases[tenant])
                    id_ddl_list.append(self.databases[tenant].id)
            # create a few deferred indexes
            for counter, database in enumerate(ddl_list):
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        defer_defn_list = self.gsi_util_obj.get_index_definition_list(dataset=self.dataset, prefix="defer")
                        if collection == "coll1":
                            create_list = self.gsi_util_obj.get_create_index_list(definition_list=defer_defn_list,
                                                                                  namespace=f"`{database.id}`.`{scope}`.`{collection}`",
                                                                                  defer_build_mix=True)
                            self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, database=database,
                                                                 capella_run=True)
            indexer_stats_before = self.get_index_metadata_stats(rest_info=rest_info)
            self.log.info(f"No. of stats for indexes available {len(indexer_stats_before)}")
            if len(indexer_stats_before) == 0:
                self.fail("Stats for indexes before rebalance/ddl conflict not available")
            self.log.info(
                "============== cbcollect just before defragmentation is triggered ==========================")
            self.collect_log_on_dataplane_nodes()
            delete_count, num_dbs_deleted = max(rev_dict.keys()) + min(rev_dict.keys()) - 15, 0
            for database in self.databases.values():
                if num_dbs_deleted < delete_count and database.id not in id_ddl_list:
                    self.delete_database(database_id=database.id)
                    num_dbs_deleted += 1
            defrag_result_b4_reb = self.get_defrag_response(rest_info=rest_info)
            self.log.info("Defrag response is {}".format(defrag_result_b4_reb))
            time_now, rebalance_started = time.time(), False
            rest_obj = RestConnection(rest_info.admin_username, rest_info.admin_password, rest_info.rest_host)
            while time.time() - time_now < 1200:
                status, progress = rest_obj._rebalance_status_and_progress()
                if status == 'running':
                    rebalance_started = True
                    break
                time.sleep(10)
            if not rebalance_started:
                self.fail("Rebalance not triggered by defrag API despite waiting 1200 seconds")
            drop_dict = {}
            with ThreadPoolExecutor() as executor:
                tasks = []
                for counter, database in enumerate(ddl_list):
                    for scope in database.collections:
                        for collection in database.collections[scope]:
                            new_defn_list = self.gsi_util_obj.get_index_definition_list(dataset=self.dataset, prefix="new2")
                            create_list = self.gsi_util_obj.get_create_index_list(definition_list=new_defn_list,
                                                                                  namespace=f"`{database.id}`.`{scope}`.`{collection}`",
                                                                                  defer_build_mix=True)
                            task = executor.submit(self.gsi_util_obj.create_gsi_indexes,
                                                   create_queries=create_list,
                                                   database=database, capella_run=True)
                            tasks.append(task)
                            list_all_indexes = self.get_all_index_names(database=database, scope=scope, collection=collection)
                            drop_list = [index for index in list_all_indexes if "new2" not in index and "defer" not in index]
                            random.shuffle(drop_list)
                            drop_index = math.ceil((len(drop_list)/10))
                            drop_list = drop_list[:drop_index]
                            drop_dict[database] = drop_list
                            for item in drop_list:
                                query = f"drop index `{item}` on `{database.id}`.`{scope}`.`{collection}`"
                                drop_task = executor.submit(self.run_query, database=database, query=query)
                                tasks.append(drop_task)
                for task in tasks:
                    task.result()
            # build all the deferred indexes
            with ThreadPoolExecutor() as executor:
                tasks = []
                for counter, database in enumerate(ddl_list):
                    build_task = executor.submit(self.build_all_indexes, databases=[database])
                    tasks.append(build_task)
                for task in tasks:
                    task.result()
            index_nodes_after = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            for node in index_nodes_after:
                num_tenant_stat = self.get_index_stats(indexer_node=node,
                                                       rest_info=rest_info)['num_tenants']
                self.log.info(f"Num_tenant stat from /stats endpoint: {num_tenant_stat} on index node {node}")
            self.get_all_index_node_usage_stats()
            rebalance_complete = False
            while time.time() - time_now < 1200:
                status, progress = rest_obj._rebalance_status_and_progress()
                if (status, progress) == ('none', 100):
                    rebalance_complete = True
                    break
                time.sleep(30)
            if not rebalance_complete:
                self.fail("Rebalance not triggered by defrag API despite waiting 1200 seconds")
            # TODO replace with smarter way to wait until all indexes are online
            time.sleep(300)
            indexer_stats_after = self.get_index_metadata_stats(rest_info=rest_info)
            self.log.info(f"Indexer stats after: {indexer_stats_after}")
            for new_db in ddl_list:
                index_list = self.get_all_index_names(database=new_db)
                for index in index_list:
                    if "new2" in index:
                        keyspace = f"`{new_db.id}`"
                        status = self.get_index_status(rest_info=rest_info, index_name=index, keyspace=keyspace)
                        self.log.info(f"Index {index} on keyspace {keyspace} status {status}")
                        if status != 'Ready':
                            self.fail(f"Index did not get built. Index {index} on keyspace {keyspace} status {status}")
            for db, index_list in drop_dict.items():
                for index in index_list:
                    if self.check_if_index_exists(database_obj=db, index_name=index):
                        self.log.info(f"Index {index} was dropped during the DDL/Rebalance conflict phase as expected.")
                    else:
                        self.log.error(f"Index {index} was not dropped during the DDL/Rebalance conflict phase as expected.")
            indexer_stats_after = self.get_index_metadata_stats(rest_info=rest_info)
            total_defer, total_new = 0, 0
            for key in indexer_stats_after.keys():
                if "defer" in key:
                    total_defer += 1
                if "new2" in key:
                    total_new += 1
            self.log.info(
                f"Stat count for newly created indexes is {total_new}. Stat count for deferred indexes {total_defer}")
            if total_defer == 0:
                self.fail("Stats for built deferred indexes built after rebalance not available")
            if total_new == 0:
                self.fail("Stats for newly indexes created during rebalance not available")
        finally:
            # TODO uncomment this after getting the appropriate keys needed to work with the s3 bucket
            pass
            # indexer_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            # storage_scheme, bucket, storage_prefix = self.get_fast_rebalance_config(rest_info=rest_info,
            #                                                                         indexer_node=indexer_nodes[0])
            # self.s3_utils_obj.check_s3_cleanup(bucket=bucket, folder=storage_prefix)

    def test_ddl_first_rebalance_queued(self):
        try:
            if not self.new_dataplane_id:
                self.fail("This test needs a new_dataplane_id parameter to run")
            dataplane = ServerlessDataPlane(self.new_dataplane_id)
            rest_api_info = self.api.get_access_to_serverless_dataplane_nodes(dataplane_id=self.new_dataplane_id)
            dataplane.populate(rest_api_info)
            rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                                  password=dataplane.admin_password,
                                                  rest_host=dataplane.rest_host)
            index_nodes_before = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            self.provision_databases(count=20, seed=f"test-rebalance-first-ddl-queued",
                                     dataplane_id=self.new_dataplane_id)
            scope_collection_map = {"scope1": ["coll1", "coll2"]}
            self.create_scopes_collections(databases=self.databases.values(), scope_collection_map=scope_collection_map)
            for counter, database in enumerate(self.databases.values()):
                self.log.info(f"Iteration number {counter+1}")
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        self.load_databases(database_obj=database, num_of_docs=self.total_doc_count, scope=scope,
                                            collection=collection, doc_template=self.dataset)
                        if collection == "coll1":
                            create_list = self.gsi_util_obj.get_create_index_list(definition_list=self.definition_list,
                                                                                  namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                            self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, database=database,
                                                                 capella_run=True)
                        self.log.info(f"Iteration for {database.id} complete. Dataload and index creation done")
            time_now, scale_complete = time.time(), False
            while time.time() - time_now < 300:
                index_nodes_after = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
                self.log.info(f"Index nodes after scale up :{index_nodes_after}")
                if len(index_nodes_after) == len(index_nodes_before) + 2:
                    scale_complete = True
                    break
                time.sleep(30)
            if not scale_complete:
                self.fail(f"Index sub-cluster did not scale despite creating indexes for {self.num_of_tenants} tenants")
            sub_cluster_2_ready, batch = False, 0
            time.sleep(60)
            while not sub_cluster_2_ready:
                self.provision_databases(count=2, seed=f"new-test-{batch}-rebalance-first-ddl-queued",
                                         dataplane_id=self.new_dataplane_id)
                new_db_list = []
                for database in self.databases.values():
                    if f"new-test-{batch}" in database.id:
                        new_db_list.append(database)
                self.create_scopes_collections(databases=new_db_list,
                                               scope_collection_map=scope_collection_map)
                for counter, database in enumerate(new_db_list):
                    self.log.info(f"Iteration number {counter + 1}")
                    for scope in database.collections:
                        for collection in database.collections[scope]:
                            self.load_databases(database_obj=database, num_of_docs=self.total_doc_count, scope=scope,
                                                collection=collection, doc_template=self.dataset)
                            if collection == "coll1":
                                create_list = self.gsi_util_obj.get_create_index_list(definition_list=self.definition_list,
                                                                                      namespace=f"`{database.id}`.`{scope}`.`{collection}`")
                                self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, database=database,
                                                                     capella_run=True)
                index_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
                num_tenants_dict = {}
                for node in index_nodes:
                    num_tenant_stat = self.get_index_stats(indexer_node=node,
                                                           rest_info=rest_info)['num_tenants']
                    num_tenants_dict[node] = num_tenant_stat
                if all(i >= 2 for i in num_tenants_dict.values()):
                    sub_cluster_2_ready = True
                batch += 1
            rev_dict = {}
            for key in num_tenants_dict:
                if num_tenants_dict[key] not in rev_dict:
                    rev_dict[num_tenants_dict[key]] = [key]
                else:
                    rev_dict[num_tenants_dict[key]].append(key)
            ddl_list, id_ddl_list = [], []
            for item in rev_dict.values():
                node = item[0]
                tenant_list = self.get_all_tenants_on_host(node, rest_info=rest_info)
                for tenant in tenant_list[:2]:
                    ddl_list.append(self.databases[tenant])
                    id_ddl_list.append(self.databases[tenant].id)
            indexer_stats_before = self.get_index_metadata_stats(rest_info=rest_info)
            self.log.info(f"No. of stats for indexes available {len(indexer_stats_before)}")
            if len(indexer_stats_before) == 0:
                self.fail("Stats for indexes before rebalance/ddl conflict not available")
            drop_dict = {}
            with ThreadPoolExecutor() as executor:
                tasks = []
                for counter, database in enumerate(ddl_list):
                    for scope in database.collections:
                        for collection in database.collections[scope]:
                            new_defn_list = self.gsi_util_obj.get_index_definition_list(dataset=self.dataset, prefix="new2")
                            create_list = self.gsi_util_obj.get_create_index_list(definition_list=new_defn_list,
                                                                                  namespace=f"`{database.id}`.`{scope}`.`{collection}`",
                                                                                  defer_build=True)
                            # add a few drop index statements during the DDL phase
                            list_all_indexes = self.get_all_index_names(database=database, scope=scope,
                                                                        collection=collection)
                            drop_list = list_all_indexes
                            random.shuffle(drop_list)
                            drop_index = math.ceil((len(drop_list) / 10))
                            drop_list = drop_list[:drop_index]
                            drop_dict[database] = drop_list
                            for item in drop_list:
                                query = f"drop index `{item}` on `{database.id}`.`{scope}`.`{collection}`"
                                drop_task = executor.submit(self.run_query, database=database, query=query)
                                tasks.append(drop_task)
                            task = executor.submit(self.gsi_util_obj.create_gsi_indexes,
                                                   create_queries=create_list,
                                                   database=database, capella_run=True)
                            tasks.append(task)
                for task in tasks:
                    task.result()
            self.log.info(
                "============== cbcollect just before defragmentation is triggered ==========================")
            self.collect_log_on_dataplane_nodes()
            self.build_all_indexes(ddl_list)
            delete_count, num_dbs_deleted = max(rev_dict.keys()) + min(rev_dict.keys()) - 15, 0
            for database in self.databases.values():
                if num_dbs_deleted < delete_count and database.id not in id_ddl_list:
                    self.delete_database(database_id=database.id)
                    num_dbs_deleted += 1
            time_now, rebalance_started = time.time(), False
            rest_obj = RestConnection(rest_info.admin_username, rest_info.admin_password, rest_info.rest_host)
            defrag_result_b4_reb = self.get_defrag_response(rest_info=rest_info)
            self.log.info("Defrag response is {}".format(defrag_result_b4_reb))
            while time.time() - time_now < 1200:
                status, progress = rest_obj._rebalance_status_and_progress()
                if status == 'running':
                    break
                time.sleep(10)
            index_nodes_after = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            for node in index_nodes_after:
                num_tenant_stat = self.get_index_stats(indexer_node=node,
                                                       rest_info=rest_info)['num_tenants']
                self.log.info(f"Num_tenant stat from /stats endpoint: {num_tenant_stat} on index node {node}")
            while time.time() - time_now < 1200:
                status, progress = rest_obj._rebalance_status_and_progress()
                if (status, progress) == ('none', 100):
                    break
                time.sleep(30)
            for new_db in ddl_list:
                index_list = self.get_all_index_names(database=new_db)
                for index in index_list:
                    if "new2" in index:
                        keyspace = f"`{new_db.id}`"
                        status = self.get_index_status(rest_info=rest_info, index_name=index, keyspace=keyspace)
                        self.log.info(f"Index {index} on keyspace {keyspace} status {status}")
                        if status != 'Ready':
                            self.fail(f"Index did not get built. Index {index} on keyspace {keyspace} status {status}")
            # check that drop index statements have worked
            for db, index_list in drop_dict.items():
                for index in index_list:
                    if self.check_if_index_exists(database_obj=db, index_name=index):
                        self.log.info(
                            f"Index {index} was dropped during the DDL/Rebalance conflict phase as expected.")
                    else:
                        self.log.error(
                            f"Index {index} was not dropped during the DDL/Rebalance conflict phase as expected.")
            status, progress = rest_obj._rebalance_status_and_progress()
            if (status, progress) != ('none', 100):
                self.fail(f"Rebalance failure after rebalance induced by defragmentation was run")
            indexer_stats_after = self.get_index_metadata_stats(rest_info=rest_info)
            total_new = 0
            for key in indexer_stats_after.keys():
                if "new2" in key:
                    total_new += 1
            self.log.info(f"No of stats for indexes created during the rebalance/DDL conflict {total_new}")
            if total_new == 0:
                self.fail("Stats for indexes created during DDL/rebalance conflict not available")
        finally:
            # TODO uncomment this after getting the appropriate keys needed to work with the s3 bucket
            pass
            # indexer_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            # storage_scheme, bucket, storage_prefix = self.get_fast_rebalance_config(rest_info=rest_info,
            #                                                                         indexer_node=indexer_nodes[0])
            # self.s3_utils_obj.check_s3_cleanup(bucket=bucket, folder=storage_prefix)
