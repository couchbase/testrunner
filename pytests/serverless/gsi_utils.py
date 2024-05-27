"""
gsi_utils.py: This file contains methods for gsi index creation, drop, build and other utils

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = 04/10/22 11:53 am

"""
import datetime
import random
import string
import time
import uuid
from threading import Event

import logger
from functools import reduce
from concurrent.futures import ThreadPoolExecutor

from couchbase_helper.query_definitions import QueryDefinition

RANGE_SCAN_TEMPLATE = "SELECT {0} FROM %s WHERE {1}"


class GSIUtils(object):
    def __init__(self, query_obj):
        self.initial_index_num = 0
        self.log = logger.Logger.get_logger()
        self.definition_list = []
        self.run_query = query_obj
        self.batch_size = 0
        self.query_event = Event()

    def generate_magma_doc_loader_index_definition(self, index_name_prefix=None, skip_primary=False):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "docloader" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'name', index_fields=['name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'name is not null')))

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", "attributes.dimensions.height > 40"),
                                is_primary=True))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'age_gender_name',
                            index_fields=['age', 'gender', 'name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'age > 40 AND '
                                                                      'gender = "M"')))

        # GSI index with missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'missing_keys', index_fields=['name', 'age',
                                                                                         'marital'],
                            missing_indexes=True, missing_field_desc=False,
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'age > 30 AND '
                                                                      'marital = "S"')))

        # Paritioned Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['name', 'body'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'body like "%%E%%"'),
                            partition_by_fields=['body'], capella_run=True))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index',
                            index_fields=['mutated', 'name', 'body',
                                          'ALL ARRAY h.name FOR h IN attributes.hobbies END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'mutated >= 0 and '
                                                                      'ANY h IN attributes.hobbies SATISFIES'
                                                                      ' h.name = "Books" END')))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_2',
                            index_fields=['age', 'name', 'ALL animals'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'any a in animals satisfies '
                                                                      'a = "Forest green (traditional)" end')))

        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_employee_data_index_definition(self, index_name_prefix=None, skip_primary=False):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "employee" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'name', index_fields=['name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'name = "employee-1" ')))

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", "test_rate > 1"), is_primary=True))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'join_day_job_title',
                            index_fields=['join_day', 'job_title'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'join_day > 15 AND '
                                                                      'job_title = "Sales"')))

        # GSI index with missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'missing_keys', index_fields=['email', 'join_yr',
                                                                                         'join_mo'],
                            missing_indexes=True, missing_field_desc=False,
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'join_yr > 2010 AND '
                                                                      'join_mo > 6')))

        # Paritioned Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['job_title'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'job_title = "Support"'),
                            partition_by_fields=['job_title'], capella_run=True))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index',
                            index_fields=['join_mo, All ARRAY vm.os FOR vm IN VMs END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'ANY vm IN VMs SATISFIES vm.os = "ubuntu" '
                                                                      'END')))
        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_hotel_data_index_definition(self, index_name_prefix=None, skip_primary=False):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "hotel" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'price', index_fields=['price'],
                            query_template=RANGE_SCAN_TEMPLATE.format("price", "price > 0")))

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("suffix", "suffix is not NULL"), is_primary=True))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'free_breakfast_avg_rating',
                            index_fields=['free_breakfast', 'avg_rating'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      'avg_rating > 3 AND '
                                                                      'free_breakfast = true')))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'free_breakfast_array_count',
                            index_fields=['free_breakfast', 'type', 'free_parking', 'array_count(public_likes)',
                                          'price', 'country'],
                            query_template=RANGE_SCAN_TEMPLATE.format("country, avg(price) as AvgPrice, min(price) as MinPrice,"
                                                                      " max(price) as MaxPrice",
                                                                      "free_breakfast=True and free_parking=True and "
                                                                      "price is not null and "
                                                                      "array_count(public_likes)>5 and "
                                                                      "`type`='Hotel' group by country")))

        # # GSI index with Flatten Keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'flatten_keys',
                            index_fields=['DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness)'
                                          ' FOR r IN reviews when r.ratings.Cleanliness < 4 END',
                                          'country', 'email', 'free_parking'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      "ANY r IN reviews SATISFIES r.author LIKE 'M%%' "
                                                                      "AND r.ratings.Cleanliness = 3 END AND "
                                                                      "free_parking = TRUE AND country IS NOT NULL ")))

        # # GSI index with missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'missing_keys',
                            index_fields=['city', 'avg_rating', 'country'],
                            missing_indexes=True, missing_field_desc=True,
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      'avg_rating > 3 AND '
                                                                      'country like "%%F%%"')))

        # Paritioned Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name", 'name like "%%Dil%%"'),
                            partition_by_fields=['name'], capella_run=True))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_overall',
                            index_fields=['price, All ARRAY v.ratings.Overall FOR v IN reviews END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("address", 'ANY v IN reviews SATISFIES v.ratings.'
                                                                           '`Overall` > 3  END and price < 1000 ')))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_rooms',
                            index_fields=['price, All ARRAY v.ratings.Rooms FOR v IN reviews END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("name",
                                                                      'ANY v IN reviews SATISFIES v.ratings.'
                                                                      '`Rooms` > 3  END and price > 1000 ')))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_checkin',
                            index_fields=['country', 'DISTINCT ARRAY `r`.`ratings`.`Check in / front desk` '
                                                     'FOR r in `reviews` END', 'array_count(`public_likes`)',
                                          'array_count(`reviews`) DESC', '`type`', 'phone', 'price', 'email',
                                          'address', 'name', 'url'],
                            query_template=RANGE_SCAN_TEMPLATE.format("address",
                                                                      'country is not null and `type` is not null '
                                                                      'and (any r in reviews satisfies '
                                                                      'r.ratings.`Check in / front desk` '
                                                                      'is not null end) ')))

        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_person_data_index_definition(self, index_name_prefix=None, skip_primary=False):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "person" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'age', index_fields=['age'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", "age > 0")))

        # Primary Query
        if not skip_primary:
            prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
            definitions_list.append(
                QueryDefinition(index_name=prim_index_name, index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", "suffix is not NULL"), is_primary=True))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'firstName_lastName', index_fields=['firstName', 'lastName'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'firstName like "%%D%%" AND '
                                                                      'LastName is not NULL')))

        # GSI index with missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'missing_keys', index_fields=['age', 'city', 'country'],
                            missing_indexes=True, missing_field_desc=False,
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'firstName like "%%D%%" AND '
                                                                      'LastName is not NULL')))

        # Paritioned Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['streetAddress'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'streetAddress is not NULL'),
                            partition_by_fields=['streetAddress'], capella_run=True))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index', index_fields=['filler1'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'filler1 like "%%in%%"')))
        self.batch_size = len(definitions_list)
        return definitions_list

    def get_create_index_list(self, definition_list, namespace, defer_build_mix=False,
                              defer_build=False, num_replica=None, deploy_node_info=None, randomise_replica_count=False):
        create_index_list = []
        for index_gen in definition_list:
            nodes_list = None
            if defer_build_mix:
                defer_build = random.choice([True, False])
            if randomise_replica_count and num_replica >= 1:
                num_replicas = random.randint(1, num_replica)
                if deploy_node_info is not None:
                    nodes_list = random.sample(deploy_node_info, k=num_replicas+1)
            else:
                num_replicas = num_replica
            query = index_gen.generate_index_create_query(namespace=namespace, defer_build=defer_build,
                                                          num_replica=num_replicas, deploy_node_info=nodes_list)
            create_index_list.append(query)
        return create_index_list

    def get_build_indexes_query(self, definition_list, namespace):
        index_name_list = [f"`{index_gen.index_name}`" for index_gen in definition_list]
        build_indexes_string = ", ".join(index_name_list)
        build_query = f'BUILD INDEX ON {namespace} ({build_indexes_string})'
        return build_query

    def get_drop_index_list(self, definition_list, namespace):
        drop_index_list = []
        for index_gen in definition_list:
            query = index_gen.generate_index_drop_query(namespace=namespace)
            drop_index_list.append(query)
        return drop_index_list

    def get_select_queries(self, definition_list, namespace, limit=0):
        select_query_list = []
        for index_gen in definition_list:
            query = index_gen.generate_query(bucket=namespace)
            if limit > 0:
                query = f'{query} LIMIT {limit}'
            select_query_list.append(query)
        return select_query_list

    def get_count_query(self, dataset, namespace):
        if dataset == 'Person' or dataset == 'default':
            query = f"select * from {namespace} where age>0"
        elif dataset == 'Employee':
            query = f"select * from {namespace} where name is not null"
        elif dataset == 'Hotel':
            query = f"select * from {namespace} where price>0"
        elif dataset == 'Magma':
            query = f"select * from {namespace} where name is not null"
        else:
            raise Exception("Provide correct dataset. Valid arguments are Person, Employee, Magma, and Hotel")
        return query

    def async_create_indexes(self, create_queries, database=None, capella_run=False, query_node=None):
        with ThreadPoolExecutor() as executor:
            tasks = []
            for query in create_queries:
                if capella_run:
                    task = executor.submit(self.run_query, database=database, query=query)
                else:
                    task = executor.submit(self.run_query, query=query, server=query_node)
                tasks.append(task)
        return tasks

    def create_gsi_indexes(self, create_queries, database=None, capella_run=False, query_node=None):
        tasks = []
        with ThreadPoolExecutor() as executor:
            for query in create_queries:
                if capella_run:
                    tasks.append(executor.submit(self.run_query, database=database, query=query))
                else:
                    tasks.append(executor.submit(self.run_query, query=query, server=query_node))
            try:
                for task in tasks:
                    task.result()
            except Exception as err:
                print(err)

    def aysnc_run_select_queries(self, select_queries, database=None, capella_run=False, query_node=False,
                                 scan_consistency=None):
        with ThreadPoolExecutor() as executor:
            tasks = []
            for query in select_queries:
                if capella_run:
                    task = executor.submit(self.run_query, database=database, query=query,
                                           scan_consistency=scan_consistency)
                else:
                    task = executor.submit(self.run_query, query=query, server=query_node,
                                           scan_consistency=scan_consistency)
                tasks.append(task)
        return tasks

    def run_continous_query_load(self, select_queries, database=None, capella_run=False,
                                 query_node=False, sleep_timer=30):
        while self.query_event.is_set():
            try:
                tasks = self.aysnc_run_select_queries(select_queries=select_queries, database=database,
                                                      capella_run=capella_run, query_node=query_node)
                for task in tasks:
                    task.result()
            except Exception as err:
                self.log.error(f"Error occurred during query load: {err}")
            time.sleep(sleep_timer)

    def range_unequal_distribution(self, number=4, factor=1.2, total=100000):
        """
        This method divides a range into unequal parts of given number
        """
        z = total * (1 - 1 / factor) / (factor ** number - 1)
        distribution_list = []
        for i in range(number):
            part = round(z * factor ** i)
            distribution_list.append(part)
        distribution_list[number - 1] = total - reduce(lambda x, y: x + y, distribution_list[:number - 1])
        return distribution_list

    def index_operations_during_phases(self, namespaces, database=None, dataset='Employee', num_of_batches=1,
                                       defer_build_mix=False, phase='before', capella_run=False, query_node=None,
                                       batch_offset=0, timeout=1500, query_weight=1):
        if phase == 'before':
            self.create_indexes_in_batches(namespaces=namespaces, database=database,
                                           dataset=dataset, num_of_batches=num_of_batches,
                                           defer_build_mix=defer_build_mix, capella_run=capella_run,
                                           query_node=query_node, batch_offset=batch_offset)
        elif phase == 'during':
            self.run_query_workload_in_batches(database=database, namespaces=namespaces, capella_run=capella_run,
                                               query_node=query_node, timeout=timeout, query_weight=query_weight)
        elif phase == "after":
            self.cleanup_operations()

    def create_indexes_in_batches(self, namespaces, database=None, dataset='Employee', num_of_batches=1,
                                  defer_build_mix=False, capella_run=False, query_node=None,
                                  batch_offset=0):
        self.initial_index_num, create_list = 0, []
        for item in range(num_of_batches):
            for namespace in namespaces:
                counter = batch_offset + item
                prefix = f'idx_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}_batch_{counter}_'
                self.definition_list = self.get_index_definition_list(dataset=dataset, prefix=prefix)
                create_list = self.get_create_index_list(definition_list=self.definition_list, namespace=namespace,
                                                         defer_build_mix=defer_build_mix)
                self.log.info(f"Create index list: {create_list}")
                self.initial_index_num += len(create_list)
                self.create_gsi_indexes(create_queries=create_list, database=database,
                                        capella_run=capella_run, query_node=query_node)

        # results = self.run_query(database=database, query="select * from system:indexes")
        # self.log.info(f"system:indexes after create_indexes_in_batches is complete: {results}")

    def run_query_workload_in_batches(self, database, namespaces, capella_run, query_node, timeout, query_weight):
        start_time = datetime.datetime.now()
        select_queries = []
        while len(select_queries) < 100:
            for namespace in namespaces:
                select_queries.extend(
                    self.get_select_queries(definition_list=self.definition_list, namespace=namespace))
            select_queries = select_queries * query_weight
        with ThreadPoolExecutor() as executor:
            while True:
                tasks = []
                for query in select_queries:
                    if capella_run:
                        task = executor.submit(self.run_query, database=database, query=query)
                    else:
                        task = executor.submit(self.run_query, query=query, server=query_node)
                    tasks.append(task)

                for task in tasks:
                    task.result()
                curr_time = datetime.datetime.now()
                if (curr_time - start_time).total_seconds() > timeout:
                    break

    def cleanup_operations(self):
        pass

    def get_index_definition_list(self, dataset, prefix=None, skip_primary=False):
        if dataset == 'Person' or dataset == 'default':
            definition_list = self.generate_person_data_index_definition(index_name_prefix=prefix,
                                                                         skip_primary=skip_primary)
        elif dataset == 'Employee':
            definition_list = self.generate_employee_data_index_definition(index_name_prefix=prefix,
                                                                           skip_primary=skip_primary)
        elif dataset == 'Hotel':
            definition_list = self.generate_hotel_data_index_definition(index_name_prefix=prefix,
                                                                        skip_primary=skip_primary)
        elif dataset == 'Magma':
            definition_list = self.generate_magma_doc_loader_index_definition(index_name_prefix=prefix,
                                                                              skip_primary=skip_primary)
        else:
            raise Exception("Provide correct dataset. Valid values are Person, Employee, Magma, and Hotel")
        return definition_list

    def get_indexes_name(self, query_definitions):
        indexes_name = [q_d.index_name for q_d in query_definitions]
        return indexes_name
