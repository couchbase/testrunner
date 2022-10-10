"""
gsi_utils.py: "This file contains methods for gsi index creation, drop, build and other utils

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = 04/10/22 11:53 am

"""
import datetime
import random
import uuid
from functools import reduce
from concurrent.futures import ThreadPoolExecutor

from couchbase_helper.query_definitions import QueryDefinition
from gsi.newtuq import QueryTests

RANGE_SCAN_TEMPLATE = "SELECT {0} FROM %s WHERE {1}"


class GSIUtils(QueryTests):
    def __init__(self):
        self.definition_list = []

    def generate_employee_data_index_definition(self, index_name_prefix=None):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "employee" + str(uuid.uuid4()).replace("-", "")

            # Single field GSI Query
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'name', index_fields=['name'],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", 'name = "employee-1" ')))

            # Primary Query
            definitions_list.append(
                QueryDefinition(index_name='#primary', index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", "test_rate > 1"), is_primary=True))

            # GSI index on multiple fields
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'join_day_&_job_tittle',
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
                                                                          'join_yr > 2010 AMD '
                                                                          'join_mo > 6')))

            # Paritioned Index
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['job_title'],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", 'job_title = "Support"'),
                                partition_by_fields=['job_title'], capella_run=False))

            # Array Index
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'array_index',
                                index_fields=['join_mo, All ARRAY vm.os FOR vm IN VMs END'],
                                query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                          'ANY vm IN VMs SATISFIES vm.os = "ubuntu" '
                                                                          'END')))

            return definitions_list

    def generate_hotel_data_index_definition(self, index_name_prefix=None):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "hotel" + str(uuid.uuid4()).replace("-", "")

            # Single field GSI Query
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'price', index_fields=['price'],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", "age > 0")))

            # Primary Query
            definitions_list.append(
                QueryDefinition(index_name='#primary', index_fields=[],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", "suffix is not NULL", is_primary=True)))

            # GSI index on multiple fields
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'free_breakfast_avg_rating',
                                index_fields=['free_breakfast', 'avg_rating'],
                                query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                          'avg_rating > 3 AND'
                                                                          'free_breakfast = true')))

            # GSI index with missing keys
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'missing_keys',
                                index_fields=['city', 'avg_rating', 'country'],
                                missing_indexes=True, missing_field_desc=True,
                                query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                          'avg_rating > 3 AND '
                                                                          'country like "%F%"')))

            # Paritioned Index
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['name'],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", 'name like "%W%"'),
                                partition_by_fields=['name'], capella_run=False))

            # Array Index
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'array_index_overall',
                                index_fields=['price, All ARRAY v.ratings.Overall FOR v IN reviews END'],
                                query_template=RANGE_SCAN_TEMPLATE.format("*", 'ANY v IN reviews SATISFIES v.ratings.'
                                                                               '`Overall` > 3  END and price < 1000 ')))

            # Array Index
            definitions_list.append(
                QueryDefinition(index_name=index_name_prefix + 'array_index_rooms',
                                index_fields=['price, All ARRAY v.ratings.Rooms FOR v IN reviews END'],
                                query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                          'ANY v IN reviews SATISFIES v.ratings.'
                                                                          '`Rooms` > 3  END and price > 1000 ')))
            return definitions_list

    def generate_person_data_index_definition(self, index_name_prefix=None):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "person" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'age', index_fields=['age'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", "age > 0")))

        # Primary Query
        definitions_list.append(
            QueryDefinition(index_name='#primary', index_fields=[],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", "suffix is not NULL"), is_primary=True))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'firstName_lastName', index_fields=['firstName', 'lastName'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'firstName like "%D% AND '
                                                                      'LastName is not NULL')))

        # GSI index with missing keys
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'missing_keys', index_fields=['age', 'city', 'country'],
                            missing_indexes=True, missing_field_desc=False,
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'firstName like "%D% AND '
                                                                      'LastName is not NULL')))

        # Paritioned Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['streetAddress'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'streetAddress is not NULL'),
                            partition_by_fields=['streetAddress'], capella_run=False))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index', index_fields=['filler1'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'filler1 like "%in%"')))
        return definitions_list

    def get_create_index_list(self, definition_list, namespace, defer_build_mix=False):
        create_index_list = []
        for index_gen in definition_list:
            if defer_build_mix:
                defer_build = random.choice([True, False])
            else:
                defer_build = False
            query = index_gen.generate_index_create_query(namespace=namespace, defer_build=defer_build)
            create_index_list.append(query)
        return create_index_list

    def get_select_queries(self, definition_list, namespace):
        select_query_list = []
        for index_gen in definition_list:
            query = index_gen.generate_query(bucket=namespace)
            select_query_list.append(query)
        return select_query_list

    def async_create_indexes(self, create_queries, database=None, capella_run=False, query_node=None):
        with ThreadPoolExecutor() as executor:
            tasks = []
            for query in create_queries:
                if capella_run:
                    task = executor.submit(self.run_cbq_query, query=query, server=query_node)
                else:
                    task = executor.submit(self.run_query, database=database, query=query)
                tasks.append(task)
        return tasks

    def range_unequal_distribution(self, number=4, factor=1.2, total=100000):
        """
        This method divides a range into unequal parts of given number
        """
        x = total * (1 - 1 / factor) / (factor ** number - 1)
        distribution_list = []
        for i in range(number):
            part = round(x * factor ** i)
            distribution_list.append(part)
        distribution_list[number - 1] = total - reduce(lambda x, y: x + y, distribution_list[:number - 1])
        return distribution_list

    def index_operations_during_phases(self, namespaces, dataset, num_of_batches=1, defer_build_mix=False,
                                       phase='before', capella_run=False, database=None, query_node=None,
                                       batch_offset=0, timeout=1500):
        if phase == 'before':
            for item in range(num_of_batches):
                for namespace in namespaces:
                    counter = batch_offset + item
                    prefix = f'{namespace}_batch_{counter}_'
                    if dataset == 'Person':
                        self.definition_list.append(self.generate_person_data_index_definition(index_name_prefix=prefix))
                    elif dataset == 'Employee':
                        self.definition_list.append(self.generate_employee_data_index_definition(index_name_prefix=prefix))
                    elif dataset == 'Hotel':
                        self.definition_list.append(self.generate_hotel_data_index_definition(index_name_prefix=prefix))
                    else:
                        raise Exception("Provide correct dataset. Valid values are Person, Employee and Hotel")
                    create_list = self.get_create_index_list(definition_list=self.definition_list, namespace=namespace,
                                                             defer_build_mix=defer_build_mix)
                    tasks = self.async_create_indexes(create_queries=create_list, database=database,
                                                      capella_run=capella_run, query_node=query_node)
                    for task in tasks:
                        task.result()
        elif phase == 'during':
            start_time = datetime.datetime.now()
            select_queries = []
            for namespace in namespaces:
                select_queries.extend(
                    self.get_select_queries(definition_list=self.definition_list, namespace=namespace))
            with ThreadPoolExecutor() as executor:
                counter = 0
                while True:
                    tasks = []
                    for query in select_queries:
                        if capella_run:
                            task = executor.submit(self.run_cbq_query, query=query, server=query_node)
                        else:
                            task = executor.submit(self.run_query, database=database, query=query)
                        tasks.append(task)

                    for task in tasks:
                        task.result()
                    curr_time = datetime.datetime.now()
                    if (curr_time - start_time).total_seconds() > timeout:
                        break
        elif phase == "after":
            pass