"""
gsi_utils.py: "This file contains methods for gsi index creation, drop, build and other utils

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = 04/10/22 11:53 am

"""
import boto3
import datetime
import random
import string
import uuid
import logger
import re
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

    def generate_magma_doc_loader_index_definition(self, index_name_prefix=None):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "docloader" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'name', index_fields=['name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'name like "%%T" ')))

        # Primary Query
        prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
        definitions_list.append(
            QueryDefinition(index_name=prim_index_name, index_fields=[],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", "attributes.dimensions.height > 40"),
                            is_primary=True))

        # GSI index on multiple fields
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'age_gender',
                            index_fields=['age, gender'],
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
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['body'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'body like "%%E%%"'),
                            partition_by_fields=['body'], capella_run=True))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index',
                            index_fields=['mutated, ALL ARRAY h.name FOR h IN attributes.hobbies END'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'mutated >= 0 and '
                                                                      'ANY h IN attributes.hobbies SATISFIES'
                                                                      ' h.name = "Books" END')))

        # Array Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'array_index_2',
                            index_fields=['age, ALL animals'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*",
                                                                      'any a in animals satisfies '
                                                                      'a = "Forest green (traditional)" end')))

        self.batch_size = len(definitions_list)
        return definitions_list

    def generate_employee_data_index_definition(self, index_name_prefix=None):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "employee" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'name', index_fields=['name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'name = "employee-1" ')))

        # Primary Query
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

    def generate_hotel_data_index_definition(self, index_name_prefix=None):
        definitions_list = []
        if not index_name_prefix:
            index_name_prefix = "hotel" + str(uuid.uuid4()).replace("-", "")

        # Single field GSI Query
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'price', index_fields=['price'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", "age > 0")))

        # Primary Query
        prim_index_name = f'#primary_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}'
        definitions_list.append(
            QueryDefinition(index_name=prim_index_name, index_fields=[],
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
                                                                      'country like "%%F%%"')))

        # Paritioned Index
        definitions_list.append(
            QueryDefinition(index_name=index_name_prefix + 'partitioned_index', index_fields=['name'],
                            query_template=RANGE_SCAN_TEMPLATE.format("*", 'name like "%%W%%"'),
                            partition_by_fields=['name'], capella_run=True))

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
        self.batch_size = len(definitions_list)
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

    def get_create_index_list(self, definition_list, namespace, defer_build_mix=False):
        create_index_list = []
        for index_gen in definition_list:
            if defer_build_mix:
                defer_build = random.choice([True, False])
            else:
                defer_build = False
            index_gen = index_gen
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
                prefix = f'idx_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}' \
                         f'_batch_{counter}_'
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

    def check_s3_cleanup(self, aws_access_key_id, aws_secret_access_key, s3_bucket='gsi-onprem', region='us-west-1'):
        s3 = boto3.client(service_name='s3', region_name=region,
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
        result = s3.list_objects_v2(Bucket=s3_bucket, Delimiter='/*')
        if len(result['Contents']) > 1:
            raise Exception("Bucket is not cleaned up after rebalance.")

    def cleanup_operations(self):
        pass

    def check_s3_cleanup(self, aws_access_key_id, aws_secret_access_key, s3_bucket='gsi-onprem', region='us-west-1',
                         storage_prefix='indexing'):
        s3 = boto3.client(service_name='s3', region_name=region,
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
        result = s3.list_objects_v2(Bucket=s3_bucket, Delimiter='/*')
        folder_path_expected = f"{storage_prefix}/"
        self.log.info(f"Expected folder list {folder_path_expected}")
        folder_list_on_aws = []
        self.log.info(f"Result from the s3 list_objects_v2 API call:{result}")
        for item in result['Contents']:
            if folder_path_expected in item['Key']:
                folder_list_on_aws.append(item['Key'])
        if len(folder_list_on_aws) > 1:
            raise Exception("Bucket is not cleaned up after rebalance.")

    def get_index_definition_list(self, dataset, prefix=None):
        if dataset == 'Person' or dataset == 'default':
            definition_list = self.generate_person_data_index_definition(index_name_prefix=prefix)
        elif dataset == 'Employee':
            definition_list = self.generate_employee_data_index_definition(index_name_prefix=prefix)
        elif dataset == 'Hotel':
            definition_list = self.generate_hotel_data_index_definition(index_name_prefix=prefix)
        elif dataset == 'Magma':
            definition_list = self.generate_magma_doc_loader_index_definition(index_name_prefix=prefix)
        else:
            raise Exception("Provide correct dataset. Valid values are Person, Employee, Magma, and Hotel")
        return definition_list
