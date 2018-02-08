import copy
import json

from base_2i import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection, RestHelper
import random
from lib import testconstants
from lib.memcached.helper.data_helper import MemcachedClientHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from threading import Thread
from pytests.query_tests_helper import QueryHelperTests
from gen_random_create_index import GenerateRandomCreateIndexStatements
from couchbase_helper.documentgenerator import JsonDocGenerator
from couchbase_helper.cluster import Cluster


class GSIIndexPartitioningTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(GSIIndexPartitioningTests, self).setUp()
        self.rest = RestConnection(self.servers[1])
        self.n1ql_server = self.get_nodes_from_services_map(
            service_type="n1ql")

        self.num_queries = self.input.param("num_queries", 100)

    def tearDown(self):
        super(GSIIndexPartitioningTests, self).tearDown()

    def test_create_partitioned_indexes(self):
        # Load Emp Dataset

        self.cluster.bucket_flush(self.master)

        self._kv_gen = JsonDocGenerator("emp_",
                                        encoding="utf-8",
                                        start=0,
                                        end=self.num_items)
        gen = copy.deepcopy(self._kv_gen)

        self._load_bucket(self.buckets[0], self.servers[0], gen, "create", 0)

        create_index_queries = self.generate_random_create_index_statements(
            bucketname=self.buckets[0].name, num_idx_nodes=3,
            num_statements=self.num_queries)

        failed_index_creation = 0
        for create_index_query in create_index_queries:

            try:
                self.n1ql_helper.run_cbq_query(
                    query=create_index_query["index_definition"],
                    server=self.n1ql_node)
            except Exception, ex:
                self.log.info(str(ex))

            self.sleep(10)

            index_metadata = self.rest.get_indexer_metadata()

            self.log.info("output from /getIndexStatus")
            self.log.info(index_metadata)

            self.log.info("Index Map")
            index_map = self.get_index_map()
            self.log.info(index_map)

            status = self.validate_partitioned_indexes(create_index_query,
                                                       index_map,
                                                       index_metadata)
            if not status:
                failed_index_creation += 1
                self.log.info(
                    "** Following query failed validation : {0}".format(
                        create_index_query["index_definition"]))

            drop_index_query = "DROP INDEX default.{0}".format(
                create_index_query["index_name"])
            try:
                self.n1ql_helper.run_cbq_query(
                    query=drop_index_query,
                    server=self.n1ql_node)
            except Exception, ex:
                self.log.info(str(ex))

            self.sleep(5)

        self.log.info(
            "Total Create Index Statements Run: {0}, Passed : {1}, Failed : {2}".format(
                self.num_queries, self.num_queries - failed_index_creation,
                failed_index_creation))
        self.assertTrue(failed_index_creation == 0,
                        "Some create index statements failed validations. Pls see the test log above for details.")

    def validate_partitioned_indexes(self, index_details, index_map,
                                     index_metadata):

        isIndexPresent = False
        isNumPartitionsCorrect = False
        isDeferBuildCorrect = False

        # Check if index exists
        for index in index_metadata["status"]:
            if index["name"] == index_details["index_name"]:
                isIndexPresent = True
                # If num-partitions are set, check no. of partitions
                expected_num_partitions = 16
                if index_details["num_partitions"] > 0:
                    expected_num_partitions = index_details["num_partitions"]

                if index["partitioned"] and index[
                    "numPartition"] == expected_num_partitions:
                    isNumPartitionsCorrect = True
                else:
                    self.log.info(
                        "Index {0} on /getIndexStatus : Partitioned={1}, num_partition={2}.. Expected numPartitions={3}".format(
                            index["name"], index["partitioned"],
                            index["numPartition"],
                            index_details["num_partitions"]))

                if index_details["defer_build"] == True and index[
                    "status"] == "Created":
                    isDeferBuildCorrect = True
                elif index_details["defer_build"] == False and index[
                    "status"] == "Ready":
                    isDeferBuildCorrect = True
                else:
                    self.log.info(
                        "Incorrect build status for index created with defer_build=True. Status for {0} is {1}".format(
                            index["name"], index["status"]))

        if not isIndexPresent:
            self.log.info("Index not listed in /getIndexStatus")

        return isIndexPresent and isNumPartitionsCorrect and isDeferBuildCorrect

    def generate_random_create_index_statements(self, bucketname="default",
                                                num_idx_nodes=4,
                                                num_statements=1):
        emp_fields = {
            'text': ["name", "dept", "languages_known", "email", "meta().id"],
            'number': ["mutated", "salary"],
            'boolean': ["is_manager"],
            'datetime': ["join_date"],
            'object': ["manages"]  # denote nested fields
        }

        emp_nested_fields = {
            'manages': {
                'text': ["reports"],
                'number': ["team_size"]
            }
        }

        index_variations_list = ["num_partitions", "num_replica", "defer_build",
                                 "partial_index", "primary_index"]

        all_emp_fields = ["name", "dept", "languages_known", "email", "mutated",
                          "salary", "is_manager", "join_date", "reports",
                          "team_size"]

        partition_key_type_list = ["leading_key", "trailing_key",
                                   "function_applied_key",
                                   "document_id", "function_applied_doc_id"]

        index_details = []

        for i in range(num_statements):

            random.seed()

            # 1. Generate a random no. of fields to be indexed
            num_index_keys = random.randint(1, len(all_emp_fields) - 1)

            # 2. Generate random fields
            index_fields = []
            for index in range(0, num_index_keys):
                index_field_list_idx = random.randint(0, len(
                    all_emp_fields) - 1)
                if all_emp_fields[
                    index_field_list_idx] not in index_fields:
                    index_fields.append(
                        all_emp_fields[index_field_list_idx])
                else:
                    # Generate a random index again
                    index_field_list_idx = random.randint(0,
                                                          len(
                                                              all_emp_fields) - 1)
                    if all_emp_fields[
                        index_field_list_idx] not in index_fields:
                        index_fields.append(
                            all_emp_fields[index_field_list_idx])

            # 3. Generate a random no. for no. of partition keys (this should be < #1)
            if num_index_keys > 1:
                num_partition_keys = random.randint(1, num_index_keys - 1)
            else:
                num_partition_keys = num_index_keys

            # 4. For each partition key, randomly select a partition key type from the list and generate a partition key with it
            partition_keys = []
            for index in range(num_partition_keys):
                key = None
                partition_key_type = partition_key_type_list[
                    random.randint(0, len(partition_key_type_list) - 1)]

                if partition_key_type == partition_key_type_list[0]:
                    key = index_fields[0]

                if partition_key_type == partition_key_type_list[1]:
                    if len(index_fields) > 1:
                        randval = random.randint(1, len(index_fields) - 1)
                        key = index_fields[randval]

                if partition_key_type == partition_key_type_list[2]:
                    idx_key = index_fields[
                        random.randint(0, len(index_fields) - 1)]

                    if idx_key in emp_fields["text"]:
                        key = ("LOWER({0})".format(idx_key))
                    elif idx_key in emp_fields["number"]:
                        key = ("({0} % 10) + ({0} * 2) ").format(idx_key)
                    elif idx_key in emp_fields["boolean"]:
                        key = ("NOT {0}".format(idx_key))
                    elif idx_key in emp_fields["datetime"]:
                        key = ("DATE_ADD_STR({0},-1,'year')".format(idx_key))
                    elif idx_key in emp_nested_fields["manages"]["text"]:
                        key = ("LOWER({0})".format(idx_key))
                    elif idx_key in emp_nested_fields["manages"]["number"]:
                        key = ("({0} % 10) + ({0} * 2)").format(idx_key)

                if partition_key_type == partition_key_type_list[3]:
                    key = "meta.id"

                if partition_key_type == partition_key_type_list[4]:
                    key = "SUBSTR(meta.id, POSITION(meta.id, '__')+2)"

                if key is not None and key not in partition_keys:
                    partition_keys.append(key)

            # 6. Choose other variation in queries from the list.
            num_index_variations = random.randint(0, len(
                index_variations_list) - 1)
            index_variations = []
            for index in range(num_index_variations):
                index_variation = index_variations_list[
                    random.randint(0, len(index_variations_list) - 1)]
                if index_variation not in index_variations:
                    index_variations.append(index_variation)

            # Primary indexes cannot be partial, so remove partial index if primary index is in the list
            if ("primary_index" in index_variations) and (
                        "partial_index" in index_variations):
                index_variations.remove("partial_index")

            # 7. Build create index queries.
            index_name = "idx" + str(random.randint(0, 1000000))
            if "primary_index" in index_variations:
                create_index_statement = "CREATE PRIMARY INDEX {0} on {1}".format(
                    index_name, bucketname)
            else:
                create_index_statement = "CREATE INDEX {0} on {1}(".format(
                    index_name, bucketname)
                create_index_statement += ",".join(index_fields) + ")"

            create_index_statement += " partition by hash("
            create_index_statement += ",".join(partition_keys) + ")"

            if "partial_index" in index_variations:
                create_index_statement += " where meta().id > 10"

            with_list = ["num_partitions", "num_replica", "defer_build"]

            num_partitions = 0
            num_replica = 0
            defer_build = False
            if (any(x in index_variations for x in with_list)):
                with_statement = []
                create_index_statement += " with {"
                if "num_partitions" in index_variations:
                    num_partitions = random.randint(4, 100)
                    with_statement.append(
                        "'num_partition':{0}".format(num_partitions))
                if "num_replica" in index_variations:
                    num_replica = random.randint(1, num_idx_nodes - 1)
                    with_statement.append(
                        "'num_replica':{0}".format(num_replica))
                if "defer_build" in index_variations:
                    defer_build = True
                    with_statement.append("'defer_build':true")
                create_index_statement += ",".join(with_statement) + "}"

            index_detail = {}
            index_detail["index_name"] = index_name
            index_detail["num_partitions"] = num_partitions
            index_detail["num_replica"] = num_replica
            index_detail["defer_build"] = defer_build
            index_detail["index_definition"] = create_index_statement

            index_details.append(index_detail)

        return index_details
