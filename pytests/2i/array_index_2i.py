import logging
import random
from string import lowercase

from couchbase.bucket import Bucket

from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.data import FIRST_NAMES, COUNTRIES
from couchbase_helper.documentgenerator import  DocumentGenerator
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.tuq_generators import TuqGenerators
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper

DATATYPES = [unicode, "scalar", int, dict, "missing", "empty", "null"]

log = logging.getLogger()

class SecondaryIndexArrayIndexTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexArrayIndexTests, self).setUp()
        self.doc_ops = self.input.param("doc_ops", True)
        self.index_field = self.input.param("index_field", "join_yr")
        self.restServer = self.get_nodes_from_services_map(service_type="index")
        self.rest = RestConnection(self.restServer)
        self.index_id_map = {}

    def tearDown(self):
        super(SecondaryIndexArrayIndexTests, self).tearDown()

    def test_create_query_drop_all_array_index(self):
        self.multi_create_index_using_rest(
            buckets=self.buckets, query_definitions=self.query_definitions)
        for bucket in self.buckets:
            for query_definition in self.query_definitions:
                self.run_full_table_scan_using_rest(bucket,
                                                    query_definition,
                                                    verify_result=True)
        self.multi_drop_index_using_rest(
            buckets=self.buckets, query_definitions=self.query_definitions)

    def test_simple_indexes_mutation(self):
        self.multi_create_index_using_rest(
            buckets=self.buckets,
            query_definitions=self.query_definitions)
        doc_list = self.full_docs_list[:len(self.full_docs_list)/2]
        for query_definition in self.query_definitions:
            index_field, data_type = self._find_datatype(query_definition)
            if index_field:
                for bucket in self.buckets:
                    for data in DATATYPES:
                        if data_type is data:
                            continue
                        else:
                            log.info("Changing datatype of {0} to {1}".format(index_field, data))
                            self.change_index_field_type(
                                bucket.name, index_field, doc_list,
                                data, query_definition)
                            #self.gen_results = TuqGenerators(self.log, self.full_docs_list)
                            self.query_using_index(bucket, query_definition, verify_results=False)
                            self.full_docs_list = self.generate_full_docs_list(self.gens_load)
        self.multi_drop_index_using_rest(buckets=self.buckets,
                                         query_definitions=self.query_definitions)

    def test_composite_indexes_mutation(self):
        definitions_list = []
        if not self.dataset is "array":
            pass
        else:
            query_definition = QueryDefinition(index_name="index_name_travel_history_leading",
                                                index_fields=["DISTINCT ARRAY t FOR t in `travel_history` END, name, age"],
                                                query_template="SELECT {0} FROM %s WHERE `travel_history` IS NOT NULL",
                                                groups=["array"], index_where_clause=" `travel_history` IS NOT NULL ")
            definitions_list.append(query_definition)
            query_definition = QueryDefinition(index_name="index_name_travel_history_non_leading_end",
                                                index_fields=["name, age, DISTINCT ARRAY t FOR t in `travel_history` END"],
                                                query_template="SELECT {0} FROM %s WHERE `travel_history` IS NOT NULL",
                                                groups=["array"], index_where_clause=" `travel_history` IS NOT NULL ")
            definitions_list.append(query_definition)
            query_definition = QueryDefinition(index_name="index_name_travel_history_non_leading_middle",
                                                index_fields=["name, DISTINCT ARRAY t FOR t in `travel_history` END", "age"],
                                                query_template="SELECT {0} FROM %s WHERE `travel_history` IS NOT NULL",
                                                groups=["array"], index_where_clause=" `travel_history` IS NOT NULL ")
            definitions_list.append(query_definition)
            self.multi_create_index_using_rest(buckets=self.buckets, query_definitions=definitions_list)
            for query_definition in definitions_list:
                index_field, data_type = self._find_datatype(query_definition)
                if index_field:
                    for bucket in self.buckets:
                        for data in DATATYPES:
                            if data_type is data:
                                continue
                            else:
                                log.info("Changing datatype of {0} to {1}".format(index_field, data))
                                doc_list = self.full_docs_list[:len(self.full_docs_list)/2]
                                self.change_index_field_type(bucket.name, index_field, doc_list, data)
                                self.run_full_table_scan_using_rest(bucket, query_definition, verify_result=True)
                                self.full_docs_list = self.generate_full_docs_list(self.gens_load)
            self.multi_drop_index_using_rest(buckets=self.buckets, query_definitions=definitions_list)

    def test_create_query_drop_index_on_missing_empty_null_field(self):
        data_types = ["empty", "null"]
        index_field, data_type = self._find_datatype(self.query_definitions[0])
        doc_list = self.full_docs_list[:len(self.full_docs_list)/2]
        for data_type in data_types:
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            definitions_list = []
            query_definition =  QueryDefinition(index_name="index_name_{0}_distinct".format(data_type),
                                                index_fields=["DISTINCT ARRAY t FOR t in `{0}` END".format(index_field)],
                                                query_template="SELECT {0} FROM %s WHERE `{0}` IS NOT NULL".format(index_field),
                                                groups=["array"], index_where_clause=" `{0}` IS NOT NULL ".format(index_field))
            definitions_list.append(query_definition)
            query_definition =  QueryDefinition(index_name="index_name_{0}_duplicate".format(data_type),
                                                index_fields=["ALL ARRAY t FOR t in `{0}` END".format(index_field)],
                                                query_template="SELECT {0} FROM %s WHERE `{0}` IS NOT NULL".format(index_field),
                                                groups=["array"], index_where_clause=" `{0}` IS NOT NULL ".format(index_field))
            definitions_list.append(query_definition)
            for bucket in self.buckets:
                for query_definition in definitions_list:
                    self.change_index_field_type(bucket.name, index_field, doc_list, data_type, query_definition)
            self.multi_create_index_using_rest(buckets=self.buckets, query_definitions=definitions_list)
            for bucket in self.buckets:
                for query_definition in definitions_list:
                    self.run_full_table_scan_using_rest(bucket, query_definition)
            self.multi_drop_index_using_rest(buckets=self.buckets, query_definitions=definitions_list)
            self.full_docs_list = self.generate_full_docs_list(self.gens_load)

    def test_create_query_drop_index_on_mixed_datatypes(self):
        query_definition = QueryDefinition(
            index_name="index_name_travel_history",
            index_fields=["DISTINCT ARRAY t FOR t in `travel_history` END"],
            query_template="SELECT {0} FROM %s WHERE `travel_history` IS NOT NULL",
            groups=["array"], index_where_clause=" `travel_history` IS NOT NULL ")
        end = 0
        for bucket in self.buckets:
            for data in DATATYPES:
                start = end
                end = end + len(self.full_docs_list)/len(DATATYPES)
                doc_list = self.full_docs_list[start:end]
                self.change_index_field_type(bucket.name,
                                             "travel_history",
                                             doc_list, data, query_definition)
        self.multi_create_index_using_rest(buckets=self.buckets,
                                           query_definitions=[query_definition])
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        for bucket in self.buckets:
            self.run_full_table_scan_using_rest(bucket, query_definition)
        self.multi_drop_index_using_rest(buckets=self.buckets,
                                         query_definitions=[query_definition])

    def test_lookup_array_index(self):
        secExpr = ["ALL DISTINCT countries"]
        log.info("Creating index index_name_1 on {0}...".format(self.buckets[0]))
        id = self._create_rest_array_index("index_name_1", self.buckets[0], secExpr)
        self.assertIsNotNone(id, "Array Index is not created.")
        log.info("Array Index index_name_1 on field {0} is created.".format(self.index_field))
        body = {"equal": "[\"Netherlands\"]"}
        content = self.rest.lookup_gsi_index_with_rest(id, body)
        self.assertIsNotNone(content, "Lookup not performed")

    def test_create_query_flush_bucket(self):
        self.multi_create_index_using_rest(buckets=self.buckets, query_definitions=self.query_definitions)
        log.info("Flushing bucket {0}...".format(self.buckets[0]))
        self.rest.flush_bucket(self.buckets[0])
        log.info("Performing Full Table Scan...")
        for query_definition in self.query_definitions:
            self.run_full_table_scan_using_rest(self.buckets[0], query_definition)
        self.sleep(60)
        self.multi_drop_index_using_rest(buckets=self.buckets,
                                         query_definitions=self.query_definitions)

    def test_create_query_drop_bucket(self):
        self.multi_create_index_using_rest(buckets=self.buckets, query_definitions=self.query_definitions)
        log.info("Deleting bucket {0}...".format(self.buckets[0]))
        BucketOperationHelper.delete_bucket_or_assert(serverInfo=self.restServer, bucket=self.buckets[0].name)
        log.info("Performing Full Table Scan...")
        # for query_definition in self.query_definitions:
        #     self.run_full_table_scan_using_rest(self.buckets[1], query_definition)
        buckets = self.buckets[1:]
        if buckets:
            self.multi_drop_index_using_rest(buckets=buckets,
                                             query_definitions=self.query_definitions)

    def test_increase_array_size(self):
        query_definition =  QueryDefinition(
            index_name="index_name_big_values",
            index_fields=["DISTINCT ARRAY t FOR t in bigValues END"],
            query_template="SELECT {0} FROM %s WHERE bigValues IS NOT NULL",
            groups=["array"], index_where_clause=" bigValues IS NOT NULL ")
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket.name)
        sec_key_size = 1000
        self._change_array_size(sec_key_size)
        generators = []
        template = '{{"name":"{0}", "age":{1}, "bigValues":{2} }}'
        for i in range(10):
            name = FIRST_NAMES[random.choice(range(len(FIRST_NAMES)))]
            id = "{0}-{1}".format(name, str(i))
            age = random.choice(range(4, 19))
            bigValues = []
            arrLen = random.choice(range(10, 15))
            indiSize = (sec_key_size * 4 / arrLen) + 50
            for j in range(arrLen):
                longStr = "".join(random.choice(lowercase) for k in range(indiSize))
                bigValues.append(longStr)
            generators.append(DocumentGenerator(id, template, [name], [age], [bigValues],
                                                start=0, end=1))
        self.load(generators, flag=self.item_flag, verify_data=False, batch_size=self.batch_size)
        self.full_docs_list = self.generate_full_docs_list(generators)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        self.multi_create_index_using_rest(buckets=self.buckets,
                                           query_definitions=[query_definition])
        for bucket in self.buckets:
            self.run_full_table_scan_using_rest(bucket, query_definition)
        sec_key_size = 4000
        self._change_array_size(sec_key_size)
        self.sleep(15)
        self.load(generators, flag=self.item_flag, verify_data=False, batch_size=self.batch_size)
        for bucket in self.buckets:
            self.run_full_table_scan_using_rest(bucket, query_definition)
        self.multi_drop_index_using_rest(buckets=self.buckets,
                                         query_definitions=[query_definition])

    def test_decrease_array_size(self):
        query_definition =  QueryDefinition(
            index_name="index_name_big_values",
            index_fields=["DISTINCT ARRAY t FOR t in bigValues END"],
            query_template="SELECT {0} FROM %s WHERE bigValues IS NOT NULL",
            groups=["array"], index_where_clause=" bigValues IS NOT NULL ")
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket.name)
        sec_key_size = 4000
        self._change_array_size(sec_key_size)
        generators = []
        template = '{{"name":"{0}", "age":{1}, "bigValues":{2} }}'
        for i in range(10):
            name = FIRST_NAMES[random.choice(range(len(FIRST_NAMES)))]
            id = "{0}-{1}".format(name, str(i))
            age = random.choice(range(4, 19))
            bigValues = []
            arrLen = random.choice(range(10, 15))
            indiSize = (sec_key_size * 4 / arrLen) + 50
            for j in range(arrLen):
                longStr = "".join(random.choice(lowercase) for k in range(indiSize))
                bigValues.append(longStr)
            generators.append(DocumentGenerator(id, template, [name], [age], [bigValues],
                                                start=0, end=1))
        self.load(generators, flag=self.item_flag, verify_data=False, batch_size=self.batch_size)
        self.full_docs_list = self.generate_full_docs_list(generators)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        self.multi_create_index_using_rest(buckets=self.buckets,
                                           query_definitions=[query_definition])
        self.run_full_table_scan_using_rest(self.buckets[0], query_definition)
        sec_key_size = 1000
        self._change_array_size(sec_key_size)
        self.sleep(15)
        self.load(generators, flag=self.item_flag, verify_data=False, batch_size=self.batch_size)
        self.run_full_table_scan_using_rest(self.buckets[0], query_definition)
        self.multi_drop_index_using_rest(buckets=self.buckets,
                                         query_definitions=[query_definition])

    def test_array_item_limit(self):
        query_definition =  QueryDefinition(index_name="index_name_big_values",
                                                index_fields=["DISTINCT ARRAY t FOR t in bigValues END"],
                                                query_template="SELECT {0} FROM %s WHERE bigValues IS NOT NULL",
                                                groups=["array"], index_where_clause=" bigValues IS NOT NULL ")
        self.rest.flush_bucket(self.buckets[0])
        generators = []
        template = '{{"name":"{0}", "age":{1}, "bigValues":{2} }}'
        for i in range(10):
            name = FIRST_NAMES[random.choice(range(len(FIRST_NAMES)))]
            id = "{0}-{1}".format(name, str(i))
            age = random.choice(range(4, 19))
            bigValues = []
            arrLen = random.choice(range(10, 15))
            indiSize = (4096 * 4)
            for j in range(arrLen):
                longStr = "".join(random.choice(lowercase) for k in range(indiSize))
                bigValues.append(longStr)
            generators.append(DocumentGenerator(id, template, [name], [age], [bigValues],
                                                start=0, end=1))
        self.load(generators, flag=self.item_flag, verify_data=False, batch_size=self.batch_size)
        self.full_docs_list = self.generate_full_docs_list(generators)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        self.multi_create_index_using_rest(buckets=self.buckets,
                                           query_definitions=[query_definition])
        for bucket in self.buckets:
            self.run_full_table_scan_using_rest(bucket, query_definition)
        self.multi_drop_index_using_rest(buckets=self.buckets,
                                         query_definitions=[query_definition])

    def _update_document(self, bucket_name, key, document):
        bucket = Bucket('couchbase://{ip}/{name}'.format(ip=self.master.ip, name=bucket_name))
        bucket.upsert(key, document)

    def _find_datatype(self, query_definition):
        for field in query_definition.index_fields:
            index_field = field.split("in ")[1].split(" END")[0].replace("`", "")
            if "TO_ARRAY" in index_field:
                index_field = index_field.split("TO_ARRAY(")[1].split(r")")[0]
            if index_field:
                if type(self.full_docs_list[0][index_field]) is list:
                    return index_field, type(self.full_docs_list[0][index_field][0])
                else:
                    return index_field, "scalar"
            else:
                return None, None

    def change_index_field_type(self, bucket_name, index_field,
                                doc_list, data_type, query_definition):
        if data_type is unicode:
            for doc in doc_list:
                doc[index_field] = [random.choice(FIRST_NAMES) for i in range(10)]
                self._update_document(bucket_name, doc["_id"], doc)
                for i in range(len(self.full_docs_list)):
                    if doc["_id"] == self.full_docs_list[i]["_id"]:
                        self.full_docs_list[i] = doc
                        continue
            query_definition.query_template = 'SELECT * FROM %s WHERE ANY t IN `{0}` SATISFIES t = "{1}" END ORDER BY _id'.\
                format(index_field, random.choice(FIRST_NAMES))
        if data_type is int:
            for doc in doc_list:
                doc[index_field] = [random.randint(1000000, 9999999) for i in range(10)]
                self._update_document(bucket_name, doc["_id"], doc)
                for i in range(len(self.full_docs_list)):
                    if doc["_id"] == self.full_docs_list[i]["_id"]:
                        self.full_docs_list[i] = doc
                        continue
            query_definition.query_template = "SELECT * FROM %s WHERE ANY t IN `{0}` SATISFIES t > {1} END ORDER BY _id".\
                format(index_field, random.randint(1000000, 9999999))
        if data_type is dict:
            for doc in doc_list:
                doc[index_field] = [{"first_name" : random.choice(FIRST_NAMES),
                                     "country": random.choice(COUNTRIES)} for i in range(10)]
                self._update_document(bucket_name, doc["_id"], doc)
                for i in range(len(self.full_docs_list)):
                    if doc["_id"] == self.full_docs_list[i]["_id"]:
                        self.full_docs_list[i] = doc
                        continue
            query_definition.query_template = "SELECT * FROM %s WHERE ANY t IN `{0}` SATISFIES t > {1} END ORDER BY _id".\
                format(index_field, {"first_name" : random.choice(FIRST_NAMES),
                                     "country": random.choice(COUNTRIES)})
        if data_type is "null":
            for doc in doc_list:
                doc[index_field] = None
                self._update_document(bucket_name, doc["_id"], doc)
                for i in range(len(self.full_docs_list)):
                    if doc["_id"] == self.full_docs_list[i]["_id"]:
                        self.full_docs_list[i] = doc
                        continue
            query_definition.query_template = "SELECT * FROM %s WHERE ANY t IN `{0}` SATISFIES t = NULL END ORDER BY _id".\
                format(index_field)
        if data_type is "empty":
            for doc in doc_list:
                doc[index_field] = []
                self._update_document(bucket_name, doc["_id"], doc)
                for i in range(len(self.full_docs_list)):
                    if doc["_id"] == self.full_docs_list[i]["_id"]:
                        self.full_docs_list[i] = doc
                        continue
        if data_type is "missing":
            for doc in doc_list:
                doc.pop(index_field, None)
                self._update_document(bucket_name, doc["_id"], doc)
                for i in range(len(self.full_docs_list)):
                    if doc["_id"] == self.full_docs_list[i]["_id"]:
                        self.full_docs_list[i] = doc
                        continue
        if data_type is "scalar":
            for doc in doc_list:
                doc[index_field] = random.choice(FIRST_NAMES)
                self._update_document(bucket_name, doc["_id"], doc)
                for i in range(len(self.full_docs_list)):
                    if doc["_id"] == self.full_docs_list[i]["_id"]:
                        self.full_docs_list[i] = doc
                        break
            query_definition.query_template = 'SELECT * FROM %s WHERE ANY t IN TO_ARRAY(`{0}`) SATISFIES t = "{1}" END ORDER BY _id'.\
                format(index_field, random.choice(FIRST_NAMES))

    def _change_array_size(self, array_size):
        doc = {"indexer.settings.max_array_seckey_size": array_size}
        self.rest.set_index_settings(doc)
