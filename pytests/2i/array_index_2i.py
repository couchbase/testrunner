import logging
import random
from couchbase.bucket import Bucket

from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from base_2i import BaseSecondaryIndexingTests

log = logging.getLogger(__name__)

class SecondaryIndexArrayIndexTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexArrayIndexTests, self).setUp()
        self.doc_ops = self.input.param("doc_ops", True)
        self.index_field = self.input.param("index_field", "join_yr")
        self.restServer = self.get_nodes_from_services_map(service_type="index")
        self.rest = RestConnection(self.restServer)

    def tearDown(self):
        super(SecondaryIndexArrayIndexTests, self).tearDown()

    def test_create_query_drop_array_index(self):
        secExpr = ["ALL DISTINCT {0}".format(self.index_field)]
        id = self._create_rest_array_index("index_name_1", self.buckets[0], secExpr)
        self.assertIsNotNone(id, "Array Index is not created.")
        log.info("Array Index index_name_1 on field {0} is created.".format(self.index_field))
        log.info("Performing Full Table Scan...")
        body = {'stale': 'ok'}
        content = self.rest.full_table_scan_gsi_index_with_rest(id, body)
        self.assertIsNotNone(content, "Table Scan not performed")
        log.info("Dropping 'index_name_1'...")
        self.rest.drop_index_with_rest(id)

    def test_create_query_drop_composite_index(self):
        secExpr = ["ALL DISTINCT {0}".format(self.index_field), "email", "hobbies"]
        for i in range(10):
            random.shuffle(secExpr)
            index_name = "index_name_{0}".format(i)
            log.info("Composite index {0} creating on {1}".format(index_name, secExpr))
            id = self._create_rest_array_index(index_name, self.buckets[0], secExpr)
            self.assertIsNotNone(id, "Array Index is not created.")
            log.info("Array Index {0} on field {1} is created.".format(index_name, secExpr))
            log.info("Performing Full Table Scan...")
            body = {'stale': 'ok'}
            content = self.rest.full_table_scan_gsi_index_with_rest(id, body)
            self.assertIsNotNone(content, "Table Scan not performed")
            log.info("Dropping '{0}'...".format(index_name))
            self.rest.drop_index_with_rest(id)

    def test_lookup_array_index(self):
        secExpr = ["ALL DISTINCT {0}".format(self.index_field)]
        log.info("Creating index index_name_1 on {0}...".format(self.buckets[0]))
        id = self._create_rest_array_index("index_name_1", self.buckets[0], secExpr)
        self.assertIsNotNone(id, "Array Index is not created.")
        log.info("Array Index index_name_1 on field {0} is created.".format(self.index_field))
        body = {"equal": "[\"Netherlands\"]"}
        content = self.rest.lookup_gsi_index_with_rest(id, body)
        self.assertIsNotNone(content, "Lookup not performed")

    def test_create_query_flush_bucket(self):
        secExpr = ["ALL DISTINCT {0}".format(self.index_field)]
        id = self._create_rest_array_index("index_name_1", self.buckets[0], secExpr)
        self.assertIsNotNone(id, "Array Index is not created.")
        log.info("Array Index index_name_1 on field {0} is created.".format(self.index_field))
        log.info("Performing Full Table Scan...")
        body = {'stale': 'ok'}
        content = self.rest.full_table_scan_gsi_index_with_rest(id, body)
        self.assertIsNotNone(content, "Table Scan not performed")
        log.info("Flushing bucket {0}...".format(self.buckets[0]))
        self.rest.flush_bucket(self.buckets[0])
        log.info("Performing Full Table Scan...")
        content = self.rest.full_table_scan_gsi_index_with_rest(id, body)
        self.assertIsNotNone(content, "Table Scan failed after flushing bucket {0}".format(self.buckets[0]))

    def test_create_query_drop_bucket(self):
        secExpr = ["ALL DISTINCT {0}".format(self.index_field)]
        id = self._create_rest_array_index("index_name_1", self.buckets[0], secExpr)
        self.assertIsNotNone(id, "Array Index is not created.")
        log.info("Array Index index_name_1 on field {0} is created.".format(self.index_field))
        log.info("Performing Full Table Scan...")
        body = {'stale': 'ok'}
        content = self.rest.full_table_scan_gsi_index_with_rest(id, body)
        self.assertIsNotNone(content, "Table Scan not performed")
        log.info("Deleting bucket {0}...".format(self.buckets[0]))
        BucketOperationHelper.delete_bucket_or_assert(serverInfo=self.restServer, bucket=self.buckets[0].name)
        self.sleep(10)
        self.assertIsNone(self._check_index_status(id, "index_name_1"), "Index still exists after dropping the bucket.")

    def test_remove_array_field(self):
        secExpr = ["ALL DISTINCT {0}".format(self.index_field)]
        log.info("Creating index index_name_1 on {0}...".format(self.buckets[0]))
        id = self._create_rest_array_index("index_name_1", self.buckets[0], secExpr)
        self.assertIsNotNone(id, "Array Index is not created.")
        log.info("Array Index index_name_1 on field {0} is created.".format(self.index_field))
        body = {'stale': 'ok'}
        content = self.rest.full_table_scan_gsi_index_with_rest(id, body)
        self.assertIsNotNone(content, "Table Scan not performed")
        log.info("Removing {0} from all documents in bucket {1}...".format(self.index_field, self.buckets[0]))
        self._flush_field(self.buckets[0])
        self.sleep(10)
        log.info("Performing full table scan after removing array index field")
        content = self.rest.full_table_scan_gsi_index_with_rest(id, body)
        self.assertIsNotNone(content, "Table Scan failed after removing array index field")
        log.info("Dropping index_name_1...")
        self.rest.drop_index_with_rest(id)

    def test_change_array_field(self):
        secExpr = ["ALL DISTINCT {0}".format(self.index_field)]
        log.info("Creating index index_name_1 on {0}...".format(self.buckets[0]))
        id = self._create_rest_array_index("index_name_1", self.buckets[0], secExpr)
        self.assertIsNotNone(id, "Array Index is not created.")
        log.info("Array Index index_name_1 on field {0} is created.".format(self.index_field))
        body = {'stale': 'ok'}
        content = self.rest.full_table_scan_gsi_index_with_rest(id, body)
        self.assertIsNotNone(content, "Table Scan not performed")
        log.info("Changing type of {0} from all documents in bucket {1}...".format(self.index_field, self.buckets[0]))
        self._change_field(self.buckets[0])
        self.sleep(10)
        log.info("Performing full table scan after removing array index field")
        content = self.rest.full_table_scan_gsi_index_with_rest(id, body)
        self.assertIsNotNone(content, "Table Scan failed after changing array index field")
        log.info("Dropping index_name_1...")
        self.rest.drop_index_with_rest(id)

    def _create_rest_array_index(self, ind_name,bucket,secExprs,
                           exprType='N1QL',partnExpr='',whereExpr='',
                           isPrimary=False,withExpr=''):
        """
        Creates index using rest API
        :param ind_name:
        :param bucket: Object of class bucket
        :param gsi_type:
        :param secExprs:
        :param exprType:
        :param partnExpr:
        :param whereExpr:
        :param isPrimary:
        :param withExpr:
        :return:
        """
        ind_content = {}
        ind_content["name"] = "{0}".format(ind_name)
        ind_content["bucket"] = "{0}".format(bucket.name)
        if self.gsi_type == 'memory_optimized':
            ind_content["using"] = "memory_optimized"
        else:
            ind_content["using"] = "forestdb"
        ind_content["exprType"] = "{0}".format(exprType)
        ind_content["whereExpr"] = "{0}".format(whereExpr)
        ind_content["secExprs"] = secExprs
        ind_content["isPrimary"] = isPrimary
        ind_content["with"] = "{0}".format(withExpr)

        log.info("Creating index {0}...".format(ind_name))
        ind_id = self.rest.create_index_with_rest(ind_content)
        return self._check_index_status(ind_id['id'], ind_name)

    def _check_index_status(self, id, name):
        content = self.rest.get_all_indexes_with_rest()
        if id in content.keys():
            if content[id]['definitions']['name'] == name:
                if content[id]['instances'][0]['state'] == 'INDEX_STATE_ACTIVE':
                    return id
        return None

    def _flush_field(self, bucket_name):
        for key, doc in self._get_documets(bucket_name, self.index_field):
            if self.index_field in doc.keys():
                del(doc[self.index_field])
                self._update_document(bucket_name, key, doc)

    def _change_field(self, bucket_name):
        count = 0
        for doc in self.full_docs_list:
            if self.index_field in doc.keys():
                if isinstance(doc[self.index_field], list):
                    doc[self.index_field] = "{0}".format(self.index_field) + str(count)
                else:
                    doc[self.index_field] = ["{0}".format(self.index_field) + str(count)]
                count += 1
                self._update_document(bucket_name, doc["_id"], doc)

    def _update_document(self, bucket_name, key, document):
        bucket = Bucket('couchbase://{ip}/{name}'.format(ip=self.master.ip, name=bucket_name))
        bucket.upsert(key, document)

    def _get_documets(self, bucket_name, field):
        bucket = Bucket('couchbase://{ip}/{name}'.format(ip=self.master.ip, name=bucket_name))
        if not bucket:
            log.info("Bucket connection is not established.")
        log.info("Updating {0} in all documents in bucket {1}...".format(field, bucket_name))
        query = "SELECT * FROM {0}".format(bucket_name)
        for row in bucket.n1ql_query(query):
            yield row[bucket.bucket]['_id'], bucket.get(key=row[bucket.bucket]['_id']).value