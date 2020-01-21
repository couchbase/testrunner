from .tuq import QueryTests
from membase.api.rest_client import RestHelper
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
import couchbase.subdocument as SD
from deepdiff import DeepDiff

class QueryXattrTests(QueryTests):
    def setUp(self):
        super(QueryXattrTests, self).setUp()
        self.log.info("==============  QueryXattrTests setup has started ==============")
        self.system_xattr_data = []
        self.user_xattr_data = []
        self.meta_ids = []
        self.ensure_primary_indexes_exist()
        self.log.info("==============  QueryXattrTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryXattrTests, self).suite_setUp()
        self.log.info("==============  QueryXattrTests suite_setup has started ==============")
        self.log.info("==============  QueryXattrTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log.info("==============  QueryXattrTests tearDown has started ==============")
        self.log.info("==============  QueryXattrTests tearDown has completed ==============")
        super(QueryXattrTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryXattrTests suite_tearDown has started ==============")
        self.log.info("==============  QueryXattrTests suite_tearDown has completed ==============")
        super(QueryXattrTests, self).suite_tearDown()

    """
    Test system xattr queries with primary index:
        1. full path - single value
        2. full path - json value
        3. full path nested json value
        4. partial path single level
        5. partial path multi level
        6. mulitple paths - full, partial - single level, partial - multi level
        7. full path before/after deletion
    """
    def test_system_xattr_primary_index(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        # full path query
        self.run_xattrs_query("SELECT meta().xattrs._system1 FROM default", "", "_system1", "", "default", xattr_data=self.system_xattr_data, primary_compare=False)

        # full path query
        self.run_xattrs_query("SELECT meta().xattrs._system2 FROM default", "", "_system2", "", "default", xattr_data=self.system_xattr_data, primary_compare=False)

        # full path query
        compare_1 = self.run_xattrs_query("SELECT meta().xattrs._system3 FROM default", "", "_system3", "", "default", xattr_data=self.system_xattr_data, primary_compare=False)

        # partial path query
        compare_2 = self.run_xattrs_query("SELECT meta().xattrs._system3.field1 FROM default", "", "_system3", "", "default", compare_fields=["field1"], xattr_data=self.system_xattr_data, primary_compare=False)

        # nested partial path query
        compare_3 = self.run_xattrs_query("SELECT meta().xattrs._system3.field1.sub_field1a FROM default", "", "_system3", "", "default", compare_fields=["field1", "sub_field1a"], xattr_data=self.system_xattr_data, primary_compare=False)

        # multiple paths single xattr query
        query_response = self.run_cbq_query("SELECT meta().xattrs._system3, meta().xattrs._system3.field1, meta().xattrs._system3.field1.sub_field1a FROM default")
        docs = query_response['results']
        self.log.info("XAttrs: " + str(docs[0:5]))
        compare = []
        for i in range(0, len(docs)):
            compare.append({'_system3': compare_1[i]['_system3'], 'field1': compare_2[i]['field1'], 'sub_field1a': compare_3[i]['sub_field1a']})
        self.log.info("Compare: " + str(compare[0:5]))
        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # multiple paths single xattr and data query
        query_response = self.run_cbq_query("SELECT meta().xattrs._system3, meta().xattrs._system3.field1, meta().xattrs._system3.field1.sub_field1a, join_day FROM default")
        docs = query_response['results']
        self.log.info("XAttrs: " + str(docs[0:5]))
        compare_4 = self.get_values_for_compare('join_day')
        join_data = [{'join_day': item['join_day']} for item in docs]
        self.log.info("Join Data: " + str(join_data[0:5]))
        xattr_data = [{'_system3': item['_system3'], 'field1': item['field1'], 'sub_field1a': item['sub_field1a']} for item in docs]
        self.log.info("XAttr Data: " + str(xattr_data[0:5]))
        diffs = DeepDiff(xattr_data, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)
        diffs = DeepDiff(join_data, compare_4, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # deleted doc xattr query
        meta_ids = self.get_meta_ids()
        delete_ids = meta_ids[0:10]
        for id in delete_ids:
            query_response = self.run_cbq_query('DELETE FROM default USE KEYS[\"' + id['id'] + '\"] returning meta().id')
            self.log.info(query_response['results'])
            self.assertTrue(query_response['results'][0]["id"] == id["id"])

        new_meta_ids = self.get_meta_ids()
        for new_id in new_meta_ids:
            self.assertTrue(new_id not in delete_ids)

        for id in delete_ids:
            query_response = self.run_cbq_query('SELECT meta().id, meta().xattrs._system3 FROM default USE KEYS[\"' + id['id'] + '\"]')
            self.assertTrue("_system3" in query_response['results'][0])
            self.assertTrue("field1" in query_response['results'][0]["_system3"] and "field2" in query_response['results'][0]["_system3"])
            self.assertTrue("id" in query_response['results'][0])
            self.assertTrue(query_response['results'][0]['id'] == id['id'])

    """
    Test system xattr queries with secondary index:
        1. full path - single value
        2. full path - json value
        3. full path nested json value
        4. partial path single level
        5. partial path multi level
        6. mulitple paths - full, partial - single level, partial - multi level
        7. full path before/after deletion
        8. full path partial index
        9. full path functional index
    """
    def test_system_xattr_secondary_index(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        # full path query
        index_statement = "CREATE INDEX idx1 ON default(meta().xattrs._system1) USING " + self.index_type
        query = "SELECT meta().xattrs._system1 FROM default where meta().xattrs._system1 is not missing"
        self.run_xattrs_query(query, index_statement, '_system1', 'idx1', 'default', xattr_data=self.system_xattr_data)

        # full path query
        index_statement = "CREATE INDEX idx2 ON default(meta().xattrs._system2) USING " + self.index_type
        query = "SELECT meta().xattrs._system2 FROM default where meta().xattrs._system2 is not missing"
        self.run_xattrs_query(query, index_statement, '_system2', 'idx2', 'default', xattr_data=self.system_xattr_data)

        # full path query
        index_statement = "CREATE INDEX idx3 ON default(meta().xattrs._system3) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx3', 'default', xattr_data=self.system_xattr_data)

        # partial path query
        index_statement = "CREATE INDEX idx4 ON default(meta().xattrs._system3.field1) USING " + self.index_type
        query = "SELECT meta().xattrs._system3.field1 FROM default where meta().xattrs._system3.field1 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx4', 'default', xattr_data=self.system_xattr_data)

        # nested partial path query
        index_statement = "CREATE INDEX idx5 ON default(meta().xattrs._system3.field1.sub_field1a) USING " + self.index_type
        query = "SELECT meta().xattrs._system3.field1.sub_field1a FROM default where meta().xattrs._system3.field1.sub_field1a is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx5', 'default', xattr_data=self.system_xattr_data)

        # multiple paths single xattr query
        index_statement = "CREATE INDEX idx6 ON default(meta().xattrs._system3.field1.sub_field1a) USING " + self.index_type
        query = "SELECT meta().xattrs._system3, meta().xattrs._system3.field1, meta().xattrs._system3.field1.sub_field1a FROM default where meta().xattrs._system3.field1.sub_field1a is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx6', 'default', xattr_data=self.system_xattr_data)

        # deleted doc xattr query
        index_statement = "CREATE INDEX idx7 ON default(meta().xattrs._system3) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx7', 'default', xattr_data=self.system_xattr_data, deleted_compare=True)

        # partial index
        index_statement = "CREATE INDEX idx8 ON default(meta().xattrs._system3) where meta().xattrs._system3.field1.sub_field1a > 0 USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing and meta().xattrs._system3.field1.sub_field1a > 0"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx8', 'default', xattr_data=self.system_xattr_data)

        # functional index
        index_statement = "CREATE INDEX idx9 ON default(ABS(meta().xattrs._system3.field1.sub_field1a) + 2) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where ABS(meta().xattrs._system3.field1.sub_field1a) + 2 > 0"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx9', 'default', xattr_data=self.system_xattr_data)

    """
    Test system xattr queries with composite secondary index:
        1. full path non-leading - single value
        2. full path non-leading - json value
        3. full path non-leading - nested json value
        4. partial path non-leading - single level
        5. partial path non-leading - multi level
        6. mulitple paths non-leading - full, partial - single level, partial - multi level
        7. full path non-leading before/after deletion
        8. full path non-leading partial index
        9. full path non-leading functional index
        10. full path leading - single value
        11. full path leading - json value
        12. full path leading - nested json value
        13. partial path leading - single level
        14. partial path leading - multi level
        15. mulitple paths leading - full, partial - single level, partial - multi level
        16. full path leading before/after deletion
        17. full path leading partial index
        18. full path leading functional index
    """
    def test_system_xattr_composite_secondary_index(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        # full path query non-leading
        index_statement = "CREATE INDEX idx1 ON default(meta().id, meta().xattrs._system1, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system1 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system1', 'idx1', 'default', xattr_data=self.system_xattr_data)

        # full path query non-leading
        index_statement = "CREATE INDEX idx2 ON default(meta().id, meta().xattrs._system2, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system2 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system2', 'idx2', 'default', xattr_data=self.system_xattr_data)

        # full path query non-leading
        index_statement = "CREATE INDEX idx3 ON default(meta().id, meta().xattrs._system3, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx3', 'default', xattr_data=self.system_xattr_data)

        # partial path query non-leading
        index_statement = "CREATE INDEX idx4 ON default(meta().id, meta().xattrs._system3.field1, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3.field1 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx4', 'default', xattr_data=self.system_xattr_data)

        # nested partial path query non-leading
        index_statement = "CREATE INDEX idx5 ON default(meta().id, meta().xattrs._system3.field1.sub_field1a, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3.field1.sub_field1a FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx5', 'default', xattr_data=self.system_xattr_data)

        # multiple paths single xattr query non-leading
        index_statement = "CREATE INDEX idx6 ON default(meta().id, meta().xattrs._system3, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3, meta().xattrs._system3.field1, meta().xattrs._system3.field1.sub_field1a FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx6', 'default', xattr_data=self.system_xattr_data)

        # deleted doc xattr query non-leading
        index_statement = "CREATE INDEX idx7 ON default(meta().id, meta().xattrs._system3, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx7', 'default', xattr_data=self.system_xattr_data, deleted_compare=True)

        # partial index non-leading
        index_statement = "CREATE INDEX idx8 ON default(meta().id, meta().xattrs._system3, join_day) where meta().xattrs._system3.field1.sub_field1a > 0 USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing and meta().xattrs._system3.field1.sub_field1a > 0"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx8', 'default', xattr_data=self.system_xattr_data)

        # functional index non-leading
        index_statement = "CREATE INDEX idx9 ON default(meta().id, ABS(meta().xattrs._system3.field1.sub_field1a) + 2, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing and ABS(meta().xattrs._system3.field1.sub_field1a) + 2 > 0"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx9', 'default', xattr_data=self.system_xattr_data)

        # full path query leading
        index_statement = "CREATE INDEX idx10 ON default(meta().xattrs._system1, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system1 FROM default where meta().xattrs._system1 is not missing"
        self.run_xattrs_query(query, index_statement, '_system1', 'idx10', 'default', xattr_data=self.system_xattr_data)

        # full path query leading
        index_statement = "CREATE INDEX idx11 ON default(meta().xattrs._system2, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system2 FROM default where meta().xattrs._system2 is not missing"
        self.run_xattrs_query(query, index_statement, '_system2', 'idx11', 'default', xattr_data=self.system_xattr_data)

        # full path query leading
        index_statement = "CREATE INDEX idx12 ON default(meta().xattrs._system3, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx12', 'default', xattr_data=self.system_xattr_data)

        # partial path query leading
        index_statement = "CREATE INDEX idx13 ON default(meta().xattrs._system3.field1, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3.field1 FROM default where meta().xattrs._system3.field1 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx13', 'default', xattr_data=self.system_xattr_data)

        # nested partial path query leading
        index_statement = "CREATE INDEX idx14 ON default(meta().xattrs._system3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3.field1.sub_field1a FROM default where meta().xattrs._system3.field1.sub_field1a is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx14', 'default', xattr_data=self.system_xattr_data)

        # multiple paths single xattr query leading
        index_statement = "CREATE INDEX idx15 ON default(meta().xattrs._system3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3, meta().xattrs._system3.field1, meta().xattrs._system3.field1.sub_field1a FROM default where meta().xattrs._system3.field1.sub_field1a is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx15', 'default', xattr_data=self.system_xattr_data)

        # deleted doc xattr query leading
        index_statement = "CREATE INDEX idx16 ON default(meta().xattrs._system3, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx16', 'default', xattr_data=self.system_xattr_data, deleted_compare=True)

        # partial index leading
        index_statement = "CREATE INDEX idx17 ON default(meta().xattrs._system3, meta().id, join_day) where meta().xattrs._system3.field1.sub_field1a > 0 USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing and meta().xattrs._system3.field1.sub_field1a > 0"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx17', 'default', xattr_data=self.system_xattr_data)

        # functional index leading
        index_statement = "CREATE INDEX idx18 ON default(ABS(meta().xattrs._system3.field1.sub_field1a) + 2, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs._system3 FROM default where ABS(meta().xattrs._system3.field1.sub_field1a) + 2 > 0"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx18', 'default', xattr_data=self.system_xattr_data)

    """
    Test sytem xattr queries with retain deleted true flag in index
        1. Single index on system xattrs
        2. Composite index meta leading key
        3. Composite index xattrs leading key
        4. Composite index data field leading key
    """
    def test_system_xattr_with_retain_deleted(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        # retain deleted doc xattrs index on single xattr field
        index_statement = "CREATE INDEX idx1 ON default(meta().xattrs._system3) USING " + self.index_type + " WITH {'retain_deleted_xattr':true}"
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx1', 'default', xattr_data=self.system_xattr_data, deleted_compare=True, with_retain=True)

        # retain deleted doc xattrs composite index meta leading
        index_statement = "CREATE INDEX idx2 ON default(meta().id, meta().xattrs._system3, join_day) USING " + self.index_type + " WITH {'retain_deleted_xattr':true}"
        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx2', 'default', xattr_data=self.system_xattr_data, deleted_compare=True, with_retain=True)

        # retain deleted doc xattrs composite index xattr leading
        index_statement = "CREATE INDEX idx3 ON default(meta().xattrs._system3, meta().id, join_day) USING " + self.index_type + " WITH {'retain_deleted_xattr':true}"
        query = "SELECT meta().xattrs._system3 FROM default where meta().xattrs._system3 is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx3', 'default', xattr_data=self.system_xattr_data, deleted_compare=True, with_retain=True)

        # retain deleted doc xattrs composite index data field leading
        index_statement = "CREATE INDEX idx4 ON default(join_day, meta().xattrs._system3, meta().id) USING " + self.index_type + " WITH {'retain_deleted_xattr':true}"
        query = "SELECT meta().xattrs._system3 FROM default where join_day is not missing"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx4', 'default', xattr_data=self.system_xattr_data, deleted_compare=True, with_retain=True, delete_leading=True)

    """
    Test system xattr queries with aggregation:
        1. full path non-leading - single value
        2. full path non-leading - json value
        3. full path non-leading - nested json value
        4. partial path non-leading - single level
        5. partial path non-leading - multi level
        6. mulitple paths non-leading - full, partial - single level, partial - multi level
        7. full path non-leading before/after deletion
        8. full path non-leading partial index
        9. full path non-leading functional index
        10. full path leading - single value
        11. full path leading - json value
        12. full path leading - nested json value
        13. partial path leading - single level
        14. partial path leading - multi level
        15. mulitple paths leading - full, partial - single level, partial - multi level
        16. full path leading before/after deletion
        17. full path leading partial index
        18. full path leading functional index
    """
    def test_system_xattr_with_aggregation(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        # full path query non-leading
        index_statement = "CREATE INDEX idx1 ON default(meta().id, meta().xattrs._system1, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system1) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system1', 'idx1', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # full path query non-leading
        index_statement = "CREATE INDEX idx2 ON default(meta().id, meta().xattrs._system2, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system2) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system2', 'idx2', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # full path query non-leading
        index_statement = "CREATE INDEX idx3 ON default(meta().id, meta().xattrs._system3, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system3) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx3', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # partial path query non-leading
        index_statement = "CREATE INDEX idx4 ON default(meta().id, meta().xattrs._system3.field1, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system3.field1) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx4', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # nested partial path query non-leading
        index_statement = "CREATE INDEX idx5 ON default(meta().id, meta().xattrs._system3.field1.sub_field1a, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system3.field1.sub_field1a) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx5', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # multiple paths single xattr query non-leading
        index_statement = "CREATE INDEX idx6 ON default(meta().id, meta().xattrs._system3, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system3) as A, MIN(meta().xattrs._system3.field1) as B, MIN(meta().xattrs._system3.field1.sub_field1a) as C FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx6', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # deleted doc xattr query non-leading
        index_statement = "CREATE INDEX idx7 ON default(meta().id, meta().xattrs._system3, join_day) USING " + self.index_type
        query = "SELECT COUNT(meta().xattrs._system3) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx7', 'default', xattr_data=self.system_xattr_data, deleted_compare=True, with_aggs=True)

        # partial index non-leading
        index_statement = "CREATE INDEX idx8 ON default(meta().id, meta().xattrs._system3, join_day) where join_day > 0 USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system3) as A FROM default where meta().id is not missing and join_day > 0 GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx8', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # functional index non-leading
        index_statement = "CREATE INDEX idx9 ON default(meta().id, ABS(meta().xattrs._system3.field1.sub_field1a) + 2, join_day) USING " + self.index_type
        query = "SELECT MIN(ABS(meta().xattrs._system3.field1.sub_field1a) + 2) as A FROM default where meta().id is not missing and ABS(meta().xattrs._system3.field1.sub_field1a) + 2 > 0 GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx9', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # full path query leading
        index_statement = "CREATE INDEX idx10 ON default(meta().xattrs._system1, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system1) as A FROM default where meta().xattrs._system1 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system1', 'idx10', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # full path query leading
        index_statement = "CREATE INDEX idx11 ON default(meta().xattrs._system2, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system2) as A FROM default where meta().xattrs._system2 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system2', 'idx11', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # full path query leading
        index_statement = "CREATE INDEX idx12 ON default(meta().xattrs._system3, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system3) as A FROM default where meta().xattrs._system3 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx12', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # partial path query leading
        index_statement = "CREATE INDEX idx13 ON default(meta().xattrs._system3.field1, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system3.field1) as A FROM default where meta().xattrs._system3.field1 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx13', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # nested partial path query leading
        index_statement = "CREATE INDEX idx14 ON default(meta().xattrs._system3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system3.field1.sub_field1a) as A FROM default where meta().xattrs._system3.field1.sub_field1a is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx14', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # multiple paths single xattr query leading
        index_statement = "CREATE INDEX idx15 ON default(meta().xattrs._system3, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs._system3) as A, MIN(meta().xattrs._system3.field1) as B, MIN(meta().xattrs._system3.field1.sub_field1a) as C FROM default where meta().xattrs._system3 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx15', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # deleted doc xattr query leading
        index_statement = "CREATE INDEX idx16 ON default(meta().xattrs._system3, meta().id, join_day) USING " + self.index_type
        query = "SELECT COUNT(meta().xattrs._system3) as A FROM default where meta().xattrs._system3 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx16', 'default', xattr_data=self.system_xattr_data, with_aggs=True, deleted_compare=True)

        # partial index leading
        index_statement = "CREATE INDEX idx17 ON default(meta().xattrs._system3, meta().id, join_day) where join_day > 0 USING " + self.index_type
        query = "SELECT COUNT(meta().xattrs._system3) as A FROM default where meta().xattrs._system3 is not missing and join_day > 0 GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx17', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

        # functional index leading
        index_statement = "CREATE INDEX idx18 ON default(ABS(meta().xattrs._system3.field1.sub_field1a) + 2, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(ABS(meta().xattrs._system3.field1.sub_field1a) + 2) as A FROM default where ABS(meta().xattrs._system3.field1.sub_field1a) + 2 > 0 GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, '_system3', 'idx18', 'default', xattr_data=self.system_xattr_data, with_aggs=True)

    """
    Test if field values can be updated with system xattr values
    """
    def test_system_xattr_crud_ops(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        # set doc value to xattr value
        meta_ids = self.get_meta_ids()
        update_ids = meta_ids[0:10]

        # grab values before setting
        compare = []
        for update_id in update_ids:
            query = 'select meta().id as id, meta().xattrs._system1 as new_field from default use keys["'+ str(update_id['id']) + '"]'
            query_response = self.run_cbq_query(query)
            doc = query_response['results']
            compare.append(doc)

        # grab updated values
        docs = []
        for update_id in update_ids:
            query = 'update default SET new_field = meta().xattrs._system1 where meta().id = "' + str(update_id['id']) + '" returning meta().id, new_field'
            query_response = self.run_cbq_query(query)
            doc = query_response['results']
            docs.append(doc)

        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    """
    Tests if system xattrs can be used in subqueries
    """
    def test_system_xattr_subquery(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        # subquery in from clause
        query = "select d._system1 as A from (select meta().xattrs._system1 from default) d"
        check_query = "select meta().xattrs._system1 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause
        query = "select d._system2 as A from (select meta().xattrs._system2 from default) d"
        check_query = "select meta().xattrs._system2 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause
        query = "select d._system3 as A from (select meta().xattrs._system3 from default) d"
        check_query = "select meta().xattrs._system3 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d._system2.field1 as A from (select meta().xattrs._system2 from default) d"
        check_query = "select meta().xattrs._system2.field1 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.field1 as A from (select meta().xattrs._system2.field1 from default) d"
        check_query = "select meta().xattrs._system2.field1 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d._system3.field1 as A from (select meta().xattrs._system3 from default) d"
        check_query = "select meta().xattrs._system3.field1 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d._system3.field1.sub_field1a as A from (select meta().xattrs._system3 from default) d"
        check_query = "select meta().xattrs._system3.field1.sub_field1a as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.field1 as A from (select meta().xattrs._system3.field1 from default) d"
        check_query = "select meta().xattrs._system3.field1 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.field1.sub_field1a as A from (select meta().xattrs._system3.field1 from default) d"
        check_query = "select meta().xattrs._system3.field1.sub_field1a as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.sub_field1a as A from (select meta().xattrs._system3.field1.sub_field1a from default) d"
        check_query = "select meta().xattrs._system3.field1.sub_field1a as A from default"
        self.run_xattrs_subquery(query, check_query)

    """
    Tests if
        1. Index cannot be created with two system xattr fields
        2. If queries cannot be made on two system xattr fields
    """
    def test_system_xattr_negative(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        # cannot create index on two system xattr fields
        index_statement = "CREATE INDEX idx1 ON default(meta().xattrs._system1, meta().xattrs._system2, meta().xattrs._system3) USING " + self.index_type
        try:
            query_response = self.run_cbq_query(index_statement)
        except Exception as ex:
            pass
        else:
            self.fail()


        # cannot query two system xattr fields
        query = "SELECT meta().xattrs._system1, meta().xattrs._system2, meta().xattrs._system3 FROM default"
        try:
            query_response = self.run_cbq_query(query)
        except Exception as ex:
            pass
        else:
            self.fail()

    '''MB-28533'''
    def test_system_xattr_array_index(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        meta_ids = self.get_meta_ids()
        self.write_xattr(meta_ids[0]['id'], "_sync", {"channels": {"channel-1": 1}})

        index_statement = "CREATE INDEX `ixChannels2` ON default((all (array (`op`.`name`) for `op` in object_pairs((((meta().`xattrs`).`_sync`).`channels`)) end))) USING " + self.index_type
        query = 'SELECT meta().ii FROM default WHERE any op in object_pairs(meta().xattrs._sync.channels) satisfies op.name = "channel-1" end'
        self.run_xattrs_query(query, index_statement, '_sync', 'ixChannels2', 'default', xattr_data=self.system_xattr_data)


    '''If you create an index on a document with xattrs with_retain_deleted = True, those xattrs will persist when the 
       document is deleted, however if you reinsert another doc with the same doc id w/o xattrs and delete the new doc, 
       the old xattrs will not be retained. This tests that special case'''
    def test_system_xattr_with_retain_deleted_reinsert(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        # retain deleted doc xattrs index on single xattr field
        index_statement = "CREATE INDEX idx1 ON default(meta().xattrs._system3) USING " + self.index_type + " WITH {'retain_deleted_xattr':true}"
        self.run_cbq_query(query=index_statement)

        try:
            meta_ids = self.get_meta_ids()
            delete_ids = meta_ids[0:3]
            for id in delete_ids:
                query_response = self.run_cbq_query('DELETE FROM default USE KEYS[\"' + id['id'] + '\"] returning meta().id')
                self.log.info(query_response['results'])
                self.assertTrue(query_response['results'][0]["id"] == id["id"])

            new_meta_ids = self.get_meta_ids()
            for new_id in new_meta_ids:
                self.assertTrue(new_id not in delete_ids)

            for id in delete_ids:
                query_response = self.run_cbq_query('SELECT meta().id, meta().xattrs._system3 FROM default USE KEYS[\"' + id['id'] + '\"]')
                self.assertTrue("_system3" in query_response['results'][0])
                self.assertTrue("field1" in query_response['results'][0]["_system3"] and "field2" in query_response['results'][0]["_system3"])
                self.assertTrue("id" in query_response['results'][0])
                self.assertTrue(query_response['results'][0]['id'] == id['id'])

                # Re-insert data with the ID, but w/o the xattr, then delete data, xattr should no longer be present
                insert = 'INSERT INTO default ( KEY, VALUE ) VALUES ("%s",{ "order_id": "1"})' % (id['id'])
                self.run_cbq_query(query=insert)

                query_response = self.run_cbq_query('DELETE FROM default USE KEYS[\"' + id['id'] + '\"] returning meta().id')
                self.log.info(query_response['results'])
                self.assertTrue(query_response['results'][0]["id"] == id["id"])

                query_response = self.run_cbq_query('SELECT meta().id, meta().xattrs._system3 FROM default USE KEYS[\"' + id['id'] + '\"]')
                self.assertTrue("_system3" not in query_response['results'][0])
                self.assertTrue(query_response['results'][0]['id'] == id['id'])
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx1")

    def test_system_xattr_rebalance_in_indexer(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')
        self.user_xattr_data = self.create_xattr_data(type='user')

        index_statement = "CREATE INDEX idx8 ON default(meta().id, meta().xattrs._system3, join_day) where meta().xattrs._system3.field1.sub_field1a > 0 USING " + self.index_type
        index_statement2 = "CREATE INDEX idx15 ON default(meta().xattrs.user3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type

        self.run_cbq_query(query=index_statement)
        self.run_cbq_query(query=index_statement2)

        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing and meta().xattrs._system3.field1.sub_field1a > 0"
        query2 = "SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a FROM default where meta().xattrs.user3.field1.sub_field1a is not missing"


        self.sleep(30)

        try:
            # rebalance in a node
            services_in = ["index"]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [],
                                                     services=services_in)
            # Run a system xattr query
            self.run_xattrs_query(query, '', '_system3', 'idx8', 'default', xattr_data=self.system_xattr_data)

            # Run a virtual xattr query
            self.run_xattrs_query("SELECT meta().xattrs.`$document` FROM default", "", "$document", "", "default", primary_compare=False, xattr_type='virtual')

            # Run a user xattr query
            self.run_xattrs_query(query2, '', 'user3', 'idx15', 'default', xattr_data=self.user_xattr_data)



            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx8")
            self.run_cbq_query(query="DROP INDEX default.idx15")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [self.servers[self.nodes_init]])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()

    def test_system_xattr_rebalance_in_query(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')
        self.user_xattr_data = self.create_xattr_data(type='user')

        index_statement = "CREATE INDEX idx8 ON default(meta().id, meta().xattrs._system3, join_day) where meta().xattrs._system3.field1.sub_field1a > 0 USING " + self.index_type
        index_statement2 = "CREATE INDEX idx15 ON default(meta().xattrs.user3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type

        self.run_cbq_query(query=index_statement)
        self.run_cbq_query(query=index_statement2)

        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing and meta().xattrs._system3.field1.sub_field1a > 0"
        query2 = "SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a FROM default where meta().xattrs.user3.field1.sub_field1a is not missing"

        self.sleep(30)

        try:
            # rebalance in a node
            services_in = ["n1ql"]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]],
                                                     [],
                                                     services=services_in)
            # Run a system xattr query
            self.run_xattrs_query(query, '', '_system3', 'idx8', 'default', xattr_data=self.system_xattr_data)

            # Run a virtual xattr query
            self.run_xattrs_query("SELECT meta().xattrs.`$document` FROM default", "", "$document", "", "default",
                                  primary_compare=False, xattr_type='virtual')

            # Run a user xattr query
            self.run_xattrs_query(query2, '', 'user3', 'idx15', 'default', xattr_data=self.user_xattr_data)

            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
        finally:
            self.run_cbq_query(query="DROP INDEX default.idx8")
            self.run_cbq_query(query="DROP INDEX default.idx15")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [self.servers[self.nodes_init]])
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()

    def test_hard_failover_and_full_recovery_and_gsi_rebalance(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')
        self.user_xattr_data = self.create_xattr_data(type='user')

        index_statement = "CREATE INDEX idx8 ON default(meta().id, meta().xattrs._system3, join_day) where meta().xattrs._system3.field1.sub_field1a > 0 USING " + self.index_type
        index_statement2 = "CREATE INDEX idx15 ON default(meta().xattrs.user3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type

        self.run_cbq_query(query=index_statement)
        self.run_cbq_query(query=index_statement2)

        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing and meta().xattrs._system3.field1.sub_field1a > 0"
        query2 = "SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a FROM default where meta().xattrs.user3.field1.sub_field1a is not missing"
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.sleep(30)

        try:
            # failover the indexer node
            failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=False)
            failover_task.result()
            self.sleep(30)
            # do a full recovery and rebalance
            self.rest.set_recovery_type('ns_1@' + index_server.ip, "full")
            self.rest.add_back_node('ns_1@' + index_server.ip)
            reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
            if reb1:
                result = self.rest.monitorRebalance()
                msg = "successfully rebalanced cluster {0}"
                self.log.info(msg.format(result))
            self.sleep(30)

            self.run_xattrs_query(query, '', '_system3', 'idx8', 'default', xattr_data=self.system_xattr_data)

            # Run a user xattr query
            self.run_xattrs_query(query2, '', 'user3', 'idx15', 'default', xattr_data=self.user_xattr_data)

            # Run a virtual xattr query
            self.run_xattrs_query("SELECT meta().xattrs.`$document` FROM default", "", "$document", "", "default",
                                  primary_compare=False, xattr_type='virtual')

        finally:
            self.run_cbq_query(query="DROP INDEX default.idx8")
            self.run_cbq_query(query="DROP INDEX default.idx15")

    def test_graceful_failover_and_full_recovery_and_gsi_rebalance(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')
        self.user_xattr_data = self.create_xattr_data(type='user')

        index_statement = "CREATE INDEX idx8 ON default(meta().id, meta().xattrs._system3, join_day) where meta().xattrs._system3.field1.sub_field1a > 0 USING " + self.index_type
        index_statement2 = "CREATE INDEX idx15 ON default(meta().xattrs.user3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type

        self.run_cbq_query(query=index_statement)
        self.run_cbq_query(query=index_statement2)

        query = "SELECT meta().xattrs._system3 FROM default where meta().id is not missing and meta().xattrs._system3.field1.sub_field1a > 0"
        query2 = "SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a FROM default where meta().xattrs.user3.field1.sub_field1a is not missing"
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)

        self.sleep(30)
        try:
            # failover the indexer node
            failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=True)
            self.run_xattrs_query(query, '', '_system3', 'idx8', 'default', xattr_data=self.system_xattr_data)

            # Run a virtual xattr query
            self.run_xattrs_query("SELECT meta().xattrs.`$document` FROM default", "", "$document", "", "default",
                                  primary_compare=False, xattr_type='virtual')

            # Run a user xattr query
            self.run_xattrs_query(query2, '', 'user3', 'idx15', 'default', xattr_data=self.user_xattr_data)
            failover_task.result()
            self.sleep(120)
            # do a full recovery and rebalance
            self.rest.set_recovery_type('ns_1@' + index_server.ip, "full")
            self.rest.add_back_node('ns_1@' + index_server.ip)
            reb1 = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
            self.run_xattrs_query(query, '', '_system3', 'idx8', 'default', xattr_data=self.system_xattr_data)

            # Run a virtual xattr query
            self.run_xattrs_query("SELECT meta().xattrs.`$document` FROM default", "", "$document", "", "default",
                                  primary_compare=False, xattr_type='virtual')

            # Run a user xattr query
            self.run_xattrs_query(query2, '', 'user3', 'idx15', 'default', xattr_data=self.user_xattr_data)
            if reb1:
                result = self.rest.monitorRebalance()
                msg = "successfully rebalanced cluster {0}"
                self.log.info(msg.format(result))

        finally:
            self.run_cbq_query(query="DROP INDEX default.idx8")
            self.run_cbq_query(query="DROP INDEX default.idx15")


    # Tests to be implemented

    """
    Tests if system xattrs can be used in joins
    """
    def test_system_xattr_join(self):
        pass

    """
    Test user xattr queries with primary index:
        1. full path - single value
        2. full path - json value
        3. full path nested json value
        4. partial path single level
        5. partial path multi level
        6. mulitple paths - full, partial - single level, partial - multi level
        7. full path before/after deletion
    """
    def test_user_xattr_primary_index(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')

        # full path query
        self.run_xattrs_query("SELECT meta().xattrs.user1 FROM default", "", "user1", "", "default", xattr_data=self.user_xattr_data, primary_compare=False)

        # full path query
        self.run_xattrs_query("SELECT meta().xattrs.user2 FROM default", "", "user2", "", "default", xattr_data=self.user_xattr_data, primary_compare=False)

        # full path query
        compare_1 = self.run_xattrs_query("SELECT meta().xattrs.user3 FROM default", "", "user3", "", "default", xattr_data=self.user_xattr_data, primary_compare=False)

        # partial path query
        compare_2 = self.run_xattrs_query("SELECT meta().xattrs.user3.field1 FROM default", "", "user3", "", "default", compare_fields=["field1"], xattr_data=self.user_xattr_data, primary_compare=False)

        # nested partial path query
        compare_3 = self.run_xattrs_query("SELECT meta().xattrs.user3.field1.sub_field1a FROM default", "", "user3", "", "default", compare_fields=["field1", "sub_field1a"], xattr_data=self.user_xattr_data, primary_compare=False)

        # multiple paths single xattr query
        query_response = self.run_cbq_query("SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a FROM default")
        docs = query_response['results']
        self.log.info("XAttrs: " + str(docs[0:5]))
        compare = []
        for i in range(0, len(docs)):
            compare.append({'user3': compare_1[i]['user3'], 'field1': compare_2[i]['field1'], 'sub_field1a': compare_3[i]['sub_field1a']})
        self.log.info("Compare: " + str(compare[0:5]))
        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # multiple paths single xattr and data query
        query_response = self.run_cbq_query("SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a, join_day FROM default")
        docs = query_response['results']
        self.log.info("XAttrs: " + str(docs[0:5]))
        compare_4 = self.get_values_for_compare('join_day')
        join_data = [{'join_day': item['join_day']} for item in docs]
        self.log.info("Join Data: " + str(join_data[0:5]))
        xattr_data = [{'user3': item['user3'], 'field1': item['field1'], 'sub_field1a': item['sub_field1a']} for item in docs]
        self.log.info("XAttr Data: " + str(xattr_data[0:5]))
        diffs = DeepDiff(xattr_data, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)
        diffs = DeepDiff(join_data, compare_4, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # deleted doc xattr query
        meta_ids = self.get_meta_ids()
        delete_ids = meta_ids[0:10]
        for id in delete_ids:
            query_response = self.run_cbq_query('DELETE FROM default USE KEYS[\"' + id['id'] + '\"] returning meta().id')
            self.log.info(query_response['results'])
            self.assertTrue(query_response['results'][0]["id"] == id["id"])

        new_meta_ids = self.get_meta_ids()
        for new_id in new_meta_ids:
            self.assertTrue(new_id not in delete_ids)

        for id in delete_ids:
            query_response = self.run_cbq_query('SELECT meta().id, meta().xattrs.user3 FROM default USE KEYS[\"' + id['id'] + '\"]')
            self.assertTrue("user3" not in query_response['results'][0])
            self.assertTrue(query_response['results'][0]['id'] == id['id'])

    """
    Test user xattr queries with secondary index:
        1. full path - single value
        2. full path - json value
        3. full path nested json value
        4. partial path single level
        5. partial path multi level
        6. mulitple paths - full, partial - single level, partial - multi level
        7. full path before/after deletion
        8. full path partial index
        9. full path functional index
    """
    def test_user_xattr_secondary_index(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')

        # full path query
        index_statement = "CREATE INDEX idx1 ON default(meta().xattrs.user1) USING " + self.index_type
        query = "SELECT meta().xattrs.user1 FROM default where meta().xattrs.user1 is not missing"
        self.run_xattrs_query(query, index_statement, 'user1', 'idx1', 'default', xattr_data=self.user_xattr_data)

        # full path query
        index_statement = "CREATE INDEX idx2 ON default(meta().xattrs.user2) USING " + self.index_type
        query = "SELECT meta().xattrs.user2 FROM default where meta().xattrs.user2 is not missing"
        self.run_xattrs_query(query, index_statement, 'user2', 'idx2', 'default', xattr_data=self.user_xattr_data)

        # full path query
        index_statement = "CREATE INDEX idx3 ON default(meta().xattrs.user3) USING " + self.index_type
        query = "SELECT meta().xattrs.user3 FROM default where meta().xattrs.user3 is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx3', 'default', xattr_data=self.user_xattr_data)

        # partial path query
        index_statement = "CREATE INDEX idx4 ON default(meta().xattrs.user3.field1) USING " + self.index_type
        query = "SELECT meta().xattrs.user3.field1 FROM default where meta().xattrs.user3.field1 is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx4', 'default', xattr_data=self.user_xattr_data)

        # nested partial path query
        index_statement = "CREATE INDEX idx5 ON default(meta().xattrs.user3.field1.sub_field1a) USING " + self.index_type
        query = "SELECT meta().xattrs.user3.field1.sub_field1a FROM default where meta().xattrs.user3.field1.sub_field1a is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx5', 'default', xattr_data=self.user_xattr_data)

        # multiple paths single xattr query
        index_statement = "CREATE INDEX idx6 ON default(meta().xattrs.user3.field1.sub_field1a) USING " + self.index_type
        query = "SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a FROM default where meta().xattrs.user3.field1.sub_field1a is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx6', 'default', xattr_data=self.user_xattr_data)

        # partial index
        index_statement = "CREATE INDEX idx8 ON default(meta().xattrs.user3) where meta().xattrs.user3.field1.sub_field1a > 0 USING " + self.index_type
        query = "SELECT meta().xattrs.user3 FROM default where meta().xattrs.user3 is not missing and meta().xattrs.user3.field1.sub_field1a > 0"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx8', 'default', xattr_data=self.user_xattr_data)

        # functional index
        index_statement = "CREATE INDEX idx9 ON default(ABS(meta().xattrs.user3.field1.sub_field1a) + 2) USING " + self.index_type
        query = "SELECT meta().xattrs.user3 FROM default where ABS(meta().xattrs.user3.field1.sub_field1a) + 2 > 0"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx9', 'default', xattr_data=self.user_xattr_data)

    """
    Test user xattr queries with composite secondary index:
        1. full path non-leading - single value
        2. full path non-leading - json value
        3. full path non-leading - nested json value
        4. partial path non-leading - single level
        5. partial path non-leading - multi level
        6. mulitple paths non-leading - full, partial - single level, partial - multi level
        7. full path non-leading before/after deletion
        8. full path non-leading partial index
        9. full path non-leading functional index
        10. full path leading - single value
        11. full path leading - json value
        12. full path leading - nested json value
        13. partial path leading - single level
        14. partial path leading - multi level
        15. mulitple paths leading - full, partial - single level, partial - multi level
        16. full path leading before/after deletion
        17. full path leading partial index
        18. full path leading functional index
    """
    def test_user_xattr_composite_secondary_index(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')

        # full path query non-leading
        index_statement = "CREATE INDEX idx1 ON default(meta().id, meta().xattrs.user1, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user1 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, 'user1', 'idx1', 'default', xattr_data=self.user_xattr_data)

        # full path query non-leading
        index_statement = "CREATE INDEX idx2 ON default(meta().id, meta().xattrs.user2, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user2 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, 'user2', 'idx2', 'default', xattr_data=self.user_xattr_data)

        # full path query non-leading
        index_statement = "CREATE INDEX idx3 ON default(meta().id, meta().xattrs.user3, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user3 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx3', 'default', xattr_data=self.user_xattr_data)

        # partial path query non-leading
        index_statement = "CREATE INDEX idx4 ON default(meta().id, meta().xattrs.user3.field1, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user3.field1 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx4', 'default', xattr_data=self.user_xattr_data)

        # nested partial path query non-leading
        index_statement = "CREATE INDEX idx5 ON default(meta().id, meta().xattrs.user3.field1.sub_field1a, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user3.field1.sub_field1a FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx5', 'default', xattr_data=self.user_xattr_data)

        # multiple paths single xattr query non-leading
        index_statement = "CREATE INDEX idx6 ON default(meta().id, meta().xattrs.user3, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx6', 'default', xattr_data=self.user_xattr_data)

        # partial index non-leading
        index_statement = "CREATE INDEX idx8 ON default(meta().id, meta().xattrs.user3, join_day) where meta().xattrs.user3.field1.sub_field1a > 0 USING " + self.index_type
        query = "SELECT meta().xattrs.user3 FROM default where meta().id is not missing and meta().xattrs.user3.field1.sub_field1a > 0"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx8', 'default', xattr_data=self.user_xattr_data)

        # functional index non-leading
        index_statement = "CREATE INDEX idx9 ON default(meta().id, ABS(meta().xattrs.user3.field1.sub_field1a) + 2, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user3 FROM default where meta().id is not missing and ABS(meta().xattrs.user3.field1.sub_field1a) + 2 > 0"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx9', 'default', xattr_data=self.user_xattr_data)

        # full path query leading
        index_statement = "CREATE INDEX idx10 ON default(meta().xattrs.user1, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user1 FROM default where meta().xattrs.user1 is not missing"
        self.run_xattrs_query(query, index_statement, 'user1', 'idx10', 'default', xattr_data=self.user_xattr_data)

        # full path query leading
        index_statement = "CREATE INDEX idx11 ON default(meta().xattrs.user2, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user2 FROM default where meta().xattrs.user2 is not missing"
        self.run_xattrs_query(query, index_statement, 'user2', 'idx11', 'default', xattr_data=self.user_xattr_data)

        # full path query leading
        index_statement = "CREATE INDEX idx12 ON default(meta().xattrs.user3, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user3 FROM default where meta().xattrs.user3 is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx12', 'default', xattr_data=self.user_xattr_data)

        # partial path query leading
        index_statement = "CREATE INDEX idx13 ON default(meta().xattrs.user3.field1, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user3.field1 FROM default where meta().xattrs.user3.field1 is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx13', 'default', xattr_data=self.user_xattr_data)

        # nested partial path query leading
        index_statement = "CREATE INDEX idx14 ON default(meta().xattrs.user3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user3.field1.sub_field1a FROM default where meta().xattrs.user3.field1.sub_field1a is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx14', 'default', xattr_data=self.user_xattr_data)

        # multiple paths single xattr query leading
        index_statement = "CREATE INDEX idx15 ON default(meta().xattrs.user3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a FROM default where meta().xattrs.user3.field1.sub_field1a is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx15', 'default', xattr_data=self.user_xattr_data)

        # partial index leading
        index_statement = "CREATE INDEX idx17 ON default(meta().xattrs.user3, meta().id, join_day) where meta().xattrs.user3.field1.sub_field1a > 0 USING " + self.index_type
        query = "SELECT meta().xattrs.user3 FROM default where meta().xattrs.user3 is not missing and meta().xattrs.user3.field1.sub_field1a > 0"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx17', 'default', xattr_data=self.user_xattr_data)

        # functional index leading
        index_statement = "CREATE INDEX idx18 ON default(ABS(meta().xattrs.user3.field1.sub_field1a) + 2, meta().id, join_day) USING " + self.index_type
        query = "SELECT meta().xattrs.user3 FROM default where ABS(meta().xattrs.user3.field1.sub_field1a) + 2 > 0"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx18', 'default', xattr_data=self.user_xattr_data)

    """
    Test user xattr queries with retain deleted true flag in index
        1. Single index on system xattrs
        2. Composite index meta leading key
        3. Composite index xattrs leading key
        4. Composite index data field leading key
    """
    def test_user_xattr_with_retain_deleted(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')

        # retain deleted doc xattrs index on single xattr field
        index_statement = "CREATE INDEX idx1 ON default(meta().xattrs.user3) USING " + self.index_type + " WITH {'retain_deleted_xattr':true}"
        query = "SELECT meta().xattrs.user3 FROM default where meta().xattrs.user3 is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx1', 'default', xattr_data=self.user_xattr_data, deleted_compare=True, with_retain=True, xattr_type='user')

        # retain deleted doc xattrs composite index meta leading
        index_statement = "CREATE INDEX idx2 ON default(meta().id, meta().xattrs.user3, join_day) USING " + self.index_type + " WITH {'retain_deleted_xattr':true}"
        query = "SELECT meta().xattrs.user3 FROM default where meta().id is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx2', 'default', xattr_data=self.user_xattr_data, deleted_compare=True, with_retain=True, xattr_type='user')

        # retain deleted doc xattrs composite index xattr leading
        index_statement = "CREATE INDEX idx3 ON default(meta().xattrs.user3, meta().id, join_day) USING " + self.index_type + " WITH {'retain_deleted_xattr':true}"
        query = "SELECT meta().xattrs.user3 FROM default where meta().xattrs.user3 is not missing"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx3', 'default', xattr_data=self.user_xattr_data, deleted_compare=True, with_retain=True, xattr_type='user')

        # retain deleted doc xattrs composite index data field leading
        index_statement = "CREATE INDEX idx4 ON default(join_day, meta().xattrs.user3, meta().id) USING " + self.index_type + " WITH {'retain_deleted_xattr':true}"
        query = "SELECT meta().xattrs.user3 FROM default where join_day is not missing"
        self.run_xattrs_query(query, index_statement, 'user', 'idx4', 'default', xattr_data=self.user_xattr_data, deleted_compare=True, with_retain=True, xattr_type='user', delete_leading=True)

    """
    Test user xattr queries with aggregation:
        1. full path non-leading - single value
        2. full path non-leading - json value
        3. full path non-leading - nested json value
        4. partial path non-leading - single level
        5. partial path non-leading - multi level
        6. mulitple paths non-leading - full, partial - single level, partial - multi level
        7. full path non-leading before/after deletion
        8. full path non-leading partial index
        9. full path non-leading functional index
        10. full path leading - single value
        11. full path leading - json value
        12. full path leading - nested json value
        13. partial path leading - single level
        14. partial path leading - multi level
        15. mulitple paths leading - full, partial - single level, partial - multi level
        16. full path leading before/after deletion
        17. full path leading partial index
        18. full path leading functional index
    """
    def test_user_xattr_with_aggregation(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')

        # full path query non-leading
        index_statement = "CREATE INDEX idx1 ON default(meta().id, meta().xattrs.user1, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user1) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user1', 'idx1', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # full path query non-leading
        index_statement = "CREATE INDEX idx2 ON default(meta().id, meta().xattrs.user2, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user2) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user2', 'idx2', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # full path query non-leading
        index_statement = "CREATE INDEX idx3 ON default(meta().id, meta().xattrs.user3, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user3) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx3', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # partial path query non-leading
        index_statement = "CREATE INDEX idx4 ON default(meta().id, meta().xattrs.user3.field1, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user3.field1) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx4', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # nested partial path query non-leading
        index_statement = "CREATE INDEX idx5 ON default(meta().id, meta().xattrs.user3.field1.sub_field1a, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user3.field1.sub_field1a) as A FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx5', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # multiple paths single xattr query non-leading
        index_statement = "CREATE INDEX idx6 ON default(meta().id, meta().xattrs.user3, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user3) as A, MIN(meta().xattrs.user3.field1) as B, MIN(meta().xattrs.user3.field1.sub_field1a) as C FROM default where meta().id is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx6', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # partial index non-leading
        index_statement = "CREATE INDEX idx8 ON default(meta().id, meta().xattrs.user3, join_day) where join_day > 0 USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user3) as A FROM default where meta().id is not missing and join_day > 0 GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx8', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # functional index non-leading
        index_statement = "CREATE INDEX idx9 ON default(meta().id, ABS(meta().xattrs.user3.field1.sub_field1a) + 2, join_day) USING " + self.index_type
        query = "SELECT MIN(ABS(meta().xattrs.user3.field1.sub_field1a) + 2) as A FROM default where meta().id is not missing and ABS(meta().xattrs.user3.field1.sub_field1a) + 2 > 0 GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx9', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # full path query leading
        index_statement = "CREATE INDEX idx10 ON default(meta().xattrs.user1, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user1) as A FROM default where meta().xattrs.user1 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user1', 'idx10', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # full path query leading
        index_statement = "CREATE INDEX idx11 ON default(meta().xattrs.user2, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user2) as A FROM default where meta().xattrs.user2 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user2', 'idx11', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # full path query leading
        index_statement = "CREATE INDEX idx12 ON default(meta().xattrs.user3, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user3) as A FROM default where meta().xattrs.user3 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx12', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # partial path query leading
        index_statement = "CREATE INDEX idx13 ON default(meta().xattrs.user3.field1, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user3.field1) as A FROM default where meta().xattrs.user3.field1 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx13', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # nested partial path query leading
        index_statement = "CREATE INDEX idx14 ON default(meta().xattrs.user3.field1.sub_field1a, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user3.field1.sub_field1a) as A FROM default where meta().xattrs.user3.field1.sub_field1a is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx14', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # multiple paths single xattr query leading
        index_statement = "CREATE INDEX idx15 ON default(meta().xattrs.user3, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(meta().xattrs.user3) as A, MIN(meta().xattrs.user3.field1) as B, MIN(meta().xattrs.user3.field1.sub_field1a) as C FROM default where meta().xattrs.user3 is not missing GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx15', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # partial index leading
        index_statement = "CREATE INDEX idx17 ON default(meta().xattrs.user3, meta().id, join_day) where join_day > 0 USING " + self.index_type
        query = "SELECT COUNT(meta().xattrs.user3) as A FROM default where meta().xattrs.user3 is not missing and join_day > 0 GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx17', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

        # functional index leading
        index_statement = "CREATE INDEX idx18 ON default(ABS(meta().xattrs.user3.field1.sub_field1a) + 2, meta().id, join_day) USING " + self.index_type
        query = "SELECT MIN(ABS(meta().xattrs.user3.field1.sub_field1a) + 2) as A FROM default where ABS(meta().xattrs.user3.field1.sub_field1a) + 2 > 0 GROUP BY join_day"
        self.run_xattrs_query(query, index_statement, 'user3', 'idx18', 'default', xattr_data=self.user_xattr_data,
                              with_aggs=True)

    """
    Test if field values can be updated with user xattr values
    """
    def test_user_xattr_crud_ops(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')

        # set doc value to xattr value
        meta_ids = self.get_meta_ids()
        update_ids = meta_ids[0:10]

        # grab values before setting
        compare = []
        for update_id in update_ids:
            query = 'select meta().id as id, meta().xattrs.user1 as new_field from default use keys["'+ str(update_id['id']) + '"]'
            query_response = self.run_cbq_query(query)
            doc = query_response['results']
            compare.append(doc)

        # grab updated values
        docs = []
        for update_id in update_ids:
            query = 'update default SET new_field = meta().xattrs.user1 where meta().id = "' + str(update_id['id']) + '" returning meta().id, new_field'
            query_response = self.run_cbq_query(query)
            doc = query_response['results']
            docs.append(doc)

        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    """
    Tests if user xattrs can be used in subqueries
    """
    def test_user_xattr_subquery(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')

        # subquery in from clause
        query = "select d.user1 as A from (select meta().xattrs.user1 from default) d"
        check_query = "select meta().xattrs.user1 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause
        query = "select d.user2 as A from (select meta().xattrs.user2 from default) d"
        check_query = "select meta().xattrs.user2 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause
        query = "select d.user3 as A from (select meta().xattrs.user3 from default) d"
        check_query = "select meta().xattrs.user3 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.user2.field1 as A from (select meta().xattrs.user2 from default) d"
        check_query = "select meta().xattrs.user2.field1 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.field1 as A from (select meta().xattrs.user2.field1 from default) d"
        check_query = "select meta().xattrs.user2.field1 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.user3.field1 as A from (select meta().xattrs.user3 from default) d"
        check_query = "select meta().xattrs.user3.field1 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.user3.field1.sub_field1a as A from (select meta().xattrs.user3 from default) d"
        check_query = "select meta().xattrs.user3.field1.sub_field1a as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.field1 as A from (select meta().xattrs.user3.field1 from default) d"
        check_query = "select meta().xattrs.user3.field1 as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.field1.sub_field1a as A from (select meta().xattrs.user3.field1 from default) d"
        check_query = "select meta().xattrs.user3.field1.sub_field1a as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.sub_field1a as A from (select meta().xattrs.user3.field1.sub_field1a from default) d"
        check_query = "select meta().xattrs.user3.field1.sub_field1a as A from default"
        self.run_xattrs_subquery(query, check_query)

    def test_user_xattr_join(self):
        pass

    """
    Tests if
        1. Index cannot be created with two system xattr fields
        2. If queries cannot be made on two system xattr fields
    """
    def test_user_xattr_negative(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')

        # cannot create index on two user xattr fields
        index_statement = "CREATE INDEX idx1 ON default(meta().xattrs.user1, meta().xattrs.user2, meta().xattrs.user3) USING " + self.index_type
        try:
            query_response = self.run_cbq_query(index_statement)
        except Exception as ex:
            pass
        else:
            self.fail()

        # cannot query two system xattr fields
        query = "SELECT meta().xattrs.user1, meta().xattrs.user2, meta().xattrs.user3 FROM default"
        try:
            query_response = self.run_cbq_query(query)
        except Exception as ex:
            pass
        else:
            self.fail()

    def test_user_xattr_array_index(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')

        meta_ids = self.get_meta_ids()
        self.write_xattr(meta_ids[0]['id'], "users", {"channels": {"channel-1": 1}})

        index_statement = "CREATE INDEX `ixChannels2` ON default((all (array (`op`.`name`) for `op` in object_pairs((((meta().`xattrs`).users).`channels`)) end))) USING " + self.index_type
        query = 'SELECT meta().ii FROM default WHERE any op in object_pairs(meta().xattrs.users.channels) satisfies op.name = "channel-1" end'
        self.run_xattrs_query(query, index_statement, 'user', 'ixChannels2', 'default', xattr_data=self.user_xattr_data)


    """
    Test will ensure all fields of virtual xattrs can be queried
    Test different query types
    """
    def test_virtual_xattr_with_primary(self):
        self.reload_data()
        self.fail_if_no_buckets()

        # full path query
        self.run_xattrs_query("SELECT meta().xattrs.`$document` FROM default", "", "$document", "", "default", primary_compare=False, xattr_type='virtual')

        # full path query
        self.run_xattrs_query("SELECT meta().xattrs.`$document` FROM default", "", "$document", "", "default", primary_compare=False, xattr_type='virtual')

        # full path query
        compare_1 = self.run_xattrs_query("SELECT meta().xattrs.`$document` FROM default", "", "$document", "", "default", primary_compare=False, xattr_type='virtual')

        # partial path query
        compare_2 = self.run_xattrs_query("SELECT meta().xattrs.`$document`.deleted FROM default", "", "$document", "", "default", compare_fields=["deleted"], primary_compare=False, xattr_type='virtual')

        # multiple paths single xattr query
        query_response = self.run_cbq_query("SELECT meta().xattrs.`$document`, meta().xattrs.`$document`.deleted FROM default")
        docs = query_response['results']
        self.log.info("XAttrs: " + str(docs[0:5]))
        compare = []
        for i in range(0, len(docs)):
            compare.append({'$document': compare_1[i]['$document'], 'deleted': compare_2[i]['deleted']})
        self.log.info("Compare: " + str(compare[0:5]))
        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # multiple paths single xattr and data query
        query_response = self.run_cbq_query("SELECT meta().xattrs.`$document`, meta().xattrs.`$document`.deleted, join_day FROM default")
        docs = query_response['results']
        self.log.info("XAttrs: " + str(docs[0:5]))
        compare_4 = self.get_values_for_compare('join_day')
        join_data = [{'join_day': item['join_day']} for item in docs]
        self.log.info("Join Data: " + str(join_data[0:5]))
        xattr_data = [{'$document': item['$document'], 'deleted': item['deleted']} for item in docs]
        self.log.info("XAttr Data: " + str(xattr_data[0:5]))
        diffs = DeepDiff(xattr_data, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)
        diffs = DeepDiff(join_data, compare_4, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # deleted doc xattr query
        meta_ids = self.get_meta_ids()
        delete_ids = meta_ids[0:10]
        for id in delete_ids:
            query_response = self.run_cbq_query('DELETE FROM default USE KEYS[\"' + id['id'] + '\"] returning meta().id')
            self.log.info(query_response['results'])
            self.assertTrue(query_response['results'][0]["id"] == id["id"])

        new_meta_ids = self.get_meta_ids()
        for new_id in new_meta_ids:
            self.assertTrue(new_id not in delete_ids)

        for id in delete_ids:
            query_response = self.run_cbq_query('SELECT meta().id, meta().xattrs.`$document` FROM default USE KEYS[\"' + id['id'] + '\"]')
            self.assertTrue("$document" in query_response['results'][0])
            self.assertTrue("deleted" in query_response['results'][0]["$document"] and "deleted" in query_response['results'][0]["$document"]
                            and query_response['results'][0]["$document"]["deleted"] is True)
            self.assertTrue("id" in query_response['results'][0])
            self.assertTrue(query_response['results'][0]['id'] == id['id'])


    """
    Test virtual xattr negative cases:
        1. Cannot create index on virtual xattrs
    """
    def test_virtual_xattr_negative(self):
        try:
            index = "create index sdqidxs21112 on default(meta().xattrs.`$document`)"
            self.run_cbq_query(query=index)
        except Exception as ex:
            self.assertTrue('GSI CreateIndex() - cause: Fails to create index.  Cannot index on Virtual Extended Attributes.'
                            in str(ex), "Error message is not what was expected, here is the error outputed %s" % ex)

    """
    Test if field values can be updated with virtual xattr values
    """
    def test_virtual_xattr_crud_ops(self):
        self.reload_data()
        self.fail_if_no_buckets()

        # set doc value to xattr value
        meta_ids = self.get_meta_ids()
        update_ids = meta_ids[0:10]

        # grab values before setting
        compare = []
        for update_id in update_ids:
            query = 'select meta().id as id, meta().xattrs.`$document`.deleted as new_field from default use keys["'+ str(update_id['id']) + '"]'
            query_response = self.run_cbq_query(query)
            doc = query_response['results']
            compare.append(doc)

        # grab updated values
        docs = []
        for update_id in update_ids:
            query = 'update default SET new_field = meta().xattrs.`$document`.deleted where meta().id = "' + str(
                update_id['id']) + '" returning meta().id, new_field'
            query_response = self.run_cbq_query(query)
            doc = query_response['results']
            docs.append(doc)

        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    """
    Tests if virtual xattrs can be used in subqueries
    """
    def test_virtual_xattr_subquery(self):
        self.reload_data()
        self.fail_if_no_buckets()

        # subquery in from clause
        query = "select d.`$document` as A from (select meta().xattrs.`$document` from default) d"
        check_query = "select meta().xattrs.`$document` as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.`$document`.deleted as A from (select meta().xattrs.`$document` from default) d"
        check_query = "select meta().xattrs.`$document`.deleted as A from default"
        self.run_xattrs_subquery(query, check_query)

        # subquery in from clause nested
        query = "select d.deleted as A from (select meta().xattrs.`$document`.deleted from default) d"
        check_query = "select meta().xattrs.`$document`.deleted as A from default"
        self.run_xattrs_subquery(query, check_query)

    """
    Test will ensure all fields of virtual xattrs and system xattrs can be simultaneously queried
    Test different query types
    """
    def test_virtual_and_system_xattr_with_primary(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.system_xattr_data = self.create_xattr_data(type='system')

        # full path query
        self.run_xattrs_mixed_query("SELECT meta().xattrs._system1, meta().xattrs.`$document` FROM default", "_system1", "", xattr_data=self.system_xattr_data)

        # full path query
        self.run_xattrs_mixed_query("SELECT meta().xattrs._system2, meta().xattrs.`$document` FROM default", "_system2", "", xattr_data=self.system_xattr_data)

        # full path query
        compare_1 = self.run_xattrs_mixed_query("SELECT meta().xattrs._system3, meta().xattrs.`$document` FROM default", "_system3", "", xattr_data=self.system_xattr_data)


        # partial path query
        compare_2 = self.run_xattrs_mixed_query("SELECT meta().xattrs._system3.field1, meta().xattrs.`$document`.deleted FROM default", "_system3", xattr_data=self.system_xattr_data, compare_fields=["field1"])

        # nested partial path query
        compare_3 = self.run_xattrs_mixed_query("SELECT meta().xattrs._system3.field1.sub_field1a, meta().xattrs.`$document`.deleted FROM default", "_system3", xattr_data=self.system_xattr_data, compare_fields=["field1", "sub_field1a"])

        #multiple paths single xattr query
        query_response = self.run_cbq_query("SELECT meta().xattrs._system3, meta().xattrs._system3.field1, meta().xattrs._system3.field1.sub_field1a, meta().xattrs.`$document`.deleted FROM default")
        docs = query_response['results']
        self.log.info("XAttrs: " + str(docs[0:5]))
        compare = []
        for i in range(0, len(docs)):
            compare.append({'_system3': compare_1[i]['_system3'], 'field1': compare_2[i]['field1'], 'sub_field1a': compare_3[i]['sub_field1a'], 'deleted': False})
        self.log.info("Compare: " + str(compare[0:5]))
        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # multiple paths single xattr and data query
        query_response = self.run_cbq_query("SELECT meta().xattrs._system3, meta().xattrs._system3.field1, meta().xattrs._system3.field1.sub_field1a, meta().xattrs.`$document`.deleted, join_day FROM default")
        docs = query_response['results']
        self.log.info("XAttrs: " + str(docs[0:5]))
        compare_4 = self.get_values_for_compare('join_day')
        join_data = [{'join_day': item['join_day']} for item in docs]
        self.log.info("Join Data: " + str(join_data[0:5]))
        xattr_data = [{'_system3': item['_system3'], 'field1': item['field1'], 'sub_field1a': item['sub_field1a'], 'deleted': False} for item in docs]
        self.log.info("XAttr Data: " + str(xattr_data[0:5]))
        diffs = DeepDiff(xattr_data, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)
        diffs = DeepDiff(join_data, compare_4, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        meta_ids = self.get_meta_ids()
        delete_ids = meta_ids[0:10]
        for id in delete_ids:
            query_response = self.run_cbq_query('DELETE FROM default USE KEYS[\"' + id['id'] + '\"] returning meta().id')
            self.log.info(query_response['results'])
            self.assertTrue(query_response['results'][0]["id"] == id["id"])

        new_meta_ids = self.get_meta_ids()
        for new_id in new_meta_ids:
            self.assertTrue(new_id not in delete_ids)

        for id in delete_ids:
            query_response = self.run_cbq_query('SELECT meta().id, meta().xattrs._system3, meta().xattrs.`$document` FROM default USE KEYS[\"' + id['id'] + '\"]')
            self.assertTrue("_system3" in query_response['results'][0])
            self.assertTrue("field1" in query_response['results'][0]["_system3"] and "field2" in query_response['results'][0]["_system3"])
            self.assertTrue("id" in query_response['results'][0])
            self.assertTrue("$document" in query_response['results'][0])
            self.assertTrue("deleted" in query_response['results'][0]["$document"] and "deleted" in query_response['results'][0]["$document"]
                            and query_response['results'][0]["$document"]["deleted"] is True)
            self.assertTrue(query_response['results'][0]['id'] == id['id'])

    """
    Test will ensure that data can be updated to a virtual and system xattr value
    """
    def test_virtual_and_system_xattr_crud_ops(self):
        self.reload_data()
        self.fail_if_no_buckets()

        # set doc value to xattr value
        meta_ids = self.get_meta_ids()
        update_ids = meta_ids[0:10]

        # grab values before setting
        compare = []
        for update_id in update_ids:
            query = 'select meta().id as id, meta().xattrs.`$document`.deleted as new_field1, meta().xattrs._system1 as new_field2 from default use keys["'+ str(update_id['id']) + '"]'
            query_response = self.run_cbq_query(query)
            doc = query_response['results']
            compare.append(doc)

        # grab updated values
        docs = []
        for update_id in update_ids:
            query = 'update default SET new_field1 = meta().xattrs.`$document`.deleted, new_field2 = meta().xattrs._system1  where meta().id = "' + str(update_id['id']) + '" returning meta().id, new_field1, new_field2'
            query_response = self.run_cbq_query(query)
            doc = query_response['results']
            docs.append(doc)

        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    """
    Test will ensure all fields of virtual xattrs and user xattrs can be simultaneously queried
    Test different query types
    """
    def test_virtual_and_user_xattr_with_primary(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')

        # full path query
        self.run_xattrs_mixed_query("SELECT meta().xattrs.user1, meta().xattrs.`$document` FROM default", "user1", "", xattr_data=self.user_xattr_data)

        # full path query
        self.run_xattrs_mixed_query("SELECT meta().xattrs.user2, meta().xattrs.`$document` FROM default", "user2", "", xattr_data=self.user_xattr_data)

        # full path query
        compare_1 = self.run_xattrs_mixed_query("SELECT meta().xattrs.user3, meta().xattrs.`$document` FROM default", "user3", "", xattr_data=self.user_xattr_data)


        # partial path query
        compare_2 = self.run_xattrs_mixed_query("SELECT meta().xattrs.user3.field1, meta().xattrs.`$document`.deleted FROM default", "user3", xattr_data=self.user_xattr_data, compare_fields=["field1"])

        # nested partial path query
        compare_3 = self.run_xattrs_mixed_query("SELECT meta().xattrs.user3.field1.sub_field1a, meta().xattrs.`$document`.deleted FROM default", "user3", xattr_data=self.user_xattr_data, compare_fields=["field1", "sub_field1a"])

        #multiple paths single xattr query
        query_response = self.run_cbq_query("SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a, meta().xattrs.`$document`.deleted FROM default")
        docs = query_response['results']
        self.log.info("XAttrs: " + str(docs[0:5]))
        compare = []
        for i in range(0, len(docs)):
            compare.append({'user3': compare_1[i]['user3'], 'field1': compare_2[i]['field1'], 'sub_field1a': compare_3[i]['sub_field1a'], 'deleted': False})
        self.log.info("Compare: " + str(compare[0:5]))
        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # multiple paths single xattr and data query
        query_response = self.run_cbq_query("SELECT meta().xattrs.user3, meta().xattrs.user3.field1, meta().xattrs.user3.field1.sub_field1a, meta().xattrs.`$document`.deleted, join_day FROM default")
        docs = query_response['results']
        self.log.info("XAttrs: " + str(docs[0:5]))
        compare_4 = self.get_values_for_compare('join_day')
        join_data = [{'join_day': item['join_day']} for item in docs]
        self.log.info("Join Data: " + str(join_data[0:5]))
        xattr_data = [{'user3': item['user3'], 'field1': item['field1'], 'sub_field1a': item['sub_field1a'], 'deleted': False} for item in docs]
        self.log.info("XAttr Data: " + str(xattr_data[0:5]))
        diffs = DeepDiff(xattr_data, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)
        diffs = DeepDiff(join_data, compare_4, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        meta_ids = self.get_meta_ids()
        delete_ids = meta_ids[0:10]
        for id in delete_ids:
            query_response = self.run_cbq_query('DELETE FROM default USE KEYS[\"' + id['id'] + '\"] returning meta().id')
            self.log.info(query_response['results'])
            self.assertTrue(query_response['results'][0]["id"] == id["id"])

        new_meta_ids = self.get_meta_ids()
        for new_id in new_meta_ids:
            self.assertTrue(new_id not in delete_ids)

        for id in delete_ids:
            query_response = self.run_cbq_query('SELECT meta().id, meta().xattrs.user3, meta().xattrs.`$document` FROM default USE KEYS[\"' + id['id'] + '\"]')
            self.assertTrue("user3" not in query_response['results'][0])
            self.assertTrue("id" in query_response['results'][0])
            self.assertTrue("$document" in query_response['results'][0])
            self.assertTrue("deleted" in query_response['results'][0]["$document"] and "deleted" in query_response['results'][0]["$document"]
                            and query_response['results'][0]["$document"]["deleted"] is True)
            self.assertTrue(query_response['results'][0]['id'] == id['id'])

    """
    Test will ensure that data can be updated to a virtual and user xattr value
    """
    def test_virtual_and_user_xattr_crud_ops(self):
        self.reload_data()
        self.fail_if_no_buckets()

        # set doc value to xattr value
        meta_ids = self.get_meta_ids()
        update_ids = meta_ids[0:10]

        # grab values before setting
        compare = []
        for update_id in update_ids:
            query = 'select meta().id as id, meta().xattrs.`$document`.deleted as new_field1, meta().xattrs.user1 as new_field2 from default use keys["'+ str(update_id['id']) + '"]'
            query_response = self.run_cbq_query(query)
            doc = query_response['results']
            compare.append(doc)

        # grab updated values
        docs = []
        for update_id in update_ids:
            query = 'update default SET new_field1 = meta().xattrs.`$document`.deleted, new_field2 = meta().xattrs.user1  where meta().id = "' + str(update_id['id']) + '" returning meta().id, new_field1, new_field2'
            query_response = self.run_cbq_query(query)
            doc = query_response['results']
            docs.append(doc)

        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    """
    Test mixed xattr negative cases:
        1. Cannot query user and system xattrs at once
        2. Cannot create an index on user and system xattrs in one index
        3. Cannot create an index on an xattr if it contains a virtual xattr anywhere (including where clause)
    """
    def test_mixed_xattr_negative(self):
        self.reload_data()
        self.fail_if_no_buckets()
        self.user_xattr_data = self.create_xattr_data(type='user')
        self.system_xattr_data = self.create_xattr_data(type='system')

        try:
            query = "SELECT meta().id, meta().xattrs.user1, meta().xattrs._system1 from default"
            self.run_cbq_query(query=query)
        except Exception as ex:
            self.assertTrue('Plan error: Can only retrieve virtual xattr and user xattr or virtual xattr and system xattr'
                            in str(ex), "Error message is not what was expected, here is the error outputed %s" % ex)

        try:
            index = "create index idx on default(meta().xattrs.`_system1`, meta().xattrs.user1)"
            self.run_cbq_query(query=index)
        except Exception as ex:
            self.assertTrue('Plan error: Only a single user or system xattribute can be indexed.'
                            in str(ex), "Error message is not what was expected, here is the error outputed %s" % ex)

        try:
            index = " create index qidxs21112 on default(meta().xattrs.`_system1`) where meta().xattrs.`$document` > 5"
            self.run_cbq_query(query=index)
        except Exception as ex:
            self.assertTrue('GSI CreateIndex() - cause: Fails to create index.  Cannot index on Virtual Extended Attributes.'
                            in str(ex), "Error message is not what was expected, here is the error outputed %s" % ex)

    """
    Can run 3 different types of compares:
    1. Compare primary query with local data
    2. Compare secondary query with primary query
    3. Compare secondary query with primary query before and after deleting docs
    """
    def run_xattrs_query(self, query, index_statement, xattr_name, index_name, bucket_name, xattr_data=[], compare_fields=[], primary_compare=True, deleted_compare=False, with_retain=False, xattr_type="system", with_aggs=False, delete_leading=False):
        if index_statement != "":
            self.run_cbq_query(index_statement)
            self._wait_for_index_online(bucket_name, index_name)
            query_response = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue(index_name in str(query_response['results'][0]))
            if with_aggs:
                self.assertTrue("index_group_aggs" in str(query_response['results'][0]))

        query_response = self.run_cbq_query(query)
        docs_1 = query_response['results']
        self.log.info("XAttrs: " + str(docs_1[0:5]))
        compare = []

        if primary_compare:
            temp_query = query.split("FROM " + bucket_name)
            compare_query = temp_query[0] + "FROM " + bucket_name + ' use index(`#primary`)' + temp_query[1]
            compare_response = self.run_cbq_query(compare_query)
            compare = compare_response['results']
        else:
            if len(compare_fields) == 0:
                compare = [xattr for xattr in xattr_data if xattr_name in xattr]
            elif len(compare_fields) == 1:
                compare = [{compare_fields[0]: xattr[xattr_name][compare_fields[0]]} for xattr in xattr_data if xattr_name in xattr]
            elif len(compare_fields) == 2:
                compare = [{compare_fields[1]: xattr[xattr_name][compare_fields[0]][compare_fields[1]]} for xattr in xattr_data if xattr_name in xattr]

        # Virtual xattrs cant be compared in the way the rest of the stuff is compared because it is autogenerated by CB
        # ,therefore we do different checks and then return the results of the query passed in instead
        if xattr_type == "virtual":
            for docs in docs_1:
                if not compare_fields:
                    self.assertTrue('$document' in str(docs) and 'CAS' in str(docs) and 'datatype' in str(docs) and
                                    'deleted' in str(docs) and 'exptime' in str(docs) and 'flags' in str(docs) and
                                    'last_modified' in str(docs) and 'seqno' in str(docs) and
                                    'value_bytes' in str(docs) and 'vbucket_uuid' in str(docs))
                else:
                    self.assertTrue(docs == {"deleted": False})
        else:
            self.log.info("Compare: " + str(compare[0:5]))
            diffs = DeepDiff(docs_1, compare, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

        if deleted_compare:
            meta_ids = self.get_meta_ids()
            delete_ids = meta_ids[0:10]
            for id in delete_ids:
                query_response = self.run_cbq_query('DELETE FROM default USE KEYS[\"' + id['id'] + '\"] returning meta().id')
                self.log.info(query_response['results'])
                self.assertTrue(query_response['results'][0]["id"] == id["id"])

            new_meta_ids = self.get_meta_ids()
            for new_id in new_meta_ids:
                self.assertTrue(new_id not in delete_ids)

            query_response = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue(index_name in str(query_response['results'][0]))

            query_response_2 = self.run_cbq_query(query)
            docs_2 = query_response_2['results']
            self.log.info("XAttrs: " + str(docs_2[0:5]))

            temp_query = query.split("FROM " + bucket_name)
            compare_query = temp_query[0] + "FROM " + bucket_name + ' use index(`#primary`)' + temp_query[1]

            compare_response = self.run_cbq_query(compare_query)
            compare_docs = compare_response['results']

            self.log.info("Compare: " + str(compare_docs[0:5]))

            if with_retain and not xattr_type == 'user':
                self.assertTrue(len(docs_1)-10 == len(compare_docs))
                if delete_leading:
                    self.assertTrue(len(docs_2) == len(compare_docs))
                    diffs = DeepDiff(docs_2, compare_docs, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
                else:
                    self.assertTrue(len(docs_2)-10 == len(compare_docs))
                    diffs = DeepDiff(docs_1, docs_2, ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
            else:
                diffs = DeepDiff(docs_2, compare_docs, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                if not with_aggs:
                    self.assertTrue(len(docs_1)-10 == len(docs_2))

        if index_statement != "":
            self.run_cbq_query("DROP INDEX default." + index_name)

        if deleted_compare and with_retain:
            self.run_cbq_query(index_statement)
            self._wait_for_index_online(bucket_name, index_name)

            query_response = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue(index_name in str(query_response['results'][0]))

            query_response_1 = self.run_cbq_query(query)
            docs_3 = query_response_1['results']

            if delete_leading or xattr_type == 'user':
                self.assertTrue(len(docs_3) == len(compare_docs))
                diffs = DeepDiff(docs_3, compare_docs, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            else:
                self.assertTrue(len(docs_3)-10 == len(compare_docs))
                diffs = DeepDiff(docs_2, docs_3, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

            self.run_cbq_query("DROP INDEX default." + index_name)

            self.reload_data()
            self.system_xattr_data = self.create_xattr_data(type=xattr_type)
        # Virtual xattrs cant be compared in the way the rest of the stuff is compared because it is autogenerated by CB
        # ,therefore we do different checks and then return the results of the query passed in instead
        if xattr_type == 'virtual':
            return docs_1
        else:
            return compare

    """
    Runs a subquery query and checks it against a non-subquery query
    """
    def run_xattrs_subquery(self, query, check_query):
        query_response = self.run_cbq_query(query)
        docs = query_response['results']
        query_response = self.run_cbq_query(check_query)
        compare = query_response['results']
        #self.assertTrue(sorted(docs) == sorted(compare))
        diffs = DeepDiff(docs, compare, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def run_xattrs_mixed_query(self, query, xattr_name, compare_fields=[], xattr_data=[]):

        query_response = self.run_cbq_query(query)
        docs_1 = query_response['results']
        self.log.info("XAttrs: " + str(docs_1[0:5]))
        compare_xattr = []
        doc_no = 0
        nested_level = 0

        if len(compare_fields) == 0:
            compare_xattr = [xattr for xattr in xattr_data if xattr_name in xattr]
        elif len(compare_fields) == 1:
            compare_xattr = [{compare_fields[0]: xattr[xattr_name][compare_fields[0]]} for xattr in xattr_data if xattr_name in xattr]
            nested_level = 1
        elif len(compare_fields) == 2:
            compare_xattr = [{compare_fields[1]: xattr[xattr_name][compare_fields[0]][compare_fields[1]]} for xattr in xattr_data if xattr_name in xattr]
            nested_level = 2

        # Check to see that all fields in a virtual xattr are present
        for docs in docs_1:
            if not compare_fields:
                self.assertTrue('$document' in str(docs) and 'CAS' in str(docs) and 'datatype' in str(docs) and
                                'deleted' in str(docs) and 'exptime' in str(docs) and 'flags' in str(docs) and
                                'last_modified' in str(docs) and 'seqno' in str(docs) and
                                'value_bytes' in str(docs) and 'vbucket_uuid' in str(docs))
                self.assertEqual(docs[xattr_name], compare_xattr[doc_no][xattr_name])
            else:
                self.assertTrue("'deleted': False" in str(docs))
                if nested_level == 1:
                    self.assertEqual(docs[compare_fields[0]], compare_xattr[doc_no][compare_fields[0]])
                elif nested_level == 2:
                    self.assertEqual(docs[compare_fields[1]], compare_xattr[doc_no][compare_fields[1]])

            doc_no = doc_no + 1
        return compare_xattr

    """
    Returns a list of meta ids
    """
    def get_meta_ids(self):
        return self.get_values_for_compare('meta().id')

    """
    Returns a list of values for the provided field
    """
    def get_values_for_compare(self, field):
        query_response = self.run_cbq_query("SELECT " + field + " FROM default")
        docs = query_response['results']
        return docs

    """
    Adds xattrs to each document in default bucket:
    System -
    1. {'_system1': int}
    2. {'_system2': {'field1': int, 'field2': int}}
    3. {'_system3': {'field1': {'sub_field1a': int, 'sub_field1b': int}, 'field2': {'sub_field2a': int, 'sub_field2b': int}}}
    
    User -
    1. {'user1': int}
    2. {'user2': {'field1': int, 'field2': int}}
    3. {'user3': {'field1': {'sub_field1a': int, 'sub_field1b': int}, 'field2': {'sub_field2a': int, 'sub_field2b': int}}}
    """
    def create_xattr_data(self, type="system"):
        cluster = Cluster('couchbase://'+str(self.master.ip))
        authenticator = PasswordAuthenticator(self.username, self.password)
        cluster.authenticate(authenticator)
        cb = cluster.open_bucket('default')
        docs = self.get_meta_ids()
        self.log.info("Docs: " + str(docs[0:5]))
        xattr_data = []
        self.log.info("Adding xattrs to data")
        val = 0
        for doc in docs:
            if type == "system":
                rv = cb.mutate_in(doc["id"], SD.upsert('_system1', val, xattr=True, create_parents=True))
                xattr_data.append({'_system1': val})
                rv = cb.mutate_in(doc["id"], SD.upsert('_system2', {'field1': val, 'field2': val*val}, xattr=True, create_parents=True))
                xattr_data.append({'_system2': {'field1': val, 'field2': val*val}})
                rv = cb.mutate_in(doc["id"], SD.upsert('_system3', {'field1': {'sub_field1a': val, 'sub_field1b': val*val}, 'field2': {'sub_field2a': 2*val, 'sub_field2b': 2*val*val}}, xattr=True, create_parents=True))
                xattr_data.append({'_system3': {'field1': {'sub_field1a': val, 'sub_field1b': val*val}, 'field2': {'sub_field2a': 2*val, 'sub_field2b': 2*val*val}}})
            if type == "user":
                rv = cb.mutate_in(doc["id"], SD.upsert('user1', val, xattr=True, create_parents=True))
                xattr_data.append({'user1': val})
                rv = cb.mutate_in(doc["id"], SD.upsert('user2', {'field1': val, 'field2': val*val}, xattr=True, create_parents=True))
                xattr_data.append({'user2': {'field1': val, 'field2': val*val}})
                rv = cb.mutate_in(doc["id"], SD.upsert('user3', {'field1': {'sub_field1a': val, 'sub_field1b': val*val}, 'field2': {'sub_field2a': 2*val, 'sub_field2b': 2*val*val}}, xattr=True, create_parents=True))
                xattr_data.append({'user3': {'field1': {'sub_field1a': val, 'sub_field1b': val*val}, 'field2': {'sub_field2a': 2*val, 'sub_field2b': 2*val*val}}})
            val = val + 1

        self.log.info("Completed adding " + type + "xattrs to data to " + str(val) + " docs")
        return xattr_data

    """
    Write a single xattr key and val to doc id
    """
    def write_xattr(self, doc_id, xattr_id, value):
        cluster = Cluster('couchbase://'+str(self.master.ip))
        authenticator = PasswordAuthenticator(self.username, self.password)
        cluster.authenticate(authenticator)
        cb = cluster.open_bucket('default')
        rv = cb.mutate_in(doc_id, SD.upsert(xattr_id, value, xattr=True, create_parents=True))
        self.assertTrue(rv.success)
        if '_' in xattr_id:
            self.system_xattr_data.append({xattr_id: value})
        else:
            self.user_xattr_data.append({xattr_id: value})

    """
    Resets the bucket data by deleting the bucket and recreating/reloading the data generated during test suite setup
    """
    def reload_data(self):
        self._all_buckets_delete(self.master)
        self._bucket_creation()
        self.gens_load = self.gen_docs(self.docs_per_day)
        self.load(self.gens_load, batch_size=1000, flag=self.item_flag)
        self.create_primary_index_for_3_0_and_greater()

    def _return_maps(self):
        index_map = self.get_index_map()
        stats_map = self.get_index_stats(perNode=False)
        return index_map, stats_map
