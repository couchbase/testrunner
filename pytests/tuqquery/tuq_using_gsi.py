import json
import logging
import threading
import time
from deepdiff import DeepDiff

from remote.remote_util import RemoteMachineShellConnection

from .tuq import QueryTests


class QueryUsingTests(QueryTests):
    def setUp(self):
        super(QueryUsingTests, self).setUp()
        self.use_partitioned = self.input.param("use_partitioned", False)
        self.use_replicas = self.input.param("use_replicas", False)

    def suite_setUp(self):
        super(QueryUsingTests, self).suite_setUp()
        self.collections_helper.create_scope(bucket_name="default", scope_name=self.scope)
        self.collections_helper.create_collection(bucket_name="default", scope_name=self.scope,
                                                  collection_name=self.collections[0])
        self.collections_helper.create_collection(bucket_name="default", scope_name=self.scope,
                                                  collection_name=self.collections[1])
        self.run_cbq_query(
            query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[
                0]) + '(KEY, VALUE) VALUES ("key2", { "type" : "hotel", "name" : "new hotel" })'))
        self.run_cbq_query(
            query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[
                0]) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "old hotel" })'))
        self.run_cbq_query(
            query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[
                1]) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" })'))
        self.run_cbq_query(
            query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[
                0]) + ' (KEY, VALUE) VALUES ("key3", { "nested" : {"fields": "fake"}, "name" : "old hotel" })'))
        self.run_cbq_query(
            query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[
                0]) + ' (KEY, VALUE) VALUES ("key4", { "numbers": [1,2,3,4] , "name" : "old hotel" })'))
        time.sleep(20)

    def tearDown(self):
        super(QueryUsingTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryUsingTests, self).suite_tearDown()

    def test_using(self):
        try:
            if self.use_partitioned:
                self.run_cbq_query(query="CREATE INDEX idx1 on default:default.{0}.{1}(name) PARTITION BY HASH(META().id) USING GSI".format(self.scope, self.collections[0]))
            elif self.use_replicas:
                self.run_cbq_query(query="CREATE INDEX idx1 on default:default.{0}.{1}(name) USING GSI WITH {{'num_replica': 1}}".format(self.scope, self.collections[0]))
            else:
                self.run_cbq_query(query="CREATE INDEX idx1 on default:default.{0}.{1}(name) USING GSI".format(self.scope, self.collections[0]))
            self.wait_for_all_indexes_online()
            results = self.run_cbq_query(query='select * from default:default.test.test1 USE INDEX (idx1 USING GSI) where name = "old hotel" order by meta().id')
            diffs = DeepDiff(results['results'], [{'test1': {'name': 'old hotel', 'type': 'hotel'}}, {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}}, {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX default:default.test.test1.idx1 USING GSI")

    def test_using_context(self):
        try:
            self.run_cbq_query(
                query="CREATE INDEX idx1 on {1}(name) USING GSI".format(self.scope, self.collections[0]), query_context="default:default.test")
            self.wait_for_all_indexes_online()
            results = self.run_cbq_query(query='select * from test1 USE INDEX (idx1 USING GSI) where name = "old hotel" order by meta().id', query_context="default:default.test")
            diffs = DeepDiff(results['results'], [{'test1': {'name': 'old hotel', 'type': 'hotel'}}, {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}}, {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX test1.idx1 USING GSI",query_context="default:default.test")

    def test_using_array(self):
        try:
            if self.use_partitioned:
                self.run_cbq_query(query="CREATE INDEX idx4 on default:default.test.{1}(ALL numbers) PARTITION BY HASH(META().id) USING GSI".format(self.scope, self.collections[0]))
            elif self.use_replicas:
                self.run_cbq_query(query="CREATE INDEX idx4 on default:default.test.{1}(ALL numbers) USING GSI WITH {{'num_replica': 1}}".format(self.scope, self.collections[0]))
            else:
                self.run_cbq_query(
                    query="CREATE INDEX idx4 on default:default.test.{1}(ALL numbers) USING GSI".format(self.scope,self.collections[0]))
            self.wait_for_all_indexes_online()
            results = self.run_cbq_query(
                query="SELECT * FROM default:default.test.test1 USE INDEX (idx4 USING GSI) WHERE ANY v in numbers SATISFIES v = 1 END")
            diffs = DeepDiff(results['results'],  [{'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX default:default.test.test1.idx4 USING GSI")

    def test_using_array_context(self):
        try:
            self.run_cbq_query(
                query="CREATE INDEX idx4 on {1}(ALL numbers) USING GSI".format(self.scope, self.collections[0]), query_context="default:default.test")
            self.wait_for_all_indexes_online()
            results = self.run_cbq_query(
                query="SELECT * FROM test1 USE INDEX (idx4 USING GSI) WHERE ANY v in numbers SATISFIES v = 1 END", query_context="default:default.test")
            diffs = DeepDiff(results['results'], [{'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP INDEX test1.idx4 USING GSI", query_context="default:default.test")

    def test_using_nested(self):
        try:
            self.run_cbq_query(
                query="CREATE INDEX idx3 on default:default.test.{1}(nested) USING GSI".format(self.scope, self.collections[0]))
            results = self.run_cbq_query(query="SELECT * FROM default:default.test.test1 b USE INDEX(idx3 USING GSI)WHERE b.nested.fields = 'fake'")
            self.assertEqual(results['results'][0]['b'], {'name': 'old hotel', 'nested': {'fields': 'fake'}})
        finally:
            self.run_cbq_query(query="DROP INDEX default:default.test.test1.idx3 USING GSI")

    def test_using_nested_context(self):
        try:
            self.run_cbq_query(
                query="CREATE INDEX idx3 on {1}(nested) USING GSI".format(self.scope, self.collections[0]), query_context="default:default.test")
            results = self.run_cbq_query(query="SELECT * FROM test1 b USE INDEX(idx3 USING GSI)WHERE b.nested.fields = 'fake'", query_context="default:default.test")
            self.assertEqual(results['results'][0]['b'], {'name': 'old hotel', 'nested': {'fields': 'fake'}})
        finally:
            self.run_cbq_query(query="DROP INDEX test1.idx3 USING GSI", query_context="default:default.test")


    def test_primary(self):
        try:
            self.run_cbq_query(
                query="CREATE PRIMARY INDEX on default:default.{0}.{1} USING GSI".format(self.scope,self.collections[0]))
            self.wait_for_all_indexes_online()
            results = self.run_cbq_query(
                query='select * from default:default.test.test1 USE INDEX (idx1 USING GSI) where name = "old hotel"')
            self.assertEqual(results['results'], [{'test1': {'name': 'old hotel', 'type': 'hotel'}}, {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}}, {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
            results = self.run_cbq_query(
                query="SELECT * FROM default:default.test.test1 USE INDEX (idx4 USING GSI) WHERE ANY v in numbers SATISFIES v = 1 END")
            self.assertEqual(results['results'][0]['test1'], {'name': 'old hotel', 'numbers': [1, 2, 3, 4]})
            results = self.run_cbq_query(query="SELECT * FROM default:default.test.test1 b USE INDEX(idx3 USING GSI)WHERE b.nested.fields = 'fake'")
            self.assertEqual(results['results'][0]['b'], {'name': 'old hotel', 'nested': {'fields': 'fake'}})
        finally:
            self.run_cbq_query(query="DROP PRIMARY INDEX on default:default.{0}.{1} USING GSI".format(self.scope,self.collections[0]))

    def test_primary_context(self):
        try:
            self.run_cbq_query(
                query="CREATE PRIMARY INDEX on {1} USING GSI".format(self.scope, self.collections[0]), query_context="default:default.test")
            self.wait_for_all_indexes_online()
            results = self.run_cbq_query(
                query='select * from test1 USE INDEX (idx1 USING GSI) where name = "old hotel"', query_context="default:default.test")
            diffs = DeepDiff(results['results'], [{'test1': {'name': 'old hotel', 'type': 'hotel'}}, {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}}, {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
            if diffs:
                self.assertTrue(False, diffs)
            results = self.run_cbq_query(
                query="SELECT * FROM test1 USE INDEX (idx4 USING GSI) WHERE ANY v in numbers SATISFIES v = 1 END", query_context="default:default.test")
            diffs = DeepDiff(results['results'], [{'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
            if diffs:
                self.assertTrue(False, diffs)
            results = self.run_cbq_query(query="SELECT * FROM test1 b USE INDEX(idx3 USING GSI)WHERE b.nested.fields = 'fake'", query_context="default:default.test")
            diffs = DeepDiff(results['results'], [{'b': {'name': 'old hotel', 'nested': {'fields': 'fake'}}}])
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query(query="DROP PRIMARY INDEX on {1} USING GSI".format(self.scope,self.collections[0]), query_context="default:default.test")