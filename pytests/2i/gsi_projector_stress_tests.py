from .gsi_index_partitioning import GSIIndexPartitioningTests
from lib.remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper
from lib.memcached.helper.data_helper import MemcachedClientHelper
import random
from threading import Thread
from lib import testconstants


class GSIProjectorTests(GSIIndexPartitioningTests):
    def setUp(self):
        super(GSIProjectorTests, self).setUp()
        self.doc_size = self.input.param("doc_size", 1024)
        self.number_of_documents = self.input.param("number_of_documents", 300000)
        self.concurrent_load = self.input.param("concurrent_load", False)

    def tearDown(self):
        super(GSIProjectorTests, self).tearDown()

    def test_multiple_buckets_index_dgm(self):
        create_index_query = "CREATE INDEX idx ON default (DISTINCT `travel-details`)"
        create_index_query2 = "CREATE INDEX idx1 ON default(body) USING GSI"
        create_index_query3 = "CREATE INDEX idx2 ON default(age) where age > 30 USING GSI"
        create_index_query4 = "CREATE INDEX idx3 ON default(name) USING GSI WITH {{'num_replica': {0}}};".format(self.num_index_replicas)
        create_index_query5 = "CREATE INDEX pidx1 ON default(name,age) partition by hash(BASE64(meta().id)) USING GSI"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query4,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query5,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        for bucket_number in range(self.standard_buckets):
            create_index_query = "CREATE INDEX idx ON standard_bucket%s(DISTINCT `travel-details`)" % bucket_number
            create_index_query2 = "CREATE INDEX idx1 ON standard_bucket%s(body) USING GSI" % bucket_number
            create_index_query3 = "CREATE INDEX idx2 ON standard_bucket%s(age) where age > 30 USING GSI" % bucket_number
            create_index_query4 = "CREATE INDEX idx3 ON standard_bucket{0}(name) USING GSI WITH {{'num_replica': {1}}};".format(
                bucket_number, self.num_index_replicas)
            create_index_query5 = "CREATE INDEX pidx1 ON standard_bucket%s(name,age) partition by hash(BASE64(meta().id)) USING GSI" % bucket_number
            try:
                self.n1ql_helper.run_cbq_query(query=create_index_query,
                                               server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                               server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                               server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=create_index_query4,
                                               server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=create_index_query5,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                self.fail(
                    "index creation failed with error : {0}".format(str(ex)))


        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=256)

        for bucket in self.buckets:
            self.shell.execute_cbworkloadgen(self.rest.username, self.rest.password, self.number_of_documents, 100, bucket.name, self.doc_size, '-j')

        create_index_query = "CREATE INDEX idx4 ON default(age) where age < 15 USING GSI"

        if self.concurrent_load:
            t1 = Thread(target=self.shell.execute_cbworkloadgen, name="load_docs", args=(self.rest.username,
                                                                                         self.rest.password,
                                                                                         self.number_of_documents, 100,
                                                                                         "default", self.doc_size, '-j'))
            t1.start()
            self.sleep(1)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))
        if self.concurrent_load:
            t1.join()

        self.wait_until_specific_index_online(index_name='idx4', timeout=900)
        self.wait_until_indexes_online()

    def test_multiple_buckets_concurrent_build(self):
        create_index_query2 = "CREATE INDEX idx1 ON default(body) USING GSI WITH {'defer_build':true}"
        create_index_query3 = "CREATE INDEX idx2 ON default(age) where age > 30 USING GSI WITH {'defer_build':true}"
        create_index_query4 = "CREATE INDEX idx3 ON default(name) USING GSI WITH {{'num_replica': {0}, 'defer_build':true}};".format(self.num_index_replicas)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query4,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        for bucket_number in range(self.standard_buckets):
            create_index_query2 = "CREATE INDEX idx1 ON standard_bucket%s(body) USING GSI WITH {'defer_build':true}" % bucket_number
            create_index_query3 = "CREATE INDEX idx2 ON standard_bucket%s(age) where age > 30 USING GSI WITH {'defer_build':true}" % bucket_number
            create_index_query4 = "CREATE INDEX idx3 ON standard_bucket{0}(name) USING GSI WITH {{'num_replica': {1}, 'defer_build':true}};".format(
                bucket_number, self.num_index_replicas)
            try:
                self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                               server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                               server=self.n1ql_node)
                self.n1ql_helper.run_cbq_query(query=create_index_query4,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                self.fail(
                    "index creation failed with error : {0}".format(str(ex)))


        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=256)

        for bucket in self.buckets:
            self.shell.execute_cbworkloadgen(self.rest.username, self.rest.password, self.number_of_documents, 100, bucket.name, self.doc_size, '-j')

        for bucket in self.buckets:
            build_index_query = "BUILD INDEX ON %s(idx1,idx2,idx3) USING GSI" % bucket.name
            try:
                self.n1ql_helper.run_cbq_query(query=build_index_query,
                                               server=self.n1ql_node)
            except Exception as ex:
                self.log.info(str(ex))
                self.fail(
                    "index creation failed with error : {0}".format(str(ex)))

        self.wait_until_indexes_online()