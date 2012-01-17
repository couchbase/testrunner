import json
import random
import logger
import time
import unittest

from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from memcached.helper.data_helper import MemcachedError


# The SpatialHelper operates on a single bucket over a single RestConnection
# It inherites from unittest.Testcase so we can make assertions
class SpatialHelper(unittest.TestCase):
    def __init__(self, bucket):
        self.bucket = bucket
        self.servers = TestInputSingleton.input.servers
        self.master = self.servers[0]
        self.rest = RestConnection(self.master)
        self.log = logger.Logger.get_logger()

        # A list of indexes that were created with create_spatial_index_fun
        self._indexes = []


    def setup_cluster(self):
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        mem_quota = int(self.rest.get_nodes_self().mcdMemoryReserved *
                        node_ram_ratio)
        self.rest.init_cluster(self.master.rest_username,
                               self.master.rest_password)
        self.rest.init_cluster_memoryQuota(self.master.rest_username,
                                      self.master.rest_password,
                                      memoryQuota=mem_quota)
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        ClusterOperationHelper.wait_for_ns_servers_or_assert(
            [self.master], self)

        rebalanced = ClusterOperationHelper.add_and_rebalance(self.servers)
        self.assertTrue(rebalanced, "cluster is not rebalanced")
        self._create_default_bucket()


    def cleanup_cluster(self):
        if not "skip_cleanup" in TestInputSingleton.input.test_params:
            # Cleanup all indexes that were create with this helper class
            for name in self._indexes:
                self.rest.delete_spatial(self.bucket, name)
                self.log.info("deleted spatial {0} from bucket {1}"
                              .format(name, self.bucket))
            BucketOperationHelper.delete_all_buckets_or_assert(
                self.servers, self)
            ClusterOperationHelper.cleanup_cluster(self.servers)
            ClusterOperationHelper.wait_for_ns_servers_or_assert(
                self.servers, self)


    def create_index_fun(self, name):
        fun = "function (doc) {emit(doc.geometry, doc);}"
        function = self._create_function(name, fun)
        self.rest.create_spatial(self.bucket, name, function)
        self._indexes.append(name)


    # If you insert docs that are already there, they are simply
    # overwritten.
    # extra_values are key value pairs that will be added to the
    # JSON document
    def insert_docs(self, num_of_docs, prefix, extra_values={},
                    wait_for_persistence=True):
        moxi = MemcachedClientHelper.proxy_client(self.master, self.bucket)
        doc_names = []
        for i in range(0, num_of_docs):
            key = doc_name = "{0}-{1}".format(prefix, i)
            doc_names.append(doc_name)
            geom = {"type": "Point", "coordinates":
                        [random.randrange(-180, 180),
                         random.randrange(-90, 90)]}
            value = {"name": doc_name, "age": 1000, "geometry": geom}
            value.update(extra_values)
            # loop till value is set
            fail_count = 0
            while True:
                try:
                    moxi.set(key, 0, 0, json.dumps(value))
                    break
                except MemcachedError as e:
                    fail_count += 1
                    if (e.status == 133 or e.status == 132) and fail_count < 60:
                        if i == 0:
                            self.log.error("moxi not fully restarted, "
                                           "waiting 5 seconds. error {0}"
                                           .format(e))
                            time.sleep(5)
                        else:
                            self.log.error(e)
                            time.sleep(1)
                    else:
                        raise e
        if wait_for_persistence:
            RebalanceHelper.wait_for_stats(self.master, self.bucket,
                                           "ep_queue_size", 0)
            RebalanceHelper.wait_for_stats(self.master, self.bucket,
                                           "ep_flusher_todo", 0)
        self.log.info("inserted {0} json documents".format(num_of_docs))
        return doc_names


    # Returns the keys of the deleted documents
    # If you try to delete a document that doesn't exists, just skip it
    def delete_docs(self, num_of_docs, prefix):
        moxi = MemcachedClientHelper.proxy_client(self.master, self.bucket)
        doc_names = []
        for i in range(0, num_of_docs):
            key = "{0}-{1}".format(prefix, i)
            try:
                moxi.delete(key)
            except MemcachedError as e:
                if e.args[0] == "Memcached error #1:  Not found":
                    continue
                else:
                    raise
            doc_names.append(key)
        # Make sure the data is persisted
        RebalanceHelper.wait_for_stats(self.master, self.bucket,
                                       "ep_queue_size", 0)
        RebalanceHelper.wait_for_stats(self.master, self.bucket,
                                       "ep_flusher_todo", 0)
        self.log.info("deleted {0} json documents".format(len(doc_names)))
        return doc_names


    def get_results(self, spatial, limit=100, extra_params={}):
        for i in range(0, 4):
            try:
                start = time.time()
                #full_set=true&connection_timeout=60000&limit=10&skip=0
                params = {"connection_timeout": 60000}
                params.update(extra_params)
                # Make "full_set=true" the default
                if not "full_set" in params:
                    params["full_set"] = True
                results = self.rest.spatial_results(self.bucket, spatial,
                                                    params, limit)
                delta = time.time() - start
                if results:
                    self.log.info("spatial returned in {0} seconds"
                                  .format(delta))
                    self.log.info("was able to get spatial results after "
                                  "trying {0} times".format((i + 1)))
                    return results
            except Exception as ex:
                self.log.error("spatial_results not ready yet , try again "
                               "in 5 seconds... , error {0}".format(ex))
                time.sleep(5)
        self.fail("unable to get spatial_results for {0} after 4 tries"
                  .format(spatial))


    def info(self, spatial):
        return self.rest.spatial_info(self.bucket, spatial)


    def _create_function(self, name, function):
        #if this view already exist then get the rev and embed it here?
        doc = {"language": "javascript",
                "_id": "_design/{0}".format(name)}
        try:
            spatial_response = self.rest.get_spatial(self.bucket, name)
            if spatial_response:
                doc["_rev"] = spatial_response["_rev"]
        except:
            pass
        doc["spatial"] = {name: function}
        self.log.info("doc {0}".format(doc))
        return json.dumps(doc)


    #create a bucket if it doesn't exist
    def _create_default_bucket(self):
        helper = RestHelper(self.rest)
        if not helper.bucket_exists(self.bucket):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(
                self.servers)
            info = self.rest.get_nodes_self()
            available_ram = int(info.memoryQuota * node_ram_ratio)
            self.rest.create_bucket(bucket=self.bucket,
                                    ramQuotaMB=available_ram)
            ready = BucketOperationHelper.wait_for_memcached(self.master,
                                                             self.bucket)
            self.assertTrue(ready, "wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(self.bucket),
                        "unable to create {0} bucket".format(self.bucket))
