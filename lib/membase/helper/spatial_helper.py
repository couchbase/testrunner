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
import memcacheConstants


# The SpatialHelper operates on a single bucket over a single RestConnection
# The original testcase needs to be passed in so we can make assertions
class SpatialHelper:
    def __init__(self, testcase, bucket):
        self.testcase = testcase
        self.bucket = bucket
        self.servers = TestInputSingleton.input.servers
        self.input = TestInputSingleton.input
        self.master = self.servers[0]
        self.rest = RestConnection(self.master)
        self.log = logger.Logger.get_logger()

        # A set of indexes that were created with create_spatial_index_fun
        # It's a set, so indexes can be updated without problems
        self._indexes = set([])


    def setup_cluster(self, do_rebalance=True):
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
            [self.master], self.testcase)

        if do_rebalance:
            rebalanced = ClusterOperationHelper.add_and_rebalance(
                self.servers)
            self.testcase.assertTrue(rebalanced, "cluster is not rebalanced")

        self._create_default_bucket()


    def cleanup_cluster(self):
        if not "skip_cleanup" in TestInputSingleton.input.test_params:
            # Cleanup all indexes that were create with this helper class
            for name in self._indexes:
                self.rest.delete_spatial(self.bucket, name)
                self.log.info("deleted spatial {0} from bucket {1}"
                              .format(name, self.bucket))
            BucketOperationHelper.delete_all_buckets_or_assert(
                self.servers, self.testcase)
            ClusterOperationHelper.cleanup_cluster(self.servers)
            ClusterOperationHelper.wait_for_ns_servers_or_assert(
                self.servers, self.testcase)


    def create_index_fun(self, name, prefix='', fun=None):
        if fun is None:
            fun = 'function (doc, meta) {if(meta.id.indexOf("' + prefix + \
                '") != -1) { emit(doc.geometry, doc);}}'
        function = self._create_function(name, fun)
        self.rest.create_spatial(self.bucket, name, function)
        self._indexes.add(name)


    # If you insert docs that are already there, they are simply
    # overwritten.
    # extra_values are key value pairs that will be added to the
    # JSON document
    # If `return_docs` is true, it'll return the full docs and not
    # only the keys
    def insert_docs(self, num_of_docs, prefix, extra_values={},
                    wait_for_persistence=True, return_docs=False):
        moxi = MemcachedClientHelper.proxy_client(self.master, self.bucket)
        doc_names = []
        for i in range(0, num_of_docs):
            key = doc_name = "{0}-{1}".format(prefix, i)
            geom = {"type": "Point", "coordinates":
                        [random.randrange(-180, 180),
                         random.randrange(-90, 90)]}
            value = {"name": doc_name, "age": 1000, "geometry": geom}
            value.update(extra_values)
            if not return_docs:
                doc_names.append(doc_name)
            else:
                doc_names.append(value)
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
            self.wait_for_persistence()
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
                # Don't care if we try to delete a document that doesn't exist
                if e.status == memcacheConstants.ERR_NOT_FOUND:
                    continue
                else:
                    raise
            doc_names.append(key)

        self.wait_for_persistence(180)

        self.log.info("deleted {0} json documents".format(len(doc_names)))
        return doc_names

    # If `num_expected` is passed in, the number of returned results will
    # be checked. If it doesn't match try again to get the results
    def get_results(self, spatial, limit=None, extra_params={},
                    num_expected=None):
        start = time.time()
        for i in range(0, 4):
            try:
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
                    # Keep on trying until we have at least the number
                    # of expected rows
                    if (num_expected is not None) and \
                            (len(results["rows"]) < num_expected):
                        continue

                    self.log.info("spatial returned in {0} seconds"
                                  .format(delta))
                    self.log.info("was able to get spatial results after "
                                  "trying {0} times".format((i + 1)))
                    return results
            except Exception as ex:
                self.log.error("spatial_results not ready yet , try again "
                               "in 5 seconds... , error {0}".format(ex))
                time.sleep(5)

        # Can't get the correct result, fail the test
        self.log.info("num_expected: {0}".format(num_expected))
        self.log.info("num results:  {0}".format(len(results["rows"])))
        self.testcase.fail(
            "unable to get spatial_results for {0} after 4 tries"
            .format(spatial))


    def info(self, spatial):
        return self.rest.spatial_info(self.bucket, spatial)


    # A request to perform the compaction normally returns immediately after
    # it is starting, without waiting until it's completed. This function
    # call keeps blocking until the compaction is done.
    # If you pass in False as a second parameter, it won't block and return
    # immediately
    # Returns True if the compaction succeded within the given timeout
    def compact(self, spatial, block=True, timeout=60):
        status, value = self.rest.spatial_compaction(self.bucket, spatial)
        if not status:
            raise Exception("Compaction returned error.")

        while True:
            status, info = self.info(spatial)
            if not info["spatial_index"]["compact_running"]:
                return True
            elif timeout < 0:
                raise Exception("Compaction timed out.")
            else:
                time.sleep(1)
                timeout -= 1


    def _create_function(self, name, function):
        #if this view already exist then get the rev and embed it here?
        doc = {"language": "javascript"}
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
            if available_ram < 256:
                available_ram = 256
            self.rest.create_bucket(bucket=self.bucket,
                                    ramQuotaMB=available_ram)
            ready = BucketOperationHelper.wait_for_memcached(self.master,
                                                             self.bucket)
            self.testcase.assertTrue(ready, "wait_for_memcached failed")
        self.testcase.assertTrue(
            helper.bucket_exists(self.bucket),
            "unable to create {0} bucket".format(self.bucket))


    # Return the keys (document ids) of a spatial view response
    def get_keys(self, results):
        keys = []
        if results:
            rows = results["rows"]
            for row in rows:
                keys.append(row["id"].encode("ascii", "ignore"))
            self.log.info("there are {0} keys".format(len(keys)))
        return keys


    # Verify that the built index is correct. Wait until all data got
    # persited on disk
    # If `full_docs` is true, the whole documents, not only the keys
    # will be verified
    # Note that the resultset might be way bigger, we only check if
    # the keys that should be inserted, were really inserted (i.e. that
    # there might be additional keys returned)
    def query_index_for_verification(self, design_name, inserted,
                                     full_docs=False,
                                     wait_for_persistence=True):
        if wait_for_persistence:
            self.wait_for_persistence()
        results = self.get_results(design_name, num_expected=len(inserted))
        result_keys = self.get_keys(results)

        if not full_docs:
            self.verify_result(inserted, result_keys)
        else:
            # The default spatial view function retrurns the full
            # document, hence the values can be used for the
            # meta information as well
            # Only verify things we can get from the value, hence not
            # the revision, nor the bbox. For the ID we use the "name"
            # of the value
            inserted_expanded = []
            for value in inserted:
                inserted_expanded.append(
                    json.dumps({'id': value['name'],
                                'geometry': value['geometry'],
                                'value': value}, sort_keys=True))

            results_collapsed = []
            for row in results['rows']:
                # Delete all top level key-values that are not part of the
                # inserted_expanded list
                del_keys = set(row.keys()) - set(['id', 'geometry', 'value'])
                for key in del_keys:
                    del row[key]
                # Delete all special values inserted by CouchDB or Couchbase
                for key in row['value'].keys():
                    if key.startswith('_') or key.startswith('$'):
                        del row['value'][key]
                results_collapsed.append(json.dumps(row, sort_keys=True))

            diff = set(inserted_expanded) - set(results_collapsed)
            self.testcase.assertEqual(diff, set())


    # Compare the inserted documents with the returned result
    # Both arguments contain a list of document names
    def verify_result(self, inserted, result):
        #not_found = []
        #for key in inserted:
        #    if not key in result:
        #        not_found.append(key)
        not_found = list(set(inserted) - set(result))
        if not_found:
            self._print_keys_not_found(not_found)
            self.testcase.fail("the spatial function did return only {0} "
                               "docs and not {1} as expected"
                               .format(len(result), len(inserted)))


    def _print_keys_not_found(self, keys_not_found, how_many=10):
        how_many = min(how_many, len(keys_not_found))

        for i in range(0, how_many):
            self.log.error("did not find key {0} in the spatial view results"
                           .format(keys_not_found[i]))

    def wait_for_persistence(self, timeout=120):
        RebalanceHelper.wait_for_persistence(self.master, self.bucket, timeout)
