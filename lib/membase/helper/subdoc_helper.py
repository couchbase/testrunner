import json
import random
import logger
import time
import unittest
import copy

from TestInput import TestInputSingleton
from couchbase_helper.document import DesignDocument, View
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from memcached.helper.data_helper import MemcachedError
from memcached.helper.data_helper import VBucketAwareMemcached
from lib.mc_bin_client import MemcachedClient, MemcachedError
from lib.memcacheConstants import *

# The SubdocHelper operates on a single bucket over a single RestConnection
# The original testcase needs to be passed in so we can make assertions
class SubdocHelper:
    def __init__(self, testcase, bucket):
        self.testcase = testcase
        self.bucket = bucket
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.master = self.servers[0]
        self.rest = RestConnection(self.master)
        self.log = logger.Logger.get_logger()
        self.client = MemcachedClient(host=self.master.ip)
        self.jsonSchema = {
            "id" : "0",
            "number" : 0,
            "array" : [],
            "child" : {},
            "isDict" : True,
            "padding": None
        }
        self.jsonSchema_longPath = {
            "id" : "0",
            "number" : 0,
            "array12345678901234567890123456789" : [],
            "child12345678901234567890123456789" : {},
            "isDict" : True,
            "padding": None
        }

    def set_bucket(self, bucket):
        self.bucket = bucket

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
            [self.master], self.testcase)

        rebalanced = ClusterOperationHelper.add_and_rebalance(
                self.servers)
        self.testcase.assertTrue(rebalanced, "cluster is not rebalanced")

        self._create_default_bucket()

    def cleanup_cluster(self):
        if not "skip_cleanup" in TestInputSingleton.input.test_params:
            BucketOperationHelper.delete_all_buckets_or_assert(
                self.servers, self.testcase)
            ClusterOperationHelper.cleanup_cluster(self.servers)
            ClusterOperationHelper.wait_for_ns_servers_or_assert(
                self.servers, self.testcase)

    '''Recursive call to create a deep nested Dictionary JSON document '''
    def _createNestedJson(self, key, dict):
        if dict['levels'] == 0:
            return
        if dict['doc'] == {}:
            dict['doc'] = copy.copy(self.jsonSchema)
        else:
            dict['doc']['child'] = copy.copy(self.jsonSchema)
            dict['doc']['child']['array'] = []
            dict['doc'] = dict['doc']['child']

        dict['doc']['id'] = key
        dict['doc']['number'] = dict['levels']

        for level in range(0, dict['levels']):
            dict['doc']['array'].append(level)
        return self._createNestedJson(key, {'doc': dict['doc'], 'levels': dict['levels']-1})

    '''Recursive call to create a deep nested Array JSON document
    This should be changed to make it as recursive for array'''
    def _createNestedJsonArray(self, key, dict):
        self.array = [[[1, 2, 3, True, True], 200, 300], 20, 30, [1000, 2000, 3000]]
        if dict['levels'] == 0:
            return
        if dict['doc'] == {}:
            dict['doc'] = copy.copy(self.jsonSchema)
        else:
            dict['doc']['child'] = copy.copy(self.jsonSchema)
            dict['doc']['child']['array'] = []
            dict['doc'] = dict['doc']['child']

        dict['doc']['id'] = key
        dict['doc']['number'] = dict['levels']

        for level in range(0, dict['levels']):
            dict['doc']['array'].append(level)
            dict['doc']['array'][level] = self.array
        return self._createNestedJson(key, {'doc': dict['doc'], 'levels': dict['levels']-1})

    '''Recursive call to create a deep nested long path Dictionary JSON document '''
    def _createNestedJson_longPath(self, key, dict):
        if dict['levels'] == 0:
            return
        if dict['doc'] == {}:
            dict['doc'] = copy.copy(self.jsonSchema_longPath)
        else:
            dict['doc']['child12345678901234567890123456789'] = copy.copy(self.jsonSchema)
            dict['doc']['child12345678901234567890123456789']['array12345678901234567890123456789'] = []
            dict['doc'] = dict['doc']['child12345678901234567890123456789']

        dict['doc']['id'] = key
        dict['doc']['number'] = dict['levels']

        for level in range(0, dict['levels']):
            dict['doc']['array12345678901234567890123456789'].append(level)
        return self._createNestedJson(key, {'doc': dict['doc'], 'levels': dict['levels']-1})


    def insert_nested_docs(self, num_of_docs, prefix='doc', levels=16, size=512, return_docs=False, long_path=False,collection=None):
        rest = RestConnection(self.master)
        smart = VBucketAwareMemcached(rest, self.bucket)
        doc_names = []

        dict = {'doc' : {}, 'levels' : levels }

        for i in range(0, num_of_docs):
            key = doc_name = "{0}-{1}".format(prefix, i)
            if long_path:
                self._createNestedJson_longPath(key, dict)
            else:
                self._createNestedJson(key, dict)
            value = dict['doc']
            if not return_docs:
                doc_names.append(doc_name)
            else:
                doc_names.append(value)
                # loop till value is set
            fail_count = 0
            while True:
                try:
                    smart.set(key, 0, 0, json.dumps(value), collection=collection)
                    break
                except MemcachedError as e:
                    fail_count += 1
                    if (e.status == 133 or e.status == 132) and fail_count < 60:
                        if i == 0:
                            self.log.error("waiting 5 seconds. error {0}"
                            .format(e))
                            time.sleep(5)
                        else:
                            self.log.error(e)
                            time.sleep(1)
                    else:
                        raise e
        self.log.info("Inserted {0} json documents".format(num_of_docs))
        return doc_names

    # If you insert docs that are already there, they are simply
    # overwritten.
    # extra_values are key value pairs that will be added to the
    # JSON document
    # If `return_docs` is true, it'll return the full docs and not
    # only the keys
    def insert_nested_specific_docs(self, num_of_docs, prefix='doc', extra_values={},
                    return_docs=False,collection=None):
        random.seed(12345)
        rest = RestConnection(self.master)
        smart = VBucketAwareMemcached(rest, self.bucket)
        doc_names = []
        for i in range(0, num_of_docs):
            key = doc_name = "{0}-{1}".format(prefix, i)
            geom = {"type": "Point", "coordinates":
                        [random.randrange(-180, 180),
                         random.randrange(-90, 90)]}
            value = {
                "padding": None,
                "d1" :{
                    "padding": None,
                    "d2" :{
                        "int_array" : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        "str_array" :["john", "doe", "john", "block", "jim", "john"],
                        "mix_array" : [1, 2, True, False, 'bird', 5.0, 6.0, repr(123)],
                        "d3" : {
                            "d4_01" : 1,
                            "d4_02" : [21, 22, 23, 24, 25 ],
                            "d4_03" : False,
                            "d4_04" : "San Francisco",
                            "d4_05" : {
                                "d5_01" : random.randrange(6, 13),
                                "d5_02" : [ random.randrange(5, 10), random.randrange(6, 13)],
                                "d5_03" : "abcdefghi",
                                "d5_04" : {
                                   "d6_01" : random.randrange(6, 13),
                                   "d6_02" : [1, 2, True, False, 'bird', 5.0, 6.0, repr(123)]
                                }
                            }
                        },
                        "d2_02" : {"d2_02_01":"name"},
                        "d2_03" :geom
                    },
                    "d1_02" :[1, 2, True, False, 'bird', 5.0, 6.0, repr(123)],
                    "d1_03" : False
                },
                "age": random.randrange(1, 1000),
                "geometry": geom,
                "array" :[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20],
                "isDict" : True,
                "dict_value" : {"name":"abc", "age":1},
                "height": random.randrange(1, 13000),
                "bloom": random.randrange(1, 6),
                "shed_leaves": random.randrange(6, 13)}
            value.update(extra_values)
            if not return_docs:
                doc_names.append(doc_name)
            else:
                doc_names.append(value)
            # loop till value is set
            fail_count = 0
            while True:
                try:
                    smart.set(key, 0, 0, json.dumps(value), collection=collection)
                    break
                except MemcachedError as e:
                    fail_count += 1
                    if (e.status == 133 or e.status == 132) and fail_count < 60:
                        if i == 0:
                            self.log.error("waiting 5 seconds. error {0}"
                                           .format(e))
                            time.sleep(5)
                        else:
                            self.log.error(e)
                            time.sleep(1)
                    else:
                        raise e
        self.log.info("Inserted {0} json documents".format(num_of_docs))
        return doc_names

    def insert_docs(self, num_of_docs, prefix='doc', extra_values={},
                    return_docs=False,collection=None):
        random.seed(12345)
        rest = RestConnection(self.master)
        smart = VBucketAwareMemcached(rest, self.bucket)
        doc_names = []
        for i in range(0, num_of_docs):
            key = doc_name = "{0}-{1}".format(prefix, i)
            geom = {"type": "Point", "coordinates":
                [random.randrange(-180, 180),
                 random.randrange(-90, 90)]}
            value = {
                "name": doc_name,
                "age": random.randrange(1, 1000),
                "geometry": geom,
                "array" :[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20],
                "isDict" : True,
                "dict_value" : {"name":"abc", "age":1},
                "height": random.randrange(1, 13000),
                "bloom": random.randrange(1, 6),
                "shed_leaves": random.randrange(6, 13)}
            value.update(extra_values)
            if not return_docs:
                doc_names.append(doc_name)
            else:
                doc_names.append(value)
                # loop till value is set
            fail_count = 0
            while True:
                try:
                    smart.set(key, 0, 0, json.dumps(value), collection=collection)
                    break
                except MemcachedError as e:
                    fail_count += 1
                    if (e.status == 133 or e.status == 132) and fail_count < 60:
                        if i == 0:
                            self.log.error("waiting 5 seconds. error {0}"
                            .format(e))
                            time.sleep(5)
                        else:
                            self.log.error(e)
                            time.sleep(1)
                    else:
                        raise e
        self.log.info("Inserted {0} json documents".format(num_of_docs))
        return doc_names

    def generate_matching_docs(self, docs_inserted, params, value=None):
        pass

    def verify_matching_keys(self, expected, current):
        pass

    # Returns the keys of the deleted documents
    # If you try to delete a document that doesn't exists, just skip it
    def delete_docs(self, num_of_docs, prefix='doc'):
        pass

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
            self.testcase.fail("Returned only {0} "
                               "docs and not {1} as expected"
                               .format(len(result), len(inserted)))


    def _print_keys_not_found(self, keys_not_found, how_many=10):
        how_many = min(how_many, len(keys_not_found))

        for i in range(0, how_many):
            self.log.error("did not find key {0} in the results"
                           .format(keys_not_found[i]))
