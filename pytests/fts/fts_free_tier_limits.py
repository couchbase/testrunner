# coding=utf-8

import json
import threading

from lib import global_vars
from lib.SystemEventLogLib.fts_service_events import SearchServiceEvents
from pytests.security.rbac_base import RbacBase
from .fts_base import FTSBaseTest
from .fts_base import NodeHelper
from lib.membase.api.rest_client import RestConnection
from deepdiff import DeepDiff
import time


class FtsFreeTierLimits(FTSBaseTest):

    def setUp(self):
        super(FtsFreeTierLimits, self).setUp()
        self.rest = RestConnection(self._cb_cluster.get_master_node())
        self.fts_rest = RestConnection(self._cb_cluster.get_random_fts_node())
        self.sample_bucket_name = "travel-sample"
        self.sample_index_name = "idx_travel_sample_fts"
        self.sample_index_name_1 = "idx_travel_sample_fts1"
        self.load_sample_buckets(self._cb_cluster.get_master_node(), self.sample_bucket_name)
        self.sample_query = {"match": "United States", "field": "country"}
        self.limit = self._input.param("limit", "num_queries_per_min")
        self.limit_value = self._input.param("limit_value", 1)
        self.fts_service_limits = f'{{"fts": {{"{self.limit}": {self.limit_value}}}}}'
        self.remove_user("testuser1")
        self.remove_user("testuser2")
        scope_limt = self._input.param("scope_limt", False)
        self.testuser1 = {'id': 'testuser1', 'name': 'testuser1', 'password': 'password', 'roles': 'admin'}
        if not scope_limt:
            self.testuser1["limits"] = self.fts_service_limits
        self.testuser2 = {'id': 'testuser2', 'name': 'testuser2', 'password': 'password', 'roles': 'admin'}
        testusers = [self.testuser1, self.testuser2]
        RbacBase().create_user_source(testusers, 'builtin', self.master)
        RbacBase().add_user_role(testusers, RestConnection(self.master), 'builtin')

        enforce_limits = self._input.param("enforce_limits", True)
        RestConnection(self.master).set_internalSetting("enforceLimits", enforce_limits)

    def tearDown(self):
        super(FtsFreeTierLimits, self).tearDown()

    def remove_user(self, name):
        try:
            self.log.info("Removing user" + name + "...")
            RbacBase().remove_user_role([name], RestConnection(
                self.master))
        except Exception as e:
            self.log.info(e)

    def test_set_limits(self):
        self.testuser1["limits"] = '{"fts":{"num_concurrent_requests": 2, "num_queries_per_min": 5, "ingress_mib_per_min": 10, "egress_mib_per_min": 10}}'
        self.testuser2["limits"] = '{"fts":{"num_concurrent_requests": 1, "num_queries_per_min": 3, "ingress_mib_per_min": 12, "egress_mib_per_min": 14}}'
        RbacBase().add_user_role([self.testuser1, self.testuser2], RestConnection(self.master), 'builtin')

        status, user1_config = RestConnection(self.master).get_user_group("testuser1")
        self.log.info(user1_config)
        status, user2_config = RestConnection(self.master).get_user_group("testuser2")
        self.log.info(user2_config)

        diffs = DeepDiff(user1_config['limits'], json.loads(self.testuser1["limits"]), ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        diffs = DeepDiff(user2_config['limits'], json.loads(self.testuser2["limits"]), ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_scope_limit_num_fts_indexes(self):
        test_pass = False
        limit_scope = "inventory"
        not_limit_scope = "tenant_agent_00"
        self.fts_rest.set_fts_tier_limit(bucket=self.sample_bucket_name, scope=limit_scope, limit=self.limit_value)

        self.container_type = "collection"
        self.scope = limit_scope
        self.collection = ["airline", "airport", "hotel", "landmark", "route"]
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        for i in range(self.limit_value):
            self._cb_cluster.create_fts_index(name=f'{self.sample_index_name}_{i}', source_name=self.sample_bucket_name,
                                              collection_index=collection_index, _type=_type, scope=index_scope,
                                              collections=index_collections)

        self.wait_for_indexing_complete()
        try:
            self._cb_cluster.create_fts_index(name=f'{self.sample_index_name}_{i+2}', source_name=self.sample_bucket_name,
                                              collection_index=collection_index, _type=_type, scope=index_scope,
                                              collections=index_collections)
        except Exception as e:
            self.log.info(str(e))
            if "num_fts_indexes" not in str(e):
                self.fail("expected error message not found")
            else:
                test_pass = True

        self.scope = not_limit_scope
        self.collection = ["bookings", "users"]
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        for i in range(self.limit_value+2):
            self._cb_cluster.create_fts_index(name=f'{self.sample_index_name_1}_{i}', source_name=self.sample_bucket_name,
                                              collection_index=collection_index, _type=_type, scope=index_scope,
                                              collections=index_collections)

        if not test_pass:
            self.fail("Could able to create index even after reaching the limit")

    def test_user_limit_num_queries_per_min(self):
        self.container_type = "collection"
        self.scope = "inventory"
        self.collection = ["airline", "airport", "hotel", "landmark", "route"]
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        fts_index = self._cb_cluster.create_fts_index(name=self.sample_index_name, source_name=self.sample_bucket_name,
                                          collection_index=collection_index, _type=_type, scope=index_scope,
                                          collections=index_collections)
        self.wait_for_indexing_complete()
        self.fts_rest.username = self.testuser1["id"]
        self.fts_rest.password = self.testuser1["password"]
        for i in range(self.limit_value):
            hits, matches, _, status= fts_index.execute_query(self.sample_query,
                                                              rest=self.fts_rest,
                                                              zero_results_ok=True,
                                                              expected_hits=None,
                                                              expected_no_of_results=None)
            self.log.info("Hits: %s" % hits)
            if hits == -1:
                self.fail("Queries are failing within the limit")
        hits, matches, _, status= fts_index.execute_query(self.sample_query,
                                                          rest=self.fts_rest,
                                                          zero_results_ok=True,
                                                          expected_hits=None,
                                                          expected_no_of_results=None)
        self.log.info("Hits: %s" % hits)
        self.log.info("matches: %s" % matches)
        if hits != -1 or "num_queries_per_min" not in matches:
            self.fail("expected error message not found")
        self.sleep(60)
        hits, matches, _, status= fts_index.execute_query(self.sample_query,
                                                          rest=self.fts_rest,
                                                          zero_results_ok=True,
                                                          expected_hits=None,
                                                          expected_no_of_results=None)
        self.log.info("Hits: %s" % hits)
        if hits == -1:
            self.fail("Queries are failing even after 1 min")

        self.fts_rest.username = self.testuser2["id"]
        self.fts_rest.password = self.testuser2["password"]
        for i in range(self.limit_value+2):
            hits, matches, _, status= fts_index.execute_query(self.sample_query,
                                                              rest=self.fts_rest,
                                                              zero_results_ok=True,
                                                              expected_hits=None,
                                                              expected_no_of_results=None)
            self.log.info("Hits: %s" % hits)

    def test_user_limit_egress_mib_per_min(self):
        test_pass = False
        self.container_type = "collection"
        self.scope = "inventory"
        self.collection = ["airline", "airport", "hotel", "landmark", "route"]
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        fts_index = self._cb_cluster.create_fts_index(name=self.sample_index_name, source_name=self.sample_bucket_name,
                                                      collection_index=collection_index, _type=_type, scope=index_scope,
                                                      collections=index_collections)
        self.wait_for_indexing_complete()
        cluster = fts_index.get_cluster()
        self.fts_rest.username = self.testuser2["id"]
        self.fts_rest.password = self.testuser2["password"]
        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": "United"}, "size": 1080}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)

        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": "United"}, "size": 1080}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)
        self.fts_rest.username = self.testuser1["id"]
        self.fts_rest.password = self.testuser1["password"]

        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": "United"}, "size": 1080}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)

        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": "United"}, "size": 10}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)

        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": "United"}, "size": 10}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)

        self.log.info(str(all_matches))
        if all_hits != -1 or "egress_mib_per_min" not in all_matches:
            self.fail("expected error message with egress_mib_per_min not found")
        else:
            test_pass = True
        self.sleep(60)

        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": "United"}, "size": 1080}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)

        if all_hits == -1:
            self.fail("Query is failing even after 1 min")

        if not test_pass:
            self.fail("Query did not fail even after reaching the limit")

    def test_user_limit_ingress_mib_per_min(self):
        test_pass = False
        self.container_type = "collection"
        self.scope = "inventory"
        self.collection = ["airline", "airport", "hotel", "landmark", "route"]
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        fts_index = self._cb_cluster.create_fts_index(name=self.sample_index_name, source_name=self.sample_bucket_name,
                                                      collection_index=collection_index, _type=_type, scope=index_scope,
                                                      collections=index_collections)
        self.wait_for_indexing_complete()
        cluster = fts_index.get_cluster()
        self.fts_rest.username = self.testuser2["id"]
        self.fts_rest.password = self.testuser2["password"]
        search_string = "a" * 1048576
        size_search_string = len(search_string.encode('utf-8'))
        self.log.info(f'size of search string : {size_search_string}')
        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": search_string}, "size": 1080}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)

        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": search_string}, "size": 1080}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)
        self.fts_rest.username = self.testuser1["id"]
        self.fts_rest.password = self.testuser1["password"]

        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": search_string}, "size": 1080}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)

        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": "United"}, "size": 10}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)
        self.log.info(str(all_matches))
        if all_hits != -1 or "ingress_mib_per_min" not in all_matches:
            self.fail("expected error message with egress_mib_per_min not found")
        else:
            test_pass = True

        self.sleep(60)

        fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": "United"}, "size": 1080}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_index.name, fts_query, rest=self.fts_rest)
        if all_hits == -1:
            self.fail("Query is failing even after 1 min")

        if not test_pass:
            self.fail("Query did not fail even after reaching the limit")

    def test_user_limit_num_concurrent_requests(self):

        self.test_pass = False
        self.container_type = "collection"
        self.scope = "inventory"
        self.collection = ["airline", "airport", "hotel", "landmark", "route"]
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        fts_index = self._cb_cluster.create_fts_index(name=self.sample_index_name, source_name=self.sample_bucket_name,
                                                      collection_index=collection_index, _type=_type, scope=index_scope,
                                                      collections=index_collections)
        self.wait_for_indexing_complete()
        cluster = fts_index.get_cluster()
        self.fts_rest.username = self.testuser1["id"]
        self.fts_rest.password = self.testuser1["password"]

        threads = []
        for i in range(self.limit_value+1):
            threads.append(threading.Thread(target=self.run_fts_query_wrapper, args=(fts_index, cluster, 60, self.fts_rest, True)))

        fts_rest2 = RestConnection(self._cb_cluster.get_random_fts_node())
        fts_rest2.username = self.testuser2["id"]
        fts_rest2.password = self.testuser2["password"]
        for i in range(self.limit_value+1):
            threads.append(threading.Thread(target=self.run_fts_query_wrapper, args=(fts_index, cluster, 60, fts_rest2, False)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        if not self.test_pass:
            self.fail("Expected error message not found")

    def run_fts_query_wrapper(self, index, cluster, time_limit, fts_rest, expect_failure):
        start_time = time.time()
        while time.time() - start_time < time_limit:
            fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": "United"}, "size": 100}
            all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, fts_query, rest=fts_rest)
            self.log.info(all_hits)
            if all_hits == -1 and "num_concurrent_requests" in all_matches:
                self.log.info("Expected error message with num_concurrent_requests found")
                self.log.info(all_matches)
                self.test_pass = True
                break
            if all_hits == -1 and "num_concurrent_requests" not in all_matches:
                self.log.info(all_matches)
                self.fail("Expected error message with num_concurrent_requests NOT found")
            if all_hits == -1 and not expect_failure:
                self.log.info(all_matches)
                self.fail(f'Failure not expected with user {fts_rest.username}')

            fts_query = {"explain": True, "fields": ["*"], "highlight": {}, "query": {"query": "States"}, "size": 100}
            all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, fts_query, rest=self.fts_rest)
            self.log.info(all_hits)
            if all_hits == -1 and "num_concurrent_requests" in all_matches:
                self.log.info("Expected error message with num_concurrent_requests found")
                self.log.info(all_matches)
                self.test_pass = True
                break
            if all_hits == -1 and "num_concurrent_requests" not in all_matches:
                self.log.info(all_matches)
                self.fail("Expected error message with num_concurrent_requests NOT found")