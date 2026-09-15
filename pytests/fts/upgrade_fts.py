import contextlib
import logging
import json
import time
import uuid

from remote.remote_util import RemoteMachineShellConnection
from newupgradebasetest import NewUpgradeBaseTest
from .fts_callable import FTSCallable
from scripts.java_sdk_setup import JavaSdkSetup
from .fts_base import (CouchbaseCluster, FTSIndex, FTSBaseTest, _ScanPlusHashMap, UDFHelper,
                       FTSConcurrentWorkload, FTSPostChangeValidator)
from lib.membase.helper.bucket_helper import BucketOperationHelper
from couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.rest_client import RestConnection
from lib.collection.collections_cli_client import CollectionsCLI
from .fts_backup_restore import FTSIndexBackupClient
from .fts_encryption_base import EARUpgradeHelper
from security.rbac_base import RbacBase


log = logging.getLogger(__name__)


class UpgradeFTS(NewUpgradeBaseTest):

    LEGACY_VECTOR_INDEX = "vec_legacy_idx"
    FEATURE_VECTOR_INDEX = "vec_feature_idx"

    EAR_INDEX = "fts_ear_upgrade_idx"

    # CBQE-8244
    CHAINED_INDEX = "chained_upgrade_idx"

    SEARCH_HISTORY_INDEX = "search_history_upgrade_idx"
    DEEP_PAGINATION_INDEX = "deep_pagination_upgrade_idx"
    COLLECTIONS_INDEX = "collections_scale_upgrade_idx"
    HIERARCHICAL_INDEX = "hierarchical_upgrade_idx"
    EAR_QUERY = {"query": "dept:Engineering"}

    def setUp(self):
        super(UpgradeFTS, self).setUp()

        self.initial_version = self.input.param('initial_version', '6.6.1-9213')
        self.upgrade_to = self.input.param("upgrade_to")
        if not self.upgrade_to and self.upgrade_versions:
            self.upgrade_to = self.upgrade_versions[-1]
            self.log.info(f"upgrade_to not set; using the last hop of "
                          f"upgrade_version: {self.upgrade_to}")
        self.cb_cluster = CouchbaseCluster("C1", self.servers, self.log)
        self._cb_cluster = self.cb_cluster

        self.vector_dimension = self.input.param("dimension", 128)
        self.vector_recall_tolerance = self.input.param("vector_recall_tolerance", 5)
        self.vector_index_build_wait = self.input.param("vector_index_build_wait", 150)

        self.ear_reencryption_timeout = self.input.param("ear_reencryption_timeout", 900)

        self.upgrade_workload_errors = []

        self.java_sdk_client = self.input.param("java_sdk_client", False)
        self.fts_port = 8094
        if self.java_sdk_client:
            self.log.info("Building docker image with java sdk client")
            JavaSdkSetup()

        self.__setup_for_test()

    def setup_es(self):
        """
        Setup Elastic search - create empty index node defined under
        'elastic' section in .ini
        """
        self.create_index_es()

    def create_index_es(self, index_name=None):
        if index_name is None:
            index_name = FTSBaseTest.get_es_index_name()
        self.es.create_empty_index_with_bleve_equivalent_std_analyzer(index_name)
        self.log.info("Created empty index %s on Elastic Search node with "
                      "custom standard analyzer(default)"
                      % index_name)

    def __setup_for_test(self):
        self._set_bleve_max_result_window()
        self.__create_buckets()

    def __create_buckets(self):
        bucket_priority = None
        bucket_size = 200
        _num_replicas = 1
        bucket_type = "membase"
        maxttl = None
        _eviction_policy = 'valueOnly'
        _bucket_storage = 'couchstore'

        bucket_params = {}
        bucket_params['server'] = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        bucket_params['replicas'] = _num_replicas
        bucket_params['size'] = bucket_size
        bucket_params['port'] = 11211
        bucket_params['password'] = "password"
        bucket_params['bucket_type'] = bucket_type
        bucket_params['enable_replica_index'] = 1
        bucket_params['eviction_policy'] = _eviction_policy
        bucket_params['bucket_priority'] = bucket_priority
        bucket_params['flush_enabled'] = 1
        bucket_params['lww'] = False
        bucket_params['maxTTL'] = maxttl
        bucket_params['compressionMode'] = "passive"
        bucket_params['bucket_storage'] = _bucket_storage

        rest = RestConnection(bucket_params['server'])
        existing = [b.name for b in rest.get_buckets()]
        if 'default' in existing:
            self.log.info("Default bucket already exists (created by base setup) — skipping.")
            return
        self.cluster.create_default_bucket(bucket_params)


    def _set_bleve_max_result_window(self):
        bmrw_value = 100000000
        for node in (self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True) or []):
            self.log.info("updating bleve_max_result_window of node : {0}".format(node))
            rest = RestConnection(node)
            rest.set_bleve_max_result_window(bmrw_value)

    def __cleanup_previous(self):
        self.cluster.cleanup_cluster(self, cluster_shutdown=False)

    def tearDown(self):
        self.upgrade_servers = self.servers
        super(UpgradeFTS, self).tearDown()

    def test_offline_upgrade(self):
        post_upgrade_errors = {}
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False)
        fts_callable.load_data(100000)
        fts_query = {"query": "dept:Engineering"}

        pre_upgrade_idx = fts_callable.create_fts_index("pre_upgrade_idx", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=None, analyzer="standard",
                                                no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(100000)
        pre_upgrade_hits, pre_upgrade_matches, _, pre_upgrade_status = pre_upgrade_idx.execute_query(query=fts_query)
        for server in self.servers:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()

        upgrade_threads = self._async_update(self.upgrade_to, self.servers)
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        self.add_built_in_server_user()

        post_upgrade_hits, post_upgrade_matches, _, post_upgrade_status = pre_upgrade_idx.execute_query(query=fts_query)

        log.info("="*20 + " Starting post offline upgrade tests")
        if sorted(pre_upgrade_matches) != sorted(post_upgrade_matches):
            errors = ["Pre-upgrade and post-upgrade results of fts query do not match."]
            post_upgrade_errors['test_offline_upgrade'] = errors
        fts_callable.delete_fts_index("pre_upgrade_idx")
        fts_callable.flush_buckets(["default"])
        errors = self._test_create_single_collection_index()
        if len(errors) > 0:
            post_upgrade_errors['_test_create_single_collection_index'] = errors
        errors = self._test_create_multicollection_index()
        if len(errors) > 0:
            post_upgrade_errors['_test_create_multicollection_index'] = errors
        errors = self._test_create_bucket_index()
        if len(errors) > 0:
            post_upgrade_errors['_test_create_bucket_index'] = errors
        errors = self._test_backup_restore()
        if len(errors) > 0:
            post_upgrade_errors['_test_backup_restore'] = errors
        errors = self._test_rbac_admin()
        if len(errors) > 0:
            post_upgrade_errors['_test_rbac_admin'] = errors
        errors = self._test_rbac_searcher()
        if len(errors) > 0:
            post_upgrade_errors['_test_rbac_searcher'] = errors
        errors = self._test_flex_pushdown_in()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_pushdown_in'] = errors
        errors = self._test_flex_pushdown_like()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_pushdown_like'] = errors
        errors = self._test_flex_pushdown_sort()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_pushdown_sort'] = errors
        errors = self._test_flex_doc_id()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_doc_id'] = errors
        errors = self._test_flex_pushdown_negative_numeric_ranges()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_pushdown_negative_numeric_ranges'] = errors
        errors = self._test_flex_and_search_pushdown()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_and_search_pushdown'] = errors
        errors = self._test_search_before()
        if len(errors) > 0:
            post_upgrade_errors['_test_search_before'] = errors
        errors = self._test_search_after()
        if len(errors) > 0:
            post_upgrade_errors['_test_search_after'] = errors
        errors = self._test_new_metrics(endpoint='_prometheusMetrics')
        if len(errors) > 0:
            post_upgrade_errors["_test_new_metrics(endpoint='_prometheusMetrics')"] = errors
        errors = self._test_new_metrics(endpoint='_prometheusMetricsHigh')
        if len(errors) > 0:
            post_upgrade_errors["_test_new_metrics(endpoint='_prometheusMetricsHigh')"] = errors

        # CBQE-8242
        errors = self._post_upgrade_crud_all_buckets(label="post-offline-upgrade")
        if errors:
            post_upgrade_errors['post_upgrade_crud_all_buckets'] = errors

        # CBQE-8242
        errors = self._post_upgrade_new_index_check(label="post-offline-upgrade")
        if errors:
            post_upgrade_errors['post_upgrade_new_index'] = errors

        self.assertEquals(len(post_upgrade_errors.keys()), 0,
                          f"The following post upgrade tests are failed: {post_upgrade_errors}")


    # =========================================================================
    # CBQE-8242, CBQE-8243
    # =========================================================================

    def _post_upgrade_crud_all_buckets(self, label="post-upgrade"):
        """Item 3: run CRUD against every bucket in the cluster after the change."""
        log.info("=" * 20 + f" {label}: CRUD on all buckets")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   servers=self.servers)
        validator = FTSPostChangeValidator(self, driver=fts_callable, label=label)
        return validator.crud_all_buckets()

    def _post_upgrade_new_index_check(self, label="post-upgrade", index_name="post_change_idx"):
        """Item 4: a NEW index created after the change must index and be queryable."""
        log.info("=" * 20 + f" {label}: new index end-to-end check")
        errors = []
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   servers=self.servers)
        try:
            fts_callable.load_data(self.num_items)
            index = fts_callable.create_fts_index(
                index_name, source_type='couchbase', source_name="default",
                index_type='fulltext-index', index_params=None, plan_params=None,
                source_params=None, source_uuid=None, collection_index=False,
                _type=None, analyzer="standard", no_check=False, cluster=self.cb_cluster)
            validator = FTSPostChangeValidator(self, driver=fts_callable, label=label)
            errors = validator.validate_index_end_to_end(index, expected_count=self.num_items)
        except Exception as err:
            errors.append(f"[{label}] creating/validating the new index failed: {err}")
        finally:
            try:
                fts_callable.delete_fts_index(index_name)
            except Exception as err:
                log.warning(f"[{label}] could not clean up '{index_name}': {err}")
        return errors

    def test_online_upgrade(self):
        partial_upgrade_errors = {}
        full_fts_upgrade_errors = {}
        post_upgrade_errors = {}
        # CBQE-8242
        workload_errors = []

        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)

        if len(fts_nodes) < 2:
            log.error("Need to have more than one FTS nodes in cluster")
            self.fail()

        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False)
        fts_callable.load_data(100000)
        fts_query = {"query": "dept:Engineering"}

        pre_upgrade_idx = fts_callable.create_fts_index("pre_upgrade_idx", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=None, analyzer="standard",
                                                no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(100000)
        pre_upgrade_hits, pre_upgrade_matches, _, pre_upgrade_status = pre_upgrade_idx.execute_query(query=fts_query)

        # Phase 1: Rebalance out first FTS node, upgrade, rebalance back in
        nodes_out = []
        nodes_out.append(fts_nodes[0])
        with FTSConcurrentWorkload(self, index=pre_upgrade_idx, driver=fts_callable,
                                   label="phase 1 FTS node rebalance-out") as workload:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
            rebalance.result()
        workload_errors.extend(workload.errors)
        upgrade_th = self._async_update(self.upgrade_to, nodes_out)
        for th in upgrade_th:
            th.join()
        log.info("==== Upgrade Complete ====")
        self.sleep(120)

        services_in = []
        for service in list(self.services_map.keys()):
            for node in nodes_out:
                node = "{0}:{1}".format(node.ip, node.port)
                if node in self.services_map[service]:
                    services_in.append(service)
        with FTSConcurrentWorkload(self, index=pre_upgrade_idx, driver=fts_callable,
                                   label="phase 1 FTS node rebalance-in") as workload:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 nodes_out, [],
                                                 services=services_in)
            rebalance.result()
        workload_errors.extend(workload.errors)

        log.info("="*20 + " Starting partial fts upgrade tests")
        partial_upgrade_hits, partial_upgrade_matches, _, partial_upgrade_status = pre_upgrade_idx.execute_query(query=fts_query)

        if sorted(pre_upgrade_matches) != sorted(partial_upgrade_matches):
            errors = ["Pre-upgrade and partial-upgrade results of fts query do not match."]
            partial_upgrade_errors['test_partial_upgrade'] = errors
        errors = self._test_create_bucket_index()
        if len(errors) > 0:
            partial_upgrade_errors['_test_create_bucket_index'] = errors

        # Phase 2: Rebalance out all FTS nodes, upgrade, rebalance back in
        nodes_out.clear()
        nodes_out = fts_nodes[0:]
        with FTSConcurrentWorkload(self, index=pre_upgrade_idx, driver=fts_callable,
                                   label="phase 2 all FTS nodes rebalance-out") as workload:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
            rebalance.result()
        workload_errors.extend(workload.errors)
        upgrade_th = self._async_update(self.upgrade_to, nodes_out)
        for th in upgrade_th:
            th.join()
        log.info("==== Upgrade Complete ====")
        self.sleep(120)
        services_in = []
        for service in list(self.services_map.keys()):
            for node in nodes_out:
                node = "{0}:{1}".format(node.ip, node.port)
                if node in self.services_map[service]:
                    services_in.append(service)
        with FTSConcurrentWorkload(self, index=pre_upgrade_idx, driver=fts_callable,
                                   label="phase 2 all FTS nodes rebalance-in") as workload:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 nodes_out, [],
                                                 services=services_in)
            rebalance.result()
        workload_errors.extend(workload.errors)

        log.info("="*20 + " Starting partial upgrade tests")

        errors = self._test_create_bucket_index()
        if len(errors) > 0:
            if "No docs were indexed for index" not in str(errors[0]):
                full_fts_upgrade_errors['_test_create_bucket_index'] = errors
        errors = self._test_backup_restore()
        if len(errors) > 0:
            full_fts_upgrade_errors['_test_backup_restore'] = errors

        # Phase 3: Rebalance out KV node 1, upgrade, rebalance back in
        nodes_out.clear()
        nodes_out.append(kv_nodes[1])
        with FTSConcurrentWorkload(self, index=pre_upgrade_idx, driver=fts_callable,
                                   label="phase 3 KV node 1 rebalance-out") as workload:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
            rebalance.result()
        workload_errors.extend(workload.errors)
        upgrade_th = self._async_update(self.upgrade_to, nodes_out)
        for th in upgrade_th:
            th.join()
        log.info("==== Upgrade Complete ====")
        self.sleep(120)
        services_in = []
        for service in list(self.services_map.keys()):
            for node in nodes_out:
                node = "{0}:{1}".format(node.ip, node.port)
                if node in self.services_map[service]:
                    services_in.append(service)

        with FTSConcurrentWorkload(self, index=pre_upgrade_idx, driver=fts_callable,
                                   label="phase 3 KV node 1 rebalance-in") as workload:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 nodes_out, [],
                                                 services=['kv,index,n1ql'])
            rebalance.result()
        workload_errors.extend(workload.errors)

        # Phase 4: Rebalance out KV node 0 (master), upgrade, rebalance back in
        nodes_out.clear()
        nodes_out.append(kv_nodes[0])
        with FTSConcurrentWorkload(self, index=pre_upgrade_idx, driver=fts_callable,
                                   label="phase 4 KV master rebalance-out") as workload:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
            rebalance.result()
        workload_errors.extend(workload.errors)

        del self.servers[0]
        self.master = self.servers[1]

        upgrade_th = self._async_update(self.upgrade_to, nodes_out)
        for th in upgrade_th:
            th.join()
        log.info("==== Upgrade Complete ====")
        self.sleep(120)

        with FTSConcurrentWorkload(self, index=pre_upgrade_idx, driver=fts_callable,
                                   label="phase 4 KV master rebalance-in") as workload:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 nodes_out, [],
                                                 services=['kv,index,n1ql'])
            rebalance.result()
        workload_errors.extend(workload.errors)

        errors = self._test_create_single_collection_index()
        if len(errors) > 0:
            post_upgrade_errors['_test_create_single_collection_index'] = errors
        errors = self._test_create_multicollection_index()
        if len(errors) > 0:
            post_upgrade_errors['_test_create_multicollection_index'] = errors
        errors = self._test_create_bucket_index()
        if len(errors) > 0:
            post_upgrade_errors['_test_create_bucket_index'] = errors
        errors = self._test_scope_limit_num_fts_indexes()
        if len(errors) > 0:
            post_upgrade_errors['_test_scope_limit_num_fts_indexes'] = errors
        errors = self._test_backup_restore()
        if len(errors) > 0:
            post_upgrade_errors['_test_backup_restore'] = errors
        errors = self._test_rbac_admin()
        if len(errors) > 0:
            post_upgrade_errors['_test_rbac_admin'] = errors
        errors = self._test_rbac_searcher()
        if len(errors) > 0:
            post_upgrade_errors['_test_rbac_searcher'] = errors
        errors = self._test_flex_pushdown_in()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_pushdown_in'] = errors
        errors = self._test_flex_pushdown_like()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_pushdown_like'] = errors
        errors = self._test_flex_pushdown_sort()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_pushdown_sort'] = errors
        errors = self._test_flex_doc_id()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_doc_id'] = errors
        errors = self._test_flex_pushdown_negative_numeric_ranges()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_pushdown_negative_numeric_ranges'] = errors
        errors = self._test_flex_and_search_pushdown()
        if len(errors) > 0:
            post_upgrade_errors['_test_flex_and_search_pushdown'] = errors
        errors = self._test_search_before()
        if len(errors) > 0:
            post_upgrade_errors['_test_search_before'] = errors
        errors = self._test_search_after()
        if len(errors) > 0:
            post_upgrade_errors['_test_search_after'] = errors
        errors = self._test_new_metrics(endpoint='_prometheusMetrics')
        if len(errors) > 0:
            post_upgrade_errors["_test_new_metrics(endpoint='_prometheusMetrics')"] = errors
        errors = self._test_new_metrics(endpoint='_prometheusMetricsHigh')
        if len(errors) > 0:
            post_upgrade_errors["_test_new_metrics(endpoint='_prometheusMetricsHigh')"] = errors

        # CBQE-8242
        errors = self._post_upgrade_crud_all_buckets(label="post-upgrade")
        if errors:
            post_upgrade_errors['post_upgrade_crud_all_buckets'] = errors

        # CBQE-8242
        errors = self._post_upgrade_new_index_check(label="post-upgrade")
        if errors:
            post_upgrade_errors['post_upgrade_new_index'] = errors

        if workload_errors:
            post_upgrade_errors['workload_during_upgrade'] = workload_errors

        self.assertEquals(len(partial_upgrade_errors.keys()), 0,
                          f"The following partial fts upgrade tests are failed: {partial_upgrade_errors}")
        self.assertEquals(len(full_fts_upgrade_errors.keys()), 0,
                          f"The following full fts upgrade tests are failed: {full_fts_upgrade_errors}")
        self.assertEquals(len(post_upgrade_errors.keys()), 0,
                          f"The following post upgrade tests are failed: {post_upgrade_errors}")


    def _create_collections(self, scope=None, collection=None):
        cli_client = CollectionsCLI(self.master)
        cli_client.create_scope(bucket="default", scope=scope)
        if type(collection) is list:
            for c in collection:
                cli_client.create_collection(bucket="default", scope=scope, collection=c)
        else:
            cli_client.create_collection(bucket="default", scope=scope, collection=collection)

    def __define_index_parameters_collection_related(self, container_type="bucket", scope=None, collection=None):
        if container_type == 'bucket':
            _type = "emp"
        else:
            index_collections = []
            if type(collection) is list:
                _type = []
                for c in collection:
                    _type.append(f"{scope}.{c}")
                    index_collections.append(c)
            else:
                _type = f"{scope}.{collection}"
                index_collections.append(collection)
        return _type

    def _test_create_single_collection_index(self):
        log.info("="*20 + " _test_create_single_collection_index")

        errors = []
        self._create_collections(scope="scope1", collection="collection1")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections="collection1", collection_index=True)
        try:
            fts_callable.load_data(100000)
        except Exception as e:
            errors.append(f"Could not load data into collection: {e}")
            return errors

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection="collection1")

        fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard",
                                                scope="scope1", collections=["collection1"], no_check=False,
                                                cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(100000)

        docs_indexed = fts_idx.get_indexed_doc_count()

        if fts_idx.collections:
            container_doc_count = fts_idx.get_src_collections_doc_count()
        else:
            container_doc_count = fts_idx.get_src_bucket_doc_count()

        log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
        if docs_indexed == 0:
            errors.append(f"No docs were indexed for index {fts_idx.name}")
        if docs_indexed != container_doc_count:
            errors.append(f"Bucket doc count = {container_doc_count}, index doc count={docs_indexed}")

        fts_callable.delete_fts_index("idx")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_create_multicollection_index(self):
        log.info("="*20 + " _test_create_multicollection_index")
        errors = []
        self._create_collections(scope="scope1", collection=["collection2", "collection3", "collection4"])
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections=["collection2", "collection3", "collection4"], collection_index=True)
        fts_callable.load_data(100000)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection=["collection2", "collection3", "collection4"])

        fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                         source_name="default", index_type='fulltext-index',
                         index_params=None, plan_params=None,
                         source_params=None, source_uuid=None, collection_index=True, _type=_type, analyzer="standard",
                                                scope="scope1", collections=["collection2", "collection3", "collection4"],
                                                no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(100000)

        docs_indexed = fts_idx.get_indexed_doc_count()

        if fts_idx.collections:
            container_doc_count = fts_idx.get_src_collections_doc_count()
        else:
            container_doc_count = fts_idx.get_src_bucket_doc_count()

        log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
        if docs_indexed == 0:
            errors.append(f"No docs were indexed for index {fts_idx.name}")
        if docs_indexed != container_doc_count:
            errors.append(f"kv doc count = {container_doc_count}, index doc count={docs_indexed}")

        fts_callable.delete_fts_index("idx")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_create_bucket_index(self):
        log.info("="*20 + " _test_create_bucket_index")
        errors = []
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False)
        fts_callable.load_data(100000)

        fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=None, analyzer="standard",
                                                no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(100000)

        docs_indexed = fts_idx.get_indexed_doc_count()

        if fts_idx.collections:
            container_doc_count = fts_idx.get_src_collections_doc_count()
        else:
            container_doc_count = fts_idx.get_src_bucket_doc_count()

        log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
        if docs_indexed == 0:
            errors.append(f"No docs were indexed for index {fts_idx.name}")
        if docs_indexed != container_doc_count:
            errors.append(f"kv doc count = {container_doc_count}, index doc count={docs_indexed}")

        fts_callable.delete_fts_index("idx")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_backup_restore(self):
        log.info("="*20 + " _test_backup_restore")
        test_errors = []
        index_definitions = {}

        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False)
        fts_idx = fts_callable.create_fts_index("idx_backup_restore", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=None, analyzer="standard", no_check=False,
                                                cluster=self.cb_cluster)
        index_definitions['idx_backup_restore'] = {}
        index_definitions['idx_backup_restore']['initial_def'] = {}
        index_definitions['idx_backup_restore']['backup_def'] = {}
        index_definitions['idx_backup_restore']['restored_def'] = {}

        _, index_def = fts_idx.get_index_defn()

        initial_index_def = index_def['indexDef']
        index_definitions['idx_backup_restore']['initial_def'] = initial_index_def

        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if not fts_nodes or len(fts_nodes) == 0:
            test_errors.append("At least 1 fts node must be presented in cluster")
        rest = RestConnection(fts_nodes[0])

        backup_filter = {"option": "include", "containers": ["default"]}
        backup_client = FTSIndexBackupClient(fts_nodes[0])

        status, content = backup_client.backup(_filter=backup_filter)
        backup_json = json.loads(content)
        backup = backup_json['indexDefs']['indexDefs']

        # store backup index definitions
        indexes_for_backup = ["idx_backup_restore"]
        for idx in indexes_for_backup:
            backup_index_def = backup[idx]
            index_definitions[idx]['backup_def'] = backup_index_def

        # delete all indexes before restoring from backup
        self.cb_cluster.delete_all_fts_indexes()

        # restoring indexes from backup
        backup_client.restore()

        # getting restored indexes definitions and storing them in indexes definitions dict
        for ix_name in indexes_for_backup:
            _,restored_index_def = rest.get_fts_index_definition(ix_name)
            index_definitions[ix_name]['restored_def'] = restored_index_def

        #compare all 3 types of index definitions: initial, backed up, and restored from backup
        errors = self._check_indexes_definitions(index_definitions=index_definitions, indexes_for_backup=indexes_for_backup)

        #errors analysis
        if len(errors.keys()) > 0:
            err_msg = ""
            for err in errors.keys():
                index_errors = errors[err]
                for msg in index_errors:
                    err_msg = err_msg + msg + "\n"
            test_errors.append(err_msg)

        fts_callable.flush_buckets(["default"])
        return test_errors

    def _check_indexes_definitions(self, index_definitions={}, indexes_for_backup=[]):
        errors = {}

        #check backup filters
        for ix_name in indexes_for_backup:
            if index_definitions[ix_name]['backup_def'] == {} and ix_name in indexes_for_backup:
                error = f"Index {ix_name} is expected to be in backup, but it is not found there!"
                if ix_name not in errors.keys():
                    errors[ix_name] = []
                errors[ix_name].append(error)

        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['backup_def'] != {} and ix_name not in indexes_for_backup:
                error = f"Index {ix_name} is not expected to be in backup, but it is found there!"
                if ix_name not in errors.keys():
                    errors[ix_name] = []
                errors[ix_name].append(error)

        #check backup json
        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['backup_def'] != {}:
                initial_index_defn = index_definitions[ix_name]['initial_def']
                backup_index_defn = index_definitions[ix_name]['backup_def']

                backup_check = self._validate_backup(backup_index_defn, initial_index_defn)
                if not backup_check:
                    if ix_name not in errors.keys():
                        errors[ix_name] = []
                    errors[ix_name].append(f"Backup fts index signature differs from original signature for index {ix_name}.")

        #check restored json
        for ix_name in index_definitions.keys():
            if index_definitions[ix_name]['restored_def'] != {}:
                initial_index_defn = index_definitions[ix_name]['initial_def']
                restored_index_defn = index_definitions[ix_name]['restored_def']['indexDef']
                restore_check = self._validate_restored(restored_index_defn, initial_index_defn)
                if not restore_check:
                    if ix_name not in errors.keys():
                        errors[ix_name] = []
                    errors[ix_name].append(f"Restored fts index signature differs from original signature for index {ix_name}")

        return errors

    def _validate_backup(self, backup, initial):
        if 'uuid' in initial.keys():
            del initial['uuid']
        if 'sourceUUID' in initial.keys():
            del initial['sourceUUID']
        if 'uuid' in backup.keys():
            del backup['uuid']
        return backup == initial

    def _validate_restored(self, restored, initial):
        del restored['uuid']
        if 'kvStoreName' in restored['params']['store'].keys():
            del restored['params']['store']['kvStoreName']
        if restored != initial:
            self.log(f"Initial index JSON: {initial}")
            self.log(f"Restored index JSON: {restored}")
            return False
        return True


    def create_users(self, users=None):
        """
        :param user: takes a list of {'id': 'xxx', 'name': 'some_name ,
                                        'password': 'passw0rd'}
        :return: Nothing
        """
        if not users:
            users = self.users
        RbacBase().create_user_source(users, 'builtin', self.master)
        self.log.info("SUCCESS: User(s) %s created"
                      % ','.join([user['name'] for user in users]))

    def assign_role(self, rest=None, roles=None):
        if not rest:
            rest = RestConnection(self.master)
        #Assign roles to users
        if not roles:
            roles = self.roles
        RbacBase().add_user_role(roles, rest, 'builtin')
        for user_role in roles:
            self.log.info("SUCCESS: Role(s) %s assigned to %s"
                          %(user_role['roles'], user_role['id']))

    def create_index_with_credentials(self, username, password, index_name, bucket_name="default", collection_index=False, _type=None, analyzer="standard", scope=None, collections=None):
        index = FTSIndex(self.cb_cluster, name=index_name, source_name=bucket_name, scope=scope, collections=collections)
        if collection_index:
            if type(_type) is list:
                for typ in _type:
                    index.add_type_mapping_to_index_definition(type=typ, analyzer=analyzer)
            else:
                index.add_type_mapping_to_index_definition(type=_type, analyzer=analyzer)

            doc_config = {}
            doc_config['mode'] = 'scope.collection.type_field'
            doc_config['type_field'] = "type"
            index.index_definition['params']['doc_config'] = {}
            index.index_definition['params']['doc_config'] = doc_config

        rest = self.get_rest_handle_for_credentials(username, password)
        index.create(rest)
        return index

    def get_rest_handle_for_credentials(self, user, password):
        rest = RestConnection(self.cb_cluster.get_random_fts_node())
        rest.username = user
        rest.password = password
        return rest

    def get_user_list(self, inp_users=None):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
        'password': 'passw0rd'}
        """
        user_list = []
        for user in inp_users:
            user_list.append({att: user[att] for att in ('id',
                                                         'name',
                                                         'password')})
        return user_list

    def get_user_role_list(self, inp_users=None):
        """
        :return:  a list of {'id': 'userid', 'name': 'some_name ,
         'roles': 'admin:fts_admin[default]'}
        """
        user_role_list = []
        for user in inp_users:
            user_role_list.append({att: user[att] for att in ('id',
                                                              'name',
                                                              'roles',
                                                              'password')})
        return user_role_list

    def create_alias_with_credentials(self, username, password, alias_name,
                                      target_indexes):
        alias_def = {"targets": {}}
        for index in target_indexes:
            # This alias is created without a scope, so the server resolves its
            # targets in the global namespace: they have to be named
            # bucket.scope.name. index.name stays short for a scoped index.
            target = index.full_name
            alias_def['targets'][target] = {}
            alias_def['targets'][target]['indexUUID'] = index.get_uuid()
        alias = FTSIndex(self.cb_cluster, name=alias_name,
                         index_type='fulltext-alias', index_params=alias_def)
        rest = self.get_rest_handle_for_credentials(username, password)
        alias.create(rest)
        return alias

    def edit_index_with_credentials(self, index, username, password):
        rest = self.get_rest_handle_for_credentials(username, password)
        _, defn = index.get_index_defn(rest)
        self.log.info(f"Old definition: {defn['indexDef']}")
        new_plan_param = {"maxPartitionsPerPIndex": 10}
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        index.update(rest)
        _, defn = index.get_index_defn()
        self.log.info(f"New definition: {defn['indexDef']}" )

    def query_index_with_credentials(self, index, username, password):
        sample_query = {"match": "Safiya Morgan", "field": "name"}

        rest = self.get_rest_handle_for_credentials(username, password)
        self.log.info("Now querying with credentials %s:%s" %(username,
                                                              password))
        hits, _, _, _ = rest.run_fts_query(index.name,
                                           {"query": sample_query})
        self.log.info("Hits: %s" %hits)

    def delete_index_with_credentials(self, index, username, password):
        rest = self.get_rest_handle_for_credentials(username, password)
        index.delete(rest)

    def _test_rbac_admin(self):
        log.info("="*20 + " _test_rbac_admin")
        errors = []
        self._create_collections(scope="scope1", collection="collection1")

        users = [{"id": "johnDoe",
                  "name": "Jonathan Downing",
                  "password": "password1",
                  "roles": "fts_admin[default]:cluster_admin"
                  }]
        users_list = self.get_user_list(inp_users=users)
        roles_list = self.get_user_role_list(inp_users=users)


        self.create_users(users=users_list)
        self.assign_role(roles=roles_list)

        for user in users_list:
            try:
                collection_index=True
                _type='scope1.collection1'
                index_scope='scope1'
                index_collections='collection1'
                index = self.create_index_with_credentials(
                    username= user['id'],
                    password=user['password'],
                    index_name="%s_%s_idx" %(user['id'], "default"),
                    bucket_name="default",
                    collection_index=collection_index,
                    _type=_type,
                    scope=index_scope,
                    collections=index_collections
                )

                alias = self.create_alias_with_credentials(
                    username= user['id'],
                    password=user['password'],
                    target_indexes=[index],
                    alias_name="%s_%s_alias" %(user['id'], "default"))
                try:
                    self.edit_index_with_credentials(
                        index=index,
                        username=user['id'],
                        password=user['password'])
                    self.sleep(60, "Waiting for index rebuild after "
                                    "update...")
                    self.query_index_with_credentials(
                        index=index,
                        username=user['id'],
                        password=user['password'])
                    self.delete_index_with_credentials(
                        alias,
                        user['id'],
                        user['password'])
                    self.delete_index_with_credentials(
                        index=index,
                        username=user['id'],
                        password=user['password'])
                except Exception as e:
                    errors.append("The user failed to edit/query/delete fts "
                                "index %s : %s" % (user['id'], e))
            except Exception as e:
                    errors.append("The user failed to create fts index/alias"
                                  " %s : %s" % (user['id'], e))
            return errors

    def _test_rbac_searcher(self):
        log.info("="*20 + " _test_rbac_searcher")
        errors = []
        self._create_collections(scope="scope1", collection="collection2")

        users = [{"id": "johnDoe", "name": "Jonathan Downing", "password": "password1", "roles": "fts_searcher[default:scope1]"}]
        users_list = self.get_user_list(inp_users=users)
        roles_list = self.get_user_role_list(inp_users=users)

        self.create_users(users=users_list)
        self.assign_role(roles=roles_list)

        for user in users_list:
            try:
                collection_index=True
                _type='scope1.collection2'
                index_scope='scope1'
                index_collections='collection2'
                self.create_index_with_credentials(
                    username= user['id'],
                    password=user['password'],
                    index_name="%s_%s_idx" %(user['id'], "default"),
                    bucket_name="default",
                    collection_index=collection_index,
                    _type=_type,
                    scope=index_scope,
                    collections=index_collections
                )
            except Exception as e:
                self.log.info("Expected exception: %s" %e)
            else:
                errors.append("An fts_searcher is able to create index!")

            # creating an alias
            try:
                self.log.info("Creating index as administrator...")
                collection_index=True
                _type='scope1.collection2'
                index_scope='scope1'
                index_collections='collection2'
                index = self.create_index_with_credentials(
                    username='Administrator',
                    password='password',
                    index_name="%s_%s_idx" % ('Admin', "default"),
                    bucket_name="default",
                    collection_index=collection_index,
                    _type=_type,
                    scope=index_scope,
                    collections=index_collections
                )
                self.log.info("Creating alias as fts_searcher...")
                self.create_alias_with_credentials(
                    username=user['id'],
                    password=user['password'],
                    target_indexes=[index],
                    alias_name="%s_%s_alias" % (user['id'], "default"))
            except Exception as e:
                self.log.info(f"Expected exception: {e}")
            else:
                errors.append("An fts_searcher is able to create alias!")

            # editing an index
            try:
                self.edit_index_with_credentials(index=index,
                                                 username=user['id'],
                                                 password=user['password'])
            except Exception as e:
                self.log.info("Expected exception while updating index: %s"
                                % e)
                self.query_index_with_credentials(index=index,
                                                  username=user['id'],
                                                  password=user['password'])
            else:
                errors.append("An fts searcher is able to edit index!")

            # deleting an index
            try:
                self.delete_index_with_credentials(index=index,
                                                   username=user['id'],
                                                   password=user['password'])
            except Exception as e:
                self.log.info("Expected exception: %s" % e)
            else:
                errors.append("An fts searcher is able to delete index!")

        return errors

    def _test_flex_pushdown_in(self):
        log.info("="*20 + " _test_flex_pushdown_in")
        errors = []
        self._create_collections(scope="scope1", collection="collection10")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1",
                                   collections="collection10", collection_index=True)
        fts_callable.load_data(100000)

        _type = self.__define_index_parameters_collection_related(container_type="collection",
                                                                  scope="scope1", collection="collection10")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True, _type=_type,
                                                analyzer="keyword", scope="scope1", collections=["collection10"],
                                                no_check=False, cluster=self.cb_cluster)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100000)

        _data_types = {
            "text":  {"field": "type", "vals": ["emp", "emp1"]},
            "number": {"field": "mutated", "vals": [0, 1]},
            "boolean": {"field": "is_manager", "vals": [True, False]},
            "datetime":    {"field": "join_date", "vals": ["1970-07-02T11:50:10", "1951-11-16T13:37:10"]}
        }
        index_configuration = "FTS"
        custom_mapping = False
        index_hint = "USING FTS"

        tests = []
        for _key in _data_types.keys():
            flex_query = "select count(*) from `default`.scope1.collection10 USE INDEX({0}) where {1} in {2}".\
                format(index_hint, _data_types[_key]['field'], _data_types[_key]['vals'])
            gsi_query = "select count(*) from `default`.scope1.collection10 where {1} in {2}".\
                format(index_hint, _data_types[_key]['field'], _data_types[_key]['vals'])
            test = {}
            test['flex_query'] = flex_query
            test['gsi_query'] = gsi_query
            test['flex_result'] = {}
            test['flex_explain'] = {}
            test['gsi_result'] = {}
            test['errors'] = []
            tests.append(test)

        self.cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection10")
        self.sleep(10)
        for test in tests:
            result = self.cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self.cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection10")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=False)
        if errors_found:
            errors.append("Errors are detected for IN/NOT Flex queries. Check logs for details.")
        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _perform_results_checks(self, tests=None, index_configuration="", custom_mapping=False, check_pushdown=True):
        for test in tests:
            result = self.cb_cluster.run_n1ql_query("explain " + test['flex_query'])
            if check_pushdown:
                if "index_group_aggs" not in str(result):
                    error = {}
                    error['error_message'] = "Index aggregate pushdown is not detected."
                    error['query'] = test['flex_query']
                    error['indexing_config'] = index_configuration
                    error['custom_mapping'] = str(custom_mapping)
                    error['collections'] = str(self.collection)
                    test['errors'].append(error)
            result = self.cb_cluster.run_n1ql_query(test['flex_query'])

            test['flex_result'] = result['results']
            if result['status'] != 'success':
                error = {}
                error['error_message'] = "Flex query was not executed successfully."
                error['query'] = test['flex_query']
                error['indexing_config'] = index_configuration
                error['custom_mapping'] = str(custom_mapping)
                error['collections'] = str(self.collection)
                test['errors'].append(error)
            if test['flex_result'] != test['gsi_result']:
                error = {}
                error['error_message'] = "Flex query results and GSI query results are different."
                error['query'] = test['flex_query']
                error['indexing_config'] = index_configuration
                error['custom_mapping'] = str(custom_mapping)
                error['collections'] = str(self.collection)
                test['errors'].append(error)

        errors_found = False
        for test in tests:
            if len(test['errors']) > 0:
                errors_found = True
                self.log.error("The following errors are detected:\n")
                for error in test['errors']:
                    self.log.error("="*10)
                    self.log.error(error['error_message'])
                    self.log.error("query: " + error['query'])
                    self.log.error("indexing config: " + error['indexing_config'])
                    self.log.error("custom mapping: " + str(error['custom_mapping']))
                    self.log.error("collections set: " + str(error['collections']))
        return errors_found

    def _test_flex_pushdown_like(self):
        log.info("="*20 + " _test_flex_pushdown_like")
        errors = []
        self._create_collections(scope="scope1", collection="collection11")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1", collections="collection11", collection_index=True)
        fts_callable.load_data(100000)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1", collection="collection11")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="keyword", scope="scope1",
                                                collections=["collection11"], no_check=False, cluster=self.cb_cluster)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100000)

        check_pushdown = False

        like_types = ["left", "right", "left_right"]
        like_conditions = ["LIKE"]
        _data_types = {
            "text":  {"field": "type", "vals": "emp"},
        }
        index_configuration = "FTS"
        custom_mapping = False
        index_hint = "USING FTS"

        tests = []
        for _key in _data_types.keys():
            for like_type in like_types:
                for like_condition in like_conditions:
                    if like_type == "left":
                        like_expression = "'%"+_data_types[_key]['vals']+"'"
                    elif like_type == "right":
                        like_expression = "'" + _data_types[_key]['vals'] + "%'"
                    else:
                        like_expression = "'%" + _data_types[_key]['vals'] + "%'"
                    flex_query = "select count(*) from `default`.scope1.collection11 USE INDEX({0}) where {1} {2} {3}".\
                        format(index_hint, _data_types[_key]['field'], like_condition, like_expression)
                    gsi_query = "select count(*) from `default`.scope1.collection11 where {1} {2} {3}".\
                        format(index_hint, _data_types[_key]['field'], like_condition, like_expression)
                    test = {}
                    test['flex_query'] = flex_query
                    test['gsi_query'] = gsi_query
                    test['flex_result'] = {}
                    test['flex_explain'] = {}
                    test['gsi_result'] = {}
                    test['errors'] = []
                    tests.append(test)

        self.cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection11")
        self.sleep(10)
        for test in tests:
            result = self.cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self.cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection11")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)

        if errors_found:
            errors.append("Errors are detected for LIKE Flex queries. Check logs for details.")
        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_flex_pushdown_sort(self):
        errors = []
        self._create_collections(scope="scope1", collection="collection12")
        fts_callable = FTSCallable(self.servers, es_validate=False,
                                   es_reset=False, scope="scope1", collections="collection12", collection_index=True)
        fts_callable.load_data(100000)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1", collection="collection12")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="keyword", scope="scope1",
                                                collections=["collection12"], no_check=False, cluster=self.cb_cluster)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100000)

        check_pushdown = False

        sort_directions = ["ASC", "DESC", ""]
        limits = ["LIMIT 10", ""]
        offsets = ["OFFSET 5", ""]
        custom_mapping = False
        _data_types = {
            "text": {"field": "type", "flex_condition": "type='emp'"},
            "number": {"field": "mutated", "flex_condition": "mutated=0"},
            "boolean": {"field": "is_manager", "flex_condition": "is_manager=true"},
            "datetime": {"field": "join_date", "flex_condition": "join_date > '2001-10-09' AND join_date < '2020-10-09'"}
        }
        index_configuration = "FTS"
        index_hint = "USING FTS"

        tests = []
        for _key in _data_types.keys():
            for sort_direction in sort_directions:
                for limit in limits:
                    for offset in offsets:
                        flex_query = "select meta().id from `default`.scope1.collection12 USE INDEX({0}) where {1} order by {2} {3} {4} {5}".\
                            format(index_hint, _data_types[_key]['flex_condition'], "meta().id", sort_direction, limit, offset)
                        gsi_query = "select meta().id from `default`.scope1.collection12 USE INDEX({0}) where {1} order by {2} {3} {4} {5}".\
                            format(index_hint, _data_types[_key]['flex_condition'], "meta().id", sort_direction, limit, offset)
                        test = {}
                        test['flex_query'] = flex_query
                        test['gsi_query'] = gsi_query
                        test['flex_result'] = {}
                        test['flex_explain'] = {}
                        test['gsi_result'] = {}
                        test['errors'] = []
                        tests.append(test)

        self.cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection12")
        self.sleep(10)
        for test in tests:
            result = self.cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self.cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection12")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=False)
        if errors_found:
            errors.append("Errors are detected for ORDER BY Flex queries. Check logs for details.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_flex_doc_id(self):
        errors = []
        self._create_collections(scope="scope1", collection="collection13")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1", collections="collection13", collection_index=True)
        fts_callable.load_data(100000)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1", collection="collection13")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                 source_name="default", index_type='fulltext-index',
                                                 index_params=None, plan_params=None,
                                                 source_params=None, source_uuid=None, collection_index=True,
                                                 _type=_type, analyzer="keyword", scope="scope1",
                                                 collections=["collection13"], no_check=False, cluster=self.cb_cluster)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['params']['doc_config']['docid_prefix_delim'] = "_"
        fts_idx.index_definition['params']['doc_config']['docid_regexp'] = ""
        fts_idx.index_definition['params']['doc_config']['mode'] = "scope.collection.docid_prefix"
        fts_idx.index_definition['params']['doc_config']['type_field'] = "type"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100000)

        check_pushdown = False
        like_expressions = ["LIKE"]
        index_configuration = "FTS"
        custom_mapping = False
        index_hint = "USING FTS"

        tests = []
        for like_expression in like_expressions:
            flex_query = "select count(*) from `default`.scope1.collection13 USE INDEX({0}) where meta().id {1} 'emp_%' and type='emp'".\
                        format(index_hint, like_expression)
            gsi_query = "select count(*) from `default`.scope1.collection13 where meta().id {1} 'emp_%' and type='emp'".\
                        format(index_hint, like_expression)
            test = {}
            test['flex_query'] = flex_query
            test['gsi_query'] = gsi_query
            test['flex_result'] = {}
            test['flex_explain'] = {}
            test['gsi_result'] = {}
            test['errors'] = []
            tests.append(test)

        self.cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection13")
        self.sleep(10)
        for test in tests:
            result = self.cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self.cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection13")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)
        if errors_found:
            errors.append("Errors are detected for DOC_ID prefix Flex queries. Check logs for details.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_flex_pushdown_negative_numeric_ranges(self):
        errors = []
        self._create_collections(scope="scope1", collection="collection14")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1", collections="collection14", collection_index=True)
        fts_callable.load_data(100000)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1", collection="collection14")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="keyword", scope="scope1",
                                                collections=["collection14"], no_check=False, cluster=self.cb_cluster)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100000)

        check_pushdown = False
        relations = ['<', '<=', '=', '>', '>=']
        _data_types = {
            "number": {"field": "salary"}
        }
        index_configuration = "FTS"
        custom_mapping = False
        index_hint = "USING FTS"

        tests = []
        for relation in relations:
            condition = ""
            if relation == '<':
                condition = ' salary > -100 and salary < -10'
            elif relation == '<=':
                condition = ' salary >= -100 and salary <= -10'
            elif relation == '>':
                condition = ' salary > -10 and salary < -1'
            elif relation == '>=':
                condition = ' salary >= -10 and salary <= -1'
            elif relation == "=":
                condition = ' salary = -10 '

            flex_query = "select count(*) from `default`.scope1.collection14 USE INDEX({0}) where {1}" .\
                    format(index_hint, condition)
            gsi_query = "select count(*) from `default`.scope1.collection14 USE INDEX({0}) where {1}" .\
                    format(index_hint, condition)
            test = {}
            test['flex_query'] = flex_query
            test['gsi_query'] = gsi_query
            test['flex_result'] = {}
            test['flex_explain'] = {}
            test['gsi_result'] = {}
            test['errors'] = []
            tests.append(test)

        self.cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection14")
        self.sleep(10)
        for test in tests:
            result = self.cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self.cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection14")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)
        if errors_found:
            errors.append("Errors are detected for negative numeric ranges Flex queries. Check logs for details.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_flex_and_search_pushdown(self):
        errors = []
        self._create_collections(scope="scope1", collection="collection15")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1", collections="collection15", collection_index=True)
        fts_callable.load_data(100000)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1", collection="collection15")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="keyword", scope="scope1",
                                                collections=["collection15"], no_check=False, cluster=self.cb_cluster)
        fts_idx.index_definition['params']['mapping']['default_analyzer'] = "keyword"
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()

        fts_callable.wait_for_indexing_complete(100000)

        check_pushdown = False
        _data_types = {
            "text":  {"field": "type",
                        "search_condition": "{'query':{'field': 'type', 'match':'emp'}}",
                        "flex_condition": "a.`type`='emp'"},
            "number": {"field": "salary",
                        "search_condition": "{'query':{'min': 1000, 'max': 100000, 'field': 'salary'}}",
                        "flex_condition": "a.salary>1000 and a.salary<100000"},
            "boolean": {"field": "is_manager",
                        "search_condition": "{'query':{'bool': true, 'field': 'is_manager'}}",
                        "flex_condition": "a.is_manager=true"},
            "datetime":    {"field": "join_date",
                        "search_condition": "{'start': '2001-10-09', 'end': '2016-10-31', 'field': 'join_date'}",
                        "flex_condition": "a.join_date > '2001-10-09' and a.join_date < '2016-10-31'"}
        }
        index_configuration = "FTS"
        custom_mapping = False
        index_hint = "USING FTS"

        tests = []
        for _key1 in _data_types.keys():
            for _key2 in _data_types.keys():
                flex_query = "select count(*) from `default`.scope1.collection15 a USE INDEX({0}) where {1} and search(a, {2})".\
                        format(index_hint, _data_types[_key1]['flex_condition'], _data_types[_key2]['search_condition'])
                gsi_query = "select count(*) from `default`.scope1.collection15 a where {0} and search(a, {1})".\
                        format(_data_types[_key1]['flex_condition'], _data_types[_key2]['search_condition'])
                test = {}
                test['flex_query'] = flex_query
                test['gsi_query'] = gsi_query
                test['flex_result'] = {}
                test['flex_explain'] = {}
                test['gsi_result'] = {}
                test['errors'] = []
                tests.append(test)

        self.cb_cluster.run_n1ql_query("create primary index on `default`.scope1.collection15")
        self.sleep(10)
        for test in tests:
            result = self.cb_cluster.run_n1ql_query(test['gsi_query'])
            test['gsi_result'] = result['results']
        self.cb_cluster.run_n1ql_query("drop primary index on `default`.scope1.collection15")
        self.sleep(10)

        errors_found = self._perform_results_checks(tests=tests,
                                                    index_configuration=index_configuration,
                                                    custom_mapping=custom_mapping, check_pushdown=check_pushdown)
        if errors_found:
            errors.append("Errors are detected for Flex + Search queries. Check logs for details.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    test_data = {
        "doc_1": {
            "num": 1,
            "str": "str_1",
            "bool": True,
            "array": ["array1_1", "array1_2"],
            "obj": {"key": "key1", "val": "val1"},
            "filler": "filler"
        },
        "doc_2": {
            "num": 2,
            "str": "str_2",
            "bool": False,
            "array": ["array2_1", "array2_2"],
            "obj": {"key": "key2", "val": "val2"},
            "filler": "filler"
        },
        "doc_3": {
            "num": 3,
            "str": "str_3",
            "bool": True,
            "array": ["array3_1", "array3_2"],
            "obj": {"key": "key3", "val": "val3"},
            "filler": "filler"
        },
        "doc_4": {
            "num": 4,
            "str": "str_4",
            "bool": False,
            "array": ["array4_1", "array4_2"],
            "obj": {"key": "key4", "val": "val4"},
            "filler": "filler"
        },
        "doc_5": {
            "num": 5,
            "str": "str_5",
            "bool": True,
            "array": ["array5_1", "array5_2"],
            "obj": {"key": "key5", "val": "val5"},
            "filler": "filler"
        },
        "doc_10": {
            "num": 10,
            "str": "str_10",
            "bool": False,
            "array": ["array10_1", "array10_2"],
            "obj": {"key": "key10", "val": "val10"},
            "filler": "filler"
        },
    }

    def _load_search_before_search_after_test_data(self, bucket, test_data):
        for key in test_data:
            query = "insert into "+bucket+" (KEY, VALUE) VALUES " \
                                          "('"+str(key)+"', " \
                                          ""+str(test_data[key])+")"
            self.cb_cluster.run_n1ql_query(query=query)

    def _test_search_before(self):
        errors = []
        bucket = self.cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, collection_index=False)

        _type = None

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=_type, analyzer="standard", no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(len(self.test_data))

        full_size = len(self.test_data)
        partial_size = 1
        partial_start_index = 3
        sort_mode = ['_id']

        cluster = fts_idx.get_cluster()
        self.sleep(10)
        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(fts_idx.name, all_fts_query)
        if all_hits is None or all_matches is None:
            errors.append(f"test is failed: no results were returned by fts query: {all_fts_query}")
            return errors
        search_before_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_before_param)):
            if search_before_param[i] == "_score":
                search_before_param[i] = str(all_matches[partial_start_index]['score'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_before": search_before_param}
        _, search_before_matches, _, _ = cluster.run_fts_query(fts_idx.name, search_before_fts_query)

        all_results_ids = []
        search_before_results_ids = []

        for match in all_matches:
            all_results_ids.append(match['id'])

        for match in search_before_matches:
            search_before_results_ids.append(match['id'])

        for i in range(0, partial_size-1):
            if i in range(0, len(search_before_results_ids) - 1):
                if search_before_results_ids[i] != all_results_ids[partial_start_index-partial_size+i]:
                    errors.append("test is failed")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_search_after(self):
        errors = []
        bucket = self.cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)

        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, collection_index=False)

        _type = None

        index = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                              source_name="default", index_type='fulltext-index',
                                              index_params=None, plan_params=None,
                                              source_params=None, source_uuid=None, collection_index=False,
                                              _type=_type, analyzer="standard", no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(len(self.test_data))
        self.sleep(10)

        full_size = len(self.test_data)
        partial_size = 1
        partial_start_index = 3
        sort_mode = ['_id']

        cluster = index.get_cluster()

        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)

        search_before_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_before_param)):
            if search_before_param[i] == "_score":
                search_before_param[i] = str(all_matches[partial_start_index]['score'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_after": search_before_param}
        _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query)
        all_results_ids = []
        search_before_results_ids = []

        for match in all_matches:
            all_results_ids.append(match['id'])

        for match in search_before_matches:
            search_before_results_ids.append(match['id'])

        for i in range(0, partial_size-1):
            if i in range(0, len(search_before_results_ids)-1):
                if search_before_results_ids[i] != all_results_ids[partial_start_index+1+i]:
                    errors.append("test is failed")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    def _test_new_metrics(self, endpoint=None):
        errors = []
        fts_node = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=False)

        self._create_collections(scope="scope1", collection="collection25")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False, scope="scope1", collections="collection25", collection_index=True)
        fts_callable.load_data(100000)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1", collection="collection25")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard", scope="scope1",
                                                collections=["collection25"], no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(100000)
        rest = RestConnection(fts_node)
        fts_port = fts_node.fts_port or self.fts_port
        status, content = rest.get_rest_endpoint_data(endpoint, ip=fts_node.ip, port=fts_port)
        if not status:
            errors.append(f"Endpoint {endpoint} is not accessible.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

    # ======================================================================
    # scan_plus / request_plus upgrade tests
    # ======================================================================

    def _scan_plus_setup_sdk(self):
        """Return (cluster, default_collection) via Couchbase Python SDK v4."""
        from couchbase.cluster import Cluster
        from couchbase.auth import PasswordAuthenticator
        from couchbase.options import ClusterOptions
        auth = PasswordAuthenticator(self.master.rest_username, self.master.rest_password)
        cluster = Cluster(f'couchbase://{self.master.ip}', ClusterOptions(auth))
        collection = cluster.bucket('default').default_collection()
        return cluster, collection

    def _test_scan_plus_pre_upgrade(self, fts_idx):
        """
        Stage 0 — all nodes pre-8.1.
        scan_plus query must fail: feature does not exist on the old version.
        """
        log.info("=" * 20 + " _test_scan_plus_pre_upgrade")
        errors = []
        hits, matches, _, status = fts_idx.execute_query(
            {"match_all": {}},
            zero_results_ok=True,
            return_raw_hits=True,
            consistency_level="scan_plus",
            consistency_vectors=None,
            fields=["val"],
        )
        if hits != -1 and status != 'fail':
            errors.append(
                f"scan_plus query unexpectedly succeeded on pre-8.1 cluster "
                f"(hits={hits}, status={status})"
            )
        else:
            log.info(f"[pre-upgrade] Got expected failure — status={status}, error={matches}")
        return errors

    def _test_scan_plus_mixed_v2(self, fts_idx, upgraded_node):
        """
        V-2 — coordinating FTS node is 8.1, others may be pre-8.1.
        scan_plus routed to the upgraded coordinator must succeed.
        """
        log.info("=" * 20 + " _test_scan_plus_mixed_v2")
        errors = []
        hits, matches, _, status = fts_idx.execute_query(
            {"match_all": {}},
            zero_results_ok=True,
            return_raw_hits=True,
            consistency_level="scan_plus",
            consistency_vectors=None,
            fields=["val"],
            node=upgraded_node,
        )
        if status == 'fail' or hits == -1:
            errors.append(
                f"[V-2] scan_plus via upgraded coordinator failed unexpectedly "
                f"(hits={hits}, status={status}, error={matches})"
            )
        return errors

    def _test_scan_plus_mixed_v3(self, fts_idx, old_node):
        """
        V-3 — coordinating FTS node is pre-8.1.
        scan_plus routed to the old coordinator must fail with a version error.
        """
        log.info("=" * 20 + " _test_scan_plus_mixed_v3")
        errors = []
        expected_err_fragment = "unsupported consistencyLevel: scan_plus"
        hits, matches, _, status = fts_idx.execute_query(
            {"match_all": {}},
            zero_results_ok=True,
            return_raw_hits=True,
            consistency_level="scan_plus",
            consistency_vectors=None,
            fields=["val"],
            node=old_node,
        )
        if hits != -1 and status != 'fail':
            errors.append(
                f"[V-3] scan_plus via pre-8.1 coordinator unexpectedly succeeded "
                f"(hits={hits}, status={status})"
            )
        else:
            err_str = str(matches)
            log.info(f"[V-3] Got expected failure — error={err_str}")
            if expected_err_fragment not in err_str:
                log.warning(
                    f"[V-3] Error did not contain expected fragment "
                    f"'{expected_err_fragment}' — update placeholder. "
                    f"Actual: {err_str}"
                )
        return errors

    def _test_scan_plus_post_upgrade(self, fts_idx, hashmap):
        """
        V-1 — all nodes at 8.1.
        scan_plus must return results that exactly match the HashMap snapshot
        (count + val).
        """
        log.info("=" * 20 + " _test_scan_plus_post_upgrade")
        errors = []
        snapshot = hashmap.snapshot()

        hits, matches, _, status = fts_idx.execute_query(
            {"match_all": {}},
            zero_results_ok=True,
            return_raw_hits=True,
            consistency_level="scan_plus",
            consistency_vectors=None,
            fields=["val"],
        )

        if hits == -1 or status == 'fail':
            return [f"[V-1] scan_plus query failed: status={status}, error={matches}"]

        log.info(f"[V-1] hits={hits}, snapshot_size={len(snapshot)}")

        if hits != len(snapshot):
            errors.append(
                f"[V-1] Doc count mismatch: FTS={hits}, snapshot={len(snapshot)}"
            )

        result_map = {}
        for match in (matches or []):
            doc_id = match.get('id')
            raw_val = match.get('fields', {}).get('val')
            if doc_id is not None and raw_val is not None:
                result_map[doc_id] = int(raw_val)

        for doc_id, expected_val in snapshot.items():
            if doc_id not in result_map:
                errors.append(
                    f"[V-1] Missing in FTS: '{doc_id}' (snapshot.val={expected_val})"
                )
            elif result_map[doc_id] != expected_val:
                errors.append(
                    f"[V-1] Val mismatch: '{doc_id}' "
                    f"result={result_map[doc_id]}, snapshot={expected_val}"
                )

        for doc_id in result_map:
            if doc_id not in snapshot:
                errors.append(f"[V-1] Extra doc in FTS: '{doc_id}'")

        return errors

    def test_scan_plus_online_upgrade(self):
        """
        Online upgrade test for the scan_plus (request_plus) consistency level.

        Validates version-gated behavior at three stages:
          Stage 0 — all nodes pre-8.1 : scan_plus must fail
          Stage 2 — one FTS node at 8.1 (mixed cluster):
              V-2: query routed to upgraded coordinator → must succeed
              V-3: query routed to old coordinator    → must fail (version error)
          Stage 4 — all FTS nodes at 8.1:
              V-1: scan_plus must return exact consistent results

        Requires ≥ 2 FTS nodes in the cluster.
        """
        mixed_upgrade_errors = {}
        post_upgrade_errors = {}

        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if len(fts_nodes) < 2:
            log.error("test_scan_plus_online_upgrade requires ≥ 2 FTS nodes")
            self.fail()

        # --- Setup: load seed docs, create index with stored val field ---
        _, collection = self._scan_plus_setup_sdk()
        hashmap = _ScanPlusHashMap()
        for _ in range(10):
            doc_id = f"scan_plus_upg_{uuid.uuid4().hex}"
            collection.upsert(doc_id, {'val': 1})
            hashmap.insert(doc_id, 1)

        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False)
        fts_idx = fts_callable.create_fts_index(
            "scan_plus_upgrade_idx",
            source_type='couchbase', source_name="default",
            index_type='fulltext-index', index_params=None, plan_params=None,
            source_params=None, source_uuid=None, collection_index=False,
            _type=None, analyzer="standard", no_check=False,
            cluster=self.cb_cluster,
        )
        fts_idx.add_child_field_to_default_mapping("val", "number")
        fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
        fts_idx.update()
        fts_callable.wait_for_indexing_complete(10)

        # --- Stage 0: Pre-upgrade ---
        log.info("=" * 20 + " Stage 0: pre-upgrade scan_plus check")
        errors = self._test_scan_plus_pre_upgrade(fts_idx)
        if errors:
            mixed_upgrade_errors['pre_upgrade'] = errors

        # --- Stage 1: Upgrade one FTS node ---
        nodes_out = [fts_nodes[0]]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
        rebalance.result()
        upgrade_th = self._async_update(self.upgrade_to, nodes_out)
        for th in upgrade_th:
            th.join()
        log.info("==== First FTS node upgraded ====")
        self.sleep(120)

        services_in = []
        for service in list(self.services_map.keys()):
            for node in nodes_out:
                node_str = "{0}:{1}".format(node.ip, node.port)
                if node_str in self.services_map[service]:
                    services_in.append(service)
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], nodes_out, [], services=services_in
        )
        rebalance.result()

        upgraded_node = fts_nodes[0]
        old_node = fts_nodes[1]

        # --- Stage 2: Mixed cluster — V-2 and V-3 ---
        log.info("=" * 20 + " Stage 2: mixed cluster checks")
        errors = self._test_scan_plus_mixed_v2(fts_idx, upgraded_node)
        if errors:
            mixed_upgrade_errors['v2_upgraded_coordinator'] = errors

        errors = self._test_scan_plus_mixed_v3(fts_idx, old_node)
        if errors:
            mixed_upgrade_errors['v3_old_coordinator'] = errors

        # --- Stage 3: Upgrade remaining FTS nodes ---
        nodes_out = fts_nodes[1:]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
        rebalance.result()
        upgrade_th = self._async_update(self.upgrade_to, nodes_out)
        for th in upgrade_th:
            th.join()
        log.info("==== Remaining FTS nodes upgraded ====")
        self.sleep(120)

        services_in = []
        for service in list(self.services_map.keys()):
            for node in nodes_out:
                node_str = "{0}:{1}".format(node.ip, node.port)
                if node_str in self.services_map[service]:
                    services_in.append(service)
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init], nodes_out, [], services=services_in
        )
        rebalance.result()

        # --- Stage 4: Post-upgrade — V-1 exact validation ---
        log.info("=" * 20 + " Stage 4: post-upgrade scan_plus validation")
        errors = self._test_scan_plus_post_upgrade(fts_idx, hashmap)
        if errors:
            post_upgrade_errors['v1_post_upgrade'] = errors

        fts_callable.delete_fts_index("scan_plus_upgrade_idx")
        fts_callable.flush_buckets(["default"])

        self.assertEquals(len(mixed_upgrade_errors.keys()), 0,
                          f"Mixed cluster scan_plus tests failed: {mixed_upgrade_errors}")
        self.assertEquals(len(post_upgrade_errors.keys()), 0,
                          f"Post-upgrade scan_plus tests failed: {post_upgrade_errors}")

    def get_nodes_in_cluster_after_upgrade(self, master_node=None):
        if master_node is None:
            rest = RestConnection(self.master)
        else:
            rest = RestConnection(master_node)
        nodes = rest.node_statuses()
        server_set = []
        for node in nodes:
            for server in self.input.servers:
                if server.ip == node.ip:
                    server_set.append(server)
        return server_set

    # =========================================================================
    # Script Search: Upgrade Tests — Epic MB-65018
    # =========================================================================

    def _setup_udf_data(self):
        """Load UDF docs and create the index. Toggle is handled by callers at each stage"""
        from .udf_datagen.udf_datagen import generate_docs, compute_ground_truth
        self._cb_cluster = self.cb_cluster
        self._udf = UDFHelper(self)
        docs = generate_docs()
        self._udf_gt = compute_ground_truth()
        self._udf_total = len(docs)
        self._udf._load_udf_docs(docs)
        self._udf._create_udf_index()
        self._udf._wait_udf_index(self._udf_total)

    def _udf_normal_query(self):
        """Fire a plain FTS query. Returns (status, total_hits)."""
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "query": {"match": "hotel", "field": "type"},
        })
        return s, r.get("total_hits", 0)

    def _udf_script_query(self):
        """Fire a custom_filter (script) query. Returns (status, response_body)."""
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function f(doc, params){ return true; }",
            }},
        })
        return s, r

    def test_udf_online_upgrade(self):
        """
        Online rolling upgrade test for UDF / custom-script queries (MB-65018).

        Stage 0 — pre-upgrade baseline (all nodes old version):
            normal query → 200, script query → error (unknown query type on old version)
        Stage 1 — upgrade one FTS node (rebalance out, install new version, rebalance in)
        Stage 2 — mixed cluster check (one new node, one old node):
            normal query → 200, script query → error
            (old node: unknown type; new node: toggle OFF by default → 400)
        Stage 3 — upgrade remaining FTS nodes
        Stage 4 — post-upgrade check, toggle OFF (design-doc default = false):
            normal query → 200, script query → 400 (udf_disabled)
        Stage 5 — operator enables toggle:
            normal query → 200, script query → 200, hits correct
        """
        errors = {}

        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if not fts_nodes or len(fts_nodes) < 2:
            self.fail("test_udf_online_upgrade requires >= 2 FTS nodes")

        # Setup: load data and create index on pre-upgrade cluster.
        self._setup_udf_data()

        # Stage 0: pre-upgrade baseline
        log.info("=" * 20 + " Stage 0: pre-upgrade UDF check")

        s, hits = self._udf_normal_query()
        if s != 200:
            errors['s0_normal'] = f"normal query failed pre-upgrade: status={s}"
        log.info(f"Stage 0: normal query status={s} hits={hits}")

        s, r = self._udf_script_query()
        if s == 200:
            errors['s0_script'] = f"script query should fail pre-upgrade (unknown type), got 200"
        log.info(f"Stage 0: script query status={s} (expect non-200 on pre-upgrade node)")

        # Stage 1: upgrade one FTS node
        nodes_out = [fts_nodes[0]]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
        rebalance.result()
        upgrade_th = self._async_update(self.upgrade_to, nodes_out)
        for th in upgrade_th:
            th.join()
        log.info("==== First FTS node upgraded ====")
        self.sleep(120)

        services_in = []
        for service in list(self.services_map.keys()):
            for node in nodes_out:
                if "{0}:{1}".format(node.ip, node.port) in self.services_map[service]:
                    services_in.append(service)
        self.cluster.async_rebalance(
            self.servers[:self.nodes_init], nodes_out, [], services=services_in
        ).result()

        # Stage 2: mixed cluster
        # old node: unknown query type → error
        # new node: toggle OFF by default → 400 udf_disabled
        # coordinator: error from at least one shard → returns error (design doc Case 3)
        log.info("=" * 20 + " Stage 2: mixed cluster UDF check")

        s, hits = self._udf_normal_query()
        if s != 200:
            errors['s2_normal'] = f"normal query failed in mixed cluster: status={s}"
        log.info(f"Stage 2: normal query status={s} hits={hits}")

        s, r = self._udf_script_query()
        if s == 200:
            errors['s2_script'] = f"script query should fail in mixed cluster, got 200"
        log.info(f"Stage 2: script query status={s} (expect non-200, mixed cluster)")

        # Stage 3: upgrade remaining FTS nodes
        nodes_out = fts_nodes[1:]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
        rebalance.result()
        upgrade_th = self._async_update(self.upgrade_to, nodes_out)
        for th in upgrade_th:
            th.join()
        log.info("==== All FTS nodes upgraded ====")
        self.sleep(120)

        services_in = []
        for service in list(self.services_map.keys()):
            for node in nodes_out:
                if "{0}:{1}".format(node.ip, node.port) in self.services_map[service]:
                    services_in.append(service)
        self.cluster.async_rebalance(
            self.servers[:self.nodes_init], nodes_out, [], services=services_in
        ).result()

        # Stage 4: post-upgrade, toggle OFF (default per design doc)
        log.info("=" * 20 + " Stage 4: post-upgrade toggle OFF")

        s, hits = self._udf_normal_query()
        if s != 200:
            errors['s4_normal'] = f"normal query failed post-upgrade: status={s}"
        log.info(f"Stage 4: normal query status={s} hits={hits}")

        s, r = self._udf_script_query()
        if s != 400:
            errors['s4_script'] = f"post-upgrade toggle OFF: expected 400 (udf_disabled), got {s}"
        log.info(f"Stage 4: script query status={s} (expect 400 — toggle OFF by default)")

        # Stage 5: enable toggle
        log.info("=" * 20 + " Stage 5: enable UDF toggle")
        self._udf._enable_udf()
        time.sleep(3)  # propagation ~1-2s

        s, hits = self._udf_normal_query()
        if s != 200:
            errors['s5_normal'] = f"normal query failed after toggle ON: status={s}"
        log.info(f"Stage 5: normal query status={s} hits={hits}")

        gt = self._udf_gt
        hits_script, err_script = self._udf._udf_query({
            "size": gt["cf_hotels"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["type"],
                "source": "function f(doc, params){ var fld = doc.fields || {}; return fld.type === 'hotel'; }",
            }},
        })
        if err_script or hits_script != gt["cf_hotels"]:
            errors['s5_script'] = f"script query failed after toggle ON: hits={hits_script} err={err_script}"
        log.info(f"Stage 5: script query hits={hits_script} (expect {gt['cf_hotels']})")

        if errors:
            self.fail("test_udf_online_upgrade failed:\n" +
                      "\n".join(f"  {k}: {v}" for k, v in errors.items()))
        log.info("test_udf_online_upgrade PASSED")

    # =========================================================================
    # =========================================================================

    def _upgrade_workload(self, label, driver, index):
        """Concurrent CRUD + query workload around one rebalance of an upgrade."""
        if driver is None:
            return contextlib.nullcontext()

        workload = FTSConcurrentWorkload(self, index=index, driver=driver, label=label)

        @contextlib.contextmanager
        def _run():
            with workload:
                yield
            self.upgrade_workload_errors.extend(workload.errors)

        return _run()

    def _assert_cluster_fully_upgraded(self, label=""):
        """Every cluster node must be on upgrade_to before version-gated checks run.

        A single node left behind silently disables cluster-wide features
        (encryption at rest among them) while every individual node upgrade still
        reports success, so the suite would otherwise carry on testing nothing.
        Returns a list of findings.
        """
        errors = []
        target = str(self.upgrade_to or "").split('-')[0]
        try:
            rest = RestConnection(self.master)
            versions = rest.get_nodes_versions()
        except Exception as err:
            return [f"[{label}] could not read node versions: {err}"]

        stale = [v for v in versions if not str(v).startswith(target)]
        if stale:
            errors.append(
                f"[{label}] not every cluster node is on {self.upgrade_to}: {versions}. "
                f"A node left on the old build blocks cluster-wide features.")
        else:
            log.info(f"[{label}] all cluster nodes on {target}: {versions}")

        try:
            compat = rest.get_pools_default().get("nodes", [{}])[0].get("clusterCompatibility")
            major = str(target).split('.')[0]
            minor = str(target).split('.')[1] if '.' in str(target) else '0'
            expected = int(major) * 65536 + int(minor)
            if compat is not None and int(compat) < expected:
                errors.append(
                    f"[{label}] cluster compatibility is {compat}, expected >= {expected} "
                    f"for {target}. Version-gated features stay disabled until it advances.")
            else:
                log.info(f"[{label}] cluster compatibility {compat} (>= {expected})")
        except Exception as err:
            log.warning(f"[{label}] could not read cluster compatibility: {err}")
        return errors

    def _remaining_cluster_nodes(self, already_upgraded):
        """Cluster nodes not in `already_upgraded`, master last."""
        done = {(n.ip, str(n.port)) for n in already_upgraded}
        rest = [n for n in self.servers[:self.nodes_init]
                if (n.ip, str(n.port)) not in done]
        master_ip = getattr(self.master, 'ip', None)
        return sorted(rest, key=lambda n: n.ip == master_ip)

    def _upgrade_rest_of_cluster(self, already_upgraded, label, driver=None, index=None):
        """Upgrade every remaining cluster node, so the cluster ends fully upgraded.

        Upgrading only the FTS nodes leaves cluster-wide features (encryption at
        rest, for one) still served by a pre-upgrade master.
        """
        remaining = self._remaining_cluster_nodes(already_upgraded)
        if not remaining:
            return []
        for node in remaining:
            self._move_rest_endpoint_off(node, label)
            self._rolling_upgrade_fts_nodes([node], label=f"{label} [{node.ip}]",
                                            driver=driver, index=index)
        return remaining

    def _move_rest_endpoint_off(self, node, label=""):
        """Ensure `node` is not self.servers[0] before it is rebalanced out.

        RebalanceTask talks to servers[0] (task.py: RestConnection(self.servers[0])),
        so rebalancing that same node out and back in makes ns_server reject the
        add as a self-join. Rotate another cluster node to the front instead;
        membership is unchanged, only the order.
        """
        if self.servers[0].ip != node.ip:
            return
        cluster = self.servers[:self.nodes_init]
        tail = self.servers[self.nodes_init:]
        replacement = next((s for s in cluster if s.ip != node.ip), None)
        if replacement is None:
            return
        self.servers = ([replacement] + [s for s in cluster if s.ip != replacement.ip]) + tail
        self.master = replacement
        self.rest = RestConnection(self.master)
        log.info(f"{label}: REST endpoint moved to {replacement.ip} before upgrading {node.ip}")

    def construct_custom_plan_params(self, replicas, partitions):
        """Plan params for an FTS index."""
        return {'numReplicas': replicas, 'indexPartitions': partitions}

    def get_fts_query_node(self):
        """Node to send FTS queries to; servers[0] may not run fts."""
        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if not fts_nodes:
            self.log.warning("no node in the cluster runs fts, falling back to %s"
                             % self.servers[0].ip)
            return self.servers[0]
        return fts_nodes[0]

    def _rolling_upgrade_fts_nodes(self, nodes_out, label="", version=None,
                                   driver=None, index=None):
        """Rebalance nodes out, install `version` on them, rebalance back in."""
        version = version or self.upgrade_to
        log.info("=" * 20 + f" Rolling upgrade {label}: {[n.ip for n in nodes_out]} -> {version}")

        # CBQE-8242
        with self._upgrade_workload(f"{label} rebalance-out", driver, index):
            self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out).result()

        for thread in self._async_update(version, nodes_out):
            thread.join()
        log.info(f"==== {label}: nodes upgraded to {version} ====")
        self.sleep(120)

        services_in = []
        for service in list(self.services_map.keys()):
            for node in nodes_out:
                if "{0}:{1}".format(node.ip, node.port) in self.services_map[service]:
                    services_in.append(service)
        with self._upgrade_workload(f"{label} rebalance-in", driver, index):
            self.cluster.async_rebalance(
                self.servers[:self.nodes_init], nodes_out, [], services=services_in
            ).result()
        log.info(f"==== {label}: nodes rebalanced back in ====")
        return nodes_out

    def _offline_upgrade_all_nodes(self, label="", version=None):
        # CBQE-8242
        """Stop every node, upgrade them all to `version`, bring the cluster back up."""
        version = version or self.upgrade_to
        log.info("=" * 20 + f" Offline upgrade {label}: stopping all nodes -> {version}")
        for server in self.servers:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()

        for thread in self._async_update(version, self.servers):
            thread.join()
        self.add_built_in_server_user()
        log.info(f"==== {label}: offline upgrade to {version} complete ====")
        self.sleep(120)

    # =========================================================================
    # =========================================================================

    def _totoro_post_upgrade_checks(self, label):
        """CBQE-8242 items 2-4 applied to the Totoro feature upgrade tests."""
        errors = {}
        if self.upgrade_workload_errors:
            errors['workload_during_upgrade'] = list(self.upgrade_workload_errors)
            self.upgrade_workload_errors = []

        crud_errors = self._post_upgrade_crud_all_buckets(label=label)
        if crud_errors:
            errors['post_upgrade_crud_all_buckets'] = crud_errors

        index_errors = self._post_upgrade_new_index_check(
            label=label, index_name="totoro_post_idx")
        if index_errors:
            errors['post_upgrade_new_index'] = index_errors
        return errors

    def _vector_feature_knobs(self):
        """Read the Totoro vector knobs from conf params."""
        knobs = {}
        bq_index_type = self.input.param("bq_index_type", None)
        if bq_index_type:
            knobs['bq_index_type'] = bq_index_type
        if self.input.param("fastmerge", False):
            knobs['fastmerge'] = True
        if self.input.param("gpu_index", False):
            knobs['gpu'] = True
        return knobs

    def _vector_feature_label(self, knobs):
        return ", ".join(f"{k}={v}" for k, v in sorted(knobs.items())) or "none"

    def _push_all_vector_data(self, fts_callable, end_index=10005001):
        """Load the vector dataset in the four xattr/base64 permutations."""
        for xattr in (False, True):
            for base64_flag in (False, True):
                try:
                    fts_callable.push_vector_data(
                        self.servers[0],
                        str(self.rest_settings.rest_username),
                        str(self.rest_settings.rest_password),
                        xattr=xattr, base64Flag=base64_flag, end_index=end_index)
                except Exception as err:
                    log.warning(f"push_vector_data(xattr={xattr}, base64={base64_flag}) failed: {err}")

    def _validate_vector_feature_definition(self, index_name, knobs, node=None):
        """Confirm the requested Totoro knobs survived a round trip through FTS."""
        errors = []
        target = node if node is not None else self.get_fts_query_node()
        status, index_def = RestConnection(target).get_fts_index_definition(name=index_name)
        if not status:
            return [f"could not read definition of '{index_name}': {index_def}"]

        definition = index_def['indexDef']
        store = definition.get('params', {}).get('store', {})
        properties = (definition.get('params', {}).get('mapping', {})
                      .get('types', {}).get('_default._default', {}).get('properties', {}))

        vector_field = None
        for prop in properties.values():
            fields = prop.get('fields') or []
            if fields and fields[0].get('type', '').startswith('vector'):
                vector_field = fields[0]
                break
        if vector_field is None:
            errors.append(f"'{index_name}': no vector field found in definition {properties}")
            return errors

        if 'bq_index_type' in knobs:
            actual = vector_field.get('vector_index_optimized_for')
            if actual != knobs['bq_index_type']:
                errors.append(f"'{index_name}': vector_index_optimized_for="
                              f"{actual!r}, expected {knobs['bq_index_type']!r}")
        if 'fastmerge' in knobs:
            if not store.get('vector_index_fast_merge', False):
                errors.append(f"'{index_name}': vector_index_fast_merge not set in params.store: {store}")
        if 'gpu' in knobs:
            if not vector_field.get('gpu', False):
                errors.append(f"'{index_name}': gpu not set on vector field: {vector_field}")

        if not errors:
            log.info(f"'{index_name}': Totoro knobs validated ({self._vector_feature_label(knobs)})")
        return errors

    def _vector_upgrade_setup(self):
        """Pre-upgrade: load vectors, build a legacy index, capture a recall baseline."""
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   variable_node=self.get_fts_query_node(),
                                   servers=self.servers)
        fts_callable.load_data(self.num_items)
        self._push_all_vector_data(fts_callable)

        plans = self.construct_custom_plan_params(0, self.input.param("num_partitions", 1))
        result, status = fts_callable.create_vector_index(
            False, False, self.LEGACY_VECTOR_INDEX, plans, dimensions=self.vector_dimension, node=self.get_fts_query_node())
        self.assertEqual(status, 200,
                         f"failed to create pre-upgrade legacy vector index: {result}")
        self.sleep(self.vector_index_build_wait, "letting the legacy vector index build")

        passed, baseline = fts_callable.run_vector_queries_stats(
            index_name=self.LEGACY_VECTOR_INDEX)
        self.assertTrue(passed,
                        f"pre-upgrade baseline kNN queries failed on the old build: {baseline}")
        log.info(f"Pre-upgrade baseline for '{self.LEGACY_VECTOR_INDEX}': {baseline}")
        return fts_callable, baseline

    def _assert_feature_index_rejected(self, fts_callable, knobs, index_name, stage, node=None):
        """A Totoro-knob index must not be creatable before the cluster supports it."""
        errors = []
        plans = self.construct_custom_plan_params(0, 1)
        result, status = fts_callable.create_vector_index(
            False, False, index_name, plans, dimensions=self.vector_dimension,
            node=node, **knobs)
        if status == 200:
            errors.append(
                f"[{stage}] vector index with {self._vector_feature_label(knobs)} was "
                f"created on a cluster that does not fully support it (result={result})")
            try:
                fts_callable.delete_fts_index(index_name)
            except Exception as err:
                log.warning(f"could not clean up unexpected index '{index_name}': {err}")
        else:
            log.info(f"[{stage}] got the expected rejection for "
                     f"{self._vector_feature_label(knobs)}: {result}")
        return errors

    def _validate_legacy_index_survived(self, fts_callable, baseline, stage):
        """The pre-upgrade index must still answer kNN queries at its baseline recall."""
        errors = []
        passed, stats = fts_callable.run_vector_queries_stats(index_name=self.LEGACY_VECTOR_INDEX)
        if not passed:
            errors.append(f"[{stage}] legacy vector index queries failed after upgrade: {stats}")
            return errors

        tolerance = self.vector_recall_tolerance
        for metric in ('fts_recall', 'fts_accuracy'):
            before, after = baseline.get(metric, 0), stats.get(metric, 0)
            if after < before - tolerance:
                errors.append(
                    f"[{stage}] legacy index {metric} regressed across upgrade: "
                    f"{before} -> {after} (tolerance {tolerance})")
        log.info(f"[{stage}] legacy index survived upgrade: {stats} (baseline {baseline})")
        return errors

    def test_vector_features_online_upgrade(self):
        """Online rolling upgrade for the Totoro vector index features."""
        errors = {}
        knobs = self._vector_feature_knobs()
        if not knobs:
            self.skipTest("no vector feature requested - set bq_index_type, "
                          "fastmerge=True and/or gpu_index=True in the conf entry")
        log.info(f"Vector feature upgrade under test: {self._vector_feature_label(knobs)}")

        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if not fts_nodes or len(fts_nodes) < 2:
            self.fail("test_vector_features_online_upgrade requires >= 2 FTS nodes")

        log.info("=" * 20 + " Stage 0: pre-upgrade vector baseline")
        fts_callable, baseline = self._vector_upgrade_setup()

        stage0 = self._assert_feature_index_rejected(
            fts_callable, knobs, "vec_feature_stage0", "Stage 0")
        if stage0:
            errors['s0_feature_index'] = stage0

        # CBQE-8242
        self._rolling_upgrade_fts_nodes([fts_nodes[0]], label="Stage 1 (first FTS node)",
                                        driver=fts_callable)

        log.info("=" * 20 + " Stage 2: mixed-cluster vector checks")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   variable_node=self.get_fts_query_node(), servers=self.servers)
        stage2 = self._validate_legacy_index_survived(fts_callable, baseline, "Stage 2")
        if stage2:
            errors['s2_legacy_index'] = stage2

        stage2_feature = self._assert_feature_index_rejected(
            fts_callable, knobs, "vec_feature_stage2", "Stage 2")
        if stage2_feature:
            errors['s2_feature_index'] = stage2_feature

        self._upgrade_rest_of_cluster([fts_nodes[0]], "Stage 3 (rest of cluster)",
                                      driver=fts_callable)

        log.info("=" * 20 + " Stage 4: post-upgrade vector feature checks")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   variable_node=self.get_fts_query_node(), servers=self.servers)
        errors.update(self._vector_post_upgrade_checks(fts_callable, knobs, baseline))

        errors.update(self._totoro_post_upgrade_checks("vector online upgrade"))

        if errors:
            self.fail("test_vector_features_online_upgrade failed:\n" +
                      "\n".join(f"  {k}: {v}" for k, v in errors.items()))
        log.info("test_vector_features_online_upgrade PASSED")

    def test_vector_features_offline_upgrade(self):
        """Offline upgrade for the Totoro vector index features."""
        errors = {}
        knobs = self._vector_feature_knobs()
        if not knobs:
            self.skipTest("no vector feature requested - set bq_index_type, "
                          "fastmerge=True and/or gpu_index=True in the conf entry")
        log.info(f"Vector feature offline upgrade under test: {self._vector_feature_label(knobs)}")

        log.info("=" * 20 + " Pre-upgrade vector baseline")
        fts_callable, baseline = self._vector_upgrade_setup()

        pre = self._assert_feature_index_rejected(
            fts_callable, knobs, "vec_feature_pre", "pre-upgrade")
        if pre:
            errors['pre_feature_index'] = pre

        self._offline_upgrade_all_nodes(label="vector features")

        log.info("=" * 20 + " Post-upgrade vector feature checks")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   variable_node=self.get_fts_query_node(), servers=self.servers)
        errors.update(self._vector_post_upgrade_checks(fts_callable, knobs, baseline))

        errors.update(self._totoro_post_upgrade_checks("vector offline upgrade"))

        if errors:
            self.fail("test_vector_features_offline_upgrade failed:\n" +
                      "\n".join(f"  {k}: {v}" for k, v in errors.items()))
        log.info("test_vector_features_offline_upgrade PASSED")

    def _vector_post_upgrade_checks(self, fts_callable, knobs, baseline):
        """Shared post-upgrade stage for the online and offline vector tests."""
        errors = {}

        survived = self._validate_legacy_index_survived(fts_callable, baseline, "post-upgrade")
        if survived:
            errors['post_legacy_index'] = survived

        result, status = fts_callable.update_vector_index(
            False, False, self.LEGACY_VECTOR_INDEX, dimensions=self.vector_dimension, **knobs, node=self.get_fts_query_node())
        if status != 200:
            errors['post_legacy_update'] = [
                f"could not enable {self._vector_feature_label(knobs)} on the existing "
                f"index '{self.LEGACY_VECTOR_INDEX}': {result}"]
        else:
            self.sleep(self.vector_index_build_wait, "letting the updated index rebuild with the feature on")
            def_errors = self._validate_vector_feature_definition(self.LEGACY_VECTOR_INDEX, knobs)
            if def_errors:
                errors['post_legacy_definition'] = def_errors
            passed, stats = fts_callable.run_vector_queries_stats(
                index_name=self.LEGACY_VECTOR_INDEX)
            if not passed:
                errors['post_legacy_feature_query'] = [
                    f"kNN queries failed after switching the existing index onto "
                    f"{self._vector_feature_label(knobs)}: {stats}"]
            else:
                log.info(f"in-place feature enable on the legacy index: {stats}")

        plans = self.construct_custom_plan_params(0, self.input.param("num_partitions", 1))
        result, status = fts_callable.create_vector_index(
            False, False, self.FEATURE_VECTOR_INDEX, plans,
            dimensions=self.vector_dimension, **knobs, node=self.get_fts_query_node())
        if status != 200:
            errors['post_feature_index_create'] = [
                f"could not create a {self._vector_feature_label(knobs)} index on the "
                f"upgraded cluster: {result}"]
            return errors

        self.sleep(self.vector_index_build_wait, "letting the new feature index build")
        def_errors = self._validate_vector_feature_definition(self.FEATURE_VECTOR_INDEX, knobs)
        if def_errors:
            errors['post_feature_definition'] = def_errors

        passed, stats = fts_callable.run_vector_queries_stats(index_name=self.FEATURE_VECTOR_INDEX)
        if not passed:
            errors['post_feature_query'] = [
                f"kNN queries failed on the new {self._vector_feature_label(knobs)} "
                f"index: {stats}"]
        else:
            tolerance = self.vector_recall_tolerance
            if stats.get('fts_recall', 0) < baseline.get('fts_recall', 0) - tolerance:
                errors['post_feature_recall'] = [
                    f"{self._vector_feature_label(knobs)} recall {stats.get('fts_recall')} is "
                    f"more than {tolerance} below the pre-upgrade baseline "
                    f"{baseline.get('fts_recall')}"]
            log.info(f"new feature index stats: {stats} (baseline {baseline})")

        return errors

    # =========================================================================
    # =========================================================================

    def _ear_setup_baseline(self, ear):
        """Pre-upgrade: index data on an unencrypted cluster and prove it is plaintext."""
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   servers=self.servers)
        fts_callable.load_data(self.num_items)
        index = fts_callable.create_fts_index(
            self.EAR_INDEX, source_type='couchbase', source_name="default",
            index_type='fulltext-index', index_params=None, plan_params=None,
            source_params=None, source_uuid=None, collection_index=False,
            _type=None, analyzer="standard", no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(self.num_items)

        hits, _, _, status = index.execute_query(query=self.EAR_QUERY)
        self.assertNotEqual(hits, -1, f"pre-upgrade baseline query failed: status={status}")
        log.info(f"Pre-upgrade baseline: {hits} hits for {self.EAR_QUERY}")

        plaintext_errors = ear.segments_plaintext_errors(
            label="pre-upgrade segments should be plaintext")
        self.assertEqual(
            plaintext_errors, [],
            "pre-upgrade segments were not readable as plaintext, so a later "
            f"'encrypted' verdict would prove nothing: {plaintext_errors}")

        return fts_callable, index, hits

    def _ear_post_upgrade_checks(self, ear, index, baseline_hits):
        """Enable encryption on the fully upgraded cluster and verify it took effect."""
        errors = {}

        upgrade_errors = self._assert_cluster_fully_upgraded("post-upgrade")
        if upgrade_errors:
            errors['cluster_not_fully_upgraded'] = upgrade_errors
            return errors

        secret_id = ear.create_kek()
        if secret_id is None:
            errors['kek'] = "failed to create the bucket-encryption KEK after upgrade"
            return errors

        status, response = ear.try_enable_bucket_encryption("default", secret_id)
        if not status:
            errors['enable'] = (f"enabling bucket encryption failed on the fully "
                                f"upgraded cluster: {response}")
            return errors
        log.info("Bucket encryption enabled post-upgrade")

        # A 200 from the bucket POST only means the request was accepted -- a
        # pre-8.1 node returns 200 while ignoring the parameter entirely. Read the
        # setting back so "enabled" means the cluster actually stored it.
        applied = ear.bucket_encryption_key_id("default")
        if applied is None or str(applied) != str(secret_id):
            errors['enable_not_applied'] = (
                f"bucket encryption was accepted but not applied: "
                f"encryptionAtRestKeyId reads back as {applied!r}, expected {secret_id!r}")
        else:
            log.info(f"bucket 'default' reports encryptionAtRestKeyId={applied}")

        # Do NOT call controller/dropEncryptionAtRestDeks here. That drops the DEKs
        # to force a ROTATION of already-encrypted data; calling it right after
        # enabling encryption throws away the DEK FTS has just started using and
        # restarts from nothing. cbauth polls FTS for keys in use, sees the empty
        # key meaning unencrypted data remains, and drives the re-encryption on its
        # own -- the test just has to wait for that cycle.

        if not ear.getinusekeys_available():
            errors['getinusekeys'] = (
                "FTS GetInUseKeys (:8094/api/encryption/GetInUseKeys) answered "
                "'Page not found' after the upgrade. FTS encryption-at-rest is "
                "expected to work on this build, so the endpoint should be present.")

        completed, deks = ear.wait_for_encryption_complete(
            "default", timeout=self.ear_reencryption_timeout)
        if not completed:
            errors['reencryption'] = (
                f"FTS still reported unencrypted data for 'default' after "
                f"{self.ear_reencryption_timeout}s (deks={deks})")
        elif not deks:
            errors['deks'] = "no FTS DEK in use for 'default' after enabling encryption"

        # Whether enabling encryption on an ALREADY-BUILT index retroactively
        # re-encrypts its existing segments is still an open question with the
        # FTS team. Report it, but only fail the test when
        # enforce_segment_encryption=True, so this one check does not block the
        # other 21 tests in the suite from running.
        # Best-effort flush of rewritten segments; not the trigger, so keep it short.
        ear.force_merge_and_wait(index.name, timeout=120)
        segment_errors = ear.segments_encrypted_errors_settled(label="post-upgrade segments")
        if segment_errors:
            errors['segments'] = segment_errors

        hits, _, _, status = index.execute_query(query=self.EAR_QUERY)
        if hits != baseline_hits:
            errors['data_loss'] = (f"hits changed across upgrade + encryption: "
                                   f"baseline={baseline_hits}, now={hits} (status={status})")
        else:
            log.info(f"Post-upgrade query still returns {hits} hits - no data loss")

        return errors

    def test_ear_online_upgrade(self):
        """Online rolling upgrade for FTS encryption at rest."""
        errors = {}
        ear = EARUpgradeHelper(self)

        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if not fts_nodes or len(fts_nodes) < 2:
            self.fail("test_ear_online_upgrade requires >= 2 FTS nodes")

        try:
            log.info("=" * 20 + " Stage 0: pre-upgrade EAR baseline")
            fts_callable, index, baseline_hits = self._ear_setup_baseline(ear)

            if ear.getinusekeys_available():
                errors['s0_getinusekeys'] = (
                    "GetInUseKeys answered on the pre-upgrade build - the cluster is "
                    "not actually running a pre-8.1 version, so this run proves nothing")

            if ear.create_kek() is not None:
                errors['s0_enable'] = (
                    "the encryption-at-rest secrets API answered on the pre-upgrade "
                    "build - the cluster is not actually pre-8.1")
            else:
                log.info("Stage 0: secrets API unreachable pre-upgrade, as expected")

            # CBQE-8242
            self._rolling_upgrade_fts_nodes([fts_nodes[0]], label="Stage 1 (first FTS node)",
                                            driver=fts_callable, index=index)

            log.info("=" * 20 + " Stage 2: mixed-cluster EAR checks")
            secret_id = ear.create_kek()
            if secret_id is None:
                log.info("Stage 2: KEK could not be created in a mixed cluster - "
                         "encryption is unreachable, which satisfies the gate")
            else:
                status, response = ear.try_enable_bucket_encryption("default", secret_id)
                if status:
                    errors['s2_enable'] = (
                        f"bucket encryption was accepted on a MIXED-version cluster - "
                        f"it must be blocked until every node is upgraded: {response}")
                    ear.try_disable_bucket_encryption("default")
                else:
                    log.info(f"Stage 2: encryption correctly blocked in mixed mode: {response}")

            hits, _, _, status = index.execute_query(query=self.EAR_QUERY)
            if hits != baseline_hits:
                errors['s2_query'] = (f"mixed-cluster query returned {hits} hits, "
                                      f"baseline was {baseline_hits} (status={status})")

            self._upgrade_rest_of_cluster([fts_nodes[0]], "Stage 3 (rest of cluster)",
                                          driver=fts_callable, index=index)

            log.info("=" * 20 + " Stage 4: post-upgrade EAR checks")
            errors.update(self._ear_post_upgrade_checks(ear, index, baseline_hits))
        finally:
            ear.cleanup_secrets()

        errors.update(self._totoro_post_upgrade_checks("EAR online upgrade"))

        if errors:
            self.fail("test_ear_online_upgrade failed:\n" +
                      "\n".join(f"  {k}: {v}" for k, v in errors.items()))
        log.info("test_ear_online_upgrade PASSED")

    def test_ear_offline_upgrade(self):
        """Offline upgrade for FTS encryption at rest."""
        errors = {}
        ear = EARUpgradeHelper(self)

        try:
            log.info("=" * 20 + " Pre-upgrade EAR baseline")
            fts_callable, index, baseline_hits = self._ear_setup_baseline(ear)

            if ear.create_kek() is not None:
                errors['pre_enable'] = (
                    "the encryption-at-rest secrets API answered on the pre-upgrade "
                    "build - the cluster is not actually pre-8.1")

            self._offline_upgrade_all_nodes(label="encryption at rest")

            log.info("=" * 20 + " Post-upgrade EAR checks")
            errors.update(self._ear_post_upgrade_checks(ear, index, baseline_hits))
        finally:
            ear.cleanup_secrets()

        errors.update(self._totoro_post_upgrade_checks("EAR offline upgrade"))

        if errors:
            self.fail("test_ear_offline_upgrade failed:\n" +
                      "\n".join(f"  {k}: {v}" for k, v in errors.items()))
        log.info("test_ear_offline_upgrade PASSED")

    # =========================================================================
    # CBQE-8244, MB-63246, MB-62427
    # =========================================================================

    def _fts_index_segment_version(self, index_name, node=None):
        """Read params.store.segmentVersion for an index, or None if unavailable."""
        try:
            target = node if node is not None else self.get_fts_query_node()
            status, index_def = RestConnection(target).get_fts_index_definition(name=index_name)
            if not status:
                return None
            return index_def['indexDef'].get('params', {}).get('store', {}).get('segmentVersion')
        except Exception as err:
            log.warning(f"could not read segmentVersion for '{index_name}': {err}")
            return None

    def _fts_panic_counts(self):
        """Panic/crash counts in fts.log per node, summed across keywords."""
        from lib.log_scanner import LogScanner
        counts = {}
        for node in self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True) or []:
            try:
                matches = LogScanner(server=node, skip_security_scan=True).scan() or {}
                counts[node.ip] = sum(matches.get('fts.log', {}).values())
            except Exception as err:
                log.warning(f"panic scan failed on {node.ip}: {err}")
                counts[node.ip] = 0
        return counts

    def _check_new_panics(self, baseline, label):
        """Errors for any fts.log panic that appeared since `baseline`."""
        errors = []
        current = self._fts_panic_counts()
        for node_ip, count in current.items():
            before = baseline.get(node_ip, 0)
            if count > before:
                errors.append(
                    f"[{label}] {count - before} new panic/crash line(s) in fts.log on "
                    f"{node_ip} (was {before}, now {count}) - see MB-62427")
        return errors, current

    def test_chained_upgrade(self):
        """Multi-hop chained upgrade carrying one index the whole way (CBQE-8244)."""
        errors = {}
        chain = [v for v in (self.upgrade_versions or []) if v]
        if len(chain) < 2:
            self.skipTest(
                "test_chained_upgrade needs at least two hops - the dispatcher "
                "must pass upgrade_version=A;B[;C] (got: %s)" % chain)

        upgrade_type = self.input.param("chained_upgrade_type", "offline")
        log.info("=" * 20 + f" Chained upgrade: {self.initial_version} -> " +
                 " -> ".join(chain) + f" ({upgrade_type})")

        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   servers=self.servers)
        fts_callable.load_data(self.num_items)
        carried_idx = fts_callable.create_fts_index(
            self.CHAINED_INDEX, source_type='couchbase', source_name="default",
            index_type='fulltext-index', index_params=None, plan_params=None,
            source_params=None, source_uuid=None, collection_index=False,
            _type=None, analyzer="standard", no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(self.num_items)

        baseline_hits, _, _, status = carried_idx.execute_query(query=self.EAR_QUERY)
        self.assertNotEqual(baseline_hits, -1,
                            f"baseline query failed on {self.initial_version}: {status}")
        indexed_before = carried_idx.get_indexed_doc_count()
        segment_versions = {self.initial_version: self._fts_index_segment_version(self.CHAINED_INDEX)}
        panic_baseline = self._fts_panic_counts()
        log.info(f"Baseline on {self.initial_version}: hits={baseline_hits}, "
                 f"indexed={indexed_before}, segmentVersion={segment_versions[self.initial_version]}")

        for hop, version in enumerate(chain, start=1):
            label = f"hop {hop} ({version})"
            log.info("=" * 20 + f" Chained upgrade {label}")

            if upgrade_type == "online":
                fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
                self._rolling_upgrade_fts_nodes(fts_nodes, label=label, version=version)
            else:
                self._offline_upgrade_all_nodes(label=label, version=version)

            fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                       variable_node=self.get_fts_query_node(),
                                       servers=self.servers)

            # MB-63246
            ingest_errors = self._validate_carried_index_ingests(
                fts_callable, carried_idx, indexed_before, label)
            if ingest_errors:
                errors[f"h{hop}_ingest"] = ingest_errors
            indexed_before = carried_idx.get_indexed_doc_count()

            hits, _, _, status = carried_idx.execute_query(query=self.EAR_QUERY)
            if hits == -1 or status == 'fail':
                errors[f"h{hop}_query"] = f"carried index could not be queried at {version}: {status}"
            elif hits < baseline_hits:
                errors[f"h{hop}_hits"] = (f"carried index lost data at {version}: "
                                          f"{hits} hits, baseline was {baseline_hits}")

            # MB-62427
            panic_errors, panic_baseline = self._check_new_panics(panic_baseline, label)
            if panic_errors:
                errors[f"h{hop}_panic"] = panic_errors

            segment_versions[version] = self._fts_index_segment_version(self.CHAINED_INDEX)
            log.info(f"{label}: hits={hits}, indexed={indexed_before}, "
                     f"segmentVersion={segment_versions[version]}")

        log.info(f"segmentVersion across the chain: {segment_versions}")

        post_errors = self._post_upgrade_new_index_check(
            label="post-chain", index_name="chained_post_idx")
        if post_errors:
            errors['post_chain_new_index'] = post_errors

        crud_errors = self._post_upgrade_crud_all_buckets(label="post-chain")
        if crud_errors:
            errors['post_chain_crud'] = crud_errors

        if errors:
            self.fail(f"test_chained_upgrade failed (chain: {self.initial_version} -> "
                      + " -> ".join(chain) + "):\n"
                      + "\n".join(f"  {k}: {v}" for k, v in errors.items()))
        log.info("test_chained_upgrade PASSED")

    def _validate_carried_index_ingests(self, fts_callable, index, indexed_before, label):
        """The carried index must pick up docs written after the hop (MB-63246)."""
        errors = []
        batch = self.input.param("chained_ingest_batch", 10000)
        try:
            fts_callable.load_data(batch)
        except Exception as err:
            return [f"[{label}] could not load the post-hop batch: {err}"]

        deadline = time.time() + self.input.param("chained_ingest_timeout", 600)
        indexed_after = indexed_before
        while time.time() < deadline:
            try:
                indexed_after = index.get_indexed_doc_count()
                if indexed_after > indexed_before:
                    break
            except Exception as err:
                log.info(f"[{label}] waiting for ingestion: {err}")
            time.sleep(10)

        if indexed_after <= indexed_before:
            errors.append(
                f"[{label}] carried index stopped ingesting after the upgrade: "
                f"indexed {indexed_before} before, {indexed_after} after loading "
                f"{batch} new docs - this is the MB-63246 signature")
        else:
            log.info(f"[{label}] carried index still ingests: "
                     f"{indexed_before} -> {indexed_after}")
        return errors

    # =========================================================================
    # =========================================================================

    def _search_history_supported(self, node):
        """True when :8094/api/searchHistory answers on this node (8.1+)."""
        try:
            RestConnection(node).get_search_history(limit=1)
            return True
        except Exception as err:
            log.info(f"searchHistory unavailable on {node.ip}: {err}")
            return False

    def _set_search_history(self, node, enabled):
        """Toggle searchHistoryEnabled on one node. Returns (ok, detail)."""
        try:
            RestConnection(node).set_node_setting("searchHistoryEnabled",
                                                  "true" if enabled else "false")
            return True, "ok"
        except Exception as err:
            return False, str(err)

    def _search_history_entries(self, node, index_name=None, limit=100):
        try:
            response = RestConnection(node).get_search_history(limit=limit, index=index_name)
            return response.get("results") or [], response.get("total", 0)
        except Exception as err:
            log.info(f"could not read search history from {node.ip}: {err}")
            return None, 0

    def test_search_history_online_upgrade(self):
        """Online rolling upgrade for FTS search history."""
        errors = {}
        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if not fts_nodes or len(fts_nodes) < 2:
            self.fail("test_search_history_online_upgrade requires >= 2 FTS nodes")

        log.info("=" * 20 + " Stage 0: pre-upgrade search-history baseline")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   servers=self.servers)
        fts_callable.load_data(self.num_items)
        index = fts_callable.create_fts_index(
            self.SEARCH_HISTORY_INDEX, source_type='couchbase', source_name="default",
            index_type='fulltext-index', index_params=None, plan_params=None,
            source_params=None, source_uuid=None, collection_index=False,
            _type=None, analyzer="standard", no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(self.num_items)

        if any(self._search_history_supported(n) for n in fts_nodes):
            errors['s0_endpoint'] = ("the searchHistory endpoint answered on the pre-upgrade "
                                     "build - the cluster is not actually pre-8.1, so this "
                                     "run proves nothing")

        self._rolling_upgrade_fts_nodes([fts_nodes[0]], label="Stage 1 (first FTS node)",
                                        driver=fts_callable, index=index)

        log.info("=" * 20 + " Stage 2: mixed-cluster search-history checks")
        upgraded, old = fts_nodes[0], fts_nodes[1]
        if not self._search_history_supported(upgraded):
            errors['s2_upgraded_node'] = (f"searchHistory did not answer on the upgraded node "
                                          f"{upgraded.ip}")
        if self._search_history_supported(old):
            errors['s2_old_node'] = (f"searchHistory answered on the NOT-yet-upgraded node "
                                     f"{old.ip}")

        hits, _, _, status = index.execute_query(query=self.EAR_QUERY)
        if hits == -1 or status == 'fail':
            errors['s2_query'] = f"query failed in the mixed cluster: status={status}"

        self._upgrade_rest_of_cluster([fts_nodes[0]], "Stage 3 (rest of cluster)",
                                      driver=fts_callable, index=index)

        log.info("=" * 20 + " Stage 4: post-upgrade search-history checks")
        current_fts = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)

        unsupported = [n.ip for n in current_fts if not self._search_history_supported(n)]
        if unsupported:
            errors['s4_endpoint'] = f"searchHistory still unavailable on {unsupported}"

        for node in current_fts:
            ok, detail = self._set_search_history(node, True)
            if not ok:
                errors.setdefault('s4_enable', []).append(f"{node.ip}: {detail}")
        self.sleep(10, "letting the searchHistoryEnabled setting propagate")

        probe_queries = self.input.param("search_history_queries", 10)
        for _ in range(probe_queries):
            index.execute_query(query=self.EAR_QUERY, zero_results_ok=True)
        self.sleep(15, "letting search history flush")

        total_entries = 0
        for node in current_fts:
            entries, total = self._search_history_entries(node, index_name=index.name)
            if entries is None:
                errors.setdefault('s4_read', []).append(f"could not read history from {node.ip}")
                continue
            total_entries += max(total, len(entries))
            log.info(f"search history on {node.ip}: {len(entries)} entries (total={total})")

        if total_entries == 0:
            errors['s4_recorded'] = (f"ran {probe_queries} queries with searchHistoryEnabled on "
                                     f"every FTS node, but no history was recorded")

        errors.update(self._totoro_post_upgrade_checks("search history upgrade"))
        if errors:
            self.fail("test_search_history_online_upgrade failed:\n" +
                      "\n".join(f"  {k}: {v}" for k, v in errors.items()))
        log.info("test_search_history_online_upgrade PASSED")

    # =========================================================================
    # =========================================================================

    DEEP_PAGINATION_FIELDS = {
        "salary": "number",
        "join_date": "datetime",
        "location": "geopoint",
    }

    def _nontextual_sort_mode(self, sort_type):
        """Sort spec for a non-textual field; "_id" breaks ties deterministically."""
        if sort_type == "numeric":
            return [{"by": "field", "field": "salary", "type": "number"}, "_id"]
        if sort_type == "datetime":
            return [{"by": "field", "field": "join_date", "type": "date"}, "_id"]
        if sort_type == "geo":
            return [{"by": "geo_distance", "field": "location", "unit": "mi",
                     "location": {"lat": 40.7128, "lon": -74.0060}}, "_id"]
        raise ValueError(f"unknown sort_type: {sort_type}")

    def _create_deep_pagination_index(self, fts_callable, index_name):
        """Index the emp dataset with docvalues on the non-textual sort fields."""
        index = fts_callable.create_fts_index(
            index_name, source_type='couchbase', source_name="default",
            index_type='fulltext-index', index_params=None, plan_params=None,
            source_params=None, source_uuid=None, collection_index=False,
            _type=None, analyzer="standard", no_check=False, cluster=self.cb_cluster)

        properties = {}
        for field, ftype in self.DEEP_PAGINATION_FIELDS.items():
            properties[field] = {
                "enabled": True, "dynamic": False,
                "fields": [{"docvalues": True, "include_term_vectors": True,
                            "index": True, "name": field, "type": ftype}],
            }
        mapping = index.index_definition['params']['mapping']
        mapping['default_mapping'] = {"dynamic": False, "enabled": True, "properties": properties}
        mapping['docvalues_dynamic'] = True
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        fts_callable.wait_for_indexing_complete(self.num_items)
        return index

    def _check_deep_pagination(self, index, sort_type, label):
        """search_after / search_before must line up with a full ordered scan."""
        errors = []
        partial_size = self.input.param("partial_size", 2)
        start_index = self.input.param("partial_start_index", 3)
        try:
            sort_mode = self._nontextual_sort_mode(sort_type)
        except ValueError as err:
            return [f"[{label}] {err}"]

        cluster = index.get_cluster()
        base_query = {"explain": False, "fields": ["*"], "highlight": {},
                      "query": {"match_all": {}}, "size": self.num_items, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, base_query)
        if not all_matches or len(all_matches) <= start_index + partial_size:
            return [f"[{label}/{sort_type}] full scan returned too few rows to paginate "
                    f"(hits={all_hits}, matches={len(all_matches or [])})"]
        all_ids = [m['id'] for m in all_matches]

        anchor = all_matches[start_index].get('decoded_sort', all_matches[start_index]['sort'])
        after_q = dict(base_query, size=partial_size, search_after=anchor)
        _, after_matches, _, _ = cluster.run_fts_query(index.name, after_q)
        for i, match in enumerate(after_matches or []):
            expected = start_index + 1 + i
            if expected < len(all_ids) and match['id'] != all_ids[expected]:
                errors.append(f"[{label}/{sort_type}] search_after position {i}: got "
                              f"{match['id']}, expected {all_ids[expected]}")

        before_q = dict(base_query, size=partial_size, search_before=anchor)
        _, before_matches, _, _ = cluster.run_fts_query(index.name, before_q)
        before_ids = [m['id'] for m in (before_matches or [])]
        expected_before = all_ids[max(0, start_index - len(before_ids)):start_index]
        if before_ids and before_ids != expected_before:
            errors.append(f"[{label}/{sort_type}] search_before returned {before_ids}, "
                          f"expected {expected_before}")

        if not errors:
            log.info(f"[{label}/{sort_type}] deep pagination consistent with the full scan")
        return errors

    def test_deep_pagination_online_upgrade(self):
        """Online rolling upgrade for deep pagination over non-textual content."""
        errors = {}
        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if not fts_nodes or len(fts_nodes) < 2:
            self.fail("test_deep_pagination_online_upgrade requires >= 2 FTS nodes")

        sort_types = [t.strip() for t in
                      str(self.input.param("sort_types", "numeric;datetime")).split(";") if t.strip()]

        log.info("=" * 20 + " Stage 0: pre-upgrade deep-pagination baseline")
        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   servers=self.servers)
        fts_callable.load_data(self.num_items)
        index = self._create_deep_pagination_index(fts_callable, self.DEEP_PAGINATION_INDEX)

        pre = self._check_deep_pagination(index, sort_types[0], "Stage 0")
        if not pre:
            errors['s0_worked'] = (f"deep pagination over a {sort_types[0]} sort already worked "
                                   f"on the pre-upgrade build - either the cluster is not "
                                   f"pre-8.1 or the check is not exercising the feature")
        else:
            log.info(f"Stage 0: deep pagination not yet supported, as expected: {pre}")

        self._rolling_upgrade_fts_nodes([fts_nodes[0]], label="Stage 1 (first FTS node)",
                                        driver=fts_callable, index=index)
        self._upgrade_rest_of_cluster([fts_nodes[0]], "Stage 3 (rest of cluster)",
                                      driver=fts_callable, index=index)

        log.info("=" * 20 + " Stage 4: post-upgrade deep-pagination checks")
        for sort_type in sort_types:
            found = self._check_deep_pagination(index, sort_type, "post-upgrade")
            if found:
                errors[f"post_{sort_type}"] = found

        errors.update(self._totoro_post_upgrade_checks("deep pagination upgrade"))
        if errors:
            self.fail("test_deep_pagination_online_upgrade failed:\n" +
                      "\n".join(f"  {k}: {v}" for k, v in errors.items()))
        log.info("test_deep_pagination_online_upgrade PASSED")

    # =========================================================================
    # =========================================================================

    FTS_MAX_COLLECTIONS_PER_INDEX = 100

    def _bulk_create_collections(self, num_scopes, collections_per_scope, bucket="default"):
        """Create num_scopes x collections_per_scope collections via the manifest API."""
        total = num_scopes * collections_per_scope
        log.info(f"Bulk creating {num_scopes} scopes x {collections_per_scope} collections "
                 f"({total} total) on '{bucket}'")
        status = BucketOperationHelper.bulk_create_collection_on_bucket(
            server_info=self.master, bucket_name=bucket,
            scopes=num_scopes, collections_per_scope=collections_per_scope,
            preserve_og_manifest=True,
            scope_prefix="bulk_scope_", collection_prefix="bulk_coll_")
        self.assertTrue(status, f"bulk collection creation failed on '{bucket}'")
        self.sleep(30, "letting the collection manifest settle")
        return total

    def test_collections_scale_online_upgrade(self):
        """Online rolling upgrade of a cluster carrying thousands of collections."""
        errors = {}
        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if not fts_nodes or len(fts_nodes) < 2:
            self.fail("test_collections_scale_online_upgrade requires >= 2 FTS nodes")

        num_scopes = self.input.param("num_scopes", 1)
        per_scope = self.input.param("num_collections_per_scope", 1000)
        fts_collections = min(self.input.param("fts_collections", 100),
                              per_scope, self.FTS_MAX_COLLECTIONS_PER_INDEX)

        total = self._bulk_create_collections(num_scopes, per_scope)
        log.info(f"Indexing {fts_collections} of {total} collections")

        scope = "bulk_scope_0"
        collections = [f"bulk_coll_0_{i}" for i in range(fts_collections)]

        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   servers=self.servers, scope=scope, collections=collections,
                                   collection_index=True)
        fts_callable.load_data(self.num_items)

        index = fts_callable.create_fts_index(
            self.COLLECTIONS_INDEX, source_type='couchbase', source_name="default",
            index_type='fulltext-index', index_params=None, plan_params=None,
            source_params=None, source_uuid=None, collection_index=True,
            _type=[f"{scope}.{c}" for c in collections], analyzer="standard",
            scope=scope, collections=collections, no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete()

        indexed_before = index.get_indexed_doc_count()
        hits_before, _, _, status = index.execute_query(query={"match_all": {}},
                                                        zero_results_ok=True)
        log.info(f"Pre-upgrade: {total} collections, index holds {indexed_before} docs, "
                 f"{hits_before} hits")
        if not indexed_before:
            self.fail(f"pre-upgrade index over {fts_collections} collections indexed 0 docs")

        self._rolling_upgrade_fts_nodes([fts_nodes[0]], label="Stage 1 (first FTS node)",
                                        driver=fts_callable, index=index)
        self._upgrade_rest_of_cluster([fts_nodes[0]], "Stage 3 (rest of cluster)",
                                      driver=fts_callable, index=index)

        log.info("=" * 20 + " Post-upgrade collection-scale checks")
        indexed_after = index.get_indexed_doc_count()
        if indexed_after < indexed_before:
            errors['doc_count'] = (f"carried index lost docs across the upgrade: "
                                   f"{indexed_before} -> {indexed_after}")

        hits_after, _, _, status = index.execute_query(query={"match_all": {}},
                                                       zero_results_ok=True)
        if hits_after == -1 or status == 'fail':
            errors['query'] = f"carried index could not be queried after upgrade: {status}"

        try:
            manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
                self.master, "default")
            scopes_after = len(manifest.get('scopes', []))
            log.info(f"Post-upgrade manifest holds {scopes_after} scopes")
            if scopes_after < num_scopes:
                errors['manifest'] = (f"scopes lost across the upgrade: expected at least "
                                      f"{num_scopes}, manifest has {scopes_after}")
        except Exception as err:
            errors['manifest_read'] = f"could not read the collection manifest: {err}"

        errors.update(self._totoro_post_upgrade_checks("collection scale upgrade"))
        if errors:
            self.fail("test_collections_scale_online_upgrade failed:\n" +
                      "\n".join(f"  {k}: {v}" for k, v in errors.items()))
        log.info("test_collections_scale_online_upgrade PASSED")

    # =========================================================================
    # =========================================================================

    def test_hierarchical_online_upgrade(self):
        """Online rolling upgrade with a hierarchical (nested) FTS index."""
        errors = {}
        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        if not fts_nodes or len(fts_nodes) < 2:
            self.fail("test_hierarchical_online_upgrade requires >= 2 FTS nodes")

        loader = SDKDataLoader(
            num_ops=self.num_items, percent_create=100,
            json_template=self.input.param("hierarchical_dataset", "hierarchical"),
            key_prefix=self.input.param("hierarchical_doc_prefix", "hier_"),
            scope="_default", collection="_default")
        for task in self.cb_cluster.async_load_all_buckets_from_generator(loader):
            task.result()

        index = self.cb_cluster.create_hierarchical_fts_index(
            name=self.HIERARCHICAL_INDEX, source_name="default")
        self.sleep(self.vector_index_build_wait, "letting the hierarchical index build")

        indexed_before = index.get_indexed_doc_count()
        hits_before, _, _, status = index.execute_query(query={"match_all": {}},
                                                        zero_results_ok=True)
        log.info(f"Pre-upgrade hierarchical index: {indexed_before} docs, {hits_before} hits")
        if not indexed_before:
            self.fail("pre-upgrade hierarchical index indexed 0 docs")

        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False,
                                   servers=self.servers)

        self._rolling_upgrade_fts_nodes([fts_nodes[0]], label="Stage 1 (first FTS node)",
                                        driver=fts_callable, index=index)
        self._upgrade_rest_of_cluster([fts_nodes[0]], "Stage 3 (rest of cluster)",
                                      driver=fts_callable, index=index)

        log.info("=" * 20 + " Post-upgrade hierarchical checks")
        indexed_after = index.get_indexed_doc_count()
        if indexed_after < indexed_before:
            errors['doc_count'] = (f"hierarchical index lost docs across the upgrade: "
                                   f"{indexed_before} -> {indexed_after}")

        hits_after, _, _, status = index.execute_query(query={"match_all": {}},
                                                       zero_results_ok=True)
        if hits_after == -1 or status == 'fail':
            errors['query'] = f"hierarchical index could not be queried after upgrade: {status}"
        elif hits_after < hits_before:
            errors['hits'] = (f"hierarchical query lost hits across the upgrade: "
                              f"{hits_before} -> {hits_after}")

        try:
            new_index = self.cb_cluster.create_hierarchical_fts_index(
                name="hier_post_idx", source_name="default")
            self.sleep(self.vector_index_build_wait, "letting the new hierarchical index build")
            validator = FTSPostChangeValidator(self, driver=fts_callable, label="post-upgrade")
            errors_new = validator.validate_index_end_to_end(new_index)
            if errors_new:
                errors['new_hierarchical_index'] = errors_new
        except Exception as err:
            errors['new_hierarchical_index'] = f"could not create one after the upgrade: {err}"
        finally:
            try:
                self.cb_cluster.delete_fts_index("hier_post_idx")
            except Exception as err:
                log.warning(f"could not clean up 'hier_post_idx': {err}")

        errors.update(self._totoro_post_upgrade_checks("hierarchical upgrade"))
        if errors:
            self.fail("test_hierarchical_online_upgrade failed:\n" +
                      "\n".join(f"  {k}: {v}" for k, v in errors.items()))
        log.info("test_hierarchical_online_upgrade PASSED")
