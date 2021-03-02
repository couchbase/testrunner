import logging
import json

from remote.remote_util import RemoteMachineShellConnection
from newupgradebasetest import NewUpgradeBaseTest
from .fts_callable import FTSCallable
from scripts.java_sdk_setup import JavaSdkSetup
from .fts_base import CouchbaseCluster
from .fts_base import FTSIndex
from membase.api.rest_client import RestConnection
from lib.collection.collections_cli_client import CollectionsCLI
from .fts_backup_restore import FTSIndexBackupClient
from security.rbac_base import RbacBase


log = logging.getLogger(__name__)


class UpgradeFTS(NewUpgradeBaseTest):

    def setUp(self):
        super(UpgradeFTS, self).setUp()

        self.initial_version = self.input.param('initial_version', '6.6.1-9213')
        self.upgrade_to = self.input.param("upgrade_to")
        self.cb_cluster = CouchbaseCluster("C1", self.servers, self.log)

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

    def create_index_es(self, index_name="es_index"):
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

        self.cluster.create_default_bucket(bucket_params)


    def _set_bleve_max_result_window(self):
        bmrw_value = 100000000
        for node in self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True):
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
        fts_callable.load_data(100)
        fts_query = {"query": "dept:Engineering"}

        pre_upgrade_idx = fts_callable.create_fts_index("pre_upgrade_idx", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=None, analyzer="standard",
                                                no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(100)
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

        self.assertEquals(len(post_upgrade_errors.keys()), 0,
                          f"The following post upgrade tests are failed: {post_upgrade_errors}")


    def test_online_upgrade(self):
        partial_upgrade_errors = {}
        full_fts_upgrade_errors = {}
        post_upgrade_errors = {}

        fts_nodes = self.get_nodes_from_services_map(service_type="fts", get_all_nodes=True)
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)

        if len(fts_nodes) < 2:
            log.error("Need to have more than one FTS nodes in cluster")
            self.fail()

        fts_callable = FTSCallable(self.servers, es_validate=False, es_reset=False)
        fts_callable.load_data(100)
        fts_query = {"query": "dept:Engineering"}

        pre_upgrade_idx = fts_callable.create_fts_index("pre_upgrade_idx", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=None, analyzer="standard",
                                                no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(100)
        pre_upgrade_hits, pre_upgrade_matches, _, pre_upgrade_status = pre_upgrade_idx.execute_query(query=fts_query)

        nodes_out = []
        nodes_out.append(fts_nodes[0])
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
        rebalance.result()
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
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 nodes_out, [],
                                                 services=services_in)
        rebalance.result()

        log.info("="*20 + " Starting partial fts upgrade tests")
        partial_upgrade_hits, partial_upgrade_matches, _, partial_upgrade_status = pre_upgrade_idx.execute_query(query=fts_query)

        if sorted(pre_upgrade_matches) != sorted(partial_upgrade_matches):
            errors = ["Pre-upgrade and partial-upgrade results of fts query do not match."]
            partial_upgrade_errors['test_partial_upgrade'] = errors
        errors = self._test_create_bucket_index()
        if len(errors) > 0:
            partial_upgrade_errors['_test_create_bucket_index'] = errors

        nodes_out.clear()
        nodes_out = fts_nodes[0:]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
        rebalance.result()
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
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 nodes_out, [],
                                                 services=services_in)
        rebalance.result()

        log.info("="*20 + " Starting partial upgrade tests")

        errors = self._test_create_bucket_index()
        if len(errors) > 0:
            if "No docs were indexed for index" not in str(errors[0]):
                full_fts_upgrade_errors['_test_create_bucket_index'] = errors
        errors = self._test_backup_restore()
        if len(errors) > 0:
            full_fts_upgrade_errors['_test_backup_restore'] = errors

        nodes_out.clear()
        nodes_out.append(kv_nodes[1])
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
        rebalance.result()
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

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 nodes_out, [],
                                                 services=['kv,index,n1ql'])
        rebalance.result()

        nodes_out.clear()
        nodes_out.append(kv_nodes[0])
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], nodes_out)
        rebalance.result()

        del self.servers[0]
        self.master = self.servers[1]

        upgrade_th = self._async_update(self.upgrade_to, nodes_out)
        for th in upgrade_th:
            th.join()
        log.info("==== Upgrade Complete ====")
        self.sleep(120)

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 nodes_out, [],
                                                 services=['kv,index,n1ql'])

        rebalance.result()

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
            fts_callable.load_data(100)
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
        fts_callable.wait_for_indexing_complete(100)

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
        fts_callable.load_data(100)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1",
                                                                  collection=["collection2", "collection3", "collection4"])

        fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                         source_name="default", index_type='fulltext-index',
                         index_params=None, plan_params=None,
                         source_params=None, source_uuid=None, collection_index=True, _type=_type, analyzer="standard",
                                                scope="scope1", collections=["collection2", "collection3", "collection4"],
                                                no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(300)

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
        fts_callable.load_data(100)

        fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=False,
                                                _type=None, analyzer="standard",
                                                no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(100)

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
            alias_def['targets'][index.name] = {}
            alias_def['targets'][index.name]['indexUUID'] = index.get_uuid()
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
                self.log.info("Index count: %s"
                                %index.get_indexed_doc_count())
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
        fts_callable.load_data(100)

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

        fts_callable.wait_for_indexing_complete(100)

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
        fts_callable.load_data(100)

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

        fts_callable.wait_for_indexing_complete(100)

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
        fts_callable.load_data(100)

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

        fts_callable.wait_for_indexing_complete(100)

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
        fts_callable.load_data(100)

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

        fts_callable.wait_for_indexing_complete(100)

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
        fts_callable.load_data(100)

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

        fts_callable.wait_for_indexing_complete(100)

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
        fts_callable.load_data(100)

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

        fts_callable.wait_for_indexing_complete(100)

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
        fts_callable.load_data(100)

        _type = self.__define_index_parameters_collection_related(container_type="collection", scope="scope1", collection="collection25")

        fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                source_name="default", index_type='fulltext-index',
                                                index_params=None, plan_params=None,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard", scope="scope1",
                                                collections=["collection25"], no_check=False, cluster=self.cb_cluster)
        fts_callable.wait_for_indexing_complete(100)
        rest = RestConnection(fts_node)
        fts_port = fts_node.fts_port or self.fts_port
        status, content = rest.get_rest_endpoint_data(endpoint, ip=fts_node.ip, port=fts_port)
        if not status:
            errors.append(f"Endpoint {endpoint} is not accessible.")

        fts_callable.delete_fts_index("idx1")
        fts_callable.flush_buckets(["default"])
        return errors

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
