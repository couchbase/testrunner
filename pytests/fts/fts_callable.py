import logger
import time
import copy
import json
from .fts_base import FTSIndex, CouchbaseCluster
from lib.membase.api.exception import FTSException
from .es_base import ElasticSearchBase
from TestInput import TestInputSingleton
from lib.couchbase_helper.documentgenerator import JsonDocGenerator
from lib.couchbase_helper.documentgenerator import SDKDataLoader
from lib.membase.api.rest_client import RestConnection
from .random_query_generator.rand_query_gen import FTSESQueryGenerator
from lib.Cb_constants.CBServer import CbServer
from lib.collection.collections_cli_client import CollectionsCLI
from scripts.java_sdk_setup import JavaSdkSetup
import json
from pathlib import Path
from pytests.fts.vector_dataset_generator.vector_dataset_loader import GoVectorLoader, VectorLoader
from pytests.fts.vector_dataset_generator.vector_dataset_generator import VectorDataset
import struct
import base64
from pytests.fts.fts_base import QUERY


class FTSCallable:
    """
    The FTS class to call when trying to anything with FTS
    outside the purview of FTSBaseTest class

    Sample usage:

        from pytests.fts.fts_callable import FTSCallable
        fts_obj = FTSCallable(nodes= self.servers, es_validate=True)
        for bucket in self.buckets:
            fts_obj.create_default_index(
                index_name="index_{0}".format(bucket.name),
                bucket_name=bucket.name)
        fts_obj.load_data(self.num_items)
        fts_obj.wait_for_indexing_complete()
        for index in fts_obj.fts_indexes:
            fts_obj.run_query_and_compare(index)
        fts_obj.delete_all()
    """

    def __init__(self, nodes, es_validate=False, es_reset=True, scope=None, collections=None, collection_index=False, is_elixir=False, reduce_query_logging=False,
                 variable_node=None, xattr_flag=False, base64_flag=False,servers = None):
        self.log = logger.Logger.get_logger()
        self.cb_cluster = CouchbaseCluster(name="C1", nodes= nodes, log=self.log, reduce_query_logging=reduce_query_logging)
        self.cb_cluster.get_buckets()
        self.fts_indexes = self.cb_cluster.get_indexes()
        """ have to have a elastic search node to run these tests """
        self.elastic_node = TestInputSingleton.input.elastic
        self.compare_es = es_validate
        self.es = None
        self.is_elixir = is_elixir
        self._num_items = 10000
        self.query_types = ["match", "bool", "match_phrase",
                            "prefix", "fuzzy", "conjunction", "disjunction",
                            "wildcard", "regexp", "query_string",
                            "numeric_range", "date_range"]
        self.scope = scope
        self.collections = collections
        self.collection_index = collection_index
        self.create_gen = None
        if self.compare_es and not self.elastic_node:
            raise "For ES result validation, pls add elastic search node in the .ini file."
        elif self.compare_es:
            self.es = ElasticSearchBase(self.elastic_node, self.log)
            if es_reset:
                self.es.delete_index("es_index")
                self.es.create_empty_index_with_bleve_equivalent_std_analyzer("es_index")
                self.log.info("Created empty index \'es_index\' on Elastic Search node with "
                              "custom standard analyzer(default)")

        self.cli_client = None
        self.num_custom_analyzers = TestInputSingleton.input.param("num_custom_analyzers", 0)
        self._update = TestInputSingleton.input.param("update", False)
        self._delete = TestInputSingleton.input.param("delete", False)
        self.upd_del_fields = TestInputSingleton.input.param("upd_del_fields", None)
        self._expires = TestInputSingleton.input.param("expires", 0)
        self.dataset = TestInputSingleton.input.param("dataset", "emp")
        self.sample_query = {"match": "Safiya Morgan", "field": "name"}
        self.run_via_n1ql = False
        self.reduce_query_logging = reduce_query_logging


        self.variable_node = variable_node
        self.store_in_xattr = xattr_flag
        self.encode_base64_vector = base64_flag
        file_path = Path('b/resources/fts/vector_index_def.json')
        self.vector_index_definition = json.loads(file_path.read_text())
        self.expected_accuracy_and_recall = TestInputSingleton.input.param("expected_accuracy_and_recall", 70)
        self.vector_field_type = "vector_base64" if self.encode_base64_vector else "vector"
        if self.encode_base64_vector:
            if self.store_in_xattr:
                self.vector_field_name = "vector_encoded"
            else:
                self.vector_field_name = "vector_data_base64"
        else:
            self.vector_field_name = "vector_data"

        self.k = TestInputSingleton.input.param("k", 2)
        self.query = {"query": {"match_none": {}}, "explain": True, "fields": ["*"],
                      "knn": [{"field": "vector_data", "k": self.k,
                               "vector": []}]}
        self.index_obj = {"name": "index_default"}
        self.vector_dataset = TestInputSingleton.input.param("vector_dataset", "siftsmall")
        self.vector_queries_count = TestInputSingleton.input.param("vector_queries_count", 5)
        self.count = 0
        self.run_n1ql_search_function = TestInputSingleton.input.param("run_n1ql_search_function", True)
        self.servers = servers
        self.inbetween_active = False
        self.inbetween_tests = 0
        self.vector_flag = TestInputSingleton.input.param("vector_search_test", False)
        self.skip_validation_if_no_query_hits = TestInputSingleton.input.param("skip_validation_if_no_query_hits", True)


    def __create_buckets(self):
        self.log.info("__create_buckets() is not implemented yet.")
        pass

    def init_kv(self):
        self.__create_buckets()
        if self.collection_index:
            for bucket in self.cb_cluster.get_buckets():
                if type(self.collections) is list:
                    for c in self.collections:
                        self.cb_cluster._create_collection(bucket=bucket.name, scope=self.scope, collection=c,
                                                           cli_client=self.cli_client)
                else:
                    self.cb_cluster._create_collection(bucket=bucket.name, scope=self.scope,
                                                       collection=self.collections, cli_client=self.cli_client)

    def create_fts_index(self, name, source_type='couchbase',
                         source_name=None, index_type='fulltext-index',
                         index_params=None, plan_params=None,
                         source_params=None, source_uuid=None, collection_index=False, _type=None, analyzer="standard",
                         scope=None, collections=None, no_check=False, cluster=None, specify_fields=False, FTSIndex=None):
        """Create fts index/alias
        @param node: Node on which index is created
        @param name: name of the index/alias
        @param source_type : 'couchbase' or 'files'
        @param source_name : name of couchbase bucket or "" for alias
        @param index_type : 'fulltext-index' or 'fulltext-alias'
        @param index_params :  to specify advanced index mapping;
                                dictionary overriding params in
                                INDEX_DEFAULTS.BLEVE_MAPPING or
                                INDEX_DEFAULTS.ALIAS_DEFINITION depending on
                                index_type
        @param plan_params : dictionary overriding params defined in
                                INDEX_DEFAULTS.PLAN_PARAMS
        @param source_params: dictionary overriding params defined in
                                INDEX_DEFAULTS.SOURCE_CB_PARAMS or
                                INDEX_DEFAULTS.SOURCE_FILE_PARAMS
        @param source_uuid: UUID of the source, may not be used
        @param collection_index: is collection index
        @param type: type mapping for collection index
        @analyzer: index analyzer
        """

        index = FTSIndex
        if index is None:
            index = self.generate_FTSIndex_info(name, source_type=source_type,
                                            source_name=source_name, index_type=index_type,
                                            index_params=index_params, plan_params=plan_params,
                                            source_params=source_params, source_uuid=source_uuid, collection_index=collection_index, _type=_type,
                                            scope=scope, collections=collections, cluster=cluster)
        else:
            index.set_cluster(self.cb_cluster)

        if collection_index:
            if not index.custom_map and not specify_fields:
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
        if specify_fields:
            index.index_definition['params']['mapping']['default_type'] = scope
            index.index_definition['params']['mapping']['default_mapping']['enabled'] = False

        if no_check:
            index.create_no_check()
        else:
            index.create()
        self.cb_cluster.get_indexes().append(index)
        return index

    def generate_FTSIndex_info(self, name, source_type='couchbase',
                           source_name=None, index_type='fulltext-index',
                           index_params=None, plan_params=None,
                           source_params=None, source_uuid=None, collection_index=False, _type=None,
                           scope=None, collections=None, cluster=None):

        if not cluster:
            cluster = self.cb_cluster

        index = FTSIndex(
            cluster=cluster,
            name=name,
            source_type=source_type,
            source_name=source_name,
            index_type=index_type,
            index_params=index_params,
            plan_params=plan_params,
            source_params=source_params,
            source_uuid=source_uuid,
            type_mapping=_type,
            collection_index=collection_index,
            scope=scope,
            collections=collections,
            is_elixir=self.is_elixir,
            reduce_query_logging=self.reduce_query_logging
        )

        return index

    def create_default_index(self, index_name, bucket_name, analyzer='standard',
                             cluster=None, collection_index=False, scope=None, collections=None, plan_params=None):

        """Create fts index
        @param index_name: name of the index/alias
        @param bucket_name : name of couchbase bucket

        @param type_mapping: UUID of the source, may not be used
        @param analyzer: index analyzer
        """
        types_mapping = self.__define_index_types_mapping(collection_index=collection_index, scope=scope,
                                                          collections=collections)
        if not cluster:
            cluster = self.cb_cluster


        index = FTSIndex(
            cluster=cluster,
            name=index_name,
            source_name=bucket_name,
            type_mapping=types_mapping,
            collection_index=collection_index,
            scope=scope,
            collections=collections,
            plan_params=plan_params
        )



        rest = RestConnection(self.cb_cluster.get_random_fts_node())
        index.create(rest)

        if collection_index:
            if not index.custom_map:
                if type(types_mapping) is list:
                    for typ in types_mapping:
                        index.add_type_mapping_to_index_definition(type=typ, analyzer=analyzer)
                else:
                    index.add_type_mapping_to_index_definition(type=types_mapping, analyzer=analyzer)

            doc_config = dict()

            doc_config['mode'] = 'scope.collection.type_field'
            doc_config['type_field'] = "type"
            index.index_definition['params']['doc_config'] = {}
            index.index_definition['params']['doc_config'] = doc_config


        return index

    def wait_for_indexing_complete(self, item_count=None, complete_wait=True, idx=None):
        """
            Wait for index_count for any index to stabilize or reach the
            index count specified by item_count
        """
        found = False
        retry = TestInputSingleton.input.param("index_retry", 20)
        for index in self.cb_cluster.get_indexes():
            if idx is not None and index.name != idx.name or index.index_type == "alias" or index.index_type == "fulltext-alias":
                continue
            found = True
            retry_count = retry
            prev_count = 0
            es_index_count = 0
            while retry_count > 0:
                try:
                    index_doc_count = index.get_indexed_doc_count()
                    if CbServer.capella_run:
                        bucket_doc_count = item_count
                    else:
                        bucket_doc_count = index.get_src_bucket_doc_count()
                    if not self.es:
                        self.log.info("Docs in bucket = %s, docs in FTS index '%s': %s"
                                      % (bucket_doc_count,
                                         index.name,
                                         index_doc_count))
                        if not complete_wait and bucket_doc_count > 0 and index_doc_count > 0:
                            break
                    else:
                        self.es.update_index('es_index')
                        es_index_count = self.es.get_index_count('es_index')
                        self.log.info("Docs in bucket = %s, docs in FTS index '%s':"
                                      " %s, docs in ES index: %s "
                                      % (bucket_doc_count,
                                         index.name,
                                         index_doc_count,
                                         es_index_count))
                    if bucket_doc_count == 0:
                        if item_count and item_count != 0:
                            self.sleep(5,
                                       "looks like docs haven't been loaded yet...")
                            retry_count -= 1
                            continue

                    if item_count and index_doc_count > item_count:
                        break

                    if bucket_doc_count == index_doc_count:
                        if self.es:
                            if bucket_doc_count == es_index_count:
                                break
                        else:
                            break

                    if prev_count < index_doc_count or prev_count > index_doc_count:
                        prev_count = index_doc_count
                        retry_count = retry
                    else:
                        retry_count -= 1
                except Exception as e:
                    self.log.info(e)
                    retry_count -= 1
                time.sleep(6)
            # now wait for num_mutations_to_index to become zero to handle the pure
            # updates scenario - where doc count remains unchanged
            retry_mut_count = 20
            if item_count == None:
                while True and retry_count:
                    num_mutations_to_index = index.get_num_mutations_to_index()
                    if num_mutations_to_index > 0:
                        self.sleep(5, "num_mutations_to_index: {0} > 0".format(num_mutations_to_index))
                        retry_mut_count -= 1
                    else:
                        break
        if not found:
            self.log.critical(f"Couldn't wait for indexing since index {idx.name} was not found")

    def create_alias(self, target_indexes, name=None, alias_def=None, bucket=None, scope=None):
        """
            Creates an alias spanning one or many target indexes
        """
        if not name:
            name = 'alias_%s' % int(time.time())

        if not alias_def:
            alias_def = {"targets": {}}
            for index in target_indexes:
                alias_def['targets'][index.name] = {}

        return self.cb_cluster.create_fts_index(name=name,
                                                index_type='fulltext-alias',
                                                index_params=alias_def,
                                                source_name=bucket,
                                                scope=scope)

    def delete_fts_index(self, name):
        """ Delete an FTSIndex object with the given name from a given node """
        for index in self.fts_indexes:
            if index.name == name:
                index.delete()

    def delete_all(self):
        """ Deletes all fts and es indexes if any"""
        for index in self.fts_indexes:
            index.delete()
        if self.es:
            self.es.delete_indices()

    def flush_buckets(self, buckets=[]):
        self.cb_cluster.flush_buckets(buckets)

    def delete_bucket(self, bucket_name):
        self.cb_cluster.delete_bucket(bucket_name)

    def async_load_data(self):
        """ Loads data into CB and ES"""
        load_tasks = []
        self.__populate_create_gen()
        if self.es:
            if not self.collection_index:
                gen = copy.deepcopy(self.create_gen)
                if isinstance(gen, list):
                    for generator in gen:
                        load_tasks.append(self.es.async_bulk_load_ES(index_name='es_index',
                                                                     gen=generator,
                                                                     op_type='create'))
                else:
                    load_tasks.append(self.es.async_bulk_load_ES(index_name='es_index',
                                                                 gen=gen,
                                                                 op_type='create'))
        load_tasks += self.cb_cluster.async_load_all_buckets_from_generator(
            self.create_gen)
        return load_tasks

    def load_data(self, num_items):
        """ A blocking load call, takes num_items as items to load"""
        self._num_items = num_items
        for task in self.async_load_data():
            task.result()

    def run_query_and_compare(self, index, num_queries=20, es_index_name="es_index"):
        """
        Runs every fts query and es_query and compares them as a single task
        Runs as many tasks as there are queries
        """
        tasks = []
        fail_count = 0
        failed_queries = []
        self.__generate_random_queries(index, num_queries, query_type=self.query_types)
        for count in range(0, len(index.fts_queries)):
            tasks.append(self.cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=es_index_name,
                query_index=count,
                use_collections=self.collection_index,
                variable_node=self.variable_node))
        num_queries = len(tasks)

        for task in tasks:
            task.result()
            if not task.passed:
                fail_count += 1
                failed_queries.append(task.query_index + 1)

        if fail_count:
            raise Exception("%s out of %s queries failed! - %s" % (fail_count,
                                                                   num_queries,
                                                                   failed_queries))
        elif not fail_count and not self.reduce_query_logging:
            self.log.info("SUCCESS: %s out of %s queries passed"
                          % (num_queries - fail_count, num_queries))

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def async_perform_update_delete(self):
        """
          Call this method to perform updates/deletes on your cluster.
          It checks if update=True or delete=True params were passed in
          the test.
          @param fields_to_update - list of fields to update in JSON
        """
        load_tasks = []
        updates = TestInputSingleton.input.param("updates", False)
        deletes = TestInputSingleton.input.param("deletes", False)
        expires = TestInputSingleton.input.param("expires", False)

        # UPDATES
        if updates:
            self.log.info("Updating keys @ {0} with expiry={1}".
                          format(self.cb_cluster.get_name(), expires))
            self.__populate_update_gen()
            if self.compare_es:
                if not self.collection_index:
                    gen = copy.deepcopy(self.update_gen)
                    if not expires:
                        if isinstance(gen, list):
                            for generator in gen:
                                load_tasks.append(self.es.async_bulk_load_ES(
                                    index_name='es_index',
                                    gen=generator,
                                    op_type="update"))
                        else:
                            load_tasks.append(self.es.async_bulk_load_ES(
                                index_name='es_index',
                                gen=gen,
                                op_type="update"))
                    else:
                        # an expire on CB translates to delete on ES
                        if isinstance(gen, list):
                            for generator in gen:
                                load_tasks.append(self.es.async_bulk_load_ES(
                                    index_name='es_index',
                                    gen=generator,
                                    op_type="delete"))
                        else:
                            load_tasks.append(self.es.async_bulk_load_ES(
                                index_name='es_index',
                                gen=gen,
                                op_type="delete"))

            load_tasks += self.cb_cluster.async_load_all_buckets_from_generator(
                kv_gen=self.update_gen,
                ops="update",
                exp=expires)

        [task.result() for task in load_tasks]
        if load_tasks:
            self.log.info("Batched updates loaded to cluster(s)")

        load_tasks = []
        # DELETES
        if deletes:
            self.log.info("Deleting keys @ {0}".format(self.cb_cluster.get_name()))
            self.__populate_delete_gen()
            if self.compare_es:
                if not self.collection_index:
                    del_gen = copy.deepcopy(self.delete_gen)
                    if isinstance(del_gen, list):
                        for generator in del_gen:
                            load_tasks.append(self.es.async_bulk_load_ES(
                                index_name='es_index',
                                gen=generator,
                                op_type="delete"))
                    else:
                        load_tasks.append(self.es.async_bulk_load_ES(
                            index_name='es_index',
                            gen=del_gen,
                            op_type="delete"))
            load_tasks += self.cb_cluster.async_load_all_buckets_from_generator(
                self.delete_gen, "delete")

        [task.result() for task in load_tasks]
        if load_tasks:
            self.log.info("Batched deletes sent to cluster(s)")

        if expires:
            self.sleep(
                expires,
                "Waiting for expiration of updated items")
            self.cb_cluster.run_expiry_pager()


    def __generate_random_queries(self, index, num_queries=1,
                                  query_type=None, seed=0):
        """
         Calls FTS-ES Query Generator for employee dataset
         @param num_queries: number of queries to return
         @query_type: a list of different types of queries to generate
                      like: query_type=["match", "match_phrase","bool",
                                        "conjunction", "disjunction"]
        """
        query_gen = FTSESQueryGenerator(num_queries, query_type=query_type,
                                        seed=seed, dataset="emp",
                                        fields=index.smart_query_fields)
        for fts_query in query_gen.fts_queries:
            index.fts_queries.append(
                json.loads(json.dumps(fts_query, ensure_ascii=False)))

        if self.compare_es:
            for es_query in query_gen.es_queries:
                # unlike fts, es queries are not nested before sending to fts
                # so enclose in query dict here
                es_query = {'query': es_query}
                self.es.es_queries.append(
                    json.loads(json.dumps(es_query, ensure_ascii=False)))
            return index.fts_queries, self.es.es_queries

        return index.fts_queries

    def __populate_create_gen(self):
        start = 0
        if not self.collection_index:
            self.create_gen = JsonDocGenerator(name="emp",
                                               encoding="utf-8",
                                               start=start,
                                               end=start + self._num_items)
        else:
            elastic_ip = None
            elastic_port = None
            elastic_username = None
            elastic_password = None
            if self.compare_es:
                elastic_ip = self.elastic_node.ip
                elastic_port = self.elastic_node.port
                elastic_username = self.elastic_node.es_username
                elastic_password = self.elastic_node.es_password
            self.create_gen = SDKDataLoader(num_ops=self._num_items, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=self.scope,
                                            collection=self.collections,
                                            json_template="emp",
                                            start=start, end=start + self._num_items,
                                            es_compare=self.compare_es, es_host=elastic_ip, es_port=elastic_port,
                                            es_login=elastic_username, es_password=elastic_password, key_prefix="emp_",
                                            upd_del_shift=self._num_items
                                            )

    def __populate_update_gen(self):
        self.update_gen = copy.deepcopy(self.create_gen)
        self.update_gen.start = 0
        self.update_gen.end = int(self.create_gen.end *
                                  (float)(30) / 100)
        self.update_gen.update()

    def __populate_delete_gen(self):
        if self.collection_index:
            self.delete_gen = copy.deepcopy(self.create_gen)
            self.delete_gen.op_type = "delete"
            self.delete_gen.encoding = "utf-8"
            self.delete_gen.start = int((self.create_gen.end)
                                        * (float)(30 / 100))
            self.delete_gen.end = self.create_gen.end
            self.delete_gen.delete()
        else:
            self.delete_gen = JsonDocGenerator(
                self.create_gen.name,
                op_type="delete",
                encoding="utf-8",
                start=int((self.create_gen.end)
                          * (float)(30 / 100)),
                end=self.create_gen.end)

    def __define_index_types_mapping(self, collection_index=False, scope=None, collections=None):
        """Defines types mapping for FTS index
        """
        if not collection_index:
            _type = None
        else:
            _type = []
            for c in collections:
                _type.append(f"{scope}.{c}")
        return _type

    def create_es_index_mapping(self, es_mapping, fts_mapping=None):
        if not (self.num_custom_analyzers > 0):
            self.es.create_index_mapping(index_name="es_index",
                                         es_mapping=es_mapping, fts_mapping=None)
        else:
            self.es.create_index_mapping(index_name="es_index",
                                         es_mapping=es_mapping, fts_mapping=fts_mapping)

    def check_if_index_exists(self, index_name, bucket_name=None, scope_name=None, index_def=False, node_def=False):
        """
            Returns true / false if index exists
            Returns index's node and server group distribution if required -> node_def = True
            Returns index definition if required -> index_def = True
        """
        rest = RestConnection(self.cb_cluster.get_random_fts_node())
        resp = rest.get_fts_index_definition(index_name, bucket_name, scope_name)
        if resp[0]:
            if node_def:
                return resp[1]['planPIndexes'][0]['nodes']
            if index_def:
                return resp[1]['indexDef']
        return resp[0]

    def get_fts_rebalance_status(self, fts_node=None):
        rest = RestConnection(self.cb_cluster.get_random_fts_node())
        resp = rest._rebalance_progress_status(fts_node)
        print("Rebalance Status : ", resp)
        return resp

    def get_fts_autoscaling_stats(self, nodes, creds):
        rest = RestConnection(self.cb_cluster.get_random_fts_node())
        stats = []
        cfg_stats = None
        node_achiever = None
        num_attempts = 1
        for node in nodes:
            attempts = 0
            stat_found = False
            stat = {"limits:billableUnitsRate": 0,
                    "limits:diskBytes": 0,
                    "limits:memoryBytes": 0,
                    "resourceUnderUtilizationWaterMark": 0,
                    "resourceUtilizationHighWaterMark": 0,
                    "resourceUtilizationLowWaterMark": 0,
                    "utilization:billableUnitsRate": 0,
                    "utilization:cpuPercent": 0,
                    "utilization:diskBytes": 0,
                    "utilization:memoryBytes": 0
                    }
            while attempts < num_attempts:
                attempts += 1
                if attempts > 1:
                    self.log.info(f"Retrying {attempts} time for node {node}")
                resp = rest.get_specific_nsstats(node, creds)
                if type(resp) != dict:
                    resp = resp.json()
                if 'utilization:memoryBytes' in resp and 'utilization:cpuPercent' in resp:
                    stat['limits:billableUnitsRate'] = resp['limits:billableUnitsRate']
                    stat['limits:diskBytes'] = resp['limits:diskBytes']
                    stat['limits:memoryBytes'] = resp['limits:memoryBytes']
                    stat['resourceUnderUtilizationWaterMark'] = resp['resourceUnderUtilizationWaterMark']
                    stat['resourceUtilizationHighWaterMark'] = resp['resourceUtilizationHighWaterMark']
                    stat['resourceUtilizationLowWaterMark'] = resp['resourceUtilizationLowWaterMark']
                    stat['utilization:billableUnitsRate'] = resp['utilization:billableUnitsRate']
                    stat['utilization:cpuPercent'] = resp['utilization:cpuPercent']
                    stat['utilization:diskBytes'] = resp['utilization:diskBytes']
                    stat["utilization:memoryBytes"] = resp["utilization:memoryBytes"]
                    stats.append(resp)
                    stat_found = True
                    node_achiever = node
                    break
                else:
                    self.log.info(f"Stats not returned for node : {node}")
                    if cfg_stats is None:
                        if node_achiever is not None:
                            cfg_stats = rest.get_fts_cfg_stats(node_achiever, creds)
                        else:
                            cfg_stats = rest.get_fts_cfg_stats(node, creds)
                        if cfg_stats is not None:
                            cfg_stats = cfg_stats.json()
            if not stat_found:
                stats.append(stat)
        return stats, cfg_stats

    def get_fts_defrag_stats(self, node, creds):
        rest = RestConnection(self.cb_cluster.get_random_fts_node())
        return rest.get_fts_defrag_output(node, creds).json()

    def create_vector_index(self, xattr_flag, base64_flag, index_name, similarity="l2_norm", dimensions=128):

        self.store_in_xattr = xattr_flag
        self.encode_base64_vector = base64_flag

        index_body = copy.deepcopy(self.vector_index_definition)
        index_body["name"] = index_name
        index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"]["fields"][0][
            "dims"] = dimensions
        index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"]["fields"][0][
            "similarity"] = similarity

        if self.encode_base64_vector:
            if self.store_in_xattr:
                index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"][
                    "fields"][0]['name'] = "vector_encoded"
            else:
                index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"][
                    "fields"][0]['name'] = "vector_data_base64"

            index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"]["fields"][0][
                'type'] = "vector_base64"

            vector_temp = index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"]
            index_body['params']['mapping']['types']['_default._default']['properties'] = {}
            if self.store_in_xattr:
                index_body['params']['mapping']['types']['_default._default']['properties'][
                    'vector_encoded'] = vector_temp
            else:
                index_body['params']['mapping']['types']['_default._default']['properties'][
                    'vector_data_base64'] = vector_temp

        if self.store_in_xattr:
            vector_temp = index_body['params']['mapping']['types']['_default._default']['properties']
            index_body['params']['mapping']['types']['_default._default']['properties'] = {}
            index_body['params']['mapping']['types']['_default._default']['properties']['_$xattrs'] = {
                "enabled": True,
                "dynamic": False,
                "properties": vector_temp}

        try:
            status, result = RestConnection(self.servers[1]).create_fts_index(index_name, index_body, mode="upgrade")
            if status:
                time.sleep(50)
                return str(result), 200
            else:
                return str(result), 100
        except Exception as e:
            print(e)
            return str(e), 100

    def update_vector_index(self, xattr_flag, base64_flag, index_name, similarity="l2_norm", dimensions=128):

        self.store_in_xattr = xattr_flag
        self.encode_base64_vector = base64_flag

        uuid = ""
        try:
            uuid = RestConnection(self.servers[1]).get_fts_index_uuid(index_name, bucket="default")
        except Exception as e:
            print(e)


        index_body = copy.deepcopy(self.vector_index_definition)
        index_body["name"] = index_name
        index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"]["fields"][0][
            "dims"] = dimensions
        index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"]["fields"][0][
            "similarity"] = similarity
        index_body["uuid"] = uuid

        if self.encode_base64_vector:
            if self.store_in_xattr:
                index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"][
                    "fields"][0]['name'] = "vector_encoded"
            else:
                index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"][
                    "fields"][0]['name'] = "vector_data_base64"

            index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"]["fields"][0][
                'type'] = "vector_base64"

            vector_temp = index_body["params"]["mapping"]["types"]["_default._default"]["properties"]["vector_data"]
            index_body['params']['mapping']['types']['_default._default']['properties'] = {}
            if self.store_in_xattr:
                index_body['params']['mapping']['types']['_default._default']['properties'][
                    'vector_encoded'] = vector_temp
            else:
                index_body['params']['mapping']['types']['_default._default']['properties'][
                    'vector_data_base64'] = vector_temp

        if self.store_in_xattr:
            vector_temp = index_body['params']['mapping']['types']['_default._default']['properties']
            index_body['params']['mapping']['types']['_default._default']['properties'] = {}
            index_body['params']['mapping']['types']['_default._default']['properties']['_$xattrs'] = {
                "enabled": True,
                "dynamic": False,
                "properties": vector_temp}

        try:
            status, result = RestConnection(self.servers[1]).update_fts_index(index_name, index_body, mode="upgrade")
            if status:
                time.sleep(50)
                return str(result), 200
            else:
                return str(result), 100
        except Exception as e:
            print(e)
            return str(e), 100

    def push_vector_data(self, node, username, password, bucket="default", scope="_default", collection="_default",
                         vector_dataset="siftsmall", xattr=False, prefix="emp", start_index=10000000,
                         end_index= 10005001, base64Flag=False):

        goloader_object = GoVectorLoader(node, username,password, bucket,scope,collection,vector_dataset,xattr,prefix,
                                         start_index,end_index,base64Flag)
        goloader_object.load_data("upgrade")

    def get_query_vectors(self, dataset_name, dimension=None):
        ds = VectorDataset(dataset_name)
        use_hdf5_datasets = True
        if ds.dataset_name in ds.supported_sift_datasets:
            use_hdf5_datasets = False
        ds.extract_vectors_from_file(use_hdf5_datasets=use_hdf5_datasets, type_of_vec="query")

        if dimension:
            import numpy as np
            ds.query_vecs = list(ds.query_vecs)

            for index in range(len(ds.query_vecs)):
                vector = ds.query_vecs[index]
                current_dim = len(vector)

                # Resize the vector to the desired dimension
                if current_dim < dimension:
                    # If the current dimension is less than the desired dimension, repeat the values
                    repeat_values = dimension - current_dim
                    repeated_values = np.tile(vector, ((dimension + current_dim - 1) // current_dim))
                    ds.query_vecs[index] = repeated_values[:dimension]
                elif current_dim > dimension:
                    # If the current dimension is greater than the desired dimension, truncate the vector
                    ds.query_vecs[index] = vector[:dimension]

        print(f"First Query vector:{str(ds.query_vecs[0])}")
        print(f"Length of first query vector: {len(ds.query_vecs[0])}")

        return ds.query_vecs
    def get_groundtruth_file(self, dataset_name):
        ds = VectorDataset(dataset_name)
        use_hdf5_datasets = True
        if ds.dataset_name in ds.supported_sift_datasets:
            use_hdf5_datasets = False
        ds.extract_vectors_from_file(use_hdf5_datasets=use_hdf5_datasets, type_of_vec="groundtruth")
        print(f"First groundtruth vector:{str(ds.neighbors_vecs[0])}")
        return ds.neighbors_vecs

    def floats_to_little_endian_bytes(self, floats):
        byte_array = bytearray()
        for num in floats:
            float_bytes = struct.pack('<f', num)
            byte_array.extend(float_bytes)

        return byte_array

    def get_base64_encoding(self, array):
        byte_array = self.floats_to_little_endian_bytes(array)
        base64_string = base64.b64encode(byte_array).decode('ascii')
        return base64_string

    def run_n1ql_query_upgrade(self, query="", node=None, timeout=70, verbose=True):
        res = RestConnection(node).query_tool(query, timeout=timeout, verbose=verbose)
        return res

    def construct_cbft_query_json_upgrade(self, index, query, fields=None, timeout=60000,
                                          facets=False,
                                          sort_fields=None,
                                          explain=False,
                                          show_results_from_item=0,
                                          highlight=False,
                                          highlight_style=None,
                                          highlight_fields=None,
                                          consistency_level='',
                                          consistency_vectors={},
                                          score='',
                                          knn=None):

        max_matches = 10000000
        max_limit_matches = 100

        query_json = copy.deepcopy(QUERY.JSON)

        # query is a unicode dict
        query_type = "match_none"
        query_json['query'][query_type] = {}
        query_json['knn'] = [query]

        query_json['indexName'] = str(self.index_obj['name'])
        query_json['explain'] = explain

        query_json['size'] = int(max_matches)

        if max_limit_matches is not None:
            query_json['limit'] = int(max_limit_matches)
        if show_results_from_item:
            query_json['from'] = int(show_results_from_item)
        if timeout is not None:
            query_json['ctl']['timeout'] = int(timeout)
        if fields:
            query_json['fields'] = fields
        if facets:
            query_json['facets'] = self.construct_facets_definition()
        if sort_fields:
            query_json['sort'] = sort_fields
        if highlight:
            query_json['highlight'] = {}
            if highlight_style:
                query_json['highlight']['style'] = highlight_style
            if highlight_fields:
                query_json['highlight']['fields'] = highlight_fields
        if consistency_level is None:
            del query_json['ctl']['consistency']['level']
        else:
            query_json['ctl']['consistency']['level'] = consistency_level
        if consistency_vectors is None:
            del query_json['ctl']['consistency']['vectors']
        elif consistency_vectors != {}:
            query_json['ctl']['consistency']['vectors'] = consistency_vectors
        if score != '':
            query_json['score'] = "none"
        if knn is not None:
            query_json['knn'] = knn
        return query_json

    def compare_results(self, listA, listB, nameA="listA", nameB="listB"):
        common_elements = set(listA) & set(listB)
        not_common_elements = set(listA) ^ set(listB)
        self.log.info(f"Elements not common in both lists: {not_common_elements}")
        percentage_exist = (len(common_elements) / len(listB)) * 100
        self.log.info(f"Percentage of elements in {nameB} that exist in {nameA}: {percentage_exist:.2f}%")
        accuracy = 0
        if listA[0] == listB[0]:
            accuracy = 1
        return accuracy, percentage_exist

    def run_fts_query(self, index_name, query_dict, bucket_name=None, scope_name=None, node=None, timeout=100,
                      rest=None):
        if not node:
            node = self.servers[0]
        if not rest:
            rest = RestConnection(node)
        try:
            total_hits, hit_list, time_taken, status = \
                rest.run_fts_query(index_name, query_dict, timeout=timeout, bucket=bucket_name, scope=scope_name)

            return total_hits, hit_list, time_taken, status
        except Exception as e:
            print(e)
            return -1, -1, -1, -1

    def execute_query_upgrade(self, query, index, zero_results_ok=True, expected_hits=None,
                              return_raw_hits=False, sort_fields=None,
                              explain=False, show_results_from_item=0, highlight=False,
                              highlight_style=None, highlight_fields=None, consistency_level='',
                              consistency_vectors={}, timeout=60000, rest=None, score='', expected_no_of_results=None,
                              node=None, knn=None, fields=None):

        vector_search = False
        if self.vector_flag:
            vector_search = True

        query_dict = self.construct_cbft_query_json_upgrade(query,
                                                            index,
                                                            fields=fields,
                                                            sort_fields=sort_fields,
                                                            explain=explain,
                                                            show_results_from_item=show_results_from_item,
                                                            highlight=highlight,
                                                            highlight_style=highlight_style,
                                                            highlight_fields=highlight_fields,
                                                            consistency_level=consistency_level,
                                                            consistency_vectors=consistency_vectors,
                                                            timeout=timeout,
                                                            score=score,
                                                            knn=knn)

        hits = -1
        matches = []
        doc_ids = []
        time_taken = 0
        status = {}

        try:
            if timeout == 0:
                # force limit in 10 min in case timeout=0(no timeout)
                rest_timeout = 600
            else:
                rest_timeout = timeout // 1000 + 10
            hits, matches, time_taken, status = \
                self.run_fts_query(str(index['name']), query_dict, scope_name="_default",
                                   bucket_name="default", node=node, timeout=rest_timeout, rest=rest)
        except Exception as e:
            print(e)

        if status == 'fail':
            return hits, matches, time_taken, status
        if hits:
            for doc in matches:
                doc_ids.append(doc['id'])
        if int(hits) == 0 and not zero_results_ok:
            print("ERROR: 0 hits returned!")
            raise FTSException(f"No docs returned for query : {query_dict}")
        if expected_hits and expected_hits != hits:
            print(f"ERROR: Expected hits: {expected_hits}, fts returned: {hits}"
                  % (expected_hits, hits))
            raise FTSException("Expected hits: %s, fts returned: %s"
                               % (expected_hits, hits))
        if expected_hits and expected_hits == hits:
            print(f"SUCCESS! Expected hits: {expected_hits}, fts returned: {hits}")
        if expected_no_of_results is not None:
            if expected_no_of_results == doc_ids.__len__():
                print(
                    f"SUCCESS! Expected number of results: {expected_no_of_results}, fts returned: {doc_ids.__len__()}")
            else:
                print(f"ERROR! Expected number of results: {expected_no_of_results}, fts returned: {doc_ids.__len__()}")
                print(doc_ids)
                raise FTSException("Expected number of results: %s, fts returned: %s"
                                   % (expected_no_of_results, doc_ids.__len__()))

        if not return_raw_hits:
            return hits, doc_ids, time_taken, status
        else:
            return hits, matches, time_taken, status

    def run_vector_query(self, vector, index, neighbours=None,
                         validate_result_count=True, load_invalid_base64_string=False):

        if isinstance(self.query, str):
            self.query = json.loads(self.query)

        if self.encode_base64_vector:
            if load_invalid_base64_string:
                self.query['knn'][0][
                    'vector_base64'] = "Q291Y2hiYXNlIGlzIGdyZWF0ICB3c2txY21lcW9qZmNlcXcgZGZlIGpkbmZldyBmamUgd2Zob3VyIGwgZnJ3OWZmIGdmaXJ3ZnJ3IGhmaXJoIGZlcmYgcmYgZXJpamZoZXJ1OWdlcmcgb2ogZmhlcm9hZiBmZTlmdSBnZXJnIHJlOWd1cmZyZWZlcmcgaHJlIG8gZXJmZ2Vyb2ZyZmdvdQ=="
            else:
                self.query['knn'][0]['vector_base64'] = self.get_base64_encoding(vector)
        else:
            self.query['knn'][0]['vector'] = vector

        self.log.info("*" * 20 + f" Running Query # {self.count} - on index {self.index_obj['name']} " + "*" * 20)
        self.count += 1
        # Run fts query via n1ql
        n1ql_hits = -1
        if self.run_n1ql_search_function:
            n1ql_query = f"SELECT COUNT(*) FROM `default`.`_default`.`_default` AS t1 WHERE SEARCH(t1, {self.query});"
            self.log.info(f" Running n1ql Query - {n1ql_query}")
            try:
                if self.inbetween_active:
                    n1ql_hits = self.run_n1ql_query_upgrade(n1ql_query, node=self.servers[1])['results'][0]['$1']
                else:
                    n1ql_hits = self.run_n1ql_query_upgrade(n1ql_query, node=self.servers[0])['results'][0]['$1']
            except Exception as e:
                print(e)


            if n1ql_hits == 0:
                n1ql_hits = -1
            self.log.info("FTS Hits for N1QL query: %s" % n1ql_hits)

        # Run fts query
        self.log.info(f" Running FTS Query - {self.query}")

        hits, matches, time_taken, status = self.execute_query_upgrade(query=self.query['query'], index=index,
                                                                       knn=self.query['knn'],
                                                                       explain=self.query['explain'],
                                                                       return_raw_hits=True,
                                                                       fields=self.query['fields'])

        if hits == 0:
            hits = -1

        if hits==-1:
            self.log.info(f"debug for hits -1 : hits : {hits}, status: {status}\n")

        self.log.info("FTS Hits for Search query: %s" % hits)
        # compare fts and n1ql results if required
        if self.run_n1ql_search_function:
            if n1ql_hits == hits:  #
                self.log.info(
                    f"Validation for N1QL and FTS Passed! N1QL hits =  {n1ql_hits}, FTS hits = {hits}")
            else:
                self.log.info({"query": self.query, "reason": f"N1QL hits =  {n1ql_hits}, FTS hits = {hits}"})

        if self.skip_validation_if_no_query_hits and hits == 0:
            hits = -1
            self.log.info(f"FTS Hits for Search query: {hits}, Skipping validations")
            return -1, -1, None, {}

        recall_and_accuracy = {}

        if neighbours is not None:
            query_vector = vector
            fts_matches = []
            for i in range(self.k):
                fts_matches.append(int(matches[i]['id'][3:]) - 10000001)

            self.log.info("*" * 5 + f"Query RESULT # {self.count}" + "*" * 5)
            self.log.info(f"FTS MATCHES: {fts_matches}")

            fts_accuracy, fts_recall = self.compare_results(neighbours[:100], fts_matches, "groundtruth", "fts")

            recall_and_accuracy['fts_accuracy'] = fts_accuracy
            recall_and_accuracy['fts_recall'] = fts_recall

            self.log.info("*" * 30)

        # validate no of results are k only
        if self.run_n1ql_search_function:
            if validate_result_count:
                if len(matches) != self.k and n1ql_hits != self.k:
                    self.log.error(
                        f"No of results are not same as k=({self.k} \n k = {self.k} || N1QL hits = {n1ql_hits}  || "
                        f"FTS hits = {hits}")

        return n1ql_hits, hits, matches, recall_and_accuracy

    def run_vector_queries(self, store_in_xattr=False, encode_base_64=False, vector_field_name="vector_data",
                           vector_field_type="vector", index_name=None):

        is_passed = True
        self.encode_base64_vector = encode_base_64
        self.store_in_xattr = store_in_xattr
        self.vector_field_name = vector_field_name
        self.vector_field_type = vector_field_type

        if store_in_xattr:
            self.vector_field_name = "_$xattrs." + self.vector_field_name

        self.query = {"query": {"match_none": {}}, "explain": True, "fields": ["*"],
                      "knn": [{"field": "vector_data", "k": self.k,
                               "vector": []}]}

        self.query['knn'][0]['field'] = self.vector_field_name

        if self.vector_field_type == "vector_base64":
            self.query['knn'][0] = {}
            self.query['knn'][0] = {"field": self.vector_field_name, "k": self.k, "vector_base64": ""}

        all_stats = []
        bad_indexes = []

        if index_name:
            index_name = index_name
        else:
            index_name = "index_default_vector"
        try:
            self.index_obj = {"name": index_name}
            index_stats = {'index_name': '', 'fts_accuracy': 0, 'fts_recall': 0}
            dataset_name = self.vector_dataset
            queries = self.get_query_vectors(dataset_name)
            neighbours = self.get_groundtruth_file(dataset_name)
            fts_accuracy = []
            fts_recall = []
            num_queries = self.vector_queries_count

            for count, q in enumerate(queries[:num_queries]):
                _, _, _, recall_and_accuracy = self.run_vector_query(vector=q.tolist(), index=self.index_obj,
                                                                     neighbours=neighbours[count])
                fts_accuracy.append(recall_and_accuracy['fts_accuracy'])
                fts_recall.append(recall_and_accuracy['fts_recall'])

            self.log.info(f"fts_accuracy: {fts_accuracy}")
            self.log.info(f"fts_recall: {fts_recall}")

            index_stats['index_name'] = self.index_obj['name']
            index_stats['fts_accuracy'] = (sum(fts_accuracy) / len(fts_accuracy)) * 100
            index_stats['fts_recall'] = (sum(fts_recall) / len(fts_recall))

            if (index_stats['fts_accuracy'] < self.expected_accuracy_and_recall or index_stats['fts_recall'] <
                    self.expected_accuracy_and_recall):
                bad_indexes.append(index_stats)
            all_stats.append(index_stats)

            self.log.info(f"Accuracy and recall for queries run on each index : {all_stats}")
            if len(bad_indexes) != 0:
                self.log.error(f"Indexes have poor accuracy and recall: {bad_indexes}")
                is_passed = False
            else:
                self.log.info(f"SUCCESS")
        except Exception as e:
            self.log.error(e)
            is_passed = False

        return is_passed

    def get_ideal_index_distribution(self,k, n):
        if k==0:
            return [0 for i in range(n)]
        quotient = k // n
        remainder = k % n
        result = [quotient] * n
        for i in range(remainder):
            result[i] += 1
        return result

    def validate_partition_distribution(self, rest):
        _, payload = rest.get_cfg_stats()
        node_defs_known = {k: v["hostPort"] for k, v in payload["nodeDefsKnown"]["nodeDefs"].items()}

        node_active_count = {}
        node_replica_count = {}

        index_active_count = {}
        index_replica_count = {}

        for k, v in payload["planPIndexes"]["planPIndexes"].items():
            index_name = v["indexName"]
            if index_name not in index_active_count:
                index_active_count[index_name] = {}
            if index_name not in index_replica_count:
                index_replica_count[index_name] = {}

            for k1, v1 in v["nodes"].items():
                if v1["priority"] == 0:
                    node_active_count[k1] = node_active_count.get(k1, 0) + 1
                    index_active_count[index_name][k1] = index_active_count[index_name].get(k1, 0) + 1
                else:
                    node_replica_count[k1] = node_replica_count.get(k1, 0) + 1
                    index_replica_count[index_name][k1] = index_replica_count[index_name].get(k1, 0) + 1

        actual_partition_count = sum(node_active_count.values()) + sum(node_replica_count.values())

        print("Actives:")
        for k, v in node_active_count.items():
            print(f"\t{node_defs_known[k]} : {v}")

        print("Replicas:")
        for k, v in node_replica_count.items():
            print(f"\t{node_defs_known[k]} : {v}")

        print(f"Actual number of index partitions in cluster: {actual_partition_count}")

        indexes_map = {}
        expected_partition_count = 0

        if "indexDefs" in payload:
            for k, v in payload["indexDefs"]["indexDefs"].items():
                indexes_map[
                    k] = f"maxPartitionsPerPIndex: {v['planParams']['maxPartitionsPerPIndex']}, indexPartitions: {v['planParams']['indexPartitions']}, numReplicas: {v['planParams']['numReplicas']}"
                curr_active_partitions = v["planParams"]["indexPartitions"]
                curr_replica_partitions = curr_active_partitions * v["planParams"]["numReplicas"]
                expected_partition_count += curr_active_partitions + curr_replica_partitions

        print(f"Expected number of index partitions in cluster: {expected_partition_count}")
        print(f"Indexes: {len(indexes_map)}")
        for k, v in indexes_map.items():
            print(f"\t{k} :: {v}")


        print("Index actives distribution:")
        error = []
        for k, v in index_active_count.items():
            print(f"\tIndex: {k}")
            index_distribution = []
            current_partition_count = 0

            for k1, v1 in v.items():
                print(f"\t\t{node_defs_known[k1]} : {v1}")
                index_distribution.append(int(v1))
                current_partition_count += int(v1)

            index_distribution.sort(reverse = True)

            if index_distribution !=  self.get_ideal_index_distribution(current_partition_count,len(v)):
                error.append(f'index {k} has faulty distribution. index distribution : {index_distribution}')

        print("Index replicas distribution:")
        for k, v in index_replica_count.items():
            print(f"\tIndex: {k}")
            index_distribution = []
            current_partition_count = 0
            for k1, v1 in v.items():
                print(f"\t\t{node_defs_known[k1]} : {v1}")
                index_distribution.append(int(v1))
                current_partition_count += int(v1)

            index_distribution.sort(reverse=True)

            if index_distribution !=  self.get_ideal_index_distribution(current_partition_count,len(v)):
                error.append(f'index {k} has faulty distribution. index distribution : {index_distribution}')

        return error
