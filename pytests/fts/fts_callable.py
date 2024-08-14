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
from lib.collection.collections_cli_client import CollectionsCLI
from scripts.java_sdk_setup import JavaSdkSetup

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

    def __init__(self, nodes, es_validate=False, es_reset=True, scope=None, collections=None, collection_index=False):
        self.log = logger.Logger.get_logger()
        self.cb_cluster = CouchbaseCluster(name="C1", nodes= nodes, log=self.log)
        self.cb_cluster.get_buckets()
        self.fts_indexes = self.cb_cluster.get_indexes()
        """ have to have a elastic search node to run these tests """
        self.elastic_node = TestInputSingleton.input.elastic
        self.compare_es = es_validate
        self.es = None
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
                         source_params=None, source_uuid=None, collection_index=False, _type=None, analyzer="standard", scope=None, collections=None, no_check=False, cluster=None):
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
            collections=collections
        )
        if collection_index:
            if not index.custom_map:
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

        if no_check:
            index.create_no_check()
        else:
            index.create()
        self.cb_cluster.get_indexes().append(index)
        return index


    def create_default_index(self, index_name, bucket_name, analyzer='standard',
                             cluster=None, collection_index=False, scope=None, collections=None):

        """Create fts index
        @param index_name: name of the index/alias
        @param bucket_name : name of couchbase bucket

        @param type_mapping: UUID of the source, may not be used
        @param analyzer: index analyzer
        """
        types_mapping = self.__define_index_types_mapping(collection_index=collection_index, scope=scope, collections=collections)
        if not cluster:
            cluster = self.cb_cluster

        index = FTSIndex(
            cluster=cluster,
            name=index_name,
            source_name=bucket_name,
            type_mapping=types_mapping,
            collection_index=collection_index,
            scope=scope,
            collections=collections
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


    def wait_for_indexing_complete(self, item_count = None):
        """
            Wait for index_count for any index to stabilize or reach the
            index count specified by item_count
        """
        retry = TestInputSingleton.input.param("index_retry", 20)
        for index in self.cb_cluster.get_indexes():
            if index.index_type == "alias":
                continue
            retry_count = retry
            prev_count = 0
            es_index_count = 0
            while retry_count > 0:
                try:
                    index_doc_count = index.get_indexed_doc_count()
                    bucket_doc_count = index.get_src_bucket_doc_count()
                    if not self.es:
                        self.log.info("Docs in bucket = %s, docs in FTS index '%s': %s"
                                      % (bucket_doc_count,
                                         index.name,
                                         index_doc_count))
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

    def create_alias(self, target_indexes, name=None, alias_def=None):
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
                                                 index_params=alias_def)

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
                use_collections=self.collection_index))

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
        else:
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
            self.create_gen = SDKDataLoader(num_ops = self._num_items, percent_create = 100,
                                           percent_update=0, percent_delete=0, scope=self.scope,
                                           collection=self.collections,
                                           json_template="emp",
                                           start=start, end=start+self._num_items,
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
                                        * (float) (30 / 100))
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
        if not collection_index :
            _type = None
        else:
            _type = []
            for c in collections:
                _type.append(f"{scope}.{c}")
        return _type

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
        num_rep = 0
        if "indexDefs" in payload:
            for k, v in payload["indexDefs"]["indexDefs"].items():
                try:
                    num_rep = v['planParams']['numReplicas']
                except:
                    num_rep = 0
                indexes_map[
                    k] = f"maxPartitionsPerPIndex: {v['planParams']['maxPartitionsPerPIndex']}, indexPartitions: {v['planParams']['indexPartitions']}, numReplicas: {num_rep}"
                curr_active_partitions = v["planParams"]["indexPartitions"]
                curr_replica_partitions = curr_active_partitions * num_rep
                expected_partition_count += curr_active_partitions + curr_replica_partitions

        print(f"Expected number of index partitions in cluster: {expected_partition_count}")
        print(f"Indexes: {len(indexes_map)}")
        pindexes_count = []
        for k, v in indexes_map.items():
            print(f"\t{k} :: {v}")
            parts = v.split(", ")
            for part in parts:
                if part.startswith("indexPartitions"):
                    index_partitions = part.split(": ")[1]
                    pindexes_count.append(index_partitions)
        print("Index actives distribution:")
        error = []
        count = 0
        for k, v in index_active_count.items():
            print(f"\tIndex: {k}")
            index_distribution = []
            current_partition_count = 0
            total = 0
            for k1, v1 in v.items():
                total += v1
                print(f"\t\t{node_defs_known[k1]} : {v1}")
                index_distribution.append(int(v1))
                current_partition_count += int(v1)
            if total != pindexes_count[count]:
                error.append(f"Total active partitions are different- total:: {total} - :: expected{pindexes_count[count]}")

            index_distribution.sort(reverse = True)

            if index_distribution !=  self.get_ideal_index_distribution(current_partition_count,len(v)):
                error.append(f'index {k} has faulty distribution. index distribution : {index_distribution}')

        if num_rep != 0:
            print("Index replicas distribution:")
            count = 0
            for k, v in index_replica_count.items():
                print(f"\tIndex: {k}")
                index_distribution = []
                current_partition_count = 0
                total = 0
                for k1, v1 in v.items():
                    total += v1
                    print(f"\t\t{node_defs_known[k1]} : {v1}")
                    index_distribution.append(int(v1))
                    current_partition_count += int(v1)
                if total != pindexes_count[count]:
                    error.append(
                        f"Total active partitions are different- total:: {total} - :: expected{pindexes_count[count]}")

                index_distribution.sort(reverse=True)

                if index_distribution != self.get_ideal_index_distribution(current_partition_count,len(v)):
                    error.append(f'index {k} has faulty distribution. index distribution : {index_distribution}')

        return error