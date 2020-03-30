import logger
import time
import copy
import json
from .fts_base import FTSIndex, CouchbaseCluster
from lib.membase.api.exception import FTSException
from .es_base import ElasticSearchBase
from TestInput import TestInputSingleton
from lib.couchbase_helper.documentgenerator import JsonDocGenerator
from lib.membase.api.rest_client import RestConnection
from .random_query_generator.rand_query_gen import FTSESQueryGenerator

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

    def __init__(self, nodes, es_validate=False, es_reset=True):
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
        if self.compare_es and not self.elastic_node:
            raise "For ES result validation, pls add in the"
        elif self.compare_es:
            self.es = ElasticSearchBase(self.elastic_node, self.log)
            if es_reset:
                self.es.delete_index("es_index")
                self.es.create_empty_index_with_bleve_equivalent_std_analyzer("es_index")
                self.log.info("Created empty index \'es_index\' on Elastic Search node with "
                              "custom standard analyzer(default)")

    def create_default_index(self, index_name, bucket_name):
        index = FTSIndex(self.cb_cluster,
                         name=index_name,
                         source_name=bucket_name)
        rest = RestConnection(self.cb_cluster.get_random_fts_node())
        index.create(rest)
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
        self.es.delete_indices()

    def async_load_data(self):
        """ Loads data into CB and ES"""
        load_tasks = []
        self.__populate_create_gen()
        if self.es:
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
                query_index=count))

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
        self.create_gen = JsonDocGenerator(name="emp",
                                           encoding="utf-8",
                                           start=start,
                                           end=start + self._num_items)

    def __populate_update_gen(self):
        self.update_gen = copy.deepcopy(self.create_gen)
        self.update_gen.start = 0
        self.update_gen.end = int(self.create_gen.end *
                                  (float)(30) / 100)
        self.update_gen.update()

    def __populate_delete_gen(self):
        self.delete_gen = JsonDocGenerator(
            self.create_gen.name,
            op_type="delete",
            start=int((self.create_gen.end)
                      * (float)(100 - 30) / 100),
            end=self.create_gen.end)
