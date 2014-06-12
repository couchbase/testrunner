from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import VBucketAwareMemcached
from mc_bin_client import MemcachedError
import logger
import time
import json

"""class used in conjunction with an XDCRReplicationBaseTest instance.
   purpose is to not clutter xdcrbasetests itself with these es safe
   implementations of similar methods"""

class ESReplicationBaseTest(object):

    xd_ref = None

    def setup_es_params(self, xd_ref):

        self.xd_ref = xd_ref
        self._es_in = xd_ref._input.param("es_in", False)
        self._cb_in = xd_ref._input.param("cb_in", False)
        self._es_out = xd_ref._input.param("es_out", False)
        self._cb_out = xd_ref._input.param("cb_out", False)
        self._es_swap = xd_ref._input.param("es_swap", False)
        self._cb_swap = xd_ref._input.param("cb_swap", False)
        self._cb_failover = xd_ref._input.param("cb_failover", False)
        self._log = logger.Logger.get_logger()


    def verify_es_results(self, verify_src=False, verification_count=10000):

        xd_ref = self.xd_ref
        rest = RestConnection(self.src_nodes[0])

        # Checking replication at destination clusters
        dest_key_index = 1
        for key in xd_ref.ord_keys[1:]:
            if dest_key_index == xd_ref.ord_keys_len:
                break
            dest_key = xd_ref.ord_keys[dest_key_index]
            dest_nodes = xd_ref._clusters_dic[dest_key]

            src_nodes = rest.get_nodes()
            self.verify_es_stats(src_nodes,
                                 dest_nodes,
                                 verify_src,
                                 verification_count)
            dest_key_index += 1


    def verify_es_stats(self, src_nodes, dest_nodes, verify_src=False, verification_count=10000):
        xd_ref = self.xd_ref

        src_master = self.xd_ref.src_master
        dest_master = self.xd_ref.dest_master

        # prepare for verification
        xd_ref._wait_flusher_empty(src_master, src_nodes)

        xd_ref._expiry_pager(src_master)
        self.sleep(30)
        if verify_src:
            xd_ref._verify_item_count(src_master, src_nodes)

        self._log.info("Verifing couchbase to elasticsearch replication")
        self.verify_es_num_docs(src_master, dest_master, verification_count=verification_count)

        if xd_ref._doc_ops is not None:
            # initial data has been modified
            # check revids
            self._verify_es_revIds(src_master, dest_master, verification_count=verification_count)

            if "create" in xd_ref._doc_ops:
                # initial data values have has been modified
                self._verify_es_values(src_master, dest_master,
                                       verification_count=verification_count)



    def verify_es_num_docs(self, src_server, dest_server, kv_store=1, retry=10, verification_count=10000):
        cb_rest = RestConnection(src_server)
        es_rest = RestConnection(dest_server)
        buckets = self.xd_ref._get_cluster_buckets(src_server)
        wait = 20
        for bucket in buckets:
            all_cb_docs = cb_rest.all_docs(bucket.name)
            cb_valid = [str(row['id']) for row in all_cb_docs['rows']]
            cb_num_items = cb_rest.get_bucket_stats(bucket.name)['curr_items']
            es_num_items = es_rest.get_bucket(bucket.name).stats.itemCount
            _retry = retry
            while _retry > 0 and cb_num_items != es_num_items:
                self._log.info("elasticsearch items %s, expected: %s....retry after %s seconds" % \
                                     (es_num_items, cb_num_items, wait))
                time.sleep(wait)
                last_es_items = es_num_items
                es_num_items = es_rest.get_bucket(bucket.name).stats.itemCount
                if es_num_items == last_es_items:
                    _retry = _retry - 1
                    # if index doesn't change reduce retry count
                elif es_num_items <= last_es_items:
                    self._log.info("%s items removed from index " % (es_num_items - last_es_items))
                    _retry = retry
                elif es_num_items >= last_es_items:
                    self._log.info("%s items added to index" % (es_num_items - last_es_items))
                    _retry = retry



            if es_num_items != cb_num_items:
                self.xd_ref.fail("Error: Couchbase has %s docs, ElasticSearch has %s docs " % \
                                (cb_num_items, es_num_items))

            # query for all es keys
            es_valid = es_rest.all_docs(keys_only=True, indices=[bucket.name], size=cb_num_items)

            if len(es_valid) != cb_num_items:
                self._log.info("WARNING: Couchbase has %s docs, ElasticSearch all_docs returned %s docs " % \
                                     (cb_num_items, len(es_valid)))
            for _id in cb_valid[:verification_count]:  # match at most 10k keys
                id_found = _id in es_valid

                if id_found == False:
                    # document missing from all_docs query do manual term search
                    doc = es_rest.search_all_nodes(_id, indices=[bucket.name])
                    if doc is None:
                        self.xd_ref.fail("Document %s Missing from ES Index (%s)" % (_id, bucket.name))

            self._log.info("Verified couchbase bucket (%s) replicated (%s) docs to elasticSearch with matching keys" % \
                                 (bucket.name, cb_num_items))


    def _verify_es_revIds(self, src_server, dest_server, kv_store=1, verification_count=10000):
        cb_rest = RestConnection(src_server)
        es_rest = RestConnection(dest_server)
        buckets = self.xd_ref._get_cluster_buckets(src_server)
        for bucket in buckets:

            # retrieve all docs from couchbase and es
            # then represent doc lists from couchbase
            # and elastic search in following format
            # [  (id, rev), (id, rev) ... ]
            all_cb_docs = cb_rest.all_docs(bucket.name)
            cb_id_rev_list = self.get_cb_id_rev_list(all_cb_docs)

            es_valid = es_rest.all_docs(indices=[bucket.name], size=len(cb_id_rev_list))
            es_id_rev_list = self.get_es_id_rev_list(es_valid)

            # verify each (id, rev) pair returned from couchbase exists in elastic search
            for cb_id_rev_pair in cb_id_rev_list:

                try:
                    # lookup es document with matching _id and revid
                    # if no exception thrown then doc was properly indexed
                    es_list_pos = es_id_rev_list.index(cb_id_rev_pair)
                except ValueError:

                    # attempt manual lookup by search term
                    es_doc = es_rest.search_all_nodes(cb_id_rev_pair[0], indices=[bucket.name])

                    if es_doc is None:
                        self.xd_ref.fail("Error during verification:  %s does not exist in ES index %s" % (cb_id_rev_pair, bucket.name))

                    # compare
                    es_id_rev_pair = (es_doc['_source']['meta']['id'], es_doc['_source']['meta']['rev'])
                    if cb_id_rev_pair != es_id_rev_pair:
                        self.xd_ref.fail("ES document %s Missmatch Couchbase doc (%s). bucket (%s)" % \
                                        (es_id_rev_pair, cb_id_rev_pair, bucket.name))

            self._log.info("Verified doc rev-ids in couchbase bucket (%s) match meta rev-ids elastic search" % \
                                 (bucket.name))

    def get_cb_id_rev_list(self, docs):
        return [(row['id'], row['value']['rev']) for row in docs['rows']]

    def get_es_id_rev_list(self, docs):
        return [(row['doc']['_id'], row['meta']['rev']) for row in docs]

    def _verify_es_values(self, src_server, dest_server, kv_store=1, verification_count=10000):
        cb_rest = RestConnection(src_server)
        es_rest = RestConnection(dest_server)
        buckets = self.xd_ref._get_cluster_buckets(src_server)
        for bucket in buckets:
            mc = VBucketAwareMemcached(cb_rest, bucket)
            es_valid = es_rest.all_docs(indices=[bucket.name], size=verification_count)

            # compare values of es documents to documents in couchbase
            for row in es_valid[:verification_count]:
                key = str(row['meta']['id'])

                try:
                    _, _, doc = mc.get(key)
                    val_src = str(json.loads(doc)['site_name'])
                    val_dest = str(row['doc']['site_name'])
                    if val_src != val_dest:
                        self.xd_ref.fail("Document %s has unexpected value (%s) expected (%s)" % \
                                        (key, val_src, val_dest))
                except MemcachedError as e:
                    self.xd_ref.fail("Error during verification.  Index contains invalid key: %s" % key)

            self._log.info("Verified doc values in couchbase bucket (%s) match values in elastic search" % \
                                 (bucket.name))

    def verify_dest_added(self):
        src_master = self.xd_ref.src_master
        dest_master = self.xd_ref.dest_master
        rest = RestConnection(src_master)
        remoteClusters = rest.get_remote_clusters()

        # check remote cluster info to ensure dest cluster has been added
        # and that it's status is correct
        for clusterInfo in remoteClusters:
            if clusterInfo['deleted'] == False:
                return

        self.xd_ref.fail("Failed to setup replication to remote cluster %s " % dest_master)
