from membase.api.rest_client import RestConnection
import time

"""class used in conjunction with an XDCRReplicationBaseTest instance.
   purpose is to not clutter xdcrbasetests itself with these es safe
   implementations of similar methods"""

class ESReplicationBaseTest(object):

    xd_ref = None

    def verify_es_results(self, xd_ref, verify_src = False, verification_count = 1000):
        self.xd_ref = xd_ref

        # Checking replication at destination clusters
        dest_key_index = 1
        for key in xd_ref.ord_keys[1:]:
            if dest_key_index == xd_ref.ord_keys_len:
                break
            dest_key = xd_ref.ord_keys[dest_key_index]
            dest_nodes = xd_ref._clusters_dic[dest_key]

            self.verify_es_stats(xd_ref.src_nodes,
                                 dest_nodes,
                                 verify_src,
                                 verification_count)
            dest_key_index += 1


    def verify_es_stats(self, src_nodes, dest_nodes, verify_src = False, verification_count = 1000):
        xd_ref = self.xd_ref

        xd_ref._wait_for_stats_all_buckets(src_nodes)

        xd_ref._expiry_pager(src_nodes[0])
        if verify_src:
            xd_ref._verify_stats_all_buckets(src_nodes)

        self.verify_es_num_docs(src_nodes[0], dest_nodes[0], verification_count = verification_count)


    def verify_es_num_docs(self, src_server, dest_server, kv_store = 1, retry = 5, verification_count = 1000):
        cb_rest = RestConnection(src_server)
        es_rest = RestConnection(dest_server)
        buckets = self.xd_ref._get_cluster_buckets(src_server)
        for bucket in buckets:
            cb_valid, cb_deleted = bucket.kvs[kv_store].key_set()
            cb_num_items = len(cb_valid)
            es_num_items = es_rest.get_bucket(bucket.name).stats.itemCount
            while retry > 0 and cb_num_items != es_num_items:
                self.xd_ref._log.info("elasticsearch items %s, expected: %s....retry" %\
                                     (es_num_items, cb_num_items))
                time.sleep(20)
                es_num_items = es_rest.get_bucket(bucket.name).stats.itemCount

            if es_num_items != cb_num_items:
                self.xd_ref.fail("Error: Couchbase has %s docs, ElasticSearch has %s docs " %\
                                (cb_num_items, es_num_items))

            # query for all es keys
            es_valid = es_rest.all_docs(keys_only=True)
            for _id in cb_valid[verification_count:]:  # match at most 1k keys
                if _id not in es_valid:
                    self.xd_ref.fail("Document %s Missing from ES Index" % _id)


