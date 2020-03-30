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
        self.config_updates_count = 0
        self.default_type = 'cbdoc'
        self.default_child_type = 'childcbdoc'
        self.delimiter = '_'
        self.regex = '.*'
        self.xd_ref = xd_ref
        self._es_in = xd_ref._input.param("es_in", False)
        self._cb_in = xd_ref._input.param("cb_in", False)
        self._es_out = xd_ref._input.param("es_out", False)
        self._cb_out = xd_ref._input.param("cb_out", False)
        self._es_swap = xd_ref._input.param("es_swap", False)
        self._cb_swap = xd_ref._input.param("cb_swap", False)
        self._cb_failover = xd_ref._input.param("cb_failover", False)
        self._run_time = xd_ref._input.param("run_time", 5)
        self._log = logger.Logger.get_logger()


    def verify_es_results(self, verification_count=0, doc_type=None):
        xd_ref = self.xd_ref
        # Checking replication at destination clusters
        self.verify_es_stats(self.xd_ref.src_nodes, self.xd_ref.dest_nodes, verification_count, doc_type)


    def verify_es_stats(self, src_nodes, dest_nodes, verification_count=0, doc_type=None):
        xd_ref = self.xd_ref

        src_master = self.xd_ref.src_master
        dest_master = self.xd_ref.dest_master

        # prepare for verification
        xd_ref._wait_flusher_empty(src_master, src_nodes)
        xd_ref._expiry_pager(src_master)
        self.sleep(20)
        self._log.info("Verifing couchbase to elasticsearch replication")
        self.verify_es_num_docs(src_master, dest_master, 1, 10, verification_count, doc_type)



    def verify_es_num_docs(self, src_server, dest_server, kv_store=1, retry=10, verification_count=0, doc_type=None):
        es_rest = RestConnection(dest_server)
        buckets = self.xd_ref._get_cluster_buckets(src_server)

        wait = 5

        for bucket in buckets:
            es_num_items = es_rest.get_bucket(bucket.name, doc_type).stats.itemCount
            cb_num_items = verification_count
            self._log.info("STATUS: elastic search item count {0} verification count {1}".format(es_num_items, cb_num_items))

            _retry = retry

            while _retry > 0 and cb_num_items != es_num_items:
                self._log.info("elasticsearch items %s, expected: %s....retry after %s seconds" % \
                                     (es_num_items, cb_num_items, wait))
                time.sleep(wait)
                last_es_items = es_num_items

                es_num_items = es_rest.get_bucket(bucket.name, doc_type).stats.itemCount

                if es_num_items == last_es_items:
                    _retry = _retry - 1
                    # if index doesn't change reduce retry count
                elif es_num_items <= last_es_items:
                    self._log.info("%s items removed from index " % (es_num_items - last_es_items))

                elif es_num_items >= last_es_items:
                    self._log.info("%s items added to index" % (es_num_items - last_es_items))


            if es_num_items != cb_num_items:
                self._log.info("ERROR: Couchbase has %s docs, ElasticSearch returned %s docs " % \
                                     (cb_num_items, es_num_items))

            assert es_num_items == cb_num_items


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

    def _first_level_rebalance_out(self, param_nodes,
                                   available_nodes,
                                   do_failover = False,
                                   monitor = True):

        nodes_out = available_nodes[:1]
        if do_failover:
            self.cluster.failover(param_nodes, nodes_out)

        tasks = self._async_rebalance(param_nodes, [], nodes_out)
        if monitor:
            [task.result() for task in tasks]
        return available_nodes[1:]

    def _first_level_rebalance_in(self, param_nodes,
                                  monitor = True):
        nodes_in = []
        if len(param_nodes) > 1:
            nodes_in = [param_nodes[1]]
            tasks = self._async_rebalance(param_nodes, nodes_in, [])
            if monitor:
                [task.result() for task in tasks]

        return nodes_in

    def _second_level_rebalance_out(self, param_nodes,
                                    available_nodes,
                                    do_swap = False,
                                    monitor = True):
        if len(available_nodes) > 1:
            nodes_in = []
            if do_swap:
                nodes_in = [param_nodes[1]]
            tasks = self._async_rebalance(param_nodes, nodes_in, available_nodes)
            if monitor:
                [task.result() for task in tasks]

    def _second_level_rebalance_in(self, param_nodes, monitor = True):
        if len(param_nodes) > 2:
            nodes_in = param_nodes[2:]
            tasks = self._async_rebalance(param_nodes, nodes_in, [])
            if monitor:
                [task.result() for task in tasks]


    def update_configurations(self, command):
        node = self.xd_ref.dest_master
        es_rest = RestConnection(node)
        #es_rest.eject_node(node)
        es_rest.update_configuration(node, commands=command)
        es_rest.start_es_node(node)

        self.config_updates_count = len(command)


    def reset_configurations(self):
        node = self.xd_ref.dest_master
        es_rest = RestConnection(node)
        es_rest.reset_configuration(node, self.config_updates_count)
        es_rest.start_es_node(node)


    def upload_bad_template(self):
        template_path = 'resources/es/templates/bad_template.json'
        for node in self.xd_ref.dest_nodes:
            es_rest = RestConnection(node)
            es_rest.replace_template(node, template_path)


    def upload_good_template(self):
        template_path = 'resources/es/templates/good_template.json'
        for node in self.xd_ref.dest_nodes:
            es_rest = RestConnection(node)
            es_rest.replace_template(node, template_path)


    def replication_setting(self, param, value):
        src_master = self.xd_ref.src_master
        rest = RestConnection(src_master)
        buckets = rest.get_buckets()
        for bucket in buckets:
            rest.set_xdcr_param(bucket.name, bucket.name, param, value)