import random
import time
import traceback
import logging

from membase.api.rest_client import RestConnection
from scripts.edgyjson.main import JSONDoc

from couchbase.cluster import Cluster

import couchbase.subdocument as SD

from .xdcrnewbasetests import XDCRNewBaseTest


class XDCRAdvFilterTests(XDCRNewBaseTest):

    def setUp(self):
        XDCRNewBaseTest.setUp(self)
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
        initial_xdcr = random.choice([True, False])
        self.load_xattrs = self._input.param("load_xattrs", False)
        self.load_binary_xattrs = self._input.param("load_binary_xattrs", False)
        self.skip_validation = self._input.param("ok_if_random_filter_invalid", False)
        self.loader = self._input.param("loader", "default")
        try:
            if initial_xdcr:
                 self.load_data()
                 self.setup_xdcr()
            else:
                self.setup_xdcr()
                self.load_data()
        except Exception as e:
            if self.skip_validation:
                if "create replication failed : status:False,content:{\"errors\":{\"filterExpression\":" in str(e):
                    self.log.warning("Random filter generated may not be valid, skipping doc count validation")
                    self.tearDown()
            else:
                self.fail(str(e))

    def tearDown(self):
        XDCRNewBaseTest.tearDown(self)

    def get_cluster_objects_for_input(self, input):
        """returns a list of cluster objects for input. 'input' is a string
           containing names of clusters separated by ':'
           eg. failover=C1:C2
        """
        clusters = []
        input_clusters = input.split(':')
        for cluster_name in input_clusters:
            clusters.append(self.get_cb_cluster_by_name(cluster_name))
        return clusters
    
    def load_binary_docs_using_cbc_pillowfight(self, server, items, bucket, batch=1000, docsize=100, rate_limit=100000):        
        import subprocess
        import multiprocessing
        num_cores = multiprocessing.cpu_count()
        cmd = "cbc-pillowfight -U couchbase://{0}/{1} -I {2} -m {5} -M {5} -B {3}  " \
              "-t {5} --rate-limit={6} --populate-only".format(server, bucket, items, batch, docsize, num_cores // 2,
                                                               rate_limit)
        cmd += " -u Administrator -P password"
        self.log.info("Executing '{0}'...".format(cmd))
        rc = subprocess.call(cmd, shell=True)
        if rc != 0:
            cmd = "cbc-pillowfight -U couchbase://{0}/{1} -I {2} -m {5} -M {5} -B {3}  " \
                  "-t {5} --rate-limit={6} --populate-only".format(server, bucket, items, batch, docsize, num_cores // 2,
                                                                   rate_limit)
            rc = subprocess.call(cmd, shell=True)
            if rc != 0:
                self.fail("Exception running cbc-pillowfight: subprocess module returned non-zero response!")

    def load_conditional_xattrs(self, num_docs, server, bucket):
        num_xattr_docs = 50 if num_docs > 50 else num_docs

        cb = None
        connection = "couchbase://" + server
        if "ip6" in server or server.startswith("["):
            connection = connection + "?ipv6=allow"
        try:
            from couchbase.cluster import PasswordAuthenticator
            cluster = Cluster(connection)
            authenticator = PasswordAuthenticator("Administrator", "password")
            cluster.authenticate(authenticator)
            cb = cluster.open_bucket(bucket)
            cb.timeout = 100
        except Exception:
            from couchbase.cluster import ClusterOptions
            from couchbase_core.cluster import PasswordAuthenticator
            cluster = Cluster(connection, ClusterOptions(
                PasswordAuthenticator("Administrator", "password")))
            cb = cluster.bucket(bucket).default_collection()
        finally:
            if not cb:
                logging.error("Connection error\n" + traceback.format_exc())

        for val in range(0, num_xattr_docs):
            dockey = str(val)
            dockey = dockey.zfill(20)

            # Distribute xattrs between even and odd docs
            if val % 2 == 0:
                cb.mutate_in(dockey, SD.upsert("boolxattr", {"foo": True}, xattr=True, create_parents=True))
                cb.mutate_in(dockey, SD.upsert("xattr1", False, xattr=True, create_parents=True))
                cb.mutate_in(dockey, SD.upsert("xattr2", "binary-doc1", xattr=True, create_parents=True))
            else:
                cb.mutate_in(dockey, SD.upsert("boolxattr", {"foo": False}, xattr=True, create_parents=True))                     
                cb.mutate_in(dockey,
                            SD.upsert("xattr2", {'field1': val, 'field2': val * val}, xattr=True, create_parents=True))
                cb.mutate_in(dockey, SD.upsert('xattr3', {'field1': {'sub_field1a': val, 'sub_field1b': val * val},
                                                        'field2': {'sub_field2a': 2 * val, 'sub_field2b': 2 * val * val}},
                                            xattr=True, create_parents=True))
                
        logging.info("Added xattrs to {0} docs".format(num_xattr_docs))

    def load_data(self, server=None, bucket="default"):
        try:
            if not server:
                server = self.src_master.ip
            num_docs = self._input.param("items", 10)
            startseqnum = random.randrange(1, 10000000, 1)

            if self.loader == "pillowfight":
                self.load_binary_docs_using_cbc_pillowfight(server, num_docs, bucket)
            else:
                JSONDoc(server=server, username="Administrator", password="password",
                        bucket=bucket, startseqnum=startseqnum,
                        randkey=False, encoding="utf-8",
                        num_docs=num_docs, template="query.json", xattrs=self.load_xattrs)
            self.sleep(30, "Waiting for docs to be loaded")

            if self.load_binary_xattrs:
                self.load_conditional_xattrs(num_docs, server, bucket)                
        except Exception as e:
            self.fail(
                "Errors encountered while loading data: {0}".format(str(e)))

    def verify_results(self):
        rdirection = self._input.param("rdirection", "unidirection")
        replications = self.src_rest.get_replications()
        self.verify_filtered_items(self.src_master, self.dest_master, replications)
        if rdirection == "bidirection":
            self.load_data(self.dest_master.ip)
            replications = self.dest_rest.get_replications()
            self.verify_filtered_items(self.dest_master, self.src_master, replications, skip_index=True)

    def test_xdcr_with_filter(self):
        tasks = []
        rebalance_in = self._input.param("rebalance_in", None)
        rebalance_out = self._input.param("rebalance_out", None)
        swap_rebalance = self._input.param("swap_rebalance", None)
        failover = self._input.param("failover", None)
        graceful = self._input.param("graceful", None)
        pause = self._input.param("pause", None)
        reboot = self._input.param("reboot", None)

        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.pause_all_replications()

        if rebalance_in:
            for cluster in self.get_cluster_objects_for_input(rebalance_in):
                tasks.append(cluster.async_rebalance_in())
                for task in tasks:
                    task.result()

        if failover:
            for cluster in self.get_cluster_objects_for_input(failover):
                cluster.failover_and_rebalance_nodes(graceful=graceful,
                                                     rebalance=True)

        if rebalance_out:
            tasks = []
            for cluster in self.get_cluster_objects_for_input(rebalance_out):
                tasks.append(cluster.async_rebalance_out())
                for task in tasks:
                    task.result()

        if swap_rebalance:
            tasks = []
            for cluster in self.get_cluster_objects_for_input(swap_rebalance):
                tasks.append(cluster.async_swap_rebalance())
                for task in tasks:
                    task.result()

        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.resume_all_replications()

        if reboot:
            for cluster in self.get_cluster_objects_for_input(reboot):
                cluster.warmup_node()
            time.sleep(60)

        self.sleep(30)
        self.perform_update_delete()

        if not self.skip_validation:
            self.verify_results()
