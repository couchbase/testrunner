import random
import time

from membase.api.rest_client import RestConnection
from scripts.edgyjson.main import JSONDoc
from lib.remote.remote_util import RemoteMachineShellConnection
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
        self.skip_validation = self._input.param("ok_if_random_filter_invalid", False)
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

    def load_data(self, server=None, bucket="default"):
        try:
            if not server:
                server = self.src_master.ip
            num_docs = self._input.param("items", 10)
            JSONDoc(server=server, username="Administrator", password="password",
                    bucket=bucket, startseqnum=random.randrange(1, 10000000, 1),
                    randkey=False, encoding="utf-8",
                    num_docs=num_docs, template="query.json", xattrs=self.load_xattrs)
            self.sleep(30, "Waiting for docs to be loaded")
        except Exception as e:
            self.fail(
                "Errors encountered while loading data: {0}".format(str(e)))

    def load_docs_with_pillowfight(self, server, items, bucket, batch=1000, docsize=100, rate_limit=100000, scope="_default", collection="_default", command_timeout=10):
        server_shell = RemoteMachineShellConnection(server)
        cmd = f"/opt/couchbase/bin/cbc-pillowfight -u Administrator -P password -U couchbase://localhost/"\
            f"{bucket} -I {items} -m {docsize} -M {docsize} -B {batch} --rate-limit={rate_limit} --populate-only --collection {scope}.{collection}"
        self.log.info("Executing '{0}'...".format(cmd))
        output, error  = server_shell.execute_command(cmd, timeout=command_timeout, use_channel=True)
        if output:
            self.log.info(f"Output: {output}")
        if error:
            self.fail(f"Failed to load docs in cluster in {bucket}.{scope}.{collection}")
        server_shell.disconnect()
        self.log.info(f"Data loaded into {bucket}.{scope}.{collection} successfully")

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

    def test_xdcr_with_filter_for_binary(self):
        rdirection = self._input.param("rdirection", "unidirection")
        items = self._input.param("items", 100)
        exclude_binary = self._input.param("filter_binary", False)
        should_be_filtered = False   # should binary docs be filtered
        self.wait_interval(10, "Wait for initial replication setup")
        replications = self.src_rest.get_replications()
        for repl in replications:
            # Assuming src and dest bucket of the replication have the same name
            bucket = repl['source']
            if repl['filterExpression']:
                exp_in_brackets = '( ' + str(repl['filterExpression']) + ' )'
                if bucket in self.filter_exp.keys():
                    self.filter_exp[bucket].add(exp_in_brackets)
                else:
                    self.filter_exp[bucket] = {exp_in_brackets}
        filter_exp = self.filter_exp['default']
        if "META()" in filter_exp:
            should_be_filtered = True
        if exclude_binary:
            self.src_rest.set_xdcr_param("default", "default", "filterBinary", "true")
            self.log.info("Set filterBinary to be True")

        self.load_docs_with_pillowfight(self.src_master, items=items, bucket="default", batch=1000, docsize=300)
        self.sleep(10, "sleeping after inserting binary docs")

        if should_be_filtered:
            if self._if_docs_count_match_on_servers():
                self.log.info("Binary docs filtered and count matches")
            else:
                self.fail("Binary docs were not replicated")
        else:
            if self._if_docs_count_match_on_servers():
                self.fail("Binary docs were replicated when they were not supposed to")