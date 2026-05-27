import random
import time
import traceback
import logging
import shutil
import subprocess
import multiprocessing

from lib.membase.api.rest_client import RestConnection
from scripts.edgyjson.main import JSONDoc
from lib.remote.remote_util import RemoteMachineShellConnection

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
        
    def load_binary_docs_using_cbc_pillowfight(self, server, items, bucket, batch=1000, docsize=100, rate_limit=100000):
        """Load binary (non-JSON) docs with cbc-pillowfight.

        Prefers a cbc-pillowfight already on the testrunner host's PATH (original behavior).
        If it is not installed locally - or the local run fails - falls back to the binary
        bundled with Couchbase Server at /opt/couchbase/bin/cbc-pillowfight, run over SSH on
        the cluster node (couchbase://localhost). The bundled binary always ships with a
        server install, so the load no longer breaks with "cbc-pillowfight: not found" on
        CI workers that lack the libcouchbase tools.

        :param server: cluster node IP string (as passed by load_data) or a node object.
        """
        server_ip = server if isinstance(server, str) else server.ip

        if shutil.which("cbc-pillowfight"):
            # -t = worker threads, kept >= 1 on single-core hosts.
            threads = max(1, multiprocessing.cpu_count() // 2)
            # argv list + shell=False (default): no shell parsing, so an IPv6/bracketed
            # server_ip or any odd ini value can't be glob-expanded, word-split, or injected.
            # -m/-M use docsize (same as the bundled path - both paths load the same size docs).
            cmd = [
                "cbc-pillowfight",
                "-U", f"couchbase://{server_ip}/{bucket}",
                "-I", str(items),
                "-m", str(docsize), "-M", str(docsize),
                "-B", str(batch),
                "-t", str(threads),
                f"--rate-limit={rate_limit}",
                "--populate-only",
                "-u", "Administrator", "-P", "password",
            ]
            self.log.info("Executing local cbc-pillowfight: '{0}'...".format(" ".join(cmd)))
            rc = subprocess.call(cmd)
            if rc != 0:
                self.log.warning(f"Local cbc-pillowfight exited {rc}; retrying once...")
                rc = subprocess.call(cmd)
            if rc == 0:
                self.log.info(f"Loaded {items} binary docs into {bucket} via local cbc-pillowfight")
                return
            self.log.warning(
                f"Local cbc-pillowfight failed (rc={rc}); falling back to bundled "
                f"/opt/couchbase/bin/cbc-pillowfight (docs may be re-populated)")
        else:
            self.log.info(
                "cbc-pillowfight not on testrunner host PATH; using bundled /opt/couchbase/bin/cbc-pillowfight on cluster node")

        self._load_binary_docs_with_bundled_pillowfight(
            self._resolve_load_node(server), items, bucket, batch=batch, docsize=docsize, rate_limit=rate_limit)

    def _resolve_load_node(self, server):
        """Return a cluster node object for a load target.

        Accepts None (-> source master), a node object (returned as-is), or an IP string
        (matched against source/destination cluster nodes). Fails the test if an IP matches
        no known cluster node, rather than silently loading the wrong cluster.

        node.ip is read verbatim from the ini, so an IPv6 literal may or may not be bracketed
        ([fd00::1] vs fd00::1). Both the target and node.ip are normalized before comparing so
        the two forms still match (and so stripping doesn't regress the already-bracketed case).
        """
        if server is None:
            return self.src_master
        if not isinstance(server, str):
            return server
        target = self._strip_ipv6_brackets(server)
        for cluster in (self.src_cluster, self.dest_cluster):
            for node in cluster.get_nodes():
                if self._strip_ipv6_brackets(node.ip) == target:
                    return node
        # Fail loud rather than silently loading a different cluster: a wrong-cluster load
        # would surface later as a confusing doc-count mismatch in verify_results.
        self.fail(f"Could not resolve a cluster node for load target IP {server}")

    @staticmethod
    def _strip_ipv6_brackets(ip):
        """Strip surrounding brackets from an IPv6 literal ([fd00::1] -> fd00::1)."""
        if isinstance(ip, str) and ip.startswith("[") and ip.endswith("]"):
            return ip[1:-1]
        return ip

    def _load_binary_docs_with_bundled_pillowfight(self, node, items, bucket, batch=1000, docsize=100, rate_limit=100000):
        """Load binary docs via /opt/couchbase/bin/cbc-pillowfight over SSH on the cluster node.

        Uses the cbc-pillowfight bundled with Couchbase Server (always present) against
        couchbase://localhost, mirroring load_docs_with_pillowfight in this file.

        Plain exec (no use_channel) is used on purpose. For root or non-root Linux SSH this
        takes the separate-stream branch in execute_command_raw, giving a real exit code and
        stderr to report on failure. The use_channel/PTY path instead merges stderr into
        stdout and returns an empty ``error`` list, so passing use_channel here would blank
        out the diagnostic. NOTE: the PTY branch is also taken when RemoteMachineShellConnection
        sets use_sudo=True - but that only happens for an "Administrator" user (forced False on
        Windows), never for the root/non-root Linux nodes this XDCR suite runs against; if that
        ever changes, switch to asserting on output contents instead of exit_code/error.
        """
        cmd = (f"/opt/couchbase/bin/cbc-pillowfight -u Administrator -P password "
               f"-U couchbase://localhost/{bucket} -I {items} -m {docsize} -M {docsize} "
               f"-B {batch} --rate-limit={rate_limit} --populate-only")
        self.log.info("Executing bundled cbc-pillowfight: '{0}' on {1}...".format(cmd, node.ip))
        output, error = [], []
        for attempt in range(1, 3):
            server_shell = RemoteMachineShellConnection(node)
            output, error, exit_code = server_shell.execute_command(cmd, get_exit_code=True)
            server_shell.disconnect()
            if output:
                self.log.info(f"cbc-pillowfight output: {output}")
            if exit_code == 0:
                self.log.info(f"Loaded {items} binary docs into {bucket} on {node.ip} via bundled cbc-pillowfight")
                return
            self.log.warning(
                f"Bundled cbc-pillowfight exited {exit_code} (attempt {attempt}/2) on {node.ip}: "
                f"stderr={error} stdout={output}")
        self.fail(
            f"Exception running cbc-pillowfight on {node.ip}: non-zero exit code. "
            f"stderr={error} stdout={output}")

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

        # Fail with the real cause here; otherwise cb.mutate_in below throws AttributeError
        # on None and load_data's except masks it as a generic "Errors encountered" message.
        if not cb:
            self.fail(f"load_conditional_xattrs: could not establish an SDK connection to {server}")

        for val in range(0, num_xattr_docs):
            dockey = str(val)
            dockey = dockey.zfill(20)

            # Distribute xattrs between even and odd docs
            if val % 2 == 0:
                cb.mutate_in(dockey, [SD.upsert("boolxattr", {"foo": True}, xattr=True, create_parents=True)])
                cb.mutate_in(dockey, [SD.upsert("xattr1", False, xattr=True, create_parents=True)])
                cb.mutate_in(dockey, [SD.upsert("xattr2", "binary-doc1", xattr=True, create_parents=True)])
            else:
                cb.mutate_in(dockey, [SD.upsert("boolxattr", {"foo": False}, xattr=True, create_parents=True)])                     
                cb.mutate_in(dockey,
                            [SD.upsert("xattr2", {'field1': val, 'field2': val * val}, xattr=True, create_parents=True)])
                cb.mutate_in(dockey, [SD.upsert('xattr3', {'field1': {'sub_field1a': val, 'sub_field1b': val * val},
                                                        'field2': {'sub_field2a': 2 * val, 'sub_field2b': 2 * val * val}},
                                            xattr=True, create_parents=True)])
                
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
        if not self.src_cluster.wait_for_outbound_mutations():
            self.log.warning("Outbound mutations did not drain to 0 on the source cluster "
                             "within the wait timeout; proceeding to the doc-count check "
                             "(a mismatch may be unreplicated backlog, not a filtering error)")
        replications = self.src_rest.get_replications()
        self.verify_filtered_items(self.src_master, self.dest_master, replications)
        if rdirection == "bidirection":
            self.load_data(self.dest_master.ip)
            if not self.dest_cluster.wait_for_outbound_mutations():
                self.log.warning("Outbound mutations did not drain to 0 on the destination "
                                 "cluster within the wait timeout; proceeding to the doc-count "
                                 "check (a mismatch may be unreplicated backlog, not filtering)")
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
