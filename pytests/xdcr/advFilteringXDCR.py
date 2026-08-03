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

from .xdcrnewbasetests import XDCRNewBaseTest, REPL_PARAM, TEST_XDCR_PARAM


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
            from couchbase.auth import PasswordAuthenticator
            from couchbase.options import ClusterOptions
            cluster = Cluster(connection, ClusterOptions(
                PasswordAuthenticator("Administrator", "password")))
            cb = cluster.bucket(bucket).default_collection()
        except Exception:
            logging.error("Connection error\n" + traceback.format_exc())
        finally:
            if not cb:
                logging.error("Connection error: Failed to establish connection")

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

    def _replication_filter_expressions(self, bucket_name):
        """Filter expressions in effect for bucket_name, recorded in
        self.filter_exp the way the base class does it."""
        for repl in self.src_rest.get_replications():
            # Assuming src and dest bucket of the replication have the same name
            bucket = repl['source']
            if repl['filterExpression']:
                exp_in_brackets = '( ' + str(repl['filterExpression']) + ' )'
                self.filter_exp.setdefault(bucket, set()).add(exp_in_brackets)
        return self.filter_exp.get(bucket_name, set())

    def _is_filter_binary_enabled(self, bucket_name):
        """Effective filterBinary for a bucket's replication. get_xdcr_param
        falls back to the global xdcrFilterBinary setting when the replication
        does not override it, and returns a bool or its string form."""
        value = self.src_rest.get_xdcr_param(bucket_name, bucket_name,
                                             REPL_PARAM.FILTER_BINARY)
        if isinstance(value, str):
            return value.strip().lower() == "true"
        return bool(value)

    def _wait_for_item_count_delta(self, cluster, bucket, baseline,
                                   expected_delta, timeout=300, poll_interval=10):
        """Poll a bucket's item count until it has grown by expected_delta.

        Returns the delta observed, which may fall short on timeout - the
        caller decides what a short count means.
        """
        end_time = time.time() + timeout
        delta = self.bucket_item_count(cluster, bucket) - baseline
        while delta < expected_delta and time.time() < end_time:
            self.sleep(poll_interval, "{0}: {1}/{2} new items visible".format(
                cluster.get_name(), delta, expected_delta))
            delta = self.bucket_item_count(cluster, bucket) - baseline
        return delta

    def _wait_for_item_count_to_settle(self, cluster, bucket, timeout=600,
                                       poll_interval=15, stable_polls=3):
        """Poll a bucket's item count until it stops changing, and return it.

        Neither of the ready-made waits fits a filtered replication:
        _wait_for_replication_to_catchup waits for src==dest, which never holds
        while a filter drops docs, and replication_changes_left is treated as
        unreliable in this framework already (see MB-9707 in xdcrnewbasetests).
        """
        end_time = time.time() + timeout
        count = self.bucket_item_count(cluster, bucket)
        stable = 1
        while stable < stable_polls and time.time() < end_time:
            self.sleep(poll_interval, "{0}/{1}: item count {2}, unchanged for "
                       "{3} poll(s)".format(cluster.get_name(), bucket, count,
                                            stable))
            new_count = self.bucket_item_count(cluster, bucket)
            stable = stable + 1 if new_count == count else 1
            count = new_count
        if stable < stable_polls:
            self.log.warning("{0}/{1} item count was still moving after {2}s "
                             "(last={3})".format(cluster.get_name(), bucket,
                                                 timeout, count))
        return count

    def _wait_for_pipeline_to_go_idle(self, bucket, timeout=600,
                                      poll_interval=15, stable_polls=3):
        """Wait until docs_processed on the C1->C2 pipeline stops advancing,
        and return the stats read last.

        Both hazards this test has to avoid are the same "is the pipeline
        busy?" question: a baseline taken before replication has started would
        absorb the setUp load into the binary-load window, and a verdict taken
        while mutations are still in flight would read a leak as a clean
        filter. Stat read failures only warn - the counters are diagnostics
        here, the item counts are the verdict.
        """
        end_time = time.time() + timeout
        stats = self.get_docs_processed_to_peer(
            self.src_cluster, self.dest_cluster, src_bucket_filter=bucket)
        processed = stats.get("docs_processed", 0)
        stable = 1
        while stable < stable_polls and time.time() < end_time:
            self.sleep(poll_interval, "docs_processed {0}, unchanged for {1} "
                       "poll(s)".format(processed, stable))
            stats = self.get_docs_processed_to_peer(
                self.src_cluster, self.dest_cluster, src_bucket_filter=bucket)
            new_processed = stats.get("docs_processed", 0)
            stable = stable + 1 if new_processed == processed else 1
            processed = new_processed
        if stable < stable_polls:
            self.log.warning("docs_processed was still advancing after {0}s "
                             "(last={1})".format(timeout, processed))
        if stats.get("_failed_reads"):
            self.log.warning("pipeline stats unreadable, fell back to a fixed "
                             "wait: {0}".format(stats.get("_failed_reads")))
        return stats

    def test_xdcr_with_filter_for_binary(self):
        """Advanced filtering versus binary (non-JSON) documents.

        Item counts come from KV (curr_items), not N1QL: the query service
        cannot see binary docs, so a COUNT(*) carrying the filter expression
        returns 0 on both clusters no matter what XDCR did with them. That is
        what used to make this test report "binary docs were replicated" off
        SRC:0, TARGET:0 while 10000 binary docs sat unreplicated on the source.
        """
        items = self._input.param("items", 100)
        bucket = self._input.param("bucket_name", "default")
        wait_timeout = self._input.param("wait_timeout", 600)

        # filterBinary reaches the replication either as a test param
        # (filter_binary=True, applied here) or as a creation-time setting
        # inside default@C1=... (filter_binary:True, e.g.
        # py-xdcr-memory-throttler.conf), so read the effective value back off
        # the replication rather than trusting one of the two conf styles.
        if self._input.param(TEST_XDCR_PARAM.FILTER_BINARY, False):
            self.src_rest.set_xdcr_param(bucket, bucket,
                                         REPL_PARAM.FILTER_BINARY, "true")
            self.log.info("Set filterBinary to be True")
        filter_binary = self._is_filter_binary_enabled(bucket)
        filter_exps = self._replication_filter_expressions(bucket)

        # What the target is allowed to gain from a binary load:
        #   filterBinary=true         -> nothing, whatever the expression says
        #   no filter expression      -> every binary doc
        #   key/metadata-only clause  -> the docs whose key matches. How many
        #                                that is depends on cbc-pillowfight's
        #                                key format, which is not a contract,
        #                                so only the src_delta bound is checked
        #   a body clause             -> nothing: a doc with no JSON body can
        #                                never match it (an expression mixing
        #                                both is covered by the bound above)
        if filter_binary or (filter_exps and not any(
                "META()" in exp for exp in filter_exps)):
            expected = "none"
        elif not filter_exps:
            expected = "all"
        else:
            expected = "some"
        self.log.info("filterBinary={0}, filter expressions={1} -> expecting "
                      "'{2}' of the binary docs on the target".format(
                          filter_binary, sorted(filter_exps), expected))

        # A baseline only means something once the JSON load from setUp has
        # finished reaching the target.
        pipeline_before = self._wait_for_pipeline_to_go_idle(
            bucket, timeout=wait_timeout)
        src_before = self.bucket_item_count(self.src_cluster, bucket)
        dest_before = self._wait_for_item_count_to_settle(
            self.dest_cluster, bucket, timeout=wait_timeout)
        self.log.info("Baseline item counts: src={0}, target={1}".format(
            src_before, dest_before))

        self.load_docs_with_pillowfight(self.src_master, items=items,
                                        bucket=bucket, batch=1000, docsize=300)

        # Ground truth for the load itself - without it the test passes
        # vacuously whenever cbc-pillowfight writes nothing.
        src_delta = self._wait_for_item_count_delta(
            self.src_cluster, bucket, src_before, items)
        self.assertGreater(src_delta, 0,
                           "cbc-pillowfight loaded no binary docs on the "
                           "source (item count stayed at {0}); nothing to "
                           "validate".format(src_before))
        self.log.info("Source gained {0} binary docs (asked for {1})".format(
            src_delta, items))

        # Let goxdcr run the binary mutations through the pipeline, then let the
        # target count settle, before reading the verdict off the item counts.
        pipeline_after = self._wait_for_pipeline_to_go_idle(
            bucket, timeout=wait_timeout)
        dest_after = self._wait_for_item_count_to_settle(
            self.dest_cluster, bucket, timeout=wait_timeout)
        dest_delta = dest_after - dest_before
        pipeline_delta = {stat: pipeline_after.get(stat, 0) - pipeline_before.get(stat, 0)
                          for stat in self.PIPELINE_STATS}
        self.log.info("Binary load window: src +{0}, target +{1}, pipeline {2}"
                      .format(src_delta, dest_delta, pipeline_delta))
        if pipeline_delta["docs_processed"] < src_delta:
            # Not fatal on its own (see MB-9707 on XDCR stat reliability), but
            # it means the verdict below rests on a possibly early snapshot.
            self.log.warning(
                "Pipeline only accounted for {0} of the {1} binary docs loaded "
                "on the source; the target count may still move".format(
                    pipeline_delta["docs_processed"], src_delta))

        if expected == "none":
            # A leak can only push the count up; <= 0 rather than == 0 so an
            # unrelated expiry/purge tick cannot fail the test on its own.
            self.assertLessEqual(dest_delta, 0,
                                 "{0} binary docs reached the target although "
                                 "filterBinary={1} and filter expressions={2} "
                                 "should have filtered every one of them".format(
                                     dest_delta, filter_binary, sorted(filter_exps)))
        elif expected == "all":
            self.assertEqual(src_delta, dest_delta,
                             "Unfiltered replication should have carried all "
                             "{0} binary docs to the target, it carried {1}"
                             .format(src_delta, dest_delta))
        else:
            self.assertTrue(0 <= dest_delta <= src_delta,
                            "Target gained {0} binary docs out of {1} loaded "
                            "on the source under filter expressions {2}"
                            .format(dest_delta, src_delta, sorted(filter_exps)))
        self.log.info("Binary doc filtering behaved as expected ('{0}'): "
                      "src +{1}, target +{2}".format(expected, src_delta, dest_delta))
