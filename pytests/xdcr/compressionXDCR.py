
from .xdcrnewbasetests import XDCRNewBaseTest, TOPOLOGY
from remote.remote_util import RemoteMachineShellConnection, RestConnection
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_helper.cluster import Cluster
import json, time

from pytests.xdcr.tenK_collection_helper import TenKCollectionHelper

class compression(XDCRNewBaseTest):

    # goxdcr exports both of these per (sourceBucketName, targetBucketName,
    # pipelineType) through the stats query API. They are counters owned by the
    # *pipeline instance*, not by the replication: restarting a pipeline resets
    # them to 0 (measured on 8.1.0-2594), which is what
    # _wait_for_compression_restart exists to absorb.
    WIRE_BYTES_METRIC = "xdcr_data_replicated_bytes"
    PRE_COMPRESSION_BYTES_METRIC = "xdcr_data_replicated_uncompress_bytes"

    def setUp(self):
        super(compression, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()
        self.chain_length = self._input.param("chain_length", 2)
        self.topology = self._input.param("ctopology", "chain")
        self.compression_type = self._input.param("compression_type", "Auto")
        self.compression_restart_wait = self._input.param("compression_restart_wait", 60)
        if self.chain_length > 2:
            self.c3_cluster = self.get_cb_cluster_by_name('C3')
            self.c3_master = self.c3_cluster.get_master_node()
        self.cluster = Cluster()

    def tearDown(self):
        super(compression, self).tearDown()

    def _get_repl_setting(self, cluster, repl_id, param):
        """One replication setting of `repl_id`, read over REST.

        goxdcr leaves a setting out of the per-replication payload while it
        still matches the cluster-wide default, so an absent key means "take
        the global value", not "unset" - which matters here because the
        shipped default for compressionType is already Auto.
        """
        rest = RestConnection(cluster.get_master_node())
        api = rest.baseUrl + "settings/replications/" + \
            str(repl_id).replace('/', '%2F')
        status, content, _ = rest._http_request(api)
        if not status:
            self.fail("Could not read settings of replication {0} on {1}".format(
                repl_id, cluster.get_name()))
        settings = json.loads(content)
        if param in settings:
            return settings[param]
        status, content, _ = rest._http_request(rest.baseUrl + "settings/replications")
        if not status:
            self.fail("Could not read the global replication settings on {0}".format(
                cluster.get_name()))
        return json.loads(content).get(param)

    def _get_repl_id(self, cluster, bucket_name):
        repl_id = None
        for repl in cluster.get_remote_clusters()[0].get_replications():
            if bucket_name in str(repl):
                repl_id = repl.get_repl_id()
        return repl_id

    def _wait_for_compression_restart(self, cluster, bucket_name):
        """Let a compressionType change take effect before anything is measured.

        Changing compressionType restarts the replication's pipeline, and the
        byte counters the verification reads belong to the pipeline instance:
        the restart zeroes them. On 8.1.0-2594 the reset lands 5-10s after the
        POST returns, so a test that starts loading two seconds later has its
        bytes counted by the instance that is about to disappear and then wiped
        - which is exactly how the uncompressed side of the comparison came
        back as a flat 0 while the compressed side did not.

        Neither /pools/default/tasks (status stays "running") nor
        xdcr_pipeline_status (stays Running) shows the restart at 1s polling
        granularity, and the counter reset itself is only observable when the
        counter was non-zero to begin with - which it is not when compression
        is configured before the first load. So this is a bounded wait rather
        than a condition poll, and it only runs when the value actually changed.
        """
        self.sleep(self.compression_restart_wait,
                   "Waiting out the pipeline restart caused by the "
                   "compressionType change on {0}/{1}".format(
                       cluster.get_name(), bucket_name))

    def _set_compression_type(self, cluster, bucket_name, compression_type="None",
                              expect_success=True, wait_for_restart=True):
        """Set compressionType on the replication of bucket_name.

        Only "None" and "Auto" are settable over REST: goxdcr keeps "Snappy"
        as an internal-only value and rejects it as user input. A rejected POST
        leaves the previous value in place, which used to surface minutes later
        as a puzzling "compression type is not X" readback assertion, so fail
        here instead with the server's own response. Callers that expect the
        POST to be rejected pass expect_success=False.

        A POST that changes the value restarts the pipeline, and by default that
        restart is waited out here so no caller has to remember to - see
        _wait_for_compression_restart. Tests whose subject *is* a rapid toggle
        pass wait_for_restart=False to keep their own timing.
        """
        repl_id = self._get_repl_id(cluster, bucket_name)
        self.assertIsNotNone(repl_id, "No replication found for bucket " + bucket_name)
        previous_type = self._get_repl_setting(cluster, repl_id, "compressionType")
        shell = RemoteMachineShellConnection(cluster.get_master_node())
        quoted_repl_id = str(repl_id).replace('/', '%2F')
        base_url = "http://" + cluster.get_master_node().ip + ":8091/settings/replications/" + quoted_repl_id
        command = "curl -s -w '\\n%{http_code}' -X POST -u Administrator:password " + base_url + \
                  " -d compressionType=" + str(compression_type)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        shell.disconnect()
        # The last line is the status code appended by -w, the rest is the body.
        http_code = output.pop() if output else ""
        if expect_success:
            self.assertEqual("200", http_code,
                             "Failed to set compressionType={0} on replication {1}: "
                             "HTTP {2}, response {3}".format(compression_type, quoted_repl_id,
                                                             http_code, output))
            if str(previous_type) != str(compression_type):
                self.log.info("compressionType on {0}/{1} changed {2} -> {3}".format(
                    cluster.get_name(), bucket_name, previous_type, compression_type))
                if wait_for_restart:
                    self._wait_for_compression_restart(cluster, bucket_name)
        return output, error

    def _replication_bytes(self, cluster, src_bucket, dest_bucket=None):
        """(bytes on the wire, bytes those mutations weighed uncompressed) for
        one replication, summed over every node of `cluster`.

        Only the Main pipeline is counted: the Backfill pipeline keeps its own
        counters over a different set of mutations, so folding the two together
        makes the ratio unreadable.
        """
        dest_bucket = dest_bucket or src_bucket
        totals = []
        for metric in (self.WIRE_BYTES_METRIC, self.PRE_COMPRESSION_BYTES_METRIC):
            total = 0.0
            matched = 0
            for series in self.query_prometheus_metric_series(cluster, metric):
                labels = series["labels"]
                if labels.get("pipelineType") != "Main" or \
                        labels.get("sourceBucketName") != src_bucket or \
                        labels.get("targetBucketName") != dest_bucket:
                    continue
                total += series["value"]
                matched += 1
            self.assertGreater(matched, 0,
                               "{0} reported no series for {1}->{2} on {3}, so there "
                               "is nothing to verify compression against".format(
                                   metric, src_bucket, dest_bucket, cluster.get_name()))
            totals.append(total)
        return totals[0], totals[1]

    def _wait_for_replication_bytes_to_settle(self, cluster, src_bucket,
                                              dest_bucket=None, timeout=300,
                                              poll_interval=15, stable_polls=3):
        """Poll the byte counters of one replication until they stop moving.

        The counters trail the data - docs are already on the target while the
        source is still accounting for them - so a snapshot taken right after a
        catch-up check reads low. A value that goes *down* means the pipeline
        restarted underneath us and everything read so far belongs to an
        instance that no longer exists, so the stability run starts over.

        poll_interval matches the 15s step query_prometheus_metric_series asks
        for: polling faster than the step would re-read the same sample and
        count that as a value holding still.
        """
        end_time = time.time() + timeout
        wire, pre_compression = self._replication_bytes(cluster, src_bucket, dest_bucket)
        stable = 1
        while stable < stable_polls and time.time() < end_time:
            self.sleep(poll_interval,
                       "{0}/{1}: {2} bytes on the wire, {3} before compression, "
                       "unchanged for {4} poll(s)".format(
                           cluster.get_name(), src_bucket, wire, pre_compression, stable))
            new_wire, new_pre = self._replication_bytes(cluster, src_bucket, dest_bucket)
            if new_pre < pre_compression:
                self.log.warning(
                    "{0}/{1}: bytes before compression dropped {2} -> {3}; the "
                    "pipeline restarted, restarting the measurement".format(
                        cluster.get_name(), src_bucket, pre_compression, new_pre))
                stable = 1
            elif (new_wire, new_pre) == (wire, pre_compression):
                stable += 1
            else:
                stable = 1
            wire, pre_compression = new_wire, new_pre
        if stable < stable_polls:
            self.log.warning(
                "{0}/{1} byte counters were still moving after {2}s (wire={3}, "
                "before compression={4})".format(cluster.get_name(), src_bucket,
                                                 timeout, wire, pre_compression))
        return wire, pre_compression

    def _verify_compression(self, cluster, compr_bucket_name="", uncompr_bucket_name="",
                            compression_type="None"):
        """Assert that `compr_bucket_name`'s replication compressed what it sent.

        The verdict comes from two counters of the *same* replication:
        xdcr_data_replicated_bytes (what went on the wire) against
        xdcr_data_replicated_uncompress_bytes (what those same mutations weighed
        before compression). Measured on 8.1.0-2594 over BlobGenerator data that
        ratio is ~5x for Auto and exactly 1.0 for None.

        The previous check compared absolute byte totals of two *different*
        replications - compressed bucket_1 against uncompressed bucket_2 - and
        was unsound three times over:

        * The counters belong to the pipeline instance, and changing
          compressionType restarts the pipeline. The cluster-wide default is
          already Auto, so of the two POSTs only the None one changed anything
          and only its pipeline restarted; its counter was zeroed seconds after
          the load it was meant to measure, leaving "uncompressed 0 > compressed
          N" -> False. That is the failure this replaces.
        * The two replications do not carry the same mutations - update/delete
          ops land on the last bucket only - and need not have started at the
          same time, so their absolute totals are not comparable even when
          nothing restarts.
        * It summed all 60 samples of the legacy
          /pools/default/buckets/<b>/stats/<stat> window. Each of those samples
          holds the current value of a cumulative counter, so the total was 60x
          the real byte count; and its haveTStamp was passed in seconds where
          ns_server expects milliseconds, so the intended "only bytes since the
          test started" filter silently never applied.
        """
        compr_repl_id = self._get_repl_id(cluster, compr_bucket_name)
        self.assertIsNotNone(compr_repl_id,
                             "No replication found for bucket " + compr_bucket_name)
        ratio = self._verify_compression_ratio(cluster, compr_bucket_name,
                                               compr_repl_id, compression_type)
        if not uncompr_bucket_name:
            return
        uncompr_repl_id = self._get_repl_id(cluster, uncompr_bucket_name)
        self.assertIsNotNone(uncompr_repl_id,
                             "No replication found for bucket " + uncompr_bucket_name)
        # The control replication establishes how much of the ratio above XDCR
        # is *not* responsible for. Its own ratio cannot be asserted - see
        # _verify_compression_ratio - but it is the only way to tell an
        # already-compressed payload from XDCR having done the work, so say so
        # in the log rather than leaving a 1.2x "compression worked" unexplained.
        control_ratio = self._verify_compression_ratio(
            cluster, uncompr_bucket_name, uncompr_repl_id, "None")
        if control_ratio > 1.05:
            self.log.warning(
                "{0}: the loader compressed client-side (the compression-off "
                "control still shows {1:.2f}x), so XDCR's own contribution "
                "cannot be isolated here - {2} reached {3:.2f}x on a payload "
                "that was already compressed before XDCR saw it. Load through "
                "a non-compressing client (BlobGenerator, not "
                "java_sdk_client=True) to measure XDCR compression itself."
                .format(cluster.get_name(), control_ratio, compr_bucket_name, ratio))
        else:
            self.log.info(
                "{0}: compression-off control replicated at {1:.2f}x, so the "
                "{2:.2f}x on {3} is XDCR's own compression".format(
                    cluster.get_name(), control_ratio, ratio, compr_bucket_name))

    def _verify_compression_ratio(self, cluster, bucket_name, repl_id, compression_type):
        """Read back compressionType on `repl_id`, check its byte counters, and
        return the ratio of pre-compression bytes to bytes on the wire.

        For compressionType=None the two counters are asserted equal only when
        the loader itself does not compress. `compressionType` decides whether
        *XDCR* compresses, not whether the payload on the wire is compressed: a
        value that reaches goxdcr already snappy-compressed is forwarded
        compressed whatever the setting says. Measured on 8.1.0-2594 with two
        compressionType=None replications differing only in the loader:
        cbc-pillowfight -yy (forced client-side snappy, which is also what the
        Java SDK does by default) gave 291000 on the wire for 1707000 of
        mutations - 5.87x with XDCR compression *off* - while the same load
        without client compression gave 1707000 == 1707000. So the equality
        holds end to end on the BlobGenerator suites and is kept there; it is
        skipped only under java_sdk_client=True, where it is not an invariant.
        """
        actual_type = self._get_repl_setting(cluster, repl_id, "compressionType")
        self.assertEqual(compression_type, actual_type,
                         "Compression Type for replication {0} is {1}, expected {2}".format(
                             repl_id, actual_type, compression_type))
        self.log.info("Compression Type for replication {0} is {1}".format(
            repl_id, actual_type))

        wire, pre_compression = self._wait_for_replication_bytes_to_settle(
            cluster, bucket_name)
        self.log.info("{0}/{1} (compressionType={2}): {3} bytes on the wire for "
                      "{4} bytes of mutations".format(cluster.get_name(), bucket_name,
                                                      actual_type, wire, pre_compression))

        # A zero here is a broken measurement, not a compression verdict: either
        # nothing replicated, or the pipeline restarted after the data went
        # through and took the counters with it.
        self.assertGreater(pre_compression, 0,
                           "Replication {0} accounted for 0 bytes of mutations, so "
                           "there is nothing to judge compression on - either no "
                           "data replicated or the pipeline restarted after it "
                           "did".format(repl_id))

        # Compressed or not, the wire count can never exceed what the same
        # mutations weigh uncompressed. If it does, the accounting is broken and
        # no ratio derived from it means anything.
        self.assertLessEqual(wire, pre_compression,
                             "Replication {0} reports {1} bytes on the wire for only "
                             "{2} bytes of mutations, which cannot happen - the byte "
                             "accounting is unusable".format(repl_id, wire, pre_compression))
        ratio = pre_compression / wire if wire else 0

        if compression_type == "None":
            if self._use_java_sdk:
                self.log.info(
                    "{0} has compression off and replicated at {1:.2f}x "
                    "({2} -> {3} bytes); not asserting equality because the "
                    "Java SDK loader compresses client-side".format(
                        bucket_name, ratio, pre_compression, wire))
            else:
                # The python memcached loader behind BlobGenerator does not
                # compress client-side - VBucketAwareMemcached takes no
                # compression argument at all - so nothing has touched these
                # values before goxdcr reads them and the two counters must
                # agree exactly. Measured 1.00x on every BlobGenerator run.
                self.assertEqual(wire, pre_compression,
                                 "Replication {0} has compression disabled and was "
                                 "loaded by a client that does not compress, yet it "
                                 "put {1} bytes on the wire for {2} bytes of "
                                 "mutations; the two should match exactly".format(
                                     repl_id, wire, pre_compression))
                self.log.info("Compression is off as expected for {0} "
                              "({1} bytes, uncompressed end to end)".format(
                                  bucket_name, wire))
        else:
            self.assertLess(wire, pre_compression,
                            "Compression did not work as expected: replication {0} is "
                            "set to {1} but put {2} bytes on the wire for {3} bytes of "
                            "mutations".format(repl_id, actual_type, wire, pre_compression))
            self.log.info("Compression worked as expected for {0}: {1:.2f}x "
                          "({2} -> {3} bytes)".format(bucket_name, ratio,
                                                      pre_compression, wire))
        return ratio

    def test_compression_with_unixdcr_incr_load(self):
        bucket_prefix = self._input.param("bucket_prefix", "standard_bucket_")
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, bucket_prefix + "1", self.compression_type)
        self._set_compression_type(self.src_cluster, bucket_prefix + "2")
        if self.chain_length > 2 and self.topology == TOPOLOGY.CHAIN:
            self._set_compression_type(self.dest_cluster, bucket_prefix + "1", self.compression_type)
            self._set_compression_type(self.dest_cluster, bucket_prefix + "2")
        if self.chain_length > 2 and self.topology == TOPOLOGY.RING:
            self._set_compression_type(self.dest_cluster, bucket_prefix + "1", self.compression_type)
            self._set_compression_type(self.dest_cluster, bucket_prefix + "2")
            self._set_compression_type(self.c3_cluster, bucket_prefix + "1", self.compression_type)
            self._set_compression_type(self.c3_cluster, bucket_prefix + "2")

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.perform_update_delete()
        # self._wait_for_replication_to_catchup()  # This function does not account for filtered replications and just assumes the number of docs to be same on source and target
        end_time = time.time() + 300 # timeout for 300 seconds
        while time.time() < end_time:
            docs_matched = self._if_docs_count_match_on_servers()
            if docs_matched:
                print("REPLICATION CAUGHT UP !!!")
                break
            else:
                print("REPLICATION NOT CAUGHT UP YET !!! Sleeping for 30 seconds before retrying")
                time.sleep(30)
        time.sleep(30)
        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name=bucket_prefix + "1",
                                 uncompr_bucket_name=bucket_prefix + "2",
                                 compression_type=self.compression_type)
        if self.chain_length > 2 and self.topology == TOPOLOGY.CHAIN:
            self._verify_compression(cluster=self.dest_cluster,
                                     compr_bucket_name=bucket_prefix + "1",
                                     uncompr_bucket_name=bucket_prefix + "2",
                                     compression_type=self.compression_type)
        if self.chain_length > 2 and self.topology == TOPOLOGY.RING:
            self._verify_compression(cluster=self.dest_cluster,
                                     compr_bucket_name=bucket_prefix + "1",
                                     uncompr_bucket_name=bucket_prefix + "2",
                                     compression_type=self.compression_type)
            self._verify_compression(cluster=self.c3_cluster,
                                     compr_bucket_name=bucket_prefix + "1",
                                     uncompr_bucket_name=bucket_prefix + "2",
                                     compression_type=self.compression_type)
        self.verify_results()



    def test_compression_with_unixdcr_backfill_load(self):
        self.setup_xdcr()
        # Set bucket compression policy=off for standard_bucket_2
        # to ensure data is uncompressed
        RestConnection(self.src_cluster.get_master_node()). \
            set_bucket_compressionMode("standard_bucket_2", "off")
        self.sleep(60)

        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=self.compression_type)
        self.verify_results()

    def test_compression_with_bixdcr_incr_load(self):
        self.setup_xdcr()
        self.sleep(60)

        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")
        self._set_compression_type(self.dest_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.dest_cluster, "standard_bucket_2")

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)
        gen_create = BlobGenerator('comprTwo-', 'comprTwo-', self._value_size, end=self._num_items)
        self.dest_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=self.compression_type)
        self._verify_compression(cluster=self.dest_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=self.compression_type)
        self.verify_results()

    def test_compression_with_bixdcr_backfill_load(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")
        self._set_compression_type(self.dest_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.dest_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()
        self.dest_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)
        gen_create = BlobGenerator('comprTwo-', 'comprTwo-', self._value_size, end=self._num_items)
        self.dest_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()
        self.dest_cluster.resume_all_replications()

        self.perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=self.compression_type)
        self._verify_compression(cluster=self.dest_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=self.compression_type)
        self.verify_results()

    def test_compression_with_pause_resume(self):
        repeat = self._input.param("repeat", 5)
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.async_perform_update_delete()

        for i in range(0, repeat):
            self.src_cluster.pause_all_replications()
            self.sleep(30)
            self.src_cluster.resume_all_replications()

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_optimistic_threshold_change(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        src_conn = RestConnection(self.src_cluster.get_master_node())
        src_conn.set_xdcr_param('standard_bucket_1', 'standard_bucket_1', 'optimisticReplicationThreshold',
                                self._optimistic_threshold)
        src_conn.set_xdcr_param('standard_bucket_2', 'standard_bucket_2', 'optimisticReplicationThreshold',
                                self._optimistic_threshold)

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=self.compression_type)
        self.verify_results()

    def test_compression_with_advanced_settings(self):
        batch_count = self._input.param("batch_count", 10)
        batch_size = self._input.param("batch_size", 2048)
        source_nozzle = self._input.param("source_nozzle", 2)
        target_nozzle = self._input.param("target_nozzle", 2)

        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        src_conn = RestConnection(self.src_cluster.get_master_node())
        src_conn.set_xdcr_param('standard_bucket_1', 'standard_bucket_1', 'workerBatchSize', batch_count)
        src_conn.set_xdcr_param('standard_bucket_1', 'standard_bucket_1', 'docBatchSizeKb', batch_size)
        src_conn.set_xdcr_param('standard_bucket_1', 'standard_bucket_1', 'sourceNozzlePerNode', source_nozzle)
        src_conn.set_xdcr_param('standard_bucket_1', 'standard_bucket_1', 'targetNozzlePerNode', target_nozzle)
        src_conn.set_xdcr_param('standard_bucket_2', 'standard_bucket_2', 'workerBatchSize', batch_count)
        src_conn.set_xdcr_param('standard_bucket_2', 'standard_bucket_2', 'docBatchSizeKb', batch_size)
        src_conn.set_xdcr_param('standard_bucket_2', 'standard_bucket_2', 'sourceNozzlePerNode', source_nozzle)
        src_conn.set_xdcr_param('standard_bucket_2', 'standard_bucket_2', 'targetNozzlePerNode', target_nozzle)

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=self.compression_type)
        self.verify_results()

    def test_compression_with_capi(self):
        self.setup_xdcr()
        self.sleep(60)
        output, error = self._set_compression_type(self.src_cluster, "default", self.compression_type,
                                                   expect_success=False)
        self.assertTrue("The value can not be specified for CAPI replication" in output[0], "Compression enabled for CAPI")
        self.log.info("Compression not enabled for CAPI as expected")

    def test_compression_with_rebalance_in(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self.src_cluster.rebalance_in()

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_rebalance_out(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self.src_cluster.rebalance_out()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=self.compression_type)
        self.verify_results()

    def test_compression_with_swap_rebalance(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self.src_cluster.swap_rebalance()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=self.compression_type)
        self.verify_results()

    def test_compression_with_failover(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        src_conn = RestConnection(self.src_cluster.get_master_node())
        graceful = self._input.param("graceful", False)
        self.recoveryType = self._input.param("recoveryType", None)
        self.src_cluster.failover(graceful=graceful)

        self.sleep(30)

        if self.recoveryType:
            server_nodes = src_conn.node_statuses()
            for node in server_nodes:
                if node.ip == self._input.servers[1].ip:
                    src_conn.set_recovery_type(otpNode=node.id, recoveryType=self.recoveryType)
                    self.sleep(30)
                    src_conn.add_back_node(otpNode=node.id)
            rebalance = self.cluster.async_rebalance(self.src_cluster.get_nodes(), [], [])
            rebalance.result()

        self._wait_for_replication_to_catchup()

        self._verify_compression(cluster=self.src_cluster,
                                 compr_bucket_name="standard_bucket_1",
                                 uncompr_bucket_name="standard_bucket_2",
                                 compression_type=self.compression_type)
        self.verify_results()

    def test_compression_with_replication_delete_and_create(self):
        self.setup_xdcr()
        self.sleep(60)

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.async_perform_update_delete()

        rest_conn = RestConnection(self.src_master)
        rest_conn.remove_all_replications()
        rest_conn.remove_all_remote_clusters()

        self.src_cluster.get_remote_clusters()[0].clear_all_replications()
        self.src_cluster.clear_all_remote_clusters()

        self.setup_xdcr()

        self._set_compression_type(self.src_cluster, "standard_bucket_1", self.compression_type)
        self._set_compression_type(self.src_cluster, "standard_bucket_2")

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_bixdcr_and_compression_one_way(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "default", self.compression_type)

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)
        gen_create = BlobGenerator('comprTwo-', 'comprTwo-', self._value_size, end=self._num_items)
        self.dest_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.perform_update_delete()

        # self._wait_for_replication_to_catchup()
        end_time = time.time() + 300 # timeout for 300 seconds
        while time.time() < end_time:
            docs_matched = self._if_docs_count_match_on_servers()
            if docs_matched:
                print("REPLICATION CAUGHT UP !!!")
                break
            else:
                print("REPLICATION NOT CAUGHT UP YET !!! Sleeping for 30 seconds before retrying")
                time.sleep(30)
        time.sleep(30)
        self._verify_compression(self.src_cluster, compr_bucket_name="default",
                                 compression_type=self.compression_type)
        self.verify_results()

    def test_compression_with_enabling_later(self):
        self.setup_xdcr()
        self.sleep(60)

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.async_perform_update_delete()
        self.sleep(10)

        self._set_compression_type(self.src_cluster, "default", self.compression_type)

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_disabling_later(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "default", self.compression_type)

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.async_perform_update_delete()
        self.sleep(10)

        self._set_compression_type(self.src_cluster, "default", "None")

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_rebalance_out_target_and_disabling(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "default", self.compression_type)

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self.dest_cluster.rebalance_out()

        # The 5s gap is the subject here - a disable/re-enable toggle while the
        # cluster is short a node - so these two skip the restart wait.
        self._set_compression_type(self.src_cluster, "default", "None",
                                   wait_for_restart=False)
        self.sleep(5)
        self._set_compression_type(self.src_cluster, "default", self.compression_type,
                                   wait_for_restart=False)

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_compression_with_rebalance_out_src_and_disabling(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_compression_type(self.src_cluster, "default", self.compression_type)

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('comprOne-', 'comprOne-', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.async_perform_update_delete()

        self.src_cluster.rebalance_out()

        # The 5s gap is the subject here - a disable/re-enable toggle while the
        # cluster is short a node - so these two skip the restart wait.
        self._set_compression_type(self.src_cluster, "default", "None",
                                   wait_for_restart=False)
        self.sleep(5)
        self._set_compression_type(self.src_cluster, "default", self.compression_type,
                                   wait_for_restart=False)

        self._wait_for_replication_to_catchup()

        self.verify_results()

    # ---- 10K Collections Scale Tests ----
    def test_compression_config_10k_collections(self):
        """
        Verify compression mode changes (Auto <-> None, the only values
        settable over REST) work correctly while replicating across 10K
        collections.

        Conf params:
            compression_type: initial compression type (default Auto)
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()
        self.sleep(30)

        self._set_compression_type(self.src_cluster, bucket_name, self.compression_type)

        result = TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="compr_phase1")
        self.log.info("Phase 1 ({}) loaded {} docs".format(
            self.compression_type, result.total_docs_loaded))

        self.sleep(30, "Waiting for partial replication")

        new_type = "None" if self.compression_type != "None" else "Auto"
        self.log.info("Switching compression from {} to {}".format(
            self.compression_type, new_type))
        self._set_compression_type(self.src_cluster, bucket_name, new_type)

        TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="compr_phase2")

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 900))
        except Exception as e:
            self.fail("Compression config 10K catch-up failed: {}".format(e))

        src_count = TenKCollectionHelper.get_bucket_item_count(self.src_master, bucket_name)
        dest_count = TenKCollectionHelper.get_bucket_item_count(self.dest_master, bucket_name)
        self.assertEqual(src_count, dest_count,
                         "Item mismatch after compression change: src={}, dest={}".format(
                             src_count, dest_count))
        self.log.info("Compression config 10K test passed")


