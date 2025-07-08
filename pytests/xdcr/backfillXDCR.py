from lib.couchbase_helper.documentgenerator import BlobGenerator
from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest, NodeHelper
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.memcached.helper.data_helper import VBucketAwareMemcached


class BackfillXDCR(XDCRNewBaseTest):
    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_master_rest = RestConnection(self.src_master)
        self.dest_master_rest = RestConnection(self.dest_master)
        self.rdirection = self._input.param("rdirection", "bidirection")

    def setup_bixdcr(self):
        self.dest_master_rest.add_remote_cluster(self.src_master.ip, self.src_master.port,
                                                 self.src_master.rest_username, self.src_master.rest_password,
                                                 "c2-to-c1")
        self.dest_master_rest.start_replication(replicationType="continuous", fromBucket="default",
                                                toCluster="c2-to-c1", toBucket="default")

    def kill_xdcr(self, wait_to_recover=True):
        src_shell = RemoteMachineShellConnection(self.src_master)
        dest_shell = RemoteMachineShellConnection(self.dest_master)

        try:
            o, err = src_shell.execute_command(
                'pgrep -x goxdcr && kill $(pgrep -x goxdcr) && echo "killed" || echo "Not running"')
            if err:
                self.log.error(f"Error killing goxdcr process on source: {err}")
            if o:
                self.log.info(f"Output from killing goxdcr on source: {o}")
        except Exception as e:
            self.log.error(f"Error killing goxdcr process on source: {str(e)}")

        try:
            o, err = dest_shell.execute_command(
                'pgrep -x goxdcr && kill $(pgrep -x goxdcr) && echo "killed" || echo "Not running"')
            if err:
                self.log.error(f"Error killing goxdcr process on destination: {err}")
            if o:
                self.log.info(f"Output from killing goxdcr on destination: {o}")
        except Exception as e:
            self.log.error(f"Error killing goxdcr process on destination: {str(e)}")

        if wait_to_recover:
            self.sleep(5, "Wait for goxdcr process to start")

    def set_internal_xdcr_settings(self, server, param, value):
        server_shell = RemoteMachineShellConnection(server)
        cmd = f"curl -X POST -u Administrator:password http://localhost:9998/xdcr/internalSettings -d '{param}={value}'"
        out, err = server_shell.execute_command(cmd, timeout=5, use_channel=True)
        server_shell.disconnect()
        if err:
            self.log.info(f"Error setting internal XDCR settings: {err}")

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

    def test_with_periodic_kill(self):
        self.setup_xdcr()
        self.wait_interval(10, "Wait for initial replication setup")
        if self.rdirection == "bidirection":
            self.setup_bixdcr()

        # clear the goxdcr.log file on source to avoid past lookup by renaming it
        o, err = RemoteMachineShellConnection(self.src_master).execute_command(
            "mv /opt/couchbase/var/lib/couchbase/logs/goxdcr.log /opt/couchbase/var/lib/couchbase/logs/goxdcr.backup.log")
        if err:
            self.fail(f"Error clearing goxdcr.log file: {err}")

        # Set internal settings to control backfill behavior
        self.set_internal_xdcr_settings(self.src_master, "BackfillReplSvcSetBackfillRaiserDelaySec", 20)
        self.log.info("Set BackfillRaiserDelaySec to 20 seconds")

        # Load some initial data into the main pipeline
        self.load_docs_with_pillowfight(self.src_master, 10000, "default", batch=1000, docsize=300)
        self.log.info("Loaded initial 10,000 documents")

        # Setup mismatched collections to trigger backfill
        self.src_master_rest.create_scope("default", "S1")
        self.src_master_rest.create_collection("default", "S1", "c1")
        self.src_master_rest.create_collection("default", "S1", "c2")
        self.src_master_rest.create_collection("default", "S1", "c3")
        self.log.info("Created S1 scope with col1, col2, col3 collections on source")

        # Load data into collections
        self.load_docs_with_pillowfight(self.src_master, 2000, "default", scope="S1", collection="c1")
        self.load_docs_with_pillowfight(self.src_master, 2000, "default", scope="S1", collection="c2")
        self.load_docs_with_pillowfight(self.src_master, 2000, "default", scope="S1", collection="c3")
        self.log.info("Loaded 2000 documents into each collection")

        # Delay backfill to simulate slower replication
        self.set_internal_xdcr_settings(self.src_master, "xdcrDevBackfillSendDelayMs", 15000)

        # Create matching scope/collection on target to trigger backfill
        self.dest_master_rest.create_scope("default", "S1")
        self.dest_master_rest.create_collection("default", "S1", "c1")
        self.load_docs_with_pillowfight(self.src_master, 1000, "default", scope="S1", collection="c1")
        self.log.info("Created S1 scope with col1 collection on target")

        # Wait for discovery and automatic backfill
        self.sleep(70, "Waiting for discovery and automatic backfill")

        # Kill XDCR on source to see how it handles recovery
        self.kill_xdcr(wait_to_recover=True)
        self.log.info("Killed and restarted XDCR process")

        # Create additional collections on source as a workaround for potential issues
        self.src_master_rest.create_collection("default", "S1", "colA")
        self.src_master_rest.create_collection("default", "S1", "colB")
        self.log.info("Created additional collections colA and colB on source")

        # Sleep for source manifest to refresh
        self.sleep(10, "Waiting for source manifest to refresh")

        # Create second missing target collection to trigger another backfill
        self.dest_master_rest.create_collection("default", "S1", "c2")
        self.load_docs_with_pillowfight(self.src_master, 1000, "default", scope="S1", collection="c2")
        self.log.info("Created col2 collection on target")

        # Wait for backfill to be raised
        self.sleep(30, "Waiting for backfill to be raised")

        self.kill_xdcr(wait_to_recover=True)
        self.log.info("Killed and restarted XDCR process again")

        # Sleep for some initializations to finish
        self.sleep(20, "Waiting for initializations to finish")

        # Create additional collections on source for testing
        self.src_master_rest.create_collection("default", "S1", "colC")
        self.src_master_rest.create_collection("default", "S1", "colD")
        self.log.info("Created additional collections colC and colD on source")

        self.set_internal_xdcr_settings(self.src_master, "xdcrDevBackfillSendDelayMs", 0)

        # Create third missing target collection
        self.dest_master_rest.create_collection("default", "S1", "c3")
        self.load_docs_with_pillowfight(self.src_master, 1000, "default", scope="S1", collection="c3")
        self.log.info("Created col3 collection on target")
        self.sleep(30, "Waiting for docs to be inserted")

        matches, count = NodeHelper.check_goxdcr_log(server=self.src_master, search_str="Unable to find shas")
        if count > 0:
            self.fail(f"Found {count} matches for 'unable to find shas' in goxdcr.log")

    def test_repl_map_cleared(self):
        src_scope, dest_scope = "S1", "S2"
        src_cols, dest_cols = ["col1", "col2"], ["col1", "col2"]
        try:
            shell = RemoteMachineShellConnection(self.src_master)
            shell.execute_command(
                "mv /opt/couchbase/var/lib/couchbase/logs/goxdcr.log /opt/couchbase/var/lib/couchbase/logs/goxdcr.backup.log")
            shell.disconnect()

            self.src_master_rest.create_scope("default", src_scope)
            for col in src_cols:
                self.src_master_rest.create_collection("default", src_scope, col)

            self.dest_master_rest.create_scope("default", dest_scope)

            gen = BlobGenerator("doc-", "doc-", start=0, end=1000, value_size=300)
            self.src_cluster.load_all_buckets_from_generator(gen)

            mapping_rules = {
                '"S1.col1"': '"S2.col1"',
                '"S1.col2"': '"S2.col2"',
                '"_default._default"': '"_default._default"'
            }

            self.setup_xdcr()
            setting_val_map = {
                "collectionsExplicitMapping": "true",
                "colMappingRules": '{' + ','.join([f'{k}:{v}' for k, v in mapping_rules.items()]) + '}',
            }

            self.src_master_rest.set_xdcr_params("default", "default", setting_val_map)

            for col in dest_cols:
                self.dest_master_rest.create_collection("default", dest_scope, col)

            for col in src_cols:
                self.load_docs_with_pillowfight(self.src_master, 2000, "default", scope=src_scope, collection=col)

            self.sleep(30, "waiting for replication")

            self._wait_for_replication_to_catchup()

            shell_conn = RemoteMachineShellConnection(self.src_master)
            out, err = shell_conn.execute_command(
                'cat /opt/couchbase/var/lib/couchbase/logs/goxdcr.log | grep "ongoingReplMap=map" | sort -u')
            if err:
                self.fail(f"Error reading logs: {err}")
            first_line = out[0]
            if "backfill_" in first_line.split('=')[-1]:
                self.fail("Backfill pipline not cleared")
        except Exception as e:
            self.fail(f"Test failed due to the Exception: {e}")

    def test_live_update(self):
        src_scope = "S1"
        dest_scope = "S2"
        src_cols = dest_cols = ["col1", "col2"]
        self.src_master_rest.create_scope("default", src_scope)
        for col in src_cols:
            self.src_master_rest.create_collection("default", src_scope, col)

        self.dest_master_rest.create_scope("default", dest_scope)

        self.setup_xdcr()
        self.load_docs_with_pillowfight(self.src_master, 2000, "default")
        mapping_rules = {
            '"S1.col1"': '"S2.col1"',
            '"S1.col2"': '"S2.col2"',
            '"_default._default"': '"_default._default"'
        }

        setting_val_map = {
            "collectionsExplicitMapping": "true",
            "colMappingRules": '{' + ','.join([f'{k}:{v}' for k, v in mapping_rules.items()]) + '}',
        }
        self.src_master_rest.set_xdcr_params("default", "default", setting_val_map)

        for col in src_cols:
            self.load_docs_with_pillowfight(server=self.src_master, items=10000, bucket="default", scope=src_scope,
                                            collection=col)

        self.sleep(20, "sleeping after loading data")

        for col in dest_cols:
            self.dest_master_rest.create_collection("default", dest_scope, col)

        self._wait_for_replication_to_catchup()

    def test_backfill_stopbackfill_explicit_mapping_deadlock(self):
        """
        Test for XDCR deadlock scenario when backfill request handler VB done handling
        calls StopBackfill while explicit mapping changes trigger handleExplicitMapChangeBackfillReq.
        """
        src_scope, dest_scope = "S1", "S2"
        src_cols, dest_cols = ["col1", "col2", "col3", "col4"], ["col1", "col2"]
        try:

            # Clear the goxdcr.log file on source to avoid past lookup
            shell = RemoteMachineShellConnection(self.src_master)
            shell.execute_command(
                "mv /opt/couchbase/var/lib/couchbase/logs/goxdcr.log /opt/couchbase/var/lib/couchbase/logs/goxdcr.backup.log")
            shell.disconnect()

            self.src_master_rest.create_scope("default", src_scope)
            for col in src_cols:
                self.src_master_rest.create_collection("default", src_scope, col)
            self.dest_master_rest.create_scope("default", dest_scope)
            for col in dest_cols:
                self.dest_master_rest.create_collection("default", dest_scope, col)

            for col in src_cols:
                self.load_docs_with_pillowfight(self.src_master, 1500, "default", scope=src_scope, collection=col)

            mapping_rules = {
                '"S1.col1"': '"S2.col1"',
                '"S1.col2"': '"S2.col2"',
                '"S1.col3"': '"S2.col3"',
                '"S1.col4"': '"S2.col4"',
                '"_default._default"': '"_default._default"'
            }

            self.setup_xdcr()
            setting_val_map = {
                "collectionsExplicitMapping": "true",
                "colMappingRules": '{' + ','.join([f'{k}:{v}' for k, v in mapping_rules.items()]) + '}',
                "xdcrDevBackfillSendDelayMs": 10000
            }
            self.src_master_rest.set_xdcr_params("default", "default", setting_val_map)

            self.dest_master_rest.create_collection("default", "S2", "col3")
            self.sleep(5, "Wait for discovery and automatic backfill to start")

            fast_backfill_setting_val_map = {
                "collectionsExplicitMapping": "true",
                "colMappingRules": '{' + ','.join([f'{k}:{v}' for k, v in mapping_rules.items()]) + '}',
                "xdcrDevBackfillSendDelayMs": 0,  # Remove send delay
            }
            self.src_master_rest.set_xdcr_params("default", "default", fast_backfill_setting_val_map)

            self.dest_master_rest.create_collection("default", "S2", "col4")

            src_mc = VBucketAwareMemcached(self.src_master_rest, "default")

            test_doc_key = "regDocA_deadlock_trigger"
            src_mc_active = src_mc.memcached(test_doc_key)
            src_mc_active.set(key=test_doc_key, exp=0, val='{"foo": "bar"}', flags=0)

            recovery_doc_key = "recovery_test_doc"
            src_mc_active = src_mc.memcached(recovery_doc_key)
            src_mc_active.set(key=recovery_doc_key, exp=0, flags=0, val='{"foo": "bar"}')

            self._wait_for_replication_to_catchup(timeout=600)

        except Exception as e:
            self.fail(f"Test failed with exception: {str(e)}")

    def test_backfill_vb_task_persistence_bug(self):
        try:
            self.setup_xdcr()

            shell = RemoteMachineShellConnection(self.src_master)
            o, err = shell.execute_command(
                "mv /opt/couchbase/var/lib/couchbase/logs/goxdcr.log /opt/couchbase/var/lib/couchbase/logs/goxdcr.backup.log")
            shell.disconnect()
            if err:
                self.log.warning(f"Error clearing goxdcr.log file: {err}")

            self.src_master_rest.create_scope("default", "S1")
            self.src_master_rest.create_collection("default", "S1", "col1")
            self.src_master_rest.create_collection("default", "S1", "col2")
            self.log.info("Created S1 scope with col1 and col2 collections on source")

            self.load_docs_with_pillowfight(self.src_master, 10000, "default", scope="S1", collection="col1")
            self.load_docs_with_pillowfight(self.src_master, 10000, "default", scope="S1", collection="col2")
            self.log.info("Loaded 10000 documents into each collection (20000 total)")

            setting_val_map = {
                "xdcrDevBackfillSendDelayMs": 10000  # 10 second delay to simulate slow backfill
            }
            self.src_master_rest.set_xdcr_params("default", "default", setting_val_map)
            self.log.info("Set backfill delay to 10000ms")

            self.sleep(15, "Wait for replication settings to take effect")

            self.dest_master_rest.create_scope("default", "S1")
            self.dest_master_rest.create_collection("default", "S1", "col1")
            self.sleep(100, "Wait for discovery and automatic backfill to start")

            self.dest_master_rest.create_collection("default", "S1", "col2")
            self.sleep(100, "Wait for second backfill to be raised")

            updated_setting_val_map = {
                "xdcrDevBackfillSendDelayMs": 0  # Remove delay to complete backfill
            }
            self.src_master_rest.set_xdcr_params("default", "default", updated_setting_val_map)
            self.sleep(30, "Wait for backfill to complete")

            self._wait_for_replication_to_catchup()

        except Exception as e:
            self.log.error(f"Test failed with error: {str(e)}")
            raise
        finally:
            # Cleanup will be handled by verify_results and tearDown
            self.log.info("Test completed, cleanup will be handled by framework")