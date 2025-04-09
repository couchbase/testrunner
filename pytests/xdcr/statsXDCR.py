import json
from time import sleep
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.couchbase_helper.documentgenerator import BlobGenerator
from lib.membase.api.rest_client import RestConnection
from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest
import re
import subprocess
import multiprocessing
import os

class StatsXDCR(XDCRNewBaseTest):
    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.initial_load = self._input.param("inital_load", False)
        self.verify_backfill = self._input.param("verify_backfill", False)

    def load_binary_docs_using_cbc_pillowfight(self, server, items, bucket, batch=1000, docsize=100, rate_limit=100000, scope="_default", collection="_default", command_timeout = 10):
        server_shell = RemoteMachineShellConnection(server)
        cmd = f"/opt/couchbase/bin/cbc-pillowfight -u Administrator -P password -U couchbase://localhost/"\
            f"{bucket} -I {items} -m {docsize} -M {docsize} -B {batch} --rate-limit={rate_limit} --populate-only --collection {scope}.{collection}"
        self.log.info("Executing '{0}'...".format(cmd))
        output, error  = server_shell.execute_command(cmd, timeout=command_timeout, use_channel=True)
        if error :
            self.log.info("Error: {0}".format(error))
            self.fail(f"Failed to load docs in source cluster in {bucket}.{scope}.{collection}")
        server_shell.disconnect()
        self.log.info(f"Data loaded into {bucket}.{scope}.{collection} successfully")

    def _extract_stats(self, data, stat):
        """
        Extracts specified stats from Prometheus metrics data.
        Returns:
            list: A list of dictionaries, where each dictionary represents a metric
                  and contains 'labels' (dictionary) and 'value' (float or int).
        """
        result = []
        lines = data.strip().split('\n')
        for line in lines:
            if line.startswith(stat):
                match = re.match(r'([a-zA-Z0-9_]+)\s*\{(.*?)\}\s*(-?\d+(\.\d+)?([eE][-+]?\d+)?)', line)
                if not match:
                    match = re.match(r'([a-zA-Z0-9_]+)\s+(-?\d+(\.\d+)?([eE][-+]?\d+)?)', line)
                    if not match:
                        continue  # skip if no match.

                if match.group(2):  # labels exist
                    labels_str = match.group(2)
                    value = float(match.group(3))
                    labels = {}
                    label_pairs = re.findall(r'([a-zA-Z0-9_]+)="([^"]+)"', labels_str)
                    for key, val in label_pairs:
                        labels[key] = val
                    result.append({"labels": labels, "value": value})
                else:  # no labels
                    value = float(match.group(2))
                    result.append({"labels": {}, "value": value})
        return result


    def get_stats(self,server, stat_name):
        server_rest = RestConnection(server)
        api = server_rest.baseUrl + "metrics"
        status, content, _ = server_rest._http_request(api=api, method="GET", timeout=30)
        if not status:
            self.fail(f"Could not retrieve stats from {api}")
        try:
            content = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                content = content.decode('latin-1')
            except UnicodeDecodeError:
                    self.fail(f"Exception  while decoding response from {api}")
        extracted = self._extract_stats(content, stat_name)
        return extracted

    def test_xdcr_data_replicated_uncompress(self):
        if self.initial_load:
            self.load_and_setup_xdcr()
        else:
            self.setup_xdcr_and_load()
        # verify replication and stats
        src_rest = RestConnection(self.src_master)
        outgoing_repls = self.get_outgoing_replications(src_rest)
        if outgoing_repls is None:
            self.fail("No outgoing replication found on src cluster")
        doc_gen = BlobGenerator("docs", "seed", 512, end=1000)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=doc_gen)
        self.wait_interval(10, "Wait for replication stats to be updated")
        stats = self.get_stats(self.src_master, "xdcr_data_replicated_uncompress_bytes")
        metrics = {}
        for data in stats:
            pipelineType = data['labels']['pipelineType']
            value = data['value']
            self.log.info(f"Pipeline type: {pipelineType}, Value:{value}")
            metrics[pipelineType] = value
        self.assertTrue(metrics['Main']>0.0, "Expected metrics value did not match")

        # pause the replication
        self.src_cluster.pause_all_replications()
        self.wait_interval(20, "Wait for replication stats to be updated")
        # verify it went to zero
        stats = self.get_stats(self.src_master, "xdcr_data_replicated_uncompress_bytes")

        for data in stats:
            pipelineType = data['labels']['pipelineType']
            value = data['value']
            self.log.info(f"Pipeline type: {pipelineType}, Value:{value}")
            metrics[pipelineType] = value
        self.assertTrue(metrics['Main']==0.0, "Expected metrics value did not match")

    def test_xdcr_data_replicated_uncompress_backfill(self):
        new_scope = "new_scope"
        new_collection = "new_collection"

        self.setup_xdcr()

        src_rest = RestConnection(self.src_master)
        dest_rest = RestConnection(self.dest_master)

        status = src_rest.create_scope("default",new_scope)
        if not status:
            self.fail("Failed to create scope on src")
        status = src_rest.create_collection("default", new_scope, new_collection)
        if not status:
            self.fail("Failed to create collection on src")
        self.load_binary_docs_using_cbc_pillowfight(self.src_master, 100000, "default", 100, 300)
        self.load_binary_docs_using_cbc_pillowfight(self.src_master, 10000, "default", 100, 300, scope=new_scope, collection=new_collection)

        self.wait_interval(10, "Waiting for docs to be inserted")

        # verify replication and stats
        outgoing_repls = self.get_outgoing_replications(src_rest)
        if outgoing_repls is None:
            self.fail("No outgoing replication found on src cluster")

        status = dest_rest.create_scope("default",new_scope)
        if not status:
            self.fail("Failed to create scope on dest")
        status = dest_rest.create_collection("default", new_scope, new_collection)
        if not status:
            self.fail("Failed to create collection on dest")
        self.load_binary_docs_using_cbc_pillowfight(self.src_master, 10000, "default", 100, 300)
        self.load_binary_docs_using_cbc_pillowfight(self.dest_master, 1000, "default", 100, 300, scope=new_scope, collection=new_collection)
        self.load_binary_docs_using_cbc_pillowfight(self.src_master, 1000, "default", 100, 300, scope=new_scope, collection=new_collection)
        self.wait_interval(90, "Wait for replication stats to be updated")
        stats = self.get_stats(self.src_master, "xdcr_data_replicated_uncompress_bytes")
        # stats = self.get_stats(self.dest_master, "xdcr_data_replicated_uncompress_bytes")
        metrics = {}
        for data in stats:
            pipelineType = data['labels']['pipelineType']
            value = data['value']
            self.log.info(f"Pipeline type: {pipelineType}, Value:{value}")
            metrics[pipelineType] = value

        self.assertTrue(metrics['Backfill'] > 0.0, "Expected metrics value did not match for backfill pipeline")

        # verify it went to zero
        self.src_cluster.pause_all_replications()
        self.wait_interval(20, "Wait for replication stats to be updated")
        stats = self.get_stats(self.src_master, "xdcr_data_replicated_uncompress_bytes")

        for data in stats:
            pipelineType = data['labels']['pipelineType']
            value = data['value']
            self.log.info(f"Pipeline type: {pipelineType}, Value:{value}")
            metrics[pipelineType] = value

        self.assertTrue(metrics['Main'] == 0.0, "Expected metrics value did not match main pipeline")
        self.assertTrue(metrics['Backfill'] == 0.0, "Expected metrics value did not match backfill pipeline")