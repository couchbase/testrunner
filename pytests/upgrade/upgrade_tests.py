from basetestcase import BaseTestCase
import json
import os
import zipfile
import pprint
import Queue
import json
import logging
from membase.helper.cluster_helper import ClusterOperationHelper
import mc_bin_client
import threading
from memcached.helper.data_helper import  VBucketAwareMemcached
from mysql_client import MySQLClient
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.tuq_helper import N1QLHelper
from couchbase_helper.query_helper import QueryHelper
from TestInput import TestInputSingleton

class UpgradeTests(BaseTestCase):

    def setUp(self):
        super(UpgradeTests, self).setUp()
        self.initialize_events = self.input.param("initialize_events","server_crash:autofailover").split(":")
        self.in_between_events = self.input.param("in_between_events","server_crash:autofailover").split(":")
        self.after_events = self.input.param("after_events","server_crash:autofailover").split(":")
        self.before_events = self.input.param("before_events","server_crash:autofailover").split(":")
        self.upgrade_type = self.input.param("upgrade_type","online").split(":")
        self.final_events = []

    def tearDown(self):
        super(UpgradeTests, self).tearDown()

    def test_upgrade(self):
        try:
            if self.initialize_events:
                initialize_events = self.run_event(self.initialize_events)
            self.finish_events(initialize_events)
            events = []
            if self.before_events:
                events += self.run_event(self.before_events)
            events += self.upgrade_event()
            if self.in_between_events:
                events += self.run_event(self.in_between_events)
            self.finish_events(events)
            if self.after_events:
                after_events = self.run_event(self.after_events)
            self.finish_events(after_events)
        except Exception, ex:
            self.log.info(ex)
            raise
        finally:
            self.log.info("any events for which we need to cleanup")
            self.cleanup_events()

    def cleanup_events(self):
        thread_list = []
        for event in self.final_events:
            t = threading.Thread(target=self.find_function(event), args = ())
            t.daemon = True
            t.start()
            thread_list.append(t)
        for t in thread_list:
            t.join()

    def run_event(self, events):
        thread_list = []
        for event in events:
            t = threading.Thread(target=self.find_function(event), args = ())
            t.daemon = True
            t.start()
            thread_list.append(t)
        return thread_list

    def find_function(self, event):
        return getattr(self, event)

    def finish_events(self, thread_list):
        for t in thread_list:
            t.join()

    def upgrade_event(self):
        self.log.info("upgrade_event")
        thread_list = []
        if self.upgrade_type == "online":
            t = threading.Thread(target=self.online_upgrade, args = ())
        else:
           t = threading.Thread(target=self.offline_upgrade, args = ())
        t.daemon = True
        t.start()
        thread_list.append(t)
        return thread_list

    def server_crash(self):
        self.log.info("server_crash")

    def server_stop(self):
        self.log.info("server_stop")

    def server_start(self):
        self.log.info("server_start")

    def failover(self):
        self.log.info("failover")

    def failover_add_back(self):
        self.log.info("failover_add_back")

    def autofailover(self):
        self.log.info("autofailover")

    def network_partitioning(self):
        self.log.info("network_partitioning")

    def stop_firewall_on_node(self):
        self.log.info("remove_network_partitioning")

    def couchbase_bucket_compaction(self):
        self.log.info("couchbase_bucket_compaction")

    def warmup(self):
        self.log.info("warmup")

    def bucket_flush(self):
        self.log.info("bucket_flush")

    def rebalance_in(self):
        self.log.info("rebalance_in")

    def rebalance_out(self):
        self.log.info("rebalance_out")

    def swap_rebalance(self):
        self.log.info("swap_rebalance")

    def cb_backup(self):
        self.log.info("cb_backup")

    def create_index(self):
        self.log.info("create_index")

    def create_views(self):
        self.log.info("create_views")

    def drop_views(self):
        self.log.info("drop_views")

    def drop_index(self):
        self.log.info("drop_index")

    def query_with_views(self):
        self.log.info("query_with_views")

    def query_explain(self):
        self.log.info("query_explain")

    def online_upgrade(self):
        self.log.info("online_upgrade")

    def offline_upgrade(self):
        self.log.info("offline_upgrade")

    def kv_ops(self):
        self.log.info("kv_ops")