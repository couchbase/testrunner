import logger
import time
from threading import Thread
import unittest
import random
import exceptions
import socket

from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
import mc_ascii_client


class MoxiTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger().get_logger()
        self.input = TestInputSingleton.input
        self.servers = self.input.servers
        self.master = self.servers[0]
        self.ip = self.master.ip
        self.finished = False
        self.keys = []
        self.keycount = 0
        self.failure_string = ""
        self.bucket_storage = self.input.param("bucket_storage", 'couchstore')

        self.cleanup()

        rest = RestConnection(self.master)
        info = rest.get_nodes_self()
        self.port = info.moxi+1

        rest.init_cluster(username=self.master.rest_username,
                          password=self.master.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        created = BucketOperationHelper.create_multiple_buckets(self.master,
                                                                replica=1,
                                                                bucket_ram_ratio=(2.0 / 3.0),
                                                                howmany=10,
                                                                sasl=False,
                                                                bucket_storage=self.bucket_storage)
        self.assertTrue(created, "bucket creation failed")

        ready = BucketOperationHelper.wait_for_memcached(self.master, "bucket-0")
        self.assertTrue(ready, "wait_for_memcached failed")


    def tearDown(self):
        self.finished = True
        self.cleanup()


    def cleanup(self):
        rest = RestConnection(self.master)
        rest.stop_rebalance()
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster([server])
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)

    def test_ascii_multiget(self):
        connections = self.input.param("connections", 500)
        self.keycount = self.input.param("keycount", 1000)
        if self.keycount < 20:
            self.keycount = 20
        duration = self.input.param("duration", 300)

        mc = mc_ascii_client.MemcachedAsciiClient(self.ip, self.port, timeout=5)

        self.keys = []
        for i in range(self.keycount):
            mc.set("k_"+str(i), 0, 0, "v_"+str(i))
            self.keys.append("k_"+str(i))

        threads = []
        for i in range(connections):
            threads.append(Thread(target=self.ascii_multiget))
        threads.append(Thread(target=self.rebalance))
        threads.append(Thread(target=self.watch_moxi))

        self.log.info("starting load and rebalance")

        for thread in threads:
            thread.start()

        end_time = time.time() + duration
        while time.time() < end_time and not self.finished:
            time.sleep(5)
        self.finished = True

        self.log.info("stopping load and rebalance")

        rest = RestConnection(self.master)
        rest.stop_rebalance()

        for thread in threads:
            thread.join()

        if self.failure_string:
            self.fail(self.failure_string)

    def ascii_multiget(self):
        mct = mc_ascii_client.MemcachedAsciiClient(self.ip, self.port, timeout=2)
        while not self.finished:
            try:
                start = random.randint(0, self.keycount-20)
                mct.getMulti(self.keys[start:start+20])
            except mc_ascii_client.MemcachedError as e:
                mct.close()
                mct = mc_ascii_client.MemcachedAsciiClient(self.ip, self.port, timeout=2)
            except exceptions.EOFError as e:
                mct.close()
                mct = mc_ascii_client.MemcachedAsciiClient(self.ip, self.port, timeout=2)
                time.sleep(1)
            except socket.error as e:
                mct.close()
                mct = mc_ascii_client.MemcachedAsciiClient(self.ip, self.port, timeout=2)
                time.sleep(1)
            except Exception as e:
                self.finished = True
                raise e

    def rebalance(self):
        while not self.finished:
            ClusterOperationHelper.begin_rebalance_in(self.master, self.servers)
            ClusterOperationHelper.end_rebalance(self.master)
            if not self.finished:
                ClusterOperationHelper.begin_rebalance_out(self.master, self.servers[-1:])
                ClusterOperationHelper.end_rebalance(self.master)

    def watch_moxi(self):
        shell = RemoteMachineShellConnection(self.master)
        moxi_pid_start, _ = shell.execute_command("pgrep moxi")
        while not self.finished:
            moxi_pid, _ = shell.execute_command("pgrep moxi")
            if moxi_pid_start != moxi_pid:
                self.finished = True
                self.failure_string = "moxi restarted"
                self.log.error("moxi restarted")
            time.sleep(5)
