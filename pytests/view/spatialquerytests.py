import logger
import time
import unittest
import threading
from threading import Thread
from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.cluster import Cluster
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
import json
import sys

from basetestcase import BaseTestCase

from membase.helper.spatial_helper import SpatialHelper


class SpatialQueryTests(BaseTestCase):
    def setUp(self):
        self.helper = SpatialHelper(self, "default")
        super(SpatialQueryTests, self).setUp()
        self.log = logger.Logger.get_logger()

        self.helper.setup_cluster()
        self.cluster = Cluster()
        self.servers = self.helper.servers

    def tearDown(self):
        super(SpatialQueryTests, self).tearDown()

    def test_simple_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Make limit queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._query_test_init(data_set)

    def test_simple_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Make skip (and limit) queries on a "
                      "simple dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._query_test_init(data_set)

    def test_simple_dataset_bbox_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Make bounding box queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_bbox_queries()
        self._query_test_init(data_set)

    def test_simple_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Make range queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._query_test_init(data_set)

    def test_multidim_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Make limit queries on a multidimensional "
                      "dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._query_test_init(data_set)

    def test_multidim_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Make skip (and limit) queries on a "
                      "multidimensional dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._query_test_init(data_set)

    def test_multidim_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Make range queries on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._query_test_init(data_set)

    def test_multidim_dataset_range_and_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Make range queries with limits on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_and_limit_queries()
        self._query_test_init(data_set)

## Rebalance In
    def test_rebalance_in_simple_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance In and limit queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_in_simple_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance In and skip (and limit) queries on a "
                      "simple dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_in_simple_dataset_bbox_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance In and bounding box queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_bbox_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_in_simple_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance In and range queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_in_multidim_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance In and limit queries on a multidimensional "
                      "dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_in_multidim_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance In and skip (and limit) queries on a "
                      "multidimensional dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_in_multidim_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance In and range queries on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_in_multidim_dataset_range_and_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance In and range queries with limits on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_and_limit_queries()
        self._rebalance_cluster(data_set)

#Rebalance Out
    def test_rebalance_out_simple_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and  limit queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_out_simple_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and skip (and limit) queries on a "
                      "simple dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_out_simple_dataset_bbox_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and bounding box queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_bbox_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_out_simple_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and range queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_out_multidim_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and limit queries on a multidimensional "
                      "dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_out_multidim_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and skip (and limit) queries on a "
                      "multidimensional dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_out_multidim_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and range queries on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._rebalance_cluster(data_set)

    def test_rebalance_out_multidim_dataset_range_and_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and range queries with limits on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_and_limit_queries()
        self._rebalance_cluster(data_set)

# Warmup Tests

    def test_warmup_simple_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Warmup with skip and limit queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._query_test_init_integration(data_set)

    def test_warmup_simple_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Warmup with  skip (and limit) queries on a "
                      "simple dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._query_test_init_integration(data_set)

    def test_warmup_simple_dataset_bbox_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Warmup with  bounding box queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_bbox_queries()
        self._query_test_init_integration(data_set)

    def test_warmup_simple_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Warmup with  range queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._query_test_init_integration(data_set)

    def test_warmup_multidim_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Warmup with  limit queries on a multidimensional "
                      "dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._query_test_init_integration(data_set)

    def test_warmup_multidim_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Warmup with  skip (and limit) queries on a "
                      "multidimensional dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._query_test_init_integration(data_set)

    def test_warmup_multidim_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Warmup with  range queries on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._query_test_init_integration(data_set)

    def test_warmup_multidim_dataset_range_and_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Warmup with  range queries with limits on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_and_limit_queries()
        self._query_test_init_integration(data_set)


# Reboot Tests
    def test_reboot_simple_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Reboot and limit queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._query_test_init_integration(data_set)

    def test_reboot_simple_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Reboot with  skip (and limit) queries on a "
                      "simple dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._query_test_init_integration(data_set)

    def test_reboot_simple_dataset_bbox_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Reboot with  bounding box queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_bbox_queries()
        self._query_test_init_integration(data_set)

    def test_reboot_simple_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Reboot with  range queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._query_test_init_integration(data_set)

    def test_reboot_multidim_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Reboot with  limit queries on a multidimensional "
                      "dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._query_test_init_integration(data_set)

    def test_reboot_multidim_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Reboot with  skip (and limit) queries on a "
                      "multidimensional dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._query_test_init_integration(data_set)

    def test_reboot_multidim_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Reboot with  range queries on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._query_test_init_integration(data_set)

    def test_reboot_multidim_dataset_range_and_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Reboot with  range queries with limits on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_and_limit_queries()
        self._query_test_init_integration(data_set)

# Failover Tests
    def test_failover_simple_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Failover and limit queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._failover_cluster(data_set)

    def test_failover_simple_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and skip (and limit) queries on a "
                      "simple dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._failover_cluster(data_set)

    def test_failover_simple_dataset_bbox_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and bounding box queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_bbox_queries()
        self._failover_cluster(data_set)

    def test_failover_simple_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and range queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._failover_cluster(data_set)

    def test_failover_multidim_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and limit queries on a multidimensional "
                      "dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._failover_cluster(data_set)

    def test_failover_multidim_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and skip (and limit) queries on a "
                      "multidimensional dataset with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._failover_cluster(data_set)

    def test_failover_multidim_dataset_range_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and range queries on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_queries()
        self._failover_cluster(data_set)

    def test_failover_multidim_dataset_range_and_limit_queries(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : Rebalance Out and range queries with limits on a "
                      "multidimensional with {0} docs".format(num_docs))

        data_set = MultidimDataSet(self.helper, num_docs)
        data_set.add_range_and_limit_queries()
        self._failover_cluster(data_set)

    ###
    # load the data defined for this dataset.
    # create views and query the data as it loads.
    # verification is optional, and best practice is to
    # set to False if you plan on running _query_all_views()
    # later in the test case
    ###
    def _query_test_init(self, data_set, verify_results = True):
        views = data_set.views

        # start loading data
        t = Thread(target=data_set.load,
                   name="load_data_set",
                   args=())
        t.start()

        # run queries while loading data
        while(t.is_alive()):
            self._query_all_views(views, False)
            time.sleep(5)
        t.join()

        # results will be verified if verify_results set
        if verify_results:
            self._query_all_views(views, verify_results)
        else:
            self._check_view_intergrity(views)

    def _query_test_init_integration(self, data_set, verify_results = True):
        views = data_set.views
        inserted_keys = data_set.load()
        target_fn = ()

        if self.helper.num_nodes_reboot >= 1:
            target_fn = self._reboot_cluster(data_set)
        elif self.helper.num_nodes_warmup >= 1:
            target_fn = self._warmup_cluster(data_set)
        elif self.helper.num_nodes_to_add >= 1 or self.helper.num_nodes_to_remove >= 1:
            target_fn = self._rebalance_cluster(data_set)

        t = Thread(target=self._query_all_views(views, False))
        t.start()
        # run queries while loading data
        while t.is_alive():
            self._rebalance_cluster(data_set)
            time.sleep(5)
        t.join()

        # results will be verified if verify_results set
        if verify_results:
            self._query_all_views(views, verify_results)
        else:
            self._check_view_intergrity(views)

    ##
    # run all queries for all views in parallel
    ##
    def _query_all_views(self, views, verify_results = True):
        query_threads = []
        for view in views:
            t = RunQueriesThread(view, verify_results)
            query_threads.append(t)
            t.start()

        [t.join() for t in query_threads]

        self._check_view_intergrity(query_threads)

    ##
    # If an error occured loading or querying data for a view
    # it is queued and checked here. Fail on the first one that
    # occurs.
    ##
    def _check_view_intergrity(self, thread_results):
        for result in thread_results:
            if result.test_results.errors:
                self.fail(result.test_results.errors[0][1])
            if result.test_results.failures:
                self.fail(result.test_results.failures[0][1])

    ###
    # Rebalance
    ###
    def _rebalance_cluster(self, data_set):
        if self.helper.num_nodes_to_add >= 1:
            rebalance = self.cluster.async_rebalance(self.servers[:1],
                self.servers[1:self.helper.num_nodes_to_add + 1],
                [])
            self._query_test_init(data_set)
            rebalance.result()

        elif self.helper.num_nodes_to_remove >= 1:
            rebalance = self.cluster.async_rebalance(self.servers[:1], [],
                self.servers[1:self.helper.num_nodes_to_add + 1])
            self._query_test_init(data_set)
            rebalance.result()

    def _failover_cluster(self, data_set):
        failover_nodes = self.servers[1 : self.helper.failover_factor + 1]
        try:
            # failover and verify loaded data
            #self.cluster.failover(self.servers, failover_nodes)
            self.cluster.failover(self.servers, self.servers[1:2])
            self.log.info("120 seconds sleep after failover before invoking rebalance...")
            time.sleep(120)
            rebalance = self.cluster.async_rebalance(self.servers,
                [], self.servers[1:2])

            self._query_test_init(data_set)

            msg = "rebalance failed while removing failover nodes {0}".format(failover_nodes)
            self.assertTrue(rebalance.result(), msg=msg)

            #verify queries after failover
            self._query_test_init(data_set)
        finally:
            self.log.info("Completed the failover testing for spatial querying")

    ###
    # Warmup
    ###
    def _warmup_cluster(self, data_set):
        for server in self.servers[0:self.helper.num_nodes_warmup]:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.start_server()
            remote.disconnect()
            self.log.info("Node {0} should be warming up ".format(server.ip))
            time.sleep(120)
        self._query_test_init(data_set)

    # REBOOT
    def _reboot_cluster(self, data_set):
        try:
            for server in self.servers[0:self.helper.num_nodes_reboot]:
                shell = RemoteMachineShellConnection(server)
                if shell.extract_remote_info().type.lower() == 'windows':
                    o, r = shell.execute_command("shutdown -r -f -t 0")
                    shell.log_command_output(o, r)
                    shell.disconnect()
                    self.log.info("Node {0} is being stopped".format(server.ip))
                elif shell.extract_remote_info().type.lower() == 'linux':
                    o, r = shell.execute_command("reboot")
                    shell.log_command_output(o, r)
                    shell.disconnect()
                    self.log.info("Node {0} is being stopped".format(server.ip))

                    time.sleep(120)
                    shell = RemoteMachineShellConnection(server)
                    command = "/sbin/iptables -F"
                    o, r = shell.execute_command(command)
                    shell.log_command_output(o, r)
                    shell.disconnect()
                    self.log.info("Node {0} backup".format(server.ip))
        finally:
            self.log.info("Warming-up server ..".format(server.ip))
            time.sleep(100)



class View:
    def __init__(self, helper, index_size, fn_str, name='dev_test_view',
                 create_on_init=True):
        self.helper = helper
        self.index_size = index_size
        self.name = name

        self.log = logger.Logger.get_logger()

        # Store failures in here. Don't forget to add them manually,
        # else the failed assertions won't make the whole test fail
        self._test_results = unittest.TestResult()

        # queries defined for this view
        self.queries = []

        if create_on_init:
            self.helper.create_index_fun(name, fn_str)



class SimpleDataSet:
    def __init__(self, helper, num_docs):
        self.helper = helper
        self.num_docs = num_docs
        self.views = self._create_views()
        self.name = "simple_dataset"


    def _create_views(self):
        view_fn = 'function (doc) {if(doc.geometry !== undefined || doc.name !== undefined ) { emit(doc.geometry, doc.name);}}'
        return [View(self.helper, self.num_docs, fn_str = view_fn)]

    def load(self):
        inserted_keys = self.helper.insert_docs(self.num_docs, self.name)
        return inserted_keys

    def add_limit_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper({"limit": 10}, 10),
                QueryHelper({"limit": 3417}, 3417),
                QueryHelper({"limit": view.index_size}, view.index_size),
                QueryHelper({"limit": 5*view.index_size}, view.index_size)]

    def add_skip_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper({"skip": 10}, view.index_size-10),
                QueryHelper({"skip": 2985}, view.index_size-2985),
                QueryHelper({"skip": view.index_size}, 0),
                QueryHelper({"skip": 5*view.index_size}, 0),
                QueryHelper({"skip": 2985, "limit": 1539}, 1539),
                QueryHelper({"skip": view.index_size-120, "limit": 1539}, 120),
                QueryCompareHelper([{"skip": 6210, "limit": 1592}],
                                   [{"skip": 6210, "limit": 1086},
                                    {"skip": 7296, "limit": 506}])
                ]

    def add_bbox_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper({"bbox": "-180,-90,180,90"}, view.index_size),
                QueryHelper({"bbox": "-900,-900,900,900"}, view.index_size),
                QueryHelper({}, view.index_size),
                QueryHelper({"bbox": "-900,-900,900,900"}, view.index_size),
                QueryCompareHelper([{"bbox": "-900,-900,900,900"}],
                                   [{}]),
                QueryCompareHelper([{"bbox": "-117,-76,34,43"}],
                                   [{"bbox": "-117,-76,34,-5"},
                                    {"bbox": "-117,-5,34,43"}]),
                ]

    def add_range_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper(
                    {"start_range": [-180, -90], "end_range": [180, 90]},
                    view.index_size),
                QueryHelper(
                    {"start_range": [-900, -900], "end_range": [900, 900]},
                    view.index_size),
                QueryCompareHelper([{"start_range": [-900, -900],
                                     "end_range": [900, 900]}],
                                   [{}]),
                QueryCompareHelper([{"start_range": [-117, -76],
                                     "end_range": [34, 43]}],
                                   [{"start_range": [-117, -76],
                                     "end_range": [34, -5]},
                                    {"start_range": [-117, -5],
                                     "end_range": [34, 43]}])
                ]

    def add_all_query_sets(self):
        self.add_limit_queries()
        self.add_skip_queries()
        self.add_bbox_queries()
        self.add_range_queries()


class MultidimDataSet:
    def __init__(self, helper, num_docs):
        self.helper = helper
        self.num_docs = num_docs
        self.views = self._create_views()
        self.name = "multidim_dataset"

    def _create_views(self):
        view_fn = '''function (doc) {
    if (doc.age !== undefined || doc.height !== undefined ||
            doc.bloom !== undefined || doc.shed_leaves !== undefined) {
        emit([doc.age, doc.height, [doc.bloom, doc.shed_leaves]], doc.name);
    }}'''
        return [View(self.helper, self.num_docs, fn_str = view_fn)]

    def load(self):
        inserted_keys = self.helper.insert_docs(self.num_docs, self.name)
        return inserted_keys

    def add_limit_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper({"limit": 10}, 10),
                QueryHelper({"limit": 3417}, 3417),
                QueryHelper({"limit": view.index_size}, view.index_size),
                QueryHelper({"limit": 5*view.index_size}, view.index_size)]

    def add_skip_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper({"skip": 10}, view.index_size-10),
                QueryHelper({"skip": 2985}, view.index_size-2985),
                QueryHelper({"skip": view.index_size}, 0),
                QueryHelper({"skip": 5*view.index_size}, 0),
                QueryHelper({"skip": 2985, "limit": 1539}, 1539),
                QueryHelper({"skip": view.index_size-120, "limit": 1539}, 120),
                QueryCompareHelper([{"skip": 6210, "limit": 1592}],
                                   [{"skip": 6210, "limit": 1086},
                                    {"skip": 7296, "limit": 506}])
                ]

    def add_range_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper(
                    {"start_range": [0, 0, 0],
                     "end_range": [1001, 13001, 13]},
                    view.index_size),
                QueryHelper(
                    {"start_range": [None, 0, None],
                     "end_range": [1001, None, None]},
                    view.index_size),
                QueryHelper(
                    {"start_range": [500, 2000, 3],
                     "end_range": [800, 11111, 9]},
                    2066),
                QueryHelper(
                    {"start_range": [500, -500, 3],
                     "end_range": [800, 11111, 9]},
                    2562),
                QueryCompareHelper(
                    [{"start_range": [500, -500, 3],
                      "end_range": [800, 11111, 9]}],
                    [{"start_range": [500, None, 3],
                      "end_range": [800, 11111, 9]}]),
                QueryCompareHelper(
                    [{"start_range": [500, -500, 3],
                      "end_range": [800, 11111, 9]}],
                    [{"start_range": [500, None, 3],
                      "end_range": [800, None, 9]}]),
                QueryCompareHelper(
                    [{"start_range": [500, 2000, 3],
                      "end_range": [800, 11111, 9]}],
                    [{"start_range": [500, 2000, 3],
                                     "end_range": [600, 8000, 9]},
                     {"start_range": [500, 8000, 3],
                      "end_range": [600, 11111, 9]},
                     {"start_range": [600, 2000, 3],
                      "end_range": [800, 11111, 9]}])
                ]

    def add_range_and_limit_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper(
                    {"start_range": [0, 0, 0],
                     "end_range": [1001, 13001, 13],
                     "limit": self.num_docs // 2},
                     self.num_docs // 2),
                QueryHelper(
                    {"start_range": [None, 0, None],
                     "end_range": [1001, None, None],
                     "limit": self.num_docs // 2},
                    self.num_docs // 2),
                QueryHelper(
                    {"start_range": [500, 2000, 3],
                     "end_range": [800, 11111, 9],
                     "limit": 1000},
                   1000),
                QueryHelper(
                    {"start_range": [500, -500, 3],
                     "end_range": [800, 11111, 9],
                     "limit": 5},
                    5),
                QueryCompareHelper(
                    [{"start_range": [500, 1800, 3],
                      "end_range": [800, 11111, 9]}],
                    [{"start_range": [500, 1800, 3],
                      "end_range": [800, 11111, 9],
                      "limit": 700},
                     {"start_range": [500, 1800, 3],
                      "end_range": [800, 11111, 9],
                      "skip": 700,
                      "limit": 100},
                     {"start_range": [500, 1800, 3],
                                     "end_range": [800, 11111, 9],
                      "skip": 800,
                      "limit": 10000},
                     ]),
                ]

    def add_all_query_sets(self):
        self.add_limit_queries()
        self.add_skip_queries()
        self.add_range_queries()
        self.add_range_and_limit_queries()


class QueryHelper:
    def __init__(self, params, expected_num_docs):
        self.params = params

        # number of docs this query should return
        self.expected_num_docs = expected_num_docs

# Put in two lists of queries, it will then join the results of the
# individual queries and compare both
class QueryCompareHelper:
    def __init__(self, queries_a, queries_b):
        self.queries_a = queries_a
        self.queries_b = queries_b



class RunQueriesThread(threading.Thread):
    def __init__(self, view, verify_results = False):
        threading.Thread.__init__(self)
        self.view = view
        self.verify_results = verify_results
        # The last retrieved results, useful when an exception happened
        self._last_results = None

        # Store failures in here. So we can make the whole test fail,
        # normally only this thread will fail
        self.test_results = unittest.TestResult()

        self.helper = self.view.helper
        self.log = self.view.log

    def run(self):
        if not len(self.view.queries) > 0 :
            self.log.info("No queries to run for this view")
            return

        try:
            self._run_queries()
        except Exception:
            self.log.error("Last query result:\n\n{0}\n\n"\
                               .format(json.dumps(self._last_results,
                                                  sort_keys=True)))
            self.test_results.addFailure(self.helper.testcase, sys.exc_info())

    def _run_queries(self):
        for query in self.view.queries:
            # Simple query
            if isinstance(query, QueryHelper):
                if self.verify_results:
                    self._last_results = self._run_query(
                        query.params, query.expected_num_docs)
                else:
                    self._last_results = self._run_query(query.params)
            # Compare queries, don't verify the individual queries
            # but only the final result
            elif isinstance(query, QueryCompareHelper):
                result_keys_a = []
                result_keys_b = []
                for params in query.queries_a:
                    self._last_results = self._run_query(params)
                    result_keys_a.extend(
                        self.helper.get_keys(self._last_results))

                for params in query.queries_b:
                    self._last_results = self._run_query(params)
                    result_keys_b.extend(
                        self.helper.get_keys(self._last_results))

                if self.verify_results:
                    diff = set(result_keys_a) - set(result_keys_b)
                    self.helper.testcase.assertEqual(diff, set())
            else:
                self.helper.testcase.fail("no queries specified")

    # If expected_num_docs is given, the results are verified
    def _run_query(self, query_params, expected_num_docs=None):
        params = {"debug": True}
        params.update(query_params)

        if expected_num_docs is not None:
            self.log.info("Quering view {0} with params: {1}".format(
                    self.view.name, params));
            results = self.helper.get_results(self.view.name, None, params)
            num_keys = len(self.helper.get_keys(results))
            self.log.info("{0}: retrieved value {1} expected: {2}"\
                              .format(self.view.name, num_keys,
                                      expected_num_docs));

            if(num_keys != expected_num_docs):
                error = "Query failed: {0} Documents Retrieved, "\
                    "expected {1}".format(num_keys, expected_num_docs)
                try:
                    self.helper.testcase.assertEqual(num_keys,
                                                      expected_num_docs,
                                                      error)
                except Exception:
                    self.log.error(error)
                    raise
            else:
                return results
        else:
            # query without verification
            self.log.info("Quering view {0} with params: {1}"\
                              .format(self.view.name, params));
            return self.helper.get_results(self.view.name, None, params)
