#!/usr/bin/env python

# This script provides a hack-ful cluster-level setUp and tearDown
# sandwich around test_FOO() methods.
#
# For example, when jenkins is driving the tests across a cluster, it
# can...
#
# 1) Start a "do_cluster.py <testrunner-style-params> setUp".
#    This will create an EPerfMaster instance and call...
#      EPerfMaster.setUp()
#      EPerfMaster.test_FOO()
#        Next, due to the is_master settings, the
#        EPerfMaster.load_phase() will run, but the
#        EPerfMaster.access_phase() will be a NO-OP
#      Also, tearDown() will be a NO-OP.
#
# 2) Next, jenkins will start N clients, running EPerfClient
#    The 0-th client will be a leader.
#      The leader can do extra work like start rebalances, compaction, etc.
#    The client's setUp() and tearDown()'s will be NO-OP's.
#    Then, after all the clients exit...
#
# 3) Finally, jenkins will call "do_cluster.py" WITHOUT the setUp
#    parameter, which makes EPerfMaster go through tearDown().
#
# At development time, we don't really use this script, and just use
# testrunner, which runs the full
# unittest.TestCase/setUp/testFoo/tearDown lifecycle.
#
import sys
import os

sys.path.append("lib")
sys.path.append("pytests")
sys.path.append(".")

from TestInput import TestInputParser, TestInputSingleton

import performance.eperf as eperf

try:
    os.symlink('testrunner', 'testrunner.py')
except OSError:
    pass

from testrunner import parse_args

_, test_params, _, _, _ = parse_args(sys.argv)
TestInputSingleton.input = TestInputParser.get_test_input(sys.argv)
test_params.update(TestInputSingleton.input.test_params)
TestInputSingleton.input.test_params = test_params

class EPerfMasterWrapper(eperf.EPerfMaster):
    def __init__(self):
        pass

obj = EPerfMasterWrapper()
obj.input = TestInputSingleton.input

# Run setUp with load_phase=0 and access_phase=0
if "setUp" in sys.argv:
    obj.setUp()
else:
    obj.setUpBase0() # This will call tearDown on our behalf.
    num_clients = obj.param("num_clients", 10)
    print num_clients
    obj.aggregate_all_stats(int(num_clients), "load")
    obj.aggregate_all_stats(int(num_clients), "reload")
    obj.aggregate_all_stats(int(num_clients), "loop")
    obj.aggregate_all_stats(int(num_clients), "warmup")
    obj.aggregate_all_stats(int(num_clients), "index")
