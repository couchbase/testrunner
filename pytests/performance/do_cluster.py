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

sys.path.append("lib")
sys.path.append("pytests")
sys.path.append(".")

import TestInput

from TestInput import TestInputParser, TestInputSingleton

import performance.eperf as eperf

TestInputSingleton.input = TestInputParser.get_test_input(sys.argv)

class EPerfMasterWrapper(eperf.EPerfMaster):
    def __init__(self):
        pass

obj = EPerfMasterWrapper()
obj.input = TestInputSingleton.input

if "setUp" in sys.argv:
    obj.setUp()
    what = obj.param("test", "test_ept_read")
    meth = getattr(obj, what)
    meth()
else:
    obj.setUpBase0() # This will call tearDown on our behalf.
    #obj.get_all_stats()
