#!/usr/bin/env python

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

if "setUp" in sys.argv:
    obj.setUp()
    what = obj.param("test", "test_ept_read")
    meth = getattr(obj, what)
    meth()
else:
    obj.setUpBase0() # This will call tearDown on our behalf.

