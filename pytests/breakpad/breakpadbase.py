import os
import re
import time
import subprocess
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from TestInput import TestInputSingleton
from lib.cluster_run_manager  import CRManager
from .logpoll import NSLogPoller
from .constants import MD_PATH, NS_NUM_NODES
from memcached.helper.data_helper import VBucketAwareMemcached

class BreakpadBase(BaseTestCase):


    def __init__(self, args):
        super(BreakpadBase, self).__init__(args)
        self.is_setup = False
        self.crm = None
        self.input = TestInputSingleton.input
        self.use_cluster_run = self.input.param('dev', True)
        self.test = self.input.param('test', None)
        self.log_pollers  = []
        self.num_nodes = 0

        self.doc_num = 0
        if self.use_cluster_run:
            self.num_nodes = self.input.param('num_nodes', NS_NUM_NODES)
            self.crm = CRManager(self.num_nodes, 0)


    def setUp(self):

        if self.test:
            if self.test !=  self._testMethodName:
                self.skipTest("disabled")

        self.is_setup = True

        if self.use_cluster_run:
            assert self.crm.clean()
            assert self.crm.start_nodes()

        # poll logs until bucket ready on each node
        for i in range(self.num_nodes):
            logp = NSLogPoller(i)
            logp.start()
            logp.setNSStartedEventFlag(True)
            assert logp.getEventQItem()
            self.log_pollers.append(logp)


        super(BreakpadBase, self).setUp()
        self.is_setup = False

    def tearDown(self):

        if self.use_cluster_run and not self.is_setup:
            assert self.crm.stop_nodes()
            self.cluster.shutdown(force=True)
            # join polling threads which indicate node exited
            for i in range(self.num_nodes):
                self.log_pollers[i].join(30)
        else:
            super(BreakpadBase, self).tearDown()


    def load_docs(self, node, num_docs, bucket = 'default', password = '',
                  exp = 0, flags = 0):

        client = VBucketAwareMemcached(RestConnection(node), bucket)
        for i in range(num_docs):
            key = "key%s"%i
            rc = client.set(key, 0, 0, "value")

    def kill_memcached(self, index, sig=6, wait=10):  #NIX
        killed = False
        pid = self.mc_pid(index)
        if pid is None:
            return False# no pid to kill

        #kill
        p4 = subprocess.Popen(["kill", "-"+str(sig), pid],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        rc, err = p4.communicate()
        err=err.rstrip()
        if err == '':
            killed = True
            time.sleep(5)
        return killed

    def mc_pid(self, index):
        pid = None
        node = "n_"+str(index)
        p = subprocess.Popen(["pgrep", "-a", "memcached"],
                                    stdout=subprocess.PIPE)
        rv, err = p.communicate()
        if err is None:
            m = re.search('([0-9]+) .*'+node, rv)
            if m:
                pid = m.group(1)

        return pid

    def dmp_to_core(self, dmp_path):
        f_core = dmp_path+'.core'
        f = open(f_core, 'w')
        cmd = subprocess.Popen(
                 [MD_PATH+"/minidump-2-core", dmp_path],
                 stdout = f)
        return f_core

    def verify_core(self, f_core):

        pc = subprocess.Popen(["file", f_core], stdout=subprocess.PIPE)
        f_info, err = pc.communicate()
        f_info = f_info.rstrip()
        return "core file" in f_info
