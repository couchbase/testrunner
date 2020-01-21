"""
Cluster Run Management API

Provides interface to ns_server cluster_run cli.
usage:

    crm = CRManager(num_nodes, start_index)
    rc = crm.start_nodes()
    rc = crm.stop_nodes()

all api method return boolean status and err string

    start_nodes()
        * start num_nodes from specified start_index
        * returns: boolean status

    stop_nodes()
        * stops all the nodes that have been started
        * returns: boolean status

    get_all()
        * get list of all nodes.
        * returns: List of Node Objects with None if not started

    start(index)
        * start a single node at specified index
        * returns: boolean status

    stop(index)
        * stop a single node at specified index
        * returns: boolean status

    get(index)
        * get a single node at specified index.
        * returns Node object None if not started

"""

import os
import signal
import socket
import threading
import logging as log
import subprocess
import platform

log.basicConfig(level=log.INFO)

os.environ.copy()
CTXDIR = os.path.dirname(os.path.abspath(__file__))
NSDIR = "{0}{1}..{1}..{1}ns_server".format(CTXDIR, os.sep)


class CRManager:
    """ cluster run management api """

    def __init__(self, num_nodes, start_index=0):
        self._num_nodes = num_nodes
        self.nodes = {}
        for i in range(num_nodes):
            self.nodes[start_index] = None
            start_index += 1

    def start_nodes(self):

        status = False
        keys = list(self.nodes.keys())
        for index in keys:
            status = self.start(index)
            if not status:
                break

        return status

    def stop_nodes(self):

        status = False
        nodes = self.get_nodes()
        for node in nodes:

            if node is not None:
                status = self.stop(node.index)

                if not status:
                    break

        return status

    def get_nodes(self):
        return list(self.nodes.values())

    def start(self, index):

        status = False
        if CRManager.connect(index):
            log.error("node-{0} already running")

        elif index in self.nodes:
            node = self.get_node(index)
            if node is None:
                node = Node(index)
                self.nodes[index] = node

            # start node
            status = node.start()
            if status is False:
                log.error(
                    "failed to start node-{0} see logs {1}".format(
                        index, node.nslog))
        else:
            log.error("invalid node index: {0}".format(index))

        return status

    def stop(self, index):

        status = False

        node = self.get_node(index)

        if index not in self.nodes:
            log.error("node-{0} does not exist".format(index))
        elif node is None:
            if CRManager.connect(index):
                log.error("node-{0} is running but not managed".format(
                    index))
            else:
                log.info("node-{0} has not been started".format(index))
        else:
            status = node.stop()
            if status:
                del self.nodes[index]
                self.nodes[index] = None
            else:
                log.error(
                    "failed to stop node-{0} see logs {1}".format(
                        index, node.nslog))

        return status

    def get_node(self, index):
        return self.nodes.get(index)

    def clean(self, make="make"):
        """ clean up cluster_run data path """
        clean = False
        curdir = os.getcwd()

        try:
            null = open(os.devnull, 'w')
            os.chdir("{0}{1}build".format(NSDIR, os.sep))
            subprocess.check_call([make, "ns_dataclean"], stdout=null)
            log.info("ns_dataclean successful")
            clean = True
        except subprocess.CalledProcessError as cpex:
            log.error("Error: command {0} failed".format(cpex.cmd))
        except OSError as ex:
            log.error("Error: unable to write to stdout\n{0}".format(ex))
        finally:
            os.chdir(curdir)

        return clean

    @property
    def num_nodes(self):
        return len(list(self.nodes.keys()))

    @staticmethod
    def connect(index):
        """ attempt to make socket connection to node at index """
        port = 9000 + index
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        errno = 0
        try:
            sock.connect((socket.gethostbyname('localhost'), port))
        except Exception as ex:
            errno = ex.errno

        return errno == 0


class Node:
    """ cluster_run process interface """

    def __init__(self, index):

        self.index = index
        self.nslog = "%s/cluster_run_n_%s.log" % (NSDIR, index)
        self.instance = None
        self.ready = threading.Event()

    def start(self):
        log.info("starting node {0}".format(self.index))

        logf = open(self.nslog, 'w')
        args = ["python",
                "cluster_run",
                "--node=%s" % 1,
                "--dont-rename",
                "--start-index=%s" % self.index]
        self.instance = subprocess.Popen(
            args, cwd=NSDIR,
            stdout=logf,
            stderr=logf)
        self.ready.set()
        return self.instance.poll() is None

    def started(self):
        return self.ready.is_set()

    def stop(self):
        log.info("stopping node {0}".format(self.index))

        self.ready.wait()

        if platform.system() == "Windows":
            log.info("Stopping cluster_run with all sub processes")
            pid = str(self.instance.pid)
            args = ["taskkill", "/F", "/T", "/PID", pid]
            if subprocess.call(args):
                log.error("taskkill returned with non-null value")
            else:
                self.instance.wait()
            return True

        self.instance.kill()
        self.instance.wait()
        rc = self.instance.returncode
        expected_rc = signal.SIGKILL * -1
        return (rc == expected_rc)
