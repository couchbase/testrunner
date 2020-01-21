from random import Random, shuffle
import sys
from threading import Thread
sys.path.append('.')
sys.path.append('lib')

from load_runner import LoadRunner

from multiprocessing import Queue
from multiprocessing.process import Process
from TestInput import TestInputParser
import logger
import time
#this process will run al the time unless it is shut down by the control center

#get the node list from nodes_statuses()
# run the mbbackup maybe every 60 minutes and then after getting the backup just delete those files

#request = {'queue':queue,'servers':servers,'interval':600}
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection


def start_load(argv):
    queue = Queue(10)
    test_input = TestInputParser.get_test_input(argv)
    load_info = {
        'server_info': [test_input.servers[0]],
        'memcached_info': {
            'bucket_name': "default",
            'bucket_port': "11210",
            'bucket_password': "",
            },
        'operation_info': {
            'operation_distribution': {'set': 10},
            'valuesize_distribution': {20: 30, 30: 5, 25: 5},
            'create_percent': 25,
            'threads': 6,
            },
        'limit_info': {
            'max_items': 0,
            'operation_count': 0,
            'time': time.time() + 24 * 60 * 60,
            'max_size': 0,
            },
        }
    thread = Thread(target=loadrunner, args=(queue, test_input.servers, load_info))
    thread.start()
    time.sleep(24 * 60 * 60)
    queue.put("stop")


def loadrunner(queue, servers, load_info):
    interval = 2 * 60 * 60
    params = {"queue": queue, "servers": servers, "interval": interval, "load_info": load_info}
    print(params)
    runner = LoadRunnerProcess(params=params)
    runner.load()
    time.sleep(24 * 60 * 60)


class LoadRunnerProcess(object):
    def __init__(self, params):
        self.queue = params["queue"]
        self.servers = [params["servers"][0]]
        self.interval = params["interval"]
        self.log = logger.Logger.get_logger()
        self.load_info = params["load_info"]

    def load(self):
        loader = LoadRunner(self.load_info)
        loader.start()
        time.sleep(6000)
        loader.stop()
        loader.wait()


################################################################################

def start_backup(argv):
    queue = Queue(10)
    test_input = TestInputParser.get_test_input(argv)
    thread = Thread(target=backup, args=(queue, test_input.servers))
    thread.start()
    time.sleep(24 * 60 * 60)
    queue.put("stop")


def backup(queue, servers):
    interval = 2 * 60 * 60
    params = {"queue": queue, "servers": servers, "interval": interval}
    backup = BackupProcess(params=params)
    backup.backup()


class BackupProcess(object):
    def __init__(self, params):
        self.queue = params["queue"]
        self.servers = params["servers"]
        self.interval = params["interval"]
        self.log = logger.Logger.get_logger()

    def backup(self):
        while True:
            try:
                x = self.queue.get_nowait()
                self.log.info("get_nowait : {0}".format(x))

                break
                #things are notmal just do another back aafter
                #waiting for self.interval
            except Exception:
                master = self.servers[0]
                rest = RestConnection(master)
                nodes = rest.node_statuses()
                map = self.node_server_map(nodes, self.servers)
                self.log.info("cluster has {0} nodes".format(len(nodes)))
                for node in nodes:
                    try:
                        from Crypto.Random import atfork
                        atfork()
                        BackupHelper(map[node]).backup('default', "/tmp")
                        BackupHelper(map[node]).backup('default', "/tmp")
                    except Exception as ex:
                        print(ex)
                    self.log.info("backed up the data into ")
                time.sleep(self.interval)


    def node_server_map(self, nodes, servers):
        map = {}
        for node in nodes:
            for server in servers:
                if node.ip == server.ip:
                    self.log.info("node.ip : {0} , server.ip : {1}".format(node.ip, server.ip))
                    map[node] = server
                    break
        return map

        def monitor(self):
            pass
            #a thread function which monitor vital system stats during mbbackup
            #to see if the ops per second drops signifcantly


class BackupHelper(object):
    def __init__(self, serverInfo):
        self.server = serverInfo
        self.log = logger.Logger.get_logger()
        self.shell = RemoteMachineShellConnection(self.server)


    #data_file = default-data/default
    def backup(self, bucket, backup_location):
        node = RestConnection(self.server).get_nodes_self()
        mbbackup_path = "{0}/{1}".format(self.server.cli_path, "mbbackup")
        data_directory = "{0}/{1}-{2}/{3}".format(node.storage[0].path, bucket, "data", bucket)
        command = "{0} {1} {2}".format(mbbackup_path,
                                       data_directory,
                                       backup_location)
        output, error = self.shell.execute_command(command)
        self.shell.log_command_output(output, error)


def start_combo(argv):
    queue = Queue(10)
    test_input = TestInputParser.get_test_input(argv)
    thread = Thread(target=combo, args=(queue, test_input))
    thread.start()
    time.sleep(24 * 60 * 60)
    queue.put("stop")


def combo(queue, input):
    combo = ComboBaseTests(input)
    combo.loop()


class ComboBaseTests(object):
    # start from 1..n
    # then from no failover x node and rebalance and
    # verify we did not lose items
    @staticmethod
    def choose_nodes(master, nodes, howmany):
        selected = []
        for node in nodes:
            if not ComboBaseTests.contains(node.ip, master.ip) and\
               not ComboBaseTests.contains(node.ip, '127.0.0.1'):
                selected.append(node)
                if len(selected) == howmany:
                    break
        return selected

    @staticmethod
    def contains(string1, string2):
        if string1 and string2:
            return string1.find(string2) != -1
        return False

    def __init__(self, input):
        self._input = input
        self._servers = self._input.servers
        self.log = logger.Logger.get_logger()

    def loop(self):
        duration = 2400
        replica = 1
        load_ratio = 5
        if 'duration' in self._input.test_params:
            duration = int(self._input.test_params['duration'])
        if 'replica' in self._input.test_params:
            replica = int(self._input.test_params['replica'])
        self.common_test_body(replica, load_ratio, duration)

    def common_test_body(self, replica, load_ratio, timeout=10):
        log = logger.Logger.get_logger()
        start_time = time.time()
        log.info("replica : {0}".format(replica))
        log.info("load_ratio : {0}".format(load_ratio))
        master = self._servers[0]
        log.info('picking server : {0} as the master'.format(master))
        rest = RestConnection(master)
        while time.time() < ( start_time + 60 * timeout):
            #rebalance out step nodes
            #let's add some items ?
            nodes = rest.node_statuses()
            delta = len(self._servers) - len(nodes)
            if delta > 0:
                if delta > 1:
                    how_many_add = Random().randint(1, delta)
                else:
                    how_many_add = 1
                self.log.info("going to add {0} nodes".format(how_many_add))
                self.rebalance_in(how_many=how_many_add)
            else:
                self.log.info("all nodes already joined the cluster")
            time.sleep(30 * 60)
            #dont rebalance out if there are not too many nodes
            if len(nodes) >= (3.0 / 4.0 * len(self._servers)):
                nodes = rest.node_statuses()
                how_many_out = Random().randint(1, len(nodes) - 1)
                self.log.info("going to remove {0} nodes".format(how_many_out))
                self.rebalance_out(how_many=how_many_out)

    def rebalance_out(self, how_many):
        msg = "choosing three nodes and rebalance them out from the cluster"
        self.log.info(msg)
        rest = RestConnection(self._servers[0])
        nodes = rest.node_statuses()
        nodeIps = [node.ip for node in nodes]
        self.log.info("current nodes : {0}".format(nodeIps))
        toBeEjected = []
        toBeEjectedServers = []
        selection = self._servers[1:]
        shuffle(selection)
        for server in selection:
            for node in nodes:
                if server.ip == node.ip:
                    toBeEjected.append(node.id)
                    toBeEjectedServers.append(server)
                    break
            if len(toBeEjected) == how_many:
                break
        if len(toBeEjected) > 0:
            self.log.info("selected {0} for rebalance out from the cluster".format(toBeEjected))
            otpNodes = [node.id for node in nodes]
            started = rest.rebalance(otpNodes, toBeEjected)
            msg = "rebalance operation started ? {0}"
            self.log.info(msg.format(started))
            if started:
                result = rest.monitorRebalance()
                msg = "successfully rebalanced out selected nodes from the cluster ? {0}"
                self.log.info(msg.format(result))
                for server in toBeEjectedServers:
                    shell = RemoteMachineShellConnection(server)
                    try:
                        shell.stop_membase()
                    except:
                        pass
                    try:
                        shell.start_membase()
                    except:
                        pass
                    shell.disconnect()
                    RestHelper(RestConnection(server)).is_ns_server_running()
                    #let's restart membase on those nodes
                return result
        return True

    def rebalance_in(self, how_many):
        rest = RestConnection(self._servers[0])
        nodes = rest.node_statuses()
        #choose how_many nodes from self._servers which are not part of
        # nodes
        nodeIps = [node.ip for node in nodes]
        self.log.info("current nodes : {0}".format(nodeIps))
        toBeAdded = []
        selection = self._servers[1:]
        shuffle(selection)
        for server in selection:
            if not server.ip in nodeIps:
                toBeAdded.append(server)
            if len(toBeAdded) == how_many:
                break

        for server in toBeAdded:
            rest.add_node('Administrator', 'password', server.ip)
            #check if its added ?
        nodes = rest.node_statuses()
        otpNodes = [node.id for node in nodes]
        started = rest.rebalance(otpNodes, [])
        msg = "rebalance operation started ? {0}"
        self.log.info(msg.format(started))
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced out selected nodes from the cluster ? {0}"
            self.log.info(msg.format(result))
            return result
        return False


if __name__ == "__main__":
    process1 = Process(target=start_load, args=(sys.argv,))
    process1.start()
    process2 = Process(target=start_combo, args=(sys.argv,))
    process2.start()
    process3 = Process(target=start_backup, args=(sys.argv,))
    process3.start()
    process1.join()
    process2.join()
    process3.join()
