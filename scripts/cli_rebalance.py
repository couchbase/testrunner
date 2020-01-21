
import getopt
import copy
import logging
import sys
from threading import Thread
from datetime import datetime
import socket
import queue

sys.path = [".", "lib"] + sys.path
import testconstants
import time
from builds.build_query import BuildQuery
import logging.config
from membase.api.exception import ServerUnavailableException
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from testconstants import MV_LATESTBUILD_REPO
from testconstants import SHERLOCK_BUILD_REPO
from testconstants import COUCHBASE_REPO
import TestInput


logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()

def usage():
    print("Please provide ini file")

def main():
    log_install_failed = "some nodes were not install successfully!"
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hi:p:', [])
        for o, a in opts:
            if o == "-h":
                usage()

        if len(sys.argv) <= 1:
            usage()

        input = TestInput.TestInputParser.get_test_input(sys.argv)
        if not input.servers:
            usage("ERROR: no servers specified. Please use the -i parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError as err:
        usage("ERROR: " + str(err))
    print(input)


    cli_command = "rebalance"
    if "rebalance_in" in input.test_params:
        # add upgraded nodes in the cluster
        # Assumption 4 nodes in ini file and add nodes from last node upwards
        remote_client = RemoteMachineShellConnection(input.servers[0])
        for server in input.servers[2:]:
            print(server.ip)
            options = "--server-add={0}:8091".format(server.ip) + " --server-add-username=Administrator --server-add-password=password"
            output, error = remote_client.execute_couchbase_cli(cli_command, options=options, cluster_host=input.servers[0].ip, cluster_port=server.port, user=server.rest_username, password=server.rest_password)
            print(output, error)
            time.sleep(5)

    if "rebalance_out" in input.test_params:
        # remove old build nodes out from the cluster
        # Assumption 4 nodes and remove nodes from the top
        cli_command = "rebalance"
        remote_client = RemoteMachineShellConnection(input.servers[2])
        for server in input.servers[:2]:
            print(server.ip)
            options = "--server-remove={0}:8091".format(server.ip) + " --server-add-username=Administrator --server-add-password=password"
            output, error = remote_client.execute_couchbase_cli(cli_command, options=options, cluster_host=input.servers[2].ip, cluster_port=server.port, user=server.rest_username, password=server.rest_password)
            print(output, error)
            time.sleep(5)



if __name__ == "__main__":
    main()
