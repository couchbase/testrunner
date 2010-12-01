#!/usr/bin/env python

#
#     Copyright 2010 NorthScale, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

# PYTHONPATH needs to be set up to point to mc_bin_client

import json
import mc_bin_client
from testrunner_common import *


def usage(err=None):
    if err:
        print "Error: %s\n" % (err)
        r = 1
    else:
        r = 0
    print """\
vbmap_vbmigrator_validation.py
 -h --help
 -v --verbose
 -s --servers <server1,server2,...,serverN>  List of servers to create a cluster with
 -c --create                                 Create cluster (requires passwordless ssh access)
 -r --replicas <count>
 -b --vbuckets <count>
 -u --username <username>                    Username for master
 -p --password <password>                    Password for master
 -m --rpm <rpm file>                         rpm file to install
"""

    sys.exit(r)


def parse_args(argv):
    config = Config()

    try:
        (opts, args) = getopt.getopt(argv[1:],
                                     'Vhvs:cr:b:u:p:i:m:', [
                'version',
                'help',
                'verbose',
                'servers=',
                'create',
                'replicas=',
                'vbuckets=',
                'username=',
                'password=',
                'rpm=',
                ])
    except IndexError:
        usage()
    except getopt.GetoptError, err:
        usage(err)

    for o, a in opts:
        if o in ("-V", "--version"):
            version()
        elif o in ("-h", "--help"):
            usage()
        elif o in ("-v", "--verbose"):
            config.verbose = True
        elif o in ("-s", "--servers"):
            for s in " ".join(a.split(",")).split(" "):
                config.servers.append(Server(s))
        elif o in ("-c", "--create"):
            config.create = True
        elif o in ("-r", "--replicas"):
            config.replicas = int(a)
        elif o in ("-b", "--vbuckets"):
            config.vbuckets = int(a)
        elif o in ("-u", "--username"):
            config.username = a
        elif o in ("-p", "--password"):
            config.password = a
        elif o in ("-m", "--rpm"):
            config.rpm = a
            m = re.search('membase-server-enterprise_x86(_64)?_([.a-zA-Z0-9]*)',a)
            config.server_version = m.group(2).replace(".rpm","")
        else:
            assert False, "unhandled option"

    if len(config.servers) == 0:
        usage("no servers specified")

    return config


if __name__ == "__main__":
    config = parse_args(sys.argv)
    initialize_membase_cluster(config)

    print "Checking the vbucket map from the ns server"
    result = ""
    # Retrieve the vbucket map from the ns server
    f=os.popen("curl -u %s:%s http://%s:%s/pools/default/buckets/default" % (config.username, config.password, config.servers[0].host, config.servers[0].http_port))
    for line in f.readlines():
        result = result + line
    decoded = json.loads(result)

    passed = True
    server_list = decoded['vBucketServerMap']['serverList']
    # Check the number of servers on the map
    if len(config.servers) != len(server_list):
        passed = False

    vb_map = decoded['vBucketServerMap']['vBucketMap']
    # Check the number of vbuckets on the map
    if config.vbuckets != len(vb_map):
        passed = False

    vb_replication_map = {}
    # Verify the vbucket map has the appropriate number of replicas and
    # that there are no loops or duplicates
    for vb_id in range(len(vb_map)):
        if (config.replicas + 1) != len(vb_map[vb_id]):
            passed = False
            continue
        server_set = set()
        prev_server_id = -1
        for server_id in vb_map[vb_id]:
            if server_id >= len(server_list) or server_id in server_set:
                passed = False
                break
            server_set.add(server_id)
            if prev_server_id != -1:
                key = str(vb_id) + "-" + server_list[prev_server_id] + "-" + server_list[server_id]
                vb_replication_map[key] = True
            prev_server_id = server_id

    if passed == True:
        print "VBucket map: Passed"
    else:
        config.return_code = 1
        print "VBucket map: Failed"

    print "Checking vbucket migrator processes on each node against VBucket map"
    # Verify vbucketmigrator processes running on each node are correctly
    # configured based on the vbucket map
    vb_replica_nums = [0] * config.vbuckets
    for src_server in server_list:
        server_name = src_server.split(":")[0]
        wait_sec = 0
        server_to_replicaVB_map = []
        # Get the vbucket migrator commands and their arguments from each server
        while wait_sec < 60:
            server_to_replicaVB_map = server_to_replica_vb_map(server_name)
            if len(server_to_replicaVB_map) >= config.replicas:
                break
            wait_sec = wait_sec + 5
            time.sleep(wait_sec)
        # Verify each vbucket replicated from a source host to a destination host
        # is found in the vbucket map
        for (dest_server, replica_vbuckets) in server_to_replicaVB_map:
            for vb_id in replica_vbuckets:
                vid = int(vb_id)
                vb_replica_nums[vid] = vb_replica_nums[vid] + 1
                key = vb_id + "-" + src_server + "-" + dest_server
                if key not in vb_replication_map:
                    passed = False
                    break
            if passed == False:
                break
        if passed == False:
            break

    # Verify the number of replicas for each vbucket aggregated from vbucket migrator
    # commands is equal to the one configured in the cluster
    for vb_id in range(len(vb_replica_nums)):
        if vb_replica_nums[vb_id] != config.replicas:
            passed = False
            break

    if passed == True:
        print "VBucket migrator: Passed"
    else:
        config.return_code = 1
        print "VBucket migrator: Failed"

    sys.exit(config.return_code)
