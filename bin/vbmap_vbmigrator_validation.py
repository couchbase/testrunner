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

import ctypes
import os
import sys
import time
import getopt
import subprocess
import re
import mc_bin_client
import random
import socket
import zlib
import json


class Server(object):
    def __init__(self,host_port):
        hp = host_port.split(":")
        self.host = hp[0]
        self.http_port = 8091
        self.moxi_port = 11211
        self.port = 11210
    def rest_str(self):
        return "%s:%d" % (self.host,self.http_port)
    def __str__(self):
        return "%s:%d" % (self.host,self.port)
    def __repr__(self):
        return "%s:%d" % (self.host,self.port)


class Config(object):
    def __init__(self):
        self.servers = []
        self.create = False
        self.replicas = 1
        self.vbuckets = 1024
        self.username = "Administrator"
        self.password = "password"
        self.verbose = False
        self.items = 100

        self.payload_size = 1024
        self.payload_pattern = '\0deadbeef\r\n\0\0cafebabe\n\r\0'
        self.server_version = "1.6.0beta4"
        self.rpm = "membase-server_x86_1.6.0beta4-25-g5bc3b72.rpm"

        self.return_code = 0

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


# ssh into each host in hosts array and execute cmd in parallel on each
def ssh(hosts,cmd):
    if len(hosts[0]) == 1:
        hosts=[hosts]
    processes=[]
    rtn=""
    for host in hosts:
        process = subprocess.Popen("ssh %s \"%s\"" % (host,cmd),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        processes.append(process)
    for process in processes:
        stdoutdata,stderrdata=process.communicate(None)
        rtn += stdoutdata
    return rtn


# run rebalance on the cluster, sending command to the first server
def rebalance():
    cmd="/opt/membase/bin/cli/membase rebalance -c localhost:%d -u %s -p %s" % (config.servers[0].http_port,config.username, config.password)
    rtn = ssh(config.servers[0].host,cmd)
    for i in range(1000):
        time.sleep(1)
        cmd="/opt/membase/bin/cli/membase rebalance-status -c localhost:%d -u %s -p %s" % (config.servers[0].http_port,config.username, config.password)
        rtn = ssh(config.servers[0].host,cmd)
        if rtn == "none\n":
            break


# add a server to the cluster, sending command to the first server
def server_add(server):
    cmd="/opt/membase/bin/cli/membase server-add -c localhost:%d -u %s -p %s --server-add=%s:%d --server-add-username=%s --server-add-password=%s" % (config.servers[0].http_port,config.username, config.password, server.host, server.http_port,config.username,config.password)
    rtn = ssh(config.servers[0].host,cmd)


# On a given node, return the list of vbuckets that are replicated to each of destination hosts
def server_to_replica_vb_map(server):
    cmd="ps aux | grep vbucketmigrator 2>&1"
    output = ssh(server, cmd)
    dest_server_to_vbucket_map = []
    for line in output.split("\n"):
        tokens = line.split()
        prev_token = ""
        host = ""
        vbucket_list = []
        for token in tokens:
            if prev_token == "-b":
                vbucket_list.append(token)
            elif prev_token == "-d":
                host = token
            prev_token = token
        if host != "" and len(vbucket_list) > 0:
            dest_server_to_vbucket_map.append((host, vbucket_list))
    return dest_server_to_vbucket_map


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

    servers=[]
    for server in config.servers:
        servers.append(server.host)
    if config.create == True:
        # if we are creating, then uninstall, clean up and reinstall the rpm
        ssh(servers,"rpm -e membase-server ; sleep 2 ; killall epmd ; killall beam ; killall -9 memcached ;killall -9 vbucketmigrator ; rm -rf /etc/opt/membase ; rm -rf /var/opt/membase ; rm -rf /opt/membase ; rpm -i %s ; service membase-server stop" % config.rpm)

    # write our custom config to the "master" node
    ssh(config.servers[0].host,"""
rm -f /etc/opt/membase/%s/ns_1/config.dat;
echo '%% Installation-time configuration overrides go in this file.
{buckets, [{configs,
            [{\\"default\\",
              [{type,membase},
               {num_vbuckets,%d},
               {num_replicas,%d},
               {ram_quota,559895808},
               {auth_type,sasl},
               {sasl_password,[]},
               {ht_size,3079},
               {ht_locks,5},
               {servers,[]},
               {map, undefined}]
             }]
}]}.
{directory, \\"/etc/opt/membase/%s\\"}.
{isasl, [{path, \\"/etc/opt/membase/%s/isasl.pw\\"}]}.' > /etc/opt/membase/%s/config""" % (config.server_version,config.vbuckets,config.replicas,config.server_version, config.server_version,config.server_version))

    # restart membase on all the servers
    for server in config.servers:
        ssh(server.host,"service membase-server restart")
        if server == config.servers[0]:
            time.sleep(20)
            process = subprocess.Popen("curl -d \"port=SAME&initStatus=done&username=%s&password=%s\" \"%s:%d/settings/web\" &> /dev/null" % (config.username,config.password,config.servers[0].host,config.servers[0].http_port),shell=True)
            process.wait()
            time.sleep(20)
    time.sleep(20)

    # create the cluster
    for server in config.servers:
        if server == config.servers[0]:
            print "Adding %s to the cluster" % server
        else:
            print "Adding %s to the cluster" % server
            server_add(server)
    time.sleep(20)
    rs = time.time()
    rebalance()
    re = time.time()
    print "Rebalance took %d seconds" % (re-rs)
    time.sleep(20)

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
