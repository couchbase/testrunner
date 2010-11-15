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
server_restart_persistence_validation.py
 -h --help
 -v --verbose
 -s --servers <server1,server2,...,serverN>  List of servers to create a cluster with
 -c --create                                 Create cluster (requires passwordless ssh access)
 -r --replicas <count>
 -b --vbuckets <count>
 -u --username <username>                    Username for master
 -p --password <password>                    Password for master
 -i --items <count>                          Number of items per vbucket
 -m --rpm <rpm file>                         rpm file to install
"""

    sys.exit(r)


def wait_on_state(client, stat, state, sub=""):
    reached = False
    while not reached:
        time.sleep(0.5)
        stats = client.stats(sub)
        if stats[stat] == state:
            reached = True


def wait_on_warmup(server):
    client = mc_bin_client.MemcachedClient(server.host, server.port)
    wait_on_state(client,'ep_warmup_thread', 'complete')
    client.close()


def generate_payload(pattern, size):
    return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]


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


# restart membase on all the servers
def restart_servers():
    for server in config.servers:
        ssh(server.host,"service membase-server restart")
        if server == config.servers[0]:
            time.sleep(20)
            process = subprocess.Popen("curl -d \"port=SAME&initStatus=done&username=%s&password=%s\" \"%s:%d/settings/web\" &> /dev/null" % (config.username,config.password,config.servers[0].host,config.servers[0].http_port),shell=True)
            process.wait()
            time.sleep(20)
    time.sleep(20)


# return a list of all the vbuckets with their status (active, replica, pending)
def vbucket_list(server):
    cmd="/opt/membase/bin/ep_engine/management/vbucketctl localhost:%d list 2>&1" % (server.port)
    vbs=ssh(server.host,cmd)
    vbuckets=[]
    for vb in vbs.split("\n"):
        try:
            _,id,state=vb.split(" ")
            vbuckets.append((id,state))
        except:
            pass
    return vbuckets


# set all items to the given server through moxi
def set_items(server, vbucket):
    client = mc_bin_client.MemcachedClient(server.host, server.moxi_port)
    client.vbucketId = vbucket
    for i in range(config.items):
        key = "key_" + `vbucket` + "_" + `i`
        payload = generate_payload(key + '\0\r\n\0\0\n\r\0', random.randint(100, 1024));
        flag = socket.htonl(ctypes.c_uint32(zlib.adler32(payload)).value)
        backoff_sec = 0
        while backoff_sec < 4 :
            (opaque, cas, data) = client.set(key,0,flag,payload)
            if cas > 0:
                break
            backoff_sec = backoff_sec + 0.1 + (backoff_sec / 20)
            print "set %s failed and retry in %f sec" % (key, backoff_sec)
            time.sleep(backoff_sec)

    client.close()


def validate_items(server, vbucket):
    client = mc_bin_client.MemcachedClient(server.host,server.moxi_port)
    client.vbucketId = vbucket;
    count = 0
    for cur_op in range(config.items):
        key = "key_" + `vbucket` + "_" + `cur_op`
        try:
            flag, keyx, value = client.get(key)
            assert (flag)
            hflag = socket.ntohl(flag)
            if hflag == ctypes.c_uint32(zlib.adler32(value)).value:
                count = count + 1
        except (mc_bin_client.MemcachedError):
            continue
    return count


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
                'items=',
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
        elif o in ("-i", "--items"):
            config.items = int(a)
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

    # Restart all the servers
    restart_servers()

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

    print "Checking vbuckets and their status on each host"
    for i in range(30):
        time.sleep(5)
        active_count = 0
        replica_count = 0
        for server in config.servers:
            vbuckets = vbucket_list(server)
            for (vb,state) in vbuckets:
                if state == "active":
                    active_count += 1
                elif state == "replica":
                    replica_count += 1
        passed = True
        if active_count != config.vbuckets:
            passed = False
        if replica_count != (config.vbuckets * config.replicas):
            passed = False
        if passed == True:
            break

    print "Writing %d items into each of %d vbuckets" % (config.items, config.vbuckets)
    for vbucket in range(config.vbuckets):
        set_items(config.servers[0], vbucket)

    print "Restarting the servers again"
    restart_servers()
    for server in config.servers:
        wait_on_warmup(server)

    print "Checking all the items loaded from the database"
    passed = True
    valid_items = 0
    total_valid_items = 0
    for vbucket in range(config.vbuckets):
        valid_items = validate_items(config.servers[0], vbucket)
        total_valid_items = total_valid_items + valid_items
        if valid_items != config.items:
            passed = False

    print "Valid keys: %d / %d" % (total_valid_items, config.items * config.vbuckets)
    if passed == True:
        print "Persistence: Passed"
    if passed == False:
        config.return_code = 1
        print "Persistence: Failed"

    sys.exit(config.return_code)
