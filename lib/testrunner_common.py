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
try:
    import json
except ImportError:
    import simplejson as json


class Server(object):
    def __init__(self,host_port):
        # host:rest_port:moxi_port:memcached_port
        hp = (host_port + ':::::::') .split(":")
        self.host = hp[0]
        self.http_port = int(hp[1] or '8091')
        self.moxi_port = int(hp[2] or '11211')
        self.port = int(hp[3] or '11210')
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


def get_stat(server, stat, sub=""):
    client = mc_bin_client.MemcachedClient(server.host, server.moxi_port)
    stats = client.stats(sub)
    client.close()
    return stats[stat]


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


def wait_on_replication(config):
    while True:
        result = ""
        f = os.popen("curl -u %s:%s http://%s:%d/nodeStatuses" % (config.username, config.password, config.servers[0].host, config.servers[0].http_port))
        for line in f.readlines():
            result = result + line
        decoded = json.loads(result)

        reached = True
        for server in config.servers:
            server_info = server.host + ":" + str(server.http_port)
            replication_status= decoded[server_info]['replication']
            if replication_status != 1.0:
                reached = False
                break
        if reached:
            break
        time.sleep(1.0)


def wait_on_persistence(server):
    client = mc_bin_client.MemcachedClient(server.host, server.moxi_port)
    client.set_flush_param("min_data_age", '0')

    wait_on_state(client, "ep_queue_size", '0')
    wait_on_state(client, "ep_flusher_todo", '0')
    client.close()


def verbose_print(str, verbose):
    if verbose:
#        print str
        if len(str) > 0:
            sys.__stdout__.write(str + "\n")


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
def rebalance(config):
    cmd="/opt/membase/bin/cli/membase rebalance -c localhost:%d -u %s -p %s" % (config.servers[0].http_port,config.username, config.password)
    rtn = ssh(config.servers[0].host,cmd)
    for i in range(1000):
        time.sleep(1)
        cmd="/opt/membase/bin/cli/membase rebalance-status -c localhost:%d -u %s -p %s" % (config.servers[0].http_port,config.username, config.password)
        rtn = ssh(config.servers[0].host,cmd)
        if rtn == "none\n":
            break


# add a server to the cluster, sending command to the first server
def server_add(server, config):
    cmd="/opt/membase/bin/cli/membase server-add -c localhost:%d -u %s -p %s --server-add=%s:%d --server-add-username=%s --server-add-password=%s" % (config.servers[0].http_port,config.username, config.password, server.host, server.http_port,config.username,config.password)
    rtn = ssh(config.servers[0].host,cmd)

# Fail over a server in the cluster
def failover(server, config):
    cmd = "/opt/membase/bin/cli/membase failover -c localhost:%d -u %s -p %s --server-failover %s" % (config.servers[0].http_port, config.username, config.password, server.host)
    rtn = ssh(config.servers[0].host, cmd)
    time.sleep(5)


# restart membase on all the servers
def restart_servers(config):
    for server in config.servers:
        ssh(server.host,"service membase-server restart")
        if server == config.servers[0]:
            time.sleep(20)
            process = subprocess.Popen("curl -d \"port=SAME&initStatus=done&username=%s&password=%s\" \"%s:%d/settings/web\" &> /dev/null" % (config.username,config.password,config.servers[0].host,config.servers[0].http_port),shell=True)
            process.wait()
            time.sleep(20)
    time.sleep(20)


# Kill -9 a given server
def kill_server(server):
    cmd = "ps aux | grep beam.smp 2>&1"
    output = ssh(server.host, cmd)
    for line in output.split("\n"):
        tokens = line.split()
        if tokens[0] == "membase":
            cmd = "kill -9 %s" % tokens[1]
            ssh(server.host, cmd)
            break
    time.sleep(5)


# Shutdown a given server softly
def shutdown_server(server):
    cmd = "service membase-server stop"
    ssh(server.host, cmd)
    time.sleep(5)


# start a given server
def start_server(server):
    cmd = "service membase-server start"
    ssh(server.host, cmd)
    time.sleep(5)


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


# Delete a given vbucket
def delete_vbucket(server, vbucket):
    client = mc_bin_client.MemcachedClient(server.host, server.moxi_port)
    client.set_vbucket_state(vbucket, 'dead')
    client.delete_vbucket(vbucket)


# Activate a given vbucket
def activate_vbucket(server, vbucket):
    client = mc_bin_client.MemcachedClient(server.host, server.moxi_port)
    client.set_vbucket_state(vbucket, 'active')


# set all items to the given server through moxi
def set_items(server, vbucket, num_of_items):
    client = mc_bin_client.MemcachedClient(server.host, server.moxi_port)
    client.vbucketId = vbucket
    for i in range(num_of_items):
        key = "key_" + `vbucket` + "_" + `i`
        payload = generate_payload(key + '\0\r\n\0\0\n\r\0', random.randint(100, 1024))
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


def validate_items(server, vbucket, num_of_items):
    client = mc_bin_client.MemcachedClient(server.host,server.moxi_port)
    client.vbucketId = vbucket
    count = 0
    for cur_op in range(num_of_items):
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


def evict_items(server, vbucket, num_of_items):
    client = mc_bin_client.MemcachedClient(server.host, server.moxi_port)
    client.vbucketId = vbucket
    for i in range(num_of_items):
        key = "key_" + `vbucket` + "_" + `i`
        client.evict_key(key)
    client.close()


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


# Initialize the membase cluster
def initialize_membase_cluster(config):
    servers=[]
    for server in config.servers:
        servers.append(server.host)

    if config.create:
        # if we are creating, then uninstall, clean up and reinstall the rpm on all the nodes
        ssh(servers,
            "rpm -e membase-server ; sleep 2 ; \
             killall epmd ; \
             killall beam ; \
             killall -9 memcached ; \
             killall -9 vbucketmigrator ; \
             rm -rf /etc/opt/membase ; \
             rm -rf /var/opt/membase ; \
             rm -rf /opt/membase ; \
             rpm -i %s ; \
             service membase-server stop" % config.rpm)

    # write our custom config to the "master" node
    ssh(config.servers[0].host, """
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
{isasl, [{path, \\"/etc/opt/membase/%s/isasl.pw\\"}]}.' > /etc/opt/membase/%s/config
""" % (config.server_version, config.vbuckets, config.replicas, config.server_version, config.server_version, config.server_version))

    # restart membase on all the servers
    restart_servers(config)

    # create the cluster
    for server in config.servers:
        if server == config.servers[0]:
            print "Adding %s to the cluster" % server
        else:
            print "Adding %s to the cluster" % server
            server_add(server, config)
    time.sleep(20)
    rs = time.time()
    rebalance(config)
    re = time.time()
    print "Rebalance took %d seconds" % (re-rs)
    time.sleep(10)
