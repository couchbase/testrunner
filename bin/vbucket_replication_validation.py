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

import mc_bin_client
from testrunner_common import *


def usage(err=None):
    if err:
        print "Error: %s\n" % (err)
        r = 1
    else:
        r = 0
    print """\
vbucket_replication_validation.py
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

    print "Checking vbuckets"
    for i in range(25):
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
            verbose_print ("Active:  %d / %d" % (active_count, config.vbuckets), config.verbose)
            passed = False
        if replica_count != (config.vbuckets * config.replicas):
            verbose_print ("Replica: %d / %d" % (replica_count, config.vbuckets * config.replicas), config.verbose)
            passed = False
        if passed == True:
            break
    print "Active:  %d / %d" % (active_count, config.vbuckets)
    print "Replica: %d / %d" % (replica_count, config.vbuckets * config.replicas)
    if passed == True:
        print "vbuckets: Passed"
    else:
        config.return_code = 1
        print "vbuckets: Failed"

    for vbucket in range(config.vbuckets):
        set_items(config.servers[0], vbucket, config.items)
    wait_on_persistence(config.servers[0])
    wait_on_replication(config)

    print "Checking replication"

    curr_items = int(get_stat(config.servers[0], "curr_items"))
    curr_items_total = int(get_stat(config.servers[0], "curr_items_tot"))
    print "curr_items stat: %d / %d" % (curr_items, config.items * config.vbuckets)
    print "curr_items_tot stat: %d / %d" % (curr_items_total, config.items * config.vbuckets * (config.replicas+1))
    if curr_items == config.items * config.vbuckets and\
       curr_items_total == config.items * config.vbuckets * (config.replicas+1):
        print "replication stat: Passed"
    else:
        config.return_code = 1
        print "replication stat: Failed"

    num_of_servers = len(servers)
    for i in range(config.replicas):
        idx = num_of_servers - (i+1)
        failover(servers[idx], config)

    passed = True
    valid_items = 0
    total_valid_items = 0
    for vbucket in range(config.vbuckets):
        valid_items = validate_items(config.servers[0], vbucket, config.items)
        total_valid_items = total_valid_items + valid_items
        if valid_items != config.items:
            passed = False

    print "Valid keys: %d / %d" % (total_valid_items, config.items * config.vbuckets)
    if passed == True:
        print "replication: Passed"
    if passed == False:
        config.return_code = 1
        print "replication: Failed"

    sys.exit(config.return_code)
