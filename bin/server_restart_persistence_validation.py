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
    initialize_membase_cluster(config)

    print "Writing %d items into each of %d vbuckets" % (config.items, config.vbuckets)
    for vbucket in range(config.vbuckets):
        set_items(config.servers[0], vbucket, config.items)

    wait_on_persistence(config.servers[0]);

    print "Restarting the servers again"
    restart_servers(config)
    for server in config.servers:
        wait_on_warmup(server)

    print "Checking all the items loaded from the database"
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
        print "Persistence: Passed"
    if passed == False:
        config.return_code = 1
        print "Persistence: Failed"

    sys.exit(config.return_code)
