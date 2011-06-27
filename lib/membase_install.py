#!/usr/bin/env python

#
#     Copyright 2010 Membase, Inc.
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

import subprocess

BASE_WEB_ADDRESS = "http://builds.hq.northscale.net/latestbuilds/"
DEFAULT_DOWNLOAD = "membase-server-community_x86_64.rpm"

def download_membase(server, instance=DEFAULT_DOWNLOAD):
    cmd = "wget %s" % (BASE_WEB_ADDRESS + instance)
    return ssh(server, cmd)

def delete_membase(server, instance):
    cmd = "rm -f %s" % (instance)
    return ssh(server, cmd)

def install_membase(server, instance=DEFAULT_DOWNLOAD):
    print "Installing membase on " + server
    cmd = "rpm -i %s" % (instance)
    return ssh(server, cmd)

def uninstall_membase(server):
    print "Uninstalling membase on " + server
    cmd = "rpm -e membase-server; rm -rf /var/opt/membase; rm -rf /etc/opt/membase"
    return ssh(server, cmd)

def reinstall_membase(server, instance=DEFAULT_DOWNLOAD):
    uninstall_membase(server)
    install_membase(server, instance)

def start_membase():
    cmd = "service membase-server start"
    return ssh(server, cmd)

def stop_membase():
    cmd = "service membase-server stop"
    return ssh(server, cmd)

def restart_membase():
    cmd = "service membase-server restart"
    return ssh(server, cmd)

def status_membase(server):
    cmd = "service membase-server status"
    return ssh(server, cmd)

# ssh into each host in hosts array and execute cmd in parallel on each
def ssh(host,cmd,key=None):
    rtn=""
    process = subprocess.Popen("ssh -i ~/.ssh/mikew_key.pem root@%s \"%s\"" % (host,cmd),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    stdoutdata,stderrdata=process.communicate()
    rtn += stdoutdata
    return rtn
