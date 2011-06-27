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

import os
import subprocess

DEF_USERNAME = "Administrator"
DEF_PASSWORD = "password"
DEF_KIND = "json"

DEF_MOXI_PORT = 11211
DEF_HTTP_PORT = 8091
DEF_RAMSIZE = 256
DEF_REPLICA = 1

CLI_EXE_LOC = "../membase-cli/membase"
SSH_EXE_LOC = "/opt/membase/bin/cli/membase"

class CLIInterface(object):
    def __init__(self, server, http_port=DEF_HTTP_PORT, username=DEF_USERNAME, password=DEF_PASSWORD, kind=DEF_KIND, debug=False, ssh=False, sshkey=None):
        self.server = server
        self.http_port = http_port
        self.username = username
        self.password = password
        self.kind = kind
        self.debug = debug
        self.ssh = ssh
        self.sshkey = sshkey
        if (debug):
            self.acting_server_args = "-c %s:%d -u %s -p %s -o %s -d" % (self.server, self.http_port, self.username, self.password, self.kind)
        else:
            self.acting_server_args = "-c %s:%d -u %s -p %s -o %s" % (self.server, self.http_port, self.username, self.password, self.kind)

    def server_list(self):
        cmd = " server-list " + self.acting_server_args
        return self.execute_command(cmd)

    def server_info(self):
        cmd = " server-info " + self.acting_server_args
        return self.execute_command(cmd)

    def server_add(self, server_to_add, rebalance=False):
        if (rebalance):
            cmd = " rebalance " + self.acting_server_args + " --server-add=%s:%d --server-add-username=%s --server-add-password=%s"\
                % (server_to_add, self.http_port, self.username, self.password)
        else:
            cmd = " server-add " + self.acting_server_args + " --server-add=%s:%d --server-add-username=%s --server-add-password=%s"\
                % (server_to_add, self.http_port, self.username, self.password)
        return self.execute_command(cmd)

    def server_readd(self, server_to_readd):
        cmd = " server-readd " + self.acting_server_args + " --server-add=%s:%d --server-add-username=%s --server-add-password=%s"\
                % (server_to_readd, self.http_port, self.username, self.password)
        return self.execute_command(cmd)

    def rebalance(self):
        cmd = " rebalance " + self.acting_server_args
        return self.execute_command(cmd)

    def rebalance_stop(self):
        cmd = " reblance-stop " + self.acting_server_args
        return self.execute_command(cmd)

    def rebalance_status(self):
        cmd = " rebalance-status " + self.acting_server_args
        return self.execute_command(cmd)

    def failover(self, server_to_failover):
        cmd = " failover " + self.acting_server_args + " --server-failover %s" % (server_to_failover)
        return self.execute_command(cmd)
        
    def cluster_init(self, c_username=DEF_USERNAME, c_password=DEF_PASSWORD, c_port=DEF_HTTP_PORT, c_ramsize=DEF_RAMSIZE):
        cmd = " cluster-init " + self.acting_server_args\
            + " --cluster-init-username=%s --cluster-init-password=%s --cluster-init-port=%d --cluster-init-ramsize=%d"\
            % (c_username, c_password, c_port, c_ramsize)
        return self.execute_command(cmd)
        
    def node_init(self, path):
        cmd = " node-init " + self.acting_server_args + " --node-init-data-path=%s" % (path)
        return self.execute_command(cmd)
        
    def bucket_list(self):
        cmd = " bucket-list " + self.acting_server_args
        return self.execute_command(cmd)

    def bucket_create(self, bucket_name, bucket_type, bucket_port, bucket_password="", bucket_ramsize=DEF_RAMSIZE, replica_count=DEF_REPLICA):
        cmd = " bucket-create " + self.acting_server_args\
            + " --bucket=%s --bucket-type=%s --bucket-port=%d --bucket-password=%s --bucket-ramsize=%d --bucket-replica=%d"\
            % (bucket_name, bucket_type, bucket_port, bucket_password, bucket_ramsize, replica_count)
        return self.execute_command(cmd)
        
    def bucket_edit(self, bucket_name, bucket_type, bucket_port, bucket_password, bucket_ramsize, replica_count):
        cmd = " bucket-edit " + self.acting_server_args\
            + " --bucket=%s --bucket-type=%s --bucket-port=%d --bucket-password=%s --bucket-ramsize=%d --bucket-replica=%d"\
            % (bucket_name, bucket_type, bucket_port, bucket_password, bucket_ramsize, replica_count)
        return self.execute_command(cmd)
        
    def bucket_delete(self, bucket_name):
        cmd = " bucket-delete " + self.acting_server_args + " --bucket=%s" % (bucket_name)
        return self.execute_command(cmd)
        
    def bucket_flush(self):
        return "I don't work yet :-("

    def execute_command(self, cmd):
        if (self.ssh):
            return self.execute_ssh(SSH_EXE_LOC + cmd)
        else:
            return self.execute_local(CLI_EXE_LOC + cmd)

    def execute_local(self, cmd):
        rtn = ""
        process = subprocess.Popen(cmd ,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdoutdata,stderrdata=process.communicate()
        rtn += stdoutdata
        return rtn

    def execute_ssh(self, cmd):
        rtn=""
        if (self.sshkey == None):
            process = subprocess.Popen("ssh root@%s \"%s\"" % (self.server,cmd),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        else:
            process = subprocess.Popen("ssh -i %s root@%s \"%s\"" % (self.sshkey, self.server, cmd),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdoutdata,stderrdata=process.communicate()
        rtn += stdoutdata
        return rtn
