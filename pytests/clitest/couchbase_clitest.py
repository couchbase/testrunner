import copy
import json
import os
from threading import Thread

from membase.api.rest_client import RestConnection
from TestInput import TestInputSingleton
from clitest.cli_base import CliBaseTest
from remote.remote_util import RemoteMachineShellConnection
from pprint import pprint
from testconstants import CLI_COMMANDS, COUCHBASE_FROM_WATSON,\
                          COUCHBASE_FROM_SPOCK, LINUX_COUCHBASE_BIN_PATH,\
                          WIN_COUCHBASE_BIN_PATH, COUCHBASE_FROM_SHERLOCK

help = {'CLUSTER': '--cluster=HOST[:PORT] or -c HOST[:PORT]',
 'COMMAND': {'bucket-compact': 'compact database and index data',
             'bucket-create': 'add a new bucket to the cluster',
             'bucket-delete': 'delete an existing bucket',
             'bucket-edit': 'modify an existing bucket',
             'bucket-flush': 'flush all data from disk for a given bucket',
             'bucket-list': 'list all buckets in a cluster',
             'cluster-edit': 'modify cluster settings',
             'cluster-init': 'set the username,password and port of the cluster',
             'failover': 'failover one or more servers',
             'group-manage': 'manage server groups',
             'help': 'show longer usage/help and examples',
             'node-init': 'set node specific parameters',
             'rebalance': 'start a cluster rebalancing',
             'rebalance-status': 'show status of current cluster rebalancing',
             'rebalance-stop': 'stop current cluster rebalancing',
             'recovery': 'recover one or more servers',
             'server-add': 'add one or more servers to the cluster',
             'server-info': 'show details on one server',
             'server-list': 'list all servers in a cluster',
             'server-readd': 'readd a server that was failed over',
             'setting-alert': 'set email alert settings',
             'setting-autofailover': 'set auto failover settings',
             'setting-compaction': 'set auto compaction settings',
             'setting-notification': 'set notification settings',
             'setting-xdcr': 'set xdcr related settings',
             'ssl-manage': 'manage cluster certificate',
             'user-manage': 'manage read only user',
             'xdcr-replicate': 'xdcr operations',
             'xdcr-setup': 'set up XDCR connection'},
 'EXAMPLES': {'Add a node to a cluster and rebalance': 'couchbase-cli rebalance -c 192.168.0.1:8091 --server-add=192.168.0.2:8091 --server-add-username=Administrator1 --server-add-password=password1 --group-name=group1 -u Administrator -p password',
              'Add a node to a cluster, but do not rebalance': 'couchbase-cli server-add -c 192.168.0.1:8091 --server-add=192.168.0.2:8091 --server-add-username=Administrator1 --server-add-password=password1 --group-name=group1 -u Administrator -p password',
              'Change the data path': 'couchbase-cli node-init -c 192.168.0.1:8091 --node-init-data-path=/tmp -u Administrator -p password',
              'Create a couchbase bucket and wait for bucket ready': 'couchbase-cli bucket-create -c 192.168.0.1:8091 --bucket=test_bucket --bucket-type=couchbase --bucket-port=11222 --bucket-ramsize=200 --bucket-replica=1 --bucket-priority=low --wait -u Administrator -p password',
              'Create a new dedicated port couchbase bucket': 'couchbase-cli bucket-create -c 192.168.0.1:8091 --bucket=test_bucket --bucket-type=couchbase --bucket-port=11222 --bucket-ramsize=200 --bucket-replica=1 --bucket-priority=high -u Administrator -p password',
              'Create a new sasl memcached bucket': 'couchbase-cli bucket-create -c 192.168.0.1:8091 --bucket=test_bucket --bucket-type=memcached --bucket-password=password --bucket-ramsize=200 --bucket-eviction-policy=valueOnly --enable-flush=1 -u Administrator -p password',
              'Delete a bucket': 'couchbase-cli bucket-delete -c 192.168.0.1:8091 --bucket=test_bucket',
              'Flush a bucket': 'couchbase-cli xdcr-replicate -c 192.168.0.1:8091 --list -u Administrator -p password',
              'List buckets in a cluster': 'couchbase-cli bucket-list -c 192.168.0.1:8091',
              'List read only user in a cluster': 'couchbase-cli ssl-manage -c 192.168.0.1:8091 --regenerate-cert=/tmp/test.pem -u Administrator -p password',
              'List servers in a cluster': 'couchbase-cli server-list -c 192.168.0.1:8091',
              'Modify a dedicated port bucket': 'couchbase-cli bucket-edit -c 192.168.0.1:8091 --bucket=test_bucket --bucket-port=11222 --bucket-ramsize=400 --bucket-eviction-policy=fullEviction --enable-flush=1 --bucket-priority=high -u Administrator -p password',
              'Remove a node from a cluster and rebalance': 'couchbase-cli rebalance -c 192.168.0.1:8091 --server-remove=192.168.0.2:8091 -u Administrator -p password',
              'Remove and add nodes from/to a cluster and rebalance': 'couchbase-cli rebalance -c 192.168.0.1:8091 --server-remove=192.168.0.2 --server-add=192.168.0.4 --server-add-username=Administrator1 --server-add-password=password1 --group-name=group1 -u Administrator -p password',
              'Server information': 'couchbase-cli server-info -c 192.168.0.1:8091',
              'Set data path and hostname for an unprovisioned cluster': 'couchbse-cli node-init -c 192.168.0.1:8091 --node-init-data-path=/tmp/data --node-init-index-path=/tmp/index --node-init-hostname=myhostname -u Administrator -p password',
              'Set recovery type to a server': 'couchbase-cli rebalance -c 192.168.0.1:8091 --recovery-buckets="default,bucket1" -u Administrator -p password',
              'Set the username, password, port and ram quota': 'couchbase-cli cluster-init -c 192.168.0.1:8091 --cluster-username=Administrator --cluster-password=password --cluster-port=8080 --cluster-ramsize=300',
              'Stop the current rebalancing': 'couchbase-cli rebalance-stop -c 192.168.0.1:8091 -u Administrator -p password',
              'change the cluster username, password, port and ram quota': 'couchbase-cli cluster-edit -c 192.168.0.1:8091 --cluster-username=Administrator1 --cluster-password=password1 --cluster-port=8080 --cluster-ramsize=300 -u Administrator -p password'},
 'OPTIONS': {'-o KIND, --output=KIND': 'KIND is json or standard\n-d, --debug',
             '-p PASSWORD, --password=PASSWORD': 'admin password of the cluster',
             '-u USERNAME, --user=USERNAME': 'admin username of the cluster'},
 'bucket-* OPTIONS': {'--bucket-eviction-policy=[valueOnly|fullEviction]': ' policy how to retain meta in memory',
                      '--bucket-password=PASSWORD': 'standard port, exclusive with bucket-port',
                      '--bucket-port=PORT': 'supports ASCII protocol and is auth-less',
                      '--bucket-priority=[low|high]': 'priority when compared to other buckets',
                      '--bucket-ramsize=RAMSIZEMB': 'ram quota in MB',
                      '--bucket-replica=COUNT': 'replication count',
                      '--bucket-type=TYPE': 'memcached or couchbase',
                      '--bucket=BUCKETNAME': ' bucket to act on',
                      '--data-only': ' compact datbase data only',
                      '--enable-flush=[0|1]': 'enable/disable flush',
                      '--enable-index-replica=[0|1]': 'enable/disable index replicas',
                      '--force': ' force to execute command without asking for confirmation',
                      '--view-only': ' compact view data only',
                      '--wait': 'wait for bucket create to be complete before returning'},
 'cluster-* OPTIONS': {'--cluster-password=PASSWORD': ' new admin password',
                       '--cluster-port=PORT': ' new cluster REST/http port',
                       '--cluster-ramsize=RAMSIZEMB': ' per node ram quota in MB',
                       '--cluster-username=USER': ' new admin username'},
 'description': 'couchbase-cli - command-line cluster administration tool',
 'failover OPTIONS': {'--force': ' failover node from cluster right away',
                      '--server-failover=HOST[:PORT]': ' server to failover'},
 'group-manage OPTIONS': {'--add-servers=HOST[:PORT];HOST[:PORT]': 'add a list of servers to group\n--move-servers=HOST[:PORT];HOST[:PORT] move a list of servers from group',
                          '--create': 'create a new group',
                          '--delete': 'delete an empty group',
                          '--from-group=GROUPNAME': 'group name that to move servers from',
                          '--group-name=GROUPNAME': 'group name',
                          '--list': 'show group/server relationship map',
                          '--rename=NEWGROUPNAME': ' rename group to new name',
                          '--to-group=GROUPNAME': 'group name tat to move servers to'},
 'node-init OPTIONS': {'--node-init-data-path=PATH': 'per node path to store data',
                       '--node-init-hostname=NAME': ' hostname for the node. Default is 127.0.0.1',
                       '--node-init-index-path=PATH': ' per node path to store index'},
 'rebalance OPTIONS': {'--recovery-buckets=BUCKETS': 'comma separated list of bucket name. Default is for all buckets.',
                       '--server-add*': ' see server-add OPTIONS',
                       '--server-remove=HOST[:PORT]': ' the server to be removed'},
 'recovery OPTIONS': {'--recovery-type=TYPE[delta|full]': 'type of recovery to be performed for a node',
                      '--server-recovery=HOST[:PORT]': ' server to recover'},
 'server-add OPTIONS': {'--group-name=GROUPNAME': 'group that server belongs',
                        '--server-add-password=PASSWORD': 'admin password for the\nserver to be added',
                        '--server-add-username=USERNAME': 'admin username for the\nserver to be added',
                        '--server-add=HOST[:PORT]': 'server to be added'},
 'server-readd OPTIONS': {'--group-name=GROUPNAME': 'group that server belongs',
                          '--server-add-password=PASSWORD': 'admin password for the\nserver to be added',
                          '--server-add-username=USERNAME': 'admin username for the\nserver to be added',
                          '--server-add=HOST[:PORT]': 'server to be added'},
 'setting-alert OPTIONS': {'--alert-auto-failover-cluster-small': " node wasn't auto fail over as cluster was too small",
                           '--alert-auto-failover-max-reached': ' maximum number of auto failover nodes was reached',
                           '--alert-auto-failover-node': 'node was auto failover',
                           '--alert-auto-failover-node-down': " node wasn't auto failover as other nodes are down at the same time",
                           '--alert-disk-space': 'disk space used for persistent storgage has reached at least 90% capacity',
                           '--alert-ip-changed': 'node ip address has changed unexpectedly',
                           '--alert-meta-oom': 'bucket memory on a node is entirely used for metadata',
                           '--alert-meta-overhead': ' metadata overhead is more than 50%',
                           '--alert-write-failed': 'writing data to disk for a specific bucket has failed',
                           '--email-host=HOST': ' email server host',
                           '--email-password=PWD': 'email server password',
                           '--email-port=PORT': ' email server port',
                           '--email-recipients=RECIPIENT': 'email recipents, separate addresses with , or ;',
                           '--email-sender=SENDER': ' sender email address',
                           '--email-user=USER': ' email server username',
                           '--enable-email-alert=[0|1]': 'allow email alert',
                           '--enable-email-encrypt=[0|1]': 'email encrypt'},
 'setting-autofailover OPTIONS': {'--auto-failover-timeout=TIMEOUT (>=30)': 'specify timeout that expires to trigger auto failover',
                                  '--enable-auto-failover=[0|1]': 'allow auto failover'},
 'setting-compaction OPTIONS': {'--compaction-db-percentage=PERCENTAGE': ' at which point database compaction is triggered',
                                '--compaction-db-size=SIZE[MB]': ' at which point database compaction is triggered',
                                '--compaction-period-from=HH:MM': 'allow compaction time period from',
                                '--compaction-period-to=HH:MM': 'allow compaction time period to',
                                '--compaction-view-percentage=PERCENTAGE': ' at which point view compaction is triggered',
                                '--compaction-view-size=SIZE[MB]': ' at which point view compaction is triggered',
                                '--enable-compaction-abort=[0|1]': ' allow compaction abort when time expires',
                                '--enable-compaction-parallel=[0|1]': 'allow parallel compaction for database and view',
                                '--metadata-purge-interval=DAYS': 'how frequently a node will purge metadata on deleted items'},
 'setting-notification OPTIONS': {'--enable-notification=[0|1]': ' allow notification'},
 'setting-xdcr OPTIONS': {'--checkpoint-interval=[1800]': ' intervals between checkpoints, 60 to 14400 seconds.',
                          '--doc-batch-size=[2048]KB': 'document batching size, 10 to 100000 KB',
                          '--failure-restart-interval=[30]': 'interval for restarting failed xdcr, 1 to 300 seconds\n--optimistic-replication-threshold=[256] document body size threshold (bytes) to trigger optimistic replication',
                          '--max-concurrent-reps=[32]': ' maximum concurrent replications per bucket, 8 to 256.',
                          '--worker-batch-size=[500]': 'doc batch size, 500 to 10000.'},
 'ssl-manage OPTIONS': {'--regenerate-cert=CERTIFICATE': 'regenerate cluster certificate AND save to a pem file',
                        '--retrieve-cert=CERTIFICATE': 'retrieve cluster certificate AND save to a pem file'},
 'usage': ' couchbase-cli COMMAND CLUSTER [OPTIONS]',
 'user-manage OPTIONS': {'--delete': ' delete read only user',
                         '--list': ' list any read only user',
                         '--ro-password=PASSWORD': ' readonly user password',
                         '--ro-username=USERNAME': ' readonly user name',
                         '--set': 'create/modify a read only user'},
 'xdcr-replicate OPTIONS': {'--checkpoint-interval=[1800]': ' intervals between checkpoints, 60 to 14400 seconds.',
                            '--create': ' create and start a new replication',
                            '--delete': ' stop and cancel a replication',
                            '--doc-batch-size=[2048]KB': 'document batching size, 10 to 100000 KB',
                            '--failure-restart-interval=[30]': 'interval for restarting failed xdcr, 1 to 300 seconds\n--optimistic-replication-threshold=[256] document body size threshold (bytes) to trigger optimistic replication',
                            '--list': ' list all xdcr replications',
                            '--max-concurrent-reps=[32]': ' maximum concurrent replications per bucket, 8 to 256.',
                            '--pause': 'pause the replication',
                            '--resume': ' resume the replication',
                            '--settings': ' update settings for the replication',
                            '--worker-batch-size=[500]': 'doc batch size, 500 to 10000.',
                            '--xdcr-clucter-name=CLUSTERNAME': 'remote cluster to replicate to',
                            '--xdcr-from-bucket=BUCKET': 'local bucket name to replicate from',
                            '--xdcr-replication-mode=[xmem|capi]': 'replication protocol, either capi or xmem.',
                            '--xdcr-replicator=REPLICATOR': ' replication id',
                            '--xdcr-to-bucket=BUCKETNAME': 'remote bucket to replicate to',
                            '--source-nozzle-per-node=[1-10]': 'the number of source nozzles per source node',
                            '--target-nozzle-per-node=[1-100]': 'the number of outgoing nozzles per target node'},
 'xdcr-setup OPTIONS': {'--create': ' create a new xdcr configuration',
                        '--delete': ' delete existed xdcr configuration',
                        '--edit': ' modify existed xdcr configuration',
                        '--list': ' list all xdcr configurations',
                        '--xdcr-certificate=CERTIFICATE': ' pem-encoded certificate. Need be present if xdcr-demand-encryption is true',
                        '--xdcr-cluster-name=CLUSTERNAME': 'cluster name',
                        '--xdcr-demand-encryption=[0|1]': ' allow data encrypted using ssl',
                        '--xdcr-hostname=HOSTNAME': ' remote host name to connect to',
                        '--xdcr-password=PASSWORD': ' remote cluster admin password',
                        '--xdcr-username=USERNAME': ' remote cluster admin username'}}

""" in 3.0, we add 'recovery  recover one or more servers' into couchbase-cli """
help_short = {'COMMANDs include': {'bucket-compact': 'compact database and index data',
                      'bucket-create': 'add a new bucket to the cluster',
                      'bucket-delete': 'delete an existing bucket',
                      'bucket-edit': 'modify an existing bucket',
                      'bucket-flush': 'flush all data from disk for a given bucket',
                      'bucket-list': 'list all buckets in a cluster',
                      'cluster-edit': 'modify cluster settings',
                      'cluster-init': 'set the username,password and port of the cluster',
                      'recovery': 'recover one or more servers',
                      'failover': 'failover one or more servers',
                      'group-manage': 'manage server groups',
                      'help': 'show longer usage/help and examples',
                      'node-init': 'set node specific parameters',
                      'rebalance': 'start a cluster rebalancing',
                      'rebalance-status': 'show status of current cluster rebalancing',
                      'rebalance-stop': 'stop current cluster rebalancing',
                      'server-add': 'add one or more servers to the cluster',
                      'server-info': 'show details on one server',
                      'server-list': 'list all servers in a cluster',
                      'server-readd': 'readd a server that was failed over',
                      'setting-alert': 'set email alert settings',
                      'setting-autofailover': 'set auto failover settings',
                      'setting-compaction': 'set auto compaction settings',
                      'setting-notification': 'set notification settings',
                      'setting-xdcr': 'set xdcr related settings',
                      'ssl-manage': 'manage cluster certificate',
                      'user-manage': 'manage read only user',
                      'xdcr-replicate': 'xdcr operations',
                      'xdcr-setup': 'set up XDCR connection'},
 'description': 'CLUSTER is --cluster=HOST[:PORT] or -c HOST[:PORT]',
 'usage': ' couchbase-cli COMMAND CLUSTER [OPTIONS]'}


class CouchbaseCliTest(CliBaseTest):
    def setUp(self):
        TestInputSingleton.input.test_params["default_bucket"] = False
        super(CouchbaseCliTest, self).setUp()

    def tearDown(self):
        super(CouchbaseCliTest, self).tearDown()


    def _get_dict_from_output(self, output):
        result = {}
        if output[0].startswith("couchbase-cli"):
            result["description"] = output[0]
            result["usage"] = output[2].split("usage:")[1]
        else:
            result["usage"] = output[0].split("usage:")[1]
            result["description"] = output[2]

        upper_key = ""
        for line in output[3:]:
            line = line.strip()
            if line == "":
                if not upper_key == "EXAMPLES":
                    upper_key = ""
                continue
            # line = ""
            if line.endswith(":") and upper_key != "EXAMPLES":
                upper_key = line[:-1]
                result[upper_key] = {}
                continue
            elif line == "COMMANDs include":
                upper_key = line
                result[upper_key] = {}
                continue
            elif upper_key in ["COMMAND", "COMMANDs include"] :
                result[upper_key][line.split()[0]] = line.split(line.split()[0])[1].strip()
                if len(line.split(line.split()[0])) > 2:
                    if line.split()[0] == 'help':
                        result[upper_key][line.split()[0]] = (line.split(line.split()[0])[1] + 'help' + line.split(line.split()[0])[-1]).strip()
                    else:
                        result[upper_key][line.split()[0]] = line.split(line.split(line.split()[0])[1])[-1].strip()
            elif upper_key in ["CLUSTER"]:
                result[upper_key] = line
            elif upper_key.endswith("OPTIONS"):
                # for u=instance:"   -u USERNAME, --user=USERNAME      admin username of the cluster"
                temp = line.split("  ")
                temp = [item for item in temp if item != ""]
                if len(temp) > 1:
                    result[upper_key][temp[0]] = temp[1]
                    previous_key = temp[0]
                else:
                    result[upper_key][previous_key] = result[upper_key][previous_key] + "\n" + temp[0]
            elif upper_key in ["EXAMPLES"] :
                if line.endswith(":"):
                    previous_key = line[:-1]
                    result[upper_key][previous_key] = ""
                else:
                    line = line.strip()
                    if line.startswith("couchbase-cli"):
                        result[upper_key][previous_key] = line.replace("\\", "")
                    else:
                        result[upper_key][previous_key] = (result[upper_key][previous_key] + line).replace("\\", "")
                    previous_line = result[upper_key][previous_key]
            elif line == "The default PORT number is 8091.":
                continue
            else:
                self.fail(line)
        return result

    def _get_cluster_info(self, remote_client, cluster_host="localhost", cluster_port=None, user="Administrator", password="password"):
        command = "server-info"
        output, error = remote_client.execute_couchbase_cli(command, cluster_host=cluster_host, cluster_port=cluster_port, user=user, password=password)
        if not error:
            content = ""
            for line in output:
                content += line
            return json.loads(content)
        else:
            self.fail("server-info return error output")

    def _create_bucket(self, remote_client, bucket="default", bucket_type="couchbase", bucket_port=11211, bucket_password=None, \
                        bucket_ramsize=200, bucket_replica=1, wait=False, enable_flush=None, enable_index_replica=None):
        options = "--bucket={0} --bucket-type={1} --bucket-port={2} --bucket-ramsize={3} --bucket-replica={4}".\
            format(bucket, bucket_type, bucket_port, bucket_ramsize, bucket_replica)
        options += (" --enable-flush={0}".format(enable_flush), "")[enable_flush is None]
        options += (" --enable-index-replica={0}".format(enable_index_replica), "")[enable_index_replica is None]
        options += (" --enable-flush={0}".format(enable_flush), "")[enable_flush is None]
        options += (" --bucket-password={0}".format(bucket_password), "")[bucket_password is None]
        options += (" --wait", "")[wait]
        cli_command = "bucket-create"

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
        if "TIMED OUT" in output[0]:
            raise Exception("Timed out.  Could not create bucket")
        else:
            self.assertTrue("SUCCESS: bucket-create" in output[0], "Fail to create bucket")

    def testHelp(self):
        command_with_error = {}
        shell = RemoteMachineShellConnection(self.master)
        if self.cb_version[:5] in COUCHBASE_FROM_SPOCK:
            self.log.info("skip moxi because it is removed in spock ")
            for x in CLI_COMMANDS:
                if x == "moxi":
                    CLI_COMMANDS.remove("moxi")
        for cli in CLI_COMMANDS:
            """ excluded_commands should separate by ';' """
            if self.excluded_commands is not None:
                if ";" in self.excluded_commands:
                    excluded_commands = self.excluded_commands.split(";")
                else:
                    excluded_commands = self.excluded_commands
                if cli in excluded_commands:
                    self.log.info("command {0} test will be skipped.".format(cli))
                    continue
            if self.os == "windows":
                cli = '.'.join([cli, "exe"])
            option = " -h"
            if cli == "erl":
                option = " -version"
            command = ''.join([self.cli_command_path, cli, option])
            self.log.info("test -h of command {0}".format(cli))
            output, error = shell.execute_command(command, use_channel=True)
            """ check if the first line is not empty """
            if not output[0]:
                self.log.error("this help command {0} may not work!".format(cli))
            if error:
                command_with_error[cli] = error[0]
        if command_with_error:
            raise Exception("some commands throw out error %s " % command_with_error)
        shell.disconnect()

    def testInfoCommands(self):
        remote_client = RemoteMachineShellConnection(self.master)

        cli_command = "server-list"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                  cluster_host="localhost", user="Administrator", password="password")
        server_info = self._get_cluster_info(remote_client)
        """ In new single node not join any cluster yet,
            IP of node will be 127.0.0.1 """
        if "127.0.0.1" in server_info["otpNode"]:
            server_info["otpNode"] = "ns_1@{0}".format(remote_client.ip)
            server_info["hostname"] = "{0}:8091".format(remote_client.ip)
        result = server_info["otpNode"] + " " + server_info["hostname"] + " " \
               + server_info["status"] + " " + server_info["clusterMembership"]
        self.assertEqual(result, "ns_1@{0} {0}:8091 healthy active" \
                                           .format(remote_client.ip))

        cli_command = "bucket-list"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                  cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual([], output)
        remote_client.disconnect()

    def testAddRemoveNodes(self):
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        nodes_failover = self.input.param("nodes_failover", 0)
        nodes_readd = self.input.param("nodes_readd", 0)
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "server-add"
        if int(nodes_add) < len(self.servers):
            for num in xrange(nodes_add):
                self.log.info("add node {0} to cluster"\
                                             .format(self.servers[num + 1].ip))
                options = "--server-add={0}:8091 \
                           --server-add-username=Administrator \
                           --server-add-password=password" \
                            .format(self.servers[num + 1].ip)
                output, error = \
                      remote_client.execute_couchbase_cli(cli_command=cli_command,\
                                       options=options, cluster_host="localhost", \
                                         user="Administrator", password="password")
                server_added = False
                if len(output) >= 1:
                    for x in output:
                        if "Server %s:8091 added" % (self.servers[num + 1].ip) in x:
                            server_added = True
                            break
                    if not server_added:
                        raise Exception("failed to add server {0}"
                                          .format(self.servers[num + 1].ip))
        else:
             raise Exception("Node add should be smaller total number"
                                                         " vms in ini file")

        cli_command = "rebalance"
        for num in xrange(nodes_rem):
            options = "--server-remove={0}:8091".format(self.servers[nodes_add - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                            options=options, cluster_host="localhost", \
                            user="Administrator", password="password")
            self.assertTrue("INFO: rebalancing" in output[0])
            if len(output) == 4:
               self.assertEqual(output[2], "SUCCESS: rebalanced cluster")
            else:
                self.assertEqual(output[1], "SUCCESS: rebalanced cluster")

        if nodes_rem == 0 and nodes_add > 0:
            cli_command = "rebalance"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                            cluster_host="localhost", user="Administrator", password="password")
            if len(output) == 4:
                self.assertEqual(output, ["INFO: rebalancing ", "", "SUCCESS: rebalanced cluster", ""])
            else:
                self.assertEqual(output, ["INFO: rebalancing . ", "SUCCESS: rebalanced cluster"])

        """ when no bucket, have to add option --force to failover
            since no data => no graceful failover.  Need to add test
            to detect no graceful failover if no bucket """
        self._create_bucket(remote_client, bucket_replica=self.num_replicas)
        cli_command = "failover"
        for num in xrange(nodes_failover):
            self.log.info("failover node {0}" \
                          .format(self.servers[nodes_add - nodes_rem - num].ip))
            options = "--server-failover={0}:8091" \
                      .format(self.servers[nodes_add - nodes_rem - num].ip)
            if self.force_failover:
                options += " --force"
                output, error = remote_client.execute_couchbase_cli(\
                        cli_command=cli_command, options=options, \
                        cluster_host="localhost", user="Administrator", \
                                                  password="password")
                if len(output) == 2:
                    self.assertEqual(output, ["SUCCESS: failover ns_1@{0}" \
                        .format(self.servers[nodes_add - nodes_rem - num].ip), ""])
                else:
                    self.assertEqual(output, ["SUCCESS: failover ns_1@{0}" \
                        .format(self.servers[nodes_add - nodes_rem - num].ip)])
            else:
                output, error = remote_client.execute_couchbase_cli(\
                        cli_command=cli_command, options=options, \
                        cluster_host="localhost", user="Administrator", \
                                                  password="password")
                output[0] = output[0].rstrip(" .")
                if len(output) == 3:
                    self.assertEqual(output, ["INFO: graceful failover", \
                                              "SUCCESS: failover ns_1@{0}" \
                        .format(self.servers[nodes_add - nodes_rem - num].ip), ""])
                else:
                    self.assertEqual(output, ["INFO: graceful failover", \
                                              "SUCCESS: failover ns_1@{0}" \
                            .format(self.servers[nodes_add - nodes_rem - num].ip)])


        cli_command = "server-readd"
        for num in xrange(nodes_readd):
            self.log.info("add back node {0} to cluster" \
                    .format(self.servers[nodes_add - nodes_rem - num ].ip))
            options = "--server-add={0}:8091 --server-add-username=Administrator \
                       --server-add-password=password" \
                            .format(self.servers[nodes_add - nodes_rem - num ].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                                              options=options, cluster_host="localhost", \
                                                user="Administrator", password="password")
            if len(output) == 2:
                self.assertEqual(output, ["SUCCESS: re-add ns_1@{0}" \
                                          .format(self.servers[nodes_add - nodes_rem - num ].ip), ""])
            else:
                self.assertEqual(output, ["SUCCESS: re-add ns_1@{0}" \
                                          .format(self.servers[nodes_add - nodes_rem - num ].ip)])

        cli_command = "rebalance"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                       cluster_host="localhost", user="Administrator", password="password")
        output[0] = output[0].rstrip(" .")
        if len(output) == 4:
            self.assertEqual(output, ["INFO: rebalancing", "", "SUCCESS: rebalanced cluster", ""])
        else:
            self.assertEqual(output, ["INFO: rebalancing", "SUCCESS: rebalanced cluster"])
        remote_client.disconnect()


    def testAddRemoveNodesWithRecovery(self):
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        nodes_failover = self.input.param("nodes_failover", 0)
        nodes_recovery = self.input.param("nodes_recovery", 0)
        nodes_readd = self.input.param("nodes_readd", 0)
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "server-add"
        if int(nodes_add) < len(self.servers):
            for num in xrange(nodes_add):
                self.log.info("add node {0} to cluster".format(self.servers[num + 1].ip))
                options = "--server-add={0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[num + 1].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
                if len(output) == 2:
                    self.assertEqual(output, ["Warning: Adding server from group-manage is deprecated",
                                              "Server {0}:8091 added".format(self.servers[num + 1].ip)])
                else:
                    self.assertEqual(output, ["Warning: Adding server from group-manage is deprecated",
                                              "Server {0}:8091 added".format(self.servers[num + 1].ip)],
                                              "")
        else:
             raise Exception("Node add should be smaller total number vms in ini file")

        cli_command = "rebalance"
        for num in xrange(nodes_rem):
            options = "--server-remove={0}:8091".format(self.servers[nodes_add - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertTrue("INFO: rebalancing" in output[0])
            if len(output) == 4:
                self.assertEqual(output[2], "SUCCESS: rebalanced cluster")
            else:
                self.assertEqual(output[1], "SUCCESS: rebalanced cluster")

        if nodes_rem == 0 and nodes_add > 0:
            cli_command = "rebalance"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
            self.assertTrue(output, ["INFO: rebalancing . ", "SUCCESS: rebalanced cluster"])

        self._create_bucket(remote_client)

        cli_command = "failover"
        for num in xrange(nodes_failover):
            self.log.info("failover node {0}".format(self.servers[nodes_add - nodes_rem - num].ip))
            options = "--server-failover={0}:8091".format(self.servers[nodes_add - nodes_rem - num].ip)
            if self.force_failover or num == nodes_failover - 1:
                options += " --force"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertTrue('SUCCESS: failover ns_1@{0}'.format(self.servers[nodes_add - nodes_rem - num].ip) in output, error)

        cli_command = "recovery"
        for num in xrange(nodes_failover):
            # try to set recovery when nodes failovered (MB-11230)
            options = "--server-recovery={0}:8091 --recovery-type=delta".format(self.servers[nodes_add - nodes_rem - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual("SUCCESS: setRecoveryType for node ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num].ip), output[0])

        for num in xrange(nodes_recovery):
            cli_command = "server-readd"
            self.log.info("add node {0} back to cluster".format(self.servers[nodes_add - nodes_rem - num].ip))
            options = "--server-add={0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[nodes_add - nodes_rem - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            if (len(output) == 2):
                self.assertEqual(output, ["SUCCESS: re-add ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num].ip), ""])
            else:
                self.assertEqual(output, ["SUCCESS: re-add ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num].ip)])
            cli_command = "recovery"
            options = "--server-recovery={0}:8091 --recovery-type=delta".format(self.servers[nodes_add - nodes_rem - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            if (len(output) == 2):
                self.assertEqual(output, ["SUCCESS: setRecoveryType for node ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num].ip), ""])
            else:
                self.assertEqual(output, ["SUCCESS: setRecoveryType for node ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num].ip)])

        cli_command = "server-readd"
        for num in xrange(nodes_readd):
            self.log.info("add back node {0} to cluster".format(self.servers[nodes_add - nodes_rem - num ].ip))
            options = "--server-add={0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[nodes_add - nodes_rem - num ].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            if (len(output) == 2):
                self.assertEqual(output, ["SUCCESS: re-add ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num ].ip), ""])
            else:
                self.assertEqual(output, ["SUCCESS: re-add ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num ].ip)])

        cli_command = "rebalance"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        self.assertTrue("INFO: rebalancing . " in output[0])
        if (len(output) == 4):
            self.assertEqual("SUCCESS: rebalanced cluster", output[2])
        else:
            self.assertEqual("SUCCESS: rebalanced cluster", output[1])

        remote_client.disconnect()


    def testStartStopRebalance(self):
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        remote_client = RemoteMachineShellConnection(self.master)

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                  cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ["(u'notRunning', None)"])

        cli_command = "server-add"
        for num in xrange(nodes_add):
            options = "--server-add={0}:8091 --server-add-username=Administrator \
                       --server-add-password=password".format(self.servers[num + 1].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                                              options=options, cluster_host="localhost", \
                                                user="Administrator", password="password")
            self.assertEqual(output, ["Warning: Adding server from group-manage is deprecated",
                                      "Server {0}:8091 added".format(self.servers[num + 1].ip)])

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                  cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ["(u'notRunning', None)"])

        self._create_bucket(remote_client)

        cli_command = "rebalance"
        t = Thread(target=remote_client.execute_couchbase_cli, name="rebalance_after_add",
                       args=(cli_command, "localhost", '', None, "Administrator", "password"))
        t.start()
        self.sleep(5)

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                  cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ["(u'running', None)"])

        t.join()

        cli_command = "rebalance"
        for num in xrange(nodes_rem):
            options = "--server-remove={0}:8091".format(self.servers[nodes_add - num].ip)
            t = Thread(target=remote_client.execute_couchbase_cli, name="rebalance_after_add",
                       args=(cli_command, "localhost", options, None, "Administrator", "password"))
            t.start()
            self.sleep(5)
            cli_command = "rebalance-status"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                      cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["(u'running', None)"])


            cli_command = "rebalance-stop"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                      cluster_host="localhost", user="Administrator", password="password")

            t.join()

            cli_command = "rebalance"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                      cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[1], "SUCCESS: rebalanced cluster")


            cli_command = "rebalance-status"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                      cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["(u'notRunning', None)"])
        remote_client.disconnect()

    def testNodeInit(self):
        server = self.servers[-1]
        remote_client = RemoteMachineShellConnection(server)
        prefix = ''
        type = remote_client.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            prefix_path = "C:"

        data_path = self.input.param("data_path", None)
        index_path = self.input.param("index_path", None)

        if data_path is not None:
            data_path = prefix + data_path.replace('|', "/")
        if index_path is not None:
            index_path = prefix + index_path.replace('|', "/")

        server_info = self._get_cluster_info(remote_client, cluster_port=server.port, \
                              user=server.rest_username, password=server.rest_password)
        data_path_before = server_info["storage"]["hdd"][0]["path"]
        index_path_before = server_info["storage"]["hdd"][0]["index_path"]

        try:
            rest = RestConnection(server)
            rest.force_eject_node()
            cli_command = "node-init"
            options = ""
            options += ("--node-init-data-path={0} ".format(data_path), "")[data_path is None]
            options += ("--node-init-index-path={0} ".format(index_path), "")[index_path is None]
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                                              options=options, cluster_host="localhost", \
                                                user="Administrator", password="password")
            self.sleep(7)  # time needed to reload couchbase
            """ no output print out.  Bug https://issues.couchbase.com/browse/MB-13704
                output    []
                error    []
                It need to check when this bug is fixed """
            #self.assertEqual(output[0], "SUCCESS: init localhost")
            rest.init_cluster()
            server_info = self._get_cluster_info(remote_client, cluster_port=server.port, user=server.rest_username, password=server.rest_password)
            data_path_after = server_info["storage"]["hdd"][0]["path"]
            index_path_after = server_info["storage"]["hdd"][0]["index_path"]
            self.assertEqual((data_path, data_path_before)[data_path is None], data_path_after)
            self.assertEqual((index_path, index_path_before)[index_path is None], index_path_after)
        finally:
            rest = RestConnection(server)
            rest.force_eject_node()
            rest.init_cluster()

    def testClusterInit(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        data_ramsize = self.input.param("data-ramsize", None)
        index_ramsize = self.input.param("index-ramsize", None)
        fts_ramsize = self.input.param("fts-ramsize", None)
        name = self.input.param("name", None)
        index_storage_mode = self.input.param("index-storage-mode", None)
        port = self.input.param("port", None)
        services = self.input.param("services", None)
        initialized = self.input.param("initialized", False)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        initial_server = self.servers[-1]
        server = copy.deepcopy(initial_server)
        hostname = "%s:%s" % (server.ip, server.port)

        if not initialized:
            rest = RestConnection(server)
            rest.force_eject_node()

        options = ""
        if username:
            options += " --cluster-username " + str(username)
            server.rest_username = username
        if password:
            options += " --cluster-password " + str(password)
            server.rest_password = password
        if data_ramsize:
            options += " --cluster-ramsize " + str(data_ramsize)
        if index_ramsize:
            options += " --cluster-index-ramsize " + str(index_ramsize)
        if fts_ramsize:
            options += " --cluster-fts-ramsize " + str(fts_ramsize)
        if name:
            options += " --cluster-name " + str(name)
            # strip quotes if the cluster name contains spaces
            if (name[0] == name[-1]) and name.startswith(("'", '"')):
                name = name[1:-1]
        if index_storage_mode:
            options += " --index-storage-setting " + str(index_storage_mode)
        elif services and "index" in services:
            index_storage_mode = "forestdb"
        if port:
            options += " --cluster-port " + str(port)
        if services:
            options += " --services " + str(services)
        else:
            services = "data"

        remote_client = RemoteMachineShellConnection(server)
        output, error = remote_client.couchbase_cli("cluster-init", hostname, options)

        if not expect_error:
            # Update the cluster manager port if it was specified to be changed
            if port:
                server.port = port

            self.assertTrue(self.verifyCommandOutput(output, expect_error, "Cluster initialized"),
                            "Expected command to succeed")
            self.assertTrue(self.isClusterInitialized(server), "Cluster was not initialized")
            self.assertTrue(self.verifyServices(server, services), "Services do not match")
            self.assertTrue(self.verifyNotificationsEnabled(server), "Notification not enabled")
            self.assertTrue(self.verifyClusterName(server, name), "Cluster name does not match")

            if "index" in services:
                self.assertTrue(self.verifyIndexStorageMode(server, index_storage_mode),
                                "Index storage mode not properly set")

            self.assertTrue(self.verifyRamQuotas(server, data_ramsize, index_ramsize, fts_ramsize),
                            "Ram quotas not set properly")
        else:
            self.assertTrue(self.verifyCommandOutput(output, expect_error, error_msg), "Expected error message not found")
            if not initialized:
                self.assertTrue(not self.isClusterInitialized(server), "Cluster was initialized, but error was received")

        # Reset the cluster (This is important for when we change the port number)
        rest = RestConnection(server)
        rest.init_cluster(initial_server.rest_username, initial_server.rest_password, initial_server.port)

    def testSettingNotification(self):
        enable = self.input.param("enable", None)
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        initialized = self.input.param("initialized", False)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[-1])
        hostname = "%s:%s" % (server.ip, server.port)

        if not initialized:
            rest = RestConnection(server)
            rest.force_eject_node()

        options = ""
        if username is not None:
            options += " -u " + str(username)
            server.rest_username = username
        if password is not None:
            options += " -p " + str(password)
            server.rest_password = password
        if enable is not None:
            options += " --enable-notification " + str(enable)

        initialy_enabled = self.verifyNotificationsEnabled(server)

        remote_client = RemoteMachineShellConnection(server)
        output, error = remote_client.couchbase_cli("setting-notification", hostname, options)
        remote_client.disconnect()

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(output, expect_error, "Notification settings updated"),
                            "Expected command to succeed")
            if enable == 1:
                self.assertTrue(self.verifyNotificationsEnabled(server), "Notification not enabled")
            else:
                self.assertTrue(not self.verifyNotificationsEnabled(server), "Notification are enabled")
        else:
            self.assertTrue(self.verifyCommandOutput(output, expect_error, error_msg), "Expected error message not found")
            self.assertTrue(self.verifyNotificationsEnabled(server) == initialy_enabled, "Notifications changed after error")

    def testSettingCluster(self):
        self.clusterSettings("setting-cluster")

    def testClusterEdit(self):
        self.clusterSettings("cluster-edit")

    def clusterSettings(self, cmd):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        new_username = self.input.param("new-username", None)
        new_password = self.input.param("new-password", None)
        data_ramsize = self.input.param("data-ramsize", None)
        index_ramsize = self.input.param("index-ramsize", None)
        fts_ramsize = self.input.param("fts-ramsize", None)
        name = self.input.param("name", None)
        port = self.input.param("port", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        init_username = self.input.param("init-username", username)
        init_password = self.input.param("init-password", password)
        init_data_memory = 256
        init_index_memory = 256
        init_fts_memory = 256
        init_name = "testrunner"

        initial_server = self.servers[-1]
        server = copy.deepcopy(initial_server)
        hostname = "%s:%s" % (server.ip, server.port)

        rest = RestConnection(server)
        rest.force_eject_node()

        if initialized:
            if init_username is None:
                init_username = "Administrator"
            if init_password is None:
                init_password = "password"
            server.rest_username = init_username
            server.rest_password = init_password
            rest = RestConnection(server)
            self.assertTrue(rest.init_cluster(init_username, init_password),
                            "Cluster initialization failed during test setup")
            self.assertTrue(rest.init_cluster_memoryQuota(init_username, init_password, init_data_memory),
                            "Setting data service RAM quota failed during test setup")
            self.assertTrue(rest.set_indexer_memoryQuota(init_username, init_password, init_index_memory),
                            "Setting index service RAM quota failed during test setup")
            self.assertTrue(rest.set_fts_memoryQuota(init_username, init_password, init_fts_memory),
                            "Setting full-text service RAM quota failed during test setup")
            self.assertTrue(rest.set_cluster_name(init_name), "Setting cluster name failed during test setup")


        options = ""
        if username is not None:
            options += " -u " + str(username)
        if password is not None:
            options += " -p " + str(password)
        if new_username is not None:
            options += " --cluster-username " + str(new_username)
            if not expect_error:
                server.rest_username = new_username
        if new_password is not None:
            options += " --cluster-password " + str(new_password)
            if not expect_error:
                server.rest_password = new_password
        if data_ramsize:
            options += " --cluster-ramsize " + str(data_ramsize)
        if index_ramsize:
            options += " --cluster-index-ramsize " + str(index_ramsize)
        if fts_ramsize:
            options += " --cluster-fts-ramsize " + str(fts_ramsize)
        if name:
            options += " --cluster-name " + str(name)
            # strip quotes if the cluster name contains spaces
            if (name[0] == name[-1]) and name.startswith(("'", '"')):
                name = name[1:-1]
        if port:
            options += " --cluster-port " + str(port)

        remote_client = RemoteMachineShellConnection(server)
        output, error = remote_client.couchbase_cli(cmd, hostname, options)
        remote_client.disconnect()

        if not expect_error:
            # Update the cluster manager port if it was specified to be changed
            if port:
                server.port = port
            if data_ramsize is None:
                data_ramsize = init_data_memory
            if index_ramsize is None:
                index_ramsize = init_index_memory
            if fts_ramsize is None:
                fts_ramsize = init_fts_memory
            if name is None:
                name = init_name

            if cmd == "cluster-edit":
                self.verifyWarningOutput(output, "The cluster-edit command is depercated, use setting-cluster instead")
            self.assertTrue(self.verifyCommandOutput(output, expect_error, "Cluster settings modified"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyRamQuotas(server, data_ramsize, index_ramsize, fts_ramsize),
                            "Ram quotas not set properly")
            self.assertTrue(self.verifyClusterName(server, name), "Cluster name does not match")
        else:
            self.assertTrue(self.verifyCommandOutput(output, expect_error, error_msg), "Expected error message not found")
            if not initialized:
                self.assertTrue(not self.isClusterInitialized(server), "Cluster was initialized, but error was received")

        # Reset the cluster (This is important for when we change the port number)
        rest = RestConnection(server)
        self.assertTrue(rest.init_cluster(initial_server.rest_username, initial_server.rest_password, initial_server.port),
                   "Cluster was not re-initialized at the end of the test")

    def testBucketCreation(self):
        bucket_name = self.input.param("bucket", "default")
        bucket_type = self.input.param("bucket_type", "couchbase")
        bucket_port = self.input.param("bucket_port", 11211)
        bucket_replica = self.input.param("bucket_replica", 1)
        bucket_password = self.input.param("bucket_password", None)
        bucket_ramsize = self.input.param("bucket_ramsize", 200)
        wait = self.input.param("wait", False)
        enable_flush = self.input.param("enable_flush", None)
        enable_index_replica = self.input.param("enable_index_replica", None)

        remote_client = RemoteMachineShellConnection(self.master)
        rest = RestConnection(self.master)
        self._create_bucket(remote_client, bucket=bucket_name, bucket_type=bucket_type, bucket_port=bucket_port, bucket_password=bucket_password, \
                        bucket_ramsize=bucket_ramsize, bucket_replica=bucket_replica, wait=wait, enable_flush=enable_flush, enable_index_replica=enable_index_replica)
        buckets = rest.get_buckets()
        result = True
        if len(buckets) != 1:
            self.log.error("Expected to get only 1 bucket")
            result = False
        bucket = buckets[0]
        if bucket_name != bucket.name:
            self.log.error("Bucket name is not correct")
            result = False
        if not (bucket_port == bucket.nodes[0].moxi or bucket_port == bucket.port):
            self.log.error("Bucket port is not correct")
            result = False
        if bucket_type == "couchbase" and "membase" != bucket.type or\
            (bucket_type == "memcached" and "memcached" != bucket.type):
            self.log.error("Bucket type is not correct")
            result = False
        if bucket_type == "couchbase" and bucket_replica != bucket.numReplicas:
            self.log.error("Bucket num replica is not correct")
            result = False
        if bucket.saslPassword != (bucket_password, "")[bucket_password is None]:
            self.log.error("Bucket password is not correct")
            result = False
        if bucket_ramsize * 1048576 not in range(int(int(buckets[0].stats.ram) * 0.95), int(int(buckets[0].stats.ram) * 1.05)):
            self.log.error("Bucket RAM size is not correct")
            result = False

        if not result:
            self.fail("Bucket was created with incorrect properties")

        remote_client.disconnect()

    def testIndexerSettings(self):
        cli_command = "setting-index"
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)

        options = (" --index-threads=3")
        options += (" --index-max-rollback-points=6")
        options += (" --index-stable-snapshot-interval=4900")
        options += (" --index-memory-snapshot-interval=220")
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                                          options=options, cluster_host="localhost", \
                                            user="Administrator", password="password")
        self.assertEqual(output, ['SUCCESS: set index settings'])
        remote_client.disconnect()

    def testBucketModification(self):
        cli_command = "bucket-edit"
        bucket_password = self.input.param("bucket_password", None)
        bucket_port = self.input.param("bucket_port", 11211)
        enable_flush = self.input.param("enable_flush", None)
        self.testBucketCreation()
        bucket_port_new = self.input.param("bucket_port_new", None)
        bucket_password_new = self.input.param("bucket_password_new", None)
        bucket_ramsize_new = self.input.param("bucket_ramsize_new", None)
        enable_flush_new = self.input.param("enable_flush_new", None)
        enable_index_replica_new = self.input.param("enable_index_replica_new", None)
        bucket_ramsize_new = self.input.param("bucket_ramsize_new", None)
        bucket = self.input.param("bucket", "default")
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)

        options = "--bucket={0}".format(bucket)
        options += (" --enable-flush={0}".format(enable_flush_new), \
                                        "")[enable_flush_new is None]
        options += (" --enable-index-replica={0}".format(enable_index_replica_new), \
                                                "")[enable_index_replica_new is None]
        options += (" --bucket-port={0}".format(bucket_port_new), \
                                               "")[bucket_port_new is None]
        options += (" --bucket-password={0}".format(bucket_password_new), \
                                           "")[bucket_password_new is None]
        options += (" --bucket-ramsize={0}".format(bucket_ramsize_new), \
                                            "")[bucket_ramsize_new is None]

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                                          options=options, cluster_host="localhost", \
                                            user="Administrator", password="password")
        self.assertEqual(output, ['SUCCESS: bucket-edit'])

        if bucket_password_new is not None:
            bucket_password_new = bucket_password
        if bucket_port_new is not None:
            bucket_port = bucket_port_new
        if bucket_ramsize_new is not None:
            bucket_ramsize = bucket_ramsize_new

        buckets = rest.get_buckets()
        result = True
        if len(buckets) != 1:
            self.log.error("Expected to ge only 1 bucket")
            result = False
        bucket = buckets[0]
        if not (bucket_port == bucket.nodes[0].moxi or bucket_port == bucket.port):
            self.log.error("Bucket port is not correct")
            result = False
        if bucket.saslPassword != (bucket_password, "")[bucket_password is None]:
            self.log.error("Bucket password is not correct")
            result = False
        if bucket_ramsize * 1048576 not in range(int(int(buckets[0].stats.ram) * 0.95), \
                                                  int(int(buckets[0].stats.ram) * 1.05)):
            self.log.error("Bucket RAM size is not correct")
            result = False

        if not result:
            self.fail("Bucket was created with incorrect properties")

        options = ""
        cli_command = "bucket-flush"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                           options=options, cluster_host="localhost",
                                             user="Administrator", password="password")
        self.assertEqual(output, ['Running this command will totally PURGE database'
                     ' data from disk. Do you really want to do it? (Yes/No)TIMED OUT:'
            ' command: bucket-flush: localhost:8091, most likely bucket is not flushed'])
        if not enable_flush:
            cli_command = "bucket-flush --force"
            options = "--bucket={0}".format(bucket)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                              options=options, cluster_host="localhost",
                                              user="Administrator", password="password")
            if self.node_version[:5] in COUCHBASE_FROM_WATSON:
                self.assertTrue(self._check_output("ERROR:", output))
            else:
                self.assertEqual(output, ['Database data will be purged from disk ...',
                'ERROR: unable to bucket-flush; please check if the bucket exists or not;'
                     ' (400) Bad Request', "{u'_': u'Flush is disabled for the bucket'}"])
            cli_command = "bucket-edit"
            options = "--bucket={0} --enable-flush=1 --bucket-ramsize=500".format(bucket)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                              options=options, cluster_host="localhost",
                                              user="Administrator", password="password")
            self.assertEqual(output, ['SUCCESS: bucket-edit'])

        cli_command = "bucket-flush --force"
        options = "--bucket={0}".format(bucket)
        if enable_flush_new is None:
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                              options=options, cluster_host="localhost",
                                              user="Administrator", password="password")
            self.assertEqual(output, ['Database data will be purged from disk ...',
                                                               'SUCCESS: bucket-flush'])
        elif int(enable_flush_new) == 0:
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                              options=options, cluster_host="localhost",
                                              user="Administrator", password="password")
            self.assertEqual(output, ['Database data will be purged from disk ...',
                                   'ERROR: unable to bucket-flush; please check if the '
                                              'bucket exists or not; (400) Bad Request',
                                         '{"_":"Flush is disabled for the bucket"}'])

        cli_command = "bucket-delete"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                          options=options, cluster_host="localhost",
                                          user="Administrator", password="password")
        self.assertEqual(output, ['SUCCESS: bucket-delete'])

        remote_client.disconnect()

    # MB-8566
    def testSettingCompacttion(self):
        '''setting-compacttion OPTIONS:
        --compaction-db-percentage=PERCENTAGE     at which point database compaction is triggered
        --compaction-db-size=SIZE[MB]             at which point database compaction is triggered
        --compaction-view-percentage=PERCENTAGE   at which point view compaction is triggered
        --compaction-view-size=SIZE[MB]           at which point view compaction is triggered
        --compaction-period-from=HH:MM            allow compaction time period from
        --compaction-period-to=HH:MM              allow compaction time period to
        --enable-compaction-abort=[0|1]           allow compaction abort when time expires
        --enable-compaction-parallel=[0|1]        allow parallel compaction for database and view'''
        compaction_db_percentage = self.input.param("compaction-db-percentage", None)
        compaction_db_size = self.input.param("compaction-db-size", None)
        compaction_view_percentage = self.input.param("compaction-view-percentage", None)
        compaction_view_size = self.input.param("compaction-view-size", None)
        compaction_period_from = self.input.param("compaction-period-from", None)
        compaction_period_to = self.input.param("compaction-period-to", None)
        enable_compaction_abort = self.input.param("enable-compaction-abort", None)
        enable_compaction_parallel = self.input.param("enable-compaction-parallel", None)
        bucket = self.input.param("bucket", "default")
        output = self.input.param("output", '')
        rest = RestConnection(self.master)
        remote_client = RemoteMachineShellConnection(self.master)
        self.testBucketCreation()
        cli_command = "setting-compacttion"
        options = "--bucket={0}".format(bucket)
        options += (" --compaction-db-percentage={0}".format(compaction_db_percentage), "")[compaction_db_percentage is None]
        options += (" --compaction-db-size={0}".format(compaction_db_size), "")[compaction_db_size is None]
        options += (" --compaction-view-percentage={0}".format(compaction_view_percentage), "")[compaction_view_percentage is None]
        options += (" --compaction-view-size={0}".format(compaction_view_size), "")[compaction_view_size is None]
        options += (" --compaction-period-from={0}".format(compaction_period_from), "")[compaction_period_from is None]
        options += (" --compaction-period-to={0}".format(compaction_period_to), "")[compaction_period_to is None]
        options += (" --enable-compaction-abort={0}".format(enable_compaction_abort), "")[enable_compaction_abort is None]
        options += (" --enable-compaction-parallel={0}".format(enable_compaction_parallel), "")[enable_compaction_parallel is None]

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ['SUCCESS: bucket-edit'])
        cluster_status = rest.cluster_status()
        remote_client.disconnect()

    """ tests for the group-manage option. group creation, renaming and deletion are tested .
        These tests require a cluster of four or more nodes. """
    def testCreateRenameDeleteGroup(self):
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "group-manage"

        if self.os == "linux":
            # create group
            options = " --create --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            output = self.del_runCmd_value(output)
            self.assertEqual(output, ["SUCCESS: group created group2"])
            # create existing group. operation should fail
            options = " --create --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            output = self.del_runCmd_value(output)
            self.assertEqual(output[0], "ERROR: unable to create group group2 (400) Bad Request")
            self.assertEqual(output[1], '{"name":"already exists"}')
            # rename group test
            options = " --rename=group3 --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            output = self.del_runCmd_value(output)
            self.assertEqual(output, ["SUCCESS: group renamed group2"])
            # delete group test
            options = " --delete --group-name=group3"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            output = self.del_runCmd_value(output)
            self.assertEqual(output, ["SUCCESS: group deleted group3"])
            # delete non-empty group test
            options = " --delete --group-name=\"Group 1\""
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            output = self.del_runCmd_value(output)
            self.assertEqual(output[0], "ERROR: unable to delete group Group 1 (400) Bad Request")
            self.assertEqual(output[1], '{"_":"group is not empty"}')
            # delete non-existing group
            options = " --delete --group-name=groupn"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            output = self.del_runCmd_value(output)
            self.assertEqual(output, ["ERROR: invalid group name:groupn"])
            remote_client.disconnect()

        if self.os == "windows":
            # create group
            options = " --create --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[0], "SUCCESS: group created group2")
            # create existing group. operation should fail
            options = " --create --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[0], "ERROR: unable to create group group2 (400) Bad Request")
            self.assertEqual(output[2], '{"name":"already exists"}')
            # rename group test
            options = " --rename=group3 --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[0], "SUCCESS: group renamed group2")
            # delete group test
            options = " --delete --group-name=group3"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[0], "SUCCESS: group deleted group3")
            # delete non-empty group test
            options = " --delete --group-name=\"Group 1\""
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[0], "ERROR: unable to delete group Group 1 (400) Bad Request")
            self.assertEqual(output[2], '{"_":"group is not empty"}')
            # delete non-existing group
            options = " --delete --group-name=groupn"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[0], "ERROR: invalid group name:groupn")
            remote_client.disconnect()

    """ tests for the group-manage option. adding and moving servers between groups are tested. 
        These tests require a cluster of four or more nodes. """
    def testAddMoveServerListGroup(self):
        nodes_add = self.input.param("nodes_add", 1)
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "group-manage"
        if self.os == "linux":
            # create a group to use in this testcase
            options = " --create --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            output = self.del_runCmd_value(output)
            self.assertEqual(output, ["SUCCESS: group created group2"])
            # add multiple servers to group
            for num in xrange(nodes_add):
                options = "--add-servers=\"{0}:8091;{1}:8091\" --group-name=group2 \
                           --server-add-username=Administrator --server-add-password=password" \
                           .format(self.servers[num + 1].ip, self.servers[num + 2].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="{0}:8091".format(self.servers[num].ip), \
                        user="Administrator", password="password")
                output = self.del_runCmd_value(output)
                # This one is before Watson
                #self.assertEqual(output[0], "SUCCESS: add server {0}:8091' to group 'group2'" \
                #                                              .format(self.servers[num + 1].ip))
                self.assertEqual(output[0], "Server {0}:8091 added to group group2" \
                                                              .format(self.servers[num + 1].ip))
                """Server 172.23.105.114:8091 added to group group2"""
                # This one is before Watson
                #self.assertEqual(output[1], "SUCCESS: add server '{0}:8091' to group 'group2'" \
                #                                              .format(self.servers[num + 2].ip))
                self.assertEqual(output[1], "Server {0}:8091 added to group group2" \
                                                              .format(self.servers[num + 2].ip))
            # add single server to group
            for num in xrange(nodes_add):
                options = "--add-servers={0}:8091 --group-name=group2 \
                           --server-add-username=Administrator --server-add-password=password" \
                           .format(self.servers[num + 3].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                output = self.del_runCmd_value(output)
                # This one is before Watson
                #self.assertEqual(output, ["SUCCESS: add server '{0}:8091' to group 'group2'" \
                #                                           .format(self.servers[num + 3].ip)])
                self.assertEqual(output, ["Server {0}:8091 added to group group2" \
                                                           .format(self.servers[num + 3].ip)])
            # list servers in group
            for num in xrange(nodes_add):
                options = " --list --group-name=group2"
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                output = self.del_runCmd_value(output)
                self.assertEqual(output[0], "group2")
                self.assertEqual(output[1], " server: {0}:8091".format(self.servers[num + 1].ip))
                self.assertEqual(output[2], " server: {0}:8091".format(self.servers[num + 2].ip))
                self.assertEqual(output[3], " server: {0}:8091".format(self.servers[num + 3].ip))
            # test move multiple servers
            for num in xrange(nodes_add):
                options = "--from-group=group2 --to-group=\"Group 1\" \
                           --move-servers=\"{0}:8091;{1}:8091;{2}:8091\"".format(self.servers[num + 1].ip, \
                                                       self.servers[num + 2].ip, self.servers[num + 3].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                output = self.del_runCmd_value(output)
                self.assertEqual(output, ["SUCCESS: move servers from group 'group2' to group 'Group 1'"])
            # clean up by deleting group
            options = " --delete --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            output = self.del_runCmd_value(output)
            self.assertEqual(output, ["SUCCESS: group deleted group2"])

        if self.os == "windows":
            # create a group to use in this testcase
            options = " --create --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[0], "SUCCESS: group created group2")
            # add multiple servers to group
            for num in xrange(nodes_add):
                options = "--add-servers=\"{0}:8091;{1}:8091\" --group-name=group2 \
                    --server-add-username=Administrator --server-add-password=password" \
                    .format(self.servers[num + 1].ip, self.servers[num + 2].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="{0}:8091".format(self.servers[num].ip), \
                        user="Administrator", password="password")
                self.assertEqual(output[0], "SUCCESS: add server '{0}:8091' to group 'group2'" \
                                 .format(self.servers[num + 1].ip))
                self.assertEqual(output[2], "SUCCESS: add server '{0}:8091' to group 'group2'" \
                                 .format(self.servers[num + 2].ip))
            # add single server to group
            for num in xrange(nodes_add):
                options = "--add-servers={0}:8091 --group-name=group2 --server-add-username=Administrator \
                           --server-add-password=password".format(self.servers[num + 3].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                self.assertEqual(output[0], "SUCCESS: add server '{0}:8091' to group 'group2'" \
                                 .format(self.servers[num + 3].ip))
            # list servers in group
            for num in xrange(nodes_add):
                options = " --list --group-name=group2"
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                self.assertEqual(output[0], "group2")
                self.assertEqual(output[2], " server: {0}:8091".format(self.servers[num + 1].ip))
                self.assertEqual(output[4], " server: {0}:8091".format(self.servers[num + 2].ip))
                self.assertEqual(output[6], " server: {0}:8091".format(self.servers[num + 3].ip))
            # test move multiple servers
            for num in xrange(nodes_add):
                options = "--from-group=group2 --to-group=\"Group 1\" \
                --move-servers=\"{0}:8091;{1}:8091;{2}:8091\"".format(self.servers[num + 1].ip, \
                                            self.servers[num + 2].ip, self.servers[num + 3].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                self.assertEqual(output[0], "SUCCESS: move servers from group 'group2' to group 'Group 1'")
            # clean up by deleting group
            options = " --delete --group-name=group2"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                    options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[0], "SUCCESS: group deleted group2")

    """ tests for the server-add option with group manage rider.
        These tests require a cluster of four or more nodes. """
    def testServerAddRebalancewithGroupManage(self):
        nodes_add = self.input.param("nodes_add", 1)
        remote_client = RemoteMachineShellConnection(self.master)

        if self.os == "linux":
            # test server-add command with group manage option
            cli_command = "server-add"
            for num in xrange(nodes_add):
                options = "--server-add={0}:8091 --server-add-username=Administrator \
                           --server-add-password=password --group-name=\"Group 1\"" \
                           .format(self.servers[num + 1].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                output = self.del_runCmd_value(output)
                # This one is before Watson
                #self.assertEqual(output, ["SUCCESS: add server '{0}:8091' to group 'Group 1'" \
                #                                            .format(self.servers[num + 1].ip)])
                self.assertEqual(output[1], "Server {0}:8091 added to group Group 1" \
                                                            .format(self.servers[num + 1].ip))

            # test rebalance command with add server and group manage option
            cli_command = "rebalance"
            for num in xrange(nodes_add):
                options = "--server-add={0}:8091 --server-add-username=Administrator \
                           --server-add-password=password --group-name=\"Group 1\"" \
                                                    .format(self.servers[num + 2].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                output = self.del_runCmd_value(output)
                # This one before watson
                #self.assertEqual(output[0], "SUCCESS: add server '{0}:8091' to group 'Group 1'" \
                #                                               .format(self.servers[num + 2].ip))
                self.assertEqual(output[1], "Server {0}:8091 added to group Group 1" \
                                                               .format(self.servers[num + 2].ip))

                self.assertTrue(self._check_output("SUCCESS: rebalanced cluster", output))

            for num in xrange(nodes_add):
                options = "--server-remove={0}:8091 --server-add={1}:8091 \
                           --server-add-username=Administrator --server-add-password=password \
                           --group-name=\"Group 1\"".format(self.servers[num + 2].ip, self.servers[num + 3].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                output = self.del_runCmd_value(output)
                # This one before watson
                #self.assertEqual(output[0], "SUCCESS: add server '{0}:8091' to group 'Group 1'" \
                self.assertTrue(self._check_output("Server %s:8091 added" %self.servers[num + 3].ip, output))
                self.assertTrue(self._check_output("SUCCESS: rebalanced cluster", output))


        if self.os == "windows":
            # test server-add command with group manage option
            cli_command = "server-add"
            for num in xrange(nodes_add):
                options = "--server-add={0}:8091 --server-add-username=Administrator \
                           --server-add-password=password --group-name=\"Group 1\""  \
                                                     .format(self.servers[num + 1].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                self.assertEqual(output[0], "SUCCESS: add server '{0}:8091' to group 'Group 1'" \
                                                            .format(self.servers[num + 1].ip))
            # test rebalance command with add server and group manage option
            cli_command = "rebalance"
            for num in xrange(nodes_add):
                options = "--server-add={0}:8091 --server-add-username=Administrator \
                           --server-add-password=password --group-name=\"Group 1\"" \
                                                    .format(self.servers[num + 2].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                self.assertEqual(output[4], "SUCCESS: add server '{0}:8091' to group 'Group 1'" \
                                                               .format(self.servers[num + 2].ip))
                # old before watson
                #self.assertEqual(output[2], "SUCCESS: rebalanced cluster")
                self.assertTrue(self._check_output("SUCCESS: rebalanced cluster", output))

            for num in xrange(nodes_add):
                options = "--server-remove={0}:8091 --server-add={1}:8091 \
                           --server-add-username=Administrator --server-add-password=password \
                           --group-name=\"Group 1\"".format(self.servers[num + 2].ip, self.servers[num + 3].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user="Administrator", password="password")
                self.assertEqual(output[0], "SUCCESS: add server '{0}:8091' to group 'Group 1'" \
                                                               .format(self.servers[num + 3].ip))
                self.assertTrue("INFO: rebalancing" in output[1])
                self.assertEqual(output[2], "SUCCESS: rebalanced cluster")

    def test_change_admin_password_with_read_only_account(self):
        """ this test automation for bug MB-20170.
            In the bug, when update password of admin, read only account is removed.
            This test is to maker sure read only account stay after update password
            of Administrator. """
        if self.cb_version[:5] in COUCHBASE_FROM_SHERLOCK:
            readonly_user = "readonlyuser_1"
            curr_passwd = "password"
            update_passwd = "password_1"
            try:
                self.log.info("remove any readonly user in cluster ")
                output, error = self.shell.execute_command("%scouchbase-cli "
                                             "user-manage --list -c %s:8091 "
                                             "-u Administrator -p %s "
                           % (self.cli_command_path, self.shell.ip, curr_passwd))
                self.log.info("read only user in this cluster %s" % output)
                if output:
                    for user in output:
                        output, error = self.shell.execute_command("%scouchbase-cli "
                                                    "user-manage --delete -c %s:8091 "
                                                    "--ro-username=%s "
                                                    "-u Administrator -p %s "
                               % (self.cli_command_path, self.shell.ip, user,
                                                                curr_passwd))
                        self.log.info("%s" % output)
                self.log.info("create a read only user account")
                output, error = self.shell.execute_command("%scouchbase-cli "
                                              "user-manage -c %s:8091 --set "
                                              "--ro-username=%s "
                                            "--ro-password=readonlypassword "
                                              "-u Administrator -p %s "
                                      % (self.cli_command_path, self.shell.ip,
                                                  readonly_user, curr_passwd))
                self.log.info("%s" % output)
                output, error = self.shell.execute_command("%scouchbase-cli "
                                             "user-manage --list -c %s:8091 "
                                              "-u Administrator -p %s "
                                                    % (self.cli_command_path,
                                                 self.shell.ip, curr_passwd))
                self.log.info("readonly user craeted in this cluster %s" % output)
                self.log.info("start update Administrator password")
                output, error = self.shell.execute_command("%scouchbase-cli "
                                                   "cluster-edit -c %s:8091 "
                                                    "-u Administrator -p %s "
                                              "--cluster-username=Administrator "
                                              "--cluster-password=%s"
                                         % (self.cli_command_path, self.shell.ip,
                                                     curr_passwd, update_passwd))
                self.log.info("%s" % output)
                self.log.info("verify if readonly user: %s still exist "
                                                                 % readonly_user )
                output, error = self.shell.execute_command("%scouchbase-cli "
                                             "user-manage --list -c %s:8091 "
                                              "-u Administrator -p %s "
                                         % (self.cli_command_path, self.shell.ip,
                                                                  update_passwd))
                self.log.info(" current readonly user in cluster: %s" % output)
                self.assertTrue(self._check_output("%s" % readonly_user, output))
            finally:
                self.log.info("reset password back to original in case test failed")
                output, error = self.shell.execute_command("%scouchbase-cli "
                                                   "cluster-edit -c %s:8091 "
                                                    "-u Administrator -p %s "
                                              "--cluster-username=Administrator "
                                              "--cluster-password=%s"
                                         % (self.cli_command_path, self.shell.ip,
                                                     update_passwd, curr_passwd))
        else:
            self.log.info("readonly account does not support on this version")

    def _check_output(self, word_check, output):
        found = False
        if len(output) >=1 :
            for x in output:
                if word_check in x:
                    self.log.info("Found \"%s\" in CLI output" % word_check)
                    found = True
        return found


class XdcrCLITest(CliBaseTest):
    XDCR_SETUP_SUCCESS = {
                          "create": "SUCCESS: init/edit CLUSTERNAME",
                          "edit": "SUCCESS: init/edit CLUSTERNAME",
                          "delete": "SUCCESS: delete CLUSTERNAME"
    }
    XDCR_REPLICATE_SUCCESS = {
                          "create": "SUCCESS: start replication",
                          "delete": "SUCCESS: delete replication",
                          "pause": "SUCCESS: pause replication",
                          "resume": "SUCCESS: resume replication"
    }
    SSL_MANAGE_SUCCESS = {'retrieve': "SUCCESS: retrieve certificate to \'PATH\'",
                           'regenerate': "SUCCESS: regenerate certificate to \'PATH\'"
                           }
    def setUp(self):
        TestInputSingleton.input.test_params["default_bucket"] = False
        super(XdcrCLITest, self).setUp()
        self.__user = "Administrator"
        self.__password = "password"

    def tearDown(self):
        for server in self.servers:
            rest = RestConnection(server)
            rest.remove_all_remote_clusters()
            rest.remove_all_replications()
            rest.remove_all_recoveries()
        super(XdcrCLITest, self).tearDown()

    def __execute_cli(self, cli_command, options, cluster_host="localhost"):
        return self.shell.execute_couchbase_cli(
                                                cli_command=cli_command,
                                                options=options,
                                                cluster_host=cluster_host,
                                                user=self.__user,
                                                password=self.__password)

    def __xdcr_setup_create(self):
        # xdcr_hostname=the number of server in ini file to add to master as replication
        xdcr_cluster_name = self.input.param("xdcr-cluster-name", None)
        xdcr_hostname = self.input.param("xdcr-hostname", None)
        xdcr_username = self.input.param("xdcr-username", None)
        xdcr_password = self.input.param("xdcr-password", None)
        demand_encyrption = self.input.param("demand-encryption", 0)
        xdcr_cert = self.input.param("xdcr-certificate", None)
        wrong_cert = self.input.param("wrong-certificate", None)

        cli_command = "xdcr-setup"
        options = "--create"
        options += (" --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name),\
                                                "")[xdcr_cluster_name is None]
        if xdcr_hostname is not None:
            options += " --xdcr-hostname={0}".format(self.servers[xdcr_hostname].ip)
        options += (" --xdcr-username={0}".format(xdcr_username), "")[xdcr_username is None]
        options += (" --xdcr-password={0}".format(xdcr_password), "")[xdcr_password is None]
        options += (" --xdcr-demand-encryption={0}".format(demand_encyrption))

        if demand_encyrption and xdcr_hostname is not None and xdcr_cert:
            if wrong_cert:
                cluster_host = "localhost"
            else:
                cluster_host = self.servers[xdcr_hostname].ip
            cert_info = "--retrieve-cert"
            if self.cb_version[:5] in COUCHBASE_FROM_SPOCK:
                cert_info = "--cluster-cert-info"
                output, _ = self.__execute_cli(cli_command="ssl-manage", options="{0} "\
                                         .format(cert_info), cluster_host=cluster_host)
                cert_file = open("cert.pem", "w")
                """ cert must be in format PEM-encoded x509.  Need to add newline
                    at the end of each line. """
                for item in output:
                    cert_file.write("%s\n" % item)
                cert_file.close()
                self.shell.copy_file_local_to_remote("cert.pem",
                                                self.root_path + "cert.pem")
                os.system("rm -f cert.pem")
            else:
                output, _ = self.__execute_cli(cli_command="ssl-manage", options="{0}={1}"\
                              .format(cert_info, xdcr_cert), cluster_host=cluster_host)
            options += (" --xdcr-certificate={0}".format(xdcr_cert),\
                                               "")[xdcr_cert is None]
            if self.cb_version[:5] in COUCHBASE_FROM_SPOCK:
                self.assertTrue(self._check_output("-----END CERTIFICATE-----",
                                                                       output))
            else:
                self.assertNotEqual(output[-1].find("SUCCESS"), -1,\
                     "ssl-manage CLI failed to retrieve certificate")

        output, error = self.__execute_cli(cli_command=cli_command, options=options)
        return output, error, xdcr_cluster_name, xdcr_hostname, cli_command, options

    def testXDCRSetup(self):
        error_expected_in_command = self.input.param("error-expected", None)
        output, _, xdcr_cluster_name, xdcr_hostname, cli_command, options = \
                                                         self.__xdcr_setup_create()
        if error_expected_in_command != "create":
            self.assertEqual(XdcrCLITest.XDCR_SETUP_SUCCESS["create"].replace("CLUSTERNAME",\
                              (xdcr_cluster_name, "")[xdcr_cluster_name is None]), output[0])
        else:
            output_error = self.input.param("output_error", "[]")
            if output_error.find("CLUSTERNAME") != -1:
                output_error = output_error.replace("CLUSTERNAME",\
                                               (xdcr_cluster_name,\
                                               "")[xdcr_cluster_name is None])
            if output_error.find("HOSTNAME") != -1:
                output_error = output_error.replace("HOSTNAME",\
                               (self.servers[xdcr_hostname].ip,\
                                     "")[xdcr_hostname is None])

            for element in output:
                self.log.info("element {0}".format(element))
                if "ERROR: unable to set up xdcr remote site remote (400) Bad Request"\
                                                                            in element:
                    self.log.info("match {0}".format(element))
                    return True
                elif "Error: hostname (ip) is missing" in element:
                    self.log.info("match {0}".format(element))
                    return True
            self.assertFalse("output string did not match")

        # MB-8570 can't edit xdcr-setup through couchbase-cli
        if xdcr_cluster_name:
            options = options.replace("--create ", "--edit ")
            output, _ = self.__execute_cli(cli_command=cli_command, options=options)
            self.assertEqual(XdcrCLITest.XDCR_SETUP_SUCCESS["edit"]\
                                     .replace("CLUSTERNAME", (xdcr_cluster_name, "")\
                                            [xdcr_cluster_name is None]), output[0])
        if not xdcr_cluster_name:
            """ MB-8573 couchbase-cli: quotes are not supported when try to remove
                remote xdcr cluster that has white spaces in the name
            """
            options = "--delete --xdcr-cluster-name=\'{0}\'".format("remote cluster")
        else:
            options = "--delete --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name)
        output, _ = self.__execute_cli(cli_command=cli_command, options=options)
        if error_expected_in_command != "delete":
            self.assertEqual(XdcrCLITest.XDCR_SETUP_SUCCESS["delete"]\
                        .replace("CLUSTERNAME", (xdcr_cluster_name, "remote cluster")\
                                             [xdcr_cluster_name is None]), output[0])
        else:
            xdcr_cluster_name = "unknown"
            options = "--delete --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name)
            output, _ = self.__execute_cli(cli_command=cli_command, options=options)
            output_error = self.input.param("output_error", "[]")
            if output_error.find("CLUSTERNAME") != -1:
                output_error = output_error.replace("CLUSTERNAME", \
                                   (xdcr_cluster_name, "")[xdcr_cluster_name is None])
            if output_error.find("HOSTNAME") != -1:
                output_error = output_error.replace("HOSTNAME", \
                          (self.servers[xdcr_hostname].ip, "")[xdcr_hostname is None])
            if self.cb_version[:5] in COUCHBASE_FROM_SPOCK:
                expect_error = ("['ERROR: unable to delete xdcr remote site localhost "
                                  "(404) Object Not Found', 'unknown remote cluster']")
                if output_error == expect_error:
                    output_error = "ERROR: unable to delete xdcr remote site"
                self.assertTrue(self._check_output(output_error, output))
            else:
                self.assertEqual(output, eval(output_error))
            return

    def testXdcrReplication(self):
        '''xdcr-replicate OPTIONS:
        --create                               create and start a new replication
        --delete                               stop and cancel a replication
        --list                                 list all xdcr replications
        --xdcr-from-bucket=BUCKET              local bucket name to replicate from
        --xdcr-cluster-name=CLUSTERNAME        remote cluster to replicate to
        --xdcr-to-bucket=BUCKETNAME            remote bucket to replicate to'''
        to_bucket = self.input.param("xdcr-to-bucket", None)
        from_bucket = self.input.param("xdcr-from-bucket", None)
        error_expected = self.input.param("error-expected", False)
        replication_mode = self.input.param("replication_mode", None)
        pause_resume = self.input.param("pause-resume", None)
        checkpoint_interval = self.input.param("checkpoint_interval", None)
        source_nozzles = self.input.param("source_nozzles", None)
        target_nozzles = self.input.param("target_nozzles", None)
        filter_expression = self.input.param("filter_expression", None)
        max_replication_lag = self.input.param("max_replication_lag", None)
        timeout_perc_cap = self.input.param("timeout_perc_cap", None)
        _, _, xdcr_cluster_name, xdcr_hostname, _, _ = self.__xdcr_setup_create()
        cli_command = "xdcr-replicate"
        options = "--create"
        options += (" --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name), "")[xdcr_cluster_name is None]
        options += (" --xdcr-from-bucket=\'{0}\'".format(from_bucket), "")[from_bucket is None]
        options += (" --xdcr-to-bucket=\'{0}\'".format(to_bucket), "")[to_bucket is None]
        options += (" --xdcr-replication-mode=\'{0}\'".format(replication_mode), "")[replication_mode is None]
        options += (" --source-nozzle-per-node=\'{0}\'".format(source_nozzles), "")[source_nozzles is None]
        options += (" --target-nozzle-per-node=\'{0}\'".format(target_nozzles), "")[target_nozzles is None]
        options += (" --filter-expression=\'{0}\'".format(filter_expression), "")[filter_expression is None]
        options += (" --checkpoint-interval=\'{0}\'".format(checkpoint_interval), "")[timeout_perc_cap is None]

        self.bucket_size = self._get_bucket_size(self.quota, 1)
        if from_bucket:
            self.cluster.create_default_bucket(self.master, self.bucket_size, self.num_replicas,
                                               enable_replica_index=self.enable_replica_index)
        if to_bucket:
            self.cluster.create_default_bucket(self.servers[xdcr_hostname], self.bucket_size, self.num_replicas,
                                               enable_replica_index=self.enable_replica_index)
        output, _ = self.__execute_cli(cli_command, options)
        if not error_expected:
            self.assertEqual(XdcrCLITest.XDCR_REPLICATE_SUCCESS["create"], output[0])
        else:
            return

        self.sleep(8)
        options = "--list"
        output, _ = self.__execute_cli(cli_command, options)
        for value in output:
            if value.startswith("stream id"):
                replicator = value.split(":")[1].strip()
                if pause_resume is not None:
                    # pause replication
                    options = "--pause"
                    options += (" --xdcr-replicator={0}".format(replicator))
                    output, _ = self.__execute_cli(cli_command, options)
                    # validate output message
                    self.assertEqual(XdcrCLITest.XDCR_REPLICATE_SUCCESS["pause"], output[0])
                    self.sleep(20)
                    options = "--list"
                    output, _ = self.__execute_cli(cli_command, options)
                    # check if status of replication is "paused"
                    for value in output:
                        if value.startswith("status"):
                            self.assertEqual(value.split(":")[1].strip(), "paused")
                    self.sleep(20)
                    # resume replication
                    options = "--resume"
                    options += (" --xdcr-replicator={0}".format(replicator))
                    output, _ = self.__execute_cli(cli_command, options)
                    self.assertEqual(XdcrCLITest.XDCR_REPLICATE_SUCCESS["resume"], output[0])
                    # check if status of replication is "running"
                    options = "--list"
                    output, _ = self.__execute_cli(cli_command, options)
                    for value in output:
                        if value.startswith("status"):
                            self.assertEqual(value.split(":")[1].strip(), "running")
                options = "--delete"
                options += (" --xdcr-replicator={0}".format(replicator))
                output, _ = self.__execute_cli(cli_command, options)
                self.assertEqual(XdcrCLITest.XDCR_REPLICATE_SUCCESS["delete"], output[0])

    def testSSLManage(self):
        '''ssl-manage OPTIONS:
        --retrieve-cert=CERTIFICATE    retrieve cluster certificate AND save to a pem file
        --regenerate-cert=CERTIFICATE  regenerate cluster certificate AND save to a pem file'''
        if self.input.param("xdcr-certificate", None) is not None:
            xdcr_cert = self.input.param("xdcr-certificate", None)
        else:
            self.fail("need params xdcr-certificate to run")
        cli_command = "ssl-manage"
        cert_info = "--retrieve-cert"
        if self.cb_version[:5] in COUCHBASE_FROM_SPOCK:
            cert_info = "--cluster-cert-info"
            output, error = self.__execute_cli(cli_command=cli_command,
                                                           options=cert_info)
            cert_file = open("cert.pem", "w")
            """ cert must be in format PEM-encoded x509.  Need to add newline
                at the end of each line. """
            for item in output:
                cert_file.write("%s\n" % item)
            cert_file.close()
            self.shell.copy_file_local_to_remote(xdcr_cert,
                                            self.root_path + xdcr_cert)
            os.system("rm -f %s " % xdcr_cert)
        else:
            options = "{0}={1}".format(cert_info, xdcr_cert)
            output, error = self.__execute_cli(cli_command=cli_command, options=options)
        self.assertFalse(error, "Error thrown during CLI execution %s" % error)
        if self.cb_version[:5] in COUCHBASE_FROM_SPOCK:
                self.assertTrue(self._check_output("-----END CERTIFICATE-----", output))
        else:
           self.assertEqual(XdcrCLITest.SSL_MANAGE_SUCCESS["retrieve"]\
                                          .replace("PATH", xdcr_cert), output[0])
        self.shell.execute_command("rm {0}".format(xdcr_cert))

        options = "--regenerate-cert={0}".format(xdcr_cert)
        output, error = self.__execute_cli(cli_command=cli_command, options=options)
        self.assertFalse(error, "Error thrown during CLI execution %s" % error)
        self.assertEqual(XdcrCLITest.SSL_MANAGE_SUCCESS["regenerate"]\
                                        .replace("PATH", xdcr_cert), output[0])
        self.shell.execute_command("rm {0}".format(xdcr_cert))

    def _check_output(self, word_check, output):
        found = False
        if len(output) >=1 :
            for x in output:
                if word_check in x:
                    self.log.info("Found \"%s\" in CLI output" % word_check)
                    found = True
        return found
