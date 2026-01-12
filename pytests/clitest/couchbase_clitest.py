import copy
import json
import os
import random
import string, re
import time
import subprocess
from threading import Thread

from TestInput import TestInputSingleton
from clitest.cli_base import CliBaseTest
from couchbase_cli import CouchbaseCLI
from upgrade.newupgradebasetest import NewUpgradeBaseTest
from security.rbac_base import RbacBase
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import CLI_COMMANDS
from couchbase_helper.documentgenerator import BlobGenerator


help = {'CLUSTER': '--cluster=HOST[:PORT] or -c HOST[:PORT]',
 'COMMAND': {'analytics-link-setup': 'manage analytics links',
             'bucket-compact': 'compact database and index data',
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
 'analytics-link-setup OPTIONS': {'--access-key-id=KEY_ID': 'AWS access key ID for S3 link',
                                 '--account-key=ACCOUNT_KEY': 'Azure account key for Azure Blob link',
                                 '--account-name=ACCOUNT_NAME': 'Azure account name for Azure Blob link',
                                 '--application-default-credentials': 'use application default credentials for GCS link',
                                 '--certificate=CERTIFICATE': 'certificate file path for Couchbase link',
                                 '--client-certificate=CERTIFICATE': 'client certificate for Azure Blob link',
                                 '--client-certificate-password=PASSWORD': 'client certificate password for Azure Blob link',
                                 '--client-id=CLIENT_ID': 'client ID for Azure Blob link',
                                 '--client-secret=SECRET': 'client secret for Azure Blob link',
                                 '--create': 'create a new analytics link',
                                 '--delete': 'delete an analytics link',
                                 '--edit': 'edit an existing analytics link',
                                 '--encryption=ENCRYPTION': 'encryption type for Couchbase link',
                                 '--endpoint=ENDPOINT': 'endpoint URL for Azure Blob link',
                                 '--hostname=HOSTNAME': 'remote hostname for Couchbase link',
                                 '--json-credentials=CREDENTIALS': 'JSON credentials for GCS link',
                                 '--link-password=PASSWORD': 'password for Couchbase link',
                                 '--link-username=USERNAME': 'username for Couchbase link',
                                 '--list': 'list all analytics links',
                                 '--managed-identity-id=ID': 'managed identity ID for Azure Blob link',
                                 '--name=LINKNAME': 'name of the analytics link',
                                 '--region=REGION': 'AWS region for S3 link',
                                 '--scope=SCOPE': 'scope name for the link',
                                 '--secret-access-key=SECRET_KEY': 'AWS secret access key for S3 link',
                                 '--service-endpoint=ENDPOINT': 'service endpoint for S3 link',
                                 '--session-token=TOKEN': 'session token for S3 link',
                                 '--shared-access-signature=SAS': 'shared access signature for Azure Blob link',
                                 '--tenant-id=TENANT_ID': 'tenant ID for Azure Blob link',
                                 '--type=TYPE': 'type of analytics link (e.g., s3, couchbase, gcs, azureblob)',
                                 '--user-certificate=CERTIFICATE': 'user certificate file path for Couchbase link',
                                 '--user-key=KEY': 'user key file path for Couchbase link',
                                 '--user-key-passphrase=PASSPHRASE': 'passphrase for user key for Couchbase link'},
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
 'setting-notification OPTIONS': {'--enable-notifications=[0|1]': ' allow notification'},
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
help_short = {'COMMANDs include': {'analytics-link-setup': 'manage analytics links',
                      'bucket-compact': 'compact database and index data',
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


class CouchbaseCliTest(CliBaseTest, NewUpgradeBaseTest):
    REBALANCE_RUNNING = "Rebalance is running"
    REBALANCE_NOT_RUN = "Rebalance is not running"

    def setUp(self):
        TestInputSingleton.input.test_params["default_bucket"] = False
        super(CouchbaseCliTest, self).setUp()


    def tearDown(self):
        super(CouchbaseCliTest, self).tearDown()

    def _check_output(self, word_check, output):
        found = False
        if len(output) >= 1:
            if isinstance(word_check, list):
                for ele in word_check:
                    for x in output:
                        if ele.lower() in str(x.lower()):
                            self.log.info("Found '{0} in CLI output".format(ele))
                            found = True
                            break
            elif isinstance(word_check, str):
                for x in output:
                    if word_check.lower() in str(x.lower()):
                        self.log.info("Found '{0}' in CLI output".format(word_check))
                        found = True
                        break
            else:
                self.log.error("invalid {0}".format(word_check))
        return found

    def _convert_server_to_url(self, server):
        """From 6.5.x, add, remove, failover... need to prefix server with
           http/https and postfix with 8091/18091
           From 7.1, only allow secure connection to add node """
        url_server = "https://{0}:1{1}".format(server.ip, server.port)
        return url_server


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
        output, error = remote_client.execute_couchbase_cli(command, cluster_host=cluster_host,
                                                            cluster_port=cluster_port, user=user,
                                                            password=password,
                                                            admin_tools_package=self.admin_tools_package)
        if not error:
            content = ""
            for line in output:
                if line[:4] == "\x1b[6n":
                    line = line[4:]
                content += line
            return json.loads(content)
        else:
            self.fail("server-info return error output")

    def _create_bucket(self, remote_client, bucket="default", bucket_type="couchbase",
                        bucket_ramsize=256, bucket_replica=1, wait=False, enable_flush=None, enable_index_replica=None):
        options = "--bucket={0} --bucket-type={1} --bucket-ramsize={2} --bucket-replica={3}".\
            format(bucket, bucket_type, bucket_ramsize, bucket_replica)
        options += (" --enable-flush={0}".format(enable_flush), "")[enable_flush is None]
        options += (" --enable-index-replica={0}".format(enable_index_replica), "")[enable_index_replica is None]
        options += (" --enable-flush={0}".format(enable_flush), "")[enable_flush is None]
        options += (" --wait", "")[wait]
        cli_command = "bucket-create"

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                          options=options, cluster_host="localhost",
                                          user="Administrator", password="password",
                                          admin_tools_package=self.admin_tools_package)
        if "TIMED OUT" in output[0]:
            raise Exception("Timed out.  Could not create bucket")
        else:
            """ need to remove dot in front of output"""
            self.assertTrue(self.cli_bucket_create_msg in output[0].lstrip("."), "Fail to create bucket")

    def testHelp(self):
        command_with_error = {}
        shell = RemoteMachineShellConnection(self.master)
        admin_cli_commands = ['couchbase-cli', 'cbstats', 'mcstat', 'mctimings']
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
            option = " -h"
            if cli == "erl":
                option = " -version"
            if self.os == "windows":
                cli = '.'.join([cli, "exe"])
            if cli not in admin_cli_commands and self.admin_tools_package:
                self.cli_command_path = self.previous_cli
            if cli in admin_cli_commands and self.admin_tools_package:
                if self.shell.extract_remote_info().type.lower() == "linux":
                    self.cli_command_path = self.linux_admin_tools_command_path
                elif self.shell.extract_remote_info().type.lower() == "windows":
                    self.cli_command_path = self.windows_admin_tools_command_path
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

    def testAnalyticsLinkSetup(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        operation = self.input.param("operation", "create")  # list, create, edit, delete

        link_name = self.input.param("link-name", None)
        if link_name is None and operation != "list":
            link_name = ''.join(random.choices(string.ascii_letters, k=random.randint(5, 8)))

        link_type = self.input.param("link-type", None)
        scope = self.input.param("scope", None)
        expect_error = self.input.param("expect-error", False)
        error_msg = self.input.param("error-msg", "")

        # Initialize link parameters based on link type
        # S3 link parameters
        access_key_id = secret_access_key = region = service_endpoint = session_token = None
        if link_type == "s3":
            access_key_id = self.input.param("access-key-id", None)
            secret_access_key = self.input.param("secret-access-key", None)
            region = self.input.param("region", None)
            service_endpoint = self.input.param("service-endpoint", None)
            session_token = self.input.param("session-token", None)

        # Couchbase link parameters
        remote_hostname = link_username = link_password = encryption = None
        user_certificate = user_key = user_key_passphrase = certificate = None
        if link_type == "couchbase":
            remote_hostname = self.input.param("remote_hostname", None)
            # If hostname is not provided and link type is couchbase, use server[1]
            if remote_hostname is None and len(self.servers) > 1:
                remote_hostname = self.servers[0].ip
            link_username = self.input.param("link-username", None)
            link_password = self.input.param("link-password", None)
            encryption = self.input.param("encryption", None)
            user_certificate = self.input.param("user-certificate", None)
            user_key = self.input.param("user-key", None)
            user_key_passphrase = self.input.param("user-key-passphrase", None)
            certificate = self.input.param("certificate", None)

        # GCS link parameters
        application_default_credentials = False
        json_credentials = None
        if link_type == "gcs":
            application_default_credentials = self.input.param("application-default-credentials", False)
            use_json_credentials = self.input.param("json-credentials", False)
            if use_json_credentials:
                self.log.info(f"Fetching certificate file path from the env: {use_json_credentials}")
                gcs_certificate = os.getenv(use_json_credentials)
                if gcs_certificate:
                    json_credentials = gcs_certificate
                else:
                    self.fail(f"Certificate file not found in the env: {use_json_credentials}")

        # Azure link parameters
        authentication = account_name = account_key = shared_access_signature = None
        managed_identity_id = client_id = client_secret = client_certificate = None
        client_certificate_password = tenant_id = endpoint = None
        if link_type == "azureblob":
            authentication = self.input.param("authentication", None)
            account_name = None if authentication == "anonymous" else self.input.param("account-name", None)
            account_key = None if authentication == "anonymous" else self.input.param("account-key", None)
            shared_access_signature = self.input.param("shared-access-signature", None)
            managed_identity_id = self.input.param("managed-identity-id", None)
            client_id = self.input.param("client-id", None)
            client_secret = self.input.param("client-secret", None)
            client_certificate = self.input.param("client-certificate", None)
            client_certificate_password = self.input.param("client-certificate-password", None)
            tenant_id = self.input.param("tenant-id", None)
            endpoint = self.input.param("endpoint", None)

        server = copy.deepcopy(self.servers[1])
        cli = CouchbaseCLI(server, username, password)

        stdout, stderr, success = cli.analytics_link_setup(
            list_links=(operation == "list"),
            create=(operation == "create"),
            edit=(operation == "edit"),
            delete=(operation == "delete"),
            name=link_name,
            link_type=link_type,
            scope=scope,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            region=region,
            service_endpoint=service_endpoint,
            session_token=session_token,
            remote_hostname=remote_hostname,
            link_username=link_username,
            link_password=link_password,
            encryption=encryption,
            user_certificate=user_certificate,
            user_key=user_key,
            user_key_passphrase=user_key_passphrase,
            certificate=certificate,
            application_default_credentials=application_default_credentials,
            json_credentials=json_credentials,
            account_name=account_name,
            account_key=account_key,
            shared_access_signature=shared_access_signature,
            managed_identity_id=managed_identity_id,
            client_id=client_id,
            client_secret=client_secret,
            client_certificate=client_certificate,
            client_certificate_password=client_certificate_password,
            tenant_id=tenant_id,
            endpoint=endpoint,
            admin_tools_package=self.admin_tools_package
        )

        self.log.info(f"stdout: {stdout}")

        if not expect_error:
            # Check stdout for error indicators
            for line in stdout:
                if "Traceback" in line or "Exception" in line or "ERROR:" in line:
                    self.fail(f"Error found in stdout: {line}")

            if operation == "list":
                # For list operation, success is determined by non-empty output
                self.assertTrue(len(stdout) > 0, "Expected analytics link list to return output. Output: %s" % stdout)
            else:
                self.assertTrue(success, "Expected analytics link setup to succeed. Output: %s" % stdout)
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                          "Expected error message not found")


    def testInfoCommands(self):
        remote_client = RemoteMachineShellConnection(self.master)

        cli_command = "server-list"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                  cluster_host="localhost", user="Administrator", password="password",
                                        admin_tools_package=self.admin_tools_package)
        server_info = self._get_cluster_info(remote_client)
        """ In new single node not join any cluster yet,
            IP of node will be 127.0.0.1 """
        if "127.0.0.1" in server_info["otpNode"]:
            server_info["otpNode"] = "ns_1@{0}".format(remote_client.ip)
            server_info["hostname"] = "{0}:8091".format(remote_client.ip)
        otpNode = remote_client.ip
        """ need to remove [ ] brackets in ipv6 raw ip address """
        if "[" in otpNode:
            otpNode = otpNode.replace("[", "").replace("]", "")
        result = server_info["otpNode"] + " " + server_info["hostname"] + " " \
               + server_info["status"] + " " + server_info["clusterMembership"]
        self.assertEqual(result.lower(), "ns_1@{0} {1}:8091 healthy active" \
                                           .format(otpNode.lower(),
                                                   remote_client.ip.lower()))

        cli_command = "bucket-list"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                  cluster_host="localhost", user="Administrator", password="password",
                                            admin_tools_package=self.admin_tools_package)
        if output and output[0] == "\x1b[6n":
            del output[0]
        self.assertEqual([], output)
        remote_client.disconnect()

    def test_priority_start_couchbase_saslauth(self):
        """
            In centos 6.x, couchbase server needs to start after saslauth start
            so the authentication works correctly as mention in ticket MB-25922
            This test requires saslauth preinstalled on vm.
        """
        if "centos 7" in self.os_version:
            self.log.info("This test only for centos 6.x")
            return
        if "windows" in self.os_version:
            self.log.info("This test is for centos 6.x only, not for windows")
            return
        if "centos" in self.os_version:
            """ If saslauth did not install on vm, mark test as failed. """
            output, error = self.shell.execute_command("/etc/init.d/saslauthd status")
            if output and "is running" not in output[0]:
                self.fail("Need a centos 6.x with saslauth preinstalled.")

            self.shell.execute_command("reboot")
            self.sleep(240, "sleep while rebooting")
            shell = RemoteMachineShellConnection(self.master)
            shell.disable_firewall()
            output, error = shell.execute_command('find /etc/rc3.d/ '\
                                                  '| egrep "saslauthd|couchbase"')
            if output:
                self.log.info("Order starting: %s " % output)
                if "saslauthd" in output[0] and "couchbase-serve" in output[1]:
                    self.log.info("Couchbase Server started after saslauthd")
                elif "couchbase-server" in output[0]:
                    self.fail("Couchbase Server started before saslauthd")
            output, error = shell.execute_command('cat /var/log/boot.log '\
                                                  '| egrep "saslauthd|couchbase"')
            if output:
                self.log.info("Order starting: %s " % output)
                if "Starting saslauth" in output[0] and "couchbase-server" in output[-1]:
                    self.log.info("Couchbase Server started after saslauthd")
                elif "couchbase-server" in output[0]:
                    self.fail("Couchbase Server started before saslauthd")
        else:
            self.log.info("This test only for centos 6.x")
            return

    def testAddRemoveNodes(self):
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        nodes_failover = self.input.param("nodes_failover", 0)
        nodes_readd = self.input.param("nodes_readd", 0)
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "server-add"
        if int(nodes_add) < len(self.servers):
            for num in range(nodes_add):
                self.log.info("add node {0} to cluster".format(
                    self.servers[num + 1].ip))
                options = "--server-add={0} \
                           --server-add-username=Administrator \
                           --server-add-password=password \
                           --no-ssl-verify " \
                            .format(self._convert_server_to_url(self.servers[num + 1]))
                output, error = \
                      remote_client.execute_couchbase_cli(cli_command=cli_command,
                                        options=options, cluster_host="localhost",
                                         cluster_port=18091, user="Administrator",
                                                              password="password",
                                     admin_tools_package=self.admin_tools_package)
                server_added = False
                output_msg = "SUCCESS: Server added"
                if self.cb_version[:3] == "4.6":
                    output_msg = "Server %s:%s added" % (self.servers[num + 1].ip,\
                                                         self.servers[num + 1].port)
                if len(output) >= 1:
                    for x in output:
                        if output_msg in x:
                            server_added = True
                            break
                    if not server_added:
                        raise Exception("failed to add server {0}"
                                          .format(self.servers[num + 1].ip))
        else:
             raise Exception("Node add should be smaller total number"
                                                         " vms in ini file")

        cli_command = "rebalance"
        for num in range(nodes_rem):
            options = "--server-remove={0}:8091".format(self.servers[nodes_add - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                            options=options, cluster_host="localhost", cluster_port=8091,
                                               user="Administrator", password="password",
                                            admin_tools_package=self.admin_tools_package)
            self.assertTrue(self.cli_rebalance_msg in output)
        if nodes_rem == 0 and nodes_add > 0:
            cli_command = "rebalance"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                            cluster_host="localhost", cluster_port=8091,
                                              user="Administrator", password="password",
                                           admin_tools_package=self.admin_tools_package)
            if len(output) == 4:
                self.assertTrue(self._check_output("SUCCESS: Rebalance complete", output))
            else:
                self.assertTrue(self.cli_rebalance_msg in output)

        """ when no bucket, have to add option --force to failover
            since no data => no graceful failover.  Need to add test
            to detect no graceful failover if no bucket """
        self._create_bucket(remote_client, bucket_replica=self.num_replicas)
        cli_command = "failover"
        for num in range(nodes_failover):
            self.log.info("failover node {0}"\
                          "MB-39220: Adding --hard flag for cli failover tests"\
                          .format(self.servers[nodes_add - nodes_rem - num].ip))
            self.sleep(10, "wait for previous command execute completely")
            options = "--server-failover={0}:8091" \
                      .format(self.servers[nodes_add - nodes_rem - num].ip)
            if self.force_failover:
                options += " --hard "
                output, error = remote_client.execute_couchbase_cli(
                        cli_command=cli_command, options=options,
                        cluster_host="localhost", cluster_port=8091,
                        user="Administrator", password="password",
                        admin_tools_package=self.admin_tools_package)
                if len(output) == 2:
                    self.assertIn("SUCCESS: Server failed over", output[0])
                else:
                    self.assertTrue("SUCCESS: Server failed over" in output)
            else:
                output, error = remote_client.execute_couchbase_cli(
                        cli_command=cli_command, options=options,
                        cluster_host="localhost", cluster_port=8091,
                        user="Administrator", password="password",
                        admin_tools_package=self.admin_tools_package)
                RestConnection(self.master).monitorRebalance()
                if not self._check_output("SUCCESS: Server failed over", output):
                    if output and output[0]:
                        raise Exception(output[0] + ". Error in command: " + cli_command)
                    else:
                        raise Exception("No printout with command: {0}".format(cli_command))

        cli_command = "server-readd"
        for num in range(nodes_readd):
            self.log.info("add back node {0} to cluster" \
                        .format(self.servers[nodes_add - nodes_rem - num ].ip))
            self.sleep(10, "wait for previous command execute completely")

            options = "--server-add=https{0}:1{1} --no-ssl-verify "\
                                                   .format(self.servers[nodes_add - nodes_rem - num].ip,
                                                    self.servers[nodes_add - nodes_rem - num].port)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                        options=options, cluster_host="localhost",
                                                          cluster_port=18091, user="Administrator",
                                                                              password="password",
                                                    admin_tools_package=self.admin_tools_package)
            self.assertTrue(self._check_output(["DEPRECATED: Please use the recovery command ",
                                 "SUCCESS: Servers recovered"], output), "Server readd failed")
        cli_command = "rebalance"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                            cluster_host="localhost", cluster_port=8091,
                                                            user="Administrator",
                                                            password="password",
                                                            admin_tools_package=self.admin_tools_package)
        self.assertTrue(self._check_output(self.cli_rebalance_msg, output))
        if self.os == 'windows':
            self.sleep(10, "windows need more time to complete rebalance")
        remote_client.disconnect()

    def testAddRemoveNodesWithRecovery(self):
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        nodes_failover = self.input.param("nodes_failover", 0)
        nodes_recovery = self.input.param("nodes_recovery", 0)
        nodes_readd = self.input.param("nodes_readd", 0)
        remote_client = RemoteMachineShellConnection(self.master)
        cluster_user = self.master.rest_username
        cluster_pwd = self.master.rest_password
        cli_command = "server-add"
        if int(nodes_add) < len(self.servers):
            for num in range(nodes_add):
                self.log.info("add node {0} to cluster".format(self.servers[num + 1].ip))
                options = "--server-add={0} \
                           --server-add-username=Administrator \
                           --server-add-password=password \
                           --no-ssl-verify "\
                           .format(self.servers[num + 1].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                                    options=options,
                                                                    cluster_host="localhost",
                                                                    cluster_port=18091,
                                                                    user=cluster_user,
                                                                    password=cluster_pwd,
                                                                    admin_tools_package=self.admin_tools_package)
                self.assertTrue(self._check_output("SUCCESS: Server added", output))
        else:
             raise Exception("Node add should be smaller total number vms in ini file")

        cli_command = "rebalance"
        for num in range(nodes_rem):
            options = "--server-remove={0}:8091".format(self.servers[nodes_add - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                                options=options,
                                                                cluster_host="localhost",
                                                                user=cluster_user,
                                                                password=cluster_pwd,
                                                                admin_tools_package=self.admin_tools_package)
            self.assertTrue(self.cli_rebalance_msg in output)

        if nodes_rem == 0 and nodes_add > 0:
            cli_command = "rebalance"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                                cluster_host="localhost",
                                                                user=cluster_user,
                                                                password=cluster_pwd,
                                                                admin_tools_package=self.admin_tools_package)
            self.assertTrue(self._check_output(self.cli_rebalance_msg, output))

        self._create_bucket(remote_client)
        self.sleep(7, "time needs for bucket ready")

        cli_command = "failover"
        for num in range(nodes_failover):
            force_failover = False
            self.log.info("failover node {0}"\
                                .format(self.servers[nodes_add - nodes_rem - num].ip))
            options = "--server-failover={0}:8091"\
                      .format(self.servers[nodes_add - nodes_rem - num].ip)
            if self.force_failover or num == nodes_failover - 1:
                options += " --hard "
                force_failover = True
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                                options=options,
                                                                cluster_host="localhost",
                                                                user=cluster_user,
                                                                password=cluster_pwd,
                                                                admin_tools_package=self.admin_tools_package)
            self.assertTrue("SUCCESS: Server failed over" in str(output),
                            "Node failover failed")
            if not force_failover:
                RestConnection(self.master).monitorRebalance()

        cli_command = "recovery"
        for num in range(nodes_failover):
            # try to set recovery when nodes failovered (MB-11230)
            options = "--server-recovery=%s --recovery-type=delta" \
                      % (self.servers[nodes_add - nodes_rem - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                                options=options,
                                                                cluster_host="localhost",
                                                                user=cluster_user,
                                                                password=cluster_pwd,
                                                                admin_tools_package=self.admin_tools_package)
            self.assertIn("SUCCESS: Servers recovered", output[0])

        for num in range(nodes_recovery):
            cli_command = "server-readd"
            self.log.info("add node {0} back to cluster"\
                          .format(self.servers[nodes_add - nodes_rem - num].ip))
            options = "--server-add={0} --no-ssl-verify "\
                                                   .format(self.servers[nodes_add - nodes_rem - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                                options=options,
                                                                cluster_host="localhost",
                                                                user=cluster_user,
                                                                password=cluster_pwd,
                                                                admin_tools_package=self.admin_tools_package)
            self.assertTrue(self._check_output(["DEPRECATED: Please use the recovery command ",
                                 "SUCCESS: Servers recovered"], output), "Server readd failed")

        if int(nodes_readd) > int(nodes_failover):
            cli_command = "server-add"
            for num in range(nodes_readd - nodes_failover):
                self.log.info("add node {0} to cluster".format(self.servers[nodes_add - num ].ip))
                options = "--server-add={0} \
                           --server-add-username=Administrator \
                           --server-add-password=password \
                           --no-ssl-verify " \
                                               .format(self.servers[nodes_add -num].ip)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                                    options=options,
                                                                    cluster_host="localhost",
                                                                    cluster_port=18091,
                                                                    user=cluster_user,
                                                                    password=cluster_pwd,
                                                                    admin_tools_package=self.admin_tools_package)
                self.assertTrue(self._check_output("SUCCESS: Server added", output))
                self.sleep(5)
        cli_command = "rebalance"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                            cluster_host="localhost",
                                                            user=cluster_user,
                                                            password=cluster_pwd,
                                                            admin_tools_package=self.admin_tools_package)
        self.assertTrue(self.cli_rebalance_msg in output)
        remote_client.disconnect()

    def testStartStopRebalance(self):
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        remote_client = RemoteMachineShellConnection(self.master)

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                  cluster_host="localhost", user="Administrator", password="password",
                                        admin_tools_package=self.admin_tools_package)
        self.assertTrue(self._check_output(self.REBALANCE_NOT_RUN, output))

        cli_command = "server-add"
        for num in range(nodes_add):
            options = "--server-add={0} --server-add-username=Administrator \
                       --server-add-password=password  --no-ssl-verify"\
                             .format(self.servers[num + 1].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                              options=options, cluster_host="localhost",
                                                user="Administrator", password="password",
                                            admin_tools_package=self.admin_tools_package)
            output_msg = "SUCCESS: Server added"
            self.assertTrue(self._check_output(output_msg, output))

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                  cluster_host="localhost", user="Administrator", password="password",
                                        admin_tools_package=self.admin_tools_package)
        self.assertTrue(self._check_output(self.REBALANCE_NOT_RUN, output))

        self._create_bucket(remote_client)

        cli_command = "rebalance"
        t = Thread(target=remote_client.execute_couchbase_cli, name="rebalance_after_add",
                       args=(cli_command, "localhost", '', None, "Administrator", "password", self.admin_tools_package))
        t.start()
        self.sleep(5)

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                  cluster_host="localhost", user="Administrator", password="password",
                                        admin_tools_package=self.admin_tools_package)
        self.assertTrue(self._check_output(self.REBALANCE_RUNNING, output))

        t.join()

        cli_command = "rebalance"
        for num in range(nodes_rem):
            options = "--server-remove={0}:8091".format(self.servers[nodes_add - num].ip)
            t = Thread(target=remote_client.execute_couchbase_cli, name="rebalance_after_add",
                       args=(cli_command, "localhost", options, None, "Administrator", "password", self.admin_tools_package))
            t.start()
            self.sleep(5)
            cli_command = "rebalance-status"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                      cluster_host="localhost", user="Administrator", password="password",
                                            admin_tools_package=self.admin_tools_package)
            self.assertTrue(self._check_output(self.REBALANCE_RUNNING, output))

            cli_command = "rebalance-stop"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                      cluster_host="localhost", user="Administrator", password="password",
                                            admin_tools_package=self.admin_tools_package)

            t.join()

            cli_command = "rebalance"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                      cluster_host="localhost", user="Administrator", password="password",
                                            admin_tools_package=self.admin_tools_package)
            self.assertTrue(self._check_output("SUCCESS: Rebalance complete", output))

            cli_command = "rebalance-status"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                      cluster_host="localhost", user="Administrator", password="password",
                                            admin_tools_package=self.admin_tools_package)
            self.assertTrue(self._check_output(self.REBALANCE_NOT_RUN, output))
        remote_client.disconnect()

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

        initial_server = self.servers[0]
        server = copy.deepcopy(initial_server)

        if not initialized:
            rest = RestConnection(server)
            rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        stdout, _, _ = cli.cluster_init(data_ramsize, index_ramsize, fts_ramsize,
                                        services, index_storage_mode, name,
                                        username, password, port,
                                        admin_tools_package=self.admin_tools_package)

        if username:
            server.rest_username = username
        if password:
            server.rest_password = password
        # strip quotes if the cluster name contains spaces
        if name and (name[0] == name[-1]) and name.startswith(("'", '"')):
            name = name[1:-1]
        if not index_storage_mode and services and "index" in services:
            index_storage_mode = "plasma"
        if not services:
            services = "data"

        if not expect_error:
            # Update the cluster manager port if it was specified to be changed
            if port:
                server.port = port

            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Cluster initialized"),
                            "Expected command to succeed")
            self.assertTrue(self.isClusterInitialized(server), "Cluster was not initialized")
            self.assertTrue(self.verifyServices(server, services), "Services do not match")
            self.assertTrue(self.verifyNotificationsEnabled(server),
                             "Notification is disabled (default is enable)")
            self.assertTrue(self.verifyClusterName(server, name), "Cluster name does not match")

            if "index" in services:
                self.assertTrue(self.verifyIndexSettings(server, None, None, None,
                                                         index_storage_mode, None, None),
                                                         "Index storage mode not properly set")

            self.assertTrue(self.verifyRamQuotas(server, data_ramsize, index_ramsize, fts_ramsize),
                            "Ram quotas not set properly")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if not initialized:
                self.assertTrue(not self.isClusterInitialized(server),
                                "Cluster was initialized, but error was received")

        # Reset the cluster (This is important for when we change the port number)
        rest = RestConnection(server)
        rest.init_cluster(initial_server.rest_username, initial_server.rest_password, initial_server.port)

    def test_set_cluster_name(self):
        """
        Test set and rename cluster name
        1. Reset node back to initial setup
        2. Set cluster-name by IP (if param is None)
        3. Rename cluster-name
        :return: nothing
        """
        cluster_name = self.input.param("cluster-name", None)
        if cluster_name is None:
            cluster_name = self.master.ip
        self.log.info("Reset node back to initial page")
        rest = RestConnection(self.master)
        rest.force_eject_node()
        options = "--cluster-username Administrator --cluster-password password " \
                  "--cluster-port 8091 --cluster-name '%s' " % cluster_name
        output, error = self.shell.couchbase_cli("cluster-init", self.master.ip,
                                                                        options,
                                   admin_tools_package=self.admin_tools_package)
        if "SUCCESS: Cluster initialized" not in output[0]:
            self.fail("Failed to initialize node '%s' " % self.master.ip)

        self.log.info("Verify hostname is set in cluster")
        settings = rest.get_pools_default()
        if cluster_name not in settings["clusterName"]:
            self.fail("Fail to set hostname in cluster. "
                      "Host name in cluster is '%s' " % settings["clusterName"])
        else:
            self.log.info("Cluster name is set to '%s' " % settings["clusterName"])

        change_hostname = self.input.param("change-hostname", None)
        if change_hostname is not None:
            if change_hostname == "ip":
                change_hostname = self.master.ip

            self.log.info("Rename hostname in cluster.")
            options = "-u Administrator -p password --cluster-name '%s' " \
                                                        % change_hostname
            output, error = self.shell.couchbase_cli("setting-cluster",
                                                     self.master.ip, options,
                                admin_tools_package=self.admin_tools_package)
            settings = rest.get_pools_default()
            if change_hostname not in settings["clusterName"]:
                self.fail("Fail to set new hostname in cluster. "
                          "Host name in cluster is '%s' " % settings["clusterName"])
            else:
                self.log.info("Cluster name is set to '%s' " % settings["clusterName"])

    def test_rebalance_display_bar(self):
        """
            Test display bar when rebalance and stop/start rebalance
        """
        server = copy.deepcopy(self.servers[0])
        add_server = self._convert_server_to_url(self.servers[1])
        stop_rebalance = self.input.param("stop-rebalance", False)
        rest = RestConnection(server)
        self.default_bucket = True
        self._bucket_creation()
        cli = CouchbaseCLI(server, "cbadminbucket", "password")
        output_add, error, msg = cli.server_add(add_server, "Administrator",
                                                "password", None, "data", None,
                                                admin_tools_package=self.admin_tools_package)
        if "SUCCESS" not in output_add[0] and "Server added" not in output_add[0]:
            self.fail("Could not add node %s to cluster" % add_server.ip)

        reb_result = ""

        if not stop_rebalance:
            output, error, msg = cli.rebalance(None, admin_tools_package=self.admin_tools_package)
        else:
            sts = rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                                ejectedNodes=[])
            for bucket in self.buckets:
                if len(bucket.vbuckets) > 128:
                    self.sleep(3, "wait for rebalance to run")
                    break
            if sts:
                _, _, stop_status = cli.rebalance_stop(admin_tools_package=self.admin_tools_package)
                if stop_status:
                    output, error, msg = cli.rebalance(None,
                                                       admin_tools_package=self.admin_tools_package)
                else:
                    self.fail("Fail to stop rebalance")
            else:
                print("Fail to rebalance using rest call")
        if output:
            reb_result = output[-1]
            if "SUCCESS" not in reb_result and "Rebalance complete" not in reb_result:
                self.fail("Rebalance failed")

            if "100.0%" not in reb_result:
                self.fail("Rebalance failed.  It not reach 100%")
            reb_result = reb_result.replace(" 100.0%", "").strip(" ")
            print("rebalance bar: %s" % reb_result)
            if " " in reb_result:
                self.fail("rebalance bar did not display correctly")
        else:
            self.fail("output is empty %s " % output)

    def testRebalanceStop(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")
        init_start_rebalance = self.input.param("init-rebalance", False)

        server = copy.deepcopy(self.servers[0])
        add_server = self.servers[1]

        rest = RestConnection(server)
        rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        if initialized:
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")
        if init_start_rebalance:
           self.assertTrue(rest.rebalance(otpNodes=["%s:%s" % (add_server.ip, add_server.port)]),
                           "Rebalance failed to start")

        stdout, _, _ = cli.rebalance_stop(admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Rebalance stopped"),
                            "Expected command to succeed")
            if init_start_rebalance:
                self.assertTrue(rest.isRebalanced(), "Rebalance does not appear to be stopped")

        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if not initialized:
                self.assertTrue(not self.isClusterInitialized(server),
                                "Cluster was initialized, but error was received")

    def testSettingAudit(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        enabled = self.input.param("enabled", None)
        log_path = self.input.param("log-path", None)
        rotate_interval = self.input.param("rotate-interval", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        if log_path is not None and log_path == "valid":
                log_path = self.log_path

        cli = CouchbaseCLI(server, username, password)
        if initialized:
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        stdout, _, _ = cli.setting_audit(enabled, log_path, rotate_interval,
                                         admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Audit settings modified"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyAuditSettings(server, enabled, log_path, rotate_interval),
                            "Audit settings were not set properly")

        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if not initialized:
                self.assertTrue(not self.isClusterInitialized(server),
                                "Cluster was initialized, but error was received")

    def testSettingCompaction(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)

        db_frag_perc = self.input.param("db-frag-perc", None)
        db_frag_size = self.input.param("db-frag-size", None)
        view_frag_perc = self.input.param("view-frag-perc", None)
        view_frag_size = self.input.param("view-frag-size", None)
        from_period = self.input.param("from-period", None)
        to_period = self.input.param("to-period", None)
        abort_outside = self.input.param("abort-outside", None)
        parallel_compact = self.input.param("parallel-compact", None)
        purgeInt = self.input.param("purge-interval", None)

        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        if initialized:
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        stdout, _, errored = cli.setting_compaction(db_frag_perc, db_frag_size, view_frag_perc, view_frag_size,
                                                    from_period, to_period, abort_outside, parallel_compact, purgeInt,
                                                    admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(errored, "Expected command to succeed")
            self.assertTrue(self.verifyCompactionSettings(server, db_frag_perc, db_frag_size, view_frag_perc,
                                                          view_frag_size, from_period, to_period, abort_outside,
                                                          parallel_compact, purgeInt),
                            "Settings don't match")

        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if not initialized:
                self.assertTrue(not self.isClusterInitialized(server),
                                "Cluster was initialized, but error was received")

    def test_gsi_compaction(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)

        compact_mode = self.input.param("compact_mode", None)
        compact_percent = self.input.param("compact_percent", None)
        compact_interval = self.input.param("compact_interval", None)
        from_period = self.input.param("from_period", None)
        to_period = self.input.param("to_period", None)
        if compact_interval is None:
            from_period = "0:0"
            to_period = "0:0"
        enable_abort = self.input.param("enable_abort", 0)
        expect_error = self.input.param("expect-error", False)
        error_msg = self.input.param("error-msg", "")

        """ reset node to setup services """
        self.rest.force_eject_node()
        cli = CouchbaseCLI(self.master, username, password, self.cb_version)
        _, _, success = cli.cluster_init(256, 512, None, "data,index,query", None, None,
                                                 self.master.rest_username,
                                                 self.master.rest_password, None,
                                         admin_tools_package=self.admin_tools_package)
        self.assertTrue(success, "Cluster initialization failed during test setup")

        if compact_interval is not None and "-" in compact_interval:
            compact_interval = compact_interval.replace("-", ",")
        stdout, _, errored = cli.setting_gsi_compaction(compact_mode, compact_percent,
                              compact_interval, from_period, to_period, enable_abort,
                                        admin_tools_package=self.admin_tools_package)
        self.assertTrue(errored, "Expected command to succeed")
        if not expect_error:
            self.verify_gsi_compact_settings(compact_mode, compact_percent,
                                             compact_interval,
                                             from_period, to_period,
                                             enable_abort)

    def testSettingAutoFailover(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        enabled = self.input.param("enabled", None)
        timeout = self.input.param("timeout", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        if initialized:
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        stdout, _, _ = cli.setting_autofailover(enabled, timeout,
                                                admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Auto-failover settings modified"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyAutofailoverSettings(server, enabled, timeout),
                            "Auto-failover settings were not set properly")

        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if not initialized:
                self.assertTrue(not self.isClusterInitialized(server),
                                "Cluster was initialized, but error was received")

    def testSettingAutoReprovision(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        enabled = self.input.param("enabled", None)
        max_nodes = self.input.param("max-nodes", 1)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        if initialized:
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        stdout, _, _ = cli.setting_autoreprovision(enabled, max_nodes,
                                                   admin_tools_package=self.admin_tools_package)

        if not expect_error:
            if enabled == 0:
                # WARNING: --max-servers will not take affect because auto-reprovision is being disabled
                max_nodes = 1
                self.assertEqual(stdout[0], 'WARNING: --max-servers will not take affect because auto-reprovision is being disabled')
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Auto-reprovision settings modified"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyAutoreprovisionSettings(server, enabled, max_nodes),
                            "Auto-failover settings were not set properly")

        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if not initialized:
                self.assertTrue(not self.isClusterInitialized(server),
                                "Cluster was initialized, but error was received")


    def testSettingNotification(self):
        enable = self.input.param("enable", None)
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        initialized = self.input.param("initialized", False)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")
        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        if initialized:
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        initialy_enabled = self.verifyNotificationsEnabled(server)

        cli = CouchbaseCLI(server, username, password)
        stdout, _, _ = cli.setting_notification(enable,
                                                admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Software notification settings updated"),
                            "Expected command to succeed")
            if enable == 1:
                self.assertTrue(self.verifyNotificationsEnabled(server), "Notification not enabled")
            else:
                self.assertTrue(not self.verifyNotificationsEnabled(server), "Notification are enabled")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg), "Expected error message not found")
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

        init_data_memory = 256
        init_index_memory = 256
        init_fts_memory = 256
        init_name = "testrunner"

        initial_server = self.servers[0]
        server = copy.deepcopy(initial_server)

        rest = RestConnection(server)
        rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        if initialized:
            _, _, success = cli.cluster_init(init_data_memory, init_index_memory, init_fts_memory, None, None,
                                             init_name, server.rest_username, server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        if cmd == "cluster-edit":
            stdout, _, _ = cli.cluster_edit(data_ramsize, index_ramsize, fts_ramsize, name, new_username,
                                            new_password, port,
                                            admin_tools_package=self.admin_tools_package)
        else:
            stdout, _, _ = cli.setting_cluster(data_ramsize, index_ramsize, fts_ramsize, name, new_username,
                                               new_password, port,
                                               admin_tools_package=self.admin_tools_package)

        if new_username and not expect_error:
                server.rest_username = new_username
        if new_password and not expect_error:
            server.rest_password = new_password
        if name and (name[0] == name[-1]) and name.startswith(("'", '"')):
            name = name[1:-1]

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
                self.verifyWarningOutput(stdout, "The cluster-edit command is depercated, use setting-cluster instead")
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Cluster settings modified"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyRamQuotas(server, data_ramsize, index_ramsize, fts_ramsize),
                            "Ram quotas not set properly")
            self.assertTrue(self.verifyClusterName(server, name), "Cluster name does not match")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg), "Expected error message not found")
            if not initialized:
                self.assertTrue(not self.isClusterInitialized(server), "Cluster was initialized, but error was received")

        # Reset the cluster (This is important for when we change the port number)
        rest = RestConnection(server)
        self.assertTrue(rest.init_cluster(initial_server.rest_username, initial_server.rest_password, initial_server.port),
                   "Cluster was not re-initialized at the end of the test")

    def testSettingIndex(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        max_rollbacks = self.input.param("max-rollback-points", None)
        stable_snap_interval = self.input.param("stable-snapshot-interval", None)
        mem_snap_interval = self.input.param("memory-snapshot-interval", None)
        storage_mode = self.input.param("storage-mode", None)
        threads = self.input.param("threads", None)
        log_level = self.input.param("log-level", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        if initialized:
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        stdout, _, _ = cli.setting_index(max_rollbacks, stable_snap_interval, mem_snap_interval, storage_mode, threads,
                                         log_level, admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Indexer settings modified"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyIndexSettings(server, max_rollbacks, stable_snap_interval, mem_snap_interval,
                                                     storage_mode, threads, log_level),
                            "Index settings were not set properly")

        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if not initialized:
                self.assertTrue(not self.isClusterInitialized(server),
                                "Cluster was initialized, but error was received")

    def testSettingLdap(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        admins = self.input.param("admins", None)
        ro_admins = self.input.param("ro-admins", None)
        enabled = self.input.param("enabled", False)
        default = self.input.param("default", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        if initialized:
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        stdout, _, _ = cli.setting_ldap(admins, ro_admins, default, enabled,
                                        admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "LDAP settings modified"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyLdapSettings(server, admins, ro_admins, default, enabled), "LDAP settings not set")

        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if not initialized:
                self.assertTrue(self.verifyLdapSettings(server, None, None, None, 0), "LDAP setting changed")

    def testSettingAlert(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        enabled = self.input.param("enabled", None)
        email_recipients = self.input.param("email-recipients", None)
        email_sender = self.input.param("email-sender", None)
        email_username = self.input.param("email-user", None)
        email_password = self.input.param("email-password", None)
        email_host = self.input.param("email-host", None)
        email_port = self.input.param("email-port", None)
        encrypted = self.input.param("encrypted", None)
        alert_af_node = self.input.param("alert-auto-failover-node", False)
        alert_af_max_reached = self.input.param("alert-auto-failover-max-reached", False)
        alert_af_node_down = self.input.param("alert-auto-failover-node-down", False)
        alert_af_small = self.input.param("alert-auto-failover-cluster-small", False)
        alert_af_disable = self.input.param("alert-auto-failover-disable", False)
        alert_ip_changed = self.input.param("alert-ip-changed", False)
        alert_disk_space = self.input.param("alert-disk-space", False)
        alert_meta_overhead = self.input.param("alert-meta-overhead", False)
        alert_meta_oom = self.input.param("alert-meta-oom", False)
        alert_write_failed = self.input.param("alert-write-failed", False)
        alert_audit_dropped = self.input.param("alert-audit-msg-dropped", False)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        if initialized:
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        stdout, _, _ = cli.setting_alert(enabled, email_recipients, email_sender, email_username, email_password,
                                         email_host, email_port, encrypted, alert_af_node, alert_af_max_reached,
                                         alert_af_node_down, alert_af_small, alert_af_disable, alert_ip_changed,
                                         alert_disk_space, alert_meta_overhead, alert_meta_oom, alert_write_failed,
                                         alert_audit_dropped,
                                         admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Email alert settings modified"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyAlertSettings(server, enabled, email_recipients, email_sender, email_username,
                                                     email_password, email_host, email_port, encrypted, alert_af_node,
                                                     alert_af_max_reached, alert_af_node_down, alert_af_small,
                                                     alert_af_disable, alert_ip_changed, alert_disk_space,
                                                     alert_meta_overhead, alert_meta_oom, alert_write_failed,
                                                     alert_audit_dropped),
                            "Alerts settings not set")

        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found. Expected message: " + str(error_msg) + "   Actual: " + str(stdout))

    def testBucketCompact(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        bucket_name = self.input.param("bucket-name", None)
        data_only = self.input.param("data-only", False)
        views_only = self.input.param("views-only", False)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")
        init_bucket_type = self.input.param("init-bucket-type", None)

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")
            if init_bucket_type is not None:
                _, _, success = cli.bucket_create(bucket_name, init_bucket_type, 256, None, None, None, None,
                                                  None, None,
                                                  admin_tools_package=self.admin_tools_package)
                self.assertTrue(success, "Bucket not created during test setup")

        cli = CouchbaseCLI(server, username, password)
        stdout, _, _ = cli.bucket_compact(bucket_name, data_only, views_only,
                                          admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Bucket compaction started"),
                            "Expected command to succeed")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")

    def testBucketCreate(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        bucket_name = self.input.param("bucket-name", None)
        bucket_type = self.input.param("bucket-type", None)
        memory_quota = self.input.param("memory-quota", None)
        eviction_policy = self.input.param("eviction-policy", None)
        replica_count = self.input.param("replica-count", None)
        enable_index_replica = self.input.param("enable-replica-index", None)
        priority = self.input.param("priority", None)
        enable_flush = self.input.param("enable-flush", None)
        wait = self.input.param("wait", False)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")
        reset_node = self.input.param("reset-node", True)

        server = copy.deepcopy(self.servers[0])

        if reset_node:
            rest = RestConnection(server)
            rest.force_eject_node()

        cli = CouchbaseCLI(server, username, password)
        if initialized and reset_node:
            _, _, success = cli.cluster_init(512, None, None, None, None, None,
                                             server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        stdout, _, _ = cli.bucket_create(bucket_name, bucket_type, memory_quota,
                                         eviction_policy, replica_count,
                                         enable_index_replica, priority,
                                         enable_flush, wait,
                                         admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error,
                                                     "Bucket created"),
                                                     "Expected command to succeed")
            self.assertTrue(self.verifyBucketSettings(server, bucket_name,
                                                bucket_type, memory_quota,
                                                eviction_policy, replica_count,
                                                enable_index_replica, priority,
                                                enable_flush),
                                                "Bucket settings not set properly")
        else:
            self.assertTrue(not self.verifyContainsBucket(server, bucket_name),
                            "Bucket was created even though an error occurred")
            self.assertTrue(self.verifyCommandOutput(stdout,
                                 expect_error, error_msg),
                                 "Expected error message not found")

    def testRecreateBucket(self):
        """
            Create a bucket name A
            Delete bucket name A
            Recreate bucket name A
            :return: nothing
        """
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        bucket_name = self.input.param("bucket-name", None)

        server=copy.deepcopy(self.servers[0])

        self.testBucketCreate()
        cli = CouchbaseCLI(server, username, password)
        cli.bucket_delete(bucket_name, admin_tools_package=self.admin_tools_package)
        self.testBucketCreate()


    def testBucketEdit(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        bucket_name = self.input.param("bucket-name", None)
        memory_quota = self.input.param("memory-quota", None)
        eviction_policy = self.input.param("eviction-policy", None)
        replica_count = self.input.param("replica-count", None)
        priority = self.input.param("priority", None)
        enable_flush = self.input.param("enable-flush", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")
        init_bucket_type = self.input.param("init-bucket-type", None)

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(512, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")
            if init_bucket_type is not None:
                _, _, success = cli.bucket_create(bucket_name, init_bucket_type, 256, None, None, None,
                                                  None, 0, None,
                                                  admin_tools_package=self.admin_tools_package)
                self.assertTrue(success, "Bucket not created during test setup")

        cli = CouchbaseCLI(server, username, password)
        stdout, _, _ = cli.bucket_edit(bucket_name, memory_quota, eviction_policy, replica_count,
                                       priority, enable_flush,
                                       admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Bucket edited"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyBucketSettings(server, bucket_name, None, memory_quota,
                                eviction_policy, replica_count, None, priority, enable_flush),
                            "Bucket settings not set properly")
        else:
            # List buckets
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")

    def testBucketDelete(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        bucket_name = self.input.param("bucket-name", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")
        init_bucket_type = self.input.param("init-bucket-type", None)

        server = copy.deepcopy(self.servers[-1])
        hostname = "%s:%s" % (server.ip, server.port)

        rest = RestConnection(server)
        rest.force_eject_node()

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(512, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")
            if init_bucket_type is not None:
                _, _, success = cli.bucket_create(bucket_name, init_bucket_type, 256, None, None, None,
                                                  None, 0, None,
                                                  admin_tools_package=self.admin_tools_package)
                self.assertTrue(success, "Bucket not created during test setup")

        cli = CouchbaseCLI(server, username, password)
        stdout, _, _ = cli.bucket_delete(bucket_name, admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Bucket deleted"),
                            "Expected command to succeed")
            self.assertTrue(not self.verifyContainsBucket(server, bucket_name), "Bucket was not deleted")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if initialized and init_bucket_type is not None:
                self.assertTrue(self.verifyContainsBucket(server, bucket_name), "Bucket should not have been deleted")

    def testBucketFlush(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        bucket_name = self.input.param("bucket-name", None)
        force = self.input.param("force", False)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        init_bucket_type = self.input.param("init-bucket-type", None)
        init_enable_flush = int(self.input.param("init-enable-flush", 0))
        insert_keys = 12

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(512, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")
            if init_bucket_type is not None:
                _, _, success = cli.bucket_create(bucket_name, init_bucket_type, 256, None, None, None,
                                                  None, init_enable_flush, None,
                                                  admin_tools_package=self.admin_tools_package)
                self.assertTrue(success, "Bucket not created during test setup")
                """ Add built-in user for memcached authentication """
                self.add_built_in_server_user(node=server)

                MemcachedClientHelper.load_bucket_and_return_the_keys([server], name=bucket_name, number_of_threads=1,
                                                                      write_only=True, number_of_items=insert_keys)
                inserted = int(rest.get_bucket_json(bucket_name)["basicStats"]["itemCount"])
                self.assertTrue(self.waitForItemCount(server, bucket_name, insert_keys))

        cli = CouchbaseCLI(server, username, password)
        stdout, _, _ = cli.bucket_flush(bucket_name, force,
                                        admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Bucket flushed"),
                            "Expected command to succeed")
            self.assertTrue(self.waitForItemCount(server, bucket_name, 0),
                            "Expected 0 keys to be in bucket after the flush")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if initialized and init_bucket_type is not None:
                self.assertTrue(self.waitForItemCount(server, bucket_name, insert_keys),
                                "Expected keys to exist after the flush failed")

    def testServerAdd(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        num_servers = self.input.param("num-add-servers", None)
        server_username = self.input.param("server-add-username", None)
        server_password = self.input.param("server-add-password", None)
        group = self.input.param("group-name", None)
        services = self.input.param("services", None)
        index_storage_mode = self.input.param("index-storage-mode", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")
        init_index_storage_mode = self.input.param("init-index-storage-mode", None)
        init_services = self.input.param("init-services", None)

        server = copy.deepcopy(self.servers[0])

        servers_list = list()
        servers_url_list = list()
        for i in range(0, num_servers):
            servers_list.append("%s:%s" % (self.servers[i+1].ip, self.servers[i+1].port))
            servers_url_list.append("{0}".format(self._convert_server_to_url(self.servers[i+1])))
        server_to_add = ",".join(servers_list)
        servers_to_add_url = ",".join(servers_url_list)

        rest = RestConnection(server)
        rest.force_eject_node()

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, 256, None, init_services,
                                             init_index_storage_mode, None,
                                             server.rest_username, server.rest_password,
                                             None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

        time.sleep(5)
        cli = CouchbaseCLI(server, username, password)
        stdout, _, _ = cli.server_add(servers_to_add_url, server_username, server_password,
                                                     group, services,
                                                     index_storage_mode,
                                                     admin_tools_package=self.admin_tools_package)

        if not services:
            services = "kv"
        if group:
            if (group[0] == group[-1]) and group.startswith(("'", '"')):
                group = group[1:-1]
        else:
            group = "Group 1"

        if not index_storage_mode and "index" in services.split(","):
            index_storage_mode = "default"

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Server added"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyPendingServer(server, server_to_add, group, services),
                            "Pending server has incorrect settings")
            self.assertTrue(self.verifyIndexSettings(server, None, None, None,
                                                     index_storage_mode, None, None),
                                                     "Invalid index storage setting")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if initialized:
                self.assertTrue(self.verifyPendingServerDoesNotExist(server, server_to_add),
                                "Pending server exists")

    def testRebalance(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        num_add_servers = self.input.param("num-add-servers", 0)
        num_remove_servers = self.input.param("num-remove-servers", 0)
        num_initial_servers = self.input.param("num-initial-servers", 1)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        self.assertTrue(num_initial_servers > num_remove_servers, "Specified more remove servers than initial servers")

        srv_idx = 0
        server = copy.deepcopy(self.servers[srv_idx])
        srv_idx += 1

        initial_servers_list = list()
        for _ in range(0, num_initial_servers-1):
            initial_servers_list.append("{0}"\
                            .format(self._convert_server_to_url(self.servers[srv_idx])))
            srv_idx += 1
        initial_servers = ",".join(initial_servers_list)

        add_servers_list = list()
        for _ in range(0, num_add_servers):
            add_servers_list.append("{0}"\
                            .format(self._convert_server_to_url(self.servers[srv_idx])))
            srv_idx += 1
        servers_to_add = ",".join(add_servers_list)

        remove_servers_list = list()
        for i in range(0, num_remove_servers):
            remove_servers_list.append("{0}:{1}"\
                                  .format(self.servers[i+1].ip, self.servers[i+1].port))
        servers_to_remove = ",".join(remove_servers_list)

        rest = RestConnection(server)
        rest.force_eject_node()

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")
            time.sleep(5)
            if initial_servers != "":
                _, _, errored = cli.server_add(initial_servers, server.rest_username, server.rest_password, None, None, None, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Unable to add initial servers")
                _, _, errored = cli.rebalance(None, self.admin_tools_package)
                self.assertTrue(errored, "Unable to complete initial rebalance")
            if servers_to_add != "":
                _, _, errored = cli.server_add(servers_to_add, server.rest_username, server.rest_password, None, None, None, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Unable to add initial servers")

        cli = CouchbaseCLI(server, username, password)
        stdout, _, _ = cli.rebalance(servers_to_remove, admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Rebalance complete"),
                            "Expected command to succeed")
            self.assertTrue(self.verifyActiveServers(server, num_initial_servers + num_add_servers - num_remove_servers),
                            "No new servers were added to the cluster")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if initialized:
                self.assertTrue(self.verifyActiveServers(server, num_initial_servers),
                                "Expected no new servers to be in the cluster")

    def testRebalanceInvalidRemoveServer(self):
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
        _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                         server.rest_password, None,
                                         admin_tools_package=self.admin_tools_package)
        self.assertTrue(success, "Cluster initialization failed during test setup")
        time.sleep(5)

        stdout, _, _ = cli.rebalance("invalid.server:8091",
                                     admin_tools_package=self.admin_tools_package)

        self.assertTrue(self.verifyCommandOutput(stdout, True, error_msg),
                        "Expected error message not found")
        self.assertTrue(self.verifyActiveServers(server, 1),
                        "Expected no new servers to be in the cluster")

    def testFailover(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        num_initial_servers = self.input.param("num-initial-servers", 2)
        invalid_node = self.input.param("invalid-node", False)
        no_failover_servers = self.input.param("no-failover-servers", False)
        force = self.input.param("force", False)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        initial_servers_list = list()
        for i in range(0, num_initial_servers - 1):
            initial_servers_list.append("{0}"\
                            .format(self._convert_server_to_url(self.servers[i + 1])))
        initial_servers = ",".join(initial_servers_list)

        server_to_failover = "{0}:{1}".format(self.servers[1].ip, self.servers[1].port)
        if invalid_node:
            server_to_failover = "invalid.server:8091"
        if no_failover_servers:
            server_to_failover = None

        rest = RestConnection(server)
        rest.force_eject_node()

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, None, None, None, None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")
            if initial_servers != "":
                time.sleep(5)
                _, _, errored = cli.server_add(initial_servers, server.rest_username, server.rest_password, None, None,
                                               None, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Unable to add initial servers")
                _, _, errored = cli.rebalance(None, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Unable to complete initial rebalance")
            _, _, success = cli.bucket_create("bucket", "couchbase", 256, None, None, None, None,
                                                  None, None,
                                              admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Bucket not created during test setup")
            time.sleep(10)

        cli = CouchbaseCLI(server, username, password)
        stdout, _, _ = cli.failover(server_to_failover, force,
                                    admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, "Server failed over"),
                            "Expected command to succeed. The command  output: " + str(stdout))
            self.assertTrue(self.verifyActiveServers(server, num_initial_servers - 1),
                            "Servers not failed over")
            self.assertTrue(self.verifyFailedServers(server, 1),
                            "Not all servers failed over have `inactiveFailed` status")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if initialized:
                self.assertTrue(self.verifyActiveServers(server, num_initial_servers),
                                "Servers should not have been failed over")

    def testUserManage(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        list = self.input.param("list", False)
        delete = self.input.param("delete", False)
        set = self.input.param("set", False)
        roles = self.input.param("roles", None)
        rbac_username = self.input.param("rbac-username", None)
        rbac_password = self.input.param("rbac-password", None)
        auth_domain = self.input.param("auth-domain", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        init_rbac_username = self.input.param("init-rbac-username", None)
        init_rbac_password = self.input.param("init-rbac-password", None)

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        payload = "name=%s&roles=%s" % (init_rbac_username, roles)
        if auth_domain == "local":
            init_user = "local/" + init_rbac_username
            rbac_user = "local/" + rbac_username
            payload = "name=%s&roles=%s&password=%s" \
                      % (init_rbac_username, roles, init_rbac_password)
        elif auth_domain == "external":
            init_user = "external/" + init_rbac_username
            rbac_user = "external/" + rbac_username
        else:
            self.fail("need to set auth-domain to run this test")

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, 256, None, "data", None, None,
                                             server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")
            if init_rbac_username and init_rbac_password:
                rest.set_user_roles(init_user, payload)
                result = self.verifyUserRoles(server, init_rbac_username, roles)
                self.assertTrue(result,
                                "Setting initial rbac user failed")

        time.sleep(5)
        cli = CouchbaseCLI(server, username, password)
        stdout, _, errored = cli.user_manage(delete, list, set, rbac_username, rbac_password,
                                             roles, auth_domain,
                                             admin_tools_package=self.admin_tools_package)

        if not expect_error:
            if list:
                self.assertTrue(stdout[0] == init_rbac_username,
                                "Listed rbac user is not correct")
            else:
                self.assertTrue(errored, "Expected command to succeed")
            if set:
                self.assertTrue(self.verifyUserRoles(server, rbac_username, roles),
                                "Read only user was not set")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")
            if initialized and init_rbac_username:
                self.assertTrue(self.verifyUserRoles(server, init_rbac_username, roles),
                                "Read only user was changed")
        self.log.info("Delete users after test")
        if init_rbac_username:
            rest.delete_user_roles(init_user)
        elif rbac_username:
            rest.delete_user_roles(rbac_user)

    def testCollectLogStart(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        all_nodes = self.input.param("all-nodes", False)
        nodes = self.input.param("nodes", 0)
        upload = self.input.param("upload", None)
        upload_host = self.input.param("upload-host", None)
        upload_customer = self.input.param("customer", None)
        upload_ticket = self.input.param("ticket", None)
        invalid_node = self.input.param("invalid-node", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        init_num_servers = self.input.param("init-num-servers", 1)

        server = copy.deepcopy(self.servers[0])
        if len(self.servers) < 4:
            mesg = "***\n Collect logs start tests need minimum 4 servers to run\n***"
            RemoteMachineShellConnection(server).stop_current_python_running(mesg)

        rest = RestConnection(server)
        rest.force_eject_node()

        servers_to_add = list()
        for idx in range(init_num_servers-1):
            servers_to_add.append("{0}"\
                       .format(self._convert_server_to_url(self.servers[idx + 1])))
        servers_to_add = ",".join(servers_to_add)

        log_nodes = None
        if nodes > 0 or invalid_node is not None:
            log_nodes = list()
            for idx in range(nodes):
                log_nodes.append("%s:%s" % (self.servers[idx + 1].ip, self.servers[idx + 1].port))
            if invalid_node is not None:
                log_nodes.append("invalid:8091")
            log_nodes = ",".join(log_nodes)

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, 256, None, "data", None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

            if init_num_servers > 1:
                time.sleep(5)
                _, _, errored = cli.server_add(servers_to_add, server.rest_username, server.rest_password, None, None, None, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Could not add initial servers")
                _, _, errored = cli.rebalance(None, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Unable to complete initial rebalance")

        time.sleep(5)
        cli = CouchbaseCLI(server, username, password)
        stdout, _, errored = cli.collect_logs_start(all_nodes, log_nodes, upload, upload_host, upload_customer, upload_ticket, admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(errored, "Expected command to succeed")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")

    def testCollectLogStop(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, 256, None, "data", None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")
        time.sleep(5)
        cli = CouchbaseCLI(server, username, password)
        stdout, _, errored = cli.collect_logs_stop(admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(errored, "Expected command to succeed")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")

    def test_mctimings_with_data_monitoring_role(self):
        """ This role only works from 5.1 and later
            params: sasl_buckets=2,default_bucket=False,nodes_init=2,
                    permission=self_bucket
            if permission=other_bucket, need to add should-fail=True
        """
        if 5.1 > float(self.cb_version[:3]):
            self.log.info("This test only work for version 5.1+")
            return
        if len(self.buckets) < 2:
            self.fail("This test requires minimum of 2 buckets")

        permission = self.input.param("permission", "all")
        username = "data_monitoring"
        bucket_names = []
        bucket_name = ""
        rest = RestConnection(self.master)
        shell = RemoteMachineShellConnection(self.master)
        for bucket in self.buckets:
            bucket_names.append(bucket.name)
        if permission == "all":
            role = '*'
            bucket_name = bucket_names[random.randint(0, 1)]
        elif permission == "self_bucket":
            role = "{0}".format(bucket_names[0])
            bucket_name = bucket_names[0]
        elif permission == "other_bucket":
            role = "{0}".format(bucket_names[1])
            bucket_name = bucket_names[0]
        testuser = [{"id": username,
                         "name": username,
                         "password": "password"}]
        rolelist = [{"id": username,
                         "name": username,
                         "roles": "data_monitoring[{0}]".format(role)}]
        kv_gen = BlobGenerator('create', 'create', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, kv_gen, "create",
                               self.expire_time, flag=self.item_flag)
        try:
            status = self.add_built_in_server_user(testuser, rolelist)
            if not status:
                self.fail("Failed to add user: {0} with role: {1} "\
                                             .format(username, role))
            cmd = self.cli_command_path + "mctimings" + self.cmd_ext
            cmd += " -h " + self.master.ip + ":11210 -u " + username
            cmd += " -P password -b " + bucket_name + " --verbose "
            output, _ = shell.execute_command(cmd)
            if not self.should_fail:
                self.assertTrue(self._check_output("The following data is collected", output))
            else:
                if self._check_output("The following data is collected", output):
                    self.fail("This user should not allow to monitor data in this bucket {0}"\
                                                                      .format(bucket_name))
                else:
                    self.log.info("Alright, user bucket A has no permission to check bucket B")
        except Exception as e:
            print(e)
        finally:
            shell.disconnect()
            if status:
                self.log.info("Remove user {0}".format(rolelist))
                RbacBase().remove_user_role(["data_monitoring"], rest)

    def test_cmd_set_stats(self):
        """ When set any items, cmd_set should increase counting number.
            params: default_bucket=False,sasl_buckets=1
        /opt/couchbase/bin/cbstats localhost:11210 all -u Administrator -p password -b standard_bucket0 | grep cmd_set
        cmd_set: 10011
        """

        shell = RemoteMachineShellConnection(self.master)
        cmd = self.cli_command_path + "cbstats" + self.cmd_ext + " "
        cmd += self.master.ip + ":11210 all -u Administrator -p password "
        cmd += "-b standard_bucket0 | grep cmd_set"

        output, _ = shell.execute_command(cmd)
        self.assertTrue(self._check_output("0", output))
        kv_gen = BlobGenerator('create', 'create', self.value_size, end=1000)
        self._load_all_buckets(self.master, kv_gen, "create",
                               self.expire_time, flag=self.item_flag)

        output, _ = shell.execute_command(cmd)
        self.assertTrue(self._check_output("1000", output))
        shell.disconnect()

    def testNodeInit(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        data_path = self.input.param("data-path", None)
        index_path = self.input.param("index-path", None)
        hostname = self.input.param("hostname", None)
        initialized = self.input.param("initialized", False)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        node_settings = rest.get_nodes_self()
        if self.os == "windows":
            self.log_path = self.log_path.replace("/cygdrive/c/", "c:/")

        if data_path is not None:
            if data_path == "valid":
                data_path = self.log_path
            elif self.os == "windows" and data_path[:1] == "/":
                data_path = "c:" + data_path

        if index_path is not None:
            if index_path == "valid":
                index_path = self.log_path
            elif self.os == "windows" and index_path[:1] == "/":
                index_path = "c:" + index_path

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, 256, None, "data", None, None,
                                             server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")
        time.sleep(5)
        cli = CouchbaseCLI(server, username, password)

        stdout, _, errored = cli.node_init(data_path, index_path, hostname,
                                           admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(errored, "Expected command to succeed")
            if data_path is None:
                data_path = node_settings.storage[0].path
            elif self.os == "windows":
                data_path = data_path.replace("\\", "")[:-1]

            if index_path is None:
                index_path = node_settings.storage[0].index_path
            elif self.os == "windows":
                index_path = index_path.replace("\\", "")[:-1]
            self.assertTrue(self.verify_node_settings(server, data_path,
                                                      index_path, hostname),
                            "Node settings not changed")
        elif self.os != "windows":
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                                                    "Expected error message not found")

    def testGroupManage(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        create = self.input.param("create", None)
        delete = self.input.param("delete", None)
        list = self.input.param("list", None)
        move = self.input.param("move-servers", 0)
        rename = self.input.param("rename", None)
        name = self.input.param("name", None)
        from_group = self.input.param("from-group", None)
        to_group = self.input.param("to-group", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        init_group = self.input.param("init-group", None)
        init_num_servers = self.input.param("init-num-servers", 1)
        invalid_move_server = self.input.param("invalid-move-server", None)

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        to_move = None
        if move > 0:
            to_move = []
            for idx in range(move):
                to_move.append("%s:%s" % (self.servers[idx].ip, self.servers[idx].port))
            to_move = ",".join(to_move)

        if invalid_move_server:
            to_move = invalid_move_server

        servers_to_add = []
        for idx in range(init_num_servers-1):
            servers_to_add.append("{0}"\
                          .format(self._convert_server_to_url(self.servers[idx + 1])))
        servers_to_add = ",".join(servers_to_add)

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, 256, None, "data", None, None, server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

            if init_num_servers > 1:
                time.sleep(5)
                _, _, errored = cli.server_add(servers_to_add, server.rest_username, server.rest_password, None, None, None, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Could not add initial servers")
                _, _, errored = cli.rebalance(None, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Unable to complete initial rebalance")

            if init_group is not None:
                time.sleep(5)
                _, _, errored = cli.group_manage(True, False, False, None, None, init_group, None, None, admin_tools_package=self.admin_tools_package)

        time.sleep(5)
        cli = CouchbaseCLI(server, username, password)
        stdout, _, errored = cli.group_manage(create, delete, list, to_move, rename, name, to_group, from_group, admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(errored, "Expected command to succeed")
            if create:
                self.assertTrue(self.verifyGroupExists(server, name), "Group doesn't exist")
            elif delete:
                self.assertTrue(not self.verifyGroupExists(server, name), "Group doesn't exist")
            elif rename:
                self.assertTrue(self.verifyGroupExists(server, rename), "Group not renamed")
            elif move > 0:
                _, _, errored = cli.group_manage(False, False, False, to_move, None, None, from_group, to_group, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Group reset failed")

        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")

    def testRecovery(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        servers = self.input.param("servers", 0)
        recovery_type = self.input.param("recovery-type", None)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        skip_failover = self.input.param("skip-failover", False)
        init_num_servers = self.input.param("init-num-servers", 1)
        invalid_recover_server = self.input.param("invalid-recover-server", None)

        server = copy.deepcopy(self.servers[0])

        rest = RestConnection(server)
        rest.force_eject_node()

        servers_to_recover = None
        if servers > 0:
            servers_to_recover = []
            for idx in range(servers):
                servers_to_recover.append("{0}:{1}".format(self.servers[idx+1].ip,
                                                     self.servers[idx+1].port))
            servers_to_recover = ",".join(servers_to_recover)

        if invalid_recover_server:
            servers_to_recover = invalid_recover_server

        servers_to_add = []
        for idx in range(init_num_servers - 1):
            server_add = "{0}".format(self._convert_server_to_url(self.servers[idx + 1]))
            servers_to_add.append(server_add)
        servers_to_add = ",".join(servers_to_add)

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, 256, None, "data", None, None,
                                             server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

            if init_num_servers > 1:
                time.sleep(5)
                _, _, errored = cli.server_add(servers_to_add, server.rest_username,
                                               server.rest_password, None, None,
                                               None,
                                               admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Could not add initial servers")
                _, _, errored = cli.rebalance(None, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Unable to complete initial rebalance")

            if servers_to_recover and not skip_failover:
                for restore_server in servers_to_recover.split(","):
                    _, errored, status = cli.failover(restore_server, True,
                                                      admin_tools_package=self.admin_tools_package)
                    self.assertTrue(status, "Unable to failover servers")

        time.sleep(5)
        cli = CouchbaseCLI(server, username, password)
        stdout, _, errored = cli.recovery(servers_to_recover, recovery_type,
                                          admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(errored, "Expected command to succeed")
            self.assertTrue(self.verifyRecoveryType(server, servers_to_recover,
                                                    recovery_type), "Servers not recovered")
        else:
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msg),
                            "Expected error message not found")

    def testServerReadd(self):
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        servers = self.input.param("servers", 0)
        initialized = self.input.param("initialized", True)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        skip_failover = self.input.param("skip-failover", False)
        init_num_servers = self.input.param("init-num-servers", 1)
        invalid_recover_server = self.input.param("invalid-recover-server", None)

        server = copy.deepcopy(self.servers[0])
        if len(self.servers) < 4:
            mesg = "***\n Sever readd tests need minimum 4 servers to run\n***"
            RemoteMachineShellConnection(server).stop_current_python_running(mesg)

        rest = RestConnection(server)
        rest.force_eject_node()

        servers_to_recover = None
        if servers > 0:
            servers_to_recover = []
            for idx in range(servers):
                servers_to_recover.append("%s" % (self.servers[idx + 1].ip))
            servers_to_recover = ",".join(servers_to_recover)

        if invalid_recover_server:
            servers_to_recover = invalid_recover_server

        servers_to_add = []
        for idx in range(init_num_servers - 1):
            servers_to_add.append("{0}".format(self.servers[idx + 1].ip))
        servers_to_add = ",".join(servers_to_add)

        if initialized:
            cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
            _, _, success = cli.cluster_init(256, 256, None, "data", None, None,
                                             server.rest_username,
                                             server.rest_password, None,
                                             admin_tools_package=self.admin_tools_package)
            self.assertTrue(success, "Cluster initialization failed during test setup")

            if init_num_servers > 1:
                time.sleep(5)
                _, _, errored = cli.server_add(servers_to_add, server.rest_username,
                                               server.rest_password, None, None, None,
                                               admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Could not add initial servers")
                _, _, errored = cli.rebalance(None, admin_tools_package=self.admin_tools_package)
                self.assertTrue(errored, "Unable to complete initial rebalance")

            if servers_to_recover and not skip_failover:
                for restore_server in servers_to_recover.split(","):
                    _, _, errored = cli.failover(restore_server, True,
                                                 admin_tools_package=self.admin_tools_package)
                    self.assertTrue(errored, "Unable to failover servers")

        time.sleep(5)
        cli = CouchbaseCLI(server, username, password)
        stdout, _, errored = cli.server_readd(servers_to_recover,
                                              admin_tools_package=self.admin_tools_package)

        if not expect_error:
            self.assertTrue(errored, "Expected command to succeed")
            self.assertTrue(self.verifyRecoveryType(server, servers_to_recover, "full"),
                                                               "Servers not recovered")
        else:
            error_msgs = ['invalid node name or node can\'t be used for delta recovery']
            error_msgs.append(error_msg)
            self.assertTrue(self.verifyCommandOutput(stdout, expect_error, error_msgs),
                                                   "Expected error message not found")

    def test_change_admin_password_with_read_only_account(self):
        """ this test automation for bug MB-20170.
            In the bug, when update password of admin, read only account is removed.
            This test is to maker sure read only account stay after update password
            of Administrator. """
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

    def test_directory_backup_structure(self):
        """ directory of backup stuctrure should be like
            /backup_path/date/date-mode/ as in
            /tmp/backup/2016-08-19T185902Z/2016-08-19T185902Z-full/
            automation test for bug in ticket MB-20021

            default params to run:
                backup_cmd=cbbackup,load_all=false,num_sasl_buckets=1 """
        backup_cmd = self.input.param("backup_cmd", None)
        load_all = self.input.param("load_all", None)
        num_backup_bucket = self.input.param("num_backup_bucket", "all")
        num_sasl_buckets = self.input.param("num_sasl_buckets", 1)
        self.bucket_size = 256
        bucket_params=self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy)
        self.cluster.create_default_bucket(bucket_params)
        self._create_standard_buckets(self.master, num_sasl_buckets)
        self.buckets = RestConnection(self.master).get_buckets()
        if load_all is None:
            self.shell.execute_cbworkloadgen("Administrator", "password", 10000,
                                              0.95, "default", 125, " -j")
        elif str(load_all).lower() == "true":
            for bucket in self.buckets:
                self.log.info("load data to bucket %s " % bucket.name)
                self.shell.execute_cbworkloadgen("Administrator", "password", 10000,
                                                      0.95, bucket.name, 125, " -j")
        self.log.info("remove any backup data on node ")
        self.shell.execute_command("rm -rf %sbackup" % self.tmp_path)
        self.log.info("create backup data on node ")
        self.shell.execute_command("mkdir %sbackup " % self.tmp_path)

        """ start backup data and check directory structure """
        output, error = self.shell.execute_command("date +%Y")
        dir_start_with = output[0] + "-"
        backup_all_buckets = True
        partial_backup_buckets = []

        if backup_cmd == "cbbackupmgr":
            backup_repo = "backup-test"
            node_credential = "--username Administrator --password password "
            self.log.info("Create backup repo : %s" % backup_repo)
            """ format of setting backup repo
                ./cbbackupmgr config --archive /tmp/backup --repo backup-test """
            self.shell.execute_command("%s%s%s config --archive %s --repo %s"
                                       % (self.cli_command_path, backup_cmd,
                                          self.cmd_ext, self.cmd_backup_path,
                                          backup_repo))
            output, error = self.shell.execute_command("ls %s" % self.backup_path)
            result = output
            if result and backup_repo in result:
                self.log.info("repo %s successful created " % backup_repo)
                output, error = self.shell.execute_command("ls %s%s"
                                            % (self.backup_path, backup_repo))
                if output and "backup-meta.json" in output:
                    self.log.info("backup-meta.json successful created ")
                elif output:
                    self.fail("fail to create backup-meta.json file")
            if result and "logs" in result:
                self.log.info("logs dir successful created ")
                output, error = self.shell.execute_command("ls %slogs"
                                                    % self.backup_path)
                if output and "backup-0.log" in output:
                    self.log.info("backup.log file successful created ")
                else:
                    self.fail("fail to create backup.log file")
            """ start backup bucket
                command format:
                cbbackupmgr backup --archive /tmp/backup --repo backup-test
                                   --host ip_addr
                                   --username Administrator
                                   --password password """
            if num_backup_bucket == "all":
                self.shell.execute_command("%s%s%s backup --archive %s "
                                           " --repo %s --host %s:8091 %s"
                                           % (self.cli_command_path, backup_cmd,
                                              self.cmd_ext, self.cmd_backup_path,
                                              backup_repo, self.shell.ip,
                                              node_credential))
                out, err = self.shell.execute_command("ls %s%s"
                                            % (self.backup_path, backup_repo))
                if out and len(out) > 1:
                    """ Since in this dir, there is a dir start with a number
                        and a file.  So the dir will list first.
                        Example of this dir: 2016-08-.. backup-meta.json """
                    if out[0].startswith(dir_start_with):
                        self.log.info("First level of backup dir is correct %s"
                                                                      % out[0])
                    else:
                        self.fail("Incorrect directory name %s.  "
                                         "It should start with %s"
                                       % (out[0], dir_start_with))
                elif out:
                    self.fail("backup did not run correctly %s" % out)
            out, err = self.shell.execute_command("ls %s%s/%s*"
                                            % (self.backup_path, backup_repo,
                                                             dir_start_with))
            """ get buckets in directory """
            if out and len(out) >=1:
                if "plan.json" in out:
                    out.remove("plan.json")
                else:
                    self.fail("Missing plan.json file in this dir")
                out = [w.split("-", 1)[0] for w in out]
            if backup_all_buckets:
                for bucket in self.buckets:
                    if bucket.name in out:
                        self.log.info("Bucket %s was backuped "
                                                 % bucket.name)
                    else:
                        self.fail("failed to backup bucket %s "
                                                 % bucket.name)
            """ Check content of backup bucket.
                  Total dir and files:
                    bucket-config.json  data  full-text.json  gsi.json
                    range.json views.json """
            backup_folder_content = ["bucket-config.json", "data",
                                     "full-text.json", "gsi.json", "gsi.metadata.json",
                                      "range.json", "views.json",
                                      "query.json", "query.metadata.json",
                                      "ranges"]
            for bucket in self.buckets:
                out, err = self.shell.execute_command("ls %s%s/%s*/%s-*"
                                            % (self.backup_path, backup_repo,
                                                dir_start_with, bucket.name))
                if out and len(out) > 1:
                    self.log.info("Check content of backup dir of bucket %s: %s"
                                                           % (bucket.name, out))
                    for ele in out:
                        if ele not in backup_folder_content:
                            self.fail("Extra file/folder in backup repo: %s" % ele)
                else:
                    self.fail("Missing backup dir or files in backup bucket %s"
                                                                 % bucket.name)
        self.log.info("Remove backup directory at the end of test")
        self.shell.execute_command("rm -rf %sbackup" % self.tmp_path)
        self.shell.disconnect()

    def test_reset_admin_password(self):
        remote_client = RemoteMachineShellConnection(self.master)

        options = ''
        cli_command = "{0}couchbase-cli{1} reset-admin-password ".format(self.cli_command_path,
                                                                         self.cmd_ext)
        output, error = remote_client.execute_command("{0} {1}".format(cli_command, options))
        self.assertTrue(self._check_output('usage: couchbase-cli reset-admin-password', output))

        options = '--blabla'
        output, error = remote_client.execute_command("{0} {1}".format(cli_command, options))
        self.assertTrue(self._check_output("unrecognized arguments:", error))

        options = '--new-password aaa'
        output, error = remote_client.execute_command("{0} {1}".format(cli_command, options))
        self.assertTrue(self._check_output("The password must be at least 6 characters long",
                                           output))

        try:
            options = '--regenerate'
            outputs = []
            for i in range(10):
                output, error = remote_client.execute_command("{0} {1}".format(cli_command,
                                                                               options))
                new_password = ""
                for x in output:
                    if not x.startswith("DEPRECATED") and len(x) >= 8:
                        new_password = x
                self.assertTrue(len(new_password) >= 8)
                self.assertTrue(new_password not in outputs)
                outputs.append(new_password)

            old_password = "password"
            apis = ["get_zone_and_nodes", "get_pools_info", "get_notifications", "get_pools",
                    "_rebalance_progress_status", "get_nodes_version", "get_alerts_settings"]
            for i in range(10):
                server = copy.deepcopy(self.servers[0])
                chars = string.ascii_letters + string.digits
                new_password = ''.join((random.choice(chars)) for x in range(random.randint(6, 30)))
                options = '--new-password "%s"' % new_password
                output, _ = remote_client.execute_command("{0} {1}".format(cli_command,
                                                                           options))
                self.assertTrue(self._check_output('SUCCESS: Administrator password changed',
                                                   output))
                server.rest_password = old_password
                rest = RestConnection(server)
                server.rest_password = new_password
                old_password = new_password

                rest_new = RestConnection(server)
                for m in apis:
                        getattr(rest_new, m)()
                for m in apis:
                        self.assertRaises(Exception, getattr(rest, m))
            for i in range(5):
                server = copy.deepcopy(self.servers[0])
                options = '--regenerate'
                output, _ = remote_client.execute_command("{0} {1}".format(cli_command,
                                                                           options))
                new_password = ""
                for x in output:
                    if not x.startswith("DEPRECATED") and len(x) >= 8:
                        new_password = x
                server.rest_password = old_password
                rest = RestConnection(server)

                server.rest_password = new_password
                rest_new = RestConnection(server)

                old_password = new_password
                for m in apis:
                    getattr(rest_new, m)()
                for m in apis:
                    self.assertRaises(Exception, getattr(rest, m))
        finally:
            self.log.info("Inside finally block")
            options = '--new-password password'
            output, error = remote_client.execute_command("{0} {1}".format(cli_command,
                                                           options))

    def test_multiple_ca(self):
        """ Tests each command related to multiple CAs
        Requires a testrunner parameter of 'ca_option'
        If the parameter passed in is not upload, generates and uploads certs in setup
        """
        # Generate the command
        options = "--"
        cli_command = f"{self.cli_command_path}couchbase-cli{self.cmd_ext} ssl-manage "
        ca_option = self.input.param("ca_option", "")
        if not ca_option: self.fail("An option must be provided")
        options += ca_option
        #TODO: have this retrieve CA IDs from the cluster
        if ca_option == "cluster-ca-delete": options += " 1"
        if ca_option == "upload-cluster-ca":
            options += f" {self.base_cb_path}var/lib/couchbase/inbox/CA/r1_ca.pem"
        options += f" -c localhost:{self.master.port} -u {self.master.rest_username} -p {self.master.rest_password}"

        # Execute the command
        remote_client = RemoteMachineShellConnection(self.master)
        command = cli_command + options
        output, error = remote_client.execute_command(command)
        remote_client.disconnect()

        # Verify the command output
        self.log.info(f"OUTPUT OF REMOTE COMMAND: {output}")
        self.log.error(f"ERROR FROM REMOTE COMMAND: {error}")
        self.assertFalse((error or self._check_output("error", output)))

    def test_backup_audit_event(self):
        """ This test will check if backup could log event in case permission denied
            due to incorrect password or role when using backup
        """
        cb_version = self.cb_version[:5]
        if cb_version == "7.0.0":
            self.fail("This test is for version 7.0.1 and above in cheshire-cat")
        bk_services = self.rest.get_nodes_services().values()
        if "backup" not in str(bk_services):
            self.rest.force_eject_node()
            cli = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password,
                               self.cb_version)
            _, _, success = cli.cluster_init(1024, 512, None, "data,index,query,eventing,backup", None, None,
                                                 self.master.rest_username,
                                                 self.master.rest_password, None,
                                                 admin_tools_package=self.admin_tools_package)
            self.sleep(15)
            self.assertTrue(success, "Cluster initialization failed during test setup")
        rest = RestConnection(self.master)
        bk_service = rest.get_nodes_services()

        """ Enable audit """
        options = " --set --audit-enabled 1 "
        output, error = self.shell.execute_couchbase_cli("setting-audit", options=options,
                                                         admin_tools_package=self.admin_tools_package)
        self.assertTrue(self._check_output('SUCCESS: Audit settings modified', output))
        self.sleep(5)
        options =  ' -d \'{"name": "Backuptest","options": null,"full_backup": true},{"name": "test","task_type":"MERGE","full_backup":null}\' '
        tls = '1' if self.master.protocol == 'https://' else ''
        cmd1 = "curl -g -k -X PUT {0}{3}:{1}8097/api/v1/plan/BackupAndMergePlan -u Administrator:wrongpassword {2}".format(self.master.protocol,
                                                                                                                        tls,
                                                                                                                        options,
                                                                                                                        self.master.ip)
        output = subprocess.check_output(cmd1, shell=True)
        words_check = "\'A user has been denied access to the REST API\'"
        cmd2 = "cat%s %s/audit.log | grep%s %s " % (self.cmd_ext, self.log_path,self.cmd_ext, words_check)
        output, error = self.shell.execute_command(cmd2)
        self.assertTrue(self._check_output('A user has been denied access to the REST API', output))
        """ test with cli """
        options = " settings --get "
        output, error = self.shell.execute_couchbase_cli("backup-service", options=options,
                                                         password="xxpasswo",
                                                         admin_tools_package=self.admin_tools_package)
        words_check = "\'REST operation failed due to authentication failure\'"
        cmd3 = "cat%s %s/audit.log | grep%s %s " % (self.cmd_ext, self.log_path,self.cmd_ext, words_check)
        output, error = self.shell.execute_command(cmd3)
        self.assertTrue(self._check_output('REST operation failed due to authentication failure', output))

    def test_cli_with_offline_upgrade(self):
        """
           Install a node with watson
           Create users with roles
           Upgrade to spock
           Verify users still in cluster.
           Add more users with different roles
           parameters in conf file:
               conf/couchbase-cli/py-offline-upgrade-rbac.conf
        """
        username = self.input.param("username", "Administrator")
        new_users = self.input.param("new-users", None)
        password = self.input.param("password", "password")
        new_password = self.input.param("new-password", None)
        command = self.input.param("command", None)
        sub_command = self.input.param("sub-command", None)
        new_users = self.input.param("new-users", None)
        new_roles = self.input.param("new-roles", None)
        if "star" in new_roles:
            new_roles = new_roles.replace("star", "*")
        enabled = self.input.param("enabled", False)
        default = self.input.param("default", None)
        self.bucket_type = self.input.param("bucket-type", "couchbase")
        self.bucket_ram = self.input.param("bucket-ram", 256)
        self.eviction_policy = self.input.param("eviction-policy", "fullEviction")
        replica_count = self.input.param("replica-count", 1)
        enable_index_replica = self.input.param("enable-replica-index", 1)
        self.priority = self.input.param("priority", "high")
        self.enable_flush = self.input.param("enable-flush", 0)
        ops_before_upgrade = self.input.param("ops-before-upgrade", False)
        ops_during_upgrade = self.input.param("ops-during-upgrade", False)
        ops_after_upgrade = self.input.param("ops-after-upgrade", False)

        """
            Start install old version with param initial_version
        """
        self._install(self.servers[:self.nodes_init])
        self.cluster.rebalance([self.master], self.servers[1:self.nodes_init], [])

        """
            During upgrade operations
        """
        self._offline_upgrade(skip_init=True)

        """
            After upgrade operations
        """
        self.sleep(5)

        if new_users and new_roles:
            self.log.info("Add new users with roles after upgrade to version %s"
                                % self.upgrade_versions)
            testuser = [{"id": "%s" % new_users,
                         "name": "%s" % new_users,
                         "password": "password"}]
            rolelist = [{"id": "%s" % new_users,
                         "name": "%s" % new_users,
                         "roles": "%s" % new_roles}]
            try:
                status = self.add_built_in_server_user(testuser, rolelist)
                if not status:
                    self.fail("Failed to add user: %s with role: %s "
                                                 % (new_users, new_roles))
                self.log.info("Verify rbac user added after upgrade ")
                found = self._verify_rbac_users(self.master, username, password,
                                                            new_users, new_roles)
                if not found:
                    self.fail("Failed to add rbac user in")
                self.log.info("Run CLI command with new user")
                cmd = "%s%s%s " % (self.cli_command_path, command, self.cmd_ext)
                if sub_command:
                    cmd += "%s --cluster %s --user %s --password %s " \
                           % (sub_command, self.master.ip, username, password)
                if command and sub_command:
                    self.shell.execute_command(cmd)
                rest = RestConnection(self.master)
                cb_version = rest.get_nodes_version()
                if self.debug_logs:
                    print("output from exe command  ", output)
            except Exception as e:
                if e:
                    print("Exception error:   ", e)

            self.log.info("Operation after upgrade")
            if ops_after_upgrade == "bucket-ops":
                self._bucket_ops(self.master, new_users, password, new_roles)

            self.shell.disconnect()

    def _verify_rbac_users(self, server, login_user, login_password, users, roles):
        cmd = "%scouchbase-cli%s user-manage " % (self.cli_command_path,
                                                  self.cmd_ext)
        cmd += "-c %s -u %s -p %s --list" % (server.ip, login_user, login_password)
        shell = RemoteMachineShellConnection(server)

        users_found = False
        roles_found = False
        found = False
        output, _ = shell.execute_command(cmd)
        shell.disconnect()

        output = [x.strip() for x in output]
        output = [x.strip(",") for x in output]
        if self.debug_logs:
            print("output from verify rbac users:\n", output)
        for x in output:
            if users in x:
                users_found = True
            if "]" in roles:
                roles_name = re.sub("[\[].*?[\]]", "", roles)
            if roles_name in x:
                roles_found = True
        if users_found and roles_found:
            self.log.info("Users '%s' and roles '%s' found in cluster"
                                                       % ( users, roles))
            found = True
        return found

    def _bucket_ops(self, server, user, password, roles):
        self.log.info("Do some bucket operation like create, delete")
        bucket_create_roles = ["admin", "cluster_admin"]
        cli = CouchbaseCLI(server, user, password)
        try:
            self.log.info("Create bucket 'bucket1' with roles {0} ".format(roles))
            cli.bucket_create("bucket1", self.bucket_type, 256,
                                         self.eviction_policy, 1,
                                         1, self.priority,
                                         0, True, admin_tools_package=self.admin_tools_package)
            self.buckets = RestConnection(server).get_buckets()
            bucket_found = False
            for bucket in self.buckets:
                if bucket.name == "bucket1":
                    if roles not in bucket_create_roles:
                        self.fail("Roles {0} should not have permission to create bucket. "\
                                                                             .format(roles))
                    self.log.info("Found bucket '{0}' in cluster".format(bucket.name))
                    bucket_found = True
            if not bucket_found:
                self.fail("Failed to create bucket by user {0} with roles: {1}"\
                                                               .format(user, roles))
        except Exception as e:
            if e and roles not in bucket_create_roles:
                print("\nBucket permission of roles '{0}' is enforced\n".format(roles))
                self.log.info("Create bucket with admin roles for next test")
                cli = CouchbaseCLI(server, "Administrator", password)
                cli.bucket_create("bucket1", self.bucket_type, self.bucket_ram,
                                             self.eviction_policy, 1,
                                             1, self.priority,
                                             self.enable_flush, True,
                                             admin_tools_package=self.admin_tools_package)

        self.log.info("Enable flush and flush bucket")
        bucket_edit_roles = ["admin", "cluster_admin", "bucket_admin[*]"]
        cli = CouchbaseCLI(server, user, password)
        output, _, _ = cli.bucket_edit("bucket1", self.bucket_ram,
                                                  self.eviction_policy, 1,
                                                  self.priority, 1,
                                                  admin_tools_package=self.admin_tools_package)
        if "SUCCESS: Bucket edited" not in output:
            if roles in bucket_edit_roles:
                self.fail("Failed to edit bucket with roles {0} ".format(roles))
            else:
                self.log.info("{0} has no permision to edit bucket".format(roles))
                print("\nEnable flush with admin roles for next test\n")
                cli = CouchbaseCLI(server, "Administrator", password)
                cli.bucket_edit("bucket1", self.bucket_ram,
                                self.eviction_policy, 1,
                                self.priority, 1,
                                admin_tools_package=self.admin_tools_package)

        self.log.info("Load data to bucket")
        shell = RemoteMachineShellConnection(server)
        cmd = "{0}cbworkloadgen{1} -n {2}:8091 -j -u {3} -p {4} -b bucket1 " \
                                        .format(self.cli_command_path, self.cmd_ext,
                                                server.ip, "Administrator", "password")
        shell.execute_command(cmd)
        shell.disconnect()
        cli = CouchbaseCLI(server, user, password)
        _, _, flushed = cli.bucket_flush("bucket1", True,
                                         admin_tools_package=self.admin_tools_package)
        rest = RestConnection(server)
        count = 0
        bucket_items = rest.get_active_key_count("bucket1")
        while int(bucket_items) != 0 and count < 3:
            self.sleep(2, "**** wait for items in bucket clear")
            bucket_items = rest.get_active_key_count("bucket1")
            count += 1
            if count == 3 and int(bucket_items) != 0:
                self.log.error("**** items in bucket does not clear")
        if int(bucket_items) != 0 :
            if roles in bucket_edit_roles:
                self.fail("Failed to flush bucket with roles {0}".format(roles))
            else:
                self.log.info("{0} has no permision to flush bucket".format(roles))
        else:
            self.log.info("Roles {0} success flush bucket ".format(roles))

        try:
            self.log.info("Delete bucket 'bucket1")
            cli = CouchbaseCLI(server, user, password)
            cli.bucket_delete("bucket1", admin_tools_package=self.admin_tools_package)
            self.buckets = RestConnection(server).get_buckets()
            for bucket in self.buckets:
                if bucket.name == "bucket1":
                    if roles in bucket_create_roles:
                        self.fail("Roles {0} failed to delete bucket.".format(roles))
                    else:
                        self.log.info("{0} has no permision to delete bucket".format(roles))
        except Exception as e:
            if e:
                print("\n{0}\n".format(str(e)))


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
        self.log.info("===== Setup destination cluster =====")
        if self.dest_nodes and len(self.dest_nodes) > 1:
            self.cluster.async_rebalance(self.dest_nodes, self.dest_nodes[1:],
                                                                 []).result()

        XdcrCLITest.XDCR_SETUP_SUCCESS = {
                      "create": "SUCCESS: Cluster reference created",
                      "edit": "SUCCESS: Cluster reference edited",
                      "delete": "SUCCESS: Cluster reference deleted"
        }
        XdcrCLITest.XDCR_REPLICATE_SUCCESS = {
                      "create": "SUCCESS: XDCR replication created",
                      "delete": "SUCCESS: XDCR replication deleted",
                      "pause": "SUCCESS: XDCR replication paused",
                      "resume": "SUCCESS: XDCR replication resume"
        }
        XdcrCLITest.SSL_MANAGE_SUCCESS = \
                      {'retrieve': "SUCCESS: retrieve certificate to \'PATH\'",
                       'regenerate': 'SUCCESS: Certificate regenerate and copied to `PATH`'
                      }

    def tearDown(self):
        for server in self.servers:
            rest = RestConnection(server)
            rest.remove_all_replications()
            rest.remove_all_remote_clusters()
            rest.remove_all_recoveries()
        super(XdcrCLITest, self).tearDown()

    def __execute_cli(self, cli_command, options, cluster_host="localhost"):
        return self.shell.execute_couchbase_cli(
                                                cli_command=cli_command,
                                                options=options,
                                                cluster_host=cluster_host,
                                                user=self.__user,
                                                password=self.__password,
                                                admin_tools_package=self.admin_tools_package)

    def __xdcr_setup_create(self):
        # xdcr_hostname=the number of server in ini file to add to master as replication
        xdcr_cluster_name = self.input.param("xdcr-cluster-name", None)
        xdcr_hostname = self.input.param("xdcr-hostname", None)
        xdcr_username = self.input.param("xdcr-username", None)
        xdcr_password = self.input.param("xdcr-password", None)
        secure_connection = self.input.param("secure-connection", "none")
        xdcr_cert = self.input.param("xdcr-certificate", None)
        wrong_cert = self.input.param("wrong-certificate", None)

        cli_command = "xdcr-setup"
        options = "--create"
        options += (" --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name),\
                                                "")[xdcr_cluster_name is None]
        if xdcr_hostname is not None:
            options += " --xdcr-hostname={0}".format(self.dest_nodes[0].ip)
        options += (" --xdcr-username={0}".format(xdcr_username), "")[xdcr_username is None]
        options += (" --xdcr-password={0}".format(xdcr_password), "")[xdcr_password is None]
        options += (" --xdcr-secure-connection={0}".format(secure_connection))

        if secure_connection == 'full' and xdcr_hostname is not None and xdcr_cert:
            if wrong_cert:
                cluster_host = "localhost"
            else:
                cluster_host = self.dest_master.ip
            cert_info = " --regenerate-cert cert.pem"
            output, _ = self.__execute_cli(cli_command="ssl-manage", options="{0} "\
                                     .format(cert_info), cluster_host=cluster_host)
            self.shell.copy_file_local_to_remote("cert.pem",
                                            self.root_path + "cert.pem")
            os.system("rm -f cert.pem")
            options += (" --xdcr-certificate={0}".format(xdcr_cert),\
                                               "")[xdcr_cert is None]
            if "enterprise" in self.cb_version:
                msgs_check = ["-----END CERTIFICATE-----", "Certificate regenerate and copied"]
                self.assertTrue(self._check_output(msgs_check, output))
            else:
                self.assertTrue(self._check_output("ERROR: This http API"\
                            " endpoint requires enterprise edition", output))

        output, error = self.__execute_cli(cli_command=cli_command, options=options)
        return output, error, xdcr_cluster_name, xdcr_hostname, cli_command, options

    def __verify_bucket_config(self, server, bucket_name, bucket_type,
                               memory_quota, eviction_policy, replica_count,
                               enable_index_replica, priority, enable_flush,
                               stdout, expect_error):
        self.assertTrue(self.verifyCommandOutput(stdout, expect_error,
                                                 "Bucket created"),
                                                 "Expected command to succeed")
        self.assertTrue(self.verifyBucketSettings(server, bucket_name,
                                                bucket_type, memory_quota,
                                                eviction_policy, replica_count,
                                                enable_index_replica, priority,
                                                enable_flush),
                                                "Bucket settings not set properly")
    def test_xdcr_recreate_bucket(self):
        """
            Setup an XDCR cluster.
            Create bucket at both remote and source
            Delete bucket at both remote and source
            Recreate bucket at both remote and source
        :return:
        """
        username = self.input.param("username", None)
        password = self.input.param("password", None)
        bucket_name = self.input.param("bucket-name", None)
        bucket_type = self.input.param("bucket-type", None)
        memory_quota = self.input.param("memory-quota", None)
        eviction_policy = self.input.param("eviction-policy", None)
        replica_count = self.input.param("replica-count", None)
        enable_index_replica = self.input.param("enable-replica-index", None)
        priority = self.input.param("priority", None)
        enable_flush = self.input.param("enable-flush", None)
        wait = self.input.param("wait", False)
        expect_error = self.input.param("expect-error")
        error_msg = self.input.param("error-msg", "")

        self.__xdcr_setup_create()
        server_s = copy.deepcopy(self.servers[0])

        cli_s = CouchbaseCLI(server_s, username, password)
        stdout_s, _, _ = cli_s.bucket_create(bucket_name, bucket_type, memory_quota,
                                         eviction_policy, replica_count,
                                         enable_index_replica, priority,
                                         enable_flush, wait,
                                         admin_tools_package=self.admin_tools_package)
        self.__verify_bucket_config(server_s, bucket_name, bucket_type,
                                    memory_quota, eviction_policy, replica_count,
                                    enable_index_replica, priority, enable_flush,
                                    stdout_s, expect_error)

        cli_s.bucket_delete(bucket_name, admin_tools_package=self.admin_tools_package)
        stdout_s, _, _ = cli_s.bucket_create(bucket_name, bucket_type, memory_quota,
                                             eviction_policy, replica_count,
                                             enable_index_replica, priority,
                                             enable_flush, wait,
                                             admin_tools_package=self.admin_tools_package)
        self.__verify_bucket_config(server_s, bucket_name, bucket_type,
                                    memory_quota, eviction_policy, replica_count,
                                    enable_index_replica, priority, enable_flush,
                                    stdout_s, expect_error)

        server_r = copy.deepcopy(self.dest_nodes[0])
        cli_r = CouchbaseCLI(server_r, username, password)
        stdout_r, _, _ = cli_r.bucket_create(bucket_name, bucket_type, memory_quota,
                                         eviction_policy, replica_count,
                                         enable_index_replica, priority,
                                         enable_flush, wait,
                                         admin_tools_package=self.admin_tools_package)
        self.__verify_bucket_config(server_r, bucket_name, bucket_type,
                                   memory_quota, eviction_policy, replica_count,
                                   enable_index_replica, priority, enable_flush,
                                   stdout_r, expect_error)

        cli_r.bucket_delete(bucket_name, admin_tools_package=self.admin_tools_package)
        stdout_r, _, _ = cli_r.bucket_create(bucket_name, bucket_type, memory_quota,
                                             eviction_policy, replica_count,
                                             enable_index_replica, priority,
                                             enable_flush, wait,
                                             admin_tools_package=self.admin_tools_package)
        self.__verify_bucket_config(server_r, bucket_name, bucket_type,
                                   memory_quota, eviction_policy, replica_count,
                                   enable_index_replica, priority, enable_flush,
                                   stdout_r, expect_error)

    def testXDCRSetup(self):
        error_expected_in_command = self.input.param("error-expected", None)
        output, _, xdcr_cluster_name, xdcr_hostname, cli_command, options = \
                                                         self.__xdcr_setup_create()
        if error_expected_in_command != "create":
            self.assertIn(XdcrCLITest.XDCR_SETUP_SUCCESS["create"].replace("CLUSTERNAME",\
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
                if self._check_output("unable to set up xdcr remote site remote (400) Bad Request",
                        output):
                    self.log.info("match {0}".format(element))
                    return True
                elif "Error: hostname (ip) is missing" in element:
                    self.log.info("match {0}".format(element))
                    return True
                else:
                    if "Error checking if target cluster supports SANs in cerificates." \
                            in element:
                        self.log.info("match {0}".format(element))
                        return True
                    elif "ERROR: --xdcr-hostname is required to create a cluster connections" \
                            in element:
                        self.log.info("match {0}".format(element))
                        return True
                    elif self._check_output("Authentication failed. Verify username and password.",
                            output):
                        self.log.info("match {0}".format(element))
                        return True
                    elif "Certificate must be a single, PEM-encoded x509 certificate" \
                            in element:
                        self.log.info("match {0}".format(element))
                        return True
                    elif self._check_output("Invalid remote cluster.", output):
                        self.log.info("match {0}".format(element))
                        return True
            self.assertFalse("output string did not match")

        # MB-8570 can't edit xdcr-setup through couchbase-cli
        if xdcr_cluster_name:
            options = options.replace("--create ", "--edit ")
            output, _ = self.__execute_cli(cli_command=cli_command, options=options)
            self.assertIn(XdcrCLITest.XDCR_SETUP_SUCCESS["edit"]\
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
            self.assertIn(XdcrCLITest.XDCR_SETUP_SUCCESS["delete"]\
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
                expect_error = ("['ERROR: unknown remote cluster : refName - unknown']")
                if output_error == expect_error:
                    output_error = "ERROR: unknown remote cluster : refName - unknown"
                self.assertTrue(self._check_output(output_error, output))
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
        if filter_expression:
            filter_expression = "REGEXP_CONTAINS(META().id, " + "\"" + filter_expression + "\")"
        timeout_perc_cap = self.input.param("timeout_perc_cap", None)
        _, _, xdcr_cluster_name, xdcr_hostname, _, _ = self.__xdcr_setup_create()
        cli_command = "xdcr-replicate"
        options = "--create"
        options += (" --xdcr-cluster-name=\'{0}\'".format(xdcr_cluster_name), "")[xdcr_cluster_name is None]
        options += (" --xdcr-from-bucket=\'{0}\'".format(from_bucket), "")[from_bucket is None]
        options += (" --xdcr-to-bucket=\'{0}\'".format(to_bucket), "")[to_bucket is None]
        options += (" --source-nozzle-per-node=\'{0}\'".format(source_nozzles), "")[source_nozzles is None]
        options += (" --target-nozzle-per-node=\'{0}\'".format(target_nozzles), "")[target_nozzles is None]
        options += (" --filter-expression=\'{0}\'".format(filter_expression), "")[filter_expression is None]
        options += (" --checkpoint-interval=\'{0}\'".format(checkpoint_interval), "")[timeout_perc_cap is None]

        # Add built-in user to dest_nodes
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.dest_nodes[0])


        # Assign user to role
        role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(self.dest_nodes[0]), 'builtin')


        self.bucket_size = self._get_bucket_size(self.quota, 1)
        if from_bucket:
            bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                              replicas=self.num_replicas,
                                                              enable_replica_index=self.enable_replica_index)
            self.cluster.create_default_bucket(bucket_params)
        if to_bucket:
            bucket_params = self._create_bucket_params(server=self.dest_nodes[0], size=self.bucket_size,
                                                              replicas=self.num_replicas,
                                                              enable_replica_index=self.enable_replica_index)
            self.cluster.create_default_bucket(bucket_params)

        self.sleep(10)
        output, _ = self.__execute_cli(cli_command, options)
        if not error_expected:
            self.assertIn(XdcrCLITest.XDCR_REPLICATE_SUCCESS["create"], output[0])
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
                    self.assertIn(XdcrCLITest.XDCR_REPLICATE_SUCCESS["pause"], output[0])
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
                    self.assertIn(XdcrCLITest.XDCR_REPLICATE_SUCCESS["resume"], output[0])
                    # check if status of replication is "running"
                    options = "--list"
                    output, _ = self.__execute_cli(cli_command, options)
                    for value in output:
                        if value.startswith("status"):
                            self.assertEqual(value.split(":")[1].strip(), "running")
                options = "--delete"
                options += (" --xdcr-replicator={0}".format(replicator))
                output, _ = self.__execute_cli(cli_command, options)
                self.assertIn(XdcrCLITest.XDCR_REPLICATE_SUCCESS["delete"], output[0])

        # Remove rbac users in dest_nodes
        role_del = ['cbadminbucket']
        RbacBase().remove_user_role(role_del, RestConnection(self.dest_nodes[0]))

    def testSSLManage(self):
        '''ssl-manage OPTIONS:
        --retrieve-cert=CERTIFICATE    retrieve cluster certificate AND save to a pem file
        --regenerate-cert=CERTIFICATE  regenerate cluster certificate AND save to a pem file'''
        if self.input.param("xdcr-certificate", None) is not None:
            xdcr_cert = self.input.param("xdcr-certificate", None)
        else:
            self.fail("need params xdcr-certificate to run")
        cli_command = "ssl-manage"
        cert_info = " --regenerate-cert cert.pem "
        output, error = self.__execute_cli(cli_command=cli_command,
                                                       options=cert_info)
        self.shell.copy_file_local_to_remote(xdcr_cert,
                                        self.root_path + xdcr_cert)
        os.system("rm -f %s " % xdcr_cert)
        self.assertFalse(error, "Error thrown during CLI execution %s" % error)
        msgs_check = ["-----END CERTIFICATE-----", "Certificate regenerate and copied"]
        self.assertTrue(self._check_output(msgs_check, output))
        self.shell.execute_command("rm {0}".format(xdcr_cert))

        options = "--regenerate-cert={0}".format(xdcr_cert)
        output, error = self.__execute_cli(cli_command=cli_command, options=options)
        self.assertFalse(error, "Error thrown during CLI execution %s" % error)
        self.assertIn(XdcrCLITest.SSL_MANAGE_SUCCESS["regenerate"]\
                                        .replace("PATH", xdcr_cert), output[0])
        self.shell.execute_command("rm {0}".format(xdcr_cert))

    def _check_output(self, word_check, output):
        found = False
        if len(output) >= 1:
            if isinstance(word_check, list):
                for ele in word_check:
                    for x in output:
                        if ele.lower() in str(x.lower()):
                            self.log.info("Found '{0} in CLI output".format(ele))
                            found = True
                            break
            elif isinstance(word_check, str):
                for x in output:
                    if word_check.lower() in str(x.lower()):
                        self.log.info("Found '{0}' in CLI output".format(word_check))
                        found = True
                        break
            else:
                self.log.error("invalid {0}".format(word_check))
        return found

    def _convert_server_to_url(self, server):
        """From 6.5.x, add, remove, failover... need to prefix server with
           http/https and postfix with 8091/18091
           From 7.1, only allow secure connection to add node """
        url_server = "https://{0}:1{1}".format(server.ip, server.port)
        return url_server
