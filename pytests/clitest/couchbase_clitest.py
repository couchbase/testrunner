import json
import logger

from threading import Thread

from membase.api.rest_client import RestConnection
from TestInput import TestInputSingleton
from clitest.cli_base import CliBaseTest
from remote.remote_util import RemoteMachineShellConnection
from pprint import pprint

help = {'CLUSTER': '--cluster=HOST[:PORT] or -c HOST[:PORT]',
 'COMMAND': {'bucket-create': 'add a new bucket to the cluster',
             'bucket-delete': 'delete an existing bucket',
             'bucket-edit': 'modify an existing bucket',
             'bucket-flush': 'flush a given bucket',
             'bucket-list': 'list all buckets in a cluster',
             'cluster-init': 'set the username,password and port of the cluster',
             'failover': '',
             'help': 'show longer usage/',
             'node-init': 'set node specific parameters',
             'rebalance': 'start a cluster rebalancing',
             'rebalance-status': 'show status of current cluster rebalancing',
             'rebalance-stop': 'stop current cluster rebalancing',
             'server-add': 'add one or more servers to the cluster',
             'server-info': 'show details on one server',
             'server-list': 'list all servers in a cluster',
             'server-readd': 'readd a server that was failed over'},
 'EXAMPLES': {'Add a node to a cluster and rebalance': 'couchbase-cli rebalance -c 192.168.0.1:8091 --server-add=192.168.0.2:8091 --server-add-username=Administrator --server-add-password=password',
              'Add a node to a cluster, but do not rebalance': 'couchbase-cli server-add -c 192.168.0.1:8091 --server-add=192.168.0.2:8091 --server-add-username=Administrator --server-add-password=password',
              'Change the data path': 'couchbase-cli node-init -c 192.168.0.1:8091 --node-init-data-path=/tmp',
              'Change the username, password, port and ram quota': 'couchbase-cli cluster-init -c 192.168.0.1:8091 --cluster-init-username=Administrator --cluster-init-password=password --cluster-init-port=8080 --cluster-init-ramsize=300',
              'Create a couchbase bucket and wait for bucket ready': 'couchbase-cli bucket-create -c 192.168.0.1:8091 --bucket=test_bucket --bucket-type=couchbase --bucket-port=11222 --bucket-ramsize=200 --bucket-replica=1 --wait',
              'Create a new dedicated port couchbase bucket': 'couchbase-cli bucket-create -c 192.168.0.1:8091 --bucket=test_bucket --bucket-type=couchbase --bucket-port=11222 --bucket-ramsize=200 --bucket-replica=1',
              'Create a new sasl memcached bucket': 'couchbase-cli bucket-create -c 192.168.0.1:8091 --bucket=test_bucket --bucket-type=memcached --bucket-password=password --bucket-ramsize=200',
              'Delete a bucket': 'couchbase-cli bucket-delete -c 192.168.0.1:8091 --bucket=test_bucket',
              'List buckets in a cluster': 'couchbase-cli bucket-list -c 192.168.0.1:8091',
              'List servers in a cluster': 'couchbase-cli server-list -c 192.168.0.1:8091',
              'Modify a dedicated port bucket': 'couchbase-cli bucket-edit -c 192.168.0.1:8091 --bucket=test_bucket --bucket-port=11222 --bucket-ramsize=400',
              'Remove a node from a cluster and rebalance': 'couchbase-cli rebalance -c 192.168.0.1:8091 --server-remove=192.168.0.2:8091',
              'Remove and add nodes from/to a cluster and rebalance': 'couchbase-cli rebalance -c 192.168.0.1:8091 --server-remove=192.168.0.2 --server-add=192.168.0.4 --server-add-username=Administrator --server-add-password=password',
              'Server information': 'couchbase-cli server-info -c 192.168.0.1:8091',
              'Stop the current rebalancing': 'couchbase-cli rebalance-stop -c 192.168.0.1:8091'},
 'OPTIONS': {'-o KIND, --output=KIND': 'KIND is json or standard\n-d, --debug',
             '-p PASSWORD, --password=PASSWORD': 'admin password of the cluster',
             '-u USERNAME, --user=USERNAME': 'admin username of the cluster'},
 'bucket-* OPTIONS': {'--bucket-password=PASSWORD': 'standard port, exclusive with bucket-port',
                      '--bucket-port=PORT': 'supports ASCII protocol and is auth-less',
                      '--bucket-ramsize=RAMSIZEMB': 'ram quota in MB',
                      '--bucket-replica=COUNT': 'replication count',
                      '--bucket-type=TYPE': 'memcached or couchbase',
                      '--bucket=BUCKETNAME': ' bucket to act on',
                      '--wait': 'wait for bucket create to be complete before returning'},
 'cluster-init OPTIONS': {'--cluster-init-password=PASSWORD': 'new admin password',
                          '--cluster-init-port=PORT': 'new cluster REST/http port',
                          '--cluster-init-ramsize=RAMSIZEMB': 'per node ram quota in MB',
                          '--cluster-init-username=USER': 'new admin username'},
 'description': 'couchbase-cli - command-line cluster administration tool',
 'failover OPTIONS': {'--server-failover=HOST[:PORT]': ' server to failover'},
 'node-init OPTIONS': {'--node-init-data-path=PATH': 'per node path to store data'},
 'rebalance OPTIONS': {'--server-add*': ' see server-add OPTIONS',
                       '--server-remove=HOST[:PORT]': ' the server to be removed'},
 'server-add OPTIONS': {'--server-add-password=PASSWORD': 'admin password for the\nserver to be added',
                        '--server-add-username=USERNAME': 'admin username for the\nserver to be added',
                        '--server-add=HOST[:PORT]': 'server to be added'},
 'server-readd OPTIONS': {'--server-add-password=PASSWORD': 'admin password for the\nserver to be added',
                          '--server-add-username=USERNAME': 'admin username for the\nserver to be added',
                          '--server-add=HOST[:PORT]': 'server to be added'},
 'usage': ' couchbase-cli COMMAND CLUSTER [OPTIONS]'}


help_short = {'COMMANDs include': {'bucket-compact': 'compact database and index data',
                      'bucket-create': 'add a new bucket to the cluster',
                      'bucket-delete': 'delete an existing bucket',
                      'bucket-edit': 'modify an existing bucket',
                      'bucket-flush': 'flush all data from disk for a given bucket',
                      'bucket-list': 'list all buckets in a cluster',
                      'cluster-edit': 'modify cluster settings',
                      'cluster-init': 'set the username,password and port of the cluster',
                      'failover': '',
                      'help': 'show longer usage/',
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
            #line = ""
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
                    #result[upper_key][previous_key] = ""
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

    def _get_cluster_info(self, remote_client, cluster_host="localhost", user="Administrator", password="password"):
        command = "server-info"
        output, error = remote_client.execute_couchbase_cli(command, cluster_host=cluster_host, user=user, password=password)
        if not error:
            content = ""
            for line in output:
                content += line
            return json.loads(content)
        else:
            self.fail("server-info return error output")

    def _create_bucket(self, remote_client, bucket="default", bucket_type="couchbase", bucket_port=11211, bucket_ramsize=200, bucket_replica=1, bucket_wait=False):
        options = "--bucket={0} --bucket-type={1} --bucket-port={2} --bucket-ramsize={3} --bucket-replica={4}".\
            format(bucket, bucket_type, bucket_port, bucket_ramsize, bucket_replica)
        cli_command = "bucket-create"

        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output[0], "SUCCESS: bucket-create")


    def testHelp(self):
        remote_client = RemoteMachineShellConnection(self.master)
        options = self.input.param('options', "")
        output, error = remote_client.execute_couchbase_cli(cli_command="", cluster_host=None, user=None, password=None, options=options)
        result = self._get_dict_from_output(output)
        expected_result = help_short
        if "-h" in options:
            expected_result = help_short
        pprint(result)
        if result == expected_result:
            self.log.info("Correct help info was found")
        else:
            self.log.fail(set(result.keys()) - set(help.keys()))
        remote_client.disconnect()

    def testInfoCommands(self):
        remote_client = RemoteMachineShellConnection(self.master)

        cli_command = "server-list"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        server_info = self._get_cluster_info(remote_client)
        result = server_info["otpNode"] + " " + server_info["hostname"] + " " + server_info["status"] + " " + server_info["clusterMembership"]
        self.assertEqual(result, "ns_1@{0} {0}:8091 healthy active".format(self.master.ip))

        cli_command = "bucket-list"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual([], output)
        remote_client.disconnect()

    def testAddRemoveNodes(self):
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        nodes_failover = self.input.param("nodes_failover", 0)
        nodes_readd = self.input.param("nodes_readd", 0)
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "server-add"
        for num in xrange(nodes_add):
            options = "--server-add={0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[num + 1].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["SUCCESS: server-add {0}:8091".format(self.servers[num + 1].ip)])

        cli_command = "rebalance"
        for num in xrange(nodes_rem):
            options = "--server-remove={0}:8091".format(self.servers[nodes_add - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["INFO: rebalancing . ", "SUCCESS: rebalanced cluster"])

        if nodes_rem == 0 and nodes_add > 0:
            cli_command = "rebalance"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["INFO: rebalancing . ", "SUCCESS: rebalanced cluster"])


        cli_command = "failover"
        for num in xrange(nodes_failover):
            options = "--server-failover={0}:8091".format(self.servers[nodes_add - nodes_rem - num].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["SUCCESS: failover ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num].ip)])

        cli_command = "server-readd"
        for num in xrange(nodes_readd):
            options = "--server-add={0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[nodes_add - nodes_rem - num ].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["SUCCESS: re-add ns_1@{0}".format(self.servers[nodes_add - nodes_rem - num ].ip)])

        cli_command = "rebalance"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ["INFO: rebalancing . ", "SUCCESS: rebalanced cluster"])
        remote_client.disconnect()

    def testStartStopRebalance(self):
        nodes_add = self.input.param("nodes_add", 1)
        nodes_rem = self.input.param("nodes_rem", 1)
        remote_client = RemoteMachineShellConnection(self.master)

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ["(u'none', None)"])

        cli_command = "server-add"
        for num in xrange(nodes_add):
            options = "--server-add={0}:8091 --server-add-username=Administrator --server-add-password=password".format(self.servers[num + 1].ip)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["SUCCESS: server-add {0}:8091".format(self.servers[num + 1].ip)])

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        self.assertEqual(output, ["(u'none', None)"])

        self._create_bucket(remote_client)

        cli_command = "rebalance"
        t = Thread(target=remote_client.execute_couchbase_cli, name="rebalance_after_add",
                       args=(cli_command, "localhost", '', None, "Administrator", "password"))
        t.start()
        self.sleep(10)

        cli_command = "rebalance-status"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
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
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["(u'running', None)"])


            cli_command = "rebalance-stop"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")


            t.join()

            cli_command = "rebalance"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output[1], "SUCCESS: rebalanced cluster")


            cli_command = "rebalance-status"
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
            self.assertEqual(output, ["(u'none', None)"])
        remote_client.disconnect()


    def bucketCreation(self):
        remote_client = RemoteMachineShellConnection(self.master)
        self._create_bucket(remote_client)
        remote_client.disconnect()


    def clusterEdit(self):
        remote_client = RemoteMachineShellConnection(self.master)
        self._create_bucket(remote_client)
        rest = RestConnection(server)
        rest.force_eject_node()
        server = self.servers[-1]
        remote_client.disconnect()

    def clusterInit(self):
        cluster_init_username = self.input.param("cluster_init_username", "Administrator")
        cluster_init_password = self.input.param("cluster_init_password", "password")
        cluster_init_port = self.input.param("cluster_init_port", 8091)
        cluster_init_ramsize = self.input.param("cluster_init_ramsize", 300)

        remote_client = RemoteMachineShellConnection(self.master)
        server = self.servers[-1]
        rest = RestConnection(server)
        rest.force_eject_node()

        cli_command = "cluster-init"
        options = "--cluster-init-username={0} --cluster-init-password={1} --cluster-init-port={2} --cluster-init-ramsize={3}".\
                format(cluster_init_username, cluster_init_password, cluster_init_port, cluster_init_ramsize)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user=None, password=None)
        self.assertEqual(output[0], "SUCCESS: init localhost")

        options = "--cluster-init-username={0} --cluster-init-password={1}".\
                format(cluster_init_username + "1", cluster_init_password + "1", cluster_init_port + "1")
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user=cluster_init_username, password=cluster_init_password)
        self.assertEqual(output[0], "SUCCESS: init localhost")

        cli_command = "server-list"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, cluster_host="localhost", user="Administrator", password="password")
        server_info = self._get_cluster_info(remote_client)
        result = server_info["otpNode"] + " " + server_info["hostname"] + " " + server_info["status"] + " " + server_info["clusterMembership"]
        self.assertEqual(result, "ns_1@{0} {0}:8091 healthy active".format(self.master.ip))


        options = "--cluster-init-username={0} --cluster-init-password={1}".\
                format(cluster_init_username, cluster_init_password, cluster_init_port)
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost", user=(cluster_init_username + "1"), password=cluster_init_password + "1")
        self.assertEqual(output[0], "SUCCESS: init localhost")



        self._create_bucket(remote_client)
        remote_client.disconnect()

