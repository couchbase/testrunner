import os
import re
import math
import time
import json
import shlex
import random
import datetime
import abc

from TestInput import TestInputSingleton
from lib.couchbase_helper.time_helper import TimeUtil
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase
from membase.api.rest_client import RestConnection
from membase.helper.rebalance_helper import RebalanceHelper
from lib.backup_service_client.models.task_template import TaskTemplate
from lib.backup_service_client.models.task_template_schedule import TaskTemplateSchedule
from lib.backup_service_client.models.task_template_options import TaskTemplateOptions
from lib.backup_service_client.configuration import Configuration
from lib.backup_service_client.api_client import ApiClient
from lib.backup_service_client.api.plan_api import PlanApi
from lib.backup_service_client.api.import_api import ImportApi
from lib.backup_service_client.api.repository_api import RepositoryApi
from lib.backup_service_client.api.configuration_api import ConfigurationApi
from lib.backup_service_client.api.active_repository_api import ActiveRepositoryApi
from lib.backup_service_client.models.body1 import Body1
from lib.backup_service_client.models.body2 import Body2
from lib.backup_service_client.models.body3 import Body3
from lib.backup_service_client.models.body4 import Body4
from lib.backup_service_client.models.body5 import Body5
from lib.backup_service_client.models.plan import Plan
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from lib.membase.helper.bucket_helper import BucketOperationHelper
from couchbase_helper.cluster import Cluster

class BackupServiceBase(EnterpriseBackupRestoreBase):

    def preamble(self):
        """ Preamble.

        1. Configures Rest API.
        2. Configures Rest API Sub-APIs.
        3. Backup Service Constants.
        """
        # Set 'skip_server_sort' to avoid sorting the first cluster in the ini file by total memory
        if not self.input.param("skip_server_sort", False):
            # Sort the available clusters by total memory in ascending order (if the total memory is the same, sort by ip)
            self.input.clusters[0].sort(key = lambda server: (ServerUtil(server).get_free_memory(), server.ip))

            # Configure the master server based on the sorted list of clusters
            self.master = next((server for server in self.servers if server.ip == self.input.clusters[0][0].ip))

        # Node to node encryption
        self.node_to_node_encryption = NodeToNodeEncryption(self.input.clusters[0][0])

        self.use_https = self.input.param('use_https', False)

        self.ssl_hints = {}

        # Define where the root certificate file is saved on the filesystem
        self.ssl_hints['root_certificate_file'] = '/tmp/root_certificate'

        # Define where the client private key and certificate are saved on the filesystem
        self.ssl_hints['client_private_key_file'] = '/tmp/client_private_key'
        self.ssl_hints['client_certificate_file'] = '/tmp/client_certificate'

        # Create/Enable/Upload custom cluster certificates and client certificates
        if self.input.param('custom_certificate', False):
            self.load_custom_certificates()

        # Select a configuration factory which creates Configuration objects that communicate over http or https
        self.configuration_factory = HttpsConfigurationFactory(self.master, hints=self.ssl_hints) if self.use_https else HttpConfigurationFactory(self.master)

        # Rest API Configuration
        self.configuration = self.configuration_factory.create_configuration()

        # Create Api Client
        self.api_client = ApiClient(self.configuration)

        # Rest API Sub-APIs
        self.plan_api = PlanApi(self.api_client)
        self.import_api = ImportApi(self.api_client)
        self.repository_api = RepositoryApi(self.api_client)
        self.configuration_api = ConfigurationApi(self.api_client)
        self.active_repository_api = ActiveRepositoryApi(self.api_client)

        # Backup Service Constants
        self.default_plans = ["_hourly_backups", "_daily_backups"]
        self.periods = ["MINUTES", "HOURS", "DAYS", "WEEKS", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

        # A connection to every single machine in the cluster
        self.multiple_remote_shell_connections = MultipleRemoteShellConnections(self.input.clusters[0])

        # Manipulate time on all nodes
        self.time = Time(self.multiple_remote_shell_connections)

        # Manipulate time on individual nodes
        self.solo_time = [Time(RemoteMachineShellConnection(server)) for server in self.input.clusters[0]]

        # Log files
        self.log_directory = f"/opt/couchbase/var/lib/couchbase/logs/backup_service.log"

    def setUp(self):
        """ Sets up.

        1. Runs preamble.
        """
        self.input = TestInputSingleton.input

        # Set 'default_setup' to avoid the custom_rebalance and skipping of the cleanup
        if not self.input.param("default_setup", False):
            # Set the custom_rebalance flag for every backup service test
            # in order for the basetest case to call the custom_rebalance
            # procedure.
            self.input.test_params["custom_rebalance"] = True

            # Skip the more general clean up process that test runner does at
            # the end of each which includes tearing down the cluster by
            # ejecting nodes, rebalancing and using diag eval to 'reset'
            # the cluster.
            self.input.test_params["skip_cleanup"] = True

        super().setUp()
        self.preamble()

        # Specifify this is a backup service test
        self.backupset.backup_service = True

        # Run cbbackupmgr on the same node as the backup service being tested
        self.backupset.backup_host = self.backupset.cluster_host

        # Stop the vboxadd-service again to prevent it from syncing the time with the host
        self.multiple_remote_shell_connections.execute_command("sudo service vboxadd-service stop")

        # Share archive directory
        self.directory_to_share = "/tmp/share"
        self.nfs_connection = NfsConnection(self.input.clusters[0][0], self.input.clusters[0], NfsShareFactory() if self.input.param("share_type", "nfs") == "nfs" else SambaShareFactory())
        self.nfs_connection.share(self.directory_to_share, self.backupset.directory)

        # A connection to every single machine in the cluster
        self.multiple_remote_shell_connections = MultipleRemoteShellConnections(self.input.clusters[0])

        if self.objstore_provider:
            self.create_staging_directory()

        # The service and cbbackupmgr should not share staging directories
        self.backupset.objstore_alternative_staging_directory = "/tmp/altstaging"

        # List of temporary directories
        self.temporary_directories = []
        self.temporary_directories.append(self.backupset.objstore_alternative_staging_directory)
        self.temporary_directories.append(self.directory_to_share)
        self.temporary_directories.append(self.backupset.directory)

        if self.input.param('node_to_node_encryption', False):
            self.assertTrue(self.node_to_node_encryption.enable())

        if self.input.param('dgm_run', False):
            self.on_dgm_run()

    def load_custom_certificates(self):
        """ Loads custom certificates

        Generates and uploads a cluster wide, per-node and client private keys and certificates.
        """
        # Generates a key/cert pair for each node and a cluster-wide root key/cert pair
        self.crypto_tester = CryptoTester(self.input.clusters[0])

        self.assertTrue(self.input.param("use_https", False), "The `use_https` param must be set to True")

        # Provisions each node with a key/cert pair and uploads the root certificate to each node in the cluster.
        self.crypto_tester.upload()

        # Write the root certificate to a file
        with open(self.ssl_hints['root_certificate_file'], "w") as f:
            f.write(self.crypto_tester.get_root_certificate())

        self.client_certificate_tester = ClientCertificateTester(self.input.clusters[0], ClientStrategy("san.email", "", "", self.master.rest_username), self.crypto_tester)

        # Enable client certificate authentication
        self.client_certificate_tester.client_auth_enable()

        # Generate a client private key and a certificate which has been signed by the root certificate.
        client_private_key, client_certificate = self.client_certificate_tester.create_client_pair()

        # Write the client private key to a file
        with open(self.ssl_hints['client_private_key_file'], "w") as f:
            f.write(client_private_key)

        # Write the client certificate to a file
        with open(self.ssl_hints['client_certificate_file'], "w") as f:
            f.write(client_certificate)

    def custom_rebalance(self):
        """Form cluster[0] into a cluster"""
        # Skip testrunner's rebalance procedure
        rebalance_required, rest_conn = False, RestConnection(self.input.clusters[0][0])

        # Fetch all the nodes
        nodes = rest_conn.get_nodes(get_all_nodes=True)
        # If any node has been failed-over, add back the node and rebalance
        for node in nodes:
            if node.has_failed_over:
                rest_conn.set_recovery_type(node.id, 'full')
                rest_conn.add_back_node(node.id)

        if any(node.has_failed_over for node in nodes):
            Cluster().rebalance(self.input.clusters[0], [], [], services=[server.services for server in self.servers])

        # If node 1 exists in isolation and nodes2 and node3 have already been added into their own cluster, eject node3 from node2
        if len(nodes) == 1 and len(self.input.clusters[0]) > 1 and len(RestConnection(self.input.clusters[0][1]).get_nodes(get_all_nodes=True)) > 1:
            # Break the second cluster apart
            Cluster().rebalance(self.input.clusters[0][1:], [], self.input.clusters[0][2:3], services=[server.services for server in self.servers])

        nodes = rest_conn.get_nodes(get_all_nodes=True)
        nodes_ips_and_ports = set((node.ip, node.port) for node in nodes)
        # If this is the first test or nodes are missing from cluster[0] then add missing nodes and rebalance cluster[0]
        if len(nodes) < len(self.input.clusters[0]):
            # Nodes which are currently not added
            servers_to_add = [server for server in self.input.clusters[0][1:] if (server.ip, server.port) not in nodes_ips_and_ports]
            # Provision node in cluster[0] into a cluster
            Cluster().rebalance(self.input.clusters[0], servers_to_add, [], services=[server.services for server in self.servers])

    def on_dgm_run(self, no_of_documents_above_threshold=100):
        """ Configure the number of items to enter the disk greater than memory state in at least 1 node

        Args:
            no_of_documents_above_threshold (int): The number of additional documents above the threshold
        """
        self.num_items = DGMUtil.calculate_dgm_threshold(self.quota, len(self.input.clusters[0]), self.value_size) + no_of_documents_above_threshold

    def mkdir(self, directory):
        """ Creates a directory
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        remote_client.execute_command(f"mkdir {directory}")
        remote_client.disconnect()

    def rmdir(self, directory):
        """ Deletes a directory
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        remote_client.execute_command(f"rm -rf {directory}")
        remote_client.disconnect()

    def chown(self, directory, user="couchbase", group="couchbase"):
        """ Recursively change file/folder ownership
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        remote_client.execute_command(f"chown -R {user}:{group} {directory}")
        remote_client.disconnect()

    def chmod(self, permissions, directory):
        """ Recursively change file/folder permissions
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        remote_client.execute_command(f"chmod -R {permissions} {directory}")
        remote_client.disconnect()

    def create_staging_directory(self):
        """ Create a staging directory on every single machine
        """
        self.multiple_remote_shell_connections.execute_command(f"mkdir {self.objstore_provider.staging_directory}")
        self.multiple_remote_shell_connections.execute_command(f"chown -R couchbase:couchbase {self.objstore_provider.staging_directory}")
        self.multiple_remote_shell_connections.execute_command(f"chmod -R 777 {self.objstore_provider.staging_directory}")

    def is_backup_service_running(self):
        """ Returns true if the backup service is running.
        """
        rest = RestConnection(self.master)
        return 'backupAPI' in json.loads(rest._http_request(rest.baseUrl + "pools/default/nodeServices")[1])['nodesExt'][0]['services'].keys()

    def uuid_to_server(self, uuid):
        """ Maps a uuid to server
        """
        rest = RestConnection(self.master)
        nodes = json.loads(rest._http_request(rest.baseUrl + "pools/nodes")[1])['nodes']
        node = next((node for node in nodes if uuid == node['nodeUUID']), None)
        if node:
            return next((server for server in self.input.clusters[0] if f"{server.ip}:{server.port}" == node['configuredHostname']), None)

    def delete_all_plans(self):
        """ Deletes all plans.

        Deletes all plans using the Rest API with the exceptions of the default plans.
        """
        for plan in self.plan_api.plan_get():
            if plan.name not in self.default_plans:
                self.plan_api.plan_name_delete(plan.name)

    def delete_all_repositories(self):
        """ Deletes all repositories.

        Pauses and Archives all repos and then deletes all the repos using the Rest API.
        """
        # Pause repositories
        for repo in self.repository_api.cluster_self_repository_state_get('active'):
            self.active_repository_api.cluster_self_repository_active_id_pause_post_with_http_info(repo.id)

        # Sleep to ensure repositories are paused
        self.sleep(5)

        # Delete all running tasks
        self.delete_all_running_tasks()

        # Archive repositories
        for repo in self.repository_api.cluster_self_repository_state_get('active'):
            self.active_repository_api.cluster_self_repository_active_id_archive_post_with_http_info(repo.id, body=Body3(id=repo.id))

        # Delete archived repositories
        for repo in self.repository_api.cluster_self_repository_state_get('archived'):
            self.repository_api.cluster_self_repository_state_id_delete_with_http_info('archived', repo.id)

        # delete imported repositories
        for repo in self.repository_api.cluster_self_repository_state_get('imported'):
            self.repository_api.cluster_self_repository_state_id_delete_with_http_info('imported', repo.id)

    def run_orphan_detection(self, server):
        """ Runs orphan detection on the server

        Runs the internal api's manual orphan detection which is only supported on the leader node.

        params:
            server (TestInputServer): The server to run the manual orphan detection on
        """
        self.assertEqual(server,self.get_leader())
        rest = RestConnection(server)
        status, content, header = rest._http_request(rest.baseUrl + f"_p/backup/internal/v1/orphanDetection", 'POST')

    def delete_task(self, state, repo_id, task_category, task_name):
        """ Delete a task
        """
        rest = RestConnection(self.master)
        assert(task_category in ['one-off', 'scheduled'])
        status, content, header = rest._http_request(rest.baseUrl + f"_p/backup/internal/v1/cluster/self/repository/{state}/{repo_id}/task/{task_category}/{task_name}", 'DELETE')

    def delete_all_running_tasks(self):
        """ Delete all one off and schedule tasks for every repository
        """
        for repo in self.repository_api.cluster_self_repository_state_get('active'):
            repository = self.repository_api.cluster_self_repository_state_id_get('active', repo.id)
            if repository.running_one_off:
                for key, task in repository.running_one_off.items():
                    self.delete_task('active', repo.id, 'one-off', task.task_name)
            if repository.running_tasks:
                for key, task in repository.running_tasks.items():
                    self.delete_task('active', repo.id, 'scheduled', task.task_name)

    def delete_temporary_directories(self):
        for directory in self.temporary_directories:
            self.multiple_remote_shell_connections.execute_command(f"rm -rf {directory}")

    def kill_process(self, process):
        """ Kill a process across all nodes
        """
        # Kill the backup service on all nodes in cluster
        self.multiple_remote_shell_connections.execute_command(f"kill -KILL `pgrep {process}`")

    def wait_for_process_to_start(self, process, retries, sleep_time):
        """ Wait for a process to start on all nodes

        params:
            process (str): The name of the process to kill and wait for.

        return:
            bool (true): If the process restarted on all nodes
        """
        for retries in range(0, retries):
            if all([output for output, error in self.multiple_remote_shell_connections.execute_command(f"pgrep {process}")]):
                return True

            self.sleep(sleep_time)

        return False

    def wait_until_http_requests_can_be_made(self, timeout=200):
        """ Waits until a http request can be made
        """
        timeout = time.time() + timeout

        while time.time() < timeout:
            if self.plan_api.plan_get_with_http_info()[1] == 200:
                return True

            self.sleep(1, "Waiting until http requests can be made to the backup service")

        return False

    def get_backups(self, state, repo_name):
        """ Returns the backups in a repository.

        Attr:
            state (str): The state of the repository (e.g. active).
            repo_name (str): The name of the repository.

        Returns:
            list: A list of backups in the repository.

        """
        return self.repository_api.cluster_self_repository_state_id_info_get(state, repo_name).backups

    def create_plan(self, plan_name, plan):
        """ Creates a plan

        Attr:
            plan_name (str): The name of the new plan.

        """
        return self.plan_api.plan_name_post(plan_name, body=plan)

    def create_repository(self, repo_name, plan_name):
        """ Creates an active repository

        Creates an active repository with a filesystem archive. If the objstore provider is set, then
        backs up to the cloud.

        Attr:
            repo_name (str): The name of the new repository.
            plan_name (str): The name of the plan to attach.
        """
        body = Body2(plan=plan_name, archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        return self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

    def get_repository(self, state, repo_name):
        """ Gets a repository.

        Attr:
            state (str): The state of the repository e.g. 'active'.
            repo_name (str): THe name of the repository to fetch.
        """
        return self.repository_api.cluster_self_repository_state_id_get('active', repo_name)

    def get_task_history(self, state, repo_name, task_name=None):
        if task_name:
            return self.repository_api.cluster_self_repository_state_id_task_history_get(state, repo_name, task_name=task_name)
        return self.repository_api.cluster_self_repository_state_id_task_history_get(state, repo_name)

    def map_task_to_backup(self, state, repo_name, task_name):
        return self.get_task_history(state, repo_name, task_name=task_name)[0].backup

    def wait_for_task(self, repo_name, task_name, timeout=200, task_scheduled_time=None, one_off=True):
        self.log.info(f"Waiting for running one off {task_name}")

        timeout = timeout + time.time()

        while time.time() <= timeout:
            repo = self.get_repository('active', repo_name)
            running_tasks = repo.running_one_off if one_off else repo.running_tasks

            if running_tasks and task_name in running_tasks:
                self.log.info(f"Task {task_name} is running (progress: {running_tasks[task_name].node_runs[0].progress})")
            else:
                task_history = self.get_task_history('active', repo_name, task_name)

                if len(task_history) > 0:
                    self.log.info(f"The task {task_name} has completed successfully.")
                    return task_history[0].status == 'done'
                else:
                    self.log.info(f"The task {task_name} has not started yet.")

            self.sleep(1)

        self.log.info(f"Task {task_name} exceeded the timeout limit.")

        return False

    def wait_for_backup_task(self, state, repo_name, retries, sleep_time, task_name=None, task_scheduled_time=None):
        """ Wait for the latest backup Task to complete.
        """
        # Wait for any existing running tasks to finish running
        for i in range(0, retries):
            repository = self.repository_api.cluster_self_repository_state_id_get(state, repo_name)

            if i == retries - 1:
                return False

            if not repository.running_one_off and not repository.running_tasks:
                break

            self.sleep(sleep_time)

        # Check the task is present in the task history with the status 'done'
        if task_name:
            for i in range(0, retries):
                task_history = self.get_task_history(state, repo_name, task_name)

                # Be more specific by filtering tasks which match the original schedule time
                if task_scheduled_time:
                    task_history = [task for task in task_history if TimeUtil.rfc3339nano_to_datetime(task.start) >= TimeUtil.rfc3339nano_to_datetime(task_scheduled_time)]

                if i == retries - 1:
                    return False

                if len(task_history) > 0:
                    if task_history[0].status == 'done':
                        return True

                    if task_history[0].status == 'failed':
                        return False

                self.sleep(sleep_time)

        # Check there is a complete task at the top in the list of backups
        for i in range(0, retries):
            backups = self.get_backups(state, repo_name)
            if len(backups) > 0 and backups[0].complete:
                return True
            self.sleep(sleep_time)

        return False

    def wait_until_documents_are_persisted(self, timeout=120):
        """ Wait until the documents are persisted to disk

        The 'ep_queue_size' stat represents the disk queue size, this reaches the value 0 when all documents have been persisted to disk.
        """
        for bucket in self.buckets:
            if not RebalanceHelper.wait_for_stats_on_all(self.backupset.cluster_host, bucket.name, 'ep_queue_size', 0, timeout_in_seconds=timeout):
                return False

        return True

    def get_archive(self, prefix=None, bucket=None, path=None):
            """ Returns a cloud archive path
            """
            prefix = prefix if prefix else self.objstore_provider.schema_prefix()
            bucket = bucket if bucket else self.objstore_provider.bucket
            path = path if path else self.backupset.directory

            return f"{prefix}{bucket}/{path}"

    def set_cloud_credentials(self, body):
        """ Set the cloud credentials of a Body

        Set the cloud credentials of a Body object using the objstore provider.
        """
        self.assertIsNotNone(self.objstore_provider, "An objstore_provider is required to set cloud credentials.")
        body.archive = self.get_archive()
        body.cloud_staging_dir = self.objstore_provider.staging_directory
        body.cloud_credentials_id = self.objstore_provider.access_key_id
        body.cloud_credentials_key = self.objstore_provider.secret_access_key
        body.cloud_credentials_region = self.objstore_provider.region
        body.cloud_endpoint = self.objstore_provider.endpoint
        body.cloud_force_path_style = True
        return body

    def take_one_off_merge(self, state, repo_name, start, end, timeout):
        """ Take a one off merge
        """
        # Perform a one off merge
        task_name = self.active_repository_api.cluster_self_repository_active_id_merge_post(repo_name, body=Body5(start=start, end=end)).task_name
        # Wait until task is completed
        self.assertTrue(self.wait_for_task(repo_name, task_name, timeout=timeout))
        return task_name

    def take_one_off_backup(self, state, repo_name, full_backup, retries, sleep_time):
        """ Take a one off backup
        """
        # Perform a one off backup
        task_name = self.active_repository_api.cluster_self_repository_active_id_backup_post(repo_name, body=Body4(full_backup = full_backup)).task_name
        # Wait until task has completed
        self.assertTrue(self.wait_for_backup_task(state, repo_name, retries, sleep_time, task_name=task_name))
        return task_name

    def take_n_one_off_backups(self, state, repo_name, generator_function, no_of_backups, doc_load_sleep=10, sleep_time=20, retries=20, data_provisioning_function=None):
        """ Take n one off backups
        """
        for i in range(0, no_of_backups):
            # Load buckets with data
            if generator_function:
                self._load_all_buckets(self.master, generator_function(i), "create", 0)

            # Call the data_provisioning_function if provided
            if data_provisioning_function:
                data_provisioning_function(i)

            # Sleep to ensure the bucket is populated with data
            if generator_function or data_provisioning_function:
                self.sleep(doc_load_sleep)

            self.take_one_off_backup(state, repo_name, i < 1, retries, sleep_time)

    def take_one_off_restore(self, state, repo_name, retries, sleep_time, target=None):
        """ Performs a one off restore

        Args:
             target (TestInputServer): The target to restore to.
        """
        if not target:
            target = self.backupset.cluster_host

        # Specify restore target
        body = Body1(target=f"{target.ip}:{target.port}", user=target.rest_username, password=target.rest_password, auto_create_buckets=True)
        # Take one off restore
        task_name = self.repository_api.cluster_self_repository_state_id_restore_post(state, repo_name, body=body).task_name

        # Check the task completed successfully
        self.assertTrue(self.wait_for_backup_task(state, repo_name, retries, sleep_time, task_name=task_name))

        return task_name

    def create_repository_with_default_plan(self, repo_name):
        """ Create a repository with a random default plan
        """
        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie default plan to repository
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

    def drop_all_buckets(self, retries=10, sleep=30):
        """ Drop all buckets
        """
        rest_connection = RestConnection(self.master)
        for bucket in self.buckets:
            rest_connection.delete_bucket(bucket, retries, sleep)

    def flush_all_buckets(self):
        """ Flush all buckets to empty cluster of documents
        """
        rest_connection = RestConnection(self.master)
        for bucket in self.buckets:
            rest_connection.change_bucket_props(bucket, flushEnabled=1)
            rest_connection.flush_bucket(bucket)

    def replace_bucket(self, cluster_host, bucket_to_replace, new_bucket, ramQuotaMB=100):
        """ Replaces an existing bucket
        """
        # The only way to create a bucket in testrunner is to delete an existing bucket
        rest_conn = RestConnection(cluster_host)
        rest_conn.delete_bucket(bucket=bucket_to_replace)
        rest_conn.create_bucket(bucket=new_bucket, ramQuotaMB=ramQuotaMB)
        self.buckets = rest_conn.get_buckets()

    def create_scope_and_collection(self, cluster_host, bucket_name, scope_name, collection_name):
        """ Create scope, collection and obtain collection id
        """
        rest_conn = RestConnection(cluster_host)
        rest_conn.create_scope(bucket_name, scope_name)
        rest_conn.create_collection(bucket_name, scope_name, collection_name)
        return rest_conn.get_collection_uid(bucket_name, scope_name, collection_name)

    def create_variable_no_of_collections_and_scopes(self, cluster_host, bucket_name, no_of_scopes, no_of_collections):
        """ Create `no_of_collections` in each `no_of_scopes` scopes in `bucket_name

        Returns:
            (dict): Returns a dictionary mapping a tuple (scope_name, collection_name) to its collection id.
        """
        rest_conn = RestConnection(cluster_host)

        # Grab the current manifest and remove the uids
        manifest = rest_conn.get_bucket_manifest(bucket_name)
        manifest.pop('uid')
        for scope in manifest['scopes']:
            scope.pop('uid')
            for collection in scope['collections']:
                collection.pop('uid')

        # Do a bulk update to create scopes and collections
        manifest['scopes'] += [{'name': f"scope{i}", 'collections': [{'name': f"collection{j}"} for j in range(no_of_collections)]} for i in range(no_of_scopes)]
        rest_conn.put_collection_scope_manifest(bucket_name, manifest)

        # Return a list of all the collection ids
        return {(f"scope{i}", f"collection{j}"): rest_conn.get_collection_uid(bucket_name, f"scope{i}", f"collection{j}") for i in range(no_of_scopes) for j in range(no_of_collections)}

    def get_hidden_repo_name(self, repo_name):
        """ Get the hidden repo name for `repo_name`
        """
        return self.repository_api.cluster_self_repository_state_id_get('active', repo_name).repo

    def get_leader(self, clusters=None):
        """ Gets the leader from the logs
        """
        if clusters is None:
            clusters = self.input.clusters[0]

        # Obtain log entries from all nodes which contain "Setting self as leader"
        # Save entries as a list of tuples where the first element is the log time and the
        # second element is the server from where that entry was obtained
        logs = [(TimeUtil.rfc3339nano_to_datetime(log.split("\t")[0]), server) for server in clusters for log in File(server, self.log_directory).read() if "Setting self as leader" in log]

        # Sort in ascending order based on log time which is the first element in tuple
        logs.sort()

        if len(logs) < 1:
            self.fail("Could not obtain the leader from the logs")

        # Make sure list is sorted in ascending order
        for log_prev, log_next in zip(logs, logs[1:]):
            self.assertLessEqual(log_prev[0], log_next[0], "The list is not sorted in ascending order")

        # The last element in the list has the latest log time
        log_time, leader = logs[-1]

        return leader

    def create_plan_and_repository(self, plan_name, repo_name, schedule, merge_map=None):
        """ Creates a plan with a schedule and attaches it to the repository

        Attr:
            plan_name (str): The name of the plan.
            repo_name (str): The name of the repository.
            schedule (list): A list of tuples of the format [(frequency, period, at_time), ..]
            merge_map (dict): A dict of the format {int: (start_offset, end_offset), } where the tuple can be None
        """
        if not merge_map:
            merge_map = {}

        def get_task_type(i):
            return "MERGE" if i in merge_map else "BACKUP"

        def get_merge_options(i):
            merge_options = merge_map.get(i, None)

            if merge_options:
                return TaskTemplateOptions(offset_start = merge_options[0], offset_end = merge_options[1])

            return merge_options

        plan = Plan(name=plan_name, tasks=[TaskTemplate(name=f"task{i}", task_type=get_task_type(i), schedule=TaskTemplateSchedule(job_type=get_task_type(i),\
                                           frequency=freq, period=period, time=at_time), merge_options = get_merge_options(i)) for i, (freq, period, at_time) in enumerate(schedule)])

        self.create_plan(plan_name, plan)
        self.create_repository(repo_name, plan_name)

        return plan, repo_name

    def schedule_test(self, schedules, cycles, merge_map=None):
        """ Runs a schedule test
        """
        ScheduleTest(self, schedules, merge_map).run(cycles)

    def schedule_task_on_non_leader_node(self, callback):
        """ Attempts to schedule a task on a non-leader node
        """
        repositories = [f"my_repo{i}" for i in range(5)]

        # Create a bunch of repositories
        for repo_name in repositories:
            self.create_repository_with_default_plan(repo_name)

        leader = self.get_leader()

        for repo_name in repositories:
            # Perform a one off backup
            task_name = self.active_repository_api.cluster_self_repository_active_id_backup_post(repo_name, body=Body4(full_backup=True)).task_name

            # Fetch repository information
            repository = self.repository_api.cluster_self_repository_state_id_get('active', repo_name)
            # Fetch task from running_one_off
            self.assertIsNotNone(repository.running_one_off, "Expected a task to be currently running")

            task = next(iter(repository.running_one_off.values()))
            # Obtain the server that the task was scheduled on
            server_task_scheduled_on = self.uuid_to_server(task.node_runs[0].node_id)

            #  If the task was scheduled on a non-leader node, call the callback
            if leader != server_task_scheduled_on:
                callback(task, server_task_scheduled_on, repo_name, task_name)
                return

        # If we're unable to have a task scheduled on a non-leader node, then simply pass
        self.log.info("Unable to schedule a task on a non-leader node")

    def replace_services(self, server, new_services):
        """ Changes the services of a server
        """
        # Remove server and rebalance
        self.cluster.rebalance(self.input.clusters[0], [], [server])

        # Add back with new services
        RestConnection(self.input.clusters[0][0]).add_node(server.rest_username, server.rest_password, server.ip, services=new_services)
        # Rebalance
        self.cluster.rebalance(self.input.clusters[0], [], [])

        # A little sleep for services to warmup
        self.sleep(15)

    # Clean up cbbackupmgr
    def tearDown(self):
        """ Tears down.

        1. Runs preamble.
        2. Deletes all plans.
        """
        self.preamble()
        if self.is_backup_service_running():
            self.delete_all_repositories()
            self.delete_all_plans()
            # If the reset_time flag is set, reset the time during the tearDown
            if self.input.param('reset_time', False):
                self.time.reset()

        # The tearDown before the setUp which means certain attributes such as the backupset do not exist
        try:
            self.delete_temporary_directories()
        except AttributeError:
            pass

        try:
            self.nfs_connection.clean(self.backupset.directory)
        except AttributeError:
            pass

        if self.input.param('node_to_node_encryption', False):
            self.node_to_node_encryption.disable()

        # We have to be careful about how we tear things down to speed up tests,
        # all rebalances must be skipped until the last test is run to avoid
        # running into MB-41898

        # Delete buckets before rebalance
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)

        # Set 'default_cleanup' to perform testrunner's default tear down process.
        self.default_cleanup = self.input.param("default_cleanup", False)

        # Call EnterpriseBackupRestoreBase's to delete archives/repositories and temporary directories
        # skipping any node ejection and rebalances
        self.clean_up(self.input.clusters[1][0], skip_eject_and_rebalance=not self.default_cleanup)

        # Kill asynchronous tasks which are still running
        self.cluster.shutdown(force=True)

        # Start the vboxadd-service again
        self.multiple_remote_shell_connections.execute_command("sudo service vboxadd-service start")

        if self.default_cleanup:
            super().tearDown()

class ServerUtil:
    """ A class to obtain information about a server
    """

    def __init__(self, server):
        self.remote_connection = RemoteMachineShellConnection(server)

    def get_free_memory(self):
        """ Gets the available on a server
        """
        output, error = self.remote_connection.execute_command("free -m | grep Mem | awk '{print $2}'")
        return int(output[0])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.remote_connection.disconnect()

class Time:
    """ A class to manipulate time.
    """

    def __init__(self, remote_connection):
        self.remote_connection = remote_connection

    def reset(self):
        """ Resets system back to the time on the hwclock.
        """
        self.remote_connection.execute_command("hwclock --hctosys")

    def change_system_time(self, time_change):
        """ Change system time.

        params:
            time_change (str): The time change e.g. "+1 month +1 day"
        """
        self.remote_connection.execute_command(f"date --set='{time_change}'")

    def get_system_time(self):
        """ Get system time
        """
        return min(TimeUtil.rfc3339nano_to_datetime(output[0]) for output, error in self.remote_connection.execute_command("date --rfc-3339=seconds | sed 's/ /T/'") if output)

    def set_timezone(self, timezone):
        """ Set the timezone
        """
        self.remote_connection.execute_command(f"timedatectl set-timezone {timezone}")

class HistoryFile:
    """ A class to manipulate a history file
    """
    def __init__(self, backupset, hidden_repo_name):
        self.backupset, self.hidden_repo_name = backupset, hidden_repo_name
        self.remote_shell = RemoteMachineShellConnection(self.backupset.cluster_host)
        self.history_file = self.get_path_to_history_file()

    def get_archive_hidden_directory(self):
        """ Get the .cbbs hidden directory present in `self.backupset.directory`
        """
        output, error = self.remote_shell.execute_command(f"ls -a {self.backupset.directory} | grep .cbbs")
        if not output:
            self.fail(f"Could not find .cbbs hidden directory in {self.backupset.directory}")
        return os.path.join(self.backupset.directory, output[0])

    def get_path_to_history_file(self):
        """ Get path to the task history file for `repo_name`
        """
        return os.path.join(self.get_archive_hidden_directory(), f"self-{self.hidden_repo_name}", 'history.0')

    def history_file_get_last_entry(self):
        """ Get contents of task history file for `repo_name`
        """
        output, error = self.remote_shell.execute_command(f"tail -n 1 {self.history_file}")
        if not output:
            self.fail(f"The history file {self.history_file} is empty")
        return output[0]

    def history_file_get_timestamp(self):
        """ Get the timestamp at the top of the history file
        """
        output, error = self.remote_shell.execute_command(f"head -n 1 {self.history_file}")
        if not output:
            self.fail(f"The history file {self.history_file} is empty")
        return output[0]

    def history_file_append(self, content):
        """ Append content to history file
        """
        chunk_size = 10000
        for i in range(0, len(content), chunk_size):
            self.remote_shell.execute_command(f"printf {shlex.quote(content[i:i+chunk_size])} >> {self.history_file}")

    def history_file_delete_last_entry(self):
        """ History file delete last entry
        """
        self.remote_shell.execute_command(f"sed -i '$ d' {self.history_file}")

class NfsConnection:
    def __init__(self, server, clients, abstract_share_factory):
        self.server, self.clients = abstract_share_factory.create_server(server), [abstract_share_factory.create_client(server, client) for client in clients]

    def share(self, directory_to_share, directory_to_mount, privileges={}):
        self.server.share(directory_to_share, privileges=privileges)

        for client in self.clients:
            client.mount(directory_to_share, directory_to_mount)

    def clean(self, directory_to_mount):
        self.server.clean()

        for client in self.clients:
            client.clean(directory_to_mount)

class AbstractShareFactory(abc.ABC):

    @abc.abstractmethod
    def create_client(self, server, client):
        raise NotImplementedError("Please Implement this method")

    @abc.abstractmethod
    def create_server(self, server):
        raise NotImplementedError("Please Implement this method")

class NfsShareFactory(AbstractShareFactory):

    def create_client(self, server, client):
        return NfsClient(server, client)

    def create_server(self, server):
        return NfsServer(server)

class SambaShareFactory(AbstractShareFactory):

    def create_client(self, server, client):
        return SambaClient(server, client)

    def create_server(self, server):
        return SambaServer(server)

class Client(abc.ABC):

    def __init__(self, server, client):
        self.server, self.client, self.remote_shell = server, client, RemoteMachineShellConnection(client)
        self.provision()

    @abc.abstractmethod
    def provision(self):
        raise NotImplementedError("Please Implement this method")

    @abc.abstractmethod
    def mount(self, directory_to_share, directory_to_mount):
        raise NotImplementedError("Please Implement this method")

    @abc.abstractmethod
    def clean(self, directory_to_mount):
        raise NotImplementedError("Please Implement this method")

class Server(abc.ABC):

    def __init__(self, server):
        self.remote_shell = RemoteMachineShellConnection(server)
        self.provision()

    @abc.abstractmethod
    def provision(self):
        raise NotImplementedError("Please Implement this method")

    @abc.abstractmethod
    def share(self, directory_to_share, privileges={}):
        raise NotImplementedError("Please Implement this method")

    @abc.abstractmethod
    def clean(self):
        raise NotImplementedError("Please Implement this method")

class NfsServer(Server):
    exports_directory = "/etc/exports"

    def __init__(self, server):
        self.remote_shell = RemoteMachineShellConnection(server)
        self.provision()

    def fetch_id(self, username, id_type='u'):
        """ Fetch a id dynamically

        Args:
            id_type (str): If 'u' fetches the uid. If 'g' fetches the gid.
        """
        accepted_id_types = ['u', 'g']

        if id_type not in accepted_id_types:
            raise ValueError(f"The id_type:{id_type} is not in {accepted_id_types}")

        output, error = self.remote_shell.execute_command(f"id -{id_type} {username}")
        return int(output[0])

    def provision(self):
        self.remote_shell.execute_command("yum -y install nfs-utils")
        self.remote_shell.execute_command("systemctl start nfs-server.service")

    def share(self, directory_to_share, privileges={}):
        """ Shares `directory_to_share`

        params:
            directory_to_share (str): The directory that will be shared, it will be created/emptied.
            privileges (dict): A dict of hosts to host specific privileges where privilegs are comma
            separated e.g. {'127.0.0.1': 'ro'}.
        """
        self.remote_shell.execute_command(f"exportfs -ua")
        self.remote_shell.execute_command(f"rm -rf {directory_to_share}")
        self.remote_shell.execute_command(f"mkdir -p {directory_to_share}")
        self.remote_shell.execute_command(f"chmod -R 777 {directory_to_share}")
        self.remote_shell.execute_command(f"chown -R couchbase:couchbase {directory_to_share}")
        self.remote_shell.execute_command(f"echo -n > {NfsServer.exports_directory}")

        # If there are no privileges every host gets read-write permissions
        if not privileges:
            privileges['*'] = 'rw'

        # Fetch uid, gid dynamically
        uid, gid = self.fetch_id('couchbase', id_type='u'), self.fetch_id('couchbase', id_type='g')

        for host, privilege in privileges.items():
            self.remote_shell.execute_command(f"echo '{directory_to_share} {host}({privilege},sync,all_squash,anonuid={uid},anongid={gid},fsid=1)' >> {NfsServer.exports_directory} && exportfs -a")

    def clean(self):
        self.remote_shell.execute_command(f"echo -n > {NfsServer.exports_directory} && exportfs -a")

class NfsClient(Client):

    def __init__(self, server, client):
        super().__init__(server, client)

    def provision(self):
        self.remote_shell.execute_command("yum -y install nfs-utils")
        self.remote_shell.execute_command("systemctl start nfs-server.service")

    def mount(self, directory_to_share, directory_to_mount):
        self.remote_shell.execute_command(f"umount -f -l {directory_to_mount}")
        self.remote_shell.execute_command(f"rm -rf {directory_to_mount}")
        self.remote_shell.execute_command(f"mkdir -p {directory_to_mount}")
        self.remote_shell.execute_command(f"mount {self.server.ip}:{directory_to_share} {directory_to_mount}")

    def clean(self, directory_to_mount):
        self.remote_shell.execute_command(f"umount -f -l {directory_to_mount}")

class SambaClient(Client):

    def __init__(self, server, client):
        super().__init__(server, client)

    def provision(self):
        self.remote_shell.execute_command(f"yum -y install samba-client cifs-utils")

    def mount(self, directory_to_share, directory_to_mount):
        self.remote_shell.execute_command(f"umount -f -l {directory_to_mount}")
        self.remote_shell.execute_command(f"rm -rf {directory_to_mount}")
        self.remote_shell.execute_command(f"mkdir -p {directory_to_mount}")
        self.remote_shell.execute_command(f"mount -t cifs -o username=Couchbase,password=password //{self.server.ip}/Anonymous {directory_to_mount}")

    def clean(self, directory_to_mount):
        self.remote_shell.execute_command(f"umount -f -l {directory_to_mount}")

class SambaServer(Server):
    samba_config_directory = "/etc/samba/smb.conf"

    def __init__(self, server):
        super().__init__(server)

    def provision(self):
        self.remote_shell.execute_command("yum -y install samba")

    def share(self, directory_to_share, privileges={}):
        """ Shares `directory_to_share`

        params:
            directory_to_share (str): The directory that will be shared, it will be created/emptied.
            privileges (dict): A dict of hosts to host specific privileges where privilegs are comma
            separated e.g. {'127.0.0.1': 'ro'}.
        """

        if privileges:
            not NotImplementedError("This is currently unsupported")

        file = (f"[global]\n"
                f"workgroup = WORKGROUP\n"
                f"security = user\n"
                f"map to guest = bad user\n"
                f"netbios name = TEST\n"
                f"[Anonymous]\n"
                f"path = {directory_to_share}\n"
                f"browsable = yes\n"
                f"writable = yes\n"
                f"guest ok = yes\n"
                f"guest account = couchbase\n"
                f"read only = no")

        self.remote_shell.execute_command(f"rm -rf {directory_to_share}")
        self.remote_shell.execute_command(f"mkdir -p {directory_to_share}")
        self.remote_shell.execute_command(f"chmod -R 777 {directory_to_share}")
        self.remote_shell.execute_command(f"chown -R couchbase:couchbase {directory_to_share}")
        self.remote_shell.execute_command(f"echo '{file}' > {SambaServer.samba_config_directory}")
        self.remote_shell.execute_command(f"systemctl restart smb.service")
        self.remote_shell.execute_command(f"(echo \"password\"; echo \"password\") | smbpasswd -a couchbase")
        self.remote_shell.execute_command(f"smbpasswd -e couchbase")

    def clean(self):
        self.remote_shell.execute_command(f"echo -n > {SambaServer.samba_config_directory} && systemctl stop smb.service")

class MultipleRemoteShellConnections:
    """ A class to handle connections to multiple servers in one go.

    Usage:
        >>> connections = RemoteMachineShellConnections(list_of_servers)
        >>> results = connections.execute_command("ls -l", get_exit_code=True)
        >>> connections.disconnect()
        >>> print(results)
    """

    def __init__(self, servers):
        """  Constructor

        params:
           servers list(TestInputServer): A list of servers.
        """
        self.connections = [RemoteMachineShellConnection(server) for server in servers]

        self.supported_methods = ['execute_command', 'disconnect']

        for method in self.supported_methods:
            setattr(self, method, self.call_in_sequence(getattr(RemoteMachineShellConnection, method)))

    def call_in_sequence(self, func):
        """ A decorator which calls func on every conn and returns a list of the returned values.
        """
        def wrap(*args, **kwargs):
            return [func(conn, *args, **kwargs) for conn in self.connections]
        return wrap

class File:
    def __init__(self, server, file_name):
        self.file_name, self.remote_client = file_name, RemoteMachineShellConnection(server)

    def read(self):
        """ Returns the contents of a file
        """
        output, error = self.remote_client.execute_command(f"cat {self.file_name}")
        return output

    def empty(self):
        """ Empties a file
        """
        self.remote_client.execute_command(f"echo -n > {self.file_name}")

    def delete(self):
        self.remote_client.execute_command(f"rm -f {self.file_name}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.remote_connection.disconnect()

    @staticmethod
    def find(output, substrings):
        """ Checks if all the substrings are present in the output
        """
        for substring in substrings:
            if not any(substring in line for line in output):
                return False, substring

        return True, None

class Collector:
    """ Collects logs
    """
    # Log file
    backup_service_log = "ns_server.backup_service.log"

    def __init__(self, server, log_redaction=False):
        self.server, self.remote_connection = server, RemoteMachineShellConnection(server)
        self.log_redaction = log_redaction
        self.out_path = f"/tmp/output-{server.ip}"
        self.zip_path = f"{self.out_path}.zip"
        self.red_path = f"{self.out_path}-redacted.zip"
        self.col_path = f"{self.out_path}/cbcollect_info_ns*"
        self.remote_connection.execute_command("yum install -y unzip")
        self.clean_up()

    def __enter__(self):
        return self

    def clean_up(self):
        self.remote_connection.execute_command(f"rm -rf {self.out_path} {self.zip_path} {self.red_path}")

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.clean_up()
        self.remote_connection.disconnect()

    def collect(self):
        self.clean_up()
        self.remote_connection.execute_cbcollect_info(self.zip_path, options= "--log-redaction-level=partial" if self.log_redaction else "")
        self.remote_connection.execute_command(f"unzip {self.red_path if self.log_redaction else self.zip_path} -d {self.out_path}")

    def files(self):
        output, error = self.remote_connection.execute_command(f"ls {self.col_path}")
        return output

class Prometheus:
    """ A class to obtain stats from Prometheus
    """

    def __init__(self, server):
        """ Constructor

        params:
            server TestInputServer: A server
        """
        self.rest_conn = RestConnection(server)

    def stats_range(self, metric):
        """ Gets `metric` from the range endpoint.
        """
        return json.loads(self.rest_conn._http_request(self.rest_conn.baseUrl + f"pools/default/stats/range/{metric}")[1])

class FailoverServer:

    def __init__(self, servers, server):
        """ Constructor

        params:
            servers list(TestInputServer): A list of servers in a cluster
            server TestInputServer: The server to failover and recover
        """
        self.servers, self.server, self.cluster, self.rest_conn = servers, server, Cluster(), RestConnection(servers[0])

    def failover(self, graceful=True):
        """ Starts the failover process

        Does not currently block while the failover is in progress.

        params:
            recovery_type (str): 'full' or 'delta'
        """
        self.rest_conn.fail_over(self.node_id, graceful=graceful)

    def recover_from_failover(self, recovery_type='full'):
        """ Recovers from a failover.

        params:
            recovery_type (str): 'full' or 'delta'
        """
        # Check the node health status to check if it's still failing over
        self.rest_conn.set_recovery_type(self.node_id, recovery_type)
        self.rest_conn.add_back_node(self.node_id)
        self.cluster.rebalance(self.servers, [], [])

    def rebalance(self):
        self.cluster.rebalance(self.servers, [], [])

    @property
    def node_id(self):
        return next((node.id for node in self.rest_conn.node_statuses() if node.ip == self.server.ip), None)

class ScheduleTest:

    def __init__(self, backup, schedules, merge_map=None):
        """ Constructor

        backup (BackupServiceBase): A BackupServiceBase object to manipulate the backup service
        """
        self.backup = backup
        # Create plans and repos using schedule
        self.plan_repo = [self.backup.create_plan_and_repository(f"plan_name{i}", f"repo_name{i}", schedule, merge_map=merge_map) for i, schedule in enumerate(schedules)]
        # The expected time calculations are made using `now`
        self.curr_time, self.curr_scheduled_time_as_string, self.curr_task_name, self.elapsed_cycles = None, None, None, 0

        self.reconstruct_schedule()

        self.backup.sleep(5)

    def run(self, cycles):
        """ Runs a schedule test
        """
        for i in range(cycles):
            if self.elapsed_cycles > 0:
                for plan, _ in self.plan_repo:
                    plan.recalculate_task_by_name(self.curr_task_name, self.now)

            # Obtain the actual time and task to execute from the repository
            for j in range(5):
                (next_time, next_time_as_string, next_task_name), repo_name = min((self.backup.get_repository('active', repo_name).next_scheduled, repo_name) for _, repo_name in self.plan_repo)
                if next_time == self.curr_time:
                    self.backup.sleep(5)
                else:
                    self.curr_time, self.curr_scheduled_time_as_string, self.curr_task_name = next_time, next_time_as_string, next_task_name
                    break

            # Predict an expected task and its time to execute
            (expected_time, expected_task), expected_repo_name = min((plan.expected_next_run_time(self.now), repo_name) for plan, repo_name in self.plan_repo) # The expected task and its time to execute

            self.backup.log.info(f"\nExpected time for {expected_task} is {expected_time} | Actual time for {self.curr_task_name} is {self.curr_time} | Using start time {self.now}")

            self.backup.assertLess(abs(expected_time - self.curr_time).total_seconds(), 120)

            # Travel to a minute before scheduled time
            self.backup.time.change_system_time(str(self.curr_time - datetime.timedelta(minutes=1)))

            # Sleep till task is triggered
            self.backup.sleep(60)
            self.now = self.backup.time.get_system_time() # Get time needed to calculate the next prediction
            self.backup.wait_for_backup_task("active", repo_name, 20, 2, self.curr_task_name, self.curr_scheduled_time_as_string)

            self.elapsed_cycles = self.elapsed_cycles + 1

    def reconstruct_schedule(self):
        """ Recomputes all expected scheduled times
        """
        self.now = self.backup.time.get_system_time()

        for plan, _ in self.plan_repo:
            plan.recalculate_all(self.now)

class DocumentUtil:

    @staticmethod
    def unique_document_key(id_field_name, scope_field_name=None, collection_field_name=None):
        """ Returns a function which extracts a key to uniquely identifies a document.

            Can be used documents emitted by the cbexport tool as if they contain a scope/collection field.

            If the scope_field_name and collection_field_name is not provided, then the unique_id is used
            on its own.
        """
        if scope_field_name and collection_field_name:
            return lambda document: (document[scope_field_name], document[collection_field_name], document[id_field_name])

        return lambda document: document[id_field_name]

class NodeToNodeEncryption:

    def __init__(self, server):
        """ Constructor

        Args:
            server (TestInputServer): A server.
        """
        self.server, self.remote_connection = server, RemoteMachineShellConnection(server)
        self.common = f"-c http://{server.ip}:{server.port} -u {server.rest_username} -p {server.rest_password}"

    def enable(self):
        """ Enables node to node encryption

        Returns:
            (bool): Upon success
        """
        self.remote_connection.execute_command(f"/opt/couchbase/bin/couchbase-cli setting-autofailover {self.common} --enable-auto-failover 0", debug=True)
        self.remote_connection.execute_command(f"/opt/couchbase/bin/couchbase-cli node-to-node-encryption {self.common} --enable", debug=True)
        self.remote_connection.execute_command(f"/opt/couchbase/bin/couchbase-cli setting-security {self.common} --set --cluster-encryption-level all", debug=True)
        self.remote_connection.execute_command(f"/opt/couchbase/bin/couchbase-cli setting-autofailover {self.common} --auto-failover-timeout 120 --enable-failover-of-server-groups 1 --max-failovers 2 --can-abort-rebalance 1", debug=True)

        output, error = self.remote_connection.execute_command(f"/opt/couchbase/bin/couchbase-cli node-to-node-encryption {self.common} --get")
        return "Node-to-node encryption is enabled" in output[0]

    def disable(self):
        """ Disables node to node encryption
        """
        self.remote_connection.execute_command(f"/opt/couchbase/bin/couchbase-cli setting-security {self.common} --set --cluster-encryption-level control", debug=True)
        self.remote_connection.execute_command(f"/opt/couchbase/bin/couchbase-cli node-to-node-encryption {self.common} --disable", debug=True)

class ClientStrategy:

    def __init__(self, path, prefix, delimiter, username):
        """ Defines the strategy for how the username is obtained from the client certificate.

        Args:
            path (str): Determines where the username is stored.
            prefix (str): The portion of the username that is removed and ignored.
            delimiter (str): The resolved username is the first element in the split on the delimiter.
        """
        self.prefix = prefix
        self.username = username
        self.delimiter = delimiter

        valid_paths = ['subject.cn', 'san.dnsname', 'san.email', 'san.uri']

        if path not in valid_paths:
            raise ValueError(f"Path `{path}` is not in {valid_paths}")

        self.path = path
        self.head, self.tail = path.split('.')

    def username_is_in_san(self):
        """ Returns true if the username is fetched from the subjectAltName in the extensions

        Otherwise, the username is fetched from the subject.
        """
        return self.head == 'san'

    def get_extension(self):
        extension = \
        {
            'basicConstraints': 'CA:FALSE',
            'subjectKeyIdentifier': 'hash',
            'authorityKeyIdentifier': 'keyid,issuer:always',
            'extendedKeyUsage': 'clientAuth',
            'keyUsage': 'digitalSignature'
        }

        # The capitalisation varies in a SAN for some reason
        field = {'dnsname': 'DNS', 'uri': 'URI', 'email': 'email'}[self.tail]

        if self.username_is_in_san():
            extension['subjectAltName'] = f"{field}:{self.username}"

        return extension

    def get_subject(self):
        return {} if self.username_is_in_san() else {f"{self.tail.upper()}": self.username}

    def get_dictionary(self):
        return {"path": self.path, "prefix": self.prefix, "delimiter": self.delimiter}

class ClientCertificateTester:

    def __init__(self, clusters, strategy, crypto_tester):
        """

        Args:
            strategy(ClientStrategy): The client strategy determines where the username is stored.
        """
        self.clusters, self.crypto, self.crypto_tester, = clusters, Crypto(clusters[0]), crypto_tester
        self.strategy = strategy

        self.rest_conn = RestConnection(self.clusters[0])

    def create_client_pair(self):
        """ Creates a key and a certificate pair which has been signed by the root private key

        Returns:
            (str, str): A private key and certificate for the client
        """
        private_key = self.crypto.create_private_key()
        signing_req = self.crypto.create_signing_request(private_key, self.strategy.get_subject())
        certificate = self.crypto.sign_request(self.crypto_tester.get_root_private_key(), self.crypto_tester.get_root_certificate(), signing_req, self.strategy.get_extension())

        return private_key, certificate

    def client_auth_enable(self):
        """ Enables client certificates on the clusters based on the strategy.
        """
        status, content = self.rest_conn.client_cert_auth('enable', [self.strategy.get_dictionary()])

    def client_auth_disable(self):
        """ Disables client certificates on the clusters.
        """
        status, content = self.rest_conn.client_cert_auth('disable', [])

class CryptoTester:

    def __init__(self, clusters):
        """ Constructor

        Args:
            clusters (list (TestInputServer)): The clusters to provision
        """
        # An object that creates key pairs
        self.clusters, self.crypto = clusters, Crypto(clusters[0])

        # Create all key pairs
        self.create()

    def get_per_node_pair(self):
        """ Get a list of all per-node private keys and certificates
        """
        return self.per_node_pair

    def get_root_private_key(self):
        """ Get the root private key
        """
        return self.root_private_key

    def get_root_certificate(self):
        """ Get the root certificate
        """
        return self.root_certificate

    def create(self):
        """ Generates all private key and certificate pairs.

        Generates a root private key and a root certificate.
        Generates a per-node private key and a certificate that has been signed by the root private key.
        """
        # Create the cluster wide key and certificate pair
        self.root_private_key = self.crypto.create_private_key()
        self.root_certificate = self.crypto.create_root_certificate(self.root_private_key, {'CN':'Couchbase Root CA'})

        self.per_node_pair = []
        # Create the per node private-key and signing request
        for server in self.clusters:
            per_node_private_key = self.crypto.create_private_key()
            per_node_signing_req = self.crypto.create_signing_request(per_node_private_key, {'CN':'Couchbase Server'})
            self.per_node_pair.append((per_node_private_key, per_node_signing_req))

        # Sign requests using the root private key to generate a per node certificate
        for i, (per_node_private_key, per_node_signing_req) in enumerate(self.per_node_pair):
            # A certificate signing request requires a certificate extension which determines
            # how the certificate will be used (e.g. for encipherment of keys).
            certificate_extension = \
            {
                'basicConstraints': 'CA:FALSE',
                'subjectKeyIdentifier': 'hash',
                'authorityKeyIdentifier': 'keyid,issuer:always',
                'extendedKeyUsage': 'serverAuth',
                'keyUsage': 'digitalSignature,keyEncipherment',
                'subjectAltName': f"IP:{self.clusters[i].ip}" # Configure the subjectAltName in the certificate extension - Supply the IP of the server
            }
            # Sign the request root private key to obain a certificate
            per_node_certificate = self.crypto.sign_request(self.root_private_key, self.root_certificate, per_node_signing_req, certificate_extension)
            # Replace the signing request with the certificate in our per-node data-structure
            self.per_node_pair[i] = per_node_private_key, per_node_certificate

    def upload(self):
        """ Activates the root certificate and the per-node key pairs.
        """
        # Place the per-node private key and certificate in /opt/couchbase/...
        for ((per_node_private_key, per_node_certificate), server) in zip(self.per_node_pair, self.clusters):
            connection = RemoteMachineShellConnection(server)
            connection.execute_command(f"mkdir -p /opt/couchbase/var/lib/couchbase/inbox/")
            connection.execute_command(f"echo '{per_node_private_key}' > /opt/couchbase/var/lib/couchbase/inbox/pkey.key")
            connection.execute_command(f"echo '{per_node_certificate}' > /opt/couchbase/var/lib/couchbase/inbox/chain.pem")
            connection.execute_command(f"chmod -R a+x /opt/couchbase/var/lib/couchbase/inbox/")
            connection.execute_command(f"chown -R couchbase:couchbase /opt/couchbase/var/lib/couchbase/inbox/")
            connection.disconnect()

        # Upload the root certificate
        for cluster in self.clusters:
            rest_connection = RestConnection(cluster)
            rest_connection.upload_cluster_ca(self.root_certificate)
            rest_connection.reload_certificate()

class Crypto:

    def __init__(self, server):
        self.remote_connection = RemoteMachineShellConnection(server)

    def clean(self, files):
        self.remote_connection.execute_command(f"rm -f {' '.join(files)}")

    def subject_to_string(self, subject):
        """Returns the string version of the subject dictionary

        Args:
            subject (dict): Key value pairs to populate the subject with.

        Returns:
            str: A string that can be passed to the -subj flag of the 'openssl req' command
        """
        return '/' +  "/".join([f"{key}={val}" for key, val in subject.items()])

    def create_private_key(self):
        """Generates a private key"""
        keyfile = "/tmp/my_key.pem"
        garbage = [keyfile]

        self.clean(garbage)

        self.remote_connection.execute_command(f"openssl genrsa -out {keyfile} 2048")
        output, error = self.remote_connection.execute_command(f"cat {keyfile}")

        self.clean(garbage)

        return "\n".join(output)

    def create_root_certificate(self, private_key, subject):
        """Generates a self-signed certificate from a private key

        Creates a signing request and self-signs (notice the usage of the x509 flag to perform the self-sign)
        """
        keyfile = "/tmp/my_key.pem"
        crtfile = "/tmp/my_certificate.pem"
        garbage = [keyfile, crtfile]

        self.clean(garbage)

        self.remote_connection.execute_command(f"echo '{private_key}' > /tmp/my_key.pem")
        self.remote_connection.execute_command(f"openssl req -new -x509 -days 3650 -sha256 -key /tmp/my_key.pem -out /tmp/my_certificate.pem -subj \"{self.subject_to_string(subject)}\"")
        output, error = self.remote_connection.execute_command(f"cat /tmp/my_certificate.pem")

        self.clean(garbage)

        return "\n".join(output)

    def create_signing_request(self, private_key, subject):
        """Generate a signing request from a private key"""
        keyfile = "/tmp/my_key"
        request = "/tmp/my_certificate_signing_request.pem"
        garbage = [keyfile, request]

        self.clean(garbage)

        self.remote_connection.execute_command(f"echo '{private_key}' > {keyfile}")
        self.remote_connection.execute_command(f"openssl req -new -key {keyfile} -out {request} -subj \"{self.subject_to_string(subject)}\"")
        output, error = self.remote_connection.execute_command(f"cat {request}")

        self.clean(garbage)

        return "\n".join(output)

    def sign_request(self, root_private_key, root_certificate, signing_request, certificate_extension):
        """Sign a request from a private key accounting for the certificate extension """
        root_key_file = "/tmp/root_private_key.pem"
        root_certificate_file = "/tmp/root_certificate.pem"
        signing_request_file = "/tmp/signing_request.pem"
        certificate_extension_file = "/tmp/certificate_extension.pem"
        certificate_file = "/tmp/certificate.pem"
        garbage = [root_key_file, root_certificate_file, signing_request_file, certificate_extension_file, certificate_file]

        certificate_extension = "\n".join([f"{key} ={val}" for key, val in certificate_extension.items()])

        self.clean(garbage)

        self.remote_connection.execute_command(f"echo '{root_private_key}' > {root_key_file}")
        self.remote_connection.execute_command(f"echo '{root_certificate}' > {root_certificate_file}")
        self.remote_connection.execute_command(f"echo '{signing_request} '> {signing_request_file}")
        self.remote_connection.execute_command(f"echo '{certificate_extension}' > {certificate_extension_file}")
        self.remote_connection.execute_command(f"openssl x509 -CA {root_certificate_file} -CAkey {root_key_file} -CAcreateserial -days 365 -req -in /tmp/signing_request.pem -out {certificate_file} -extfile {certificate_extension_file}")
        output, error = self.remote_connection.execute_command(f"cat {certificate_file}")

        self.clean(garbage)

        return "\n".join(output)

class DGMUtil:

    def calculate_dgm_threshold(bucket_size, no_of_nodes_in_cluster, document_size):
        """ Calculates the number of items required to hit the disk greater than memory state

        If this threshold is crossed, then at least 1 of the nodes will be in the dgm state.

        Args:
            bucket_size (int): The bucket size in MB (megabytes).
            no_of_nodes_in_cluster (int): The number of nodes in the cluster.
            document_size (int): The document size in bytes.
        """
        return math.ceil((no_of_nodes_in_cluster * bucket_size * 10**6) / (document_size))

class AbstractConfigurationFactory(abc.ABC):

    def __init__(self, server, hints=None):
        self.hints, self.server = hints, server

    def create_configuration_common(self):
        """ Creates a configuration and sets its credentials
        """
        configuration = Configuration()

        configuration.username = self.server.rest_username
        configuration.password = self.server.rest_password

        return configuration

    def create_configuration(self):
        """ Creates a Configuration object
        """
        raise NotImplementedError("Please Implement this method")

class HttpConfigurationFactory(AbstractConfigurationFactory):

    def create_configuration(self):
        """ Creates a http configuration object.
        """
        configuration = self.create_configuration_common()

        configuration.host = f"http://{self.server.ip}:8091/_p/backup/api/v1"

        return configuration

class HttpsConfigurationFactory(AbstractConfigurationFactory):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # The following keys must be set in order to enable ssl verification
        self.have_hints_for_ssl_verification = all(key in self.hints for key in ['client_private_key_file',  'client_certificate_file', 'root_certificate_file'])

    def create_configuration(self):
        """ Creates a https configuration object.
        """
        configuration = self.create_configuration_common()

        # The certificate used by default is self-signed, so we cannot verify
        # it's integrity by going to a trusted CA and checking if it's signed
        # by the CA's public key so we can just omit that process.
        configuration.host = f"https://{self.server.ip}:18091/_p/backup/api/v1"

        if self.have_hints_for_ssl_verification:
            configuration.key_file = self.hints['client_private_key_file']
            configuration.cert_file = self.hints['client_certificate_file']
            configuration.ssl_ca_cert = self.hints['root_certificate_file']

        # If the hints are set, we have required certificates so we can enable verify_ssl
        configuration.verify_ssl = self.have_hints_for_ssl_verification

        return configuration
