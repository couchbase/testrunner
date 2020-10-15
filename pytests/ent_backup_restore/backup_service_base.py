import os
import re
import json
import shlex
import random
import datetime

from TestInput import TestInputSingleton
from lib.couchbase_helper.time_helper import TimeUtil
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase
from membase.api.rest_client import RestConnection
from lib.backup_service_client.configuration import Configuration
from lib.backup_service_client.api_client import ApiClient
from lib.backup_service_client.api.plan_api import PlanApi
from lib.backup_service_client.api.import_api import ImportApi
from lib.backup_service_client.api.repository_api import RepositoryApi
from lib.backup_service_client.api.configuration_api import ConfigurationApi
from lib.backup_service_client.api.active_repository_api import ActiveRepositoryApi
from lib.backup_service_client.models.body2 import Body2
from lib.backup_service_client.models.body3 import Body3
from lib.backup_service_client.models.body4 import Body4
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from lib.membase.helper.bucket_helper import BucketOperationHelper

class BackupServiceBase(EnterpriseBackupRestoreBase):
    def preamble(self):
        """ Preamble.

        1. Configures Rest API.
        2. Configures Rest API Sub-APIs.
        3. Backup Service Constants.
        """
        # Rest API Configuration
        self.configuration = Configuration()
        self.configuration.host = f"http://{self.master.ip}:{8091}/_p/backup/api/v1"
        self.configuration.username = self.master.rest_username
        self.configuration.password = self.master.rest_password
        self.api_client = ApiClient(self.configuration)

        # Rest API Sub-APIs
        self.plan_api = PlanApi(self.api_client)
        self.import_api = ImportApi(self.api_client)
        self.repository_api = RepositoryApi(self.api_client)
        self.configuration_api = ConfigurationApi(self.api_client)
        self.active_repository_api = ActiveRepositoryApi(self.api_client)

        # Manipulate time
        self.time = Time(RemoteMachineShellConnection(self.master))

        # Backup Service Constants
        self.default_plans = ["_hourly_backups", "_daily_backups"]

    def setUp(self):
        """ Sets up.

        1. Runs preamble.
        """
        # Indicate that this is a backup service test
        TestInputSingleton.input.test_params["backup_service_test"] = True

        # Skip the more general clean up process that test runner does at
        # the end of each which includes tearing down the cluster by
        # ejecting nodes, rebalancing and using diag eval to 'reset'
        # the cluster.
        TestInputSingleton.input.test_params["skip_cleanup"] = True

        super().setUp()
        self.preamble()

        # Specifify this is a backup service test
        self.backupset.backup_service = True

        # Run cbbackupmgr on the same node as the backup service being tested
        self.backupset.backup_host = self.backupset.cluster_host

        # Share archive directory
        self.directory_to_share = "/tmp/share"
        self.nfs_connection = NfsConnection(self.input.clusters[0][0], self.input.clusters[0])
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

    def get_backups(self, state, repo_name):
        """ Returns the backups in a repository.

        Attr:
            state (str): The state of the repository (e.g. active).
            repo_name (str): The name of the repository.

        Returns:
            list: A list of backups in the repository.

        """
        return self.repository_api.cluster_self_repository_state_id_info_get(state, repo_name).backups

    def get_task_history(self, state, repo_name, task_name=None):
        if task_name:
            return self.repository_api.cluster_self_repository_state_id_task_history_get(state, repo_name, task_name=task_name)
        return self.repository_api.cluster_self_repository_state_id_task_history_get(state, repo_name)

    def map_task_to_backup(self, state, repo_name, task_name):
        return self.get_task_history(state, repo_name, task_name=task_name)[0].backup

    def wait_for_backup_task(self, state, repo_name, retries, sleep_time, task_name=None):
        """ Wait for the latest backup Task to complete.
        """
        for i in range(0, retries):
            repository = self.repository_api.cluster_self_repository_state_id_get(state, repo_name)

            if i == retries - 1:
                return False

            if not repository.running_one_off:
                break

            self.sleep(sleep_time)

        if task_name:
            for i in range(0, retries):
                task_history = self.get_task_history(state, repo_name, task_name)

                if i == retries - 1:
                    return False

                if len(task_history) > 0 and task_history[0].status == 'done':
                    return True

                self.sleep(sleep_time)

        for i in range(0, retries):
            backups = self.get_backups(state, repo_name)
            if len(backups) > 0 and backups[0].complete:
                return True
            self.sleep(sleep_time)

        return False

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

    def create_repository_with_default_plan(self, repo_name):
        """ Create a repository with a random default plan
        """
        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie default plan to repository
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

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

    def get_hidden_repo_name(self, repo_name):
        """ Get the hidden repo name for `repo_name`
        """
        return self.repository_api.cluster_self_repository_state_id_get('active', repo_name).repo

    def read_logs(self, server):
        """ Returns the backup service logs
        """
        log_directory = f"/opt/couchbase/var/lib/couchbase/logs/backup_service.log"

        remote_client = RemoteMachineShellConnection(server)
        output, error = remote_client.execute_command(f"cat {log_directory}")
        remote_client.disconnect()
        return output

    def get_leader(self):
        """ Gets the leader from the logs
        """
        # Obtain log entries from all nodes which contain "Setting self as leader"
        # Save entries as a list of tuples where the first element is the log time and the
        # second element is the server from where that entry was obtained
        logs = [(TimeUtil.rfc3339nano_to_datetime(log.split("\t")[0]), server) for server in self.input.clusters[0] for log in self.read_logs(server) if "Setting self as leader" in log]

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

        # We have to be careful about how we tear things down to speed up tests,
        # all rebalances must be skipped until the last test is run to avoid
        # running into MB-41898

        # Delete buckets before rebalance
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)

        # Call EnterpriseBackupRestoreBase's to delete archives/repositories and temporary directories
        # skipping any node ejection and rebalances
        self.clean_up(self.input.clusters[1][0], skip_eject_and_rebalance=True)

        # Kill asynchronous tasks which are still running
        self.cluster.shutdown(force=True)

        # If this is the last test, we want to tear down the cluster we provisioned on the first test
        # Note a test must mark it is the last test by supplying setting `last_test` as a test param
        if self.input.param("last_test", False):
            super().tearDown()

class Time:
    """ A class to manipulate time.
    """

    def __init__(self, remote_connection):
        self.remote_connection = remote_connection

    def reset(self):
        """ Resets system back to the time on the hwclock.
        """
        self.set_timezone("UTC")
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
        return datetime.datetime.utcfromtimestamp(min(int(output[0]) for output, error in self.remote_connection.execute_command("date +%s") if output)).replace(tzinfo=datetime.timezone.utc)

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
    def __init__(self, server, clients):
        self.server, self.clients = NfsServer(server), [NfsClient(server, client) for client in clients]

    def share(self, directory_to_share, directory_to_mount, privileges={}):
        self.server.share(directory_to_share, privileges=privileges)

        for client in self.clients:
            client.mount(directory_to_share, directory_to_mount)

    def clean(self, directory_to_mount):
        self.server.clean()

        for client in self.clients:
            client.clean(directory_to_mount)

class NfsServer:
    exports_directory = "/etc/exports"

    def __init__(self, server):
        self.remote_shell = RemoteMachineShellConnection(server)
        self.provision()

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

        for host, privilege in privileges.items():
            self.remote_shell.execute_command(f"echo '{directory_to_share} {host}({privilege},sync,all_squash,anonuid=997,anongid=996,fsid=1)' >> {NfsServer.exports_directory} && exportfs -a")

    def clean(self):
        self.remote_shell.execute_command(f"echo -n > {NfsServer.exports_directory} && exportfs -a")

class NfsClient:
    def __init__(self, server, client):
        self.server, self.client, self.remote_shell = server, client, RemoteMachineShellConnection(client)
        self.provision()

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
