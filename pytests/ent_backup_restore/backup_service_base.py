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
from lib.backup_service_client.models.body2 import Body2
from lib.backup_service_client.models.body3 import Body3
from lib.backup_service_client.models.body4 import Body4
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
        # Sort the available clusters by total memory in ascending order (if the total memory is the same, sort by ip)
        TestInputSingleton.input.clusters[0].sort(key = lambda server: (ServerUtil(server).get_free_memory(), server.ip))

        # Configure the master server based on the sorted list of clusters
        self.master = self.input.clusters[0][0]

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

        # Backup Service Constants
        self.default_plans = ["_hourly_backups", "_daily_backups"]
        self.periods = ["MINUTES", "HOURS", "DAYS", "WEEKS", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

        # A connection to every single machine in the cluster
        self.multiple_remote_shell_connections = MultipleRemoteShellConnections(self.input.clusters[0])

        # Manipulate time on all nodes
        self.time = Time(self.multiple_remote_shell_connections)

        # Manipulate time on individual nodes
        self.solo_time = [Time(RemoteMachineShellConnection(server)) for server in self.input.clusters[0]]

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

        # Stop the vboxadd-service again to prevent it from syncing the time with the host
        self.multiple_remote_shell_connections.execute_command("sudo service vboxadd-service stop")

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

        # We have to be careful about how we tear things down to speed up tests,
        # all rebalances must be skipped until the last test is run to avoid
        # running into MB-41898

        # Delete buckets before rebalance
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)

        # Call EnterpriseBackupRestoreBase's to delete archives/repositories and temporary directories
        # skipping any node ejection and rebalances
        self.clean_up(self.input.clusters[1][0], skip_eject_and_rebalance=not self.input.param("last_test", False))

        # Kill asynchronous tasks which are still running
        self.cluster.shutdown(force=True)

        # Start the vboxadd-service again
        self.multiple_remote_shell_connections.execute_command("sudo service vboxadd-service start")

        # If this is the last test, we want to tear down the cluster we provisioned on the first test
        # Note a test must mark it is the last test by supplying setting `last_test` as a test param
        if self.input.param("last_test", False):
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
