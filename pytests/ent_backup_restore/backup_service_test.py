import datetime
import itertools
import json
import random
import re
import threading
import time
from collections import (
    Counter
)
from multiprocessing import (
    Pool
)

from backup_service_client.rest import (
    ApiException
)
from couchbase_helper.cluster import (
    Cluster
)
from couchbase_helper.document import (
    View
)
from couchbase_helper.documentgenerator import (
    BlobGenerator,
    DocumentGenerator
)
from ent_backup_restore.backup_service_base import (
    BackupServiceBase,
    Collector,
    DocumentUtil,
    FailoverServer,
    File,
    HistoryFile,
    HttpConfigurationFactory,
    MultipleRemoteShellConnections,
    Prometheus,
    ScheduleTest,
    Time,
    Tag
)
from membase.api.exception import (
    ServerAlreadyJoinedException
)
from membase.api.rest_client import (
    RestConnection
)
from remote.remote_util import (
    RemoteMachineShellConnection,
    RemoteUtilHelper
)

from lib.backup_service_client.api.active_repository_api import (
    ActiveRepositoryApi
)
from lib.backup_service_client.api.plan_api import (
    PlanApi
)
from lib.backup_service_client.api_client import (
    ApiClient
)
from lib.backup_service_client.models.body import (
    Body
)
from lib.backup_service_client.models.body1 import (
    Body1
)
from lib.backup_service_client.models.body2 import (
    Body2
)
from lib.backup_service_client.models.body3 import (
    Body3
)
from lib.backup_service_client.models.body4 import (
    Body4
)
from lib.backup_service_client.models.body5 import (
    Body5
)
from lib.backup_service_client.models.body6 import (
    Body6
)
from lib.backup_service_client.models.plan import (
    Plan
)
from lib.backup_service_client.models.service_configuration import (
    ServiceConfiguration
)
from lib.backup_service_client.models.task_template import (
    TaskTemplate
)
from lib.backup_service_client.models.task_template_options import (
    TaskTemplateOptions
)
from lib.backup_service_client.models.task_template_schedule import (
    TaskTemplateSchedule
)
from lib.couchbase_helper.time_helper import (
    TimeUtil
)
from pytests.eventing.eventing_constants import (
    HANDLER_CODE
)
from pytests.fts.fts_callable import (
    FTSCallable
)


class BackupServiceTest(BackupServiceBase):
    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_default_plans(self):
        """ Test the default plans exist.

        1. Retrieve plans.
        2. Ensure the default plans exist.
        """
        plans = self.get_all_plans()
        self.assertTrue(set(self.default_plans) == set([plan.name for plan in plans]))

    # Backup Service: Backup Plan Cases

    def test_invalid_plan(self):
        """ Test a user cannot add an invalid plan.

        A user cannot Add/Update a Plan with Tasks containing invalid types, schedules, merge options or full backup flag.

        Invalid entries include: negative, out-of-range, non-existing values, empty/long missing, illegal characters.

        1. Define a selection of invalid Plans.
        2. Add the plan and check if the operation fails.
        """
        # Invalid Plans
        invalid_plans = \
        [
            Plan(name="my_plan!", description=None, services=None, tasks=None), # Special character in name
            Plan(name="a" * 51  , description=None, services=None, tasks=None), # Name exceeds 50 characters
            Plan(name="my_plan" , description="a" * 141, services=None, tasks=None), # Description exceeds 140 characters
            Plan(name="my_plan" , description=None, services=['BadService'], tasks=None), # Non existant service
            Plan(name="my_plan" , description=None, services=1, tasks=None), # The service is a number
            Plan(name="my_plan" , description=None, services="Bad", tasks=None), # The service is a string
            Plan(name="my_plan" , description=None, services=[1], tasks=None), # The service is a list of numbers
            Plan(name="my_plan" , description=1, services="Bad", tasks=None), # The description is a number
            "It's not even a plan!", # A Plan that not valid JSON
        ]

        # Add invalid Plans and expect them to fail
        for plan in invalid_plans:
            self.assertEqual(self.create_plan(plan.name if isinstance(plan, Plan) else "my_plan", http_info=True, body=plan)[1], 400)

        # Testing an empty plan name throws a 404 as the endpoint does not exist
        self.assertEqual(self.create_plan("", http_info=True, body=Plan(name="", description=None, services=None, tasks=None))[1], 404)

        # Define a generic schedule for backups and merges
        generic_backup_schedule = TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00", start_now=False)
        generic_merge_schedule = TaskTemplateSchedule(job_type="MERGE", frequency=1, period="HOURS", time="22:00", start_now=False)

        # Invalid Tasks
        invalid_tasks = \
        [
            TaskTemplate(name="my_task!", task_type="BACKUP",  schedule=generic_backup_schedule, merge_options=None, full_backup=None), # Special character in name
            TaskTemplate(name="my_task", task_type="BAD", schedule=generic_backup_schedule, merge_options=None, full_backup=None), # Non-existing task type
            TaskTemplate(name="my_task", task_type="", schedule=generic_backup_schedule, merge_options=None, full_backup=None), # Empty task type
            TaskTemplate(name="my_task", task_type=None, schedule=generic_backup_schedule, merge_options=None, full_backup=None), # Missing task type
            TaskTemplate(name="", task_type="BACKUP", schedule=generic_backup_schedule, merge_options=None, full_backup=None), # Name is empty
            TaskTemplate(name=None, task_type="BACKUP", schedule=generic_backup_schedule, merge_options=None, full_backup=None), # Name is missing
            TaskTemplate(name="my_task", task_type="BACKUP", schedule=None), # Missing schedule
            TaskTemplate(name="my_task", task_type="BACKUP", schedule=generic_backup_schedule, merge_options=None, full_backup="Bad"), # Full backup is not a bool
        ]

        # Add invalid Tasks and expect them to fail
        for task in invalid_tasks:
            # Add plan with an invalid Task (Should fail)
            self.assertEqual(self.create_plan("my_plan", http_info=True, body=Plan(name="my_plan", tasks=[task]))[1], 400)

        # Invalid Schedules for Backups
        invalid_backup_schedules = \
        [
            TaskTemplateSchedule(job_type="BACKUP", frequency=-1, period="HOURS", time="22:00", start_now=False), # Negative freq
            TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="BAD", time="22:00", start_now=False), # Non-existant period
            TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00:23", start_now=False), # Add seconds to time field
            TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="25:00", start_now=False), # Invalid time
            TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="-4:00", start_now=False), # Invalid time
            TaskTemplateSchedule(job_type="BACKUP", frequency=5001, period="HOURS", time="22:00", start_now=False), # Large Frequency
            TaskTemplateSchedule(job_type="BAD", frequency=1, period="HOURS", time="22:00", start_now=False), # Non-existant job type
            TaskTemplateSchedule(job_type="BACKUP", period="HOURS", time="22:00", start_now=False), # Missing frequency
            TaskTemplateSchedule(job_type="BACKUP", frequency=1, time="22:00", start_now=False), # Missing period
            TaskTemplateSchedule(job_type="BACKUP", frequency="1", period="HOURS", time="22:00", start_now=False), # Frequency is a float
            TaskTemplateSchedule(job_type="BACKUP", frequency=1.0, period="HOURS", time="22:00", start_now=False), # Frequency is a string
        ]

        # Add invalid Schedules and expect them to fails
        for schedule in invalid_backup_schedules:
            # Add plan with an invalid Task (Should fail)
            self.assertEqual(self.create_plan("my_plan", http_info=True, body=Plan(name="my_plan", tasks=[TaskTemplate(name="my_task", task_type="BACKUP", schedule=schedule)]))[1], 400)

    def test_valid_plan(self):
        """ Test a user can add a valid plan.

        A user can Add/Update a Plan with Tasks containing containing valid types, schedules, merge options or full backup flag.

        1. Define a selection of valid Plans.
        2. Add the plan and check if the operation succeeds.
        """
        # Services
        services = ["data", "gsi", "cbas", "ft", "eventing","views"]

        # Valid schedule/tasks
        schedule = TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00", start_now=False)
        tasks = [TaskTemplate(name="my_task", task_type="BACKUP", schedule=schedule)]

        # Valid Plans
        valid_plans = \
        [
            Plan(name="my_plan", description=None, services=None, tasks=tasks), # A plan with a name
            Plan(name="a", description=None, services=None, tasks=tasks), # Name length equal to 1
            Plan(name="a" * 50  , description=None, services=None, tasks=tasks), # Name length equal to 50
            Plan(name="my_plan", description="A plan with a description", services=None, tasks=tasks), # A plan with a description
            Plan(name="my_plan" , description="a", services=None, tasks=tasks), # Description length equal to 1 character
            Plan(name="my_plan" , description="a" * 140, services=None, tasks=tasks), # Description length equal to 140 characters
            Plan(name="my_plan" , description=None, services=services, tasks=tasks), # All services
        ]

        # Individual services
        for service in services:
            valid_plans.append(Plan(name="my_plan" , description=None, services=[service], tasks=tasks)), # Non existant service

        # 10 random subset of services of size 3
        for i in range(0, 10):
            random.sample(services, 3)

        # Add valid Plans and expect them to succeed
        for plan in valid_plans:
            # Add plan
            self.assertEqual(self.create_plan(plan.name, http_info=True, body=plan)[1], 200)

            self.delete_plan(plan.name)

         # Define a generic schedule for backups and merges
        generic_backup_schedule = TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00", start_now=False)
        generic_merge_schedule = TaskTemplateSchedule(job_type="MERGE", frequency=1, period="HOURS", time="22:00", start_now=False)

        # Valid Tasks
        valid_tasks = \
        [
            TaskTemplate(name="my_task", task_type="BACKUP",  schedule=generic_backup_schedule, merge_options=None, full_backup=None), # A task with a name
            TaskTemplate(name="a", task_type="BACKUP", schedule=generic_backup_schedule, merge_options=None, full_backup=None), # Name length equal to 1
            TaskTemplate(name="a" * 50, task_type="BACKUP", schedule=generic_backup_schedule, merge_options=None, full_backup=None), # Name length equal to 50
            TaskTemplate(name="my_task", task_type="BACKUP", schedule=generic_backup_schedule, merge_options=None, full_backup=True), # Specify full back = True
            TaskTemplate(name="my_task", task_type="BACKUP", schedule=generic_backup_schedule, merge_options=None, full_backup=False), # Specify full back = False
        ]

        # Add valid Tasks and expect them to succeed
        for task in valid_tasks:
            # Add plan with an valid Task (Should succeed)
            self.assertEqual(self.create_plan("my_plan", http_info=True, body=Plan(name="my_plan", tasks=[task]))[1], 200)

            self.delete_plan("my_plan")

        periods = ["MINUTES", "HOURS", "DAYS", "WEEKS", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

        # Valid Schedules for Backups
        valid_backup_schedules = \
        [
            TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00", start_now=False), # A frequency of 1, an hourly period, a sensible time
            TaskTemplateSchedule(job_type="BACKUP", frequency=5000, period="HOURS", time="22:00", start_now=False), # A frequency of 5000
            TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="05:00", start_now=False), # A valid time
            TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00"), # Missing start_now
            TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", start_now=False), # Missing time
        ]

        # Add all periods
        for period in periods:
            TaskTemplateSchedule(job_type="BACKUP", frequency=1, period=period, time="22:00", start_now=False)

        # Add Valid Schedules and expect them to succeed
        for schedule in valid_backup_schedules:
            # Add plan with an valid Task (Should succeed)
            self.assertEqual(self.create_plan("my_plan", http_info=True, body=Plan(name="my_plan", tasks=[TaskTemplate(name="my_task", task_type="BACKUP", schedule=schedule)]))[1], 200)

            # Delete plan
            self.delete_plan("my_plan")

    def test_delete_plan(self):
        """ A user can delete a Plan not currently tied to a repository.

        1. Add a plan.
        2. Delete the plan and check if the operation succeeded.
        """
        plan_name = "my_plan"

        # Add plan
        self.create_plan(plan_name)

        # Retrieve and confirm plan exists
        plan = self.get_plan(plan_name)
        self.assertEqual(plan.name, plan_name)

        # Delete plan
        self.delete_plan(plan_name)

        # Check if plan has been successfully deleted
        self.assertTrue(plan_name not in [plan.name for plan in self.get_all_plans()])

    def test_delete_plan_tied_to_repository(self):
        """ A user cannot delete a Plan currently tied to a repository.

        1. Add a plan.
        2. Tie the plan to a repository.
        2. Delete the plan and check if the operation fails.
        """
        plan_name, repo_name = "my_plan", "my_repo"

        # Add plan
        self.create_plan(plan_name)

        # Retrieve and confirm plan exists
        plan = self.get_plan(plan_name)
        self.assertEqual(plan.name, plan_name)

        # Add repo and tie plan to repository
        self.create_repository(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None))

        # Delete plan (Should fail)
        self.assertEqual(self.delete_plan(plan_name, http_info=True)[1], 400)

        # Check if plan has not been successfully deleted
        self.assertTrue(plan_name in [plan.name for plan in self.get_all_plans()])

    def test_plan_task_limits(self):
        """ Test the min and max limits on task creation

        Create a plan with no Tasks and expect the operation to fail.
        Create a plan with 1 Task and expect the operation to succeed.
        Create a plan with 14 Tasks and expect the operation to succeed.
        Create a plans with > 14 Tasks and expect the operation to fail.
        """
        generic_backup_schedule = TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00", start_now=False)

        def get_task(i):
            return TaskTemplate(name=f"my_task{i}", task_type="BACKUP", schedule=generic_backup_schedule, merge_options=None, full_backup=None)

        # Add 0, 1 and 14 Tasks in a Plan and expect it to succeed.
        for no_of_tasks in [1, 14]:
            # Add plan with an valid Task (Should succeed)
            self.assertEqual(self.create_plan("my_plan", http_info=True, body=Plan(name="my_plan", tasks=[get_task(i) for i in range(0, no_of_tasks)]))[1], 200)

            self.delete_plan("my_plan")

        # Add 15, 20 and 100 Tasks in a Plan and expect it to fail.
        for no_of_tasks in [0, 15, 20, 100]:
            # Add plan with an invalid Task (Should fail)
            self.assertEqual(self.create_plan("my_plan", http_info=True, body=Plan(name="my_plan", tasks=[get_task(i) for i in range(0, no_of_tasks)]))[1], 400)

    def test_attach_plan_to_repository(self):
        """ Tie a plan to n repositories and test the plan was attached successfully.

        1. Add a Plan "my_plan".
        2. Create n repositories with "my_plan".
        3. Retrieve n repositories and confirm all repositories are using "my_plan".
        """
        plan_name, no_of_repositories = "my_plan", 20

        # Add plan
        self.create_plan(plan_name)

        # Add repositories and tie plan to repository
        for i in range(0, no_of_repositories):
            self.create_repository(f"my_repo{i}", body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None))

        # Fetch active repositories
        repos = [repo for repo in self.get_repositories('active')]

        # Check there are no_of_repositories repositories
        self.assertEqual(len(repos), no_of_repositories)

        # Check all entries have the plan: plan_name
        self.assertEqual(set(repo.plan_name for repo in repos), set([plan_name]))

    def test_large_number_of_plans(self):
        """ Add a large number of plans with the maximum number of Tasks.

        Add 100 plans each with 14 Tasks.
        """
        generic_backup_schedule = TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00", start_now=False)

        def get_task(i):
            return TaskTemplate(name=f"my_task{i}", task_type="BACKUP", schedule=generic_backup_schedule, merge_options=None, full_backup=None)

        no_of_plans, no_of_tasks = 100, 14

        # Generate no_of_tasks tasks
        tasks = [get_task(i) for i in range(0, no_of_tasks)]

        # Add no_of_plans plans with no_of_tasks tasks
        for i in range(0, no_of_plans):
            # Add plan with an valid Task (Should succeed)
            self.assertEqual(self.create_plan(f"my_plan{i}", http_info=True, body=Plan(name=f"my_plan{i}", tasks=tasks))[1], 200)

        # Delete plans
        for i in range(0, no_of_plans):
            self.delete_plan(f"my_plan{i}")

    def test_create_plans_with_same_name(self):
        """ Add a plan, then add a plan with the same name and check if the operation fails.
        """
        # Create a plan
        plan_name = "my_plan"
        self.create_plan(plan_name)

        # Add plan with the same the name and expect it fail.
        try:
            self.create_plan(plan_name)
        except Exception as e:
            if e.status != 400:
                self.fail(f"Output of plan create was {e.status}")

    def test_deleted_plans_name_can_be_reused(self):
        """ Add a plan, delete the plan and check if its name can be reused.
        """
        plan_name = "my_plan"

        # Add plan
        self.create_plan(plan_name)

        # Delete plan
        self.delete_plan(plan_name)

        # Add plan with the same name and expect it to succeed.
        self.create_plan(plan_name)

    def test_creating_plan_simultaneously_on_two_nodes(self):
        """ Test consistent metadata by creating a plan on two nodes simultaneously and expecting one of the operations failing.

        Test consistent metadata by testing that creating a plan on two nodes simultaneously and expecting one of the operations to fail.
        """
        # Create a plan
        plan_name = "my_plan"

        # Define distinct schedules
        schedules = []
        schedules.append(TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="15:00", start_now=False))
        schedules.append(TaskTemplateSchedule(job_type="BACKUP", frequency=2, period="DAYS", time="16:00", start_now=False))

        # Make create_plan global so it works with multiprocessing.Pool
        global create_plan

        def create_plan(target_schedule):
            """Create a plan on the specified server"""
            target, schedule = target_schedule

            # Rest API Configuration
            configuration = HttpConfigurationFactory(target).create_configuration()

            self.sys_log_count[Tag.PLAN_CREATED] += 1
            _, status, _, response_data = PlanApi(ApiClient(configuration)).plan_name_post_with_http_info(plan_name,  body=Plan(name=plan_name,tasks=[TaskTemplate(name=f"my_task{target.ip.split('.')[-1]}", task_type="BACKUP", schedule=schedule)]))
            return status

        # Create repos on two different servers simultaneously
        with Pool(2) as p:
            result = p.map(create_plan, zip(self.input.clusters[0][:2], schedules))

        # Expect that the operation succeeds on at least one server
        self.assertIn(200, result)

    # Backup Repository Testing

    def test_correct_buckets_backed_up(self):
        """ Test the correct buckets are backed up.

        Params:
            all_buckets (bool): If True triggers a task that backups all buckets. Otherwise randomly picks a bucket.
            standard_buckets (int): The number of buckets to create.

        *** Previously used sasl_buckets, now moved to standard_buckets ***

        1. Create a Plan with a Backup Task.
        2. Add Repo and tie Plan to Repo.
        3. Perform a one off backup Task.
        4. Wait for Task to Complete.
        5. Check if the expected buckets were backed up by using the Info endpoint.
        """
        all_buckets = self.input.param("all_buckets", False)

        if len(self.buckets) < 2:
            self.fail("At least 2 buckets are required for this test, set test param standard_buckets=2")

        repo_name, plan_name, task_name = "my_repo", "my_plan", "my_task"

        # The buckets to backup (either all buckets or 1 bucket)
        buckets = set([bucket.name for bucket in self.buckets] if all_buckets else [random.choice(self.buckets).name])

        # Create a Backup Task
        my_task = TaskTemplate(name=task_name,
                               task_type="BACKUP",
                               schedule=TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", start_now=False))

        # Add plan
        self.create_plan(plan_name, body=Plan(tasks=[my_task]))

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None if all_buckets else next(iter(buckets))))

        # Perform a one off backup
        task_name = self.take_one_off_backup_with_name(repo_name).task_name

        # Wait until backup task has completed
        self.assertTrue(self.wait_for_backup_task("active", repo_name, 20 * len(buckets), 5, task_name))

        # Collect the buckets backed up through the info endpoint
        buckets_backed_up = set(bucket.name for bucket in self.get_backups("active", repo_name)[0].buckets)

        # Check if the expected buckets were backed up
        self.assertEqual(buckets_backed_up, buckets, f"The selected buckets '{buckets.difference(buckets_backed_up)}' were not backed up")

    def test_scheduled_merge(self):
        """ Test a scheduled merge is successful

        Params:
            offset (bool): If True, an offset is added to the Merge task. Otherwise no offset is used.

        *** scheduled merges using offsets always fail in 7.6.0-2176. see MB-61076 ***
        *** fixed in 7.6.0-2181 ***

        1.  Create Backup & Merge schedules with 1 & 2 minute intervals respectively.
        2.  Add schedules to respective tasks + add offset if necessary.
        3.  Create a plan with both tasks.
        4.  Create a repository using that plan.
        5.  Wait for the first merge to complete.
        6.  Fail if this merge doesn't have the 'done' status.

        """
        offset = self.input.param("offset", False)

        backup_schedule = TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="MINUTES", start_now=True)
        merge_schedule = TaskTemplateSchedule(job_type="MERGE", frequency=2, period="MINUTES", start_now=False)

        backup_task = TaskTemplate(name="backup_task", task_type="BACKUP", schedule=backup_schedule)
        merge_task = TaskTemplate(name="merge_task", task_type="MERGE", schedule=merge_schedule)

        if offset:
            merge_task.merge_options = TaskTemplateOptions(offset_start=0, offset_end=0)

        self.create_plan(plan_name="test_plan", body=Plan(tasks=[backup_task, merge_task]))

        self.create_repository(repo_name="test_repo", body=Body2(plan="test_plan", archive=self.backupset.directory))

        mergeTaskDone = self.wait_for_task("test_repo", merge_task.name, one_off=False)
        self.assertEqual(mergeTaskDone, True, "Scheduled merge not successful")

    def test_invalid_repository(self):
        """ Test a user cannot add an invalid repository.

        A user cannot Add/Update a repository containing invalid fields.

        Invalid entries include negative, out of range, non-existing values, empty/long, missing, illegal characters, non-unique names, non-existing buckets.

        1. Define a selection of invalid repositories.
        2. Add the repository and check if the operation fails.
        """
        plan_name = random.choice(self.default_plans)

        invalid_repositories = \
        [
            ("my_repo!", Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None)), # Special character in name
            ("my_repo",  Body2(plan=plan_name, archive="", bucket_name=None)), # Empty archive name
            ("my_repo",  Body2(plan=plan_name, archive=None, bucket_name=None)), # Missing archive name
            ("",         Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None)), # Empty repository name
            ("a"*51,     Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None)), # Long repo name
            ("my_repo",  Body2(plan=plan_name, archive=self.backupset.directory, bucket_name="Bad")), # Non existing bucket name
            ("my_repo",  Body2(plan=plan_name, archive='a_relative_path', bucket_name=None)), # Relative path
        ]

        # Add invalid repositories
        for repo_name, body in invalid_repositories:
            # Add invalid repository and expect it to fail
            self.assertTrue(self.create_repository(repo_name, body=body, http_info=True)[1] in [400, 404], f"An invalid repository configuration was added {repo_name, body}.")
            # Delete repositories
            self.delete_all_repositories()

    def test_invalid_cloud_repository(self):
        """ Test a user cannot add an invalid cloud repository.

        A user cannot Add/Update a repository containing invalid fields.

        Params:
            objstore_provider (bool): If true, creates a cloud repository otherwise creates a filesystem repository.

        1. Define a selection of invalid repositories.
        2. Add the repository and check if the operation fails.
        """
        # This test requires an objstore_provider for Cloud testing
        self.assertIsNotNone(self.objstore_provider)

        plan_name, provider = random.choice(self.default_plans), self.objstore_provider

        id_, key, region, staging_directory, endpoint = provider.access_key_id, provider.secret_access_key, provider.region, provider.staging_directory, provider.endpoint

        # Note cannot test invalid credentials as LocalStack accepts any credentials
        invalid_bodies = \
        [
            Body2(plan=plan_name, archive=self.get_archive(prefix="bad://"), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_region=region, cloud_endpoint=endpoint), # Non-existant prefix
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=None, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_region=region, cloud_endpoint=endpoint), # Missing staging directory
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir="", cloud_credentials_id=id_, cloud_credentials_key=key, cloud_region=region, cloud_endpoint=endpoint), # Empty staging directory
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=None, cloud_credentials_key=key, cloud_region=region, cloud_endpoint=endpoint), # Missing username
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=None, cloud_region=region, cloud_endpoint=endpoint), # Missing password
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_region=None, cloud_endpoint=endpoint), # Missing region
        ]

        for invalid_body in invalid_bodies:
            invalid_body.cloud_force_path_style = True

        invalid_repositories = [("my_repo", invalid_body) for invalid_body in invalid_bodies]

        # Add invalid repositories
        for repo_name, body in invalid_repositories:
            # Add invalid repository and expect it to fail
            self.assertTrue(self.create_repository(repo_name, body=body, http_info=True, should_succeed=False, set_cloud_creds=False)[1] in [400, 404], f"An invalid repository configuration was added {repo_name, body}.")
            # Delete repositories
            self.delete_all_repositories()

    def test_valid_repository(self):
        """ Test a user can add a valid repository.

        A user can Add/Update a repository containing valid fields.

        Params:
            sasl_buckets (int): The number of buckets to create.

        1. Define a selection of valid repositories.
        2. Add the repository and check if the operation succeeds.
        """
        if len(self.buckets) < 1:
            self.fail("At least 1 bucket is required for this test, set test param sasl_buckets=1")

        plan_name, bucket_name = random.choice(self.default_plans), random.choice(self.buckets).name

        valid_repositories = \
        [
            ("my_repo", Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None)), # A repository with a name
            ("a"*1, Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None)), # Short repo name
            ("a"*50, Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None)), # Long repo name
            ("my_repo", Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=bucket_name)), # A repo with a bucket name
        ]

        # Add valid repositories
        for repo_name, body in valid_repositories:
            # Add invalid repository and expect it to fail
            self.assertEqual(self.create_repository(repo_name, body=body, http_info=True)[1], 200, f"A valid repository configuration could not be added {repo_name, body}.")
            # Delete repositories
            self.delete_all_repositories()

    def test_valid_cloud_repository(self):
        """ Test a user can add a valid cloud repository

        A user can Add/Update a cloud repository containing valid fields.

        Params:
            sasl_buckets (int): The number of buckets to create.
            objstore_provider (bool): If true, creates a cloud repository otherwise creates a filesystem repository.

        1. Define a selection of valid repositories.
        2. Add the repository and check if the operation succeeds.
        """
        if len(self.buckets) < 1:
            self.fail("At least 1 bucket is required for this test, set test param sasl_buckets=1")

        # This test requires an objstore_provider for Cloud testing
        self.assertIsNotNone(self.objstore_provider)

        plan_name, bucket_name, provider = random.choice(self.default_plans), random.choice(self.buckets).name, self.objstore_provider

        def get_archive(prefix=provider.schema_prefix(), bucket=provider.bucket, path="my_archive"):
            """ Returns a cloud archive path
            """
            return f"{prefix}{bucket}/{path}"

        id_, key, region, staging_directory, endpoint = provider.access_key_id, provider.secret_access_key, provider.region, provider.staging_directory, provider.endpoint

        # TODO: LocalStack cannot mock cloud_force_path_style=False
        valid_bodies = \
        [
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_region=region, cloud_endpoint=endpoint, cloud_force_path_style=True), # Just a repository in the cloud
            # Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_region=region, cloud_endpoint=endpoint, cloud_force_path_style=False), # Uncomment when the testing environment supports cloud_force_path_style=False
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_region=region, cloud_endpoint=endpoint, bucket_name=bucket_name, cloud_force_path_style=True), # A repo in the cloud with a bucket
        ]

        valid_repositories = [("my_repo", valid_body) for valid_body in valid_bodies]

        # Add valid repositories
        for repo_name, body in valid_repositories:
            # Add invalid repository and expect it to fail
            self.assertEqual(self.create_repository(repo_name, body=body, http_info=True)[1], 200, f"A valid repository configuration could not be added {repo_name, body}.")
            # Delete repositories
            self.delete_all_repositories()

    def test_pause_and_resume_repository(self):
        """ Test if a repository can be paused and resumed.

        1. Create a Plan which backs up every minute starting now.
        2. Tie Plan to Repo.
        3. Wait until backup task is completed.
        4. Pause the repository.
        5. Pause the paused repository
        6. Wait for 70 seconds and check no new backups have been started
        7. Resume a repository.
        8. Resume a resumed repository.
        """
        repo_name, plan_name, task_name = "my_repo", "my_plan", "my_task"

        # Create a Task which starts_now
        my_task = TaskTemplate(name=task_name,
                               task_type="BACKUP",
                               schedule=TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="MINUTES", start_now=True))

        # Create a Plan
        self.create_plan(plan_name, body=Plan(tasks=[my_task]))

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory))

        # Wait until backup task has completed
        self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 5))

        # Pause the repository and expect it to succeed
        self.pause_repository(repo_name, http_info=True)

        # Pause a paused repository
        self.pause_repository(repo_name, http_info=True)

        # Count current number of backups
        no_of_backups = len(self.get_backups("active", repo_name))

        # Sleep for 70 seconds
        self.sleep(70)

        # Check no backups were started after 70 seconds
        self.assertEqual(no_of_backups, len(self.get_backups("active", repo_name)))

        # Resume the repository and expect it to succeed
        self.assertEqual(self.resume_repository(repo_name, http_info=True)[1], 200)

        # Resume a resume repository
        self.assertEqual(self.resume_repository(repo_name, http_info=True)[1], 200)

    def test_apply_plan_to_repository(self):
        """ A backup plan can be applied to a repository.
        """
        repo_name, plan_name, task_name = "my_repo", "my_plan", "my_task"

        # Add plan
        self.create_plan(plan_name)

        # Add repo and tie plan to repository
        self.assertEqual(self.create_repository(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None), http_info=True)[1], 200)

    def test_importing_repository(self):
        """ A user can import a repository.

        Test a user can import a repository from the filesystem or from the cloud.

        params:
            objstore_provider (bool): If true, imports from the cloud otherwise imports from the filesystem.
        """
        # Create backup config using cbbackupmgr
        self.backup_create()

        # Take 3 backups using cbbackupmgr
        for i in range(3):
            self.backup_cluster()

        self.chown(self.backupset.directory)

        repo_name = "my_repo"
        body = Body6(id=repo_name, archive=self.backupset.directory, repo=self.backupset.name)

        if self.objstore_provider:
            self.set_cloud_credentials(body)
            self.chown(self.objstore_provider.staging_directory)

        # Import repository
        self.assertEqual(self.import_repository_with_name(http_info=True, body=body)[1], 200)

        # Get backups from imported repository
        imported_backups = [backup._date for backup in self.get_backups("imported", repo_name)]

        # Check the backup names on the filesystem are equal to the backups in the imported repository
        self.assertEqual(set(self.backups), set(imported_backups))

    def test_importing_sensitive_repository(self):
        """ A user can import a repository.

        Test a user cannot import a sensitive repository from the filesystem.
        """
        # Create backup config using cbbackupmgr
        self.backup_create()

        # Take 3 backups using cbbackupmgr
        for i in range(0, 3):
            self.backup_cluster()

        self.chown(self.directory_to_share, user="root", group="root")
        self.chmod("660", self.directory_to_share)

        repo_name = "my_repo"
        body = Body6(id=repo_name, archive=self.backupset.directory, repo=self.backupset.name)

        # Import repository
        self.assertEqual(self.import_repository_with_name(http_info=True, should_succeed=False, body=body)[1], 500)

    def test_importing_read_only_repository(self):
        """ Test a user can import a read only repository.
        """
        # Create backup config using cbbackupmgr
        self.backup_create()

        # Take 3 backups using cbbackupmgr
        for i in range(0, 3):
            self.backup_cluster()

        self.chmod("444", self.directory_to_share)

        body = Body6(id="my_repo", archive=self.backupset.directory, repo=self.backupset.name)

        # Import repository
        self.assertEqual(self.import_repository_with_name(http_info=True, should_succeed=False, body=body)[1], 500)

    def test_one_off_backup(self):
        """ Test one off backup

        params:
            full_backup (bool): If true tests a full backup
            objstore_provider (bool): If true, imports from the cloud otherwise imports from the filesystem.
        """
        repo_name, task_name = "my_repo", "my_task"

        full_backup = self.input.param("full_backup", False)

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)
            self.chown(self.objstore_provider.staging_directory)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Fetch repository data
        repository = self.get_repositories('active')[0]

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        # Sleep to ensure the bucket is populated with data
        self.sleep(10)

        # Perform a one off backup
        task_name = self.take_one_off_backup_with_name(repo_name, body=Body4(full_backup=full_backup)).task_name

        # Wait until task has completed
        self.assertTrue(self.wait_for_task(repo_name, task_name, timeout=400))

        # Configure repository name
        self.backupset.name = repository.repo

        backup_name = self.map_task_to_backup("active", repo_name, task_name)

        # Check backup content equals content of backup cluster
        self.perform_backup_data_validation(self.backupset.backup_host, self.input.clusters[0], "ent-backup", self.num_items, backup_name)

    def test_one_off_incr_backup(self):
        """ Test one off backup full backup followed by one off incremental backup

        params:
            objstore_provider (bool): If true, imports from the cloud otherwise imports from the filesystem.
        """
        repo_name, task_name = "my_repo", "my_task"

        full_backup = self.input.param("full_backup", False)

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)
            self.chown(self.objstore_provider.staging_directory)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Fetch repository data
        repository = self.get_repositories('active')[0]

        def get_blob_gen(i):
            return BlobGenerator("ent-backup", "ent-backup-", self.value_size * i, start=self.num_items * i, end=self.num_items * (i + 1))

        for i in range(0, 2):

            # Load buckets with data
            self._load_all_buckets(self.master, get_blob_gen(i), "create", 0)

            # Sleep to ensure the bucket is populated with data
            self.sleep(10)

            # Perform a one off backup
            task_name = self.take_one_off_backup_with_name(repo_name, body=Body4(full_backup= i < 1)).task_name

            # Wait until task has completed
            self.assertTrue(self.wait_for_task(repo_name, task_name, timeout=400))

            # Configure repository name
            self.backupset.name = repository.repo

            backup_name = self.map_task_to_backup("active", repo_name, task_name)

            filter_keys = set(key for key, val in get_blob_gen(i))

            # Check backup content equals content of backup cluster
            self.perform_backup_data_validation(self.backupset.backup_host, self.input.clusters[0], "ent-backup",
                    self.num_items, backup_name, filter_keys=filter_keys)

    def test_one_off_merge(self):
        """ Test one off merge
        """
        repo_name, task_name = "my_repo", "my_task"

        full_backup = self.input.param("full_backup", False)

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        no_of_backups, subtrahend_for_no_of_backups = 4, 2

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)
            self.chown(self.objstore_provider.staging_directory)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Fetch repository data
        repository = self.get_repositories('active')[0]

        def get_blob_gen(start_index, end_index):
            return BlobGenerator("ent-backup", "ent-backup-", self.value_size, start=self.num_items * start_index, end=self.num_items * (end_index + 1))

        for i in range(0, no_of_backups):
            # Load buckets with data
            self._load_all_buckets(self.master, get_blob_gen(i, i), "create", 0)

            # Sleep to ensure the bucket is populated with data
            self.sleep(10)

            # Perform a one off backup
            task_name = self.take_one_off_backup_with_name(repo_name, body=Body4(full_backup= i < 1)).task_name

            # Wait until task has completed
            self.assertTrue(self.wait_for_task(repo_name, task_name, timeout=400))

        backups = [backup._date for backup in self.get_backups("active", repo_name)]

        task_name = self.take_one_off_merge_with_name(repo_name, body=Body5(start=backups[0], end=backups[no_of_backups - subtrahend_for_no_of_backups - 1], data_range="")).task_name

        self.assertTrue(self.wait_for_task(repo_name, task_name, timeout=800))

        backup_name = self.map_task_to_backup("active", repo_name, task_name)

        filter_keys = set(key for key, val in get_blob_gen(0, no_of_backups - 1 - subtrahend_for_no_of_backups))

        # Configure repository name
        self.backupset.name = repository.repo

        backups = [backup._date for backup in self.get_backups("active", repo_name)]

        # Check backup content equals content of backup cluster
        self.perform_backup_data_validation(self.backupset.backup_host, self.input.clusters[0], "ent-backup",
                self.num_items, backup_name, filter_keys=filter_keys)

        backups = [backup._date for backup in self.get_backups("active", repo_name)]
        self.assertEqual(len(backups) - 1, no_of_backups - subtrahend_for_no_of_backups)

    def test_one_off_cloud_merge_fails(self):
        """ Should not be able to merge with cloud options
        """
        repo_name, plan_name, task_name = "my_repo", "my_plan", "my_task"

        # Create a Backup Task
        my_task = TaskTemplate(name=task_name,
                               task_type="BACKUP",
                               schedule=TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", start_now=False))

        # Add plan
        self.create_plan(plan_name, body=Plan(tasks=[my_task]))

        # Create body for creating an active repository
        body = Body2(plan=plan_name, archive=self.backupset.directory)

        # An objstore provider is required
        self.assertIsNotNone(self.objstore_provider)

        # Manually create staging directory with permissions
        self.create_staging_directory()

        # Set cloud credentials
        self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Take backups
        for i in range(0, 6):
            # Perform a one off backup
            task_name = self.take_one_off_backup_with_name(repo_name).task_name
            # Wait until task has completed
            self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 5, task_name=task_name))

        backups = [backup._date for backup in self.get_backups("active", repo_name)]

        self.assertEqual(self.take_one_off_merge_with_name(repo_name, http_info=True, valid_call=False, body=Body5(start=backups[0], end=backups[len(backups) - 2]))[1], 500)

    def test_one_off_restore(self):
        """ Test one off restore
        """
        repo_name, task_name = "my_repo", "my_task"

        full_backup = self.input.param("full_backup", False)

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)
            self.chown(self.objstore_provider.staging_directory)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Fetch repository data
        repository = self.get_repositories('active')[0]

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        # Sleep to ensure the bucket is populated with data
        self.sleep(10)

        # Perform a one off backup
        task_name = self.take_one_off_backup_with_name(repo_name, body=Body4(full_backup=full_backup)).task_name

        # Wait until task has completed
        self.assertTrue(self.wait_for_task(repo_name, task_name, timeout=400))

        # Get backup name
        backup_name = self.map_task_to_backup("active", repo_name, task_name)

        # Flush all buckets to empty cluster of all documents
        rest_connection = RestConnection(self.master)
        for bucket in self.buckets:
            rest_connection.change_bucket_props(bucket, flushEnabled=1)
            rest_connection.flush_bucket(bucket)

        # Configure repository name
        self.backupset.name = repository.repo

        cluster_host = self.master
        protocol = "https://" if cluster_host.port == "18091" else ""
        body = Body1(target=f"{protocol}{cluster_host.ip}:{cluster_host.port}", user=cluster_host.rest_username, password=cluster_host.rest_password)
        # Perform a restore
        task_name = self.take_one_off_restore_with_name("active", repo_name, body=body).task_name
        # Wait until task has completed
        self.assertTrue(self.wait_for_task(repo_name, task_name))

        # Check backup content equals content of cluster after restore
        self.perform_backup_data_validation(self.backupset.backup_host, self.input.clusters[0], "ent-backup", self.num_items, backup_name)

    def test_invalid_tasks(self):
        """ Merging/Restoring backups that do not exist.

        Merging backups that do not exist

        Params:
            sasl_buckets (int): The number of buckets to create.
            objstore_provider (bool): If true, imports from the cloud otherwise imports from the filesystem.
        """
        repo_name, plan_name, task_name = "my_repo", "my_plan", "my_task"

        # Create a Backup Task
        my_task = TaskTemplate(name=task_name,
                               task_type="BACKUP",
                               schedule=TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", start_now=False))

        # Add plan
        self.create_plan(plan_name, body=Plan(tasks=[my_task]))

        body = Body2(plan=plan_name, archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Take backups
        for i in range(0, 6):
            # Perform a one off backup
            task_name = self.take_one_off_backup_with_name(repo_name).task_name
            self.sleep(3)
            # Wait until task has completed
            self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 5, task_name=task_name))

        backups = [backup._date for backup in self.get_backups("active", repo_name)]

        if not self.objstore_provider:
        # Start a merge task, this succeeds (don't know if this is a bug)
            task_name = self.take_one_off_merge_with_name(repo_name, should_succeed=False, body=Body5(start="Bad Start Point", end="Bad End Point")).task_name
            self.sleep(3)

            # Check if the merge task failed with a sensible warning message delivered to the user (TODO MB-41565)
            self.assertEqual(self.get_task_history("active", "my_repo", task_name=task_name)[0].status, "failed")

        body = Body1(target="http://127.0.0.1/", user="Administrator", password="password", start="Bad Start Point", end="Bad End Point")

        # Perform a restore
        self.assertEqual(self.take_one_off_restore_with_name("active", repo_name, http_info=True, valid_call=False)[1], 400)

    def test_user_warned_if_bucket_no_longer_exists(self):
        """ Test user warned adequately warned if the target bucket(s) no longer exist.

        Params:
            sasl_buckets (int): The number of buckets to create.
            objstore_provider (bool): If true, imports from the cloud otherwise imports from the filesystem.
        """
        repo_name, plan_name, task_name, bucket_name = "my_repo", "my_plan", "my_task", random.choice(self.buckets).name

        # Create a Backup Task
        my_task = TaskTemplate(name=task_name,
                               task_type="BACKUP",
                               schedule=TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", start_now=False))

        # Add plan
        self.create_plan(plan_name, body=Plan(tasks=[my_task]))

        body = Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=bucket_name)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)
            self.create_staging_directory()

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Delete all buckets
        RestConnection(self.backupset.cluster_host).delete_all_buckets()

        # Perform a one off backup
        _, status, _, response_data = self.take_one_off_backup_with_name(repo_name, http_info=True, valid_call=False)

        # Load as json
        data = json.loads(response_data.data)

        # Check status and warning message
        self.assertEqual(status, 500)
        self.assertEqual(data['msg'], "Could not send task")
        self.assertEqual(data['extras'], f"failed bucket check for bucket '{bucket_name}': element not found")

    def test_archived_repository(self):
        """ Test moving between active to archived

        Active to Archived
        """
        repo_name = "my_repo"

        # Add repo and tie plan to repository
        self.create_repository(repo_name, body=Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory, bucket_name=None))

        # Archive repository
        self.assertEqual(self.archive_repository(repo_name, http_info=True, body=Body3(id=repo_name))[1], 200)

        # Fetch repository
        repository = self.get_repositories('archived')[0]

        # Check the archived repository exists
        self.assertEqual((repository.id, repository.state), (repo_name, "archived"))

    def test_delete_archived_repository(self):
        """ Test a user can delete an archived repository

        params:
            remove_repository (bool): If set, checks the file is removed from the filesystem.
            objstore_provider (bool): If true, imports from the cloud otherwise imports from the filesystem.
        """
        repo_name, remove_repository = "my_repo", self.input.param("remove_repository", False)

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            self.set_cloud_credentials(body)

        # Add repo and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Archive repository
        self.assertEqual(self.archive_repository(repo_name, http_info=True, body=Body3(id=repo_name))[1], 200)

        # Fetch repository
        repository = self.get_repositories('archived')[0]

        # Check the archived repository exists
        self.assertEqual((repository.id, repository.state), (repo_name, "archived"))

        # Delete archived repository
        self.delete_repository('archived', repo_name, http_info=True, remove_repository=remove_repository)

        # Check there are no archived repositories
        self.assertEqual(len(self.get_repositories('archived')), 0)

        # Fetch repo info
        self.backupset.name, self.backupset.objstore_staging_directory = repository.repo, self.backupset.objstore_alternative_staging_directory
        output, _ = self.backup_info()

        if remove_repository:
            # Check the repository does not exist on the filesystem
            self.assertTrue(f"Backup Repository `{repository.repo}` not found" in output[0])
        else:
            # Check the repositry exists on the filesystem
            self.assertEqual(json.loads(output[0])['name'], repository.repo)


    def test_delete_active_repository(self):
        """ Test a user cannot delete an active repository
        """
        repo_name, plan_name, = "my_repo", "my_plan"

        # Add plan
        self.create_plan(plan_name)

        # Retrieve and confirm plan exists
        plan = self.get_plan(plan_name)
        self.assertEqual(plan.name, plan_name)

        # Add repo and tie plan to repository
        self.create_repository(repo_name, plan_name)

        # Delete archived repository
        self.assertEqual(self.delete_repository('active', repo_name, http_info=True, remove_repository=False)[1], 400)

    def test_user_can_history_info_examine_restore_from_archived_repository(self):
        """ Test if a user can task history/info/examine/restore from an archived repository.
        """
        repo_name, task_name = "my_repo", "my_task"

        full_backup = self.input.param("full_backup", False)

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)
            self.chown(self.objstore_provider.staging_directory)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Fetch repository data
        repository = self.get_repositories('active')[0]

        # Configure repository name and staging directory
        if self.objstore_provider:
            self.backupset.objstore_staging_directory = repository.cloud_info.staging_dir
        self.backupset.name = repository.repo

        def get_blob_gen(i):
            return DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100 * i, 100 * (i + 1))), start=0, end=self.num_items)

        backup_names = []

        for i in range(0, 2):
            # Load buckets with data
            self._load_all_buckets(self.master, get_blob_gen(i), "create", 0)

            # Sleep to ensure the bucket is populated with data
            self.sleep(10)

            # Perform a one off backup
            task_name = self.take_one_off_backup_with_name(repo_name, body=Body4(full_backup= i < 1)).task_name

            # Wait until task has completed
            self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 20, task_name=task_name))

            backup_names.append(self.map_task_to_backup("active", repo_name, task_name))

        # Archive repository
        self.assertEqual(self.archive_repository(repo_name, http_info=True, body=Body3(id=repo_name))[1], 200)

        # Check if the task history can be fetched from an archived repo
        task_history = self.get_task_history("archived", repo_name)
        self.assertEqual(len(task_history), 2)

        # Check if the info can be fetched from an archived repo
        backups = self.get_backups("archived", repo_name)
        self.assertEqual(len(backups), 2)

        # Check user can examine an archived repo
        _, status, _, response_data = self.examine_repository("archived", repo_name, http_info=True, body=Body("test_docs-0", "default"))
        self.assertEqual(status, 200)
        data = [json.loads(json_object) for json_object in response_data.data.rstrip().split("\n")]
        self.assertEqual(data[0][0]['document']['value']['age'], 0)
        self.assertEqual(data[0][1]['document']['value']['age'], 100)

        # Flush all buckets to empty cluster of all documents
        rest_connection = RestConnection(self.master)
        for bucket in self.buckets:
            rest_connection.change_bucket_props(bucket, flushEnabled=1)
            rest_connection.flush_bucket(bucket)

        cluster_host = self.master
        body = Body1(target=f"{cluster_host.ip}:{cluster_host.port}", user=cluster_host.rest_username, password=cluster_host.rest_password)

        # Perform a restore
        task_name = self.take_one_off_restore_with_name("archived", repo_name, body=body).task_name

        # Wait until task has completed
        self.assertTrue(self.wait_for_backup_task("archived", repo_name, 20, 5))

        # Check the restore task succeeded
        task = self.get_task_history("archived", repo_name, task_name=task_name)[0]
        self.assertEqual(task.status, 'done')
        self.assertIsNone(task.node_runs[0].error)
        self.assertEqual(len(task.node_runs[0].stats.transfers), 2)
        self.assertEqual(set(transfer.backup for transfer in task.node_runs[0].stats.transfers), set(backup_names))

    def test_manually_deleting_archive_location(self):
        """ Test manually deleting archive location gives adequate warning.
        """
        repo_name = "my_repo"

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory, bucket_name=random.choice(self.buckets).name)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)
            self.create_staging_directory()

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        remote_client = RemoteMachineShellConnection(self.master)
        if self.objstore_provider:
            self.objstore_provider.teardown("", remote_client)
        else:
            remote_client.execute_command(f"rm -rf {self.backupset.directory}")
        remote_client.disconnect()

        _, status, _, response_data = self.get_backups("active", repo_name, http_info=True)

        # Load as json
        data = json.loads(response_data.data)

        # Check status and warning message
        self.assertEqual(status, 500)
        self.assertEqual(data['msg'], "Could not get repository info")
        self.assertIn(f"Error opening archive", data['extras'])

    def test_manually_deleting_staging_directory(self):
        """ Test manually deleting staging directory recreates staging directory.

        Params:
            objstore_provider (bool): If true, imports from the cloud otherwise imports from the filesystem.
        """
        repo_name = "my_repo"

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory, bucket_name=random.choice(self.buckets).name)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)
            self.create_staging_directory()

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        self.assertIsNotNone(self.objstore_provider)

        remote_client = RemoteMachineShellConnection(self.master)
        remote_client.execute_command(f"rm -rf {self.objstore_provider.staging_directory}")
        remote_client.disconnect()

        self.assertEqual(self.get_backups("active", repo_name, http_info=True)[1], 200)

    def test_bad_archive_locations(self):
        """ Test bad archive locations.
        """
        archive_locations = \
            [
                ("/etc/shadow", None, None),
                ("/tmp/non_empty", ("couchbase", "couchbase"), "777"),
                ("/tmp/non_empty/just_a_file", ("couchbase", "couchbase"), "777"),
                ("/tmp/non_accessible", ("couchbase", "couchbase"), "000"),
            ]

        remote_client = RemoteMachineShellConnection(self.master)

        self.temporary_directories.append("/tmp/non_empty")
        self.temporary_directories.append("/tmp/non_accessible")

        remote_client.execute_command(f"mkdir -p /tmp/non_empty /tmp/non_accessible {self.backupset.directory}")
        remote_client.execute_command("touch /tmp/non_empty/just_a_file")

        for archive, user_group, permission in archive_locations:

            if user_group:
                self.chown(archive, user=user_group[0], group=user_group[1])

            if permission:
                self.chmod(permission, archive)

            # Create body
            body = Body2(plan=random.choice(self.default_plans), archive=archive)

            # Create repository
            self.assertEqual(self.create_repository("my_repo", body=body, http_info=True, should_succeed=False)[1], 500, f"Created archive in bad location {archive}")

        remote_client.disconnect()

    def test_health_indicators(self):
        """ Test health indicators
        """
        task_name, plan_name, repo_name = "task_name", "plan_name", "my_repo"

        def delete_the_target_bucket():
            # Delete all buckets
            RestConnection(self.backupset.cluster_host).delete_all_buckets()
            self.sleep(60)

        # [(Health Issue, Healthy, Action Function), ..]
        health_actions = \
        [
            (None, True, None),
            (None, True, self.pause_repository),
            ("cannot verify bucket exists: element not found", False, delete_the_target_bucket)
        ]

        # Create a Backup Task
        my_task = TaskTemplate(name=task_name,
                               task_type="BACKUP",
                               schedule=TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="MINUTES", start_now=True))

        # Add plan
        self.create_plan(plan_name, body=Plan(tasks=[my_task]))

        for health_issue, healthy, action in health_actions:

            # Create repository
            self.create_repository(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=random.choice(self.buckets).name), http_info=True)

            if action:
                action()

            repository_health = self.get_repositories('active')[0].health

            self.assertEqual(healthy, repository_health.healthy)
            self.assertEqual(health_issue, repository_health.health_issue)

            self.delete_all_repositories()

    def test_creating_repo_simultaneously_on_two_nodes(self):
        """ Test consistent metadata by creating a repo on two nodes simultaneously and expecting one of the operations failing.

        Test consistent metadata by testing that creating a plan on two nodes simultaneously and expecting one of the operations to fail.
        """
        repo_name = "my_repo"

        # Make create_repo global so it works with multiprocessing.Pool
        global create_repo

        def create_repo(target):
            """Create a plan on the specified server"""

            # Rest API Configuration
            configuration = HttpConfigurationFactory(target).create_configuration()

            # Add repositories and tie plan to repository
            _, status, _, response_data = self.create_repository(repo_name, body=Body2(plan=self.default_plans[0], archive=f"{self.backupset.directory}/{target.ip}"), http_info=True)

            return status

        # Create plans on two different servers simultaneously
        with Pool(2) as p:
            result = p.map(create_repo, self.input.clusters[0][:2])

        # Expect that the operation didn't succeed on both servers
        self.assertNotEqual(result, [200, 200])

    # Examine / Info

    def test_user_can_see_backups(self):
        """ Test user can info repository
        """
        repo_name, task_name = "my_repo", "my_task"

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        def get_blob_gen(i):
            return DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100 * i, 100 * (i + 1))), start=0, end=self.num_items)

        self.take_n_one_off_backups("active", repo_name, get_blob_gen, 2)

        self.assertEqual(len(self.get_backups("active", repo_name)), 2)

    def test_user_can_delete_backups(self):
        """ Test user can delete backups
        """
        repo_name, task_name = "my_repo", "my_task"

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        def get_blob_gen(i):
            return DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100 * i, 100 * (i + 1))), start=0, end=self.num_items)

        self.take_n_one_off_backups("active", repo_name, get_blob_gen, 2)

        backups = self.get_backups("active", repo_name)
        self.assertEqual(len(backups), 2)

        backup_to_delete = random.choice(backups)._date
        self.assertEqual(self.delete_backup(repo_name, backup_to_delete, http_info=True)[1], 200)

        backups = self.get_backups("active", repo_name)
        self.assertEqual(len(backups), 1)

    def test_user_cannot_delete_backup_that_does_not_exist(self):
        """ Test a user cannot delete a backup that does not exist
        """
        repo_name, task_name = "my_repo", "my_task"

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        def get_blob_gen(i):
            return DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100 * i, 100 * (i + 1))), start=0, end=self.num_items)

        self.take_n_one_off_backups("active", repo_name, get_blob_gen, 2)

        backups = self.get_backups("active", repo_name)
        self.assertEqual(len(backups), 2)

        backup_to_delete = "Backup"
        self.assertEqual(self.delete_backup(repo_name, backup_to_delete, http_info=True)[1], 500)

        backups = self.get_backups("active", repo_name)
        self.assertEqual(len(backups), 2)

    def test_user_can_observe_document_changing(self):
        """ Test user can observe a document changing between backups
        """
        repo_name, task_name = "my_repo", "my_task"

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        def get_blob_gen(i):
            return DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100 * i, 100 * (i + 1))), start=0, end=self.num_items)

        self.take_n_one_off_backups("active", repo_name, get_blob_gen, 2)

        backups = self.get_backups("active", repo_name)
        self.assertEqual(len(backups), 2)

        # Check user can examine repo
        _, status, _, response_data = self.examine_repository("active", repo_name, http_info=True, body=Body("test_docs-0", "default"))
        self.assertEqual(status, 200)
        data = [json.loads(json_object) for json_object in response_data.data.rstrip().split("\n")]

        self.assertEqual(data[0][0]['document']['value']['age'], 0)
        self.assertEqual(data[0][1]['document']['value']['age'], 100)

    def test_bucket_names_with_fullstops(self):
        """ Test a user can examine a bucket name in the form 'a.b.c.d'
        """
        repo_name, task_name, bucket_name = "my_repo", "my_task", "a.b.c.d"

        # Create `bucket_name`
        self.replace_bucket(self.backupset.cluster_host, "default", bucket_name)

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        def get_blob_gen(i):
            return DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100 * i, 100 * (i + 1))), start=0, end=10)

        self.take_n_one_off_backups("active", repo_name, get_blob_gen, 2)

        backups = self.get_backups("active", repo_name)
        self.assertEqual(len(backups), 2)

        # Check user can examine repo with a bucket name containing fullstops which have been escaped
        _, status, _, response_data = self.examine_repository("active", repo_name, http_info=True, body=Body("test_docs-0", "a\\.b\\.c\\.d"))
        self.assertEqual(status, 200)
        data = [json.loads(json_object) for json_object in response_data.data.rstrip().split("\n")]
        self.assertEqual(data[0][0]['document']['value']['age'], 0)
        self.assertEqual(data[0][1]['document']['value']['age'], 100)

    def test_collection_unaware_backups(self):
        """ Test collection unaware backups
        """
        repo_name, task_name, bucket_name, scope_name, collection_name, no_of_backups = "my_repo", "my_task", "bucket_name", "_default", "_default", 2

        # Create bucket `bucket_name`
        self.replace_bucket(self.backupset.cluster_host, "default", bucket_name)

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        def get_blob_gen(i):
            return DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100 * i, 100 * (i + 1))), start=0, end=10)

        self.take_n_one_off_backups("active", repo_name, get_blob_gen, no_of_backups)

        self.assertEqual(len(self.get_backups("active", repo_name)), no_of_backups)

        # Check user can examine collection-less backups using a data path consisting of a bucket name
        _, status, _, response_data = self.examine_repository("active", repo_name, http_info=True, body=Body("test_docs-0", bucket_name))
        self.assertEqual(status, 200)
        data = [json.loads(json_object) for json_object in response_data.data.rstrip().split("\n")]
        for i in range(no_of_backups):
            self.assertEqual(data[0][i]['document']['value']['age'], i * 100)

        # Check user can examine collection-less backups using a data path consisting of the ._default._default suffix
        _, status, _, response_data = self.examine_repository("active", repo_name, http_info=True, body=Body("test_docs-0", f"{bucket_name}.{scope_name}.{collection_name}"))
        self.assertEqual(status, 200)
        data = [json.loads(json_object) for json_object in response_data.data.rstrip().split("\n")]
        for i in range(no_of_backups):
            self.assertEqual(data[0][i]['document']['value']['age'], i * 100)

    def test_collection_aware_backups(self):
        """ Test collection aware backups
        """
        repo_name, task_name, bucket_name, scope_name, collection_name, no_of_backups = "my_repo", "my_task", "my_bucket", "my_scope", "my_collection", 2

        # Create bucket `bucket_name`
        self.replace_bucket(self.backupset.cluster_host, "default", bucket_name)

        # Create scope, collection and obtain collection id
        collection_id = self.create_scope_and_collection(self.backupset.cluster_host, bucket_name, scope_name, collection_name)

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        remote_shell = RemoteMachineShellConnection(self.backupset.cluster_host)
        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Load collection with data, increasing the body size by 1 each iteration
        def provision_with_data(i):
            """ Load collection with 10 documents, increasing the body size by 1 each iteration
            """
            remote_shell.execute_cbworkloadgen(self.backupset.cluster_host.rest_username, self.backupset.cluster_host.rest_password, 10, "1", bucket_name, 2 + i, f"-j -c {collection_id}")

        self.take_n_one_off_backups("active", repo_name, None, no_of_backups, data_provisioning_function=provision_with_data)

        self.assertEqual(len(self.get_backups("active", repo_name)), no_of_backups)

        # Check user can examine collection aware backups using a  data path consisting of bucket.collection.scope
        _, status, _, response_data = self.examine_repository("active", repo_name, http_info=True, body=Body("pymc0", f"{bucket_name}.{scope_name}.{collection_name}"))
        self.assertEqual(status, 200)
        data = [json.loads(json_object) for json_object in response_data.data.rstrip().split("\n")]
        for i in range(no_of_backups):
            self.assertEqual(data[0][i]['document']['value']['body'], '0' * (i + 2))

        # Check if user supplies only a bucket name to a collection aware bucket it fails
        _, status, _, response_data = self.examine_repository("active", repo_name, http_info=True, body=Body("pymc0", f"{bucket_name}"))
        self.assertEqual(status, 500)
        data = json.loads(response_data.data)
        self.assertEqual("Could not run examine command on repository", data["msg"])
        self.assertIn("examining a collection aware backup at the bucket or scope level is unsupported", data["extras"])

    # Monitor Tasks and Task History

    def test_task_history(self):
        """ Test a user can obtain their Task history.
        """
        repo_name, task_name = "my_repo", "my_task"

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        def get_blob_gen(i):
            return DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100 * i, 100 * (i + 1))), start=0, end=self.num_items)

        self.take_n_one_off_backups("active", repo_name, get_blob_gen, 2)

        self.assertEqual(len(self.get_task_history("active", repo_name)), 2)

    def test_large_task_history(self):
        """ Test user can obtain the history of a large quantity of backups.
        """
        repo_name, task_name, no_of_backups = "my_repo", "my_task", 100

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        self.take_n_one_off_backups("active", repo_name, None, no_of_backups, sleep_time=10)

        self.assertEqual(len(self.get_task_history("active", repo_name)), no_of_backups)

    # Not possible to trigger task history rotation, need somewhere between 1000 to 1,000,000
    def test_large_task_history_rotation(self):
        """ Test the task history is rotated
        """
        repo_name, task_name = "my_repo", "my_task"

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Pause the repository to prevent scheduled tasks from executing
        self.pause_repository(repo_name)

        no_of_backups = 5 # No of backups to take
        rotation_size = 5 # The rotation size in Mib
        size_in_bytes = rotation_size * (1024 ** 2) # The rotation size in bytes
        size_in_bytes = size_in_bytes + 100000 # Overflow history file by 100000 bytes over rotation size
        # Compute the entry size required such that if one more backup is added, the history rotates
        size_of_entry, remainder = int(size_in_bytes / no_of_backups), size_in_bytes % no_of_backups
        # Configure task history rotation
        self.configuration_api.config_post(body=ServiceConfiguration(history_rotation_period=1, history_rotation_size=rotation_size))

        # Create object to manipulate history file
        history = HistoryFile(self.backupset, self.get_hidden_repo_name(repo_name))

        def pad_last_entry(size_of_entry):
            """ Pad the last entry in task history file to `size_of_entry` bytes
            """
            last_entry = history.history_file_get_last_entry() + '\n'
            size_of_last_entry = len(last_entry)
            last_entry = json.loads(last_entry)
            last_entry['padding'] = 'a' * (size_of_entry - len(',"padding":""') - size_of_last_entry)
            last_entry = json.dumps(last_entry, separators=(',', ':')) + '\n'
            print(len(last_entry))
            history.history_file_delete_last_entry()
            history.history_file_append(last_entry)

        # List of tasks that should exist after task rotation
        remaining_tasks = [None] * no_of_backups

        # Take history file to tipping over point
        # We must rotate twice, since this is how many log files we retain
        for j in range(2):
            for i in range(0, no_of_backups):
                remaining_tasks[i] = self.take_one_off_backup("active", repo_name, i < 1, 20, 20)

                if i == no_of_backups - 1:
                    size_of_entry = size_of_entry + remainder

                if i < 1:
                    pad_last_entry(size_of_entry - len(history.history_file_get_timestamp() + '\n'))
                else:
                    pad_last_entry(size_of_entry)
            history.history_file = history.history_file[:-1] + "1"

        # Advance 2 days and take a backup to trigger history rotation (New contents over threshold get appended to next history file)
        self.time.change_system_time("+2 days")
        remaining_tasks.append(self.take_one_off_backup("active", repo_name, False, 20, 20))

        # Advance 2 days and take a backup to trigger old history file deletion (Old history file gets deleted)
        self.time.change_system_time("+2 days")
        remaining_tasks.append(self.take_one_off_backup("active", repo_name, False, 20, 20))

        task_history = self.get_task_history("active", repo_name)

        # Reverse list so newest task is first
        remaining_tasks.reverse()

        # Check the tasks we expected to remain exist
        self.assertEqual(remaining_tasks, [task.task_name for task in task_history])

    def test_task_history_offset_limit(self):
        """ Test the offset and limit filters in the task history
        """
        repo_name, task_name, no_of_backups = "my_repo", "my_task", 30

        offset_limit = \
        [
            (5, 5),
            (15, 9),
            (20, 20),
        ]

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        self.take_n_one_off_backups("active", repo_name, None, no_of_backups, sleep_time=10)

        task_history = self.get_task_history("active", repo_name)
        self.assertEqual(len(task_history), no_of_backups)

        for offset, limit in offset_limit:
            paginated_task_history = self.get_task_history("active", repo_name, offset=offset, limit=limit)
            self.assertEqual(task_history[offset: offset + limit], paginated_task_history)
            self.assertLessEqual(len(paginated_task_history), limit)

        # Cases for 0 offsets and limits
        self.assertEqual(len(self.get_task_history("active", repo_name, offset=0, limit=0)), no_of_backups)
        self.assertEqual(len(self.get_task_history("active", repo_name, offset=10, limit=0)), no_of_backups - 10)
        self.assertEqual(len(self.get_task_history("active", repo_name, offset=0, limit=10)), 10)

    def test_task_history_start_time(self):
        """ Test start time produces a list of tasks more recent than the start time and includes the start time
        """
        repo_name, task_name, no_of_backups = "my_repo", "my_task", 30

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        self.take_n_one_off_backups("active", repo_name, None, no_of_backups, sleep_time=10)

        start = random.choice(self.get_task_history("active", repo_name)).start

        task_start_times = [TimeUtil.rfc3339nano_to_datetime(task.start) for task in self.get_task_history("active", repo_name, first=start)]

        start = TimeUtil.rfc3339nano_to_datetime(start)

        # Check start is in the list of task_start_times
        self.assertIn(start, task_start_times)

        # The only thing that should be present in the task_start_times should be more recent or equal compared to start
        for time in task_start_times:
            self.assertLessEqual(start, time)

    def test_tasks_are_reverse_chronologically_ordered(self):
        """ Test tasks are ordered with new tasks at the start of the task history.
        """
        repo_name, task_name, no_of_backups = "my_repo", "my_task", 10

        body = Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        self.take_n_one_off_backups("active", repo_name, None, no_of_backups)

        task_start_times = [TimeUtil.rfc3339nano_to_datetime(task.start) for task in self.get_task_history("active", repo_name)]

        # Check the prev_time is more recent compared to the next_time
        for prev_time, next_time in zip(task_start_times, task_start_times[1:]):
            self.assertGreater(prev_time, next_time)

    # Shared directory

    def test_shared_directory(self):
        """ Test same archive path but the location is not shared

        params:
            lose_shared (str): If set unmounts 'some' or 'all' nodes lose shared access.
            permissions (str): If set changes permissions to 'ro' or 'wo'.
            leader_case (bool): If True, Non-leader nodes have read only access.
        """
        repo_name = "my_repo"
        lose_shared = self.input.param("lose_shared", None)
        permissions = self.input.param("permissions", None)
        leader_case = self.input.param("leader_case", False)

        self.create_repository_with_default_plan(repo_name)

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        # Sleep to ensure the bucket is populated with data
        self.sleep(10)

        # A list of clients that lose shared access in the `lose_share` and `some_nodes` case
        clients_that_lost_shared_access = []

        if lose_shared == 'some':
            # Pick a 2 random clients and unmount the archive location
            # Consequently, either the leader and the node lose shared access Or both non-leader nodes lose shared access.
            for client in random.sample(self.nfs_connection.clients, 2):
                client.clean(self.nfs_connection.directory_to_mount)
                clients_that_lost_shared_access.append(client)

        if lose_shared == 'all':
            # Unshare shared directory and unmount archive location for all nodes
            self.nfs_connection.clean()

        if permissions == 'wo':
            # Make shared directory write only
            self.chmod("222", self.directory_to_share)

        if permissions == 'ro':
            # Make shared directory read only
            self.chmod("444", self.directory_to_share)

        # Non-leader nodes have ro permissions
        if leader_case:
            leadernode = self.get_leader()
            privileges = {server.ip: 'rw' if server == leadernode else 'ro' for server in self.input.clusters[0]}
            self.nfs_connection.clean()
            self.nfs_connection.share(privileges)

        # Perform a one off backup
        _, status, _, response_data = self.take_one_off_backup_with_name(repo_name, http_info=True, should_succeed=False)

        # If the status is 500, that means that backup was not able to proceed due to
        # the detection of the unshared archive location in which case we can simply return.
        if status == 500:
            self.sys_log_count[Tag.BACKUP_STARTED] -= 1
            return

        # Check the task was started
        self.assertEqual(status, 200)

        # Sleep to give time for the task to fail and be removed
        self.sleep(60)

        # Fetch repository information
        repository = self.get_repository('active', repo_name)
        self.assertIsNone(repository.running_one_off)

        # Fetch the task history
        task_history, status, _, _ = self.get_task_history('active', repo_name, http_info=True)

        # If the task history can be fetched, check the task didn't complete
        if status == 200:
            if len(task_history) == 0:
                return

            task = task_history[0]
            self.assertEqual(task.status, "failed")
            self.assertEqual(task.error_code, 2)

    def test_privileges_revoked_later(self):
        """ Test same archive path but the location is not shared or privileges are dropped.

        params:
            case: If ro, makes the shared directory read only. Otherwise, unmounts the shared directory.
        """
        repo_name, case = "my_repo", self.input.param("case", "ro")

        self.assertIn(case, ["ro", "unmount"])

        self.create_repository_with_default_plan(repo_name)

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        # Sleep to ensure the bucket is populated with data
        self.sleep(10)

        # Perform a one off backup
        task_name = self.take_one_off_backup_with_name(repo_name).task_name
        # Wait until the backup task has completed
        self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 5, task_name=task_name))

        if case == "ro":
            # Make shared directory read only
            self.chmod("444", self.directory_to_share)
        else:
            # Unmount shared directory
            for client in self.nfs_connection.clients:
                client.clean(self.nfs_connection.directory_to_mount)

        # Perform a one off backup
        self.take_one_off_backup_with_name(repo_name)

        self.sleep(10)

        if case == "ro":
            # Make the shared directory accessible again
            self.chmod("777", self.directory_to_share)
        else:
            # Remount shared directory
            for client in self.nfs_connection.clients:
                client.mount(self.directory_to_share, self.nfs_connection.directory_to_mount)

        self.sleep(30)
        # Check the 2nd backup does not exist
        self.assertEqual(len(self.get_backups("active", repo_name)), 1)

    # Cbbackupmgr Info / Restore

    def test_cbbackupmgr_info(self):
        """ Test the user can obtain the info of a repository via cbbackupmgr
        """
        repo_name, no_of_backups = "my_repo", 2

        self.create_repository_with_default_plan(repo_name)

        # Fetch repository information
        repository = self.get_repository('active', repo_name)

        def get_blob_gen(i):
            return DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100 * i, 100 * (i + 1))), start=0, end=self.num_items)

        # Take some backups
        self.take_n_one_off_backups("active", repo_name, get_blob_gen, no_of_backups)

        # Fetch repo info using cbbackupmgr (using alternate staging directory)
        self.backupset.name, self.backupset.objstore_staging_directory = repository.repo, self.backupset.objstore_alternative_staging_directory
        output, _ = self.backup_info()

        if not output:
            self.fail("Unable to obtain the output from `cbbackupmgr info`")

        info = json.loads(output[0])

        # Check repository name and the number of backups in cbbackupmgr info, backup service info and task history
        self.assertEqual(info['name'], repository.repo)
        self.assertEqual(len(info['backups']), no_of_backups)
        self.assertEqual(len(self.get_task_history("active", repo_name)), no_of_backups)
        self.assertEqual(len(self.get_backups("active", repo_name)), no_of_backups)

        # Compare backup service info, backup service task history and cbbackupmgr info
        # The task history has to be reversed as the newest task ends up at the top but the newest backup ends up at the end
        # of their respective lists
        for i, backup_service_backup, task, cbbackupmgr_backup in zip(range(0, no_of_backups), self.get_backups("active", repo_name), reversed(self.get_task_history("active", repo_name)), info['backups']):
            # Check the backup name between cbbackupmgr and the backup service match
            self.assertEqual(backup_service_backup._date, cbbackupmgr_backup['date'])

            # Check the 'Backuping up to `backup-name`' in the description of the stats matches the `backup-name` in the backup service backup
            self.assertEqual(self.map_task_to_backup("active", repo_name, task.task_name), backup_service_backup._date)

            # Check the size of the backup between cbbackupmgr and the backup service are equal
            self.assertEqual(backup_service_backup.size, cbbackupmgr_backup['size'])

            # Check the items of the backup between cbbackupmgr and the backup service matches number of items we backed up
            self.assertEqual(sum(bucket.items for bucket in backup_service_backup.buckets), self.num_items)
            self.assertEqual(sum(bucket['items'] for bucket in cbbackupmgr_backup['buckets']), self.num_items)

            # Check the first backup is full and any subsequent backups are incremental (as configured in take_n_one_off_backups)
            self.assertEqual(cbbackupmgr_backup['type'], 'FULL' if i < 1 else 'INCR')

    def test_cbbackupmgr_restore(self):
        """ Test the user can restore a backup service backup using cbbackupmgr
        """
        repo_name, full_backup = "my_repo", self.input.param("full_backup", False)

        self.create_repository_with_default_plan(repo_name)

        # Fetch repository information
        repository = self.get_repository('active', repo_name)

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        # Sleep to ensure the bucket is populated with data
        self.assertTrue(self.wait_until_documents_are_persisted(timeout=200))

        # Take a one off backup using the backup service
        task_name = self.take_one_off_backup("active", repo_name, full_backup, 20, 20)

        # Get backup name
        backup_name = self.map_task_to_backup("active", repo_name, task_name)
        self.backups.append(backup_name)

        # Flush all buckets to empty cluster of all documents
        self.flush_all_buckets()

        # Use cbbackupmgr to restore using (using alternate staging directory)
        self.backupset.name = repository.repo
        self.backupset.objstore_staging_directory, old_staging_dir = self.backupset.objstore_alternative_staging_directory, self.backupset.objstore_staging_directory
        self.backup_restore()
        self.backupset.objstore_staging_directory = old_staging_dir

        # Sleep to ensure the bucket is populated with data
        self.assertTrue(self.wait_until_documents_are_persisted(timeout=200))

        # Delete the alt staging directory before cbriftdump
        self.rmdir(self.backupset.objstore_alternative_staging_directory)

        # Configure repository name and staging directory (validate_backup_data needs to grab the bucket-uuid's from this staging directory)
        if self.objstore_provider:
            self.backupset.objstore_staging_directory = repository.cloud_info.staging_dir

        # Check backup content equals content of cluster after restore
        self.validate_backup_data(self.backupset.backup_host, self.input.clusters[0], "ent-backup", False, False, "memory", self.num_items, None, backup_name = backup_name, skip_stats_check=True)

    # Nsserver tests

    def test_killing_the_backup_service(self):
        """ Test if nsserver restarts the backup service

        1. Set the time to 0800 Friday and create a repository using the _daily_backups plan.
        2. Take a one off backup.
        3. Kill the backup service and check if the Saturday task (the next task) is scheduled.
        If `skiptask` is set then skip over the Saturday task during the downtime and check if the Sunday task was scheduled.

        params:
            skiptask (bool): If set, advances the time to 1 minute after the scheduled time during the downtime
        """
        repo_name, full_backup, skiptask = "my_repo", self.input.param("full_backup", False), self.input.param("skiptask", False)

        self.time.change_system_time("next friday 0800")

        body = Body2(plan="_daily_backups", archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie default plan to repository
        self.create_repository(repo_name, body=body)

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        # Sleep to ensure the bucket is populated with data
        self.assertTrue(self.wait_until_http_requests_can_be_made())

        # Take a one off backup using the backup service
        task_name = self.take_one_off_backup("active", repo_name, full_backup, 20, 20)

        if skiptask:
            # Get the time of the next scheduled task
            previous_task_time, _, previous_task_name = self.get_repository('active', repo_name).next_scheduled
            # This should be the Saturday task
            self.assertEqual(previous_task_name, "backup_saturday")
            # Kill the backup service
            self.kill_process("backup")
            # Change time to 1 minute after the task is scheduled to skip it
            self.time.change_system_time(str(previous_task_time + datetime.timedelta(minutes=1)))
        else:
            # Kill the backup service
            self.kill_process("backup")

        if not self.wait_for_process_to_start("backup", 20, 5):
            self.fail("Failed to restart the backup service")

        # Give the service time to restart
        self.sleep(20)

        time, _, task_name = self.get_repository('active', repo_name).next_scheduled

        if skiptask:
            self.assertEqual(task_name, "backup_sunday" if skiptask else "backup_saturday")

        # Change time to 1 minute before the task is scheduled
        self.time.change_system_time(str(time - datetime.timedelta(minutes=1)))

        self.sleep(60)

        # Wait for the scheduled task to complete
        if not self.wait_for_backup_task("active", repo_name, 20, 20, task_name):
            self.fail("The task was not completed or took too long to complete")

    # RBAC

    def test_rbac_create_repository(self):
        """ A test non-admin user cannot use the backup service
        """
        # Change API credentials
        self.configuration.username, self.configuration.password, rest_connection = "Mallory", "password", RestConnection(self.master)

        roles = \
        [
            ('admin', True),
            ('backup_admin', True),
            ('ro_admin', False),
            ('security_admin_local', False),
            ('cluster_admin', False),
            ('', False),
        ]

        for role, operation_should_succeed in roles:
            # Set Mallory's role to `role`
            rest_connection.add_set_builtin_user("Mallory", f"name={self.configuration.username}&roles={role}&password={self.configuration.password}")
            self.sleep(5)
            # Create a repository using the new credentials
            _, status, _, response_data = self.create_repository("my_repo", body=Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory, bucket_name=None),
                                                                 http_info=True, should_succeed=operation_should_succeed)
            # Check the status
            self.assertIn(status, [200] if operation_should_succeed else [403, 400])
            # Clean up if a repository was created successfully
            if status == 200:
                self.backup_service_cleanup()

            # Check the error message if the status is 400
            if status == 400:
                self.log.info("Response: " + response_data.data)
                self.assertEqual(json.loads(response_data.data)['message'], 'Forbidden. User needs one of the following permissions')

        # Restore configuration so we can tear the test down correctly
        self.configuration.username, self.configuration.password = self.master.rest_username, self.master.rest_password

    def test_rbac_other_operations(self):
        """ Test if non admin roles cannot trigger a one off backup, get the task history or the list of backups
        """
        repo_name = "my_repo"

        self.create_repository_with_default_plan(repo_name)

        # Change API credentials
        self.configuration.username, self.configuration.password, rest_connection = "Mallory", "password", RestConnection(self.master)

        roles = ['ro_admin', 'security_admin_local', 'cluster_admin', '']

        actions = \
        [
            (self.take_one_off_backup_with_name, [repo_name, True]),
            (self.get_task_history, ["active", repo_name, True]),
            (self.get_backups, ["active", repo_name, True])
        ]

        for role in roles:
            # Set Mallory's role to `role`
            rest_connection.add_set_builtin_user("Mallory", f"name={self.configuration.username}&roles={role}&password={self.configuration.password}")
            # Call various backup service endpoints
            for func, arg in actions:
                status = func(*arg)[1]
                if role == "ro_admin" and (func == self.get_task_history or func == self.get_backups): # https://issues.couchbase.com/browse/MB-61072
                    self.assertIn(status, [200], msg="ro_admin should have access to backup service info. Tried: " + str(func))
                else:
                    self.assertIn(status, [403, 400], msg="role " + str(role) + " has access to: " + str(func))

        # Restore configuration so we can tear the test down correctly
        self.configuration.username, self.configuration.password = self.master.rest_username, self.master.rest_password

    # Cluster Ops

    def test_cluster_ops(self):
        """ Test if tasks are not scheduled during a rebalance and subsequently rescheduled after
        """
        repo_name, cluster_op, allowed_ops = "my_repo", self.input.param("cluster_op", "rebalance"), ["rebalance", "failover"]

        self.assertIn(cluster_op, allowed_ops, f"The input param `cluster_op` must be in {allowed_ops}")

        self.time.change_system_time(f"next friday 0800")

        body = Body2(plan="_daily_backups", archive=self.backupset.directory)
        if self.objstore_provider:
            body = self.set_cloud_credentials(body)
        # Add repositories and tie plan to repository
        self.create_repository(repo_name, body=body)

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)
        # Sleep to ensure the bucket is populated with data
        self.sleep(10)

        time, _, task_name = self.get_repository('active', repo_name).next_scheduled
        # Check the saturday task is scheduled
        self.assertEqual(task_name, "backup_saturday")

        # Change time to 1 minute before the task is scheduled
        self.time.change_system_time(str(time - datetime.timedelta(minutes=1)))
        # Wait until we are at the moment a few seconds before the task is scheduled
        self.sleep(50)

        cluster, rest_connection, failover_server = Cluster(), RestConnection(self.master), FailoverServer(self.input.clusters[0], self.input.clusters[0][0])

        if cluster_op == "rebalance":
            # Start a long cluster op
            cluster.rebalance(self.input.clusters[0], [], self.input.clusters[0][1:], services=[server.services for server in self.servers])

        if cluster_op == "failover":
            failover_server.failover()
            self.sleep(60) # The failover method does not block
            failover_server.recover_from_failover()

        time, _, task_name = self.get_repository('active', repo_name).next_scheduled

        # Check the saturday task was skipped
        self.assertEqual(task_name, "backup_sunday")

    # Test Stats

    def test_backup_stats(self):
        """ Test the backup stats

        Tests the following stats:
        1. backup_dispatched
        2. backup_data_size
        3. backup_task_run

        Stats not tested include backup_location_check' and 'backup_task_orphaned'.
        """
        # Create a repository with a default plan
        repo_name = "repo_name"
        self.create_repository_with_default_plan(repo_name)

        # Take a one off backup
        self._load_all_buckets(self.master, DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100, 200)), start=0, end=self.num_items), "create", 0)
        self.wait_until_documents_are_persisted()
        self.take_one_off_backup("active", repo_name, True, 20, 20)

        def get_stats_latest_value(stat):
            """ Fetch the latest value of the stat
            """
            time, stat = stat['data'][0]['values'][-1]
            return int(stat)

        timeout, prometheus = time.time() + 200, Prometheus(self.backupset.cluster_host)

        # Check the stats have a sensible value
        while time.time() < timeout:
            if all(len(prometheus.stats_range(stat)['data']) > 0 for stat in ['backup_dispatched', 'backup_data_size', 'backup_task_run']):
                self.assertGreaterEqual(get_stats_latest_value(prometheus.stats_range('backup_dispatched')), 1)
                self.assertGreaterEqual(get_stats_latest_value(prometheus.stats_range('backup_data_size')), 1000000)
                self.assertGreaterEqual(get_stats_latest_value(prometheus.stats_range('backup_task_run')), 1)
                return

            self.sleep(1, "Waiting for prometheus stats to become available")

        self.fail("Timed out while trying to wait for prometheus stats to become available")

    def test_orphan_detection(self):
        """ Test if the backup service cleans up an orphaned task correctly

        1. Start a task on a non-leader node and failover all non-leader nodes.
        2. If an orphaned task is produced, manually trigger orphan detection and check if it's cleaned up correctly.
        """
        base_repo_name, repositories = "my_repo", 5

        for i in range(repositories):
            self.create_repository_with_default_plan(f"{base_repo_name}{i}")

        leader = self.get_leader()

        # We're going to rebalancing things out, preferably it should not be the first node as to not break testrunner's setUp and tearDown
        self.assertEqual(leader, self.input.clusters[0][0])

        # Load buckets with a lot of data so the subequent backup takes a long time
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", 5000, end=100), "create", 0)

        for i in range(repositories):
            repo_name = f"{base_repo_name}{i}"
            # Perform a one off backup
            task_name = self.take_one_off_backup_with_name(repo_name, body=Body4(full_backup=True)).task_name
            # Fetch repository information
            repository = self.get_repository('active', repo_name)

            # Fetch task from running_one_off
            self.assertIsNotNone(repository.running_one_off, "Expected a task to be currently running")
            task = next(iter(repository.running_one_off.values()))

            # Obtain the server that the task was scheduled on
            server_task_scheduled_on = self.uuid_to_server(task.node_runs[0].node_id)

            # If the task is a non-leader node, we attempt to create an orphan
            if leader != server_task_scheduled_on:
                failover_servers = [FailoverServer(self.input.clusters[0], server) for server in self.input.clusters[0][1:]]

                # Failover non-leader nodes so only the leader node is left
                for failover_server in failover_servers:
                    failover_server.failover(graceful=False)

                task_history_names = [task.task_name for task in self.get_task_history("active", repo_name)]
                repository = self.get_repository("active", repo_name)

                # If we're unable to create the test conditions, then simply pass
                if task_name in task_history_names or not repository.running_one_off:
                    self.log.info("Unable to produce an orphaned task")
                    return

                # Check the orphan detection moves the orphan task into the task history
                self.assertNotIn(task_name, task_history_names)
                self.run_orphan_detection(leader)
                self.sleep(5)
                self.assertIn(task_name, [task.task_name for task in self.get_task_history("active", repo_name)])

                # Return as the conditions of the test have been met
                return

        # If we're unable to have a task scheduled on a non-leader node, then simply pass
        self.log.info("Unable to schedule a task on a non-leader node")

    # Schedule testing

    def test_next_scheduled_time(self):
        """ Test if the next scheduled time is correct for a simple task schedule
        """
        self.time.change_system_time(f"next friday 0800")

        # Define a simple schedule
        schedule = \
        [
            (1, 'HOURS', '22:00'),
        ]

        self.schedule_test([schedule], 1)

    def test_complex_schedule(self):
        """ Test a complex schedule
        """
        self.time.change_system_time(f"next friday 0745")

        # Define a complex schedule
        schedule = \
        [
            (1, 'MONDAY', '01:00'),
            (6, 'DAYS', None),
            (1, 'WEEKS', '22:04'),
            (8, 'MONDAY', '13:11'),
            (10, 'WEDNESDAY', '14:25'),
            (11, 'FRIDAY', '03:37'),
            (24, 'DAYS', '19:42'),
        ]

        self.schedule_test([schedule], 5)

    def test_tasks_scheduled_for_same_time(self):
        """ Tests if at least 1 task succeeds when tasks are scheduled at the same time
        """
        repo_name, plan_name = "my_repo", "my_plan"

        # Define a schedule which schedules tasks for the same time.
        schedule = \
        [
            (1, 'HOURS', '05:00'),
            (1, 'HOURS', '05:00'),
            (1, 'HOURS', '05:00'),
            (1, 'HOURS', '05:00'),
        ]

        self.time.change_system_time("next friday 0800")
        self.create_plan_and_repository(plan_name, repo_name, schedule)

        # Allow the repository to calculate its schedule
        self.sleep(15)

        # The time and task name to be executed next by the backup service
        time, _, task_name = self.get_repository('active', repo_name).next_scheduled

        # Travel to scheduled time
        self.time.change_system_time(str(time - datetime.timedelta(minutes=1)))
        self.sleep(60)

        # Check at least 1 of the tasks succeed as its currently not possible to schedule tasks
        # for the same time and expect them all to succeed.
        # The tasks do not take a long time so we can wait for them serially
        self.assertTrue(any([self.wait_for_backup_task("active", repo_name, 20, 10, f"task{i}") for i in range(4)]))

    def test_merge_options(self):
        """ Test the merge options backup the correct set of backups
        """
        self.time.change_system_time("next friday 0800")
        #              m (The merge window) (Starts 1 day ago with a window size of 2)
        #            <--->
        # Time/Day 1 2 3 4 5
        #    02:00 b b b b b (The last backup before the merge is the 5th Task)
        #    06:00         m (The merge is the 6th Task)

        # Define a schedule
        schedule = \
        [
            (1, 'DAYS', '02:00'),
            (5, 'DAYS', '06:00'),
        ]

        # Make the second task a merge with offset_start = 1, offset_end = 2
        merge_map = {1: (1, 2)}

        schedule_test = ScheduleTest(self, [schedule], merge_map=merge_map)

        schedule_test.run(5)
        self.sleep(60) # Takes a moment to update the backups
        backups_before_merge = self.get_backups("active", "repo_name0")
        self.assertEqual(len(backups_before_merge), 5)

        schedule_test.run(1)
        self.sleep(60) # Takes a moment to update the backups
        backups_after_merge = self.get_backups("active", "repo_name0")

        # Check the merge backed up the correct backups
        self.assertEqual(len(backups_after_merge), 3)
        self.assertIn(backups_before_merge[0]._date, [backup._date for backup in backups_after_merge]) # Check backup 1 is not part of the merge
        self.assertIn(backups_before_merge[4]._date, [backup._date for backup in backups_after_merge]) # Check backup 5 is not part of the merge

    def test_concurrent_merge(self):
        self.time.change_system_time("next friday 0800")
        #              m (The merge window) (Starts 1 day ago with a window size of 2)
        #            <--->
        # Time/Day 1 2 3 4 5
        #    02:00 b b b b b (The last backup before the merge is the 5th Task)
        #    06:00        2m (The merge is the 6th and 7th Task)

        # Define a schedule
        schedule = \
        [
            (1, 'DAYS', '02:00'),
            (5, 'DAYS', '06:00'), # Concurrent merge
            (5, 'DAYS', '06:00'), # Concurrent merge
        ]

        # Make the second and third task a merge with offset_start = 1, offset_end = 2
        merge_map = {1: (1, 2), 2: (1, 2)}

        schedule_test = ScheduleTest(self, [schedule], merge_map=merge_map)

        schedule_test.run(5)
        self.sleep(60) # Takes a moment to update the backups
        backups_before_merge = self.get_backups("active", "repo_name0")
        self.assertEqual(len(backups_before_merge), 5)

        # The time and task name to be executed next by the backup service
        time, _, task_name = self.get_repository('active', "repo_name0").next_scheduled

        # Travel to scheduled time
        self.time.change_system_time(str(time - datetime.timedelta(minutes=1)))
        self.sleep(60)

        # Check serially that at least 1 of the tasks succeed
        self.assertTrue(any([self.wait_for_backup_task("active", "repo_name0", 20, 10, f"task{i}") for i in range(1,3)]))

    def test_multiple_repository_schedules(self):
        """ Test the schedules of multiple repositories
        """
        schedule_a = \
        [
            (1, 'SUNDAY', '01:00'),
        ]

        schedule_b = \
        [
            (1, 'MONDAY', '01:00'),
        ]

        self.time.change_system_time("next friday 0800")

        self.schedule_test([schedule_a, schedule_b], 6)

    # Concurrent scheduling of test

    def test_concurrent_repository_schedules(self):
        """ Test repository-wise concurrent schedules
        """
        repo_name, plan_name = "my_repo", "my_plan"

        schedule_a = \
        [
            (1, 'SUNDAY', '01:00'),
        ]

        schedule_b = \
        [
            (1, 'SUNDAY', '01:00'),
        ]

        self.time.change_system_time("next friday 0800")
        self.create_plan_and_repository("plan_name0", "repo_name0", schedule_a)
        self.create_plan_and_repository("plan_name1", "repo_name1", schedule_b)

        # The time and task name to be executed next by the backup service
        time, _, task_name = self.get_repository('active', "repo_name0").next_scheduled

        # Travel to scheduled time
        self.time.change_system_time(str(time - datetime.timedelta(minutes=1)))
        self.sleep(60)

        # Check serially that at least 1 of the tasks succeed
        self.assertTrue(any([self.wait_for_backup_task("active", f"repo_name{i}", 20, 10, f"task0") for i in range(2)]))

    def test_task_queue_overpopulation(self):
        """ Trigger a lot of one-off-tasks

        Unfortunately, it's impossible to cause the Task Queue to overpopulate if 3 nodes are being used.
        Conversely, we can test the service is resilient to task queue overpopulation instead. The exact
        behaviour is for the task to run, however many tasks will fail because there is mutual exclusion
        at the archive level.
        """
        repo_name = "my_repo"

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        for i in range(10):
            self.create_repository_with_default_plan(f"{repo_name}{i}")

        for i in range(50):
            for j in range(10):
                # Perform a one off backup
                self.assertEqual(self.take_one_off_backup_with_name(f"{repo_name}{j}", body=Body4(full_backup = True), http_info=True)[1], 200)

    def test_time_change_detection(self):
        """ Test Tasks are rescheduled after time change

        params:
            change: Set time 'forward' or 'backward'
        """
        backwards = self.input.param('change', 'backward') == 'backward'

        # Manipulate time
        # self.time = Time(self.multiple_remote_shell_connections)

        self.time.change_system_time("next friday 0745")

        schedule = \
        [
            (1, 'HOURS', None),
        ]

        schedule_test = ScheduleTest(self, [schedule])

        # Should schedule tasks at 9, 10, 11
        schedule_test.run(3)

        self.sleep(15)

        self.time.change_system_time('0745' if backwards else '1315')

        # Give the backup service time to reconstruct the schedule
        self.sleep(60)

        next_time, _, _ = self.get_repository('active', schedule_test.plan_repo[0][1]).next_scheduled

        # The next time should be scheduled at 12 or 15 depending on time travel direction
        self.assertEqual(next_time.hour, 12 if backwards else 15)

    def test_user_warned_when_desyncing_nodes(self):
        """ Test NSServer's time de-sync warning
        """
        self.time.change_system_time('next friday 0800')

        # Manipulate time
        time_n2 = Time(RemoteMachineShellConnection(self.input.clusters[0][1]))
        time_n3 = Time(RemoteMachineShellConnection(self.input.clusters[0][2]))

        self.create_repository_with_default_plan("repo_name")

        time_n2.change_system_time('14:00')
        time_n3.change_system_time('15:00')

        timeout = time.time() + 200

        while time.time() < timeout:
            for alert in RestConnection(self.input.clusters[0][0]).get_alerts():
                if "time on node" and "is not synchronized" in alert['msg']:
                    return

            self.sleep(1, "Waiting for time de-sync warning")

        self.fail("Could not find the time de-sync warning in the alerts")

    def test_desyncing_non_leader_nodes(self):
        """ Test if a task scheduled on a non-leader node with its time de-synced
            results in a different backup name.

            Currently the name of the backup is different and the start and end times
            are not aware of the timezone change so we can simply check the start
            and times have a consistent run time, i.e. by being scheduled in a
            different timezones, the difference between the start and end times
            is not too large.
        """
        change_timezone = self.input.param("change_timezone", False)

        # Load buckets with a lot of data so the subequent backup takes a long time
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", 5000, end=100), "create", 0)

        self.time.change_system_time('next friday 0800')

        if change_timezone:
            self.solo_time[1].set_timezone('Pacific/Apia')
            self.solo_time[2].set_timezone('Pacific/Apia')
        else:
            self.solo_time[1].change_system_time('15:00')
            self.solo_time[2].change_system_time('15:00')

        def callback(task, server_task_scheduled_on, repo_name, task_name):
            self.wait_for_backup_task("active", repo_name, 20, 2, task_name)
            task = self.get_task_history("active", repo_name, task_name=task_name)[0]

            task_time_start, task_time_end = TimeUtil.rfc3339nano_to_datetime(task.start), TimeUtil.rfc3339nano_to_datetime(task.end)
            node_time_start, node_time_end = TimeUtil.rfc3339nano_to_datetime(task.node_runs[0].start), TimeUtil.rfc3339nano_to_datetime(task.node_runs[0].end)

            self.assertLess(abs(task_time_end - task_time_start).total_seconds(), 120)
            self.assertLess(abs(node_time_end - node_time_start).total_seconds(), 120)

        self.schedule_task_on_non_leader_node(callback)

    def test_timezone_modifications(self):
        """ Tests timezone modifications

        Tests the backup service responds to timezone changes.
        """
        curr_leader = self.get_leader()
        self.configuration.host = f"http://{curr_leader.ip}:{8091}/_p/backup/api/v1"

        self.time.change_system_time('next friday 0745')

        failover_server = FailoverServer(self.input.clusters[0], curr_leader)

        schedule = \
        [
            (1, 'HOURS', None),
        ]

        schedule_test = ScheduleTest(self, [schedule])

        # Should schedule tasks at 9
        schedule_test.run(1)

        self.time.set_timezone('Pacific/Apia')

        # Failover the leader
        failover_server.failover(graceful=False)
        self.sleep(60)
        failover_server.rebalance()

        curr_leader = self.get_leader(cluster=[server for server in self.input.clusters[0] if server != curr_leader])
        self.configuration.host = f"http://{curr_leader.ip}:{8091}/_p/backup/api/v1"

        # Should schedule tasks at 10
        schedule_test.run(1)

        time, _, _ = self.get_repository('active', schedule_test.plan_repo[0][1]).next_scheduled

        # TODO: when the backup service responds to timezone changes.
        # self.assertEqual(time.hour, None)
        # self.assertEqual(time.tzinfo, None)

    # Specific services

    def test_restoring_eventing(self):
        """ Test the backup service can restore eventing:

        params:
            sasl_buckets (int): The number of buckets to create. At least 3 buckets are required.
        """
        repo_name, full_backup, rest_connection = "my_repo", self.input.param("full_backup", True), RestConnection(self.backupset.cluster_host)

        self.create_repository_with_default_plan(repo_name)

        # Create eventing related stuff
        self.src_bucket_name, self.dst_bucket_name, self.metadata_bucket_name, self.use_memory_manager = "standard_bucket0", "standard_bucket1", "standard_bucket2", True
        eventing_body = self.create_save_function_body("eventing_function", HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.deploy_function(eventing_body)

        # Perform a one off backup
        self.take_one_off_backup("active", repo_name, full_backup, 20, 20)

        # Delete eventing related stuff
        self.bkrs_undeploy_and_delete_function(eventing_body, rest_connection)

        # Perform a one off restore
        self.take_one_off_restore("active", repo_name, 20, 5, self.backupset.cluster_host)

        # Check the eventing related stuff was restored
        function_details = json.loads(rest_connection.get_function_details("eventing_function"))
        self.assertTrue(function_details['appname'], eventing_body['appname'])
        self.assertTrue(function_details['appcode'], eventing_body['appcode'])

        # Delete eventing related stuff
        self.bkrs_undeploy_and_delete_function(eventing_body, rest_connection)

    def test_restoring_views(self):
        """ Test the backup service can restore views.
        """
        repo_name, full_backup= "my_repo", self.input.param("full_backup", True)
        design_doc_name, view = "design_doc", View("view_name", "function (doc) { emit(doc._id, doc); }")

        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)
        self.create_repository_with_default_plan(repo_name)

        # Create a view
        self.cluster.create_view(self.backupset.cluster_host, design_doc_name, view, "default")

        # Check the view exists
        result = self.cluster.query_view(self.backupset.cluster_host, "dev_" + design_doc_name, view.name, {"full_set": "true", "stale": "false", "connection_timeout": 60000}, timeout=30)
        self.assertEqual(len(result['rows']), self.num_items)

        # Perform a one off backup
        self.take_one_off_backup("active", repo_name, full_backup, 20, 20)

        # Delete view related stuff
        success = self.cluster.delete_view(self.backupset.cluster_host, design_doc_name, view, "default")
        self.assertTrue(success, "Unable to delete view")

        # Perform a one off restore
        self.take_one_off_restore("active", repo_name, 20, 5, self.backupset.cluster_host)

        # Check the view exists after the restore
        result = self.cluster.query_view(self.backupset.cluster_host, "dev_" + design_doc_name, view.name, {"full_set": "true", "stale": "false", "connection_timeout": 60000}, timeout=30)
        self.assertEqual(len(result['rows']), self.num_items, "Failed to restore view")

        # Delete view related stuff
        self.cluster.delete_view(self.backupset.cluster_host, design_doc_name, view, "default")

    def test_restoring_indexes(self):
        """ Test the backup service can restore indexes.
        """
        repo_name, full_backup = "my_repo", self.input.param("full_backup", True)
        username = self.backupset.cluster_host.rest_username
        password = self.backupset.cluster_host.rest_password

        self._load_all_buckets(self.master, DocumentGenerator('test_docs', '{{"age": {0}}}', range(1000), start=0, end=self.num_items), "create", 0)
        self.create_repository_with_default_plan(repo_name)

        rest_connection = RestConnection(self.master)

        # Create an index
        remote_client = RemoteMachineShellConnection(self.backupset.cluster_host)
        remote_client.execute_command(f"{self.cli_command_location}cbindex -type create -bucket default -index age -fields=age --auth={username}:{password}")

        # Perform a one off backup
        self.take_one_off_backup("active", repo_name, full_backup, 20, 20)

        # Delete the index
        remote_client.execute_command(f"{self.cli_command_location}cbindex -type drop -bucket default -index age --auth={username}:{password}")

        # Perform a one off restore
        self.take_one_off_restore("active", repo_name, 20, 5, self.backupset.cluster_host)

        output, error = remote_client.execute_command(f"{self.cli_command_location}cbindex -type list -auth={username}:{password}")

        # Test the index was restored succesfully
        self.assertTrue(any('Index:default/_default/_default/age' in line for line in output), "The index was not restored successfully")

        # Delete the index
        remote_client.execute_command(f"{self.cli_command_location}cbindex -type drop -bucket default -index age --auth={username}:{password}")
        remote_client.disconnect()

    def test_restoring_fts(self):
        """ Test the backup service can restore fts.
        """
        repo_name, full_backup = "my_repo", self.input.param("full_backup", True)

        self._load_all_buckets(self.master, DocumentGenerator('test_docs', '{{"age": {0}}}', range(1000), start=0, end=self.num_items), "create", 0)
        self.create_repository_with_default_plan(repo_name)

        rest_connection, fts_callable = RestConnection(self.master), FTSCallable(nodes=self.servers, es_validate=False)

        # Create fts index/alias
        index = fts_callable.create_default_index(index_name="index_default", bucket_name="default")
        fts_callable.wait_for_indexing_complete()
        alias = fts_callable.create_alias(target_indexes=[index])

        # Perform a one off backup
        self.take_one_off_backup("active", repo_name, full_backup, 20, 20)

        # Delete index and alias
        rest_connection.delete_fts_index(index.name)
        rest_connection.delete_fts_index(alias.name)

        # Perform a one off restore
        self.take_one_off_restore("active", repo_name, 20, 5, self.backupset.cluster_host)

        # Test the index and alias was restored successfully
        status, result = rest_connection.get_fts_index_definition(index.name)
        self.assertEqual(result['indexDef']['name'], index.name)
        status, result = rest_connection.get_fts_index_definition(alias.name)
        self.assertEqual(result['indexDef']['name'], alias.name)

        # Delete index and alias
        rest_connection.delete_fts_index(index.name)
        rest_connection.delete_fts_index(alias.name)

    def test_restoring_analytics(self):
        """ Test the backup service can restore analytics
        """
        repo_name, full_backup, rest_connection = "my_repo", self.input.param("full_backup", True), RestConnection(self.input.clusters[0][2])
        username = self.backupset.cluster_host.rest_username
        password = self.backupset.cluster_host.rest_password

        self.replace_services(self.input.clusters[0][2], ['cbas'])

        def drop():
            rest_connection.execute_statement_on_cbas("DROP DATASET dataset1", None, username=username, password=password)
            rest_connection.execute_statement_on_cbas("DROP DATASET dataset2", None, username=username, password=password)
            self.sleep(5)

        def create():
            rest_connection.execute_statement_on_cbas("CREATE DATASET dataset1 ON `default` WHERE TONUMBER(`age`)<30", None, username=username, password=password)
            rest_connection.execute_statement_on_cbas("CREATE DATASET dataset2 ON `default` WHERE TONUMBER(`age`)>30", None, username=username, password=password)

        def connect():
            rest_connection.execute_statement_on_cbas("CONNECT LINK Local", None, username=username, password=password)
            self.sleep(5)

        drop()

        self._load_all_buckets(self.master, DocumentGenerator('test_docs', '{{"age": {0}}}', range(1000), start=0, end=self.num_items), "create", 0)
        self.create_repository_with_default_plan(repo_name)

        # Create analytics related stuff
        create()
        connect()

        # Perform a one off backup
        self.take_one_off_backup("active", repo_name, full_backup, 20, 20)

        # Delete analytics related stuff
        drop()

        # Perform a one off restore
        self.take_one_off_restore("active", repo_name, 20, 5, self.backupset.cluster_host)

        # Connect the links after the restore completes
        connect()

        self.sleep(10)
        # Check the dataverse and links are restored
        value = json.loads(rest_connection.execute_statement_on_cbas("SELECT VALUE COUNT(*) FROM dataset1", None, username=username, password=password))
        self.assertEqual(value['results'], [30])
        value = json.loads(rest_connection.execute_statement_on_cbas("SELECT VALUE COUNT(*) FROM dataset2", None, username=username, password=password))
        self.assertEqual(value['results'], [969])
        drop()

    def test_restoring_collections(self):
        """ Test the backup service can restore collections

        Params:
            no_of_scopes (int): The number of scopes.
            no_of_collections (int): The number of collections in each scope.
            full_backup (bool): Take a full backup.
        """
        repo_name, full_backup, bucket_name = "full_name", self.input.param("full_backup", True), "default"
        no_of_scopes, no_of_collections, no_of_items = self.input.param("no_of_scopes", 5), self.input.param("no_of_collections", 5), self.input.param("items", 100)
        remote_connection = RemoteMachineShellConnection(self.backupset.cluster_host)

        # Create a repository with a default plan
        self.create_repository_with_default_plan(repo_name)

        # Create scopes and collections
        ids = self.create_variable_no_of_collections_and_scopes(self.backupset.cluster_host, bucket_name, no_of_scopes, no_of_collections)

        # Populate each collection with documents and prefix each document key with its collection id
        for key in ids:
            remote_connection.execute_cbworkloadgen(self.backupset.cluster_host.rest_username, self.backupset.cluster_host.rest_password, no_of_items, "1", bucket_name, 3, f"-j -c {ids[key]} --prefix={ids[key]}-pymc")

        self.sleep(30)

        # A function which identifies our documents uniquely
        document_key_function = DocumentUtil.unique_document_key("name", "scope", "collection")

        # Collect the documents from the bucket
        data = remote_connection.execute_cbexport(self.backupset.cluster_host, bucket_name, "scope", "collection")
        data.sort(key=document_key_function)
        self.assertEqual(len(data), no_of_collections * no_of_scopes * no_of_items)

        # Perform a one off backup
        self.take_one_off_backup("active", repo_name, full_backup, 20, 20)

        # Drop all buckets
        self.drop_all_buckets()

        # Perform a one off restore
        self.take_one_off_restore("active", repo_name, 20, 20, self.backupset.cluster_host)

        self.sleep(120)

        # Check the documents were successfully restored
        data_after_restore = remote_connection.execute_cbexport(self.backupset.cluster_host, bucket_name, "scope", "collection")
        data_after_restore.sort(key=document_key_function)
        self.assertEqual(data, data_after_restore)

        data_after_restore_dict = {document_key_function(document): document for document in data_after_restore}

        # Predict the body of each document
        for scope_no, collection_no, item_no in itertools.product(range(no_of_scopes), range(no_of_collections), range(no_of_items)):
            scope_name, collection_name = f"scope{scope_no}", f"collection{collection_no}"
            collection_id = ids[(scope_name, collection_name)]
            document = data_after_restore_dict[(scope_name, collection_name, f"{collection_id}-pymc{item_no}")]
            self.assertEqual(document['index'], str(item_no))
            self.assertEqual(document['body'], "000")

    def test_merging_and_restoring_with_collections(self):
        """ Test merging and restoring with collections
        """
        repo_name, full_backup, bucket_name, no_of_backups = "full_name", self.input.param("full_backup", True), "default", 2
        no_of_scopes, no_of_collections, no_of_items = self.input.param("no_of_scopes", 5), self.input.param("no_of_collections", 5), self.input.param("items", 100)
        remote_connection = RemoteMachineShellConnection(self.backupset.cluster_host)

        # Create a repository with a default plan
        self.create_repository_with_default_plan(repo_name)

        # Create scopes and collections
        ids = self.create_variable_no_of_collections_and_scopes(self.backupset.cluster_host, bucket_name, no_of_scopes, no_of_collections)

        # Take n one off backups
        def provisioning_function(i):
            for key in ids:
                remote_connection.execute_cbworkloadgen(self.backupset.cluster_host.rest_username, self.backupset.cluster_host.rest_password, no_of_items, "1", bucket_name, 2 + i, f"-j -c {ids[key]} --prefix={ids[key]}-pymc")

        self.take_n_one_off_backups("active", repo_name, None, no_of_backups, doc_load_sleep=30, data_provisioning_function=provisioning_function)

        backups = [backup._date for backup in self.get_backups("active", repo_name)]
        self.assertEqual(len(backups), no_of_backups)

        # Take a merge
        self.take_one_off_merge("active", repo_name, backups[0], backups[-1], 800)

        backups = [backup._date for backup in self.get_backups("active", repo_name)]
        self.assertEqual(len(backups), 1)

        # A function which identifies our documents uniquely
        document_key_function = DocumentUtil.unique_document_key("name", "scope", "collection")

        # Collect the documents from the bucket
        data = remote_connection.execute_cbexport(self.backupset.cluster_host, bucket_name, "scope", "collection")
        data.sort(key=document_key_function)
        self.assertEqual(len(data), no_of_collections * no_of_scopes * no_of_items)

        # Drop all buckets
        self.drop_all_buckets()

        # Perform a one off restore
        self.take_one_off_restore("active", repo_name, 20, 5, self.backupset.cluster_host)

        self.sleep(120)

        # Check the documents were successfully restored
        data_after_restore = remote_connection.execute_cbexport(self.backupset.cluster_host, bucket_name, "scope", "collection")
        data_after_restore.sort(key=document_key_function)
        self.assertEqual(data, data_after_restore)

        data_after_restore_dict = {document_key_function(document): document for document in data_after_restore}

        for scope_no, collection_no, item_no in itertools.product(range(no_of_scopes), range(no_of_collections), range(no_of_items)):
            scope_name, collection_name = f"scope{scope_no}", f"collection{collection_no}"
            collection_id = ids[(scope_name, collection_name)]
            document = data_after_restore_dict[(scope_name, collection_name, f"{collection_id}-pymc{item_no}")]
            self.assertEqual(document['index'], str(item_no))
            self.assertEqual(document['body'], "0" * (1 + no_of_backups))

    # Test logs

    def test_logs_can_be_collected(self):
        """ Test the logs can be collected
        """
        with Collector(self.backupset.cluster_host) as collector:
            collector.collect()

            # Check the backup service log file is present
            self.assertIn(Collector.backup_service_log, collector.files())

            # Check the backup service log file is populated with at least a 100 lines
            output, error = collector.remote_connection.execute_command(f"cat {collector.col_path}/{Collector.backup_service_log}")
            self.assertGreaterEqual(len(output), 100)

    def test_logs_can_be_redacted(self):
        """ Test the logs can be redacted
        """
        # Examine key 'test_doc-0'
        self.test_user_can_observe_document_changing()

        with Collector(self.backupset.cluster_host, log_redaction=True) as collector:
            collector.collect()

            # Check the backup service log file is present
            self.assertIn(Collector.backup_service_log, collector.files())

            # Check the backup service log file is populated with at least a 100 lines
            output, error = collector.remote_connection.execute_command(f"cat {collector.col_path}/{Collector.backup_service_log}")

            for line in output:
                if 'test_docs-0' in output:
                    self.fail(f"The redacted key was found in the output: {line}")

            output = [line for line in output if 'Running command /opt/couchbase/bin/cbbackupmgr examine' in line]
            self.assertGreaterEqual(len(output), 1)

            for line in output:
                self.assertIsNotNone(re.search(r'<ud>[A-z0-9]{40}<\/ud>', line))

    def test_logging_preamble(self):
        """ Execute a subset of backup service events to trigger logging.
        """
        # Change the system time
        self.time.change_system_time("next friday 0745")

        # Rebalance 3 node cluster into a 1 node cluster
        self.cluster.rebalance(self.input.clusters[0], [], self.input.clusters[0][1:], services=[server.services for server in self.servers])

        # Define a simple schedule
        schedule = \
        [
            (1, 'HOURS', '22:00'),
        ]

        # Schedule test
        self.schedule_test([schedule], 1)

        # One off restore
        self.take_one_off_restore("active", "repo_name0", 20, 5, self.backupset.cluster_host)

    def test_logging(self):
        """ Test a subset of operations to see if they are logged
        """
        file = File(self.backupset.cluster_host, self.log_directory)

        # Empty log file and execute some backup service events
        file.empty()
        self.test_logging_preamble()

        substrings = \
        [
            '(Rebalance) Starting rebalance',
            '(Rebalance) Removing node from service',
            '(Rebalance) Rebalance done',
            '(REST) POST /api/v1/plan/plan_name0',
            '(HTTP Manager) Added plan',
            '(Runner) Running command /opt/couchbase/bin/cbbackupmgr config',
            '(REST) POST /api/v1/cluster/self/repository/active/repo_name0',
            '(ClockKeeper) Adding job',
            '(Dispatcher) Dispatching task',
            '(Worker) Adding task to queue',
            '(Worker) Running task',
            '(Worker) Task done',
            '(Runner) Running command /opt/couchbase/bin/cbbackupmgr restore'
        ]

        # Read logs
        output = file.read()

        # Check if the substrings are present in the output
        success, string_not_found = File.find(output, substrings)
        self.assertTrue(success, f"Could not find: '{string_not_found}' in the logs")

    def test_logs_for_sensitive_information(self):
        """ Test the logs do not contain AWS credentials
        """
        repo_name, file = "my_repo", File(self.backupset.cluster_host, self.log_directory)

        # This test requires an objstore_provider for Cloud testing
        self.assertIsNotNone(self.objstore_provider)

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        # Empty logs
        file.empty()

        # Create a cloud repository with a default plan
        self.create_repository_with_default_plan(repo_name)

        # Perform a one off backup
        self.take_one_off_backup("active", repo_name, True, 20, 20)

        # Retrieve logs
        output = file.read()
        self.assertGreater(len(output), 0)

        # Check aws credentials are not present in the logs
        for line in output:
            self.assertNotIn(self.objstore_provider.access_key_id, line)
            self.assertNotIn(self.objstore_provider.secret_access_key, line)

    def test_audit_service(self):
        """ Test that the service generates audit events
        """
        repo_name = "repo_name"
        rest_conn = RestConnection(self.backupset.cluster_host)
        log_path = "/opt/couchbase/var/lib/couchbase/logs"
        file = File(self.backupset.cluster_host, f"{log_path}/audit.log")
        file.empty()

        audit_descriptors = rest_conn.get_audit_descriptors()

        non_backup_descriptors = [str(descriptor['id']) for descriptor in audit_descriptors if descriptor['module'] != 'backup']

        rest_conn.setAuditSettings(logPath=log_path, services_to_disable=non_backup_descriptors)

        self.create_repository_with_default_plan(repo_name)

        # One off backup
        self.take_one_off_backup("active", repo_name, False, 20, 20)

        # One off restore
        self.take_one_off_restore("active", repo_name, 20, 5, self.backupset.cluster_host)

        # Pause the repository and expect it to succeed
        self.assertEqual(self.pause_repository(repo_name, http_info=True)[1], 200)

        # Attempt to perform actions with invalid username/password combinations
        self.generate_authentication_failures(repo_name)

        # Read logs
        events = []
        event_count = Counter()
        # Due to an interaction in which emptying the audit log results in non-JSON artifacts
        # where the non-JSON artifact is a large list of nulls populating the first line of the
        # audit log, we have to filter out non-JSON content
        for line in file.read():
            try:
                event = json.loads(line)
                events.append(event)
                event_count[event['name']] += 1
            except json.JSONDecodeError:
                pass

        self.assertEqual(event_count['authentication failure'], 2)
        # Create a dictionary of event names to events
        seen_events = {event['name']: event for event in events}
        # Check the expected set of event names are present in the set of seen_events
        self.assertTrue({'Backup repository', 'Restore repository', 'Fetch repository', 'Pause repository', 'Add repository', 'authentication failure'}.issubset(seen_events.keys()))

        # Check some of the descriptions
        self.assertEqual(seen_events['Add repository']['description'], 'A new active backup repository was added')
        self.assertEqual(seen_events['Backup repository']['description'], 'A manual backup was triggered on an active repository')

        # Check the important fields are present
        for event in seen_events.values():
            expected_fields = {'id', 'name', 'description', 'local'}
            self.assertTrue(expected_fields.issubset(event.keys()))

            # Check these fields are non-empty
            for field in expected_fields:
                self.assertTrue(event[field])

            ip_port_fields = {'ip', 'port'}
            # Check the ip and port fields are present in the local field
            self.assertTrue(ip_port_fields.issubset(event['local'].keys()))

    def test_minimum_tls_version(self):
        """ Test the backup service respects the min_tls_version

        1. Use the couchbase-cli to update the global tls-min-version.
        2. Use curl to restrict the tls version used to make the https request.
        3. Test any version less than the tls-min-version fails and any version
        greater than the tls-min-version succeeds.
        """
        remote_shell = RemoteMachineShellConnection(self.master)

        remote_shell.execute_command("yum -y install curl")

        username = self.master.rest_username
        password = self.master.rest_password

        versions = [1.2]

        def set_tls_version(version):
            """ Sets the global min-tls-version using the couchbase-cli

            Returns:
                (bool): Returns True if the min-tls-version was set successfully
            """
            command = f"/opt/couchbase/bin/couchbase-cli setting-security -c localhost -u {username} -p {password} --tls-min-version tlsv{version} --set"
            _, _, exit_code = remote_shell.execute_command(command, get_exit_code=True)
            return exit_code == 0

        def make_http_request_with_tls_version(version):
            """ Returns True if a http request could be made using this specific tls version

            Returns:
                (bool):
            """

            command = f"curl --tlsv{version} --tls-max {version} https://{self.master.ip}:18097/api/v1/plan --insecure -u {username}:{password}"
            _, _, exit_code = remote_shell.execute_command(command, get_exit_code=True)
            return exit_code == 0

        for i, version in enumerate(versions):
            # Given a version, everything greater or equal to that version should work
            # Conversely, everything less than that version should not work
            should_not_work, should_work = [v for v in versions if v < version], [v for v in versions if v >= version]

            # Use the couchbase-cli to set the global cluster min-tls-version
            self.assertTrue(set_tls_version(version), "Could not set min-tls-version using couchbase-cli")

            for v in should_not_work:
                self.assertFalse(make_http_request_with_tls_version(v))

            for v in should_work:
                self.assertTrue(make_http_request_with_tls_version(v))

        set_tls_version(min(versions))
        remote_shell.disconnect()

    def test_sanity(self):
        """ A sanity tests """
        tests = [self.test_one_off_backup, self.test_one_off_restore, self.test_one_off_merge]

        for test in tests:
            test()
            self.tearDown()
            self.setUp()
