import json
import random
from ent_backup_restore.backup_service_base import BackupServiceBase
from membase.api.rest_client import RestConnection
from lib.backup_service_client.models.task_template import TaskTemplate
from lib.backup_service_client.models.task_template_schedule import TaskTemplateSchedule
from backup_service_client.rest import ApiException
from lib.backup_service_client.models.body import Body
from lib.backup_service_client.models.body1 import Body1
from lib.backup_service_client.models.body2 import Body2
from lib.backup_service_client.models.body3 import Body3
from lib.backup_service_client.models.body4 import Body4
from lib.backup_service_client.models.body5 import Body5
from lib.backup_service_client.models.body6 import Body6
from lib.backup_service_client.models.plan import Plan
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection

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
        plans = self.plan_api.plan_get()
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
            self.assertEqual(self.plan_api.plan_name_post_with_http_info(plan.name if isinstance(plan, Plan) else "my_plan", body=plan)[1], 400)

        # Testing an empty plan name throws a 404 as the endpoint does not exist
        self.assertEqual(self.plan_api.plan_name_post_with_http_info("", body=Plan(name="", description=None, services=None, tasks=None))[1], 404)

        # Define a generic schedule for backups and merges
        generic_backup_schedule = TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00", start_now=False)
        generic_merge_schedule = TaskTemplateSchedule(job_type="MERGE", frequency=1, period="HOURS", time="22:00", start_now=False)

        # Invalid Tasks
        invalid_tasks = \
        [
            TaskTemplate(name="my_task!", task_type="BACKUP",  schedule=generic_backup_schedule, options=None, full_backup=None), # Special character in name
            TaskTemplate(name="my_task", task_type="BAD", schedule=generic_backup_schedule, options=None, full_backup=None), # Non-existing task type
            TaskTemplate(name="my_task", task_type="", schedule=generic_backup_schedule, options=None, full_backup=None), # Empty task type
            TaskTemplate(name="my_task", task_type=None, schedule=generic_backup_schedule, options=None, full_backup=None), # Missing task type
            TaskTemplate(name="", task_type="BACKUP", schedule=generic_backup_schedule, options=None, full_backup=None), # Name is empty
            TaskTemplate(name=None, task_type="BACKUP", schedule=generic_backup_schedule, options=None, full_backup=None), # Name is missing
            TaskTemplate(name="my_task", task_type="BACKUP", schedule=None), # Missing schedule
            TaskTemplate(name="my_task", task_type="BACKUP", schedule=generic_backup_schedule, options=None, full_backup="Bad"), # Full backup is not a bool
        ]

        # Add invalid Tasks and expect them to fail
        for task in invalid_tasks:
            # Add plan with an invalid Task (Should fail)
            self.assertEqual(self.plan_api.plan_name_post_with_http_info("my_plan", body=Plan(name="my_plan", tasks=[task]))[1], 400)

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
            self.assertEqual(self.plan_api.plan_name_post_with_http_info("my_plan", body=Plan(name="my_plan", tasks=[TaskTemplate(name="my_task", task_type="BACKUP", schedule=schedule)]))[1], 400)

    def test_valid_plan(self):
        """ Test a user can add a valid plan.

        A user can Add/Update a Plan with Tasks containing containing valid types, schedules, merge options or full backup flag.

        1. Define a selection of valid Plans.
        2. Add the plan and check if the operation succeeds.
        """
        # Services
        services = ["data", "gsi", "cbas", "ft", "eventing","views"]

        # Valid Plans
        valid_plans = \
        [
            Plan(name="my_plan", description=None, services=None, tasks=None), # A plan with a name
            Plan(name="a", description=None, services=None, tasks=None), # Name length equal to 1
            Plan(name="a" * 50  , description=None, services=None, tasks=None), # Name length equal to 50
            Plan(name="my_plan", description="A plan with a description", services=None, tasks=None), # A plan with a description
            Plan(name="my_plan" , description="a", services=None, tasks=None), # Description length equal to 1 character
            Plan(name="my_plan" , description="a" * 140, services=None, tasks=None), # Description length equal to 140 characters
            Plan(name="my_plan" , description=None, services=services, tasks=None), # All services
        ]

        # Individual services
        for service in services:
            valid_plans.append(Plan(name="my_plan" , description=None, services=[service], tasks=None)), # Non existant service

        # 10 random subset of services of size 3
        for i in range(0, 10):
            random.sample(services, 3)

        # Add valid Plans and expect them to succeed
        for plan in valid_plans:
            # Add plan
            self.assertEqual(self.plan_api.plan_name_post_with_http_info(plan.name, body=plan)[1], 200)

            self.plan_api.plan_name_delete(plan.name)

         # Define a generic schedule for backups and merges
        generic_backup_schedule = TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00", start_now=False)
        generic_merge_schedule = TaskTemplateSchedule(job_type="MERGE", frequency=1, period="HOURS", time="22:00", start_now=False)

        # Valid Tasks
        valid_tasks = \
        [
            TaskTemplate(name="my_task", task_type="BACKUP",  schedule=generic_backup_schedule, options=None, full_backup=None), # A task with a name
            TaskTemplate(name="a", task_type="BACKUP", schedule=generic_backup_schedule, options=None, full_backup=None), # Name length equal to 1
            TaskTemplate(name="a" * 50, task_type="BACKUP", schedule=generic_backup_schedule, options=None, full_backup=None), # Name length equal to 50
            TaskTemplate(name="my_task", task_type="BACKUP", schedule=generic_backup_schedule, options=None, full_backup=True), # Specify full back = True
            TaskTemplate(name="my_task", task_type="BACKUP", schedule=generic_backup_schedule, options=None, full_backup=False), # Specify full back = False
        ]

        # Add valid Tasks and expect them to succeed
        for task in valid_tasks:
            # Add plan with an valid Task (Should succeed)
            self.assertEqual(self.plan_api.plan_name_post_with_http_info("my_plan", body=Plan(name="my_plan", tasks=[task]))[1], 200)

            self.plan_api.plan_name_delete("my_plan")

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
            self.assertEqual(self.plan_api.plan_name_post_with_http_info("my_plan", body=Plan(name="my_plan", tasks=[TaskTemplate(name="my_task", task_type="BACKUP", schedule=schedule)]))[1], 200)

            # Delete plan
            self.plan_api.plan_name_delete("my_plan")

    def test_delete_plan(self):
        """ A user can delete a Plan not currently tied to a repository.

        1. Add a plan.
        2. Delete the plan and check if the operation succeeded.
        """
        plan_name = "my_plan"

        # Add plan
        self.plan_api.plan_name_post(plan_name)

        # Retrieve and confirm plan exists
        plan = self.plan_api.plan_name_get(plan_name)
        self.assertEqual(plan.name, plan_name)

        # Delete plan
        self.plan_api.plan_name_delete(plan_name)

        # Check if plan has been successfully deleted
        self.assertTrue(plan_name not in [plan.name for plan in self.plan_api.plan_get()])

    def test_delete_plan_tied_to_repository(self):
        """ A user cannot delete a Plan currently tied to a repository.

        1. Add a plan.
        2. Tie the plan to a repository.
        2. Delete the plan and check if the operation fails.
        """
        plan_name, repo_name = "my_plan", "my_repo"

        # Add plan
        self.plan_api.plan_name_post(plan_name)

        # Retrieve and confirm plan exists
        plan = self.plan_api.plan_name_get(plan_name)
        self.assertEqual(plan.name, plan_name)

        # Add repo and tie plan to repository
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None))

        # Delete plan (Should fail)
        self.assertEqual(self.plan_api.plan_name_delete_with_http_info(plan_name)[1], 400)

        # Check if plan has not been successfully deleted
        self.assertTrue(plan_name in [plan.name for plan in self.plan_api.plan_get()])

    def test_plan_task_limits(self):
        """ Test the min and max limits on task creation

        Create a plan with no Tasks and expect the operation to succeed.
        Create a plan with 1 Task and expect the operation to succeed.
        Create a plan with 14 Tasks and expect the operation to succeed.
        Create a plans with > 14 Tasks and expect the operation to fail.
        """
        generic_backup_schedule = TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", time="22:00", start_now=False)

        def get_task(i):
            return TaskTemplate(name=f"my_task{i}", task_type="BACKUP", schedule=generic_backup_schedule, options=None, full_backup=None)

        # Add 0, 1 and 14 Tasks in a Plan and expect it to succeed.
        for no_of_tasks in [0, 1, 14]:
            # Add plan with an valid Task (Should succeed)
            self.assertEqual(self.plan_api.plan_name_post_with_http_info("my_plan", body=Plan(name="my_plan", tasks=[get_task(i) for i in range(0, no_of_tasks)]))[1], 200)

            self.plan_api.plan_name_delete("my_plan")

        # Add 15, 20 and 100 Tasks in a Plan and expect it to fail.
        for no_of_tasks in [15, 20, 100]:
            # Add plan with an invalid Task (Should fail)
            self.assertEqual(self.plan_api.plan_name_post_with_http_info("my_plan", body=Plan(name="my_plan", tasks=[get_task(i) for i in range(0, no_of_tasks)]))[1], 400)

    def test_attach_plan_to_repository(self):
        """ Tie a plan to n repositories and test the plan was attached successfully.

        1. Add a Plan "my_plan".
        2. Create n repositories with "my_plan".
        3. Retrieve n repositories and confirm all repositories are using "my_plan".
        """
        plan_name, no_of_repositories = "my_plan", 20

        # Add plan
        self.plan_api.plan_name_post(plan_name)

        # Add repositories and tie plan to repository
        for i in range(0, no_of_repositories):
            self.active_repository_api.cluster_self_repository_active_id_post(f"my_repo{i}", body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None))

        # Fetch active repositories
        repos = [repo for repo in self.repository_api.cluster_self_repository_state_get('active')]

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
            return TaskTemplate(name=f"my_task{i}", task_type="BACKUP", schedule=generic_backup_schedule, options=None, full_backup=None)

        no_of_plans, no_of_tasks = 100, 14

        # Generate no_of_tasks tasks
        tasks = [get_task(i) for i in range(0, no_of_tasks)]

        # Add no_of_plans plans with no_of_tasks tasks
        for i in range(0, no_of_plans):
            # Add plan with an valid Task (Should succeed)
            self.assertEqual(self.plan_api.plan_name_post_with_http_info(f"my_plan{i}", body=Plan(name=f"my_plan{i}", tasks=tasks))[1], 200)

        # Delete plans
        for i in range(0, no_of_plans):
            self.plan_api.plan_name_delete(f"my_plan{i}")

    def test_create_plans_with_same_name(self):
        """ Add a plan, then add a plan with the same name and check if the operation fails.
        """
        # Create a plan
        plan_name = "my_plan"
        self.plan_api.plan_name_post(plan_name, body=Plan(name=plan_name, tasks=None))

        # Add plan with the same the name and expect it fail.
        self.assertEqual(self.plan_api.plan_name_post_with_http_info(plan_name, body=Plan(name=plan_name, tasks=None))[1], 400)

    def test_deleted_plans_name_can_be_reused(self):
        """ Add a plan, delete the plan and check if its name can be reused.
        """
        plan_name = "my_plan"

        # Add plan
        self.plan_api.plan_name_post(plan_name)

        # Delete plan
        self.plan_api.plan_name_delete(plan_name)

        # Add plan with the same name and expect it to succeed.
        self.plan_api.plan_name_post(plan_name)

    # Backup Repository Testing

    def test_correct_buckets_backed_up(self):
        """ Test the correct buckets are backed up.

        Params:
            all_buckets (bool): If True triggers a task that backups all buckets. Otherwise randomly picks a bucket.
            sasl_buckets (int): The number of buckets to create.

        1. Create a Plan with a Backup Task.
        2. Add Repo and tie Plan to Repo.
        3. Perform a one off backup Task.
        4. Wait for Task to Complete.
        5. Check if the expected buckets were backed up by using the Info endpoint.
        """
        all_buckets = self.input.param("all_buckets", False)

        if len(self.buckets) < 2:
            self.fail("At least 2 buckets are required for this test, set test param sasl_buckets=2")

        repo_name, plan_name, task_name = "my_repo", "my_plan", "my_task"

        # The buckets to backup (either all buckets or 1 bucket)
        buckets = set([bucket.name for bucket in self.buckets] if all_buckets else [random.choice(self.buckets).name])

        # Create a Backup Task
        my_task = TaskTemplate(name=task_name,
                               task_type="BACKUP",
                               schedule=TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="HOURS", start_now=False))

        # Add plan
        self.plan_api.plan_name_post(plan_name, body=Plan(tasks=[my_task]))

        # Add repositories and tie plan to repository
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None if all_buckets else next(iter(buckets))))

        # Perform a one off backup
        task_name = self.active_repository_api.cluster_self_repository_active_id_backup_post(repo_name).task_name

        # Wait until backup task has completed
        self.assertTrue(self.wait_for_backup_task("active", repo_name, 20 * len(buckets), 5, task_name))

        # Collect the buckets backed up through the info endpoint
        buckets_backed_up = set(bucket.name for bucket in self.repository_api.cluster_self_repository_state_id_info_get("active", repo_name).backups[0].buckets)

        # Check if the expected buckets were backed up
        self.assertEqual(buckets_backed_up, buckets, f"The selected buckets '{buckets.difference(buckets_backed_up)}' were not backed up")

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
        ]

        # This test requires an objstore_provider for Cloud testing
        self.assertIsNotNone(self.objstore_provider)

        provider = self.objstore_provider
        id_, key, region, staging_directory, endpoint = provider.access_key_id, provider.secret_access_key, provider.region, provider.staging_directory, provider.endpoint

        invalid_bodies = \
        [
            Body2(plan=plan_name, archive=self.get_archive(prefix="bad://"), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_credentials_region=region, cloud_endpoint=endpoint), # Non-existant prefix
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=None, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_credentials_region=region, cloud_endpoint=endpoint), # Missing staging directory
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir="", cloud_credentials_id=id_, cloud_credentials_key=key, cloud_credentials_region=region, cloud_endpoint=endpoint), # Empty staging directory
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=None, cloud_credentials_key=key, cloud_credentials_region=region, cloud_endpoint=endpoint), # Missing username
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=None, cloud_credentials_region=region, cloud_endpoint=endpoint), # Missing password
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_credentials_region=None, cloud_endpoint=endpoint), # Missing region
        ]

        # Note cannot test invalid credentials as LocalStack accepts any credentials
        for invalid_body in invalid_bodies:
            invalid_body.cloud_force_path_style = True
            invalid_repositories.append(("my_repo", invalid_body)) # Non existing schema prefix

        # Add invalid repositories
        for repo_name, body in invalid_repositories:
            # Add invalid repository and expect it to fail
            self.assertTrue(self.active_repository_api.cluster_self_repository_active_id_post_with_http_info(repo_name, body=body)[1] in [400, 404], f"An invalid repository configuration was added {repo_name, body}.")
            # Delete repositories
            self.delete_all_repositories()


    def test_valid_repository(self):
        """ Test a user can add a valid repository.

        A user can Add/Update a repository containing valid fields.

        Params:
            sasl_buckets (int): The number of buckets to create.
            objstore_provider (bool): If true, creates a cloud repository otherwise creates a filesystem repository.

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

        # This test requires an objstore_provider for Cloud testing
        self.assertIsNotNone(self.objstore_provider)

        provider = self.objstore_provider

        def get_archive(prefix=provider.schema_prefix(), bucket=provider.bucket, path="my_archive"):
            """ Returns a cloud archive path
            """
            return f"{prefix}{bucket}/{path}"

        id_, key, region, staging_directory, endpoint = provider.access_key_id, provider.secret_access_key, provider.region, provider.staging_directory, provider.endpoint

        # TODO: create staging directory with correct permissions
        # TODO: LocalStack cannot mock cloud_force_path_style=False
        valid_bodies = \
        [
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_credentials_region=region, cloud_endpoint=endpoint, cloud_force_path_style=True), # Just a repository in the cloud
            # Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_credentials_region=region, cloud_endpoint=endpoint, cloud_force_path_style=False), # Uncomment when the testing environment supports cloud_force_path_style=False
            Body2(plan=plan_name, archive=self.get_archive(), cloud_staging_dir=staging_directory, cloud_credentials_id=id_, cloud_credentials_key=key, cloud_credentials_region=region, cloud_endpoint=endpoint, bucket_name=bucket_name, cloud_force_path_style=True), # A repo in the cloud with a bucket
        ]

        for valid_body in valid_bodies:
            valid_repositories.append(("my_repo", valid_body)) # Non existing schema prefix

        # Add invalid repositories
        for repo_name, body in valid_repositories:
            # Add invalid repository and expect it to fail
            self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_post_with_http_info(repo_name, body=body)[1], 200, f"A valid repository configuration could not be added {repo_name, body}.")

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
        self.plan_api.plan_name_post(plan_name, body=Plan(tasks=[my_task]))

        # Add repositories and tie plan to repository
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory))

        # Wait until backup task has completed
        self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 5))

        # Pause the repository and expect it to succeed
        self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_pause_post_with_http_info(repo_name)[1], 200)

        # Pause a paused repository
        self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_pause_post_with_http_info(repo_name)[1], 200)

        # Count current number of backups
        no_of_backups = len(self.get_backups("active", repo_name))

        # Sleep for 70 seconds
        self.sleep(70)

        # Check no backups were started after 70 seconds
        self.assertEqual(no_of_backups, len(self.get_backups("active", repo_name)))

        # Resume the repository and expect it to succeed
        self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_pause_post_with_http_info(repo_name)[1], 200)

        # Resume a resume repository
        self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_pause_post_with_http_info(repo_name)[1], 200)

    def test_apply_plan_to_repository(self):
        """ A backup plan can be applied to a repository.
        """
        repo_name, plan_name, task_name = "my_repo", "my_plan", "my_task"

        # Add plan
        self.plan_api.plan_name_post(plan_name)

        # Add repo and tie plan to repository
        self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_post_with_http_info(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None))[1], 200)

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
        self.assertEqual(self.import_api.cluster_self_repository_import_post_with_http_info(body=body)[1], 200)

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

        self.chown(self.backupset.directory, user="root", group="root")

        repo_name = "my_repo"
        body = Body6(id=repo_name, archive=self.backupset.directory, repo=self.backupset.name)

        # Import repository
        self.assertEqual(self.import_api.cluster_self_repository_import_post_with_http_info(body=body)[1], 400)

    def test_importing_read_only_repository(self):
        """ Test a user can import a read only repository.
        """
        # Create backup config using cbbackupmgr
        self.backup_create()

        # Take 3 backups using cbbackupmgr
        for i in range(0, 3):
            self.backup_cluster()

        self.chown(self.backupset.directory)
        self.chmod(self.backupset.directory, 440)

        body = Body6(id="my_repo", archive=self.backupset.directory, repo=self.backupset.name)

        # Import repository
        self.assertEqual(self.import_api.cluster_self_repository_import_post_with_http_info(body=body)[1], 200)

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
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        # Fetch repository data
        repository = self.repository_api.cluster_self_repository_state_get('active')[0]

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        # Sleep to ensure the bucket is populated with data
        self.sleep(10)

        # Perform a one off backup
        task_name = self.active_repository_api.cluster_self_repository_active_id_backup_post(repo_name, body=Body4(full_backup=full_backup)).task_name

        # Wait until task has completed
        self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 20, task_name=task_name))

        # Configure repository name and staging directory
        if self.objstore_provider:
            self.backupset.objstore_staging_directory = repository.cloud_info.staging_dir
        self.backupset.name = repository.repo

        # Check backup content equals content of backup cluster
        self.validate_backup_data(self.backupset.backup_host, [self.master], "ent-backup", False, False, "memory", self.num_items, None)

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
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        # Fetch repository data
        repository = self.repository_api.cluster_self_repository_state_get('active')[0]

        def get_blob_gen(i):
            return BlobGenerator("ent-backup", "ent-backup-", self.value_size * i, start=self.num_items * i, end=self.num_items * (i + 1))

        for i in range(0, 2):

            # Load buckets with data
            self._load_all_buckets(self.master, get_blob_gen(i), "create", 0)

            # Sleep to ensure the bucket is populated with data
            self.sleep(10)

            # Perform a one off backup
            task_name = self.active_repository_api.cluster_self_repository_active_id_backup_post(repo_name, body=Body4(full_backup= i < 1)).task_name

            # Wait until task has completed
            self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 20, task_name=task_name))

            # Configure repository name and staging directory
            if self.objstore_provider:
                self.backupset.objstore_staging_directory = repository.cloud_info.staging_dir
            self.backupset.name = repository.repo

            backup_name = self.map_task_to_backup("active", repo_name, task_name)

            filter_keys = set(key for key, val in get_blob_gen(i))

            # Check backup content equals content of backup cluster
            self.validate_backup_data(self.backupset.backup_host, [self.master], "ent-backup", False, False, "memory", self.num_items, None, backup_name=backup_name, filter_keys=filter_keys)

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
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        # Fetch repository data
        repository = self.repository_api.cluster_self_repository_state_get('active')[0]

        def get_blob_gen(start_index, end_index):
            return BlobGenerator("ent-backup", "ent-backup-", self.value_size, start=self.num_items * start_index, end=self.num_items * (end_index + 1))

        for i in range(0, no_of_backups):

            # Load buckets with data
            self._load_all_buckets(self.master, get_blob_gen(i, i), "create", 0)

            # Sleep to ensure the bucket is populated with data
            self.sleep(10)

            # Perform a one off backup
            task_name = self.active_repository_api.cluster_self_repository_active_id_backup_post(repo_name, body=Body4(full_backup= i < 1)).task_name

            # Wait until task has completed
            self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 10, task_name=task_name))


        backups = [backup._date for backup in self.get_backups("active", repo_name)]

        task_name = self.active_repository_api.cluster_self_repository_active_id_merge_post(repo_name, body=Body5(start=backups[0], end=backups[no_of_backups - subtrahend_for_no_of_backups - 1], data_range="")).task_name

        self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 20, task_name=task_name))

        backup_name = self.map_task_to_backup("active", repo_name, task_name)

        filter_keys = set(key for key, val in get_blob_gen(0, no_of_backups - 1 - subtrahend_for_no_of_backups))

        # Configure repository name
        self.backupset.name = repository.repo

        backups = [backup._date for backup in self.get_backups("active", repo_name)]

        # Check backup content equals content of backup cluster
        self.validate_backup_data(self.backupset.backup_host, [self.master], "ent-backup", False, False, "memory", self.num_items, None, backup_name=backup_name, filter_keys = filter_keys)

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
        self.plan_api.plan_name_post(plan_name, body=Plan(tasks=[my_task]))

        # Create body for creating an active repository
        body = Body2(plan=plan_name, archive=self.backupset.directory)

        # An objstore provider is required
        self.assertIsNotNone(self.objstore_provider)

        # Manually create staging directory with permissions
        self.create_staging_directory()

        # Set cloud credentials
        self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        # Take backups
        for i in range(0, 6):
            # Perform a one off backup
            task_name = self.active_repository_api.cluster_self_repository_active_id_backup_post(repo_name).task_name
            # Wait until task has completed
            self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 5, task_name=task_name))

        backups = [backup._date for backup in self.get_backups("active", repo_name)]

        self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_merge_post_with_http_info(repo_name, body=Body5(start=backups[0], end=backups[len(backups) - 2]))[1], 500)

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
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        # Fetch repository data
        repository = self.repository_api.cluster_self_repository_state_get('active')[0]

        # Load buckets with data
        self._load_all_buckets(self.master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        # Sleep to ensure the bucket is populated with data
        self.sleep(10)

        # Perform a one off backup
        task_name = self.active_repository_api.cluster_self_repository_active_id_backup_post(repo_name, body=Body4(full_backup=full_backup)).task_name

        # Wait until task has completed
        self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 20, task_name=task_name))

        # Flush all buckets to empty cluster of all documents
        rest_connection = RestConnection(self.master)
        for bucket in self.buckets:
            rest_connection.change_bucket_props(bucket, flushEnabled=1)
            rest_connection.flush_bucket(bucket)

        # Configure repository name and staging directory
        if self.objstore_provider:
            self.backupset.objstore_staging_directory = repository.cloud_info.staging_dir
        self.backupset.name = repository.repo

        cluster_host = self.master

        body = Body1(target=f"{cluster_host.ip}:{cluster_host.port}", user=cluster_host.rest_username, password=cluster_host.rest_password)

        # Perform a restore
        task_name = self.repository_api.cluster_self_repository_state_id_restore_post("active", repo_name, body=body).task_name

        # Wait until task has completed
        self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 5))

        # Check backup content equals content of cluster after restore
        self.validate_backup_data(self.backupset.backup_host, [self.master], "ent-backup", False, False, "memory", self.num_items, None)

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
        self.plan_api.plan_name_post(plan_name, body=Plan(tasks=[my_task]))

        body = Body2(plan=plan_name, archive=self.backupset.directory)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)

        # Add repositories and tie plan to repository
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        # Take backups
        for i in range(0, 6):
            # Perform a one off backup
            task_name = self.active_repository_api.cluster_self_repository_active_id_backup_post(repo_name).task_name
            self.sleep(3)
            # Wait until task has completed
            self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 5, task_name=task_name))

        backups = [backup._date for backup in self.get_backups("active", repo_name)]

        if not self.objstore_provider:
        # Start a merge task, this succeeds (don't know if this is a bug)
            task_name = self.active_repository_api.cluster_self_repository_active_id_merge_post(repo_name, body=Body5(start="Bad Start Point", end="Bad End Point")).task_name
            self.sleep(3)

            # Check if the merge task failed with a sensible warning message delivered to the user (TODO MB-41565)
            self.assertEqual(self.repository_api.cluster_self_repository_state_id_task_history_get("active", "my_repo", task_name=task_name)[0].status, "failed")

        body = Body1(target="http://127.0.0.1/", user="Administrator", password="password", start="Bad Start Point", end="Bad End Point")

        # Perform a restore
        self.assertEqual(self.repository_api.cluster_self_repository_state_id_restore_post_with_http_info("active", repo_name)[1], 400)

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
        self.plan_api.plan_name_post(plan_name, body=Plan(tasks=[my_task]))

        body = Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=bucket_name)

        if self.objstore_provider:
            body = self.set_cloud_credentials(body)
            self.create_staging_directory()

        # Add repositories and tie plan to repository
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        # Delete all buckets
        RestConnection(self.backupset.cluster_host).delete_all_buckets()

        # Perform a one off backup
        _, status, _, response_data = self.active_repository_api.cluster_self_repository_active_id_backup_post_with_http_info(repo_name)

        # Load as json
        data = json.loads(response_data.data)

        # Check status and warning message
        self.assertEqual(status, 500)
        self.assertEqual(data['msg'], "could not send task")
        self.assertEqual(data['extras'], f"failed bucket check for bucket '{bucket_name}': element not found")

    def test_archived_repository(self):
        """ Test moving between active to archived

        Active to Archived
        """
        repo_name = "my_repo"

        # Add repo and tie plan to repository
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=Body2(plan=random.choice(self.default_plans), archive=self.backupset.directory, bucket_name=None))

        # Archive repository
        self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_archive_post_with_http_info(repo_name, body=Body3(id=repo_name))[1], 200)

        # Fetch repository
        repository = self.repository_api.cluster_self_repository_state_get('archived')[0]

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
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        # Archive repository
        self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_archive_post_with_http_info(repo_name, body=Body3(id=repo_name))[1], 200)

        # Fetch repository
        repository = self.repository_api.cluster_self_repository_state_get('archived')[0]

        # Check the archived repository exists
        self.assertEqual((repository.id, repository.state), (repo_name, "archived"))

        # Delete archived repository
        self.repository_api.cluster_self_repository_state_id_delete_with_http_info('archived', repo_name, remove_repository=remove_repository)

        # Check there are no archived repositories
        self.assertEqual(len(self.repository_api.cluster_self_repository_state_get('archived')), 0)

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
        self.plan_api.plan_name_post(plan_name)

        # Retrieve and confirm plan exists
        plan = self.plan_api.plan_name_get(plan_name)
        self.assertEqual(plan.name, plan_name)

        # Add repo and tie plan to repository
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=None))

        # Delete archived repository
        self.assertEqual(self.repository_api.cluster_self_repository_state_id_delete_with_http_info('active', repo_name, remove_repository=False)[1], 400)

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
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        # Fetch repository data
        repository = self.repository_api.cluster_self_repository_state_get('active')[0]

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
            task_name = self.active_repository_api.cluster_self_repository_active_id_backup_post(repo_name, body=Body4(full_backup= i < 1)).task_name

            # Wait until task has completed
            self.assertTrue(self.wait_for_backup_task("active", repo_name, 20, 20, task_name=task_name))

            backup_names.append(self.map_task_to_backup("active", repo_name, task_name))

        # Archive repository
        self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_archive_post_with_http_info(repo_name, body=Body3(id=repo_name))[1], 200)

        # Check if the task history can be fetched from an archived repo
        task_history = self.get_task_history("archived", repo_name)
        self.assertEqual(len(task_history), 2)

        # Check if the info can be fetched from an archived repo
        backups = self.get_backups("archived", repo_name)
        self.assertEqual(len(backups), 2)

        # Check user can examine an archived repo
        _, status, _, response_data = self.repository_api.cluster_self_repository_state_id_examine_post_with_http_info("archived", repo_name, body=Body("test_docs-0", "default"))
        self.assertEqual(status, 200)
        data = [json.loads(json_object) for json_object in response_data.data.rstrip().split("\n")]
        self.assertEqual(data[0]['value']['age'], 0)
        self.assertEqual(data[1]['value']['age'], 100)

        # Flush all buckets to empty cluster of all documents
        rest_connection = RestConnection(self.master)
        for bucket in self.buckets:
            rest_connection.change_bucket_props(bucket, flushEnabled=1)
            rest_connection.flush_bucket(bucket)

        cluster_host = self.master
        body = Body1(target=f"{cluster_host.ip}:{cluster_host.port}", user=cluster_host.rest_username, password=cluster_host.rest_password)

        # Perform a restore
        task_name = self.repository_api.cluster_self_repository_state_id_restore_post("archived", repo_name, body=body).task_name

        # Wait until task has completed
        self.assertTrue(self.wait_for_backup_task("archived", repo_name, 20, 5))

        # Check the restore task succeeded
        task = self.get_task_history("archived", repo_name, task_name)[0]
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
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        remote_client = RemoteMachineShellConnection(self.master)
        if self.objstore_provider:
            self.objstore_provider.teardown("", remote_client)
        else:
            remote_client.execute_command(f"rm -rf {self.backupset.directory}")
        remote_client.disconnect()

        _, status, _, response_data = self.repository_api.cluster_self_repository_state_id_info_get_with_http_info("active", repo_name)

        # Load as json
        data = json.loads(response_data.data)

        # Check status and warning message
        self.assertEqual(status, 500)
        self.assertEqual(data['msg'], "could not info repository")
        self.assertIn(f"Error getting archive information", data['extras'])

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
        self.active_repository_api.cluster_self_repository_active_id_post(repo_name, body=body)

        self.assertIsNotNone(self.objstore_provider)

        remote_client = RemoteMachineShellConnection(self.master)
        remote_client.execute_command(f"rm -rf {self.objstore_provider.staging_directory}")
        remote_client.disconnect()

        self.assertEqual(self.repository_api.cluster_self_repository_state_id_info_get_with_http_info("active", repo_name)[1], 200)

    def test_bad_archive_locations(self):
        """ Test bad archive locations.
        """
        archive_locations = \
            [
                ("/etc/shadow", None, None),
                ("/tmp/non_empty", ("couchbase", "couchbase"), "777"),
                (self.backupset.directory, ("root", "root"), "770"),
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
            self.assertEqual(self.active_repository_api.cluster_self_repository_active_id_post_with_http_info("my_repo", body=body)[1], 500, f"Created archive in bad location {archive}")

        remote_client.disconnect()

    def test_health_indicators(self):
        """ Test health indicators
        """
        task_name, plan_name, repo_name = "task_name", "plan_name", "my_repo"

        def pause_repository():
            self.active_repository_api.cluster_self_repository_active_id_pause_post(repo_name)

        def delete_the_target_bucket():
            # Delete all buckets
            RestConnection(self.backupset.cluster_host).delete_all_buckets()
            self.sleep(60)

        # [(Health Issue, Healthy, Action Function), ..]
        health_actions = \
        [
            (None, True, None),
            (None, True, pause_repository),
            ("cannot verify bucket exists: element not found", False, delete_the_target_bucket)
        ]

        # Create a Backup Task
        my_task = TaskTemplate(name=task_name,
                               task_type="BACKUP",
                               schedule=TaskTemplateSchedule(job_type="BACKUP", frequency=1, period="MINUTES", start_now=True))

        # Add plan
        self.plan_api.plan_name_post(plan_name, body=Plan(tasks=[my_task]))

        for health_issue, healthy, action in health_actions:

            # Create repository
            self.active_repository_api.cluster_self_repository_active_id_post_with_http_info(repo_name, body=Body2(plan=plan_name, archive=self.backupset.directory, bucket_name=random.choice(self.buckets).name))

            if action:
                action()

            repository_health = self.repository_api.cluster_self_repository_state_get('active')[0].health

            self.assertEqual(healthy, repository_health.healthy)
            self.assertEqual(health_issue, repository_health.health_issue)

            self.delete_all_repositories()
