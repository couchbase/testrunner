import random
from ent_backup_restore.backup_service_base import BackupServiceBase
from lib.backup_service_client.models.task_template import TaskTemplate
from lib.backup_service_client.models.task_template_schedule import TaskTemplateSchedule
from backup_service_client.rest import ApiException
from lib.backup_service_client.models.body2 import Body2
from lib.backup_service_client.models.plan import Plan

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
