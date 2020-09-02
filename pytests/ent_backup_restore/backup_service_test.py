from ent_backup_restore.backup_service_base import BackupServiceBase

from backup_service_client.rest import ApiException
from lib.backup_service_client.models.body2 import Body2

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

        try:
            # Delete plan (Should fail)
            self.plan_api.plan_name_delete(plan_name)
        except ApiException:
            self.log.info("Attempted to add delete plan tied to a repository which failed as expected.")

        # Check if plan has not been successfully deleted
        self.assertTrue(plan_name in [plan.name for plan in self.plan_api.plan_get()])
