from ent_backup_restore.backup_service_base import BackupServiceBase

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
