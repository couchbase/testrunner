import json
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase
from membase.api.rest_client import RestConnection
from lib.backup_service_client.configuration import Configuration
from lib.backup_service_client.api_client import ApiClient
from lib.backup_service_client.api.plan_api import PlanApi
from lib.backup_service_client.api.repository_api import RepositoryApi
from lib.backup_service_client.api.active_repository_api import ActiveRepositoryApi
from lib.backup_service_client.models.body3 import Body3

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
        self.repository_api = RepositoryApi(self.api_client)
        self.active_repository_api = ActiveRepositoryApi(self.api_client)

        # Backup Service Constants
        self.default_plans = ["_hourly_backups", "_daily_backups"]

    def setUp(self):
        """ Sets up.

        1. Runs preamble.
        """
        super().setUp()
        self.preamble()

    def is_backup_service_running(self):
        """ Returns true if the backup service is running.
        """
        rest = RestConnection(self.master)
        return 'backupAPI' in json.loads(rest._http_request(rest.baseUrl + "pools/default/nodeServices")[1])['nodesExt'][0]['services'].keys()

    def delete_all_plans(self):
        """ Deletes all plans.

        Deletes all plans using the Rest API with the exceptions of the default plans.
        """
        for plan in self.plan_api.plan_get():
            if plan.name not in self.default_plans:
                self.plan_api.plan_name_delete(plan.name)

    def delete_all_repositories(self):
        """ Deletes all repositories.

        Archives all repos and then deletes the archived repos using the Rest API.
        """
        for repo in self.repository_api.cluster_self_repository_state_get('active'):
            self.active_repository_api.cluster_self_repository_active_id_archive_post(repo.id, body=Body3(id=repo.id))

        for repo in self.repository_api.cluster_self_repository_state_get('archived'):
            self.repository_api.cluster_self_repository_state_id_delete('archived', repo.id)

    def tearDown(self):
        """ Tears down.

        1. Runs preamble.
        2. Deletes all plans.
        """
        self.preamble()
        if self.is_backup_service_running():
            self.delete_all_repositories()
            self.delete_all_plans()
        super().tearDown()
