import logger
from basetestcase import BaseTestCase
from scripts.deployE2E import DeployE2EServices


class E2EBaseTest(BaseTestCase):

    def setUp(self):
        super(E2EBaseTest, self).setUp()
        self.log = logger.Logger.get_logger()
        self.service_handler = DeployE2EServices(self.master.ip,
                                                 "Administrator", "password")
        self.setup_services()

    def tearDown(self):
        self.service_handler.deleteExistingDockerContainersOnHost()
        self.service_handler.deleteExistingDockerImagesOnHost()

        super(E2EBaseTest, self).tearDown()


    def setup_services(self):
        self.service_handler.deploy()
        self.booking_endpoint = self.service_handler.getBookingEndpoint()
        self.profile_endpoint = self.service_handler.getProfileEndpoint()
        self.inventory_endpoint = self.service_handler.getInventoryEndpoint()