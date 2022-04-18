from basetestcase import BaseTestCase
from e2e.rest_helper import RestHelper
from scripts.deployE2E import DeployE2EServices


class E2EBaseTest(BaseTestCase):
    def setUp(self):
        super(E2EBaseTest, self).setUp()
        self.service_handler = DeployE2EServices(self.master.ip,
                                                 "Administrator", "password")

        # Fetch required API endpoints
        self.booking_endpoint = self.service_handler.get_endpoint("booking")
        self.profile_endpoint = self.service_handler.get_endpoint("profile")
        self.inventory_endpoint = \
            self.service_handler.get_endpoint("inventory")

        self.username = "test_app"
        self.password = "test_password"

        setup_services = self.input.param("setup_services", True)
        if self.case_number == 1 and setup_services:
            self.service_handler.deploy()
            self.__setup_test_app_user()

    def tearDown(self):
        if self.case_number == self.total_testcases:
            self.service_handler.tearDown()
        super(E2EBaseTest, self).tearDown()

    def __setup_test_app_user(self):
        user_info = {"username": self.username, "firstname": "test_app",
                     "lastname": "e2e"}

        self.log.info("Deleting user %s to avoid failures" % self.username)
        rest_url = "http://%s/%s" % (self.profile_endpoint, "deleteUser")
        response = RestHelper.post_request(rest_url, user_info)

        if response.status_code != 200:
            raise Exception(
                "Requests status content:{0}".format(response.content))
        self.log.info("Result from delete user endpoint: {0}".format(
            response.json()["Msg"]))

        self.log.info("Creating user %s" % self.username)
        rest_url = "http://%s/%s" % (self.profile_endpoint, "createUser")
        user_info["password"] = self.password
        response = RestHelper.post_request(rest_url, user_info)
        if response.status_code != 200:
            raise Exception("Requests status content:%s" % response.content)
        self.log.info("Result from create user endpoint: {0}".format(
            response.json()["Msg"]))
