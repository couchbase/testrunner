from .e2e_base import E2EBaseTest
from .rest_helper import RestHelper

class ProfilesTests(E2EBaseTest):

    def setUp(self):
        E2EBaseTest.setUp(self)

    def tearDown(self):
        E2EBaseTest.tearDown(self)

    def create_user(username="test_username", password="test_password", first_name="test_firstname",
                    last_name="test_lastname"):
        ep = "{0}/{1}".format(self.get_profile_endpoint(), "createUser")
        self.log.info("Creating user '%s'".format(username))
        response = RestHelper.post_request(
            ep, {"username": username, "password": password,
                 "firstname": first_name, "lastname": last_name})
        if response.status_code != 200:
            raise Exception("Requests status code: %s" % response.status_code)
        self.log.info(response.json()["Msg"])