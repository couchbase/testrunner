import json
import random

from .e2e_base import E2EBaseTest
from .rest_helper import RestHelper


class ProfilesTests(E2EBaseTest):
    def setUp(self):
        E2EBaseTest.setUp(self)

    def tearDown(self):
        E2EBaseTest.tearDown(self)

    def create_user(self, username="test_username", password="test_password",
                    first_name="test_firstname", last_name="test_lastname"):

        rest_url = "http://%s/%s" % (self.profile_endpoint, "createUser")
        user_info = {
            "username": username+"{0}".format(random.randrange(0, 10000)),
            "password": password,
            "firstname": first_name+"{0}".format(random.randrange(0, 10000)),
            "lastname": last_name
        }
        self.log.info("UserInfo: {0}".format(json.dumps(user_info)))

        response = RestHelper.post_request(rest_url, user_info)
        if response.status_code != 200:
            raise Exception("Requests status content:{0}"
                            .format(response.content))
        self.log.info("Result from create user endpoint: {0}"
                      .format(response.json()["Msg"]))

        self.log.info("Deleting created user %s" % username)
        user_info.pop("password")
        rest_url = "http://%s/%s" % (self.profile_endpoint, "deleteUser")
        response = RestHelper.post_request(rest_url, user_info)

        if response.status_code != 200:
            raise Exception("Requests status content:{0}"
                            .format(response.content))
        self.log.info("Result from delete user endpoint: {0}"
                      .format(response.json()["Msg"]))
