import random

from basetestcase import BaseTestCase
from e2e.rest_helper import RestHelper
from scripts.deployE2E import DeployE2EServices


class E2EBaseTest(BaseTestCase):
    def setUp(self):
        super(E2EBaseTest, self).setUp()
        self.log.info("==== Starting E2E Base Test setUp() ====")
        self.service_handler = DeployE2EServices(self.master.ip,
                                                 "Administrator", "password")

        # Fetch required API endpoints
        self.booking_endpoint = self.service_handler.get_endpoint("booking")
        self.profile_endpoint = self.service_handler.get_endpoint("profile")
        self.inventory_endpoint = \
            self.service_handler.get_endpoint("inventory")

        self.username_format = "test_user_%s"
        self.password = "password"
        self.lastname = "gauntlet"

        setup_services = self.input.param("setup_services", True)
        if self.case_number == 1 and setup_services:
            self.service_handler.deploy()
            self.__create_test_users()
            result = self.__deploy_flight_inventory()
            if not result:
                raise Exception("Failed to deploy flight inventory")
        self.log.info("==== Finished E2E Base Test setUp() ====")

    def tearDown(self):
        if self.case_number == self.total_testcases:
            self.service_handler.tearDown()
        super(E2EBaseTest, self).tearDown()

    def __create_test_users(self):
        wallet_doc_id = "bankAccount"
        default_wallet_credits = 1000
        self.log.info("Creating 10 test users")
        for index in range(1, 11):
            username = self.username_format % index

            # Create user
            result = self._setup_test_user(username, self.password,
                                           username, self.lastname)
            if not result:
                raise Exception("Failed to create user")

            # Create user wallet
            card_num = "%0.12d" % random.randint(0, 999999999999)
            cvv = "%0.3d" % random.randint(0, 999)
            expiry = "%0.2d/%0.2d" % (random.randint(1, 12),
                                      random.randint(23, 30))
            result = self._create_user_wallet(
                doc_id=wallet_doc_id, username=username,
                firstname=username, lastname=self.lastname,
                card_num=card_num, cvv=cvv, expiry=expiry)
            if not result:
                raise Exception("Failed to create user wallet")

            # Load user wallet
            result = self._load_wallet(username, default_wallet_credits)
            if not result:
                raise Exception("Failed to load user wallet")

    def _setup_test_user(self, username, password, firstname, lastname):
        result = True
        user_info = {"username": username, "firstname": firstname,
                     "lastname": lastname}

        self.log.info("Deleting user %s to avoid failures" % username)
        rest_url = "http://%s/%s" % (self.profile_endpoint, "deleteUser")
        response = RestHelper.post_request(rest_url, user_info)
        self.log.debug("Result from delete user: %s" % response.json())

        if response.status_code != 200:
            raise Exception(
                "Requests status content:{0}".format(response.content))

        self.log.info("Creating user %s" % username)
        rest_url = "http://%s/%s" % (self.profile_endpoint, "createUser")
        user_info["password"] = password
        response = RestHelper.post_request(rest_url, user_info)
        self.log.debug("Result from create user: %s" % response.json())
        if response.status_code != 200:
            result = False
            self.log.error("Requests status content:%s" % response.content)
        return result

    def _create_user_wallet(self, doc_id, firstname, lastname, username,
                            card_num, cvv, expiry):
        result = True
        rest_data = {"doc_id": doc_id, "username": username,
                     "firstname": firstname, "lastname": lastname,
                     "card_number": card_num, "cvv": cvv, "expiry": expiry}
        self.log.info("Creating wallet for user %s" % username)
        endpoint = "http://%s/%s" % (self.profile_endpoint, "createWallet")
        response = RestHelper.post_request(endpoint, rest_data)
        self.log.debug("Result from create user wallet: %s" % response.json())
        if response.status_code != 200:
            result = False
            self.log.error("Requests status content:%s" % response.content)
        return result

    def _load_wallet(self, username, amount_to_load):
        result = True
        rest_data = {"username": username, "amount": amount_to_load}

        self.log.info("Loading '%s' credits into user wallet '%s'"
                      % (amount_to_load, username))
        endpoint = "http://%s/%s" % (self.profile_endpoint, "loadWallet")
        response = RestHelper.post_request(endpoint, rest_data)
        self.log.debug("Result from load user wallet: %s" % response.json())
        if response.status_code != 200:
            result = False
            self.log.error("Requests status content:%s" % response.content)
        return result

    def __deploy_flight_inventory(self):
        result = True
        doc = {"docId": "AA001",
               "flight_id": "AA001",
               "status": "active",
               "departing_airport": "AZ",
               "arriving_airport": "CA",
               "departure_date": "2012-04-23T18:25:43.511Z",
               "model": "Boeing 737",
               "airline": "American Airlines",
               "seats": [{"available": True, "seatnumber": "1A",
                          "class": "economy", "price": 500},
                         {"available": True, "seatnumber": "1B",
                          "class": "economy", "price": 500},
                         {"available": True, "seatnumber": "1C",
                          "class": "economy", "price": 500},
                         {"available": True, "seatnumber": "2A",
                          "class": "economy", "price": 500},
                         {"available": True, "seatnumber": "2B",
                          "class": "economy", "price": 500},
                         {"available": True, "seatnumber": "2C",
                          "class": "economy", "price": 500}]}
        self.log.info("Loading flight inventory")
        endpoint = "http://%s/%s" % (self.inventory_endpoint, "newflight")
        response = RestHelper.post_request(endpoint, doc)
        self.log.debug("Result from create flight: %s" % response.json())
        if response.status_code != 200:
            result = False
            self.log.error("Requests status content:%s" % response.content)
        return result
