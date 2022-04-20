import hashlib
import json
import random
import urllib.parse
from random import choice

from e2e.e2e_base import E2EBaseTest
from e2e.rest_helper import RestHelper
from table_view import TableView


class ProfilesTests(E2EBaseTest):
    def setUp(self):
        E2EBaseTest.setUp(self)

    def tearDown(self):
        E2EBaseTest.tearDown(self)

    def __get_flight_ids(self, airline):
        rest_url = "http://%s/%s/%s" % (self.inventory_endpoint, "flights",
                                        urllib.parse.quote(airline))
        response = RestHelper.get_request(rest_url)
        if response.status_code != 200:
            return list()
        return [f_data["flights"]["flight_id"] for f_data in response.json()]

    def test_create_user(
            self, username="test_username", password="test_password",
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

        # Create user wallet
        card_num = "%0.12d" % random.randint(0, 999999999999)
        cvv = "%0.3d" % random.randint(0, 999)
        expiry = "%0.2d/%0.2d" % (random.randint(1, 12),
                                  random.randint(23, 30))
        result = self._create_user_wallet(
            doc_id="bankAccount", username=username,
            firstname=username, lastname=self.lastname,
            card_num=card_num, cvv=cvv, expiry=expiry)
        if not result:
            raise Exception("Failed to create user wallet")

        # Load user wallet
        result = self._load_wallet(username, 100)
        if not result:
            raise Exception("Failed to load user wallet")

        self.log.info("Deleting created user %s" % username)
        user_info.pop("password")
        rest_url = "http://%s/%s" % (self.profile_endpoint, "deleteUser")
        response = RestHelper.post_request(rest_url, user_info)

        if response.status_code != 200:
            raise Exception("Requests status content:{0}"
                            .format(response.content))
        self.log.info("Result from delete user endpoint: {0}"
                      .format(response.json()["Msg"]))

    def test_book_flight(self):
        num_tickets_to_book = 3
        flight_ids = self.__get_flight_ids("American Airlines")
        endpoint = "http://%s/%s" % (self.profile_endpoint, "confirmBooking")
        table = TableView(self.log.info)
        table.set_headers(["Username", "Booking ID", "Status",
                           "Class", "Num Seats"])
        for index in range(1, num_tickets_to_book):
            username = self.username_format % index
            booking_data = {
                "username": username,
                "password": self.password,
                "flightId": choice(flight_ids),
                "flightSeats": choice(range(1, 3)),
                "bookingClass": "economy",
                "bankAccount": hashlib.md5(
                    username.encode('utf-8')).hexdigest()
            }

            response = RestHelper.post_request(endpoint, booking_data)
            if response.status_code != 200:
                self.log.error("Request returned code %s: %s"
                               % (response.status_code, response.json()))
                self.fail("Booking failed")
            response = response.json()["Msg"]
            table.add_row([username, response["id"], response["status"],
                           response["bookingClass"], response["flightSeats"]])
        table.display("Booking details:")

    def test_list_booking_history(self):
        table = TableView(self.log.info)
        table.set_headers(["Username", "Num tickets", "IDs"])
        num_tickets_to_book = 3
        endpoint = "http://%s/%s" % (self.profile_endpoint, "allBookings")
        for index in range(1, num_tickets_to_book):
            username = self.username_format % index
            auth_data = {"username": username,
                         "password": self.password}
            response = RestHelper.post_request(endpoint, auth_data)
            if response.status_code != 200:
                self.log.error("Request returned code %s: %s"
                               % (response.status_code, response.json()))
                self.fail("Fetching booking history failed")
            bookings = response.json()["Msg"][0]["bookings"]
            table.add_row([username, len(bookings), "\n".join(bookings)])
        table.display("Booking history:")

    def test_get_booking_id(self):
        endpoint = "http://%s/%s" % (self.profile_endpoint, "allBookings")
        booked_tickets = list()
        for index in range(1, 11):
            username = self.username_format % index
            auth_data = {"username": username,
                         "password": self.password}
            response = RestHelper.post_request(endpoint, auth_data)
            if response.status_code != 200:
                self.log.error("Request returned code %s: %s"
                               % (response.status_code, response.json()))
                self.fail("Fetching booking history failed")
            bookings = response.json()["Msg"][0]["bookings"]
            if len(bookings) > 0:
                booked_tickets.append([username, bookings])

        target_user = choice(booked_tickets)
        booking_id = choice(target_user[1])
        self.log.info("User: %s, fetching booking info for '%s'"
                      % (target_user[0], booking_id))
        endpoint = "http://%s/%s" % (self.profile_endpoint, "getBooking")
        data = {"username": target_user[0], "password": self.password,
                "id": booking_id}
        response = RestHelper.post_request(endpoint, data)
        if response.status_code != 200:
            self.log.error("Request returned code %s: %s"
                           % (response.status_code, response.json()))
            self.fail("Fetching booking history failed")

        response = response.json()["Msg"]
        table = TableView(self.log.info)
        table.add_row(["Booking ID", response["id"]])
        table.add_row(["Flight", response["flightId"]])
        table.add_row(["Status", response["status"]])
        table.add_row(["Seats",
                       "%s (%s)" % (response["flightSeats"],
                                    ", ".join(response["TicketsBooked"]))])
        table.add_row(["Class", response["bookingClass"]])
        table.add_row(["Cost", response["totalCost"]])
        table.display("Booking info:")

    def test_cancel_booking(self):
        endpoint = "http://%s/%s" % (self.profile_endpoint, "allBookings")
        booked_tickets = list()
        for index in range(1, 11):
            username = self.username_format % index
            auth_data = {"username": username,
                         "password": self.password}
            response = RestHelper.post_request(endpoint, auth_data)
            if response.status_code != 200:
                self.log.error("Request returned code %s: %s"
                               % (response.status_code, response.json()))
                self.fail("Fetching booking history failed")
            bookings = response.json()["Msg"][0]["bookings"]
            if len(bookings) > 0:
                booked_tickets.append([username, bookings])

        target_user = choice(booked_tickets)
        booking_id = choice(target_user[1])
        self.log.info("Cancel %s for user %s"
                      % (booking_id, target_user[0]))
        endpoint = "http://%s/%s" % (self.profile_endpoint, "cancelBooking")
        data = {"username": target_user[0], "password": self.password,
                "id": booking_id}

        response = RestHelper.post_request(endpoint, data)
        if response.status_code != 200:
            self.log.error("Request returned code %s: %s"
                           % (response.status_code, response.json()))
            self.fail("Fetching booking history failed")

        # Fetch booking status to confirm cancellation
        endpoint = "http://%s/%s" % (self.profile_endpoint, "getBooking")
        data = {"username": target_user[0], "password": self.password,
                "id": booking_id}
        response = RestHelper.post_request(endpoint, data)
        if response.status_code != 200:
            self.log.error("Request returned code %s: %s"
                           % (response.status_code, response.json()))
            self.fail("Fetching booking history failed")

        response = response.json()["Msg"]
        table = TableView(self.log.info)
        table.add_row(["Booking ID", response["id"]])
        table.add_row(["Flight", response["flightId"]])
        table.add_row(["Status", response["status"]])
        table.add_row(["Seats",
                       "%s (%s)" % (response["flightSeats"],
                                    ", ".join(response["TicketsBooked"]))])
        table.add_row(["Class", response["bookingClass"]])
        table.display("Ticket status:")

        self.assertEqual(response["status"], "Booking Cancelled")
