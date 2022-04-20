import urllib.parse

from e2e.e2e_base import E2EBaseTest
from e2e.rest_helper import RestHelper
from table_view import TableView


class InventoryTests(E2EBaseTest):
    def setUp(self):
        E2EBaseTest.setUp(self)

    def tearDown(self):
        E2EBaseTest.tearDown(self)

    def test_get_flights_for_airline(self):
        target_airline = "American Airlines"
        rest_url = "http://%s/%s/%s" % (self.inventory_endpoint, "flights",
                                        urllib.parse.quote(target_airline))
        response = RestHelper.get_request(rest_url)
        if response.status_code != 200:
            raise Exception("Requests status content:{0}"
                            .format(response.content))
        self.log.info("Flights for airline: %s" % target_airline)
        table = TableView(self.log.info)
        table.set_headers(["Flight Id", "Model", "Departure",
                           "Arrival", "Departure Time", "Status"])
        for f_data in response.json():
            f_data = f_data["flights"]
            table.add_row(
                [f_data["flight_id"], f_data["model"],
                 f_data["departing_airport"], f_data["arriving_airport"],
                 f_data["departure_date"], f_data["status"]])

        table.display("Flights for airline: %s" % target_airline)
