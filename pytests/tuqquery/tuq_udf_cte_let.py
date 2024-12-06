from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
import time
from deepdiff import DeepDiff
from membase.api.exception import CBQError
import threading

class QueryUdfCteLetTests(QueryTests):

    def setUp(self):
        super(QueryUdfCteLetTests, self).setUp()
        self.log.info("==============  QueryUdfCteLetTests setup has started ==============")
        self.fully_qualified = self.input.param("fully_qualified", False)

        self.bucket_name = "travel-sample"
        self.rest.load_sample("travel-sample")
        init_time = time.time()
        while True:
            next_time = time.time()
            query_response = self.run_cbq_query("SELECT COUNT(*) FROM `" + self.bucket_name + "`")
            self.log.info(f"{self.bucket_name}+ count: {query_response['results'][0]['$1']}")
            if query_response['results'][0]['$1'] == 31591:
                break
            if next_time - init_time > 600:
                break
            time.sleep(2)

        self.wait_for_all_indexes_online(build_deferred=True)
        
        self.log.info("==============  QueryUdfCteLetTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryUdfCteLetTests, self).suite_setUp()
        self.log.info("==============  QueryUdfCteLetTests suite_setup has started ==============")
        self.log.info("==============  QueryUdfCteLetTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryUdfCteLetTests tearDown has started ==============")
        travel_sample = self.get_bucket_from_name("travel-sample")
        if travel_sample:
            self.delete_bucket(travel_sample)
        self.log.info("==============  QueryUdfCteLetTests tearDown has completed ==============")
        super(QueryUdfCteLetTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryUdfCteLetTests suite_tearDown has started ==============")
        self.log.info("==============  QueryUdfCteLetTests suite_tearDown has completed ==============")
        super(QueryUdfCteLetTests, self).suite_tearDown()

    def test_simple_udf(self):
        if self.fully_qualified:
            collection = "`travel-sample`.inventory.airport"
        else:
            collection = "airport"
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.all_paris_airport() {{
            (SELECT airportname FROM {collection} WHERE city = 'Paris' ORDER BY airportname)
        }}
        """
        self.run_cbq_query(query)
        query = "EXECUTE FUNCTION `travel-sample`.inventory.all_paris_airport()"
        result = self.run_cbq_query(query)
        expected = [
            {"airportname": "All Airports"},
            {"airportname": "Charles De Gaulle"},
            {"airportname": "Gare Montparnasse"},
            {"airportname": "Gare de LEst"},
            {"airportname": "Gare de Lyon"},
            {"airportname": "Gare du Nord"},
            {"airportname": "La Defense Heliport"},
            {"airportname": "Le Bourget"},
            {"airportname": "Orly"}
        ]
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")

    def test_simple_udf_with_parameters(self):
        if self.fully_qualified:
            collection = "`travel-sample`.inventory.airport"
        else:
            collection = "airport"
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.airport_by_city(city_name) {{
            (SELECT airportname FROM {collection} WHERE city = city_name ORDER BY airportname)
        }}
        """
        self.run_cbq_query(query)
        query = "EXECUTE FUNCTION `travel-sample`.inventory.airport_by_city('Paris')"
        result = self.run_cbq_query(query)
        expected = [
            {"airportname": "All Airports"},
            {"airportname": "Charles De Gaulle"},
            {"airportname": "Gare Montparnasse"},
            {"airportname": "Gare de LEst"},
            {"airportname": "Gare de Lyon"},
            {"airportname": "Gare du Nord"},
            {"airportname": "La Defense Heliport"},
            {"airportname": "Le Bourget"},
            {"airportname": "Orly"}
        ]
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")

    def test_udf_correlated_cte(self):
        if self.fully_qualified:
            airport_collection = "`travel-sample`.inventory.airport"
            hotel_collection = "`travel-sample`.inventory.hotel"
        else:
            airport_collection = "airport"
            hotel_collection = "hotel"
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.correlated_cte_col() {{ (
            WITH
                cte1 AS (SELECT city FROM {airport_collection} WHERE airportname = 'Charles De Gaulle'),
                cte2 AS (SELECT name FROM {hotel_collection} WHERE city IN (SELECT RAW city FROM cte1) AND lower(name) like '%hyatt%')
                SELECT * FROM cte2
            )
        }}
        """
        self.run_cbq_query(query)
        query = "EXECUTE FUNCTION `travel-sample`.inventory.correlated_cte_col()"
        result = self.run_cbq_query(query)
        expected = [
            {'cte2': {'name': 'Hyatt Regency Paris Etoile'}},
            {'cte2': {'name': 'Hyatt Regency Paris-Madeleine'}}
        ]
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")

    def test_udf_correlated_cte_with_parameters(self):
        if self.fully_qualified:
            airport_collection = "`travel-sample`.inventory.airport"
            hotel_collection = "`travel-sample`.inventory.hotel"
        else:
            airport_collection = "airport"
            hotel_collection = "hotel"
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.correlated_cte_col(airport_name, hotel_string) {{ (
            WITH
            cte1 AS (SELECT city FROM {airport_collection} WHERE airportname = airport_name),
            cte2 AS (SELECT name FROM {hotel_collection} WHERE city IN (SELECT RAW city FROM cte1) AND lower(name) like '%' || hotel_string || '%')
            SELECT * FROM cte2
            )
        }}
        """
        self.run_cbq_query(query)
        query = "EXECUTE FUNCTION `travel-sample`.inventory.correlated_cte_col('Charles De Gaulle', 'hyatt')"
        result = self.run_cbq_query(query)
        expected = [
            {'cte2': {'name': 'Hyatt Regency Paris Etoile'}},
            {'cte2': {'name': 'Hyatt Regency Paris-Madeleine'}}
        ]
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")

    def test_udf_correlated_cte_array_param(self):
        if self.fully_qualified:
            airport_collection = "`travel-sample`.inventory.airport"
            hotel_collection = "`travel-sample`.inventory.hotel"
        else:
            airport_collection = "airport"
            hotel_collection = "hotel"

        query = f"update `travel-sample`.inventory.hotel h SET v.test = [1,2,3] FOR v IN h.reviews END"
        self.run_cbq_query(query)

        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.correlated_cte_param_array(airport_name, service_level) {{ (
            WITH
                cte1 AS (SELECT city FROM {airport_collection} WHERE airportname = airport_name),
                cte2 AS (
                    SELECT h.name
                    FROM {hotel_collection} h 
                    WHERE h.city IN (SELECT RAW city FROM cte1) AND ANY x IN h.reviews SATISFIES EVERY a IN x.test SATISFIES a > service_level END END
                )
                SELECT DISTINCT cte2.name FROM cte2 ORDER BY cte2.name LIMIT 5
            ) }}
        """
        self.run_cbq_query(query)
        query = "EXECUTE FUNCTION `travel-sample`.inventory.correlated_cte_param_array('Charles De Gaulle', 0)"
        result = self.run_cbq_query(query)
        expected = [{'name': 'Aloha Youth Hostel'}, {'name': 'Castille Paris'}, {'name': 'Centre International BVJ Paris-Louvre'}, {'name': 'Champs Elysées Plaza'}, {'name': 'Hotel Ampère'}]
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")

    def test_udf_cte_unnest(self):
        if self.fully_qualified:
            city_hotel = "`travel-sample`.inventory.city_hotel"
            hotel_collection = "`travel-sample`.inventory.hotel"
        else:
            city_hotel = "city_hotel"
            hotel_collection = "hotel"
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.city_hotel(city_name) {{ (
            SELECT * FROM {hotel_collection} WHERE city = city_name
        ) }}
        """
        self.run_cbq_query(query)
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.cte_udf_unnest(city_name) {{ (
            WITH CTE AS
                    (
                    SELECT a.*, abc.hotel.name, abc.hotel.city
                    FROM {city_hotel}(city_name) as abc
                    UNNEST abc.hotel.reviews a
                ) SELECT author, city, name, ratings FROM CTE ORDER BY name, author Limit 1
        ) }}
        """
        self.run_cbq_query(query)

        query = "EXECUTE FUNCTION `travel-sample`.inventory.cte_udf_unnest('Paris')"
        result = self.run_cbq_query(query)
        expected = [{
            'author': 'Agustin Bosco',
            'city': 'Paris',
            'name': 'Aloha Youth Hostel',
            'ratings': {'Cleanliness': 5, 'Location': 5, 'Overall': 4, 'Rooms': 5, 'Service': 5, 'Sleep Quality': 5, 'Value': 5}
        }]
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")

    def test_udf_correlated_let(self):
        if self.fully_qualified:
            airport_collection = "`travel-sample`.inventory.airport"
            hotel_collection = "`travel-sample`.inventory.hotel"
        else:
            airport_collection = "airport"
            hotel_collection = "hotel"
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.correlated_let() {{ (
        SELECT cte2
        LET
            cte1 = (SELECT RAW city FROM {airport_collection} WHERE airportname = 'Charles De Gaulle'),
            cte2 = (SELECT name FROM {hotel_collection} WHERE city IN cte1 AND lower(name) like '%hyatt%')
        ) }}
        """
        self.run_cbq_query(query)
        query = "EXECUTE FUNCTION `travel-sample`.inventory.correlated_let()"
        result = self.run_cbq_query(query)
        expected = [
            {'cte2': [
                {'name': 'Hyatt Regency Paris Etoile'}, 
                {'name': 'Hyatt Regency Paris-Madeleine'}
            ]}
        ]
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")

    def test_udf_correlated_let_with_parameters(self):
        if self.fully_qualified:
            airport_collection = "`travel-sample`.inventory.airport"
            hotel_collection = "`travel-sample`.inventory.hotel"
        else:
            airport_collection = "airport"
            hotel_collection = "hotel"
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.correlated_let(city_name, hotel_string) {{ (
        SELECT cte2
        LET
            cte1 = (SELECT RAW city FROM {airport_collection} WHERE airportname = city_name),
            cte2 = (SELECT name FROM {hotel_collection} WHERE city IN cte1 AND lower(name) like '%' || hotel_string || '%')
        ) }}
        """
        self.run_cbq_query(query)
        query = "EXECUTE FUNCTION `travel-sample`.inventory.correlated_let('Charles De Gaulle', 'hyatt')"
        result = self.run_cbq_query(query)
        expected = [
            {'cte2': [
                {'name': 'Hyatt Regency Paris Etoile'}, 
                {'name': 'Hyatt Regency Paris-Madeleine'}
            ]}
        ]
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")

    def test_udf_correlated_let_array_param(self):
        if self.fully_qualified:
            airport_collection = "`travel-sample`.inventory.airport"
            hotel_collection = "`travel-sample`.inventory.hotel"
        else:
            airport_collection = "airport"
            hotel_collection = "hotel"

        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.correlated_let_param_array(airport_name, service_level) {{ (
            SELECT cte2
            LET
                cte1 = (SELECT RAW city FROM {airport_collection} WHERE airportname = airport_name),
                cte2 = (SELECT h.name FROM {hotel_collection} h WHERE h.city IN cte1 AND ANY x IN h.reviews SATISFIES x.ratings.Service > service_level END ORDER BY h.name LIMIT 5)
        ) }}
        """
        self.run_cbq_query(query)
        query = "EXECUTE FUNCTION `travel-sample`.inventory.correlated_let_param_array('Charles De Gaulle', 0)"
        result = self.run_cbq_query(query)
        expected = [{'cte2': [
            {'name': 'Aloha Youth Hostel'},
            {'name': 'Castille Paris'},
            {'name': 'Centre International BVJ Paris-Louvre'},
            {'name': 'Champs Elysées Plaza'},
            {'name': 'Hotel Ampère'}
        ]}]
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")

    def test_udf_let_unnest(self):
        if self.fully_qualified:
            city_hotel = "`travel-sample`.inventory.city_hotel"
            hotel_collection = "`travel-sample`.inventory.hotel"
        else:
            city_hotel = "city_hotel"
            hotel_collection = "hotel"
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.city_hotel(city_name) {{ (
            SELECT * FROM {hotel_collection} WHERE city = city_name
        ) }}
        """
        self.run_cbq_query(query)
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.cte_let_unnest(city_name) {{ (
            SELECT CTE
            LET CTE = (
                    SELECT a.author, a.ratings, abc.hotel.name, abc.hotel.city
                    FROM {city_hotel}(city_name) as abc
                    UNNEST abc.hotel.reviews a
                    ORDER BY abc.hotel.name, a.author
                    LIMIT 1 
            )
        ) }}
        """
        self.run_cbq_query(query)
        query = "EXECUTE FUNCTION `travel-sample`.inventory.cte_let_unnest('Paris')"
        result = self.run_cbq_query(query)
        expected = [
            {'CTE': [{
                'author': 'Agustin Bosco',
                'city': 'Paris',
                'name': 'Aloha Youth Hostel',
                'ratings': {'Service': 5.0, 'Cleanliness': 5.0, 'Overall': 4.0, 'Value': 5.0, 'Sleep Quality': 5.0, 'Rooms': 5.0, 'Location': 5.0}
            }]}
        ]
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")

    def test_udf_recursive(self):
        if self.fully_qualified:
            udf_recursion = "`travel-sample`.inventory.udf_recursion"
        else:
            udf_recursion = "udf_recursion"
        query = "CREATE OR REPLACE FUNCTION `travel-sample`.inventory.udf_recursion() { ( SELECT 1 ) }"
        self.run_cbq_query(query)
        query = f"""
        CREATE OR REPLACE FUNCTION `travel-sample`.inventory.udf_recursion(Item, AllItems, NumberOfRecursion)
        {{(
            CASE
                WHEN NumberOfRecursion-1 < 0 then "end"
                WHEN Item.parentId is missing then Item
                ELSE {udf_recursion}(AllItems.[Item.parentId], AllItems, NumberOfRecursion-1)
            END
        ) }};
        """
        self.run_cbq_query(query)
        query = """
        EXECUTE FUNCTION `travel-sample`.inventory.udf_recursion(
            {"parentId": "6789", "value": "ChildData4"},
            {"1234": {"value": "ParentData1"},
            "4567": {"value": "ParentData2"},
            "9876": {"parentId": "1234", "value": "ChildData1"},
            "6789": {"parentId": "4567", "value": "ChildData2"},
            "5555": {"parentId": "1234", "value": "ChildData3"},
            "7777": {"parentId": "6789", "value": "ChildData4"}
            }, 4)
        """        
        result = self.run_cbq_query(query)
        expected = {"value": "ParentData2"}
        self.assertEqual(result['results'][0], expected, f"Results are not as expected: {result['results'][0]}")
