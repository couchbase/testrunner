from .cbas_base import *


class CBASArrayIndexes(CBASBaseTest):
    ARRAY_OPT_ENABLED = "SET `compiler.arrayindex` \"true\";"
    ARRAY_OPT_DISABLED = "SET `compiler.arrayindex` \"false\";"
    CREATE_INDEX_STMTS = {
        "travelHotelLikesIdx": f"""
            CREATE INDEX  travelHotelLikesIdx 
            IF NOT EXISTS 
            ON            hotel (
               UNNEST public_likes : string
            );
        """,
        "travelHotelCleanServiceIdx": f"""
            CREATE INDEX  travelHotelCleanServiceIdx 
            IF NOT EXISTS
            ON            hotel (
                UNNEST reviews
                SELECT ratings.Cleanliness : bigint,
                       ratings.Service     : bigint
            );
        """,
        "travelHotelCityCleanServiceIdx": f"""
            CREATE INDEX  travelHotelCityCleanServiceIdx 
            IF NOT EXISTS
            ON            hotel (
                city : string,
                UNNEST reviews
                SELECT ratings.Cleanliness : bigint,
                       ratings.Service     : bigint
            );
        """,
        "travelHotelLocationIdx": f"""
            CREATE INDEX  travelHotelLocationIdx 
            IF NOT EXISTS
            ON            hotel (
                UNNEST reviews
                SELECT ratings.Location : bigint
            );
        """,
        "travelRouteUtcIdx": f"""
            CREATE INDEX  travelRouteUtcIdx 
            IF NOT EXISTS
            ON            route (
                UNNEST schedule
                SELECT utc : string
            );
        """
    }
    QUERIES = {
        1: f"""
            FROM          hotel H
            WHERE         "Vallie Ryan" IN H.public_likes
            SELECT        COUNT(*);
        """,
        2: f"""
            FROM          hotel H
            WHERE         ( SOME review IN H.reviews
                            SATISFIES review.ratings.Cleanliness > 3 AND
                                      review.ratings.Service > 3 )
            SELECT        H.id, H.name
            ORDER BY      H.id, H.name
            LIMIT         10;
        """,
        3: f"""
            FROM          hotel H
            WHERE         H.city = "Santa Barbara" AND
                          ( SOME review IN H.reviews
                            SATISFIES review.ratings.Cleanliness > 3 AND
                                      review.ratings.Service > 3 )
            SELECT        H.id, H.name
            ORDER BY      H.id, H.name
            LIMIT         10;
        """,
        4: f"""
            FROM          hotel H
            WHERE         ANY AND EVERY r IN H.reviews
                          SATISFIES r.ratings.Location > 3
            SELECT        COUNT(*);
        """,
        5: f"""
            FROM          route R1,
                          R1.schedule R1S,
                          route R2
            WHERE         R1.airlineid = "airline_3788" AND
                          ( SOME entry IN R2.schedule
                            SATISFIES entry.utc /* +indexnl */ = TO_STRING(R1S.utc) )
            SELECT        DISTINCT R1.utc, R2.airlineid
            ORDER BY      R2.airlineid ASC
            LIMIT         10;
        """,
        6: f"""
            FROM             route R1,
                             R1.schedule R1S
            LEFT OUTER JOIN  (
                FROM    route R2,
                        R2.schedule entry
                SELECT  entry.utc AS utc, R2.airlineid AS airlineid
            )               AS R2EU
            ON              R2EU.utc /* +indexnl */ = TO_STRING(R1S.utc)
            WHERE           R1.airlineid = "airline_2987"
            SELECT          DISTINCT R1S.utc, R2EU.airlineid
            ORDER BY        R2EU.airlineid ASC
            LIMIT           10;
        """
    }

    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        super(CBASArrayIndexes, self).setUp()

        # We will always use the travel-sample for array index tests.
        self.load_sample_buckets(servers=[self.master],
                                 bucketName=self.cb_bucket_name,
                                 total_items=63288)

        # Create dataset on the CBAS bucket. (By default, this is "travel_ds" in the "travel" bucket.)
        for dataset in ['hotel', 'route']:
            self.log.info(f'Creating dataset {dataset}.')
            self.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                          cbas_dataset_name=dataset,
                                          where_field='type',
                                          where_value=dataset)

    def tearDown(self):
        super(CBASArrayIndexes, self).tearDown()

    def test_array_index(self):
        index_name = self.input.param('index_name', None)
        if index_name is None:
            return

        query_num = int(self.input.param('query_id', None))
        self.log.info(f"*** Starting array index test for query number {query_num}. ***")

        stmt = ' '.join(self.CREATE_INDEX_STMTS[index_name].split())
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(stmt)
        self.assertTrue(status == "success", "Could not create array index!")
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)
        self.log.info('Waiting for index to populate...')
        self.sleep(20)

        stmt = self.ARRAY_OPT_DISABLED + ' '.join(self.QUERIES[query_num].split())
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(stmt)
        self.assertTrue(status == "success", f"Could not execute query {query_num} w/o compiler flag!")
        self.assertTrue(len(results) > 0, f"No results found after executing query {query_num} w/o compiler flag.")
        non_optimized_results = results

        stmt = self.ARRAY_OPT_ENABLED + ' '.join(self.QUERIES[query_num].split())
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(stmt)
        self.assertTrue(status == "success", f"Could not execute query {query_num} w/ compiler flag!")
        for result in results:
            self.assertTrue(result in non_optimized_results, f"Result {result} not found in non-optimized results!")

        stmt = self.ARRAY_OPT_ENABLED + "EXPLAIN " + ' '.join(self.QUERIES[query_num].split())
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(stmt)
        self.assertTrue("index-search" in str(results), "INDEX-SEARCH is found in EXPLAIN. Index is not being used!")
        self.assertTrue(index_name in str(results), "Index is not being used!")
