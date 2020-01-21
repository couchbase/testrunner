from .cbas_base import *


class QueryDetails:
    query_details = [
        {
            "id": "extential_quantification",
            "dataset_id": "beer-sample",
            "query": "WITH nested_breweries AS (SELECT bw.name AS brewer, bw.phone, (SELECT br.name, br.abv FROM beers br WHERE br.brewery_id = meta(bw).id) AS beers FROM breweries bw) SELECT VALUE nb FROM nested_breweries nb WHERE (SOME b IN nb.beers SATISFIES b.name LIKE \"%IPA%\") LIMIT 5;",
            "expected_status": "success",
            "expected_hits": "5"
        },
        {
            "id": "universal_quantification",
            "dataset_id": "beer-sample",
            "query": "WITH nested_breweries AS (SELECT bw.name AS brewer, bw.phone, (SELECT br.name, br.abv FROM beers br WHERE br.brewery_id = meta(bw).id) AS beers FROM breweries bw) SELECT VALUE nb FROM nested_breweries nb WHERE (EVERY b IN nb.beers SATISFIES b.name LIKE \"%IPA%\") LIMIT 5;",
            "expected_status": "success",
            "expected_hits": "5"
        },
        {
            "id": "lookup_table_metadata",
            "dataset_id": "beer-sample",
            "query": "select DataverseName from Metadata.`Dataverse`;",
            "expected_status": "success",
            "expected_hits": "2"
        },
        {
            "id": "simple_aggregation",
            "dataset_id": "beer-sample",
            "query": "SELECT COUNT(*) AS num_beers FROM beers;",
            "expected_status": "success",
            "expected_hits": "1"
        },
        {
            "id": "simple_aggregation_unwrapped",
            "dataset_id": "beer-sample",
            "query": "SELECT VALUE COUNT(b) FROM beers b;",
            "expected_status": "success",
            "expected_hits": "1"
        },
        {
            "id": "aggregation_array_count",
            "dataset_id": "beer-sample",
            "query": "SELECT VALUE ARRAY_COUNT((SELECT b FROM beers b));",
            "expected_status": "success",
            "expected_hits": "1"
        },
        {
            "id": "grouping_aggregation",
            "dataset_id": "beer-sample",
            "query": "SELECT br.brewery_id, COUNT(*) AS num_beers FROM beers br GROUP BY br.brewery_id HAVING num_beers > 30 and br.brewery_id != \"\";",
            "expected_status": "success",
            "expected_hits": "11"
        },
        {
            "id": "hash_based_grouping_aggregation",
            "dataset_id": "beer-sample",
            "query": "SELECT br.brewery_id, COUNT(*) AS num_beers FROM beers br /*+ hash */ GROUP BY br.brewery_id HAVING num_beers > 30 and br.brewery_id != \"\";",
            "expected_status": "success",
            "expected_hits": "11"
        },
        {
            "id": "grouping_limits",
            "dataset_id": "beer-sample",
            "query": "SELECT bw.name, COUNT(*) AS num_beers, AVG(br.abv) AS abv_avg, MIN(br.abv) AS abv_min, MAX(br.abv) AS abv_max FROM breweries bw, beers br WHERE br.brewery_id = meta(bw).id GROUP BY bw.name ORDER BY num_beers DESC LIMIT 3;",
            "expected_status": "success",
            "expected_hits": "3"
        },
        {
            "id": "equijoin_limits",
            "dataset_id": "beer-sample",
            "query": "SELECT * FROM beers b1, beers b2 WHERE b1.name = b2.name AND b1.brewery_id != b2.brewery_id LIMIT 20;",
            "expected_status": "success",
            "expected_hits": "20"
        }
    ]

    dataset_details = [
        {
            "id": "beer-sample",
            "cb_bucket_name": "beer-sample",
            "cbas_bucket_name": "beerBucket",
            "ds_details": [
                {
                    "ds_name": "beers",
                    "where_field": "type",
                    "where_value": "beer"
                },
                {
                    "ds_name": "breweries",
                    "where_field": "type",
                    "where_value": "brewery"
                }
            ]
        }
    ]

    def __init__(self, query_id):
        self.query_id = query_id

    def get_query_and_dataset_details(self):
        query_record = None
        dataset_record = None

        for record in self.query_details:
            if record['id'] == self.query_id:
                query_record = record

        if query_record:
            for record in self.dataset_details:
                if record['id'] == query_record['dataset_id']:
                    dataset_record = record

        return query_record, dataset_record


class CBASDemoQueries(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket":False})
        super(CBASDemoQueries, self).setUp()

    def tearDown(self):
        super(CBASDemoQueries, self).tearDown()

    def test_demo_query(self):
        query_details = QueryDetails(self.query_id)
        query_record, dataset_record = query_details.get_query_and_dataset_details()

        # Delete Default bucket and load beer-sample bucket
#         self.cluster.bucket_delete(server=self.master, bucket="default")
        self.load_sample_buckets(servers=[self.master],
                                 bucketName=dataset_record['cb_bucket_name'],
                                 total_items=self.beer_sample_docs_count)
        if "add_all_cbas_nodes" in self.input.test_params and self.input.test_params["add_all_cbas_nodes"] and len(self.cbas_servers) > 1:
            self.add_all_cbas_node_then_rebalance()
        # Create bucket on CBAS
        self.create_bucket_on_cbas(
            cbas_bucket_name=dataset_record['cbas_bucket_name'],
            cb_bucket_name=dataset_record['cb_bucket_name'],
            cb_server_ip=self.cb_server_ip)

        # Create datasets on the CBAS bucket
        for dataset in dataset_record['ds_details']:
            self.create_dataset_on_bucket(
                cbas_bucket_name=dataset_record['cbas_bucket_name'],
                cbas_dataset_name=dataset['ds_name'],
                where_field=dataset['where_field'],
                where_value=dataset['where_value'])

        # Connect to Bucket
        self.connect_to_bucket(
            cbas_bucket_name=dataset_record['cbas_bucket_name'],
            cb_bucket_password=self.cb_bucket_password)
        
        num_items = self.get_item_count(self.master, dataset_record['cb_bucket_name'])
        self.assertTrue(self.wait_for_ingestion_complete(["beers", "breweries"], num_items), "Data ingestion couldn't complete in 300 secs")

        # Execute Query
        status, metrics, errors, results, _ = self.execute_statement_on_cbas(
            query_record['query'], self.master)
        self.log.info('Actual Status : ' + status)
        self.log.info('Expected Status : ' + query_record['expected_status'])
        self.log.info('Actual # Hits : ' + str(metrics['resultCount']))
        self.log.info('Expected # Hits : ' + query_record['expected_hits'])

        # Validate Query output
        result = False
        if (status == query_record['expected_status']) and (
            int(metrics['resultCount']) == int(query_record['expected_hits'])):
            result = True

        if not result:
            self.fail("FAIL : Status and/or # Hits not as expected")
