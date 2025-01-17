from .tuq import QueryTests
import random
import string
from ast import literal_eval

class QueryTimeSeriesTests(QueryTests):
    def setUp(self):
        super(QueryTimeSeriesTests, self).setUp()
        self.range_t1 = self.input.param("range_t1", "2023-01-05T08:00:00Z")
        self.range_t2 = self.input.param("range_t2", "2023-01-05T17:00:00Z")
        self.ts_project = self.input.param("ts_project", [0,3,1,5])
        self.ts_ranges = self.input.param("ts_ranges", ["Jan-1", "Jan-3"])
        self.ts_interval = self.input.param("ts_interval", 0)
        self.bucket = "default"
        self.scope = "stocks"
        self.collection = "price"
        self.collection_flat = "price_flat"

    def suite_setUp(self):
        super(QueryTimeSeriesTests, self).suite_setUp()
        self.run_cbq_query(f"CREATE SCOPE {self.bucket}.stocks if not exists")
        self.sleep(3)
        self.run_cbq_query(f"CREATE COLLECTION {self.bucket}.stocks.price if not exists")
        self.run_cbq_query(f"CREATE COLLECTION {self.bucket}.stocks.price_flat if not exists")
        self.run_cbq_query(f"CREATE PRIMARY INDEX if not exists ON {self.bucket}.stocks.price")
        self.run_cbq_query(f"CREATE PRIMARY INDEX if not exists ON {self.bucket}.stocks.price_flat")
        self.run_cbq_query(f"DELETE FROM {self.bucket}.stocks.price")
        self.run_cbq_query(f"DELETE FROM {self.bucket}.stocks.price_flat")
        self.run_cbq_query(f"CREATE INDEX idx1 if not exists ON {self.bucket}.stocks.price(ticker, ts_start, ts_end)")
        self.run_cbq_query(f"CREATE INDEX idx2 if not exists ON {self.bucket}.stocks.price(ticker)")

        # Generate datapoint every hour (24) with one document per day 
        # Data from Jan-1 to Jan-10
        self.populate_data(f"{self.bucket}.stocks.price", "IBM", "2023-01-01T00:00:00Z", "2023-01-10T00:00:00Z")
        # Data from Jan-3 to Jan-7
        self.populate_data(f"{self.bucket}.stocks.price", "GOOG", "2023-01-03T00:00:00Z", "2023-01-07T00:00:00Z")
        # Data from Jan-5 to Jan-12
        self.populate_data(f"{self.bucket}.stocks.price", "BASE", "2023-01-05T00:00:00Z", "2023-01-12T00:00:00Z")
        # Regular Data from Jan-1 to Jan-10
        self.populate_data(f"{self.bucket}.stocks.price", "AAPL", "2023-01-05T00:00:00Z", "2023-01-12T00:00:00Z", regular=True)

        # Put data generated above in flat collection with one datapoint per document
        self.populate_flat_data(f"{self.bucket}.stocks.price", f"{self.bucket}.stocks.price_flat")

    def tearDown(self):
        super(QueryTimeSeriesTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryTimeSeriesTests, self).suite_tearDown()

    def populate_data(self, collection, ticker, date_start, date_end, regular=False, interval="hour", document_period="day", document_increment=1):

        ms_per_unit = {"second": 1000, "minute": 60000, "hour": 3600000, "day": 86400000, "week": 604800000}

        insert_irregular = f'INSERT INTO {collection} (KEY k, VALUE v) \
            SELECT $ticker ||":"|| d AS k, \
                {{"ticker": $ticker, "ts_start":dstart, "ts_end":dstart+dend-1*$interval, "ts_interval": 0,\
                  "ts_data": ARRAY [ dstart+pos, price+round(random()*10,2) , volume+round(random()*100,0), \
                                    "{"".join(random.choices(string.ascii_lowercase, k=10))}", {random.choice([True, False])}, null, \
                                    {{"prix": {random.randint(0,10)*10}}}, \
                                    {random.choices(string.ascii_uppercase, k=5)} ] FOR pos IN ARRAY_RANGE(0, dend, intrvl) END }} AS v \
            FROM DATE_RANGE_STR($start, $end, "{document_period}", {document_increment}) AS d \
            LET dstart = STR_TO_MILLIS(d), \
                intrvl = {ms_per_unit[interval]}, \
                dend = {ms_per_unit[document_period]*document_increment}, \
                price = 120.0, \
                volume = 80;'
        
        insert_regular = f'INSERT INTO {collection} (KEY k, VALUE v) \
            SELECT $ticker ||":"|| d AS k, \
                {{"ticker": $ticker, "ts_start":dstart, "ts_end":dstart+dend-1*$interval, "ts_interval": $interval,\
                  "ts_data": ARRAY [ price+round(random()*10,2) , volume+round(random()*100,0), \
                                    "{"".join(random.choices(string.ascii_lowercase, k=10))}", {random.choice([True, False])}, null, \
                                    {{"prix": {random.randint(0,10)*10}}}, \
                                    {random.choices(string.ascii_uppercase, k=5)} ] FOR pos IN ARRAY_RANGE(0, dend, intrvl) END }} AS v \
            FROM DATE_RANGE_STR($start, $end, "{document_period}", {document_increment}) AS d \
            LET dstart = STR_TO_MILLIS(d), \
                intrvl = {ms_per_unit[interval]}, \
                dend = {ms_per_unit[document_period]*document_increment}, \
                price = 120.0, \
                volume = 80;'
        
        if regular:
            insert = insert_regular
        else:
            insert = insert_irregular

        result = self.run_cbq_query(insert, query_params={"$ticker": f'"{ticker}"', "$start": f'"{date_start}"', "$end": f'"{date_end}"', "$interval": ms_per_unit[interval]})

    def populate_flat_data(self,from_collection, into_collection):
        # Data point is in single document as:
        # - ticker: IBM
        # - ts: 1682947800000
        # - values: ...
        insert_from_ts = f'INSERT INTO {into_collection} (KEY k, VALUE v)\
            SELECT d.ticker ||":"|| MILLIS_TO_TZ(t._t, "UTC") as k,\
                {{"ticker": d.ticker, "t": t._t, "price": t._v0, "volume": t._v1}} as v\
            FROM {from_collection} as d UNNEST _timeseries(d) AS t'
        result = self.run_cbq_query(insert_from_ts)

    
    def test_query_all(self):
        ts_query = f'SELECT MILLIS_TO_TZ(min(t._t), "UTC"), MILLIS_TO_TZ(max(t._t), "UTC"), sum(t._v0) FROM default.stocks.price as d UNNEST _timeseries(d) AS t'
        ts_query_flat = f'SELECT MILLIS_TO_TZ(min(d.t), "UTC"), MILLIS_TO_TZ(max(d.t), "UTC"), sum(d.price) FROM default.stocks.price_flat as d'
        result1 = self.run_cbq_query(ts_query)
        result2 = self.run_cbq_query(ts_query_flat)
        self.assertTrue(result1['results'])
        self.assertEqual(result1['results'], result2['results'])

    def test_query_range(self):
        get_range = self.run_cbq_query(f'SELECT RAW [ STR_TO_MILLIS("{self.range_t1}"), STR_TO_MILLIS("{self.range_t2}")]')
        ts_range1 = get_range['results'][0][0]
        ts_range2 = get_range['results'][0][1]
        ts_query = f'SELECT MILLIS_TO_TZ(min(t._t), "UTC") AS t1, MILLIS_TO_TZ(max(t._t), "UTC") as t2, sum(t._v0) as sum_v0 FROM default.stocks.price as d UNNEST _timeseries(d, {{"ts_ranges": [{ts_range1}, {ts_range2}]}}) AS t WHERE d.ticker in ["IBM", "GOOG", "AAPL"] AND (d.ts_start <= {ts_range2} AND d.ts_end >= {ts_range1})'
        result1 = self.run_cbq_query(ts_query)
        self.assertEqual(result1['results'][0]['t1'], self.range_t1)
        self.assertEqual(result1['results'][0]['t2'], self.range_t2)
        self.assertTrue(result1['results'][0]['sum_v0'] > 0)

    def test_value_project(self):
        ts_ranges = [1672560000000, 1672560000000+1]
        project_values = "t._t"
        if type(self.ts_project) is str:
            for pos in sorted(literal_eval(self.ts_project)):
                project_values += f", t._v{pos}"
        else:
            project_values += f", t._v{self.ts_project}"
        ts_query1 = f'SELECT t.* FROM default.stocks.price as d UNNEST _timeseries(d, {{"ts_project": {self.ts_project}, "ts_ranges": {ts_ranges}}}) AS t WHERE d.ticker = "IBM"'
        ts_query2 = f'SELECT {project_values} FROM default.stocks.price as d UNNEST _timeseries(d, {{"ts_ranges": {ts_ranges}}}) AS t WHERE d.ticker = "IBM"'
        result1 = self.run_cbq_query(ts_query1)
        result2 = self.run_cbq_query(ts_query2)
        self.assertEqual(result1['results'], result2['results'])

    def test_value_complex(self):
        ts_ranges = [1672560000000, 1672560000000+1]
        ts_query1 = f'SELECT t._t, t._v5, t._v5.[\'prix\'] as v5_prix, t._v6, t._v6[3] as v6_3 FROM default.stocks.price as d UNNEST _timeseries(d, {{"ts_ranges": {ts_ranges}}}) AS t WHERE d.ticker = "IBM"'
        result1 = self.run_cbq_query(ts_query1)
        self.assertTrue(type(result1['results'][0]['_v5']) is dict)
        self.assertTrue(type(result1['results'][0]['v5_prix']) is int)
        self.assertTrue(type(result1['results'][0]['_v6']) is list)
        self.assertTrue(type(result1['results'][0]['v6_3']) is str)

    def test_value_missing(self):
        ts_query = 'SELECT d.ticker, t._t, t._v0, t._v1 FROM \
            [ { "ts_data": [ [10], [11], null, [13], [14] ], "ts_start":1, "ts_end":4, "ticker": "ONE", "ts_interval": 1 }, \
              { "ts_data": [ [20], [21], [22, null, 223], [23,230] ], "ts_start":1, "ts_end":4, "ticker": "TWO", "ts_interval": 1 }, \
              { "ts_data": [ [3,52], null, [2,[51]]], "ts_start":2, "ts_end":5, "ticker": "FIVE", "ts_interval": 0} ] AS d\
                LEFT UNNEST _timeseries(d, {"ts_ranges": [1,4]}) AS t'
        result = self.run_cbq_query(ts_query)
        expected_result = [
            {'_t': 1, '_v0': 10, 'ticker': 'ONE'}, {'_t': 2, '_v0': 11, 'ticker': 'ONE'},
            {'_t': 3, '_v0': None, 'ticker': 'ONE'}, {'_t': 4, '_v0': 13, 'ticker': 'ONE'},
            {'_t': 1, '_v0': 20, 'ticker': 'TWO'}, {'_t': 2, '_v0': 21, 'ticker': 'TWO'},
            {'_t': 3, '_v0': 22, '_v1': None, 'ticker': 'TWO'}, {'_t': 4, '_v0': 23, '_v1': 230, 'ticker': 'TWO'},
            {'_t': 3, '_v0': 52, 'ticker': 'FIVE'}, {'_t': 2, '_v0': [51], 'ticker': 'FIVE'}]
        self.assertEqual(result['results'], expected_result)

    def test_value_mix(self):
        ts_query = 'SELECT d.ticker, t._t, t._v0, t._v1 FROM \
            [ { "ts_data": [ [10], [11], [12], [13], [14] ], "ts_start":1, "ts_end":4, "ticker": "ONE", "ts_interval": 1 }, \
              { "ts_data": [ [20], [21], [22], [23,230] ], "ts_start":1, "ts_end":4, "ticker": "TWO", "ts_interval": 1 }, \
              { "ts_data": [ [30], [31], [32], [33] ], "ts_start":1, "ts_end":4, "ticker": "THREE", "ts_interval": 2 }, \
              { "ts_data": [ [2,41], [5,"44"], [3,"FORTY TWO"], [6,45]], "ts_start":2, "ts_end":6, "ticker": "FOUR"}, \
              { "ts_data": [ [3,52], [2,[51]]], "ts_start":2, "ts_end":5, "ticker": "FIVE", "ts_interval": 0} ] AS d\
                LEFT UNNEST _timeseries(d, {"ts_ranges": [1,4]}) AS t'
        result = self.run_cbq_query(ts_query)
        expected_result = [
            {'_t': 1, '_v0': 10, 'ticker': 'ONE'}, {'_t': 2, '_v0': 11, 'ticker': 'ONE'},
            {'_t': 3, '_v0': 12, 'ticker': 'ONE'}, {'_t': 4, '_v0': 13, 'ticker': 'ONE'},
            {'_t': 1, '_v0': 20, 'ticker': 'TWO'}, {'_t': 2, '_v0': 21, 'ticker': 'TWO'},
            {'_t': 3, '_v0': 22, 'ticker': 'TWO'}, {'_t': 4, '_v0': 23, '_v1': 230, 'ticker': 'TWO'},
            {'_t': 1, '_v0': 30, 'ticker': 'THREE'}, {'_t': 3, '_v0': 31, 'ticker': 'THREE'},
            {'_t': 2, '_v0': 41, 'ticker': 'FOUR'}, {'_t': 3, '_v0': 'FORTY TWO', 'ticker': 'FOUR'},
            {'_t': 3, '_v0': 52, 'ticker': 'FIVE'}, {'_t': 2, '_v0': [51], 'ticker': 'FIVE'}]
        self.assertEqual(result['results'], expected_result)

    def test_meta_path(self):
        # Query with explicit path to non default
        ts_query1 = 'SELECT d.ticker, t._t, t._v0 FROM \
            [ { "datapoint": [ [2,41], [5,44], [3,42], [6,45], [2,41], [2,42]], \
                "range": [2, 6], "ticker": "FOUR", "meta": {"ts_interval": 0} } ] AS d \
            LEFT UNNEST _timeseries(d, {"ts_ranges": [2,3], \
                "ts_interval":"meta.ts_interval", \
                "ts_start": "`range`[0]", "ts_end": "`range`[1]", \
                "ts_data":"datapoint"}) AS t'
        # Query with mixed implicit and explicit path to non default
        ts_query2 = 'SELECT d.ticker, t._t, t._v0 FROM \
            [ { "ts_data": [ [2,41], [5,44], [3,42], [6,45], [2,41], [2,42]], \
                "range": [2, 6], "ticker": "FOUR", "meta": {"ts_interval": 0} } ] AS d \
            LEFT UNNEST _timeseries(d, {"ts_ranges": [2,3], \
                "ts_interval":"meta.ts_interval", \
                "ts_start": "`range`[0]", "ts_end": "`range`[1]"}) AS t'
        result1 = self.run_cbq_query(ts_query1)
        result2 = self.run_cbq_query(ts_query2)
        expected_result = [
            {'_t': 2, '_v0': 41, 'ticker': 'FOUR'},
            {'_t': 3, '_v0': 42, 'ticker': 'FOUR'},
            {'_t': 2, '_v0': 41, 'ticker': 'FOUR'},
            {'_t': 2, '_v0': 42, 'ticker': 'FOUR'}
        ]
        self.assertEqual(result1['results'], expected_result)
        self.assertEqual(result2['results'], expected_result)
        
    def test_range_invalid(self):
        if type(self.ts_ranges) is str:
            ranges = literal_eval(self.ts_ranges)
        else:
            ranges = self.ts_ranges
        ts_query = f'SELECT sum(t._v0) as sum_v0 FROM default.stocks.price AS d UNNEST _timeseries(d, {{"ts_ranges": {ranges}}}) AS t'
        result = self.run_cbq_query(ts_query)
        self.assertEqual(result['results'], [{'sum_v0': None}])

    def test_range_multi(self):
        t1 = 1672876800000
        t2 = t1 + 86400000*2
        t3 = t2 + 86400000*3
        t4 = t3 + 86400000*3
        days = self.run_cbq_query(f'SELECT ARRAY_CONCAT(DATE_RANGE_STR(MILLIS_TO_TZ({t1}, "UTC"), MILLIS_TO_TZ({t2+1}, "UTC"), "day"), DATE_RANGE_STR(MILLIS_TO_TZ({t3}, "UTC"), MILLIS_TO_TZ({t4+1}, "UTC"), "day")) as days')
        expected_days = days['results'][0]['days']
        ts_query = f'SELECT MILLIS_TO_TZ(day*86400000,"UTC") AS day, sum(t._v0) as sum_v0 FROM default.stocks.price as d UNNEST _timeseries(d, {{"ts_ranges": [[{t1},{t2}],[{t3},{t4}]]}}) AS t GROUP BY IDIV(t._t,86400000) AS day ORDER BY day'
        result1 = self.run_cbq_query(ts_query)
        i = 0
        for day in result1['results']:
            self.assertEqual(day['day'][:10], expected_days[i][:10])
            i += 1

    def test_keep(self):
        ts_query_keep = f'SELECT DISTINCT d.*, t FROM default.stocks.price AS d USE KEYS ["IBM:2023-01-05T00:00:00Z", "IBM:2023-01-06T00:00:00Z" ] LEFT UNNEST _timeseries(d, {{"ts_keep":true, "ts_ranges":[1672876800000, 1672876800000+3600000*28]}}) AS t LIMIT 1;'
        ts_query_not_keep = f'SELECT DISTINCT d.*, t FROM default.stocks.price AS d USE KEYS ["IBM:2023-01-05T00:00:00Z", "IBM:2023-01-06T00:00:00Z" ] LEFT UNNEST _timeseries(d, {{"ts_keep":false, "ts_ranges":[1672876800000, 1672876800000+3600000*28]}}) AS t LIMIT 1;'
        ts_query_default = f'SELECT DISTINCT d.*, t FROM default.stocks.price AS d USE KEYS ["IBM:2023-01-05T00:00:00Z", "IBM:2023-01-06T00:00:00Z" ] LEFT UNNEST _timeseries(d, {{"ts_ranges":[1672876800000, 1672876800000+3600000*28]}}) AS t LIMIT 1;'
        result_keep = self.run_cbq_query(ts_query_keep)
        result_not_keep = self.run_cbq_query(ts_query_not_keep)
        result_default = self.run_cbq_query(ts_query_default)
        self.assertEqual(result_not_keep['results'][0], result_default['results'][0])
        keys_not_keep = list(result_not_keep['results'][0].keys())
        keys_keep = list(result_keep['results'][0].keys())
        keys_not_keep.sort()
        keys_keep.sort()
        self.assertEqual(keys_not_keep, ['t', 'ticker', 'ts_end', 'ts_interval', 'ts_start'])
        self.assertEqual(keys_keep, ['t', 'ticker', 'ts_data', 'ts_end', 'ts_interval', 'ts_start'])

    def test_transaction(self):
        ts_query = f'SELECT MILLIS_TO_TZ(t._t,"UTC") AS ts, sum(t._v1) as volume FROM default.stocks.price as d UNNEST _timeseries(d, {{"ts_ranges": [1672876800000,1672876800000+3600000*2]}}) AS t GROUP by t._t'
        begin = self.run_cbq_query(query="BEGIN WORK", server=self.master)
        txid = begin['results'][0]['txid']
        result = self.run_cbq_query(ts_query, txnid=txid)
        commit = self.run_cbq_query("COMMIT", txnid=txid)
        self.assertEqual(len(result['results']), 3)

    def test_udf(self):
        hours = 4
        ts_query = f'SELECT MILLIS_TO_TZ(t._t,"UTC") AS ts, sum(t._v1) as volume FROM default.stocks.price as d UNNEST _timeseries(d, {{"ts_ranges": [1672876800000,1672876800000+3600000*hr]}}) AS t GROUP by t._t'
        udf = self.run_cbq_query(f'CREATE OR REPLACE FUNCTION volume(hr) {{ ({ts_query}) }}')
        result = self.run_cbq_query(f'EXECUTE FUNCTION volume({hours})')
        self.assertEqual(len(result['results'][0]), hours+1)

    def test_misc(self):
        ts_query = 'SELECT _timeseries(d, {"ts_ranges":[1677730957000, 1677730959000]}) as ts FROM [ { "ts_data": [ [1677730957000, 16.57], [1677730958000, 16.58], [1677730959000, 16.59] ], "ts_start":1677730957000, "ts_end":1677730959000, "ticker": "BASE" } ] AS d ;'
        result = self.run_cbq_query(ts_query)
        self.assertEqual(result['results'][0]['ts'],  [{"_t": 1677730957000,"_v0": 16.57}, {"_t": 1677730958000, "_v0": 16.58}, {"_t": 1677730959000, "_v0": 16.59}])

    def test_options_invalid(self):
        ts_query = f'SELECT DISTINCT sum(t._v0) FROM default.stocks.price AS d LEFT UNNEST _timeseries(d, {{"unknown": "option", "ts_ranges":[1672876800000, 1672876800000+3600000*2]}}) AS t;'
        result = self.run_cbq_query(ts_query)
        self.assertTrue(result['results'][0]['$1'] > 0)

    def test_interval_invalid(self):
        ts_query = f'SELECT t.* FROM {{ "ts_data": [[16.50, 50], [16.51, 51]], "ts_start":10, "ts_end":20, "ts_interval": {self.ts_interval}}} AS d UNNEST _timeseries(d, {{"ts_project":0}}) AS t'
        result = self.run_cbq_query(ts_query)
        self.assertEqual(result['results'], [])

    def test_static_constants(self):
        ts_query = '''
            WITH docs AS ( [ ] ), range_start AS (1375056000000), range_end AS (1375574400000)
                SELECT MILLIS_TO_TZ(t._t,"UTC") AS day, t._v0 AS low, t._v1 AS high
                FROM docs AS d
                UNNEST _timeseries(d, {"ts_ranges": [range_start, range_end]}) AS t
        '''
        result = self.run_cbq_query(ts_query)
        self.assertEqual(result['results'], [])


