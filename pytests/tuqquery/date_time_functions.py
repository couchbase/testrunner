import logging
import random
from dateutil import parser

from .tuq import QueryTests

log = logging.getLogger(__name__)

FORMATS = ["2006-01-02T15:04:05.999+07:00",
           "2006-01-02T15:04:05.999",
           "2006-01-02T15:04:05",
           "2006-01-02 15:04:05.999+07:00",
           "2006-01-02 15:04:05.999",
           "2006-01-02 15:04:05",
           "2006-01-02",
           "15:04:05.999+07:00",
           "15:04:05.999",
           "15:04:05"]

PARTS = ["millennium",
         "century",
         "decade",
         "year",
         "quarter",
         "month",
         "day",
         "hour",
         "minute",
         "second",
         "millisecond",
         "week",
         "day_of_year", "doy",
         "day_of_week", "dow",
         "iso_week",
         "iso_year",
         "iso_dow"]

TIMEZONES = ["UTC"]


class DateTimeFunctionClass(QueryTests):
    def setUp(self):
        super(DateTimeFunctionClass, self).setUp()

    def tearDown(self):
        super(DateTimeFunctionClass, self).tearDown()

    def test_date_part_millis(self):
        for count in range(5):
            if count == 0:
                milliseconds = 0
            else:
                milliseconds = random.randint(658979899785, 876578987695)
            for part in PARTS:
                expected_utc_query = 'SELECT DATE_PART_STR(MILLIS_TO_UTC({0}), "{1}")'.format(milliseconds, part)
                expected_utc_result = self.run_cbq_query(expected_utc_query)
                actual_utc_query = self._generate_date_part_millis_query(milliseconds, part, "UTC")
                actual_utc_result = self.run_cbq_query(actual_utc_query)
                self.log.info(actual_utc_query)
                self.assertEqual(actual_utc_result["results"][0]["$1"], expected_utc_result["results"][0]["$1"],
                                 "Actual result {0} and expected result {1} don't match for {2} milliseconds and \
                                 {3} parts".format(actual_utc_result["results"][0], expected_utc_result["results"][0]["$1"],
                                                   milliseconds, part))
                expected_local_query = 'SELECT DATE_PART_STR(MILLIS_TO_STR({0}), "{1}")'.format(milliseconds, part)
                expected_local_result = self.run_cbq_query(expected_local_query)
                actual_local_query = self._generate_date_part_millis_query(milliseconds, part)
                self.log.info(actual_local_query)
                actual_local_result = self.run_cbq_query(actual_local_query)
                self.assertEqual(actual_local_result["results"][0]["$1"], expected_local_result["results"][0]["$1"],
                                 "Actual result {0} and expected result {1} don't match for {2} milliseconds and \
                                 {3} parts".format(actual_local_result["results"][0], expected_local_result["results"][0]["$1"],
                                                   milliseconds, part))

    def test_date_part_millis_for_negative_inputs(self):
        expressions = ['\"123abc\"', 675786.869876, -658979899786, '\"\"', "null", {"a": 1, "b": 2}, {}]
        for expression in expressions:
            for part in PARTS:
                query = 'SELECT DATE PART_MILLIS({0}, "{1}")'.format(expression, part)
                try:
                    actual_result = self.run_cbq_query(query)
                except Exception as ex:
                    msg = "syntax error"
                    if msg not in str(ex):
                        raise

    def test_date_format_str(self):
        local_formats = ["2006-01-02T15:04:05+07:00",
           "2006-01-02T15:04:05",
           "2006-01-02 15:04:05+07:00",
           "2006-01-02 15:04:05",
           "2006-01-02 15:04:05",
           "2006-01-02",
           "15:04:05+07:00",
           "15:04:05"]
        for expression in local_formats:
            for expected_format in local_formats:
                if expression != expected_format:
                    date_format_query = 'DATE_FORMAT_STR("{0}", "{1}")'.format(expression, expected_format)
                    query = 'SELECT LENGTH(' + date_format_query + ')'
                    actual_result = self.run_cbq_query(query)
                    query = 'SELECT LENGTH("{0}")'.format(expected_format)
                    expected_result = self.run_cbq_query(query)
                    if actual_result["results"][0]["$1"] != expected_result["results"][0]["$1"]:
                        query = 'SELECT ' + date_format_query
                        str_actual_result = self.run_cbq_query(query)
                        query = 'SELECT "{0}"'.format(expected_format)
                        str_expected_result = self.run_cbq_query(query)
                        self.log.info("Test is failed. Actual result is {0}, expected result is {1}.".format(str_actual_result, str_expected_result))
                    self.assertEqual(actual_result["results"][0]["$1"], expected_result["results"][0]["$1"],
                                     "Results mismatch for query {0}".format(date_format_query))

    def test_date_range_str(self):
        error_query = []
        local_parts = ["millennium",
                       "century",
                       "decade",
                       "year",
                       "quarter",
                       "month",
                       "week",
                       "day",
                       "hour",
                       "minute",
                       "second",
                       "millisecond"]
        count = 3
        for first_expression in FORMATS:
            expect_null_result = 0
            for part in local_parts:
                query = 'SELECT DATE_ADD_STR("{0}", {1}, "{2}")'.format(first_expression, count, part)
                expected_result = self.run_cbq_query(query)
                temp_expression = expected_result["results"][0]["$1"]
                self.assertIsNotNone(temp_expression, "result is {0} for query {1}".format(expected_result, query))
                query = self._generate_date_format_str_query(temp_expression, first_expression)
                result = self.run_cbq_query(query)
                second_expression = result["results"][0]["$1"]
                if (first_expression == "2006-01-02T15:04:05" or first_expression == "2006-01-02 15:04:05") and part == "millisecond":
                    expect_null_result = 1
                if part in local_parts[:8]:
                    if not (self._is_date_part_present(first_expression) and
                                self._is_date_part_present(second_expression)):
                        expect_null_result = 1
                else:
                    if not (self._is_time_part_present(first_expression) and
                                self._is_time_part_present(second_expression)):
                        expect_null_result = 1
                query = self._generate_date_range_str_query(first_expression, second_expression, part)
                log.info(query)
                try:
                    actual_result = self.run_cbq_query(query)
                except Exception:
                    error_query.append(query)
                else:
                    lst = actual_result["results"][0]["$1"]
                    if not expect_null_result and not lst:
                        error_query.append(query)
                    elif lst:
                        if len(lst) != count:
                            error_query.append(query)
        self.assertFalse(error_query, "Queries Failed are: {0}".format(error_query))

    def test_date_range_millis(self):
        error_query = []
        local_parts = ["millennium",
                       "century",
                       "decade",
                       "year",
                       "quarter",
                       "month",
                       "week",
                       "day",
                       "hour",
                       "minute",
                       "second",
                       "millisecond"]
        count = 3
        for i in range(5):
            expect_null_result = 0
            for part in local_parts:
                first_millis = random.randint(658979899785, 876578987695)
                expected_utc_query = 'SELECT MILLIS_TO_STR({0})'.format(
                    first_millis)
                expected_utc_result = self.run_cbq_query(expected_utc_query)
                first_expression = expected_utc_result["results"][0]["$1"]
                query = 'SELECT DATE_ADD_STR("{0}", {1}, "{2}")'.format(
                    first_expression, count, part)
                expected_result = self.run_cbq_query(query)
                temp_expression = expected_result["results"][0]["$1"]
                self.assertIsNotNone(temp_expression,
                                     "result is {0} for query {1}".format(
                                         expected_result, query))
                query = self._generate_date_format_str_query(
                    temp_expression, first_expression)
                result = self.run_cbq_query(query)
                second_expression = result["results"][0]["$1"]
                if part in local_parts[:8]:
                    if not (self._is_date_part_present(first_expression) and
                                self._is_date_part_present(second_expression)):
                        expect_null_result = 1
                else:
                    if not (self._is_time_part_present(first_expression) and
                                self._is_time_part_present(second_expression)):
                        expect_null_result = 1
                second_millis = self._convert_to_millis(second_expression)
                query = self._generate_date_range_millis_query(first_millis,
                                                               second_millis, part)
                log.info(query)
                try:
                    actual_result = self.run_cbq_query(query)
                except Exception:
                    error_query.append(query)
                else:
                    lst = actual_result["results"][0]["$1"]
                    if not expect_null_result and not lst:
                        error_query.append(query)
                    elif lst:
                        if len(lst) != count:
                            error_query.append(query)
        self.assertFalse(error_query, "Queries Failed are: {0}".format(
            error_query))

    def test_date_range_str_for_intervals(self):
        #Set Interval
        intervals = [0, 2, 10, -1]
        start_date = "2006-01-02T15:04:05"
        end_date = "2006-01-10T15:04:05"
        for interval in intervals:
            query = self._generate_date_range_str_query(
                start_date, end_date, "day", interval)
            self.log.info(query)
            actual_result = self.run_cbq_query(query)
            lst = actual_result["results"][0]["$1"]
            if interval < 1:
                self.assertEqual(len(lst), 0,
                                 "Query {0} Failed".format(query))
            else:
                if not (8%interval):
                    self.assertEqual(len(lst), (8//interval),
                                     "Query {0} Failed".format(query))
                else:
                    self.assertEqual(len(lst), (8//interval)+1,
                                     "Query {0} Failed".format(query))

    def test_date_range_millis_for_intervals(self):
        #Set Interval
        intervals = [0, 2, 10, -1]
        start_date = "2006-01-02T15:04:05"
        end_date = "2006-01-10T15:04:05"
        start_millis = self._convert_to_millis(start_date)
        end_millis = self._convert_to_millis(end_date)
        for interval in intervals:
            query = self._generate_date_range_millis_query(
                start_millis, end_millis, "day", interval)
            self.log.info(query)
            actual_result = self.run_cbq_query(query)
            lst = actual_result["results"][0]["$1"]
            if interval < 1:
                self.assertEqual(len(lst), 0, "Query {0} Failed".format(query))
            else:
                if not (8%interval):
                    self.assertEqual(len(lst), (8//interval),
                                     "Query {0} Failed".format(query))
                else:
                    self.assertEqual(len(lst), (8//interval)+1,
                                     "Query {0} Failed".format(query))

    def test_new_functions(self):
        local_formats = ["2006-01-02T00:00:00"]
        for expression in local_formats:
            query = 'SELECT STR_TO_UTC(CLOCK_STR("{0}"))'.format(expression)
            expected_result = self.run_cbq_query(query)
            self.log.info("Expected Result : %s", expected_result)

            query = 'SELECT CLOCK_UTC("{0}")'.format(expression)
            actual_result = self.run_cbq_query(query)
            self.log.info("Actual Result : %s", actual_result)

            str_expected = expected_result["results"][0]["$1"]
            str_expected = str_expected[:str_expected.find("T")]
            str_actual = actual_result["results"][0]["$1"]
            str_actual = str_actual[:str_actual.find("T")]

            self.assertEqual(str_actual, str_expected, "{0} failed ".format(query))


            query = 'SELECT STR_TO_UTC(NOW_STR("{0}"))'.format(expression)
            expected_result = self.run_cbq_query(query)
            self.log.info("Expected Result : %s", expected_result)

            query = 'SELECT NOW_UTC("{0}")'.format(expression)
            actual_result = self.run_cbq_query(query)
            self.log.info("Actual Result : %s", actual_result)

            str_expected = expected_result["results"][0]["$1"]
            str_expected = str_expected[:str_expected.find("T")]
            str_actual = actual_result["results"][0]["$1"]
            str_actual = str_actual[:str_actual.find("T")]

            self.assertEqual(str_actual, str_expected, "{0} failed ".format(query))


            query = 'SELECT STR_TO_ZONE_NAME(CLOCK_STR("{0}"), "UTC")'.format(expression)
            expected_result = self.run_cbq_query(query)
            self.log.info("Expected Result : %s", expected_result)

            query = 'SELECT CLOCK_TZ("UTC", "{0}")'.format(expression)
            actual_result = self.run_cbq_query(query)
            self.log.info("Actual Result : %s", actual_result)

            str_expected = expected_result["results"][0]["$1"]
            str_expected = str_expected[:str_expected.find("T")]
            str_actual = actual_result["results"][0]["$1"]
            str_actual = str_actual[:str_actual.find("T")]

            self.assertEqual(str_actual, str_expected, "{0} failed ".format(query))


            query = 'SELECT STR_TO_ZONE_NAME(NOW_STR("{0}"), "UTC")'.format(expression)
            expected_result = self.run_cbq_query(query)
            self.log.info("Expected Result : %s", expected_result)

            query = 'SELECT NOW_TZ("UTC", "{0}")'.format(expression)
            actual_result = self.run_cbq_query(query)
            self.log.info("Actual Result : %s", actual_result)

            str_expected = expected_result["results"][0]["$1"]
            str_expected = str_expected[:str_expected.find("T")]
            str_actual = actual_result["results"][0]["$1"]
            str_actual = str_actual[:str_actual.find("T")]

            self.assertEqual(str_actual, str_expected, "{0} failed ".format(query))

    def test_date_add_calendar_month(self):
        result = self.run_cbq_query("SELECT DATE_ADD_STR('2020-01-31 00:00:00Z', 1, 'calendar_month')")
        self.assertEqual(result['results'][0]['$1'], "2020-02-29 00:00:00Z")

        result = self.run_cbq_query("SELECT DATE_ADD_STR('2020-02-29 00:00:00Z', -1, 'calendar_month')")
        self.assertEqual(result['results'][0]['$1'], "2020-01-31 00:00:00Z")

        result = self.run_cbq_query("SELECT DATE_ADD_STR('2020-01-31 00:00:00Z', 13, 'calendar_month')")
        self.assertEqual(result['results'][0]['$1'], "2021-02-28 00:00:00Z")

    def test_date_range_calendar_month(self):
        result = self.run_cbq_query("select DATE_RANGE_STR('2020-01-31','2021-07-01','calendar_month', 3)")
        self.assertEqual(result['results'][0]['$1'], ["2020-01-31", "2020-04-30", "2020-07-31", "2020-10-31", "2021-01-31", "2021-04-30"])

        result = self.run_cbq_query("select DATE_RANGE_STR('2021-07-01', '2020-01-31', 'calendar_month',-3)")
        self.assertEqual(result['results'][0]['$1'], ["2021-07-01", "2021-04-01", "2021-01-01", "2020-10-01", "2020-07-01", "2020-04-01"])

    def test_str_to_millis(self):
        dates = ["1985-03-26T11:22:53-06", "1985-03-26T11:22:53-0600", "1985-03-26T11:22:53-06:00"]
        for date in dates:
            with self.subTest(f'Date: {date}'):
                result = self.run_cbq_query(f'select STR_TO_MILLIS("{date}")')
                self.assertEqual(result['results'][0]['$1'], 480705773000)

    def test_date_trunc_str(self):
        date = "2016-05-18T03:59:59.123Z"
        expected = {
            'millennium': '2000-01-01T00:00:00Z',
            'century': '2000-01-01T00:00:00Z',
            'decade': '2010-01-01T00:00:00Z',
            'year': '2016-01-01T00:00:00Z',
            'quarter': '2016-04-01T00:00:00Z',
            'month': '2016-05-01T00:00:00Z',
            'calendar_month': '2016-05-01T00:00:00Z',
            'week': '2016-05-15T00:00:00Z',
            'iso_week': '2016-05-16T00:00:00Z',
            'day': '2016-05-18T00:00:00Z',
            'hour': '2016-05-18T03:00:00Z',
            'minute': '2016-05-18T03:59:00Z',
            'second': '2016-05-18T03:59:59Z',
            'millisecond': '2016-05-18T03:59:59.123Z'
        }
        for date_part in expected:
            with self.subTest(f'Date part: {date_part}'):
                result = self.run_cbq_query(f"SELECT DATE_TRUNC_STR('{date}', '{date_part}')")
                self.assertEqual(result['results'][0]['$1'], expected[date_part], f"Failed to truncate to {date_part}. We got {result['results'][0]['$1']} instead of {expected[date_part]}")

    def test_str_utc_minus(self):
        date = '2024-09-01T08:32:56.123'
        result = self.run_cbq_query(f"SELECT STR_TO_UTC('{date}-02:00') d1, STR_TO_UTC('{date}-01:00') d2, STR_TO_UTC('{date}+00:00') d3, STR_TO_UTC('{date}+01:00') d4")
        dates = result['results'][0]
        date1 = parser.parse(dates["d1"])
        date2 = parser.parse(dates["d2"])
        date3 = parser.parse(dates["d3"])
        date4 = parser.parse(dates["d4"])
        self.log.info(f"date1: {date1}, date2: {date2} and date3: {date3}")
        td1 = date1 - date2
        td2 = date2 - date3
        td3 = date3 - date4
        # check difference between dates is 1 hour
        self.assertEqual(td1.total_seconds()/3600, 1.0)
        self.assertEqual(td2.total_seconds()/3600, 1.0)
        self.assertEqual(td3.total_seconds()/3600, 1.0)