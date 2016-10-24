import logging
import random
import time

from datetime import datetime
from dateutil.parser import parse
from threading import Thread
from tuq import QueryTests

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
         "iso_dow",
         "timezone"]

TIMEZONES = ["utc"]


class DateTimeFunctionClass(QueryTests):
    def setUp(self):
        super(DateTimeFunctionClass, self).setUp()

    def tearDown(self):
        super(DateTimeFunctionClass, self).tearDown()

    def test_date_part_millis(self):
        for count in range(5):
            milliseconds = random.randint(658979899786, 876578987695)
            time_tuple = time.gmtime(milliseconds/1000)
            local_parts = self._generate_expected_results_for_date_part_millis(time_tuple)
            for part in local_parts:
                    query = self._generate_date_part_millis_query(milliseconds, part)
                    actual_result = self.run_cbq_query(query)
                    self.assertEqual(actual_result["results"][0]["$1"], local_parts[part],
                                     "Actual result {0} and expected result {1} don't match for {2} milliseconds and \
                                     {3} parts".format(actual_result["results"][0], local_parts[part],
                                                       milliseconds, part))

    def test_date_part_millis_for_zero(self):
        #Special Case when expression is 0
        time_tuple = datetime(1970, 1, 1, 0, 0, 0).timetuple()
        local_parts = self._generate_expected_results_for_date_part_millis(time_tuple)
        for part in local_parts:
            query = self._generate_date_part_millis_query(0, part)
            actual_result = self.run_cbq_query(query)
            self.assertEqual(actual_result["results"][0]["$1"], local_parts[part],
                             "Actual result {0} and expected result {1} don't match for 0 milliseconds and {2} parts".
                             format(actual_result["results"][0], local_parts[part], part))

    def test_date_part_millis_for_negative_inputs(self):
        expressions = ['\"123abc\"', 675786.869876, -658979899786, '\"\"', "null", {"a": 1, "b": 2}, {}]
        for expression in expressions:
            for part in PARTS:
                query = 'SELECT DATE PART_MILLIS({0}, "{1}")'.format(expression, part)
                try:
                    actual_result = self.run_cbq_query(query)
                except Exception, ex:
                    msg = "syntax error"
                    if msg not in str(ex):
                        raise

    def test_date_format_str(self):
        local_formats = ["2006-01-02T15:04:05.999",
           "2006-01-02T15:04:05",
           "2006-01-02 15:04:05.999",
           "2006-01-02 15:04:05",
           "2006-01-02"]
        for expression in local_formats:
            for expected_format in FORMATS:
                query = self._generate_date_format_str_query(expression, expected_format)
                actual_result = self.run_cbq_query(query)
                query = 'SELECT MILLIS_TO_UTC(MILLIS("{0}"), "{1}")'.format(expression, expected_format)
                expected_result = self.run_cbq_query(query)
                self.assertEqual(actual_result["results"][0]["$1"], expected_result["results"][0]["$1"],
                         "Resulting format {0} doesn't match with expected {1}".format(
                             actual_result["results"][0]["$1"], expected_result["results"][0]["$1"]))

    def test_array_date_range(self):
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
                if part in local_parts[:8]:
                    if not (self._is_date_part_present(first_expression) and
                                self._is_date_part_present(second_expression)):
                        expect_null_result = 1
                else:
                    if not (self._is_time_part_present(first_expression) and
                                self._is_time_part_present(second_expression)):
                        expect_null_result = 1
                query = self._generate_array_date_range_query(first_expression, second_expression, part)
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
                        if len(lst) != count+1:
                            error_query.append(query)
        self.assertFalse(error_query, "Queries Failed are: {0}".format(error_query))

    def test_array_date_range_for_intervals(self):
        #Set Interval
        intervals = [0, 2, 10, -1]
        start_date = "2006-01-02T15:04:05"
        end_date = "2006-01-10T15:04:05"
        for interval in intervals:
            query = self._generate_array_date_range_query(start_date, end_date, "day", interval)
            actual_result = self.run_cbq_query(query)
            if interval < 0:
                self.assertIsNone(actual_result["results"][0]["$1"],
                                  "{0}  Failed. Result {1}".format(query, actual_result))
            lst = actual_result["results"][0]["$1"]
            if interval == 0:
                self.asserEqual(len(lst), 0, "Query {1} Failed".format(query))
            self.asserEqual(len(lst), 8/interval, "Query {1} Failed".format(query))

    def test_new_functions(self):
        local_formats = ["2006-01-02"]
        for expression in local_formats:
            query = 'SELECT STR_TO_UTC(CLOCK_STR("{0}"))'.format(expression)
            expected_result = self.run_cbq_query(query)
            query = 'SELECT CLOCK_UTC("{0}")'.format(expression)
            actual_result = self.run_cbq_query(query)
            self.assertEqual(actual_result["results"][0]["$1"], expected_result["results"][0]["$1"],
                             "{0} failed ".format(query))
            query = 'SELECT STR_TO_UTC(NOW_STR("{0}"))'.format(expression)
            expected_result = self.run_cbq_query(query)
            query = 'SELECT NOW_UTC("{0}")'.format(expression)
            actual_result = self.run_cbq_query(query)
            self.assertEqual(actual_result["results"][0]["$1"], expected_result["results"][0]["$1"],
                             "{0} failed ".format(query))

    def _generate_date_part_millis_query(self, expression, part, timezone=None):
        if not timezone:
            query = 'SELECT DATE_PART_MILLIS({0}, "{1}")'.format(expression, part)
        else:
            query = 'SELECT DATE_PART_MILLIS({0}, "{1}", "{2}")'.format(expression, part, timezone)
        return query

    def _generate_expected_results_for_date_part_millis(self, time_tuple):
        local_parts = {"millennium": (time_tuple.tm_year-1)//1000 + 1,
                       "century": (time_tuple.tm_year-1)//100 + 1,
                       "decade": time_tuple.tm_year/10,
                       "year": time_tuple.tm_year,
                       "quarter": (time_tuple.tm_mon-1)//3 + 1,
                       "month": time_tuple.tm_mon,
                       "day": time_tuple.tm_mday,
                       "hour": time_tuple.tm_hour,
                       "minute": time_tuple.tm_min,
                       "second": time_tuple.tm_sec,
                       "week": (time_tuple.tm_yday-1)//7 + 1,
                       "day_of_year": time_tuple.tm_yday,
                       "doy": time_tuple.tm_yday,
                       "day_of_week": (time_tuple.tm_wday + 1)%7,
                       "dow": (time_tuple.tm_wday + 1)%7}
        return local_parts

    def _generate_date_format_str_query(self, expression, format):
        query = 'SELECT DATE_FORMAT_STR("{0}", "{1}")'.format(expression, format)
        return query

    def _generate_array_date_range_query(self, initial_date, final_date, part, increment=None):
        if increment is None:
            query = 'SELECT ARRAY_DATE_RANGE("{0}", "{1}", "{2}")'.format(initial_date, final_date, part)
        else:
            query = 'SELECT ARRAY_DATE_RANGE("{0}", "{1}", "{2}", {3})'.format(initial_date, final_date, part, increment)
        return query

    def _is_date_part_present(self, expression):
        return (len(expression.split("-")) > 1)

    def _is_time_part_present(self, expression):
        return (len(expression.split(":")) > 1)