import logging
import random
import time

from datetime import datetime
from dateutil.parser import parse
from tuq import QueryTests

log = logging.getLogger(__name__)

FORMATS = ["2006-01-02T15:04:05.999Z07:00",
           "2006-01-02T15:04:05Z07:00",
           "2006-01-02T15:04:05.999",
           "2006-01-02T15:04:05",
           "2006-01-02 15:04:05.999Z07:00",
           "2006-01-02 15:04:05Z07:00",
           "2006-01-02 15:04:05.999",
           "2006-01-02 15:04:05",
           "2006-01-02",
           "15:04:05.999Z07:00",
           "15:04:05Z07:00",
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
        pass

    def _generate_array_date_range_query(self, initial_date, final_date, part, increment=None):
        pass