from .tuq import QueryTests
import time
from membase.api.exception import CBQError
from datetime import datetime, timedelta
import pytz

class QueryAUSTests(QueryTests):

    def setUp(self):
        super(QueryAUSTests, self).setUp()
        self.log.info("==============  QueryAUSTests setup has started ==============")
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
        
        self.log.info("==============  QueryAUSTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryAUSTests, self).suite_setUp()
        self.log.info("==============  QueryAUSTests suite_setup has started ==============")
        self.log.info("==============  QueryAUSTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryAUSTests tearDown has started ==============")
        travel_sample = self.get_bucket_from_name("travel-sample")
        if travel_sample:
            self.delete_bucket(travel_sample)
        self.log.info("==============  QueryAUSTests tearDown has completed ==============")
        super(QueryAUSTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryAUSTests suite_tearDown has started ==============")
        self.log.info("==============  QueryAUSTests suite_tearDown has completed ==============")
        super(QueryAUSTests, self).suite_tearDown()

    '''
    AUS is Automatic Update Statistics.
    It allows you to define a daily schedule to update the statistics of the cluster.
    AUS settings is defined in system:aus catalog collection.
    AUS schedule can then be found in system:tasks_cache WHERE class = "auto_update_statistics"
    '''

    def test_check_aus_default(self):
        aus_query = "SELECT * FROM system:aus"
        expected_default = [
            {
                "aus": {
                    "all_buckets": False,
                    "change_percentage": 10,
                    "enable": False
                }
            }
        ]
        aus_result = self.run_cbq_query(aus_query)
        self.log.info(f"AUS result: {aus_result}")
        self.assertEqual(aus_result['results'], expected_default)

    def test_error_time_less_than_30_minutes(self):
        expected_error = "Invalid schema or semantics detected in the Auto Update Statistics settings document. 'schedule.start_time' must be earlier than 'schedule.end_time' by at least 30 minutes."
        aus_schedule = 'UPDATE system:aus SET schedule = {"start_time": "14:40", "end_time": "14:50", "timezone": "America/Los_Angeles", "days": ["Friday","Saturday"]}'
        try:
            self.run_cbq_query(aus_schedule)
            self.fail("Expected error not raised")
        except CBQError as e:
            self.log.info(f"Expected error: {e}")
            error = self.process_CBQE(e, 0)
            self.assertEqual(error['code'], 20003)
            self.assertEqual(error['msg'], expected_error)

    def test_error_time_start_time_not_earlier_than_end_time(self):
        expected_error = "Invalid schema or semantics detected in the Auto Update Statistics settings document. 'schedule.start_time' must be earlier than 'schedule.end_time' by at least 30 minutes."
        aus_schedule = 'UPDATE system:aus SET schedule = {"start_time": "14:40", "end_time": "14:50", "timezone": "America/Los_Angeles", "days": ["Friday","Saturday"]}'
        try:
            self.run_cbq_query(aus_schedule)
            self.fail("Expected error not raised")
        except CBQError as e:
            self.log.info(f"Expected error: {e}")
            error = self.process_CBQE(e, 0)
            self.assertEqual(error['code'], 20003)
            self.assertEqual(error['msg'], expected_error)

    def test_error_invalid_timezone(self):
        expected_error = "Invalid schema or semantics detected in the Auto Update Statistics settings document. Invalid value 'American/Los_Angeles' (string) for setting 'schedule.timezone'."
        aus_schedule = 'UPDATE system:aus SET schedule = {"start_time": "14:40", "end_time": "15:10", "timezone": "American/Los_Angeles", "days": ["Friday","Saturday"]}'
        try:
            self.run_cbq_query(aus_schedule)
            self.fail("Expected error not raised")
        except CBQError as e:
            self.log.info(f"Expected error: {e}")
            error = self.process_CBQE(e, 0)
            self.assertEqual(error['code'], 20003)
            self.assertEqual(error['msg'], expected_error)

    def test_error_invalid_days(self):
        expected_error = "Invalid schema or semantics detected in the Auto Update Statistics settings document. Invalid value '[\"Lundi\" \"Mardi\"]' ([]interface {}) for setting 'schedule.days'."
        aus_schedule = 'UPDATE system:aus SET schedule = {"start_time": "14:40", "end_time": "15:10", "timezone": "America/Los_Angeles", "days": ["Lundi","Mardi"]}'
        try:
            self.run_cbq_query(aus_schedule)
            self.fail("Expected error not raised")
        except CBQError as e:
            self.log.info(f"Expected error: {e}")
            error = self.process_CBQE(e, 0)
            self.assertEqual(error['code'], 20003)
            self.assertEqual(error['msg'], expected_error)

    def test_error_empty_days(self):
        expected_error = "Invalid schema or semantics detected in the Auto Update Statistics settings document. Invalid value '[]' ([]interface {}) for setting 'schedule.days'."
        aus_schedule = 'UPDATE system:aus SET schedule = {"start_time": "14:40", "end_time": "15:10", "timezone": "America/Los_Angeles", "days": []}'
        try:
            self.run_cbq_query(aus_schedule)
            self.fail("Expected error not raised")
        except CBQError as e:
            self.log.info(f"Expected error: {e}")
            error = self.process_CBQE(e, 0)
            self.assertEqual(error['code'], 20003)
            self.assertEqual(error['msg'], expected_error)

    def test_error_invalid_change_percentage(self):
        invalid_values = [110, -10, '100']
        expected_error_reason = "Setting 'change_percentage' must be: Integer between 0 and 100."
        for value in invalid_values:
            type = "int64" if isinstance(value, int) else "string"
            expected_error = f"Invalid schema or semantics detected in the Auto Update Statistics settings document. Invalid value '{value}' ({type}) for setting 'change_percentage'."
            value = f'"{value}"' if isinstance(value, str) else value
            aus_schedule = f'UPDATE system:aus SET change_percentage = {value}'
            try:
                self.run_cbq_query(aus_schedule)
                self.fail("Expected error not raised")
            except CBQError as e:
                self.log.info(f"Expected error: {e}")
                error = self.process_CBQE(e, 0)
                self.assertEqual(error['code'], 20003)
                self.assertEqual(error['msg'], expected_error)
                self.assertEqual(error['reason']['cause'], expected_error_reason)

    def test_error_invalid_time(self):
        invalid_times = ["09:40am", "09:10pm", "09:10:37"]
        expected_error_reason = "Setting 'schedule.start_time' must be: Valid timestamp in HH:MM format."
        for time in invalid_times:
            expected_error = f"Invalid schema or semantics detected in the Auto Update Statistics settings document. Invalid value '\"{time}\"' (value.stringValue) for setting 'schedule.start_time'."
            aus_schedule = f'UPDATE system:aus SET schedule = {{"start_time": "{time}", "end_time": "15:10", "timezone": "America/Los_Angeles", "days": ["Friday","Saturday"]}}'
            try:
                self.run_cbq_query(aus_schedule)
                self.fail("Expected error not raised")
            except CBQError as e:
                self.log.info(f"Expected error: {e}")
                error = self.process_CBQE(e, 0)
                self.assertEqual(error['code'], 20003)
                self.assertEqual(error['msg'], expected_error)
                self.assertEqual(error['reason']['cause'], expected_error_reason)

    def test_error_invalid_all_buckets(self):
        invalid_values = [0, 1, 'yes', 'no', 'true', 'false']
        expected_error_reason = "Setting 'all_buckets' must be: boolean."
        for value in invalid_values:
            type = "string" if isinstance(value, str) else "int64"
            expected_error = f"Invalid schema or semantics detected in the Auto Update Statistics settings document. Invalid value '{value}' ({type}) for setting 'all_buckets'."
            value = f'"{value}"' if isinstance(value, str) else value
            aus_schedule = f'UPDATE system:aus SET all_buckets = {value}'
            try:
                self.run_cbq_query(aus_schedule)
                self.fail("Expected error not raised")
            except CBQError as e:
                self.log.info(f"Expected error: {e}")
                error = self.process_CBQE(e, 0)
                self.assertEqual(error['code'], 20003)
                self.assertEqual(error['msg'], expected_error)
                self.assertEqual(error['reason']['cause'], expected_error_reason)

    def test_set_aus_schedule(self):
        # current time HH:MM in America/Los_Angeles + 2 minutes
        start_time = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(minutes=2)).strftime('%H:%M')
        end_time = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(minutes=32)).strftime('%H:%M')  

        aus_update = f'UPDATE system:aus SET schedule = {{"start_time": "{start_time}", "end_time": "{end_time}", "timezone": "America/Los_Angeles", "days": ["Friday","Saturday"]}}'
        self.run_cbq_query(aus_update)

        aus_query = "SELECT * FROM system:aus"
        aus_result = self.run_cbq_query(aus_query)
        self.log.info(f"AUS result: {aus_result}")
        aus_settings = aus_result['results'][0]['aus']  

        self.assertEqual(aus_settings['schedule']['start_time'], start_time)
        self.assertEqual(aus_settings['schedule']['end_time'], end_time)
        self.assertEqual(aus_settings['schedule']['timezone'], "America/Los_Angeles")
        self.assertEqual(aus_settings['schedule']['days'], ["Friday","Saturday"])
        self.assertEqual(aus_settings['enable'], False)
        self.assertEqual(aus_settings['all_buckets'], False)
        self.assertEqual(aus_settings['change_percentage'], 10)
