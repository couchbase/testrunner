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

        system_stats = self.rest.fetch_system_stats()
        self.num_nodes = len(system_stats['nodes'])
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

    def test_check_scheduled_task(self):
        delay = 30
        # Enable AUS and set schedule within next 5 minutes for today
        start_time = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(minutes=delay)).strftime('%H:%M')
        end_time = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(minutes=delay + 30)).strftime('%H:%M')
        today = datetime.now(pytz.timezone('America/Los_Angeles')).strftime('%A')

        aus_update = f'UPDATE system:aus SET schedule = {{"start_time": "{start_time}", "end_time": "{end_time}", "timezone": "America/Los_Angeles", "days": ["{today}"]}}, enable = true'
        self.run_cbq_query(aus_update)

        # wait 5 seconds for task to be scheduled
        self.sleep(5)

        aus_task_query = "SELECT * FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'scheduled'"
        aus_task_result = self.run_cbq_query(aus_task_query)
        self.log.info(f"AUS task result: {aus_task_result}")
        for node in range(self.num_nodes):
            self.assertEqual(aus_task_result['results'][node]['tasks_cache']['class'], "auto_update_statistics")
            self.assertEqual(aus_task_result['results'][node]['tasks_cache']['state'], "scheduled")
            task_delay = aus_task_result['results'][node]['tasks_cache']['delay']
            task_delay_minutes = int(task_delay.split('m')[0])
            task_delay_seconds = int(task_delay.split('m')[1].split('.')[0])
            task_delay_in_seconds = task_delay_minutes * 60 + task_delay_seconds
            self.assertAlmostEqual(task_delay_in_seconds, delay * 60, delta=60)

    def test_check_completed_task(self):
        # Enable AUS and set schedule within next 1 minutes for today
        start_time = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(minutes=1)).strftime('%H:%M')
        end_time = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(minutes=31)).strftime('%H:%M')
        today = datetime.now(pytz.timezone('America/Los_Angeles')).strftime('%A')
        tomorrow = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(days=1)).strftime('%A')
        after_tomorrow = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(days=2)).strftime('%A')

        aus_update = f'UPDATE system:aus SET schedule = {{"start_time": "{start_time}", "end_time": "{end_time}", "timezone": "America/Los_Angeles", "days": ["{today}", "{tomorrow}", "{after_tomorrow}"]}}, enable = true'
        self.run_cbq_query(aus_update)

        # wait 2 minutes for task to be completed
        self.sleep(120)

        # Check completed task
        aus_task_query = "SELECT * FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'completed'"
        aus_task_keyspaces = "SELECT array_flatten(array_agg(results.keyspaces_evaluated), 1) keyspaces_evaluated, array_flatten(array_agg(results.keyspaces_updated), 1) keyspaces_updated FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'completed'"
        aus_task_keyspaces_result = self.run_cbq_query(aus_task_keyspaces)
        self.log.info(f"AUS task completed result: {aus_task_keyspaces_result}")
        expected_keyspaces_evaluated = ['default:travel-sample.inventory.hotel', 'default:travel-sample.inventory.airport', 'default:travel-sample.inventory.airline', 'default:travel-sample.inventory.route', 'default:travel-sample.inventory.landmark', 'default:travel-sample._system._query', 'default:travel-sample._system._mobile', 'default:travel-sample.tenant_agent_04.users', 'default:travel-sample.tenant_agent_04.bookings', 'default:travel-sample.tenant_agent_03.bookings', 'default:travel-sample.tenant_agent_03.users', 'default:travel-sample.tenant_agent_02.bookings', 'default:travel-sample.tenant_agent_02.users', 'default:travel-sample.tenant_agent_01.users', 'default:travel-sample.tenant_agent_01.bookings', 'default:travel-sample._default._default', 'default:travel-sample.tenant_agent_00.bookings', 'default:travel-sample.tenant_agent_00.users', 'default:default._system._mobile', 'default:default._system._query', 'default:default._default._default']
        expected_keyspaces_updated = ['default:travel-sample.inventory.hotel', 'default:travel-sample.inventory.airport', 'default:travel-sample.inventory.route', 'default:travel-sample.inventory.landmark', 'default:travel-sample._default._default']
        keyspaces_evaluated = aus_task_keyspaces_result['results'][0]['keyspaces_evaluated']
        keyspaces_updated = aus_task_keyspaces_result['results'][0]['keyspaces_updated']
        # order expected list and results list
        expected_keyspaces_evaluated.sort()
        expected_keyspaces_updated.sort()
        keyspaces_evaluated.sort()
        keyspaces_updated.sort()
        self.assertEqual(keyspaces_evaluated, expected_keyspaces_evaluated)
        self.assertEqual(keyspaces_updated, expected_keyspaces_updated)

        # Check next scheduled task
        aus_task_query = "SELECT * FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'scheduled'"
        aus_task_result = self.run_cbq_query(aus_task_query)
        self.log.info(f"AUS task scheduled result: {aus_task_result}")
        # delay should be approximately 24h59m51.71499357s from now (1 day from now)
        expected_delay = 23 * 3600 + 59 * 60 + 51
        for node in range(self.num_nodes):
            # convert task result delay from format "24h59m51.71499357s" to seconds
            task_delay = aus_task_result['results'][node]['tasks_cache']['delay']
            task_delay_hours = int(task_delay.split('h')[0])
            task_delay_minutes = int(task_delay.split('h')[1].split('m')[0])
            task_delay_seconds = int(task_delay.split('m')[1].split('.')[0])
            task_delay_in_seconds = task_delay_hours * 3600 + task_delay_minutes * 60 + task_delay_seconds
        self.assertAlmostEqual(task_delay_in_seconds, expected_delay, delta=120)

    def test_cancel_first_scheduled_task(self):
        # Enable AUS and set schedule within next 1 minutes for today for next 3 days
        delay = 10
        start_time = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(minutes=delay)).strftime('%H:%M')
        end_time = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(minutes=delay + 30)).strftime('%H:%M')
        today = datetime.now(pytz.timezone('America/Los_Angeles')).strftime('%A')
        tomorrow = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(days=1)).strftime('%A')
        after_tomorrow = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(days=2)).strftime('%A')

        aus_update = f'UPDATE system:aus SET schedule = {{"start_time": "{start_time}", "end_time": "{end_time}", "timezone": "America/Los_Angeles", "days": ["{today}", "{tomorrow}", "{after_tomorrow}"]}}, enable = true'
        self.run_cbq_query(aus_update)

        # wait 5 seconds for task to be scheduled
        self.sleep(5)

        # Cancel first scheduled task
        aus_task_scheduled = "SELECT * FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'scheduled'"
        aus_task_scheduled_result = self.run_cbq_query(aus_task_scheduled)
        self.log.info(f"AUS task scheduled result: {aus_task_scheduled_result}")
        for node in range(self.num_nodes):
            task_id = aus_task_scheduled_result['results'][node]['tasks_cache']['id']
            task_name = aus_task_scheduled_result['results'][node]['tasks_cache']['name']
            aus_cancel = f'DELETE FROM system:tasks_cache WHERE id = "{task_id}"'
            self.run_cbq_query(aus_cancel)

            # wait 3 seconds for task to be cancelled
            self.sleep(3)

            # Check cancelled task
            aus_task_cancelled = f"SELECT * FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'cancelled' AND name = '{task_name}'"
            aus_task_cancelled_result = self.run_cbq_query(aus_task_cancelled)
            self.log.info(f"AUS task cancelled result: {aus_task_cancelled_result}")
            self.assertEqual(aus_task_cancelled_result['results'][0]['tasks_cache']['id'], task_id)
            self.assertEqual(aus_task_cancelled_result['results'][0]['tasks_cache']['state'], "cancelled")

        # Check next scheduled task
        aus_task_scheduled = "SELECT * FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'scheduled'"
        aus_task_scheduled_result = self.run_cbq_query(aus_task_scheduled)
        self.log.info(f"AUS task scheduled result: {aus_task_scheduled_result}")
        # delay should be approximately 24h59m51.71499357s from now (1 day from now)
        expected_delay = 24 * 3600 + delay * 60
        for node in range(self.num_nodes):
            # convert task result delay from format "24h59m51.71499357s" to seconds
            task_delay = aus_task_scheduled_result['results'][node]['tasks_cache']['delay']
            task_delay_hours = int(task_delay.split('h')[0])
            task_delay_minutes = int(task_delay.split('h')[1].split('m')[0])
            task_delay_seconds = int(task_delay.split('m')[1].split('.')[0])
            task_delay_in_seconds = task_delay_hours * 3600 + task_delay_minutes * 60 + task_delay_seconds
            self.assertAlmostEqual(task_delay_in_seconds, expected_delay, delta=60)
        
    def test_cancel_next_scheduled_task(self):
        # Enable AUS and set schedule within next 1 minutes for today for next 3 days
        delay = 1
        start_time = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(minutes=delay)).strftime('%H:%M')
        end_time = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(minutes=delay + 30)).strftime('%H:%M')
        today = datetime.now(pytz.timezone('America/Los_Angeles')).strftime('%A')
        tomorrow = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(days=1)).strftime('%A')
        after_tomorrow = (datetime.now(pytz.timezone('America/Los_Angeles')) + timedelta(days=2)).strftime('%A')

        aus_update = f'UPDATE system:aus SET schedule = {{"start_time": "{start_time}", "end_time": "{end_time}", "timezone": "America/Los_Angeles", "days": ["{today}", "{tomorrow}", "{after_tomorrow}"]}}, enable = true'
        self.run_cbq_query(aus_update)

        # wait 120 seconds for task to be completed
        self.sleep(120)

        # Check completed task
        aus_task_query = "SELECT * FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'completed'"
        aus_task_result = self.run_cbq_query(aus_task_query)
        self.log.info(f"AUS task completed result: {aus_task_result}")
        
        # check next scheduled task for tomorrow
        aus_task_scheduled = "SELECT * FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'scheduled'"
        aus_task_scheduled_result = self.run_cbq_query(aus_task_scheduled)
        self.log.info(f"AUS task scheduled result: {aus_task_scheduled_result}")
        for node in range(self.num_nodes):
            task_id = aus_task_scheduled_result['results'][node]['tasks_cache']['id']
            task_name = aus_task_scheduled_result['results'][node]['tasks_cache']['name']
            expected_delay = 24 * 3600 - delay * 60 + 120
            # convert task result delay to seconds
            task_delay = aus_task_scheduled_result['results'][node]['tasks_cache']['delay']
            task_delay_hours = int(task_delay.split('h')[0])
            task_delay_minutes = int(task_delay.split('h')[1].split('m')[0])
            task_delay_seconds = int(task_delay.split('m')[1].split('.')[0])
            task_delay_in_seconds = task_delay_hours * 3600 + task_delay_minutes * 60 + task_delay_seconds
            self.assertAlmostEqual(task_delay_in_seconds, expected_delay, delta=120)

        # wait 32 minutes to be outside of the 30 minutes schedule time before cancelling
        self.sleep(32 * 60)
    
        # cancel next scheduled task for tomorrow
        aus_cancel = f'DELETE FROM system:tasks_cache WHERE state = "scheduled"'
        self.run_cbq_query(aus_cancel)
        self.sleep(5)

        # Check cancelled task
        aus_task_query = "SELECT * FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'cancelled'"
        aus_task_result = self.run_cbq_query(aus_task_query)
        self.log.info(f"AUS task cancelled result: {aus_task_result}")
        for node in range(self.num_nodes):
            self.assertEqual(aus_task_result['results'][node]['tasks_cache']['state'], "cancelled")

        # Check next scheduled task for after tomorrow 
        aus_task_query = "SELECT * FROM system:tasks_cache WHERE class = 'auto_update_statistics' AND state = 'scheduled'"
        aus_task_result = self.run_cbq_query(aus_task_query)
        self.log.info(f"AUS task scheduled result: {aus_task_result}")
        for node in range(self.num_nodes):
            # delay should be approximately 2 day from now
            expected_delay = 2 * 24 * 3600 - delay * 60 - 32 * 60
            # convert task result delay from format "24h59m51.71499357s" to seconds
            task_delay = aus_task_result['results'][node]['tasks_cache']['delay']
            task_delay_hours = int(task_delay.split('h')[0])
            task_delay_minutes = int(task_delay.split('h')[1].split('m')[0])
            task_delay_seconds = int(task_delay.split('m')[1].split('.')[0])
            task_delay_in_seconds = task_delay_hours * 3600 + task_delay_minutes * 60 + task_delay_seconds
            self.assertAlmostEqual(task_delay_in_seconds, expected_delay, delta=120)
