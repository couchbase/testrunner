from pytests.tuqquery.tuq import QueryTests
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
import threading


class QueryStatsTests(QueryTests):
    def setUp(self):
        super(QueryStatsTests, self).setUp()
        self.log.info("==============  QueryStatsTests setup has started ==============")
        self.shell = RemoteMachineShellConnection(self.master)
        self.rest = RestConnection(self.master)
        self.info = self.shell.extract_remote_info()
        if self.info.type.lower() == 'windows':
            self.curl_path = "%scurl" % self.path
        else:
            self.curl_path = "curl"

    def tearDown(self):
        super(QueryStatsTests, self).tearDown()
        self.log.info("==============  QueryStatsTests teardown has started ==============")

    def test_query_errors(self):
        try:
            self.run_cbq_query("SELECT * FROM fake_bucket")
        except Exception as e:
            self.log.info(f"Error running query: {e}")
            pass
        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_errors")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
    
    def test_query_warnings(self):
        try:
            self.run_cbq_query("SELECT * FROM default USE KEYS VALIDATE ['missing_key1', 'missing_key2'];")
        except Exception as e:
            self.log.info(f"Error running query: {e}")
        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_warnings")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
    
    def test_query_invalid_requests(self):
        try:
            self.run_cbq_query("SELECT * FROM fake_bucket")
        except Exception as e:
            self.log.info(f"Error running query: {e}")
            pass
        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_invalid_requests")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
    
    def test_query_result_count_size(self):
        try:
            self.run_cbq_query("SELECT * FROM default LIMIT 10000;")
        except Exception as e:
            self.log.info(f"Error running query: {e}")
        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_result_count")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
        stat = self.get_query_stat_for_node(self.master, "query_result_size")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
    
    def test_query_service_time(self):
        try:
            self.run_cbq_query("SELECT * FROM default LIMIT 10000;")
        except Exception as e:
            self.log.info(f"Error running query: {e}")
        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_service_time")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
    
    def test_query_active_requests(self):
        threads = []
        for _ in range(30):
            t = threading.Thread(target=self.run_query_in_thread)
            threads.append(t)
            t.start()

        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_active_requests")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
        for t in threads:
            t.join()
    
    def test_query_selects(self):
        threads = []
        for _ in range(30):
            t = threading.Thread(target=self.run_query_in_thread)
            threads.append(t)
            t.start()

        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_selects")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
        for t in threads:
            t.join()
    
    def test_query_requests(self):
        threads = []
        for _ in range(30):
            t = threading.Thread(target=self.run_query_in_thread)
            threads.append(t)
            t.start()

        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_requests")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
        for t in threads:
            t.join()
    
    def test_query_requests_1000ms(self):
        threads = []
        for _ in range(5):
            t = threading.Thread(target=self.run_query_in_thread)
            threads.append(t)
            t.start()

        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_requests_1000ms")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
        for t in threads:
            t.join()
            
    def test_query_requests_5000ms(self):
        threads = []
        for _ in range(30):
            t = threading.Thread(target=self.run_query_in_thread_5000ms)
            threads.append(t)
            t.start()

        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_requests_5000ms")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
        for t in threads:
            t.join()
    
    def test_query_request_time(self):
        threads = []
        for _ in range(30):
            t = threading.Thread(target=self.run_query_in_thread)
            threads.append(t)
            t.start()

        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_request_time")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
        for t in threads:
            t.join()
    
    def test_query_queued_requests(self):
        threads = []
        for _ in range(100):
            t = threading.Thread(target=self.run_query_in_thread)
            threads.append(t)
            t.start()

        self.sleep(10)
        stat = self.get_query_stat_for_node(self.master, "query_queued_requests")
        stat_value = float(stat.split(": ")[1])
        self.assertTrue(stat_value > 0.0, "We expect the stat value to be greater than 0")
        for t in threads:
            t.join()
    
    def get_query_stat_for_node(self, node,stat_name):
        endpoint = f"http://{node.ip}:8091/pools/default/buckets/@query/nodes/{node.ip}:8091/stats"
        curl_output = self.shell.execute_command(f"{self.curl_path} --user {self.rest.username}:{self.rest.password} --silent --request GET --data zoom=minute {endpoint} | jq -r -c '.op.samples | \"{stat_name}: \" + (.{stat_name} | add | tostring)'")
        self.log.info(curl_output)
        return curl_output[0][0]
    
    def run_query_in_thread(self):
        try:
            self.run_cbq_query("SELECT * FROM default;")
        except Exception as e:
            self.log.info(f"Error running query: {e}")
    
    def run_query_in_thread_5000ms(self):
        try:
            self.run_cbq_query("SELECT * FROM default UNION SELECT * FROM default UNION SELECT * FROM default;")
        except Exception as e:
            self.log.info(f"Error running query: {e}")