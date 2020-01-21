import logging
import threading
import json
from .tuq_monitoring import QueryMonitoringTests

class QueryProfilingTests(QueryMonitoringTests):
    def setUp(self):
        super(QueryProfilingTests, self).setUp()
        self.rest.set_profiling(self.master, "off")
        self.rest.set_profiling_controls(self.master, False)

    def suite_setUp(self):
        super(QueryProfilingTests, self).suite_setUp()

    def tearDown(self):
        super(QueryProfilingTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryProfilingTests, self).suite_tearDown()

    '''Check that the profiling settings can be changed, and that those changes happen per node not cluster wide.'''
    def test_set_profile_settings(self):
        # Check that the profile setting can be changed and that the change is on a per node basis
        self.rest.set_profiling(self.master, "phases")
        result = self.rest.get_query_admin_settings(self.master)
        self.assertTrue(result['profile'] == 'phases')
        node2= self.rest.get_query_admin_settings(self.servers[1])
        self.assertTrue(node2['profile'] == 'off')

        self.rest.set_profiling(self.master, "timings")
        result = self.rest.get_query_admin_settings(self.master)
        self.assertTrue(result['profile'] == 'timings')
        node2 = self.rest.get_query_admin_settings(self.servers[1])
        self.assertTrue(node2['profile'] == 'off')

        # If an invalid setting is specified, the setting is not changed 'on' is not a valid setting for profile
        self.rest.set_profiling(self.master, "on")
        result = self.rest.get_query_admin_settings(self.master)
        self.assertTrue(result['profile'] == 'timings')
        node2 = self.rest.get_query_admin_settings(self.servers[1])
        self.assertTrue(node2['profile'] == 'off')

        self.rest.set_profiling(self.master, "off")
        result = self.rest.get_query_admin_settings(self.master)
        self.assertTrue(result['profile'] == 'off')
        node2 = self.rest.get_query_admin_settings(self.servers[1])
        self.assertTrue(node2['profile'] == 'off')

    '''Check that the controls settings can be changed, and that those changes happen per node not cluster wide.'''
    def test_set_controls_settings(self):
        self.rest.set_profiling_controls(self.master, True)
        result = self.rest.get_query_admin_settings(self.master)
        self.assertTrue(result['controls'] == True)
        node2 = self.rest.get_query_admin_settings(self.servers[1])
        self.assertTrue(node2['controls'] == False)

        # Check an invalid value, the setting should be unchanged by an invalid value
        self.rest.set_profiling_controls(self.master, "hello")
        result = self.rest.get_query_admin_settings(self.master)
        self.assertTrue(result['controls'] == True)
        node2 = self.rest.get_query_admin_settings(self.servers[1])
        self.assertTrue(node2['controls'] == False)

        self.rest.set_profiling_controls(self.master, False)
        result = self.rest.get_query_admin_settings(self.master)
        self.assertTrue(result['controls'] == False)
        node2 = self.rest.get_query_admin_settings(self.servers[1])
        self.assertTrue(node2['controls'] == False)

    '''Basic check for the profiling setting profile=phases
        -Check that the default profile setting does not contain phaseTimes information
        -Check that setting profile = phases on the master node does not change the setting on other nodes
        -Check that you can change profile to phases in the middle of a query.'''
    def test_profiling_phases(self):
        # Test 1: Check to see if profiling = off is effective
        self.rest.set_profiling(self.master, "off")
        e = threading.Event()
        thread1 = threading.Thread(name='run_parallel_query', target=self.run_parallel_query,
                                   args=[self.servers[0]])
        thread1.start()
        e.set()
        # Profiling is off, therefore it should not contain phaseTimes
        result = self.run_cbq_query('select * from system:active_requests')
        self.assertTrue('phaseTimes' not in result['results'][0]['active_requests'])
        thread1.join()
        result = self.run_cbq_query('select * from system:completed_requests')
        self.assertTrue('phaseTimes' not in result['results'][0]['completed_requests'])

        # Reset completed log
        self.run_cbq_query('delete from system:completed_requests')

        # Test 2: Turn profiling = phases and make sure it impacts only the node whose setting was changed
        self.rest.set_profiling(self.master, "phases")
        e = threading.Event()
        thread1 = threading.Thread(name='run_parallel_query_master', target=self.run_parallel_query,
                                   args=[self.servers[0]])
        thread2 = threading.Thread(name='run_parallel_query_second', target=self.run_parallel_query,
                                   args=[self.servers[1]])
        thread1.start()
        thread2.start()
        e.set()
        # Make sure the node with profiling set to phases contains phaseTimes and the other query does not.
        result = self.run_cbq_query('select * from system:active_requests')
        self.assertTrue('phaseTimes' in result['results'][0]['active_requests'])
        self.assertTrue('phaseTimes' not in result['results'][2]['active_requests'])
        thread1.join()
        thread2.join()
        # Make sure completed_requests contains phasetimes for the query run on the node with profiling set to phases
        result = self.run_cbq_query('select * from system:completed_requests')
        self.assertTrue('phaseTimes' in result['results'][0]['completed_requests'])
        self.assertTrue('phaseTimes' not in result['results'][1]['completed_requests'])

        # Reset completed log
        self.run_cbq_query('delete from system:completed_requests')

        # Test 3: Set profiling to phases in the middle of the running query and see if it takes effect.
        self.rest.set_profiling(self.master, "off")
        e = threading.Event()
        thread1 = threading.Thread(name='run_parallel_query', target=self.run_parallel_query,
                                   args=[self.servers[0]])
        thread1.start()
        e.set()
        # Change profiling to phases in the middle of the query run and see if phaseTimes is included in active_requests
        self.rest.set_profiling(self.master, "phases")
        result = self.run_cbq_query('select * from system:active_requests')
        self.assertTrue('phaseTimes' in result['results'][0]['active_requests'])
        thread1.join()
        # Check if completed_requests contains the phaseTimes
        result = self.run_cbq_query('select * from system:completed_requests')
        self.assertTrue('phaseTimes' in result['results'][0]['completed_requests'])

    '''Basic check for the profiling setting profile=timings
        -Check that the default profile setting does not contain phaseTimes'''
    def test_profiling_timings_default(self):
        # Test 1: Check to see if profiling = off
        self.rest.set_profiling(self.master, "off")
        e = threading.Event()
        thread1 = threading.Thread(name='run_parallel_query', target=self.run_parallel_query,
                                   args=[self.servers[0]])
        thread1.start()
        e.set()
        # Profiling is off, therefore it should not contain phaseTimes
        result = self.run_cbq_query('select * from system:active_requests')
        self.assertTrue('phaseTimes' not in result['results'][0]['active_requests'])
        thread1.join()
        result = self.run_cbq_query('select * from system:completed_requests')
        self.assertTrue('phaseTimes' not in result['results'][0]['completed_requests'])

    """Check that setting profile = timings on the master node does not change the setting on other nodes"""
    def test_profiling_timings_propagation(self):
        # Create flags to ensure the queries were both inside of active_requests
        query_one = False
        query_two = False
        # Test 2: Set profile = timings and make sure it impacts only the node whose setting was changed
        self.rest.set_profiling(self.master, "timings")
        e = threading.Event()
        thread1 = threading.Thread(name='run_parallel_query_master', target=self.run_parallel_query,
                                   args=[self.servers[0]])
        thread2 = threading.Thread(name='run_parallel_query_second', target=self.run_parallel_query,
                                   args=[self.servers[1]])
        thread1.start()
        thread2.start()
        e.set()
        # Make sure the node with profiling set to phases contains phaseTimes and the other query does not.
        results = self.run_cbq_query('select *, meta().plan from system:active_requests')
        for result in results['results']:
            if result['active_requests']['statement'] == 'select * from default union select * from default' \
                    and result['active_requests']['node'] =='%s:%s' % (self.master.ip, self.master.port):
                self.assertTrue('plan' in result and 'phaseTimes' in result['active_requests'])
                query_one = True
            elif result['active_requests']['statement'] == 'select * from default union select * from default':
                self.assertTrue('plan' not in result and
                                'phaseTimes' not in result['active_requests'])
                query_two = True

        self.assertTrue(query_one and query_two, "One of the queries did not appear inside of active_requests : %s" % results)

        thread1.join()
        thread2.join()

        query_one = False
        query_two = False
        # Make sure completed_requests contains phasetimes for the query run on the node with profiling set to phases
        results = self.run_cbq_query('select *, meta().plan from system:completed_requests')
        for result in results['results']:
            if result['completed_requests']['statement'] == 'select * from default union select * from default' \
                    and result['completed_requests']['node'] =='%s:%s' % (self.master.ip, self.master.port):
                self.assertTrue('plan' in result and 'phaseTimes' in result['completed_requests'])
                query_one = True
            elif result['completed_requests']['statement'] == 'select * from default union select * from default':
                self.assertTrue('plan' not in result and
                                'phaseTimes' not in result['completed_requests'])
                query_two = True

        self.assertTrue(query_one and query_two, "One of the queries did not appear inside of completed_requests : %s" % results)

    """-Check that you can change profile to timings in the middle of a query."""
    def test_profiling_timings_in_flight(self):
        # Test 3: Set profiling to phases in the middle of the running query and see if it takes effect.
        self.rest.set_profiling(self.master, "off")
        e = threading.Event()
        thread1 = threading.Thread(name='run_parallel_query', target=self.run_parallel_query,
                                   args=[self.servers[0]])
        thread1.start()
        e.set()
        # Change profiling to phases in the middle of the query run and see if phaseTimes is included in active_requests
        self.rest.set_profiling(self.master, "timings")
        result = self.run_cbq_query('select *, meta().plan from system:active_requests')
        self.assertTrue('plan' in result['results'][0] and 'phaseTimes' in result['results'][0]['active_requests'])
        thread1.join()
        # Check if completed_requests contains the phaseTimes
        result = self.run_cbq_query('select *, meta().plan from system:completed_requests')
        self.assertTrue('plan' in result['results'][0] and 'phaseTimes' in result['results'][0]['completed_requests'])


    def test_profiling_controls(self):
        self.rest.get_query_admin_settings(self.master)
        e = threading.Event()
        thread1 = threading.Thread(name='run_controlled_query', target=self.run_controlled_query,
                                   args=[self.servers[0]])
        thread1.start()
        e.set()
        result = self.run_cbq_query('select * from system:active_requests')
        self.log.info(json.dumps(result, sort_keys=True, indent=3))
        thread1.join()
        result = self.run_cbq_query('select * from system:completed_requests')
        self.log.info(json.dumps(result, sort_keys=True, indent=3))

        self.rest.set_profiling_controls(self.master, True)
        result = self.rest.get_query_admin_settings(self.master)
        self.log.info(result)
        e = threading.Event()
        thread1 = threading.Thread(name='run_controlled_query', target=self.run_controlled_query,
                                   args=[self.servers[0]])
        thread1.start()
        e.set()
        result = self.run_cbq_query('select * from system:active_requests')
        self.log.info(json.dumps(result, sort_keys=True, indent=3))
        thread1.join()
        result = self.run_cbq_query('select * from system:completed_requests')
        self.log.info(json.dumps(result, sort_keys=True, indent=3))


    def run_parallel_query(self, server):
        logging.info('parallel query is active')
        query = 'select * from default union select * from default'
        self.run_cbq_query(query, server=server)

    def run_controlled_query(self, server):
        logging.info('parallel query is active')
        query = 'select * from default where a=$a&$a=1'
        self.run_cbq_query(query, server=server)