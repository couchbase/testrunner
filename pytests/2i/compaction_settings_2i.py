import logging
from datetime import datetime
from .base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

log = logging.getLogger(__name__)
QUERY_TEMPLATE = "SELECT {0} FROM %s "
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

class SecondaryIndexingCompactionTests(BaseSecondaryIndexingTests):
    def setUp(self):
        self.change_indexer_time = False
        super(SecondaryIndexingCompactionTests, self).setUp()
        self.assertNotEqual(self.gsi_type.lower(), "memory_optimized", "GSI type is set to MOI")
        self.initial_index_number = self.input.param("initial_index_number", 2)
        query_template = QUERY_TEMPLATE
        self.query_template = query_template.format("job_title")
        self.whereCondition= self.input.param("whereCondition", " job_title != \"Sales\" ")
        self.query_template += " WHERE {0}".format(self.whereCondition)
        self.load_query_definitions = []
        for x in range(self.initial_index_number):
            index_name = "index_name_"+str(x)
            query_definition = QueryDefinition(index_name=index_name, index_fields = ["job_title"],
                        query_template = self.query_template, groups = ["simple"])
            self.load_query_definitions.append(query_definition)
        self.multi_create_index(self.buckets, self.load_query_definitions)


    def tearDown(self):
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if self.change_indexer_time:
            self.change_system_date(servers, "yesterday")
        super(SecondaryIndexingCompactionTests, self).tearDown()

    def test_set_append_only_compaction(self):
        fragmentation = [20, 45, 34, 50]
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(servers[0])
        initial_index_map = rest.get_index_stats()

        def set_compaction(frag):
            status, content, header = rest.set_indexer_compaction(mode="full", fragmentation=frag)
            self.assertTrue(status, "Error in setting Append Only Compaction... {0}".format(content))
            self.sleep(120)

        def check_compaction():
            count = 0
            check = False
            while not check and count < 10:
                final_index_map = rest.get_index_stats()
                for bucket in self.buckets:
                    for index in list(final_index_map[bucket.name].keys()):
                        initial_compaction = initial_index_map[bucket.name][index]["num_compactions"]
                        final_compaction = final_index_map[bucket.name][index]["num_compactions"]
                        check = initial_compaction < final_compaction
                        if not check:
                            count += 1
                            break
                self.sleep(60)
                count += 1
            return check
        kv_ops = self._run_kvops_tasks()
        for frag in fragmentation:
            set_compaction(frag)
            self.assertTrue(check_compaction(), "Compaction was not triggered for {0}% fragmentation".format(frag))
            initial_index_map = rest.get_index_stats()
        self._run_tasks(kv_ops)

    def test_set_circular_compaction(self):
        kv_ops = self._run_kvops_tasks()
        date = datetime.now()
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(servers[0])
        initial_index_map = rest.get_index_stats()
        #Trust Me this works
        dayOfWeek = (date.weekday() + (date.hour+((date.minute+5)//60))//24)%7
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+1)//60),
                                              indexFromMinute=(date.minute+1)%60)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))
        self.sleep(120)
        final_index_map = rest.get_index_stats()
        self.check_compaction_number(initial_index_map, final_index_map)
        self._run_tasks(kv_ops)

    def test_set_compaction_during_compaction(self):
        kv_ops = self._run_kvops_tasks()
        date = datetime.now()
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(servers[0])
        initial_index_map = rest.get_index_stats()
        #Trust Me this works
        dayOfWeek = (date.weekday() + (date.hour+((date.minute+5)//60))//24)%7
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+2)//60),
                                              indexFromMinute=(date.minute+2)%60)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))
        self.sleep(140)
        dayOfWeek = (dayOfWeek+1)%7
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+5)//60),
                                              indexFromMinute=(date.minute+5)%60)
        self.assertTrue(status, "Error in setting Circular Compaction during compaction... {0}".format(content))
        final_index_map = rest.get_index_stats()
        self.check_compaction_number(initial_index_map, final_index_map)
        self._run_tasks(kv_ops)

    def test_set_compaction_end_time_abort(self):
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(servers[0])
        initial_index_map = rest.get_index_stats()
        kv_ops = self._run_kvops_tasks()
        date = datetime.now()
        #Trust Me this works
        dayOfWeek = (date.weekday() + (date.hour+((date.minute+5)//60))//24)%7
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+2)//60),
                                              indexFromMinute=(date.minute+2)%60,
                                              indexToHour=date.hour+((date.minute+3)//60),
                                              indexToMinute=(date.minute+3)%60,
                                              abortOutside=True)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))
        self.sleep(300)
        final_index_map = rest.get_index_stats()
        self.check_compaction_number(initial_index_map, final_index_map)
        self._validate_compaction_for_abort_or_complete()
        self._run_tasks(kv_ops)

    def test_set_compaction_start_time_greater_than_end_time(self):
        kv_ops = self._run_kvops_tasks()
        date = datetime.now()
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(servers[0])
        initial_index_map = rest.get_index_stats()
        #Trust Me this works
        dayOfWeek = (date.weekday() + (date.hour+((date.minute+5)//60))//24)%7
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+2)//60),
                                              indexFromMinute=(date.minute+2)%60,
                                              indexToHour=date.hour+((date.minute+1)//60),
                                              indexToMinute=(date.minute+1)%60,
                                              abortOutside=True)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))
        self.sleep(180)
        final_index_map = rest.get_index_stats()
        self.check_compaction_number(initial_index_map, final_index_map)
        self._validate_compaction_for_abort_or_complete()
        self._run_tasks(kv_ops)

    def test_set_compaction_end_time_during_compaction(self):
        kv_ops = self._run_kvops_tasks()
        date = datetime.now()
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(servers[0])
        initial_index_map = rest.get_index_stats()
        #Trust Me this works
        dayOfWeek = (date.weekday() + (date.hour+((date.minute+5)//60))//24)%7
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+2)//60),
                                              indexFromMinute=(date.minute+2)%60)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))
        self.sleep(140)
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+2)//60),
                                              indexFromMinute=(date.minute+2)%60,
                                              indexToHour=date.hour+(date.minute+4)//60,
                                              indexToMinute=(date.minute+4)%60,
                                              abortOutside=True)
        self.assertTrue(status, "Error in setting Circular Compaction during compaction... {0}".format(content))
        self.sleep(60)
        final_index_map = rest.get_index_stats()
        self.check_compaction_number(initial_index_map, final_index_map)
        self._validate_compaction_for_abort_or_complete()
        self._run_tasks(kv_ops)

    def test_set_compaction_abort_during_compaction(self):
        kv_ops = self._run_kvops_tasks()
        date = datetime.now()
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(servers[0])
        initial_index_map = rest.get_index_stats()
        #Trust Me this works
        dayOfWeek = (date.weekday() + (date.hour+((date.minute+5)//60))//24)%7
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+2)//60),
                                              indexFromMinute=(date.minute+2)%60,
                                              indexToHour=date.hour+(date.minute+3)//60,
                                              indexToMinute=(date.minute+3)%60,
                                              abortOutside=False)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))
        self.sleep(140)
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+2)//60),
                                              indexFromMinute=(date.minute+2)%60,
                                              indexToHour=date.hour+(date.minute+3)//60,
                                              indexToMinute=(date.minute+3)%60,
                                              abortOutside=True)
        self.assertTrue(status, "Error in setting Circular Compaction during compaction... {0}".format(content))
        self.sleep(60)
        final_index_map = rest.get_index_stats()
        self.check_compaction_number(initial_index_map, final_index_map)
        self._validate_compaction_for_abort_or_complete()
        self._run_tasks(kv_ops)

    def test_abort_24_hours(self):
        kv_ops = self._run_kvops_tasks()
        date = datetime.now()
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(servers[0])
        initial_index_map = rest.get_index_stats()
        #Trust Me this works
        dayOfWeek = (date.weekday() + (date.hour+((date.minute+5)//60))//24)%7
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+2)//60),
                                              indexFromMinute=(date.minute+2)%60,
                                              abortOutside=True)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))
        self.sleep(140)
        self.change_system_date(servers, "tomorrow")
        self.sleep(30)
        final_index_map = rest.get_index_stats()
        self.check_compaction_number(initial_index_map, final_index_map)
        self._validate_compaction_for_abort_or_complete()
        self.change_system_date(servers, "yesterday")
        self._run_tasks(kv_ops)

    def test_24_hours_without_abort(self):
        kv_ops = self._run_kvops_tasks()
        date = datetime.now()
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(servers[0])
        initial_index_map = rest.get_index_stats()
        #Trust Me this works
        dayOfWeek = (date.weekday() + (date.hour+((date.minute+5)//60))//24)%7
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+2)//60),
                                              indexFromMinute=(date.minute+2)%60,
                                              abortOutside=False)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))
        self.sleep(140)
        self.change_system_date(servers, "tomorrow")
        self.sleep(30)
        final_index_map = rest.get_index_stats()
        self.check_compaction_number(initial_index_map, final_index_map)
        self._validate_compaction_for_abort_or_complete()
        self.change_system_date(servers, "yesterday")
        self._run_tasks(kv_ops)

    def test_change_start_time_twice_a_day(self):
        kv_ops = self._run_kvops_tasks()
        date = datetime.now()
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(servers[0])
        initial_index_map = rest.get_index_stats()
        #Trust Me this works
        dayOfWeek = (date.weekday() + (date.hour+((date.minute+5)//60))//24)%7
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+2)//60),
                                              indexFromMinute=(date.minute+2)%60)
        self.assertTrue(status, "Error in setting Circular Compaction... {0}".format(content))
        self.sleep(200)
        first_index_map = rest.get_index_stats()
        self.check_compaction_number(initial_index_map, first_index_map)
        status, content, header = rest.set_indexer_compaction(indexDayOfWeek=DAYS[dayOfWeek],
                                              indexFromHour=date.hour+((date.minute+5)//60),
                                              indexFromMinute=(date.minute+5)%60)
        self.assertTrue(status, "Error in setting Circular Compaction during compaction... {0}".format(content))
        self.sleep(300)
        second_index_map = rest.get_index_stats()
        self.check_compaction_number(first_index_map, second_index_map)
        self._run_tasks(kv_ops)

    def change_system_date(self, servers, date):
        for server in servers:
            log.info("Changing system time on {0}".format(server.ip))
            remote = RemoteMachineShellConnection(server)
            remote.stop_couchbase()
            cmd = "date -s '{0}'".format(date)
            success, error = remote.execute_command(cmd)
            remote.start_couchbase()
        self.change_indexer_time = True

    def check_compaction_number(self, initial_map, final_map, buckets=None):
        if not buckets:
            buckets = self.buckets
        for bucket in buckets:
            for index in list(final_map[bucket.name].keys()):
                initial_compaction = initial_map[bucket.name][index]["num_compactions"]
                final_compaction = final_map[bucket.name][index]["num_compactions"]
                self.assertTrue((initial_compaction<final_compaction),
                                "Initial Compaction {0} is not less than final Compaction {1} for index {2} \
                                 on bucket {3}".format(initial_compaction, final_compaction,
                                                       index, bucket.name))

    def _run_kvops_tasks(self):
        tasks_ops =[]
        if self.doc_ops:
            tasks_ops = self.async_run_doc_ops()
        return tasks_ops

    def _run_tasks(self, tasks):
        for task in tasks:
            task.result()

    def _validate_compaction_for_abort_or_complete(self):
        pass

    def _validate_compaction_log(self):
        pass
