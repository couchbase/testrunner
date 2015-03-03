from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition

class SecondaryIndexingStatsConfigTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingStatsConfigTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingStatsConfigTests, self).tearDown()

    def test_index_stats(self):
        #Create Index
        self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = True, drop_index = False)
        #Check Index Stats
        index_map = self.get_index_stats()
        self.log.info(index_map)
        for query_definition in self.query_definitions:
            index_name = query_definition.index_name
            for bucket in self.buckets:
                bucket_name = bucket.name
                check_keys = ['items_count', 'total_scan_duration', 'num_docs_queued',
                 'num_requests', 'num_rows_returned', 'num_docs_queued','num_docs_pending','delete_bytes' ]
                map = self._create_stats_map(items_count = 2016)
                self.verify_index_stats(index_map, index_name, bucket_name, map, check_keys)
        #Drop Index
        self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = False, drop_index = True)
        index_map = self.get_index_stats()
        self.assertTrue(len(index_map) == 0, "Index Stats still show {0}".format(index_map))

    def test_get_index_settings(self):
        #Check Index Settings
        map = self.get_index_settings()
        for node in map.keys():
            val = map[node]
            gen = self._create_settings_map()
            for key in val.keys():
                self.assertTrue(key in gen.keys(), "{0} != {1} ".format(val, gen))

    def test_set_index_settings(self):
        #Check Index Settings
        map1 = self._set_settings_map()
        self.log.info(map1)
        self.set_index_settings(map1)
        map = self.get_index_settings()
        for node in map.keys():
            val = map[node]
            self.assertTrue(sorted(val) == sorted(map1), "{0} != {1} ".format(val, map1))

    def _create_stats_map(self, items_count = 0, total_scan_duration = 0,
        delete_bytes = 0, scan_wait_duration = 0, insert_bytes = 0,
        num_rows_returned = 0, num_docs_indexed = 0, num_docs_pending = 0,
        scan_bytes_read = 0, get_bytes = 0, num_docs_queued = 0,num_requests = 0,
        disk_size = 0):
        map = {}
        map['items_count'] = items_count
        map['disk_size'] = disk_size
        map['items_count'] = items_count
        map['total_scan_duration'] = total_scan_duration
        map['delete_bytes'] = delete_bytes
        map['scan_wait_duration'] = scan_wait_duration
        map['insert_bytes'] = insert_bytes
        map['num_rows_returned'] = num_rows_returned
        map['num_docs_indexed'] = num_docs_indexed
        map['num_docs_pending'] = num_docs_pending
        map['scan_bytes_read'] = scan_bytes_read
        map['get_bytes'] = get_bytes
        map['num_docs_queued'] = num_docs_queued
        map['num_requests'] = num_requests
        return map

    def _create_settings_map(self):
        map = {"indexer.settings.compaction.check_period":1200000,
        "indexer.settings.compaction.interval":"00:00,00:00",
        "indexer.settings.compaction.min_frag":30,
        "indexer.settings.compaction.min_size":1048576,
        "indexer.settings.inmemory_snapshot.interval":200,
        "indexer.settings.log_level":"info",
        "indexer.settings.log_override":"",
        "indexer.settings.max_cpu_percent":400,
        "indexer.settings.memory_quota":0,
        "indexer.settings.persisted_snapshot.interval":30000,
        "indexer.settings.recovery.max_rollbacks":5,
        "projector.settings.log_level":"info",
        "projector.settings.log_override":""}
        return map

    def _set_settings_map(self):
        map = {
        "indexer.settings.compaction.check_period":120,
        "indexer.settings.compaction.interval":"10:10,20:00",
        "indexer.settings.compaction.min_frag":3,
        "indexer.settings.compaction.min_size":1048,
        "indexer.settings.inmemory_snapshot.interval":2,
        "indexer.settings.log_level":"info",
        "indexer.settings.log_override":"",
        "indexer.settings.max_cpu_percent":40,
        "indexer.settings.memory_quota":100,
        "indexer.settings.persisted_snapshot.interval":300,
        "indexer.settings.recovery.max_rollbacks":1,
        "projector.settings.log_level":"info",
        "projector.settings.log_override":""
        }
        return map