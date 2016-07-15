from base_2i import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection
from datetime import datetime

class SecondaryIndexingStatsConfigTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingStatsConfigTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingStatsConfigTests, self).tearDown()

    def test_index_stats(self):
        """
        Tests index stats when indexes are created and dropped
        """
        #Create Index
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = False)
        #Check Index Stats
        self.sleep(30)
        index_map = self.get_index_stats()
        self.log.info(index_map)
        for query_definition in self.query_definitions:
            index_name = query_definition.index_name
            for bucket in self.buckets:
                bucket_name = bucket.name
                check_keys = ['items_count', 'total_scan_duration', 'num_docs_queued',
                 'num_requests', 'num_rows_returned', 'num_docs_queued',
                 'num_docs_pending','delete_bytes' ]
                map = self._create_stats_map(items_count=2016)
                self._verify_index_stats(index_map, index_name, bucket_name, map, check_keys)

    def test_get_index_settings(self):
        #Check Index Settings
        map = self.get_index_settings()
        for node in map.keys():
            val = map[node]
            gen = self._create_settings_map()
            for key in gen.keys():
                self.assertTrue(key in val.keys(), "{0} not in {1} ".format(key, val))

    def test_set_index_settings(self):
        #Check Index Settings
        map1 = self._set_settings_map()
        self.log.info(map1)
        self.set_index_settings(map1)
        map = self.get_index_settings()
        for node in map.keys():
            val = map[node]
            for key in map1.keys():
                self.assertTrue(key in val.keys(), "{0} not in {1} ".format(key, val))

    def _verify_index_stats(self, index_map, index_name, bucket_name, index_stat_values, check_keys=None):
        self.assertIn(bucket_name, index_map.keys(), "bucket name {0} not present in stats".format(bucket_name))
        self.assertIn(index_name, index_map[bucket_name].keys(),
                        "index name {0} not present in set of indexes {1}".format(index_name,
                                                                                  index_map[bucket_name].keys()))
        for key in index_stat_values.keys():
            self.assertIn(key, index_map[bucket_name][index_name].keys(),
                            "stats {0} not present in Index stats {1}".format(key,
                                                                                  index_map[bucket_name][index_name]))
            if check_keys:
                if key in check_keys:
                    self.assertEqual(str(index_map[bucket_name][index_name][key]), str(index_stat_values[key]),
                                    " for key {0} : {1} != {2}".format(key,
                                                                       index_map[bucket_name][index_name][key],
                                                                       index_stat_values[key]))
            else:
                self.assertEqual(str(index_stat_values[key]), str(index_map[bucket_name][index_name][key]),
                                " for key {0} : {1} != {2}".format(key,
                                                                   index_map[bucket_name][index_name][key],
                                                                   index_stat_values[key]))

    def set_index_settings(self, settings):
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for server in servers:
            RestConnection(server).set_index_settings(settings)

    def get_index_settings(self):
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        index_settings_map = {}
        for server in servers:
            key = "{0}:{1}".format(server.ip, server.port)
            index_settings_map[key] = RestConnection(server).get_index_settings()
        return index_settings_map

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
        map = { "indexer.settings.recovery.max_rollbacks" : 5,
        "indexer.settings.bufferPoolBlockSize" : 16384,
        "indexer.settings.max_cpu_percent" : 400,
        "queryport.client.settings.poolOverflow" : 30,
        "indexer.settings.memProfile" : False,
        "indexer.settings.statsLogDumpInterval" : 60,
        "indexer.settings.persisted_snapshot.interval" : 5000,
        "indexer.settings.inmemory_snapshot.interval" : 200,
        "indexer.settings.compaction.check_period" : 30,
        "indexer.settings.largeSnapshotThreshold" : 200,
        "indexer.settings.log_level" : "debug",
        "indexer.settings.scan_timeout" : 120000,
        "indexer.settings.maxVbQueueLength" : 0,
        "indexer.settings.send_buffer_size" : 1024,
        "indexer.settings.compaction.min_size" : 1048576,
        "indexer.settings.cpuProfFname" : "",
        "indexer.settings.memory_quota" : 268435456,
        "indexer.settings.memProfFname" : "",
        "projector.settings.log_level" : "debug",
        "queryport.client.settings.poolSize" : 1000,
        "indexer.settings.max_writer_lock_prob" : 20,
        "indexer.settings.compaction.interval" : "00:00,00:00",
        "indexer.settings.cpuProfile" : False,
        "indexer.settings.compaction.min_frag" : 30,
        "indexer.settings.sliceBufSize" : 50000,
        "indexer.settings.wal_size" : 4096,
        "indexer.settings.fast_flush_mode" : True,
        "indexer.settings.smallSnapshotThreshold" : 30,
        "indexer.settings.persisted_snapshot_init_build.interval": 5000
}
        return map

    def _set_settings_map(self):
        map = { "indexer.settings.recovery.max_rollbacks" : 4,
        "indexer.settings.bufferPoolBlockSize" : 16384,
        "indexer.settings.max_cpu_percent" : 400, 
        "indexer.settings.memProfile" : False,
        "indexer.settings.statsLogDumpInterval" : 60,
        "indexer.settings.persisted_snapshot.interval" : 5000,
        "indexer.settings.inmemory_snapshot.interval" : 200,
        "indexer.settings.compaction.check_period" : 31,
        "indexer.settings.largeSnapshotThreshold" : 200,
        "indexer.settings.log_level" : "debug",
        "indexer.settings.scan_timeout" : 120000,
        "indexer.settings.maxVbQueueLength" : 0,
        "indexer.settings.send_buffer_size" : 1024,
        "indexer.settings.compaction.min_size" : 1048576,
        "indexer.settings.cpuProfFname" : "",
        "indexer.settings.memory_quota" : 268435456,
        "indexer.settings.memProfFname" : "",
        "indexer.settings.persisted_snapshot_init_build.interval": 5000,
        "indexer.settings.max_writer_lock_prob" : 20,
        "indexer.settings.compaction.interval" : "00:00,00:00",
        "indexer.settings.cpuProfile" : False,
        "indexer.settings.compaction.min_frag" : 31,
        "indexer.settings.sliceBufSize" : 50000,
        "indexer.settings.wal_size" : 4096,
        "indexer.settings.fast_flush_mode" : True,
        "indexer.settings.smallSnapshotThreshold" : 30,
        "projector.settings.log_level" : "debug",
        "queryport.client.settings.poolSize" : 1000,
        "queryport.client.settings.poolOverflow" : 30
}
        return map
