# coding=utf-8

from threading import Thread

from .fts_base import FTSBaseTest
from lib.membase.api.rest_client import RestConnection

class FTSIndexCompactionAPI(FTSBaseTest):

    def setUp(self):
        super(FTSIndexCompactionAPI, self).setUp()
        self.rest = RestConnection(self._cb_cluster.get_random_fts_node())

    def tearDown(self):
        super(FTSIndexCompactionAPI, self).tearDown()

    def test_start_compaction(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        for idx in self._cb_cluster.get_indexes():
            start_compaction_status, start_compaction_content = self.rest.start_fts_index_compaction(idx.name)
            self.assertTrue(start_compaction_status, f"Index compaction did not started, response is: \n{start_compaction_content}")

    def test_start_compaction_sequential(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        for idx in self._cb_cluster.get_indexes():
            start_compaction_status, start_compaction_content = self.rest.start_fts_index_compaction(idx.name)
            start_compaction_status_again, start_compaction_content_again = self.rest.start_fts_index_compaction(idx.name)
            self.assertTrue(start_compaction_status_again, f"Second sequential index compaction call is failed, "
                                                           f"response is: \n{start_compaction_content}")

    def test_concurrent_compaction(self):
        RestConnection(self.master).modify_memory_quota(index_quota=256,kv_quota=1000,fts_quota=2500)
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        for idx in self._cb_cluster.get_indexes():
            start_compaction_status, start_compaction_content = self.rest.start_fts_index_compaction(idx.name)
            start_compaction_status_again, start_compaction_content_again = self.rest.start_fts_index_compaction(idx.name)
            self.assertTrue(start_compaction_status_again, f"Second sequential index compaction call is failed, "
                                                           f"response is: \n{start_compaction_content}")

        compaction_threads = []
        for idx in self._cb_cluster.get_indexes():
            compaction_thread = Thread(target=self._call_compaction, args=(idx.name,), daemon=True)
            compaction_thread.start()
            compaction_threads.append(compaction_thread)
        for th in compaction_threads:
            th.join()

    def test_start_alias_compaction(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()

        alias_name = 'idx_alias'
        alias_def = {"targets": {}}
        for idx in self._cb_cluster.get_indexes():
            alias_def['targets'][idx.name] = {}

        index_alias = self._cb_cluster.create_fts_index(name=alias_name,
                                          index_type='fulltext-alias',
                                          index_params=alias_def)
        start_compaction_status, start_compaction_content = self.rest.start_fts_index_compaction(index_alias.name)
        self.assertFalse(start_compaction_status,
                         f"Index alias compaction started unexpectedly, response is: \n{start_compaction_content}")

    def test_start_compaction_and_mutations_concurrently(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()

        threads = []
        for bucket in self._cb_cluster.get_buckets():
            self._cb_cluster.run_n1ql_query(f'create primary index on {bucket.name}')
            mutation_thread = Thread(target=self._call_mutations, args=(bucket.name,))
            mutation_thread.start()
            threads.append(mutation_thread)

        for idx in self._cb_cluster.get_indexes():
            compaction_thread = Thread(target=self._call_compaction, args=(idx.name,))
            compaction_thread.start()
            threads.append(compaction_thread)

        for th in threads:
            th.join()

    def test_start_compaction_and_delete_index_concurrently(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()

        threads = []

        for idx in self._cb_cluster.get_indexes():
            compaction_thread = Thread(target=self._call_compaction, args=(idx.name,))
            compaction_thread.start()
            threads.append(compaction_thread)

        for idx in self._cb_cluster.get_indexes():
            index_drop_thread = Thread(target=self._drop_fts_index, args=(idx,))
            index_drop_thread.start()
            threads.append(index_drop_thread)

        for th in threads:
            th.join()

    def test_start_compaction_and_kv_drop_concurrently(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()

        threads = []

        for idx in self._cb_cluster.get_indexes():
            compaction_thread = Thread(target=self._call_compaction, args=(idx.name,))
            compaction_thread.start()
            threads.append(compaction_thread)

        for bucket in self._cb_cluster.get_buckets():
            bucket_drop_thread = Thread(target=self._drop_bucket, args=(bucket,))
            bucket_drop_thread.start()
            threads.append(bucket_drop_thread)

        for th in threads:
            th.join()

    def test_start_compaction_missed_index_negative(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        start_compaction_status, start_compaction_content = self.rest.start_fts_index_compaction("missed_index")
        self.assertFalse(start_compaction_status, f"Index compaction for missed index is started, response is: \n{start_compaction_content}")
        self.assertTrue("index not found" in start_compaction_content['error'], f"Unexpected error message, response is: \n{start_compaction_content}")

    def test_index_size_reduction(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        indexes_stats = {}
        for idx in self._cb_cluster.get_indexes():
            idx.index_definition['params']['store']['numSnapshotsToKeep'] = 1
            idx.index_definition['uuid'] = idx.get_uuid()
            idx.update()

            indexes_stats[idx.name] = {}
            _, stat_val = self.rest.get_fts_stats(index_name=idx.name, bucket_name=idx._source_name, stat_name='num_root_filesegments')
            indexes_stats[idx.name]['num_root_filesegments_before_compaction'] = stat_val
            _, stat_val = self.rest.get_fts_stats(index_name=idx.name, bucket_name=idx._source_name,
                                                  stat_name='num_bytes_used_disk')
            indexes_stats[idx.name]['num_bytes_used_disk_before_compaction'] = stat_val

            self.rest.start_fts_index_compaction(idx.name)
            while not self._is_compaction_finished(idx=idx):
                self.sleep(2, "Waiting for compaction finish")

            _, stat_val = self.rest.get_fts_stats(index_name=idx.name, bucket_name=idx._source_name, stat_name='num_root_filesegments')
            indexes_stats[idx.name]['num_root_filesegments_after_compaction'] = stat_val
            _, stat_val = self.rest.get_fts_stats(index_name=idx.name, bucket_name=idx._source_name,
                                                  stat_name='num_bytes_used_disk')
            indexes_stats[idx.name]['num_bytes_used_disk_after_compaction'] = stat_val

        for idx in self._cb_cluster.get_indexes():
            self.assertTrue(indexes_stats[idx.name]['num_root_filesegments_before_compaction'] >=
                            indexes_stats[idx.name]['num_root_filesegments_after_compaction'],
                            "Index disk segments number has increased after compaction.")

            self.assertTrue(indexes_stats[idx.name]['num_bytes_used_disk_before_compaction'] >=
                            indexes_stats[idx.name]['num_bytes_used_disk_after_compaction'],
                            "Index size has increased after compaction.")

    def test_cancel_compaction(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        for idx in self._cb_cluster.get_indexes():
            self.rest.start_fts_index_compaction(idx.name)
            _, content = self.rest.get_fts_index_compactions(idx.name)
            compaction_keys = content['status'].keys()
            cancel_key = ''
            cancel_response = ''
            for key in compaction_keys:
                cancel_key = key
                _, cancel_response = self.rest.cancel_fts_index_compaction(index_name=idx.name, uuid=key)
                break
            _, content = self.rest.get_fts_index_compactions(idx.name)
            compaction_keys_after_cancel = content['status'].keys()
            self.assertFalse(cancel_key in compaction_keys_after_cancel, f"Compaction cancellation is failed. \n"
                                                                         f"Response is: {cancel_response}")

    def _is_compaction_finished(self, idx=None):
        compaction_state = self._get_compaction_status(idx=idx)
        if "In progress" in str(compaction_state):
            return False
        return True

    def _get_compaction_status(self, idx=None):
        _, compactions_state_content = self.rest.get_fts_index_compactions(idx.name)
        return compactions_state_content


    def _call_compaction(self, index_name=None):
        status, content = self.rest.start_fts_index_compaction(index_name)
        self.assertTrue(status, f"FTS index compaction REST call is failed. Response is: \n {content}")
        return status, content

    def _call_mutations(self, bucket_name='default'):
        self._cb_cluster.run_n1ql_query(f"update {bucket_name} set email=email||'.xxx'")

    def _drop_fts_index(self, index=None):
        self._cb_cluster.delete_fts_index(name=index.name)

    def _drop_bucket(self, bucket=None):
        self._cb_cluster.delete_bucket(name=bucket.name)


