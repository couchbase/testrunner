import ntplib

from couchbase_helper.documentgenerator import BlobGenerator

from .xdcrnewbasetests import XDCRNewBaseTest
from .xdcrnewbasetests import NodeHelper
from .xdcrnewbasetests import Utility, BUCKET_NAME, OPS


# Assumption that at least 2 nodes on every cluster
# https://docs.google.com/a/globallogic.com/spreadsheets/d/1Z_Dn04xLQS5ruoqsS0Ab7w6TJJAUPL-EB7Gj4BF1y_M/edit?pli=1#gid=1082690422
class LwwXDCR(XDCRNewBaseTest):

    def setUp(self):
        super(LwwXDCR, self).setUp()
        self.c1_cluster = self.get_cb_cluster_by_name('C1')
        self.c2_cluster = self.get_cb_cluster_by_name('C2')
        self.__time_sync_enabled = self._input.param(
            "time_sync_enabled",
            "").split('-')
        # Specify delay in minutes between cluster.
        # E.g. C1-60-C2 -> C1 is 60 minutes faster than C2.
        self.__clocks = self._input.param(
            "clocks",
            "0:0").split(":")

        self.__setup_clocks_on_clusters()

    def __setup_wall_clocks(self):
        c = ntplib.NTPClient()
        date_str = c.request("0.in.pool.ntp.org", version=3, timeout=10)
        for i, time_diff in enumerate(self.__clocks):
            cb_cluster = self.get_cb_cluster_by_name("C" + str(i))
            set_time = date_str.tx_time + float(time_diff)
            cb_cluster.set_wall_clock_time(set_time)
            # Restart Couchbase server.
            cb_cluster.restart_couchbase_on_all_nodes()

    def __enable_time_sync(self):
        self.c1_cluster.enable_time_sync(
            enabled=(
                "C1" in self.__time_sync_enabled))

        self.c2_cluster.enable_time_sync(
            enabled=(
                "C2" in self.__time_sync_enabled))

    def __setup_clocks_on_clusters(self):
        self.__enable_time_sync()
        self.__setup_wall_clocks()

    def __verify_keys_on_cluster(self, keys, cluster):
        pass

    def __wait_for_replication_to_finish(self):
        self.merge_all_buckets()
        for cb_cluster in self.get_cb_clusters():
            for remote_cluster_ref in cb_cluster.get_remote_clusters():
                src_cluster = remote_cluster_ref.get_src_cluster()
                dest_cluster = remote_cluster_ref.get_dest_cluster()

                src_cluster.wait_for_flusher_empty()
                dest_cluster.wait_for_flusher_empty()

                src_cluster.wait_for_outbound_mutations()
                dest_cluster.wait_for_outbound_mutations()

                src_cluster.verify_items_count()
                dest_cluster.verify_items_count()

    # Row-27
    def test_seq_upd_on_uni_xdcr_all_enabled(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()

        gen = BlobGenerator("C-new-", "C2-new-", self._value_size, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)
        gen = BlobGenerator("C-new-", "C1-new-", self._value_size, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications()

        self.verify_results()

    # Row-28
    def test_seq_upd_on_bi_xdcr_all_enabled(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()
        self.c2_cluster.pause_all_replications()

        gen = BlobGenerator("C-new-", "C1-new-", self._value_size, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        # to add some delay in timestamp
        self.sleep(2)
        gen = BlobGenerator("C-new-", "C2-new-", self._value_size, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications()
        self.c2_cluster.resume_all_replications()

        self.verify_results()

    # Row-29
    def test_seq_add_del_on_bi_xdcr_all_enabled(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()
        self.c2_cluster.pause_all_replications()

        kv_gen = self.c1_cluster.get_kv_gen()[OPS.CREATE]
        new_kv_gen = BlobGenerator(
            kv_gen.name,
            'C-New-Update',
            self._value_size,
            start=0,
            end=1)

        self.c2_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.DELETE)
        # to add some delay in timestamp
        self.sleep(2)
        self.c1_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.UPDATE)

        self.c1_cluster.resume_all_replications()
        self.c2_cluster.resume_all_replications()

        self.verify_results()

    # Row-30
    def test_seq_upd_on_uni_xdcr_dest_disabled(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()

        gen = BlobGenerator("C-new-", "C2-new-", self._value_size, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)
        gen = BlobGenerator("C-new-", "C1-new-", self._value_size, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications()

        # TODO add excepton in key not present on C1/C2.
        self.verify_results()

    # Row-31
    def test_seq_upd_on_uni_xdcr_src_disabled(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()

        gen = BlobGenerator("C-new-", "C1-new-", self._value_size, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        gen = BlobGenerator("C-new-", "C2-new-", self._value_size, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications()

        self.verify_results()

    # Row-32
    def test_seq_upd_on_bi_xdcr_all_disabled(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()
        self.c2_cluster.pause_all_replications()

        gen = BlobGenerator("C-new-", "C1-new-", self._value_size, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        gen = BlobGenerator("C-new-", "C2-new-", self._value_size, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications()
        self.c2_cluster.resume_all_replications()

        self.verify_results()

    # Row-33
    def test_parallel_upd_on_bi_xdcr_all_enabled(self):
        raise Exception("No Implemented")
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()
        self.c2_cluster.pause_all_replications()
        # TODO

        self.c1_cluster.resume_all_replications()
        self.c2_cluster.resume_all_replications()

    # Row-34
    def test_seq_add_del_on_bi_xdcr_all_enabled_failover(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()
        self.c2_cluster.pause_all_replications()

        kv_gen = self.c1_cluster.get_kv_gen()[OPS.CREATE]
        new_kv_gen = BlobGenerator(
            kv_gen.name,
            'C-New-Update',
            self._value_size,
            start=0,
            end=1)

        self.c2_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.DELETE)

        self.c2_cluster.failover_and_rebalance_nodes()

        self.c1_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.UPDATE)

        self.c2_cluster.failover_and_rebalance_nodes()
        self.c1_cluster.resume_all_replications()
        self.c2_cluster.resume_all_replications()

        self.verify_results()

    # Row-35
    def test_seq_upd_on_bi_xdcr_all_enabled_C1_delay_1_hr(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()
        self.c2_cluster.pause_all_replications()

        kv_gen = self.c1_cluster.get_kv_gen()[OPS.CREATE]
        new_kv_gen = BlobGenerator(kv_gen.name, 'D2-', self._value_size, end=1)

        self.c2_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.UPDATE)

        # add delay
        self.sleep(2)
        new_kv_gen = BlobGenerator(kv_gen.name, 'D3-', self._value_size, end=1)

        self.c1_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.UPDATE)

        self.c1_cluster.resume_all_replications()
        self.c2_cluster.resume_all_replications()

        self.verify_results()

    # Row-36
    def test_seq_upd_on_uni_xdcr_src_clock_drift(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()

        # Fasten the C2 clock by an hour
        self.__clocks = {'C1': 0, 'C2': 60}
        self.__setup_wall_clocks()

        kv_gen = self.c1_cluster.get_kv_gen()[OPS.CREATE]
        new_kv_gen = BlobGenerator(kv_gen.name, 'D2-', self._value_size, end=1)

        self.c2_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.UPDATE)

        # add delay
        self.sleep(2)
        new_kv_gen = BlobGenerator(kv_gen.name, 'D3-', self._value_size, end=1)

        self.c1_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.UPDATE)

        self.c1_cluster.resume_all_replications()

        self.verify_results()

    # Row-37
    def test_seq_upd_on_bi_xdcr_all_enabled_C2_delay_1_hr(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()
        self.c2_cluster.pause_all_replications()

        gen = BlobGenerator("C-new-", "D1-", self._value_size, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        # add delay in timestamp
        self.sleep(2)
        gen = BlobGenerator("C-new-", "D2-", self._value_size, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications()
        self.c2_cluster.resume_all_replications()

        self.verify_results()

    # Row-38
    def test_seq_del_upd_on_bi_xdcr_all_enabled_C1_delay_1_hr(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()
        self.c2_cluster.pause_all_replications()

        kv_gen = self.c1_cluster.get_kv_gen()[OPS.CREATE]
        new_kv_gen = BlobGenerator(kv_gen.name, 'D2-', self._value_size, end=1)

        self.c2_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.DELETE)

        self.sleep(2)
        new_kv_gen = BlobGenerator(kv_gen.name, 'D3-', self._value_size, end=1)
        self.c1_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.UPDATE)

        self.c1_cluster.resume_all_replications()
        self.c2_cluster.resume_all_replications()

        self.verify_results()

    # Row-39
    def test_seq_upd_del_on_bi_xdcr_all_enabled_C1_delay_1_hr(self):
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications()
        self.c2_cluster.pause_all_replications()

        kv_gen = self.c1_cluster.get_kv_gen()[OPS.CREATE]
        new_kv_gen = BlobGenerator(kv_gen.name, 'D2-', self._value_size, end=1)

        self.c2_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.UPDATE)

        self.sleep(2)
        new_kv_gen = BlobGenerator(kv_gen.name, 'D3-', self._value_size, end=1)
        self.c1_cluster.load_all_buckets_from_generator(
            new_kv_gen,
            ops=OPS.DELETE)

        self.c1_cluster.resume_all_replications()
        self.c2_cluster.resume_all_replications()

        self.verify_results()

    def tearDown(self):
        super(LwwXDCR, self).tearDown()
