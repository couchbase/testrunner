from couchbase.stats_tools import StatsCommon
from membase.api.rest_client import RestConnection
from couchbase.documentgenerator import BlobGenerator

from xdcrbasetests import XDCRReplicationBaseTest
from xdcrbasetests import XDCRConstants


class XdcrMiscTests(XDCRReplicationBaseTest):
    def setUp(self):
        super(XdcrMiscTests, self).setUp()

    def tearDown(self):
        super(XdcrMiscTests, self).tearDown()

    def setup_extended(self):
        pass

    def __setup_replication_clusters(self, src_master, dest_master, src_cluster_name, dest_cluster_name):
        self._link_clusters(src_master, dest_cluster_name, dest_master)
        self._link_clusters(dest_master, src_cluster_name, src_master)

    def test_verify_mb8825(self):
        # Setting up replication clusters.
        src_cluster_name, dest_cluster_name = "remote-dest-src", "remote-src-dest"
        self.__setup_replication_clusters(self.src_master, self.dest_master, src_cluster_name, dest_cluster_name)

        # Step-3 Load 10k items ( sets=80, deletes=20) on source cluster.
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        # Step-4 XDCR Source -> Remote
        self._replicate_clusters(self.src_master, dest_cluster_name)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        # Step-5 Wait for replication to finish 50% at destination node
        expected_items = (self.gen_create.end) * 0.5
        dest_master_buckets = self._get_cluster_buckets(self.dest_master)

        tasks = []
        for bucket in dest_master_buckets:
            tasks.append(self.cluster.async_wait_for_stats([self.dest_master], bucket, '', 'curr_items', '>=', expected_items))
        for task in tasks:
            task.result(self.wait_timeout * 5)

        # Perform 20% delete on Source cluster.
        tasks = []
        self.gen_delete = BlobGenerator('loadOne', 'loadOne-', self._value_size, start=0, end=int((self.num_items) * (float)(self._percent_delete) / 100))
        tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))

        # Step-6 XDCR Remote -> Source
        self._replicate_clusters(self.dest_master, src_cluster_name)
        self.merge_buckets(self.dest_master, self.src_master, bidirection=False)

        # Wait for delete tasks to be finished
        for task in tasks:
            task.result()

        # Step-8 Compare the source and destination cluster items - item count, meta data, data content.
        self.verify_results()

        # Verify if no deletion performed at source node:
        src_master_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in src_master_buckets:
            src_stat_ep_num_ops_del_meta = 0
            src_stat_ep_num_ops_set_meta = 0
            src_stat_ep_num_ops_get_meta = 0
            src_stat_ep_num_ops_del_meta_res_fail = 0
            src_stat_ep_num_ops_set_meta_res_fail = 0
            for src_node in self.src_nodes:
                src_stat_ep_num_ops_del_meta += int(StatsCommon.get_stats([src_node], bucket, '', 'ep_num_ops_del_meta')[src_node])
                src_stat_ep_num_ops_set_meta += int(StatsCommon.get_stats([src_node], bucket, '', 'ep_num_ops_set_meta')[src_node])
                src_stat_ep_num_ops_get_meta += int(StatsCommon.get_stats([src_node], bucket, '', 'ep_num_ops_get_meta')[src_node])
                src_stat_ep_num_ops_del_meta_res_fail += int(StatsCommon.get_stats([src_node], bucket, '', 'ep_num_ops_del_meta_res_fail')[src_node])
                src_stat_ep_num_ops_set_meta_res_fail += int(StatsCommon.get_stats([src_node], bucket, '', 'ep_num_ops_set_meta_res_fail')[src_node])

            self.assertEqual(src_stat_ep_num_ops_set_meta, 0, "Number of set [%s] operation occurs at bucket = %s, while expected to 0" % (src_stat_ep_num_ops_set_meta, bucket))
            self.assertEqual(src_stat_ep_num_ops_del_meta, 0, "Number of delete [%s] operation occurs at bucket = %s, while expected to 0" % (src_stat_ep_num_ops_del_meta, bucket))

            dest_stat_ep_num_ops_del_meta = 0
            for dest_node in self.dest_nodes:
                dest_stat_ep_num_ops_del_meta += int(StatsCommon.get_stats([dest_node], bucket, '', 'ep_num_ops_del_meta')[dest_node])

            if self.rep_type == "xmem":
                self.assertEqual(src_stat_ep_num_ops_del_meta_res_fail, dest_stat_ep_num_ops_del_meta, "Number of failed delete [%s] operation occurs at bucket = %s, while expected to %s" % (src_stat_ep_num_ops_del_meta_res_fail, bucket, dest_stat_ep_num_ops_del_meta))
                self.assertTrue(src_stat_ep_num_ops_set_meta_res_fail > 0, "Number of failed set [%s] operation occurs at bucket = %s, while expected greater than 0" % (src_stat_ep_num_ops_set_meta_res_fail, bucket))

            elif self.rep_type == "capi":
                self.assertTrue(src_stat_ep_num_ops_get_meta > 0, "Number of get [%s] operation occurs at bucket = %s, while expected greater than 0" % (src_stat_ep_num_ops_get_meta, bucket))

    def test_diff_version_xdcr(self):
        self.gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self.num_items)
        self.gen_delete2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size,
            start=int((self.num_items) * (float)(100 - self._percent_delete) / 100), end=self.num_items)
        self.gen_update2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, start=0,
            end=int(self.num_items * (float)(self._percent_update) / 100))

        # Step-2 Setting up replication clusters.
        src_cluster_name, dest_cluster_name = "remote-dest-src", "remote-src-dest"
        self.__setup_replication_clusters(self.src_master, self.dest_master, src_cluster_name, dest_cluster_name)

        self.rep_type = "capi"
        self._replicate_clusters(self.src_master, dest_cluster_name)

        self.rep_type = "xmem"
        self._replicate_clusters(self.dest_master, src_cluster_name)

        # Step-3 Load 10k items ( sets=80, deletes=20) on source cluster.
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)
        self._async_update_delete_data()
        # Step-4 XDCR Source -> Remote
        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        # Step-5 verify data
        self.sleep(120)
        self.verify_results()

    def test_xdcr_within_same_cluster(self):
        remote_cluster_name = "same-cluster"
        self._link_clusters(self.src_master, remote_cluster_name, self.src_master)

        buckets = self._get_cluster_buckets(self.src_master)
        self.assertTrue(len(buckets) >= 2, "Number of buckets required for this is greater than 1 on source cluster")
        src_bucket, dest_bucket = buckets[0], buckets[1]

        self._load_bucket(src_bucket, self.src_master, self.gen_create, "create", 0)

        # Step-4 XDCR Source -> Remote
        rest_conn_src = RestConnection(self.src_master)
        rest_conn_src.start_replication(XDCRConstants.REPLICATION_TYPE_CONTINUOUS,
                                        src_bucket.name, remote_cluster_name,
                                        self.rep_type, toBucket=dest_bucket.name)

        dest_bucket.kvs[1] = src_bucket.kvs[1]

        self._verify_item_count(self.src_master, self.src_nodes)
        self._verify_data_all_buckets(self.src_master)
