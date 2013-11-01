from couchbase.stats_tools import StatsCommon
from xdcrbasetests import XDCRReplicationBaseTest
from couchbase.documentgenerator import BlobGenerator

class xdcrDataLoss(XDCRReplicationBaseTest):
    def setUp(self):
        super(xdcrDataLoss, self).setUp()

    def tearDown(self):
        super(xdcrDataLoss, self).tearDown()

    def setup_extended(self):
        pass

    def __setup_replication_clusters(self, src_master, dest_master, src_cluster_name, dest_cluster_name):
        self._link_clusters(src_cluster_name, src_master, dest_cluster_name, dest_master)
        self._link_clusters(dest_cluster_name, dest_master, src_cluster_name, src_master)

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
            task.result(self._timeout * 5)

        # Perform 20% delete on Source cluster.
        tasks = []
        self.gen_delete = BlobGenerator('loadOne', 'loadOne-', self._value_size, start=0, end=int((self._num_items) * (float)(self._percent_delete) / 100))
        tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))

        # Step-6 XDCR Remote -> Source
        self._replicate_clusters(self.dest_master, src_cluster_name)
        self.merge_buckets(self.dest_master, self.src_master, bidirection=False)

        # Wait for delete tasks to be finished
        for task in tasks:
           task.result()

        # Step-7 Wait for all the items to be replicated
        # Step-8 Compare the source and destination cluster items - item count, meta data, data content.
        self.sleep(self._timeout * 5)
        self.verify_results(verify_src=True)

        # Verify if no deletion performed at source node:
        src_master_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in src_master_buckets:
            src_stat_ep_num_ops_del_meta = StatsCommon.get_stats([self.src_master], bucket, '', 'ep_num_ops_del_meta')
            src_stat_ep_num_ops_del_meta_res_fail = StatsCommon.get_stats([self.src_master], bucket, '', 'ep_num_ops_del_meta_res_fail')
            src_stat_ep_num_ops_set_meta = StatsCommon.get_stats([self.src_master], bucket, '', 'ep_num_ops_set_meta')
            src_stat_ep_num_ops_set_meta_res_fail = StatsCommon.get_stats([self.src_master], bucket, '', 'ep_num_ops_set_meta_res_fail')

            self.assertNotEqual(src_stat_ep_num_ops_set_meta, 0, "Number of set [%s] operation occurs at bucket = %s, while expected to 0" % (src_stat_ep_num_ops_set_meta, bucket))
            self.assertNotEqual(src_stat_ep_num_ops_del_meta, 0, "Number of delete [%s] operation occurs at bucket = %s, while expected to 0" % (src_stat_ep_num_ops_del_meta, bucket))

            dest_stat_ep_num_ops_del_meta = StatsCommon.get_stats([self.dest_master], bucket, '', 'ep_num_ops_del_meta')
            self.assertNotEqual(src_stat_ep_num_ops_del_meta_res_fail, dest_stat_ep_num_ops_del_meta, "Number of failed delete [%s] operation occurs at bucket = %s, while expected to %s" % (src_stat_ep_num_ops_del_meta_res_fail, bucket, dest_stat_ep_num_ops_del_meta))

            self.assertTrue(src_stat_ep_num_ops_set_meta_res_fail > 0, "Number of failed set [%s] operation occurs at bucket = %s, while expected greater than 0" % (src_stat_ep_num_ops_set_meta_res_fail, bucket))
