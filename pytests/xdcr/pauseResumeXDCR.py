from couchbase.documentgenerator import BlobGenerator
from xdcrbasetests import XDCRReplicationBaseTest
from membase.api.rest_client import RestConnection
from membase.api.exception import XDCRException
from threading import Thread

""" Class   :   PauseResumeBaseTest
    Parent  :   XDCRReplicationBaseTest
    Methods :   setUp()
                teardown()
                pause_xdcr()
                resume_xdcr()
                pause_all_replications()
                resume_all replications()
                pause_replication()
                resume_replication()
                post_pause_validations()
                post_resume_validations()
    Usage   :   Base class providing API for Pause-Resume XDCR feature. Just derive this class to test with pause and
                resume on XDCR. See example - PauseResumeTest(PauseResumeBaseTest)
    """
class PauseResumeXDCRBaseTest(XDCRReplicationBaseTest):
    def setUp(self):
        super(PauseResumeXDCRBaseTest, self).setUp()
        self.pause_xdcr_cluster = self._input.param("pause", "")
        self.gen_create2 = BlobGenerator('loadTwo', 'loadTwo',
                                         self._value_size, end=self.num_items)
        self.gen_delete2 = BlobGenerator('loadTwo', 'loadTwo-',
                                         self._value_size,
                                         start=int((self.num_items) *
                                                   (float)(100 - self._percent_delete) / 100),
                                         end=self.num_items)
        self.gen_update2 = BlobGenerator('loadTwo', 'loadTwo-',
                                         self._value_size, start=0,
                                         end=int(self.num_items *
                                                 (float)(self._percent_update) / 100))

    def tearDown(self):
        super(PauseResumeXDCRBaseTest, self).tearDown()

    # Method capable of bidirectionally pausing replications
    # based on self.pause_xdcr_cluster
    def pause_xdcr(self, verify=True):
        if "source" in self.pause_xdcr_cluster:
            self.pause_all_replication(self.src_master, verify=verify)
        if "destination" in self.pause_xdcr_cluster:
            self.pause_all_replication(self.dest_master, verify=verify)

    # Method capable of bidirectionally resuming replications
    # based on self.pause_xdcr_cluster
    def resume_xdcr(self, verify=True):
        if "source" in self.pause_xdcr_cluster:
            self.resume_all_replication(self.src_master, verify=verify)
        if "destination" in self.pause_xdcr_cluster:
            self.resume_all_replication(self.dest_master, verify=verify)

    # Pauses all replication from a cluster, uni-directionally
    def pause_all_replication(self, master, verify=False, fail_expected=False):
        buckets = self._get_cluster_buckets(master)
        for bucket in buckets:
            try:
                src_bucket_name = dest_bucket_name = bucket.name
                if not RestConnection(master).is_replication_paused(src_bucket_name, dest_bucket_name):
                    self.pause_replication(master, src_bucket_name, dest_bucket_name, verify)
            except XDCRException, ex:
                if not fail_expected:
                    raise
                print ex

    # Resumes all replication from a cluster, uni-directionally
    def resume_all_replication(self, master, verify=False, fail_expected=False):
        buckets = self._get_cluster_buckets(master)
        for bucket in buckets:
            try:
                src_bucket_name = dest_bucket_name = bucket.name
                if RestConnection(master).is_replication_paused(src_bucket_name, dest_bucket_name):
                    self.resume_replication(master, src_bucket_name, dest_bucket_name, verify)
            except XDCRException, ex:
                if not fail_expected:
                    raise
                print ex

    """ Pauses replication between src_bucket and dest_bucket
        Argument 'master' indicates if the bucket whose outbound
        replication is paused resides on 'source' or 'destination' cluster
        If C1 <--> C2 and C1.B1 -> C2.B1 is to be paused
        C1 is source, C2 is destination
    """
    def pause_replication(self, master, src_bucket_name, dest_bucket_name,
                          verify=False):
        self.log.info("##### Pausing xdcr on node:{0}, src_bucket:{1} and dest_bucket:{2} #####"
                      .format(master.ip, src_bucket_name, dest_bucket_name))

        RestConnection(master).set_xdcr_param(src_bucket_name,
                                              dest_bucket_name,
                                              'pauseRequested',
                                              'true')
        if verify:
            # check if the replication has really been paused
            self.post_pause_validations(master, src_bucket_name, dest_bucket_name)

    """ Resumes replication between src_bucket and dest_bucket
        Argument 'master' indicates if the bucket whose outbound
        replication is to be resumed resides on 'source' or 'destination' cluster
        If C1 <--> C2 and C1.B1 -> C2.B1 is to be resumed
        C1 is source, C2 is destination
    """
    def resume_replication(self, master, src_bucket_name, dest_bucket_name,
                           verify=False):
        self.log.info("##### Resume xdcr on node:{0}, src_bucket:{1} and dest_bucket:{2} #####"
                      .format(master.ip, src_bucket_name, dest_bucket_name))
        RestConnection(master).set_xdcr_param(src_bucket_name,
                                              dest_bucket_name,
                                              'pauseRequested',
                                              'false')
        if verify:
            # check if the replication has really been resumed
            self.post_resume_validations(master, src_bucket_name, dest_bucket_name)

    """ Post pause validations include -
        1. incoming replication on remote (paired) bucket falling to 0
        2. XDCR stat active_vbreps =0 on all nodes
        3. XDCR queue = 0 on all nodes for the bucket with xdcr paused
        Note : This method should be called immediately after a call to pause xdcr
        and the validation is on the replication paused
        To perform pause validation at both local and remote clusters,
        call this method specifying 'source' and 'destination' for cluster variable
        If C1(B1) <--> C2(B1),
        post_pause_validations('source', src_bucket_name, dest_bucket_name)
        is called to validate pause on C1.B1 -> C2.B1
        post_pause_validations('destination', src_bucket_name, dest_bucket_name)
        is called to validate pause on C2.B1 -> C1.B1
    """
    def post_pause_validations(self, master, src_bucket_name, dest_bucket_name):
        # if the validation is for source cluster
        if master == self.src_master:
            dest_nodes = self.dest_nodes
            src_nodes = self.src_nodes
        # if validation is for destination cluster, swap the 'src' and 'dest' orientation
        # because the validation code below is for 'src' cluster
        else:
            dest_nodes = self.src_nodes
            src_nodes = self.dest_nodes

        # Is bucket replication paused?
        if not RestConnection(master).is_replication_paused(src_bucket_name,
                                                            dest_bucket_name):
            raise XDCRException("XDCR is not paused for SrcBucket: {0}, Target Bucket: {1}".
                           format(src_bucket_name, dest_bucket_name))

        tasks = []
        # incoming ops on remote cluster = 0
        tasks.append(self.cluster.async_wait_for_xdcr_stat(dest_nodes,
                                                       dest_bucket_name, '',
                                                       'xdc_ops', '==', 0))
        # Docs in replication queue at source = 0
        tasks.append(self.cluster.async_wait_for_xdcr_stat(src_nodes,
                                                       src_bucket_name, '',
                                                       'replication_docs_rep_queue',
                                                       '==', 0))
        # active_vbreps falls to 0
        tasks.append(self.cluster.async_wait_for_xdcr_stat(src_nodes,
                                                       src_bucket_name, '',
                                                       'replication_active_vbreps',
                                                       '==', 0))
        for task in tasks:
            task.result()

    """ Post pause validations include -
        1. number of active_vbreps after resume equal the setting value
        2. XDCR queue != 0 on all nodes for the bucket with xdcr resumed
        Note : This method should be called immediately after a call to resume xdcr
        and the validation is only on the bucket whose replication is paused
    """
    def post_resume_validations(self, master, src_bucket_name, dest_bucket_name):
        # if the validation is for source cluster
        if master == self.src_master:
            dest_nodes = self.dest_nodes
            src_nodes = self.src_nodes
        else:
            dest_nodes = self.src_nodes
            src_nodes = self.dest_nodes

        rest_conn = RestConnection(master)
        if rest_conn.is_replication_paused(src_bucket_name, dest_bucket_name):
            raise XDCRException("Replication is not resumed for SrcBucket: {0}, Target Bucket: {1}".
                                format(src_bucket_name, dest_bucket_name))

        if self.is_cluster_replicating(master, src_bucket_name):
            # check active_vbreps on all source nodes
            task = self.cluster.async_wait_for_xdcr_stat(src_nodes,
                                                         src_bucket_name, '',
                                                         'replication_active_vbreps', '>=', 0)
            task.result(self.wait_timeout)
            # check incoming xdc_ops on remote nodes
            if self.is_cluster_replicating(master, src_bucket_name):
                task = self.cluster.async_wait_for_xdcr_stat(dest_nodes,
                                                         dest_bucket_name, '',
                                                         'xdc_ops', '>=', 0)
                task.result(self.wait_timeout)
        else:
            self.log.info("Replication is complete on {0}, resume validations have been skipped".
                          format(src_nodes[0].ip))

    """ Check replication_changes_left on master, 3 times if 0 """
    def is_cluster_replicating(self, master, src_bucket_name):
        count = 0
        while count < 3:
            outbound_mutations = self.get_xdcr_stat(master, src_bucket_name, 'replication_changes_left')
            if outbound_mutations == 0:
                self.log.info("Outbound mutations on {0} is {1}".format(master.ip, outbound_mutations))
                count += 1
                continue
            else:
                self.log.info("Outbound mutations on {0} is {1}".format(master.ip, outbound_mutations))
                self.log.info("Node {0} is replicating".format(master.ip))
                return True
        else:
            self.log.info("Outbound mutations on {0} is {1}".format(master.ip, outbound_mutations))
            self.log.info("Cluster with node {0} is not replicating".format(master.ip))
            return False


class PauseResumeTest(PauseResumeXDCRBaseTest):
    def setUp(self):
        super(PauseResumeTest, self).setUp()
        self.consecutive_pause_resume = int(self._input.param("consecutive_pause_resume", 1))
        self.delete_bucket = self._input.param("delete_bucket", "")
        self.reboot = self._input.param("reboot", "")
        self.pause_wait = self._input.param("pause_wait", 5)
        self.rebalance_in = self._input.param("rebalance_in", "")
        self.rebalance_out = self._input.param("rebalance_out", "")
        self.swap_rebalance = self._input.param("swap_rebalance", "")
        self._num_rebalance = self._input.param("num_rebalance", 1)
        self._failover = self._input.param("failover", "")
        self.encrypt_after_pause = self._input.param("encrypt_after_pause", "")
        self.verify_src = False

    def tearDown(self):
        super(PauseResumeTest, self).tearDown()


    def __async_load_xdcr(self):
        load_tasks = self._async_load_all_buckets(self.src_master, self.gen_create, "create", 0)
        # if this is not a bidirectional replication or
        # we plan to delete dest bucket which might result in
        # uni-directional replication in the middle of the test
        if self._replication_direction_str in "bidirection" and \
           self.delete_bucket != "destination":
            load_tasks += self._async_load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        #load for 20 secs before pause
        self.sleep(20)
        return load_tasks

    def __update_deletes(self):
        if self._replication_direction_str in "unidirection":
            self._async_modify_data()
        elif self._replication_direction_str in "bidirection" and \
             self.delete_bucket != "destination":
            self._async_update_delete_data()
        self.sleep(self.wait_timeout / 2)

    def __merge_buckets(self):
        if self._replication_direction_str in "unidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        elif self._replication_direction_str in "bidirection" and \
             self.delete_bucket != "destination":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
            self.verify_src = True

    # Test with pause and resume
    def replication_with_pause_and_resume(self):
        count = 0
        #start loading
        load_tasks = self.__async_load_xdcr()
        tasks = []
        #are we doing consecutive pause/resume
        while count < self.consecutive_pause_resume:

            self.pause_xdcr()
            if count < 1:

                # rebalance-in?
                if self.rebalance_in != "":
                    self._rebalance = self.rebalance_in
                    tasks += self._async_rebalance_in()

                # rebalance-out/failover
                if self.rebalance_out != "" or self._failover != "":
                    self._rebalance = self.rebalance_out
                    tasks += self._async_rebalance_out()

                 # swap rebalance?
                if self.swap_rebalance != "":
                    self._rebalance = self.swap_rebalance
                    tasks += self._async_swap_rebalance()

                if self.encrypt_after_pause != "":
                    self._switch_to_encryption(self.encrypt_after_pause)

                # delete all destination buckets and recreate them?
                if self.delete_bucket == 'destination':
                    dest_buckets = self._get_cluster_buckets(self.dest_master)
                    for bucket in dest_buckets:
                        RestConnection(self.dest_master).delete_bucket(bucket.name)
                        # Avoid ValueError
                        self.buckets.remove(bucket)
                    self._create_buckets(self.dest_nodes)

                # reboot nodes?
                if self.reboot == "dest_node":
                    self.reboot_node(self.dest_nodes[len(self.dest_nodes) - 1])
                elif self.reboot == "dest_cluster":
                    threads = []
                    for node in self.dest_nodes:
                        threads.append(Thread(target=self.reboot_node, args=(node,)))
                    for thread in threads:
                        thread.start()
                    for thread in threads:
                        thread.join()

            self.sleep(self.pause_wait)

            # resume all bidirectional replications
            self.resume_xdcr()
            count += 1

        # wait for rebalance to complete
        for task in tasks:
            self.log.info("Waiting for rebalance to complete...")
            task.result()

        # wait for load to complete
        for task in load_tasks:
            self.log.info("Waiting for loading to complete...")
            task.result()

        self.__update_deletes()
        self.__merge_buckets()
        self.verify_results()

    def view_query_pause_resume(self):

        load_tasks = self.__async_load_xdcr()

        dest_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in dest_buckets:
            views = self.make_default_views(bucket.name, self._num_views,
                                            self._is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[self._is_dev_ddoc]

        query = {"full_set": "true", "stale": "false"}
        tasks = self.async_create_views(self.dest_master, ddoc_name, views,
                                         self.default_bucket_name)

        [task.result(self._poll_timeout) for task in tasks]
        # Wait for load data to finish if asynchronous
        [load_task.result() for load_task in load_tasks]

        # Resume the XDCR's paused
        self.resume_xdcr()

        self.__merge_buckets()
        self._verify_item_count(self.src_master, self.src_nodes)
        self._verify_item_count(self.dest_master, self.dest_nodes)
        tasks = []
        for view in views:
            tasks.append(self.cluster.async_query_view(self.dest_master,
                                                       prefix + ddoc_name,
                                                       view.name, query,
                                                       dest_buckets[0].kvs[1].__len__()))

        [task.result(self._poll_timeout) for task in tasks]
        self.verify_results()

    def pause_resume_single_bucket(self):
        pause_bucket_name = self._input.param("pause_bucket","default")
        load_tasks = self.__async_load_xdcr()

        self.pause_replication(self.src_master,pause_bucket_name,pause_bucket_name)
        # wait till replication is paused
        self.sleep(10)
        # check if remote cluster is still replicating
        if self.is_cluster_replicating(self.dest_master, pause_bucket_name):
            task = self.cluster.async_wait_for_xdcr_stat(self.src_nodes,
                                                         pause_bucket_name, '',
                                                         'xdc_ops', '>', 0)
            task.result()
            self.log.info("Inbound mutations for {0} are not affected".format(pause_bucket_name))
        # check if pause on one bucket does not affect other replications
        src_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in src_buckets:
            if bucket.name != pause_bucket_name:
                if self.is_cluster_replicating(self.src_master, bucket.name):
                    task = self.cluster.async_wait_for_xdcr_stat(self.dest_nodes,
                                                         bucket.name, '',
                                                         'xdc_ops', '>', 0)
                    task.result()
                    self.log.info("Pausing one replication does not affect other replications")
                else:
                    self.log.info("Other buckets have completed replication")
        self.resume_replication(self.src_master,pause_bucket_name,pause_bucket_name)
        [task.result() for task in load_tasks]
        self.__merge_buckets()
        self.verify_results()

