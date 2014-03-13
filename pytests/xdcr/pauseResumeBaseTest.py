from couchbase.documentgenerator import BlobGenerator
from xdcrbasetests import XDCRReplicationBaseTest
from membase.api.rest_client import RestConnection
from membase.api.exception import XDCRException


class PauseResumeBaseTest(XDCRReplicationBaseTest):
    def setUp(self):
        super(PauseResumeBaseTest, self).setUp()
        self.gen_create2 = BlobGenerator('loadTwo', 'loadTwo',
                                         self._value_size, end=self._num_items)
        self.gen_delete2 = BlobGenerator('loadTwo', 'loadTwo-',
                                         self._value_size,
                                         start=int((self._num_items) *
                                                   (float)(100 - self._percent_delete) / 100),
                                         end=self._num_items)
        self.gen_update2 = BlobGenerator('loadTwo', 'loadTwo-',
                                         self._value_size, start=0,
                                         end=int(self._num_items *
                                                 (float)(self._percent_update) / 100))

        self.pause_xdcr_cluster = self._input.param("pause_xdcr", "both")

    def tearDown(self):
        super(PauseResumeBaseTest, self).tearDown()

    def pause_all_replication(self, master, verify=False, fail_expected=False):
        buckets = self._get_cluster_buckets(master)
        for bucket in buckets:
            try:
                src_bucket_name = dest_bucket_name = bucket.name
                if not RestConnection(self.src_master).is_replication_paused(src_bucket_name, dest_bucket_name):
                    self.pause_replication(self.src_master, src_bucket_name, dest_bucket_name, verify)
            except XDCRException, ex:
                if not fail_expected:
                    raise
                print ex

    def resume_all_replication(self, master, verify=False, fail_expected=False):
        buckets = self._get_cluster_buckets(master)
        for bucket in buckets:
            try:
                src_bucket_name = dest_bucket_name = bucket.name
                if RestConnection(self.src_master).is_replication_paused(src_bucket_name, dest_bucket_name):
                    self.resume_replication(self.src_master, src_bucket_name, dest_bucket_name, verify)
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
        self.log.info("Pausing xdcr on node:{0}, src_bucket:{1} and dest_bucket:{2}"
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
        self.log.info("Resume xdcr on node:{0}, src_bucket:{1} and dest_bucket:{2}"
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

        if self.is_cluster_replicating(src_nodes, src_bucket_name):
            # check active_vbreps on all source nodes
            task = self.cluster.async_wait_for_xdcr_stat(src_nodes,
                                                         src_bucket_name, '',
                                                         'replication_active_vbreps', '>', 0)
            task.result(self._timeout)
            # check incoming xdc_ops on remote nodes
            task = self.cluster.async_wait_for_xdcr_stat(dest_nodes,
                                                         dest_bucket_name, '',
                                                         'xdc_ops', '>', 0)
            task.result(self._timeout)
        else:
            self.log.info("Replication is complete on {0}, resume validation have been skipped".format(src_nodes[0].ip))

    """ Check replication_changes_left on every node, 3 times if 0 """
    def is_cluster_replicating(self, src_nodes, src_bucket_name):
        count = 0
        for node in src_nodes:
            while count < 3:
                if self.get_xdcr_stat(node, src_bucket_name, 'replication_changes_left') == 0:
                    count += 1
                    continue
                else:
                    break
            else:
                return False
        return True
