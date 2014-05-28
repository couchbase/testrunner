from xdcrbasetests import XDCRReplicationBaseTest, XDCRConstants
from remote.remote_util import RemoteMachineShellConnection
from lib.membase.api.rest_client import RestConnection
from membase.api.exception import XDCRCheckpointException
from mc_bin_client import MemcachedClient, MemcachedError
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached


class XDCRCheckpointUnitTest(XDCRReplicationBaseTest):
    def setUp(self):
        super(XDCRCheckpointUnitTest, self).setUp()
        if not self._default_bucket:
            self.fail("Remove \'default_bucket=false\', these unit tests are designed to run on default bucket")
        self.init()

    def tearDown(self):
        self.log.info("Checkpoints recorded in this run -")
        for record in self.chkpt_records:
            self.log.info(record)
        super(XDCRCheckpointUnitTest, self).tearDown()

    def init(self):
        self.keys_loaded = []
        self.key_counter = 0
        # some keys that will always hash to vb0
        self.vb0_keys = ['pymc1098', 'pymc1108', 'pymc2329', 'pymc4019', 'pymc4189', 'pymc7238','pymc10031', 'pymc10743',
                         'pymc11935', 'pymc13210', 'pymc13380', 'pymc13562', 'pymc14824', 'pymc15120', 'pymc15652',
                         'pymc16291', 'pymc16301', 'pymc16473', 'pymc18254', 'pymc18526']
        self.chkpt_records = []
        self.num_commit_for_chkpt_calls_so_far = 0
        self.num_successful_chkpts_so_far = 0
        self.num_failed_chkpts_so_far = 0
        self.num_pre_replicate_calls_so_far = 0
        self.num_successful_prereps_so_far = 0
        self.num_failed_prereps_so_far = 0
        # since we do not install before every test, discounting already recorded checkpoints, pre-replicates"""
        self.num_commit_for_chkpt_beginning = self.num_successful_chkpts_beginning = self.num_failed_chkpts_beginning = 0
        self.num_pre_replicates_beginning = self.num_successful_prereps_beginning = self.num_failed_prereps_beginning = 0
        self.read_chkpt_history_new_vb0node()

    """ * Call everytime the active vb0 on dest moves *
        We don't install everytime a test is run so it is important to know the checkpoint history on the node.
        Hence we call this in the beginning of every run.
        We determine if checkpointing is successful based on the CAPI posts and return codes at destination from couchdb log
        When the active vb0 moves, we must remember we are accessing new set of logs. To rely on _was_pre_successful()
        and was_checkpointing_successful(), we should get a log snapshot to compare against
    """
    def read_chkpt_history_new_vb0node(self):
        # get these numbers from logs
        node = self.get_active_vb0_node(self.dest_master)
        self.num_commit_for_chkpt_beginning, self.num_successful_chkpts_beginning, self.num_failed_chkpts_beginning = \
            self.get_checkpoint_call_history(node)
        self.num_pre_replicates_beginning, self.num_successful_prereps_beginning,self.num_failed_prereps_beginning = \
            self.get_pre_replicate_call_history(node)
        self.log.info("From previous runs on {} : Num of commit calls : {} ; num of successful commits : {} \
        num of failed commits : {}".format(node.ip, self.num_commit_for_chkpt_beginning, \
        self.num_successful_chkpts_beginning,self.num_failed_chkpts_beginning))
        self.log.info("From previous runs on {} : Num of pre_replicate calls : {} ; num of successful pre_replicates : {} \
        num of failed pre_replicates : {}".format(node.ip,self.num_pre_replicates_beginning, \
                                            self.num_successful_prereps_beginning, self.num_failed_prereps_beginning ))

        self.num_commit_for_chkpt_calls_so_far = self.num_commit_for_chkpt_beginning
        self.num_successful_chkpts_so_far = self.num_successful_chkpts_beginning
        self.num_failed_chkpts_so_far = self.num_failed_chkpts_beginning
        self.num_pre_replicate_calls_so_far = self.num_pre_replicates_beginning
        self.num_successful_prereps_so_far = self.num_successful_prereps_beginning
        self.num_failed_prereps_so_far = self.num_failed_prereps_beginning

    """ Returns node containing active vb0 """
    def get_active_vb0_node(self, master):
        nodes = self.src_nodes
        ip = VBucketAwareMemcached(RestConnection(master),'default').vBucketMap[0].split(':')[0]
        if master == self.dest_master:
            nodes = self.dest_nodes
        for node in nodes:
            if ip == node.ip:
                return node
        raise XDCRCheckpointException("Error determining the node containing active vb0")

    """ Sample XDCR checkpoint record -
      {u'total_docs_checked': 1,                        :
       u'upr_snapshot_end_seqno': 1,                    : UPR snapshot end sequence number
       u'upr_snapshot_seqno': 1,                        : UPR snapshot starting sequence number
       u'seqno': 1,                                     : the sequence number we checkpointed at
       u'start_time': u'Tue, 20 May 2014 22:17:51 GMT', : start time of ep_engine
       u'total_data_replicated': 151,                   : number of bytes replicated to dest
       u'commitopaque': [169224017468010, 2],           : remote failover log
       u'total_docs_written': 1,                        : number of docs replicated to dest
       u'end_time': u'Tue, 20 May 2014 22:18:56 GMT',   : time at checkpointing
       u'failover_uuid': 77928303208376}                : local vb_uuid

        Main method that validates a checkpoint record """
    def get_and_validate_latest_checkpoint(self):
        rest_con = RestConnection(self.src_master)
        try:
            checkpoint_record = rest_con.get_recent_xdcr_vb_ckpt('default', 'default', '0')
            self.log.info("Checkpoint record : {}".format(checkpoint_record))
            self.chkpt_records.append(checkpoint_record)
        except Exception as e:
            raise XDCRCheckpointException("Error retrieving last checkpoint document - {}".format(e))

        commit_opaque = checkpoint_record["commitopaque"]
        failover_uuid = checkpoint_record["failover_uuid"]

        upr_snap_seqno = checkpoint_record["upr_snapshot_seqno"]
        seqno = checkpoint_record["seqno"]
        start_time = checkpoint_record["start_time"]
        total_docs_checked = checkpoint_record["total_docs_checked"]
        total_docs_written = checkpoint_record["total_docs_written"]
        total_data_repl = checkpoint_record["total_data_replicated"]
        end_time = checkpoint_record["end_time"]

        self.log.info ("Verifying commitopaque/remote failover log ...")
        if seqno != 0:
            self.validate_remote_failover_log(commit_opaque[0], commit_opaque[1])
            self.log.info ("Verifying last checkpointed seqno ...")
            self.validate_last_checkpointed_seqno(int(seqno))
            self.log.info ("Verifying local failover uuid ...")
            local_vb_uuid, _ = self.get_failover_log(self.src_master)
            self.assertTrue(int(local_vb_uuid) == int(failover_uuid),
                        "local failover_uuid is wrong in checkpoint record! Expected: {0} seen: {1}".
                        format(local_vb_uuid,failover_uuid))
            self.log.info("Checkpoint record verified")
        else:
            self.log.info("Skipping checkpoint record checks for checkpoint-0")
        return True

    """ Checks if commitopaque in a checkpoint record matches remote failover log """
    def validate_remote_failover_log(self, vb_uuid, high_seqno):
        # TAP based validation
        remote_uuid, remote_highseq = self.get_failover_log(self.dest_master)
        self.log.info("Remote failover log = [{},{}]".format(remote_uuid, remote_highseq))
        if int(remote_uuid) != int(vb_uuid):
            raise XDCRCheckpointException("vb_uuid in commitopaque {} does not match remote vb_uuid {}"
                                          .format(remote_uuid))
        if int(remote_highseq) != int(high_seqno):
            raise XDCRCheckpointException("vb_high_seqno in commitopaque does not match remote vb_high_seqno")

    """ Gets failover log [vb_uuid, high_seqno] from node containing vb0 """
    def get_failover_log(self, master):
        vb0_active_node = self.get_active_vb0_node(master)
        stats = MemcachedClientHelper.direct_client(vb0_active_node, 'default').stats('vbucket-seqno')
        return stats['vb_0:uuid'], stats['vb_0:high_seqno']

    """ Checks if seqno in checkpoint record points to the  """
    def validate_last_checkpointed_seqno(self, last_seqno=0):
        shell = RemoteMachineShellConnection(self.src_master)
        os_type = shell.extract_remote_info().distribution_type
        if os_type.lower() == 'windows':
            logs_dir = "C:/Program Files/Couchbase/Server/var/lib/couchbase/logs/xdcr.*"
        else:
            logs_dir = "/opt/couchbase/var/lib/couchbase/logs/xdcr.*"
        out, error = shell.execute_command("ls -rt  %s | xargs grep \"source start sequence\" | tail -10" % (logs_dir))
        for replicated_seqnum in out:
            self.log.info(replicated_seqnum)
        out, error = shell.execute_command("ls -rt  %s | xargs grep \"source start sequence\" | tail -1" % (logs_dir))
        self.log.info(out)
        seqno = int(out[0][-1])
        self.assertTrue(last_seqno == seqno,"The last checkpointed_seqno from record is %s, while log says %s"
                                            %(last_seqno, seqno))

    """ Gets _commit_for_checkpoint call history recorded so far on a node """
    def get_checkpoint_call_history(self, node):
        shell = RemoteMachineShellConnection(node)
        os_type = shell.extract_remote_info().distribution_type
        if os_type.lower() == 'windows':
            couchdb_log = "C:/Program Files/Couchbase/Server/var/lib/couchbase/logs/couchdb.*"
        else:
            couchdb_log = "/opt/couchbase/var/lib/couchbase/logs/couchdb.*"
        total_chkpt_calls, error = shell.execute_command("grep \"POST /_commit_for_checkpoint\" {} | wc -l"
                                                                     .format(couchdb_log))
        total_successful_chkpts, error = shell.execute_command("grep \"POST /_commit_for_checkpoint 200\" {} | wc -l"
                                                                     .format(couchdb_log))
        self.log.info(int(total_successful_chkpts[0]))
        if self.num_successful_chkpts_so_far != 0:
            checkpoint_number = int(total_successful_chkpts[0]) - self.num_successful_chkpts_beginning
            self.log.info("Checkpoint on this node (this run): {}".format(checkpoint_number))
        shell.disconnect()
        total_commit_failures = int(total_chkpt_calls[0]) - int(total_successful_chkpts[0])
        return int(total_chkpt_calls[0]), int(total_successful_chkpts[0]) , total_commit_failures

    """ Gets total number of pre_replicate responses made from dest, number of
        successful and failed pre_replicate calls so far on the current dest node """
    def get_pre_replicate_call_history(self, node):
        shell = RemoteMachineShellConnection(node)
        os_type = shell.extract_remote_info().distribution_type
        if os_type.lower() == 'windows':
            couchdb_log = "C:/Program Files/Couchbase/Server/var/lib/couchbase/logs/couchdb.*"
        else:
            couchdb_log = "/opt/couchbase/var/lib/couchbase/logs/couchdb.*"
        total_prerep_calls, error = shell.execute_command("grep \"POST /_pre_replicate\" {} | wc -l"
                                                                     .format(couchdb_log))
        total_successful_prereps, error = shell.execute_command("grep \"POST /_pre_replicate 200\" {} | wc -l"
                                                                     .format(couchdb_log))
        shell.disconnect()
        total_prerep_failures = int(total_prerep_calls[0]) - int(total_successful_prereps[0])
        return int(total_prerep_calls[0]), int(total_successful_prereps[0]), total_prerep_failures

    """ From destination couchdb log tells if checkpointing was successful """
    def was_checkpointing_successful(self):
        node = self.get_active_vb0_node(self.dest_master)
        total_commit_calls, success, failures = self.get_checkpoint_call_history(node)
        if success > self.num_successful_chkpts_so_far :
            self.log.info("_commit_for_checkpoint was successful: last recorded success:{} , now :{}".
                          format(self.num_successful_chkpts_so_far, success))
            self.num_successful_chkpts_so_far = success
            return True
        elif failures > self.num_failed_chkpts_so_far:
            self.log.info("_commit_for_checkpoint was NOT successful: last recorded failure :{} , now :{}".
                          format(self.num_failed_chkpts_so_far, failures))
            self.num_failed_chkpts_so_far = failures
        elif total_commit_calls == self.num_commit_for_chkpt_calls_so_far:
            self.log.info("Checkpointing did not happen!")
        return False

    """ Tells if pre-replicate was successful based on source->dest _pre_replicate CAPI posts """
    def was_pre_rep_successful(self):
        node = self.get_active_vb0_node(self.dest_master)
        total_commit_calls, success, failures = self.get_pre_replicate_call_history(node)
        if success > self.num_successful_prereps_so_far :
            self.log.info("_pre_replicate was successful: last recorded success :{} , now :{}".
                          format(self.num_successful_prereps_so_far, success))
            self.num_successful_prereps_so_far = success
            return True
        elif failures > self.num_failed_prereps_so_far:
            self.log.error("_pre_replicate was NOT successful: last recorded failure :{} , now :{}".
                          format(self.num_failed_prereps_so_far, failures))
            self.num_failed_prereps_so_far = failures
        elif total_commit_calls == self.num_pre_replicate_calls_so_far:
            self.log.error("ERROR: Pre-replication did NOT happen!")
        return False

    """ Load one mutation into source node containing active vb0 """
    def load_one_mutation_into_source_vb0(self, vb0_active_src_node):
        key = self.vb0_keys[self.key_counter]
        memc_client = MemcachedClient(vb0_active_src_node.ip, 11210)
        try:
            memc_client.set(key, exp=0, flags=0, val="dummy val")
            self.key_counter += 1
            self.keys_loaded.append(key)
            self.log.info("Loaded key {} onto vb0 in {}".format(key, vb0_active_src_node.ip))
            self.log.info ("deleted, flags, exp, rev_id, cas for key {} = {}".format(key, memc_client.getMeta(key)))
        except MemcachedError as e:
            self.log.error(e)

    """ Initial load, 3 further updates on same key onto vb0
        Note: Checkpointing happens during the second mutation,but only if it's time to checkpoint """
    def mutate_and_checkpoint(self, n=3):
        count = 1
        # get vb0 active source node
        active_src_node = self.get_active_vb0_node(self.src_master)
        while count <=n:
            self.sleep(self._checkpoint_interval + 10)
            self.log.info("################ New mutation:{} ##################".format(self.key_counter+1))
            self.load_one_mutation_into_source_vb0(active_src_node)
            if self.was_checkpointing_successful():
                self.log.info("Validating checkpoint record ...")
                self.get_and_validate_latest_checkpoint()
            else:
                self.log.info("Checkpointing failed - may not be an error if vb_uuid changed ")
                return False
            count += 1
        return True

    """ Rebalance-out active vb0 node from a cluster """
    def rebalance_out_activevb0_node(self, master):
        pre_rebalance_uuid, _ =self.get_failover_log(master)
        self.log.info("Starting rebalance-out ...")
        # find which node contains vb0
        node = self.get_active_vb0_node(master)
        self.log.info("Node {} contains active vb0".format(node))
        if master == self.src_master:
            tasks = self._async_rebalance(self.src_nodes, [], [node])
            for task in tasks:
                task.result()
            if master == node:
                self.src_nodes.remove(self.src_master)
                self.src_master = self.src_nodes[0]
            post_rebalance_uuid, _= self.get_failover_log(self.get_active_vb0_node(self.src_master))
            self.log.info("Remote uuid before rebalance :{}, after rebalance : {}".
                      format(pre_rebalance_uuid, post_rebalance_uuid))
            # source rebalance on tap?
            if RestConnection(self.src_master).get_internal_replication_type() == 'tap':
                self.assertTrue(int(pre_rebalance_uuid) != int(post_rebalance_uuid),
                                "vb_uuid of vb0 is same before and after TAP rebalance")
            else:
                self.log.info("Current internal replication = UPR,hence vb_uuid did not change," \
                          "Subsequent _commit_for_checkpoints are expected to pass")
            self.verify_next_checkpoint_passes()
        else:
            tasks = self._async_rebalance(self.dest_nodes, [], [node])
            for task in tasks:
                task.result()
            if master == node:
                self.dest_nodes.remove(self.dest_master)
                self.dest_master = self.dest_nodes[0]
            post_rebalance_uuid, _= self.get_failover_log(self.get_active_vb0_node(self.dest_master))
            self.log.info("Remote uuid before rebalance :{}, after rebalance : {}".
                      format(pre_rebalance_uuid, post_rebalance_uuid))
            # destination rebalance on tap?
            if RestConnection(self.dest_master).get_internal_replication_type() == 'tap':
                self.assertTrue(int(pre_rebalance_uuid) != int(post_rebalance_uuid),
                                "vb_uuid of vb0 is same before and after TAP rebalance")
                self.read_chkpt_history_new_vb0node()
                self.verify_next_checkpoint_fails_after_dest_uuid_change()
            else:
                self.log.info("Current internal replication = UPR,hence destination vb_uuid did not change," \
                          "Subsequent _commit_for_checkpoints are expected to pass")
                self.verify_next_checkpoint_passes()

    """ Failover active vb0 node from a cluster """
    def failover_activevb0_node(self, master):
        pre_failover_uuid, _ =self.get_failover_log(master)
        self.log.info("Starting failover ...")
        # find which node contains vb0, we will failover that node
        node = self.get_active_vb0_node(master)
        self.log.info("Node {} contains active vb0".format(node))
        if master == self.src_master:
            nodes = self.src_nodes
        else:
            nodes = self.dest_nodes
        tasks = self._async_failover(nodes, [node])
        for task in tasks:
            task.result()
        if "source" in self._failover:
            post_failover_uuid, _= self.get_failover_log(self.get_active_vb0_node(self.src_master))
        else:
            post_failover_uuid, _= self.get_failover_log(self.get_active_vb0_node(self.dest_master))
        self.log.info("Remote uuid before failover :{}, after failover : {}".format(pre_failover_uuid, post_failover_uuid))
        self.assertTrue(int(pre_failover_uuid) != int(post_failover_uuid),"Remote vb_uuid is same before and after failover")

    """ Crash node, check uuid before and after crash """
    def crash_node(self, master):
        count = 0
        pre_crash_uuid, _ = self.get_failover_log(master)
        node = self.get_active_vb0_node(master)
        self.log.info("Crashing node {} containing vb0 ...".format(node))
        shell = RemoteMachineShellConnection(node)
        os_info = shell.extract_remote_info()
        shell.kill_erlang(os_info)
        # If we are killing dest node, try to mutate key at source to cause xdcr activity
        if master == self.dest_master:
            while count < 5:
                self.load_one_mutation_into_source_vb0(self.get_active_vb0_node(self.src_master))
                count += 1
        shell.start_couchbase()
        shell.disconnect()
        self.sleep(10)
        post_crash_uuid, _=self.get_failover_log(master)
        self.log.info("Remote uuid before crash :{}, after crash : {}".format(pre_crash_uuid, post_crash_uuid))
        self.assertTrue(int(pre_crash_uuid) != int(post_crash_uuid),
                        "Remote vb_uuid is same before and after erlang crash - MB-11085 ")

    """Tests dest node(containing vb0) crash"""
    def test_dest_node_crash(self):
        self.mutate_and_checkpoint()
        self.crash_node(self.dest_master)
        self.verify_next_checkpoint_fails_after_dest_uuid_change()
        self.verify_revid()

    """ Tests if pre_replicate and commit_for_checkpoint following source crash is successful"""
    def test_source_node_crash(self):
        self.mutate_and_checkpoint(n=2)
        self.crash_node(self.src_master)
        if self.was_pre_rep_successful():
            self.log.info("_pre_replicate following the source crash was successful: {}".
                          format(self.num_successful_prereps_so_far))
            if self.mutate_and_checkpoint(n=1):
                self.log.info("Checkpointing resumed normally after source crash")
            else:
                self.fail("Checkpointing failed once again after the last uuid change")
        else:
            self.fail("ERROR: _pre_replicate following source crash was unsuccessful")
        self.verify_revid()

    """ Tests if vb_uuid changes after bucket flush, subsequent checkpoint fails indicating that and
        next checkpoint is successful"""
    def test_dest_bucket_flush(self):
        self.mutate_and_checkpoint()
        self.cluster.async_bucket_flush(self.dest_master, 'default')
        self.verify_next_checkpoint_fails_after_dest_uuid_change()
        self.verify_revid()

    """ Tests if vb_uuid at destination changes, next checkpoint fails and then recovers eventually """
    def test_dest_bucket_delete_recreate(self):
        self.mutate_and_checkpoint()
        self.cluster.bucket_delete(self.dest_master, 'default')
        self._create_buckets(self.dest_nodes)
        self.verify_next_checkpoint_fails_after_dest_uuid_change()
        self.verify_revid()

    """ Checks if _pre_replicate and _commit_for_checkpoint are successful after source bucket recreate """
    def test_source_bucket_delete_recreate(self):
        self.mutate_and_checkpoint(n=2)
        self.cluster.bucket_delete(self.src_master, 'default')
        self._create_buckets(self.src_nodes)
        dest_cluster_name = self._get_cluster_names()[1]
        RestConnection(self.src_master).start_replication(XDCRConstants.REPLICATION_TYPE_CONTINUOUS,
                                                          'default', dest_cluster_name, self.rep_type)
        self.sleep(5)
        self.key_counter = 0
        self.keys_loaded = []
        if self.was_pre_rep_successful():
            self.log.info("_pre_replicate following the source bucket recreate was successful: {}".
                          format(self.num_successful_prereps_so_far))
            self.verify_next_checkpoint_passes()
        else:
            self.fail("ERROR: _pre_replicate following source bucket recreate was unsuccessful")
        self.verify_revid()

    """ Test rebalance-out of vb0 node at source/destination and checkpointing behavior """
    def test_rebalance(self):
        self.mutate_and_checkpoint(n=2)
        if "destination" in self._rebalance:
            self.rebalance_out_activevb0_node(self.dest_master)
        elif "source" in self._rebalance :
            self.rebalance_out_activevb0_node(self.src_master)
        self.verify_revid()

    """ Test failover of vb0 node at source/destination and checkpointing behavior """
    def test_failover(self):
        self.mutate_and_checkpoint(n=2)
        if "destination" in self._failover:
            self.failover_activevb0_node(self.dest_master)
            self.read_chkpt_history_new_vb0node()
            self.verify_next_checkpoint_fails_after_dest_uuid_change()
        elif "source" in self._failover:
            self.failover_activevb0_node(self.src_master)
            self.verify_next_checkpoint_passes()
        self.verify_revid()


    """ Checks if the subsequent _commit_for_checkpoint and _pre_replicate
        fail after a dest vb_uuid change. Also checks if the next checkpoint
        call is successful
    """
    def verify_next_checkpoint_fails_after_dest_uuid_change(self):
        if not self.mutate_and_checkpoint(n=1):
            self.log.info ("Checkpointing failed as expected after remote uuid change, not a bug")
            if not self.was_pre_rep_successful():
                self.log.info("_pre_replicate following the failed checkpoint was unsuccessful, but this is expected")
                self.verify_next_checkpoint_passes()
            else:
                self.fail("ERROR :_pre_replicate following the failed checkpoint was successful")
        else:
            self.fail("Checkpointing passed unexpectedly, despite remote_uuid change following most recent crash/topology change ")

    """ Checks if the subsequent _commit_for_checkpoint and _pre_replicate pass
        happens if dest vb_uuid did not change or only source uuid changed
    """
    def verify_next_checkpoint_passes(self):
        if self.mutate_and_checkpoint(n=1):
            self.log.info ("Checkpointing was successful")
        else:
            self.fail("Checkpointing failed unexpectedly")

    """ Checks revIDs of loaded keys and logs missing keys """
    def verify_revid(self):
        missing_keys = False
        src_node = self.get_active_vb0_node(self.src_master)
        dest_node = self.get_active_vb0_node(self.dest_master)
        src_client = MemcachedClient(src_node.ip, 11210)
        dest_client = MemcachedClient(dest_node.ip, 11210)
        for key in self.keys_loaded:
            src_meta = src_client.getMeta(key)
            dest_meta = dest_client.getMeta(key)
            try:
                self.log.info("deleted, flags, exp, rev_id, cas for key from Source({0}) {1} = {2}"
                               .format(src_node.ip, key, src_meta))
                self.log.info("deleted, flags, exp, rev_id, cas for key from Destination({0}) {1} = {2}"
                               .format(dest_node.ip, key, dest_meta))
                if src_meta == dest_meta:
                    self.log.info("RevID verification successful for key {}".format(key))
                else:
                    self.fail("RevID verification failed for key {}".format(key))
            except MemcachedError:
                self.log.error("Key {} is missing at destination".format(key))
                missing_keys = True
        if missing_keys:
            self.fail("Some keys are missing at destination")
