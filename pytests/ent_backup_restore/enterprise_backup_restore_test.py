import re
from random import randrange

from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase
from membase.api.rest_client import RestConnection, Bucket
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from security.auditmain import audit

AUDITBACKUPID = 20480
AUDITRESTOREID= 20485


class EnterpriseBackupRestoreTest(EnterpriseBackupRestoreBase):
    def setUp(self):
        super(EnterpriseBackupRestoreTest, self).setUp()

    def tearDown(self):
        super(EnterpriseBackupRestoreTest, self).tearDown()

    def test_backup_create(self):
        self.backup_create_validate()

    def test_backup_restore_sanity(self):
        """
        1. Create default bucket on the cluster and loads it with given number of items
        2. Perform updates and create backups for specified number of times (test param number_of_backups)
        3. Perform restores for the same number of times with random start and end values
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", self.expires)
        self.ops_type = self.input.param("ops-type", "update")
        if self.auto_failover:
            self.log.info("Enabling auto failover on " + str(self.backupset.cluster_host))
            rest_conn = RestConnection(self.backupset.cluster_host)
            rest_conn.update_autofailover_settings(self.auto_failover, self.auto_failover_timeout)
        self.backup_create_validate()
        for i in range(1, self.backupset.number_of_backups + 1):
            if self.ops_type == "update":
                self._load_all_buckets(self.master, gen, "update", self.expires)
            elif self.ops_type == "delete":
                self._load_all_buckets(self.master, gen, "delete", self.expires)
            self.backup_cluster_validate()
        self.targetMaster = True
        start = randrange(1, self.backupset.number_of_backups + 1)
        if start == self.backupset.number_of_backups:
            end = start
        else:
            end = randrange(start, self.backupset.number_of_backups + 1)
        restored = {"{0}/{1}".format(start, end): ""}
        for i in range(1, self.backupset.number_of_backups + 1):
            if self.reset_restore_cluster:
                self.backup_reset_clusters(self.cluster_to_restore)
                if self.same_cluster:
                    self._initialize_nodes(Cluster(), self.servers[:self.nodes_init])
                else:
                    self._initialize_nodes(Cluster(), self.input.clusters[0][:self.nodes_init])
            self.backupset.start = start
            self.backupset.end = end
            self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")
            if self.backupset.number_of_backups == 1:
                continue
            while "{0}/{1}".format(start, end) in restored:
                start = randrange(1, self.backupset.number_of_backups + 1)
                if start == self.backupset.number_of_backups:
                    end = start
                else:
                    end = randrange(start, self.backupset.number_of_backups + 1)
            restored["{0}/{1}".format(start, end)] = ""

    def test_backup_restore_after_rebalance(self):
        """
        1. Create default bucket on the cluster and loads it with given number of items
        2. Does a rebalance on cluster to be backed up with specified number of servers in (test param nodes_in) and
        servers out (test param nodes_out)
        3. Takes a backup
        4. Does a rebalance on cluster to be restored to with specified number of servers in (test param nodes_in) and
        servers out (test param nodes_out)
        5. Performs a restore on the restore cluster
        """
        serv_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        serv_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create_validate()
        self.backupset.number_of_backups = 1
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, serv_in, serv_out)
        rebalance.result()
        self.backup_cluster_validate()
        if not self.same_cluster:
            self._initialize_nodes(Cluster(), self.input.clusters[0][:self.nodes_init])
            serv_in = self.input.clusters[0][self.nodes_init: self.nodes_init + self.nodes_in]
            serv_out = self.input.clusters[0][self.nodes_init - self.nodes_out: self.nodes_init]
            rebalance = self.cluster.async_rebalance(self.cluster_to_restore, serv_in, serv_out)
        else:
            rebalance = self.cluster.async_rebalance(self.cluster_to_restore, serv_out, serv_in)
        rebalance.result()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function="<=")

    def test_backup_restore_with_rebalance(self):
        """
        1. Create default bucket on the cluster and loads it with given number of items
        2. Does a rebalance on cluster to be backed up with specified number of servers in (test param nodes_in) and
        servers out (test param nodes_out)
        3. Takes a backup while rebalance is going on
        4. Does a rebalance on cluster to be restored to with specified number of servers in (test param nodes_in) and
        servers out (test param nodes_out)
        5. Performs a restore on the restore cluster while rebalance is going on
        """
        serv_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        serv_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create_validate()
        self.backupset.number_of_backups = 1
        self.cluster.async_rebalance(self.cluster_to_backup, serv_in, serv_out)
        self.sleep(10)
        self.backup_cluster_validate()
        if not self.same_cluster:
            self._initialize_nodes(Cluster(), self.input.clusters[0][:self.nodes_init])
            serv_in = self.input.clusters[0][self.nodes_init: self.nodes_init + self.nodes_in]
            serv_out = self.input.clusters[0][self.nodes_init - self.nodes_out: self.nodes_init]
            self.cluster.async_rebalance(self.cluster_to_restore, serv_in, serv_out)
        else:
            self.cluster.async_rebalance(self.cluster_to_restore, serv_out, serv_in)
        self.sleep(10)
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function="<=")

    def test_backup_restore_with_ops(self):
        """
        1. Create default bucket on the cluster and loads it with given number of items
        2. Perform the specified ops (test param ops-type) and create backups for specified number of times
        (test param number_of_backups)
        3. Perform restores for the same number of times with random start and end values
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.ops_type = self.input.param("ops-type", "update")
        self.backup_create()
        for i in range(1, self.backupset.number_of_backups + 1):
            self._backup_restore_with_ops()
        start = randrange(1, self.backupset.number_of_backups + 1)
        if start == self.backupset.number_of_backups:
            end = start
        else:
            end = randrange(start, self.backupset.number_of_backups + 1)
        restored = {"{0}/{1}".format(start, end): ""}
        for i in range(1, self.backupset.number_of_backups + 1):
            self.backupset.start = start
            self.backupset.end = end
            self._backup_restore_with_ops(backup=False)
            if self.backupset.number_of_backups == 1:
                continue
            while "{0}/{1}".format(start, end) in restored:
                start = randrange(1, self.backupset.number_of_backups + 1)
                if start == self.backupset.number_of_backups:
                    end = start
                else:
                    end = randrange(start, self.backupset.number_of_backups + 1)
            restored["{0}/{1}".format(start, end)] = ""

    def _backup_restore_with_ops(self, exp=0, backup=True, compare_uuid=False, compare_function="==", replicas=False,
                                 mode="memory"):
        self.ops_type = self.input.param("ops-type", "update")
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, self.ops_type, exp)
        if backup:
            self.backup_cluster_validate()
        else:
            self.backup_restore_validate(compare_uuid=compare_uuid, seqno_compare_function=compare_function,
                                         replicas=replicas, mode=mode)

    def test_backup_list(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backup and validates it
        3. Executes list command on the backupset and validates the output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_list_validate()

    def test_backup_list_optional_switches(self):
        """
        1. Creates specified buckets on the cluster and loads it with given number of items
           Note: this test should be run with 2 buckets
        2. Creates two backupsets
        3. Creates two backups on each of the backupset
        4. Executes list command with --name and validates
        5. Executes list command with --name and --incr-backup and validates
        6. Executes list command with --name, --incr-backup and --bucket-backup and validates
        """
        rest_conn = RestConnection(self.backupset.cluster_host)
        rest_conn.create_bucket(bucket="default2",ramQuotaMB=512)
        self.buckets.append(Bucket(name="default2"))
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=2)
        self.backupset.name = "backup2"
        self.backup_create()
        self._take_n_backups(n=2)
        incr_names = 0
        backup_name = False
        self.backupset.backup_list_name = "backup"
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if self.backupset.backup_list_name in line:
                backup_name = True
            if self.backups[0] in line:
                incr_names += 1
            if self.backups[1] in line:
                incr_names += 1
        self.assertTrue(backup_name, "Expected backup name not found in output")
        self.log.info("Expected backup name found in output")
        self.assertEqual(incr_names, 2, "Expected backups were not listed for --name option")
        self.log.info("Expected backups listed for --name option")
        incr_names = 0
        backup_name = False
        self.backupset.backup_list_name = "backup2"
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if self.backupset.backup_list_name in line:
                backup_name = True
            if self.backups[2] in line:
                incr_names += 1
            if self.backups[3] in line:
                incr_names += 1
        self.assertTrue(backup_name, "Expected backup name not found in output")
        self.log.info("Expected backup name found in output")
        self.assertEqual(incr_names, 2, "Expected backups were not listed for --name option")
        self.log.info("Expected backups listed for --name option")
        buckets = 0
        name = False
        self.backupset.backup_list_name = "backup"
        self.backupset.backup_incr_backup = self.backups[0]
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if self.backupset.backup_incr_backup in line:
                name = True
            if self.buckets[0].name in line:
                buckets += 1
            if self.buckets[1].name in line:
                buckets += 1
        self.assertTrue(name, "Expected incremental backup name not found in output")
        self.log.info("Expected incrmental backup name found in output")
        self.assertEqual(buckets, 2, "Expected buckets were not listed for --incr-backup option")
        self.log.info("Expected buckets were listed for --incr-backup option")
        name = False
        items = 0
        self.backupset.backup_list_name = "backup2"
        self.backupset.backup_incr_backup = self.backups[2]
        self.backupset.bucket_backup = self.buckets[0].name
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if self.buckets[0].name in line:
                name = True
            if "shard" in line:
                split = line.split(" ")
                split = [s for s in split if s]
                items += int(split[1])
        self.assertTrue(name, "Expected bucket not listed for --bucket-backup option")
        self.log.info("Expected bucket listed for --bucket-backup option")
        self.assertEqual(items, self.num_items, "Mismatch in items for --bucket-backup option")
        self.log.info("Expected number of items for --bucket-backup option")

    def _take_n_backups(self, n=1, validate=False):
        for i in range(1, n + 1):
            if validate:
                self.backup_cluster_validate()
            else:
                self.backup_cluster()

    def test_backup_compact(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backup and validates it
        3. Executes compact command on the backupset and validates the output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_compact_validate()

    def test_restore_from_compacted_backup(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backup and validates it
        3. Executes compact command on the backupset
        4. Restores from the compacted backup and validates it
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_compact()
        self.backup_restore_validate()

    def test_backup_restore_with_nodes_reshuffle(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Enlists the default zone of current cluster - backsup the cluster and validates
        3. Creates a new zone - shuffles cluster host to new zone
        4. Restores to cluster host and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        rest_conn = RestConnection(self.backupset.cluster_host)
        zones = rest_conn.get_zone_names().keys()
        source_zone = zones[0]
        target_zone = "test_backup_restore"
        self.log.info("Current nodes in group {0} : {1}".format(source_zone,
                                                                str(rest_conn.get_nodes_in_zone(source_zone).keys())))
        self.log.info("Taking backup with current groups setup")
        self.backup_create()
        self.backup_cluster_validate()
        self.log.info("Creating new zone " + target_zone)
        rest_conn.add_zone(target_zone)
        self.log.info("Moving {0} to new zone {1}".format(self.backupset.cluster_host.ip, target_zone))
        rest_conn.shuffle_nodes_in_zones(["{0}".format(self.backupset.cluster_host.ip)],source_zone,target_zone)
        self.log.info("Restoring to {0} after group change".format(self.backupset.cluster_host.ip))
        self.backup_restore_validate()
        self.log.info("Moving {0} back to old zone {1}".format(self.backupset.cluster_host.ip, source_zone))
        rest_conn.shuffle_nodes_in_zones(["{0}".format(self.backupset.cluster_host.ip)],target_zone,source_zone)
        self.log.info("Deleting new zone " + target_zone)
        rest_conn.delete_zone(target_zone)

    def test_backup_restore_with_firewall(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates backupset on backup host
        3. Enables firewall on cluster host and validates if backup cluster command throws expected error
        4. Disables firewall on cluster host, takes backup and validates
        5. Enables firewall on restore host and validates if backup restore command throws expected error
        6. Disables firewall on restore host, restores and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.log.info("Enabling firewall on cluster host before backup")
        RemoteUtilHelper.enable_firewall(self.backupset.cluster_host)
        output, error = self.backup_cluster()
        self.assertTrue("getsockopt: connection refused" in output[0],
                        "Expected error not thrown by backup cluster when firewall enabled")
        self.log.info("Disabling firewall on cluster host to take backup")
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.disable_firewall()
        self.log.info("Trying backup now")
        self.backup_cluster_validate()
        self.log.info("Enabling firewall on restore host before restore")
        RemoteUtilHelper.enable_firewall(self.backupset.restore_cluster_host)
        output, error = self.backup_restore()
        self.assertTrue("getsockopt: connection refused" in output[0],
                       "Expected error not thrown by backup restore when firewall enabled")
        self.log.info("Disabling firewall on restore host to restore")
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.disable_firewall()
        self.log.info("Trying restore now")
        self.backup_restore_validate()

    def test_backup_restore_with_audit(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates backupset on backup host
        3. Creates a backup of the cluster host - verifies if corresponding entry was created in audit log
        4. Restores data on to restore host - verifies if corresponding entry was created in audit log
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        audit_obj = audit(AUDITBACKUPID, self.backupset.cluster_host)
        status = audit_obj.getAuditStatus()
        self.log.info("Audit status on {0} is {1}".format(self.backupset.cluster_host.ip, status))
        if not status:
            self.log.info("Enabling audit on {0}".format(self.backupset.cluster_host.ip))
            audit_obj.setAuditEnable('true')
        self.backup_create()
        self.backup_cluster()
        field_verified, value_verified = audit_obj.validateEvents(self._get_event_expected_results(action='backup'))
        self.assertTrue(field_verified, "One of the fields is not matching")
        self.assertTrue(value_verified, "Values for one of the fields is not matching")
        audit_obj = audit(AUDITBACKUPID, self.backupset.restore_cluster_host)
        status = audit_obj.getAuditStatus()
        self.log.info("Audit status on {0} is {1}".format(self.backupset.restore_cluster_host.ip, status))
        if not status:
            self.log.info("Enabling audit on {0}".format(self.backupset.restore_cluster_host.ip))
            audit_obj.setAuditEnable('true')
        self.backup_restore()
        audit_obj = audit(AUDITRESTOREID, self.backupset.restore_cluster_host)
        field_verified, value_verified = audit_obj.validateEvents(self._get_event_expected_results(action='restore'))
        self.assertTrue(field_verified, "One of the fields is not matching")
        self.assertTrue(value_verified, "Values for one of the fields is not matching")

    def _get_event_expected_results(self, action):
        if action == 'backup':
            expected_results = {
                "real_userid:source": "memcached",
                "real_userid:user": "default",
                "name": "opened DCP connection",
                "id": AUDITBACKUPID,
                "description": "opened DCP connection",
                "timestamp": "{0}".format(self.backups[0]),
                "bucket": "{0}".format(self.buckets[0].name),
                "sockname": "{0}:11210".format(self.backupset.cluster_host.ip),
                "peername": "{0}".format(self.backupset.backup_host.ip)
                }
        elif action == 'restore':
            expected_results = {
                "real_userid:source": "memcached",
                "real_userid:user": "unknown",
                "name": "authentication succeeded",
                "id": AUDITRESTOREID,
                "description": "Authentication to the cluster succeeded",
                "timestamp": "{0}".format(self.backups[0]),
                "bucket": "{0}".format(self.buckets[0].name),
                "sockname": "{0}:11210".format(self.backupset.restore_cluster_host.ip),
                "peername": "{0}".format(self.backupset.backup_host.ip)
                }
        return expected_results

    def test_backup_restore_with_lesser_nodes(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Adds another node to restore cluster and rebalances - note the test has to be run with nodes_init >= 3 so
           that cluster host had more nodes than restore host
        3. Creates backupset on backup host
        4. Creates backup of cluster host with 3 or more number of nodes and validates
        5. Restores to restore host with lesser number of nodes (2) and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        rest_conn = RestConnection(self.backupset.restore_cluster_host)
        rest_conn.add_node(self.input.clusters[0][1].rest_username, self.input.clusters[0][1].rest_password,
                           self.input.clusters[0][1].ip)
        rebalance = self.cluster.async_rebalance(self.cluster_to_restore, [], [])
        rebalance.result()
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_restore_validate()

    def test_backup_with_full_disk(self):
        """
        Things to be done before running this testcase:
            - scripts/install.py has to be run with init_nodes=False
            - scripts/cbqe3043.py has to be run against the ini file - this script will mount a 20MB partition on the
              nodes required for the test
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a bucket on the backup host and pumps it with 50000 items so that 20MB disk is almost full
        3. Sets backup directory to the 20MB partition and creates a backupset
        4. Keeps taking backup until no space left on device error is hit
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        rest_conn = RestConnection(self.backupset.backup_host)
        rest_conn.create_bucket(bucket="default",ramQuotaMB=512)
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=50000)
        self._load_all_buckets(self.backupset.backup_host, gen, "create", 0)
        self.backupset.directory = "/cbqe3043/entbackup"
        self.backup_create()
        output, error = self.backup_cluster()
        while "Backup successfully completed" in output[0]:
            output, error = self.backup_cluster()
        self.assertTrue("no space left on device" in output[0],
                        "Expected error message not thrown by backup when disk is full")
        self.log.info("Expected no space left on device error thrown by backup command")

    def test_backup_and_restore_with_memcached_buckets(self):
        """
        1. Creates specified buckets on the cluster and loads it with given number of items- memcached bucket has to
           be created for this test (memcached_buckets=1)
        2. Creates a backupset, takes backup of the cluster host and validates
        3. Executes list command on the backup and validates that memcached bucket has been skipped
        4. Restores the backup and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        status, output, message = self.backup_list()
        if not status:
            self.fail("Getting backup list to validate memcached buckets failed.")
        for line in output:
            self.assertTrue("memcached_bucket0" not in line,
                            "Memcached bucket found in backup list output after backup")
        self.log.info("Memcached bucket not found in backup list output after backup as expected")
        self.backup_restore()

    def test_backup_with_erlang_crash_and_restart(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills and restarts erlang process
        4. Validates backup output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                          backup_host=self.backupset.backup_host,
                                          directory=self.backupset.directory, name=self.backupset.name,
                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                          no_progress_bar=self.no_progress_bar,
                                          cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang()
        conn.start_couchbase()
        output = backup_result.result(timeout=200)
        self.assertTrue("Backup successfully completed" in output[0],
                        "Backup failed with erlang crash and restart within 180 seconds")
        self.log.info("Backup succeeded with erlang crash and restart within 180 seconds")

    def test_backup_with_couchbase_stop_and_start(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills and restarts couchbase server
        4. Validates backup output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                          backup_host=self.backupset.backup_host,
                                          directory=self.backupset.directory, name=self.backupset.name,
                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                          no_progress_bar=self.no_progress_bar,
                                          cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.stop_couchbase()
        conn.start_couchbase()
        output = backup_result.result(timeout=200)
        self.assertTrue("Backup successfully completed" in output[0],
                        "Backup failed with couchbase stop and start within 180 seconds")
        self.log.info("Backup succeeded with couchbase stop and start within 180 seconds")

    def test_backup_with_memcached_crash_and_restart(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills and restarts memcached process
        4. Validates backup output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                          backup_host=self.backupset.backup_host,
                                          directory=self.backupset.directory, name=self.backupset.name,
                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                          no_progress_bar=self.no_progress_bar,
                                          cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.pause_memcached()
        conn.unpause_memcached()
        output = backup_result.result(timeout=200)
        self.assertTrue("Backup successfully completed" in output[0],
                        "Backup failed with memcached crash and restart within 180 seconds")
        self.log.info("Backup succeeded with memcached crash and restart within 180 seconds")

    def test_backup_with_erlang_crash(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills erlang process
        4. Waits for 200s and Validates backup error
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                          backup_host=self.backupset.backup_host,
                                          directory=self.backupset.directory, name=self.backupset.name,
                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                          no_progress_bar=self.no_progress_bar,
                                          cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang()
        output = backup_result.result(timeout=200)
        self.assertTrue("Error backing up cluster: Not all data was backed up due to connectivity issues." in output[0],
                        "Expected error message not thrown by Backup 180 seconds after erlang crash")
        self.log.info("Expected error message thrown by Backup 180 seconds after erlang crash")
        conn.start_couchbase()
        self.sleep(30)

    def test_backup_with_couchbase_stop(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills couchbase server
        4. Waits for 200s and Validates backup error
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                          backup_host=self.backupset.backup_host,
                                          directory=self.backupset.directory, name=self.backupset.name,
                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                          no_progress_bar=self.no_progress_bar,
                                          cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.stop_couchbase()
        output = backup_result.result(timeout=200)
        self.assertTrue("Error backing up cluster: Not all data was backed up due to connectivity issues." in output[0],
                        "Expected error message not thrown by Backup 180 seconds after couchbase-server stop")
        self.log.info("Expected error message thrown by Backup 180 seconds after couchbase-server stop")
        conn.start_couchbase()
        self.sleep(30)

    def test_backup_with_memcached_crash(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills memcached process
        4. Waits for 200s and Validates backup error
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                          backup_host=self.backupset.backup_host,
                                          directory=self.backupset.directory, name=self.backupset.name,
                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                          no_progress_bar=self.no_progress_bar,
                                          cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.pause_memcached()
        output = backup_result.result(timeout=200)
        self.assertTrue("Error backing up cluster: Not all data was backed up due to connectivity issues." in output[0],
                        "Expected error message not thrown by Backup 180 seconds after memcached crash")
        self.log.info("Expected error message thrown by Backup 180 seconds after memcached crash")
        conn.unpause_memcached()
        self.sleep(30)

    def test_restore_with_erlang_crash_and_restart(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills and restarts erlang process
        4. Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        restore_result = self.cluster.async_restore_cluster(restore_host=self.backupset.restore_cluster_host,
                                                           backup_host=self.backupset.backup_host,
                                                           backups=self.backups, start=self.backupset.start,
                                                           end=self.backupset.end, directory=self.backupset.directory,
                                                           name=self.backupset.name,
                                                           force_updates=self.backupset.force_updates,
                                                           no_progress_bar=self.no_progress_bar,
                                                           cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.kill_erlang()
        conn.start_couchbase()
        output = restore_result.result(timeout=200)
        self.assertTrue("Restore completed successfully" in output[0],
                        "Restore failed with erlang crash and restart within 180 seconds")
        self.log.info("Restore succeeded with erlang crash and restart within 180 seconds")

    def test_restore_with_couchbase_stop_and_start(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills and restarts couchbase process
        4. Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        restore_result = self.cluster.async_restore_cluster(restore_host=self.backupset.restore_cluster_host,
                                                           backup_host=self.backupset.backup_host,
                                                           backups=self.backups, start=self.backupset.start,
                                                           end=self.backupset.end, directory=self.backupset.directory,
                                                           name=self.backupset.name,
                                                           force_updates=self.backupset.force_updates,
                                                           no_progress_bar=self.no_progress_bar,
                                                           cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.stop_couchbase()
        conn.start_couchbase()
        output = restore_result.result(timeout=200)
        self.assertTrue("Restore completed successfully" in output[0],
                        "Restore failed with couchbase stop and start within 180 seconds")
        self.log.info("Restore succeeded with couchbase stop and start within 180 seconds")

    def test_restore_with_memcached_crash_and_restart(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills and restarts memcached process
        4. Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        restore_result = self.cluster.async_restore_cluster(restore_host=self.backupset.restore_cluster_host,
                                                           backup_host=self.backupset.backup_host,
                                                           backups=self.backups, start=self.backupset.start,
                                                           end=self.backupset.end, directory=self.backupset.directory,
                                                           name=self.backupset.name,
                                                           force_updates=self.backupset.force_updates,
                                                           no_progress_bar=self.no_progress_bar,
                                                           cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.pause_memcached()
        conn.unpause_memcached()
        output = restore_result.result(timeout=200)
        self.assertTrue("Restore completed successfully" in output[0],
                        "Restore failed with memcached crash and restart within 180 seconds")
        self.log.info("Restore succeeded with memcached crash and restart within 180 seconds")

    def test_restore_with_erlang_crash(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills erlang process
        4. Waits for 200s and Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        restore_result = self.cluster.async_restore_cluster(restore_host=self.backupset.restore_cluster_host,
                                                           backup_host=self.backupset.backup_host,
                                                           backups=self.backups, start=self.backupset.start,
                                                           end=self.backupset.end, directory=self.backupset.directory,
                                                           name=self.backupset.name,
                                                           force_updates=self.backupset.force_updates,
                                                           no_progress_bar=self.no_progress_bar,
                                                           cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.kill_erlang()
        output = restore_result.result(timeout=200)
        self.assertTrue("Error restoring cluster: Not all data was backed up due to connectivity issues." in output[0],
                        "Expected error message not thrown by Restore 180 seconds after erlang crash")
        self.log.info("Expected error message thrown by Restore 180 seconds after erlang crash")
        conn.start_couchbase()
        self.sleep(30)

    def test_restore_with_couchbase_stop(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills couchbase server
        4. Waits for 200s and Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        restore_result = self.cluster.async_restore_cluster(restore_host=self.backupset.restore_cluster_host,
                                                           backup_host=self.backupset.backup_host,
                                                           backups=self.backups, start=self.backupset.start,
                                                           end=self.backupset.end, directory=self.backupset.directory,
                                                           name=self.backupset.name,
                                                           force_updates=self.backupset.force_updates,
                                                           no_progress_bar=self.no_progress_bar,
                                                           cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.stop_couchbase()
        output = restore_result.result(timeout=200)
        self.assertTrue("Error restoring cluster: Not all data was backed up due to connectivity issues." in output[0],
                        "Expected error message not thrown by Restore 180 seconds after couchbase-server stop")
        self.log.info("Expected error message thrown by Restore 180 seconds after couchbase-server stop")
        conn.start_couchbase()
        self.sleep(30)

    def test_restore_with_memcached_crash(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills memcached process
        4. Waits for 200s and Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        restore_result = self.cluster.async_restore_cluster(restore_host=self.backupset.restore_cluster_host,
                                                           backup_host=self.backupset.backup_host,
                                                           backups=self.backups, start=self.backupset.start,
                                                           end=self.backupset.end, directory=self.backupset.directory,
                                                           name=self.backupset.name,
                                                           force_updates=self.backupset.force_updates,
                                                           no_progress_bar=self.no_progress_bar,
                                                           cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.pause_memcached()
        output = restore_result.result(timeout=200)
        self.assertTrue("Error restoring cluster: Not all data was backed up due to connectivity issues." in output[0],
                        "Expected error message not thrown by Restore 180 seconds after memcached crash")
        self.log.info("Expected error message thrown by Restore 180 seconds after memcached crash")
        conn.unpause_memcached()
        self.sleep(30)

    def test_backup_merge(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Takes specified number of backups (param number_of_backups - should be atleast 2 for this test case)
        3. Executes list command and validates if all backups are present
        4. Randomly selects a start and end and merges the backups
        5. Executes list command again and validates if the new merges set of backups are listed
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=self.backupset.number_of_backups)
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        backup_count = 0
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line):
                backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line).group()
                if backup_name in self.backups:
                    backup_count += 1
                    self.log.info("{0} matched in list command output".format(backup_name))
        self.assertEqual(backup_count, len(self.backups), "Initial number of backups did not match")
        self.log.info("Initial number of backups matched")
        self.backupset.start = randrange(1, self.backupset.number_of_backups)
        self.backupset.end = randrange(self.backupset.start + 1, self.backupset.number_of_backups + 1)
        status, output, message = self.backup_merge()
        if not status:
            self.fail(message)
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        backup_count = 0
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line):
                backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line).group()
                if backup_name in self.backups:
                    backup_count += 1
                    self.log.info("{0} matched in list command output".format(backup_name))
        self.assertEqual(backup_count, len(self.backups), "Merged number of backups did not match")
        self.log.info("Merged number of backups matched")

    def test_backup_merge_with_restore(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Takes two backups - restores from the backups and validates
        3. Merges both the backups - restores from merged backup and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=2)
        self.backupset.start = 1
        self.backupset.end = 2
        output, error = self.backup_restore()
        if error:
            self.fail("Restoring backup failed")
        self.log.info("Finished restoring backup before merging")
        status, output, message = self.backup_merge()
        if not status:
            self.fail(message)
        self.backupset.start = 1
        self.backupset.end = 1
        output, error = self.backup_restore()
        if error:
            self.fail("Restoring backup failed")
        self.log.info("Finished restoring backup after merging")

    def test_backup_merge_with_unmerged(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Takes two backups - merges them into one
        3. Takes 2 more backups - merges the new backups with already merged ones and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=2)
        self.backupset.start = 1
        self.backupset.end = 2
        self.log.info("Merging existing incremental backups")
        status, output, message = self.backup_merge()
        if not status:
            self.fail(message)
        self.log.info("Taking more backups")
        self._take_n_backups(n=2)
        self.backupset.start = 1
        self.backupset.end = 3
        self.log.info("Merging new backups into already merged backup")
        status, output, message = self.backup_merge()
        if not status:
            self.fail(message)
        self.log.info("Successfully merged new backups with already merged backup")

    def test_backup_purge(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset
        3. Initiates a backup and kills the erlang server while backup is going on
        4. Waits for the backup command to timeout
        5. Executes backup command again with purge option
        6. Validates the old backup is deleted and new backup is created successfully
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        old_backup_name = ""
        new_backup_name = ""
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                          backup_host=self.backupset.backup_host,
                                          directory=self.backupset.directory, name=self.backupset.name,
                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                          no_progress_bar=self.no_progress_bar,
                                          cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang()
        output = backup_result.result(timeout=200)
        self.log.info(str(output))
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line):
                old_backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line).group()
                self.log.info("Backup name before purge: " + old_backup_name)
        conn.start_couchbase()
        self.sleep(30)
        output, error = self.backup_cluster()
        if error or "Backup successfully completed" not in output[0]:
            self.fail("Taking cluster backup failed.")
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line):
                new_backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line).group()
                self.log.info("Backup name after purge: " + new_backup_name)
        self.assertNotEqual(old_backup_name, new_backup_name,
                            "Old backup name and new backup name are same when purge is used")
        self.log.info("Old backup name and new backup name are not same when purge is used")

    def test_backup_resume(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset
        3. Initiates a backup and kills the erlang server while backup is going on
        4. Waits for the backup command to timeout
        5. Executes backup command again with resume option
        6. Validates the old backup is resumes and backup is completed successfully
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        old_backup_name = ""
        new_backup_name = ""
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                          backup_host=self.backupset.backup_host,
                                          directory=self.backupset.directory, name=self.backupset.name,
                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                          no_progress_bar=self.no_progress_bar,
                                          cli_command_location=self.cli_command_location)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang()
        output = backup_result.result(timeout=200)
        self.log.info(str(output))
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line):
                old_backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line).group()
                self.log.info("Backup name before resume: " + old_backup_name)
        conn.start_couchbase()
        self.sleep(30)
        output, error = self.backup_cluster()
        if error or "Backup successfully completed" not in output[0]:
            self.fail("Taking cluster backup failed.")
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line):
                new_backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{9}Z", line).group()
                self.log.info("Backup name after resume: " + new_backup_name)
        self.assertEqual(old_backup_name, new_backup_name,
                            "Old backup name and new backup name are not same when resume is used")
        self.log.info("Old backup name and new backup name are same when resume is used")
