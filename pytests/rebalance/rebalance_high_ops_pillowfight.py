import random
import subprocess
from threading import Thread

from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster, PasswordAuthenticator
from couchbase.exceptions import NotFoundError

from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.memcached.helper.data_helper import VBucketAwareMemcached
from couchbase_helper.document import DesignDocument, View
from remote.remote_util import RemoteMachineShellConnection


class RebalanceHighOpsWithPillowFight(BaseTestCase):
    def setUp(self):
        super(RebalanceHighOpsWithPillowFight, self).setUp()
        self.rate_limit = self.input.param("rate_limit", 100000)
        self.batch_size = self.input.param("batch_size", 1000)
        self.doc_size = self.input.param("doc_size", 100)
        self.loader = self.input.param("loader", "pillowfight")
        self.instances = self.input.param("instances", 1)
        self.recovery_type = self.input.param("recovery_type", None)
        self.node_out = self.input.param("node_out", 0)
        self.threads = self.input.param("threads", 5)
        self.use_replica_to = self.input.param("use_replica_to", False)
        self.run_with_views = self.input.param("run_with_views", False)
        self.default_view_name = "upgrade-test-view"
        self.ddocs_num = self.input.param("ddocs-num", 1)
        self.view_num = self.input.param("view-per-ddoc", 2)
        self.is_dev_ddoc = self.input.param("is-dev-ddoc", False)
        self.ddocs = []
        self.run_view_query_iterations = self.input.param("run_view_query_iterations", 10)
        self.rebalance_quirks = self.input.param('rebalance_quirks', False)
        self.flusher_batch_split_trigger = self.input.param('flusher_batch_split_trigger', None)

        if self.rebalance_quirks:
            for server in self.servers:
                rest = RestConnection(server)
                rest.diag_eval(
                    "[ns_config:set({node, N, extra_rebalance_quirks}, [reset_replicas, trivial_moves]) || N <- ns_node_disco:nodes_wanted()].")
                # rest.diag_eval(
                #    "[ns_config:set({node, N, disable_rebalance_quirks}, [disable_old_master]) || N <- ns_node_disco:nodes_wanted()].")

        if self.flusher_batch_split_trigger:
            self.set_flusher_batch_split_trigger(flusher_batch_split_trigger=self.flusher_batch_split_trigger,
                                                 buckets=self.buckets)

    def tearDown(self):
        super(RebalanceHighOpsWithPillowFight, self).tearDown()

    def load_buckets_with_high_ops(self, server, bucket, items, batch=20000,
                                   threads=5, start_document=0, instances=1, ttl=0):
        cmd_format = "python scripts/thanosied.py  --spec couchbase://{0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --workers {9} --ttl {10} --rate_limit {11} " \
                     "--passes 1"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if self.num_replicas > 0 and self.use_replica_to:
            cmd_format = "{} --replicate_to 1".format(cmd_format)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password,
                                items, batch, threads, start_document,
                                cb_version, instances, ttl, self.rate_limit)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen.")
        if output:
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load.split(':')[1].strip())
            self.assertEqual(total_loaded, items,
                             "Failed to load {} items. Loaded only {} items".format(
                                 items,
                                 total_loaded))

    def test_kill_memcached_during_pillowfight(self):
        """
        To validate MB-34173
        Reference: 'Tests Needed To Verify MB-34173' google doc
        :return:
        """

        cmd_to_use = "cbc-pillowfight --json --num-items {0} -t 4 " \
                     "--spec couchbase://{1}:11210/default --rate-limit {2}" \
                     .format(self.num_items, self.master.ip, self.rate_limit)

        # Select random node from the cluster to kill memcached
        target_server = self.servers[random.randint(0, self.nodes_init-1)]
        shell_conn = RemoteMachineShellConnection(target_server)

        # Create PillowFight loader thread
        self.loader = "pillowfight"
        load_thread = self.load_docs(self.num_items, custom_cmd=cmd_to_use)
        load_thread.start()
        self.sleep(10, "Sleep before killing memcached process on {0}"
                   .format(target_server))
        # Kill memcached on the target_server
        shell_conn.kill_memcached()
        shell_conn.disconnect()

        # Wait for loader thread to complete
        load_thread.join()

        # Verify last_persistence_snap start/stop values
        self.sleep(5, "Sleep before last_persistence__snap verification")
        self.check_snap_start_corruption()

    def update_buckets_with_high_ops(self, server, bucket, items, ops,
                                     batch=20000, threads=5, start_document=0,
                                     instances=1):
        cmd_format = "python scripts/thanosied.py  --spec couchbase://{0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --workers {9} --rate_limit {10} " \
                     "--passes 1  --update_counter {7}"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if self.num_replicas > 0 and self.use_replica_to:
            cmd_format = "{} --replicate_to 1".format(cmd_format)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password,
                                items, batch, threads, start_document,
                                cb_version, instances, ops)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen.")
        if output:
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load.split(':')[1].strip())
            self.assertEqual(total_loaded, items,
                             "Failed to update {} items. Updated only {} items".format(
                                 items,
                                 total_loaded))

    def delete_buckets_with_high_ops(self, server, bucket, items, ops,
                                     batch=20000, threads=5,
                                     start_document=0,
                                     instances=1):
        cmd_format = "python scripts/thanosied.py  --spec couchbase://{0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --workers {9} --rate_limit {10} " \
                     "--passes 0  --delete --num_delete {4}"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if self.num_replicas > 0 and self.use_replica_to:
            cmd_format = "{} --replicate_to 1".format(cmd_format)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password,
                                items, batch, threads, start_document,
                                cb_version, instances, ops)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen.")
        if output:
            self.log.info("output : {}".format(output))
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load.split(':')[1].strip())
            # Tool seems to be returning wrong number for deleted
            # Since its verified using check_dataloss_for_high_ops_loader inside the script its ok to skip here
            self.log.info("Total deleted from the datagen tool : {}".format(total_loaded))

    def load(self, server, items, batch=1000, docsize=100, rate_limit=100000,
             start_at=0, custom_cmd=None):
        from lib.testconstants import COUCHBASE_FROM_SPOCK
        rest = RestConnection(server)
        import multiprocessing

        num_threads = multiprocessing.cpu_count() // 2
        num_cycles = int(items / batch * 1.5 / num_threads)

        cmd = "cbc-pillowfight -U couchbase://{0}/default -I {1} -m {3} -M {3} -B {2} -c {5} --sequential --json -t {4} " \
              "--rate-limit={6} --start-at={7}" \
            .format(server.ip, items, batch, docsize, num_threads, num_cycles,
                    rate_limit, start_at)

        cmd = custom_cmd or cmd

        if self.num_replicas > 0 and self.use_replica_to:
            cmd += " --replicate-to=1"
        if rest.get_nodes_version()[:5] in COUCHBASE_FROM_SPOCK:
            cmd += " -u Administrator -P password"
        self.log.info("Executing '{0}'...".format(cmd))
        rc = subprocess.call(cmd, shell=True)
        if rc != 0:
            self.fail(
                "Exception running cbc-pillowfight: subprocess module returned non-zero response!")

    def load_docs(self, num_items=0, start_document=0, ttl=0, custom_cmd=None):
        if num_items == 0:
            num_items = self.num_items
        if self.loader == "pillowfight":
            load_thread = Thread(target=self.load,
                                 name="pillowfight_load",
                                 args=(
                                     self.master, num_items, self.batch_size,
                                     self.doc_size, self.rate_limit,
                                     start_document, custom_cmd))
            return load_thread
        elif self.loader == "high_ops":
            if num_items == 0:
                num_items = self.num_items
            load_thread = Thread(target=self.load_buckets_with_high_ops,
                                 name="high_ops_load",
                                 args=(self.master, self.buckets[0], num_items,
                                       self.batch_size,
                                       self.threads, start_document,
                                       self.instances, ttl))
            return load_thread

    def check_dataloss_for_high_ops_loader(self, server, bucket, items,
                                           batch=20000, threads=5,
                                           start_document=0,
                                           updated=False, ops=0, ttl=0, deleted=False, deleted_items=0,
                                           validate_expired=None, passes=0):
        from lib.memcached.helper.data_helper import VBucketAwareMemcached

        cmd_format = "python scripts/thanosied.py  --spec couchbase://{0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --validation 1 " \
                     "--rate_limit {9} --passes {10}"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if updated:
            cmd_format = "{} --update_counter 0".format(cmd_format)
        if deleted:
            cmd_format = "{} --deleted --deleted_items {}".format(cmd_format, deleted_items)
        if ttl > 0:
            cmd_format = "{} --ttl {}".format(cmd_format, ttl)
        if validate_expired:
            cmd_format = "{} --validate_expired".format(cmd_format)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password,
                                int(items), batch, threads, start_document, cb_version, self.rate_limit, passes)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        errors = []
        rest = RestConnection(self.master)
        VBucketAware = VBucketAwareMemcached(rest, bucket.name)
        _, _, _ = VBucketAware.request_map(rest, bucket.name)
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen validator.")
        if output:
            loaded = output.split('\n')[:-1]
            for load in loaded:
                if "Missing keys:" in load:
                    keys = load.split(":")[1].strip().replace('[', '').replace(']', '')
                    keys = keys.split(',')
                    for key in keys:
                        key = key.strip()
                        key = key.replace('\'', '').replace('\\', '')
                        vBucketId = VBucketAware._get_vBucket_id(key)
                        errors.append(
                            ("Missing key: {0}, VBucketId: {1}".format(key, vBucketId)))
                if "Mismatch keys: " in load:
                    keys = load.split(":")[1].strip().replace('[', '').replace(']', '')
                    keys = keys.split(',')
                    for key in keys:
                        key = key.strip()
                        key = key.replace('\'', '').replace('\\', '')
                        vBucketId = VBucketAware._get_vBucket_id(key)
                        errors.append((
                            "Wrong value for key: {0}, VBucketId: {1}".format(
                                key, vBucketId)))
        return errors

    def check_dataloss(self, server, bucket, num_items):
        if RestConnection(server).get_nodes_version()[:5] < '5':
            bkt = Bucket('couchbase://{0}/{1}'.format(server.ip, bucket.name))
        else:
            cluster = Cluster("couchbase://{}".format(server.ip))
            auth = PasswordAuthenticator(server.rest_username,
                                         server.rest_password)
            cluster.authenticate(auth)
            bkt = cluster.open_bucket(bucket.name)

        rest = RestConnection(self.master)
        VBucketAware = VBucketAwareMemcached(rest, bucket.name)
        _, _, _ = VBucketAware.request_map(rest, bucket.name)
        batch_start = 0
        batch_end = 0
        batch_size = 10000
        errors = []
        while num_items > batch_end:
            batch_end = batch_start + batch_size
            keys = []
            for i in range(batch_start, batch_end, 1):
                keys.append(str(i).rjust(20, '0'))
            try:
                bkt.get_multi(keys)
                self.log.info(
                    "Able to fetch keys starting from {0} to {1}".format(
                        keys[0], keys[len(keys) - 1]))
            except Exception as e:
                self.log.error(e)
                self.log.info("Now trying keys in the batch one at a time...")
                key = ''
                try:
                    for key in keys:
                        bkt.get(key)
                except NotFoundError:
                    vBucketId = VBucketAware._get_vBucket_id(key)
                    errors.append("Missing key: {0}, VBucketId: {1}".
                                  format(key, vBucketId))
            batch_start += batch_size
        return errors

    def check_data(self, server, bucket, num_items=0, start_document=0,
                   updated=False, ops=0, batch_size=0, ttl=0, deleted=False,
                   deleted_items=0, validate_expired=None, passes=1):
        if batch_size == 0:
            batch_size = self.batch_size
        if self.loader == "pillowfight":
            return self.check_dataloss(server, bucket, num_items)
        elif self.loader == "high_ops":
            return self.check_dataloss_for_high_ops_loader(server, bucket,
                                                           num_items,
                                                           self.batch_size,
                                                           self.threads,
                                                           start_document,
                                                           updated, ops, ttl,
                                                           deleted, deleted_items, validate_expired, passes)

    def test_rebalance_in(self):
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]

        load_thread = self.load_docs()
        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()
        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items)
        load_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 [])
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init + self.nodes_in])
        if self.run_with_views:
            view_query_thread.join()
        num_items_to_validate = self.num_items * 3
        errors = self.check_data(self.master, bucket, num_items=num_items_to_validate)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per "
                             "replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_rebalance_in_with_update_workload(self):
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()
        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()

        update_thread = Thread(target=self.update_buckets_with_high_ops,
                               name="update_high_ops_load",
                               args=(self.master, self.buckets[0], self.num_items,
                                     self.num_items, self.batch_size,
                                     self.threads, 0, self.instances))

        update_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 [])
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        update_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init + self.nodes_in])
        if self.run_with_views:
            view_query_thread.join()
        num_items_to_validate = self.num_items
        errors = self.check_data(self.master, bucket, num_items_to_validate, 0, True, self.num_items * 2)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        else:
            if errors:
                self.fail("FATAL : Few mutations missed. See above for details.")

        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_rebalance_in_with_delete_workload(self):
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()
        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        self.sleep(60)

        delete_thread = Thread(target=self.delete_buckets_with_high_ops,
                               name="delete_high_ops_load",
                               args=(
                                   self.master, self.buckets[0], self.num_items,
                                   self.num_items, self.batch_size,
                                   self.threads, 0, self.instances))

        delete_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 [])
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        delete_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init + self.nodes_in])
        if self.run_with_views:
            view_query_thread.join()
        num_items_to_validate = self.num_items
        errors = self.check_data(self.master, bucket, num_items_to_validate, 0,
                                 deleted=True, deleted_items=num_items_to_validate, passes=0)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if rest.get_active_key_count(bucket) != 0:
            self.fail(
                "FATAL: Data loss detected!! Docs Deleted : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        else:
            if errors:
                self.fail(
                    "FATAL : Few mutations missed. See above for details.")

        if self.num_replicas > 0:
            self.assertEqual(0,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys deleted from replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_rebalance_in_with_expiry(self):
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 10, bucket=bucket)
        load_thread = self.load_docs(ttl=10)
        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()
        self.log.info('starting the load thread...')
        load_thread.start()

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 [])
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init + self.nodes_in])
        if self.run_with_views:
            view_query_thread.join()
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 10, bucket=bucket)
        self.sleep(30)
        errors = self.check_data(self.master, bucket, self.num_items, 0, False, 0, self.batch_size, ttl=10,
                                 validate_expired=True, passes=0)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if rest.get_active_key_count(bucket) != 0:
            self.fail(
                "FATAL: Data loss detected!! Docs expected to be expired : {0}, docs present: {1}".
                    format(self.num_items,
                           rest.get_active_key_count(bucket)))
        else:
            if errors:
                self.fail(
                    "FATAL : Few mutations missed. See above for details.")

        if self.num_replicas > 0:
            self.assertEqual(0,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys deleted from replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 0, (rest.get_replica_key_count(bucket) // self.num_replicas)))

    def test_rebalance_out(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]

        self.log.info("Servers Out: {0}".format(servs_out))
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()

        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()

        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items)
        load_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], servs_out)
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()

        if self.run_with_views:
            view_query_thread.join()

        num_items_to_validate = self.num_items * 3
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per "
                             "replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_rebalance_out_with_update_workload(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()

        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()

        update_thread = Thread(target=self.update_buckets_with_high_ops,
                               name="update_high_ops_load",
                               args=(
                                   self.master, self.buckets[0], self.num_items,
                                   self.num_items, self.batch_size,
                                   self.threads, 0, self.instances))

        update_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], servs_out)
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        update_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        if self.run_with_views:
            view_query_thread.join()
        num_items_to_validate = self.num_items
        errors = self.check_data(self.master, bucket, num_items_to_validate, 0,
                                 True, self.num_items * 2)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        else:
            if errors:
                self.fail(
                    "FATAL : Few mutations missed. See above for details.")

        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_rebalance_out_with_delete_workload(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()

        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        self.sleep(60)

        delete_thread = Thread(target=self.delete_buckets_with_high_ops,
                               name="delete_high_ops_load",
                               args=(
                                   self.master, self.buckets[0], self.num_items,
                                   self.num_items, self.batch_size,
                                   self.threads, 0, self.instances))

        delete_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], servs_out)
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        delete_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        if self.run_with_views:
            view_query_thread.join()
        num_items_to_validate = self.num_items
        errors = self.check_data(self.master, bucket, num_items_to_validate, 0,
                                 deleted=True,
                                 deleted_items=num_items_to_validate, passes=0)

        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if rest.get_active_key_count(bucket) != 0:
            self.fail(
                "FATAL: Data loss detected!! Docs Deleted : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        else:
            if errors:
                self.fail(
                    "FATAL : Few mutations missed. See above for details.")

        if self.num_replicas > 0:
            self.assertEqual(0,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys deleted from replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_rebalance_out_with_expiry(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 10, bucket=bucket)
        load_thread = self.load_docs(ttl=10)
        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()
        self.log.info('starting the load thread...')
        load_thread.start()

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], servs_out)
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        if self.run_with_views:
            view_query_thread.join()
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 10, bucket=bucket)
        self.sleep(30)
        errors = self.check_data(self.master, bucket, self.num_items, 0, False, 0, self.batch_size, ttl=10,
                                 validate_expired=True, passes=0)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error

        if rest.get_active_key_count(bucket) != 0:
            self.fail(
                "FATAL: Data loss detected!! Docs expected to be expired : {0}, docs present: {1}".
                    format(self.num_items,
                           rest.get_active_key_count(bucket)))
        else:
            if errors:
                self.fail(
                    "FATAL : Few mutations missed. See above for details.")

        if self.num_replicas > 0:
            self.assertEqual(0,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys deleted from replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 0, (rest.get_replica_key_count(
                                     bucket) // self.num_replicas)))

    def test_rebalance_in_out(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]
        self.log.info("Servers Out: {0}".format(servs_out))
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()

        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()

        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items)
        load_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 servs_out)
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        if self.run_with_views:
            view_query_thread.join()

        num_items_to_validate = self.num_items * 3
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_rebalance_in_out_with_update_workload(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()
        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()

        update_thread = Thread(target=self.update_buckets_with_high_ops,
                               name="update_high_ops_load",
                               args=(
                                   self.master, self.buckets[0], self.num_items,
                                   self.num_items, self.batch_size,
                                   self.threads, 0, self.instances))

        update_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 servs_out)
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        update_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        if self.run_with_views:
            view_query_thread.join()

        num_items_to_validate = self.num_items
        errors = self.check_data(self.master, bucket, num_items_to_validate, 0,
                                 True, self.num_items * 2)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        else:
            if errors:
                self.fail(
                    "FATAL : Few mutations missed. See above for details.")

        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_rebalance_in_out_with_delete_workload(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()
        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        self.sleep(60)

        delete_thread = Thread(target=self.delete_buckets_with_high_ops,
                               name="delete_high_ops_load",
                               args=(
                                   self.master, self.buckets[0], self.num_items,
                                   self.num_items, self.batch_size,
                                   self.threads, 0, self.instances))

        delete_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 servs_out)
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        delete_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        if self.run_with_views:
            view_query_thread.join()

        num_items_to_validate = self.num_items
        errors = self.check_data(self.master, bucket, num_items_to_validate, 0,
                                 deleted=True,
                                 deleted_items=num_items_to_validate, passes=0)

        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if rest.get_active_key_count(bucket) != 0:
            self.fail(
                "FATAL: Data loss detected!! Docs Deleted : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        else:
            if errors:
                self.fail(
                    "FATAL : Few mutations missed. See above for details.")

        if self.num_replicas > 0:
            self.assertEqual(0,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys deleted from replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_rebalance_in_out_with_expiry(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 10, bucket=bucket)
        load_thread = self.load_docs(ttl=10)
        if self.run_with_views:
            self.log.info('creating ddocs and views')
            self.create_ddocs_and_views()
            self.log.info('starting the view query thread...')
            view_query_thread = self.run_view_queries()
            view_query_thread.start()
        self.log.info('starting the load thread...')
        load_thread.start()

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 servs_out)
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        if self.run_with_views:
            view_query_thread.join()
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 10, bucket=bucket)
        self.sleep(30)
        errors = self.check_data(self.master, bucket, self.num_items, 0, False, 0, self.batch_size, ttl=10,
                                 validate_expired=True, passes=0)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if rest.get_active_key_count(bucket) != 0:
            self.fail(
                "FATAL: Data loss detected!! Docs expected to be expired : {0}, docs present: {1}".
                    format(self.num_items,
                           rest.get_active_key_count(bucket)))
        else:
            if errors:
                self.fail(
                    "FATAL : Few mutations missed. See above for details.")

        if self.num_replicas > 0:
            self.assertEqual(0,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys deleted from replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 0, (rest.get_replica_key_count(
                                     bucket) // self.num_replicas)))

    def test_graceful_failover_addback(self):
        node_out = self.servers[self.node_out]
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        self.log.info('starting the load thread...')
        load_thread.start()
        # This code was specifically added for CBSE-6791
        if self.nodes_init == 1 and self.flusher_batch_split_trigger:
            self.cluster.rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [])
        load_thread.join()
        load_thread = self.load_docs(num_items=(self.num_items),
                                     start_document=self.num_items)
        load_thread.start()
        nodes_all = rest.node_statuses()
        for node in nodes_all:
            if node.ip == node_out.ip:
                break

        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            "graceful", wait_for_pending=360)

        failover_task.result()
        load_thread.join()

        load_thread = self.load_docs(num_items=self.num_items * 2,
                                     start_document=(self.num_items * 2))
        load_thread.start()
        if self.flusher_batch_split_trigger:
            for i in range(10):
                # do delta recovery and cancel add back few times
                # This was added to reproduce MB-34173
                rest.set_recovery_type(node.id, self.recovery_type)
                self.sleep(10)
                rest.add_back_node(node.id)
        rest.set_recovery_type(node.id, self.recovery_type)
        self.sleep(30)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])

        reached = RestHelper(rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        load_thread.join()
        if self.nodes_init == 1 and self.flusher_batch_split_trigger:
            self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init + 1])
        elif self.flusher_batch_split_trigger:
            self.check_snap_start_corruption(servers_to_check=self.servers[:self.nodes_init])
        num_items_to_validate = self.num_items * 4
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_multiple_rebalance_in_out(self):
        servs_out = [self.servers[self.nodes_init - i - 1] for i in
                     range(self.nodes_out)]
        self.log.info("Servers Out: {0}".format(servs_out))
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()
        self.log.info('starting the initial load...')
        load_thread.start()
        load_thread.join()

        self.log.info('starting the load before rebalance in...')
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items)
        load_thread.start()

        # Add 1 node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 [])
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        num_items_to_validate = self.num_items * 3
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

        self.log.info('starting the load before rebalance out...')
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items * 3)

        load_thread.start()
        # Remove 1 node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], servs_out)
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        num_items_to_validate = self.num_items * 5
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

        self.log.info('starting the load before swap rebalance...')
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items * 5)

        load_thread.start()
        # Swap rebalance 1 node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 servs_out)
        rebalance.result()
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        num_items_to_validate = self.num_items * 7
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_start_stop_rebalance_multiple_times(self):
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()[0]
        load_thread = self.load_docs()

        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items)
        load_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 self.nodes_init:self.nodes_init + self.nodes_in],
                                                 [])
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        for i in range(1, 100):
            self.sleep(20)
            stopped = rest.stop_rebalance(wait_timeout=10)
            self.assertTrue(stopped, msg="Unable to stop rebalance in iteration {0}".format(i))
            self.sleep(10)
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], [], [])
            # rebalance.result()
            rest.monitorRebalance(stop_if_loop=False)
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        num_items_to_validate = self.num_items * 3
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

    def test_rebalance_in_with_indexer_node(self):
        rest = RestConnection(self.master)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [self.servers[self.nodes_init]],
                                                 [], ["index", "n1ql"])

        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        bucket = rest.get_buckets()[0]

        # Create Secondary index
        create_index_statement = "create index idx1 on default(body) using GSI"
        self.run_n1ql_query(create_index_statement)

        load_thread = self.load_docs()

        self.log.info('starting the load thread...')
        load_thread.start()
        load_thread.join()
        load_thread = self.load_docs(num_items=(self.num_items * 2),
                                     start_document=self.num_items)
        load_thread.start()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[
                                                 (self.nodes_init + 1):(self.nodes_init + self.nodes_in + 1)],
                                                 [])
        # rebalance.result()
        rest.monitorRebalance(stop_if_loop=False)
        load_thread.join()
        if self.flusher_batch_split_trigger:
            self.check_snap_start_corruption()
        num_items_to_validate = self.num_items * 3
        errors = self.check_data(self.master, bucket, num_items_to_validate)
        if errors:
            self.log.info("Missing keys count : {0}".format(len(errors)))
        # for error in errors:
        #     print error
        if num_items_to_validate != rest.get_active_key_count(bucket):
            self.fail(
                "FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                    format(num_items_to_validate,
                           rest.get_active_key_count(bucket)))
        if self.num_replicas > 0:
            self.assertEqual(num_items_to_validate,
                             (rest.get_replica_key_count(
                                 bucket) // self.num_replicas),
                             "Not all keys present in replica vbuckets. Expected No. of items : {0}, Item count per replica: {1}".format(
                                 num_items_to_validate, (
                                         rest.get_replica_key_count(
                                             bucket) // self.num_replicas)))

        # Fetch count of indexed documents
        query = "select count(body) from default where body is not missing"
        count = self.run_n1ql_query(create_index_statement)
        self.assertEqual(num_items_to_validate, count,
                         "Indexed document count not as expected. It is {0}, expected : {1}".format(count,
                                                                                                    num_items_to_validate))

    def run_n1ql_query(self, query):
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.n1ql_helper = N1QLHelper(shell=self.shell,
                                      max_verify=self.max_verify,
                                      buckets=self.buckets,
                                      item_flag=self.item_flag,
                                      n1ql_port=self.n1ql_port,
                                      full_docs_list=self.full_docs_list,
                                      log=self.log, input=self.input,
                                      master=self.master,
                                      use_rest=True
                                      )

        return self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)

    def run_view_queries(self):
        view_query_thread = Thread(target=self.view_queries, name="run_queries",
                                   args=(self.run_view_query_iterations,))
        return view_query_thread

    def view_queries(self, iterations):
        query = {"connectionTimeout": 60000}
        for count in range(iterations):
            for i in range(self.view_num):
                self.cluster.query_view(self.master, self.ddocs[0].name,
                                        self.default_view_name + str(i), query,
                                        expected_rows=None, bucket="default",
                                        retry_time=2)

    def create_ddocs_and_views(self):
        self.default_view = View(self.default_view_name, None, None)
        for bucket in self.buckets:
            for i in range(int(self.ddocs_num)):
                views = self.make_default_views(self.default_view_name,
                                                self.view_num,
                                                self.is_dev_ddoc,
                                                different_map=True)
                ddoc = DesignDocument(self.default_view_name + str(i), views)
                self.ddocs.append(ddoc)
                for view in views:
                    self.cluster.create_view(self.master, ddoc.name, view,
                                             bucket=bucket)