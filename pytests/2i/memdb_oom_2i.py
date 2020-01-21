import logging
import random

from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from .base_2i import BaseSecondaryIndexingTests

log = logging.getLogger(__name__)
QUERY_TEMPLATE = "SELECT {0} FROM %s "

class SecondaryIndexMemdbOomTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexMemdbOomTests, self).setUp()
        self.indexMemQuota = self.input.param("indexMemQuota", 256)
        query_template = QUERY_TEMPLATE
        self.query_template = query_template.format("job_title")
        self.doc_ops = self.input.param("doc_ops", True)
        self.initial_index_number = self.input.param("initial_index_number", 2)
        self.oomServer = self.get_nodes_from_services_map(service_type="index")
        self.whereCondition= self.input.param("whereCondition", " job_title != \"Sales\" ")
        self.query_template += " WHERE {0}".format(self.whereCondition)
        rest = RestConnection(self.oomServer)
        if self.indexMemQuota > 256:
            log.info("Setting indexer memory quota to {0} MB...".format(self.indexMemQuota))
            rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=self.indexMemQuota)
            self.sleep(30)
        self.deploy_node_info = ["{0}:{1}".format(self.oomServer.ip, self.oomServer.port)]
        self.load_query_definitions = []
        for x in range(self.initial_index_number):
            index_name = "index_name_"+str(x)
            query_definition = QueryDefinition(
                index_name=index_name, index_fields=["VMs"],
                query_template=self.query_template, groups=["simple"])
            self.load_query_definitions.append(query_definition)
        self.multi_create_index(buckets=self.buckets,
                                query_definitions=self.load_query_definitions,
                                deploy_node_info=self.deploy_node_info)
        log.info("Setting indexer memory quota to 256 MB...")
        rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=256)
        self.sleep(30)

    def tearDown(self):
        super(SecondaryIndexMemdbOomTests, self).tearDown()

    def test_indexer_oom(self):
        """
        1. Get indexer in OOM.
        2. Validate if the status of all the indexes on that indexer is Paused from index Status.
        3. Validate if the indexer state is Paused from index Stats
        :return:
        """
        self._push_indexer_off_the_cliff()
        self.assertTrue(self._validate_index_status_oom(), "Indexes ain't in Paused State")
        self.assertTrue(self._validate_indexer_status_oom(), "Indexer Status isn't Paused")

    def test_plasma_oom_for_disable_persistence(self):
        self.multi_drop_index(query_definitions=self.load_query_definitions)
        doc = {"indexer.plasma.disablePersistence": True}
        self.rest.set_index_settings(doc)
        self.sleep(15)
        self.multi_create_index(buckets=self.buckets,
                                query_definitions=self.load_query_definitions,
                                deploy_node_info=self.deploy_node_info)
        self.multi_create_index(buckets=self.buckets,
                                query_definitions=self.query_definitions,
                                deploy_node_info=self.deploy_node_info)
        self._push_indexer_off_the_cliff()
        self.assertTrue(self._validate_index_status_oom(), "Indexes ain't in Paused State")
        self.assertTrue(self._validate_indexer_status_oom(), "Indexer Status isn't Paused")

    def test_oom_increase_mem_quota(self):
        """
        1. Get OOM
        2. Drop Already existing indexes
        3. Verify if state of indexes is changed to ready
        :return:
        """
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        indexer_memQuota = self.get_indexer_mem_quota()
        log.info("Current Indexer Memory Quota is {}".format(indexer_memQuota))
        for cnt in range(5):
            indexer_memQuota += 200
            log.info("Increasing Indexer Memory Quota to {0}".format(indexer_memQuota))
            rest = RestConnection(self.oomServer)
            rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=indexer_memQuota)
            self.sleep(120)
            indexer_oom = self._validate_indexer_status_oom()
            if not indexer_oom:
                break
        self.assertFalse(self._validate_indexer_status_oom(), "Indexer still in OOM")
        self.sleep(60)
        log.info("=== Indexer out of OOM ===")
        self._verify_bucket_count_with_index_count(self.load_query_definitions)
        self.multi_query_using_index(buckets=self.buckets,
                    query_definitions=self.load_query_definitions)

    def test_oom_drop_indexes(self):
        """
        1. Get OOM
        2. Drop Already existing indexes
        3. Verify if state of indexes is changed to ready
        :return:
        """
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        for i in range(len(self.load_query_definitions)):
            for bucket in self.buckets:
                log.info("Dropping {0} from bucket {1}".format(self.load_query_definitions[i].index_name, bucket.name))
                self.drop_index(bucket=bucket, query_definition=self.load_query_definitions[i])
                self.sleep(10)
            check = self._validate_indexer_status_oom()
            if not check:
                log.info("Indexer out of OOM...")
                self.load_query_definitions = self.load_query_definitions[i+1:]
                break
        self.sleep(60)
        self.assertFalse(self._validate_indexer_status_oom(), "Indexer still in OOM")
        self._verify_bucket_count_with_index_count(self.load_query_definitions)
        self.multi_query_using_index(buckets=self.buckets,
                                     query_definitions=self.load_query_definitions)

    def test_oom_flush_bucket(self):
        """
        1. Get OOM
        2. Flush a bucket
        3. Verify if state of indexes is changed
        :return:
        """
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        rest = RestConnection(self.oomServer)
        for bucket in self.buckets:
            log.info("Flushing bucket {0}...".format(bucket.name))
            rest.flush_bucket(bucket)
            self.sleep(120)
            if not self._validate_indexer_status_oom():
                log.info("Indexer out of OOM...")
                break
        self.sleep(60)
        check_for_oom = self._validate_indexer_status_oom()
        count = 0
        while check_for_oom and count < 15:
            self.sleep(60)
            check_for_oom = self._validate_indexer_status_oom()
            count += 1
        if count == 15:
            self.assertFalse(self._validate_indexer_status_oom(), "Indexer still in OOM")
        self._verify_bucket_count_with_index_count(self.load_query_definitions)
        self.multi_query_using_index(buckets=self.buckets,
                    query_definitions=self.load_query_definitions, verify_results=False)

    def test_oom_delete_bucket(self):
        """
        1. Get OOM
        2. Delete a bucket
        3. Verify if state of indexes is changed
        :return:
        """
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        for i in range(len(self.buckets)):
            log.info("Deleting bucket {0}...".format(self.buckets[i].name))
            BucketOperationHelper.delete_bucket_or_assert(serverInfo=self.oomServer, bucket=self.buckets[i].name)
            self.sleep(60)
            check = self._validate_indexer_status_oom()
            if not check:
                if i < len(self.buckets):
                    self.buckets = self.buckets[i+1:]
                else:
                    #TODO: Pras: Need better solution here
                    self.buckets = []
                break
            log.info("Indexer Still in OOM...")
        self.sleep(60)
        self.assertFalse(self._validate_indexer_status_oom(), "Indexer still in OOM")
        self._verify_bucket_count_with_index_count(self.load_query_definitions)
        self.multi_query_using_index(buckets=self.buckets,
                        query_definitions=self.load_query_definitions)

    def test_oom_kv_rebalance_in(self):
        """
        1. Get indexer in OOM
        2. Rebalance a kv node in.
        3. Get indexer out of OOM.
        4. Query for indexes.
        5. Validate if queries giving correct result.
        :return:
        """
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.nodes_in_list, [], services = ["kv"])
        rebalance.result()
        self._bring_indexer_back_to_life()
        self.sleep(60)
        self._verify_bucket_count_with_index_count(self.load_query_definitions)
        self.multi_query_using_index(buckets=self.buckets,
                    query_definitions=self.load_query_definitions)

    def test_oom_kv_rebalance_out(self):
        """
        1. Get indexer in OOM
        2. Rebalance a kv node out.
        3. Get indexer out of OOM.
        4. Query for indexes.
        5. Validate if queries giving correct result.
        :return:
        """
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)[1]
        log.info("Rebalancing KV node {ip} out...".format(ip=kv_node.ip))
        rebalance = self.cluster.async_rebalance(self.servers, [], [kv_node])
        rebalance.result()
        self._bring_indexer_back_to_life()
        self.sleep(60)
        self._verify_bucket_count_with_index_count(self.load_query_definitions)
        self.multi_query_using_index(buckets=self.buckets, query_definitions=self.load_query_definitions)

    def test_oom_kv_restart(self):
        """
        1. Get indexer to OOM.
        2. Stop Couchbase on one of the KV nodes.
        3. Get indexer out of OOM.
        4. Query - Should Fail
        5. Start Couchbase on that KV node.
        6. Query - Should pass
        :return:
        """
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)[1]
        log.info("Stopping Couchbase on {0}".format(kv_node.ip))
        remote = RemoteMachineShellConnection(kv_node)
        remote.stop_server()
        for i in range(len(self.load_query_definitions)):
            for bucket in self.buckets:
                log.info("Dropping {0} from bucket {1}".format(self.load_query_definitions[i].index_name, bucket.name))
                self.drop_index(bucket=bucket, query_definition=self.load_query_definitions[i])
                self.sleep(120)
            check = self._validate_indexer_status_oom()
            if not check:
                log.info("Indexer out of OOM...")
                self.load_query_definitions = self.load_query_definitions[i+1:]
                break
            self.sleep(60)
        try:
            self._verify_bucket_count_with_index_count(self.load_query_definitions)
            self.multi_query_using_index(buckets=self.buckets,
                                                      query_definitions=self.load_query_definitions)
        except Exception as ex:
            log.info(str(ex))
        finally:
            log.info("Starting Couchbase on {0}".format(kv_node.ip))
            remote.start_server()
            self.sleep(60)
            self._verify_bucket_count_with_index_count(self.load_query_definitions)
            self.multi_query_using_index(buckets=self.buckets, query_definitions=self.load_query_definitions)

    def test_oom_indexer_reboot(self):
        """
        1. OOM
        2. Reboot Indexer
        :return:
        """
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        log.info("Rebooting {0}".format(self.oomServer.ip))
        self._reboot_node(self.oomServer)
        check = self._validate_indexer_status_oom()
        if check:
            log.info("Indexer in OOM after reboot...")
            self._bring_indexer_back_to_life()
        self.sleep(60)
        self._verify_bucket_count_with_index_count(self.load_query_definitions)
        self.multi_query_using_index(buckets=self.buckets, query_definitions=self.load_query_definitions)

    def test_oom_reduce_mem_quota(self):
        """
        1. Build indexes without hitting OOM.
        2. Reduce memory quota of indexer.
        3. Check if indexer hits OOM
        :return:
        """
        indexer_memQuota = self.get_indexer_mem_quota()
        log.info("Current Indexer Memory Quota is {}".format(indexer_memQuota))
        rest = RestConnection(self.oomServer)
        count = 0
        while count < 5:
            used_memory = self.get_indexer_mem_quota()
            #Setting memory to 90 % of used memory.
            set_memory = int(used_memory) * 90//100
            rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=set_memory)
            self.sleep(120)
            check = self._validate_indexer_status_oom()
            if check:
                log.info("Indexer is Paused after setting memory quota to {0}".format(set_memory))
                break
        if count == 5:
            self.assertFalse(self._validate_indexer_status_oom(), "Indexer still in OOM")

    def test_change_mem_quota_when_index_building(self):
        rest = RestConnection(self.oomServer)
        log.info("Setting indexer memory quota to 700 MB...")
        rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=700)
        self.sleep(30)
        query_definitions = []
        for x in range(3):
            index_name = "index_"+str(x)
            query_definition = QueryDefinition(index_name=index_name, index_fields=["job_title"],
                        query_template=self.query_template, groups=["simple"])
            query_definitions.append(query_definition)
        create_tasks = []
        build_tasks = []
        index_info = {}
        for bucket in self.buckets:
            if not bucket in list(index_info.keys()):
                index_info[bucket] = []
            for query_definition in query_definitions:
                index_info[bucket].append(query_definition.index_name)
                task = self.async_create_index(bucket.name, query_definition)
                create_tasks.append(task)
        for task in create_tasks:
            task.result()
        if self.defer_build:
            log.info("Building Indexes...")
            for key, val in index_info.items():
                task = self.async_build_index(bucket=key, index_list=val)
                build_tasks.append(task)
        self.sleep(10)
        log.info("Setting indexer memory quota to 500 MB...")
        rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=500)
        self.sleep(30)
        for task in build_tasks:
            task.result()

    def test_oom_query_replica_index(self):
        """
        Configuration: kv:n1ql, kv, index, index
        1. Achieve OOM on one of the indexer nodes.
        2. Create replica index on second indexer node at the same time.
        3. Query for the replica indexes, which shouldn't fail.
        :return:
        """
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[1]
        deploy_node_info = ["{0}:{1}".format(index_node.ip, index_node.port)]
        index_name = "replica_index"
        replica_definition = QueryDefinition(index_name=index_name, index_fields=["job_title"],
                        query_template=self.query_template, groups=["simple"])
        self.create_index(self.buckets[0].name, replica_definition, deploy_node_info)
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        self.multi_query_using_index(buckets=self.buckets,
                    query_definitions=[replica_definition], verify_results=False)

    def test_oom_create_build_index(self):
        """
        Create an
        :return:
        """
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        index_name = "oom_index"
        query_definition = QueryDefinition(index_name=index_name, index_fields=["join_mo"],
                                           query_template="", groups=["simple"])
        try:
            self.create_index(self.buckets[0].name, query_definition, self.deploy_node_info)
        except Exception as ex:
            log.info("{0}".format(str(ex)))

    def test_oom_create_index(self):
        """
        Create indexes
        :return:
        """
        self.defer_build = False
        self.assertTrue(self._push_indexer_off_the_cliff(), "OOM Can't be achieved")
        index_name = "oom_index"
        query_definition = QueryDefinition(index_name=index_name, index_fields=["join_mo"],
                                           query_template="", groups=["simple"])
        try:
            task = self.async_create_index(self.buckets[0].name, query_definition)
            task.result()
        except Exception as ex:
            log.info("Cannot Create Index om Paused Indexer as expected")
            log.info("{0}".format(str(ex)))

    def _push_indexer_off_the_cliff(self):
        """
        Internal Method to create OOM scenario
        :return:
        """
        cnt = 0
        docs = 50
        while cnt < 10:
            if self._validate_indexer_status_oom():
                log.info("OOM on {0} is achieved".format(self.oomServer.ip))
                return True
            for task in self.kv_mutations(docs):
                task.result()
            self.sleep(30)
            cnt += 1
            docs += 50
        return False

    def _bring_indexer_back_to_life(self):
        """
        Private method to bring the indexer out of OOM.
        This is achieved by flushing a bucket
        :return:
        """
        rest = RestConnection(self.oomServer)
        log.info("Bringing Indexer out of OOM by dropping indexes.")
        for i in range(len(self.load_query_definitions)):
            for bucket in self.buckets:
                self.drop_index(bucket=bucket, query_definition=self.load_query_definitions[i])
                self.sleep(60)
            if not self._validate_indexer_status_oom():
                if i < len(self.load_query_definitions):
                    self.load_query_definitions = self.load_query_definitions[i+1:]
                else:
                    self.load_query_definitions = []
                break
        log.info("Setting indexer memory quota to 500 MB...")
        rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=500)
        self.sleep(180)
        self.assertFalse(self._validate_indexer_status_oom(), "Indexer is still in OOM")

    def _validate_index_status_oom(self):
        """
        Verifies if indexer is Online
        :return:
        """
        host = "{0}:8091".format(self.oomServer.ip)
        rest = RestConnection(self.oomServer)
        index_status = rest.get_index_status()
        for index in index_status.values():
            for vals in index.values():
                if vals["status"].lower() != "paused":
                    if vals["hosts"] == host:
                        return False
            return True

    def _validate_indexer_status_oom(self):
        """
        Verifies if indexer is OOM
        :return:
        """
        rest = RestConnection(self.oomServer)
        index_stats = rest.get_indexer_stats()
        if index_stats["indexer_state"].lower() == "paused":
            return True
        else:
            return False

    def kv_mutations(self, docs=1):
        if not docs:
            docs = self.docs_per_day
        gens_load = self.generate_docs(docs)
        self.full_docs_list = self.generate_full_docs_list(gens_load)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        tasks = self.async_load(generators_load=gens_load, op_type="create",
                                batch_size=self.batch_size)
        return tasks

    def get_indexer_mem_quota(self):
        """
        Sets Indexer memory Quota
        :param memQuota:
        :return:
        int indexer memory quota
        """
        rest = RestConnection(self.oomServer)
        content = rest.cluster_status()
        return int(content['indexMemoryQuota'])

    def _reboot_node(self, node):
        self.log.info("Rebooting node '{0}'....".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        if shell.extract_remote_info().type.lower() == 'windows':
            o, r = shell.execute_command("shutdown -r -f -t 0")
        elif shell.extract_remote_info().type.lower() == 'linux':
            o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        # wait for restart and warmup on all node
        self.sleep(self.wait_timeout * 5)
        # disable firewall on these nodes
        self.stop_firewall_on_node(node)
        # wait till node is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self, wait_if_warmup=True)
