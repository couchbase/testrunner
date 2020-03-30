from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator, JSONNonDocGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from .xdcrbasetests import XDCRReplicationBaseTest
from .esbasetests import ESReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection
from random import randrange


import time

#Assumption that at least 2 nodes on every cluster
class ESTests(XDCRReplicationBaseTest, ESReplicationBaseTest):
    def setUp(self):
        super(ESTests, self).setUp()
        self.setup_es_params(self)
        self.verify_dest_added()
        self.setup_doc_gens()

    def tearDown(self):
        super(ESTests, self).tearDown()

    def setup_doc_gens(self, template=None):
        # create json doc generators
        ordering = list(range(self.num_items/4))
        sites1 = ['google', 'bing', 'yahoo', 'wiki']
        sites2 = ['mashable', 'techcrunch', 'hackernews', 'slashdot']
        template = '{{ "ordering": {0}, "site_name": "{1}" }}'

        self.gen_create =\
            DocumentGenerator('es_xdcr_docs', template, ordering, sites1, start=0, end=self.num_items)

        self.gen_recreate =\
            DocumentGenerator('es_xdcr_docs', template, ordering, sites2, start=0, end=self.num_items)

        self.gen_delete =\
            DocumentGenerator('es_xdcr_docs', template, ordering,
                               sites1, start=0, end=self.num_items)


        self.gen_blob = BlobGenerator('loadOne', 'loadOne', self._value_size, end=self.num_items)

        self.gen_with_typedelimiter =\
            DocumentGenerator('es_xdcr_docs{0}{1}'.format(self.delimiter, self.default_type), template, ordering,
                               sites1, start=0, end=self.num_items)

        values = ['1', '10']
        self.gen_num =\
            JSONNonDocGenerator('es_xdcr_docs', values, start=0, end=self.num_items)

    def _async_modify_data(self, batch_size=1):
        tasks = []

        """Setting up creates/updates/deletes at source nodes"""
        if self._doc_ops is not None:
            # allows multiple of them but one by one
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_recreate, "create", self._expires, batch_size=batch_size))
            if "create" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_create, "create", 0, batch_size=batch_size))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0, batch_size=batch_size))
            if "read" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_create, "read", 0, batch_size=batch_size))
        return tasks

    def modify_data(self, batch_size=1):
        tasks = self._async_modify_data(batch_size=batch_size)
        [t.result() for t in tasks]


    #overriding xdcr verify results method for specific es verification
    def verify_results(self, verification_count=0, doc_type='couchbaseDocument'):
        self.verify_es_results(verification_count=verification_count, doc_type=doc_type)


    """Testing Unidirectional load( Loading only at source) Verifying whether ES/XDCR replication is successful on
    subsequent destination clusters.Create/Update/Delete operations are performed based on doc-ops specified by the user. """
    def load_with_async_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self._doc_ops = "update"
        self.modify_data(batch_size=1000)
        self.verify_results(verification_count=self.num_items)


    def test_plugin_connect(self):
        pass

    def test_multi_bucket_doctypes_with_async_ops(self):
        bucket_idx = 0
        buckets = self._get_cluster_buckets(self.src_master)

        tasks = []
        for bucket in buckets:
            if bucket_idx % 2 == 0:
                t = self._async_load_bucket(bucket, self.src_master, self.gen_create, "create", 0)
                tasks.append(t)
            else:
                t = self._async_load_bucket(bucket, self.src_master, self.gen_blob, "create", 0)
                tasks.append(t)
            bucket_idx = bucket_idx + 1

        for task in tasks:
           task.result()

    """Test coverage for elasticsearch and couchbase topology changes during data loading"""
    def test_topology(self):
        availableESNodes = self.dest_nodes[1:]
        availableCBNodes = self.src_nodes[1:]

        if self._es_in:
            task = self._async_rebalance(self.dest_nodes, [], availableESNodes)
            availableESNodes = []

        if self._cb_in:
            tasks = self._async_rebalance(self.src_nodes, [], availableCBNodes)
            [task.result() for task in tasks]
            availableCBNodes = []

        # load data
        tasks = \
            self._async_load_all_buckets(self.src_master, self.gen_create, "create", 0, batch_size=100)


        # peform initial rebalances
        if self._es_out or self._es_swap:
            availableESNodes = self._first_level_rebalance_out(self.dest_nodes,
                                                               availableESNodes,
                                                               monitor = False)
        elif self._es_in:
            availableESNodes = self._first_level_rebalance_in(self.dest_nodes,
                                                              monitor = False)

        if self._cb_out or self._cb_swap:
            availableCBNodes = self._first_level_rebalance_out(self.src_nodes,
                                                               availableCBNodes,
                                                               self._cb_failover)
        elif self._cb_in:
            availableCBNodes = self._first_level_rebalance_in(self.src_nodes)

        # wait for initial data loading and load some more data
        [task.result() for task in tasks]

        self._doc_ops = "update"
        tasks = self._async_modify_data(batch_size=100)

        # add/remove remaining nodes
        if self._es_out or self._es_swap:
            self._second_level_rebalance_out(self.dest_nodes,
                                             availableESNodes,
                                             self._es_swap,
                                             monitor = False)
        elif self._es_in:
            self._second_level_rebalance_in(self.dest_nodes, monitor = False)

        if self._cb_out or self._cb_swap:
            self._second_level_rebalance_out(self.src_nodes,
                                             availableCBNodes,
                                             self._cb_swap)
        elif self._cb_in:
            self._second_level_rebalance_in(self.src_nodes)

	    # wait for secondary data loading tasks and verify results
        [task.result() for task in tasks]
        self.verify_results(verification_count=self.num_items)

    def test_expiry(self):
        expiry = 10
        self._load_all_buckets(self.src_master, self.gen_create, "create", expiry)
        self.sleep(expiry + 5)
        self.verify_results(verification_count=0)

    def test_deletes(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
        self.verify_results(verification_count=0)

    def test_ignore_deletes(self):
        buckets = self._get_cluster_buckets(self.src_master)
        val = ""
        for b in buckets:
            if val == "":
                val = b
            else:
                val += ":{0}".format(b)
        config_commands = ['couchbase.ignoreDeletes: {0}'.format(b)]
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.verify_results(verification_count=self.num_items)
        self.update_configurations(config_commands)
        self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
        self.verify_results(verification_count=self.num_items)

    def test_wrap_counters(self):
        config_commands = ['couchbase.wrapCounters: True']
        self.update_configurations(config_commands)
        self._load_all_buckets(self.src_master, self.gen_num, "create", 0)
        self.verify_results(verification_count=(self.num_items))

    def test_ignore_failures(self):
        config_commands = ['couchbase.ignoreFailures: True']
        self.update_configurations(config_commands)
        self.upload_bad_template()
        self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
        self.verify_results(verification_count=self.num_items)
        self.upload_good_template()

    def test_delimiter_type_selector(self):
        config_commands = []
        config_commands.append('couchbase.typeSelector: org.elasticsearch.transport.couchbase.capi.DelimiterTypeSelector')
        config_commands.append('couchbase.typeSelector.documentTypeDelimiter: {0}'.format(self.delimiter))
        self.update_configurations(config_commands)
        self._load_all_buckets(self.src_master, self.gen_with_typedelimiter, "create", 0)
        self.verify_results(verification_count=self.num_items, doc_type=self.default_type)


    def test_regex_type_selector(self):
        config_commands = []
        config_commands.append('couchbase.typeSelector: org.elasticsearch.transport.couchbase.capi.RegexTypeSelector')
        config_commands.append('couchbase.typeSelector.{0}: {1}'.format(self.default_type, self.regex))
        self.update_configurations(config_commands)
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.verify_results(verification_count=self.num_items, doc_type=self.default_type)
        self.reset_configurations()


    def test_parent_child_mapping(self):
        pass

    def test_doc_routing(self):
        config_commands = []
        config_commands.append('couchbase.typeSelector: org.elasticsearch.transport.couchbase.capi.RegexTypeSelector')
        config_commands.append('couchbase.typeSelector.{0}: {1}'.format(self.default_type, self.regex))
        config_commands.append('couchbase.documentTypeRoutingFields.{0}: {1}'.format(self.default_type, 'site_name'))

        self.update_configurations(config_commands)
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.verify_results(verification_count=self.num_items, doc_type=self.default_type)
        self.reset_configurations()

    def test_doc_include_filter(self):
        config_commands = []
        config_commands.append('couchbase.keyFilter: org.elasticsearch.transport.couchbase.capi.RegexKeyFilter')
        config_commands.append('couchbase.keyFilter.type: Include')
        config_commands.append('couchbase.keyFilter.keyFiltersRegex.*: {0}'.format(self.regex))

        self.update_configurations(config_commands)
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.verify_results(verification_count=self.num_items)
        self.reset_configurations()

    def test_doc_exclude_filter(self):
        config_commands = []
        config_commands.append('couchbase.keyFilter: org.elasticsearch.transport.couchbase.capi.RegexKeyFilter')
        config_commands.append('couchbase.keyFilter.type: Exclude')
        config_commands.append('couchbase.keyFilter.keyFiltersRegex.*: {0}'.format(self.regex))
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.update_configurations(config_commands)
        self._load_all_buckets(self.src_master, self.gen_create, "delete", 0)
        self.verify_results(verification_count=self.num_items)
        self.reset_configurations()

    def test_alias_index(self):
        pass

    def test_pause_resume(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.verify_results(verification_count=self.num_items)
        self.replication_setting('pauseRequested', 'true')
        self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
        self.replication_setting('pauseRequested', 'false')
        self.verify_results(verification_count=0)

    def test_low_replication_streams(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.replication_setting('maxConcurrentReps', 2)
        self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
        self.verify_results(verification_count=0)

    def test_high_replication_streams(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.replication_setting('maxConcurrentReps', 256)
        self.verify_results(verification_count=self.num_items)
        self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
        self.verify_results(verification_count=0)

    def test_limit_connection(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.verify_results(verification_count=self.num_items)
        self.replication_setting('httpConnections', 1)
        self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
        self.verify_results(verification_count=0)

    def test_low_checkpoint_interval(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.verify_results(verification_count=self.num_items)
        self.replication_setting('checkpointInterval', 10)
        self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
        self.verify_results(verification_count=0)

    def test_longevity(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.verify_results(verification_count=self.num_items)
        self.sleep(self._run_time)
        self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
        self.verify_results(verification_count=0)