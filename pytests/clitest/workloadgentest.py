from clitest.cli_base import CliBaseTest
from membase.api.rest_client import RestConnection, Bucket

class workloadgenTests(CliBaseTest):

    def setUp(self):
        super(workloadgenTests, self).setUp()
        self.num_items = self.input.param("items", 1000)
        self.set_get_ratio = self.input.param("set_get_ratio", 0.9)
        self.item_size = self.input.param("item_size", 256)
        self.command_options = self.input.param("command_options", '')

    def tearDown(self):
        super(workloadgenTests, self).tearDown()

    def workloadgen_test(self):
        """We use cbworkloadgen to load items into cluster

        Verify the results based on the -i (number of items)
        and -r (set,get ratio) option information we have in
        command line and the actual item count in cluster"""

        for bucket in self.buckets:
            self.shell.execute_cbworkloadgen(self.couchbase_usrname, self.couchbase_password, self.num_items,
                                             self.set_get_ratio, bucket.name, self.item_size, self.command_options)

        self.verify_results()

    def verify_results(self):
        stats_tasks = []
        for bucket in self.buckets:
            stats_tasks.append(self.cluster.async_wait_for_stats(self.servers[:self.num_servers], bucket, '',
                                                                 'curr_items', '==', self.num_items))
            stats_tasks.append(self.cluster.async_wait_for_stats(self.servers[:self.num_servers], bucket, '',
                                                                 'vb_active_curr_items', '==', self.num_items))

        for task in stats_tasks:
            task.result(self.wait_timeout)