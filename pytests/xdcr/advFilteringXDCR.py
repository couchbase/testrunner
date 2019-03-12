import random
import time

from membase.api.rest_client import RestConnection
from scripts.edgyjson.main import JSONDoc

from xdcrnewbasetests import XDCRNewBaseTest


class XDCRAdvFilterTests(XDCRNewBaseTest):

    def setUp(self):
        super(XDCRAdvFilterTests, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
        initial_xdcr = self._input.param("initial_xdcr", False)
        if initial_xdcr:
            self.load_data()
            self.setup_xdcr()
        else:
            self.setup_xdcr()
            self.load_data()


    def tearDown(self):
        XDCRNewBaseTest.tearDown(self)

    def get_cluster_objects_for_input(self, input):
        """returns a list of cluster objects for input. 'input' is a string
           containing names of clusters separated by ':'
           eg. failover=C1:C2
        """
        clusters = []
        input_clusters = input.split(':')
        for cluster_name in input_clusters:
            clusters.append(self.get_cb_cluster_by_name(cluster_name))
        return clusters

    def load_data(self, server=None, bucket="default"):
        try:
            num_docs = self._input.param("items", 10)
            if not server:
                server = self.src_master.ip
            JSONDoc(server=server, username="Administrator", password="password",
                    bucket=bucket, startseqnum=random.randrange(1, 10000000, 1),
                    randkey=False, encoding="utf-8",
                    num_docs=num_docs, template="mix.json")
            self.sleep(30, "Waiting for docs to be loaded")
        except Exception as e:
            self.fail(
                "Errors encountered while loading data: {0}".format(e.message))

    def __execute_query(self, server, query):
        try:
            self.log.info("Executing {0} on {1}".format(query, server.ip))
            res = RestConnection(server).query_tool(query)
            if "COUNT" in query:
                return (int(res["results"][0]['$1']))
            else:
                return 0
        except Exception as e:
            self.fail(
                "Errors encountered while executing query {0} on {1} : {2}".format(query, server, e.message))

    def _create_index(self, server, bucket="default"):
        query_check_index_exists = "SELECT COUNT(*) FROM system:indexes " \
                                   "WHERE name='" + bucket + "_index'"
        if not self.__execute_query(server, query_check_index_exists):
            self.__execute_query(server, "CREATE PRIMARY INDEX " + bucket + "_index "
                                 + "ON " + bucket)

    def _get_doc_count(self, server, filter_exp, bucket="default"):
        doc_count = self.__execute_query(server, "SELECT COUNT(*) FROM "
                                         + bucket +
                                         " WHERE " + filter_exp)
        return doc_count if doc_count else 0

    def verify_results(self, replications):
        for repl in replications:
            # Assuming src and dest bucket of the replication have the same name
            bucket = repl['source']
            self._create_index(self.src_master, bucket)
            self._create_index(self.dest_master, bucket)
            if repl['filterExpression']:
                filter_exp = repl['filterExpression']
            src_count = self._get_doc_count(self.src_master, filter_exp)
            dest_count = self._get_doc_count(self.dest_master, filter_exp)
            if src_count != dest_count:
                self.fail("Doc count {0} on {1} does not match "
                          "doc count {2} on {3} "
                          "after applying filter {4}"
                          .format(src_count, self.src_master.ip,
                                  dest_count, self.dest_master.ip,
                                  filter_exp))
            self.log.info("Doc count {0} on {1} matches "
                          "doc count {2} on {3} "
                          "after applying filter {4}"
                          .format(src_count, self.src_master.ip,
                                  dest_count, self.dest_master.ip,
                                  filter_exp))

    def test_xdcr_with_filter(self):
        tasks = []
        rebalance_in = self._input.param("rebalance_in", None)
        rebalance_out = self._input.param("rebalance_out", None)
        swap_rebalance = self._input.param("swap_rebalance", None)
        failover = self._input.param("failover", None)
        graceful = self._input.param("graceful", None)
        pause = self._input.param("pause", None)
        reboot = self._input.param("reboot", None)

        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.pause_all_replications()

        if rebalance_in:
            for cluster in self.get_cluster_objects_for_input(rebalance_in):
                tasks.append(cluster.async_rebalance_in())
                for task in tasks:
                    task.result()

        if failover:
            for cluster in self.get_cluster_objects_for_input(failover):
                cluster.failover_and_rebalance_nodes(graceful=graceful,
                                                     rebalance=True)

        if rebalance_out:
            tasks = []
            for cluster in self.get_cluster_objects_for_input(rebalance_out):
                tasks.append(cluster.async_rebalance_out())
                for task in tasks:
                    task.result()

        if swap_rebalance:
            tasks = []
            for cluster in self.get_cluster_objects_for_input(swap_rebalance):
                tasks.append(cluster.async_swap_rebalance())
                for task in tasks:
                    task.result()

        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.get_remote_clusters():
                    remote_cluster_refs.resume_all_replications()

        if reboot:
            for cluster in self.get_cluster_objects_for_input(reboot):
                cluster.warmup_node()
            time.sleep(60)

        self.sleep(30)
        self.perform_update_delete()

        rdirection = self._input.param("rdirection", "unidirection")
        replications = self.src_rest.get_replications()
        self.verify_results(replications)
        if rdirection == "bidirection":
            self.load_data(server=self.dest_master.ip)
            replications = self.dest_rest.get_replications()
            self.verify_results(replications)
