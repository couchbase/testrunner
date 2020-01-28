import logger
from lib.membase.api.rest_client import RestConnection
import time

class N1QLCallable:
    """
    The N1QL class to call when trying to do anything via executing n1ql queries

    Sample usage:

        from pytests.tuqquery.n1ql_callable import N1QLCallable
        n1ql_callable = N1QLCallable(self.servers)
        result = n1ql_callable.run_n1ql_query("select * from system:indexes")
        n1ql_callable.create_gsi_index(keyspace="bucket_name", name="test_idx1", fields="email", using="gsi", is_primary=False, index_condition="")
        n1ql_callable.drop_gsi_index(keyspace="default", name="test_idx1", is_primary=False)
    """

    def __init__(self, nodes):
        if not nodes:
            raise Exception("Need to specify at least 1 node.")
        self.nodes = nodes
        self.log = logger.Logger.get_logger()

    def run_n1ql_query(self, query="", node=None):
        if not node:
            node = self._find_n1ql_node()
        res = RestConnection(node).query_tool(query)
        return res

    def create_gsi_index(self, keyspace="", name="", fields="", using="gsi", is_primary=False, index_condition="", time_to_wait=30):
        index_not_exists = self.run_n1ql_query("select * from system:indexes where name='{0}' and keyspace_id='{1}'".format(name, keyspace))['metrics']['resultCount'] == 0
        if index_not_exists:
            self.log.info("creating index: {0} {1} {2}".format(keyspace, name, using))
            if is_primary:
                self.run_n1ql_query("CREATE PRIMARY INDEX ON `{0}` USING {1}".format(keyspace, using))
            else:
                if index_condition != '':
                    self.run_n1ql_query("CREATE INDEX {0} ON `{1}`({2}) WHERE {3}  USING {4}".format(name, keyspace, fields, index_condition, using))
                else:
                    self.run_n1ql_query("CREATE INDEX {0} ON `{1}`({2}) USING {3}".format(name, keyspace, fields, using))
            attempts = 0
            while attempts < time_to_wait:
                index_state = self.run_n1ql_query("select state from system:indexes where name='{0}' and keyspace_id='{1}'".format(name, keyspace))
                if index_state and index_state["results"] and index_state["results"][0]["state"] == "online":
                    break
                time.sleep(1)
                attempts = attempts + 1
        else:
            self.log.info("index {0} on `{1}` already exests".format(name, keyspace))

    def drop_gsi_index(self, name="", keyspace="", is_primary=False, time_to_wait=30):
        self.log.info("dropping index: {0}.{1}".format(keyspace, name))
        index_exists = self.run_n1ql_query("select * from system:indexes where name='{0}' and keyspace_id='{1}'".format(name, keyspace))['metrics']['resultCount'] == 1
        if index_exists:
            if is_primary:
                self._drop_primary_index(keyspace)
            else:
                self._drop_index(keyspace, name)
            attempts = 0
            while attempts < time_to_wait:
                index_exists = self.run_n1ql_query(
                    "select * from system:indexes where name='{0}' and keyspace_id='{1}'".format(name, keyspace))[
                                   'metrics']['resultCount'] == 1
                if not index_exists:
                    break
                time.sleep(1)
                attempts = attempts + 1
        else:
            self.log.info("index: {0}.{1} does not exist. Nothing to drop.".format(keyspace, name))

    def _find_n1ql_node(self):
        services_map = self._get_services_map()
        for node in self.nodes:
            key = node.ip+":"+node.port
            services = services_map[key]
            if "n1ql" in services:
                return node
        raise Exception("Cannot find any n1ql node!")

    def _get_services_map(self):
        rest = RestConnection(self.nodes[0])
        return rest.get_nodes_services()

    def _drop_index(self, bucket, index):
        self.run_n1ql_query("drop index `{0}`.`{1}`".format(bucket, index))

    def _drop_primary_index(self, bucket):
        self.run_n1ql_query("drop primary index on `{0}`".format(bucket))
