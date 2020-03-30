# EPerf tests with mongoDB.

import pymongo
from . import eperf
from perf_engines import mcsoda
from perf_engines import mcsoda_mongo

CONFIGSVR_PORT = 27019 # Needs to match the ports defined by scripts/install.py
SHARDSVR_PORT  = 27018
MONGOS_PORT    = 27017

class EPerfMasterMongo(eperf.EPerfMaster):

    def setUpBase1(self):
        self.is_leader = self.parami("prefix", 0) == 0

        pass # Mongo has no vbuckets.

    def set_up_rest(self):
        pass # There's no REST for couchbase when using mongo.

    def start_stats(self, test_name, servers=None,
                    process_names=['mongod', 'mongos'],
                    test_params = None, client_id = '',
                    collect_server_stats = True):
        return None

    def admin_db(self):
        master = self.input.servers[0]
        self.log.info("Connecting pymongo: {0}:{1}".format(master.ip, MONGOS_PORT))
        conn = pymongo.Connection(master.ip, MONGOS_PORT)
        self.log.info(conn)
        self.log.info(conn['admin'])
        return conn, conn['admin']

    def set_up_cluster(self):
        master = self.input.servers[0]

        conn, admin = self.admin_db()
        try:
            admin.command("addshard", master.ip + ":" + str(SHARDSVR_PORT),
                          allowLocal=True)
            admin.command("enablesharding", "default") # The shard key defaults to "_id".
        except Exception as ex:
            self.log.error(ex)

        conn.disconnect()

    def tear_down_cluster(self):
        pass # TODO.

    def set_up_buckets(self):
        conn, admin = self.admin_db()
        try:
            admin.command("shardcollection", "default.default",
                          key={"_id": 1}) # The shard key defaults to "_id".
        except Exception as ex:
            self.log.error(ex)

        conn.disconnect()

    def tear_down_buckets(self):
        pass # TODO.

    def set_up_proxy(self, bucket=None):
        pass

    def tear_down_proxy(self):
        pass # TODO.

    def set_up_dgm(self):
        pass # TODO - need DGM for mongo.

    def wait_until_drained(self):
        pass # TODO.

    def wait_until_warmed_up(self):
        pass # TODO.

    def nodes(self, num_nodes):
        conn, admin = self.admin_db()

        i = 1
        while i < num_nodes and i < len(self.input.servers):
            try:
                x = self.input.servers[i].ip + ":" + str(SHARDSVR_PORT)
                admin.command("addshard", x)
            except Exception as ex:
                self.log.error(ex)

            i = i + 1

        conn.disconnect()

    def mk_protocol(self, host):
        return "mongo://" + host + ":" + str(MONGOS_PORT)

    def mcsoda_run(self, cfg, cur, protocol, host_port, user, pswd,
                   stats_collector = None, stores = None, ctl = None,
                   heartbeat=None, why = None, bucket=None):
        return mcsoda.run(cfg, cur, protocol, host_port, user, pswd,
                          stats_collector=stats_collector,
                          stores=[mcsoda_mongo.StoreMongo()],
                          ctl=ctl)


class EPerfClientMongo(EPerfMasterMongo):

    def setUp(self):
        self.dgm = False
        self.is_master = False
        self.level_callbacks = []
        self.latched_rebalance_done = False
        self.setUpBase0()
        self.is_leader = self.parami("prefix", 0) == 0

        pass # Skip super's setUp().  The master should do the real work.

    def tearDwon(self):
        pass # Skip super's tearDown().  The master should do the real work.
