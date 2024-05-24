from .xdcrnewbasetests import XDCRNewBaseTest
from xdcr.mobile_sim_util import MobileSim



class xdcrMobileConvergence(XDCRNewBaseTest):
    def setUp(self):
        super(xdcrMobileConvergence, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()


    def load_with_async_ops(self):
        self.load_and_setup_xdcr()

        mobile_sim = MobileSim("172.23.104.168")
        mobile_sim.install_mobile_sim()
        mobile_sim_pids = []

        master_node = self.src_cluster.get_master_node()
        for bucket in self.src_cluster.get_buckets():
            self.log.info("Running simulator on cluster {} for bucket {}".format(master_node.ip,
                                                                                bucket.name))
            pid = mobile_sim.run_simulator(master_node.ip, master_node.rest_username,
                                        master_node.rest_password, bucket.name)
            mobile_sim_pids.append(pid)

        self.async_perform_update_delete()
        self.verify_results()

        for pid in mobile_sim_pids:
                mobile_sim.kill_simulator(pid)