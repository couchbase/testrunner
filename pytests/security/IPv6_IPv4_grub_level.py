import logger
log = logger.Logger.get_logger()
from remote.remote_util import RemoteMachineShellConnection
import time
from membase.api.rest_client import RestConnection

class IPv6_IPv4():

    """
    pure_ipv4 - IPv6 is disabled at grub level on all nodes
    pure_ipv6 - Nodes with just IPv6 stack enabled on all nodes
    hostname  -
    ipv4_ipv6 - Dual Stack present on all nodes

    First item in list denotes - cluster setting IPv4 and the second item denotes - cluster setting IPv6
    """
    port_map = {
                 "pure_ipv4":{"ipv4":[True,False],"ipv6":["NA","NA"]},
                 "pure_ipv6": {"ipv4":["NA","NA"], "ipv6":[False,True]},
                 "hostname" : {"ipv4":[True,"NA"],"ipv6":["NA",True]},
                 "ipv4_ipv6": {"ipv4":[True,"NA"],"ipv6":["NA",True]}
     }

    def __init__(self,servers):
        self.servers = servers

    def transfer_file(self,src):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.copy_file_local_to_remote(src,"/tmp/block_ports.py")
            shell.disconnect()

    def delete_files(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.delete_file("/tmp/","block_ports.py")
            shell.delete_file("/tmp/","block_ports_out")
            shell.disconnect()

    def upgrade_addr_family(self,addr_family):
        """
        Assumes that the /etc/hosts file (on a centos machine) has the following entry format
            IPv4-Address1 Hostname1
            IPv4-Address2 Hostname2
            IPv4-Address3 Hostname3
        These lines are commented for IPv6 Cluster family and uncommented for IPv4 Cluster family
        """
        current_setting = self.get_addr_family()
        # if current_setting == addr_family:
        #     log.info("In the same address family.")
        #     return
        log.info("{0}".format(current_setting))
        if current_setting != "ipv4" and current_setting != "ipv6":
            log.info("Cluster in mixed mode. Exiting.")
            return
        if addr_family == "ipv6":
            log.info("Changing cluster to IPv6 mode.")
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                shell.execute_command("sed -i '/^172./s/^/#/g' /etc/hosts")
                shell.disconnect()
            self.set_addr_family("ipv6")
        elif addr_family == "ipv4":
            log.info("Changing cluster to IPv4 mode.")
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                shell.execute_command("sed -i '/172./s/^#//g' /etc/hosts")
                shell.disconnect()
            self.set_addr_family("ipv4")

    def get_addr_family(self):
        master = self.servers[0]
        shell = RemoteMachineShellConnection(master)
        options = "-c {0} -u {1} -p {2} --get".format(master.ip,"Administrator","password")
        output,error = shell.couchbase_cli("ip-family",master.ip,options)
        log.info("OUTPUT {0} ERROR {1}".format(output,error))
        shell.disconnect()
        if output[0][-4:] == "ipv4" or output[0][-4:] == "ipv6":
            for server in self.servers:
                server.addr_family = output[0][-4:]
            return output[0][-4:]
        else:
            return None

    def set_addr_family(self,addr_family):
        self.disable_autofailover()
        master = self.servers[0]
        shell = RemoteMachineShellConnection(master)
        options = "-c {0} -u {1} -p {2} --set --{3}".format(master.ip, "Administrator", "password",addr_family)
        output, error = shell.couchbase_cli("ip-family", master.ip, options)
        log.info("OUTPUT {0} ERROR {1}".format(output, error))
        for server in self.servers:
            server.addr_family = addr_family
        shell.disconnect()

    def disable_autofailover(self):
        """
        Disable AutoFailover before setting ip family using rest api
        """
        for server in self.servers:
            rest = RestConnection(server)
            rest.update_autofailover_settings(False, 120)

    def disable_IPV6_grub_level(self):
        """
        Disable IPV6 at grub level for all nodes in the cluster
        """
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command(
                '''sed -i 's/ipv6.disable=0 //; s/ipv6.disable=1 //; s/GRUB_CMDLINE_LINUX="/GRUB_CMDLINE_LINUX="ipv6.disable=1 /' /etc/default/grub''')
            shell.execute_command("grub2-mkconfig -o /boot/grub2/grub.cfg")
            shell.reboot_node()
            time.sleep(10)
            shell = RemoteMachineShellConnection(server)
            output, error = shell.execute_command("ifconfig | grep inet6")
            if output == [] and error == []:
                log.info("IPv6 Successfully Disabled for {0}".format(server.ip))
            else:
                log.info("Cant disable IPv6")
                log.info("Output message is {0} and error message is {1}".format(output, error))
            output, error = shell.execute_command("iptables -F")
            shell.disconnect()

    def enable_IPV6_grub_level(self):
        """
        Enable IPV6 at grub level for all nodes in the cluster
        """
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command("sed -i 's/ipv6.disable=1/ipv6.disable=0/' /etc/default/grub")
            shell.execute_command("grub2-mkconfig -o /boot/grub2/grub.cfg")
            shell.reboot_node()
            time.sleep(10)
            shell = RemoteMachineShellConnection(server)
            output, error = shell.execute_command("ifconfig | grep inet6")
            if output == []:
                log.info("Cant enable IPv6")
                log.info("Output message is {0} and error message is {1}".format(output, error))
            elif output != []:
                log.info("IPv6 Successfully Enabled for {0}".format(server.ip))
            output, error = shell.execute_command("iptables -F")
            shell.disconnect()

    def check_ports(self, ports, addr_family):
        """
        Check netstat output to see if ports are listening on given address family
        """
        listening = True
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            for port in ports:
                if addr_family == "ipv6":
                    cmd = "lsof -nP -i6TCP:{0} -sTCP:LISTEN".format(port)
                elif addr_family == "ipv4":
                    cmd = "lsof -nP -i4TCP:{0} -sTCP:LISTEN".format(port)
                output, error = shell.execute_command(cmd)
                log.info("OUTPUT {0} ERROR {0}".format(output,error))
                if output == [] and error != []:
                    log.info("{0} is not listening on {1} on {2}".format(server.ip,port,addr_family))
                    listening = False
                else:
                    log.info("{0} is listening on {1} on {2}".format(server.ip,port,addr_family))
        shell.disconnect()
        return listening

    def block_ports(self,ports,addr_family):
        pids = []
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.stop_couchbase()
            output = [""]
            while output!=[]:
                time.sleep(10)
                cmd = "lsof -nP -iTCP:{0} -sTCP:LISTEN".format(ports[0])
                output, error = shell.execute_command(cmd)
                log.info(f"Output of command is {output} and error is {error}")

            for port in ports:
                cmd = "nohup python3 /tmp/block_ports.py {0} {1} > /tmp/block_ports_out 2>&1 & echo $!".format(port, addr_family)
                output, error = shell.execute_command(cmd)
                log.info("OUTPUT : {0} ERROR : {1}".format(output,error))
                pids.append(output[0])
            shell.start_couchbase()
            time.sleep(120)
        shell.disconnect()
        return pids

    def unblock_ports(self,pids):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            cmd = "kill -9 {0}".format(' '.join(pids))
            output, error = shell.execute_command(cmd)
            log.info("OUTPUT : {0} ERROR : {1}".format(output, error))
            shell.disconnect()

    def retrieve_port_map(self):
        return self.port_map

if __name__ == "__main__":
    pass