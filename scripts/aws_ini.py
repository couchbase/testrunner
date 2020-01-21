#!/usr/bin/env python
import sys
from optparse import OptionParser

from boto.ec2 import regions
from boto.exception import NoAuthHandlerFound


class AwsIni:
    """Auto-generated resource files (*.ini) for EC2 instances
    """

    def __init__(self):
        try:
            self.connections = [region.connect() for region in regions()]
        except NoAuthHandlerFound:
            print("ERROR: use environment variables to set credentials:\n" + \
                  "AWS_ACCESS_KEY_ID\nAWS_SECRET_ACCESS_KEY")
            sys.exit()

    def parse_args(self):
        usage = "usage: %prog [options] server-tag-value [client-tag-value]" + \
                "\n\nExample: %prog --clusters=2 --clients=6 " + \
                "--server-tag=Class --client-tag=Class --username root " + \
                "--password couchbase xperf-node xperf-client\n\n" + \
                "Use environment variables to set credentials:\n" + \
                "AWS_ACCESS_KEY_ID\nAWS_SECRET_ACCESS_KEY"

        parser = OptionParser(usage)
        parser.add_option("--clusters", dest="clusters",
                          help="use clusters section in output file",
                          metavar="2", default=0, type="int")
        parser.add_option("--clients", dest="clients",
                          help="use clients section in output file",
                          metavar="6", default=0, type="int")
        parser.add_option("--server-tag", dest="server_tag",
                          help="use this tag to group servers by",
                          metavar="Name", default='Name')
        parser.add_option("--client-tag", dest="client_tag",
                          help="use this tag to group clients by",
                          metavar="Name", default='Name')
        parser.add_option("--username", dest="username",
                          help="SSH username",
                          metavar="ec2-user", default='ec2-user')
        parser.add_option("--ssh-key", dest="ssh_key",
                          help="SSH public key",
                          metavar="/root/key.pem", default='/root/key.pem')
        parser.add_option("--password", dest="password",
                          help="SSH password (suppress ssh key if specified)",
                          metavar="couchbase", default='')

        self.options, self.args = parser.parse_args()

        if len(self.args) not in list(range(1, 3)):
            parser.print_help()
            sys.exit()

    def _get_instances(self):
        for connection in self.connections:
            for reservation in connection.get_all_instances():
                for instance in reservation.instances:
                    yield instance

    def _get_servers(self, kind='node'):
        if kind == 'node':
            tag_key = self.options.server_tag
            tag_value = self.args[0]
        else:
            tag_key = self.options.client_tag
            tag_value = self.args[1]

        for instance in self._get_instances():
            if (instance.tags.get(tag_key) == tag_value and
                    instance.state == "running"):
                yield instance

    def _print_header(self):
        print("[global]")
        print("username:{0}".format(self.options.username))
        if self.options.password:
            print("password:{0}".format(self.options.password))
        else:
            print("ssh_key:{0}".format(self.options.ssh_key))
        print("port:8091")
        print("data_path:/data")
        print("index_path:/data2")

    def _print_clusters(self):
        for cluster_id in range(self.options.clusters):
            print("\n[cluster{0}]".format(cluster_id + 1))

            servers = list(self._get_servers())
            cluster_quota = len(servers) // self.options.clusters
            left_index = cluster_quota * cluster_id
            right_index = cluster_quota * (cluster_id + 1)

            for (i, server) in enumerate(servers[left_index:right_index],
                                         start=1):
                print("{0}:{1}".format(i, server.public_dns_name))

    def _print_servers(self):
        print("\n[servers]")
        for (i, server) in enumerate(self._get_servers(kind='node'), start=1):
            print("{0}:{1}".format(i, server.public_dns_name))
            print("#{0}".format(server.private_ip_address))

    def _print_clients(self):
        print("\n[clients]")
        for (i, client) in enumerate(self._get_servers(kind='client'), start=1):
            if i > self.options.clients:
                break
            else:
                print("{0}:{1}".format(i, client.public_dns_name))

    def _print_footer(self):
        print("\n[membase]\n" + \
              "rest_username:Administrator\n" + \
              "rest_password:password")

    def print_file(self):
        # [global] section
        self._print_header()

        # [clusterN] section
        if self.options.clusters:
            self._print_clusters()

        # [servers] section
        self._print_servers()

        # [clients] section
        if self.options.clients:
            self._print_clients()

        # [membase] section
        self._print_footer()


def main():
    aws_ini = AwsIni()
    aws_ini.parse_args()
    aws_ini.print_file()

if __name__ == "__main__":
    main()
