from argparse import ArgumentParser
import sys, json, subprocess
import boto3
import paramiko
from google.cloud.compute_v1 import ImagesClient, InstancesClient, ZoneOperationsClient
from google.cloud.compute_v1.types.compute import AccessConfig, AttachedDisk, AttachedDiskInitializeParams, BulkInsertInstanceRequest, BulkInsertInstanceResource, DeleteInstanceRequest, GetFromFamilyImageRequest, InstanceProperties, ListInstancesRequest, NetworkInterface, Operation
from google.cloud.dns import Client as DnsClient
import dns.resolver
import time
import io
"""
  Azure needs to install azure cli 'az' in dispatcher slave
"""

def check_root_login(host, username ="root", password="couchbase"):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host,
                    username=username,
                    password=password)
        return True
    except :
        return False

def install_iptables(host, username="root", password="couchbase"):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host,
                username=username,
                password=password)
    commands = []
    stdin, stdout, stderr = ssh.exec_command("yum --help")
    if stdout.channel.recv_exit_status() != 0:
        commands.append("apt-get install iptables")
    else:
        commands.append("yum install -y iptables-services",)
        commands.append("systemctl start iptables")

    for command in commands:
            stdin, stdout, stderr = ssh.exec_command(command)
            if stdout.channel.recv_exit_status() != 0:
                ssh.exec_command("sudo shutdown")
                time.sleep(10)
                ssh.close()
                raise Exception("iptables could not be installed on {}".format(host))

    ssh.close()

def create_non_root_user(host, username="root", password="couchbase"):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host,
                username=username,
                password=password)
    commands = ["useradd -m nonroot",
                "echo -e 'couchbase\ncouchbase' | sudo passwd nonroot",
                "echo \"nonroot       soft  nofile         200000\" >> /etc/security/limits.conf",
                "echo \"nonroot       hard  nofile         200000\" >> /etc/security/limits.conf"]

    for command in commands:
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.channel.recv_exit_status() != 0:
            break

    if check_root_login(host):
        print("nonroot login to host {} successful.".format(host))
    else:
        print("nonroot login to host {} failed. Terminating the EC2 instance".format(host))
        ssh.exec_command("sudo shutdown")
        time.sleep(10)

def install_elastic_search(host, username="root", password="couchbase"):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host,
                username=username,
                password=password)
    commands = ["yum install -y wget",
                "yum install -y java",
                "wget https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.3.tar.gz",
                "tar -zxvf elasticsearch-1.7.3.tar.gz",
                "mv elasticsearch-1.7.3 /usr/share/elasticsearch",
                "/usr/share/elasticsearch/bin/elasticsearch -d"]
    for command in commands:
        stdin, stdout, stderr = ssh.exec_command(command)
        if stdout.channel.recv_exit_status() != 0:
            break

def post_provisioner(host, username, ssh_key_path, modify_hosts=False):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host,
                    username=username,
                    key_filename=ssh_key_path)

        commands = ["echo -e 'couchbase\ncouchbase' | sudo passwd root",
                    "sudo sed -i '/#PermitRootLogin yes/c\PermitRootLogin yes' /etc/ssh/sshd_config",
                    "sudo sed -i '/PermitRootLogin no/c\PermitRootLogin yes' /etc/ssh/sshd_config",
                    "sudo sed -i '/PermitRootLogin prohibit-password/c\PermitRootLogin yes' /etc/ssh/sshd_config",
                    "sudo sed -i '/PermitRootLogin forced-commands-only/c\#PermitRootLogin forced-commands-only' /etc/ssh/sshd_config",
                    "sudo sed -i '/PasswordAuthentication no/c\PasswordAuthentication yes' /etc/ssh/sshd_config",
                    "sudo service sshd restart",
                    "sudo shutdown -P +800"]

        for command in commands:
            stdin, stdout, stderr = ssh.exec_command(command)
            if stdout.channel.recv_exit_status() != 0:
                break

        if check_root_login(host):
            print("root login to host {} successful.".format(host))
        else:
            print("root login to host {} failed. Terminating the EC2 instance".format(host))
            ssh.exec_command("sudo shutdown")
            time.sleep(10)

        if modify_hosts:
            # add hostname to /etc/hosts so node-init-hostname works by binding to 127.0.0.1
            # as recommended in https://docs.couchbase.com/server/current/cloud/couchbase-cloud-deployment.html
            sftp = ssh.open_sftp()
            with io.BytesIO() as fl:
                sftp.getfo('/etc/hosts', fl)
                hosts = fl.getvalue().decode()
                modified_hosts = str()
                for line in hosts.splitlines():
                    ip, hostnames = line.split(maxsplit=1)
                    if ip == "127.0.0.1":
                        hostnames += f" {host}"
                    modified_hosts += f"{ip} {hostnames}\n"
                # need to be root to write to /etc/hosts so write to a new file and move it with sudo
                sftp.putfo(io.BytesIO(modified_hosts.encode()), '/tmp/newhosts')
                ssh.exec_command("sudo mv /tmp/newhosts /etc/hosts")

        ssh.close()


def aws_terminate(name):
    ec2_client = boto3.client('ec2')
    instances = ec2_client.describe_instances(
        Filters=[
            {
                'Name': 'tag:Name',
                'Values': [
                    name
                ]
            }
        ]
    )
    instance_ids = []
    for reservation in instances['Reservations']:
        for instance in reservation['Instances']:
            instance_ids.append(instance['InstanceId'])
    if instance_ids:
        ec2_client.terminate_instances(
            InstanceIds=instance_ids
        )

AWS_AMI_MAP = {
    "couchbase": {
        "amzn2": {
            "aarch64": "ami-0289ff69e0069c2ed",
            "x86_64": "ami-070ac986a212c4d9b"
        },
        "al2023": {
            "aarch64": "ami-08f0305fa4286a9d0",
            "x86_64": "ami-037d1d9dfa436c7c6"
        },
        "ubuntu20": {
            "x86_64" : "ami-09168ff916a2e8ed3",
            "aarch64": "ami-0d70a59d7191a8079"
        },
        "ubuntu22": {
            "x86_64" : "ami-09c0393bac9d9b6ed",
            "aarch64": "ami-05b98dc6de6e09e97"
        },
        "ubuntu22nonroot" : {
            "x86_64" : "ami-09c0393bac9d9b6ed",
            "aarch64": "ami-05b98dc6de6e09e97"
        },
        "oel8" : {
            "x86_64" : "ami-0b5aaeac901e41860"
        },
        "rhel8" : {
            "x86_64" : "ami-07f5ef252bd61130b"
        },
        "rhel9" : {
            "x86_64" : "ami-0859d5937ea3b22db"
        },
        "suse15" : {
            "x86_64" : "ami-059c3ca86322facbe"
        },
        "suse12": {
            "x86_64" : "ami-023f4e041769c362b"
        },
        "alma9": {
            "x86_64" : "ami-0ec549aa7bb28072e"
        },
        "centos7": {
            "x86_64" : "ami-0599a9ff8a4ca809c"
        },
        "rocky9": {
            "x86_64" : "ami-0441302605ba7fdb4"
        }
    },
    # The following AMIs are faulty and not working
    # TODO - Investigate and create new AMI for the same
    "elastic-fts": "ami-0c48f8b3129e57beb",
    "localstack": "ami-0702052d7d7f58aad"
}

AWS_OS_USERNAME_MAP = {
    "amzn2": "ec2-user",
    "ubuntu20": "ubuntu",
    "centos7": "centos",
    "centos": "centos",
    "al2023": "ec2-user",
    "ubuntu22": "ubuntu",
    "ubuntu22nonroot": "ubuntu",
    "oel8": "ec2-user",
    "rhel8": "ec2-user",
    "rhel9": "ec2-user",
    "suse15": "ec2-user",
    "suse12": "ec2-user",
    "alma9": "ec2-user",
    "rocky9": "rocky"
}

def aws_get_servers(name, count, os, type, ssh_key_path, architecture=None):
    instance_type = "t3.xlarge"
    ssh_username = AWS_OS_USERNAME_MAP[os]

    if type != "couchbase":
        image_id = AWS_AMI_MAP[type]
        # TODO: Change to centos x86 AMI (CBQE-7627)
        if type == "localstack":
            instance_type = "t4g.xlarge"
            ssh_username = "ec2-user"
    else:
        image_id = AWS_AMI_MAP["couchbase"][os][architecture]
        if architecture in ["aarch64", "arm64"]:
            instance_type = "t4g.xlarge"

    if type == "elastic-fts":
        '''
        The elastic-fts Instances are created with the following config
        OS - Centos7
        Architecture - x86
        Instance Type - t2.large
        Elastic Search Version - 1.7.3
        '''
        instance_type = "t2.large"
        os = "centos7"
        architecture = "x86_64"
        ssh_username = AWS_OS_USERNAME_MAP[os]
        image_id = AWS_AMI_MAP["couchbase"][os][architecture]

    ec2_resource = boto3.resource('ec2', region_name='us-east-1')
    ec2_client = boto3.client('ec2', region_name='us-east-1')

    instances = ec2_resource.create_instances(
        ImageId=image_id,
        MinCount=count,
        MaxCount=count,
        InstanceType=instance_type,
        LaunchTemplate={
            'LaunchTemplateName': 'qe-al-template',
            'Version': '1'
        },
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': name
                    },
                    {
                        'Key': 'Owner',
                        'Value': 'ServerRegression'
                    }
                ]
            },
        ],
        InstanceInitiatedShutdownBehavior='terminate'
    )

    instance_ids = [instance.id for instance in instances]

    print("Waiting for instances: ", instance_ids)

    ec2_client.get_waiter('instance_status_ok').wait(InstanceIds=instance_ids)

    instances = ec2_client.describe_instances(InstanceIds=instance_ids)
    ips = [instance['PublicDnsName'] for instance in instances['Reservations'][0]['Instances']]
    print("EC2 Instances : ", ips)
    for ip in ips:
        post_provisioner(ip, ssh_username, ssh_key_path)
        if type == "elastic-fts":
            install_elastic_search(ip)
        if "suse" not in os:
            install_iptables(ip)
        if "nonroot" in os:
            create_non_root_user(ip)
            check_root_login(ip,username="nonroot",password="couchbase")
    return ips

ZONE = "us-central1-a"
PROJECT = "couchbase-qe"
DISK_SIZE_GB = 40
SSH_USERNAME = "couchbase"
# Long term we should move the provisioning to a server manager that handles these variables in a config file rather than hardcoding
CLOUD_DNS_DOMAIN_NAME = "couchbase6.com"
CLOUD_DNS_ZONE_NAME = "couchbase6"

def gcp_terminate(name):
    client = InstancesClient()
    try:
        instances = client.list(ListInstancesRequest(project=PROJECT, zone=ZONE, filter=f"labels.name = {name}"))
        for instance in instances:
            client.delete(DeleteInstanceRequest(instance=instance.name, project=PROJECT, zone=ZONE))
        delete_dns_records(instances)
    except Exception:
        return

GCP_TEMPLATE_MAP = {
    "couchbase": {
        "centos7": {
            "x86_64": { "project": "centos-cloud", "family": "centos-7" }
        }
    },
    # TODO
    "elastic-fts": { "project": "couchbase-qe", "image": "" },
    "localstack": { "project": "couchbase-qe", "image": "" }
}

def gcp_wait_for_operation(operation):
    print(f"Waiting for {operation} to complete")
    client = ZoneOperationsClient()
    while True:
        result = client.get(project=PROJECT, zone=ZONE, operation=operation)

        if result.status == Operation.Status.DONE:
            if result.error:
                raise Exception(result.error)
            return result

        time.sleep(1)


def hostname_from_ip(ip, for_dns=False):
    hostname = f"gcp-{ip.replace('.', '-')}.{CLOUD_DNS_DOMAIN_NAME}"
    if for_dns:
        hostname += "."
    return hostname


def ip_from_instance(instance):
    return instance.network_interfaces[0].access_configs[0].nat_i_p


def delete_dns_records(instances):
    modify_dns_records(instances, delete=True)


def create_dns_records(instances):
    modify_dns_records(instances, delete=False)


def modify_dns_records(instances, delete):
    zone = DnsClient().zone(CLOUD_DNS_ZONE_NAME, CLOUD_DNS_DOMAIN_NAME)
    changes = zone.changes()
    for instance in instances:
        ip = ip_from_instance(instance)
        hostname = hostname_from_ip(ip, for_dns=True)
        resource_record_set = zone.resource_record_set(
            name=hostname, rrdatas=[ip], record_type="A", ttl=60)
        if delete:
            changes.delete_record_set(resource_record_set)
        else:
            changes.add_record_set(resource_record_set)
    changes.create()


def wait_for_dns_propagation(instances):
    propagated = False
    while not propagated:
        propagated = True
        for instance in instances:
            ip = ip_from_instance(instance)
            hostname = hostname_from_ip(ip)
            try:
                answer = dns.resolver.resolve(hostname)
                propagated &= answer[0].to_text() == ip
            except Exception:
                propagated = False
                time.sleep(1)
                continue


def gcp_get_servers(name, count, os, type, ssh_key_path, architecture):
    machine_type = "e2-standard-4"

    if type != "couchbase":
        image_descriptor = GCP_TEMPLATE_MAP[type]
    else:
        image_descriptor = GCP_TEMPLATE_MAP["couchbase"][os][architecture]

    if "family" in image_descriptor:
        image = ImagesClient().get_from_family(GetFromFamilyImageRequest(project=image_descriptor["project"], family=image_descriptor["family"]))
    else:
        image = ImagesClient().get(project=image_descriptor["project"], image=image_descriptor["image"])

    disk = AttachedDisk(auto_delete=True, boot=True, initialize_params=AttachedDiskInitializeParams(source_image=image.self_link, disk_size_gb=DISK_SIZE_GB))
    network_interface = NetworkInterface(network="global/networks/default", access_configs=[AccessConfig(type_=AccessConfig.Type.ONE_TO_ONE_NAT)])
    instances = BulkInsertInstanceResource(name_pattern=f"{name}-####", count=count, instance_properties=InstanceProperties(labels={ "name": name }, machine_type=machine_type, disks=[disk], network_interfaces=[network_interface]))
    op = InstancesClient().bulk_insert(BulkInsertInstanceRequest(project=PROJECT, zone=ZONE, bulk_insert_instance_resource_resource=instances))

    gcp_wait_for_operation(op.name)

    instances = list(InstancesClient().list(ListInstancesRequest(project=PROJECT, zone=ZONE, filter=f"labels.name = {name}")))

    create_dns_records(instances)

    wait_for_dns_propagation(instances)

    hostnames = [hostname_from_ip(ip_from_instance(instance))
                 for instance in instances]

    for hostname in hostnames:
        post_provisioner(hostname, SSH_USERNAME, ssh_key_path, modify_hosts=True)

    return hostnames

AZ_TEMPLATE_MAP = {
    "couchbase"  : "qe-image-20211129",
    "elastic-fts": "qe-es-image-20211129",
    "localstack" : "qe-localstack-image-20211129"
}
def az_get_servers(name, count, os, type, ssh_key_path, architecture):
    vm_type = "Standard_B4ms"
    security_group_name = "qe-test-nsg"
    group_name = "qe-test"
    ips = []
    internal_ips = []

    if type != "couchbase":
        image_name = AZ_TEMPLATE_MAP[type]
    else:
        image_name = AZ_TEMPLATE_MAP["couchbase"]

    """ az vm create -g qe-test -n from-w22 --image 'qe-test-image-20211112-westus2' --nsg qe-test-nsg
           --size Standard_B4ms --output json  --count 2 --public-ip-sku Standard
        image_name = "qe-test-image-20211112-westus2"
    """
    for x in range(1, count + 1):
        cmd = "az vm create -g {0} -n {1}{2} --image {3} --nsg {4} --size {5} --public-ip-sku Standard --output json "\
              .format(group_name, name, x, image_name, security_group_name, vm_type)
        print("\ncreate vm {0}{1}".format(name, x))
        stdout = subprocess.check_output(cmd, shell=True)
        if isinstance(stdout, bytes):
            # convert to string to load json
            stdout = stdout.decode('utf-8')
        if isinstance(stdout, str):
            stdout = json.loads(stdout)
            ips.append(stdout["publicIpAddress"])
            internal_ips.append(stdout["privateIpAddress"])
    """ no need post_provisioner run on azure """
    print("public ips of vms: ", ips)
    print("private ips of vms: ", internal_ips)
    return ips, internal_ips

def az_terminate(name):
    group_name = "qe-test"
    cmd = "az vm list |  grep -o '\"computerName\": \"[^\"]*' | grep -o '[^\"]*$' | grep ^{}".format(name)
    vms = subprocess.check_output(cmd, shell=True).decode('utf-8')
    vms = vms.split("\n")
    vms = [s for s in vms if s]
    for vm_name in vms:
        cmd1 = "az vm delete -g {0} -n {1} -y "\
               .format(group_name, vm_name)
        cmd2 = "az network nic delete -g {0} -n {1}VMNic --no-wait"\
                .format(group_name, vm_name)
        cmd3 = "az network public-ip delete -g {0} -n {1}PublicIP"\
                .format(group_name, vm_name)
        print("\ndelete vm {0}".format(name))
        subprocess.check_output(cmd1, shell=True)
        print("\ndelete network of vm {0}".format(name))
        subprocess.check_output(cmd2, shell=True)
        print("\ndelete public ip of vm {0}".format(name))
        subprocess.check_output(cmd3, shell=True)


if __name__ == "__main__":
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="provider")

    providers = ["aws", "gcp", "az"]

    for provider in providers:
        provider_parser = subparsers.add_parser(provider)
        provider_subparsers = provider_parser.add_subparsers(dest="cmd")
        provider_terminate_parser = provider_subparsers.add_parser("terminate")
        provider_terminate_parser.add_argument("name", type=str)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if args.provider == "aws":
        if args.cmd == "terminate":
            aws_terminate(args.name)
    elif args.provider == "gcp":
        if args.cmd == "terminate":
            gcp_terminate(args.name)
    elif args.provider == "az":
        if args.cmd == "terminate":
            az_terminate(args.name)
