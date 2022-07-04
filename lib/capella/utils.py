import uuid
from TestInput import TestInputServer
from capellaAPI.CapellaAPI import CapellaAPI as CapellaAPIBase
import time
import base64
import random
import urllib3
urllib3.disable_warnings()

class CapellaCredentials:
    def __init__(self, config):
        self.pod = config["pod"]
        self.tenant_id = config["tenant_id"]
        self.capella_user = config["capella_user"]
        self.capella_pwd = config["capella_pwd"]
        self.access_key = config["access_key"]
        self.secret_key = config["secret_key"]
        self.project_id = config["project_id"]

def base64_encode(string):
    return base64.b64encode(string.encode('utf-8')).decode('utf-8')

def generate_cidr():
    first = random.randint(0, 256)
    second = random.randint(0, 16) * 16
    return f"10.{first}.{second}.0/20"

class CapellaAPI:
    def __init__(self, credentials):
        self.tenant_id = credentials.tenant_id
        self.project_id = credentials.project_id
        self.api = CapellaAPIBase(credentials.pod, credentials.secret_key, credentials.access_key, credentials.capella_user, credentials.capella_pwd)

    def get_cluster_info(self, cluster_id):
        resp = self.api.get_cluster_info(cluster_id)
        resp.raise_for_status()
        return resp.json()

    def get_cluster_srv(self, cluster_id):
        info = self.get_cluster_info(cluster_id)
        return info["endpointsSrv"]

    def get_cluster_state(self, cluster_id):
        info = self.get_cluster_info(cluster_id)
        return info["status"]

    def create_cluster(self, config, timeout=1800):
        end_time = time.time() + timeout
        while time.time() < end_time:
            cidr = generate_cidr()
            config["cidr"] = cidr
            resp = self.api.create_cluster_customAMI(self.tenant_id, config)
            try:
                resp.raise_for_status()
                cluster_id = resp.json()["id"]
                return cluster_id
            except Exception as e:
                if "ErrClusterInvalidCIDRNotUnique" in resp.text:
                    continue
                else:
                    raise e
        raise Exception("Timeout trying to create cluster")

    def create_cluster_and_wait(self, config, timeout=1800):
        start_time = time.time()
        cluster_id = self.create_cluster(config, timeout)
        time_left = timeout - (time.time() - start_time)
        self.wait_for_cluster(cluster_id, "Creating cluster", time_left)
        self.allow_my_ip(cluster_id)
        return cluster_id

    def create_bucket(self, cluster_id, bucket_params):
        resp = self.api.create_bucket(self.tenant_id, self.project_id, cluster_id, bucket_params)
        resp.raise_for_status()

    def allow_my_ip(self, cluster_id):
        resp = self.api.allow_my_ip(self.tenant_id, self.project_id, cluster_id)
        resp.raise_for_status()

    def delete_bucket(self, cluster_id, name):
        bucket_id = base64_encode(name)
        resp = self.api.delete_bucket(self.tenant_id, self.project_id, cluster_id, bucket_id)
        resp.raise_for_status()

    def get_nodes(self, cluster_id):
        resp = self.api.get_nodes(self.tenant_id, self.project_id, cluster_id)
        resp.raise_for_status()
        resp = resp.json()
        return [server["data"] for server in resp["data"]]

    def delete_cluster(self, cluster_id):
        resp = self.api.delete_cluster(cluster_id)
        resp.raise_for_status()
        return resp

    def create_db_user(self, cluster_id, user, pwd):
        resp = self.api.create_db_user(self.tenant_id, self.project_id, cluster_id, user, pwd)
        resp.raise_for_status()
        return resp

    def jobs(self, cluster_id):
        resp = self.api.jobs(self.project_id, self.tenant_id, cluster_id)
        resp.raise_for_status()
        resp = resp.json()
        return [job["data"] for job in resp["data"]]

    def wait_for_cluster_step(self, cluster_id, msg=""):
        jobs = self.jobs(cluster_id)
        state = self.get_cluster_state(cluster_id)
        if state != "healthy" or len(jobs) > 0:
            for job in jobs:
                if job["clusterId"] == cluster_id:
                    step, progress = job["currentStep"], job["completionPercentage"]
                    print("{}: Status=={}, State=={}, Progress=={}%".format(msg, state, step, progress))
            return False
        else:
            return True

    def wait_for_cluster(self, cluster_id, msg="", timeout=1800):
        end_time = time.time() + timeout
        while time.time() < end_time:
            complete = self.wait_for_cluster_step(cluster_id, msg)
            if complete:
                return
            else:
                time.sleep(2)
        raise Exception("Timeout waiting for cluster to be healthy")

    def get_nodes_formatted(self, cluster_id, username=None, password=None):
        nodes = self.get_nodes(cluster_id)
        nodes = format_nodes(nodes, username, password)
        return nodes

    def create_eventing_function(self, cluster_id, name, body, function_scope=None):
        resp = self.api.create_eventing_function(cluster_id, name, body, function_scope)
        resp.raise_for_status()

    def deploy_eventing_function(self, cluster_id, name, function_scope=None):
        resp = self.api.deploy_eventing_function(cluster_id, name, function_scope)
        resp.raise_for_status()

    def undeploy_eventing_function(self, cluster_id, name, function_scope=None):
        resp = self.api.undeploy_eventing_function(cluster_id, name, function_scope)
        resp.raise_for_status()

    def pause_eventing_function(self, cluster_id, name, function_scope=None):
        resp = self.api.pause_eventing_function(cluster_id, name, function_scope)
        resp.raise_for_status()

    def resume_eventing_function(self, cluster_id, name, function_scope=None):
        resp = self.api.resume_eventing_function(cluster_id, name, function_scope)
        resp.raise_for_status()

    def get_composite_eventing_status(self, cluster_id):
        resp = self.api.get_composite_eventing_status(cluster_id)
        resp.raise_for_status()
        return resp.json()

    def get_all_eventing_stats(self, cluster_id, seqs_processed=False):
        resp = self.api.get_all_eventing_stats(cluster_id, seqs_processed)
        resp.raise_for_status()
        return resp.json()

    def delete_eventing_function(self, cluster_id, name, function_scope=None):
        resp = self.api.delete_eventing_function(cluster_id, name, function_scope)
        resp.raise_for_status()

    def update_specs(self, cluster_id, specs):
        resp = self.api.update_specs(self.tenant_id, self.project_id, cluster_id, specs)
        resp.raise_for_status()

def format_nodes(nodes, username=None, password=None):
    servers = list()
    for node in nodes:
        temp_server = TestInputServer()
        temp_server.ip = node["hostname"]
        temp_server.hostname = node["hostname"]
        capella_services = node["services"]
        services = []
        for service in capella_services:
            if service == "Data":
                services.append("kv")
            elif service == "Index":
                services.append("index")
            elif service == "Query":
                services.append("n1ql")
            elif service == "Search":
                services.append("fts")
            elif service == "Eventing":
                services.append("eventing")
            elif service == "Analytics":
                services.append("cbas")
        temp_server.services = services
        temp_server.port = "18091"
        temp_server.rest_username = username
        temp_server.rest_password = password
        temp_server.hosted_on_cloud = True
        temp_server.memcached_port = "11207"
        servers.append(temp_server)
    return servers

def get_service_counts(servers):
    services_count = {}
    for server in servers:
        services = server.services
        if not isinstance(services, list):
            services = services.split(",")
        # sort so kv,index,n1ql and kv,n1ql,index are equal
        services = ",".join(sorted(services))
        if services in services_count:
            services_count[services] += 1
        else:
            services_count[services] = 1
    return services_count

def create_specs(provider, services_count, compute, disk_type, disk_iops, disk_size):
    specs = []
    for service_group, count in services_count.items():
        spec = {
            "count": count,
            "compute": {
                "type": compute,
                "cpu": 0, 
                "memoryInGb": 0
            },
            "services": [{ "type": service } for service in service_group.split(",")],
            "disk": {
                "type": disk_type,
                "sizeInGb": disk_size
            }
        }
        if provider == "aws":
            spec["disk"]["iops"] = disk_iops
        specs.append(spec)
    return specs

def spec_options_from_input(input):
    def get_option(option_name, default_value):
        option = input.param(option_name, None)
        if option is None:
            option = input.capella.get(option_name, None)
        return option or default_value

    provider = get_option("provider", "aws")

    default_region = "us-east-2" if provider == "aws" else "us-east-1"
    region = get_option("region", default_region)

    default_compute = "m5.xlarge" if provider == "aws" else "n2-standard"
    compute = get_option("compute", default_compute)

    default_disk_type = "gp3" if provider == "aws" else "pd-ssd"
    disk_type = get_option("disk_type", default_disk_type)

    default_disk_iops = 3000 if provider == "aws" else None
    disk_iops = get_option("disk_iops", default_disk_iops)
    if disk_iops:
        disk_iops = int(disk_iops)

    default_disk_size = 50
    disk_size = int(get_option("disk_size", default_disk_size))

    return provider, region, compute, disk_type, disk_iops, disk_size

def create_capella_config(input, services_count):
    provider, region, compute, disk_type, disk_iops, disk_size = spec_options_from_input(input)

    specs = create_specs(provider, services_count, compute, disk_type, disk_iops, disk_size)

    if provider == "aws":
        provider = "hostedAWS"
    elif provider == "gcp":
        provider = "hostedGCP"

    config = {
        "region": region,
        "provider": provider,
        "name": str(uuid.uuid4()),
        "cidr": None,
        "singleAZ": True,
        "specs": specs,
        "package": "enterprise",
        "projectId": input.capella["project_id"],
        "description": "",
        "server": None
    }

    if input.capella.get("image"):
        image = input.capella["image"]
        token = input.capella["override_token"]
        server_version = input.capella["server_version"]
        config["overRide"] = {"token": token,
                                "image": image,
                                "server": server_version}

    return config
