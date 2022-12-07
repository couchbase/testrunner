import uuid
from TestInput import TestInputServer
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIDedicated
from capellaAPI.capella.serverless.CapellaAPI import CapellaAPI as CapellaAPIServerless
import time
import base64
import random
import urllib3
urllib3.disable_warnings()
import logger
import requests
import json
import subprocess
import time

class CapellaCredentials:
    def __init__(self, config):
        self.pod = config["pod"]
        self.tenant_id = config["tenant_id"]
        self.capella_user = config["capella_user"]
        self.capella_pwd = config["capella_pwd"]
        self.access_key = config.get("access_key")
        self.secret_key = config.get("secret_key")
        self.project_id = config["project_id"]
        self.token_for_internal_support = config.get("token_for_internal_support")
        self.dataplane_id = config["dataplane_id"]

class ServerlessDatabase:
    def __init__(self, database_id, doc_count=0, namespaces=None):
        self.collections = dict()
        self.id = database_id
        self._doc_count = doc_count
        self._namespaces = namespaces if namespaces else [f"{database_id}._default._default"]

    def populate(self, info, creds, rest_api_info):
        self.srv = info["connect"]["srv"]
        self.data_api = info["connect"]["dataApi"]
        self.access_key = creds["access"]
        self.secret_key = creds["secret"]
        self.rest_srv = rest_api_info['srv']
        self.admin_username = rest_api_info['couchbaseCreds']['username']
        self.admin_password = rest_api_info['couchbaseCreds']['password']
        self.nebula = get_host_from_srv(self.srv)
        try:
            self.rest_host = get_host_from_srv(self.rest_srv)
        except Exception as err:
            print(err)
            print("Rest srv domain resolution failure. Cannot run any of the rest APIs")

    @property
    def doc_count(self):
        """
        An unreliable way of keeping track of docs in a DB. Does not actually count the number of docs
        but allows the user to set the count.
        """
        return self._doc_count

    @doc_count.setter
    def doc_count(self, doc_count):
        """
        An unreliable way of keeping track of docs in a DB. Does not actually count the number of docs
        but allows the user to set the count.
        """
        self._doc_count = doc_count

    @property
    def namespaces(self):
        """
        returns all the scopes/collections in a DB
        """
        return self._namespaces

    @namespaces.setter
    def namespaces(self, namespaces):
        """
       sets namespaces variable to scopes/collections in a DB
        """
        self._namespaces = namespaces

class ServerlessDataPlane:
    def __init__(self, dataplane_id):
        self.id = dataplane_id

    def populate(self, rest_api_info):
        self.rest_srv = rest_api_info['srv']
        self.admin_username = rest_api_info['couchbaseCreds']['username']
        self.admin_password = rest_api_info['couchbaseCreds']['password']
        self.rest_host = get_host_from_srv(self.rest_srv)

def get_host_from_srv(srv, attempts=12, retry_wait_time=15):
    import dns.resolver
    srvInfo = {}
    log = logger.Logger.get_logger()
    for i in range(1, attempts+1):
        try:
            dns.resolver.Cache().flush()
            srv_records = dns.resolver.resolve('_couchbases._tcp.' + srv, 'SRV')
            break
        except Exception as ex:
            if i == attempts:
                raise(ex)
            log.warn(f"Failed ({ex.__class__.__name__}) to resolve {srv}. Retrying in {retry_wait_time} seconds ...")
            time.sleep(retry_wait_time)
    for srv in srv_records:
        srvInfo['host'] = str(srv.target).rstrip('.')
    return srvInfo['host']

def base64_encode(string):
    return base64.b64encode(string.encode('utf-8')).decode('utf-8')

def generate_cidr():
    first = random.randint(0, 255)
    second = random.randint(0, 15) * 16
    return f"10.{first}.{second}.0/20"

class CapellaAPI:
    def __init__(self, credentials):
        self.tenant_id = credentials.tenant_id
        self.project_id = credentials.project_id
        self.dataplane_id = credentials.dataplane_id
        self.api = CapellaAPIDedicated(credentials.pod, credentials.secret_key, credentials.access_key, credentials.capella_user, credentials.capella_pwd)
        self.serverless_api = CapellaAPIServerless(credentials.pod, credentials.capella_user, credentials.capella_pwd, credentials.token_for_internal_support)
        self.log = logger.Logger.get_logger()

    def get_cluster_info(self, cluster_id):
        resp = self.api.get_cluster_info(cluster_id)
        resp.raise_for_status()
        return resp.json()

    def get_database_info(self, database_id):
        resp = self.serverless_api.get_serverless_db_info(self.tenant_id, self.project_id, database_id)
        resp.raise_for_status()
        return resp.json()["data"]

    def get_cluster_srv(self, cluster_id):
        info = self.get_cluster_info(cluster_id)
        return info["endpointsSrv"]

    def get_cluster_state(self, cluster_id):
        info = self.get_cluster_info(cluster_id)
        return info["status"]

    def get_database_state(self, database_id):
        info = self.get_database_info(database_id)
        return info["status"]["state"]

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
                    self.log.info(f"config: {config}")
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
                    self.log.info("{}: Status=={}, State=={}, Progress=={}%".format(msg, state, step, progress))
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

    def create_serverless_database(self, config) -> str:
        if 'overRide' in config:
            self.log.info(f"Dataplane ID parameter has been passed in the ini file."
                          f"Will use {config['overRide']['dataplaneId']} for DB creation")
            resp = self.serverless_api.create_serverless_database_overRide(config)
        else:
            self.log.info(f"Dataplane ID parameter not passed in the ini file. Will use default DP for DB creation")
            resp = self.serverless_api.create_serverless_database(self.tenant_id, config)
        resp.raise_for_status()
        return resp.json()["databaseId"]

    def get_access_to_serverless_dataplane_nodes(self, dataplane_id) -> str:
        resp = self.serverless_api.get_access_to_serverless_dataplane_nodes(dataplane_id)
        resp.raise_for_status()
        return resp.json()

    def get_resident_dataplane_id(self, database_id):
        resp = self.serverless_api.get_database_debug_info(database_id=database_id)
        resp.raise_for_status()
        dataplane_id = resp.json()['dataplane']['id']
        return dataplane_id

    def wait_for_database_deleted(self, database_id, timeout=300):
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                state = self.get_database_state(database_id)
                msg = {
                    "database_id": database_id,
                    "state": state
                }
                self.log.info(
                    "waiting for database to be deleted {}".format(msg))
            except requests.exceptions.HTTPError as err:
                if err.response.status_code == 404:
                    return
                time.sleep(2)
        raise Exception("timeout waiting for database to be deleted {}".format(
            {"database_id": database_id}))

    def wait_for_database_step(self, database_id):
        state = self.get_database_state(database_id)
        if state != "healthy":
            msg = {
                "database_id": database_id,
                "state": state
            }
            self.log.info("waiting for database to be healthy {}".format(msg))
            return False
        else:
            return True

    def generate_api_keys(self, database_id):
        resp = self.serverless_api.generate_keys(
            self.tenant_id, self.project_id, database_id)
        resp.raise_for_status()
        return resp.json()

    def delete_serverless_database(self, database_id):
        resp = self.serverless_api.delete_database(
            self.tenant_id, self.project_id, database_id)
        resp.raise_for_status()

    def create_dataplane_wait_for_ready(self, overRide=None):
        if overRide is None:
            config = {
                "provider": "aws",
                "region": "us-east-1"
            }
        else:
            self.log.info("OverRiding Dataplane ...")
            config = {
                "provider": "aws",
                "region": "us-east-1",
                "overRide": overRide
            }

        resp = self.serverless_api.create_serverless_dataplane(config)
        self.log.info(f"Dataplane with ID {resp.json()['dataplaneId']} created")
        if self.wait_for_dataplane_ready(resp.json()['dataplaneId']):
            self.log.info(f"Would use DataplaneId : {resp.json()['dataplaneId']} for further execution")
            self.dataplane_id = resp.json()['dataplaneId']
            return resp.json()['dataplaneId']
        else:
            self.log.info(f"Timed out waiting for dataplane to be ready, Aborting...")
            return None

    def wait_for_dataplane_ready(self, dataplane_id):
        t_end = time.time() + 60 * 25
        while time.time() < t_end:
            self.log.info(f"Waiting for Dataplane : {dataplane_id} to become ready...")
            resp = self.get_dataplane_deployment_status(dataplane_id)
            if resp['status']['state'] == "ready":
                return True
            time.sleep(5)
        return False

    def override_width_and_weight(self, database_id, override):
        override_obj = {"overRide": override}
        resp = self.serverless_api.update_database(database_id, override_obj)
        resp.raise_for_status()

    def get_databases_id(self):
        resp = self.serverless_api.get_all_serverless_databases()
        all_ids = []
        if resp and isinstance(resp.json(), list):
            for database in resp.json():
                if 'dataplaneId' in database['config'] and database['config']['dataplaneId'] == self.dataplane_id:
                    all_ids.append(database['id'])
        return all_ids

    def get_fts_stats(self):
        fts_nodes = self.get_fts_nodes_of_dataplane(self.dataplane_id)
        creds = self.get_access_to_serverless_dataplane_nodes(self.dataplane_id)
        stats = []
        http_resp = False
        for count, node in enumerate(fts_nodes):
            CurlUrl = f"curl -s -XGET https://{fts_nodes[count]}:18094/api/nsstats -u {creds['couchbaseCreds']['username']}:{creds['couchbaseCreds']['password']} --insecure | jq | grep -E 'util|resource|limits'"
            resp = subprocess.getstatusoutput(CurlUrl)[1]
            try:
                resp = resp[2:] + "}" + resp[:2]
                resp = resp[len(resp) - 2:] + "{" + resp[:len(resp) - 1]
                x = json.loads(resp)
                stats.append(x)
            except Exception as e:
                self.log.info(f"Error : {e}")
                print(f"Response : {resp}")
                curl_http = f"curl -v https://{fts_nodes[count]}:18094/api/nsstats -u {creds['couchbaseCreds']['username']}:{creds['couchbaseCreds']['password']} --insecure"
                http_resp = subprocess.getstatusoutput(curl_http)
        return stats, fts_nodes, http_resp

    def get_fts_nodes_of_dataplane(self, dataplane_id):
        resp = self.serverless_api.get_serverless_dataplane_info(dataplane_id)
        info_nodes = resp.json()['couchbase']['nodes']
        fts_hostname=[]
        for node in info_nodes:
            if node['services'][0]['type'] == 'fts':
                fts_hostname.append(node['hostname'])
        return fts_hostname

    def delete_dataplane(self, dataplane_id):
        resp = self.serverless_api.delete_dataplane(dataplane_id)
        resp.raise_for_status()

    def get_dataplane_deployment_status(self, dataplane_id):
        resp = self.serverless_api.get_dataplane_deployment_status(dataplane_id=dataplane_id)
        resp.raise_for_status()
        return resp.json()

    def modify_cluster_specs(self, dataplane_id, specs):
        resp = self.serverless_api.modify_cluster_specs(dataplane_id=dataplane_id,
                                                        specs=specs)
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
    dataplane_id = get_option("dataplane_id", None)
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

    return provider, region, compute, disk_type, disk_iops, disk_size, dataplane_id

def create_capella_config(input, services_count):
    provider, region, compute, disk_type, disk_iops, disk_size, dataplane_id = spec_options_from_input(input)

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


def create_serverless_config(input, skip_import_sample=True, seed=None, dp_id=None):
    provider, region, _, _, _, _, dataplane_id = spec_options_from_input(input)
    if seed:
        name = f"{seed}"
    else:
        name = str(uuid.uuid4())
    config = {
        "name": name,
        "region": region,
        "provider": provider,
        "projectId": input.capella["project_id"],
        "tenantId": input.capella["tenant_id"],
        "dontImportSampleData": skip_import_sample,
    }

    if dp_id is not None:
        if "overRide" not in config:
            config['overRide'] = {}
        config['overRide']['dataplaneId'] = dp_id
        return config

    if dataplane_id:
        if "overRide" not in config:
            config['overRide'] = {}
        config['overRide']['dataplaneId'] = dataplane_id
    return config

def set_custom_bucket_width(config, width=None, weight=None):
    override = {}
    if width:
        override["width"] = width
    if weight:
        override["weight"] = weight
    config["overRide"] = override