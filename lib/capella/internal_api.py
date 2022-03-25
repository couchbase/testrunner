import random
import uuid
from TestInput import TestInputServer
import base64
import time
import json
import httplib2
http = httplib2.Http(timeout=600, disable_ssl_certificate_validation=True)


class pod:
    def __init__(self, url):
        self.url = url


class tenant:
    def __init__(self, id, user, pwd):
        self.id = id
        self.user = user
        self.pwd = pwd
        self.api_secret_key = None
        self.api_access_key = None
        self.project_id = None
        self.clusters = dict()


def base64_encode(string):
    return base64.b64encode(string.encode('utf-8')).decode('utf-8')


class capella_utils():
    memcached_port = "11207"
    jwt = None

    @staticmethod
    def get_authorization_internal(pod, tenant):
        if capella_utils.jwt is None:
            basic = base64.b64encode('{}:{}'.format(
                tenant.user, tenant.pwd).encode('utf-8')).decode('utf-8')
            _, content = http.request(
                "{}/sessions".format(pod.url), method="POST", headers={"Authorization": "Basic %s" % basic})
            capella_utils.jwt = json.loads(content).get("jwt")
        cbc_api_request_headers = {
            'Authorization': 'Bearer %s' % capella_utils.jwt,
            'Content-Type': 'application/json'
        }
        return cbc_api_request_headers

    @staticmethod
    def create_project(pod, tenant, name):
        project_details = {
            "name": name,
            "tenantId": tenant.id
        }

        uri = '{}/v2/organizations/{}/projects'.format(pod.url, tenant.id)
        capella_header = capella_utils.get_authorization_internal(pod, tenant)
        response, content = http.request(uri, method="POST", body=json.dumps(
            project_details), headers=capella_header)
        project_id = json.loads(content).get("id")
        tenant.project_id = project_id
        # print("Project ID: {}".format(project_id))

    @staticmethod
    def delete_project(pod, tenant):
        uri = '{}/v2/organizations/{}/projects/{}'.format(
            pod.url, tenant.id, tenant.project_id)
        response, content = http.request(
            uri, method="DELETE", body='', headers=capella_utils.get_authorization_internal(pod, tenant))
        # print("Project Deleted: {}".format(tenant.project_id))

    @staticmethod
    def generate_cidr():
        first = random.randint(0, 256)
        second = random.randint(0, 16) * 16
        return f"10.{first}.{second}.0/20"

    @staticmethod
    def create_cluster(pod, tenant, cluster_details):
        while True:
            subnet = capella_utils.generate_cidr()
            # print("Trying with cidr: {}".format(subnet))
            cluster_details.update(
                {"cidr": subnet, "projectId": tenant.project_id})
            uri = '{}/v2/organizations/{}/clusters/deploy'.format(pod.url, tenant.id)
            response, content = http.request(uri, method="POST", body=json.dumps(
                cluster_details), headers=capella_utils.get_authorization_internal(pod, tenant))
            if int(response.get("status")) >= 200 and int(response.get("status")) < 300:
                # print("Cluster created successfully!")
                break
        cluster_id = json.loads(content).get("id")
        print(
            "Cluster created with cluster ID: {}".format(cluster_id))
        capella_utils.wait_until_done(
            pod, tenant, cluster_id, "Creating Cluster")
        cluster_srv = capella_utils.get_cluster_srv(pod, tenant, cluster_id)
        capella_utils.add_allowed_ip(pod, tenant, cluster_id)
        servers = capella_utils.get_nodes(pod, tenant, cluster_id)
        return cluster_id, cluster_srv, servers

    @staticmethod
    def wait_until_done(pod, tenant, cluster_id, msg="", prnt=False):
        while True:
            try:
                content = capella_utils.jobs(pod, tenant, cluster_id)
                state = capella_utils.get_cluster_state(
                    pod, tenant, cluster_id)
                # if prnt:
                #     print(content)
                if content.get("data") or state != "healthy":
                    for data in content.get("data"):
                        data = data.get("data")
                        if data.get("clusterId") == cluster_id:
                            step, progress = data.get("currentStep"), data.get(
                                "completionPercentage")
                            print(
                                "{}: Status=={}, State=={}, Progress=={}%".format(msg, state, step, progress))
                    time.sleep(2)
                else:
                    print("{} Ready!!!".format(msg))
                    break
            except:
                print("ERROR!!!")
                break

    @staticmethod
    def destroy_cluster(pod, tenant, cluster_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        response, content = http.request(base_url_internal, method="DELETE",
                                         body='', headers=capella_utils.get_authorization_internal(pod, tenant))

    @staticmethod
    def create_bucket(pod, tenant, cluster_id, bucket_params={}):
        while True:
            state = capella_utils.get_cluster_state(pod, tenant, cluster_id)
            if state == "healthy":
                break
            time.sleep(1)
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/buckets'.format(base_url_internal)
        default = {"name": "default", "bucketConflictResolution": "seqno", "memoryAllocationInMb": 100,
                   "flush": False, "replicas": 0, "durabilityLevel": "none", "timeToLive": None}
        default.update(bucket_params)
        response, content = http.request(uri, method="POST", body=json.dumps(
            default), headers=capella_utils.get_authorization_internal(pod, tenant))
        if int(response.get("status")) >= 200 and int(response.get("status")) < 300:
            print("Bucket create successfully!")

    @staticmethod
    def delete_bucket(pod, tenant, cluster_id, name):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/buckets'.format(base_url_internal)
        response, content = http.request(
            uri, method="GET", body='', headers=capella_utils.get_authorization_internal(pod, tenant))
        content = json.loads(content)
        bucket_id = None
        # print(content)
        for bucket in content.get("buckets").get("data"):
            if bucket.get("data").get("name") == name:
                bucket_id = bucket.get("data").get("id")
        if bucket_id:
            uri = uri + "/" + bucket_id
            response, content = http.request(
                uri, method="DELETE", headers=capella_utils.get_authorization_internal(pod, tenant))
            if int(response.get("status")) >= 200 and int(response.get("status")) < 300:
                print("Bucket deleted successfully!")
            else:
                pass
                # print(content)
        else:
            print("Bucket not found.")

    @staticmethod
    def scale(pod, tenant, cluster_id, scale_params):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/specs'.format(base_url_internal)
        scale_params = json.dumps(scale_params)
        response, content = http.request(
            uri, method="POST", body=scale_params, headers=capella_utils.get_authorization_internal(pod, tenant))
        return response, content
        # time.sleep(10)
        # capella_utils.wait_until_done(pod, tenant, cluster.id, "Scaling Operation")

    @staticmethod
    def jobs(pod, tenant, cluster_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/jobs'.format(base_url_internal)
        response, content = http.request(
            uri, method="GET", body='', headers=capella_utils.get_authorization_internal(pod, tenant))
        return json.loads(content)

    @staticmethod
    def get_cluster_info(pod, tenant, cluster_id):
        uri = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        response, content = http.request(
            uri, method="GET", body='', headers=capella_utils.get_authorization_internal(pod, tenant))
        return json.loads(content)

    @staticmethod
    def get_cluster_state(pod, tenant, cluster_id):
        content = capella_utils.get_cluster_info(pod, tenant, cluster_id)
        # print(content)
        return content.get("data").get("status").get("state")

    @staticmethod
    def get_cluster_srv(pod, tenant, cluster_id):
        content = capella_utils.get_cluster_info(pod, tenant, cluster_id)
        return content.get("data").get("connect").get("srv")

    @staticmethod
    def get_nodes(pod, tenant, cluster_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/nodes'.format(base_url_internal)
        response, content = http.request(
            uri, method="GET", body='', headers=capella_utils.get_authorization_internal(pod, tenant))
        # print(json.loads(content))
        return [server.get("data") for server in json.loads(content).get("data")]

    @staticmethod
    def get_nodes_formatted(pod, tenant, cluster_id, username=None, password=None):
        servers = capella_utils.get_nodes(pod, tenant, cluster_id)
        nodes = list()
        for server in servers:
            temp_server = TestInputServer()
            temp_server.ip = server.get("hostname")
            temp_server.hostname = server.get("hostname")
            capella_services = server.get("services")
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
            nodes.append(temp_server)
        return nodes

    @staticmethod
    def create_db_user(pod, tenant, cluster_id, user, pwd):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        body = {"name": user, "password": pwd, "permissions": {
            "data_reader": {}, "data_writer": {}}}
        uri = '{}/users'.format(base_url_internal)
        response, content = http.request(uri, method="POST", body=json.dumps(
            body), headers=capella_utils.get_authorization_internal(pod, tenant))
        # print(json.loads(content))
        return json.loads(content)

    @staticmethod
    def add_allowed_ip(pod, tenant, cluster_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        _, content = http.request("https://ifconfig.me/all.json", method="GET")
        ip = json.loads(content).get("ip_addr")
        body = {"create": [{"cidr": "{}/32".format(ip), "comment": ""}]}
        uri = '{}/allowlists-bulk'.format(base_url_internal)
        _, content = http.request(uri, method="POST", body=json.dumps(
            body), headers=capella_utils.get_authorization_internal(pod, tenant))

    @staticmethod
    def setup_replication(pod, tenant, cluster_id, source_bucket, target, settings, direction):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)

        target["bucket"] = base64_encode(target["bucket"])
        source_bucket = base64_encode(source_bucket)

        body = {"sourceBucket": source_bucket, "target": target,
                "settings": settings, "direction": direction}
        uri = '{}/xdcr'.format(base_url_internal)
        _, content = http.request(uri, method="POST", body=json.dumps(
            body), headers=capella_utils.get_authorization_internal(pod, tenant))

    @staticmethod
    def list_replications(pod, tenant, cluster_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/xdcr'.format(base_url_internal)
        _, content = http.request(
            uri, method="GET", headers=capella_utils.get_authorization_internal(pod, tenant))
        resp = json.loads(content)
        replications = []
        for row in resp["data"]:
            replications.append(row["data"])
        return replications

    @staticmethod
    def pause_replication(pod, tenant, cluster_id, replication_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/xdcr/{}/pause'.format(base_url_internal, replication_id)
        _, content = http.request(
            uri, method="POST", headers=capella_utils.get_authorization_internal(pod, tenant))
        print(content)

    @staticmethod
    def resume_replication(pod, tenant, cluster_id, replication_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/xdcr/{}/start'.format(base_url_internal, replication_id)
        _, content = http.request(
            uri, method="POST", headers=capella_utils.get_authorization_internal(pod, tenant))

    @staticmethod
    def delete_replication(pod, tenant, cluster_id, replication_id):
        base_url_internal = '{}/v2/organizations/{}/projects/{}/clusters/{}'.format(
            pod.url, tenant.id, tenant.project_id, cluster_id)
        uri = '{}/xdcr/{}'.format(base_url_internal, replication_id)
        _, content = http.request(
            uri, method="DELETE", headers=capella_utils.get_authorization_internal(pod, tenant))

    @staticmethod
    def create_function(pod, tenant, cluster_id, name, body, function_scope=None):
        base_url_proxy = '{}/v2/databases/{}/proxy'.format(pod.url, cluster_id)
        uri = '{}/_p/event/api/v1/functions/{}'.format(base_url_proxy, name)

        if function_scope is not None:
            uri += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        status, content = http.request(
            uri, method="POST", body=json.dumps(body), headers=capella_utils.get_authorization_internal(pod, tenant))
        return status, content

    @staticmethod
    def function_settings(pod, tenant, cluster_id, name, body, function_scope=None):
        base_url_proxy = '{}/v2/databases/{}/proxy'.format(pod.url, cluster_id)
        uri = '{}/_p/event/api/v1/functions/{}/settings'.format(
            base_url_proxy, name)

        if function_scope is not None:
            uri += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])

        status, content = http.request(
            uri, method="POST", body=json.dumps(body), headers=capella_utils.get_authorization_internal(pod, tenant))

        return status, content

    @staticmethod
    def get_composite_eventing_status(pod, tenant, cluster_id):
        base_url_proxy = '{}/v2/databases/{}/proxy'.format(pod.url, cluster_id)
        uri = '{}/_p/event/api/v1/status'.format(base_url_proxy)
        status, content = http.request(
            uri, method="GET", headers=capella_utils.get_authorization_internal(pod, tenant))
        return status, content

    @staticmethod
    def get_all_eventing_stats(pod, tenant, cluster_id, seqs_processed=False):
        base_url_proxy = '{}/v2/databases/{}/proxy'.format(pod.url, cluster_id)
        uri = '{}/_p/event/api/v1/status'.format(base_url_proxy)
        if seqs_processed:
            uri += "?type=full"
        status, content = http.request(
            uri, method="GET", headers=capella_utils.get_authorization_internal(pod, tenant))
        return status, content

    @staticmethod
    def delete_function(pod, tenant, cluster_id, name, function_scope=None):
        base_url_proxy = '{}/v2/databases/{}/proxy'.format(pod.url, cluster_id)
        uri = '{}/_p/event/deleteAppTempStore/?name={}'.format(
            base_url_proxy, name)
        if function_scope is not None:
            uri += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        status, content = http.request(
            uri, method="GET", headers=capella_utils.get_authorization_internal(pod, tenant))
        return status, content

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def spec_options_from_input(input):
        def get_ini_or_param(key, default):
            value = input.param(key, None)
            if value is None:
                value = input.capella.get(key)
            return value or default

        provider = get_ini_or_param("provider", "aws")

        default_region = "us-east-2" if provider == "aws" else "us-east-1"
        region = get_ini_or_param("region", default_region)

        default_compute = "m5.xlarge" if provider == "aws" else "n2-standard"
        compute = get_ini_or_param("compute", default_compute)

        default_disk_type = "gp3" if provider == "aws" else "pd-ssd"
        disk_type = get_ini_or_param("disk_type", default_disk_type)

        default_disk_iops = 3000 if provider == "aws" else None
        disk_iops = get_ini_or_param("disk_iops", default_disk_iops)
        if disk_iops:
            disk_iops = int(disk_iops)

        default_disk_size = 50
        disk_size = int(get_ini_or_param("disk_size", default_disk_size))

        return provider, region, compute, disk_type, disk_iops, disk_size

    @staticmethod
    def create_capella_config(input, services_count):
        provider, region, compute, disk_type, disk_iops, disk_size = capella_utils.spec_options_from_input(
            input)

        specs = capella_utils.create_specs(
            provider, services_count, compute, disk_type, disk_iops, disk_size)

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
            "package": "developerPro",
            "projectId": None,
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
