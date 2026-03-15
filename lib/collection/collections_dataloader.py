import docker
from requests.exceptions import ConnectionError


class SDKDocloaderParams(object):
    """Parameters for Java SDK Client document loader"""
    def __init__(self, username, password, json_template, key_prefix, key_suffix,
                 start_seq_num, num_ops, percent_create, percent_update, percent_delete,
                 load_pattern, print_sdk_logs, scope="", collection=""):
        self.username = username
        self.password = password
        self.json_template = json_template
        self.key_prefix = key_prefix
        self.key_suffix = key_suffix
        self.start_seq_num = start_seq_num
        self.num_ops = num_ops
        self.percent_create = percent_create
        self.percent_update = percent_update
        self.percent_delete = percent_delete
        self.load_pattern = load_pattern
        self.print_sdk_logs = print_sdk_logs
        self.scope = scope
        self.collection = collection


class DockerManager(object):
    def __init__(self, tag):
        self.tag = tag
        self.environment = None
        self.client = docker.from_env()
        self.handle = None

    def start(self, env):
        self.environment = env
        print(self.environment)
        try:
            image = self.client.images.get("jsc:latest")
            image.tag("jsc", tag=self.tag)
            self.handle = self.client.containers.run("jsc:" + self.tag,
                                                     environment=self.environment,
                                                     detach=True)
            self.stream_logs()
            self.terminate()
        except ConnectionError as e:
            print('Error connecting to docker service, please start/restart it:', e)

    def stream_logs(self):
        for line in self.handle.logs(stream=True):
            if b"Exception:" in line:
                raise Exception("Exception occurred {}".format(line))

def terminate(self, tag):
        self.client.images.remove(tag)
        self.client.containers.prune()
class JavaSDKClient(object):
    def __init__(self, server, bucket, params):
        self.server = server
        self.bucket = bucket
        self.params = params
        self.tag = self.server.ip + '_' + self.bucket
        self.docker_instance = DockerManager(self.tag)

    def params_to_environment(self):
        _environment = {
                        "CLUSTER": self.server.ip,
                        "USERNAME": self.params.username,
                        "PASSWORD": self.params.password,
                        "BUCKET": self.bucket,
                        "SCOPE": self.params.scope,
                        "COLLECTION": self.params.collection,
                        "N": self.params.num_ops,
                        "PC": self.params.percent_create,
                        "PU": self.params.percent_update,
                        "PD": self.params.percent_delete,
                        "L": self.params.load_pattern,
                        "DSN": self.params.start_seq_num,
                        "DPX": self.params.key_prefix,
                        "DSX": self.params.key_suffix,
                        "DT": self.params.json_template,
                        "O": self.params.print_sdk_logs
                        }
        return _environment

    def do_ops(self):
        # 1 docker image per bucket, identified by server_bucket tag
        env = self.params_to_environment()
        self.docker_instance.start(env)

    def cleanup(self):
        self.docker_instance.terminate("jsc" + ':' + self.tag)
