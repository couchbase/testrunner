import docker
from requests.exceptions import ConnectionError


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
            for line in self.handle.logs(stream=True):
                print(line.strip())
            # self.client.remove_container(self.handle.id)
        except ConnectionError as e:
            print('Error connecting to docker service, please start/restart it:', e)

    def _list_containers(self):
        images = []
        for image in self.client.images.list():
            images.append(image.id)
        return images

    def check_status(self):
        pass

    def stream_logs(self):
        pass

    def terminate(self):
        for container in self._list_images():
            container.stop()



class JavaSDKClient(object):
    def __init__(self, server, bucket, params):
        self.server = server
        self.bucket = bucket
        self.params = params
        self.docker_instance = DockerManager(self.server.ip + '_' + self.bucket)

    def params_to_environment(self):
        _environment = {
                        "CLUSTER": self.server.ip,
                        "USERNAME": self.server.rest_username or "Administrator",
                        "PASSWORD": self.server.rest_password or "password",
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
