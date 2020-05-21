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
            #self.client.remove_container(self.handle.id)
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


    def params_to_environment(self):
        _environment = {}
        _environment["CLUSTER"] = self.server.ip
        #_environment["USERNAME"] = self.server.rest_username
        #_environment["PASSWORD"] = self.server.rest_password
        _environment["BUCKET"] = self.bucket
        _environment["SCOPE"] = self.params.scope
        _environment["COLLECTION"] = self.params.collection
        _environment["N"] = self.params.num_ops
        _environment["PC"] = self.params.percent_create
        _environment["PU"] = self.params.percent_update
        _environment["PD"] = self.params.percent_delete
        _environment["L"] = self.params.load_pattern
        _environment["DSN"] = self.params.start_seq_num
        _environment["DPX"] = self.params.key_prefix
        _environment["DSX"] = self.params.key_suffix
        _environment["DT"] = self.params.json_template
        _environment["O"] = self.params.print_sdk_logs

        return _environment

    def do_ops(self):
        # 1 docker image per bucket, identified by server_bucket tag
        self.docker_instance = DockerManager(self.server.ip + '_' + self.bucket)
        env = self.params_to_environment()
        self.docker_instance.start(env)