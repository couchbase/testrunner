
try:
    import docker
except ImportError:
    print("WARN: fail to import docker")

class DocFilterLoader:
    def __init__(self, node, username, password, bucket, scope, collection,prefix):
        self.node = node
        self.username = username
        self.password = password
        self.bucket = bucket
        self.scope = scope
        self.collection = collection
        self.docker_client = docker.from_env()
        self.prefix = prefix

    def load_data(self, container_name=None):
        try:
            cont = self.docker_client.containers.get("doc_filter_container")
            cont.stop()
            cont.remove()
        except Exception as _:
            print("Exception expected. Container not found\n")

        try:
            docker_image = "sequoiatools/docfilter_datagen:v2.1"
            docker_run_params = f"--host {self.node.ip} --bucket {self.bucket} --scope {self.scope} --collection {self.collection} --username {self.username} --password {self.password} --docprefix {self.prefix}"
            print("Docker run params: {}".format(docker_run_params))

            # Run the Docker pull command
            try:
                print(f"Pulling docker image {docker_image}")
                self.docker_client.images.pull(docker_image)
            except docker.errors.APIError as e:
                print("Exception will pulling docker image {}: {}".
                      format(docker_image, e))

            # Run the Docker run command
            try:
                print(f"Running docker container {docker_image} with name {container_name}")
                docker_output = self.docker_client.containers.run(docker_image, docker_run_params,
                                                                  name=container_name,user="root",security_opt=["seccomp=unconfined"])
                print(f"Docfilter datagen result: {docker_output}")
                return docker_output
            except Exception as e:
                print(f"Exception while running docker container: {e}")

        except Exception as e:
            print(f"Error: {e}")