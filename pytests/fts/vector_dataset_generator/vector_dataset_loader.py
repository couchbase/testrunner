import docker

class VectorLoader:

    def __init__(self, node, username, password, bucket, scope, collection, dataset, capella=False,
                 create_bucket_struct=False, use_cbimport=False):

        self.node = node
        self.username = username
        self.password = password
        self.bucket = bucket
        self.dataset = dataset
        print(type(self.dataset))
        if not isinstance(self.dataset, list):
            self.dataset = [self.dataset]
        self.scope = scope
        self.collection = collection
        self.capella_run = capella
        self.use_cbimport = use_cbimport
        self.create_bucket_struct = create_bucket_struct
        self.docker_client = docker.from_env()

    def load_data(self, container_name=None):
        try:
            docker_image = "sequoiatools/vectorloader"
            dataset_name = self.dataset[0]
            docker_run_params = f"-n {self.node.ip} -u {self.username} -p {self.password} " \
                                f"-b {self.bucket} -sc {self.scope} -coll {self.collection} " \
                                f"-ds {dataset_name} -c {self.capella_run} -cbs {self.create_bucket_struct} -i {self.use_cbimport}"

            # Run the Docker pull command
            try:
                print(f"Pulling docker image {docker_image}")
                self.docker_client.images.pull('sequoiatools/vectorloader')
            except docker.errors.APIError as e:
                print("Exception will pulling docker image {}: {}".
                      format(docker_image, e))

            # Run the Docker run command
            try:
                print(f"Running docker container {docker_image} with name {container_name}")
                docker_output = self.docker_client.containers.run(docker_image, docker_run_params,
                                                                  name=container_name)
            except Exception as e:
                print(f"Exception while running docker container: {e}")

        except Exception as e:
            print(f"Error: {e}")
