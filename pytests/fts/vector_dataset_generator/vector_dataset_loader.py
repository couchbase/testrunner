

try:
    import docker
except ImportError:
    print("WARN: fail to import docker")


class VectorLoader:

    def __init__(self, node, username, password, bucket, scope, collection, dataset, capella=False,
                 create_bucket_struct=False, use_cbimport=False, dims_for_resize=[], percentages_to_resize=[],
                 iterations=1, update=False, faiss_indexes=[], faiss_index_node='127.0.0.1'):

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
        self.iterations = iterations
        self.create_bucket_struct = create_bucket_struct
        self.docker_client = docker.from_env()
        self.percentages_to_resize = percentages_to_resize
        self.dims_for_resize = dims_for_resize
        self.update = update
        if self.update:
            self.use_cbimport = False
        self.faiss_indexes = faiss_indexes
        self.faiss_index_node = faiss_index_node

    def load_data(self, container_name=None):
        try:
            docker_image = "sequoiatools/vectorloader"
            dataset_name = self.dataset[0]
            docker_run_params = f"-n {self.node.ip} -u {self.username} -p {self.password} " \
                                f"-b {self.bucket} -sc {self.scope} -coll {self.collection} " \
                                f"-ds {dataset_name} -c {self.capella_run} -cbs {self.create_bucket_struct} -i {self.use_cbimport} " \
                                f"-iter {self.iterations}"

            if len(self.percentages_to_resize) > 0:
                total_percentage = 0
                for per in self.percentages_to_resize:
                    total_percentage += per

                if total_percentage > 1:
                    raise ValueError("Total percentage of docs to update should be less than 1.")

                per_arg = "-per"
                for per in self.percentages_to_resize:
                    per_arg += " " + str(per)

                docker_run_params += " " + per_arg

                if len(self.dims_for_resize) > 0:
                    dims_arg = "-dims"
                    for dim in self.dims_for_resize:
                        dims_arg += " " + str(dim)

                    docker_run_params += " " + dims_arg

                if self.update:
                    docker_run_params += " -update " + str(self.update)

                if len(self.faiss_indexes) > 0:
                    faiss_index_arg = "-indexes"
                    for faiss_index in self.faiss_indexes:
                        faiss_index_arg += " " + faiss_index

                    docker_run_params += " " + faiss_index_arg

                    docker_run_params += " -fn" + " " + self.faiss_index_node

                print("docker run params: {}".format(docker_run_params))

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


class GoVectorLoader:
    def __init__(self, node, username, password, bucket, scope, collection, dataset, xattr, prefix, si, ei, base64,
                 percentage_to_resize=[], dimension_to_resize=[]):
        self.node = node
        self.username = username
        self.password = password
        self.bucket = bucket
        self.scope = scope
        self.collection = collection
        self.dataset = dataset
        self.prefix = prefix
        self.xattr = xattr
        self.si = si
        self.ei = ei
        self.base64 = base64
        self.percentage_to_resize = percentage_to_resize
        self.dimension_to_resize = dimension_to_resize
        self.docker_client = docker.from_env()

    def load_data(self, container_name=None):
        try:
            docker_image = "sequoiatools/govectorloader"
            if len(self.percentage_to_resize) == 0 or len(self.dimension_to_resize) == 0:
                docker_run_params = f"-nodeAddress={self.node.ip} -bucketName={self.bucket} -scopeName={self.scope} -collectionName={self.collection} -documentIdPrefix={self.prefix} -username={self.username} -password={self.password} -datasetName={self.dataset} -startIndex={self.si} -endIndex={self.ei}  -base64Flag={self.base64} -xattrFlag={self.xattr}"
            else:
                pr = ','.join(map(str, self.percentage_to_resize))
                dr = ','.join(map(str, self.dimension_to_resize))
                docker_run_params = f"-nodeAddress={self.node.ip} -bucketName={self.bucket} -scopeName={self.scope} -collectionName={self.collection} -documentIdPrefix={self.prefix} -username={self.username} -password={self.password} -datasetName={self.dataset} -startIndex={self.si} -endIndex={self.ei} -base64Flag={self.base64} -xattrFlag={self.xattr} -percentagesToResize={pr} -dimensionsForResize={dr}"
            print("docker run params: {}".format(docker_run_params))

            # Run the Docker pull command
            try:
                print(f"Pulling docker image {docker_image}")
                self.docker_client.images.pull('sequoiatools/govectorloader')
            except docker.errors.APIError as e:
                print("Exception will pulling docker image {}: {}".
                      format(docker_image, e))

            # Run the Docker run command
            try:
                print(f"Running docker container {docker_image} with name {container_name}")
                docker_output = self.docker_client.containers.run(docker_image, docker_run_params,
                                                                  name=container_name)
                # print(docker_output)
            except Exception as e:
                print(f"Exception while running docker container: {e}")

        except Exception as e:
            print(f"Error: {e}")
