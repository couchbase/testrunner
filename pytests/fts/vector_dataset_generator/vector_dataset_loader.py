
try:
    import docker
except ImportError:
    print("WARN: fail to import docker")


class VectorLoader:

    def __init__(self, node, username, password, bucket, scope, collection, dataset, capella=False,
                 create_bucket_struct=False, use_cbimport=False, dims_for_resize=[], percentages_to_resize=[],
                 iterations=1, update=False, faiss_indexes=[], faiss_index_node='127.0.0.1',
                 start_key=0):

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
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            self.docker_client = None
            print(f"WARN: docker unavailable for VectorLoader: {e}")
        self.percentages_to_resize = percentages_to_resize
        self.dims_for_resize = dims_for_resize
        self.update = update
        if self.update:
            self.use_cbimport = False
        self.faiss_indexes = faiss_indexes
        self.faiss_index_node = faiss_index_node
        self.start_key = start_key

    def load_data(self, container_name=None):
        try:
            if self.docker_client is None:
                raise RuntimeError("Docker is unavailable; cannot run VectorLoader container.")
            docker_image = "sequoiatools/vectorloader:v1.1"
            dataset_name = self.dataset[0]
            docker_run_params = f"-n {self.node.ip} -u {self.username} -p {self.password} " \
                                f"-b {self.bucket} -sc {self.scope} -coll {self.collection} " \
                                f"-ds {dataset_name} -c {self.capella_run} -cbs {self.create_bucket_struct} -i {self.use_cbimport} " \
                                f"-iter {self.iterations} -sk {int(self.start_key)}"

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
                self.docker_client.images.pull(docker_image)
            except docker.errors.APIError as e:
                print("Exception will pulling docker image {}: {}".
                      format(docker_image, e))

            # Run the Docker run command
            try:
                print(f"Running docker container {docker_image} with name {container_name} \n {docker_run_params}")
                docker_output = self.docker_client.containers.run(docker_image, docker_run_params,
                                                                  name=container_name)
            except Exception as e:
                print(f"Exception while running docker container: {e}")

        except Exception as e:
            print(f"Error: {e}")


class GoVectorLoader:
    def __init__(self, node, username, password, bucket, scope, collection, dataset, xattr, prefix, si, ei, base64,
                 percentage_to_resize=[], dimension_to_resize=[],load_invalid_vecs=False,invalid_vecs_dims = 128,
                 provideDefaultDocs=False,batchSize=300,
                 docSchema=None, departmentsCount=None, projectsPerDept=None, locationsCount=None,
                 employeesPerDept=None, embeddingFieldName=None, seed=None):
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
        self.load_invalid_vecs = load_invalid_vecs
        self.invalid_vecs_dims = invalid_vecs_dims
        self.provideDefaultDocs = provideDefaultDocs
        self.batchSize = batchSize
        # hierarchical/nested doc generation options (optional)
        self.docSchema = docSchema
        self.departmentsCount = departmentsCount
        self.projectsPerDept = projectsPerDept
        self.locationsCount = locationsCount
        self.employeesPerDept = employeesPerDept
        self.embeddingFieldName = embeddingFieldName
        self.seed = seed
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            self.docker_client = None
            print(f"WARN: docker unavailable for GoVectorLoader: {e}")

    def load_data(self, container_name=None):
        if self.docker_client is None:
            raise RuntimeError("Docker is unavailable; cannot run GoVectorLoader container. "
                               "Either run on a machine with Docker or pre-load docs and set hierarchical_skip_doc_load=True.")
        try:
            cont = self.docker_client.containers.get("upgrade")
            cont.stop()
            cont.remove()
        except Exception as e:
            print(e)

        try:
            docker_image = "sequoiatools/govectorloader"
            if len(self.percentage_to_resize) == 0 or len(self.dimension_to_resize) == 0:
                docker_run_params = f"-nodeAddress={self.node.ip} -bucketName={self.bucket} -scopeName={self.scope} -collectionName={self.collection} -documentIdPrefix={self.prefix} -username={self.username} -password={self.password} -datasetName={self.dataset} -startIndex={self.si} -endIndex={self.ei}  -base64Flag={self.base64} -xattrFlag={self.xattr} -invalidVecsLoader={self.load_invalid_vecs} -invalidDimensions={self.invalid_vecs_dims} -provideDefaultDocs={self.provideDefaultDocs} -batchSize={self.batchSize}"
            else:
                pr = ','.join(map(str, self.percentage_to_resize))
                dr = ','.join(map(str, self.dimension_to_resize))
                docker_run_params = f"-nodeAddress={self.node.ip} -bucketName={self.bucket} -scopeName={self.scope} -collectionName={self.collection} -documentIdPrefix={self.prefix} -username={self.username} -password={self.password} -datasetName={self.dataset} -startIndex={self.si} -endIndex={self.ei} -base64Flag={self.base64} -xattrFlag={self.xattr} -percentagesToResize={pr} -dimensionsForResize={dr} -invalidVecsLoader={self.load_invalid_vecs} -invalidDimensions={self.invalid_vecs_dims} -provideDefaultDocs={self.provideDefaultDocs} -batchSize={self.batchSize}"

            # Optional hierarchical doc schema parameters
            if self.docSchema:
                docker_run_params += f" -docSchema {self.docSchema}"
            if self.departmentsCount is not None:
                docker_run_params += f" -departmentsCount {self.departmentsCount}"
            if self.projectsPerDept is not None:
                docker_run_params += f" -projectsPerDept {self.projectsPerDept}"
            if self.locationsCount is not None:
                docker_run_params += f" -locationsCount {self.locationsCount}"
            if self.employeesPerDept is not None:
                docker_run_params += f" -employeesPerDept {self.employeesPerDept}"
            if self.embeddingFieldName:
                docker_run_params += f" -embeddingFieldName {self.embeddingFieldName}"
            if self.seed is not None:
                docker_run_params += f" -seed {self.seed}"
            print("docker run params: {}".format(docker_run_params))

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
                                                                  name=container_name)
                # print(docker_output)
            except Exception as e:
                print(f"Exception while running docker container: {e}")

        except Exception as e:
            print(f"Error: {e}")

