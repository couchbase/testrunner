"""
Downloads dataset, extract vectors and dumps into couchbase.
"""
import concurrent.futures
import time
import uuid
from datetime import timedelta
from functools import partial

import docker
import pkg_resources
import requests

from .vector_dataset_generator import VectorDataset

min_sdk_version = "3.0.0"
couchbase_version = pkg_resources.get_distribution("couchbase").version
print(f"Import: Couchbase SDK version: {couchbase_version}")

if pkg_resources.parse_version(couchbase_version) >= pkg_resources.parse_version(min_sdk_version):
    print("Import: Couchbase SDK version is greater than or equal to 3.0.0")
    from couchbase.cluster import Cluster, ClusterOptions
    from couchbase.auth import PasswordAuthenticator
    from couchbase.exceptions import (
        BucketAlreadyExistsException,
        CollectionAlreadyExistsException,
        ScopeAlreadyExistsException
    )

    from couchbase.cluster import ClusterTimeoutOptions
    from couchbase.management.buckets import BucketType, CreateBucketSettings, ConflictResolutionType
    from couchbase.management.collections import CollectionSpec


########################################################################################
# Func to upsert vector to couchbase collection.
def upsert_vector(collection, counter, vector, dataset_name):
    id = str(uuid.uuid4())
    sno = counter
    sname = number_to_alphabetic(counter)
    data_record = {
        "sno": sno,
        "sname": sname,
        "id": id,
        "vector_data": vector.tolist()
    }
    last_print_time = time.time()
    for retry in range(3):
        try:
            elapsed_time = time.time() - last_print_time
            if elapsed_time >= 300:
                print(f"From dataset {dataset_name} Uploading vector no: {counter} with ID: {id} to {collection.name}")
                last_print_time = time.time()
            collection.upsert(id, data_record)
        except Exception as e:
            print(f"{e} Error uploading vector no: {counter} with id: {id} to collection: {collection.name}")
            retry += 1
            print(f"{e} Retrying after 1sec.. {retry} {id}")
            time.sleep(1)


def number_to_alphabetic(n):
    """Gives the alphabet equivalent to the number mentioned.

    Args:
        n (int): number

    Returns:
        _type_: _description_
    """
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(remainder + ord('a')) + result
    return result


########################################################################################

class CouchbaseOps:
    """
        CouchbaseOps provides a way to
        - Create necessary buckets, scopes and
        collectionreate data into couchbase collection
        - Create fts index on vector data fields using supported index types
        - Retrieve documents based on the index query.
    """
    couchbase_endpoint_ip = ""
    username = ""
    password = ""
    dataset_name = ""
    prefix_for_buckets = ""
    bucket_name = ""
    scope_name = ""
    collection_name = ""

    def __init__(
            self,
            couchbase_endpoint_ip,
            username="Administrator",
            password="password",
            dataset_name="sift",
            bucket_name="",
            scope_name="",
            collection_name="",
            capella_run=False,
            cbs=False
    ):
        self.couchbase_endpoint_ip = couchbase_endpoint_ip
        self.username = username
        self.password = password
        self.dataset_name = dataset_name
        self.prefix_for_buckets = "VS"
        if bucket_name == "":
            self.bucket_name = (
                    self.prefix_for_buckets + "_vector_bucket_" + self.dataset_name.upper()
            )
        else:
            self.bucket_name = bucket_name
        if scope_name == "":
            self.scope_name = self.prefix_for_buckets + "_vector_scope_" + self.dataset_name.upper()
        else:
            self.scope_name = scope_name

        if collection_name == "":
            self.collection_name = (
                    self.prefix_for_buckets + "_vector_collection_" + self.dataset_name.upper()
            )
        else:
            self.collection_name = collection_name
            self.capella_run = capella_run
            self.cbs = cbs

    def create_bucket(self, cluster):
        bucket_manager = cluster.buckets()
        try:
            print("Creating bucket: {}".format(self.bucket_name))
            bucket_manager.create_bucket(
                CreateBucketSettings(
                    name=self.bucket_name,
                    flush_enabled=True,
                    ram_quota_mb=100,
                    num_replicas=0,
                    conflict_resolution_type=ConflictResolutionType.SEQUENCE_NUMBER,
                    bucket_type=BucketType.COUCHBASE))
        except BucketAlreadyExistsException:
            print("Bucket: {} already exists. So not creating it again".format(self.bucket_name))

    def create_scope(self):
        url = f"http://{self.couchbase_endpoint_ip}:8091/pools/default/buckets/{self.bucket_name}/scopes"
        data = {"name": self.scope_name}

        print(url)
        print(data)
        response = requests.post(url, auth=(self.username, self.password), json=data)

        if response.status_code == 200:
            print(f"Scope '{self.scope_name}' created successfully.")
        else:
            print(
                f"Failed to create scope. Status code: {response.status_code}, Response: {response.text}")

    def create_collection(self):
        url = f"http://{self.couchbase_endpoint_ip}:8091/pools/default/buckets/{self.bucket_name}/scopes/{self.scope_name}/collections"
        data = {"name": self.collection_name}
        print(url)
        print(data)
        response = requests.post(url, auth=(self.username, self.password), json=data)

        if response.status_code == 200:
            print(f"Collection '{self.collection_name}' created successfully.")
        else:
            print(
                f"Failed to create collection. Status code: {response.status_code}, Response: {response.text}")

    def create_bucket_scope_collection(self, cluster, couchbase_endpoint):
        """
        Creates couchbase bucket, scope and collection

        Returns:
            couchbase collection object
        """
        print(f"Creating bucket on {couchbase_endpoint} with bucket name:{self.bucket_name}")
        self.create_bucket(cluster)
        time.sleep(5)
        bucket = cluster.bucket(self.bucket_name)

        coll_manager = bucket.collections()
        try:
            print(f"Creating scope in bucket:{self.bucket_name} with scope name:{self.scope_name}")
            coll_manager.create_scope(self.scope_name)
            time.sleep(5)
        except ScopeAlreadyExistsException as e:
            print(f"Scope with name {self.scope_name} exists already, skipping creation again")
        except Exception as e:
            print(f"Scope Creation failed, collection name: {self.scope_name}")
            return

        collection_spec = CollectionSpec(
            self.collection_name,
            scope_name=self.scope_name)

        try:
            print(f"Creating collection in scope:{self.scope_name} with collection name:{self.collection_name}")
            collection = coll_manager.create_collection(collection_spec)
            time.sleep(5)
        except CollectionAlreadyExistsException as ex:
            print(f"Collection with name {self.collection_name} exists already, skipping creation again")
        except Exception as e:
            print(f"Error: Collection Creation failed, collection name: {self.collection_name}")

        collection = bucket.scope(self.scope_name).collection(self.collection_name)
        return collection

    def upsert(self):
        """
        Dumps train vectors into Couchbase collection which is created
        automatically

        Args:
            use_hdf5_datasets (bool, optional): To choose tar.gz or hdf5 files .
            Defaults to False.
        """
        auth = PasswordAuthenticator(self.username, self.password)
        if not self.capella_run:
            couchbase_endpoint = "couchbase://" + self.couchbase_endpoint_ip
            cluster = Cluster(couchbase_endpoint, ClusterOptions(auth))
        else:
            timeout_options = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=120),
                                                    query_timeout=timedelta(seconds=10))
            options = ClusterOptions(PasswordAuthenticator(self.username, self.password),
                                     timeout_options=timeout_options)
            cluster = Cluster('couchbases://' + self.couchbase_endpoint_ip + '?ssl=no_verify',
                              options)
            couchbase_endpoint = f"couchbases://{self.couchbase_endpoint_ip}"

        print(
            f"user:{self.username} pass: {self.password} endpoint: {couchbase_endpoint} bucket_name: {self.bucket_name} {self.scope_name}   {self.collection_name} "
        )

        # create Bucket, Scope and Collection.
        if self.cbs:
            time.sleep(10)
            collection = self.create_bucket_scope_collection(cluster, couchbase_endpoint)
            if collection is None:
                print(f"Error: collection object cannot be None")
                return
        else:
            bucket = cluster.bucket(self.bucket_name)
            collection = bucket.scope(self.scope_name).collection(self.collection_name)

        # initialize the needed vectors.
        ds = VectorDataset(self.dataset_name)
        use_hdf5_datasets = True
        if self.dataset_name in ds.supported_sift_datasets:
            use_hdf5_datasets = False
        ds.extract_vectors_from_file(use_hdf5_datasets, type_of_vec="train")

        # dump train vectors into couchbase collection in vector data
        # type fomat.
        if ds.train_vecs is not None and len(ds.train_vecs) > 0:
            print(f"Spawning {1000} threads to speedup the upsert.")
            with concurrent.futures.ThreadPoolExecutor(1000) as executor:
                upsert_partial = partial(upsert_vector, collection, dataset_name=self.dataset_name)
                futures = {executor.submit(upsert_partial, counter, d): d for counter, d in
                           enumerate(ds.train_vecs, start=1)}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Error: {e}")
        else:
            print("Error: train vectors data structure is empty, please check the dataset")


class VectorLoader:

    def __init__(self, node, username, password, bucket, scope, collection, dataset, capella=False,
                 create_bucket_struct=False):

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
        self.create_bucket_struct = create_bucket_struct
        self.docker_client = docker.from_env()

    def load_data(self, container_name=None):
        min_sdk_version = "3.0.0"
        try:
            couchbase_version = pkg_resources.get_distribution("couchbase").version
            print(f"Couchbase SDK version: {couchbase_version}")

            if pkg_resources.parse_version(couchbase_version) >= pkg_resources.parse_version(min_sdk_version):
                print("Couchbase SDK version is greater than or equal to 3.0.0")
                if self.capella_run and self.create_bucket_struct:
                    print("creating bucket isn't allowed with capella clusters using SDK, aborting")
                    return
                for dataset_name in self.dataset:
                    cbops = CouchbaseOps(
                        couchbase_endpoint_ip=self.node, username=self.username, password=self.password,
                        bucket_name=self.bucket,
                        dataset_name=dataset_name,
                        scope_name=self.scope, collection_name=self.collection,
                        capella_run=self.capella_run,
                        cbs=self.create_bucket_struct
                    )
                    cbops.upsert()
            else:
                print("Couchbase SDK version is less than 3.0.0.")
                docker_image = "sequoiatools/vectorloader"
                dataset_name = self.dataset[0]
                docker_run_params = f"-n {self.node.ip} -u {self.username} -p {self.password} " \
                                    f"-b {self.bucket} -sc {self.scope} -coll {self.collection} " \
                                    f"-ds {dataset_name} -c {self.capella_run} -cbs {self.create_bucket_struct}"

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

        except pkg_resources.DistributionNotFound:
            print("Couchbase SDK is not installed.")
        except Exception as e:
            print(f"Error: {e}")
