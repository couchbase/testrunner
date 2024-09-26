"""
Downloads dataset, extract vectors and dumps into couchbase.
"""

import glob
import os
import re
import shutil
import struct
import tarfile
from pathlib import Path
from urllib.request import urlretrieve

try:
    import h5py
    import numpy as np
    import wget
except ImportError:
    print("Install h5py, numpy and wget modules if running FTS vector tests")

########################################################################################
# Global Variables
########################################################################################

# Map of all available HDF5 formatted dataset.
HDF5_FORMATTED_DATASETS = {
    "fashion-mnist": {
        "dataset_name": "fashion-mnist",
        "dimension": 784,
        "train_size": 60000,
        "test_size": 10000,
        "neighbors": 100,
        "distance_type": "euclidean",
        "url": "http://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5",
        "local_file_path": "/tmp/vectordb_datasets/hdf5_format_datasets/fashion-mnist-784-euclidean.hdf5",
    },
    "gist": {
        "dataset_name": "gist",
        "dimension": 960,
        "train_size": 1000000,
        "test_size": 1000,
        "neighbors": 100,
        "distance_type": "euclidean",
        "url": "http://ann-benchmarks.com/gist-960-euclidean.hdf5",
        "local_file_path": "/tmp/vectordb_datasets/hdf5_format_datasets/gist-960-euclidean.hdf5",
    },
    "mnist": {
        "dataset_name": "mnist",
        "dimension": 784,
        "train_size": 60000,
        "test_size": 10000,
        "neighbors": 100,
        "distance_type": "euclidean",
        "url": "http://ann-benchmarks.com/mnist-784-euclidean.hdf5",
        "local_file_path": "/tmp/vectordb_datasets/hdf5_format_datasets/mnist-784-euclidean.hdf5",
    },
    "sift": {
        "dataset_name": "sift",
        "dimension": 128,
        "train_size": 1000000,
        "test_size": 10000,
        "neighbors": 100,
        "distance_type": "euclidean",
        "url": "http://ann-benchmarks.com/sift-128-euclidean.hdf5",
        "local_file_path": "/tmp/vectordb_datasets/hdf5_format_datasets/sift-128-euclidean.hdf5",
    },
}


########################################################################################

class VectorDataset:
    """
    Deals with SIFT/GIST data set at http://corpus-texmex.irisa.fr/
    """

    dataset_name = ""
    dataset_path = ""
    sift_base_url = ""
    train_dataset_filepath = ""
    query_dataset_filepath = ""
    learn_dataset_filepath = ""
    groundtruth_dataset_filepath = ""

    supported_sift_datasets = ["sift", "siftsmall", "gist"]
    # siftsmall : Dimension:128,LearnVectors:10,000, query Vectors:100
    # 25,000 fvecs
    local_base_dir = "/tmp/vectordb_datasets"

    # Following vars will have the dataset initialize for a give dataset name.
    # It means, train_data will have all the vectors initialized in-memory.
    train_vecs = None
    query_vecs = None
    learn_vecs = None
    neighbors_vecs = None
    distances_vecs = None

    def __init__(self, dataset_name):
        self.dataset_name = dataset_name
        self.sift_base_url = "ftp://ftp.irisa.fr/local/texmex/corpus/"
        print(f"Initialized dataset name:{self.dataset_name}, dataset base_url:{self.sift_base_url}")

    def print_details(self):
        """
        Only for debugging purpose to check all the paths and variable set.
        """
        print(f"Dataset Name: {self.dataset_name}")
        print(f"Dataset Path: {self.dataset_path}")
        print(f"Sift_base_url: {self.sift_base_url}")
        print(f"Train dataset filepath: {self.train_dataset_filepath}")
        print(f"Query dataset filepath: {self.query_dataset_filepath}")
        print(f"Learn dataset filepath: {self.learn_dataset_filepath}")
        print(f"Ground Truth dataset filepath: {self.groundtruth_dataset_filepath}")
        print(f"Supported sift datasets: {self.supported_sift_datasets}")

    def download_dataset(self):
        """
        Supported dataset names: siftsmall and sift. Once dataset is downloaded
        it sets self.dataset_path path

        Args:

        Returns: The local_base_dir where the dataset was downloaded.
        """
        if self.dataset_name not in self.supported_sift_datasets:
            print(
                f"Error: {self.dataset_name} not supported, "
                f"only {', '.join(self.supported_sift_datasets)} supported"
            )
        else:
            tar_file_name = self.dataset_name + ".tar.gz"  # Ex: sift.tar.gz
            local_tar_file_path = os.path.join(self.local_base_dir,
                                               tar_file_name)  # Ex: /tmp/vectordb_datasets/sift.tar.gz
            sift_tar_file_url = os.path.join(self.sift_base_url, tar_file_name)
            if not os.path.exists(self.local_base_dir):
                Path(self.local_base_dir).mkdir(parents=True, exist_ok=True)
            dataset_dir_path = os.path.join(self.local_base_dir, self.dataset_name)  # Ex: /tmp/vectordb_datasets/sift
            if os.path.exists(dataset_dir_path):  # Remove existing dir Ex: /tmp/vectordb_datasets/sift
                shutil.rmtree(dataset_dir_path)

            if not os.path.exists(local_tar_file_path):
                try:
                    # Changing permissions to allow wget to write files
                    self.change_permissions(self.local_base_dir)
                    print(f"Downloading {sift_tar_file_url} to {self.local_base_dir}")
                    wget.download(sift_tar_file_url, out=self.local_base_dir)
                    # Changing permissions to all files after downloading.
                    self.change_permissions(self.local_base_dir)
                except Exception as e:
                    print(
                        f"Unable to get file from  {sift_tar_file_url} to local dir {self.local_base_dir}"
                        f"exception {e}"
                    )
            else:
                print(f"Skipping download as tar file as it exists at the path {local_tar_file_path} already")
            with tarfile.open(local_tar_file_path, "r:gz") as tar:
                print(f"Untar downloaded tar.gz to {self.local_base_dir}")
                tar.extractall(self.local_base_dir)
                if os.path.exists(dataset_dir_path):
                    print(f"dataset created and available at {dataset_dir_path}")
                else:
                    print(f"Error: Unable to extract the tar file, it does not exist at {dataset_dir_path}")
                    return self.local_base_dir
                self.change_permissions(dataset_dir_path)
            return self.local_base_dir

    def set_dataset_paths(self):
        """
        Downloads all necessary dataset files and initiate all the paths for
        further use.

        Args:

        Returns:
                True if everything is good False to indicate something gone
                wrong
        """
        print("Setting necessary paths for the dataset")
        self.dataset_path = self.download_dataset()
        if not os.path.exists(self.dataset_path):
            print("Dataset Dir {self.dataset_path} does not exist")
            return False

        self.train_dataset_filepath = os.path.join(
            self.dataset_path, self.dataset_name, self.dataset_name + "_base.fvecs"
        )
        if not os.path.exists(self.train_dataset_filepath):
            print("Train dataset filepath {self.train_dataset_filepath} does not exist")
            return False

        self.query_dataset_filepath = os.path.join(
            self.dataset_path, self.dataset_name, self.dataset_name + "_query.fvecs"
        )
        if not os.path.exists(self.query_dataset_filepath):
            print("Query dataset filepath {self.query_dataset_filepath} does not exist")
            return False

        self.learn_dataset_filepath = os.path.join(
            self.dataset_path, self.dataset_name, self.dataset_name + "_learn.fvecs"
        )
        if not os.path.exists(self.learn_dataset_filepath):
            print("Learn dataset filepath {self.learn_dataset_filepath} does not exist")
            return False

        self.groundtruth_dataset_filepath = os.path.join(
            self.dataset_path,
            self.dataset_name,
            self.dataset_name + "_groundtruth.ivecs",
        )
        if not os.path.exists(self.groundtruth_dataset_filepath):
            print(
                "Groundtruth dataset filepath {self.groundtruth_dataset_filepath} does not exist"
            )
            return False

        return True

    def validate_dataset(self):
        """
        Check for required files to exist in the dataset dir. This to be called
        only after set_dataset_paths. Dataset dir will have fillowing files
        sift*_query.fvecs sift*_learn.fvecs sift*_groundtruth.ivecs
        sift*_base.fvecs

        Args:

        Returns:
        """
        print(f"Validating dataset at dir {self.dataset_path}")
        file_patterns_to_check = [
            "sift.*_query.fvecs",
            "sift.*_learn.fvecs",
            "sift.*_groundtruth.ivecs",
            "sift.*_base.fvecs",
        ]
        missing_patterns = []
        all_available_files = glob.glob(
            os.path.join(self.dataset_path, self.dataset_name, "*vecs")
        )
        all_available_files = [os.path.basename(file) for file in all_available_files]
        print(f"All file names in dir {all_available_files}")
        # Get all files in dataset dir and check for each file existence in the
        # list.
        for each_pattern in file_patterns_to_check:
            regex_pattern = re.compile(each_pattern)
            file_found = False
            for file_name in all_available_files:
                if regex_pattern.match(file_name):
                    print(
                        f" Success Matched file {file_name} with pattern {each_pattern}"
                    )
                    file_found = True
                    break
            if not file_found:
                missing_patterns.append(each_pattern)

        if missing_patterns:
            print(
                f"Files missing with pattern {missing_patterns} in dataset dir:{self.dataset_path}"
            )
            return False

        print(
            f"Successfull Validation of dataset with name {self.dataset_name}"
            f" at {os.path.join(self.dataset_path, self.dataset_name)}"
        )
        return True

    def extract_vectors_from_file(self, use_hdf5_datasets, type_of_vec="train"):
        """
        Extracts the vectors either from tar.gz files or from hdf5 files based
        use_hdf5_datasets Initialize the necessary vector datastructures as
        follows
            - train_vecs
            - query_vecs
            - neighbors_vecs
            - distances_vecs - Gets initialized only when "use_hdf5_datasets"
              used.
        Args:
            param1 (str) : use_hdf5_datasets True or False

        Returns:
            numpy.ndarray : Vectors read from the file
        """

        self.set_dataset_paths()
        if use_hdf5_datasets:
            print(f"Extracting needed vectors of type: {type_of_vec} from hdf5 files for dataset {self.dataset_name}")
            ds_error = self.extract_vectors_using_hdf5_files(self.dataset_name, type_of_vec)
            if ds_error != "":
                print(f"Error: Could not extract vectors from hdf5 file for dataset: {self.dataset_name}")
            else:
                print(f"{type_of_vec} vectors are initialized")
        else:
            print(f"Extracting needed vectors of type: {type_of_vec} from tar.gz files for dataset {self.dataset_name}")
            filepath = ""
            out_vector = None
            vector_types = ["train", "query", "learn", "groundtruth"]
            if type_of_vec == "train":
                filepath = self.train_dataset_filepath
            elif type_of_vec == "query":
                filepath = self.query_dataset_filepath
            elif type_of_vec == "learn":
                filepath = self.learn_dataset_filepath
            elif type_of_vec == "groundtruth":
                filepath = self.groundtruth_dataset_filepath
            try:
                if type_of_vec != "groundtruth":
                    with open(
                            filepath, "rb"
                    ) as file:  # Open the file in binary mode to read the data from the fvec/ivec/bvec files.
                        (dimension,) = struct.unpack(
                            "i", file.read(4)
                        )
                        print(f"Dimension of the vector type {type_of_vec} is :{dimension}")
                        # First 4bytes denote dimension of the vector. Next
                        # bytes of size "dimension" number of 4bytes will
                        # give us the full vector.
                        num_vectors = os.path.getsize(filepath) // (4 + 4 * dimension)
                        print(
                            f"Total number of vectors in {type_of_vec} dataset: {num_vectors}"
                        )
                        file.seek(0)  # Let us move the cursor back to first position to start with first vector.
                        out_vector = np.zeros(
                            (num_vectors, dimension)
                        )  # Initializing the two dimension matrix with rows x columns = num_vectors x dimension
                        for i in range(num_vectors):
                            file.read(
                                4
                            )  # To move cursor by 4 bytes to ignore dimension of the vector.
                            # Read float values of size 4bytes of length "dimension"
                            out_vector[i] = struct.unpack(
                                "f" * dimension, file.read(dimension * 4)
                            )
                    if type_of_vec == "train":
                        self.train_vecs = out_vector
                    if type_of_vec == "query":
                        self.query_vecs = out_vector
                    if type_of_vec == "learn":
                        self.learn_vecs = out_vector
                    print(f"{type_of_vec} vectors are initialized")
                else:
                    with open(filepath, 'rb') as fid:
                        # Read the vector size
                        d = np.fromfile(fid, dtype=np.int32, count=1)[0]
                        vecsizeof = 1 * 4 + d * 4

                        # Get the number of vectors
                        fid.seek(0, 2)
                        a = 1
                        bmax = fid.tell() // vecsizeof
                        b = bmax

                        assert a >= 1
                        if b > bmax:
                            b = bmax

                        if b == 0 or b < a:
                            return np.array([])

                        # Compute the number of vectors that are really read and go to starting positions
                        n = b - a + 1
                        fid.seek((a - 1) * vecsizeof, 0)

                        # Read n vectors
                        v = np.fromfile(fid, dtype=np.int32, count=(d + 1) * n).astype(np.float64)
                        v = v.reshape((d + 1, n), order='F')

                        # Check if the first column (dimension of the vectors) is correct
                        assert np.sum(v[0, 1:] == v[0, 0]) == n - 1
                        v = v[1:, :]
                        transposed_array = np.transpose(v)
                    self.neighbors_vecs = transposed_array
                    print(f"{type_of_vec} vectors are initialized")
            except FileNotFoundError:
                print(f"Error: File '{filepath}' not found.")
            except Exception as e:
                print(f"Error: An error occurred while extracting train vectors: {str(e)}")

    def change_permissions(self, directory_path):
        """
        Args:
            param1 (str) : directory_path
        """
        try:
            # Set read and write permissions for the owner, group, and others
            os.chmod(
                directory_path, 0o777
            )  # 0o777 corresponds to read, write, and execute for everyone

            print(f"Permissions changed successfully for {directory_path}")
        except Exception as e:
            print(f"Error: Error occurred while changing permissions: {str(e)}")

    def extract_vectors_using_hdf5_files(self, dataset_name, type_of_vec="train"):
        """_summary_

        Args:
            dataset_name (str): dataset name
            type_of_vec (str, optional): train or query or neightbors or distances . Defaults to "train"

        Returns:
             str: Empty string on successfull extraction of data from hdf5 files.
        """
        supported_datasets = HDF5_FORMATTED_DATASETS.keys()
        if dataset_name in supported_datasets:
            # URL of the HDF5 file url =
            # "http://ann-benchmarks.com/sift-128-euclidean.hdf5"
            url = HDF5_FORMATTED_DATASETS[dataset_name]["url"]
            local_file_path = HDF5_FORMATTED_DATASETS[dataset_name]["local_file_path"]

            # download the dataset if local does not exist.
            if not os.path.exists(local_file_path):
                # Create dir path if does not exist.
                directory_path = os.path.dirname(local_file_path)
                os.makedirs(directory_path, exist_ok=True)
                # Download the HDF5 file
                print(f"Downloading dataset using url {url} to {local_file_path}")
                urlretrieve(url, local_file_path)

            # Open the HDF5 file.
            with h5py.File(local_file_path, "r") as hdf_file:
                # Dataset has following types of the data.
                # train, query, neighbors and distances
                [print(f"Hdf file {local_file_path} has following data : {keys}") for keys in hdf_file]

                if type_of_vec == "train":
                    # Dealing with train dataset
                    train_dataset = hdf_file["train"]
                    # Convert dataset to NumPy array
                    self.train_vecs = np.array(train_dataset)
                    # Print the shape of the array
                    print("Shape of the train dataset:", self.train_vecs.shape)
                    print("First 5 elements train_data:", self.train_vecs[:5])

                if type_of_vec == "query":
                    # Dealing with test dataset
                    test_dataset = hdf_file["test"]
                    # Convert dataset to NumPy array
                    self.query_vecs = np.array(test_dataset)
                    # Print the shape of the array
                    print("Shape of the test dataset:", self.query_vecs.shape)
                    print("First 5 elements test_data:", self.query_vecs[:5])

                if type_of_vec == "neighbors":
                    # Dealing with test neighbours
                    neighbors_dataset = hdf_file["neighbors"]
                    # Convert dataset to NumPy array
                    self.neighbors_vecs = np.array(neighbors_dataset)
                    # Print the shape of the array
                    print("Shape of the neighbors dataset:", self.neighbors_vecs.shape)
                    print("First 5 elements neighbors_data:", self.neighbors_vecs[:5])

                if type_of_vec == "distances":
                    # Dealing with test distances
                    distances_dataset = hdf_file["distances"]
                    # Convert dataset to NumPy array
                    self.distances_vecs = np.array(distances_dataset)
                    # Print the shape of the array
                    print("Shape of the distances dataset:", self.distances_vecs.shape)
                    print("First 5 elements distances_data:", self.distances_vecs[:5])
                return ""
        else:
            print(f"Error: dataset name {dataset_name} is not supported")
            return "Error: dataset name" + dataset_name + "is not supported"
