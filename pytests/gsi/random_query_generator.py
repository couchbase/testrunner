try:
    import docker
    import logging
except ImportError:
    print("WARN: fail to import docker")

from basetestcase import BaseTestCase

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class RandomQueryGenerator(BaseTestCase):
    def __init__(self, username, password, debug=False):

        self.username = username
        self.password = password
        self.docker_client = docker.from_env()
        self.docker_image = "sequoiatools/random_query_generator"
        self.debug = debug
        # Pulling the docker image
        try:
            logging.info(f"Pulling docker image {self.docker_image}")
            self.docker_client.images.pull('sequoiatools/random_query_generator')
        except docker.errors.APIError as e:
            logging.info(f"Exception will pulling docker image {self.docker_image}: {e}")

    def random_query_generator(self, create_query, node, container_name="random_query_generator", dataset="hotel", num_queries=10):

        docker_run_params = f"-nodeAddress {node} -username {self.username} -password {self.password} -create_query \"{create_query}\" -dataset {dataset} -num_queries {num_queries}"
        logging.info(f"docker run command is : {docker_run_params}")

        #Runinng the docker run command
        try:
            logging.info(f"Running docker container {self.docker_image}")
            docker_output = self.docker_client.containers.run(self.docker_image, docker_run_params)
            try:
                split_output = str(docker_output).split("\\n")
                logging.info(split_output[-5])
                logging.info(split_output[-4])
                logging.info(split_output[-3])
                logging.info(split_output[-2])
                if self.debug:
                    for i in split_output[:-5]:
                        logging.info(i)
            except Exception as e:
                print(f"Exception while getting docker output: {e}")
                raise Exception(e)



        except Exception as e:
            print(f"Exception while running docker container: {e}")
            raise Exception(e)


