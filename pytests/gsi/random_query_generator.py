import logging
import subprocess
import time

try:
    import docker
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
        # Check if Docker daemon is running and start it if not
        self._manage_docker_daemon()
        # Pulling the docker image
        try:
            logging.info(f"Pulling docker image {self.docker_image}")
            self.docker_client.images.pull('sequoiatools/random_query_generator')
        except docker.errors.APIError as e:
            logging.info(f"Exception while pulling docker image {self.docker_image}: {e}")

    def _manage_docker_daemon(self):
        """Check if Docker daemon is running and start it if not."""
        try:
            ps_output = subprocess.run(['ps', '-ef', '|', 'grep', 'docker'],
                                        shell=True, capture_output=True, text=True)
            docker_status = ps_output.stdout
            logging.info(f"Docker daemon status: {docker_status}")
            # Kill any running Docker service first
            if 'dockerd' in docker_status:
                logging.info("Killing existing Docker daemon...")
                try:
                    subprocess.run(['pkill', '-f', 'dockerd'], shell=True, capture_output=True)
                    time.sleep(5)  # Wait for the process to be killed
                    logging.info("Docker daemon killed successfully")
                except Exception as e:
                    logging.warning(f"Failed to kill Docker daemon: {e}")
            # Start Docker daemon
            logging.info("Starting Docker daemon...")
            docker_cmd = "nohup /usr/bin/dockerd -D -H 0.0.0.0:2375 -H unix:///var/run/docker.sock --data-root=/data > /dev/null 2>&1 &"
            subprocess.Popen(docker_cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(120)
            # Verify Docker daemon is running
            ps_output = subprocess.run(['ps', '-ef', '|', 'grep', 'docker'],
                                        shell=True, capture_output=True, text=True)
            docker_status = ps_output.stdout
            logging.info(f"Docker daemon status: {docker_status}")
            if 'dockerd' not in docker_status:
                raise Exception("Docker daemon is not running")
            else:
                logging.info(f"Docker daemon is running: {docker_status}")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to manage Docker daemon: {str(e)}") from e

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
