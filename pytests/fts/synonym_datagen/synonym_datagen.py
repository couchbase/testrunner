try:
    import docker
except ImportError:
    print("WARN: fail to import docker")

class SynonymDatagen:
    def __init__(self): 
        self.docker_client = docker.from_env()

    def start_server(self, container_name=None):
        try:
            docker_image = "sequoiatools/synonym_loader:t1"

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
                docker_output = self.docker_client.containers.run(docker_image,
                                                                  name=container_name,user="root",security_opt=["seccomp=unconfined"],ports={'5100/tcp': 5100},detach=True)
                print(f"Docfilter datagen result: {docker_output}")
                return docker_output
            except Exception as e:
                print(f"Exception while running docker container: {e}")
            

        except Exception as e:
            print(f"Error: {e}")