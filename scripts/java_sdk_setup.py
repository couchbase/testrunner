import subprocess
import docker


class JavaSdkSetup(object):
    def __init__(self):
        self.base_dir = "java_sdk_client/collections/"
        self.image_tag = "jsc:latest"
        self.client = docker.APIClient()
        self.build_docker_image()

    def _run_cmds(self):
        cmds = ["mvn -f " + self.base_dir + "pom.xml clean install  > /dev/null && echo 0 || echo 1;",
                "chmod 777 " + self.base_dir + "configure.sh  > /dev/null && echo 0 || echo 1;"]
        for cmd in cmds:
            self.__execute_on_slave(cmd, 30)

    def __execute_on_slave(self, command, timeout):
        print(command)
        return subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).wait(timeout)

    def build_docker_image(self):
        # Only remove dangling images
        self._cleanup(True)
        num_retries = 2
        while num_retries:
            if self._inspect_docker_image():
                print("Docker image built")
                break
            else:
                num_retries -= 1
                try:
                    # Remove all images
                    self._cleanup(False)
                    self._run_cmds()
                    response = [line for line in self.client.build(path=self.base_dir, rm=True, tag="jsc")]
                except Exception as e:
                    raise("Unable to build docker image ".format(e))
        else:
            raise("Unable to build docker image after 3 retries")

    def _inspect_docker_image(self):
        if len(self.client.images(name=self.image_tag)) == 1:
            return True
        else:
            return False

    def _cleanup(self, dangling=True):
        self.client.prune_images(filters={"dangling": dangling})
        self.client.prune_containers()
