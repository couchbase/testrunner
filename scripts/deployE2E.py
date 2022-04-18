# Draft script to quickly deploy and teardown services.
# Note: This will be short lived and it is using subprocess
# so might lead to a bad exception handling
# New script with better exception handling using python SDK's will be up soon.
# TODO: Gauntlet and E2E are used interchangebly due to
#       E2E repo re-naming to gauntlet. To be addressed in new script
import logging
import os
import socket
import subprocess
import sys
import site
import time
from importlib import reload
reload(site)

script_dir = os.path.dirname(os.path.realpath(__file__))


class DeployE2EServices:
    def __init__(self, capella_hostname, capella_username, capella_password):
        self.log = logging.getLogger()

        self.capellaHostname = capella_hostname
        self.capellaUsername = capella_username
        self.capellaPassword = capella_password

        # self.e2eRepo = "git clone https://github.com/couchbaselabs/e2e-app.git"
        self.services_to_run = dict()
        self.services_to_run['profile'] = dict()
        self.services_to_run['booking'] = dict()
        self.services_to_run['inventory'] = dict()

        self.services_to_run['profile'] = dict()
        self.services_to_run['booking'] = dict()
        self.services_to_run['inventory'] = dict()

        self.services_to_run["booking"]["container_port_map"] = {8070: 8082}
        self.services_to_run["profile"]["container_port_map"] = {8090: 5000}
        self.services_to_run["inventory"]["container_port_map"] = {9010: 10000}

        self.bookingServiceName = "booking"

        #TODO remove command based execution from subprocess and handle using python SDK's
        self.bookingServiceDockerRunCommand = "docker run -d -t -i -p 8070:8082 -e CAPELLA_USERNAME={0} -e " \
                                              "CAPELLA_PASSWORD={1} -e DB_HOSTNAME={2} --name bookingService_container " \
                                              "couchbaseqe/gauntlet:booking"

        self.profileServiceName = "profile"
        self.profileServiceDockerRunCommand = "docker run -d -t -i -p 8090:5000 -e CAPELLA_USERNAME={0} -e " \
                                              "CAPELLA_PASSWORD={1} -e DB_HOSTNAME={2} -e BOOKING_HOST={3} -e " \
                                              "BOOKING_PORT=8070 --name profileService_container " \
                                              "couchbaseqe/gauntlet:profile "

        self.inventoryServiceName = "inventory"
        self.inventoryServiceDockerRunCommand = "docker run -d -t -i -p 9010:10000 -e CAPELLA_USERNAME={0} -e " \
                                                "CAPELLA_PASSWORD={1} -e DB_HOSTNAME={2}  --name " \
                                                "inventoryService_container couchbaseqe/gauntlet:inventory"

        self.dockerContainerListCommand = "docker container ls -a"
        self.dockerE2EContainersDeleteCommand = "docker rm -f bookingService_container profileService_container " \
                                            "inventoryService_container "

        self.dockerE2EImagesDeleteCommand = "docker rmi -f couchbaseqe/gauntlet:profile couchbaseqe/gauntlet:booking couchbaseqe/gauntlet:inventory"

        self.hostIP = self.get_host_ip()
        self.bookingHostname = self.hostIP
        print("Booking host will be deployed on : {0}".format(self.bookingHostname))

    def deploy(self):
        try:
            os.chdir('gauntlet')

            self.printCapellaDetails()
            #self.downloadE2ERepo()
            self.deleteExistingDockerContainersOnHost()
            self.deployService(self.bookingServiceName,
                               self.bookingServiceDockerRunCommand.format(self.capellaUsername, self.capellaPassword,
                                                                          self.capellaHostname))
            self.deployService(self.profileServiceName,
                               self.profileServiceDockerRunCommand.format(self.capellaUsername, self.capellaPassword,
                                                                          self.capellaHostname,
                                                                          self.bookingHostname))
            self.deployService(self.inventoryServiceName,
                               self.inventoryServiceDockerRunCommand.format(self.capellaUsername, self.capellaPassword,
                                                                            self.capellaHostname))
            time.sleep(10)
            subprocess.call(self.dockerContainerListCommand, shell=True, cwd=os.getcwd())

            time.sleep(10)
            subprocess.call("docker logs bookingService_container", shell=True, cwd=os.getcwd())

            time.sleep(10)
            subprocess.call("docker logs inventoryService_container", shell=True, cwd=os.getcwd())

            time.sleep(10)
            subprocess.call("docker logs profileService_container", shell=True, cwd=os.getcwd())

            os.chdir('..')
        except Exception as ex:
                print("Error while deploying. So quitting Gauntlet deployment!!. Exception details: {0}".format(ex))
                self.tearDown()

    def printCapellaDetails(self):
        print(
            "Mock Capella details are DB_HOSTNAME: " + self.capellaHostname + " DB_USERNAME: " + self.capellaUsername + " DB_PASSWORD: " + self.capellaPassword)

    # def downloadE2ERepo(self):
    #     try:
    #         os.system("rm -rf e2e-app")
    #
    #         print("Downloading e2e Repo:{0} ".format(self.e2eRepo))
    #       #  git.repo.base.Repo.clone_from(self.e2eRepo, "e2e-app")
    #         os.system(self.e2eRepo)
    #
    #         os.chdir('e2e-app')
    #     except Exception as ex:
    #         raise Exception("Error Downloading E2E Repo. Exiting!!. Exception Details:{0}".format(ex))

    def deployService(self, serviceName, dockerRunCommand):
        #self.createDockerImage(serviceName)
        try:
            print("Docker command to deploy {0}Service: {1}".format(serviceName, dockerRunCommand))
            subprocess.call(dockerRunCommand, shell=True, cwd=os.getcwd())
        except Exception as ex:
            print(
                "Docker run exception for service: {0} . Exiting!!. Exception Details:{1}".format(serviceName, ex))
            raise ex

    def createDockerImage(self, serviceName):
        try:
            createDockerImageCommand = "sh createDockerImages.sh {0} false".format(serviceName)
            print("Command for creating {0} docker Image:  {1}".format(serviceName,
                                                                       createDockerImageCommand))
            subprocess.call(createDockerImageCommand, shell=True, cwd=os.getcwd())
            print("Completed creating {0} docker Image. Now deploying it!!".format(serviceName))
        except Exception as  ex:
            print("Exception while creating docker image for service: {0}. Exiting!!. Exception "
                            "Details:{1} \n".format(serviceName,ex))
            raise ex

    def deleteExistingDockerContainersOnHost(self):
        try:
            subprocess.call(self.dockerContainerListCommand, shell=True, cwd=os.getcwd())
            subprocess.call(self.dockerE2EContainersDeleteCommand, shell=True, cwd=os.getcwd())
        except Exception as ex:
            print("Exception while removing existing dockercontainers.May be no Gauntlet containers existed on this host \n")


    def deleteExistingDockerImagesOnHost(self):
        try:
            subprocess.call(self.dockerE2EImagesDeleteCommand, shell=True, cwd=os.getcwd())
        except Exception as ex:
            print("Exception while removing existing docker Images.May be no Gauntlet Images existed on this host \n")

    def get_host_ip(self):
        ip = None

        # could be "linux", "linux2", "linux3", ...
        if sys.platform.startswith("linux"):
            ip = os.popen(
                'ifconfig eth0 | grep "inet " | xargs | cut -d" " -f 2')\
                .read().strip()
        elif sys.platform == "darwin":
            st = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                st.connect(('10.255.255.255', 1))
                ip = st.getsockname()[0]
            except Exception:
                ip = '127.0.0.1'
            finally:
                st.close()
        self.log.info("Host_ip = %s" % ip)
        return ip

    def get_endpoint(self, service_name):
        port = list(self.services_to_run[service_name][
            'container_port_map'].keys())[0]
        return "%s:%s" % (self.hostIP, port)

    def tearDown(self):
        self.deleteExistingDockerContainersOnHost()
        self.deleteExistingDockerImagesOnHost()


if __name__ == "__main__":
    DeployE2EServices(sys.argv[1], sys.argv[2], sys.argv[3]).deploy()
