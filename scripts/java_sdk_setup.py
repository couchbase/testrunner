import subprocess


class JavaSdkSetup(object):
    def __init__(self):
        self.run()

    def run(self):
        cmds = ["chmod 777 java_sdk_client/collections/configure.sh  > /dev/null && echo 0 || echo 1;",
                "mvn -f java_sdk_client/collections/pom.xml clean install  > /dev/null && echo 0 || echo 1;"
                "docker build -t jsc java_sdk_client/collections  > /dev/null && echo 0 || echo 1;"
                ]
        for cmd in cmds:
            num_retries = 2
            while num_retries:
                try:
                    print(cmd + "\n")
                    if self._execute_on_slave(cmd, 30):
                        num_retries -= 1
                    else:
                        break
                except Exception as e:
                    raise("Unable to perform {0} due to {1}".format(cmd, e))
            else:
                raise("Unable to perform {0} after 3 retries".format(cmd))

    def _execute_on_slave(self, command, timeout):
        return subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).wait(timeout)