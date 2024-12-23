import subprocess
import locale
import logger
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

class JavaSdkSetup(object):
    def __init__(self):
        self.run()
        self.log = logger.Logger.get_logger()

    def run(self):
        cmd = "mvn -f java_sdk_client/collections/pom.xml clean install  > /dev/null && echo 0 || echo 1;"
        try:
            self._execute_on_slave(cmd, 30)
        except Exception as err:
            self.log.warning("WARNING: Exception occurred while compiling java_sdk_client..continuing")
            self.log.error(err)

    def _execute_on_slave(self, command, timeout):
        return subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).wait(timeout)