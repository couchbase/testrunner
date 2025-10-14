import subprocess
import logger  # assuming you have your own logger module
import os

class JavaSdkSetup(object):
    def __init__(self):
        self.log = logger.Logger.get_logger()
        self.run()

    def run(self):
        cmd = "mvn -f java_sdk_client/collections/pom.xml clean install -Dproject.build.sourceEncoding=UTF-8"
        try:
            output = self._execute_on_slave(cmd, 30)
            self.log.info("Maven build completed successfully.")
            self.log.debug(output)
        except subprocess.TimeoutExpired:
            self.log.error("Maven build timed out after 30s.")
        except subprocess.CalledProcessError as err:
            self.log.warning("WARNING: Exception occurred while compiling java_sdk_client..continuing")
            self.log.error(err.output)
        except Exception as err:
            self.log.error(f"Unexpected error: {err}")

    def _execute_on_slave(self, command, timeout):
        env = os.environ.copy()
        env["LC_ALL"] = "C.UTF-8"
        env["LANG"] = "C.UTF-8"
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            timeout=timeout,
            text=True,
            env=env,
            check=True  # raises CalledProcessError if return code != 0
        )
        return result.stdout