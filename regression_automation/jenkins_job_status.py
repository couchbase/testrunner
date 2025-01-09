import sys

from jenkins import Jenkins

sys.path = ["..", "../py_constants"] + sys.path
from cb_constants.jenkins import JenkinsConstants


class JenkinsJobStatus:
    def __init__(self, jenkins_url, job_name):
        self.job_name = job_name
        self.jenkins = Jenkins(jenkins_url)

    def job_status(self, job_num):
        info = self.jenkins.get_build_info(self.job_name, job_num)
        status = "NA"
        if info["building"]:
            status = "RUNNING"
        elif 'result' in info:
            status = info["result"]
        print(f"Desc: {info['description']}")
        print(f"Status: {status}")
        print("")


if __name__ == "__main__":
    j = JenkinsJobStatus(JenkinsConstants.OLD_JENKINS_URL,
                         JenkinsConstants.DEFAULT_DISPATCHER_JOB)
    for j_num in sys.argv[1:]:
        j.job_status(j_num)
