import sys

from jenkins import Jenkins, JenkinsException

sys.path = ["..", "../py_constants"] + sys.path
from cb_constants.jenkins import JenkinsConstants


class JenkinsJobStatus:
    def __init__(self, jenkins_url, job_name):
        self.job_name = job_name
        self.jenkins = Jenkins(jenkins_url)

    def job_status(self, job_num):
        status = "NA"
        try:
            info = self.jenkins.get_build_info(self.job_name, job_num)
            if info["building"]:
                status = "RUNNING"
            elif 'result' in info:
                status = info["result"]
            desc = f"Desc: {info['description']}"
        except JenkinsException as e:
            if f"number[{job_num}] does not exist" in str(e):
                desc = f"Job: {job_num}"
                status = "NOT_FOUND"
            else:
                raise e
        print(desc)
        print(f"Status: {status}")
        print("")


if __name__ == "__main__":
    j = JenkinsJobStatus(JenkinsConstants.OLD_JENKINS_URL,
                         JenkinsConstants.DEFAULT_DISPATCHER_JOB)
    for j_num in sys.argv[1:]:
        j.job_status(j_num)
