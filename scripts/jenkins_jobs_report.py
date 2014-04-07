import urllib2
import json
import csv
import getopt
import sys
from hashlib import md5

sys.path.extend(('.', 'lib'))
from lib.memcached.helper.data_helper import MemcachedClientHelper

API = "/api/json"
JOBS_FILE = "scripts/jobs.ini"
JENKINS_URL = "http://qa.hq.northscale.net/job/"
CB_BUCKET_NAME = "jenkins"

class Job(object):
    def __init__(self):
        self.actions = []
        self.description = ''
        self.displayName = ''
        self.url = ''
        self.buildable = True
        self.build_histories = []
        self.name = ''
        self.color = ''
        self.firstBuild = ''
        self.healthReport = []
        self.inQueue = False
        self.keepDependencies = False
        self.lastBuild = {}
        self.lastCompletedBuild = {}
        self.lastFailedBuild = {}
        self.lastStableBuild = {}
        self.lastSuccessfulBuild = {}
        self.lastUnstableBuild = {}
        self.lastUnsuccessfulBuild = {}
        self.nextBuildNumber = {}
        self.property = []
        self.queueItem = None
        self.concurrentBuild = False
        self.downstreamProjects = []
        self.scm = {}
        self.upstreamProjects = []

class Build(object):
    def __init__(self):
        self.actions = []
        self.failCount = ''
        self.totalCount = ''
        self.skipCount = ''
        self.urlName = 'testReport'
        self.result = ''
        self.version_number = ''
        self.priority = 'N/A'
        self.timestamp = ''
        self.testReport = TestReport()

class TestReport(object):
     def __init__(self):
         self.duration = ''
         self.failCount = None
         self.passCount = None
         self.skipCount = None
         self.suites = []

def get_jobs_stats(job_url=None, onlyLastBuild=False):
    if not job_url:
        f = open(JOBS_FILE, 'r')
        lines = f.readlines()
    else:
        lines = [job_url]
    jobs = []
    for k in lines:
        url = k.strip() + API
        print "read base info from %s" % url
        content = urllib2.urlopen(url).read()
        json_parsed = json.loads(content)
        job = Job()
        job.actions = json_parsed["actions"]
        job.displayName = json_parsed["displayName"]
        job.url = json_parsed["url"]
        job.buildable = json_parsed["buildable"]
        job.build_histories = json_parsed["builds"]
        job.name = json_parsed["name"]
        job.color = json_parsed["color"]
        job.firstBuild = json_parsed["firstBuild"]
        job.healthReport = json_parsed["healthReport"]
        job.inQueue = json_parsed["inQueue"]
        job.keepDependencies = json_parsed["keepDependencies"]
        job.lastBuild = json_parsed["lastBuild"]
        job.lastCompletedBuild = json_parsed["lastCompletedBuild"]
        job.lastFailedBuild = json_parsed["lastFailedBuild"]
        job.lastStableBuild = json_parsed["lastStableBuild"]
        job.lastSuccessfulBuild = json_parsed["lastSuccessfulBuild"]
        job.lastUnstableBuild = json_parsed["lastUnstableBuild"]
        job.lastUnsuccessfulBuild = json_parsed["lastUnsuccessfulBuild"]
        job.nextBuildNumber = json_parsed["nextBuildNumber"]
        job.property = json_parsed["property"]
        job.queueItem = json_parsed["queueItem"]
        job.concurrentBuild = json_parsed["concurrentBuild"]
        job.downstreamProjects = json_parsed["downstreamProjects"]
        job.scm = json_parsed["scm"]
        job.upstreamProjects = json_parsed["upstreamProjects"]
        jobs.append(job)

    for job in jobs:
        last = (3,1)[onlyLastBuild]
        for build_history in job.build_histories:
                print build_history
            # get last 5 results only
                if last <= 0:
                    break
                last -= 1
                url = build_history["url"] + API
                content = urllib2.urlopen(url).read()
                # content = content.replace('None', '"None"').replace(': True', ': "True"').replace(':True', ':"True"').replace(': False', ': "False"').replace(':False', ':"False"')
                # print content
                json_parsed = json.loads(content)
                build = Build()
                build_history["result"] = build
                build.actions = json_parsed["actions"]
                build.result = json_parsed["result"]
                build.timestamp = json_parsed["timestamp"]
                for action in build.actions:
                    if "failCount" in action:
                        build.failCount = action["failCount"]
                        build.totalCount = action["totalCount"]
                        build.skipCount = action["skipCount"]
                        build.urlName = action["urlName"]
                        report_url = build_history["url"] + build.urlName + API
                        report_content = urllib2.urlopen(report_url).read()
                        # report_content = report_content.replace('None', '"None"').replace(': True', ': "True"').replace(': False', ': "False"')  # .replace(':False', ':"False"')
                        # print report_content
                        report_json_parsed = json.loads(report_content)
                        build.testReport.duration = report_json_parsed["duration"]
                        build.testReport.failCount = report_json_parsed["failCount"]
                        build.testReport.passCount = report_json_parsed["passCount"]
                        build.testReport.skipCount = report_json_parsed["skipCount"]
                        build.testReport.suites = report_json_parsed["suites"]
                    elif "parameters" in action:
                        for parameter in action["parameters"]:
                            if parameter["name"] == "version_number":
                                build.version_number = parameter["value"].replace("-rel", "")
                            if parameter["name"] == "priority":
                                build.priority = parameter["value"]
    return jobs

def main():
    jobs = get_jobs_stats()

    results = {}
    for job in jobs:
        results[job.name] = {}
        for build_history in job.build_histories:
            if "result" in  build_history:
                version = build_history["result"].version_number
                if version in results[job.name]:
                    if results[job.name][version]["number"] > build_history["number"]:
                        # take only the latest one
                        continue
                results[job.name][version] = {}
                results[job.name][version]["result"] = build_history["result"].result
                results[job.name][version]["failCount"] = build_history["result"].failCount
                results[job.name][version]["totalCount"] = build_history["result"].totalCount
                results[job.name][version]["skipCount"] = build_history["result"].skipCount
                results[job.name][version]["number"] = build_history["number"]


    versions = []
    for job in results:
        for version in results[job]:
            versions.append(version)

    versions = list(set(versions))
    versions.sort()

    f = csv.writer(open("test.csv", "wb+"))


    # Write CSV Header, If you dont need that, remove this line
    f.writerow(["JOB NAME"] + versions)

    job_names = results.keys()
    job_names.sort()

    for x in job_names:
        print "handle results from %s" % x
        if x is None:
            continue
        row = [x]
        for version in versions:
            if version in results[x]:
                print version
    #            if results[x][version]["result"]:
    #                row.append("Result:" + results[x][version]["result"] + " FailCount:" + str(results[x][version]["failCount"]) + " TotalCount:" + str(results[x][version]["totalCount"]) + " SkipCount:" + str(results[x][version]["skipCount"]) + " Number:" + str(results[x][version]["number"]))
    #            else:
                row.append("Result:" + str(results[x][version]["result"]) + " FailCount:" + str(results[x][version]["failCount"]) + " TotalCount:" + str(results[x][version]["totalCount"]) + " SkipCount:" + str(results[x][version]["skipCount"]) + " Number:" + str(results[x][version]["number"]))
            else:
                row.append("")
        f.writerow(row)

    print "SEE RESULTS IN results.csv"

def build_json_result(jobs):
    jsons = []
    for job in jobs:
        os = ((('N/A', 'UBUNTU')[job.name.lower().find('ubuntu') != -1], 'CENTOS')[job.name.lower().find('cent') != -1],'WINDOWS')[job.name.lower().find('win') != -1]
        rq = {'name' : job.name, 'os' : os}
        if len(job.build_histories) > 0:
            rq.update({'priority': job.build_histories[0]['result'].priority,
                        'build': job.build_histories[0]['result'].version_number,
                        'timestamp': job.build_histories[0]['result'].timestamp,
                        'totalCount': job.build_histories[0]['result'].totalCount,
                        'failCount': job.build_histories[0]['result'].failCount,
                        'result': job.build_histories[0]['result'].result,
                        'build_id' : job.lastBuild['number']})
        key = md5(rq["name"] + str(rq["build_id"])).hexdigest()
        jsons.append((key, json.dumps(rq)))
        print rq
    return jsons

def usage(err=None):
    print """\
Syntax: install.py [options]

Options:
 -o json\file       Output will be send to CB if json is defined. Output will be written to csv file if file defined
 -s <server>        Server ip with CB like localhost
 -p <port>          Server port with CB. Default is 8091
 --user <user>          User for server with CB
 --pass <password>      Password for server with CB
 --jenkins <jenkins url>   Jenkins url, default is http://qa.hq.northscale.net/job/
 --name <job name>      job name, if None all jobs statistics will be collected

Examples:
 jenkins_jobs_report.py -o json -s localhost -p 8091 --user Administrator --pass password --name "centos_x64--00_04--warmup-P0"
"""
    sys.exit(err)

def _parse_variables():
    varbls = {"-o" : "file", "-s" : None, "-p" : '8091',
            "--user" : 'Administrator', "--pass": "password",
            "--jenkins" : JENKINS_URL, "--name" : None,
            "server_info" : None}
    (opts, _) = getopt.getopt(sys.argv[1:], 'o:s:p:', ['user=','password=','jenkins=','name='])
    for o, a in opts:
        if o == "-h":
            usage()
        elif o == "-o" or o == "-s":
            varbls[o] = a.lower()
        else:
            varbls[o] = a
    if varbls["-s"]:
        varbls["server_info"] = {"username" : varbls["--user"], "password" : varbls["--pass"],
                                 "ip" : varbls["-s"], "port" : varbls["-p"]}
    else:
        if varbls["-o"] == "json":
            usage()
    return varbls

def send_json(server_info, job=None):
    if job:
        jobs = get_jobs_stats(onlyLastBuild=True, job_url=JENKINS_URL + job)
    else:
        jobs = get_jobs_stats(onlyLastBuild=True)
    jsons = build_json_result(jobs)
    client = MemcachedClientHelper.direct_client(server_info, CB_BUCKET_NAME)
    for key, rq in jsons:
        try:
            client.set(key, 0, 0, rq)
        except Exception, ex:
            sys.exit(str(ex))

if __name__ == "__main__":
    varbls = _parse_variables()
    if varbls["-o"] == "json":
        send_json(varbls["server_info"], varbls["--name"])
    else:
        main()
