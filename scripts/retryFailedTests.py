# needs -> sudo pip install jenkinsapi , libcouchbase 2.9.3 -> 2.10.2
from optparse import OptionParser
from couchbase.n1ql import N1QLQuery
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
from jenkinsapi.jenkins import Jenkins

repo_ip="172.23.98.63"
repo_username="retry"
repo_password="password"
job_map={"executor":"test_suite_executor","jython":"test_suite_executor-jython",
         "docker":"test_suite_executor-docker"}

def main():

    usage = '%prog -v version -o os -p fail% -s suite -c component -S serverPoolId -b branch'
    parser = OptionParser(usage)
    parser.add_option('-v', '--version', dest='version')
    parser.add_option('-o', '--os', dest='os', default="CENTOS", choices=['CENTOS', 'WIN', 'UBUNTU'], help='options: CENTOS,WIN,UBUNTU or default: %default')
    parser.add_option('-t', '--type', dest='type', default='executor', choices=['executor', 'jython', 'docker'], help='options: executor,jython,docker or default: %default')
    parser.add_option('-f', '--fail', type='int', dest='fail_percent', default=100, help='default: %default')
    parser.add_option('-s', '--suite', dest='suite', default="12hour", help='default: %default')
    parser.add_option('-c', '--component', dest='component', default=None, help='default: %default')
    parser.add_option('-S', '--serverPoolId', dest='serverPoolId', default='regression', help='default: %default')
    parser.add_option('-b', '--branch', dest='branch', default='master', help='default: %default')
    parser.add_option('--dry', action="store_true", dest="dry", default=False, help='dry run the jobs: %default')

    options, args = parser.parse_args()

    if options.version is None:
        print("pass build version")
        print(parser.print_help())
        exit(1)
    rerun=getFailedJobs(options)
    #print rerun
    for k, v in list(rerun.items()):
        sub=(",").join(v)
        param={}
        if options.os=="WIN":
            param["OS"]="windows"
        else:
            param["OS"]=str.lower(options.os)
        param["version_number"]=options.version
        param["suite"]=options.suite
        param["component"]=str.lower(str(k))
        param["subcomponent"]=str(sub)
        param["serverPoolId"]=options.serverPoolId
        param["branch"]=options.branch
        param["token"]="extended_sanity"
        print(("running job for subcomponent {0} : {1}".format(param["component"], param)))
        if not options.dry:
            print(("triggered jenkins job with: {}".format(param)))
            trigger_job(param)



def getFailedJobs(options):
    cluster = Cluster('couchbase://172.23.98.63')
    authenticator = PasswordAuthenticator(repo_username, repo_password)
    cluster.authenticate(authenticator)
    bkt=cluster.open_bucket('server')
    url="http://qa.sc.couchbase.com/job/{}/".format(job_map[options.type])
    #bkt = Bucket('couchbase://172.23.109.245/server',username=repo_username,password=repo_password)
    query='SELECT component,subComponent,name,build_id,url from server ' \
          'WHERE os=$os and `build`=$build and failCount >=(totalCount*$fail_percent)/100 ' \
          'and url=$url order by component'
    q = N1QLQuery(query, os=options.os, build=options.version, fail_percent=options.fail_percent, url=url)
    if options.component is not None:
        query='SELECT component,subComponent,name,build_id,url from server ' \
          'WHERE os=$os and `build`=$build and failCount >=(totalCount*$fail_percent)/100 ' \
          'and url=$url and component=$component' \
              ' order by component'
        q = N1QLQuery(query, os=options.os, build=options.version, fail_percent=options.fail_percent, component=str.upper(options.component),
                      url=url)
    retryJob={}
    print(("Running query:{}".format(q)))
    count=0
    for row in bkt.n1ql_query(q):
        count+=1
        print(("failed job: {}".format(row)))
        component=row["component"]
        #print component
        if component not in retryJob:
            retryJob[component]=[]
        if "subComponent" in row:
            subcomponent=row["subComponent"]
            #print "subcomponent:", subcomponent
            sub=retryJob[component]
            sub.append(subcomponent)
            #print retryJob
        else:
            subcomponent=getSubcomponent(row["build_id"], options)
            #print "subcomponent from jenkins:",subcomponent
            sub = retryJob[component]
            sub.append(subcomponent)
    print(("total failed jobs:{}".format(count)))
    return retryJob

def trigger_job(param):
    j = Jenkins("http://qa.sc.couchbase.com/")
    job = j.build_job("test_suite_dispatcher", param)
    print(("job started: {}".format(job)))

def getSubcomponent(build_id, options):
    job=job_map[options.type]
    j = Jenkins("http://qa.sc.couchbase.com/")
    job = j.get_job(job)
    build = job.get_build(build_id)
    parameters = build.get_actions()['parameters']

    subcomponent=None
    for p in parameters:
        if p["name"] == "subcomponent":
            subcomponent=p["value"]
            return subcomponent
    return subcomponent

if __name__ == "__main__":
    main()
    #getSubcomponent()
    # param={'version_number': '6.0.0-1680', 'serverPoolId': 'regression', 'component': 'eventing', 'token': 'extended_sanity', 'branch': 'master', 'subcomponent': 'volume,dataset', 'suite': '12hour', 'OS': 'centos'}
    # triggerJob(param)