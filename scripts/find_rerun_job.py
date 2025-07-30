import os as OS
import subprocess
import sys
from couchbase.cluster import Cluster
try:
    from couchbase.cluster import PasswordAuthenticator
    from couchbase.n1ql import N1QLQuery
except ImportError:
    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import ClusterOptions
try:
    import requests
except ImportError:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "requests"])
    import requests
import argparse
import get_jenkins_params as jenkins_api
import traceback


host = '172.23.120.87'
bucket_name = 'rerun_jobs'


def get_run_results():
    run_results = {}
    return run_results


def parse_args():
    """
    Parse command line arguments into a dictionary
    :return: Dictionary of parsed command line arguments
    :rtype: dict
    """
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("build_version", type=str,
                                 help="Couchbase build version of the "
                                      "job")
    argument_parser.add_argument("--executor_jenkins_job",
                                 action='store_true',
                                 help="Run with current executor job")
    argument_parser.add_argument("--jenkins_job", action="store_true",
                                 help="Run with current jenkins job")
    argument_parser.add_argument("--store_data", action="store_true",
                                 help="Store the test_run details. To "
                                      "be used only after testrunner "
                                      "is run")
    argument_parser.add_argument("--install_failure",
                                 action='store_true',
                                 help="Was there install failure in "
                                      "the run?")
    args = vars(argument_parser.parse_args())
    return args


def build_args(build_version, executor_jenkins_job=False,
               jenkins_job=False, store_data=False,
               install_failure=False):
    """
    Build a dictionary of arguments needed for the program
    :param build_version: Couchbase build version of the job
    :type build_version: str
    :param executor_jenkins_job: Run with current Executor job
    :type executor_jenkins_job: bool
    :param jenkins_job: Run with current jenkins job
    :type jenkins_job: bool
    :param store_data: "Store the test_run details. To be used only
    after testrunner is run"
    :type store_data: bool
    :param install_failure: Was there install failure in the run?
    :type install_failure: bool
    :return: Dictionary of parameters
    :rtype: dict
    """
    return locals()


def get_cluster():
    try:
        # SDK 4 way of creating a cluster connection
        auth = ClusterOptions(PasswordAuthenticator('Administrator',
                                                    'esabhcuoc'))
        return Cluster('couchbase://{}'.format(host), auth)
    except Exception:
        # Fall back to prev. behavior (Running on old slaves)
        cluster = Cluster('couchbase://{}'.format(host))
        authenticator = PasswordAuthenticator('Administrator', 'esabhcuoc')
        cluster.authenticate(authenticator)
        return cluster


def select_bucket(cluster, bucket_name, scope_name=None, collection_name=None):
    try:
        if collection_name is None:
            # Select default collection for all operations
            return cluster.bucket(bucket_name).default_collection()
        else:
            # Select a specific collection
            return cluster.bucket(bucket_name)\
                .scope(scope_name).collection(collection_name)
    except Exception:
        # Fall back to prev. behavior (Running on old slaves)
        try:
            return cluster.bucket(bucket_name)
        except AttributeError:
            # For SDK-2 slaves
            return cluster.open_bucket(bucket_name)


def run_query(cluster, bucket, query):
    print(f"Running query: {query}")
    try:
        # SDK 4 way of running and fetching the result rows
        res = cluster.query(query)
        # Return the rows as a list since we are checking for row length
        return [row for row in res.rows()]
    except Exception:
        # Fall back to prev. behavior (Running on old slaves)
        try:
            return bucket.n1ql_query(N1QLQuery(query))
        except Exception:
            return bucket.query(query)


def find_rerun_job(args):
    """
    Find if the job was run previously
    :param args: Dictionary of arguments. Run build_args() if calling
    this from python script or parse_args() if running from shell
    :type args: dict
    :return: If the job was run previously
    :rtype: bool
    """
    name = None
    store_data = args['store_data']
    install_failure = args['install_failure']
    if args['executor_jenkins_job']:
        os = OS.getenv('os')
        component = OS.getenv('component')
        sub_component = OS.getenv('subcomponent')
        version_build = OS.getenv('version_number')
        parameters = OS.getenv('parameters')
        bucket_type, gsi_type = get_bucket_gsi_types(parameters)
        name = "{}_{}_{}".format(os, component, sub_component)
        if bucket_type:
            name = "{}_{}".format(name, bucket_type)
        if gsi_type:
            name = "{}_{}".format(name, gsi_type)
    elif args['jenkins_job']:
        name = OS.getenv('JOB_NAME')
        version_build = args['build_version']
    else:
        os = args['os']
        component = args['component']
        sub_component = args['sub_component']
        parameters = args['parameters']
        bucket_type, gsi_type = get_bucket_gsi_types(parameters)
        if os and component and sub_component:
            name = "{}_{}_{}".format(os, component, sub_component)
            if bucket_type:
                name = "{}_{}".format(name, bucket_type)
            if gsi_type:
                name = "{}_{}".format(name, gsi_type)
        elif args['name']:
            name = args['name']
        version_build = args['build_version']
    if not name or not version_build:
        return False, {}
    cluster = get_cluster()
    rerun_jobs = select_bucket(cluster, bucket_name)
    rerun = False
    doc_id = "{}_{}".format(name, version_build)
    try:
        run_document = rerun_jobs.get(doc_id, quiet=True)
        if not store_data:
            if not run_document.success:
                return False, {}
            else:
                return True, run_document.value
        parameters = jenkins_api.get_params(OS.getenv('BUILD_URL'))
        run_results = get_run_results()
        job_to_store = {
            "job_url": OS.getenv('BUILD_URL'),
            "build_id": OS.getenv('BUILD_ID'),
            "run_params": parameters,
            "run_results": run_results,
            "install_failure": install_failure}
        if run_document.success:
            rerun = True
            run_document = run_document.value
        else:
            run_document = {
                "build": version_build,
                "num_runs": 0,
                "jobs": []}
        run_document['num_runs'] += 1
        run_document['jobs'].append(job_to_store)
        rerun_jobs.upsert(doc_id, run_document, ttl=(7*24*60*60))
        return rerun, run_document
    except Exception as e:
        print(e)
        return False, {}


def should_dispatch_job(os, component, sub_component, version,
                        parameters, only_pending_jobs=False,
                        only_failed_jobs=False,
                        only_unstable_jobs=False,
                        only_install_failed=False):
    """
    Finds if a job has to be dispatched for a particular os, component,
    subcomponent and version. The method finds if the job had run
    successfully previously, if the job is currently running.
    :param os: Os of the job
    :type os: str
    :param component: Component of the job
    :type component: str
    :param sub_component: Sub-component of the job
    :type sub_component: str
    :param version: Version of the server for the job
    :type version: str
    :param parameters: Get the test parameters
    :type parameters: str
    :param only_pending_jobs: Run only pending jobs
    :type only_pending_jobs: bool
    :param only_failed_jobs: Run only failed jobs
    :type only_failed_jobs: bool
    :param only_unstable_jobs: Run only unstable jobs
    :type only_unstable_jobs: bool
    :param only_install_failed: Run only install failed jobs
    :type only_install_failed: bool
    :return: Boolean on whether to dispatch the job or not
    :rtype: bool
    """
    try:
        bucket_type, gsi_type = get_bucket_gsi_types(parameters)
        doc_id = "{0}_{1}_{2}".format(os, component, sub_component)
        if bucket_type:
            doc_id = "{0}_{1}".format(doc_id, bucket_type)
        if gsi_type:
            doc_id = "{0}_{1}".format(doc_id, gsi_type)
        doc_id = "{0}_{1}".format(doc_id, version)
        cluster = get_cluster()
        rerun_jobs = select_bucket(cluster, bucket_name)
        rerun_jobs.timeout = 60
        user_name = "{0}-{1}%{2}".format(component, sub_component, version)
        query = "select * from `QE-server-pool` where username like " \
                "'{0}' and state = 'booked' and os = '{1}'" \
                .format(user_name, os)
        qe_server_pool = select_bucket(cluster, "QE-server-pool")
        qe_server_pool.timeout = 60
        n1ql_result = run_query(cluster, qe_server_pool, query)
        if list(n1ql_result):
            print("Tests are already running. Not dispatching another job")
            return False
        pending = False
        failed = False
        unstable = False
        successful = False
        install_failed = False
        run_document = rerun_jobs.get(doc_id, quiet=True)
        if not run_document.success:
            # No run for this job has occured yet.
            if only_failed_jobs or only_unstable_jobs or only_install_failed:
                pending = True
            else:
                # Run job since it's not been run yet
                pending = True
        # Check if the collected jobs had required results to run the job
        greenboard = select_bucket(cluster, "greenboard")
        greenboard.timeout = 60
        job_doc_id = "{0}_server".format(version)
        jobs_doc = greenboard.get(job_doc_id, quiet=True)

        if not jobs_doc.success and pending:
            print("No jobs found for this build yet. Run the job")
            return True

        try:
            jobs_doc = jobs_doc.value
        except AttributeError:
            jobs_doc = jobs_doc.content_as[dict]

        jobs_name = "{0}-{1}_{2}".format(os, component, sub_component)
        last_job_url = ""
        # Change all windows versions to windows as os param to search
        # through greenboard document
        if "windows" in os:
            os = "win"
        if "os" in jobs_doc:
            if os.upper() in jobs_doc["os"]:
                job_found = False
                if not bucket_type:
                    bucket_type = "COUCHSTORE"
                if not gsi_type:
                    gsi_type = "UNDEFINED"
                jobs_name = "{0}bucket_storage={1}GSI_type={" \
                            "2}".format(jobs_name,
                                        bucket_type.upper(),
                                        gsi_type.upper())
                # Search for job in all components
                for comp in jobs_doc["os"][os.upper()].keys():
                    if jobs_name in jobs_doc["os"][os.upper()][comp]:
                        jobs = jobs_doc["os"][os.upper()][comp][jobs_name]
                        job_found = True
                        pending = False
                        for job in jobs:
                            job_result = job["result"]
                            last_job_url = "{0}{1}".format(job["url"],
                                                           job[
                                                               "build_id"])
                            if job_result == "FAILURE":
                                failed = True
                            elif job_result == "UNSTABLE":
                                unstable = True
                            elif job_result == "ABORTED":
                                unstable = True
                            elif job_result == "SUCCESS":
                                successful = True
                            elif job_result == "INST_FAIL":
                                install_failed = True
                        break
                if not job_found:
                    pending = True
                else:
                    print("Found the job run in greenboard records")
            else:
                pending = True
        else:
            pending = True
        if pending and run_document.success:
            # Job was run previously but has not been collected yet.
            # Check the last run job directly instead.
            run_document = run_document.value
            last_job = run_document['jobs'][-1]
            last_job_url = last_job['job_url'].rstrip('/')
            result = jenkins_api.get_js(last_job_url, "tree=result")
            if result and "result" in result:
                pending = False
                job_result = result["result"]
                if job_result == "UNSTABLE":
                    unstable = True
                if job_result == "FAILURE":
                    failed = True
                if job_result == "ABORTED":
                    unstable = True
                if job_result == "SUCCESS":
                    successful = True
                if job_result == "INST_FAIL":
                    install_failed = True
        if successful:
            # there was a successful run for this job. Don't dispatch
            # further jobs
            print("Job had run successfully previously.")
            print("{} is the successful job.".format(last_job_url))
            return False
        if only_install_failed:
            if (not successful and not unstable) and install_failed and not pending:
                # Run only if the job had install failure
                print("Job had install failure previously. Running only install failed jobs")
                return True
            else:
                print("Job has run previously without install failure. Or is still pending")
                return False
        if only_failed_jobs:
            if (not successful and not unstable) and failed and not pending:
                # Run only if the job had failed
                print("Job had failed previously. Running only failed jobs")
                return True
            else:
                print("Job has run previously without Failure. Or is "
                      "still pending")
                return False
        if only_unstable_jobs:
            if (not successful) and unstable and not pending:
                # Run only if previous jobs were unstable
                print("Previous jobs were unstable. Running only unstable jobs")
                return True
            else:
                print("Job has run previously successfully. Or they are "
                      "still pending. ")
                return False
        if only_pending_jobs:
            if pending:
                # Run only if the job wasn't run previously
                print("Running pending jobs")
                return True
            else:
                print("Job were run previously")
                return False
        print("None of the rerun conditions were met. Running the job")
        return True
    except Exception as e:
        print("Exception occured while finding if job has to be "
              "dispatched: %s" % e)
        traceback.print_exc()
        return True


def get_bucket_gsi_types(parameters):
    """
    Parse the test params and determince the bucket type and gsi type
    if any.
    :param parameters: Get the test parameters from job
    :type parameters: str
    :return: Bucket storage type and gsi type, if any
    :rtype: string, string
    """
    bucket_type = ""
    gsi_type = ""
    for parameter in parameters.split(","):
        if "bucket_storage" in parameter:
            bucket_type = parameter.split("=")[1]
        elif "gsi_type" in parameter:
            gsi_type = parameter.split("=")[1]
    return bucket_type, gsi_type


if __name__ == "__main__":
    # args = parse_args()
    # rerun, document = find_rerun_job(args)
    os = sys.argv[1]
    component = sys.argv[2]
    subcomponent = sys.argv[3]
    version = sys.argv[4]
    parameters = sys.argv[5]
    failed = sys.argv[6] == "true"
    unstable = sys.argv[7] == "true"
    pending = sys.argv[8] == "true"
    install_failed = sys.argv[9] == "true"
    # print(rerun.__str__())
    output = should_dispatch_job(os, component, subcomponent,
                                 version, parameters,
                                 only_pending_jobs=pending,
                                 only_failed_jobs=failed,
                                 only_unstable_jobs=unstable,
                                 only_install_failed=install_failed)
    print(output)
