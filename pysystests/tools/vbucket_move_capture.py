import time
import sys
import subprocess
import os
import datetime
import json
import getopt

sys.path.append("../.")
sys.path.append("../lib")

import TestInput

def usage(error=None):
    print("""\
Syntax: vbucket_move_capture.py [options]

Options
 -i <file>        Path to .ini file containing cluster information.
 -p <key=val,...> Comma-separated key=value info.

Available keys:
 interval = <time_interval_to_capture_vbucket_distribution>
 bucket = <bucket_name>

Example:
python tools/vbucket_move_capture.py -i cluster.ini -p interval=200,bucket=default
""")
    sys.exit(error)

if __name__ == "__main__":
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hi:p', [])
        for o, a in opts:
            if o == "-h":
                usage()

        input = TestInput.TestInputParser.get_test_input(sys.argv)
        if not input.servers:
            usage("ERROR: no servers specified. Please use the -i parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError as error:
        usage("ERROR: " + str(error))

    bucket = input.param("bucket", "default")
    time_sleep = input.param("interval", 300)

    while True:
        count = len(input.servers)
        i = 0
        num_nodes = 0
        bucket_json = {}
        server_list = []
        while (count - i > 0):
            cmd = "curl -s http://%s:8091/pools/%s/buckets/%s " % (input.servers[i].ip, bucket, bucket)
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdoutdata, stderrdata=p.communicate()
            bucket_info = "" + stdoutdata
            if bucket_info.find("Requested resource not found") > -1:
                raise Exception ("Node %s doesn't have bucket %s" % (vm_ip, bucket))
            bucket_json = json.loads(bucket_info)
            for server in bucket_json["vBucketServerMap"]["serverList"]:
                tmp = server.split(":")
                server_list.append(tmp[0])
            num_nodes = len(server_list)
            if num_nodes > 1:
                break
            else:
                server_list = []
                ++i

        if i == count:
            raise Exception ("There is no cluster with more than 1 node")

        vbucket_map = bucket_json["vBucketServerMap"]["vBucketMap"]
        k = {}

        for i in range(num_nodes):
            k[i] = 0
        for i in vbucket_map:
            for m in range(num_nodes):
                if i[0] == m:
                    k[m] += 1
        print("#################################################")
        print("TIME NOW: ")
        print(datetime.datetime.now())
        print("*************************************************")
        print(" ")
        for i in range(num_nodes):
            print("node %s have %d vbuckets" % (server_list[i], k[i]))
        print(" ")
        print("******************** Sleep in %s seconds ************" % time_sleep)
        print(" ")
        time.sleep(time_sleep)
