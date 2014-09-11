from librabbitmq import Connection
import argparse
import paramiko
import sys
import time
import os
import shutil

python_exe = "python"
if os.system("grep \'centos\' /etc/issue -i -q") == 0:
    python_exe = "python2.7"

def get_ssh_client(ip, username=None, password=None, timeout=10):
    client = None
    try:
        ip = ip.split(':')[0]
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        username = username or cfg.SSH_USER
        password = password or cfg.SSH_PASSWORD
        client.connect(ip, username=username, password=password, timeout=timeout)
        print("Successfully SSHed to {0}".format(ip))
    except Exception as ex:
        print ex
        sys.exit(1)
    return client

def get_sftp_client(ip, username=None, password=None,):
    try:
        ip = ip.split(':')[0]
        trans = paramiko.Transport((ip))
        username = username or cfg.SSH_USER
        password = password or cfg.SSH_PASSWORD
        trans.connect(username=username, password=password)
        print ("SFTPing to {0}".format(ip))
        sftp_client = paramiko.SFTPClient.from_transport(trans)
        return sftp_client
    except Exception as ex:
        print ex
        sys.exit(1)

def start_rabbitmq():
    vhost_present = False
    tries = 1
    print("\n##### Setting up RabbitMQ @ {0} #####".format(cfg.RABBITMQ_IP))
    rabbitmq_client = get_ssh_client(cfg.RABBITMQ_IP)
    _, stdout, _ = rabbitmq_client.exec_command("ps aux|grep rabbitmq|grep -v grep|awk \'{print $2}\'")
    print ("Killing existing RabbitMQ process ...")
    for pid in stdout:
        if pid == "":
            continue
        rabbitmq_client.exec_command("sudo kill -9 {0}".format(pid))
    if cfg.RABBITMQ_LOG_LOCATION is not "":
        print("Deleting RabbitMQ logs from {0}".format(cfg.RABBITMQ_LOG_LOCATION))
        rabbitmq_client.exec_command("rm -rf {0}/*.*".format(cfg.RABBITMQ_LOG_LOCATION))
    print ("Starting RabbitMQ ...")
    rabbitmq_client.exec_command("screen -dmS rabbitmq sh -c \'sudo rabbitmq-server start; exec bash;\'")
    time.sleep(20)
    _, stdout, _ = rabbitmq_client.exec_command("sudo rabbitmqctl status")
    for line in stdout.readlines():
        sys.stdout.write(line)
    print("Rabbitmq has been restarted and is now running!")
    _, stdout, _ = rabbitmq_client.exec_command("sudo rabbitmqctl list_vhosts")
    for line in stdout.readlines():
        if not vhost_present:
            if cfg.CB_CLUSTER_TAG in line:
                vhost_present = True
        sys.stdout.write(line)
    if not vhost_present :
        print ("Adding vhost {0} and setting permissions".format(cfg.CB_CLUSTER_TAG))
        rabbitmq_client.exec_command("sudo rabbitmqctl add_vhost {0}".format(cfg.CB_CLUSTER_TAG))
        rabbitmq_client.exec_command("sudo rabbitmqctl set_permissions -p {0} guest '.*' '.*' '.*'".format(cfg.CB_CLUSTER_TAG))
        _, stdout, _ = rabbitmq_client.exec_command("sudo rabbitmqctl list_vhosts")
        for line in stdout.readlines():
            sys.stdout.write(line)
    time.sleep(30)
    while True:
        try:
            tries += 1
            Connection(host=cfg.RABBITMQ_IP, userid="guest", password="guest", virtual_host=cfg.CB_CLUSTER_TAG)
            print("Connected to RabbitMQ vhost")
            break
        except Exception as e:
            print e
            if tries <= 5:
                print("Retrying connection {0}/5 ...".format(tries))
                rabbitmq_client.exec_command("sudo rabbitmqctl delete_vhost {0}".format(cfg.CB_CLUSTER_TAG))
                rabbitmq_client.exec_command("sudo rabbitmqctl add_vhost {0}".format(cfg.CB_CLUSTER_TAG))
                rabbitmq_client.exec_command("sudo rabbitmqctl set_permissions -p {0} guest '.*' '.*' '.*'".format(cfg.CB_CLUSTER_TAG))
                time.sleep(30)
                continue
            sys.exit(1)
    rabbitmq_client.close()

def start_worker(worker_ip):
    print("##### Setting up Celery Worker @ {0} #####".format(worker_ip))
    worker_client = get_ssh_client(worker_ip)

    # Copy testcfg.py file to all workers
    worker_client.open_sftp().put("./testcfg.py", os.path.join(cfg.WORKER_PYSYSTESTS_PATH, "testcfg.py"))

    # kill celery,remove screenlog
    _, stdout, _ = worker_client.exec_command("ps aux|grep celery|grep -v grep|awk \'{print $2}\'")
    for pid in stdout:
        if pid == "":
            continue
        print ("Killing existing Celery process ...pid {0}".format(pid))
        worker_client.exec_command("sudo kill -9 {0}".format(pid))
    worker_client.exec_command("screen -ls | grep \'celery\' | awk '{print $1}' | xargs -i screen -X -S {} quit")
    worker_client.exec_command("screen -wipe")
    worker_client.exec_command("rm -rf {0}/screenlog.0".format(cfg.WORKER_PYSYSTESTS_PATH))
    _, stdout, _ = worker_client.exec_command("ps aux|grep memc|grep -v grep|awk \'{print $2}\'")
    for pid in stdout.readlines():
        print("Killing memcached process with pid {0}".format(pid))
        worker_client.exec_command("kill -9 {0}".format(pid))
    worker_client.exec_command("memcached -u couchbase -d -l {0} -p 11911".format(worker_ip))
    _, stdout, _ = worker_client.exec_command("ps aux|grep memc|grep -v grep|awk \'{print $2}\'")
    for pid in stdout.readlines():
        print("Memcached is now running with pid {0}".format(pid))
    print("Starting celery worker...")
    if worker_ip == cfg.WORKERS[0]:
        _, stdout, _ = worker_client.exec_command("cd {0}; pwd; screen -dmS celery -L sh -c  \ "
        "\'celery worker -c 8 -A app -B -l ERROR --purge -I app.init; exec bash;\'".format(cfg.WORKER_PYSYSTESTS_PATH))
    else:
        _, stdout, _ = worker_client.exec_command("cd {0}; pwd; screen -dmS celery -L sh -c \
         \'celery worker -c 16 -A app -l ERROR -I app.init; exec bash;\'".format(cfg.WORKER_PYSYSTESTS_PATH))
    time.sleep(20)
    #read_screenlog(worker_ip, cfg.WORKER_PYSYSTESTS_PATH, stop_if_EOF=True)
    worker_client.close()

def start_seriesly():
    print("##### Setting up Seriesly @ {0} #####".format(cfg.SERIESLY_IP))
    cbmonitor_client = get_ssh_client(cfg.SERIESLY_IP)
    print("Killing seriesly ...")
    cbmonitor_client.exec_command("killall -9 seriesly")
    if cfg.SERIESLY_DB_LOCATION is not "":
        print("Deleting old Seriesly db files from {0}".format(cfg.SERIESLY_DB_LOCATION))
        cbmonitor_client.exec_command("rm -rf {0}/*.*".format(cfg.SERIESLY_DB_LOCATION))
    # kill all existing screens
    cbmonitor_client.exec_command("screen -ls | grep \'seriesly\' | awk \'{print $1}\' | xargs -i screen -X -S {} quit")
    cbmonitor_client.exec_command("screen -ls | grep \'webapp\' | awk \'{print $1}\' | xargs -i screen -X -S {} quit")
    cbmonitor_client.exec_command("screen -ls | grep \'ns_collector\' | awk \'{print $1}\' | xargs -i screen -X -S {} quit")
    cbmonitor_client.exec_command("screen -ls | grep \'atop_collector\' | awk \'{print $1}\' | xargs -i screen -X -S {} quit")
    cbmonitor_client.exec_command("rm -rf {0}/screenlog.0".format(cfg.CBMONITOR_HOME_DIR))
    # screen 1 - start seriesly
    print ("Starting seriesly...")
    cbmonitor_client.exec_command("screen -dmS seriesly -L sh -c \'cd {0}; ./seriesly; exec bash;\'".
                                  format(cfg.SERIESLY_LOCATION))
    _, stdout, _ = cbmonitor_client.exec_command("pgrep seriesly")
    print ("Seriesly is running with pid {0}".format(stdout.readlines()[0]))

def start_cbmonitor():
    print("\n##### Setting up CBMonitor @ {0} #####".format(cfg.SERIESLY_IP))
    cbmonitor_client = get_ssh_client(cfg.SERIESLY_IP)
    # screen 2 - start webserver
    print ("Starting Django webserver...")
    _, stdout, _ = cbmonitor_client.exec_command("pgrep webapp")
    for pid in stdout.readlines():
        cbmonitor_client.exec_command("kill -9 {0}".format(pid))
    cbmonitor_client.exec_command("cd {0}; screen -dmS webapp -L sh -c \'./bin/webapp add-user -S;./bin/webapp syncdb; \
     ./bin/webapp runserver {1}:8000; exec bash;\'".format(cfg.CBMONITOR_HOME_DIR, cfg.SERIESLY_IP))
    time.sleep(5)
    _, stdout, _ = cbmonitor_client.exec_command("pgrep webapp")
    print ("Webserver is running with pid {0}".format(stdout.readlines()[0]))
    # screen 3 - start ns_collector
    print ("Starting ns_collector...")
    cbmonitor_client.exec_command("cd {0}; screen -dmS ns_collector -L sh -c \'./bin/ns_collector sample.cfg; exec bash;\'".
                                  format(cfg.CBMONITOR_HOME_DIR))
    time.sleep(5)
    _, stdout, _ = cbmonitor_client.exec_command("pgrep ns_collector")
    print ("ns_collector is running with pid {0}".format(stdout.readlines()[0]))
    # screen 4 - start atop_collector
    print ("Starting atop_collector...")
    cbmonitor_client.exec_command("cd {0}; screen -dmS atop_collector -L sh -c \'./bin/atop_collector sample.cfg; exec bash;\'".
                                  format(cfg.CBMONITOR_HOME_DIR))
    time.sleep(5)
    _, stdout, _ = cbmonitor_client.exec_command("pgrep atop_collector")
    print ("atop_collector is running with pid {0}".format(stdout.readlines()[0]))
    read_screenlog(cfg.SERIESLY_IP, cfg.CBMONITOR_HOME_DIR, stop_if_EOF=True, lines_to_read=100)
    cbmonitor_client.close()

def read_screenlog(ip, screenlog_dir, retry=10, stop_if_EOF=False, lines_to_read=20000):
    line = ""
    line_count = 0
    last_pos = 0
    transport_client = get_sftp_client(ip)
    screen_log = "{0}/screenlog.0".format(screenlog_dir)
    op_file = transport_client.open(screen_log, 'r')
    while "Test Complete" not in line and line_count < lines_to_read:
        op_file.seek(last_pos)
        line = op_file.readline()
        last_pos = op_file.tell()
        if line is not None and line is not "":
            sys.stdout.write(line)
            line_count += 1
        else:
            #Reached EOF, will retry after 'retry' secs
            if stop_if_EOF:
                break
            time.sleep(retry)
    op_file.close()
    transport_client.close()

def run_setup():
    # kick off the setup test
    print("\n##### Starting cluster setup from {0} #####".format(cfg.SETUP_JSON))
    worker_client = get_ssh_client(cfg.WORKERS[0])
    # Import templates if needed
    for template in cfg.SETUP_TEMPLATES:
        print ("Importing document template {0}...".format(template.split('--')[1].split('--')[0]))
        temp = "{0} cbsystest.py import template {1}".format(python_exe, template)
        print temp
        _, stdout, _ = worker_client.exec_command("cd {0}; {1} cbsystest.py import template {2} --cluster {3}".
                                                  format(cfg.WORKER_PYSYSTESTS_PATH, python_exe, template, cfg.CB_CLUSTER_TAG))
        for line in stdout.readlines():
            print line
    print ("Running test ...")
    _, stdout, _ = worker_client.exec_command("cd {0}; {1} cbsystest.py run test --cluster \'{2}\' --fromfile \'{3}\'".
                                              format(cfg.WORKER_PYSYSTESTS_PATH, python_exe, cfg.CB_CLUSTER_TAG, cfg.SETUP_JSON))
    read_screenlog(cfg.WORKERS[0], cfg.WORKER_PYSYSTESTS_PATH)
    worker_client.close()

def run_test():
    print "\n##### Starting system test #####"
    start_worker(cfg.WORKERS[0])
    # import doc template in worker
    worker_client = get_ssh_client(cfg.WORKERS[0])
    for template in cfg.TEST_TEMPLATES:
        print ("Importing document template {0}...".format(template.split('--')[1].split('--')[0]))
        temp = "{0} cbsystest.py import template {1}".format(python_exe, template)
        print temp
        _, stdout, _ = worker_client.exec_command("cd {0}; {1} cbsystest.py import template {2} --cluster {3}".
                                                  format(cfg.WORKER_PYSYSTESTS_PATH, python_exe, template, cfg.CB_CLUSTER_TAG))
        for line in stdout.readlines():
            print line
    # Start sys test
    print ("Starting system test from {0}...".format(cfg.TEST_JSON))
    _, stdout, _ = worker_client.exec_command("cd {0}; {1} cbsystest.py run test --cluster \'{2}\' --fromfile \'{3}\'".
                                              format(cfg.WORKER_PYSYSTESTS_PATH, python_exe, cfg.CB_CLUSTER_TAG, cfg.TEST_JSON))
    time.sleep(5)
    for line in stdout.readlines():
        sys.stdout.write(line)
    read_screenlog(cfg.WORKERS[0], cfg.WORKER_PYSYSTESTS_PATH)
    worker_client.close()

def pre_install_check():
    try:
        print("##### Pre-install inspection #####")
        print("Inspecting Couchbase server VMs ...")
        for vm_ip in cfg.CLUSTER_IPS:
            if cfg.COUCHBASE_OS == "windows":
                vm_client = get_ssh_client(vm_ip, cfg.COUCHBASE_SSH_USER, cfg.COUCHBASE_SSH_PASSWORD)
            else:
                vm_client = get_ssh_client(vm_ip)
            vm_client.close()
        print ("Inspecting RabbitMQ ...")
        rabbitmq = get_ssh_client(cfg.RABBITMQ_IP)
        rabbitmq.close()
        print ("Inspecting Worker ...")
        worker = get_ssh_client(cfg.WORKERS[0])
        worker.close()
        print ("Inspecting CBMonitor ...")
        cbmonitor = get_ssh_client(cfg.SERIESLY_IP)
        cbmonitor.close()
        print("Inspection complete!")
    except Exception as e:
        print e
        sys.exit()

def upload_stats():
    print "\n##### Uploading stats to CBFS #####"
    worker_client = get_ssh_client(cfg.WORKERS[0])
    push_stats_cmd = "cd {0}; {1} tools/push_stats.py  --version {2} --build {3} --spec {4} \
    --name {5} --cluster {6}".format(cfg.WORKER_PYSYSTESTS_PATH, python_exe, args['build'].split('-')[0], args['build'].split('-')[1],
    cfg.TEST_JSON, cfg.TEST_JSON[cfg.TEST_JSON.rfind('/') + 1 : cfg.TEST_JSON.find('.')] , cfg.CB_CLUSTER_TAG)
    print ("Executing {0}".format(push_stats_cmd))
    _, stdout, _ = worker_client.exec_command(push_stats_cmd)
    time.sleep(30)
    for line in stdout.readlines():
        print line
    worker_client.close()

def install_couchbase():
    print("Installing version {0} Couchbase on servers ...".format(args['build']))
    install_cmd = "cd ..; {0} scripts/install.py -i {1} -p product=cb,version={2},parallel=true,{3}".\
                    format(python_exe, cfg.CLUSTER_INI, args['build'], args['params'])
    print("Executing : {0}".format(install_cmd))
    os.system(install_cmd)
    if cfg.CLUSTER_RAM_QUOTA != "":
        os.system("curl -d memoryQuota={0} \"http://{1}:{2}@{3}:8091/pools/default\"".
                  format(cfg.CLUSTER_RAM_QUOTA, cfg.COUCHBASE_USER, cfg.COUCHBASE_PWD, cfg.CLUSTER_IPS[0]))
    for ip in cfg.CLUSTER_IPS:
        os.system("curl -X POST -d \'ale:set_loglevel(xdcr_trace, debug).\' \"http://{0}:{1}@{2}:8091/diag/eval\"".
                  format(cfg.COUCHBASE_USER, cfg.COUCHBASE_PWD, ip))

def warn_skip(task):
    print("\nWARNING : Skipping {0}\n".format(task))
    return True

def run(args):
    exlargs = args['exclude']

    # Pre-install check
    ("inspect" in exlargs) and warn_skip("Inspection") or pre_install_check()

    # Install Couchbase
    ("install" in exlargs) and warn_skip("Installation") or install_couchbase()

    # Setup RabbitMQ
    ("rabbitmq" in exlargs) and warn_skip("RabbitMQ") or start_rabbitmq()

    # Setup Seriesly
    ("seriesly" in exlargs) and warn_skip("Seriesly") or start_seriesly()

    # Start workers
    ("worker" in exlargs) and warn_skip("Celery Worker setup") or\
                               [start_worker(ip) for ip in cfg.WORKERS]

    # Cluster-setup/create buckets, set RAM quota
    ("setup" in exlargs) and warn_skip("Cluster setup") or run_setup()

    # Start cbmonitor
    ("cbmonitor" in exlargs) and warn_skip("CBMonitor") or start_cbmonitor()

    # Run test
    ("systest" in exlargs) and warn_skip("System Test") or run_test()

    # Upload stats
    ("stats" in exlargs) and warn_skip("Uploading Stats to CBFS") or upload_stats()

    print("\n############################# Execution Complete! #################################")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tool for running system tests \
                 \nUsage: python runsystest.py --build 3.0.0-355 \
                                              --testcfg xdcr/testcfg_source.py \
                                              --params upr=true,xdcr_upr=false \
                                              --exclude install,seriesly,worker,cbmonitor,cluster,systest,stats")
    parser.add_argument("--build", help="required param: build-version for system test to run on", required=True)
    parser.add_argument("--testcfg", default="testcfg.py", help="required param: location of testcfg file in testcfg dir ")
    parser.add_argument("--params", help="optional param: additional build params eg:vbuckets=1024,upr=true,xdcr_upr=false",
                        required=False)
    parser.add_argument("--exclude",
                            nargs='+',
                            default="",
                            help="optional param: inspect install rabbitmq seriesly worker cbmonitor setup systest stats",
                            required=False)

    try:
        args = vars(parser.parse_args())
        testcfg = args['testcfg']
        if os.path.basename(os.path.abspath(os.getcwd())) != 'pysystests':
            raise Exception("Run script from testrunner/pysystests folder, current folder is: %s" % os.getcwd())
        shutil.copy(testcfg, "./testcfg.py")
        print "Copied {0} to {1}/testcfg.py".format(testcfg, os.getcwd())
        cfg = __import__("testcfg")
        run(args)
    except Exception as e:
        print e
        raise
