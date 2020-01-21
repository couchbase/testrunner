import os
import sys
import time
import subprocess
import configparser
sys.path = ['.']+sys.path
from lib.cluster_run_manager import CRManager

TR_DIR        = os.getcwd()
NS_SERVER_DIR = "../ns_server"


def print_help():
    print("Usage: <start_cluster_and_run_tests.py> <ini> <conf> [VERBOSE]")

def exit_with_help():
    print_help()
    sys.exit(-1)

def parse_ini_servers(ini):
    num_nodes = 0
    Config = configparser.ConfigParser()
    try:
        Config.read(ini)
        opts = Config.options('servers')
        num_nodes = len(opts)
        assert num_nodes > 0
    except configparser.NoSectionError:
        print("Error: unable parse server section {0}".format(ini))
    except AssertionError:
        print("Error: no nodes specified in server section")

    num_nodes > 0 or sys.exit(-1)

    return num_nodes

def ns_clean(make, verbose = 1):
    """ clean ns_server directory and recompile source """

    clean = False
    stdout = None

    if verbose == 0: # quiet
        stdout = open(os.devnull, 'w')

    try:
        os.chdir("{0}{1}build".format(NS_SERVER_DIR, os.sep))
        subprocess.check_call(make.split() + ["ns_dataclean"], stdout = stdout)
        clean = True
    except subprocess.CalledProcessError as cpex:
        print("Error: command {0} failed".format(cpex.cmd))
    except OSError as ex:
        print("Error: unable to write to stdout\n{0}".format(ex))
    finally:
        os.chdir(TR_DIR)

    clean or sys.exit(-1)

def run_test(ini, conf, verbose, debug):
    """ run testrunner process """

    rc = -1
    try:
        os.chdir(TR_DIR)

        args = ["python", "testrunner.py", "-i", ini, "-c", conf]
        params = "makefile=True"

        if verbose == 0:
            params += ",log_level=CRITICAL"
        else:
            if debug == 1:
                params += ",log_level=DEBUG"

        args.extend(["-p", params])

        r, w = os.pipe()
        proc = subprocess.Popen(args, stdout = subprocess.PIPE)
        with open("make_test.log", "w") as log:
            while proc.poll() is None:
                line = proc.stdout.readline()
                if line:
                    print(line[:-1])
                    log.write(str(line))
        rc = proc.returncode
    except OSError as ex:
        print("Error: unable to write to stdout\n{0}".format(ex))

    return rc

def get_verbosity():
    """ 1 = display, 0 = quiet """

    if len(sys.argv) > 4:
        try:
            return int(sys.argv[4])
        except TypeError:
            print("Error: verbose must be a numerical value")
            exit_with_help()
    return 1

def get_debug():
    """ 1 = debug, 0 = info """

    if len(sys.argv) > 5:
        try:
            return int(sys.argv[5])
        except TypeError:
            print("Error: debug must be a numerical value")
            exit_with_help()
    return 1

def main():

    # parse env vars
    len(sys.argv) >= 3 or exit_with_help()
    make = sys.argv[1]
    ini = sys.argv[2]
    conf = sys.argv[3]
    verbose = get_verbosity()
    debug = get_debug()
    num_nodes = parse_ini_servers(ini)


    # setup cluster run nodes
    ns_clean(make, verbose)

    crm = CRManager(num_nodes)
    assert crm.start_nodes()

    # run test
    rc = run_test(ini, conf, verbose, debug)

    # done
    crm.stop_nodes()

    sys.exit(rc)

if __name__ == '__main__':
    main()
