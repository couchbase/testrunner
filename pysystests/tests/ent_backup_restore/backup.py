import argparse
import sys

import paramiko as paramiko

BACKUP_LOCATION = "/tmp/backup"
BACKUP_MANAGER_LOCATION = "/opt/couchbase/bin/cbbackupmgr"
BACKUP_CLIENT = ""

def get_ssh_client(ip, username=None, password=None, timeout=10):
    client = None
    try:
        ip = ip.split(':')[0]
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        username = username
        password = password
        client.connect(ip, username=username, password=password, timeout=timeout)
        print("Successfully SSHed to {0}".format(ip))
    except Exception as ex:
        print ex
        sys.exit(1)
    return client


def create_backup(name, exclude_buckets=[], include_buckets=[], disable_bucket_config=False, disable_views=False,
                  disable_gsi_indexes=False, disable_ft_indexes=False, disable_data=False):
    arg = "config --archive {0} --repo {1}".format(BACKUP_LOCATION, name)
    if exclude_buckets:
        arg += " --exclude-buckets \"{0}\"".format(",".join(exclude_buckets))
    if include_buckets:
        arg += " --include-buckets=\"{0}\"".format(",".join(include_buckets))
    if disable_bucket_config:
        arg += " --disable-bucket-config"
    if disable_views:
        arg += " --disable-views"
    if disable_gsi_indexes:
        arg += " --disable-gsi-indexes"
    if disable_ft_indexes:
        arg += " --disable-ft-indexes"
    if disable_data:
        arg += " --disable-data"
    backup_client = get_ssh_client(BACKUP_CLIENT, "root", "couchbase")
    cmd = "{0} {1}".format(BACKUP_MANAGER_LOCATION, arg)
    _, stdout, _ = backup_client.exec_command(cmd)
    for line in stdout.readlines():
        sys.stdout.write(line)
    backup_client.close()


def take_backup(name, ip, port=8091, cluster_host_username="Administrator", cluster_host_password="password",
                resume=False, purge=False):
    arg = "backup --archive {0} --repo {1} --host http://{2}:{3} --username {4} --password {5}". \
        format(BACKUP_LOCATION, name, ip, port, cluster_host_username, cluster_host_password)
    if resume:
        arg += " --resume"
    if purge:
        arg += " --purge"
    backup_client = get_ssh_client(BACKUP_LOCATION, "root", "couchbase")
    cmd = "{0} {1}".format(BACKUP_MANAGER_LOCATION, arg)
    _, stdout, _ = backup_client.exec_command(cmd)
    for line in stdout.readlines():
        sys.stdout.write(line)
    backup_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to create and take backups.")
    create = parser.add_subparsers(description="create")
    create_ = create.add_parser("create")
    create_.add_argument("--repo", required=True)
    create_.add_argument("--exclude-buckets", default=False, required=False)
    create_.add_argument("--include-buckets", default=False, required=False)
    create_.add_argument("--disable-bucket-config", default=False, required=False)
    create_.add_argument("--disable-views", default=False, required=False)
    create_.add_argument("--disable-gsi-indexes", default=False, required=False)
    create_.add_argument("--disable-ft-indexes", default=False, required=False)
    create_.add_argument("--disable-data", default=False, required=False)
    backup = parser.add_subparsers(description="backup")
    backup_ = backup.add_parser("backup")
    backup_.add_argument("--repo", required=True)
    backup_.add_argument("--host", required=True)
    backup_.add_argument("--port", default=8091, required=False)
    backup_.add_argument("--username", default="Administrator", required=False)
    backup_.add_argument("--password", default="password", required=False)
    backup_.add_argument("--resume", default=False, required=False)
    backup_.add_argument("--purge", default=False, required=False)
    args = parser.parse_args()
    if args.subparsers == "create":
        create_backup(args['repo'], args['exclude-buckets'], args['include-buckets'], args['disable-bucket-config'],
                      args['disable=views'], args['disable-gsi-indexes'], args['disable-ft-indexes'],
                      args['disable-data'])
    elif args.subparsers == 'backup':
        take_backup(args['repo'], args['host'], args['port'], args['username'], args['password'], args['resume'],
                    args['purge'])

