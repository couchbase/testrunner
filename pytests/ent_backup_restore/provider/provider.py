#!/usr/bin/python3

import abc

class Provider(metaclass=abc.ABCMeta):
    def __init__(self, access_key_id, bucket, cacert, endpoint, no_ssl_verify, region, secret_access_key, staging_directory):
        """Instantiate a new 'Provider' object. Should only be created by implementing super classes. Defines all the
        required shared functionality between cloud providers.
        """
        self.access_key_id = access_key_id
        self.bucket = bucket
        self.cacert = cacert
        self.endpoint = endpoint
        self.no_ssl_verify = no_ssl_verify
        self.region = region
        self.secret_access_key = secret_access_key
        self.staging_directory = staging_directory

    @abc.abstractmethod
    def schema_prefix(self):
        """Returns the schema prefix expected by cbbackupmgr for the given cloud provider."""
        raise NotImplementedError

    @abc.abstractmethod
    def setup(self):
        """Run any pre-testing setup. For most cloud providers this will mean ensuring the bucket exists and is ready
        for cbbackupmgr to use.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def teardown(self, info, remote_client):
        """Run any post-testing teardown operations. For most cloud providers this will mean removing any objects
        created by cbbackupgmr.

        Each cloud provider should ensure that they use the common '_remove_staging_directory' function to cleanup the
        staging directory.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def remove_bucket(self):
        """Remove the storage bucket being used for testing."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_json_object(self, key):
        """Returns the object from the object store with the given key. The object must contain valid JSON."""
        raise NotImplementedError

    @abc.abstractmethod
    def list_objects(self, prefix=None):
        """Returns a list of all the objects in the object store. If a prefix is provided, only objects with the given
        prefix will be returned.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete_objects(self, prefix):
        """Remove all the objects from the object store with the given prefix."""
        raise NotImplementedError

    @abc.abstractmethod
    def list_backups(self, archive, repo):
        """List all the backups that currently exist in the remote given archive/repo."""
        raise NotImplementedError

    @abc.abstractmethod
    def num_multipart_uploads(self):
        """Returns the number of in-progress multipart uploads (the setup/teardown) logic should abort any multipart
        uploads in the event that cbbackupmgr crashes and doesn't do it itself. This will allow testing to continue
        without leaking logic into the following tests.
        """
        raise NotImplementedError

    def _remove_staging_directory(self, info, remote_client):
        if info in ('linux', 'mac'):
            command = f"rm -rf {self.staging_directory}"
            output, error = remote_client.execute_command(command)
            remote_client.log_command_output(output, error)
        elif info == 'windows':
            remote_client.remove_directory_recursive(self.staging_directory)
