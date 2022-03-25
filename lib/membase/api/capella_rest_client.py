from membase.api.on_prem_rest_client import RestConnection as OnPremRestConnection
from lib.capella.internal_api import capella_utils as CapellaAPI
from lib.Cb_constants.CBServer import CbServer
import json

class RestConnection(OnPremRestConnection):
    def __init__(self, serverInfo):
        super(RestConnection, self).__init__(serverInfo)
        self.pod = CbServer.pod
        self.tenant = CbServer.tenant
        self.cluster_id = CbServer.cluster_id

    def delete_bucket(self, bucket='default', num_retries=3, poll_interval=5):
        CapellaAPI.delete_bucket(
            self.pod, self.tenant, self.cluster_id, bucket)

    def create_bucket(self, bucket='',
                      ramQuotaMB=1,
                      replicaNumber=1,
                      proxyPort=11211,
                      bucketType='membase',
                      replica_index=1,
                      threadsNumber=3,
                      flushEnabled=1,
                      evictionPolicy='valueOnly',
                      lww=False,
                      maxTTL=None,
                      compressionMode='passive',
                      storageBackend='couchstore'):

        if ramQuotaMB == 0:
            ramQuotaMB = 100

        params = {'name': bucket,
                  'memoryAllocationInMb': ramQuotaMB,
                  "replicas": replicaNumber,
                  "bucketConflictResolution": "seqno",
                  "flush": False,
                  "durabilityLevel": "none",
                  "timeToLive": maxTTL
                  }

        if flushEnabled == 1:
            params['flush'] = True

        if lww:
            params['bucketConflictResolution'] = 'lww'

        CapellaAPI.create_bucket(
            self.pod, self.tenant, self.cluster_id, params)

    def create_function(self, name, body, function_scope=None, username=None, password=None):
        if "n1ql_consistency" not in body["settings"]:
            body["settings"]["n1ql_consistency"] = "none"
        if "user_prefix" not in body["settings"]:
            body["settings"]["user_prefix"] = "eventing"
        status, content = CapellaAPI.create_function(
            self.pod, self.tenant, self.cluster_id, name, body, function_scope)
        if not status:
            raise Exception(content)
        return content

    def lifecycle_operation(self, name, operation, function_scope=None, username=None, password=None):
        if operation == "deploy":
            body = {
                "deployment_status": True,
                "processing_status": True,
            }
            return CapellaAPI.function_settings(self.pod, self.tenant, self.cluster_id, name, body, function_scope)
        elif operation == "undeploy":
            body = {
                "deployment_status": False,
                "processing_status": False
            }
            return CapellaAPI.function_settings(self.pod, self.tenant, self.cluster_id, name, body, function_scope)
        elif operation == "pause":
            body = {
                "processing_status": False,
                "deployment_status": True,
            }
            return CapellaAPI.function_settings(self.pod, self.tenant, self.cluster_id, name, body, function_scope)
        elif operation == "resume":
            body = {
                "processing_status": True,
                "deployment_status": True,
            }
            return CapellaAPI.function_settings(self.pod, self.tenant, self.cluster_id, name, body, function_scope)

    def get_composite_eventing_status(self, username=None, password=None):
        status, content = CapellaAPI.get_composite_eventing_status(self.pod, self.tenant, self.cluster_id)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def get_all_eventing_stats(self, seqs_processed=False, eventing_map=None, username=None, password=None):
        status, content = CapellaAPI.get_all_eventing_stats(self.pod, self.tenant, self.cluster_id, seqs_processed)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def delete_single_function(self, name, function_scope=None, username=None, password=None):
        status, content = CapellaAPI.delete_function(self.pod, self.tenant, self.cluster_id, name, function_scope)
        if not status:
            raise Exception(content)
        return content