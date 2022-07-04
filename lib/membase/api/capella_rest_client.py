from membase.api.on_prem_rest_client import RestConnection as OnPremRestConnection
from lib.capella.utils import CapellaAPI
from lib.Cb_constants.CBServer import CbServer

class RestConnection(OnPremRestConnection):
    def __init__(self, serverInfo):
        super(RestConnection, self).__init__(serverInfo)
        self.capella_api = CapellaAPI(CbServer.capella_credentials)
        self.cluster_id = CbServer.capella_cluster_id

    def delete_bucket(self, bucket='default', num_retries=3, poll_interval=5):
        self.capella_api.delete_bucket(self.cluster_id, bucket)

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

        self.capella_api.create_bucket(self.cluster_id, params)

    def create_function(self, name, body, function_scope=None, username=None, password=None):
        if "n1ql_consistency" not in body["settings"]:
            body["settings"]["n1ql_consistency"] = "none"
        if "user_prefix" not in body["settings"]:
            body["settings"]["user_prefix"] = "eventing"
        self.capella_api.create_eventing_function(self.cluster_id, name, body, function_scope)

    def lifecycle_operation(self, name, operation, function_scope=None, username=None, password=None):
        if operation == "deploy":
            self.capella_api.deploy_eventing_function(self.cluster_id, name, function_scope)
        elif operation == "undeploy":
            self.capella_api.undeploy_eventing_function(self.cluster_id, name, function_scope)
        elif operation == "pause":
            self.capella_api.pause_eventing_function(self.cluster_id, name, function_scope)
        elif operation == "resume":
            self.capella_api.resume_eventing_function(self.cluster_id, name, function_scope)

    def get_composite_eventing_status(self, username=None, password=None):
        return self.capella_api.get_composite_eventing_status(self.cluster_id)

    # This is mocked because Capella does not expose the endpoint.
    def get_all_eventing_stats(self, seqs_processed=False, eventing_map=None, username=None, password=None):
        return {}

    def delete_single_function(self, name, function_scope=None, username=None, password=None):
        self.capella_api.delete_eventing_function(self.cluster_id, name, function_scope)