from membase.api.capella_rest_client import RestConnection as CapellaRestConnection
from membase.api.on_prem_rest_client import RestConnection as OnPremRestConnection, RestHelper, Bucket, vBucket
from TestInput import TestInputSingleton

if TestInputSingleton.input and TestInputSingleton.input.param("capella_run", False):
    RestConnection = CapellaRestConnection
else:
    RestConnection = OnPremRestConnection
