from TestInput import TestInputSingleton

if TestInputSingleton.input and TestInputSingleton.input.param("capella_run", False):
    from membase.api.capella_rest_client import RestConnection
else:
    from membase.api.on_prem_rest_client import RestConnection, RestHelper, Bucket, vBucket
