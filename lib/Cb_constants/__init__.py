from Cb_constants.CBServer import CbServer
# from Cb_constants.ClusterRun import ClusterRun


constants = CbServer
try:
    from TestInput import TestInputSingleton
    if TestInputSingleton.input:
        global_port = TestInputSingleton.input.param("port", None)
        server_port = TestInputSingleton.input.servers("port", None)
        # if global_port == ClusterRun.port or server_port == ClusterRun.port:
        #    constants = ClusterRun
except:
    pass
