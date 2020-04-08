import os
import testcfg as cfg

# cluster setup
try:
    cfg.CLUSTER
except NameError as AttributeError:
    cfg.CLUSTER = {}

function = "setitup"
if 'default_bucket' in cfg.CLUSTER:
    function = function + ",default_bucket=" + str(cfg.CLUSTER['default_bucket'])
if 'default_mem_quota' in cfg.CLUSTER:
    function = function + ",default_mem_quota=" + str(cfg.CLUSTER['default_mem_quota'])
if 'sasl_buckets' in cfg.CLUSTER and cfg.CLUSTER['sasl_buckets'] > 0:
    function = function + ",sasl_buckets=" + str(cfg.CLUSTER['sasl_buckets'])
if 'sasl_mem_quota' in cfg.CLUSTER:
    function = function + ",sasl_mem_quota=" + str(cfg.CLUSTER['sasl_mem_quota'])
if 'standard_buckets' in cfg.CLUSTER and cfg.CLUSTER['standard_buckets'] > 0:
    function = function + ",standard_buckets=" + str(cfg.CLUSTER['standard_buckets'])
if 'standard_mem_quota' in cfg.CLUSTER:
    function = function + ",standard_mem_quota=" + str(cfg.CLUSTER['standard_mem_quota'])
if 'initial_nodes' in cfg.CLUSTER:
    function = function + ",initial_nodes=" + str(cfg.CLUSTER['initial_nodes'])

if 'xdcr' in cfg.CLUSTER and cfg.CLUSTER['xdcr']:
    function = function + ",xdcr=True"
    if 'bidirection' in cfg.CLUSTER and cfg.CLUSTER['bidirection']:
        function = function + ",rdirection=bidirection"

os.system("cd .. && ./testrunner -i " + cfg.CLUSTER['ini'] + " -t cluster_setup.SETUP." + function + " && cd pysystests")
