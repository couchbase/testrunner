DIST_DIR=./dist
DIRS=b conf lib longevity pytests resources scripts unittests
FILES=Makefile README TestInput.py
SLEEP_TIME=3
VERBOSE=0
DEBUG=0
TESTNAME=conf/py-all-dev.conf

.PHONY: clean testrunner test test-quick

testrunner:
	mkdir -p $(DIST_DIR)/testrunner
	tar -cvf $(DIST_DIR)/testrunner.tar --exclude='*.pyc' $(DIRS) $(FILES)
	tar -C $(DIST_DIR)/testrunner -xvf $(DIST_DIR)/testrunner.tar
	rm -f $(DIST_DIR)/testrunner.tar
	tar -C $(DIST_DIR) -czvf $(DIST_DIR)/testrunner.tar.gz testrunner

clean:
	rm -rf $(DIST_DIR)

test:
	git submodule init; git submodule update --init --force --remote
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini $(TESTNAME)

dcp-test:
	git submodule init; git submodule update --init --force --remote
	python3 testrunner.py -i b/resources/dev-4-nodes.ini -c conf/py-dcp.conf -p skip_cleanup=False,dev=True,test=$(TEST)

simple-test:
	git submodule init; git submodule update --init --force --remote
	python3 scripts/start_cluster_and_run_tests.py $(MAKE) b/resources/dev-4-nodes-xdcr.ini conf/simple.conf $(VERBOSE) $(DEBUG)

#test-views:
#	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/py-view.conf
test-viewquery:
	git submodule init; git submodule update --init --force --remote
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/view-conf/py-viewquery.conf

# required before merging changes to view engine
test-views-pre-merge:
	git submodule init; git submodule update --init --force --remote
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/view-conf/py-view-pre-merge.conf

test-views-pre-merge-viewci:
	git submodule init; git submodule update --init --force --remote
	scripts/start_cluster_and_run_tests.sh b/resources/dev-single-node.ini conf/view-conf/py-view-pre-merge-sanscreatedeleteviews.conf
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/view-conf/py-view-pre-merge-sanscreatedeleteviews.conf
# required before merging changes to view engine
test-viewmerge:
	git submodule init; git submodule update --init --force --remote
	echo "Running view merge tests with single node cluster"
	scripts/start_cluster_and_run_tests.sh b/resources/dev-single-node.ini conf/view-conf/py-viewmerge.conf
	echo "Running view merge tests with 4 nodes cluster"
	sleep $(SLEEP_TIME)
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/view-conf/py-viewmerge.conf

test-viewmerge-viewci:
	git submodule init; git submodule update --init --force --remote
	scripts/start_cluster_and_run_tests.sh b/resources/dev-single-node.ini conf/view-conf/py-viewmerge-sansdevview.conf
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/view-conf/py-viewmerge-sansdevview.conf

# required before merging gsi code
test-gsi-integrations-tests:
	git submodule init; git submodule update --init --force
	echo "Running gsi integration tests with 4 node cluster"
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini conf/simple_gsi_n1ql.conf 1 1 $(PARAMS)

test-fts:
	git submodule init; git submodule update --init --force --remote
	echo "Running fts 2 node cluster"
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes-xdcr_n1ql_fts.ini conf/fts/py-fts-simpletopology.conf 1 1 get-cbcollect-info=False,GROUP=PS,fts_quota=1000,index_type=scorch,skip_log_scan=False,skip_disable_nton=True,validate_index_partition=False

e2e-kv-single-node:
	git submodule init; git submodule update --init --force --remote
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini conf/py-all-dev.conf

test-eventing-sanity-tests:
	git submodule init; git submodule update --init --force --remote
	python3 scripts/start_cluster_and_run_tests.py $(MAKE) b/resources/dev-4-nodes-xdcr_n1ql_gsi.ini conf/eventing/eventing_sanity.conf $(VERBOSE) $(DEBUG)

test-xdcr-merge:
	git submodule init; git submodule update --init --force --remote
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes-xdcr.ini conf/py-xdcrmerge.conf

# specify number of nodes and testcase
any-test:
	git submodule init; git submodule update --init --force --remote
	scripts/start_cluster_and_run_tests.sh $(NODES) $(TEST)

# specify number of nodes and test conf
any-suite:
	git submodule init; git submodule update --init --force --remote
	scripts/start_cluster_and_run_tests.sh $(NODES) $(SUITE)
