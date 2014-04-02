DIST_DIR=./dist
DIRS=b conf lib longevity pytests resources scripts unittests
FILES=Makefile README TestInput.py
SLEEP_TIME=3
VERBOSE=0
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
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini $(TESTNAME)

simple-test:
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes-xdcr.ini conf/simple.conf $(VERBOSE)

#test-views:
#	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/py-view.conf
test-viewquery:
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/view-conf/py-viewquery.conf

# required before merging changes to view engine
test-views-pre-merge:
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/view-conf/py-view-pre-merge.conf

# required before merging changes to view engine
test-viewmerge:
	echo "Running view merge tests with single node cluster"
	scripts/start_cluster_and_run_tests.sh b/resources/dev-single-node.ini conf/view-conf/py-viewmerge.conf
	echo "Running view merge tests with 4 nodes cluster"
	sleep $(SLEEP_TIME)
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/view-conf/py-viewmerge.conf

e2e-kv-single-node:
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini conf/py-all-dev.conf

test-xdcr-merge:
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes-xdcr.ini conf/py-xdcrmerge.conf

# specify number of nodes and testcase
any-test:
	scripts/start_cluster_and_run_tests.sh $(NODES) $(TEST)

# specify number of nodes and test conf
any-suite:
	scripts/start_cluster_and_run_tests.sh $(NODES) $(SUITE)
