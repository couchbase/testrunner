DIST_DIR=./dist
DIRS=b conf lib longevity pytests resources scripts unittests
FILES=Makefile README TestInput.py

.PHONY: clean testrunner test test-quick

testrunner:
	mkdir -p ${DIST_DIR}/testrunner
	tar -cvf ${DIST_DIR}/testrunner.tar --exclude='*.pyc' ${DIRS} ${FILES}
	tar -C ${DIST_DIR}/testrunner -xvf ${DIST_DIR}/testrunner.tar
	rm -f ${DIST_DIR}/testrunner.tar
	tar -C ${DIST_DIR} -czvf ${DIST_DIR}/testrunner.tar.gz testrunner

clean:
	rm -rf ${DIST_DIR}

test:
ifdef TESTNAME
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini ${TESTNAME}
else
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini conf/py-all-dev.conf
endif

test-simple:
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini conf/simple.conf
simple-test:
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini conf/simple.conf

test-views:
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/py-view.conf
test-viewquery:
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini conf/py-viewquery.conf

e2e-kv-single-node:
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini conf/py-all-dev.conf

test-xdcr-basic:
	scripts/start_cluster_and_run_tests.sh b/resources/dev-2-nodes-xdcr.ini conf/xdcr-basic.conf

# Note: remove the comment after all advanced unit tests pass
#test-xdcr-adv:
#	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes-xdcr.ini conf/xdcr-adv.conf
#test-xdcr-full:
#    test-xdcr-basic
#    test-xdcr-adv

# specify number of nodes and testcase
any-test:
	scripts/start_cluster_and_run_tests.sh ${NODES} ${TEST}

# specify number of nodes and test conf
any-suite:
	scripts/start_cluster_and_run_tests.sh ${NODES} ${SUITE}
