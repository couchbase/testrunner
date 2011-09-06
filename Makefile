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
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini conf/py-all-dev.conf

test-quick:
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini conf/dev-quick.conf
