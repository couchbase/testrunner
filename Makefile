DIST_DIR=./dist
TESTRUNNER_DIST=${DIST_DIR}/testrunner

.PHONY: clean testrunner

testrunner:
	mkdir -p ${TESTRUNNER_DIST}
	tar cvf ${TESTRUNNER_DIST}/testrunner.tar --exclude='*.pyc' conf tests testrunner.sh
	tar cvfh ${TESTRUNNER_DIST}/testrunner_libbin.tar --exclude='*.pyc' lib bin
	tar xvf ${TESTRUNNER_DIST}/testrunner.tar -C ${TESTRUNNER_DIST}
	tar xvf ${TESTRUNNER_DIST}/testrunner_libbin.tar -C ${TESTRUNNER_DIST}
	rm -f ${TESTRUNNER_DIST}/testrunner.tar ${TESTRUNNER_DIST}/testrunner_libbin.tar
	touch ${TESTRUNNER_DIST}/server.txt
	tar czvf ${DIST_DIR}/testrunner.tar.gz -C ${DIST_DIR} testrunner

clean:
	rm -rf ${DIST_DIR}
