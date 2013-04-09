#!/bin/sh -ex
./bin/testrunner -i ${ini_file} -c ${test_conf} -p load_phase=1,stats=0
./bin/testrunner -i ${ini_file} -c ${test_conf} -p load_phase=1,hot_load_phase=1,stats=0
./bin/testrunner -i ${ini_file} -c ${test_conf} -p index_phase=1,stats=0
./bin/testrunner -i ${ini_file} -c ${test_conf} -p access_phase=1
