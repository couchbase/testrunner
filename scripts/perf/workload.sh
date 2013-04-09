#!/bin/sh -ex
./bin/testrunner -i ${ini_file} -c ${test_conf} -p load_phase=1,hot_load_phase=0,index_phase=0,access_phase=0,stats=0
./bin/testrunner -i ${ini_file} -c ${test_conf} -p load_phase=1,hot_load_phase=1,index_phase=0,access_phase=0,stats=0
./bin/testrunner -i ${ini_file} -c ${test_conf} -p load_phase=0,hot_load_phase=0,index_phase=1,access_phase=0,stats=0
./bin/testrunner -i ${ini_file} -c ${test_conf} -p load_phase=0,hot_load_phase=0,index_phase=0,access_phase=1,stats=1
