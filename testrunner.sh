#!/bin/sh
# 
# Syntax:
# -c <file>   - path to config file
# -f <file>   - path to file containing a list of servers
# -q          - quiet mode, suppress output
# -s <server> - single server, can be used instead of -f
# -t <test>   - run test

export SERVER SERVERFILE TESTNAME CONFIGFILE 

function usage {
	echo "Syntax: testrunner.sh [options]"
	echo
	echo "Options:"
	echo " -c <file>        Config file name"
	echo " -f <file>        Path to file containing server list"
	echo " -s <server>      Hostname of server"
	echo " -t <test>        Test name"
	echo
}

while getopts "c:f:hqs:t:" flag; do
	case "$flag" in
		c) CONFIGFILE=$OPTARG;;
		f) SERVERFILE=$OPTARG;;
		h) usage; exit;;
		s) SERVER=$OPTARG;;
		t) TESTNAME=$OPTARG;;
	esac
done

if [ -z "$SERVERFILE" ] && [ -z "$SERVER" ]; then
	echo "Server(s) must be specified with -f or -s."
	usage;
	exit 255;
fi

if [ -z "$TESTNAME" ] && [ -z "$CONFIGFILE" ]; then
	echo "Test(s) must be specified with -c or -t."
	usage;
	exit 255;
fi

if [ -n "$CONFIGFILE" ] && [ -n "$TESTNAME" ]; then
	echo "Error: Only specify -c or -t, not both."
	usage;
	exit 255;
fi

if [ -n "$SERVERFILE" ] && [ -s "$SERVER" ]; then
	echo "Error: Only specify -f or -s, not both."
	usage;
	exit 255;
fi

# figure out our log file name
LOGFILE=logs/`date "+%Y%m%d%H%M%S"`.log

touch $LOGFILE

if [ "$?" -eq 1 ]; then
	echo "Couldn't create file $LOGFILE."
	exit 255
fi

if [ -n "$TESTNAME" ]; then
	echo "[$TESTNAME] start" >> $LOGFILE

	if [ ! -x "tests/$TESTNAME/run.sh" ]; then
		echo "[ERROR] $TESTNAME: not found" >> $LOGFILE 
		exit
	fi

	tests/$TESTNAME/run.sh >> $LOGFILE

	if [ "$?" -eq 1 ]; then
		echo "[$TESTNAME] FAIL" >> $LOGFILE
	else
		echo "[$TESTNAME] PASS" >> $LOGFILE
	fi

	exit
fi

# if a config file is specified, do each test one by one, the same way as above.

for TESTNAME in `cat conf/$CONFIGFILE`; do
	echo "[$TESTNAME] start" >> $LOGFILE

	if [ ! -x "tests/$TESTNAME/run.sh" ]; then
		echo "[ERROR] $TESTNAME: not found" >> $LOGFILE	
	else
		tests/$TESTNAME/run.sh >> $LOGFILE

		if [ "$?" -eq 1 ]; then
			echo "[$TESTNAME] FAIL" >> $LOGFILE
		else
			echo "[$TESTNAME] PASS" >> $LOGFILE
		fi
	fi
done
