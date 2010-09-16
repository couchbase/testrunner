#!/bin/sh

export SERVER SERVERFILE TESTNAME CONFIGFILE VERSION KEYFILE PYTHONPATH 

PYTHONPATH="lib"

if [ -z "$KEYFILE" ]; then
	KEYFILE=~/.ssh/ustest20090719.pem
fi

function usage {
	echo "Syntax: testrunner.sh [options]"
	echo
	echo "Options:"
	echo " -c <file>        Config file name"
	echo " -f <file>        Path to file containing server list"
	echo " -s <server>      Hostname of server"
	echo " -t <test>        Test name"
	echo " -v <version>     Version to install (if installing)."
	echo "                  Should resemble \"1.6.0beta3a-19-g81e14cc\""
	echo
}

while getopts "c:f:hqs:t:v:" flag; do
	case "$flag" in
		c) CONFIGFILE=$OPTARG;;
		f) SERVERFILE=$OPTARG;;
		h) usage; exit;;
		s) SERVER=$OPTARG;;
		t) TESTNAME=$OPTARG;;
		v) VERSION=$OPTARG;;
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
	echo "[$TESTNAME] START "`date` >> $LOGFILE

	if [ ! -x "tests/$TESTNAME/run.sh" ]; then
		echo "[ERROR] $TESTNAME: not found" >> $LOGFILE 
		exit
	fi

	tests/$TESTNAME/run.sh >> $LOGFILE

	if [ "$?" -eq 1 ]; then
		echo "[$TESTNAME] FAIL "`date` >> $LOGFILE
	else
		echo "[$TESTNAME] PASS "`date` >> $LOGFILE
	fi

	# I got tired of figuring out wtf log file it was then cating it.
	echo "LOGFILE: $LOGFILE"
	cat $LOGFILE

	exit
fi

# if a config file is specified, do each test one by one, the same way as above.

for TESTNAME in `cat conf/$CONFIGFILE`; do
	echo "[$TESTNAME] START "`date` >> $LOGFILE

	if [ ! -x "tests/$TESTNAME/run.sh" ]; then
		echo "[ERROR] $TESTNAME: not found" >> $LOGFILE	
	else
		tests/$TESTNAME/run.sh >> $LOGFILE

		if [ "$?" -ne 0 ]; then
			echo "[$TESTNAME] FAIL "`date` >> $LOGFILE
		else
			echo "[$TESTNAME] PASS "`date` >> $LOGFILE
		fi
	fi
done

# I got tired of figuring out wtf log file it was then cating it.
echo "LOGFILE: $LOGFILE"
cat $LOGFILE
