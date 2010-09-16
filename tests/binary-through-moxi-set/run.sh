#!/bin/sh
#
# Variables passed:
#    SERVER
#    SERVERFILE
#    CONFIGFILE
#    TESTNAME
#    KEYFILE

RETVAL=0

if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		echo "[$TESTNAME] Running set against $entry"
		bin/binclient.py $entry set a 1
		if [ $? -eq 1 ]; then
			RETVAL=1
		fi	
	done
else
	echo "[$TESTNAME] Running set against $SERVER"
	bin/binclient.py $SERVER set a 1
	if [ $? -eq 1 ]; then
		RETVAL=1
	fi	
fi

exit $RETVAL
