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
		tests/$TESTNAME/ascii-through-moxi-gets.pl -s $entry
		if [ $? -eq 1 ]; then
			RETVAL=1
		fi	
	done
else
	tests/$TESTNAME/ascii-through-moxi-gets.pl -s $SERVER
	if [ $? -eq 1 ]; then
		RETVAL=1
	fi	
fi

exit $RETVAL
