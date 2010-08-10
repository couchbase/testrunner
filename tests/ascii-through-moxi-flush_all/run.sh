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
		echo "[$TESTNAME] Running ascii-through-moxi-flush_all.pl -s $entry"
		tests/$TESTNAME/ascii-through-moxi-flush_all.pl -s $entry
		if [ $? -eq 1 ]; then
			RETVAL=1
		fi	
	done
else
	echo "[$TESTNAME] Running ascii-through-moxi-flush_all.pl -s $SERVER"
	tests/$TESTNAME/ascii-through-moxi-flush_all.pl -s $SERVER
	if [ $? -eq 1 ]; then
		RETVAL=1
	fi	
fi

exit $RETVAL
