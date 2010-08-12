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
		echo "[$TESTNAME] Running get against $entry"
		# do the set first so we know we have valid data to get.
		lib/binclient.py $entry set a 1
		OUTPUT=`lib/binclient.py $entry get a`
		if [ $? -eq 1 ] || [ "$OUTPUT" -ne "1" ]; then
			echo "[$TESTNAME] got unexpected output: $OUTPUT (expected: 1)"
			RETVAL=1
		fi	
	done
else
	echo "[$TESTNAME] Running get against $SERVER"
	# do the set first so we know we have valid data to get.
	lib/binclient.py $SERVER set a 1
	OUTPUT=`lib/binclient.py $SERVER get a`
	if [ $? -eq 1 ] || [ "$OUTPUT" -ne "1" ]; then
		echo "[$TESTNAME] got unexpected output: $OUTPUT (expected: 1)"
		RETVAL=1
	fi	
fi

exit $RETVAL
