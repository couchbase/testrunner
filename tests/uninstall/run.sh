#!/bin/sh
#
# Variables passed:
#    SERVER
#    SERVERFILE
#    CONFIGFILE
#    TESTNAME
#    VERSION
#    KEYFILE


RETVAL=0

if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		echo "[$TESTNAME] Uninstalling $entry"
		ssh -i $KEYFILE root@$entry "rpm -e membase-server || dpkg -r membase-server" &>/dev/null
                # right now we don't remove /var/opt diretories
		OUTPUT=`ssh -i $KEYFILE root@$entry "find /etc/opt/membase /opt/membase -type f 2>/dev/null" 2> /dev/null | grep -v -e default -e rpmsave -e ns_1 -e pyc -e erl_crash.dump -e ip`

		if [ -n "$OUTPUT" ]; then
			echo "[$TESTNAME] $OUTPUT"
			RETVAL=1
		fi
	done
	SERVER=""
else
	echo "[$TESTNAME] Uninstalling $entry"
	ssh -i $KEYFILE root@$SERVER "rpm -e membase-server || dpkg -r membase-server" &>/dev/null
        # right now we don't remove /var/opt diretories
	OUTPUT=`ssh -i $KEYFILE root@$SERVER "find /etc/opt/membase /opt/membase -type f 2>/dev/null" 2> /dev/null | grep -v -e default -e rpmsave -e ns_1 -e pyc -e erl_crash.dump -e ip`

	if [ -n "$OUTPUT" ]; then
		echo "[$TESTNAME] $OUTPUT"
		RETVAL=1
	fi
fi

sleep 5

exit $RETVAL
