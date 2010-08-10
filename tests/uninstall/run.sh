#!/bin/sh
#
# Variables passed:
#    SERVER
#    SERVERFILE
#    CONFIGFILE
#    TESTNAME
#    VERSION

export SSHKEY="/Users/randi/.ssh/ustest20090719.pem"
# export SSHKEY="~/.ssh/ustest20090719.pem"

RETVAL=0

if [ -z "$SERVER" ]; then
	for entry in `cat $SERVERFILE`; do
		echo "[$TESTNAME] Uninstalling $entry"
		ssh -i $SSHKEY root@$entry "rpm -e northscale-server" 2>/dev/null >/dev/null
		OUTPUT=`ssh -i ${SSHKEY} root@$entry "find /etc/opt/NorthScale /opt/NorthScale /var/opt/NorthScale -type f 2>/dev/null" | grep -v default | grep -v rpmsave | grep -v ns_1` 

		if [ -n "$OUTPUT" ]; then
			echo "[$TESTNAME] $OUTPUT"
			RETVAL=1
		fi
	done
	SERVER=""
else
	echo "[$TESTNAME] Uninstalling $entry"
	ssh -i $SSHKEY root@$SERVER "rpm -e northscale-server" 2>/dev/null >/dev/null
	OUTPUT=`ssh -i $SSHKEY root@$SERVER "find /etc/opt/NorthScale /opt/NorthScale /var/opt/NorthScale -type f 2>/dev/null| grep -v default | grep -v rpmsave| grep -v ns_1 "`

	if [ -n "$OUTPUT" ]; then
		echo "[$TESTNAME] $OUTPUT"
		RETVAL=1
	fi
fi

exit $RETVAL
