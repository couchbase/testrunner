#!/bin/sh

COUNT=10
ITERATIONS=0 # 0 is unlimited
X=0

while getopts "i:c:s:" flag; do
        case "$flag" in
                c) COUNT=$OPTARG;;
                i) ITERATIONS=$OPTARG;;
		s) SERVER=$OPTARG;;
        esac
done

set -- $args

if [ -z "$SERVER" ]; then
	echo "Syntax: $0 [-c <count>] [-i <iterations> ] -s <hostname[:port]>"
	exit 1
fi

while true; do
	X=$(($X+1))

	echo $X: `./validate_ascii.pl -s -c $COUNT $SERVER`
	if [ $? -ne 0 ]; then
		exit 1
	fi

	echo $X: `./validate_ascii.pl -g -c $COUNT $SERVER`
	if [ $? -ne 0 ]; then
		exit 1
	fi

	if [ $ITERATIONS -ne 0 ]; then
		if [ $X -eq $ITERATIONS ]; then
			exit 0
		fi
	fi		
done
