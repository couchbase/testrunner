#!/bin/sh

if [ -z "$1" ]; then
	echo "Syntax: $0 <hostname[:port]>"
	exit 1
fi

while true; do
	./validate_ascii.pl -s -c 10 $1
	./validate_ascii.pl -g -c 10 $1
done
