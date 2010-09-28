#!/usr/bin/perl -w

use strict;

my $sshkey = $ENV{'KEYFILE'};

my $node = shift;
my $newnode = shift;

my $output = `ssh -i $sshkey root\@$node "/opt/NorthScale/bin/cli/membase server-add -c 127.0.0.1:8091 -u Administrator -p password --server-add=$newnode --server-add-username=Administrator --server-add-password=password"`;
chomp $output;

if ($output !~ /^SUCCESS: server-add $newnode:8091/) {
	print "Error: $output\n";
	exit 1;
}

exit 0;
