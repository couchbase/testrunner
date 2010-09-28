#!/usr/bin/perl -w

use strict;

my $sshkey = $ENV{'KEYFILE'};

my $node = shift;
my $newnode = shift;

# if we're behind some kind of NAT or whatever, the ip address may be different from what we think it is. sigh.
# grab it from the server-info

my $output = `ssh -i $sshkey root\@$node "/opt/NorthScale/bin/cli/membase server-info -c $newnode:8091"`;
chomp $output;

if ($output =~ /\"hostname\": \"([\d\.]+):/) {
	$newnode = $1;
} else {
	print "Error: Couldn't get hostname from server-info.\n";
	print "$output\n";
	exit 1;
}

$output = `ssh -i $sshkey root\@$node "/opt/NorthScale/bin/cli/membase failover -c $node:8091 --server-failover=$newnode"`;

if ($output !~ /^SUCCESS: failover ns_1\@$newnode/) {
	print "Error: $output\n";
	exit 1;
}

print "Rebalancing.\n";
$output = `ssh -i $sshkey root\@$node "/opt/NorthScale/bin/cli/membase rebalance -c $node:8091"`;
chomp $output;

if ($output !~ /SUCCESS: rebalanced cluster/) {
	print "Error: Couldn't rebalance.\n";
	print "$output\n";
	exit 1;
}

exit 0;
