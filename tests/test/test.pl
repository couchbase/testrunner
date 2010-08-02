#!/usr/bin/perl -w

use strict;
use Getopt::Std;

my %opts;
getopts('s:', \%opts);

print "Sample test server list start:\n";

for my $server (split /,/, $opts{'s'}) {
	print "$server\n";
}

print "Sample test server list end.\n";
