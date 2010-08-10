#!/usr/bin/perl -w

use strict;
use Getopt::Std;


my %opts;

getopts('s:', \%opts);
my $server = $opts{'s'};

my $output = `lib/binclient.py $opts{'s'} set a 1`;

if ($output) {
	print "Unexpected data from set: $output";
	exit 1;
}

exit 0;
