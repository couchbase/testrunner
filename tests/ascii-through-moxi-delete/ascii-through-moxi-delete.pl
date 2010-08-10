#!/usr/bin/perl -w

use strict;
use Getopt::Std;
use IO::Socket::INET;


my %opts;

getopts('s:', \%opts);
my $server = $opts{'s'};

my $sock = new IO::Socket::INET (
	PeerAddr => $server,
	PeerPort => '11211',
	Proto    => 'tcp'
);

unless ($sock) {
	print "Couldn't connect to $server.\n";
	exit 1;
}

print $sock "set a 0 0 1\r\nb\r\n";

my $input = <$sock>;
chomp $input;

if ($input !~ /^STORED/) {
	print "Unexpected data from set: $input (expected: STORED)\n";
	exit 1;
} 

print $sock "delete a\r\n";
$input = <$sock>;

if ($input !~ /^DELETED) {
	print "Unexpected data from delete: $input (expected: DELETED)\n";
	exit 1;
}

exit 0;
