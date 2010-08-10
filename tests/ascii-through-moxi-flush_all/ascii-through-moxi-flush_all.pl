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
	print "Unexpected data from set: $input\n";
	exit 1;
} 

print $sock "flush_all\r\n";

$input = <$sock>;
if ($input !~ /^OK/) {
	print "Unexpected data from flush_all: $input (Expected: OK)\n";
	exit 1;
}

print $sock "get a\r\n";
$input = <$sock>;
if ($input !~ /^END/) {
	print "Unexpected data from get: $input (Expected: END)\n";
	exit 1;
}

exit 0;
