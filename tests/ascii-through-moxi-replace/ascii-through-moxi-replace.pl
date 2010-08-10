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

# delete the value in case it exists
print $sock "delete a\r\n";
my $input = <$sock>;

print $sock "delete b\r\n";
$input = <$sock>;

print $sock "set a 0 0 1\r\nb\r\n";
$input = <$sock>;
if ($input !~ /^STORED/) {
	print "Unexpected data from set: $input (expected: STORED)\n";
	exit 1;
} 

print $sock "replace a 0 0 1\r\nc\r\n";
$input = <$sock>;
if ($input !~ /^STORED/) {
	print "Unexpected data from replace: $input (expected: STORED)\n";
	exit 1;
}

print $sock "replace b 0 0 1\r\nd\r\n";
$input = <$sock>;
if ($input !~ /^NOT_STORED/) {
	print "Unexpected data from replace: $input (expected: NOT_STORED)\n";
	exit 1;
}

exit 0;
