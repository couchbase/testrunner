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

# delete the key just in case.
print $sock "delete a\r\n";
my $input = <$sock>;

# do the set - don't bother checking the output
print $sock "set a 0 0 1\r\n1\r\n";
$input = <$sock>;

# incr
print $sock "incr a 1\r\n";
$input = <$sock>;
if ($input !~ /^2/) {
	print "Unexpected data from incr: $input (expected 2)\n";
	exit 1;
}

exit 0;
