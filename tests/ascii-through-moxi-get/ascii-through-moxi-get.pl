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
print $sock "set a 0 0 1\r\nb\r\n";
$input = <$sock>;

print $sock "get a\r\n";
$input = <$sock>;
if ($input !~ /^VALUE a 0 1/) {
	print "Unexpected data from set: $input\n";
	exit 1;
} 

$input = <$sock>;
if ($input !~ /^b/) {
	print "Unexpected data from set: $input\n";
	exit 1;
} 

$input = <$sock>;
if ($input !~ /^END/) {
	print "Unexpected data from set: $input\n";
	exit 1;
} 
	

exit 0;
