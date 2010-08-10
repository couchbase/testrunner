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

my $casid;
# do the gets - need the cas id
print $sock "gets a\r\n";
$input = <$sock>;
if ($input =~ /^VALUE a 0 1 (\d)/) {
	$casid = $1;
} else {
	print "Unexpected data from gets: $input (expected: VALUE a 0 1 $casid)\n";
	exit 1;
} 

$input = <$sock>;
if ($input !~ /^b/) {
	print "Unexpected data from gets: $input (expected: b)\n";
	exit 1;
} 

$input = <$sock>;
if ($input !~ /^END/) {
	print "Unexpected data from gets: $input (expected: END)\n";
	exit 1;
} 

# now we do the cas update
print $sock "cas a 0 0 1 $casid\r\nc\r\n";
$input = <$sock>;
if ($input !~ /^STORED/) {
	print "Unexpected data from cas: $input (expected: STORED)\n";
	exit 1;
} 

# update the value with a set
print $sock "set a 0 0 1\r\nd\r\n";
$input = <$sock>;
if ($input !~ /^STORED/) {
	print "Unexpected data from set: $input (expected: STORED)\n";
	exit 1;
} 

# now try to update with cas without fetching the value first
print $sock "cas a 0 0 1 $casid\r\ne\r\n";
$input = <$sock>;
if ($input !~ /^EXISTS/) {
	print "Unexpected data from cas: $input (expected: EXISTS)\n";
	exit 1;
} 

exit 0;
