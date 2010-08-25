#!/usr/bin/perl -w

use strict;

use IO::Socket;

my $server_addr = "ec2-184-73-97-235.compute-1.amazonaws.com";

my $file = "memcached_data.txt";
#my $file = "keyset_10.out";

my $sock = new IO::Socket::INET ( 
                                 PeerAddr => $server_addr,
                                 PeerPort => '11211',
                                 Proto    => 'tcp',
                                );

die "Could not create socket: $!\n" unless $sock;

open FILE, "<$file" or die "Couldn't open $file: $!\n";

foreach my $keyval (<FILE>) {
	my ($key, $value) = split /\s/, $keyval;
	my $len = length($value);
	print $sock "set $key 0 0 $len\r\n$value\r\n";
	my $input = <$sock>;
	chomp $input;
	if ($input !~ /^STORED/) {
		print "$key: $input\n";
		exit;
	}
}

foreach my $keyval (<FILE>) {
	my ($key, $value) = split /\s/, $keyval;
	my $len = length($value);
	print $sock "get $key\r\n";
	my $input = <$sock>;
	chomp $input;
	if ($input !~ /^VALUE $key 0 $value/) {
		print "$key: $input\n";
		exit;
	}
	$input = <$sock>;
	chomp $input;
	if ($input !~ /^$value/) {
		print "$key: $input\n";
		exit;
	}
	$input = <$sock>;
}

close FILE;
