#!/usr/bin/perl -w

use strict;
use Getopt::Std;
use IO::Socket;

my %opts;
my $server;
my $port = 11211;

sub usage {
	print "Usage: $0 [options] <server[:port]>\n\n";
	print "Options:\n";
	print "  -g       Get data\n";
	print "  -s       Set data\n";
	print "  -c <N>   Number of key/value pairs [required]\n";
}

sub get_data {
	my $sock = new IO::Socket::INET(
			PeerAddr => $server,
			PeerPort => $port,
			Proto    => 'tcp'
	);

	unless ($sock) {
		print "Could not create socket!\n";
		exit 1;
	}

	my $x = 0;
	my $error = 0;

	while ($x < $opts{'c'}) {
		print $sock "get $x\r\n";

		my $input = <$sock>;
		chomp $input;

		unless ($input =~ /^VALUE $x 0 26/) {
			print "$x: $input\n";
			$error = 1;
			$x++;
			next;
		}

		$input = <$sock>;
		chomp ($input);

		unless ($input =~ /^abcdefghijklmnopqrstuvwxyz/) {
			print "$x: $input\n";
			$error = 1;
			$x++;
			next;
		}

		$input = <$sock>;
		$x++;
	}

	if ($error) {
		print "FAIL\n";
	} else {
		print "PASS\n";
	}

	exit $error;
}

sub set_data {
	my $sock = new IO::Socket::INET(
			PeerAddr => $server,
			PeerPort => $port,
			Proto    => 'tcp'
	);

	unless ($sock) {
		print "Could not create socket!\n";
		exit 1;
	}

	my $x = 0;
	my $error = 0;

	while ($x < $opts{'c'}) {
		print $sock "set $x 0 0 26\r\nabcdefghijklmnopqrstuvwxyz\r\n";

		my $input = <$sock>;
		chomp $input;

		if ($input !~ /^STORED/) {
			$error = 1;
			print "\n$x: $input\n";
		}

		$x++;
	}

	if ($error) {
		print "FAIL\n";
	} else {
		print "PASS\n";
	}

	exit $error;
}

getopts('gsc:', \%opts);

my $serverport = shift;

unless ($serverport) {
	usage;
	exit 1;
}

if ($serverport =~ /([\d\.]+):(\d+)/) {
	$server = $1;
	$port = $2;
} else {
	$server = $serverport;
}

unless ($opts{'c'}) {
	usage;
	exit 1;
}

if ($opts{'g'}) {
	get_data;
} elsif ($opts{'s'}) {
	set_data;
}

