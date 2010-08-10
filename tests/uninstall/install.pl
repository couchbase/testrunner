#!/usr/bin/perl -w

use strict;
use Getopt::Std;

my %opts;
my ($os, $arch);
getopts('s:', \%opts);

my $sshkey = $ENV{'SSHKEY'};
my $version = $ENV{'VERSION'};

# figure out what OS we're using
my @output = `ssh -i $sshkey root\@$opts{'s'} "cat /etc/redhat-release ; uname -m"`;

foreach my $line (@output) {
	chomp($line);

	# It'd be prettier if we just ||'ed this, but we might someday have
	# different packages for 5.2 and 5.4 so I'm keeping them separate
	# for now.
	if ($line =~ /^CentOS release 5.2 \(Final\)$/) {
		$os = "rhel_5.4";
	} elsif ($line =~ /^CentOS release 5.4 \(Final\)$/) {
		$os = "rhel_5.4";
	} 

	if ($line =~ /^x86_64$/) {
		$arch = "x86_64";
	} elsif ($line =~ /^x86$/) {
		$arch = "x86";
	} 
}

my $file = "northscale-server_".$os."_".$arch."_".$version.".tar.gz";
my $md5sum = `curl -s http://builds.hq.northscale.net/latestbuilds/$file.md5`;
chomp $md5sum;
$md5sum =~ s/ .*$//;

# first, remove any old installs and misc directories
`ssh -i $sshkey root\@$opts{'s'} "rpm -e northscale-server; rm -rf /var/opt /opt /etc/opt; cd /tmp;" 2>&1 >/dev/null`;

# now, get the md5sum of a file if it exists
my $r_md5sum = `ssh -i $sshkey root\@$opts{'s'} "md5sum /tmp/$file"`;
chomp $r_md5sum;
$r_md5sum =~ s/ .*$//;

my $command = "cd /tmp; rm -rf northscale-server*.rpm;" ;

if ($md5sum ne $r_md5sum) {
	print "Fetching package for $opts{'s'}.\n";
	$command .= " wget -q http://builds.hq.northscale.net/latestbuilds/$file &&";
}

$command .= " tar -zxf $file; rpm -i northscale-server*.rpm";

`ssh -i $sshkey root\@$opts{'s'} "$command" 2>&1 >/dev/null`;
