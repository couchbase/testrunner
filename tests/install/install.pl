#!/usr/bin/perl -w

use strict;
use Getopt::Std;

my %opts;
my ($os, $arch);
getopts('s:', \%opts);

my $sshkey = $ENV{'KEYFILE'};
my $version = $ENV{'VERSION'};

# figure out what OS we're using
my $output = `ssh -i $sshkey root\@$opts{'s'} "cat /etc/redhat-release 2>/dev/null"`;
chomp($output);

# It'd be prettier if we just ||'ed this, but we might someday have
# different packages for 5.2 and 5.4 so I'm keeping them separate
# for now.
if ($output =~ /^CentOS release 5\.[24] \(Final\)/) {
	$os = "rhel_5.4";
} 

unless ($os) {
	$output = `ssh -i $sshkey root\@$opts{'s'} "cat /etc/lsb-release 2>/dev/null| grep DISTRIB_DESCRIPTION"`;
	chomp $output;
	
	if ($output =~ /Ubuntu 10.04 LTS/) {
		$os = "ubuntu_10.04";
	}
}



$output = `ssh -i $sshkey root\@$opts{'s'} "uname -m"`;
chomp $output;

# 64 bit is always specified as x86_64, but 32 bit can be specified as x86 or i686.
if ($output =~ /^x86_64$/) {
	$arch = "x86_64";
} elsif ($output =~ /^x86$/ || $output =~ /^i686$/) {
	if ($os =~ /rhel_5.4/) {
		$arch = "x86";
	} elsif ($os =~ /ubuntu_10.04/ || $os =~ /ubuntu_9.04/) {
		$arch = "i686";
	}
} else {
	print "Unknown OS/arch combo.\n";
}

my $file = "northscale-server_".$os."_".$arch."_".$version.".tar.gz";
my $md5sum = `curl -s http://builds.hq.northscale.net/latestbuilds/$file.md5`;
chomp $md5sum;
$md5sum =~ s/ .*$//;

# first, remove any old installs and misc directories
if ($os =~ /rhel_5.4/) {
	`ssh -i $sshkey root\@$opts{'s'} "rpm -e northscale-server; rm -rf /var/opt /opt /etc/opt; cd /tmp;" 2>&1 >/dev/null`;
} elsif ($os =~ /ubuntu_10.04/ || $os =~ /ubuntu_9.04/) {
	`ssh -i $sshkey root\@$opts{'s'} "dpkg -r northscale-server; rm -rf /var/opt /opt /etc/opt; cd /tmp;" 2>&1 >/dev/null`;
}

# now, get the md5sum of a file if it exists
my $r_md5sum = `ssh -i $sshkey root\@$opts{'s'} "md5sum /tmp/$file 2>/dev/null"`;
chomp $r_md5sum;
$r_md5sum =~ s/ .*$//;

my $command = "cd /tmp; rm -rf northscale-server*.rpm;" ;

if ($md5sum ne $r_md5sum) {
	$command .= " wget -q http://builds.hq.northscale.net/latestbuilds/$file &&";
}

$command .= " tar -zxf $file;";

if ($os =~ /rhel_5.4/) {
	$command .= " rpm -i northscale-server*.rpm";
} elsif ($os =~ /ubuntu_10.04/ || $os =~ /ubuntu_9.04/) {
	$command .= " dpkg -i northscale-server*.deb";
}
`ssh -i $sshkey root\@$opts{'s'} "$command" 2>&1 >/dev/null`;
