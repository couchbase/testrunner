#!/usr/bin/perl -w

use strict;
use Getopt::Std;

my %opts;
my ($os, $arch, $ext);
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
	$ext = "rpm";
} 

unless ($os) {
	$output = `ssh -i $sshkey root\@$opts{'s'} "cat /etc/lsb-release 2>/dev/null| grep DISTRIB_DESCRIPTION" 2> /dev/null`;
	chomp $output;
	
	if ($output =~ /Ubuntu 10.04 LTS/) {
		$os = "ubuntu_10.04";
		$ext = "deb";
	}
}



$output = `ssh -i $sshkey root\@$opts{'s'} "uname -m" 2> /dev/null`;
chomp $output;

# 64 bit is always specified as x86_64, but 32 bit can be specified as x86 or i686.
if ($output =~ /^x86_64$/) {
	$arch = "x86_64";
} elsif ($output =~ /^x86$/ || $output =~ /^i686$/) {
	if ($os =~ /rhel_5.4/) {
		$arch = "x86";
	} elsif ($os =~ /ubuntu_10.04/ || $os =~ /ubuntu_9.04/ || $os =~ /ubunty_9.10/) {
		$arch = "x86";
	}
} else {
	print "Unknown OS/arch combo.\n";
}


my $file;
if ($version eq "latest") {
    $file = "membase-server_".$arch.".".$ext;
} else {
    $file = "membase-server_".$arch."_".$version.".".$ext;
}

my $md5sum = `curl -s http://builds.hq.northscale.net/latestbuilds/$file.md5`;
chomp $md5sum;
$md5sum =~ s/ .*$//;

# first, remove any old installs and misc directories
if ($os =~ /rhel_5.4/) {
	`ssh -i $sshkey root\@$opts{'s'} "rpm -e membase-server; rm -rf /var/opt/membase /opt/membase /etc/opt/membase; cd /tmp;" 2>&1 >/dev/null`;
} elsif ($os =~ /ubuntu_10.04/ || $os =~ /ubuntu_9.04/ || $os =~ /ubuntu_9.10/) {
	`ssh -i $sshkey root\@$opts{'s'} "dpkg -r membase-server; rm -rf /var/opt/membas /opt/membase /etc/opt/membase; cd /tmp;" 2>&1 >/dev/null`;
}

# now, get the md5sum of a file if it exists
my $r_md5sum = `ssh -i $sshkey root\@$opts{'s'} "md5sum /tmp/$file 2>/dev/null"`;
chomp $r_md5sum;
$r_md5sum =~ s/ .*$//;

my $command = "cd /tmp;" ;

if ($md5sum ne $r_md5sum) {
        print "[install] Fetching http://builds.hq.northscale.net/latestbuilds/$file\n";
	$command .= " rm -f $file ; wget -q http://builds.hq.northscale.net/latestbuilds/$file &&";
}

if ($os =~ /rhel_5.4/) {
	$command .= " rpm -i $file";
} elsif ($os =~ /ubuntu_10.04/ || $os =~ /ubuntu_9.04/ || $os =~ /ubuntu_9.10/) {
	$command .= " dpkg -i $file";
}
`ssh -i $sshkey root\@$opts{'s'} "$command" 2>&1 >/dev/null`;
`curl -d "port=SAME&initStatus=done&username=Administrator&password=password" "$opts{'s'}:8080/settings/web" &> /dev/null`;
