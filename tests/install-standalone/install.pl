#!/usr/bin/perl -w

use strict;
use Getopt::Std;

my %opts;
my ($os, $arch, $ext);
getopts('s:', \%opts);

my $sshkey = $ENV{'KEYFILE'};
my $version = $ENV{'VERSION'};

# figure out what OS we're using
my $output = `ssh -i $sshkey root\@$opts{'s'} "cat /etc/redhat-release 2>/dev/null" 2>/dev/null`;
chomp($output);

# It'd be prettier if we just ||'ed this, but we might someday have
# different packages for 5.2 and 5.4 so I'm keeping them separate
# for now.
if ($output =~ /^CentOS release 5\.[24]/) {
	$os = "rhel_5.4";
	$ext = "rpm";
}

if ($output =~ /^Red Hat Enterprise Linux Server release 5\.[24]/) {
	$os = "rhel_5.4";
	$ext = "rpm";
} 

unless ($os) {
	$output = `ssh -i $sshkey root\@$opts{'s'} "cat /etc/lsb-release 2>/dev/null| grep DISTRIB_DESCRIPTION" 2> /dev/null`;
	chomp $output;
	
	if ($output =~ /Ubuntu 10.04/) {
		$os = "ubuntu_10.04";
		$ext = "deb";
	}

	if ($output =~ /Ubuntu 9.10/) {
		$os = "ubuntu_9.10";
		$ext = "deb";
	}

	if ($output =~ /Ubuntu 9.04/) {
		$os = "ubuntu_9.04";
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
	} elsif ($os =~ /ubuntu_10.04/ || $os =~ /ubuntu_9.04/ || $os =~ /ubuntu_9.10/) {
		$arch = "x86";
	} else {
	    print "$opts{'s'}: Unknown OS/arch combo.\n";
	}
} else {
	print "$opts{'s'}: Unknown OS/arch combo.\n";
}


my $file = "membase-server-enterprise_".$arch."_".$version.".".$ext;

my $md5sum = `curl -s http://builds.hq.northscale.net/latestbuilds/$file.md5`;
chomp $md5sum;
$md5sum =~ s/ .*$//;

# first, remove any old installs and misc directories
if ($os =~ /rhel_5.4/) {
	`ssh -i $sshkey root\@$opts{'s'} "rpm -e membase-server ; killall beam ; killall -9 memcached ; killall -9 vbucketmigrator; rm -rf /var/opt/membase /opt/membase /etc/opt/membase; cd /tmp;" 2>/dev/null`;
} elsif ($os =~ /ubuntu_10.04/ || $os =~ /ubuntu_9.04/ || $os =~ /ubuntu_9.10/) {
	`ssh -i $sshkey root\@$opts{'s'} "dpkg -r membase-server ; killall beam ; killall -9 memcached ; killall -9 vbucketmigrator; rm -rf /var/opt/membase /opt/membase /etc/opt/membase; cd /tmp;" 2>/dev/null`;
}

my $command = "cd /tmp;" ;

# now, get the md5sum of a file if it exists
my $r_md5sum = `ssh -i $sshkey root\@$opts{'s'} "md5sum /tmp/$file 2>/dev/null" 2>/devnull`;
chomp $r_md5sum;
$r_md5sum =~ s/ .*$//;

if ($md5sum ne $r_md5sum) {
        print "[install] Fetching http://builds.hq.northscale.net/latestbuilds/$file to $opts{'s'}\n";
	$command .= " rm -f $file ; wget -q http://builds.hq.northscale.net/latestbuilds/$file &&";
}

if ($os =~ /rhel_5.4/) {
	$command .= " rpm -i $file --noscripts;";
        $command .= "cd /opt ; tar cvf membase.tar membase ; rpm -e membase-server;";
} elsif ($os =~ /ubuntu_10.04/ || $os =~ /ubuntu_9.04/ || $os =~ /ubuntu_9.10/) {
	$command .= " dpkg -i $file --unpack;";
        $command .= "cd /opt ; tar cvf membase.tar membase ; dpkg -r membase-server;";
}

$command .= "tar xvf membase.tar ; rm -rf /var/opt/membase ; rm -rf /etc/opt/membase ; rm -f /opt/membase.tar ;";

$command .= 'nsdir=\\$(ls -d /opt/membase/1.6*) ; \
rm -rf /tmp/membase_data ; \
mkdir -p /tmp/membase_data ; \
chmod 777 /tmp/membase_data ; \
echo \"PRAGMA journal_mode = TRUNCATE; \
PRAGMA synchronous = NORMAL;\" > /tmp/init.sql; \
max_size=\\$(free -b | awk \'/Mem/{print \$4 \"/1.42\"}\' | bc) ;\
\$nsdir/bin/memcached/memcached -p 11211 -E \$nsdir/bin/ep_engine/ep.so -r -e \"ht_size=12289;ht_locks=23;dbname=/ebs/membase/default;min_data_age=0;queue_age_cap=900;max_size=\${max_size};vb0=true;initfile=/tmp/init.sql\" -u membase </dev/null &>/tmp/memcached.log &';

system("ssh -i $sshkey root\@$opts{'s'} \"$command\" &> /dev/null");
