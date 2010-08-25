#!/usr/bin/perl -w

use strict;

my $keyin = "keyset_10";
my $keyout = "keyset_10.out";

open FILE, "<$keyin" or die "Couldn't open $keyin: $!\n";
open FILEOUT, ">$keyout" or die "Couldn't open $keyout: $!\n"; 

foreach my $key (<FILE>) {
	chomp $key;
	print FILEOUT "$key abcdefghijklmnopqrstuvwxyz_$key\n";
}

close FILE;
close FILEOUT;
