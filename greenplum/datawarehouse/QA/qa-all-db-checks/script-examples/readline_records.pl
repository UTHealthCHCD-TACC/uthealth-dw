#!/bin/perl
my @columns2;

$num_args = $#ARGV +1;

if ($num_args <1)
{
	print "\nUsage: perl readline_records.pl R:\\TDCJ\\March24-22_Gwen\\MDR0095_FY2016_UTMB(gbaillar@utmb.edu).csv\n";
	exit;
}

$infile = $ARGV[0];

open INF1, "<", "$infile" or die $!;
$nline=0;
$n_spec=0;
while($line=<INF1>)
{
	$nline=$nline+1;

	$s1=substr($line,0,11); #Report Date;

	if ($s1 ne "Report Date")
	{
		$n_spec=$n_spec+1 ;
		chop($line);
		print "[$nline] $line\n" if $n_spec<0;
	}
}
close(INF1);

print "\nTotal lines in file= $nline\n";
print "\nTotal bad lines    = $n_spec\n";


