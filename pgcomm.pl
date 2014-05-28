#!/usr/bin/env perl
#############################################################################
# PROGRAM
#     pgcomm.pl
#
# SYNOPSIS
#     This program queries a column's comment field to display the purpose
#     of the column. Typically need to run using sudo.
#
# USAGE
#     sudo pglist.pl [OPTIONS]
#
#     Options:
#         -c [column]         Column name.
#
#         -d [database]       Database name.
#
#         -h                  Usage
#
#         -t [table]          Table name.
#
# MODULES/FILES REQUIRED
#     DBI
#     Time::localtime
#     Data::Dump
#     Getopt::Std
#
# PROGRAMS REQUIRED
#     None
#
# AUTHOR
#     Steve Dickinson
#
# COMMENTS
#     Example alias:
#     alias pgcm='sudo ~/bin/pgcomm.pl'
#############################################################################
# INTERRUPT SIGNALS
#############################################################################
$SIG{'INT' } = 'clean_up'; $SIG{'HUP' } = 'clean_up';
$SIG{'QUIT'} = 'clean_up'; $SIG{'TRAP'} = 'clean_up';
$SIG{'ABRT'} = 'clean_up'; $SIG{'STOP'} = 'clean_up';
$SIG{'TSTP'} = 'clean_up';

use strict;
use warnings;

##############################################################################
# Configure the following variables as necessary for your local system.
# No other configuration is necessary.
# *NOTE: You will more than likely need to run this program in 'sudo' mode
#        to access the secured files it needs to look up.
##############################################################################
my $driver		= "dbi:Pg";				# DBI data_source driver
my $username 	= "pgsql";				# database user
my $password 	= "";					# database user password
my $pgdata_path	= "/pg92/data/base";	# $PGDATA location (pgXX part will change w/ Postgres version)
##############################################################################

use DBI;
use Time::localtime;
use Data::Dump qw(dump);
use Getopt::Std;
$Getopt::Std::STANDARD_HELP_VERSION = 1;

my $VERSION = "0.1";

my (%opts, $data_source, $arg_database, $arg_table, $arg_column, $query, $db_oid, $tb_oid, 
	$oid, $name, $results, $dbh, $sth, $query_results, $col_num, $comment);

my $batch_mode 	= 0;
my $prog 		= $0;

$pgdata_path =~ /^\/pg(\d+)\//;
my $pg_version = $1;

sub VERSION_MESSAGE {
	print $prog . " v." . $VERSION;
	exit(0);
}

sub HELP_MESSAGE {
my $usage = <<END;
NAME
pgcomm - Display PostgreSQL table's column comment field

SYNOPSIS
     sudo pgcomm -d [database_name] -t [table_name] -c [column_name]

DESCRIPTION
    This program looks up the comment field of a PostgreSQL 
    database table. Typically it needs to be run in sudo mode.

    The following options are available:

    -b                  Batch mode. Used to condense the program's output to one line per execution.

    -c [column]         Column name.

    -d [database]       Database name.

    -h                  Usage

    -t [table]          Table name.


EXAMPLES
> sudo $prog -d college -t students -c ssn
Column description:
This column is used to store the student's encrypted social security number.

END

	print $usage;
	exit(0);
}

if (! -d $pgdata_path) {
	print "ERROR: \$PGDATA path '$pgdata_path' does not exist!\n";
	print "       Perhaps you should try running this using sudo.\n";
	exit;
} 

my $opts_status = getopts('c:d:t:bh', \%opts);
if (!$opts_status) {
	print "\n";
	HELP_MESSAGE();
}

if (defined($opts{h})) {
	HELP_MESSAGE();
}

if (defined($opts{b})) {
	$batch_mode = 1;
}

if (defined($opts{c})) {
	$arg_column = $opts{c};
	$arg_column = sanitize($arg_column);
}

if (defined($opts{d})) {
	$arg_database = $opts{d};
	$arg_database = sanitize($arg_database);
}

if (defined($opts{t})) {
	$arg_table = $opts{t};
	$arg_table = sanitize($arg_table);
}

if (!defined($opts{d}) || !defined($opts{t}) || !defined($opts{c})) {
	print "\nError: A database name, a table name and a column name must be given.\n\n";
	HELP_MESSAGE();
}

format LINE = 
@<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< @*
$name, $results
.

#-rw-------  1 pgsql  pgsql  25616384 Dec 15 03:39 /pg90/data/base/736895/738844
$data_source  	= "$driver:dbname=$arg_database";
$dbh 			= DBI->connect($data_source, $username, $password) or die $DBI::errstr;

$oid 			= `sudo ./pgoid.pl -d $arg_database -t $arg_table`;
chomp ($oid);

$query			= "SELECT ordinal_position FROM information_schema.columns WHERE table_name = ? AND column_name = ?;";
$sth 			= $dbh->prepare($query);
$sth->execute($arg_table, $arg_column);
$query_results 	= $sth->fetchrow_arrayref;
$col_num 		= $query_results->[0];
if ($col_num !~ /^\d+$/) {
	print "\nERROR: Could not find column '$arg_column'!\n\n";
	exit(1);
}

$query			= "SELECT col_description(?,?)";
$sth 			= $dbh->prepare($query);
$sth->execute($oid, $col_num);
$query_results 	= $sth->fetchrow_arrayref;
$comment = $query_results->[0];
if (!defined($comment)) {
	$comment = "[undefined]";
}
$sth->finish;

if ($batch_mode == 0) {
	print "\n" . $arg_database . "." . $arg_table . "." . $arg_column . "\n";
	print "\nColumn description:\n";
	print "$comment\n";
	print "\n";
} else {
	print $arg_database . "." . $arg_table . "." . $arg_column . ": " . $comment . "\n";
}

$dbh->disconnect;

exit(0);

sub sanitize {
	my ($input) = @_;

	$input =~s /\s+//g;
	$input =~s /;//g;
	$input =~s /[^\w]+//g;

	return $input;
}

sub get_oid {
	my ($type, $query_results) = @_;
	my $oid;

	if ($query_results && $query_results =~ /^(\d+)$/) {
		$oid = $1;
	} else {
		$~ = "LINE";
		$results =  "ERROR: Could not determine oid for $type $name! Does $name exist?\n";
		write();
		exit;
	}

	return $oid;
}

sub clean_up {
    exit(0);
}
