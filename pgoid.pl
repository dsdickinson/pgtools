#!/usr/bin/env perl
#############################################################################
# PROGRAM
#     pgoid.pl
#
# SYNOPSIS
#     This program looks up the oid for a PostgreSQL database or table.
#     Typically need to run using sudo.
#
# USAGE
#     sudo pgoid.pl [OPTIONS]
#
#     Options:
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
my $default_db	= "template1";			# default database used for connection 
##############################################################################

use DBI;
use Time::localtime;
use Data::Dump qw(dump);
use Getopt::Std;
$Getopt::Std::STANDARD_HELP_VERSION = 1;

my $VERSION = "0.1";

my (%opts, $prog, $data_source, $arg_database, $arg_table, $query, $oid, 
	$name, $results, $dbh);
$prog = $0;

$pgdata_path =~ /^\/pg(\d+)\//;
my $pg_version = $1;

sub VERSION_MESSAGE {
	print $prog . " v." . $VERSION;
	exit(0);
}

sub HELP_MESSAGE {
my $usage = <<END;
NAME
pgoid - Get PostgreSQL database or table oid.

SYNOPSIS
     sudo pgoid [-d ] database_name
     sudo pgoid [-d ] database_name [-t] table_name

DESCRIPTION
    This program looks up the oid of a PostgreSQL database or table.
    Typically it needs to be run in sudo mode.

    The following options are available:
    -d [database]       Database name. (required option)

    -h                  Usage

    -t [table]          Table name.


EXAMPLES
> sudo $prog -d college 
1785034

> sudo $prog -d college -t students
1775990

END

	print $usage;
	exit(0);
}

if (! -d $pgdata_path) {
	print "ERROR: \$PGDATA path '$pgdata_path' does not exist!\n";
	print "       Perhaps you should try running this using sudo.\n";
	exit;
} 

my $opts_status = getopts('d:t:h', \%opts);
if (!$opts_status) {
	print "\n";
	HELP_MESSAGE();
}

if (defined($opts{h})) {
	HELP_MESSAGE();
}

if (defined($opts{d})) {
	$arg_database = $opts{d};
	$arg_database = sanitize($arg_database);
}

if (defined($opts{t})) {
	$arg_table = $opts{t};
	$arg_table = sanitize($arg_table);
}

if (!defined($opts{d}) && !defined($opts{t})) {
	print "\nError: a database name or table name is required.\n\n";
	HELP_MESSAGE();
}

if (!defined($opts{d}) && defined($opts{t})) {
	print "\nError: a database name must accompany a table name.\n\n";
	HELP_MESSAGE();
}

format LINE = 
@<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< @*
$name, $results
.

if (defined($opts{d})) {
	#-rw-------  1 pgsql  pgsql  4 Dec 14 05:00 /pg90/data/base/753229/PG_VERSION
	$name 			= $arg_database;
	$data_source  	= "$driver:dbname=$arg_database";
	$dbh 			= DBI->connect($data_source, $username, $password) or die $DBI::errstr;
	$query			= "SELECT oid FROM pg_database WHERE datname = ?;";
	$oid = get_oid($dbh, $query, $arg_database);
	$dbh->disconnect;
}

if (defined($opts{t})) {
	#-rw-------  1 pgsql  pgsql  25616384 Dec 15 03:39 /pg90/data/base/736895/738844
	$name 			= $arg_database . "." . $arg_table;
	$data_source  	= "$driver:dbname=$arg_database";
	$dbh 			= DBI->connect($data_source, $username, $password) or die $DBI::errstr;
	$query			= "SELECT oid FROM pg_class WHERE relname = ? AND relkind = 'r';";
	$oid = get_oid($dbh, $query, $arg_table);
	$dbh->disconnect;
}

if (!defined($oid)) {
	print "ERROR: OID for $name could not be found!\n";
} else {
	print "$oid\n";
}

exit(0);

sub sanitize {
	my ($input) = @_;

	$input =~s /\s+//g;
	$input =~s /;//g;
	$input =~s /[^\w]+//g;

	return $input;
}

sub get_oid {
	my ($dbh, $this_query, $this_arg) = @_;
	my ($sth, $query_results, $oid);

	$sth 			= $dbh->prepare($this_query);
	$sth->execute($this_arg);
	$query_results 	= $sth->fetchrow_arrayref;
	$sth->finish;

	if ($query_results->[0] && $query_results->[0] =~ /^(\d+)$/) {
		$oid = $1;
	} else {
		$oid = undef;
	}

	return $oid;
}

sub clean_up {
    exit(0);
}
