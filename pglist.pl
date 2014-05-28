#!/usr/bin/env perl
#############################################################################
# PROGRAM
#     pglist.pl
#
# SYNOPSIS
#     This program looks up the last modification time of a PostgreSQL
#     database and a respective table. Typically need to run using sudo.
#
# USAGE
#     sudo pglist.pl [OPTIONS]
#
#     Options:
#         -a                  If the -d option is specified, show all tables.
#
#         -A                  If the -d option is specified, show only tables that have a primary key.
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
#     alias pgls='sudo ~/bin/pglist.pl'
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

my (%opts, $prog, $data_source, $arg_database, $arg_table, $query, $db_oid, $tb_oid, 
	$arg_all, $name, $results, $dbh);
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
pglist - List PostgreSQL table modification timestamps

SYNOPSIS
     sudo pglist [-d [-a | -A]] database_name
     sudo pglist [-d [-a | -A]] database_name [-t] table_name

DESCRIPTION
    This program looks up the last modification times of a PostgreSQL 
    database and its respective tables. Typically it needs to be run in sudo mode.

    The following options are available:

    -a                  If the -d option is specified, show all tables.

    -A                  If the -d option is specified, show only tables that have a primary key.

    -d [database]       Database name. (required option)

    -h                  Usage

    -t [table]          Table name.


EXAMPLES
> sudo $prog -d college -a 
college                Mon Dec 12 10:53:20 2011 /pg${pg_version}/data/base/555555/PG_VERSION
college.students       Fri Dec 16 03:55:42 2011 /pg${pg_version}/data/base/555555/791234
college.books          Sat Dec 31 10:30:12 2011 /pg${pg_version}/data/base/555555/795555
college.rooms          Sun Dec 18 19:22:41 2011 /pg${pg_version}/data/base/555555/793245
college.classes        Sun Dec 25 17:10:01 2011 /pg${pg_version}/data/base/555555/799999
college.schedules      Thu Dec 29 02:43:05 2011 /pg${pg_version}/data/base/555555/797737

> sudo $prog -d college -t students
college                Mon Dec 12 10:53:20 2011 /pg${pg_version}/data/base/555555/PG_VERSION
college.students       Fri Dec 16 03:55:42 2011 /pg${pg_version}/data/base/555555/791234

END

	print $usage;
	exit(0);
}

if (! -d $pgdata_path) {
	print "ERROR: \$PGDATA path '$pgdata_path' does not exist!\n";
	print "       Perhaps you should try running this using sudo.\n";
	exit;
} 

my $opts_status = getopts('d:t:aAh', \%opts);
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

if (defined($opts{a})) {
	$arg_all = "true";
}

if (defined($opts{A})) {
	$arg_all = "true";
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
	$data_source  	= "$driver:dbname=$default_db";
	$dbh 			= DBI->connect($data_source, $username, $password) or die $DBI::errstr;
	#$query			= "SELECT oid FROM pg_database WHERE datname = ?;";
	#get_results($dbh, $query, $arg_database, "database");
	get_results($dbh, $arg_database, undef, "database");
	if (defined($arg_all)) {
		$data_source  	= "$driver:dbname=$arg_database";
		if (defined($opts{a})) {
			$query			= "SELECT relname FROM pg_class WHERE relkind = 'r' ORDER BY relname;";
		} else {
			$query			= "SELECT relname FROM pg_class WHERE relkind = 'r' AND relhaspkey='t' ORDER BY relname;";
		}
		my $dbh 		= DBI->connect($data_source, $username, $password) or die $DBI::errstr;
		my $sth 		= $dbh->prepare($query);
		$sth->execute();
		while (my $array_ref = $sth->fetchrow_arrayref) {
			my $relname = $array_ref->[0];
			$name 		= $arg_database . "." . $relname;
			#$query		= "SELECT oid FROM pg_class WHERE relname = ? AND relkind = 'r';";
			get_results($dbh, $arg_database, $relname, "table");
		}
		$sth->finish;
	}
	$dbh->disconnect;
}

if (defined($opts{t})) {
	#-rw-------  1 pgsql  pgsql  25616384 Dec 15 03:39 /pg90/data/base/736895/738844
	$name 			= $arg_database . "." . $arg_table;
	$data_source  	= "$driver:dbname=$arg_database";
	$dbh 			= DBI->connect($data_source, $username, $password) or die $DBI::errstr;
	#$query			= "SELECT oid FROM pg_class WHERE relname = ? AND relkind = 'r';";
	get_results($dbh, $arg_database, $arg_table, "table");
	$dbh->disconnect;
}

exit(0);

sub get_results {
	#my ($dbh, $this_query, $this_arg, $this_type) = @_;
	my ($dbh, $this_database, $this_table, $this_type) = @_;
	my ($sth, $oid, $stat_path, $ls_cmd, $timestamp);

	#$sth 			= $dbh->prepare($this_query);
	#$sth->execute($this_arg);
	#$query_results 	= $sth->fetchrow_arrayref;
	#$sth->finish;

	#$oid 			= get_oid($this_type, $query_results->[0], $this_arg);
	if ($this_type eq "database") {
		$oid		= `sudo ./pgoid.pl -d $this_database`;
		chomp ($oid);
		$db_oid 	= $oid;
		$stat_path	= "$pgdata_path/$db_oid/PG_VERSION";
	} elsif ($this_type eq "table") {
		$oid		= `sudo ./pgoid.pl -d $arg_database -t $this_table`;
		chomp ($oid);
		$tb_oid 	= $oid;
		$stat_path	= "$pgdata_path/$db_oid/$tb_oid";
	} else {
		$~ 			= "LINE";
		$results 	= "ERROR: Unknown type '$this_type' specified!";
		write();
		exit;
	}

	if (! -e $stat_path) {
		if (defined($arg_all)) {
			$timestamp = "Not found";
		} else {
			$~ 			= "LINE";
			$results 	= "ERROR: PostgreSQL data path '$stat_path' does not exist!";
			write();
			exit;
		}
	} else {
		$timestamp = ctime((stat($stat_path))[9]);
	}
	do_printing($timestamp, $stat_path);
}

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

sub do_printing {
	my ($timestamp, $path) = @_;

	$~ = "LINE";
	$results        = "$timestamp $path";
	
	write; 
}

sub clean_up {
    exit(0);
}
