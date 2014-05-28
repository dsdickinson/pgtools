#!/usr/bin/env perl
#############################################################################
# PROGRAM
#     pgcol.pl
#
# SYNOPSIS
#     This program displays all tables of a specific database that contain a 
#     specific column.
#
# USAGE
#     pgcol.pl [OPTIONS]
#
#     Options:
#         -c [column]         Column name.
#
#         -d [database]       Database name.
#
#         -h                  Usage
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
##############################################################################
my $driver		= "dbi:Pg";				# DBI data_source driver
my $username 	= "pgsql";				# database user
my $password 	= "";					# database user password
my $default_db	= "template1";			# default database used for connection 
##############################################################################

use DBI;
use Time::localtime;
use Data::Dump qw(dump);
use Getopt::Std;
$Getopt::Std::STANDARD_HELP_VERSION = 1;

my $VERSION = "0.1";

my (%opts, $prog, $data_source, $arg_column, $arg_database, $query, $fields, $field1, $field2, $name, $results);
$prog = $0;

sub VERSION_MESSAGE {
	print $prog . " v." . $VERSION;
	exit(0);
}

sub HELP_MESSAGE {
my $usage = <<END;
NAME
pgcol - Display PostgreSQL tables and views that refer to a specific column

SYNOPSIS
    pgcol -d [database_name] -c [column_name]

DESCRIPTION
    This program displays all tables and views of a specific database that 
    refer to a specific column.

    The following options are available:

    -c [column]         Column name.

    -d [database]       Database name.

    -h                  Usage


Syntax examples:
> pgcol.pl -d college -c countyid
classroom
classroom_v
book
book_v
computer
computer__v
location
location_v

END

	print $usage;
	exit(0);
}

my $opts_status = getopts('c:d:h', \%opts);
if (!$opts_status) {
	print "\n";
	HELP_MESSAGE();
}

if (defined($opts{h})) {
	HELP_MESSAGE();
}

if (defined($opts{c})) {
	$arg_column = $opts{c};
	$arg_column = sanitize($arg_column);
}

if (!defined($arg_column)) {
    print "\nError: a table column name is required.\n\n";
    HELP_MESSAGE();
}

if (defined($opts{d})) {
	$arg_database = $opts{d};
	$arg_database = sanitize($arg_database);
}

if (!defined($arg_database)) {
	$arg_database = $default_db;
}

my $column_name	= "table_name";
$data_source  	= "$driver:dbname=$arg_database";
$query			= "SELECT $column_name FROM information_schema.columns WHERE column_name = '$arg_column' ORDER BY table_name";
get_results($data_source, $query);

exit(0);

sub get_results {
	my ($this_data_source, $this_query) = @_;
	my ($dbh, $sth, $query_results, $error);

	my %attr = (
    	PrintError => 0,
    	RaiseError => 1,
	);

	$dbh 			= DBI->connect($this_data_source, $username, $password, \%attr) or die $DBI::errstr;
	$sth 			= $dbh->prepare($this_query);
	$sth->execute();
	$query_results 	= $sth->fetchall_arrayref({});
	$sth->finish;
	$dbh->disconnect;

	foreach my $row (@$query_results) {
		print $row->{$column_name} . "\n";
	}
}

sub sanitize {
	my ($input) = @_;

	$input =~s /\s+//g;
	$input =~s /;//g;
	$input =~s /[^\w]+//g;

	return $input;
}

sub clean_up {
    exit(0);
}
