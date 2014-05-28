#!/usr/bin/env perl
#############################################################################
# PROGRAM
#     pgtab.pl
#
# SYNOPSIS
#     This program displays specific and often accessed pre-configured tables.
#
# USAGE
#     sudo pgtab.pl [OPTIONS]
#
#     Options:
#         -d [database]       Database name.
#
#         -h                  Usage
#
#         -i                  Displays currently configured tables.
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
##############################################################################
my $driver		= "dbi:Pg";				# DBI data_source driver
my $username 	= "pgsql";				# database user
my $password 	= "";					# database user password
my $default_db	= "template1";			# default database used if not specified 
my $tables		= {                     # tables and fields to display
	"book"				=> "id, name",
	"location"			=> "id, name",
	"student"			=> "id, name",
	"student_status"	=> "id, abbr, oneword",
	"room"				=> "id, nickname, description",
};
##############################################################################

use DBI;
use Time::localtime;
use Data::Dump qw(dump);
use Getopt::Std;
$Getopt::Std::STANDARD_HELP_VERSION = 1;

my $VERSION = "0.1";

my (%opts, $prog, $data_source, $arg_database, $arg_table, $query, $fields, $field1, $field2, $name, $results);
$prog = $0;

sub VERSION_MESSAGE {
	print $prog . " v." . $VERSION;
	exit(0);
}

sub HELP_MESSAGE {
my $usage = <<END;
usage: $prog [OPTIONS]

This program displays specific and often accessed pre-configured tables.

Options:
    -d [database]       Database name.

    -h                  Usage

    -i                  Displays currently configured tables.

    -t [table]          Table name.


Syntax examples:
> pgtab.pl -d college -t books (all options specified)
> pgtab.pl -t books            (defaults to college database)
> pgtab.pl books               (defaults to college database and considers this a table reference)

END

	print $usage;
	exit(0);
}

sub INFO_MESSAGE {
	my ($table, $fields);
format LIST = 
@<<<<<<<<<<<<<<<<<<<<< @*
$table, $fields
.

	$~ = "LIST";
	print "\nThe following tables are currently configured for display:\n\n";
	$table = "TABLE";
	$fields = "FIELDS";
	write();
	foreach $table (sort keys %{$tables}) {
		$fields =  $tables->{$table};
		write();
	}
	exit(0);
}

my $opts_status = getopts('d:t:hi', \%opts);
if (!$opts_status) {
	print "\n";
	HELP_MESSAGE();
}

if (defined($opts{h})) {
	HELP_MESSAGE();
}

if (defined($opts{i})) {
	INFO_MESSAGE();
}

if (defined($opts{d})) {
	$arg_database = $opts{d};
	$arg_database = sanitize($arg_database);
}

if (defined($opts{t})) {
	$arg_table = $opts{t};
	$arg_table = sanitize($arg_table);
	if (! defined($tables->{$arg_table})) {
		print "\nError: Table '$arg_table not defined!\n\n";
		exit(1);
	}
} else {
	my $args = $#ARGV + 1;
	if ($args == 1) {
		$arg_table = $ARGV[0];
	} else {
		print "\nError: Incorrect number of arguments specified!\n\n";
		exit(1);
	}
}	
$fields = $tables->{$arg_table};

if (!defined($arg_database)) {
	$arg_database = $default_db;
}

if (!defined($arg_table)) {
	print "\nError: a database table name is required.\n\n";
	HELP_MESSAGE();
} else {
	if (!defined($tables->{$arg_table})) {
		print "\nError: Table $arg_table is not currently configured.\n";
		INFO_MESSAGE();
	}
}

$data_source  	= "$driver:dbname=$arg_database";
$query			= "SELECT $fields FROM $arg_table WHERE seq = 0 ORDER BY id";
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
	$query_results 	= $sth->fetchall_arrayref;
	$sth->finish;
	$dbh->disconnect;

	print "$fields\n";
	print dump $query_results;
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
