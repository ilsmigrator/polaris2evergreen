#!/usr/bin/perl
# -*- cperl -*-

# Database Functions:
# GET:      %record  = %{getrecord(tablename, id)};
# GETALL:   @records =   getrecord(tablename);     # Not efficient for enormous tables.
# FIND:     @records = findrecord(tablename, fieldname, exact_value);
# RAW:      my $dbh  = dbconn(); # Return a DBI database handle object.

use DBI;
use Carp;
use Data::Dumper;

sub dbconn {
  # Returns a connection to the database.
  # Used by the other functions in this file.
  my $db = DBI->connect("dbi:ODBC:$dsn", {'RaiseError' => 1})
    or die ("Cannot Connect: $DBI::errstr\n");
  #my $q = $db->prepare("use $dbconfig::database");
  #$q->execute();
  return $db;
}

sub getrecord {
# GET:     %record  = %{getrecord(tablename, id)};
# GETALL:  @recrefs = getrecord(tablename);     # Don't use this way on enormous tables.
  my ($table, $id, $q) = @_;
  die "Too many arguments: getrecord(".(join', ',@_).")" if $q;
  my $db = dbconn();
#  eval {
    $q = $db->prepare("SELECT * FROM $table".(($id)?" WHERE id = '$id'":""));  $q->execute();
#  }; use Carp;  croak() if $@;
  my @answer; my $r;
  while ($r = $q->fetchrow_hashref()) {
    if (wantarray) {
      push @answer, $r;
    } else {
      return $r;
    }
  }
  return @answer;
}

sub findrecord {
# FIND:    @records = findrecord(tablename, fieldname, exact_value);
  my ($table, $field, $value, $q) = @_;
  croak "Too many arguments: findrecord(".(join', ',@_).")" if $q;
  croak "No table" if not $table;
  croak "No field" if not $field;
  my $db = dbconn();
  eval {
    $q = $db->prepare("SELECT * FROM $table WHERE $field=?");
    $q->execute($value);
  }; carp(Dumper(+{ table => $table, field => $field, value => $value, function => 'findrecord', })) if $@;
  my @answer; my $r;
  while ($r = $q->fetchrow_hashref()) {
    if (wantarray) {
      push @answer, $r;
    } else {
      return $r;
    }
  }
  return @answer;
}

42;
