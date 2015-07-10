#!/usr/bin/perl
# -*- cperl -*-

# Functions Provided:
# ADD:      $result  = addrecord(tablename, $record_as_hashref);
# UPDATE:   @changes = @{updaterecord(tablename, $record_as_hashref)};
# GET:      %record  = %{getrecord(tablename, id)};
# GETALL:   @records =   getrecord(tablename);     # Not for enormous tables.
# GETNEW:   @records =   getsince(tablename, timestampfield, datetimeobject);
# FIND:     @records = findrecord(tablename, fieldname, exact_value);
# FINDNULL: @records = findnull(tablename, fieldname); # Finds records where that field IS NULL.
# SEARCH:   @records =   searchrecord(tablename, fieldname, value_substring);
# COUNT:    %counts  = %{countfield(tablename, fieldname)}; # Returns a hash with counts for each value.
# COUNT:    %counts  = %{countfield(tablename, fieldname, start_dt, end_dt)}; # Ditto, but within the date range; pass DateTime objects.
# GET BY DATE:        (Last 3 args optional.  Dates, if specified, must be formatted for the db already.)
#           @records = @{getrecordbydate(tablename, datefield, mindate, maxdate, maxfields)};

use strict;
use DBI;
use Carp;
use Data::Dumper;
use HTML::Entities;

our $database = 'evergreen';
our $host     = '127.0.0.1';
our ($user, $password);
print "Enter username for database access: ";
$user = <STDIN>; chomp $user;
print "Enter password for database access: ";
$password = <STDIN>; chomp $password;

my $now = DateTime->now();
my $pgnow = DateTime::To::Postgres($now);

my ($jotcount, $jotsexpected); $|=1;
sub jot {
  my ($char) = @_;
  print "$char";
  print "   $jotcount (" . (sprintf "%0.1f", (100 * $jotcount / $jotsexpected)) . "%)\n  " if not ++$jotcount % 60;
}

#############################################################################

do './patron-info.txt' or die "Failed to parse patron-info.txt: $@ [$!]";
my @user = @{$main::VAR1};
$jotsexpected = scalar @user;
print "I have $jotsexpected patron records to attempt to import.\n  ";

open LOG, '>', 'import-patron.log';

for my $user (@user) {
  my $extantcard = findrecord('actor.card', 'barcode', $$user{card}[0]{barcode});
  if ($extantcard) {
    my $extantusr = getrecord('actor.usr', $$extantcard{usr});
    if ($extantusr) {
      print LOG " * ALREADY HAVE $$user{card}[0], $$extantusr{first_given_name} $$extantusr{second_given_name} $$extantusr{family_name}\n";
      jot 's';
      next;
    }
    die "Barcode $$user{card}[0] already used: " . Dumper(+{ extantcard => $extantcard, importinfo => $user });
  }
  my $extantusrname = findrecord('actor.usr', 'usrname', $$user{usr}{usrname});
  if ($extantusrname) {
    print LOG " * SKIPPING duplicate username $$user{usr}{usrname}, $$user{usr}{first_given_name} $$user{usr}{second_given_name} $$user{usr}{family_name} (see $$extantusrname{id})\n";
    jot 'X';
    next;
  }
  jot '.';
  # Start with $$user{usr}
  my $u = $$user{usr};
  print LOG " * $$u{first_given_name} $$u{second_given_name} $$u{family_name}.\n";
  $$u{expire_date} ||= $pgnow;
  $$u{create_date} ||= $pgnow;
  my $result = addrecord('actor.usr', $u);
  my $uid = $db::added_record_id;
  $uid or die "No user record ID for user: " . Dumper(\$user);
  print LOG "    + uid: $uid\n";
  # Now $$user{card}
  my $cid;
  for my $c (@{$$user{card}}) {
    print LOG "    * Card: $$c{barcode}\n";
    $$c{usr} = $uid;
    my $result = addrecord('actor.card', $c);
    $cid = $db::added_record_id if $$c{active};
  }
  $cid or die "No card record ID for user: " . Dumper(\$user);
  # Update the user record with the card id:
  my $urec = getrecord('actor.usr', $uid);
  ref $urec or die "Failed to retrieve usr record $uid (wanted to update card field)";
  $$urec{card} = $cid;
  updaterecord('actor.usr', $urec) or lcarp("Failed to update card field for usr $$urec{id}.");
  my ($aid, %inserted);
  for my $adr (@{$$user{usr_address}}) {
    my ($indication, $id, $arec) = @$adr;
    $$arec{usr} = $uid;
    print LOG "    * Address: $$arec{street1} $$arec{street2} $$arec{city} $$arec{state} $$arec{post_code}\n";
    if (not $inserted{$id}) {
      $inserted{$id} = addrecord('actor.usr_address', $arec);
      if ($inserted{$id}) {
        $aid = $db::added_record_id;
      }}}
  $aid or die "No address record ID for user: " . Dumper(\$user);
  $urec = getrecord('actor.usr', $uid);
  ref $urec or die "Failed to retrieve usr record $uid (wanted to update address fields)";
  $$urec{mailing_address} = $$urec{billing_address} = $aid;
  updaterecord('actor.usr', $urec) or lcarp("Failed to update address fields for usr $$urec{id}.");
  for my $n (@{$$user{usr_note}}) {
    my $encval       = $$n{encvalue}; delete $$n{encvalue};
    $$n{usr}         = $uid;
    $$n{create_date} = $pgnow;
    $$n{creator}   ||= 1;
    $$n{value}       = decode_entities($encval);
    my $len = '(' . (length $$n{value}) . ' bytes)';
    print LOG "    * Note: $$n{title}: $len\n";
    addrecord('actor.usr_note', $n) or lcarp("Failed to save note: " . Dumper(+{ %$n, encvalue => $encval }));
  }
}

print "   $jotcount (" . (sprintf "%0.1f", (100 * $jotcount / $jotsexpected)) . "%)
Done.\n";

exit 0; # Subroutines follow.
#############################################################################

sub lcarp {
  my ($msg) = @_;
  print LOG " * Carped: " . $msg;
  carp $msg;
}

sub DateTime_MS_to_PG {
  my ($ms) = @_;
  my $dt = DateTime::From::TSQL($ms);
  return DateTime::To::Postgres($dt);
}

sub DateTime::To::Postgres {
  my ($dt) = @_;
  croak "DateTime::Format::ForDB needs a DateTime object.\n" if not ref $dt;
  use DateTime::Format::Pg;
  my $pg = DateTime::Format::Pg->format_datetime($dt);
  return $pg;
}

sub DateTime::From::TSQL {
  my ($dtstring) = @_;
  carp "DateTime::From::TSQL called sans argument" if not defined $dtstring;
  use DateTime::Format::MySQL; # Believe it or not, this proves useful.
  # TSQL datetimes are only a little different from the MySQL equivalent.
  # Example of a TSQL datetime :  2008-02-08 13:50:51.590
  # Example of a MySQL datetime:  2008-02-08 00:00:00
  # We don't have any need for sub-second resolution,
  # so for now we'll just toss that and parse the rest.
  # (Frankly in most cases I just need the date part.)
  if ($dtstring =~ /^(.*?)(?:[.]\d+)?$/) {
    $dtstring = $1;
  }
  my $dt = undef;
  eval {
    $dt = DateTime::Format::MySQL->parse_datetime($dtstring);
  };
  carp "Jim Carey stars in an Ed Wood flick: $dtstring ($@)" if not ref $dt;
  return $dt;
}

sub dbconn {
  # Returns a connection to the database.
  # Used by the other functions in this file.
  use DBD::Pg;
  #my $db = DBI->connect($database, $host) or croak "The way is shut: $Postgres::error";
  my $db = DBI->connect("dbi:Pg:dbname=$database", $user, $password, {AutoCommit => 1})
    or croak "The way is shut.";
  return $db;
}
sub getsince {
# GETNEW:  @records =   getsince(tablename, timestampfield, datetimeobject);
  my ($table, $dtfield, $dt, $q) = @_;
  die "Too many arguments: getrecord(".(join', ',@_).")" if $q;
  my $when = DateTime::Format::ts($dt);
  my $db = dbconn();
  $q = $db->prepare("SELECT * FROM $table WHERE $dtfield >= $when");  $q->execute();
  my @answer; my $r;
  while ($r = $q->fetchrow_hashref()) {
    push @answer, $r;
  }
  return @answer;
}

sub getrecordbydate {
# GET BY DATE:        (Dates, if specified, must be formatted for the database already.)
#          @records = @{getrecordbydate(tablename, datefield, mindate, maxdate, maxfields)};
  my ($table, $field, $mindate, $maxdate, $maxfields, $q) = @_;
  die "Too many arguments: getrecordbydate(".(join', ',@_).")" if $q;
  die "Must specify either mindate or maxdate (or both) when calling getrecordbydate." if ((not $mindate) and (not $maxdate));
  die "Must specify date field when calling getrecordbydate." if not $field;
  #warn "DEBUG:  getrecordbydate(table $table, field $field, min $mindate, max $maxdate, maxfields $maxfields);";
  my $db = dbconn();
  my (@where, @arg);
  if ($mindate) {
    push @where, "$field >= ?";
    push @arg, $mindate;
  }
  if ($maxdate) {
    push @where, "$field <= ?";
    push @arg, $maxdate;
  }
  $q = $db->prepare("SELECT * FROM $table WHERE " . (join " AND ", @where) . ";");  $q->execute(@arg);
  my (@r, $r);
  while ($r = $q->fetchrow_hashref()) { push @r, $r; }
  if ($maxfields and @r > $maxfields) {
    # Fortuitously, database-formatted datetime strings sort correctly when sorted ASCIIbetically:
    @r = sort { $$a{$field} <=> $$b{$field} } @r;
#    if ($maxdate and not $mindate) {
      # If only the maxdate is specified, we want the _last_ n items before that:
      @r = @r[(0 - $maxfields) .. -1];
#    } else {
      # Otherwise, take the first n:
#      @r = @r[1 .. $maxfields];
#    }
  }
  return \@r;
}

sub getrecord {
# GET:     %record  = %{getrecord(tablename, id)};
# GETALL:  @recrefs = getrecord(tablename);     # Don't use this way on enormous tables.
  my ($table, $id, $q) = @_;
  croak "Too many arguments: getrecord(".(join', ',@_).")" if $q;
  carp "table should be just the table name." if ref $table;
  carp "id should be just a number, no funny business" if ref $id;
  my $db = dbconn();
  eval {
    $q = $db->prepare("SELECT * FROM $table".(($id)?" WHERE id = '$id'":""));  $q->execute();
  }; carp $@ if $@;
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

sub changerecord {
  # Used by updaterecord.  Do not call directly; use updaterecord instead.
  my ($table, $id, $field, $value) = @_;
  my $db = dbconn();
  my $q = $db->prepare("update $table set $field=? where id='$id'");
  my $answer;
  eval { $answer = $q->execute($value); };
  carp "Unable to change record: $@" if $@;
  return $answer;
}

sub updaterecord {
# UPDATE:  @changes = @{updaterecord(tablename, $record_as_hashref)};
# See end of function for format of the returned changes arrayref
  my ($table, $r, $f) = @_;
  die "Too many arguments: updaterecord(".(join', ',@_).")" if $f;
  die "Invalid record: $r" if not (ref $r eq 'HASH');
  my %r = %{$r};
  my $o = getrecord($table, $r{id});
  die "No such record: $r{id}" if not ref $o;
  my %o = %{$o};
  my @changes = ();
  foreach $f (keys %r) {
    if (($r{$f} || '') ne ($o{$f} || '')) {
      my $result = changerecord($table, $r{id}, $f, $r{$f});
      push @changes, [$f, $r{$f}, $o{$f}, $result];
    }
  }
  return \@changes;
  # Each entry in this arrayref is an arrayref containing:
  # field changed, new value, old value, result
}

sub addrecord {
# ADD:     $result  = addrecord(tablename, $record_as_hashref);
  my ($table, $r, $f) = @_;
  die "Too many arguments: addrecord(".(join', ',@_).")" if $f;
  my %r = %{$r};
  my $db = dbconn();
  #my @clause = map { "$_=?" } sort keys %r;
  #my @value  = map { $r{$_} } sort keys %r;
  #my $atat = '@' x 2;
  #my $q = $db->prepare("INSERT INTO $table SET ". (join ", ", @clause));
  my @f = sort keys %r;
  my $fields = join ", ", @f;
  my $slots  = join ", ", map { '?' } @f;
  my @value = map { $r{$_} } @f;
  my $q = $db->prepare("INSERT INTO $table ($fields) VALUES ($slots)");
  my $result = $q->execute(@value);
  if ($result) {
    #($db::added_record_id) = $q->{pg_oid_status}; # Calling code can read this magic variable if desired.
    my $idq = $db->prepare("SELECT currval(pg_get_serial_sequence('$table','id'));");
    $idq->execute();
    my $ar = $idq->fetchrow_arrayref();
    $db::added_record_id = $$ar[0];
    $db::added_record_id or carp "I don't seem to have correctly retrieved the ID of the added record.";
  } else {
    carp "addrecord failed: " . Dumper(+{
                                         error  => $q->errstr,
                                         table  => $table,
                                         record => $r,
                                        });
  }
  return $result;
}

sub countfield {
# COUNT:   $number  = countfind(tablename, fieldname);
  my ($table, $field, $startdt, $enddt, %crit) = @_;
  my $q;
  die "Incorrect arguments: date arguments, if defined, must be DateTime objects." if (defined $startdt and not ref $startdt) or (defined $enddt and not ref $enddt);
  die "Incorrect arguments: you must define both dates or neither" if (ref $startdt and not ref $enddt) or (ref $enddt and not ref $startdt);
  for my $criterion (keys %crit) {
    die "Incorrect arguments:  criterion $criterion specified without values." if not $crit{$criterion};
  }
  my $whereclause;
  if (ref $enddt) {
    my $start = DateTime::Format::MySQL->format_datetime($startdt);
    my $end   = DateTime::Format::MySQL->format_datetime($enddt);
    $whereclause = " WHERE fromtime > '$start' AND fromtime < '$end'";
  }
  for my $field (keys %crit) {
    my $v = $crit{$field};
    my $whereword = $whereclause ? 'AND' : 'WHERE';
    if (ref $v eq 'ARRAY') {
      $whereclause .= " $whereword $field IN (" . (join ',', @$v) . ") ";
    } else {
      warn "Skipping criterion of unknown type: $field => $v";
    }
  }
  my $db = dbconn();
  $q = $db->prepare("SELECT id, $field FROM $table $whereclause");
  $q->execute();
  my %c;
  while (my $r = $q->fetchrow_hashref()) {
    ++$c{$$r{$field}};
  }
  return \%c;
}

sub findrecord {
# FIND:    @records = findrecord(tablename, fieldname, exact_value);
  my ($table, $field, $value, $q) = @_;
  die "Too many arguments: findrecord(".(join', ',@_).")" if $q;
  my $db = dbconn();
  croak "field is too complicated" if ref $field;
  croak "value is too complicated" if ref $value;
  $q = $db->prepare("SELECT * FROM $table WHERE $field=?");  $q->execute($value);
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

sub findnull {
# FIND:    @records = findnull(tablename, fieldname); # Finds records where that field IS NULL.
  my ($table, $field, $q) = @_;
  die "Too many arguments: findnull(".(join', ',@_).")" if $q;
  my $db = dbconn();
  $q = $db->prepare("SELECT * FROM $table WHERE $field IS NULL");  $q->execute();
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

sub searchrecord {
# SEARCH:  @records = @{searchrecord(tablename, fieldname, value_substring)};
  my ($table, $field, $value, $q) = @_;
  die "Too many arguments: searchrecord(".(join', ',@_).")" if $q;
  my $db = dbconn();
  $q = $db->prepare("SELECT * FROM $table WHERE $field LIKE '%$value%'");  $q->execute();
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

1;

