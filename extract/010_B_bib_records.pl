#!/usr/bin/perl
# -*- cperl -*-

use strict;
use Carp;
use Data::Dumper;
use HTML::Entities;
require './db-pls.pl';

my $bibsperfile = 2000;
my $debug = 1;

our $database = 'Polaris';
our $host     = '127.0.0.1';
our ($user, $password);
print "Enter username for database access: ";
$user = <STDIN>; chomp $user;
print "Enter password for database access: ";
$password = <STDIN>; chomp $password;
print "Enter hostname of database server: ";
$server = <STDIN>; chomp $server;
our $dsn      = "driver={SQL Server};Server=$server; database=Polaris;uid=$user;pwd=$password;";

my ($jotcount, $jotsexpected); $|=1;
sub jot {
  my ($char) = @_;
  print "$char" if not ++$jotcount % 10;
  print "   $jotcount (" . (sprintf "%0.1f", (100 * $jotcount / $jotsexpected)) . "%)\n  "
    if not $jotcount % 600;
}

#my @bibr = map { findrecord('Polaris.BibliographicRecords', 'BrowseAuthor', $_)
#               } ('Roberts, Nora.', 'Williams, Tad.', 'Shakespeare, William, 1564-1616.');
my @bibr = getrecord('Polaris.BibliographicRecords');

$jotsexpected = scalar @bibr;
print "Selected $jotsexpected bib records.\n  ";

my %marctom = map {
  $$_{MARCTypeOfMaterialID} => $_,
} getrecord('Polaris.MarcTypeOfMaterial');
open  TOM, '>', 'marctom.txt';
print TOM Dumper(\%marctom);
close TOM;

open LOG, '>', 'bib-extract-0001.log';

my (@bib, @file);
for my $bibr (@bibr) {
  jot '.';
  print LOG "Bib $$bibr{BibliographicRecordID}\n";
  my @t;
  for my $tr (findrecord('Polaris.BibliographicTags', 'BibliographicRecordID', $$bibr{BibliographicRecordID})) {
    print LOG "  Tag $$tr{TagNumber}" if $debug > 1;
    my @sr = findrecord('Polaris.BibliographicSubfields', 'BibliographicTagID', $$tr{BibliographicTagID});
    print LOG ", " . @sr . " subfields.\n" if $debug > 1;
    push @t, [ $tr, @sr ];
  }
  print LOG "  " . @t . " Tags.\n";
  push @bib, +{ bibrec   => $bibr,
                marctag  => [@t],
                marctom  => [findrecord('Polaris.BibliographicTOMIndex', 'BibliographicRecordID', $$bibr{BibliographicRecordID})],
              };
  if (not $jotcount % $bibsperfile) {
    push @file, ('' . writebibs(@bib));
    @bib = ();
  }
}
push @file, ('' . writebibs(@bib));

print "\n\nFiles written:\n" . (join "\n", map { " * $_" } @file) . "\nDone.\n";

exit 0; # Subroutines follow.

sub writebibs {
  my (@b) = @_;
  my $filename = 'bib-and-marc-data-' . (sprintf "%04d", (1 + scalar @file)) . '.txt';
  open  OUT, '>', $filename;
  print OUT Dumper( \@b );
  close OUT;
  print LOG "Closed $filename, closing logfile too.\n";
  close LOG;
  my $logfile = 'bib-extract-' . (sprintf "%04d", (2 + scalar @file)) . '.log';
  open  LOG, '>', $logfile;
  print LOG "Opened new logfile, continuing where we left off...\n";
  return $filename;
}

sub DateTime_MS_to_PG {
  my ($ms) = @_;
  #my $dt = DateTime::From::TSQL($ms);
  #use DateTime::Format::Pg;
  #my $pg = DateTime::Format::Pg->format_datetime($dt);
  #return $pg;
  return $ms; # Forget it.  We'll do the date conversion at import
              # time.  It'll be easier on a Linux system with a
              # working CPAN.pm than on a Windows system with PPM.
}

sub DateTime::From::TSQL {
  my ($dtstring) = @_;
  carp "DateTime::From::TSQL called sans argument" if not defined $dtstring;
  use DateTime::Format::MySQL; # Believe it or not, this proves useful.
  # TSQL datetimes are only a little different from the MySQL equivalent.
  # Example of a TSQL datetime :  2008-02-08 13:50:51.590
  # Example of a MySQL datetime:  2008-02-08 00:00:00
  # We don't have any need for sub-second resolution, so for now we'll
  # just toss that and parse the rest.  (Frankly, doing the seconds is
  # more overkill than we have any real use for.  Half the time we
  # could live without the minutes and hours in a pinch.)
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

