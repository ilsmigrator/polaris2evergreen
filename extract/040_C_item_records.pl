#!/usr/bin/perl
# -*- cperl -*-

use strict;
use Carp;
use Data::Dumper;
use HTML::Entities;
require './db-pls.pl';

my $itemsperfile = 2000;
my $debug = 1;

our $database = 'Polaris';
our $host     = '127.0.0.1';
our ($user, $password);
print "Enter username for database access: ";
$user = <STDIN>; chomp $user;
print "Enter password for database access: ";
$password = <STDIN>; chomp $password;
our $dsn      = "driver={SQL Server};Server=$server; database=Polaris;uid=$user;pwd=$password;";

my ($jotcount, $jotsexpected); $|=1;
sub jot {
  my ($char) = @_;
  print "$char";
  print "   $jotcount (" . (sprintf "%0.1f", (100 * $jotcount / $jotsexpected)) . "%)\n  "
    if not ++$jotcount % 60;
}

my @cir = getrecord('Polaris.CircItemRecords');

$jotsexpected = scalar @cir;
print "Selected $jotsexpected item records.\n  ";

open LOG, '>', 'item-extract-0001.log';

my (@item, @file, %status, %col, %mtype, %fc, %lp, %sl, $ecount);
for my $i (@cir) {
  jot '.';
  print LOG "Item $$i{ItemRecordID}\n";
  my $d     = findrecord('Polaris.ItemRecordDetails', 'ItemRecordID',     $$i{ItemRecordID});
  my $statr = findrecord('Polaris.ItemStatuses',      'ItemStatusID',     $$i{ItemStatusID});
  my $acrec = findrecord('Polaris.Collections',       'CollectionID',     $$i{AssignedCollectionID});
  my $mtrec = findrecord('Polaris.MaterialTypes',     'MaterialTypeID',   $$i{MaterialTypeID});
  my $lprec = findrecord('Polaris.LoanPeriodCodes',   'LoanPeriodCodeID', $$i{LoanPeriodCodeID});
  my $slrec = findrecord('Polaris.ShelfLocations',    'ShelfLocationID',  $$i{ShelfLocationID});
  if (ref $statr) {
    $status{$$i{ItemStatusID}}      ||= ${$statr}{Description};
  } else {
    $ecount++;
    print LOG "  Failed to find status record $$i{ItemStatusID}\n";
  }
  if (ref $acrec) {
    $col{$$i{AssignedCollectionID}} ||= ${$acrec}{Name};
  } else {
    $ecount++;
    print LOG " Failed to find collection record $$i{AssignedCollectionID}\n";
  }
  if ($mtrec) {
    $mtype{$$i{MaterialTypeID}}     ||= ${$mtrec}{Description};
  } else {
    $ecount++;
    print LOG " Failed to find material type record $$i{MaterialTypeID}\n";
  }
  $fc{$$i{FineCodeID}}            ||= getfinecode($$i{FineCodeID});
  if (ref $lprec) {
    $lp{$$i{LoanPeriodCodeID}}      ||= ${$lprec}{Description};
  } else {
    $ecount++;
    print LOG " Failed to find loan period code record $$i{LoanPeriodCodeID}\n";
  }
  if (ref $slrec) {
    $sl{$$i{ShelfLocationID}}       ||= ${$slrec}{Description};
  } else {
    $ecount++;
    print LOG " Failed to find shelf location record $$i{ShelfLocationID}\n";
  }
  push @item, +{# lowercase fields go into asset.copy as-is, or mostly as-is.
                # (DateTimes may still need to be converted for Pg.)
                # Capitalized fields need to be transformed or are foreign keys.
                Barcode                  => $$i{Barcode},
                Status                   => $status{$$i{ItemStatusID}},
                status_changed_time      => DateTime_MS_to_PG($$i{LastCircTransactionDate}),
                BibliographicRecordID    => $$i{AssociatedBibRecordID},
                AssignedCollection       => $col{$$i{AssignedCollectionID}},
                MaterialType             => $mtype{$$i{MaterialTypeID}},
                YTDCircCount             => $$i{YTDCircCount},
                LifetimeCircCount        => $$i{LifetimeCircCount},
                FreeTextBlock            => $$i{FreeTextBlock},
                ManualBlockID            => $$i{ManualBlockID},
                FineCodeID               => $$i{FineCodeID},
                FinePerDay               => $fc{$$i{FineCodeID}}{PerDay},
                MaxFine                  => $fc{$$i{FineCodeID}}{Maximum},
                LoanPeriod               => $lp{$$i{LoanPeriodCodeID}},
                StatisticalCodeID        => $$i{StatisticalCodeID},
                ShelfLocation            => $sl{$$i{ShelfLocationID}},
                NonCirculating           => $$i{NonCirculating},
                OriginalCheckOutDate     => $$i{OriginalCheckOutDate},
                OriginalDueDate          => $$i{OriginalDueDate},
                ItemStatusDate           => $$i{ItemStatusDate},
                CheckInDate              => $$i{CheckInDate},
                LastCheckOutRenewDate    => $$i{LastCheckOutRenewDate},
                FirstAvailableDate       => $$i{FirstAvailableDate},
                PublicNote               => $$d{PublicNote},
                NonPublicNote            => $$d{NonPublicNote},
                CreationDate             => $$d{CreationDate},
                LastInventoryDate        => $$d{LastInventoryDate},
                Price                    => $$d{Price},
                PhysicalCondition        => $$d{PhysicalCondition},
                CallNumber               => $$d{CallNumber},
                DonorID                  => $$d{DonorID},
                # Both Polaris tables have some additional fields,
                # which I didn't bother with, because we don't use
                # them here.  If you are a multi-branch library,
                # you probably also want to grab these (at minimum):
                #OwningBranchID           => $$d{OwningBranchID},
                #AssignedBranchID         => $$i{AssignedBranchID},
                #LoaningOrgID             => $$i{LoaningOrgID},
               };
}
print "\n\nWARNING: logged 'Failed to find' $ecount times.\n" if $ecount;
push @file, ('' . writeitems(@item));

print "\n\nFiles written:\n" . (join "\n", map { " * $_" } @file) . "\nDone.\n";

exit 0; # Subroutines follow.

sub getfinecode {
  my ($id) = @_;
  my $rec = findrecord('Polaris.FineCodes', 'FineCodeID', $id);
  my $desc = $$rec{Description};
  my ($perday, $max) = $desc =~ m!([0-9.]+)/([0-9.]+)!;
  return +{
           id          => $id,
           Description => $desc,
           PerDay      => $perday,
           Maximum     => $max,
          };
}

sub writeitems {
  my (@i) = @_;
  my $filename = 'item-data-' . (sprintf "%04d", (1 + scalar @file)) . '.txt';
  open OUT, '>', $filename;
  print OUT Dumper( \@i );
  close OUT;
  print LOG "Closed $filename, closing logfile too.\n";
  close LOG;
  my $logfile = 'item-extract-' . (sprintf "%04d", (2 + scalar @file)) . '.log';
  open LOG, '>', $logfile;
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
  # just toss that and parse the rest.  (Frankly in most cases I could
  # live with just the date part; needing time down to the minute is
  # rare enough, and seconds are just overkill.  Microseconds, meh.)
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

