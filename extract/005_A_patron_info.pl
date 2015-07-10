#!/usr/bin/perl
# -*- cperl -*-

# Database Functions:
# GET:      %record  = %{getrecord(tablename, id)};
# GETALL:   @records =   getrecord(tablename);     # Not efficient for enormous tables.
# FIND:     @records = findrecord(tablename, fieldname, exact_value);
# RAW:      my $dbh  = dbconn(); # Return a DBI database handle object.

use strict;
use HTML::Entities;
use Carp;
use DateTime;
require './db-pls.pl';

open LOG, ">>", "patron-info-extract.log";
print LOG "
*********************************************************************************
 " . DateTime->now() . " Started odbc-extract-patron-info.pl
*********************************************************************************\n";

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

print LOG " " . DateTime->now(). " Constructed DSN for DB connections\n";

my ($jotcount, $rc); $|=1;
sub jot {
  my ($char) = @_;
  print "$char";
  print " $jotcount\n  " if not ++$jotcount % 60;
}

our $orgunitid = 1; # OU id in Pg

our $eighteenyearsago = DateTime::Format::MySQL->format_datetime(
                        DateTime->now()->subtract( years => 18 ));

my (@usr, @map, %stop, %zipcode, %stopdesc, %addrtype, %username);
my %profile = (
               # Polaris.Patrons.PatronCodeID  => evergreen.actor.usr.profile
               1 => 2, # Adult                 => Patron
               2 => 2, # Adult Non-Resident    => Patron
               3 => 2, # Juvenile              => Patron
               4 => 2, # Juvenile Non-Resident => Patron
               5 => 4, # Staff                 => Cataloger
               6 => 2, # Board                 => Patron
               7 => 2, # Homebound             => Patron
               8 => 2, # Teacher               => Patron
               9 => 2, # Homeschooler          => Patron
              );

#my @patr = findrecord('Polaris.Patrons', 'PatronCodeID', 5);
my @patr = getrecord('Polaris.Patrons');

print "Selected " . @patr . " patrons.\n  ";
print LOG " " . DateTime->now(). " Selected " . @patr . " patrons.\n";
my ($xcount, $pcount, $ncount, $acount, $ccount);
for my $p (@patr) {
  my $r = findrecord('Polaris.PatronRegistration', 'PatronID', $$p{PatronID});
  my (@un, @ua, @uc, $u);
  if ($$r{NameFirst} =~ /Input Required/) {
    jot 'x'; $xcount++;
  } else {
    jot '.'; $pcount++;
    my $first  = $$r{NameFirst};
    my $middle = $$r{NameMiddle};
    if ($first =~ / / and not $middle) {
      ($first, $middle) = $first =~ /(\S+)\s+(.*)/;
    }
    my $expiredt = DateTime::From::TSQL($$r{UpdateDate} ||
                                        $$r{EntryDate} ||
                                        $$r{RegistrationDate})->add(months => 18);
    my $expiredate = DateTime::Format::MySQL->format_datetime($expiredt);
    $u = +{
              # card is a foreign key into actor.card
              profile            => $profile{$$p{PatronCodeID}},
              usrname            => createusername($$r{EmailAddress},
                                                   $$r{NameLast}, $first, $middle,
                                                   $$p{Barcode}),
              email              => $$r{EmailAddress},
              passwd             => $$r{Password},
              standing           => 1, # "Good", as opposed to 2, which means "Barred", see config.standing
              # ident_type, ident_value, ident_type2, ident_value2 are foreign keys, handled below.
              net_access_level   => 1, # "Filtered", not that our ILS really needs to know this.  (The field exists for ILS/WAP integration.)
              # photo_url is something we will not bother with at migration time, maybe later
              prefix             => namecase($$r{NameTitle}),
              first_given_name   => namecase($first),
              second_given_name  => namecase($middle),
              family_name        => namecase($$r{NameLast}),
              suffix             => $$r{NameSuffix},
              # alias is something we can add after migration, I believe
              day_phone          => $$r{PhoneVoice1},
              other_phone        => $$r{PhoneVoice2},
              evening_phone      => $$r{PhoneVoice3},
              # mailing_address is a foreign key
              # billing_address is a foreign key
              home_ou            => $orgunitid,
              dob                => DateTime_MS_to_PG($$r{Birthdate}),
              active             => (($$r{PatronFullName} =~ /^[*][*]/) ? 'false' : 'true'),
              # master_account defaults to false, which is fine
              # super_user defaults to false, which is fine
              # barred defaults to false, which is fine
              deleted            => (($$r{PatronFullName} =~ /^[*][*]/) ? 'true' : 'false'),
              juvenile           => (($$r{BirthDate} ge $eighteenyearsago) ? 'true' : 'false'),
              # usrgroup is a foreign key
              # claims_returned_count may need to be computed
              credit_forward_balance => $$p{CreditsAmount},
              # last_xact_id, not sure what to do for that, if anything
              # alert_message can probably be left empty?
              create_date        => DateTime_MS_to_PG($$r{RegistrationDate}),
              expire_date        => DateTime_MS_to_PG($expiredate),
             };
    my $otherdata = +{
                      LegacyPatronID        => $$p{PatronID},
                      LegacyPatronCode      => $$p{PatronCodeID},
                      LegacySystemBlocks    => $$p{SystemBlocks},
                      LegacyYTDCirc         => $$p{YTDCircCount},
                      LegacyLifetimeCirc    => $$p{LifetimeCircCount},
                      LegacyLastActDate     => $$p{LastActivityDate},
                      LegacyClaimCount      => $$p{ClaimCount},
                      LegacyLostCount       => $$p{LostItemCount},
                      LegacyCharges         => $$p{ChargesAmount},
                      LegacyCredits         => $$p{CreditsAmount},
                      LegacySchoolDistrict  => $$r{User1},
                      LegacyGender          => $$r{Gender},
                      LegacyPrefReadingList => $$r{ReadingList},
                      LegacyPrefDeliveryOpt => (($$r{DeliveryOptionID} == 2) ? 'email'
                                                : (($$r{DeliveryOptionID} == 8) ? 'SMS' : 'default')),
                      LegacyEmailAlternate  => $$r{AltEmailAddress},
                      LegacyPrefEnableSMS   => $$r{EnableSMS},
                      LegacySMSNumber       => $$r{TxtPhoneNumber},
                      LegacyCarrierID1      => $$r{Phone1CarrierID},
                      LegacyCarrierID2      => $$r{Phone2CarrierID},
                      LegacyCarrierID3      => $$r{Phone3CarrierID},
                     };
    my @identnum = ('', '2');
    if ($$r{User2} and ($$r{User2} =~ /^([A-Z]{2}[0-9]{6})(?: +(.*))?$/)) {
      # Driver's License
      my ($value, $note);
      my $i = shift @identnum;
      $$u{"ident_value". $i} = $value;
      $$u{"ident_type" . $i} = 1;
      push @un, +{
                  title     => "Drivers License Note",
                  encvalue  => encode_entities($note),
                 } if $note;
    } elsif ($$r{User2}) {
      push @un, +{
                  title     => "Unparsed Drivers License",
                  encvalue  => encode_entities($$r{User2}),
                 };
    }
    # TODO: are there any other forms of ID we should support here?
    if (not $$u{ident_type2}) {
      # We haven't always collected ID in the past; this kludge allows
      # us to grandfather in our existing patron base.
      my $i = shift @identnum;
      $$u{"ident_type" . $i}  = 99; # TODO: make sure there's an identification_type record for this.
      $$u{"ident_value". $i} = $$p{PatronID};
    }
    push @un, +{
                title            => "Parent Name",
                encvalue         => encode_entities($$r{User3}),
               } if $$r{User3};
    push @un, +{
                title            => "Polaris User4 Field",
                encvalue         => encode_entities($$r{User4}),
               } if $$r{User4};
    push @un, +{
                title            => "Polaris User5 Field",
                encvalue         => encode_entities($$r{User5}),
               } if $$r{User5};
    for my $notes (findrecord('Polaris.PatronNotes', 'PatronID', $$p{PatronID})) {
      push @un, +{
                  title          => 'Polaris Notes',
                  encvalue       => encode_entities($$notes{NonBlockingStatusNotes}),
                 } if $$notes{NonBlockingStatusNotes};
      push @un, +{
                  title          => 'Polaris Blocking Notes',
                  encvalue       => encode_entities($$notes{BlockingStatusNotes}),
                 } if $$notes{BlockingStatusNotes};
    }
    for my $stop (findrecord('Polaris.PatronStops', 'PatronID', $$p{PatronID})) {
      $stopdesc{$$stop{PatronStopID}}
        ||= findrecord('Polaris.PatronStopDescriptions', 'PatronStopID', $$stop{PatronStopID});
      push @un, +{
                  title          => 'Polaris Stop',
                  encvalue       => encode_entities($stopdesc{$$stop{PatronStopID}}{Description}),
                 };
    }
    for my $ftb (findrecord('Polaris.PatronFreeTextBlocks', 'PatronID', $$p{PatronID})) {
      push @un, +{
                  title          => 'Free-Text Block',
                  encvalue       => encode_entities($$ftb{FreeTextBlock}),
                 }
    }
    if ($$r{AltEmailAddress}) {
      push @un, +{
                  title           => 'Alternate Email Address',
                  encvalue        => encode_entities($$r{AltEmailAddress}),
                 };
    }
    push @uc, +{
                barcode => $$p{Barcode},
                active  => 'true',
               };
    push @uc, +{ barcode => $$p{FormerID}, } if $$p{FormerID};

    my %addrseen;
    for my $addy (grep { not $addrseen{$$_{PatronID}}{$$_{AddressID}}++
                           # de-duplicate the addresses that are listed repeatedly
                           # in Polaris.PatronAddresses referencing the same address
                           # with a different "type".
                       } findrecord('Polaris.PatronAddresses', 'PatronID', $$p{PatronID})) {
      $addrtype{$$addy{AddressTypeID}} ||= findrecord('Polaris.AddressTypes', 'AddressTypeID', $$addy{AddressTypeID});
      my $address = findrecord('Polaris.Addresses', 'AddressID', $$addy{AddressID});
      $zipcode{$$address{PostalCodeID}}
        ||= findrecord('Polaris.PostalCodes', 'PostalCodeID', $$address{PostalCodeID});
      push @ua, [ $addrtype{$$addy{AddressTypeID}}{Description},
                  $$address{AddressID}, # Use this datum for de-duplication.
                  +{
                    valid        => (($zipcode{$$address{PostalCodeID}}{PostalCode} =~ /99999/)
                                     ? 'false' : 'true'),
                    street1      => $$address{StreetOne},
                    street2      => $$address{StreetTwo},
                    city         => $zipcode{$$address{PostalCodeID}}{City},
                    state        => $zipcode{$$address{PostalCodeID}}{State},
                    county       => $zipcode{$$address{PostalCodeID}}{County},
                    country      => 'USA',
                    post_code    => $zipcode{$$address{PostalCodeID}}{PostalCode},
                    address_type => $$addy{FreeTextLabel},
                   }
                ];
    }
    my $rh = +{ enabled => 0 };
    if ($$r{ReadingList}) {
      $rh = +{ enabled => 1,
               history => [ findrecord('Polaris.PatronReadingHistory', 'PatronID', $$p{PatronID}) ],
               # In order to actually do anything with these, you need the item records, of course.
               # Those will be gathered by a different extraction script.
             };
    }
    $ncount += scalar @un;
    $acount += scalar @ua;
    $ccount += scalar @uc;

    push @map, [ $$p{PatronID} => $$p{Barcode} ];
    push @usr, +{
                 usr         => $u,
                 usr_note    => \@un,
                 usr_address => \@ua,
                 card        => \@uc,
                 otherdata   => $otherdata,
                 readhist    => $rh,
                };
  }
}
print LOG " " . DateTime->now(). " Counts: p=$pcount, x=$xcount, a=$acount, c=$ccount, n=$ncount\n";

open  OUT, '>', 'patron-info.txt';
print OUT Dumper( \@usr );
close OUT;

print LOG " " . DateTime->now(). " Wrote patron-info.txt\n";

open OUT, '>', 'patron-id-to-barcode-map.txt';
print OUT Dumper( \@map );
close OUT;

print LOG " " . DateTime->now(). " Wrote patron-id-to-barcode-map.txt\n";

print LOG " " . DateTime->now(). " Finished.  Exiting...\n";
exit 0; # Subroutines follow.

sub createusername {
  my ($email, $last, $first, $middle, $barcode) = @_;
  $middle ||= '';
  if ($email) {
    my ($emailusername) = $email =~ /^(.*?)[@]/;
    my $candidate = lc $emailusername;
    $candidate =~ s/[^A-Za-z0-9_.-]//g; # No esoteric characters or funny business.
    if ($candidate and not $username{$candidate}) {
      $username{$candidate} = $barcode || 'true';
      return $candidate;
    }}
  my ($f) = $first =~ /(\w)/;
  my $m = ($middle =~ /(\w)/) ? $1 : '';
  for my $candidate (
                     "$last$f$m",
                     "$last$first",
                     "$last$first$m",
                     "$last$first$middle",
                     "$last$f$middle",
                     "$first$m$last",
                     "$first$middle$last",
                     $barcode,
                    ) {
    $candidate = lc $candidate;
    $candidate =~ s/[^A-Za-z0-9_.-]//g; # No esoteric characters or funny business.
    if ($candidate and not $username{$candidate}) {
      $username{$candidate} = $barcode || 'true';
      return $candidate;
    }}
  while (1) {
    my $sylcount = 2 + int rand(3);
    my $candidate = join '', map { randomsyllable() } 1 .. $sylcount;
    if ($candidate and not $username{$candidate}) {
      $username{$candidate} = $barcode || 'true';
      return $candidate;
    }}
}

sub randomsyllable {
  my @ci = #qw(b bl br c ch d dr dl f fr fl g gr gl h j l m n p pr pl ph pw
           #   qu r s st str sh sc sl sn sp squ sw t tr tl th v w y);
           qw(b c ch d f g h j l m n p ph r s st sh t tr th v);
  my @v  = qw(a e i o u); #qw(a ai ay e ei ea i o oi oa oo ou oy u);
  my @cf = qw(b ck d f ff g gh h k l lb ld lf lg lk ll lm ln lp ls lt
              m m n n p r rb rch rd rf rg rk rm rn rp rt rv
              s sh sk sp st t tch th ts v x y z);
  my $ci = $ci[rand @ci];
  my $v  = $v[rand @v];
  my $cf = $cf[rand @cf];
  my $r = rand(100);
  if ($r < 20) {
    $ci = '';
  } elsif ($r < 40) {
    $cf = '';
  }
  return $ci . $v . $cf;
}

sub namecase {
  my ($orig) = @_;
  return '' if not $orig;
  return $orig if $orig ne uc $orig; # Leave mixed-case names alone.
  return $orig if $orig =~ /[(].*[)]/;
  return ucfirst lc $orig;
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


