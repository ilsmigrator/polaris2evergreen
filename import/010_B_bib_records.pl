#!/usr/bin/perl
# -*- cperl -*-

# To create the .MRC file that this script needs as input, use the
# Polaris staff client, as follows.
# 1. Under the Utilities menu, choose Exporting,
# 2. On the dialog box, pull down the Tools menu and choose Configuration
# 3. Set the paths as you like, so you can find the files it will output.  OK.
# 4. Checkmark the box to Include item holdings information.
# 5. Under the Tools menu, click Export.
# 6. Click Start and wait until it finishes.  This takes a bit.
#    (Started: 9:45am; finished by 10:15; what wizardry is this?)
# 7. Click Done.
# 8. The resulting .MRC file is the one this script needs to find.

use strict;
use Carp;
use Data::Dumper;
use Term::ANSIColor;

my %arg = @ARGV;

my ($jotcount, $jotsexpected); $|=1;
sub jot {
  my ($char) = @_;
  print "$char";
  print "   $jotcount (" . (sprintf "%0.1f", (100 * $jotcount / $jotsexpected)) . "%)\n  " if not ++$jotcount % 60;
}

#############################################################################

my @mf = <*.MRC>;
my $prefix = FindCommonPrefix(map { lc $_ } @mf);
$prefix =~ s/[.]?MRC$//i;
printheading("Found " . @mf . " MARC input files.");
for my $mf (@mf) {
  my ($rest, $extn) = $mf =~ /^$prefix(.*?)[.](MRC)/i;
  my @arg = ('/usr/local/bin/yaz-marcdump',
	     '-f' => 'MARC-8',
	     '-t' => 'UTF-8',
	     '-o' => 'marcxml',
	     '-s' => qq[$prefix$rest],
	     '-C' => ($arg{chunksize} || 5000),
	     '-c' => qq[cfile_${prefix}$rest],
	     $mf);
  printheading(@arg);
  system(@arg);
  my $chunkpattern = lc $prefix;
  my @chunk = <${chunkpattern}*>;
  printitem("  Chunks: @chunk");
  if (@chunk) { # all_export_20150911130414000019
    for my $chunk (@chunk) {
      my $xml = `/usr/local/bin/yaz-marcdump -f MARC-8 -t UTF-8 -o marcxml $chunk`;
      open XML, ">", "xml_$chunk.xml";
      print XML $xml;
      close XML;
      printitem("  * Wrote: $chunk.xml");
    }
  } else {
    printwarning("  No chunks.");
  }
}
printheading("Finished.");

sub printwarning {
  print color "yellow on_black";
  print @_;
  print color "reset";
  print "\n";
}
sub printheading {
  print color "green on_black";
  print @_;
  print color "reset";
  print "\n";
}
sub printitem {
  print color "cyan on_black";
  print @_;
  print color "reset";
  print "\n";
}

sub FindCommonPrefix { # http://www.perlmonks.org/?node_id=274133
  my $model= pop @_;
  my $len= length($model);
  for my $item (  @_  ) {
    my $dif= $model ^ substr($item,0,$len);
    $len= length( ( $dif =~ /^(\0*)/ )[0] );
    substr( $model, $len )= "";
  }
  return $model;
}

