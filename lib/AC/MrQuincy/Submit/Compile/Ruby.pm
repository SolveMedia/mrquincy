# -*- perl -*-

# Copyright (c) 2014
# Author: Jeff Weisberg <jaw @ solvemedia.com>
# Created: 2014-Mar-31 22:53 (EDT)
# Function: 

package AC::MrQuincy::Submit::Compile::Ruby;
use AC::Import;
use AC::Dumper;
use JSON;
use strict;

our @EXPORT = 'compile_ruby';

sub compile_ruby {
    my $comp  = shift;
    my $parse = shift;

    die "oops! forgot to write the ruby compiler!\n";
}


1;
