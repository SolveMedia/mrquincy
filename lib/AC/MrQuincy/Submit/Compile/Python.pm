# -*- perl -*-

# Copyright (c) 2014
# Author: Jeff Weisberg <jaw @ solvemedia.com>
# Created: 2014-Mar-31 22:53 (EDT)
# Function: 

package AC::MrQuincy::Submit::Compile::Python;
use AC::Import;
use AC::Dumper;
use JSON;
use strict;

our @EXPORT = 'compile_python';

sub compile_python {
    my $comp  = shift;
    my $parse = shift;

    die "oops! forgot to write the python compiler!\n";
}


1;
