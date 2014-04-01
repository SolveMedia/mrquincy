# -*- perl -*-

# Copyright (c) 2010 AdCopy
# Author: Jeff Weisberg
# Created: 2010-Jan-14 12:46 (EST)
# Function: iterate over a file

package AC::MrQuincy::Iter::File;
use AC::MrQuincy::Iter;
use JSON;
our @ISA = 'AC::MrQuincy::Iter';
use strict;

sub new {
    my $class = shift;
    my $fd    = shift;
    my $pf    = shift;

    return bless {
        fd	 => $fd,
    }, $class;
}

sub _nextrow {
    my $me = shift;

    if( $me->{buf} ){
        my $r = $me->{buf};
        delete $me->{buf};
        return $r;
    }

    my $fd = $me->{fd};
    my $l  = scalar <$fd>;
    return unless $l;

    return decode_json($l);
}


1;
