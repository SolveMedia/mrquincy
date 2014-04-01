# -*- perl -*-

# Copyright (c) 2009 AdCopy
# Author: Jeff Weisberg
# Created: 2009-Oct-28 15:37 (EDT)
# Function: end-programmer convenience object
#
# $Id$

package AC::MrGamoo::Submit::Request;
use AC::MrGamoo::Submit::TieIO;
use AC::Misc;
use AC::Daemon;
use AC::ISOTime;
use strict;


sub new {
    my $class = shift;
    my $c     = shift;

    my $me = bless {
        file	=> $c->{file},
        config	=> $c->{content}{config},
        initres	=> $c->{initres},
        @_,
    }, $class;

    return $me;
}


# get config param
sub config {
    my $me = shift;
    my $k  = shift;

    return $me->{config}{$k};
}

# get result of init block
sub initvalue {
    my $me = shift;

    return $me->{initres};
}

# let user output a key+value via $R->output(...)
sub output {
    my $me = shift;

    $me->{func_output}->( @_ ) if $me->{func_output};
}



1;
