# -*- perl -*-

# Copyright (c) 2011
# Author: Jeff Weisberg <jaw @ solvemedia.com>
# Created: 2011-May-11 14:51 (EDT)
# Function: output to end user console

package AC::MrQuincy::Client::Console;
use AC::MrQuincy::Client::Console::TCP;
use AC::MrQuincy::Client::Console::UDP;
use AC::Dumper;
use strict;

sub output {
    my $me  = shift;
    my $req = shift;
    my $mrm = shift;

    AC::DC::IO->request_exit() if $req->{type} eq 'finish';

    print STDERR "$req->{msg}"                               if $req->{type} eq 'stderr';
    print STDERR "$req->{msg}\n"                             if $req->{type} eq 'error';
    print "$req->{msg}"                                      if $req->{type} eq 'stdout';

    my $f = $mrm->{console}{ $req->{type} };
    $f->( $req->{msg} ) if $f;

}

1;
