# -*- perl -*-

# Copyright (c) 2011
# Author: Jeff Weisberg <jaw @ solvemedia.com>
# Created: 2011-May-11 14:33 (EDT)
# Function: end-user console over udp
#
# $Id$

package AC::MrQuincy::Client::Console::UDP;
use AC::Protocol;
use AC::DC::Debug;
use AC::Dumper;
use AC::Misc;
use strict;

our @ISA = ('AC::DC::IO::UDP', 'AC::MrQuincy::Client::Console');

my $BUFSIZ  = 65536;

sub new {
    my $class = shift;
    my $udp   = shift;
    my $mrm   = shift;

    # recvfrom
    my $buf;
    my $fd = $udp->{fd};
    my $peer = recv($fd, $buf, $BUFSIZ, 0);

    eval {
        my $proto = AC::Protocol->decode_header($buf);
        # print STDERR dumper($proto), "\n";
        my $data  = substr($buf, AC::Protocol->header_size());
        my $req   = AC::Protocol->decode_request($proto, $data);

        $udp->AC::MrQuincy::Client::Console::output( $req, $mrm );
    };
    if( $@ ){
        print STDERR "$@" if $@;
        print STDERR hex_dump($buf), "\n";
    }
}


1;

