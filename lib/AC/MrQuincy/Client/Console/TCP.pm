# -*- perl -*-

# Copyright (c) 2011
# Author: Jeff Weisberg <jaw @ solvemedia.com>
# Created: 2011-May-11 14:09 (EDT)
# Function: end user console over tcp
#
# $Id$

package AC::MrQuincy::Client::Console::TCP;
use AC::Protocol;
use AC::DC::Debug;
use AC::Dumper;
use strict;

our @ISA = ('AC::DC::IO::TCP', 'AC::MrQuincy::Client::Console');

my $TIMEOUT = 5;

sub new {
    my $class = shift;
    my $fd    = shift;
    my $ip    = shift;
    my $srvr  = shift;
    my $mrm   = shift;

    my $me = $class->SUPER::new( info => 'console server', from_ip => $ip );
    $me->{mr_con_fdebug} = $srvr->{mr_con_fdebug};
    $me->{_mrm} = $mrm;

    $me->start($fd);
    $me->timeout_rel($TIMEOUT);
    $me->set_callback('read',    \&read);
    $me->set_callback('timeout', \&timeout);
    $me->set_callback('shutdown', \&check);
}


sub read {
    my $me  = shift;
    my $evt = shift;

    my($proto, $data, $content) = read_protocol_no_content( $me, $evt );
    return unless $proto;

    $data = AC::Protocol->decode_request($proto, $data) if $data;
    $me->output( $data, $me->{_mrm} );
    $me->{gotdata} = 1;
}

sub timeout {
    my $me = shift;

    verbose("timeout!");
    $me->shut();
}

sub check {
    my $me = shift;

    return if $me->{gotdata};
    verbose("no data!");
}

1;
