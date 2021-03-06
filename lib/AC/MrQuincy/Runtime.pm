# -*- perl -*-

# Copyright (c) 2014
# Author: Jeff Weisberg <jaw @ solvemedia.com>
# Created: 2014-Mar-31 21:41 (EDT)
# Function: Mr Quincy end-user task runtime

package AC::MrQuincy::Runtime;
use AC::MrQuincy::Iter::File;
use AC::Import;
use JSON;
use strict;

sub new {
    my $class = shift;
    my $conf  = shift;
    my $init  = shift;

    # mrquincy expects m/r data on fd#3
    open STDDAT, '>&=', 3;
    select STDDAT; $| = 1;
    select STDOUT; $| = 1;

    return bless {
        conf	=> $conf,
        init	=> $init,
    }, $class;
}

# for running the init section
sub new_init {
    my $class = shift;
    my $conf  = shift;

    return bless {
        conf	=> $conf,
    }, $class;
}

# get config param
sub config {
    my $me = shift;
    my $k  = shift;

    return $me->{conf}{$k};
}

# get result of init block
sub initvalue {
    my $me = shift;

    return $me->{init};
}

sub filter {
    my $me = shift;
    my $d  = shift;

    return if $d->{tstart} <  $me->{conf}{start};
    return if $d->{tstart} >= $me->{conf}{end};
    return 1;
}

sub progress {
    my $me = shift;
    print STDDAT "\n";	# mapio will drop the empty record
}

sub output {
    my $me = shift;
    print STDDAT encode_json( \@_ ) , "\n";
}

sub print {
    my $me = shift;
    print @_;
}

1;
