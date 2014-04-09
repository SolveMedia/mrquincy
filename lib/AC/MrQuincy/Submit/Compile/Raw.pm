# -*- perl -*-

# Copyright (c) 2014
# Author: Jeff Weisberg <jaw @ solvemedia.com>
# Created: 2014-Apr-09 11:04 (EDT)
# Function: shell scripts, and such

package AC::MrQuincy::Submit::Compile::Raw;
use AC::Import;
use AC::Dumper;
use strict;

our @EXPORT = 'compile_raw';


sub compile_raw {
    my $comp  = shift;
    my $parse = shift;

    my $prog = $parse->{content};

    my @job;

    push @job, section($comp, 'map', $prog->{map}{code});

    if( $prog->{reduce} ){
        for my $i (0 .. @{$prog->{reduce}}-1){
            push @job, section($comp, "reduce/$i", $prog->{reduce}[$i]{code});
        }
    }

    if( $prog->{final} ){
        push @job, section($comp, 'final', $prog->{final}{code});
    }

    return \@job;
}

sub section {
    my $comp = shift;
    my $name = shift;
    my $code = shift;

    return {
        phase	=> $name,
        src	=> $code,
        maxrun	=> $comp->config( 'maxrun',     ),
        timeout => $comp->config( 'tasktimeout' ),
    };
}


1;
