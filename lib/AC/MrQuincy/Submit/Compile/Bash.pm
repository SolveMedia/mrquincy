# -*- perl -*-

# Copyright (c) 2014
# Author: Jeff Weisberg <jaw @ solvemedia.com>
# Created: 2014-Mar-31 22:53 (EDT)
# Function: 

package AC::MrQuincy::Submit::Compile::Bash;
use AC::Import;
use AC::Dumper;
use JSON;
use strict;

our @EXPORT = 'compile_bash';

sub compile_bash {
    my $comp  = shift;
    my $parse = shift;

    my $prog = $parse->{content};

    # there is no init section

    push @job, compile_map( $comp, $prog );
    if( $prog->{reduce} ){
        for my $i (0 .. @{$prog->{reduce}}-1){
            push @job, compile_reduce( $comp, $prog, $i );
        }
    }
    if( $prog->{final} ){
        push @job, compile_final( $comp, $prog );
    }

    return \@job;
}

sub boilerplate {
    my $comp = shift;
    my $prog = shift;

    # libs?
    my $code = <<EOBP;
#!/usr/bin/bash

$prog->{common}

EOBP
    ;
    return $code;
}



1;
