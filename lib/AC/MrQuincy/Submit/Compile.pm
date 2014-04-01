# -*- perl -*-

# Copyright (c) 2009 AdCopy
# Author: Jeff Weisberg
# Created: 2009-Oct-27 17:39 (EDT)
# Function: compile job component


package AC::MrQuincy::Submit::Compile;
use AC::MrQuincy::Submit::Compile::Perl;
use AC::MrQuincy::Submit::Compile::Ruby;
use AC::MrQuincy::Submit::Compile::Python;
use JSON;
use strict;

sub new {
    my $class = shift;

    return bless {
        @_,
    }, $class;
}

sub compile {
    my $me = shift;
    my $parse = shift;

    # merge config
    $me->add_config( $parse->{content}{config} );

    if( $me->{lang} eq 'perl' ){
        return compile_perl( $me, $parse );
    }
    if( $me->{lang} eq 'ruby' ){
        return compile_ruby( $me, $parse );
    }
    if( $me->{lang} eq 'python' ){
        return compile_python( $me, $parse );
    }
    if( $me->{lang} eq 'bash' ){
        return compile_bash( $me, $parse );
    }

    # ...

    die "unknown lang '$me->{lang}'. cannot compile\n";
}

sub add_config {
    my $me  = shift;
    my $cfg = shift;

    # merge mrjob config into cmdline config
    for my $k (keys %$cfg){
        $me->{config}{$k} = $cfg->{$k} unless defined $me->{config}{$k};
    }
}

sub options {
    my $me = shift;

    return encode_json( $me->{config} || {} );
}

1;

