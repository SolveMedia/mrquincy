# -*- perl -*-

# Copyright (c) 2009 AdCopy
# Author: Jeff Weisberg
# Created: 2009-Nov-10 11:48 (EST)
# Function: iterator for reducing

package AC::MrQuincy::Iter;
use strict;

sub key {
    my $me = shift;

    # move ahead to 1st row of next key
    while( defined(my $r = $me->_nextrow()) ){
        next if $r->[0] eq $me->{key};

        $me->_putback($r);
        $me->{key} = $r->[0];
        return $r->[0];
    }

    return;	# eof
}

sub next {

    my($data, $end) = _next(@_);
    if( wantarray ){
        return ($data, $end);
    }else{
        return $data;
    }
}

sub _next {
    my $me = shift;

    my $r = $me->_nextrow();
    return (undef, 1) unless $r;	# eof

    return $r->[1] if $me->{key} eq $r->[0];

    # end of key
    $me->_putback($r);
    return (undef, 1);
}

sub foreach {
    my $me  = shift;
    my $sub = shift;

    while(1){
        my($r,$end) = $me->next();
        last if $end;
        $sub->($r);
    }
}

sub count {
    my $me = shift;

    my $n = 0;
    while(1){
        my($r,$end) = $me->next();
        return $n if $end;
        $n ++;
    }
}

sub sum {
    my $me = shift;
    my @sum;

    while(1){
        my($r,$end) = $me->next();
        if( $end ){
            if( wantarray ){
                return @sum;
            }else{
                return $sum[0];
            }
        }

        for my $i (0 .. $#_){
            $sum[$i] += $r->{ $_[$i] };
        }
    }
}

################################################################

sub _putback {
    my $me = shift;
    my $r  = shift;

    $me->{buf} = $r;
}


1;
