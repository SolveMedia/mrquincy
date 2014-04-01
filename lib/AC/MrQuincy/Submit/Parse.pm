# -*- perl -*-

# Copyright (c) 2009 AdCopy
# Author: Jeff Weisberg
# Created: 2009-Oct-27 17:39 (EDT)
# Function: compile job component


package AC::MrQuincy::Submit::Parse;
use strict;

my %PARSE = (
    config	=> { tag => 'config',  multi => 0, },
    doc 	=> { tag => 'block',   multi => 1, },
    init	=> { tag => 'block',   multi => 0, },
    common	=> { tag => 'simple',  multi => 0, },
    map		=> { tag => 'block',   multi => 0, required => 1, },
    reduce	=> { tag => 'block',   multi => 1, },
    final	=> { tag => 'block',   multi => 0, },
    readinput	=> { tag => 'block',   multi => 0, },
    );

my %BLOCK = (
    init	=> 'simple',
    cleanup	=> 'simple',
    attr	=> 'config',
   );

sub new {
    my $class = shift;

    my $me = bless {
        @_,
        # file | text
    }, $class;

    if( $me->{file} ){
        open(my $fd, $me->{file}) || $me->_die("cannot open file: $!");
        local $/ = undef;
        $me->{text} = <$fd>;
        close $fd;
    }
    $me->{lines} = [ split /^/m, $me->{text} ];

    $me->_parse();
    $me->_check();

    delete $me->{lines};
    delete $me->{text};

    return $me;
}



sub _die {
    my $me  = shift;
    my $err = shift;

    if( $me->{_lineno} ){
        die "ERROR: $err\nfile: $me->{file} line: $me->{_lineno}\n$me->{_line}\n";
    }
    die "ERROR: $err\nfile: $me->{file}\n";
}

sub _next {
    my $me = shift;

    return unless @{ $me->{lines} };
    $me->{_line} = shift @{ $me->{lines} };
    $me->{_lineno} ++;
    # $me->{_file_content} .= $me->{_line};
    return $me->{_line};
}

sub _parse {
    my $me = shift;

    while(1){
        my $line = $me->_next();
        last unless defined $line;
        chomp $line;

        # white, comment, or start
        $line =~ s/^%#.*//;
        $line =~ s/#.*//;
        next if $line =~ /^\s*$/;

        my($tag) = $line =~ m|^<%(.*)>\s*$|;
        my $d    = $PARSE{$tag};

        if( $d->{tag} eq 'block'){
            $me->_add_block($tag, $me->_parse_block($tag));
        }
        elsif( $d->{tag} eq 'simple' ){
            $me->_add_block($tag, $me->_parse_block_simple($tag));
        }
        elsif( $d->{tag} eq 'config' ){
            $me->_add_config($tag, $me->_parse_config($tag));
        }
        else{
            $me->_die("syntax error");
        }
    }

    delete $me->{_lineno};
    delete $me->{_line};
    delete $me->{_fd};

    1;
}

sub _lineno_info {
    my $me  = shift;

    # should have the number of the _next_ line
    return sprintf "#line %d $me->{file}\n", $me->{_lineno} + 1;
}

sub _parse_block {
    my $me  = shift;
    my $tag = shift;

    my $b = {};
    $b->{code} = $me->_lineno_info();

    while(1){
        my $line = $me->_next();
        $me->_die("end of file reached looking for end of $tag section") unless defined $line;
        last if $line =~ m|^</%$tag>\s*$|;

        my($tag) = $line =~ m|^<%(.*)>\s*$|;

        if( $BLOCK{$tag} eq 'simple' ){
            $b->{$tag} .= $me->_parse_block_simple( $tag );
            $b->{code} .= $me->_lineno_info();
        }elsif( $BLOCK{$tag} eq 'config' ){
            $b->{$tag} = $me->_parse_config( $tag );
        }elsif( $tag ){
            $me->_die("syntax error");

        }else{
            $b->{code} .= $line;
        }
    }

    return $b;
}

sub _parse_block_simple {
    my $me  = shift;
    my $tag = shift;

    my $b = $me->_lineno_info();

    while(1){
        my $line = $me->_next();
        $me->_die("end of file reached looking for end of $tag section") unless defined $line;
        last if $line =~ m|^</%$tag>\s*$|;
        $b .= $line;
    }

    return $b;
}

sub _parse_config {
    my $me  = shift;
    my $tag = shift;

    my $c = {};

    while(1){
        my $line = $me->_next();
        $me->_die("end of file reached looking for end of '$tag' section") unless defined $line;
        return $c if $line =~ m|^</%$tag>\s*$|;

        $line =~ s/#.*$//;
        $line =~ s/^\s+//;
        $line =~ s/\s+$//;
        next unless $line;
        my($k, $v) = split /\s+=>\s*/, $line, 2;
        $c->{$k} = $v;
    }
}

sub _add_block {
    my $me  = shift;
    my $tag = shift;
    my $blk = shift;

    my $d = $PARSE{$tag};

    if( $d->{multi} ){
        push @{$me->{content}{$tag}}, $blk;
    }else{
        $me->_die("redefinition of '$tag' section") if $me->{content}{$tag};
        $me->{content}{$tag} = $blk;
    }
}

sub add_config {
    my $me  = shift;
    my $cfg = shift;

    $me->_add_config('config', $cfg);
}

sub _add_config {
    my $me  = shift;
    my $tag = shift;
    my $cfg = shift;

    my $d = $PARSE{$tag};

    if( $d->{multi} ){
        # merge
        @{ $me->{content}{$tag} }{ keys %$cfg } = values %$cfg;
    }else{
        $me->_die("redefinition of '$tag' section") if $me->{content}{$tag};
        $me->{content}{$tag} = $cfg;
    }
}


sub _check {
    my $me = shift;

    for my $s (keys %PARSE){
        next unless $PARSE{$s}{required};
        next if $me->{content}{$s};
        $me->_die("missing required section '$s'");
    }
    1;
}


sub reduce_width {
    my $me = shift;

    return $me->{content}{config}{reduces};
}

1;
