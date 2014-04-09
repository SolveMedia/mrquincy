# -*- perl -*-

# Copyright (c) 2014
# Author: Jeff Weisberg <jaw @ solvemedia.com>
# Created: 2014-Mar-31 17:31 (EDT)
# Function: compile job into perl

package AC::MrQuincy::Submit::Compile::Perl;
use AC::Import;
use AC::Dumper;
use IPC::Open3;
use JSON;
use strict;

our @EXPORT = 'compile_perl';

# defaults, may be overridden in Compile->new
my $PERLBIN	= '/usr/local/bin/perl';
my $LIBDIR	= '/home/adcopy/lib';

sub compile_perl {
    my $comp  = shift;
    my $parse = shift;

    my $prog = $parse->{content};

    my @job;

    $comp->{confjs}   = encode_json( $comp->{config} );

    # compile + run init section
    if( $comp->{runinit} && $prog->{init} ){
        my $init = compile_init( $comp, $prog );
        my $code = $init->{src};
        my $r = eval $code;
        die if $@;
        $comp->{initjs} = encode_json($r) if $r;
    }

    $comp->{initjs} ||= encode_json({});

    push @job, compile_map( $comp, $prog );
    if( $prog->{reduce} ){
        for my $i (0 .. @{$prog->{reduce}}-1){
            push @job, compile_reduce( $comp, $prog, $i );
        }
    }
    if( $prog->{final} ){
        push @job, compile_final( $comp, $prog );
    }

    # print STDERR dumper(\@job); exit;
    for my $j (@job){
        syntax_check( $comp, $j->{phase}, $j->{src} );
    };

    return \@job;
}

sub boilerplate {
    my $comp = shift;
    my $prog = shift;

    my $perl = $comp->{perlbin} || $PERLBIN;
    my $libs = $comp->{libdir}  || $LIBDIR;
    $libs = [ $libs ] unless ref $libs;
    my $mods = $comp->{modules};
    $mods = [ $mods ] if $mods && !ref $mods;

    my $code = "#!$perl\n";
    $code .= "use lib '$_';\n" for @$libs;

    if( $mods ){
        $code .= "use $_;\n"   for @$mods;
    }

    $code .= <<EOBP;
use AC::MrQuincy::Runtime;
use AC::Logfile;
use JSON;
use strict;

$prog->{common}

EOBP
    ;
    return $code;
}

sub compile_init {
    my $comp = shift;
    my $prog = shift;

    my $sec = $prog->{init};

    my $code = boilerplate($comp, $prog, $sec);
    my $uniq = "$$\_$^T\_" . int(rand(0xffff));
    $code .= <<EOCONF;
my \$R = do {
    my \$conf = decode_json(<<'__END_OF_CONFIG_$uniq\__');
$comp->{confjs}
__END_OF_CONFIG_$uniq\__
    AC::MrQuincy::Runtime->new_init( \$conf );
};

EOCONF
    ;

    $code .= "sub program {\n$sec->{code}\n}\n";
    $code .= "program();\n";

    return {
        phase	=> 'init',
        maxrun	=> $comp->config( 'maxrun',      $sec ),
        timeout => $comp->config( 'tasktimeout', $sec ),
        src	=> $code,
    };
}


sub compile_common {
    my $comp = shift;
    my $prog = shift;
    my $sec  = shift;
    my $name = shift;
    my $loop = shift;

    # customize me!
    my $code = boilerplate($comp, $prog);
    my $uniq = "$$\_$^T\_" . int(rand(0xffff));
    $code .= <<EOCOMMON;
my \$R = do {
    my \$conf = decode_json(<<'__END_OF_CONFIG_$uniq\__');
$comp->{confjs}
__END_OF_CONFIG_$uniq\__

    my \$init = decode_json(<<'__END_OF_INITRES_$uniq\__');
$comp->{initjs}
__END_OF_INITRES_$uniq\__

    AC::MrQuincy::Runtime->new( \$conf, \$init );
};

EOCOMMON
    ;

    $code .= "{\n";
    $code .= $sec->{init} if $sec->{init};
    $code .= "\nsub program {\n$sec->{code}\nreturn;\n}\n";
    $code .= $loop;
    $code .= $sec->{cleanup} if $sec->{cleanup};
    $code .= "};\n";

    return {
        phase	=> $name,
        maxrun	=> $comp->config( 'maxrun',      $sec ),
        timeout => $comp->config( 'tasktimeout', $sec ),
        width	=> $comp->config( 'taskwidth',   $sec ),
        src	=> $code,
    };
}

sub compile_map {
    my $comp = shift;
    my $prog = shift;

    my $sec = $prog->{map};

    my $loop = "\nwhile(<>){\n";

    if( $comp->config('lineinput', $sec) ){
        $loop .= 'my $d = $_;' . "\n";
    }else{
        $loop .= "\t" . 'my $d = parse_dancr_log( $_ );' . "\n";
        $loop .= "\t" . 'next unless $R->filter(  $d );' . "\n";
    }

    $loop .= <<'EOW';
    my($key, $data) = program( $d );
    $R->output( $key, $data ) if defined $key;
}

EOW
    ;

    return compile_common($comp, $prog, $sec, 'map', $loop);
}

sub compile_reduce {
    my $comp = shift;
    my $prog = shift;
    my $nred = shift;

    my $sec = $prog->{reduce}[$nred];
    my $loop = <<'EOW';

do {
  my $iter = AC::MrQuincy::Iter::File->new( \*STDIN );
  while( defined(my $k = $iter->key()) ){

      my($key, $data) = program( $k, $iter );
      $R->output( $key, $data ) if defined $key;
  }
};

EOW
    ;

    return compile_common($comp, $prog, $sec, "reduce/$nred", $loop);
}

sub compile_final {
    my $comp = shift;
    my $prog = shift;

    my $sec = $prog->{final};

    my $loop = <<'EOW';

while(<>){
    my $d = decode_json( $_ );
    my @o = program( @$d );

    $R->output(@o) if @o;
}

EOW
    ;

    return compile_common($comp, $prog, $sec, 'final', $loop);

}


sub syntax_check {
    my $comp = shift;
    my $name = shift;
    my $code = shift;

    my $file = "/tmp/mrjob.$$";

    open(my $tmp, '>', $file);
    print $tmp $code;
    close $tmp;

    my $perl = $comp->{perlbin} || $PERLBIN;

    my $errs;
    my $pid = open3($errs, $errs, $errs, $perl, '-c', $file);
    my $out;
    while(<$errs>){ $out .= $_ }
    waitpid( $pid, 0 );
    my $status = $?;

    if( $status ){
        print STDERR "syntax error checking section $name\n";
        print STDERR $out;
        exit -1;
    }

    return 1;

}


1;
