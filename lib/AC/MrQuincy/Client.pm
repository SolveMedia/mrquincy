# -*- perl -*-

# Copyright (c) 2010 AdCopy
# Author: Jeff Weisberg
# Created: 2010-Jan-25 16:32 (EST)
# Function: m/r client
#
# $Id$

package AC::MrQuincy::Client;
use AC::MrQuincy::Submit::Compile;
use AC::MrQuincy::Submit::Parse;
use AC::MrQuincy::Client::Console;
use AC::DC::Debug;
use AC::Daemon;
use AC::Conf;
use AC::Misc;
use AC::Protocol;
use AC::DC::IO;
use JSON;
use Sys::Hostname;
use Socket;

require 'AC/MrQuincy/proto/mrmagoo.pl';
require 'AC/MrQuincy/proto/std_reply.pl';
use strict;


sub new {
    my $class = shift;
    my $from  = shift;	# file | text
    my $src   = shift;

    my $host  = hostname();
    my $user  = getpwuid($<);
    my $trace = "$user/$$\@$host:" . ($from eq 'file' ? $src : 'text');

    # parse job
    my $mrp = AC::MrQuincy::Submit::Parse->new( $from => $src );


    return bless {
        fdebug  => sub{},
        prog	=> $mrp,
        job	=> {
            jobid		=> unique(),
            traceinfo		=> $trace,
            reduce_width	=> $mrp->reduce_width(),
        },
    }, $class;
}

sub compile {
    my $me   = shift;
    my $conf = shift;

    my $mrc = AC::MrQuincy::Submit::Compile->new(
        config	=> $conf,
        @_,		# lang, perlbin, libdir, runinit, ...
        # ...
    );

    my $mrj = $mrc->compile( $me->{prog} );

    $me->{job}{options} = $mrc->options();
    $me->{job}{section} = $mrj;

    return $me;
}

sub id {
    my $me = shift;
    return $me->{job}{jobid};
}

# use the specified master
sub master {
    my $me     = shift;
    my $master = shift;

    $me->{master} = $master;
}

sub set_debug {
    my $me = shift;
    my $en = shift;

    $me->{fdebug} = $en ? sub { print STDERR "@_\n" } : sub{};
}


################################################################

sub open_console {
    my $me = shift;

    my $c = AC::DC::IO::TCP::Server->new( 0, 'AC::MrQuincy::Client::Console::TCP', $me );
    my $port = $c->port();
    my $u = AC::DC::IO::UDP::Server->new( $port, 'AC::MrQuincy::Client::Console::UDP', $me );

    $c->{mr_con_fdebug} = $u->{mr_con_fdebug} = $me->{fdebug};
    $me->{job}{console} = ":$port";

}

sub run_console {
    my $me = shift;

    $me->{console} = { @_ };

    AC::DC::IO->mainloop();
}

sub submit {
    my $me   = shift;
    my $seed = shift;	# [ "ipaddr:port", ... ]

    my $req = AC::Protocol->encode_request( {
        type		=> 'mrmagoo_jobcreate',
        msgidno		=> $^T,
        want_reply	=> 1,
    }, $me->{job} );

    my $ok;
    if( my $master = $me->{master} ){
        # use specified master (for debugging)
        my($addr, $port) = split /:/, $master;
        $me->_submit_to( $addr, $port, $req );
        $me->{master} = { addr => $addr, port => $port };
        $ok = 1;
    }else{
        # pick server
        $ok = $me->_pick_master_and_send( $req, $seed );
    }

    return $ok ? $me->id() : undef;
}

sub abort {
    my $me = shift;

    return unless $me->{master};
    my $res = $me->_submit_to( $me->{master}{addr}, $me->{master}{port}, AC::Protocol->encode_request( {
        type		=> 'mrgamoo_jobabort',
        msgidno		=> $^T,
        want_reply	=> 1,
    }, {
        jobid		=> $me->{id},
    }));

}

################################################################

sub _pick_master_and_send {
    my $me   = shift;
    my $req  = shift;
    my $seed = shift;

    my @serverlist;

    my $listreq = AC::Protocol->encode_request( {
        type		=> 'mrmagoo_status',
        msgidno		=> $^T,
        want_reply	=> 1,
    }, {});

    # get the full list of servers
    # contact each seed passed in above, until we get a reply
    for my $s ( @$seed ){
        my($addr, $port) = split /:/, $s;
        $me->{fdebug}->("attempting to fetch server list from $addr:$port");
        eval {
            alarm(1);
            my $reply = AC::Protocol->send_request( inet_aton($addr), $port, $listreq, $me->{fdebug} );
            my $res   = AC::Protocol->decode_reply($reply);
            alarm(0);
            my $list = $res->{status};
            @serverlist = @$list if $list && @$list;
        };
        last if @serverlist;
    }

    # sort+filter list
    @serverlist = sort { ($a->{sort_metric} <=> $b->{sort_metric}) || int(rand(3)) - 1 }
      grep { $_->{status} == 200 } @serverlist;

    # try all addresses
    # RSN - sort addresslist in a Peers::pick_best_addr_for_peer() like manner?

    my @addrlist = map { @{$_->{ip}} } @serverlist;

    for my $ip (@addrlist){
        my $addr = inet_itoa($ip->{ipv4});
        my $res;
        eval {
            alarm(30);
            $res = $me->_submit_to( $addr, $ip->{port}, $req );
            alarm(0);
        };
        next unless $res && $res->{status_code} == 200;
        $me->{master} = { addr => $addr, port => $ip->{port} };
        return 1;
    }
    return ;
}

sub _submit_to {
    my $me   = shift;
    my $addr = shift;
    my $port = shift;
    my $req  = shift;

    $me->{fdebug}->("sending job to $addr:$port");
    my $reply = AC::Protocol->send_request( inet_aton($addr), $port, $req, $me->{fdebug}, 120 );
    my $res   = AC::Protocol->decode_reply($reply);

    return $res;
}


1;

