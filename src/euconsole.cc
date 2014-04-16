/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-21 15:22 (EDT)
  Function: 

*/
#define CURRENT_SUBSYSTEM	't'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "misc.h"
#include "network.h"
#include "euconsole.h"

#include "mrmagoo.pb.h"

#include <sys/types.h>
#include <poll.h>
#include <sys/socket.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <sys/wait.h>

#define MAXBUF		4096


EUConsole::EUConsole(const char *type, const ACPMRMTaskCreate *g){

    _udp = 0;
    if( !g->has_console() ) return;

    _udp = socket(PF_INET, SOCK_DGRAM, 0);

    _gm.set_jobid( g->jobid() );
    _gm.set_server_id( myserver_id );
    _gm.set_type( type );
    _addr = g->console();
}

EUConsole::~EUConsole(){
    _flush();
    close(_udp);
}

void
EUConsole::_flush(void){
    if( !_udp ) return;
    if( _buf.empty() ) return;

    _gm.set_msg( _buf );
    toss_request(_udp, _addr.c_str(), PHMT_MR_DIAGMSG, &_gm );
    _buf.clear();
}

void
EUConsole::send(const char *msg, int len){

    if( !_udp ) return;
    // discard single NULL (keepalive)
    if( len == 1 && msg[0] == 0 ) return;

    _buf.append(msg, len);
    if( _buf.size() < MAXBUF ) return;

    _flush();
}

void
EUConsole::done(void){
    _flush();
}
