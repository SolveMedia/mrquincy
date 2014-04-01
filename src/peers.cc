/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-17 16:17 (EDT)
  Function: 

*/

#define CURRENT_SUBSYSTEM	'p'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "misc.h"
#include "network.h"
#include "runmode.h"
#include "thread.h"
#include "peers.h"

#include "mrmagoo.pb.h"

#include <netinet/in.h>
#include <strings.h>

#define MAXFAIL		2


Peer::Peer(const ACPMRMStatus *g){

    _gstatus  = new ACPMRMStatus( *g );
    _num_fail = 0;
    _last_try = 0;
    _last_up  = 0;
    _id       = _gstatus->server_id().c_str();

    switch( g->status() ){
    case 200:
    case 102:
        _status = PEER_STATUS_UP;
        break;
    default:
        _status = PEER_STATUS_DN;
    }

    // determine best addr
    const ACPIPPort *best = 0;
    for(int i=0; i<g->ip_size(); i++){
        const ACPIPPort *ip = & g->ip(i);
        if( ip->has_natdom() ){
            if( ! mydatacenter.compare(ip->natdom()) ) best = ip;
        }else{
            if( !best ) best = ip;
        }
    }
    if( best ){
        bestaddr.ipv4 = ntohl(best->ipv4());
        bestaddr.port = best->port();
        bestaddr.name = g->server_id();
    }
}

Peer::~Peer(){
    delete _gstatus;
}


void
Peer::update(const ACPMRMStatus *g){

    if( g->timestamp() < _gstatus->timestamp() ) return;

    *_gstatus = *g;
}

void
Peer::status_reply(ACPMRMStatus *g) const{

    *g = *_gstatus;
    g->set_via( myserver_id );

    string path = g->path();
    path.append(" ");
    path.append( myserver_id );
    g->set_path( path );
}

void
Peer::is_up(void){

    _num_fail = 0;
    _status   = PEER_STATUS_UP;
    _last_up  = _last_try = lr_now();

    _gstatus->set_status( 200 );
    _gstatus->set_timestamp( _last_up );
    _gstatus->set_lastup( _last_up );
}

void
Peer::is_down(void){

    _status = PEER_STATUS_DN;
    _gstatus->set_status( 0 );
    _gstatus->set_timestamp( _last_try );

}

void
Peer::maybe_down(void){

    _num_fail ++;
    _status   = PEER_STATUS_MAYBEDN;
    _last_try = lr_now();

    if( _num_fail > MAXFAIL ) is_down();

}

