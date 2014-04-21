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


void copy_status(const ACPMRMStatus *src, ACPMRMStatus *dst){

    if( src->has_hostname() )        dst->set_hostname(        src->hostname().c_str() );
    if( src->has_datacenter() )      dst->set_datacenter(      src->datacenter().c_str() );
    if( src->has_subsystem() )       dst->set_subsystem(       src->subsystem().c_str() );
    if( src->has_environment() )     dst->set_environment(     src->environment().c_str() );
    if( src->has_server_id() )       dst->set_server_id(       src->server_id().c_str() );
    if( src->has_path() )            dst->set_path(            src->path().c_str() );
    if( src->has_via() )             dst->set_via(             src->via().c_str() );
    if( src->has_status() )          dst->set_status(          src->status() );
    if( src->has_timestamp() )       dst->set_timestamp(       src->timestamp() );
    if( src->has_lastup() )          dst->set_lastup(          src->lastup() );
    if( src->has_sort_metric() )     dst->set_sort_metric(     src->sort_metric() );
    if( src->has_capacity_metric() ) dst->set_capacity_metric( src->capacity_metric() );
    if( src->has_boottime() )        dst->set_boottime(        src->boottime() );

    dst->clear_ip();
    for(int i=0; i< src->ip_size(); i++){
        const ACPIPPort *ps = & src->ip(i);
        ACPIPPort *pd = dst->add_ip();

        pd->set_ipv4( ps->ipv4() );
        pd->set_port( ps->port() );
        if( ps->has_natdom() )
            pd->set_natdom( ps->natdom().c_str() );
    }

}


Peer::Peer(const ACPMRMStatus *g){

    _gstatus  = new ACPMRMStatus;
    copy_status(g, _gstatus);
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
        bestaddr.name = g->server_id().c_str();
    }

}

Peer::~Peer(){
    delete _gstatus;
}


void
Peer::update(const ACPMRMStatus *g){

    if( g->timestamp() < _gstatus->timestamp() ) return;

    copy_status( g, _gstatus );

}

void
Peer::status_reply(ACPMRMStatus *g) const{

    copy_status( _gstatus, g);

    g->set_via( myserver_id.c_str() );

    string path = g->path().c_str();
    path.append(" ");
    path.append( myserver_id.c_str() );
    g->set_path( path.c_str() );
}

void
Peer::is_up(void){

    _num_fail = 0;
    _status   = PEER_STATUS_UP;
    _last_up  = _last_try = lr_now();
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

