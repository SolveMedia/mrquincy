/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-17 12:51 (EDT)
  Function: 

*/
#define CURRENT_SUBSYSTEM	'P'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "misc.h"
#include "network.h"
#include "runmode.h"
#include "thread.h"
#include "peers.h"

#include "mrmagoo.pb.h"

#include <strings.h>

#include <sstream>
#include <iomanip>
using std::ostringstream;



#define KEEPDOWN	300	// keep data about down servers for how long?
#define KEEPLOST	600	// keep data about servers we have not heard about for how long?
#define KEEPDEAD	300	// keep data in the graveyard
#define HYSTERESIS	 30

PeerDB *peerdb = 0;

static void *peerdb_periodic(void*);


void
peerdb_init(void){

    peerdb = new PeerDB;

    start_thread(peerdb_periodic, 0);
}

static void *
peerdb_periodic(void *notused){

    while(1){
        if( runmode.mode() == RUN_MODE_EXITING ) return 0;
        peerdb->cleanup();
        sleep(15);
    }
}

static void
format_dt(int sec, ostringstream &b){

    int d = sec / 86400;
    sec %= 86400;

    int h = sec / 3600;
    sec %= 3600;

    int m = sec / 60;
    sec %= 60;

    char buf[64];

    if( !d ){
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d", h,m,sec);
    }else if( d < 14 ){
        snprintf(buf, sizeof(buf), "%dd+%02dh", d,h);
    }else{
        snprintf(buf, sizeof(buf), "%ddays", d);
    }

    b << std::setw(16) << buf;
}


// report peers, status, + metrics - for diag
int
PeerDB::report(NTD *ntd){

    ostringstream out;
    _lock.r_lock();

    out << "# name                      status    load      disk upd          uptime\n";

    for(list<Peer*>::const_iterator it=_allpeers.begin(); it != _allpeers.end(); it++){
        const Peer *p = *it;

        out << std::setw(30) << std::left  << p->_id
            << std::setw(4)  << std::right << p->_gstatus->status()
            << std::setw(8)  << std::right << p->_gstatus->sort_metric()
            << std::setw(10) << std::right << p->_gstatus->capacity_metric()
            << std::setw(4)  << std::right << (lr_now() - p->_gstatus->lastup());

        format_dt( lr_now() - p->_gstatus->boottime(), out );

        out << "\n";

    }
    _lock.r_unlock();

    int sz = out.str().length();
    ntd->out_resize( sz + 1 );
    memcpy(ntd->gpbuf_out, out.str().c_str(), sz);

    return sz;
}

void
PeerDB::getall(list<NetAddr> *l){

    _lock.r_lock();

    for(list<Peer*>::const_iterator it=_allpeers.begin(); it != _allpeers.end(); it++){
        const Peer *p = *it;
        // skip down servers
        if( p->status() == PEER_STATUS_DN ) continue;
        if( p->_gstatus->status() != 200 )  continue;
        l->push_back( p->bestaddr );
    }
    _lock.r_unlock();

    // and myself
    l->push_back( mynetaddr );
}

void
PeerDB::cleanup(void){
    hrtime_t now = lr_now();
    list<Peer*>  deathrow;

    // clean old down peers
    // clean graveyard

    _lock.w_lock();
    for(list<Peer*>::iterator it=_allpeers.begin(); it != _allpeers.end(); it++){
        Peer *p = *it;

        if( p->_gstatus->timestamp() < now - KEEPLOST || p->_gstatus->lastup() < now - KEEPDOWN ){
            // don't modify the list while iterating over it, push onto a tmp list
            deathrow.push_back(p);
        }
    }

    for(list<Peer*>::iterator it=deathrow.begin(); it != deathrow.end(); it++){
        Peer *p = *it;
        _kill(p);
    }
    _lock.w_unlock();


    deathrow.clear();

    // delete anything that's been in the graveyard too long

    _lock.w_lock();
    for(list<Peer*>::iterator it=_graveyard.begin(); it != _graveyard.end(); it++){
        Peer *p = *it;

        if( p->_last_try < now - KEEPDEAD ){
            deathrow.push_back(p);
        }
    }
    for(list<Peer*>::iterator it=deathrow.begin(); it != deathrow.end(); it++){
        Peer *p = *it;

        DEBUG("deleting %s", p->_id );
        _graveyard.remove(p);
        delete p;
    }

    _lock.w_unlock();
}

// find a random peer to kibitz with
Peer *
PeerDB::random(void){
    Peer *pmaybe=0, *pall=0, *pscept=0;
    int   nmaybe=0,  nall=0,  nscept=0;


    // prefer:
    //  maybe down
    //  sceptical
    //  sometimes a seed (return 0)
    //  any known peer

    _lock.r_lock();

    for(list<Peer*>::iterator it=_allpeers.begin(); it != _allpeers.end(); it++){
        Peer *p = *it;

        if( p->status() == PEER_STATUS_MAYBEDN ){
            nmaybe++;
            if( !pmaybe || (random_n(nmaybe) == 0) ) pmaybe = p;
        }else{
            nall++;
            if( !pall   || (random_n(nall) == 0) )   pall   = p;
        }
    }

    if( !pmaybe ){
        for(list<Peer*>::iterator it=_sceptical.begin(); it != _sceptical.end(); it++){
            Peer *p = *it;

            nscept++;
            if( !pscept || (random_n(nscept) == 0) ) pscept = p;
        }
    }

    _lock.r_unlock();

    if( pmaybe ) return pmaybe;
    if( pscept ) return pscept;

    // sometimes, use seed. so we can recover from a partition
    if( random_n(2 * nall + 2) == 0 ) return 0;

    return pall;
}

// add all peers to reply
void
PeerDB::reply_peers(ACPMRMStatusReply *res){

    _lock.r_lock();

    for(list<Peer*>::const_iterator it=_allpeers.begin(); it != _allpeers.end(); it++){
        const Peer *p = *it;
        ACPMRMStatus *g = res->add_status();
        p->status_reply( g );
    }

    _lock.r_unlock();
}

Peer *
PeerDB::_find(const char *id){
    Peer *peer = 0;

    for(list<Peer*>::iterator it=_allpeers.begin(); it != _allpeers.end(); it++){
        Peer *p = *it;
        if( !strcmp(p->_id, id) ){
            peer = p;
            break;
        }
    }

    if( !peer ){
        for(list<Peer*>::iterator it=_sceptical.begin(); it != _sceptical.end(); it++){
            Peer *p = *it;
            if( !strcmp(p->_id, id) ){
                peer = p;
                break;
            }
        }
    }

    return peer;
}

Peer *
PeerDB::find(const char *id){

    _lock.r_lock();
    Peer *peer = _find(id);
    _lock.r_unlock();
    return peer;
}

NetAddr *
PeerDB::find_addr(const char *id){

    Peer *peer = find(id);
    if(peer) return & peer->bestaddr;

    // is it me?
    if( !myserver_id.compare(id) ) return &mynetaddr;
    return 0;
}

void
PeerDB::_upgrade(Peer *p){

    // remove from _sceptical
    // add to _allpeers
    DEBUG("upgrade %s", p->_id );

    _lock.w_lock();

    _sceptical.remove(p);
    _allpeers.push_back(p);
    if( p->_status == PEER_STATUS_SCEPTICAL ) p->_status = PEER_STATUS_UP;

    _lock.w_unlock();
}

void
PeerDB::_kill(Peer *p){

    // move to graveyard
    VERBOSE("removing old peer %s", p->_id );

    _lock.w_lock();

    _allpeers.remove(p);
    _sceptical.remove(p);
    _graveyard.push_back(p);

    p->_status   = PEER_STATUS_DEAD;
    p->_last_try = lr_now();

    _lock.w_unlock();

}


static int
_update_ok(ACPMRMStatus *g){

    if( !g->IsInitialized() )                                 return 0; // corrupt
    if( ! myserver_id.compare( g->server_id() ) )             return 0; // don't want my own info
    if( config->environment.compare( g->environment() ) )     return 0; // same env?
    if( g->subsystem().compare("mrquincy") )                  return 0; // and subsystem?
    if( g->timestamp() < lr_now() - KEEPLOST  )   	      return 0; // we'd just toss it out
    if( g->lastup()    < lr_now() - KEEPDOWN  )   	      return 0; // we'd just toss it out

    return 1;
}

void
PeerDB::add_peer(ACPMRMStatus *g){

    if( !_update_ok(g) ) return;

    const string *id = & g->server_id();
    Peer *p = find( id->c_str() );

    DEBUG("add peer %s", id->c_str());

    if( p ){
        if( p->status() == PEER_STATUS_SCEPTICAL ){
            _upgrade(p);
            VERBOSE("discovered new peer %s", id->c_str());
        }

        DEBUG("update existing %s", id->c_str());
        // update existing entry
        _lock.w_lock();
        p->update( g );
        _lock.w_unlock();
        return;
    }

    VERBOSE("discovered new peer %s", id->c_str());

    _lock.w_lock();

    // check again while locked
    p = _find( id->c_str() );

    if( !p ){
        // add new entry

        DEBUG("add new peer %s", id->c_str());
        p = new Peer(g);
        _allpeers.push_back( p );
    }

    _lock.w_unlock();
}

void
PeerDB::add_sceptical(ACPMRMStatus *g){

    if( !_update_ok(g) ) return;

    const string *id = & g->server_id();
    Peer *p = find( id->c_str() );

    // already known
    if( p ) return;

    DEBUG("sceptical update from %s", id->c_str());

    _lock.w_lock();

    // check again while locked
    p = _find( id->c_str() );

    if( !p ){
        // add new entry

        DEBUG("add new scept %s", id->c_str());
        p = new Peer(g);
        p->_status = PEER_STATUS_SCEPTICAL;
        _sceptical.push_back( p );
    }

    _lock.w_unlock();
}

void
PeerDB::peer_up(const char *id){

    DEBUG("up %s", id);
    _lock.w_lock();
    Peer *p = _find( id );
    if( p ){
        int os = p->_status;
        p->is_up();
        if( os == PEER_STATUS_SCEPTICAL ) _upgrade(p);
        if( os != PEER_STATUS_UP ) VERBOSE("peer %s is now up", id);
    }
    _lock.w_unlock();
}

void
PeerDB::peer_dn(const char *id){

    DEBUG("dn %s", id);
    _lock.w_lock();
    Peer *p = _find( id );
    if( p ){
        int os = p->_status;
        p->maybe_down();
        if( os == PEER_STATUS_SCEPTICAL ) _kill(p);
        if( p->status() == PEER_STATUS_DN && os != PEER_STATUS_DN ) VERBOSE("peer %s is now down", id);
    }
    _lock.w_unlock();
}

