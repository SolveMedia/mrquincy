/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-14 15:53 (EDT)
  Function: 

*/

#define CURRENT_SUBSYSTEM	'k'

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
#include <unistd.h>


#define TIMEOUT		15



static void *periodic(void*);
static void *kibitz_with_random_peer(void*);



void
kibitz_init(void){

    start_thread(periodic, 0);
}


static void *
periodic(void *notused){

    while(1){
        if( runmode.mode() == RUN_MODE_EXITING ) return 0;

        start_thread(kibitz_with_random_peer, 0);
        sleep(5);
    }
}

static NetAddr *
random_peer(void){

    // find a nice peer to talk to
    Peer *p = peerdb->random();
    if( p ) return & p->bestaddr;

    NetAddr *peer = 0;
    int n = 0;

    // use a seed
    for(NetAddr_List::iterator it=config->seedpeers.begin(); it != config->seedpeers.end(); it++){
        NetAddr *a = *it;

        if( a->is_self() ) continue;

        if( !peer ) peer = a;
        if( random_n(++n) == 0 ) peer = a;
    }

    return peer;
}


static void*
kibitz_with_random_peer(void *notused){
    NTD ntd;
    ACPMRMStatusRequest req;
    ACPMRMStatusReply   res;

    NetAddr *peer = random_peer();
    if( !peer ) return 0;

    DEBUG("kibitz with peer %s", peer->name.c_str());

    // connect
    // build request
    // send request
    // recv reply (list of all peers)
    // process (ignore info about self)

    int fd = tcp_connect(peer, TIMEOUT);
    if( fd < 0 ){
        DEBUG("cannot connect to %s", peer->name.c_str());
        peerdb->peer_dn( peer->name.c_str() );
        return 0;
    }

    DEBUG("connected");

    about_myself( req.mutable_myself() );

    ntd.fd = fd;

    write_request( &ntd, PHMT_MR_STATUS, &req, 0, TIMEOUT );
    int r = read_proto( &ntd, 0, TIMEOUT );

    DEBUG("read %d", r);

    if( r ){
        protocol_header *phi = (protocol_header*) ntd.gpbuf_in;
        res.ParsePartialFromArray( ntd.in_data(), phi->data_length );
        DEBUG("%s", res.ShortDebugString().c_str());

        if( ! res.IsInitialized() ){
            DEBUG("invalid request. missing required fields");
        }else{
            DEBUG("found %d peers", res.status_size());

            for(int i=0; i<res.status_size(); i++){
                ACPMRMStatus *s = res.mutable_status(i);
                peerdb->add_peer( s );
            }
        }
    }

    // update status of the random peer
    if( r ){
        peerdb->peer_up( peer->name.c_str() );
    }else{
        peerdb->peer_dn( peer->name.c_str() );
    }

    DEBUG("done");
    close(fd);
    return 0;
}
