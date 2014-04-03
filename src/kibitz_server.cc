/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-14 16:44 (EDT)
  Function: kibitz server

*/
#define CURRENT_SUBSYSTEM	'K'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "misc.h"
#include "network.h"
#include "hrtime.h"
#include "peers.h"

#include <stdio.h>
#include <strings.h>

#include "std_ipport.pb.h"
#include "mrmagoo.pb.h"

#define TIMEOUT 30

// request => client's own status
// reply   => list of all known peers

int
mr_status(struct NTD *ntd){
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;
    ACPMRMStatusRequest req;
    ACPMRMStatusReply   res;


    // parse request
    req.ParsePartialFromArray( ntd->in_data(), phi->data_length );
    DEBUG("l=%d, %s", phi->data_length, req.ShortDebugString().c_str());

    if( ! req.IsInitialized() ){
        DEBUG("invalid request. missing required fields");
        return 0;
    }

    DEBUG("recvd status");

    // update sceptical
    if( req.has_myself() )
        peerdb->add_sceptical( req.mutable_myself() );


    if( !(phi->flags & PHFLAG_WANTREPLY) ) return 0;

    // build reply - add everyone's statuses
    peerdb->reply_peers( &res );
    ACPMRMStatus *s = res.add_status();
    about_myself(s);

    if( !res.IsInitialized() ){
        BUG("incomplete status message: %s", res.InitializationErrorString().c_str() );
        return 0;
    }

    // serialize + reply
    write_reply(ntd, &res, 0, TIMEOUT);

    // nothing else to send
    return 0;
}

