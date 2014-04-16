/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-18 17:16 (EDT)
  Function: xfer files from remote to local

*/

#define CURRENT_SUBSYSTEM	'x'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "misc.h"
#include "network.h"
#include "runmode.h"
#include "thread.h"
#include "lock.h"
#include "peers.h"
#include "queued.h"

#include "mrmagoo.pb.h"
#include "scrible.pb.h"
#include "std_reply.pb.h"

#include <sstream>
using std::ostringstream;

#define MAXXFER		5	// RSN - config
#define TIMEOUT		15

class Xfer {
public:
    ACPMRMFileXfer  _g;
    hrtime_t        _created;
    const char     *_status;
    int             _filesize;

    Xfer() { _status = "PENDING"; _created = lr_now(); _filesize = 0; }
};



static void *xfer_periodic(void*);
static void *do_xfer(void*);
static int  try_xfer(Xfer *, int);

extern int scriblr_save_file(int fd, const string *filename, int size, string *hash, int to);

static QueuedXfer	xferq;


void
xfer_init(void){
    start_thread(xfer_periodic, 0);
}

static void *
xfer_periodic(void *notused){

    while(1){
        // can we start any queued xfers?
        xferq.start_more(MAXXFER);
        sleep(5);
    }
}

// handle xfer request from network
int
handle_xfer(NTD *ntd){
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header *pho = (protocol_header*) ntd->gpbuf_out;
    Xfer  *req = new Xfer;

    // parse request
    req->_g.ParsePartialFromArray( ntd->in_data(), phi->data_length );
    DEBUG("l=%d, %s", phi->data_length, req->_g.ShortDebugString().c_str());

    if( ! req->_g.IsInitialized() ){
        DEBUG("invalid request. missing required fields");
        return 0;
    }

    DEBUG("recvd xfer request");

    xferq.start_or_queue( (void*)req, 0, MAXXFER );

    return reply_ok(ntd);
}

void
json_xfer(string *dst){
    xferq.json(dst);
}

void
QueuedXfer::json1(const char *st, void *x, string *dst){
    Xfer *g = (Xfer*)x;

    ostringstream b;

    b << "{\"jobid\": \""       << g->_g.jobid()   << "\", "
      << "\"copyid\": \""       << g->_g.copyid()  << "\", "
      << "\"status\": \""       << st              << "\", "
      << "\"start_time\": "     << g->_created
      << "}";

    dst->append(b.str().c_str());
}

bool
QueuedXfer::same(void *xa, void *xb){
    Xfer *ga = (Xfer*)xa;
    Xfer *gb = (Xfer*)xb;

    const string *id = & ga->_g.copyid();
    return id->compare( gb->_g.copyid() ) ? 0 : 1;
}

void
QueuedXfer::start(void *xg){
    Xfer *g = (Xfer*)xg;

    start_thread(do_xfer, (void*)g);
}

void
QueuedXfer::send_status(void *xg){
    Xfer *g = (Xfer*)xg;
    ACPMRMActionStatus st;

    if( !g->_g.has_master() ) return;

    st.set_jobid( g->_g.jobid().c_str() );
    st.set_xid( g->_g.copyid().c_str() );
    st.set_phase( g->_status );

    toss_request( udp4_fd, g->_g.master().c_str(), PHMT_MR_XFERSTATUS, &st);
}

// the final status goes over tcp
static void
send_final_status(const Xfer *g){
    ACPMRMActionStatus st;

    if( !g->_g.has_master() || g->_g.master().empty() ) return;

    st.set_jobid( g->_g.jobid().c_str() );
    st.set_xid( g->_g.copyid().c_str() );
    st.set_phase( g->_status );
    st.set_final_amount( g->_filesize );

    DEBUG("sending final status to %s", g->_g.master().c_str());

    make_request(g->_g.master().c_str(), PHMT_MR_TASKSTATUS, &st, TIMEOUT );
}


static void *
do_xfer(void *x){
    Xfer *g = (Xfer*)x;
    bool ok = 1;

    g->_status = "RUNNING";
    xferq.send_status(x);

    // try each location, several times
    int tries = 2 * g->_g.location_size() + 1;

    for(int i=0; i<tries; i++){
        ok = try_xfer(g, i % g->_g.location_size());
        if( ok ) break;
        sleep(5);	// maybe the problem will clear
    }

    DEBUG("xfer done");

    if( ok )
        g->_status = "FINISHED";
    else
        g->_status = "FAILED";

    send_final_status(g);

    xferq.done(x);
    delete g;

    // start another one
    xferq.start_more(MAXXFER);
}

static int
try_xfer(Xfer *g, int l){
    NTD ntd;
    protocol_header *phi = (protocol_header*) ntd.gpbuf_in;
    ACPScriblRequest req;
    ACPScriblReply   res;

    DEBUG("trying xfer %s", g->_g.jobid().c_str());

    // find addr of remote
    const string *location = & g->_g.location(l);
    NetAddr *na = peerdb->find_addr(location->c_str() );

    if( !na ){
        VERBOSE("cannot find xfer peer %s", location->c_str());
        // extra delay, maybe it will show up
        sleep(2);
        return 0;
    }

    // build get req
    req.set_filename( g->_g.filename().c_str() );

    // connect
    int fd = tcp_connect(na, TIMEOUT);
    if( fd<0 ){
        VERBOSE("xfer cannot connect to %s", location->c_str());
        return 0;
    }

    ntd.fd = fd;
    // send request
    int s = write_request(&ntd, PHMT_SCRIB_GET, &req, 0, TIMEOUT);
    if( s<1 ){
        VERBOSE("xfer write failed");
        close(fd);
        return 0;
    }

    // recv response
    s = read_proto(&ntd, 0, TIMEOUT);
    if( s<1 ){
        VERBOSE("xfer read failed");
        close(fd);
        return 0;
    }

    // parse response
    res.ParsePartialFromArray( ntd.in_data(), phi->data_length );
    DEBUG("l=%d, %s", phi->data_length, res.ShortDebugString().c_str());

    if( res.status_code() != 200 ){
        VERBOSE("xfer request failed: %s", res.status_message().c_str());
        close(fd);
        return 0;
    }

    // stream to disk
    string *dstfile;
    if( g->_g.has_dstname() )
        dstfile = g->_g.mutable_dstname();
    else
        dstfile = g->_g.mutable_filename();

    s = scriblr_save_file(fd, dstfile, phi->content_length, res.mutable_hash_sha1(), TIMEOUT );
    close(fd);

    if( !s ){
        VERBOSE("xfer save file failed");
        return 0;
    }

    g->_filesize = phi->content_length;
    return 1;

}

