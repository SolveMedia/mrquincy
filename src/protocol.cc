/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-19 13:20 (EDT)
  Function: network protocol

*/

#define CURRENT_SUBSYSTEM	'N'

#include "defs.h"
#include "diag.h"
#include "thread.h"
#include "config.h"
#include "lock.h"
#include "misc.h"
#include "hrtime.h"
#include "network.h"
#include "runmode.h"
#include "peers.h"

#include "std_reply.pb.h"
#include "heartbeat.pb.h"

#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <netdb.h>
#include <signal.h>
#include <string.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/loadavg.h>
#include <sys/sendfile.h>
#include <strings.h>

extern int udp4_fd, udp6_fd;


static int
parse_addr(const char *addr, NetAddr *na){
    struct in_addr a;
    char buf[64];

    strncpy(buf, addr, sizeof(buf));
    char *colon = index(buf, ':');
    int port = myport;

    if(colon){
        *colon = 0;
        port = atoi(colon + 1);
    }

    int v = inet_aton(buf, &a);
    if( !v ) return 0;		// RSN - look it up in peerdb?

    na->ipv4 = a.s_addr;
    na->port = port;

    return 1;
}


int
write_request(NTD *ntd, int reqno, google::protobuf::Message *g, int contlen, int to){
    protocol_header *pho = (protocol_header*) ntd->gpbuf_out;

    string gout;
    g->SerializeToString( &gout );
    int gsz = gout.length();
    DEBUG("send %s", g->ShortDebugString().c_str());

    pho->version        = PHVERSION;
    pho->type           = reqno;
    pho->flags          = PHFLAG_WANTREPLY;
    pho->msgidno        = random_n(0xFFFFFFFF);
    pho->auth_length    = 0;
    pho->content_length = contlen;
    pho->data_length    = gsz;

    cvt_header_to_network( pho );
    // send header
    int i = write_to(ntd->fd, (char*)pho, sizeof(protocol_header), to);
    if( i != sizeof(protocol_header) ) return -1;

    // send data
    i = write_to(ntd->fd, gout.c_str(), gsz, to);
    if( i != gsz ) return -1;

    return sizeof(protocol_header) + gsz;
}

int
write_reply(NTD *ntd, google::protobuf::Message *g, int contlen, int to){
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header *pho = (protocol_header*) ntd->gpbuf_out;

    if( !(phi->flags & PHFLAG_WANTREPLY) ) return 0;

    string gout;
    g->SerializeToString( &gout );
    int gsz = gout.length();

    ntd_copy_header_for_reply(ntd);
    pho->flags          = PHFLAG_ISREPLY;
    pho->data_length    = gsz;
    pho->content_length = contlen;
    pho->auth_length    = 0;

    cvt_header_to_network( pho );

    // send header
    int i = write_to(ntd->fd, (char*)pho, sizeof(protocol_header), to);
    if( i != sizeof(protocol_header) ) return -1;

    // send data
    i = write_to(ntd->fd, gout.c_str(), gsz, to);
    if( i != gsz ) return -1;

    return sizeof(protocol_header) + gsz;
}


int
make_request(NetAddr *addr, int reqno, google::protobuf::Message *g, int to){
    NTD ntd;
    ACPStdReply res;
    protocol_header *phi = (protocol_header*) ntd.gpbuf_in;
    int s = 0;

    int fd = tcp_connect(addr, to);
    ntd.fd = fd;

    // connect + send request
    s = write_request(&ntd, reqno, g, 0, to);
    if( s < 1 ){
        close(fd);
        return 0;
    }

    // read reply
    s = read_proto(&ntd, 0, to);
    close(fd);

    if( s < 1 ){
        return 0;
    }

    // check reply. all good?
    res.ParsePartialFromArray( ntd.in_data(), phi->data_length );
    DEBUG("recv l=%d, %s", phi->data_length, res.ShortDebugString().c_str());

    if( res.status_code() != 200 ){
        VERBOSE("todo create request failed: %s", res.status_message().c_str());
        return 0;
    }

    return 1;
}

int
make_request(const char *addr, int reqno, google::protobuf::Message *g, int to){
    NetAddr na;

    if( !parse_addr(addr, &na) ) return 0;
    return make_request(&na, reqno, g, to);
}

// toss + forget, do not wait for a response
static void
_toss_request(int fd, sockaddr_in *sa, int reqno, google::protobuf::Message *g){
    NTD ntd(0, 2048);
    protocol_header *pho = (protocol_header*) ntd.gpbuf_out;

    string gout;
    g->SerializeToString( &gout );
    int gsz = gout.length();
    ntd.out_resize( sizeof(protocol_header) + gsz );

    pho->version        = PHVERSION;
    pho->type           = reqno;
    pho->flags          = 0;
    pho->msgidno        = random_n(0xFFFFFFFF);
    pho->auth_length    = 0;
    pho->content_length = 0;
    pho->data_length    = gsz;

    cvt_header_to_network( pho );

    memcpy(ntd.out_data(), gout.c_str(), gsz);

    int efd = fd;
    if( fd == 0 ){
        // create transient socket
        efd = socket(PF_INET, SOCK_DGRAM, 0);
    }

    DEBUG("sending udp");
    sendto(efd, ntd.gpbuf_out, sizeof(protocol_header) + gsz, 0, (sockaddr*)sa, sizeof(sockaddr_in));

    if( fd == 0 ) close(efd );
}

void
toss_request(int fd, NetAddr *na, int reqno, google::protobuf::Message *g){
    struct sockaddr_in sa;

    // RSN - ipv6

    sa.sin_family      = AF_INET;
    sa.sin_port        = htons(na->port);
    sa.sin_addr.s_addr = na->ipv4;

    _toss_request(fd, &sa, reqno, g);
}

void
toss_request(int fd, const char *addr, int reqno, google::protobuf::Message *g){
    NetAddr na;

    if( !parse_addr(addr, &na) ) return;
    toss_request(fd, &na, reqno, g);
}


