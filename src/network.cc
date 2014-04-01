/*
  Copyright (c) 2008 by Jeff Weisberg
  Author: Jeff Weisberg <jaw @ tcp4me.com>
  Created: 2008-Dec-28 14:40 (EST)
  Function: network requests
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


#define READ_TIMEOUT	30
#define WRITE_TIMEOUT	30

static int handle_unknown(NTD*);
static int handle_status(NTD*);
static int handle_hbreq(NTD*);
static int report_status(NTD*);
static int report_peers(NTD*);
static int report_json(NTD*);

extern void install_handler(int, void(*)(int));
extern int scriblr_put(NTD*);
extern int scriblr_get(NTD*);
extern int scriblr_del(NTD*);
extern int scriblr_chk(NTD*);
extern int mr_status(NTD*);
extern int handle_xfer(NTD*);
extern int handle_task(NTD*);
extern int handle_taskabort(NTD*);
extern int handle_job(NTD*);
extern int handle_jobabort(NTD*);
extern int handle_jobstatus(NTD*);
extern int handle_mrdelete(NTD*);

extern void json_task(string *);
extern void json_xfer(string *);
extern void json_job(string *);

void hexdump(const char *, const uchar *, int);

static struct {
    int (*fnc)(NTD*);
    // ... ?
} request_handler[] = {
    { handle_status  },		// status
    { 0 },
    { handle_hbreq   },		// heartbeat request
    { 0 },
    { 0 },
    { 0 },	// 5
    { 0 },
    { 0 },
    { 0 },
    { 0 },
    { 0 },	// 10

    { scriblr_put },
    { scriblr_get },
    { scriblr_del },
    { scriblr_chk },

    { handle_job },
    { handle_task },
    { handle_jobabort },
    { handle_taskabort },
    { handle_jobstatus },
    { handle_xfer },
    { handle_mrdelete },
    { 0 }, //mr_diagmsg },
    { handle_jobstatus },
    { mr_status },		// kibitz

    // ...
};

static struct {
    char *url;
    int (*fnc)(NTD *);
} http_handler[] = {
    { "/status", report_status },
    { "/peers",  report_peers  },
    { "/json",   report_json },

};



// where am I? for heartbeats
int      myport = 0;
uint32_t myipv4 = 0;
char     myhostname[256];
string   myserver_id;
string   mydatacenter;
string   myipandport;
NetAddr  mynetaddr;


int tcp4_fd = 0, tcp6_fd = 0, udp4_fd = 0, udp6_fd = 0;


void
cvt_header_from_network(protocol_header *ph){

    ph->version        = ntohl(ph->version);
    ph->type           = ntohl(ph->type);
    ph->msgidno        = ntohl(ph->msgidno);
    ph->auth_length    = ntohl(ph->auth_length);
    ph->data_length    = ntohl(ph->data_length);
    ph->content_length = ntohl(ph->content_length);
    ph->flags          = ntohl(ph->flags);
}

void
cvt_header_to_network(protocol_header *ph){

    ph->version        = htonl(ph->version);
    ph->type           = htonl(ph->type);
    ph->msgidno        = htonl(ph->msgidno);
    ph->auth_length    = htonl(ph->auth_length);
    ph->data_length    = htonl(ph->data_length);
    ph->content_length = htonl(ph->content_length);
    ph->flags          = htonl(ph->flags);
}


static void
sigsegv(int sig){
    int i;
    pthread_t self = pthread_self();

    DEBUG("caught segv");

    // attempt to abort the thread and continue

    // if there is a lock being held, we are f***ed.
    // if we winddown+restart, we could end up with no running dancrs (aka f***ed)

    // put the system into a fast windown+restart
    // if we are not hung, puds::janitor will cancel the shutdown

    runmode.errored();

    BUG("segv");
    exit(EXIT_ERROR_RESTART);

}

static void
sigother(int sig){
    int i;
    pthread_t self = pthread_self();

    runmode.errored();

    BUG("caught sig %d", sig);
    sleep(1);
}

/****************************************************************/

int
read_to(int fd, char *buf, int len, int to){
    struct pollfd pf[1];

    pf[0].fd = fd;
    pf[0].events = POLLIN;
    pf[0].revents = 0;

    // timeout is msec
    int r = poll( pf, 1, to * 1000 );
    // 0 => TO, -1 => error, else # fds

    if( r < 0 ) return -1;
    if( r == 0 ){
        errno = ETIME;
        return -1;
    }

    if( pf[0].revents & POLLIN ){
        int r = read(fd, buf, len);
        DEBUG("read %d -> %d", len, r);
        return r;
    }

    return 0;
}

int
write_to(int fd, const char *buf, int len, int to){
    struct pollfd pf[1];
    int sent = 0;

    while( sent != len ){
        pf[0].fd = fd;
        pf[0].events = POLLOUT;
        pf[0].revents = 0;

        // timeout is msec
        int r = poll( pf, 1, to * 1000 );
        // 0 => TO, -1 => error, else # fds

        if( r < 0 ) return -1;
        if( r == 0 ){
            errno = ETIME;
            return -1;
        }

        if( pf[0].revents & POLLOUT ){
            int s = write(fd, buf, len);
            if( s == -1 && errno == EAGAIN ) continue;
            if( s < 1 ) return -1;
            sent += s;
        }
    }

    return sent;
}

int
sendfile_to(int dst, int src, int len, int to){
    struct pollfd pf[1];
    off_t off = 0;

    while( off != len ){
        pf[0].fd = dst;
        pf[0].events = POLLOUT;
        pf[0].revents = 0;

        int r = poll( pf, 1, to * 1000 );

        if( r < 0 ) return -1;
        if( r == 0 ){
            errno = ETIME;
            return -1;
        }

        if( pf[0].revents & POLLOUT ){
            int s = sendfile(dst, src, &off, len - off);
            DEBUG("sendfile %d -> %d %d, %d", len, s, errno, off);
            if( s == -1 && errno == EAGAIN ) continue;
            if( s < 1 ) return -1;
        }
    }

    return off;
}

void
init_tcp(int fd){

    int size = 1024 * 1024;	// is default max. ndd -set /dev/tcp tcp_max_buf X" to increase

    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size));
    // disable nagle
    int i = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &i, sizeof(i));
    // make non-blocking
    fcntl(fd, F_SETFL, O_NDELAY);
}

int
tcp_connect(NetAddr *na, int to){
    struct sockaddr_in sa;
    struct pollfd pf[1];

    // RSN - ipv6

    sa.sin_family      = AF_INET;
    sa.sin_port        = htons(na->port);
    sa.sin_addr.s_addr = na->ipv4;

    DEBUG("connect %s %d", inet_ntoa(sa.sin_addr), na->port);

    int fd = socket(PF_INET, SOCK_STREAM, 0);
    if( fd == -1 ){
	FATAL("cannot create tcp4 socket");
    }

    init_tcp(fd);

    int i = connect(fd, (sockaddr*)&sa, sizeof(sa));
    if( i == -1 && errno != EINPROGRESS ){
        DEBUG("cannot connect");
        close(fd);
        return -1;
    }

    pf[0].fd = fd;
    pf[0].events = POLLOUT;
    pf[0].revents = 0;

    int r = poll( pf, 1, to * 1000 );
    DEBUG("r %d -> %x", r, pf[0].revents);
    if( r <= 0 ){
        close(fd);
        if( r == 0 ) errno = ETIME;
        return -1;
    }

    // man page says:
    //     int getsockopt(int s, int level, int optname, void *optval, int *optlen);
    // compiler complains:
    //     error:   initializing argument 5 of `int getsockopt(int, int, int, void*, socklen_t*)'

    int opt, optlen = sizeof(opt);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, (void*)&opt, (socklen_t*)&optlen);

    if( (pf[0].revents & (POLLERR | POLLHUP | POLLNVAL)) || opt ){
        DEBUG("connect failed %x, %x", pf[0].revents, opt);
        close(fd);
        return -1;
    }

    return fd;
}


/****************************************************************/
// quick+dirty. not fully RFC compliant
// just enough to export some data to argus
static void
network_http(NTD *ntd){

    // parse url
    char *url = ntd->gpbuf_in + 4;
    int urllen = 0;

    for(int i=0; url[i] != ' ' && url[i] != '\n' && i<ntd->in_size-4; i++){
        urllen ++;
    }
    url[urllen] = 0;

    DEBUG("url %s", url);

    int (*fnc)(NTD*) = 0;
    for(int i=0; i<ELEMENTSIN(http_handler); i++){
        if( !strcmp(url, http_handler[i].url) ){
            fnc = http_handler[i].fnc;
            break;
        }
    }

    if( !fnc ){
#       define RESPONSE "HTTP/1.0 404 Not Found\r\nServer: AC/MrQuincy\r\n\r\n"
        write_to(ntd->fd, RESPONSE, sizeof(RESPONSE)-1, WRITE_TIMEOUT);
#       undef  RESPONSE
        return;
    }

    // process req
    int rl = fnc(ntd);

    // respond
#   define RESPONSE "HTTP/1.0 200 OK\r\nServer: AC/MrQuincy\r\nContent-Type: text/plain\r\n\r\n"
    write_to(ntd->fd, RESPONSE, sizeof(RESPONSE)-1, WRITE_TIMEOUT);
#   undef  RESPONSE

    write_to(ntd->fd, ntd->gpbuf_out, rl, WRITE_TIMEOUT);
}


static int
network_process(NTD *ntd){
    protocol_header *ph = (protocol_header*) ntd->gpbuf_in;

    if( !strncmp( ntd->gpbuf_in, "GET ", 4) ){
        network_http(ntd);
        return 0;
    }

    if( ph->version != PHVERSION ) return 0;

    int (*fnc)(NTD*);
    int mt = ph->type;
    DEBUG("processing request");
    if( mt >= ELEMENTSIN(request_handler) ){
        fnc = handle_unknown;
    }else{
        fnc = request_handler[mt].fnc;
    }
    if( !fnc ) fnc = handle_unknown;

    return fnc(ntd);
}

int
read_proto(NTD *ntd, int reqp, int to){
    protocol_header *ph = (protocol_header*) ntd->gpbuf_in;

    // read header
    int i = read_to(ntd->fd, ntd->gpbuf_in, sizeof(protocol_header), READ_TIMEOUT);

    if( reqp && i > 4 && !strncmp( ntd->gpbuf_in, "GET ", 4) ){
        DEBUG("http request");
        int l = i;

        // read http request
        while(1){
            int rl = ntd->in_size - l;
            if( !rl ) return 0;
            i = read_to(ntd->fd, ntd->gpbuf_in + l, rl, READ_TIMEOUT);
            if( i > 0 ) l += i;

            // do we have entire req?
            if( strstr(ntd->gpbuf_in, "\r\n\r\n") ) break;
            if( strstr(ntd->gpbuf_in, "\n\n") )     break;

            if( !i ){
                return 0; // eof
            }
            if( i < 1 ){
		if( errno == EINTR ) continue;
		DEBUG("read error");
		return 0;
	    }
        }

        network_http(ntd);
        return 0;
    }

    if( i != sizeof(protocol_header) ){
	DEBUG("read header failed");
	return 0;
    }

    // convert buffer from network byte order
    cvt_header_from_network( ph );

    // validate
    if( ph->version != PHVERSION ){
	VERBOSE("invalid request recvd. unknown version(%d)", ph->version);
	return 0;
    }

    if( ph->data_length > ntd->in_size - sizeof(protocol_header) ){
        ntd->in_resize( ph->data_length + sizeof(protocol_header) );
    }

    // read gpb
    if( ph->data_length ){

        DEBUG("reading protobuf");

        int len = 0;
        char *buf = ntd->gpbuf_in + sizeof(protocol_header);

        while( len < ph->data_length ){
            int rlen = ph->data_length - len;
            i = read_to(ntd->fd, buf + len, rlen, READ_TIMEOUT);
            if( i < 1 ){
                if( errno == EINTR ) continue;
                DEBUG("read error");
                return 0;
            }
            len += i;
        }

        ntd->have_data = 1;
    }

    return 1;

}

static void *
network_tcp_read_req(void* xfd){
    int fd = (int)xfd;
    NTD ntd;

    ntd.fd = fd;
    ntd.is_tcp = 1;

    int r = read_proto(&ntd, 1, READ_TIMEOUT);
    if( r ){
        int rl = network_process(&ntd);
        if( rl ){
            int i = write_to(fd, ntd.gpbuf_out, rl, WRITE_TIMEOUT);
            if( i != rl )
                DEBUG("write response failed %d", errno);
        }
    }

    close(fd);
    return 0;
}


static void *
network_tcp4(void *notused){
    struct sockaddr_in sa;
    socklen_t l = sizeof(sa);

    while(1){
	if( runmode.mode() == RUN_MODE_EXITING ) break;

	int nfd = accept(tcp4_fd, (sockaddr *)&sa, &l);

	if(nfd == -1){
	    DEBUG("accept failed");
	    continue;
	}

	if( !config->check_acl( (sockaddr*)&sa ) ){
	    VERBOSE("network connection refused from %s", inet_ntoa(sa.sin_addr) );
	    close(nfd);
	    continue;
	}

	DEBUG("new connection from %s", inet_ntoa(sa.sin_addr) );

        init_tcp(nfd);

        start_thread(network_tcp_read_req, (void*)nfd);
    }

    close(tcp4_fd);
    tcp4_fd = 0;
}

static void *
network_udp4(void *notused){
    NTD ntd;
    struct sockaddr_in sa;
    socklen_t l = sizeof(sa);
    protocol_header *ph = (protocol_header*) ntd.gpbuf_in;

    ntd.fd = udp4_fd;

    while(1){
	if( runmode.mode() == RUN_MODE_EXITING ) break;

        ntd.have_data = 0;
        int i = recvfrom(udp4_fd, ntd.gpbuf_in, ntd.in_size, 0, (sockaddr*)&sa, &l);

	if( !config->check_acl( (sockaddr*)&sa ) ){
	    VERBOSE("network connection refused from %s", inet_ntoa(sa.sin_addr) );
	    continue;
	}

	DEBUG("new udp from %s", inet_ntoa(sa.sin_addr) );

        // hexdump("recvd ", (uchar*)ntd.gpbuf_in, i);

        cvt_header_from_network( (protocol_header*) ntd.gpbuf_in );

        if( ph->data_length < ntd.in_size - sizeof(protocol_header) ){
            ntd.have_data = 1;
        }

        int rl = network_process(&ntd);
        if( rl ) sendto(udp4_fd, ntd.gpbuf_out, rl, 0, (sockaddr*)&sa, sizeof(sa));
    }

    close(udp4_fd);
    udp4_fd = 0;
}

void
network_init(void){
    char buf[16];
    struct sockaddr_in sa;
    struct hostent *he;

    myport = config->port_mrquincy;
    if( !myport ){
	FATAL("cannot determine port to use");
    }
    gethostname( myhostname, sizeof(myhostname));
    he = gethostbyname( myhostname );
    if( !he || !he->h_length ){
	FATAL("cannot determine my ipv4 addr");
    }
    myipv4 = ((struct in_addr *)*he->h_addr_list)->s_addr;

    myipandport = inet_ntoa(*((struct in_addr *)he->h_addr_list[0]));
    myipandport.append(":");
    snprintf(buf, sizeof(buf), "%d", myport);
    myipandport.append(buf);

    DEBUG("hostname %s, ip %x => %s", myhostname, myipv4, myipandport.c_str());

    // mrm[/env]@hostname
    myserver_id = "mrm";
    if( config->environment.compare("prod") ){
        myserver_id.append("/");
        myserver_id.append(config->environment);
    }
    myserver_id.append("@");

    // find + remove domain (2nd dot from end)
    int dot1=0, dot2=0, dot3=0;
    int hlen = strlen(myhostname);
    for(int i=hlen-1; i>=0; i--){
        if( myhostname[i] == '.' ){
            if( dot2 ){
                dot3 = i;
                break;
            }
            if( dot1 ){
                dot2 = i;
                continue;
            }
            dot1 = i;
        }
    }

    // append local hostname
    if(!dot2)  dot2 = dot1;
    if( dot2 )
        myserver_id.append(myhostname, dot2);
    else
        myserver_id.append(myhostname);

    DEBUG("server id: %s", myserver_id.c_str());

    // datacenter
    if( dot3 ){
        mydatacenter.append(myhostname+dot3+1, dot2-dot3-1);
    }else{
        mydatacenter.append(myhostname + (dot2 ? dot2+1 : 0));
    }
    DEBUG("datacenter: %s", mydatacenter.c_str());

    mynetaddr.ipv4 = myipv4;
    mynetaddr.port = myport;
    mynetaddr.name = myserver_id;

    // open sockets
    tcp4_fd = socket(PF_INET, SOCK_STREAM, 0);
    if( tcp4_fd == -1 ){
	FATAL("cannot create tcp4 socket");
    }

    sa.sin_family = AF_INET;
    sa.sin_port   = htons(myport);
    sa.sin_addr.s_addr = INADDR_ANY;

    int i = 1;
    setsockopt(tcp4_fd, SOL_SOCKET, SO_REUSEADDR, &i, sizeof(i));

    i = bind(tcp4_fd, (sockaddr*)&sa, sizeof(sa));
    if( i == -1 ){
	FATAL("cannot bind to tcp4 port");
    }
    listen(tcp4_fd, 10);

    udp4_fd = socket(PF_INET, SOCK_DGRAM, 0);
    if( udp4_fd == -1 ){
	FATAL("cannot create udp4 socket");
    }
    i = 1;
    setsockopt(udp4_fd, SOL_SOCKET, SO_REUSEADDR, &i, sizeof(i));

    i = bind(udp4_fd, (sockaddr*)&sa, sizeof(sa));
    if( i == -1 ){
	FATAL("cannot bind to udp4 port");
    }

    // RSN - ipv6


    // set descriptor limits - this will be increased in child tasks
    struct rlimit fdrl;
    getrlimit(RLIMIT_NOFILE, &fdrl);
    fdrl.rlim_cur = 256;
    setrlimit(RLIMIT_NOFILE, &fdrl);
    DEBUG("limit fd %d, %d", fdrl.rlim_cur, fdrl.rlim_max);

    // install alarm handler
    install_handler( SIGALRM, sigother );
    // install segv handler
    install_handler( SIGSEGV, sigsegv  );
    install_handler( SIGABRT, sigother );
    install_handler( SIGFPE,  sigother );
    install_handler( SIGBUS,  sigother );

    VERBOSE("starting network on tcp/%d as id %s (%s)", myport, myserver_id.c_str(), config->environment.c_str());

    for(i=0; i<config->threads; i++){
        start_thread(network_tcp4, 0);
        start_thread(network_udp4, 0);
        // ...
    }
}

void
network_manage(void){
    int nbusy, i, nt;
    time_t prevt = lr_now(), nowt;
    long long preq = 0;

    while(1){
        nowt = lr_now();

        switch(runmode.mode()){
        case RUN_MODE_EXITING:

	    if( tcp4_fd || udp4_fd || tcp6_fd || udp6_fd ){
		// tell network_accept threads to finish
		DEBUG("shutting network down");
	    }else{
		DEBUG("network finished");
		return;
	    }
            break;
        }

        // RSN - determine stats, ...



	sleep(1);
    }
}

//################################################################

static int
report_status(NTD *ntd){
    return snprintf(ntd->gpbuf_out, ntd->out_size, "OK\n");
}

static int
report_peers(NTD *ntd){
    return peerdb->report(ntd);
}

static int
report_json(NTD *ntd){
    string buf;

    buf.append( "{\"xfer\": " );
    json_xfer( &buf );
    buf.append( ",\n \"task\": " );
    json_task( &buf );
    buf.append( ",\n \"job\": " );
    json_job( &buf );
    buf.append( "}\n" );

    ntd->out_resize( buf.size() );
    memcpy(ntd->gpbuf_out, buf.c_str(), buf.size());
    return buf.size();
}


//################################################################

// invalid request
static int
handle_unknown(NTD* ntd){
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header *pho = (protocol_header*) ntd->gpbuf_out;

    if( !(phi->flags & PHFLAG_WANTREPLY) ) return 0;

    ntd_copy_header_for_reply(ntd);
    pho->flags   = PHFLAG_ISREPLY | PHFLAG_ISERROR;

    cvt_header_to_network( pho );
    return sizeof(protocol_header);
}

// status request (eg. from monitoring system)
static int
handle_status(NTD* ntd){
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header *pho = (protocol_header*) ntd->gpbuf_out;
    ACPStdReply g;

    if( !(phi->flags & PHFLAG_WANTREPLY) ) return 0;

    if( runmode.mode() != RUN_LOLA_RUN ){
        g.set_status_code( 500 );
        g.set_status_message( "shutdown underway" );
    }else{
        g.set_status_code( 200 );
        g.set_status_message( "OK" );
    }

    g.SerializeToArray( ntd->out_data(), ntd->data_size() );
    ntd_copy_header_for_reply(ntd);
    pho->data_length = g.GetCachedSize();

    cvt_header_to_network( pho );
    return sizeof(protocol_header) + g.GetCachedSize();
}

// hb request (eg. from yenta)
static int
handle_hbreq(NTD* ntd){
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header *pho = (protocol_header*) ntd->gpbuf_out;
    ACPHeartBeat g;
    struct statvfs vfs;
    double load[3];

    if( !(phi->flags & PHFLAG_WANTREPLY) ) return 0;

    getloadavg( load, 3 );

    if( runmode.mode() != RUN_LOLA_RUN ){
	g.set_status_code( 500 );
	g.set_status_message( "shutdown underway" );
    }else{
	g.set_status_code( 200 );
	g.set_status_message( "Awesome" );
	g.set_sort_metric( (int)(load[1] * 1000) );
    }

    g.set_subsystem( "mrmagoo" );
    g.set_hostname( myhostname );
    g.set_environment( config->environment.c_str() );
    g.set_timestamp( lr_now() );
    g.set_port( myport );
    g.set_server_id( myserver_id );
    g.set_process_id( getpid() );

    // determine disk space
    if( ! statvfs( config->basedir.c_str(), &vfs ) ){
        g.set_capacity_metric( vfs.f_bavail / 2048 );	// MB avail
    }

    DEBUG("sending hb: %s", g.ShortDebugString().c_str());
    g.SerializeToArray( ntd->out_data(), ntd->data_size() );
    ntd_copy_header_for_reply(ntd);
    pho->data_length = g.GetCachedSize();

    cvt_header_to_network( pho );
    return sizeof(protocol_header) + g.GetCachedSize();
}

int
reply_ok(NTD *ntd){
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header *pho = (protocol_header*) ntd->gpbuf_out;
    ACPStdReply g;

    if( !(phi->flags & PHFLAG_WANTREPLY) ) return 0;

    ntd_copy_header_for_reply(ntd);

    g.set_status_code( 200 );
    g.set_status_message( "OK" );
    g.SerializeToArray( ntd->out_data(), ntd->data_size() );
    pho->data_length = g.GetCachedSize();

    cvt_header_to_network( pho );
    return sizeof(protocol_header) + g.GetCachedSize();
}

int
reply_error(NTD *ntd, int code, const char *msg){
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header *pho = (protocol_header*) ntd->gpbuf_out;
    ACPStdReply g;

    VERBOSE("sending reply error %d %s", code, msg);

    if( !(phi->flags & PHFLAG_WANTREPLY) ) return 0;

    ntd_copy_header_for_reply(ntd);
    phi->flags   = PHFLAG_ISREPLY | PHFLAG_ISERROR;

    g.set_status_code( code );
    g.set_status_message( msg );
    g.SerializeToArray( ntd->out_data(), ntd->data_size() );
    pho->data_length = g.GetCachedSize();

    cvt_header_to_network( pho );
    return sizeof(protocol_header) + g.GetCachedSize();
}
