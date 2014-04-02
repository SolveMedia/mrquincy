/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-25 17:17 (EDT)
  Function: job admin - queue, network

*/
#define CURRENT_SUBSYSTEM	'j'

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
#include "job.h"

#include "mrmagoo.pb.h"
#include "std_reply.pb.h"

#include <sys/types.h>
#include <poll.h>
#include <sys/socket.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sstream>
using std::ostringstream;

extern int scriblr_delete_file(const string *);


// keep a queue of jobs
// only so many run at a time, the rest stay queued

static QueuedJob jobq;

static void *job_periodic(void*);



void
job_init(void){
    start_thread(job_periodic, 0);
}

static void *
job_periodic(void *notused){

    while(1){
        // can we start any queued jobs?
        jobq.start_more(MAXJOB);
        sleep(5);
    }
}

static void
adjust_console(int fd, Job *req){

    if( ! req->has_console() ) return;
    if( req->console().c_str()[0] != ':' ) return;

    // convert :port -> inet:port
    sockaddr_in sa;
    socklen_t sal = sizeof(sa);
    getpeername(fd, (sockaddr*)&sa, &sal);

    string cons = inet_ntoa(sa.sin_addr);
    cons.append( req->console() );
    req->set_console( cons );
    DEBUG("console: %s", cons.c_str());
}


// handle job request from network
int
handle_job(NTD *ntd){
    protocol_header  *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header  *pho = (protocol_header*) ntd->gpbuf_out;
    Job *req = new Job;

    // parse request
    req->ParsePartialFromArray( ntd->in_data(), phi->data_length );
    DEBUG("l=%d, %s", phi->data_length, req->ShortDebugString().c_str());

    if( ! req->IsInitialized() ){
        DEBUG("invalid request. missing required fields");
        return 0;
    }

    adjust_console( ntd->fd, req );

    VERBOSE("new job: %s - %s", req->jobid().c_str(), req->traceinfo().c_str());

    jobq.start_or_queue( (void*)req, MAXJOB );

    return reply_ok(ntd);
}

int
handle_jobabort(NTD *ntd){
    protocol_header  *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header  *pho = (protocol_header*) ntd->gpbuf_out;
    ACPMRMJobAbort req;

    // parse request
    req.ParsePartialFromArray( ntd->in_data(), phi->data_length );
    DEBUG("l=%d, %s", phi->data_length, req.ShortDebugString().c_str());

    if( ! req.IsInitialized() ){
        DEBUG("invalid request. missing required fields");
        return 0;
    }

    DEBUG("recvd job abort %s", req.jobid().c_str());

    jobq.abort(&req.jobid());

    return reply_ok(ntd);
}

// task + xfer status updates
int
handle_jobstatus(NTD *ntd){
    protocol_header  *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header  *pho = (protocol_header*) ntd->gpbuf_out;
    ACPMRMActionStatus req;

    // parse request
    req.ParsePartialFromArray( ntd->in_data(), phi->data_length );
    DEBUG("l=%d, %s", phi->data_length, req.ShortDebugString().c_str());

    if( ! req.IsInitialized() ){
        DEBUG("invalid request. missing required fields");
        return 0;
    }

    DEBUG("recvd job status %s", req.jobid().c_str());

    int f = jobq.update(&req);

    return f ? reply_ok(ntd) : reply_error(ntd, 404, "Not Found");
}

int
handle_mrdelete(NTD *ntd){
    protocol_header  *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header  *pho = (protocol_header*) ntd->gpbuf_out;
    ACPMRMFileDel req;

    // parse request
    req.ParsePartialFromArray( ntd->in_data(), phi->data_length );
    DEBUG("l=%d, %s", phi->data_length, req.ShortDebugString().c_str());


    DEBUG("recvd delete request");

    int s = 1;
    for(int i=0; i<req.filename_size(); i++){
        int d = scriblr_delete_file( & req.filename(i) );
        if(!d) s = 0;
    }

    return s ? reply_ok(ntd) : reply_error(ntd, 404, "Not Found");
}


//****************************************************************

int
QueuedJob::update(ACPMRMActionStatus *g){
    Job *found = 0;
    const string *id = & g->jobid();

    _lock.r_lock();

    for(list<void*>::iterator it=_running.begin(); it != _running.end(); it++){
        void *g = *it;
        Job *j = (Job*)g;
        if( !id->compare( j->jobid() ) ){
            found = j;
            break;
        }
    }

    int s = 0;

    if( found )
        s = found->update( & g->xid(), & g->phase(), g->progress() );

    _lock.r_unlock();

    return s;
}


void
QueuedJob::abort(const string *id){

    Job *found = 0;
    int killpid = 0;

    // find job
    //   dequeue if queued
    //   kill if running

    _lock.w_lock();

    for(list<void*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        void *g = *it;
        Job *j = (Job*)g;
        if( !id->compare( j->jobid() ) ){
            found = j;
            break;
        }
    }
    if( found ){
        _queue.remove((void*)found);
    }else{
        for(list<void*>::iterator it=_running.begin(); it != _running.end(); it++){
            void *g = *it;
            Job *j = (Job*)g;
            if( !id->compare( j->jobid() ) ){
                found = j;
                break;
            }
        }
    }

    if(found)
        found->abort();

    _lock.w_unlock();

}

bool
QueuedJob::same(void *xa, void *xb){
    Job *ga = (Job*)xa;
    Job *gb = (Job*)xb;

    const string *id = & ga->jobid();
    return id->compare( gb->jobid() ) ? 0 : 1;
}

// running job gets its very own thread!
static void*
start_job(void *x){
    Job *j = (Job*)x;

    j->run();

    jobq.done(x);
    sleep(15);
    delete j;
}


void
QueuedJob::start(void *x){
    start_thread( start_job, x);
}

void
json_job(string *dst){
    jobq.json(dst);
}


void
QueuedJob::json1(const char *st, void *x, string *dst){
    Job *j = (Job*)x;
    j->json(st, dst);
}

void
Job::json(const char *st, string *dst){
    ostringstream b;

    _lock.r_lock();
    const char *ph = (_stepno >= _plan.size()) ? "cleanup" : _plan[_stepno]->_phase.c_str();
    if( _state == JOB_STATE_PLANNING ) ph = "planning";
    if( _state == JOB_STATE_QUEUED   ) ph = "queued";


    b << "{\"jobid\": \""       << jobid()      << "\", "
      << "\"state\": \""        << st           << "\", "
      << "\"phase\": \""        << ph           << "\", "
      << "\"traceinfo\": \""    << traceinfo()  << "\", "
      << "\"options\": "        << options()    << ", "		// options is json
      << "\"start_time\": "     << _created
      << "}";

    dst->append(b.str());
    _lock.r_unlock();
}

ToDo *
Job::find_todo_x(const string *xid) const{

    for(list<ToDo*>::const_iterator it=_running.begin(); it != _running.end(); it++){
        ToDo *t = *it;
        if( ! xid->compare( t->_xid ) ) return t;
    }
    return 0;
}

void
Job::send_eu_msg_x(const char *type, const char *msg) const{

    if( has_console() ){
        ACPMRMDiagMsg gm;

        gm.set_jobid( jobid() );
        gm.set_server_id( myserver_id );
        gm.set_type( type );
        gm.set_msg(  msg );

        toss_request( udp4_fd, console().c_str(), PHMT_MR_DIAGMSG, &gm );
    }
}


void
Job::kvetch(const char *msg, const char *arg1, const char *arg2, const char *arg3) const {
    char buf[1024];

    snprintf(buf, sizeof(buf), msg, arg1, arg2, arg3);
    VERBOSE("%s", buf);

    send_eu_msg_x("error", buf);
}

void
Job::inform(const char *msg, const char *arg1, const char *arg2, const char *arg3) const {
    char buf[1024];

    snprintf(buf, sizeof(buf), msg, arg1, arg2, arg3);
    DEBUG("job: %s %s", jobid().c_str(), buf);

    send_eu_msg_x("debug", buf);
}

void
Job::report(const char *msg, const char *arg1, const char *arg2, const char *arg3) const {
    char buf[1024];

    snprintf(buf, sizeof(buf), msg, arg1, arg2, arg3);
    VERBOSE("job: %s %s", jobid().c_str(), buf);

    send_eu_msg_x("report", buf);
}

void
Job::report_finish(void) const {
    char buf[1024];

    send_eu_msg_x("finish", "");
}


Step::~Step(){
    int ntask = _tasks.size();
    for(int i=0; i<ntask; i++){
        TaskToDo *t = _tasks[i];
        delete t;
    }
}

Server::~Server(){
    for(list<Delete*>::iterator it=_to_delete.begin(); it != _to_delete.end(); it++){
        Delete *d = *it;
        delete d;
    }
}

Job::~Job(){

    _lock.w_lock();

    int nstep = _plan.size();
    for(int i=0; i<nstep; i++){
        Step *s = _plan[i];
        delete s;
    }

    int nserv = _servers.size();
    for(int i=0; i<nserv; i++){
        Server *s = _servers[i];
        delete s;
    }

    _lock.w_unlock();
}
