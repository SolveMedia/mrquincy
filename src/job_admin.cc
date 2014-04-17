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
#include <math.h>

#include <sstream>
#include <iomanip>
using std::ostringstream;

extern int scriblr_delete_file(const string *);


#define MAXJOB		(config->hw_cpus ? config->hw_cpus : 1)		// maximum number of running jobs


// keep a queue of jobs
// only so many run at a time, the rest stay queued

static QueuedJob jobq;

static void *job_periodic(void*);



void
job_init(void){
    start_thread(job_periodic, 0);
}

void
job_shutdown(void){
    jobq.shutdown();
}

static void *
job_periodic(void *notused){

    while(1){
        // can we start any queued jobs?
        jobq.start_more(MAXJOB);
        sleep(5);
    }
}

int
job_nrunning(void){
    return jobq.nrunning();
}

int
Job::init(int fd, const char *buf, int len){

    // parse protobuf
    _g.ParsePartialFromArray( buf, len );
    DEBUG("l=%d, %s", len, _g.ShortDebugString().c_str());

    if( ! _g.IsInitialized() ){
        DEBUG("invalid request. missing required fields");
        return 0;
    }

    // for quick access
    _id = _g.jobid().c_str();

    VERBOSE("new job: %s - %s", _id, _g.traceinfo().c_str());

    // adjust console
    if( _g.has_console() && _g.console().c_str()[0] == ':' ){

        // find peer ip addr
        sockaddr_in sa;
        socklen_t sal = sizeof(sa);
        getpeername(fd, (sockaddr*)&sa, &sal);

        // convert :port -> inet:port
        string cons = inet_ntoa(sa.sin_addr);
        cons.append( _g.console().c_str() );
        _g.set_console( cons.c_str() );
        DEBUG("console: %s", cons.c_str());
    }

    return 1;
}


// handle job request from network
int
handle_job(NTD *ntd){
    protocol_header  *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header  *pho = (protocol_header*) ntd->gpbuf_out;
    Job *req = new Job;

    // parse request
    int ok = req->init(ntd->fd, ntd->in_data(), phi->data_length );
    if( !ok ){
        delete req;
        return 0;
    }

    int prio = req->priority();
    if( !prio ) prio = lr_now() >> 8;

    jobq.start_or_queue( (void*)req, prio, MAXJOB );

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
    if( f )
        return reply_ok(ntd);

    // not found - must be something orphaned when something restarted
    // tell the slave to abort the task
    if( !ntd->is_tcp ){
        ACPMRMTaskAbort ab;
        ab.set_jobid( req.jobid().c_str() );
        ab.set_taskid( req.xid().c_str() );
        DEBUG("sending job 404 to %s", inet_ntoa(ntd->peer.sin_addr));
        toss_request(udp4_fd, & ntd->peer, PHMT_MR_TASKABORT, &ab);
    }

    return reply_error(ntd, 404, "Not Found");
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
        if( !id->compare( j->_id ) ){
            found = j;
            break;
        }
    }

    int s = 0;

    if( found )
        s = found->update( & g->xid(), & g->phase(), g->progress(), g->final_amount() );

    _lock.r_unlock();

    return s;
}

// system is shutting down - kill running tasks, drain the queue
void
QueuedJob::shutdown(void){

    _lock.w_lock();

    for(list<QueueElem*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        QueueElem *e = *it;
        Job *j = (Job*)e->_elem;
        delete j;
        delete e;
    }
    _queue.clear();

    for(list<void*>::iterator it=_running.begin(); it != _running.end(); it++){
        void *g = *it;
        Job *j = (Job*)g;
        VERBOSE("aborting job %s", j->_id);
        j->abort();
    }
    _running.clear();

    _lock.w_unlock();
}

void
QueuedJob::abort(const string *id){

    QueueElem *found = 0;
    int killpid = 0;

    // find job
    //   dequeue if queued
    //   kill if running

    _lock.w_lock();

    for(list<QueueElem*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        QueueElem *e = *it;
        Job *j = (Job*)e->_elem;
        if( !id->compare( j->_id ) ){
            found = e;
            break;
        }
    }
    if( found ){
        _queue.remove(found);
        Job *j = (Job*)found->_elem;
        delete j;
        delete found;
    }else{
        Job *fj = 0;
        for(list<void*>::iterator it=_running.begin(); it != _running.end(); it++){
            void *g = *it;
            Job *j = (Job*)g;
            if( !id->compare( j->_id ) ){
                fj = j;
                break;
            }
        }

        if( fj ) fj->abort();
    }

    _lock.w_unlock();
}

bool
QueuedJob::same(void *xa, void *xb){
    Job *ja = (Job*)xa;
    Job *jb = (Job*)xb;

    const string *id = & ja->_g.jobid();
    return id->compare( jb->_g.jobid() ) ? 0 : 1;
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


    b << "{\"jobid\": \""       << _id                    << "\", "
      << "\"state\": \""        << st                     << "\", "
      << "\"phase\": \""        << ph           	  << "\", "
      << "\"traceinfo\": \""    << _g.traceinfo().c_str() << "\", "
      << "\"options\": "        << _g.options().c_str()   << ", "		// options is json
      << "\"start_time\": "     << _created
      << "}";

    _lock.r_unlock();

    dst->append(b.str().c_str());
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

    if( _g.has_console() ){
        ACPMRMDiagMsg gm;

        gm.set_jobid( _id );
        gm.set_server_id( myserver_id.c_str() );
        gm.set_type( type );
        gm.set_msg(  msg );

        toss_request( udp4_fd, _g.console().c_str(), PHMT_MR_DIAGMSG, &gm );
    }
}


void
Job::kvetch(const char *msg, const char *arg1, const char *arg2, const char *arg3, const char *arg4) const {
    char buf[1024];

    snprintf(buf, sizeof(buf), msg, arg1, arg2, arg3, arg4);
    VERBOSE("job: %s %s", _id, buf);

    send_eu_msg_x("error", buf);
}

void
Job::inform(const char *msg, const char *arg1, const char *arg2, const char *arg3, const char *arg4) const {
    char buf[1024];

    snprintf(buf, sizeof(buf), msg, arg1, arg2, arg3, arg4);
    DEBUG("job: %s %s", _id, buf);

    send_eu_msg_x("debug", buf);
}

void
Job::report(const char *msg, const char *arg1, const char *arg2, const char *arg3, const char *arg4) const {
    char buf[1024];

    snprintf(buf, sizeof(buf), msg, arg1, arg2, arg3, arg4);
    VERBOSE("job: %s %s", _id, buf);

    send_eu_msg_x("report", buf);
}

void
Job::notify_finish(void) const {
    char buf[1024];

    send_eu_msg_x("finish", "");
}

float
Job::efficency_x(void){

    int jt = _run_time ? _run_time : (lr_now() - _run_start);
    float efficency = 0;
    if( jt )
        return _task_run_time * 100.0 / (jt * _servers.size());
    return 0;
}

void
Job::log_progress(bool rp){

    _lock.r_lock();
    const char *ph = (_stepno >= _plan.size()) ? "cleanup" : _plan[_stepno]->_phase.c_str();


    ostringstream b;

    if( rp && !_n_task_running && !_n_xfer_running && !_pending.size() ){
        // done - don't display all 0s
        b << "status: phase finished;"
          << " map "           << _totalmapsize / 1000000 << "MB"
          << ", task "         << _n_tasks_run
          << ", xfer "         << _n_xfers_run
          << ", dele "         << _n_deleted
          << "; effcy "        << efficency_x()
            ;
    }else{
        b << "status: phase "  << ph
          << ", task "         << _n_task_running
          << ", xfer "         << _n_xfer_running
          << ", pend "         << _pending.size()
          << "; effcy "        << efficency_x()
          << "; (ran: task "   << _n_tasks_run
          << ", xfer "         << _n_xfers_run
          << ", dele "         << _n_deleted
          << ")";
    }

    send_eu_msg_x("progress", b.str().c_str());

    _lock.r_unlock();
}


static void
format_dt(int sec, ostringstream &b){

    int h = sec / 3600;
    sec %= 3600;

    int m = sec / 60;
    sec %= 60;

    char buf[64];

    if( !h ){
        snprintf(buf, sizeof(buf), "%d:%02d", m,sec);
    }else{
        snprintf(buf, sizeof(buf), "%d:%02d:%02d", h, m,sec);
    }

    b << buf;
}

void
Job::report_final_stats(void){

    _lock.r_lock();

    int nstep = _plan.size();
    for(int i=0; i<nstep; i++){
        Step *s = _plan[i];
        s->report_final_stats(this);
    }

    ostringstream b;
        b << "summary"
          << " input "         << _totalmapsize / 1000000 << " MB(gz)"
          << ", task "         << _n_tasks_run
          << ", failed "       << _n_fails
          << ", xfer "         << _n_xfers_run
          << ", dele "         << _n_deleted	<< "; ";

        format_dt(_task_run_time, b);
        b << " cpu, ";
        format_dt(_run_time, b);
        b << " wall; effcy "   << efficency_x()
            ;

    _lock.r_unlock();

    report("%s", b.str().c_str());
}

void
Step::report_final_stats(Job *j){
    ostringstream b;

    // phase map, 14 tasks, 37 xfers, 1234 secs run, 123 sec wall,
    //     12/23/34 min/ave/max => 4.4 effcy, 53% lumpy

    // iterate tasks: measure min/max/ave/std
    int   min=0, max=0, sum=0;
    float sum2=0;
    int   ntask = _tasks.size();
    int   nserv = j->_servers.size();

    if( !_run_start ) return;
    if( !_run_time  ) _run_time = lr_now() - _run_start;

    for(int i=0; i<ntask; i++){
        TaskToDo *t = _tasks[i];

        sum  += t->_run_time;
        sum2 += t->_run_time * t->_run_time;
        if( !min || min > t->_run_time ) min = t->_run_time;
        if( max < t->_run_time )         max = t->_run_time;
    }

    // xfers...
    b << "phase " << _phase
      << ", "     << ntask 	  << " tasks"
      << ", "     << _n_xfers_run << " xfers " << _xfer_size / 1000000 << "MB; ";
    format_dt(sum, b); 		b << " cpu, ";
    format_dt(_run_time, b);	b << " wall";

    if( ntask > 1 && sum > 0 ){
        float ave = sum / (float)ntask;
        float std = sqrt( sum2/ntask - ave * ave );
        float rsd = std / ave;

        b << "\n    "
          << min << "/" << (int)ave << "/" << max << " secs min/ave/max => "
          << 100.0 * sum / _run_time / nserv      << " effcy, "	// parrellelism - higher is better
          << 100.0 * rsd 			  << " lumpy";	// lopsidedness - lower is better
    }

    j->send_eu_msg_x("report", b.str().c_str());
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

    if( _n_threads ){
        VERBOSE("~Job sees %d threads still active", _n_threads );
        sleep(1);
    }

    DEBUG("destroy job %s", _id );

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

    for(list<XferToDo*>::iterator it=_xfers.begin(); it != _xfers.end(); it++){
        XferToDo *x = *it;
        delete x;
    }

    _lock.w_unlock();
}
