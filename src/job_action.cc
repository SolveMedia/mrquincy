/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-27 14:55 (EDT)
  Function: todo state transitions

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

#define TIMEOUT			30

void
Job::derunning_x(ToDo *t){
    _running.remove(t);
}

void
Job::enrunning_x(ToDo *t){
    _pending.remove(t);
    _running.push_back(t);
}

void
ToDo::retry_or_abort(void){

    if( ++_tries >= TODOMAXFAIL ){
        _state = JOB_TODO_STATE_FINISHED;
        _job->abort();
    }

    // retry
    _state = JOB_TODO_STATE_PENDING;
    _delay_until = lr_now() + TODORETRYDELAY;
    _job->_pending.push_back(this);
}

void
TaskToDo::failed(void){

    _job->inform("task %s failed", _xid.c_str());
    _job->derunning_x(this);
    _job->_servers[ _serveridx ]->_n_task_running --;
    _job->_n_task_running --;

    retry_or_abort();
}
void
XferToDo::failed(void){

    _job->inform("xfer %s failed", _xid.c_str());
    _job->derunning_x(this);
    _job->_servers[ _serveridx ]->_n_xfer_running --;
    _job->_n_xfer_running --;

    retry_or_abort();
}

void
ToDo::timedout(void){
    DEBUG("time out");
    abort();
    failed();
}

static void *
todo_start(void *x){
    ToDo *t = (ToDo*)x;
    t->start();
}

int
ToDo::start_check(void){
    // are we good to go?
    if( _job->_n_threads >= JOBMAXTHREAD ) return 0;
    if( _delay_until > lr_now() ) return 0;
    return 1;
}

void
ToDo::start_common(void){

    _state = JOB_TODO_STATE_RUNNING;
    _last_status = lr_now();
    _job->_n_threads ++;
    _job->_pending.remove(this);
    _job->_running.push_back(this);
}

int
TaskToDo::maybe_start(void){

    // too much running?
    if( _job->_servers[ _serveridx ]->_n_task_running >= SERVERTASKMAX ) return 0;
    if( ! start_check() ) return 0;

    // we could create these at task construction time, but deferring until here
    // saves us effort if the job is aborted
    if( !_tries )
        create_deles();

    _job->inform("starting task %s", _xid.c_str());
    _job->_servers[ _serveridx ]->_n_task_running ++;
    _job->_n_task_running ++;
    _job->_n_tasks_run ++;

    _run_start   = lr_now();
    start_common();

    DEBUG("started");
    start_thread(todo_start, (void*)(ToDo*)this);
    return 1;
}

int
XferToDo::maybe_start(void){

    // too much running?
    if( _job->_servers[ _serveridx ]->_n_xfer_running >= SERVERXFERMAX ) return 0;
    if( ! start_check() ) return 0;

    _job->inform("starting xfer %s", _xid.c_str());
    _job->_servers[ _serveridx ]->_n_xfer_running ++;
    _job->_n_xfer_running ++;
    _job->_n_xfers_run ++;
    start_common();

    DEBUG("started");
    start_thread(todo_start, (void*)(ToDo*)this);
    return 1;
}

void
Job::thread_done(void){
    _lock.w_lock();
    _n_threads--;
    _lock.w_unlock();
}

int
TaskToDo::start(void){

    if( ! make_request( _job->_servers[_serveridx], PHMT_MR_TASKCREATE, &_g, TIMEOUT ) ){
        string status = "FAILED";
        _job->update( &_xid, &status, 0 );
    }

    _job->thread_done();
}

int
XferToDo::start(void){

    if( ! make_request( _job->_servers[_serveridx], PHMT_MR_FILEXFER, &_g, TIMEOUT ) ){
        string status = "FAILED";
        _job->update( &_xid, &status, 0 );
    }

    _job->thread_done();
}


void
TaskToDo::abort(void){
    DEBUG("abort task");
    ACPMRMTaskAbort req;

    if( _state != JOB_TODO_STATE_RUNNING ) return;

    req.set_jobid( _job->jobid() );
    req.set_taskid( _xid );

    // udp fire+forget
    toss_request(udp4_fd, _job->_servers[ _serveridx ], PHMT_MR_TASKABORT, &req );
}

// remove task from run list, send abort request (tcp)
void
TaskToDo::cancel(void){
    DEBUG("abort task");
    ACPMRMTaskAbort req;

    _job->derunning_x(this);
    _state = JOB_TODO_STATE_FINISHED;
    _job->_servers[ _serveridx ]->_n_task_running --;
    _job->_n_task_running --;

    if( _state != JOB_TODO_STATE_RUNNING ) return;

    req.set_jobid( _job->jobid() );
    req.set_taskid( _xid );

    make_request( _job->_servers[_serveridx], PHMT_MR_TASKABORT, &req, TIMEOUT );
}

void
XferToDo::abort(void){
    DEBUG("abort xfer");
    // there is no abort method, just wait...
}
void
XferToDo::cancel(void){
    DEBUG("cancel xfer");
    // there is no cancel method, just wait...
}


void
TaskToDo::finished(void){

    _job->inform("task %s finished", _xid.c_str());
    _job->derunning_x(this);
    _state = JOB_TODO_STATE_FINISHED;
    _job->_servers[ _serveridx ]->_n_task_running --;
    _job->_n_task_running --;

    _run_time = lr_now() - _run_start;
    _job->_task_run_time += _run_time;

    create_xfers();
}

void
XferToDo::finished(void){

    _job->derunning_x(this);
    _state = JOB_TODO_STATE_FINISHED;
    _job->_servers[ _serveridx ]->_n_xfer_running --;
    _job->_n_xfer_running --;

    delete this;
}

XferToDo::XferToDo(Job *j, const string *name, int src, int dst){

    _job         = j;
    _serveridx   = dst;
    unique( &_xid );
    _state       = JOB_TODO_STATE_PENDING;
    _tries       = 0;
    _delay_until = 0;

    _g.set_jobid(   j->jobid() );
    _g.set_console( j->console() );
    _g.set_master(  myipandport );
    _g.set_copyid(  _xid );
    _g.set_filename( *name );
    _g.add_location( _job->_servers[src]->name );

}

void
TaskToDo::create_xfers(void){

    // for all outfiles
    // new Xfer -> job pend

    int nserv = _job->_servers.size();
    int noutf = _g.outfile_size();

    for(int i=0; i<noutf; i++){
        if( _serveridx == (i % nserv) ) continue;	// no need to copy
        XferToDo *x = new XferToDo(_job, &_g.outfile(i), _serveridx, i%nserv);
        _job->_pending.push_back(x);

        // backup - (i+1) % nserv

    }
}

