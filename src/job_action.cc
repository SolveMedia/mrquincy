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

#define IO_TIMEOUT		30

#define XFERMAX			48
#define TODORETRYDELAY		15
#define TODORETRYQUICK	 	5
#define SERVERTASKMAX		10
#define SERVERXFERMAX		20


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
ToDo::retry_or_abort(bool did_timeout){

    if( ++_tries >= TODOMAXFAIL ){
        _state = JOB_TODO_STATE_FINISHED;
        _job->abort();
        return;
    }

    if( _job->_n_fails > _job->_servers.size() / 2 ){
        _job->abort();
        return;
    }

    // retry
    _state = JOB_TODO_STATE_PENDING;
    int delay = (did_timeout ? TODORETRYDELAY * _tries : TODORETRYQUICK);
    _delay_until = lr_now() + delay/2 + random_n( delay/2 ) + random_n( delay/2 );

    _job->_pending.push_back(this);
}

void
TaskToDo::failed(bool did_timeout){

    _job->kvetch("task %s failed", _xid.c_str());
    _job->derunning_x(this);
    _job->_servers[ _serveridx ]->_n_task_running --;
    _job->_n_task_running --;
    _job->_n_fails ++;

    retry_or_abort(did_timeout);
}
void
XferToDo::failed(bool did_timeout){

    _job->kvetch("xfer %s failed file %s host %s - %s", _xid.c_str(), _g.filename().c_str(),
                 _job->_servers[_serveridx]->name.c_str(), _g.location(0).c_str());

    _job->derunning_x(this);
    _job->_servers[ _serveridx ]->_n_xfer_running --;
    _job->_servers[ _peeridx   ]->_n_xfer_peering --;
    _job->_n_xfer_running --;
    _job->_n_fails ++;

    retry_or_abort(did_timeout);
}

void
ToDo::timedout(void){

    _job->kvetch("task %s timed out", _xid.c_str());
    abort();
    failed(1);
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

    _job->inform("starting task %s - %s on %s", _xid.c_str(), _g.phase().c_str(),
                 _job->_servers[ _serveridx ]->name.c_str());

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
    if( _job->_servers[ _peeridx   ]->_n_xfer_peering >= SERVERXFERMAX ) return 0;
    if( _job->_n_xfer_running >= XFERMAX ) return 0;
    if( ! start_check() ) return 0;

    _job->inform("starting xfer %s - %s : %s", _xid.c_str(), _g.filename().c_str(),
                 _job->_servers[ _serveridx ]->name.c_str());

    _job->_servers[ _serveridx ]->_n_xfer_running ++;
    _job->_servers[ _peeridx   ]->_n_xfer_peering ++;
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
    try_to_do_something(1);
}

int
TaskToDo::start(void){

    if( ! make_request( _job->_servers[_serveridx], PHMT_MR_TASKCREATE, &_g, IO_TIMEOUT ) ){
        string status = "FAILED";
        _job->update( &_xid, &status, 0, 0 );
    }

    _job->thread_done();
}

int
XferToDo::start(void){

    if( ! make_request( _job->_servers[_serveridx], PHMT_MR_FILEXFER, &_g, IO_TIMEOUT ) ){
        string status = "FAILED";
        _job->update( &_xid, &status, 0, 0 );
    }

    _job->thread_done();
}


void
TaskToDo::abort(void){
    DEBUG("abort task");
    ACPMRMTaskAbort req;

    if( _state != JOB_TODO_STATE_RUNNING ) return;

    req.set_jobid( _job->_id );
    req.set_taskid( _xid.c_str() );

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

    req.set_jobid( _job->_id );
    req.set_taskid( _xid.c_str() );

    make_request( _job->_servers[_serveridx], PHMT_MR_TASKABORT, &req, IO_TIMEOUT );
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
TaskToDo::finished(int amount){

    _job->inform("task %s finished", _xid.c_str());
    _job->derunning_x(this);
    _state = JOB_TODO_STATE_FINISHED;
    _job->_servers[ _serveridx ]->_n_task_running --;
    _job->_n_task_running --;

    // so how slow was it?
    if( amount ){
        _run_time = amount;
    }else{
        _run_time = lr_now() - _run_start;
    }

    _job->_task_run_time += _run_time;

    create_xfers();
}

void
XferToDo::finished(int amount){

    _job->derunning_x(this);
    _state = JOB_TODO_STATE_FINISHED;
    _job->_servers[ _serveridx ]->_n_xfer_running --;
    _job->_servers[ _peeridx   ]->_n_xfer_peering --;
    _job->_n_xfer_running --;

    // tally up file xfer sizes
    _job->_plan[ _job->_stepno ]->_xfer_size += amount;
    _job->_plan[ _job->_stepno ]->_n_xfers_run ++;

}

XferToDo::XferToDo(Job *j, const string *name, int src, int dst){

    unique( &_xid );
    _job         = j;
    _serveridx   = dst;
    _peeridx     = src;
    _state       = JOB_TODO_STATE_PENDING;
    _tries       = 0;
    _delay_until = 0;

    _g.set_jobid(   j->_id );
    _g.set_console( j->_g.console().c_str() );
    _g.set_master(  myipandport.c_str() );
    _g.set_copyid(  _xid.c_str() );
    _g.set_filename( name->c_str() );
    _g.add_location( _job->_servers[src]->name.c_str() );

}

void
TaskToDo::create_xfers(void){

    // files from last step can stay where they are
    if( _job->_stepno == _job->_plan.size() - 1 ) return;

    // for all outfiles
    // new Xfer -> job pend

    int nserv = _job->_servers.size();
    int noutf = _g.outfile_size();

    for(int i=0; i<noutf; i++){
        if( _serveridx == (i % nserv) ) continue;	// no need to copy
        XferToDo *x = new XferToDo(_job, &_g.outfile(i), _serveridx, i%nserv);
        _job->_pending.push_back(x);
        _job->_xfers.push_back(x);

        // backup - (i+1) % nserv

    }
}

