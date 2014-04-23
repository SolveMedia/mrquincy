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
Job::depending_x(ToDo *t){
    _pending.remove(t);
}

void
ToDo::retry_or_abort(bool did_timeout){

    if( _job->_n_fails > _job->_servers.size() * TODOMAXFAIL / 2){
        _job->abort();
        return;
    }

    if( ++_tries >= TODOMAXFAIL ){
        const char *serv = _job->_servers[_serveridx]->name.c_str();

        // cannot replace a map task
        // only replace if the failure is likely because the server is down
        if( _job->_stepno && (did_timeout || !peerdb->is_it_up(serv) )){
            maybe_replace(1);
        }else{
            _job->abort();
        }
        return;
    }

    // retry
    int delay = (did_timeout ? TODORETRYDELAY * _tries : TODORETRYQUICK);
    _delay_until = lr_now() + delay/2 + random_n( delay/2 ) + random_n( delay/2 );

    pend();
}

int
XferToDo::maybe_replace(bool import){
    // wait until the task fails, and replace that
    return 0;
}

int
TaskToDo::maybe_replace(bool important){

    if( replace() )
        return 1;

    if( important ) _job->abort();

    return 0;
}

void
TaskToDo::failed(bool did_timeout){

    if( _state != JOB_TODO_STATE_RUNNING ) return;

    _job->kvetch("task %s failed", _xid.c_str());
    _job->derunning_x(this);
    _job->_servers[ _serveridx ]->_n_task_running --;
    _job->_servers[ _serveridx ]->_n_fails ++;
    _job->_n_task_running --;
    _job->_n_fails ++;
    _state = JOB_TODO_STATE_FINISHED;

    TaskToDo *alt = _replaces ? _replaces : _replacedby;
    if( alt ){
        // if there is an alternate task still running, let it finish
        if( ! alt->is_finished() ) return;
    }

    retry_or_abort(did_timeout);
}
void
XferToDo::failed(bool did_timeout){

    _job->kvetch("xfer %s failed file %s host %s - %s", _xid.c_str(), _g.filename().c_str(),
                 _job->_servers[_serveridx]->name.c_str(), _g.location(0).c_str());

    _job->derunning_x(this);
    _job->_servers[ _serveridx ]->_n_xfer_running --;
    _job->_servers[ _peeridx   ]->_n_xfer_peering --;
    _job->_servers[ _serveridx ]->_n_fails ++;
    _job->_n_xfer_running --;
    _job->_n_fails ++;
    _state = JOB_TODO_STATE_FINISHED;

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

    // check prereqs
    if( !_prerequisite.empty() ){
        for(list<ToDo*>::iterator it=_prerequisite.begin(); it !=_prerequisite.end(); it++){
            ToDo *t = *it;

            if( ! t->is_finished() ) return 0;
        }

        _prerequisite.clear();
    }

    // ok, let's start....

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

void
TaskToDo::discard(void){

    DEBUG("discard %s %d", _xid.c_str(), _state);
    if( _state == JOB_TODO_STATE_PENDING ){
        _job->depending_x(this);
        _state = JOB_TODO_STATE_FINISHED;
        return;
    }

    if( _state == JOB_TODO_STATE_RUNNING ){
        _job->derunning_x(this);
        _state = JOB_TODO_STATE_FINISHED;
        _job->_servers[ _serveridx ]->_n_task_running --;
        _job->_n_task_running --;
        return;
    }
}

// remove task from run list, send abort request (tcp)
void
TaskToDo::cancel(void){
    ACPMRMTaskAbort req;

    int prevstate = _state;

    discard();

    if( prevstate != JOB_TODO_STATE_RUNNING ) return;

    req.set_jobid( _job->_id );
    req.set_taskid( _xid.c_str() );

    make_request( _job->_servers[_serveridx], PHMT_MR_TASKABORT, &req, IO_TIMEOUT );
}

// same, but udp
void
TaskToDo::cancel_light(void){
    ACPMRMTaskAbort req;

    int prevstate = _state;

    discard();

    if( prevstate != JOB_TODO_STATE_RUNNING ) return;

    req.set_jobid( _job->_id );
    req.set_taskid( _xid.c_str() );

    toss_request(udp4_fd, _job->_servers[ _serveridx ], PHMT_MR_TASKABORT, &req );
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

    if( _state == JOB_TODO_STATE_RUNNING ){
        _job->_servers[ _serveridx ]->_n_task_running --;
        _job->_n_task_running --;
    }
    _state = JOB_TODO_STATE_FINISHED;

    // so how slow was it?
    if( amount ){
        _run_time = amount;
    }else{
        _run_time = lr_now() - _run_start;
    }

    _job->_task_run_time += _run_time;

    create_xfers();

    // if this was replaced, abort the replacement (and vv)
    if( _replacedby ) _replacedby->cancel_light();
    if( _replaces   ) _replaces->cancel_light();

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
        int dst = i % nserv;

        // if file will be processed on this server, we do not need to copy it
        // instead, make a backup copy on another server
        if( _serveridx == dst ) dst = (i+1) % nserv;

        XferToDo *x = new XferToDo(_job, &_g.outfile(i), _serveridx, dst);
        _job->_pending.push_back(x);
        _job->_xfers.push_back(x);
    }
}

