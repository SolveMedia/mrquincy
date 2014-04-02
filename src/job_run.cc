/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-26 11:13 (EDT)
  Function: 

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

#include <sstream>
using std::ostringstream;



Job::Job(){
    _state          = JOB_STATE_QUEUED;
    _created        = lr_now();
    _want_abort     = 0;
    _stepno         = 0;
    _n_xfer_running = 0;
    _n_task_running = 0;
    _n_deleted      = 0;
    _n_threads	    = 0;
    _n_tasks_run    = 0;
    _n_xfers_run    = 0;
    _run_start      = 0;
    _run_time       = 0;
    _task_run_time  = 0;
    _totalmapsize   = 0;

}

void
Job::abort(void){
    _want_abort = 1;
    VERBOSE("abort requested");
}

void
Job::log_progress(bool rp){

    _lock.r_lock();
    const char *ph = (_stepno >= _plan.size()) ? "cleanup" : _plan[_stepno]->_phase.c_str();

    int jt = lr_now() - _run_start;
    float efficency = 0;
    if( jt )
        efficency = _task_run_time * 100.0 / (jt * _servers.size());

    ostringstream b;

    if( rp && !_n_task_running && !_n_xfer_running && !_pending.size() ){
        // done - don't display all 0s
        b << "status: phase finished;"
          << " map "           << _totalmapsize / 1000000 << "MB"
          << ", task "         << _n_tasks_run
          << ", xfer "         << _n_xfers_run
          << ", dele "         << _n_deleted
          << "; effcy "        << efficency
            ;
    }else{
        b << "status: phase "  << ph
          << ", task "         << _n_task_running
          << ", xfer "         << _n_xfer_running
          << ", pend "         << _pending.size()
          << "; effcy "        << efficency
          << "; (ran: task "   << _n_tasks_run
          << ", xfer "         << _n_xfers_run
          << ", dele "         << _n_deleted
          << ")";
    }

    if(rp)
        report(b.str().c_str());
    else
        inform(b.str().c_str());

    _lock.r_unlock();
}

void
Job::run(void){

    DEBUG("running job");

    do {
        if( ! plan() )       break;
        if( ! start_step() ) break;

        DEBUG("state %d", _state );

        while( _state == JOB_STATE_RUNNING && !_want_abort ){
            try_to_do_something();
            check_timeouts();
            log_progress(0);
            sleep(5);
        }
    } while(0);

    cleanup();
    log_progress(1);
    report_finish();

    DEBUG("done");
}

int
ToDo::update(const string *status, int progress){

    _last_status = lr_now();

    if( _state != JOB_TODO_STATE_RUNNING ) return 1;
    _progress    = progress;

    if( _status == *status ) return 1;
    _status      = *status;

    DEBUG("update %s -> %s", _xid.c_str(), status->c_str());

    if( !status->compare("FINISHED") ){
        finished();
    }
    if( !status->compare("FAILED") ){
        failed();
    }

    return 1;
}

int
Job::update(const string *xid, const string *status, int progress){

    _lock.w_lock();

    // find action + update it
    ToDo * t = find_todo_x(xid);
    if( t ){
        t->update(status, progress);
    }
    _lock.w_unlock();

    try_to_do_something();
    return 1;
}

int
Job::start_step_x(void){

    // move tasks from plan -> pending
    Step *step = _plan[ _stepno ];
    int ntask = step->_tasks.size();

    for(int i=0; i<ntask; i++){
        _pending.push_back( step->_tasks[i] );
        step->_tasks[i]->pending();
    }

    inform("starting phase %s", _plan[_stepno]->_phase.c_str());

    return 1;
}

int
Job::start_step(void){

    _lock.w_lock();
    int s = start_step_x();
    _lock.w_unlock();

    return s;
}

int
Job::next_step_x(void){

    _stepno ++;

    if( _stepno >= _plan.size() ){
        // finished
        DEBUG("job finished");
        _state = JOB_STATE_FINISHED;
        return 0;
    }

    return start_step_x();
}

int
Job::try_to_do_something(void){

    if( _state != JOB_STATE_RUNNING ) return 0;
    if( _want_abort ) return 0;

    _lock.w_lock();

    // nothing running, nothing pending => next phase
    if( _running.empty() && _pending.empty() )
        if( ! next_step_x() ){
            _lock.w_unlock();
            return 0;		// finished
        }
    _lock.w_unlock();

    // RSN - check load ave

    _lock.w_lock();

    // can we start anything?
    maybe_start_something_x();

    _lock.w_unlock();
    return 1;
}

int
Job::maybe_start_something_x(void){
    int started = 0;

    // start modifies _pending, create a tmp list to iterate
    list<ToDo*> tlist = _pending;

    for(list<ToDo*>::iterator it=tlist.begin(); it != tlist.end(); it++){
        ToDo *t = *it;
        int n = t->maybe_start();
        if( n < 0 ) break;
        started += n;
        if( started    >= TODOSTARTMAX ) break;
        if( _n_threads >= JOBMAXTHREAD ) break;
    }

    return started;
}

int
Job::check_timeouts(void){
    hrtime_t now = lr_now();

    _lock.w_lock();

    // failing modifies _running, create a tmp list to iterate
    list<ToDo*> tlist = _running;

    for(list<ToDo*>::iterator it=tlist.begin(); it != tlist.end(); it++){
        ToDo *t = *it;
        DEBUG("last %s - %lld", t->_xid.c_str(), t->_last_status);
        if( t->_last_status + TODOTIMEOUT < now ) t->timedout();
    }

    _lock.w_unlock();
    return 1;
}
