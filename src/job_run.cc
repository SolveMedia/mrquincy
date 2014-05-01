/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-26 11:13 (EDT)
  Function: run the job

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

#include <unistd.h>

#include <sstream>
using std::ostringstream;



Job::Job(){
    _state           = JOB_STATE_QUEUED;
    _created         = lr_now();
    _want_abort      = 0;
    _stepno          = 0;
    _n_xfer_running  = 0;
    _n_task_running  = 0;
    _n_deleted       = 0;
    _n_threads       = 0;
    _n_tasks_run     = 0;
    _n_xfers_run     = 0;
    _run_start       = lr_now(); // moved forward after planning
    _run_time        = 0;
    _task_run_time   = 0;
    _totalmapsize    = 0;
    _n_fails         = 0;

}

void
Job::abort(void){

    if( !_want_abort )
        kvetch("job aborted");

    _want_abort = 1;
}

void
Job::run(void){

    DEBUG("running job");

    do {
        if( ! plan() )       break;
        if( ! start_step() ) break;

        DEBUG("state %d", _state );

        while( _state == JOB_STATE_RUNNING && !_want_abort ){
            try_to_do_something(1);
            check_timeouts();
            maybe_specexec();
            log_progress(0);
            sleep(5);
        }
    } while(0);

    cleanup();
    _run_time = lr_now() - _run_start;
    log_progress(1);
    report_final_stats();
    notify_finish();	// tell end-user we are done

    DEBUG("done");
}

int
ToDo::update(const string *status, int progress, int amount){

    _last_status = lr_now();

    if( _state != JOB_TODO_STATE_RUNNING ) return 0;
    _progress    = progress;

    if( _status == *status ) return 0;
    _status.assign( status->c_str() );

    DEBUG("update %s -> %s", _xid.c_str(), status->c_str());

    if( !status->compare("FINISHED") ){
        finished(amount);
        return 1;
    }
    if( !status->compare("FAILED") ){
        failed(0);
        return 1;
    }

    return 0;
}

int
Job::update(const string *xid, const string *status, int progress, int amount){

    _lock.w_lock();

    // find action + update it
    ToDo * t = find_todo_x(xid);
    int done = 0;

    if( t ){
        done = t->update(status, progress, amount);
    }
    _lock.w_unlock();

    if( done )
        try_to_do_something(0);

    return 1;
}

void
ToDo::pend(void){
    _job->_pending.push_back( this );
    pending();
}

int
Job::start_step_x(void){

    Step *step = _plan[ _stepno ];
    int ntask  = step->_tasks.size();

    // stats
    step->_run_start = lr_now();

    if( _stepno ){
        Step * p = _plan[ _stepno - 1];
        p->_run_time = lr_now() - p->_run_start;
    }

    // move tasks from plan -> pending

    for(int i=0; i<ntask; i++){
        step->_tasks[i]->pend();
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
Job::try_to_do_something(bool try_harder){
    bool leave = 0;

    // if running via update() don't bother waiting for the locks

    if( try_harder ){
        _lock.r_lock();
    }else{
        if( _lock.r_trylock() ) return 0;
    }

    if( _state != JOB_STATE_RUNNING ) leave = 1;
    if( _want_abort ) leave = 1;
    _lock.r_unlock();
    if( leave ) return 0;

    if( try_harder ){
        _lock.w_lock();
    }else{
        if( _lock.w_trylock() ) return 0;
    }

    // nothing running, nothing pending => next phase
    if( _running.empty() && _pending.empty() )
        if( ! next_step_x() ){
            _lock.w_unlock();
            return 0;		// finished
        }
    _lock.w_unlock();

    // RSN - check load ave

    if( try_harder ){
        _lock.w_lock();
    }else{
        if( _lock.w_trylock() ) return 0;
    }

    // can we start anything?
    int st = maybe_start_something_x();

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
        if( t->_last_status + TODOTIMEOUT < now ) t->timedout();
    }

    _lock.w_unlock();
    return 1;
}

int
Job::maybe_specexec(void){

    _lock.r_lock();

    int ntask = _plan[ _stepno ]->_width;
    int nserv = _servers.size();

    int l = 1;
    if( !_stepno ) l = 0;	// we don't replace map tasks

    // not worthwhile
    if( ntask < 5 ) l = 0;
    if( nserv < 5 ) l = 0;

    // too soon
    if( _plan[_stepno]->_run_start > lr_now() - 30 ) l = 0;

    // wait until most finish
    if( _n_task_running >= ntask / 20 ) l = 0;

    _lock.r_unlock();

    if( !l ) return 0;

    _lock.w_lock();

    for(list<ToDo*>::iterator it = _running.begin(); it != _running.end(); it++){
        ToDo *t = *it;
        // speculatively start some alternate tasks. maybe they will finish sooner.
        t->maybe_replace(0);
    }

    _lock.w_unlock();

    return 1;
}

