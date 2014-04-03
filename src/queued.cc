/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-19 10:35 (EDT)
  Function: 

*/
#define CURRENT_SUBSYSTEM	'q'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "misc.h"
#include "network.h"
#include "runmode.h"
#include "thread.h"
#include "lock.h"
#include "queued.h"

#include "mrmagoo.pb.h"
#include "std_reply.pb.h"

#define MINSTATUS	5


int
Queued::nrunning(void){
    _lock.r_lock();
    int n = _running.size();
    _lock.r_unlock();
    return n;
}

void *
Queued::dequeue(void){

    void *g = 0;

    _lock.w_lock();
    if( !_queue.empty() ){
        g = _queue.front();
        _queue.pop_front();
    }
    _lock.w_unlock();
    return g;
}

void
Queued::start_or_queue(void *g, int max){

    if( is_dupe(g) ) return;

    _lock.w_lock();

    if( _queue.empty() && _running.empty() ) _last_status = lr_now();

    if( nrunning() >= max ){
        _queue.push_back(g);
    }else{
        _running.push_back(g);
        start(g);
    }
    _lock.w_unlock();
}

bool
Queued::is_dupe(void *g){
    bool dupe = 0;

    _lock.r_lock();
    for(list<void*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        void *x = *it;
        if( same(g, x) ){
            dupe = 1;
            break;
        }
    }
    for(list<void*>::iterator it=_running.begin(); it != _running.end(); it++){
        void *x = *it;
        if( same(g, x) ){
            dupe = 1;
            break;
        }
    }
    _lock.r_unlock();

    return dupe;
}

void
Queued::json(string *dst){
    int n = 0;

    dst->append("[");

    _lock.r_lock();
    for(list<void*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        void *x = *it;
        if( n++ ) dst->append(",\n    ");
        json1("queued", x, dst);
    }
    for(list<void*>::iterator it=_running.begin(); it != _running.end(); it++){
        void *x = *it;
        if( n++ ) dst->append(",\n    ");
        json1("running", x, dst);
    }
    _lock.r_unlock();

    dst->append("]");
}

void
Queued::done(void *g){

    _lock.w_lock();
    _queue.remove(g);
    _running.remove(g);
    _lock.w_unlock();
}

void
Queued::start_more(int max){

    _lock.w_lock();
    while( 1 ){
        if( _queue.empty() ) break;
        if( _running.size() >= max ) break;

        void *g = _queue.front();
        _queue.pop_front();
        _running.push_back(g);
        start(g);
    }
    _lock.w_unlock();

    // send statuses
    if( _last_status > lr_now() - MINSTATUS ) return;
    _last_status = lr_now();

    _lock.r_lock();
    for(list<void*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        void *g = *it;
        send_status(g);
    }
    for(list<void*>::iterator it=_running.begin(); it != _running.end(); it++){
        void *g = *it;
        send_status(g);
    }
    _lock.r_unlock();
}

