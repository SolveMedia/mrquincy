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

#include <unistd.h>

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


void
Queued::start_or_queue(void *g, const char *id, int prio, int max){

    _lock.w_lock();

    if( is_dupe(id) ){
        _lock.w_unlock();
        DEBUG("dupe request %s", id);
        return;
    }

    QueueElem *e = new QueueElem(g, id, prio);
    e->_last_status = lr_now() - random_n(MINSTATUS);

    if( nrunning() >= max ){

        // insert in priority order
        list<QueueElem*>::iterator it;
        for(it=_queue.begin(); it != _queue.end(); it++){
            QueueElem *c = *it;
            if( c->_prio > prio ) break;
        }
        _queue.insert(it, e);

    }else{
        _running.push_back(e);
        start(g);
    }
    _lock.w_unlock();
}

bool
Queued::is_dupe(const char *id){
    bool dupe = 0;

    for(list<QueueElem*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        QueueElem *e = *it;
        if( e->_id == id ){
            dupe = 1;
            break;
        }
    }
    for(list<QueueElem*>::iterator it=_running.begin(); it != _running.end(); it++){
        QueueElem *e = *it;
        if( e->_id == id ){
            dupe = 1;
            break;
        }
    }

    return dupe;
}

void
Queued::json(string *dst){
    int n = 0;

    dst->append("[");

    _lock.r_lock();
    for(list<QueueElem*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        QueueElem *e = *it;
        void *x = e->_elem;
        if( n++ ) dst->append(",\n    ");
        json1("queued", x, dst);
    }
    for(list<QueueElem*>::iterator it=_running.begin(); it != _running.end(); it++){
        QueueElem *e = *it;
        void *x = e->_elem;
        if( n++ ) dst->append(",\n    ");
        json1("running", x, dst);
    }
    _lock.r_unlock();

    dst->append("]");
}


void
Queued::abort(const char *id){

    QueueElem *found = 0;

    _lock.w_lock();

    for(list<QueueElem*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        QueueElem *e = *it;
        if( e->_id == id ){
            found = e;
            break;
        }
    }
    if( found ){
        _queue.remove(found);
        _abort_q( found->_elem );
        delete found;
    }else{
        for(list<QueueElem*>::iterator it=_running.begin(); it != _running.end(); it++){
            QueueElem *e = *it;
            if( e->_id == id ){
                found = e;
                break;
            }
        }
        if( found ){
            _running.remove(found);
            _abort_r( found->_elem );
        }
    }

    _lock.w_unlock();
}

// system is shutting down - kill running tasks, drain the queue
void
Queued::shutdown(void){

    _lock.w_lock();

    for(list<QueueElem*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        QueueElem *e = *it;
        _abort_q( e->_elem );
        delete e;
    }
    _queue.clear();

    for(list<QueueElem*>::iterator it=_running.begin(); it != _running.end(); it++){
        QueueElem *e = *it;
        _abort_r( e->_elem );
    }
    _running.clear();

    _lock.w_unlock();
}

void
Queued::done(void *g){

    _lock.w_lock();

    // find + remove
    for(list<QueueElem*>::iterator it=_running.begin(); it != _running.end(); it++){
        QueueElem *e = *it;
        if( e->_elem == g ){
            _running.remove(e);
            delete e;
            break;
        }
    }

    _lock.w_unlock();
}

void
Queued::start_more(int max){

    _lock.w_lock();
    while( 1 ){
        if( _queue.empty() ) break;
        if( _running.size() >= max ) break;

        QueueElem *e = _queue.front();
        void *g = e->_elem;
        _queue.pop_front();
        _running.push_back(e);
        start(g);
    }
    _lock.w_unlock();

    // send statuses
    hrtime_t old = lr_now() - MINSTATUS;

    _lock.r_lock();
    for(list<QueueElem*>::iterator it=_queue.begin(); it != _queue.end(); it++){
        QueueElem *e = *it;
        void *g = e->_elem;
        if( e->_last_status > old ) continue;
        send_status(g);
        usleep( 1000 );
    }
    for(list<QueueElem*>::iterator it=_running.begin(); it != _running.end(); it++){
        QueueElem *e = *it;
        void *g = e->_elem;
        if( e->_last_status > old ) continue;
        send_status(g);
        usleep( 1000 );
    }
    _lock.r_unlock();
}

