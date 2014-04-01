/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-27 14:56 (EDT)
  Function: cleanup finished job

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

#define MAXFILES	50
#define TIMEOUT		30



Delete::Delete(const string *file){
    _filename  = *file;
}

void
Job::add_delete_x(const string *file, int idx){
    Delete *d = new Delete (file);

    _servers[idx]->_to_delete.push_back(d);
}

// add this task's files to the list to be deleted
void
TaskToDo::create_deles(void){

    int noutf = _g.outfile_size();
    int nserv = _job->_servers.size();
    for(int i=0; i<noutf; i++){
        _job->add_delete_x(&_g.outfile(i), _serveridx);		// src
        _job->add_delete_x(&_g.outfile(i), i % nserv);		// dst
        // backup - (i+1)%nserv
    }
}

// cancel all running tasks
int
Job::stop_tasks(void){

    _lock.r_lock();
    _pending.clear();

    // try several times
    for(int i=0; i<2; i++){
        list<ToDo*> runtmp = _running;

        // (try to) abort anything running
        for(list<ToDo*>::iterator it=runtmp.begin(); it != runtmp.end(); it++){
            ToDo *t = *it;
            t->cancel();
        }

        if( _running.empty() ) break;
        sleep(1);
    }

    _lock.r_unlock();

    return 1;
}

// delete all of the tmp files
void
Job::do_deletes(void){
    ACPMRMFileDel req;
    int ndele = 0;

    string jobdir = "mrtmp/j_";	// see also: job_plan.cc
    jobdir.append( jobid() );


    _lock.r_lock();

    for(int idx=0; idx<_servers.size(); idx++){
        Server *srvr = _servers[idx];
        req.clear_filename();

        for(list<Delete*>::iterator it=srvr->_to_delete.begin(); it != srvr->_to_delete.end(); it++){
            Delete *d = *it;
            req.add_filename( d->_filename );
            ndele++;

            if( req.filename_size() >= MAXFILES )
                make_request(srvr, PHMT_MR_FILEDEL, &req, TIMEOUT);
        }

        // remove job dir
        req.add_filename( jobdir );

        if( req.filename_size() )
            make_request(srvr, PHMT_MR_FILEDEL, &req, TIMEOUT);
    }

    _lock.r_unlock();

    _lock.w_lock();
    _n_deleted = ndele;
    _lock.w_unlock();
}



// NB: cleanup always happens in the main job thread

int
Job::cleanup(void){

    stop_tasks();
    do_deletes();

    return 1;
}


