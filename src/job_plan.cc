/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-26 12:27 (EDT)
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

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <strings.h>
#include <signal.h>
#include <poll.h>

#include <sstream>
#include <algorithm>
using std::ostringstream;


#define REDUCEFACTOR		1.95		// reduce width factor
#define REDUCEDECAY		.5
#define WRITE_TIMEOUT		15
#define FILESPEC		"mrtmp/j_%s/out_%03d_%03d_%03d"
//                                    jobid  stepno srctask dsttask

// NB: task #n (normally) runs on server #n (mod #servers)


int
Job::plan(void){

    inform("planning...");

    _lock.w_lock();
    _state = JOB_STATE_PLANNING;
    int nstep = _g.section_size();

    // allocate the steps
    _plan.resize( nstep );

    for(int i=0; i<nstep; i++){
        Step * s = new Step;
        s->_phase = _g.section(i).phase().c_str();
        _plan[i] = s;
    }
    _lock.w_unlock();

    if( ! plan_servers() ) return 0;
    if( ! plan_map() )     return 0;
    if( ! plan_reduce() )  return 0;
    if( ! plan_files() )   return 0;

    _lock.w_lock();
    _state = JOB_STATE_RUNNING;
    _run_start = lr_now();
    _lock.w_unlock();

    inform("done planning");

    _lock.r_lock();
    int maps = _plan[0]->_tasks.size();

    ostringstream b;
    b << "plan: input size: " << (_totalmapsize / 1000000LL)
      << " MB(gz), maps: "    << maps
      << ", reduces: ";

    for(int i=1; i<_plan.size(); i++){
        if( i > 1 ) b << "+";
        b << _plan[i]->_tasks.size();
    }
    if( _plan.size() == 1 ) b << "0";

    const char *bc = b.str().c_str();
    report(bc);

    _lock.r_unlock();

    return 1;
}


// what servers are currently available?
int
Job::plan_servers(void){
    list<NetAddr> servers;

    peerdb->getall(&servers);

    _lock.w_lock();
    for(list<NetAddr>::iterator it=servers.begin(); it != servers.end(); it++){
        Server *s = new Server ;
        NetAddr *n = (NetAddr *)s;
        *n = *it;
        _servers.push_back( s );
    }

    std::random_shuffle( _servers.begin(), _servers.end() );

    _lock.w_unlock();

    DEBUG("found %d servers", _servers.size() );

    return 1;
}

// fork, wire up pipes, exec planner
static int
spawn(Job *j, const char *prog, int fin, int fout){

    int pid = fork();
    if( pid == -1 ){
        j->kvetch("fork failed: %s", strerror(errno));
        return 0;
    }

    // parent => done
    if( pid ) return pid;

    // child

    // rewire fds
    dup2( fin,  0 );
    dup2( fout, 1 );
    for(int i=4; i<256; i++) close(i);

    // build argv
    int argc = 0;
    char *argv[2];

    argv[0] = (char*)prog;
    argv[1] = 0;

    // make sure we are not running as root
    setregid(65535, 65535);
    setreuid(65535, 65535);
    // set an alarm
    alarm( PLANTIMEOUT );

    execv(prog, argv);
    j->kvetch("cannot run planner: %s", strerror(errno));
    exit(-1);
}

static int
run_planner(Job *j, const string *opts, int *pid){
    int fdin[2], fdout[2];

    // create pipes
    if( pipe(fdin) ){
        j->kvetch("pipe failed: %s", strerror(errno));
        return -1;
    }
    if( pipe(fdout) ){
        j->kvetch("pipe failed: %s", strerror(errno));
        close(fdin[0]);
        close(fdin[1]);
        return -1;
    }

    // run planner
    int p = spawn( j, config->plan_prog.c_str(), fdin[0], fdout[1] );
    if(!p){
        close(fdin[0]);
        close(fdin[1]);
        close(fdout[0]);
        close(fdout[1]);
        return -1;
    }

    *pid = p;
    close(fdin[0]);
    close(fdout[1]);

    // send list of servers
    j->send_server_list( fdin[1] );

    // send options data
    write_to( fdin[1], opts->c_str(), opts->size(), WRITE_TIMEOUT );
    write_to( fdin[1], "\n", 1, WRITE_TIMEOUT );
    close( fdin[1] );

    return fdout[0];
}

void
Job::send_server_list(int fd){
    char buf[32];

    int len = snprintf(buf, sizeof(buf), "servers %d\n", _servers.size());
    write_to( fd, buf, len, WRITE_TIMEOUT );
    for(int i=0; i<_servers.size(); i++){
        write_to(fd, _servers[i]->name.c_str(), _servers[i]->name.size(), WRITE_TIMEOUT );
        write_to(fd, "\n", 1, WRITE_TIMEOUT );
    }
}


void
wait_for_fd(int fd){
    struct pollfd pf[1];

    pf[0].fd = fd;
    pf[0].events  = POLLIN;
    pf[0].revents = 0;

    poll( pf, 1, -1 );
}


/*
  task 82
  map mrm@gefiltefish7-r4.ccsphl 105131597 21
  file dancr/2014/03/25/02/2759_prod_DF3u.p9VrdtCRcN8_.gz

*/

int
Job::plan_map(void){
    int pid = 0;

    int fd = run_planner( this, &_g.options(), &pid );
    if( fd == -1 ) return 0;
    FILE *f = fdopen( fd, "r" );
    if( !f ){
        kvetch("fdopen failed: %s", strerror(errno));
        close(fd);
        if( pid ) kill(pid, 9);
        return 0;
    }

    // wait for planner
    wait_for_fd(fd);

    _lock.w_lock();
    // read data from planner
    Step *map = _plan[0];
    int ms = map->read_map_plan(this, f);
    _lock.w_unlock();

    close( fd );
    int exitval;
    waitpid(pid, &exitval, 0);
    if( exitval ) ms = 0;

    return ms;
}

TaskToDo::TaskToDo(Job *j, int sec, int tno){

    _totalsize   = 0;
    _job         = j;
    _state       = 0;
    _tries       = 0;
    _run_start   = 0;
    _run_time    = 0;
    _delay_until = 0;
    _replaces    = 0;
    _replacedby  = 0;
    _taskno      = tno;

    _g.set_jobid(   j->_id );
    _g.set_console( j->_g.console().c_str() );
    _g.set_master(  myipandport.c_str() );

    if( j->_g.has_priority() ){
        _g.set_priority( j->_g.priority() );
    }else{
        // by default, order (roughly) by job creation time
        _g.set_priority( lr_now() >> 8 );
    }

    const ACPMRMJobPhase *jp = &j->_g.section(sec);

    _g.set_phase(   jp->phase().c_str() );
    _g.set_jobsrc(  jp->src().c_str() );
    _g.set_maxrun(  jp->maxrun() );
    _g.set_timeout( jp->timeout() );

    unique( &_xid );
    _g.set_taskid( _xid.c_str() );
}

int
Step::read_map_plan(Job *j, FILE *f){

    // read # tasks
    char buf[32];
    fgets(buf, sizeof(buf), f);
    if( strncmp(buf, "task", 4) ){
        j->kvetch("planner protocol botched: expected task");
        return 0;
    }

    // skip to whitespace
    char *p = buf;
    while( *p && !isspace(*p) ) p++;
    if( *p ) p++;
    int ntask = atoi( p );

    DEBUG("tasks %d", ntask);

    // read in the tasks

    _tasks.resize(ntask);
    _width = ntask;

    for(int i=0; i<ntask; i++){
        TaskToDo *t = new TaskToDo(j, 0, i);
        int s = t->read_map_plan(f);
        _tasks[i] = t;
        if( !s ) return 0;
        j->_totalmapsize += t->_totalsize;
    }

    return 1;
}

int
Job::server_index(const char *s){

    for(int i=0; i<_servers.size(); i++){
        if( ! _servers[i]->name.compare(s) ) return i;
    }

    return -1;
}

int
TaskToDo::read_map_plan(FILE *f){
    char buf[1024];

    fgets(buf, sizeof(buf), f);
    if( strncmp(buf, "map", 3) ){
        _job->kvetch("planner protocol botched: expected map: %s", buf);
        return 0;
    }

    //   map mrm@gefiltefish7-r4.ccsphl 105131597 21

    // skip to server
    char *sns = buf;
    while( *sns && !isspace(*sns) ) sns++;
    if( *sns ) sns++;

    // total size
    char *szs = sns;
    while( *szs && !isspace(*szs) ) szs++;
    if( *szs ) szs ++;

    // #files
    char *nfs = szs;
    while( *nfs && !isspace(*nfs) ) nfs++;
    if( *nfs ) nfs ++;

    if( !*nfs ){
        _job->kvetch("planner protocol botched: malformed map: %s", buf);
        return 0;
    }

    // copy server name, lookup index
    strncpy(buf, sns, szs - sns - 1);
    buf[szs - sns - 1] = 0;

    _serveridx = _job->server_index(buf);
    _totalsize = atoll(szs);
    int nfile  = atoi(nfs);

    if( _serveridx == -1 ){
        _job->kvetch("required server is not available - %s", buf);
        return 0;
    }

    DEBUG("sidx %d size %lld files %d", _serveridx, _totalsize, nfile);

    //   file dancr/2014/03/25/02/2759_prod_DF3u.p9VrdtCRcN8_.gz

    // read in file list
    for(int i=0; i<nfile; i++){
        fgets(buf, sizeof(buf), f);
        // chomp newline
        char *nl = rindex(buf, '\n');
        if(nl) *nl = 0;

        char *fs = buf;
        while( *fs && !isspace(*fs) ) fs++;
        if( *fs ) fs ++;

        _g.add_infile(fs);
    }

    return 1;
}

int
Job::plan_reduce(void){

    _lock.w_lock();
    int nserv = _servers.size();
    int nstep = _g.section_size();

    for(int i=1; i<nstep; i++){
        Step *step = _plan[i];
        int prevw  = _plan[i-1]->_tasks.size();
        int ntask;

        if( _g.section(i).phase() == "final" )
            ntask = 1;	// final
        else if( _g.section(i).has_width() )
            ntask = _g.section(i).width();
        else
            ntask = nserv * REDUCEFACTOR; // QQQ - MAX( nserv * REDUCEFACTOR, int(prevw * REDUCEDECAY) );

        DEBUG("%s ntask %d", step->_phase.c_str(), ntask);

        step->_tasks.resize(ntask);
        step->_width = ntask;

        for(int j=0; j<ntask; j++){
            TaskToDo *t = new TaskToDo(this, i, j);
            // assign a server
            t->_serveridx = j % nserv;
            step->_tasks[j] = t;
        }
    }

    _lock.w_unlock();
    return 1;
}

int
Job::plan_files(void){

    _lock.w_lock();
    int nstep  = _g.section_size();
    int nserv  = _servers.size();

    for(int i=0; i<nstep; i++){
        Step *step = _plan[i];
        int ntask  = step->_tasks.size();
        int infile, outfile;

        if( i == (nstep-1) ){
            // last step
            outfile = 1;
            infile  = _plan[i-1]->_tasks.size();
        }
        else if( i == 0 ){
            // first (map) step
            outfile = (nstep > 1) ? _plan[i+1]->_tasks.size() : 1;
            infile  = 0;	// already figured
        }else{
            outfile = (nstep > i+1) ? _plan[i+1]->_tasks.size() : 1;
            infile  = _plan[i-1]->_tasks.size();
        }

        for(int j=0; j<ntask; j++){
            TaskToDo *t = step->_tasks[j];
            t->wire_files(i, infile, outfile);
        }

        DEBUG("phase %s: tasks %d, files: %d out", step->_phase.c_str(), ntask, outfile);
    }

    _lock.w_unlock();
    return 1;
}

int
TaskToDo::wire_files(int stepno, int infiles, int outfiles){
    char buf[256];

    // outfiles: out_$step_$task_*
    for(int i=0; i<outfiles; i++){
        // ...out_$step_$task_$out
        snprintf(buf, sizeof(buf), FILESPEC, _g.jobid().c_str(), stepno, _taskno, i);
        _g.add_outfile(buf);
    }

    // infiles (map already has infiles): out_$prevstep_*_$task
    if( !_g.infile_size() ){
        for(int i=0; i<infiles; i++){
            snprintf(buf, sizeof(buf), FILESPEC, _g.jobid().c_str(), stepno-1, i, _taskno);
            _g.add_infile(buf);
        }
    }

    return 1;
}

/****************************************************************/

int
TaskToDo::replace(){

    // pick the best server
    int nserv = _job->_servers.size();
    int bestm=-1, besti=0;

    for(int i=0; i<nserv; i++){
        Server *s = _job->_servers[i];
        int m = s->_n_task_running * 1000 + s->_n_fails * 500 + s->_n_xfer_running * 100
            + peerdb->current_load(s->name.c_str())
            + s->_n_task_redo * 1000;

        if( (bestm == -1) || (m < bestm) ){
            besti = i;
            bestm = m;
        }
    }

    replace( besti );
}

// replace a failed or slow task with one on another server
int
TaskToDo::replace(int newsrvr){

    if( _replacedby ) return 0;
    if( _replaces   ) return 0;

    // mostly, it looks like the task it is replacing
    TaskToDo *nt = new TaskToDo( _job, _job->_stepno, _taskno );
    _replacedby    = nt;
    nt->_replaces  = this;
    nt->_serveridx = newsrvr;
    nt->_g.set_priority( _g.priority() );

    nt->wire_files(_job->_stepno, _g.infile_size(), _g.outfile_size());

    // create xfers for input files
    int nserv = _job->_servers.size();
    int ninf  = _g.infile_size();

    _job->kvetch("replacing task %s -> %s, new server %s",
            _xid.c_str(), nt->_xid.c_str(), _job->_servers[newsrvr]->name.c_str());

    // we need to find the input files and get them to the new server
    Step *prevstep = _job->_plan[ _job->_stepno - 1];
    for(int i=0; i<ninf; i++){
        // file i "out_step_$i_server" came from previous step task#i
        // (the other copy is on the down server)
        int src = prevstep->_tasks[i]->_serveridx;

        // if the file originated on the down server, use the backup copy
        if( _serveridx == src ) src = (src+1) % nserv;

        XferToDo *x = new XferToDo(_job, &_g.infile(i), src, newsrvr);
        DEBUG("  + xfer %d -> %d; %s %s", src, newsrvr, _g.infile(i).c_str(), _job->_servers[src]->name.c_str());
        _job->_pending.push_back(x);
        _job->_xfers.push_back(x);

        // we cannot start the task until the files are xfered
        nt->_prerequisite.push_back(x);

        // create deletes for these extra files now
        _job->add_delete_x(&_g.infile(i), newsrvr);
    }

    _job->_servers[newsrvr]->_n_task_redo ++;

    // let'er go
    nt->pend();

    return 1;
}
