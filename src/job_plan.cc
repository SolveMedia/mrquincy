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


#define WRITE_TIMEOUT		15
#define FILESPEC		"mrtmp/j_%s/out_%03d_%03d_%03d"
//                                    jobid  stepno srctask dsttask

// NB: task #n runs on server #n (mod #servers)


int
Job::plan(void){

    inform("planning...");

    _lock.w_lock();
    _state = JOB_STATE_PLANNING;
    int nstep = section_size();

    // allocate the steps
    _plan.resize( nstep );

    for(int i=0; i<nstep; i++){
        Step * s = new Step;
        s->_phase = section(i).phase();
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
    b << "job: " << jobid()
      << " plan: bytes: " << (_totalmapsize / 1000000LL)
      << " MB, maps: " << maps
      << ", reduces: " << (_plan.size() - 1) << "x" << reduce_width();

    const char *bc = b.str().c_str();
    VERBOSE("%s", bc);
    inform(bc);

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

    int fd = run_planner( this, &options(), &pid );
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

TaskToDo::TaskToDo(Job *j, int n){

    _totalsize   = 0;
    _job         = j;
    _state       = 0;
    _tries       = 0;
    _run_start   = 0;
    _run_time    = 0;
    _delay_until = 0;

    _g.set_jobid(   j->jobid() );
    _g.set_console( j->console() );
    _g.set_master(  myipandport );

    const ACPMRMJobPhase *jp = &j->section(n);

    _g.set_phase(   jp->phase() );
    _g.set_jobsrc(  jp->src() );
    _g.set_maxrun(  jp->maxrun() );
    _g.set_timeout( jp->timeout() );

    unique( &_xid );
    _g.set_taskid( _xid );
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
    for(int i=0; i<ntask; i++){
        TaskToDo *t = new TaskToDo(j, 0);
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
    int nstep = section_size();

    if( !has_reduce_width() || !reduce_width() ){
        set_reduce_width( nserv * REDUCEFACTOR );
    }

    int width = reduce_width();

    for(int i=1; i<nstep; i++){
        Step *step = _plan[i];
        int ntask  = step->_phase.compare("final") ? width : 1;

        DEBUG("%s ntask %d", step->_phase.c_str(), ntask);

        step->_tasks.resize(ntask);
        for(int j=0; j<ntask; j++){
            TaskToDo *t = new TaskToDo(this, i);
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
    int nstep  = section_size();
    int width  = reduce_width();
    int nserv  = _servers.size();

    for(int i=0; i<nstep; i++){
        Step *step = _plan[i];
        int ntask  = step->_tasks.size();
        int infile, outfile;

        if( !step->_phase.compare("final") ){
            outfile = 1;
            infile  = _plan[i-1]->_tasks.size();
        }
        else if( !step->_phase.compare("map") ){
            outfile = (nstep > 1) ? _plan[i+1]->_tasks.size() : 1;
            infile  = 0;	// already figured
        }else{
            outfile = (nstep > i+1) ? _plan[i+1]->_tasks.size() : 1;
            infile  = _plan[i-1]->_tasks.size();
        }

        for(int j=0; j<ntask; j++){
            TaskToDo *t = step->_tasks[j];
            t->wire_files(i, j, infile, outfile);
        }

        DEBUG("phase %s: tasks %d, files: %d out", step->_phase.c_str(), ntask, outfile);
    }

    _lock.w_unlock();
    return 1;
}

int
TaskToDo::wire_files(int stepno, int taskno, int infiles, int outfiles){
    char buf[256];

    // outfiles
    for(int i=0; i<outfiles; i++){
        snprintf(buf, sizeof(buf), FILESPEC, _g.jobid().c_str(), stepno, taskno, i);
        _g.add_outfile(buf);
    }

    // infiles (map already has infiles)
    if( !_g.infile_size() ){
        for(int i=0; i<infiles; i++){
            snprintf(buf, sizeof(buf), FILESPEC, _g.jobid().c_str(), stepno-1, i, taskno);
            _g.add_infile(buf);
        }
    }

    return 1;
}

