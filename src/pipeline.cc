/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-21 16:04 (EDT)
  Function: the running task pipeline

*/

#define CURRENT_SUBSYSTEM	't'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "misc.h"
#include "network.h"
#include "pipeline.h"


#include "mrmagoo.pb.h"
#include "std_reply.pb.h"

#include <sys/types.h>
#include <poll.h>
#include <sys/socket.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <libgen.h>


// RSN - config
#define CATPROG		"/usr/bin/cat"
#define GZCATPROG	"/usr/bin/gzcat"
#define SORTPROG	"/usr/bin/sort"


static int spawn(const char *, const ACPMRMTaskCreate *, int, int, int, int);



Pipeline::~Pipeline(){
    _cleanup();
}

void
Pipeline::_cleanup(void){
    // remove tmpfile
    if( ! _tmpfile.empty() ) unlink( _tmpfile.c_str() );
}

void
Pipeline::abort(void){
    DEBUG("abort");
    _cleanup();
    if( _pid ) kill( _pid, 9 );
}

bool
Pipeline::still_producing(void){

    // is the input pipeline still running?
    int ev;
    int w = ::waitpid( _inpid, &ev, WNOHANG );

    return (w == _inpid) ? 0 : 1;
}

int
Pipeline::waitpid(void){
    int w, exitval;

    for(int t=0; t<10; t++){
        w = ::waitpid( _pid, &exitval, WNOHANG );
        if( w == _pid ){
            // finished
            DEBUG("wait value %d", exitval);
            _cleanup();
            return exitval;
        }
        // not done yet? wait a bit, maybe kill it
        if( t > 7 ) kill( _pid, 3 );
        sleep(1);
    }

    // kill 9
    abort();
    ::waitpid( _pid, &exitval, 0 );
    DEBUG("wait again value %d", exitval);
    return exitval;
}

static void
_fail(const char *msg){
    // uh oh!
    VERBOSE("%s: %s", msg, strerror(errno));
    exit(-1);
}

static int
set_nbio(int fd){
    fcntl(fd, F_SETFL, O_NDELAY);
    return fd;
}

// create one of:
// map:
//   cat files | prog
//   gzcat files | prog
// reduce:
//   sort files | prog
//   gzcat files | sort | prog


Pipeline::Pipeline(const ACPMRMTaskCreate *g, int* outfds){

    // save job src in tmp file
    _tmpfile = config->basedir;
    _tmpfile.append("/mrtmp/bin");
    mkdirp( _tmpfile.c_str(), 0777 );
    _tmpfile.append("/p");
    unique( &_tmpfile );

    DEBUG("tmpfile: %s", _tmpfile.c_str());

    int tfd = open( _tmpfile.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0755 );
    if( tfd < 0 ) _fail("open failed");
    const string *jobsrc = & g->jobsrc();
    write( tfd, jobsrc->c_str(), jobsrc->size() );
    close(tfd);


    // [read write]
    int pprogin[2];	// pipe into prog
    int pprogout[2];	// std out from prog
    int pprogerr[2];	// std err from prog
    int pprogdat[2];	// data from prog
    int pinterm[2];	// intermediate pipe, if needed
    int unusedfd = pprogerr[1];	// tie unneeded fds to stderr

    // set up pipes for the end-user program
    DEBUG("creating pipes");
    if( pipe(pprogin) )  _fail( "pipe failed");
    if( pipe(pprogout) ) _fail( "pipe failed");
    if( pipe(pprogerr) ) _fail( "pipe failed");
    if( pipe(pprogdat) ) _fail( "pipe failed");

    // attach these (non-blockingly) back to task.cc:run_task_prog()
    outfds[0] = set_nbio( pprogout[0] );
    outfds[1] = set_nbio( pprogerr[0] );
    outfds[2] = set_nbio( pprogdat[0] );

    // spawn the end-user program
    _pid = spawn( _tmpfile.c_str(), 0, pprogin[0], pprogout[1], pprogerr[1], pprogdat[1] );
    DEBUG("spawn job prog: %d", _pid);

    // what do we need to run?
    // XXX - currently, all files are assumed to be compressed.
    // should be configurable.
    const char *e1=0, *e2=0;

    if( !g->phase().compare("map") ){
        // gzcat files
        e1 = GZCATPROG;
        pinterm[1] = pprogin[1];
    }else{
        // gzcat files | sort
        // RSN - sort files
        e1 = GZCATPROG;
        e2 = SORTPROG;

        if( pipe(pinterm) ) _fail( "pipe failed");
    }

    // intermediate?
    if( e2 ){
        int p2 = spawn( e2, 0, pinterm[0], pprogin[1], pprogerr[1], unusedfd );
        DEBUG("spawn %s: %d", e2, p2);
        _inpid = p2;
    }

    // initial prog (with files)
    // QQQ - stdin?
    int p1 = spawn( e1, g, unusedfd, pinterm[1], pprogerr[1], 0 );
    DEBUG("spawn %s %d", e1, p1);
    if( !e2 ) _inpid = p1;

    // close the far ends of the pipes
    close(pprogin[0]);
    close(pprogin[1]);
    close(pprogout[1]);
    close(pprogerr[1]);
    close(pprogdat[1]);
    if( e2 ){
        close(pinterm[0]);
        close(pinterm[1]);
    }

}


static int
spawn(const char *prog, const ACPMRMTaskCreate *g, int fin, int fout, int ferr, int fdat){

    // fork
    // wire up fds
    // exec

    int pid = fork();
    if( pid == -1 ) _fail("fork failed");

    // parent => done
    if( pid ) return pid;

    // child

    // rewire fds
    dup2( fin,  0 );
    dup2( fout, 1 );
    dup2( ferr, 2 );
    dup2( fdat, 3 );
    for(int i=4; i<256; i++) close(i);

    // build argv
    int argc = g ? g->infile_size() : 0;
    const char * argv[argc + 2];

    argv[0] = (char*)prog;
    for(int i=0; i<argc; i++){
        argv[i+1] = g->infile(i).c_str();
    }

    argv[argc+1] = 0;

    // make sure we are not running as root
    setregid(65535, 65535);
    setreuid(65535, 65535);

    signal( SIGPIPE, SIG_DFL );

    execv(prog, (char**)argv);
    VERBOSE("exec failed %s: %s", prog, strerror(errno));
    _fail("exec failed");
}

