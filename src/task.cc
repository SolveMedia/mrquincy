/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-18 17:16 (EDT)
  Function: run tasks

*/

#define CURRENT_SUBSYSTEM	't'

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
#include "mapio.h"
#include "euconsole.h"
#include "pipeline.h"

#include "mrmagoo.pb.h"
#include "std_reply.pb.h"

#include <sys/types.h>
#include <poll.h>
#include <sys/socket.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <sys/wait.h>

#include <sstream>
using std::ostringstream;


#define MAXTASK		(config->hw_cpus ? 3 * config->hw_cpus / 2 : 1)
#define TIMEOUT		15
#define MAXTRIES	3	// try running task up to this many times
#define TASKMAXRUN	7200	// RSN - config, task conf
#define TASKTIMEOUT	300	// ''
#define EUBUFSIZE	8192


class Task {
public:
    ACPMRMTaskCreate  _g;
    hrtime_t          _created;
    int               _pid;
    const char       *_status;
    int               _progress;
    int               _runtime;
    bool	      _aborted;

    Task() { _pid = 0; _status = "PENDING"; _progress = 0; _created = lr_now(); _runtime = 0; _aborted = 0; }
};


extern void install_handler(int, void(*)(int));
extern int  create_pipeline(ACPMRMTaskCreate *, int*);

static void *task_periodic(void*);
static void *do_task(void*);
static int  try_task(Task *);
static void run_task_prog(int, Task*);


static QueuedTask	taskq;


void
task_init(void){
    start_thread(task_periodic, 0);
}

void
task_shutdown(void){
    taskq.shutdown();
}

static void *
task_periodic(void *notused){

    while(1){
        // can we start any queued tasks?
        taskq.start_more(MAXTASK);
        sleep(1);
    }
}

int
task_nrunning(void){
    return taskq.nrunning();
}

// handle task request from network
int
handle_task(NTD *ntd){
    protocol_header  *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header  *pho = (protocol_header*) ntd->gpbuf_out;
    Task *req = new Task;

    // parse request
    req->_g.ParsePartialFromArray( ntd->in_data(), phi->data_length );
    DEBUG("l=%d, %s", phi->data_length, req->_g.ShortDebugString().c_str());

    if( ! req->_g.IsInitialized() ){
        DEBUG("invalid request. missing required fields");
        exit(-1);
        return 0;
    }

    DEBUG("recvd task request");

    taskq.start_or_queue( (void*)req, req->_g.taskid().c_str(), req->_g.priority(), MAXTASK );

    return reply_ok(ntd);
}

int
handle_taskabort(NTD *ntd){
    protocol_header  *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header  *pho = (protocol_header*) ntd->gpbuf_out;
    ACPMRMTaskAbort req;

    // parse request
    req.ParsePartialFromArray( ntd->in_data(), phi->data_length );
    DEBUG("l=%d, %s", phi->data_length, req.ShortDebugString().c_str());

    if( ! req.IsInitialized() ){
        DEBUG("invalid request. missing required fields");
        return 0;
    }

    DEBUG("recvd task abort %s", req.taskid().c_str());

    taskq.abort(req.taskid().c_str());

    return reply_ok(ntd);
}


void
QueuedTask::_abort_q(void *x){
    Task *t = (Task*)x;

    delete t;
}

void
QueuedTask::_abort_r(void *x){
    Task *t = (Task*)x;

    t->_aborted = 1;
    if( t->_pid ) kill( t->_pid, 3 );
}

void
QueuedTask::start(void *xg){
    Task *t = (Task*)xg;

    start_thread(do_task, (void*)t);
}

void
QueuedTask::send_status(void *xg){
    Task *t = (Task*)xg;
    const ACPMRMTaskCreate *g = & t->_g;
    ACPMRMActionStatus st;

    if( !g->has_master() || g->master().empty() ) return;

    st.set_jobid( g->jobid().c_str() );
    st.set_xid( g->taskid().c_str() );
    st.set_phase( t->_status );
    st.set_progress( t->_progress );

    DEBUG("sending status to %s", g->master().c_str());

    toss_request( udp4_fd, g->master().c_str(), PHMT_MR_TASKSTATUS, &st);
}

// the final status goes over tcp
static void
send_final_status(const Task *t){
    ACPMRMActionStatus st;
    const ACPMRMTaskCreate *g = & t->_g;

    if( !g->has_master() || g->master().empty() ) return;

    st.set_jobid( g->jobid().c_str() );
    st.set_xid( g->taskid().c_str() );
    st.set_phase( t->_status );
    st.set_progress( t->_progress );
    st.set_final_amount(  t->_runtime );

    DEBUG("sending final status to %s", g->master().c_str());

    make_request(g->master().c_str(), PHMT_MR_TASKSTATUS, &st, TIMEOUT );
}

void
json_task(string *dst){
    taskq.json(dst);
}

void
QueuedTask::json1(const char *st, void *x, string *dst){
    Task *t = (Task*)x;
    const ACPMRMTaskCreate *g = & t->_g;
    ostringstream b;

    b << "{\"jobid\": \""       << g->jobid()   << "\", "
      << "\"taskid\": \""       << g->taskid()  << "\", "
      << "\"status\": \""       << st           << "\", "
      << "\"phase\": \""        << g->phase()   << "\", "
      << "\"start_time\": "     << t->_created  << ", "
      << "\"pid\": "            << t->_pid
      << "}";

    dst->append(b.str().c_str());
}

static void *
do_task(void *x){
    Task *t = (Task*)x;
    const ACPMRMTaskCreate *g = & t->_g;
    bool ok = 1;

    t->_status = "STARTING";
    taskq.send_status(x);
    t->_status = "RUNNING";

    //  try several times
    int tries = MAXTRIES;
    for(int i=0; i<tries; i++){
        hrtime_t start = lr_now();
        ok = try_task(t);
        t->_runtime = lr_now() - start;
        if( ok ) break;
        if( t->_runtime >= TASKTIMEOUT/2 ) break;
        if( t->_aborted ) break;
        sleep(5);
    }

    if( ok )
        t->_status = "FINISHED";
    else
        t->_status = "FAILED";

    DEBUG("task done (%s) %s - %d sec", t->_status, g->taskid().c_str(), t->_runtime);
    send_final_status(t);

    taskq.done(x);
    delete t;

    // and start another one...
    taskq.start_more(MAXTASK);
}

/****************************************************************/

extern int tcp4_fd;
static void
close_files(int pfd){
    int maxfd = getdtablesize();
    DEBUG("maxfd %d", maxfd);

    // close most fds
    for(int i=tcp4_fd; i<maxfd; i++){
        if( i == pfd ) continue;
        close(i);
    }
}

static void
read_progress(int fd, Task *t){
    int progress;

    // if the child sends something, it should be integer progress
    // NB. nothing bad happens if this blocks
    int r = read(fd, &progress, sizeof(int));
    if( r != sizeof(int) ) return;
    t->_progress = progress;
    DEBUG("progress %d", progress);
}

// start the task in a child process + wait for it to finish
// use a pipe to detect the child ending

static int
try_task(Task *t){
    const ACPMRMTaskCreate *g = & t->_g;

    DEBUG("task running %s", g->taskid().c_str());

    // create pipe
    int pipfd[2];	// [read, write]
    int pr = pipe( pipfd );
    if( pr ){
        VERBOSE("cannot open pipes: ", strerror(errno));
        return 0;
    }

    // fork child
    int pid = fork();
    if( pid == -1 ){
        VERBOSE("cannot fork: %s", strerror(errno));
        close(pipfd[0]);
        close(pipfd[1]);
        return 0;
    }
    if( pid == 0 ){
        // child
        close(pipfd[0]);
        close_files(pipfd[1]);
        run_task_prog(pipfd[1], t);
        _exit(-1);
    }
    // parent
    t->_pid = pid;
    close(pipfd[1]);

    DEBUG("child pid %d - running", pid);


    int maxrun = g->has_maxrun() ? g->maxrun() : 0;
    if( !maxrun ) maxrun = TASKMAXRUN;
    hrtime_t timeout = lr_now() + maxrun;

    // pipe will poll as readable/hup when child exits
    struct pollfd pf[1];

    while( 1 ){
        pf[0].fd = pipfd[0];
        pf[0].events  = POLLIN;
        pf[0].revents = 0;

        int to = timeout - lr_now();
        if( to <= 0 ) break;

        pr = poll( pf, 1, to * 1000 );
        DEBUG("poll done %d %d %x", pr, errno, pf[0].revents);
        if( pf[0].revents & (POLLHUP | POLLERR) ) break;

        // if any data comes through, suck it in
        if( pf[0].revents & POLLIN ) read_progress(pipfd[0], t);
    }

    close(pipfd[0]);

    // if it didn't finish, kill it
    if( lr_now() >= timeout ){
        kill(pid, 9);
    }

    // wait for it
    int exitval;
    waitpid(pid, &exitval, 0);

    DEBUG("child pid %d exited %d", pid, exitval);

    if( exitval ) VERBOSE("task child exited %d", exitval);

    return (exitval==0) ? 1 : 0;

}

/****************************************************************/

static void
run_task_sig(int sig){
    // 0 => all processes in my process group
    VERBOSE("signal %d: abort", sig);
    kill(0, 9);
    _exit(1);
}

static void
setup_process(int need){

    setsid();

    // all signals -> abort
    for(int i=1; i<32; i++){
        if( i == 18 ) continue;		// sig child
        install_handler(i, run_task_sig);
    }

    // increase open file limit - we have lots of output files
    struct rlimit fdrl;
    getrlimit(RLIMIT_NOFILE, &fdrl);
    fdrl.rlim_cur = fdrl.rlim_max;
    setrlimit(RLIMIT_NOFILE, &fdrl);

    if( fdrl.rlim_max < need ){
        VERBOSE("not enough file descriptors available (have %d, need %d)", fdrl.rlim_max, need);
        _exit(-1);
    }

    int r = chdir( config->basedir.c_str() );
    if( r < 0 )
        FATAL("cannot chdir %s: %s", config->basedir.c_str(), strerror(errno));
}


/****************************************************************/

// set up io and run the program

static void
run_task_prog(int parent_fd, Task *t){
    hrtime_t t0 = lr_now();
    const ACPMRMTaskCreate *g = & t->_g;

    setup_process(g->outfile_size() + 8);

    // there is a possibility of deadlock:
    // if another thread malloc()s during the fork, this process will inherit the lock
    // and malloc will hang.
    // workarounds:
    //   alarm + retry
    //   serialize g -> tmpfile + execv
    //   fix malloc
    // NB: this runs in a seperate process because it needs ~37 gajillion file descriptors
    // and is process group leader of the pipeline process group

    // for end user output
    DEBUG("setting up eu consoles");
    alarm(10);
    EUConsole eu_out("stdout", g);
    EUConsole eu_err("stderr", g);
    char *eubuf = (char*)malloc(EUBUFSIZE);
    if( !eubuf ) FATAL("out of memory");
    alarm(0);

    // create the processing pipeline (eg. gzcat files | sort | program)
    //   progfd 0 - is program stdout, 1 is stderr, 2 is crunched data
    DEBUG("creating pipeline");
    int progfd[3];
    Pipeline pl(g, progfd);

    // create buffered input - to read data from user program
    DEBUG("setting up input buffer");
    BufferedInput inbuf(progfd[2]);

    // open outfiles - data from user program gets directed to many possible files
    DEBUG("creating output files");
    MapOutSet out(g);

    // RSN - thread{ sleep, send status }

    DEBUG("running task io loop");
    int tasktimeout = g->timeout();
    if( !tasktimeout ) tasktimeout = TASKTIMEOUT;
    DEBUG("task timeout %d", tasktimeout);

    while(1){
        // wait for prog to send data or timeout
        struct pollfd pf[3];
        for(int i=0; i<3; i++){
            pf[i].fd = progfd[i];
            pf[i].events  = POLLIN;
            pf[i].revents = 0;
        }

        int r = poll(pf, 3, tasktimeout * 1000);
        if( r == -1 && (errno == EINTR || errno == EAGAIN) ) continue;
        if( r <= 0 ){
            VERBOSE("task timeout (%d)", int(lr_now() - t0));
            pl.abort();
            _exit(1);
        }

        //DEBUG("poll => %d, %x/%x/%x", r, pf[0].revents, pf[1].revents, pf[2].revents);

        // read + process

        if( pf[2].revents & POLLIN ){
            inbuf.read( &out );
        }

        if( pf[0].revents & POLLIN ){
            int r = read(progfd[0], eubuf, EUBUFSIZE);
            //DEBUG("read eu-out %d", r);
            if( r > 0 ) eu_out.send(eubuf, r);
        }
        if( pf[1].revents & POLLIN ){
            int r = read(progfd[1], eubuf, EUBUFSIZE);
            //DEBUG("read eu-err %d", r);
            if( r > 0 ) eu_err.send(eubuf, r);
        }

        // done
        if( (pf[0].revents & (POLLHUP | POLLERR))
            && (pf[1].revents & (POLLHUP | POLLERR))
            && (pf[2].revents & (POLLHUP | POLLERR)) ) break;

    }


    // get program exit value - use it as our exit value
    int exitval = pl.waitpid();

    // close outfiles
    out.close();

    eu_out.done();
    eu_err.done();
    pl.done();

    hrtime_t t1 = lr_now();
    DEBUG("run time: %d", t1 - t0);

    if( exitval ) VERBOSE("task pipeline exited %d", exitval);

    _exit( exitval ? -1 : 0 );
}

/*
  a task ends up running as:

  a thread doing do_task

  (which starts up)
  a process running run_task_prog

  (which starts up)
  a pipeline of processes including the end-user program, sort, and gzcat

*/
