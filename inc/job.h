/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-25 11:36 (EDT)
  Function: job

*/
#ifndef __mrquincy_job_h_
#define __mrquincy_job_h_

#include <vector>
#include <list>
#include <string>
using std::vector;
using std::list;
using std::string;

#include "mrmagoo.pb.h"
#include <stdio.h>


class Job;

class Delete {
public:
    string		_filename;

    Delete(const string *);
};

class Server : public NetAddr {
public:
    int			_isup;
    int             	_n_task_running;
    int             	_n_xfer_running;
    int             	_n_dele_running;
    list<Delete*>	_to_delete;

    Server(){ _isup = 1; _n_task_running = _n_xfer_running = _n_dele_running = 0; }
    ~Server();
};

#define JOB_TODO_STATE_NOTREADY 0
#define JOB_TODO_STATE_PENDING	1
#define JOB_TODO_STATE_RUNNING	2
#define JOB_TODO_STATE_FINISHED	3

class ToDo {
protected:
    Job			*_job;
    string		_xid;
    int			_serveridx;
    int			_state;
    int			_tries;

    hrtime_t		_last_status;
    hrtime_t		_delay_until;
    string		_status;
    int			_progress;

    int			update(const string*, int);
    void		pending(void){ _state = JOB_TODO_STATE_PENDING; }

    virtual int		maybe_start(void) = 0;
    virtual void	abort(void) = 0;
    virtual void	cancel(void) = 0;
    virtual void	finished(void) = 0;
    virtual void	failed(void) = 0;
    void		timedout(void);
    void		retry_or_abort(void);
    int			start_check(void);
    void		start_common(void);

public:
    virtual int		start(void) = 0;
    friend class Job;
};

class TaskToDo : public ToDo {
    ACPMRMTaskCreate	_g;
    long long		_totalsize;	// map only

    // stats
    hrtime_t		_run_start;
    int			_run_time;

    TaskToDo(Job *, int);
    int			read_map_plan(FILE*);
    int			wire_files(int, int, int, int);
    void		create_xfers(void);
    void		create_deles(void);

    virtual int		maybe_start(void);
    virtual void	abort(void);
    virtual void	cancel(void);
    virtual void	finished(void);
    virtual void	failed(void);

public:
    virtual int		start(void);

    friend class Job;
    friend class Step;
};

class XferToDo : public ToDo {
    ACPMRMFileXfer	_g;

    virtual int		maybe_start(void);
    virtual void	abort(void);
    virtual void	cancel(void);
    virtual void	finished(void);
    virtual void	failed(void);

public:
    virtual int		start(void);

    XferToDo(Job*, const string *, int, int);

    friend class Job;
};

class Step {
    string		_phase;

    vector<TaskToDo*>	_tasks;

    ~Step();
    int			read_map_plan(Job *, FILE*);

    friend class Job;
};


//****************************************************************

#define JOB_STATE_QUEUED	0
#define JOB_STATE_PLANNING	1
#define JOB_STATE_RUNNING	2
#define JOB_STATE_FINISHED	3
#define JOB_STATE_CLEANUP	4
#define JOB_STATE_DEAD		5

class Job : public ACPMRMJobCreate {

    RWLock		_lock;
    hrtime_t        	_created;
    long long		_totalmapsize;
    bool		_want_abort;
    int             	_state;
    int             	_stepno;
    int             	_n_task_running;
    int             	_n_xfer_running;
    int			_n_threads;

    vector<Server*> 	_servers;
    list<ToDo*>     	_running;
    list<ToDo*>     	_pending;
    vector<Step*>    	_plan;

    // stats...
    hrtime_t		_run_start;
    int			_run_time;
    int			_task_run_time;
    int			_n_tasks_run;
    int			_n_xfers_run;
    int             	_n_deleted;

    void		abort(void);
    int			update(const string*, const string*, int);
    void		send_eu_msg_x(const char *, const char *) const;

    int			plan(void);
    int			plan_servers(void);
    int			plan_map(void);
    int			plan_reduce(void);
    int			plan_files(void);

    int			start_step(void);
    int			start_step_x(void);
    int			next_step_x(void);
    int			cleanup(void);
    int			stop_tasks(void);
    int			try_to_do_something(void);
    int			maybe_start_something_x(void);
    int			check_timeouts(void);
    ToDo*		find_todo_x(const string *) const;
    void		thread_done(void);

    int			server_index(const char *);
    void		enrunning_x(ToDo*);
    void		derunning_x(ToDo*);
    void		log_progress(bool);
    void		json(const char *, string *);
    void		add_delete_x(const string *, int);
    void		do_deletes(void);

public:
    Job();
    ~Job();
    void		run(void);
    void		kvetch(const char *m, const char *a=0, const char *b=0, const char *c=0) const;		// errors
    void		inform(const char *m, const char *a=0, const char *b=0, const char *c=0) const;		// diags
    void		report(const char *m, const char *a=0, const char *b=0, const char *c=0) const;		// stats
    void		report_finish(void) const;
    void		send_server_list(int);

    friend class QueuedJob;
    friend class Step;
    friend class ToDo;
    friend class TaskToDo;
    friend class XferToDo;
};


#define MAXJOB			5		// maximum number of running jobs
#define PLANTIMEOUT		(15 * 60)	// planner program max runtime
#define REDUCEFACTOR		2		// reduce widthe factor

#define TODOSTARTMAX		20		// maximum actions to start at a time
#define TODOTIMEOUT		30
#define TODOMAXFAIL		3
#define TODORETRYDELAY		10
#define JOBMAXTHREAD		5
#define SERVERTASKMAX		10
#define SERVERXFERMAX		20

#endif // __mrquincy_job_h_

