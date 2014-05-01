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

    DISALLOW_COPY(Delete);
};

class Server : public NetAddr {
public:
    int			_isup;
    int             	_n_task_running;
    int             	_n_xfer_running;	// xfers running on this server
    int			_n_xfer_peering;	// xfers running elsewhere pulling from this server
    int             	_n_dele_running;
    int			_n_fails;		// tasks which failed on this server
    int			_n_task_redo;		// replacement tasks sent to this server
    list<Delete*>	_to_delete;

    Server(){ _isup = 1; _n_task_running = _n_xfer_running = _n_xfer_peering = _n_dele_running = _n_fails = 0; _n_task_redo = 0;}
    ~Server();

    DISALLOW_COPY(Server);
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

    ToDo()		{ }
    int			update(const string*, int, int);
    void		pending(void){ _state = JOB_TODO_STATE_PENDING; }

    virtual int		maybe_start(void) = 0;
    virtual int		maybe_replace(bool) = 0;
    virtual void	abort(void) = 0;
    virtual void	cancel(void) = 0;
    virtual void	finished(int) = 0;
    virtual void	failed(bool) = 0;
    void		timedout(void);
    void		retry_or_abort(bool);
    int			start_check(void);
    void		start_common(void);
    void		pend(void);

public:
    virtual int		start(void) = 0;
    bool		is_finished(void){ return _state == JOB_TODO_STATE_FINISHED; }

    friend class Job;
    DISALLOW_COPY(ToDo);
};

class TaskToDo : public ToDo {
    ACPMRMTaskCreate	_g;
    long long		_totalsize;	// map only
    int			_taskno;

    // stats
    hrtime_t		_run_start;
    int			_run_time;

    TaskToDo		*_replacedby;
    TaskToDo		*_replaces;
    list<ToDo*>		_prerequisite;

    TaskToDo(Job *, int, int);
    int			read_map_plan(FILE*);
    int			wire_files(int, int, int);
    void		create_xfers(void);
    void		create_deles(void);
    int			replace(int);
    int			replace(void);
    void		cancel_light(void);
    void		discard(void);
    virtual int		maybe_start(void);
    virtual int		maybe_replace(bool);
    virtual void	abort(void);
    virtual void	cancel(void);
    virtual void	finished(int);
    virtual void	failed(bool);

public:
    virtual int		start(void);

    friend class Job;
    friend class Step;
    DISALLOW_COPY(TaskToDo);
};

class XferToDo : public ToDo {
    ACPMRMFileXfer	_g;
    int			_peeridx;

    virtual int		maybe_start(void);
    virtual int		maybe_replace(bool);
    virtual void	abort(void);
    virtual void	cancel(void);
    virtual void	finished(int);
    virtual void	failed(bool);

public:
    virtual int		start(void);

    XferToDo(Job*, const string *, int, int);

    friend class Job;
    DISALLOW_COPY(XferToDo);
};

class Step {
    string		_phase;
    int			_width;

    vector<TaskToDo*>	_tasks;

    // stats
    hrtime_t		_run_start;
    int			_run_time;
    long long		_xfer_size;
    int			_n_xfers_run;

    Step(){ _run_start = 0; _run_time = 0; _xfer_size = 0; _n_xfers_run = 0; _width = 0; }
    ~Step();
    int			read_map_plan(Job *, FILE*);
    void		report_final_stats(Job *);

    friend class Job;
    friend class XferToDo;
    friend class TaskToDo;
    DISALLOW_COPY(Step);
};


//****************************************************************

#define JOB_STATE_QUEUED	0
#define JOB_STATE_PLANNING	1
#define JOB_STATE_RUNNING	2
#define JOB_STATE_FINISHED	3
#define JOB_STATE_CLEANUP	4
#define JOB_STATE_DEAD		5

class Job {
    ACPMRMJobCreate	_g;
    RWLock		_lock;
    const char 		*_id;
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
    list<XferToDo*>	_xfers;
    vector<Step*>    	_plan;

    // stats...
    hrtime_t		_run_start;
    int			_run_time;
    int			_task_run_time;
    int			_n_tasks_run;
    int			_n_xfers_run;
    int             	_n_deleted;
    int			_n_fails;

    void		abort(void);
    int			update(const string*, const string*, int, int);
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
    int			try_to_do_something(bool);
    int			maybe_start_something_x(void);
    int			check_timeouts(void);
    int			maybe_specexec(void);
    ToDo*		find_todo_x(const string *) const;
    void		thread_done(void);

    int			server_index(const char *);
    void		enrunning_x(ToDo*);
    void		derunning_x(ToDo*);
    void		depending_x(ToDo*);
    void		log_progress(bool);
    void		json(const char *, string *);
    void		add_delete_x(const string *, int);
    void		do_deletes(void);
    void		report_final_stats(void);
    float		efficency_x(void);

public:
    Job();
    ~Job();
    int			init(int, const char *, int);
    int			priority(void){ return _g.priority(); }
    int			current_width(void){ return _plan[_stepno]->_tasks.size(); }
    void		run(void);
    void		kvetch(const char *m, const char *a=0, const char *b=0, const char *c=0, const char *d=0) const;		// errors
    void		inform(const char *m, const char *a=0, const char *b=0, const char *c=0, const char *d=0) const;		// diags
    void		inform2(const char *m, const char *a=0, const char *b=0, const char *c=0, const char *d=0) const;		// diags + log
    void		report(const char *m, const char *a=0, const char *b=0, const char *c=0, const char *d=0) const;		// stats
    void		notify_finish(void) const;
    void		send_server_list(int);

    friend class QueuedJob;
    friend class Step;
    friend class ToDo;
    friend class TaskToDo;
    friend class XferToDo;
    DISALLOW_COPY(Job);
};


#define PLANTIMEOUT		(15 * 60)	// planner program max runtime

#define TODOSTARTMAX		20		// maximum actions to start at a time
#define TODOTIMEOUT		30
#define TODOMAXFAIL		3
#define JOBMAXTHREAD		5

#endif // __mrquincy_job_h_

