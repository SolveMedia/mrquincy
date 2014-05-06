/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-19 10:31 (EDT)
  Function: queued xfers + tasks

*/

#ifndef __mrquincy_queued_h_
#define __mrquincy_queued_h_

#include "mrmagoo.pb.h"

class QueueElem {
    int			_prio;
    hrtime_t		_last_status;
    void		*_elem;
    string		_id;

    QueueElem(void* x, const char *id, int p){ _prio = p; _elem = x; _id = id; }

    friend class Queued;
    friend class QueuedTask;
    friend class QueuedXfer;
    friend class QueuedJob;
};

class Queued {
protected:
    RWLock		_lock;
    list<QueueElem*>	_queue;
    list<QueueElem*>	_running;

public:
    int  nrunning(void);
    void start_more(int);
    bool is_dupe(const char *);
    virtual void start(void*) = 0;
    virtual void send_status(void*) = 0;
    virtual void json1(const char *, void *, string *) = 0;
    void shutdown(void);
    virtual void _abort_q(void*) = 0;
    virtual void _abort_r(void*) = 0;
    void done(void*);
    void json(string *);
    void abort(const char *);

    void start_or_queue(void*, const char *, int, int);
};

class QueuedXfer : public Queued {

public:
    virtual void start(void*);
    virtual void send_status(void*);
    virtual void json1(const char *, void *, string *);
    virtual void _abort_q(void*);
    virtual void _abort_r(void*) {};
};

class QueuedTask : public Queued {

public:
    virtual void start(void*);
    virtual void send_status(void*);
    virtual void json1(const char *, void *, string *);
    virtual void _abort_q(void*);
    virtual void _abort_r(void*);
};

class QueuedJob : public Queued {

public:
    virtual void start(void*);
    virtual void send_status(void*) {};
    virtual void json1(const char *, void *, string *);
    virtual void _abort_q(void*);
    virtual void _abort_r(void*);

    int  update(ACPMRMActionStatus*);
};


#endif //__mrquincy_queued_h_

