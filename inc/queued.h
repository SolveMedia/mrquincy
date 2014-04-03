/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-19 10:31 (EDT)
  Function: queued xfers + tasks

*/

#ifndef __mrquincy_queued_h_
#define __mrquincy_queued_h_

#include "mrmagoo.pb.h"

class Queued {
protected:
    RWLock		_lock;
    list<void*>		_queue;
    list<void*>		_running;
    hrtime_t		_last_status;

public:
    void enqueue(void*);
    void *dequeue(void);
    int  nrunning(void);
    void start_more(int);
    bool is_dupe(void*);
    virtual void start(void*) = 0;
    virtual void send_status(void*) = 0;
    virtual bool same(void*, void*) = 0;
    virtual void json1(const char *, void *, string *) = 0;
    virtual void shutdown(void) = 0;
    void done(void*);
    void json(string *);

    void start_or_queue(void*, int);
};

class QueuedXfer : public Queued {

public:
    virtual void start(void*);
    virtual void send_status(void*);
    virtual bool same(void*, void*);
    virtual void json1(const char *, void *, string *);
    virtual void shutdown(void) {};
};

class QueuedTask : public Queued {

public:
    virtual void start(void*);
    virtual void send_status(void*);
    virtual bool same(void*, void*);
    virtual void json1(const char *, void *, string *);
    virtual void shutdown(void);
    void abort(const string *);
};

class QueuedJob : public Queued {

public:
    virtual void start(void*);
    virtual void send_status(void*) {};
    virtual bool same(void*, void*);
    virtual void json1(const char *, void *, string *);
    virtual void shutdown(void);
    void abort(const string *);
    int  update(ACPMRMActionStatus*);
};


#endif //__mrquincy_queued_h_

