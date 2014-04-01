/*
  Copyright (c) 2008 by Jeff Weisberg
  Author: Jeff Weisberg <jaw @ tcp4me.com>
  Created: 2008-Dec-28 13:03 (EST)
  Function: 
 
  $Id$

*/

#define CURRENT_SUBSYSTEM	'T'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "thread.h"

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

int
start_thread(void *(*func)(void*), void *arg){
    pthread_t *tid = new pthread_t;
    pthread_attr_t attr;
    int err;

    pthread_attr_init(&attr);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    err = pthread_create(tid, &attr, func, arg);

    DEBUG("starting thread %x => %d", tid, err);
    if(err) PROBLEM("cannot create thread: %d", err);

    return err;
}
