/*
  Copyright (c) 2008 by Jeff Weisberg
  Author: Jeff Weisberg <jaw @ tcp4me.com>
  Created: 2008-Dec-27 19:41 (EST)
  Function: 
*/

#ifndef __mrquincy_thread_h_
#define __mrquincy_thread_h_

#ifndef _REENTRANT
#define _REENTRANT
#endif

#include <pthread.h>

int start_thread(void *(*func)(void*), void *arg);


#endif // __mrquincy_thread_h_
