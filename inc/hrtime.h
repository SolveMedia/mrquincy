/*
  Copyright (c) 2008 by Jeff Weisberg
  Author: Jeff Weisberg <jaw @ tcp4me.com>
  Created: 2008-Dec-27 19:18 (EST)
  Function: high-resolution time
*/

#ifndef __mrquincy_hrtime_h_
#define __mrquincy_hrtime_h_

#include <sys/time.h>

#ifndef HAVE_HRTIME
typedef long long hrtime_t;
inline hrtime_t gethrtime(void){
    struct timeval tv;
    gettimeofday(&tv, 0);
    return (hrtime_t)( (long long)tv.tv_sec * 1000000LL + (long long)tv.tv_usec) * 1000LL;
}
#endif

#define hr_now()	gethrtime()
#define lr_now()	time(0)

#define ONE_SECOND_HR	1000000000LL
#define ONE_MSEC_HR	1000000LL

#endif // __mrquincy_hrtime_h_
