/*
  Copyright (c) 2008 by Jeff Weisberg
  Author: Jeff Weisberg <jaw @ tcp4me.com>
  Created: 2008-Dec-27 19:08 (EST)
  Function: 
*/

#ifndef __mrquincy_defs_h_
#define __mrquincy_defs_h_

#define DEBUGING	1

#define DISALLOW_COPY(T) \
	T(const T &);	\
	void operator=(const T&)

#endif // __mrquincy_defs_h_
