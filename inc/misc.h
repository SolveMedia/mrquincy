/*
  Copyright (c) 2008 by Jeff Weisberg
  Author: Jeff Weisberg <jaw @ tcp4me.com>
  Created: 2008-Dec-31 13:37 (EST)
  Function: 
*/


#ifndef __mrquincy_misc_h_
#define __mrquincy_misc_h_

#include <stdlib.h>
#include <atomic.h>

#define ELEMENTSIN(T) (sizeof(T)/sizeof(T[0]))

#define MAX(a,b)	(((a)<(b)) ? (b) : (a))
#define MIN(a,b)	(((a)<(b)) ? (a) : (b))
#define ABS(x)		(((x) < 0) ? -(x) : (x))
#define BOUND(x,min,max) MAX(MIN((x),(max)),(min))

inline int random_n(int n){ return n ? random() % n : 0; }
inline int probability(float p){ return random() < p * 0x7FFFFFFF; }


// use solaris atomic_ops if we have them
#if 0
#  define ATOMIC_SET32(a,b)		((a)  = (b))
#  define ATOMIC_SET64(a,b)		((a)  = (b))
#  define ATOMIC_ADD32(a,b)		((a) += (b))
#  define ATOMIC_ADD64(a,b)		((a) += (b))
#else
#  define ATOMIC_SET32(a,b)		atomic_swap_32( (uint32_t*)&a, b )
#  define ATOMIC_SET64(a,b)		atomic_swap_64( (uint64_t*)&a, b )
#  define ATOMIC_ADD32(a,b)		atomic_add_32(  (uint32_t*)&a, b )
#  define ATOMIC_ADD64(a,b)		atomic_add_64(  (uint64_t*)&a, b )
#endif


#define ATOMIC_SETPTR(a,b) 	((sizeof(void*) == 8) ? ATOMIC_SET64(a, (uint64_t)b) : ATOMIC_SET32(a, (uint32_t)b))


extern int current_load(void);



#endif // __mrquincy_misc_h_
