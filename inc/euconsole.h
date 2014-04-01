/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-21 15:23 (EDT)
  Function: end user console

*/
#ifndef __mrquincy_euconsole_h_
#define __mrquincy_euconsole_h_

#include "mrmagoo.pb.h"

class EUConsole {

    int			_udp;
    ACPMRMDiagMsg	_gm;
    string		_buf;
    string		_addr;

    void _flush(void);
public:
    EUConsole(const char *, ACPMRMTaskCreate*);
    ~EUConsole();

    void send(const char *m, int l);
    void done(void);
};


#endif //  __mrquincy_euconsole_h_
