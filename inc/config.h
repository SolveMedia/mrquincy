/*
  Copyright (c) 2008 by Jeff Weisberg
  Author: Jeff Weisberg <jaw @ tcp4me.com>
  Created: 2008-Dec-28 12:17 (EST)
  Function:
*/

#ifndef __mrquincy_config_h_
#define __mrquincy_config_h_

#include <stdint.h>

#include <list>
#include <string>
using std::list;
using std::string;


#define FILE_HW_MEM	"/var/run/adcopy.mem"
#define FILE_HW_CPU	"/var/run/adcopy.cpu"

struct sockaddr;
class NetAddr;

struct ACL {
    uint32_t	ipv4;
    uint32_t	mask;
};


typedef list<struct ACL*> ACL_List;
typedef list<NetAddr *>   NetAddr_List;

class Config {
public:
    int		hw_cpus;
    int		threads;

    int 	port_console;
    int 	port_mrquincy;
    int		enable_scriblr;

    int 	debuglevel;
    char 	debugflags[256/8];
    char 	traceflags[256/8];

    string 	environment;
    string	basedir;

    ACL_List		acls;
    NetAddr_List	seedpeers;

    string	error_mailto;
    string	error_mailfrom;
    string	plan_prog;

    int check_acl(const sockaddr *);
protected:
    Config();
    ~Config();

    friend int read_config(const char*);
};

extern Config *config;

extern int read_config(const char *);

#endif // __mrquincy_config_h_

