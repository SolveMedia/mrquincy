/*
  Copyright (c) 2008 by Jeff Weisberg
  Author: Jeff Weisberg <jaw @ tcp4me.com>
  Created: 2008-Dec-28 12:16 (EST)
  Function:

  $Id: config.cc,v 1.3 2012/04/02 18:13:20 jaw Exp $

*/

#define CURRENT_SUBSYSTEM	'c'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "misc.h"
#include "network.h"

#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>

struct Config *config = 0;

#define SET_STR_VAL(p)	\
static int set_##p (Config *cf, string *v){	\
    if(v){					\
	cf->p.assign( *v );			\
    }						\
    return 0;					\
}

#define ADD_STR_VAL(p)	\
static int add_##p (Config *cf, string *v){	\
    if(v){					\
	cf->p.push_back( *v );			\
    }						\
    return 0;					\
}

#define SET_INT_VAL(p,m)	\
static int set_##p (Config *cf, string *v){		\
    if(!v && m){					\
	FATAL("number must be specified");		\
    }							\
    if( v ){						\
        cf->p = atoi( v->c_str() );			\
    }else{						\
	cf->p = 0;					\
    }							\
    return 0;						\
}


static int set_debug(Config *, string *);
static int set_trace(Config *, string *);
static int add_acl(Config *, string *);
static int add_peer(Config *, string *);
static int ignore_conf(Config *cf, string *s) { return 0; }

SET_INT_VAL(tcp_threads, 0);
SET_INT_VAL(udp_threads, 0);
SET_INT_VAL(port_mrquincy, 0);
SET_INT_VAL(port_console, 0);
SET_INT_VAL(debuglevel, 0);

SET_INT_VAL(enable_scriblr, 0);
SET_INT_VAL(hw_cpus, 0);

SET_STR_VAL(environment);
SET_STR_VAL(basedir);
SET_STR_VAL(error_mailto);
SET_STR_VAL(error_mailfrom);
SET_STR_VAL(plan_prog);

static struct {
    const char *word;
    int (*fnc)(Config *, string *);
} confmap[] = {
    { "cpus",		set_hw_cpus	   },
    { "port",           set_port_mrquincy  },
    { "tcp_threads",	set_tcp_threads	   },
    { "udp_threads",	set_udp_threads	   },
    { "console",        set_port_console   },
    { "environment",    set_environment    },
    { "basedir",	set_basedir        },
    { "debug",          set_debug          },
    { "trace",          set_trace          },
    { "debuglevel",     set_debuglevel     },
    { "error_mailto",   set_error_mailto   },
    { "error_mailfrom", set_error_mailfrom },
    { "scriblr",	set_enable_scriblr },
    { "allow",		add_acl     	   },
    { "seedpeer",	add_peer 	   },
    { "syslog",		ignore_conf        },	// NYI
    { "planprog",	set_plan_prog      },
    // ...
};

static struct {
    const char *name;
    int         value;
} debugname[] = {
    { "config",           'c' },
    { "network",          'N' },
    { "console",          'C' },
    { "thread",           'T' },
    { "daemon",           'd' },
    { "scriblr",          's' },
    { "crypto",           'y' },
    { "kibitz_server",	  'K' },
    { "kibitz_client",	  'k' },
    { "peerdb",		  'P' },
    { "peer",		  'p' },
    { "xfer",	          'x' },
    { "task",		  't' },
    { "job",	          'j' },
    { "mapio",		  'i' },
};

static void
store_value(Config *cf, string *k, string *v){
    int i, l;

    DEBUG("store value %s => %s", k->c_str(), v?v->c_str():"");

    // search table
    l = sizeof(confmap) / sizeof(confmap[0]);
    for(i=0; i<l; i++){
	if( !k->compare( confmap[i].word ) ){
	    confmap[i].fnc(cf, v);
	    return;
	}
    }

    FATAL("invalid entry in config file '%s'", k->c_str());
}


static int
read_token(FILE *f, string *k, int spacep){
    int c;

    k->clear();

    while(1){
	c = fgetc(f);
	if(c == EOF) return -1;
	if(c == '#'){
	    // eat til eol
	    while(1){
		c = fgetc(f);
		if(c == EOF)  return -1;
		if(c == '\n') break;
	    }
	    if( k->length() ) return 0;
	    continue;
	}
	if(c == '\n'){
	    if( k->length() ) return 0;
	    continue;
	}
	if( !spacep && isspace(c) ){
	    if( k->length() ) return 1;
	    continue;
	}
	// skip leading space
	if( spacep && isspace(c) && ! k->length() ) continue;


	k->append(1,c);
    }
}

int
read_config(const char *filename){
    FILE *f;
    Config *cf;
    int i;
    string k, v;
    int rt = 0;

    f = fopen(filename, "r");
    if(!f){
	FATAL("cannot open file '%s': %s", filename, strerror(errno));
    }

    cf = new Config;

    while(1){
	i = read_token(f, &k, 0);
	if(i == -1) break;	// eof
	if(i == 0){
	    store_value(cf, &k, 0);
	    continue;
	}
	i = read_token(f, &v, 1);
	store_value(cf, &k, &v);
	if(i == -1) break;	// eof
    }

    fclose(f);
    Config *old = config;
    ATOMIC_SETPTR( config, cf);

    if( old ){
        sleep(2);
        delete old;
    }
    return 0;
}

//################################################################

static int
debug_name_to_val(const string *name){

    int l = ELEMENTSIN(debugname);
    for(int i=0; i<l; i++){
	if( !name->compare( debugname[i].name ) ){
	    return debugname[i].value;
	}
    }

    PROBLEM("invalid debug flag '%s'", name->c_str());
    return 0;
}

static int
set_debug(Config *cf, string *v){

    if(!v) return 0;

    int c = debug_name_to_val( v );

    if( c ){
	cf->debugflags[ c/8 ] |= 1 << (c&7);
        debug_enabled = 1;
    }
    return 0;
}

static int
set_trace(Config *cf, string *v){

    if(!v) return 0;

    int c = debug_name_to_val( v );
    if( c )
	cf->traceflags[ c/8 ] |= 1 << (c&7);

    return 0;
}

// addr
// addr/mask
static int
add_acl(Config *cf, string *v){
    char addr[32];
    int mlen = 32;
    int p = 0;

    // parse addr
    while( p < v->length() ){
	int c = v->at(p);
	if( c == '/' || isspace(c) ) break;
	addr[p] = c;
	addr[++p] = 0;
    }

    // parse masklen
    p ++;	// skip /
    if( p < v->length() ){
	mlen = atoi( v->c_str() + p );
    }

    // add to list
    struct ACL *acl = new struct ACL;
    struct in_addr a;

    inet_aton(addr, &a);
    acl->mask = ntohl(0xFFFFFFFF << (32 - mlen));
    acl->ipv4 = a.s_addr & acl->mask;
    cf->acls.push_back( acl );

    DEBUG("acl %s mask %d => %x + %x", addr, mlen, acl->ipv4, acl->mask);

    return 0;
}

static int
add_peer(Config *cf, string *v){
    char addr[32];
    int port = PORT_MRQUINCY;
    int p = 0;

    // parse addr
    while( p < v->length() ){
	int c = v->at(p);
	if( c == ':' || isspace(c) ) break;
	addr[p] = c;
	addr[++p] = 0;
    }

    // parse port
    p ++;	// skip :
    if( p < v->length() ){
	port = atoi( v->c_str() + p );
    }
    // find end of port, so we can trim
    while( p < v->length() && isdigit( v->at(p) ) ) p++;

    // add to list
    struct NetAddr *na = new NetAddr;
    struct in_addr a;

    na->name = "seed/";
    na->name.append( *v, 0, p );
    na->port = port;

    // RSN - ipv6
    inet_aton(addr, &a);
    na->ipv4 = a.s_addr;

    cf->seedpeers.push_back( na );

    DEBUG("seedpeer %s => %x + %x", v->c_str(), a.s_addr, port);

    return 0;
}

//################################################################

static int
int_from_file(const char *file){
    FILE *f;
    int r;

    f = fopen(file, "r");
    if(!f){
        return 0;
    }

    fscanf(f, "%d", &r);
    fclose(f);

    return r;
}

//################################################################

Config::Config(){

    // read hw confs
    hw_cpus        = int_from_file( FILE_HW_CPU );
    port_mrquincy  = PORT_MRQUINCY;
    port_console   = PORT_CONSOLE;
    debuglevel     = 0;
    enable_scriblr = 1;
    udp_threads	   = 2;
    tcp_threads	   = 4;
    environment.assign("unknown");

    memset(debugflags, 0, sizeof(debugflags));
    memset(traceflags, 0, sizeof(traceflags));

}

Config::~Config(){

    for(ACL_List::iterator it=acls.begin(); it != acls.end(); it++){
        ACL *a = *it;
        delete a;
    }
    for(NetAddr_List::iterator it=seedpeers.begin(); it != seedpeers.end(); it++){
        NetAddr *a = *it;
        delete a;
    }
}

//################################################################

int
Config::check_acl(const sockaddr* sa){
    sockaddr_in *in = (sockaddr_in*)sa;

    DEBUG("check acl %x", in->sin_addr.s_addr);

    ACL_List::iterator final = acls.end(), it;

    for(it=acls.begin(); it != final; it++){
	ACL *a = *it;

	DEBUG("check %08x == %08x + %08x == %08x",
	      a->ipv4, in->sin_addr.s_addr, a->mask, (in->sin_addr.s_addr & a->mask));

	if( a->ipv4 == (in->sin_addr.s_addr & a->mask) ) return 1;
    }

    return 0;
}

