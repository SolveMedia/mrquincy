/*
  Copyright (c) 2008 by Jeff Weisberg
  Author: Jeff Weisberg <jaw @ tcp4me.com>
  Created: 2008-Dec-28 10:42 (EST)
  Function: diagnostics
*/

#include "defs.h"
#include "diag.h"
#include "misc.h"
#include "config.h"
#include "hrtime.h"
#include "thread.h"
#include "runmode.h"

#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <syslog.h>
#include <unistd.h>

static void send_error_email(const char*, int, int);

extern int flag_debugall;
extern int flag_foreground;

int debug_enabled = 0;		// at least one debug is enabled
static char hostname[256];	// for email

struct LevelConf {
    int syslogprio;
    int to_stderr;
    int to_console;
    int to_email;
    int with_info;
    int with_trace;
    int is_fatal;
};

struct LevelConf logconf[] = {
    // syslog        to_         with_   fatal
    { LOG_DEBUG,     1, 0, 0,    1, 0,    0, },		// debug
    { LOG_DEBUG,     1, 0, 0,    1, 0,    0, },
    { LOG_DEBUG,     1, 0, 0,    1, 0,    0, },
    { LOG_DEBUG,     1, 0, 0,    1, 0,    0, },
    { LOG_DEBUG,     1, 0, 0,    1, 0,    0, },
    { LOG_DEBUG,     1, 0, 0,    1, 0,    0, },
    { LOG_DEBUG,     1, 0, 0,    1, 0,    0, },
    { LOG_DEBUG,     1, 0, 0,    1, 0,    0, },
    { LOG_INFO,      1, 1, 0,    0, 0,    0, },		// verbose
    { LOG_WARNING,   1, 1, 1,    1, 0,    0, },		// problem
    { LOG_ERR,       1, 1, 1,    1, 1,    0, },		// bug
    { LOG_ERR,       1, 1, 1,    1, 1,    1  },		// fatal
};

void
diag_init(void){

    gethostname(hostname, sizeof(hostname));

    // RSN - make configurable
    openlog( MYNAME, LOG_NDELAY|LOG_PID, LOG_LOCAL4);
}


void
diag(int level, const char *file, const char *func, int line, int system, const char *fmt, ...){
    char buf[1024];
    pthread_t tid = pthread_self();
    struct LevelConf *lcf;
    int l = 0;
    int p;
    va_list ap;

    if( level < 8 && !flag_debugall ){
	// is debugging enabled for this message?

	// if config is not yet loaded, only debug via -d
	if( !config ) return ;

	// debugging enabled at this level
	if( level < (8 - config->debuglevel) ) return;

	// for this subsystem
	system &= 0xFF;
	if( level ){
	    if( !( config->debugflags[ system / 8 ] & (1<<(system&7)) )) return;
	}else{
	    if( !( config->traceflags[ system / 8 ] & (1<<(system&7)) )) return;
	}
    }

    if( level < 0 || level >= ELEMENTSIN(logconf) ){
        lcf = & logconf[ ELEMENTSIN(logconf) - 1 ];
    }else{
        lcf = & logconf[ level ];
    }

    va_start(ap, fmt);

    // add boilerplate info
    buf[0] = 0;

    if( lcf->with_info ){
	snprintf(buf, sizeof(buf), "tid:%d %s:%d in %s(): ", tid, file, line, func);
	l = strlen(buf);
    }

    // messages
    vsnprintf(buf + l, sizeof(buf) - l, fmt, ap);
    l = strlen(buf);
    va_end(ap);

    // terminate
    if( l >= sizeof(buf) - 2 ) l = sizeof(buf) - 2;
    buf[l++] = '\n';
    buf[l]   = 0;

    // stderr
    if( flag_foreground && lcf->to_stderr )
	write(2, buf, l);

    // syslog
    p = lcf->syslogprio;
    syslog(p, "%s", buf);

#if 0 // XXX
    // consoles
    if( lcf->to_console )
        Console::broadcast(level, buf, l);
#endif
    // email
    if( lcf->to_email && (/*!flag_foreground || XXX */ lcf->with_trace) )
	send_error_email( buf, l, lcf->with_trace );

    if( lcf->is_fatal ){
	// fatal - abort
	exit(EXIT_ERROR_RESTART);
    }
}

void
attach_cmd(FILE *dst, const char *prog, const char *label){
    FILE *p;
    char cmd[128];

    snprintf(cmd, sizeof(cmd), prog, getpid());
    p = popen(cmd, "r");
    if(p){
        char buf[32];

        fprintf(dst, "\n%s:\n", label);
        while(1){
            int i = fread(buf, 1, sizeof(buf), p);
            if( i <= 0 ) break;
            fwrite(buf, 1, i, dst);
        }

        pclose(p);
    }
}


static void
send_error_email( const char *msg, int len, int with_trace ){
    FILE *f, *p;
    const char *mailto;
    char cmd[128];

    if( config->error_mailto.empty() ) return;

    snprintf(cmd, sizeof(cmd), "env PATH=/usr/lib:/usr/libexec:/usr/sbin:/usr/bin sendmail -t -f '%s'",
	     config->error_mailfrom.c_str());

    f = popen(cmd, "w");
    if(!f) return;

    fprintf(f, "To: %s\nFrom: %s\nSubject: " MYNAME " daemon error\n\n",
	    config->error_mailto.c_str(), config->error_mailfrom.c_str());
    fprintf(f, "an error was detected in " MYNAME "d\n\n");

    fprintf(f, "host: %s\npid:  %d\n\nerror:\n%s\n", hostname, getpid(), msg);

    // stack trace?
    if( with_trace ){
        // non-portability alert! these are solaris only
        attach_cmd(f, "/usr/bin/pstack %d", "trace");
        attach_cmd(f, "/usr/bin/pmap %d",   "pmap");
        attach_cmd(f, "/usr/bin/pfiles %d", "files");
        attach_cmd(f, "/usr/bin/plimit %d", "limits");
    }

    pclose(f);

}
