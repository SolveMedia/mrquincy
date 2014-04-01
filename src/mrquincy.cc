/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-13 11:50 (EDT)
  Function: start + run Mr. Quincy

*/

#include "defs.h"
#include "diag.h"
#include "daemon.h"
#include "config.h"
#include "network.h"
#include "hrtime.h"
#include "thread.h"
#include "runmode.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>


int flag_foreground   = 0;
int flag_debugall     = 0;
char *filename_config = 0;
RunMode runmode;

void* runmode_manage(void*);
void *reload_config(void*);

extern void peerdb_init(void);
extern void myself_init(void);
extern void kibitz_init(void);
extern void xfer_init(void);
extern void task_init(void);
extern void job_init(void);

void
usage(void){
    fprintf(stderr, "mrquincyd [options]\n"
	    "  -f    foreground\n"
	    "  -d    enable debugging\n"
	    "  -c config file\n");
    exit(0);
}

int
main(int argc, char **argv){
     extern char *optarg;
     extern int optind;
     int prev_status = 0;
     int save_argc = argc;
     char **save_argv = argv;
     int c;

     srandom( time(0) );

     // parse command line
     while( (c = getopt(argc, argv, "c:dfh")) != -1 ){
	 switch(c){
	 case 'f':
	     flag_foreground = 1;
	     break;
	 case 'd':
	     flag_debugall = 1;
             debug_enabled = 1;
	     break;
	 case 'c':
	     filename_config = optarg;
	     break;
	 case 'h':
	     usage();
	     break;
	 }
     }
     argc -= optind;
     argv += optind;

     if( !filename_config ){
	 fprintf(stderr, "no config specified!\ntry -c config\n");
         exit(-1);
     }

     //	init logging
     diag_init();

     // daemonize
     if( flag_foreground){
	 daemon_siginit();
     }else{
	 prev_status = daemonize(5, "mrquincyd", save_argc, save_argv);
     }

     VERBOSE( "starting." );

     // read config file
     if( read_config(filename_config) ){
	 FATAL("cannot read config file");
     }

     if( prev_status && prev_status != (EXIT_NORMAL_RESTART<<8) ){
         // previous process restarted due to an error - send an email
         PROBLEM("previous dancrd restarted due to an error (%d)", prev_status);
     }

     // init subsystems
     // ...
     start_thread( runmode_manage, 0 );
     start_thread( reload_config, (void*)filename_config );

     //console_init();
     peerdb_init();
     network_init();
     myself_init();
     kibitz_init();
     xfer_init();
     task_init();
     job_init();

     VERBOSE("running.");

     // manage threads
     // this does not return until we shutdown
     network_manage();

     VERBOSE("exiting");
     exit(runmode.final_exit_value());

}


void *
reload_config(void *file){
    struct stat sb;
    time_t lastmod = lr_now();

    while(1){
        sleep(15);

	// watch file
	int i = stat((char*)file, &sb);
	if( i == -1 ){
	    VERBOSE("cannot stat configfile '%s': %s", file, strerror(errno));
	    continue;
	}

        if( sb.st_mtime > lastmod ){
            lastmod = sb.st_mtime;
            VERBOSE("config changed, reloading");
            read_config( (char*)file );
        }
    }
}


// normal exit:
//   network_manage finishes + returns to main
//   main exits
///
// normal winddown:
//   puds janitor causes runmode transition windown=>exiting

// runmode_manage handles shutting down in the cases
// where the normal processes are hung

// !!! - this thread must not do anything which could ever hang
//   - no locks, no mallocs, no i/o, no std::string, std::...
//   - no debug, no verbose, ...


#define TIME_LIMIT	60
#define PUDS_LIMIT	1800

void*
runmode_manage(void*){
    time_t texit=0, twind=0, terrd=0;
    time_t nowt;

    while(1){
        nowt = lr_now();

        switch(runmode.mode()){
        case RUN_MODE_EXITING:
	    if( !texit ) texit = nowt + TIME_LIMIT;
	    if( texit < nowt ) _exit(runmode.final_exit_value());
            break;

        case RUN_MODE_WINDDOWN:
            if( !twind ) twind = nowt + PUDS_LIMIT;
            if( twind < nowt ) _exit(runmode.final_exit_value());
            break;

        case RUN_MODE_ERRORED:
            if( !terrd ) terrd = nowt + TIME_LIMIT;
            if( terrd < nowt ) _exit(EXIT_ERROR_RESTART);
            break;

        default:
            twind = texit = terrd = 0;		// shutdown canceled, reset
        }

        sleep(5);

    }
    return 0;
}

