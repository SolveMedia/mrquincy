# Copyright (c) 2014
# Author: Jeff Weisberg <jaw @ solvemedia.com>
# Created: 2014-Mar-13 11:41 (EDT)
# Function: Mr. Qunicy's Makefile


PROTO =  std_ipport.o heartbeat.o mrmagoo.o scrible.o std_reply.o

OBJS =  lock.o diag.o misc.o config.o daemon.o thread.o network.o \
	mrquincy.o scriblr.o crypto.o base64.o \
	kibitz_myself.o kibitz_server.o kibitz_client.o peerdb.o peers.o \
	euconsole.o protocol.o queued.o xfer.o task.o mapio.o pipeline.o \
	job_admin.o job_run.o job_plan.o job_action.o job_cleanup.o

OBJS += alloc.o

# import site-wide compiler options
include ../../Makefile.inc

CFLAGS = -g $(FLAGS) -O3 -ffast-math -pthreads -I. -I`pwd`/../inc  -I/usr/local/include -I/usr/sfw/include -I$(LOCALDIR)/include
CCFLAGS=$(CFLAGS)
LDFLAGS = -L$(LOCALDIR)/lib -L/usr/sfw/lib/amd64 -lprotobuf -lpthread -lrt -lsocket -lnsl -lgen -lssl -lcrypto -lsasl -lz -lsendfile
PCC=protoc
CVT=../../../tools/proto2pl

all: mrquincyd

mrquincyd: $(PROTO) $(OBJS)
	$(CCC) -o mrquincyd $(CFLAGS) $(PROTO) $(OBJS) $(LDFLAGS)

install:
	-mv ../../../bin/mrquincyd ../../../bin/mrquincyd-
	cp mrquincyd ../../../bin/

clean:
	rm -f $(OBJS) mrquincyd

realclean:
	rm -f $(OBJS) $(PROTO) mrquincd

%.o:%.proto
	$(PCC) --proto_path=. --cpp_out=. $<
	$(CCC) $(CFLAGS) -c -o $*.o $*.pb.cc
	$(CVT) $< ../lib/AC/MrQuincy/proto/$*.pl

