/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-14 13:45 (EDT)
  Function: peer data

*/

#ifndef __mrquincy_peers_h_
#define __mrquincy_peers_h_

#include "lock.h"
#include <list>
#include <string>
using std::list;
using std::string;

class ACPMRMStatus;
class ACPMRMStatusReply;


#define PEER_STATUS_UNK		0
#define PEER_STATUS_UP		1
#define PEER_STATUS_MAYBEDN	2
#define PEER_STATUS_DN		3
#define PEER_STATUS_SCEPTICAL	4
#define PEER_STATUS_DEAD	5


class Peer {

    // local status + timestamps
    int			_status;
    int			_num_fail;
    hrtime_t		_last_try;
    hrtime_t		_last_up;

    const char 		*_id;		// server_id
    ACPMRMStatus 	*_gstatus;

public:
    NetAddr		bestaddr;

protected:
    Peer(const ACPMRMStatus *);
    ~Peer();

    void update(const ACPMRMStatus*);		// update with new info
    void is_up(void);
    void is_down(void);
    void maybe_down(void);
    void status_reply(ACPMRMStatus*) const ;	// add status to reply
    int  status(void) const { return _status; }


    DISALLOW_COPY(Peer);

    friend class PeerDB;
};

/****************************************************************/

class PeerDB {

    RWLock	_lock;
    list<Peer*>	_allpeers;
    list<Peer*>	_sceptical;
    list<Peer*> _graveyard;

    void _upgrade(Peer*);	// sceptical -> allpeers
    void _kill(Peer*);		// * -> graveyard
    Peer *_find(const char *);

public:
    void add_peer(ACPMRMStatus*g);
    void add_sceptical(ACPMRMStatus*g);
    void reply_peers(ACPMRMStatusReply *);
    Peer *find(const char *);
    NetAddr *find_addr(const char*);
    Peer *random(void);
    void peer_up(const char*);
    void peer_dn(const char*);
    void cleanup(void);
    int  report(NTD*);
    void getall( list<NetAddr> *);

protected:
    PeerDB()	{};
    ~PeerDB()	{};
    DISALLOW_COPY(PeerDB);

    friend void peerdb_init(void);
};

extern PeerDB *peerdb;

void about_myself(ACPMRMStatus *);



#endif // __mrquincy_peers_h_

