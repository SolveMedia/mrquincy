/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-14 10:25 (EDT)
  Function: file access

*/

#define CURRENT_SUBSYSTEM	's'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "misc.h"
#include "network.h"
#include "crypto.h"

#include "std_reply.pb.h"
#include "scrible.pb.h"

#include <strings.h>
#include <libgen.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>

#define TIMEOUT		15


static int
validate(const char *file){

    if( strstr(file, "/.") ) return 0;
    if( !strncmp(file, "../", 3)) return 0;
    return 1;
}

static int
file_size(const char *file){
    struct stat st;

    if( stat(file, &st) == -1 ) return -1;
    return st.st_size;
}

static void
file_hash(const char *file, char *buf, int len){
    HashSHA1 h(file);
    h.digest64(buf, len);
}

static int
reply(NTD *ntd, int code, const char *msg, const char *hash){
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header *pho = (protocol_header*) ntd->gpbuf_out;
    ACPScriblReply g;

    VERBOSE("sending reply %d %s", code, msg);

    if( !(phi->flags & PHFLAG_WANTREPLY) ) return 0;

    ntd_copy_header_for_reply(ntd);
    if( code == 200 )
        pho->flags = PHFLAG_ISREPLY;
    else
        pho->flags = PHFLAG_ISREPLY | PHFLAG_ISERROR;

    g.set_status_code( code );
    g.set_status_message( msg );
    if( hash && hash[0] ) g.set_hash_sha1( hash );
    g.SerializeToArray( ntd->out_data(), ntd->data_size() );
    pho->data_length = g.GetCachedSize();

    cvt_header_to_network( pho );
    return sizeof(protocol_header) + g.GetCachedSize();
}

static int
parse_request(NTD *ntd, ACPScriblRequest *req){
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;

    if( ! ntd->have_data ) return 0;

    req->ParsePartialFromArray( ntd->in_data(), phi->data_length );
    DEBUG("l=%d, %s", phi->data_length, req->ShortDebugString().c_str());

    if( ! req->IsInitialized() ){
        DEBUG("invalid request. missing required fields");
        return 0;
    }

    return 1;
}

static int
parse_and_validate(NTD *ntd, ACPScriblRequest *req){

    if( !parse_request(ntd, req) )
        return reply(ntd, 500, "Error", 0);

    const string *fn = & req->filename();
    if( !validate(fn->c_str()) )
        return reply(ntd, 500, "Error", 0);

    return 0;
}

// for scriblr_put + file xfer
int
scriblr_save_file(int fd, const string *filename, int size, string *hash, int to){

    if( !validate( filename->c_str() ) ){
        DEBUG("invalid filename");
        return 0;
    }

    // file -> dir, file
    string file = config->basedir;
    file.append("/");
    file.append( *filename );
    DEBUG("filename: %s", file.c_str());

    // create dirs
    int lsl = file.rfind('/');
    string dir;
    if( lsl != -1 ){
        dir.append(file, 0, lsl);
        mkdirp( dir.c_str(), 0777 );
    }
    DEBUG("dir: %s", dir.c_str());

    // open tmp file
    string tmp = file;
    tmp.append(".tmp");

    FILE *f = fopen( tmp.c_str(), "w" );
    if( !f ){
        PROBLEM("cannot save file %s: %s", tmp.c_str(), strerror(errno));
        return 0;
    }

    int writ = 0;
    char buf[8192];

    // copy data
    // NB: there is no reverse-sendfile
    while( writ != size ){
        int s = size - writ;
        if( s > sizeof(buf) ) s = sizeof(buf);

        int r = read_to(fd, buf, s, to);
        DEBUG("read %d -> %d", s, r);
        if( r<1 ) break;
        fwrite(buf, 1, r, f);

        writ += r;
    }
    fclose(f);

    // verify
    int vfysz  = file_size( tmp.c_str() );
    if( vfysz > 0 )
        file_hash( tmp.c_str(), buf, sizeof(buf) );
    else
        buf[0] = 0;

    if( vfysz != size || hash->compare(buf) ){
        VERBOSE("verify failed %s, %d %s != %d %s", tmp.c_str(), size, hash->c_str(), vfysz, buf);
        unlink( tmp.c_str() );
        return 0;
    }

    // copy out hash
    hash->assign( buf );

    rename( tmp.c_str(), file.c_str() );

    return 1;
}

int
scriblr_put(NTD *ntd){
    ACPScriblRequest req;
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;

    if( ! config->enable_scriblr )
        return reply(ntd, 500, "Error (writes off)", 0);

    int r = parse_and_validate(ntd, &req);
    if( r ) return r;

    int size = phi->content_length;
    const string *filename = & req.filename();
    string *hash           = req.mutable_hash_sha1();
    VERBOSE("put file %s size %d", filename->c_str(), size);

    r = scriblr_save_file(ntd->fd, filename, size, hash, TIMEOUT);

    if( !r )
        return reply(ntd, 500, "Error", 0);

    return reply(ntd, 200, "OK", hash->c_str() );
}

int
scriblr_get(NTD *ntd){
    ACPScriblRequest req;
    ACPScriblReply   res;
    protocol_header *phi = (protocol_header*) ntd->gpbuf_in;
    protocol_header *pho = (protocol_header*) ntd->gpbuf_out;

    int r = parse_and_validate(ntd, &req);
    if( r ) return r;

    if( !(phi->flags & PHFLAG_WANTREPLY) ) return 0;

    string file = config->basedir;
    file.append("/");
    file.append( req.filename() );
    DEBUG("filename: %s", file.c_str());

    // get size, sha1
    int size = file_size( file.c_str() );
    if( size == -1 )
        return reply(ntd, 404, "Not Found", 0);

    char buf[64];
    file_hash( file.c_str(), buf, sizeof(buf) );

    int f = open( file.c_str(), O_RDONLY );

    if( !f )
        return reply( ntd, 500, "Error", 0);
    DEBUG("file %s -> %d %s", file.c_str(), size, buf);

    // build reply
    res.set_status_code( 200 );
    res.set_status_message( "OK" );
    res.set_hash_sha1( buf );

    write_reply(ntd, &res, size, TIMEOUT );

    // stream file -> network
    sendfile_to(ntd->fd, f, size, TIMEOUT);
    close(f);

    // caller needs to do nothing
    return 0;
}

// also used for mr_delete
int
scriblr_delete_file(const string *filename){

    if( !validate(filename->c_str()) ) return 0;

    string file = config->basedir;
    file.append("/");
    file.append( *filename );
    DEBUG("filename: %s", file.c_str());

    struct stat st;

    // already gone?
    if( lstat(file.c_str(), &st) == -1 )
        return 1;

    switch( st.st_mode & S_IFMT ){
    case S_IFDIR:
        rmdir( file.c_str() );
        break;
    case S_IFREG:
        unlink( file.c_str() );
        break;
    default:
        PROBLEM("cannot delete %s", file.c_str());
        break;
    }

    // gone?
    if( lstat(file.c_str(), &st) == -1 )
        return 1;

    return 0;
}

int
scriblr_del(NTD *ntd){
    ACPScriblRequest req;

    int r = parse_and_validate(ntd, &req);
    if( r ) return r;

    if( scriblr_delete_file( & req.filename() ) )
        return reply(ntd, 200, "OK", 0);

    return reply(ntd, 500, "Error", 0);
}

int
scriblr_chk(NTD *ntd){
    ACPScriblRequest req;

    int r = parse_and_validate(ntd, &req);
    if( r ) return r;

    string file = config->basedir;
    file.append("/");
    file.append( req.filename() );
    int s = file_size( file.c_str() );

    if( s == -1 )
        return reply(ntd, 500, "Error", 0);

    return reply(ntd, 200, "OK", 0);
}

