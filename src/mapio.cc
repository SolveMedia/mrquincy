/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-21 11:04 (EDT)
  Function: reduce input, map output io

*/

#define CURRENT_SUBSYSTEM	'i'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "misc.h"
#include "network.h"
#include "mapio.h"

#include "mrmagoo.pb.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <libgen.h>
#include <errno.h>
#include <unistd.h>
#include <sys/uio.h>
#include "zlib.h"


#define READSIZE		65536		// try to read this much at a time
#define INITIALSIZE		(2*READSIZE)	// initially allocate a buffer this big
#define OUTBUFSIZE		16384		// output buffer size
#define OUTBLKSIZE		8192		// try to write in multiples of this size


BufferedInput::BufferedInput(int fd){

    _fd     = fd;
    _curpos = 0;
    _buf    = (char*)malloc(INITIALSIZE);
    _bufsiz = INITIALSIZE;

}

BufferedInput::~BufferedInput(){
    free(_buf);
}

void
BufferedInput::read(MapOutSet *out){

    // do we have enough space?
    if( _curpos + READSIZE > _bufsiz ){
        _buf = (char*)realloc( _buf, 2 * _bufsiz );
        if( !_buf ) FATAL("out of memory!");
        _bufsiz *= 2;
        DEBUG("realloc -> %d", _bufsiz);
    }

    // read data from user program
    int r = ::read(_fd, _buf + _curpos, READSIZE);
    //DEBUG("read -> %d", r);
    if( r < 1 ) return;

    // process all records (\n terminated)
    int recstart  = 0;
    int lookstart = _curpos;

    _curpos += r;

    while( recstart < _curpos ){
        // do we have a full record?
        int looklen = _curpos - lookstart;
        char *nlpos = (char*)memchr( _buf+lookstart, '\n', looklen );

        if( !nlpos ) break;	// not found

        int reclen = nlpos - _buf - recstart + 1;
        // DEBUG("found record len %d", reclen);

        // we have a record
        // send it to the output processor
        out->output(_buf + recstart, reclen);

        // move ahead and look for more
        recstart += reclen;
        lookstart = recstart;
    }

    // reset
    if( _curpos == recstart ){
        // the newline was the end of the buffer, clear and go
        _curpos = 0;
        // DEBUG("flush");
        return;
    }else if( recstart ){
        // move remaining data
        int newlen = _curpos - recstart;
        memmove(_buf, _buf + recstart, newlen );
        _curpos = newlen;
        // DEBUG("shift buf -> %d", newlen);
    }

}

/****************************************************************/

MapOutSet::MapOutSet(ACPMRMTaskCreate *g){

    _nfile = g->outfile_size();
    _file.reserve( _nfile );

    // set up outputs
    for(int i=0; i<_nfile; i++){
        const string *file = & g->outfile(i);
        // RSN - compress or not? configurable?
        // tests suggest that cpu-wise, compress overhead is negligible.
        // and disk + network are significantly improved.
        // perhaps not configurable.

        // _file[i] = new BufferedMapOutput( file->c_str() );
        _file[i] = new CompressedMapOutput( file->c_str() );
    }
}

void
MapOutSet::close(void){

    for(int i=0; i<_nfile; i++){
        _file[i]->close();
    }
}

// input: [ key, data ]
// key: "string", number, RSN: [array of string, number]
static int
_findkey(const char *buf, int len){
    int pos;

    if( *buf != '[' ){
        // not json - find first whitespace
        for(pos=1; pos<len && !isspace(*buf++); pos++) ;
        return pos;
    }

    // eat leading punct + space
    buf++; len--;
    while( isspace(*buf) ){ buf++; len--; }

    switch( *buf ){
    case '"':
        // string until "
        pos = 2; buf++;
        while(pos<len){
            if( *buf == '\\' ){
                buf ++;
                pos ++;
            }else if( *buf == '"' ){
                break;
            }
            buf ++;
            pos ++;
        }
        return pos;

    case '[':
    case '{':
        // RSN ...
        DEBUG("unsupported key type");
        return 0;
    default:
        // number. read until , or space
        for(pos=1; pos<len && !isspace(*buf) && *buf != ','; pos++) buf++;
        return pos;
    }

    return 0;
}

// djb hash
static int
_hashval(const char *buf, int len){
    unsigned long hash = 5381;
    int c;

    while( len-- ){
        c = *buf++;
        hash = ((hash << 5) + hash) ^ c; /* hash * 33 xor c */
    }
    return hash & 0x7FFFFFFF;
}

void
MapOutSet::output(const char *buf, int len){

    // only one output? short inpu? short circuit
    if( _nfile == 1 || len < 4 ){
        _file[0]->output(buf, len);
        return;
    }

    // find the key + hash it
    int keylen  = _findkey( buf, len );
    int hashval = keylen > 0 ? _hashval( buf, keylen ) : 0;

    hashval %= _nfile;
    _file[ hashval ]->output(buf, len);
}

/****************************************************************/

static int
_validate(const char *file){

    if( strstr(file, "/.") ) return 0;
    if( !strncmp(file, "../", 3)) return 0;
    return 1;
}

static void
_mkdirs(const char *f){
    string file = f;
    int lsl = file.rfind('/');
    string dir;
    if( lsl != -1 ){
        dir.append(file, 0, lsl);
        mkdirp( dir.c_str(), 0777 );
    }
}

void
MapOutput::init(const char *file){
    if( !_validate(file) ) FATAL("invalid filename %s", file);
    _mkdirs(file);
}

// NB: stdio runs into trouble with > 255 FILE*s

BufferedMapOutput::BufferedMapOutput(const char *file){

    init(file);
    _buf    = (char*)malloc(OUTBUFSIZE);
    _bufsiz = OUTBUFSIZE;
    _curpos = 0;
    _fd     = open(file, O_WRONLY | O_CREAT | O_TRUNC, 0666);

    if( _fd < 0 ) FATAL("cannot open file %s: %s", file, strerror(errno));
}

void
BufferedMapOutput::close(void){

    if( _curpos ){
        write( _fd, _buf, _curpos );
    }

    ::close(_fd);
}

void
BufferedMapOutput::output(const char *buf, int len){

    if( _curpos + len < _bufsiz ){
        // buffer it
        memcpy(_buf + _curpos, buf, len);
        _curpos += len;
        return;
    }

    // flush everything (mod BLK)
    int wlen = (_curpos + len) & ~(OUTBLKSIZE-1) - _curpos;

    struct iovec iov[2];
    iov[0].iov_base = _buf;
    iov[0].iov_len  = _curpos;
    iov[1].iov_base = (void*)buf;
    iov[1].iov_len  = wlen;

    writev( _fd, iov, 2 );

    // buffer the remainder
    int rem = len - wlen;
    if( rem ){
        memcpy(_buf, buf + wlen, rem);
    }
    _curpos = rem;
}

/****************************************************************/

CompressedMapOutput::CompressedMapOutput(const char *file){

    init(file);
    _gfd = gzopen(file, "wb");
    if( _gfd == 0 ) FATAL("cannot gzopen file %s", file);

}

void
CompressedMapOutput::close(void){
    gzclose(_gfd);
}

void
CompressedMapOutput::output(const char *buf, int len){

    gzwrite( _gfd, buf, len );
}

