/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-13 20:38 (EDT)
  Function: crypto
*/

#define CURRENT_SUBSYSTEM	'y'

#include "defs.h"
#include "diag.h"
#include "config.h"
#include "crypto.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

extern void base64_encode(const char *, int, char *, int);

void
HashSHA1::_init(void){
    SHA1_Init( &_ctx );
    _size = 0;
    _done = 0;
}

HashSHA1::HashSHA1(void){
    _init();
}

void
HashSHA1::update(const char *d, int len){

    SHA1_Update( &_ctx, d, len );
}

HashSHA1::HashSHA1(const char *file){

    _init();
    FILE *f = fopen(file, "r");
    if( !f ){
        VERBOSE("cannot open %s", file);
        return;
    }
    while(1){
        char buf[1024];
        int r = fread(buf, 1, sizeof(buf), f);
        if( r<1 ) break;
        update( buf, r );
    }
    fclose(f);
}

void
HashSHA1::_digest(void){
    if( !_done ){
        SHA1_Final( (unsigned char *)_hash, &_ctx );
        _done = 1;
    }
}

void
HashSHA1::digest(char *d, int len){

    _digest();
    if( len > 20 ) len = 20;
    memcpy(d, _hash, len);
}

void
HashSHA1::digest64(char *d, int len){

    _digest();
    base64_encode(_hash, 20, d, len);
}




