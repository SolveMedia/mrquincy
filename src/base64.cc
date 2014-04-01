/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-14 11:54 (EDT)
  Function: base64 encode

*/

// it is a bit silly to pull in all of libsasl just for base64
// this should either be rewritten or replaced with the openssl code

extern "C" {
#include <sasl/saslutil.h>
}

void
base64_encode(const char *src, int slen, char *dst, int dlen){
    unsigned int l;

    // punt
    sasl_encode64(src, slen, dst, dlen, &l );

    // remove trailing =s for perl compat
    while( l && dst[l-1] == '=' ){
        dst[--l] = 0;
    }
}

