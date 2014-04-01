/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-13 20:36 (EDT)
  Function: crypto
*/

#ifndef __mrquincy_crypto_h_
#define __mrquincy_crypto_h_

extern "C" {
#include <openssl/sha.h>
}

class HashSHA1 {
    SHA_CTX	_ctx;
    int		_size;
    bool	_done;
    char	_hash[20];

    void _init(void);
    void _digest(void);

public:
    HashSHA1(const char *file);
    HashSHA1(void);

    void update(const char *, int);
    void digest(char *, int);
    void digest64(char *, int);
    int size(void) const {return _size; }

};


#endif // __mrquincy_crypto_h_
