

#include <stdlib.h>
#include <strings.h>
#include <stdio.h>

// input: [ key, data ]
// key: "string", number, RSN: [array of string, number]
static int
_findkey(const char *buf, int len){
    int pos;

    if( *buf != '[' ){
        // not json - find first whitespace
        for(pos=1; pos<len && !isspace(*buf++); pos++) ;
        return pos-1;
    }

    // skip white


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
        return pos -1;

    case '[':
    case '{':
        fprintf(stderr, "unsupported key type\n");
        return 0;
    default:
        // number. read until , or space
        for(pos=1; pos<len && !isspace(*buf) && *buf != ','; pos++) buf++;
        return pos - 1;
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
test(const char *s){

    int kl = _findkey(s, strlen(s));
    printf("%s\t%d\n", s, kl);
}

int
main(int argc, char **argv){

    test( "abcd efghi");
    test( "[1234, 1234");
    test( "[1234 5678" );
    test( "[\"foobar\", 1234");
    test( "[\"fo\\\"bar\", 1234");


}
