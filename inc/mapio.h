/*
  Copyright (c) 2014
  Author: Jeff Weisberg <jaw @ solvemedia.com>
  Created: 2014-Mar-21 10:37 (EDT)
  Function: reduce input, map output io

*/
#ifndef __mrquincy_mapio_h_
#define __mrquincy_mapio_h_

#include <vector>
using std::vector;
#include "zlib.h"


class MapOutSet;
class ACPMRMTaskCreate;

class BufferedInput {
    char	*_buf;
    int		_bufsiz;
    int		_curpos;
    int		_fd;


public:
    BufferedInput(int);
    ~BufferedInput();

    void read(MapOutSet *);

};

//****************************************************************

class MapOutput {

protected:
    void init(const char *);
public:
    virtual ~MapOutput() {};
    virtual void output(const char *, int) = 0;
    virtual void close(void) = 0;
};

class BufferedMapOutput : public MapOutput {
    int		_fd;
    char	*_buf;
    int		_bufsiz;
    int		_curpos;
public:
    BufferedMapOutput(const char *);
    virtual ~BufferedMapOutput() {}
    virtual void output(const char *, int);
    virtual void close(void);
};


class CompressedMapOutput : public MapOutput {

    gzFile	_gfd;
public:
    CompressedMapOutput(const char *);
    virtual ~CompressedMapOutput() {}
    virtual void output(const char *, int);
    virtual void close(void);
};

//****************************************************************

class MapOutSet {
    int				_nfile;
    vector<MapOutput*>		_file;

public:
    MapOutSet(ACPMRMTaskCreate*);

    void output(const char *, int);
    void close(void);
};


#endif // __mrquincy_mapio_h_
