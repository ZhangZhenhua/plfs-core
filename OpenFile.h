#ifndef __OpenFile_H__
#define __OpenFile_H__
#include "COPYRIGHT.h"
#include <string>
#include <map>
#include "WriteFile.h"
#include "Index.h"
#include "Metadata.h"
using namespace std;

#define PPATH 1024

class Plfs_fd : public Metadata {
    public:
        Plfs_fd( WriteFile *, Index *, pid_t, mode_t, const char * );
        WriteFile  *getWritefile();
        Index      *getIndex();
        void       setWriteFds( int, int, Index * );
        void       getWriteFds( int *, int *, Index ** );
        pid_t      getPid();
        const char *getPath() { return this->path.c_str(); }
    private:
        WriteFile *writefile;
        Index     *index;
        pid_t     pid;
        mode_t    mode;
        string    path;
};

#endif
