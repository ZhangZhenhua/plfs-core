#ifndef __WriteFile_H__
#define __WriteFile_H__

#include "COPYRIGHT.h"
#include <map>
using namespace std;
#include "Util.h"
#include "Index.h"
#include "Metadata.h"

// THREAD SAFETY
// we use a mutex when writers are added or removed
// - the data_mux protects the fds map
// we do lookups into the fds map on writes
// but we assume that either FUSE is using us and it won't remove any writers
// until the release which means that everyone has done a close
// or that ADIO is using us and there won't be concurrency

// if we get multiple writers, we create an index mutex to protect it

// Each writeType has its own naming conventions for data files.
// SINGLE_HOST_WRITE is used for tranditional byteRange writing.
// SIMPLE_FORMULA_WRITE is used for ADIO patterned write.
// SIMPLE_FORMULA_WITHOUT_INDEX is similar to SIMPLE_FORMULA_WRITE except
// not writing index file, e.g. for ranks other than zero in MPI job
enum WriteType {
    SINGLE_HOST_WRITE,
    SIMPLE_FORMULA_WRITE,
    SIMPLE_FORMULA_WITHOUT_INDEX,
};

// Every proc may own multiple data files, one ByteRange data file and
// multiple SimpleFormula data files.
struct
OpenFd {
    int sh_fd;                  // ByteRange data file fd
    vector< int > sf_fds;       // SimpleFormula data files fd
                                // one proc may open multiple SimpleFormula
                                // data files, push back and stash them here
                                // the fd at back() is the one currently
                                // being used
    int writers;
};

class WriteFile : public Metadata
{
    public:
        WriteFile(string, string, string, mode_t, size_t index_buffer_mbs);
        ~WriteFile();

        int openIndex( pid_t, WriteType );
        int closeIndex();
        int openNewDataFile( pid_t pid , WriteType wType);

        int addWriter( pid_t, bool child, WriteType );
        int removeWriter( pid_t );
        size_t numWriters();
        size_t maxWriters() {
            return max_writers;
        }

        int truncate( off_t offset );
        int extend( off_t offset );

        ssize_t write( const char *, size_t, off_t, pid_t, WriteType );

        int sync( );
        int sync( pid_t pid );

        void setContainerPath( string path );
        void setSubdirPath (string path);

        // get current operating data file fd
        int whichFd ( OpenFd *ofd, WriteType wType );
        int restoreFds(bool droppings_were_truncd);
        Index *getIndex() {
            return index;
        }

        double createTime() {
            return createtime;
        }

        void setFormulaTime(double ts){
            formulaTime = ts;
        }
        void setMetalinkTime(double ts){
            metalinkTime = ts;
        }
        struct OpenFd *getFd( pid_t pid );
        int closeFd( int fd );

    private:
        int openIndexFile( string path, string host, pid_t, mode_t
                           , string *index_path, WriteType wType);
        int openDataFile(string path, string host, pid_t, mode_t , WriteType);
        int openFile( string, mode_t mode );
        int Close( );
        int setOpenFd(OpenFd *ofd, int fd, WriteType wType);
        int restore_helper(int fd, string *path);

        string container_path;
        string subdir_path;
        string logical_path;
        string hostname;
        map< pid_t, OpenFd  > fds;
        map< int, string > paths;      // need to remember fd paths to restore
        pthread_mutex_t    index_mux;  // to use the shared index
        pthread_mutex_t    data_mux;   // to access our map of fds
        bool has_been_renamed; // use this to guard against a truncate following
        // a rename
        size_t index_buffer_mbs;
        Index *index;
        mode_t mode;
        double createtime;
        double formulaTime;  // used for MPIIO only,
                             // keeps rank0's formula creation time
        double metalinkTime; // use this timestamp to create metalink
        size_t max_writers;
        // Keeps track of writes for flush of index
        int write_count;
};

#define WF_MAP_ITR map< WriteType , int >::iterator
#define WF_FD_ITR vector< int >::iterator

#endif
