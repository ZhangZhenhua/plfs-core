#include "plfs.h"
#include "plfs_private.h"
#include "COPYRIGHT.h"
#include "WriteFile.h"
#include "Container.h"

#include <assert.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <errno.h>
#include <string>
using namespace std;

// the path here is a physical path.  A WriteFile just uses one hostdir.
// so the path sent to WriteFile should be the physical path to the
// shadow or canonical container (i.e. not relying on symlinks)
// anyway, this should all happen above WriteFile and be transparent to
// WriteFile.  This comment is for educational purposes only.
WriteFile::WriteFile(string path, string hostname, 
        mode_t mode, size_t buffer_mbs ) : Metadata::Metadata() 
{
    this->physical_path     = path;
    this->hostname          = hostname;
    this->index             = NULL;
    this->mode              = mode;
    this->has_been_renamed  = false;
    this->createtime        = Util::getTime();
    this->write_count       = 0;
    this->index_buffer_mbs  = buffer_mbs;
    this->max_writers       = 0;
    pthread_mutex_init( &data_mux, NULL );
    pthread_mutex_init( &index_mux, NULL );
}

void WriteFile::setPath ( string p ) {
    this->physical_path    = p;
    this->has_been_renamed = true;
}

WriteFile::~WriteFile() {
    mlog(WF_DAPI, "Delete self %s", physical_path.c_str() );
    Close();
    if ( index ) {
        closeIndex();
        delete index;
        index = NULL;
    }

    pthread_mutex_destroy( &data_mux );
    pthread_mutex_destroy( &index_mux );
}

int WriteFile::sync( pid_t pid ) {
    int ret=0; 
    OpenFd *ofd = getFd( pid );
    if ( ofd == NULL ) {
        // ugh, sometimes FUSE passes in weird pids, just ignore this
        //ret = -ENOENT;
    } else {
        ret = ofd->plf->sync();
        Util::MutexLock( &index_mux, __FUNCTION__ );
        if ( ret == 0 ) index->flush();
        if ( ret == 0 ) index->sync();
        Util::MutexUnlock( &index_mux, __FUNCTION__ );
        if ( ret != 0 ) ret = -errno;
    }
    return ret;
}


// returns -errno or number of writers
int WriteFile::addWriter( pid_t pid, bool child ) {
    int ret = 0;

    Util::MutexLock(   &data_mux, __FUNCTION__ );
    struct OpenFd *ofd = getFd( pid );
    if ( ofd ) {
        ofd->writers++;
    } else {
        PhysicalLogfile *plf = 
            openDataFile(physical_path,hostname,pid,DROPPING_MODE);
        if (plf) {
            struct OpenFd ofd;
            ofd.writers = 1;
            ofd.plf = plf;
            fds[pid] = ofd;
        } else {
            ret = -errno;
            delete plf;
        }
    }
    int writers = incrementOpens(0); 
    if ( ret == 0 && ! child ) writers = incrementOpens(1);
    max_writers++;
    mlog(WF_DAPI, "%s (%d) on %s now has %d writers", 
            __FUNCTION__, pid, physical_path.c_str(), writers );
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ( ret == 0 ? writers : ret );
}

size_t WriteFile::numWriters( ) {
    int writers = incrementOpens(0); 
    bool paranoid_about_reference_counting = false;
    if ( paranoid_about_reference_counting ) {
        int check = 0;
        Util::MutexLock(   &data_mux, __FUNCTION__ );
        map<pid_t, OpenFd >::iterator pids_itr;
        for( pids_itr = fds.begin(); pids_itr != fds.end(); pids_itr++ ) {
            check += pids_itr->second.writers;
        }
        if ( writers != check ) {
            mlog(WF_DRARE, "%s %d not equal %d", __FUNCTION__, 
                    writers, check ); 
            assert( writers==check );
        }
        Util::MutexUnlock( &data_mux, __FUNCTION__ );
    }
    return writers;
}

// each pid has it's own OpenFd
struct OpenFd * WriteFile::getFd( pid_t pid ) {
    map<pid_t,OpenFd>::iterator itr;
    struct OpenFd *ofd = NULL;
    if ( (itr = fds.find( pid )) != fds.end() ) {
	/*
        ostringstream oss;
        oss << __FILE__ << ":" << __FUNCTION__ << " found fd " 
            << itr->second->fd << " with writers " 
            << itr->second->writers
            << " from pid " << pid;
        mlog(WF_DCOMMON, "%s", oss.str().c_str() );
	*/
        ofd = &(itr->second);
    } else {
        // here's the code that used to do it so a child could share
        // a parent fd but for some reason I commented it out
        /*
           // I think this code is a mistake.  We were doing it once
           // when a child was writing to a file that the parent opened
           // but shouldn't it be OK to just give the child a new datafile?
        if ( fds.size() > 0 ) {
            ostringstream oss;
            // ideally instead of just taking a random pid, we'd rather
            // try to find the parent pid and look for it
            // we need this code because we've seen in FUSE that an open
            // is done by a parent but then a write comes through as the child
            mlog(WF_DRARE, "%s WARNING pid %d is not mapped. "
                    "Borrowing fd %d from pid %d",
                    __FILE__, (int)pid, (int)fds.begin()->second->fd,
                    (int)fds.begin()->first );
            ofd = fds.begin()->second;
        } else {
            mlog(WF_DCOMMON, "%s no fd to give to %d", __FILE__, (int)pid);
            ofd = NULL;
        }
        */
        mlog(WF_DCOMMON, "%s no fd to give to %d", __FILE__, (int)pid);
        ofd = NULL;
    }
    return ofd;
}

int 
WriteFile::closeFd( PhysicalLogfile *plf ) {
    map<PhysicalLogfile *,string>::iterator paths_itr;
    paths_itr = paths.find(plf);
    int ret = 0;
    // shouldn't be possible to close twice since we remove it
    if (paths_itr == paths.end()) {
        // possibly it was already closed
        mlog(WF_DAPI, "%s:%s nothing to close\n", __FILE__,__FUNCTION__);
    } else {
        string path = paths_itr->second;
        ret = plf->close(); 
        mlog(WF_DAPI, "%s:%s closed open logfile for %s: %d %s",
            __FILE__, __FUNCTION__, path.c_str(), ret, 
            ( ret != 0 ? strerror(errno) : "success" ) );
        paths.erase(plf);
        delete plf;
    }
    return ret;
}

// returns -errno or number of writers
int 
WriteFile::removeWriter( pid_t pid ) {
    int ret = 0;
    Util::MutexLock(   &data_mux , __FUNCTION__);
    struct OpenFd *ofd = getFd( pid );
    int writers = incrementOpens(-1);
    if ( ofd == NULL ) {
        // if we can't find it, we still decrement the writers count
        // this is strange but sometimes fuse does weird things w/ pids
        // if the writers goes zero, when this struct is freed, everything
        // gets cleaned up
        mlog(WF_CRIT, "%s can't find pid %d", __FUNCTION__, pid );
        assert( 0 );
    } else {
        ofd->writers--;
        if (ofd->writers <= 0) {
            fds.erase(pid);
            ret = closeFd(ofd->plf);
        }
    }
    mlog(WF_DAPI, "%s (%d) on %s now has %d writers: %d", 
            __FUNCTION__, pid, physical_path.c_str(), writers, ret );
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ( ret == 0 ? writers : ret );
}

int WriteFile::extend( off_t offset ) {
    // make a fake write
    if ( fds.begin() == fds.end() ) return -ENOENT;
    pid_t p = fds.begin()->first;
    index->addWrite( offset, 0, p, createtime, createtime );
    addWrite( offset, 0 );   // maintain metadata
    return 0;
}

// we are currently doing synchronous index writing.
// this is where to change it to buffer if you'd like
// We were thinking about keeping the buffer around the
// entire duration of the write, but that means our appended index will
// have a lot duplicate information. buffer the index and flush on the close
//
// returns bytes written or -errno
ssize_t WriteFile::write(const char *buf, size_t size, off_t offset, pid_t pid){
    int ret = 0; 
    ssize_t written;
    OpenFd *ofd = getFd( pid );
    if ( ofd == NULL ) {
        // we used to return -ENOENT here but we can get here legitimately 
        // when a parent opens a file and a child writes to it.
        // so when we get here, we need to add a child datafile
        ret = addWriter( pid, true );
        if ( ret > 0 ) {
            // however, this screws up the reference count
            // it looks like a new writer but it's multiple writers
            // sharing an fd ...
            ofd = getFd( pid );
        }
    }
    if ( ofd && ret >= 0 ) {
        PhysicalLogfile *plf = ofd->plf;
        // write the data file if size > 0.
        double begin, end;
        begin = Util::getTime();
        ret = written = ( size ? plf->append(buf,size) : 0 );
        end = Util::getTime();

        // then the index
        if ( ret >= 0 ) {
            Util::MutexLock(   &index_mux , __FUNCTION__);
            index->addWrite( offset, ret, pid, begin, end );
            // TODO: why is 1024 a magic number?
            if (write_count%1024==0 && write_count>0) {
                index->flush();
                // Check if the index has grown too large stop buffering
                if(index->memoryFootprintMBs() > index_buffer_mbs) {
                    index->stopBuffering();
                    mlog(WF_DCOMMON, "The index grew too large, "
                         "no longer buffering");
                }
            }
            if (ret >= 0) addWrite(offset, size); // track our own metadata
            Util::MutexUnlock( &index_mux, __FUNCTION__ );
        }
    }
    write_count++;
    // return bytes written or error
    return ( ret >= 0 ? written : -errno );
}

// this assumes that the hostdir exists and is full valid path
// returns 0 or -errno
int WriteFile::openIndex( pid_t pid ) {
    int ret = 0;
    string index_path;
    PhysicalLogfile *plf = openIndexFile(physical_path, hostname, pid, 
            DROPPING_MODE, &index_path);
    if ( plf == NULL) {
        ret = -errno;
    } else {
        Util::MutexLock(&index_mux , __FUNCTION__);
        index = new Index(physical_path, plf);
        Util::MutexUnlock(&index_mux, __FUNCTION__);
        mlog(WF_DAPI, "In open Index path is %s",index_path.c_str());
        index->index_path=index_path;
        if(index_buffer_mbs) index->startBuffering();
    }
    return ret;
}

int WriteFile::closeIndex( ) {
    int ret = 0;
    Util::MutexLock(   &index_mux , __FUNCTION__);
    assert(index);
    ret = index->flush();
    ret = closeFd(index->getFd());
    delete(index);
    index = NULL;
    Util::MutexUnlock( &index_mux, __FUNCTION__ );
    return ret;
}

// returns 0 or -errno
int WriteFile::Close() {
    int failures = 0;

    Util::MutexLock(   &data_mux , __FUNCTION__);
    map<pid_t,OpenFd >::iterator itr;
        // these should already be closed here
        // from each individual pid's close but just in case
    for( itr = fds.begin(); itr != fds.end(); itr++ ) {
        if ( closeFd( itr->second.plf ) != 0 ) {
            failures++;
        }
    }
    fds.clear();
    Util::MutexUnlock( &data_mux, __FUNCTION__ );

    return ( failures ? -EIO : 0 );
}

// returns 0 or -errno 
int WriteFile::truncate( off_t offset ) {
    Metadata::truncate( offset );
    index->truncateHostIndex( offset );
    return 0;
}

PhysicalLogfile * 
WriteFile::openIndexFile(string path, string host, pid_t p, mode_t m,
        string* index_path) 
{
    *index_path = Container::getIndexPath(path,host,p,createtime);
    return openFile(*index_path,m);
}

PhysicalLogfile * 
WriteFile::openDataFile(string path, string host, pid_t p, mode_t m){
    return openFile(Container::getDataPath(path,host,p,createtime),m);
}

// returns a PhysicalLogfile or NULL 
PhysicalLogfile * 
WriteFile::openFile( string physicalpath, mode_t mode ) {
    PhysicalLogfile *plf = new PhysicalLogfile(physicalpath);
    mode_t old_mode=umask(0);
    int ret = plf->open(mode);
    mlog(WF_DAPI, "%s.%s open %s : %d %s", __FILE__, __FUNCTION__, 
            physicalpath.c_str(), ret, ( ret < 0 ? strerror(errno) : "" ) );
    if ( ret == 0 ) paths[plf] = physicalpath;    // remember so restore works
    umask(old_mode);
    if (ret != 0) {
        delete plf;
        return NULL;
    } else {
        return plf;
    }
}

// we call this after any calls to f_truncate
// if fuse::f_truncate is used, we will have open handles that get messed up
// in that case, we need to restore them
// what if rename is called and then f_truncate?
// return 0 or -errno
int 
WriteFile::restoreFds( ) {
    map<PhysicalLogfile *,string>::iterator paths_itr;
    map<pid_t, OpenFd >::iterator pids_itr;

    // if an open WriteFile ever gets truncated after being renamed, that
    // will be really tricky.  Let's hope that never happens, put an assert
    // to guard against it.  I guess it if does happen we just need to do
    // reg ex changes to all the paths
    assert( ! has_been_renamed );

    mlog(WF_DAPI, "Entering %s",__FUNCTION__);
    
    // first reset the index fd
    if ( index ) {
        PhysicalLogfile *plf = NULL;
        Util::MutexLock( &index_mux, __FUNCTION__ );
        index->flush();

        paths_itr = paths.find( index->getFd() );
        if ( paths_itr == paths.end() ) return -ENOENT;
        string indexpath = paths_itr->second;

        if ( closeFd( index->getFd() ) != 0 )    return -errno;
        if ( (plf = openFile( indexpath, mode )) == NULL ) return -errno;
        index->resetFd( plf );
        Util::MutexUnlock( &index_mux, __FUNCTION__ );
    }

    // then the data fds
    for( pids_itr = fds.begin(); pids_itr != fds.end(); pids_itr++ ) {

        paths_itr = paths.find( pids_itr->second.plf );
        if ( paths_itr == paths.end() ) return -ENOENT;

        string datapath = paths_itr->second;
        if ( closeFd( pids_itr->second.plf ) != 0 ) return -errno;

        pids_itr->second.plf = openFile( datapath, mode );
        if ( pids_itr->second.plf == NULL ) return -errno;
    }

    // normally we return ret at the bottom of our functions but this
    // function has so much error handling, we just cut out early on any 
    // error.  therefore, if we get here, it's happy days!
    mlog(WF_DAPI, "Exiting %s",__FUNCTION__);
    return 0;
}
