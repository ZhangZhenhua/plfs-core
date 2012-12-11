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
WriteFile::WriteFile(string logical, string path, string hostname, mode_t mode,
                     size_t buffer_mbs) : Metadata::Metadata()
{
    this->logical_path      = logical;
    this->container_path    = path;
    this->subdir_path       = path;
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

void WriteFile::setContainerPath ( string p )
{
    this->container_path    = p;
    this->has_been_renamed = true;
}

void WriteFile::setSubdirPath (string p)
{
    this->subdir_path     = p;
}

WriteFile::~WriteFile()
{
    mlog(WF_DAPI, "Delete self %s", container_path.c_str() );
    Close();
    if ( index ) {
        closeIndex();
        delete index;
        index = NULL;
    }
    pthread_mutex_destroy( &data_mux );
    pthread_mutex_destroy( &index_mux );
}

// a helper function to set OpenFd for each WriteType
int WriteFile::setOpenFd(OpenFd *ofd, int fd, WriteType wType)
{
    assert(ofd != NULL && fd > 0);
    switch( wType ){
       case SINGLE_HOST_WRITE:
          ofd->sh_fd = fd;
          break;
       case SIMPLE_FORMULA_WRITE:
       case SIMPLE_FORMULA_WITHOUT_INDEX:
          ofd->sf_fds.push_back(fd);
          break;
       default:
           mlog(WF_DRARE, "%s, unknown write type", __FUNCTION__);
           return -1;
    }
    return 0;
}

int WriteFile::sync()
{
    int ret = 0;
    // iterate through and sync all open fds
    Util::MutexLock( &data_mux, __FUNCTION__ );
    map<pid_t, OpenFd >::iterator pids_itr;
    WF_FD_ITR fd_itr;
    OpenFd *ofd;
    for( pids_itr = fds.begin(); pids_itr != fds.end() && ret==0; pids_itr++ ) {
        ofd = &pids_itr->second;
        for(fd_itr = ofd->sf_fds.begin(); fd_itr != ofd->sf_fds.end();
            fd_itr ++){
            ret = Util::Fsync( *fd_itr );
        }
        ret = Util::Fsync(ofd->sh_fd);
    }
    Util::MutexUnlock( &data_mux, __FUNCTION__ );

    // now sync the index
    Util::MutexLock( &index_mux, __FUNCTION__ );
    if ( ret == 0 ) {
        index->flush();
        index->sync();
    }
    Util::MutexUnlock( &index_mux, __FUNCTION__ );
    return ret;
}

int WriteFile::sync( pid_t pid )
{
    int ret=0;
    OpenFd *ofd = getFd( pid );
    if ( ofd == NULL ) {
        // ugh, sometimes FUSE passes in weird pids, just ignore this
        //ret = -ENOENT;
    } else {
        WF_FD_ITR itr;
        for(itr = ofd->sf_fds.begin(); itr != ofd->sf_fds.end(); itr ++ ){
            ret = Util::Fsync( *itr );
        }
        ret = Util::Fsync( ofd->sh_fd );
        Util::MutexLock( &index_mux, __FUNCTION__ );
        if ( ret == 0 ) {
            index->flush();
            index->sync();
        }
        Util::MutexUnlock( &index_mux, __FUNCTION__ );
        if ( ret != 0 ) {
            ret = -errno;
        }
    }
    return ret;
}

// returns -errno or number of writers
int WriteFile::addWriter( pid_t pid, bool child , WriteType wType)
{
    int ret = 0;
    Util::MutexLock(   &data_mux, __FUNCTION__ );
    struct OpenFd *ofd = getFd( pid );
    if ( ofd ) {
        ofd->writers++;
    } else {
        if (child==true){
            // child may use different hostdirId with parent, so
            // set it up corrently for child
            ContainerPaths paths;
            ret = findContainerPaths(logical_path,paths,pid);
            if (ret!=0){
                PLFS_EXIT(ret);
            }
            ret=Container::makeHostDir(paths, mode, PARENT_ABSENT,
                                       pid, Util::getTime());
        }
        int fd = openDataFile(subdir_path, hostname, pid, DROPPING_MODE, wType);
        if ( fd >= 0 ) {
            struct OpenFd ofd;
            ofd.writers = 1;
            setOpenFd(&ofd,fd,wType);
            fds[pid] = ofd;
        } else {
            ret = -errno;
        }
    }
    int writers = incrementOpens(0);
    if ( ret == 0 && ! child ) {
        writers = incrementOpens(1);
    }
    max_writers++;
    mlog(WF_DAPI, "%s (%d) on %s now has %d writers",
         __FUNCTION__, pid, container_path.c_str(), writers );
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ( ret == 0 ? writers : ret );
}

size_t WriteFile::numWriters( )
{
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

// ok, something is broken again.
// it shows up when a makefile does a command like ./foo > bar
// the bar gets 1 open, 2 writers, 1 flush, 1 release
// and then the reference counting is screwed up
// the problem is that a child is using the parents fd
struct OpenFd *WriteFile::getFd( pid_t pid ) {
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

int WriteFile::whichFd ( OpenFd *ofd, WriteType wType )
{
    int fd;
    if (ofd == NULL) return -1;
    switch( wType ){
       case SINGLE_HOST_WRITE:
          fd = ofd->sh_fd;
          break;
       case SIMPLE_FORMULA_WRITE:
       case SIMPLE_FORMULA_WITHOUT_INDEX:
          if ( ofd->sf_fds.empty() ){
              fd = -1;
          }else{
              fd = ofd->sf_fds.back();
          }
          break;
       default:
           mlog(WF_DRARE, "%s, unknown write type", __FUNCTION__);
           fd = -1;
    }
    return fd;
}

int WriteFile::closeFd( int fd )
{
    map<int,string>::iterator paths_itr;
    paths_itr = paths.find( fd );
    string path = ( paths_itr == paths.end() ? "ENOENT?" : paths_itr->second );
    int ret = Util::Close( fd );
    mlog(WF_DAPI, "%s:%s closed fd %d for %s: %d %s",
         __FILE__, __FUNCTION__, fd, path.c_str(), ret,
         ( ret != 0 ? strerror(errno) : "success" ) );
    paths.erase ( fd );
    return ret;
}

// returns -errno or number of writers
int
WriteFile::removeWriter( pid_t pid )
{
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
        if ( ofd->writers <= 0 ) {
            WF_FD_ITR itr;
            for(itr = ofd->sf_fds.begin(); itr != ofd->sf_fds.end(); itr ++ ){
                ret = closeFd( *itr );
            }
            ret = closeFd(ofd->sh_fd);
            fds.erase( pid );
        }
    }
    mlog(WF_DAPI, "%s (%d) on %s now has %d writers: %d",
         __FUNCTION__, pid, container_path.c_str(), writers, ret );
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ( ret == 0 ? writers : ret );
}

int
WriteFile::extend( off_t offset )
{
    // make a fake write
    if ( fds.begin() == fds.end() ) {
        return -ENOENT;
    }
    pid_t p = fds.begin()->first;
    index->extend(offset, p, createtime);
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
ssize_t
WriteFile::write(const char *buf, size_t size, off_t offset, pid_t pid,
                 WriteType wType)
{
    int ret = 0;
    ssize_t written;
    OpenFd *ofd = getFd( pid );
    if ( ofd == NULL ) {
        // we used to return -ENOENT here but we can get here legitimately
        // when a parent opens a file and a child writes to it.
        // so when we get here, we need to add a child datafile
        ret = addWriter( pid, true , wType);
        if ( ret > 0 ) {
            // however, this screws up the reference count
            // it looks like a new writer but it's multiple writers
            // sharing an fd ...
            ofd = getFd( pid );
        }
    }
    int fd = whichFd(ofd, wType);
    if ( fd == -1 ){
        fd = openNewDataFile(pid, wType);
    }
    if ( fd > 0 ) {
        // write the data file
        double begin, end;
        begin = Util::getTime();
        ret = written = ( size ? Util::Write( fd, buf, size ) : 0 );
        end = Util::getTime();
        // then the index
        if ( ret >= 0 ) {
            write_count++;
            Util::MutexLock(   &index_mux , __FUNCTION__);
            if (wType == SINGLE_HOST_WRITE){
                index->addWrite( offset, ret, pid, begin, end );
            }else if (wType == SIMPLE_FORMULA_WRITE){
                index->updateSimpleFormula(begin,end);
            }else if (wType == SIMPLE_FORMULA_WITHOUT_INDEX){
                // do nothing
            }else{
                mlog(WF_DCOMMON, "Unexpected write type %d", wType);
                return -1;
            }
            // TODO: why is 1024 a magic number?
            int flush_count = 1024;
            if (write_count%flush_count==0) {
                ret = index->flush();
                // Check if the index has grown too large stop buffering
                if(index->memoryFootprintMBs() > index_buffer_mbs) {
                    index->stopBuffering();
                    mlog(WF_DCOMMON, "The index grew too large, "
                         "no longer buffering");
                }
            }
            if (ret >= 0) {
                addWrite(offset, size);    // track our own metadata
            }
            Util::MutexUnlock( &index_mux, __FUNCTION__ );
        }
    }
    // return bytes written or error
    return ( ret >= 0 ? written : -errno );
}

// this assumes that the hostdir exists and is full valid path
// returns 0 or -errno
int WriteFile::openIndex( pid_t pid, WriteType wType) {
    int ret = 0;
    bool new_index = false;
    string index_path;

    int fd = openIndexFile(subdir_path, hostname, pid, DROPPING_MODE,
                           &index_path, wType);
    if ( fd < 0 ) {
        ret = -errno;
    } else {
        if ( index == NULL ) {
           Util::MutexLock(&index_mux , __FUNCTION__);
           if ( index == NULL ){
              index = new Index(container_path);
              new_index = true;
           }
           Util::MutexUnlock(&index_mux, __FUNCTION__);
        }
        mlog(WF_DAPI, "In open Index path is %s",index_path.c_str());
        index->setCurrentFd(fd,index_path);
        if(new_index && index_buffer_mbs) {
            index->startBuffering();
        }
    }
    return ret;
}

int WriteFile::closeIndex( )
{
    int ret = 0;
    vector< int > fd_list;
    vector< int >::iterator itr;
    Util::MutexLock(   &index_mux , __FUNCTION__);
    ret = index->flush();
    index->getFd( fd_list );
    for(itr=fd_list.begin(); itr!=fd_list.end(); itr++){
        ret = closeFd( *itr );
    }
    delete( index );
    index = NULL;
    Util::MutexUnlock( &index_mux, __FUNCTION__ );
    return ret;
}

int WriteFile::openNewDataFile( pid_t pid, WriteType wType )
{
    int ret;
    Util::MutexLock(   &data_mux, __FUNCTION__ );
    int fd = openDataFile( subdir_path, hostname, pid, DROPPING_MODE, wType);
    if ( fd >= 0 ) {
        struct OpenFd *ofd = getFd( pid );
        if (ofd == NULL){
            struct OpenFd ofd;
            ofd.writers = 1;
            setOpenFd(&ofd, fd, wType);
            fds[pid] = ofd;
        } else {
            setOpenFd(ofd, fd, wType);
        }
        ret = fd;
    } else {
        ret = -errno;
    }
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ret;
}

// returns 0 or -errno
int WriteFile::Close()
{
    int failures = 0;
    Util::MutexLock(   &data_mux , __FUNCTION__);
    map<pid_t,OpenFd >::iterator itr;
    WF_FD_ITR fd_itr;
    // these should already be closed here
    // from each individual pid's close but just in case
    for( itr = fds.begin(); itr != fds.end(); itr++ ) {
        for(fd_itr = itr->second.sf_fds.begin();
            fd_itr != itr->second.sf_fds.end();
            fd_itr++){
            if ( closeFd( *fd_itr ) != 0 ) {
                failures++;
            }
        }
        if ( closeFd( itr->second.sh_fd ) != 0 ){
            failures++;
        }
    }
    fds.clear();
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ( failures ? -EIO : 0 );
}

// returns 0 or -errno
int WriteFile::truncate( off_t offset )
{
    Metadata::truncate( offset );
    index->truncateHostIndex( offset );
    return 0;
}

int WriteFile::openIndexFile(string path, string host, pid_t p, mode_t m,
                             string *index_path, WriteType wType)
{
    switch (wType){
        case SINGLE_HOST_WRITE:
           *index_path = Container::getIndexPath(path,host,p,createtime,
                                                 BYTERANGE);
           break;
        case SIMPLE_FORMULA_WRITE:
           if (index->getFd(SIMPLEFORMULA) > 0 ) return -1;
           // store SimpleFormula index file in canonical container
           // so we can always get the correct metalink locations,
           // even after renaming the file
           *index_path = Container::getIndexPath(container_path,host,p,
                                                 metalinkTime,SIMPLEFORMULA);
           break;
        case SIMPLE_FORMULA_WITHOUT_INDEX:
           // do nothing
           return -1;
        default:
           mlog(WF_DRARE, "%s, unexpected write type", __FUNCTION__);
           return -1;
    }
    mlog(WF_DBG, "WF: opening index file %s", (*index_path).c_str());
    return openFile(*index_path,m);
}

int WriteFile::openDataFile(string path, string host, pid_t p, mode_t m,
                            WriteType wType)
{
    string data_path;
    switch (wType){
        case SINGLE_HOST_WRITE:
           data_path = Container::getDataPath(path,host,p,createtime,
                                              BYTERANGE);
           break;
        case SIMPLE_FORMULA_WRITE:
        case SIMPLE_FORMULA_WITHOUT_INDEX:
           data_path = Container::getDataPath(path,host,p,formulaTime,
                                              SIMPLEFORMULA);
           break;
        default:
           mlog(WF_DRARE, "%s, unknown write type", __FUNCTION__);
           return -1;
    }
    mlog(WF_DBG, "WF: opening data file %s", data_path.c_str());
    return openFile(data_path , m);
}

// returns an fd or -1
int WriteFile::openFile( string physicalpath, mode_t mode )
{
    mode_t old_mode=umask(0);
    int flags = O_WRONLY | O_APPEND | O_CREAT;
    int fd = Util::Open( physicalpath.c_str(), flags, mode );
    if ( fd >= 0 ) {
        paths[fd] = physicalpath;    // remember so restore works
    }
    mlog(WF_DAPI, "%s.%s open %s : %d %s",
         __FILE__, __FUNCTION__,
         physicalpath.c_str(),
         fd, ( fd < 0 ? strerror(errno) : "" ) );
    umask(old_mode);
    return ( fd >= 0 ? fd : -errno );
}

// a helper function to resotre data file fd
int WriteFile::restore_helper(int fd, string *path)
{
    int ret;
    map<int,string>::iterator paths_itr;
    paths_itr = paths.find( fd );
    if ( paths_itr == paths.end() ) {
        return -ENOENT;
    }
    *path = paths_itr->second;
    if ( closeFd( fd ) != 0 ) {
        return -errno;
    }
    ret = openFile( *path, mode );
    if ( ret < 0 ) {
        return -errno;
    }
    return ret;
}

// we call this after any calls to f_truncate
// if fuse::f_truncate is used, we will have open handles that get messed up
// in that case, we need to restore them
// what if rename is called and then f_truncate?
// return 0 or -errno
int WriteFile::restoreFds( bool droppings_were_truncd )
{
    map<pid_t, OpenFd >::iterator pids_itr;
    WF_FD_ITR fd_itr;
    OpenFd *ofd;
    int ret = 0;
    // "has_been_renamed" is set at "addWriter, setPath" executing path.
    // This assertion will be triggered when user open a file with write mode
    // and do truncate. Has nothing to do with upper layer rename so I comment
    // out this assertion but remain previous comments here.
    // if an open WriteFile ever gets truncated after being renamed, that
    // will be really tricky.  Let's hope that never happens, put an assert
    // to guard against it.  I guess it if does happen we just need to do
    // reg ex changes to all the paths
    //assert( ! has_been_renamed );
    mlog(WF_DAPI, "Entering %s",__FUNCTION__);
    // first reset the index fd
    if ( index ) {
        Util::MutexLock( &index_mux, __FUNCTION__ );
        index->flush();
        vector< int > fd_list;
        index->getFd( fd_list );
        vector< int >::iterator itr;
        for(itr=fd_list.begin(); itr!=fd_list.end(); itr++){
            string indexpath;
            if ( (ret = restore_helper(*itr, &indexpath)) < 0 ){
                return ret;
            }
            index->setCurrentFd( ret, indexpath );
        }
        if (droppings_were_truncd) {
            // this means that they were truncd to 0 offset
            index->resetPhysicalOffsets();
        }
        Util::MutexUnlock( &index_mux, __FUNCTION__ );
    }
    // then the data fds
    for( pids_itr = fds.begin(); pids_itr != fds.end(); pids_itr++ ) {
        ofd = &pids_itr->second;
        string unused;
        // handle ByteRange data file first
        if ( (ret = restore_helper(ofd->sh_fd, &unused)) < 0 ){
            return ret;
        } 
        ofd->sh_fd = ret;
        // then SimpleFormula data files
        for(fd_itr = ofd->sf_fds.begin(); fd_itr != ofd->sf_fds.end();
            fd_itr ++){
            if ( (ret = restore_helper(*fd_itr, &unused)) < 0 ){
                return ret;
            }
            *fd_itr = ret;
        }
    }
    // normally we return ret at the bottom of our functions but this
    // function had so much error handling, I just cut out early on any
    // error.  therefore, if we get here, it's happy days!
    mlog(WF_DAPI, "Exiting %s",__FUNCTION__);
    return 0;
}
