#include <sys/time.h>
#include "COPYRIGHT.h"
#include <time.h>
#include <math.h>
#include <sstream>
using namespace std;

#include "Container.h"
#include "plfs.h"
#include "Util.h"

#define BLKSIZE 512
extern SharedState shared;
                
blkcnt_t Container::bytesToBlocks( size_t total_bytes ) {
    return (blkcnt_t)ceil((float)total_bytes/BLKSIZE);
}

size_t Container::hashValue( const char *str ) {
        // wonder if we need a fancy hash function or if we could just
        // count the bits or something in the string?
        // be pretty simple to just sum each char . . .
    size_t sum = 0, i;
    for( i = 0; i < strlen( str ); i++ ) {
        sum += (size_t)str[i];
    }
    //cerr << __FUNCTION__ << " : " << str << " => " << sum << endl;
    return sum;
    /*
    #include <openssl/md5.h>
    unsigned char *ret = NULL;
    unsigned char value[MD5_DIGEST_LENGTH/sizeof(unsigned char)];
    ret = MD5( str, strlen(str), &value ); 
    */
}

// our simple rule currently is a directory with S_ISUID set
// maybe change it to just be a directory with some magic string
// in the name.  The problem with the S_ISUID bit is that it
// makes makeTopLevel much more complicated
// the problem with the magic string is it screws up readdir
// it's really convenient that the name of the user's logical file
// is the same name as the container
bool Container::isContainer( const char *physical_path ) {
    struct stat buf;
    if ( Util::Lstat( physical_path, &buf ) != 0 ) {
        return false;
    }
    return isContainer( &buf );
}

bool Container::isContainer( struct stat *buf ) {
    bool ret = ( buf->st_mode & S_IFDIR && buf->st_mode & S_ISUID );
    return ret;
}

int Container::freeIndex( Index **index ) {
    delete *index;
    *index = NULL;
    return 0;
}

// a helper routine for functions that are accessing a file
// which may not exist.  It's not an error if it doesn't
// exist since it might not exist yet due to an error condition
int Container::ignoreNoEnt( int ret ) {
    if ( ret != 0 && ( errno == ENOENT || errno == ENOTDIR ) ) {
        return 0;
    } else {
        return ret;
    }
}

int Container::Chmod( const char *path, mode_t mode ) {
    return Container::Modify( CHMOD, path, 0, 0, NULL, mode );  
}

// just do the droppings and the access file
int Container::Utime( const char *path, const struct utimbuf *buf ) {
    return Container::Modify( UTIME, path, 0, 0, buf, 0 );  
}

int Container::Chown( const char *path, uid_t uid, gid_t gid ) {
    return Container::Modify( CHOWN, path, uid, gid, NULL, 0 );  
}

int Container::Modify( ContainerModification type, const char *path,
        uid_t uid, gid_t gid,
        const struct utimbuf *utbuf,
        mode_t mode )
{
    cerr << __FUNCTION__ << " on " << path << endl;
    struct dirent *dent = NULL;
    DIR *dir            = NULL; 
    int ret             = 0;

    Util::Opendir( path, &dir );
    if ( dir == NULL ) { cerr << "wtf?" << endl; return 0; }
    while( ret == 0 && (dent = readdir( dir )) != NULL ) {
        mode_t use_mode = mode;
        if ( ! strncmp( dent->d_name, ".", 1 ) ) continue;  // skip . and .. 
        string full_path( path ); full_path += "/"; full_path += dent->d_name;
        if ( Util::isDirectory( full_path.c_str() ) ) {
            ret = Container::Modify(type,full_path.c_str(),uid, gid,utbuf,mode);
            if ( ret != 0 ) break;
            use_mode = dirMode( mode );
        }
        if ( type == UTIME ) {
            ret = Util::Utime( full_path.c_str(), utbuf );
        } else if ( type == CHOWN ) {
            ret = Util::Chown( full_path.c_str(), uid, gid );
        } else if ( type == CHMOD ) {
            ret = Util::Chmod( full_path.c_str(), use_mode );
        }
        cerr << "Modified dropping " << full_path << ": " << ret << endl; 
    }
    Util::Closedir( dir );
    return ret;
}

// this is the function that returns the container index
// should first check for top-level index and if it exists, just use it
int Container::populateIndex( const char *path, Index *index ) {
    int ret;
    string hostindex;
    
    DIR *td = NULL, *hd = NULL; struct dirent *tent = NULL;
    while((ret = nextdropping(path,&hostindex,INDEXPREFIX, &td,&hd,&tent))== 1){
        //fprintf( stderr, "# need to build index from %s\n", hostindex.c_str() );
        ret = index->readIndex( hostindex );
        if ( ret != 0 ) break;
    }
    return ret; 
}

string Container::getDataPath( const char *path, const char *host, int pid ) {
    return getChunkPath( path, host, pid, DATAPREFIX );
}

string Container::getIndexPath( const char *path, const char *host, int pid ) {
    return getChunkPath( path, host, pid, INDEXPREFIX );
}

// now host index is shared by multiple pids
string Container::getIndexPath( const char *path, const char *host ) {
    return getChunkPath( path, host, SHARED_PID, INDEXPREFIX );
}

// this function takes a container path, a hostname, a pid, and a type and 
// returns a path to a chunk (type is either DATAPREFIX or INDEXPREFIX)
// the resulting path looks like this:
// container/HOSTDIRPREFIX.hash(host)/type.host.pid
string Container::getChunkPath( const char *container, const char *host, 
        int pid, const char *type )
{
    return chunkPath( getHostDirPath(container,host).c_str(), type, host, pid );
}

string Container::chunkPath( const char *hostdir, const char *type, 
        const char *host, int pid ) 
{
    ostringstream oss;
    oss << hostdir << "/" << type << host << "." << pid;
    return oss.str();
}

string Container::hostdirFromChunk( string chunkpath, const char *type ) {
    chunkpath.erase( chunkpath.rfind(type), chunkpath.size() );
    return chunkpath;
}

// take the path to an index and a pid, and return the path to that chunk file
// path to index looks like: container/HOSTDIRPREFIX.XXX/INDEXPREFIX.host.pid
string Container::chunkPathFromIndexPath( string hostindex, pid_t pid ) {
    string host      = hostFromChunk( hostindex, INDEXPREFIX );
    string hostdir   = hostdirFromChunk( hostindex, INDEXPREFIX );
    return chunkPath( hostdir.c_str(), DATAPREFIX, host.c_str(), pid );
}

// a chunk looks like: container/HOSTDIRPREFIX.XXX/type.host.pid
string Container::containerFromChunk( string chunkpath ) {
    chunkpath.erase( chunkpath.rfind(HOSTDIRPREFIX), chunkpath.size() );
    return chunkpath;
}

// a chunk looks like: container/HOSTDIRPREFIX.XXX/type.host.pid
// where type is either DATAPREFIX or INDEXPREFIX
string Container::hostFromChunk( string chunkpath, const char *type ) {
    chunkpath.erase( 0, chunkpath.rfind(type) + strlen(type) );
    chunkpath.erase( chunkpath.rfind("."), chunkpath.size() );
    return chunkpath;
}

// this function drops a file in the metadir which contains
// stat info so that we can later satisfy stats using just readdir
int Container::addMeta( off_t last_offset, size_t total_bytes, 
        const char *path, const char *host ) 
{
    string metafile;
    struct timeval time;
    if ( gettimeofday( &time, NULL ) != 0 ) {
        fprintf( stderr, "WTF: gettimeofday in %s failed: %s\n",
                __FUNCTION__, strerror(errno ) );
        return -errno;
    }
    ostringstream oss;
    oss << getMetaDirPath(path) << "/" 
        << last_offset << "." << total_bytes  << "."
        << time.tv_sec << "." << time.tv_usec << "."
        << host;
    metafile = oss.str();
    return ignoreNoEnt(Util::Creat( metafile.c_str(), DEFAULT_MODE ));
}

string Container::fetchMeta( string metafile_name, 
        off_t *last_offset, size_t *total_bytes,
        struct timespec *time ) 
{
    istringstream iss( metafile_name );
    string host;
    char dot;
    iss >> *last_offset >> dot >> *total_bytes
        >> dot >> time->tv_sec >> dot
        >> time->tv_nsec >> dot >> host;
    time->tv_nsec *= 1000; // convert from micro
    return host;
}

string Container::getOpenHostsDir( string path ) {
    string openhostsdir( path );
    openhostsdir += "/";
    openhostsdir += OPENHOSTDIR;
    return openhostsdir;
}

// a function that reads the open hosts dir to discover which hosts currently
// have the file open
int Container::discoverOpenHosts( const char *path, set<string> *openhosts ) {
    struct dirent *dent = NULL;
    DIR *openhostsdir   = NULL; 
    Util::Opendir( (getOpenHostsDir(path)).c_str(), &openhostsdir );
    if ( openhostsdir == NULL ) return 0;
    while( (dent = readdir( openhostsdir )) != NULL ) {
        if ( ! strncmp( dent->d_name, ".", 1 ) ) continue;  // skip . and ..
        cerr << "Host " <<dent->d_name <<" has open handle on " <<path <<endl;
        openhosts->insert( dent->d_name );
    }
    Util::Closedir( openhostsdir );
    return 0;
}

string Container::getOpenrecord( const char *path, const char *host ) {
    string openrecord = getOpenHostsDir( path );
    openrecord += "/";
    openrecord += host;
    return openrecord;
}

// if this fails because the openhostsdir doesn't exist, then make it
// and try again
int Container::addOpenrecord( const char *path, const char *host ) {
    string openrecord = getOpenrecord( path, host );
    int ret = Util::Creat( openrecord.c_str(), DEFAULT_MODE );
    if ( ret != 0 && ( errno == ENOENT || errno == ENOTDIR ) ) {
        makeMeta(getOpenHostsDir(path), S_IFDIR, DEFAULT_MODE);
        ret = Util::Creat( openrecord.c_str(), DEFAULT_MODE );
    }
    return ret;
}

int Container::removeOpenrecord( const char *path, const char *host ) {
    string openrecord = getOpenrecord( path, host ); 
    return Util::Unlink( openrecord.c_str() );
}

// can this work without an access file?
// just return the directory mode right but change it to be a normal file
mode_t Container::getmode( const char *path ) {
    struct stat stbuf;
    if ( Util::Lstat( path, &stbuf ) < 0 ) {
        cerr << "Failed to getmode for " << path << endl;
        return DEFAULT_MODE;
    } else {
        return fileMode(stbuf.st_mode);
    }
}

// assumes the stbuf struct is already populated w/ stat of top level container 
int Container::getattr( const char *path, struct stat *stbuf ) {
        // Need to walk the whole structure
        // and build up the stat.
        // three ways to do so:
        // used cached info when available
        // otherwise, either stat the data files or
        // read the index files
        // stating the data files is much faster 
        // (see ~/Testing/plfs/doc/SC09/data/stat/stat_full.png)
        // but doesn't correctly account for holes
        // but reading index files might fail if they're being buffered
        // safest to use_cache and stat_data
        // ugh, but we can't stat the data dropping, actually need to read the
        // index.  this is because Chombo truncates the thing to a future
        // value and we don't see it since it's only in the index file
        // maybe safest to get all of them.  But using both is no good bec 
        // it adds both the index and the data.  ugh.
    bool use_cache = true;
    const char *prefix   = INDEXPREFIX;
    
    int chunks = 0;
    int ret = 0;

        // the easy stuff has already been copied from the directory
        // but get the permissions and stuff from the access file
    string accessfile = getAccessFilePath( path );
    if ( Util::Lstat( accessfile.c_str(), stbuf ) < 0 ) {
        fprintf( stderr, "lstat of %s failed: %s\n",
                accessfile.c_str(), strerror( errno ) );
    }
    stbuf->st_size    = 0;  
    stbuf->st_blocks  = 0;
    stbuf->st_mode    = fileMode( stbuf->st_mode );

        // first read the open dir to see who has
        // the file open
        // then read the meta dir to pull all useful
        // droppings out of there (use everything as
        // long as it's not open), if we can't use
        // meta than we need to pull the info from
        // the hostdir by stating the data files and
        // maybe even actually reading the index files!
    set< string > openHosts;
    set< string > validMeta;
    if ( use_cache ) {
        discoverOpenHosts( path, &openHosts );
        time_t most_recent_mod = 0;

        DIR *metadir;
        Util::Opendir( (getMetaDirPath(path)).c_str(), &metadir );
        struct dirent *dent = NULL;
        if ( metadir != NULL ) {
            while( (dent = readdir( metadir )) != NULL ) {
                if ( ! strncmp( dent->d_name, ".", 1 ) ) continue;  // . and ..
                off_t last_offset;
                size_t total_bytes;
                struct timespec time;
                string host = fetchMeta( dent->d_name, 
                        &last_offset, &total_bytes, &time );
                if ( openHosts.find(host) != openHosts.end() ) {
                    cerr << "Can't use metafile " << dent->d_name << " because "
                         << host << " has an open handle." << endl;
                    continue;
                }
                cerr << "Pulled meta " << last_offset << " " << total_bytes
                     << ", " << time.tv_sec << "." << time.tv_nsec 
                     << " on host " << host << endl;

                // oh, let's get rewrite correct.  if someone writes
                // a file, and they close it and then later they
                // open it again and write some more then we'll
                // have multiple metadata droppings.  That's fine.
                // just consider all of them.
                stbuf->st_size   =  max( stbuf->st_size, last_offset );
                stbuf->st_blocks += bytesToBlocks( total_bytes );
                most_recent_mod  =  max( most_recent_mod, time.tv_sec );
                validMeta.insert( host );
            }
            Util::Closedir( metadir );
        }
        stbuf->st_mtime = most_recent_mod;
    }

    // if we're using cached data we don't do this part unless there
    // were open hosts
    if ( ! use_cache || openHosts.size() > 0 ) {
        string dropping; 
        blkcnt_t index_blocks = 0, data_blocks = 0;
        off_t    index_size = 0, data_size = 0;
        DIR *td = NULL, *hd = NULL; struct dirent *tent = NULL;
        while((ret=nextdropping(path,&dropping,prefix, &td,&hd,&tent))==1)
        {
            string host = hostFromChunk( dropping, prefix );
            if ( validMeta.find(host) != validMeta.end() ) {
                cerr << "Used stashed stat info for " << host << endl;
                continue;
            } else {
                //cerr << "Will aggregate stat info for " << host << endl;
                chunks++;
            }

            // we'll always stat the dropping to get at least the timestamp
            // if it's an index dropping then we'll read it
            // it it's a data dropping, we'll just use more stat info
            struct stat dropping_st;
            if (Util::Lstat(dropping.c_str(), &dropping_st) < 0 ) {
                ret = -errno;
                fprintf( stderr, "lstat of %s failed: %s\n",
                    dropping.c_str(), strerror( errno ) );
                continue;   // shouldn't this be break?
            }
            stbuf->st_ctime = max( dropping_st.st_ctime, stbuf->st_ctime );
            stbuf->st_atime = max( dropping_st.st_atime, stbuf->st_atime );
            stbuf->st_mtime = max( dropping_st.st_mtime, stbuf->st_mtime );

            if ( dropping.find(DATAPREFIX) != dropping.npos ) {
                data_blocks += dropping_st.st_blocks;
                data_size   += dropping_st.st_size;
            } else {
                Index *index = new Index( path, SHARED_PID );
                index->readIndex( dropping ); 
                index_blocks     += bytesToBlocks( index->totalBytes() );
                index_size        = max( index->lastOffset(), index_size );
                delete index;
            }

        }
        stbuf->st_blocks += max( data_blocks, index_blocks );
        stbuf->st_size   = max( stbuf->st_size, max( data_size, index_size ) );
    }
    cerr << "Examined " << chunks << " droppings:"
         << path << " total size " << stbuf->st_size <<  ", usage "
         << stbuf->st_blocks << " at " << stbuf->st_blksize << endl;
    return ret;
}

// this assumes we're in a mutex!
// it's a little bit complex bec we use S_ISUID to determine whether it is 
// a container.  but Mkdir doesn't handle S_ISUID so we can't do an atomic
// Mkdir.  
// we need to Mkdir, chmod, rename
// we want atomicity bec someone might make a dir of the same name as a file
// and we need to be absolutely sure a container is really a container and not
// just a directory
// returns -errno or 0
int Container::makeTopLevel( const char *expanded_path,  const char *hostname, mode_t mode ){
    /*
        // ok, instead of mkdir tmp ; chmod tmp ; rename tmp top
        // we tried just mkdir top ; chmod top to remove the rename 
        // but the damn chmod would sometimes take 5 seconds on 5 hosts
        // also, this is safe within an MPI group but if some
        // external proc does an ls or something, it might catch
        // it between the mkdir and the chmod, so the mkdir/chmod/rename
        // is best
    */

    // ok, let's try making the temp dir in /tmp
    // change all slashes to .'s so we have a unique filename
    // actually, if we use /tmp, probably don't need the hostname in there...
    // shouldn't need the pid in there because this should be wrapped in a mux
    // doesn't work in /tmp because rename can't go acros different file sys's

    // ok, here's the real code:  mkdir tmp ; chmod tmp; rename tmp
    string strPath( expanded_path );
    string tmpName( strPath + "." + hostname ); 
    if ( Util::Mkdir( tmpName.c_str(), dirMode(mode) ) < 0 ) {
        if ( errno != EEXIST && errno != EISDIR ) {
            fprintf( stderr, "Mkdir %s to %s failed: %s\n",
                tmpName.c_str(), expanded_path, strerror(errno) );
            return -errno;
        }
    }
    if (Util::Chmod( tmpName.c_str(), containerMode(mode) ) < 0 ) {
        fprintf( stderr, "chmod %s to %s failed: %s\n",
                tmpName.c_str(), expanded_path, strerror(errno) );
        int saveerrno = errno;
        if ( Util::Rmdir( tmpName.c_str() ) != 0 ) {
            fprintf( stderr, "rmdir of %s failed : %s\n",
                tmpName.c_str(), strerror(errno) );
        }
        return -saveerrno;
    }
    
    // ok, this rename sometimes takes a long time
    // what if we check first to see if the dir already exists
    // and if it does don't bother with the rename
    // this just moves the bottleneck to the isDirectory
    // plus scared it could double it if they were both slow...
    //if ( ! isDirectory( expanded_path ) ) 
    if ( Util::Rename( tmpName.c_str(), expanded_path ) < 0 ) {
        int saveerrno = errno;
        if ( Util::Rmdir( tmpName.c_str() ) < 0 ) {
            fprintf( stderr, "rmdir of %s failed : %s\n",
                    tmpName.c_str(), strerror(errno) );
        }
        // probably what happened is some other node outraced us
        // if it is here now as a container, that's what happened
        // this check for whether it's a container might be slow
        // if worried about that, change it to check saveerrno
        // if it's something like EEXIST or ENOTEMPTY or EISDIR
        // then that probably means the same thing 
        //if ( ! isContainer( expanded_path ) ) 
        if ( saveerrno != EEXIST && saveerrno != ENOTEMPTY 
                && saveerrno != EISDIR && saveerrno != ENOENT ) {
            fprintf( stderr, "rename %s to %s failed: %s\n",
                    tmpName.c_str(), expanded_path, strerror(saveerrno) );
            return -saveerrno;
        }
    } else {
        // we made the top level container
        // this is like the only time we know that we won the global race
        // hmmm, any optimizations we could make here?
        // make the metadir after we do the rename so that all nodes
        // don't make an extra unnecessary dir, but this does create
        // a race if someone wants to use the meta dir and it doesn't
        // exist, so we need to make sure we never assume the metadir
        if ( makeMeta( getMetaDirPath( strPath ), S_IFDIR, DEFAULT_MODE ) < 0 ){
            return -errno;
        }
        if ( makeMeta( getOpenHostsDir(strPath), S_IFDIR, DEFAULT_MODE ) < 0 ) {
            return -errno;
        }
        if ( makeMeta( getAccessFilePath(strPath), S_IFREG, mode ) < 0 ) {
            return -errno;
        }
    }
    return 0;
}

// returns 0 or -errno
int Container::makeHostDir( const char *path, const char *host, mode_t mode ) {
    int ret = makeMeta( getHostDirPath(path,host), S_IFDIR, dirMode(mode) );
    return ( ret == 0 ? ret : -errno );
}

// this just creates a dir/file but it ignores an EEXIST error
int Container::makeMeta( string path, mode_t type, mode_t mode ) {
    int ret;
    if ( type == S_IFDIR ) {
        ret = Util::Mkdir( path.c_str(), mode );
    } else if ( type == S_IFREG ) {
        ret = Util::Creat( path.c_str(), mode ); 
    } else {
        cerr << "WTF.  Unknown type passed to " << __FUNCTION__ << endl;
        ret = -1;
        errno = ENOSYS;
    }
    return ( ret == 0 || errno == EEXIST ) ? 0 : -1;
}

// this returns the path to the metadir
// don't ever assume that this exists bec it's possible
// that it doesn't yet
string Container::getMetaDirPath( string strPath ) {
    string metadir( strPath + "/" + METADIR ); 
    return metadir;
}

string Container::getAccessFilePath( string path ) {
    string accessfile( path + "/" + ACCESSFILE );
    return accessfile;
}

string Container::getHostDirPath( const char* expanded_path, 
        const char* hostname )
{
    ostringstream oss;
    size_t host_value = (hashValue( hostname ) % shared.params.subdirs) + 1;
    oss << expanded_path << "/" << HOSTDIRPREFIX << host_value; 
    fprintf( stderr, "%s : %s %s -> %s\n", 
            __FUNCTION__, hostname, expanded_path, oss.str().c_str() );
    return oss.str();
}

// this makes the mode of a directory look like it's the mode
// of a file.  
// e.g. someone does a stat on a container, make it look like a file
mode_t Container::fileMode( mode_t mode ) {
    int dirmask  = ~(S_ISUID | S_IFDIR);
    mode         = ( mode & dirmask ) | S_IFREG;    
    return mode;
}

// this makes a mode for a file look like a directory
// e.g. someone is creating a file which is actually a container
// so use this to get the mode to pass to the mkdir
mode_t Container::dirMode( mode_t mode ) {
    int filemask = ~(S_IFREG);
    mode = ( mode & filemask ) | S_IXUSR | S_IXGRP | S_IFDIR;
    return mode;
}

mode_t Container::containerMode( mode_t mode ) {
    return dirMode(mode) | S_ISUID;
}

int Container::createHelper( const char *expanded_path, const char *hostname, 
        mode_t mode, int flags, int *extra_attempts ) 
{
    // TODO we're in a mutex here so only one thread will
    // make the dir, and the others will stat it
    // but we could reduce the number of stats by maintaining
    // some memory state that the first thread sets and the
    // others check

        // first the top level container
    double begin_time, end_time;
    int res = 0;
    if ( ! isContainer( expanded_path ) ) {
        fprintf( stderr, "Making top level container %s\n", expanded_path );
        begin_time = time(NULL);
        res = makeTopLevel( expanded_path, hostname, mode );
        end_time = time(NULL);
        if ( end_time - begin_time > 2 ) {
            fprintf( stderr, "WTF: TopLevel create of %s took %.2f\n", 
                    expanded_path, end_time - begin_time );
        }
        if ( res != 0 ) return res;
    }

        // then the host dir
    if ( res == 0 ) {
        res = makeHostDir( expanded_path, hostname, mode ); 
    }
    return res;
}

// This must be in a mutex
// the mutex prevents multiple procs on the same node from trying to make
// the container at the same time.  In the normal no-error case, one will
// get in first and make it, then the others will get in, stat it and quit
// but the stats are networked.  instead using some static data structure
// to record that this container was created.  However, we will need to
// remove this record on unlink.
int Container::create( const char *expanded_path, const char *hostname,
        mode_t mode, int flags, int *extra_attempts ) 
{
    int res = 0;
    do {
        res = createHelper(expanded_path, hostname, mode,flags,extra_attempts);
        if ( res != 0 ) {
            int checkbystat = 0;
            if ( checkbystat ) {
                    // this check maybe slows us down?
                    // should be OK to just check the various errnos instead 
                    // of doing the stat
                string host_dir = getHostDirPath( expanded_path, hostname );
                if ( isContainer(expanded_path) && Util::isDirectory(hostname)){
                    // something failed locally but it must have succeeded 
                    // elsewhere.  Good enough.
                    res = 0;
                    break;
                } 
            } else if ( errno != EEXIST && errno != ENOENT && errno != EISDIR
                    && errno != ENOTEMPTY ) 
            {
                // if it's some other errno, than it's a real error so return it
                res = -errno;
                break;
            }
        }
        if ( res != 0 ) (*extra_attempts)++;
    } while( res && *extra_attempts <= 5 );

    if ( res == 0 ) {
    }
    return res;
}

// returns the first dirent that matches a prefix (or NULL)
struct dirent *Container::getnextent( DIR *dir, const char *prefix ) {
    if ( dir == NULL ) return NULL; // this line not necessary, but doesn't hurt
    struct dirent *next = NULL;
    do {
        next = readdir( dir );
    } while( next && strncmp( next->d_name, prefix, strlen(prefix) ) != 0 );
    return next;
}

// this function traverses a container and returns the next dropping
// it is shared by different parts of the code that want to traverse
// a container and fetch all the indexes or to traverse a container
// and fetch all the chunks
// this returns 0 if done.  1 if OK.  -errno if a problem
int Container::nextdropping( string physical_path, 
        string *droppingpath, const char *dropping_type,
        DIR **topdir, DIR **hostdir, struct dirent **topent ) 
{
        // open it on the initial 
    if ( *topdir == NULL ) {
        Util::Opendir( physical_path.c_str(), topdir );
        if ( *topdir == NULL ) return -errno;
    }

        // make sure topent is valid
    if ( *topent == NULL ) {
        *topent = getnextent( *topdir, HOSTDIRPREFIX );
        if ( *topent == NULL ) {
            // all done
            Util::Closedir( *topdir );
            *topdir = NULL;
            return 0;
        }
    }

        // set up the hostpath here.  We either need it in order to open hostdir
        // or we need it in order to populate the chunk
    string hostpath = physical_path;
    hostpath       += "/";
    hostpath       += (*topent)->d_name;

        // make sure hostdir is valid
    if ( *hostdir == NULL ) {
        Util::Opendir( hostpath.c_str(), hostdir );
        if ( *hostdir == NULL ) {
            fprintf(stderr,"opendir %s: %s\n",hostpath.c_str(),strerror(errno));
            return -errno;
        }
    }

        // get the next hostent, if null, reset topent and hostdir and try again
    struct dirent *hostent = getnextent( *hostdir, dropping_type );
    if ( hostent == NULL ) {
        Util::Closedir( *hostdir );
        *topent  = NULL;
        *hostdir = NULL;
        return nextdropping( physical_path, droppingpath, dropping_type, 
                topdir, hostdir, topent );
    }

        // once we make it here, we have a hostent to an dropping 
    droppingpath->clear();
    droppingpath->assign( hostpath + "/" + hostent->d_name );
    return 1;
}

// returns 0 or -errno
int Container::Truncate( const char *path, off_t offset ) {
    int ret;
    string indexfile;

    DIR *td = NULL, *hd = NULL; struct dirent *tent = NULL;
    while((ret = nextdropping(path,&indexfile,INDEXPREFIX, &td,&hd,&tent))== 1){
        Index *index = new Index( path, SHARED_PID );
        ret = index->readIndex( indexfile );
        if ( ret == 0 ) {
            if ( index->lastOffset() > offset ) {
                index->truncate( offset );
                int fd = Util::Open( indexfile.c_str(), O_TRUNC );
                if ( fd < 0 ) {
                    cerr << "Couldn't overwrite index file " << indexfile
                         << ": " << strerror( fd ) << endl;
                    return -errno;
                }
                ret = index->rewriteIndex( fd );
                Util::Close( fd );
            }
        } else {
            cerr << "Failed to read index file " << indexfile 
                 << ": " << strerror( -ret ) << endl;
            break;
        }
    }
    // now remove all the meta droppings
    ret = Util::Opendir( getMetaDirPath( path ).c_str(), &td ); 
    if ( ret == 0 ) {
        while( ( tent = readdir( td ) ) != NULL ) {
            if ( strcmp( ".", tent->d_name ) && strcmp( "..", tent->d_name ) ) {
                string metadropping = getMetaDirPath( path );
                metadropping += "/"; metadropping += tent->d_name;
                cerr << "Need to remove meta dropping: " << metadropping << endl;
                Util::Unlink( metadropping.c_str() ); 
            }
        }
        Util::Closedir( td );
    }
    return ret;
}
