#ifndef __PLFSINDEX_H__
#define __PLFSINDEX_H__

/**
 * Abstract class for data-index information.
 *
 * If a class is derived from this class and implements all its interfaces,
 * then the user could use the function 'plfs_reader()' to read the data
 * at the given position.
 */
class PLFSIndex {
public:
    virtual ~PLFSIndex() {};
    virtual void lock(const char *function) = 0;
    virtual void unlock(const char *function) = 0;
    virtual int getChunkFd( pid_t chunk_id ) = 0;
    virtual int setChunkFd( pid_t chunk_id, int fd ) = 0;
    virtual int globalLookup( int *fd, off_t *chunk_off, size_t *length,
                      string& path, bool *hole, pid_t *chunk_id,
                      off_t logical ) = 0;
};

/**
 * This function performs multi-threaded read.
 *
 * This function takes care of thread pool and open file cache. The only
 * thing you need to do is providing a class derived from PLFSIndex.
 */
ssize_t plfs_reader(void *unused, char *buf, size_t size,
                    off_t offset, PLFSIndex *index);

#endif
