#ifndef __PLFS_H_
#define __PLFS_H_

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <utime.h>
#ifdef HAVE_SYS_STATVFS_H
    #include <sys/statvfs.h>
#endif

#ifdef __cplusplus 
    extern "C" 
    { 
    class Plfs_fd;
#else
    typedef void * Plfs_fd;
#endif


typedef enum {
    LOGICAL_PATH,
    PHYSICAL_PATH
}Plfs_path_type;


/*
   All PLFS function declarations in this file are in alphabetical order.
   Please retain this as edits are made.

   All PLFS functions are either approximations of POSIX file IO calls or
   utility functions.

   Most PLFS functions return 0 or -errno, except write and read which return
   the number of bytes or -errno

   Many of the utility functions are shared by the ADIO and the FUSE layers
   of PLFS.  Typical applications should try to use those layers.  However,
   it is also possible for an application to be ported to use the PLFS API
   directly.  In this case, at a minimum, the application can call
   plfs_open(), plfs_write(), plfs_read, plfs_close().

   This code does allow for multiple threads to share a single Plfs_fd ptr
   To add more threads to a Plfs_fd ptr, just call plfs_open multiple times.
   The first time call it with a NULL ptr, then subsequent times call it
   with the original ptr.  I'm not sure whether it's thread safe though or
   whether the caller takes care of that.  I believe each function should
   specify itself.
*/

/* is_plfs_file
    returns bool.  Also if mode_t * is not NULL, leaves it 0 if the path
    doesn't exist, or if it does exist, it fills it in with S_IFDIR etc
    This allows multiple possible return values: yes, it is a plfs file,
    no: it is a directory
    no: it is a normal flat file
    no: it is a symbolic link
    etc.
*/
int is_plfs_file( const char *path, mode_t * );

int plfs_access( const char *path, int mask );

const char * plfs_buildtime();

int plfs_chmod( const char *path, mode_t mode );

int plfs_chown( const char *path, uid_t, gid_t );

int plfs_close( Plfs_fd *, pid_t, int open_flags );

/* plfs_create
   you don't need to call this, you can also pass O_CREAT to plfs_open
*/
int plfs_create( const char *path, mode_t mode, int flags, pid_t pid ); 

void plfs_debug( const char *format, ... );

int plfs_dump_index( FILE *fp, const char *path, int compress );

int plfs_flatten_index( Plfs_fd *, const char *path, Plfs_path_type path_type);

/* Plfs_fd can be NULL */
int plfs_getattr( Plfs_fd *, const char *path, struct stat *stbuf );

int plfs_index_stream(Plfs_fd **pfd, char ** buffer);

int plfs_link( const char *path, const char *to );
/* 
   query the mode that was used to create the file
   this should only be called on a plfs file
*/
int plfs_mode( const char *path, mode_t *mode );

int plfs_mkdir( const char *path, mode_t );

/* plfs_open
*/
int plfs_open( Plfs_fd **, const char *path, 
        int flags, pid_t pid, mode_t , char *index_stream=NULL);

/* query a plfs_fd about how many writers and readers are using it */
int plfs_query( Plfs_fd *, size_t *writers, size_t *readers );

ssize_t plfs_read( Plfs_fd *, char *buf, size_t size, off_t offset );

/* plfs_readdir
 * the void * needs to be a pointer to a vector<string> but void * is
 * used here so it compiles with C code
 */
int plfs_readdir( const char *path, void * ); 

int plfs_readlink( const char *path, char *buf, size_t bufsize );

int plfs_rename( const char *from, const char *to );

int plfs_rmdir( const char *path );

void plfs_serious_error(const char *msg,pid_t pid );
/*
   a funtion to get stats back from plfs operations
   the void * needs to be a pointer to an STL string but void * is used here
   so it compiles with C code
*/
void plfs_stats( void * );

int plfs_statvfs( const char *path, struct statvfs *stbuf );

int plfs_symlink( const char *path, const char *to );

/* individual writers can be sync'd.  */
int plfs_sync( Plfs_fd *, pid_t );

/* Plfs_fd can be NULL, but then path must be valid */
int plfs_trunc( Plfs_fd *, const char *path, off_t );

int plfs_unlink( const char *path );

int plfs_utime( const char *path, struct utimbuf *ut );

const char * plfs_version();

ssize_t plfs_write( Plfs_fd *, const char *, size_t, off_t, pid_t );

double plfs_wtime();

#ifdef __cplusplus 
    }
#endif

#endif
