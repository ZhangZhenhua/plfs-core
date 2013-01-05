#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include "FileWriter.hxx"
#include <Util.h>

FileWriter::FileWriter() {
    pthread_mutex_init(&mlock, NULL);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    fptr = NULL;
#else
    fd = -1;
#endif
    current_pos = 0;
}

FileWriter::~FileWriter() {
    pthread_mutex_destroy(&mlock);
}

int
FileWriter::open_file(const char *filename) {
    int ret = 0;
#ifdef SMALLFILE_USE_LIBC_FILEIO
    int fd = -1; // For debug messages only.
#endif

    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    if (fptr == NULL) {
        fptr = fopen(filename, "a");
        if (fptr) fd = fileno(fptr);
#else
    if (fd == -1) { // Physical file shouldn't be opened twice or more.
        fd = Util::Open(filename, O_CREAT | O_EXCL | O_WRONLY, DEFAULT_FMODE);
#endif
        if (fd < 0) {
            mlog(SMF_ERR, "Can't open file:%s for write, errno = %d.",
                 filename, errno);
            ret = -errno;
        }
    }
    Util::MutexUnlock(&mlock, __FUNCTION__);
    mlog(SMF_DAPI, "Open file %s:%d return %d.", filename, fd, ret);
    return ret;
}

int
FileWriter::append(const void *buf, size_t length, off_t *physical_offset) {
    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    assert(fptr);
    int fd = fileno(fptr); // For debug messages only.
#else
    assert(fd >= 0);
#endif
    if (physical_offset) *physical_offset = current_pos;
    while (length > 0) {
#ifdef SMALLFILE_USE_LIBC_FILEIO
        ssize_t written = fwrite(buf, 1, length, fptr);
        if (ferror(fptr)) written = -1;
#else
        ssize_t written = Util::Write(fd, buf, length);
#endif
        if (written < 0) {
            mlog(SMF_ERR, "Failed to write data to %d, errno = %d.",
                 fd, errno);
            break;
        } else if (written == 0) {
            mlog(SMF_WARN, "Write zero bytes to %d, retrying.", fd);
            continue;
        }
        current_pos += written;
        length -= written;
    }
    Util::MutexUnlock(&mlock, __FUNCTION__);
    mlog(SMF_DAPI, "Append data to %d.", fd);
    return (length == 0) ? 0 : -1;
}

int
FileWriter::sync() {
    int ret = 0;
    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    int fd = -1; // For debug messages only.
    if (fptr) {
        fd = fileno(fptr);
        ret = fflush(fptr);
        if (ret != 0) ret = -errno;
    }
#else
    if (fd >= 0) ret = Util::Fsync(fd);
#endif
    Util::MutexUnlock(&mlock, __FUNCTION__);
    if (fd >= 0) mlog(SMF_DAPI, "File %d is synced to disk.", fd);
    return ret;
}

int
FileWriter::close_file() {
    int fd_closed = -1; // For debug messages only.
    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    if (fptr != NULL) {
        fd_closed = fileno(fptr);
        fclose(fptr);
        fptr = NULL;
        current_pos = 0;
    }
#else
    if (fd >= 0) {
        fd_closed = fd;
        Util::Close(fd);
        fd = -1;
        current_pos = 0;
    }
#endif
    Util::MutexUnlock(&mlock, __FUNCTION__);
    if (fd_closed >= 0) mlog(SMF_DAPI, "File %d is closed.", fd_closed);
    return 0;
}

bool
FileWriter::is_opened() {
    bool retval;
    Util::MutexLock(&mlock, __FUNCTION__);
#ifdef SMALLFILE_USE_LIBC_FILEIO
    retval = !(fptr == NULL);
#else
    retval = !(fd == -1);
#endif
    Util::MutexUnlock(&mlock, __FUNCTION__);
    return retval;
}
