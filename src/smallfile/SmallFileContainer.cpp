#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <vector>
#include <string>
#include <set>
#include <algorithm>
#include <iostream>
#include <Util.h>
#include "MinimumHeap.hxx"
#include "SmallFileLayout.h"
#include "SmallFileContainer.hxx"
#include "SmallFileIndex.hxx"

using namespace std;

SmallFileContainer::SmallFileContainer(void *init_para) : index_cache(16)
{
    PathExpandInfo *expinfo = (PathExpandInfo *)init_para;
    vector<string>::iterator itr;

    pmount = expinfo->pmount;
    dirpath = expinfo->dirpath;
    /* Sanity check, whether the directory is complete? */
    for (itr = pmount->backends.begin();
         itr != pmount->backends.end();
         itr++)
    {
        string statfile;
        int ret;
        get_statfile(*itr, dirpath, statfile);
        if (Util::Access(statfile.c_str(), F_OK) != 0) {
            ret = makeTopLevelDir(*itr, dirpath, statfile);
            if (ret) mlog(SMF_ERR, "Failed to create SMFContainer:%d.", ret);
        }
    }
    pthread_rwlock_init(&writers_lock, NULL);
    pthread_mutex_init(&chunk_lock, NULL);
}

SmallFileContainer::~SmallFileContainer() {
    clear_chunk_cache();
    pthread_rwlock_destroy(&writers_lock);
    pthread_mutex_destroy(&chunk_lock);
}

int
SmallFileContainer::makeTopLevelDir(const string &backend,
                                    const string &dirpath,
                                    const string &statfile)
{
    int ret;
    string cdirpath = backend + DIR_SEPERATOR + dirpath + DIR_SEPERATOR
        + SMALLFILE_CONTAINER_NAME;

    ret = Util::Mkdir(cdirpath.c_str(), DEFAULT_DIR_MODE);
    if (!ret || errno == EEXIST)
        ret = Util::Creat(statfile.c_str(), DEFAULT_FMODE);
    return ret;
}

int
SmallFileContainer::init_data_source(void *resource, RecordReader **reader) {
    set<string> dir_contents;
    vector<string>::iterator itr;
    ReaddirOp op(NULL, &dir_contents, true, true);

    // Read all dropping.name.x.
    op.filter(NAME_PREFIX);
    for (itr = pmount->backends.begin();
         itr != pmount->backends.end();
         itr++)
    {
        int ret;
        string container_dir(*itr + DIR_SEPERATOR + dirpath + DIR_SEPERATOR +
                             SMALLFILE_CONTAINER_NAME);
        ret = op.do_op(container_dir.c_str(), DT_DIR);
        if (ret && ret != -ENOENT) return ret;
    }
    copy(dir_contents.begin(), dir_contents.end(),
         back_inserter(droppings_names));
    *reader = new EmptyRecordReader();
    return 0;
}

int
SmallFileContainer::merge_object(void *object, void *meta) {
    const char *filename = (const char *)object;
    ssize_t *did = (ssize_t *)meta;

    if (did) *did = droppings_names.size();
    droppings_names.push_back(filename);
    return 0;
}

WriterPtr
SmallFileContainer::get_writer(pid_t pid) {
    WriterPtr retval;
    map<pid_t, WriterPtr>::iterator itr;
    string namefile;

    pthread_rwlock_wrlock(&writers_lock);
    itr = writers.find(pid);
    if (itr != writers.end()) {
        retval = itr->second;
        pthread_rwlock_unlock(&writers_lock);
        return retval;
    }
    if (writers.size() >= pmount->max_writers) {
        // If there are too many writers already, we borrow one from another
        // process instead of creating a new one ourselves.
        itr = writers.begin();
        for (int choosen = pid % writers.size(); choosen > 0; choosen--,itr++);
        assert(itr != writers.end());
        writers[pid] = itr->second;
        retval = itr->second;
        pthread_rwlock_unlock(&writers_lock);
        return retval;
    }
    string aggregated_dir = pmount->backends[pid % pmount->backends.size()];
    aggregated_dir += DIR_SEPERATOR + dirpath;
    aggregated_dir += DIR_SEPERATOR SMALLFILE_CONTAINER_NAME;
    generate_dropping_name(aggregated_dir, pid, namefile);
    if (require(MEMCACHE_FULLYLOADED, this) == 0) {
        /*
         * We must get the list of the dropping.name.x files before adding
         * a new writer, otherwise the writer will get a wrong dropping_id.
         */
        ssize_t did = -1;
        release(MEMCACHE_FULLYLOADED, this);
        update((void *)namefile.c_str(), &did);
        assert(did != -1); // Make sure we get the right dropping id.
        retval.reset(new SMF_Writer(namefile, did));
        writers[pid] = retval;
    }
    pthread_rwlock_unlock(&writers_lock);
    return retval;
}

bool
SmallFileContainer::file_exist(const string &filename) {
    bool exist = false;
    int ret;

#ifdef SMALLFILE_USE_LIBC_FILEIO
    sync_writers(WRITER_SYNC_NAMEFILE);
#endif
    ret = require(MEMCACHE_FULLYLOADED, this);
    if (!ret) {
        ret = files.require(MEMCACHE_FULLYLOADED, &droppings_names);
        if (!ret) {
            FileMetaDataPtr metadata;
            metadata = files.get_metadata(filename);
            if (metadata) exist = true;
            files.release(MEMCACHE_FULLYLOADED, &droppings_names);
        }
        release(MEMCACHE_FULLYLOADED, this);
    }
    return exist;
}

int
SmallFileContainer::readdir(set<string> *res) {
#ifdef SMALLFILE_USE_LIBC_FILEIO
    sync_writers(WRITER_SYNC_NAMEFILE);
#endif
    int ret = require(MEMCACHE_FULLYLOADED, this);
    if (ret) return ret;
    ret = files.read_names(res, &droppings_names);
    release(MEMCACHE_FULLYLOADED, this);
    return ret;
}

IndexPtr
SmallFileContainer::get_index(const string &filename) {
    IndexPtr retval;
    FileMetaDataPtr metadata;
    int ret;

#ifdef SMALLFILE_USE_LIBC_FILEIO
    sync_writers(WRITER_SYNC_INDEXFILE);
#endif
    ret = require(MEMCACHE_FULLYLOADED, this);
    if (!ret) {
        ret = files.require(MEMCACHE_FULLYLOADED, &droppings_names);
        if (!ret) {
            metadata = files.get_metadata(filename);
            if (metadata) {
                struct index_init_para_t init_para;
                bool created;
                init_para.namefiles = &droppings_names;
                init_para.fids = &metadata->index_mapping;
                retval = index_cache.insert(filename, &init_para, created);
            }
            files.release(MEMCACHE_FULLYLOADED, &droppings_names);
        } else {
            mlog(SMF_ERR, "Can't build names mapping! ret = %d.", ret);
        }
        release(MEMCACHE_FULLYLOADED, this);
    } else {
        mlog(SMF_ERR, "Can't get the list of name files. ret = %d.", ret);
    }
    return retval;
}

int
SmallFileContainer::create(const string &filename, pid_t pid) {
    int ret;
    WriterPtr writer = get_writer(pid);

    ret = writer->create(filename, &files);
    mlog(SMF_DAPI, "Create a new file %s. ret = %d.", filename.c_str(), ret);
    return ret;
}

int
SmallFileContainer::rename(const string &from, const string &to, pid_t pid) {
    int ret;
    WriterPtr writer = get_writer(pid);

    ret = writer->rename(from, to, &files);
    mlog(SMF_DAPI, "Rename %s to %s, ret = %d.",
         from.c_str(), to.c_str(), ret);
    return ret;
}

int
SmallFileContainer::remove(const string &filename, pid_t pid) {
    int ret;
    WriterPtr writer = get_writer(pid);

    ret = writer->remove(filename, &files);
    mlog(SMF_DAPI, "Remove %s, ret = %d.", filename.c_str(), ret);
    return ret;
}

ssize_t
SmallFileContainer::write(const string &filename, const void *buf,
                          off_t offset, size_t count, pid_t pid) {
    WriterPtr writer = get_writer(pid);
    IndexPtr indexptr = index_cache.lookup(filename);
    ssize_t ret;

    mlog(SMF_DAPI, "Write %lu@%lu to %s from pid-%lu.",
         (unsigned long)count, (unsigned long)offset,
         filename.c_str(), (unsigned long)pid);
    ret = writer->write(filename, buf, offset, count, &files, indexptr.get());
    if (ret > 0) files.expand_filesize(filename, offset+ret);
    return ret;
}

int
SmallFileContainer::truncate(const string &filename, off_t offset, pid_t pid) {
    WriterPtr writer = get_writer(pid);
    IndexPtr indexptr = index_cache.lookup(filename);
    int ret;

    ret = writer->truncate(filename, offset, &files, indexptr.get());
    if (ret == 0) files.truncate_file(filename, offset);
    return ret;
}

int
SmallFileContainer::utime(const string &filename, struct utimbuf *ut,
                          pid_t pid)
{
    int ret;
    WriterPtr writer = get_writer(pid);

    ret = writer->utime(filename, ut, &files);
    mlog(SMF_DAPI, "Utime %s, ret = %d.", filename.c_str(), ret);
    return ret;
}

int
SmallFileContainer::sync_writers(int sync_level) {
    int ret = 0;
    map<pid_t, WriterPtr>::iterator itr;
    pthread_rwlock_rdlock(&writers_lock);
    for (itr = writers.begin(); itr != writers.end(); itr++) {
        ret = itr->second->sync(sync_level);
        if (ret) break;
    }
    pthread_rwlock_unlock(&writers_lock);
    return ret;
}

/**
 * Delete all dropping files and the container directory.
 *
 * It might be called by rmdir(). It only performs actual deletion when the
 * directory is empty.
 */

int
SmallFileContainer::delete_if_empty() {
    vector<string> &backends = pmount->backends;
    struct stat statbuf;
    struct dirent *dirp;
    DIR *dp;
    vector<string>::const_iterator itr;

    mlog(SMF_INFO, "All dropping files are about to be deleted for %s.",
         dirpath.c_str());
    // Hold the writers_lock, so that nobody can make changes to this.
    pthread_rwlock_wrlock(&writers_lock);
    writers.clear();
    for (itr = backends.begin(); itr != backends.end(); itr++) {
        string directory(*itr + DIR_SEPERATOR + dirpath
                         + DIR_SEPERATOR + SMALLFILE_CONTAINER_NAME);
        if (Util::Lstat(directory.c_str(), &statbuf) < 0) {
            if (errno == ENOENT) {
                mlog(SMF_INFO,"Small File Container not found in %s, skip it.",
                     directory.c_str());
                continue;
            }
            mlog(SMF_ERR, "Can't stat small file container in %s, errno = %d",
                 directory.c_str(), errno);
            pthread_rwlock_unlock(&writers_lock);
            return -errno;
        }
        if (!S_ISDIR(statbuf.st_mode)) {
            mlog(SMF_EMERG, "Small File Container is not a directory! Someone "
                 "plays with the backend file-system:%s?", directory.c_str());
            pthread_rwlock_unlock(&writers_lock);
            return -EFAULT;
        }
        if (Util::Opendir(directory.c_str(), &dp)) {
            mlog(SMF_ERR, "Failed to open directory %s, errno = %d.",
                 directory.c_str(), errno);
            pthread_rwlock_unlock(&writers_lock);
            return -errno;
        }
        while (Util::Readdir(dp, &dirp) == 0) {
            if (strcmp(dirp->d_name, ".") == 0 ||
                strcmp(dirp->d_name, "..") == 0)
                continue;
            string filename(directory + DIR_SEPERATOR + dirp->d_name);
            Util::Unlink(filename.c_str());
        }
        Util::Closedir(dp);
        if (Util::Rmdir(directory.c_str())) {
            pthread_rwlock_unlock(&writers_lock);
            return -errno;
        }
    }
    pthread_rwlock_unlock(&writers_lock);
    return 0;
}

void
SmallFileContainer::get_data_file(ssize_t did, string &pathname) {
    int ret;

    ret = require(MEMCACHE_FULLYLOADED, this);
    if (!ret) dropping_name2data(droppings_names[did], pathname);
    release(MEMCACHE_FULLYLOADED, this);
}

void
SmallFileContainer::clear_chunk_cache() {
    map<pid_t, int>::iterator itr;

    for (itr = chunk_map.begin(); itr != chunk_map.end(); itr++) {
        Util::Close(itr->second);
    }
    chunk_map.clear();
}
