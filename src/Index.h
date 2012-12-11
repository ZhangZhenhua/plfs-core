#ifndef __Index_H__
#define __Index_H__

#include "COPYRIGHT.h"
#include <set>
#include <map>
#include <vector>
#include <list>
using namespace std;

#include "plfs.h"
#include "Util.h"
#include "Metadata.h"


enum IndexEntryType {
    BYTERANGE     = 0,  // SingleByteRangeEntry class
    SIMPLEFORMULA = 1,  // SimpleFormulaEntry class
};

#define CON_ENTRY_ITR map<off_t,SingleByteRangeInMemEntry>::iterator
#define INDEX_FD_ITR map<IndexEntryType, int>::iterator

// the LocalEntry (SingleByteRangeEntry) and the SingleByteRangeInMemEntry
// should maybe be derived from one another. there are two types of index files
// on a write, every host has a host index
// on a read, all the index files get aggregated into one container index

class IndexFileInfo
{
    public:
        IndexFileInfo();
        void *listToStream(vector<IndexFileInfo> &list,int *bytes);
        vector<IndexFileInfo> streamToList(void *addr);
        //bool operator<(IndexFileInfo d1);
        double timestamp;
        string hostname;
        string path;  // add this to support deep metalink
                      // The deep metalink entries may point to a subdir
                      // in shadow backend
        IndexEntryType type;
        pid_t  id;
};

// this is a pure virtual class
// base class for all kinds of index entries,
// newly added index type must inherit it
class IndexEntry
{
    public:
        virtual ~IndexEntry() { return; };
        virtual bool overlap( const IndexEntry& ) { return false; };
        virtual bool contains ( off_t ) const  = 0;
        virtual bool splittable ( off_t ) const  = 0;
        virtual bool abut   ( const IndexEntry& ) { return false; };
        virtual off_t logical_tail( ) const  = 0;
        virtual bool follows(const IndexEntry&) { return false; };
        virtual bool preceeds(const IndexEntry&) { return false; };

    protected:
        double begin_timestamp;
        double end_timestamp;
};

// this is the class that represents the records that get written into the
// index file for each host.
class SingleByteRangeEntry : public IndexEntry
{
    public:
        SingleByteRangeEntry();
        SingleByteRangeEntry( off_t o, size_t s, pid_t p );
        SingleByteRangeEntry( const SingleByteRangeEntry& copy );
        bool overlap( const SingleByteRangeEntry& );
        bool contains ( off_t ) const;
        bool splittable ( off_t ) const;
        bool abut   ( const SingleByteRangeEntry& );
        off_t logical_tail( ) const;
        bool follows(const SingleByteRangeEntry&);
        bool preceeds(const SingleByteRangeEntry&);

    protected:
        off_t  logical_offset;
        off_t  physical_offset;  // I tried so hard to not put this in here
        // to save some bytes in the index entries
        // on disk.  But truncate breaks it all.
        // we assume that each write makes one entry
        // in the data file and one entry in the index
        // file.  But when we remove entries and
        // rewrite the index, then we break this
        // assumption.  blech.
        size_t length;
        pid_t  id;      // needs to be last so no padding

        friend class Index;
};


// this is the class that represents one record in the in-memory
// data structure that is
// the index for the entire container (the aggregation of the multiple host
// index files).
// this in-memory structure is used to answer read's by finding the appropriate
// requested logical offset within one of the physical host index files
class SingleByteRangeInMemEntry : SingleByteRangeEntry
{
    public:
        bool mergable( const SingleByteRangeInMemEntry& );
        bool abut( const SingleByteRangeInMemEntry& );
        bool follows( const SingleByteRangeInMemEntry& );
        bool preceeds( const SingleByteRangeInMemEntry& );
        SingleByteRangeInMemEntry split(off_t); //split in half,
                                                //this is back, return front

    protected:
        pid_t original_chunk;   // we just need to track this so we can
        // rewrite the index appropriately for
        // things like truncate to the middle or
        // for the as-yet-unwritten index flattening

        friend ostream& operator <<(ostream&,const SingleByteRangeInMemEntry&);

        friend class Index;
};

// this is the class that represents the records that get written into the
// index file for each host.
class SimpleFormulaEntry : public IndexEntry
{
    public:
        SimpleFormulaEntry();
        SimpleFormulaEntry( const SimpleFormulaEntry& copy );
        bool overlap( const SimpleFormulaEntry& );
        bool overlap( off_t offset, size_t length, off_t &lo,
                      off_t &po, size_t &s , pid_t &pid);
        bool contains ( off_t ) const;
        bool splittable ( off_t ) const;
        bool abut   ( const SimpleFormulaEntry& );
        off_t logical_tail( ) const;
        bool follows(const SimpleFormulaEntry&);
        bool preceeds(const SimpleFormulaEntry&);

    protected:
        size_t nw;  // number of writers
        size_t write_size; // the length for each write
        size_t numWrites; // number of writes fit in this pattern
        off_t  logical_start_offset;
        off_t  logical_end_offset; // range of this simple formula
        off_t  last_offset; // last_offset is set to logical_end_offset when
                            // formula is created. If comes a trunc operation
                            // later to truncate file size in middle of this
                            // formula, set last_offset to it.
                            // we can use a specailized formulaEntry to
                            // represent a truncate op, but we will have a
                            // really expensive getattr ( merge all index
                            // entries and walk through to get correct
                            // last_offset).
        double formulaTime;  // formula creation time
        plfs_io_pattern  strided; // STRIDED or SEGMENTED

        friend class Index;
};

// this is the class that represents one record in the in-memory
// data structure for simpleFormula entry
class SimpleFormulaInMemEntry : public SimpleFormulaEntry
{
   protected:
        double metalinkTime;
        int id;  // Use this interger to associate with a local file
                 // This will indicate the first ChunkFile location in
                 // chunk_map. if this entry contains m writers, then
                 // rank n (n<m) will use id+n to find its cf
                 
        friend ostream& operator <<(ostream&,const SimpleFormulaInMemEntry&);
        friend class Index;
};

// this is a way to associate a integer with a local file
// so that the aggregated index can just have an int to point
// to a local file instead of putting the full string in there
typedef struct {
    string path;
    int fd;
} ChunkFile;

class Index : public Metadata
{
    public:
        Index( string );
        ~Index();

        int readIndex( string hostindex );

        void setPath( string );

        bool ispopulated( );

        void addWrite( off_t offset, size_t bytes, pid_t, double, double );

        size_t memoryFootprintMBs();    // how much area the index is occupying

        int flush();
        int sync();

        off_t lastOffset( );

        void lock( const char *function );
        void unlock(  const char *function );

        int getFd( vector<int> &);

        int getFd( IndexEntryType eType) {
            return current_fd[eType];
        }
        void setCurrentFd( int fd, string indexpath );

        int resetPhysicalOffsets();

        size_t totalBytes( );

        int getChunkFd( pid_t chunk_id );

        int setChunkFd( pid_t chunk_id, int fd );

        int globalLookup( int *fd, off_t *chunk_off, size_t *length,
                          string& path, bool *hole, pid_t *chunk_id,
                          off_t logical );

        int insertGlobal( SingleByteRangeInMemEntry * );
        void merge( Index *other);
        void truncate( off_t offset );
        void extend(off_t offset, pid_t p, double ts);
        int rewriteIndex( int fd );
        void truncateHostIndex( off_t offset );

        void compress();
        int debug_from_stream(void *addr);
        int global_to_file(int fd);
        int global_from_stream(void *addr);
        int global_to_stream(void **buffer,size_t *length);
        friend ostream& operator <<(ostream&,const Index&);
        // Added to get chunk path on write
        map< int, string> index_paths;
        void startBuffering();
        void stopBuffering();
        bool isBuffering();
        int addSimpleFormulaWrite(int nw, int size, plfs_io_pattern strided,
                                  off_t start,off_t end, off_t last, double ts,
                                  bool & extended);
        int updateSimpleFormula(double begin, double end);

    private:
        void init( string );
        int chunkFound( int *, off_t *, size_t *, off_t,
                        string&, pid_t *, double *,
                        SingleByteRangeInMemEntry * );
        int cleanupReadIndex(int, void *, off_t, int, const char *,
                             const char *);
        void *mapIndex( string, int *, off_t * );
        int handleOverlap( SingleByteRangeInMemEntry& g_entry,
                           pair< map<off_t,SingleByteRangeInMemEntry>::iterator,
                           bool > &insert_ret );
        map<off_t,SingleByteRangeInMemEntry>::iterator insertGlobalEntryHint(
            SingleByteRangeInMemEntry *g_entry ,
            map<off_t,SingleByteRangeInMemEntry>::iterator hint);
        pair<map<off_t,SingleByteRangeInMemEntry>::iterator,bool> 
        insertGlobalEntry(SingleByteRangeInMemEntry *g_entry);
        size_t splitEntry(SingleByteRangeInMemEntry *,set<off_t> &,
                          multimap<off_t,SingleByteRangeInMemEntry> &);
        void findSplits(SingleByteRangeInMemEntry&,set<off_t> &);
        // where we buffer the byte range index (i.e. write)
        vector< SingleByteRangeEntry > byteRangeIndex;

        // where we buffer the simple formula index
        vector< SimpleFormulaEntry > simpleFormulaIndex;

        // this is a global byteRange index made by aggregating multiple locals
        map< off_t, SingleByteRangeInMemEntry > global_byteRange_index;

        // a global simpleFormula index by reading in multiple locals
        map< double, SimpleFormulaInMemEntry > global_simpleFormula_index;

        // this is a way to associate a integer with a local file
        // so that the aggregated index can just have an int to point
        // to a local file instead of putting the full string in there
        vector< ChunkFile >       chunk_map;

        // need to remember the current offset position within each chunk
        map<pid_t,off_t> physical_offsets;

        // keep index file descriptor here, one for each index entry type
        map< IndexEntryType, int > current_fd;

        bool   populated;
        pid_t  mypid;
        string physical_path;
        int    chunk_id;
        off_t  last_offset;
        size_t total_bytes;
        bool buffering;    // are we buffering the index on write?
        bool buffer_filled; // were we buffering but ran out of space?
        pthread_mutex_t    fd_mux;   // to allow thread safety

        bool compress_contiguous; // set true for performance. 0 for tracing.

};

#endif
