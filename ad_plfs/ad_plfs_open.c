/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_open.c,v 1.18 2005/05/23 23:27:44 rross Exp $    
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

// don't worry about any hints yet
void ADIOI_PLFS_Open(ADIO_File fd, int *error_code)
{
    // I think perm is the mode and amode is the flags
    int err = 0, perm, amode, old_mask, rank;
    Plfs_fd *pfd = NULL;

    MPI_Comm_rank( fd->comm, &rank );
    static char myname[] = "ADIOI_PLFS_OPEN";
    fprintf( stderr, "%s: begin\n", myname );

    if (fd->perm == ADIO_PERM_NULL) {
        old_mask = umask(022);
        umask(old_mask);
        perm = old_mask ^ 0666;
    }
    else perm = fd->perm;

    amode = 0;//O_META;
    if (fd->access_mode & ADIO_RDONLY)
        amode = amode | O_RDONLY;
    if (fd->access_mode & ADIO_WRONLY)
        amode = amode | O_WRONLY;
    if (fd->access_mode & ADIO_RDWR)
        amode = amode | O_RDWR;
    if (fd->access_mode & ADIO_EXCL)
        amode = amode | O_EXCL;

    // MPI_File_open is a collective call so only create it once
    if (fd->access_mode & ADIO_CREATE) {
        // first create the top-level directory with just one proc
        if ( rank == 0 ) {
            err = plfs_create( fd->filename, perm, amode );
        }
        MPI_Bcast( &err, 1, MPI_INT, 0, fd->comm );

        // then create the individual hostdirs with one proc per node
        // this fd->hints->ranklist thing doesn't work
        /*
        if ( err == 0 && rank != 0 && rank == fd->hints->ranklist[0] ) {
            err = plfs_create( fd->filename, perm, amode );
        }
        MPI_Bcast( &err, 1, MPI_INT, 0, fd->comm );
        */
    }

    // handle any error from a create
    if ( err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
        fprintf( stderr, "%s: failure on create\n", myname );
        return;
    }

    // if we get here, it is time to open the file
    err = plfs_open( &pfd, fd->filename, amode, rank, perm );
    fd->fd_direct = -1;

    if ( err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
        fprintf( stderr, "%s: failure\n", myname );
    } else {
        fprintf( stderr, "%s: Success!\n", myname );
        fd->fs_ptr = pfd;
        *error_code = MPI_SUCCESS;
    }
}
