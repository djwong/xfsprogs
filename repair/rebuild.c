/*
 * Copyright (C) 2018 Oracle.  All Rights Reserved.
 *
 * Author: Darrick J. Wong <darrick.wong@oracle.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it would be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write the Free Software Foundation,
 * Inc.,  51 Franklin St, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#include <libxfs.h>
#include "btree.h"
#include "err_protos.h"
#include "libxlog.h"
#include "incore.h"
#include "globals.h"
#include "dinode.h"
#include "slab.h"
#include "rmap.h"

/* Borrowed routines from xfs_scrub.c */

struct xfs_repair_bmap_extent {
	struct xfs_rmap_irec		rmap;
	xfs_agnumber_t			agno;
};

struct xfs_repair_bmap {
	struct xfs_slab			*extslab;
	xfs_ino_t			ino;
	xfs_rfsblock_t			bmbt_blocks;
	int				whichfork;
};

/* Record extents that belong to this inode's fork. */
STATIC int
xfs_repair_bmap_extent_fn(
	struct xfs_btree_cur		*cur,
	struct xfs_rmap_irec		*rec,
	void				*priv)
{
	struct xfs_repair_bmap		*rb = priv;
	struct xfs_repair_bmap_extent	rbe;

	/* Skip extents which are not owned by this inode and fork. */
	if (rec->rm_owner != rb->ino)
		return 0;
	else if (rb->whichfork == XFS_DATA_FORK &&
		 (rec->rm_flags & XFS_RMAP_ATTR_FORK))
		return 0;
	else if (rb->whichfork == XFS_ATTR_FORK &&
		 !(rec->rm_flags & XFS_RMAP_ATTR_FORK))
		return 0;
	else if (rec->rm_flags & XFS_RMAP_BMBT_BLOCK) {
		rb->bmbt_blocks += rec->rm_blockcount;
		return 0;
	}

	rbe.rmap = *rec;
	rbe.agno = cur->bc_private.a.agno;
	return slab_add(rb->extslab, &rbe);
}

/* Compare two bmap extents. */
static int
xfs_repair_bmap_extent_cmp(
	const void				*a,
	const void				*b)
{
	const struct xfs_repair_bmap_extent	*ap = a;
	const struct xfs_repair_bmap_extent	*bp = b;

	if (ap->rmap.rm_offset > bp->rmap.rm_offset)
		return 1;
	else if (ap->rmap.rm_offset < bp->rmap.rm_offset)
		return -1;
	return 0;
}

/* Repair an inode fork. */
STATIC int
xfs_repair_bmap(
	struct xfs_inode		*ip,
	struct xfs_trans		**tpp,
	int				whichfork)
{
	struct xfs_repair_bmap		rb = {0};
	struct xfs_bmbt_irec		bmap;
	struct xfs_defer_ops		dfops;
	struct xfs_mount		*mp = ip->i_mount;
	struct xfs_buf			*agf_bp = NULL;
	struct xfs_repair_bmap_extent	*rbe;
	struct xfs_btree_cur		*cur;
	struct xfs_slab_cursor		*scur = NULL;
	xfs_fsblock_t			firstfsb;
	xfs_agnumber_t			agno;
	xfs_extlen_t			extlen;
	int				baseflags;
	int				flags;
	int				error = 0;

	ASSERT(whichfork == XFS_DATA_FORK || whichfork == XFS_ATTR_FORK);

	/* Don't know how to repair the other fork formats. */
	if (XFS_IFORK_FORMAT(ip, whichfork) != XFS_DINODE_FMT_EXTENTS &&
	    XFS_IFORK_FORMAT(ip, whichfork) != XFS_DINODE_FMT_BTREE)
		return ENOTTY;

	/* Only files, symlinks, and directories get to have data forks. */
	if (whichfork == XFS_DATA_FORK && !S_ISREG(VFS_I(ip)->i_mode) &&
	    !S_ISDIR(VFS_I(ip)->i_mode) && !S_ISLNK(VFS_I(ip)->i_mode))
		return EINVAL;

	/* If we somehow have delalloc extents, forget it. */
	if (whichfork == XFS_DATA_FORK && ip->i_delayed_blks)
		return EBUSY;

	/* We require the rmapbt to rebuild anything. */
	if (!xfs_sb_version_hasrmapbt(&mp->m_sb))
		return EOPNOTSUPP;

	/* Don't know how to rebuild realtime data forks. */
	if (XFS_IS_REALTIME_INODE(ip) && whichfork == XFS_DATA_FORK)
		return EOPNOTSUPP;

	/* Collect all reverse mappings for this fork's extents. */
	init_slab(&rb.extslab, sizeof(*rbe));
	rb.ino = ip->i_ino;
	rb.whichfork = whichfork;
	for (agno = 0; agno < mp->m_sb.sb_agcount; agno++) {
		error = -libxfs_alloc_read_agf(mp, *tpp, agno, 0, &agf_bp);
		if (error)
			goto out;
		cur = libxfs_rmapbt_init_cursor(mp, *tpp, agf_bp, agno);
		error = -libxfs_rmap_query_all(cur, xfs_repair_bmap_extent_fn, &rb);
		libxfs_btree_del_cursor(cur, error ? XFS_BTREE_ERROR :
				XFS_BTREE_NOERROR);
		if (error)
			goto out;
	}

	/* Blow out the in-core fork and zero the on-disk fork. */
	libxfs_trans_ijoin(*tpp, ip, 0);
	if (XFS_IFORK_PTR(ip, whichfork) != NULL)
		libxfs_idestroy_fork(ip, whichfork);
	XFS_IFORK_FMT_SET(ip, whichfork, XFS_DINODE_FMT_EXTENTS);
	XFS_IFORK_NEXT_SET(ip, whichfork, 0);

	/* Reinitialize the on-disk fork. */
	if (whichfork == XFS_DATA_FORK) {
		memset(&ip->i_df, 0, sizeof(struct xfs_ifork));
		ip->i_df.if_flags |= XFS_IFEXTENTS;
	} else if (whichfork == XFS_ATTR_FORK) {
		if (slab_count(rb.extslab) == 0)
			ip->i_afp = NULL;
		else {
			ip->i_afp = kmem_zone_zalloc(xfs_ifork_zone, KM_NOFS);
			ip->i_afp->if_flags |= XFS_IFEXTENTS;
		}
	}
	libxfs_trans_log_inode(*tpp, ip, XFS_ILOG_CORE);
	error = -libxfs_trans_roll_inode(tpp, ip);
	if (error)
		goto out;

	baseflags = XFS_BMAPI_NORMAP;
	if (whichfork == XFS_ATTR_FORK)
		baseflags |= XFS_BMAPI_ATTRFORK;

	/* "Remap" the extents into the fork. */
	init_slab_cursor(rb.extslab, xfs_repair_bmap_extent_cmp, &scur);
	rbe = pop_slab_cursor(scur);
	while (rbe != NULL) {
		/* Form the "new" mapping... */
		bmap.br_startblock = XFS_AGB_TO_FSB(mp, rbe->agno,
				rbe->rmap.rm_startblock);
		bmap.br_startoff = rbe->rmap.rm_offset;
		flags = 0;
		if (rbe->rmap.rm_flags & XFS_RMAP_UNWRITTEN)
			flags = XFS_BMAPI_PREALLOC;
		while (rbe->rmap.rm_blockcount > 0) {
			libxfs_defer_init(&dfops, &firstfsb);
			extlen = min(rbe->rmap.rm_blockcount, MAXEXTLEN);
			bmap.br_blockcount = extlen;

			/* Drop the block counter... */
			ip->i_d.di_nblocks -= extlen;

			/* Re-add the extent to the fork. */
			error = -libxfs_bmapi_remap(*tpp, ip,
					bmap.br_startoff, extlen,
					bmap.br_startblock, &dfops,
					baseflags | flags);
			if (error)
				goto out;

			bmap.br_startblock += extlen;
			bmap.br_startoff += extlen;
			rbe->rmap.rm_blockcount -= extlen;
			error = -libxfs_defer_ijoin(&dfops, ip);
			if (error)
				goto out_cancel;
			error = -libxfs_defer_finish(tpp, &dfops);
			if (error)
				goto out;
			/* Make sure we roll the transaction. */
			error = -libxfs_trans_roll_inode(tpp, ip);
			if (error)
				goto out;
		}
		rbe = pop_slab_cursor(scur);
	}
	free_slab_cursor(&scur);
	free_slab(&rb.extslab);

	/* Decrease nblocks to reflect the freed bmbt blocks. */
	if (rb.bmbt_blocks) {
		ip->i_d.di_nblocks -= rb.bmbt_blocks;
		libxfs_trans_log_inode(*tpp, ip, XFS_ILOG_CORE);
		error = -libxfs_trans_roll_inode(tpp, ip);
		if (error)
			goto out;
	}

	return error;
out_cancel:
	libxfs_defer_cancel(&dfops);
out:
	if (scur)
		free_slab_cursor(&scur);
	if (rb.extslab)
		free_slab(&rb.extslab);
	return error;
}

/* Rebuild some inode's bmap. */
int
rebuild_bmap(
	struct xfs_mount	*mp,
	xfs_ino_t		ino,
	int			whichfork,
	unsigned long		nr_extents,
	struct xfs_buf		**ino_bpp,
	struct xfs_dinode	**dinop,
	int			*dirty)
{
	struct xfs_inode	*ip;
	struct xfs_trans	*tp;
	struct xfs_buf		*bp;
	unsigned long long	resblks;
	xfs_daddr_t		bp_bn;
	int			bp_length;
	int			error;

	bp_bn = (*ino_bpp)->b_bn;
	bp_length = (*ino_bpp)->b_length;

	resblks = libxfs_bmbt_calc_size(mp, nr_extents);
	error = -libxfs_trans_alloc(mp, &M_RES(mp)->tr_itruncate,
			resblks, 0, 0, &tp);
	if (error)
		return error;

	/*
	 * Repair magic: the caller thinks it owns the buffer that backs
	 * the inode.  The _iget call will want to grab the buffer to
	 * load the inode, so the buffer must be attached to the
	 * transaction.  Furthermore, the _iget call drops the buffer
	 * once the inode is loaded, so if we've made any changes we
	 * have to log those to the transaction so they get written...
	 */
	libxfs_trans_bjoin(tp, *ino_bpp);
	if (*dirty) {
		libxfs_trans_log_buf(tp, *ino_bpp, 0, XFS_BUF_SIZE(*ino_bpp));
		*dirty = 0;
	}

	/* ...then rebuild the bmbt... */
	error = -libxfs_iget(mp, tp, ino, 0, &ip, &xfs_default_ifork_ops);
	if (error)
		goto out_trans;
	error = xfs_repair_bmap(ip, &tp, whichfork);
	if (error)
		goto out_irele;

	/*
	 * ...and then regrab the same inode buffer so that we return to
	 * the caller with the inode buffer locked and the dino pointer
	 * up to date.  We bhold the buffer so that it doesn't get
	 * released during the transaction commit.
	 */
	error = -libxfs_imap_to_bp(mp, tp, &ip->i_imap, dinop, ino_bpp, 0, 0);
	if (error)
		goto out_irele;
	libxfs_trans_bhold(tp, *ino_bpp);
	error = -libxfs_trans_commit(tp);
	IRELE(ip);
	return error;
out_irele:
	IRELE(ip);
out_trans:
	libxfs_trans_cancel(tp);
	/* Try to regrab the old buffer so we don't lose it... */
	if (!libxfs_trans_read_buf(mp, NULL, mp->m_ddev_targp, bp_bn, bp_length,
			0, &bp, NULL))
		*ino_bpp = bp;
	return error;
}
