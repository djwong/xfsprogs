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
#include "libxfs_priv.h"
#include "xfs_fs.h"
#include "xfs_shared.h"
#include "xfs_format.h"
#include "xfs_log_format.h"
#include "xfs_trans_resv.h"
#include "xfs_bit.h"
#include "xfs_sb.h"
#include "xfs_mount.h"
#include "xfs_defer.h"
#include "xfs_inode.h"
#include "xfs_trans.h"
#include "xfs_alloc.h"
#include "xfs_btree.h"
#include "xfs_rtrmap_btree.h"
#include "xfs_trace.h"
#include "xfs_cksum.h"
#include "xfs_ag_resv.h"

/*
 * Realtime Reverse map btree.
 *
 * This is a per-ag tree used to track the owner(s) of a given extent
 * in the realtime device.  See the comments in xfs_rmap_btree.c for
 * more information.
 *
 * This tree is basically the same as the regular rmap btree except that
 * it doesn't live in free space, and the startblock and blockcount
 * fields have been widened to 64 bits.
 */

static struct xfs_btree_cur *
xfs_rtrmapbt_dup_cursor(
	struct xfs_btree_cur	*cur)
{
	struct xfs_btree_cur	*new;

	new = xfs_rtrmapbt_init_cursor(cur->bc_mp, cur->bc_tp,
			cur->bc_private.b.ip);

	/*
	 * Copy the firstblock, dfops, and flags values,
	 * since init cursor doesn't get them.
	 */
	new->bc_private.b.firstblock = cur->bc_private.b.firstblock;
	new->bc_private.b.dfops = cur->bc_private.b.dfops;
	new->bc_private.b.flags = cur->bc_private.b.flags;

	return new;
}

static void *
xfs_rtrmapbt_verify(
	struct xfs_buf		*bp)
{
	struct xfs_mount	*mp = bp->b_target->bt_mount;
	struct xfs_btree_block	*block = XFS_BUF_TO_BLOCK(bp);
	void			*failed_at;
	int			level;

	if (block->bb_magic != cpu_to_be32(XFS_RTRMAP_CRC_MAGIC))
		return __this_address;

	if (!xfs_sb_version_hasrmapbt(&mp->m_sb))
		return __this_address;
	if ((failed_at = xfs_btree_lblock_v5hdr_verify(bp,
			XFS_RMAP_OWN_UNKNOWN)))
		return failed_at;
	level = be16_to_cpu(block->bb_level);
	if (level > mp->m_rtrmap_maxlevels)
		return __this_address;

	return xfs_btree_lblock_verify(bp, mp->m_rtrmap_mxr[level != 0]);
}

static void
xfs_rtrmapbt_read_verify(
	struct xfs_buf	*bp)
{
	xfs_failaddr_t	fa;

	if (!xfs_btree_lblock_verify_crc(bp))
		xfs_verifier_error(bp, -EFSBADCRC, __this_address);
	else {
		fa = xfs_rtrmapbt_verify(bp);
		if (fa)
			xfs_verifier_error(bp, -EFSCORRUPTED, fa);
	}

	if (bp->b_error)
		trace_xfs_btree_corrupt(bp, _RET_IP_);
}

static void
xfs_rtrmapbt_write_verify(
	struct xfs_buf	*bp)
{
	xfs_failaddr_t	fa;

	fa = xfs_rtrmapbt_verify(bp);
	if (fa) {
		trace_xfs_btree_corrupt(bp, _RET_IP_);
		xfs_verifier_error(bp, -EFSCORRUPTED, fa);
		return;
	}
	xfs_btree_lblock_calc_crc(bp);

}

const struct xfs_buf_ops xfs_rtrmapbt_buf_ops = {
	.name			= "xfs_rtrmapbt",
	.verify_read		= xfs_rtrmapbt_read_verify,
	.verify_write		= xfs_rtrmapbt_write_verify,
	.verify_struct		= xfs_rtrmapbt_verify,
};

static const struct xfs_btree_ops xfs_rtrmapbt_ops = {
	.rec_len		= sizeof(struct xfs_rtrmap_rec),
	.key_len		= 2 * sizeof(struct xfs_rtrmap_key),

	.dup_cursor		= xfs_rtrmapbt_dup_cursor,
	.buf_ops		= &xfs_rtrmapbt_buf_ops,
};

/*
 * Allocate a new allocation btree cursor.
 */
struct xfs_btree_cur *
xfs_rtrmapbt_init_cursor(
	struct xfs_mount	*mp,
	struct xfs_trans	*tp,
	struct xfs_inode	*ip)
{
	struct xfs_ifork	*ifp = XFS_IFORK_PTR(ip, XFS_DATA_FORK);
	struct xfs_btree_cur	*cur;

	cur = kmem_zone_zalloc(xfs_btree_cur_zone, KM_NOFS);
	cur->bc_tp = tp;
	cur->bc_mp = mp;
	cur->bc_btnum = XFS_BTNUM_RTRMAP;
	cur->bc_flags = XFS_BTREE_LONG_PTRS | XFS_BTREE_ROOT_IN_INODE |
			XFS_BTREE_CRC_BLOCKS | XFS_BTREE_IROOT_RECORDS |
			XFS_BTREE_OVERLAPPING;
	cur->bc_blocklog = mp->m_sb.sb_blocklog;
	cur->bc_ops = &xfs_rtrmapbt_ops;
	cur->bc_nlevels = be16_to_cpu(ifp->if_broot->bb_level) + 1;
	cur->bc_statoff = XFS_STATS_CALC_INDEX(xs_rmap_2);

	cur->bc_private.b.forksize = XFS_IFORK_SIZE(ip, XFS_DATA_FORK);
	cur->bc_private.b.ip = ip;
	cur->bc_private.b.firstblock = NULLFSBLOCK;
	cur->bc_private.b.dfops = NULL;
	cur->bc_private.b.allocated = 0;
	cur->bc_private.b.flags = 0;
	cur->bc_private.b.whichfork = XFS_DATA_FORK;

	return cur;
}

/*
 * Calculate number of records in an rmap btree block.
 */
int
xfs_rtrmapbt_maxrecs(
	struct xfs_mount	*mp,
	int			blocklen,
	bool			leaf)
{
	blocklen -= XFS_RTRMAP_BLOCK_LEN;

	if (leaf)
		return blocklen / sizeof(struct xfs_rtrmap_rec);
	return blocklen /
		(2 * sizeof(struct xfs_rtrmap_key) + sizeof(xfs_rtrmap_ptr_t));
}

/* Compute the maximum height of an rmap btree. */
void
xfs_rtrmapbt_compute_maxlevels(
	struct xfs_mount		*mp)
{
	mp->m_rtrmap_maxlevels = xfs_btree_compute_maxlevels(mp,
			mp->m_rtrmap_mnr, mp->m_sb.sb_rblocks);
	ASSERT(mp->m_rtrmap_maxlevels <= XFS_BTREE_MAXLEVELS);
}
