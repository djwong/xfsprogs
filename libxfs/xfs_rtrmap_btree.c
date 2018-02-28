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
#include "xfs_rmap.h"
#include "xfs_rtrmap_btree.h"
#include "xfs_trace.h"
#include "xfs_cksum.h"
#include "xfs_ag_resv.h"
#include "xfs_bmap.h"

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

STATIC int
xfs_rtrmapbt_alloc_block(
	struct xfs_btree_cur	*cur,
	union xfs_btree_ptr	*start,
	union xfs_btree_ptr	*new,
	int			*stat)
{
	struct xfs_alloc_arg	args;
	int			error;

	memset(&args, 0, sizeof(args));
	args.tp = cur->bc_tp;
	args.mp = cur->bc_mp;
	args.fsbno = cur->bc_private.b.firstblock;
	args.firstblock = args.fsbno;
	xfs_rmap_ino_bmbt_owner(&args.oinfo, cur->bc_private.b.ip->i_ino,
			cur->bc_private.b.whichfork);

	if (args.fsbno == NULLFSBLOCK) {
		args.fsbno = be64_to_cpu(start->l);
		args.type = XFS_ALLOCTYPE_START_BNO;
		/*
		 * Make sure there is sufficient room left in the AG to
		 * complete a full tree split for an extent insert.  If
		 * we are converting the middle part of an extent then
		 * we may need space for two tree splits.
		 *
		 * We are relying on the caller to make the correct block
		 * reservation for this operation to succeed.  If the
		 * reservation amount is insufficient then we may fail a
		 * block allocation here and corrupt the filesystem.
		 */
		args.minleft = args.tp->t_blk_res;
	} else if (cur->bc_private.b.dfops->dop_low) {
		args.type = XFS_ALLOCTYPE_START_BNO;
	} else {
		args.type = XFS_ALLOCTYPE_NEAR_BNO;
	}

	args.minlen = args.maxlen = args.prod = 1;
	args.wasdel = 0;
	error = xfs_alloc_vextent(&args);
	if (error)
		goto error0;

	if (args.fsbno == NULLFSBLOCK && args.minleft) {
		/*
		 * Could not find an AG with enough free space to satisfy
		 * a full btree split.  Try again without minleft and if
		 * successful activate the lowspace algorithm.
		 */
		args.fsbno = 0;
		args.type = XFS_ALLOCTYPE_FIRST_AG;
		args.minleft = 0;
		error = xfs_alloc_vextent(&args);
		if (error)
			goto error0;
		cur->bc_private.b.dfops->dop_low = true;
	}
	if (args.fsbno == NULLFSBLOCK) {
		XFS_BTREE_TRACE_CURSOR(cur, XBT_EXIT);
		*stat = 0;
		return 0;
	}
	ASSERT(args.len == 1);
	cur->bc_private.b.firstblock = args.fsbno;
	cur->bc_private.b.allocated++;
	cur->bc_private.b.ip->i_d.di_nblocks++;
	xfs_trans_log_inode(args.tp, cur->bc_private.b.ip, XFS_ILOG_CORE);

	new->l = cpu_to_be64(args.fsbno);

	XFS_BTREE_TRACE_CURSOR(cur, XBT_EXIT);
	*stat = 1;
	return 0;

 error0:
	XFS_BTREE_TRACE_CURSOR(cur, XBT_ERROR);
	return error;
}

STATIC int
xfs_rtrmapbt_free_block(
	struct xfs_btree_cur	*cur,
	struct xfs_buf		*bp)
{
	struct xfs_mount	*mp = cur->bc_mp;
	struct xfs_inode	*ip = cur->bc_private.b.ip;
	struct xfs_trans	*tp = cur->bc_tp;
	xfs_fsblock_t		fsbno = XFS_DADDR_TO_FSB(mp, XFS_BUF_ADDR(bp));
	struct xfs_owner_info	oinfo;

	xfs_rmap_ino_bmbt_owner(&oinfo, ip->i_ino, cur->bc_private.b.whichfork);
	xfs_bmap_add_free(mp, cur->bc_private.b.dfops, fsbno, 1, &oinfo);
	ip->i_d.di_nblocks--;

	xfs_trans_log_inode(tp, ip, XFS_ILOG_CORE);
	return 0;
}

/*
 * Calculate number of records in the in-core realtime rmap btree inode root.
 */
STATIC int
xfs_rtrmapbt_broot_maxrecs(
	int			blocklen,
	bool			leaf)
{
	blocklen -= XFS_RTRMAP_BLOCK_LEN;

	if (leaf)
		return blocklen / sizeof(struct xfs_rtrmap_rec);
	return blocklen / (2 * sizeof(struct xfs_rtrmap_key) +
			sizeof(xfs_rtrmap_ptr_t));
}

STATIC int
xfs_rtrmapbt_get_minrecs(
	struct xfs_btree_cur	*cur,
	int			level)
{
	struct xfs_ifork	*ifp;

	if (level == cur->bc_nlevels - 1) {
		ifp = XFS_IFORK_PTR(cur->bc_private.b.ip,
				    cur->bc_private.b.whichfork);

		return xfs_rtrmapbt_broot_maxrecs(ifp->if_broot_bytes,
				level == 0) / 2;
	}

	return cur->bc_mp->m_rtrmap_mnr[level != 0];
}

STATIC int
xfs_rtrmapbt_get_maxrecs(
	struct xfs_btree_cur	*cur,
	int			level)
{
	struct xfs_ifork	*ifp;

	if (level == cur->bc_nlevels - 1) {
		ifp = XFS_IFORK_PTR(cur->bc_private.b.ip,
				    cur->bc_private.b.whichfork);

		return xfs_rtrmapbt_broot_maxrecs(ifp->if_broot_bytes,
				level == 0);
	}

	return cur->bc_mp->m_rtrmap_mxr[level != 0];
}

STATIC void
xfs_rtrmapbt_init_key_from_rec(
	union xfs_btree_key	*key,
	union xfs_btree_rec	*rec)
{
	key->rtrmap.rm_startblock = rec->rtrmap.rm_startblock;
	key->rtrmap.rm_owner = rec->rtrmap.rm_owner;
	key->rtrmap.rm_offset = rec->rtrmap.rm_offset;
}

STATIC void
xfs_rtrmapbt_init_high_key_from_rec(
	union xfs_btree_key	*key,
	union xfs_btree_rec	*rec)
{
	uint64_t		off;
	int			adj;

	adj = be64_to_cpu(rec->rtrmap.rm_blockcount) - 1;

	key->rtrmap.rm_startblock = rec->rtrmap.rm_startblock;
	be64_add_cpu(&key->rtrmap.rm_startblock, adj);
	key->rtrmap.rm_owner = rec->rtrmap.rm_owner;
	key->rtrmap.rm_offset = rec->rtrmap.rm_offset;
	if (XFS_RMAP_NON_INODE_OWNER(be64_to_cpu(rec->rtrmap.rm_owner)) ||
	    XFS_RMAP_IS_BMBT_BLOCK(be64_to_cpu(rec->rtrmap.rm_offset)))
		return;
	off = be64_to_cpu(key->rtrmap.rm_offset);
	off = (XFS_RMAP_OFF(off) + adj) | (off & ~XFS_RMAP_OFF_MASK);
	key->rtrmap.rm_offset = cpu_to_be64(off);
}

STATIC void
xfs_rtrmapbt_init_rec_from_cur(
	struct xfs_btree_cur	*cur,
	union xfs_btree_rec	*rec)
{
	rec->rtrmap.rm_startblock = cpu_to_be64(cur->bc_rec.r.rm_startblock);
	rec->rtrmap.rm_blockcount = cpu_to_be64(cur->bc_rec.r.rm_blockcount);
	rec->rtrmap.rm_owner = cpu_to_be64(cur->bc_rec.r.rm_owner);
	rec->rtrmap.rm_offset = cpu_to_be64(
			xfs_rmap_irec_offset_pack(&cur->bc_rec.r));
}

STATIC void
xfs_rtrmapbt_init_ptr_from_cur(
	struct xfs_btree_cur	*cur,
	union xfs_btree_ptr	*ptr)
{
	ptr->l = 0;
}

STATIC int64_t
xfs_rtrmapbt_key_diff(
	struct xfs_btree_cur	*cur,
	union xfs_btree_key	*key)
{
	struct xfs_rmap_irec	*rec = &cur->bc_rec.r;
	struct xfs_rtrmap_key	*kp = &key->rtrmap;
	__u64			x, y;

	x = be64_to_cpu(kp->rm_startblock);
	y = rec->rm_startblock;
	if (x > y)
		return 1;
	else if (y > x)
		return -1;

	x = be64_to_cpu(kp->rm_owner);
	y = rec->rm_owner;
	if (x > y)
		return 1;
	else if (y > x)
		return -1;

	x = XFS_RMAP_OFF(be64_to_cpu(kp->rm_offset));
	y = rec->rm_offset;
	if (x > y)
		return 1;
	else if (y > x)
		return -1;
	return 0;
}

STATIC int64_t
xfs_rtrmapbt_diff_two_keys(
	struct xfs_btree_cur	*cur,
	union xfs_btree_key	*k1,
	union xfs_btree_key	*k2)
{
	struct xfs_rtrmap_key	*kp1 = &k1->rtrmap;
	struct xfs_rtrmap_key	*kp2 = &k2->rtrmap;
	__u64			x, y;

	x = be64_to_cpu(kp1->rm_startblock);
	y = be64_to_cpu(kp2->rm_startblock);
	if (x > y)
		return 1;
	else if (y > x)
		return -1;

	x = be64_to_cpu(kp1->rm_owner);
	y = be64_to_cpu(kp2->rm_owner);
	if (x > y)
		return 1;
	else if (y > x)
		return -1;

	x = XFS_RMAP_OFF(be64_to_cpu(kp1->rm_offset));
	y = XFS_RMAP_OFF(be64_to_cpu(kp2->rm_offset));
	if (x > y)
		return 1;
	else if (y > x)
		return -1;
	return 0;
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
			mp->m_sb.sb_rrmapino)))
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

STATIC int
xfs_rtrmapbt_keys_inorder(
	struct xfs_btree_cur	*cur,
	union xfs_btree_key	*k1,
	union xfs_btree_key	*k2)
{
	if (be64_to_cpu(k1->rtrmap.rm_startblock) <
	    be64_to_cpu(k2->rtrmap.rm_startblock))
		return 1;
	if (be64_to_cpu(k1->rtrmap.rm_owner) <
	    be64_to_cpu(k2->rtrmap.rm_owner))
		return 1;
	if (XFS_RMAP_OFF(be64_to_cpu(k1->rtrmap.rm_offset)) <=
	    XFS_RMAP_OFF(be64_to_cpu(k2->rtrmap.rm_offset)))
		return 1;
	return 0;
}

STATIC int
xfs_rtrmapbt_recs_inorder(
	struct xfs_btree_cur	*cur,
	union xfs_btree_rec	*r1,
	union xfs_btree_rec	*r2)
{
	if (be64_to_cpu(r1->rtrmap.rm_startblock) <
	    be64_to_cpu(r2->rtrmap.rm_startblock))
		return 1;
	if (XFS_RMAP_OFF(be64_to_cpu(r1->rtrmap.rm_offset)) <
	    XFS_RMAP_OFF(be64_to_cpu(r2->rtrmap.rm_offset)))
		return 1;
	if (be64_to_cpu(r1->rtrmap.rm_owner) <=
	    be64_to_cpu(r2->rtrmap.rm_owner))
		return 1;
	return 0;
}

static const struct xfs_btree_ops xfs_rtrmapbt_ops = {
	.rec_len		= sizeof(struct xfs_rtrmap_rec),
	.key_len		= 2 * sizeof(struct xfs_rtrmap_key),

	.dup_cursor		= xfs_rtrmapbt_dup_cursor,
	.alloc_block		= xfs_rtrmapbt_alloc_block,
	.free_block		= xfs_rtrmapbt_free_block,
	.get_minrecs		= xfs_rtrmapbt_get_minrecs,
	.get_maxrecs		= xfs_rtrmapbt_get_maxrecs,
	.init_key_from_rec	= xfs_rtrmapbt_init_key_from_rec,
	.init_high_key_from_rec	= xfs_rtrmapbt_init_high_key_from_rec,
	.init_rec_from_cur	= xfs_rtrmapbt_init_rec_from_cur,
	.init_ptr_from_cur	= xfs_rtrmapbt_init_ptr_from_cur,
	.key_diff		= xfs_rtrmapbt_key_diff,
	.buf_ops		= &xfs_rtrmapbt_buf_ops,
	.diff_two_keys		= xfs_rtrmapbt_diff_two_keys,
	.keys_inorder		= xfs_rtrmapbt_keys_inorder,
	.recs_inorder		= xfs_rtrmapbt_recs_inorder,
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
