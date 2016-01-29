/*
 * Copyright (c) 2014 Red Hat, Inc.
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it would be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write the Free Software Foundation,
 * Inc.,  51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
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
#include "xfs_inode.h"
#include "xfs_trans.h"
#include "xfs_alloc.h"
#include "xfs_btree.h"
#include "xfs_rmap_btree.h"
#include "xfs_trace.h"
#include "xfs_cksum.h"
#include "xfs_ag_resv.h"

/*
 * Reverse map btree.
 *
 * This is a per-ag tree used to track the owner(s) of a given extent. With
 * reflink it is possible for there to be multiple owners, which is a departure
 * from classic XFS. Owner records for data extents are inserted when the
 * extent is mapped and removed when an extent is unmapped.  Owner records for
 * all other block types (i.e. metadata) are inserted when an extent is
 * allocated and removed when an extent is freed. There can only be one owner
 * of a metadata extent, usually an inode or some other metadata structure like
 * an AG btree.
 *
 * The rmap btree is part of the free space management, so blocks for the tree
 * are sourced from the agfl. Hence we need transaction reservation support for
 * this tree so that the freelist is always large enough. This also impacts on
 * the minimum space we need to leave free in the AG.
 *
 * The tree is ordered by [ag block, owner, offset]. This is a large key size,
 * but it is the only way to enforce unique keys when a block can be owned by
 * multiple files at any offset. There's no need to order/search by extent
 * size for online updating/management of the tree. It is intended that most
 * reverse lookups will be to find the owner(s) of a particular block, or to
 * try to recover tree and file data from corrupt primary metadata.
 */

static struct xfs_btree_cur *
xfs_rmapbt_dup_cursor(
	struct xfs_btree_cur	*cur)
{
	return xfs_rmapbt_init_cursor(cur->bc_mp, cur->bc_tp,
			cur->bc_private.a.agbp, cur->bc_private.a.agno);
}

STATIC void
xfs_rmapbt_set_root(
	struct xfs_btree_cur	*cur,
	union xfs_btree_ptr	*ptr,
	int			inc)
{
	struct xfs_buf		*agbp = cur->bc_private.a.agbp;
	struct xfs_agf		*agf = XFS_BUF_TO_AGF(agbp);
	xfs_agnumber_t		seqno = be32_to_cpu(agf->agf_seqno);
	int			btnum = XFS_BTNUM_RMAP;
	struct xfs_perag	*pag = xfs_perag_get(cur->bc_mp, seqno);

	ASSERT(ptr->s != 0);

	agf->agf_roots[btnum] = ptr->s;
	be32_add_cpu(&agf->agf_levels[btnum], inc);
	pag->pagf_levels[btnum] += inc;
	xfs_perag_put(pag);

	xfs_alloc_log_agf(cur->bc_tp, agbp, XFS_AGF_ROOTS | XFS_AGF_LEVELS);
}

STATIC int
xfs_rmapbt_alloc_block(
	struct xfs_btree_cur	*cur,
	union xfs_btree_ptr	*start,
	union xfs_btree_ptr	*new,
	int			*stat)
{
	struct xfs_perag	*pag;
	int			error;
	xfs_agblock_t		bno;

	XFS_BTREE_TRACE_CURSOR(cur, XBT_ENTRY);

	/* Allocate the new block from the freelist. If we can't, give up.  */
	error = xfs_alloc_get_freelist(cur->bc_tp, cur->bc_private.a.agbp,
				       &bno, 1);
	if (error) {
		XFS_BTREE_TRACE_CURSOR(cur, XBT_ERROR);
		return error;
	}

	if (bno == NULLAGBLOCK) {
		XFS_BTREE_TRACE_CURSOR(cur, XBT_EXIT);
		*stat = 0;
		return 0;
	}

	xfs_extent_busy_reuse(cur->bc_mp, cur->bc_private.a.agno, bno, 1, false);

	xfs_trans_agbtree_delta(cur->bc_tp, 1);
	new->s = cpu_to_be32(bno);

	if (xfs_sb_version_hasrmapxbt(&cur->bc_mp->m_sb)) {
		pag = xfs_perag_get(cur->bc_mp, cur->bc_private.a.agno);
		xfs_ag_resv_alloc_block(pag->pagf_rmapbt_resv, cur->bc_tp, pag);
		xfs_perag_put(pag);
	}

	XFS_BTREE_TRACE_CURSOR(cur, XBT_EXIT);
	*stat = 1;
	return 0;
}

STATIC int
xfs_rmapbt_free_block(
	struct xfs_btree_cur	*cur,
	struct xfs_buf		*bp)
{
	struct xfs_buf		*agbp = cur->bc_private.a.agbp;
	struct xfs_agf		*agf = XFS_BUF_TO_AGF(agbp);
	struct xfs_perag	*pag;
	xfs_agblock_t		bno;
	int			error;

	bno = xfs_daddr_to_agbno(cur->bc_mp, XFS_BUF_ADDR(bp));
	error = xfs_alloc_put_freelist(cur->bc_tp, agbp, NULL, bno, 1);
	if (error)
		return error;

	xfs_extent_busy_insert(cur->bc_tp, be32_to_cpu(agf->agf_seqno), bno, 1,
			      XFS_EXTENT_BUSY_SKIP_DISCARD);
	xfs_trans_agbtree_delta(cur->bc_tp, -1);

	if (xfs_sb_version_hasrmapxbt(&cur->bc_mp->m_sb)) {
		pag = xfs_perag_get(cur->bc_mp, cur->bc_private.a.agno);
		xfs_ag_resv_alloc_block(pag->pagf_rmapbt_resv, cur->bc_tp, pag);
		xfs_perag_put(pag);
	}

	xfs_trans_binval(cur->bc_tp, bp);
	return 0;
}

STATIC int
xfs_rmapbt_get_minrecs(
	struct xfs_btree_cur	*cur,
	int			level)
{
	return cur->bc_mp->m_rmap_mnr[level != 0];
}

STATIC int
xfs_rmapbt_get_maxrecs(
	struct xfs_btree_cur	*cur,
	int			level)
{
	return cur->bc_mp->m_rmap_mxr[level != 0];
}

STATIC void
xfs_rmapbt_init_key_from_rec(
	union xfs_btree_key	*key,
	union xfs_btree_rec	*rec)
{
	key->rmap.rm_startblock = rec->rmap.rm_startblock;
}

STATIC void
xfs_rmapxbt_init_key_from_rec(
	union xfs_btree_key	*key,
	union xfs_btree_rec	*rec)
{
	key->rmapx.rm_startblock = rec->rmap.rm_startblock;
	key->rmapx.rm_owner = rec->rmap.rm_owner;
	key->rmapx.rm_offset = rec->rmap.rm_offset;
}

STATIC void
xfs_rmapbt_init_rec_from_key(
	union xfs_btree_key	*key,
	union xfs_btree_rec	*rec)
{
	rec->rmap.rm_startblock = key->rmap.rm_startblock;
}

STATIC void
xfs_rmapxbt_init_rec_from_key(
	union xfs_btree_key	*key,
	union xfs_btree_rec	*rec)
{
	rec->rmap.rm_startblock = key->rmapx.rm_startblock;
	rec->rmap.rm_owner = key->rmapx.rm_owner;
	rec->rmap.rm_offset = key->rmapx.rm_offset;
}

STATIC void
xfs_rmapbt_init_rec_from_cur(
	struct xfs_btree_cur	*cur,
	union xfs_btree_rec	*rec)
{
	rec->rmap.rm_startblock = cpu_to_be32(cur->bc_rec.r.rm_startblock);
	rec->rmap.rm_blockcount = cpu_to_be32(cur->bc_rec.r.rm_blockcount);
	rec->rmap.rm_owner = cpu_to_be64(cur->bc_rec.r.rm_owner);
	rec->rmap.rm_offset = cpu_to_be64(cur->bc_rec.r.rm_offset);
}

STATIC void
xfs_rmapbt_init_ptr_from_cur(
	struct xfs_btree_cur	*cur,
	union xfs_btree_ptr	*ptr)
{
	struct xfs_agf		*agf = XFS_BUF_TO_AGF(cur->bc_private.a.agbp);

	ASSERT(cur->bc_private.a.agno == be32_to_cpu(agf->agf_seqno));
	ASSERT(agf->agf_roots[XFS_BTNUM_RMAP] != 0);

	ptr->s = agf->agf_roots[XFS_BTNUM_RMAP];
}

STATIC __int64_t
xfs_rmapbt_key_diff(
	struct xfs_btree_cur	*cur,
	union xfs_btree_key	*key)
{
	struct xfs_rmap_irec	*rec = &cur->bc_rec.r;
	struct xfs_rmap_key	*kp = &key->rmap;

	return (__int64_t)be32_to_cpu(kp->rm_startblock) - rec->rm_startblock;
}

STATIC __int64_t
xfs_rmapxbt_key_diff(
	struct xfs_btree_cur	*cur,
	union xfs_btree_key	*key)
{
	struct xfs_rmap_irec	*rec = &cur->bc_rec.r;
	struct xfs_rmapx_key	*kp = &key->rmapx;
	__int64_t		d;

	d = (__int64_t)be32_to_cpu(kp->rm_startblock) - rec->rm_startblock;
	if (d)
		return d;
	d = (__int64_t)be64_to_cpu(kp->rm_owner) - rec->rm_owner;
	if (d)
		return d;
	d = (__int64_t)be64_to_cpu(kp->rm_offset) - rec->rm_offset;
	return d;
}

static bool
xfs_rmapbt_verify(
	struct xfs_buf		*bp)
{
	struct xfs_mount	*mp = bp->b_target->bt_mount;
	struct xfs_btree_block	*block = XFS_BUF_TO_BLOCK(bp);
	struct xfs_perag	*pag = bp->b_pag;
	unsigned int		level;

	/*
	 * magic number and level verification
	 *
	 * During growfs operations, we can't verify the exact level or owner as
	 * the perag is not fully initialised and hence not attached to the
	 * buffer.  In this case, check against the maximum tree depth.
	 *
	 * Similarly, during log recovery we will have a perag structure
	 * attached, but the agf information will not yet have been initialised
	 * from the on disk AGF. Again, we can only check against maximum limits
	 * in this case.
	 */
	if (xfs_sb_version_hasrmapxbt(&mp->m_sb) &&
	    block->bb_magic != cpu_to_be32(XFS_RMAPX_CRC_MAGIC))
		return false;
	if (!xfs_sb_version_hasrmapxbt(&mp->m_sb) &&
	    block->bb_magic != cpu_to_be32(XFS_RMAP_CRC_MAGIC))
		return false;

	if (!xfs_sb_version_hasrmapbt(&mp->m_sb))
		return false;
	if (!xfs_btree_sblock_v5hdr_verify(bp))
		return false;

	level = be16_to_cpu(block->bb_level);
	if (pag && pag->pagf_init) {
		if (level >= pag->pagf_levels[XFS_BTNUM_RMAPi])
			return false;
	} else if (!xfs_sb_version_hasreflink(&mp->m_sb) &&
		  level >= maxlevels)
		return false;

	return xfs_btree_sblock_verify(bp, mp->m_rmap_mxr[level != 0]);
}

static void
xfs_rmapbt_read_verify(
	struct xfs_buf	*bp)
{
	if (!xfs_btree_sblock_verify_crc(bp))
		xfs_buf_ioerror(bp, -EFSBADCRC);
	else if (!xfs_rmapbt_verify(bp))
		xfs_buf_ioerror(bp, -EFSCORRUPTED);

	if (bp->b_error) {
		trace_xfs_btree_corrupt(bp, _RET_IP_);
		xfs_verifier_error(bp);
	}
}

static void
xfs_rmapbt_write_verify(
	struct xfs_buf	*bp)
{
	if (!xfs_rmapbt_verify(bp)) {
		trace_xfs_btree_corrupt(bp, _RET_IP_);
		xfs_buf_ioerror(bp, -EFSCORRUPTED);
		xfs_verifier_error(bp);
		return;
	}
	xfs_btree_sblock_calc_crc(bp);

}

const struct xfs_buf_ops xfs_rmapbt_buf_ops = {
	.name			= "xfs_rmapbt",
	.verify_read		= xfs_rmapbt_read_verify,
	.verify_write		= xfs_rmapbt_write_verify,
};

#if defined(DEBUG) || defined(XFS_WARN)
STATIC int
xfs_rmapbt_keys_inorder(
	struct xfs_btree_cur	*cur,
	union xfs_btree_key	*k1,
	union xfs_btree_key	*k2)
{
	if (be32_to_cpu(k1->rmap.rm_startblock) <
	    be32_to_cpu(k2->rmap.rm_startblock))
		return 1;
	return 0;
}

STATIC int
xfs_rmapxbt_keys_inorder(
	struct xfs_btree_cur	*cur,
	union xfs_btree_key	*k1,
	union xfs_btree_key	*k2)
{
	if (be32_to_cpu(k1->rmapx.rm_startblock) <
	    be32_to_cpu(k2->rmapx.rm_startblock))
		return 1;
	if (be64_to_cpu(k1->rmapx.rm_owner) <
	    be64_to_cpu(k2->rmapx.rm_owner))
		return 1;
	if (be64_to_cpu(k1->rmapx.rm_offset) <=
	    be64_to_cpu(k2->rmapx.rm_offset))
		return 1;
	return 0;
}

STATIC int
xfs_rmapbt_recs_inorder(
	struct xfs_btree_cur	*cur,
	union xfs_btree_rec	*r1,
	union xfs_btree_rec	*r2)
{
	if (be32_to_cpu(r1->rmap.rm_startblock) <
	    be32_to_cpu(r2->rmap.rm_startblock))
		return 1;
	if (be64_to_cpu(r1->rmap.rm_offset) <
	    be64_to_cpu(r2->rmap.rm_offset))
		return 1;
	if (be64_to_cpu(r1->rmap.rm_owner) <=
	    be64_to_cpu(r2->rmap.rm_owner))
		return 1;
	return 0;
}
#endif	/* DEBUG */

static const struct xfs_btree_ops xfs_rmapbt_ops = {
	.rec_len		= sizeof(struct xfs_rmap_rec),
	.key_len		= sizeof(struct xfs_rmap_key),

	.dup_cursor		= xfs_rmapbt_dup_cursor,
	.set_root		= xfs_rmapbt_set_root,
	.alloc_block		= xfs_rmapbt_alloc_block,
	.free_block		= xfs_rmapbt_free_block,
	.get_minrecs		= xfs_rmapbt_get_minrecs,
	.get_maxrecs		= xfs_rmapbt_get_maxrecs,
	.init_key_from_rec	= xfs_rmapbt_init_key_from_rec,
	.init_rec_from_key	= xfs_rmapbt_init_rec_from_key,
	.init_rec_from_cur	= xfs_rmapbt_init_rec_from_cur,
	.init_ptr_from_cur	= xfs_rmapbt_init_ptr_from_cur,
	.key_diff		= xfs_rmapbt_key_diff,
	.buf_ops		= &xfs_rmapbt_buf_ops,
#if defined(DEBUG) || defined(XFS_WARN)
	.keys_inorder		= xfs_rmapbt_keys_inorder,
	.recs_inorder		= xfs_rmapbt_recs_inorder,
#endif
};

static const struct xfs_btree_ops xfs_rmapxbt_ops = {
	.rec_len		= sizeof(struct xfs_rmap_rec),
	.key_len		= sizeof(struct xfs_rmapx_key),

	.dup_cursor		= xfs_rmapbt_dup_cursor,
	.set_root		= xfs_rmapbt_set_root,
	.alloc_block		= xfs_rmapbt_alloc_block,
	.free_block		= xfs_rmapbt_free_block,
	.get_minrecs		= xfs_rmapbt_get_minrecs,
	.get_maxrecs		= xfs_rmapbt_get_maxrecs,
	.init_key_from_rec	= xfs_rmapxbt_init_key_from_rec,
	.init_rec_from_key	= xfs_rmapxbt_init_rec_from_key,
	.init_rec_from_cur	= xfs_rmapbt_init_rec_from_cur,
	.init_ptr_from_cur	= xfs_rmapbt_init_ptr_from_cur,
	.key_diff		= xfs_rmapxbt_key_diff,
	.buf_ops		= &xfs_rmapbt_buf_ops,
#if defined(DEBUG) || defined(XFS_WARN)
	.keys_inorder		= xfs_rmapxbt_keys_inorder,
	.recs_inorder		= xfs_rmapbt_recs_inorder,
#endif
};

/*
 * Allocate a new allocation btree cursor.
 */
struct xfs_btree_cur *
xfs_rmapbt_init_cursor(
	struct xfs_mount	*mp,
	struct xfs_trans	*tp,
	struct xfs_buf		*agbp,
	xfs_agnumber_t		agno)
{
	struct xfs_agf		*agf = XFS_BUF_TO_AGF(agbp);
	struct xfs_btree_cur	*cur;

	cur = kmem_zone_zalloc(xfs_btree_cur_zone, KM_SLEEP);
	cur->bc_tp = tp;
	cur->bc_mp = mp;
	if (xfs_sb_version_hasrmapxbt(&mp->m_sb))
		cur->bc_btnum = XFS_BTNUM_RMAPX;
	else
		cur->bc_btnum = XFS_BTNUM_RMAP;
	cur->bc_flags = XFS_BTREE_CRC_BLOCKS;
	cur->bc_blocklog = mp->m_sb.sb_blocklog;
	if (xfs_sb_version_hasrmapxbt(&cur->bc_mp->m_sb))
		cur->bc_ops = &xfs_rmapxbt_ops;
	else
		cur->bc_ops = &xfs_rmapbt_ops;
	cur->bc_nlevels = be32_to_cpu(agf->agf_levels[XFS_BTNUM_RMAP]);

	cur->bc_private.a.agbp = agbp;
	cur->bc_private.a.agno = agno;

	return cur;
}

/*
 * Calculate number of records in an rmap btree block.
 */
int
xfs_rmapbt_maxrecs(
	struct xfs_mount	*mp,
	int			blocklen,
	int			leaf)
{
	blocklen -= XFS_RMAP_BLOCK_LEN;

	if (leaf)
		return blocklen / sizeof(struct xfs_rmap_rec);
	return blocklen /
		(sizeof(struct xfs_rmap_key) + sizeof(xfs_rmap_ptr_t));
}

/*
 * Calculate number of records in an rmapx btree block.
 */
int
xfs_rmapxbt_maxrecs(
	struct xfs_mount	*mp,
	int			blocklen,
	int			leaf)
{
	blocklen -= XFS_RMAP_BLOCK_LEN;

	if (leaf)
		return blocklen / sizeof(struct xfs_rmap_rec);
	return blocklen /
		(sizeof(struct xfs_rmapx_key) + sizeof(xfs_rmap_ptr_t));
}

/* Calculate the refcount btree size for some records. */
xfs_extlen_t
xfs_rmapbt_calc_size(
	struct xfs_mount	*mp,
	unsigned long long	len)
{
	return xfs_btree_calc_size(mp, mp->m_rmap_mxr, len);
}

/*
 * Calculate the maximum refcount btree size.
 */
xfs_extlen_t
xfs_rmapbt_max_size(
	struct xfs_mount	*mp)
{
	/* Bail out if we're uninitialized, which can happen in mkfs. */
	if (mp->m_rmap_mxr[0] == 0)
		return 0;

	return xfs_rmapbt_calc_size(mp, mp->m_sb.sb_agblocks);
}

static int
xfs_rmapbt_cb_getroot(
	struct xfs_mount	*mp,
	xfs_agnumber_t		agno,
	struct xfs_buf		**bpp,
	int			*level,
	xfs_agblock_t		*bno)
{
	struct xfs_agf		*agfp;
	int			error;

	error = xfs_alloc_read_agf(mp, NULL, agno, 0, bpp);
	if (error)
		return error;
	agfp = XFS_BUF_TO_AGF(*bpp);
	*level = be32_to_cpu(agfp->agf_levels[XFS_BTNUM_RMAPi]);
	*bno = be32_to_cpu(agfp->agf_roots[XFS_BTNUM_RMAPi]);
	return 0;
}

static xfs_agblock_t
xfs_rmapbt_cb_getptr(
	struct xfs_mount	*mp,
	struct xfs_btree_block	*block)
{
	__be32			*pp;

	pp = XFS_RMAP_PTR_ADDR(block, 1, mp->m_rmap_mxr[1]);
	return be32_to_cpu(*pp);
}

static xfs_agblock_t
xfs_rmapxbt_cb_getptr(
	struct xfs_mount	*mp,
	struct xfs_btree_block	*block)
{
	__be32			*pp;

	pp = XFS_RMAPX_PTR_ADDR(block, 1, mp->m_rmap_mxr[1]);
	return be32_to_cpu(*pp);
}

/* Count the blocks in the reference count tree. */
static int
xfs_rmapbt_count_blocks(
	struct xfs_mount	*mp,
	xfs_agnumber_t		agno,
	xfs_extlen_t		*tree_blocks)
{
	if (xfs_sb_version_hasrmapxbt(&mp->m_sb))
		return xfs_btree_count_blocks(mp, xfs_rmapbt_cb_getroot,
				xfs_rmapxbt_cb_getptr, &xfs_rmapbt_buf_ops,
				agno, tree_blocks);
	return xfs_btree_count_blocks(mp, xfs_rmapbt_cb_getroot,
			xfs_rmapbt_cb_getptr, &xfs_rmapbt_buf_ops, agno,
			tree_blocks);
}

/*
 * Create reserved block pools for each allocation group.
 */
int
xfs_rmapbt_alloc_reserve_pool(
	struct xfs_mount	*mp)
{
	xfs_agnumber_t		agno;
	struct xfs_perag	*pag;
	xfs_extlen_t		pool_len;
	xfs_extlen_t		tree_len;
	int			error = 0;
	int			err;

	if (!xfs_sb_version_hasrmapxbt(&mp->m_sb))
		return 0;

	/* Reserve 1% of the AG or enough for 1 block per record. */
	pool_len = max(mp->m_sb.sb_agblocks / 100, xfs_rmapbt_max_size(mp));
	xfs_ag_resv_type_init(mp, pool_len);

	for (agno = 0; agno < mp->m_sb.sb_agcount; agno++) {
		pag = xfs_perag_get(mp, agno);
		if (pag->pagf_rmapbt_resv) {
			xfs_perag_put(pag);
			continue;
		}
		tree_len = 0;
		err = xfs_rmapbt_count_blocks(mp, agno, &tree_len);
		if (err && !error)
			error = err;
		err = xfs_ag_resv_init(mp, pag, pool_len, tree_len,
				XFS_AG_RESV_AGFL, &pag->pagf_rmapbt_resv);
		xfs_perag_put(pag);
		if (err && !error)
			error = err;
	}

	return error;
}

/*
 * Free the reference count btree pools.
 */
int
xfs_rmapbt_free_reserve_pool(
	struct xfs_mount	*mp)
{
	xfs_agnumber_t		agno;
	struct xfs_perag	*pag;
	xfs_extlen_t		pool_len, i;
	int			error = 0;
	int			err;

	if (!xfs_sb_version_hasrmapxbt(&mp->m_sb))
		return 0;

	pool_len = 0;
	for (agno = 0; agno < mp->m_sb.sb_agcount; agno++) {
		pag = xfs_perag_get(mp, agno);
		if (!pag->pagf_rmapbt_resv) {
			xfs_perag_put(pag);
			continue;
		}
		i = xfs_ag_resv_blocks(pag->pagf_rmapbt_resv);
		if (pool_len < i)
			pool_len = i;
		err = xfs_ag_resv_free(pag->pagf_rmapbt_resv, pag);
		pag->pagf_rmapbt_resv = NULL;
		xfs_perag_put(pag);
		if (err && !error)
			error = err;
	}
	xfs_ag_resv_type_free(mp, pool_len);

	return error;
}
