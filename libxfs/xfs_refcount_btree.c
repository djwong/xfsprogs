/*
 * Copyright (c) 2000-2001,2005 Silicon Graphics, Inc.
 * Copyright (c) 2016 Oracle.
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
#include "xfs_sb.h"
#include "xfs_mount.h"
#include "xfs_btree.h"
#include "xfs_bmap.h"
#include "xfs_refcount_btree.h"
#include "xfs_alloc.h"
#include "xfs_trace.h"
#include "xfs_cksum.h"
#include "xfs_trans.h"
#include "xfs_bit.h"
#include "xfs_ag_resv.h"

static struct xfs_btree_cur *
xfs_refcountbt_dup_cursor(
	struct xfs_btree_cur	*cur)
{
	return xfs_refcountbt_init_cursor(cur->bc_mp, cur->bc_tp,
			cur->bc_private.a.agbp, cur->bc_private.a.agno,
			cur->bc_private.a.flist);
}

STATIC void
xfs_refcountbt_set_root(
	struct xfs_btree_cur	*cur,
	union xfs_btree_ptr	*ptr,
	int			inc)
{
	struct xfs_buf		*agbp = cur->bc_private.a.agbp;
	struct xfs_agf		*agf = XFS_BUF_TO_AGF(agbp);
	xfs_agnumber_t		seqno = be32_to_cpu(agf->agf_seqno);
	struct xfs_perag	*pag = xfs_perag_get(cur->bc_mp, seqno);

	ASSERT(ptr->s != 0);

	agf->agf_refcount_root = ptr->s;
	be32_add_cpu(&agf->agf_refcount_level, inc);
	pag->pagf_refcount_level += inc;
	xfs_perag_put(pag);

	xfs_alloc_log_agf(cur->bc_tp, agbp, XFS_AGF_ROOTS | XFS_AGF_LEVELS);
}

STATIC int
xfs_refcountbt_alloc_block(
	struct xfs_btree_cur	*cur,
	union xfs_btree_ptr	*start,
	union xfs_btree_ptr	*new,
	int			*stat)
{
	struct xfs_alloc_arg	args;		/* block allocation args */
	struct xfs_perag	*pag;
	int			error;		/* error return value */

	pag = xfs_perag_get(cur->bc_mp, cur->bc_private.a.agno);
	memset(&args, 0, sizeof(args));
	args.tp = cur->bc_tp;
	args.mp = cur->bc_mp;
	args.type = XFS_ALLOCTYPE_NEAR_BNO;
	args.fsbno = XFS_AGB_TO_FSB(cur->bc_mp, cur->bc_private.a.agno,
			xfs_refc_block(args.mp));
	args.firstblock = args.fsbno;
	XFS_RMAP_AG_OWNER(&args.oinfo, XFS_RMAP_OWN_REFC);
	args.minlen = args.maxlen = args.prod = 1;
	args.resv = pag->pagf_refcountbt_resv;

	error = xfs_alloc_vextent(&args);
	if (error)
		goto out_error;
	if (args.fsbno == NULLFSBLOCK) {
		XFS_BTREE_TRACE_CURSOR(cur, XBT_EXIT);
		*stat = 0;
		xfs_perag_put(pag);
		return 0;
	}
	ASSERT(args.agno == cur->bc_private.a.agno);
	ASSERT(args.len == 1);

	new->s = cpu_to_be32(args.agbno);

	xfs_ag_resv_alloc_block(pag->pagf_refcountbt_resv, cur->bc_tp, pag);
	xfs_perag_put(pag);

	XFS_BTREE_TRACE_CURSOR(cur, XBT_EXIT);
	*stat = 1;
	return 0;

out_error:
	xfs_perag_put(pag);
	XFS_BTREE_TRACE_CURSOR(cur, XBT_ERROR);
	return error;
}

STATIC int
xfs_refcountbt_free_block(
	struct xfs_btree_cur	*cur,
	struct xfs_buf		*bp)
{
	struct xfs_mount	*mp = cur->bc_mp;
	struct xfs_perag	*pag;
	xfs_fsblock_t		fsbno = XFS_DADDR_TO_FSB(mp, XFS_BUF_ADDR(bp));
	struct xfs_owner_info	oinfo;
	int			error;

	XFS_RMAP_AG_OWNER(&oinfo, XFS_RMAP_OWN_REFC);
	error = xfs_free_extent(cur->bc_tp, fsbno, 1, &oinfo);
	if (error)
		return error;

	pag = xfs_perag_get(cur->bc_mp, cur->bc_private.a.agno);
	xfs_ag_resv_free_block(pag->pagf_refcountbt_resv, cur->bc_tp, pag);
	xfs_perag_put(pag);

	xfs_trans_binval(cur->bc_tp, bp);
	return error;
}

STATIC int
xfs_refcountbt_get_minrecs(
	struct xfs_btree_cur	*cur,
	int			level)
{
	return cur->bc_mp->m_refc_mnr[level != 0];
}

STATIC int
xfs_refcountbt_get_maxrecs(
	struct xfs_btree_cur	*cur,
	int			level)
{
	return cur->bc_mp->m_refc_mxr[level != 0];
}

STATIC void
xfs_refcountbt_init_key_from_rec(
	union xfs_btree_key	*key,
	union xfs_btree_rec	*rec)
{
	ASSERT(rec->refc.rc_startblock != 0);

	key->refc.rc_startblock = rec->refc.rc_startblock;
}

STATIC void
xfs_refcountbt_init_rec_from_key(
	union xfs_btree_key	*key,
	union xfs_btree_rec	*rec)
{
	ASSERT(key->refc.rc_startblock != 0);

	rec->refc.rc_startblock = key->refc.rc_startblock;
}

STATIC void
xfs_refcountbt_init_rec_from_cur(
	struct xfs_btree_cur	*cur,
	union xfs_btree_rec	*rec)
{
	ASSERT(cur->bc_rec.rc.rc_startblock != 0);

	rec->refc.rc_startblock = cpu_to_be32(cur->bc_rec.rc.rc_startblock);
	rec->refc.rc_blockcount = cpu_to_be32(cur->bc_rec.rc.rc_blockcount);
	rec->refc.rc_refcount = cpu_to_be32(cur->bc_rec.rc.rc_refcount);
}

STATIC void
xfs_refcountbt_init_ptr_from_cur(
	struct xfs_btree_cur	*cur,
	union xfs_btree_ptr	*ptr)
{
	struct xfs_agf		*agf = XFS_BUF_TO_AGF(cur->bc_private.a.agbp);

	ASSERT(cur->bc_private.a.agno == be32_to_cpu(agf->agf_seqno));
	ASSERT(agf->agf_refcount_root != 0);

	ptr->s = agf->agf_refcount_root;
}

STATIC __int64_t
xfs_refcountbt_key_diff(
	struct xfs_btree_cur	*cur,
	union xfs_btree_key	*key)
{
	struct xfs_refcount_irec	*rec = &cur->bc_rec.rc;
	struct xfs_refcount_key		*kp = &key->refc;

	return (__int64_t)be32_to_cpu(kp->rc_startblock) - rec->rc_startblock;
}

STATIC bool
xfs_refcountbt_verify(
	struct xfs_buf		*bp)
{
	struct xfs_mount	*mp = bp->b_target->bt_mount;
	struct xfs_btree_block	*block = XFS_BUF_TO_BLOCK(bp);
	struct xfs_perag	*pag = bp->b_pag;
	unsigned int		level;

	if (block->bb_magic != cpu_to_be32(XFS_REFC_CRC_MAGIC))
		return false;

	if (!xfs_sb_version_hasreflink(&mp->m_sb))
		return false;
	if (!xfs_btree_sblock_v5hdr_verify(bp))
		return false;

	level = be16_to_cpu(block->bb_level);
	if (pag && pag->pagf_init) {
		if (level >= pag->pagf_refcount_level)
			return false;
	} else if (level >= mp->m_ag_maxlevels)
		return false;

	return xfs_btree_sblock_verify(bp, mp->m_refc_mxr[level != 0]);
}

STATIC void
xfs_refcountbt_read_verify(
	struct xfs_buf	*bp)
{
	if (!xfs_btree_sblock_verify_crc(bp))
		xfs_buf_ioerror(bp, -EFSBADCRC);
	else if (!xfs_refcountbt_verify(bp))
		xfs_buf_ioerror(bp, -EFSCORRUPTED);

	if (bp->b_error) {
		trace_xfs_btree_corrupt(bp, _RET_IP_);
		xfs_verifier_error(bp);
	}
}

STATIC void
xfs_refcountbt_write_verify(
	struct xfs_buf	*bp)
{
	if (!xfs_refcountbt_verify(bp)) {
		trace_xfs_btree_corrupt(bp, _RET_IP_);
		xfs_buf_ioerror(bp, -EFSCORRUPTED);
		xfs_verifier_error(bp);
		return;
	}
	xfs_btree_sblock_calc_crc(bp);

}

const struct xfs_buf_ops xfs_refcountbt_buf_ops = {
	.name			= "xfs_refcountbt",
	.verify_read		= xfs_refcountbt_read_verify,
	.verify_write		= xfs_refcountbt_write_verify,
};

#if defined(DEBUG) || defined(XFS_WARN)
STATIC int
xfs_refcountbt_keys_inorder(
	struct xfs_btree_cur	*cur,
	union xfs_btree_key	*k1,
	union xfs_btree_key	*k2)
{
	return be32_to_cpu(k1->refc.rc_startblock) <
	       be32_to_cpu(k2->refc.rc_startblock);
}

STATIC int
xfs_refcountbt_recs_inorder(
	struct xfs_btree_cur	*cur,
	union xfs_btree_rec	*r1,
	union xfs_btree_rec	*r2)
{
	struct xfs_refcount_irec	a, b;

	int ret = be32_to_cpu(r1->refc.rc_startblock) +
		be32_to_cpu(r1->refc.rc_blockcount) <=
		be32_to_cpu(r2->refc.rc_startblock);
	if (!ret) {
		a.rc_startblock = be32_to_cpu(r1->refc.rc_startblock);
		a.rc_blockcount = be32_to_cpu(r1->refc.rc_blockcount);
		a.rc_refcount = be32_to_cpu(r1->refc.rc_refcount);
		b.rc_startblock = be32_to_cpu(r2->refc.rc_startblock);
		b.rc_blockcount = be32_to_cpu(r2->refc.rc_blockcount);
		b.rc_refcount = be32_to_cpu(r2->refc.rc_refcount);
		trace_xfs_refcount_rec_order_error(cur->bc_mp,
				cur->bc_private.a.agno, &a, &b);
	}

	return ret;
}
#endif	/* DEBUG */

static const struct xfs_btree_ops xfs_refcountbt_ops = {
	.rec_len		= sizeof(struct xfs_refcount_rec),
	.key_len		= sizeof(struct xfs_refcount_key),

	.dup_cursor		= xfs_refcountbt_dup_cursor,
	.set_root		= xfs_refcountbt_set_root,
	.alloc_block		= xfs_refcountbt_alloc_block,
	.free_block		= xfs_refcountbt_free_block,
	.get_minrecs		= xfs_refcountbt_get_minrecs,
	.get_maxrecs		= xfs_refcountbt_get_maxrecs,
	.init_key_from_rec	= xfs_refcountbt_init_key_from_rec,
	.init_rec_from_key	= xfs_refcountbt_init_rec_from_key,
	.init_rec_from_cur	= xfs_refcountbt_init_rec_from_cur,
	.init_ptr_from_cur	= xfs_refcountbt_init_ptr_from_cur,
	.key_diff		= xfs_refcountbt_key_diff,
	.buf_ops		= &xfs_refcountbt_buf_ops,
#if defined(DEBUG) || defined(XFS_WARN)
	.keys_inorder		= xfs_refcountbt_keys_inorder,
	.recs_inorder		= xfs_refcountbt_recs_inorder,
#endif
};

/*
 * Allocate a new refcount btree cursor.
 */
struct xfs_btree_cur *
xfs_refcountbt_init_cursor(
	struct xfs_mount	*mp,
	struct xfs_trans	*tp,
	struct xfs_buf		*agbp,
	xfs_agnumber_t		agno,
	struct xfs_bmap_free	*flist)
{
	struct xfs_agf		*agf = XFS_BUF_TO_AGF(agbp);
	struct xfs_btree_cur	*cur;

	ASSERT(agno != NULLAGNUMBER);
	ASSERT(agno < mp->m_sb.sb_agcount);
	cur = kmem_zone_zalloc(xfs_btree_cur_zone, KM_SLEEP);

	cur->bc_tp = tp;
	cur->bc_mp = mp;
	cur->bc_btnum = XFS_BTNUM_REFC;
	cur->bc_blocklog = mp->m_sb.sb_blocklog;
	cur->bc_ops = &xfs_refcountbt_ops;

	cur->bc_nlevels = be32_to_cpu(agf->agf_refcount_level);

	cur->bc_private.a.agbp = agbp;
	cur->bc_private.a.agno = agno;
	cur->bc_private.a.flist = flist;
	cur->bc_flags |= XFS_BTREE_CRC_BLOCKS;

	return cur;
}

/*
 * Calculate the number of records in a refcount btree block.
 */
int
xfs_refcountbt_maxrecs(
	struct xfs_mount	*mp,
	int			blocklen,
	bool			leaf)
{
	blocklen -= XFS_REFCOUNT_BLOCK_LEN;

	if (leaf)
		return blocklen / sizeof(struct xfs_refcount_rec);
	return blocklen / (sizeof(struct xfs_refcount_key) +
			   sizeof(xfs_refcount_ptr_t));
}

/* Calculate the refcount btree size for some records. */
xfs_extlen_t
xfs_refcountbt_calc_size(
	struct xfs_mount	*mp,
	unsigned long long	len)
{
	return xfs_btree_calc_size(mp, mp->m_refc_mxr, len);
}

/*
 * Calculate the maximum refcount btree size.
 */
xfs_extlen_t
xfs_refcountbt_max_size(
	struct xfs_mount	*mp)
{
	/* Bail out if we're uninitialized, which can happen in mkfs. */
	if (mp->m_refc_mxr[0] == 0)
		return 0;

	return xfs_refcountbt_calc_size(mp, mp->m_sb.sb_agblocks);
}

static int
xfs_refcountbt_cb_getroot(
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
	*level = be32_to_cpu(agfp->agf_refcount_level);
	*bno = be32_to_cpu(agfp->agf_refcount_root);
	return 0;
}

static xfs_agblock_t
xfs_refcountbt_cb_getptr(
	struct xfs_mount	*mp,
	struct xfs_btree_block	*block)
{
	__be32			*pp;

	pp = XFS_REFCOUNT_PTR_ADDR(block, 1, mp->m_refc_mxr[1]);
	return be32_to_cpu(*pp);
}

/* Count the blocks in the reference count tree. */
static int
xfs_refcountbt_count_tree_blocks(
	struct xfs_mount	*mp,
	xfs_agnumber_t		agno,
	xfs_extlen_t		*tree_blocks)
{
	return xfs_btree_count_blocks(mp, xfs_refcountbt_cb_getroot,
			xfs_refcountbt_cb_getptr, &xfs_refcountbt_buf_ops, agno,
			tree_blocks);
}

/*
 * Create reserved block pools for each allocation group.
 */
int
xfs_refcountbt_alloc_reserve_pool(
	struct xfs_mount	*mp)
{
	xfs_agnumber_t		agno;
	struct xfs_perag	*pag;
	xfs_extlen_t		pool_len;
	xfs_extlen_t		tree_len;
	int			error = 0;
	int			err;

	if (!xfs_sb_version_hasreflink(&mp->m_sb))
		return 0;

	pool_len = xfs_refcountbt_max_size(mp);
	xfs_ag_resv_type_init(mp, pool_len);

	for (agno = 0; agno < mp->m_sb.sb_agcount; agno++) {
		pag = xfs_perag_get(mp, agno);
		if (pag->pagf_refcountbt_resv) {
			xfs_perag_put(pag);
			continue;
		}
		tree_len = 0;
		err = xfs_refcountbt_count_tree_blocks(mp, agno, &tree_len);
		if (err && !error)
			error = err;
		err = xfs_ag_resv_init(mp, pag, pool_len, tree_len, 0,
				&pag->pagf_refcountbt_resv);
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
xfs_refcountbt_free_reserve_pool(
	struct xfs_mount	*mp)
{
	xfs_agnumber_t		agno;
	struct xfs_perag	*pag;
	xfs_extlen_t		pool_len, i;
	int			error = 0;
	int			err;

	if (!xfs_sb_version_hasreflink(&mp->m_sb))
		return 0;

	pool_len = 0;
	for (agno = 0; agno < mp->m_sb.sb_agcount; agno++) {
		pag = xfs_perag_get(mp, agno);
		if (!pag->pagf_refcountbt_resv) {
			xfs_perag_put(pag);
			continue;
		}
		i = xfs_ag_resv_blocks(pag->pagf_refcountbt_resv);
		if (pool_len < i)
			pool_len = i;
		err = xfs_ag_resv_free(pag->pagf_refcountbt_resv, pag);
		pag->pagf_refcountbt_resv = NULL;
		xfs_perag_put(pag);
		if (err && !error)
			error = err;
	}
	xfs_ag_resv_type_free(mp, pool_len);

	return error;
}
