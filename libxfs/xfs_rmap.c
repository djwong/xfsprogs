
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
#include "xfs_da_format.h"
#include "xfs_da_btree.h"
#include "xfs_btree.h"
#include "xfs_trans.h"
#include "xfs_alloc.h"
#include "xfs_rmap_btree.h"
#include "xfs_trans_space.h"
#include "xfs_trace.h"


/*
 * Lookup the first record less than or equal to [bno, len]
 * in the btree given by cur.
 */
STATIC int
xfs_rmap_lookup_le(
	struct xfs_btree_cur	*cur,
	xfs_agblock_t		bno,
	xfs_extlen_t		len,
	uint64_t		owner,
	int			*stat)
{
	cur->bc_rec.r.rm_startblock = bno;
	cur->bc_rec.r.rm_blockcount = len;
	cur->bc_rec.r.rm_owner = owner;
	return xfs_btree_lookup(cur, XFS_LOOKUP_LE, stat);
}

/*
 * Update the record referred to by cur to the value given
 * by [bno, len, ref].
 * This either works (return 0) or gets an EFSCORRUPTED error.
 */
STATIC int
xfs_rmap_update(
	struct xfs_btree_cur	*cur,
	struct xfs_rmap_irec	*irec)
{
	union xfs_btree_rec	rec;

	rec.rmap.rm_startblock = cpu_to_be32(irec->rm_startblock);
	rec.rmap.rm_blockcount = cpu_to_be32(irec->rm_blockcount);
	rec.rmap.rm_owner = cpu_to_be64(irec->rm_owner);
	return xfs_btree_update(cur, &rec);
}

/*
 * Get the data from the pointed-to record.
 */
STATIC int
xfs_rmap_get_rec(
	struct xfs_btree_cur	*cur,
	struct xfs_rmap_irec	*irec,
	int			*stat)
{
	union xfs_btree_rec	*rec;
	int			error;

	error = xfs_btree_get_rec(cur, &rec, stat);
	if (error || !*stat)
		return error;

	irec->rm_startblock = be32_to_cpu(rec->rmap.rm_startblock);
	irec->rm_blockcount = be32_to_cpu(rec->rmap.rm_blockcount);
	irec->rm_owner = be64_to_cpu(rec->rmap.rm_owner);
	return 0;
}

/*
 * Find the extent in the rmap btree and remove it.
 *
 * The record we find should always span a range greater than or equal to the
 * the extent being freed. This makes the code simple as, in theory, we do not
 * have to handle ranges that are split across multiple records as extents that
 * result in bmap btree extent merges should also result in rmap btree extent
 * merges.  The owner field ensures we don't merge extents from different
 * structures into the same record, hence this property should always hold true
 * if we ensure that the rmap btree supports at least the same size maximum
 * extent as the bmap btree (2^21 blocks at present).
 *
 * Complexity: when growing the filesystem, we "free" an extent when growing the
 * last AG. This extent is new space and so it is not tracked as used space in
 * the btree. The growfs code will pass in an owner of XFS_RMAP_OWN_NULL to
 * indicate that it expected that there is no owner of this extent. We verify
 * that - the extent lookup result in a record that does not overlap.
 *
 * Complexity #2: EFIs do not record the owner of the extent, so when recovering
 * EFIs from the log we pass in XFS_RMAP_OWN_UNKNOWN to tell the rmap btree to
 * ignore the owner (i.e. wildcard match) so we don't trigger corruption checks
 * during log recovery.
 */
int
xfs_rmap_free(
	struct xfs_trans	*tp,
	struct xfs_buf		*agbp,
	xfs_agnumber_t		agno,
	xfs_agblock_t		bno,
	xfs_extlen_t		len,
	uint64_t		owner)
{
	struct xfs_btree_cur	*cur;
	struct xfs_mount	*mp = tp->t_mountp;
	struct xfs_rmap_irec	ltrec;
	int			error;
	int			i;

	/*
	 * if rmap btree is not supported, then just return success without
	 * doing anything.
	 */
	if (!xfs_sb_version_hasrmapbt(&tp->t_mountp->m_sb))
		return 0;

	trace_xfs_rmap_free_extent(mp, agno, bno, len, owner);
	cur = xfs_rmapbt_init_cursor(mp, tp, agbp, agno);

	/*
	 * We always have a left record because there's a static record
	 * for the AG headers at rm_startblock == 0.
	 */
	error = xfs_rmap_lookup_le(cur, bno, len, owner, &i);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);

	error = xfs_rmap_get_rec(cur, &ltrec, &i);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);

	/* special growfs case - bno is beyond last record */
	if (owner == XFS_RMAP_OWN_NULL) {
		XFS_WANT_CORRUPTED_GOTO(mp, bno > ltrec.rm_startblock +
						ltrec.rm_blockcount, out_error);
		goto out_done;
	}

	/* make sure the extent we found covers the entire freeing range. */
	XFS_WANT_CORRUPTED_GOTO(mp, ltrec.rm_startblock <= bno, out_error);
	XFS_WANT_CORRUPTED_GOTO(mp, ltrec.rm_blockcount >= len, out_error);

/*
	if (owner != ltrec.rm_owner ||
	    bno > ltrec.rm_startblock + ltrec.rm_blockcount)
 */
	//printk("rmfree  ag %d bno 0x%x/0x%x/0x%llx, ltrec 0x%x/0x%x/0x%llx\n",
	//		agno, bno, len, owner, ltrec.rm_startblock,
	//		ltrec.rm_blockcount, ltrec.rm_owner);
	XFS_WANT_CORRUPTED_GOTO(mp, bno <= ltrec.rm_startblock + ltrec.rm_blockcount,
				out_error);
	XFS_WANT_CORRUPTED_GOTO(mp, owner == ltrec.rm_owner ||
				(owner < XFS_RMAP_OWN_NULL &&
				 owner >= XFS_RMAP_OWN_MIN), out_error);

	/* exact match is easy */
	if (ltrec.rm_startblock == bno && ltrec.rm_blockcount == len) {
	//printk("remove exact\n");
		/* remove extent from rmap tree */
		error = xfs_btree_delete(cur, &i);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);
	} else if (ltrec.rm_startblock == bno) {
	//printk("remove left\n");
		/*
		 * overlap left hand side of extent
		 *
		 *       ltbno                ltlen
		 * Orig:    |oooooooooooooooooooo|
		 * Freeing: |fffffffff|
		 * Result:            |rrrrrrrrrr|
		 *         bno       len
		 */
		ltrec.rm_startblock += len;
		ltrec.rm_blockcount -= len;
		error = xfs_rmap_update(cur, &ltrec);
		if (error)
			goto out_error;
	} else if (ltrec.rm_startblock + ltrec.rm_blockcount == bno + len) {
	//printk("remove right\n");
		/*
		 * overlap right hand side of extent
		 *
		 *       ltbno                ltlen
		 * Orig:    |oooooooooooooooooooo|
		 * Freeing:            |fffffffff|
		 * Result:  |rrrrrrrrrr|
		 *                    bno       len
		 */
		ltrec.rm_blockcount -= len;
		error = xfs_rmap_update(cur, &ltrec);
		if (error)
			goto out_error;
	} else {
		/*
		 * overlap middle of extent
		 *
		 *       ltbno                ltlen
		 * Orig:    |oooooooooooooooooooo|
		 * Freeing:       |fffffffff|
		 * Result:  |rrrrr|         |rrrr|
		 *               bno       len
		 */
		xfs_extlen_t	orig_len = ltrec.rm_blockcount;
	//printk("remove middle\n");

		ltrec.rm_blockcount = bno - ltrec.rm_startblock;;
		error = xfs_rmap_update(cur, &ltrec);
		if (error)
			goto out_error;

		error = xfs_btree_increment(cur, 0, &i);
		if (error)
			goto out_error;

		cur->bc_rec.r.rm_startblock = bno + len;
		cur->bc_rec.r.rm_blockcount = orig_len - len -
						     ltrec.rm_blockcount;
		cur->bc_rec.r.rm_owner = ltrec.rm_owner;
		error = xfs_btree_insert(cur, &i);
		if (error)
			goto out_error;
	}

out_done:
	trace_xfs_rmap_free_extent_done(mp, agno, bno, len, owner);
	xfs_btree_del_cursor(cur, XFS_BTREE_NOERROR);
	return 0;

out_error:
	trace_xfs_rmap_free_extent_error(mp, agno, bno, len, owner);
	xfs_btree_del_cursor(cur, XFS_BTREE_ERROR);
	return error;
}

/*
 * When we allocate a new block, the first thing we do is add a reference to the
 * extent in the rmap btree. This is how we track the owner of the extent and th
 * enumber of references to it.
 *
 * Initially, we do not have shared extents, and so the extent can only have a
 * single reference count and owner. This makes the initial implementation easy,
 * but does not allow us to use the rmap tree for tracking reflink shared files.
 * Hence the initial implementation is simply a lookup to find the place to
 * insert (and checking we don't find a duplicate/overlap) and then insertng the
 * appropriate record.
 */
int
xfs_rmap_alloc(
	struct xfs_trans	*tp,
	struct xfs_buf		*agbp,
	xfs_agnumber_t		agno,
	xfs_agblock_t		bno,
	xfs_extlen_t		len,
	uint64_t		owner)
{
	struct xfs_btree_cur	*cur;
	struct xfs_mount	*mp = tp->t_mountp;
	struct xfs_rmap_irec	ltrec;
	struct xfs_rmap_irec	gtrec;
	int			have_gt;
	int			error;
	int			i;

	/*
	 * if rmap btree is not supported, then just return success without
	 * doing anything.
	 */
	if (!xfs_sb_version_hasrmapbt(&tp->t_mountp->m_sb))
		return 0;

	trace_xfs_rmap_alloc_extent(mp, agno, bno, len, owner);
	cur = xfs_rmapbt_init_cursor(mp, tp, agbp, agno);

	/*
	 * chekc to see if we find an existing record for this extent rather
	 * than just the location for insert.
	 */
	error = xfs_rmap_lookup_le(cur, bno, len, owner, &i);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);

	error = xfs_rmap_get_rec(cur, &ltrec, &i);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);
	//printk("rmalloc ag %d bno 0x%x/0x%x/0x%llx, ltrec 0x%x/0x%x/0x%llx\n",
	//		agno, bno, len, owner, ltrec.rm_startblock,
	//		ltrec.rm_blockcount, ltrec.rm_owner);

	XFS_WANT_CORRUPTED_GOTO(mp, ltrec.rm_startblock + ltrec.rm_blockcount <= bno,
				out_error);

	error = xfs_btree_increment(cur, 0, &have_gt);
	if (error)
		goto out_error;
	if (have_gt) {
		error = xfs_rmap_get_rec(cur, &gtrec, &i);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);
	//printk("rmalloc ag %d bno 0x%x/0x%x/0x%llx, gtrec 0x%x/0x%x/0x%llx\n",
	//		agno, bno, len, owner, gtrec.rm_startblock,
	//		gtrec.rm_blockcount, gtrec.rm_owner);
		XFS_WANT_CORRUPTED_GOTO(mp, bno + len <= gtrec.rm_startblock,
					out_error);
	} else {
		gtrec.rm_owner = XFS_RMAP_OWN_NULL;
	}

	/* cursor currently points one record past ltrec */
	if (ltrec.rm_owner == owner &&
	    ltrec.rm_startblock + ltrec.rm_blockcount == bno) {
		/*
		 * left edge contiguous
		 *
		 *       ltbno     ltlen
		 * orig:   |ooooooooo|
		 * adding:           |aaaaaaaaa|
		 * result: |rrrrrrrrrrrrrrrrrrr|
		 *                  bno       len
		 */
		//printk("add left\n");
		ltrec.rm_blockcount += len;
		if (gtrec.rm_owner == owner &&
		    bno + len == gtrec.rm_startblock) {
			//printk("add middle\n");
			/*
			 * right edge also contiguous
			 *
			 *       ltbno     ltlen    gtbno     gtlen
			 * orig:   |ooooooooo|         |ooooooooo|
			 * adding:           |aaaaaaaaa|
			 * result: |rrrrrrrrrrrrrrrrrrrrrrrrrrrrr|
			 */
			ltrec.rm_blockcount += gtrec.rm_blockcount;
			error = xfs_btree_delete(cur, &i);
			if (error)
				goto out_error;
			XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);
		}

		error = xfs_btree_decrement(cur, 0, &have_gt);
		if (error)
			goto out_error;
		error = xfs_rmap_update(cur, &ltrec);
		if (error)
			goto out_error;
	} else if (gtrec.rm_owner == owner &&
		   bno + len == gtrec.rm_startblock) {
		/*
		 * right edge contiguous
		 *
		 *                 gtbno     gtlen
		 * Orig:             |ooooooooo|
		 * adding: |aaaaaaaaa|
		 * Result: |rrrrrrrrrrrrrrrrrrr|
		 *        bno       len
		 */
		//printk("add right\n");
		gtrec.rm_startblock = bno;
		gtrec.rm_blockcount += len;
		error = xfs_rmap_update(cur, &gtrec);
		if (error)
			goto out_error;
	} else {
		//printk("add no match\n");
		/* no contiguous edge with identical owner */
		cur->bc_rec.r.rm_startblock = bno;
		cur->bc_rec.r.rm_blockcount = len;
		cur->bc_rec.r.rm_owner = owner;
		error = xfs_btree_insert(cur, &i);
		if (error)
			goto out_error;
	}

	trace_xfs_rmap_alloc_extent_done(mp, agno, bno, len, owner);
	xfs_btree_del_cursor(cur, XFS_BTREE_NOERROR);
	return 0;

out_error:
	trace_xfs_rmap_alloc_extent_error(mp, agno, bno, len, owner);
	xfs_btree_del_cursor(cur, XFS_BTREE_ERROR);
	return error;
}
