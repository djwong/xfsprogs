
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
#include "xfs_bmap.h"
#include "xfs_bmap_btree.h"

/*
 * Lookup the first record less than or equal to [bno, len, owner, offset]
 * in the btree given by cur.
 */
int
xfs_rmap_lookup_le(
	struct xfs_btree_cur	*cur,
	xfs_agblock_t		bno,
	xfs_extlen_t		len,
	uint64_t		owner,
	uint64_t		offset,
	int			*stat)
{
	cur->bc_rec.r.rm_startblock = bno;
	cur->bc_rec.r.rm_blockcount = len;
	cur->bc_rec.r.rm_owner = owner;
	cur->bc_rec.r.rm_offset = offset;
	return xfs_btree_lookup(cur, XFS_LOOKUP_LE, stat);
}

/*
 * Lookup the record exactly matching [bno, len, owner, offset]
 * in the btree given by cur.
 */
int
xfs_rmap_lookup_eq(
	struct xfs_btree_cur	*cur,
	xfs_agblock_t		bno,
	xfs_extlen_t		len,
	uint64_t		owner,
	uint64_t		offset,
	int			*stat)
{
	cur->bc_rec.r.rm_startblock = bno;
	cur->bc_rec.r.rm_blockcount = len;
	cur->bc_rec.r.rm_owner = owner;
	cur->bc_rec.r.rm_offset = offset;
	return xfs_btree_lookup(cur, XFS_LOOKUP_EQ, stat);
}

/*
 * Update the record referred to by cur to the value given
 * by [bno, len, owner, offset].
 * This either works (return 0) or gets an EFSCORRUPTED error.
 */
STATIC int
xfs_rmap_update(
	struct xfs_btree_cur	*cur,
	struct xfs_rmap_irec	*irec)
{
	union xfs_btree_rec	rec;

	trace_xfs_rmapbt_update(cur->bc_mp, cur->bc_private.a.agno,
			irec->rm_startblock, irec->rm_blockcount,
			irec->rm_owner, irec->rm_offset);

	rec.rmap.rm_startblock = cpu_to_be32(irec->rm_startblock);
	rec.rmap.rm_blockcount = cpu_to_be32(irec->rm_blockcount);
	rec.rmap.rm_owner = cpu_to_be64(irec->rm_owner);
	rec.rmap.rm_offset = cpu_to_be64(irec->rm_offset);
	return xfs_btree_update(cur, &rec);
}

int
xfs_rmapbt_insert(
	struct xfs_btree_cur	*rcur,
	xfs_agblock_t		agbno,
	xfs_extlen_t		len,
	uint64_t		owner,
	uint64_t		offset)
{
	int			i;
	int			error;

	trace_xfs_rmapbt_insert(rcur->bc_mp, rcur->bc_private.a.agno, agbno,
			len, owner, offset);

	error = xfs_rmap_lookup_eq(rcur, agbno, len, owner, offset, &i);
	if (error)
		goto done;
	XFS_WANT_CORRUPTED_GOTO(rcur->bc_mp, i == 0, done);

	rcur->bc_rec.r.rm_startblock = agbno;
	rcur->bc_rec.r.rm_blockcount = len;
	rcur->bc_rec.r.rm_owner = owner;
	rcur->bc_rec.r.rm_offset = offset;
	error = xfs_btree_insert(rcur, &i);
	if (error)
		goto done;
	XFS_WANT_CORRUPTED_GOTO(rcur->bc_mp, i == 1, done);
done:
	return error;
}

STATIC int
xfs_rmapbt_delete(
	struct xfs_btree_cur	*rcur,
	xfs_agblock_t		agbno,
	xfs_extlen_t		len,
	uint64_t		owner,
	uint64_t		offset)
{
	int			i;
	int			error;

	trace_xfs_rmapbt_delete(rcur->bc_mp, rcur->bc_private.a.agno, agbno,
			len, owner, offset);

	error = xfs_rmap_lookup_eq(rcur, agbno, len, owner, offset, &i);
	if (error)
		goto done;
	XFS_WANT_CORRUPTED_GOTO(rcur->bc_mp, i == 1, done);

	error = xfs_btree_delete(rcur, &i);
	if (error)
		goto done;
	XFS_WANT_CORRUPTED_GOTO(rcur->bc_mp, i == 1, done);
done:
	return error;
}

/*
 * Get the data from the pointed-to record.
 */
int
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
	irec->rm_offset = be64_to_cpu(rec->rmap.rm_offset);
	return 0;
}

/*
 * Find the extent in the rmap btree and remove it.
 *
 * The record we find should always be an exact match for the extent that we're
 * looking for, since we insert them into the btree without modification.
 *
 * Special Case #1: when growing the filesystem, we "free" an extent when
 * growing the last AG. This extent is new space and so it is not tracked as
 * used space in the btree. The growfs code will pass in an owner of
 * XFS_RMAP_OWN_NULL to indicate that it expected that there is no owner of this
 * extent. We verify that - the extent lookup result in a record that does not
 * overlap.
 *
 * Special Case #2: EFIs do not record the owner of the extent, so when
 * recovering EFIs from the log we pass in XFS_RMAP_OWN_UNKNOWN to tell the rmap
 * btree to ignore the owner (i.e. wildcard match) so we don't trigger
 * corruption checks during log recovery.
 */
int
xfs_rmap_free(
	struct xfs_trans	*tp,
	struct xfs_buf		*agbp,
	xfs_agnumber_t		agno,
	xfs_agblock_t		bno,
	xfs_extlen_t		len,
	struct xfs_owner_info	*oinfo)
{
	struct xfs_mount	*mp = tp->t_mountp;
	struct xfs_btree_cur	*cur;
	struct xfs_rmap_irec	ltrec;
	uint64_t		ltoff;
	int			error = 0;
	int			i;
	uint64_t		owner;
	uint64_t		offset;

	if (!xfs_sb_version_hasrmapbt(&mp->m_sb))
		return 0;

	trace_xfs_rmap_free_extent(mp, agno, bno, len, oinfo);
	cur = xfs_rmapbt_init_cursor(mp, tp, agbp, agno);

	xfs_owner_info_unpack(oinfo, &owner, &offset);

	/*
	 * We should always have a left record because there's a static record
	 * for the AG headers at rm_startblock == 0 created by mkfs/growfs that
	 * will not ever be removed from the tree.
	 */
	error = xfs_rmap_lookup_le(cur, bno, len, owner, offset, &i);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);

	error = xfs_rmap_get_rec(cur, &ltrec, &i);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);
	ltoff = ltrec.rm_offset & ~XFS_RMAP_OFF_BMBT;

	/*
	 * For growfs, the incoming extent must be beyond the left record we
	 * just found as it is new space and won't be used by anyone. This is
	 * just a corruption check as we don't actually do anything with this
	 * extent.
	 */
	if (owner == XFS_RMAP_OWN_NULL) {
		XFS_WANT_CORRUPTED_GOTO(mp, bno > ltrec.rm_startblock +
						ltrec.rm_blockcount, out_error);
		goto out_done;
	}

/*
	if (owner != ltrec.rm_owner ||
	    bno > ltrec.rm_startblock + ltrec.rm_blockcount)
 */
	//printk("rmfree  ag %d bno 0x%x/0x%x/0x%llx, ltrec 0x%x/0x%x/0x%llx\n",
	//		agno, bno, len, owner, ltrec.rm_startblock,
	//		ltrec.rm_blockcount, ltrec.rm_owner);

	/* make sure the extent we found covers the entire freeing range. */
	XFS_WANT_CORRUPTED_GOTO(mp, !XFS_RMAP_IS_UNWRITTEN(ltrec.rm_blockcount),
		out_error);
	XFS_WANT_CORRUPTED_GOTO(mp, ltrec.rm_startblock <= bno &&
		ltrec.rm_startblock + XFS_RMAP_LEN(ltrec.rm_blockcount) >=
		bno + len, out_error);

	/* make sure the owner matches what we expect to find in the tree */
	XFS_WANT_CORRUPTED_GOTO(mp, owner == ltrec.rm_owner ||
				    XFS_RMAP_NON_INODE_OWNER(owner), out_error);

	/* check the offset, if necessary */
	if (!XFS_RMAP_NON_INODE_OWNER(owner)) {
		if (XFS_RMAP_IS_BMBT(offset)) {
			XFS_WANT_CORRUPTED_GOTO(mp,
					XFS_RMAP_IS_BMBT(ltrec.rm_offset),
					out_error);
		} else {
			XFS_WANT_CORRUPTED_GOTO(mp,
					ltrec.rm_offset <= offset, out_error);
			XFS_WANT_CORRUPTED_GOTO(mp,
					offset <= ltoff + ltrec.rm_blockcount,
					out_error);
		}
	}

	if (ltrec.rm_startblock == bno && ltrec.rm_blockcount == len) {
	//printk("remove exact\n");
		/* exact match, simply remove the record from rmap tree */
		error = xfs_btree_delete(cur, &i);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);
	} else if (ltrec.rm_startblock == bno) {
	//printk("remove left\n");
		/*
		 * overlap left hand side of extent: move the start, trim the
		 * length and update the current record.
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
		 * overlap right hand side of extent: trim the length and update
		 * the current record.
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
		 * overlap middle of extent: trim the length of the existing
		 * record to the length of the new left-extent size, increment
		 * the insertion position so we can insert a new record
		 * containing the remaining right-extent space.
		 *
		 *       ltbno                ltlen
		 * Orig:    |oooooooooooooooooooo|
		 * Freeing:       |fffffffff|
		 * Result:  |rrrrr|         |rrrr|
		 *               bno       len
		 */
		xfs_extlen_t	orig_len = ltrec.rm_blockcount;
	//printk("remove middle\n");

		ltrec.rm_blockcount = bno - ltrec.rm_startblock;
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
		cur->bc_rec.r.rm_offset = offset;
		error = xfs_btree_insert(cur, &i);
		if (error)
			goto out_error;
	}

out_done:
	trace_xfs_rmap_free_extent_done(mp, agno, bno, len, oinfo);
	xfs_btree_del_cursor(cur, XFS_BTREE_NOERROR);
	return 0;

out_error:
	trace_xfs_rmap_free_extent_error(mp, agno, bno, len, oinfo);
	xfs_btree_del_cursor(cur, XFS_BTREE_ERROR);
	return error;
}

/*
 * A mergeable rmap should have the same owner, cannot be unwritten, and
 * must be a bmbt rmap if we're asking about a bmbt rmap.
 */
static bool
is_mergeable_rmap(
	struct xfs_rmap_irec	*irec,
	uint64_t		owner,
	uint64_t		offset)
{
	if (irec->rm_owner == XFS_RMAP_OWN_NULL)
		return false;
	if (irec->rm_owner != owner)
		return false;
	if (XFS_RMAP_IS_UNWRITTEN(irec->rm_blockcount))
		return false;
	if (XFS_RMAP_IS_ATTR_FORK(offset) ^
	    XFS_RMAP_IS_ATTR_FORK(irec->rm_offset))
		return false;
	if (XFS_RMAP_IS_BMBT(offset) ^ XFS_RMAP_IS_BMBT(irec->rm_offset))
		return false;
	return true;
}

/*
 * When we allocate a new block, the first thing we do is add a reference to
 * the extent in the rmap btree. This takes the form of a [agbno, length,
 * owner, offset] record.  Flags are encoded in the high bits of the offset
 * field.
 */
int
xfs_rmap_alloc(
	struct xfs_trans	*tp,
	struct xfs_buf		*agbp,
	xfs_agnumber_t		agno,
	xfs_agblock_t		bno,
	xfs_extlen_t		len,
	struct xfs_owner_info	*oinfo)
{
	struct xfs_mount	*mp = tp->t_mountp;
	struct xfs_btree_cur	*cur;
	struct xfs_rmap_irec	ltrec;
	struct xfs_rmap_irec	gtrec;
	int			have_gt;
	int			error = 0;
	int			i;
	uint64_t		owner;
	uint64_t		offset;

	if (!xfs_sb_version_hasrmapbt(&mp->m_sb))
		return 0;

	trace_xfs_rmap_alloc_extent(mp, agno, bno, len, oinfo);
	cur = xfs_rmapbt_init_cursor(mp, tp, agbp, agno);

	xfs_owner_info_unpack(oinfo, &owner, &offset);

	/*
	 * For the initial lookup, look for and exact match or the left-adjacent
	 * record for our insertion point. This will also give us the record for
	 * start block contiguity tests.
	 */
	error = xfs_rmap_lookup_le(cur, bno, len, owner, offset, &i);
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
	if (!is_mergeable_rmap(&ltrec, owner, offset))
		ltrec.rm_owner = XFS_RMAP_OWN_NULL;

	XFS_WANT_CORRUPTED_GOTO(mp,
		ltrec.rm_owner == XFS_RMAP_OWN_NULL ||
		ltrec.rm_startblock + ltrec.rm_blockcount <= bno, out_error);

	/*
	 * Increment the cursor to see if we have a right-adjacent record to our
	 * insertion point. This will give us the record for end block
	 * contiguity tests.
	 */
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
	if (!is_mergeable_rmap(&gtrec, owner, offset))
		gtrec.rm_owner = XFS_RMAP_OWN_NULL;

	/*
	 * Note: cursor currently points one record to the right of ltrec, even
	 * if there is no record in the tree to the right.
	 */
	if (ltrec.rm_owner == owner &&
	    ltrec.rm_startblock + ltrec.rm_blockcount == bno) {
		/*
		 * left edge contiguous, merge into left record.
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
			 * right edge also contiguous, delete right record
			 * and merge into left record.
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

		/* point the cursor back to the left record and update */
		error = xfs_btree_decrement(cur, 0, &have_gt);
		if (error)
			goto out_error;
		error = xfs_rmap_update(cur, &ltrec);
		if (error)
			goto out_error;
	} else if (gtrec.rm_owner == owner &&
		   bno + len == gtrec.rm_startblock) {
		/*
		 * right edge contiguous, merge into right record.
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
		/*
		 * no contiguous edge with identical owner, insert
		 * new record at current cursor position.
		 */
		cur->bc_rec.r.rm_startblock = bno;
		cur->bc_rec.r.rm_blockcount = len;
		cur->bc_rec.r.rm_owner = owner;
		cur->bc_rec.r.rm_offset = offset;
		error = xfs_btree_insert(cur, &i);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);
	}

	trace_xfs_rmap_alloc_extent_done(mp, agno, bno, len, oinfo);
	xfs_btree_del_cursor(cur, XFS_BTREE_NOERROR);
	return 0;

out_error:
	trace_xfs_rmap_alloc_extent_error(mp, agno, bno, len, oinfo);
	xfs_btree_del_cursor(cur, XFS_BTREE_ERROR);
	return error;
}

/* Encode logical offset for a rmapbt record */
STATIC uint64_t
b2r_off(
	int		whichfork,
	xfs_fileoff_t	off)
{
	uint64_t	x;

	x = off;
	if (whichfork == XFS_ATTR_FORK)
		x |= XFS_RMAP_OFF_ATTR;
	return x;
}

/* Encode blockcount for a rmapbt record */
STATIC xfs_extlen_t
b2r_len(
	struct xfs_bmbt_irec	*irec)
{
	xfs_extlen_t		x;

	x = irec->br_blockcount;
	if (irec->br_state == XFS_EXT_UNWRITTEN)
		x |= XFS_RMAP_LEN_UNWRITTEN;
	return x;
}

static int
__xfs_rmap_move(
	struct xfs_btree_cur	*rcur,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*PREV,
	long			start_adj);

static int
__xfs_rmap_resize(
	struct xfs_btree_cur	*rcur,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*PREV,
	long			size_adj);

/* Combine two adjacent rmap extents */
static int
__xfs_rmap_combine(
	struct xfs_btree_cur	*rcur,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*LEFT,
	struct xfs_bmbt_irec	*RIGHT,
	struct xfs_bmbt_irec	*PREV)
{
	int			error;

	if (!rcur)
		return 0;

	trace_xfs_rmap_combine(rcur->bc_mp, rcur->bc_private.a.agno, ino,
			whichfork, LEFT, PREV, RIGHT);

	/* Delete right rmap */
	error = xfs_rmapbt_delete(rcur,
			XFS_FSB_TO_AGBNO(rcur->bc_mp, RIGHT->br_startblock),
			b2r_len(RIGHT), ino,
			b2r_off(whichfork, RIGHT->br_startoff));
	if (error)
		goto done;

	/* Delete prev rmap */
	if (!isnullstartblock(PREV->br_startblock)) {
		error = xfs_rmapbt_delete(rcur,
				XFS_FSB_TO_AGBNO(rcur->bc_mp,
						PREV->br_startblock),
				b2r_len(PREV), ino,
				b2r_off(whichfork, PREV->br_startoff));
		if (error)
			goto done;
	}

	/* Enlarge left rmap */
	return __xfs_rmap_resize(rcur, ino, whichfork, LEFT,
			PREV->br_blockcount + RIGHT->br_blockcount);
done:
	return error;
}

/* Extend a left rmap extent */
static int
__xfs_rmap_lcombine(
	struct xfs_btree_cur	*rcur,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*LEFT,
	struct xfs_bmbt_irec	*PREV)
{
	int			error;

	if (!rcur)
		return 0;

	trace_xfs_rmap_lcombine(rcur->bc_mp, rcur->bc_private.a.agno, ino,
			whichfork, LEFT, PREV);

	/* Delete prev rmap */
	if (!isnullstartblock(PREV->br_startblock)) {
		error = xfs_rmapbt_delete(rcur,
				XFS_FSB_TO_AGBNO(rcur->bc_mp,
						PREV->br_startblock),
				b2r_len(PREV), ino,
				b2r_off(whichfork, PREV->br_startoff));
		if (error)
			goto done;
	}

	/* Enlarge left rmap */
	return __xfs_rmap_resize(rcur, ino, whichfork, LEFT,
			PREV->br_blockcount);
done:
	return error;
}

/* Extend a right rmap extent */
static int
__xfs_rmap_rcombine(
	struct xfs_btree_cur	*rcur,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*RIGHT,
	struct xfs_bmbt_irec	*PREV)
{
	int			error;

	if (!rcur)
		return 0;

	trace_xfs_rmap_rcombine(rcur->bc_mp, rcur->bc_private.a.agno, ino,
			whichfork, RIGHT, PREV);

	/* Delete prev rmap */
	if (!isnullstartblock(PREV->br_startblock)) {
		error = xfs_rmapbt_delete(rcur,
				XFS_FSB_TO_AGBNO(rcur->bc_mp,
						PREV->br_startblock),
				b2r_len(PREV), ino,
				b2r_off(whichfork, PREV->br_startoff));
		if (error)
			goto done;
	}

	/* Enlarge right rmap */
	return __xfs_rmap_move(rcur, ino, whichfork, RIGHT,
			-PREV->br_blockcount);
done:
	return error;
}

/* Insert a rmap extent */
static int
__xfs_rmap_insert(
	struct xfs_btree_cur	*rcur,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*rec)
{
	if (!rcur)
		return 0;

	trace_xfs_rmap_insert(rcur->bc_mp, rcur->bc_private.a.agno, ino,
			whichfork, rec);

	return xfs_rmapbt_insert(rcur,
			XFS_FSB_TO_AGBNO(rcur->bc_mp, rec->br_startblock),
			b2r_len(rec), ino,
			b2r_off(whichfork, rec->br_startoff));
}

/* Delete a rmap extent */
static int
__xfs_rmap_delete(
	struct xfs_btree_cur	*rcur,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*rec)
{
	if (!rcur)
		return 0;

	trace_xfs_rmap_delete(rcur->bc_mp, rcur->bc_private.a.agno, ino,
			whichfork, rec);

	return xfs_rmapbt_delete(rcur,
			XFS_FSB_TO_AGBNO(rcur->bc_mp, rec->br_startblock),
			b2r_len(rec), ino,
			b2r_off(whichfork, rec->br_startoff));
}

/* Change the start of an rmap */
static int
__xfs_rmap_move(
	struct xfs_btree_cur	*rcur,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*PREV,
	long			start_adj)
{
	int			error;
	struct xfs_bmbt_irec	irec;

	if (!rcur)
		return 0;

	trace_xfs_rmap_move(rcur->bc_mp, rcur->bc_private.a.agno, ino,
			whichfork, PREV, start_adj);

	/* Delete prev rmap */
	error = xfs_rmapbt_delete(rcur,
			XFS_FSB_TO_AGBNO(rcur->bc_mp, PREV->br_startblock),
			b2r_len(PREV), ino,
			b2r_off(whichfork, PREV->br_startoff));
	if (error)
		goto done;

	/* Re-add rmap with new start */
	irec = *PREV;
	irec.br_startblock += start_adj;
	irec.br_startoff += start_adj;
	irec.br_blockcount -= start_adj;
	return xfs_rmapbt_insert(rcur,
			XFS_FSB_TO_AGBNO(rcur->bc_mp, irec.br_startblock),
			b2r_len(&irec), ino,
			b2r_off(whichfork, irec.br_startoff));
done:
	return error;
}

/* Change the logical offset of an rmap */
static int
__xfs_rmap_slide(
	struct xfs_btree_cur	*rcur,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*PREV,
	long			start_adj)
{
	int			error;

	if (!rcur)
		return 0;

	trace_xfs_rmap_slide(rcur->bc_mp, rcur->bc_private.a.agno, ino,
			whichfork, PREV, start_adj);

	/* Delete prev rmap */
	error = xfs_rmapbt_delete(rcur,
			XFS_FSB_TO_AGBNO(rcur->bc_mp, PREV->br_startblock),
			b2r_len(PREV), ino,
			b2r_off(whichfork, PREV->br_startoff));
	if (error)
		goto done;

	/* Re-add rmap with new logical offset */
	return xfs_rmapbt_insert(rcur,
			XFS_FSB_TO_AGBNO(rcur->bc_mp, PREV->br_startblock),
			b2r_len(PREV), ino,
			b2r_off(whichfork, PREV->br_startoff + start_adj));
done:
	return error;
}

/* Change the size of an rmap */
static int
__xfs_rmap_resize(
	struct xfs_btree_cur	*rcur,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*PREV,
	long			size_adj)
{
	int			i;
	int			error;
	struct xfs_bmbt_irec	irec;
	struct xfs_rmap_irec	rrec;

	if (!rcur)
		return 0;

	trace_xfs_rmap_resize(rcur->bc_mp, rcur->bc_private.a.agno, ino,
			whichfork, PREV, size_adj);

	error = xfs_rmap_lookup_eq(rcur,
			XFS_FSB_TO_AGBNO(rcur->bc_mp, PREV->br_startblock),
			b2r_len(PREV), ino,
			b2r_off(whichfork, PREV->br_startoff), &i);
	if (error)
		goto done;
	XFS_WANT_CORRUPTED_GOTO(rcur->bc_mp, i == 1, done);
	error = xfs_rmap_get_rec(rcur, &rrec, &i);
	if (error)
		goto done;
	XFS_WANT_CORRUPTED_GOTO(rcur->bc_mp, i == 1, done);
	irec = *PREV;
	irec.br_blockcount += size_adj;
	rrec.rm_blockcount = b2r_len(&irec);
	error = xfs_rmap_update(rcur, &rrec);
	if (error)
		goto done;
done:
	return error;
}

/*
 * Free up any items left in the list.
 */
void
xfs_rmap_cancel(
	struct xfs_rmap_list	*rlist)	/* list of bmap_free_items */
{
	struct xfs_rmap_intent	*free;	/* free list item */
	struct xfs_rmap_intent	*next;

	if (rlist->rl_count == 0)
		return;
	ASSERT(rlist->rl_first != NULL);
	for (free = rlist->rl_first; free; free = next) {
		next = free->ri_next;
		kmem_free(free);
	}
	rlist->rl_count = 0;
	rlist->rl_first = NULL;
}

static xfs_agnumber_t
rmap_ag(
	struct xfs_mount	*mp,
	struct xfs_rmap_intent	*ri)
{
	switch (ri->ri_type) {
	case XFS_RMAP_COMBINE:
	case XFS_RMAP_LCOMBINE:
		return XFS_FSB_TO_AGNO(mp, ri->ri_u.a.left.br_startblock);
	case XFS_RMAP_RCOMBINE:
		return XFS_FSB_TO_AGNO(mp, ri->ri_u.a.right.br_startblock);
	case XFS_RMAP_INSERT:
	case XFS_RMAP_DELETE:
	case XFS_RMAP_MOVE:
	case XFS_RMAP_SLIDE:
	case XFS_RMAP_RESIZE:
		return XFS_FSB_TO_AGNO(mp, ri->ri_prev.br_startblock);
	default:
		ASSERT(0);
	}
	return 0; /* shut up, gcc */
}

/*
 * Free up any items left in the extent list, using the given transaction.
 */
int
__xfs_rmap_finish(
	struct xfs_mount	*mp,
	struct xfs_trans	*tp,
	struct xfs_rmap_list	*rlist)
{
	struct xfs_rmap_intent	*free;	/* free list item */
	struct xfs_rmap_intent	*next;
	struct xfs_btree_cur	*rcur = NULL;
	struct xfs_buf		*agbp = NULL;
	int			error = 0;
	xfs_agnumber_t		agno;

	if (rlist->rl_count == 0)
		return 0;

	ASSERT(rlist->rl_first != NULL);
	for (free = rlist->rl_first; free; free = next) {
		agno = rmap_ag(mp, free);
		ASSERT(agno != NULLAGNUMBER);
		if (rcur && agno < rcur->bc_private.a.agno) {
			error = -EFSCORRUPTED;
			break;
		}

		ASSERT(rcur == NULL || agno >= rcur->bc_private.a.agno);
		if (rcur == NULL || agno > rcur->bc_private.a.agno) {
			if (rcur) {
				xfs_btree_del_cursor(rcur, XFS_BTREE_NOERROR);
				xfs_trans_brelse(tp, agbp);
			}

			error = xfs_alloc_read_agf(mp, tp, agno, 0, &agbp);
			if (error)
				break;

			rcur = xfs_rmapbt_init_cursor(mp, tp, agbp, agno);
			if (!rcur) {
				xfs_trans_brelse(tp, agbp);
				error = -ENOMEM;
				break;
			}
		}

		switch (free->ri_type) {
		case XFS_RMAP_COMBINE:
			error = __xfs_rmap_combine(rcur, free->ri_ino,
					free->ri_whichfork, &free->ri_u.a.left,
					&free->ri_u.a.right, &free->ri_prev);
			break;
		case XFS_RMAP_LCOMBINE:
			error = __xfs_rmap_lcombine(rcur, free->ri_ino,
					free->ri_whichfork, &free->ri_u.a.left,
					&free->ri_prev);
			break;
		case XFS_RMAP_RCOMBINE:
			error = __xfs_rmap_rcombine(rcur, free->ri_ino,
					free->ri_whichfork, &free->ri_u.a.right,
					&free->ri_prev);
			break;
		case XFS_RMAP_INSERT:
			error = __xfs_rmap_insert(rcur, free->ri_ino,
					free->ri_whichfork, &free->ri_prev);
			break;
		case XFS_RMAP_DELETE:
			error = __xfs_rmap_delete(rcur, free->ri_ino,
					free->ri_whichfork, &free->ri_prev);
			break;
		case XFS_RMAP_MOVE:
			error = __xfs_rmap_move(rcur, free->ri_ino,
					free->ri_whichfork, &free->ri_prev,
					free->ri_u.b.adj);
			break;
		case XFS_RMAP_SLIDE:
			error = __xfs_rmap_slide(rcur, free->ri_ino,
					free->ri_whichfork, &free->ri_prev,
					free->ri_u.b.adj);
			break;
		case XFS_RMAP_RESIZE:
			error = __xfs_rmap_resize(rcur, free->ri_ino,
					free->ri_whichfork, &free->ri_prev,
					free->ri_u.b.adj);
			break;
		default:
			ASSERT(0);
		}

		if (error)
			break;
		next = free->ri_next;
		kmem_free(free);
	}

	if (rcur)
		xfs_btree_del_cursor(rcur, error ? XFS_BTREE_ERROR :
				XFS_BTREE_NOERROR);
	if (agbp)
		xfs_trans_brelse(tp, agbp);

	for (; free; free = next) {
		next = free->ri_next;
		kmem_free(free);
	}

	rlist->rl_count = 0;
	rlist->rl_first = NULL;
	return error;
}

/*
 * Free up any items left in the intent list.
 */
int
xfs_rmap_finish(
	struct xfs_mount	*mp,
	struct xfs_trans	**tpp,
	struct xfs_inode	*ip,
	struct xfs_rmap_list	*rlist)
{
	int			error;

	if (rlist->rl_count == 0)
		return 0;

	error = xfs_trans_roll(tpp, ip);
	if (error)
		return error;

	return __xfs_rmap_finish(mp, *tpp, rlist);
}

/*
 * Record a rmap intent; the list is kept sorted first by AG and then by
 * increasing age.
 */
static int
__xfs_rmap_add(
	struct xfs_mount	*mp,
	struct xfs_rmap_list	*rlist,
	struct xfs_rmap_intent	*ri)
{
	struct xfs_rmap_intent	*cur;		/* current (next) element */
	struct xfs_rmap_intent	*new;
	struct xfs_rmap_intent	*prev;		/* previous element */
	xfs_agnumber_t		new_agno, cur_agno;

	if (!xfs_sb_version_hasrmapbt(&mp->m_sb))
		return 0;

	new = kmem_zalloc(sizeof(struct xfs_rmap_intent), KM_SLEEP | KM_NOFS);
	*new = *ri;
	new_agno = rmap_ag(mp, new);
	ASSERT(new_agno != NULLAGNUMBER);

	for (prev = NULL, cur = rlist->rl_first;
	     cur != NULL;
	     prev = cur, cur = cur->ri_next) {
		cur_agno = rmap_ag(mp, cur);
		if (cur_agno > new_agno)
			break;
	}
	if (prev)
		prev->ri_next = new;
	else
		rlist->rl_first = new;
	new->ri_next = cur;
	rlist->rl_count++;
	return 0;
}

/* Combine two adjacent rmap extents */
int
xfs_rmap_combine(
	struct xfs_mount	*mp,
	struct xfs_rmap_list	*rlist,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*left,
	struct xfs_bmbt_irec	*right,
	struct xfs_bmbt_irec	*prev)
{
	struct xfs_rmap_intent	ri;

	ri.ri_type = XFS_RMAP_COMBINE;
	ri.ri_ino = ino;
	ri.ri_whichfork = whichfork;
	ri.ri_prev = *prev;
	ri.ri_u.a.left = *left;
	ri.ri_u.a.right = *right;

	return __xfs_rmap_add(mp, rlist, &ri);
}

/* Extend a left rmap extent */
int
xfs_rmap_lcombine(
	struct xfs_mount	*mp,
	struct xfs_rmap_list	*rlist,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*LEFT,
	struct xfs_bmbt_irec	*PREV)
{
	struct xfs_rmap_intent	ri;

	ri.ri_type = XFS_RMAP_LCOMBINE;
	ri.ri_ino = ino;
	ri.ri_whichfork = whichfork;
	ri.ri_prev = *PREV;
	ri.ri_u.a.left = *LEFT;

	return __xfs_rmap_add(mp, rlist, &ri);
}

/* Extend a right rmap extent */
int
xfs_rmap_rcombine(
	struct xfs_mount	*mp,
	struct xfs_rmap_list	*rlist,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*RIGHT,
	struct xfs_bmbt_irec	*PREV)
{
	struct xfs_rmap_intent	ri;

	ri.ri_type = XFS_RMAP_RCOMBINE;
	ri.ri_ino = ino;
	ri.ri_whichfork = whichfork;
	ri.ri_prev = *PREV;
	ri.ri_u.a.right = *RIGHT;

	return __xfs_rmap_add(mp, rlist, &ri);
}

/* Insert a rmap extent */
int
xfs_rmap_insert(
	struct xfs_mount	*mp,
	struct xfs_rmap_list	*rlist,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*new)
{
	struct xfs_rmap_intent	ri;

	ri.ri_type = XFS_RMAP_INSERT;
	ri.ri_ino = ino;
	ri.ri_whichfork = whichfork;
	ri.ri_prev = *new;

	return __xfs_rmap_add(mp, rlist, &ri);
}

/* Delete a rmap extent */
int
xfs_rmap_delete(
	struct xfs_mount	*mp,
	struct xfs_rmap_list	*rlist,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*new)
{
	struct xfs_rmap_intent	ri;

	ri.ri_type = XFS_RMAP_DELETE;
	ri.ri_ino = ino;
	ri.ri_whichfork = whichfork;
	ri.ri_prev = *new;

	return __xfs_rmap_add(mp, rlist, &ri);
}

/* Change the start of an rmap */
int
xfs_rmap_move(
	struct xfs_mount	*mp,
	struct xfs_rmap_list	*rlist,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*PREV,
	long			start_adj)
{
	struct xfs_rmap_intent	ri;

	ri.ri_type = XFS_RMAP_MOVE;
	ri.ri_ino = ino;
	ri.ri_whichfork = whichfork;
	ri.ri_prev = *PREV;
	ri.ri_u.b.adj = start_adj;

	return __xfs_rmap_add(mp, rlist, &ri);
}

/* Change the logical offset of an rmap */
int
xfs_rmap_slide(
	struct xfs_mount	*mp,
	struct xfs_rmap_list	*rlist,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*PREV,
	long			start_adj)
{
	struct xfs_rmap_intent	ri;

	ri.ri_type = XFS_RMAP_SLIDE;
	ri.ri_ino = ino;
	ri.ri_whichfork = whichfork;
	ri.ri_prev = *PREV;
	ri.ri_u.b.adj = start_adj;

	return __xfs_rmap_add(mp, rlist, &ri);
}

/* Change the size of an rmap */
int
xfs_rmap_resize(
	struct xfs_mount	*mp,
	struct xfs_rmap_list	*rlist,
	xfs_ino_t		ino,
	int			whichfork,
	struct xfs_bmbt_irec	*PREV,
	long			size_adj)
{
	struct xfs_rmap_intent	ri;

	ri.ri_type = XFS_RMAP_RESIZE;
	ri.ri_ino = ino;
	ri.ri_whichfork = whichfork;
	ri.ri_prev = *PREV;
	ri.ri_u.b.adj = size_adj;

	return __xfs_rmap_add(mp, rlist, &ri);
}
