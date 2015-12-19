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
#include "xfs_refcount.h"

/*
 * Look up the first record less than or equal to [bno, len] in the btree
 * given by cur.
 */
int
xfs_refcountbt_lookup_le(
	struct xfs_btree_cur	*cur,
	xfs_agblock_t		bno,
	int			*stat)
{
	trace_xfs_refcountbt_lookup(cur->bc_mp, cur->bc_private.a.agno, bno,
			XFS_LOOKUP_LE);
	cur->bc_rec.rc.rc_startblock = bno;
	cur->bc_rec.rc.rc_blockcount = 0;
	return xfs_btree_lookup(cur, XFS_LOOKUP_LE, stat);
}

/*
 * Look up the first record greater than or equal to [bno, len] in the btree
 * given by cur.
 */
int					/* error */
xfs_refcountbt_lookup_ge(
	struct xfs_btree_cur	*cur,	/* btree cursor */
	xfs_agblock_t		bno,	/* starting block of extent */
	int			*stat)	/* success/failure */
{
	trace_xfs_refcountbt_lookup(cur->bc_mp, cur->bc_private.a.agno, bno,
			XFS_LOOKUP_GE);
	cur->bc_rec.rc.rc_startblock = bno;
	cur->bc_rec.rc.rc_blockcount = 0;
	return xfs_btree_lookup(cur, XFS_LOOKUP_GE, stat);
}

/*
 * Get the data from the pointed-to record.
 */
int
xfs_refcountbt_get_rec(
	struct xfs_btree_cur		*cur,
	struct xfs_refcount_irec	*irec,
	int				*stat)
{
	union xfs_btree_rec	*rec;
	int			error;

	error = xfs_btree_get_rec(cur, &rec, stat);
	if (!error && *stat == 1) {
		irec->rc_startblock = be32_to_cpu(rec->refc.rc_startblock);
		irec->rc_blockcount = be32_to_cpu(rec->refc.rc_blockcount);
		irec->rc_refcount = be32_to_cpu(rec->refc.rc_refcount);
		trace_xfs_refcountbt_get(cur->bc_mp, cur->bc_private.a.agno,
				irec);
	}
	return error;
}

/*
 * Update the record referred to by cur to the value given
 * by [bno, len, refcount].
 * This either works (return 0) or gets an EFSCORRUPTED error.
 */
STATIC int
xfs_refcountbt_update(
	struct xfs_btree_cur		*cur,
	struct xfs_refcount_irec	*irec)
{
	union xfs_btree_rec	rec;

	trace_xfs_refcountbt_update(cur->bc_mp, cur->bc_private.a.agno, irec);
	rec.refc.rc_startblock = cpu_to_be32(irec->rc_startblock);
	rec.refc.rc_blockcount = cpu_to_be32(irec->rc_blockcount);
	rec.refc.rc_refcount = cpu_to_be32(irec->rc_refcount);
	return xfs_btree_update(cur, &rec);
}

/*
 * Insert the record referred to by cur to the value given
 * by [bno, len, refcount].
 * This either works (return 0) or gets an EFSCORRUPTED error.
 */
STATIC int
xfs_refcountbt_insert(
	struct xfs_btree_cur		*cur,
	struct xfs_refcount_irec	*irec,
	int				*i)
{
	trace_xfs_refcountbt_insert(cur->bc_mp, cur->bc_private.a.agno, irec);
	cur->bc_rec.rc.rc_startblock = irec->rc_startblock;
	cur->bc_rec.rc.rc_blockcount = irec->rc_blockcount;
	cur->bc_rec.rc.rc_refcount = irec->rc_refcount;
	return xfs_btree_insert(cur, i);
}

/*
 * Remove the record referred to by cur, then set the pointer to the spot
 * where the record could be re-inserted, in case we want to increment or
 * decrement the cursor.
 * This either works (return 0) or gets an EFSCORRUPTED error.
 */
STATIC int
xfs_refcountbt_delete(
	struct xfs_btree_cur	*cur,
	int			*i)
{
	struct xfs_refcount_irec	irec;
	int			found_rec;
	int			error;

	error = xfs_refcountbt_get_rec(cur, &irec, &found_rec);
	if (error)
		return error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);
	trace_xfs_refcountbt_delete(cur->bc_mp, cur->bc_private.a.agno, &irec);
	error = xfs_btree_delete(cur, i);
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, *i == 1, out_error);
	if (error)
		return error;
	error = xfs_refcountbt_lookup_ge(cur, irec.rc_startblock, &found_rec);
out_error:
	return error;
}

/*
 * Adjusting the Reference Count
 *
 * As stated elsewhere, the reference count btree (refcbt) stores
 * >1 reference counts for extents of physical blocks.  In this
 * operation, we're either raising or lowering the reference count of
 * some subrange stored in the tree:
 *
 *      <------ adjustment range ------>
 * ----+   +---+-----+ +--+--------+---------
 *  2  |   | 3 |  4  | |17|   55   |   10
 * ----+   +---+-----+ +--+--------+---------
 * X axis is physical blocks number;
 * reference counts are the numbers inside the rectangles
 *
 * The first thing we need to do is to ensure that there are no
 * refcount extents crossing either boundary of the range to be
 * adjusted.  For any extent that does cross a boundary, split it into
 * two extents so that we can increment the refcount of one of the
 * pieces later:
 *
 *      <------ adjustment range ------>
 * ----+   +---+-----+ +--+--------+----+----
 *  2  |   | 3 |  2  | |17|   55   | 10 | 10
 * ----+   +---+-----+ +--+--------+----+----
 *
 * For this next step, let's assume that all the physical blocks in
 * the adjustment range are mapped to a file and are therefore in use
 * at least once.  Therefore, we can infer that any gap in the
 * refcount tree within the adjustment range represents a physical
 * extent with refcount == 1:
 *
 *      <------ adjustment range ------>
 * ----+---+---+-----+-+--+--------+----+----
 *  2  |"1"| 3 |  2  |1|17|   55   | 10 | 10
 * ----+---+---+-----+-+--+--------+----+----
 *      ^
 *
 * For each extent that falls within the interval range, figure out
 * which extent is to the left or the right of that extent.  Now we
 * have a left, current, and right extent.  If the new reference count
 * of the center extent enables us to merge left, center, and right
 * into one record covering all three, do so.  If the center extent is
 * at the left end of the range, abuts the left extent, and its new
 * reference count matches the left extent's record, then merge them.
 * If the center extent is at the right end of the range, abuts the
 * right extent, and the reference counts match, merge those.  In the
 * example, we can left merge (assuming an increment operation):
 *
 *      <------ adjustment range ------>
 * --------+---+-----+-+--+--------+----+----
 *    2    | 3 |  2  |1|17|   55   | 10 | 10
 * --------+---+-----+-+--+--------+----+----
 *          ^
 *
 * For all other extents within the range, adjust the reference count
 * or delete it if the refcount falls below 2.  If we were
 * incrementing, the end result looks like this:
 *
 *      <------ adjustment range ------>
 * --------+---+-----+-+--+--------+----+----
 *    2    | 4 |  3  |2|18|   56   | 11 | 10
 * --------+---+-----+-+--+--------+----+----
 *
 * The result of a decrement operation looks as such:
 *
 *      <------ adjustment range ------>
 * ----+   +---+       +--+--------+----+----
 *  2  |   | 2 |       |16|   54   |  9 | 10
 * ----+   +---+       +--+--------+----+----
 *      DDDD    111111DD
 *
 * The blocks marked "D" are freed; the blocks marked "1" are only
 * referenced once and therefore the record is removed from the
 * refcount btree.
 */

#define RCNEXT(rc)	((rc).rc_startblock + (rc).rc_blockcount)
/*
 * Split a left rcextent that crosses agbno.
 */
STATIC int
try_split_left_rcextent(
	struct xfs_btree_cur		*cur,
	xfs_agblock_t			agbno)
{
	struct xfs_refcount_irec	left, tmp;
	int				found_rec;
	int				error;

	error = xfs_refcountbt_lookup_le(cur, agbno, &found_rec);
	if (error)
		goto out_error;
	if (!found_rec)
		return 0;

	error = xfs_refcountbt_get_rec(cur, &left, &found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);
	if (left.rc_startblock >= agbno || RCNEXT(left) <= agbno)
		return 0;

	trace_xfs_refcount_split_left_extent(cur->bc_mp, cur->bc_private.a.agno,
			&left, agbno);
	tmp = left;
	tmp.rc_blockcount = agbno - left.rc_startblock;
	error = xfs_refcountbt_update(cur, &tmp);
	if (error)
		goto out_error;

	error = xfs_btree_increment(cur, 0, &found_rec);
	if (error)
		goto out_error;

	tmp = left;
	tmp.rc_startblock = agbno;
	tmp.rc_blockcount -= (agbno - left.rc_startblock);
	error = xfs_refcountbt_insert(cur, &tmp, &found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);
	return error;

out_error:
	trace_xfs_refcount_split_left_extent_error(cur->bc_mp,
			cur->bc_private.a.agno, error, _RET_IP_);
	return error;
}

/*
 * Split a right rcextent that crosses agbno.
 */
STATIC int
try_split_right_rcextent(
	struct xfs_btree_cur	*cur,
	xfs_agblock_t		agbnext)
{
	struct xfs_refcount_irec	right, tmp;
	int				found_rec;
	int				error;

	error = xfs_refcountbt_lookup_le(cur, agbnext - 1, &found_rec);
	if (error)
		goto out_error;
	if (!found_rec)
		return 0;

	error = xfs_refcountbt_get_rec(cur, &right, &found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);
	if (RCNEXT(right) <= agbnext)
		return 0;

	trace_xfs_refcount_split_right_extent(cur->bc_mp,
			cur->bc_private.a.agno, &right, agbnext);
	tmp = right;
	tmp.rc_startblock = agbnext;
	tmp.rc_blockcount -= (agbnext - right.rc_startblock);
	error = xfs_refcountbt_update(cur, &tmp);
	if (error)
		goto out_error;

	tmp = right;
	tmp.rc_blockcount = agbnext - right.rc_startblock;
	error = xfs_refcountbt_insert(cur, &tmp, &found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);
	return error;

out_error:
	trace_xfs_refcount_split_right_extent_error(cur->bc_mp,
			cur->bc_private.a.agno, error, _RET_IP_);
	return error;
}

/*
 * Merge the left, center, and right extents.
 */
STATIC int
merge_center(
	struct xfs_btree_cur		*cur,
	struct xfs_refcount_irec	*left,
	struct xfs_refcount_irec	*center,
	unsigned long long		extlen,
	xfs_agblock_t			*agbno,
	xfs_extlen_t			*aglen)
{
	int				error;
	int				found_rec;

	error = xfs_refcountbt_lookup_ge(cur, center->rc_startblock,
			&found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);

	error = xfs_refcountbt_delete(cur, &found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);

	if (center->rc_refcount > 1) {
		error = xfs_refcountbt_delete(cur, &found_rec);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1,
				out_error);
	}

	error = xfs_refcountbt_lookup_le(cur, left->rc_startblock,
			&found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);

	left->rc_blockcount = extlen;
	error = xfs_refcountbt_update(cur, left);
	if (error)
		goto out_error;

	*aglen = 0;
	return error;

out_error:
	trace_xfs_refcount_merge_center_extents_error(cur->bc_mp,
			cur->bc_private.a.agno, error, _RET_IP_);
	return error;
}

/*
 * Merge with the left extent.
 */
STATIC int
merge_left(
	struct xfs_btree_cur		*cur,
	struct xfs_refcount_irec	*left,
	struct xfs_refcount_irec	*cleft,
	xfs_agblock_t			*agbno,
	xfs_extlen_t			*aglen)
{
	int				error;
	int				found_rec;

	if (cleft->rc_refcount > 1) {
		error = xfs_refcountbt_lookup_le(cur, cleft->rc_startblock,
				&found_rec);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1,
				out_error);

		error = xfs_refcountbt_delete(cur, &found_rec);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1,
				out_error);
	}

	error = xfs_refcountbt_lookup_le(cur, left->rc_startblock,
			&found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);

	left->rc_blockcount += cleft->rc_blockcount;
	error = xfs_refcountbt_update(cur, left);
	if (error)
		goto out_error;

	*agbno += cleft->rc_blockcount;
	*aglen -= cleft->rc_blockcount;
	return error;

out_error:
	trace_xfs_refcount_merge_left_extent_error(cur->bc_mp,
			cur->bc_private.a.agno, error, _RET_IP_);
	return error;
}

/*
 * Merge with the right extent.
 */
STATIC int
merge_right(
	struct xfs_btree_cur		*cur,
	struct xfs_refcount_irec	*right,
	struct xfs_refcount_irec	*cright,
	xfs_agblock_t			*agbno,
	xfs_extlen_t			*aglen)
{
	int				error;
	int				found_rec;

	if (cright->rc_refcount > 1) {
		error = xfs_refcountbt_lookup_le(cur, cright->rc_startblock,
			&found_rec);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1,
				out_error);

		error = xfs_refcountbt_delete(cur, &found_rec);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1,
				out_error);
	}

	error = xfs_refcountbt_lookup_le(cur, right->rc_startblock,
			&found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);

	right->rc_startblock -= cright->rc_blockcount;
	right->rc_blockcount += cright->rc_blockcount;
	error = xfs_refcountbt_update(cur, right);
	if (error)
		goto out_error;

	*aglen -= cright->rc_blockcount;
	return error;

out_error:
	trace_xfs_refcount_merge_right_extent_error(cur->bc_mp,
			cur->bc_private.a.agno, error, _RET_IP_);
	return error;
}

#define XFS_FIND_RCEXT_SHARED	1
#define XFS_FIND_RCEXT_COW	2
/*
 * Find the left extent and the one after it (cleft).  This function assumes
 * that we've already split any extent crossing agbno.
 */
STATIC int
find_left_extent(
	struct xfs_btree_cur		*cur,
	struct xfs_refcount_irec	*left,
	struct xfs_refcount_irec	*cleft,
	xfs_agblock_t			agbno,
	xfs_extlen_t			aglen,
	int				flags)
{
	struct xfs_refcount_irec	tmp;
	int				error;
	int				found_rec;

	left->rc_blockcount = cleft->rc_blockcount = 0;
	error = xfs_refcountbt_lookup_le(cur, agbno - 1, &found_rec);
	if (error)
		goto out_error;
	if (!found_rec)
		return 0;

	error = xfs_refcountbt_get_rec(cur, &tmp, &found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);

	if (RCNEXT(tmp) != agbno)
		return 0;
	if ((flags & XFS_FIND_RCEXT_SHARED) && tmp.rc_refcount < 2)
		return 0;
	if ((flags & XFS_FIND_RCEXT_COW) && tmp.rc_refcount > 1)
		return 0;
	/* We have a left extent; retrieve (or invent) the next right one */
	*left = tmp;

	error = xfs_btree_increment(cur, 0, &found_rec);
	if (error)
		goto out_error;
	if (found_rec) {
		error = xfs_refcountbt_get_rec(cur, &tmp, &found_rec);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1,
				out_error);

		/* if tmp starts at the end of our range, just use that */
		if (tmp.rc_startblock == agbno)
			*cleft = tmp;
		else {
			/*
			 * There's a gap in the refcntbt at the start of the
			 * range we're interested in (refcount == 1) so
			 * create the implied extent and pass it back.
			 */
			cleft->rc_startblock = agbno;
			cleft->rc_blockcount = min(aglen,
					tmp.rc_startblock - agbno);
			cleft->rc_refcount = 1;
		}
	} else {
		/*
		 * No extents, so pretend that there's one covering the whole
		 * range.
		 */
		cleft->rc_startblock = agbno;
		cleft->rc_blockcount = aglen;
		cleft->rc_refcount = 1;
	}
	trace_xfs_refcount_find_left_extent(cur->bc_mp, cur->bc_private.a.agno,
			left, cleft, agbno);
	return error;

out_error:
	trace_xfs_refcount_find_left_extent_error(cur->bc_mp,
			cur->bc_private.a.agno, error, _RET_IP_);
	return error;
}

/*
 * Find the right extent and the one before it (cright).  This function
 * assumes that we've already split any extents crossing agbno + aglen.
 */
STATIC int
find_right_extent(
	struct xfs_btree_cur		*cur,
	struct xfs_refcount_irec	*right,
	struct xfs_refcount_irec	*cright,
	xfs_agblock_t			agbno,
	xfs_extlen_t			aglen,
	int				flags)
{
	struct xfs_refcount_irec	tmp;
	int				error;
	int				found_rec;

	right->rc_blockcount = cright->rc_blockcount = 0;
	error = xfs_refcountbt_lookup_ge(cur, agbno + aglen, &found_rec);
	if (error)
		goto out_error;
	if (!found_rec)
		return 0;

	error = xfs_refcountbt_get_rec(cur, &tmp, &found_rec);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1, out_error);

	if (tmp.rc_startblock != agbno + aglen)
		return 0;
	if ((flags & XFS_FIND_RCEXT_SHARED) && tmp.rc_refcount < 2)
		return 0;
	if ((flags & XFS_FIND_RCEXT_COW) && tmp.rc_refcount > 1)
		return 0;
	/* We have a right extent; retrieve (or invent) the next left one */
	*right = tmp;

	error = xfs_btree_decrement(cur, 0, &found_rec);
	if (error)
		goto out_error;
	if (found_rec) {
		error = xfs_refcountbt_get_rec(cur, &tmp, &found_rec);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp, found_rec == 1,
				out_error);

		/* if tmp ends at the end of our range, just use that */
		if (RCNEXT(tmp) == agbno + aglen)
			*cright = tmp;
		else {
			/*
			 * There's a gap in the refcntbt at the end of the
			 * range we're interested in (refcount == 1) so
			 * create the implied extent and pass it back.
			 */
			cright->rc_startblock = max(agbno, RCNEXT(tmp));
			cright->rc_blockcount = right->rc_startblock -
					cright->rc_startblock;
			cright->rc_refcount = 1;
		}
	} else {
		/*
		 * No extents, so pretend that there's one covering the whole
		 * range.
		 */
		cright->rc_startblock = agbno;
		cright->rc_blockcount = aglen;
		cright->rc_refcount = 1;
	}
	trace_xfs_refcount_find_right_extent(cur->bc_mp, cur->bc_private.a.agno,
			cright, right, agbno + aglen);
	return error;

out_error:
	trace_xfs_refcount_find_right_extent_error(cur->bc_mp,
			cur->bc_private.a.agno, error, _RET_IP_);
	return error;
}
#undef RCNEXT

/*
 * Try to merge with any extents on the boundaries of the adjustment range.
 */
STATIC int
try_merge_rcextents(
	struct xfs_btree_cur	*cur,
	xfs_agblock_t		*agbno,
	xfs_extlen_t		*aglen,
	int			adjust,
	int			flags)
{
	struct xfs_refcount_irec	left = {0}, cleft = {0};
	struct xfs_refcount_irec	cright = {0}, right = {0};
	int				error;
	unsigned long long		ulen;
	bool				cequal;

	/*
	 * Find the extent just below agbno [left], just above agbno [cleft],
	 * just below (agbno + aglen) [cright], and just above (agbno + aglen)
	 * [right].
	 */
	error = find_left_extent(cur, &left, &cleft, *agbno, *aglen, flags);
	if (error)
		return error;
	error = find_right_extent(cur, &right, &cright, *agbno, *aglen, flags);
	if (error)
		return error;

	/* No left or right extent to merge; exit. */
	if (left.rc_blockcount == 0 && right.rc_blockcount == 0)
		return 0;

	cequal = (cleft.rc_startblock == cright.rc_startblock) &&
		 (cleft.rc_blockcount == cright.rc_blockcount);

	/* Try to merge left, cleft, and right.  cleft must == cright. */
	ulen = (unsigned long long)left.rc_blockcount + cleft.rc_blockcount +
			right.rc_blockcount;
	if (left.rc_blockcount != 0 && right.rc_blockcount != 0 &&
	    cleft.rc_blockcount != 0 && cright.rc_blockcount != 0 &&
	    cequal &&
	    left.rc_refcount == cleft.rc_refcount + adjust &&
	    right.rc_refcount == cleft.rc_refcount + adjust &&
	    ulen < MAXREFCEXTLEN) {
		trace_xfs_refcount_merge_center_extents(cur->bc_mp,
			cur->bc_private.a.agno, &left, &cleft, &right);
		return merge_center(cur, &left, &cleft, ulen, agbno, aglen);
	}

	/* Try to merge left and cleft. */
	ulen = (unsigned long long)left.rc_blockcount + cleft.rc_blockcount;
	if (left.rc_blockcount != 0 && cleft.rc_blockcount != 0 &&
	    left.rc_refcount == cleft.rc_refcount + adjust &&
	    ulen < MAXREFCEXTLEN) {
		trace_xfs_refcount_merge_left_extent(cur->bc_mp,
			cur->bc_private.a.agno, &left, &cleft);
		error = merge_left(cur, &left, &cleft, agbno, aglen);
		if (error)
			return error;

		/*
		 * If we just merged left + cleft and cleft == cright,
		 * we no longer have a cright to merge with right.  We're done.
		 */
		if (cequal)
			return 0;
	}

	/* Try to merge cright and right. */
	ulen = (unsigned long long)right.rc_blockcount + cright.rc_blockcount;
	if (right.rc_blockcount != 0 && cright.rc_blockcount != 0 &&
	    right.rc_refcount == cright.rc_refcount + adjust &&
	    ulen < MAXREFCEXTLEN) {
		trace_xfs_refcount_merge_right_extent(cur->bc_mp,
			cur->bc_private.a.agno, &cright, &right);
		return merge_right(cur, &right, &cright, agbno, aglen);
	}

	return error;
}

/*
 * Adjust the refcounts of middle extents.  At this point we should have
 * split extents that crossed the adjustment range; merged with adjacent
 * extents; and updated agbno/aglen to reflect the merges.  Therefore,
 * all we have to do is update the extents inside [agbno, agbno + aglen].
 */
STATIC int
adjust_rcextents(
	struct xfs_btree_cur	*cur,
	xfs_agblock_t		agbno,
	xfs_extlen_t		aglen,
	int			adj,
	struct xfs_bmap_free	*flist,
	struct xfs_owner_info	*oinfo)
{
	struct xfs_refcount_irec	ext, tmp;
	int				error;
	int				found_rec, found_tmp;
	xfs_fsblock_t			fsbno;

	error = xfs_refcountbt_lookup_ge(cur, agbno, &found_rec);
	if (error)
		goto out_error;

	while (aglen > 0) {
		error = xfs_refcountbt_get_rec(cur, &ext, &found_rec);
		if (error)
			goto out_error;
		if (!found_rec) {
			ext.rc_startblock = cur->bc_mp->m_sb.sb_agblocks;
			ext.rc_blockcount = 0;
			ext.rc_refcount = 0;
		}

		/*
		 * Deal with a hole in the refcount tree; if a file maps to
		 * these blocks and there's no refcountbt recourd, pretend that
		 * there is one with refcount == 1.
		 */
		if (ext.rc_startblock != agbno) {
			tmp.rc_startblock = agbno;
			tmp.rc_blockcount = min(aglen,
					ext.rc_startblock - agbno);
			tmp.rc_refcount = 1 + adj;
			trace_xfs_refcount_modify_extent(cur->bc_mp,
					cur->bc_private.a.agno, &tmp);

			/*
			 * Either cover the hole (increment) or
			 * delete the range (decrement).
			 */
			if (tmp.rc_refcount) {
				error = xfs_refcountbt_insert(cur, &tmp,
						&found_tmp);
				if (error)
					goto out_error;
				XFS_WANT_CORRUPTED_GOTO(cur->bc_mp,
						found_tmp == 1, out_error);
			} else {
				fsbno = XFS_AGB_TO_FSB(cur->bc_mp,
						cur->bc_private.a.agno,
						tmp.rc_startblock);
				xfs_bmap_add_free(cur->bc_mp, flist, fsbno,
						tmp.rc_blockcount, oinfo);
			}

			agbno += tmp.rc_blockcount;
			aglen -= tmp.rc_blockcount;

			error = xfs_refcountbt_lookup_ge(cur, agbno,
					&found_rec);
			if (error)
				goto out_error;
		}

		/* Stop if there's nothing left to modify */
		if (aglen == 0)
			break;

		/*
		 * Adjust the reference count and either update the tree
		 * (incr) or free the blocks (decr).
		 */
		if (ext.rc_refcount == MAXREFCOUNT)
			goto skip;
		ext.rc_refcount += adj;
		trace_xfs_refcount_modify_extent(cur->bc_mp,
				cur->bc_private.a.agno, &ext);
		if (ext.rc_refcount > 1) {
			error = xfs_refcountbt_update(cur, &ext);
			if (error)
				goto out_error;
		} else if (ext.rc_refcount == 1) {
			error = xfs_refcountbt_delete(cur, &found_rec);
			if (error)
				goto out_error;
			XFS_WANT_CORRUPTED_GOTO(cur->bc_mp,
					found_rec == 1, out_error);
			goto advloop;
		} else {
			fsbno = XFS_AGB_TO_FSB(cur->bc_mp,
					cur->bc_private.a.agno,
					ext.rc_startblock);
			xfs_bmap_add_free(cur->bc_mp, flist, fsbno,
					ext.rc_blockcount, oinfo);
		}

skip:
		error = xfs_btree_increment(cur, 0, &found_rec);
		if (error)
			goto out_error;

advloop:
		agbno += ext.rc_blockcount;
		aglen -= ext.rc_blockcount;
	}

	return error;
out_error:
	trace_xfs_refcount_modify_extent_error(cur->bc_mp,
			cur->bc_private.a.agno, error, _RET_IP_);
	return error;
}

/*
 * Adjust the reference count of a range of AG blocks.
 *
 * @mp: XFS mount object
 * @tp: XFS transaction object
 * @agbp: Buffer containing the AGF
 * @agno: AG number
 * @agbno: Start of range to adjust
 * @aglen: Length of range to adjust
 * @adj: +1 to increment, -1 to decrement reference count
 * @flist: freelist (only required if adj == -1)
 * @owner: owner of the blocks (only required if adj == -1)
 */
STATIC int
xfs_refcountbt_adjust_refcount(
	struct xfs_mount	*mp,
	struct xfs_trans	*tp,
	struct xfs_buf		*agbp,
	xfs_agnumber_t		agno,
	xfs_agblock_t		agbno,
	xfs_extlen_t		aglen,
	int			adj,
	struct xfs_bmap_free	*flist,
	struct xfs_owner_info	*oinfo)
{
	struct xfs_btree_cur	*cur;
	int			error;

	cur = xfs_refcountbt_init_cursor(mp, tp, agbp, agno, flist);

	/*
	 * Ensure that no rcextents cross the boundary of the adjustment range.
	 */
	error = try_split_left_rcextent(cur, agbno);
	if (error)
		goto out_error;

	error = try_split_right_rcextent(cur, agbno + aglen);
	if (error)
		goto out_error;

	/*
	 * Try to merge with the left or right extents of the range.
	 */
	error = try_merge_rcextents(cur, &agbno, &aglen, adj,
			XFS_FIND_RCEXT_SHARED);
	if (error)
		goto out_error;

	/* Now that we've taken care of the ends, adjust the middle extents */
	error = adjust_rcextents(cur, agbno, aglen, adj, flist, oinfo);
	if (error)
		goto out_error;

	xfs_btree_del_cursor(cur, XFS_BTREE_NOERROR);
	return 0;

out_error:
	trace_xfs_refcount_adjust_error(mp, agno, error, _RET_IP_);
	xfs_btree_del_cursor(cur, XFS_BTREE_ERROR);
	return error;
}

/*
 * Increase the reference count of a range of AG blocks.
 */
int
xfs_refcount_increase(
	struct xfs_mount	*mp,
	struct xfs_trans	*tp,
	struct xfs_buf		*agbp,
	xfs_agnumber_t		agno,
	xfs_agblock_t		agbno,
	xfs_extlen_t		aglen,
	struct xfs_bmap_free	*flist)
{
	trace_xfs_refcount_increase(mp, agno, agbno, aglen);
	return xfs_refcountbt_adjust_refcount(mp, tp, agbp, agno, agbno,
			aglen, 1, flist, NULL);
}

/*
 * Decrease the reference count of a range of AG blocks.
 */
int
xfs_refcount_decrease(
	struct xfs_mount	*mp,
	struct xfs_trans	*tp,
	struct xfs_buf		*agbp,
	xfs_agnumber_t		agno,
	xfs_agblock_t		agbno,
	xfs_extlen_t		aglen,
	struct xfs_bmap_free	*flist,
	struct xfs_owner_info	*oinfo)
{
	trace_xfs_refcount_decrease(mp, agno, agbno, aglen);
	return xfs_refcountbt_adjust_refcount(mp, tp, agbp, agno, agbno,
			aglen, -1, flist, oinfo);
}

/*
 * Decrease the reference count on a range of blocks as part of unmapping
 * blocks from a file.  The blocks will be freed if the refcount becomes zero.
 */
int
xfs_refcount_put_extent(
	struct xfs_mount	*mp,
	struct xfs_trans	*tp,
	struct xfs_bmap_free	*flist,
	xfs_fsblock_t		fsbno,
	xfs_filblks_t		fslen,
	struct xfs_owner_info	*oinfo)
{
	int			error;
	struct xfs_buf		*agbp;
	xfs_agnumber_t		agno;		/* allocation group number */
	xfs_agblock_t		agbno;		/* ag start of range to free */
	xfs_extlen_t		aglen;		/* ag length of range to free */

	agno = XFS_FSB_TO_AGNO(mp, fsbno);
	agbno = XFS_FSB_TO_AGBNO(mp, fsbno);
	aglen = fslen;

	/*
	 * Drop reference counts in the refcount tree.
	 */
	error = xfs_alloc_read_agf(mp, tp, agno, 0, &agbp);
	if (error)
		return error;

	error = xfs_refcount_decrease(mp, tp, agbp, agno, agbno, aglen, flist,
			oinfo);
	xfs_trans_brelse(tp, agbp);
	return error;
}

/*
 * Given an AG extent, find the lowest-numbered run of shared blocks within
 * that range and return the range in fbno/flen.  If find_maximal is set,
 * return the longest extent of shared blocks; if not, just return the first
 * extent we find.  If no shared blocks are found, flen will be set to zero.
 */
int
xfs_refcount_find_shared(
	struct xfs_mount	*mp,
	xfs_agnumber_t		agno,
	xfs_agblock_t		agbno,
	xfs_extlen_t		aglen,
	xfs_agblock_t		*fbno,
	xfs_extlen_t		*flen,
	bool			find_maximal)
{
	struct xfs_btree_cur	*cur;
	struct xfs_buf		*agbp;
	struct xfs_refcount_irec	tmp;
	int			error;
	int			i, have;
	int			bt_error = XFS_BTREE_ERROR;

	trace_xfs_refcount_find_shared(mp, agno, agbno, aglen);

	if (xfs_always_cow) {
		*fbno = agbno;
		*flen = aglen;
		return 0;
	}

	error = xfs_alloc_read_agf(mp, NULL, agno, 0, &agbp);
	if (error)
		goto out;
	cur = xfs_refcountbt_init_cursor(mp, NULL, agbp, agno, NULL);

	/* By default, skip the whole range */
	*fbno = agbno + aglen;
	*flen = 0;

	/* Try to find a refcount extent that crosses the start */
	error = xfs_refcountbt_lookup_le(cur, agbno, &have);
	if (error)
		goto out_error;
	if (!have) {
		/* No left extent, look at the next one */
		error = xfs_btree_increment(cur, 0, &have);
		if (error)
			goto out_error;
		if (!have)
			goto done;
	}
	error = xfs_refcountbt_get_rec(cur, &tmp, &i);
	if (error)
		goto out_error;
	XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);

	/* If the extent ends before the start, look at the next one */
	if (tmp.rc_startblock + tmp.rc_blockcount <= agbno) {
		error = xfs_btree_increment(cur, 0, &have);
		if (error)
			goto out_error;
		if (!have)
			goto done;
		error = xfs_refcountbt_get_rec(cur, &tmp, &i);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);
	}

	/* If the extent ends after the range we want, bail out */
	if (tmp.rc_startblock >= agbno + aglen)
		goto done;

	/* We found the start of a shared extent! */
	if (tmp.rc_startblock < agbno) {
		tmp.rc_blockcount -= (agbno - tmp.rc_startblock);
		tmp.rc_startblock = agbno;
	}

	*fbno = tmp.rc_startblock;
	*flen = min(tmp.rc_blockcount, agbno + aglen - *fbno);
	if (!find_maximal)
		goto done;

	/* Otherwise, find the end of this shared extent */
	while (*fbno + *flen < agbno + aglen) {
		error = xfs_btree_increment(cur, 0, &have);
		if (error)
			goto out_error;
		if (!have)
			break;
		error = xfs_refcountbt_get_rec(cur, &tmp, &i);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(mp, i == 1, out_error);
		if (tmp.rc_startblock >= agbno + aglen ||
		    tmp.rc_startblock != *fbno + *flen)
			break;
		*flen = min(*flen + tmp.rc_blockcount, agbno + aglen - *fbno);
	}

done:
	bt_error = XFS_BTREE_NOERROR;
	trace_xfs_refcount_find_shared_result(mp, agno, *fbno, *flen);

out_error:
	xfs_btree_del_cursor(cur, bt_error);
	xfs_buf_relse(agbp);
out:
	if (error)
		trace_xfs_refcount_find_shared_error(mp, agno, error, _RET_IP_);
	return error;
}

/*
 * Recovering CoW Blocks After a Crash
 *
 * Due to the way that the copy on write mechanism works, there's a window of
 * opportunity in which we can lose track of allocated blocks during a crash.
 * Because CoW uses delayed allocation in the in-core CoW fork, writeback
 * causes blocks to be allocated and stored in the CoW fork.  The blocks are
 * no longer in the free space btree but are not otherwise recorded anywhere
 * until the write completes and the blocks are mapped into the file.  A crash
 * in between allocation and remapping results in the replacement blocks being
 * lost.  This situation is exacerbated by the CoW extent size hint because
 * allocations can hang around for long time.
 *
 * However, there is a place where we can record these allocations before they
 * become mappings -- the reference count btree.  The btree does not record
 * extents with refcount == 1, so we can record allocations with a refcount of
 * 1.  Blocks being used for CoW writeout cannot be shared, so there should be
 * no conflict with shared block records.  These mappings should be created
 * when we allocate blocks to the CoW fork and deleted when they're removed
 * from the CoW fork.
 *
 * Minor nit: records for in-progress CoW allocations and records for shared
 * extents must never be merged, to preserve the property that (except for CoW
 * allocations) there are no refcount btree entries with refcount == 1.  The
 * only time this could potentially happen is when unsharing a block that's
 * adjacent to CoW allocations, so we must be careful to avoid this.
 *
 * At mount time we recover lost CoW allocations by searching the refcount
 * btree for these refcount == 1 mappings.  These represent CoW allocations
 * that were in progress at the time the filesystem went down, so we can free
 * them to get the space back.
 *
 * This mechanism is superior to creating EFIs for unmapped CoW extents for
 * several reasons -- first, EFIs pin the tail of the log and would have to be
 * periodically relogged to avoid filling up the log.  Second, CoW completions
 * will have to file an EFD and create new EFIs for whatever remains in the
 * CoW fork; this partially takes care of (1) but extent-size reservations
 * will have to periodically relog even if there's no writeout in progress.
 * This can happen if the CoW extent size hint is set, which you really want.
 * Third, EFIs cannot currently be automatically relogged into newer
 * transactions to advance the log tail.  Fourth, stuffing the log full of
 * EFIs places an upper bound on the number of CoW allocations that can be
 * held filesystem-wide at any given time.  Recording them in the refcount
 * btree doesn't require us to maintain any state in memory and doesn't pin
 * the log.
 */
/*
 * Adjust the refcounts of CoW allocations.  These allocations are "magic"
 * in that they're not referenced anywhere else in the filesystem, so we
 * stash them in the refcount btree with a refcount of 1 until either file
 * remapping (or CoW cancellation) happens.
 */
STATIC int
adjust_cow_rcextents(
	struct xfs_btree_cur	*cur,
	xfs_agblock_t		agbno,
	xfs_extlen_t		aglen,
	enum xfs_adjust_cow	adj,
	struct xfs_bmap_free	*flist,
	struct xfs_owner_info	*oinfo)
{
	struct xfs_refcount_irec	ext, tmp;
	int				error;
	int				found_rec, found_tmp;

	if (aglen == 0)
		return 0;

	/* Find any overlapping refcount records */
	error = xfs_refcountbt_lookup_ge(cur, agbno, &found_rec);
	if (error)
		goto out_error;
	error = xfs_refcountbt_get_rec(cur, &ext, &found_rec);
	if (error)
		goto out_error;
	if (!found_rec) {
		ext.rc_startblock = cur->bc_mp->m_sb.sb_agblocks;
		ext.rc_blockcount = 0;
		ext.rc_refcount = 0;
	}

	switch (adj) {
	case XFS_ADJUST_COW_ALLOC:
		/* Adding a CoW reservation, there should be nothing here. */
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp,
				ext.rc_startblock >= agbno + aglen, out_error);

		tmp.rc_startblock = agbno;
		tmp.rc_blockcount = aglen;
		tmp.rc_refcount = 1;
		trace_xfs_refcount_modify_extent(cur->bc_mp,
				cur->bc_private.a.agno, &tmp);

		error = xfs_refcountbt_insert(cur, &tmp,
				&found_tmp);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp,
				found_tmp == 1, out_error);
		break;
	case XFS_ADJUST_COW_FREE:
		/* Removing a CoW reservation, there should be one extent. */
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp,
			ext.rc_startblock == agbno, out_error);
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp,
			ext.rc_blockcount == aglen, out_error);
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp,
			ext.rc_refcount == 1, out_error);

		ext.rc_refcount = 0;
		trace_xfs_refcount_modify_extent(cur->bc_mp,
				cur->bc_private.a.agno, &ext);
		error = xfs_refcountbt_delete(cur, &found_rec);
		if (error)
			goto out_error;
		XFS_WANT_CORRUPTED_GOTO(cur->bc_mp,
				found_rec == 1, out_error);
		break;
	default:
		ASSERT(0);
	}

	return error;
out_error:
	trace_xfs_refcount_modify_extent_error(cur->bc_mp,
			cur->bc_private.a.agno, error, _RET_IP_);
	return error;
}

/*
 * Add or remove refcount btree entries for CoW reservations.
 */
STATIC int
xfs_refcountbt_adjust_cow_refcount(
	struct xfs_mount	*mp,
	struct xfs_trans	*tp,
	struct xfs_buf		*agbp,
	xfs_agnumber_t		agno,
	xfs_agblock_t		agbno,
	xfs_extlen_t		aglen,
	enum xfs_adjust_cow	adj,
	struct xfs_bmap_free	*flist)
{
	struct xfs_btree_cur	*cur;
	int			error;

	cur = xfs_refcountbt_init_cursor(mp, tp, agbp, agno, flist);

	/*
	 * Ensure that no rcextents cross the boundary of the adjustment range.
	 */
	error = try_split_left_rcextent(cur, agbno);
	if (error)
		goto out_error;

	error = try_split_right_rcextent(cur, agbno + aglen);
	if (error)
		goto out_error;

	/*
	 * Try to merge with the left or right extents of the range.
	 */
	error = try_merge_rcextents(cur, &agbno, &aglen, adj,
			XFS_FIND_RCEXT_COW);
	if (error)
		goto out_error;

	/* Now that we've taken care of the ends, adjust the middle extents */
	error = adjust_cow_rcextents(cur, agbno, aglen, adj, flist, NULL);
	if (error)
		goto out_error;

	xfs_btree_del_cursor(cur, XFS_BTREE_NOERROR);
	return 0;

out_error:
	trace_xfs_refcount_adjust_cow_error(mp, agno, error, _RET_IP_);
	xfs_btree_del_cursor(cur, XFS_BTREE_ERROR);
	return error;
}

/*
 * Record a CoW allocation in the refcount btree.
 */
int
xfs_refcountbt_cow_alloc(
	struct xfs_mount	*mp,
	struct xfs_trans	*tp,
	struct xfs_bmbt_irec	*imap,
	struct xfs_bmap_free	*flist)
{
	xfs_agnumber_t		agno;		/* allocation group number */
	xfs_agblock_t		agbno;		/* ag start of range to free */
	xfs_extlen_t		aglen;		/* ag length of range to free */
	struct xfs_buf		*agbp;
	struct xfs_owner_info	oinfo;
	int			error;

	XFS_RMAP_AG_OWNER(&oinfo, XFS_RMAP_OWN_COW);
	agno = XFS_FSB_TO_AGNO(mp, imap->br_startblock);
	agbno = XFS_FSB_TO_AGBNO(mp, imap->br_startblock);
	aglen = imap->br_blockcount;
	trace_xfs_refcount_cow_increase(mp, agno, agbno, aglen);

	error = xfs_alloc_read_agf(mp, tp, agno, 0, &agbp);
	if (error)
		return error;

	error = xfs_refcountbt_adjust_cow_refcount(mp, tp, agbp, agno, agbno,
			aglen, XFS_ADJUST_COW_ALLOC, flist);
	xfs_trans_brelse(tp, agbp);
	return error;
}

/*
 * Remove a CoW allocation from the refcount btree.
 */
int
xfs_refcountbt_cow_free(
	struct xfs_mount	*mp,
	struct xfs_inode	*ip,
	struct xfs_trans	**tpp,
	struct xfs_bmbt_irec	*imap)
{
	xfs_agnumber_t		agno;		/* allocation group number */
	xfs_agblock_t		agbno;		/* ag start of range to free */
	xfs_extlen_t		aglen;		/* ag length of range to free */
	struct xfs_buf		*agbp;
	struct xfs_owner_info	oinfo;
	struct xfs_bmap_free	flist;
	xfs_fsblock_t		fsb;
	int			error;

	XFS_RMAP_AG_OWNER(&oinfo, XFS_RMAP_OWN_COW);
	agno = XFS_FSB_TO_AGNO(mp, imap->br_startblock);
	agbno = XFS_FSB_TO_AGBNO(mp, imap->br_startblock);
	aglen = imap->br_blockcount;
	trace_xfs_refcount_cow_decrease(mp, agno, agbno, aglen);

	/* Remove refcount btree reservation */
	xfs_bmap_init(&flist, &fsb);
	error = xfs_alloc_read_agf(mp, *tpp, agno, 0, &agbp);
	if (error)
		goto out_cancel;
	error = xfs_refcountbt_adjust_cow_refcount(mp, *tpp, agbp, agno, agbno,
			aglen, XFS_ADJUST_COW_FREE, &flist);
	if (error)
		goto out_relse;
	xfs_trans_brelse(*tpp, agbp);
	error = xfs_bmap_finish(tpp, &flist, ip);
	if (error)
		goto out;

	error = xfs_trans_roll(tpp, ip);
	if (error)
		return error;

	/* Remove rmap entry */
	if (xfs_sb_version_hasrmapbt(&mp->m_sb)) {
		xfs_bmap_init(&flist, &fsb);
		error = xfs_alloc_read_agf(mp, *tpp, agno, 0, &agbp);
		if (error)
			goto out_cancel;
		error = xfs_rmap_free(*tpp, agbp, agno, agbno, aglen, &oinfo);
		if (error)
			goto out_relse;
		xfs_trans_brelse(*tpp, agbp);
		error = xfs_bmap_finish(tpp, &flist, ip);
		if (error)
			goto out;

		error = xfs_trans_roll(tpp, ip);
		if (error)
			return error;
	}

	return error;

out_relse:
	xfs_trans_brelse(*tpp, agbp);
out_cancel:
	xfs_bmap_cancel(&flist);
out:
	return error;
}
