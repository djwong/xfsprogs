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
#include "xfs_alloc.h"
#include "xfs_trace.h"
#include "xfs_cksum.h"
#include "xfs_trans.h"
#include "xfs_bit.h"
#include "xfs_bmap.h"
#include "xfs_bmap_btree.h"
#include "xfs_ag_resv.h"
#include "xfs_trans_space.h"
#include "xfs_refcount_btree.h"
#include "xfs_rmap_btree.h"
#include "xfs_btree.h"

/*
 * Per-AG Block Reservations
 *
 * For some kinds of allocation group metadata structures, it is advantageous
 * to reserve a small number of blocks in each AG so that future expansions of
 * that data structure do not encounter ENOSPC because errors during a btree
 * split cause the filesystem to go offline.
 *
 * Prior to the introduction of reflink, this wasn't an issue because the free
 * space btrees maintain a reserve of space (the AGFL) to handle any expansion
 * that may be necessary; and allocations of other metadata (inodes, BMBT,
 * dir/attr) aren't restricted to a single AG.  However, with reflink it is
 * possible to allocate all the space in an AG, have subsequent reflink/CoW
 * activity expand the refcount btree, and discover that there's no space left
 * to handle that expansion.  Since we can calculate the maximum size of the
 * refcount btree, we can reserve space for it and avoid ENOSPC.
 *
 * Handling per-AG reservations consists of three changes to the allocator's
 * behavior:  First, because these reservations are always needed, we decrease
 * the ag_max_usable counter to reflect the size of the AG after the reserved
 * blocks are taken.  Second, the reservations must be reflected in the
 * fdblocks count to maintain proper accounting.  Third, each AG must maintain
 * its own reserved block counter so that we can calculate the amount of space
 * that must remain free to maintain the reservations.  Fourth, the "remaining
 * reserved blocks" count must be used when calculating the length of the
 * longest free extent in an AG and to clamp maxlen in the per-AG allocation
 * functions.  In other words, we maintain a virtual allocation via in-core
 * accounting tricks so that we don't have to clean up after a crash. :)
 *
 * Reserved blocks can be managed by passing one of the enum xfs_ag_resv_type
 * values via struct xfs_alloc_arg or directly to the xfs_free_extent
 * function.  It might seem a little funny to maintain a reservoir of blocks
 * to feed another reservoir, but the AGFL only holds enough blocks to get
 * through the next transaction.  The per-AG reservation is to ensure (we
 * hope) that each AG never runs out of blocks.  Each data structure wanting
 * to use the reservation system should update ask/used in xfs_ag_resv_init.
 */

/*
 * Are we critically low on blocks?  For now we'll define that as the number
 * of blocks we can get our hands on being less than 10% of what we reserved
 * or less than some arbitrary number (eight).
 */
bool
xfs_ag_resv_critical(
	struct xfs_perag		*pag,
	enum xfs_ag_resv_type		type)
{
	xfs_extlen_t			avail;
	xfs_extlen_t			orig;

	switch (type) {
	case XFS_AG_RESV_METADATA:
		avail = pag->pagf_freeblks - pag->pag_agfl_resv.ar_reserved;
		orig = pag->pag_meta_resv.ar_asked;
		break;
	case XFS_AG_RESV_AGFL:
		avail = pag->pagf_freeblks + pag->pagf_flcount -
			pag->pag_meta_resv.ar_reserved;
		orig = pag->pag_agfl_resv.ar_asked;
		break;
	default:
		ASSERT(0);
		return false;
	}

	trace_xfs_ag_resv_critical(pag, type, avail);

	return avail < orig / 10 || avail < XFS_BTREE_MAXLEVELS;
}

/*
 * How many blocks are reserved but not used, and therefore must not be
 * allocated away?
 */
xfs_extlen_t
xfs_ag_resv_needed(
	struct xfs_perag		*pag,
	enum xfs_ag_resv_type		type)
{
	xfs_extlen_t			len;

	len = pag->pag_meta_resv.ar_reserved + pag->pag_agfl_resv.ar_reserved;
	switch (type) {
	case XFS_AG_RESV_METADATA:
	case XFS_AG_RESV_AGFL:
		len -= XFS_AG_RESV(pag, type)->ar_reserved;
		break;
	case XFS_AG_RESV_NONE:
		/* empty */
		break;
	default:
		ASSERT(0);
	}

	trace_xfs_ag_resv_needed(pag, type, len);

	return len;
}

/* Clean out a reservation */
static int
ag_resv_free(
	struct xfs_perag		*pag,
	enum xfs_ag_resv_type		type)
{
	struct xfs_ag_resv		*resv;
	struct xfs_ag_resv		t;
	int				error;

	trace_xfs_ag_resv_free(pag, type, 0);

	resv = XFS_AG_RESV(pag, type);
	t = *resv;
	resv->ar_reserved = 0;
	resv->ar_asked = 0;
	pag->pag_mount->m_ag_max_usable += t.ar_asked;

	if (type == XFS_AG_RESV_AGFL)
		error = xfs_mod_fdblocks(pag->pag_mount, t.ar_asked, false);
	else
		error = xfs_mod_fdblocks(pag->pag_mount, t.ar_reserved, false);
	if (error)
		trace_xfs_ag_resv_free_error(pag->pag_mount, pag->pag_agno,
				error, _RET_IP_);
	return error;
}

/* Free a per-AG reservation. */
int
xfs_ag_resv_free(
	struct xfs_perag		*pag)
{
	int				error = 0;
	int				err2;

	err2 = ag_resv_free(pag, XFS_AG_RESV_AGFL);
	if (err2 && !error)
		error = err2;
	err2 = ag_resv_free(pag, XFS_AG_RESV_METADATA);
	if (err2 && !error)
		error = err2;
	return error;
}

static int
ag_resv_init(
	struct xfs_perag		*pag,
	enum xfs_ag_resv_type		type,
	xfs_extlen_t			ask,
	xfs_extlen_t			used)
{
	struct xfs_mount		*mp = pag->pag_mount;
	struct xfs_ag_resv		*resv;
	int				error;

	resv = XFS_AG_RESV(pag, type);
	if (used > ask)
		ask = used;
	resv->ar_asked = ask;
	resv->ar_reserved = ask - used;
	mp->m_ag_max_usable -= ask;

	trace_xfs_ag_resv_init(pag, type, ask);

	if (type == XFS_AG_RESV_AGFL)
		error = xfs_mod_fdblocks(mp, -(int64_t)resv->ar_asked, false);
	else
		error = xfs_mod_fdblocks(mp, -(int64_t)resv->ar_reserved, false);
	if (error)
		trace_xfs_ag_resv_init_error(pag->pag_mount, pag->pag_agno,
				error, _RET_IP_);

	return error;
}

/* Create a per-AG block reservation. */
int
xfs_ag_resv_init(
	struct xfs_perag		*pag)
{
	xfs_extlen_t			ask;
	xfs_extlen_t			used;
	int				error = 0;
	int				err2;

	if (pag->pag_meta_resv.ar_asked)
		goto init_agfl;

	/* Create the metadata reservation. */
	ask = used = 0;

	err2 = xfs_refcountbt_calc_reserves(pag->pag_mount, pag->pag_agno,
			&ask, &used);
	if (err2 && !error)
		error = err2;

	err2 = ag_resv_init(pag, XFS_AG_RESV_METADATA, ask, used);
	if (err2 && !error)
		error = err2;

init_agfl:
	if (pag->pag_agfl_resv.ar_asked)
		return error;

	/* Create the AGFL metadata reservation */
	ask = used = 0;

	err2 = xfs_rmapbt_calc_reserves(pag->pag_mount, pag->pag_agno,
			&ask, &used);
	if (err2 && !error)
		error = err2;

	err2 = ag_resv_init(pag, XFS_AG_RESV_AGFL, ask, used);
	if (err2 && !error)
		error = err2;

	return error;
}

/* Allocate a block from the reservation. */
void
xfs_ag_resv_alloc_extent(
	struct xfs_perag		*pag,
	enum xfs_ag_resv_type		type,
	struct xfs_alloc_arg		*args)
{
	struct xfs_ag_resv		*resv;
	xfs_extlen_t			leftover;
	uint				field;

	trace_xfs_ag_resv_alloc_extent(pag, type, args->len);

	switch (type) {
	case XFS_AG_RESV_METADATA:
	case XFS_AG_RESV_AGFL:
		resv = XFS_AG_RESV(pag, type);
		break;
	default:
		ASSERT(0);
		/* fall through */
	case XFS_AG_RESV_NONE:
		field = args->wasdel ? XFS_TRANS_SB_RES_FDBLOCKS :
				       XFS_TRANS_SB_FDBLOCKS;
		xfs_trans_mod_sb(args->tp, field, -(int64_t)args->len);
		return;
	}

	if (args->len > resv->ar_reserved) {
		leftover = args->len - resv->ar_reserved;
		if (type != XFS_AG_RESV_AGFL)
			xfs_trans_mod_sb(args->tp, XFS_TRANS_SB_FDBLOCKS,
					-(int64_t)leftover);
		resv->ar_reserved = 0;
	} else
		resv->ar_reserved -= args->len;
}

/* Free a block to the reservation. */
void
xfs_ag_resv_free_extent(
	struct xfs_perag		*pag,
	enum xfs_ag_resv_type		type,
	struct xfs_trans		*tp,
	xfs_extlen_t			len)
{
	xfs_extlen_t			leftover;
	struct xfs_ag_resv		*resv;

	trace_xfs_ag_resv_free_extent(pag, type, len);

	switch (type) {
	case XFS_AG_RESV_METADATA:
	case XFS_AG_RESV_AGFL:
		resv = XFS_AG_RESV(pag, type);
		break;
	default:
		ASSERT(0);
		/* fall through */
	case XFS_AG_RESV_NONE:
		xfs_trans_mod_sb(tp, XFS_TRANS_SB_FDBLOCKS, (int64_t)len);
		return;
	}

	if (resv->ar_reserved + len > resv->ar_asked) {
		leftover = resv->ar_reserved + len - resv->ar_asked;
		if (type != XFS_AG_RESV_AGFL)
			xfs_trans_mod_sb(tp, XFS_TRANS_SB_FDBLOCKS,
					(int64_t)leftover);
		resv->ar_reserved = resv->ar_asked;
	} else
		resv->ar_reserved += len;
}
