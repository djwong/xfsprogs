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
 * Reserved blocks can be obtained by passing the reservation descriptor to
 * the allocator via the resv field in struct xfs_alloc_arg.  For anything
 * that grows in the free space (such as the rmap btree), use the
 * XFS_AG_RESV_AGFL flag to tell the per-AG reservation code to hold the
 * reservation unless the AGFL is trying to allocate blocks.  It might seem
 * a little funny to maintain a reservoir of blocks to feed another reservoir,
 * but the AGFL only holds enough blocks to get through the next transaction.
 * The per-AG reservation is to ensure (we hope) that each AG never runs out
 * of blocks.
 *
 * The xfs_ag_resv structure maintains a reservation in a specific AG; this
 * structure can be passed via struct xfs_alloc_arg to allocate the reserved
 * space, and the alloc_block/free_block functions should be used to count
 * allocations and frees from the reservation.  The two resv_type* functions
 * are used to update ag_max_usable.
 */

static inline xfs_extlen_t
resv_needed(
	struct xfs_ag_resv		*ar)
{
	ASSERT(ar->ar_blocks >= ar->ar_inuse);
	return ar->ar_blocks - ar->ar_inuse;
}

struct xfs_ag_resv xfs_ag_agfl_resv = { NULL };

/*
 * Are we critically low on blocks?  For now we'll define that as the number
 * of blocks we can get our hands on being less than 10% of what we reserved
 * or less than some arbitrary number (eight), or if the free space is less
 * than all the reservations.
 */
bool
xfs_ag_resv_critical(
	struct xfs_ag_resv		*ar,
	struct xfs_perag		*pag)
{
	xfs_extlen_t			avail;

	if (pag->pagf_freeblks < pag->pag_reserved_blocks)
		return true;
	avail = pag->pagf_freeblks - pag->pag_reserved_blocks;
	if (ar->ar_flags & XFS_AG_RESV_AGFL)
		avail += pag->pag_agfl_reserved_blocks;
	else
		avail += resv_needed(ar);

	return avail < ar->ar_blocks / 10 || avail < 8;
}

/*
 * How many blocks are reserved but not used, and therefore must not be
 * allocated away?
 */
xfs_extlen_t
xfs_ag_resv_needed(
	struct xfs_ag_resv		*ar,
	struct xfs_perag		*pag)
{
	xfs_extlen_t			len;

	/* Preserve all allocated blocks except those reserved for AGFL */
	if (ar == &xfs_ag_agfl_resv) {
		ASSERT(pag->pag_reserved_blocks >=
		       pag->pag_agfl_reserved_blocks);
		len = pag->pag_reserved_blocks - pag->pag_agfl_reserved_blocks;
		trace_xfs_ag_resv_agfl_needed(pag->pag_mount, pag->pag_agno,
				-1, -1, len, pag);
		return len;
	}

	/* Preserve all allocated blocks */
	if (ar == NULL) {
		trace_xfs_ag_resv_nores_needed(pag->pag_mount, pag->pag_agno,
				-2, -2, pag->pag_reserved_blocks, pag);
		return pag->pag_reserved_blocks;
	}

	/* Preserve all blocks except our reservation */
	ASSERT(pag->pag_reserved_blocks >= resv_needed(ar));
	len = pag->pag_reserved_blocks - resv_needed(ar);
	trace_xfs_ag_resv_needed(ar->ar_mount, ar->ar_agno, ar->ar_blocks,
			ar->ar_inuse, len, pag);
	return len;
}

/* Free a per-AG reservation. */
int
xfs_ag_resv_free(
	struct xfs_ag_resv		*ar,
	struct xfs_perag		*pag)
{
	int				error;

	trace_xfs_ag_resv_free(ar->ar_mount, ar->ar_agno, ar->ar_blocks,
			ar->ar_inuse, pag);
	pag->pag_reserved_blocks -= resv_needed(ar);
	if (ar->ar_flags & XFS_AG_RESV_AGFL)
		pag->pag_agfl_reserved_blocks -= resv_needed(ar);
	error = xfs_mod_fdblocks(ar->ar_mount, resv_needed(ar), false);
	kmem_free(ar);

	if (error)
		trace_xfs_ag_resv_free_error(ar->ar_mount, ar->ar_agno,
				error, _RET_IP_);
	return error;
}

/* Create a per-AG block reservation. */
int
xfs_ag_resv_init(
	struct xfs_mount		*mp,
	struct xfs_perag		*pag,
	xfs_extlen_t			blocks,
	xfs_extlen_t			inuse,
	unsigned int			flags,
	struct xfs_ag_resv		**par)
{
	struct xfs_ag_resv		*ar;
	int				error;

	if (blocks < inuse) {
		mp->m_ag_max_usable -= inuse - blocks;
		blocks = inuse;
	}
	ar = kmem_alloc(sizeof(struct xfs_ag_resv), KM_SLEEP);
	ar->ar_mount = mp;
	ar->ar_agno = pag->pag_agno;
	ar->ar_blocks = blocks;
	ar->ar_inuse = inuse;
	ar->ar_flags = flags;

	pag->pag_reserved_blocks += resv_needed(ar);
	if (ar->ar_flags & XFS_AG_RESV_AGFL)
		pag->pag_agfl_reserved_blocks += resv_needed(ar);
	*par = ar;

	error = xfs_mod_fdblocks(mp, -(int64_t)resv_needed(ar), false);
	trace_xfs_ag_resv_init(mp, pag->pag_agno, blocks, inuse, pag);
	if (!error && pag->pag_reserved_blocks > pag->pagf_freeblks)
		error = -ENOSPC;

	if (error)
		trace_xfs_ag_resv_init_error(ar->ar_mount, ar->ar_agno,
				error, _RET_IP_);
	return error;
}

/* Allocate a block from the reservation. */
void
xfs_ag_resv_alloc_block(
	struct xfs_ag_resv		*ar,
	struct xfs_trans		*tp,
	struct xfs_perag		*pag)
{
	if (!ar)
		return;

	trace_xfs_ag_resv_alloc_block(ar->ar_mount, ar->ar_agno, ar->ar_blocks,
			ar->ar_inuse, pag);
	ar->ar_inuse++;
	if (ar->ar_inuse <= ar->ar_blocks) {
		pag->pag_reserved_blocks--;
		if (ar->ar_flags & XFS_AG_RESV_AGFL)
			pag->pag_agfl_reserved_blocks--;
		xfs_trans_mod_sb(tp, XFS_TRANS_SB_FDBLOCKS, 1);
	} else {
		ar->ar_blocks++;
		ar->ar_mount->m_ag_max_usable--;
	}
}

/* Free a block to the reservation. */
void
xfs_ag_resv_free_block(
	struct xfs_ag_resv		*ar,
	struct xfs_trans		*tp,
	struct xfs_perag		*pag)
{
	if (!ar)
		return;

	trace_xfs_ag_resv_free_block(ar->ar_mount, ar->ar_agno, ar->ar_blocks,
			ar->ar_inuse, pag);
	ar->ar_inuse--;
	pag->pag_reserved_blocks++;
	if (ar->ar_flags & XFS_AG_RESV_AGFL)
		pag->pag_agfl_reserved_blocks++;
	xfs_mod_fdblocks(ar->ar_mount, -1, false);
}
