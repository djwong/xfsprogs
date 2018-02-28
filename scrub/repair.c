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
#include "xfs.h"
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include "list.h"
#include "path.h"
#include "xfs_scrub.h"
#include "common.h"
#include "scrub.h"
#include "progress.h"
#include "repair.h"

/*
 * Prioritize action items in order of how long we can wait.
 * 0 = do it now, 10000 = do it later.
 *
 * To minimize the amount of repair work, we want to prioritize metadata
 * objects by perceived corruptness.  If CORRUPT is set, the fields are
 * just plain bad; try fixing that first.  Otherwise if XCORRUPT is set,
 * the fields could be bad, but the xref data could also be bad; we'll
 * try fixing that next.  Finally, if XFAIL is set, some other metadata
 * structure failed validation during xref, so we'll recheck this
 * metadata last since it was probably fine.
 *
 * For metadata that lie in the critical path of checking other metadata
 * (superblock, AG{F,I,FL}, inobt) we scrub and fix those things before
 * we even get to handling their dependencies, so things should progress
 * in order.
 */

/* Sort action items in severity order. */
static int
PRIO(
	struct action_item	*aitem,
	int			order)
{
	if (aitem->flags & XFS_SCRUB_OFLAG_CORRUPT)
		return order;
	else if (aitem->flags & XFS_SCRUB_OFLAG_XCORRUPT)
		return 100 + order;
	else if (aitem->flags & XFS_SCRUB_OFLAG_XFAIL)
		return 200 + order;
	else if (aitem->flags & XFS_SCRUB_OFLAG_PREEN)
		return 300 + order;
	abort();
}

/* Sort the repair items in dependency order. */
static int
xfs_action_item_priority(
	struct action_item	*aitem)
{
	switch (aitem->type) {
	case XFS_SCRUB_TYPE_SB:
	case XFS_SCRUB_TYPE_AGF:
	case XFS_SCRUB_TYPE_AGFL:
	case XFS_SCRUB_TYPE_AGI:
	case XFS_SCRUB_TYPE_BNOBT:
	case XFS_SCRUB_TYPE_CNTBT:
	case XFS_SCRUB_TYPE_INOBT:
	case XFS_SCRUB_TYPE_FINOBT:
	case XFS_SCRUB_TYPE_REFCNTBT:
	case XFS_SCRUB_TYPE_RMAPBT:
	case XFS_SCRUB_TYPE_INODE:
	case XFS_SCRUB_TYPE_BMBTD:
	case XFS_SCRUB_TYPE_BMBTA:
	case XFS_SCRUB_TYPE_BMBTC:
		return PRIO(aitem, aitem->type - 1);
	case XFS_SCRUB_TYPE_DIR:
	case XFS_SCRUB_TYPE_XATTR:
	case XFS_SCRUB_TYPE_SYMLINK:
	case XFS_SCRUB_TYPE_PARENT:
		return PRIO(aitem, XFS_SCRUB_TYPE_DIR);
	case XFS_SCRUB_TYPE_RTBITMAP:
	case XFS_SCRUB_TYPE_RTSUM:
		return PRIO(aitem, XFS_SCRUB_TYPE_RTBITMAP);
	case XFS_SCRUB_TYPE_UQUOTA:
	case XFS_SCRUB_TYPE_GQUOTA:
	case XFS_SCRUB_TYPE_PQUOTA:
		return PRIO(aitem, XFS_SCRUB_TYPE_UQUOTA);
	}
	abort();
}

/* Make sure that btrees get repaired before headers. */
static int
xfs_action_item_compare(
	void				*priv,
	struct list_head		*a,
	struct list_head		*b)
{
	struct action_item		*ra;
	struct action_item		*rb;

	ra = container_of(a, struct action_item, list);
	rb = container_of(b, struct action_item, list);

	return xfs_action_item_priority(ra) - xfs_action_item_priority(rb);
}

/*
 * Figure out which AG metadata must be fixed before we can move on
 * to the inode scan.
 */
void
xfs_action_list_find_mustfix(
	struct xfs_action_list		*alist,
	struct xfs_action_list		*immediate_alist,
	unsigned long long		*broken_primaries,
	unsigned long long		*broken_secondaries)
{
	struct action_item		*n;
	struct action_item		*aitem;

	list_for_each_entry_safe(aitem, n, &alist->list, list) {
		if (!(aitem->flags & XFS_SCRUB_OFLAG_CORRUPT))
			continue;
		switch (aitem->type) {
		case XFS_SCRUB_TYPE_RMAPBT:
			(*broken_secondaries)++;
			break;
		case XFS_SCRUB_TYPE_FINOBT:
		case XFS_SCRUB_TYPE_INOBT:
			alist->nr--;
			list_move_tail(&aitem->list, &immediate_alist->list);
			immediate_alist->nr++;
			/* fall through */
		case XFS_SCRUB_TYPE_BNOBT:
		case XFS_SCRUB_TYPE_CNTBT:
		case XFS_SCRUB_TYPE_REFCNTBT:
			(*broken_primaries)++;
			break;
		default:
			abort();
			break;
		}
	}
}

/* Allocate a certain number of repair lists for the scrub context. */
bool
xfs_action_lists_alloc(
	size_t				nr,
	struct xfs_action_list		**listsp)
{
	struct xfs_action_list		*lists;
	xfs_agnumber_t			agno;

	lists = calloc(nr, sizeof(struct xfs_action_list));
	if (!lists)
		return false;

	for (agno = 0; agno < nr; agno++)
		xfs_action_list_init(&lists[agno]);
	*listsp = lists;

	return true;
}

/* Free the repair lists. */
void
xfs_action_lists_free(
	struct xfs_action_list		**listsp)
{
	free(*listsp);
	*listsp = NULL;
}

/* Initialize repair list */
void
xfs_action_list_init(
	struct xfs_action_list		*alist)
{
	INIT_LIST_HEAD(&alist->list);
	alist->nr = 0;
	alist->sorted = false;
}

/* Number of repairs in this list. */
size_t
xfs_action_list_length(
	struct xfs_action_list		*alist)
{
	return alist->nr;
};

/* Add to the list of repairs. */
void
xfs_action_list_add(
	struct xfs_action_list		*alist,
	struct action_item		*aitem)
{
	list_add_tail(&aitem->list, &alist->list);
	alist->nr++;
	alist->sorted = false;
}

/* Splice two repair lists. */
void
xfs_action_list_splice(
	struct xfs_action_list		*dest,
	struct xfs_action_list		*src)
{
	if (src->nr == 0)
		return;

	list_splice_tail_init(&src->list, &dest->list);
	dest->nr += src->nr;
	src->nr = 0;
	dest->sorted = false;
}

/* Repair everything on this list. */
bool
xfs_action_list_process(
	struct scrub_ctx		*ctx,
	int				fd,
	struct xfs_action_list		*alist,
	unsigned int			repair_flags)
{
	struct action_item		*aitem;
	struct action_item		*n;
	enum check_outcome		fix;

	if (!alist->sorted) {
		list_sort(NULL, &alist->list, xfs_action_item_compare);
		alist->sorted = true;
	}

	list_for_each_entry_safe(aitem, n, &alist->list, list) {
		fix = xfs_repair_metadata(ctx, fd, aitem, repair_flags);
		switch (fix) {
		case CHECK_DONE:
			if (!(repair_flags & ALP_NOPROGRESS))
				progress_add(1);
			alist->nr--;
			list_del(&aitem->list);
			free(aitem);
			continue;
		case CHECK_ABORT:
			return false;
		case CHECK_RETRY:
			continue;
		case CHECK_REPAIR:
			abort();
		}
	}

	return !xfs_scrub_excessive_errors(ctx);
}

/* Defer all the repairs until phase 4. */
void
xfs_action_list_defer(
	struct scrub_ctx		*ctx,
	xfs_agnumber_t			agno,
	struct xfs_action_list		*alist)
{
	ASSERT(agno < ctx->geo.agcount);

	xfs_action_list_splice(&ctx->action_lists[agno], alist);
}

/* Run actions now and defer unfinished items for later. */
bool
xfs_action_list_process_or_defer(
	struct scrub_ctx		*ctx,
	xfs_agnumber_t			agno,
	struct xfs_action_list		*alist)
{
	bool				moveon;

	moveon = xfs_action_list_process(ctx, ctx->mnt_fd, alist,
			ALP_REPAIR_ONLY | ALP_NOPROGRESS);
	if (!moveon)
		return moveon;

	xfs_action_list_defer(ctx, agno, alist);
	return true;
}
