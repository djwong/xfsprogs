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
#include <dirent.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include "list.h"
#include "path.h"
#include "workqueue.h"
#include "xfs_scrub.h"
#include "common.h"
#include "progress.h"
#include "scrub.h"
#include "repair.h"
#include "vfs.h"

/* Phase 4: Repair filesystem. */

/* Fix all the problems in our per-AG list. */
static void
xfs_repair_ag(
	struct workqueue		*wq,
	xfs_agnumber_t			agno,
	void				*priv)
{
	struct scrub_ctx		*ctx = (struct scrub_ctx *)wq->wq_ctx;
	bool				*pmoveon = priv;
	struct xfs_action_list		*alist;
	size_t				unfixed;
	size_t				new_unfixed;
	unsigned int			flags = 0;
	bool				moveon;

	alist = &ctx->action_lists[agno];
	unfixed = xfs_action_list_length(alist);

	/* Repair anything broken until we fail to make progress. */
	do {
		moveon = xfs_action_list_process(ctx, ctx->mnt_fd, alist, flags);
		if (!moveon) {
			*pmoveon = false;
			return;
		}
		new_unfixed = xfs_action_list_length(alist);
		if (new_unfixed == unfixed)
			break;
		unfixed = new_unfixed;
	} while (unfixed > 0 && *pmoveon);

	if (!*pmoveon)
		return;

	/* Try once more, but this time complain if we can't fix things. */
	flags |= ALP_NOFIX_COMPLAIN;
	moveon = xfs_action_list_process(ctx, ctx->mnt_fd, alist, flags);
	if (!moveon)
		*pmoveon = false;
}

/* Process all the action items. */
static bool
xfs_process_action_items(
	struct scrub_ctx		*ctx)
{
	struct workqueue		wq;
	xfs_agnumber_t			agno;
	bool				moveon = true;
	int				ret;

	ret = workqueue_create(&wq, (struct xfs_mount *)ctx,
			scrub_nproc_workqueue(ctx));
	if (ret) {
		str_error(ctx, ctx->mntpoint, _("Could not create workqueue."));
		return false;
	}
	for (agno = 0; agno < ctx->geo.agcount; agno++) {
		if (xfs_action_list_length(&ctx->action_lists[agno]) > 0) {
			ret = workqueue_add(&wq, xfs_repair_ag, agno, &moveon);
			if (ret) {
				moveon = false;
				str_error(ctx, ctx->mntpoint,
_("Could not queue repair work."));
				break;
			}
		}
		if (!moveon)
			break;
	}
	workqueue_destroy(&wq);

	pthread_mutex_lock(&ctx->lock);
	if (moveon && ctx->errors_found == 0 && want_fstrim) {
		fstrim(ctx);
		progress_add(1);
	}
	pthread_mutex_unlock(&ctx->lock);

	return moveon;
}

/* Fix everything that needs fixing. */
bool
xfs_repair_fs(
	struct scrub_ctx		*ctx)
{
	return xfs_process_action_items(ctx);
}

/* Estimate how much work we're going to do. */
bool
xfs_estimate_repair_work(
	struct scrub_ctx	*ctx,
	uint64_t		*items,
	unsigned int		*nr_threads,
	int			*rshift)
{
	xfs_agnumber_t		agno;
	size_t			need_fixing = 0;

	for (agno = 0; agno < ctx->geo.agcount; agno++)
		need_fixing += xfs_action_list_length(&ctx->action_lists[agno]);
	need_fixing++;
	*items = need_fixing;
	*nr_threads = scrub_nproc(ctx) + 1;
	*rshift = 0;
	return true;
}
