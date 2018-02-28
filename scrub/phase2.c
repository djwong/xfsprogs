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
#include <sys/types.h>
#include <sys/statvfs.h>
#include "list.h"
#include "path.h"
#include "workqueue.h"
#include "xfs_scrub.h"
#include "common.h"
#include "scrub.h"
#include "repair.h"

/* Phase 2: Check internal metadata. */

/* Scrub each AG's metadata btrees. */
static void
xfs_scan_ag_metadata(
	struct workqueue		*wq,
	xfs_agnumber_t			agno,
	void				*arg)
{
	struct scrub_ctx		*ctx = (struct scrub_ctx *)wq->wq_ctx;
	bool				*pmoveon = arg;
	struct xfs_action_list		alist;
	struct xfs_action_list		immediate_alist;
	unsigned long long		broken_primaries;
	unsigned long long		broken_secondaries;
	bool				moveon;
	char				descr[DESCR_BUFSZ];

	xfs_action_list_init(&alist);
	xfs_action_list_init(&immediate_alist);
	snprintf(descr, DESCR_BUFSZ, _("AG %u"), agno);

	/*
	 * First we scrub and fix the AG headers, because we need
	 * them to work well enough to check the AG btrees.
	 */
	moveon = xfs_scrub_ag_headers(ctx, agno, &alist);
	if (!moveon)
		goto err;

	/* Repair header damage. */
	moveon = xfs_action_list_process_or_defer(ctx, agno, &alist);
	if (!moveon)
		goto err;

	/* Now scrub the AG btrees. */
	moveon = xfs_scrub_ag_metadata(ctx, agno, &alist);
	if (!moveon)
		goto err;

	/*
	 * Figure out if we need to perform early fixing.  The only
	 * reason we need to do this is if the inobt is broken, which
	 * prevents phase 3 (inode scan) from running.  We can rebuild
	 * the inobt from rmapbt data, but if the rmapbt is broken even
	 * at this early phase then we are sunk.
	 */
	broken_secondaries = 0;
	broken_primaries = 0;
	xfs_action_list_find_mustfix(&alist, &immediate_alist,
			&broken_primaries, &broken_secondaries);
	if (broken_secondaries && !debug_tweak_on("XFS_SCRUB_FORCE_REPAIR")) {
		if (broken_primaries)
			str_info(ctx, descr,
_("Corrupt primary and secondary block mapping metadata."));
		else
			str_info(ctx, descr,
_("Corrupt secondary block mapping metadata."));
		str_info(ctx, descr,
_("Filesystem might not be repairable."));
	}

	/* Repair (inode) btree damage. */
	moveon = xfs_action_list_process_or_defer(ctx, agno, &immediate_alist);
	if (!moveon)
		goto err;

	/* Everything else gets fixed during phase 4. */
	xfs_action_list_defer(ctx, agno, &alist);

	return;
err:
	*pmoveon = false;
}

/* Scrub whole-FS metadata btrees. */
static void
xfs_scan_fs_metadata(
	struct workqueue		*wq,
	xfs_agnumber_t			agno,
	void				*arg)
{
	struct scrub_ctx		*ctx = (struct scrub_ctx *)wq->wq_ctx;
	bool				*pmoveon = arg;
	struct xfs_action_list		alist;
	bool				moveon;

	xfs_action_list_init(&alist);
	moveon = xfs_scrub_fs_metadata(ctx, &alist);
	if (!moveon)
		*pmoveon = false;

	xfs_action_list_defer(ctx, agno, &alist);
}

/* Scan all filesystem metadata. */
bool
xfs_scan_metadata(
	struct scrub_ctx	*ctx)
{
	struct xfs_action_list	alist;
	struct workqueue	wq;
	xfs_agnumber_t		agno;
	bool			moveon = true;
	int			ret;

	ret = workqueue_create(&wq, (struct xfs_mount *)ctx,
			scrub_nproc_workqueue(ctx));
	if (ret) {
		str_info(ctx, ctx->mntpoint, _("Could not create workqueue."));
		return false;
	}

	/*
	 * In case we ever use the primary super scrubber to perform fs
	 * upgrades (followed by a full scrub), do that before we launch
	 * anything else.
	 */
	xfs_action_list_init(&alist);
	moveon = xfs_scrub_primary_super(ctx, &alist);
	if (!moveon)
		return moveon;
	moveon = xfs_action_list_process_or_defer(ctx, 0, &alist);
	if (!moveon)
		return moveon;

	for (agno = 0; moveon && agno < ctx->geo.agcount; agno++) {
		ret = workqueue_add(&wq, xfs_scan_ag_metadata, agno, &moveon);
		if (ret) {
			moveon = false;
			str_info(ctx, ctx->mntpoint,
_("Could not queue AG %u scrub work."), agno);
			goto out;
		}
	}

	if (!moveon)
		goto out;

	ret = workqueue_add(&wq, xfs_scan_fs_metadata, 0, &moveon);
	if (ret) {
		moveon = false;
		str_info(ctx, ctx->mntpoint,
_("Could not queue filesystem scrub work."));
		goto out;
	}

out:
	workqueue_destroy(&wq);
	return moveon;
}

/* Estimate how much work we're going to do. */
bool
xfs_estimate_metadata_work(
	struct scrub_ctx	*ctx,
	uint64_t		*items,
	unsigned int		*nr_threads,
	int			*rshift)
{
	*items = xfs_scrub_estimate_ag_work(ctx);
	*nr_threads = scrub_nproc(ctx);
	*rshift = 0;
	return true;
}
