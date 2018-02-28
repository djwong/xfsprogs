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
#ifndef XFS_SCRUB_REPAIR_H_
#define XFS_SCRUB_REPAIR_H_

struct xfs_action_list {
	struct list_head	list;
	size_t			nr;
	bool			sorted;
};

bool xfs_action_lists_alloc(size_t nr, struct xfs_action_list **listsp);
void xfs_action_lists_free(struct xfs_action_list **listsp);

void xfs_action_list_init(struct xfs_action_list *alist);
size_t xfs_action_list_length(struct xfs_action_list *alist);
void xfs_action_list_add(struct xfs_action_list *dest,
		struct action_item *item);
void xfs_action_list_splice(struct xfs_action_list *dest,
		struct xfs_action_list *src);

void xfs_action_list_find_mustfix(struct xfs_action_list *actions,
		struct xfs_action_list *immediate_alist,
		unsigned long long *broken_primaries,
		unsigned long long *broken_secondaries);

/* Passed through to xfs_repair_metadata() */
#define ALP_REPAIR_ONLY		(XRM_REPAIR_ONLY)
#define ALP_NOFIX_COMPLAIN	(XRM_NOFIX_COMPLAIN)
#define ALP_NOPROGRESS		(1U << 31)

bool xfs_action_list_process(struct scrub_ctx *ctx, int fd,
		struct xfs_action_list *alist, unsigned int repair_flags);
void xfs_action_list_defer(struct scrub_ctx *ctx, xfs_agnumber_t agno,
		struct xfs_action_list *alist);
bool xfs_action_list_process_or_defer(struct scrub_ctx *ctx, xfs_agnumber_t agno,
		struct xfs_action_list *alist);

#endif /* XFS_SCRUB_REPAIR_H_ */
