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
#ifndef __XFS_PERAG_POOL_H__
#define	__XFS_PERAG_POOL_H__

/* Free a per-AG reservation type. */
static inline void
xfs_ag_resv_type_free(
	struct xfs_mount		*mp,
	xfs_extlen_t			blocks)
{
	mp->m_ag_max_usable += blocks;
}

/* Allocate a per-AG reservation type. */
static inline void
xfs_ag_resv_type_init(
	struct xfs_mount		*mp,
	xfs_extlen_t			blocks)
{
	mp->m_ag_max_usable -= blocks;
}

#define XFS_AG_RESV_AGFL	1	/* reservation feeds the agfl */

struct xfs_ag_resv {
	struct xfs_mount		*ar_mount;
	xfs_agnumber_t			ar_agno;
	/* number of blocks reserved for our client */
	xfs_extlen_t			ar_blocks;
	/* number of blocks in use */
	xfs_extlen_t			ar_inuse;
	unsigned int			ar_flags;
};

extern struct xfs_ag_resv	xfs_ag_agfl_resv;

int xfs_ag_resv_free(struct xfs_ag_resv *ar, struct xfs_perag *pag);
int xfs_ag_resv_init(struct xfs_mount *mp, struct xfs_perag *pag,
		xfs_extlen_t blocks, xfs_extlen_t inuse, unsigned int flags,
		struct xfs_ag_resv **par);

xfs_extlen_t xfs_ag_resv_needed(struct xfs_ag_resv *ar,
		struct xfs_perag *pag);

/* How many blocks have we reserved? */
static inline xfs_extlen_t xfs_ag_resv_blocks(
	struct xfs_ag_resv	*ar)
{
	return ar->ar_blocks;
}

void xfs_ag_resv_alloc_block(struct xfs_ag_resv *ar, struct xfs_trans *tp,
		struct xfs_perag *pag);
void xfs_ag_resv_free_block(struct xfs_ag_resv *ar, struct xfs_trans *tp,
		struct xfs_perag *pag);

bool xfs_ag_resv_critical(struct xfs_ag_resv *ar, struct xfs_perag *pag);

#endif	/* __XFS_PERAG_POOL_H__ */
