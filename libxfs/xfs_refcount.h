/*
 * Copyright (c) 2000,2005 Silicon Graphics, Inc.
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
#ifndef __XFS_REFCOUNT_H__
#define __XFS_REFCOUNT_H__

extern int xfs_refcountbt_lookup_le(struct xfs_btree_cur *cur,
		xfs_agblock_t bno, int *stat);
extern int xfs_refcountbt_lookup_ge(struct xfs_btree_cur *cur,
		xfs_agblock_t bno, int *stat);
extern int xfs_refcountbt_get_rec(struct xfs_btree_cur *cur,
		struct xfs_refcount_irec *irec, int *stat);

extern int xfs_refcount_increase(struct xfs_mount *mp, struct xfs_trans *tp,
		struct xfs_buf *agbp, xfs_agnumber_t agno, xfs_agblock_t agbno,
		xfs_extlen_t  aglen, struct xfs_bmap_free *flist);
extern int xfs_refcount_decrease(struct xfs_mount *mp, struct xfs_trans *tp,
		struct xfs_buf *agbp, xfs_agnumber_t agno, xfs_agblock_t agbno,
		xfs_extlen_t aglen, struct xfs_bmap_free *flist,
		struct xfs_owner_info *oinfo);

extern int xfs_refcount_put_extent(struct xfs_mount *mp, struct xfs_trans *tp,
		struct xfs_bmap_free *flist, xfs_fsblock_t fsbno,
		xfs_filblks_t len, struct xfs_owner_info *oinfo);

extern int xfs_refcount_find_shared(struct xfs_mount *mp, xfs_agnumber_t agno,
		xfs_agblock_t agbno, xfs_extlen_t aglen, xfs_agblock_t *fbno,
		xfs_extlen_t *flen, bool find_maximal);

enum xfs_adjust_cow {
	XFS_ADJUST_COW_ALLOC	= 0,
	XFS_ADJUST_COW_FREE	= -1,
};

extern int xfs_refcountbt_cow_alloc(struct xfs_mount *mp, struct xfs_trans *tp,
		struct xfs_bmbt_irec *imap, struct xfs_bmap_free *flist);
extern int xfs_refcountbt_cow_free(struct xfs_mount *mp, struct xfs_inode *ip,
		struct xfs_trans **tpp, struct xfs_bmbt_irec *imap);

#endif	/* __XFS_REFCOUNT_H__ */
