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
#ifndef __XFS_REFCOUNT_BTREE_H__
#define	__XFS_REFCOUNT_BTREE_H__

/*
 * Reference Count Btree on-disk structures
 */

struct xfs_buf;
struct xfs_btree_cur;
struct xfs_mount;

/*
 * Btree block header size
 */
#define XFS_REFCOUNT_BLOCK_LEN	XFS_BTREE_SBLOCK_CRC_LEN

/*
 * Record, key, and pointer address macros for btree blocks.
 *
 * (note that some of these may appear unused, but they are used in userspace)
 */
#define XFS_REFCOUNT_REC_ADDR(block, index) \
	((struct xfs_refcount_rec *) \
		((char *)(block) + \
		 XFS_REFCOUNT_BLOCK_LEN + \
		 (((index) - 1) * sizeof(struct xfs_refcount_rec))))

#define XFS_REFCOUNT_KEY_ADDR(block, index) \
	((struct xfs_refcount_key *) \
		((char *)(block) + \
		 XFS_REFCOUNT_BLOCK_LEN + \
		 ((index) - 1) * sizeof(struct xfs_refcount_key)))

#define XFS_REFCOUNT_PTR_ADDR(block, index, maxrecs) \
	((xfs_refcount_ptr_t *) \
		((char *)(block) + \
		 XFS_REFCOUNT_BLOCK_LEN + \
		 (maxrecs) * sizeof(struct xfs_refcount_key) + \
		 ((index) - 1) * sizeof(xfs_refcount_ptr_t)))

extern struct xfs_btree_cur *xfs_refcountbt_init_cursor(struct xfs_mount *mp,
		struct xfs_trans *tp, struct xfs_buf *agbp, xfs_agnumber_t agno,
		struct xfs_bmap_free *flist);
extern int xfs_refcountbt_maxrecs(struct xfs_mount *mp, int blocklen,
		bool leaf);

extern xfs_extlen_t xfs_refcountbt_calc_size(struct xfs_mount *mp,
		unsigned long long len);
extern xfs_extlen_t xfs_refcountbt_max_size(struct xfs_mount *mp);

extern int xfs_refcountbt_alloc_reserve_pool(struct xfs_mount *mp);
extern int xfs_refcountbt_free_reserve_pool(struct xfs_mount *mp);

#endif	/* __XFS_REFCOUNT_BTREE_H__ */
