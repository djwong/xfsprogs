/*
 * Copyright (c) 2014 Red Hat, Inc.
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
#ifndef __XFS_RMAP_BTREE_H__
#define	__XFS_RMAP_BTREE_H__

struct xfs_buf;
struct xfs_btree_cur;
struct xfs_mount;
struct xfs_rmap_list;

/* rmaps only exist on crc enabled filesystems */
#define XFS_RMAP_BLOCK_LEN	XFS_BTREE_SBLOCK_CRC_LEN

/*
 * Record, key, and pointer address macros for btree blocks.
 *
 * (note that some of these may appear unused, but they are used in userspace)
 */
#define XFS_RMAP_REC_ADDR(block, index) \
	((struct xfs_rmap_rec *) \
		((char *)(block) + XFS_RMAP_BLOCK_LEN + \
		 (((index) - 1) * sizeof(struct xfs_rmap_rec))))

#define XFS_RMAP_KEY_ADDR(block, index) \
	((struct xfs_rmap_key *) \
		((char *)(block) + XFS_RMAP_BLOCK_LEN + \
		 ((index) - 1) * sizeof(struct xfs_rmap_key)))

#define XFS_RMAP_PTR_ADDR(block, index, maxrecs) \
	((xfs_rmap_ptr_t *) \
		((char *)(block) + XFS_RMAP_BLOCK_LEN + \
		 (maxrecs) * sizeof(struct xfs_rmap_key) + \
		 ((index) - 1) * sizeof(xfs_rmap_ptr_t)))

struct xfs_btree_cur *xfs_rmapbt_init_cursor(struct xfs_mount *mp,
				struct xfs_trans *tp, struct xfs_buf *bp,
				xfs_agnumber_t agno);
int xfs_rmapbt_maxrecs(struct xfs_mount *mp, int blocklen, int leaf);

int xfs_rmap_lookup_le(struct xfs_btree_cur *cur, xfs_agblock_t	bno,
		xfs_extlen_t len, uint64_t owner, uint64_t offset, int *stat);
int xfs_rmap_lookup_eq(struct xfs_btree_cur *cur, xfs_agblock_t	bno,
		xfs_extlen_t len, uint64_t owner, uint64_t offset, int *stat);
int xfs_rmapbt_insert(struct xfs_btree_cur *rcur, xfs_agblock_t	agbno,
		xfs_extlen_t len, uint64_t owner, uint64_t offset);
int xfs_rmap_get_rec(struct xfs_btree_cur *cur, struct xfs_rmap_irec *irec,
		int *stat);

/* functions for updating the rmapbt for bmbt blocks and AG btree blocks */
int xfs_rmap_alloc(struct xfs_trans *tp, struct xfs_buf *agbp,
		   xfs_agnumber_t agno, xfs_agblock_t bno, xfs_extlen_t len,
		   struct xfs_owner_info *oinfo);
int xfs_rmap_free(struct xfs_trans *tp, struct xfs_buf *agbp,
		  xfs_agnumber_t agno, xfs_agblock_t bno, xfs_extlen_t len,
		  struct xfs_owner_info *oinfo);

/* functions for updating the rmapbt based on bmbt map/unmap operations */
int xfs_rmap_combine(struct xfs_mount *mp, struct xfs_rmap_list *rlist,
		xfs_ino_t ino, int whichfork, struct xfs_bmbt_irec *LEFT,
		struct xfs_bmbt_irec *RIGHT, struct xfs_bmbt_irec *PREV);
int xfs_rmap_lcombine(struct xfs_mount *mp, struct xfs_rmap_list *rlist,
		xfs_ino_t ino, int whichfork, struct xfs_bmbt_irec *LEFT,
		struct xfs_bmbt_irec *PREV);
int xfs_rmap_rcombine(struct xfs_mount *mp, struct xfs_rmap_list *rlist,
		xfs_ino_t ino, int whichfork, struct xfs_bmbt_irec *RIGHT,
		struct xfs_bmbt_irec *PREV);
int xfs_rmap_insert(struct xfs_mount *mp, struct xfs_rmap_list *rlist,
		xfs_ino_t ino, int whichfork, struct xfs_bmbt_irec *rec);
int xfs_rmap_delete(struct xfs_mount *mp, struct xfs_rmap_list *rlist,
		xfs_ino_t ino, int whichfork, struct xfs_bmbt_irec *rec);
int xfs_rmap_move(struct xfs_mount *mp, struct xfs_rmap_list *rlist,
		xfs_ino_t ino, int whichfork, struct xfs_bmbt_irec *PREV,
		long start_adj);
int xfs_rmap_slide(struct xfs_mount *mp, struct xfs_rmap_list *rlist,
		xfs_ino_t ino, int whichfork, struct xfs_bmbt_irec *PREV,
		long start_adj);
int xfs_rmap_resize(struct xfs_mount *mp, struct xfs_rmap_list *rlist,
		xfs_ino_t ino, int whichfork, struct xfs_bmbt_irec *PREV,
		long size_adj);

enum xfs_rmap_intent_type {
	XFS_RMAP_COMBINE,
	XFS_RMAP_LCOMBINE,
	XFS_RMAP_RCOMBINE,
	XFS_RMAP_INSERT,
	XFS_RMAP_DELETE,
	XFS_RMAP_MOVE,
	XFS_RMAP_SLIDE,
	XFS_RMAP_RESIZE,
};

struct xfs_rmap_intent {
	struct xfs_rmap_intent			*ri_next;
	enum xfs_rmap_intent_type		ri_type;
	xfs_ino_t				ri_ino;
	int					ri_whichfork;
	struct xfs_bmbt_irec			ri_prev;
	union {
		struct {
			struct xfs_bmbt_irec	left;
			struct xfs_bmbt_irec	right;
		} a;
		struct {
			long			adj;
		} b;
	} ri_u;
};

void	xfs_rmap_cancel(struct xfs_rmap_list *rlist);
int	__xfs_rmap_finish(struct xfs_mount *mp, struct xfs_trans *tp,
			struct xfs_rmap_list *rlist);
int	xfs_rmap_finish(struct xfs_mount *mp, struct xfs_trans **tpp,
			struct xfs_inode *ip, struct xfs_rmap_list *rlist);

/* functions for changing rmap ownership */
int xfs_rmap_change_extent_owner(struct xfs_mount *mp, struct xfs_inode *ip,
		xfs_ino_t ino, xfs_fileoff_t isize, struct xfs_trans *tp,
		int whichfork, xfs_ino_t new_owner,
		struct xfs_rmap_list *rlist);
int xfs_rmap_change_bmbt_owner(struct xfs_btree_cur *bcur, struct xfs_buf *bp,
		struct xfs_owner_info *old_owner,
		struct xfs_owner_info *new_owner);

extern xfs_extlen_t xfs_rmapbt_calc_size(struct xfs_mount *mp,
		unsigned long long len);
extern xfs_extlen_t xfs_rmapbt_max_size(struct xfs_mount *mp);

extern int xfs_rmapbt_alloc_reserve_pool(struct xfs_mount *mp);
extern int xfs_rmapbt_free_reserve_pool(struct xfs_mount *mp);

#endif	/* __XFS_RMAP_BTREE_H__ */
