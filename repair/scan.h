/*
 * Copyright (c) 2000-2001,2005 Silicon Graphics, Inc.
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
#ifndef _XR_SCAN_H
#define _XR_SCAN_H

struct blkmap;

int scan_lbtree(
	xfs_fsblock_t	root,
	int		nlevels,
	int		(*func)(struct xfs_btree_block	*block,
				int			level,
				int			type,
				int			whichfork,
				xfs_fsblock_t		bno,
				xfs_ino_t		ino,
				xfs_rfsblock_t		*tot,
				uint64_t		*nex,
				struct blkmap		**blkmapp,
				bmap_cursor_t		*bm_cursor,
				int			isroot,
				int			check_dups,
				int			*dirty,
				uint64_t		magic,
				void			*priv),
	int		type,
	int		whichfork,
	xfs_ino_t	ino,
	xfs_rfsblock_t	*tot,
	uint64_t	*nex,
	struct blkmap	**blkmapp,
	bmap_cursor_t	*bm_cursor,
	int		isroot,
	int		check_dups,
	uint64_t	magic,
	void		*priv,
	const struct xfs_buf_ops *ops);

int scan_bmapbt(
	struct xfs_btree_block	*block,
	int			level,
	int			type,
	int			whichfork,
	xfs_fsblock_t		bno,
	xfs_ino_t		ino,
	xfs_rfsblock_t		*tot,
	uint64_t		*nex,
	struct blkmap		**blkmapp,
	bmap_cursor_t		*bm_cursor,
	int			isroot,
	int			check_dups,
	int			*dirty,
	uint64_t		magic,
	void			*priv);

void
scan_ags(
	struct xfs_mount	*mp,
	int			scan_threads);

struct rmap_priv {
	struct aghdr_cnts	*agcnts;
	struct xfs_rmap_irec	high_key;
	struct xfs_rmap_irec	last_rec;
	xfs_agblock_t		nr_blocks;
};

int
process_rtrmap_reclist(
	struct xfs_mount	*mp,
	struct xfs_rtrmap_rec	*rp,
	int			numrecs,
	struct xfs_rmap_irec	*last_rec,
	struct xfs_rmap_irec	*high_key,
	const char		*name);

int scan_rtrmapbt(
	struct xfs_btree_block	*block,
	int			level,
	int			type,
	int			whichfork,
	xfs_fsblock_t		bno,
	xfs_ino_t		ino,
	xfs_rfsblock_t		*tot,
	uint64_t		*nex,
	struct blkmap		**blkmapp,
	bmap_cursor_t		*bm_cursor,
	int			isroot,
	int			check_dups,
	int			*dirty,
	uint64_t		magic,
	void			*priv);

#endif /* _XR_SCAN_H */
