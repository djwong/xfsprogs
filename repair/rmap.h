/*
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
#ifndef RMAP_H_
#define RMAP_H_

extern bool collect_rmaps;

extern bool needs_rmap_work(struct xfs_mount *);

extern void init_rmaps(struct xfs_mount *);
extern void free_rmaps(struct xfs_mount *);

extern int add_rmap(struct xfs_mount *, xfs_ino_t, int, struct xfs_bmbt_irec *);
extern int add_ag_rmap(struct xfs_mount *, xfs_agnumber_t agno,
		xfs_agblock_t agbno, xfs_extlen_t len, uint64_t owner);
extern int add_bmbt_rmap(struct xfs_mount *, xfs_ino_t, int, xfs_fsblock_t);
extern int fold_raw_rmaps(struct xfs_mount *mp, xfs_agnumber_t agno);

extern int add_fixed_ag_rmap_data(struct xfs_mount *, xfs_agnumber_t);
extern int store_ag_btree_rmap_data(struct xfs_mount *, xfs_agnumber_t);

extern size_t rmap_record_count(struct xfs_mount *, xfs_agnumber_t);
extern int init_rmap_cursor(xfs_agnumber_t, struct xfs_slab_cursor **);
extern void rmap_avoid_check(void);
extern int check_rmaps(struct xfs_mount *, xfs_agnumber_t);

extern void fix_freelist(struct xfs_mount *, xfs_agnumber_t, bool);
extern void rmap_store_agflcount(struct xfs_mount *, xfs_agnumber_t, int);

#endif /* RMAP_H_ */
