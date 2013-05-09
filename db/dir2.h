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

extern const field_t	dir2_flds[];
extern const field_t	dir2_hfld[];
extern const field_t	dir2_block_tail_flds[];
extern const field_t	dir2_data_free_flds[];
extern const field_t	dir2_data_hdr_flds[];
extern const field_t	dir2_data_union_flds[];
extern const field_t	dir2_free_hdr_flds[];
extern const field_t	dir2_leaf_entry_flds[];
extern const field_t	dir2_leaf_hdr_flds[];
extern const field_t	dir2_leaf_tail_flds[];

extern const field_t	da_blkinfo_flds[];
extern const field_t	da_node_entry_flds[];
extern const field_t	da_node_hdr_flds[];

extern int	dir2_data_union_size(void *obj, int startoff, int idx);
extern int	dir2_size(void *obj, int startoff, int idx);
