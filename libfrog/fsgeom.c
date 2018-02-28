/*
 * Copyright (c) 2000-2005 Silicon Graphics, Inc.
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
#include "libxfs.h"
#include "fsgeom.h"

void
xfs_report_geom(
	struct xfs_fsop_geom	*geo,
	const char		*mntpoint,
	const char		*logname,
	const char		*rtname)
{
	int			isint;
	int			lazycount;
	int			dirversion;
	int			logversion;
	int			attrversion;
	int			projid32bit;
	int			crcs_enabled;
	int			cimode;
	int			ftype_enabled;
	int			finobt_enabled;
	int			spinodes;
	int			rmapbt_enabled;
	int			reflink_enabled;

	isint = geo->logstart > 0;
	lazycount = geo->flags & XFS_FSOP_GEOM_FLAGS_LAZYSB ? 1 : 0;
	dirversion = geo->flags & XFS_FSOP_GEOM_FLAGS_DIRV2 ? 2 : 1;
	logversion = geo->flags & XFS_FSOP_GEOM_FLAGS_LOGV2 ? 2 : 1;
	attrversion = geo->flags & XFS_FSOP_GEOM_FLAGS_ATTR2 ? 2 : \
			(geo->flags & XFS_FSOP_GEOM_FLAGS_ATTR ? 1 : 0);
	cimode = geo->flags & XFS_FSOP_GEOM_FLAGS_DIRV2CI ? 1 : 0;
	projid32bit = geo->flags & XFS_FSOP_GEOM_FLAGS_PROJID32 ? 1 : 0;
	crcs_enabled = geo->flags & XFS_FSOP_GEOM_FLAGS_V5SB ? 1 : 0;
	ftype_enabled = geo->flags & XFS_FSOP_GEOM_FLAGS_FTYPE ? 1 : 0;
	finobt_enabled = geo->flags & XFS_FSOP_GEOM_FLAGS_FINOBT ? 1 : 0;
	spinodes = geo->flags & XFS_FSOP_GEOM_FLAGS_SPINODES ? 1 : 0;
	rmapbt_enabled = geo->flags & XFS_FSOP_GEOM_FLAGS_RMAPBT ? 1 : 0;
	reflink_enabled = geo->flags & XFS_FSOP_GEOM_FLAGS_REFLINK ? 1 : 0;

	printf(_(
"meta-data=%-22s isize=%-6d agcount=%u, agsize=%u blks\n"
"         =%-22s sectsz=%-5u attr=%u, projid32bit=%u\n"
"         =%-22s crc=%-8u finobt=%u, sparse=%u, rmapbt=%u\n"
"         =%-22s reflink=%u\n"
"data     =%-22s bsize=%-6u blocks=%llu, imaxpct=%u\n"
"         =%-22s sunit=%-6u swidth=%u blks\n"
"naming   =version %-14u bsize=%-6u ascii-ci=%d, ftype=%d\n"
"log      =%-22s bsize=%-6d blocks=%u, version=%d\n"
"         =%-22s sectsz=%-5u sunit=%d blks, lazy-count=%d\n"
"realtime =%-22s extsz=%-6d blocks=%lld, rtextents=%lld\n"),
		mntpoint, geo->inodesize, geo->agcount, geo->agblocks,
		"", geo->sectsize, attrversion, projid32bit,
		"", crcs_enabled, finobt_enabled, spinodes, rmapbt_enabled,
		"", reflink_enabled,
		"", geo->blocksize, (unsigned long long)geo->datablocks,
			geo->imaxpct,
		"", geo->sunit, geo->swidth,
		dirversion, geo->dirblocksize, cimode, ftype_enabled,
		isint ? _("internal log") : logname ? logname : _("external"),
			geo->blocksize, geo->logblocks, logversion,
		"", geo->logsectsize, geo->logsunit / geo->blocksize, lazycount,
		!geo->rtblocks ? _("none") : rtname ? rtname : _("external"),
		geo->rtextsize * geo->blocksize, (unsigned long long)geo->rtblocks,
			(unsigned long long)geo->rtextents);
}
