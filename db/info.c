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
#include "libxfs.h"
#include "command.h"
#include "init.h"
#include "output.h"
#include "fsgeom.h"

static void
info_help(void)
{
	dbprintf(_(
"\n"
" Pretty-prints the filesystem geometry as derived from the superblock.\n"
" The output has the same format as mkfs.\n"
"\n"
));

}

static int
info_f(
	int			argc,
	char			**argv)
{
	struct xfs_fsop_geom	geo;
	int			error;

	error = -libxfs_fs_geometry(&mp->m_sb, &geo,
			XFS_FS_GEOM_MAX_STRUCT_VER);
	if (error) {
		dbprintf(_("could not obtain geometry\n"));
		exitcode = 1;
		return 0;
	}

	xfs_report_geom(&geo, fsdevice, x.logname, x.rtname);
	return 0;
}

static const struct cmdinfo info_cmd = {
	.name =		"info",
	.altname =	"i",
	.cfunc =	info_f,
	.argmin =	0,
	.argmax =	0,
	.canpush =	0,
	.args =		NULL,
	.oneline =	N_("dump superblock info"),
	.help =		info_help,
};

void
info_init(void)
{
	add_command(&info_cmd);
}
