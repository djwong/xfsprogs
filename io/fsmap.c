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

#include "platform_defs.h"
#include "command.h"
#include "init.h"
#include "io.h"
#include "input.h"

static cmdinfo_t fsmap_cmd;

static void
fsmap_help(void)
{
	printf(_(
"\n"
" prints the block mapping for an XFS filesystem"
"\n"
" Example:\n"
" 'fsmap -vp' - tabular format verbose map, including unwritten extents\n"
"\n"
" fsmap prints the map of disk blocks used by the whole filesystem.\n"
" The map lists each extent used by the file, as well as regions in the\n"
" filesystem that do not have any corresponding blocks (free space).\n"
" By default, each line of the listing takes the following form:\n"
"     extent: [startoffset..endoffset] owner startblock..endblock\n"
" All the file offsets and disk blocks are in units of 512-byte blocks.\n"
" -n -- query n extents.\n"
" -v -- Verbose information, specify ag info.  Show flags legend on 2nd -v\n"
"\n"));
}

static int
numlen(
	off64_t	val)
{
	off64_t	tmp;
	int	len;

	for (len = 0, tmp = val; tmp > 0; tmp = tmp/10)
		len++;
	return (len == 0 ? 1 : len);
}

static const char *
special_owner(
	__int64_t	owner)
{
	switch (owner) {
	case FMV_OWN_FREE:
		return _("free space");
	case FMV_OWN_UNKNOWN:
		return _("unknown");
	case FMV_OWN_FS:
		return _("static fs metadata");
	case FMV_OWN_LOG:
		return _("journalling log");
	case FMV_OWN_AG:
		return _("per-AG metadata");
	case FMV_OWN_INOBT:
		return _("inode btree");
	case FMV_OWN_INODES:
		return _("inodes");
	case FMV_OWN_REFC:
		return _("refcount btree");
	case FMV_OWN_COW:
		return _("cow reservation");
	default:
		return _("unknown");
	}
}

static void
dump_map(
	unsigned long long	nr,
	struct getfsmapx	*map)
{
	unsigned long long	i;
	struct getfsmapx	*p;

	for (i = 0, p = map + 2; i < map->fmv_entries; i++, p++) {
		printf("\t%llu: [%lld..%lld]: ", i + nr,
			(long long) p->fmv_block,
			(long long)(p->fmv_block + p->fmv_length - 1));
		if (p->fmv_oflags & FMV_OF_SPECIAL_OWNER)
			printf("%s", special_owner(p->fmv_owner));
		else if (p->fmv_oflags & FMV_OF_EXTENT_MAP)
			printf(_("inode %lld extent map"),
				(long long) p->fmv_owner);
		else
			printf(_("inode %lld %lld..%lld"),
				(long long) p->fmv_owner,
				(long long) p->fmv_offset,
				(long long)(p->fmv_offset + p->fmv_length - 1));
		printf(_(" %lld blocks\n"),
			(long long)p->fmv_length);
	}
}

/*
 * Verbose mode displays:
 *   extent: [startblock..endblock]: startoffset..endoffset \
 *	ag# (agoffset..agendoffset) totalbbs flags
 */
#define MINRANGE_WIDTH	16
#define MINAG_WIDTH	2
#define MINTOT_WIDTH	5
#define NFLG		7		/* count of flags */
#define	FLG_NULL	00000000	/* Null flag */
#define	FLG_SHARED	01000000	/* shared extent */
#define	FLG_ATTR_FORK	00100000	/* attribute fork */
#define	FLG_PRE		00010000	/* Unwritten extent */
#define	FLG_BSU		00001000	/* Not on begin of stripe unit  */
#define	FLG_ESU		00000100	/* Not on end   of stripe unit  */
#define	FLG_BSW		00000010	/* Not on begin of stripe width */
#define	FLG_ESW		00000001	/* Not on end   of stripe width */
static void
dump_map_verbose(
	unsigned long long	nr,
	struct getfsmapx	*map,
	bool			*dumped_flags,
	struct xfs_fsop_geom	*fsgeo)
{
	unsigned long long	i;
	struct getfsmapx	*p;
	int			agno;
	off64_t 		agoff, bbperag;
	int			foff_w, boff_w, aoff_w, tot_w, agno_w, own_w, nr_w;
	char			rbuf[32], bbuf[32], abuf[32], obuf[32], nbuf[32];
	int			sunit, swidth;
	int			flg = 0;

	foff_w = boff_w = aoff_w = own_w = MINRANGE_WIDTH;
	nr_w = 4;
	tot_w = MINTOT_WIDTH;
	bbperag = (off64_t)fsgeo->agblocks *
		  (off64_t)fsgeo->blocksize / BBSIZE;
	sunit = (fsgeo->sunit * fsgeo->blocksize) / BBSIZE;
	swidth = (fsgeo->swidth * fsgeo->blocksize) / BBSIZE;

	/*
	 * Go through the extents and figure out the width
	 * needed for all columns.
	 */
	for (i = 0, p = map + 2; i < map->fmv_entries; i++, p++) {
		if (p->fmv_oflags & FMV_OF_PREALLOC ||
		    p->fmv_oflags & FMV_OF_ATTR_FORK ||
		    p->fmv_oflags & FMV_OF_SHARED)
			flg = 1;
		if (sunit &&
		    (p->fmv_block  % sunit != 0 ||
		     ((p->fmv_block + p->fmv_length) % sunit) != 0 ||
		     p->fmv_block % swidth != 0 ||
		     ((p->fmv_block + p->fmv_length) % swidth) != 0))
			flg = 1;
		if (flg)
			*dumped_flags = true;
		snprintf(nbuf, sizeof(nbuf), "%llu", nr + i);
		nr_w = max(nr_w, strlen(nbuf));
		snprintf(bbuf, sizeof(bbuf), "[%lld..%lld]:",
			(long long) p->fmv_block,
			(long long)(p->fmv_block + p->fmv_length - 1));
		boff_w = max(boff_w, strlen(bbuf));
		if (p->fmv_oflags & FMV_OF_SPECIAL_OWNER)
			own_w = max(own_w, strlen(special_owner(p->fmv_owner)));
		else {
			snprintf(obuf, sizeof(obuf), "%lld",
				(long long)p->fmv_owner);
			own_w = max(own_w, strlen(obuf));
		}
		if (p->fmv_oflags & FMV_OF_EXTENT_MAP)
			foff_w = max(foff_w, strlen(_("extent_map")));
		else if (p->fmv_oflags & FMV_OF_SPECIAL_OWNER)
			;
		else {
			snprintf(rbuf, sizeof(rbuf), "%lld..%lld",
				(long long) p->fmv_offset,
				(long long)(p->fmv_offset + p->fmv_length - 1));
			foff_w = max(foff_w, strlen(rbuf));
		}
		agno = p->fmv_block / bbperag;
		agoff = p->fmv_block - (agno * bbperag);
		snprintf(abuf, sizeof(abuf),
			"(%lld..%lld)",
			(long long)agoff,
			(long long)(agoff + p->fmv_length - 1));
		aoff_w = max(aoff_w, strlen(abuf));
		tot_w = max(tot_w,
			numlen(p->fmv_length));
	}
	agno_w = max(MINAG_WIDTH, numlen(fsgeo->agcount));
	if (nr == 0)
		printf("%*s: %-*s %-*s %-*s %*s %-*s %*s%s\n",
			nr_w, _("EXT"),
			boff_w, _("BLOCK-RANGE"),
			own_w, _("OWNER"),
			foff_w, _("FILE-OFFSET"),
			agno_w, _("AG"),
			aoff_w, _("AG-OFFSET"),
			tot_w, _("TOTAL"),
			flg ? _(" FLAGS") : "");
	for (i = 0, p = map + 2; i < map->fmv_entries; i++, p++) {
		flg = FLG_NULL;
		if (p->fmv_oflags & FMV_OF_PREALLOC)
			flg |= FLG_PRE;
		if (p->fmv_oflags & FMV_OF_ATTR_FORK)
			flg |= FLG_ATTR_FORK;
		if (p->fmv_oflags & FMV_OF_SHARED)
			flg |= FLG_SHARED;
		/*
		 * If striping enabled, determine if extent starts/ends
		 * on a stripe unit boundary.
		 */
		if (sunit) {
			if (p->fmv_block  % sunit != 0) {
				flg |= FLG_BSU;
			}
			if (((p->fmv_block +
			      p->fmv_length ) % sunit ) != 0) {
				flg |= FLG_ESU;
			}
			if (p->fmv_block % swidth != 0) {
				flg |= FLG_BSW;
			}
			if (((p->fmv_block +
			      p->fmv_length ) % swidth ) != 0) {
				flg |= FLG_ESW;
			}
		}
		snprintf(bbuf, sizeof(bbuf), "[%lld..%lld]:",
			(long long) p->fmv_block,
			(long long)(p->fmv_block + p->fmv_length - 1));
		if (p->fmv_oflags & FMV_OF_SPECIAL_OWNER) {
			snprintf(obuf, sizeof(obuf), "%s",
				special_owner(p->fmv_owner));
			snprintf(rbuf, sizeof(rbuf), " ");
		} else {
			snprintf(obuf, sizeof(obuf), "%lld",
				(long long)p->fmv_owner);
			snprintf(rbuf, sizeof(rbuf), "%lld..%lld",
				(long long) p->fmv_offset,
				(long long)(p->fmv_offset + p->fmv_length - 1));
		}
		agno = p->fmv_block / bbperag;
		agoff = p->fmv_block - (agno * bbperag);
		snprintf(abuf, sizeof(abuf),
			"(%lld..%lld)",
			(long long)agoff,
			(long long)(agoff + p->fmv_length - 1));
		if (p->fmv_oflags & FMV_OF_EXTENT_MAP)
			printf("%*llu: %-*s %-*s %-*s %*d %-*s %*lld\n",
				nr_w, nr + i,
				boff_w, bbuf,
				own_w, obuf,
				foff_w, _("extent map"),
				agno_w, agno,
				aoff_w, abuf,
				tot_w, (long long)p->fmv_length);
		else {
			printf("%*llu: %-*s %-*s %-*s", nr_w, nr + i,
				boff_w, bbuf, own_w, obuf, foff_w, rbuf);
			printf(" %*d %-*s", agno_w, agno,
				aoff_w, abuf);
			printf(" %*lld", tot_w,
				(long long)p->fmv_length);
			if (flg == FLG_NULL) {
				printf("\n");
			} else {
				printf(" %-*.*o\n", NFLG, NFLG, flg);
			}
		}
	}
}

static void
dump_verbose_key(void)
{
	printf(_(" FLAG Values:\n"));
	printf(_("    %*.*o Shared extent\n"),
		NFLG+1, NFLG+1, FLG_SHARED);
	printf(_("    %*.*o Attribute fork\n"),
		NFLG+1, NFLG+1, FLG_ATTR_FORK);
	printf(_("    %*.*o Unwritten preallocated extent\n"),
		NFLG+1, NFLG+1, FLG_PRE);
	printf(_("    %*.*o Doesn't begin on stripe unit\n"),
		NFLG+1, NFLG+1, FLG_BSU);
	printf(_("    %*.*o Doesn't end   on stripe unit\n"),
		NFLG+1, NFLG+1, FLG_ESU);
	printf(_("    %*.*o Doesn't begin on stripe width\n"),
		NFLG+1, NFLG+1, FLG_BSW);
	printf(_("    %*.*o Doesn't end   on stripe width\n"),
		NFLG+1, NFLG+1, FLG_ESW);
}

int
fsmap_f(
	int			argc,
	char			**argv)
{
	struct getfsmapx	*p;
	struct getfsmapx	*nmap;
	struct getfsmapx	*map;
	struct xfs_fsop_geom	fsgeo;
	long long		start = 0;
	long long		end = -1;
	int			nmap_size;
	int			map_size;
	int			nflag = 0;
	int			vflag = 0;
	int			fmv_iflags = 0;	/* flags for XFS_IOC_GETFSMAPX */
	int			i = 0;
	int			c;
	unsigned long long	nr = 0;
	size_t			fsblocksize, fssectsize;
	bool			dumped_flags = false;

	init_cvtnum(&fsblocksize, &fssectsize);

	while ((c = getopt(argc, argv, "n:v")) != EOF) {
		switch (c) {
		case 'n':	/* number of extents specified */
			nflag = atoi(optarg);
			break;
		case 'v':	/* Verbose output */
			vflag++;
			break;
		default:
			return command_usage(&fsmap_cmd);
		}
	}

	if (argc > optind) {
		start = cvtnum(fsblocksize, fssectsize, argv[optind]);
		if (start < 0) {
			fprintf(stderr,
				_("Bad rmap start_fsb %s.\n"),
				argv[optind]);
			return 0;
		}
	}

	if (argc > optind + 1) {
		end = cvtnum(fsblocksize, fssectsize, argv[optind + 1]);
		if (end < 0) {
			fprintf(stderr,
				_("Bad rmap end_fsb %s.\n"),
				argv[optind + 1]);
			return 0;
		}
	}

	if (vflag) {
		c = xfsctl(file->name, file->fd, XFS_IOC_FSGEOMETRY_V1, &fsgeo);
		if (c < 0) {
			fprintf(stderr,
				_("%s: can't get geometry [\"%s\"]: %s\n"),
				progname, file->name, strerror(errno));
			exitcode = 1;
			return 0;
		}
	}

	map_size = nflag ? nflag + 2 : 32;	/* initial guess - 32 */
	map = malloc(map_size * sizeof(*map));
	if (map == NULL) {
		fprintf(stderr, _("%s: malloc of %lu bytes failed.\n"),
			progname, map_size * sizeof(*map));
		exitcode = 1;
		return 0;
	}

	map->fmv_iflags = fmv_iflags;
	map->fmv_block = start / 512;
	map->fmv_owner = 0;
	map->fmv_offset = 0;
	map->fmv_length = 0;
	(map + 1)->fmv_block = (unsigned long long)end / 512;
	(map + 1)->fmv_owner = ULLONG_MAX;
	(map + 1)->fmv_offset = ULLONG_MAX;

	/* Count mappings */
	if (!nflag) {
		map->fmv_count = 2;
		i = xfsctl(file->name, file->fd, XFS_IOC_GETFSMAPX, map);
		if (i < 0) {
			fprintf(stderr, _("%s: xfsctl(XFS_IOC_GETFSMAPX)"
				" iflags=0x%llx [\"%s\"]: %s\n"),
				progname, map->fmv_iflags, file->name,
				strerror(errno));
			free(map);
			exitcode = 1;
			return 0;
		}
		if (map->fmv_entries > map_size * 2) {
			unsigned long long nr;

			nr = 5ULL * map->fmv_entries / 4 + 2;
			nmap_size = nr > INT_MAX ? INT_MAX : nr;
			nmap = realloc(map, nmap_size * sizeof(*map));
			if (nmap == NULL) {
				fprintf(stderr,
					_("%s: cannot realloc %lu bytes\n"),
					progname, map_size*sizeof(*map));
			} else {
				map = nmap;
				map_size = nmap_size;
			}
		}
	}

	map->fmv_count = map_size;
	do {
		/* Get some extents */
		i = xfsctl(file->name, file->fd, XFS_IOC_GETFSMAPX, map);
		if (i < 0) {
			fprintf(stderr, _("%s: xfsctl(XFS_IOC_GETFSMAPX)"
				" iflags=0x%llx [\"%s\"]: %s\n"),
				progname, map->fmv_iflags, file->name,
				strerror(errno));
			free(map);
			exitcode = 1;
			return 0;
		}

		if (map->fmv_entries == 0)
			break;

		if (!vflag)
			dump_map(nr, map);
		else
			dump_map_verbose(nr, map, &dumped_flags, &fsgeo);

		p = map + 1 + map->fmv_entries;
		if (p->fmv_oflags & FMV_OF_LAST)
			break;

		nr += map->fmv_entries;
		map->fmv_block = p->fmv_block;
		map->fmv_owner = p->fmv_owner;
		map->fmv_offset = p->fmv_offset;
		map->fmv_oflags = p->fmv_oflags;
		map->fmv_length = p->fmv_length;
	} while(true);

	if (dumped_flags)
		dump_verbose_key();

	free(map);
	return 0;
}

void
fsmap_init(void)
{
	fsmap_cmd.name = "fsmap";
	fsmap_cmd.cfunc = fsmap_f;
	fsmap_cmd.argmin = 0;
	fsmap_cmd.argmax = -1;
	fsmap_cmd.flags = CMD_NOMAP_OK;
	fsmap_cmd.args = _("[-v] [-n nx] [start] [end]");
	fsmap_cmd.oneline = _("print filesystem mapping for a range of blocks");
	fsmap_cmd.help = fsmap_help;

	add_command(&fsmap_cmd);
}
